use std::{future::Future, marker::PhantomData, sync::Arc};
use serde::{Deserialize, Serialize};
use tokio::{
  sync::Mutex,
  task,
};
use tokio::sync::oneshot;
use crate::providers::{ProviderEmitter, ProviderFunction as OldFunction, ProviderFunctionResult, ProviderManager, ProviderRef, ProviderSender as OldSender};
pub trait Provider {
  type Config;
  type Output;
  type Function;
  type Response;
}

pub enum ProviderInputMsg<T> {
  Function(T, oneshot::Sender<ProviderFunctionResult>),
  Stop,
}

pub trait SyncProvider: Provider where ProviderSender: From<crossbeam::channel::Sender<ProviderInputMsg<Self::Function>>>, Self::Config: 'static + Send, <Self as Provider>::Function: 'static + Send {
  fn spawn(
    config: Self::Config,
    common: CommonProviderState<crossbeam::channel::Receiver<ProviderInputMsg<Self::Function>>>,
  );
  fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  {
    let (input_tx, input_rx) =
      crossbeam::channel::bounded(1);

    let common = provider_manager.create_common_state(config_hash.clone(), input_rx);

    let task_handle = task::spawn_blocking(move || {
      Self::spawn(config, common);
      tracing::info!("Provider stopped: {}", config_hash);
    });
    Ok(ProviderRef {
      input_tx: OldSender::SelfManaged(input_tx.into()),
      task_handle,
    })
  }
}

pub trait AsyncProvider: Provider where ProviderSender: From<tokio::sync::mpsc::Sender<ProviderInputMsg<Self::Function>>>, Self::Config: 'static + Send, <Self as Provider>::Function: 'static + Send,
{
  fn spawn(
    config: Self::Config,
    common: CommonProviderState<tokio::sync::mpsc::Receiver<ProviderInputMsg<Self::Function>>>
  ) -> impl Future + Send;

  fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);

    let common = provider_manager.create_common_state(config_hash.clone(), input_rx);

    let task_handle = task::spawn(async move {
      Self::spawn(config, common).await;
      tracing::info!("Provider stopped: {}", config_hash);
    });
    Ok(ProviderRef {
      input_tx: OldSender::SelfManaged(input_tx.into()),
      task_handle,
    })
  }
}

pub struct CommonProviderState<T> {
  pub emitter: ProviderEmitter,

  /// Wrapper around the receiver channel for incoming inputs to the
  /// provider.
  pub input_rx: T,

  /// Shared `sysinfo` instance.
  pub sysinfo: Arc<Mutex<sysinfo::System>>,
}

macro_rules! build_enum {
    ($([$($mac:tt)+])* $name:ident{$($($provider:ident: $value_type:ty),+);+}) => {
      $(#[$($mac)+]
      )*
      pub enum $name{
        $($($provider($value_type),)+)+
      }
      $($(
        impl From<$value_type> for $name {
          fn from(value: $value_type) -> Self {
            Self::$provider(value)
          }
        }
      )+)+
    };
}

macro_rules! declare_provider_set {
    (sync[$($sync_provider:ident),+] async[$($async_provider:ident),+]) => {
      // build_enum!(SelfManagedProviders{} $(more),+);
      build_enum!(
        [derive(Deserialize, Debug)]
        [serde(tag = "type", rename_all = "snake_case")]
        ProviderConfig{
          $($sync_provider: <$sync_provider as Provider>::Config),+;
          $($async_provider: <$async_provider as Provider>::Config),+
        }
      );
      build_enum!(
        [derive(Debug, Clone, PartialEq, Serialize)]
        [serde(untagged)]
        ProviderOutput{
          $($sync_provider: <$sync_provider as Provider>::Output),+;
          $($async_provider: <$async_provider as Provider>::Output),+
        }
      );
      build_enum!(
        [derive(Debug, Clone, Serialize, Deserialize)]
        [serde(tag = "type", content = "function", rename_all = "snake_case")]
        ProviderFunction{
          $($sync_provider: <$sync_provider as Provider>::Function),+;
          $($async_provider: <$async_provider as Provider>::Function),+
        }
      );
      build_enum!(
        [derive(Debug, Clone, Serialize)]
        [serde(untagged)]
        ProviderResponse{
          $($sync_provider: <$sync_provider as Provider>::Response),+;
          $($async_provider: <$async_provider as Provider>::Response),+
        }
      );
      build_enum!(
        ProviderSender{
          $($sync_provider: crossbeam::channel::Sender<ProviderInputMsg<<$sync_provider as Provider>::Function>>),+;
          $($async_provider: tokio::sync::mpsc::Sender<ProviderInputMsg<<$async_provider as Provider>::Function>>),+
       }
      );
      pub fn spawn_managed(config_hash: String, config: ProviderConfig, provider_manager: &ProviderManager) -> anyhow::Result<ProviderRef> {
        match config {
          $(
            ProviderConfig::$sync_provider(inner) => $sync_provider::spawn_managed(config_hash, inner, provider_manager),
          )+
          $(
            ProviderConfig::$async_provider(inner) => $async_provider::spawn_managed(config_hash, inner, provider_manager),
          )+
        }
      }
      pub async fn call_function(sender: &ProviderSender, f: OldFunction, response_tx: oneshot::Sender<ProviderFunctionResult>) -> anyhow::Result<()> {
        match (sender, f) {
          $(
            (ProviderSender::$sync_provider(ref tx),  OldFunction::SelfManaged(ProviderFunction::$sync_provider(_f))) => tx.send(ProviderInputMsg::Function(_f, response_tx)).map_err(|err|err.into()),
          )+
          $(
            (ProviderSender::$async_provider(ref tx),  OldFunction::SelfManaged(ProviderFunction::$async_provider(_f))) => tx.send(ProviderInputMsg::Function(_f, response_tx)).await.map_err(|err|err.into()),
          )+
          _ => panic!("Sender type did not match function type"),
        }
      }
      pub async fn stop(sender: &ProviderSender) -> anyhow::Result<()> {
        match (sender) {
          $(
            ProviderSender::$sync_provider(ref tx) => tx.send(ProviderInputMsg::Stop).map_err(|err|err.into()),
          )+
          $(
            ProviderSender::$async_provider(ref tx) => tx.send(ProviderInputMsg::Stop).await.map_err(|err|err.into()),
          )+
        }
      }
    };
}

pub struct TestSyncProvider {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestSyncProviderConfig {}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct TestSyncProviderOutput {}

impl Provider for TestSyncProvider {
  type Config = TestSyncProviderConfig;
  type Output = TestSyncProviderOutput;
  type Function = PhantomData<Self>;
  type Response = PhantomData<Self>;
}

impl SyncProvider for TestSyncProvider {
  fn spawn(config: <Self as Provider>::Config, common: CommonProviderState<crossbeam::channel::Receiver<ProviderInputMsg<<Self as Provider>::Function>>>) {
    todo!()
  }
}


pub struct TestAsyncProvider {}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestAsyncProviderConfig();

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct TestAsyncProviderOutput();

impl Provider for TestAsyncProvider {
  type Config = TestAsyncProviderConfig;
  type Output = TestAsyncProviderOutput;
  type Function = PhantomData<Self>;
  type Response = PhantomData<Self>;

}

impl AsyncProvider for TestAsyncProvider {
  async fn spawn(config: <Self as Provider>::Config, common: CommonProviderState<tokio::sync::mpsc::Receiver<ProviderInputMsg<<Self as Provider>::Function>>>) {
    todo!()
  }
}

declare_provider_set!(sync[TestSyncProvider] async[TestAsyncProvider]);
