use std::{future::Future, marker::PhantomData, sync::Arc};
use crossbeam::channel::SendError;
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

trait ProviderChannel {
  type Sender;
  type Receiver;
}

pub trait SyncProvider where Self : Provider + ProviderChannel<Sender=crossbeam::channel::Sender<ProviderInputMsg<Self::Function>>, Receiver=crossbeam::channel::Receiver<ProviderInputMsg<Self::Function>>>, ProviderSender: From<Self::Sender> {
  fn spawn(
    config: Self::Config,
    common: CommonSyncProviderState<Self>,
  );

  fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  where Self::Config: 'static + Send, <Self as Provider>::Function: 'static + Send
  {
    let (input_tx, input_rx) =
      crossbeam::channel::bounded(1);

    let common = CommonProviderState {
      input_rx,
      emitter: ProviderEmitter {
        emit_tx: provider_manager.emit_tx.clone(),
        config_hash: config_hash.clone(),
        prev_emission: None,
      },
      sysinfo: provider_manager.sysinfo.clone(),
    };

    let task_handle = task::spawn_blocking(move || {
      Self::spawn(config, common);
      tracing::info!("Provider stopped: {}", config_hash);
    });
    Ok(ProviderRef {
      input_tx: OldSender::SelfManaged(input_tx.into()),
      task_handle,
    })
  }
  fn call(
    tx: &Self::Sender,
    msg: ProviderInputMsg<Self::Function>,
  ) -> Result<(), SendError<ProviderInputMsg<Self::Function>>> {
    tx.send(msg)
  }
}

pub trait AsyncProvider where Self : Provider + ProviderChannel<Sender=tokio::sync::mpsc::Sender<ProviderInputMsg<Self::Function>>, Receiver=tokio::sync::mpsc::Receiver<ProviderInputMsg<Self::Function>>> + 'static, ProviderSender: From<Self::Sender> {
  #[allow(refining_impl_trait)]
  fn spawn(
    config: Self::Config,
    common: CommonAsyncProviderState<Self>
  ) -> impl Future + Send;

  fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  where
    Self::Config: 'static + Send, <Self as Provider>::Function: Send
  {
    let (input_tx, input_rx) = tokio::sync::mpsc::channel(1);

    let common = CommonProviderState {
      input_rx,
      emitter: ProviderEmitter {
        emit_tx: provider_manager.emit_tx.clone(),
        config_hash: config_hash.clone(),
        prev_emission: None,
      },
      sysinfo: provider_manager.sysinfo.clone(),
    };

    let task_handle = task::spawn(async move {
      Self::spawn(config, common).await;
      tracing::info!("Provider stopped: {}", config_hash);
    });
    Ok(ProviderRef {
      input_tx: OldSender::SelfManaged(input_tx.into()),
      task_handle,
    })
  }

  async fn call(
    tx: &Self::Sender,
    msg: ProviderInputMsg<Self::Function>,
  ) -> Result<(), tokio::sync::mpsc::error::SendError<ProviderInputMsg<Self::Function>>> where Self::Function : Send + Sync {
    tx.send(msg).await
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
pub type CommonSyncProviderState<T: ProviderChannel> =
  CommonProviderState<T::Receiver>;
pub type CommonAsyncProviderState<T: ProviderChannel> =
  CommonProviderState<T::Receiver>;

macro_rules! build_enum {
    ([$($mac:tt)*] $name:ident{$($body:tt)*} -> {}) => {
      $($mac)*
      pub enum $name {
        $($body)*
      }
    };
    ([$($mac:tt)*] $name:ident{$($body:tt)*} -> { $($($provider:ident)|+);+ => $trt:ident::$associated:ident, $($other:tt)* }) => {
      build_enum!(
        [$($mac)*]
        $name{
          $($body)*
          $($($provider(<$provider as $trt>::$associated),)+)+
        }
        -> {$($other)*}
      );
      $($(
        impl From<<$provider as $trt>::$associated> for $name {
          fn from(value: <$provider as $trt>::$associated) -> Self {
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
        [
        #[derive(Deserialize, Debug)]
        #[serde(tag = "type", rename_all = "snake_case")]
        ]
        ProviderConfig{} -> {$($sync_provider)|+ ; $($async_provider)|+ => Provider::Config,}
      );
      build_enum!(
        [
          #[derive(Debug, Clone, PartialEq, Serialize)]
          #[serde(untagged)]
        ]
        ProviderOutput{} -> {$($sync_provider)|+ ; $($async_provider)|+ => Provider::Output,}
      );
      build_enum!(
        [
          #[derive(Debug, Clone, Serialize, Deserialize)]
          #[serde(tag = "type", content = "function", rename_all = "snake_case")]
        ]
        ProviderFunction{} -> {$($sync_provider)|+ ; $($async_provider)|+ => Provider::Function,}
      );
      build_enum!(
        [
          #[derive(Debug, Clone, Serialize)]
          #[serde(untagged)]
        ]
        ProviderResponse{} -> {$($sync_provider)|+ ; $($async_provider)|+ => Provider::Response,}
      );
      build_enum!([] ProviderSender{} -> {
          $($sync_provider)|+ ; $($async_provider)|+ => ProviderChannel::Sender,
      });

      $(
        impl ProviderChannel for $sync_provider {
          type Sender = crossbeam::channel::Sender<ProviderInputMsg<<$sync_provider as Provider>::Function>>;
          type Receiver = crossbeam::channel::Receiver<ProviderInputMsg<<$sync_provider as Provider>::Function>>;
        }
      )+

      $(
        impl ProviderChannel for $async_provider {
          type Sender = tokio::sync::mpsc::Sender<ProviderInputMsg<<$async_provider as Provider>::Function>>;
          type Receiver = tokio::sync::mpsc::Receiver<ProviderInputMsg<<$async_provider as Provider>::Function>>;
        }
      )+
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
            (ProviderSender::$sync_provider(ref tx),  OldFunction::SelfManaged(ProviderFunction::$sync_provider(_f))) => $sync_provider::call(tx, ProviderInputMsg::Function(_f, response_tx)).map_err(|err|err.into()),
          )+
          $(
            (ProviderSender::$async_provider(ref tx),  OldFunction::SelfManaged(ProviderFunction::$async_provider(_f))) => $async_provider::call(tx, ProviderInputMsg::Function(_f, response_tx)).await.map_err(|err|err.into()),
          )+
          _ => panic!(),
        }
      }
      pub async fn stop(sender: &ProviderSender) -> anyhow::Result<()> {
        match (sender) {
          $(
            ProviderSender::$sync_provider(ref tx) => $sync_provider::call(tx, ProviderInputMsg::Stop).map_err(|err|err.into()),
          )+
          $(
            ProviderSender::$async_provider(ref tx) => $async_provider::call(tx, ProviderInputMsg::Stop).await.map_err(|err|err.into()),
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
  fn spawn(config: <Self as Provider>::Config, common: CommonSyncProviderState<Self>) {
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
  async fn spawn(config: <Self as Provider>::Config, common: CommonAsyncProviderState<Self>) {
    todo!()
  }
}

declare_provider_set!(sync[TestSyncProvider] async[TestAsyncProvider]);
