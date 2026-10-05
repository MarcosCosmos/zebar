use std::{future::Future, marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{
  sync::{mpsc, Mutex},
  task,
};

use crate::providers::{
  ProviderEmitter, ProviderInputMsg, ProviderManager, ProviderRef,
  ProviderSender,
};

pub trait Provider {
  type Config;
  type Output;
  type Function;
}

pub trait SyncSpawn: Provider {
  fn spawn(
    config: Self::Config,
    common: CommonSyncProviderState,
  );

  async fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  where
    <Self as Provider>::Config: 'static + Send,
  {
    let (input_tx, input_rx) =
      crossbeam::channel::bounded::<ProviderInputMsg>(1);

    let common = CommonSyncProviderState {
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
      input_tx: ProviderSender::Sync(
        input_tx,
      ),
      task_handle,
    })
  }
}

pub trait AsyncSpawn: Provider {
  #[allow(refining_impl_trait)]
  fn spawn(
    config: Self::Config,
    common: CommonAsyncProviderState,
  ) -> impl Future + Send;

  async fn spawn_managed(
    config_hash: String,
    config: Self::Config,
    provider_manager: &ProviderManager,
  ) -> anyhow::Result<ProviderRef>
  where
    <Self as Provider>::Config: 'static + Send,
  {
    let (input_tx, input_rx) = mpsc::channel::<ProviderInputMsg>(1);

    let common = CommonAsyncProviderState {
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
      input_tx: ProviderSender::Async(input_tx),
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
pub type CommonSyncProviderState =
  CommonProviderState<crossbeam::channel::Receiver<ProviderInputMsg>>;
pub type CommonAsyncProviderState =
  CommonProviderState<mpsc::Receiver<ProviderInputMsg>>;

macro_rules! build_enum {
    ($name:ident{$($body:tt)*}) => {
      #[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
      #[serde(rename_all = "camelCase")]
      pub enum $name {
        $($body)*
      }
    };
    ($name:ident{$($body:tt)*} $($provider:ident),+ => $associated:ident) => {
      build_enum!(
        $name{
          $($provider(<$provider as Provider>::$associated),)+
        }
      );
      $(
        impl From<<$provider as Provider>::$associated> for $name {
          fn from(value: <$provider as Provider>::$associated) -> Self {
            Self::$provider(value)
          }
        }
      )+
    };
}

macro_rules! declare_provider_set {
    ($($provider:ident),+) => {
      // build_enum!(SelfManagedProviders{} $(more),+);
      build_enum!(ProviderConfig{} $($provider),+ => Config);
      build_enum!(ProviderOutput{} $($provider),+ => Output);
      build_enum!(ProviderFunction{} $($provider),+ => Function);
      pub async fn spawn_managed(config_hash: String, config: ProviderConfig, provider_manager: &ProviderManager) -> anyhow::Result<ProviderRef> {
        match config {
          $(
            ProviderConfig::$provider(inner) => $provider::spawn_managed(config_hash, inner, provider_manager).await,
          )+
        }
      }
    };
}

pub struct TestSyncProvider {}

pub struct TestAsyncProvider {}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestSyncProviderConfig();

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct TestAsyncProviderConfig();

impl Provider for TestAsyncProvider {
  type Config = TestAsyncProviderConfig;
  type Output = PhantomData<Self>;
  type Function = PhantomData<Self>;
}

impl AsyncSpawn for TestAsyncProvider {
  async fn spawn(config: Self::Config, common: CommonAsyncProviderState) {
    todo!()
  }
}

impl Provider for TestSyncProvider {
  type Config = TestSyncProviderConfig;
  type Output = PhantomData<Self>;
  type Function = PhantomData<Self>;
}

impl SyncSpawn for TestSyncProvider {
  fn spawn(config: Self::Config, common: CommonSyncProviderState) {
    todo!()
  }
}

declare_provider_set!(TestSyncProvider, TestAsyncProvider);
