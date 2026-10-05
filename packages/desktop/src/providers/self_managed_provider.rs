use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use crate::providers::{ProviderEmitter, ProviderInputMsg, ProviderManager, ProviderRef};
use serde::{Deserialize, Serialize};

pub trait Provider {
  type Config;
  type Output;
  type Function;
}

pub trait SyncSpawn: Provider {
  fn spawn(config: Self::Config, common: CommonSyncProviderState);

  async fn spawn_managed(config_hash: String, config: Self::Config, provider_manager: &ProviderManager) -> anyhow::Result<ProviderRef> where <Self as Provider>::Config: 'static + Send {
    provider_manager.spawn_sync::<Self>(config_hash, config).await
  }
}

pub trait AsyncSpawn: Provider {
  #[allow(refining_impl_trait)]
  fn spawn(config: Self::Config, common: CommonAsyncProviderState) -> impl Future + Send;

  async fn spawn_managed(config_hash: String, config: Self::Config, provider_manager: &ProviderManager) -> anyhow::Result<ProviderRef> where <Self as Provider>::Config: 'static + Send {
    provider_manager.spawn_async::<Self>(config_hash, config).await
  }
}

/// an enum with no variants creates no-op logic, and is currently used in place of the eventual ! never type
/// To be used as filler for function and output on providers that have no such thing
#[derive(Deserialize, Debug)]
pub enum ProviderVoid {
}

pub struct CommonProviderState<T> {
  pub emitter: ProviderEmitter,

  /// Wrapper around the receiver channel for incoming inputs to the
  /// provider.
  pub input_rx: T,

  /// Shared `sysinfo` instance.
  pub sysinfo: Arc<Mutex<sysinfo::System>>,
}
pub type CommonSyncProviderState = CommonProviderState<crossbeam::channel::Receiver<ProviderInputMsg>>;
pub type CommonAsyncProviderState = CommonProviderState<mpsc::Receiver<ProviderInputMsg>>;


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


pub struct TestSyncProvider {
}

pub struct TestAsyncProvider {
}

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