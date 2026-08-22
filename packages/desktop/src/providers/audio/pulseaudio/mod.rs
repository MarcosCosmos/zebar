mod client;
mod volume_levels;

mod provider;
mod state;


pub use provider::AudioProvider;

use crate::providers::{AudioFunction, ProviderFunctionResult};

pub struct AudioFunctionWithReply {
  f: AudioFunction,
  sender: tokio::sync::oneshot::Sender<ProviderFunctionResult>,
}
