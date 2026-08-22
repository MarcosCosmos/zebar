use crossbeam::channel::{unbounded, Sender};
use tokio::sync::mpsc::unbounded_channel;
use crate::providers::{
  audio::{
    common::AudioOutput, pulseaudio::client::PulseAudioClient,
    AudioFunctionWithReply, AudioProviderConfig,
  },
  sway::SwayProvider,
  CommonProviderState, Provider, ProviderFunction, ProviderInputMsg,
  RuntimeType,
};

use tracing::{info, warn, error};

pub struct AudioProvider(CommonProviderState);

impl AudioProvider {
  pub fn new(
    common: CommonProviderState,
  ) -> Self {
    Self(common)
  }
}

#[async_trait]
impl Provider for AudioProvider {
  fn runtime_type(&self) -> RuntimeType {
    RuntimeType::Async
  }

  async fn start_async(&mut self) {
    let (state_tx, mut state_rx) = unbounded_channel();
    let (func_tx, func_rx) = unbounded_channel();
    let client = PulseAudioClient::new(state_tx, func_rx);

    loop {
      tokio::select! {
        Some(output) = state_rx.recv() => {
          self.0.emitter.emit_output(output);
        },
        Some(input) = self.0.input.async_rx.recv() => {
          match input {
            ProviderInputMsg::Stop => {
              break;
            },
            ProviderInputMsg::Function(
              ProviderFunction::Audio(f),
              sender
            ) => {
              if let Err(err) = func_tx.send(AudioFunctionWithReply {f, sender}) {
                warn!("Failed to send function request. Details: {}", err);
              }
            },
            _ => {}
          }
        },
      }
    }
  }
}
