use rocket::serde::{Deserialize, Serialize};
use swayipc_async::EventType;

use crate::providers::{
  sway::{client::SwayClient, SwayOutput},
  CommonProviderState, Provider, ProviderFunction,
  ProviderFunctionResponse, ProviderInputMsg, RuntimeType,
};

pub struct SwayProvider(CommonProviderState);

impl SwayProvider {
  pub fn new(common: CommonProviderState) -> SwayProvider {
    SwayProvider(common)
  }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SwayProviderConfig {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwayFunction {
  RunCommand(String),
  // SendTick(String),
}

#[async_trait]
impl Provider for SwayProvider {
  fn runtime_type(&self) -> RuntimeType {
    RuntimeType::Async
  }

  async fn start_async(&mut self) {
    match SwayClient::new([EventType::Workspace, EventType::Mode]).await {
      Ok(mut client) => loop {
        tokio::select! {
          state = client.next_state() => {
            self.0.emitter.emit_output(state);
          }
          Some(input) = self.0.input.async_rx.recv() => {
            match input {
              ProviderInputMsg::Stop => {
                break;
              }
              ProviderInputMsg::Function(
                ProviderFunction::Sway(f),
                sender,
              ) => match f {
                SwayFunction::RunCommand(payload) => {
                  let result = client.run_command(payload)
                    .await
                    .map(|_| ProviderFunctionResponse::Null)
                    .map_err(|err| err.to_string());
                  sender.send(result).unwrap();
                }
              },
              _ => {}
            }
          }
        }
      },
      Err(err) => {
        error!("{:?}", err);
        self
          .0
          .emitter
          .emit_output::<SwayOutput>(Err(anyhow::anyhow!(
            "Failed to initialize sway client."
          )));
      }
    }
  }
}
