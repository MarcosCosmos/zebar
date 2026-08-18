use crate::providers::{
  CommonProviderState, Provider, ProviderFunction,
  ProviderFunctionResponse, ProviderInputMsg, RuntimeType,
};
use async_trait::async_trait;
use clap::builder::Str;
use crossbeam::channel::unbounded;
use futures_util::stream::StreamExt;
use futures_util::TryFutureExt;
pub(crate) use output::SwayOutput;
use rocket::{futures::future::err, yansi::Paint};
use serde::{Deserialize, Serialize};
use swayipc_async::{Connection, EventType, Fallible, Workspace};

mod output;
mod client;
pub use output::*;

use client::*;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SwayProviderConfig {}

pub struct SwayProvider(CommonProviderState);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwayFunction {
  RunCommand(String),
  // SendTick(String),
}

fn workspace_eq(a: &Workspace, b: &Workspace) -> bool {
  a.id == b.id
    && a.num == b.num
    && a.name == b.name
    && a.layout == b.layout
    && a.visible == b.visible
    && a.focused == b.focused
    && a.urgent == b.urgent
    && a.representation == b.representation
    && a.rect == b.rect
    && a.output == b.output
    && a.focus == b.focus
}

impl SwayProvider {
  pub fn new(common: CommonProviderState) -> SwayProvider { SwayProvider(common) }
}

#[async_trait]
impl Provider for SwayProvider {
  fn runtime_type(&self) -> RuntimeType {
    RuntimeType::Async
  }

  async fn start_async(&mut self) {
    match SwayClient::new([EventType::Workspace, EventType::Mode]).await {
      Ok(mut client) => {
        loop {
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
        }
      },
      Err(err) => {
        error!("{:?}", err);
        self.0
            .emitter
            .emit_output::<SwayOutput>(Err(anyhow::anyhow!(
              "Failed to initialize sway client."
            )));
      }
    }
    }
}
