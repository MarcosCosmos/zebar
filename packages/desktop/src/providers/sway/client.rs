use std::{error::Error as StdError, fmt, fmt::Formatter};

use futures_util::{stream::StreamExt, TryFutureExt};
use swayipc_async::{Connection, Error, Event, EventType};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio_util::sync::CancellationToken;

use crate::providers::sway::SwayOutput;

pub struct SwayClient {
  event_rx: UnboundedReceiver<anyhow::Result<Event>>,
  write_con: Connection,
  cancellation: CancellationToken,
}

#[derive(Debug)]
pub struct SwayErrorGroup(Vec<Error>);

impl From<Vec<Error>> for SwayErrorGroup {
  fn from(errs: Vec<Error>) -> Self {
    Self(errs)
  }
}

impl fmt::Display for SwayErrorGroup {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match writeln!(f, "Sway encountered one or more errors. Details:")? {
      () => {
        for err in &self.0 {
          writeln!(f, "{}", err)?
        }
      }
    }

    Ok(())
  }
}

impl StdError for SwayErrorGroup {}

impl SwayErrorGroup {
  pub fn new(errs: Vec<Error>) -> SwayErrorGroup {
    SwayErrorGroup(errs)
  }
}

impl SwayClient {
  pub async fn new<T: AsRef<[EventType]>>(
    event_types: T,
  ) -> anyhow::Result<SwayClient> {
    Connection::new()
            .and_then(async |write_con| {
                Connection::new()
                    .and_then(async |read_con| read_con.subscribe(event_types).await)
                    .await.map(|mut events| {
                        let (event_tx, event_rx) = unbounded_channel();
                        let cancellation = CancellationToken::new();
                        let child_token = cancellation.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::select! {
                                    event = events.next() => match event {
                                        Some(event) => {
                                            let result: anyhow::Result<Event> = event.map_err(|err| err.into());
                                            if event_tx.send(result).is_err() {
                                                warn!("Failed to send sway event as the receiver has closed");
                                                break;
                                            };
                                        },
                                        None => break,
                                    },
                                    _ = child_token.cancelled() => {
                                        break;
                                    }
                                }
                            }
                        });
                        SwayClient { write_con, event_rx, cancellation }
                    })
            })
            .await.map_err(|err| err.into())
  }

  pub async fn next_state(&mut self) -> anyhow::Result<SwayOutput> {
    _ = self.event_rx.recv().await;
    match self.write_con.get_workspaces().await? {
      all_workspaces => match self.write_con.get_binding_modes().await? {
        binding_modes => {
          let active_binding_mode =
            match self.write_con.get_binding_state().await? {
              name if !name.is_empty() => Some(name),
              _ => None,
            };
          Ok(SwayOutput {
            all_workspaces,
            binding_modes,
            active_binding_mode,
          })
        }
      },
    }
  }

  pub async fn run_command<T: AsRef<str>>(
    &mut self,
    payload: T,
  ) -> anyhow::Result<()> {
    match self.write_con.run_command(payload).await {
      Ok(results) => {
        let errs: Vec<Error> = results
          .into_iter()
          .filter_map(move |err| err.err())
          .collect();
        if errs.is_empty() {
          Ok(())
        } else {
          Err(SwayErrorGroup::new(errs).into())
        }
      }
      Err(err) => Err(err.into()),
    }
  }
}

impl Drop for SwayClient {
  fn drop(&mut self) {
    self.cancellation.cancel();
  }
}
