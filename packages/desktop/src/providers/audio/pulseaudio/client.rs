use std::{
  cell::{Ref, RefCell, RefMut},
  fmt,
  fmt::Formatter,
  ops::Deref,
  rc::Rc,
};

use gdk::{
  gio::spawn_blocking, glib::bitflags::__private::serde::de::StdError,
};
use gtk::{false_, SortColumn::Default};
use libpulse_binding::{
  callbacks::ListResult,
  context::{
    subscribe::{Facility, InterestMaskSet, Operation},
    Context, FlagSet, State as PAState,
  },
  def::Retval,
  error::PAErr,
  mainloop::standard::{IterateResult, Mainloop},
  proplist::Proplist,
  volume::Volume,
};
use tokio::sync::mpsc::{
  error::TryRecvError, UnboundedReceiver, UnboundedSender,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::providers::{
  audio::{
    common::{AudioDevice, AudioOutput, DeviceType},
    pulseaudio::{
      state::{PulseAudioDevice, PulseAudioState},
      volume_levels::{OutputWithLevels, VolumeLevels},
    },
    AudioFunctionWithReply,
  },
  AudioFunction, ProviderFunctionResponse, SetMuteArgs, SetVolumeArgs,
};

#[derive(Debug, Copy, Clone)]
pub enum PulseAudioError {
  CreatePropList,
  UpdatePropList,
  CreateMainLoop,
  CreateContext,
  PAErr(PAErr),
  Quit(Retval),
  ContextTerminated,
  SourceListChange,
  SinkListChange,
}

pub struct PulseAudioClient(CancellationToken);

impl From<PAErr> for PulseAudioError {
  fn from(value: PAErr) -> Self {
    PulseAudioError::PAErr(value)
  }
}

impl fmt::Display for PulseAudioError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      PulseAudioError::CreatePropList => {
        writeln!(f, "Failed to create PulseAudio proplist")
      }
      PulseAudioError::UpdatePropList => {
        writeln!(f, "Failed to update PulseAudio proplist")
      }
      PulseAudioError::CreateMainLoop => {
        writeln!(f, "Failed to create PulseAudio main_loop")
      }
      PulseAudioError::CreateContext => {
        writeln!(f, "Failed to create PulseAudio context")
      }
      PulseAudioError::PAErr(code) => fmt::Display::fmt(&code, f),
      PulseAudioError::Quit(val) => {
        writeln!(
          f,
          "The PulseAudio client quit with return value: {}",
          val.0
        )
      }
      PulseAudioError::ContextTerminated => {
        writeln!(f, "The PulseAudio context failed or terminated.")
      }
      PulseAudioError::SourceListChange => {
        writeln!(f, "An error occurred while processing an update to the PulseAudio source list")
      }
      PulseAudioError::SinkListChange => {
        writeln!(f, "An error occurred while processing an update to the PulseAudio source list")
      }
    }
  }
}

impl StdError for PulseAudioError {}

struct InnerClient {
  state_tx: UnboundedSender<anyhow::Result<AudioOutput>>,
  function_rx: UnboundedReceiver<AudioFunctionWithReply>,
  main_loop: Rc<RefCell<Mainloop>>,
  context: Rc<RefCell<Context>>,
  cancellation: CancellationToken,
  state: Rc<RefCell<PulseAudioState>>,
}

impl InnerClient {
  pub fn new(
    state_tx: UnboundedSender<anyhow::Result<AudioOutput>>,
    function_rx: UnboundedReceiver<AudioFunctionWithReply>,
    cancellation: CancellationToken,
  ) -> anyhow::Result<InnerClient> {
    let Some(mut proplist) = Proplist::new() else {
      Err(PulseAudioError::CreatePropList)?
    };

    if proplist.set_str("APPLICATION_NAME", "zebar").is_err() {
      Err(PulseAudioError::UpdatePropList)?
    };

    let mut main_loop = {
      let Some(mut moop) = Mainloop::new() else {
        Err(PulseAudioError::CreateMainLoop)?
      };
      Rc::new(RefCell::new(moop))
    };

    let mut context = {
      let Some(ctx) = Context::new_with_proplist(
        main_loop.borrow().deref(),
        "ZebarContext",
        &proplist,
      ) else {
        Err(PulseAudioError::CreateContext)?
      };
      Rc::new(RefCell::new(ctx))
    };

    context
      .borrow_mut()
      .connect(None, FlagSet::NOFLAGS, None)
      .map_err(|err| PulseAudioError::PAErr(err))?;

    Ok(InnerClient {
      state_tx,
      function_rx,
      main_loop,
      context,
      cancellation,
      state: Rc::default(),
    })
  }

  fn find_device<T: AsRef<str>>(
    &self,
    id: Option<T>,
  ) -> Option<Ref<PulseAudioDevice>> {
    id.as_ref()
      .map(|id| id.as_ref())
      .and_then(|key |Ref::filter_map(self.state.borrow(), |state| state.devices.get(key)).ok())
  }

  pub fn run(&mut self) -> anyhow::Result<()> {
    // Wait for pulse to initially become ready
    loop {
      match self.main_loop.borrow_mut().iterate(false) {
        IterateResult::Quit(val) => Err(PulseAudioError::Quit(val))?,
        IterateResult::Err(err) => Err(PulseAudioError::PAErr(err))?,
        IterateResult::Success(_) => {}
      };
      match self.context.borrow().get_state() {
        PAState::Ready => {
          break;
        }
        PAState::Failed | PAState::Terminated => {
          Err(PulseAudioError::ContextTerminated)?
        }
        _ => {}
      }
    }
    Self::init_state(&self.context, &self.state, &self.state_tx);
    self
      .context
      .borrow_mut()
      .set_subscribe_callback(Some(Box::new({
        let context = self.context.clone();
        let state = self.state.clone();
        let state_tx = self.state_tx.clone();
        move |facility, op, i| {
          Self::update_state(&context, &state, &state_tx, facility, op, i)
        }
      })));
    self
      .context
      .borrow_mut()
      .subscribe(InterestMaskSet::SERVER | InterestMaskSet::SINK, |_| ());

    loop {
      if self.cancellation.is_cancelled() {
        self.quit();
        break Ok(());
      }

      self.main_loop.borrow_mut().iterate(false);

      match self.function_rx.try_recv() {
        Ok(AudioFunctionWithReply { f, sender }) => {
          let mut introspector = self.context.borrow_mut().introspect();
          match f {
            AudioFunction::SetVolume(SetVolumeArgs {
              volume,
              device_id,
            }) => {
              let target = self.find_device(device_id);
              match target {
                Some(device) => {
                  let mut levels = device.levels.clone();
                  levels.set_percent(volume as f64);
                  let channels = levels.into();
                  match device.device_type {
                    DeviceType::Playback => {
                      introspector.set_sink_volume_by_name(
                        &device.id, &channels, None,
                      );
                      if sender
                        .send(Ok(ProviderFunctionResponse::Null))
                        .is_err()
                      {
                        warn!("Failed to send function response as the receiver has closed");
                      }
                    }
                    DeviceType::Recording => {
                      introspector.set_source_volume_by_name(
                        &device.id, &channels, None,
                      );
                      if sender
                        .send(Ok(ProviderFunctionResponse::Null))
                        .is_err()
                      {
                        warn!("Failed to send function response as the receiver has closed");
                      }
                    }
                  }
                }
                None => {
                  if sender
                    .send(Err(
                      "Could not set volume: no device to adjust"
                        .to_string(),
                    ))
                    .is_err()
                  {
                    warn!(
                                    "Failed to send pulseaudio state as the receiver has closed"
                                    );
                  };
                }
              };
            }
            AudioFunction::SetMute(SetMuteArgs { mute, device_id }) => {
              let target = self.find_device(device_id);
              match target {
                Some(device) => match device.device_type {
                  DeviceType::Playback => {
                    introspector
                      .set_sink_mute_by_name(&device.id, mute, None);
                    if sender
                      .send(Ok(ProviderFunctionResponse::Null))
                      .is_err()
                    {
                      warn!("Failed to send function response as the receiver has closed");
                    }
                  }
                  DeviceType::Recording => {
                    introspector
                      .set_source_mute_by_name(&device.id, mute, None);
                    if sender
                      .send(Ok(ProviderFunctionResponse::Null))
                      .is_err()
                    {
                      warn!("Failed to send function response as the receiver has closed");
                    }
                  }
                },
                None => {
                  if sender
                    .send(Err(
                      "Could not set mute: no device to adjust"
                        .to_string(),
                    ))
                    .is_err()
                  {
                    warn!(
                                    "Failed to send pulseaudio state as the receiver has closed"
                                    );
                  };
                }
              };
            }
          };
        }
        Err(TryRecvError::Disconnected) => {
          self.quit();
          break Err(TryRecvError::Disconnected)?;
        }
        Err(TryRecvError::Empty) => {}
      };
    }
  }

  fn init_state(
    context: &Rc<RefCell<Context>>,
    state: &Rc<RefCell<PulseAudioState>>,
    state_tx: &UnboundedSender<anyhow::Result<AudioOutput>>,
  ) {
    let introspect = context.borrow().introspect();
    state.borrow_mut().devices.clear();
    enum DoneKind {
      Defaults,
      Sources,
      Sinks,
    }

    let on_done = Rc::new(RefCell::new({
      let _state = state.clone();
      let tx = state_tx.clone();
      let mut sources = false;
      let mut sinks = false;
      let mut defaults = false;
      move |kind: DoneKind| {
        match kind {
          DoneKind::Defaults => {
            assert!(!defaults);
            *&mut defaults = true;
          }
          DoneKind::Sources => {
            assert!(!*&sources);
            *&mut sources = true;
          }
          DoneKind::Sinks => {
            assert!(!*&sinks);
            *&mut sinks = true;
          }
        }
        if sources && sinks && defaults {
          Self::emit_output(_state.borrow(), &tx);
        }
      }
    }));
    introspect.get_server_info({
      // note: this kind of non-rc ref may not work at runtime we're going
      // to roll the dice and learn!
      let _state = state.clone();
      let _on_done = on_done.clone();
      move |info| {
        {
          let state = &mut _state.borrow_mut();
          state.default_source =
            info.default_source_name.as_ref().map(ToString::to_string);
          state.default_sink =
            info.default_sink_name.as_ref().map(ToString::to_string);
        }
        _on_done.borrow_mut()(DoneKind::Defaults);
      }
    });

    introspect.get_source_info_list({
      let _state = state.clone();
      let tx = state_tx.clone();
      let _on_done = on_done.clone();
      move |info| match info {
        ListResult::Item(source) => {
          let id = source
            .name
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
          let mut state = _state.borrow_mut();
          state.devices.insert(
            id.clone(),
            PulseAudioDevice {
              id,
              description: source
                .description
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
              levels: source.volume.into(),
              device_type: DeviceType::Recording,
              is_muted: source.mute,
            },
          );
        }
        ListResult::End => {
          _on_done.borrow_mut()(DoneKind::Sources);
        }
        ListResult::Error => {
          if tx
            .send(Err(PulseAudioError::SourceListChange.into()))
            .is_err()
          {
            warn!(
              "Failed to send PulseAudio state as the receiver has closed"
            );
          };
        }
      }
    });

    introspect.get_sink_info_list({
      let _state = state.clone();
      let tx = state_tx.clone();
      let _on_done = on_done.clone();
      move |info| match info {
        ListResult::Item(sink) => {
          let id = sink
            .name
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
          let mut state = _state.borrow_mut();
          state.devices.insert(
            id.clone(),
            PulseAudioDevice {
              id,
              description: sink
                .description
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
              levels: sink.volume.into(),
              device_type: DeviceType::Playback,
              is_muted: sink.mute,
            },
          );
        }
        ListResult::End => {
          _on_done.borrow_mut()(DoneKind::Sinks);
        }
        ListResult::Error => {
          if tx
            .send(Err(PulseAudioError::SinkListChange.into()))
            .is_err()
          {
            warn!(
              "Failed to send PulseAudio state as the receiver has closed"
            );
          };
        }
      }
    });
  }

  fn update_state(
    context: &Rc<RefCell<Context>>,
    state: &Rc<RefCell<PulseAudioState>>,
    state_tx: &UnboundedSender<anyhow::Result<AudioOutput>>,
    facility: Option<Facility>,
    operation: Option<Operation>,
    index: u32,
  ) {
    // todo: optimise to do incremental changes instead of blanket
    // rewriting
    Self::init_state(context, state, state_tx);
  }

  fn emit_output(
    state: Ref<PulseAudioState>,
    state_tx: &UnboundedSender<anyhow::Result<AudioOutput>>,
  ) {
    if state_tx.send(Ok(From::from(&*state))).is_err() {
      warn!("Failed to send PulseAudio state as the receiver has closed");
    };
  }

  fn quit(&mut self) {
    self.main_loop.borrow_mut().quit(Retval(0));
  }
}

/// Note: apparently libpulse_binding is not actually thread safe, so we
/// should ensure there is actually only ever one client per zebar backend
/// instance (although that should be how zebar operates *anyway*.
impl PulseAudioClient {
  pub fn new(
    state_tx: UnboundedSender<anyhow::Result<AudioOutput>>,
    function_rx: UnboundedReceiver<AudioFunctionWithReply>,
  ) -> PulseAudioClient {
    let mut cancellation = CancellationToken::new();
    let child_token = cancellation.child_token();

    spawn_blocking(move || {
      let result: anyhow::Result<()> =
        InnerClient::new(state_tx.clone(), function_rx, child_token)
          .map_err(|err| err.into())
          .and_then(|mut client| client.run());
      if let Some(err) = result.err() {
        if state_tx.send(Err(err.into())).is_err() {
          warn!(
            "Failed to send pulseaudio state as the receiver has closed"
          );
        };
      }
    });

    PulseAudioClient(cancellation)
  }
}

impl Drop for PulseAudioClient {
  fn drop(&mut self) {
    self.0.cancel();
  }
}
impl Drop for InnerClient {
  fn drop(&mut self) {
    self.cancellation.cancel();
    self.quit();
  }
}
