use std::collections::HashMap;

use crate::providers::audio::{
  common::{AudioDevice, AudioOutput, DeviceType},
  pulseaudio::volume_levels::VolumeLevels,
};

/// Note: only includes fields we are currently using
pub struct PulseAudioDevice {
  pub description: String,
  pub id: String,
  pub device_type: DeviceType,
  pub levels: VolumeLevels,
  pub is_muted: bool,
}

#[derive(Default)]
pub struct PulseAudioState {
  pub devices: HashMap<String, PulseAudioDevice>,
  pub default_sink: Option<String>,
  pub default_source: Option<String>,
}

impl From<&PulseAudioState> for AudioOutput {
  fn from(state: &PulseAudioState) -> Self {
    let mut output = AudioOutput::default();
    for device in state.devices.values() {
      let output_device = AudioDevice {
        name: device.description.clone(),
        device_id: device.id.clone(),
        device_type: device.device_type.clone(),
        volume: device.levels.percent() as u32,
        is_muted: device.is_muted,
        is_default_recording: state
          .default_source
          .as_deref()
          .is_some_and(|id| id == device.id),
        is_default_playback: state
          .default_sink
          .as_deref()
          .is_some_and(|id| id == device.id),
      };
      if device.device_type == DeviceType::Playback {
        output.playback_devices.push(output_device.clone());
        if output_device.is_default_playback {
          output.default_playback_device = Some(output_device.clone());
        }
      } else {
        output.recording_devices.push(output_device.clone());
        if output_device.is_default_recording {
          output.default_recording_device = Some(output_device.clone());
        }
      }
      output.all_devices.push(output_device);
    }
    output
  }
}
