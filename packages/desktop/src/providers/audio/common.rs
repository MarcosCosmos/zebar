use serde::{Deserialize, Serialize};
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AudioProviderConfig {}

#[derive(Default, Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioOutput {
  pub playback_devices: Vec<AudioDevice>,
  pub recording_devices: Vec<AudioDevice>,
  pub all_devices: Vec<AudioDevice>,
  pub default_playback_device: Option<AudioDevice>,
  pub default_recording_device: Option<AudioDevice>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioDevice {
  pub name: String,
  pub device_id: String,
  pub device_type: DeviceType,
  pub volume: u32,
  pub is_default_playback: bool,
  pub is_default_recording: bool,
  pub is_muted: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DeviceType {
  Playback,
  Recording,
}
