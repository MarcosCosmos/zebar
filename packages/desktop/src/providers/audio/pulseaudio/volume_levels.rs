use std::{
  collections::HashMap,
  ops::{Deref, DerefMut},
};

use libpulse_binding::volume::{ChannelVolumes, Volume};

use crate::providers::audio::common::AudioOutput;

/// sourced the volume level logic from ironbar: https://github.com/JakeStanger/ironbar/blob/5b96bcffac54dd82347badcc07f79d58efa715c7/src/clients/volume/mod.rs#L520
#[derive(Debug, Clone)]
pub struct VolumeLevels(Vec<u32>);

#[derive(Debug, Default)]
pub struct OutputWithLevels {
  pub inner: AudioOutput,
  pub levels: HashMap<String, VolumeLevels>,
}

impl VolumeLevels {
  pub fn percent(&self) -> f64 {
    let avg: u32 = self.iter().sum::<u32>() / self.len() as u32;
    let base_delta = (Volume::NORMAL.0 - Volume::MUTED.0) as f64 / 100.0;

    ((avg - Volume::MUTED.0) as f64 / base_delta).round()
  }

  pub fn set_percent(&mut self, percent: f64) {
    let volume = percent_to_volume(percent);
    self.fill(volume);
  }
}

impl Deref for VolumeLevels {
  type Target = Vec<u32>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl DerefMut for VolumeLevels {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl Into<ChannelVolumes> for VolumeLevels {
  fn into(self) -> ChannelVolumes {
    let mut cv = ChannelVolumes::default();
    cv.set_len(self.len() as u8);
    cv.get_mut().copy_from_slice(unsafe {
      std::mem::transmute::<&[u32], &[Volume]>(&self)
    });
    cv
  }
}

impl From<ChannelVolumes> for VolumeLevels {
  fn from(value: ChannelVolumes) -> Self {
    let levels: &[u32] = unsafe {
      &*(std::ptr::from_ref::<[Volume]>(value.get()) as *const [u32])
    };
    Self(Vec::from(&levels[..value.len() as usize]))
  }
}

/// Converts a percentage volume into a Pulse volume value,
/// which can be used for setting channel volumes.
pub fn percent_to_volume(target_percent: f64) -> u32 {
  let base_delta =
    (Volume::NORMAL.0 as f32 - Volume::MUTED.0 as f32) / 100.0;

  if target_percent < 0.0 {
    Volume::MUTED.0
  } else if target_percent == 100.0 {
    Volume::NORMAL.0
  } else if target_percent >= 150.0 {
    (Volume::NORMAL.0 as f32 * 1.5) as u32
  } else if target_percent < 100.0 {
    Volume::MUTED.0 + target_percent as u32 * base_delta as u32
  } else {
    Volume::NORMAL.0 + (target_percent - 100.0) as u32 * base_delta as u32
  }
}
