fn main() {
  tauri_build::try_build(tauri_build::Attributes::new().app_manifest(
    tauri_build::AppManifest::new().commands(&[
      "listen_provider",
      "unlisten_provider",
      "call_provider_function",
    ]),
  ))
  .unwrap()
}
