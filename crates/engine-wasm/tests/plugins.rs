//! End-to-end tests against the real reference plugin (`data/plugins/title.wasm`,
//! built from `plugins-src/title-extractor`). Exercises the params envelope
//! (`extract_v2`) and the `describe` manifest — the wasm#3 surface.

use std::path::PathBuf;

use pumper_core::config::PluginConfig;
use pumper_core::Plugins;
use pumper_engine_wasm::WasmPluginHost;
use serde_json::json;

fn host() -> WasmPluginHost {
    // data/plugins lives at the repo root, two levels above this crate.
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/plugins");
    WasmPluginHost::new(&PluginConfig { dir, ..Default::default() }).expect("host")
}

/// The built `title.wasm` is a local runtime artifact (`data/` is gitignored;
/// `plugins-src/title-extractor` is the tracked source). Skip when it isn't
/// present — a fresh checkout hasn't run the wasm build — so CI stays green while
/// the test still verifies the real module locally after `cargo build
/// --target wasm32-unknown-unknown`.
fn title_present(host: &WasmPluginHost) -> bool {
    let present = host.list().iter().any(|n| n == "title");
    if !present {
        eprintln!("skipping: data/plugins/title.wasm not built (see plugins-src/title-extractor)");
    }
    present
}

#[tokio::test]
async fn extract_v2_envelope_forwards_params() {
    let host = host();
    if !title_present(&host) {
        return;
    }
    // params.tag = "h2" makes the reference plugin extract the <h2> into `value`
    // — proving the params envelope reaches the plugin via extract_v2.
    let out = host
        .run(
            "title",
            "<title>Home</title><h1>Big</h1><h2>Sub</h2>",
            &json!({ "tag": "h2" }),
        )
        .await
        .expect("run");
    assert_eq!(out["title"], json!("Home"));
    assert_eq!(out["h1"], json!("Big"));
    assert_eq!(out["value"], json!("Sub"), "params.tag drove the extra field");
    assert_eq!(out["tag"], json!("h2"));
}

#[tokio::test]
async fn describe_manifest_surfaces_in_metadata() {
    let host = host();
    if !title_present(&host) {
        return;
    }
    let manifests = host.manifests();
    let title = manifests
        .iter()
        .find(|m| m["name"] == json!("title"))
        .expect("title plugin present");
    // The name is authoritative (from the file stem); the rest comes from describe().
    assert_eq!(title["version"], json!("0.2.0"));
    assert!(title["description"].as_str().is_some());
    assert!(title["params_schema"].is_object());
}
