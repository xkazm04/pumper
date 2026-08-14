//! End-to-end tests against the real reference plugin (`data/plugins/title.wasm`,
//! built from `plugins-src/title-extractor`). Exercises the params envelope
//! (`extract_v2`) and the `describe` manifest — the wasm#3 surface.
//!
//! Two layers, because they catch different breaks:
//!
//! 1. [`every_shipped_plugin_still_exports_the_host_abi`] reads the *sources*
//!    and runs everywhere, with no wasm toolchain and no build step. It is the
//!    guard against the failure this file could not see: a plugin that drops an
//!    ABI export still compiles, so only something that checks the export list
//!    turns red.
//! 2. The `#[ignore]`d tests load the *built artifacts* — the only way to prove
//!    a real module executes. CI installs them (`just plugins-install`) and runs
//!    exactly these with `--ignored`; locally that is `just plugins-verify`.

use std::collections::BTreeSet;
use std::path::PathBuf;

use pumper_core::config::PluginConfig;
use pumper_core::Plugins;
use pumper_engine_wasm::WasmPluginHost;
use serde_json::json;

/// Run `just plugins-install` first — named once so every artifact test says
/// the same thing.
const NEEDS_INSTALL: &str = "run `just plugins-install` (or `just plugins-verify`)";

fn host() -> WasmPluginHost {
    // data/plugins lives at the repo root, two levels above this crate.
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/plugins");
    WasmPluginHost::new(&PluginConfig {
        dir,
        ..Default::default()
    })
    .expect("host")
}

/// The built `title.wasm` is a local runtime artifact (`data/` is gitignored;
/// `plugins-src/title-extractor` is the tracked source). The tests are
/// `#[ignore]`d so a default run never depends on it; when run explicitly
/// (`just plugins-verify`, or CI's artifact step) a missing artifact is a loud
/// failure, not a silent green.
fn title_present(host: &WasmPluginHost) -> bool {
    let present = host.list().iter().any(|n| n == "title");
    if !present {
        eprintln!("skipping: data/plugins/title.wasm not built — {NEEDS_INSTALL}");
    }
    present
}

// ── the host ABI, checked at the source ──────────────────────────────────────

/// The repo root, three levels up from `crates/engine-wasm/tests/`.
fn plugins_src() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../plugins-src")
}

/// EXPECTED: the `#[no_mangle] pub extern "C"` exports of every tracked plugin
/// source. This is the [EXPECTED-diff idiom][idiom] — a new `plugins-src` crate
/// fails here until it is listed, so "which modules must keep the ABI" is a
/// fact the test suite owns rather than a convention in a doc.
///
/// `busyloop` is the one deliberate outlier: it is the fuel-trap demo, exports
/// only the legacy `extract`, and is not a hook plugin.
///
/// [idiom]: crates/server/src/routes/mod.rs
const EXPECTED_PLUGIN_ABI: &[(&str, &[&str])] = &[
    ("busyloop", &["alloc", "extract"]),
    ("delta-slim", &["alloc", "describe", "extract_v2"]),
    (
        "title-extractor",
        &["alloc", "describe", "extract", "extract_v2"],
    ),
    ("trigger-gate", &["alloc", "describe", "extract_v2"]),
];

/// The `#[no_mangle] pub extern "C" fn <name>` exports declared in one source
/// file, in sorted order. Deliberately textual: it must answer for a crate that
/// this host cannot link (wasm32 cdylib), on a machine with no wasm target.
fn declared_exports(src: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut lines = src.lines().map(str::trim).peekable();
    while let Some(line) = lines.next() {
        if line != "#[no_mangle]" {
            continue;
        }
        // Skip further attributes between `#[no_mangle]` and the signature.
        while lines.peek().is_some_and(|l| l.starts_with("#[")) {
            lines.next();
        }
        let Some(sig) = lines.peek() else { continue };
        if let Some(rest) = sig.strip_prefix("pub extern \"C\" fn ") {
            if let Some(name) = rest.split('(').next() {
                out.insert(name.trim().to_string());
            }
        }
    }
    out
}

/// The anti-pattern: a hook plugin that stops exporting `extract_v2` still
/// COMPILES. The host then reports it as not-executable, every hook pointed at
/// it takes the fail-open path, and the pipeline edge is silently ungated —
/// production behaviour indistinguishable from a gate that said yes. Nothing in
/// this repo turned red for that, because nothing built or inspected these
/// crates at all.
///
/// This runs in the ordinary `cargo test --workspace`: no wasm target, no
/// artifacts, no build step.
#[test]
fn every_shipped_plugin_still_exports_the_host_abi() {
    let dir = plugins_src();
    let mut found: Vec<(String, BTreeSet<String>)> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| {
            let crate_name = e.file_name().to_string_lossy().into_owned();
            let lib = e.path().join("src/lib.rs");
            let src = std::fs::read_to_string(&lib)
                .unwrap_or_else(|err| panic!("read {}: {err}", lib.display()));
            (crate_name, declared_exports(&src))
        })
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));

    let expected: Vec<(String, BTreeSet<String>)> = EXPECTED_PLUGIN_ABI
        .iter()
        .map(|(name, abi)| {
            (
                (*name).to_string(),
                abi.iter().map(|s| (*s).to_string()).collect(),
            )
        })
        .collect();

    assert_eq!(
        found.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        expected.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        "plugins-src crates changed — add the new one to EXPECTED_PLUGIN_ABI (and \
         it is already built + installed by `just plugins-install`, which globs)"
    );
    for ((name, got), (_, want)) in found.iter().zip(expected.iter()) {
        assert_eq!(
            got, want,
            "plugins-src/{name} no longer exports the ABI it is contracted to: \
             a hook plugin missing `extract_v2` loads, answers `has() == false`, \
             and silently fails its hop open"
        );
    }
}

#[tokio::test]
#[ignore = "requires the built data/plugins/title.wasm — `just plugins-verify` (CI runs this step)"]
async fn extract_v2_envelope_forwards_params() {
    let host = host();
    assert!(
        title_present(&host),
        "data/plugins/title.wasm missing — {NEEDS_INSTALL}"
    );
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
    assert_eq!(
        out["value"],
        json!("Sub"),
        "params.tag drove the extra field"
    );
    assert_eq!(out["tag"], json!("h2"));
}

#[tokio::test]
#[ignore = "requires the built data/plugins/title.wasm — `just plugins-verify` (CI runs this step)"]
async fn describe_manifest_surfaces_in_metadata() {
    let host = host();
    assert!(
        title_present(&host),
        "data/plugins/title.wasm missing — {NEEDS_INSTALL}"
    );
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
