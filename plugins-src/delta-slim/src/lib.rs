//! Example Pumper trigger TRANSFORM plugin (M15 "WASM everywhere" v1).
//!
//! Attached to a trigger as `plugins.transform`, it shapes the `_trigger`
//! object before it is merged into the target job's params. Same host ABI as
//! extraction plugins (`alloc` / `extract_v2` / `describe`); the envelope's
//! `doc` is the `_trigger` object as a JSON string.
//!
//! Contract: the output must be a JSON OBJECT — it becomes the new `_trigger`
//! payload. The host re-stamps provenance keys (trigger_id, source_kind,
//! source_job_id, event_id, source_id, depth, chain) afterwards, so this
//! plugin cannot forge or lose lineage. Non-object output or a trap keeps the
//! original envelope (fail-open, loud log on the host).
//!
//! Logic: keep only the keys listed in `params.keep` (plus a `slimmed: true`
//! marker), or with `params.keep` absent, cap `keys` to `params.max_keys`
//! (default 10) and drop nothing else — a payload-diet knob for targets that
//! only need a summary.

use serde_json::{json, Map, Value};

/// Reserve `len` bytes and hand the host a pointer to write the input into.
#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr() as u32;
    std::mem::forget(buf); // freed when the whole store is torn down after the call
    ptr
}

/// Packs an output JSON string into the `(ptr << 32) | len` return convention.
fn emit(out: String) -> u64 {
    let bytes = out.into_bytes();
    let out_ptr = bytes.as_ptr() as u32;
    let out_len = bytes.len() as u32;
    std::mem::forget(bytes);
    ((out_ptr as u64) << 32) | out_len as u64
}

fn read_input<'a>(ptr: u32, len: u32) -> &'a str {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    std::str::from_utf8(input).unwrap_or("")
}

#[no_mangle]
pub extern "C" fn extract_v2(ptr: u32, len: u32) -> u64 {
    let envelope: Value = serde_json::from_str(read_input(ptr, len)).unwrap_or(Value::Null);
    let delta: Value = envelope
        .get("doc")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    let Value::Object(delta) = delta else {
        // Can't shape what we can't parse — return an empty object; the host
        // restamps provenance so nothing lineage-bearing is lost.
        return emit(json!({ "slimmed": true }).to_string());
    };
    let mut out: Map<String, Value> = match envelope.pointer("/params/keep").and_then(Value::as_array) {
        Some(keep) => {
            let mut m = Map::new();
            for key in keep.iter().filter_map(Value::as_str) {
                if let Some(v) = delta.get(key) {
                    m.insert(key.to_string(), v.clone());
                }
            }
            m
        }
        None => {
            let max_keys = envelope
                .pointer("/params/max_keys")
                .and_then(Value::as_u64)
                .unwrap_or(10) as usize;
            let mut m = delta.clone();
            if let Some(Value::Array(keys)) = m.get_mut("keys") {
                keys.truncate(max_keys);
            }
            m
        }
    };
    out.insert("slimmed".into(), json!(true));
    emit(Value::Object(out).to_string())
}

/// Self-describing manifest for `GET /plugins?kind=transform`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.1.0",
            "kind": "transform",
            "description": "Trigger transform: keep only params.keep keys of the _trigger delta, or cap `keys` to params.max_keys (default 10). Provenance is host-restamped.",
            "params_schema": {
                "keep": "string[]? — exact keys to keep (provenance is re-added by the host)",
                "max_keys": "number? — cap on the `keys` array when `keep` is absent (default 10)",
            },
            "output_schema": { "slimmed": "bool", "...": "the shaped delta" },
        })
        .to_string(),
    )
}
