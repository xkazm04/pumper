//! Example Pumper trigger TRANSFORM plugin (M15 "WASM everywhere" v1).
//!
//! Attached to a trigger as `plugins.transform`, it shapes the `_trigger`
//! object before it is merged into the target job's params. Same host ABI as
//! extraction plugins (`alloc` / `extract_v2` / `describe`); the envelope's
//! `doc` is the `_trigger` object as a JSON string.
//!
//! Contract: the output must be a JSON OBJECT — it becomes the new `_trigger`
//! payload. The host re-stamps its own keys afterwards, so this plugin cannot
//! forge or lose them. Non-object output or a trap keeps the original envelope
//! (fail-open, loud log on the host).
//!
//! Logic: keep only the keys listed in `params.keep`, plus a `slimmed: true`
//! marker — a payload diet for targets that only need a summary. With
//! `params.keep` absent the delta passes through unchanged.
//!
//! ## What this plugin may NOT shrink
//!
//! Two classes of key are host-owned and come back however this plugin shapes
//! them (`HOST_OWNED_KEYS` in `crates/server/src/triggers.rs`):
//!
//! - **Lineage** — `trigger_id`, `source_kind`, `source_job_id`, `event_id`,
//!   `source_id`, `depth`, `chain`.
//! - **Work scope** — `keys`, `keys_truncated`.
//!
//! The second class is why this plugin lost its `max_keys` knob. `_trigger` IS
//! the target job's params, and `crates/apps/extractor` and
//! `crates/apps/plugin` read `_trigger.keys` as the list of records to process.
//! `max_keys` (default 10, against a host `key_cap` of 200) therefore did not
//! slim a payload — it turned a 200-key hop into a 10-record extract. Worse,
//! listing `keys` out of `params.keep` deleted it, and an absent `keys` is not
//! "no keys": it sends the extractor down its "every live record, up to 10,000"
//! path. Shrinking what a WEBHOOK carries is legitimate; shrinking what a JOB
//! does is not, and the throttle for that is `[triggers] key_cap` on the host.

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
    let mut out = shape(&delta, envelope.pointer("/params/keep"));
    out.insert("slimmed".into(), json!(true));
    emit(Value::Object(out).to_string())
}

/// Keeps only the keys named in `keep`; with no `keep` list, the delta passes
/// through untouched.
///
/// Extracted so the one thing this plugin decides is testable on the host — a
/// wasm entry point that reads raw pointers is not.
fn shape(delta: &Map<String, Value>, keep: Option<&Value>) -> Map<String, Value> {
    let Some(keep) = keep.and_then(Value::as_array) else {
        return delta.clone();
    };
    let mut out = Map::new();
    for key in keep.iter().filter_map(Value::as_str) {
        if let Some(v) = delta.get(key) {
            out.insert(key.to_string(), v.clone());
        }
    }
    out
}

/// Self-describing manifest for `GET /plugins?kind=transform`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.2.0",
            "kind": "transform",
            "description": "Trigger transform: keep only params.keep keys of the _trigger delta (absent keep = pass through). Host-owned keys come back regardless: lineage, and the target's work scope (`keys`, `keys_truncated`).",
            "params_schema": {
                "keep": "string[]? — exact keys to keep. Lineage and work-scope keys are re-added by the host whether or not you list them; to bound a hop's key list use [triggers] key_cap, which is the host's knob.",
            },
            "output_schema": { "slimmed": "bool", "...": "the shaped delta" },
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta() -> Map<String, Value> {
        let Value::Object(m) = json!({
            "trigger_id": "T1",
            "source_kind": "dataset",
            "dataset": "grants",
            "count": 3,
            "keys": ["k1", "k2", "k3"],
            "keys_truncated": false,
            "depth": 1,
            "chain": ["T1"],
        }) else {
            unreachable!()
        };
        m
    }

    /// The payload diet this plugin exists for still works.
    #[test]
    fn keep_narrows_the_payload_to_the_named_keys() {
        let out = shape(&delta(), Some(&json!(["dataset", "count"])));
        assert_eq!(out.len(), 2);
        assert_eq!(out["dataset"], "grants");
        assert_eq!(out["count"], 3);
        // A name that is not in the delta is simply not invented.
        let out = shape(&delta(), Some(&json!(["dataset", "nonesuch"])));
        assert_eq!(out.len(), 1);
    }

    /// The anti-pattern the removed `max_keys` embodied: `keys` is a WORK LIST
    /// for extractor/plugin targets, not a sample. Nothing in this plugin may
    /// shorten it — and with no `keep` list the delta is handed on exactly as
    /// the host built it rather than quietly capped at 10.
    #[test]
    fn an_absent_keep_list_shortens_nothing() {
        let d = delta();
        assert_eq!(shape(&d, None), d);
        assert_eq!(shape(&d, Some(&json!("not-an-array"))), d);
        assert_eq!(shape(&d, None)["keys"], json!(["k1", "k2", "k3"]));
    }
}
