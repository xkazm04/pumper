//! Example Pumper trigger PREDICATE plugin (M15 "WASM everywhere" v1).
//!
//! Attached to a trigger as `plugins.predicate`, it decides fire/skip over the
//! `_trigger` delta envelope. Host ABI (same as extraction plugins):
//!   alloc(len) -> ptr             reserve `len` bytes in linear memory
//!   extract_v2(ptr, len) -> u64   input is a `{"doc": .., "params": ..}`
//!                                 envelope; `doc` is the `_trigger` object as
//!                                 a JSON string
//!   describe() -> u64             self-describing manifest (`kind: predicate`)
//! Output is packed `(out_ptr << 32) | out_len` pointing at UTF-8 JSON.
//!
//! Contract: the output must be `{"pass": bool}`. Anything else (or a trap)
//! takes the trigger's fail-open path on the host side.
//!
//! Logic: pass when the delta's `count` >= `params.min_count` (default 1) and,
//! when `params.dataset` is set, the delta's `dataset` equals it. So one module
//! serves "only fan out on big batches" and "only this dataset" edges without
//! recompiling.

use serde_json::{json, Value};

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
    // `doc` is the `_trigger` object serialized as a string.
    let delta: Value = envelope
        .get("doc")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    let min_count = envelope
        .pointer("/params/min_count")
        .and_then(Value::as_u64)
        .unwrap_or(1);
    let count = delta.get("count").and_then(Value::as_u64).unwrap_or(0);
    let dataset_ok = match envelope.pointer("/params/dataset").and_then(Value::as_str) {
        Some(want) => delta.get("dataset").and_then(Value::as_str) == Some(want),
        None => true,
    };
    emit(json!({ "pass": count >= min_count && dataset_ok }).to_string())
}

/// Self-describing manifest for `GET /plugins?kind=predicate`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.1.0",
            "kind": "predicate",
            "description": "Trigger predicate: pass when the delta's count >= params.min_count (default 1) and, if params.dataset is set, the dataset matches.",
            "params_schema": {
                "min_count": "number? — minimum revision count to fire (default 1)",
                "dataset": "string? — only fire for this exact dataset",
            },
            "output_schema": { "pass": "bool" },
        })
        .to_string(),
    )
}
