//! Example Pumper WASM plugin. Implements the host ABI:
//!   alloc(len) -> ptr             reserve `len` bytes in linear memory
//!   extract(ptr, len) -> u64      legacy: input is the raw document
//!   extract_v2(ptr, len) -> u64   input is a `{"doc": .., "params": ..}` envelope
//!   describe() -> u64             optional self-describing manifest
//! Every output is a packed `(out_ptr << 32) | out_len` pointing at UTF-8 JSON.
//!
//! This one pulls `<title>` and `<h1>`, and — via `extract_v2` params — an
//! arbitrary extra tag: `{"params": {"tag": "h2"}}` adds `"value": <h2 text>`.

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

fn extract_fields(doc: &str) -> Value {
    json!({
        "plugin": "title-extractor",
        "title": between(doc, "<title>", "</title>"),
        "h1": between(doc, "<h1>", "</h1>"),
        "input_bytes": doc.len(),
    })
}

/// Legacy ABI: the input bytes are the raw document.
#[no_mangle]
pub extern "C" fn extract(ptr: u32, len: u32) -> u64 {
    emit(extract_fields(read_input(ptr, len)).to_string())
}

/// Params-aware ABI: the input is a `{"doc", "params"}` envelope. `params.tag`
/// (optional) adds a `"value"` field extracted from `<tag>…</tag>`, so one
/// module can be reused per job with a different selector — no recompile.
#[no_mangle]
pub extern "C" fn extract_v2(ptr: u32, len: u32) -> u64 {
    let envelope: Value = serde_json::from_str(read_input(ptr, len)).unwrap_or(Value::Null);
    let doc = envelope.get("doc").and_then(Value::as_str).unwrap_or("");
    let mut out = extract_fields(doc);
    if let Some(tag) = envelope.pointer("/params/tag").and_then(Value::as_str) {
        let value = between(doc, &format!("<{tag}>"), &format!("</{tag}>"));
        if let Value::Object(map) = &mut out {
            map.insert("value".into(), json!(value));
            map.insert("tag".into(), json!(tag));
        }
    }
    emit(out.to_string())
}

/// Self-describing manifest for `GET /plugins`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.2.0",
            "description": "Extracts <title>/<h1>; params.tag adds an arbitrary tag's text as `value`.",
            "params_schema": { "tag": "string? — extra HTML tag to extract into `value`" },
            "output_schema": { "title": "string?", "h1": "string?", "value": "string?", "tag": "string?" },
        })
        .to_string(),
    )
}

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = s.find(open)? + open.len();
    let end = s[start..].find(close)? + start;
    Some(s[start..end].trim())
}
