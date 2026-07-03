//! Example Pumper WASM plugin. Implements the host ABI:
//!   alloc(len) -> ptr        reserve `len` bytes in linear memory
//!   extract(ptr, len) -> u64 read the input doc, return (out_ptr<<32 | out_len)
//! Output bytes are UTF-8 JSON. This one pulls <title> and <h1> from HTML.

use serde_json::json;

/// Reserve `len` bytes and hand the host a pointer to write the input into.
#[no_mangle]
pub extern "C" fn alloc(len: u32) -> u32 {
    let mut buf: Vec<u8> = Vec::with_capacity(len as usize);
    let ptr = buf.as_mut_ptr() as u32;
    std::mem::forget(buf); // freed when the whole store is torn down after the call
    ptr
}

#[no_mangle]
pub extern "C" fn extract(ptr: u32, len: u32) -> u64 {
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let doc = std::str::from_utf8(input).unwrap_or("");

    let out = json!({
        "plugin": "title-extractor",
        "title": between(doc, "<title>", "</title>"),
        "h1": between(doc, "<h1>", "</h1>"),
        "input_bytes": len,
    })
    .to_string();

    let bytes = out.into_bytes();
    let out_ptr = bytes.as_ptr() as u32;
    let out_len = bytes.len() as u32;
    std::mem::forget(bytes);
    ((out_ptr as u64) << 32) | out_len as u64
}

fn between<'a>(s: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = s.find(open)? + open.len();
    let end = s[start..].find(close)? + start;
    Some(s[start..end].trim())
}
