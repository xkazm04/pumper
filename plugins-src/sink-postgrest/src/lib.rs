//! Reference Pumper **sink connector** (N10 "WASM sinks & connectors").
//!
//! Attached to a dataset watch as `sink: "plugin:sink-postgrest"`, it POSTs each
//! `dataset.changed` delivery as a row into a [PostgREST](https://postgrest.org)
//! table — reverse-ETL to Postgres without a line of Rust in the server.
//!
//! It exists to demonstrate the whole capability path end to end, and it is the
//! smallest thing that does:
//!
//! 1. `describe()` declares `capabilities.http` — WHICH hosts, WHICH methods.
//!    The host builds this module's linker from that block, so the import below
//!    resolves only because the manifest asked for it. Delete the
//!    `capabilities` key and this module stops loading, with
//!    `executable: false` and a `capability_error` on `GET /plugins`.
//! 2. The operator still has to say yes: `[plugins] allow_http_hosts` is empty
//!    by default, and an empty list means no plugin reaches anything. Both
//!    lists must pass.
//! 3. `extract_v2` receives `{doc, params}` where `doc` is the delivery
//!    envelope (`{delivery_id, event, body}`) and `params.target` is the
//!    watch's url. It answers `{delivered, permanent?, error?}`, which the
//!    server maps onto the same retry/DLQ ladder every other sink uses.
//!
//! ## The host list is compile-time, and that is the point
//!
//! [`DECLARED_HOSTS`] ships as loopback only, because a manifest is a static
//! declaration: whoever installs the `.wasm` is trusting exactly the hosts it
//! names. Pointing this at a remote PostgREST means editing that const and
//! rebuilding — which is the audit trail, not a limitation to route around.
//!
//! ## Idempotency
//!
//! `delivery_id` is stable across in-process retries, the DLQ drain and manual
//! replay, so it is sent as the row's primary key with
//! `Prefer: resolution=merge-duplicates`. A replayed delivery updates its row
//! instead of creating a second one — the same contract the
//! `x-pumper-delivery-id` header gives an HTTP receiver.

use serde_json::{json, Value};

/// The hosts this connector's manifest declares. Loopback only, deliberately:
/// see the module docs.
const DECLARED_HOSTS: [&str; 2] = ["localhost", "127.0.0.1"];

// The granted host import. It links ONLY because `describe()` below declares
// `capabilities.http`; without that block the module does not load at all.
//
// In: a pointer/length pair to JSON `{method, url, headers, body}`.
// Out: `(ptr << 32) | len` of JSON `{status, body}` or `{error}`.
#[cfg(target_arch = "wasm32")]
#[link(wasm_import_module = "env")]
extern "C" {
    fn pumper_http_request(ptr: u32, len: u32) -> u64;
}

/// Host-target stand-in so `just plugins-test` can compile and run this crate's
/// unit tests off wasm. It is never reachable from a test — every test targets
/// the extracted pure functions, precisely because the pointer ABI above is not
/// meaningful on a 64-bit host — and its `0` is the ABI's "no bytes".
#[cfg(not(target_arch = "wasm32"))]
unsafe fn pumper_http_request(_ptr: u32, _len: u32) -> u64 {
    0
}

/// Reserve `len` bytes and hand the host a pointer to write into. Used for the
/// input envelope AND for the host's reply to a capability call.
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

/// Unpacks what the host returned from a capability call.
fn read_packed<'a>(packed: u64) -> &'a str {
    let ptr = (packed >> 32) as u32;
    let len = (packed & 0xffff_ffff) as u32;
    if len == 0 {
        return "";
    }
    read_input(ptr, len)
}

/// Performs one host HTTP call and parses the reply.
fn http_request(request: &Value) -> Value {
    let payload = request.to_string();
    let bytes = payload.as_bytes();
    let ptr = alloc(bytes.len() as u32);
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
    }
    let packed = unsafe { pumper_http_request(ptr, bytes.len() as u32) };
    serde_json::from_str(read_packed(packed)).unwrap_or_else(|_| json!({"error": "unreadable"}))
}

#[no_mangle]
pub extern "C" fn extract_v2(ptr: u32, len: u32) -> u64 {
    let envelope: Value = serde_json::from_str(read_input(ptr, len)).unwrap_or(Value::Null);
    let delivery: Value = envelope
        .get("doc")
        .and_then(Value::as_str)
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    let target = envelope
        .pointer("/params/target")
        .and_then(Value::as_str)
        .unwrap_or("");
    let Some(request) = build_request(&delivery, target) else {
        // No target and no delivery id are CONFIGURATION faults: they will look
        // identical on every retry, so say permanent and let the row go
        // straight to the DLQ instead of climbing the whole ladder.
        return emit(
            json!({
                "delivered": false,
                "permanent": true,
                "error": "sink-postgrest needs a `target` (the watch's url) and a delivery id",
            })
            .to_string(),
        );
    };
    emit(classify(&http_request(&request)).to_string())
}

/// Builds the PostgREST insert for one delivery, or `None` when the sink is
/// misconfigured.
///
/// Extracted so the shape of what this connector actually sends is testable on
/// the host — a wasm entry point that reads raw pointers is not.
pub fn build_request(delivery: &Value, target: &str) -> Option<Value> {
    let delivery_id = delivery.get("delivery_id").and_then(Value::as_str)?;
    if target.is_empty() {
        return None;
    }
    let row = json!({
        "id": delivery_id,
        "event": delivery.get("event").and_then(Value::as_str).unwrap_or("unknown"),
        "payload": delivery.get("body").cloned().unwrap_or(Value::Null),
    });
    Some(json!({
        "method": "POST",
        "url": target,
        "headers": {
            "content-type": "application/json",
            // Idempotency: a replay updates its row rather than duplicating it.
            "prefer": "resolution=merge-duplicates",
        },
        "body": row.to_string(),
    }))
}

/// Maps the host's `{status, body}` / `{error}` onto the sink contract.
///
/// The split that matters is `permanent`: a 4xx means PostgREST will keep
/// rejecting these exact bytes (a column that does not exist, a schema
/// violation), so the delivery should go `dead` now instead of burning five
/// backed-off retries to reach the same place. A 5xx or a transport error may
/// clear, and the DLQ ladder is exactly what covers it.
pub fn classify(response: &Value) -> Value {
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        return json!({"delivered": false, "permanent": false, "error": error});
    }
    let status = response.get("status").and_then(Value::as_u64).unwrap_or(0);
    match status {
        200..=299 => json!({"delivered": true}),
        // 429 is rate limiting, not a rejection of the body.
        429 => json!({"delivered": false, "permanent": false, "error": "postgrest: 429"}),
        400..=499 => json!({
            "delivered": false,
            "permanent": true,
            "error": format!("postgrest rejected the row: {status}"),
        }),
        other => json!({
            "delivered": false,
            "permanent": false,
            "error": format!("postgrest: {other}"),
        }),
    }
}

/// Self-describing manifest. **The `capabilities` block is the load-bearing
/// part**: the host parses it, builds this module's linker from it, and refuses
/// to link the module if it imports anything the block did not declare.
///
/// A value, not a string literal inside `describe()`, so the manifest the host
/// will read is the exact thing this crate's own test asserts on — the pointer
/// ABI `describe()` returns through is not meaningful off wasm.
pub fn manifest() -> Value {
    json!({
            "version": "0.1.0",
            "kind": "sink",
            "description": "Reference sink connector: POSTs each delivery as a row into a \
                            PostgREST table. Idempotent on delivery_id via \
                            Prefer: resolution=merge-duplicates.",
            "capabilities": {
                "http": { "hosts": DECLARED_HOSTS, "methods": ["POST"] },
            },
            "params_schema": {
                "target": "string — the PostgREST table endpoint; supplied by the watch's `url`.",
            },
            "output_schema": {
                "delivered": "bool",
                "permanent": "bool? — true when the receiver will keep rejecting these bytes",
                "error": "string?",
            },
    })
}

#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(manifest().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery() -> Value {
        json!({
            "delivery_id": "d-1",
            "event": "dataset.changed",
            "body": {"app": "grants", "count": 3},
        })
    }

    /// The idempotency contract, which is the only reason a replayed delivery
    /// does not become a duplicate row.
    #[test]
    fn the_row_is_keyed_on_the_stable_delivery_id_and_merges_duplicates() {
        let req = build_request(&delivery(), "http://localhost:3000/deliveries").expect("request");
        assert_eq!(req["method"], "POST");
        assert_eq!(req["headers"]["prefer"], "resolution=merge-duplicates");
        let row: Value = serde_json::from_str(req["body"].as_str().expect("body")).expect("json");
        assert_eq!(row["id"], "d-1");
        assert_eq!(row["payload"]["count"], 3);
    }

    /// A misconfigured sink must not climb the whole retry ladder to reach the
    /// same answer five times.
    #[test]
    fn a_missing_target_is_permanent_not_retried() {
        assert!(build_request(&delivery(), "").is_none());
        assert!(build_request(&json!({"event": "x"}), "http://localhost:3000/d").is_none());
    }

    /// The classification the server's DLQ ladder reads. A 4xx backwards here
    /// would retry an impossible delivery five times; a 5xx backwards would
    /// dead-letter a recoverable one.
    #[test]
    fn a_rejected_row_is_permanent_and_an_outage_is_not() {
        assert_eq!(classify(&json!({"status": 201}))["delivered"], true);
        let rejected = classify(&json!({"status": 422}));
        assert_eq!(rejected["delivered"], false);
        assert_eq!(rejected["permanent"], true);
        for transient in [json!({"status": 503}), json!({"status": 429})] {
            assert_eq!(classify(&transient)["permanent"], false);
        }
        // A refusal from the host (an undeclared host, no bridge, a timeout)
        // arrives as `error` with no status, and is retryable: an operator can
        // fix `allow_http_hosts` and the drain is what then succeeds.
        let refused = classify(&json!({"error": "host 'x' is not in the operator's allow list"}));
        assert_eq!(refused["delivered"], false);
        assert_eq!(refused["permanent"], false);
    }

    /// The manifest is the sandbox's input, so a typo in it is a security bug,
    /// not a cosmetic one. Pinned here rather than trusted.
    #[test]
    fn the_manifest_declares_exactly_the_import_this_module_uses() {
        let manifest = manifest();
        let http = &manifest["capabilities"]["http"];
        assert_eq!(http["methods"], json!(["POST"]));
        assert_eq!(http["hosts"], json!(DECLARED_HOSTS));
        assert!(
            manifest["capabilities"].get("kv").is_none(),
            "this connector stores nothing, so it must not ask for kv"
        );
    }
}
