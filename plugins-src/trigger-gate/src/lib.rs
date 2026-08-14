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
//! Contract: the output must be `{"pass": bool}` (a `reason` string rides
//! along). Anything else (or a trap) takes the trigger's fail-open path on the
//! host side.
//!
//! Logic: a batch-size gate and a dataset gate, each applied **only when the
//! envelope carries the field it reads**. `count` clears `params.min_count`
//! (default 1); `dataset` equals `params.dataset` when that is set. So one
//! module serves "only fan out on big batches" and "only this dataset" edges
//! without recompiling.
//!
//! ## Why "only when the envelope carries the field" is load-bearing
//!
//! The three `_trigger` envelopes are NOT the same shape (see
//! `crates/server/src/triggers.rs`):
//!
//! | builder                  | `count` | `dataset` |
//! | ------------------------ | ------- | --------- |
//! | `dataset_trigger_obj`    | yes     | yes       |
//! | `terminal_trigger_obj`   | **no**  | **no**    |
//! | `external_trigger_obj`   | **no**  | **no**    |
//!
//! This plugin used to read `count` with `unwrap_or(0)` against a `min_count`
//! defaulting to 1, so on a job- or external-kind hop `0 >= 1` was false and it
//! answered a well-formed `{"pass": false}` forever. That is the one place the
//! host's fail-open doctrine inverts: nothing failed, so the host recorded
//! `predicate_veto` — which the ledger defines as "a predicate that ran and
//! answered" — and the edge was dead permanently while every surface reported a
//! healthy gate. An absent field now means the rule is **inapplicable**, and
//! `reason` says which rule sat out and why.

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

/// What the envelope calls itself, for the reason string. Absent on a delta
/// this plugin could not parse at all.
fn source_kind(delta: &Value) -> &str {
    delta
        .get("source_kind")
        .and_then(Value::as_str)
        .unwrap_or("unrecognized")
}

/// The gate's answer and the reason it reports.
///
/// A rule whose field is absent from the envelope is **inapplicable**, not
/// failed: it records why it sat out and lets the other rules decide. Only a
/// field that is present and does not clear its bar is a veto — a veto is a
/// claim the gate looked at the thing it gates on, and the ledger word
/// `predicate_veto` means exactly that.
fn decide(delta: &Value, params: &Value) -> (bool, String) {
    let mut reasons: Vec<String> = Vec::new();

    // The dataset gate. Only dataset-kind envelopes carry `dataset`.
    if let Some(want) = params.get("dataset").and_then(Value::as_str) {
        match delta.get("dataset").and_then(Value::as_str) {
            Some(got) if got == want => reasons.push(format!("dataset '{got}' matches")),
            Some(got) => {
                return (
                    false,
                    format!("dataset '{got}' is not the required '{want}'"),
                )
            }
            None => reasons.push(format!(
                "no 'dataset' field on this {} envelope — params.dataset ('{want}') does not apply",
                source_kind(delta)
            )),
        }
    }

    // The batch-size gate. Only dataset-kind envelopes carry `count`; a job
    // hop's numbers live under `result_summary` and an external hop's under
    // `payload`, neither of which this predicate claims to understand.
    let min_count = params.get("min_count").and_then(Value::as_u64).unwrap_or(1);
    match delta.get("count").and_then(Value::as_u64) {
        Some(count) if count >= min_count => {
            reasons.push(format!("count {count} >= min_count {min_count}"))
        }
        Some(count) => return (false, format!("count {count} < min_count {min_count}")),
        None => reasons.push(format!(
            "no 'count' field on this {} envelope — min_count ({min_count}) does not apply",
            source_kind(delta)
        )),
    }

    (true, reasons.join("; "))
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
    let params = envelope.get("params").cloned().unwrap_or(Value::Null);
    let (pass, reason) = decide(&delta, &params);
    emit(json!({ "pass": pass, "reason": reason }).to_string())
}

/// Self-describing manifest for `GET /plugins?kind=predicate`.
#[no_mangle]
pub extern "C" fn describe() -> u64 {
    emit(
        json!({
            "version": "0.2.0",
            "kind": "predicate",
            "description": "Trigger predicate for DATASET-change hops: pass when the delta's count >= params.min_count (default 1) and, if params.dataset is set, the dataset matches. Job- and external-kind envelopes carry neither field, so both rules sit out and the hop passes — see `reason`.",
            "params_schema": {
                "min_count": "number? — minimum revision count to fire (default 1). Reads the delta's `count`, which ONLY dataset-kind hops carry; ignored (with a reason) on job/external hops.",
                "dataset": "string? — only fire for this exact dataset. Reads the delta's `dataset`, which ONLY dataset-kind hops carry; ignored (with a reason) on job/external hops.",
            },
            "output_schema": {
                "pass": "bool",
                "reason": "string — which rules applied, and which sat out because the envelope lacks their field",
            },
        })
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // The three real envelope shapes, transcribed from
    // `crates/server/src/triggers.rs` — `dataset_trigger_obj`,
    // `terminal_trigger_obj`, `external_trigger_obj`. Their DIFFERENCES are the
    // whole subject of these tests, so they are written out rather than derived
    // from one another.

    fn dataset_delta() -> Value {
        json!({
            "trigger_id": "T1",
            "source_kind": "dataset",
            "app": "src",
            "dataset": "grants",
            "kind": "any",
            "count": 3,
            "keys": ["k1", "k2", "k3"],
            "source_job_id": "J1",
            "depth": 1,
            "chain": ["T1"],
        })
    }

    fn job_delta() -> Value {
        json!({
            "trigger_id": "T1",
            "source_kind": "job",
            "app": "src",
            "status": "succeeded",
            "source_job_id": "J1",
            "result_summary": { "new": 7, "changed": 2 },
            "depth": 1,
            "chain": ["T1"],
        })
    }

    fn external_delta() -> Value {
        json!({
            "trigger_id": "T1",
            "source_kind": "external",
            "source_id": "S1",
            "source_name": "partner",
            "event_id": "E1",
            "payload": { "count": 9, "dataset": "grants" },
            "depth": 1,
            "chain": ["T1"],
        })
    }

    /// THE bug. `count` lives only in the dataset envelope, so reading it with
    /// `unwrap_or(0)` against a `min_count` defaulting to 1 made `0 >= 1` false
    /// and this predicate returned a well-formed `{"pass": false}` FOREVER on
    /// two of the three source kinds. Nothing failed, so the host recorded a
    /// `predicate_veto` — a healthy gate saying no — and the edge was dead with
    /// every surface reporting normal operation.
    #[test]
    fn a_countless_envelope_passes_instead_of_being_vetoed_forever() {
        for (kind, delta) in [("job", job_delta()), ("external", external_delta())] {
            // Default params: the shipped, documented configuration.
            let (pass, reason) = decide(&delta, &json!({}));
            assert!(pass, "a {kind} hop must not be vetoed by default: {reason}");
            assert!(
                reason.contains("count"),
                "and it must say which rule sat out: {reason}"
            );

            // And with the knobs the docs show operators writing.
            let (pass, reason) = decide(&delta, &json!({ "min_count": 10 }));
            assert!(
                pass,
                "min_count cannot veto an envelope with no count ({kind}): {reason}"
            );
            let (pass, reason) = decide(&delta, &json!({ "dataset": "grants" }));
            assert!(
                pass,
                "params.dataset cannot veto an envelope with no dataset ({kind}): {reason}"
            );
        }
    }

    /// An external hop carries `count`/`dataset` INSIDE `payload`, and this
    /// predicate does not claim to read payloads. A nested field must not be
    /// mistaken for a top-level one in either direction.
    #[test]
    fn a_nested_payload_count_is_not_read_as_the_deltas_count() {
        let (pass, reason) = decide(&external_delta(), &json!({ "min_count": 100 }));
        assert!(pass, "{reason}");
        assert!(
            reason.contains("does not apply"),
            "the payload's own count is not this rule's input: {reason}"
        );
    }

    /// The behaviour that was always right stays right: on the envelope this
    /// predicate is actually for, both rules apply and a real veto is a veto.
    #[test]
    fn a_dataset_delta_still_gates_on_count_and_dataset() {
        let d = dataset_delta();
        assert!(decide(&d, &json!({ "min_count": 2 })).0);
        assert!(!decide(&d, &json!({ "min_count": 10 })).0);
        assert!(decide(&d, &json!({ "dataset": "grants" })).0);
        assert!(!decide(&d, &json!({ "dataset": "orgs" })).0);
        // Default min_count still rejects an empty batch.
        assert!(!decide(&json!({ "source_kind": "dataset", "count": 0 }), &json!({})).0);
        // Both rules must clear.
        assert!(!decide(&d, &json!({ "min_count": 2, "dataset": "orgs" })).0);
        assert!(decide(&d, &json!({ "min_count": 2, "dataset": "grants" })).0);
    }

    /// A veto is a claim that the gate looked at the thing it gates on, so only
    /// a PRESENT field that misses its bar may produce one. The reason has to
    /// distinguish the two, because the host records both as `pass=false` and
    /// an operator has nothing else to read.
    #[test]
    fn an_inapplicable_rule_is_reported_not_silently_collapsed_into_a_veto() {
        let (pass, reason) = decide(&job_delta(), &json!({ "min_count": 5, "dataset": "grants" }));
        assert!(pass);
        assert!(reason.contains("job"), "names the envelope kind: {reason}");
        assert!(
            reason.matches("does not apply").count() == 2,
            "both rules must account for themselves: {reason}"
        );

        let (pass, reason) = decide(&dataset_delta(), &json!({ "min_count": 5 }));
        assert!(!pass);
        assert!(
            reason.contains("count 3 < min_count 5"),
            "a real veto names the numbers: {reason}"
        );
    }

    /// A delta the sandbox could not parse at all must fail OPEN, like every
    /// other unanswerable case on the host side — never silently closed.
    #[test]
    fn an_unparseable_delta_fails_open_not_closed() {
        let (pass, reason) = decide(&Value::Null, &json!({ "min_count": 3 }));
        assert!(pass, "{reason}");
        assert!(reason.contains("unrecognized"), "{reason}");
    }
}
