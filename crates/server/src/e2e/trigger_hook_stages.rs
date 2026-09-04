//! Per-stage generated-input targets over the trigger hook pipeline, run
//! against the **real** `WasmPluginHost` under its fuel budget.
//!
//! `trigger_plugins.rs` next door drives the same pipeline end to end on
//! hand-written cases, and stays the end-to-end contract; this file is not a
//! replacement for it. What it adds is the thing an end-to-end target cannot
//! do: **a crash at the predicate stage masks every defect at the transform
//! stage on that input.** The hop stops, the transform never runs, and the
//! envelopes that would have exercised the re-stamp are exactly the ones no
//! end-to-end case can reach. So there is one target per stage, each fed that
//! stage's own input type and asserting that stage's own oracle, triaged in
//! pipeline order — the predicate target's crash set is drained first.
//!
//! The transform target is deliberately fed *both* halves: the envelopes the
//! predicate stage accepted, and the envelopes on which it produced nothing at
//! all — an honest veto, or a gate that did not answer under `on_error: skip`.
//! The second half is the measurable, and it is 0-by-construction for any
//! harness that only drives the whole pipeline.
//!
//! Both targets run under a small per-call fuel budget, so a plugin that loops
//! is a trap and a finding rather than a hang, and wall time is bounded by
//! construction — which makes "took far longer than the budget predicts" a
//! finding class of its own rather than a flake to suppress.
//!
//! **No generated-input crate is used, because the tree declares none.** No
//! workspace manifest names `proptest`, `quickcheck`, `arbitrary` or `bolero`;
//! `rand` and `arbitrary` reach `Cargo.lock` only transitively (wasmtime's
//! `cranelift-control`, `zip`), so reaching for either would mean adding a new
//! direct dependency edge, which one test file did not justify. The generator
//! is therefore the dozen lines of SplitMix64 below, and it is load-bearing
//! code, not scaffolding. When a case fails, the assertion
//! carries the **generated input itself**, not the seed that produced it: a
//! seed names an input only relative to the generator that consumed it, and
//! the edit most likely to re-point it is the fix for the defect it recorded.

use std::sync::Arc;
use std::time::Instant;

use pumper_core::config::PluginConfig;
use pumper_core::storage::TRIGGER_OUTCOMES;
use pumper_core::{PluginHook, Plugins, Trigger, TriggerPluginHooks};
use pumper_engine_wasm::WasmPluginHost;
use serde_json::{json, Value};

use crate::triggers::{
    apply_plugin_hooks, external_trigger_obj, host_owned_overrides, predicate_verdict, HookSlot,
};

/// Per-call CPU budget for every module in this file. Small on purpose: the
/// looping fixture has to exhaust it fast enough that non-termination reads as
/// a trap. Everything else here returns a constant and spends a few hundred
/// units, so a case that needs more than [`WALL_CEILING_MS`] is not a slow
/// machine — it is a pathological path in the stage under test.
const FUEL: u64 = 200_000;

/// The wall-clock a fuel-bounded call must not exceed. Three orders of
/// magnitude above what 200k fuel plus one instantiation costs, so this is a
/// finding threshold and not a timeout.
const WALL_CEILING_MS: u128 = 2_000;

/// Cases per target. Each is one instantiate + one call.
const CASES: usize = 300;

/// The generator's identity. Recorded so a run can be repeated; it is a
/// convenience for sharing a run in progress, never the artifact of a failure.
const SEED: u64 = 0x_9E37_79B9_7F4A_7C15;

// ── the generator ────────────────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn seeded(case: usize) -> Self {
        Rng(SEED ^ (case as u64).wrapping_mul(0xD1B5_4A32_D192_ED03))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

fn trigger_with(hooks: Option<TriggerPluginHooks>) -> Trigger {
    Trigger {
        id: "T1".into(),
        name: None,
        source_kind: "dataset".into(),
        source_app: "src".into(),
        source_dataset: Some("*".into()),
        on_change: None,
        on_status: None,
        target_app: "fake".into(),
        params: json!({}),
        budget_usd: None,
        priority: 0,
        max_attempts: 1,
        enabled: true,
        created_at: chrono::Utc::now(),
        filters: None,
        plugin_hooks: hooks,
    }
}

fn hook(plugin: &str, on_error: Option<&str>) -> PluginHook {
    PluginHook {
        plugin: plugin.into(),
        params: json!({}),
        on_error: on_error.map(String::from),
    }
}

/// One generated `_trigger` envelope, then a mutation drawn from the space just
/// outside the well-formed one.
///
/// **What this generator cannot produce, stated so a green run is readable.**
/// The external shape is built by the host's own `external_trigger_obj`, so it
/// cannot drift. The dataset and terminal shapes are mirrored by hand, because
/// the real builders need a `Job` and a `&[&Revision]` that only the crawl
/// archive behind them supplies — a field added to `dataset_trigger_obj` will
/// not appear here, and this target will report green on the older shape. The
/// mutations are single-key and adjacent by design: an envelope that is wrong
/// in every respect only ever exercises the first check.
fn envelope(rng: &mut Rng) -> Value {
    let depth = rng.below(3) as u32;
    let chain = vec!["T0".to_string(), "T1".to_string()];
    let mut obj = match rng.below(3) {
        0 => external_trigger_obj(
            &trigger_with(None),
            "S1",
            "webhook",
            "E1",
            &json!({ "n": rng.below(1000) }),
            depth,
            &chain,
        ),
        // Mirrors `dataset_trigger_obj`.
        1 => json!({
            "trigger_id": "T1", "source_kind": "dataset", "app": "src",
            "dataset": "grants", "kind": "any", "count": rng.below(500),
            "keys": (0..rng.below(4)).map(|k| format!("k{k}")).collect::<Vec<_>>(),
            "keys_truncated": rng.below(2) == 1, "source_job_id": "J1",
            "depth": depth, "chain": chain,
        }),
        // Mirrors `terminal_trigger_obj`.
        _ => json!({
            "trigger_id": "T1", "source_kind": "job", "app": "src",
            "status": "succeeded", "source_job_id": "J1",
            "result_summary": { "new": rng.below(9), "changed": Value::Null },
            "depth": depth, "chain": chain,
        }),
    };
    let map = obj.as_object_mut().expect("builders return objects");
    match rng.below(6) {
        // Valid by construction — the well-formed half has to stay reachable.
        0 | 1 => {}
        // Right key, wrong type.
        2 => {
            map.insert("depth".into(), json!("deep"));
        }
        // The work-scope key gone: absent `keys` is "sweep everything".
        3 => {
            map.remove("keys");
        }
        // At the host's key_cap.
        4 => {
            map.insert("keys".into(), json!(vec!["k"; 200]));
        }
        // Array collapsed to a scalar.
        _ => {
            map.insert("chain".into(), json!("T1"));
        }
    }
    obj
}

// ── wat fixtures ─────────────────────────────────────────────────────────────

/// A module in the plugin ABI shape whose `extract_v2` always returns `out`.
fn returning_wat(out: &str) -> String {
    let escaped = out.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        "(module (memory (export \"memory\") 2) (data (i32.const 16) \"{escaped}\") \
         (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
         (func (export \"extract_v2\") (param i32 i32) (result i64) \
           (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const {len}))))",
        len = out.len()
    )
}

const BURN_WAT: &str = "(module (memory (export \"memory\") 1) \
     (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
     (func (export \"extract_v2\") (param i32 i32) (result i64) \
       (loop $l (br $l)) (unreachable)))";

const TRAP_WAT: &str = "(module (memory (export \"memory\") 1) \
     (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
     (func (export \"extract_v2\") (param i32 i32) (result i64) (unreachable)))";

/// The predicate outputs the generator draws from: the whole grammar
/// `predicate_verdict` accepts (bare booleans, `{"pass": bool}`) and the space
/// just outside it (right key wrong type, near-miss key, other JSON scalars and
/// containers, bytes that are not JSON at all), plus the two sandbox failures.
const PREDICATE_OUTPUTS: &[(&str, &str)] = &[
    ("p_true", "true"),
    ("p_false", "false"),
    ("p_pass", r#"{"pass":true}"#),
    ("p_nopass", r#"{"pass":false}"#),
    ("p_extra", r#"{"pass":true,"why":"ok"}"#),
    ("p_wrongtype", r#"{"pass":"yes"}"#),
    ("p_nearmiss", r#"{"passed":true}"#),
    ("p_object", "{}"),
    ("p_null", "null"),
    ("p_number", "42"),
    ("p_string", "\"pass\""),
    ("p_array", "[true]"),
    ("p_notjson", "pass: yes"),
];

/// Transform outputs across the JSON space: objects that reshape legitimately,
/// objects that try to forge or drop host-owned keys, every non-object JSON
/// value, and the same two sandbox failures.
const TRANSFORM_OUTPUTS: &[(&str, &str)] = &[
    ("t_shape", r#"{"summary":"3 fresh"}"#),
    ("t_empty", "{}"),
    (
        "t_forge",
        r#"{"depth":99,"chain":["EVIL"],"trigger_id":"EVIL","event_id":"forged","keys":[]}"#,
    ),
    ("t_narrow", r#"{"keys":["k1"],"slimmed":true}"#),
    ("t_true", "true"),
    ("t_array", "[1,2]"),
    ("t_string", "\"shaped\""),
    ("t_null", "null"),
    ("t_number", "12"),
    ("t_notjson", "<not json>"),
];

/// A real `WasmPluginHost` over a private temp dir holding every fixture in
/// this file. One host per test so parallel tests do not share a directory.
fn host(tag: &str) -> Arc<dyn Plugins> {
    let dir = std::env::temp_dir().join(format!("pumper-hook-stages-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("fixture dir");
    for (name, out) in PREDICATE_OUTPUTS.iter().chain(TRANSFORM_OUTPUTS) {
        std::fs::write(dir.join(format!("{name}.wasm")), returning_wat(out)).expect("fixture");
    }
    std::fs::write(dir.join("x_trap.wasm"), TRAP_WAT).expect("fixture");
    std::fs::write(dir.join("x_burn.wasm"), BURN_WAT).expect("fixture");
    Arc::new(
        WasmPluginHost::new(&PluginConfig {
            dir,
            fuel: FUEL,
            ..Default::default()
        })
        .expect("wasm host"),
    )
}

/// Draws a module name and the output it returns, or `None` for the two sandbox
/// failures, which have no output to model.
fn draw<'a>(rng: &mut Rng, catalogue: &[(&'a str, &'a str)]) -> (&'a str, Option<&'a str>) {
    let n = catalogue.len();
    match rng.below(n + 2) {
        i if i < n => (catalogue[i].0, Some(catalogue[i].1)),
        i if i == n => ("x_trap", None),
        _ => ("x_burn", None),
    }
}

/// The reference model for the predicate stage, computed from the generated
/// output string alone — no host, no `apply_plugin_hooks`. Deliberately simpler
/// than the thing it checks, and derived from the contract rather than from the
/// implementation's control flow.
#[derive(Debug, PartialEq)]
enum Answer {
    Pass,
    Veto,
    /// The gate did not answer: malformed verdict, non-JSON bytes, or a sandbox
    /// failure. Fail-open applies, and the stage owes an incident either way.
    Unanswered,
}

fn model(out: Option<&str>) -> Answer {
    let Some(out) = out else {
        return Answer::Unanswered;
    };
    match serde_json::from_str::<Value>(out)
        .ok()
        .as_ref()
        .and_then(predicate_verdict)
    {
        Some(true) => Answer::Pass,
        Some(false) => Answer::Veto,
        None => Answer::Unanswered,
    }
}

// ── target 1: the predicate stage ────────────────────────────────────────────

/// **The predicate stage's own oracle: a verdict is produced, or the fail-open
/// default fires AND is recorded.**
///
/// The failure this exists to catch is the one `triggers.rs` already paid for
/// once by hand: a predicate that never answered takes the same fail-open path
/// as a predicate that said yes, so a gate nobody deployed is indistinguishable
/// from a gate that passed. `missing_hook_plugins` fixed that for one stage;
/// this asserts it across the whole verdict grammar and both sandbox failures —
/// an empty incident list may only ever mean a real `pass=true`.
///
/// This target's crash set is drained before the transform target's findings
/// are read: a transform finding on an envelope this one is still failing on is
/// not minimal.
#[tokio::test]
async fn the_predicate_stage_answers_or_records_across_the_verdict_grammar() {
    let plugins = host("predicate");
    let (mut vetoed, mut unanswered, mut discarded, mut over_budget) = (0usize, 0, 0, 0);

    for case in 0..CASES {
        let rng = &mut Rng::seeded(case);
        let obj = envelope(rng);
        let (plugin, out) = draw(rng, PREDICATE_OUTPUTS);
        let on_error = [None, Some("skip"), Some("fire")][rng.below(3)];
        let t = trigger_with(Some(TriggerPluginHooks {
            predicate: Some(hook(plugin, on_error)),
            transform: None,
        }));
        // The generated input, carried into every message below. This, not the
        // seed, is what a failure hands the reader.
        let case_id = format!("plugin={plugin} on_error={on_error:?} envelope={obj}");

        let started = Instant::now();
        let verdict = apply_plugin_hooks(plugins.as_ref(), &t, obj.clone()).await;
        let elapsed = started.elapsed().as_millis();
        if elapsed > WALL_CEILING_MS {
            over_budget += 1;
        }

        for i in &verdict.incidents {
            assert_eq!(i.slot, HookSlot::Predicate, "{case_id}");
            assert!(
                TRIGGER_OUTCOMES.contains(&i.outcome),
                "outcome '{}' is not a recordable trigger_runs.outcome — {case_id}",
                i.outcome
            );
            if i.outcome == "hook_host_error" {
                discarded += 1;
            }
        }

        match model(out) {
            Answer::Pass => {
                assert_eq!(
                    verdict.obj,
                    Some(obj),
                    "a passing gate reshapes nothing — {case_id}"
                );
                assert!(
                    verdict.incidents.is_empty(),
                    "a clean pass owes no row — {case_id}"
                );
            }
            Answer::Veto => {
                vetoed += 1;
                assert_eq!(
                    verdict.obj, None,
                    "pass=false must stop the hop — {case_id}"
                );
                assert_eq!(
                    verdict
                        .incidents
                        .iter()
                        .map(|i| i.outcome)
                        .collect::<Vec<_>>(),
                    vec!["predicate_veto"],
                    "a veto is one healthy decision row — {case_id}"
                );
            }
            Answer::Unanswered => {
                unanswered += 1;
                // The whole point. Fail-open still fires unless on_error=skip —
                // and either way the ledger is told, so an ungated hop is
                // never spelled the same way as a gated one.
                assert!(
                    !verdict.incidents.is_empty(),
                    "a gate that did not answer left no row: fail-open is now \
                     indistinguishable from pass=true — {case_id}"
                );
                assert_ne!(
                    verdict.incidents[0].outcome, "predicate_veto",
                    "a sandbox that crashed was recorded as a healthy veto — {case_id}"
                );
                assert_eq!(
                    verdict.obj.is_some(),
                    on_error != Some("skip"),
                    "fail-open default did not match on_error — {case_id}"
                );
                assert!(
                    verdict.stop_reason().is_some() || verdict.obj.is_some(),
                    "{case_id}"
                );
            }
        }
    }

    // The generator's own report. A discard fraction near one means the target
    // is testing the host's reject path and reporting green — the failure this
    // whole shape exists to avoid — and a rise after a generator edit is a
    // generator that got worse while the suite stayed green.
    let discard = discarded as f64 / CASES as f64;
    eprintln!(
        "[predicate target] cases={CASES} vetoed={vetoed} unanswered={unanswered} \
         discard_fraction={discard:.3} over_budget={over_budget}"
    );
    assert_eq!(
        over_budget, 0,
        "a fuel-bounded call ran past {WALL_CEILING_MS}ms: \
         a pathological path in the stage, not a slow machine"
    );
    assert!(
        discard < 0.2,
        "discard fraction {discard:.3}: the generator is \
         mostly producing envelopes the host rejects before any verdict is read"
    );
    assert!(
        vetoed > 0 && unanswered > 0,
        "the generator reached only one \
         branch of the verdict grammar (vetoed={vetoed} unanswered={unanswered})"
    );
}

// ── target 2: the transform stage ────────────────────────────────────────────

/// **The transform stage's own oracle: output parses as JSON and host-owned
/// keys are re-stamped from the original, or the output is recorded as
/// malformed and the original envelope is kept — never a record of what the
/// sandbox proposed.**
///
/// Justified in writing so a tidy-up does not delete it as redundant with the
/// e2e cases: **it is the drain for the masking**. Half of these envelopes are
/// ones the predicate stage vetoed or trapped on, which means no end-to-end
/// case can reach the transform stage with them at all — the hop stopped three
/// lines earlier. The count of distinct transform failure classes reached on
/// exactly that subset is printed below, and it is the number this target
/// exists to move off zero.
#[tokio::test]
async fn the_transform_stage_restamps_or_records_including_behind_a_stopped_predicate() {
    let plugins = host("transform");
    let (mut behind_stop, mut classes_behind_stop, mut over_budget) = (0usize, Vec::new(), 0);

    for case in 0..CASES {
        let rng = &mut Rng::seeded(case);
        let obj = envelope(rng);

        // The predicate stage as the upstream lane: it decides whether an
        // end-to-end harness could have reached the transform with this
        // envelope at all. Its verdict is a label here, not a filter.
        let (p_plugin, p_out) = draw(rng, PREDICATE_OUTPUTS);
        let p_on_error = [None, Some("skip"), Some("fire")][rng.below(3)];
        // The upstream stage produces no envelope at all on exactly two shapes:
        // an honest veto, and a gate that did not answer under `on_error: skip`.
        // Those are the envelopes no end-to-end case can carry to the transform.
        let stopped = match model(p_out) {
            Answer::Veto => true,
            Answer::Unanswered => p_on_error == Some("skip"),
            Answer::Pass => false,
        };
        if stopped {
            behind_stop += 1;
        }

        let (plugin, out) = draw(rng, TRANSFORM_OUTPUTS);
        let t = trigger_with(Some(TriggerPluginHooks {
            predicate: None,
            transform: Some(hook(plugin, None)),
        }));
        let case_id = format!(
            "plugin={plugin} behind_predicate={p_plugin}/{p_on_error:?} stopped={stopped} \
             envelope={obj}"
        );

        let started = Instant::now();
        let verdict = apply_plugin_hooks(plugins.as_ref(), &t, obj.clone()).await;
        if started.elapsed().as_millis() > WALL_CEILING_MS {
            over_budget += 1;
        }

        // A transform never stops a hop, whatever it returns.
        let Some(shaped) = verdict.obj.clone() else {
            panic!("a transform never skips — {case_id}")
        };
        assert!(
            shaped.is_object(),
            "the envelope left the stage as non-JSON-object — {case_id}"
        );
        assert!(
            host_owned_overrides(&obj, &shaped).is_empty(),
            "a host-owned key survived the sandbox's proposal: {:?} — {case_id}",
            host_owned_overrides(&obj, &shaped)
        );

        let object_output = out
            .and_then(|o| serde_json::from_str::<Value>(o).ok())
            .is_some_and(|v| v.is_object());
        if object_output {
            assert!(
                verdict.incidents.is_empty(),
                "a well-formed reshape owes no row — {case_id}"
            );
        } else {
            assert_eq!(
                shaped, obj,
                "a malformed transform must keep the ORIGINAL envelope, not a \
                 partial record of what the plugin proposed — {case_id}"
            );
            assert_eq!(verdict.incidents.len(), 1, "{case_id}");
            let i = &verdict.incidents[0];
            assert_eq!(i.slot, HookSlot::Transform, "{case_id}");
            assert!(TRIGGER_OUTCOMES.contains(&i.outcome), "{case_id}");
            if stopped && !classes_behind_stop.contains(&i.outcome) {
                classes_behind_stop.push(i.outcome);
            }
        }
    }

    classes_behind_stop.sort_unstable();
    eprintln!(
        "[transform target] cases={CASES} behind_a_stopped_predicate={behind_stop} \
         distinct_failure_classes_behind_it={} {classes_behind_stop:?} over_budget={over_budget}",
        classes_behind_stop.len()
    );
    assert_eq!(
        over_budget, 0,
        "a fuel-bounded call ran past {WALL_CEILING_MS}ms"
    );
    assert!(
        behind_stop > 0,
        "no envelope reached the transform stage behind a predicate that stopped \
         the hop: this target is measuring nothing the e2e file does not"
    );
    assert!(
        !classes_behind_stop.is_empty(),
        "0 distinct transform failure classes behind a stopped predicate. If this \
         holds after the generator is widened, the masking is not costing anything \
         here and the target is a cost with no finding"
    );
}
