//! Trigger plugin hooks through the **real** `WasmPluginHost` — sandbox and
//! all — rather than the in-process stub the unit tests use.
//!
//! The stub can prove the host's *interpretation* of a plugin's output; only
//! this can prove that a real wasm module, executed under a real fuel budget in
//! a real store, produces that output at all, and that the fail-open contract
//! survives the sandbox's actual failure modes (fuel exhaustion, traps,
//! non-JSON output).
//!
//! Most of the fixtures are inline `wat`, compiled by wasmtime transparently
//! when written to a `.wasm` file — so these run unconditionally, with no
//! dependency on a build step. The tests that exercise the SHIPPED plugins
//! (`plugins-src/trigger-gate`, `plugins-src/delta-slim`) need
//! `just plugins-install` to have run and are `#[ignore]`d for it, following
//! `crates/engine-wasm/tests/plugins.rs`. They are not optional: CI installs
//! the artifacts and runs exactly these with `--ignored`, so a shipped plugin
//! that stops honouring the host ABI turns the build red instead of quietly
//! failing its hop open in production. `just plugins-verify` is the same thing
//! locally.

use std::path::PathBuf;
use std::sync::Arc;

use pumper_core::config::PluginConfig;
use pumper_core::{
    EnqueueOptions, JobStatus, NewTrigger, PluginHook, Plugins, Trigger, TriggerPluginHooks,
};
use pumper_engine_wasm::WasmPluginHost;
use serde_json::{json, Value};

use super::harness::{test_state_with_plugins, FakeApp};
use crate::state::AppState;
use crate::triggers::{apply_plugin_hooks, fire_terminal_triggers, missing_hook_plugins};

/// The object half of a hook verdict — the fail-open behaviour these tests were
/// written against. The incident half (what the ledger is told) is asserted by
/// the ledger tests further down, against the same real host.
async fn hook_obj(plugins: &dyn Plugins, trigger: &Trigger, obj: Value) -> Option<Value> {
    apply_plugin_hooks(plugins, trigger, obj).await.obj
}

// ── wat fixtures ─────────────────────────────────────────────────────────────

/// A module in the plugin ABI shape whose `extract_v2` always returns `out`.
/// `alloc` hands back a pointer well clear of the returned data, so the host's
/// input write cannot clobber it.
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

/// Spins forever: the host's fuel budget is the only thing that stops it.
const BURN_WAT: &str = "(module (memory (export \"memory\") 1) \
     (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
     (func (export \"extract_v2\") (param i32 i32) (result i64) \
       (loop $l (br $l)) (unreachable)))";

/// Traps immediately — the plugin-panic class.
const TRAP_WAT: &str = "(module (memory (export \"memory\") 1) \
     (func (export \"alloc\") (param i32) (result i32) (i32.const 4096)) \
     (func (export \"extract_v2\") (param i32 i32) (result i64) (unreachable)))";

/// A real `WasmPluginHost` over a private temp dir seeded with `(name, wat)`
/// fixtures. `fuel` is per call — the fuel-exhaustion test wants it small.
fn wat_host(tag: &str, fuel: u64, modules: &[(&str, &str)]) -> Arc<dyn Plugins> {
    let dir = std::env::temp_dir().join(format!(
        "pumper-trigger-plugins-{tag}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("fixture dir");
    for (name, wat) in modules {
        std::fs::write(dir.join(format!("{name}.wasm")), wat).expect("write fixture");
    }
    Arc::new(
        WasmPluginHost::new(&PluginConfig {
            dir,
            fuel,
            ..Default::default()
        })
        .expect("wasm host"),
    )
}

/// The `data/plugins` directory the shipped plugins install into, four levels
/// up from this crate.
fn installed_host() -> WasmPluginHost {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/plugins");
    WasmPluginHost::new(&PluginConfig {
        dir,
        ..Default::default()
    })
    .expect("wasm host")
}

fn hook(plugin: &str, params: Value) -> PluginHook {
    PluginHook {
        plugin: plugin.into(),
        params,
        on_error: None,
    }
}

fn trigger(hooks: Option<TriggerPluginHooks>) -> Trigger {
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

/// The `_trigger` delta a hook receives.
fn delta() -> Value {
    json!({
        "trigger_id": "T1",
        "source_kind": "dataset",
        "source_job_id": "J1",
        "dataset": "grants",
        "count": 3,
        "keys": ["k1", "k2", "k3"],
        "depth": 1,
        "chain": ["T1"],
    })
}

fn predicate_only(plugin: &str, params: Value) -> TriggerPluginHooks {
    TriggerPluginHooks {
        predicate: Some(hook(plugin, params)),
        transform: None,
    }
}

fn transform_only(plugin: &str, params: Value) -> TriggerPluginHooks {
    TriggerPluginHooks {
        predicate: None,
        transform: Some(hook(plugin, params)),
    }
}

// ── real-host behaviour ──────────────────────────────────────────────────────

#[tokio::test]
async fn a_real_wasm_predicate_vetoes_the_hop_and_a_passing_one_does_not() {
    let plugins = wat_host(
        "verdicts",
        200_000_000,
        &[
            ("veto", &returning_wat(r#"{"pass":false}"#)),
            ("allow", &returning_wat(r#"{"pass":true}"#)),
        ],
    );
    let vetoed = trigger(Some(predicate_only("veto", json!({}))));
    assert_eq!(
        hook_obj(plugins.as_ref(), &vetoed, delta()).await,
        None,
        "a real module returning pass=false must stop the hop"
    );
    let allowed = trigger(Some(predicate_only("allow", json!({}))));
    assert_eq!(
        hook_obj(plugins.as_ref(), &allowed, delta()).await,
        Some(delta()),
        "pass=true leaves the envelope untouched"
    );
}

#[tokio::test]
async fn a_real_wasm_transform_reshapes_but_cannot_forge_provenance() {
    // The module returns its own shape AND tries to rewrite lineage.
    let forging = r#"{"summary":"3 fresh","depth":99,"chain":["EVIL"],"trigger_id":"EVIL","event_id":"forged"}"#;
    let plugins = wat_host(
        "transform",
        200_000_000,
        &[("shape", &returning_wat(forging))],
    );
    let t = trigger(Some(transform_only("shape", json!({}))));
    let out = hook_obj(plugins.as_ref(), &t, delta())
        .await
        .expect("a transform never skips");
    // The plugin's shaping survives…
    assert_eq!(out["summary"], "3 fresh");
    assert!(out.get("keys").is_none(), "dropped keys stay dropped");
    // …and every host-owned key is re-stamped from the original.
    assert_eq!(out["depth"], 1);
    assert_eq!(out["chain"], json!(["T1"]));
    assert_eq!(out["trigger_id"], "T1");
    assert_eq!(out["source_job_id"], "J1");
    assert!(
        out.get("event_id").is_none(),
        "a key absent from the original cannot be conjured by the sandbox"
    );
}

/// Fail-OPEN is the contract, and each sandbox failure mode has to honour it
/// separately — a fuel trap, an explicit trap and a contract violation take
/// three different paths out of the host.
#[tokio::test]
async fn every_sandbox_failure_mode_fails_open_not_closed() {
    let plugins = wat_host(
        "failures",
        100_000, // small budget: the burner exhausts it fast
        &[
            ("burn", BURN_WAT),
            ("boom", TRAP_WAT),
            ("garbage", &returning_wat("this is not json")),
        ],
    );

    // 1. Fuel exhaustion in a predicate → the hop still fires.
    let t = trigger(Some(predicate_only("burn", json!({}))));
    assert_eq!(
        hook_obj(plugins.as_ref(), &t, delta()).await,
        Some(delta()),
        "a predicate that burns its fuel budget must not wedge the edge"
    );
    // 2. An outright trap → same.
    let t = trigger(Some(predicate_only("boom", json!({}))));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, Some(delta()));
    // 3. Non-JSON output → same, and specifically NOT a malformed envelope
    //    leaking into target params.
    let t = trigger(Some(predicate_only("garbage", json!({}))));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, Some(delta()));

    // The transform half of each: the ORIGINAL envelope survives intact.
    for plugin in ["burn", "boom", "garbage"] {
        let t = trigger(Some(transform_only(plugin, json!({}))));
        assert_eq!(
            hook_obj(plugins.as_ref(), &t, delta()).await,
            Some(delta()),
            "a failing {plugin} transform must keep the untransformed envelope"
        );
    }

    // …and `on_error: "skip"` is still the way to flip a predicate closed.
    let mut t = trigger(Some(predicate_only("boom", json!({}))));
    if let Some(h) = t.plugin_hooks.as_mut().and_then(|h| h.predicate.as_mut()) {
        h.on_error = Some("skip".into());
    }
    assert_eq!(
        hook_obj(plugins.as_ref(), &t, delta()).await,
        None,
        "on_error=skip is the opt-in to failing closed"
    );
}

// ── the unknown-plugin path, made loud ───────────────────────────────────────

#[test]
fn missing_hook_plugins_names_only_configured_absent_ones() {
    let plugins = wat_host("presence", 200_000_000, &[("here", &returning_wat("true"))]);
    // No hooks at all: nothing to be missing.
    assert!(missing_hook_plugins(plugins.as_ref(), &trigger(None)).is_empty());
    // Configured AND loaded: not missing.
    let t = trigger(Some(predicate_only("here", json!({}))));
    assert!(missing_hook_plugins(plugins.as_ref(), &t).is_empty());
    // Configured and absent: named.
    let t = trigger(Some(predicate_only("nowhere", json!({}))));
    assert_eq!(missing_hook_plugins(plugins.as_ref(), &t), vec!["nowhere"]);
    // Both hooks absent: both named, predicate first.
    let t = trigger(Some(TriggerPluginHooks {
        predicate: Some(hook("gone-a", json!({}))),
        transform: Some(hook("gone-b", json!({}))),
    }));
    assert_eq!(
        missing_hook_plugins(plugins.as_ref(), &t),
        vec!["gone-a", "gone-b"]
    );
}

/// The bug this closes: a configured predicate whose module was never built
/// into `data/plugins/` passed silently, so a gate nobody deployed looked
/// exactly like a gate that said yes. It must still fire (fail-open), but it
/// must leave a mark.
#[tokio::test]
async fn a_configured_hook_with_no_loaded_plugin_is_recorded_not_only_silently_passed() {
    let plugins = wat_host("missing", 200_000_000, &[]);
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let t = state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("gated"),
            source_kind: "job",
            source_app: "fake",
            source_dataset: None,
            on_change: None,
            on_status: Some("succeeded"),
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: Some(&TriggerPluginHooks {
                predicate: Some(hook("never-built", json!({}))),
                transform: None,
            }),
        })
        .await
        .unwrap();

    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;

    // Fail-open holds: the hop was enqueued.
    assert_eq!(
        state
            .storage
            .jobs_by_trigger(&t.id, 10)
            .await
            .unwrap()
            .len(),
        1,
        "a missing plugin must not wedge the edge"
    );
    // …and the ledger says why it was not actually gated.
    let decisions = state
        .storage
        .list_trigger_runs_page(&t.id, None, 50)
        .await
        .unwrap();
    let missing = decisions
        .iter()
        .find(|d| d.outcome == "plugin_missing")
        .expect("a plugin_missing row");
    let detail = missing.detail.as_deref().unwrap_or_default();
    assert!(detail.contains("never-built"), "names the plugin: {detail}");
    assert!(
        detail.contains("NOT gated"),
        "and says what that cost: {detail}"
    );
    assert!(
        decisions.iter().any(|d| d.outcome == "fired"),
        "the hop is recorded as fired too — the row is a note, not a veto"
    );
}

// ── the sandbox failure classes, in the ledger ───────────────────────────────

/// Creates a job-kind trigger on `fake` with the given hooks and returns it.
async fn hooked_trigger(state: &AppState, hooks: TriggerPluginHooks) -> Trigger {
    state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("gated"),
            source_kind: "job",
            source_app: "fake",
            source_dataset: None,
            on_change: None,
            on_status: Some("succeeded"),
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: Some(&hooks),
        })
        .await
        .expect("create trigger")
}

async fn outcomes_of(state: &AppState, trigger_id: &str) -> Vec<String> {
    state
        .storage
        .list_trigger_runs_page(trigger_id, None, 50)
        .await
        .expect("decisions")
        .into_iter()
        .map(|d| d.outcome)
        .collect()
}

/// The question the decision ledger exists to answer — "why didn't my trigger
/// gate?" — was unanswerable for every sandbox failure: a real module that
/// trapped let the hop fire with nothing but a `warn!` behind it. Through the
/// REAL host, with a real trap.
#[tokio::test]
async fn a_real_trap_leaves_a_hook_trap_row_while_the_hop_still_fires() {
    let plugins = wat_host("ledger-trap", 200_000_000, &[("boom", TRAP_WAT)]);
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let t = hooked_trigger(&state, predicate_only("boom", json!({}))).await;

    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;

    let decisions = state
        .storage
        .list_trigger_runs_page(&t.id, None, 50)
        .await
        .unwrap();
    let trap = decisions
        .iter()
        .find(|d| d.outcome == "hook_trap")
        .expect("the trap is in the ledger");
    let detail = trap.detail.as_deref().unwrap_or_default();
    assert!(detail.contains("boom"), "names the plugin: {detail}");
    assert!(
        detail.contains("NOT gated"),
        "and says the hop went through ungated: {detail}"
    );
    assert!(
        !decisions.iter().any(|d| d.outcome == "predicate_veto"),
        "a crashed sandbox must not be recorded as a gate decision"
    );
    assert!(
        decisions.iter().any(|d| d.outcome == "fired"),
        "fail-open is unchanged — the hop still fires"
    );
}

/// The other half: `on_error: skip` stops the hop, and the row still says
/// `hook_trap` rather than borrowing `predicate_veto`. This is the exact
/// conflation an operator could not see through — a crashed sandbox rendered as
/// a healthy "the gate said no".
#[tokio::test]
async fn a_fuel_burn_under_on_error_skip_stops_the_hop_without_faking_a_veto() {
    // Small budget so the spinning module exhausts it fast.
    let plugins = wat_host("ledger-fuel", 200_000, &[("burn", BURN_WAT)]);
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let mut hooks = predicate_only("burn", json!({}));
    if let Some(p) = hooks.predicate.as_mut() {
        p.on_error = Some("skip".into());
    }
    let t = hooked_trigger(&state, hooks).await;

    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;

    assert!(
        state
            .storage
            .jobs_by_trigger(&t.id, 10)
            .await
            .unwrap()
            .is_empty(),
        "on_error=skip really did stop the hop"
    );
    let decisions = state
        .storage
        .list_trigger_runs_page(&t.id, None, 50)
        .await
        .unwrap();
    let trap = decisions
        .iter()
        .find(|d| d.outcome == "hook_trap")
        .expect("the fuel exhaustion is in the ledger");
    assert!(trap
        .detail
        .as_deref()
        .unwrap_or_default()
        .contains("on_error=skip"));
    assert!(
        !decisions.iter().any(|d| d.outcome == "predicate_veto"),
        "THE bug: a crashed predicate under on_error=skip used to be indistinguishable \
         from a predicate that answered pass=false"
    );
}

/// A transform that answers bytes which are not JSON: the envelope survives
/// untransformed (fail-open), and the ledger says the shaping never happened.
#[tokio::test]
async fn non_json_transform_output_is_recorded_as_malformed() {
    let plugins = wat_host(
        "ledger-garbage",
        200_000_000,
        &[("garbage", &returning_wat("this is not json"))],
    );
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let t = hooked_trigger(&state, transform_only("garbage", json!({}))).await;

    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;

    let outcomes = outcomes_of(&state, &t.id).await;
    assert!(
        outcomes.iter().any(|o| o == "hook_malformed"),
        "expected a hook_malformed row, got {outcomes:?}"
    );
    assert!(outcomes.iter().any(|o| o == "fired"));
}

/// The repo's own fixture shape proved `has()` lied: a module with a `memory`
/// and nothing else compiled, loaded, and answered `has() == true` — so a hook
/// pointed at it looked deployed and silently did nothing. Loading stays
/// permissive (dynamic-app discovery depends on describe-only modules being
/// loadable); what changed is that it is no longer reported as USABLE.
#[tokio::test]
async fn a_module_without_the_extract_abi_is_not_a_usable_hook_plugin() {
    let plugins = wat_host(
        "no-abi",
        200_000_000,
        &[
            ("describe_only", "(module (memory (export \"memory\") 1))"),
            ("real", &returning_wat(r#"{"pass":true}"#)),
        ],
    );
    // Still loaded and still listed for discovery, flagged for what it is…
    let manifests = plugins.manifests();
    let entry = manifests
        .iter()
        .find(|m| m["name"] == json!("describe_only"))
        .expect("a describe-only module is still listed");
    assert_eq!(entry["executable"], json!(false));
    let real = manifests
        .iter()
        .find(|m| m["name"] == json!("real"))
        .expect("the real plugin is listed");
    assert_eq!(real["executable"], json!(true));
    // …but it is not offered as something a caller can run.
    assert!(!plugins.has("describe_only"), "has() answers executability");
    assert!(plugins.has("real"));
    assert_eq!(plugins.list(), vec!["real".to_string()]);

    // And a hook pointed at it is reported, not silently believed.
    let t = trigger(Some(predicate_only("describe_only", json!({}))));
    assert_eq!(
        missing_hook_plugins(plugins.as_ref(), &t),
        vec!["describe_only"],
        "the dry-run and the fire path must see the same truth"
    );
    let verdict = apply_plugin_hooks(plugins.as_ref(), &t, delta()).await;
    assert_eq!(verdict.obj, Some(delta()), "fail-open holds");
    assert_eq!(verdict.incidents[0].outcome, "hook_not_executable");
}

/// The known gap this closes: `plugin_missing` was written per hook PER EVENT,
/// forever. A busy edge with one typo buried its own ledger under identical
/// rows. It is a fact about the deployment, so it is recorded once — and
/// re-armed by the only thing that can change the answer.
#[tokio::test]
async fn a_missing_plugin_is_recorded_once_not_once_per_event() {
    let plugins = wat_host("dedup", 200_000_000, &[]);
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let t = hooked_trigger(&state, predicate_only("never-built", json!({}))).await;

    for _ in 0..3 {
        let job = succeeded_source(&state).await;
        fire_terminal_triggers(&state, &job).await;
    }
    let outcomes = outcomes_of(&state, &t.id).await;
    assert_eq!(
        outcomes.iter().filter(|o| *o == "plugin_missing").count(),
        1,
        "three events, one deployment fault, one row — got {outcomes:?}"
    );
    assert_eq!(
        outcomes.iter().filter(|o| *o == "fired").count(),
        3,
        "the per-event decisions are all still there — only the repeated \
         deployment fact is bounded"
    );

    // A reload is the state change that could make the answer different, so it
    // re-arms the report (POST /plugins/reload clears the same set).
    state.plugin_missing_reported.lock().await.clear();
    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;
    assert_eq!(
        outcomes_of(&state, &t.id)
            .await
            .iter()
            .filter(|o| *o == "plugin_missing")
            .count(),
        2,
        "after a reload the fault is reported again"
    );
}

/// A hook whose plugin IS loaded records nothing — the loud row must mean
/// something.
#[tokio::test]
async fn a_loaded_hook_plugin_records_no_plugin_missing_row() {
    let plugins = wat_host(
        "present",
        200_000_000,
        &[("allow", &returning_wat(r#"{"pass":true}"#))],
    );
    let (state, _store) = test_state_with_plugins(vec![Arc::new(FakeApp)], plugins).await;
    let t = state
        .storage
        .create_trigger(&NewTrigger {
            name: Some("gated"),
            source_kind: "job",
            source_app: "fake",
            source_dataset: None,
            on_change: None,
            on_status: Some("succeeded"),
            target_app: "fake",
            params: &json!({}),
            budget_usd: None,
            priority: 0,
            max_attempts: 1,
            filters: None,
            plugin_hooks: Some(&TriggerPluginHooks {
                predicate: Some(hook("allow", json!({}))),
                transform: None,
            }),
        })
        .await
        .unwrap();

    let job = succeeded_source(&state).await;
    fire_terminal_triggers(&state, &job).await;
    let decisions = state
        .storage
        .list_trigger_runs_page(&t.id, None, 50)
        .await
        .unwrap();
    assert!(
        !decisions.iter().any(|d| d.outcome == "plugin_missing"),
        "a deployed plugin must not be reported as missing"
    );
}

async fn succeeded_source(state: &AppState) -> pumper_core::Job {
    let job = state
        .storage
        .enqueue("fake", EnqueueOptions::default())
        .await
        .unwrap();
    let mut job = state.storage.get(job.id).await.unwrap().unwrap();
    job.status = JobStatus::Succeeded;
    job
}

// ── the SHIPPED plugins (need `just plugins-install`) ────────────────────────

const NEEDS_INSTALL: &str =
    "requires data/plugins/{trigger-gate,delta-slim}.wasm — run `just plugins-verify`, \
     which installs them and runs exactly these tests (CI runs the same step)";

#[tokio::test]
#[ignore = "requires the built data/plugins/trigger-gate.wasm — `just plugins-verify` (CI runs this step)"]
async fn shipped_trigger_gate_gates_on_min_count_and_dataset() {
    let plugins = installed_host();
    assert!(plugins.has("trigger-gate"), "{NEEDS_INSTALL}");
    let plugins: Arc<dyn Plugins> = Arc::new(plugins);

    // count=3 clears min_count=2 → fires.
    let t = trigger(Some(predicate_only(
        "trigger-gate",
        json!({"min_count": 2}),
    )));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, Some(delta()));
    // …and does not clear min_count=10 → vetoed.
    let t = trigger(Some(predicate_only(
        "trigger-gate",
        json!({"min_count": 10}),
    )));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, None);
    // The dataset knob: the delta is `grants`.
    let t = trigger(Some(predicate_only(
        "trigger-gate",
        json!({"dataset": "grants"}),
    )));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, Some(delta()));
    let t = trigger(Some(predicate_only(
        "trigger-gate",
        json!({"dataset": "orgs"}),
    )));
    assert_eq!(hook_obj(plugins.as_ref(), &t, delta()).await, None);
}

#[tokio::test]
#[ignore = "requires the built data/plugins/delta-slim.wasm — `just plugins-verify` (CI runs this step)"]
async fn shipped_delta_slim_slims_the_envelope_without_losing_lineage() {
    let plugins = installed_host();
    assert!(plugins.has("delta-slim"), "{NEEDS_INSTALL}");
    let plugins: Arc<dyn Plugins> = Arc::new(plugins);

    // `keep` mode: only the named keys survive — plus the host's provenance.
    let t = trigger(Some(transform_only(
        "delta-slim",
        json!({ "keep": ["dataset", "count"] }),
    )));
    let out = hook_obj(plugins.as_ref(), &t, delta())
        .await
        .expect("transform never skips");
    assert_eq!(out["dataset"], "grants");
    assert_eq!(out["count"], 3);
    assert_eq!(out["slimmed"], true);
    assert!(out.get("keys").is_none(), "keys were not kept");
    assert_eq!(out["depth"], 1, "provenance is host-restamped regardless");
    assert_eq!(out["chain"], json!(["T1"]));
    assert_eq!(out["trigger_id"], "T1");

    // `max_keys` mode: the key list is capped, nothing else is dropped.
    let t = trigger(Some(transform_only("delta-slim", json!({ "max_keys": 1 }))));
    let out = hook_obj(plugins.as_ref(), &t, delta())
        .await
        .expect("transform never skips");
    assert_eq!(out["keys"], json!(["k1"]));
    assert_eq!(
        out["count"], 3,
        "count stays exact — only the sample shrinks"
    );
}
