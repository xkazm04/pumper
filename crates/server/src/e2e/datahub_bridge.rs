//! The DataHub bridge against a mock GMS — the emitter's first tests that
//! exercise the wire instead of a pure aspect builder.
//!
//! No mock-HTTP crate is added: the workspace has none, and the repo already
//! owns this shape (`harness::TestReceiver`, `e2e/fetch_proxy.rs`) — a loopback
//! axum server on an ephemeral port. [`MockGms`] is the DataHub-flavoured
//! version: it records every ingestion POST's parsed entity batch, and answers
//! from a status script so a mid-batch failure can be scripted exactly.
//!
//! What is pinned here:
//! - ingestion is **batched at 25 entities**, so one oversized payload can't
//!   take down a whole emission;
//! - a failing batch **aborts the rest** and the status entry says how many
//!   entities already landed (there is no rollback, and deliberately no retry);
//! - `POST /datahub/sync` is **not re-entrant** — a second concurrent call is
//!   rejected rather than doubling the GMS load and racing the lineage
//!   read-merge.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use super::harness::{test_state_with, wait_for, FakeApp};
use crate::datahub::SyncOutcome;
use crate::state::AppState;

/// A loopback stand-in for a DataHub GMS. Records the entity batch of every
/// ingestion POST and replies with the next scripted status (200 once the
/// script runs out).
struct MockGms {
    addr: SocketAddr,
    batches: Arc<Mutex<Vec<Vec<Value>>>>,
    graphql: Arc<Mutex<usize>>,
    /// Remote governance state per `"<app>.<dataset>"`, matched against the URN
    /// in the GraphQL variables. Absent ⇒ `dataset: null`, i.e. a dataset
    /// DataHub has never seen (all signals false).
    remote: Arc<Mutex<HashMap<String, Value>>>,
    /// While set, every governance read fails — a DataHub outage, which is the
    /// only way to reach the poll's abort path.
    outage: Arc<Mutex<bool>>,
}

/// A DataHub `dataset` node carrying the three signals governance reads.
fn remote_state(deprecated: bool, cost_pause: bool, failing: bool) -> Value {
    json!({
        "deprecation": {"deprecated": deprecated},
        "tags": {"tags": if cost_pause {
            json!([{"tag": {"urn": "urn:li:tag:cost:pause"}}])
        } else {
            json!([])
        }},
        "health": [{"type": "ASSERTIONS", "status": if failing { "FAIL" } else { "PASS" }}],
    })
}

impl MockGms {
    async fn spawn(statuses: Vec<u16>, delay: Duration) -> Self {
        Self::spawn_with(statuses, delay, Duration::ZERO).await
    }

    /// `delay` slows the ingestion path; `graphql_delay` slows the governance
    /// read path (`/api/graphql`) independently, so a *hanging poll* can be
    /// staged without also stalling emissions.
    async fn spawn_with(statuses: Vec<u16>, delay: Duration, graphql_delay: Duration) -> Self {
        let batches: Arc<Mutex<Vec<Vec<Value>>>> = Arc::new(Mutex::new(Vec::new()));
        let graphql: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
        let remote: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
        let outage: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
        let script = Arc::new(Mutex::new(VecDeque::from(statuses)));

        let batches_h = batches.clone();
        let graphql_h = graphql.clone();
        let remote_h = remote.clone();
        let outage_h = outage.clone();
        let handler = move |req: axum::extract::Request| {
            let batches = batches_h.clone();
            let graphql = graphql_h.clone();
            let remote = remote_h.clone();
            let outage = outage_h.clone();
            let script = script.clone();
            async move {
                let is_graphql = req.uri().path().contains("/api/graphql");
                let is_ingest = req.method() == axum::http::Method::POST && !is_graphql;
                let body = axum::body::to_bytes(req.into_body(), 1 << 22)
                    .await
                    .unwrap_or_default();
                if is_ingest {
                    let parsed: Vec<Value> = serde_json::from_slice(&body).unwrap_or_default();
                    batches.lock().unwrap().push(parsed);
                }
                if is_graphql {
                    *graphql.lock().unwrap() += 1;
                    tokio::time::sleep(graphql_delay).await;
                    if *outage.lock().unwrap() {
                        return (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            "GMS down".to_string(),
                        );
                    }
                    // Answer from the scripted remote state for whichever
                    // dataset URN this read asked about.
                    let urn = serde_json::from_slice::<Value>(&body)
                        .ok()
                        .and_then(|q| q["variables"]["urn"].as_str().map(String::from))
                        .unwrap_or_default();
                    let dataset = remote
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|(k, _)| urn.contains(&format!(",{k},")))
                        .map(|(_, v)| v.clone())
                        .unwrap_or(Value::Null);
                    return (
                        axum::http::StatusCode::OK,
                        json!({ "data": { "dataset": dataset } }).to_string(),
                    );
                }
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                let status = script.lock().unwrap().pop_front().unwrap_or(200);
                (
                    axum::http::StatusCode::from_u16(status).unwrap(),
                    "{}".to_string(),
                )
            }
        };
        let app = axum::Router::new().fallback(axum::routing::any(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback GMS");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            addr,
            batches,
            graphql,
            remote,
            outage,
        }
    }

    /// Governance GraphQL reads received so far.
    fn graphql_reads(&self) -> usize {
        *self.graphql.lock().unwrap()
    }

    /// Takes GMS down (or brings it back) for the governance read path.
    fn set_outage(&self, down: bool) {
        *self.outage.lock().unwrap() = down;
    }

    /// Scripts the remote state of one `"<app>.<dataset>"`.
    fn set_remote(&self, app_dataset: &str, state: Value) {
        self.remote
            .lock()
            .unwrap()
            .insert(app_dataset.to_string(), state);
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batches.lock().unwrap().iter().map(Vec::len).collect()
    }

    fn requests(&self) -> usize {
        self.batches.lock().unwrap().len()
    }
}

/// A state wired to `gms`, with the emit toggles pinned so entity counts are
/// arithmetic rather than config-dependent.
async fn state_for(gms: &MockGms) -> (AppState, pumper_core::testing::TempStore) {
    let url = gms.url();
    test_state_with(vec![Arc::new(FakeApp)], move |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = url;
        c.datahub.emit_schema = true;
        c.datahub.emit_profile = true;
        // Topology needs schedules/triggers, of which a fresh temp store has
        // none — off here so the entity count is exactly datasets × aspects.
        c.datahub.emit_flows = false;
    })
    .await
}

/// `n` datasets, one record each ⇒ 4 aspects per dataset (properties,
/// operation, profile, schema).
const ASPECTS_PER_DATASET: usize = 4;

async fn seed_datasets(state: &AppState, n: usize) {
    for i in 0..n {
        state
            .datasets
            .upsert_trusted("fake", &format!("d{i}"), "k1", &json!({"v": i}), None)
            .await
            .expect("seed record");
    }
}

/// The anti-pattern: one giant ingestion POST, where a single oversized payload
/// fails the whole emission.
#[tokio::test]
async fn ingestion_is_batched_at_25_not_one_giant_post() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = state_for(&gms).await;
    // 7 datasets × 4 aspects = 28 entities ⇒ 25 + 3.
    seed_datasets(&state, 7).await;

    let summary = match crate::datahub::full_sync(&state).await {
        SyncOutcome::Ran(v) => v,
        SyncOutcome::Busy => panic!("nothing else is syncing"),
    };
    assert_eq!(summary["ok"], true, "summary: {summary}");
    assert_eq!(summary["entities"], 7 * ASPECTS_PER_DATASET);
    assert_eq!(gms.batch_sizes(), vec![25, 3]);
}

/// A batch failure aborts the remainder — and the recorded error must say what
/// already landed, because the earlier batches are at GMS with no rollback and
/// (by design) no retry.
#[tokio::test]
async fn a_failed_batch_aborts_the_rest_and_reports_what_already_landed() {
    // First batch OK, second rejected. A third would mean "kept going".
    let gms = MockGms::spawn(vec![200, 500], Duration::ZERO).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 15).await; // 60 entities ⇒ 25 / 25 / 10

    let summary = match crate::datahub::full_sync(&state).await {
        SyncOutcome::Ran(v) => v,
        SyncOutcome::Busy => panic!("nothing else is syncing"),
    };
    assert_eq!(summary["ok"], false, "summary: {summary}");
    assert_eq!(
        gms.requests(),
        2,
        "the failing batch must abort the emission, not continue through it"
    );
    let err = summary["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("partial: 25 of 60 entities already ingested"),
        "the error must name what already landed, got: {err}"
    );

    // The failure is filed as the last ERROR and counted, and the status view
    // exposes both slots independently.
    let status = crate::datahub::status(&state);
    assert_eq!(status["emissions"]["failed"], 1);
    assert_eq!(status["emissions"]["ok"], 0);
    assert!(status["emissions"]["last_error"]["error"]
        .as_str()
        .unwrap()
        .contains("partial: 25 of 60"));
    assert!(status["emissions"]["last_success"].is_null());
}

/// The anti-pattern this replaces: `on_job_success` bare-`tokio::spawn`ing its
/// emission, so the shutdown drain (which only knows about the fan-out pool)
/// exited over an in-flight emission without waiting for it or counting it.
/// Now the emission IS a fan-out unit: visible to `inflight()`, and the drain
/// either finishes it or reports it as abandoned.
#[tokio::test]
async fn a_job_emission_is_tracked_by_the_drain_not_silently_detached() {
    let gms = MockGms::spawn(vec![], Duration::from_millis(300)).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 2).await;
    let job = state
        .storage
        .enqueue("fake", pumper_core::EnqueueOptions::default())
        .await
        .expect("enqueue");

    crate::datahub::on_job_success(&state, &job, Vec::new()).await;
    assert_eq!(
        state.fanout.inflight(),
        1,
        "the emission must be a tracked fan-out unit, not a detached spawn"
    );

    // What the shutdown drain does: wait, bounded — and here it completes, so
    // the metadata actually reached GMS instead of vanishing with the process.
    assert_eq!(state.fanout.drain(Duration::from_secs(10)).await, 0);
    assert!(gms.requests() >= 1, "the drained emission must have posted");
    assert_eq!(crate::datahub::status(&state)["emissions"]["ok"], 1);
}

/// The anti-pattern: two `/datahub/sync` calls running at once — double GMS
/// load, and two lineage read-merges that can interleave into lost edges.
#[tokio::test]
async fn a_second_sync_during_one_in_flight_is_rejected_not_run() {
    // Slow GMS so the first sync is provably still in flight.
    let gms = MockGms::spawn(vec![], Duration::from_millis(400)).await;
    let (state, _store) = state_for(&gms).await;
    seed_datasets(&state, 7).await;

    let first = tokio::spawn({
        let state = state.clone();
        async move { matches!(crate::datahub::full_sync(&state).await, SyncOutcome::Ran(_)) }
    });
    // Wait until the first sync is actually talking to GMS.
    wait_for(
        "the first sync to reach GMS",
        Duration::from_secs(5),
        || {
            let gms_requests = gms.requests();
            async move { gms_requests > 0 }
        },
    )
    .await;

    assert!(
        matches!(crate::datahub::full_sync(&state).await, SyncOutcome::Busy),
        "a concurrent full sync must be rejected (409), not doubled"
    );
    assert_eq!(
        crate::datahub::status(&state)["emissions"]["sync_running"],
        true
    );

    assert!(first.await.expect("first sync task"), "first sync must run");
    // Rejection is not a wedge: the slot is free once the first one finishes.
    assert!(matches!(
        crate::datahub::full_sync(&state).await,
        SyncOutcome::Ran(_)
    ));
}

// ── governance: preview + durable audit ─────────────────────────────────────

/// A state wired to `gms` with the governance actuator in the requested mode.
/// `govern = false` is a real case, not a placeholder: the preview must work
/// before the switch is flipped.
async fn govern_state_for(
    gms: &MockGms,
    govern: bool,
) -> (AppState, pumper_core::testing::TempStore) {
    let url = gms.url();
    test_state_with(vec![Arc::new(FakeApp)], move |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = url;
        c.datahub.govern = govern;
        c.datahub.emit_flows = false;
    })
    .await
}

/// Runs one governance poll to completion. The interval is aged first so this
/// works for the second and third poll of a test, not just the first.
async fn poll_once(state: &AppState, gms: &MockGms) {
    let before = gms.graphql_reads();
    {
        let mut g = state.datahub_govern.lock().unwrap();
        g.last_poll = std::time::Instant::now().checked_sub(Duration::from_secs(3600));
    }
    crate::datahub::govern_tick(state);
    wait_for(
        "the governance poll to finish",
        Duration::from_secs(15),
        || {
            let reads = gms.graphql_reads();
            let idle = !state.datahub_govern.lock().unwrap().in_flight;
            async move { reads > before && idle }
        },
    )
    .await;
}

/// The one catalog-managed schedule governance is allowed to touch, plus a
/// hand-made one it must never touch.
async fn seed_schedules(state: &AppState) -> (String, String) {
    let managed = state
        .storage
        .create_managed_schedule("fake", "0 * * * *", pumper_core::CATALOG_MANAGED_BY)
        .await
        .expect("catalog schedule");
    let hand = state
        .storage
        .create_schedule(pumper_core::NewSchedule {
            app: "fake",
            cron: "30 * * * *",
            params: json!({}),
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once",
            max_attempts: None,
            budget_usd: None,
        })
        .await
        .expect("hand-made schedule");
    (managed.id, hand.id)
}

/// The anti-pattern: the first poll after `govern = true` acting immediately,
/// with no way to see what it was about to do. The preview reads the same remote
/// state and reports the same plan — while `govern` is still OFF, and while
/// touching nothing.
#[tokio::test]
async fn preview_reports_what_governance_would_do_without_doing_any_of_it() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = govern_state_for(&gms, false).await;
    seed_datasets(&state, 2).await;
    gms.set_remote("fake.d0", remote_state(true, true, false));
    gms.set_remote("fake.d1", remote_state(false, false, true));
    let (managed, hand) = seed_schedules(&state).await;

    let p = crate::datahub::governance_preview(&state).await;
    assert_eq!(p["governing"], false, "the preview must not need govern on");
    assert_eq!(p["quiet"], false, "preview: {p}");
    assert_eq!(p["poll_would_abort"], false);

    let disable = &p["would"]["disable_schedules"][0];
    assert_eq!(disable["app"], "fake");
    assert_eq!(disable["dataset"], "d0");
    assert_eq!(
        disable["schedule_ids"],
        json!([managed]),
        "only the catalog-managed row may be named; the hand-made one ({hand}) is sacred"
    );
    assert_eq!(p["would"]["pause_apps"], json!(["fake"]));
    let sync = &p["would"]["enqueue_syncs"][0];
    assert_eq!(
        (&sync["app"], &sync["dataset"]),
        (&json!("fake"), &json!("d1"))
    );
    assert_eq!(sync["registered"], true);
    assert!(sync["idempotency_key"]
        .as_str()
        .unwrap()
        .starts_with("datahub-govern-sync:fake:d1:"));

    // …and nothing happened: the schedule is still on, nothing is paused, and
    // the audit trail is empty because no action was executed.
    assert!(
        state
            .storage
            .get_schedule(&managed)
            .await
            .unwrap()
            .unwrap()
            .enabled,
        "a preview must not disable anything"
    );
    assert_eq!(
        crate::datahub::status(&state)["govern"]["paused_apps"],
        json!([])
    );
    assert!(state
        .storage
        .list_datahub_govern_actions(10)
        .await
        .unwrap()
        .is_empty());
}

/// A GMS that cannot be read aborts a real poll entirely. The preview must say
/// so out loud rather than reporting an empty plan that looks like "all quiet".
#[tokio::test]
async fn a_read_error_is_reported_by_the_preview_not_silently_quiet() {
    // A GMS nobody is listening on: every governance read fails.
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = "http://127.0.0.1:1".into();
        c.datahub.emit_flows = false;
    })
    .await;
    seed_datasets(&state, 1).await;
    let p = crate::datahub::governance_preview(&state).await;
    assert_eq!(p["totals"]["read_errors"], 1, "preview: {p}");
    assert_eq!(
        p["poll_would_abort"], true,
        "a real poll aborts on the first read error — the preview must say so"
    );
    assert_eq!(p["quiet"], true);
}

/// The anti-pattern: an executed governance action living ONLY in
/// `GovernState.last` — erased by the next poll and by every restart, while the
/// schedule it disabled stays disabled with no recorded reason.
#[tokio::test]
async fn an_executed_action_is_audited_durably_not_only_in_memory() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = govern_state_for(&gms, true).await;
    seed_datasets(&state, 1).await;
    gms.set_remote("fake.d0", remote_state(true, true, false));
    let (managed, _hand) = seed_schedules(&state).await;

    let mut events = state.events.subscribe();
    poll_once(&state, &gms).await;

    // The action happened…
    assert!(
        !state
            .storage
            .get_schedule(&managed)
            .await
            .unwrap()
            .unwrap()
            .enabled,
        "the deprecation must have disabled the catalog-managed schedule"
    );
    // …and left a durable row naming what, on which app, and on what evidence.
    let rows = state.storage.list_datahub_govern_actions(10).await.unwrap();
    let disable = rows
        .iter()
        .find(|r| r.action == "disable_schedule")
        .expect("a disable must be audited");
    assert_eq!(disable.target, "fake");
    assert_eq!(disable.dataset.as_deref(), Some("d0"));
    assert_eq!(disable.subject.as_deref(), Some(managed.as_str()));
    assert_eq!(disable.evidence, "deprecation");
    let pause = rows
        .iter()
        .find(|r| r.action == "pause_app")
        .expect("entering the paused set must be audited");
    assert_eq!(pause.evidence, "cost:pause");

    // The trail is on the status surface…
    let status = crate::datahub::status_json(&state).await;
    assert_eq!(
        status["govern"]["recent_actions"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
        rows.len()
    );
    // …and every executed action also reached the event bus.
    let mut seen = Vec::new();
    while let Ok((_, ev)) = events.try_recv() {
        if ev.status == crate::datahub::GOVERN_EVENT_STATUS {
            seen.push(ev.result.clone().unwrap_or(Value::Null));
        }
    }
    assert!(
        seen.iter().any(|e| e["action"] == "disable_schedule"),
        "governance actions must reach the bus, not only the log: {seen:?}"
    );
}

// ── governance: reversible + outage-safe ────────────────────────────────────

async fn schedule_enabled(state: &AppState, id: &str) -> bool {
    state
        .storage
        .get_schedule(id)
        .await
        .expect("schedule read")
        .expect("schedule exists")
        .enabled
}

/// The anti-pattern: governance acting on the LEVEL of the deprecation flag, so
/// an operator who re-enabled a schedule had it disabled again within the poll
/// interval — forever, with no override. Governance now acts on the CHANGE.
#[tokio::test]
async fn a_manual_re_enable_survives_the_next_poll_until_datahub_changes() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = govern_state_for(&gms, true).await;
    seed_datasets(&state, 1).await;
    gms.set_remote("fake.d0", remote_state(true, false, false));
    let (managed, _hand) = seed_schedules(&state).await;

    // Poll 1: the transition disables it.
    poll_once(&state, &gms).await;
    assert!(!schedule_enabled(&state, &managed).await);

    // The operator disagrees and turns it back on.
    state
        .storage
        .set_managed_schedule_enabled(&managed, true, pumper_core::CATALOG_MANAGED_BY)
        .await
        .expect("operator re-enable");

    // Poll 2, with the deprecation flag STILL standing: respected.
    poll_once(&state, &gms).await;
    assert!(
        schedule_enabled(&state, &managed).await,
        "an unchanged deprecation must not undo an operator's re-enable"
    );

    // DataHub un-deprecates, then deprecates again: THAT is a change, and
    // governance acts on it.
    gms.set_remote("fake.d0", remote_state(false, false, false));
    poll_once(&state, &gms).await;
    assert!(schedule_enabled(&state, &managed).await);
    gms.set_remote("fake.d0", remote_state(true, false, false));
    poll_once(&state, &gms).await;
    assert!(
        !schedule_enabled(&state, &managed).await,
        "a NEW deprecation must act again"
    );
}

/// The invariant a restart must not break: the last-acted level is persisted
/// (migration 0038), not in-memory, so a fresh process does not re-disable a
/// schedule the remote merely still wants disabled.
#[tokio::test]
async fn a_restart_does_not_re_disable_a_schedule_the_operator_re_enabled() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = govern_state_for(&gms, true).await;
    seed_datasets(&state, 1).await;
    gms.set_remote("fake.d0", remote_state(true, false, false));
    let (managed, _hand) = seed_schedules(&state).await;

    poll_once(&state, &gms).await;
    assert!(!schedule_enabled(&state, &managed).await);
    state
        .storage
        .set_managed_schedule_enabled(&managed, true, pumper_core::CATALOG_MANAGED_BY)
        .await
        .expect("operator re-enable");

    // A restart: same store, brand-new governance memory.
    let restarted = AppState {
        datahub_govern: Default::default(),
        ..state.clone()
    };
    poll_once(&restarted, &gms).await;
    assert!(
        schedule_enabled(&restarted, &managed).await,
        "after a restart, an UNCHANGED deprecation must not disable the schedule again"
    );
}

/// The anti-pattern: the poll aborting on the first read error before it can
/// recompute `paused_apps`, so an app paused just before a DataHub outage sat
/// at budget $0 for the whole outage — the tag could not be un-read while GMS
/// was down, and only a restart cleared it.
#[tokio::test]
async fn a_pause_expires_loudly_when_the_outage_outlasts_the_staleness_window() {
    let gms = MockGms::spawn(vec![], Duration::ZERO).await;
    let (state, _store) = govern_state_for(&gms, true).await;
    seed_datasets(&state, 1).await;
    gms.set_remote("fake.d0", remote_state(false, true, false));

    poll_once(&state, &gms).await;
    assert_eq!(
        crate::datahub::status(&state)["govern"]["paused_apps"],
        json!(["fake"])
    );
    assert_eq!(
        crate::datahub::effective_budget(&state, "fake", Some(5.0)),
        Some(0.0),
        "the pause must really be zeroing the budget"
    );

    // GMS goes down. One poll inside the staleness window keeps the pause —
    // a blip must not un-pause anything.
    gms.set_outage(true);
    poll_once(&state, &gms).await;
    assert_eq!(
        crate::datahub::status(&state)["govern"]["paused_apps"],
        json!(["fake"]),
        "a short outage must not drop the pause"
    );

    // The outage outlasts `govern_pause_max_stale_secs` (900s by default).
    {
        let mut g = state.datahub_govern.lock().unwrap();
        g.last_success = std::time::Instant::now().checked_sub(Duration::from_secs(3600));
    }
    poll_once(&state, &gms).await;
    assert_eq!(
        crate::datahub::status(&state)["govern"]["paused_apps"],
        json!([]),
        "governance that has gone blind must stop enforcing, not freeze at $0 forever"
    );
    assert_eq!(
        crate::datahub::effective_budget(&state, "fake", Some(5.0)),
        Some(5.0)
    );
    // Loudly: an audit row names the expiry and why.
    let rows = state
        .storage
        .list_datahub_govern_actions(20)
        .await
        .expect("audit rows");
    let expiry = rows
        .iter()
        .find(|r| r.action == "expire_pause")
        .expect("the expiry must be audited, not silent");
    assert_eq!(
        (expiry.target.as_str(), expiry.evidence.as_str()),
        ("fake", "stale")
    );

    // And it is not permanent: once GMS answers again, the tag re-pauses.
    gms.set_outage(false);
    poll_once(&state, &gms).await;
    assert_eq!(
        crate::datahub::status(&state)["govern"]["paused_apps"],
        json!(["fake"])
    );
}

/// The anti-pattern: `govern_tick` stamping `last_poll` when the poll STARTED,
/// so a poll slower than the interval overlapped the next one and two tasks
/// raced to write `paused_apps`. Completion now gates the next poll, and an
/// in-flight poll blocks a tick outright — proven here with a GMS that hangs
/// its GraphQL reads while the interval is artificially aged past due.
#[tokio::test]
async fn a_tick_during_a_hanging_poll_does_not_start_a_second_poll() {
    let gms = MockGms::spawn_with(vec![], Duration::ZERO, Duration::from_secs(3)).await;
    let url = gms.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], move |c| {
        c.datahub.enabled = true;
        c.datahub.gms_url = url;
        c.datahub.govern = true;
    })
    .await;
    seed_datasets(&state, 1).await;

    crate::datahub::govern_tick(&state);
    wait_for("the poll to reach GMS", Duration::from_secs(5), || {
        let reads = gms.graphql_reads();
        async move { reads > 0 }
    })
    .await;

    // Age the completion stamp well past the interval, so the ONLY thing that
    // can hold this tick back is the in-flight guard.
    {
        let mut g = state.datahub_govern.lock().unwrap();
        assert!(g.in_flight, "the first poll must be marked in flight");
        g.last_poll = std::time::Instant::now().checked_sub(Duration::from_secs(3600));
    }
    crate::datahub::govern_tick(&state);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        gms.graphql_reads(),
        1,
        "a tick during an in-flight poll must not start a second poll"
    );

    // And the guard is not a wedge: completion clears it and re-stamps.
    wait_for("the poll to finish", Duration::from_secs(15), || {
        let done = !state.datahub_govern.lock().unwrap().in_flight;
        async move { done }
    })
    .await;
    let g = state.datahub_govern.lock().unwrap();
    assert!(
        g.last_poll
            .is_some_and(|t| t.elapsed() < Duration::from_secs(60)),
        "completion must re-stamp last_poll, so the interval restarts from the END"
    );
}
