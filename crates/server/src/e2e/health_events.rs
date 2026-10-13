//! Source health, told and enforced — end to end through `run_one`.
//!
//! Two halves:
//!
//! - **Tell somebody.** A run whose verdict moves a source INTO `degraded` /
//!   `quarantined` / `retired` queues one `source.*` delivery to `[webhooks]
//!   health_url`. The verdict rows are seeded directly (`source_runs`), because
//!   `FakeApp` does not call `observe_extraction` and the thing under test is
//!   the worker's read of them, not the detector.
//! - **Close the seams.** With `[resilience] enforce = true`, a degraded
//!   source's documents neither alert saved searches nor fan out through a
//!   materialized view, and a degraded run's job-result echo stays out of the
//!   index. With `enforce = false` every one of those behaves exactly as before.

use std::sync::{Arc, Mutex};

use pumper_core::resilience::{source_id, SourceState};
use pumper_core::{
    EnqueueOptions, Result, Search, SearchDoc, SearchHit, SearchMaterialize, SearchRequest,
    SearchResponse,
};
use serde_json::{json, Value};
use uuid::Uuid;

use super::harness::{test_state_indexed, test_state_with, FakeApp, TestReceiver};
use crate::state::AppState;
use crate::worker;

// ── fixtures ─────────────────────────────────────────────────────────────────

/// Seeds one `source_runs` row — the verdict a run recorded.
async fn seed_run(
    state: &AppState,
    source: &str,
    job_id: &str,
    state_after: &str,
    created_at: &str,
    diagnosis: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO source_runs (source_id, job_id, docs, fetch_ok_rate, verdict, diagnosis, \
                                  score, state_after, created_at) \
         VALUES (?1, ?2, 50, 1.0, 'broken', ?3, 1.0, ?4, ?5)",
    )
    .bind(source)
    .bind(job_id)
    .bind(diagnosis)
    .bind(state_after)
    .bind(created_at)
    .execute(&state.storage.pool())
    .await
    .expect("seed source_runs row");
}

async fn enqueue_fake(state: &AppState) -> Uuid {
    state
        .storage
        .enqueue(
            "fake",
            EnqueueOptions {
                params: json!({}),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue")
        .id
}

async fn deliveries_of_kind(state: &AppState, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries WHERE kind = ?1")
        .bind(kind)
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}

async fn set_state(state: &AppState, app: &str, dataset: &str, to: SourceState) {
    let store = state.health.store().expect("enabled health store");
    store.ensure_source(app, dataset).await.unwrap();
    assert!(store
        .set_state_manual(&source_id(app, dataset), to, "test fixture")
        .await
        .unwrap());
}

/// Runs one job whose own verdict row on `fake/d` lands in `after`, with the
/// source's previous run (another job) having landed in `before`.
async fn run_transition(state: &AppState, before: Option<&str>, after: &str) -> Uuid {
    let store = state.health.store().expect("enabled health store");
    store.ensure_source("fake", "d").await.unwrap();
    let src = source_id("fake", "d");
    if let Some(before) = before {
        let prior = Uuid::new_v4().to_string();
        seed_run(
            state,
            &src,
            &prior,
            before,
            "2026-01-01T00:00:00.000Z",
            None,
        )
        .await;
    }
    let job = enqueue_fake(state).await;
    seed_run(
        state,
        &src,
        &job.to_string(),
        after,
        "2026-01-02T00:00:00.000Z",
        Some("markup_drift"),
    )
    .await;
    // What `commit_run` does on an actual transition: `state_since` takes the
    // run's own `created_at`. A run that moved nothing leaves it alone.
    if before != Some(after) {
        stamp_transition(state, &src, after, "2026-01-02T00:00:00.000Z").await;
    }
    assert!(worker::run_one(state).await, "one queued job");
    job
}

async fn stamp_transition(state: &AppState, src: &str, to: &str, at: &str) {
    sqlx::query("UPDATE sources SET state = ?2, state_since = ?3 WHERE id = ?1")
        .bind(src)
        .bind(to)
        .bind(at)
        .execute(&state.storage.pool())
        .await
        .expect("stamp transition");
}

// ── tell somebody ────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_healthy_to_degraded_run_sends_exactly_one_source_degraded() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = true;
        c.webhooks.health_url = Some(url);
        c.webhooks.health_secret = Some("hs".into());
    })
    .await;

    let job = run_transition(&state, Some("healthy"), "degraded").await;

    let hits = rx.hits_so_far();
    assert_eq!(hits.len(), 1, "exactly one delivery for one transition");
    let (headers, body) = &hits[0];
    assert_eq!(headers["x-pumper-event"], "source.degraded");
    assert!(
        headers.contains_key("x-pumper-signature"),
        "health_secret set → signed"
    );
    let payload: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(
        payload,
        json!({
            "source_id": "fake/d",
            "app": "fake",
            "dataset": "d",
            "from": "healthy",
            "to": "degraded",
            "reason": "markup_drift",
            "job_id": job.to_string(),
            "at": "2026-01-02T00:00:00.000Z",
        })
    );
    assert_eq!(deliveries_of_kind(&state, "health").await, 1);
}

/// The anti-pattern: re-alerting every run a source spends degraded.
#[tokio::test]
async fn a_run_that_stays_degraded_sends_nothing() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = true;
        c.webhooks.health_url = Some(url);
    })
    .await;

    run_transition(&state, Some("degraded"), "degraded").await;

    assert!(rx.hits_so_far().is_empty(), "no transition, no delivery");
    assert_eq!(deliveries_of_kind(&state, "health").await, 0);
}

/// The operator overrode the source to `degraded` between runs (the manual
/// path announces that itself). The next run stays `degraded`. Its prior RUN
/// was healthy, but this run did not move the stored state, so no alert.
#[tokio::test]
async fn a_run_after_a_manual_override_to_the_same_state_sends_nothing() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = true;
        c.webhooks.health_url = Some(url);
    })
    .await;
    let src = source_id("fake", "d");
    state
        .health
        .store()
        .unwrap()
        .ensure_source("fake", "d")
        .await
        .unwrap();
    seed_run(
        &state,
        &src,
        &Uuid::new_v4().to_string(),
        "healthy",
        "2026-01-01T00:00:00.000Z",
        None,
    )
    .await;
    stamp_transition(&state, &src, "degraded", "2026-01-01T12:00:00.000Z").await;
    let job = enqueue_fake(&state).await;
    seed_run(
        &state,
        &src,
        &job.to_string(),
        "degraded",
        "2026-01-02T00:00:00.000Z",
        None,
    )
    .await;
    assert!(worker::run_one(&state).await);

    assert!(rx.hits_so_far().is_empty(), "the override was the entry");
    assert_eq!(deliveries_of_kind(&state, "health").await, 0);
}

#[tokio::test]
async fn no_health_url_sends_nothing() {
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = true;
    })
    .await;

    run_transition(&state, Some("healthy"), "degraded").await;

    let all: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_deliveries")
        .fetch_one(&state.storage.pool())
        .await
        .unwrap();
    assert_eq!(all, 0, "nobody configured, nobody told");
}

/// `[resilience] enabled = false` must cost nothing and send nothing — even
/// with a `health_url` configured and verdict rows present in the table.
#[tokio::test]
async fn detection_disabled_sends_nothing_even_with_a_url() {
    let rx = TestReceiver::spawn(vec![]).await;
    let url = rx.url();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], |c| {
        c.resilience.enabled = false;
        c.webhooks.health_url = Some(url);
    })
    .await;
    assert!(state.health.store().is_none());

    let src = source_id("fake", "d");
    seed_run(
        &state,
        &src,
        &Uuid::new_v4().to_string(),
        "healthy",
        "2026-01-01T00:00:00.000Z",
        None,
    )
    .await;
    let job = enqueue_fake(&state).await;
    seed_run(
        &state,
        &src,
        &job.to_string(),
        "degraded",
        "2026-01-02T00:00:00.000Z",
        None,
    )
    .await;
    assert!(worker::run_one(&state).await);

    assert!(rx.hits_so_far().is_empty());
    assert_eq!(deliveries_of_kind(&state, "health").await, 0);
}

// ── close the seams ──────────────────────────────────────────────────────────

/// An index that answers every query with fixed hits, and records what it was
/// asked to index.
struct FixedSearch {
    hits: Vec<SearchHit>,
    indexed: Arc<Mutex<Vec<SearchDoc>>>,
}

#[async_trait::async_trait]
impl Search for FixedSearch {
    async fn index(&self, docs: Vec<SearchDoc>) -> Result<()> {
        self.indexed.lock().unwrap().extend(docs);
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse {
            hits: self.hits.clone(),
            total: self.hits.len() as u64,
            ..Default::default()
        })
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(0)
    }
}

fn hit(app: &str, dataset: &str, key: &str) -> SearchHit {
    SearchHit {
        id: format!("{app}:{dataset}:{key}"),
        app: app.into(),
        dataset: dataset.into(),
        url: format!("https://x/{key}"),
        title: format!("doc {key}"),
        score: 1.0,
        snippet: String::new(),
    }
}

/// One mixed-source saved search with a materialized view and a watch on the
/// view; returns `(alert hits, view-watch hits, view record count)`.
async fn run_saved_search_fixture(enforce: bool) -> (Vec<Value>, Vec<Value>, usize) {
    let search = Arc::new(FixedSearch {
        hits: vec![hit("bad", "d", "1"), hit("ok", "d", "2")],
        indexed: Arc::default(),
    });
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.resilience.enabled = true;
        c.resilience.enforce = enforce;
    })
    .await;
    set_state(&state, "bad", "d", SourceState::Degraded).await;

    let alerts = TestReceiver::spawn(vec![]).await;
    let view_watch = TestReceiver::spawn(vec![]).await;
    state
        .storage
        .create_saved_search(
            "doc",
            None,
            None,
            &alerts.url(),
            None,
            Some(&SearchMaterialize {
                app: "view".into(),
                dataset: "hits".into(),
            }),
        )
        .await
        .unwrap();
    state
        .storage
        .create_watch("view", "hits", &view_watch.url(), None, "webhook")
        .await
        .unwrap();

    enqueue_fake(&state).await;
    assert!(worker::run_one(&state).await);

    let parse = |rx: &TestReceiver| -> Vec<Value> {
        rx.hits_so_far()
            .iter()
            .map(|(_, b)| serde_json::from_slice(b).unwrap())
            .collect()
    };
    let view_records = state
        .datasets
        .list("view", "hits", 100)
        .await
        .unwrap()
        .len();
    (parse(&alerts), parse(&view_watch), view_records)
}

fn ids(payload: &Value, array: &str, field: &str) -> Vec<String> {
    let mut out: Vec<String> = payload[array]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m[field].as_str().unwrap().to_string())
        .collect();
    out.sort();
    out
}

#[tokio::test]
async fn enforced_degraded_source_neither_alerts_nor_fans_out_through_a_view() {
    let (alerts, view_watch, view_records) = run_saved_search_fixture(true).await;

    assert_eq!(alerts.len(), 1, "the healthy source still alerts");
    assert_eq!(alerts[0]["event"], "search.matched");
    assert_eq!(
        ids(&alerts[0], "matches", "id"),
        vec!["ok:d:2"],
        "the degraded source's hit must not alert"
    );

    assert_eq!(view_watch.len(), 1, "the healthy delta still fans out");
    assert_eq!(
        ids(&view_watch[0], "changes", "key"),
        vec!["ok:d:2"],
        "the degraded source's view delta must not reach the view's watch"
    );

    assert_eq!(
        view_records, 2,
        "suppression gates pushes, never the view's stored records"
    );
}

#[tokio::test]
async fn soak_mode_alerts_and_fans_out_for_a_degraded_source_unchanged() {
    let (alerts, view_watch, view_records) = run_saved_search_fixture(false).await;

    assert_eq!(alerts.len(), 1);
    assert_eq!(ids(&alerts[0], "matches", "id"), vec!["bad:d:1", "ok:d:2"]);
    assert_eq!(view_watch.len(), 1);
    assert_eq!(
        ids(&view_watch[0], "changes", "key"),
        vec!["bad:d:1", "ok:d:2"]
    );
    assert_eq!(view_records, 2);
}

/// The job-result echo (`_job` / `_records`) of a run judged on a degraded
/// pair; returns the echo docs the index was handed.
async fn run_echo_fixture(enforce: bool, judged: SourceState) -> Vec<SearchDoc> {
    let indexed: Arc<Mutex<Vec<SearchDoc>>> = Arc::default();
    let search = Arc::new(FixedSearch {
        hits: vec![],
        indexed: indexed.clone(),
    });
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.resilience.enabled = true;
        c.resilience.enforce = enforce;
    })
    .await;
    set_state(&state, "fake", "d", judged).await;
    let job = enqueue_fake(&state).await;
    seed_run(
        &state,
        &source_id("fake", "d"),
        &job.to_string(),
        judged.as_str(),
        "2026-01-02T00:00:00.000Z",
        None,
    )
    .await;
    assert!(worker::run_one(&state).await);

    let docs = indexed.lock().unwrap().clone();
    docs.into_iter()
        .filter(|d| {
            d.dataset == worker::JOB_RESULT_DATASET || d.dataset == worker::JOB_RECORD_DATASET
        })
        .collect()
}

#[tokio::test]
async fn an_enforced_degraded_run_does_not_index_its_echo() {
    assert!(
        run_echo_fixture(true, SourceState::Degraded)
            .await
            .is_empty(),
        "a run judged on a degraded pair must not index its echo"
    );
    assert!(
        run_echo_fixture(true, SourceState::Quarantined)
            .await
            .is_empty(),
        "…nor a quarantined one"
    );
}

#[tokio::test]
async fn the_echo_is_indexed_in_soak_mode_and_for_a_healthy_run() {
    assert_eq!(
        run_echo_fixture(false, SourceState::Degraded).await.len(),
        1,
        "enforce = false: the `_job` snapshot is indexed exactly as before"
    );
    assert_eq!(
        run_echo_fixture(true, SourceState::Suspect).await.len(),
        1,
        "a pair that may index does not withhold the echo"
    );
}
