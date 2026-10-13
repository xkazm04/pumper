//! The job heartbeat must never starve its own job of pool connections.
//!
//! THE DEFECT (found by the Director's smoke on the built binary): the worker's
//! heartbeat tick AWAITED `Storage::heartbeat` inside the job's `select!`. While
//! that arm ran, the app future was not polled — and the app's in-flight
//! queries (a 16-wide fetch fan-out over an 8-connection pool) each held a
//! connection that only polling could release. The beat waited the full 30s
//! acquire timeout for a connection its own job was holding; every fetch in
//! the wave timed out with it. A 40-URL run fetched 0-7 of 40, at random,
//! enrolled or not.
//!
//! These run the REAL stack — `AppState::init` (HttpEngine + HttpCache +
//! governor + the 8-connection pool), the real worker seam `run_one`, the real
//! extractor — against a local fixture server that behaves like
//! `python -m http.server` (Last-Modified, 304 on If-Modified-Since), with a
//! heartbeat interval short enough that ticks are certain to land mid fan-out.
//!
//! The reaper property is pinned too: the fix spawns the write, but the TICK
//! still needs the job loop to be polled, so a wedged job stops beating.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::Path as AxPath;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use pumper_core::storage::EnqueueOptions;
use pumper_core::{AppContext, Result, ScrapeApp};
use serde_json::{json, Value};

use super::harness::test_state_with;
use crate::state::AppState;

const LAST_MODIFIED: &str = "Mon, 01 Jan 2024 00:00:00 GMT";

/// Heartbeat interval for the reproduction: the first tick fires immediately
/// and the fixture's per-response delay stretches each fan-out past a second,
/// so several ticks land while fetches (and their queries) are in flight.
const TEST_HEARTBEAT_SECS: u64 = 1;
const RESPONSE_DELAY: Duration = Duration::from_millis(150);

fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("apps")
        .join("extractor")
        .join("tests")
        .join("fixtures")
        .join("hn")
}

async fn serve_fixture(
    AxPath(name): AxPath<String>,
    headers: HeaderMap,
) -> axum::response::Response {
    tokio::time::sleep(RESPONSE_DELAY).await;
    if headers.get("if-modified-since").is_some() {
        return StatusCode::NOT_MODIFIED.into_response();
    }
    match std::fs::read_to_string(fixtures_dir().join(&name)) {
        Ok(body) => (
            [
                ("content-type", "text/html"),
                ("last-modified", LAST_MODIFIED),
            ],
            body,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Serves the 40 HN fixtures; returns their URLs.
async fn fixture_server() -> Vec<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = axum::Router::new().route("/{name}", axum::routing::get(serve_fixture));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let mut urls: Vec<String> = std::fs::read_dir(fixtures_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".html"))
        .map(|n| format!("{base}/{n}"))
        .collect();
    urls.sort();
    assert_eq!(urls.len(), 40, "the HN fixture set");
    urls
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pumper-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A real `AppState` (real engines, real pool) under a scratch dir.
async fn live_state(dir: &std::path::Path) -> AppState {
    let mut config = pumper_core::config::Config::default();
    config.storage.database_path = dir.join("live.db");
    config.storage.artifacts_dir = dir.join("artifacts");
    config.search.dir = dir.join("search");
    config.plugins.enabled = false;
    config.plugins.dir = dir.join("plugins");
    config.browser.user_data_dir = dir.join("browser");
    config.fetcher.profiles_dir = dir.join("profiles");
    // Local fixtures: politeness would only stretch the wall clock.
    config.governor.default_rps = 200.0;
    config.governor.jitter_ms = 0;
    config.worker.heartbeat_secs = TEST_HEARTBEAT_SECS;
    AppState::init(config).await.expect("init live state")
}

/// Enqueues one extractor job and runs it through the real worker seam.
async fn run_job(state: &AppState, params: Value) -> Value {
    let job = state
        .storage
        .enqueue(
            "extractor",
            EnqueueOptions {
                params,
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(crate::worker::run_one(state).await, "job claimed");
    let row = state.storage.get(job.id).await.unwrap().unwrap();
    row.result.unwrap_or_else(|| json!({"error": row.error}))
}

fn rules() -> Value {
    serde_json::from_str(&std::fs::read_to_string(fixtures_dir().join("ruleset.json")).unwrap())
        .unwrap()
}

fn fetched(out: &Value) -> (Value, Value) {
    (out["fetched"].clone(), out["fetch_ok_rate"].clone())
}

/// (a) CONTROL shape: inline run, then the same inline run again (a revisit).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_heartbeat_mid_fan_out_starves_no_fetch_not_a_frozen_job() {
    let dir = scratch_dir("hb-control");
    let state = live_state(&dir).await;
    let urls = fixture_server().await;
    let params = json!({"rules": rules(), "urls": urls, "dataset": "hn"});
    let first = run_job(&state, params.clone()).await;
    let second = run_job(&state, params).await;
    eprintln!(
        "CONTROL run1 {:?} run2 {:?}",
        fetched(&first),
        fetched(&second)
    );
    assert_eq!(first["fetched"], 40, "{first}");
    assert_eq!(first["fetch_ok_rate"], 1.0, "{first}");
    assert_eq!(second["fetched"], 40, "{second}");
    assert_eq!(second["fetch_ok_rate"], 1.0, "{second}");
}

/// (b) The smoke's enrolment shape: inline run, import, `rules_ref` run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_enrolled_revisit_fetches_every_url_not_a_starved_fraction() {
    let dir = scratch_dir("hb-enrolled");
    let state = live_state(&dir).await;
    let urls = fixture_server().await;
    let first = run_job(
        &state,
        json!({"rules": rules(), "urls": urls, "dataset": "hn"}),
    )
    .await;
    let parsed: pumper_core::RuleSet = serde_json::from_value(rules()).unwrap();
    let hash = state.datasets.register_ruleset(&parsed).await.unwrap();
    state
        .datasets
        .import_ruleset("extractor/hn", &hash)
        .await
        .unwrap();
    let second = run_job(
        &state,
        json!({"rules_ref": "active", "urls": urls, "dataset": "hn"}),
    )
    .await;
    eprintln!(
        "CASE run1 {:?} run2 {:?}",
        fetched(&first),
        fetched(&second)
    );
    assert_eq!(first["fetched"], 40, "{first}");
    assert_eq!(second["fetched"], 40, "{second}");
    assert_eq!(second["fetch_ok_rate"], 1.0, "{second}");
    assert_eq!(second["enrolled"]["bodies_pinned"], 40, "{second}");
}

#[test]
fn a_beat_in_flight_skips_the_tick_not_queues_a_second_waiter() {
    assert!(crate::worker::should_beat(false));
    assert!(!crate::worker::should_beat(true));
}

/// Yields once, then either blocks its thread (a non-yielding wedge) or
/// sleeps cooperatively (slow but alive) for `hold`.
struct Holds {
    wedge: bool,
    hold: Duration,
}

#[async_trait]
impl ScrapeApp for Holds {
    fn name(&self) -> &'static str {
        "holds"
    }
    fn description(&self) -> &'static str {
        "test app that holds its slot"
    }
    async fn run(&self, _ctx: AppContext) -> Result<Value> {
        tokio::task::yield_now().await;
        if self.wedge {
            std::thread::sleep(self.hold);
        } else {
            tokio::time::sleep(self.hold).await;
        }
        Ok(json!({"ok": true}))
    }
}

async fn heartbeat_at(state: &AppState, id: uuid::Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT heartbeat_at FROM jobs WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(&state.storage.pool())
        .await
        .unwrap()
}

/// Runs a `Holds` job and samples its heartbeat at 1.5s and 3.5s.
async fn sample_beats(wedge: bool) -> (Option<String>, Option<String>) {
    let app = Arc::new(Holds {
        wedge,
        hold: Duration::from_secs(5),
    });
    let (state, _store) = test_state_with(vec![app], |c| c.worker.heartbeat_secs = 1).await;
    let job = state
        .storage
        .enqueue("holds", EnqueueOptions::default())
        .await
        .unwrap();
    let runner = {
        let state = state.clone();
        tokio::spawn(async move { crate::worker::run_one(&state).await })
    };
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let early = heartbeat_at(&state, job.id).await;
    tokio::time::sleep(Duration::from_millis(2000)).await;
    let late = heartbeat_at(&state, job.id).await;
    runner.await.unwrap();
    (early, late)
}

/// The reaper property, preserved: spawning the write must not decouple the
/// beat from the job loop. A wedged (non-yielding) job stops beating, so its
/// lease goes stale and the reaper recovers it; a slow-but-alive job keeps
/// beating.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wedged_job_stops_beating_but_a_slow_one_keeps_its_lease() {
    let (early, late) = sample_beats(true).await;
    assert!(early.is_some());
    assert_eq!(
        early, late,
        "a wedged job must stop beating (reaper backstop)"
    );

    let (early, late) = sample_beats(false).await;
    assert!(
        late > early,
        "a slow-but-alive job keeps beating: {early:?} -> {late:?}"
    );
}
