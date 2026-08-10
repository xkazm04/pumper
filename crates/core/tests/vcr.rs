//! End-to-end VCR through the AppContext choke point: a record-mode context
//! persists its fetches/research into the cassette artifact; a replay-mode
//! context serves them back with **panicking engines** wired — proof that
//! replay never touches the network — at $0 spend; a MISS is the typed error.
//!
//! Plus **attempt integrity**: a job can run more than once, and all its
//! attempts share one cassette path. Which attempt's recording survives is the
//! difference between a deterministic replay and a confident lie.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, ScriptedResearcher, TempStore, TestContext};
use pumper_core::vcr::{Cassette, CassetteStart, Recorder, Vcr, ENTRY_CAP_BYTES};
use pumper_core::{
    Error, FetchRequest, HttpClient, HttpRequest, HttpResponse, ResearchRequest, Result,
};

const GOOD_PAGE: &str = "<html><body><article>A perfectly ordinary page with plenty of \
    real readable content, well past the two-hundred-and-fifty character default \
    threshold the fetcher uses for its escalation decisions, so the http tier wins \
    outright and the recorded outcome is a clean http-engine result.</article></body></html>";

/// Serves `GOOD_PAGE` for every URL.
struct StubHttp;
#[async_trait]
impl HttpClient for StubHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: std::collections::HashMap::new(),
            body: GOOD_PAGE.into(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

fn artifacts(store: &TempStore) -> PathBuf {
    store.path().join("artifacts").join("vcr-test").join("job")
}

#[tokio::test]
async fn record_then_replay_serves_the_recorded_fetch_without_engines_or_spend() {
    let store = TempStore::new("vcr-e2e").await;
    let dir = artifacts(&store);

    // --- Record: a live-ish fetch through the context writes the cassette.
    let ctx = TestContext::new(&store.storage, "vcr-test")
        .engines(engines_with(
            Arc::new(StubHttp),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .artifacts_dir(dir.clone())
        .vcr(Vcr::Record(Arc::new(Recorder::new(dir.clone()))))
        .build();
    let out = ctx
        .fetch(FetchRequest::new("https://example.test/page"))
        .await
        .unwrap();
    assert_eq!(out.engine, "http");
    assert!(
        dir.join("cassette.ndjson").is_file(),
        "cassette artifact written"
    );

    // --- Replay: Dead engines panic on ANY engine call — replay must not make one.
    let recorded_job = uuid::Uuid::new_v4();
    let cassette = Cassette::load(&dir, recorded_job).await.unwrap();
    assert_eq!(cassette.len(), 1);
    let replay_ctx = TestContext::new(&store.storage, "vcr-test")
        .artifacts_dir(store.path().join("artifacts").join("vcr-test").join("job2"))
        .vcr(Vcr::Replay(Arc::new(cassette)))
        .build();
    let replayed = replay_ctx
        .fetch(FetchRequest::new("https://example.test/page"))
        .await
        .unwrap();
    assert_eq!(replayed.engine, "http");
    assert_eq!(replayed.html.as_deref(), Some(GOOD_PAGE));
    assert!(replayed.cost_usd.is_none(), "replay spends nothing");
    assert!(replayed.trace[0]
        .detail
        .as_deref()
        .unwrap()
        .contains(&recorded_job.to_string()));

    // The replayed call is marked in the cost ledger at $0.
    let events = replay_ctx
        .costs
        .job_events(replay_ctx.job_id)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].cost_usd, 0.0);
    assert_eq!(events[0].detail.as_deref(), Some("vcr_replay"));
}

#[tokio::test]
async fn replay_miss_is_typed_and_never_falls_through_to_a_live_fetch() {
    let store = TempStore::new("vcr-miss").await;
    let dir = artifacts(&store);
    // Record one URL...
    let ctx = TestContext::new(&store.storage, "vcr-test")
        .engines(engines_with(
            Arc::new(StubHttp),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .artifacts_dir(dir.clone())
        .vcr(Vcr::Record(Arc::new(Recorder::new(dir.clone()))))
        .build();
    ctx.fetch(FetchRequest::new("https://example.test/recorded"))
        .await
        .unwrap();
    // ...then replay a DIFFERENT one with panicking engines: the typed miss
    // must surface before any engine is consulted (Dead would panic).
    let cassette = Cassette::load(&dir, uuid::Uuid::new_v4()).await.unwrap();
    let replay_ctx = TestContext::new(&store.storage, "vcr-test")
        .vcr(Vcr::Replay(Arc::new(cassette)))
        .build();
    let err = replay_ctx
        .fetch(FetchRequest::new("https://example.test/UNRECORDED"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReplayMiss(_)), "got: {err}");
    assert!(err.to_string().contains("UNRECORDED"));
}

#[tokio::test]
async fn research_records_and_replays_at_zero_cost() {
    let store = TempStore::new("vcr-research").await;
    let dir = artifacts(&store);
    // Record: a scripted model answer, with a real recorded cost.
    let researcher = ScriptedResearcher::new().on(
        "extract",
        pumper_core::ResearchOutput {
            text: "the recorded answer".into(),
            json: None,
            cost_usd: Some(0.37),
            duration_ms: Some(1200),
            num_turns: Some(2),
            session_id: None,
        },
    );
    let ctx = TestContext::new(&store.storage, "vcr-test")
        .engines(engines_with(
            Arc::new(Dead),
            Arc::new(Dead),
            Arc::new(researcher),
        ))
        .artifacts_dir(dir.clone())
        .vcr(Vcr::Record(Arc::new(Recorder::new(dir.clone()))))
        .build();
    let req = ResearchRequest::new("extract the fields from https://example.test/page");
    let out = ctx.research(req.clone()).await.unwrap();
    assert_eq!(out.cost_usd, Some(0.37));

    // Replay: Dead researcher panics if driven — the cassette must serve it.
    // (Shadowing `ctx` keeps the llm-chokepoint inventory honest: both calls
    // below ARE the chokepoint, `AppContext::research`.)
    let cassette = Cassette::load(&dir, uuid::Uuid::new_v4()).await.unwrap();
    let ctx = TestContext::new(&store.storage, "vcr-test")
        .vcr(Vcr::Replay(Arc::new(cassette)))
        .build();
    let replayed = ctx.research(req.clone()).await.unwrap();
    assert_eq!(replayed.text, "the recorded answer");
    assert_eq!(replayed.cost_usd, Some(0.0), "$0 engine spend on replay");

    // A DIFFERENT prompt is a typed miss, never a live model call.
    let err = ctx
        .research(ResearchRequest::new("a prompt that was never recorded"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::ReplayMiss(_)), "got: {err}");
}

// ── Attempt integrity ───────────────────────────────────────────────────────

/// A page whose body identifies the attempt that fetched it, padded past the
/// fetcher's 250-char escalation floor so the http tier wins outright.
fn attempt_page(tag: &str) -> String {
    format!(
        "<html><body><article>{tag} — {}</article></body></html>",
        "padding so the http tier is satisfied and never escalates. ".repeat(6)
    )
}

/// Serves a body that names the attempt currently running.
struct AttemptHttp(String);

#[async_trait]
impl HttpClient for AttemptHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: std::collections::HashMap::new(),
            body: attempt_page(&self.0),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// Runs one recording attempt: `tag` names it, `urls` are what it fetches, and
/// `recorder` carries the attempt's cassette-start policy.
async fn record_attempt(
    store: &TempStore,
    dir: &Path,
    recorder: Recorder,
    tag: &str,
    urls: &[&str],
) {
    recorder.prepare().await;
    let ctx = TestContext::new(&store.storage, "vcr-test")
        .engines(engines_with(
            Arc::new(AttemptHttp(tag.to_string())),
            Arc::new(Dead),
            Arc::new(Dead),
        ))
        .artifacts_dir(dir.to_path_buf())
        .vcr(Vcr::Record(Arc::new(recorder)))
        .build();
    for url in urls {
        ctx.fetch(FetchRequest::new(*url)).await.unwrap();
    }
}

/// **The anti-pattern.** Attempt 1 records part of the run and fails; attempt 2
/// runs the whole thing and succeeds. Because the recorder appended and the
/// loader takes the FIRST recording of each request, the cassette used to hand
/// a replay attempt 1's data — the data from the run that failed — while
/// reporting itself as a faithful reproduction of the job.
#[tokio::test]
async fn retry_does_not_replay_failed_attempt() {
    let store = TempStore::new("vcr-retry").await;
    let dir = artifacts(&store);

    // Attempt 1: gets through two URLs, then (notionally) dies.
    record_attempt(
        &store,
        &dir,
        Recorder::new(dir.clone()),
        "attempt-1",
        &["https://example.test/a", "https://example.test/b"],
    )
    .await;

    // Attempt 2: a fresh start (no checkpoint restored) — completes the run.
    record_attempt(
        &store,
        &dir,
        Recorder::new(dir.clone()),
        "attempt-2",
        &[
            "https://example.test/a",
            "https://example.test/b",
            "https://example.test/c",
        ],
    )
    .await;

    let job = uuid::Uuid::new_v4();
    let cassette = Cassette::load(&dir, job).await.unwrap();
    assert_eq!(
        cassette.len(),
        3,
        "the cassette holds attempt 2's run, not both runs' entries"
    );
    for url in [
        "https://example.test/a",
        "https://example.test/b",
        "https://example.test/c",
    ] {
        let entry = cassette.resolve("GET", url, url).unwrap();
        let body = entry.body.as_ref().unwrap()["html"].as_str().unwrap();
        assert!(
            body.contains("attempt-2") && !body.contains("attempt-1"),
            "{url} replays the FAILED attempt's data: {body}"
        );
    }
}

/// The other half of the rule, and the one a naive "always truncate" gets
/// wrong: a resumed attempt does not re-fetch what the earlier attempt already
/// did, so wiping the cassette would leave replay MISSes for fetches the job
/// genuinely made. The shutdown-suspend path re-queues without even burning an
/// attempt, and this is the behaviour it gets: **resume, not restart**.
#[tokio::test]
async fn a_resumed_attempt_keeps_the_work_it_is_not_redoing() {
    let store = TempStore::new("vcr-resume").await;
    let dir = artifacts(&store);

    record_attempt(
        &store,
        &dir,
        Recorder::new(dir.clone()),
        "attempt-1",
        &["https://example.test/page-1"],
    )
    .await;
    // Resumed from a checkpoint: picks up at page 2 and never re-fetches page 1.
    record_attempt(
        &store,
        &dir,
        Recorder::resuming(dir.clone()),
        "attempt-2",
        &["https://example.test/page-2"],
    )
    .await;

    let cassette = Cassette::load(&dir, uuid::Uuid::new_v4()).await.unwrap();
    assert_eq!(cassette.len(), 2, "both halves of the job are replayable");
    let first = cassette
        .resolve("GET", "https://example.test/page-1", "page-1")
        .expect("the suspended attempt's fetch must survive the resume");
    assert!(first.body.as_ref().unwrap()["html"]
        .as_str()
        .unwrap()
        .contains("attempt-1"));
}

/// An attempt that fetches NOTHING must still not leave the previous attempt's
/// cassette standing in for a run it never made. That is why the start policy
/// is applied eagerly at attempt start (`prepare`) rather than only on the
/// first recorded entry.
#[tokio::test]
async fn an_attempt_that_records_nothing_still_clears_the_stale_cassette() {
    let store = TempStore::new("vcr-empty-attempt").await;
    let dir = artifacts(&store);

    record_attempt(
        &store,
        &dir,
        Recorder::new(dir.clone()),
        "attempt-1",
        &["https://example.test/a"],
    )
    .await;
    assert!(Cassette::load(&dir, uuid::Uuid::new_v4()).await.is_ok());

    // Attempt 2 starts fresh and fails before its first fetch.
    Recorder::new(dir.clone()).prepare().await;

    let err = Cassette::load(&dir, uuid::Uuid::new_v4())
        .await
        .expect_err("a job with no recorded fetches has nothing to replay");
    assert!(matches!(err, Error::ReplayMiss(_)), "got: {err}");
}

/// The 128 MiB total cap must bind on the cassette actually on disk. The
/// per-`Recorder` byte counter used to start at zero on every attempt, so a
/// resuming attempt believed it had the whole budget again and the cap could be
/// exceeded arbitrarily by retrying.
#[tokio::test]
async fn the_total_cap_binds_on_the_file_not_on_a_fresh_counter() {
    let store = TempStore::new("vcr-cap-across-attempts").await;
    let dir = artifacts(&store);
    // A cap big enough for one entry, not two.
    let total = 900;

    record_attempt(
        &store,
        &dir,
        Recorder::with_caps(dir.clone(), ENTRY_CAP_BYTES, total),
        "attempt-1",
        &["https://example.test/a"],
    )
    .await;
    let on_disk = tokio::fs::metadata(dir.join("cassette.ndjson"))
        .await
        .unwrap()
        .len();
    assert!(on_disk > 0 && on_disk < total as u64, "one entry fits");

    // The resuming attempt inherits those bytes and must run out of budget.
    record_attempt(
        &store,
        &dir,
        Recorder::with_caps_starting(dir.clone(), ENTRY_CAP_BYTES, total, CassetteStart::Resume),
        "attempt-2",
        &["https://example.test/b"],
    )
    .await;

    let cassette = Cassette::load(&dir, uuid::Uuid::new_v4()).await.unwrap();
    let err = cassette
        .resolve("GET", "https://example.test/b", "b")
        .expect_err("the second entry must be an honest truncated marker, not a cap breach");
    assert!(err.to_string().contains("truncated"), "got: {err}");
}
