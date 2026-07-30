//! End-to-end VCR through the AppContext choke point: a record-mode context
//! persists its fetches/research into the cassette artifact; a replay-mode
//! context serves them back with **panicking engines** wired — proof that
//! replay never touches the network — at $0 spend; a MISS is the typed error.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, ScriptedResearcher, TempStore, TestContext};
use pumper_core::vcr::{Cassette, Recorder, Vcr};
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
