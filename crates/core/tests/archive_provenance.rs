//! Archive provenance, end to end through the seams a real consumer touches.
//!
//! The `[archive]` tier trades **freshness for availability**: when a host is
//! dead, blocked, or rate-limiting, pumper serves a stored snapshot instead.
//! That trade is only safe if the consumer can tell it happened — and until
//! 2026-08 it could not. `FETCHED_VIA_HEADER` / `SNAPSHOT_TS_HEADER` had one
//! writer (`engine-archive`) and **zero readers**: the tiered fetcher dropped
//! `HttpResponse.headers` on every tier, so a body served out of a 2019 capture
//! reached apps, receipts and cassettes byte-indistinguishable from today's page.
//!
//! These tests pin the whole chain rather than any one link:
//!   archive engine → `FetchOutcome.snapshot` → the job's cost-event receipt →
//!   the VCR cassette → a replayed outcome.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig};
use pumper_core::engine::{snapshot_provenance, FETCHED_VIA_HEADER, SNAPSHOT_TS_HEADER};
use pumper_core::governor::Governor;
use pumper_core::testing::{Dead, TempStore, TestContext};
use pumper_core::vcr::{fetch_entry, to_fetch_outcome};
use pumper_core::{
    EngineSet, Error, FetchRequest, FetchTier, Fetcher, HttpClient, HttpRequest, HttpResponse,
    Result, TierVerdict,
};

const CAPTURED_AT: &str = "2019-03-11T09:15:00+00:00";

const GOOD_PAGE: &str = "<html><body><article>A perfectly ordinary page with plenty of \
    real readable content, well past the two-hundred-and-fifty character default \
    threshold the fetcher uses for its escalation decisions, so whichever tier \
    serves it wins outright instead of escalating. The bytes are deliberately \
    identical whichever tier produced them, because the whole point of these \
    tests is that the body cannot tell you where it came from — only the \
    provenance can.</article></body></html>";

/// Stands in for `ArchiveEngine`: serves a snapshot body and marks it with the
/// two provenance headers, exactly as `engine-archive` does at its step 4.
struct MarkedArchive;
#[async_trait]
impl HttpClient for MarkedArchive {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let mut headers = HashMap::new();
        headers.insert(FETCHED_VIA_HEADER.to_string(), "archive".to_string());
        headers.insert(SNAPSHOT_TS_HEADER.to_string(), CAPTURED_AT.to_string());
        Ok(HttpResponse {
            status: 200,
            headers,
            body: GOOD_PAGE.into(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// A live origin serving the same bytes, unmarked.
struct LiveHttp;
#[async_trait]
impl HttpClient for LiveHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: GOOD_PAGE.into(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// An HTTP client that refuses to be reached — the archive tier must win before
/// any live request happens.
struct NoLive;
#[async_trait]
impl HttpClient for NoLive {
    async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
        Err(Error::http("the live tier must not be reached"))
    }
}

fn engines(http: Arc<dyn HttpClient>, archive: Option<Arc<dyn HttpClient>>) -> Arc<EngineSet> {
    let fetch = Fetcher::new(
        http.clone(),
        Arc::new(Dead),
        Arc::new(Dead),
        Arc::new(Governor::new(&GovernorConfig::default())),
        &FetcherConfig::default(),
    )
    .with_archive(archive);
    Arc::new(EngineSet::new(http, Arc::new(Dead), Arc::new(Dead), fetch))
}

/// THE user moment: *"I built a price dataset off a crawl. Half of it came from
/// archived snapshots because the host started blocking us, and nothing in the
/// data, the receipt, or the trace said so."*
///
/// One metered fetch through the app-facing chokepoint has to answer all three.
#[tokio::test]
async fn an_archived_fetch_is_not_indistinguishable_from_a_live_one_at_the_app_seam() {
    let store = TempStore::new("archive-prov").await;
    let ctx = TestContext::new(&store.storage, "arch")
        .engines(engines(Arc::new(NoLive), Some(Arc::new(MarkedArchive))))
        .build();

    let mut req = FetchRequest::new("https://example.test/price");
    req.archive_max_age = Some(86_400);
    let out = ctx.fetch(req).await.expect("archive tier serves");

    // 1. The data: a typed field, not a phrase in the escalation prose.
    let snapshot = out
        .snapshot
        .as_ref()
        .expect("archive win carries provenance");
    assert_eq!(snapshot.via, "archive");
    assert_eq!(snapshot.captured_at.as_deref(), Some(CAPTURED_AT));

    // 2. The trace: the winning tier's own entry names the capture.
    let winner = out
        .trace
        .iter()
        .find(|t| t.verdict == TierVerdict::Ok)
        .expect("a winning trace entry");
    assert_eq!(winner.tier, FetchTier::Archive);
    assert!(
        winner
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains("2019-03-11"),
        "trace detail was {:?}",
        winner.detail
    );

    // 3. The receipt: the job's cost event says which page-in-time it bought.
    let events = ctx.costs.job_events(ctx.job_id).await.expect("events");
    let fetch_event = events
        .iter()
        .find(|e| e.engine == "archive")
        .expect("the fetch is metered under the winning tier");
    assert!(
        fetch_event
            .detail
            .as_deref()
            .unwrap_or_default()
            .contains(CAPTURED_AT),
        "receipt detail was {:?}",
        fetch_event.detail
    );
}

/// The control: the same bytes off the live web leave no provenance anywhere,
/// so `is_some()` is a sound test for "this came out of a store".
#[tokio::test]
async fn a_live_fetch_leaves_no_snapshot_provenance() {
    let store = TempStore::new("archive-prov-live").await;
    let ctx = TestContext::new(&store.storage, "arch")
        .engines(engines(Arc::new(LiveHttp), Some(Arc::new(MarkedArchive))))
        .build();

    // No `archive_max_age` => tier zero is never attempted, even though wired.
    let out = ctx
        .fetch(FetchRequest::new("https://example.test/price"))
        .await
        .expect("live tier serves");
    assert_eq!(out.engine, "http");
    assert!(out.snapshot.is_none());

    let events = ctx.costs.job_events(ctx.job_id).await.expect("events");
    assert!(
        events
            .iter()
            .all(|e| !e.detail.as_deref().unwrap_or_default().contains("snapshot")),
        "a live fetch must not mention a snapshot: {events:?}"
    );
}

/// A cassette is replayed to reproduce a run exactly. If provenance did not
/// round-trip, every replayed archive fetch would come back claiming it was
/// live — a *new* lie, told confidently, in the one mode whose whole purpose is
/// fidelity. The cassette's header map (unused until now) carries it.
#[tokio::test]
async fn a_replayed_archive_fetch_does_not_come_back_looking_live() {
    let store = TempStore::new("archive-prov-vcr").await;
    let ctx = TestContext::new(&store.storage, "arch")
        .engines(engines(Arc::new(NoLive), Some(Arc::new(MarkedArchive))))
        .build();

    let mut req = FetchRequest::new("https://example.test/price");
    req.archive_max_age = Some(86_400);
    let out = ctx.fetch(req).await.expect("archive tier serves");

    let entry = fetch_entry(&out);
    assert_eq!(
        snapshot_provenance(&entry.headers).map(|p| p.via),
        Some("archive".to_string()),
        "the cassette entry has to carry the provenance out"
    );
    let replayed = to_fetch_outcome(&entry, uuid::Uuid::new_v4()).expect("replay");
    let snapshot = replayed
        .snapshot
        .as_ref()
        .expect("a replayed archive fetch is still an archive fetch");
    assert_eq!(snapshot.via, "archive");
    assert_eq!(snapshot.captured_at.as_deref(), Some(CAPTURED_AT));
}
