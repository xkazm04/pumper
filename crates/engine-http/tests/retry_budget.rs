//! Live tests of the **retry loop itself** against a local hit-counting server.
//!
//! Until this file existed, nothing anywhere executed `HttpEngine::send`'s loop
//! more than once: `retry_delay` was tested exhaustively as a pure function, and
//! the cross-engine conformance battery runs with `retries: 0`. The most
//! cost-bearing algorithm in the crate — the one that decides how much wall
//! clock one hostile URL may spend — had zero end-to-end coverage.
//!
//! The governor is switched **off** in these fixtures on purpose: it penalises
//! 503s with its own doubling spacing, which would mix a second, unrelated
//! source of delay into every wall-clock assertion here. Politeness is tested in
//! `crates/core`; this file tests the ladder.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use pumper_core::config::{CacheConfig, GovernorConfig, HttpConfig, StorageConfig};
use pumper_core::{Governor, HttpCache, HttpClient, HttpRequest, Storage};
use pumper_engine_http::HttpEngine;

/// How the loopback origin behaves, and how many times it has been asked.
struct Origin {
    hits: AtomicUsize,
    /// Hits that answer 503 before the first 200. `usize::MAX` = never succeed.
    fail_first: usize,
    /// `Retry-After` (seconds) on each 503, or `None` to leave it off.
    retry_after: Option<u64>,
}

async fn handler(State(origin): State<Arc<Origin>>) -> impl IntoResponse {
    let n = origin.hits.fetch_add(1, Ordering::SeqCst);
    if n < origin.fail_first {
        let mut headers = HeaderMap::new();
        if let Some(secs) = origin.retry_after {
            headers.insert("retry-after", HeaderValue::from(secs));
        }
        return (StatusCode::SERVICE_UNAVAILABLE, headers, "slow down").into_response();
    }
    (StatusCode::OK, HeaderMap::new(), "the real body").into_response()
}

async fn spawn_origin(fail_first: usize, retry_after: Option<u64>) -> (String, Arc<Origin>) {
    let origin = Arc::new(Origin {
        hits: AtomicUsize::new(0),
        fail_first,
        retry_after,
    });
    let app = Router::new()
        .route("/resource", get(handler))
        .with_state(origin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/resource"), origin)
}

/// A real `HttpEngine` over a throwaway SQLite cache, with the governor off.
async fn new_engine(root: &Path, cfg: HttpConfig) -> HttpEngine {
    let storage = Storage::connect(&StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts"),
        ..StorageConfig::default()
    })
    .await
    .expect("storage");
    let cache = Arc::new(HttpCache::new(storage.pool(), &CacheConfig::default()));
    let governor = Arc::new(Governor::new(&GovernorConfig {
        enabled: false,
        ..GovernorConfig::default()
    }));
    std::mem::forget(storage); // leak the pool for the test's lifetime
    HttpEngine::new(&cfg, governor, cache, root.join("profiles")).expect("engine")
}

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "pumper-retry-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

/// The loop really does loop: two retryable statuses, then a win — three hits on
/// the origin from one `fetch()`, with the backoff sleeps in between.
#[tokio::test]
async fn a_retryable_status_ladder_runs_every_attempt_and_then_wins() {
    let root = temp_root("ladder");
    let (url, origin) = spawn_origin(2, None).await;
    let engine = new_engine(
        &root,
        HttpConfig {
            retries: 3,
            timeout_secs: 5,
            ..HttpConfig::default()
        },
    )
    .await;

    let started = Instant::now();
    let resp = engine
        .fetch(HttpRequest::get(&url))
        .await
        .expect("the third attempt succeeds");
    let elapsed = started.elapsed();

    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, "the real body");
    assert_eq!(
        origin.hits.load(Ordering::SeqCst),
        3,
        "one fetch must have made three attempts (503, 503, 200)"
    );
    // Backoff floors are 500 ms then 1 s (+ up to 25% jitter), so the whole
    // ladder is ~1.5-1.9 s. A generous ceiling: this asserts the sleeps happened
    // and that nothing multiplied them.
    assert!(
        elapsed >= Duration::from_millis(1_400) && elapsed < Duration::from_secs(8),
        "ladder took {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// THE amplification bug. A host answering `429/503 Retry-After: 600` used to
/// buy three sleeps of up to 750 s each on top of four attempts — **~37.5
/// minutes for one fetch**, past `[worker] job_timeout_secs` (900 s), so a
/// single hostile URL killed every other unit of work in its job.
///
/// The budget must refuse the sleep outright rather than truncate it (retrying
/// earlier than the server asked would trade a wall-clock bug for a politeness
/// one), so the fetch fails immediately after the first attempt.
#[tokio::test]
async fn a_ten_minute_retry_after_cannot_outlive_the_fetch_budget() {
    let root = temp_root("retryafter");
    let (url, origin) = spawn_origin(usize::MAX, Some(600)).await;
    let engine = new_engine(
        &root,
        HttpConfig {
            retries: 3,
            timeout_secs: 5,
            total_budget_secs: 10,
            ..HttpConfig::default()
        },
    )
    .await;

    let started = Instant::now();
    let err = engine
        .fetch(HttpRequest::get(&url))
        .await
        .expect_err("a permanently rate-limited host must not succeed");
    let elapsed = started.elapsed();

    assert_eq!(
        origin.hits.load(Ordering::SeqCst),
        1,
        "the 600 s sleep cannot fit the budget, so no second attempt is made"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the fetch must fail on its own clock, not sleep the budget away: {elapsed:?}"
    );
    let shown = err.to_string();
    assert!(
        shown.contains("total_budget_secs"),
        "the failure must name the knob that stopped it: {shown}"
    );
    assert!(shown.contains(&url), "and the URL that spent it: {shown}");
    // Retryable: a rate limit is a fact about a live host, not about the request.
    assert!(!err.is_terminal_for_job(), "{shown}");

    let _ = std::fs::remove_dir_all(&root);
}

/// THE transport-classification bug, in attempt counts. An unsupported scheme
/// used to burn `retries + 1` attempts and three governor slots re-deriving what
/// reqwest knew deterministically before the first socket — and then failed with
/// a *retryable* error, so the worker re-queued and ran the whole ladder again
/// on every job attempt.
#[tokio::test]
async fn a_deterministic_transport_failure_costs_one_attempt_not_the_whole_ladder() {
    let root = temp_root("badscheme");
    let (url, origin) = spawn_origin(usize::MAX, None).await;
    // The same live authority, with a scheme reqwest cannot speak: if any
    // attempt reached the network the origin's counter would move.
    let unspeakable = url.replacen("http://", "ftp://", 1);
    let engine = new_engine(
        &root,
        HttpConfig {
            retries: 3,
            timeout_secs: 5,
            ..HttpConfig::default()
        },
    )
    .await;

    let started = Instant::now();
    let err = engine
        .fetch(HttpRequest::get(&unspeakable))
        .await
        .expect_err("an ftp:// URL must be refused");
    let elapsed = started.elapsed();

    assert_eq!(
        origin.hits.load(Ordering::SeqCst),
        0,
        "the request was never sendable, so nothing may have reached the origin"
    );
    // The ladder's own sleeps are 0.5 s + 1 s + 2 s. Finishing inside half a
    // second is proof that exactly one iteration ran.
    assert!(
        elapsed < Duration::from_millis(500),
        "the backoff ladder must not have run at all: {elapsed:?}"
    );
    assert!(
        err.is_terminal_for_job(),
        "a URL that cannot be requested is frozen into the job row — every \
         attempt re-derives the same refusal: {err}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The mirror risk, and the reason the classifier is narrow: a *transient*
/// transport failure must still get every attempt it is configured for. Nothing
/// listens on loopback port 1, so this is a connect failure — the class that
/// bundles DNS, TLS and a service mid-restart, all left retryable on purpose.
#[tokio::test]
async fn a_transient_transport_failure_still_gets_every_configured_attempt() {
    let root = temp_root("refused");
    let engine = new_engine(
        &root,
        HttpConfig {
            retries: 2,
            timeout_secs: 5,
            total_budget_secs: 60,
            ..HttpConfig::default()
        },
    )
    .await;

    let started = Instant::now();
    let err = engine
        .fetch(HttpRequest::get("http://127.0.0.1:1/nothing-listens-here"))
        .await
        .expect_err("nothing listens on port 1");
    let elapsed = started.elapsed();

    // The engine reports the count itself, so this is the number, not a proxy.
    assert!(
        err.to_string().contains("failed after 3 attempts"),
        "a connect failure must still ride its full ladder: {err}"
    );
    assert!(
        !err.is_terminal_for_job(),
        "a connect failure may succeed later (resolver blip, service restart): {err}"
    );
    // 0.5 s + 1 s of backoff actually elapsed between those three attempts.
    assert!(
        elapsed >= Duration::from_millis(1_400),
        "the retries were not really spaced: {elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The budget also has to cut a ladder that is individually well-behaved: with
/// `retries = 20` and a 1 s `Retry-After`, every single sleep is reasonable and
/// the total is not. Before the deadline this fetch made 21 attempts.
#[tokio::test]
async fn the_budget_stops_a_long_ladder_of_individually_short_sleeps() {
    let root = temp_root("longladder");
    let (url, origin) = spawn_origin(usize::MAX, Some(1)).await;
    let engine = new_engine(
        &root,
        HttpConfig {
            retries: 20,
            timeout_secs: 5,
            total_budget_secs: 4,
            ..HttpConfig::default()
        },
    )
    .await;

    let started = Instant::now();
    let err = engine
        .fetch(HttpRequest::get(&url))
        .await
        .expect_err("a permanently 503 host must not succeed");
    let elapsed = started.elapsed();
    let hits = origin.hits.load(Ordering::SeqCst);

    assert!(
        (2..=6).contains(&hits),
        "the ladder must run more than once and stop well short of its 21 \
         configured attempts; made {hits}"
    );
    assert!(
        elapsed < Duration::from_secs(8),
        "one fetch stayed near its 4 s budget: {elapsed:?}"
    );
    assert!(err.to_string().contains("total_budget_secs"), "{err}");

    let _ = std::fs::remove_dir_all(&root);
}
