//! `POST /fetch-proxy` (M17 remote fetch fabric, serving side) plus a full
//! coordinator->node round-trip over loopback: RemoteEngine -> live axum
//! server -> local (stub) HTTP engine -> envelope back.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::config::{Config, GovernorConfig, RemoteConfig};
use pumper_core::testing::{engines_with, Dead, TempStore};
use pumper_core::{
    Error, Governor, HttpClient, HttpRequest, HttpResponse, NoPlugins, NoSearch, Result,
};
use pumper_engine_remote::{RemoteEngine, REMOTE_SECRET_HEADER};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::routes;
use crate::state::{AppState, AppStateParts};

/// Local-stack stub standing in for the real HttpEngine: records every request
/// it serves and answers with a canned body.
struct RecordingHttp {
    seen: Mutex<Vec<HttpRequest>>,
}

impl RecordingHttp {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
        })
    }
    fn seen(&self) -> Vec<HttpRequest> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl HttpClient for RecordingHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        self.seen.lock().unwrap().push(req.clone());
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::from([("x-origin".into(), "node-local".into())]),
            body: "<html>fetched by the node's local stack</html>".into(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// A headless state whose engine set carries `http` as its HTTP engine and
/// whose `[remote]` is configured by `remote`.
async fn proxy_state(http: Arc<dyn HttpClient>, remote: RemoteConfig) -> (AppState, TempStore) {
    let store = TempStore::new("fetch-proxy-e2e").await;
    let mut config = Config::default();
    config.storage.database_path = store.path().join("pumper.db");
    config.storage.artifacts_dir = store.path().join("artifacts");
    // Hermetic session vault: the profile guard reads this directory, so a
    // developer who happens to have `data/profiles/acme` on disk must not be
    // able to flip these tests.
    config.fetcher.profiles_dir = store.path().join("profiles");
    config.remote = remote;
    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor: Arc::new(Governor::new(&GovernorConfig::default())),
        engines: engines_with(http, Arc::new(Dead), Arc::new(Dead)),
        plugins: Arc::new(NoPlugins),
        search: Arc::new(NoSearch),
        registry: HashMap::new(),
    })
    .expect("assemble fetch-proxy test state");
    (state, store)
}

fn enabled_remote() -> RemoteConfig {
    RemoteConfig {
        enabled: true,
        nodes: Vec::new(), // serve-only node
        secret: "sesame".into(),
        timeout_secs: 45,
        max_body_bytes: 2 * 1024 * 1024,
        ..RemoteConfig::default()
    }
}

async fn post_proxy(
    router: &axum::Router,
    secret: Option<&str>,
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/fetch-proxy")
        .header("content-type", "application/json");
    if let Some(s) = secret {
        builder = builder.header(REMOTE_SECRET_HEADER, s);
    }
    let resp = router
        .clone()
        .oneshot(
            builder
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn proxy_runs_the_request_through_the_local_stack_and_returns_the_envelope() {
    let http = RecordingHttp::new();
    let (state, _store) = proxy_state(http.clone(), enabled_remote()).await;
    let router = routes::router(state);

    let (status, body) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://target.example/page" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], 200);
    assert_eq!(
        body["body"],
        "<html>fetched by the node's local stack</html>"
    );
    assert_eq!(body["final_url"], "https://target.example/page");
    assert_eq!(body["headers"]["x-origin"], "node-local");

    // The node's local engine saw the inner request, with the [remote] caps
    // stamped on (absent in the inner request => the node ceilings apply).
    let seen = http.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].url, "https://target.example/page");
    assert_eq!(seen[0].max_body_bytes, Some(2 * 1024 * 1024));
    assert_eq!(seen[0].timeout_secs, Some(45));
}

/// The anti-pattern: **a peer answering a profiled fetch logged out**. This test
/// used to assert the opposite — that `profile: "acme"` was threaded through to
/// the node's engine — which pinned the leak as the contract. A node that does
/// not hold the session cannot serve the session; the coordinator falls back to
/// the node that does, and pays one extra fetch instead of storing a login wall
/// as a dataset revision.
#[tokio::test]
async fn a_profile_this_node_does_not_hold_is_refused_not_fetched_logged_out() {
    let http = RecordingHttp::new();
    let (state, _store) = proxy_state(http.clone(), enabled_remote()).await;
    let router = routes::router(state);

    let (status, body) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://target.example/page", "profile": "acme" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "unprocessable");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("acme"),
        "{body}"
    );
    assert!(
        http.seen().is_empty(),
        "the refusal must happen BEFORE the fetch — a 200 carrying the logged-out page is \
         exactly what this guard exists to prevent"
    );
}

/// ...and the guard is a *presence* check, not a blanket ban: a node that really
/// holds the jar still serves the fetch under it.
#[tokio::test]
async fn a_profile_this_node_does_hold_is_served_under_it() {
    let http = RecordingHttp::new();
    let (state, store) = proxy_state(http.clone(), enabled_remote()).await;
    let jar = store.path().join("profiles").join("acme");
    std::fs::create_dir_all(&jar).unwrap();
    std::fs::write(jar.join("cookies.json"), "[]").unwrap();
    let router = routes::router(state);

    let (status, body) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://target.example/page", "profile": "acme" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(http.seen()[0].profile.as_deref(), Some("acme"));
}

/// The anti-pattern: **a node that will fetch its own API for whoever holds the
/// shared secret**. Every other route here is unauthenticated because the bind
/// is loopback — and a fabric node has to bind off loopback to be reachable at
/// all, so `/fetch-proxy` is the exact place that argument runs out.
///
/// Note what this asserts and what it does not: the guard reads the **target**
/// URL. The node addresses in this whole test file are `127.0.0.1`, and the
/// round-trip test below still passes — that is the check that this guard is
/// applied to the right URL rather than being inert.
#[tokio::test]
async fn a_node_refuses_to_fetch_its_own_loopback_api_for_a_peer() {
    let http = RecordingHttp::new();
    let (state, _store) = proxy_state(http.clone(), enabled_remote()).await;
    let router = routes::router(state);

    for target in [
        "http://127.0.0.1:8088/jobs",
        "http://localhost:8088/plugins/reload",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.5/internal",
        "file:///etc/passwd",
    ] {
        let (status, body) = post_proxy(&router, Some("sesame"), json!({ "url": target })).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{target}: {body}");
        assert_eq!(body["code"], "forbidden", "{target}");
    }
    assert!(
        http.seen().is_empty(),
        "a refused target must never reach the fetch stack"
    );
}

/// The opt-out is real: an operator who deliberately proxies a LAN gets their
/// private targets back — and still cannot make the node read a local file.
#[tokio::test]
async fn the_private_target_opt_out_relaxes_addresses_only() {
    let http = RecordingHttp::new();
    let mut remote = enabled_remote();
    remote.allow_private_targets = true;
    let (state, _store) = proxy_state(http.clone(), remote).await;
    let router = routes::router(state);

    let (status, body) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "http://10.0.0.5/internal" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(http.seen().len(), 1);

    let (status, _) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "file:///etc/passwd" }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(http.seen().len(), 1, "the scheme guard is not opt-outable");
}

#[tokio::test]
async fn peer_caps_are_clamped_never_raised() {
    let http = RecordingHttp::new();
    let (state, _store) = proxy_state(http.clone(), enabled_remote()).await;
    let router = routes::router(state);

    // A peer asking for MORE than the node allows is clamped down...
    let (status, _) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://t.example/", "max_body_bytes": 999_999_999u64, "timeout_secs": 3600 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // ...while a peer asking for LESS keeps its tighter bound.
    let (status, _) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://t.example/", "max_body_bytes": 1024, "timeout_secs": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let seen = http.seen();
    assert_eq!(seen[0].max_body_bytes, Some(2 * 1024 * 1024));
    assert_eq!(seen[0].timeout_secs, Some(45));
    assert_eq!(seen[1].max_body_bytes, Some(1024));
    assert_eq!(seen[1].timeout_secs, Some(5));
}

#[tokio::test]
async fn missing_or_wrong_secret_is_401_and_never_fetches() {
    let http = RecordingHttp::new();
    let (state, _store) = proxy_state(http.clone(), enabled_remote()).await;
    let router = routes::router(state);

    let (status, body) = post_proxy(&router, None, json!({ "url": "https://t.example/" })).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["code"], "unauthorized");

    let (status, _) = post_proxy(
        &router,
        Some("wrong"),
        json!({ "url": "https://t.example/" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    assert!(
        http.seen().is_empty(),
        "an unauthenticated call must never reach the local engine"
    );
}

#[tokio::test]
async fn disabled_fabric_is_404_even_with_the_right_secret() {
    let http = RecordingHttp::new();
    let mut remote = enabled_remote();
    remote.enabled = false;
    let (state, _store) = proxy_state(http.clone(), remote).await;
    let router = routes::router(state);

    let (status, _) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://t.example/" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(http.seen().is_empty());
}

/// The anti-pattern: **a failure status the caller's transport will retry**.
/// This used to be a `502`, which is in the shipped `[http] retryable_statuses`
/// — so the coordinator's own HTTP engine retried a *deterministic* peer-side
/// failure `[http] retries` more times, each paying this node's full fetch time
/// with exponential backoff between, before the fabric's failover ladder even
/// learned the node was bad.
#[tokio::test]
async fn a_failed_proxied_fetch_is_not_answered_with_a_retryable_status() {
    struct FailingHttp;
    #[async_trait::async_trait]
    impl HttpClient for FailingHttp {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            Err(Error::Http("connect timeout".into()))
        }
    }
    let (state, _store) = proxy_state(Arc::new(FailingHttp), enabled_remote()).await;
    let router = routes::router(state);
    let (status, body) = post_proxy(
        &router,
        Some("sesame"),
        json!({ "url": "https://t.example/" }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "unprocessable");
    assert!(
        !pumper_core::config::HttpConfig::default()
            .retryable_statuses
            .contains(&status.as_u16()),
        "a coordinator's transport would retry this status"
    );
}

/// Minimal reqwest transport for the loopback round-trip (the real deployment's
/// RemoteEngine transports through the governed HttpEngine).
struct PlainClient(reqwest::Client);

#[async_trait::async_trait]
impl HttpClient for PlainClient {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let mut builder = match req.method {
            pumper_core::HttpMethod::Get => self.0.get(&req.url),
            pumper_core::HttpMethod::Post => self.0.post(&req.url),
        };
        for (k, v) in &req.headers {
            builder = builder.header(k, v);
        }
        if let Some(body) = &req.body {
            builder = builder.body(body.clone());
        }
        let resp = builder
            .send()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;
        let status = resp.status().as_u16();
        let final_url = resp.url().to_string();
        let body = resp.text().await.map_err(|e| Error::Http(e.to_string()))?;
        Ok(HttpResponse {
            status,
            headers: HashMap::new(),
            body,
            final_url,
            cache_hit: false,
        })
    }
}

/// The full fabric loop over loopback: a coordinator-side RemoteEngine POSTs to
/// a REAL bound node (this router served by axum), whose local stub engine
/// serves the page; the envelope decodes back into an HttpResponse.
#[tokio::test]
async fn remote_engine_round_trips_through_a_live_node() {
    let node_http = RecordingHttp::new();
    let (state, _store) = proxy_state(node_http.clone(), enabled_remote()).await;
    let router = routes::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let coordinator_cfg = RemoteConfig {
        enabled: true,
        // A loopback NODE address — the target guard reads the target URL, not
        // this one, and the assertions below are what prove that distinction is
        // real rather than a comment.
        nodes: vec![format!("http://{addr}")],
        secret: "sesame".into(),
        timeout_secs: 30,
        max_body_bytes: 1024 * 1024,
        ..RemoteConfig::default()
    };
    // Local fallback must not be needed on the happy path.
    struct PanicLocal;
    #[async_trait::async_trait]
    impl HttpClient for PanicLocal {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            panic!("healthy node: the coordinator must not fall back to local");
        }
    }
    let engine = RemoteEngine::with_transport(
        &coordinator_cfg,
        Arc::new(PlainClient(reqwest::Client::new())),
        Arc::new(PanicLocal),
    );

    let resp = engine
        .fetch(HttpRequest::get("https://target.example/page"))
        .await
        .expect("round trip");
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, "<html>fetched by the node's local stack</html>");
    assert_eq!(resp.final_url, "https://target.example/page");
    assert_eq!(
        resp.headers.get("x-origin").map(String::as_str),
        Some("node-local")
    );
    // And the node's local stack really served it.
    assert_eq!(node_http.seen().len(), 1);
    assert_eq!(node_http.seen()[0].url, "https://target.example/page");
}
