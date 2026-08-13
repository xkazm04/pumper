//! Remote fetch fabric, coordinator side (M17 v1): an [`HttpClient`] that ships
//! a serialized [`HttpRequest`] to a peer pumper node's `POST /fetch-proxy`
//! endpoint and gets the [`HttpResponse`] back as JSON. The peer runs the
//! request through its own **local** fetch stack — HTTP engine, politeness
//! governor, cache, body caps — so a proxied fetch is exactly as polite as a
//! local one, just from a different egress IP/geography.
//!
//! ## Wire format
//!
//! - Request: `POST <node>/fetch-proxy` with the [`HttpRequest`]'s own serde
//!   JSON as the body and the shared secret in [`REMOTE_SECRET_HEADER`].
//! - Response: the [`HttpResponse`] serde JSON (`status`, `headers`, `body`,
//!   `final_url`, `cache_hit`), deserialized here via the [`ProxyResponse`]
//!   mirror (kept field-for-field identical; a mismatch is a typed error, not
//!   a silent zero).
//!
//! ## Routing + fallback
//!
//! Nodes are tried by simple round-robin (an atomic cursor over `[remote]
//! nodes`). **Any** node failure — transport error, non-2xx proxy status,
//! unparseable envelope — falls back to the local engine for that fetch, so a
//! dead or misconfigured node degrades throughput, never correctness. With no
//! nodes configured the engine is a pure pass-through to local.
//!
//! The outbound proxy call itself runs through the **local** transport (the
//! real HTTP engine), so peer nodes are governed/spaced like any other host.
//! Cluster-wide governor state is deliberately OUT of this v1 — each node's
//! governor protects targets independently; the shared-brain merge is M01's
//! host-weather bundle, later.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::RemoteConfig;
use pumper_core::{Error, HttpClient, HttpMethod, HttpRequest, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Header carrying the cluster shared secret on every proxy call. The peer's
/// `/fetch-proxy` route rejects a missing/mismatched value with 401.
pub const REMOTE_SECRET_HEADER: &str = "x-pumper-remote-secret";

/// Path of the proxy endpoint on a peer node.
pub const FETCH_PROXY_PATH: &str = "/fetch-proxy";

/// Wire mirror of [`HttpResponse`] (which is deliberately `Serialize`-only in
/// core). Field names/types match its serde output exactly; `#[serde(default)]`
/// on the non-essential fields keeps a slightly-older peer's envelope readable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub final_url: String,
    #[serde(default)]
    pub cache_hit: bool,
}

impl From<ProxyResponse> for HttpResponse {
    fn from(p: ProxyResponse) -> Self {
        HttpResponse {
            status: p.status,
            headers: p.headers,
            body: p.body,
            final_url: p.final_url,
            cache_hit: p.cache_hit,
        }
    }
}

/// The envelope (JSON with an escaped `body` string) is bigger than the inner
/// body it carries; this slack keeps a body that fits the configured cap from
/// being rejected at the transport layer purely for its JSON escaping overhead.
const ENVELOPE_SLACK_BYTES: u64 = 64 * 1024;

/// Coordinator-side remote fetch engine. Construct over the local HTTP engine
/// (used both as the transport for proxy calls and as the fallback) and wire
/// into the tiered fetcher via `Fetcher::with_remote`.
pub struct RemoteEngine {
    /// Peer base URLs, trailing-slash-trimmed (`http://10.0.0.2:8088`).
    nodes: Vec<String>,
    /// Shared secret sent in [`REMOTE_SECRET_HEADER`].
    secret: String,
    /// Transport for the proxy POST itself — the local governed HTTP engine,
    /// so peers are spaced/retried like any host.
    transport: Arc<dyn HttpClient>,
    /// Local fallback: serves the fetch when no node can.
    local: Arc<dyn HttpClient>,
    /// Round-robin cursor over `nodes`.
    next: AtomicUsize,
    /// `[remote] timeout_secs`: per proxy call, end to end.
    timeout_secs: u64,
    /// `[remote] max_body_bytes`: cap on the proxied response body. The
    /// transport-level cap adds [`ENVELOPE_SLACK_BYTES`] of JSON headroom.
    max_body_bytes: u64,
}

impl RemoteEngine {
    /// `local` is used both to POST to peers and as the per-fetch fallback.
    pub fn new(cfg: &RemoteConfig, local: Arc<dyn HttpClient>) -> Self {
        Self::with_transport(cfg, local.clone(), local)
    }

    /// Split construction for tests (scripted transport, observable fallback).
    pub fn with_transport(
        cfg: &RemoteConfig,
        transport: Arc<dyn HttpClient>,
        local: Arc<dyn HttpClient>,
    ) -> Self {
        Self {
            nodes: cfg
                .nodes
                .iter()
                .map(|n| n.trim_end_matches('/').to_string())
                .filter(|n| !n.is_empty())
                .collect(),
            secret: cfg.secret.clone(),
            transport,
            local,
            next: AtomicUsize::new(0),
            timeout_secs: cfg.timeout_secs,
            max_body_bytes: cfg.max_body_bytes,
        }
    }

    /// The next node in round-robin order. `None` when no nodes are configured.
    fn pick_node(&self) -> Option<&str> {
        if self.nodes.is_empty() {
            return None;
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.nodes.len();
        Some(&self.nodes[idx])
    }

    /// One attempt against one node. Every failure mode is a typed error the
    /// caller turns into a local fallback.
    async fn try_node(&self, node: &str, req: &HttpRequest) -> Result<HttpResponse> {
        let payload = serde_json::to_string(req)
            .map_err(|e| Error::Http(format!("serialize proxied request: {e}")))?;
        let mut proxy = HttpRequest::get(format!("{node}{FETCH_PROXY_PATH}"));
        proxy.method = HttpMethod::Post;
        proxy.body = Some(payload);
        proxy
            .headers
            .insert(REMOTE_SECRET_HEADER.to_string(), self.secret.clone());
        proxy
            .headers
            .insert("content-type".to_string(), "application/json".to_string());
        // A proxied fetch must never be served from (or written to) the local
        // response cache under the proxy URL — the target URL lives in the body.
        proxy.no_cache = true;
        proxy.timeout_secs = Some(self.timeout_secs);
        proxy.max_body_bytes = Some(
            self.max_body_bytes
                .saturating_mul(2)
                .saturating_add(ENVELOPE_SLACK_BYTES),
        );
        let resp = self.transport.fetch(proxy).await?;
        if !resp.is_success() {
            return Err(Error::Http(format!(
                "node {node} answered /fetch-proxy with status {}",
                resp.status
            )));
        }
        let parsed: ProxyResponse = serde_json::from_str(&resp.body)
            .map_err(|e| Error::Http(format!("node {node} sent an unparseable envelope: {e}")))?;
        Ok(parsed.into())
    }
}

#[async_trait]
impl HttpClient for RemoteEngine {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let Some(node) = self.pick_node() else {
            return self.local.fetch(req).await;
        };
        let node = node.to_string();
        match self.try_node(&node, &req).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                warn!(node = %node, error = %e, "remote fetch node failed — falling back to local");
                self.local.fetch(req).await
            }
        }
    }

    /// Binary fetches are served **locally**, always — the same engine a fetch
    /// falls back to when every node is unreachable.
    ///
    /// `/fetch-proxy` speaks a JSON envelope whose `body` is a `String`, so a
    /// binary body cannot travel it at all; dispatching one to a peer could only
    /// mangle it. Serving locally keeps the substitution this engine makes what
    /// it claims to be — it changes *where* a fetch egresses from, never whether
    /// it succeeds.
    ///
    /// The anti-pattern this closes: a **decorator that silently drops a
    /// capability**. Inheriting the trait's default would have made every binary
    /// fetch through this position answer "this engine does not support binary
    /// fetch_bytes" — even though the engine it wraps supports it perfectly —
    /// and the only thing that changed was an operator turning `[remote]` on.
    async fn fetch_bytes(&self, req: HttpRequest) -> Result<Vec<u8>> {
        self.local.fetch_bytes(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    fn cfg(nodes: &[&str]) -> RemoteConfig {
        RemoteConfig {
            enabled: true,
            nodes: nodes.iter().map(|s| s.to_string()).collect(),
            secret: "sesame".into(),
            timeout_secs: 30,
            max_body_bytes: 1024 * 1024,
        }
    }

    fn envelope(body: &str) -> String {
        serde_json::to_string(&ProxyResponse {
            status: 200,
            headers: HashMap::from([("x-served-by".into(), "node".into())]),
            body: body.into(),
            final_url: "https://target.example/page".into(),
            cache_hit: false,
        })
        .unwrap()
    }

    /// Transport stub: records every request; answers from a script of
    /// `Result<(status, body)>`, repeating the last entry when exhausted.
    struct Scripted {
        seen: Mutex<Vec<HttpRequest>>,
        script: Mutex<Vec<Result<(u16, String)>>>,
    }

    impl Scripted {
        fn new(script: Vec<Result<(u16, String)>>) -> Arc<Self> {
            Arc::new(Self {
                seen: Mutex::new(Vec::new()),
                script: Mutex::new(script),
            })
        }
        fn seen(&self) -> Vec<HttpRequest> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpClient for Scripted {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            self.seen.lock().unwrap().push(req.clone());
            let mut script = self.script.lock().unwrap();
            let entry = if script.len() > 1 {
                script.remove(0)
            } else {
                script[0]
                    .as_ref()
                    .map(|ok| ok.clone())
                    .map_err(|e| Error::Http(e.to_string()))
            };
            let (status, body) = entry?;
            Ok(HttpResponse {
                status,
                headers: HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// Local stub that must never be reached (proxy-success paths).
    struct DeadLocal;
    #[async_trait]
    impl HttpClient for DeadLocal {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            panic!("local engine must not be called when the node answers");
        }
    }

    /// Local stub that serves a marker body (fallback paths).
    struct MarkerLocal;
    #[async_trait]
    impl HttpClient for MarkerLocal {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body: "served locally".into(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    #[tokio::test]
    async fn proxy_success_decodes_the_envelope_and_skips_local() {
        let transport = Scripted::new(vec![Ok((200, envelope("<html>remote body</html>")))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:8088/"]),
            transport.clone(),
            Arc::new(DeadLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "<html>remote body</html>");
        assert_eq!(resp.final_url, "https://target.example/page");
        assert_eq!(
            resp.headers.get("x-served-by").map(String::as_str),
            Some("node")
        );
    }

    #[tokio::test]
    async fn wire_request_carries_secret_caps_and_the_serialized_inner_request() {
        let transport = Scripted::new(vec![Ok((200, envelope("x")))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:8088/"]), // trailing slash trimmed
            transport.clone(),
            Arc::new(DeadLocal),
        );
        let mut inner = HttpRequest::get("https://target.example/page");
        inner.profile = Some("acme".into());
        engine.fetch(inner).await.unwrap();

        let seen = transport.seen();
        assert_eq!(seen.len(), 1);
        let proxy = &seen[0];
        assert_eq!(proxy.url, "http://node-a:8088/fetch-proxy");
        assert_eq!(proxy.method, HttpMethod::Post);
        assert_eq!(
            proxy.headers.get(REMOTE_SECRET_HEADER).map(String::as_str),
            Some("sesame")
        );
        assert!(proxy.no_cache, "a proxy call must bypass the local cache");
        assert_eq!(proxy.timeout_secs, Some(30));
        assert_eq!(
            proxy.max_body_bytes,
            Some(2 * 1024 * 1024 + ENVELOPE_SLACK_BYTES),
            "transport cap = 2x body cap + envelope slack"
        );
        // The body is the inner HttpRequest's own serde JSON — the peer
        // deserializes it straight back into an HttpRequest.
        let round: HttpRequest = serde_json::from_str(proxy.body.as_deref().unwrap()).unwrap();
        assert_eq!(round.url, "https://target.example/page");
        assert_eq!(round.profile.as_deref(), Some("acme"));
    }

    #[tokio::test]
    async fn nodes_rotate_round_robin() {
        let transport = Scripted::new(vec![Ok((200, envelope("x")))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1", "http://node-b:2", "http://node-c:3"]),
            transport.clone(),
            Arc::new(DeadLocal),
        );
        for _ in 0..4 {
            engine
                .fetch(HttpRequest::get("https://t.example/"))
                .await
                .unwrap();
        }
        let hosts: Vec<String> = transport.seen().iter().map(|r| r.url.clone()).collect();
        assert_eq!(
            hosts,
            [
                "http://node-a:1/fetch-proxy",
                "http://node-b:2/fetch-proxy",
                "http://node-c:3/fetch-proxy",
                "http://node-a:1/fetch-proxy", // wraps
            ]
        );
    }

    #[tokio::test]
    async fn transport_error_falls_back_to_local() {
        let transport = Scripted::new(vec![Err(Error::Http("connection refused".into()))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://dead-node:9"]),
            transport,
            Arc::new(MarkerLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "served locally");
    }

    #[tokio::test]
    async fn non_success_proxy_status_falls_back_to_local() {
        // e.g. the node rejects our secret with 401, or is disabled (404).
        let transport = Scripted::new(vec![Ok((401, "{\"error\":\"bad secret\"}".into()))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:8088"]),
            transport,
            Arc::new(MarkerLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "served locally");
    }

    #[tokio::test]
    async fn unparseable_envelope_falls_back_to_local() {
        let transport = Scripted::new(vec![Ok((200, "not json at all".into()))]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:8088"]),
            transport,
            Arc::new(MarkerLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "served locally");
    }

    #[tokio::test]
    async fn no_nodes_is_a_pure_local_pass_through() {
        // The transport must never be touched — there is nothing to route to.
        struct PanicTransport;
        #[async_trait]
        impl HttpClient for PanicTransport {
            async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
                panic!("no proxy call without configured nodes");
            }
        }
        let engine = RemoteEngine::with_transport(
            &cfg(&[]),
            Arc::new(PanicTransport),
            Arc::new(MarkerLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "served locally");
    }

    /// Local stub that can serve binary bodies — i.e. the real `HttpEngine`'s
    /// capability, which this decorator wraps.
    struct BinaryLocal;
    #[async_trait]
    impl HttpClient for BinaryLocal {
        async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
            panic!("the binary path must not go through fetch");
        }
        async fn fetch_bytes(&self, _req: HttpRequest) -> Result<Vec<u8>> {
            Ok(vec![0x50, 0x4B, 0x03, 0x04])
        }
    }

    /// The anti-pattern: a **decorator that drops the capability it wraps**.
    /// `fetch_bytes` is a default-bodied trait method, so a wrapper that forgets
    /// it does not fail to compile — it silently answers "this engine does not
    /// support binary fetch_bytes" on behalf of an engine that supports it
    /// perfectly. Enabling `[remote]` would have been enough to break every
    /// binary fetch routed through this position.
    #[tokio::test]
    async fn a_decorator_does_not_drop_the_binary_capability_it_wraps() {
        // Nodes configured: a *fetch* would be dispatched to a peer, and the
        // binary path must still be served locally rather than inherit a refusal.
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a.example"]),
            Arc::new(DeadLocal),
            Arc::new(BinaryLocal),
        );
        let bytes = engine
            .fetch_bytes(HttpRequest::get("https://target.example/a.zip"))
            .await
            .expect("the wrapped engine can do this, so the wrapper must too");
        assert_eq!(bytes, vec![0x50, 0x4B, 0x03, 0x04]);
    }

    #[test]
    fn proxy_response_mirrors_http_response_serde() {
        // HttpResponse is Serialize-only in core; the mirror must read its
        // exact output. Serialize a real HttpResponse, read it back here.
        let real = HttpResponse {
            status: 203,
            headers: HashMap::from([("k".into(), "v".into())]),
            body: "b".into(),
            final_url: "https://f.example/".into(),
            cache_hit: true,
        };
        let json = serde_json::to_string(&real).unwrap();
        let mirror: ProxyResponse = serde_json::from_str(&json).unwrap();
        let back: HttpResponse = mirror.into();
        assert_eq!(back.status, 203);
        assert_eq!(back.headers.get("k").map(String::as_str), Some("v"));
        assert_eq!(back.body, "b");
        assert_eq!(back.final_url, "https://f.example/");
        assert!(back.cache_hit);
    }
}
