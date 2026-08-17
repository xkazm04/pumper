//! Remote fetch fabric, coordinator side (M17 v1): an [`HttpClient`] that ships
//! a serialized [`HttpRequest`] to a peer pumper node's `POST /fetch-proxy`
//! endpoint and gets the [`HttpResponse`] back as JSON. The peer runs the
//! request through its own **local** fetch stack — HTTP engine, politeness
//! governor, cache, body caps — just from a different egress IP/geography.
//!
//! **Politeness, precisely.** A proxied fetch is as polite as a local one *on
//! the serving node*: the peer's governor spaces it and learns the target's
//! `429`/`503` penalties like any host. It is **not** polite in the
//! coordinator's own governor, which never sees the target at all — so the
//! coordinator does not learn that host's penalties, and if the fetch later
//! escalates to the browser tier (governed coordinator-side) or falls back to
//! local, it starts from an unpenalized spacing. Per-node politeness is
//! preserved; cluster-wide politeness is not, and that is the v1 trade, not an
//! oversight (see the shared-brain note at the bottom of this comment).
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
//! ## Routing, failover + fallback
//!
//! An atomic cursor over `[remote] nodes` picks the **starting** node (so a
//! healthy cluster rotates a, b, c, a…); from there the fetch walks the
//! remaining peers. **Any** node failure — transport error, non-2xx proxy
//! status, unparseable envelope, over-cap body, deadline — moves to the next
//! eligible peer, and only when they are exhausted does the fetch fall back to
//! the **local** engine.
//!
//! Local is the last resort, not the second, because the whole point of the
//! fabric is that traffic leaves from somewhere other than the coordinator's IP.
//! Before this, one dead peer out of three sent a deterministic **third** of all
//! egress out of exactly the address the operator deployed the fabric to stop
//! using — and did it silently, since each of those fetches merely logged one
//! `warn!` before succeeding locally. Worse, on a host that blocks the
//! coordinator that leaked third comes back thin/blocked, feeds the learned tier
//! router three strikes, and pins the whole host to the browser tier for every
//! future fetch.
//!
//! Three bounds keep failover from becoming its own outage:
//!
//! - a failed node goes on a **cooldown** (`[remote] node_cooldown_secs`,
//!   default 60; `0` disables) and is skipped while it lasts, so the next N
//!   fetches do not each re-discover the same dead peer;
//! - at most [`MAX_NODE_ATTEMPTS`] distinct peers are tried per fetch — a total
//!   cluster outage costs a bounded 3 × `timeout_secs`, not N × ;
//! - each attempt runs under an **end-to-end** deadline of `timeout_secs`
//!   ([`RemoteEngine::attempt`]).
//!
//! That last one is a correction, not a decoration. `timeout_secs` has always
//! been documented "per proxy call, end to end" and was in fact handed to the
//! HTTP engine, which applies a request timeout **per attempt** inside its
//! `for attempt in 0..=retries` loop (`[http] retries` = 3). Worse, `502` — what
//! `/fetch-proxy` used to return when the peer's own fetch failed — is in the
//! default `[http] retryable_statuses`, so one deterministic peer-side failure
//! cost four full proxy attempts with exponential backoff before the local
//! ladder even started: minutes, against a 900s job timeout. `/fetch-proxy` now
//! answers **422** for a failed proxied fetch (a status the transport does not
//! retry — the same reasoning that made a transact capability refusal a 422
//! rather than a retryable 502), and the deadline here bounds everything else.
//!
//! With no nodes configured the engine is a pure pass-through to local.
//!
//! ## What never leaves the coordinator
//!
//! Substituting a peer for the local stack is only honest while it changes
//! *where* a fetch egresses from and nothing else. Two kinds of fetch fail that
//! test and are therefore served locally, always:
//!
//! - **binary bodies** (`fetch_bytes`) — the envelope's `body` is a `String`, so
//!   a binary body cannot travel the wire format at all;
//! - **profiled fetches** ([`must_serve_locally`]) — a profile is a cookie jar on
//!   the **coordinator's** disk (`<profiles_dir>/<name>/cookies.json`) and
//!   nothing replicates it across the cluster. A peer asked to serve one opens a
//!   jar it does not have; the HTTP engine treats `NotFound` as "start empty",
//!   so the peer answers `200` with logged-out or login-wall HTML that is
//!   indistinguishable from the real page at every layer above — and it flows
//!   through extraction into stored dataset revisions as real records. That is a
//!   **correctness** failure, and this module comment used to deny it existed
//!   ("degrades throughput, never correctness").
//!
//! The outbound proxy call itself runs through the **local** transport (the
//! real HTTP engine), so peer nodes are governed/spaced like any other host.
//! Cluster-wide governor state is deliberately OUT of this v1 — each node's
//! governor protects targets independently; the shared-brain merge is M01's
//! host-weather bundle, later.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::RemoteConfig;
use pumper_core::fetcher::{REMOTE_NODE_HEADER, REMOTE_TARGET_HEADER};
use pumper_core::{Error, HttpClient, HttpMethod, HttpRequest, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Header carrying the cluster shared secret on every proxy call. The peer's
/// `/fetch-proxy` route rejects a missing/mismatched value with 401.
pub const REMOTE_SECRET_HEADER: &str = "x-pumper-remote-secret";

/// Path of the proxy endpoint on a peer node.
pub const FETCH_PROXY_PATH: &str = "/fetch-proxy";

/// Why a fetch must be served by the coordinator's own stack instead of being
/// dispatched to a peer — `None` means "a peer may serve this".
///
/// Pure and total: this is the guard on the substitution the whole crate makes,
/// so it has to be assertable without a network, a peer, or a cookie jar.
///
/// Today it has exactly one rule, and it is a **data-correctness** rule rather
/// than a capability one. A `profile` names a persistent cookie jar under the
/// *coordinator's* `[fetcher] profiles_dir`. Peers have their own vaults and
/// nothing syncs them, so a peer handed `profile: "acme"` runs the fetch through
/// an empty jar — `engine-http` maps a missing `cookies.json` to
/// `CookieStore::default()` with no warning — and returns a perfectly valid
/// `200` carrying the logged-out page. The coordinator cannot tell, extraction
/// cannot tell, and the row lands in a dataset revision as real data.
///
/// Keeping the fetch local (rather than letting the peer refuse and falling
/// back) is the cheaper of the two safe answers: the fallback costs a wasted
/// round-trip on *every* profiled fetch forever, to reach a peer that can never
/// legitimately serve one. The serving side refuses as well (see
/// `routes::remote`), but as defence-in-depth against an older or hostile
/// coordinator, not as the primary lever.
///
/// Sibling of `pumper_core::require_existing_profile`, which closed the same
/// class of hole for browser transact flows ("an empty profile is a LOGGED-OUT
/// browser").
pub fn must_serve_locally(req: &HttpRequest) -> Option<&'static str> {
    req.profile.as_ref().map(|_| {
        "a profiled fetch runs under a cookie jar that exists only on this node; \
         a peer would serve it logged out"
    })
}

/// Why the serving side must refuse to fetch `url` on a peer's behalf — `None`
/// means the target is in policy.
///
/// `/fetch-proxy` is the one route in this service that turns a caller-supplied
/// string into an arbitrary outbound request, and every *other* route on a
/// pumper node is unauthenticated by design (the safety argument is the loopback
/// bind — see `docs/deployment.md`). But a fabric peer has to be reachable at a
/// routable address, so the fabric is exactly the deployment where that argument
/// no longer holds: without this guard a node will happily fetch
/// `http://127.0.0.1:8088/jobs` — *its own* API — for whoever holds the shared
/// secret, and any other host on its LAN besides.
///
/// `allow_private = true` (`[remote] allow_private_targets`) is the opt-out for
/// an operator who genuinely proxies a LAN. It relaxes the address ranges only;
/// the scheme check is unconditional.
///
/// **Known limit, stated rather than papered over:** this is a pure predicate
/// over the URL, so it blocks address *literals* (in every WHATWG form —
/// `127.0.0.1`, `0x7f.1`, `2130706433` all parse to the same `Ipv4Addr`) and the
/// `localhost` family by name. A **hostname that resolves into** a private range
/// is not caught; catching that needs resolve-then-pin plumbing inside the HTTP
/// engine, which this cannot reach and which would still race DNS rebinding.
pub fn blocked_target(url: &str, allow_private: bool) -> Option<String> {
    let parsed = match url::Url::parse(url) {
        Ok(parsed) => parsed,
        Err(e) => return Some(format!("target URL is not parseable ({e})")),
    };
    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Some(format!(
                "target scheme '{other}' is not fetchable through the remote fabric \
                 (http/https only)"
            ))
        }
    }
    let Some(host) = parsed.host() else {
        return Some("target URL carries no host".to_string());
    };
    if allow_private {
        return None;
    }
    let blocked = match &host {
        url::Host::Ipv4(ip) => blocked_v4(*ip),
        url::Host::Ipv6(ip) => blocked_v6(*ip),
        url::Host::Domain(name) => blocked_name(name),
    }?;
    Some(format!(
        "target host '{host}' is {blocked}; this node refuses to fetch it for a peer \
         (set [remote] allow_private_targets = true if this cluster deliberately \
         scrapes its own network)"
    ))
}

/// The reason an IPv4 target is out of policy, or `None` for a routable address.
fn blocked_v4(ip: std::net::Ipv4Addr) -> Option<&'static str> {
    let o = ip.octets();
    if ip.is_loopback() {
        Some("a loopback address")
    } else if ip.is_unspecified() || o[0] == 0 {
        Some("in the unspecified / this-network block (0.0.0.0/8)")
    } else if ip.is_private() {
        Some("a private address (RFC 1918)")
    } else if ip.is_link_local() {
        // 169.254.0.0/16 — the cloud metadata services live at 169.254.169.254.
        Some("a link-local address (169.254.0.0/16, incl. cloud metadata)")
    } else if o[0] == 100 && (64..128).contains(&o[1]) {
        Some("in the carrier-grade NAT block (100.64.0.0/10)")
    } else if ip.is_broadcast() {
        Some("the broadcast address")
    } else if ip.is_multicast() {
        Some("a multicast address")
    } else {
        None
    }
}

/// The reason an IPv6 target is out of policy, or `None` for a routable address.
///
/// `is_unique_local` / `is_unicast_link_local` are still unstable in std, so the
/// two prefixes are matched here by their defining bits rather than left out.
fn blocked_v6(ip: std::net::Ipv6Addr) -> Option<&'static str> {
    if ip.is_loopback() {
        return Some("a loopback address");
    }
    if ip.is_unspecified() {
        return Some("the unspecified address");
    }
    // `::ffff:127.0.0.1` and `::127.0.0.1` are the same destinations wearing a
    // different notation; judge them by the v4 address they carry.
    if let Some(v4) = ip.to_ipv4() {
        return blocked_v4(v4);
    }
    let head = ip.segments()[0];
    if head & 0xfe00 == 0xfc00 {
        Some("a unique-local address (fc00::/7)")
    } else if head & 0xffc0 == 0xfe80 {
        Some("a link-local address (fe80::/10)")
    } else if ip.is_multicast() {
        Some("a multicast address")
    } else {
        None
    }
}

/// The reason a *named* target is out of policy. Only the loopback family is
/// decidable without DNS; see [`blocked_target`]'s known limit.
fn blocked_name(name: &str) -> Option<&'static str> {
    // A fully-qualified name may carry a root dot: `localhost.` resolves exactly
    // like `localhost`, and comparing the raw string would miss it.
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    let loopback = name == "localhost"
        || name.ends_with(".localhost")
        || name == "localhost.localdomain"
        || name == "ip6-localhost"
        || name == "ip6-loopback";
    loopback.then_some("a loopback name")
}

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

/// How much bigger than `[remote] max_body_bytes` the *transport* cap on a proxy
/// call is allowed to be — headroom for JSON escaping, since a body of quotes
/// and control characters roughly doubles when it is escaped into the envelope's
/// `body` string. The decoded body is checked against the real cap separately
/// ([`body_over_cap`]); this multiplier is not a raised limit.
const BODY_CAP_TRANSPORT_MULTIPLIER: u64 = 2;

/// How many *distinct* peers one fetch will try before giving up on the fabric
/// and serving locally.
///
/// A bound rather than "walk every node": with a 30-node cluster in a total
/// outage, looping the whole list would spend 30 × `[remote] timeout_secs`
/// before the local ladder even starts, against a `[worker] job_timeout_secs`
/// of 900. Three attempts caps the fabric's share of one HTTP-tier fetch at
/// 3 × `timeout_secs`, which is 90s at the shipped default — and three healthy
/// peers failing in a row is already a cluster-wide event, not a bad node.
const MAX_NODE_ATTEMPTS: usize = 3;

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
    /// Round-robin cursor over `nodes`, bumped once per fetch. It picks the
    /// *starting* node; the failover walk continues from there.
    next: AtomicUsize,
    /// Per-node cooldown deadline, parallel to `nodes`, in [`RemoteEngine::now_ms`]
    /// units. `0` = healthy. A failed node is skipped until its deadline passes,
    /// so the next N fetches do not each re-discover the same dead peer (and
    /// each pay a full timeout budget to do it).
    cooldown_until: Vec<AtomicU64>,
    /// Monotonic base for `cooldown_until`.
    started: std::time::Instant,
    /// `[remote] node_cooldown_secs` as millis. `0` disables cooldown.
    cooldown_ms: u64,
    /// `[remote] timeout_secs`: per proxy call, **end to end** — enforced as a
    /// deadline around the whole node attempt (see [`RemoteEngine::attempt`]),
    /// because the inner HTTP engine applies it per *retry attempt*.
    timeout_secs: u64,
    /// `[remote] max_body_bytes`: cap on the proxied response body, enforced on
    /// the decoded inner body ([`body_over_cap`]) as well as on the transport,
    /// where the cap is [`BODY_CAP_TRANSPORT_MULTIPLIER`]× this plus
    /// [`ENVELOPE_SLACK_BYTES`] of JSON headroom.
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
        let nodes: Vec<String> = cfg
            .nodes
            .iter()
            .map(|n| n.trim_end_matches('/').to_string())
            .filter(|n| !n.is_empty())
            .collect();
        Self {
            // `cooldown_until` is indexed by node position, so it must be built
            // from the SAME filtered list — sizing it from `cfg.nodes` would
            // desync the two the moment a blank entry is dropped.
            cooldown_until: nodes.iter().map(|_| AtomicU64::new(0)).collect(),
            nodes,
            secret: cfg.secret.clone(),
            transport,
            local,
            next: AtomicUsize::new(0),
            started: std::time::Instant::now(),
            cooldown_ms: cfg.node_cooldown_secs.saturating_mul(1_000),
            timeout_secs: cfg.timeout_secs,
            max_body_bytes: cfg.max_body_bytes,
        }
    }

    /// Milliseconds since this engine was constructed — the clock the cooldown
    /// map is written in. A monotonic offset rather than a wall clock, so a
    /// system clock step can never park a node in cooldown for a century.
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    /// Whether node `idx` is inside its failure cooldown right now.
    fn is_cooling(&self, idx: usize) -> bool {
        still_cooling(
            self.cooldown_until[idx].load(Ordering::Relaxed),
            self.now_ms(),
        )
    }

    /// Records a node failure: skip it until the cooldown expires. `Relaxed`
    /// for the same reason the round-robin cursor is — this is a scheduling
    /// *hint*, and the worst a lost update can do is send one extra fetch at a
    /// node that is already known bad.
    fn mark_failed(&self, idx: usize) {
        let until = self.now_ms().saturating_add(self.cooldown_ms);
        self.cooldown_until[idx].store(until, Ordering::Relaxed);
    }

    /// Records a node success: clear any cooldown so recovery is immediate
    /// rather than waiting out a penalty the node no longer deserves.
    fn mark_healthy(&self, idx: usize) {
        self.cooldown_until[idx].store(0, Ordering::Relaxed);
    }

    /// One node attempt under the **end-to-end** `[remote] timeout_secs` budget.
    ///
    /// The budget is also handed to the inner request, where `engine-http`
    /// applies it *per attempt* inside its `for attempt in 0..=retries` loop.
    /// That is what used to make the documented "per proxy call, end to end"
    /// false by a factor of `retries + 1`: a black-holed node cost four full
    /// timeouts. The deadline here is what makes the sentence true again — one
    /// slow attempt may still use the whole budget, but four cannot.
    async fn attempt(&self, node: &str, req: &HttpRequest) -> Result<HttpResponse> {
        let budget = std::time::Duration::from_secs(self.timeout_secs);
        match tokio::time::timeout(budget, self.try_node(node, req)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Http(format!(
                "node {node} did not answer {FETCH_PROXY_PATH} within the {}s end-to-end \
                 [remote] timeout_secs budget",
                self.timeout_secs
            ))),
        }
    }

    /// One attempt against one node. Every failure mode is a typed error the
    /// caller turns into the next node, then into a local fallback.
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
                .saturating_mul(BODY_CAP_TRANSPORT_MULTIPLIER)
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
        if let Some(reason) = body_over_cap(parsed.body.len(), self.max_body_bytes) {
            return Err(Error::Http(format!("node {node} {reason}")));
        }
        let mut resp: HttpResponse = parsed.into();
        if let Some(reason) = envelope_mismatch(&req.url, &resp.headers) {
            return Err(Error::Http(format!("node {node} {reason}")));
        }
        stamp_egress(&mut resp.headers, node);
        Ok(resp)
    }
}

/// Why an envelope does not answer the question that was asked — `None` when the
/// peer echoed the URL it was handed.
///
/// **Nothing used to bind the answer to the request.** The coordinator
/// deserialized whatever the peer sent and the tiered fetcher minted the outcome
/// with the *requested* URL and the peer's body, so a buggy or hostile peer
/// could return arbitrary content for any URL and have it stored, indexed and
/// attributed with no detectable trace. One node quietly serving a cached copy
/// of the wrong page would have been indistinguishable from the site changing.
///
/// The echo is deliberately **not** `final_url`: that is where the fetch *ended*
/// and legitimately differs after a redirect, which is exactly why it cannot
/// serve as the binding. A missing echo is a mismatch too — an unmarked envelope
/// is an unverifiable one, and the failure mode is a fallback, not bad data.
fn envelope_mismatch(
    requested: &str,
    headers: &std::collections::HashMap<String, String>,
) -> Option<String> {
    let echoed = headers.iter().find_map(|(name, value)| {
        name.eq_ignore_ascii_case(REMOTE_TARGET_HEADER)
            .then(|| value.trim())
    });
    match echoed {
        Some(echoed) if echoed == requested => None,
        Some(echoed) => Some(format!(
            "answered for '{echoed}' but was asked for '{requested}' — refusing the envelope \
             rather than storing one node's answer under another URL"
        )),
        None => Some(format!(
            "returned an envelope with no {REMOTE_TARGET_HEADER} echo, so it cannot be bound to \
             '{requested}' (a peer older than this binding, or one that is not a pumper node)"
        )),
    }
}

/// Replaces the wire-artifact headers with the one fact a consumer wants: which
/// node served this body.
///
/// The echo header is **stripped**, not just ignored: it exists only to bind the
/// envelope to the request, and leaving it on a response that flows onward would
/// mean a second reader could mistake a verified marker for a live-origin one.
/// Any `REMOTE_NODE_HEADER` the target site itself sent is overwritten for the
/// same reason — the namespace is reserved, and only this function may write it.
fn stamp_egress(headers: &mut std::collections::HashMap<String, String>, node: &str) {
    headers.retain(|name, _| {
        !name.eq_ignore_ascii_case(REMOTE_TARGET_HEADER)
            && !name.eq_ignore_ascii_case(REMOTE_NODE_HEADER)
    });
    headers.insert(REMOTE_NODE_HEADER.to_string(), node.to_string());
}

/// Whether a node whose cooldown deadline is `until_ms` is still being skipped
/// at `now_ms`. Both are [`RemoteEngine::now_ms`] offsets.
///
/// A named function rather than an inline `>` so the boundary is assertable
/// without waiting out a real minute: a deadline that has just been *reached* is
/// over (the node rejoins), and a healthy node's `0` is never in the future.
fn still_cooling(until_ms: u64, now_ms: u64) -> bool {
    until_ms > now_ms
}

/// Why a decoded inner body is over this coordinator's cap — `None` when it
/// fits.
///
/// The transport-level cap on the proxy call is deliberately loose
/// (`2 × max_body_bytes + `[`ENVELOPE_SLACK_BYTES`]) because the JSON envelope,
/// with its escaped `body` string, is bigger than the body it carries — worst
/// case roughly double for a body full of quotes and control characters. That
/// slack is *transport* headroom, not a raised limit, and nothing used to check
/// the body itself once it was decoded. So a peer whose own `[remote]
/// max_body_bytes` had drifted upward could hand this coordinator a body up to
/// twice its stated cap, and the coordinator paid for the whole transfer twice
/// (once over the wire, once in the decoded `String`) before storing it.
///
/// The `2×` multiplier lived only in a test assertion string until now.
fn body_over_cap(body_len: usize, cap: u64) -> Option<String> {
    (body_len as u64 > cap).then(|| {
        format!(
            "returned a {body_len}-byte body, over this coordinator's [remote] max_body_bytes \
             of {cap} (the transport cap allows {}× that plus envelope slack for JSON escaping, \
             which is headroom for encoding — not a raised limit)",
            BODY_CAP_TRANSPORT_MULTIPLIER
        )
    })
}

#[async_trait]
impl HttpClient for RemoteEngine {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        // Correctness gate before the routing gate: a fetch a peer cannot serve
        // *correctly* must not be dispatched even when peers are healthy.
        if let Some(reason) = must_serve_locally(&req) {
            debug!(url = %req.url, reason, "remote fabric: serving this fetch locally");
            return self.local.fetch(req).await;
        }
        if self.nodes.is_empty() {
            return self.local.fetch(req).await;
        }
        // One cursor bump per fetch (not per attempt), so a healthy cluster
        // still rotates a, b, c, a... exactly as before.
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let mut attempted = 0usize;
        let mut skipped_cooling = 0usize;
        for hop in 0..self.nodes.len() {
            if attempted >= MAX_NODE_ATTEMPTS {
                break;
            }
            let idx = (start.wrapping_add(hop)) % self.nodes.len();
            if self.is_cooling(idx) {
                skipped_cooling += 1;
                continue;
            }
            attempted += 1;
            let node = self.nodes[idx].clone();
            match self.attempt(&node, &req).await {
                Ok(resp) => {
                    self.mark_healthy(idx);
                    return Ok(resp);
                }
                Err(e) => {
                    self.mark_failed(idx);
                    warn!(
                        node = %node,
                        error = %e,
                        cooldown_ms = self.cooldown_ms,
                        "remote fetch node failed — cooling it down and trying the next peer"
                    );
                }
            }
        }
        // Local is the LAST resort, not the second. Reaching it means every
        // eligible peer failed (or is cooling), which is the one case where
        // egress from the coordinator's own IP is better than no fetch at all —
        // so it is worth a line an operator can count.
        warn!(
            url = %req.url,
            attempted,
            skipped_cooling,
            nodes = self.nodes.len(),
            "remote fabric exhausted — this fetch egresses from the COORDINATOR's own IP"
        );
        self.local.fetch(req).await
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
            ..RemoteConfig::default()
        }
    }

    /// The one URL these tests fetch. A single constant because every envelope
    /// must now echo the URL it was asked for — an envelope that answers a
    /// different question is refused, which is the point of `envelope_mismatch`.
    const TARGET: &str = "https://target.example/page";

    fn envelope(body: &str) -> String {
        envelope_echoing(TARGET, body)
    }

    /// An envelope whose `REMOTE_TARGET_HEADER` echo says `echo` — the seam a
    /// mismatched or unmarked peer answer is caught at.
    fn envelope_echoing(echo: &str, body: &str) -> String {
        let mut headers = HashMap::from([("x-served-by".to_string(), "node".to_string())]);
        if !echo.is_empty() {
            headers.insert(REMOTE_TARGET_HEADER.to_string(), echo.to_string());
        }
        serde_json::to_string(&ProxyResponse {
            status: 200,
            headers,
            body: body.into(),
            final_url: TARGET.into(),
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
        let inner = HttpRequest::get("https://target.example/page");
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
        // No `profile` assertion here on purpose — a profiled fetch never gets
        // this far. See `a_profiled_fetch_is_served_locally_not_logged_out_by_a_peer`.
        assert_eq!(round.profile, None);
    }

    /// Local stub that answers as the **session holder** — the body only a
    /// coordinator with the profile's cookie jar could produce.
    struct LoggedInLocal;
    #[async_trait]
    impl HttpClient for LoggedInLocal {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            assert_eq!(
                req.profile.as_deref(),
                Some("acme"),
                "the profile must survive the redirect to local"
            );
            Ok(HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body: "<html>welcome back, acme</html>".into(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// The anti-pattern: **a substitution that changes the answer**. The fabric
    /// is allowed to change which IP a fetch leaves from; it is not allowed to
    /// change what comes back. A peer has no copy of the coordinator's cookie
    /// jar, and `engine-http` starts an empty one for a jar it cannot find, so a
    /// profiled fetch dispatched to a peer returns a logged-out page with a `200`
    /// and no signal of any kind — and that page is what gets extracted and
    /// stored as a dataset revision.
    ///
    /// Both halves are asserted: the peer is never asked, AND the body returned
    /// is the session holder's.
    #[tokio::test]
    async fn a_profiled_fetch_is_served_locally_not_logged_out_by_a_peer() {
        struct NeverDispatched;
        #[async_trait]
        impl HttpClient for NeverDispatched {
            async fn fetch(&self, _req: HttpRequest) -> Result<HttpResponse> {
                panic!("a profiled fetch must never be dispatched to a peer");
            }
        }
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:8088", "http://node-b:8088"]),
            Arc::new(NeverDispatched),
            Arc::new(LoggedInLocal),
        );
        let mut req = HttpRequest::get("https://target.example/dashboard");
        req.profile = Some("acme".into());
        let resp = engine.fetch(req).await.unwrap();
        assert_eq!(resp.body, "<html>welcome back, acme</html>");
    }

    #[test]
    fn only_a_profile_pins_a_fetch_to_the_coordinator() {
        assert!(must_serve_locally(&HttpRequest::get("https://t.example/")).is_none());
        let mut profiled = HttpRequest::get("https://t.example/");
        profiled.profile = Some("acme".into());
        let why = must_serve_locally(&profiled).expect("a profiled fetch stays local");
        assert!(why.contains("logged out"), "{why}");
    }

    #[test]
    fn a_routable_target_is_not_blocked() {
        for url in [
            "https://target.example/page",
            "http://93.184.216.34/",
            "https://[2606:2800:220:1:248:1893:25c8:1946]/",
            "http://sub.domain.example.co.uk:8080/a?b=c",
        ] {
            assert_eq!(
                blocked_target(url, false),
                None,
                "{url} should be fetchable"
            );
        }
    }

    /// The anti-pattern: an SSRF guard that only knows the **dotted-quad**
    /// spelling of loopback. `http://2130706433/` and `http://0x7f.1/` reach the
    /// same socket, and the URL parser the fetching stack uses folds every one of
    /// these into `127.0.0.1` — so a guard that compares strings passes them all.
    #[test]
    fn every_spelling_of_loopback_is_blocked_not_only_the_dotted_quad() {
        for url in [
            "http://127.0.0.1:8088/jobs",
            "http://127.1/",
            "http://2130706433/",
            "http://0x7f.0.0.1/",
            "http://0177.0.0.1/",
            "http://localhost:8088/jobs",
            "http://LOCALHOST./",
            "http://api.localhost/",
            "http://[::1]:8088/",
            "http://[::ffff:127.0.0.1]/",
        ] {
            let why = blocked_target(url, false).unwrap_or_else(|| panic!("{url} must be refused"));
            assert!(
                why.contains("refuses to fetch it for a peer"),
                "{url}: {why}"
            );
        }
    }

    #[test]
    fn private_link_local_and_non_http_targets_are_refused() {
        for url in [
            "http://10.0.0.2:8088/",
            "http://192.168.1.1/",
            "http://172.16.9.9/",
            "http://169.254.169.254/latest/meta-data/",
            "http://100.64.0.1/",
            "http://0.0.0.0/",
            "http://[fd00::1]/",
            "http://[fe80::1]/",
        ] {
            assert!(
                blocked_target(url, false).is_some(),
                "{url} must be refused"
            );
        }
        for url in ["file:///etc/passwd", "ftp://example.com/x", "not a url"] {
            assert!(
                blocked_target(url, false).is_some(),
                "{url} must be refused"
            );
        }
    }

    /// The opt-out relaxes **addresses**, never the scheme: an operator who
    /// deliberately scrapes their own LAN did not thereby ask this node to read
    /// its own filesystem.
    #[test]
    fn the_opt_out_relaxes_addresses_not_schemes() {
        assert_eq!(blocked_target("http://10.0.0.2:8088/", true), None);
        assert_eq!(blocked_target("http://127.0.0.1:8088/jobs", true), None);
        assert!(blocked_target("file:///etc/passwd", true).is_some());
        assert!(blocked_target("gopher://10.0.0.2/", true).is_some());
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
            engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
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

    // ── failover, cooldown, attempt budget ──────────────────────────────────

    /// Transport that answers **per node**: each proxy URL gets its own scripted
    /// result. The old `Scripted` stub answered the same thing to everyone, which
    /// is exactly why no test could see that a dead node was never failed over.
    struct PerNode {
        answers: HashMap<String, Result<(u16, String)>>,
        seen: Mutex<Vec<String>>,
    }

    impl PerNode {
        fn new(answers: &[(&str, Result<(u16, String)>)]) -> Arc<Self> {
            Arc::new(Self {
                answers: answers
                    .iter()
                    .map(|(node, r)| {
                        let key = format!("{node}{FETCH_PROXY_PATH}");
                        let value = match r {
                            Ok(ok) => Ok(ok.clone()),
                            Err(e) => Err(Error::Http(e.to_string())),
                        };
                        (key, value)
                    })
                    .collect(),
                seen: Mutex::new(Vec::new()),
            })
        }
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl HttpClient for PerNode {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            self.seen.lock().unwrap().push(req.url.clone());
            let (status, body) = match self.answers.get(&req.url) {
                Some(Ok(ok)) => ok.clone(),
                Some(Err(e)) => return Err(Error::Http(e.to_string())),
                None => panic!("unscripted node {}", req.url),
            };
            Ok(HttpResponse {
                status,
                headers: HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn dead() -> Result<(u16, String)> {
        Err(Error::Http("connection refused".into()))
    }

    /// The anti-pattern: **failover to the thing the feature exists to avoid**.
    /// The fabric's entire product claim is "this fetch left from a different
    /// IP". With one dead peer out of three, the old code sent a deterministic
    /// third of all egress out of exactly the coordinator's own address — after
    /// trying precisely one node and giving up. A healthy peer was sitting right
    /// there.
    #[tokio::test]
    async fn a_dead_node_fails_over_to_a_peer_not_to_the_coordinator() {
        let transport = PerNode::new(&[
            ("http://node-a:1", dead()),
            (
                "http://node-b:2",
                Ok((200, envelope("<html>from node B</html>"))),
            ),
        ]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1", "http://node-b:2"]),
            transport.clone(),
            Arc::new(DeadLocal), // reaching local at all is the failure
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "<html>from node B</html>");
        assert_eq!(
            transport.seen(),
            ["http://node-a:1/fetch-proxy", "http://node-b:2/fetch-proxy"]
        );
    }

    /// Local is still the floor — it is just no longer the *second* step.
    #[tokio::test]
    async fn every_peer_dead_falls_back_to_local() {
        let transport = PerNode::new(&[
            ("http://node-a:1", dead()),
            ("http://node-b:2", Ok((500, "boom".into()))),
        ]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1", "http://node-b:2"]),
            transport.clone(),
            Arc::new(MarkerLocal),
        );
        let resp = engine
            .fetch(HttpRequest::get("https://target.example/page"))
            .await
            .unwrap();
        assert_eq!(resp.body, "served locally");
        assert_eq!(transport.seen().len(), 2, "both peers tried before local");
    }

    /// Two dead peers and one healthy one, driven four times — the whole point
    /// of the cooldown in one number.
    async fn dead_node_probes(cooldown_secs: u64) -> (usize, usize) {
        let nodes = ["http://node-a:1", "http://node-b:2", "http://node-c:3"];
        let transport = PerNode::new(&[
            ("http://node-a:1", dead()),
            ("http://node-b:2", dead()),
            ("http://node-c:3", Ok((200, envelope("c")))),
        ]);
        let mut c = cfg(&nodes);
        c.node_cooldown_secs = cooldown_secs;
        let engine = RemoteEngine::with_transport(&c, transport.clone(), Arc::new(DeadLocal));
        for _ in 0..4 {
            engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        }
        let hits = transport.seen();
        let count = |prefix: &str| hits.iter().filter(|u| u.starts_with(prefix)).count();
        (count("http://node-a:1"), count("http://node-b:2"))
    }

    /// The anti-pattern: **re-discovering the same corpse on every fetch**.
    /// Without a cooldown, each fetch that reaches a dead node pays its whole
    /// timeout budget again to learn exactly what the last one learned. With it,
    /// a dead peer is probed **once** however many fetches follow.
    #[tokio::test]
    async fn a_failed_node_is_skipped_while_its_cooldown_holds() {
        assert_eq!(
            dead_node_probes(60).await,
            (1, 1),
            "each dead peer must be discovered once, not once per fetch"
        );
    }

    /// `node_cooldown_secs = 0` is the documented "no cooldown" setting, and it
    /// has to mean *off* rather than *never expires* — the two obvious readings
    /// of a zero deadline, only one of which is safe. Contrast with the test
    /// above: same cluster, same four fetches, dead peers re-probed instead.
    ///
    /// The counts are 2 and 3, not 4 and 4, because the round-robin cursor
    /// decides where each fetch *starts*: a fetch that starts at the healthy
    /// node never walks to the dead ones at all. That asymmetry is precisely the
    /// leak this direction closes — it used to decide who got served locally.
    #[tokio::test]
    async fn a_zero_cooldown_means_no_cooldown_not_a_permanent_one() {
        let (a, b) = dead_node_probes(0).await;
        assert!(
            a > 1 && b > 1,
            "with cooldown off the dead peers are re-probed, got a={a} b={b}"
        );
        assert_eq!((a, b), (2, 3));
    }

    /// A node that comes back must come back **immediately**, not after serving
    /// out a penalty it no longer deserves.
    #[test]
    fn a_success_clears_a_cooldown_and_a_reached_deadline_has_expired() {
        // The pure boundary, so expiry is assertable without waiting a minute.
        assert!(still_cooling(60_000, 59_999));
        assert!(
            !still_cooling(60_000, 60_000),
            "the deadline itself is over"
        );
        assert!(!still_cooling(60_000, 60_001));
        assert!(!still_cooling(0, 0), "a healthy node is never cooling");
    }

    /// The anti-pattern: **turning a cluster outage into an N× latency
    /// multiplier**. Walking every node before falling back means a 30-node
    /// cluster spends 30 timeout budgets on a fetch that was going to be served
    /// locally anyway.
    #[tokio::test]
    async fn a_total_outage_costs_a_bounded_attempt_budget_not_the_whole_cluster() {
        let nodes = [
            "http://n1:1",
            "http://n2:2",
            "http://n3:3",
            "http://n4:4",
            "http://n5:5",
            "http://n6:6",
        ];
        let answers: Vec<(&str, Result<(u16, String)>)> =
            nodes.iter().map(|n| (*n, dead())).collect();
        let transport = PerNode::new(&answers);
        let engine =
            RemoteEngine::with_transport(&cfg(&nodes), transport.clone(), Arc::new(MarkerLocal));
        let resp = engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        assert_eq!(resp.body, "served locally");
        assert_eq!(
            transport.seen().len(),
            MAX_NODE_ATTEMPTS,
            "at most {MAX_NODE_ATTEMPTS} peers per fetch, whatever the cluster size"
        );
    }

    /// The anti-pattern: **a cap enforced only where it is cheap to enforce**.
    /// The transport cap is deliberately `2× + slack` so JSON escaping does not
    /// reject a legal body — but nothing checked the *decoded* body, so a peer
    /// whose own `max_body_bytes` had drifted upward silently doubled this
    /// coordinator's stated cap, and it paid for the bytes twice (wire, then
    /// decoded String) before finding out.
    #[tokio::test]
    async fn a_body_over_this_coordinators_cap_is_refused_not_silently_doubled() {
        let mut c = cfg(&["http://node-a:1"]);
        c.max_body_bytes = 32;
        let big = envelope(&"x".repeat(64));
        let transport = PerNode::new(&[("http://node-a:1", Ok((200, big)))]);
        let engine = RemoteEngine::with_transport(&c, transport, Arc::new(MarkerLocal));
        let resp = engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        assert_eq!(
            resp.body, "served locally",
            "an over-cap peer body is a node failure, not content"
        );
    }

    #[test]
    fn the_body_cap_is_a_ceiling_not_a_target() {
        assert_eq!(body_over_cap(32, 32), None, "exactly at the cap fits");
        assert_eq!(body_over_cap(0, 0), None);
        let why = body_over_cap(33, 32).expect("over the cap");
        assert!(why.contains("max_body_bytes"), "{why}");
        assert!(
            why.contains(&BODY_CAP_TRANSPORT_MULTIPLIER.to_string()),
            "the 2x transport multiplier must be stated somewhere other than a test: {why}"
        );
    }

    // ── egress attribution + envelope binding ───────────────────────────────

    /// The anti-pattern: **an answer that was never bound to its question**.
    /// The coordinator deserialized whatever a peer sent and the tiered fetcher
    /// minted the outcome with the *requested* URL and the peer's body — so a
    /// buggy or hostile node could return arbitrary content for any URL and have
    /// it stored, indexed and attributed with no detectable trace. A node serving
    /// a cached copy of the wrong page looked exactly like the site changing.
    #[tokio::test]
    async fn a_peer_answering_for_the_wrong_url_is_refused_not_stored() {
        let transport = PerNode::new(&[(
            "http://node-a:1",
            Ok((
                200,
                envelope_echoing("https://attacker.example/other", "<html>wrong page</html>"),
            )),
        )]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1"]),
            transport,
            Arc::new(MarkerLocal),
        );
        let resp = engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        assert_eq!(
            resp.body, "served locally",
            "a mismatched envelope is a node failure, not content"
        );
    }

    /// An envelope with no echo at all cannot be bound either — and "unverifiable"
    /// has to fail closed, or the binding is opt-in for the peer being checked.
    #[tokio::test]
    async fn an_unmarked_envelope_is_refused_rather_than_trusted() {
        let transport = PerNode::new(&[(
            "http://node-a:1",
            Ok((200, envelope_echoing("", "<html>unbindable</html>"))),
        )]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1"]),
            transport,
            Arc::new(MarkerLocal),
        );
        let resp = engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        assert_eq!(resp.body, "served locally");
    }

    #[test]
    fn the_binding_is_the_requested_url_not_the_final_one() {
        let headers = |v: &str| HashMap::from([(REMOTE_TARGET_HEADER.to_string(), v.to_string())]);
        assert_eq!(envelope_mismatch(TARGET, &headers(TARGET)), None);
        // Case-insensitive header lookup: a real HTTP stack guarantees no casing.
        let upper = HashMap::from([(REMOTE_TARGET_HEADER.to_uppercase(), TARGET.to_string())]);
        assert_eq!(envelope_mismatch(TARGET, &upper), None);
        // A redirect target is NOT the binding — the question was the other URL.
        let why = envelope_mismatch(TARGET, &headers("https://target.example/after-redirect"))
            .expect("a different URL is a mismatch");
        assert!(why.contains("was asked for"), "{why}");
        assert!(envelope_mismatch(TARGET, &HashMap::new()).is_some());
    }

    /// The anti-pattern: **a reserved header a target site could forge**. The
    /// namespace is reserved and only the coordinator may write it, so anything
    /// arriving under it from the wire is discarded — including the echo, which
    /// is a wire artifact with no meaning past this seam.
    #[test]
    fn wire_artifacts_are_stripped_and_the_node_marker_cannot_be_forged() {
        let mut headers = HashMap::from([
            ("content-type".to_string(), "text/html".to_string()),
            (REMOTE_TARGET_HEADER.to_string(), TARGET.to_string()),
            (
                REMOTE_NODE_HEADER.to_uppercase(),
                "http://attacker.example".to_string(),
            ),
        ]);
        stamp_egress(&mut headers, "http://node-a:1");
        assert_eq!(
            headers.get(REMOTE_NODE_HEADER).map(String::as_str),
            Some("http://node-a:1")
        );
        assert_eq!(headers.len(), 2, "the echo is stripped: {headers:?}");
        assert!(!headers.contains_key(REMOTE_TARGET_HEADER));
        assert_eq!(
            headers.get("content-type").map(String::as_str),
            Some("text/html"),
            "a real target header must survive"
        );
    }

    /// A peer-served body arrives carrying the node that served it — the fact the
    /// whole fabric exists to produce, and the one nothing in the product could
    /// previously confirm.
    #[tokio::test]
    async fn a_peer_served_body_names_the_node_that_served_it() {
        let transport = PerNode::new(&[
            ("http://node-a:1", dead()),
            ("http://node-b:2", Ok((200, envelope("<html>b</html>")))),
        ]);
        let engine = RemoteEngine::with_transport(
            &cfg(&["http://node-a:1", "http://node-b:2"]),
            transport,
            Arc::new(DeadLocal),
        );
        let resp = engine.fetch(HttpRequest::get(TARGET)).await.unwrap();
        assert_eq!(
            pumper_core::fetcher::remote_egress(&resp.headers),
            Some("http://node-b:2"),
            "the node that actually served it, not the one it started at"
        );
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
