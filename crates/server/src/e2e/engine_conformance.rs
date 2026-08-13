//! **Engine conformance**: one fixed battery, run over every *production*
//! engine, plus the inventories that keep the battery honest.
//!
//! Five capability traits define what an engine is in this repo, and until now
//! nothing anywhere ran a fixed battery over every implementor — each engine
//! tested only itself. Both existing guard rails (`fetch_chokepoint.rs`,
//! `llm_chokepoint.rs`) police **consumers**; nothing has ever policed an
//! **implementor**. The result was a set of silent capability holes, one of
//! which had the wrong retry class:
//!
//! - `Browser::transact`'s default returned `Error::Browser`, which
//!   `Error::is_terminal_for_job` classes **retryable** — so a job that reached
//!   an engine without flow support burned its whole backoff ladder producing
//!   the same "does not support transact flows" sentence four times. The trait's
//!   own doc said it should "fail loudly"; it failed loudly four times and
//!   billed for the privilege.
//! - `HttpClient::fetch_bytes` was overridden by exactly **one** of four
//!   production clients. `ArchiveEngine` and `RemoteEngine` inherited the
//!   refusal — `RemoteEngine` being a *decorator*, so enabling `[remote]` would
//!   have removed a capability from an engine that has it. Meanwhile
//!   `apps/cms-fee-schedule` calls `ctx.engines.http.fetch_bytes(...)` and works
//!   only because `state.rs` happens to place the raw `HttpEngine` there, with
//!   no type, test or comment pinning it.
//!
//! **Why here.** `crates/core` depends on no engine crate (apps and engines
//! depend on core, never the reverse), so a *cross-engine* battery cannot live
//! in core. `crates/server` depends on all seven — it is the only crate that can
//! see every implementor at once.
//!
//! **What is checked, and how.** No obligation is asserted by matching an error
//! message: every probe is `Ok` vs `Err`, or a typed classification
//! (`is_terminal_for_job`). Message text is free to be reworded.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pumper_core::config::{ArchiveConfig, CacheConfig, GovernorConfig, HttpConfig, RemoteConfig};
use pumper_core::testing::TempStore;
use pumper_core::{
    Browser, EngineSet, Error, Governor, HttpCache, HttpClient, HttpRequest, PageAction,
    RenderRequest, RenderedPage, Result, TransactRequest,
};
use pumper_engine_archive::ArchiveEngine;
use pumper_engine_browser::BrowserEngine;
use pumper_engine_http::HttpEngine;
use pumper_engine_remote::RemoteEngine;

/// Bytes the loopback origin serves, chosen so a truncated or text-decoded read
/// is visibly wrong: byte 0x00 and 0xFF are not valid UTF-8 together.
const BINARY_BODY: &[u8] = &[0x50, 0x4B, 0x03, 0x04, 0x00, 0xFF, 0xFE, 0x41];

/// A URL nothing can serve: loopback port 1 refuses instantly, so the "an engine
/// must not fabricate success" probe costs no wall clock and touches no network
/// beyond the loopback interface.
const DEAD_URL: &str = "http://127.0.0.1:1/nothing-listens-here";

// ── the battery ──────────────────────────────────────────────────────────────

/// What one `HttpClient` implementor answered to the fixed battery.
///
/// `binary_capable` is decided by **behavior, not by message text**: hand the
/// client a URL that definitely serves bytes and see whether bytes come back.
/// An implementor inheriting the trait's default `fetch_bytes` returns `Err`
/// there; one that implements it returns the body.
#[derive(Debug, PartialEq, Eq)]
struct HttpClientConformance {
    binary_capable: bool,
}

/// Runs the obligations **every** `HttpClient` owes, whatever it fetches, and
/// reports the one capability that legitimately differs between implementors.
///
/// Obligations (asserted, not reported):
/// 1. A fetch that cannot succeed returns `Err` — never `Ok` with an empty or
///    fabricated body. An engine is driven with untrusted URLs off a crawl
///    frontier; a fabricated `Ok` becomes a "successfully fetched" empty page in
///    a dataset.
/// 2. The same for `fetch_bytes`, whose default body is a refusal: an engine
///    that cannot do binary work must say so, never hand back `Ok(vec![])`.
/// 3. Neither method panics. A panic in an engine takes a worker slot with it.
async fn probe_http_client(
    name: &str,
    client: &dyn HttpClient,
    bytes_url: &str,
) -> HttpClientConformance {
    let dead = client.fetch(HttpRequest::get(DEAD_URL)).await;
    assert!(
        dead.is_err(),
        "{name}: a fetch of an unreachable URL returned Ok — an engine may fail, \
         but it may never fabricate a successful fetch"
    );
    let dead_bytes = client.fetch_bytes(HttpRequest::get(DEAD_URL)).await;
    assert!(
        dead_bytes.is_err(),
        "{name}: fetch_bytes of an unreachable URL returned Ok — an engine that \
         cannot serve bytes must refuse, not hand back an empty body"
    );

    let binary_capable = match client.fetch_bytes(HttpRequest::get(bytes_url)).await {
        Ok(bytes) => {
            assert_eq!(
                bytes, BINARY_BODY,
                "{name}: fetch_bytes returned a body that is not the bytes the \
                 origin served — binary means byte-exact, no decoding"
            );
            true
        }
        Err(_) => false,
    };
    HttpClientConformance { binary_capable }
}

// ── fixtures ─────────────────────────────────────────────────────────────────

/// A loopback origin that serves [`BINARY_BODY`] for every path. Returns its
/// base URL; the server lives for the rest of the test process.
async fn binary_origin() -> String {
    let handler = || async { BINARY_BODY.to_vec() };
    let app = axum::Router::new().fallback(axum::routing::any(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// The real `HttpEngine`, wired as production wires it but with retries off and
/// a short timeout so the unreachable-URL probes are instant.
fn http_engine(store: &TempStore) -> Arc<HttpEngine> {
    let cfg = HttpConfig {
        retries: 0,
        timeout_secs: 5,
        ..HttpConfig::default()
    };
    let governor = Arc::new(Governor::new(&GovernorConfig::default()));
    let cache = Arc::new(HttpCache::new(
        store.storage.pool(),
        &CacheConfig::default(),
    ));
    Arc::new(
        HttpEngine::new(&cfg, governor, cache, store.path().join("profiles"))
            .expect("build the real http engine"),
    )
}

// ── the tests ────────────────────────────────────────────────────────────────

/// THE conformance run: the same battery over every production `HttpClient`,
/// with the binary-capability answers pinned as an EXPECTED map.
///
/// The map is the point. Before it, "which engines can do binary fetches?" had
/// no answer anywhere — not a type, not a test, not a comment — and the one app
/// that depends on the answer (`cms-fee-schedule`) was right only by accident of
/// wiring. A new engine now has to state its answer here, in review, once.
#[tokio::test]
async fn every_production_http_client_answers_the_same_battery() {
    let store = TempStore::new("engine-conformance").await;
    let origin = binary_origin().await;
    let bytes_url = format!("{origin}/artifact.zip");
    let http = http_engine(&store);

    // Every production `HttpClient`, constructed the way the server constructs
    // it. `ArchiveEngine` and `RemoteEngine` both wrap the real HTTP engine —
    // which is exactly what makes a dropped capability invisible.
    let archive = Arc::new(ArchiveEngine::new(
        &ArchiveConfig {
            base_url: origin.clone(),
            ..ArchiveConfig::default()
        },
        http.clone(),
    ));
    let remote = Arc::new(RemoteEngine::new(
        &RemoteConfig {
            enabled: true,
            // No nodes: the engine serves everything from its local stack, which
            // is the state a serve-only node runs in.
            nodes: Vec::new(),
            ..RemoteConfig::default()
        },
        http.clone(),
    ));

    let subjects: Vec<(&str, Arc<dyn HttpClient>)> = vec![
        ("engine-http::HttpEngine", http.clone()),
        ("engine-archive::ArchiveEngine", archive),
        ("engine-remote::RemoteEngine", remote),
    ];

    let mut got: BTreeMap<&str, bool> = BTreeMap::new();
    for (name, client) in &subjects {
        let report = probe_http_client(name, client.as_ref(), &bytes_url).await;
        got.insert(name, report.binary_capable);
    }

    // EXPECTED: which production clients serve binary bodies, and why the rest
    // do not. Every `false` here is a *decision*, not an oversight.
    let expected: BTreeMap<&str, bool> = BTreeMap::from([
        // The one engine that talks to the network itself.
        ("engine-http::HttpEngine", true),
        // A decorator: `/fetch-proxy` carries a String body, so a binary fetch
        // is served by the local stack rather than mangled over the wire. It
        // forwards — a decorator that dropped this would remove a capability
        // from an engine that has it, purely because `[remote]` was enabled.
        ("engine-remote::RemoteEngine", true),
        // Deliberate refusal: "the bytes of a snapshot" has no specified
        // meaning (which capture? what does a freshness window mean for an
        // immutable artifact?). It refuses as ITSELF, naming the archive and
        // the alternative, instead of inheriting a generic default.
        ("engine-archive::ArchiveEngine", false),
    ]);

    assert_eq!(
        got, expected,
        "the binary-fetch capability matrix changed. A new engine must declare \
         whether it serves binary bodies HERE, with the reason — the silent \
         default is what let `cms-fee-schedule` depend on an unpinned wiring."
    );
}

/// The invariant `apps/cms-fee-schedule` bets on: whatever sits at
/// `EngineSet.http` can do binary fetches.
///
/// It calls `ctx.engines.http.fetch_bytes(...)` to download a release ZIP, and
/// it works only because `state.rs` puts the raw `HttpEngine` in that slot. One
/// wiring change — dropping the archive or remote engine in there "so binary
/// fetches get the tier too" — turns that app into a runtime failure with
/// nothing failing first.
#[tokio::test]
async fn whatever_sits_at_engineset_http_can_fetch_bytes() {
    let store = TempStore::new("engine-conformance-wiring").await;
    let origin = binary_origin().await;
    let http = http_engine(&store);

    // An `EngineSet` assembled the way `AppState` assembles one.
    let engines = EngineSet::new(
        http.clone(),
        Arc::new(NoFlows),
        Arc::new(pumper_core::testing::Dead),
        pumper_core::Fetcher::new(
            http.clone(),
            Arc::new(NoFlows),
            Arc::new(pumper_core::testing::Dead),
            Arc::new(Governor::new(&GovernorConfig::default())),
            &pumper_core::config::FetcherConfig::default(),
        ),
    );

    let bytes = engines
        .http
        .fetch_bytes(HttpRequest::get(format!("{origin}/release.zip")))
        .await
        .expect("the engine at EngineSet.http must serve binary bodies");
    assert_eq!(bytes, BINARY_BODY);
}

/// A `Browser` that implements nothing but `render` — i.e. every wrapper, mock
/// and future engine that does not opt into flows. It is not a bespoke stub of
/// the behavior under test: the body being exercised is the **trait's own
/// default**, which is the thing this test exists to pin.
struct NoFlows;

#[async_trait::async_trait]
impl Browser for NoFlows {
    async fn render(&self, _req: RenderRequest) -> Result<RenderedPage> {
        Err(Error::Browser("no rendering in this test".into()))
    }
}

/// A flow every engine must refuse: `submit: true` is rejected pre-flight by
/// `TransactRequest::validate`, which the one engine that implements flows
/// re-runs at its own door before touching Chrome. So this probes the retry
/// class of a refusal on **both** kinds of engine without launching a browser.
fn refused_flow() -> TransactRequest {
    TransactRequest {
        url: "https://example.test/apply".into(),
        profile: None,
        steps: Vec::new(),
        submit_action: PageAction::Click {
            selector: "#submit".into(),
        },
        submit: true,
        idempotency_key: "conformance-probe".into(),
        wait_for_selector: None,
        extra_wait_ms: None,
        max_body_bytes: None,
    }
}

/// THE retry-class bug. An engine that never implemented `transact` used to
/// refuse with `Error::Browser`, which `is_terminal_for_job` classes as
/// **retryable** — so the job re-queued with backoff and re-reached the identical
/// refusal on every attempt. Which engine is wired is fixed for the life of a
/// job, so the second attempt could never learn anything the first did not.
///
/// Run over every `Browser` in the battery, because the obligation is the
/// trait's, not one implementor's: a flow refusal fails ONCE.
#[tokio::test]
async fn a_flow_refusal_fails_once_instead_of_burning_the_retry_ladder() {
    let store = TempStore::new("engine-conformance-flows").await;
    let browsers: Vec<(&str, Arc<dyn Browser>)> = vec![
        // The trait default — what every non-opted-in engine inherits.
        ("Browser::transact default body", Arc::new(NoFlows)),
        // The one production engine that DOES implement flows still owes the
        // same retry class for a request it refuses pre-flight (`submit: true`
        // is re-validated at the engine door, before any Chrome work).
        (
            "engine-browser::BrowserEngine",
            Arc::new(BrowserEngine::new(
                &pumper_core::config::BrowserConfig::default(),
                store.path().join("profiles"),
            )),
        ),
        // `Dead` is the shared test harness's engine, and its contract is that
        // any call is a test bug — so it is deliberately NOT in this list; it
        // panics rather than returning an error to classify.
    ];

    for (name, browser) in &browsers {
        let err = browser
            .transact(refused_flow())
            .await
            .expect_err("a refused flow must not produce an evidence bundle");
        assert!(
            err.is_terminal_for_job(),
            "{name}: a flow refusal came back RETRYABLE ({err}). The refusal is a \
             pure function of the request and the wiring, both immutable for the \
             life of the job — every retry reaches the identical sentence and \
             bills for it."
        );
    }
}

/// A profile name no vault can ever accept: the space is outside
/// `validate_profile_name`'s alphabet, so the refusal is a pure function of the
/// request — the same on attempt 1 and attempt 4.
const UNSAFE_PROFILE: &str = "portal login";

/// THE second retry-class bug, one contract line from the first. A typo'd
/// `profile` was typed `Error::Profile`, which `is_terminal_for_job` classes
/// **retryable**, so a job carrying it burned its whole backoff ladder on four
/// identical refusals — on the most expensive tier, and for an app that ACTS on
/// live pages. A name is frozen into the job row at enqueue; no attempt can
/// learn anything the first did not.
///
/// Run over **every seam that checks a name**, because the obligation belongs to
/// the class of refusal, not to one call site: fixing `transact` and leaving
/// `render` retryable is exactly the half-fix this battery exists to catch. The
/// answers are pinned as an EXPECTED map so a `false` has to be argued in review
/// rather than appearing by accident.
#[tokio::test]
async fn a_deterministic_profile_refusal_fails_once_on_every_seam() {
    let store = TempStore::new("engine-conformance-profile").await;
    let browser = BrowserEngine::new(
        &pumper_core::config::BrowserConfig::default(),
        store.path().join("profiles"),
    );
    let http = http_engine(&store);

    let mut got: BTreeMap<&str, bool> = BTreeMap::new();

    // 1. The browser engine's RENDER seam.
    let mut render = RenderRequest::new("https://example.test/page");
    render.profile = Some(UNSAFE_PROFILE.into());
    let err = browser
        .render(render)
        .await
        .expect_err("an unsafe profile name must never reach Chrome");
    got.insert("engine-browser::render", err.is_terminal_for_job());

    // 2. The browser engine's TRANSACT seam (a different validator, same fact).
    let mut flow = refused_flow();
    flow.submit = false; // isolate the profile refusal from the submit refusal
    flow.profile = Some(UNSAFE_PROFILE.into());
    let err = browser
        .transact(flow)
        .await
        .expect_err("an unsafe profile name must never reach Chrome");
    got.insert("engine-browser::transact", err.is_terminal_for_job());

    // 3. The HTTP tier takes the same parameter, through a different path
    //    (`profile_cookies_path`), and is included so the hole is VISIBLE
    //    rather than merely absent from the battery.
    let mut req = HttpRequest::get("http://127.0.0.1:1/never-fetched");
    req.profile = Some(UNSAFE_PROFILE.into());
    let err = http
        .fetch(req)
        .await
        .expect_err("an unsafe profile name must never open a jar");
    got.insert("engine-http::fetch", err.is_terminal_for_job());

    let expected: BTreeMap<&str, bool> = BTreeMap::from([
        // Both browser seams refuse through `require_safe_profile_name`, which
        // types the refusal `Error::BadRequest` — terminal, and a 400 at the
        // request boundary.
        ("engine-browser::render", true),
        ("engine-browser::transact", true),
        // Closed in the same round it was pinned: `HttpEngine::jar_for` now
        // refuses an unsafe name with the terminal `Error::BadRequest`, before
        // the jar-cache lookup so a cached entry cannot launder one. All three
        // seams that check a profile name now agree, which is the point of an
        // EXPECTED map over seams rather than a test per seam — the gap was
        // visible here for exactly as long as it existed.
        ("engine-http::fetch", true),
    ]);

    assert_eq!(
        got, expected,
        "the retry class of a deterministic profile refusal changed. A refusal \
         that is a pure function of the request must fail ONCE (`true`); a \
         `false` here is a seam that still spends four attempts re-deriving the \
         same sentence, and must be argued in review."
    );
}

// ── inventories: the battery can only run what it can reach ──────────────────

/// Workspace root — `crates/server/../..`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Source trees that live under `src/` but are **not production code**, so the
/// engines in them are fixtures rather than implementors the battery owes
/// anything to. Excluded by path because neither is behind a `#[cfg(test)] mod
/// tests` the line scanner could see:
///
/// - `crates/server/src/e2e/` is the whole e2e suite, gated at its `mod e2e;`
///   declaration (`pumper-server` is binary-only, so its tests live under
///   `src/` to get crate access). Every `HttpClient` in it is a canned stub.
/// - `crates/core/src/testing.rs` is the shared harness, gated by the
///   `test-support` feature and documented as shipping panicking stubs.
const NON_PRODUCTION_PATHS: &[&str] = &["crates/server/src/e2e/", "crates/core/src/testing.rs"];

/// Every **production** `impl HttpClient for X` in the workspace, as
/// `<repo-relative path>::<type>`.
///
/// Test-module implementors are excluded by a `#[cfg(test)]` scan rather than by
/// path: a stub inside `src/` is still inside `src/`, and there are dozens.
fn production_http_clients() -> BTreeMap<String, ()> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let mut found = BTreeMap::new();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        if rel.contains("/tests/")
            || rel.contains("/benches/")
            || NON_PRODUCTION_PATHS.iter().any(|p| rel.starts_with(p))
        {
            continue;
        }
        let mut in_test_mod = false;
        for line in text.lines() {
            let trimmed = line.trim();
            // Everything after the file's `#[cfg(test)] mod tests` is fixtures.
            if trimmed.starts_with("#[cfg(test)]") {
                in_test_mod = true;
            }
            if in_test_mod || trimmed.starts_with("//") {
                continue;
            }
            if let Some(rest) = trimmed
                .strip_prefix("impl HttpClient for ")
                .or_else(|| trimmed.strip_prefix("impl pumper_core::HttpClient for "))
            {
                let ty = rest.trim_end_matches(" {").trim();
                found.insert(format!("{rel}::{ty}"), ());
            }
        }
    }
    found
}

/// The inventory that keeps the battery above honest.
///
/// A battery is only as good as its subject list, and a subject list written by
/// hand rots the moment someone adds an engine. This pins **every** production
/// `HttpClient` in the workspace: each is either in the battery, or listed here
/// with the reason it cannot be — never silently absent, which is the outcome
/// this whole direction exists to end.
#[test]
fn every_production_http_client_is_in_the_battery_or_exempt_with_a_reason() {
    // ── in the battery ───────────────────────────────────────────────────────
    const IN_BATTERY: &[&str] = &[
        "crates/engine-http/src/lib.rs::HttpEngine",
        "crates/engine-archive/src/lib.rs::ArchiveEngine",
        "crates/engine-remote/src/lib.rs::RemoteEngine",
    ];
    // ── out of reach, with the reason ────────────────────────────────────────
    const EXEMPT: &[(&str, &str)] = &[(
        "crates/apps/crawl/src/lib.rs::MeteringHttpClient",
        // A *decorator*, and the one remaining `fetch_bytes` hole: it wraps the
        // raw HTTP client and inherits the trait's refusal, so a binary fetch
        // through the crawler's client would be refused by a wrapper around an
        // engine that supports it — the same class of bug `RemoteEngine` had.
        // Latent today (the crawl frontier never asks for bytes), and unreachable
        // from here because the type is private to `app-crawl`. Closing it means
        // forwarding to `inner`, in that crate.
        "private to app-crawl; decorator that still drops fetch_bytes (latent)",
    )];

    let found = production_http_clients();
    let mut unaccounted: Vec<&String> = found
        .keys()
        .filter(|k| !IN_BATTERY.contains(&k.as_str()) && !EXEMPT.iter().any(|(name, _)| name == k))
        .collect();
    unaccounted.sort();
    assert!(
        unaccounted.is_empty(),
        "these production HttpClient implementors are in neither the conformance \
         battery nor the exemption list: {unaccounted:?}. Add the engine to \
         `every_production_http_client_answers_the_same_battery` (preferred) or \
         list it as exempt with the reason it cannot be reached."
    );

    // The reverse direction: a battery subject that no longer exists means the
    // list is describing an engine that was deleted or renamed.
    for name in IN_BATTERY {
        assert!(
            found.contains_key(*name),
            "{name} is in the battery but no longer exists in the workspace"
        );
    }
    for (name, _) in EXEMPT {
        assert!(
            found.contains_key(*name),
            "{name} is listed exempt but no longer exists — drop the exemption"
        );
    }
}

/// The other half of the `cms-fee-schedule` invariant: the runtime test above
/// proves an `EngineSet` built with the real `HttpEngine` can fetch bytes, but
/// only the server's own wiring decides what actually lands in that slot.
///
/// Pinned as text because there is no type to assert on — `EngineSet.http` is an
/// `Arc<dyn HttpClient>`, which is precisely why the invariant was invisible.
/// If either line moves, re-verify that the engine at `EngineSet.http` still
/// serves binary bodies before updating this test.
#[test]
fn the_server_still_wires_the_binary_capable_engine_at_engineset_http() {
    let state_rs = workspace_root().join("crates/server/src/state.rs");
    let text = std::fs::read_to_string(&state_rs).expect("read state.rs");
    let joined: String = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("//"))
        .collect::<Vec<_>>()
        .join("")
        .split_whitespace()
        .collect();

    assert!(
        joined.contains("lethttp=Arc::new(HttpEngine::new("),
        "state.rs no longer binds `http` to a raw HttpEngine — the engine at \
         EngineSet.http may have changed, and `cms-fee-schedule` calls \
         fetch_bytes on it. (Written without the raw-engine expression itself: \
         fetch_chokepoint.rs scans string literals as code.)"
    );
    assert!(
        joined.contains("EngineSet::new(http,"),
        "state.rs no longer passes `http` as EngineSet's http engine — re-verify \
         that whatever it passes now serves binary bodies (see \
         `whatever_sits_at_engineset_http_can_fetch_bytes`)"
    );
}
