//! Behavioural tests for the LightTrack emitter fired from the
//! [`pumper_core::AppContext::research`] chokepoint (`crates/core/src/app.rs`),
//! next to `crates/core/src/lighttrack.rs`'s own unit tests of the pure
//! env-config and body-shaping logic.
//!
//! These drive the emitter end to end: a real (loopback) TCP listener stands
//! in for the LightTrack collector, so what lands on the wire — or doesn't —
//! is what's asserted on, not just what the code intends to send.

use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use pumper_core::error::{ClaudeFailure, ClaudeSpend};
use pumper_core::testing::TestContext;
use pumper_core::testing::{engines_with, research_output, Dead, ScriptedResearcher, TempStore};
use pumper_core::ResearchRequest;

/// `LIGHTTRACK_URL`/`LIGHTTRACK_KEY` are process-global; serialize every test
/// in this file that touches them so they can't race each other.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn clear_env() {
    std::env::remove_var("LIGHTTRACK_URL");
    std::env::remove_var("LIGHTTRACK_KEY");
}

/// A minimal loopback HTTP/1.1 collector: accept one connection, read the
/// request (headers + `Content-Length` body), hand back a bare `200`, and
/// return the parsed body plus the lower-cased header map so a test can
/// assert on `authorization` too.
async fn accept_one_request(
    listener: &TcpListener,
) -> (Value, std::collections::HashMap<String, String>) {
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("no connection reached the collector within 2s")
        .expect("accept failed");

    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk).await.expect("read failed");
        assert!(n > 0, "connection closed before headers completed");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut headers = std::collections::HashMap::new();
    let mut content_length = 0usize;
    for line in head.split("\r\n").skip(1) {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim().to_string();
            if k == "content-length" {
                content_length = v.parse().unwrap_or(0);
            }
            headers.insert(k, v);
        }
    }
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut chunk).await.expect("read failed");
        assert!(n > 0, "connection closed before body completed");
        body.extend_from_slice(&chunk[..n]);
    }
    let _ = stream
        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
        .await;

    let json: Value = serde_json::from_slice(&body).expect("collector body was not JSON");
    (json, headers)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Proof that a connection was NOT made within a short grace window — the
/// inert-by-default guarantee, from the outside.
async fn assert_no_connection(listener: &TcpListener) {
    match tokio::time::timeout(Duration::from_millis(300), listener.accept()).await {
        Err(_) => {} // timed out waiting — exactly what "no traffic" looks like
        Ok(Ok(_)) => panic!("a connection reached the collector despite LIGHTTRACK_URL unset"),
        Ok(Err(e)) if e.kind() == ErrorKind::WouldBlock => {}
        Ok(Err(e)) => panic!("unexpected accept error: {e}"),
    }
}

async fn local_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    (listener, format!("http://{addr}"))
}

#[tokio::test]
async fn unconfigured_lighttrack_emits_no_network_traffic() {
    let _guard = ENV_LOCK.lock().await;
    clear_env();

    let (listener, url) = local_listener().await;
    // Deliberately NOT set as LIGHTTRACK_URL — a real collector is listening,
    // but this run never opted in, so it must never be told about it.
    let _unused = url;

    let store = TempStore::new("lighttrack-unconfigured").await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("answer"));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine))
        .build();

    ctx.research(ResearchRequest::new("what changed?").with_use_case("connector.change_summary"))
        .await
        .unwrap();

    assert_no_connection(&listener).await;
    clear_env();
}

#[tokio::test]
async fn a_successful_call_posts_the_use_case_key_as_name() {
    let _guard = ENV_LOCK.lock().await;
    clear_env();
    let (listener, url) = local_listener().await;
    std::env::set_var("LIGHTTRACK_URL", &url);
    std::env::set_var("LIGHTTRACK_KEY", "test-key-123");

    let store = TempStore::new("lighttrack-success").await;
    let mut out = research_output("summary text");
    out.cost_usd = Some(0.0421);
    out.model = Some("claude-sonnet-5".to_string());
    let engine = Arc::new(ScriptedResearcher::new().on("", out));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine))
        .build();

    ctx.research(
        ResearchRequest::new("summarize the diff").with_use_case("connector.change_summary"),
    )
    .await
    .unwrap();

    let (body, headers) = accept_one_request(&listener).await;
    assert_eq!(body["project_id"], "pumper");
    assert_eq!(body["provider"], "anthropic");
    assert_eq!(body["operation"], "chat");
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["name"], "connector.change_summary",
        "the declared use-case key must reach LightTrack as `name`: {body}"
    );
    assert_eq!(body["model"], "claude-sonnet-5");
    assert!(body.get("error").is_none(), "a success must omit `error`");
    assert!((body["cost_usd"].as_f64().unwrap() - 0.0421).abs() < 1e-9);
    assert_eq!(
        headers.get("authorization").map(String::as_str),
        Some("Bearer test-key-123")
    );

    clear_env();
}

#[tokio::test]
async fn a_failed_call_still_emits_carrying_its_reported_spend() {
    let _guard = ENV_LOCK.lock().await;
    clear_env();
    let (listener, url) = local_listener().await;
    std::env::set_var("LIGHTTRACK_URL", &url);

    let store = TempStore::new("lighttrack-failure").await;
    let engine = Arc::new(ScriptedResearcher::new().on_failure(
        "",
        "cli reported error: budget exceeded mid-run",
        ClaudeSpend::reported(ClaudeFailure::CliError, 0.55),
    ));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine))
        .budget_usd(10.0)
        .build();

    ctx.research(
        ResearchRequest::new("an expensive question").with_use_case("provisioner.draft_proposal"),
    )
    .await
    .expect_err("the engine failed");

    let (body, _headers) = accept_one_request(&listener).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["name"], "provisioner.draft_proposal");
    assert!(
        (body["cost_usd"].as_f64().unwrap() - 0.55).abs() < 1e-9,
        "the emitted spend must match what the internal ledger just recorded: {body}"
    );
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("budget exceeded mid-run"),
        "{body}"
    );

    clear_env();
}

#[tokio::test]
async fn a_call_that_never_ran_emits_nothing() {
    let _guard = ENV_LOCK.lock().await;
    clear_env();
    let (listener, url) = local_listener().await;
    std::env::set_var("LIGHTTRACK_URL", &url);

    let store = TempStore::new("lighttrack-spawn-fail").await;
    let engine = Arc::new(ScriptedResearcher::new().on_failure(
        "",
        "failed to spawn 'claude': not found",
        ClaudeSpend::unreported(ClaudeFailure::Spawn),
    ));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine))
        .build();

    ctx.research(ResearchRequest::new("anything").with_use_case("state_tax.extract"))
        .await
        .expect_err("the engine could not start");

    // No process ran, so the internal ledger wrote no row either (see
    // `llm_chokepoint.rs::a_call_that_never_ran_writes_no_cost_row`) — the
    // external sink must stay silent for exactly the same reason.
    assert_no_connection(&listener).await;

    clear_env();
}
