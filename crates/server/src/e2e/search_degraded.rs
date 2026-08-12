//! A search answer says what index it came from.
//!
//! The trap: an enabled-but-EMPTY index (schema drift wipes it and the
//! delta-driven refill only rolls forward) answers `200 {total: 0, hits: []}` —
//! identical to a genuine miss. Both surfaces are pinned here, because the MCP
//! agent is the caller most likely to conclude "this data does not exist" from
//! a silent empty page and report that back as fact.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pumper_core::{Result, SearchDoc, SearchHit, SearchRequest, SearchResponse};
use serde_json::{json, Value};
use tower::ServiceExt;

use super::harness::{test_state_indexed, FakeApp};
use crate::mcp::handle_rpc;
use crate::routes;

/// A `Search` with a scriptable corpus size that always matches nothing — the
/// two axes this feature reads (`doc_count`, and an empty hit list) made
/// independent, so "empty page" and "empty index" can be told apart in a test
/// exactly as the response now tells them apart for a caller.
struct SizedSearch(u64);

#[async_trait::async_trait]
impl pumper_core::Search for SizedSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse::default())
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(self.0)
    }
}

/// A populated index that simply did not match — the control case.
struct PopulatedSearch;

#[async_trait::async_trait]
impl pumper_core::Search for PopulatedSearch {
    async fn index(&self, _docs: Vec<SearchDoc>) -> Result<()> {
        Ok(())
    }
    async fn query(&self, _req: SearchRequest) -> Result<SearchResponse> {
        Ok(SearchResponse {
            total: 1,
            hits: vec![SearchHit {
                id: "fake:1".into(),
                app: "fake".into(),
                dataset: "_records".into(),
                title: "a hit".into(),
                url: "https://example.test/1".into(),
                snippet: "a <b>hit</b>".into(),
                score: 1.0,
            }],
            ..Default::default()
        })
    }
    async fn delete_ids(&self, _ids: &[String]) -> Result<()> {
        Ok(())
    }
    async fn delete_dataset(&self, _app: &str, _dataset: &str) -> Result<()> {
        Ok(())
    }
    async fn doc_count(&self) -> Result<u64> {
        Ok(4_200)
    }
}

async fn http_search(search: Arc<dyn pumper_core::Search>, enabled: bool) -> (StatusCode, Value) {
    let (state, _store) = test_state_indexed(vec![Arc::new(FakeApp)], search, |c| {
        c.search.enabled = enabled;
    })
    .await;
    let router = routes::router(state);
    let resp = router
        .oneshot(
            Request::builder()
                .uri("/search?q=grants")
                .body(Body::empty())
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
async fn wiped_index_says_so_not_silent_empty() {
    let (status, body) = http_search(Arc::new(SizedSearch(0)), true).await;
    assert_eq!(status, StatusCode::OK, "a wiped index is not an error");
    assert_eq!(body["total"], 0);
    assert_eq!(
        body["index"]["degraded"], true,
        "an enabled index holding 0 docs must not answer like an honest miss: {body}"
    );
    assert_eq!(body["index"]["doc_count"], 0);
    assert!(
        body["index"]["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("search-backfill"),
        "the reason names the recovery: {body}"
    );
    // Additive only — nothing the existing consumers read was renamed.
    for key in ["query", "total", "count", "hits", "facets"] {
        assert!(
            !body[key].is_null(),
            "missing pre-existing key {key}: {body}"
        );
    }
}

#[tokio::test]
async fn disabled_search_says_disabled_not_no_matches() {
    let (status, body) = http_search(Arc::new(SizedSearch(0)), false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["index"]["enabled"], false);
    assert_eq!(body["index"]["degraded"], true);
    assert!(
        body["index"]["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("disabled"),
        "a disabled index is a different degradation from a wiped one: {body}"
    );
}

#[tokio::test]
async fn a_populated_index_is_not_flagged_degraded() {
    let (status, body) = http_search(Arc::new(PopulatedSearch), true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["index"]["degraded"], false);
    assert_eq!(body["index"]["doc_count"], 4_200);
    assert!(body["index"]["reason"].is_null(), "{body}");
}

/// The MCP `search` tool answers through the same renderer, so an agent cannot
/// be handed a silent empty page while the HTTP surface tells the truth.
#[tokio::test]
async fn the_mcp_search_tool_carries_the_same_index_signal() {
    let (mut state, _store) =
        test_state_indexed(vec![Arc::new(FakeApp)], Arc::new(SizedSearch(0)), |_| {}).await;
    let mut config = (*state.config).clone();
    config.mcp.enabled = true;
    state.config = Arc::new(config);

    let resp = handle_rpc(
        &state,
        &json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "search", "arguments": { "q": "grants" } }
        }),
    )
    .await
    .expect("tools/call gets a response");
    let out = &resp["result"]["structuredContent"];
    assert_eq!(resp["result"]["isError"], false, "{resp}");
    assert_eq!(out["total"], 0);
    assert_eq!(
        out["index"]["degraded"], true,
        "the agent surface must carry the same signal: {out}"
    );
    assert!(
        out["facets"].is_null(),
        "the tool still returns no facets — the shared renderer keeps that difference: {out}"
    );
}
