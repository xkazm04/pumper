//! Dynamic WASM apps (M28 v1 slice): `[plugins] app_dir` discovery is listing
//! ONLY. These tests pin the whole contract end to end through the real
//! router: a discovered module shows up in `GET /apps` as `dynamic: true,
//! runnable: false` with a reason, is excluded from `?format=tools`, and an
//! enqueue attempt is a typed 409 — never a job.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::harness::{test_state_with, FakeApp};
use crate::routes;

/// A wasm-text module (wasmtime's default `wat` feature compiles it straight
/// from the file) exporting `describe()` → packed ptr/len of a JSON manifest.
fn describing_wat(manifest_json: &str) -> String {
    let escaped = manifest_json.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        "(module (memory (export \"memory\") 1) (data (i32.const 16) \"{escaped}\") \
         (func (export \"describe\") (result i64) \
           (i64.or (i64.shl (i64.const 16) (i64.const 32)) (i64.const {len}))))",
        len = manifest_json.len()
    )
}

async fn request(router: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn dynamic_app_is_listed_read_only_and_enqueue_is_rejected() {
    let app_dir = std::env::temp_dir().join(format!(
        "pumper-e2e-dynamic-apps-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&app_dir);
    std::fs::create_dir_all(&app_dir).unwrap();
    std::fs::write(
        app_dir.join("quotes.wasm"),
        describing_wat(r#"{"description":"quote scraper","params_schema":{"type":"object"}}"#),
    )
    .unwrap();

    let dir = app_dir.clone();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], move |config| {
        config.plugins.app_dir = Some(dir);
    })
    .await;
    let router = routes::router(state);

    // Listing: static app untouched, dynamic app appended read-only.
    let (status, body) = request(
        &router,
        Request::builder().uri("/apps").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let apps = body["apps"].as_array().unwrap();
    let fake = apps.iter().find(|a| a["name"] == "fake").expect("static app listed");
    assert!(fake.get("dynamic").is_none(), "static entries are unchanged");
    let quotes = apps.iter().find(|a| a["name"] == "quotes").expect("dynamic app listed");
    assert_eq!(quotes["dynamic"], true);
    assert_eq!(quotes["runnable"], false);
    assert_eq!(quotes["ready"], false);
    assert_eq!(quotes["description"], "quote scraper");
    assert_eq!(quotes["params_schema"]["type"], "object");
    assert!(
        quotes["reason"].as_str().unwrap().contains("component-model host"),
        "reason explains what is missing"
    );

    // Tools view: a tool an agent cannot call must not be advertised.
    let (status, body) = request(
        &router,
        Request::builder()
            .uri("/apps?format=tools")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["tools"].as_array().unwrap().iter().all(|t| t["name"] != "quotes"),
        "dynamic apps are excluded from ?format=tools"
    );

    // Enqueue: typed 409 carrying the same reason — and no job created.
    let (status, body) = request(
        &router,
        Request::post("/apps/quotes/jobs")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let msg = body["error"].as_str().unwrap();
    assert!(msg.contains("not runnable") && msg.contains("component-model host"), "{msg}");

    // A name known to neither surface stays a plain 404.
    let (status, _) = request(
        &router,
        Request::post("/apps/nonexistent/jobs")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let _ = std::fs::remove_dir_all(&app_dir);
}
