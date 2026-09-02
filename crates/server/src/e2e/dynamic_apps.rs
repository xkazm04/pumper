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

use pumper_core::JobStatus;
use serde_json::json;

use super::harness::{test_state_with, FakeApp};
use crate::{routes, worker};

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
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn dynamic_app_is_listed_read_only_and_enqueue_is_rejected() {
    let app_dir =
        std::env::temp_dir().join(format!("pumper-e2e-dynamic-apps-{}", std::process::id()));
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
    let fake = apps
        .iter()
        .find(|a| a["name"] == "fake")
        .expect("static app listed");
    assert!(
        fake.get("dynamic").is_none(),
        "static entries are unchanged"
    );
    let quotes = apps
        .iter()
        .find(|a| a["name"] == "quotes")
        .expect("dynamic app listed");
    assert_eq!(quotes["dynamic"], true);
    assert_eq!(quotes["runnable"], false);
    assert_eq!(quotes["ready"], false);
    assert_eq!(quotes["description"], "quote scraper");
    assert_eq!(quotes["params_schema"]["type"], "object");
    assert!(
        quotes["reason"]
            .as_str()
            .unwrap()
            .contains("describe-only core module"),
        "reason explains what this file is and why it cannot run"
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
        body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["name"] != "quotes"),
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
    assert!(
        msg.contains("not runnable") && msg.contains("describe-only core module"),
        "{msg}"
    );

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

// ---- Runnable components (N09) ---------------------------------------------

/// The shared conformance fixture: the smallest real `pumper:app@0.1.0`
/// component, compiled here from the component text format so this gate needs
/// no external wasm toolchain (see the fixture's own header).
fn echo_component() -> Vec<u8> {
    wat::parse_file("../engine-wasm/tests/fixtures/echo-app.wat").expect("fixture compiles")
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "pumper-e2e-wasm-app-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The whole N09 claim, end to end and through the real worker: a component
/// dropped in `[plugins] app_dir` is a registered app — enqueueable at the
/// normal door, executed by the normal loop, its result stored like any other
/// job's — with no Rust build and no registry edit.
#[tokio::test]
async fn a_component_is_registered_and_runs_through_the_worker() {
    let app_dir = scratch_dir("runs");
    let module = echo_component();
    std::fs::write(app_dir.join("echo.wasm"), &module).unwrap();
    let sha = pumper_engine_wasm::app_host::module_sha256(&module);

    let dir = app_dir.clone();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], move |config| {
        config.plugins.app_dir = Some(dir);
        config.wasm_apps.enabled = true;
    })
    .await;
    let router = routes::router(state.clone());

    // Listed ONCE, runnable, and carrying which build is answering.
    let (status, body) = request(
        &router,
        Request::builder().uri("/apps").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listed: Vec<&Value> = body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["name"] == "echo")
        .collect();
    assert_eq!(
        listed.len(),
        1,
        "a runnable dynamic app is in the registry AND the dynamic list — it must \
         still appear exactly once: {body}"
    );
    let echo = listed[0];
    assert_eq!(echo["dynamic"], true);
    assert_eq!(echo["runnable"], true);
    assert_eq!(echo["ready"], true);
    assert_eq!(echo["module_sha256"], sha);
    assert_eq!(echo["pinned"], false, "nothing in the catalog pins it");
    assert_eq!(echo["description"], "echo app");

    // An agent can see it as a tool, because it can now actually call it.
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
        body["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "echo"),
        "a runnable dynamic app IS an offerable tool: {body}"
    );

    // Enqueue at the ordinary door — not a 409.
    let (status, body) = request(
        &router,
        Request::post("/apps/echo/jobs")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let job_id: uuid::Uuid = serde_json::from_value(body["id"].clone()).unwrap();

    // And the ordinary worker loop runs it.
    assert!(worker::run_one(&state).await, "the worker claims the job");
    let row = state.storage.get(job_id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Succeeded, "{:?}", row.error);
    let result = row.result.expect("a stored result");
    assert_eq!(result["ok"], true, "the guest's own JSON is the job result");
    assert_eq!(
        result["module_sha256"], sha,
        "the result names the build that produced it"
    );
    assert_eq!(
        result["wasm"]["fuel_budget"],
        json!(pumper_core::config::WasmAppsConfig::default().fuel_per_job),
        "the run reports the budget it ran under: {result}"
    );

    let _ = std::fs::remove_dir_all(&app_dir);
}

/// Default-OFF, and it must mean the host does not exist — not that it exists
/// and refuses. The component is still LISTED (an operator has to be able to
/// see what the dir holds) with a reason naming the switch, and enqueue is the
/// same typed 409 a describe-only module gets.
#[tokio::test]
async fn a_component_is_inert_until_wasm_apps_is_enabled() {
    let app_dir = scratch_dir("off");
    std::fs::write(app_dir.join("echo.wasm"), echo_component()).unwrap();

    let dir = app_dir.clone();
    let (state, _store) = test_state_with(vec![Arc::new(FakeApp)], move |config| {
        config.plugins.app_dir = Some(dir);
        // config.wasm_apps.enabled stays at its default
    })
    .await;
    assert!(
        !state.registry.contains_key("echo"),
        "a disabled host must register nothing"
    );
    let router = routes::router(state);

    let (status, body) = request(
        &router,
        Request::builder().uri("/apps").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let echo = body["apps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "echo")
        .expect("still listed — an operator must see what the dir holds");
    assert_eq!(echo["runnable"], false);
    assert!(
        echo["reason"].as_str().unwrap().contains("wasm_apps"),
        "the reason names the switch: {echo}"
    );

    let (status, _) = request(
        &router,
        Request::post("/apps/echo/jobs")
            .header("content-type", "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let _ = std::fs::remove_dir_all(&app_dir);
}
