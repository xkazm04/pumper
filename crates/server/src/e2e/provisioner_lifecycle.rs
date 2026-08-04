//! End-to-end over `POST /provisioner/proposals/{key}/validate` and
//! `.../promote` plus `GET /provisioner/proposals`, driving the REAL
//! `app-provisioner` crate (stubbed engines) through the whole lifecycle:
//! propose -> list -> validate -> promote -> expired. Mirrors how
//! `fetch_proxy.rs` exercises a real engine against the live HTTP router
//! rather than calling handler functions directly.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use http_body_util::BodyExt;
use pumper_core::config::{Config, GovernorConfig, ProvisionerConfig};
use pumper_core::testing::{
    engines_with, research_output, Dead, ScriptedResearcher, TempStore, TestContext,
};
use pumper_core::{
    Governor, HttpClient, HttpRequest, HttpResponse, NoPlugins, NoSearch, Result, ScrapeApp,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::routes;
use crate::state::{AppState, AppStateParts};

/// Long enough to clear the fetcher's 250-char escalation floor, so the http
/// tier wins outright and this test never needs a browser stub — the same
/// shape `app_provisioner`'s own e2e fixture uses.
const LISTING_PAGE: &str = r#"<html><head><title>Widget Price Index</title></head><body>
    <h1>Widget Prices</h1>
    <p>The widget price index is published every week and tracks the retail
    price of the most commonly traded widget models across the domestic
    market. Prices are collected from published retailer listings and are
    stated in United States dollars.</p>
    <div id="list">
        <div class="card"><h3>Alpha</h3><span class="price">$10</span></div>
        <div class="card"><h3>Beta</h3><span class="price">$20</span></div>
    </div></body></html>"#;

/// Serves [`LISTING_PAGE`] for BOTH the compile's sampling fetch and the
/// validate route's fresh re-fetch — proving `POST .../validate` really does
/// perform a live fetch through the shared engine seam rather than replaying
/// the stored sample.
struct ListingHost;

#[async_trait::async_trait]
impl HttpClient for ListingHost {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: LISTING_PAGE.to_string(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// Scripts the two `ctx.research` calls a compile makes (discovery, then one
/// draft) — matched by substring, so it answers for any prompt.
fn scripted_researcher() -> Arc<ScriptedResearcher> {
    Arc::new(
        ScriptedResearcher::new()
            .on(
                "Goal:",
                research_output(
                    json!({"candidates": [{
                        "url": "https://a.example/widgets",
                        "name": "Widget Price Index",
                        "cadence": "weekly",
                        "expected_fields": ["name", "price"]
                    }]})
                    .to_string(),
                ),
            )
            .on(
                "Draft extraction rules",
                research_output(
                    json!({
                        "heading": {"type": "css", "selector": "h1"},
                        "items": {"type": "each", "selector": ".card", "container": "#list",
                                  "fields": {"name": {"type": "css", "selector": "h3"},
                                             "price": {"type": "css", "selector": ".price"}}}
                    })
                    .to_string(),
                ),
            ),
    )
}

/// A headless state with the real `provisioner` app registered and
/// [`ListingHost`] + [`scripted_researcher`] wired as its engines.
async fn lifecycle_state(proposal_max_age_secs: u64) -> (AppState, TempStore) {
    let store = TempStore::new("provisioner-lifecycle-e2e").await;
    let mut config = Config::default();
    config.storage.database_path = store.path().join("pumper.db");
    config.storage.artifacts_dir = store.path().join("artifacts");
    config.provisioner = ProvisionerConfig {
        proposal_max_age_secs,
    };
    let mut registry: HashMap<String, Arc<dyn ScrapeApp>> = HashMap::new();
    registry.insert("provisioner".into(), Arc::new(app_provisioner::Provisioner));
    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor: Arc::new(Governor::new(&GovernorConfig::default())),
        engines: engines_with(Arc::new(ListingHost), Arc::new(Dead), scripted_researcher()),
        plugins: Arc::new(NoPlugins),
        search: Arc::new(NoSearch),
        registry,
    })
    .expect("assemble provisioner-lifecycle test state");
    (state, store)
}

/// Runs the REAL `run()` — the same path `POST /apps/provisioner/jobs` drives
/// a job through, minus the queue — so the proposal record this test lists /
/// validates / promotes is exactly what a live compile would have written.
async fn propose(state: &AppState, prompt: &str) -> Value {
    let ctx = TestContext::new(&state.storage, "provisioner")
        .params(json!({ "prompt": prompt }))
        .engines(state.engines.clone())
        .build();
    app_provisioner::Provisioner
        .run(ctx)
        .await
        .expect("a reachable candidate must not hard-error the compile")
}

async fn body_json(resp: Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn get(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    body_json(resp).await
}

async fn post(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    body_json(resp).await
}

/// Finds one proposal's list row by key, panicking with the full list on a miss
/// (much easier to debug than a bare `None`).
fn find_row<'a>(items: &'a [Value], key: &str) -> &'a Value {
    items
        .iter()
        .find(|r| r["key"] == json!(key))
        .unwrap_or_else(|| panic!("proposal '{key}' not in list: {items:?}"))
}

#[tokio::test]
async fn propose_list_validate_promote_is_the_real_lifecycle_end_to_end() {
    // A tight window: exercised directly in the "expired" stage below, and
    // proven NOT to false-positive on the fresh proposal in the "list" stage.
    let (state, _store) = lifecycle_state(1_000).await;
    let router = routes::router(state.clone());

    // ── propose ──────────────────────────────────────────────────────────
    let out = propose(&state, "track widget prices weekly").await;
    let key = out["proposal_key"].as_str().unwrap().to_string();
    assert_eq!(out["accepted"], json!(true), "{out}");

    // ── list: fresh, planned, not expired ───────────────────────────────
    let (status, body) = get(&router, "/provisioner/proposals").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let items = body.as_array().expect("bare array without cursor=");
    let row = find_row(items, &key);
    assert_eq!(row["status"], json!("planned"));
    assert_eq!(row["expired"], json!(false));
    assert_eq!(row["verdict"], json!("accepted"));
    assert_eq!(row["engine"], json!("http"));
    assert_eq!(row["url"], json!("https://a.example/widgets"));

    // ── validate: a FRESH fetch through the same stubbed engine ─────────
    let (status, body) = post(&router, &format!("/provisioner/proposals/{key}/validate")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("validated"));
    assert_eq!(body["validation"]["accepted"], json!(true));
    assert_eq!(body["validation"]["sample"]["engine"], json!("http"));

    let (_, body) = get(&router, "/provisioner/proposals").await;
    let row = find_row(body.as_array().unwrap(), &key);
    assert_eq!(
        row["status"],
        json!("validated"),
        "list reflects the new status"
    );

    // ── promote: the paste-ready, still-inert TOML fragment ─────────────
    let (status, body) = post(&router, &format!("/provisioner/proposals/{key}/promote")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], json!("promoted"));
    let toml = body["catalog_toml"].as_str().expect("catalog_toml string");
    assert!(toml.contains("status = \"planned\""), "{toml}");
    assert!(toml.contains("cron = \"\""), "{toml}");
    assert!(toml.contains("https://a.example/widgets"), "{toml}");

    // Nothing was ever written to the actual catalog — this route only ever
    // returns the fragment.
    let catalog_path = std::path::Path::new("catalog/data-sources.toml");
    if catalog_path.exists() {
        let on_disk = std::fs::read_to_string(catalog_path).unwrap_or_default();
        assert!(
            !on_disk.contains(&key),
            "promote must never touch the real catalog file"
        );
    }

    // ── every transition is a recorded revision ──────────────────────────
    let (status, hist) = get(
        &router,
        &format!("/datasets/provisioner/proposals/history?key={key}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hist}");
    let statuses: Vec<String> = hist["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|r| r["data"]["status"].as_str().map(str::to_string))
        .collect();
    assert!(statuses.contains(&"planned".to_string()), "{statuses:?}");
    assert!(statuses.contains(&"validated".to_string()), "{statuses:?}");
    assert!(statuses.contains(&"promoted".to_string()), "{statuses:?}");

    // ── expired: a SEPARATE, still-planned proposal aged past the window ──
    let out2 = propose(&state, "track gadget prices weekly").await;
    let key2 = out2["proposal_key"].as_str().unwrap().to_string();
    sqlx::query(
        "UPDATE records SET updated_at = ?1 WHERE app = 'provisioner' AND dataset = 'proposals' \
         AND key = ?2",
    )
    .bind((chrono::Utc::now() - chrono::Duration::seconds(2_000)).to_rfc3339())
    .bind(&key2)
    .execute(&state.storage.pool())
    .await
    .expect("backdate the second proposal for the expiry check");

    let (_, body) = get(&router, "/provisioner/proposals").await;
    let items = body.as_array().unwrap();
    let stale_row = find_row(items, &key2);
    assert_eq!(stale_row["status"], json!("planned"));
    assert_eq!(
        stale_row["expired"],
        json!(true),
        "a planned proposal aged past proposal_max_age_secs must report expired"
    );
    // The first proposal, now `promoted`, is never flagged expired regardless
    // of age — only a still-`planned` proposal can be rotting.
    let promoted_row = find_row(items, &key);
    assert_eq!(promoted_row["expired"], json!(false));
}

#[tokio::test]
async fn unknown_key_is_404_for_validate_and_promote() {
    let (state, _store) = lifecycle_state(1_000).await;
    let router = routes::router(state);

    let (status, body) = post(&router, "/provisioner/proposals/nope/validate").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = post(&router, "/provisioner/proposals/nope/promote").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

/// The promotion gate's own job (`app_provisioner::may_promote`) is unit-tested
/// in the app crate; this proves the ROUTE actually wires it — a proposal
/// whose status is already `failed` must 409, not hand out a fragment.
#[tokio::test]
async fn a_failed_validation_blocks_promotion_with_409() {
    let (state, _store) = lifecycle_state(1_000).await;
    state
        .datasets
        .upsert_stamped(
            "provisioner",
            "proposals",
            "stale",
            &json!({
                "prompt": "p",
                "catalog_row": {
                    "id": "stale", "app": "", "market": "", "name": "n",
                    "url": "https://x.example", "category": "", "engine": "http",
                    "access": "scrape", "cadence": "weekly", "cron": "", "status": "planned",
                    "confidence": 1, "dataset": "proposed:stale", "notes": "n",
                },
                "rule_set": {}, "seeds": [], "samples": [], "cadence": "weekly",
                "budget": Value::Null, "sample_stats": {}, "confidence": 0,
                "confidence_scale": "x", "catalog_confidence": 1, "accepted": true,
                "verdict": "accepted", "provisioned": false, "status": "failed",
                "intended_dataset": "proposed:stale", "iterations": 1, "cost_usd": 0.0,
            }),
            None,
            None,
        )
        .await
        .expect("seed a failed proposal directly");

    let router = routes::router(state);
    let (status, body) = post(&router, "/provisioner/proposals/stale/promote").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}
