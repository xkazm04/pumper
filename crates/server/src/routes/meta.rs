//! Liveness, Prometheus metrics, the OpenAPI document, and the app registry
//! listing — the service's self-description endpoints.

use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// Serves the generated OpenAPI 3.1 document. The spec is rebuilt from the same
/// route registration used by `router`, so it always matches what is served.
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "meta",
    responses((status = 200, description = "OpenAPI 3.1 document for this API"))
)]
pub(crate) async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(super::openapi_router().split_for_parts().1)
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses((status = 200, description = "Service is up (`{\"status\":\"ok\"}`)"))
)]
pub(crate) async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// ---- Observability --------------------------------------------------------

/// How long a rendered `/metrics` body is served from cache before the aggregate
/// queries are re-run. Short enough that a scrape is never meaningfully stale.
const METRICS_TTL: std::time::Duration = std::time::Duration::from_secs(5);

fn metrics_response(body: String) -> Response {
    ([("content-type", "text/plain; version=0.0.4")], body).into_response()
}

/// Prometheus-style text exposition of queue + platform gauges. Cached for
/// `METRICS_TTL` so a burst of scrapes doesn't re-run the aggregate queries each
/// time (the render touches jobs, costs, schedules, and timing in one pass).
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "health",
    responses((status = 200, description = "Prometheus text exposition (content-type text/plain; version=0.0.4)", content_type = "text/plain"))
)]
pub(crate) async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    {
        let cached = state.metrics_cache.lock().await;
        if let Some((at, body)) = cached.as_ref() {
            if at.elapsed() < METRICS_TTL {
                return Ok(metrics_response(body.clone()));
            }
        }
    }

    let counts = state.storage.status_counts().await?;
    let failures = state.storage.failure_counts().await?;
    let timing = state.storage.job_timing_stats().await?;
    let schedules = state.storage.list_schedules().await?;
    let mut out = String::new();
    out.push_str("# HELP pumper_jobs Jobs by status\n# TYPE pumper_jobs gauge\n");
    for status in ["queued", "running", "succeeded", "failed", "cancelled"] {
        let n = counts.iter().find(|(s, _)| s == status).map_or(0, |(_, n)| *n);
        out.push_str(&format!("pumper_jobs{{status=\"{status}\"}} {n}\n"));
    }
    // Permanent failures per app. DB-derived (current `failed` row count per app),
    // so not strictly monotonic — a retried job leaves the failed set.
    out.push_str(
        "# HELP pumper_job_failures_total Permanently-failed jobs by app (DB-derived count)\n\
         # TYPE pumper_job_failures_total counter\n",
    );
    for (app, n) in &failures {
        out.push_str(&format!("pumper_job_failures_total{{app=\"{app}\"}} {n}\n"));
    }
    out.push_str(
        "# HELP pumper_job_duration_seconds Job execution time (started -> finished)\n\
         # TYPE pumper_job_duration_seconds summary\n",
    );
    out.push_str(&format!(
        "pumper_job_duration_seconds_sum {}\npumper_job_duration_seconds_count {}\n",
        timing.duration_sum, timing.duration_count
    ));
    out.push_str(
        "# HELP pumper_job_duration_seconds_max Longest job execution time\n\
         # TYPE pumper_job_duration_seconds_max gauge\n",
    );
    out.push_str(&format!("pumper_job_duration_seconds_max {}\n", timing.duration_max));
    out.push_str(
        "# HELP pumper_job_queue_wait_seconds Queue wait (created -> started)\n\
         # TYPE pumper_job_queue_wait_seconds summary\n",
    );
    out.push_str(&format!(
        "pumper_job_queue_wait_seconds_sum {}\npumper_job_queue_wait_seconds_count {}\n",
        timing.wait_sum, timing.wait_count
    ));
    out.push_str(
        "# HELP pumper_job_queue_wait_seconds_max Longest queue wait\n\
         # TYPE pumper_job_queue_wait_seconds_max gauge\n",
    );
    out.push_str(&format!("pumper_job_queue_wait_seconds_max {}\n", timing.wait_max));
    out.push_str("# HELP pumper_cost_usd Total engine spend by app\n# TYPE pumper_cost_usd gauge\n");
    for entry in state.costs.summary(None, None).await? {
        out.push_str(&format!(
            "pumper_cost_usd{{app=\"{}\",engine=\"{}\"}} {}\n",
            entry.app, entry.engine, entry.cost_usd
        ));
    }
    out.push_str(
        "# HELP pumper_apps Registered apps (ready = all preconditions satisfied)\n\
         # TYPE pumper_apps gauge\n",
    );
    let ready_apps = state
        .registry
        .values()
        .filter(|a| a.requires().iter().all(|r| r.is_satisfied()))
        .count();
    out.push_str(&format!("pumper_apps{{ready=\"true\"}} {ready_apps}\n"));
    out.push_str(&format!(
        "pumper_apps{{ready=\"false\"}} {}\n",
        state.registry.len() - ready_apps
    ));
    out.push_str("# HELP pumper_schedules Configured schedules\n# TYPE pumper_schedules gauge\n");
    let enabled = schedules.iter().filter(|s| s.enabled).count();
    out.push_str(&format!("pumper_schedules{{enabled=\"true\"}} {enabled}\n"));
    out.push_str(&format!(
        "pumper_schedules{{enabled=\"false\"}} {}\n",
        schedules.len() - enabled
    ));

    *state.metrics_cache.lock().await = Some((std::time::Instant::now(), out.clone()));
    Ok(metrics_response(out))
}

// ---- Apps -----------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/apps",
    tag = "apps",
    responses((status = 200, description = "`{apps: [{name, description, schedule, requires, ready, default_params}]}` — `requires` lists preconditions (e.g. `env:CENSUS_API_KEY`); `ready` is false when any is unmet here; `default_params` is the app's default job params (a POST body's `params` shallow-merges over these)."))
)]
pub(crate) async fn list_apps(State(state): State<AppState>) -> Json<Value> {
    let mut apps: Vec<_> = state.registry.values().collect();
    apps.sort_by_key(|app| app.name());
    let apps: Vec<_> = apps
        .into_iter()
        .map(|app| {
            let requires: Vec<String> = app.requires().iter().map(|r| r.label()).collect();
            // `ready` = every declared precondition is satisfied here (e.g. the
            // required API-key env var is set), so a credential-gated app is
            // distinguishable from a runnable one before its first failed job.
            let ready = app.requires().iter().all(|r| r.is_satisfied());
            json!({
                "name": app.name(),
                "description": app.description(),
                "schedule": app.schedule(),
                "requires": requires,
                "ready": ready,
                // Machine-readable defaults so a client can see exactly which keys
                // it is overriding — the replace-vs-merge fix below is only safe
                // because the caller can now see what it is merging over.
                "default_params": app.default_params(),
            })
        })
        .collect();
    Json(json!({ "apps": apps }))
}
