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
        let n = counts
            .iter()
            .find(|(s, _)| s == status)
            .map_or(0, |(_, n)| *n);
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
    out.push_str(&format!(
        "pumper_job_duration_seconds_max {}\n",
        timing.duration_max
    ));
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
    out.push_str(&format!(
        "pumper_job_queue_wait_seconds_max {}\n",
        timing.wait_max
    ));
    out.push_str(
        "# HELP pumper_cost_usd Total engine spend by app\n# TYPE pumper_cost_usd gauge\n",
    );
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
    out.push_str(&webhook_metrics(
        &state.storage.delivery_health().await?,
        chrono::Utc::now(),
    ));
    // The remote fabric's egress split. This is the ONE `state.engines.fetch`
    // access in this file and it is a counter read, not a fetch — no job, no
    // budget, no cassette — so it carries a reviewed row in
    // `fetch_chokepoint.rs`'s EXPECTED_RAW_ENGINE_CALLS rather than an exemption.
    out.push_str(&egress_metrics(state.engines.fetch.egress_counters()));
    out.push_str(&checkpoint_metrics(state.checkpoint_failures.totals()));

    *state.metrics_cache.lock().await = Some((std::time::Instant::now(), out.clone()));
    Ok(metrics_response(out))
}

/// The remote fetch fabric's egress split, as Prometheus text.
///
/// Answers the question the fabric could not answer about itself: *did the
/// geo-distributed egress actually happen, and how much of it didn't?*
/// `local_fallback` is the one that matters operationally — every fetch counted
/// there left from the coordinator's own IP, the address the operator deployed
/// the fabric to stop using. A misconfigured secret makes every peer answer 401
/// and every fetch fall back silently, which before this series produced exactly
/// one `warn!` that read the same whether one fetch or a million had leaked.
///
/// Counters are process-lifetime and reset on restart, like the rest of this
/// endpoint's counters. Both series are emitted even when the fabric is off (a
/// pure pass-through records neither, so both read 0) — an absent series and a
/// zero series are different answers, and "0 peer-served" is the honest one.
fn egress_metrics(counters: &pumper_core::fetcher::EgressCounters) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_remote_egress_fetches Live-HTTP-tier fetches by who egressed them: \
         peer = served by a remote fabric node, local_fallback = egressed from this \
         coordinator despite the fabric being configured\n\
         # TYPE pumper_remote_egress_fetches counter\n",
    );
    for (served_by, n) in [
        ("peer", counters.peer_served()),
        ("local_fallback", counters.local_fallback()),
    ] {
        out.push_str(&format!(
            "pumper_remote_egress_fetches{{served_by=\"{served_by}\"}} {n}\n"
        ));
    }
    out
}

/// Checkpoint saves that did not land, by reason, as Prometheus text.
///
/// Answers the durability question the platform could not answer about itself:
/// *is anything resuming from nothing?* A `storage_error` means a run has no
/// durable state at all — the next reap or restart resumes it from whatever
/// older snapshot survives; a `stale_lineage` means another attempt owns the job
/// and this task's save was correctly discarded (routine during a reap, alarming
/// in a steady state).
///
/// **Process-lifetime counters, reset on restart** — like `egress_metrics`
/// above, and deliberately NOT a `jobs.result` scan: the per-run stamp is
/// carried onto the stored result on the worker's success arm only, so a scan
/// would undercount exactly the cancelled/failed/panicked/timed-out/suspended
/// runs an operator most wants counted, and retention pruning would make the
/// total shrink over time.
///
/// Both series are emitted even at zero: an absent series and a zero series are
/// different answers, and "0 checkpoint failures" is the honest one.
fn checkpoint_metrics(failures: crate::progress::CheckpointFailures) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_checkpoint_failures_total Checkpoint saves that did not land, by reason: \
         storage_error = the write failed, so the run has no durable state of its own; \
         stale_lineage = another attempt owns the job and this save was discarded. \
         Process-lifetime, reset on restart\n\
         # TYPE pumper_checkpoint_failures_total counter\n",
    );
    for (reason, n) in [
        (
            crate::progress::CheckpointFailure::StaleLineage.as_str(),
            failures.stale_lineage,
        ),
        (
            crate::progress::CheckpointFailure::StorageError.as_str(),
            failures.storage_error,
        ),
    ] {
        out.push_str(&format!(
            "pumper_checkpoint_failures_total{{reason=\"{reason}\"}} {n}\n"
        ));
    }
    out
}

/// Renders the webhook-delivery block of `/metrics` from one health snapshot.
///
/// Split out from [`metrics`] and pure (`now` is passed in) because this is the
/// answer to "has everything been failing for six hours?", and until it existed
/// the only way to ask was to hand-poll `GET /webhooks/deliveries` — with the
/// docs naming the wrong status.
///
/// Gauges, not process counters: every number is DB-derived, so the retention
/// janitor can lower `_total` series. The existing `pumper_job_failures_total`
/// sets that precedent and states it in its HELP; these do the same rather than
/// pretending to be monotonic.
fn webhook_metrics(
    health: &pumper_core::DeliveryHealth,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_webhook_deliveries Webhook deliveries by status: pending = in flight, \
         failed = still on the retry ladder, dead = dead-letter queue, delivered = accepted\n\
         # TYPE pumper_webhook_deliveries gauge\n",
    );
    for (status, n) in [
        ("pending", health.pending),
        ("delivered", health.delivered),
        ("failed", health.failed),
        ("dead", health.dead),
    ] {
        out.push_str(&format!(
            "pumper_webhook_deliveries{{status=\"{status}\"}} {n}\n"
        ));
    }
    out.push_str(
        "# HELP pumper_webhook_oldest_undelivered_seconds Age of the oldest delivery the \
         receiver has not accepted (pending + failed; 0 when none). Excludes dead, which is \
         terminal and would pin this gauge forever\n\
         # TYPE pumper_webhook_oldest_undelivered_seconds gauge\n",
    );
    out.push_str(&format!(
        "pumper_webhook_oldest_undelivered_seconds {}\n",
        health.oldest_undelivered_secs(now)
    ));
    out.push_str(
        "# HELP pumper_webhook_delivery_attempts_total Send attempts across the delivery log, \
         retries included (DB-derived, so retention pruning can lower it)\n\
         # TYPE pumper_webhook_delivery_attempts_total counter\n",
    );
    out.push_str(&format!(
        "pumper_webhook_delivery_attempts_total {}\n",
        health.attempts
    ));
    out.push_str(
        "# HELP pumper_webhook_deliveries_succeeded_total Deliveries the receiver accepted — \
         one per delivered row, its final attempt being the successful one (DB-derived)\n\
         # TYPE pumper_webhook_deliveries_succeeded_total counter\n",
    );
    out.push_str(&format!(
        "pumper_webhook_deliveries_succeeded_total {}\n",
        health.delivered
    ));
    out
}

#[cfg(test)]
mod webhook_metric_tests {
    use super::*;
    use pumper_core::DeliveryHealth;

    fn at(secs: i64) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    /// The anti-pattern: `/metrics` exported jobs, costs and schedules and NOT
    /// one webhook series, so "every delivery has been failing for six hours"
    /// was invisible to a dashboard. Every gauge an operator would alert on has
    /// to be in the body.
    #[test]
    fn metrics_carry_every_delivery_status_not_just_a_total() {
        let health = DeliveryHealth {
            pending: 1,
            delivered: 7,
            failed: 2,
            dead: 3,
            attempts: 19,
            oldest_undelivered: Some(at(1_000)),
        };
        let body = webhook_metrics(&health, at(21_600 + 1_000));
        for expect in [
            "pumper_webhook_deliveries{status=\"pending\"} 1",
            "pumper_webhook_deliveries{status=\"delivered\"} 7",
            "pumper_webhook_deliveries{status=\"failed\"} 2",
            "pumper_webhook_deliveries{status=\"dead\"} 3",
            "pumper_webhook_oldest_undelivered_seconds 21600",
            "pumper_webhook_delivery_attempts_total 19",
            "pumper_webhook_deliveries_succeeded_total 7",
        ] {
            assert!(body.contains(expect), "missing {expect:?} in:\n{body}");
        }
        // Every series is declared: an undeclared metric is a scrape warning.
        assert_eq!(body.matches("# HELP ").count(), 4);
        assert_eq!(body.matches("# TYPE ").count(), 4);
    }

    /// The anti-pattern this series exists to kill: the remote fetch fabric's
    /// whole claim is "traffic leaves from somewhere else", and a misconfigured
    /// secret makes every peer answer 401 and every fetch fall silently back to
    /// the coordinator's own IP — producing one `warn!` that reads identically
    /// whether one fetch or a million leaked. A fallback that is invisible to a
    /// dashboard is a fabric that can be entirely off without anyone noticing.
    #[test]
    fn a_silent_fallback_to_local_egress_is_not_invisible_to_a_dashboard() {
        let counters = pumper_core::fetcher::EgressCounters::default();
        let body = egress_metrics(&counters);
        // Off/unused reads as an explicit zero on BOTH series: an absent series
        // and a zero series are different answers to "did any egress go remote".
        assert!(
            body.contains("pumper_remote_egress_fetches{served_by=\"peer\"} 0\n"),
            "{body}"
        );
        assert!(
            body.contains("pumper_remote_egress_fetches{served_by=\"local_fallback\"} 0\n"),
            "{body}"
        );
        // Declared exactly once, or the scrape warns.
        assert_eq!(body.matches("# HELP ").count(), 1);
        assert_eq!(body.matches("# TYPE ").count(), 1);
    }

    /// The anti-pattern: a checkpoint that stopped landing was counted in an
    /// `AtomicU64` the worker read in ONE of its seven outcome arms, and
    /// `/metrics` carried fourteen series and not one of them. "Is anything
    /// resuming from nothing?" had no answer a dashboard could ask.
    #[test]
    fn a_dropped_checkpoint_is_not_invisible_to_a_dashboard() {
        use crate::progress::CheckpointFailures;
        let body = checkpoint_metrics(CheckpointFailures {
            stale_lineage: 2,
            storage_error: 5,
        });
        assert!(
            body.contains("pumper_checkpoint_failures_total{reason=\"stale_lineage\"} 2\n"),
            "{body}"
        );
        assert!(
            body.contains("pumper_checkpoint_failures_total{reason=\"storage_error\"} 5\n"),
            "{body}"
        );
        // A healthy process reads as explicit zeros on BOTH reasons: an absent
        // series and a zero series are different answers.
        let quiet = checkpoint_metrics(CheckpointFailures::default());
        assert!(
            quiet.contains("pumper_checkpoint_failures_total{reason=\"stale_lineage\"} 0\n"),
            "{quiet}"
        );
        assert!(
            quiet.contains("pumper_checkpoint_failures_total{reason=\"storage_error\"} 0\n"),
            "{quiet}"
        );
        // Declared exactly once, or the scrape warns. The reset-on-restart
        // contract is stated in the HELP, like the egress counters above.
        assert_eq!(quiet.matches("# HELP ").count(), 1);
        assert_eq!(quiet.matches("# TYPE ").count(), 1);
        assert!(quiet.contains("reset on restart"), "{quiet}");
    }

    /// An empty backlog must read 0, never "unknown" or a stale age — a gauge
    /// that keeps its last value once the queue clears is an alert that never
    /// resolves.
    #[test]
    fn an_empty_backlog_reports_zero_age_not_a_stale_one() {
        let body = webhook_metrics(&DeliveryHealth::default(), at(9_999));
        assert!(body.contains("pumper_webhook_oldest_undelivered_seconds 0\n"));
        assert!(body.contains("pumper_webhook_deliveries{status=\"dead\"} 0\n"));
    }

    /// Clock skew (a row stamped in the future) must not render a negative age,
    /// which Prometheus would happily graph as a spike downward.
    #[test]
    fn future_stamped_row_reads_zero_not_negative() {
        let health = DeliveryHealth {
            oldest_undelivered: Some(at(5_000)),
            ..Default::default()
        };
        assert_eq!(health.oldest_undelivered_secs(at(4_000)), 0);
    }
}

// ---- Apps -----------------------------------------------------------------

#[derive(serde::Deserialize, utoipa::IntoParams)]
pub(crate) struct AppsQuery {
    /// Output shape: absent = the classic `{apps: [..]}` listing; `tools` =
    /// `{tools: [..]}` MCP tool-definition JSON (name/description/inputSchema
    /// per app, plus cost_class/examples/output_shape metadata).
    format: Option<String>,
}

#[utoipa::path(
    get,
    path = "/apps",
    tag = "apps",
    params(AppsQuery),
    responses(
        (status = 200, description = "`{apps: [{name, description, schedule, requires, ready, default_params, cost_class, output_shape, has_params_schema}]}` — `requires` lists preconditions (e.g. `env:CENSUS_API_KEY`); `ready` is false when any is unmet here; `default_params` is the app's default job params (a POST body's `params` shallow-merges over these); `cost_class` is free|metered|claude. When `[plugins] app_dir` is set, discovered dynamic WASM apps are appended with `dynamic: true, runnable: false` and a `reason` string — read-only manifests, not enqueueable. With `?format=tools`: `{tools: [..]}` — each app as an MCP tool definition (`inputSchema` = the app's params JSON Schema, permissive `{type: object}` when undeclared), directly consumable as agent tool definitions; dynamic apps are excluded there because a tool an agent cannot call is a trap."),
        (status = 400, description = "Unknown `format`", body = Object),
    )
)]
pub(crate) async fn list_apps(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<AppsQuery>,
) -> Result<Json<Value>, ApiError> {
    let mut apps: Vec<_> = state.registry.values().collect();
    apps.sort_by_key(|app| app.name());
    match query.format.as_deref() {
        Some("tools") => {
            let tools: Vec<Value> = apps
                .into_iter()
                .map(|app| crate::registry::tool_definition(app.as_ref()))
                .collect();
            Ok(Json(json!({ "tools": tools })))
        }
        Some(other) => Err(ApiError(
            axum::http::StatusCode::BAD_REQUEST,
            format!("unknown format '{other}' (omit for the app listing, or 'tools')"),
        )),
        None => {
            let apps: Vec<_> = apps
                .into_iter()
                .map(|app| {
                    let requires: Vec<String> = app.requires().iter().map(|r| r.label()).collect();
                    // `ready` = every declared precondition is satisfied here (e.g. the
                    // required API-key env var is set), so a credential-gated app is
                    // distinguishable from a runnable one before its first failed job.
                    let ready = app.requires().iter().all(|r| r.is_satisfied());
                    let manifest = app.manifest();
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
                        // Manifest highlights; the full tool definition (schema +
                        // examples) lives under `?format=tools`.
                        "cost_class": manifest.cost_class.as_str(),
                        "output_shape": manifest.output_shape,
                        "has_params_schema": manifest.params_schema.is_some(),
                    })
                })
                .collect();
            // Dynamic WASM apps (M28 v1): appended after the compiled-in apps,
            // carrying `dynamic: true, runnable: false` + a reason — visible so
            // an operator can see what `[plugins] app_dir` picked up, but never
            // enqueueable (the enqueue handler rejects them with the same reason).
            let mut apps = apps;
            apps.extend(state.dynamic_apps.iter().cloned());
            Ok(Json(json!({ "apps": apps })))
        }
    }
}
