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
    out.push_str(&claim_failure_metrics(
        state
            .claim_failures
            .load(std::sync::atomic::Ordering::Relaxed),
    ));
    // The store's report on itself. Four blocks, each answering a question the
    // gauges above cannot: how the engine is behaving (`store_op_metrics`),
    // whether the queue is moving rather than merely deep (`queue_age_metrics`),
    // what the store costs on disk including the sidecar (`store_size_metrics`),
    // and whether maintenance is actually running (`maintenance_metrics`).
    let instrument = state.storage.instrument();
    out.push_str(&store_op_metrics(&instrument.snapshot()));
    out.push_str(&queue_age_metrics(&state.storage.queue_ages().await?));
    out.push_str(&store_size_metrics(&state.storage.size_facts().await?));
    out.push_str(&maintenance_metrics(&pass_counts(&instrument)));
    out.push_str(&activity_metrics(
        state.activity.reading(),
        instrument.pool_saturated(),
    ));

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

/// Failed job-claim attempts, process-lifetime.
///
/// The claim loop's failure arm is rate-limited before it reaches the remote
/// telemetry channel — the first failure of an outage and one report every five
/// minutes thereafter, instead of one event per two-second poll. This series is
/// what keeps that limiter from being a blindfold: the events are capped, the
/// count is not, and it lives in the LOCAL sink, because a channel that is
/// itself down cannot be where its own outage is counted.
///
/// Emitted at zero like every other counter here: an absent series and a zero
/// series are different answers.
fn claim_failure_metrics(total: u64) -> String {
    format!(
        "# HELP pumper_worker_claim_failures_total Job-claim attempts that failed (store          unreachable or erroring). Process-lifetime, reset on restart. The matching log lines are          rate-limited; this count is not
         # TYPE pumper_worker_claim_failures_total counter
         pumper_worker_claim_failures_total {total}
"
    )
}

// ---- The store's report on itself -----------------------------------------

/// Formats a microsecond figure as Prometheus seconds.
fn secs(micros: u64) -> String {
    format!("{:.6}", micros as f64 / 1_000_000.0)
}

/// Per-key store telemetry, as Prometheus text.
///
/// Answers the question the platform could not answer about itself before this
/// series existed: *which part of the store is slow, and is it slow because the
/// engine is working or because it is waiting?* `/metrics` carried fifteen
/// series about jobs, costs and deliveries and not one about the SQLite layer
/// underneath them, so "the app feels slow after a few weeks" had nothing to
/// interrogate.
///
/// **Keyed by `(op, table, phase)`, never by statement text** — statements embed
/// values, which is unbounded cardinality. The `table` label is the join with
/// the accounting report below: "this table is big AND its writes are
/// degrading" is one finding, and it needs both halves keyed alike.
///
/// `phase` is load-bearing, not decoration. `acquire` is the wait for a pooled
/// connection and `execute` is the statement; averaging them would point at
/// neither remedy, since one indicts pool sizing and the other an index.
///
/// **`pumper_store_slow_ops_total` carries its predicate**: the line it counts
/// against is published as `pumper_store_slow_line_seconds` on the identical
/// labels, so "N slow operations" cannot be quoted in a conversation where
/// everyone assumes a different N. The lines are LOCAL-store lines (2ms for a
/// pool handoff, 5ms for a point write) — a server-derived 100ms would sit
/// above every pathology this exists to catch.
///
/// The `_total` series are process-lifetime and reset on restart, like the
/// egress and checkpoint counters above. The p95 is NOT: it is derived from a
/// 256-record window per key, which is why `pumper_store_window_samples` and
/// `pumper_store_window_seconds` are emitted beside it. A p95 over 7 samples is
/// the 7th sample, and a rate over a window that turns out to be nine hours
/// long is a different claim from the same rate over ninety seconds.
///
/// Every series is emitted for every key even at zero: an absent series and a
/// zero series are different answers, and "0 busy waits on `records`" is the
/// honest one.
fn store_op_metrics(reports: &[pumper_core::KeyReport]) -> String {
    let mut out = String::new();
    // (metric, type, help, extractor) — one table so a new figure cannot ship
    // without a HELP line, and the label set cannot drift between series.
    #[allow(clippy::type_complexity)]
    let blocks: &[(&str, &str, &str, &dyn Fn(&pumper_core::KeyReport) -> String)] = &[
        (
            "pumper_store_ops_total",
            "counter",
            "Measured store operations by family, the table it touches, and phase \
             (acquire = waiting for a pooled connection, execute = running the statements). \
             Process-lifetime, reset on restart. A PARTIAL census: only the families listed \
             in the op label are instrumented — schedules, watches, triggers, deliveries and \
             the caches are not measured at all",
            &|r| r.lifetime.to_string(),
        ),
        (
            "pumper_store_slow_ops_total",
            "counter",
            "Operations at or past this key's slow line, which is published as \
             pumper_store_slow_line_seconds on the SAME labels — the count means nothing \
             without it. Process-lifetime; the matching log lines are rate-limited per key, \
             this count is not",
            &|r| r.slow_lifetime.to_string(),
        ),
        (
            "pumper_store_slow_line_seconds",
            "gauge",
            "The predicate behind pumper_store_slow_ops_total: the duration at which one \
             operation of this family counts as slow. Calibrated for a LOCAL store (an \
             embedded read is microseconds-to-low-milliseconds), not for a networked one",
            &|r| secs(r.slow_line_micros),
        ),
        (
            "pumper_store_busy_total",
            "counter",
            "Operations that ended in SQLITE_BUSY / SQLITE_LOCKED, or in a pool timeout on \
             the acquire phase. Classified by the driver's result code, never by message \
             text. Separates contention (indicts pool sizing or a writer-hog) from engine \
             work (indicts the query plan). Process-lifetime",
            &|r| r.busy_lifetime.to_string(),
        ),
        (
            "pumper_store_errors_total",
            "counter",
            "Operations that failed for any reason other than contention. \
             Process-lifetime",
            &|r| r.errors_lifetime.to_string(),
        ),
        (
            "pumper_store_rows_total",
            "counter",
            "Rows touched, summed over this key's operations. Separates 'the query got \
             slower' from 'the table got bigger' — the remedy for one is an index, for the \
             other a retention policy. Acquire-phase keys touch no rows and read 0. \
             Process-lifetime",
            &|r| r.rows_lifetime.to_string(),
        ),
        (
            "pumper_store_duration_p95_seconds",
            "gauge",
            "Nearest-rank p95 over this key's window: sort the last N recorded durations \
             ascending and take element ceil(0.95*n) — an OBSERVED sample, never an \
             interpolation. N is pumper_store_window_samples; read it beside this figure",
            &|r| secs(r.p95_micros),
        ),
        (
            "pumper_store_window_samples",
            "gauge",
            "Records currently in this key's window (at most 256). The n behind the p95: a \
             p95 over 7 samples is the 7th sample",
            &|r| r.samples.to_string(),
        ),
        (
            "pumper_store_window_seconds",
            "gauge",
            "Wall-clock span the current window covers, whole seconds. 0 means the whole \
             window happened inside one second, not that there is no window — read it with \
             pumper_store_window_samples. After a burst the window may cover ninety \
             seconds; after a quiet night, nine hours",
            &|r| r.window_secs.to_string(),
        ),
        (
            "pumper_store_op_worst_seconds",
            "gauge",
            "Worst single duration observed for this key since process start. A LIFETIME \
             fact kept outside the window on purpose: 'worst ever' must not evaporate when \
             the ring wraps",
            &|r| secs(r.worst_micros),
        ),
    ];
    for (metric, kind, help, value) in blocks {
        out.push_str(&format!("# HELP {metric} {help}\n# TYPE {metric} {kind}\n"));
        for r in reports {
            out.push_str(&format!(
                "{metric}{{op=\"{}\",table=\"{}\",phase=\"{}\"}} {}\n",
                r.op.as_str(),
                r.table,
                r.phase.as_str(),
                value(r)
            ));
        }
    }
    out
}

/// Age of the oldest job in each waiting state, as Prometheus text.
///
/// The companion `pumper_jobs{status}` already exists and is deliberately NOT
/// duplicated here: a depth says how much work is piled up, these say whether
/// the pile is moving. A steady depth with a growing oldest-queued age is a
/// worker that has stopped claiming; a growing oldest-running age is a job that
/// has stopped finishing. Neither is visible in the depth alone.
///
/// Both read 0 when the state is empty rather than keeping their last value —
/// a gauge that stays high once the queue clears is an alert that never
/// resolves, which is the failure `pumper_webhook_oldest_undelivered_seconds`
/// already guards against.
fn queue_age_metrics(ages: &pumper_core::QueueAges) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_jobs_oldest_queued_age_seconds Age of the oldest queued job \
         (created -> now; 0 when nothing is queued). The complement of pumper_jobs{status}: \
         depth says how much is waiting, this says whether it is moving\n\
         # TYPE pumper_jobs_oldest_queued_age_seconds gauge\n",
    );
    out.push_str(&format!(
        "pumper_jobs_oldest_queued_age_seconds {:.3}\n",
        ages.oldest_queued_secs
    ));
    out.push_str(
        "# HELP pumper_jobs_oldest_running_age_seconds Age of the oldest running job \
         (started -> now; 0 when nothing is running). A rising value with a flat job count \
         is a run that has stopped finishing\n\
         # TYPE pumper_jobs_oldest_running_age_seconds gauge\n",
    );
    out.push_str(&format!(
        "pumper_jobs_oldest_running_age_seconds {:.3}\n",
        ages.oldest_running_secs
    ));
    out
}

/// What the store costs on disk, as Prometheus text.
///
/// "The database is 2 GB" triggers panic; a number that says how much of that
/// is recycled-but-unreturned space, and how much is the un-checkpointed
/// sidecar, triggers a fix. The three parts are deliberately separate series
/// rather than one `size`, because they answer three different questions and
/// conflating them makes the report unable to answer its own follow-up ("will
/// anything shrink the file?").
///
/// `wal` is the part a size report usually forgets. Under WAL the database is a
/// file SET, the sidecar is a permanent resident, and its growth is bounded
/// only by checkpoints actually happening — which is why it is the harm figure
/// the maintenance gate escalates on.
///
/// Every series is emitted even at zero: on a store that has never been
/// written there genuinely is no sidecar, and "0" is the honest answer rather
/// than a missing series that reads as "not measured".
fn store_size_metrics(size: &pumper_core::StoreSize) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_store_bytes Database size on disk by part: main = page_count x \
         page_size (pages ALLOCATED, not bytes in live rows); free = freelist pages, i.e. \
         space already recycled inside the file and never returned to the filesystem; \
         wal = the -wal sidecar, which holds commits not yet folded into the main file\n\
         # TYPE pumper_store_bytes gauge\n",
    );
    for (part, n) in [
        ("main", size.main_bytes),
        ("free", size.free_bytes),
        ("wal", size.wal_bytes),
    ] {
        out.push_str(&format!("pumper_store_bytes{{part=\"{part}\"}} {n}\n"));
    }
    out.push_str(
        "# HELP pumper_store_pages Database pages by kind: total = page_count, \
         free = freelist_count. The ratio is what says whether reclamation would pay\n\
         # TYPE pumper_store_pages gauge\n",
    );
    for (kind, n) in [("total", size.page_count), ("free", size.freelist_pages)] {
        out.push_str(&format!("pumper_store_pages{{kind=\"{kind}\"}} {n}\n"));
    }
    out.push_str(
        "# HELP pumper_store_page_size_bytes SQLite page size, the multiplier behind \
         pumper_store_bytes\n\
         # TYPE pumper_store_page_size_bytes gauge\n",
    );
    out.push_str(&format!(
        "pumper_store_page_size_bytes {}\n",
        size.page_size
    ));
    out
}

/// Every `(task, outcome)` pair the instrument counts, for
/// [`maintenance_metrics`]. Built here so the renderer stays pure.
fn pass_counts(
    inst: &pumper_core::StoreInstrument,
) -> Vec<(pumper_core::MaintenanceTask, pumper_core::PassOutcome, u64)> {
    let mut out = Vec::new();
    for task in pumper_core::MaintenanceTask::ALL {
        for outcome in pumper_core::PassOutcome::ALL {
            out.push((*task, *outcome, inst.pass_count(*task, *outcome)));
        }
    }
    out
}

/// Maintenance passes by task and outcome, as Prometheus text.
///
/// The two questions this answers are the ones that otherwise become folklore:
/// *is maintenance actually running?* and *was that stall at 14:03 us?*
///
/// Three outcomes, never two. "Ran and found nothing to do", "deferred because
/// the application was busy" and "attempted and failed" are different results,
/// and a log that records only successes cannot tell a healthy store from a
/// scheduler that has been deferring for a month — the discovery then arrives
/// as a disk-full report.
///
/// Every pair is emitted at zero, which is the whole point here: a
/// `deferred` series pinned at 0 while `ran` climbs is a healthy gate, and a
/// `ran` series stuck at 0 while `deferred` climbs is the finding.
fn maintenance_metrics(
    counts: &[(pumper_core::MaintenanceTask, pumper_core::PassOutcome, u64)],
) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_store_maintenance_passes_total Maintenance passes by task and \
         outcome: ran = the pass executed (work=0 is still a run), deferred = the activity \
         gate said the application was busy and no escalation rung applied, failed = it was \
         attempted and errored. Process-lifetime, reset on restart\n\
         # TYPE pumper_store_maintenance_passes_total counter\n",
    );
    for (task, outcome, n) in counts {
        out.push_str(&format!(
            "pumper_store_maintenance_passes_total{{task=\"{}\",outcome=\"{}\"}} {n}\n",
            task.as_str(),
            outcome.as_str()
        ));
    }
    out
}

/// The activity gauge the maintenance gate reads, as Prometheus text.
///
/// Publishing the gate's own input is what makes the gate auditable rather than
/// a black box: an operator looking at a `deferred` count that never stops
/// climbing needs to see WHAT the gate was seeing. A gauge stuck above zero
/// with an idle process is a leaked guard, and until this series existed there
/// was no way to tell that from a genuinely busy server.
///
/// `pumper_store_pool_saturated` is the second half of the same picture: it is
/// demand for the machine that a count of requests and jobs cannot see, and it
/// alone can hold maintenance off.
fn activity_metrics(reading: u64, pool_saturated: bool) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP pumper_store_activity_gauge In-flight foreground work: HTTP requests being          handled plus jobs currently running. The input to the quiet-window maintenance gate          — a pass runs only when this reads 0 and the minimum interval has elapsed
         # TYPE pumper_store_activity_gauge gauge
",
    );
    out.push_str(&format!(
        "pumper_store_activity_gauge {reading}
"
    ));
    out.push_str(
        "# HELP pumper_store_pool_saturated 1 when the most recent connection acquisition on          any measured family waited past its slow line. Counts as busy for the maintenance          gate: a saturated pool is demand for the machine that the activity gauge cannot          see
         # TYPE pumper_store_pool_saturated gauge
",
    );
    out.push_str(&format!(
        "pumper_store_pool_saturated {}
",
        pool_saturated as u8
    ));
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

    /// The rate limiter in front of the claim loop's failure arm is only
    /// legitimate because the COUNT survives it: an unreachable store now ships
    /// one telemetry event per five minutes instead of one per poll, and this
    /// series is where the other 149 went.
    #[test]
    fn a_rate_limited_claim_outage_still_has_a_full_count() {
        let body = claim_failure_metrics(150);
        assert!(
            body.contains(
                "pumper_worker_claim_failures_total 150
"
            ),
            "{body}"
        );
        // Explicit zero on a healthy process, like every other counter here.
        let quiet = claim_failure_metrics(0);
        assert!(
            quiet.contains(
                "pumper_worker_claim_failures_total 0
"
            ),
            "{quiet}"
        );
        assert_eq!(quiet.matches("# HELP ").count(), 1);
        assert_eq!(quiet.matches("# TYPE ").count(), 1);
        assert!(quiet.contains("rate-limited"), "{quiet}");
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

    // ---- the store's report on itself ------------------------------------

    fn instrument_with_traffic() -> pumper_core::StoreInstrument {
        use pumper_core::{OpOutcome, StoreOp, StorePhase};
        let inst = pumper_core::StoreInstrument::new();
        // A healthy claim and a pathological one, so the p95 and the slow
        // count both have something to say.
        for _ in 0..9 {
            inst.record(
                StoreOp::JobClaim,
                StorePhase::Execute,
                std::time::Duration::from_micros(400),
                1,
                OpOutcome::Ok,
            );
        }
        inst.record(
            StoreOp::JobClaim,
            StorePhase::Execute,
            std::time::Duration::from_millis(90),
            1,
            OpOutcome::Ok,
        );
        // One lock wait on the write path — the fact a duration alone cannot
        // carry.
        inst.record(
            StoreOp::DatasetWrite,
            StorePhase::Execute,
            std::time::Duration::from_millis(120),
            500,
            OpOutcome::Busy,
        );
        inst
    }

    /// The anti-pattern this block exists to kill: `/metrics` exported jobs,
    /// costs, schedules and deliveries and NOT one number about the SQLite
    /// layer underneath them, so "the store feels slow" had nothing to
    /// interrogate. Every key must carry its table, its phase, and a slow count
    /// that travels with the line it was counted against.
    #[test]
    fn store_telemetry_names_its_table_its_phase_and_its_slow_line() {
        let body = store_op_metrics(&instrument_with_traffic().snapshot());
        for expect in [
            "pumper_store_ops_total{op=\"job_claim\",table=\"jobs\",phase=\"execute\"} 10",
            "pumper_store_slow_ops_total{op=\"job_claim\",table=\"jobs\",phase=\"execute\"} 1",
            // count-carries-predicate: the line is published on the SAME labels.
            "pumper_store_slow_line_seconds{op=\"job_claim\",table=\"jobs\",phase=\"execute\"} \
             0.005000",
            "pumper_store_rows_total{op=\"dataset_write\",table=\"records\",phase=\"execute\"} 500",
            "pumper_store_busy_total{op=\"dataset_write\",table=\"records\",phase=\"execute\"} 1",
            // Nearest-rank over 10 samples takes element ceil(0.95*10) = 10 —
            // an observed sample, which is the 90ms outlier.
            "pumper_store_duration_p95_seconds{op=\"job_claim\",table=\"jobs\",phase=\"execute\"} \
             0.090000",
            "pumper_store_window_samples{op=\"job_claim\",table=\"jobs\",phase=\"execute\"} 10",
        ] {
            let expect = expect.replace("             ", "");
            assert!(body.contains(&expect), "missing {expect:?} in:\n{body}");
        }
        // A pool wait must never be folded into query time: the acquire key is
        // present, distinct, and carries a DIFFERENT line.
        assert!(body
            .contains("pumper_store_slow_line_seconds{op=\"job_claim\",table=\"jobs\",phase=\"acquire\"} 0.002000"));
        // Untouched keys are emitted at zero — an absent series and a zero
        // series are different answers.
        assert!(body.contains(
            "pumper_store_ops_total{op=\"job_recovery\",table=\"jobs\",phase=\"acquire\"} 0"
        ));
        // Every series is declared exactly once, or the scrape warns.
        assert_eq!(body.matches("# HELP ").count(), 10);
        assert_eq!(body.matches("# TYPE ").count(), 10);
        // The census is partial and the HELP says so, or a dashboard reads
        // these as covering the whole store.
        assert!(body.contains("PARTIAL census"), "{body}");
    }

    /// The queue's depth already ships as `pumper_jobs{status}`; what it cannot
    /// say is whether the pile is MOVING. And an empty queue must resolve to
    /// zero rather than keeping its last age — a gauge that stays high once the
    /// queue clears is an alert that never resolves.
    #[test]
    fn queue_ages_complement_the_depth_and_resolve_to_zero_when_empty() {
        let body = queue_age_metrics(&pumper_core::QueueAges {
            oldest_queued_secs: 942.0,
            oldest_running_secs: 0.0,
        });
        assert!(
            body.contains("pumper_jobs_oldest_queued_age_seconds 942.000"),
            "{body}"
        );
        assert!(
            body.contains("pumper_jobs_oldest_running_age_seconds 0.000"),
            "an empty running set reads 0, not absent: {body}"
        );
        assert_eq!(body.matches("# HELP ").count(), 2);
        assert_eq!(body.matches("# TYPE ").count(), 2);
    }

    /// "The database is 2 GB" triggers panic; knowing how much of it is
    /// recycled-but-unreturned space and how much is an un-checkpointed sidecar
    /// triggers a fix. The sidecar is the part a size report forgets — under
    /// WAL the database is a file SET, and reading only the main file
    /// understates the store by exactly the commits not yet folded in.
    #[test]
    fn the_size_report_separates_allocated_free_and_sidecar_bytes() {
        let body = store_size_metrics(&pumper_core::StoreSize {
            page_size: 4096,
            page_count: 1000,
            freelist_pages: 250,
            main_bytes: 4_096_000,
            free_bytes: 1_024_000,
            wal_bytes: 67_108_864,
        });
        for expect in [
            "pumper_store_bytes{part=\"main\"} 4096000",
            "pumper_store_bytes{part=\"free\"} 1024000",
            "pumper_store_bytes{part=\"wal\"} 67108864",
            "pumper_store_pages{kind=\"total\"} 1000",
            "pumper_store_pages{kind=\"free\"} 250",
            "pumper_store_page_size_bytes 4096",
        ] {
            assert!(body.contains(expect), "missing {expect:?} in:\n{body}");
        }
        // A pristine store reads explicit zeros on all three parts.
        let fresh = store_size_metrics(&pumper_core::StoreSize::default());
        assert!(
            fresh.contains("pumper_store_bytes{part=\"wal\"} 0"),
            "{fresh}"
        );
        assert_eq!(fresh.matches("# HELP ").count(), 3);
        assert_eq!(fresh.matches("# TYPE ").count(), 3);
    }

    /// Deferral is an outcome. A maintenance series that counted only successes
    /// could not tell a healthy store from a scheduler that has been deferring
    /// for a month, and the discovery would arrive as a disk-full report.
    #[test]
    fn maintenance_reports_deferred_as_loudly_as_it_reports_ran() {
        let inst = pumper_core::StoreInstrument::new();
        for _ in 0..4 {
            inst.record_pass(pumper_core::MaintenancePass {
                task: pumper_core::MaintenanceTask::WalCheckpoint,
                trigger: None,
                gauge: 3,
                duration_ms: 0,
                work: 0,
                outcome: pumper_core::PassOutcome::Deferred,
                detail: "busy".into(),
                at: chrono::Utc::now(),
            });
        }
        let body = maintenance_metrics(&pass_counts(&inst));
        assert!(body.contains(
            "pumper_store_maintenance_passes_total{task=\"wal_checkpoint\",outcome=\"deferred\"} 4"
        ), "{body}");
        // The other two outcomes for the same task read explicit zeros — which
        // is what makes "it has deferred four times and never run" legible.
        assert!(
            body.contains(
                "pumper_store_maintenance_passes_total{task=\"wal_checkpoint\",outcome=\"ran\"} 0"
            ),
            "{body}"
        );
        assert!(body.contains(
            "pumper_store_maintenance_passes_total{task=\"wal_checkpoint\",outcome=\"failed\"} 0"
        ), "{body}");
        // Every task x outcome pair is present, so no series can be missing on
        // the day it first matters.
        assert_eq!(
            body.matches("pumper_store_maintenance_passes_total{")
                .count(),
            pumper_core::MaintenanceTask::ALL.len() * pumper_core::PassOutcome::ALL.len()
        );
        assert_eq!(body.matches("# HELP ").count(), 1);
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
