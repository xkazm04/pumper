//! Result delivery via webhooks. When a job reaches a terminal state and set a
//! `callback_url`, the worker fires the job JSON at that URL so consuming apps
//! don't have to poll; dataset watches receive `dataset.changed` events the
//! same way. If a secret was supplied, the body is signed with HMAC-SHA256 and
//! sent as `X-Pumper-Signature: sha256=<hex>` so the receiver can verify
//! authenticity. Every delivery is logged to `webhook_deliveries` — `dead` rows
//! are the dead-letter queue, replayable via the API.
//!
//! ## Lifecycle
//!
//! Every send runs on a [`crate::fanout::FanoutPool`] instance dedicated to
//! deliveries ([`DELIVERY_CONCURRENCY`]). It used to be a bare `tokio::spawn`
//! per delivery, which put outbound POSTs *outside* the process's drainable
//! lifecycle: a graceful shutdown exited with requests mid-flight, and a test
//! had no synchronization point to wait on — only a deadline poll. On the pool
//! they are bounded, drained by the worker's shutdown drain, and panic-contained.
//!
//! ## Sinks (M22)
//!
//! A watch's `sink` column selects the delivery connector; the body is shaped
//! at dispatch time and the *transport* branches inside [`deliver`] so every
//! sink rides the exact same machinery — delivery log, in-process retries,
//! backed-off DLQ drain, and manual replay:
//!
//! - `webhook` (default): POST the payload at the watch URL, HMAC-signed.
//! - `slack`: POST a compact incoming-webhook message (`{"text": ...}`
//!   summarizing the delta + count) at the watch URL. Same HTTP transport, so
//!   retries/DLQ are identical; Slack ignores the extra `x-pumper-*` headers.
//! - `file`: append the payload as one NDJSON line to
//!   `data/sinks/<watch_id>.ndjson`. The delivery row's `url` is the
//!   `file://<watch_id>.ndjson` pseudo-URL, which the transport re-validates
//!   (filename chars only) so nothing in the log can escape the sinks dir.
//!
//! WASM sinks are deliberately OUT of v1. The seam for them is the transport
//! branch in [`deliver`]: a future `plugin:<name>` sink value would resolve
//! through the plugin host with the same `(delivery_id, event, body)` contract
//! and report `(delivered, attempts, last_error, permanent)` like the built-ins.

use std::path::{Path, PathBuf};
use std::time::Duration;

use hmac::{Hmac, Mac};
use pumper_core::config::WebhooksConfig;
use pumper_core::{Delivery, Job, Storage, Watch};
use sha2::Sha256;
use tracing::{debug, warn};

use crate::state::AppState;

type HmacSha256 = Hmac<Sha256>;

const MAX_ATTEMPTS: u64 = 3;

/// Upper bound on how long a receiver's `Retry-After` hint may delay the NEXT
/// in-process attempt. A large hint ("come back in 300s") belongs to the DLQ
/// ladder, not a parked delivery-pool slot — the ladder's first rung (30s)
/// already covers it — so in-process honoring is capped here and the ladder
/// takes anything longer.
const RETRY_AFTER_INPROC_CAP: Duration = Duration::from_secs(5);

/// How the retry ladder should treat one attempt's HTTP outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryClass {
    /// 2xx — delivered.
    Success,
    /// Worth retrying: 5xx, 429 (rate-limited — honor `Retry-After`), and any
    /// unexpected non-4xx status. The receiver may accept the same body later.
    Transient,
    /// A 4xx the receiver will keep rejecting the same body for (400, 404, 405,
    /// 410, 422…). Retrying only burns attempts and delays the `dead` state; the
    /// sender should stop now.
    Permanent,
}

/// Classify a response status for the retry ladder. The split is by class, not a
/// hand-maintained list: 2xx succeeds; 429 is transient (rate-limited, carries a
/// `Retry-After` the loop honors); every OTHER 4xx is permanent; everything else
/// (5xx, and the odd 3xx that escaped redirect-following) is transient.
fn classify_status(status: u16) -> DeliveryClass {
    match status {
        200..=299 => DeliveryClass::Success,
        429 => DeliveryClass::Transient,
        400..=499 => DeliveryClass::Permanent,
        _ => DeliveryClass::Transient,
    }
}

/// Parses a `Retry-After` header value into a delay. Handles the delta-seconds
/// form (`Retry-After: 120`); the HTTP-date form is intentionally not honored in
/// this in-process loop (it returns `None`, so the caller falls back to its
/// linear backoff and the DLQ ladder still applies). Negative/garbage → `None`.
fn parse_retry_after(value: &str) -> Option<Duration> {
    let secs: i64 = value.trim().parse().ok()?;
    (secs >= 0).then(|| Duration::from_secs(secs as u64))
}

/// Auto-drain backoff schedule (seconds) indexed by a delivery's `retry_count`:
/// 30s → 1m → 5m → 30m → 2h. Past the last entry the row is marked `dead`.
const DRAIN_BACKOFF_SECS: &[i64] = &[30, 60, 300, 1800, 7200];
/// Max background retries before a delivery is declared `dead` (= backoff len).
const DRAIN_MAX_RETRIES: i64 = 5;
/// Deliveries re-sent per drain tick — a small batch so one tick can't stampede
/// a just-recovered receiver.
const DRAIN_BATCH: i64 = 20;

/// Concurrency of the delivery pool (`AppState::deliveries`).
///
/// A **dedicated** [`crate::fanout::FanoutPool`] instance, not the worker's, for
/// three reasons:
///
/// 1. **Sizing.** The worker pool is `[worker] fanout_concurrency` (default 4) —
///    one slot per finished job's whole derived-work unit. One job can dispatch
///    many deliveries (every watch × every changed dataset), each an HTTP round
///    trip at the client's 15s timeout. Sharing would let one watch burst park
///    every worker fan-out slot behind a slow receiver.
/// 2. **Backpressure meaning.** At the ceiling a `FanoutPool` runs the unit
///    *inline on its caller*. For a job's fan-out that costs a scrape permit —
///    a designed trade. For a delivery the caller is either a fan-out unit
///    (which would then serialize the rest of the job's work behind one
///    receiver) or the scheduler's drain tick (which must stay fire-and-forget).
///    Separate ceilings keep one from triggering the other's inline path.
/// 3. **Escape hatch.** `fanout_concurrency = 0` is the documented "everything
///    inline" control arm; it must not silently turn every webhook into a
///    45s-worst-case blocking call inside the job's fan-out.
///
/// 16 outbound POSTs in flight is well inside one `reqwest` client's pool and
/// keeps a single slow receiver from serializing the rest. Not a config key:
/// per-deployment delivery throughput is a separate, deferred concern.
pub(crate) const DELIVERY_CONCURRENCY: usize = 16;

/// Backlog ceiling of the delivery pool. Above it a dispatch runs inline on its
/// caller — slow, never dropped (same contract as the worker pool). Deliberately
/// far above the worker pool's 64: `DRAIN_BATCH` (20) plus a fan-out burst must
/// never reach it in normal operation, because for the scheduler's drain tick
/// "inline" means the tick waits on a delivery.
pub(crate) const DELIVERY_MAX_QUEUED: usize = 1024;

/// Queues a best-effort, logged delivery of a terminal job to its callback.
pub async fn dispatch(state: &AppState, job: Job) {
    let Some(url) = job.callback_url.clone() else {
        return;
    };
    let secret = job.callback_secret.clone();
    let id = job.id.to_string();
    dispatch_event(state, "job", &id, &url, "job.terminal", &job, secret).await;
}

/// Queues a best-effort, logged delivery of a `dataset.changed` event through
/// the watch's configured sink. Body shaping happens here (once, so the logged
/// body is exactly what the DLQ drain re-sends); transport branching happens
/// in [`deliver`].
pub async fn dispatch_change(state: &AppState, watch: Watch, payload: serde_json::Value) {
    match watch.sink.as_str() {
        "file" => {
            // The pseudo-URL names the file from the watch id ONLY — never
            // user input — and the transport re-validates it before writing.
            let url = file_sink_url(&watch.id);
            dispatch_event(
                state,
                "change",
                &watch.id,
                &url,
                "dataset.changed",
                &payload,
                None,
            )
            .await;
        }
        "slack" => {
            let body = slack_summary(&payload);
            dispatch_event(
                state,
                "change",
                &watch.id,
                &watch.url,
                "dataset.changed",
                &body,
                watch.secret.clone(),
            )
            .await;
        }
        // "webhook" and anything unrecognized (fail toward the original,
        // most-informative behavior rather than dropping the event).
        _ => {
            dispatch_event(
                state,
                "change",
                &watch.id,
                &watch.url,
                "dataset.changed",
                &payload,
                watch.secret.clone(),
            )
            .await;
        }
    }
}

// ---- Sink helpers ---------------------------------------------------------

const FILE_SINK_SCHEME: &str = "file://";

/// `data/sinks/` — a sibling of the artifacts dir so all on-disk output lives
/// under the same data root (`data/artifacts` → `data/sinks`).
fn sinks_dir(storage: &Storage) -> PathBuf {
    storage
        .artifacts_dir
        .parent()
        .unwrap_or_else(|| Path::new("data"))
        .join("sinks")
}

/// The pseudo-URL logged for a file-sink delivery: `file://<watch_id>.ndjson`.
fn file_sink_url(watch_id: &str) -> String {
    format!("{FILE_SINK_SCHEME}{watch_id}.ndjson")
}

/// Resolves a logged file-sink URL to a filename inside `dir`. The name must
/// be a bare filename of `[A-Za-z0-9._-]` with no `..` — anything else (path
/// separators, traversal, a tampered log row) is rejected, so a delivery row
/// can never write outside the sinks dir.
fn file_sink_path(dir: &Path, url: &str) -> Option<PathBuf> {
    let name = url.strip_prefix(FILE_SINK_SCHEME)?;
    let valid = !name.is_empty()
        && !name.contains("..")
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    valid.then(|| dir.join(name))
}

/// Compact Slack incoming-webhook message summarizing a dataset delta. Built
/// once at dispatch so the logged body (and any DLQ replay) is exactly what
/// Slack accepts.
fn slack_summary(payload: &serde_json::Value) -> serde_json::Value {
    let app = payload["app"].as_str().unwrap_or("?");
    let dataset = payload["dataset"].as_str().unwrap_or("?");
    let count = payload["count"].as_u64().unwrap_or(0);
    let job = payload["job_id"].as_str().unwrap_or("-");
    let noun = if count == 1 { "revision" } else { "revisions" };
    serde_json::json!({
        "text": format!("pumper: `{app}/{dataset}` changed — {count} {noun} (job {job})"),
    })
}

/// Queues a best-effort, logged delivery of an arbitrary event — the generic
/// entry point for new event kinds (e.g. saved-search matches).
//
// The seven arguments are the webhook wire contract itself (state, kind, ref_id,
// url, event, payload, secret); every one is independently supplied by the
// caller, so collapsing them behind a default-able struct would let a caller
// silently omit `secret` (unsigned delivery) or `ref_id` (unattributable log
// line). The transport + storage handles used to be two more parameters here;
// they now come from `state`, which is what brought this back under clippy's
// threshold and let the old `#[allow(clippy::too_many_arguments)]` go.
pub async fn dispatch_event(
    state: &AppState,
    kind: &str,
    ref_id: &str,
    url: &str,
    event: &str,
    payload: &impl serde::Serialize,
    secret: Option<String>,
) {
    let body = match serde_json::to_vec(payload) {
        Ok(body) => body,
        Err(e) => {
            warn!(kind = %kind, ref_id = %ref_id, "webhook serialize failed: {e}");
            return;
        }
    };
    queue_logged(
        state,
        kind.to_string(),
        ref_id.to_string(),
        url.to_string(),
        event.to_string(),
        body,
        secret,
    )
    .await;
}

/// Queues a best-effort, logged `job.failed` delivery to the global failure
/// subscriber (`[webhooks] failure_url`). Fires on PERMANENT failure only — a
/// job's own `callback_url` already receives the terminal job JSON, so this is
/// the cross-app firehose path, not a per-job duplicate.
pub async fn dispatch_failure(state: &AppState, url: &str, secret: Option<String>, job: &Job) {
    let payload = serde_json::json!({
        "event": "job.failed",
        "job_id": job.id,
        "app": job.app,
        "error": job.error,
        "attempts": job.attempts,
        "schedule_id": job.schedule_id,
    });
    dispatch_event(
        state,
        "failure",
        &job.id.to_string(),
        url,
        "job.failed",
        &payload,
        secret,
    )
    .await;
}

/// Re-sends a logged delivery (the dead-letter replay path). The caller has
/// already resolved the signing secret from the delivery's source, and has
/// already **claimed** the row.
///
/// Returns as soon as the send is queued on the delivery pool, never when the
/// send completes: this is called both from the replay route (which answers 202)
/// and from [`drain_due`] on the scheduler tick, and the tick must not block on
/// a receiver.
pub async fn replay(
    state: &AppState,
    delivery_id: String,
    url: String,
    event: String,
    body: Vec<u8>,
    secret: Option<String>,
) {
    let storage = state.storage.clone();
    let client = state.webhook_client.clone();
    let tag = delivery_id.clone();
    state
        .deliveries
        .run_tagged("webhook-replay", tag, async move {
            let outcome = deliver(
                &storage,
                &client,
                &url,
                &event,
                &delivery_id,
                &body,
                secret.as_deref(),
            )
            .await;
            log_outcome(&storage, &delivery_id, &url, outcome).await;
        })
        .await;
}

/// Renders the outcome of a delivery that has **no log row** to record it
/// against (see [`queue_logged`]'s `create_delivery` failure branch).
///
/// The anti-pattern this replaces: `let _ = deliver(...)`. Storage was down, so
/// the send went out and its result was thrown away — the one delivery in the
/// system whose fate nothing anywhere could report, not the DLQ, not the log,
/// not `/metrics`. A `warn!` line carrying attempts and the last error is the
/// honest ceiling when the durable store is the thing that failed.
fn unlogged_outcome_summary(outcome: &(bool, i64, Option<String>, bool)) -> String {
    let (delivered, attempts, last_error, _permanent) = outcome;
    if *delivered {
        format!("delivered after {attempts} attempt(s)")
    } else {
        format!(
            "NOT delivered after {attempts} attempt(s), and it is not in the DLQ (the log write \
             is what failed): {}",
            last_error.as_deref().unwrap_or("no error recorded")
        )
    }
}

/// Creates the log row, runs the delivery loop, records the outcome — as one
/// tracked unit on the delivery pool.
async fn queue_logged(
    state: &AppState,
    kind: String,
    ref_id: String,
    url: String,
    event: String,
    body: Vec<u8>,
    secret: Option<String>,
) {
    let storage = state.storage.clone();
    let client = state.webhook_client.clone();
    let tag = format!("{kind}:{ref_id}");
    state
        .deliveries
        .run_tagged("webhook", tag, async move {
            let delivery_id = match storage
                .create_delivery(
                    &kind,
                    &ref_id,
                    &url,
                    &event,
                    &String::from_utf8_lossy(&body),
                )
                .await
            {
                Ok(id) => id,
                Err(e) => {
                    warn!(url = %url, "delivery log write failed (sending anyway): {e}");
                    // No persisted id — send with a generated one so the receiver still
                    // gets an idempotency key (this delivery just isn't in the log/DLQ).
                    let fallback_id = uuid::Uuid::new_v4().to_string();
                    let outcome = deliver(
                        &storage,
                        &client,
                        &url,
                        &event,
                        &fallback_id,
                        &body,
                        secret.as_deref(),
                    )
                    .await;
                    warn!(
                        url = %url, delivery = %fallback_id, kind = %kind, ref_id = %ref_id,
                        "unlogged webhook delivery finished: {}",
                        unlogged_outcome_summary(&outcome)
                    );
                    return;
                }
            };
            let outcome = deliver(
                &storage,
                &client,
                &url,
                &event,
                &delivery_id,
                &body,
                secret.as_deref(),
            )
            .await;
            log_outcome(&storage, &delivery_id, &url, outcome).await;
        })
        .await;
}

async fn log_outcome(
    storage: &Storage,
    delivery_id: &str,
    url: &str,
    outcome: (bool, i64, Option<String>, bool),
) {
    let (delivered, attempts, last_error, permanent) = outcome;
    let result = if delivered {
        debug!(delivery = %delivery_id, url = %url, "webhook delivered");
        storage
            .finish_delivery(delivery_id, true, attempts, last_error.as_deref())
            .await
    } else if permanent {
        // A permanent 4xx: the receiver will keep rejecting this body, so the
        // DLQ ladder would only spend 30s→…→2h to reach the same `dead` state.
        // Mark it `dead` now — a delivery the operator can see and replay, but
        // that stops pretending a resend will change the answer.
        debug!(delivery = %delivery_id, url = %url, "webhook delivery permanently rejected; marking dead");
        storage
            .kill_delivery(delivery_id, attempts, last_error.as_deref())
            .await
    } else {
        // Don't give up: schedule a backed-off auto-drain retry (or mark the row
        // `dead` past the cap). A receiver outage longer than the ~6s in-process
        // loop is exactly what this recovers, instead of silently losing events.
        debug!(delivery = %delivery_id, url = %url, "webhook delivery failed; scheduling drain retry");
        storage
            .fail_delivery(
                delivery_id,
                attempts,
                last_error.as_deref(),
                DRAIN_MAX_RETRIES,
                DRAIN_BACKOFF_SECS,
            )
            .await
    };
    if let Err(e) = result {
        warn!(delivery = %delivery_id, "failed to record delivery outcome: {e}");
    }
}

/// Resolves the signing secret for a delivery from its source, so a drain retry
/// or a manual replay re-signs with the **current** secret.
///
/// Every kind pumper writes must be resolvable here, or that kind's retries go
/// out UNSIGNED and a verifying receiver 401s them all the way down the retry
/// ladder to `dead` — a delivery the operator can see, replay, and still never
/// get accepted. The four kinds and where their secret lives:
///
/// | kind      | `ref_id`        | secret source                       |
/// |-----------|-----------------|-------------------------------------|
/// | `job`     | job id          | the job's `callback_secret` (DB)    |
/// | `change`  | watch id        | the watch's `secret` (DB)           |
/// | `search`  | saved-search id | the saved search's `secret` (DB)    |
/// | `failure` | job id          | `[webhooks] failure_secret` (config)|
///
/// `failure` is the reason this takes a config handle: its secret is the only
/// one that is NOT a row, so it has to be threaded in explicitly rather than
/// looked up. The handle is the `[webhooks]` section only — `Storage` stays
/// config-free.
///
/// Best-effort: a deleted source, an unparseable id, or an unknown kind yields
/// `None` (the delivery is re-sent unsigned, which is what it was sent as).
/// Unknown kinds resolve to `None` rather than falling through to a watch
/// lookup — looking up an arbitrary `ref_id` in `watches` can only ever hit by
/// accident, and signing with an unrelated watch's secret is worse than not
/// signing. Shared by the manual replay route and the auto-drain so they can't
/// drift.
pub async fn resolve_secret(
    storage: &Storage,
    webhooks: &WebhooksConfig,
    delivery: &Delivery,
) -> Option<String> {
    match delivery.kind.as_str() {
        "job" => {
            let job_id = uuid::Uuid::parse_str(&delivery.ref_id).ok()?;
            storage
                .get(job_id)
                .await
                .ok()
                .flatten()
                .and_then(|j| j.callback_secret)
        }
        "change" => storage
            .get_watch(&delivery.ref_id)
            .await
            .ok()
            .flatten()
            .and_then(|w| w.secret),
        "search" => storage
            .get_saved_search(&delivery.ref_id)
            .await
            .ok()
            .flatten()
            .and_then(|s| s.secret),
        "failure" => webhooks.failure_secret.clone(),
        _ => None,
    }
}

/// Whether a **manually** requested replay of a delivery in `status` may
/// proceed. Extracted because the route used to have no gate at all: it re-sent
/// `delivered` rows (a duplicate the receiver never asked for), `pending` rows
/// (a second sender racing the first, and racing its outcome write), and raced
/// the auto-drain for `failed` rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayGate {
    /// Replayable: `failed`/`dead`, or `delivered` under an explicit `force`.
    Allowed,
    /// In flight — an initial send or a drain retry owns this row right now.
    InFlight,
    /// Terminal-and-successful (`delivered`) without `force`, or a status this
    /// build doesn't know.
    NotReplayable,
}

pub(crate) fn replay_gate(status: &str, force: bool) -> ReplayGate {
    match status {
        // `dead` is exactly the row a human replays: the ladder gave up on it.
        "failed" | "dead" => ReplayGate::Allowed,
        "delivered" if force => ReplayGate::Allowed,
        "delivered" => ReplayGate::NotReplayable,
        "pending" => ReplayGate::InFlight,
        _ => ReplayGate::NotReplayable,
    }
}

/// How long a delivery may sit `pending` before [`reclaim_stale`] decides no
/// process is still working on it.
///
/// Worst case for one in-process delivery: `MAX_ATTEMPTS` (3) sends at the
/// webhook client's 15s timeout, plus the linear backoff sleeps between them
/// (0s + 2s + 4s) = **51s**. Ten minutes is ~11.8× that, so a delivery that is
/// merely slow is never double-sent; only one that no longer has a sender is
/// reclaimed. (Deliberately generous: a duplicate send is a real cost to the
/// receiver, while a reclaim delayed by minutes costs nothing — the row was
/// already stranded.)
pub(crate) const STALE_PENDING_SECS: i64 = 600;

/// Returns crash-interrupted `pending` deliveries to the retry ladder.
///
/// Runs at the head of [`drain_due`] rather than in the retention janitor: the
/// janitor ticks every 6 hours **and returns immediately unless some retention
/// knob is enabled** (all are off by default), so a default deployment would
/// never reclaim anything. The drain tick, in contrast, is exactly the loop that
/// consumes what this produces.
async fn reclaim_stale(state: &AppState) {
    match state
        .storage
        .reclaim_stale_deliveries(STALE_PENDING_SECS)
        .await
    {
        Ok(0) => {}
        Ok(n) => warn!(
            reclaimed = n,
            stale_after_secs = STALE_PENDING_SECS,
            "webhook drain: returned interrupted deliveries to the retry ladder (their sender \
             died before recording an outcome)"
        ),
        Err(e) => warn!("webhook drain: stale-pending reclaim failed: {e}"),
    }
}

/// One auto-drain pass: reclaim stranded rows, then re-send failed deliveries
/// whose backoff is due. Claims each row atomically (so a concurrent tick can't
/// double-send), resolves its secret, and hands it to [`replay`]. Piggybacked on
/// the scheduler tick.
pub async fn drain_due(state: &AppState) {
    reclaim_stale(state).await;
    let due = match state.storage.due_deliveries(DRAIN_BATCH).await {
        Ok(due) => due,
        Err(e) => {
            warn!("webhook drain: due-scan failed: {e}");
            return;
        }
    };
    for delivery in due {
        // Atomic claim: skip if another tick already took it. Note this claim
        // covers `failed` ONLY — the drain must never resurrect a `dead` row.
        match state.storage.begin_delivery_retry(&delivery.id).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                warn!(delivery = %delivery.id, "webhook drain: claim failed: {e}");
                continue;
            }
        }
        let secret = resolve_secret(&state.storage, &state.config.webhooks, &delivery).await;
        replay(
            state,
            delivery.id.clone(),
            delivery.url.clone(),
            delivery.event.clone(),
            delivery.body.into_bytes(),
            secret,
        )
        .await;
    }
}

/// The sink transport. `file://` pseudo-URLs append to the local sinks dir;
/// everything else (webhook + slack) is the HTTP retry loop: up to
/// MAX_ATTEMPTS sends with linear backoff. Returns
/// `(delivered, attempts_made, last_error, permanent)`. `permanent` is set when
/// the receiver returned a 4xx it will keep rejecting — the caller then marks
/// the row `dead` immediately instead of climbing the DLQ ladder. This is the
/// single branch point every path (fresh dispatch, DLQ drain, manual replay)
/// funnels through — and the seam where a future WASM `plugin:` sink would hook
/// in.
#[allow(clippy::too_many_arguments)]
async fn deliver(
    storage: &Storage,
    client: &reqwest::Client,
    url: &str,
    event: &str,
    delivery_id: &str,
    body: &[u8],
    secret: Option<&str>,
) -> (bool, i64, Option<String>, bool) {
    if url.starts_with(FILE_SINK_SCHEME) {
        return deliver_file(&sinks_dir(storage), url, event, delivery_id, body).await;
    }
    let mut last_error = None;
    // Sleep before the NEXT attempt: linear backoff by default, overridden by a
    // transient response's `Retry-After` (capped). Recomputed after each attempt.
    let mut next_sleep = Duration::from_secs(0);
    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 && !next_sleep.is_zero() {
            tokio::time::sleep(next_sleep).await;
        }
        // Default backoff for the attempt that follows this one (linear: 2s, 4s).
        next_sleep = Duration::from_secs(2 * (attempt + 1));
        // Per-attempt timestamp, covered by the signature so the receiver can
        // reject stale deliveries. The delivery id is STABLE across retries and
        // replays — that stability is what makes it a usable idempotency key.
        let ts = chrono::Utc::now().timestamp();
        let mut req = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-pumper-event", event)
            .header("x-pumper-delivery-id", delivery_id)
            .header("x-pumper-timestamp", ts.to_string())
            .body(body.to_vec());
        if let Some(secret) = secret {
            let sig = sign(secret.as_bytes(), ts, delivery_id, body);
            req = req.header("x-pumper-signature", format!("sha256={sig}"));
        }
        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                return (true, attempt as i64 + 1, None, false);
            }
            Ok(resp) => {
                let status = resp.status();
                match classify_status(status.as_u16()) {
                    DeliveryClass::Permanent => {
                        // A 4xx the receiver will keep rejecting: stop now rather
                        // than burning the remaining in-process attempts AND the
                        // whole DLQ ladder to reach the same `dead` state.
                        return (
                            false,
                            attempt as i64 + 1,
                            Some(format!("non-2xx (permanent): {status}")),
                            true,
                        );
                    }
                    _ => {
                        last_error = Some(format!("non-2xx: {status}"));
                        // Rate-limited / transient: honor a Retry-After hint for
                        // the next in-process sleep, capped so a huge hint can't
                        // park the pool slot (the DLQ ladder covers longer waits).
                        if let Some(ra) = resp
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(parse_retry_after)
                        {
                            next_sleep = ra.min(RETRY_AFTER_INPROC_CAP);
                        }
                    }
                }
            }
            Err(e) => last_error = Some(format!("send error: {e}")),
        }
    }
    (false, MAX_ATTEMPTS as i64, last_error, false)
}

/// File-sink transport: append one NDJSON envelope line. The envelope carries
/// the stable delivery id so a consumer can dedup lines re-appended by the
/// DLQ drain or a manual replay — the same idempotency contract webhooks get
/// via the `x-pumper-delivery-id` header. Single attempt: a local append that
/// fails (disk full, permissions) fails identically on an immediate retry, so
/// recovery is left to the backed-off DLQ drain.
async fn deliver_file(
    dir: &Path,
    url: &str,
    event: &str,
    delivery_id: &str,
    body: &[u8],
) -> (bool, i64, Option<String>, bool) {
    let Some(path) = file_sink_path(dir, url) else {
        // A malformed file-sink URL will never parse on a retry — permanent.
        return (
            false,
            1,
            Some(format!("invalid file-sink url '{url}'")),
            true,
        );
    };
    // serde_json output is single-line; a body that fails to parse (shouldn't
    // happen — we serialized it) is embedded as a JSON string, keeping every
    // line valid NDJSON.
    let payload = serde_json::from_slice::<serde_json::Value>(body)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(body).into_owned()));
    let line = serde_json::json!({
        "delivery_id": delivery_id,
        "event": event,
        "delivered_at": chrono::Utc::now().to_rfc3339(),
        "payload": payload,
    });
    match append_line(&path, &line).await {
        Ok(()) => (true, 1, None, false),
        // A disk/permissions failure may clear — leave it to the DLQ ladder.
        Err(e) => (false, 1, Some(format!("file sink append: {e}")), false),
    }
}

async fn append_line(path: &Path, line: &serde_json::Value) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut bytes = serde_json::to_vec(line)?;
    bytes.push(b'\n');
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(&bytes).await?;
    file.flush().await
}

/// Signature base `HMAC(secret, "{ts}.{delivery_id}." ++ body)` — the timestamp
/// and delivery id are covered so a captured request can't be replayed with a
/// fresh timestamp, and the receiver can bind the signature to the idempotency key.
/// `pub(crate)`: the inbound ingress surface verifies with the exact same base
/// (inverted), so the two directions cannot drift.
pub(crate) fn sign(secret: &[u8], ts: i64, delivery_id: &str, body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(format!("{ts}.{delivery_id}.").as_bytes());
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Inbound verification: the inverse of [`sign`]. `provided` is the hex digest
/// from `x-pumper-signature` (any `sha256=` prefix already stripped).
///
/// Two bases, one scheme:
/// - `context = Some((ts, delivery_id))` — the full pumper scheme
///   (`"{ts}.{id}." ++ body`), used when the sender supplied
///   `x-pumper-timestamp` (pumper-to-pumper federation).
/// - `context = None` — bare `HMAC(secret, body)`, byte-for-byte the scheme
///   GitHub uses for `x-hub-signature-256`, so a GitHub webhook can point
///   straight at `/ingest/{id}` with a shared secret.
///
/// The comparison is constant-time (`Mac::verify_slice`), so the digest can't
/// be recovered byte-by-byte through timing. A malformed hex digest fails.
pub(crate) fn verify_signature(
    secret: &[u8],
    context: Option<(i64, &str)>,
    body: &[u8],
    provided: &str,
) -> bool {
    let Ok(provided) = hex::decode(provided) else {
        return false;
    };
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    if let Some((ts, delivery_id)) = context {
        mac.update(format!("{ts}.{delivery_id}.").as_bytes());
    }
    mac.update(body);
    mac.verify_slice(&provided).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::testing::TempStore;

    /// A `Delivery` shell carrying only what [`resolve_secret`] reads.
    fn delivery(kind: &str, ref_id: &str) -> Delivery {
        Delivery {
            id: "d-1".into(),
            kind: kind.into(),
            ref_id: ref_id.into(),
            url: "https://x/hook".into(),
            event: "e".into(),
            body: "{}".into(),
            status: "failed".into(),
            attempts: 1,
            last_error: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    fn webhooks_with_failure_secret(secret: Option<&str>) -> WebhooksConfig {
        WebhooksConfig {
            failure_secret: secret.map(str::to_string),
            ..Default::default()
        }
    }

    /// The anti-pattern: a `search` delivery's retries went out UNSIGNED because
    /// `resolve_secret` fell through to a watch lookup that could never hit a
    /// saved-search id — so a verifying receiver 401'd the whole ladder to
    /// `dead`.
    #[tokio::test]
    async fn search_replay_signed_not_unsigned() {
        let store = TempStore::new("resolve-search").await;
        let search = store
            .storage
            .create_saved_search("q", None, None, "https://x/hook", Some("s3-search"), None)
            .await
            .expect("create saved search");
        let cfg = WebhooksConfig::default();
        assert_eq!(
            resolve_secret(&store.storage, &cfg, &delivery("search", &search.id)).await,
            Some("s3-search".to_string())
        );
    }

    /// The anti-pattern: `failure` deliveries are signed on first send (the
    /// dispatcher has the config) but their retries were not, because the secret
    /// is the one that lives in config rather than in a row.
    #[tokio::test]
    async fn failure_replay_signed_not_unsigned() {
        let store = TempStore::new("resolve-failure").await;
        let cfg = webhooks_with_failure_secret(Some("s3-failure"));
        let job_id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            resolve_secret(&store.storage, &cfg, &delivery("failure", &job_id)).await,
            Some("s3-failure".to_string()),
            "the failure secret comes from [webhooks], not from the job row"
        );
        // No secret configured → unsigned, exactly as the first send was.
        assert_eq!(
            resolve_secret(
                &store.storage,
                &WebhooksConfig::default(),
                &delivery("failure", &job_id)
            )
            .await,
            None
        );
    }

    #[tokio::test]
    async fn job_and_change_kinds_resolve_their_row_secrets() {
        let store = TempStore::new("resolve-job-change").await;
        let cfg = WebhooksConfig::default();

        let job = store
            .storage
            .enqueue(
                "fake",
                pumper_core::EnqueueOptions {
                    callback_url: Some("https://x/hook".into()),
                    callback_secret: Some("s3-job".into()),
                    max_attempts: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("enqueue");
        assert_eq!(
            resolve_secret(&store.storage, &cfg, &delivery("job", &job.id.to_string())).await,
            Some("s3-job".to_string())
        );

        let watch = store
            .storage
            .create_watch("fake", "*", "https://x/hook", Some("s3-watch"), "webhook")
            .await
            .expect("create watch");
        assert_eq!(
            resolve_secret(&store.storage, &cfg, &delivery("change", &watch.id)).await,
            Some("s3-watch".to_string())
        );
    }

    /// A deleted source must yield `None` (re-send unsigned) rather than an
    /// error or a stale secret — and an unknown kind must NOT fall through to a
    /// watch lookup, which could only ever match by accident and would sign with
    /// an unrelated subscriber's secret.
    #[tokio::test]
    async fn deleted_source_and_unknown_kind_resolve_none_not_a_watch_lookup() {
        let store = TempStore::new("resolve-missing").await;
        let cfg = webhooks_with_failure_secret(Some("s3-failure"));
        for d in [
            delivery("job", &uuid::Uuid::new_v4().to_string()),
            delivery("job", "not-a-uuid"),
            delivery("change", "watch-that-was-deleted"),
            delivery("search", "search-that-was-deleted"),
        ] {
            assert_eq!(
                resolve_secret(&store.storage, &cfg, &d).await,
                None,
                "deleted/unparseable source for kind {:?}",
                d.kind
            );
        }
        // An unknown kind gets nothing — not the failure secret, not a watch's.
        let watch = store
            .storage
            .create_watch("fake", "*", "https://x/hook", Some("s3-watch"), "webhook")
            .await
            .expect("create watch");
        assert_eq!(
            resolve_secret(&store.storage, &cfg, &delivery("mystery", &watch.id)).await,
            None
        );
    }

    /// The anti-pattern the gate exists for: `POST /replay` re-sent whatever row
    /// it was handed — a `delivered` one (an unrequested duplicate) and a
    /// `pending` one (a second sender racing the first).
    #[test]
    fn replay_gate_blocks_delivered_and_inflight_not_only_missing_rows() {
        assert_eq!(replay_gate("failed", false), ReplayGate::Allowed);
        assert_eq!(replay_gate("dead", false), ReplayGate::Allowed);
        assert_eq!(replay_gate("pending", false), ReplayGate::InFlight);
        assert_eq!(replay_gate("pending", true), ReplayGate::InFlight);
        assert_eq!(replay_gate("delivered", false), ReplayGate::NotReplayable);
        assert_eq!(replay_gate("delivered", true), ReplayGate::Allowed);
        assert_eq!(
            replay_gate("brand-new-status", true),
            ReplayGate::NotReplayable
        );
    }

    /// The anti-pattern: `let _ = deliver(...)` on the branch where the log row
    /// could not be written — the send happened and its result was dropped on
    /// the floor, so the ONE delivery with no DLQ row also had no report.
    #[test]
    fn unlogged_fallback_reports_outcome_not_silence() {
        let failed = unlogged_outcome_summary(&(false, 3, Some("non-2xx: 503".into()), false));
        assert!(failed.contains('3'), "attempts are in the line: {failed}");
        assert!(
            failed.contains("503"),
            "last error is in the line: {failed}"
        );
        assert!(
            failed.contains("NOT delivered") && failed.contains("not in the DLQ"),
            "says both what happened and why it isn't recoverable: {failed}"
        );
        // A missing error string must not render as an empty tail.
        let no_error = unlogged_outcome_summary(&(false, 1, None, false));
        assert!(no_error.contains("no error recorded"), "{no_error}");
        // The success case is still reported — "it went out" is the fact an
        // operator needs when the log row is missing.
        let ok = unlogged_outcome_summary(&(true, 2, None, false));
        assert!(ok.starts_with("delivered after 2"), "{ok}");
    }

    /// THE anti-pattern this closes: `deliver` treated every non-2xx identically,
    /// so a receiver that returns a PERMANENT 4xx (410 Gone, 400 malformed, 404,
    /// 422) burned all 3 in-process attempts AND climbed the full 5-rung DLQ
    /// ladder (30s→1m→5m→30m→2h) — ~8 sends over ~2.6h — before the row read
    /// `dead`. A permanent error is retried as if transient. Only 429 among the
    /// 4xx is transient (rate-limited); 5xx stays transient.
    #[test]
    fn permanent_4xx_is_not_retried_but_429_and_5xx_are() {
        use DeliveryClass::*;
        assert_eq!(classify_status(200), Success);
        assert_eq!(classify_status(204), Success);
        // The permanent set: a resend cannot fix these.
        for s in [400u16, 401, 403, 404, 405, 410, 422] {
            assert_eq!(classify_status(s), Permanent, "status {s} must be permanent");
        }
        // Rate-limited and server errors are worth retrying.
        assert_eq!(classify_status(429), Transient, "429 is rate-limited, not permanent");
        for s in [500u16, 502, 503, 504] {
            assert_eq!(classify_status(s), Transient, "status {s} must be transient");
        }
    }

    /// A 429's `Retry-After` hint is honored (delta-seconds), capped so a large
    /// hint can't park a delivery-pool slot; the HTTP-date form and garbage yield
    /// `None` so the loop falls back to its linear backoff.
    #[test]
    fn retry_after_delta_seconds_is_parsed_and_capped() {
        assert_eq!(parse_retry_after("2"), Some(Duration::from_secs(2)));
        assert_eq!(parse_retry_after("  120 "), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("0"), Some(Duration::from_secs(0)));
        // Not honored in this loop: HTTP-date form and garbage.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after("-5"), None);
        assert_eq!(parse_retry_after(""), None);
        // The in-process honoring is capped: a big hint is clamped to the cap
        // (the DLQ ladder covers anything longer).
        let honored = parse_retry_after("300").unwrap().min(RETRY_AFTER_INPROC_CAP);
        assert_eq!(honored, RETRY_AFTER_INPROC_CAP);
    }

    #[tokio::test]
    async fn a_permanent_rejection_marks_the_row_dead_now_not_after_the_ladder() {
        let store = TempStore::new("permanent-dead").await;
        let id = store
            .storage
            .create_delivery("job", "ref", "https://x/hook", "evt", "{}")
            .await
            .expect("create delivery");
        // The permanent branch of log_outcome: kill_delivery, not fail_delivery.
        log_outcome(
            &store.storage,
            &id,
            "https://x/hook",
            (false, 1, Some("non-2xx (permanent): 410 Gone".into()), true),
        )
        .await;
        let row = store
            .storage
            .get_delivery(&id)
            .await
            .expect("read delivery")
            .expect("row exists");
        assert_eq!(row.status, "dead", "a permanent 4xx is dead immediately");
        // Contrast: a transient failure schedules a retry (status `failed`), it
        // does NOT jump to dead.
        let id2 = store
            .storage
            .create_delivery("job", "ref", "https://x/hook", "evt", "{}")
            .await
            .expect("create delivery");
        log_outcome(
            &store.storage,
            &id2,
            "https://x/hook",
            (false, 3, Some("non-2xx: 503".into()), false),
        )
        .await;
        let row2 = store
            .storage
            .get_delivery(&id2)
            .await
            .expect("read delivery")
            .expect("row exists");
        assert_eq!(row2.status, "failed", "a transient 5xx ladders, not dies");
    }

    #[test]
    fn stale_pending_threshold_clears_the_worst_case_in_process_delivery() {
        // 3 attempts x 15s client timeout + the between-attempt sleeps. Each of
        // the 2 gaps is linear backoff (2s, 4s) OR, when a transient response
        // carried a Retry-After, at most RETRY_AFTER_INPROC_CAP — so the worst
        // case is 2 gaps of the cap.
        let cap = RETRY_AFTER_INPROC_CAP.as_secs() as i64;
        let worst_gap = cap.max(4); // the larger of the linear tail and the cap
        let worst_case_secs = (MAX_ATTEMPTS as i64) * 15 + 2 * worst_gap;
        assert!(
            STALE_PENDING_SECS >= worst_case_secs * 10,
            "reclaim must not race a merely-slow delivery: {STALE_PENDING_SECS}s vs \
             {worst_case_secs}s worst case"
        );
    }

    #[test]
    fn verify_inverts_sign() {
        let secret = b"s3cr3t";
        let body = br#"{"ok":true}"#;
        let sig = sign(secret, 1_700_000_000, "d-1", body);
        assert!(verify_signature(
            secret,
            Some((1_700_000_000, "d-1")),
            body,
            &sig
        ));
        // Any covered component changing must fail: ts, id, body, secret.
        assert!(!verify_signature(
            secret,
            Some((1_700_000_001, "d-1")),
            body,
            &sig
        ));
        assert!(!verify_signature(
            secret,
            Some((1_700_000_000, "d-2")),
            body,
            &sig
        ));
        assert!(!verify_signature(
            secret,
            Some((1_700_000_000, "d-1")),
            b"tampered",
            &sig
        ));
        assert!(!verify_signature(
            b"wrong",
            Some((1_700_000_000, "d-1")),
            body,
            &sig
        ));
    }

    #[test]
    fn file_sink_path_accepts_only_bare_safe_filenames() {
        let dir = Path::new("data/sinks");
        // The URL our own dispatch builds resolves inside the dir.
        let url = file_sink_url("0b6a9de1-2f7e-4d3c-9d59-000000000000");
        let path = file_sink_path(dir, &url).expect("watch-id filename is valid");
        assert!(path.starts_with(dir));
        assert!(path.to_string_lossy().ends_with(".ndjson"));

        // Anything a tampered delivery row could try is rejected.
        for bad in [
            "file://../escape.ndjson",
            "file://..",
            "file://a/b.ndjson",
            "file://a\\b.ndjson",
            "file://",
            "file://C:evil.ndjson",
            "https://example.test/hook",
        ] {
            assert!(file_sink_path(dir, bad).is_none(), "must reject {bad:?}");
        }
    }

    #[test]
    fn slack_summary_is_a_compact_text_message() {
        let payload = serde_json::json!({
            "event": "dataset.changed",
            "app": "grants",
            "dataset": "grants/opportunities",
            "count": 3,
            "job_id": "j-1",
            "changes": [{"k": 1}, {"k": 2}, {"k": 3}],
        });
        let msg = slack_summary(&payload);
        let text = msg["text"].as_str().expect("incoming-webhook `text` field");
        assert!(text.contains("grants/opportunities"), "names the dataset");
        assert!(text.contains('3'), "carries the delta count");
        assert!(text.contains("j-1"), "carries the job id");
        assert!(
            msg.get("changes").is_none(),
            "summary only — full revisions never leave for Slack"
        );
        // Singular form for one revision.
        let one = slack_summary(&serde_json::json!({"count": 1}));
        assert!(one["text"].as_str().unwrap().contains("1 revision ("));
    }

    #[test]
    fn bare_mode_matches_github_hub_signature_scheme() {
        // GitHub signs exactly HMAC-SHA256(secret, body) — bare mode must
        // accept that digest and reject the timestamped one, and vice versa.
        use hmac::Mac;
        let secret = b"gh-secret";
        let body = br#"{"ref":"refs/heads/main"}"#;
        let mut mac = HmacSha256::new_from_slice(secret).unwrap();
        mac.update(body);
        let github_hex = hex::encode(mac.finalize().into_bytes());
        assert!(verify_signature(secret, None, body, &github_hex));
        assert!(!verify_signature(secret, Some((0, "x")), body, &github_hex));
        // Garbage hex never verifies (and never panics).
        assert!(!verify_signature(secret, None, body, "not-hex"));
    }
}
