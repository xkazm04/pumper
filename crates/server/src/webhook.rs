//! Result delivery via webhooks. When a job reaches a terminal state and set a
//! `callback_url`, the worker fires the job JSON at that URL so consuming apps
//! don't have to poll; dataset watches receive `dataset.changed` events the
//! same way. If a secret was supplied, the body is signed with HMAC-SHA256 and
//! sent as `X-Pumper-Signature: sha256=<hex>` so the receiver can verify
//! authenticity. Every delivery is logged to `webhook_deliveries` — failed
//! rows are the dead-letter queue, replayable via the API.

use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use pumper_core::{Delivery, Job, Storage, Watch};
use sha2::Sha256;
use tracing::{debug, warn};

use crate::state::AppState;

type HmacSha256 = Hmac<Sha256>;

const MAX_ATTEMPTS: u64 = 3;

/// Auto-drain backoff schedule (seconds) indexed by a delivery's `retry_count`:
/// 30s → 1m → 5m → 30m → 2h. Past the last entry the row is marked `dead`.
const DRAIN_BACKOFF_SECS: &[i64] = &[30, 60, 300, 1800, 7200];
/// Max background retries before a delivery is declared `dead` (= backoff len).
const DRAIN_MAX_RETRIES: i64 = 5;
/// Deliveries re-sent per drain tick — a small batch so one tick can't stampede
/// a just-recovered receiver.
const DRAIN_BATCH: i64 = 20;

/// Spawns a best-effort, logged delivery of a terminal job to its callback.
pub fn dispatch(client: reqwest::Client, storage: Arc<Storage>, job: Job) {
    let Some(url) = job.callback_url.clone() else {
        return;
    };
    let secret = job.callback_secret.clone();
    let id = job.id.to_string();
    dispatch_event(
        client,
        storage,
        "job",
        &id,
        &url,
        "job.terminal",
        &job,
        secret,
    );
}

/// Spawns a best-effort, logged delivery of a `dataset.changed` event.
pub fn dispatch_change(
    client: reqwest::Client,
    storage: Arc<Storage>,
    watch: Watch,
    payload: serde_json::Value,
) {
    dispatch_event(
        client,
        storage,
        "change",
        &watch.id.clone(),
        &watch.url.clone(),
        "dataset.changed",
        &payload,
        watch.secret.clone(),
    );
}

/// Spawns a best-effort, logged delivery of an arbitrary event — the generic
/// entry point for new event kinds (e.g. saved-search matches).
//
// clippy::too_many_arguments (8/7) — the eight are the webhook wire contract
// itself (transport, storage, kind, ref_id, url, event, payload, secret); every
// one is independently supplied by the caller, so collapsing them behind a
// default-able struct would let a caller silently omit `secret` (unsigned
// delivery) or `ref_id` (unattributable log line). Allowed at this one site
// rather than bulk-suppressed. FOLLOW-UP: introduce a `DispatchEvent` builder
// with `secret`/`ref_id` as required constructor args, then drop this allow.
#[allow(clippy::too_many_arguments)]
pub fn dispatch_event(
    client: reqwest::Client,
    storage: Arc<Storage>,
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
    spawn_logged(
        client,
        storage,
        kind.to_string(),
        ref_id.to_string(),
        url.to_string(),
        event.to_string(),
        body,
        secret,
    );
}

/// Spawns a best-effort, logged `job.failed` delivery to the global failure
/// subscriber (`[webhooks] failure_url`). Fires on PERMANENT failure only — a
/// job's own `callback_url` already receives the terminal job JSON, so this is
/// the cross-app firehose path, not a per-job duplicate.
pub fn dispatch_failure(
    client: reqwest::Client,
    storage: Arc<Storage>,
    url: &str,
    secret: Option<String>,
    job: &Job,
) {
    let payload = serde_json::json!({
        "event": "job.failed",
        "job_id": job.id,
        "app": job.app,
        "error": job.error,
        "attempts": job.attempts,
        "schedule_id": job.schedule_id,
    });
    dispatch_event(
        client,
        storage,
        "failure",
        &job.id.to_string(),
        url,
        "job.failed",
        &payload,
        secret,
    );
}

/// Re-sends a logged delivery (the dead-letter replay path). The caller has
/// already resolved the signing secret from the delivery's source.
pub fn replay(
    client: reqwest::Client,
    storage: Arc<Storage>,
    delivery_id: String,
    url: String,
    event: String,
    body: Vec<u8>,
    secret: Option<String>,
) {
    tokio::spawn(async move {
        let outcome = deliver(
            &client,
            &url,
            &event,
            &delivery_id,
            &body,
            secret.as_deref(),
        )
        .await;
        log_outcome(&storage, &delivery_id, &url, outcome).await;
    });
}

/// Creates the log row, runs the delivery loop, records the outcome.
#[allow(clippy::too_many_arguments)]
fn spawn_logged(
    client: reqwest::Client,
    storage: Arc<Storage>,
    kind: String,
    ref_id: String,
    url: String,
    event: String,
    body: Vec<u8>,
    secret: Option<String>,
) {
    tokio::spawn(async move {
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
                let _ = deliver(
                    &client,
                    &url,
                    &event,
                    &fallback_id,
                    &body,
                    secret.as_deref(),
                )
                .await;
                return;
            }
        };
        let outcome = deliver(
            &client,
            &url,
            &event,
            &delivery_id,
            &body,
            secret.as_deref(),
        )
        .await;
        log_outcome(&storage, &delivery_id, &url, outcome).await;
    });
}

async fn log_outcome(
    storage: &Storage,
    delivery_id: &str,
    url: &str,
    outcome: (bool, i64, Option<String>),
) {
    let (delivered, attempts, last_error) = outcome;
    let result = if delivered {
        debug!(delivery = %delivery_id, url = %url, "webhook delivered");
        storage
            .finish_delivery(delivery_id, true, attempts, last_error.as_deref())
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

/// Resolves the signing secret for a delivery from its source (the job's callback
/// secret or the watch's secret), so a replay re-signs with the current secret.
/// Best-effort: a missing/deleted source or an unparseable job id yields `None`
/// (the delivery is simply re-sent unsigned). Shared by the manual replay route
/// and the auto-drain so they can't drift.
pub async fn resolve_secret(storage: &Storage, delivery: &Delivery) -> Option<String> {
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
        _ => storage
            .get_watch(&delivery.ref_id)
            .await
            .ok()
            .flatten()
            .and_then(|w| w.secret),
    }
}

/// One auto-drain pass: re-send failed deliveries whose backoff is due. Claims
/// each row atomically (so a concurrent tick can't double-send), resolves its
/// secret, and hands it to [`replay`]. Piggybacked on the scheduler tick.
pub async fn drain_due(state: &AppState) {
    let due = match state.storage.due_deliveries(DRAIN_BATCH).await {
        Ok(due) => due,
        Err(e) => {
            warn!("webhook drain: due-scan failed: {e}");
            return;
        }
    };
    for delivery in due {
        // Atomic claim: skip if another tick already took it.
        match state.storage.begin_delivery_retry(&delivery.id).await {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                warn!(delivery = %delivery.id, "webhook drain: claim failed: {e}");
                continue;
            }
        }
        let secret = resolve_secret(&state.storage, &delivery).await;
        replay(
            state.webhook_client.clone(),
            state.storage.clone(),
            delivery.id.clone(),
            delivery.url.clone(),
            delivery.event.clone(),
            delivery.body.into_bytes(),
            secret,
        );
    }
}

/// The retry loop: up to MAX_ATTEMPTS sends with linear backoff. Returns
/// (delivered, attempts_made, last_error).
async fn deliver(
    client: &reqwest::Client,
    url: &str,
    event: &str,
    delivery_id: &str,
    body: &[u8],
    secret: Option<&str>,
) -> (bool, i64, Option<String>) {
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
        }
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
                return (true, attempt as i64 + 1, None);
            }
            Ok(resp) => last_error = Some(format!("non-2xx: {}", resp.status())),
            Err(e) => last_error = Some(format!("send error: {e}")),
        }
    }
    (false, MAX_ATTEMPTS as i64, last_error)
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
