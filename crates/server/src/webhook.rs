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
use pumper_core::{Job, Storage, Watch};
use sha2::Sha256;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

const MAX_ATTEMPTS: u64 = 3;

/// Spawns a best-effort, logged delivery of a terminal job to its callback.
pub fn dispatch(client: reqwest::Client, storage: Arc<Storage>, job: Job) {
    let Some(url) = job.callback_url.clone() else {
        return;
    };
    let secret = job.callback_secret.clone();
    let id = job.id.to_string();
    dispatch_event(client, storage, "job", &id, &url, "job.terminal", &job, secret);
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
    dispatch_event(client, storage, "failure", &job.id.to_string(), url, "job.failed", &payload, secret);
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
        let outcome = deliver(&client, &url, &event, &body, secret.as_deref()).await;
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
            .create_delivery(&kind, &ref_id, &url, &event, &String::from_utf8_lossy(&body))
            .await
        {
            Ok(id) => id,
            Err(e) => {
                warn!(url = %url, "delivery log write failed (sending anyway): {e}");
                let _ = deliver(&client, &url, &event, &body, secret.as_deref()).await;
                return;
            }
        };
        let outcome = deliver(&client, &url, &event, &body, secret.as_deref()).await;
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
    if delivered {
        debug!(delivery = %delivery_id, url = %url, "webhook delivered");
    } else {
        warn!(delivery = %delivery_id, url = %url, "webhook delivery gave up after retries");
    }
    if let Err(e) = storage
        .finish_delivery(delivery_id, delivered, attempts, last_error.as_deref())
        .await
    {
        warn!(delivery = %delivery_id, "failed to record delivery outcome: {e}");
    }
}

/// The retry loop: up to MAX_ATTEMPTS sends with linear backoff. Returns
/// (delivered, attempts_made, last_error).
async fn deliver(
    client: &reqwest::Client,
    url: &str,
    event: &str,
    body: &[u8],
    secret: Option<&str>,
) -> (bool, i64, Option<String>) {
    let signature = secret.map(|s| sign(s.as_bytes(), body));
    let mut last_error = None;
    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
        }
        let mut req = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-pumper-event", event)
            .body(body.to_vec());
        if let Some(sig) = &signature {
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

fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}
