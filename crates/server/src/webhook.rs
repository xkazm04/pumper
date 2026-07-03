//! Result delivery via webhooks. When a job reaches a terminal state and set a
//! `callback_url`, the worker fires the job JSON at that URL so consuming apps
//! don't have to poll. If a `callback_secret` was supplied, the body is signed
//! with HMAC-SHA256 and sent as `X-Pumper-Signature: sha256=<hex>` so the
//! receiver can verify authenticity.

use std::time::Duration;

use hmac::{Hmac, Mac};
use pumper_core::Job;
use sha2::Sha256;
use tracing::{debug, warn};

type HmacSha256 = Hmac<Sha256>;

/// Spawns a best-effort delivery with a few retries. Never blocks the worker.
pub fn dispatch(client: reqwest::Client, job: Job) {
    let Some(url) = job.callback_url.clone() else {
        return;
    };
    let secret = job.callback_secret.clone();

    tokio::spawn(async move {
        let body = match serde_json::to_vec(&job) {
            Ok(body) => body,
            Err(e) => {
                warn!(job = %job.id, "webhook serialize failed: {e}");
                return;
            }
        };
        let signature = secret.as_ref().map(|s| sign(s.as_bytes(), &body));

        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
            }
            let mut req = client
                .post(&url)
                .header("content-type", "application/json")
                .header("x-pumper-event", "job.terminal")
                .body(body.clone());
            if let Some(sig) = &signature {
                req = req.header("x-pumper-signature", format!("sha256={sig}"));
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(job = %job.id, url = %url, "webhook delivered");
                    return;
                }
                Ok(resp) => warn!(job = %job.id, status = %resp.status(), "webhook non-2xx"),
                Err(e) => warn!(job = %job.id, "webhook send error: {e}"),
            }
        }
        warn!(job = %job.id, url = %url, "webhook delivery gave up after retries");
    });
}

fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}
