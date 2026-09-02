//! The **host side** of a plugin's declared capabilities (N10): the real
//! network and the real store behind [`PluginCapabilityHost`].
//!
//! The split is deliberate and load-bearing. `engine-wasm` owns the DECISION —
//! it builds each plugin's linker from that plugin's manifest and runs
//! [`pumper_core::plugin::authorize_http`] before any request leaves — and this
//! module owns only the PERFORMANCE of an already-authorized call. A second
//! implementation of this trait therefore cannot ship a weaker gate, because it
//! never sees the gate.
//!
//! Two things this module is careful about:
//!
//! * **The chokepoint.** A plugin's HTTP goes through the same
//!   [`HttpClient`] every raw-HTTP caller in the process uses — the one whose
//!   `send` acquires the per-host politeness governor, consults the response
//!   cache, applies the body cap and teaches the governor from 429s. A plugin
//!   is therefore governed exactly like the crawler is, and shows up on
//!   `GET /hosts` like any other traffic. It is NOT routed through
//!   `Fetcher::fetch`: that is the tiered GET ladder (browser and Claude
//!   escalation included), and a connector POSTing a delivery has no business
//!   escalating into a browser.
//! * **The namespace.** `plugin_kv`'s primary key starts with the plugin name,
//!   and the plugin name is supplied by the host, never by the guest. There is
//!   no call shape that reads another plugin's keys.

use std::sync::Arc;

use pumper_core::engine::{HttpClient, HttpMethod, HttpRequest};
use pumper_core::plugin::{PluginCapabilityHost, PluginHttpRequest, PluginHttpResponse};
use sqlx::SqlitePool;

/// Wall-clock ceiling on one plugin HTTP call.
///
/// A plugin call holds an admission permit for the whole call (the permit rides
/// with the blocking work), so a slow origin does not merely delay one delivery
/// — it occupies a slot in `[plugins] max_concurrent`. That is the
/// "admission-gate starvation by slow HTTP calls" risk the design named, and
/// this bound plus `caps::MAX_CAPABILITY_CALLS` is what keeps it finite:
/// worst case per invocation is `MAX_CAPABILITY_CALLS × PLUGIN_HTTP_TIMEOUT`.
const PLUGIN_HTTP_TIMEOUT_SECS: u64 = 20;

/// Body cap for a plugin's own request. Well under the sandbox's 4 MiB payload
/// cap, because a connector reads an API response, not a page.
const PLUGIN_HTTP_MAX_BODY_BYTES: u64 = 2 * 1024 * 1024;

/// Largest value one plugin may store under one key.
const KV_MAX_VALUE_BYTES: usize = 64 * 1024;

/// Keys one plugin may hold. The store is a cursor/dedup scratchpad, not a
/// dataset — a connector that needs more than this wants `upsert_many` and a
/// dynamic app.
const KV_MAX_KEYS_PER_PLUGIN: i64 = 1_000;

pub struct ServerCapabilityHost {
    http: Arc<dyn HttpClient>,
    pool: SqlitePool,
}

impl ServerCapabilityHost {
    pub fn new(http: Arc<dyn HttpClient>, pool: SqlitePool) -> Self {
        Self { http, pool }
    }
}

/// Maps the plugin's method string onto what this process's HTTP engine can
/// actually send.
///
/// `None` is an honest refusal, not a downgrade to GET: silently turning a
/// `DELETE` into a `GET` would make a connector believe it deleted something.
/// The v1 engine speaks GET and POST; a manifest may declare more, and the
/// refusal names the gap.
pub fn transport_method(method: &str) -> Option<HttpMethod> {
    match method.trim().to_ascii_uppercase().as_str() {
        "GET" => Some(HttpMethod::Get),
        "POST" => Some(HttpMethod::Post),
        _ => None,
    }
}

#[async_trait::async_trait]
impl PluginCapabilityHost for ServerCapabilityHost {
    async fn http_request(&self, plugin: &str, req: PluginHttpRequest) -> PluginHttpResponse {
        let Some(method) = transport_method(&req.method) else {
            return PluginHttpResponse::failed(format!(
                "method '{}' is declared but this host's HTTP engine sends GET and POST only",
                req.method
            ));
        };
        let request = HttpRequest {
            url: req.url.clone(),
            method,
            headers: req.headers,
            body: req.body,
            // A connector's call is an ACTION with a receiver, not a document
            // read: serving a POST's answer out of a TTL cache would make a
            // retried delivery look delivered.
            no_cache: true,
            max_body_bytes: Some(PLUGIN_HTTP_MAX_BODY_BYTES),
            timeout_secs: Some(PLUGIN_HTTP_TIMEOUT_SECS),
            ..HttpRequest::get(req.url.clone())
        };
        match self.http.fetch(request).await {
            Ok(resp) => {
                tracing::debug!(
                    plugin = %plugin, url = %req.url, status = resp.status,
                    "plugin capability http call"
                );
                PluginHttpResponse::ok(resp.status, resp.body)
            }
            Err(e) => PluginHttpResponse::failed(e.to_string()),
        }
    }

    async fn kv_get(&self, plugin: &str, key: &str) -> Option<String> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM plugin_kv WHERE plugin = ?1 AND key = ?2")
                .bind(plugin)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .unwrap_or_else(|e| {
                    // A store failure is not an absent key, but the ABI has one
                    // miss value; the log is where the difference survives.
                    tracing::warn!(plugin = %plugin, "plugin kv_get failed: {e}");
                    None
                });
        row.map(|(v,)| v)
    }

    async fn kv_put(
        &self,
        plugin: &str,
        key: &str,
        value: &str,
    ) -> std::result::Result<(), String> {
        if key.is_empty() {
            return Err("an empty key is not a key".into());
        }
        if value.len() > KV_MAX_VALUE_BYTES {
            return Err(format!(
                "value is {} bytes, over the {KV_MAX_VALUE_BYTES}-byte per-key cap",
                value.len()
            ));
        }
        // Counted BEFORE the insert, and only for a key that does not exist yet,
        // so overwriting a cursor never trips the ceiling.
        let existing: Option<(i64,)> =
            sqlx::query_as("SELECT 1 FROM plugin_kv WHERE plugin = ?1 AND key = ?2")
                .bind(plugin)
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| e.to_string())?;
        if existing.is_none() {
            let (count,): (i64,) =
                sqlx::query_as("SELECT COUNT(*) FROM plugin_kv WHERE plugin = ?1")
                    .bind(plugin)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| e.to_string())?;
            if count >= KV_MAX_KEYS_PER_PLUGIN {
                return Err(format!(
                    "this plugin already holds {count} keys, at the {KV_MAX_KEYS_PER_PLUGIN} \
                     ceiling — the kv capability is a cursor scratchpad, not a dataset"
                ));
            }
        }
        sqlx::query(
            "INSERT INTO plugin_kv (plugin, key, value, updated_at) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (plugin, key) DO UPDATE SET value = excluded.value, \
             updated_at = excluded.updated_at",
        )
        .bind(plugin)
        .bind(key)
        .bind(value)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&self.pool)
        .await
        .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal that must not become a downgrade: a connector told the host
    /// to DELETE and got a GET would believe a deletion happened.
    #[test]
    fn an_unsendable_method_is_refused_not_downgraded_to_get() {
        assert_eq!(transport_method("get"), Some(HttpMethod::Get));
        assert_eq!(transport_method(" POST "), Some(HttpMethod::Post));
        assert_eq!(transport_method("DELETE"), None);
        assert_eq!(transport_method("PUT"), None);
        assert_eq!(transport_method(""), None);
    }
}
