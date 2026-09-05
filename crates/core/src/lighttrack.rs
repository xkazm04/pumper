//! LightTrack emitter: an **opt-in external** sink for research-chokepoint
//! calls, alongside the internal cost ledger [`crate::costs::CostLedger`]
//! [`crate::app::AppContext::research`] already writes to on every call.
//!
//! `.ai/use-cases.json` declares 9 LLM call sites for this workspace but has
//! no way to report usage against them — the internal ledger tracks spend per
//! `app`, which is coarser than a use case (several apps declare more than
//! one; `provisioner` has two). This module POSTs one event per research call
//! to `{LIGHTTRACK_URL}/v1/events`, carrying the use-case key as `name` so the
//! gap between declared and observed becomes visible externally.
//!
//! Three rules govern everything here:
//!
//! - **Opt-in and inert by default.** [`config_from_env`] returns `None`
//!   whenever `LIGHTTRACK_URL` is unset — no client is built, no DNS lookup
//!   happens, nothing touches the network. A job that never configures
//!   LightTrack behaves exactly as it did before this module existed.
//! - **Fire-and-forget.** This workspace runs unattended jobs; a telemetry
//!   sink being down must be invisible to them. [`emit_research`] never
//!   returns a `Result` and never blocks its caller — the actual HTTP POST
//!   runs on its own spawned task with a short timeout, and every failure
//!   mode (build error, timeout, connection refused, non-2xx) is at most a
//!   `tracing::debug!`.
//! - **Emitted for failures too.** The chokepoint already meters a failed
//!   call's spend into the ledger *before* the error propagates
//!   ([`crate::error::ClaudeSpend::ledger_event`]) so a budget cannot be
//!   defeated by failing. [`emit_research`] is called from the same two
//!   branches, right after that internal write, carrying the identical
//!   `cost_usd` — this module never disturbs the meter-before-raise ordering,
//!   it only observes it.

use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;

/// Bounded so a dead or slow sink can never hold a research call's resources
/// open for long — the request is already gone by the time this fires.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// This workspace's identity in LightTrack. Fixed — every event this process
/// emits is from `pumper`.
const PROJECT_ID: &str = "pumper";

/// Every metered call behind the chokepoint is an Anthropic chat completion
/// (the `claude` CLI); there is only one provider/operation pair to report.
const PROVIDER: &str = "anthropic";
const OPERATION: &str = "chat";

/// Resolved opt-in configuration. Constructing one at all means the operator
/// asked for this — the empty-string guard on `url` treats `LIGHTTRACK_URL=`
/// the same as unset rather than POSTing to a blank path.
#[derive(Debug, Clone)]
struct Config {
    url: String,
    key: Option<String>,
}

/// Reads the opt-in configuration from the process environment. `None` means
/// "do nothing" — the caller must not construct a client, a request, or any
/// other network-adjacent resource in that case.
fn config_from_env() -> Option<Config> {
    let url = std::env::var("LIGHTTRACK_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let key = std::env::var("LIGHTTRACK_KEY")
        .ok()
        .filter(|s| !s.trim().is_empty());
    Some(Config { url, key })
}

/// One shared client for the process, built lazily on the first opted-in
/// call. Avoids re-negotiating TLS/connection setup per research call; a
/// process that never configures LightTrack never builds one at all.
fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            // A client with no custom transport wiring failing to build would
            // mean something is deeply wrong with the process (e.g. TLS
            // backend init) — fall back to the bare default rather than
            // panic in a telemetry path.
            .unwrap_or_default()
    })
}

/// The wire body — see the contract in the module's callers. `cost_usd` and
/// `error` are omitted (not sent as `null`) when absent, per the contract:
/// token counts are not available on this transport, and a successful call
/// carries no error message.
#[derive(Debug, Serialize)]
struct Event {
    project_id: &'static str,
    provider: &'static str,
    model: String,
    operation: &'static str,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// One research call's outcome, as this module needs it. Deliberately not
/// [`crate::engine::ResearchOutput`] or [`crate::Error`] — the chokepoint
/// stays the one place that reads those shapes and decides what to forward.
pub(crate) struct ResearchEvent<'a> {
    /// The declared use-case key (`.ai/use-cases.json`), e.g.
    /// `"research.web_agent"` — sourced from `ResearchRequest::use_case`,
    /// which every call site sets explicitly. `None` is OMITTED from the
    /// event rather than sent as a placeholder: the sink already counts
    /// name-less events in its own unattributed bucket, so a magic string
    /// would turn a counted absence into a fake use case that reads as
    /// undeclared traffic. Absent is a fact; "unattributed" is a claim.
    pub use_case: Option<&'a str>,
    /// The model that actually ran. `None` only for shapes that never reach
    /// a real model call — a test double is the only case in practice, since
    /// [`crate::app::AppContext::research`] does not call this for cache
    /// hits or VCR replays in the first place.
    pub model: Option<&'a str>,
    pub latency_ms: u64,
    pub cost_usd: Option<f64>,
    /// `Some` for a failed call — its presence, not its text, decides
    /// `status`.
    pub error: Option<&'a str>,
}

/// Cap on the error text forwarded to LightTrack — a "short message" per the
/// contract, and this workspace's Claude errors can otherwise carry a
/// truncated CLI stderr dump.
const MAX_ERROR_CHARS: usize = 500;

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max_chars).collect();
        out.push('…');
        out
    }
}

/// Reports one research call to LightTrack, if configured. A no-op — no env
/// read even bothers to allocate — the instant `LIGHTTRACK_URL` is unset.
///
/// **Fire-and-forget.** Returns immediately; the actual POST (if any) runs on
/// a spawned task the caller never waits on and can never observe an error
/// from. Call it *after* the internal ledger write, from both the success and
/// failure branches of the chokepoint, so this can never be the reason a
/// budget-relevant spend goes unrecorded internally — it only ever adds a
/// second, best-effort record of what the first one already wrote.
pub(crate) fn emit_research(ev: ResearchEvent<'_>) {
    let Some(cfg) = config_from_env() else {
        return;
    };
    let name = ev.use_case.map(str::to_string);
    let model = ev.model.unwrap_or("unknown").to_string();
    let status = if ev.error.is_some() {
        "error"
    } else {
        "success"
    };
    let body = Event {
        project_id: PROJECT_ID,
        provider: PROVIDER,
        model,
        operation: OPERATION,
        status,
        name,
        latency_ms: ev.latency_ms,
        cost_usd: ev.cost_usd,
        error: ev.error.map(|e| truncate(e, MAX_ERROR_CHARS)),
    };
    let url = format!("{}/v1/events", cfg.url.trim_end_matches('/'));
    let key = cfg.key.clone();
    tokio::spawn(async move {
        let mut req = client().post(&url).json(&body);
        if let Some(key) = &key {
            req = req.bearer_auth(key);
        }
        match req.send().await {
            Ok(resp) if !resp.status().is_success() => {
                tracing::debug!(
                    status = %resp.status(),
                    "lighttrack event rejected — telemetry only, job is unaffected"
                );
            }
            Err(e) => {
                tracing::debug!(
                    "lighttrack event send failed: {e} — telemetry only, job is unaffected"
                );
            }
            Ok(_) => {}
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests in this module that mutate `LIGHTTRACK_URL`/`_KEY` —
    /// process env is global, and `cargo test` runs test fns on multiple
    /// threads within one binary by default.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        std::env::remove_var("LIGHTTRACK_URL");
        std::env::remove_var("LIGHTTRACK_KEY");
    }

    #[test]
    fn unset_url_means_unconfigured() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        assert!(config_from_env().is_none());
        std::env::set_var("LIGHTTRACK_URL", "");
        assert!(
            config_from_env().is_none(),
            "an empty LIGHTTRACK_URL must be treated as unset, not as a blank target"
        );
        clear_env();
    }

    #[test]
    fn a_configured_url_is_read_with_its_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var("LIGHTTRACK_URL", "https://lighttrack.example/api");
        std::env::set_var("LIGHTTRACK_KEY", "secret-key");
        let cfg = config_from_env().expect("configured");
        assert_eq!(cfg.url, "https://lighttrack.example/api");
        assert_eq!(cfg.key.as_deref(), Some("secret-key"));
        clear_env();
    }

    #[test]
    fn the_key_is_optional() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        std::env::set_var("LIGHTTRACK_URL", "https://lighttrack.example/api");
        let cfg = config_from_env().expect("configured");
        assert_eq!(cfg.key, None);
        clear_env();
    }

    #[test]
    fn error_text_is_truncated_not_sent_whole() {
        let long = "x".repeat(10_000);
        let short = truncate(&long, MAX_ERROR_CHARS);
        assert!(short.chars().count() <= MAX_ERROR_CHARS + 1);
    }

    #[test]
    fn success_event_omits_error_and_a_failure_carries_it() {
        let success = Event {
            project_id: PROJECT_ID,
            provider: PROVIDER,
            model: "claude-sonnet-5".into(),
            operation: OPERATION,
            status: "success",
            name: Some("research.web_agent".into()),
            latency_ms: 42,
            cost_usd: Some(0.12),
            error: None,
        };
        let json = serde_json::to_value(&success).unwrap();
        assert!(json.get("error").is_none(), "success must omit `error`");
        assert_eq!(json["name"], "research.web_agent");
        assert_eq!(json["status"], "success");

        // A call site that set no use case sends NO name. The sink counts
        // name-less events in its own unattributed bucket; a placeholder would
        // turn that counted absence into a fake use case reading as undeclared
        // traffic.
        let anonymous = Event {
            project_id: PROJECT_ID,
            provider: PROVIDER,
            model: "claude-sonnet-5".into(),
            operation: OPERATION,
            status: "success",
            name: None,
            latency_ms: 42,
            cost_usd: Some(0.12),
            error: None,
        };
        let json = serde_json::to_value(&anonymous).unwrap();
        assert!(
            json.get("name").is_none(),
            "an unset use case omits `name` rather than inventing one"
        );

        let failure = Event {
            error: Some("cli reported error: boom".into()),
            status: "error",
            cost_usd: None,
            ..success
        };
        let json = serde_json::to_value(&failure).unwrap();
        assert!(
            json.get("cost_usd").is_none(),
            "cost_usd must be omitted, never sent as 0, when unknown"
        );
        assert_eq!(json["error"], "cli reported error: boom");
        assert_eq!(json["status"], "error");
    }
}
