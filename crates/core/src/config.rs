use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::{Error, Result};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub worker: WorkerConfig,
    pub storage: StorageConfig,
    pub http: HttpConfig,
    pub browser: BrowserConfig,
    pub claude: ClaudeConfig,
    pub fetcher: FetcherConfig,
    pub governor: GovernorConfig,
    pub cache: CacheConfig,
    pub plugins: PluginConfig,
    pub search: SearchConfig,
    pub triggers: TriggersConfig,
    pub webhooks: WebhooksConfig,
}

/// Global outbound-webhook subscriptions that aren't tied to a per-resource row
/// (watches/saved-searches). A single config-level firehose is the lightest fit
/// for a cross-app "any job failed" signal, which has no natural per-resource key.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct WebhooksConfig {
    /// If set, every job that fails *permanently* (attempts exhausted, including
    /// reaper-caused failures) POSTs a `job.failed` event here. Independent of a
    /// job's own `callback_url`, which already receives the terminal job JSON.
    pub failure_url: Option<String>,
    /// Optional HMAC-SHA256 signing secret for `failure_url` deliveries.
    pub failure_secret: Option<String>,
}

/// Reactive-pipeline trigger evaluation limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TriggersConfig {
    /// Max reactive chain depth; hops past this are skipped (warn-logged).
    pub max_depth: u32,
    /// Max keys inlined into `params._trigger.keys` (`count` stays exact).
    pub key_cap: usize,
}

impl Default for TriggersConfig {
    fn default() -> Self {
        Self { max_depth: 8, key_cap: 200 }
    }
}

impl Config {
    /// Loads from `$PUMPER_CONFIG` or `./config.toml`, falling back to defaults.
    pub fn load() -> Result<Config> {
        let path = PathBuf::from(
            std::env::var("PUMPER_CONFIG").unwrap_or_else(|_| "config.toml".to_string()),
        );
        if path.exists() {
            let raw = std::fs::read_to_string(&path)?;
            let mut cfg: Config =
                toml::from_str(&raw).map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
            cfg.normalize();
            Ok(cfg)
        } else {
            tracing::warn!("config file {} not found, using defaults", path.display());
            Ok(Config::default())
        }
    }

    /// Cross-section fixups applied after parsing. Currently: the browser proxy
    /// falls back to `[http] proxy` when unset, so a single `[http] proxy` knob
    /// routes both the HTTP and browser tiers.
    fn normalize(&mut self) {
        if self.browser.proxy.is_none() {
            self.browser.proxy = self.http.proxy.clone();
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: "127.0.0.1".into(), port: 8088 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerConfig {
    /// Max jobs running at once across all apps.
    pub concurrency: usize,
    /// Hard wall-clock limit per job.
    pub job_timeout_secs: u64,
    /// Fallback poll interval when the queue is idle.
    pub poll_interval_secs: u64,
    /// Per-app cap so one busy app can't starve others (0 = unlimited). Fairness
    /// for the multi-app queue.
    pub default_app_concurrency: usize,
    /// Per-app overrides of `default_app_concurrency`.
    pub app_concurrency: HashMap<String, usize>,
    /// How often the scheduler re-checks cron schedules for due firings.
    pub schedule_tick_secs: u64,
    /// Grace period on graceful shutdown: how long to wait for in-flight jobs to
    /// finish before re-queuing whatever is still running (mirrors
    /// `recover_stuck`) and exiting.
    pub shutdown_drain_secs: u64,
    /// How often (seconds) the worker stamps a liveness heartbeat on each running
    /// job. The reaper uses the heartbeat to tell a slow-but-alive job from a
    /// hung one, so a job that keeps `.await`-ing (however slow) is never reaped
    /// while a task wedged in a non-yielding loop stops heartbeating and is.
    /// `0` disables heartbeating.
    pub heartbeat_secs: u64,
    /// A running job whose last heartbeat is older than this (seconds) is treated
    /// as hung and re-queued by the reaper with failure semantics (attempts +
    /// backoff apply; an exhausted job fails permanently). Must exceed
    /// `heartbeat_secs`. `0` disables the reaper.
    pub stale_after_secs: u64,
    /// Priority-aging starvation guard: a queued job's *effective* priority rises
    /// by one level for every this-many seconds it has waited, so a low-priority
    /// job under a continuous high-priority stream eventually claims instead of
    /// starving forever. `0` disables aging — claim order is then exactly
    /// `priority DESC, created_at` (the historical behaviour).
    pub priority_aging_coefficient_secs: f64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            job_timeout_secs: 900,
            poll_interval_secs: 2,
            default_app_concurrency: 0,
            app_concurrency: HashMap::new(),
            schedule_tick_secs: 15,
            shutdown_drain_secs: 25,
            // Heartbeat every 30s; reap after 120s (4 missed beats) so a slow but
            // alive job is never mistaken for a hung one, while a wedged task is
            // recovered within a couple of minutes.
            heartbeat_secs: 30,
            stale_after_secs: 120,
            // +1 effective priority per 15 min waited: same-minute enqueues keep
            // their intended priority order, while a job starved behind a busy
            // higher-priority stream escalates past it within the hour rather
            // than never. Matches the job-timeout / schedule scale.
            priority_aging_coefficient_secs: 900.0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub database_path: PathBuf,
    pub artifacts_dir: PathBuf,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: "data/pumper.db".into(),
            artifacts_dir: "data/artifacts".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct HttpConfig {
    pub user_agent: String,
    pub timeout_secs: u64,
    pub retries: u32,
    /// Hard cap on a single response body (bytes). The engine streams the body in
    /// chunks and aborts with a typed error the moment the cumulative size would
    /// exceed this, so one huge/hostile URL can't balloon memory. Per-request
    /// `HttpRequest.max_body_bytes` overrides it. Default 16 MiB — comfortably
    /// above the largest real HTML/JSON pages we fetch (SEDIA clean-text and
    /// census blobs land in the low single-digit MiB), while still bounding a
    /// multi-GB response.
    pub max_body_bytes: u64,
    /// Max redirects a single request will follow before erroring. Was a
    /// hardcoded 10; now tunable for hosts with deep redirect chains.
    pub redirect_limit: usize,
    /// HTTP status codes that trigger a retry (with backoff). Was hardcoded
    /// `[429, 502, 503, 504]`; overridable so operators can add/remove codes
    /// (e.g. drop 502 for a flaky-but-not-retryable upstream).
    pub retryable_statuses: Vec<u16>,
    /// Outbound proxy for all HTTP requests: an `http`/`https`/`socks5` URL with
    /// optional `user:pass@` auth (e.g. `http://user:pass@proxy:8080`,
    /// `socks5://127.0.0.1:1080`). Applied at client-build time. Per-request
    /// `HttpRequest.proxy` overrides it via a small client pool. `None` = direct.
    pub proxy: Option<String>,
}

/// Default response-body cap: 16 MiB. See `HttpConfig::max_body_bytes`.
pub const DEFAULT_MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
                         (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
                .into(),
            timeout_secs: 30,
            retries: 3,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            redirect_limit: 10,
            retryable_statuses: vec![429, 502, 503, 504],
            proxy: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BrowserConfig {
    /// Explicit chrome/chromium binary; auto-detected when unset.
    pub chrome_executable: Option<PathBuf>,
    pub headless: bool,
    /// Persistent profile dir — cookies and logins survive across runs.
    pub user_data_dir: PathBuf,
    /// Settle time after navigation before the DOM is captured.
    pub default_wait_ms: u64,
    pub nav_timeout_secs: u64,
    /// Max renders (tabs) running at once against the shared Chrome instance.
    /// Each render opens a page/tab; without a cap N concurrent renders spawn N
    /// unbounded tabs. `0` = unlimited.
    pub max_concurrent_renders: usize,
    /// Block heavy subresources (images, fonts, media — never stylesheets) via
    /// CDP request interception so scraping renders download only what the DOM
    /// needs. Per-request `RenderRequest.load_all_resources` opts a single render
    /// back into loading everything. When `false`, interception is not enabled at
    /// all (zero overhead) and `load_all_resources` is moot.
    pub block_resources: bool,
    /// Relaunch the shared Chrome instance after this many renders to shed
    /// accumulated memory/leaked tabs. `0` disables periodic recycling (Chrome
    /// still relaunches on crash).
    pub recycle_after_renders: u64,
    /// Proxy for the headless browser, passed as Chrome's `--proxy-server` launch
    /// arg (`http`/`https`/`socks5` URL). When unset it falls back to
    /// `[http] proxy` at config load, so one knob usually serves both engines.
    /// Note: Chrome's `--proxy-server` does not accept `user:pass@` auth in the
    /// URL — an authenticated proxy prompts interactively, so browser-tier proxy
    /// auth is unsupported (a known gap).
    pub proxy: Option<String>,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            chrome_executable: None,
            headless: true,
            user_data_dir: "data/browser-profile".into(),
            default_wait_ms: 1000,
            nav_timeout_secs: 30,
            max_concurrent_renders: 4,
            block_resources: true,
            recycle_after_renders: 200,
            proxy: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClaudeConfig {
    /// Binary name or full path; npm shims are handled on Windows.
    pub binary: String,
    /// Fallback model when neither a role nor a request overrides it.
    pub model: Option<String>,
    /// Fallback reasoning effort: low | medium | high | xhigh | max.
    pub effort: Option<String>,
    pub timeout_secs: u64,
    /// Optional hard spend ceiling per run (`--max-budget-usd`).
    pub max_budget_usd: Option<f64>,
    /// Skip discovery of hooks/skills/plugins/CLAUDE.md for faster startup.
    pub bare: bool,
    /// Local power mode: run headless CLI with permission prompts disabled.
    pub skip_permissions: bool,
    pub allowed_tools: Vec<String>,
    /// Named presets apps select by name — e.g. "research" (Sonnet, normal
    /// reasoning) vs "compose" (Opus, xhigh reasoning). Any field a request
    /// sets explicitly overrides the role.
    pub roles: HashMap<String, ClaudeRole>,
    /// TTL for cached research outputs (identical prompts served from disk
    /// instead of re-spending). 0 disables the research cache.
    pub research_cache_ttl_secs: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ClaudeRole {
    pub model: Option<String>,
    pub effort: Option<String>,
    pub max_budget_usd: Option<f64>,
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        let mut roles = HashMap::new();
        roles.insert(
            "research".into(),
            ClaudeRole {
                model: Some("claude-sonnet-5".into()),
                effort: Some("high".into()),
                max_budget_usd: None,
            },
        );
        roles.insert(
            "compose".into(),
            ClaudeRole {
                model: Some("claude-opus-4-8".into()),
                effort: Some("xhigh".into()),
                max_budget_usd: None,
            },
        );
        Self {
            binary: "claude".into(),
            model: None,
            effort: None,
            timeout_secs: 600,
            max_budget_usd: None,
            bare: false,
            skip_permissions: true,
            allowed_tools: vec!["WebSearch".into(), "WebFetch".into()],
            roles,
            research_cache_ttl_secs: 24 * 3600,
        }
    }
}

/// Tiered-fetcher tuning that isn't per-request.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FetcherConfig {
    /// Default escalation threshold: a tier whose extracted text is shorter
    /// than this (in chars) is "thin" and escalates. A per-request
    /// `FetchRequest.min_content_chars` overrides it.
    pub min_content_chars: usize,
    /// Age (seconds) after which a host's learned tier memory decays: strikes
    /// older than this — and the browser pin they earned — lapse, so a host that
    /// failed a while ago gets a fresh crack at the cheap HTTP tier instead of
    /// staying pinned until a lucky win. `0` disables aging (the old
    /// pin-forever behaviour). Default: 7 days.
    pub host_memory_ttl_secs: u64,
    /// How often (seconds) the governor's learned per-host penalties are
    /// snapshotted to the DB so they survive a restart (restored on boot).
    /// `0` disables persistence (penalties stay purely in-memory). Default: 60s.
    pub host_penalty_persist_secs: u64,
    /// Root of the session vault: each named login profile lives in
    /// `<profiles_dir>/<name>/` — `cookies.json` (the HTTP tier's persistent
    /// cookie jar) and `browser/` (that profile's Chrome user-data-dir). Created
    /// on first use. Default: `data/profiles`.
    pub profiles_dir: PathBuf,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            min_content_chars: 250,
            host_memory_ttl_secs: 7 * 24 * 3600,
            host_penalty_persist_secs: 60,
            profiles_dir: "data/profiles".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct GovernorConfig {
    /// Per-domain politeness spacing. Disable to remove all rate limiting.
    pub enabled: bool,
    /// Default requests-per-second per host (0 = unlimited).
    pub default_rps: f64,
    /// Random extra delay (0..jitter_ms) added per request to de-sync bursts.
    pub jitter_ms: u64,
    /// Per-host overrides, keyed by hostname (e.g. "news.ycombinator.com").
    pub per_domain: HashMap<String, f64>,
    /// Learned-penalty base: the first 429/503 adds this much extra spacing,
    /// doubling on each subsequent hit.
    pub penalty_base_secs: u64,
    /// Hard cap on the learned penalty.
    pub penalty_cap_secs: u64,
    /// Floor (ms) below which a decaying penalty is dropped to zero.
    pub penalty_floor_ms: u64,
}

impl Default for GovernorConfig {
    fn default() -> Self {
        use crate::governor::{
            DEFAULT_PENALTY_BASE_SECS, DEFAULT_PENALTY_CAP_SECS, DEFAULT_PENALTY_FLOOR_MS,
        };
        Self {
            enabled: true,
            default_rps: 2.0,
            jitter_ms: 250,
            per_domain: HashMap::new(),
            penalty_base_secs: DEFAULT_PENALTY_BASE_SECS,
            penalty_cap_secs: DEFAULT_PENALTY_CAP_SECS,
            penalty_floor_ms: DEFAULT_PENALTY_FLOOR_MS,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    pub enabled: bool,
    /// Default time-to-live for cached responses.
    pub ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self { enabled: true, ttl_secs: 3600 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PluginConfig {
    pub enabled: bool,
    /// Directory scanned for `.wasm` plugin modules.
    pub dir: PathBuf,
    /// Per-call CPU instruction budget (fuel). Bounds runaway plugins.
    pub fuel: u64,
    /// Hard cap on a plugin instance's linear memory.
    pub max_memory_mb: usize,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: "data/plugins".into(),
            fuel: 200_000_000,
            max_memory_mb: 64,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    pub enabled: bool,
    /// Directory for the embedded Tantivy index.
    pub dir: PathBuf,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self { enabled: true, dir: "data/search-index".into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_defaults_match_prior_hardcoded_values() {
        let h = HttpConfig::default();
        assert_eq!(h.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(h.max_body_bytes, 16 * 1024 * 1024);
        assert_eq!(h.redirect_limit, 10);
        assert_eq!(h.retryable_statuses, vec![429, 502, 503, 504]);
        assert!(h.proxy.is_none());
    }

    #[test]
    fn http_proxy_and_caps_parse_from_toml() {
        let cfg: Config = toml::from_str(
            r#"
            [http]
            proxy = "socks5://127.0.0.1:1080"
            max_body_bytes = 1048576
            redirect_limit = 3
            retryable_statuses = [429, 503]
            "#,
        )
        .unwrap();
        assert_eq!(cfg.http.proxy.as_deref(), Some("socks5://127.0.0.1:1080"));
        assert_eq!(cfg.http.max_body_bytes, 1_048_576);
        assert_eq!(cfg.http.redirect_limit, 3);
        assert_eq!(cfg.http.retryable_statuses, vec![429, 503]);
    }

    #[test]
    fn browser_proxy_falls_back_to_http_proxy_on_normalize() {
        // Unset browser proxy inherits [http] proxy.
        let mut cfg: Config = toml::from_str(r#"
            [http]
            proxy = "http://gw:8080"
        "#)
        .unwrap();
        assert!(cfg.browser.proxy.is_none(), "not yet normalized");
        cfg.normalize();
        assert_eq!(cfg.browser.proxy.as_deref(), Some("http://gw:8080"));
    }

    #[test]
    fn fetcher_profiles_dir_defaults_and_overrides() {
        assert_eq!(FetcherConfig::default().profiles_dir, PathBuf::from("data/profiles"));
        let cfg: Config = toml::from_str(
            r#"
            [fetcher]
            profiles_dir = "/var/lib/pumper/profiles"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.fetcher.profiles_dir, PathBuf::from("/var/lib/pumper/profiles"));
        // Untouched sibling keys keep their defaults.
        assert_eq!(cfg.fetcher.min_content_chars, 250);
    }

    #[test]
    fn explicit_browser_proxy_wins_over_http_proxy() {
        let mut cfg: Config = toml::from_str(r#"
            [http]
            proxy = "http://gw:8080"
            [browser]
            proxy = "http://browser-gw:9090"
        "#)
        .unwrap();
        cfg.normalize();
        assert_eq!(cfg.browser.proxy.as_deref(), Some("http://browser-gw:9090"));
    }
}
