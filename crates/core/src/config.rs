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
            cfg.validate()
                .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
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

    /// Rejects semantically-broken key combinations that parse fine but produce a
    /// silently-dead service. Each rule guards an invariant that was previously
    /// only a doc-comment; the failure modes they prevent are invisible at the
    /// config layer and surface far away (reap storms, a worker that claims
    /// nothing, a penalty cap that never applies).
    ///
    /// `0` is a documented disable switch for `heartbeat_secs`, `stale_after_secs`
    /// and `priority_aging_coefficient_secs`, so each rule only binds when the
    /// features it relates are both actually on.
    pub fn validate(&self) -> Result<()> {
        let w = &self.worker;

        // The reaper decides "hung" by comparing a job's last heartbeat against
        // `stale_after_secs`. If beats are rarer than the threshold, every healthy
        // in-flight job looks hung: re-queued mid-run, restarted, reaped again,
        // until `max_attempts` is exhausted. No job ever completes.
        if w.heartbeat_secs > 0 && w.stale_after_secs > 0 && w.stale_after_secs <= w.heartbeat_secs {
            return Err(Error::Config(format!(
                "[worker] stale_after_secs ({}) must exceed heartbeat_secs ({}) — \
                 otherwise every healthy job is reaped as hung",
                w.stale_after_secs, w.heartbeat_secs
            )));
        }

        // Both the reaper and the timeout terminate a job. If the reaper fires
        // first it re-queues with attempt semantics, racing the timeout that was
        // meant to be the job's hard wall.
        if w.stale_after_secs > 0 && w.job_timeout_secs > 0 && w.job_timeout_secs <= w.stale_after_secs
        {
            return Err(Error::Config(format!(
                "[worker] job_timeout_secs ({}) must exceed stale_after_secs ({}) — \
                 otherwise the reaper races the job timeout",
                w.job_timeout_secs, w.stale_after_secs
            )));
        }

        // A worker with no concurrency claims nothing: the queue fills and drains
        // never, with no error anywhere.
        if w.concurrency == 0 {
            return Err(Error::Config(
                "[worker] concurrency must be > 0 — a worker with 0 slots claims no jobs".into(),
            ));
        }

        // A cap below the base means the very first penalty already exceeds it, so
        // the cap silently stops being a cap.
        let g = &self.governor;
        if g.enabled && g.penalty_base_secs > g.penalty_cap_secs {
            return Err(Error::Config(format!(
                "[governor] penalty_cap_secs ({}) must be >= penalty_base_secs ({}) — \
                 otherwise the cap never applies",
                g.penalty_cap_secs, g.penalty_base_secs
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Cross-origin allow-list for the HTTP API. Empty (the default) means CORS
    /// is OFF — same-origin only — so this unauthenticated, mutating API cannot be
    /// driven cross-origin by any site the operator happens to visit (a permissive
    /// allow-all is defeated by DNS-rebinding). Add specific origins (e.g.
    /// "http://localhost:5173") to opt a trusted local UI in.
    pub cors_allowed_origins: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { host: "127.0.0.1".into(), port: 8088, cors_allowed_origins: Vec::new() }
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
    /// Revision-history retention. When `> 0`, a janitor periodically prunes
    /// `record_revisions` older than this many days, always keeping the newest
    /// `revision_retention_keep_min` revisions per record so the diff chain stays
    /// usable. `0` (the default) disables pruning — a dataset's accrued history is
    /// the product's value, so deleting it must be an explicit opt-in.
    pub revision_retention_days: u64,
    /// Newest revisions always kept per record when pruning is enabled.
    pub revision_retention_keep_min: i64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: "data/pumper.db".into(),
            artifacts_dir: "data/artifacts".into(),
            revision_retention_days: 0, // off by default
            revision_retention_keep_min: 5,
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
    fn shipped_defaults_are_valid() {
        Config::default().validate().expect("shipped defaults must satisfy their own invariants");
    }

    #[test]
    fn stale_after_below_heartbeat_is_rejected() {
        let mut cfg = Config::default();
        cfg.worker.heartbeat_secs = 300;
        cfg.worker.stale_after_secs = 120;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("stale_after_secs"), "{err}");
        assert!(err.contains("heartbeat_secs"), "{err}");
    }

    #[test]
    fn stale_after_equal_to_heartbeat_is_rejected() {
        let mut cfg = Config::default();
        cfg.worker.heartbeat_secs = 120;
        cfg.worker.stale_after_secs = 120;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_disables_the_reaper_rather_than_failing_validation() {
        // `0` is the documented disable switch for both knobs; a disabled reaper
        // cannot mis-reap, so the ordering rule must not bind.
        let mut cfg = Config::default();
        cfg.worker.heartbeat_secs = 300;
        cfg.worker.stale_after_secs = 0;
        assert!(cfg.validate().is_ok());

        let mut cfg = Config::default();
        cfg.worker.heartbeat_secs = 0;
        cfg.worker.stale_after_secs = 5;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn job_timeout_below_stale_after_is_rejected() {
        let mut cfg = Config::default();
        cfg.worker.heartbeat_secs = 30;
        cfg.worker.stale_after_secs = 600;
        cfg.worker.job_timeout_secs = 300;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("job_timeout_secs"), "{err}");
    }

    #[test]
    fn zero_worker_concurrency_is_rejected() {
        let mut cfg = Config::default();
        cfg.worker.concurrency = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("concurrency"), "{err}");
    }

    #[test]
    fn governor_penalty_cap_below_base_is_rejected() {
        let mut cfg = Config::default();
        cfg.governor.penalty_base_secs = 60;
        cfg.governor.penalty_cap_secs = 30;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("penalty_cap_secs"), "{err}");
    }

    #[test]
    fn disabled_governor_skips_its_penalty_rule() {
        let mut cfg = Config::default();
        cfg.governor.enabled = false;
        cfg.governor.penalty_base_secs = 60;
        cfg.governor.penalty_cap_secs = 30;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn the_shipped_config_file_is_valid() {
        // Guards against the repo's own config.toml drifting into a state that
        // would refuse to boot.
        let raw = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.toml"))
            .expect("repo config.toml must be readable from the core crate");
        let mut cfg: Config = toml::from_str(&raw).expect("repo config.toml must parse");
        cfg.normalize();
        cfg.validate().expect("repo config.toml must satisfy the invariants");
    }

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
