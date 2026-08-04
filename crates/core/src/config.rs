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
    pub derived: DerivedConfig,
    pub webhooks: WebhooksConfig,
    pub resilience: ResilienceConfig,
    pub datahub: DatahubConfig,
    pub archive: ArchiveConfig,
    pub remote: RemoteConfig,
    pub recipes: RecipesConfig,
    pub catalog: CatalogConfig,
    pub provisioner: ProvisionerConfig,
    pub contracts: ContractsConfig,
    pub ingress: IngressConfig,
    pub mcp: McpConfig,
    pub economics: EconomicsConfig,
    pub refresher: RefresherConfig,
}

/// Background cache refresher (M02 self-refreshing mirror): a scheduler-tick
/// piggyback that conditionally revalidates `http_cache` entries just before
/// their predicted next change (learned from the revalidation log's EWMA
/// inter-change intervals), so app-facing fetches find warm, provably-fresh
/// entries instead of paying live-network latency.
///
/// Strictly opportunistic and strictly polite: every request first takes
/// `Governor::try_acquire` — an idle-slot-only, non-blocking claim — so
/// background refreshes can never queue behind or delay live jobs, and per-tick
/// budgets bound total background traffic. Default OFF.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RefresherConfig {
    /// Master switch. `false` = no background revalidation at all (the
    /// revalidation log still accrues from demand-path revalidations).
    pub enabled: bool,
    /// How close (seconds) a key's predicted next change must be before the
    /// refresher considers it near-due.
    pub horizon_secs: u64,
    /// Max background revalidations per scheduler tick across all hosts.
    pub global_per_tick: usize,
    /// Max background revalidations per host per tick.
    pub per_host_per_tick: usize,
    /// Revalidation-log retention (days); older observations are pruned by the
    /// tick so the append-only log stays bounded.
    pub retention_days: u32,
}

impl Default for RefresherConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            horizon_secs: 300,
            global_per_tick: 10,
            per_host_per_tick: 2,
            retention_days: 30,
        }
    }
}

/// Information economics (M04): joining the cost ledger with per-job yield
/// (`job_yield`) into $/new-record telemetry and an ADVISORY budget/cadence
/// planner (`GET /economics`).
///
/// Advisory-only today: `enforce = false` is a stub for the deferred
/// enforcement mode, where the scheduler would read the planner's recommended
/// `budget_usd` for scheduled runs. Nothing reads it as `true` yet — flipping
/// it changes no behavior until that seam lands, and it ships default-OFF so
/// landing the seam can't silently start moving money.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct EconomicsConfig {
    /// DEFERRED enforcement seam (see above). Default false; currently inert
    /// beyond being surfaced in the /economics payload.
    pub enforce: bool,
    /// Per-app value weights for the planner: how much one fresh (new/changed)
    /// record from this app is worth relative to the fleet baseline of 1.0.
    /// Record counts are not value — a rare grant record may be worth 10k HN
    /// rows — and this is the human-settable dial that encodes that. Apps not
    /// listed weigh 1.0.
    pub weights: HashMap<String, f64>,
}

impl EconomicsConfig {
    /// The planner weight for one app (default 1.0; non-finite or negative
    /// configured values are treated as unset rather than poisoning the math).
    pub fn weight(&self, app: &str) -> f64 {
        match self.weights.get(app) {
            Some(w) if w.is_finite() && *w >= 0.0 => *w,
            _ => 1.0,
        }
    }
}

/// MCP server (`/mcp`): the registry, datasets, and search exposed as native
/// agent tools over the Model Context Protocol's streamable-HTTP transport.
///
/// Default-OFF, and read-mostly even when on: the enqueue tool — the one that
/// can spend money and load targets — has its own switch plus a hard per-call
/// budget ceiling, so a connected agent can browse everything but actuate
/// nothing until the operator opts in twice.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct McpConfig {
    /// Master switch. `false` = `/mcp` is not mounted at all.
    pub enabled: bool,
    /// Whether the `enqueue_job` tool is offered. `false` (default) = the MCP
    /// surface is read-only: list/query/search but no job creation.
    pub allow_enqueue: bool,
    /// Hard ceiling (USD) on the `budget_usd` any MCP enqueue may carry; a
    /// larger requested budget is clamped, an absent one defaults to this.
    /// `0` = MCP-enqueued jobs run with a zero budget (free tiers only).
    pub max_job_budget_usd: f64,
    /// Cap (seconds) on the `wait_job` tool's `timeout_secs`: a larger request
    /// is clamped, an absent one defaults to this. Bounds how long one MCP
    /// tool call may hold its HTTP response open awaiting a terminal status.
    pub wait_job_max_secs: u64,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_enqueue: false,
            max_job_budget_usd: 1.0,
            wait_job_max_secs: 60,
        }
    }
}

/// Inbound event ingress (`POST /ingest/{id}`): HMAC-verified external webhooks
/// stamped onto the event bus as `external` events that triggers can match.
///
/// This is the FIRST write surface designed for non-localhost callers, so it is
/// disabled by default and every source carries its own signing secret. When
/// disabled the routes still mount but every ingest returns 409; the CRUD
/// surface still works, so sources can be staged before the flip.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IngressConfig {
    /// Master switch. `false` = every `POST /ingest/{id}` is refused (409).
    pub enabled: bool,
    /// Hard cap on an inbound event body (bytes). Inbound payloads are event
    /// notifications, not documents; anything bigger should arrive as a fetch
    /// by the triggered job, not as the trigger itself.
    pub max_body_bytes: usize,
    /// Per-source token-bucket rate limit (events/minute; also the burst size).
    pub rate_limit_per_min: u32,
    /// Max accepted clock skew (seconds) for timestamped signatures
    /// (`x-pumper-timestamp` present). Bounds replay of a captured request.
    pub max_skew_secs: i64,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_body_bytes: 256 * 1024,
            rate_limit_per_min: 60,
            max_skew_secs: 300,
        }
    }
}

/// Catalog GitOps reconciler (`catalog/data-sources.toml` as desired state for
/// the schedules table). The reconciler always *plans* at boot and logs drift
/// loudly; this only controls whether it is allowed to APPLY that plan on its
/// own. Manual applies go through `POST /catalog/reconcile` either way.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct CatalogConfig {
    /// Apply the boot-time reconcile plan automatically (create/update/disable
    /// catalog-managed schedules; orphans are never auto-touched). Default OFF:
    /// a bad TOML edit should be a loud log, not a silent mass-disable.
    pub auto_reconcile: bool,
}

/// The `provisioner` app's proposal lifecycle (`provisioner/proposals`):
/// `GET /provisioner/proposals` marks a still-`planned` proposal `expired`
/// once it has sat unreviewed past this window.
///
/// A lazily-computed field on the LIST read path, not a maintenance tick that
/// mutates the record: expiry is a judgement about staleness at read time, and
/// nothing downstream needs it to be an eagerly stamped fact — the same reason
/// `[retention] preview` and `[resilience] enforcement_preview` are read-only
/// dry runs rather than background sweeps. See `docs/features/apps.md`
/// "provisioner: proposal lifecycle".
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ProvisionerConfig {
    /// Age (seconds) past which a `planned` proposal is reported `expired`.
    /// `0` opts out (nothing ever expires). Default 30 days.
    pub proposal_max_age_secs: u64,
}

impl Default for ProvisionerConfig {
    fn default() -> Self {
        Self {
            proposal_max_age_secs: 30 * 24 * 3600,
        }
    }
}

/// Declared data contracts (`[source.contract]` blocks in the catalog).
/// Contracts are always *evaluated* at publish time for sources that declare
/// one — verdicts are recorded and surfaced on `/catalog/health` and
/// `/sources` either way; this only controls whether a violated contract also
/// *gates* (suppresses the dataset's pushes/triggers, like
/// `[resilience] enforce` does for inferred health).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ContractsConfig {
    /// Gate pushes on contract violations (verdict `block` instead of `warn`).
    /// Default OFF — soak mode, exactly how resilience started: watch the
    /// verdicts on `/catalog/health` until the contracts prove non-flaky.
    pub enforce: bool,
}

/// Tier-zero archive engine (Wayback Machine CDX, v1). When enabled, the server
/// wires an archive engine into the tiered fetcher; a fetch that sets
/// `archive_max_age` then tries a stored snapshot BEFORE touching the live site
/// (zero load on the target, zero ban risk). Disabled by default — when off the
/// engine is never constructed and `archive_max_age` is inert.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ArchiveConfig {
    /// Master switch. `false` = the archive engine is never built.
    pub enabled: bool,
    /// Base URL of the Wayback deployment: both the CDX index
    /// (`<base>/cdx/search/cdx`) and raw snapshot bodies
    /// (`<base>/web/<ts>id_/<url>`) are served under it. Overridable for a
    /// self-hosted pywb instance.
    pub base_url: String,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "https://web.archive.org".into(),
        }
    }
}

/// Distributed fetch fabric (M17 v1). One config drives both sides of the seam:
///
/// - **serving**: when `enabled` and `secret` is set, this node's
///   `POST /fetch-proxy` route accepts a serialized `HttpRequest` from a peer
///   (authenticated by the shared secret) and runs it through the LOCAL fetch
///   stack — HTTP engine, politeness governor, cache, body caps — returning the
///   response as JSON.
/// - **dispatching**: when `enabled` and `nodes` is non-empty, the tiered
///   fetcher's live-HTTP tier routes through the remote engine: round-robin
///   over `nodes`, falling back to the local engine on any node error.
///
/// Default OFF with no nodes: nothing is served, nothing is dispatched, and the
/// fetcher behaves exactly as before. Cluster-wide governor state is a later
/// slice (M01's host-weather bundle) — each node governs targets independently.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RemoteConfig {
    /// Master switch for BOTH sides. `false` = `/fetch-proxy` rejects every
    /// call and the remote engine is never built.
    pub enabled: bool,
    /// Peer base URLs to dispatch fetches to (e.g. `"http://10.0.0.2:8088"`).
    /// Empty = serve-only: this node accepts proxied fetches but never sends any.
    pub nodes: Vec<String>,
    /// Shared secret, sent/required in the `x-pumper-remote-secret` header.
    /// MUST be non-empty when `enabled` — an unauthenticated `/fetch-proxy`
    /// would be an open proxy.
    pub secret: String,
    /// Per proxy call timeout (seconds), end to end, on the dispatching side.
    pub timeout_secs: u64,
    /// Cap (bytes) on a proxied response body: the serving side clamps the
    /// inner request's `max_body_bytes` to this, and the dispatching side sizes
    /// its transport cap from it.
    pub max_body_bytes: u64,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            nodes: Vec::new(),
            secret: String::new(),
            timeout_secs: 60,
            // Matches `[http] max_body_bytes`' 16 MiB default.
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }
}

/// Learned API-recipe replay (M05 step 4): when a *validated* recipe (see
/// `GET /recipes` and [`crate::recipes`]) exists for a fetch's host, the tiered
/// fetcher replays the discovered JSON API ahead of the archive and live tiers,
/// returning structured JSON with a `TierTrace` entry of tier `api_recipe`.
/// Default OFF: recipes stay pure discovery data unless a fetch opts in
/// (`FetchRequest.use_recipes`) or this section flips `enabled = true`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RecipesConfig {
    /// Master switch: consider recipes on every fetch. A single fetch can opt
    /// in without it via `FetchRequest.use_recipes = true`.
    pub enabled: bool,
    /// When ON, an *unvalidated* recipe is also tried opportunistically, and a
    /// successful replay whose payload still overlaps the recipe's expected
    /// field paths marks it validated. OFF (default) = only validated recipes
    /// are ever replayed; validation stays a manual/operator decision.
    pub auto_validate: bool,
    /// Consecutive failed/thin replays after which a validated recipe is
    /// un-validated (back to opportunistic-only). Must be >= 1.
    pub max_failures: u32,
}

impl Default for RecipesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_validate: false,
            max_failures: 3,
        }
    }
}

/// Extraction-health detection: how a source's runs are judged against its own
/// past, and whether that judgement is allowed to gate anything.
///
/// The two switches are deliberately separate. `enabled = false` is a complete
/// no-op. `enforce = false` — the shipping default — computes and stores every
/// verdict while gating nothing: no trust stamps, no suppressed pushes, no
/// downgraded syncs. That is the soak mode, and enforcement is meant to be
/// turned on only after `source_runs` shows the false-positive rate is
/// acceptable on real data. On an unattended box a false quarantine that
/// silently stops a working pipeline is worse than a detection a week late.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ResilienceConfig {
    /// Master switch. `false` = no detection, no writes, no reads.
    pub enabled: bool,
    /// Whether verdicts gate anything (trust stamping, push suppression,
    /// `sync_many` downgrade, quarantine datasets, search-index skip).
    pub enforce: bool,
    /// Documents a run needs before distributional tests apply. Below it only
    /// the assumption-free total-collapse rule can fire.
    pub min_cohort_docs: u32,
    /// Healthy runs pooled into the rolling baseline.
    pub window_runs: u32,
    /// Degradation score at which a run counts as *tripped*.
    pub degrade_score: f64,
    /// Score at which a tripped run also counts as *severe*, accelerating
    /// `degraded` → `quarantined`.
    pub quarantine_score: f64,
    /// Below this fraction of successful fetches a run is `inconclusive`: you
    /// cannot judge an extractor on documents you did not receive.
    pub fetch_ok_floor: f64,
    /// Robust-z flag threshold (Iglewicz–Hoaglin) for distributional signals.
    pub mad_z: f64,
    /// Normalized fingerprint drift at or below which an input is "unchanged".
    pub drift_low: f64,
    /// Normalized fingerprint drift at or above which it has "moved".
    pub drift_high: f64,
    /// Days before mined invariants are re-derived from live records.
    pub invariant_refresh_days: i64,
    /// Records an invariant must hold over before it is trusted.
    pub invariant_min_support: u32,
    /// Fraction of sampled records an invariant must hold on.
    pub invariant_min_confidence: f64,
    /// Fraction of a cohort that must break an invariant to count as violated.
    pub invariant_violation_ratio: f64,
    /// Runs of per-field sketches kept by the retention janitor.
    pub sketch_retention_runs: u32,
    /// Consecutive **judged and clean** runs needed to climb one rung back up:
    /// `quarantined` → `probation` → `healthy`. Counted since the last state
    /// transition, so each rung costs the full streak, and a single tripped run
    /// during `probation` drops straight back to `quarantined`.
    ///
    /// Runs the detector could not judge (`inconclusive`, `content_empty`,
    /// `below_cohort`) do not count — a source cannot heal on evidence nobody
    /// looked at. `0` would let a source un-quarantine itself on the first run
    /// that happened not to trip, so it is rejected.
    pub recovery_runs: u32,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Soak first. See the struct doc.
            enforce: false,
            min_cohort_docs: 30,
            window_runs: 20,
            degrade_score: 0.6,
            quarantine_score: 0.85,
            fetch_ok_floor: 0.7,
            mad_z: 3.5,
            // A near-duplicate document sits around 0.05 normalized Hamming
            // distance and two unrelated ones around 0.5, so 0.08/0.20 brackets
            // "the same page" against "meaningfully different".
            drift_low: 0.08,
            drift_high: 0.20,
            invariant_refresh_days: 14,
            invariant_min_support: 500,
            invariant_min_confidence: 0.99,
            invariant_violation_ratio: 0.2,
            sketch_retention_runs: 60,
            // Three clean judged runs per rung, i.e. six to walk all the way back
            // from quarantine. Symmetric with the descent (two of the last three
            // to degrade, three consecutive to quarantine): recovery should cost
            // at least what the fall did.
            recovery_runs: 3,
        }
    }
}

/// DataHub metadata emission. Pumper pushes *metadata only* (dataset entities,
/// schema, lineage, per-run operation events) to a DataHub instance over its
/// plain OpenAPI surface — record data never leaves the local store. Disabled
/// by default; when disabled the emitter is never constructed and job execution
/// is byte-for-byte unchanged.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DatahubConfig {
    /// Master switch. `POST /datahub/sync` returns 409 while disabled.
    pub enabled: bool,
    /// GMS base URL, e.g. `http://localhost:8080` (quickstart) or
    /// `https://<tenant>.acryl.io/gms` (DataHub Cloud).
    pub gms_url: String,
    /// Personal access token. Falls back to the `DATAHUB_TOKEN` env var so the
    /// secret can live in `.env` instead of the checked-in config.
    pub token: Option<String>,
    /// DataHub fabric/environment segment of every dataset URN (`PROD`/`DEV`/…).
    pub env: String,
    /// Emit `schemaMetadata` inferred from each dataset's newest record.
    pub emit_schema: bool,
    /// Emit `datasetProfile` (row counts) on sync and job completion.
    pub emit_profile: bool,
    /// Emit pipeline topology (M25): schedules as `dataFlow` entities, job runs
    /// as `dataJob` entities with input/output dataset edges, triggers as
    /// dataset-level lineage edges, and column-level `fineGrainedLineage` where
    /// a declarative RuleSet makes field provenance mechanical.
    pub emit_flows: bool,
    /// Governance pull loop (M26): periodically read DataHub state for Pumper
    /// URNs and act on it — deprecation disables catalog-managed schedules,
    /// a `cost:pause` tag zeroes the Claude-tier budget for that app's jobs,
    /// failing assertions enqueue an immediate sync. Default OFF: remote
    /// state driving an unattended box is opt-in, every action is loud-logged
    /// and surfaced on `GET /datahub/status`, and all actions are reversible.
    pub govern: bool,
    /// Seconds between governance polls (min 30; only meaningful with `govern`).
    pub govern_interval_secs: u64,
    /// How long the `cost:pause` set may survive **without a successful poll**
    /// before it expires loudly (warn + audit row + event). Default 900s = 3×
    /// the default interval.
    ///
    /// The failure this bounds: the poll aborts on the first read error, so
    /// during a DataHub outage the paused set can never be recomputed — an app
    /// paused just before the outage stayed budget-$0 for as long as the outage
    /// lasted, with no way to un-read the tag. Governance that has gone blind
    /// must stop enforcing what it can no longer observe. `0` disables expiry
    /// (pauses freeze until the next successful poll — the old behavior).
    pub govern_pause_max_stale_secs: u64,
}

impl Default for DatahubConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gms_url: "http://localhost:8080".into(),
            token: None,
            env: "PROD".into(),
            emit_schema: true,
            emit_profile: true,
            emit_flows: true,
            govern: false,
            govern_interval_secs: 300,
            govern_pause_max_stale_secs: 900,
        }
    }
}

impl DatahubConfig {
    /// Config token, else `DATAHUB_TOKEN` from the environment.
    pub fn resolve_token(&self) -> Option<String> {
        self.token.clone().filter(|t| !t.is_empty()).or_else(|| {
            std::env::var("DATAHUB_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
        })
    }
}

/// Global outbound-webhook subscriptions that aren't tied to a per-resource row
/// (watches/saved-searches). A single config-level firehose is the lightest fit
/// for a cross-app "any job failed" signal, which has no natural per-resource key.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WebhooksConfig {
    /// If set, every job that fails *permanently* (attempts exhausted, including
    /// reaper-caused failures) POSTs a `job.failed` event here. Independent of a
    /// job's own `callback_url`, which already receives the terminal job JSON.
    pub failure_url: Option<String>,
    /// Optional HMAC-SHA256 signing secret for `failure_url` deliveries.
    pub failure_secret: Option<String>,
    /// Auto-drain the dead-letter queue: a background task (piggybacked on the
    /// scheduler tick) re-sends `failed` deliveries with exponential backoff, so a
    /// brief receiver outage no longer means permanent silent event loss. `false`
    /// reverts to manual-only replay. Default: true.
    pub auto_retry: bool,
}

impl Default for WebhooksConfig {
    fn default() -> Self {
        Self {
            failure_url: None,
            failure_secret: None,
            auto_retry: true,
        }
    }
}

/// Derived-dataset (M11) recompute limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DerivedConfig {
    /// Max derived-spec chain depth (a derived dataset feeding another spec);
    /// hops past this are skipped (warn-logged). Cycles are rejected at
    /// spec-create time; this cap bounds the acyclic chains.
    pub max_depth: u32,
    /// Max source rows one aggregate group re-scans during incremental
    /// maintenance (M11 v2). A group past the bound gets a `stale: true`
    /// derived row instead of a wrong number; `POST /derived/{id}/backfill`
    /// computes it exactly and clears the flag.
    pub max_group_scan: i64,
}

impl Default for DerivedConfig {
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_group_scan: 10_000,
        }
    }
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
        Self {
            max_depth: 8,
            key_cap: 200,
        }
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
            let mut cfg: Config = toml::from_str(&raw)
                .map_err(|e| Error::Config(format!("{}: {e}", path.display())))?;
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
        if w.heartbeat_secs > 0 && w.stale_after_secs > 0 && w.stale_after_secs <= w.heartbeat_secs
        {
            return Err(Error::Config(format!(
                "[worker] stale_after_secs ({}) must exceed heartbeat_secs ({}) — \
                 otherwise every healthy job is reaped as hung",
                w.stale_after_secs, w.heartbeat_secs
            )));
        }

        // Both the reaper and the timeout terminate a job. If the reaper fires
        // first it re-queues with attempt semantics, racing the timeout that was
        // meant to be the job's hard wall.
        if w.stale_after_secs > 0
            && w.job_timeout_secs > 0
            && w.job_timeout_secs <= w.stale_after_secs
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

        // Extraction health. Each rule guards a combination that parses fine and
        // then produces either a detector that can never fire or one that fires
        // on everything — both invisible at the config layer.
        let r = &self.resilience;
        if r.enabled {
            // With the quarantine threshold at or below the degrade threshold,
            // every tripped run is also severe: the whole hysteresis ladder
            // collapses and one bad run walks a source to quarantine.
            if r.degrade_score >= r.quarantine_score {
                return Err(Error::Config(format!(
                    "[resilience] degrade_score ({}) must be < quarantine_score ({}) — \
                     otherwise every tripped run is severe and the hysteresis ladder collapses",
                    r.degrade_score, r.quarantine_score
                )));
            }
            // A floor outside (0,1] either gates nothing (<=0) or gates every run
            // (>1), and the second silently disables detection entirely.
            if !(r.fetch_ok_floor > 0.0 && r.fetch_ok_floor <= 1.0) {
                return Err(Error::Config(format!(
                    "[resilience] fetch_ok_floor ({}) must be in (0, 1] — \
                     above 1 every run is inconclusive and nothing is ever judged",
                    r.fetch_ok_floor
                )));
            }
            // Below 5 documents no proportion test has the power to separate a
            // broken run from noise, so the cohort floor would be decorative.
            if r.min_cohort_docs < 5 {
                return Err(Error::Config(format!(
                    "[resilience] min_cohort_docs ({}) must be >= 5 — \
                     no rate test can separate signal from noise below that",
                    r.min_cohort_docs
                )));
            }
            // An empty baseline window means every run is judged against nothing.
            if r.window_runs == 0 {
                return Err(Error::Config(
                    "[resilience] window_runs must be > 0 — a zero-run baseline \
                     leaves every run with nothing to be compared against"
                        .into(),
                ));
            }
            // Drift bands that cross make the divergence table unreadable: a
            // drift could be simultaneously "unchanged" and "moved".
            if r.drift_low >= r.drift_high {
                return Err(Error::Config(format!(
                    "[resilience] drift_low ({}) must be < drift_high ({})",
                    r.drift_low, r.drift_high
                )));
            }
            // A zero-run recovery streak means a quarantined source releases
            // itself on the first run that merely failed to trip — which is the
            // exact behaviour quarantine exists to prevent.
            if r.recovery_runs == 0 {
                return Err(Error::Config(
                    "[resilience] recovery_runs must be > 0 — a source would otherwise \
                     un-quarantine itself on the first run that happened not to trip"
                        .into(),
                ));
            }
            // Sketches are the baseline substrate; keeping fewer than the window
            // means the baseline read silently sees a short window.
            if r.sketch_retention_runs < r.window_runs {
                return Err(Error::Config(format!(
                    "[resilience] sketch_retention_runs ({}) must be >= window_runs ({}) — \
                     otherwise retention prunes the baseline the detector reads",
                    r.sketch_retention_runs, r.window_runs
                )));
            }
        }

        // The archive engine builds every CDX/snapshot URL off base_url; a value
        // that isn't an absolute http(s) URL yields requests that fail far from
        // here with an unhelpful reqwest parse error on every single fetch.
        let a = &self.archive;
        if a.enabled
            && !matches!(
                url::Url::parse(&a.base_url).as_ref().map(|u| u.scheme()),
                Ok("http") | Ok("https")
            )
        {
            return Err(Error::Config(format!(
                "[archive] base_url ('{}') must be an absolute http(s) URL",
                a.base_url
            )));
        }

        // Remote fetch fabric. An enabled fabric without a secret is an OPEN
        // PROXY (any caller could fetch arbitrary URLs from this node's IP);
        // a node URL that isn't absolute http(s) fails far away on every
        // dispatch; zero caps parse fine and then reject/starve every call.
        let rm = &self.remote;
        if rm.enabled {
            if rm.secret.trim().is_empty() {
                return Err(Error::Config(
                    "[remote] secret must be set when the fetch fabric is enabled — \
                     an unauthenticated /fetch-proxy is an open proxy"
                        .into(),
                ));
            }
            for node in &rm.nodes {
                if !matches!(
                    url::Url::parse(node).as_ref().map(|u| u.scheme()),
                    Ok("http") | Ok("https")
                ) {
                    return Err(Error::Config(format!(
                        "[remote] node ('{node}') must be an absolute http(s) URL"
                    )));
                }
            }
            if rm.timeout_secs == 0 || rm.max_body_bytes == 0 {
                return Err(Error::Config(format!(
                    "[remote] timeout_secs ({}) and max_body_bytes ({}) must be > 0 \
                     when the fetch fabric is enabled",
                    rm.timeout_secs, rm.max_body_bytes
                )));
            }
        }

        // Retention. Every knob is off by default, so these rules only bind on a
        // deployment that deliberately turned deletion on — and each one catches
        // a value that parses fine and then deletes more, or less, than the
        // operator can see from the file.
        let st = &self.storage;
        if st.revision_retention_days > 0 && st.revision_retention_keep_min < 1 {
            return Err(Error::Config(format!(
                "[storage] revision_retention_keep_min ({}) must be >= 1 when \
                 revision_retention_days is set — keeping zero revisions per record deletes the \
                 whole history of every record past the cutoff, not just its tail",
                st.revision_retention_keep_min
            )));
        }
        if st.artifact_retention_include_cassettes && st.artifact_retention_days == 0 {
            return Err(Error::Config(
                "[storage] artifact_retention_include_cassettes is set while \
                 artifact_retention_days is 0 — the flag reads as 'cassettes are unprotected' \
                 but nothing reclaims artifacts at all, so it does nothing"
                    .into(),
            ));
        }
        // Artifact retention keeps a body alive while a REPLAYABLE revision points
        // at it, so the pin only lasts as long as that revision does. A revision
        // window shorter than the artifact window means history is pruned first
        // and the body loses its pin early — the shorter of the two silently
        // becomes the real artifact window, which is not what the file says.
        if st.artifact_retention_days > 0
            && st.revision_retention_days > 0
            && st.revision_retention_days < st.artifact_retention_days
        {
            return Err(Error::Config(format!(
                "[storage] revision_retention_days ({}) must be >= artifact_retention_days ({}) \
                 — artifact pins are held by replayable revisions, so pruning history first \
                 un-pins bodies before their own window is up",
                st.revision_retention_days, st.artifact_retention_days
            )));
        }

        // A zero strike threshold would un-validate a recipe on its first
        // failure ever recorded — before any success could reset the counter —
        // making auto-validation a one-shot coin flip. Catch it at boot.
        if self.recipes.max_failures == 0 {
            return Err(Error::Config(
                "[recipes] max_failures must be >= 1 — a validated recipe needs at \
                 least one consecutive failure before it is un-validated"
                    .into(),
            ));
        }

        // Ingress guards only bind when the surface is actually reachable. A
        // zero body cap or zero rate limit parses fine and then rejects every
        // single inbound event — an enabled surface that silently drops 100% of
        // traffic is a misconfiguration, not a policy.
        let i = &self.ingress;
        if i.enabled {
            if i.max_body_bytes == 0 {
                return Err(Error::Config(
                    "[ingress] max_body_bytes must be > 0 when ingress is enabled — \
                     a zero cap rejects every inbound event"
                        .into(),
                ));
            }
            if i.rate_limit_per_min == 0 {
                return Err(Error::Config(
                    "[ingress] rate_limit_per_min must be > 0 when ingress is enabled — \
                     a zero bucket admits no events"
                        .into(),
                ));
            }
        }

        // MCP: a negative budget ceiling parses fine and then makes every
        // clamped enqueue budget negative — which `filter(|b| *b > 0.0)`-style
        // guards downstream silently turn into "unlimited". Reject at the door.
        if self.mcp.enabled && !(self.mcp.max_job_budget_usd >= 0.0) {
            return Err(Error::Config(format!(
                "[mcp] max_job_budget_usd ({}) must be >= 0 when mcp is enabled",
                self.mcp.max_job_budget_usd
            )));
        }

        // Refresher guards only bind when the loop actually runs. Zero budgets
        // or a zero horizon parse fine and then produce a tick that scans the
        // freshness model every interval while refreshing nothing — an enabled
        // feature that silently does no work.
        let rf = &self.refresher;
        if rf.enabled {
            if rf.global_per_tick == 0 || rf.per_host_per_tick == 0 {
                return Err(Error::Config(format!(
                    "[refresher] global_per_tick ({}) and per_host_per_tick ({}) must be > 0 \
                     when the refresher is enabled — zero budgets refresh nothing",
                    rf.global_per_tick, rf.per_host_per_tick
                )));
            }
            if rf.horizon_secs == 0 {
                return Err(Error::Config(
                    "[refresher] horizon_secs must be > 0 when the refresher is enabled — \
                     a zero window never finds a near-due key"
                        .into(),
                ));
            }
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
        Self {
            host: "127.0.0.1".into(),
            port: 8088,
            cors_allowed_origins: Vec::new(),
        }
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
    /// Poisoned-checkpoint escape: after this many attempts have started from a
    /// job's durable checkpoint without any of them completing, the checkpoint
    /// is discarded and the next attempt starts fresh — restored state is
    /// advisory, and a blob that reliably kills its consumer must not retry
    /// forever. `0` disables restores entirely (checkpoints still persist).
    pub max_resume_failures: i64,
    /// How many finished jobs may run their post-completion fan-out (search
    /// indexing, watch webhooks, dataset triggers, saved-search alerts +
    /// materialization, the terminal event and result webhook) concurrently,
    /// **off** the worker's scrape permits. That work is derived and outbound;
    /// running it inline meant a slow index or a large materialization burned
    /// one of the `concurrency` slots for its whole duration. `0` runs it
    /// inline on the job's own permit (the historical behaviour).
    pub fanout_concurrency: usize,
    /// Backlog ceiling for the fan-out pool. At the ceiling a job's fan-out
    /// runs inline on its worker permit instead — slower, but never dropped: a
    /// dropped fan-out is a webhook that silently never arrives.
    pub fanout_max_queued: usize,
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
            // Three strikes: enough to ride out an unlucky crash/reap streak,
            // few enough that a genuinely poisoned blob stops burning attempts.
            max_resume_failures: 3,
            // Matches the default scrape concurrency: fan-out is roughly one
            // unit per finished job, so a pool the size of the queue keeps up
            // without becoming a second unbounded execution surface.
            fanout_concurrency: 4,
            // ~64 jobs of backlog before a job pays for its own fan-out inline.
            // Deep enough that a burst of quick jobs never blocks on a slow
            // index; shallow enough that the backlog can't grow into memory
            // pressure unnoticed.
            fanout_max_queued: 64,
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
    /// Artifact-tree retention (days). When `> 0`, the janitor reclaims archived
    /// bodies under `artifacts_dir` older than this — **except** any body a
    /// *replayable* revision still points at, which is pinned regardless of age
    /// (`crate::retention`). `0` (the default) disables it: a body is the
    /// evidence behind a record, and `POST /provenance/.../rederive`, the
    /// crawl→extract seam and VCR replay all read the tree, so reclaiming it is
    /// an explicit opt-in exactly like revision history.
    pub artifact_retention_days: u64,
    /// Let artifact retention reclaim VCR cassettes too. Off by default:
    /// `cassette.ndjson` is the entire substrate of `Vcr::Replay`, nothing
    /// records which job will be replayed next, and a missing cassette is a hard
    /// `ReplayMiss` rather than a degraded result.
    pub artifact_retention_include_cassettes: bool,
    /// Days of `cost_events` kept. Events of jobs that are still queued/running
    /// are never pruned (they back the budget ceiling). `0` = off.
    pub cost_event_retention_days: u64,
    /// Days of `delivered` webhook deliveries kept. `pending`/`failed` rows are
    /// the live retry queue and are never pruned. `0` = off.
    pub webhook_delivery_retention_days: u64,
    /// Days of `dead` (retry-exhausted) webhook deliveries kept — the DLQ tail.
    /// Separate from the delivered knob because a dead letter is failure
    /// evidence. `0` = off.
    pub webhook_dead_letter_retention_days: u64,
    /// Days of per-job yield telemetry kept (backs `GET /economics`). `0` = off.
    pub job_yield_retention_days: u64,
    /// Days of saved-search `seen` doc-ids kept. **Turning this on can re-alert:**
    /// a pruned row makes an already-notified document look new again, so a
    /// still-matching doc fires its webhook a second time. `0` = off.
    pub saved_search_seen_retention_days: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            database_path: "data/pumper.db".into(),
            artifacts_dir: "data/artifacts".into(),
            revision_retention_days: 0, // off by default
            revision_retention_keep_min: 5,
            // Every retention knob below is off by default for the same reason:
            // each one deletes something a reader still might want, and this
            // service is local-first with no second operator to ask.
            artifact_retention_days: 0,
            artifact_retention_include_cassettes: false,
            cost_event_retention_days: 0,
            webhook_delivery_retention_days: 0,
            webhook_dead_letter_retention_days: 0,
            job_yield_retention_days: 0,
            saved_search_seen_retention_days: 0,
        }
    }
}

// Retention plumbing needs the SQLite-backed `Storage` types, so it rides the
// same feature gate they do.
#[cfg(feature = "storage")]
impl StorageConfig {
    /// The ledger knobs in the shape [`crate::storage::Storage::prune_ledgers`]
    /// takes. One conversion, so the janitor cannot wire a key to the wrong table.
    pub fn ledger_retention(&self) -> crate::storage::LedgerRetention {
        crate::storage::LedgerRetention {
            cost_event_days: self.cost_event_retention_days,
            delivered_webhook_days: self.webhook_delivery_retention_days,
            dead_webhook_days: self.webhook_dead_letter_retention_days,
            job_yield_days: self.job_yield_retention_days,
            saved_search_seen_days: self.saved_search_seen_retention_days,
        }
    }

    /// True when anything at all is bounded — the janitor's enable check.
    pub fn any_retention_enabled(&self, sketches: bool) -> bool {
        self.revision_retention_days > 0
            || self.artifact_retention_days > 0
            || self.ledger_retention().any_enabled()
            || sketches
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
    /// Cap on captured HTML per render (bytes). The browser-tier mirror of
    /// `[http] max_body_bytes`: a JS-heavy page can build a huge DOM, and without
    /// this the whole serialized HTML is buffered into an unbounded String — the
    /// exact scenario the HTTP cap guards, but on the more expensive tier. Default
    /// 16 MiB, matching `[http] max_body_bytes`. `0` disables the cap.
    /// `RenderRequest.max_body_bytes` overrides it per render.
    pub max_html_bytes: u64,
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
            max_html_bytes: DEFAULT_MAX_BODY_BYTES,
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
        Self {
            enabled: true,
            ttl_secs: 3600,
        }
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
    /// Max plugin executions running at once across the whole host. Each call
    /// builds its own `Store` (so `max_memory_mb` bounds ONE instance) and holds
    /// a blocking-pool thread, so without a global cap a large `plugin` job admits
    /// `max_memory_mb × concurrent_calls` of wasm memory and can saturate tokio's
    /// blocking pool. `0` → `available_parallelism()` (fallback 4). Default: 0.
    pub max_concurrent: usize,
    /// Optional directory scanned for **dynamic app** `.wasm` modules (M28 v1
    /// slice). A module here that exports `describe()` is listed READ-ONLY in
    /// `GET /apps` (`dynamic: true, runnable: false`) — discovery + manifest
    /// only; executing dynamic apps needs the component-model host (next
    /// slice). `None` (the default) disables discovery entirely.
    pub app_dir: Option<PathBuf>,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: "data/plugins".into(),
            fuel: 200_000_000,
            max_memory_mb: 64,
            max_concurrent: 0,
            app_dir: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SearchConfig {
    pub enabled: bool,
    /// Directory for the embedded Tantivy index.
    pub dir: PathBuf,
    /// Cap on hits a materialized saved search (`materialize` set) writes into
    /// its target dataset per run. Bounds both the query and the per-run removal
    /// detection over the view — a broad query stays a bounded view, not an
    /// unbounded dataset copy.
    pub max_materialize_results: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: "data/search-index".into(),
            max_materialize_results: 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_defaults_are_valid() {
        Config::default()
            .validate()
            .expect("shipped defaults must satisfy their own invariants");
    }

    #[test]
    fn remote_defaults_are_off_and_empty() {
        let r = RemoteConfig::default();
        assert!(!r.enabled);
        assert!(r.nodes.is_empty());
        assert!(r.secret.is_empty());
        assert_eq!(r.timeout_secs, 60);
        assert_eq!(r.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        // Disabled: the empty secret is inert.
        Config::default()
            .validate()
            .expect("default [remote] is valid");
    }

    #[test]
    fn enabled_remote_without_a_secret_or_with_bad_nodes_is_rejected() {
        // No secret => open proxy => rejected at boot.
        let mut cfg = Config::default();
        cfg.remote.enabled = true;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[remote] secret"), "{err}");

        // A relative / schemeless node URL fails every dispatch far from here.
        let mut cfg = Config::default();
        cfg.remote.enabled = true;
        cfg.remote.secret = "s3cret".into();
        cfg.remote.nodes = vec!["node-a:8088".into()];
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[remote] node"), "{err}");

        // Zero caps starve/reject every call.
        let mut cfg = Config::default();
        cfg.remote.enabled = true;
        cfg.remote.secret = "s3cret".into();
        cfg.remote.max_body_bytes = 0;
        assert!(cfg.validate().is_err());

        // Enabled + secret + absolute nodes (or none: serve-only) is valid.
        let mut cfg = Config::default();
        cfg.remote.enabled = true;
        cfg.remote.secret = "s3cret".into();
        cfg.remote.nodes = vec!["http://10.0.0.2:8088".into()];
        cfg.validate().expect("a well-formed [remote] validates");
    }

    #[test]
    fn enabled_refresher_with_zero_budget_or_horizon_is_rejected() {
        let mut cfg = Config::default();
        cfg.refresher.enabled = true;
        cfg.refresher.global_per_tick = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("global_per_tick"), "{err}");

        let mut cfg = Config::default();
        cfg.refresher.enabled = true;
        cfg.refresher.horizon_secs = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("horizon_secs"), "{err}");

        // Disabled: the same zeros are inert (the feature is off).
        let mut cfg = Config::default();
        cfg.refresher.global_per_tick = 0;
        cfg.refresher.horizon_secs = 0;
        cfg.validate().expect("disabled refresher never binds");
    }

    /// The defaults must be a completely inert retention posture: a deployment
    /// that never mentions `[storage]` deletes nothing at all.
    #[test]
    fn retention_is_entirely_off_by_default() {
        let cfg = Config::default();
        let s = &cfg.storage;
        assert_eq!(s.revision_retention_days, 0);
        assert_eq!(s.artifact_retention_days, 0);
        assert!(!s.artifact_retention_include_cassettes);
        assert_eq!(s.cost_event_retention_days, 0);
        assert_eq!(s.webhook_delivery_retention_days, 0);
        assert_eq!(s.webhook_dead_letter_retention_days, 0);
        assert_eq!(s.job_yield_retention_days, 0);
        assert_eq!(s.saved_search_seen_retention_days, 0);
        cfg.validate().expect("the inert default validates");
    }

    /// `keep_min = 0` with pruning on deletes a record's ENTIRE history past the
    /// cutoff rather than trimming its tail — the config reads like a trim and
    /// behaves like an erase.
    #[test]
    fn zero_keep_min_with_revision_pruning_on_is_rejected() {
        let mut cfg = Config::default();
        cfg.storage.revision_retention_days = 30;
        cfg.storage.revision_retention_keep_min = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("revision_retention_keep_min"), "{err}");
        // Inert while pruning is off.
        cfg.storage.revision_retention_days = 0;
        cfg.validate()
            .expect("keep_min is meaningless with pruning off");
    }

    /// The cassette opt-in with no artifact retention parses fine and does
    /// nothing, while reading as "cassettes are unprotected".
    #[test]
    fn cassette_opt_in_without_artifact_retention_is_rejected() {
        let mut cfg = Config::default();
        cfg.storage.artifact_retention_include_cassettes = true;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("artifact_retention_include_cassettes"),
            "{err}"
        );
    }

    /// Artifact pins are held by replayable revisions, so a shorter revision
    /// window silently becomes the real artifact window.
    #[test]
    fn a_revision_window_shorter_than_the_artifact_window_is_rejected() {
        let mut cfg = Config::default();
        cfg.storage.artifact_retention_days = 90;
        cfg.storage.revision_retention_days = 30;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("revision_retention_days"), "{err}");
        cfg.storage.revision_retention_days = 90;
        cfg.validate().expect("equal windows are fine");
        // Revision retention off means history is kept forever — pins never
        // expire early, so there is nothing to reject.
        cfg.storage.revision_retention_days = 0;
        cfg.validate()
            .expect("unbounded history cannot un-pin anything");
    }

    /// The ledger knobs must map to the tables they name, and `any_enabled` must
    /// be false for a default config or the janitor spins on nothing.
    #[test]
    fn ledger_retention_maps_each_key_to_its_own_table() {
        let mut cfg = Config::default();
        assert!(!cfg.storage.ledger_retention().any_enabled());
        cfg.storage.cost_event_retention_days = 1;
        cfg.storage.webhook_delivery_retention_days = 2;
        cfg.storage.webhook_dead_letter_retention_days = 3;
        cfg.storage.job_yield_retention_days = 4;
        cfg.storage.saved_search_seen_retention_days = 5;
        let l = cfg.storage.ledger_retention();
        assert_eq!(
            (
                l.cost_event_days,
                l.delivered_webhook_days,
                l.dead_webhook_days,
                l.job_yield_days,
                l.saved_search_seen_days
            ),
            (1, 2, 3, 4, 5)
        );
        assert!(l.any_enabled());
        assert!(cfg.storage.any_retention_enabled(false));
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
    fn resilience_thresholds_that_collapse_the_ladder_are_rejected() {
        let mut cfg = Config::default();
        cfg.resilience.degrade_score = 0.9;
        cfg.resilience.quarantine_score = 0.85;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("degrade_score"), "{err}");

        // A fetch floor above 1 makes every run inconclusive — detection off.
        let mut cfg = Config::default();
        cfg.resilience.fetch_ok_floor = 1.5;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("fetch_ok_floor"));

        // Retention shorter than the baseline window prunes what the detector reads.
        let mut cfg = Config::default();
        cfg.resilience.window_runs = 20;
        cfg.resilience.sketch_retention_runs = 5;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("sketch_retention_runs"));

        // A zero recovery streak un-quarantines a source on the first run that
        // merely failed to trip — the exact behaviour quarantine exists for.
        let mut cfg = Config::default();
        cfg.resilience.recovery_runs = 0;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("recovery_runs"));
    }

    #[test]
    fn disabled_resilience_skips_its_own_rules() {
        // `enabled = false` is a complete no-op, so a nonsense threshold in a
        // section nothing reads must not refuse the boot.
        let mut cfg = Config::default();
        cfg.resilience.enabled = false;
        cfg.resilience.degrade_score = 0.99;
        cfg.resilience.quarantine_score = 0.1;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn resilience_ships_in_soak_mode() {
        // The shipping default detects and stores but gates nothing. If this flips
        // by accident, a fresh install starts suppressing pushes on day one.
        let r = ResilienceConfig::default();
        assert!(r.enabled, "detection is on by default");
        assert!(!r.enforce, "enforcement must be opt-in after a soak");
    }

    #[test]
    fn archive_ships_disabled_with_a_valid_base() {
        let a = ArchiveConfig::default();
        assert!(!a.enabled, "the archive tier must be opt-in");
        assert_eq!(a.base_url, "https://web.archive.org");
        Config::default().validate().unwrap();
    }

    #[test]
    fn recipes_ship_fully_off_and_reject_a_zero_strike_threshold() {
        let r = RecipesConfig::default();
        assert!(!r.enabled, "recipe replay must be opt-in");
        assert!(!r.auto_validate, "auto-validation must be opt-in");
        assert_eq!(r.max_failures, 3);

        let mut cfg = Config::default();
        cfg.recipes.max_failures = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[recipes] max_failures"), "{err}");
    }

    #[test]
    fn enabled_archive_rejects_a_broken_base_url() {
        let mut cfg = Config::default();
        cfg.archive.enabled = true;
        cfg.archive.base_url = "not a url".into();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[archive] base_url"), "{err}");
        // Disabled => the rule doesn't bind (nothing reads the section).
        cfg.archive.enabled = false;
        assert!(cfg.validate().is_ok());
        // Enabled with a sane base passes.
        cfg.archive.enabled = true;
        cfg.archive.base_url = "http://localhost:8090".into();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn ingress_ships_disabled_with_sane_limits() {
        // The first non-localhost write surface MUST be opt-in.
        let i = IngressConfig::default();
        assert!(!i.enabled, "ingress must be opt-in");
        assert!(i.max_body_bytes > 0);
        assert!(i.rate_limit_per_min > 0);
        assert!(i.max_skew_secs > 0);
    }

    #[test]
    fn enabled_ingress_rejects_zero_caps() {
        let mut cfg = Config::default();
        cfg.ingress.enabled = true;
        cfg.ingress.max_body_bytes = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[ingress] max_body_bytes"), "{err}");

        let mut cfg = Config::default();
        cfg.ingress.enabled = true;
        cfg.ingress.rate_limit_per_min = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[ingress] rate_limit_per_min"), "{err}");

        // Disabled => the rules don't bind (nothing reaches the surface).
        let mut cfg = Config::default();
        cfg.ingress.max_body_bytes = 0;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn mcp_ships_disabled_and_read_only() {
        // An agent-actuatable surface must be double opt-in: mount, then enqueue.
        let m = McpConfig::default();
        assert!(!m.enabled, "mcp must be opt-in");
        assert!(
            !m.allow_enqueue,
            "enqueue must be a second, separate opt-in"
        );
        assert!(m.max_job_budget_usd >= 0.0);
    }

    #[test]
    fn enabled_mcp_rejects_negative_budget_ceiling() {
        let mut cfg = Config::default();
        cfg.mcp.enabled = true;
        cfg.mcp.max_job_budget_usd = -1.0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("[mcp] max_job_budget_usd"), "{err}");
        // Disabled => the rule doesn't bind.
        cfg.mcp.enabled = false;
        assert!(cfg.validate().is_ok());
        // Enabled with zero (free-tiers-only) is allowed.
        cfg.mcp.enabled = true;
        cfg.mcp.max_job_budget_usd = 0.0;
        assert!(cfg.validate().is_ok());
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
        let raw =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../config.toml"))
                .expect("repo config.toml must be readable from the core crate");
        let mut cfg: Config = toml::from_str(&raw).expect("repo config.toml must parse");
        cfg.normalize();
        cfg.validate()
            .expect("repo config.toml must satisfy the invariants");
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
        let mut cfg: Config = toml::from_str(
            r#"
            [http]
            proxy = "http://gw:8080"
        "#,
        )
        .unwrap();
        assert!(cfg.browser.proxy.is_none(), "not yet normalized");
        cfg.normalize();
        assert_eq!(cfg.browser.proxy.as_deref(), Some("http://gw:8080"));
    }

    #[test]
    fn fetcher_profiles_dir_defaults_and_overrides() {
        assert_eq!(
            FetcherConfig::default().profiles_dir,
            PathBuf::from("data/profiles")
        );
        let cfg: Config = toml::from_str(
            r#"
            [fetcher]
            profiles_dir = "/var/lib/pumper/profiles"
            "#,
        )
        .unwrap();
        assert_eq!(
            cfg.fetcher.profiles_dir,
            PathBuf::from("/var/lib/pumper/profiles")
        );
        // Untouched sibling keys keep their defaults.
        assert_eq!(cfg.fetcher.min_content_chars, 250);
    }

    #[test]
    fn explicit_browser_proxy_wins_over_http_proxy() {
        let mut cfg: Config = toml::from_str(
            r#"
            [http]
            proxy = "http://gw:8080"
            [browser]
            proxy = "http://browser-gw:9090"
        "#,
        )
        .unwrap();
        cfg.normalize();
        assert_eq!(cfg.browser.proxy.as_deref(), Some("http://browser-gw:9090"));
    }
}
