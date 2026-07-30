use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::cache::ResearchCache;
use crate::costs::{CostLedger, SpentTotal};
use crate::datasets::{ChangeKind, Datasets, Record, UpsertSummary};
use crate::engine::{EngineSet, ResearchOutput, ResearchRequest};
use crate::fetcher::{FetchOutcome, FetchRequest};
use crate::plugin::Plugins;
use crate::{Error, Result};

/// A throttled live-progress seam. A long-running app (e.g. the crawler) calls
/// [`ProgressReporter::report`] with a compact JSON snapshot; the runtime
/// persists the latest snapshot (surfaced on `GET /jobs/{id}`) and emits it as a
/// `progress` job event through the EventBus. Implementations MUST be cheap and
/// non-blocking — `report` may be called very frequently — and throttle their
/// own persistence/emission (the server impl coalesces to ≥ every 2s or N
/// updates). Progress is in-flight telemetry only: a restart drops it.
pub trait ProgressReporter: Send + Sync {
    /// Report the job's current progress snapshot. Fire-and-forget.
    fn report(&self, snapshot: Value);
}

/// No-op reporter — the default when a runtime wires no progress seam (tests,
/// embedders). Reporting is silently dropped.
pub struct NoProgress;

impl ProgressReporter for NoProgress {
    fn report(&self, _snapshot: Value) {}
}

/// Durable-execution checkpoint seam. A long-running app hands
/// [`CheckpointSink::save`] a compact JSON snapshot of its resumable state; the
/// runtime persists it keyed by job id (attempts-lineage guarded, like
/// `complete`) so a crash, reap, timeout, or graceful-shutdown suspend costs a
/// *resume* instead of a restart. Implementations throttle their own writes
/// (the server impl coalesces like the progress reporter); `force` bypasses the
/// throttle for final/suspend snapshots. Returns `false` when the write did not
/// land (stale lineage or a storage error) so apps can count it — persistence
/// failures must never fail the job.
#[async_trait]
pub trait CheckpointSink: Send + Sync {
    async fn save(&self, state: Value, force: bool) -> bool;
}

/// No-op sink — the default when a runtime wires no checkpoint seam (tests,
/// embedders). Saves are silently dropped (reported as landed).
pub struct NoCheckpoints;

#[async_trait]
impl CheckpointSink for NoCheckpoints {
    async fn save(&self, _state: Value, _force: bool) -> bool {
        true
    }
}

/// Everything a job run gets from the runtime: its params, the engines, the
/// dataset store (dedup + change detection), the sandboxed WASM plugin host,
/// and a per-job artifacts directory for raw dumps (HTML, JSON, screenshots).
pub struct AppContext {
    pub job_id: Uuid,
    /// Name of the running app; scopes dataset records.
    pub app: String,
    pub params: Value,
    pub engines: Arc<EngineSet>,
    pub datasets: Arc<Datasets>,
    /// Cost ledger: every metered engine call is attributed to this job.
    pub costs: Arc<CostLedger>,
    /// Spend ceiling for the whole job (from enqueue); None = unlimited.
    pub budget_usd: Option<f64>,
    /// This job's running spend, seeded from the ledger at construction and
    /// advanced by each metered seam. Backs `remaining_budget_usd` so the
    /// per-call budget check doesn't re-`SUM` the job's whole cost history.
    pub spent_usd: Arc<SpentTotal>,
    /// Cost-aware cache for Claude research runs (TTL-bound, key = request).
    pub research_cache: Arc<ResearchCache>,
    /// Learned per-host tier routing (skip the HTTP tier where it never wins).
    pub tiers: Arc<crate::tiers::TierMemory>,
    /// Extraction health: judges a run against the source's own past, and gates
    /// the write paths below when enforcement is on. [`Resilience::disabled`]
    /// makes every consultation a no-op.
    pub health: Arc<crate::resilience::Resilience>,
    /// Sandboxed WASM plugin host (fuel + memory limited).
    pub plugins: Arc<dyn Plugins>,
    /// Throttled live-progress seam: long-running apps report compact snapshots
    /// that surface on `GET /jobs/{id}` and as `progress` SSE events.
    pub progress: Arc<dyn ProgressReporter>,
    /// Durable-execution seam: `ctx.checkpoint(..)` persists resumable state
    /// through this sink (throttled; lineage-guarded server-side).
    pub checkpoints: Arc<dyn CheckpointSink>,
    /// The last persisted checkpoint for this job, handed back on re-claim —
    /// `None` on a fresh first attempt or after the poisoned-checkpoint escape.
    /// Advisory: apps must tolerate any stored shape and start fresh on doubt.
    pub restored: Option<Value>,
    pub artifacts_dir: PathBuf,
}

impl AppContext {
    /// Persists a durable checkpoint of this job's resumable state (throttled —
    /// safe to call in a tight loop). On a later attempt of the same job, the
    /// last persisted snapshot comes back via [`restore`](Self::restore).
    /// Returns whether the write landed (`false` = throttle-skipped is *not*
    /// reported false; only stale-lineage/storage failures are).
    pub async fn checkpoint(&self, state: Value) -> bool {
        self.checkpoints.save(state, false).await
    }

    /// Unthrottled checkpoint — for final/suspend snapshots where losing the
    /// write means re-doing real work on resume.
    pub async fn checkpoint_now(&self, state: Value) -> bool {
        self.checkpoints.save(state, true).await
    }

    /// The last checkpoint persisted by a prior attempt of this job, if any.
    /// Advisory: treat unexpected shapes as "start fresh", never as an error.
    pub fn restore(&self) -> Option<&Value> {
        self.restored.as_ref()
    }

    /// Writes a file under `data/artifacts/<app>/<job_id>/` and returns its path.
    pub async fn save_artifact(&self, name: &str, bytes: &[u8]) -> Result<PathBuf> {
        // `name` may be composed from job params (e.g. census `cbp-{naics}.json`),
        // so reject anything that isn't a single safe segment — otherwise a `..`
        // or absolute name escapes the per-job artifact dir.
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
            || std::path::Path::new(name).is_absolute()
        {
            return Err(Error::App(format!("unsafe artifact name: {name:?}")));
        }
        tokio::fs::create_dir_all(&self.artifacts_dir).await?;
        let path = self.artifacts_dir.join(name);
        tokio::fs::write(&path, bytes).await?;
        Ok(path)
    }

    /// Reads the stored body of a source-dataset record — the crawl→extract/plugin
    /// seam. Records written by the crawl carry `artifact_path` + `job_id`, and
    /// their bodies live at `data/artifacts/<source_app>/<job_id>/<artifact_path>`,
    /// under the shared artifacts root (this job's own dir is two levels below it).
    /// Lets an app run over already-crawled bodies instead of re-fetching. Returns
    /// the body, or a human reason to report per key.
    ///
    /// `source_app`, `job_id` and `artifact_path` all come from untrusted
    /// record/param data, and `Path::join` lets an absolute or `..` component
    /// escape the artifacts root (an arbitrary server-file read into job output), so
    /// each must be a single safe path segment.
    pub async fn read_source_artifact(
        &self,
        source_app: &str,
        record: &Record,
    ) -> std::result::Result<String, String> {
        let artifact = record
            .data
            .get("artifact_path")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "record has no artifact_path".to_string())?;
        let job_id = record
            .data
            .get("job_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "record has no job_id".to_string())?;
        safe_path_segment(source_app, "source app")?;
        safe_path_segment(job_id, "job_id")?;
        safe_path_segment(artifact, "artifact_path")?;
        let root = self
            .artifacts_dir
            .parent()
            .and_then(std::path::Path::parent)
            .ok_or_else(|| "cannot resolve artifacts root".to_string())?;
        let path = root.join(source_app).join(job_id).join(artifact);
        tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| format!("unreadable artifact {}: {e}", path.display()))
    }

    /// USD this job still may spend under its ceiling. None = unlimited.
    ///
    /// Reads the in-context running total rather than re-aggregating the ledger:
    /// this is on the pre-flight path of every metered call, so a `SELECT SUM`
    /// here costs O(spend events so far) per call and O(n²) over a job.
    pub async fn remaining_budget_usd(&self) -> Result<Option<f64>> {
        let Some(budget) = self.budget_usd else {
            return Ok(None);
        };
        Ok(Some((budget - self.spent_usd.get()).max(0.0)))
    }

    /// Clamps a per-call budget ceiling to the job's remaining headroom: keep the
    /// caller's own ceiling when it is already tighter, else adopt the headroom.
    /// Shared by the two metered seams, which otherwise re-typed the expression.
    fn clamp_to_headroom(ceiling: Option<f64>, remaining: f64) -> f64 {
        ceiling.map_or(remaining, |b| b.min(remaining))
    }

    /// Records one engine call against this job: writes the cost event and
    /// advances the running spend total that governs the budget ceiling.
    ///
    /// [`AppContext::fetch`] calls this for you. Call it directly only when an
    /// app must drive an engine raw — the crawler owns its own concurrency,
    /// robots and frontier control, so it cannot route through `fetch` — and
    /// would otherwise be invisible to the cost ledger and budget enforcement.
    ///
    /// Accounting never fails the caller's job: a failed write is warn-logged.
    pub async fn meter(
        &self,
        engine: &str,
        url: Option<&str>,
        cost_usd: f64,
        detail: Option<&str>,
    ) {
        self.spent_usd.add(cost_usd);
        if let Err(e) = self
            .costs
            .record(self.job_id, &self.app, engine, url, cost_usd, detail)
            .await
        {
            tracing::warn!(job = %self.job_id, "cost event write failed: {e}");
        }
    }

    /// Teaches the learned tier router about one fetch outcome for `host`: an
    /// HTTP win resets the host, an HTTP loss (thin/blocked/error) adds a strike,
    /// and hosts that persistently lose start straight at the browser tier.
    ///
    /// [`AppContext::fetch`] calls this for you; raw-engine apps should call it
    /// so their per-host outcomes still train the router. Never fails the job.
    pub async fn learn_tier(&self, host: &str, winner: &str, http_lost: bool) {
        if let Err(e) = self.tiers.record(host, winner, http_lost).await {
            tracing::warn!(job = %self.job_id, "tier memory write failed: {e}");
        }
    }

    /// Errors when the job's spend ceiling is already reached — the abort
    /// switch for metered Claude calls. Returns the remaining headroom.
    async fn require_budget(&self) -> Result<Option<f64>> {
        match self.remaining_budget_usd().await? {
            Some(remaining) if remaining <= 0.0 => Err(Error::App(format!(
                "job budget of ${:.2} exhausted — aborting before further metered spend",
                self.budget_usd.unwrap_or(0.0)
            ))),
            other => Ok(other),
        }
    }

    /// Metered tiered fetch: same as `engines.fetch.fetch(...)` but records a
    /// cost event (tier used, escalation trail, Claude spend) against this job.
    /// Prefer this over calling the fetcher directly.
    pub async fn fetch(&self, mut req: FetchRequest) -> Result<FetchOutcome> {
        let host = url::Url::parse(&req.url)
            .ok()
            .and_then(|u| u.host_str().map(str::to_lowercase));

        // Learned tier routing: hosts where the HTTP tier persistently loses
        // start straight at the browser (escalating strategies only).
        let mut tier_note = None;
        if let Some(host) = &host {
            if !req.skip_http
                && matches!(
                    req.strategy,
                    crate::fetcher::FetchStrategy::Auto
                        | crate::fetcher::FetchStrategy::AutoWithResearch
                )
            {
                match self.tiers.preferred(host).await {
                    Ok(Some(pref)) if pref == "browser" => {
                        req.skip_http = true;
                        tier_note = Some(
                            "http tier skipped: learned host preference (persistent http losses)"
                                .to_string(),
                        );
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(job = %self.job_id, "tier memory read failed: {e}"),
                }
            }
        }

        // Budget-governed escalation: only the Claude tier spends money. With
        // headroom, clamp the tier's per-call ceiling to what's left; with none,
        // downgrade to the free tiers instead of failing the whole fetch.
        let mut budget_note = None;
        if matches!(
            req.strategy,
            crate::fetcher::FetchStrategy::AutoWithResearch
        ) {
            match self.remaining_budget_usd().await? {
                Some(remaining) if remaining <= 0.0 => {
                    req.strategy = crate::fetcher::FetchStrategy::Auto;
                    budget_note = Some(format!(
                        "claude tier skipped: job budget of ${:.2} exhausted",
                        self.budget_usd.unwrap_or(0.0)
                    ));
                }
                Some(remaining) => {
                    req.max_budget_usd =
                        Some(Self::clamp_to_headroom(req.max_budget_usd, remaining));
                }
                None => {}
            }
        }
        let url = req.url.clone();
        let mut outcome = self.engines.fetch.fetch(req).await?;
        // Router-level skips are recorded as structured `skipped_by_router`
        // trace entries and kept as human trail lines alongside.
        if let Some(note) = tier_note {
            outcome.trace.push(crate::fetcher::TierTrace {
                tier: crate::fetcher::FetchTier::Http,
                verdict: crate::fetcher::TierVerdict::SkippedByRouter,
                http_status: None,
                content_chars: None,
                cache_hit: None,
                latency_ms: 0,
                cost_usd: None,
                detail: Some("learned host preference (persistent http losses)".to_string()),
            });
            outcome.escalations.push(note);
        }
        if let Some(note) = budget_note {
            outcome.trace.push(crate::fetcher::TierTrace {
                tier: crate::fetcher::FetchTier::Claude,
                verdict: crate::fetcher::TierVerdict::SkippedByRouter,
                http_status: None,
                content_chars: None,
                cache_hit: None,
                latency_ms: 0,
                cost_usd: None,
                detail: Some("job budget exhausted".to_string()),
            });
            outcome.escalations.push(note);
        }
        // Teach the router: an HTTP win resets the host, an HTTP loss (the
        // http tier's trace verdict is thin/blocked/error) adds a strike. Keyed
        // on the structured verdict enum, not the free-text trail.
        if let Some(host) = &host {
            let http_lost = outcome.trace.iter().any(|t| {
                t.tier == crate::fetcher::FetchTier::Http
                    && matches!(
                        t.verdict,
                        crate::fetcher::TierVerdict::Thin
                            | crate::fetcher::TierVerdict::Blocked
                            | crate::fetcher::TierVerdict::Error
                    )
            });
            self.learn_tier(host, outcome.engine, http_lost).await;
        }
        let detail = (!outcome.escalations.is_empty()).then(|| outcome.escalations.join("; "));
        self.meter(
            outcome.engine,
            Some(&url),
            outcome.cost_usd.unwrap_or(0.0),
            detail.as_deref(),
        )
        .await;
        Ok(outcome)
    }

    /// Metered Claude research — **the only way to reach the model.** The
    /// researcher behind [`EngineSet`] is `pub(crate)` precisely so this is not
    /// a convention an app can forget: it is cache-aware and budget-governed,
    /// and a direct call loses all of it. Identical requests within the cache
    /// TTL are served from disk at zero cost (recorded as a `cache_hit`
    /// event); misses refuse to start once the job budget is exhausted, clamp
    /// the per-call ceiling to the remaining headroom, and store their output
    /// for the next caller. `resume_session` requests bypass the cache.
    pub async fn research(&self, mut req: ResearchRequest) -> Result<ResearchOutput> {
        let cacheable = req.resume_session.is_none() && self.research_cache.enabled();
        let key = cacheable.then(|| ResearchCache::key(&req));
        if let Some(key) = &key {
            if let Some(mut hit) = self.research_cache.get(key).await? {
                let saved = hit.cost_usd.take();
                let detail = saved.map_or("cache_hit".to_string(), |c| {
                    format!("cache_hit (saved ~${c:.4})")
                });
                self.meter("claude", None, 0.0, Some(&detail)).await;
                hit.cost_usd = Some(0.0);
                return Ok(hit);
            }
        }

        if let Some(remaining) = self.require_budget().await? {
            req.max_budget_usd = Some(Self::clamp_to_headroom(req.max_budget_usd, remaining));
        }
        let out = self.engines.researcher().research(req).await?;
        self.meter("claude", None, out.cost_usd.unwrap_or(0.0), None)
            .await;
        if let Some(key) = &key {
            if let Err(e) = self.research_cache.put(key, &out).await {
                tracing::warn!(job = %self.job_id, "research cache write failed: {e}");
            }
        }
        Ok(out)
    }

    pub fn require_str(&self, key: &str) -> Result<&str> {
        self.params
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| Error::App(format!("missing required string param '{key}'")))
    }

    /// Upserts one record into `<this app>/<dataset>`, reporting new/changed/unchanged.
    pub async fn upsert(&self, dataset: &str, key: &str, value: &Value) -> Result<ChangeKind> {
        let (dataset, trust) = self.write_target(dataset).await;
        self.datasets
            .upsert_trusted(&self.app, &dataset, key, value, trust)
            .await
    }

    /// Upserts a batch and returns a new/changed/unchanged summary — the primary
    /// dedup + change-detection entry point for periodic scrapes.
    pub async fn upsert_many(
        &self,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        let (dataset, trust) = self.write_target(dataset).await;
        self.datasets
            .upsert_many_trusted(&self.app, &dataset, items, trust)
            .await
    }

    /// Full-snapshot sync: upserts the batch, then marks previously-seen keys
    /// that are absent from it as removed. Use instead of `upsert_many` when
    /// `items` is the complete current state of the dataset (e.g. a full API
    /// listing) — the summary's `removed` keys are the disappeared-record
    /// signal (delisted grants, closed vacancies, removed listings).
    ///
    /// **A degrading source never tombstones its own dataset.** When the source's
    /// health state suppresses removals this silently downgrades to
    /// [`upsert_many`](Self::upsert_many): a half-broken run produces a
    /// short-but-nonempty batch, and removal detection would then tombstone every
    /// key missing from it — the single most destructive thing a degrading source
    /// can do. `detect_removed` already refuses an *empty* batch; a partial batch
    /// is the case that guard does not cover.
    ///
    /// The check lives here, in the one method every caller reaches removal
    /// detection through, rather than in each app — a control wired into one
    /// caller silently exempts all the others.
    pub async fn sync_many(
        &self,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        let mut summary = self.upsert_many(dataset, items).await?;
        let state = self.health.enforced_state(&self.app, dataset).await;
        if state.suppresses_removals() {
            tracing::warn!(
                job = %self.job_id,
                dataset,
                state = state.as_str(),
                "removal detection suppressed: source is degrading, so a short batch \
                 must not tombstone the keys missing from it"
            );
            return Ok(summary);
        }
        let present: Vec<String> = items.iter().map(|(k, _)| k.clone()).collect();
        summary.removed = self
            .datasets
            .detect_removed(&self.app, dataset, &present)
            .await?;
        Ok(summary)
    }

    /// Where a write to `dataset` goes and what stamp it carries, given the
    /// source's health. Quarantined sources write to the shadow dataset
    /// `<dataset>@q`, which is an ordinary dataset — so every existing tool
    /// (listing, export, changes, duplicates) already works on it.
    ///
    /// Returns `(dataset, trust)` unchanged when enforcement is off, which is the
    /// shipping default: soak mode computes verdicts and gates nothing.
    async fn write_target(&self, dataset: &str) -> (String, Option<&'static str>) {
        let state = self.health.enforced_state(&self.app, dataset).await;
        (
            crate::resilience::write_dataset(dataset, state),
            state.trust(),
        )
    }

    /// Judges this run's extraction against the source's own history, records the
    /// verdict, and moves the source's state. `Ok(None)` when detection is off.
    ///
    /// Call this **before** upserting: the state it settles is what
    /// [`upsert_many`](Self::upsert_many) and [`sync_many`](Self::sync_many) then
    /// gate on, and judging afterwards would stamp trust and infer removals from
    /// a verdict that did not exist yet.
    pub async fn observe_extraction(
        &self,
        dataset: &str,
        docs: &[crate::resilience::ObservedDoc],
        fetch: crate::resilience::FetchHealth,
    ) -> Result<Option<crate::resilience::SourceVerdict>> {
        self.health
            .observe(
                &self.app,
                &crate::resilience::RunReport {
                    job_id: self.job_id,
                    dataset,
                    docs,
                    fetch,
                    build_id: build_id(),
                },
            )
            .await
    }
}

/// This build's identity, stamped on every run row so a fleet-wide break
/// correlates with a deploy in one query instead of looking like thirty sites
/// changing on the same day. `PUMPER_BUILD_ID` when set (a commit sha in CI),
/// else the crate version.
fn build_id() -> Option<String> {
    Some(std::env::var("PUMPER_BUILD_ID").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string()))
}

/// Rejects a string that is not a single safe path segment (empty, `.`/`..`,
/// contains a separator, or absolute) — the path-traversal guard when composing a
/// filesystem path from untrusted record/param data.
fn safe_path_segment(s: &str, what: &str) -> std::result::Result<(), String> {
    if s.is_empty()
        || s == "."
        || s == ".."
        || s.contains('/')
        || s.contains('\\')
        || std::path::Path::new(s).is_absolute()
    {
        return Err(format!("unsafe {what}: {s:?}"));
    }
    Ok(())
}

/// A precondition an app needs to actually run in this deployment — declared
/// statically so the registry can report *readiness*, not just *registration*
/// (e.g. a credential-gated app is otherwise indistinguishable from a ready one
/// over `GET /apps`, and only surfaces its gap via a failed job).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// An environment variable that must be set (typically an API key/credential).
    Env(&'static str),
}

impl Requirement {
    /// Whether this precondition is satisfied in the current environment.
    pub fn is_satisfied(&self) -> bool {
        match self {
            Requirement::Env(name) => std::env::var(name).is_ok(),
        }
    }

    /// Stable label for the API / metrics (e.g. `env:CENSUS_API_KEY`).
    pub fn label(&self) -> String {
        match self {
            Requirement::Env(name) => format!("env:{name}"),
        }
    }
}

// ── App manifest (agent-ready registry) ─────────────────────────────────────

/// How a run of this app spends money — coarse, static, and honest, so an
/// agent (or a human) can tell a free API sync from a Claude-driven job
/// before enqueueing anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostClass {
    /// Free engines only (HTTP/browser); a run never produces spend events.
    Free,
    /// May spend via metered escalation (e.g. a fetch strategy that can reach
    /// the Claude tier), but doesn't drive the model by design.
    Metered,
    /// Drives Claude by design — every meaningful run costs real money and
    /// should carry a `budget_usd`.
    Claude,
}

impl CostClass {
    pub fn as_str(self) -> &'static str {
        match self {
            CostClass::Free => "free",
            CostClass::Metered => "metered",
            CostClass::Claude => "claude",
        }
    }
}

/// One worked example invocation: params that are known-good against the
/// app's `params_schema`. The server's manifest test validates every example
/// against its own schema, so examples can't drift into lies.
#[derive(Debug, Clone)]
pub struct ManifestExample {
    /// What this invocation does, phrased for an agent picking between examples.
    pub description: &'static str,
    /// The `params` object to POST.
    pub params: Value,
}

/// Rich machine-operable self-description of a [`ScrapeApp`]: a JSON Schema
/// for its params, worked examples, the shape of its result, and its cost
/// class. Served by `GET /apps` (and as MCP tool definitions via
/// `GET /apps?format=tools` and the `/mcp` endpoint); enqueue validates params
/// against `params_schema` when one is declared.
///
/// The default ([`AppManifest::default`]) declares nothing: no schema (params
/// accepted as before), no examples, `Free`. Every existing app therefore
/// compiles and behaves untouched; apps opt into richer manifests by
/// overriding [`ScrapeApp::manifest`].
#[derive(Debug, Clone, Default)]
pub struct AppManifest {
    /// JSON Schema (draft 2020-12) for the job `params` object. When `Some`,
    /// the server rejects an enqueue whose merged params fail it (422 with
    /// JSON-pointer paths). Keep `additionalProperties` permissive unless a
    /// stray key is genuinely an error — the schema is a contract for agents,
    /// not a straitjacket for humans.
    pub params_schema: Option<Value>,
    /// Worked invocations, each guaranteed (by test) to pass `params_schema`.
    pub examples: Vec<ManifestExample>,
    /// Human/agent description of the job-result JSON shape. Advisory.
    pub output_shape: Option<&'static str>,
    /// How runs of this app spend money.
    pub cost_class: CostClass,
}

impl Default for CostClass {
    fn default() -> Self {
        CostClass::Free
    }
}

/// One scraping use case. Implement this in a crate under `crates/apps/` and
/// register it in the server's `registry.rs` — that is the whole integration.
#[async_trait]
pub trait ScrapeApp: Send + Sync {
    /// Unique name; becomes the API path segment (`POST /apps/<name>/jobs`).
    fn name(&self) -> &'static str;

    fn description(&self) -> &'static str {
        ""
    }

    /// Recurring schedule as a cron expression with seconds
    /// (`"0 0 */6 * * *"` = every 6 hours). `None` = manual runs only.
    fn schedule(&self) -> Option<&'static str> {
        None
    }

    /// Preconditions that must hold for this app to actually run here (e.g. a
    /// required API-key env var). Reported by `GET /apps` alongside a resolved
    /// `ready` flag, so a credential-gated app is distinguishable from a ready one
    /// before its first failed job. Default: none.
    fn requires(&self) -> &'static [Requirement] {
        &[]
    }

    /// Params used for scheduled runs and for API calls without a body.
    fn default_params(&self) -> Value {
        Value::Object(Default::default())
    }

    /// The app's agent-ready manifest: params JSON Schema, worked examples,
    /// output shape, cost class. The default declares nothing — `name()` +
    /// `default_params()` already describe the app well enough to list it —
    /// so every app compiles untouched; the most-used apps override this with
    /// rich manifests. When a schema is declared, enqueue enforces it (422).
    fn manifest(&self) -> AppManifest {
        AppManifest::default()
    }

    /// Executes one job. The returned JSON is stored as the job result.
    async fn run(&self, ctx: AppContext) -> Result<Value>;
}

#[cfg(test)]
mod tests {
    use super::safe_path_segment;

    /// The ONE artifact path-traversal guard shared by extractor + plugin
    /// (`read_source_artifact`). Gutting it to `Ok(())` must turn this red —
    /// it is the only thing between untrusted record/param data and a
    /// filesystem path outside the artifacts root.
    #[test]
    fn safe_path_segment_rejects_every_escape_shape() {
        for bad in [
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "/etc/passwd",
            "..\\up",
            "C:\\x",
            "/",
        ] {
            assert!(
                safe_path_segment(bad, "test").is_err(),
                "must reject {bad:?}"
            );
        }
    }

    #[test]
    fn safe_path_segment_accepts_plain_segments() {
        for ok in ["page-0001.html", "grants-gov", "a.b_c-d", "café", ".hidden"] {
            assert!(safe_path_segment(ok, "test").is_ok(), "must accept {ok:?}");
        }
    }
}
