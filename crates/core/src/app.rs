use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::cache::ResearchCache;
use crate::costs::CostLedger;
use crate::datasets::{ChangeKind, Datasets, UpsertSummary};
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
    /// Cost-aware cache for Claude research runs (TTL-bound, key = request).
    pub research_cache: Arc<ResearchCache>,
    /// Learned per-host tier routing (skip the HTTP tier where it never wins).
    pub tiers: Arc<crate::tiers::TierMemory>,
    /// Sandboxed WASM plugin host (fuel + memory limited).
    pub plugins: Arc<dyn Plugins>,
    /// Throttled live-progress seam: long-running apps report compact snapshots
    /// that surface on `GET /jobs/{id}` and as `progress` SSE events.
    pub progress: Arc<dyn ProgressReporter>,
    pub artifacts_dir: PathBuf,
}

impl AppContext {
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

    /// USD this job still may spend under its ceiling. None = unlimited.
    pub async fn remaining_budget_usd(&self) -> Result<Option<f64>> {
        let Some(budget) = self.budget_usd else {
            return Ok(None);
        };
        let spent = self.costs.job_total(self.job_id).await?;
        Ok(Some((budget - spent).max(0.0)))
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
        if matches!(req.strategy, crate::fetcher::FetchStrategy::AutoWithResearch) {
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
                        Some(req.max_budget_usd.map_or(remaining, |b| b.min(remaining)));
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
            if let Err(e) = self.tiers.record(host, outcome.engine, http_lost).await {
                tracing::warn!(job = %self.job_id, "tier memory write failed: {e}");
            }
        }
        let detail = (!outcome.escalations.is_empty()).then(|| outcome.escalations.join("; "));
        if let Err(e) = self
            .costs
            .record(
                self.job_id,
                &self.app,
                outcome.engine,
                Some(&url),
                outcome.cost_usd.unwrap_or(0.0),
                detail.as_deref(),
            )
            .await
        {
            tracing::warn!(job = %self.job_id, "cost event write failed: {e}");
        }
        Ok(outcome)
    }

    /// Metered Claude research: same as `engines.claude.research(...)` but
    /// cache-aware and budget-governed. Identical requests within the cache
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
                if let Err(e) = self
                    .costs
                    .record(self.job_id, &self.app, "claude", None, 0.0, Some(&detail))
                    .await
                {
                    tracing::warn!(job = %self.job_id, "cost event write failed: {e}");
                }
                hit.cost_usd = Some(0.0);
                return Ok(hit);
            }
        }

        if let Some(remaining) = self.require_budget().await? {
            req.max_budget_usd = Some(req.max_budget_usd.map_or(remaining, |b| b.min(remaining)));
        }
        let out = self.engines.claude.research(req).await?;
        if let Err(e) = self
            .costs
            .record(self.job_id, &self.app, "claude", None, out.cost_usd.unwrap_or(0.0), None)
            .await
        {
            tracing::warn!(job = %self.job_id, "cost event write failed: {e}");
        }
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
        self.datasets.upsert(&self.app, dataset, key, value).await
    }

    /// Upserts a batch and returns a new/changed/unchanged summary — the primary
    /// dedup + change-detection entry point for periodic scrapes.
    pub async fn upsert_many(
        &self,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        self.datasets.upsert_many(&self.app, dataset, items).await
    }

    /// Full-snapshot sync: upserts the batch, then marks previously-seen keys
    /// that are absent from it as removed. Use instead of `upsert_many` when
    /// `items` is the complete current state of the dataset (e.g. a full API
    /// listing) — the summary's `removed` keys are the disappeared-record
    /// signal (delisted grants, closed vacancies, removed listings).
    pub async fn sync_many(
        &self,
        dataset: &str,
        items: &[(String, Value)],
    ) -> Result<UpsertSummary> {
        let mut summary = self.datasets.upsert_many(&self.app, dataset, items).await?;
        let present: Vec<String> = items.iter().map(|(k, _)| k.clone()).collect();
        summary.removed = self
            .datasets
            .detect_removed(&self.app, dataset, &present)
            .await?;
        Ok(summary)
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

    /// Params used for scheduled runs and for API calls without a body.
    fn default_params(&self) -> Value {
        Value::Object(Default::default())
    }

    /// Executes one job. The returned JSON is stored as the job result.
    async fn run(&self, ctx: AppContext) -> Result<Value>;
}
