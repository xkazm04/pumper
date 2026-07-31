//! M44 "speak a data source into existence" — v1: a PROPOSAL COMPILER.
//!
//! From one sentence ("track Czech senior Rust salaries weekly") the app:
//!   1. runs a research session (the same Claude engine seam `app-research`
//!      uses — schema-guarded, budget-clamped, cached) to identify 1–3
//!      candidate source URLs;
//!   2. samples each candidate through the tiered fetcher (the readable path);
//!   3. drafts a declarative [`RuleSet`] against the sampled bodies and
//!      DRY-RUNS it through the real extraction engine
//!      ([`pumper_core::extract_one_with_report`]), iterating up to
//!      `max_iterations` until a strict majority of fields match;
//!   4. emits a complete **provision proposal** record into
//!      `provisioner/proposals`: `{catalog_row (TOML-shaped Source), rule_set,
//!      seeds, cadence, budget, sample_stats, confidence}`.
//!
//! The app NEVER writes `catalog/data-sources.toml` and NEVER creates
//! schedules: the emitted catalog row is always `status = "planned"` with an
//! empty `cron`, so even a human pasting it verbatim into the catalog cannot
//! make the reconciler start anything — going live is a deliberate human edit.
//! That invariant is enforced by [`build_catalog_row`] and pinned by test.

use std::collections::BTreeMap;

use async_trait::async_trait;
use pumper_core::{
    extract_one_with_report, salvage_json, AppContext, AppManifest, CostClass, Error,
    FetchRequest, FetchStrategy, ManifestExample, ResearchRequest, Result, RuleSet, ScrapeApp,
    Source,
};
use pumper_core::datasets::Provenance;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Provisioner;

/// At most this many candidate URLs are researched and sampled.
const MAX_CANDIDATES: usize = 3;

/// Default repair iterations for the draft → dry-run loop.
const DEFAULT_MAX_ITERATIONS: u32 = 2;

/// Hard cap on repair iterations regardless of params.
const ITERATIONS_CAP: u32 = 5;

/// Max chars of a sampled body inlined into the drafting prompt.
const SAMPLE_PROMPT_CAP_CHARS: usize = 12_000;

/// Max chars of the proposal-record key derived from the prompt.
const KEY_CAP_CHARS: usize = 64;

/// Default archive-tier freshness window for candidate SAMPLING (seconds, 24h).
///
/// Sampling exists to learn a page's SHAPE well enough to draft selectors, not
/// to capture its current values — and layouts move on the scale of months. So
/// a day-old web-archive snapshot is a fully adequate sample, and taking it
/// spares an unknown third-party host the first-contact hit from a compile that
/// may be re-run several times over one prompt. The archive tier is
/// opportunistic, never terminal: an absent, older, or thin snapshot falls
/// straight through to the live ladder. `0` opts out (live-only).
const DEFAULT_SAMPLE_ARCHIVE_MAX_AGE_SECS: u64 = 86_400;

// ── discovery ───────────────────────────────────────────────────────────────

/// One candidate source URL the research stage proposed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub url: String,
    pub name: String,
    /// Proposed refresh cadence in catalog vocabulary
    /// (`daily|weekly|monthly|quarterly|annual|on-demand`).
    pub cadence: String,
    /// Fields the source is expected to yield (drives the rule draft).
    pub expected_fields: Vec<String>,
}

/// Parses the discovery report `{candidates: [{url, name, cadence,
/// expected_fields}]}` defensively: entries without an http(s) URL are
/// dropped, duplicates (by URL) collapse, and the list is capped at
/// [`MAX_CANDIDATES`]. Anything unparseable yields an empty list — the caller
/// decides that is a loud failure, not this parser.
fn parse_candidates(v: &Value) -> Vec<Candidate> {
    let Some(items) = v.get("candidates").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<Candidate> = Vec::new();
    for item in items {
        let Some(url) = item.get("url").and_then(Value::as_str) else {
            continue;
        };
        let url = url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            continue;
        }
        if out.iter().any(|c| c.url == url) {
            continue;
        }
        out.push(Candidate {
            url: url.to_string(),
            name: item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(url)
                .trim()
                .to_string(),
            cadence: normalize_cadence(item.get("cadence").and_then(Value::as_str)),
            expected_fields: item
                .get("expected_fields")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        });
        if out.len() == MAX_CANDIDATES {
            break;
        }
    }
    out
}

/// Maps a free-text cadence onto the catalog vocabulary; unknown → "weekly"
/// (a conservative default the human reviewer sees and can change).
fn normalize_cadence(s: Option<&str>) -> String {
    match s.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some(c @ ("daily" | "weekly" | "monthly" | "quarterly" | "annual" | "on-demand")) => {
            c.to_string()
        }
        _ => "weekly".to_string(),
    }
}

// ── dry-run harness ─────────────────────────────────────────────────────────

/// Per-field outcome across the sampled bodies.
#[derive(Debug, Clone, Serialize)]
pub struct FieldStat {
    /// Docs where the field's rule bound (Matched, or ContainerEmpty — a
    /// present-but-quiet listing is not a broken selector).
    pub matched_docs: usize,
    pub total_docs: usize,
}

impl FieldStat {
    /// A field "holds" when it bound on at least half the sampled docs.
    fn holds(&self) -> bool {
        self.total_docs > 0 && self.matched_docs * 2 >= self.total_docs
    }
}

/// The dry-run verdict for one drafted rule set over the sampled bodies.
#[derive(Debug, Clone, Serialize)]
pub struct DryRun {
    pub stats: BTreeMap<String, FieldStat>,
    pub docs: usize,
    pub fields_total: usize,
    pub fields_matched: usize,
    /// The STOP RULE: a strict majority of fields hold across the samples.
    pub accepted: bool,
    /// Fields that failed to hold, worst first — the repair-prompt feedback.
    pub worst_fields: Vec<String>,
}

/// Runs a drafted rule set through the REAL extraction engine over the sampled
/// bodies and scores it. A rule set that doesn't compile is an `Err` — the
/// loop feeds the compile error back to the model as repair feedback rather
/// than treating it as a scored-zero draft.
pub fn dry_run(rules: &RuleSet, bodies: &[String]) -> Result<DryRun> {
    if rules.fields.is_empty() {
        return Err(Error::App("drafted rule set has no fields".into()));
    }
    let compiled = rules.compile()?;
    let mut stats: BTreeMap<String, FieldStat> = rules
        .fields
        .keys()
        .map(|name| {
            (
                name.clone(),
                FieldStat {
                    matched_docs: 0,
                    total_docs: bodies.len(),
                },
            )
        })
        .collect();
    for body in bodies {
        let (_, report) = extract_one_with_report(&compiled, body);
        for (name, status) in &report.fields {
            if !status.is_miss() {
                if let Some(stat) = stats.get_mut(name) {
                    stat.matched_docs += 1;
                }
            }
        }
    }
    let fields_total = stats.len();
    let fields_matched = stats.values().filter(|s| s.holds()).count();
    // Strict majority: more than half the fields must hold.
    let accepted = fields_matched * 2 > fields_total;
    let mut worst: Vec<(&String, &FieldStat)> =
        stats.iter().filter(|(_, s)| !s.holds()).collect();
    worst.sort_by_key(|(name, s)| (s.matched_docs, name.as_str().to_string()));
    let worst_fields = worst.into_iter().map(|(name, _)| name.clone()).collect();
    Ok(DryRun {
        stats,
        docs: bodies.len(),
        fields_total,
        fields_matched,
        accepted,
        worst_fields,
    })
}

/// Overall confidence 0–100: the fraction of (field × doc) cells that bound.
pub fn confidence(dry: &DryRun) -> u8 {
    let cells = dry.fields_total * dry.docs;
    if cells == 0 {
        return 0;
    }
    let bound: usize = dry.stats.values().map(|s| s.matched_docs).sum();
    ((bound as f64 / cells as f64) * 100.0).round().min(100.0) as u8
}

// ── proposal record ─────────────────────────────────────────────────────────

/// Stable, readable proposal key from the prompt: lowercase alnum runs joined
/// by dashes, capped. Same prompt → same key, so re-compiles upsert (change
/// detection sees a revised proposal, not a pile of near-duplicates).
pub fn proposal_key(prompt: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in prompt.chars() {
        if out.len() >= KEY_CAP_CHARS {
            break;
        }
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    let out = out.trim_end_matches('-').to_string();
    if out.is_empty() {
        "proposal".to_string()
    } else {
        out
    }
}

/// The TOML-shaped catalog row for the proposal — the exact `[[source]]`
/// field set ([`pumper_core::Source`]), so a human can paste it into
/// `catalog/data-sources.toml` after review.
///
/// INVARIANT (the never-provisions rule): `status` is ALWAYS `"planned"` and
/// `cron` is ALWAYS empty, whatever the proposed cadence — the reconciler only
/// schedules `live` sources with a cron, so nothing this app emits can start a
/// pipeline. Going live is a human edit, never machine output.
pub fn build_catalog_row(prompt: &str, primary: &Candidate, conf: u8) -> Source {
    Source {
        id: proposal_key(prompt),
        // No app crate exists for a proposed source; the reviewer wires one
        // (typically crawl + extractor with the proposed rule set).
        app: String::new(),
        market: String::new(),
        name: primary.name.clone(),
        url: primary.url.clone(),
        category: String::new(),
        engine: "http".into(),
        access: "public".into(),
        cadence: primary.cadence.clone(),
        cron: String::new(),      // never scheduled by this app
        status: "planned".into(), // never live from machine output
        confidence: conf,
        dataset: proposal_key(prompt),
        notes: format!("compiled by provisioner from prompt: {prompt}"),
        contract: None, // contracts are human-declared, never machine-proposed
    }
}

/// Renders the catalog row as a paste-ready `[[source]]` TOML fragment.
fn catalog_toml(row: &Source) -> String {
    #[derive(Serialize)]
    struct Frag<'a> {
        source: [&'a Source; 1],
    }
    toml::to_string(&Frag { source: [row] }).unwrap_or_default()
}

/// Assembles the full proposal record written to `provisioner/proposals`.
#[allow(clippy::too_many_arguments)]
pub fn build_proposal(
    prompt: &str,
    row: &Source,
    rule_set: &Value,
    seeds: &[String],
    budget_usd: Option<f64>,
    dry: &DryRun,
    iterations: u32,
    cost_usd: f64,
) -> Value {
    json!({
        "prompt": prompt,
        "catalog_row": serde_json::to_value(row).unwrap_or(Value::Null),
        "catalog_toml": catalog_toml(row),
        "rule_set": rule_set,
        "seeds": seeds,
        "cadence": row.cadence,
        "budget": budget_usd,
        "sample_stats": serde_json::to_value(dry).unwrap_or(Value::Null),
        "confidence": confidence(dry),
        "accepted": dry.accepted,
        "iterations": iterations,
        "cost_usd": cost_usd,
    })
}

/// Truncates a sampled body for the drafting prompt (char-boundary safe).
fn excerpt(body: &str, max: usize) -> String {
    if body.chars().count() <= max {
        return body.to_string();
    }
    body.chars().take(max).collect()
}

// ── the app ─────────────────────────────────────────────────────────────────

// ── durable execution ───────────────────────────────────────────────────────

/// The resumable unit of a compile (M23): everything the METERED discovery
/// stage produced. Stages after it (sampling, drafting) are free-tier or
/// re-derivable, and their inputs (page bodies) are far larger than the state
/// they would save — so the checkpoint stops here deliberately.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryCheckpoint {
    /// The prompt's proposal key. A checkpoint is only honored for a run
    /// compiling the SAME prompt — restoring another prompt's candidates would
    /// silently compile the wrong source.
    proposal_key: String,
    candidates: Vec<Candidate>,
    /// Research session to resume, so the drafting calls keep the discovery
    /// context instead of paying to rebuild it.
    #[serde(default)]
    session_id: Option<String>,
}

impl DiscoveryCheckpoint {
    /// Best-effort persist — a checkpoint that fails to write costs a repeated
    /// research call on re-claim, never the run.
    async fn save(&self, ctx: &AppContext) {
        match serde_json::to_value(self) {
            Ok(v) => {
                ctx.checkpoint_now(v).await;
            }
            Err(e) => tracing::warn!("provisioner checkpoint serialize failed: {e}"),
        }
    }

    /// Restores a checkpoint for THIS prompt, or `None` (fresh run, foreign
    /// prompt, or an unreadable blob — all of which simply re-run discovery).
    fn restore(ctx: &AppContext, proposal_key: &str) -> Option<Self> {
        Self::from_blob(ctx.restore(), proposal_key)
    }

    /// The restore decision, pure so it is testable without a runtime: a blob
    /// is honored only when it parses AND belongs to this prompt AND actually
    /// carries candidates.
    fn from_blob(blob: Option<&Value>, proposal_key: &str) -> Option<Self> {
        let cp: Self = serde_json::from_value(blob?.clone()).ok()?;
        (cp.proposal_key == proposal_key && !cp.candidates.is_empty()).then_some(cp)
    }
}

#[async_trait]
impl ScrapeApp for Provisioner {
    fn name(&self) -> &'static str {
        "provisioner"
    }

    fn description(&self) -> &'static str {
        "Compile a natural-language prompt into a reviewed provisioning PROPOSAL: \
         research 1-3 candidate source URLs, sample them via the tiered fetcher, \
         draft a declarative rule set and dry-run it through the real extraction \
         engine (iterating until a majority of fields match), then emit a \
         {catalog_row, rule_set, seeds, cadence, budget, sample_stats, confidence} \
         record into provisioner/proposals. NEVER writes data-sources.toml or \
         creates schedules — the emitted row is always status=planned with no cron; \
         a human applies it via the catalog reconciler. The metered discovery \
         stage is checkpointed, so a reaped/suspended compile resumes without \
         re-spending it. Params: {\"prompt\": \"...\", \"budget_usd\": 1.0, \
         \"max_iterations\": 2, \"sample_archive_max_age\": 86400}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["prompt"],
                "properties": {
                    "prompt": { "type": "string", "minLength": 1 },
                    "budget_usd": { "type": "number", "minimum": 0 },
                    "max_iterations": { "type": "integer", "minimum": 1, "maximum": 5 },
                    "sample_archive_max_age": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Archive-tier freshness window (seconds) for candidate \
                                        SAMPLING only; a snapshot no older than this may serve \
                                        the shape-learning fetch instead of hitting the origin. \
                                        Default 86400, 0 = live-only."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Compile a source proposal from one sentence, with a spend ceiling",
                    params: json!({
                        "prompt": "track Czech senior Rust developer salary listings weekly",
                        "budget_usd": 1.0
                    }),
                },
                ManifestExample {
                    description: "Allow an extra repair iteration for a tricky source",
                    params: json!({
                        "prompt": "monitor US federal fuel-price weekly averages",
                        "budget_usd": 2.0,
                        "max_iterations": 3
                    }),
                },
            ],
            output_shape: Some(
                "{proposal_key, candidates, seeds, iterations, accepted, confidence, \
                 sample_stats, cost_usd, resumed_discovery} — the full proposal record is \
                 upserted into provisioner/proposals (stamped with the primary sampled URL \
                 as its source) and saved as the proposal.json artifact; \
                 nothing is written to the catalog and no schedule is created",
            ),
            cost_class: CostClass::Claude,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let prompt = ctx.require_str("prompt")?.to_string();
        let budget_usd = ctx.params.get("budget_usd").and_then(Value::as_f64);
        let max_iterations = ctx
            .params
            .get("max_iterations")
            .and_then(Value::as_u64)
            .map(|n| (n as u32).clamp(1, ITERATIONS_CAP))
            .unwrap_or(DEFAULT_MAX_ITERATIONS);
        let sample_archive_max_age = ctx
            .params
            .get("sample_archive_max_age")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_SAMPLE_ARCHIVE_MAX_AGE_SECS);
        let mut cost_usd = 0.0f64;

        // Durable execution (M23): discovery is the one irreversibly METERED
        // stage of a compile — a reap or shutdown between it and the drafting
        // loop used to re-spend that research call from scratch. Its outcome
        // (the candidate list + the resumable research session) is checkpointed
        // and restored on re-claim; everything after it is free-tier work that
        // is cheaper to redo than to persist.
        let key = proposal_key(&prompt);
        let resumed = DiscoveryCheckpoint::restore(&ctx, &key);
        let resumed_discovery = resumed.is_some();

        // ── stage 1: discover candidate sources ─────────────────────────────
        let (mut session_id, candidates) = if let Some(cp) = resumed {
            tracing::info!(
                job = %ctx.job_id,
                candidates = cp.candidates.len(),
                "provisioner resumed from checkpoint: discovery research not re-spent"
            );
            (cp.session_id, cp.candidates)
        } else {
            let discovery_schema = json!({
            "type": "object",
            "required": ["candidates"],
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["url"],
                        "properties": {
                            "url": { "type": "string" },
                            "name": { "type": "string" },
                            "cadence": { "type": "string" },
                            "expected_fields": {
                                "type": "array", "items": { "type": "string" }
                            }
                        }
                    }
                }
            }
        });
        let mut discover = ResearchRequest::new(format!(
            "You are provisioning a web data source. For the goal below, identify \
             the 1-3 BEST public web pages that carry the data (listing/index pages \
             preferred over articles), what fields each yields, and how often the \
             data changes.\n\nGoal: {prompt}\n\nRespond with ONLY a JSON object: \
             {{\"candidates\": [{{\"url\": string, \"name\": string, \
             \"cadence\": \"daily|weekly|monthly|quarterly|annual|on-demand\", \
             \"expected_fields\": string[]}}]}}"
        ))
        .with_role("research");
        discover.max_budget_usd = budget_usd;
        discover.json_schema = Some(discovery_schema);
            let out = ctx.research(discover).await?;
            cost_usd += out.cost_usd.unwrap_or(0.0);
            let session_id = out.session_id.clone();
            let report = out
                .json
                .clone()
                .or_else(|| salvage_json(&out.text))
                .unwrap_or(Value::Null);
            let candidates = parse_candidates(&report);
            if candidates.is_empty() {
                return Err(Error::App(format!(
                    "provisioner: research produced no usable candidate URLs for prompt {prompt:?}"
                )));
            }
            // Checkpoint the metered stage's outcome BEFORE any further work.
            DiscoveryCheckpoint {
                proposal_key: key.clone(),
                candidates: candidates.clone(),
                session_id: session_id.clone(),
            }
            .save(&ctx)
            .await;
            (session_id, candidates)
        };

        // ── stage 2: sample candidates via the tiered fetcher ───────────────
        let mut seeds: Vec<String> = Vec::new();
        let mut bodies: Vec<String> = Vec::new();
        for (i, cand) in candidates.iter().enumerate() {
            let mut req = FetchRequest::new(&cand.url);
            req.strategy = FetchStrategy::Auto;
            // Sampling is shape-learning against an UNKNOWN third-party host —
            // exactly the case both cheap seams were built for: a learned API
            // recipe (M05) can replace a heavy render outright, and a recent
            // archive snapshot (M18) is an adequate shape sample. Both are
            // opportunistic: neither can terminate the ladder, so a miss simply
            // fetches live as before.
            req.use_recipes = true;
            req.archive_max_age = (sample_archive_max_age > 0).then_some(sample_archive_max_age);
            match ctx.fetch(req).await {
                Ok(mut outcome) => {
                    let body = outcome
                        .text
                        .take()
                        .or_else(|| outcome.markdown.take())
                        .unwrap_or_default();
                    if body.trim().is_empty() {
                        continue;
                    }
                    ctx.save_artifact(&format!("sample-{i}.html"), body.as_bytes())
                        .await?;
                    seeds.push(cand.url.clone());
                    bodies.push(body);
                }
                Err(e) => {
                    tracing::warn!(url = %cand.url, "provisioner sample fetch failed: {e}");
                }
            }
        }
        if bodies.is_empty() {
            return Err(Error::App(
                "provisioner: no candidate URL yielded a sampleable body".into(),
            ));
        }
        let primary = candidates
            .iter()
            .find(|c| Some(&c.url) == seeds.first())
            .unwrap_or(&candidates[0]);
        let expected = if primary.expected_fields.is_empty() {
            "fields you judge most valuable for the goal".to_string()
        } else {
            primary.expected_fields.join(", ")
        };

        // ── stage 3: draft rules and dry-run through the real engine ────────
        let rules_shape = "Respond with ONLY a JSON object mapping field names to \
             extraction rules. Rule shapes: {\"type\":\"css\",\"selector\":\"h1\"}, \
             {\"type\":\"css\",\"selector\":\"a\",\"attr\":\"href\"}, \
             {\"type\":\"regex\",\"pattern\":\"...\",\"group\":1}, \
             {\"type\":\"each\",\"selector\":\".card\",\"container\":\"#list\",\
             \"fields\":{...css/regex/const rules...}} (use `each` for repeating \
             listings), optional \"transforms\":[{\"op\":\"trim\"}|{\"op\":\"to_number\"}].";
        let mut last_rules_value: Option<Value> = None;
        let mut last_dry: Option<DryRun> = None;
        let mut feedback: Option<String> = None;
        let mut iterations = 0u32;
        while iterations < max_iterations {
            // Stop gracefully at the budget ceiling between metered calls; the
            // first call keeps the seam's own refusal behavior.
            if iterations > 0 {
                if let Some(remaining) = ctx.remaining_budget_usd().await? {
                    if remaining <= 0.0 {
                        break;
                    }
                }
            }
            iterations += 1;
            let draft_prompt = match &feedback {
                None => format!(
                    "Draft extraction rules for this goal: {prompt}\n\
                     Target fields: {expected}\n\n{rules_shape}\n\n\
                     Sampled page body from {url} (truncated):\n\n{body}",
                    url = primary.url,
                    body = excerpt(&bodies[0], SAMPLE_PROMPT_CAP_CHARS),
                ),
                Some(fb) => format!(
                    "Your drafted rules failed the dry run: {fb}\n\
                     Revise the rule set (same JSON shape, full object, all fields)."
                ),
            };
            let mut req = ResearchRequest::new(draft_prompt).with_role("research");
            req.max_budget_usd = budget_usd;
            req.resume_session = session_id.clone();
            let out = ctx.research(req).await?;
            cost_usd += out.cost_usd.unwrap_or(0.0);
            if out.session_id.is_some() {
                session_id = out.session_id.clone();
            }
            let Some(draft) = out.json.clone().or_else(|| salvage_json(&out.text)) else {
                feedback = Some("response was not a JSON object of rules".into());
                continue;
            };
            let rules: RuleSet = match serde_json::from_value(draft.clone()) {
                Ok(r) => r,
                Err(e) => {
                    feedback = Some(format!("rules did not parse: {e}"));
                    continue;
                }
            };
            match dry_run(&rules, &bodies) {
                Ok(dry) => {
                    let accepted = dry.accepted;
                    feedback = (!accepted).then(|| {
                        format!(
                            "{}/{} fields matched a majority of {} sampled docs; \
                             failing fields (worst first): {}",
                            dry.fields_matched,
                            dry.fields_total,
                            dry.docs,
                            dry.worst_fields.join(", ")
                        )
                    });
                    last_rules_value = Some(draft);
                    last_dry = Some(dry);
                    if accepted {
                        break;
                    }
                }
                Err(e) => {
                    feedback = Some(format!("rules failed to compile: {e}"));
                    last_rules_value = Some(draft);
                }
            }
        }
        let (rules_value, dry) = match (last_rules_value, last_dry) {
            (Some(r), Some(d)) => (r, d),
            _ => {
                return Err(Error::App(format!(
                    "provisioner: no runnable rule set after {iterations} iteration(s) \
                     (last feedback: {})",
                    feedback.as_deref().unwrap_or("none")
                )))
            }
        };

        // ── stage 4: emit the proposal (and ONLY the proposal) ──────────────
        let conf = confidence(&dry);
        let row = build_catalog_row(&prompt, primary, conf);
        let proposal = build_proposal(
            &prompt, &row, &rules_value, &seeds, budget_usd, &dry, iterations, cost_usd,
        );
        // Provenance (M12): a proposal is derived from ONE page — the primary
        // candidate whose sampled body the rules were drafted and dry-run
        // against — so that URL is a real per-record fact, not a guess. The
        // other sampled seeds are listed inside the record itself.
        //
        // `rules_hash` stays None on purpose: the drafted RuleSet is this
        // record's PAYLOAD, not the thing that extracted it. Stamping it would
        // claim a derivation that never happened (and the field is what the
        // replay path resolves against). `artifact_sha` likewise — the sample
        // artifacts are the inputs, not an archived copy of this record.
        let prov = Provenance {
            source_url: Some(primary.url.clone()),
            ..Provenance::default()
        };
        let change = ctx
            .upsert_with_provenance("proposals", &key, &proposal, prov)
            .await?;
        ctx.save_artifact("proposal.json", &serde_json::to_vec_pretty(&proposal)?)
            .await?;

        Ok(json!({
            "proposal_key": key,
            "change": format!("{change:?}"),
            "candidates": candidates,
            "seeds": seeds,
            "iterations": iterations,
            "accepted": dry.accepted,
            "confidence": conf,
            "sample_stats": serde_json::to_value(&dry)?,
            "cost_usd": cost_usd,
            "session_id": session_id,
            // Durable execution: true when this attempt reused a prior
            // attempt's discovery instead of re-spending the research call.
            "resumed_discovery": resumed_discovery,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::Catalog;

    #[test]
    fn discovery_checkpoint_round_trips_and_is_bound_to_its_prompt() {
        let key = proposal_key("track czech rust salaries");
        let cp = DiscoveryCheckpoint {
            proposal_key: key.clone(),
            candidates: vec![cand("https://a.example/list")],
            session_id: Some("sess-1".into()),
        };
        let blob = serde_json::to_value(&cp).unwrap();

        let back = DiscoveryCheckpoint::from_blob(Some(&blob), &key).expect("same prompt resumes");
        assert_eq!(back.candidates, cp.candidates);
        assert_eq!(back.session_id.as_deref(), Some("sess-1"));

        // A checkpoint from a DIFFERENT prompt must never be adopted — it would
        // silently compile the wrong source.
        assert!(DiscoveryCheckpoint::from_blob(Some(&blob), &proposal_key("something else")).is_none());
    }

    #[test]
    fn unusable_checkpoints_fall_back_to_a_fresh_discovery() {
        let key = proposal_key("p");
        // Fresh run, corrupt blob, and an empty-candidate blob all mean "run
        // discovery" — a checkpoint must never be able to fail the job.
        assert!(DiscoveryCheckpoint::from_blob(None, &key).is_none());
        assert!(DiscoveryCheckpoint::from_blob(Some(&json!("garbage")), &key).is_none());
        assert!(DiscoveryCheckpoint::from_blob(
            Some(&json!({ "proposal_key": key, "candidates": [] })),
            &key
        )
        .is_none());
    }

    fn cand(url: &str) -> Candidate {
        Candidate {
            url: url.into(),
            name: "Test Source".into(),
            cadence: "weekly".into(),
            expected_fields: vec!["title".into(), "price".into()],
        }
    }

    /// A realistic listing-page fixture the dry-run loop iterates against.
    const FIXTURE: &str = r#"
        <html><body><h1>Widget Prices</h1>
        <div id="list">
            <div class="card"><h3>Alpha</h3><span class="price">$10</span></div>
            <div class="card"><h3>Beta</h3><span class="price">$20</span></div>
        </div></body></html>"#;

    fn ruleset(v: Value) -> RuleSet {
        serde_json::from_value(v).unwrap()
    }

    // ── discovery parsing ───────────────────────────────────────────────────

    #[test]
    fn candidates_parse_defensively_cap_dedupe_and_drop_bad_urls() {
        let v = json!({"candidates": [
            {"url": "https://a.example/jobs", "name": "A", "cadence": "daily",
             "expected_fields": ["title", " salary ", ""]},
            {"url": "https://a.example/jobs"},                  // duplicate
            {"url": "ftp://b.example"},                          // bad scheme
            {"name": "no url at all"},
            {"url": "https://c.example", "cadence": "hourly-ish"}, // unknown cadence
            {"url": "https://d.example"},
            {"url": "https://e.example"},                        // over the cap
        ]});
        let got = parse_candidates(&v);
        assert_eq!(got.len(), MAX_CANDIDATES);
        assert_eq!(got[0].url, "https://a.example/jobs");
        assert_eq!(got[0].cadence, "daily");
        assert_eq!(got[0].expected_fields, vec!["title", "salary"]);
        // Unknown cadence normalizes to the conservative default.
        assert_eq!(got[1].cadence, "weekly");
        assert_eq!(got[2].url, "https://d.example");
    }

    #[test]
    fn unparseable_discovery_yields_empty_not_panic() {
        assert!(parse_candidates(&json!("prose")).is_empty());
        assert!(parse_candidates(&json!({"candidates": "nope"})).is_empty());
        assert!(parse_candidates(&Value::Null).is_empty());
    }

    // ── dry-run harness (the LLM boundary stubbed: drafts are fixtures) ─────

    #[test]
    fn good_draft_is_accepted_by_the_dry_run() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "items": {"type": "each", "selector": ".card", "container": "#list",
                      "fields": {"name": {"type": "css", "selector": "h3"},
                                 "price": {"type": "css", "selector": ".price",
                                           "transforms": [{"op": "to_number"}]}}}
        }));
        let dry = dry_run(&rules, &[FIXTURE.to_string()]).unwrap();
        assert!(dry.accepted);
        assert_eq!(dry.fields_matched, 2);
        assert!(dry.worst_fields.is_empty());
        assert_eq!(confidence(&dry), 100);
    }

    #[test]
    fn majority_miss_is_rejected_and_names_the_worst_fields() {
        // 1 of 3 fields binds — no majority.
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "author": {"type": "css", "selector": ".author"},
            "date": {"type": "css", "selector": "time"}
        }));
        let dry = dry_run(&rules, &[FIXTURE.to_string()]).unwrap();
        assert!(!dry.accepted);
        assert_eq!(dry.fields_matched, 1);
        assert_eq!(dry.worst_fields, vec!["author".to_string(), "date".into()]);
    }

    #[test]
    fn exactly_half_matched_is_not_a_majority() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "missing": {"type": "css", "selector": ".nope"}
        }));
        let dry = dry_run(&rules, &[FIXTURE.to_string()]).unwrap();
        assert!(!dry.accepted, "strict majority: 1/2 must not pass");
    }

    #[test]
    fn uncompilable_draft_is_feedback_not_a_scored_zero() {
        let rules = ruleset(json!({"x": {"type": "css", "selector": ":::"}}));
        assert!(dry_run(&rules, &[FIXTURE.to_string()]).is_err());
        // And an empty rule object is equally a loud error.
        assert!(dry_run(&ruleset(json!({})), &[FIXTURE.to_string()]).is_err());
    }

    #[test]
    fn iteration_loop_converges_on_the_repaired_draft() {
        // The dry-run loop with the LLM stubbed as a fixture sequence, the way
        // research's tests stub its engine boundary: draft 1 fails (feedback
        // names the broken field), draft 2 — "repaired" — is accepted. Two
        // iterations, matching max_iterations' default.
        let bodies = vec![FIXTURE.to_string()];
        let draft1 = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "name": {"type": "css", "selector": ".product-name"}, // wrong
            "price": {"type": "css", "selector": ".cost"}         // wrong
        }));
        let first = dry_run(&draft1, &bodies).unwrap();
        assert!(!first.accepted);
        assert_eq!(first.worst_fields, vec!["name".to_string(), "price".into()]);

        let draft2 = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "name": {"type": "css", "selector": ".card h3", "all": true},
            "price": {"type": "css", "selector": ".card .price", "all": true}
        }));
        let second = dry_run(&draft2, &bodies).unwrap();
        assert!(second.accepted, "repaired draft must stop the loop");
        assert_eq!(second.fields_matched, 3);
    }

    #[test]
    fn confidence_reflects_partial_binding() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "missing": {"type": "css", "selector": ".nope"}
        }));
        let dry = dry_run(&rules, &[FIXTURE.to_string()]).unwrap();
        assert_eq!(confidence(&dry), 50);
    }

    // ── proposal record ─────────────────────────────────────────────────────

    #[test]
    fn proposal_carries_the_full_promised_shape() {
        let rules = ruleset(json!({"heading": {"type": "css", "selector": "h1"}}));
        let dry = dry_run(&rules, &[FIXTURE.to_string()]).unwrap();
        let c = cand("https://a.example/widgets");
        let row = build_catalog_row("track widget prices weekly", &c, confidence(&dry));
        let p = build_proposal(
            "track widget prices weekly",
            &row,
            &json!({"heading": {"type": "css", "selector": "h1"}}),
            &["https://a.example/widgets".to_string()],
            Some(1.0),
            &dry,
            2,
            0.42,
        );
        for key in [
            "prompt", "catalog_row", "catalog_toml", "rule_set", "seeds", "cadence",
            "budget", "sample_stats", "confidence", "accepted", "iterations", "cost_usd",
        ] {
            assert!(p.get(key).is_some(), "proposal missing key {key}");
        }
        assert_eq!(p["cadence"], json!("weekly"));
        assert_eq!(p["confidence"], json!(100));
        assert_eq!(p["seeds"], json!(["https://a.example/widgets"]));
        // The catalog row is TOML-shaped: it round-trips through the real
        // catalog parser.
        let parsed = Catalog::parse(p["catalog_toml"].as_str().unwrap()).unwrap();
        assert_eq!(parsed.sources.len(), 1);
        assert_eq!(parsed.sources[0].url, "https://a.example/widgets");
    }

    #[test]
    fn never_writes_catalog_invariant_row_is_inert_and_app_is_unscheduled() {
        // The app itself is on-demand: no schedule, ever.
        assert!(Provisioner.schedule().is_none());
        // Whatever cadence the research proposes, the emitted row is planned
        // with no cron — parsed through the REAL catalog machinery, it is
        // invisible to `live()` and unschedulable, so even pasting the
        // fragment verbatim into data-sources.toml starts nothing.
        for cadence in ["daily", "weekly", "on-demand"] {
            let mut c = cand("https://a.example");
            c.cadence = cadence.into();
            let row = build_catalog_row("some prompt", &c, 90);
            assert_eq!(row.status, "planned");
            assert_eq!(row.cron, "");
            assert!(!row.is_scheduled());
            let toml = catalog_toml(&row);
            let parsed = Catalog::parse(&toml).unwrap();
            assert_eq!(parsed.live().count(), 0, "a proposal must never be live");
        }
    }

    #[test]
    fn proposal_key_is_stable_and_sanitized() {
        assert_eq!(
            proposal_key("Track Czech senior Rust salaries, weekly!"),
            "track-czech-senior-rust-salaries-weekly"
        );
        assert_eq!(proposal_key("a"), "a");
        assert_eq!(proposal_key("???"), "proposal");
        // Deterministic: same prompt, same key (upsert = revision, not dupe).
        assert_eq!(proposal_key("x y"), proposal_key("x y"));
        assert!(proposal_key(&"long ".repeat(50)).len() <= KEY_CAP_CHARS);
    }
}
