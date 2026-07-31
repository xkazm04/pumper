//! Shared layer for the four agentic US-trades reference apps (trade-wages,
//! homewyse-pricing, state-tax, valuation-multiples).
//!
//! Concerns live here so they stay consistent across all four apps:
//!   - [`research_json`]: the whole metered research → archive → parse-or-salvage
//!     step every one of them opens with.
//!   - [`salvage_json`]: recover a JSON object the agent emitted but the engine
//!     couldn't parse (markdown fence / surrounding prose). One pass, no re-run,
//!     no cost — it works on text already paid for.
//!   - [`validate`]: plausibility guards (monotone bands, rate ranges, positive
//!     magnitudes) so a nonsensical record is rejected with per-record detail
//!     instead of silently upserted.

use pumper_core::{
    salvage_json, AppContext, Error, Provenance, ResearchOutput, ResearchRequest, Result,
};
use serde_json::{json, Value};

/// Runs a metered research request, archives the raw answer as `research.json`,
/// and returns its JSON alongside the raw output (which the caller still needs
/// for cost/duration reporting).
///
/// Prefers the schema-validated `output.json`, salvaging a fenced/prose-wrapped
/// object from the raw text before giving up — one pass, no metered re-run.
/// `app` names the caller in the error.
///
/// All four agentic trades apps open with exactly this; copy-pasting it four
/// times let the artifact name, the salvage fallback and the error shape drift
/// independently.
pub async fn research_json(
    ctx: &AppContext,
    app: &str,
    request: ResearchRequest,
) -> Result<(Value, ResearchOutput)> {
    research_json_named(ctx, app, request, "research.json").await
}

/// [`research_json`] with an explicit artifact name — for apps that make more
/// than one metered call per job (e.g. state-licensing's per-trade chunking):
/// each call's raw answer lands in its own artifact instead of the calls
/// silently overwriting one shared `research.json`.
pub async fn research_json_named(
    ctx: &AppContext,
    app: &str,
    request: ResearchRequest,
    artifact_name: &str,
) -> Result<(Value, ResearchOutput)> {
    // Metered seam: records a cost event against the job, honors budget_usd, and
    // serves identical re-runs from the research cache (see core/app.rs).
    let output = ctx.research(request).await?;

    let artifact = match &output.json {
        Some(j) => serde_json::to_vec_pretty(j)?,
        None => output.text.clone().into_bytes(),
    };
    ctx.save_artifact(artifact_name, &artifact).await?;

    let data = match output.json.clone() {
        Some(j) => j,
        None => salvage_json(&output.text).ok_or_else(|| {
            Error::App(format!(
                "{app}: agent did not return JSON (text starts: {})",
                output.text.chars().take(160).collect::<String>()
            ))
        })?,
    };
    Ok((data, output))
}

/// **Provenance (M12) for an agentic record**: registers the *derivation spec*
/// that produced this run's answer — the exact prompt, the structured-output
/// schema, and the model/effort the operator pinned — in the content-addressed
/// rules registry, and returns its hash as [`Provenance::rules_hash`].
///
/// For a scraper the RuleSet is a set of selectors; for these apps it is the
/// prompt + `--json-schema` contract, which is precisely the thing that has to
/// be recovered to explain (or re-derive) a stored figure after the live prompt
/// moves on — and precisely the thing that silently changes between vintages.
/// The hash is a content hash of registered JSON, not an assertion about the
/// sources the agent visited.
///
/// `source_url` is deliberately left `None`: an agentic answer is synthesized
/// from many pages the app never sees, so any single URL here would be a
/// fabrication. A registry write failure is warn-logged and degrades to an
/// unstamped write — provenance is metadata and must never fail a paid run.
pub async fn research_provenance(
    ctx: &AppContext,
    app: &str,
    request: &ResearchRequest,
) -> Provenance {
    let spec = json!({
        "kind": "agentic_research",
        "app": app,
        "prompt": request.prompt,
        "role": request.role,
        "json_schema": request.json_schema,
        "model": request.model,
        "effort": request.effort,
    });
    match ctx.register_rules(&spec).await {
        Ok(hash) => Provenance {
            rules_hash: Some(hash),
            ..Provenance::default()
        },
        Err(e) => {
            tracing::warn!(app, "research derivation-spec registration failed: {e}");
            Provenance::default()
        }
    }
}

/// The `year` param an agentic trades app was refreshed for. Central so the four
/// apps parse the vintage identically.
pub fn year_param<'a>(ctx: &'a AppContext, default: &'a str) -> &'a str {
    ctx.params
        .get("year")
        .and_then(Value::as_str)
        .unwrap_or(default)
}

/// Whether a re-run is being forced (`force: true`), bypassing every freshness gate.
pub fn forced(ctx: &AppContext) -> bool {
    ctx.params
        .get("force")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// **Vintage freshness gate** for the frozen-fact apps (`state-tax`,
/// `trade-wages`): true when the app already holds a record at `sentinel_key`
/// whose stored `year` equals `year` — i.e. re-deriving would re-pay a 25-30 turn
/// agentic run to reproduce constants that were fixed when the IRS / BLS
/// published them. `force: true` always returns false (re-run). Returns
/// `Ok(false)` when nothing is held yet.
pub async fn vintage_held(
    ctx: &AppContext,
    app: &str,
    dataset: &str,
    sentinel_key: &str,
    year: &str,
) -> Result<bool> {
    if forced(ctx) {
        return Ok(false);
    }
    let held = ctx
        .datasets
        .get(app, dataset, sentinel_key)
        .await?
        .and_then(|r| {
            r.data
                .get("year")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    Ok(held.as_deref() == Some(year))
}

/// **Age freshness gate** for the apps whose figures drift within a year
/// (`homewyse-pricing`, `valuation-multiples`): true when the app holds a record
/// at `sentinel_key` younger than `max_age_days`. `force: true` always returns
/// false. Returns `Ok(false)` when nothing is held yet.
pub async fn fresh_by_age(
    ctx: &AppContext,
    app: &str,
    dataset: &str,
    sentinel_key: &str,
    max_age_days: i64,
) -> Result<bool> {
    if forced(ctx) {
        return Ok(false);
    }
    let age = ctx
        .datasets
        .get(app, dataset, sentinel_key)
        .await?
        .map(|r| (chrono::Utc::now() - r.updated_at).num_days().max(0));
    Ok(age.is_some_and(|a| a < max_age_days))
}

/// Age freshness gate scoped to records matching `path == value` (e.g. one
/// locality) — for `homewyse-pricing`, whose keys are per-locality so a whole-
/// dataset "newest" check would let a Texas run wrongly satisfy a national one.
/// True when the newest matching record is younger than `max_age_days`.
pub async fn fresh_by_age_where(
    ctx: &AppContext,
    app: &str,
    dataset: &str,
    path: &str,
    value: &str,
    max_age_days: i64,
) -> Result<bool> {
    if forced(ctx) {
        return Ok(false);
    }
    let filter = [pumper_core::datasets::JsonFilter::Eq {
        path: path.to_string(),
        value: value.to_string(),
    }];
    let recs = ctx
        .datasets
        .list_filtered(app, dataset, &filter, None, 1)
        .await?;
    let age = recs
        .first()
        .map(|r| (chrono::Utc::now() - r.updated_at).num_days().max(0));
    Ok(age.is_some_and(|a| a < max_age_days))
}

/// Reads the `max_age_days` param (default `default_days`), clamped to `>= 0`.
pub fn max_age_days(ctx: &AppContext, default_days: i64) -> i64 {
    ctx.params
        .get("max_age_days")
        .and_then(Value::as_i64)
        .map(|d| d.max(0))
        .unwrap_or(default_days)
}

/// Plausibility validation for parsed trades records. These are cheap sanity
/// gates — NOT a re-run loop: a record that fails is rejected (with reasons)
/// and reported in the job result; valid siblings still upsert. The agent's
/// answer is already paid for, so there is no retry.
pub mod validate {
    use serde_json::Value;

    /// A rejected record: its dataset key and the plausibility reasons it failed.
    #[derive(Debug, Clone)]
    pub struct Rejection {
        pub key: String,
        pub reasons: Vec<String>,
    }

    impl Rejection {
        pub fn to_json(&self) -> Value {
            serde_json::json!({ "key": self.key, "reasons": self.reasons })
        }
    }

    /// Numeric field accessor tolerant of JSON numbers and numeric strings
    /// (the agent sometimes quotes a figure, e.g. `"30.10"`).
    pub fn num(rec: &Value, field: &str) -> Option<f64> {
        match rec.get(field) {
            Some(Value::Number(n)) => n.as_f64(),
            Some(Value::String(s)) => s.trim().replace([',', '$'], "").parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Push a violation if the ordering low ≤ median ≤ high is broken. Values
    /// that are absent are skipped — presence is a schema concern, not a
    /// plausibility one — but any present pair must be ordered.
    pub fn require_monotone(
        reasons: &mut Vec<String>,
        label: &str,
        low: Option<f64>,
        median: Option<f64>,
        high: Option<f64>,
    ) {
        if let (Some(l), Some(m)) = (low, median) {
            if l > m {
                reasons.push(format!("{label}: low {l} > median {m}"));
            }
        }
        if let (Some(m), Some(h)) = (median, high) {
            if m > h {
                reasons.push(format!("{label}: median {m} > high {h}"));
            }
        }
        if let (Some(l), Some(h)) = (low, high) {
            if l > h {
                reasons.push(format!("{label}: low {l} > high {h}"));
            }
        }
    }

    /// Push a violation if the value is present and not strictly positive.
    pub fn require_positive(reasons: &mut Vec<String>, label: &str, v: Option<f64>) {
        if let Some(v) = v {
            if v <= 0.0 {
                reasons.push(format!("{label}: {v} not > 0"));
            }
        }
    }

    /// Push a violation if the value is present and negative. For magnitudes
    /// where zero is a legitimate answer (a $0 license fee in a no-license
    /// state, a $0 bond where none is required) — unlike [`require_positive`],
    /// which treats 0 as implausible.
    pub fn require_nonnegative(reasons: &mut Vec<String>, label: &str, v: Option<f64>) {
        if let Some(v) = v {
            if v < 0.0 {
                reasons.push(format!("{label}: {v} < 0"));
            }
        }
    }

    /// Push a violation if the value is present and above `max` — a coarse
    /// sanity ceiling for agent-returned dollar magnitudes (a $80M "license
    /// fee" is a parse or hallucination, not a fact).
    pub fn require_at_most(reasons: &mut Vec<String>, label: &str, v: Option<f64>, max: f64) {
        if let Some(v) = v {
            if v > max {
                reasons.push(format!("{label}: {v} > plausibility cap {max}"));
            }
        }
    }

    /// Push a violation if the value is present and outside the percentage
    /// range [0, 100].
    pub fn require_rate(reasons: &mut Vec<String>, label: &str, v: Option<f64>) {
        if let Some(v) = v {
            if !(0.0..=100.0).contains(&v) {
                reasons.push(format!("{label}: rate {v} outside [0,100]"));
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[test]
        fn num_reads_numbers_and_numeric_strings() {
            let rec = json!({ "a": 30.1, "b": "1,200", "c": "$45.5", "d": "x" });
            assert_eq!(num(&rec, "a"), Some(30.1));
            assert_eq!(num(&rec, "b"), Some(1200.0));
            assert_eq!(num(&rec, "c"), Some(45.5));
            assert_eq!(num(&rec, "d"), None);
            assert_eq!(num(&rec, "missing"), None);
        }

        #[test]
        fn monotone_flags_out_of_order_bands() {
            let mut r = Vec::new();
            require_monotone(&mut r, "band", Some(1.0), Some(2.0), Some(3.0));
            assert!(r.is_empty());
            require_monotone(&mut r, "band", Some(5.0), Some(2.0), Some(3.0));
            assert_eq!(r.len(), 2); // low>median and low>high
        }

        #[test]
        fn monotone_skips_absent_values() {
            let mut r = Vec::new();
            require_monotone(&mut r, "band", None, Some(2.0), None);
            assert!(r.is_empty());
        }

        #[test]
        fn positive_flags_zero_and_negative() {
            let mut r = Vec::new();
            require_positive(&mut r, "wage", Some(10.0));
            assert!(r.is_empty());
            require_positive(&mut r, "wage", Some(0.0));
            require_positive(&mut r, "wage", Some(-1.0));
            assert_eq!(r.len(), 2);
        }

        #[test]
        fn nonnegative_allows_zero_flags_negative() {
            let mut r = Vec::new();
            require_nonnegative(&mut r, "fee", Some(0.0));
            require_nonnegative(&mut r, "fee", Some(250.0));
            require_nonnegative(&mut r, "fee", None);
            assert!(r.is_empty());
            require_nonnegative(&mut r, "fee", Some(-5.0));
            assert_eq!(r.len(), 1);
        }

        #[test]
        fn at_most_flags_values_over_the_cap() {
            let mut r = Vec::new();
            require_at_most(&mut r, "bond", Some(15_000.0), 5_000_000.0);
            require_at_most(&mut r, "bond", None, 5_000_000.0);
            assert!(r.is_empty());
            require_at_most(&mut r, "bond", Some(80_000_000.0), 5_000_000.0);
            assert_eq!(r.len(), 1);
        }

        #[test]
        fn rate_flags_out_of_range() {
            let mut r = Vec::new();
            require_rate(&mut r, "top", Some(0.0));
            require_rate(&mut r, "top", Some(13.3));
            require_rate(&mut r, "top", Some(100.0));
            assert!(r.is_empty());
            require_rate(&mut r, "top", Some(-1.0));
            require_rate(&mut r, "top", Some(133.0));
            assert_eq!(r.len(), 2);
        }
    }
}

/// Canonical trade taxonomy: the five home-services trades pumper covers,
/// with a stable label + BLS SOC code, and a normalizer that maps the many
/// phrasings a model returns ("plumber", "Plumbing services", "HVAC/R") onto
/// one canonical label. Used by the trade-keyed apps for prompt construction
/// and record keys so phrasing drift can't mint duplicate keys or defeat
/// change detection.
pub mod taxonomy {
    use pumper_core::{AppContext, Result};
    use serde_json::{json, Value};

    /// Virtual app namespace + dataset holding the governed taxonomy registry.
    /// Records are keyed by canonical label (`Plumbing`), shape:
    /// `{trade, soc_code, naics: [..], aliases: [..], enabled, source}` where
    /// `source` is `"seed" | "proposed" | "approved"`. The compile-time enum
    /// below stays the FALLBACK: when this dataset is absent or empty, every
    /// accessor behaves exactly as the enum always has.
    pub const TAXONOMY_APP: &str = "trades";
    pub const TAXONOMY_DATASET: &str = "taxonomy";
    /// Read cap for the registry — far past any plausible trade count (~25-50).
    const TAXONOMY_READ_LIMIT: i64 = 1_000;

    /// A canonical home-services trade.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Trade {
        Plumbing,
        Electrical,
        Hvac,
        Landscaping,
        PoolService,
    }

    impl Trade {
        /// Every trade, in the canonical prompt order.
        pub const ALL: [Trade; 5] = [
            Trade::Plumbing,
            Trade::Electrical,
            Trade::Hvac,
            Trade::Landscaping,
            Trade::PoolService,
        ];

        /// The canonical display label — the stable string used in record keys.
        pub fn label(self) -> &'static str {
            match self {
                Trade::Plumbing => "Plumbing",
                Trade::Electrical => "Electrical",
                Trade::Hvac => "HVAC",
                Trade::Landscaping => "Landscaping",
                Trade::PoolService => "Pool service",
            }
        }

        /// Best-fit BLS SOC occupation code (Landscaping and Pool service share
        /// 37-3011 — the closest OEWS occupation for both).
        pub fn soc_code(self) -> &'static str {
            match self {
                Trade::Plumbing => "47-2152",
                Trade::Electrical => "47-2111",
                Trade::Hvac => "49-9021",
                Trade::Landscaping => "37-3011",
                Trade::PoolService => "37-3011",
            }
        }

        /// The 6-digit NAICS 2017 codes the trade's businesses file under.
        /// Plumbing & HVAC are fused in 238220 (Census cannot split them);
        /// pool service falls under the broader 561790.
        pub fn naics(self) -> &'static [&'static str] {
            match self {
                Trade::Plumbing => &["238220"],
                Trade::Electrical => &["238210"],
                Trade::Hvac => &["238220"],
                Trade::Landscaping => &["561730"],
                Trade::PoolService => &["561790"],
            }
        }

        /// Lowercase keyword aliases — the same keywords [`Trade::from_label`]
        /// matches on, exposed as data so seed registry records carry them.
        pub fn aliases(self) -> &'static [&'static str] {
            match self {
                Trade::Plumbing => &["plumb"],
                Trade::Electrical => &["electric"],
                Trade::Hvac => &["hvac", "heating", "air condition", "cooling"],
                Trade::Landscaping => &["landscap", "lawn", "groundskeep", "yard"],
                Trade::PoolService => &["pool"],
            }
        }

        /// Normalize a model-returned trade name onto a canonical trade. Matches
        /// on keywords so variants ("plumber", "Electrical services", "HVAC/R",
        /// "lawn care") all resolve. Returns None for genuinely unknown labels —
        /// the caller keeps the raw string and flags it.
        pub fn from_label(s: &str) -> Option<Trade> {
            let l = s.trim().to_lowercase();
            if l.is_empty() {
                return None;
            }
            if l.contains("plumb") {
                Some(Trade::Plumbing)
            } else if l.contains("electric") {
                Some(Trade::Electrical)
            } else if l.contains("hvac")
                || l.contains("heating")
                || l.contains("air condition")
                || l.contains("cooling")
            {
                Some(Trade::Hvac)
            } else if l.contains("pool") {
                Some(Trade::PoolService)
            } else if l.contains("landscap")
                || l.contains("lawn")
                || l.contains("groundskeep")
                || l.contains("yard")
            {
                Some(Trade::Landscaping)
            } else {
                None
            }
        }
    }

    /// Resolve a raw model label to `(canonical_label, is_known)`. Unknown labels
    /// keep the raw string (never fabricated) so nothing is silently dropped.
    pub fn canonicalize(raw: &str) -> (String, bool) {
        match Trade::from_label(raw) {
            Some(t) => (t.label().to_string(), true),
            None => (raw.trim().to_string(), false),
        }
    }

    /// Comma-joined canonical labels for prompt construction — the single source
    /// of the trade list that used to be re-typed in each app's prompt string.
    pub fn prompt_list() -> String {
        Trade::ALL
            .iter()
            .map(|t| t.label())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// One entry of the governed trade taxonomy — either a compile-time seed
    /// (mirroring [`Trade`]) or a registry record a human enabled.
    #[derive(Debug, Clone, PartialEq)]
    pub struct TradeEntry {
        /// Canonical display label — the stable string used in record keys.
        pub label: String,
        /// Best-fit BLS SOC occupation code.
        pub soc_code: String,
        /// 6-digit NAICS 2017 codes for the trade's businesses.
        pub naics: Vec<String>,
        /// Lowercase keyword aliases for canonicalization.
        pub aliases: Vec<String>,
        /// `"seed" | "proposed" | "approved"`.
        pub source: String,
    }

    impl TradeEntry {
        fn from_trade(t: Trade) -> TradeEntry {
            TradeEntry {
                label: t.label().to_string(),
                soc_code: t.soc_code().to_string(),
                naics: t.naics().iter().map(|s| s.to_string()).collect(),
                aliases: t.aliases().iter().map(|s| s.to_string()).collect(),
                source: "seed".to_string(),
            }
        }
    }

    /// The compile-time fallback taxonomy: the five enum trades as entries, in
    /// canonical prompt order.
    pub fn seed_entries() -> Vec<TradeEntry> {
        Trade::ALL.iter().copied().map(TradeEntry::from_trade).collect()
    }

    /// The seed entry as a `trades/taxonomy` dataset record (enabled, source
    /// `"seed"`). Used to materialize the registry the first time a proposer
    /// run touches it.
    pub fn seed_record(t: Trade) -> (String, Value) {
        let e = TradeEntry::from_trade(t);
        (
            e.label.clone(),
            json!({
                "trade": e.label,
                "soc_code": e.soc_code,
                "naics": e.naics,
                "aliases": e.aliases,
                "enabled": true,
                "source": "seed",
            }),
        )
    }

    /// Parse one registry record into an entry. `None` when the record is
    /// unusable (no label) — a malformed row must degrade to "ignored", never
    /// poison the whole taxonomy.
    fn parse_entry(data: &Value) -> Option<(TradeEntry, bool)> {
        let label = data.get("trade").and_then(Value::as_str)?.trim().to_string();
        if label.is_empty() {
            return None;
        }
        let strings = |field: &str| -> Vec<String> {
            data.get(field)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.trim().to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default()
        };
        let entry = TradeEntry {
            label: label.clone(),
            soc_code: data
                .get("soc_code")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            naics: data
                .get("naics")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
            aliases: strings("aliases"),
            source: data
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("proposed")
                .to_string(),
        };
        // Governance default: a record must OPT IN with enabled:true. Absent
        // or false ⇒ not part of the live taxonomy (proposer writes false).
        let enabled = data.get("enabled").and_then(Value::as_bool).unwrap_or(false);
        Some((entry, enabled))
    }

    /// Pure merge of registry records over the compile-time seeds:
    /// - dataset ABSENT/EMPTY ⇒ exactly [`seed_entries`] (zero behavior change);
    /// - a record matching a seed label overrides that seed's fields when
    ///   enabled, and REMOVES the trade when explicitly `enabled: false`
    ///   (governed off-switch);
    /// - enabled records for new trades are appended, sorted by label;
    /// - disabled/malformed extra records are ignored.
    pub fn merge_taxonomy(records: &[Value]) -> Vec<TradeEntry> {
        let mut seeds = seed_entries();
        if records.is_empty() {
            return seeds;
        }
        let parsed: Vec<(TradeEntry, bool)> =
            records.iter().filter_map(parse_entry).collect();
        // Override / remove seeds by label.
        let mut out: Vec<TradeEntry> = Vec::new();
        for seed in seeds.drain(..) {
            match parsed.iter().find(|(e, _)| e.label == seed.label) {
                Some((e, true)) => {
                    // Enabled override: registry fields win, but empty fields
                    // fall back to the seed's (a sparse row must not erase
                    // the SOC/NAICS the enum already knows).
                    out.push(TradeEntry {
                        label: seed.label.clone(),
                        soc_code: if e.soc_code.is_empty() {
                            seed.soc_code.clone()
                        } else {
                            e.soc_code.clone()
                        },
                        naics: if e.naics.is_empty() {
                            seed.naics.clone()
                        } else {
                            e.naics.clone()
                        },
                        aliases: if e.aliases.is_empty() {
                            seed.aliases.clone()
                        } else {
                            e.aliases.clone()
                        },
                        source: e.source.clone(),
                    });
                }
                Some((_, false)) => {} // governed off-switch
                None => out.push(seed),
            }
        }
        // Append enabled NEW trades, deterministic order.
        let mut extra: Vec<TradeEntry> = parsed
            .into_iter()
            .filter(|(e, enabled)| *enabled && !out.iter().any(|s| s.label == e.label))
            .filter(|(e, _)| !Trade::ALL.iter().any(|t| t.label() == e.label))
            .map(|(e, _)| e)
            .collect();
        extra.sort_by(|a, b| a.label.cmp(&b.label));
        out.extend(extra);
        out
    }

    /// The live trade taxonomy: the `trades/taxonomy` registry merged over the
    /// compile-time enum seeds. When the registry dataset is absent or empty
    /// this returns exactly the five enum trades — the enum is the permanent
    /// fallback, so existing callers see zero behavior change until a human
    /// enables registry records.
    pub async fn taxonomy(ctx: &AppContext) -> Result<Vec<TradeEntry>> {
        let recs = ctx
            .datasets
            .list(TAXONOMY_APP, TAXONOMY_DATASET, TAXONOMY_READ_LIMIT)
            .await?;
        let data: Vec<Value> = recs.into_iter().map(|r| r.data).collect();
        Ok(merge_taxonomy(&data))
    }

    /// Comma-joined labels of a taxonomy slice — the registry-aware
    /// counterpart of [`prompt_list`]. Identical output when only seeds exist.
    pub fn prompt_list_of(entries: &[TradeEntry]) -> String {
        entries
            .iter()
            .map(|e| e.label.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Registry-aware [`canonicalize`]: the enum matcher runs FIRST (exact
    /// legacy semantics for the five seeds, including its keyword precedence),
    /// then enabled registry entries' aliases catch new trades. Unknown labels
    /// keep the raw string, flagged.
    pub fn canonicalize_in(entries: &[TradeEntry], raw: &str) -> (String, bool) {
        if let Some(t) = Trade::from_label(raw) {
            return (t.label().to_string(), true);
        }
        let l = raw.trim().to_lowercase();
        if !l.is_empty() {
            for e in entries {
                if e.aliases.iter().any(|a| l.contains(a.as_str())) {
                    return (e.label.clone(), true);
                }
            }
        }
        (raw.trim().to_string(), false)
    }

    /// Deduped NAICS code list of a taxonomy slice, truncated to `prefix_len`
    /// digits (6 = CBP grain, 4 = nonemployer grain, 2 = sector grain), in
    /// entry order. With only seeds this reproduces each census app's
    /// compile-time default code set.
    pub fn naics_prefixes(entries: &[TradeEntry], prefix_len: usize) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for e in entries {
            for code in &e.naics {
                let p: String = code.chars().take(prefix_len).collect();
                if !p.is_empty() && !out.contains(&p) {
                    out.push(p);
                }
            }
        }
        out
    }

    /// NAICS codes from the REGISTRY for the census apps: `None` when the
    /// registry dataset is absent/empty (caller keeps its compile-time
    /// defaults — zero behavior change), `Some(codes)` otherwise.
    pub async fn registry_naics(
        ctx: &AppContext,
        prefix_len: usize,
    ) -> Result<Option<Vec<String>>> {
        let recs = ctx
            .datasets
            .list(TAXONOMY_APP, TAXONOMY_DATASET, TAXONOMY_READ_LIMIT)
            .await?;
        if recs.is_empty() {
            return Ok(None);
        }
        let data: Vec<Value> = recs.into_iter().map(|r| r.data).collect();
        let codes = naics_prefixes(&merge_taxonomy(&data), prefix_len);
        Ok(if codes.is_empty() { None } else { Some(codes) })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn normalizes_common_variants() {
            assert_eq!(Trade::from_label("Plumbing"), Some(Trade::Plumbing));
            assert_eq!(Trade::from_label("plumber"), Some(Trade::Plumbing));
            assert_eq!(
                Trade::from_label("Electrical services"),
                Some(Trade::Electrical)
            );
            assert_eq!(Trade::from_label("HVAC/R"), Some(Trade::Hvac));
            assert_eq!(Trade::from_label("Heating & Cooling"), Some(Trade::Hvac));
            assert_eq!(
                Trade::from_label("Pool maintenance"),
                Some(Trade::PoolService)
            );
            assert_eq!(Trade::from_label("Lawn care"), Some(Trade::Landscaping));
            assert_eq!(Trade::from_label("Landscaping"), Some(Trade::Landscaping));
        }

        #[test]
        fn unknown_labels_return_none() {
            assert_eq!(Trade::from_label("Roofing"), None);
            assert_eq!(Trade::from_label(""), None);
        }

        #[test]
        fn canonicalize_keeps_raw_for_unknown() {
            assert_eq!(canonicalize("plumber"), ("Plumbing".to_string(), true));
            assert_eq!(canonicalize("Roofing"), ("Roofing".to_string(), false));
        }

        #[test]
        fn prompt_list_is_the_five_canonical_labels() {
            assert_eq!(
                prompt_list(),
                "Plumbing, Electrical, HVAC, Landscaping, Pool service"
            );
        }

        #[test]
        fn soc_codes_are_stable() {
            assert_eq!(Trade::Plumbing.soc_code(), "47-2152");
            assert_eq!(Trade::PoolService.soc_code(), "37-3011");
        }

        #[test]
        fn empty_registry_merges_to_exactly_the_enum_seeds() {
            let entries = merge_taxonomy(&[]);
            assert_eq!(entries.len(), 5);
            assert_eq!(prompt_list_of(&entries), prompt_list());
            assert!(entries.iter().all(|e| e.source == "seed"));
            assert_eq!(entries[0].soc_code, "47-2152");
            assert_eq!(entries[0].naics, vec!["238220".to_string()]);
        }

        #[test]
        fn seed_records_round_trip_to_the_same_taxonomy() {
            // A registry materialized purely from seed_record rows must merge
            // to the identical taxonomy — the "dataset present but only seeds"
            // state changes nothing.
            let recs: Vec<serde_json::Value> = Trade::ALL
                .iter()
                .map(|t| seed_record(*t).1)
                .collect();
            assert_eq!(merge_taxonomy(&recs), seed_entries());
        }

        #[test]
        fn enabled_new_trade_is_appended_disabled_is_not() {
            let roofing = json!({
                "trade": "Roofing", "soc_code": "47-2181",
                "naics": ["238160"], "aliases": ["roof"],
                "enabled": true, "source": "approved",
            });
            let pest = json!({
                "trade": "Pest control", "soc_code": "37-2021",
                "naics": ["561710"], "aliases": ["pest", "extermin"],
                "enabled": false, "source": "proposed",
            });
            let entries = merge_taxonomy(&[roofing, pest]);
            assert_eq!(entries.len(), 6, "5 seeds + enabled Roofing");
            assert_eq!(entries[5].label, "Roofing");
            assert_eq!(
                prompt_list_of(&entries),
                "Plumbing, Electrical, HVAC, Landscaping, Pool service, Roofing"
            );
            assert!(!entries.iter().any(|e| e.label == "Pest control"));
        }

        #[test]
        fn enabled_absent_defaults_to_disabled_governance() {
            // Proposer writes enabled:false; a row missing the flag entirely
            // must ALSO stay out — enabling is an explicit human act.
            let row = json!({ "trade": "Roofing", "naics": ["238160"], "aliases": ["roof"] });
            assert_eq!(merge_taxonomy(&[row]).len(), 5);
        }

        #[test]
        fn seed_can_be_governed_off_or_overridden() {
            let off = json!({ "trade": "Pool service", "enabled": false, "source": "seed" });
            let entries = merge_taxonomy(&[off]);
            assert_eq!(entries.len(), 4);
            assert!(!entries.iter().any(|e| e.label == "Pool service"));

            // Sparse enabled override keeps the seed's SOC/NAICS (never erased).
            let sparse = json!({ "trade": "Plumbing", "enabled": true, "source": "approved" });
            let entries = merge_taxonomy(&[sparse]);
            let p = entries.iter().find(|e| e.label == "Plumbing").unwrap();
            assert_eq!(p.soc_code, "47-2152");
            assert_eq!(p.naics, vec!["238220".to_string()]);
            assert_eq!(p.source, "approved");
        }

        #[test]
        fn canonicalize_in_matches_enum_first_then_registry_aliases() {
            let mut entries = seed_entries();
            entries.push(TradeEntry {
                label: "Roofing".into(),
                soc_code: "47-2181".into(),
                naics: vec!["238160".into()],
                aliases: vec!["roof".into(), "shingle".into()],
                source: "approved".into(),
            });
            // Legacy inputs behave exactly as canonicalize() always has.
            assert_eq!(canonicalize_in(&entries, "plumber"), ("Plumbing".into(), true));
            assert_eq!(
                canonicalize_in(&entries, "pool landscaping"),
                canonicalize("pool landscaping"),
                "enum keyword precedence preserved"
            );
            // New registry trade now resolves.
            assert_eq!(canonicalize_in(&entries, "Roof repair"), ("Roofing".into(), true));
            assert_eq!(canonicalize_in(&entries, "Chimneys"), ("Chimneys".into(), false));
        }

        #[test]
        fn naics_prefixes_reproduce_the_census_apps_default_code_sets() {
            let seeds = seed_entries();
            // census-density (6-digit CBP set — same codes, order irrelevant to fetch).
            let six = naics_prefixes(&seeds, 6);
            let mut sorted = six.clone();
            sorted.sort();
            assert_eq!(sorted, vec!["238210", "238220", "561730", "561790"]);
            // census-nonemp (4-digit) and sector (2-digit) grains.
            assert_eq!(naics_prefixes(&seeds, 4), vec!["2382", "5617"]);
            assert_eq!(naics_prefixes(&seeds, 2), vec!["23", "56"]);
        }
    }
}

/// Cross-source unified layer for the trades domain. Mirrors `grants-common`:
/// each source app calls [`unified::sync_operator_economics`] at the end of its
/// run, which JOINS the four source datasets into one row per canonical trade in
/// the virtual `trades/operator_economics` dataset (key `US:<trade>`).
pub mod unified {
    use super::taxonomy;
    use pumper_core::{AppContext, Result, UpsertSummary};
    use serde_json::{json, Value};

    /// Virtual app namespace holding the cross-source trades dataset.
    pub const UNIFIED_APP: &str = "trades";
    pub const OPERATOR_ECONOMICS: &str = "operator_economics";
    /// Per state × trade licensing / bonding / insurance reference written by
    /// the `state-licensing` app (keys `<ST>:<trade>`), joined here as a
    /// `compliance` block on the matching per-state rows.
    pub const COMPLIANCE: &str = "compliance";
    /// Read cap for the compliance dataset: well past 51 jurisdictions × 5
    /// trades (= 255) so the join can't silently truncate.
    const COMPLIANCE_READ_LIMIT: i64 = 5_000;
    /// The national-roll-up locality (matches homewyse-pricing's default).
    const NATIONAL_LOCALITY: &str = "United States";
    /// Read cap for the pricing dataset: well past 51 localities × 5 trades × 4
    /// jobs (≈1020) so the summary can't silently truncate once localities drive.
    const PRICING_READ_LIMIT: i64 = 50_000;

    /// Rebuilds `trades/operator_economics` from the current state of the four
    /// source datasets: wage band (trade-wages), pricing summary (homewyse),
    /// tax context (state-tax), and valuation multiples (valuation-multiples).
    /// Emits a national roll-up row `US:<trade>` **and** a per-state row
    /// `<ST>:<trade>` for every state tax record — the per-state rows carry that
    /// state's REAL top-marginal rate instead of a national median. Wage /
    /// valuation stay the national roll-up on state rows (`wage_grain: national`)
    /// until per-state OEWS lands. Idempotent `upsert_many` (a join, never a
    /// full-snapshot sync — absent source data must not mark rows removed).
    pub async fn sync_operator_economics(ctx: &AppContext) -> Result<UpsertSummary> {
        // Federal small-business constants — national, same for every trade.
        let federal = ctx
            .datasets
            .get("state-tax", "tax", "federal:US")
            .await?
            .map(|r| r.data);

        // Real per-state tax records (code → record) + the illustrative national
        // median used only by the `US:{trade}` roll-up.
        let state_records = ctx.datasets.list("state-tax", "tax", 200).await?;
        let mut state_tax: Vec<(String, Value)> = Vec::new();
        let mut state_rates: Vec<f64> = Vec::new();
        for r in &state_records {
            if r.data.get("level").and_then(Value::as_str) != Some("state") {
                continue;
            }
            if let Some(rate) = r.data.get("top_marginal_rate").and_then(Value::as_f64) {
                state_rates.push(rate);
            }
            if let Some(code) = r.data.get("state").and_then(Value::as_str) {
                state_tax.push((code.to_string(), r.data.clone()));
            }
        }
        state_rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_state_rate = median(&state_rates);

        // All priced jobs. Cap raised well past 51 localities × 5 trades × 4 jobs
        // (≈1020) so the summary can't silently truncate once localities are driven.
        let pricing_recs = ctx
            .datasets
            .list("homewyse-pricing", "pricing", PRICING_READ_LIMIT)
            .await?;
        let pricing: Vec<&Value> = pricing_recs.iter().map(|r| &r.data).collect();

        // Per state × trade compliance (state-licensing app): keyed `<ST>:<trade>`,
        // looked up per-row below. State-grain data, so the national `US:{trade}`
        // roll-up carries no compliance block (Null, never a fabricated average).
        let compliance_recs = ctx
            .datasets
            .list(UNIFIED_APP, COMPLIANCE, COMPLIANCE_READ_LIMIT)
            .await?;
        let compliance: std::collections::HashMap<String, Value> = compliance_recs
            .into_iter()
            .map(|r| (r.key, r.data))
            .collect();

        let mut items: Vec<(String, Value)> = Vec::new();
        // Trade universe = the governed taxonomy registry, enum as fallback —
        // an enabled registry trade gets its unified rows on the next sync
        // with zero new per-trade code.
        for entry in taxonomy::taxonomy(ctx).await? {
            let label = entry.label.as_str();
            // Wage + valuation are national roll-ups (`US:{label}`) — per-state
            // OEWS wages are deferred (trades#2 phase c); valuation stays national
            // by design (per-state broker comps are too thin to be honest).
            let wage = ctx
                .datasets
                .get("trade-wages", "wages", &format!("US:{label}"))
                .await?
                .map(|r| r.data);
            let valuation = ctx
                .datasets
                .get("valuation-multiples", "valuation", &format!("US:{label}"))
                .await?
                .map(|r| r.data);

            // National roll-up row (pricing filtered to the national locality, so a
            // Texas price no longer contaminates the national envelope).
            let national_pricing = summarize_pricing(&pricing, label, NATIONAL_LOCALITY);
            if wage.is_some()
                || valuation.is_some()
                || national_pricing.is_some()
                || federal.is_some()
            {
                items.push((
                    format!("US:{label}"),
                    json!({
                        "trade": label,
                        "state": "US",
                        "soc_code": entry.soc_code,
                        "wage_band": wage.as_ref().map(wage_band),
                        "wage_grain": "national",
                        "pricing": national_pricing,
                        "pricing_locality": NATIONAL_LOCALITY,
                        "tax": tax_context(federal.as_ref(), median_state_rate),
                        // Compliance is state-grain (licensing is a state power);
                        // a national roll-up would be a fabricated average.
                        "compliance": Value::Null,
                        "valuation": valuation.as_ref().map(valuation_summary),
                    }),
                ));
            }

            // Per-state rows carry the REAL state tax (the actionable win). Wage /
            // valuation stay the national roll-up (labeled `wage_grain: national`);
            // pricing is per-locality — non-null once a locality matching the state
            // code is priced, else null (never the contaminated average).
            for (code, trec) in &state_tax {
                let state_pricing = summarize_pricing(&pricing, label, code);
                items.push((
                    format!("{code}:{label}"),
                    json!({
                        "trade": label,
                        "state": code,
                        "soc_code": entry.soc_code,
                        "wage_band": wage.as_ref().map(wage_band),
                        "wage_grain": "national",
                        "pricing": state_pricing,
                        "pricing_locality": code,
                        "tax": state_tax_context(federal.as_ref(), trec),
                        "compliance": compliance
                            .get(&format!("{code}:{label}"))
                            .map(compliance_summary)
                            .unwrap_or(Value::Null),
                        "valuation": valuation.as_ref().map(valuation_summary),
                    }),
                ));
            }
        }

        ctx.datasets
            .upsert_many(UNIFIED_APP, OPERATOR_ECONOMICS, &items)
            .await
    }

    /// Compact wage-band subset lifted from a trade-wages record.
    fn wage_band(rec: &Value) -> Value {
        json!({
            "soc_code": rec.get("soc_code"),
            "occupation": rec.get("occupation"),
            "entry_hourly": rec.get("entry_hourly"),
            "median_hourly": rec.get("median_hourly"),
            "experienced_hourly": rec.get("experienced_hourly"),
            "median_annual": rec.get("median_annual"),
            "employment": rec.get("employment"),
        })
    }

    /// The compact federal-constants subset, shared by the national and per-state
    /// tax contexts.
    fn federal_summary(federal: Option<&Value>) -> Value {
        federal
            .map(|f| {
                json!({
                    "self_employment_tax_rate": f.get("self_employment_tax_rate"),
                    "qbi_deduction_pct": f.get("qbi_deduction_pct"),
                    "standard_deduction_single": f.get("standard_deduction_single"),
                    "section_179_limit": f.get("section_179_limit"),
                    "top_marginal_rate": f.get("top_marginal_rate"),
                })
            })
            .unwrap_or(Value::Null)
    }

    /// National roll-up tax context: federal constants + one illustrative median
    /// state rate (the `US:{trade}` row only — a per-state row carries its real rate).
    fn tax_context(federal: Option<&Value>, median_state_rate: Option<f64>) -> Value {
        json!({
            "federal": federal_summary(federal),
            "illustrative_state_top_marginal_rate_median": median_state_rate,
        })
    }

    /// Per-state tax context: federal constants + the state's REAL top-marginal
    /// rate — so a Texan (0%) and a Californian (13.3%) no longer receive the same
    /// median middle number, which was right for neither.
    fn state_tax_context(federal: Option<&Value>, state: &Value) -> Value {
        json!({
            "federal": federal_summary(federal),
            "state": {
                "state": state.get("state"),
                "income_tax_type": state.get("income_tax_type"),
                "top_marginal_rate": state.get("top_marginal_rate"),
            },
        })
    }

    /// Compact compliance subset lifted from a state-licensing record: what it
    /// costs to legally exist in the state — requirement level, license/bond
    /// dollars, insurance minimum, workers-comp signal, plus the honesty
    /// fields (`grain`, `local_variation`, `year`) so a consumer knows the
    /// figure is state-grain and whether counties/cities complicate it.
    fn compliance_summary(rec: &Value) -> Value {
        json!({
            "requirement_level": rec.get("requirement_level"),
            "license_cost_usd": rec.get("license_cost_usd"),
            "bond_amount_usd": rec.get("bond_amount_usd"),
            "insurance_min_liability_usd": rec.get("insurance_min_liability_usd"),
            "workers_comp_required": rec.get("workers_comp_required"),
            "grain": rec.get("grain"),
            "local_variation": rec.get("local_variation"),
            "year": rec.get("year"),
        })
    }

    /// Compact valuation subset lifted from a valuation-multiples record.
    fn valuation_summary(rec: &Value) -> Value {
        json!({
            "sde_multiple_low": rec.get("sde_multiple_low"),
            "sde_multiple_median": rec.get("sde_multiple_median"),
            "sde_multiple_high": rec.get("sde_multiple_high"),
            "revenue_multiple": rec.get("revenue_multiple"),
        })
    }

    /// Summarize the priced jobs for a trade **in one locality** into a compact
    /// band: job count and the low/median/high envelope across jobs. Filtering on
    /// locality is what stops two localities' prices (e.g. Texas + national) from
    /// being silently averaged into one envelope. Returns None if none priced.
    fn summarize_pricing(pricing: &[&Value], trade_label: &str, locality: &str) -> Option<Value> {
        let mut lows = Vec::new();
        let mut medians = Vec::new();
        let mut highs = Vec::new();
        for r in pricing {
            if r.get("trade").and_then(Value::as_str) != Some(trade_label) {
                continue;
            }
            if r.get("locality").and_then(Value::as_str) != Some(locality) {
                continue;
            }
            if let Some(v) = r.get("low").and_then(Value::as_f64) {
                lows.push(v);
            }
            if let Some(v) = r.get("median").and_then(Value::as_f64) {
                medians.push(v);
            }
            if let Some(v) = r.get("high").and_then(Value::as_f64) {
                highs.push(v);
            }
        }
        if medians.is_empty() && lows.is_empty() && highs.is_empty() {
            return None;
        }
        medians.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(json!({
            "jobs_priced": medians.len(),
            "low": lows.iter().cloned().fold(None, min_opt),
            "median": median(&medians),
            "high": highs.iter().cloned().fold(None, max_opt),
        }))
    }

    fn min_opt(acc: Option<f64>, v: f64) -> Option<f64> {
        Some(acc.map_or(v, |a| a.min(v)))
    }
    fn max_opt(acc: Option<f64>, v: f64) -> Option<f64> {
        Some(acc.map_or(v, |a| a.max(v)))
    }

    /// Median of a pre-sorted slice.
    fn median(sorted: &[f64]) -> Option<f64> {
        if sorted.is_empty() {
            return None;
        }
        let n = sorted.len();
        Some(if n % 2 == 1 {
            sorted[n / 2]
        } else {
            (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn median_handles_odd_and_even() {
            assert_eq!(median(&[1.0, 2.0, 3.0]), Some(2.0));
            assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));
            assert_eq!(median(&[]), None);
        }

        #[test]
        fn summarize_pricing_isolates_by_locality_no_contamination() {
            let job = |trade: &str, locality: &str, med: f64| json!({ "trade": trade, "locality": locality, "low": med - 10.0, "median": med, "high": med + 10.0 });
            let rows = [
                job("Plumbing", "United States", 300.0),
                job("Plumbing", "Texas", 250.0), // must NOT pollute the national envelope
                job("Plumbing", "United States", 340.0),
            ];
            let refs: Vec<&Value> = rows.iter().collect();
            let national = summarize_pricing(&refs, "Plumbing", "United States").unwrap();
            assert_eq!(national["jobs_priced"], 2, "only the two US jobs");
            assert_eq!(national["median"], 320.0); // (300+340)/2, Texas excluded
            let tx = summarize_pricing(&refs, "Plumbing", "Texas").unwrap();
            assert_eq!(tx["jobs_priced"], 1);
            assert_eq!(tx["median"], 250.0);
            // A locality with no priced jobs → None, never a fabricated average.
            assert!(summarize_pricing(&refs, "Plumbing", "Ohio").is_none());
        }

        #[test]
        fn compliance_summary_carries_costs_and_honesty_fields() {
            let rec = json!({
                "state": "CA", "trade": "Plumbing",
                "requirement_level": "exam_license",
                "license_cost_usd": 600.0,
                "bond_amount_usd": 25000.0,
                "insurance_min_liability_usd": 1000000.0,
                "workers_comp_required": false,
                "grain": "state", "local_variation": false, "year": "2026",
                "notes": "CSLB C-36",
            });
            let c = compliance_summary(&rec);
            assert_eq!(c["requirement_level"], "exam_license");
            assert_eq!(c["bond_amount_usd"], 25000.0);
            assert_eq!(c["grain"], "state");
            assert_eq!(c["local_variation"], false);
            // Absent fields stay Null — never fabricated.
            let sparse = compliance_summary(&json!({ "requirement_level": "none" }));
            assert_eq!(sparse["requirement_level"], "none");
            assert!(sparse["bond_amount_usd"].is_null());
        }

        #[test]
        fn state_tax_context_carries_the_real_rate_not_a_median() {
            let federal = json!({ "self_employment_tax_rate": 0.153, "top_marginal_rate": 0.37 });
            let tx = json!({ "state": "TX", "income_tax_type": "none", "top_marginal_rate": 0.0 });
            let ca = json!({ "state": "CA", "income_tax_type": "graduated", "top_marginal_rate": 0.133 });
            let tx_ctx = state_tax_context(Some(&federal), &tx);
            let ca_ctx = state_tax_context(Some(&federal), &ca);
            // Texan gets 0%, Californian gets 13.3% — not the same middle number.
            assert_eq!(tx_ctx["state"]["top_marginal_rate"], 0.0);
            assert_eq!(ca_ctx["state"]["top_marginal_rate"], 0.133);
            assert_eq!(tx_ctx["federal"]["top_marginal_rate"], 0.37);
        }
    }
}

#[cfg(test)]
mod salvage_tests {
    use super::*;

    #[test]
    fn salvages_a_clean_object() {
        let v = salvage_json(r#"{"locality":"Texas","trades":[]}"#).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_a_fenced_object() {
        let raw = "```json\n{\"locality\":\"Texas\",\"trades\":[]}\n```";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_an_object_wrapped_in_prose() {
        let raw = "Here is the pricing data you asked for:\n{\"locality\":\"Texas\",\
                   \"trades\":[{\"trade\":\"Plumbing\",\"jobs\":[]}]}\nHope that helps!";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
        assert_eq!(v["trades"][0]["trade"], "Plumbing");
    }

    #[test]
    fn does_not_close_early_on_a_brace_inside_a_string() {
        let raw = r#"prefix {"note":"a } inside a string","ok":true} suffix"#;
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["note"], "a } inside a string");
    }

    #[test]
    fn returns_none_when_there_is_no_object() {
        assert!(salvage_json("I could not find reliable pricing data.").is_none());
    }
}
