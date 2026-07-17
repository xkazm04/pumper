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

use pumper_core::{salvage_json, AppContext, Error, ResearchOutput, ResearchRequest, Result};
use serde_json::Value;

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
    // Metered seam: records a cost event against the job, honors budget_usd, and
    // serves identical re-runs from the research cache (see core/app.rs).
    let output = ctx.research(request).await?;

    let artifact = match &output.json {
        Some(j) => serde_json::to_vec_pretty(j)?,
        None => output.text.clone().into_bytes(),
    };
    ctx.save_artifact("research.json", &artifact).await?;

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

/// The `year` param an agentic trades app was refreshed for. Central so the four
/// apps parse the vintage identically.
pub fn year_param<'a>(ctx: &'a AppContext, default: &'a str) -> &'a str {
    ctx.params.get("year").and_then(Value::as_str).unwrap_or(default)
}

/// Whether a re-run is being forced (`force: true`), bypassing every freshness gate.
pub fn forced(ctx: &AppContext) -> bool {
    ctx.params.get("force").and_then(Value::as_bool).unwrap_or(false)
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
        .and_then(|r| r.data.get("year").and_then(Value::as_str).map(str::to_string));
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
    let recs = ctx.datasets.list_filtered(app, dataset, &filter, None, 1).await?;
    let age = recs.first().map(|r| (chrono::Utc::now() - r.updated_at).num_days().max(0));
    Ok(age.is_some_and(|a| a < max_age_days))
}

/// Reads the `max_age_days` param (default `default_days`), clamped to `>= 0`.
pub fn max_age_days(ctx: &AppContext, default_days: i64) -> i64 {
    ctx.params.get("max_age_days").and_then(Value::as_i64).map(|d| d.max(0)).unwrap_or(default_days)
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

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn normalizes_common_variants() {
            assert_eq!(Trade::from_label("Plumbing"), Some(Trade::Plumbing));
            assert_eq!(Trade::from_label("plumber"), Some(Trade::Plumbing));
            assert_eq!(Trade::from_label("Electrical services"), Some(Trade::Electrical));
            assert_eq!(Trade::from_label("HVAC/R"), Some(Trade::Hvac));
            assert_eq!(Trade::from_label("Heating & Cooling"), Some(Trade::Hvac));
            assert_eq!(Trade::from_label("Pool maintenance"), Some(Trade::PoolService));
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
            assert_eq!(prompt_list(), "Plumbing, Electrical, HVAC, Landscaping, Pool service");
        }

        #[test]
        fn soc_codes_are_stable() {
            assert_eq!(Trade::Plumbing.soc_code(), "47-2152");
            assert_eq!(Trade::PoolService.soc_code(), "37-3011");
        }
    }
}

/// Cross-source unified layer for the trades domain. Mirrors `grants-common`:
/// each source app calls [`unified::sync_operator_economics`] at the end of its
/// run, which JOINS the four source datasets into one row per canonical trade in
/// the virtual `trades/operator_economics` dataset (key `US:<trade>`).
pub mod unified {
    use super::taxonomy::Trade;
    use pumper_core::{AppContext, Result, UpsertSummary};
    use serde_json::{json, Value};

    /// Virtual app namespace holding the cross-source trades dataset.
    pub const UNIFIED_APP: &str = "trades";
    pub const OPERATOR_ECONOMICS: &str = "operator_economics";
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
        let pricing_recs =
            ctx.datasets.list("homewyse-pricing", "pricing", PRICING_READ_LIMIT).await?;
        let pricing: Vec<&Value> = pricing_recs.iter().map(|r| &r.data).collect();

        let mut items: Vec<(String, Value)> = Vec::new();
        for trade in Trade::ALL {
            let label = trade.label();
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
                        "soc_code": trade.soc_code(),
                        "wage_band": wage.as_ref().map(wage_band),
                        "wage_grain": "national",
                        "pricing": national_pricing,
                        "pricing_locality": NATIONAL_LOCALITY,
                        "tax": tax_context(federal.as_ref(), median_state_rate),
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
                        "soc_code": trade.soc_code(),
                        "wage_band": wage.as_ref().map(wage_band),
                        "wage_grain": "national",
                        "pricing": state_pricing,
                        "pricing_locality": code,
                        "tax": state_tax_context(federal.as_ref(), trec),
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
            let job = |trade: &str, locality: &str, med: f64| {
                json!({ "trade": trade, "locality": locality, "low": med - 10.0, "median": med, "high": med + 10.0 })
            };
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
