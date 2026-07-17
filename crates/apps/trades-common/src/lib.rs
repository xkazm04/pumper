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

    /// Rebuilds `trades/operator_economics` from the current state of the four
    /// source datasets: wage band (trade-wages), pricing summary (homewyse),
    /// federal + illustrative-state tax context (state-tax), and valuation
    /// multiples (valuation-multiples). One row per canonical trade, keyed
    /// `US:<trade>`. Idempotent `upsert_many` (a join, never a full-snapshot
    /// sync — absent source data must not mark rows removed).
    pub async fn sync_operator_economics(ctx: &AppContext) -> Result<UpsertSummary> {
        // Federal small-business constants — national, same for every trade.
        let federal = ctx
            .datasets
            .get("state-tax", "tax", "federal:US")
            .await?
            .map(|r| r.data);

        // Illustrative state context: the median top marginal rate across the
        // state records we currently hold (a single representative number so the
        // join stays compact rather than embedding 51 states per trade).
        let state_records = ctx.datasets.list("state-tax", "tax", 200).await?;
        let mut state_rates: Vec<f64> = state_records
            .iter()
            .filter(|r| r.data.get("level").and_then(Value::as_str) == Some("state"))
            .filter_map(|r| r.data.get("top_marginal_rate").and_then(Value::as_f64))
            .collect();
        state_rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median_state_rate = median(&state_rates);

        // All priced jobs, grouped per trade below.
        let pricing = ctx.datasets.list("homewyse-pricing", "pricing", 1000).await?;

        let mut items: Vec<(String, Value)> = Vec::new();
        for trade in Trade::ALL {
            let label = trade.label();
            let key = format!("US:{label}");

            let wage = ctx
                .datasets
                .get("trade-wages", "wages", &key)
                .await?
                .map(|r| r.data);
            let valuation = ctx
                .datasets
                .get("valuation-multiples", "valuation", &key)
                .await?
                .map(|r| r.data);
            let pricing_summary = summarize_pricing(&pricing, label);

            // A trade with no data in ANY source isn't a real join row yet.
            if wage.is_none()
                && valuation.is_none()
                && pricing_summary.is_none()
                && federal.is_none()
            {
                continue;
            }

            items.push((
                key,
                json!({
                    "trade": label,
                    "soc_code": trade.soc_code(),
                    "wage_band": wage.as_ref().map(wage_band),
                    "pricing": pricing_summary,
                    "tax": tax_context(federal.as_ref(), median_state_rate),
                    "valuation": valuation.as_ref().map(valuation_summary),
                }),
            ));
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

    /// Federal constants + one illustrative state top-marginal rate.
    fn tax_context(federal: Option<&Value>, median_state_rate: Option<f64>) -> Value {
        json!({
            "federal": federal.map(|f| json!({
                "self_employment_tax_rate": f.get("self_employment_tax_rate"),
                "qbi_deduction_pct": f.get("qbi_deduction_pct"),
                "standard_deduction_single": f.get("standard_deduction_single"),
                "section_179_limit": f.get("section_179_limit"),
                "top_marginal_rate": f.get("top_marginal_rate"),
            })),
            "illustrative_state_top_marginal_rate_median": median_state_rate,
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

    /// Summarize all priced jobs for a trade into a compact band: job count and
    /// the low/median/high envelope across jobs. Returns None if none priced.
    fn summarize_pricing(pricing: &[pumper_core::Record], trade_label: &str) -> Option<Value> {
        let mut lows = Vec::new();
        let mut medians = Vec::new();
        let mut highs = Vec::new();
        for r in pricing {
            if r.data.get("trade").and_then(Value::as_str) != Some(trade_label) {
                continue;
            }
            if let Some(v) = r.data.get("low").and_then(Value::as_f64) {
                lows.push(v);
            }
            if let Some(v) = r.data.get("median").and_then(Value::as_f64) {
                medians.push(v);
            }
            if let Some(v) = r.data.get("high").and_then(Value::as_f64) {
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
