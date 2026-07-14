//! US trades SERVICE PRICING via the Claude research engine.
//!
//! Typical prices a customer pays for common pool/plumbing/electrical/HVAC/landscaping
//! jobs — service-call fees, hourly labor rates, and headline installs — as a
//! low/median/high USD range, synthesized by the agent from cost guides (Homewyse,
//! Angi, Thumbtack, HomeAdvisor) with web search + page fetch. Pricing is the weakest
//! reference-data domain (no clean government API), so agentic synthesis is the right
//! tool — this is the "no fixed crawler works" case the Claude engine exists for.
//! Upserted into the `pricing` dataset; the run's cost / duration / turns are reported
//! back in the result so a consumer (e.g. the Ledgerline admin console) can meter it.
//!
//! Data type: PEER PRICING BENCHMARKS. Access: the local Claude Code CLI (no API key;
//! uses the local subscription). This is a metered engine — every run costs real money,
//! unlike the http Census apps. Params: {"locality": "United States", "year": "2025",
//! "role": "research|compose", "model": "...", "effort": "...", "max_turns": 20}.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};
use trades_common::salvage_json;
use trades_common::taxonomy;
use trades_common::unified;
use trades_common::validate::{self, Rejection};

pub struct HomewysePricing;

const DEFAULT_LOCALITY: &str = "United States";
const DEFAULT_YEAR: &str = "2025";

#[async_trait]
impl ScrapeApp for HomewysePricing {
    fn name(&self) -> &'static str {
        "homewyse-pricing"
    }

    fn description(&self) -> &'static str {
        "US trades SERVICE PRICING via the Claude research engine — typical service-call \
         fees, hourly labor rates and headline install prices (low/median/high) for \
         pool/plumbing/electrical/HVAC/landscaping, synthesized from cost guides with web \
         search. Upserted into the `pricing` dataset; reports cost/turns. No API key (uses \
         the local Claude CLI — this engine costs money per run). Params: {\"locality\": \
         \"United States\", \"year\": \"2025\", \"role\": \"research|compose\", \
         \"max_turns\": 20}."
    }

    fn default_params(&self) -> Value {
        json!({ "locality": DEFAULT_LOCALITY, "year": DEFAULT_YEAR })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let locality = ctx
            .params
            .get("locality")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_LOCALITY)
            .to_string();
        let year = ctx
            .params
            .get("year")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_YEAR)
            .to_string();
        let role = ctx
            .params
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("research")
            .to_string();
        // Bound cost by default; overridable per job.
        let max_turns = ctx
            .params
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|t| t as u32)
            .or(Some(20));

        let trades = taxonomy::prompt_list();
        let prompt = format!(
            "You are a home-services pricing analyst. Using web search and page fetches, \
             research the TYPICAL PRICE A CUSTOMER PAYS in {locality} ({year}) for common \
             jobs in these trades: {trades}. \
             Cross-check across at least two cost guides (e.g. Homewyse, Angi, Thumbtack, \
             HomeAdvisor). For each trade give 3-4 representative jobs, each with a \
             low/median/high USD range.\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"locality\": string, \"year\": string, \"trades\": [{{\"trade\": string, \
             \"jobs\": [{{\"job\": string, \"unit\": \"flat|hour|sqft|visit\", \"low\": number, \
             \"median\": number, \"high\": number}}]}}]}}"
        );

        let mut request = ResearchRequest::new(prompt).with_role(role);
        request.max_turns = max_turns;
        request.model = ctx.params.get("model").and_then(Value::as_str).map(String::from);
        request.effort = ctx.params.get("effort").and_then(Value::as_str).map(String::from);
        // Constrain the final answer to the pricing schema (`claude --json-schema`): the
        // CLI validates the structured output, so the agent can't emit the malformed JSON
        // that failed ~1/3 of runs (e.g. a dropped key, `"low":150,"300,"high":500`). The
        // salvage_json fallback below still catches anything the schema path misses.
        request.json_schema = Some(pricing_schema());
        // Metered seam: records a cost event against the job, honors budget_usd,
        // and serves identical re-runs from the research cache (see core/app.rs).
        let output = ctx.research(request).await?;

        let artifact = match &output.json {
            Some(j) => serde_json::to_vec_pretty(j)?,
            None => output.text.clone().into_bytes(),
        };
        ctx.save_artifact("research.json", &artifact).await?;

        // The agent usually returns a clean object, but ~1/3 of runs wrap it in a
        // markdown fence or add a sentence around it, which the engine can't parse into
        // `output.json`. Salvage the object from the raw text before giving up — this is
        // free (no re-run), unlike a job-level retry of the whole (metered) research.
        let data = match output.json.clone() {
            Some(j) => j,
            None => salvage_json(&output.text).ok_or_else(|| {
                Error::App(format!(
                    "homewyse-pricing: agent did not return JSON (text starts: {})",
                    output.text.chars().take(160).collect::<String>()
                ))
            })?,
        };

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut trade_summaries: Vec<Value> = Vec::new();
        // Plausibility guards: a priced job whose band is out of order or
        // non-positive is rejected (with reasons) rather than upserted. One
        // pass, no re-run — the answer is already paid for.
        let mut rejected: Vec<Rejection> = Vec::new();
        let mut unknown_trades: Vec<String> = Vec::new();
        if let Some(trades) = data.get("trades").and_then(Value::as_array) {
            for t in trades {
                let raw = t.get("trade").and_then(Value::as_str).unwrap_or("").trim().to_string();
                // Normalize to a canonical label so the pricing rows join to the
                // unified layer by the same trade string; unknown labels flagged.
                let (trade, known) = taxonomy::canonicalize(&raw);
                if !raw.is_empty() && !known {
                    unknown_trades.push(raw.clone());
                }
                let mut job_count = 0;
                if let Some(jobs) = t.get("jobs").and_then(Value::as_array) {
                    for j in jobs {
                        let job = j.get("job").and_then(Value::as_str).unwrap_or("").to_string();
                        if trade.is_empty() || job.is_empty() {
                            continue;
                        }
                        // Key on a stable slug of trade+job, not the model's raw
                        // free text: otherwise trivial phrasing drift ("Install
                        // 30-gal heater" vs "install 30 gal heater") mints a new key
                        // every run and accumulates stale duplicate rows unboundedly.
                        // The original strings are still stored for display.
                        let key = format!("{locality}:{}:{}", slugify(&trade), slugify(&job));
                        let low = validate::num(j, "low");
                        let median = validate::num(j, "median");
                        let high = validate::num(j, "high");
                        let mut reasons = Vec::new();
                        validate::require_positive(&mut reasons, "low", low);
                        validate::require_positive(&mut reasons, "median", median);
                        validate::require_positive(&mut reasons, "high", high);
                        validate::require_monotone(&mut reasons, "price", low, median, high);
                        if !reasons.is_empty() {
                            rejected.push(Rejection { key, reasons });
                            continue;
                        }
                        job_count += 1;
                        all_records.push((
                            key,
                            json!({
                                "locality": locality,
                                "year": year,
                                "trade": trade,
                                "job": job,
                                "unit": j.get("unit").and_then(Value::as_str).unwrap_or("flat"),
                                // Store the validated numbers, not the raw values:
                                // a string-quoted price ("1234") passes validation
                                // via validate::num but, stored raw, is read back as
                                // a non-number and silently dropped from the rollup.
                                "low": low,
                                "median": median,
                                "high": high,
                            }),
                        ));
                    }
                }
                trade_summaries.push(json!({ "trade": trade, "jobs_priced": job_count }));
            }
        }

        if all_records.is_empty() {
            return Err(Error::App(
                "homewyse-pricing: agent JSON contained no priced jobs".into(),
            ));
        }

        let summary = ctx.upsert_many("pricing", &all_records).await?;

        // Cross-source layer: rebuild trades/operator_economics from all four
        // source datasets (mirrors grants-common's sync_unified).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/pricing/{year}"),
            "locality": locality,
            "year": year,
            "trades": trade_summaries,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "rejected": rejected.iter().map(Rejection::to_json).collect::<Vec<_>>(),
            "rejected_count": rejected.len(),
            "unknown_trades": unknown_trades,
            "unified": { "new": unified.new.len(), "changed": unified.changed.len() },
            // Metered-engine telemetry — the console reads cost_usd for the run.
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
        }))
    }
}

/// The structured-output contract for `claude --json-schema`. Constrains the agent's
/// final answer so the CLI returns validated JSON of exactly this shape — the root-cause
/// fix for the malformed-JSON runs. Kept intentionally lenient (unit is a free string the
/// app normalizes; extra fields tolerated) so a valid answer is never rejected.
fn pricing_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "locality": { "type": "string" },
            "year": { "type": "string" },
            "trades": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "trade": { "type": "string" },
                        "jobs": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "job": { "type": "string" },
                                    "unit": { "type": "string" },
                                    "low": { "type": "number" },
                                    "median": { "type": "number" },
                                    "high": { "type": "number" }
                                },
                                "required": ["job", "low", "median", "high"]
                            }
                        }
                    },
                    "required": ["trade", "jobs"]
                }
            }
        },
        "required": ["locality", "year", "trades"]
    })
}

/// Canonical slug for a free-text label: lowercased alphanumerics with runs of
/// other characters collapsed to single hyphens. Gives a stable dataset key so
/// minor phrasing/whitespace/case drift maps to the same record instead of
/// accumulating duplicates.
fn slugify(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
            prev_dash = false;
        } else if !prev_dash && !out.is_empty() {
            out.push('-');
            prev_dash = true;
        }
    }
    out.trim_end_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::slugify;

    #[test]
    fn slugify_stabilizes_phrasing_drift() {
        assert_eq!(slugify("Install 30-gal water heater"), "install-30-gal-water-heater");
        // Case / spacing / punctuation drift collapses to the same key.
        assert_eq!(slugify("install 30 gal water heater"), slugify("Install 30-gal  water heater"));
        // Meaningful differences are preserved.
        assert_ne!(slugify("30-gal heater"), slugify("40-gal heater"));
        assert_eq!(slugify("  --Trim--  "), "trim");
    }
}
