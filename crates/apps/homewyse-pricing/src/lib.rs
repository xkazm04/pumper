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
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, ResearchRequest, Result, ScrapeApp,
};
use serde_json::{json, Value};
use trades_common::coverage;
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

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "locality": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Market the prices are researched for (a country, state or metro). Scopes both the record keys and the freshness gate — a Texas refresh never satisfies a national one."
                    },
                    "year": { "type": "string", "description": "Pricing year stamped on every record." },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": { "type": "integer", "minimum": 1 },
                    "max_age_days": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Age freshness gate for THIS locality (default 90). A newer record skips the metered run."
                    },
                    "force": { "type": "boolean", "description": "Bypass the age gate and re-pay the ~20-turn research run." }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "National refresh (free no-op if priced within 90 days)",
                    params: json!({ "locality": DEFAULT_LOCALITY, "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description:
                        "Force fresh metro pricing regardless of how recently it was priced",
                    params: json!({
                        "locality": "Phoenix, AZ",
                        "year": DEFAULT_YEAR,
                        "force": true,
                        "max_turns": 25
                    }),
                },
            ],
            output_shape: Some(
                "{source, locality, year, trades: [{trade, jobs_priced}], records, \
                 coverage: {unit, covered, expected, ratio, floor, short, missing}, \
                 warnings: [string], new, \
                 changed, unchanged, rejected: [{key, reasons}], rejected_count, \
                 unknown_trades, unified: {new, changed}, cost_usd, duration_ms, \
                 num_turns} — or {source, locality, year, skipped, cost_usd: 0.0} when \
                 the age gate holds",
            ),
            cost_class: CostClass::Claude,
        }
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

        // Age freshness gate (scoped to THIS locality): consumer prices drift
        // within a year, so gate on record age rather than vintage. A no-op
        // refresh inside `max_age_days` (default 90) skips the ~20-turn agentic run
        // unless `force: true`.
        let max_age = trades_common::max_age_days(&ctx, 90);
        if trades_common::fresh_by_age_where(
            &ctx,
            "homewyse-pricing",
            "pricing",
            "$.locality",
            &locality,
            max_age,
        )
        .await?
        {
            return Ok(json!({
                "source": format!("agentic/pricing/{year}"),
                "locality": locality,
                "year": year,
                "skipped": format!("priced within {max_age}d (pass force:true to re-run)"),
                "cost_usd": 0.0,
            }));
        }

        // Trade universe = governed registry, enum fallback (identical list
        // while the registry dataset is absent).
        let entries = taxonomy::taxonomy(&ctx).await?;
        let trades = taxonomy::prompt_list_of(&entries);
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
        request.model = ctx
            .params
            .get("model")
            .and_then(Value::as_str)
            .map(String::from);
        request.effort = ctx
            .params
            .get("effort")
            .and_then(Value::as_str)
            .map(String::from);
        // Constrain the final answer to the pricing schema (`claude --json-schema`): the
        // CLI validates the structured output, so the agent can't emit the malformed JSON
        // that failed ~1/3 of runs (e.g. a dropped key, `"low":150,"300,"high":500`). The
        // salvage_json fallback below still catches anything the schema path misses.
        request.json_schema = Some(pricing_schema());
        // Provenance (M12): pin the derivation spec (prompt + structured-output
        // schema + model/effort) behind this answer. Pricing is the weakest-
        // sourced domain here, so knowing exactly which prompt produced a stored
        // band is the difference between an explainable figure and a rumor.
        let prov = trades_common::research_provenance(&ctx, "homewyse-pricing", &request).await;
        let (data, output) =
            trades_common::research_json(&ctx, "homewyse-pricing", request).await?;

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut trade_summaries: Vec<Value> = Vec::new();
        // Plausibility guards: a priced job whose band is out of order or
        // non-positive is rejected (with reasons) rather than upserted. One
        // pass, no re-run — the answer is already paid for.
        let mut rejected: Vec<Rejection> = Vec::new();
        let mut unknown_trades: Vec<String> = Vec::new();
        if let Some(trades) = data.get("trades").and_then(Value::as_array) {
            for t in trades {
                let raw = t
                    .get("trade")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                // Normalize to a canonical label so the pricing rows join to the
                // unified layer by the same trade string; unknown labels flagged.
                let (trade, known) = taxonomy::canonicalize_in(&entries, &raw);
                if !raw.is_empty() && !known {
                    unknown_trades.push(raw.clone());
                }
                let mut job_count = 0;
                if let Some(jobs) = t.get("jobs").and_then(Value::as_array) {
                    for j in jobs {
                        let job = j
                            .get("job")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
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
                                // Honest-Null, NOT `unwrap_or("flat")`. `unit` is
                                // not in the schema's required list, so an omitted
                                // one used to be *fabricated* as a flat job price
                                // — turning a $150/hour labor rate into a $150
                                // job. Every other figure in this family reports
                                // absence as absence; a semantic field is no
                                // different. Rejecting the row instead would throw
                                // away a validated price band over a missing
                                // display label, so the band is kept and the unit
                                // is null.
                                "unit": price_unit(j),
                                // Store the validated numbers, not the raw values:
                                // a string-quoted price ("1234") passes validation
                                // via validate::num but, stored raw, is read back as
                                // a non-number and silently dropped from the rollup.
                                // This is the fix `trades_common::validate::
                                // store_numbers` generalized for the four siblings
                                // that cloned raw model JSON; constructing the
                                // record from the parsed numbers, as here, is the
                                // same guarantee.
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

        // Coverage of the trade roster — the family's shared shape for "a
        // near-total rejection is not a silent success" (see
        // `trades_common::coverage`). A trade counts as covered only if it came
        // back with at least one *priced* job: a trade whose every job was
        // rejected contributed a `jobs_priced: 0` summary and nothing else,
        // which used to read as a green run.
        let present: std::collections::HashSet<String> = trade_summaries
            .iter()
            .filter(|s| s["jobs_priced"].as_u64().unwrap_or(0) > 0)
            .filter_map(|s| s["trade"].as_str().map(str::to_string))
            .collect();
        let roster: Vec<&str> = entries.iter().map(|e| e.label.as_str()).collect();
        let cov = coverage::Coverage::of_roster("priced trades", &roster, &present);
        let warnings: Vec<String> = cov.warning().into_iter().collect();

        if all_records.is_empty() {
            return Err(Error::App(
                "homewyse-pricing: agent JSON contained no priced jobs".into(),
            ));
        }

        // One research call produced every row, so a batch-level stamp is the
        // honest grain.
        let summary = ctx
            .upsert_many_with_provenance("pricing", &all_records, prov)
            .await?;

        // Cross-source layer: rebuild trades/operator_economics from all four
        // source datasets (mirrors grants-common's sync_unified).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/pricing/{year}"),
            "locality": locality,
            "year": year,
            "trades": trade_summaries,
            "records": all_records.len(),
            "coverage": cov.to_json(),
            "warnings": warnings,
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

/// The price unit the model reported, or `Null` when it reported none.
///
/// **Never a default.** `unit` is not in `pricing_schema`'s required list, and
/// the old `unwrap_or("flat")` therefore fabricated a semantic field: a job the
/// model priced per hour, whose `unit` it happened to omit, was stored as a flat
/// job price — $150/hour read back as a $150 job. That is the one outright
/// fabrication in a family whose stated convention is honest-Null everywhere
/// else. A blank or whitespace-only unit is treated as absent for the same
/// reason.
fn price_unit(job: &Value) -> Value {
    match job.get("unit").and_then(Value::as_str).map(str::trim) {
        Some(u) if !u.is_empty() => json!(u),
        _ => Value::Null,
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
    use super::{price_unit, slugify, HomewysePricing};
    use pumper_core::ScrapeApp;
    use serde_json::json;

    /// The manifest must describe the params the app actually ships: every key
    /// in `default_params` and in every worked example has to be a declared
    /// property. A schema that drifts from its own canonical invocations is
    /// worse than no schema — enqueue enforces it, so the drift shows up as a
    /// 422 on the app's own documented call.
    #[test]
    fn manifest_declares_every_param_it_ships() {
        let app = HomewysePricing;
        let m = app.manifest();
        let schema = m.params_schema.expect("rich manifest declares a schema");
        let props = schema["properties"]
            .as_object()
            .expect("schema declares properties");
        assert!(!m.examples.is_empty(), "a schema needs worked examples");
        let shape = m.output_shape.expect("agents need the result shape");
        // Family contract: every agentic trades app reports the shared
        // `coverage` block and the `warnings[]` a shortfall lands in.
        assert!(
            trades_common::coverage::shape_declares_coverage(shape),
            "output_shape must declare {:?}: {shape}",
            trades_common::coverage::RESULT_FIELDS
        );
        let mut shipped = vec![app.default_params()];
        shipped.extend(m.examples.iter().map(|e| e.params.clone()));
        for params in shipped {
            for key in params.as_object().expect("params are an object").keys() {
                assert!(props.contains_key(key), "undeclared param '{key}'");
            }
        }
    }

    /// The one outright fabrication in the family: `unit` is not required by the
    /// schema, and `unwrap_or("flat")` turned a job the model priced per hour
    /// into a flat job price — $150/hour stored as a $150 job.
    #[test]
    fn an_absent_unit_is_null_not_a_fabricated_flat() {
        assert_eq!(price_unit(&json!({ "unit": "hour" })), json!("hour"));
        assert!(price_unit(&json!({ "low": 1 })).is_null(), "no unit at all");
        assert!(price_unit(&json!({ "unit": "" })).is_null(), "empty string");
        assert!(
            price_unit(&json!({ "unit": "   " })).is_null(),
            "whitespace"
        );
        assert!(price_unit(&json!({ "unit": null })).is_null());
        // A non-string unit is not silently coerced into one either.
        assert!(price_unit(&json!({ "unit": 3 })).is_null());
    }

    #[test]
    fn slugify_stabilizes_phrasing_drift() {
        assert_eq!(
            slugify("Install 30-gal water heater"),
            "install-30-gal-water-heater"
        );
        // Case / spacing / punctuation drift collapses to the same key.
        assert_eq!(
            slugify("install 30 gal water heater"),
            slugify("Install 30-gal  water heater")
        );
        // Meaningful differences are preserved.
        assert_ne!(slugify("30-gal heater"), slugify("40-gal heater"));
        assert_eq!(slugify("  --Trim--  "), "trim");
    }
}
