//! Small-business VALUATION multiples for US home-services trades, via the Claude
//! research engine.
//!
//! For each trade Ledgerline serves (plumbing, electrical, HVAC, landscaping, pool):
//! the typical seller's-discretionary-earnings (SDE) valuation multiple — median +
//! low/high band — and a revenue multiple, synthesized from business-broker data
//! (BizBuySell Insight, brokerage reports). This grounds the wealth/valuation read,
//! which today uses hardcoded per-trade SDE bands; the pipeline replaces those with
//! sourced, refreshable multiples. Upserted into the `valuation` dataset.
//!
//! Data type: BUSINESS VALUATION MULTIPLES. Access: the local Claude CLI (no API key;
//! costs money per run) — BizBuySell is 403/Akamai-walled to a crawler and multiples
//! live across paywalled broker reports, so agentic synthesis is the right tool. The 5
//! trades come back in ONE structured call. Params: {"year": "2025",
//! "role": "research|compose", "max_turns": 25}.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, ResearchRequest, Result, ScrapeApp,
};
use serde_json::{json, Value};
use trades_common::taxonomy;
use trades_common::unified;
use trades_common::validate::{self, Rejection};

pub struct ValuationMultiples;

const DEFAULT_YEAR: &str = "2025";

#[async_trait]
impl ScrapeApp for ValuationMultiples {
    fn name(&self) -> &'static str {
        "valuation-multiples"
    }

    fn description(&self) -> &'static str {
        "Small-business VALUATION multiples for US home-services trades (plumbing, \
         electrical, HVAC, landscaping, pool), via the Claude research engine — median + \
         low/high SDE multiple and a revenue multiple per trade, synthesized from \
         business-broker data. Upserted into the `valuation` dataset; grounds the \
         wealth/valuation read. No API key (local Claude CLI; costs money per run). \
         Params: {\"year\": \"2025\", \"role\": \"research|compose\", \"max_turns\": 25}."
    }

    fn default_params(&self) -> Value {
        json!({ "year": DEFAULT_YEAR })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "year": { "type": "string", "description": "Year the multiples are compiled for." },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": { "type": "integer", "minimum": 1 },
                    "max_age_days": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Age freshness gate (default 90). Broker multiples drift slowly, so a recent refresh skips the metered run."
                    },
                    "force": { "type": "boolean", "description": "Bypass the age gate and re-pay the ~25-turn research run." }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description:
                        "Refresh SDE + revenue multiples (free no-op if valued within 90 days)",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Force a re-valuation with a tighter freshness window",
                    params: json!({ "year": DEFAULT_YEAR, "max_age_days": 30, "force": true }),
                },
            ],
            output_shape: Some(
                "{source, year, records, new, changed, unchanged, rejected: [{key, \
                 reasons}], rejected_count, unknown_trades, unified: {new, changed}, \
                 cost_usd, duration_ms, num_turns} — or {source, year, skipped, records, \
                 cost_usd: 0.0} when the age gate holds",
            ),
            cost_class: CostClass::Claude,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
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
        let max_turns = ctx
            .params
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|t| t as u32)
            .or(Some(25));

        // Age freshness gate: broker multiples drift within a year (but slowly),
        // so gate on record age (default 90d) rather than vintage. Skips the
        // ~25-turn agentic run for a recent refresh unless `force: true`.
        let max_age = trades_common::max_age_days(&ctx, 90);
        let sentinel = format!("US:{}", taxonomy::Trade::ALL[0].label());
        if trades_common::fresh_by_age(&ctx, "valuation-multiples", "valuation", &sentinel, max_age)
            .await?
        {
            let held = ctx
                .datasets
                .list("valuation-multiples", "valuation", 100)
                .await?
                .len();
            return Ok(json!({
                "source": format!("agentic/valuation/{year}"),
                "year": year,
                "skipped": format!("valued within {max_age}d (pass force:true to re-run)"),
                "records": held,
                "cost_usd": 0.0,
            }));
        }

        // Trade universe = governed registry, enum fallback (identical list —
        // and prompt — while the registry dataset is absent).
        let entries = taxonomy::taxonomy(&ctx).await?;
        let trades = taxonomy::prompt_list_of(&entries);
        let n_trades = entries.len();
        let prompt = format!(
            "You are a business-valuation analyst for small US home-services companies. \
             For {year}, compile the typical SMALL-BUSINESS valuation multiples for each of \
             these trades: {trades}. Use web \
             search + business-broker sources (e.g. BizBuySell Insight, brokerage reports). \
             Give the seller's-discretionary-earnings (SDE) multiple as a median with a \
             low/high band, plus a typical revenue multiple.\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"year\": string, \"trades\": [{{\"trade\": string, \
             \"sde_multiple_median\": number, \"sde_multiple_low\": number, \
             \"sde_multiple_high\": number, \"revenue_multiple\": number, \
             \"notes\": string}}]}}\n\
             Multiples are ratios (e.g. 2.5 means 2.5x SDE). Include all {n_trades} trades."
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
        // Constrain the final answer to the multiples schema (`claude --json-schema`);
        // salvage_json below still catches anything the schema path misses.
        request.json_schema = Some(multiples_schema());
        // Provenance (M12): pin the derivation spec (prompt + structured-output
        // schema + model/effort) that produced these multiples.
        let prov = trades_common::research_provenance(&ctx, "valuation-multiples", &request).await;
        let (data, output) =
            trades_common::research_json(&ctx, "valuation-multiples", request).await?;

        let (all_records, rejected, unknown_trades) =
            collect_valuation_records(&entries, &data, &year);

        if all_records.is_empty() {
            return Err(Error::App(
                "valuation-multiples: agent JSON contained no plausible trades".into(),
            ));
        }

        // One research call produced every row, so a batch-level stamp is the
        // honest grain.
        let summary = ctx
            .upsert_many_with_provenance("valuation", &all_records, prov)
            .await?;

        // Cross-source layer: rebuild trades/operator_economics from all four
        // source datasets (mirrors grants-common's sync_unified).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/valuation/{year}"),
            "year": year,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "rejected": rejected.iter().map(Rejection::to_json).collect::<Vec<_>>(),
            "rejected_count": rejected.len(),
            "unknown_trades": unknown_trades,
            "unified": { "new": unified.new.len(), "changed": unified.changed.len() },
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
        }))
    }
}

/// Validate + normalize the agent's `trades` array into upsertable records.
/// Returns `(records, rejected, unknown_trades)`:
/// - Plausibility guards: the SDE band must be ordered (low ≤ median ≤ high)
///   and all multiples positive. Violators rejected with reasons.
/// - Unknown trade labels keep the raw string and are flagged, not dropped.
fn collect_valuation_records(
    entries: &[taxonomy::TradeEntry],
    data: &Value,
    year: &str,
) -> (Vec<(String, Value)>, Vec<Rejection>, Vec<String>) {
    let mut all_records: Vec<(String, Value)> = Vec::new();
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
            if raw.is_empty() {
                continue;
            }
            // Normalize to a canonical label; unknown labels keep raw + flag.
            let (trade, known) = taxonomy::canonicalize_in(entries, &raw);
            if !known {
                unknown_trades.push(raw.clone());
            }
            let key = format!("US:{trade}");
            let mut reasons = Vec::new();
            for f in [
                "sde_multiple_low",
                "sde_multiple_median",
                "sde_multiple_high",
                "revenue_multiple",
            ] {
                validate::require_positive(&mut reasons, f, validate::num(t, f));
            }
            validate::require_monotone(
                &mut reasons,
                "sde_multiple",
                validate::num(t, "sde_multiple_low"),
                validate::num(t, "sde_multiple_median"),
                validate::num(t, "sde_multiple_high"),
            );
            if !reasons.is_empty() {
                rejected.push(Rejection { key, reasons });
                continue;
            }
            let mut rec = t.clone();
            // Store the canonical label so key and `trade` field agree.
            rec["trade"] = json!(trade);
            // National by trade — state = "US" so the ingest lifts market = "US".
            rec["state"] = json!("US");
            rec["year"] = json!(year);
            all_records.push((key, rec));
        }
    }
    (all_records, rejected, unknown_trades)
}

/// Structured-output contract for `claude --json-schema`. Lenient (extra fields
/// tolerated) so a valid answer is never rejected, but pins the multiples shape.
fn multiples_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "year": { "type": "string" },
            "trades": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "trade": { "type": "string" },
                        "sde_multiple_median": { "type": "number" },
                        "sde_multiple_low": { "type": "number" },
                        "sde_multiple_high": { "type": "number" },
                        "revenue_multiple": { "type": "number" },
                        "notes": { "type": "string" }
                    },
                    "required": [
                        "trade", "sde_multiple_median", "sde_multiple_low", "sde_multiple_high"
                    ]
                }
            }
        },
        "required": ["year", "trades"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The manifest must describe the params the app actually ships: every key
    /// in `default_params` and in every worked example has to be a declared
    /// property. A schema that drifts from its own canonical invocations is
    /// worse than no schema — enqueue enforces it, so the drift shows up as a
    /// 422 on the app's own documented call.
    #[test]
    fn manifest_declares_every_param_it_ships() {
        let app = ValuationMultiples;
        let m = app.manifest();
        let schema = m.params_schema.expect("rich manifest declares a schema");
        let props = schema["properties"]
            .as_object()
            .expect("schema declares properties");
        assert!(!m.examples.is_empty(), "a schema needs worked examples");
        assert!(m.output_shape.is_some(), "agents need the result shape");
        let mut shipped = vec![app.default_params()];
        shipped.extend(m.examples.iter().map(|e| e.params.clone()));
        for params in shipped {
            for key in params.as_object().expect("params are an object").keys() {
                assert!(props.contains_key(key), "undeclared param '{key}'");
            }
        }
    }

    // A multiples entry shaped like the agent's structured answer for one trade.
    fn multiples_entry(trade: &str) -> Value {
        json!({
            "trade": trade,
            "sde_multiple_low": 2.0, "sde_multiple_median": 2.8, "sde_multiple_high": 3.5,
            "revenue_multiple": 0.65,
            "notes": "BizBuySell Insight, 2025 broker reports",
        })
    }

    #[test]
    fn inverted_sde_band_is_rejected_with_reasons_not_upserted() {
        let mut bad = multiples_entry("HVAC");
        // Median above the high end of the band: implausible, must not upsert.
        bad["sde_multiple_median"] = json!(4.0);
        let data = json!({ "trades": [bad, multiples_entry("Plumbing")] });
        let (records, rejected, _) =
            collect_valuation_records(&taxonomy::seed_entries(), &data, "2025");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "US:Plumbing");
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].key, "US:HVAC");
        assert!(rejected[0]
            .reasons
            .iter()
            .any(|r| r.contains("sde_multiple")));
    }

    #[test]
    fn model_phrasing_lands_on_the_canonical_key_and_national_stamp() {
        // "HVAC/R" must key as US:HVAC with the stored `trade` field agreeing,
        // stamped state=US + year so the ingest lifts market = "US".
        let data = json!({ "trades": [multiples_entry("HVAC/R")] });
        let (records, rejected, unknown) =
            collect_valuation_records(&taxonomy::seed_entries(), &data, "2025");
        assert!(rejected.is_empty());
        assert!(unknown.is_empty());
        let (key, rec) = &records[0];
        assert_eq!(key, "US:HVAC");
        assert_eq!(rec["trade"], "HVAC");
        assert_eq!(rec["state"], "US");
        assert_eq!(rec["year"], "2025");
    }

    #[test]
    fn non_positive_multiple_is_rejected_and_unknown_trade_flagged() {
        let mut bad = multiples_entry("Roofing");
        bad["revenue_multiple"] = json!(-0.5);
        let data = json!({ "trades": [bad] });
        let (records, rejected, unknown) =
            collect_valuation_records(&taxonomy::seed_entries(), &data, "2025");
        // Unknown label is flagged even when the record is rejected on values.
        assert!(records.is_empty());
        assert_eq!(rejected[0].key, "US:Roofing");
        assert!(rejected[0]
            .reasons
            .iter()
            .any(|r| r.contains("revenue_multiple")));
        assert_eq!(unknown, vec!["Roofing".to_string()]);
    }
}
