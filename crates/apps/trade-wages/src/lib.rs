//! Tradesperson WAGE bands for US home-services trades, via the Claude research
//! engine.
//!
//! For each trade Ledgerline serves (plumbing, electrical, HVAC, landscaping, pool):
//! the occupation's BLS OEWS wage band — entry (10th percentile) / median / experienced
//! (90th percentile), both hourly and annual — plus the SOC occupation the figures
//! come from and the national employment count. This grounds a "what to pay your first
//! hire / a fair wage" read: the entry band is the new-hire number, the median the
//! going rate, the 90th percentile the top-talent ceiling. Upserted into the `wages`
//! dataset.
//!
//! Data type: OCCUPATION WAGES. Access: the local Claude CLI (no API key; costs money
//! per run). BLS OEWS is authoritative but its TIMESERIES API returns no data for these
//! series and the QCEW slice endpoint 404s (both dead-ended) — so the agent WEB-FETCHES
//! the current OEWS occupation figures (bls.gov/oes) during research, the same way the
//! tax pipeline pulled live rates. National by trade in ONE call; per-state wage detail
//! can layer on later (census-density already carries a per-state payroll signal).
//! Params: {"year": "2024", "role": "research|compose", "max_turns": 25}.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};
use trades_common::taxonomy;
use trades_common::unified;
use trades_common::validate::{self, Rejection};

pub struct TradeWages;

const DEFAULT_YEAR: &str = "2024";

#[async_trait]
impl ScrapeApp for TradeWages {
    fn name(&self) -> &'static str {
        "trade-wages"
    }

    fn description(&self) -> &'static str {
        "Tradesperson WAGE bands for US home-services trades (plumbing, electrical, HVAC, \
         landscaping, pool), via the Claude research engine — the BLS OEWS occupation's \
         entry (10th pct) / median / experienced (90th pct) hourly + annual wage per \
         trade, with the SOC code + national employment. Upserted into the `wages` \
         dataset; grounds a 'what to pay your first hire' read. No API key (local Claude \
         CLI; costs money per run). Params: {\"year\": \"2024\", \"role\": \
         \"research|compose\", \"max_turns\": 25}."
    }

    fn default_params(&self) -> Value {
        json!({ "year": DEFAULT_YEAR })
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

        // Vintage freshness gate: a BLS OEWS vintage is frozen, so re-running for a
        // year we already hold would re-pay a 25-turn agentic run for identical
        // figures. Skip unless `force: true`. Sentinel = the first trade's row.
        let sentinel = format!("US:{}", taxonomy::Trade::ALL[0].label());
        if trades_common::vintage_held(&ctx, "trade-wages", "wages", &sentinel, &year).await? {
            let held = ctx.datasets.list("trade-wages", "wages", 100).await?.len();
            return Ok(json!({
                "source": format!("agentic/wages/{year}"),
                "year": year,
                "skipped": "vintage already held (pass force:true to re-run)",
                "records": held,
                "cost_usd": 0.0,
            }));
        }

        let trades = taxonomy::prompt_list();
        let prompt = format!(
            "You are a US labor-market analyst. For BLS OEWS year {year}, compile the \
             national wage band for the tradesperson occupation behind each of these \
             home-services trades: {trades}. \
             Use web search on bls.gov/oes to get the current figures. Map each trade to \
             its best-fit BLS SOC occupation (e.g. Plumbing -> 47-2152 Plumbers, \
             Pipefitters & Steamfitters; Electrical -> 47-2111 Electricians; HVAC -> \
             49-9021; Landscaping/Pool -> 37-3011 Landscaping & Groundskeeping Workers or \
             the closest fit).\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"year\": string, \"trades\": [{{\"trade\": string, \"soc_code\": string, \
             \"occupation\": string, \"median_hourly\": number, \"median_annual\": number, \
             \"entry_hourly\": number, \"entry_annual\": number, \"experienced_hourly\": number, \
             \"experienced_annual\": number, \"employment\": number}}]}}\n\
             entry = 10th percentile, experienced = 90th percentile. Hourly in dollars \
             (e.g. 30.10), annual in whole dollars. Include all 5 trades."
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
        // Constrain the final answer to the wage schema (`claude --json-schema`) so the
        // CLI validates structure; salvage_json below still catches anything it misses.
        request.json_schema = Some(wages_schema());
        let (data, output) = trades_common::research_json(&ctx, "trade-wages", request).await?;

        let (all_records, rejected, unknown_trades) = collect_wage_records(&data, &year);

        if all_records.is_empty() {
            return Err(Error::App(
                "trade-wages: agent JSON contained no plausible trades".into(),
            ));
        }
        let summary = ctx.upsert_many("wages", &all_records).await?;

        // Cross-source layer: rebuild trades/operator_economics from the current
        // state of all four source datasets (mirrors grants-common's sync_unified).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/wages/{year}"),
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
/// - Plausibility guards: wage bands must be ordered (entry ≤ median ≤
///   experienced, hourly + annual) and all magnitudes positive. Violators
///   are rejected with reasons; valid trades still upsert.
/// - Trade labels the model returned that don't map to a canonical trade are
///   kept raw (not dropped) but surfaced so drift is visible.
fn collect_wage_records(
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
            // Normalize to a canonical label so phrasing drift can't mint a
            // duplicate key; unknown labels keep the raw string and are flagged.
            let (trade, known) = taxonomy::canonicalize(&raw);
            if !known {
                unknown_trades.push(raw.clone());
            }
            let key = format!("US:{trade}");
            let mut reasons = Vec::new();
            for f in [
                "entry_hourly",
                "median_hourly",
                "experienced_hourly",
                "entry_annual",
                "median_annual",
                "experienced_annual",
                "employment",
            ] {
                validate::require_positive(&mut reasons, f, validate::num(t, f));
            }
            validate::require_monotone(
                &mut reasons,
                "hourly",
                validate::num(t, "entry_hourly"),
                validate::num(t, "median_hourly"),
                validate::num(t, "experienced_hourly"),
            );
            validate::require_monotone(
                &mut reasons,
                "annual",
                validate::num(t, "entry_annual"),
                validate::num(t, "median_annual"),
                validate::num(t, "experienced_annual"),
            );
            if !reasons.is_empty() {
                rejected.push(Rejection { key, reasons });
                continue;
            }
            let mut rec = t.clone();
            // Store the canonical label so the record key and its `trade`
            // field agree, regardless of the model's phrasing.
            rec["trade"] = json!(trade);
            // National by trade — state = "US" so the ingest lifts market = "US".
            rec["state"] = json!("US");
            rec["year"] = json!(year);
            rec["source"] = json!("BLS OEWS (agentic)");
            all_records.push((key, rec));
        }
    }
    (all_records, rejected, unknown_trades)
}

/// Structured-output contract for `claude --json-schema`. Lenient (extra fields
/// tolerated) so a valid answer is never rejected, but pins the wage-band shape.
fn wages_schema() -> Value {
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
                        "soc_code": { "type": "string" },
                        "occupation": { "type": "string" },
                        "median_hourly": { "type": "number" },
                        "median_annual": { "type": "number" },
                        "entry_hourly": { "type": "number" },
                        "entry_annual": { "type": "number" },
                        "experienced_hourly": { "type": "number" },
                        "experienced_annual": { "type": "number" },
                        "employment": { "type": "number" }
                    },
                    "required": [
                        "trade", "median_hourly", "median_annual", "entry_hourly",
                        "entry_annual", "experienced_hourly", "experienced_annual"
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

    // A wage entry shaped like the agent's structured answer (BLS OEWS national
    // figures for one trade).
    fn wage_entry(trade: &str) -> Value {
        json!({
            "trade": trade, "soc_code": "47-2152",
            "occupation": "Plumbers, Pipefitters & Steamfitters",
            "entry_hourly": 18.4, "median_hourly": 30.1, "experienced_hourly": 50.0,
            "entry_annual": 38_300, "median_annual": 62_600, "experienced_annual": 104_000,
            "employment": 469_000,
        })
    }

    #[test]
    fn out_of_order_wage_band_is_rejected_with_reasons_not_upserted() {
        let mut bad = wage_entry("Electrical");
        // Median above the 90th percentile: implausible, must not upsert.
        bad["median_hourly"] = json!(60.0);
        let data = json!({ "trades": [bad, wage_entry("Plumbing")] });
        let (records, rejected, _) = collect_wage_records(&data, "2024");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "US:Plumbing");
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0].key, "US:Electrical");
        assert!(rejected[0].reasons.iter().any(|r| r.contains("hourly")));
    }

    #[test]
    fn phrasing_drift_canonicalizes_the_key_so_duplicates_cannot_mint() {
        // "Plumbers" (a model phrasing) must land on the same key as "Plumbing"
        // and the stored `trade` field must agree with the key.
        let data = json!({ "trades": [wage_entry("Plumbers")] });
        let (records, rejected, unknown) = collect_wage_records(&data, "2024");
        assert!(rejected.is_empty());
        assert!(unknown.is_empty());
        let (key, rec) = &records[0];
        assert_eq!(key, "US:Plumbing");
        assert_eq!(rec["trade"], "Plumbing");
        assert_eq!(rec["state"], "US");
        assert_eq!(rec["year"], "2024");
        assert_eq!(rec["source"], "BLS OEWS (agentic)");
    }

    #[test]
    fn unknown_trade_labels_are_kept_and_flagged_not_dropped() {
        let data = json!({ "trades": [wage_entry("Roofing")] });
        let (records, _, unknown) = collect_wage_records(&data, "2024");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "US:Roofing");
        assert_eq!(unknown, vec!["Roofing".to_string()]);
    }

    #[test]
    fn non_positive_magnitudes_are_rejected_even_when_ordered() {
        let mut bad = wage_entry("HVAC");
        bad["employment"] = json!(0);
        let data = json!({ "trades": [bad] });
        let (records, rejected, _) = collect_wage_records(&data, "2024");
        assert!(records.is_empty());
        assert!(rejected[0].reasons.iter().any(|r| r.contains("employment")));
    }
}
