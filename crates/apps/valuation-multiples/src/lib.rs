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
use pumper_core::{AppContext, Error, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};

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

        let prompt = format!(
            "You are a business-valuation analyst for small US home-services companies. \
             For {year}, compile the typical SMALL-BUSINESS valuation multiples for each of \
             these trades: Plumbing, Electrical, HVAC, Landscaping, Pool service. Use web \
             search + business-broker sources (e.g. BizBuySell Insight, brokerage reports). \
             Give the seller's-discretionary-earnings (SDE) multiple as a median with a \
             low/high band, plus a typical revenue multiple.\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"year\": string, \"trades\": [{{\"trade\": string, \
             \"sde_multiple_median\": number, \"sde_multiple_low\": number, \
             \"sde_multiple_high\": number, \"revenue_multiple\": number, \
             \"notes\": string}}]}}\n\
             Multiples are ratios (e.g. 2.5 means 2.5x SDE). Include all 5 trades."
        );

        let mut request = ResearchRequest::new(prompt).with_role(role);
        request.max_turns = max_turns;
        request.model = ctx.params.get("model").and_then(Value::as_str).map(String::from);
        request.effort = ctx.params.get("effort").and_then(Value::as_str).map(String::from);
        let output = ctx.engines.claude.research(request).await?;

        let artifact = match &output.json {
            Some(j) => serde_json::to_vec_pretty(j)?,
            None => output.text.clone().into_bytes(),
        };
        ctx.save_artifact("research.json", &artifact).await?;

        let data = output.json.clone().ok_or_else(|| {
            Error::App(format!(
                "valuation-multiples: agent did not return JSON (text starts: {})",
                output.text.chars().take(160).collect::<String>()
            ))
        })?;

        let mut all_records: Vec<(String, Value)> = Vec::new();
        if let Some(trades) = data.get("trades").and_then(Value::as_array) {
            for t in trades {
                let trade = t
                    .get("trade")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if trade.is_empty() {
                    continue;
                }
                let mut rec = t.clone();
                // National by trade — state = "US" so the ingest lifts market = "US".
                rec["state"] = json!("US");
                rec["year"] = json!(year);
                all_records.push((format!("US:{trade}"), rec));
            }
        }

        if all_records.is_empty() {
            return Err(Error::App(
                "valuation-multiples: agent JSON contained no trades".into(),
            ));
        }

        let summary = ctx.upsert_many("valuation", &all_records).await?;

        Ok(json!({
            "source": format!("agentic/valuation/{year}"),
            "year": year,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
        }))
    }
}
