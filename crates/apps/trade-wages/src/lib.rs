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

        let prompt = format!(
            "You are a US labor-market analyst. For BLS OEWS year {year}, compile the \
             national wage band for the tradesperson occupation behind each of these \
             home-services trades: Plumbing, Electrical, HVAC, Landscaping, Pool service. \
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
                "trade-wages: agent did not return JSON (text starts: {})",
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
                rec["source"] = json!("BLS OEWS (agentic)");
                all_records.push((format!("US:{trade}"), rec));
            }
        }

        if all_records.is_empty() {
            return Err(Error::App(
                "trade-wages: agent JSON contained no trades".into(),
            ));
        }

        let summary = ctx.upsert_many("wages", &all_records).await?;

        Ok(json!({
            "source": format!("agentic/wages/{year}"),
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
