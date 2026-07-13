//! US STATE + FEDERAL income-tax reference for solo-trades operators, via the Claude
//! research engine.
//!
//! For each US state (50 + DC): the INDIVIDUAL income-tax structure a sole
//! proprietor's business income is taxed under — type (none / flat / graduated), top
//! marginal rate, and the top-bracket threshold. Plus a single FEDERAL record with the
//! small-business constants a trades operator needs: self-employment tax rate, the QBI
//! deduction, the standard deduction, and the §179 expensing limit. This grounds the
//! forecast's state-tax set-aside component (which today excludes state tax when the
//! operator's state has no data) and a future tax read. Upserted into the `tax` dataset.
//!
//! Data type: TAX RULES. Access: the local Claude CLI (no API key; costs money per
//! run). State income-tax rates are well-documented, finite facts, so ONE call returns
//! all 52 jurisdictions — the agent synthesizes + web-verifies them in a single
//! structured response rather than 51 per-state runs. Params: {"year": "2025",
//! "role": "research|compose", "max_turns": 30}.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct StateTax;

const DEFAULT_YEAR: &str = "2025";

#[async_trait]
impl ScrapeApp for StateTax {
    fn name(&self) -> &'static str {
        "state-tax"
    }

    fn description(&self) -> &'static str {
        "US state + federal income-tax reference for a sole-proprietor trades operator, \
         via the Claude research engine — per-state individual income-tax type / top \
         marginal rate / top-bracket threshold (all 50 + DC), plus a federal record \
         (SE tax, QBI, standard deduction, §179 limit). Upserted into the `tax` dataset; \
         grounds the forecast state-tax set-aside. No API key (local Claude CLI; costs \
         money per run). Params: {\"year\": \"2025\", \"role\": \"research|compose\", \
         \"max_turns\": 30}."
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
            .or(Some(30));

        let prompt = format!(
            "You are a US small-business tax analyst. For tax year {year}, compile the \
             INDIVIDUAL income-tax structure each US state (all 50 states + DC) applies to \
             a sole proprietor's business income, plus the federal small-business \
             constants. Use web search to verify current figures.\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"year\": string, \
             \"federal\": {{\"self_employment_tax_rate\": number, \"qbi_deduction_pct\": number, \
             \"standard_deduction_single\": number, \"section_179_limit\": number, \
             \"top_marginal_rate\": number}}, \
             \"states\": [{{\"state\": string (2-letter USPS code), \"state_name\": string, \
             \"income_tax_type\": \"none\"|\"flat\"|\"graduated\", \"top_marginal_rate\": number, \
             \"top_bracket_threshold\": number, \"notes\": string}}]}}\n\
             Include ALL 50 states + DC (51 entries). Rates are percentages (e.g. 13.3, and \
             0 for no-income-tax states). top_bracket_threshold is the single-filer income \
             where the top rate applies (0 for flat/none)."
        );

        let mut request = ResearchRequest::new(prompt).with_role(role);
        request.max_turns = max_turns;
        request.model = ctx.params.get("model").and_then(Value::as_str).map(String::from);
        request.effort = ctx.params.get("effort").and_then(Value::as_str).map(String::from);
        // Metered seam: records a cost event against the job, honors budget_usd,
        // and serves identical re-runs from the research cache (see core/app.rs).
        let output = ctx.research(request).await?;

        let artifact = match &output.json {
            Some(j) => serde_json::to_vec_pretty(j)?,
            None => output.text.clone().into_bytes(),
        };
        ctx.save_artifact("research.json", &artifact).await?;

        let data = output.json.clone().ok_or_else(|| {
            Error::App(format!(
                "state-tax: agent did not return JSON (text starts: {})",
                output.text.chars().take(160).collect::<String>()
            ))
        })?;

        let mut all_records: Vec<(String, Value)> = Vec::new();

        // Federal small-business constants — one national record (state = "US" so the
        // ingest lifts market = "US").
        if let Some(fed) = data.get("federal").filter(|v| v.is_object()) {
            let mut rec = fed.clone();
            rec["level"] = json!("federal");
            rec["state"] = json!("US");
            rec["state_name"] = json!("United States");
            rec["year"] = json!(year);
            all_records.push(("federal:US".to_string(), rec));
        }

        // Per-state individual income-tax structure.
        if let Some(states) = data.get("states").and_then(Value::as_array) {
            for s in states {
                let st = s
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_uppercase();
                if st.is_empty() {
                    continue;
                }
                let mut rec = s.clone();
                rec["level"] = json!("state");
                rec["state"] = json!(st);
                rec["year"] = json!(year);
                all_records.push((format!("state:{st}"), rec));
            }
        }

        if all_records.len() < 20 {
            return Err(Error::App(format!(
                "state-tax: only {} records parsed — expected ~52 (federal + 51 states)",
                all_records.len()
            )));
        }

        let summary = ctx.upsert_many("tax", &all_records).await?;

        Ok(json!({
            "source": format!("agentic/tax/{year}"),
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
