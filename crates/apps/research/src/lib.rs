//! Example app: agentic web research via the Claude Code CLI engine.
//! Serves as the template for research-style use cases where a crawler
//! can't cut it — the agent searches, reads pages, and synthesizes.

use async_trait::async_trait;
use pumper_core::{AppContext, ResearchRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Research;

#[async_trait]
impl ScrapeApp for Research {
    fn name(&self) -> &'static str {
        "research"
    }

    fn description(&self) -> &'static str {
        "Web research via Claude Code CLI. Params: {\"query\": \"...\", \
         \"role\": \"research|compose\", \"model\": \"claude-...\", \
         \"effort\": \"low|medium|high|xhigh|max\", \"max_turns\": 25}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let query = ctx.require_str("query")?.to_string();
        let max_turns = ctx
            .params
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|turns| turns as u32);
        // Model/effort are chosen by the caller: default to the "research" role
        // (Sonnet, normal reasoning); an app can pass "compose" for Opus @ xhigh,
        // or override model/effort directly.
        let role = ctx
            .params
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("research")
            .to_string();
        let model = ctx.params.get("model").and_then(Value::as_str).map(String::from);
        let effort = ctx.params.get("effort").and_then(Value::as_str).map(String::from);

        let prompt = format!(
            "You are a web research agent. Research the topic below using web search and \
             page fetches. Cross-check important claims across at least two sources.\n\n\
             Topic: {query}\n\n\
             Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
             {{\"summary\": string, \"key_findings\": string[], \
             \"sources\": [{{\"url\": string, \"title\": string}}]}}"
        );

        let mut request = ResearchRequest::new(prompt).with_role(role);
        request.max_turns = max_turns;
        request.model = model;
        request.effort = effort;
        let output = ctx.engines.claude.research(request).await?;

        let report = output
            .json
            .clone()
            .unwrap_or_else(|| Value::String(output.text.clone()));
        let result = json!({
            "query": query,
            "report": report,
            "structured": output.json.is_some(),
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
            "session_id": output.session_id,
        });
        ctx.save_artifact("report.json", &serde_json::to_vec_pretty(&result)?)
            .await?;
        Ok(result)
    }
}
