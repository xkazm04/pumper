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
        // Actually use the json_schema guardrail so the model is steered to the
        // shape we promise downstream instead of accepting any object it returns.
        request.json_schema = Some(json!({
            "type": "object",
            "required": ["summary", "key_findings", "sources"],
            "properties": {
                "summary": { "type": "string" },
                "key_findings": { "type": "array", "items": { "type": "string" } },
                "sources": { "type": "array" }
            }
        }));
        let output = ctx.research(request).await?;

        // `structured` must mean "matched the promised shape", not merely "some
        // JSON came back" — otherwise a hallucinated/wrong-shape object is stamped
        // structured:true and stored as if trustworthy.
        let structured = output.json.as_ref().is_some_and(is_report_shaped);
        let report = if structured {
            output.json.clone().unwrap()
        } else {
            Value::String(output.text.clone())
        };
        let result = json!({
            "query": query,
            "report": report,
            "structured": structured,
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

/// True when a research report matches the promised shape: a `summary` string
/// plus `key_findings` and `sources` arrays. Guards against marking a
/// hallucinated or wrong-shape object as `structured`.
fn is_report_shaped(v: &Value) -> bool {
    v.get("summary").is_some_and(Value::is_string)
        && v.get("key_findings").is_some_and(Value::is_array)
        && v.get("sources").is_some_and(Value::is_array)
}
