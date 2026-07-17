//! Example app: agentic web research via the Claude Code CLI engine.
//! Serves as the template for research-style use cases where a crawler
//! can't cut it — the agent searches, reads pages, and synthesizes.

use async_trait::async_trait;
use pumper_core::{salvage_json, AppContext, ResearchRequest, Result, ScrapeApp};
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
         \"effort\": \"low|medium|high|xhigh|max\", \"max_turns\": 25, \
         \"session_id\": \"...\" (resume a prior run's session_id to drill down on \
         its accumulated context instead of researching from scratch — the query \
         is then a follow-up question), \"max_budget_usd\": 0.0 (per-run Claude \
         spend ceiling)}"
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let query = ctx.require_str("query")?.to_string();
        let max_turns = ctx
            .params
            .get("max_turns")
            .and_then(Value::as_u64)
            .map(|turns| turns as u32);
        // Resume a prior run's agent session so a follow-up drills down on the
        // context it already built, instead of re-paying the full search+fetch+
        // synthesize loop. The prior run returns `session_id` in its result.
        let session_id =
            ctx.params.get("session_id").and_then(Value::as_str).map(String::from);
        let resumed = session_id.is_some();
        let max_budget_usd = ctx.params.get("max_budget_usd").and_then(Value::as_f64);
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

        // A resumed turn is a follow-up: the agent already holds the topic and its
        // sources in session, so a full "you are a web research agent…" preamble
        // would waste turns re-establishing context. Both prompts pin the SAME
        // JSON shape so a resumed report is held to the same contract.
        let shape = "Respond with ONLY a JSON object (no markdown fences, no prose) of this \
             shape:\n{\"summary\": string, \"key_findings\": string[], \
             \"sources\": [{\"url\": string, \"title\": string}]}";
        let prompt = if session_id.is_some() {
            format!(
                "Follow-up on the research so far. Using the context you already have (search \
                 further only if needed):\n\n{query}\n\n{shape}"
            )
        } else {
            format!(
                "You are a web research agent. Research the topic below using web search and \
                 page fetches. Cross-check important claims across at least two sources.\n\n\
                 Topic: {query}\n\n{shape}"
            )
        };

        let mut request = ResearchRequest::new(prompt).with_role(role);
        request.max_turns = max_turns;
        request.model = model;
        request.effort = effort;
        request.resume_session = session_id;
        request.max_budget_usd = max_budget_usd;
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

        // Before giving up on structure, salvage a fenced/prose-wrapped object from
        // the raw text — no re-run, no extra cost, on text already paid for (the
        // same recovery the four trades apps use via `research_json`). `structured`
        // still means "matched the promised shape", not merely "some JSON came
        // back", so a hallucinated/wrong-shape object isn't stamped trustworthy.
        let parsed = output.json.clone().or_else(|| salvage_json(&output.text));
        let structured = parsed.as_ref().is_some_and(is_report_shaped);
        let report = if structured {
            parsed.unwrap()
        } else {
            Value::String(output.text.clone())
        };
        let result = json!({
            "query": query,
            "report": report,
            "structured": structured,
            "resumed": resumed,
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
