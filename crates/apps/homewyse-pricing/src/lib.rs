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

        let prompt = format!(
            "You are a home-services pricing analyst. Using web search and page fetches, \
             research the TYPICAL PRICE A CUSTOMER PAYS in {locality} ({year}) for common \
             jobs in these trades: Plumbing, Electrical, HVAC, Landscaping, Pool service. \
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
        if let Some(trades) = data.get("trades").and_then(Value::as_array) {
            for t in trades {
                let trade = t.get("trade").and_then(Value::as_str).unwrap_or("").to_string();
                let mut job_count = 0;
                if let Some(jobs) = t.get("jobs").and_then(Value::as_array) {
                    for j in jobs {
                        let job = j.get("job").and_then(Value::as_str).unwrap_or("").to_string();
                        if trade.is_empty() || job.is_empty() {
                            continue;
                        }
                        job_count += 1;
                        all_records.push((
                            format!("{locality}:{trade}:{job}"),
                            json!({
                                "locality": locality,
                                "year": year,
                                "trade": trade,
                                "job": job,
                                "unit": j.get("unit").and_then(Value::as_str).unwrap_or("flat"),
                                "low": j.get("low"),
                                "median": j.get("median"),
                                "high": j.get("high"),
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

        Ok(json!({
            "source": format!("agentic/pricing/{year}"),
            "locality": locality,
            "year": year,
            "trades": trade_summaries,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
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

/// Best-effort recovery of a JSON object the agent emitted but the engine couldn't
/// parse into `output.json` — the common failure is a markdown ```json fence or a
/// leading/trailing sentence. No re-run, no cost: it works on the raw text we already
/// paid for. Returns None only when there's no parseable object at all.
fn salvage_json(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Some(v);
    }
    let unfenced = strip_code_fence(trimmed);
    if let Ok(v) = serde_json::from_str::<Value>(unfenced.trim()) {
        return Some(v);
    }
    let span = first_balanced_object(unfenced)?;
    serde_json::from_str::<Value>(span).ok()
}

/// Strip a leading ```json (or bare ```) fence and its closing ``` if present.
fn strip_code_fence(text: &str) -> &str {
    let t = text.trim();
    let Some(rest) = t.strip_prefix("```") else {
        return t;
    };
    // drop the optional language tag on the fence's first line
    let rest = rest.split_once('\n').map(|(_, r)| r).unwrap_or(rest);
    rest.strip_suffix("```").unwrap_or(rest).trim()
}

/// The first brace-balanced `{...}` span in `text`, respecting quoted strings and
/// escapes so a `}` inside a string value doesn't close the object early.
fn first_balanced_object(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let start = text.find('{')?;
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for i in start..bytes.len() {
        let c = bytes[i];
        if in_str {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_str = false;
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[start..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salvages_a_clean_object() {
        let v = salvage_json(r#"{"locality":"Texas","trades":[]}"#).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_a_fenced_object() {
        let raw = "```json\n{\"locality\":\"Texas\",\"trades\":[]}\n```";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_an_object_wrapped_in_prose() {
        let raw = "Here is the pricing data you asked for:\n{\"locality\":\"Texas\",\
                   \"trades\":[{\"trade\":\"Plumbing\",\"jobs\":[]}]}\nHope that helps!";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
        assert_eq!(v["trades"][0]["trade"], "Plumbing");
    }

    #[test]
    fn does_not_close_early_on_a_brace_inside_a_string() {
        let raw = r#"prefix {"note":"a } inside a string","ok":true} suffix"#;
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["note"], "a } inside a string");
    }

    #[test]
    fn returns_none_when_there_is_no_object() {
        assert!(salvage_json("I could not find reliable pricing data.").is_none());
    }
}
