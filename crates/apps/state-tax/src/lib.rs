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
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, ResearchRequest, Result, ScrapeApp,
};
use serde_json::{json, Value};
use trades_common::coverage;
use trades_common::unified;
use trades_common::validate::{self, Rejection};

pub struct StateTax;

const DEFAULT_YEAR: &str = "2025";

/// The 50 states + DC, enumerated in code so completeness is checked against a
/// fixed roster rather than a run-count heuristic. Missing entries are reported.
const US_JURISDICTIONS: [&str; 51] = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
];

/// Federal fields validated as rates (percentage points — `validate::RATE_UNIT`).
const FEDERAL_RATE_FIELDS: [&str; 3] = [
    "self_employment_tax_rate",
    "qbi_deduction_pct",
    "top_marginal_rate",
];

/// Every federal field that must be stored as a JSON NUMBER. The dollar
/// constants are here too: they are not rate-validated, but the model quotes
/// them just as readily (`"$1,250,000"`), and a quoted dollar figure drops out
/// of a consumer exactly the same way a quoted rate does.
const FEDERAL_NUMERIC_FIELDS: [&str; 5] = [
    "self_employment_tax_rate",
    "qbi_deduction_pct",
    "top_marginal_rate",
    "standard_deduction_single",
    "section_179_limit",
];

/// Every per-state field that must be stored as a JSON number.
const STATE_NUMERIC_FIELDS: [&str; 2] = ["top_marginal_rate", "top_bracket_threshold"];

/// The closed vocabulary `income_tax_type` may hold, and the phrasings a model
/// reaches for. Order is precedence: the `none` rule has to see "no income tax"
/// before the `graduated` rule's substrings do.
const INCOME_TAX_TYPES: &[(&str, &[validate::Phrase])] = &[
    (
        "none",
        &[
            validate::Phrase::Exact("none"),
            validate::Phrase::Exact("n/a"),
            validate::Phrase::Prefix("no "),
            validate::Phrase::Contains("no income tax"),
            validate::Phrase::Contains("no state income tax"),
        ],
    ),
    (
        "flat",
        &[
            validate::Phrase::Contains("flat"),
            validate::Phrase::Contains("single rate"),
            validate::Phrase::Contains("single-rate"),
        ],
    ),
    (
        "graduated",
        &[
            validate::Phrase::Contains("graduat"),
            validate::Phrase::Contains("progressiv"),
            validate::Phrase::Contains("bracket"),
            validate::Phrase::Contains("tiered"),
            validate::Phrase::Contains("marginal"),
        ],
    ),
];

/// Normalize the agent's phrasing of a state's income-tax structure onto the
/// closed vocabulary the prompt asks for. `None` for anything unclassifiable —
/// the record is REJECTED, never guessed at.
///
/// Left out of `tax_schema()`'s `income_tax_type` as a JSON-schema `enum` on
/// purpose: the schema constrains the CLI's final answer, so a hard enum turns
/// a recoverable "progressive" into a failed 30-turn call. Normalizing here
/// keeps the answer and still closes the vocabulary at the store.
fn normalize_income_tax_type(raw: &str) -> Option<&'static str> {
    validate::normalize_choice(raw, INCOME_TAX_TYPES)
}

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

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "year": {
                        "type": "string",
                        "description": "Tax year the rates are compiled for. Doubles as the vintage freshness key — a year already held costs nothing."
                    },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": { "type": "integer", "minimum": 1 },
                    "force": {
                        "type": "boolean",
                        "description": "Bypass the vintage freshness gate and re-pay the ~30-turn research run."
                    },
                    "allow_shrink": {
                        "type": "boolean",
                        "description": "Let a run that covered materially less than the 50-states-+-DC roster still tombstone the jurisdictions it did not return. Off by default: a short answer suppresses removal detection instead of deleting the shortfall."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description:
                        "Compile a tax year (free no-op when that vintage is already held)",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Force a re-research of a held vintage at high effort",
                    params: json!({ "year": DEFAULT_YEAR, "force": true, "effort": "high", "max_turns": 40 }),
                },
            ],
            output_shape: Some(
                "{source, year, records, states_covered, states_expected, missing_states, \
                 coverage: {unit, covered, expected, ratio, floor, short, missing}, \
                 warnings: [string], new, changed, unchanged, removed, \
                 removals_suppressed, rejected: [{key, reasons}], rejected_count, \
                 unified: {dataset, trust, new, changed, unchanged, \
                 inputs_truncated, join_complete}, index_datasets, cost_usd, duration_ms, num_turns} — or \
                 {source, year, skipped, records, cost_usd: 0.0} when the vintage gate holds",
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
            .or(Some(30));

        // Vintage freshness gate: tax-year constants are frozen once published, so
        // a re-run for a year we already hold would re-pay a 30-turn agentic run to
        // reproduce identical facts. Skip unless `force: true`.
        if trades_common::vintage_held(&ctx, "state-tax", "tax", "federal:US", &year).await? {
            let held = ctx.datasets.list("state-tax", "tax", 200).await?.len();
            return Ok(json!({
                "source": format!("agentic/tax/{year}"),
                "year": year,
                "skipped": "vintage already held (pass force:true to re-run)",
                "records": held,
                "cost_usd": 0.0,
            }));
        }

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
        // Constrain the final answer to the tax schema (`claude --json-schema`);
        // salvage_json below still catches anything the schema path misses.
        request.json_schema = Some(tax_schema());
        // Pin the derivation spec before `request` is consumed by the call.
        let prov = trades_common::research_provenance(&ctx, "state-tax", &request).await;
        let (data, output) = trades_common::research_json(&ctx, "state-tax", request).await?;

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut rejected: Vec<Rejection> = Vec::new();

        // Federal small-business constants — one national record (state = "US" so the
        // ingest lifts market = "US"). Rate fields must fall in [0,100] and be
        // percentage points, not fractions (`validate::RATE_UNIT`).
        if let Some(fed) = data.get("federal").filter(|v| v.is_object()) {
            let mut reasons = Vec::new();
            for f in FEDERAL_RATE_FIELDS {
                validate::require_rate(&mut reasons, f, validate::num(fed, f));
            }
            if reasons.is_empty() {
                let mut rec = fed.clone();
                // Store the validated NUMBER, not the model's raw value: a quoted
                // `"15.3"` validates via `validate::num` but, stored raw, reads
                // back as a non-number and drops out of every `as_f64` consumer.
                validate::store_numbers(&mut rec, &FEDERAL_NUMERIC_FIELDS);
                rec["level"] = json!("federal");
                rec["state"] = json!("US");
                rec["state_name"] = json!("United States");
                rec["year"] = json!(year);
                all_records.push(("federal:US".to_string(), rec));
            } else {
                rejected.push(Rejection {
                    key: "federal:US".to_string(),
                    reasons,
                });
            }
        }

        // Per-state individual income-tax structure. Top marginal rate ∈ [0,100].
        let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
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
                let mut reasons = Vec::new();
                validate::require_rate(
                    &mut reasons,
                    "top_marginal_rate",
                    validate::num(s, "top_marginal_rate"),
                );
                // `income_tax_type` is a CLOSED vocabulary the prompt states and
                // nothing enforced: "progressive", "Graduated" and "N/A" all
                // stored and flowed into `state_tax_context`. Normalize onto the
                // three canonical values, reject what will not classify — the
                // shape `state-licensing::normalize_requirement_level` already
                // established, now shared as `validate::normalize_choice`.
                let tax_type = s
                    .get("income_tax_type")
                    .and_then(Value::as_str)
                    .and_then(normalize_income_tax_type);
                if tax_type.is_none() {
                    reasons.push(format!(
                        "income_tax_type: {:?} not one of none|flat|graduated",
                        s.get("income_tax_type")
                    ));
                }
                if !reasons.is_empty() {
                    rejected.push(Rejection {
                        key: format!("state:{st}"),
                        reasons,
                    });
                    continue;
                }
                let mut rec = s.clone();
                validate::store_numbers(&mut rec, &STATE_NUMERIC_FIELDS);
                rec["level"] = json!("state");
                rec["state"] = json!(st);
                rec["income_tax_type"] = json!(tax_type.expect("checked above"));
                rec["year"] = json!(year);
                present.insert(st.clone());
                all_records.push((format!("state:{st}"), rec));
            }
        }

        // Completeness against the fixed 50-states-+-DC roster. This is not a
        // report any more: it decides whether this run may tombstone.
        let cov = coverage::Coverage::of_roster("states", &US_JURISDICTIONS, &present);
        let mut warnings: Vec<String> = cov.warning().into_iter().collect();

        if present.is_empty() {
            return Err(Error::App(
                "state-tax: agent JSON contained no plausible state records".into(),
            ));
        }

        // Full 50-state + DC snapshot, so a *complete* run syncs: a state that
        // drops out is marked removed instead of lingering as stale data.
        //
        // A SHORT run does not. `sync_many`'s own protection — the
        // degrading-source removal guard — cannot engage for this family
        // (`enforce` defaults off, and nothing here calls `observe_extraction`),
        // and core's own doc says `detect_removed` "already refuses an *empty*
        // batch; a partial batch is the case that guard does not cover". So the
        // completeness floor in `coverage::write_snapshot` is what stands between
        // a 30-of-51 answer and 21 tombstoned states; read its doc comment before
        // touching this. `allow_shrink: true` is the escape hatch.
        //
        // Provenance (M12): the same derivation-spec pin its sibling research
        // apps use — an agentic answer has no single source URL, so rules_hash
        // is the only honest stamp.
        let write = coverage::write_snapshot(
            &ctx,
            "tax",
            &all_records,
            prov,
            &cov,
            trades_common::allow_shrink(&ctx),
        )
        .await?;
        let summary = write.summary;
        if let Some(reason) = &write.removals_suppressed {
            warnings.push(reason.clone());
        }

        // Cross-source layer: state-tax contributes the federal + illustrative-state
        // tax context to trades/operator_economics (mirrors grants-common's sync).
        let join = unified::sync_operator_economics(&ctx).await?;

        let mut out = json!({
            "source": format!("agentic/tax/{year}"),
            "year": year,
            "records": all_records.len(),
            "states_covered": present.len(),
            "states_expected": US_JURISDICTIONS.len(),
            "missing_states": cov.missing(),
            "coverage": cov.to_json(),
            "warnings": warnings,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            // Honest removal reporting: this is the one app in the family whose
            // write path can tombstone, so the count is always present — 0 when
            // nothing was removed, and accompanied by a `warnings[]` entry when
            // the completeness floor suppressed removal detection entirely.
            "removed": summary.removed.len(),
            "removals_suppressed": write.removals_suppressed,
            "rejected": rejected.iter().map(Rejection::to_json).collect::<Vec<_>>(),
            "rejected_count": rejected.len(),
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
        });
        // One shared shape for the join's report (`unified`, incl. the dataset it
        // actually landed in and any truncated input), and the `index_datasets`
        // declaration without which no watch, trigger, contract or search doc
        // ever sees a `trades/*` revision.
        join.merge_into(&mut out);
        Ok(unified::with_product_index(out))
    }
}

/// Structured-output contract for `claude --json-schema`. Lenient (extra fields
/// tolerated) so a valid answer is never rejected, but pins the tax shape.
fn tax_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "year": { "type": "string" },
            "federal": {
                "type": "object",
                "properties": {
                    "self_employment_tax_rate": { "type": "number" },
                    "qbi_deduction_pct": { "type": "number" },
                    "standard_deduction_single": { "type": "number" },
                    "section_179_limit": { "type": "number" },
                    "top_marginal_rate": { "type": "number" }
                }
            },
            "states": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "state": { "type": "string" },
                        "state_name": { "type": "string" },
                        "income_tax_type": { "type": "string" },
                        "top_marginal_rate": { "type": "number" },
                        "top_bracket_threshold": { "type": "number" },
                        "notes": { "type": "string" }
                    },
                    "required": ["state", "income_tax_type", "top_marginal_rate"]
                }
            }
        },
        "required": ["year", "federal", "states"]
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
        let app = StateTax;
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

    #[test]
    fn roster_is_51_unique_uppercase_jurisdictions_including_dc() {
        // Completeness is checked against this fixed roster, not a run count —
        // a duplicate or lowercase entry would silently mask a missing state
        // (the agent's codes are uppercased before the contains() check).
        let set: std::collections::HashSet<&str> = US_JURISDICTIONS.iter().copied().collect();
        assert_eq!(US_JURISDICTIONS.len(), 51);
        assert_eq!(set.len(), 51);
        assert!(set.contains("DC"));
        assert!(US_JURISDICTIONS
            .iter()
            .all(|j| j.len() == 2 && j.chars().all(|c| c.is_ascii_uppercase())));
    }

    #[test]
    fn tax_schema_requires_the_fields_the_validators_key_on() {
        // The downstream loop rejects states on top_marginal_rate and keys on
        // `state`; the schema must force both to exist so `claude --json-schema`
        // catches shape drift before the salvage path does.
        let s = tax_schema();
        assert_eq!(s["required"], json!(["year", "federal", "states"]));
        assert_eq!(
            s["properties"]["states"]["items"]["required"],
            json!(["state", "income_tax_type", "top_marginal_rate"])
        );
        assert_eq!(
            s["properties"]["states"]["items"]["properties"]["top_marginal_rate"]["type"],
            "number"
        );
    }

    #[test]
    fn tax_schema_declares_every_federal_constant_as_a_number() {
        // The federal record feeds require_rate() on three of these; a string
        // type here would pass the schema but fail validation downstream.
        let s = tax_schema();
        let fed = &s["properties"]["federal"]["properties"];
        for f in [
            "self_employment_tax_rate",
            "qbi_deduction_pct",
            "standard_deduction_single",
            "section_179_limit",
            "top_marginal_rate",
        ] {
            assert_eq!(fed[f]["type"], "number", "federal field {f}");
        }
    }
}
