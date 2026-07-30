//! US contractor LICENSING / BONDING / INSURANCE compliance reference for
//! solo-trades operators, via the Claude research engine.
//!
//! For each US state (50 + DC) × canonical trade: the state-level contractor
//! license requirement (`none` / `registration` / `exam_license`), typical
//! initial license cost, required surety-bond amount, minimum general-liability
//! insurance for licensure, and whether workers'-comp coverage is required for
//! a sole proprietor with NO employees. This is the "what does it cost to
//! legally exist" half of operator economics — the tax apps already cover
//! "what will I earn and pay". Upserted into the cross-source
//! `trades/compliance` dataset (keys `<ST>:<trade>`), then joined onto the
//! matching `trades/operator_economics` per-state rows as a `compliance` block.
//!
//! Data type: LICENSING RULES (state-grain; county/city variation is flagged
//! via `local_variation` + notes, never invented as figures). Access: the local
//! Claude CLI (no API key; costs money per run). Licensing is trade-specific,
//! so the research is CHUNKED BY TRADE — one 51-jurisdiction structured call
//! per trade (5 calls for the full roster), mirroring state-tax's proven
//! one-call/51-jurisdiction pattern. Each trade is vintage-freshness-gated
//! BEFORE its metered call: rules change rarely, so a year already held is
//! skipped unless `force: true`. Params: {"year": "2026", "trades": [labels],
//! "role": "research|compose", "max_turns": 30}.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, ResearchRequest, Result, ScrapeApp,
};
use serde_json::{json, Value};
use trades_common::taxonomy;
use trades_common::unified::{self, COMPLIANCE, UNIFIED_APP};
use trades_common::validate::{self, Rejection};

pub struct StateLicensing;

const DEFAULT_YEAR: &str = "2026";

/// Sentinel jurisdiction for the per-trade vintage gate: California licenses
/// every covered trade, so a successful run for a trade always lands a
/// `CA:<trade>` record — its stored `year` tells us whether this vintage is
/// already held (a rejected CA record simply re-runs the trade next time,
/// which errs on the side of re-verifying).
const SENTINEL_STATE: &str = "CA";

/// Plausibility ceilings for agent-returned dollar magnitudes. Coarse on
/// purpose — they catch a hallucinated $80M "license fee", not a debatable
/// $450 vs $500.
const MAX_LICENSE_COST_USD: f64 = 50_000.0;
const MAX_BOND_USD: f64 = 5_000_000.0;
const MAX_INSURANCE_USD: f64 = 100_000_000.0;

/// The 50 states + DC, enumerated in code so completeness is checked against a
/// fixed roster rather than a run-count heuristic. Missing entries are reported.
/// (Deliberately a local copy of state-tax's roster — the two apps' completeness
/// checks must not be couplable to one accidental edit.)
const US_JURISDICTIONS: [&str; 51] = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
];

#[async_trait]
impl ScrapeApp for StateLicensing {
    fn name(&self) -> &'static str {
        "state-licensing"
    }

    fn description(&self) -> &'static str {
        "US contractor licensing / bonding / insurance compliance reference per state × \
         trade, via the Claude research engine — state-level requirement (none / \
         registration / exam_license), typical license cost, surety-bond amount, \
         liability-insurance minimum, and the solo-operator workers'-comp signal for \
         all 50 states + DC, chunked one metered call per trade. Upserted into \
         `trades/compliance` (keys `<ST>:<trade>`) and joined as a `compliance` block \
         onto the per-state trades/operator_economics rows. No API key (local Claude \
         CLI; costs money per run). Params: {\"year\": \"2026\", \"trades\": \
         [canonical labels], \"role\": \"research|compose\", \"max_turns\": 30}."
    }

    // Annual refresh: licensing statutes and fee schedules mostly change at
    // year boundaries; mid-January catches the new year's rules once they are
    // published. Each trade re-checks the vintage gate first, so the scheduled
    // run only pays for trades whose year isn't already held.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 8 15 1 *")
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
                        "description": "Vintage year the licensing rules are compiled for (freshness-gate key)."
                    },
                    "trades": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "Canonical trade labels to research (default: all five). One metered 51-jurisdiction call per trade."
                    },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": { "type": "integer", "minimum": 1 },
                    "force": {
                        "type": "boolean",
                        "description": "Bypass the per-trade vintage freshness gate and re-pay the research."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Annual full-roster refresh (all five trades; vintage-gated, so \
                                  already-held trades cost nothing)",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Force-re-research a single trade's 51 jurisdictions",
                    params: json!({ "year": DEFAULT_YEAR, "trades": ["Plumbing"], "force": true }),
                },
            ],
            output_shape: Some(
                "{source, year, trades_run: [..], trades_skipped: [..], records, \
                 coverage: {<trade>: {states_covered, missing_states}}, new, changed, \
                 unchanged, rejected: [..], rejected_count, unified: {new, changed}, \
                 cost_usd, duration_ms, num_turns} — per-trade vintage skips are free; \
                 cost fields are summed across the per-trade metered calls",
            ),
            cost_class: CostClass::Claude,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let year = trades_common::year_param(&ctx, DEFAULT_YEAR).to_string();
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

        // Trade universe = governed `trades/taxonomy` registry, compile-time
        // enum as fallback (identical five while the registry is absent).
        let taxonomy_entries = taxonomy::taxonomy(&ctx).await?;

        // Requested trades (default: the whole live taxonomy). Unknown labels
        // are an error up front — a typo must not silently research nothing.
        let trades: Vec<taxonomy::TradeEntry> =
            match ctx.params.get("trades").and_then(Value::as_array) {
                Some(arr) => {
                    let mut out: Vec<taxonomy::TradeEntry> = Vec::new();
                    for v in arr {
                        let raw = v.as_str().unwrap_or("");
                        let (canonical, known) = taxonomy::canonicalize_in(&taxonomy_entries, raw);
                        let entry = taxonomy_entries
                            .iter()
                            .find(|e| known && e.label == canonical)
                            .ok_or_else(|| {
                                Error::App(format!(
                                    "state-licensing: unknown trade label {raw:?} (canonical: {})",
                                    taxonomy::prompt_list_of(&taxonomy_entries)
                                ))
                            })?;
                        if !out.iter().any(|e| e.label == entry.label) {
                            out.push(entry.clone());
                        }
                    }
                    out
                }
                None => taxonomy_entries.clone(),
            };

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut rejected: Vec<Rejection> = Vec::new();
        let mut trades_run: Vec<String> = Vec::new();
        let mut trades_skipped: Vec<String> = Vec::new();
        let mut coverage = serde_json::Map::new();
        let (mut cost_usd, mut duration_ms, mut num_turns) = (0.0_f64, 0_u64, 0_u64);

        for trade in trades {
            let label = trade.label.as_str();

            // Vintage freshness gate BEFORE the metered call: licensing rules
            // change rarely, so a trade whose sentinel record already carries
            // this year would re-pay a ~30-turn agentic run to reproduce the
            // same facts. `force: true` bypasses (handled inside vintage_held).
            let sentinel = format!("{SENTINEL_STATE}:{label}");
            if trades_common::vintage_held(&ctx, UNIFIED_APP, COMPLIANCE, &sentinel, &year).await? {
                trades_skipped.push(label.to_string());
                continue;
            }

            let prompt = licensing_prompt(&year, label);
            let mut request = ResearchRequest::new(prompt).with_role(role.clone());
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
            // Constrain the final answer to the licensing schema
            // (`claude --json-schema`); salvage_json still backstops it.
            request.json_schema = Some(licensing_schema());

            // Per-trade artifact name so five calls don't overwrite one file.
            let artifact = format!("research-{}.json", artifact_slug(label));
            let (data, output) =
                trades_common::research_json_named(&ctx, "state-licensing", request, &artifact)
                    .await?;
            cost_usd += output.cost_usd.unwrap_or(0.0);
            duration_ms += output.duration_ms.unwrap_or(0);
            num_turns += output.num_turns.unwrap_or(0);
            trades_run.push(label.to_string());

            let (records, mut trade_rejected, present) =
                parse_trade_records(&data, label, &trade.soc_code, &year);
            let missing: Vec<&str> = US_JURISDICTIONS
                .iter()
                .copied()
                .filter(|j| !present.contains(*j))
                .collect();
            coverage.insert(
                label.to_string(),
                json!({
                    "states_covered": present.len(),
                    "states_expected": US_JURISDICTIONS.len(),
                    "missing_states": missing,
                }),
            );
            if present.is_empty() {
                return Err(Error::App(format!(
                    "state-licensing: agent JSON for trade {label} contained no plausible \
                     state records"
                )));
            }
            all_records.extend(records);
            rejected.append(&mut trade_rejected);
        }

        // Partial-scope runs are the norm (per-trade chunking + vintage skips),
        // so this is an idempotent JOIN-style upsert — never a full-snapshot
        // sync, which would mark every other trade's rows removed. Written
        // twice on purpose: the app's OWN namespace (`state-licensing/
        // compliance`) is what the catalog freshness monitor watches
        // (`/catalog/health` lists `source.app/source.dataset`), and the
        // cross-source copy (`trades/compliance`) sits beside
        // operator_economics for trades-namespace consumers and the join.
        ctx.datasets
            .upsert_many(self.name(), COMPLIANCE, &all_records)
            .await?;
        let summary = ctx
            .datasets
            .upsert_many(UNIFIED_APP, COMPLIANCE, &all_records)
            .await?;

        // Land the `compliance` block on the per-state operator_economics rows
        // (mirrors state-tax's end-of-run unified sync).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/licensing/{year}"),
            "year": year,
            "trades_run": trades_run,
            "trades_skipped": trades_skipped,
            "records": all_records.len(),
            "coverage": coverage,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "rejected": rejected.iter().map(Rejection::to_json).collect::<Vec<_>>(),
            "rejected_count": rejected.len(),
            "unified": { "new": unified.new.len(), "changed": unified.changed.len() },
            "cost_usd": cost_usd,
            "duration_ms": duration_ms,
            "num_turns": num_turns,
        }))
    }
}

/// The one-call/51-jurisdiction research prompt for a single trade.
fn licensing_prompt(year: &str, trade: &str) -> String {
    format!(
        "You are a US contractor-licensing compliance analyst. For year {year} and the \
         trade **{trade}**, compile what a SOLO owner-operator (sole proprietor, no \
         employees) must have to legally operate in EVERY US state (all 50 states + DC). \
         Use web search to verify current rules. STATE-LEVEL grain only: where licensing \
         is delegated to counties/cities (e.g. some trades in TX or CO), report the \
         state-level requirement, set local_variation to true, and explain in notes — \
         NEVER invent county figures.\n\n\
         Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
         {{\"year\": string, \"trade\": string, \
         \"states\": [{{\"state\": string (2-letter USPS code), \"state_name\": string, \
         \"requirement_level\": \"none\"|\"registration\"|\"exam_license\", \
         \"license_cost_usd\": number (typical initial state-level cost: application + \
         exam + first-period fee; 0 when requirement_level is none), \
         \"bond_amount_usd\": number (state-required surety bond; 0 if none), \
         \"insurance_min_liability_usd\": number (minimum general-liability coverage the \
         state requires for licensure; 0 if none required), \
         \"workers_comp_required\": boolean (is workers'-comp coverage required for a \
         sole proprietor with NO employees), \
         \"local_variation\": boolean, \"notes\": string}}]}}\n\
         Include ALL 50 states + DC (51 entries). requirement_level meanings: \"none\" = \
         no state credential needed, \"registration\" = register/pay a fee but no exam, \
         \"exam_license\" = pass an exam / hold a state license. Dollar amounts are USD \
         numbers without symbols."
    )
}

/// Parse + validate one trade's agent answer into `(<ST>:<trade>, record)`
/// upsert items. Returns (records, rejections, states-present set).
fn parse_trade_records(
    data: &Value,
    trade_label: &str,
    soc_code: &str,
    year: &str,
) -> (
    Vec<(String, Value)>,
    Vec<Rejection>,
    std::collections::HashSet<String>,
) {
    let mut records = Vec::new();
    let mut rejected = Vec::new();
    let mut present = std::collections::HashSet::new();

    let Some(states) = data.get("states").and_then(Value::as_array) else {
        return (records, rejected, present);
    };
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
        let key = format!("{st}:{trade_label}");
        let mut reasons = Vec::new();

        // Requirement level must normalize onto the closed taxonomy — an
        // unclassifiable level would poison every downstream consumer.
        let level = s
            .get("requirement_level")
            .and_then(Value::as_str)
            .and_then(normalize_requirement_level);
        if level.is_none() {
            reasons.push(format!(
                "requirement_level: {:?} not one of none|registration|exam_license",
                s.get("requirement_level")
            ));
        }

        // Dollar magnitudes: zero is legitimate (no-license states), negative
        // or absurd is not.
        let license_cost = validate::num(s, "license_cost_usd");
        let bond = validate::num(s, "bond_amount_usd");
        let insurance = validate::num(s, "insurance_min_liability_usd");
        validate::require_nonnegative(&mut reasons, "license_cost_usd", license_cost);
        validate::require_at_most(&mut reasons, "license_cost_usd", license_cost, MAX_LICENSE_COST_USD);
        validate::require_nonnegative(&mut reasons, "bond_amount_usd", bond);
        validate::require_at_most(&mut reasons, "bond_amount_usd", bond, MAX_BOND_USD);
        validate::require_nonnegative(&mut reasons, "insurance_min_liability_usd", insurance);
        validate::require_at_most(
            &mut reasons,
            "insurance_min_liability_usd",
            insurance,
            MAX_INSURANCE_USD,
        );

        if !reasons.is_empty() {
            rejected.push(Rejection { key, reasons });
            continue;
        }

        let mut rec = s.clone();
        rec["state"] = json!(st);
        rec["trade"] = json!(trade_label);
        rec["soc_code"] = json!(soc_code);
        rec["year"] = json!(year);
        rec["requirement_level"] = json!(level.unwrap());
        // Honest grain marker: every figure here is state-level; county/city
        // texture only ever appears as local_variation + notes.
        rec["grain"] = json!("state");
        if rec.get("local_variation").and_then(Value::as_bool).is_none() {
            rec["local_variation"] = Value::Null;
        }
        if rec.get("workers_comp_required").and_then(Value::as_bool).is_none() {
            rec["workers_comp_required"] = Value::Null;
        }
        present.insert(st);
        records.push((key, rec));
    }
    (records, rejected, present)
}

/// Normalize the agent's phrasing of a requirement level onto the closed
/// taxonomy. Returns None for genuinely unclassifiable strings (rejected, not
/// guessed).
fn normalize_requirement_level(raw: &str) -> Option<&'static str> {
    let l = raw.trim().to_lowercase();
    if l.is_empty() {
        return None;
    }
    if l == "none" || l.starts_with("no ") || l == "not required" || l == "n/a" {
        Some("none")
    } else if l.contains("regist") {
        Some("registration")
    } else if l.contains("exam") || l.contains("licen") || l.contains("certif") {
        Some("exam_license")
    } else {
        None
    }
}

/// Artifact-name-safe slug for a trade label ("Pool service" → "pool-service").
fn artifact_slug(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Structured-output contract for `claude --json-schema`. Lenient (extra
/// fields tolerated) so a valid answer is never rejected, but pins the
/// licensing shape the validators key on.
fn licensing_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "year": { "type": "string" },
            "trade": { "type": "string" },
            "states": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "state": { "type": "string" },
                        "state_name": { "type": "string" },
                        "requirement_level": { "type": "string" },
                        "license_cost_usd": { "type": "number" },
                        "bond_amount_usd": { "type": "number" },
                        "insurance_min_liability_usd": { "type": "number" },
                        "workers_comp_required": { "type": "boolean" },
                        "local_variation": { "type": "boolean" },
                        "notes": { "type": "string" }
                    },
                    "required": ["state", "requirement_level"]
                }
            }
        },
        "required": ["year", "trade", "states"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roster_is_51_unique_uppercase_jurisdictions_including_dc_and_sentinel() {
        let set: std::collections::HashSet<&str> = US_JURISDICTIONS.iter().copied().collect();
        assert_eq!(US_JURISDICTIONS.len(), 51);
        assert_eq!(set.len(), 51);
        assert!(set.contains("DC"));
        // The vintage gate keys on the sentinel state — it must be on the roster.
        assert!(set.contains(SENTINEL_STATE));
        assert!(US_JURISDICTIONS
            .iter()
            .all(|j| j.len() == 2 && j.chars().all(|c| c.is_ascii_uppercase())));
    }

    #[test]
    fn licensing_schema_requires_the_fields_the_validators_key_on() {
        // parse_trade_records keys on `state` and rejects on
        // `requirement_level`; the schema must force both to exist so
        // `claude --json-schema` catches shape drift before the salvage path.
        let s = licensing_schema();
        assert_eq!(s["required"], json!(["year", "trade", "states"]));
        assert_eq!(
            s["properties"]["states"]["items"]["required"],
            json!(["state", "requirement_level"])
        );
        for f in [
            "license_cost_usd",
            "bond_amount_usd",
            "insurance_min_liability_usd",
        ] {
            assert_eq!(
                s["properties"]["states"]["items"]["properties"][f]["type"],
                "number",
                "field {f}"
            );
        }
    }

    #[test]
    fn requirement_level_normalizes_common_variants_and_rejects_junk() {
        assert_eq!(normalize_requirement_level("none"), Some("none"));
        assert_eq!(normalize_requirement_level("No state license"), Some("none"));
        assert_eq!(
            normalize_requirement_level("registration"),
            Some("registration")
        );
        assert_eq!(
            normalize_requirement_level("Registration only"),
            Some("registration")
        );
        assert_eq!(
            normalize_requirement_level("exam_license"),
            Some("exam_license")
        );
        assert_eq!(
            normalize_requirement_level("Exam + license"),
            Some("exam_license")
        );
        assert_eq!(
            normalize_requirement_level("State license required"),
            Some("exam_license")
        );
        assert_eq!(normalize_requirement_level("varies wildly"), None);
        assert_eq!(normalize_requirement_level(""), None);
    }

    #[test]
    fn parse_keys_records_st_trade_and_stamps_grain_and_year() {
        let data = json!({
            "year": "2026", "trade": "Plumbing",
            "states": [
                { "state": "ca", "state_name": "California",
                  "requirement_level": "Exam + license",
                  "license_cost_usd": 600, "bond_amount_usd": 25000,
                  "insurance_min_liability_usd": 0,
                  "workers_comp_required": false, "local_variation": false,
                  "notes": "CSLB C-36" },
            ],
        });
        let (records, rejected, present) =
            parse_trade_records(&data, "Plumbing", "47-2152", "2026");
        assert!(rejected.is_empty());
        assert!(present.contains("CA"));
        assert_eq!(records.len(), 1);
        let (key, rec) = &records[0];
        assert_eq!(key, "CA:Plumbing"); // uppercased state, canonical label
        assert_eq!(rec["requirement_level"], "exam_license"); // normalized
        assert_eq!(rec["grain"], "state");
        assert_eq!(rec["year"], "2026");
        assert_eq!(rec["soc_code"], "47-2152");
    }

    #[test]
    fn parse_rejects_negative_costs_and_unclassifiable_levels() {
        let data = json!({
            "year": "2026", "trade": "Plumbing",
            "states": [
                { "state": "TX", "requirement_level": "it depends",
                  "license_cost_usd": 100 },
                { "state": "FL", "requirement_level": "exam_license",
                  "license_cost_usd": -50 },
                { "state": "NV", "requirement_level": "exam_license",
                  "bond_amount_usd": 80000000.0 },
                { "state": "OH", "requirement_level": "none",
                  "license_cost_usd": 0 },
            ],
        });
        let (records, rejected, present) =
            parse_trade_records(&data, "Plumbing", "47-2152", "2026");
        assert_eq!(rejected.len(), 3, "TX level, FL negative, NV over-cap");
        assert_eq!(records.len(), 1, "OH's honest $0 for a no-license state passes");
        assert!(present.contains("OH") && !present.contains("TX"));
    }

    #[test]
    fn parse_keeps_absent_booleans_null_never_fabricated() {
        let data = json!({
            "states": [ { "state": "WY", "requirement_level": "none" } ],
        });
        let (records, rejected, _) = parse_trade_records(&data, "HVAC", "49-9021", "2026");
        assert!(rejected.is_empty());
        let (_, rec) = &records[0];
        assert!(rec["workers_comp_required"].is_null());
        assert!(rec["local_variation"].is_null());
    }

    #[test]
    fn artifact_slug_is_filename_safe() {
        assert_eq!(artifact_slug("Pool service"), "pool-service");
        assert_eq!(artifact_slug("HVAC"), "hvac");
    }
}
