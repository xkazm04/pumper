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
//!
//! The trade universe comes from the governed `trades/taxonomy` registry
//! (compile-time enum as fallback — identical behavior while the registry is
//! absent). This app also hosts the TAXONOMY PROPOSER mode: `{"propose_trade":
//! "roofing"}` runs ONE metered research call that maps the candidate trade to
//! its SOC code / NAICS codes / aliases and upserts a `trades/taxonomy` record
//! with `source: "proposed", enabled: false`. Nothing consumes it until a
//! human flips `enabled` — there is NO auto-enable.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, ManifestExample, ResearchRequest, Result, ScrapeApp,
};
use serde_json::{json, Value};
use trades_common::coverage;
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
         CLI; costs money per run). Trade universe comes from the governed \
         `trades/taxonomy` registry (compile-time enum as fallback). Params: \
         {\"year\": \"2024\", \"role\": \"research|compose\", \"max_turns\": 25, \
         \"propose_trade\": \"roofing\" (taxonomy-proposer mode: one metered call \
         drafts a trades/taxonomy record with enabled:false — a human flips it)}."
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
                        "description": "BLS OEWS vintage. Doubles as the freshness key — a vintage already held costs nothing."
                    },
                    "role": { "type": "string", "enum": ["research", "compose"] },
                    "model": { "type": "string" },
                    "effort": { "type": "string", "enum": ["low", "medium", "high", "xhigh", "max"] },
                    "max_turns": { "type": "integer", "minimum": 1 },
                    "force": {
                        "type": "boolean",
                        "description": "Bypass the vintage freshness gate and re-pay the ~25-turn research run."
                    },
                    "propose_trade": {
                        "type": "string",
                        "minLength": 1,
                        "description": "TAXONOMY-PROPOSER mode: one metered call drafts a trades/taxonomy record for this candidate trade with enabled=false. No wages are researched, and nothing consumes the draft until a human flips `enabled`."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Refresh the wage bands for an OEWS vintage (free no-op when already held)",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Taxonomy-proposer mode: draft a registry entry for a new trade (enabled=false)",
                    params: json!({ "propose_trade": "roofing" }),
                },
            ],
            output_shape: Some(
                "{source, year, records, coverage: {unit, covered, expected, ratio, \
                 floor, short, missing}, warnings: [string], new, changed, unchanged, \
                 rejected: [{key, \
                 reasons}], rejected_count, unknown_trades, unified: {new, changed}, \
                 cost_usd, duration_ms, num_turns}; the vintage gate returns {source, \
                 year, skipped, records, cost_usd: 0.0}; propose_trade mode returns the \
                 drafted taxonomy record instead of wages",
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
            .or(Some(25));

        // Taxonomy-proposer mode: one metered call that DRAFTS a registry
        // record for a candidate trade (enabled: false — a human flips it).
        if let Some(candidate) = ctx.params.get("propose_trade").and_then(Value::as_str) {
            return propose_trade(&ctx, candidate, role, max_turns).await;
        }

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

        // Trade universe = governed registry, enum fallback (identical list —
        // and prompt — while the registry dataset is absent).
        let entries = taxonomy::taxonomy(&ctx).await?;
        let trades = taxonomy::prompt_list_of(&entries);
        let n_trades = entries.len();
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
             (e.g. 30.10), annual in whole dollars. Include all {n_trades} trades."
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
        // Provenance (M12): pin the derivation spec (prompt + structured-output
        // schema + model/effort) that produced this answer, so a stored wage band
        // stays explainable after the live prompt moves on. Registered BEFORE the
        // metered call so the pinned spec is the one actually sent.
        let prov = trades_common::research_provenance(&ctx, "trade-wages", &request).await;
        let (data, output) = trades_common::research_json(&ctx, "trade-wages", request).await?;

        let (all_records, rejected, unknown_trades) = collect_wage_records(&entries, &data, &year);

        // Coverage of the trade roster this run was asked for. Before this, one
        // surviving trade out of five was a green job with `rejected_count: 4`
        // and nothing else to read; the shared `coverage` block + `warnings[]`
        // is the family's answer to that (see `trades_common::coverage`).
        // Keys are `US:{label}`, so the label is the roster member.
        let present: std::collections::HashSet<String> = all_records
            .iter()
            .map(|(k, _)| k.trim_start_matches("US:").to_string())
            .collect();
        let roster: Vec<&str> = entries.iter().map(|e| e.label.as_str()).collect();
        let cov = coverage::Coverage::of_roster("trades", &roster, &present);
        let warnings: Vec<String> = cov.warning().into_iter().collect();

        if all_records.is_empty() {
            return Err(Error::App(
                "trade-wages: agent JSON contained no plausible trades".into(),
            ));
        }
        // One batch, one derivation — a batch-level stamp is the honest grain
        // here (every row came out of the same single research call).
        let summary = ctx
            .upsert_many_with_provenance("wages", &all_records, prov)
            .await?;

        // Cross-source layer: rebuild trades/operator_economics from the current
        // state of all four source datasets (mirrors grants-common's sync_unified).
        let unified = unified::sync_operator_economics(&ctx).await?;

        Ok(json!({
            "source": format!("agentic/wages/{year}"),
            "year": year,
            "records": all_records.len(),
            "coverage": cov.to_json(),
            "warnings": warnings,
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
    entries: &[taxonomy::TradeEntry],
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
            // Registry-aware: enum matcher first (legacy semantics), then
            // enabled registry trades' aliases.
            let (trade, known) = taxonomy::canonicalize_in(entries, &raw);
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

// ---------------------------------------------------------------------------
// Taxonomy proposer mode
// ---------------------------------------------------------------------------

/// One metered research call that maps a candidate trade (e.g. "roofing") to a
/// DRAFT `trades/taxonomy` registry record: canonical label, SOC code, 6-digit
/// NAICS codes, keyword aliases. Upserted with `source: "proposed",
/// enabled: false` — a human flips `enabled` to make the trade live across the
/// research + census apps. NEVER auto-enabled, never expands scheduled runs.
async fn propose_trade(
    ctx: &AppContext,
    candidate: &str,
    role: String,
    max_turns: Option<u32>,
) -> Result<Value> {
    let candidate = candidate.trim();
    if candidate.is_empty() {
        return Err(Error::App("trade-wages: propose_trade is empty".into()));
    }

    // Materialize the seed rows the first time the registry is touched, WITHOUT
    // overwriting rows a human may already have edited (get-before-upsert).
    let mut seeded = 0usize;
    for t in taxonomy::Trade::ALL {
        let (key, rec) = taxonomy::seed_record(t);
        if ctx
            .datasets
            .get(taxonomy::TAXONOMY_APP, taxonomy::TAXONOMY_DATASET, &key)
            .await?
            .is_none()
        {
            ctx.datasets
                .upsert_many(
                    taxonomy::TAXONOMY_APP,
                    taxonomy::TAXONOMY_DATASET,
                    &[(key, rec)],
                )
                .await?;
            seeded += 1;
        }
    }

    // Already covered? Then the metered call would buy nothing.
    let entries = taxonomy::taxonomy(ctx).await?;
    let (canonical, known) = taxonomy::canonicalize_in(&entries, candidate);
    if known {
        return Ok(json!({
            "source": "agentic/taxonomy-propose",
            "candidate": candidate,
            "skipped": format!("already covered by canonical trade {canonical:?}"),
            "seeded": seeded,
            "cost_usd": 0.0,
        }));
    }

    let existing = taxonomy::prompt_list_of(&entries);
    let prompt = format!(
        "You are a US labor-market taxonomist for home-services trades. Map the \
         candidate trade **{candidate}** onto the reference classifications, using web \
         search to verify (bls.gov SOC/OEWS for the occupation, census.gov NAICS 2017 \
         for the industry codes).\n\n\
         Respond with ONLY a JSON object (no markdown fences, no prose) of this shape:\n\
         {{\"trade\": string (canonical short display label, e.g. \"Roofing\"), \
         \"soc_code\": string (best-fit BLS SOC, format NN-NNNN), \
         \"occupation\": string (the SOC occupation title), \
         \"naics\": [string] (the 6-digit NAICS 2017 code(s) these businesses file \
         under), \
         \"aliases\": [string] (3-8 lowercase keyword stems that identify this trade in \
         free text, e.g. \"roof\", \"shingle\" — stems, so \"roof\" also matches \
         \"roofer\"/\"roofing\"), \
         \"notes\": string (one sentence on the mapping and any ambiguity)}}\n\
         The label must NOT duplicate an existing trade ({existing})."
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
    request.json_schema = Some(propose_schema());
    let artifact = format!("propose-{}.json", propose_slug(candidate));
    let (data, output) =
        trades_common::research_json_named(ctx, "trade-wages", request, &artifact).await?;

    // Validate. A failed proposal reports its reasons (the answer is already
    // paid for) and upserts NOTHING — bad mappings must not enter the registry
    // even in a disabled state a human might flip half-read.
    match parse_proposed_entry(&data, candidate) {
        Err(reasons) => Ok(json!({
            "source": "agentic/taxonomy-propose",
            "candidate": candidate,
            "proposed": Value::Null,
            "rejected": reasons,
            "seeded": seeded,
            "cost_usd": output.cost_usd,
            "duration_ms": output.duration_ms,
            "num_turns": output.num_turns,
        })),
        Ok((key, record)) => {
            // The agent's canonical label must not collide with a live trade,
            // and a seed/approved row is never overwritten by a proposal.
            let (resolved, dup) = taxonomy::canonicalize_in(&entries, &key);
            if dup {
                return Ok(json!({
                    "source": "agentic/taxonomy-propose",
                    "candidate": candidate,
                    "proposed": Value::Null,
                    "rejected": [format!(
                        "proposed label {key:?} resolves to existing trade {resolved:?}"
                    )],
                    "seeded": seeded,
                    "cost_usd": output.cost_usd,
                }));
            }
            if let Some(prior) = ctx
                .datasets
                .get(taxonomy::TAXONOMY_APP, taxonomy::TAXONOMY_DATASET, &key)
                .await?
            {
                let src = prior
                    .data
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if src != "proposed" {
                    return Ok(json!({
                        "source": "agentic/taxonomy-propose",
                        "candidate": candidate,
                        "proposed": Value::Null,
                        "rejected": [format!(
                            "registry already holds a {src:?} record for {key:?} — not overwritten"
                        )],
                        "seeded": seeded,
                        "cost_usd": output.cost_usd,
                    }));
                }
            }
            let summary = ctx
                .datasets
                .upsert_many(
                    taxonomy::TAXONOMY_APP,
                    taxonomy::TAXONOMY_DATASET,
                    &[(key.clone(), record.clone())],
                )
                .await?;
            Ok(json!({
                "source": "agentic/taxonomy-propose",
                "candidate": candidate,
                "proposed": record,
                "key": key,
                "enabled": false,
                "next_step": "review the record, then set enabled:true on trades/taxonomy \
                              to make every trades + census app cover it on its next run",
                "seeded": seeded,
                "new": summary.new.len(),
                "changed": summary.changed.len(),
                "cost_usd": output.cost_usd,
                "duration_ms": output.duration_ms,
                "num_turns": output.num_turns,
            }))
        }
    }
}

/// Validate the proposer's answer into a `(key, record)` registry upsert.
/// Pure so the guards are testable. Errors carry per-field reasons.
fn parse_proposed_entry(
    data: &Value,
    candidate: &str,
) -> std::result::Result<(String, Value), Vec<String>> {
    let mut reasons = Vec::new();

    let label = data
        .get("trade")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if label.is_empty() {
        reasons.push("trade: missing canonical label".into());
    }

    let soc = data
        .get("soc_code")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if !is_soc_code(soc) {
        reasons.push(format!("soc_code: {soc:?} not NN-NNNN"));
    }

    let naics: Vec<String> = data
        .get("naics")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .collect()
        })
        .unwrap_or_default();
    if naics.is_empty() {
        reasons.push("naics: no codes".into());
    }
    for c in &naics {
        if c.len() != 6 || !c.chars().all(|ch| ch.is_ascii_digit()) {
            reasons.push(format!("naics: {c:?} not a 6-digit code"));
        }
    }

    let aliases: Vec<String> = data
        .get("aliases")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    if aliases.is_empty() {
        reasons.push("aliases: none".into());
    }

    if !reasons.is_empty() {
        return Err(reasons);
    }
    Ok((
        label.to_string(),
        serde_json::json!({
            "trade": label,
            "soc_code": soc,
            "occupation": data.get("occupation").cloned().unwrap_or(Value::Null),
            "naics": naics,
            "aliases": aliases,
            "notes": data.get("notes").cloned().unwrap_or(Value::Null),
            "proposed_from": candidate,
            // Governance: proposals are born DISABLED. A human flips this.
            "enabled": false,
            "source": "proposed",
        }),
    ))
}

/// `NN-NNNN` SOC-code shape check (e.g. `47-2181`).
fn is_soc_code(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 7
        && b[2] == b'-'
        && b[..2].iter().all(u8::is_ascii_digit)
        && b[3..].iter().all(u8::is_ascii_digit)
}

/// Artifact-name-safe slug for a candidate trade ("Pest control" → "pest-control").
fn propose_slug(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Structured-output contract for the proposer (`claude --json-schema`).
fn propose_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "trade": { "type": "string" },
            "soc_code": { "type": "string" },
            "occupation": { "type": "string" },
            "naics": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
            "aliases": { "type": "array", "items": { "type": "string" }, "minItems": 1 },
            "notes": { "type": "string" }
        },
        "required": ["trade", "soc_code", "naics", "aliases"]
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
        let app = TradeWages;
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
        let (records, rejected, _) = collect_wage_records(&taxonomy::seed_entries(), &data, "2024");
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
        let (records, rejected, unknown) =
            collect_wage_records(&taxonomy::seed_entries(), &data, "2024");
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
        let (records, _, unknown) = collect_wage_records(&taxonomy::seed_entries(), &data, "2024");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, "US:Roofing");
        assert_eq!(unknown, vec!["Roofing".to_string()]);
    }

    #[test]
    fn registry_trade_resolves_in_wage_records_when_enabled() {
        // A registry-enabled trade must key + canonicalize like a seed trade.
        let mut entries = taxonomy::seed_entries();
        entries.push(taxonomy::TradeEntry {
            label: "Roofing".into(),
            soc_code: "47-2181".into(),
            naics: vec!["238160".into()],
            aliases: vec!["roof".into()],
            source: "approved".into(),
        });
        let data = json!({ "trades": [wage_entry("Roofing contractors")] });
        let (records, _, unknown) = collect_wage_records(&entries, &data, "2024");
        assert_eq!(records[0].0, "US:Roofing");
        assert!(unknown.is_empty());
    }

    #[test]
    fn proposed_entry_is_born_disabled_with_source_proposed() {
        let data = json!({
            "trade": "Roofing", "soc_code": "47-2181",
            "occupation": "Roofers",
            "naics": ["238160"], "aliases": ["Roof", "shingle"],
            "notes": "clean mapping",
        });
        let (key, rec) = parse_proposed_entry(&data, "roofing").unwrap();
        assert_eq!(key, "Roofing");
        assert_eq!(rec["enabled"], false, "NEVER auto-enabled");
        assert_eq!(rec["source"], "proposed");
        assert_eq!(rec["proposed_from"], "roofing");
        assert_eq!(rec["aliases"][0], "roof", "aliases lowercased");
        // The record must merge into the taxonomy ONLY once a human flips enabled.
        assert_eq!(taxonomy::merge_taxonomy(&[rec.clone()]).len(), 5);
        let mut flipped = rec.clone();
        flipped["enabled"] = json!(true);
        assert_eq!(taxonomy::merge_taxonomy(&[flipped]).len(), 6);
    }

    #[test]
    fn proposed_entry_rejects_bad_soc_naics_and_missing_aliases() {
        let bad = json!({
            "trade": "Roofing", "soc_code": "472181",
            "naics": ["23816"], "aliases": [],
        });
        let reasons = parse_proposed_entry(&bad, "roofing").unwrap_err();
        assert!(reasons.iter().any(|r| r.contains("soc_code")));
        assert!(reasons.iter().any(|r| r.contains("6-digit")));
        assert!(reasons.iter().any(|r| r.contains("aliases")));
        assert!(parse_proposed_entry(&json!({}), "roofing").is_err());
    }

    #[test]
    fn soc_code_shape_check() {
        assert!(is_soc_code("47-2181"));
        assert!(!is_soc_code("47-218"));
        assert!(!is_soc_code("4a-2181"));
        assert!(!is_soc_code(""));
    }

    #[test]
    fn non_positive_magnitudes_are_rejected_even_when_ordered() {
        let mut bad = wage_entry("HVAC");
        bad["employment"] = json!(0);
        let data = json!({ "trades": [bad] });
        let (records, rejected, _) = collect_wage_records(&taxonomy::seed_entries(), &data, "2024");
        assert!(records.is_empty());
        assert!(rejected[0].reasons.iter().any(|r| r.contains("employment")));
    }
}
