//! US SOLO / self-employed trades density + receipts via Census **Nonemployer
//! Statistics (NES)**.
//!
//! Nonemployer establishments are businesses with NO paid employees — sole
//! proprietors and the self-employed — i.e. exactly Ledgerline's solo-trades target
//! market (the pool/plumbing/electrical/HVAC/landscaping one-person shop). We pull
//! the count of nonemployer establishments (`NESTAB`) and their total receipts
//! (`NRCPTOT`, $1,000s) per trade NAICS by state, and derive the **average receipts
//! per solo operator** — a revenue benchmark for a one-person business. Upserted into
//! the `nonemployers` dataset so a scheduled annual run only surfaces what changed.
//! Fast path — GET JSON API, no HTML, no browser.
//!
//! Data type: SOLO-OPERATOR DENSITY + REVENUE. Access: FREE Census key (shared with
//! census-density; `params.api_key` or env `CENSUS_API_KEY`). A separate Ledgerline
//! consumer from the grant pipeline, so deliberately NOT in catalog/data-sources.toml.
//!
//! Contract notes (verified 2026-07-03): `https://api.census.gov/data/{year}/nonemp`
//! `?get=NESTAB,NRCPTOT&for=state:*&NAICS2017={code}` (requires the free key; a keyless
//! request 302s to a 200 HTML page, not JSON). Success is a JSON array-of-arrays: row
//! 0 is the header, matched by NAME. Nonemployer data is DISCLOSURE-SUPPRESSED at the
//! 6-digit NAICS × state level (HTTP 204), so we pull **4-digit** trade codes: 2382
//! (building equipment: plumbing/HVAC/electrical) and 5617 (services to buildings &
//! dwellings: landscaping/pool). A NAICS whose data is fully suppressed is recorded
//! with a note rather than failing the whole run. NES lags ~2 years (override
//! `params.year`; default 2021).

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
    UpsertSummary,
};
use serde_json::{json, Value};

pub struct CensusNonemp;

const DEFAULT_YEAR: &str = "2021";

/// (4-digit NAICS 2017 code, friendly label) for the solo trades Ledgerline serves.
/// 4-digit because nonemployer data at 6-digit × state is disclosure-suppressed.
const DEFAULT_TRADES: &[(&str, &str)] = &[
    (
        "2382",
        "Building equipment contractors (plumbing, HVAC, electrical)",
    ),
    (
        "5617",
        "Services to buildings & dwellings (landscaping, pool)",
    ),
];

#[async_trait]
impl ScrapeApp for CensusNonemp {
    fn name(&self) -> &'static str {
        "census-nonemp"
    }

    fn description(&self) -> &'static str {
        "US SOLO / self-employed trades density + receipts from Census Nonemployer \
         Statistics (NES JSON API). Nonemployer establishment counts + total receipts \
         per trade NAICS by state, plus the derived average receipts per solo operator, \
         upserted into the `nonemployers` dataset. Requires a FREE Census API key \
         (params.api_key or env CENSUS_API_KEY; shared with census-density). Params: \
         {\"year\": \"2021\", \"states\": \"06,12,48\" (FIPS list; default all), \
         \"naics\": [\"2382\",\"5617\"] (4-digit; 6-digit is suppressed for \
         nonemployers), \"api_key\": \"...\"}"
    }

    // Needs a Census API key (shared with census-density); the env var is the
    // readiness signal for scheduled runs, which can't carry an inline key.
    fn requires(&self) -> &'static [pumper_core::Requirement] {
        &[pumper_core::Requirement::Env("CENSUS_API_KEY")]
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
                        "description": "NES vintage (NES lags ~2 years). Also selects the NAICS classification predicate: >= 2022 → NAICS2022, else NAICS2017."
                    },
                    "states": {
                        "type": "string",
                        "description": "Comma-separated state FIPS list (e.g. \"06,12,48\"). Empty or \"*\" = all states."
                    },
                    "naics": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "4-digit NAICS trade codes. 6-digit × state is disclosure-suppressed for nonemployers (HTTP 204). Default: the enabled trades/taxonomy registry codes, else 2382 + 5617."
                    },
                    "api_key": {
                        "type": "string",
                        "description": "Free Census API key; falls back to env CENSUS_API_KEY."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Annual refresh of the default trade codes across all states",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Building-equipment contractors in CA/TX/FL only",
                    params: json!({
                        "year": DEFAULT_YEAR,
                        "states": "06,48,12",
                        "naics": ["2382"]
                    }),
                },
            ],
            output_shape: Some(
                "{source, year, trades: [{naics, label, states_reported, total_nonemployers, \
                 total_receipts_thousands, national_avg_receipts_per_operator, \
                 top_states_by_density, top_states_by_avg_receipts} | {naics, label, note}], \
                 market_blend, records, new, changed, unchanged} — a fully suppressed NAICS \
                 yields a `note` entry, not a failure",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let year = ctx
            .params
            .get("year")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_YEAR)
            .to_string();
        let states = ctx
            .params
            .get("states")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();

        // params.naics overrides everything; otherwise the governed
        // `trades/taxonomy` registry drives the code list at this app's
        // 4-digit grain (a human-enabled trade is covered on the next run).
        // Registry absent/empty ⇒ compile-time DEFAULT_TRADES, exactly as before.
        let label_for = |c: &str| -> String {
            DEFAULT_TRADES
                .iter()
                .find(|(k, _)| *k == c)
                .map(|(_, l)| l.to_string())
                .unwrap_or_else(|| c.to_string())
        };
        let trades: Vec<(String, String)> = match ctx.params.get("naics").and_then(Value::as_array)
        {
            Some(arr) => arr
                .iter()
                .filter_map(Value::as_str)
                .map(|c| (c.to_string(), label_for(c)))
                .collect(),
            None => match trades_common::taxonomy::registry_naics(&ctx, 4).await? {
                Some(codes) => codes
                    .into_iter()
                    .map(|c| {
                        let l = label_for(&c);
                        (c, l)
                    })
                    .collect(),
                None => DEFAULT_TRADES
                    .iter()
                    .map(|(c, l)| (c.to_string(), l.to_string()))
                    .collect(),
            },
        };

        let api_key = census_common::api_key(&ctx, "census-nonemp")?;

        let for_clause = if states.is_empty() || states == "*" {
            "for=state:*".to_string()
        } else {
            format!("for=state:{states}")
        };

        // The NAICS classification vintage is year-dependent: NES 2017–2021 expose the
        // trade codes under the NAICS2017 predicate, but the 2022 vintage switched to
        // the 2022 classification, so the 2022 endpoint rejects NAICS2017 with HTTP 400
        // "unknown predicate variable". Pick the variable from the requested year.
        let naics_var = match year.parse::<u32>() {
            Ok(y) if y >= 2022 => "NAICS2022",
            _ => "NAICS2017",
        };

        // Provenance (M12) is per-request — one URL, one archived artifact per
        // NAICS — so each trade's rows are upserted with their own stamp and the
        // run reports one merged rollup, rather than one anonymous batch.
        let mut summary = UpsertSummary::default();
        let mut record_count = 0usize;
        let mut trade_summaries: Vec<Value> = Vec::new();

        for (naics, label) in &trades {
            let url = format!(
                "https://api.census.gov/data/{year}/nonemp?get=NAME,NESTAB,NRCPTOT&{for_clause}&{naics_var}={naics}&key={api_key}"
            );
            let resp = ctx
                .engines
                .http
                .fetch(HttpRequest::get(url.clone()))
                .await?;
            // 204 No Content (fully suppressed) or a non-JSON body → record a note,
            // don't fail the whole run.
            if resp.status == 204 || resp.body.trim().is_empty() {
                trade_summaries.push(json!({
                    "naics": naics, "label": label,
                    "note": "no data — nonemployer figures suppressed at this level",
                }));
                continue;
            }
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "Census NES {year} NAICS {naics}: HTTP {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(160).collect::<String>()
                )));
            }
            if !resp.body.trim_start().starts_with('[') {
                let hint = if resp.body.contains("key") {
                    " — looks like an invalid/missing API key"
                } else {
                    ""
                };
                return Err(Error::App(format!(
                    "Census NES {year} NAICS {naics}: response was not JSON{hint} \
                     (starts: {})",
                    resp.body.chars().take(160).collect::<String>()
                )));
            }
            let rows: Vec<Vec<String>> = serde_json::from_str(&resp.body).map_err(|e| {
                Error::App(format!(
                    "Census NES {year} NAICS {naics}: bad JSON rows: {e}"
                ))
            })?;
            // The archived bytes ARE what `artifact_sha` hashes — bind them once
            // so the stamp can never describe a body other than the stored one.
            let artifact = serde_json::to_vec_pretty(&rows)?;
            ctx.save_artifact(&format!("nonemp-{naics}.json"), &artifact)
                .await?;

            let header = rows.first().cloned().unwrap_or_default();
            let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
            let i_estab = idx("NESTAB").ok_or_else(|| {
                Error::App(format!(
                    "Census NES NAICS {naics}: no NESTAB column in {header:?}"
                ))
            })?;
            let i_rcpt = idx("NRCPTOT").ok_or_else(|| {
                Error::App(format!(
                    "Census NES NAICS {naics}: no NRCPTOT column in {header:?}"
                ))
            })?;
            let i_state = idx("state").ok_or_else(|| {
                Error::App(format!(
                    "Census NES NAICS {naics}: no state column in {header:?}"
                ))
            })?;

            let TradeRollup {
                records,
                ranked,
                total_estab,
                total_rcpt,
            } = map_trade_rows(&rows, i_estab, i_rcpt, i_state, naics, label, &year);
            record_count += records.len();
            // Stamp THIS request's key-redacted URL + the sha of the artifact it
            // was archived as onto every row it produced.
            census_common::merge_summary(
                &mut summary,
                ctx.upsert_many_with_provenance(
                    "nonemployers",
                    &records,
                    census_common::http_provenance(&url, &artifact),
                )
                .await?,
            );

            let mut by_density = ranked.clone();
            by_density.sort_by_key(|(_, estab, _)| std::cmp::Reverse(*estab));
            let mut by_avg = ranked.clone();
            by_avg.sort_by_key(|(_, _, avg)| std::cmp::Reverse(*avg));
            let national_avg = if total_estab > 0 {
                (total_rcpt * 1000) / total_estab
            } else {
                0
            };

            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "states_reported": ranked.len(),
                "total_nonemployers": total_estab,
                "total_receipts_thousands": total_rcpt,
                "national_avg_receipts_per_operator": national_avg,
                "top_states_by_density": by_density.iter().take(5)
                    .map(|(s, e, _)| json!({ "state": s, "nonemployers": e })).collect::<Vec<_>>(),
                "top_states_by_avg_receipts": by_avg.iter().take(5)
                    .map(|(s, _, a)| json!({ "state": s, "avg_receipts_per_operator": a })).collect::<Vec<_>>(),
            }));
        }

        // Re-derive the blended employer+solo `census/market_blend` dataset
        // (shared logic lives in app-census-density). BOTH Census apps trigger
        // the blend after their own upserts because they run annually and
        // independently — whichever refreshes last would otherwise leave the
        // blend stale until the other's next run. Degrades gracefully (a note,
        // not a failure) when census-density has never run.
        let market_blend = match app_census_density::sync_market_blend(&ctx).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        // `with_product_index` puts `census/market_blend` + `census/saturation`
        // in the worker's index + hook scope for this run — see
        // `census_common::product_index_datasets`.
        Ok(census_common::with_product_index(json!({
            "source": format!("census/nonemp/{year}"),
            "year": year,
            "trades": trade_summaries,
            "market_blend": market_blend,
            "records": record_count,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        })))
    }
}

/// Per-trade rollup of the parsed NES rows: dataset records keyed
/// `{naics}:{state_fips}` plus the ranking rows and totals the trade
/// summary is built from.
struct TradeRollup {
    records: Vec<(String, Value)>,
    /// (state label, nonemployers, avg receipts $/operator)
    ranked: Vec<(String, i64, i64)>,
    total_estab: i64,
    total_rcpt: i64,
}

/// Map the Census array-of-arrays payload (row 0 = header, addressed by the
/// pre-resolved column indices) into per-state records for one trade NAICS.
fn map_trade_rows(
    rows: &[Vec<String>],
    i_estab: usize,
    i_rcpt: usize,
    i_state: usize,
    naics: &str,
    label: &str,
    year: &str,
) -> TradeRollup {
    let mut records: Vec<(String, Value)> = Vec::new();
    let mut ranked: Vec<(String, i64, i64)> = Vec::new();
    let mut total_estab: i64 = 0;
    let mut total_rcpt: i64 = 0;

    for row in rows.iter().skip(1) {
        let Some(estab) = census_common::census_num(row.get(i_estab)) else {
            // Suppressed/jammed primary cell → not a reported operator place.
            continue;
        };
        // NRCPTOT is in $1,000s.
        let rcpt = census_common::census_num(row.get(i_rcpt)).unwrap_or(0);
        let st_fips = row.get(i_state).cloned().unwrap_or_default();
        let state = census_common::state_abbr(&st_fips).to_string();
        let avg = if estab > 0 { (rcpt * 1000) / estab } else { 0 };

        total_estab += estab;
        total_rcpt += rcpt;
        ranked.push((state.clone(), estab, avg));

        records.push((
            format!("{naics}:{st_fips}"),
            json!({
                "naics": naics,
                "trade": label,
                "state": state,
                "state_fips": st_fips,
                "nonemployers": estab,
                "receipts_thousands": rcpt,
                "avg_receipts_per_operator": avg,
                "year": year,
            }),
        ));
    }

    TradeRollup {
        records,
        ranked,
        total_estab,
        total_rcpt,
    }
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
        let app = CensusNonemp;
        let m = app.manifest();
        let schema = m.params_schema.expect("rich manifest declares a schema");
        let props = schema["properties"]
            .as_object()
            .expect("schema declares properties");
        assert!(!m.examples.is_empty(), "a schema needs worked examples");
        assert!(m.output_shape.is_some(), "agents need the result shape");
        let mut shipped = vec![app.default_params()];
        shipped.extend(m.examples.iter().map(|e| e.params.clone()));
        for params in shipped {
            for key in params.as_object().expect("params are an object").keys() {
                assert!(props.contains_key(key), "undeclared param '{key}'");
            }
        }
    }

    /// Wiring guard: `run()` must return its result through
    /// `census_common::with_product_index`. Without that declaration the two
    /// `census/*` products this run re-derives are invisible — no per-record
    /// search doc, and (worker `run_indexed_apps`) no watch, trigger or saved
    /// search scoped to app `census` can EVER fire for this run.
    ///
    /// The needle is split so this assertion cannot match itself.
    #[test]
    fn run_result_declares_the_census_product_datasets() {
        let needle = concat!("census_common::with_product_index", "(json!(");
        assert_eq!(
            include_str!("lib.rs").matches(needle).count(),
            1,
            "census-nonemp's run() must wrap its result exactly once with {needle}"
        );
        let empty = json!({});
        assert_eq!(
            census_common::with_product_index(empty)["index_datasets"],
            json!([
                { "app": "census", "dataset": "market_blend" },
                { "app": "census", "dataset": "saturation" },
            ])
        );
    }

    // Rows shaped like the real NES array-of-arrays payload: row 0 is the
    // header ["NAME","NESTAB","NRCPTOT","state"], data rows follow.
    // "-666666666" is the Census disclosure-suppression sentinel.
    fn nes_rows(data: &[[&str; 4]]) -> Vec<Vec<String>> {
        let mut rows = vec![vec![
            "NAME".to_string(),
            "NESTAB".to_string(),
            "NRCPTOT".to_string(),
            "state".to_string(),
        ]];
        rows.extend(
            data.iter()
                .map(|r| r.iter().map(|c| c.to_string()).collect::<Vec<_>>()),
        );
        rows
    }

    fn rollup(data: &[[&str; 4]]) -> TradeRollup {
        map_trade_rows(
            &nes_rows(data),
            1,
            2,
            3,
            "2382",
            "Building equipment",
            "2021",
        )
    }

    #[test]
    fn suppressed_establishment_rows_are_dropped_not_counted_as_zero() {
        let r = rollup(&[
            ["California", "100", "5000", "06"],
            ["Wyoming", "-666666666", "5000", "56"],
        ]);
        // The jammed NESTAB cell drops the whole row: it must not appear as a
        // zero-operator state, and its receipts must not leak into the totals.
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].0, "2382:06");
        assert_eq!(r.total_estab, 100);
        assert_eq!(r.total_rcpt, 5000);
        assert_eq!(r.ranked.len(), 1);
    }

    #[test]
    fn suppressed_receipts_currently_sum_as_zero_not_null() {
        // CURRENT behavior (unwrap_or(0)): a suppressed NRCPTOT cell keeps the
        // row but records $0 receipts, indistinguishable from a genuine zero —
        // it also drags avg and the national average down. Flagged, not fixed:
        // behavior changes are out of scope for this test pass.
        let r = rollup(&[["California", "100", "-666666666", "06"]]);
        assert_eq!(r.records.len(), 1);
        let v = &r.records[0].1;
        assert_eq!(v["receipts_thousands"], 0);
        assert_eq!(v["avg_receipts_per_operator"], 0);
        assert_eq!(r.total_rcpt, 0);
    }

    #[test]
    fn avg_receipts_converts_thousands_to_dollars_per_operator() {
        // NRCPTOT is $1,000s: 500 → $500,000 across 10 operators = $50,000 each.
        let r = rollup(&[["California", "10", "500", "06"]]);
        let (key, v) = &r.records[0];
        assert_eq!(key, "2382:06");
        assert_eq!(v["avg_receipts_per_operator"], 50_000);
        assert_eq!(v["state"], "CA");
        assert_eq!(v["state_fips"], "06");
        assert_eq!(v["nonemployers"], 10);
    }
}
