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
//!
//! # COUNTY GRAIN — contract verdict: **ASSUMED**, not verified (N35, 2026-09-02)
//!
//! The census-density blend has always asserted "NES is state-only". That
//! assertion was never tested against the live API: this app only ever issued
//! `for=state:*`. N35 needed the answer, so the probe is now BUILT — `geo:
//! "county"` issues `for=county:*&in=state:{states}` for each requested NAICS
//! and reports a per-NAICS verdict under `county_probe` (`served` /
//! `empty` / `unsupported_geography` / `http_error`) — but it could **not be
//! RUN**: the build box that shipped it has no network and no Census key, the
//! same posture in which the BFS contract header (`census-bfs/src/lib.rs`) was
//! pinned, except that one was later LIVE-verified and this one has not been.
//!
//! **Recorded verdict: ASSUMED UNAVAILABLE.** Every downstream consumer is
//! built for that answer and degrades honestly if it is wrong in either
//! direction:
//!  - the blend's county cells carry `solo_grain: "state_carried"` and a Null
//!    `solo_operators` — never a per-county number nobody published;
//!  - the moment a real keyed run returns `served`, the county rows land in
//!    `nonemployers` with `geo: "county"` and the blend joins them at county
//!    grain with `solo_grain: "county"` — **no code change**, only this header
//!    and `docs/features/apps.md` need updating with the LIVE-verified verdict.
//!
//! Run it with a key to settle it:
//! `POST /jobs {"app":"census-nonemp","params":{"geo":"county","states":"06",
//! "naics":["2382"]}}` → read `county_probe`.

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
                        "description": "Comma-separated state FIPS list (e.g. \"06,12,48\"). Empty or \"*\" = all states; REQUIRED when geo=county."
                    },
                    "geo": {
                        "type": "string",
                        "enum": ["state", "county"],
                        "description": "Geographic grain. `county` is the N35 PROBE of whether NES serves county rows at 4-digit NAICS — the answer is reported per NAICS under `county_probe` and is recorded as ASSUMED-unavailable in this app's doc header until a keyed run settles it. `county` REQUIRES a `states` FIPS filter."
                    },
                    "naics": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "4-digit NAICS trade codes. 6-digit × state is disclosure-suppressed for nonemployers (HTTP 204). Default: the enabled trades/taxonomy registry codes, else 2382 + 5617."
                    },
                    "allow_vintage_rewind": {
                        "type": "boolean",
                        "description": "Permit a run whose `year` is OLDER than the vintage this app already holds. Default false: these records are keyed without the year, so an older run overwrites current data and publishes the regression as a forward change (a `changed` revision, every watch/trigger on the dataset, a search re-index). Set true only when re-pointing the store at an older vintage is the intent."
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
                ManifestExample {
                    description: "N35 county probe: does NES serve county rows at 4-digit NAICS? \
                         Read the per-NAICS verdict under `county_probe`",
                    params: json!({
                        "year": DEFAULT_YEAR,
                        "geo": "county",
                        "states": "06",
                        "naics": ["2382"]
                    }),
                },
            ],
            output_shape: Some(
                "{source, geo, county_probe: {geo, verdicts: [{naics, verdict, detail}], \
                 served}, year, trades: [{naics, label, states_reported, total_nonemployers, \
                 total_receipts_thousands, national_avg_receipts_per_operator, \
                 states_with_receipts, suppressed: {places_dropped, receipts_cells}, \
                 top_states_by_density, top_states_by_avg_receipts} | {naics, label, note}], \
                 market_blend (the census/market_blend + census/atlas products this run \n                 re-derives), suppression, empty_answers, index_datasets, records, new, \
                 changed, unchanged} — a fully suppressed NAICS yields a `note` entry, not a \
                 failure; a suppressed NRCPTOT cell yields Null receipts (never $0), so the \
                 receipts totals cover `states_with_receipts` only",
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
        // N35: `county` is the probe grain. Default `state` — the shipped
        // behaviour, byte for byte.
        let geo = ctx
            .params
            .get("geo")
            .and_then(Value::as_str)
            .unwrap_or("state")
            .to_string();
        if geo == "county" && (states.is_empty() || states == "*") {
            return Err(Error::App(
                "geo=county requires a `states` FIPS filter (e.g. \"06\") — the NES county \
                 probe fans out `for=county:*&in=state:{states}`, and county:* nationwide \
                 is not a request this app will make blind"
                    .into(),
            ));
        }

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

        // Vintage watermark, BEFORE any write: NES records are keyed without the
        // year, so a run with an older `year` overwrites current data with older
        // data and publishes the regression as a forward change.
        let vintage = census_common::guard_vintage(&ctx, "nonemployers", &year).await?;

        let for_clause = nes_for_clause(&geo, &states);

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
        // Run-level suppression telemetry: what the API declined to tell us.
        let mut run_suppressed_places = 0usize;
        let mut run_suppressed_receipts = 0usize;
        let mut empty_answers = 0usize;
        // N35 probe: one verdict per NAICS, only in county mode.
        let mut probe_verdicts: Vec<Value> = Vec::new();

        for (naics, label) in &trades {
            let url = format!(
                "https://api.census.gov/data/{year}/nonemp?get=NAME,NESTAB,NRCPTOT&{for_clause}&{naics_var}={naics}&key={api_key}"
            );
            let resp = ctx
                .engines
                .http
                .fetch(HttpRequest::get(url.clone()))
                .await?;
            // THE PROBE. In county mode an answer that is not rows is the
            // ANSWER — "NES does not serve this grain" — so it is recorded as a
            // verdict and the run continues. In state mode nothing changes: a
            // non-success or non-JSON body is still a failed run, because the
            // shipped grain returning junk is a fault, not a finding.
            if geo == "county" {
                let verdict = county_probe_verdict(resp.status, &resp.body);
                probe_verdicts.push(json!({
                    "naics": naics,
                    "verdict": verdict,
                    "status": resp.status,
                    "detail": resp.body.chars().take(160).collect::<String>(),
                }));
                if verdict != PROBE_SERVED {
                    trade_summaries.push(json!({
                        "naics": naics, "label": label,
                        "note": format!("county probe: {verdict}"),
                    }));
                    if verdict == PROBE_EMPTY {
                        empty_answers += 1;
                    }
                    continue;
                }
            }
            // An empty answer (204, or a 200 with no body) is Census saying
            // "nothing published at this grain" for THIS trade → note it, don't
            // fail the whole run. Shared with the three sibling apps so the
            // guard cannot drift out of one of them again.
            if census_common::is_empty_answer(resp.status, &resp.body) {
                trade_summaries.push(json!({
                    "naics": naics, "label": label,
                    "note": "no data — nonemployer figures suppressed at this level",
                }));
                empty_answers += 1;
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
            // County mode only: the geography column trails the requested vars,
            // and is matched by NAME like every other column in this family.
            let cols = NesCols {
                estab: i_estab,
                rcpt: i_rcpt,
                state: i_state,
                county: idx("county"),
            };
            if geo == "county" && cols.county.is_none() {
                return Err(Error::App(format!(
                    "Census NES {year} NAICS {naics}: county probe returned rows with no \
                     'county' column in {header:?} — the answer is not county grain"
                )));
            }

            let TradeRollup {
                records,
                ranked,
                total_estab,
                total_rcpt,
                suppressed_places,
                suppressed_receipts,
            } = map_trade_rows(&rows, &cols, &geo, naics, label, &year);
            run_suppressed_places += suppressed_places;
            run_suppressed_receipts += suppressed_receipts;
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
            // Only states that REPORTED receipts can be ranked by them: a
            // suppressed state used to enter this ranking at $0 and sit at the
            // bottom as if it were the country's poorest trade market.
            let mut by_avg: Vec<(String, i64, i64)> = ranked
                .iter()
                .filter_map(|(s, e, avg)| avg.map(|a| (s.clone(), *e, a)))
                .collect();
            by_avg.sort_by_key(|(_, _, avg)| std::cmp::Reverse(*avg));
            // The denominator is the operator count of the states that reported
            // receipts — mixing in suppressed states' operators would divide
            // real money by more operators than earned it.
            let estab_with_receipts: i64 = ranked
                .iter()
                .filter(|(_, _, avg)| avg.is_some())
                .map(|(_, e, _)| *e)
                .sum();
            let national_avg = if estab_with_receipts > 0 {
                Value::from((total_rcpt * 1000) / estab_with_receipts)
            } else {
                Value::Null
            };

            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "states_reported": ranked.len(),
                "total_nonemployers": total_estab,
                "total_receipts_thousands": total_rcpt,
                "national_avg_receipts_per_operator": national_avg,
                // The receipts figures above cover only these states — the rest
                // are disclosure-suppressed, not zero.
                "states_with_receipts": by_avg.len(),
                "suppressed": {
                    "places_dropped": suppressed_places,
                    "receipts_cells": suppressed_receipts,
                },
                "top_states_by_density": by_density.iter().take(5)
                    .map(|(s, e, _)| json!({ "state": s, "nonemployers": e })).collect::<Vec<_>>(),
                "top_states_by_avg_receipts": by_avg.iter().take(5)
                    .map(|(s, _, a)| json!({ "state": s, "avg_receipts_per_operator": a })).collect::<Vec<_>>(),
            }));
        }

        // The store now holds this vintage — move the watermark.
        census_common::record_vintage(&ctx, "nonemployers", &year).await?;

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
        // The probe's answer, in the run result rather than only in the logs:
        // `served` on ANY requested NAICS is the finding that retires the
        // blend's `state_carried` fallback, and it must be readable by a human
        // and by a trigger alike.
        let county_probe = if geo == "county" {
            json!({
                "geo": "county",
                "served": probe_verdicts
                    .iter()
                    .any(|v| v["verdict"] == PROBE_SERVED),
                "verdicts": probe_verdicts,
                "doc_header_verdict": "ASSUMED unavailable — see the crate doc header",
            })
        } else {
            Value::Null
        };
        Ok(census_common::with_product_index(json!({
            "source": format!("census/nonemp/{year}"),
            "geo": geo,
            "county_probe": county_probe,
            "year": year,
            "vintage": vintage,
            "trades": trade_summaries,
            "market_blend": market_blend,
            // What the API declined to tell us this run, so a shrinking corpus
            // reads as suppression rather than as a market that vanished.
            "suppression": {
                "places_dropped": run_suppressed_places,
                "receipts_cells": run_suppressed_receipts,
            },
            "empty_answers": empty_answers,
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
    /// (state label, nonemployers, avg receipts $/operator) — states whose
    /// receipts are suppressed carry `None` for the average and are ranked
    /// nowhere, rather than ranked last at $0.
    ranked: Vec<(String, i64, Option<i64>)>,
    total_estab: i64,
    /// Sum of the receipts that were actually REPORTED. Suppressed cells add
    /// nothing (they used to add a fabricated 0), so this is a total over
    /// `states_with_receipts`, not over `states_reported`.
    total_rcpt: i64,
    /// Rows dropped entirely — the primary NESTAB cell was suppressed.
    suppressed_places: usize,
    /// Reported rows whose NRCPTOT cell was suppressed: the operator count is
    /// real, the money is unknown.
    suppressed_receipts: usize,
}

/// Pre-resolved NES column indices, matched by NAME (the geography columns
/// trail the requested `get=` vars, so position is never assumed).
pub struct NesCols {
    pub estab: usize,
    pub rcpt: usize,
    pub state: usize,
    /// Present only on a county-grain answer.
    pub county: Option<usize>,
}

/// The four county-probe verdicts. Strings, not an enum, because they are
/// published in the run result and read by a human deciding whether the blend's
/// `state_carried` fallback can be retired.
pub const PROBE_SERVED: &str = "served";
/// Census answered 204 / an empty body: the grain is *accepted* but nothing is
/// published for this NAICS — suppression, not a refusal of the geography.
pub const PROBE_EMPTY: &str = "empty";
/// The API refused the geography hierarchy itself (the shape BFS returns for
/// `for=state:*`): the strongest evidence that NES has no county grain.
pub const PROBE_UNSUPPORTED: &str = "unsupported_geography";
/// Any other non-success, or a success whose body is not a JSON array (the
/// missing-key HTML page).
pub const PROBE_HTTP_ERROR: &str = "http_error";

/// What one county-probe response says about NES county availability.
///
/// Extracted and tested rather than branched inline, because this predicate is
/// the entire deliverable of the probe: the difference between
/// `unsupported_geography` (NES has no county grain — the blend's
/// `state_carried` fallback is permanent) and `empty` (it does, but this NAICS
/// is suppressed — the fallback is per-cell) is the whole question, and reading
/// it off an HTTP status alone would answer it wrong.
pub fn county_probe_verdict(status: u16, body: &str) -> &'static str {
    if census_common::is_empty_answer(status, body) {
        return PROBE_EMPTY;
    }
    if (200..300).contains(&status) {
        return if body.trim_start().starts_with('[') {
            PROBE_SERVED
        } else {
            PROBE_HTTP_ERROR
        };
    }
    let lower = body.to_lowercase();
    if lower.contains("geography") || lower.contains("geo hierarchy") {
        PROBE_UNSUPPORTED
    } else {
        PROBE_HTTP_ERROR
    }
}

/// The `for=`/`in=` geography clause: all states, a state FIPS subset, or
/// `county:*` within the given states (the N35 probe).
pub fn nes_for_clause(geo: &str, states: &str) -> String {
    if geo == "county" {
        format!("for=county:*&in=state:{states}")
    } else if states.is_empty() || states == "*" {
        "for=state:*".to_string()
    } else {
        format!("for=state:{states}")
    }
}

/// Map the Census array-of-arrays payload (row 0 = header, addressed by the
/// pre-resolved column indices) into per-place records for one trade NAICS.
///
/// State rows keep their historical key (`{naics}:{state_fips}`) and shape; a
/// county row is keyed `{naics}:{state_fips}{county_fips}` and stamped
/// `geo: "county"` so the blend can tell the grains apart in SQL rather than
/// by key length.
fn map_trade_rows(
    rows: &[Vec<String>],
    cols: &NesCols,
    geo: &str,
    naics: &str,
    label: &str,
    year: &str,
) -> TradeRollup {
    let mut records: Vec<(String, Value)> = Vec::new();
    let mut ranked: Vec<(String, i64, Option<i64>)> = Vec::new();
    let mut total_estab: i64 = 0;
    let mut total_rcpt: i64 = 0;
    let mut suppressed_places = 0usize;
    let mut suppressed_receipts = 0usize;

    for row in rows.iter().skip(1) {
        let Some(estab) = census_common::census_num(row.get(cols.estab)) else {
            // Suppressed/jammed primary cell → not a reported operator place.
            suppressed_places += 1;
            continue;
        };
        // NRCPTOT is in $1,000s. Kept as an Option all the way to the record: a
        // suppressed receipts cell is NOT $0 of business. Defaulting it to 0
        // used to travel the whole pipeline — into `total_receipts_thousands`,
        // into the national average, into the blend's `solo_receipts_thousands`
        // and out the far end as a $0 succession-wave receipt for a state that
        // simply wasn't allowed to report.
        let rcpt = census_common::census_num(row.get(cols.rcpt));
        if rcpt.is_none() {
            suppressed_receipts += 1;
        }
        let st_fips = row.get(cols.state).cloned().unwrap_or_default();
        let state = census_common::state_abbr(&st_fips).to_string();
        let county_fips = cols.county.and_then(|i| row.get(i)).cloned();
        // The place label matches the employer side's `place_of` exactly
        // (`CA` / `CA·037`), because the blend joins the two by it.
        let place = match &county_fips {
            Some(c) => format!("{state}·{c}"),
            None => state.clone(),
        };
        let key = match &county_fips {
            Some(c) => format!("{naics}:{}", census_common::county_fips5(&st_fips, c)),
            None => format!("{naics}:{st_fips}"),
        };
        let avg = match rcpt {
            Some(r) if estab > 0 => Some((r * 1000) / estab),
            _ => None,
        };

        total_estab += estab;
        total_rcpt += rcpt.unwrap_or(0);
        ranked.push((place.clone(), estab, avg));

        records.push((
            key,
            json!({
                "naics": naics,
                "trade": label,
                "state": state,
                "state_fips": st_fips,
                // The grain this row IS. Legacy rows predate the field and are
                // read as `state` (the only grain that existed then) — see the
                // blend's solo split.
                "geo": geo,
                "county_fips": county_fips,
                "place": place,
                "nonemployers": estab,
                // Null, not absent: the column stays in every CSV export and
                // every consumer sees an explicit "suppressed" rather than a
                // field that silently came and went between vintages.
                "receipts_thousands": rcpt.map(Value::from).unwrap_or(Value::Null),
                "avg_receipts_per_operator": avg.map(Value::from).unwrap_or(Value::Null),
                "year": year,
            }),
        ));
    }

    TradeRollup {
        records,
        ranked,
        total_estab,
        total_rcpt,
        suppressed_places,
        suppressed_receipts,
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
        let cols = NesCols {
            estab: 1,
            rcpt: 2,
            state: 3,
            county: None,
        };
        map_trade_rows(
            &nes_rows(data),
            &cols,
            "state",
            "2382",
            "Building equipment",
            "2021",
        )
    }

    /// The N35 probe's whole deliverable: telling "NES refuses this geography"
    /// apart from "NES serves it and withheld this cell". Answering the first
    /// with the second's verdict would retire the blend's `state_carried`
    /// fallback on evidence that never existed.
    #[test]
    fn a_refused_geography_is_not_read_as_a_suppressed_cell() {
        // The BFS-shaped refusal: HTTP 400 naming the geography hierarchy.
        assert_eq!(
            county_probe_verdict(400, "error: unknown/unsupported geography hierarchy"),
            PROBE_UNSUPPORTED
        );
        // Suppression: the grain is accepted, this NAICS is not published.
        assert_eq!(county_probe_verdict(204, ""), PROBE_EMPTY);
        assert_eq!(county_probe_verdict(200, "   "), PROBE_EMPTY);
        // The answer we are looking for.
        assert_eq!(
            county_probe_verdict(200, "[[\"NAME\",\"NESTAB\"]]"),
            PROBE_SERVED
        );
        // A 200 that is the missing-key HTML page is NOT a served verdict.
        assert_eq!(
            county_probe_verdict(200, "<html>missing key</html>"),
            PROBE_HTTP_ERROR
        );
        // A non-success that says nothing about geography stays undiagnosed
        // rather than being promoted to "NES has no counties".
        assert_eq!(county_probe_verdict(500, "server error"), PROBE_HTTP_ERROR);
    }

    /// A county answer must key and label itself as county grain — otherwise it
    /// lands on top of the state row for the same NAICS and the state's solo
    /// count silently becomes one county's.
    #[test]
    fn county_rows_key_on_five_digit_fips_and_never_overwrite_the_state_row() {
        let mut rows = vec![vec![
            "NAME".to_string(),
            "NESTAB".to_string(),
            "NRCPTOT".to_string(),
            "state".to_string(),
            "county".to_string(),
        ]];
        rows.push(
            ["Los Angeles County", "900", "4000", "06", "037"]
                .iter()
                .map(|c| c.to_string())
                .collect(),
        );
        let cols = NesCols {
            estab: 1,
            rcpt: 2,
            state: 3,
            county: Some(4),
        };
        let r = map_trade_rows(&rows, &cols, "county", "2382", "Building equipment", "2021");
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].0, "2382:06037", "not the state key 2382:06");
        let v = &r.records[0].1;
        assert_eq!(v["geo"], "county");
        assert_eq!(v["county_fips"], "037");
        assert_eq!(v["state_fips"], "06");
        // The place label is the one the employer side writes, so the blend can
        // join the two halves without a second convention.
        assert_eq!(v["place"], "CA·037");
        assert_eq!(r.ranked[0].0, "CA·037");

        // The shipped state path is untouched: same key, and `geo` says state.
        let s = rollup(&[["California", "100", "5000", "06"]]);
        assert_eq!(s.records[0].0, "2382:06");
        assert_eq!(s.records[0].1["geo"], "state");
        assert_eq!(s.records[0].1["county_fips"], Value::Null);
        assert_eq!(s.records[0].1["place"], "CA");
    }

    /// `county` fans out inside the requested states; the state clause is
    /// unchanged in both of its shipped forms.
    #[test]
    fn the_county_clause_stays_inside_the_requested_states() {
        assert_eq!(nes_for_clause("state", ""), "for=state:*");
        assert_eq!(nes_for_clause("state", "*"), "for=state:*");
        assert_eq!(nes_for_clause("state", "06,48"), "for=state:06,48");
        assert_eq!(
            nes_for_clause("county", "06"),
            "for=county:*&in=state:06",
            "county:* nationwide is never requested"
        );
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
        assert_eq!(r.suppressed_places, 1, "the drop is counted, not absorbed");
    }

    /// The anti-pattern this now defends against (was FLAGGED here, unfixed,
    /// until 2026-08-12): a suppressed NRCPTOT cell recorded as `$0` receipts,
    /// indistinguishable from a genuine zero. That fabricated zero travelled the
    /// whole pipeline — into the state's `avg_receipts_per_operator`, into the
    /// national average, into the blend's `solo_receipts_thousands` and out the
    /// far end as a **$0 succession-wave receipt** for a state that was simply
    /// not allowed to report its money.
    #[test]
    fn suppressed_receipts_are_null_not_a_fabricated_zero() {
        let r = rollup(&[["California", "100", "-666666666", "06"]]);
        // The row is KEPT — the operator count is real data.
        assert_eq!(r.records.len(), 1);
        let v = &r.records[0].1;
        assert_eq!(v["nonemployers"], 100);
        // The money is unknown, and says so.
        assert_eq!(v["receipts_thousands"], Value::Null);
        assert_eq!(v["avg_receipts_per_operator"], Value::Null);
        // Nothing was added to the reported-receipts total, and the withholding
        // is counted rather than absorbed.
        assert_eq!(r.total_rcpt, 0);
        assert_eq!(r.suppressed_receipts, 1);
        assert_eq!(r.suppressed_places, 0);
        // The state is ranked by density but not by receipts it never reported.
        assert_eq!(r.ranked, vec![("CA".to_string(), 100, None)]);
    }

    /// The other half of the honesty: `census_num` distinguishes a REPORTED
    /// zero from a suppressed cell, and a reported zero must survive as a
    /// measured 0 — the fix above must not turn genuine zeros into Nulls.
    #[test]
    fn a_reported_zero_stays_a_measured_zero() {
        let r = rollup(&[["Wyoming", "10", "0", "56"]]);
        let v = &r.records[0].1;
        assert_eq!(v["receipts_thousands"], 0);
        assert_eq!(v["avg_receipts_per_operator"], 0);
        assert_eq!(r.suppressed_receipts, 0);
        assert_eq!(r.ranked, vec![("WY".to_string(), 10, Some(0))]);
        // And a reported zero establishment count is a real 0-operator state,
        // not a dropped row.
        let z = rollup(&[["Wyoming", "0", "0", "56"]]);
        assert_eq!(z.records.len(), 1);
        assert_eq!(z.records[0].1["nonemployers"], 0);
        // 0 operators → no per-operator average to compute (not a divide-by-0).
        assert_eq!(z.records[0].1["avg_receipts_per_operator"], Value::Null);
        assert_eq!(z.suppressed_places, 0);
    }

    /// End-to-end suppression honesty across the app boundary: a suppressed
    /// NRCPTOT must reach `census/market_blend` as an ABSENT succession figure,
    /// not as `$0`. This is the far end of the chain the flipped test above
    /// starts — the blend reads `receipts_thousands` off the stored record.
    #[test]
    fn a_suppressed_receipts_cell_yields_no_succession_dollars_in_the_blend() {
        let solo_record = |data: &[[&str; 4]]| rollup(data).records[0].1.clone();
        let suppressed = solo_record(&[["California", "100", "-666666666", "06"]]);
        let reported = solo_record(&[["California", "100", "500", "06"]]);
        let employers = vec![serde_json::json!({
            "naics": "238220", "geo": "state", "place": "CA", "state_fips": "06",
            "establishments": 10, "year": "2022",
        })];
        let bands = vec![
            serde_json::json!({"sector":"23","state_fips":"06","age_band":"55 to 64","owners":40,"year":"2021"}),
            serde_json::json!({"sector":"23","state_fips":"06","age_band":"25 to 54","owners":60,"year":"2021"}),
        ];
        let blend = |solo: Value| {
            app_census_density::blend_market(
                &employers,
                &[solo],
                &std::collections::BTreeMap::new(),
                &bands,
                &[],
            )[0]
            .1
            .clone()
        };
        // Reported receipts → a real dollar figure (40% of $500k).
        let ok = blend(reported);
        assert_eq!(ok["succession_receipts"], 200_000);
        // Suppressed receipts → the share is still known, the dollars are NOT.
        let sup = blend(suppressed);
        assert_eq!(sup["pct_owners_55plus"], serde_json::json!(0.4));
        assert_eq!(
            sup["succession_receipts"],
            Value::Null,
            "a withheld receipts cell must not become a $0 succession wave"
        );
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
