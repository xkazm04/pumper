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
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct CensusNonemp;

const DEFAULT_YEAR: &str = "2021";

/// (4-digit NAICS 2017 code, friendly label) for the solo trades Ledgerline serves.
/// 4-digit because nonemployer data at 6-digit × state is disclosure-suppressed.
const DEFAULT_TRADES: &[(&str, &str)] = &[
    ("2382", "Building equipment contractors (plumbing, HVAC, electrical)"),
    ("5617", "Services to buildings & dwellings (landscaping, pool)"),
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
        let states = ctx
            .params
            .get("states")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();

        let trades: Vec<(String, String)> =
            match ctx.params.get("naics").and_then(Value::as_array) {
                Some(arr) => arr
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|c| {
                        let label = DEFAULT_TRADES
                            .iter()
                            .find(|(k, _)| *k == c)
                            .map(|(_, l)| l.to_string())
                            .unwrap_or_else(|| c.to_string());
                        (c.to_string(), label)
                    })
                    .collect(),
                None => DEFAULT_TRADES
                    .iter()
                    .map(|(c, l)| (c.to_string(), l.to_string()))
                    .collect(),
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

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut trade_summaries: Vec<Value> = Vec::new();

        for (naics, label) in &trades {
            let url = format!(
                "https://api.census.gov/data/{year}/nonemp?get=NAME,NESTAB,NRCPTOT&{for_clause}&{naics_var}={naics}&key={api_key}"
            );
            let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
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
                Error::App(format!("Census NES {year} NAICS {naics}: bad JSON rows: {e}"))
            })?;
            ctx.save_artifact(
                &format!("nonemp-{naics}.json"),
                &serde_json::to_vec_pretty(&rows)?,
            )
            .await?;

            let header = rows.first().cloned().unwrap_or_default();
            let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
            let i_estab = idx("NESTAB").ok_or_else(|| {
                Error::App(format!("Census NES NAICS {naics}: no NESTAB column in {header:?}"))
            })?;
            let i_rcpt = idx("NRCPTOT").ok_or_else(|| {
                Error::App(format!("Census NES NAICS {naics}: no NRCPTOT column in {header:?}"))
            })?;
            let i_state = idx("state").ok_or_else(|| {
                Error::App(format!("Census NES NAICS {naics}: no state column in {header:?}"))
            })?;

            // (state label, nonemployers, avg receipts $/operator)
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

                all_records.push((
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

            let mut by_density = ranked.clone();
            by_density.sort_by(|a, b| b.1.cmp(&a.1));
            let mut by_avg = ranked.clone();
            by_avg.sort_by(|a, b| b.2.cmp(&a.2));
            let national_avg = if total_estab > 0 { (total_rcpt * 1000) / total_estab } else { 0 };

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

        let summary = ctx.upsert_many("nonemployers", &all_records).await?;

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

        Ok(json!({
            "source": format!("census/nonemp/{year}"),
            "year": year,
            "trades": trade_summaries,
            "market_blend": market_blend,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        }))
    }
}

