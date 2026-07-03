//! US trades-business DENSITY via the Census County Business Patterns (CBP) API.
//!
//! The market-strength backbone for Ledgerline's geographic launch ranking: how
//! many plumbing/HVAC, electrical, landscaping and pool-service establishments (plus
//! their employment + payroll) exist per state (or county), by NAICS. Upserted into
//! the `establishments` dataset so a scheduled annual run only surfaces what changed.
//! Also joins a Census ACS population/household base to rank by SATURATION
//! (establishments per 10k), not just absolute size. Fast path — GET JSON APIs, no
//! HTML parsing, no browser.
//!
//! Data type: REFERENCE DENSITY (establishment counts). Access: FREE key required.
//! Serves the Ledgerline bookkeeping app's geographic launch ranking — a separate
//! Pumper consumer from the grant-writing pipeline in `catalog/data-sources.toml`,
//! so it is deliberately NOT listed in that (grant-focused) catalog.
//!
//! Contract notes (verified 2026-07-03): `https://api.census.gov/data/{year}/cbp`
//! **requires a free API key** — a keyless request 302-redirects to
//! `/data/missing_key.html` (a 200 HTML page, not JSON). Success is a JSON
//! array-of-arrays: row 0 is the header (e.g. `["ESTAB","EMP","PAYANN","state",
//! "NAICS2017"]`), each further row a data tuple. Columns are matched by NAME (the
//! geography column trails the requested `get=` vars), never by fixed position.
//! Plumbing & HVAC are FUSED in NAICS 238220 (Census cannot split them); electrical
//! is 238210; landscaping 561730; pool service falls under the broader 561790
//! (Other Services to Buildings & Dwellings). Key: params.api_key → env
//! CENSUS_API_KEY. CBP vintages from 2017 use the `NAICS2017` predicate variable
//! (override via params.naics_var for other vintages).

use std::collections::BTreeMap;

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct CensusDensity;

const DEFAULT_YEAR: &str = "2022";
const DEFAULT_NAICS_VAR: &str = "NAICS2017";

/// (NAICS 2017 code, friendly label) for the trades Ledgerline serves. Plumbing &
/// HVAC are fused in 238220; pool service falls under the broader 561790.
const DEFAULT_TRADES: &[(&str, &str)] = &[
    ("238220", "Plumbing, heating & A/C contractors"),
    ("238210", "Electrical contractors"),
    ("561730", "Landscaping services"),
    ("561790", "Other services to buildings & dwellings (incl. pool service)"),
];

#[async_trait]
impl ScrapeApp for CensusDensity {
    fn name(&self) -> &'static str {
        "census-density"
    }

    fn description(&self) -> &'static str {
        "US trades-business density from Census County Business Patterns (CBP JSON \
         API). Establishment counts, employment & annual payroll per trade NAICS, by \
         state (or county), upserted into the `establishments` dataset. Requires a \
         FREE Census API key (params.api_key or env CENSUS_API_KEY; sign up at \
         https://api.census.gov/data/key_signup.html). Params: {\"year\": \"2022\", \
         \"geo\": \"state|county\", \"states\": \"06,12,48\" (FIPS list; REQUIRED for \
         county), \"naics\": [\"238220\",...], \"naics_var\": \"NAICS2017\", \
         \"normalize\": true, \"denominator\": \"households|population|owner_occupied\", \
         \"api_key\": \"...\"}"
    }

    // Annual source — enable a yearly refresh once CENSUS_API_KEY is set in the
    // environment (scheduled runs use default_params and can't carry a key inline):
    // fn schedule(&self) -> Option<&'static str> { Some("0 0 6 15 3 *") } // Mar 15

    fn default_params(&self) -> Value {
        json!({ "year": DEFAULT_YEAR, "geo": "state" })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let year = ctx
            .params
            .get("year")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_YEAR)
            .to_string();
        let geo = ctx
            .params
            .get("geo")
            .and_then(Value::as_str)
            .unwrap_or("state")
            .to_string();
        // Comma-separated FIPS list; "" or "*" => all states. Required for county.
        let states = ctx
            .params
            .get("states")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        let naics_var = ctx
            .params
            .get("naics_var")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_NAICS_VAR)
            .to_string();
        // Saturation normalization: divide establishment counts by a Census ACS
        // population/household base so the ranking reflects DENSITY, not raw size.
        let normalize = ctx
            .params
            .get("normalize")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let denom_kind = ctx
            .params
            .get("denominator")
            .and_then(Value::as_str)
            .unwrap_or("households")
            .to_string();
        let acs_dataset = ctx
            .params
            .get("acs_dataset")
            .and_then(Value::as_str)
            .unwrap_or("acs/acs5")
            .to_string();
        let acs_year = ctx
            .params
            .get("acs_year")
            .and_then(Value::as_str)
            .unwrap_or(&year)
            .to_string();

        // Trades: params.naics (array of codes) overrides the defaults; a custom
        // code keeps its own string as the label.
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

        // Key: param → env. Census requires it (keyless 302 → missing_key.html).
        let api_key = ctx
            .params
            .get("api_key")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| std::env::var("CENSUS_API_KEY").ok())
            .filter(|k| !k.trim().is_empty());
        let api_key = match api_key {
            Some(k) => k,
            None => {
                return Err(Error::App(
                    "census-density needs a free Census API key — set env \
                     CENSUS_API_KEY or pass params.api_key. Get one instantly at \
                     https://api.census.gov/data/key_signup.html"
                        .into(),
                ))
            }
        };

        if geo == "county" && (states.is_empty() || states == "*") {
            return Err(Error::App(
                "geo=county requires a `states` FIPS filter (e.g. \"06,12,48\") — \
                 CBP does not serve county:* across all states at once"
                    .into(),
            ));
        }

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut trade_summaries: Vec<Value> = Vec::new();
        // place label -> combined establishments across all trades (overall ranking).
        let mut overall: BTreeMap<String, i64> = BTreeMap::new();

        for (naics, label) in &trades {
            let url = build_url(&year, &geo, &states, naics, &naics_var, &api_key);
            let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "Census CBP {year} NAICS {naics}: HTTP {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(160).collect::<String>()
                )));
            }
            // Success bodies are a JSON array; anything else is the missing-key HTML
            // (200) or a plaintext error page.
            if !resp.body.trim_start().starts_with('[') {
                let hint = if resp.body.contains("key") {
                    " — looks like an invalid/missing API key"
                } else {
                    ""
                };
                return Err(Error::App(format!(
                    "Census CBP {year} NAICS {naics}: response was not JSON{hint} \
                     (starts: {})",
                    resp.body.chars().take(160).collect::<String>()
                )));
            }
            let rows: Vec<Vec<String>> = serde_json::from_str(&resp.body).map_err(|e| {
                Error::App(format!("Census CBP {year} NAICS {naics}: bad JSON rows: {e}"))
            })?;
            ctx.save_artifact(&format!("cbp-{naics}.json"), &serde_json::to_vec_pretty(&rows)?)
                .await?;

            let header = rows.first().cloned().unwrap_or_default();
            let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
            let i_estab = match idx("ESTAB") {
                Some(i) => i,
                None => {
                    return Err(Error::App(format!(
                        "Census CBP {year} NAICS {naics}: no ESTAB column in {header:?}"
                    )))
                }
            };
            let i_geo = match idx(geo.as_str()) {
                Some(i) => i,
                None => {
                    return Err(Error::App(format!(
                        "Census CBP {year} NAICS {naics}: no '{geo}' column in {header:?}"
                    )))
                }
            };
            let i_state = idx("state");
            let i_emp = idx("EMP");
            let i_pay = idx("PAYANN");

            let mut places_reported: u32 = 0;
            let mut total_estab: i64 = 0;
            let mut total_emp: i64 = 0;
            let mut ranked: Vec<(String, i64)> = Vec::new();

            for row in rows.iter().skip(1) {
                let geo_code = row.get(i_geo).cloned().unwrap_or_default();
                let estab = row
                    .get(i_estab)
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let emp = i_emp
                    .and_then(|i| row.get(i))
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                let pay = i_pay
                    .and_then(|i| row.get(i))
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);

                let (st_fips, county_fips) = if geo == "county" {
                    let st = i_state
                        .and_then(|i| row.get(i))
                        .cloned()
                        .unwrap_or_default();
                    (st, Some(geo_code.clone()))
                } else {
                    (geo_code.clone(), None)
                };
                let place = match &county_fips {
                    Some(c) => format!("{}·{}", state_abbr(&st_fips), c),
                    None => state_abbr(&st_fips).to_string(),
                };
                let key = match &county_fips {
                    Some(c) => format!("{naics}:{st_fips}{c}"),
                    None => format!("{naics}:{st_fips}"),
                };

                places_reported += 1;
                total_estab += estab;
                total_emp += emp;
                ranked.push((place.clone(), estab));
                *overall.entry(place.clone()).or_insert(0) += estab;

                all_records.push((
                    key,
                    json!({
                        "naics": naics,
                        "trade": label,
                        "geo": geo,
                        "place": place,
                        "state_fips": st_fips,
                        "county_fips": county_fips,
                        "establishments": estab,
                        "employees": emp,
                        "annual_payroll_thousands": pay,
                        "year": year,
                    }),
                ));
            }

            ranked.sort_by(|a, b| b.1.cmp(&a.1));
            let top: Vec<Value> = ranked
                .iter()
                .take(5)
                .map(|(p, e)| json!({ "place": p, "establishments": e }))
                .collect();
            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "places_reported": places_reported,
                "total_establishments": total_estab,
                "total_employees": total_emp,
                "top": top,
            }));
        }

        let summary = ctx.upsert_many("establishments", &all_records).await?;

        let mut overall_vec: Vec<(String, i64)> =
            overall.iter().map(|(k, v)| (k.clone(), *v)).collect();
        overall_vec.sort_by(|a, b| b.1.cmp(&a.1));
        let top_overall: Vec<Value> = overall_vec
            .iter()
            .take(10)
            .map(|(p, e)| json!({ "place": p, "combined_establishments": e }))
            .collect();

        // Per-capita saturation: join the combined establishment counts to an ACS
        // population/household base and rank by establishments per 10k of that base.
        // Degrades gracefully — a denominator-fetch failure leaves the absolute
        // ranking intact and records the reason under `normalization`.
        let mut saturation: Vec<Value> = Vec::new();
        let normalization: Value = if normalize {
            match fetch_denominator(&ctx, &acs_dataset, &acs_year, &geo, &states, &api_key).await {
                Ok(denom) => {
                    let mut rows: Vec<(String, i64, i64, f64)> = overall
                        .iter()
                        .filter_map(|(place, estab)| {
                            let d = denom.get(place)?;
                            let base = match denom_kind.as_str() {
                                "population" => d.population,
                                "owner_occupied" => d.owner_occupied,
                                _ => d.households,
                            };
                            if base <= 0 {
                                return None;
                            }
                            let per_10k = (*estab as f64) / (base as f64) * 10_000.0;
                            Some((place.clone(), *estab, base, per_10k))
                        })
                        .collect();
                    rows.sort_by(|a, b| b.3.total_cmp(&a.3));
                    let matched = rows.len();
                    saturation = rows
                        .iter()
                        .take(60)
                        .map(|(p, e, base, per_10k)| {
                            json!({
                                "place": p,
                                "combined_establishments": e,
                                "base": base,
                                "per_10k": (per_10k * 100.0).round() / 100.0,
                            })
                        })
                        .collect();
                    json!({
                        "dataset": acs_dataset,
                        "acs_year": acs_year,
                        "denominator": denom_kind,
                        "places_matched": matched,
                    })
                }
                Err(e) => json!({ "skipped": format!("{e}") }),
            }
        } else {
            json!({ "skipped": "normalize=false" })
        };

        Ok(json!({
            "source": format!("census/cbp/{year}"),
            "geo": geo,
            "year": year,
            "trades": trade_summaries,
            "top_places_overall": top_overall,
            "top_places_by_saturation": saturation,
            "normalization": normalization,
            "records": all_records.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        }))
    }
}

/// Build a CBP API query. State mode returns all states (or a FIPS subset); county
/// mode fans out `county:*` within the supplied state FIPS list.
fn build_url(
    year: &str,
    geo: &str,
    states: &str,
    naics: &str,
    naics_var: &str,
    key: &str,
) -> String {
    format!(
        "https://api.census.gov/data/{year}/cbp?get=ESTAB,EMP,PAYANN&{}&{naics_var}={naics}&key={key}",
        for_clause(geo, states)
    )
}

/// The `for=`/`in=` geography clause shared by the CBP and ACS queries: all states,
/// a state FIPS subset, or `county:*` within the given states.
fn for_clause(geo: &str, states: &str) -> String {
    if geo == "county" {
        format!("for=county:*&in=state:{states}")
    } else if states.is_empty() || states == "*" {
        "for=state:*".to_string()
    } else {
        format!("for=state:{states}")
    }
}

/// Place label matching the CBP loop: state abbreviation, or `AB·CCC` for a county.
fn place_of(st_fips: &str, county_fips: Option<&str>) -> String {
    match county_fips {
        Some(c) => format!("{}·{}", state_abbr(st_fips), c),
        None => state_abbr(st_fips).to_string(),
    }
}

/// ACS population/household base for saturation. Jam values (negatives) → 0.
struct Denom {
    population: i64,
    households: i64,
    owner_occupied: i64,
}

/// Fetch the ACS denominator (total population, households, owner-occupied units)
/// for the same geography, keyed by the same place label as the CBP loop so the two
/// join cleanly. ACS 5-year by default (covers every county).
async fn fetch_denominator(
    ctx: &AppContext,
    dataset: &str,
    year: &str,
    geo: &str,
    states: &str,
    key: &str,
) -> Result<BTreeMap<String, Denom>> {
    // B01003_001E total population; B11001_001E total households; B25003_002E
    // owner-occupied housing units.
    let url = format!(
        "https://api.census.gov/data/{year}/{dataset}?get=B01003_001E,B11001_001E,B25003_002E&{}&key={key}",
        for_clause(geo, states)
    );
    let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
    if !resp.is_success() {
        return Err(Error::App(format!(
            "ACS {dataset} {year}: HTTP {} (starts: {})",
            resp.status,
            resp.body.chars().take(120).collect::<String>()
        )));
    }
    if !resp.body.trim_start().starts_with('[') {
        return Err(Error::App(format!(
            "ACS {dataset} {year}: response was not JSON (starts: {})",
            resp.body.chars().take(120).collect::<String>()
        )));
    }
    let rows: Vec<Vec<String>> = serde_json::from_str(&resp.body)
        .map_err(|e| Error::App(format!("ACS {dataset} {year}: bad JSON rows: {e}")))?;
    ctx.save_artifact("acs-denominator.json", &serde_json::to_vec_pretty(&rows)?)
        .await?;

    let header = rows.first().cloned().unwrap_or_default();
    let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
    let i_pop = idx("B01003_001E");
    let i_hh = idx("B11001_001E");
    let i_own = idx("B25003_002E");
    let i_geo = idx(geo)
        .ok_or_else(|| Error::App(format!("ACS {dataset}: no '{geo}' column in {header:?}")))?;
    let i_state = idx("state");

    let num = |row: &[String], i: Option<usize>| -> i64 {
        i.and_then(|i| row.get(i))
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(0)
    };

    let mut map: BTreeMap<String, Denom> = BTreeMap::new();
    for row in rows.iter().skip(1) {
        let geo_code = row.get(i_geo).cloned().unwrap_or_default();
        let (st_fips, county_fips) = if geo == "county" {
            let st = i_state.and_then(|i| row.get(i)).cloned().unwrap_or_default();
            (st, Some(geo_code))
        } else {
            (geo_code, None)
        };
        let place = place_of(&st_fips, county_fips.as_deref());
        map.insert(
            place,
            Denom {
                population: num(row, i_pop),
                households: num(row, i_hh),
                owner_occupied: num(row, i_own),
            },
        );
    }
    Ok(map)
}

/// 2-digit state FIPS → USPS abbreviation (50 states + DC + PR). Unknown codes
/// pass through unchanged so nothing is silently dropped.
fn state_abbr(fips: &str) -> &str {
    match fips {
        "01" => "AL", "02" => "AK", "04" => "AZ", "05" => "AR", "06" => "CA",
        "08" => "CO", "09" => "CT", "10" => "DE", "11" => "DC", "12" => "FL",
        "13" => "GA", "15" => "HI", "16" => "ID", "17" => "IL", "18" => "IN",
        "19" => "IA", "20" => "KS", "21" => "KY", "22" => "LA", "23" => "ME",
        "24" => "MD", "25" => "MA", "26" => "MI", "27" => "MN", "28" => "MS",
        "29" => "MO", "30" => "MT", "31" => "NE", "32" => "NV", "33" => "NH",
        "34" => "NJ", "35" => "NM", "36" => "NY", "37" => "NC", "38" => "ND",
        "39" => "OH", "40" => "OK", "41" => "OR", "42" => "PA", "44" => "RI",
        "45" => "SC", "46" => "SD", "47" => "TN", "48" => "TX", "49" => "UT",
        "50" => "VT", "51" => "VA", "53" => "WA", "54" => "WV", "55" => "WI",
        "56" => "WY", "72" => "PR",
        other => other,
    }
}
