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

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
};
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
    (
        "561790",
        "Other services to buildings & dwellings (incl. pool service)",
    ),
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

    // Needs a Census API key. A scheduled run uses default_params (no inline key),
    // so the env var is the readiness signal `GET /apps` reports.
    fn requires(&self) -> &'static [pumper_core::Requirement] {
        &[pumper_core::Requirement::Env("CENSUS_API_KEY")]
    }

    // Annual source — enable a yearly refresh once CENSUS_API_KEY is set in the
    // environment (scheduled runs use default_params and can't carry a key inline):
    // fn schedule(&self) -> Option<&'static str> { Some("0 0 6 15 3 *") } // Mar 15

    fn default_params(&self) -> Value {
        json!({ "year": DEFAULT_YEAR, "geo": "state" })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "year": { "type": "string", "description": "CBP vintage (CBP lags ~2 years)." },
                    "geo": {
                        "type": "string",
                        "enum": ["state", "county"],
                        "description": "Geographic grain. `county` REQUIRES a `states` FIPS filter — CBP does not serve county:* nationwide."
                    },
                    "states": {
                        "type": "string",
                        "description": "Comma-separated state FIPS list (e.g. \"06,12,48\"). Empty or \"*\" = all states; required when geo=county."
                    },
                    "naics": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "6-digit NAICS trade codes. Default: the enabled trades/taxonomy registry codes, else the four built-in trade codes."
                    },
                    "naics_var": {
                        "type": "string",
                        "description": "Classification predicate (NAICS2017 / NAICS2022) — must match the vintage the requested year publishes."
                    },
                    "normalize": {
                        "type": "boolean",
                        "description": "Join an ACS base and rank by establishments per 10k (saturation). Default true; a denominator failure degrades to the absolute ranking."
                    },
                    "denominator": {
                        "type": "string",
                        "enum": ["households", "population", "owner_occupied"],
                        "description": "Which ACS base normalization divides by."
                    },
                    "acs_dataset": { "type": "string", "description": "ACS dataset path for the denominator (default acs/acs5)." },
                    "acs_year": { "type": "string", "description": "ACS vintage for the denominator (defaults to `year`)." },
                    "api_key": { "type": "string", "description": "Free Census API key; falls back to env CENSUS_API_KEY." }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description:
                        "All states, default trade codes, saturation normalized per household",
                    params: json!({ "year": DEFAULT_YEAR, "geo": "state" }),
                },
                ManifestExample {
                    description:
                        "County grain inside three states (a states filter is mandatory for county)",
                    params: json!({
                        "year": DEFAULT_YEAR,
                        "geo": "county",
                        "states": "06,48,12",
                        "denominator": "owner_occupied"
                    }),
                },
            ],
            output_shape: Some(
                "{source, geo, year, trades: [{naics, label, places_reported, \
                 total_establishments, total_employees, national_avg_wage, \
                 national_avg_establishment_size, top}], top_places_overall, \
                 top_places_by_saturation, normalization, market_blend, records, new, \
                 changed, unchanged} — suppressed cells are absent (Null), never zeroed",
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
        // A human-enabled `trades/taxonomy` registry trade is covered on the
        // next run with zero code change; when the registry dataset is
        // absent/empty the compile-time DEFAULT_TRADES behave exactly as before.
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
            None => match trades_common::taxonomy::registry_naics(&ctx, 6).await? {
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

        // Key: param → env. Census requires it (keyless 302 → missing_key.html).
        let api_key = census_common::api_key(&ctx, "census-density")?;

        if geo == "county" && (states.is_empty() || states == "*") {
            return Err(Error::App(
                "geo=county requires a `states` FIPS filter (e.g. \"06,12,48\") — \
                 CBP does not serve county:* across all states at once"
                    .into(),
            ));
        }

        // Provenance (M12) is per-request — one CBP URL and one archived
        // artifact per NAICS — so each trade's rows are upserted with their own
        // stamp and the run reports one merged rollup.
        let mut summary = pumper_core::UpsertSummary::default();
        let mut record_count = 0usize;
        let mut trade_summaries: Vec<Value> = Vec::new();
        // place label -> combined establishments across all trades (overall ranking).
        let mut overall: BTreeMap<String, i64> = BTreeMap::new();

        for (naics, label) in &trades {
            let url = build_url(&year, &geo, &states, naics, &naics_var, &api_key);
            let resp = ctx
                .engines
                .http
                .fetch(HttpRequest::get(url.clone()))
                .await?;
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
                Error::App(format!(
                    "Census CBP {year} NAICS {naics}: bad JSON rows: {e}"
                ))
            })?;
            // Bind the archived bytes once: `artifact_sha` must hash exactly what
            // was stored, never a re-serialization of it.
            let artifact = serde_json::to_vec_pretty(&rows)?;
            ctx.save_artifact(&format!("cbp-{naics}.json"), &artifact)
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

            let mut trade_records: Vec<(String, Value)> = Vec::new();
            let mut places_reported: u32 = 0;
            let mut total_estab: i64 = 0;
            let mut total_emp: i64 = 0;
            let mut total_pay: i64 = 0;
            let mut ranked: Vec<(String, i64)> = Vec::new();

            for row in rows.iter().skip(1) {
                let geo_code = row.get(i_geo).cloned().unwrap_or_default();
                let Some(estab) = census_common::census_num(row.get(i_estab)) else {
                    // Suppressed/jammed primary cell: not a genuinely reported
                    // place — skip rather than fabricate a 0-establishment row.
                    continue;
                };
                // Keep the Option so a *suppressed* cell (None) can be told apart
                // from a genuine 0 — a suppressed input must yield a Null derived
                // ratio, never a fabricated $0 wage.
                let emp_opt = i_emp.and_then(|i| census_common::census_num(row.get(i)));
                let pay_opt = i_pay.and_then(|i| census_common::census_num(row.get(i)));
                let emp = emp_opt.unwrap_or(0);
                let pay = pay_opt.unwrap_or(0);
                // PAYANN is in $1,000s (mirrors the solo side's receipts convention).
                let avg_annual_wage = match (pay_opt, emp_opt) {
                    (Some(p), Some(e)) if e > 0 => Value::from((p as f64 * 1000.0) / e as f64),
                    _ => Value::Null,
                };
                let avg_establishment_size = match (emp_opt, estab) {
                    (Some(e), s) if s > 0 => Value::from(e as f64 / s as f64),
                    _ => Value::Null,
                };

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
                    Some(c) => format!("{}·{}", census_common::state_abbr(&st_fips), c),
                    None => census_common::state_abbr(&st_fips).to_string(),
                };
                let key = match &county_fips {
                    Some(c) => format!("{naics}:{st_fips}{c}"),
                    None => format!("{naics}:{st_fips}"),
                };

                places_reported += 1;
                total_estab += estab;
                total_emp += emp;
                total_pay += pay;
                ranked.push((place.clone(), estab));
                *overall.entry(place.clone()).or_insert(0) += estab;

                trade_records.push((
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
                        "avg_annual_wage": avg_annual_wage,
                        "avg_establishment_size": avg_establishment_size,
                        "year": year,
                    }),
                ));
            }

            ranked.sort_by_key(|(_, e)| std::cmp::Reverse(*e));
            let top: Vec<Value> = ranked
                .iter()
                .take(5)
                .map(|(p, e)| json!({ "place": p, "establishments": e }))
                .collect();
            // National employer-side benchmarks: sum(pay)/sum(emp) and
            // sum(emp)/sum(estab) across reported places (the solo side reports the
            // analogous national receipts-per-operator). Suppressed cells contribute
            // 0 to the sums, same as the raw totals, so the ratio is over reported places.
            let national_avg_wage = if total_emp > 0 {
                Value::from((total_pay as f64 * 1000.0) / total_emp as f64)
            } else {
                Value::Null
            };
            let national_avg_establishment_size = if total_estab > 0 {
                Value::from(total_emp as f64 / total_estab as f64)
            } else {
                Value::Null
            };
            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "places_reported": places_reported,
                "total_establishments": total_estab,
                "total_employees": total_emp,
                "national_avg_wage": national_avg_wage,
                "national_avg_establishment_size": national_avg_establishment_size,
                "top": top,
            }));

            record_count += trade_records.len();
            census_common::merge_summary(
                &mut summary,
                ctx.upsert_many_with_provenance(
                    "establishments",
                    &trade_records,
                    census_common::http_provenance(&url, &artifact),
                )
                .await?,
            );
        }

        let mut overall_vec: Vec<(String, i64)> =
            overall.iter().map(|(k, v)| (k.clone(), *v)).collect();
        overall_vec.sort_by_key(|(_, e)| std::cmp::Reverse(*e));
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
                    let sat = SaturationWrite {
                        geo: &geo,
                        denom_kind: &denom_kind,
                        acs_dataset: &acs_dataset,
                        acs_year: &acs_year,
                        year: &year,
                    };
                    let sat_records = saturation_records(&rows, &sat);
                    let sat_sum = sync_saturation(&ctx, &sat_records).await?;
                    json!({
                        "dataset": format!("{MARKET_APP}/{SATURATION_DATASET}"),
                        "acs_dataset": acs_dataset,
                        "acs_year": acs_year,
                        "denominator": denom_kind,
                        "places_matched": matched,
                        "persisted": sat_records.len(),
                        "new": sat_sum.new.len(),
                        "changed": sat_sum.changed.len(),
                        "unchanged": sat_sum.unchanged,
                    })
                }
                Err(e) => json!({ "skipped": format!("{e}") }),
            }
        } else {
            json!({ "skipped": "normalize=false" })
        };

        // Blend the employer counts just upserted with census-nonemp's solo
        // counts into the shared `census/market_blend` dataset. Degrades
        // gracefully — a blend failure (or the other app never having run)
        // must not fail an otherwise-good CBP scrape.
        let market_blend = match sync_market_blend(&ctx).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        // `with_product_index` is what puts the two `census/*` products in the
        // worker's index + hook scope — without it no watch, trigger or saved
        // search on app `census` can fire, and neither product is searchable.
        Ok(census_common::with_product_index(json!({
            "source": format!("census/cbp/{year}"),
            "geo": geo,
            "year": year,
            "trades": trade_summaries,
            "top_places_overall": top_overall,
            "top_places_by_saturation": saturation,
            "normalization": normalization,
            "market_blend": market_blend,
            "records": record_count,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        })))
    }
}

// ---------------------------------------------------------------------------
// Blended employer + solo total-market view.
//
// census-density counts EMPLOYER businesses (CBP, 6-digit NAICS) and
// census-nonemp counts SOLO operators (Nonemployer Statistics, 4-digit NAICS —
// 6-digit is disclosure-suppressed). Neither alone is the market: a state can
// look "thin" on employer firms while teeming with one-person shops. The blend
// gives the TRUE total per trade group × state.
//
// Honest join grain: (4-digit NAICS prefix × state FIPS). NES is state-only and
// 4-digit-only, so CBP's 6-digit state rows are rolled UP to their 4-digit
// prefix (238220+238210 → 2382) and county rows are excluded — anything finer
// would fabricate solo counts we don't have. Vintages differ (CBP lags ~1y,
// NES ~2y), so each side's year is carried on the record instead of pretending
// they match.
//
// The result lives under the virtual shared app namespace `census` (the
// grants-common `grants/unified` pattern): all the Census apps re-derive it
// after their own upserts, so the blend stays fresh regardless of which run
// happens last.
//
// Two optional joins ride on the cell grain, each Null (never a fabricated
// zero) when its source app hasn't run — and each COARSER than the cell, which
// the labels say out loud:
//  - SUCCESSION (census-nesd `owner_age`): NES-D publishes per-state owner
//    demographics at 2-digit SECTOR grain only, so `pct_owners_55plus` is the
//    share across the reported age bands of the naics4's SECTOR (2382 → 23),
//    and `succession_receipts` = that sector share × the solo side's trade
//    receipts — a wave-size indicator in dollars, not a per-business or
//    per-trade prediction. Labeled `succession_grain: "naics_sector"`.
//  - FORMATION (census-bfs `formation_velocity`): the BFS API is US-NATIONAL
//    only (no state geography), so the inbound-competition block joined by the
//    naics4's 2-digit sector is the same NATIONAL signal on every state row —
//    carried under a `formation` object labeled
//    `grain: "naics_sector_national"` + `scope: "national"` so a national
//    sector-level signal can't silently read as state- or trade-level.
// ---------------------------------------------------------------------------

/// The virtual namespace and its two product datasets — defined in
/// `census-common` (every census app needs them to declare `index_datasets`)
/// and re-exported here, where the blend that writes them lives.
pub use census_common::{MARKET_APP, MARKET_BLEND_DATASET, SATURATION_DATASET};

/// Well over the worst case (4 trades × 52 states employer-side; NES is
/// smaller), while still bounding a runaway county-mode dataset read.
const BLEND_READ_LIMIT: i64 = 50_000;

/// Whether a dataset read came back **at** its cap — i.e. it is a WINDOW over
/// the dataset, not the dataset.
///
/// The blend joins five reads, each capped at [`BLEND_READ_LIMIT`]. A read that
/// returns exactly the cap has almost certainly left rows behind, and blending
/// it produces cells that look complete while missing whole states or trades:
/// an `employer_only` marker that means "the solo read was truncated", not "no
/// solo operators exist". `>=` rather than `==` because a cap can only be
/// tightened, never exceeded — an off-by-one must fail safe (cordis's
/// `aggregate_truncated` precedent).
fn read_hit_cap(rows_read: usize, limit: i64) -> bool {
    rows_read as i64 >= limit
}

/// The per-input read cap for this run: [`BLEND_READ_LIMIT`], or the
/// `blend_read_limit` param when an operator lowers it (a diagnostic knob —
/// lowering it does not make the blend cheaper to trust, it makes the
/// truncation report fire, which is the point). Clamped to at least 1: a cap of
/// 0 would read nothing and call it a complete corpus.
fn blend_read_limit(ctx: &AppContext) -> i64 {
    ctx.params
        .get("blend_read_limit")
        .and_then(Value::as_i64)
        .map(|n| n.max(1))
        .unwrap_or(BLEND_READ_LIMIT)
}

/// The run-level facts every saturation record carries: which geography and ACS
/// base the ranking was computed against.
pub struct SaturationWrite<'a> {
    pub geo: &'a str,
    pub denom_kind: &'a str,
    pub acs_dataset: &'a str,
    pub acs_year: &'a str,
    pub year: &'a str,
}

/// The FULL saturation ranking as dataset records — not just the top 60 the
/// result JSON shows, so the headline metric is queryable by the launch-ranking
/// UI, triggers and exports, and change-detection can see it move.
///
/// `rows` are `(place, combined establishments, base, per-10k)` as ranked.
pub fn saturation_records(
    rows: &[(String, i64, i64, f64)],
    w: &SaturationWrite<'_>,
) -> Vec<(String, Value)> {
    rows.iter()
        .map(|(p, e, base, per_10k)| {
            (
                p.clone(),
                json!({
                    "place": p,
                    "geo": w.geo,
                    "combined_establishments": e,
                    "base": base,
                    "denominator_kind": w.denom_kind,
                    "per_10k": (per_10k * 100.0).round() / 100.0,
                    "acs_dataset": w.acs_dataset,
                    "acs_year": w.acs_year,
                    "year": w.year,
                }),
            )
        })
        .collect()
}

/// Persists the saturation ranking into the virtual `census` namespace with a
/// real provenance stamp (the namespace bypasses `AppContext`'s automatic one).
pub async fn sync_saturation(
    ctx: &AppContext,
    records: &[(String, Value)],
) -> Result<pumper_core::UpsertSummary> {
    let prov = census_common::derived_provenance(ctx, SATURATION_DATASET, &SATURATION_INPUTS);
    ctx.datasets
        .upsert_many_stamped(MARKET_APP, SATURATION_DATASET, records, None, Some(&prov))
        .await
}

/// What a saturation row is derived from: this run's own CBP establishment
/// counts divided by an ACS base fetched in the same run.
const SATURATION_INPUTS: [&str; 2] = ["census-density/establishments", "census-acs/denominator"];

/// Reads both apps' live records, blends them, and upserts
/// `census/market_blend`. Returns a compact summary for the job result. If
/// either side has no data yet (the other app may never have run), reports
/// `blended: 0` with a note instead of writing half-truths.
pub async fn sync_market_blend(ctx: &AppContext) -> Result<Value> {
    let limit = blend_read_limit(ctx);
    // Truncation is measured on the RAW read (the cap is a SQL `LIMIT`), before
    // tombstones are filtered out in Rust — filtering first would hide a
    // capped read behind a smaller live count.
    let mut truncated: Vec<&str> = Vec::new();
    let live = |recs: Vec<pumper_core::Record>| -> Vec<Value> {
        recs.into_iter()
            .filter(|r| r.removed_at.is_none())
            .map(|r| r.data)
            .collect()
    };
    // The blend only ever uses state rows (the solo side has no county grain), so
    // filter `geo = state` in SQL — SQLite drops the county rows before they cross
    // the boundary and get JSON-parsed. Previously this read the ENTIRE
    // establishments dataset (up to 50k) and discarded county rows in Rust after
    // deserialization (~98% wasted on a nationwide county run), and the
    // `ORDER BY updated_at DESC LIMIT 50000` meant a large dataset could silently
    // return a recency window instead of the state rows the blend needs.
    let employers_raw = ctx
        .datasets
        .list_filtered(
            "census-density",
            "establishments",
            &[pumper_core::datasets::JsonFilter::Eq {
                path: "$.geo".into(),
                value: "state".into(),
            }],
            None,
            limit,
        )
        .await?;
    if read_hit_cap(employers_raw.len(), limit) {
        truncated.push("census-density/establishments");
    }
    let employers = live(employers_raw);
    let solos_raw = ctx
        .datasets
        .list("census-nonemp", "nonemployers", limit)
        .await?;
    if read_hit_cap(solos_raw.len(), limit) {
        truncated.push("census-nonemp/nonemployers");
    }
    let solos = live(solos_raw);
    if employers.is_empty() || solos.is_empty() {
        let missing = if employers.is_empty() {
            "census-density"
        } else {
            "census-nonemp"
        };
        return Ok(json!({
            "blended": 0,
            "note": format!("no live records from {missing} yet — run it to enable the blend"),
        }));
    }

    // Per-capita base per place (state), read from the persisted saturation
    // dataset — the blend itself does no ACS fetch (census-nonemp also calls this
    // path), so the denominator join reads the base census-density stored. Empty
    // when saturation hasn't run yet → cells emit null base (graceful).
    let bases_raw = ctx
        .datasets
        .list(MARKET_APP, SATURATION_DATASET, limit)
        .await?;
    if read_hit_cap(bases_raw.len(), limit) {
        truncated.push("census/saturation");
    }
    let bases = live(bases_raw);
    let base_by_place: BTreeMap<String, (i64, String)> = bases
        .iter()
        .filter_map(|r| {
            let place = r.get("place").and_then(Value::as_str)?.to_string();
            let base = r.get("base").and_then(Value::as_i64)?;
            let kind = r
                .get("denominator_kind")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Some((place, (base, kind)))
        })
        .collect();

    // Optional succession + formation inputs, read by app/dataset NAME (no
    // crate dependency — census-nesd/census-bfs depend on this crate for the
    // re-blend hook, so a reverse edge would cycle). Empty when those apps
    // haven't run → the blend emits Null fields (graceful).
    let owner_age_raw = ctx.datasets.list("census-nesd", "owner_age", limit).await?;
    if read_hit_cap(owner_age_raw.len(), limit) {
        truncated.push("census-nesd/owner_age");
    }
    let owner_age = live(owner_age_raw);
    let formation_velocity_raw = ctx
        .datasets
        .list("census-bfs", "formation_velocity", limit)
        .await?;
    if read_hit_cap(formation_velocity_raw.len(), limit) {
        truncated.push("census-bfs/formation_velocity");
    }
    let formation_velocity = live(formation_velocity_raw);

    let items = blend_market(
        &employers,
        &solos,
        &base_by_place,
        &owner_age,
        &formation_velocity,
    );
    let count = |cov: &str| items.iter().filter(|(_, v)| v["coverage"] == cov).count();
    let (both, employer_only, solo_only) =
        (count("both"), count("employer_only"), count("solo_only"));
    let with_succession = items
        .iter()
        .filter(|(_, v)| !v["pct_owners_55plus"].is_null())
        .count();
    let with_formation = items
        .iter()
        .filter(|(_, v)| !v["formation"].is_null())
        .count();
    // Stamped, not anonymous: these rows land in a namespace no app owns, so
    // `ctx.datasets` is called directly and the context's automatic provenance
    // never runs — see `census_common::derived_provenance`.
    let prov = census_common::derived_provenance(ctx, MARKET_BLEND_DATASET, &BLEND_INPUTS);
    let summary = ctx
        .datasets
        .upsert_many_stamped(MARKET_APP, MARKET_BLEND_DATASET, &items, None, Some(&prov))
        .await?;
    let mut out = json!({
        "dataset": format!("{MARKET_APP}/{MARKET_BLEND_DATASET}"),
        "blended": items.len(),
        "matched_both": both,
        "employer_only": employer_only,
        "solo_only": solo_only,
        "with_succession": with_succession,
        "with_formation": with_formation,
        // A blend over a capped read is a blend over a WINDOW: the coverage
        // markers and totals below describe what was read, not what exists.
        "inputs_truncated": truncated,
        "blend_complete": truncated.is_empty(),
        "new": summary.new.len(),
        "changed": summary.changed.len(),
        "unchanged": summary.unchanged,
    });
    if let (false, Value::Object(map)) = (truncated.is_empty(), &mut out) {
        map.insert(
            "warnings".into(),
            json!([format!(
                "blend inputs hit the {limit}-row read cap ({}) — the blended cells are computed \
                 over a WINDOW of those datasets, so coverage markers, totals and per-10k figures \
                 are PARTIAL for this run",
                truncated.join(", ")
            )]),
        );
    }
    Ok(out)
}

/// The datasets `sync_market_blend` derives from, in read order — the `inputs`
/// half of every blend row's provenance stamp.
const BLEND_INPUTS: [&str; 5] = [
    "census-density/establishments",
    "census-nonemp/nonemployers",
    "census/saturation",
    "census-nesd/owner_age",
    "census-bfs/formation_velocity",
];

/// Pure blend: employer state rows (6-digit NAICS, from `establishments`) +
/// solo state rows (4-digit NAICS, from `nonemployers`) → one record per
/// (4-digit NAICS group × state FIPS), keyed `{naics4}:{state_fips}`.
///
/// Employer county rows are skipped (NES has no county grain); a group present
/// on only one side is still emitted — with 0 on the missing side and a
/// `coverage` marker — so the dataset shows WHERE the blend is partial rather
/// than hiding it.
///
/// `owner_age` are census-nesd `owner_age` band records (2-digit SECTOR grain,
/// joined via the naics4's sector prefix; may be empty) and
/// `formation_velocity` census-bfs `formation_velocity` records (NATIONAL
/// sector grain — one per sector, no state; may be empty); each contributes
/// Null fields, never zeros, when absent for a cell.
pub fn blend_market(
    employers: &[Value],
    solos: &[Value],
    base_by_place: &BTreeMap<String, (i64, String)>,
    owner_age: &[Value],
    formation_velocity: &[Value],
) -> Vec<(String, Value)> {
    // (naics4, state_fips) → accumulating blend halves.
    #[derive(Default)]
    struct Cell {
        state: Option<String>,
        trade: Option<String>,
        employer_estab: Option<i64>,
        employer_naics: BTreeSet<String>,
        employer_year: Option<String>,
        solo_estab: Option<i64>,
        /// Present only when the solo side reported receipts — the succession
        /// dollar figure needs real receipts, not a defaulted 0.
        solo_receipts_thousands: Option<i64>,
        solo_year: Option<String>,
    }
    let str_field = |v: &Value, f: &str| v.get(f).and_then(Value::as_str).map(str::to_string);
    let num_field = |v: &Value, f: &str| v.get(f).and_then(Value::as_i64).unwrap_or(0);

    // SUCCESSION input: (2-digit sector, state_fips) → reported age bands +
    // vintage. NES-D is sector grain — records carry `sector` (e.g. "23").
    let mut age_bands: BTreeMap<(String, String), Vec<(String, i64)>> = BTreeMap::new();
    let mut age_year: BTreeMap<(String, String), String> = BTreeMap::new();
    for r in owner_age {
        let (Some(sector), Some(st)) = (str_field(r, "sector"), str_field(r, "state_fips")) else {
            continue;
        };
        let (Some(band), Some(owners)) = (
            str_field(r, "age_band"),
            r.get("owners").and_then(Value::as_i64),
        ) else {
            continue;
        };
        let key = (sector, st);
        if let Some(y) = str_field(r, "year") {
            age_year.entry(key.clone()).or_insert(y);
        }
        age_bands.entry(key).or_default().push((band, owners));
    }

    // FORMATION input: sector category → NATIONAL velocity record (the BFS API
    // has no state geography — one record per sector, keyed `US|{sector}`).
    let velocity_by_sector: BTreeMap<String, &Value> = formation_velocity
        .iter()
        .filter_map(|r| {
            let sector = r.get("sector").and_then(Value::as_str)?.to_string();
            Some((sector, r))
        })
        .collect();

    let mut cells: BTreeMap<(String, String), Cell> = BTreeMap::new();

    for e in employers {
        // Only state rows: the solo side has no county grain to join against.
        if e.get("geo").and_then(Value::as_str) != Some("state") {
            continue;
        }
        let (Some(naics), Some(st)) = (str_field(e, "naics"), str_field(e, "state_fips")) else {
            continue;
        };
        // 6-digit → 4-digit trade group (codes shorter than 4 pass through).
        let naics4: String = naics.chars().take(4).collect();
        let cell = cells.entry((naics4, st)).or_default();
        *cell.employer_estab.get_or_insert(0) += num_field(e, "establishments");
        cell.employer_naics.insert(naics);
        cell.employer_year = cell.employer_year.take().or_else(|| str_field(e, "year"));
        cell.state
            .get_or_insert_with(|| str_field(e, "place").unwrap_or_default());
    }

    for s in solos {
        let (Some(naics4), Some(st)) = (str_field(s, "naics"), str_field(s, "state_fips")) else {
            continue;
        };
        let cell = cells.entry((naics4, st)).or_default();
        *cell.solo_estab.get_or_insert(0) += num_field(s, "nonemployers");
        if let Some(rcpt) = s.get("receipts_thousands").and_then(Value::as_i64) {
            *cell.solo_receipts_thousands.get_or_insert(0) += rcpt;
        }
        cell.solo_year = cell.solo_year.take().or_else(|| str_field(s, "year"));
        if let Some(state) = str_field(s, "state") {
            cell.state.get_or_insert(state);
        }
        // The 4-digit group label lives on the solo side; keep it.
        if let Some(trade) = str_field(s, "trade") {
            cell.trade.get_or_insert(trade);
        }
    }

    cells
        .into_iter()
        .map(|((naics4, st_fips), c)| {
            let coverage = match (c.employer_estab.is_some(), c.solo_estab.is_some()) {
                (true, true) => "both",
                (true, false) => "employer_only",
                _ => "solo_only",
            };
            let employer = c.employer_estab.unwrap_or(0);
            let solo = c.solo_estab.unwrap_or(0);
            let total = employer + solo;
            let solo_share = if total > 0 {
                Value::from(((solo as f64 / total as f64) * 10_000.0).round() / 10_000.0)
            } else {
                Value::Null
            };
            // Per-capita market density: total (employer+solo) operators per 10k of
            // the state's ACS base — the number the launch ranking actually wants,
            // and which didn't exist on the blend before. Null when no base is
            // known for the place (saturation hasn't run) — never fabricated.
            let (base, denom_kind, total_market_per_10k) =
                match c.state.as_deref().and_then(|st| base_by_place.get(st)) {
                    Some((b, kind)) if *b > 0 => (
                        Value::from(*b),
                        Value::from(kind.clone()),
                        Value::from(
                            ((total as f64 / *b as f64) * 10_000.0 * 100.0).round() / 100.0,
                        ),
                    ),
                    _ => (Value::Null, Value::Null, Value::Null),
                };
            // SUCCESSION: 55+ owner share across reported NES-D bands of the
            // naics4's 2-digit SECTOR (NES-D's per-state grain — 2382 joins
            // through 23), and the wave in dollars against the solo side's
            // receipts. Nulls (never a fabricated 0%) when NES-D hasn't run /
            // is suppressed for the cell, and no dollar figure without real
            // receipts. Sector grain is coarser than the trade cell — labeled.
            let sector2: String = naics4.chars().take(2).collect();
            let sector_key = (sector2, st_fips.clone());
            let pct_55 = age_bands
                .get(&sector_key)
                .and_then(|bands| census_common::owner_age_share_55plus(bands));
            let pct_owners_55plus = pct_55
                .map(|p| Value::from((p * 10_000.0).round() / 10_000.0))
                .unwrap_or(Value::Null);
            let succession_grain = pct_55
                .map(|_| Value::from("naics_sector"))
                .unwrap_or(Value::Null);
            let owner_age_year = age_year
                .get(&sector_key)
                .map(|y| Value::from(y.clone()))
                .unwrap_or(Value::Null);
            let succession_receipts = match (pct_55, c.solo_receipts_thousands) {
                (Some(p), Some(rcpt)) => Value::from((p * rcpt as f64 * 1000.0).round() as i64),
                _ => Value::Null,
            };
            // FORMATION: NATIONAL sector-grain velocity joined by the naics4's
            // 2-digit sector — the BFS API serves no state geography, so a
            // state row's formation context is the national sector signal, and
            // the block's labels say so (grain + scope) so it can't read as
            // state- or trade-level data.
            let formation = census_common::bfs_sector_category(&naics4)
                .and_then(|cat| velocity_by_sector.get(&cat))
                .map(|v| {
                    json!({
                        "sector": v.get("sector").cloned().unwrap_or(Value::Null),
                        "t12m_applications":
                            v.get("t12m_applications").cloned().unwrap_or(Value::Null),
                        "yoy_delta_pct":
                            v.get("yoy_delta_pct").cloned().unwrap_or(Value::Null),
                        "accel_pct": v.get("accel_pct").cloned().unwrap_or(Value::Null),
                        "t12m_high_propensity":
                            v.get("t12m_high_propensity").cloned().unwrap_or(Value::Null),
                        "as_of_period":
                            v.get("as_of_period").cloned().unwrap_or(Value::Null),
                        "grain": "naics_sector_national",
                        "scope": "national",
                    })
                })
                .unwrap_or(Value::Null);
            let value = json!({
                "naics4": naics4,
                "trade": c.trade,
                "state": c.state,
                "state_fips": st_fips,
                "employer_establishments": employer,
                "employer_naics": c.employer_naics.into_iter().collect::<Vec<_>>(),
                "employer_year": c.employer_year,
                "solo_operators": solo,
                "solo_year": c.solo_year,
                "total_market": total,
                "solo_share": solo_share,
                "base": base,
                "denominator_kind": denom_kind,
                "total_market_per_10k": total_market_per_10k,
                "pct_owners_55plus": pct_owners_55plus,
                "succession_grain": succession_grain,
                "owner_age_year": owner_age_year,
                "succession_receipts": succession_receipts,
                "formation": formation,
                "coverage": coverage,
            });
            (format!("{naics4}:{st_fips}"), value)
        })
        .collect()
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
        Some(c) => format!("{}·{}", census_common::state_abbr(st_fips), c),
        None => census_common::state_abbr(st_fips).to_string(),
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
            let st = i_state
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or_default();
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
        let app = CensusDensity;
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
    /// `census/*` products are invisible — no per-record search doc, and (worker
    /// `run_indexed_apps`) no watch, trigger or saved search scoped to app
    /// `census` can EVER fire for this run.
    ///
    /// The needle is split so this assertion cannot match itself.
    #[test]
    fn run_result_declares_the_census_product_datasets() {
        let needle = concat!("census_common::with_product_index", "(json!(");
        assert_eq!(
            include_str!("lib.rs").matches(needle).count(),
            1,
            "census-density's run() must wrap its result exactly once with {needle}"
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

    /// A read that comes back AT the cap is a window, not the dataset — the
    /// anti-pattern is blending it as if it were complete (`>=`, never `==`, so
    /// an over-fetch fails safe too).
    #[test]
    fn a_capped_read_is_truncated_not_a_complete_corpus() {
        assert!(!read_hit_cap(0, 10));
        assert!(!read_hit_cap(9, 10));
        assert!(read_hit_cap(10, 10));
        assert!(read_hit_cap(11, 10));
        assert!(!read_hit_cap(49_999, BLEND_READ_LIMIT));
        assert!(read_hit_cap(50_000, BLEND_READ_LIMIT));
    }

    #[test]
    fn saturation_records_carry_the_run_grain_and_rounded_ratio() {
        let rows = vec![("CA".to_string(), 400i64, 10_000i64, 400.004_f64)];
        let recs = saturation_records(
            &rows,
            &SaturationWrite {
                geo: "state",
                denom_kind: "households",
                acs_dataset: "acs/acs5",
                acs_year: "2022",
                year: "2022",
            },
        );
        assert_eq!(recs.len(), 1);
        let (key, v) = &recs[0];
        assert_eq!(key, "CA");
        assert_eq!(v["combined_establishments"], 400);
        assert_eq!(v["base"], 10_000);
        assert_eq!(v["denominator_kind"], "households");
        assert_eq!(v["per_10k"], json!(400.0));
        assert_eq!(v["acs_year"], "2022");
    }

    fn emp(naics: &str, geo: &str, place: &str, st: &str, estab: i64) -> Value {
        json!({
            "naics": naics, "geo": geo, "place": place, "state_fips": st,
            "establishments": estab, "year": "2022",
        })
    }

    fn solo(naics4: &str, state: &str, st: &str, nonemp: i64) -> Value {
        json!({
            "naics": naics4, "trade": "Building equipment contractors",
            "state": state, "state_fips": st, "nonemployers": nonemp, "year": "2021",
        })
    }

    #[test]
    fn rolls_six_digit_employers_into_four_digit_group_and_joins_solo() {
        // 238220 + 238210 both belong to trade group 2382.
        let employers = vec![
            emp("238220", "state", "CA", "06", 100),
            emp("238210", "state", "CA", "06", 50),
        ];
        let solos = vec![solo("2382", "CA", "06", 300)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert_eq!(items.len(), 1);
        let (key, v) = &items[0];
        assert_eq!(key, "2382:06");
        assert_eq!(v["employer_establishments"], 150);
        assert_eq!(v["employer_naics"], json!(["238210", "238220"]));
        assert_eq!(v["solo_operators"], 300);
        assert_eq!(v["total_market"], 450);
        assert_eq!(v["solo_share"], json!(0.6667)); // 300/450 rounded to 4dp
        assert_eq!(v["coverage"], "both");
        assert_eq!(v["employer_year"], "2022");
        assert_eq!(v["solo_year"], "2021");
        assert_eq!(v["state"], "CA");
        assert_eq!(v["trade"], "Building equipment contractors");
    }

    #[test]
    fn county_employer_rows_are_excluded_from_the_state_grain_blend() {
        let employers = vec![
            emp("238220", "county", "CA·037", "06", 40),
            emp("238220", "state", "CA", "06", 100),
        ];
        let solos = vec![solo("2382", "CA", "06", 10)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1["employer_establishments"], 100);
    }

    #[test]
    fn one_sided_groups_are_emitted_with_coverage_markers() {
        let employers = vec![emp("561730", "state", "TX", "48", 80)];
        let solos = vec![solo("2382", "FL", "12", 25)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert_eq!(items.len(), 2);
        let by_key: BTreeMap<_, _> = items.into_iter().collect();
        let e = &by_key["5617:48"];
        assert_eq!(e["coverage"], "employer_only");
        assert_eq!(e["solo_operators"], 0);
        assert_eq!(e["total_market"], 80);
        assert_eq!(e["solo_share"], json!(0.0));
        let s = &by_key["2382:12"];
        assert_eq!(s["coverage"], "solo_only");
        assert_eq!(s["employer_establishments"], 0);
        assert_eq!(s["solo_share"], json!(1.0));
        assert_eq!(s["state"], "FL");
    }

    #[test]
    fn zero_totals_yield_null_share_not_a_division_artifact() {
        let employers = vec![emp("238220", "state", "AK", "02", 0)];
        let solos = vec![solo("2382", "AK", "02", 0)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert_eq!(items[0].1["solo_share"], Value::Null);
        assert_eq!(items[0].1["total_market"], 0);
    }

    #[test]
    fn per_capita_base_joins_by_place_or_stays_null() {
        let employers = vec![emp("238220", "state", "CA", "06", 100)];
        let solos = vec![solo("2382", "CA", "06", 300)];
        // Base known for CA (households = 10,000): 400 operators / 10k * 10k = 400.
        let mut bases = BTreeMap::new();
        bases.insert("CA".to_string(), (10_000i64, "households".to_string()));
        let items = blend_market(&employers, &solos, &bases, &[], &[]);
        let v = &items[0].1;
        assert_eq!(v["base"], 10_000);
        assert_eq!(v["denominator_kind"], "households");
        assert_eq!(v["total_market_per_10k"], json!(400.0));

        // No base for the place → nulls, never a fabricated number.
        let none = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert!(none[0].1["base"].is_null());
        assert!(none[0].1["total_market_per_10k"].is_null());
    }

    // NES-D owner-age records are 2-digit SECTOR grain (e.g. "23"), never 4-digit.
    fn band(sector: &str, st: &str, label: &str, owners: i64) -> Value {
        json!({
            "sector": sector, "state_fips": st, "age_band": label,
            "owners": owners, "year": "2021",
        })
    }

    #[test]
    fn succession_fields_join_owner_age_onto_the_cell() {
        let employers = vec![emp("238220", "state", "CA", "06", 100)];
        // Solo side WITH receipts (NRCPTOT convention: $1,000s).
        let solos = vec![json!({
            "naics": "2382", "trade": "Building equipment contractors",
            "state": "CA", "state_fips": "06", "nonemployers": 300,
            "receipts_thousands": 500, "year": "2021",
        })];
        let ages = vec![
            band("23", "06", "25 to 54", 60),
            band("23", "06", "55 to 64", 30),
            band("23", "06", "65 or over", 10),
        ];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &ages, &[]);
        let v = &items[0].1;
        // The 2382 cell joins its SECTOR's (23) bands — sector grain, labeled.
        assert_eq!(v["pct_owners_55plus"], json!(0.4));
        assert_eq!(v["succession_grain"], "naics_sector");
        assert_eq!(v["owner_age_year"], "2021");
        // 40% of $500k receipts = $200,000 succession wave.
        assert_eq!(v["succession_receipts"], 200_000);
    }

    #[test]
    fn no_owner_age_data_or_no_receipts_yields_nulls_not_zeros() {
        let employers = vec![emp("238220", "state", "CA", "06", 100)];
        // solo() helper has no receipts_thousands field.
        let solos = vec![solo("2382", "CA", "06", 300)];
        // No NES-D data at all → both succession fields Null (and no grain label).
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert!(items[0].1["pct_owners_55plus"].is_null());
        assert!(items[0].1["succession_grain"].is_null());
        assert!(items[0].1["succession_receipts"].is_null());
        // NES-D present but receipts unreported → share yes, dollars Null.
        let ages = vec![
            band("23", "06", "55 to 64", 1),
            band("23", "06", "25 to 54", 1),
        ];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &ages, &[]);
        assert_eq!(items[0].1["pct_owners_55plus"], json!(0.5));
        assert!(items[0].1["succession_receipts"].is_null());
    }

    // ── Store-backed: the virtual `census` namespace bypasses AppContext's own
    // stamping, so these two write paths are the ones that used to be anonymous.
    // Dead engines throughout — neither path fetches.

    async fn seeded_ctx(tag: &str) -> (pumper_core::testing::TempStore, AppContext) {
        seeded_ctx_with(tag, json!({})).await
    }

    async fn seeded_ctx_with(
        tag: &str,
        params: Value,
    ) -> (pumper_core::testing::TempStore, AppContext) {
        let store = pumper_core::testing::TempStore::new(tag).await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "census-density")
            .params(params)
            .build();
        ctx.datasets
            .upsert_many(
                "census-density",
                "establishments",
                &[(
                    "238220:06".to_string(),
                    emp("238220", "state", "CA", "06", 100),
                )],
            )
            .await
            .expect("seed employers");
        ctx.datasets
            .upsert_many(
                "census-nonemp",
                "nonemployers",
                &[("2382:06".to_string(), solo("2382", "CA", "06", 300))],
            )
            .await
            .expect("seed solos");
        (store, ctx)
    }

    /// The anti-pattern: a derived product whose every revision reads
    /// `Provenance::default()` — no producing job, no inputs, no as-of — so a
    /// `/provenance/census/market_blend/{key}` lookup answers "unknown" for a
    /// number the launch ranking is built on.
    #[tokio::test]
    async fn blended_revisions_carry_job_inputs_and_as_of_not_default_provenance() {
        let (_store, ctx) = seeded_ctx("census-blend-prov").await;
        let out = sync_market_blend(&ctx).await.expect("blend");
        assert_eq!(out["blended"], 1);

        let revs = ctx
            .datasets
            .history(MARKET_APP, MARKET_BLEND_DATASET, "2382:06", 10)
            .await
            .expect("history");
        let p = &revs.first().expect("one revision").provenance;
        assert!(!p.is_empty(), "blend revisions must not be anonymous");
        assert_eq!(p.job_id.as_deref(), Some(&*ctx.job_id.to_string()));
        let url = p.source_url.as_deref().expect("derived source_url");
        assert!(url.starts_with("derived://census/market_blend?"), "{url}");
        for input in BLEND_INPUTS {
            assert!(url.contains(input), "{url} must name input {input}");
        }
        assert!(url.contains("&as_of=20"), "{url} must carry an as-of");
        // A derived row has no archived body and no RuleSet — it must not claim
        // to be replayable.
        assert!(!p.replayable());
    }

    #[tokio::test]
    async fn saturation_revisions_carry_the_same_derived_stamp() {
        let store = pumper_core::testing::TempStore::new("census-sat-prov").await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "census-density").build();
        let recs = saturation_records(
            &[("CA".to_string(), 400, 10_000, 400.0)],
            &SaturationWrite {
                geo: "state",
                denom_kind: "households",
                acs_dataset: "acs/acs5",
                acs_year: "2022",
                year: "2022",
            },
        );
        let sum = sync_saturation(&ctx, &recs).await.expect("saturation");
        assert_eq!(sum.new.len(), 1);
        let revs = ctx
            .datasets
            .history(MARKET_APP, SATURATION_DATASET, "CA", 10)
            .await
            .expect("history");
        let p = &revs.first().expect("one revision").provenance;
        assert_eq!(p.job_id.as_deref(), Some(&*ctx.job_id.to_string()));
        assert!(p
            .source_url
            .as_deref()
            .expect("derived source_url")
            .starts_with("derived://census/saturation?"));
    }

    /// The anti-pattern: an input read that came back AT the cap is blended as
    /// if it were the whole corpus, so `employer_only` silently means "the solo
    /// read was truncated" and every total is partial with nothing saying so.
    #[tokio::test]
    async fn a_truncated_input_read_flags_the_blend_instead_of_blending_silently() {
        let (_store, ctx) = seeded_ctx("census-blend-trunc").await;
        let complete = sync_market_blend(&ctx).await.expect("blend");
        assert_eq!(complete["blend_complete"], true);
        assert_eq!(complete["inputs_truncated"], json!([]));
        assert!(complete.get("warnings").is_none());

        // Exactly at the cap (one seeded row per side, cap 1) — the boundary the
        // silent version got wrong: a full page is a WINDOW, not a corpus.
        let (_s2, capped) =
            seeded_ctx_with("census-blend-trunc-cap", json!({ "blend_read_limit": 1 })).await;
        let out = sync_market_blend(&capped).await.expect("blend");
        assert_eq!(out["blend_complete"], false);
        assert_eq!(
            out["inputs_truncated"],
            json!([
                "census-density/establishments",
                "census-nonemp/nonemployers"
            ]),
            "both at-cap reads must be named; the empty ones must not be"
        );
        let warning = out["warnings"][0].as_str().expect("a warning");
        assert!(
            warning.contains("read cap") && warning.contains("PARTIAL"),
            "{warning}"
        );
        // The blend still ran — a truncated read is reported, not fatal.
        assert_eq!(out["blended"], 1);
    }

    #[test]
    fn formation_block_joins_by_sector_and_keeps_its_national_grain_label() {
        let employers = vec![emp("238220", "state", "CA", "06", 100)];
        let solos = vec![solo("2382", "CA", "06", 300)];
        // BFS velocity is NATIONAL — one record per sector, no state fields.
        let velocity = vec![json!({
            "sector": "NAICS23", "geo": "US",
            "t12m_applications": 1320.0, "yoy_delta_pct": 10.0,
            "accel_pct": 0.0, "t12m_high_propensity": 400.0,
            "as_of_period": "2026-06", "grain": "naics_sector_national",
        })];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &velocity);
        let f = &items[0].1["formation"];
        assert_eq!(f["sector"], "NAICS23");
        assert_eq!(f["t12m_applications"], json!(1320.0));
        assert_eq!(f["yoy_delta_pct"], json!(10.0));
        assert_eq!(f["as_of_period"], "2026-06");
        // National sector-grain honesty travels with the block: a state row's
        // formation context is a NATIONAL signal and must say so.
        assert_eq!(f["grain"], "naics_sector_national");
        assert_eq!(f["scope"], "national");

        // A different sector's national velocity must not leak in.
        let velocity_other = vec![json!({ "sector": "NAICS72", "geo": "US" })];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &velocity_other);
        assert!(items[0].1["formation"].is_null());
    }
}
