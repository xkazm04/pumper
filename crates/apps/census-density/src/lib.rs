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

/// The CBP ingester — and, through the blend it owns, the publisher of the
/// three `census/*` products.
///
/// Carries the operator's `[census]` section (N35) because the atlas's scope
/// rail and its metered pricing driver are policy, not per-job params: a
/// scheduled run cannot carry them inline, and a key that binds nothing is the
/// bug `[research] max_watched_sources` documented. `Default` is the shipped
/// section, so an embedder constructing the app by hand behaves as before.
#[derive(Debug, Clone, Default)]
pub struct CensusDensity {
    census: pumper_core::config::CensusConfig,
}

impl CensusDensity {
    /// Construct with the operator's `[census]` section.
    pub fn with_config(census: &pumper_core::config::CensusConfig) -> Self {
        Self {
            census: census.clone(),
        }
    }
}

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
                    "allow_vintage_rewind": {
                        "type": "boolean",
                        "description": "Permit a run whose `year` is OLDER than the vintage this app already holds. Default false: these records are keyed without the year, so an older run overwrites current data and publishes the regression as a forward change (a `changed` revision, every watch/trigger on the dataset, a search re-index). Set true only when re-pointing the store at an older vintage is the intent."
                    },
                    "api_key": { "type": "string", "description": "Free Census API key; falls back to env CENSUS_API_KEY." },
                    "atlas_states_k": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "How many states the county atlas is scoped to (default [census] atlas_states_k = 10). The atlas never contains a county outside them."
                    },
                    "atlas_top_n": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Counties per trade kept by EACH of the atlas's two rankings (saturation, total_market_per_10k). Default [census] atlas_top_n = 25."
                    },
                    "atlas_metros": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "How many metros the pricing plan may name. Default [census] atlas_metros = 5."
                    },
                    "metro_pricing": {
                        "type": "boolean",
                        "description": "Ask the runtime to create the planned homewyse-pricing schedules. Default [census] metro_pricing = false — the plan is ALWAYS computed and reported; this turns it into metered spend."
                    },
                    "blend_read_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Diagnostic: lower the blend's per-input row cap so the truncation report fires."
                    }
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
                 national_avg_establishment_size, suppressed: {places_dropped, \
                 employees_cells, payroll_cells}, top} | {naics, label, note}], \
                 top_places_overall, top_places_by_saturation, normalization: \
                 {places_matched, places_excluded_no_denominator_row, \
                 places_excluded_base_not_positive, ...}, market_blend (carrying \n                 market_profile: the state x trade product this run republishes, and 
                 atlas: {states, state_rank_basis, counties_ranked, metro_pricing: 
                 {enabled, plan, requested, unmapped_counties}} - the county/metro 
                 launch atlas), \
                 suppression, empty_answers, index_datasets, records, new, changed, \
                 unchanged} — suppressed cells are absent (Null), never zeroed, and \
                 are counted; a trade the API publishes nothing for (HTTP 204) yields \
                 a `note` entry, not a failed run",
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

        // Vintage watermark, BEFORE any write: CBP records are keyed without the
        // year, so a run with an older `year` overwrites current data with older
        // data and publishes the regression as a forward change.
        let vintage = census_common::guard_vintage(&ctx, "establishments", &year).await?;

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
        // Per-trade ranked place->establishments, folded into the overall ranking
        // AFTER the loop by `overall_ranking`, which drops overlapping-grain NAICS
        // (a covering code plus a finer subset of it) so they are not summed twice.
        let mut trades_ranked: Vec<(String, Vec<(String, i64)>)> = Vec::new();
        // Run-level suppression telemetry: what the API declined to tell us.
        let mut empty_answers = 0usize;
        let mut suppression = Suppression::default();

        for (naics, label) in &trades {
            let url = build_url(&year, &geo, &states, naics, &naics_var, &api_key);
            let resp = ctx
                .engines
                .http
                .fetch(HttpRequest::get(url.clone()))
                .await?;
            // An empty answer (204, or a 200 with no body) is Census saying
            // "nothing published at this grain" for THIS trade — a note, never
            // the end of the run. Checked before `is_success`, which counts 204
            // as success and used to drop it into the "not JSON" error below.
            if census_common::is_empty_answer(resp.status, &resp.body) {
                trade_summaries.push(json!({
                    "naics": naics, "label": label,
                    "note": "no data — CBP figures suppressed or not published at this \
                             geography/NAICS grain",
                }));
                empty_answers += 1;
                continue;
            }
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
            let cols = CbpCols {
                estab: i_estab,
                geo: i_geo,
                state: idx("state"),
                emp: idx("EMP"),
                pay: idx("PAYANN"),
            };

            let CbpRollup {
                records: trade_records,
                mut ranked,
                places_reported,
                total_estab,
                total_emp,
                total_pay: _,
                suppressed,
                paired,
            } = map_cbp_rows(&rows, &cols, naics, label, &geo, &year);
            suppression.merge(&suppressed);
            trades_ranked.push((naics.clone(), ranked.clone()));

            ranked.sort_by_key(|(_, e)| std::cmp::Reverse(*e));
            let top: Vec<Value> = ranked
                .iter()
                .take(5)
                .map(|(p, e)| json!({ "place": p, "establishments": e }))
                .collect();
            // National employer-side benchmarks, each over the places that
            // reported BOTH halves of its own ratio — see [`PairedTotals`]. The
            // raw totals below are sums over whatever WAS reported and are not
            // interchangeable with these denominators.
            let (national_avg_wage, national_avg_establishment_size) = national_benchmarks(&paired);
            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "places_reported": places_reported,
                "total_establishments": total_estab,
                "total_employees": total_emp,
                "national_avg_wage": national_avg_wage,
                "national_avg_establishment_size": national_avg_establishment_size,
                "suppressed": suppressed.as_json(),
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

        let overall = overall_ranking(&trades_ranked);
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
                    let Normalized {
                        mut rows,
                        no_denominator_row,
                        base_not_positive,
                    } = normalize_places(&overall, &denom, &denom_kind);
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
                        // Places that HAVE establishment counts but no saturation
                        // figure, split by why. Both used to vanish silently, so
                        // a ranking over 12 of 52 states looked like the ranking.
                        "places_excluded_no_denominator_row": no_denominator_row,
                        "places_excluded_base_not_positive": base_not_positive,
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

        // The store now holds this vintage — move the watermark.
        census_common::record_vintage(&ctx, "establishments", &year).await?;

        // Blend the employer counts just upserted with census-nonemp's solo
        // counts into the shared `census/market_blend` dataset. Degrades
        // gracefully — a blend failure (or the other app never having run)
        // must not fail an otherwise-good CBP scrape.
        let atlas_settings = AtlasSettings::from_config(&self.census).with_params(&ctx);
        let market_blend = match sync_market_blend_with(&ctx, &atlas_settings).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        // `with_product_index` is what puts the two `census/*` products in the
        // worker's index + hook scope — without it no watch, trigger or saved
        // search on app `census` can fire, and neither product is searchable.
        Ok(with_market_index(census_common::with_product_index(
            json!({
                "source": format!("census/cbp/{year}"),
                "geo": geo,
                "year": year,
                "vintage": vintage,
                "trades": trade_summaries,
                "top_places_overall": top_overall,
                "top_places_by_saturation": saturation,
                "normalization": normalization,
                // What the API declined to tell us this run, so a shrinking corpus
                // reads as suppression rather than as a market that vanished.
                "suppression": suppression.as_json(),
                "empty_answers": empty_answers,
                "market_blend": market_blend,
                "records": record_count,
                "new": summary.new.len(),
                "changed": summary.changed.len(),
                "unchanged": summary.unchanged,
            }),
        )))
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

/// Pre-resolved CBP column indices (matched by NAME — the geography column
/// trails the requested `get=` vars, so position is never assumed).
pub struct CbpCols {
    pub estab: usize,
    pub geo: usize,
    pub state: Option<usize>,
    pub emp: Option<usize>,
    pub pay: Option<usize>,
}

/// What one request's payload declined to tell us. Counted rather than
/// discarded: "312 places reported" means something different when 40 more were
/// dropped for a suppressed ESTAB cell, and before this the difference was
/// invisible in every surface.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Suppression {
    /// Rows dropped entirely — the primary cell (ESTAB) was suppressed, so the
    /// place is not a reported place at all.
    pub places_dropped: usize,
    /// Reported rows whose EMP cell was suppressed (the row is kept; the
    /// derived ratios are Null).
    pub employees: usize,
    /// Reported rows whose PAYANN cell was suppressed.
    pub payroll: usize,
}

impl Suppression {
    pub fn merge(&mut self, other: &Suppression) {
        self.places_dropped += other.places_dropped;
        self.employees += other.employees;
        self.payroll += other.payroll;
    }

    pub fn as_json(&self) -> Value {
        json!({
            "places_dropped": self.places_dropped,
            "employees_cells": self.employees,
            "payroll_cells": self.payroll,
        })
    }
}

/// One request's parsed rollup: the dataset records, the ranking rows, the
/// totals the trade summary is built from, and what was suppressed.
pub struct CbpRollup {
    pub records: Vec<(String, Value)>,
    /// (place label, establishments) — also the per-place contribution to the
    /// overall cross-trade ranking.
    pub ranked: Vec<(String, i64)>,
    pub places_reported: u32,
    pub total_estab: i64,
    pub total_emp: i64,
    pub total_pay: i64,
    pub suppressed: Suppression,
    pub paired: PairedTotals,
}

/// The sums the two national benchmarks are computed over.
///
/// Both benchmarks are RATIOS, and a ratio is only a benchmark when its
/// numerator and denominator describe the **same places**. Suppression is
/// per-cell, so they do not: a state that reports `EMP` but has `PAYANN`
/// withheld used to add its employees to the denominator of
/// `national_avg_wage` while adding nothing to the numerator, pushing the
/// national wage down by however much of the country was suppressed that
/// vintage — a number that moves with Census's disclosure rules rather than
/// with the labour market. The mirror case (payroll reported, employees
/// withheld) pushed it up.
///
/// So each ratio carries its own pair of sums, accumulated only from the places
/// that reported everything that ratio needs.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PairedTotals {
    /// Employees of places that reported BOTH `EMP` and `PAYANN`.
    pub wage_emp: i64,
    /// Annual payroll ($1,000s) of those same places.
    pub wage_pay: i64,
    /// Employees of places that reported `EMP`.
    pub size_emp: i64,
    /// Establishments of those same places.
    pub size_estab: i64,
}

/// The two national employer-side benchmarks — `(avg annual wage, avg
/// establishment size)` — each over its own reported-both subset, `Null` when
/// nothing in the payload supports it.
pub fn national_benchmarks(p: &PairedTotals) -> (Value, Value) {
    let wage = if p.wage_emp > 0 {
        Value::from((p.wage_pay as f64 * 1000.0) / p.wage_emp as f64)
    } else {
        Value::Null
    };
    let size = if p.size_estab > 0 {
        Value::from(p.size_emp as f64 / p.size_estab as f64)
    } else {
        Value::Null
    };
    (wage, size)
}

/// Map the CBP array-of-arrays payload (row 0 = header, addressed by the
/// pre-resolved indices) into per-place records for one trade NAICS.
///
/// Suppression rules, all of them counted: a suppressed **ESTAB** drops the row
/// (it is not a reported place, and a 0-establishment row would be a
/// fabrication); a suppressed **EMP**/**PAYANN** keeps the row but leaves the
/// derived ratio `Null` — never a fabricated $0 wage.
pub fn map_cbp_rows(
    rows: &[Vec<String>],
    cols: &CbpCols,
    naics: &str,
    label: &str,
    geo: &str,
    year: &str,
) -> CbpRollup {
    let mut out = CbpRollup {
        records: Vec::new(),
        ranked: Vec::new(),
        places_reported: 0,
        total_estab: 0,
        total_emp: 0,
        total_pay: 0,
        suppressed: Suppression::default(),
        paired: PairedTotals::default(),
    };

    for row in rows.iter().skip(1) {
        let geo_code = row.get(cols.geo).cloned().unwrap_or_default();
        let Some(estab) = census_common::census_num(row.get(cols.estab)) else {
            // Suppressed/jammed primary cell: not a genuinely reported place —
            // skip rather than fabricate a 0-establishment row, and COUNT it.
            out.suppressed.places_dropped += 1;
            continue;
        };
        // Keep the Option so a *suppressed* cell (None) can be told apart from a
        // genuine 0 — a suppressed input must yield a Null derived ratio.
        let emp_opt = cols.emp.and_then(|i| census_common::census_num(row.get(i)));
        let pay_opt = cols.pay.and_then(|i| census_common::census_num(row.get(i)));
        if cols.emp.is_some() && emp_opt.is_none() {
            out.suppressed.employees += 1;
        }
        if cols.pay.is_some() && pay_opt.is_none() {
            out.suppressed.payroll += 1;
        }
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
            let st = cols
                .state
                .and_then(|i| row.get(i))
                .cloned()
                .unwrap_or_default();
            (st, Some(geo_code.clone()))
        } else {
            (geo_code.clone(), None)
        };
        let place = place_of(&st_fips, county_fips.as_deref());
        let key = match &county_fips {
            Some(c) => format!("{naics}:{st_fips}{c}"),
            None => format!("{naics}:{st_fips}"),
        };

        out.places_reported += 1;
        out.total_estab += estab;
        out.total_emp += emp_opt.unwrap_or(0);
        out.total_pay += pay_opt.unwrap_or(0);
        // The two national benchmarks are ratios, so each needs BOTH of its
        // halves from the SAME set of places — see [`PairedTotals`].
        if let Some(e) = emp_opt {
            out.paired.size_emp += e;
            out.paired.size_estab += estab;
            if let Some(p) = pay_opt {
                out.paired.wage_emp += e;
                out.paired.wage_pay += p;
            }
        }
        out.ranked.push((place.clone(), estab));

        out.records.push((
            key,
            json!({
                "naics": naics,
                "trade": label,
                "geo": geo,
                "place": place,
                "state_fips": st_fips,
                "county_fips": county_fips,
                "establishments": estab,
                // Null, not 0: a withheld EMP/PAYANN cell is unknown, and a
                // fabricated zero here is the same lie the solo side's
                // `receipts_thousands` used to tell (it read as a state whose
                // plumbers employ nobody and pay nothing).
                "employees": emp_opt.map(Value::from).unwrap_or(Value::Null),
                "annual_payroll_thousands": pay_opt.map(Value::from).unwrap_or(Value::Null),
                "avg_annual_wage": avg_annual_wage,
                "avg_establishment_size": avg_establishment_size,
                "year": year,
            }),
        ));
    }

    out
}

/// The saturation ranking plus the places that could NOT be ranked, by reason.
pub struct Normalized {
    /// (place, combined establishments, base, per-10k).
    pub rows: Vec<(String, i64, i64, f64)>,
    /// Places with establishments but no ACS row at all (a geography the
    /// denominator query didn't cover).
    pub no_denominator_row: usize,
    /// Places whose chosen base is 0 or negative (an ACS jam value, or a
    /// genuinely empty base) — dividing would fabricate an infinity.
    pub base_not_positive: usize,
}

/// Fold each trade's ranked place->establishments into one cross-trade overall
/// ranking, dropping overlapping-grain NAICS so they are not double-counted.
///
/// When the requested trades span overlapping grains — a covering code and a
/// finer subset of it (e.g. sector `"23"` and its subgroup `"2382"`, or `"2382"`
/// and its 6-digit component `"238220"`) — the CBP request for the covering code
/// ALREADY contains the subset's establishments, so summing both inflates every
/// place. This mirrors the blend's guard exactly: reuse
/// [`census_common::covering_naics`] (keep the covering code, drop the covered)
/// and only fold the kept codes into the overall map.
fn overall_ranking(trades_ranked: &[(String, Vec<(String, i64)>)]) -> BTreeMap<String, i64> {
    let contributing: BTreeSet<String> = trades_ranked.iter().map(|(n, _)| n.clone()).collect();
    let (counted, _dropped) = census_common::covering_naics(&contributing);
    let counted: BTreeSet<String> = counted.into_iter().collect();
    let mut overall: BTreeMap<String, i64> = BTreeMap::new();
    for (naics, ranked) in trades_ranked {
        if !counted.contains(naics) {
            continue;
        }
        for (place, estab) in ranked {
            *overall.entry(place.clone()).or_insert(0) += *estab;
        }
    }
    overall
}

/// Join establishment counts to an ACS base and rank by establishments per 10k.
///
/// Extracted from the `filter_map` that used to do this inline, because a
/// `return None` there was a **silent drop**: a place with no ACS row and a
/// place whose base is 0 both simply disappeared from the ranking, so
/// `places_matched` was the only number reported and there was nothing to
/// compare it against.
pub fn normalize_places(
    overall: &BTreeMap<String, i64>,
    denom: &BTreeMap<String, Denom>,
    denom_kind: &str,
) -> Normalized {
    let mut out = Normalized {
        rows: Vec::new(),
        no_denominator_row: 0,
        base_not_positive: 0,
    };
    for (place, estab) in overall {
        let Some(d) = denom.get(place) else {
            out.no_denominator_row += 1;
            continue;
        };
        let base = match denom_kind {
            "population" => d.population,
            "owner_occupied" => d.owner_occupied,
            _ => d.households,
        };
        if base <= 0 {
            out.base_not_positive += 1;
            continue;
        }
        let per_10k = (*estab as f64) / (base as f64) * 10_000.0;
        out.rows.push((place.clone(), *estab, base, per_10k));
    }
    out
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

/// The dimensions a saturation key carries, stamped on every record so a reader
/// can tell a current row from a legacy `{place}`-keyed one.
pub const SATURATION_KEY_GRAIN: &str = "geo|denominator_kind|place";

/// A saturation record's key: `{geo}|{denominator_kind}|{place}`.
///
/// The key used to be the bare place, which made the record's OWN dimensions
/// invisible to the store: a `denominator=population` run rewrote the
/// `denominator=households` ranking under the same keys, and change detection
/// reported the substitution as an ordinary movement in the numbers — every
/// state "changed", for a re-parameterisation, not a market shift. (State and
/// county runs did not in fact collide, since `place_of` already distinguishes
/// `CA` from `CA·037` — but nothing in the key SAID so, and a future geography
/// whose label is not place-unique would have collided silently.)
///
/// MIGRATION: legacy `{place}`-keyed rows are not rewritten and cannot be
/// tombstoned from here — `detect_removed` needs a `RemovalGuard` only
/// `AppContext::sync_many` can mint, and that is scoped to the app's OWN
/// namespace, which `census` is not. They linger until an operator removes them
/// (`DELETE /datasets/census/saturation/records/{place}`). They cannot corrupt
/// the blend: [`blend_market`]'s base join takes the most recently updated row
/// per place, and every run rewrites the new-keyed rows.
pub fn saturation_key(geo: &str, denom_kind: &str, place: &str) -> String {
    format!("{geo}|{denom_kind}|{place}")
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
                saturation_key(w.geo, w.denom_kind, p),
                json!({
                    "place": p,
                    "geo": w.geo,
                    "key_grain": SATURATION_KEY_GRAIN,
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

/// Adds the cross-FAMILY `market/profile` spec (N33) to a result that
/// `census_common::with_product_index` has already stamped, so the `market`
/// namespace enters this run's `indexed_apps` too — without it a watch, trigger,
/// saved search or contract evaluation on the product cannot fire for a
/// census-driven refresh, however often the run rewrites it.
///
/// **Why here and not in `census_common::product_index_datasets`,** which is
/// where it belongs: that helper is shared by all four census apps, and three of
/// them (`census-nonemp`, `census-nesd`, `census-bfs`) pin its exact two-element
/// output in their own tests. Adding the spec there is a one-line change plus
/// three test updates in crates this change may not touch — reported as a seam,
/// not made. Until then the declaration rides the app that OWNS the blend, and
/// the other three publish the profile without indexing it.
fn with_market_index(mut result: Value) -> Value {
    if let Some(specs) = result
        .get_mut("index_datasets")
        .and_then(Value::as_array_mut)
    {
        specs.push(trades_common::market::product_index_spec());
        // N35: the county atlas, for the same reason and by the same route —
        // `census_common::product_index_datasets` is pinned by three sibling
        // crates' tests, so the third `census/*` product is declared here, on
        // the app that owns it.
        specs.push(json!({ "app": MARKET_APP, "dataset": ATLAS_DATASET }));
    }
    result
}

/// What a saturation row is derived from: this run's own CBP establishment
/// counts divided by an ACS base fetched in the same run.
const SATURATION_INPUTS: [&str; 2] = ["census-density/establishments", "census-acs/denominator"];

/// Reads both apps' live records, blends them, and upserts
/// `census/market_blend`. Returns a compact summary for the job result. If
/// either side has no data yet (the other app may never have run), reports
/// `blended: 0` with a note instead of writing half-truths.
pub async fn sync_market_blend(ctx: &AppContext) -> Result<Value> {
    // The three sibling census apps re-derive the blend through this signature
    // and hold no `[census]` section of their own, so they get the shipped
    // defaults plus whatever their job params say. `census-density` — the app
    // that OWNS the atlas and the county runs — calls the `_with` form with the
    // operator's section.
    sync_market_blend_with(ctx, &AtlasSettings::default().with_params(ctx)).await
}

/// [`sync_market_blend`] with the atlas rails supplied by the caller.
pub async fn sync_market_blend_with(ctx: &AppContext, atlas: &AtlasSettings) -> Result<Value> {
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
    let mut employers = live(employers_raw);
    // N35: county rows are read SEPARATELY rather than by dropping the geo
    // filter, so each grain gets the whole cap instead of competing for one
    // recency window — a nationwide county run is ~3,000 counties x N trades and
    // would otherwise push every state row out of a shared 50k read. The
    // truncation report names the grain, because a capped county read and a
    // capped state read make different cells wrong.
    let counties_raw = ctx
        .datasets
        .list_filtered(
            "census-density",
            "establishments",
            &[pumper_core::datasets::JsonFilter::Eq {
                path: "$.geo".into(),
                value: "county".into(),
            }],
            None,
            limit,
        )
        .await?;
    if read_hit_cap(counties_raw.len(), limit) {
        truncated.push("census-density/establishments@county");
    }
    employers.extend(live(counties_raw));
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
    let base_by_place = base_index(&bases);

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
    // LAST WRITER PUBLISHES (N33): the cross-family `market/profile` (state x
    // trade) is rebuilt at the end of this blend AND at the end of the trades
    // join, so whichever family refreshed last republishes the product and it
    // is never a cycle behind either half. The join itself lives in
    // `trades_common::market` — this crate already depends on that library for
    // the trade taxonomy, and the direction stays one-way.
    //
    // Reported, never fatal: the blend rows above are already written, and a
    // downstream join's failure must not turn a successful refresh into a
    // failed run.
    let profile = match trades_common::market::sync_market_profile(ctx).await {
        Ok(v) => v,
        Err(e) => json!({ "profiled": 0, "error": e.to_string() }),
    };
    if let Value::Object(map) = &mut out {
        map.insert("market_profile".into(), profile);
    }
    // N35: the county/metro atlas over the cells just written. Built from the
    // in-memory `items`, never a re-read, so the atlas can never describe a
    // different blend than the one this run published. Reported, never fatal —
    // same rule as the profile above.
    let atlas_block = match sync_atlas(ctx, &items, &bases, atlas).await {
        Ok(v) => v,
        Err(e) => json!({ "counties_ranked": 0, "error": e.to_string() }),
    };
    if let Value::Object(map) = &mut out {
        map.insert("atlas".into(), atlas_block);
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

/// One place's per-capita base, as the blend reads it back out of `saturation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaceBase {
    pub base: i64,
    pub denominator_kind: String,
    /// ACS vintage the base came from — carried onto the blend row's `vintages`
    /// block. `None` on a legacy row written before the field existed.
    pub acs_year: Option<String>,
}

/// place → base for the blend's per-capita join.
///
/// `saturation` holds one row per (geo, denominator, place), so a place can
/// appear several times — with DIFFERENT bases. Three rules make the pick
/// deterministic instead of "whichever the map iterator wrote last":
///  - **both grains are indexed** (N35): the blend now emits county cells as
///    well as state ones, and a county cell's per-10k needs the county's ACS
///    base. Place labels are grain-distinct by construction (`place_of` writes
///    `CA` and `CA·037`), so one map holds both without collision;
///  - **state rows are indexed first**, so a county row can never supply a
///    STATE cell's base even if some future row mislabels its place — the
///    guarantee the state-only filter used to provide;
///  - **first wins** within a pass, and `Datasets::list` returns
///    `updated_at DESC`, so the most recently written denominator is the one in
///    force. That is also what keeps a legacy `{place}`-keyed row from
///    shadowing a current one.
pub fn base_index(bases: &[Value]) -> BTreeMap<String, PlaceBase> {
    let mut out: BTreeMap<String, PlaceBase> = BTreeMap::new();
    // Legacy rows predate `geo`; treat a missing one as state (the only grain
    // that existed then) rather than dropping it.
    let grain = |r: &Value| {
        r.get("geo")
            .and_then(Value::as_str)
            .unwrap_or("state")
            .to_string()
    };
    for pass in ["state", "county"] {
        for r in bases {
            if grain(r) != pass {
                continue;
            }
            let (Some(place), Some(base)) = (
                r.get("place").and_then(Value::as_str),
                r.get("base").and_then(Value::as_i64),
            ) else {
                continue;
            };
            out.entry(place.to_string()).or_insert(PlaceBase {
                base,
                denominator_kind: r
                    .get("denominator_kind")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                acs_year: r
                    .get("acs_year")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            });
        }
    }
    out
}

/// Pure blend: employer rows (6-digit NAICS, from `establishments`) + solo rows
/// (4-digit NAICS, from `nonemployers`) → one record per (4-digit NAICS group ×
/// GEOGRAPHY), keyed `{naics4}:{geo_fips}` — a 2-digit state FIPS or a 5-digit
/// county FIPS.
///
/// **The geo dimension (N35).** County employer rows used to be dropped here
/// ("the solo side has no county grain"), which made a county launch ranking
/// impossible to build from the product. They now form their own cells, and
/// each cell says what its solo half actually is:
///  - `solo_grain: "state"` — a state cell, both halves at the cell's grain;
///  - `solo_grain: "county"` — NES served county rows for this cell (the probe
///    in `census-nonemp` answered `served`) and they are joined at county;
///  - `solo_grain: "state_carried"` — NES published nothing at county grain, so
///    `solo_operators` is **Null** and `total_market` counts employers only.
///    The state's solo total rides along as `solo_state_operators`, labelled,
///    as CONTEXT — apportioning it across counties would fabricate exactly the
///    per-county number this whole product refuses to invent.
///
/// A county cell carries `state_fips: null` and names its state in
/// `parent_state_fips` on purpose: `trades_common::market::build_profiles`
/// indexes blend cells by `{naics4}:{state_fips}` off the record's own fields,
/// so a county cell that filled `state_fips` in would collide with — and
/// silently replace — the state cell backing every `market/profile` row.
/// `market/profile` is state × trade and stays that way.
///
/// A group present on only one side is still emitted — with 0 on the missing
/// side and a `coverage` marker — so the dataset shows WHERE the blend is
/// partial rather than hiding it.
///
/// `owner_age` are census-nesd `owner_age` band records (2-digit SECTOR grain,
/// joined via the naics4's sector prefix; may be empty) and
/// `formation_velocity` census-bfs `formation_velocity` records (NATIONAL
/// sector grain — one per sector, no state; may be empty); each contributes
/// Null fields, never zeros, when absent for a cell.
pub fn blend_market(
    employers: &[Value],
    solos: &[Value],
    base_by_place: &BTreeMap<String, PlaceBase>,
    owner_age: &[Value],
    formation_velocity: &[Value],
) -> Vec<(String, Value)> {
    // (naics4, geo_fips) → accumulating blend halves.
    #[derive(Default)]
    struct Cell {
        /// The place LABEL (`CA` / `CA·037`) — the key the per-capita base and
        /// the saturation ranking are both indexed by.
        state: Option<String>,
        /// `state` | `county`.
        geo: String,
        /// The state this cell is in, whatever its grain.
        parent_state_fips: String,
        /// `Some` only on a county cell.
        county_fips: Option<String>,
        trade: Option<String>,
        /// Per-CONTRIBUTING-CODE establishment counts, resolved to a single sum
        /// only at emit time — see `census_common::covering_naics`. Summing as
        /// we go was the double-count bug: a registry listing both `2382` and
        /// `238220` produced two stored records that both roll up into cell
        /// `2382`, i.e. the aggregate plus a part of itself.
        employer_by_naics: BTreeMap<String, i64>,
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

    // The GEOGRAPHY a source row belongs to: `(geo, parent state FIPS, county
    // FIPS, geo FIPS)`. `None` for a row whose grain says county but which
    // carries no county code — placing it would be a guess.
    let geo_of = |v: &Value| -> Option<(String, String, Option<String>, String)> {
        // Rows written before the geo dimension existed are state rows: that is
        // the only grain either app ever wrote.
        let geo = v
            .get("geo")
            .and_then(Value::as_str)
            .unwrap_or("state")
            .to_string();
        let st = str_field(v, "state_fips")?;
        if geo == "county" {
            let county = str_field(v, "county_fips")?;
            let fips5 = census_common::county_fips5(&st, &county);
            Some((geo, st, Some(county), fips5))
        } else {
            let fips = st.clone();
            Some((geo, st, None, fips))
        }
    };
    let stamp_geo = |cell: &mut Cell, geo: &str, st: &str, county: &Option<String>| {
        if cell.geo.is_empty() {
            cell.geo = geo.to_string();
            cell.parent_state_fips = st.to_string();
            cell.county_fips = county.clone();
        }
    };

    // The state's solo total per (naics4, state FIPS) — the figure a
    // `state_carried` county cell reports as CONTEXT beside its Null solo count.
    let mut solo_by_state: BTreeMap<(String, String), i64> = BTreeMap::new();
    for s in solos {
        if s.get("geo").and_then(Value::as_str).unwrap_or("state") != "state" {
            continue;
        }
        let (Some(naics4), Some(st)) = (str_field(s, "naics"), str_field(s, "state_fips")) else {
            continue;
        };
        *solo_by_state.entry((naics4, st)).or_insert(0) += num_field(s, "nonemployers");
    }

    for e in employers {
        let (Some(naics), Some((geo, st, county, geo_fips))) = (str_field(e, "naics"), geo_of(e))
        else {
            continue;
        };
        // 6-digit → 4-digit trade group (codes shorter than 4 pass through).
        let naics4: String = naics.chars().take(4).collect();
        let cell = cells.entry((naics4, geo_fips)).or_default();
        stamp_geo(cell, &geo, &st, &county);
        *cell.employer_by_naics.entry(naics).or_insert(0) += num_field(e, "establishments");
        cell.employer_year = cell.employer_year.take().or_else(|| str_field(e, "year"));
        cell.state
            .get_or_insert_with(|| str_field(e, "place").unwrap_or_default());
    }

    for s in solos {
        let (Some(naics4), Some((geo, st, county, geo_fips))) = (str_field(s, "naics"), geo_of(s))
        else {
            continue;
        };
        let cell = cells.entry((naics4, geo_fips)).or_default();
        stamp_geo(cell, &geo, &st, &county);
        *cell.solo_estab.get_or_insert(0) += num_field(s, "nonemployers");
        if let Some(rcpt) = s.get("receipts_thousands").and_then(Value::as_i64) {
            *cell.solo_receipts_thousands.get_or_insert(0) += rcpt;
        }
        cell.solo_year = cell.solo_year.take().or_else(|| str_field(s, "year"));
        // The place label: `place` on a county row, the state abbreviation on a
        // state row (which is what `state` has always held).
        if let Some(place) = str_field(s, "place").or_else(|| str_field(s, "state")) {
            cell.state.get_or_insert(place);
        }
        // The 4-digit group label lives on the solo side; keep it.
        if let Some(trade) = str_field(s, "trade") {
            cell.trade.get_or_insert(trade);
        }
    }

    cells
        .into_iter()
        .map(|((naics4, geo_fips), c)| {
            let is_county = c.geo == "county";
            // The state this cell sits in — the join key for every input that
            // is published per state (NES-D bands, the state's solo total).
            let st_fips = c.parent_state_fips.clone();
            // WHAT THE SOLO HALF OF THIS CELL IS. The one label a sub-state
            // consumer has to read before comparing two cells.
            let solo_grain = match (is_county, c.solo_estab.is_some()) {
                (false, _) => "state",
                (true, true) => "county",
                (true, false) => "state_carried",
            };
            // Mixed-grain resolution BEFORE the sum: keep the covering
            // aggregate, drop the components it already contains.
            let contributing: BTreeSet<String> = c.employer_by_naics.keys().cloned().collect();
            let (counted_naics, dropped_naics) = census_common::covering_naics(&contributing);
            let employer_estab: Option<i64> = (!counted_naics.is_empty()).then(|| {
                counted_naics
                    .iter()
                    .filter_map(|n| c.employer_by_naics.get(n))
                    .sum()
            });
            let coverage = match (employer_estab.is_some(), c.solo_estab.is_some()) {
                (true, true) => "both",
                (true, false) => "employer_only",
                _ => "solo_only",
            };
            let employer = employer_estab.unwrap_or(0);
            let solo = c.solo_estab.unwrap_or(0);
            // A `state_carried` cell has an UNKNOWN solo half, not an empty
            // one: `0` here would read as "no solo operators in this county",
            // which is a claim nobody published. State cells keep their shipped
            // behaviour exactly (a missing side is 0 with a coverage marker).
            let solo_known = solo_grain != "state_carried";
            let total = employer + if solo_known { solo } else { 0 };
            let solo_share = if solo_known && total > 0 {
                Value::from(((solo as f64 / total as f64) * 10_000.0).round() / 10_000.0)
            } else {
                Value::Null
            };
            // The state's solo total, as CONTEXT on a state-carried county cell
            // — never apportioned, never added into `total_market`.
            let solo_state_operators = if solo_grain == "state_carried" {
                solo_by_state
                    .get(&(naics4.clone(), st_fips.clone()))
                    .map(|n| Value::from(*n))
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            // Per-capita market density: total (employer+solo) operators per 10k of
            // the state's ACS base — the number the launch ranking actually wants,
            // and which didn't exist on the blend before. Null when no base is
            // known for the place (saturation hasn't run) — never fabricated.
            //
            // COVERAGE CAVEAT, machine-readable: the numerator is whatever the
            // cell actually has. On an `employer_only` cell it counts employer
            // firms alone and on a `solo_only` cell solo operators alone, so
            // comparing two places' per-10k figures without reading the basis
            // compares a total market against half of one. The value and the
            // basis are emitted together — a consumer that reads one sees the
            // other.
            let place_base = c.state.as_deref().and_then(|st| base_by_place.get(st));
            let (base, denom_kind, total_market_per_10k, per_10k_basis) = match place_base {
                Some(b) if b.base > 0 => (
                    Value::from(b.base),
                    Value::from(b.denominator_kind.clone()),
                    Value::from(
                        ((total as f64 / b.base as f64) * 10_000.0 * 100.0).round() / 100.0,
                    ),
                    Value::from(per_10k_basis(coverage, solo_grain)),
                ),
                _ => (Value::Null, Value::Null, Value::Null, Value::Null),
            };
            let base_acs_year = place_base
                .and_then(|b| b.acs_year.clone())
                .map(Value::from)
                .unwrap_or(Value::Null);
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
            // The four input vintages, read off the values already computed so
            // the block cannot drift from the fields it summarizes.
            let employer_vintage = c
                .employer_year
                .clone()
                .map(Value::from)
                .unwrap_or(Value::Null);
            let solo_vintage = c.solo_year.clone().map(Value::from).unwrap_or(Value::Null);
            let owner_age_vintage = owner_age_year.clone();
            let formation_as_of = formation
                .get("as_of_period")
                .cloned()
                .unwrap_or(Value::Null);
            let value = json!({
                "naics4": naics4,
                "trade": c.trade,
                "state": c.state,
                // NULL ON A COUNTY CELL, on purpose: `market/profile`'s join
                // indexes blend cells by `{naics4}:{state_fips}` off these very
                // fields, and a county cell that filled this in would replace
                // the state cell behind every state x trade profile row. The
                // county cell names its state in `parent_state_fips` instead.
                "state_fips": if is_county { Value::Null } else { Value::from(st_fips.clone()) },
                "geo": c.geo,
                "geo_fips": geo_fips,
                "parent_state_fips": st_fips,
                "county_fips": c.county_fips,
                // The grain of each half, said out loud. `employer_grain` is
                // always the cell's own grain (CBP publishes both); the solo
                // side is the one that can be coarser than the cell.
                "employer_grain": c.geo,
                "solo_grain": solo_grain,
                "solo_state_operators": solo_state_operators,
                "employer_establishments": employer,
                "employer_naics": counted_naics,
                // Codes present in the store but NOT counted, because a coarser
                // code in the same cell already contains them. Empty in the
                // normal single-grain case; non-empty means the taxonomy is
                // mixed-grain and the correction is on the record, not silent.
                "employer_naics_covered": dropped_naics,
                "employer_year": c.employer_year,
                // Null (never 0) when the solo half is state-carried: nobody
                // published a solo count for this county.
                "solo_operators": if solo_known { Value::from(solo) } else { Value::Null },
                "solo_year": c.solo_year,
                "total_market": total,
                "solo_share": solo_share,
                "base": base,
                "denominator_kind": denom_kind,
                "total_market_per_10k": total_market_per_10k,
                "total_market_per_10k_basis": per_10k_basis,
                "pct_owners_55plus": pct_owners_55plus,
                "succession_grain": succession_grain,
                "owner_age_year": owner_age_year,
                "succession_receipts": succession_receipts,
                "formation": formation,
                "coverage": coverage,
                // WHAT THIS ROW IS MADE OF, by vintage. The blend is re-derived
                // by four apps — weekly, once BFS runs — so `updated_at` moves
                // constantly while the market data underneath is 2021/2022
                // stock. Without this block a consumer reading freshness off the
                // envelope concludes the numbers are current; they are not, and
                // now the record says which year each input came from.
                //
                // Deliberately NO derivation timestamp: it would land in the
                // change-detection hash and mark every row `changed` on every
                // re-derive. The as-of of the derivation lives on the revision's
                // provenance stamp instead (`census_common::derived_provenance`).
                "vintages": {
                    "employer_cbp_year": employer_vintage,
                    "solo_nes_year": solo_vintage,
                    "owner_age_nesd_year": owner_age_vintage,
                    "formation_bfs_as_of": formation_as_of,
                    "base_acs_year": base_acs_year,
                },
            });
            // `{naics4}:{geo_fips}` — a 2-digit state FIPS or a 5-digit county
            // FIPS. State keys are byte-identical to the ones shipped before
            // the geo dimension, so no state row is rewritten by this change.
            (format!("{naics4}:{geo_fips}"), value)
        })
        .collect()
}

/// What a cell's `total_market_per_10k` actually counted, from its coverage.
///
/// The ratio is `total / base`, and `total` is only a TOTAL market on a `both`
/// cell. On the one-sided cells it is half a market over a whole population —
/// a number that reads as "this state is empty" when it means "the other half
/// of the data hasn't been ingested for this trade". Naming the basis on the
/// record is what stops the two from being compared as if they were the same
/// measure.
fn per_10k_basis(coverage: &str, solo_grain: &str) -> &'static str {
    match (coverage, solo_grain) {
        // The sub-state case, and the one most likely to be misread: employers
        // at county grain over a county population, with the solo half missing
        // because NES does not publish it there.
        (_, "state_carried") => {
            "employer_only at county grain — the solo half is published only per STATE \
             (solo_grain: state_carried) and is NOT counted"
        }
        ("both", _) => "employer+solo",
        ("employer_only", _) => "employer_only — solo operators NOT counted",
        _ => "solo_only — employer establishments NOT counted",
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
        Some(c) => format!("{}·{}", census_common::state_abbr(st_fips), c),
        None => census_common::state_abbr(st_fips).to_string(),
    }
}

/// ACS population/household base for saturation. Jam values (negatives) → 0.
pub struct Denom {
    pub population: i64,
    pub households: i64,
    pub owner_occupied: i64,
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

// ---------------------------------------------------------------------------
// N35 — the sub-state launch atlas.
//
// `census/market_blend` answers "which STATE" at naics4 grain. The atlas is the
// grain people actually open a business in: the top counties per trade, ranked
// two ways, scoped to the top-K states, with the metro each one sits in — and
// with every block saying what grain it is, because a county row assembled from
// county employers, state-carried solos, sector-grain succession and national
// formation is four grains in one record and reads as one unless it says so.
// ---------------------------------------------------------------------------

/// The county atlas, in the same virtual `census` namespace as the other two
/// products.
pub const ATLAS_DATASET: &str = "atlas";

/// What the atlas is derived from, in read order.
const ATLAS_INPUTS: [&str; 2] = ["census/market_blend", "census/saturation"];

/// **How the top-K states are chosen, and why it is not what the card asked
/// for.** The design says "the top-K states by formation velocity". BFS — the
/// only formation source in the fleet — publishes at NATIONAL geography only
/// (`census-bfs`: `for=state:*` is HTTP 400, and every velocity record carries
/// `grain: naics_sector_national`). There is no per-state formation velocity to
/// rank by, and inventing one by apportioning the national series across states
/// is exactly the fabrication this family refuses.
///
/// So the scope rail ranks states by the largest honest signal that IS
/// state-grain: the blend's own `total_market` summed across trades. The basis
/// is published on the run result and on every atlas record, so nobody reads
/// this ranking as a formation ranking.
pub const ATLAS_STATE_RANK_BASIS: &str =
    "state_total_market — BFS formation velocity is NATIONAL-grain only and cannot rank states";

/// The rails the atlas runs under: `[census]` for an operator, `params.*` for
/// one run.
#[derive(Debug, Clone, PartialEq)]
pub struct AtlasSettings {
    pub states_k: usize,
    pub top_n: usize,
    pub metros: usize,
    /// Whether the metro plan is actually REQUESTED as schedules. Default
    /// false: the plan is computed and reported either way.
    pub metro_pricing: bool,
    pub metro_pricing_cron: String,
    /// `None` = ask for no ceiling.
    pub metro_pricing_budget_usd: Option<f64>,
}

impl Default for AtlasSettings {
    fn default() -> Self {
        Self::from_config(&pumper_core::config::CensusConfig::default())
    }
}

impl AtlasSettings {
    /// The operator's `[census]` section.
    pub fn from_config(c: &pumper_core::config::CensusConfig) -> Self {
        Self {
            states_k: c.atlas_states_k.max(1),
            top_n: c.atlas_top_n.max(1),
            metros: c.atlas_metros,
            metro_pricing: c.metro_pricing,
            metro_pricing_cron: c.metro_pricing_cron.clone(),
            metro_pricing_budget_usd: (c.metro_pricing_budget_usd > 0.0)
                .then_some(c.metro_pricing_budget_usd),
        }
    }

    /// Per-run overrides. Params only ever narrow or widen numbers a run may
    /// legitimately choose; `metro_pricing` can be turned ON here as well,
    /// because a one-off "plan and buy it" is a job, not a policy — and the
    /// runtime's own `max_app_schedules_per_run` cap still bounds it.
    pub fn with_params(mut self, ctx: &AppContext) -> Self {
        let usize_param = |name: &str| {
            ctx.params
                .get(name)
                .and_then(Value::as_u64)
                .map(|n| n as usize)
        };
        if let Some(k) = usize_param("atlas_states_k") {
            self.states_k = k.max(1);
        }
        if let Some(n) = usize_param("atlas_top_n") {
            self.top_n = n.max(1);
        }
        if let Some(m) = usize_param("atlas_metros") {
            self.metros = m;
        }
        if let Some(b) = ctx.params.get("metro_pricing").and_then(Value::as_bool) {
            self.metro_pricing = b;
        }
        self
    }
}

/// The top-K states the county atlas is scoped to, ranked by the blend's own
/// state-grain `total_market` — see [`ATLAS_STATE_RANK_BASIS`] for why this is
/// not formation velocity.
///
/// Ties break on state FIPS so the scope is stable run to run: a ranking that
/// reshuffles under a tie would silently move counties in and out of the atlas.
pub fn top_states_for_atlas(blend: &[(String, Value)], k: usize) -> Vec<Value> {
    let mut totals: BTreeMap<String, (i64, String)> = BTreeMap::new();
    for (_, v) in blend {
        if v.get("geo").and_then(Value::as_str).unwrap_or("state") != "state" {
            continue;
        }
        let Some(fips) = v.get("parent_state_fips").and_then(Value::as_str) else {
            continue;
        };
        let entry = totals
            .entry(fips.to_string())
            .or_insert_with(|| (0, String::new()));
        entry.0 += v.get("total_market").and_then(Value::as_i64).unwrap_or(0);
        if entry.1.is_empty() {
            entry.1 = v
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or(fips)
                .to_string();
        }
    }
    let mut ranked: Vec<(String, i64, String)> = totals
        .into_iter()
        .map(|(fips, (total, label))| (fips, total, label))
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    ranked
        .into_iter()
        .take(k)
        .enumerate()
        .map(|(i, (fips, total, label))| {
            json!({
                "rank": i + 1,
                "state_fips": fips,
                "state": label,
                "state_total_market": total,
                "ranked_by": ATLAS_STATE_RANK_BASIS,
            })
        })
        .collect()
}

/// place → per-10k saturation, off the persisted `census/saturation` rows the
/// blend already read. County rows only: the atlas ranks counties, and a
/// state's saturation is not a county's.
pub fn county_saturation_index(bases: &[Value]) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for r in bases {
        if r.get("geo").and_then(Value::as_str) != Some("county") {
            continue;
        }
        let (Some(place), Some(per_10k)) = (
            r.get("place").and_then(Value::as_str),
            r.get("per_10k").and_then(Value::as_f64),
        ) else {
            continue;
        };
        out.entry(place.to_string()).or_insert(per_10k);
    }
    out
}

/// Rank the county cells of the scoped states two ways and emit the union.
///
/// **Two rankings, not one blended score.** `saturation_per_10k` is the
/// employer-side ranking `census/saturation` already publishes (establishments
/// per 10k, county grain); `total_market_per_10k` is the blend cell's own
/// density, which on a `state_carried` county counts employers ONLY. They
/// answer different questions and a county can top one and miss the other, so
/// each record carries both ranks and a Null for the ranking it did not enter —
/// never a fabricated position.
///
/// A county outside `allowed_states` is not in the atlas at all: the scope rail
/// is what bounds both this dataset and the CBP fan-out behind it.
pub fn atlas_records(
    blend: &[(String, Value)],
    saturation: &BTreeMap<String, f64>,
    state_ranks: &BTreeMap<String, usize>,
    top_n: usize,
) -> Vec<(String, Value)> {
    // naics4 → the county cells of the scoped states.
    let mut by_trade: BTreeMap<String, Vec<(&String, &Value)>> = BTreeMap::new();
    for (key, v) in blend {
        if v.get("geo").and_then(Value::as_str) != Some("county") {
            continue;
        }
        let Some(state_fips) = v.get("parent_state_fips").and_then(Value::as_str) else {
            continue;
        };
        if !state_ranks.contains_key(state_fips) {
            continue;
        }
        let Some(naics4) = v.get("naics4").and_then(Value::as_str) else {
            continue;
        };
        by_trade
            .entry(naics4.to_string())
            .or_default()
            .push((key, v));
    }

    let place_of_cell = |v: &Value| {
        v.get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let mut out: Vec<(String, Value)> = Vec::new();
    for (naics4, cells) in by_trade {
        // Ranking 1: county saturation (establishments per 10k).
        let mut by_sat: Vec<(&String, f64)> = cells
            .iter()
            .filter_map(|(k, v)| saturation.get(&place_of_cell(v)).map(|s| (*k, *s)))
            .collect();
        by_sat.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        let sat_rank: BTreeMap<&String, usize> = by_sat
            .iter()
            .take(top_n)
            .enumerate()
            .map(|(i, (k, _))| (*k, i + 1))
            .collect();

        // Ranking 2: the blend cell's own total-market density.
        let mut by_market: Vec<(&String, f64)> = cells
            .iter()
            .filter_map(|(k, v)| {
                v.get("total_market_per_10k")
                    .and_then(Value::as_f64)
                    .map(|m| (*k, m))
            })
            .collect();
        by_market.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        let market_rank: BTreeMap<&String, usize> = by_market
            .iter()
            .take(top_n)
            .enumerate()
            .map(|(i, (k, _))| (*k, i + 1))
            .collect();

        for (key, v) in cells {
            let (sat, market) = (sat_rank.get(key), market_rank.get(key));
            if sat.is_none() && market.is_none() {
                continue;
            }
            let place = place_of_cell(v);
            let county_fips5 = v
                .get("geo_fips")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let metro = census_common::cbsa_for_county(&county_fips5)
                .map(|m| {
                    json!({
                        "cbsa_code": m.code,
                        "cbsa_title": m.title,
                        "crosswalk_vintage": census_common::CBSA_VINTAGE,
                    })
                })
                .unwrap_or(Value::Null);
            let state_fips = v
                .get("parent_state_fips")
                .and_then(Value::as_str)
                .unwrap_or("");
            let rank = |r: Option<&usize>| r.map(|n| Value::from(*n)).unwrap_or(Value::Null);
            let field = |name: &str| v.get(name).cloned().unwrap_or(Value::Null);
            out.push((
                key.clone(),
                json!({
                    "naics4": naics4,
                    "trade": field("trade"),
                    "place": place,
                    "geo": "county",
                    "geo_fips": county_fips5,
                    "state": census_common::state_abbr(state_fips),
                    "state_fips": state_fips,
                    "county_fips": field("county_fips"),
                    "atlas_state_rank": state_ranks.get(state_fips).map(|r| Value::from(*r)).unwrap_or(Value::Null),
                    "atlas_state_rank_basis": ATLAS_STATE_RANK_BASIS,
                    "rank_by_saturation": rank(sat),
                    "rank_by_total_market_per_10k": rank(market),
                    "saturation_per_10k": saturation
                        .get(&place_of_cell(v))
                        .map(|s| Value::from(*s))
                        .unwrap_or(Value::Null),
                    "employer_establishments": field("employer_establishments"),
                    "solo_operators": field("solo_operators"),
                    "solo_state_operators": field("solo_state_operators"),
                    "total_market": field("total_market"),
                    "total_market_per_10k": field("total_market_per_10k"),
                    "total_market_per_10k_basis": field("total_market_per_10k_basis"),
                    "base": field("base"),
                    "denominator_kind": field("denominator_kind"),
                    "coverage": field("coverage"),
                    "metro": metro,
                    // EVERY BLOCK, GRAIN-LABELLED. A county atlas row is four
                    // grains stacked: employers at county, solos at county or
                    // state-carried, succession at 2-digit sector x state,
                    // formation at national sector. Without these five labels
                    // the row reads as one measurement of one county.
                    "grains": {
                        "cell": "naics4|county",
                        "employer": field("employer_grain"),
                        "solo": field("solo_grain"),
                        "saturation": "county",
                        "succession": field("succession_grain"),
                        "formation": v.get("formation")
                            .and_then(|f| f.get("grain"))
                            .cloned()
                            .unwrap_or(Value::Null),
                        "state_scope": ATLAS_STATE_RANK_BASIS,
                    },
                    "pct_owners_55plus": field("pct_owners_55plus"),
                    "formation": field("formation"),
                    "vintages": field("vintages"),
                }),
            ));
        }
    }
    out
}

/// The metro pricing plan: which markets the atlas would pay to price.
///
/// Counties are walked in atlas order (best rank first) and folded into their
/// CBSA, so the metros picked are the ones the atlas actually ranked highest —
/// and a metro is named once however many of its counties made the list. A
/// county the crosswalk does not know is COUNTED, never substituted: an
/// unmapped county silently priced as its state is how a national research run
/// gets billed as a metro one.
pub struct MetroPlan {
    pub entries: Vec<Value>,
    /// Counties in the atlas whose metro the crosswalk does not know.
    pub unmapped_counties: usize,
    /// Metros the atlas found beyond the `metros` cap — reported, not dropped
    /// silently.
    pub truncated: usize,
}

pub fn metro_pricing_plan(
    atlas: &[(String, Value)],
    max_metros: usize,
    cron: &str,
    budget_usd: Option<f64>,
) -> MetroPlan {
    // Best rank a county reached in either ranking — the order metros are
    // chosen in.
    let best_rank = |v: &Value| -> usize {
        let r = |n: &str| {
            v.get(n)
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .unwrap_or(usize::MAX)
        };
        r("rank_by_saturation").min(r("rank_by_total_market_per_10k"))
    };
    let mut ordered: Vec<&(String, Value)> = atlas.iter().collect();
    ordered.sort_by(|a, b| {
        best_rank(&a.1)
            .cmp(&best_rank(&b.1))
            .then_with(|| a.0.cmp(&b.0))
    });

    let mut order: Vec<String> = Vec::new();
    let mut by_cbsa: BTreeMap<String, (String, BTreeSet<String>, BTreeSet<String>)> =
        BTreeMap::new();
    let mut unmapped: BTreeSet<String> = BTreeSet::new();
    for (_, v) in ordered {
        let fips5 = v.get("geo_fips").and_then(Value::as_str).unwrap_or("");
        let Some(metro) = v.get("metro").and_then(|m| m.get("cbsa_code")) else {
            unmapped.insert(fips5.to_string());
            continue;
        };
        let Some(code) = metro.as_str() else {
            unmapped.insert(fips5.to_string());
            continue;
        };
        let title = v
            .get("metro")
            .and_then(|m| m.get("cbsa_title"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let slot = by_cbsa.entry(code.to_string()).or_insert_with(|| {
            order.push(code.to_string());
            (title, BTreeSet::new(), BTreeSet::new())
        });
        slot.1.insert(fips5.to_string());
        if let Some(n) = v.get("naics4").and_then(Value::as_str) {
            slot.2.insert(n.to_string());
        }
    }

    let truncated = order.len().saturating_sub(max_metros);
    let entries = order
        .into_iter()
        .take(max_metros)
        .map(|code| {
            let (title, counties, trades) = by_cbsa.remove(&code).expect("ordered code");
            let mut schedule = json!({
                "app": "homewyse-pricing",
                "cron": cron,
                "params": { "locality": title },
            });
            // The budget rail `POST /schedules` validates. Omitted, never 0:
            // `validate_budget_usd` refuses a non-positive ceiling.
            if let (Some(b), Value::Object(map)) = (budget_usd, &mut schedule) {
                map.insert("budget_usd".into(), Value::from(b));
            }
            json!({
                "cbsa_code": code,
                "cbsa_title": title,
                "locality": title,
                "counties": counties.into_iter().collect::<Vec<_>>(),
                "naics4": trades.into_iter().collect::<Vec<_>>(),
                "schedule": schedule,
            })
        })
        .collect();
    MetroPlan {
        entries,
        unmapped_counties: unmapped.len(),
        truncated,
    }
}

/// Builds the atlas from a blend that has just been computed (never a re-read:
/// the atlas must describe THIS blend), upserts it, and returns the run block —
/// including the metro pricing plan, which is always reported and only
/// REQUESTED when the driver is on.
pub async fn sync_atlas(
    ctx: &AppContext,
    blend: &[(String, Value)],
    bases: &[Value],
    settings: &AtlasSettings,
) -> Result<Value> {
    let states = top_states_for_atlas(blend, settings.states_k);
    let state_ranks: BTreeMap<String, usize> = states
        .iter()
        .filter_map(|s| {
            Some((
                s.get("state_fips")?.as_str()?.to_string(),
                s.get("rank")?.as_u64()? as usize,
            ))
        })
        .collect();
    let saturation = county_saturation_index(bases);
    let records = atlas_records(blend, &saturation, &state_ranks, settings.top_n);
    let plan = metro_pricing_plan(
        &records,
        settings.metros,
        &settings.metro_pricing_cron,
        settings.metro_pricing_budget_usd,
    );

    // The runtime owns schedule creation; the app only asks, and only when the
    // driver is on. A refusal (the per-run ceiling) is reported, not swallowed.
    let mut requested = 0usize;
    let mut refused = 0usize;
    if settings.metro_pricing {
        for entry in &plan.entries {
            let Some(body) = entry.get("schedule") else {
                continue;
            };
            if ctx.request_schedule(body.clone()) {
                requested += 1;
            } else {
                refused += 1;
            }
        }
    }

    let prov = census_common::derived_provenance(ctx, ATLAS_DATASET, &ATLAS_INPUTS);
    let summary = ctx
        .datasets
        .upsert_many_stamped(MARKET_APP, ATLAS_DATASET, &records, None, Some(&prov))
        .await?;
    let county_cells = blend
        .iter()
        .filter(|(_, v)| v.get("geo").and_then(Value::as_str) == Some("county"))
        .count();
    Ok(json!({
        "dataset": format!("{MARKET_APP}/{ATLAS_DATASET}"),
        "states_k": settings.states_k,
        "top_n": settings.top_n,
        "states": states,
        "state_rank_basis": ATLAS_STATE_RANK_BASIS,
        "county_cells_available": county_cells,
        "counties_ranked": records.len(),
        "counties_with_saturation": saturation.len(),
        "new": summary.new.len(),
        "changed": summary.changed.len(),
        "unchanged": summary.unchanged,
        "metro_pricing": {
            // The plan is ALWAYS computed and reported; `enabled` says whether
            // any of it was actually asked for.
            "enabled": settings.metro_pricing,
            "cap": settings.metros,
            "plan": plan.entries,
            "metros_truncated": plan.truncated,
            "unmapped_counties": plan.unmapped_counties,
            "crosswalk_counties": census_common::cbsa_crosswalk_len(),
            "requested": requested,
            "refused_by_run_cap": refused,
        },
    }))
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
        let app = CensusDensity::default();
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
        // N33: this app's run also republishes the cross-FAMILY product, and a
        // namespace the result does not name is a namespace no watch, trigger,
        // saved search or contract evaluation can fire for. The spec is added by
        // `with_market_index`, which run() wraps around the shared stamp.
        let needle = concat!("with_market_index", "(census_common::with_product_index");
        assert_eq!(
            include_str!("lib.rs").matches(needle).count(),
            1,
            "census-density's run() must add the market spec exactly once"
        );
        // Bound in two steps on purpose: written as one nested call this line
        // would match the needle above and the count would check itself.
        let stamped = census_common::with_product_index(json!({}));
        assert_eq!(
            with_market_index(stamped)["index_datasets"],
            json!([
                { "app": "census", "dataset": "market_blend" },
                { "app": "census", "dataset": "saturation" },
                { "app": "market", "dataset": "profile" },
                // N35: the county atlas is a product like the other two — a
                // namespace/dataset pair the result does not name is one no
                // watch, trigger or saved search can ever fire for.
                { "app": "census", "dataset": "atlas" },
            ])
        );
        // A result with no index_datasets at all is passed through untouched —
        // there is nowhere honest to append to.
        assert_eq!(
            with_market_index(json!({ "records": 1 })),
            json!({ "records": 1 })
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

    // CBP payload shaped like the real one: header then data rows.
    fn cbp_rows(data: &[[&str; 4]]) -> Vec<Vec<String>> {
        let mut rows = vec![["ESTAB", "EMP", "PAYANN", "state"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()];
        rows.extend(
            data.iter()
                .map(|r| r.iter().map(|c| c.to_string()).collect::<Vec<_>>()),
        );
        rows
    }

    fn cbp_rollup(data: &[[&str; 4]]) -> CbpRollup {
        map_cbp_rows(
            &cbp_rows(data),
            &CbpCols {
                estab: 0,
                geo: 3,
                state: Some(3),
                emp: Some(1),
                pay: Some(2),
            },
            "238220",
            "Plumbing",
            "state",
            "2022",
        )
    }

    /// The anti-pattern: suppression counted as data. A withheld ESTAB drops the
    /// place (it is not a reported place); a withheld EMP/PAYANN keeps the place
    /// but must leave the derived ratio Null rather than fabricate a $0 wage.
    /// Both are COUNTED — "312 places reported" means something different when
    /// 40 more were dropped, and that difference used to be invisible.
    #[test]
    fn suppressed_cbp_cells_are_counted_not_absorbed_as_zeros() {
        let r = cbp_rollup(&[
            ["100", "500", "30000", "06"],
            // ESTAB withheld → the whole place is dropped.
            ["-666666666", "500", "30000", "48"],
            // EMP withheld → place kept, both employee-derived ratios Null.
            ["50", "D", "9000", "12"],
        ]);
        assert_eq!(r.records.len(), 2);
        assert_eq!(r.places_reported, 2);
        assert_eq!(r.suppressed.places_dropped, 1);
        assert_eq!(r.suppressed.employees, 1);
        assert_eq!(r.suppressed.payroll, 0);
        assert_eq!(r.total_estab, 150, "the dropped place adds nothing");

        let ca = &r.records[0].1;
        assert_eq!(ca["establishments"], 100);
        assert_eq!(ca["avg_annual_wage"], json!(60_000.0)); // 30000k/500
        let fl = &r.records[1].1;
        assert_eq!(fl["avg_annual_wage"], Value::Null);
        assert_eq!(fl["avg_establishment_size"], Value::Null);

        // A REPORTED zero is still a measured zero, never suppression.
        let z = cbp_rollup(&[["0", "0", "0", "02"]]);
        assert_eq!(z.records.len(), 1);
        assert_eq!(z.records[0].1["establishments"], 0);
        assert_eq!(z.suppressed, Suppression::default());
    }

    /// The anti-pattern: a national ratio whose numerator and denominator come
    /// from different sets of places. Suppression is per-CELL, so a state that
    /// reported `EMP` with `PAYANN` withheld used to add its employees to the
    /// denominator of `national_avg_wage` and nothing to the numerator — the
    /// published national wage then moved with Census's disclosure rules rather
    /// than with the labour market. It is the employer-side twin of the
    /// `receipts_thousands` fabrication.
    #[test]
    fn national_benchmarks_divide_only_by_the_places_that_reported_both_halves() {
        // CA reports everything; TX reports employees but its payroll is
        // withheld; FL's employee count is withheld.
        let r = cbp_rollup(&[
            ["100", "1000", "50000", "06"],
            ["50", "500", "-666666666", "48"],
            ["25", "D", "9000", "12"],
        ]);
        assert_eq!(r.suppressed.payroll, 1);
        assert_eq!(r.suppressed.employees, 1);

        // Wage: only CA reported both → $50,000k over 1,000 employees.
        // The buggy pairing divided 50,000k by CA+TX's 1,500 employees ($33.3k)
        // — a third lower, purely because Texas was not allowed to answer.
        let (wage, size) = national_benchmarks(&r.paired);
        assert_eq!(wage, json!(50_000.0));
        // Establishment size: CA + TX reported employees (FL did not), so the
        // denominator is their 150 establishments, not all 175.
        assert_eq!(size, json!(10.0));

        // The raw totals stay sums-over-reported and are NOT the ratio's halves.
        assert_eq!(r.total_emp, 1500);
        assert_eq!(r.total_estab, 175);

        // A withheld cell is Null on the record too — never a fabricated 0.
        let tx = &r.records[1].1;
        assert_eq!(tx["employees"], 500);
        assert_eq!(tx["annual_payroll_thousands"], Value::Null);
        let fl = &r.records[2].1;
        assert_eq!(fl["employees"], Value::Null);
        assert_eq!(fl["annual_payroll_thousands"], 9000);

        // Nothing reported → no benchmark, rather than a 0 or a divide-by-zero.
        let none = national_benchmarks(&PairedTotals::default());
        assert_eq!(none, (Value::Null, Value::Null));
    }

    /// The anti-pattern: a place silently vanishing from the saturation ranking.
    /// A place with no ACS row and a place whose base is 0 both used to `return
    /// None` inside a `filter_map`, so `places_matched` was the only number
    /// anyone saw and there was nothing to compare it against.
    #[test]
    fn places_that_cannot_be_normalized_are_counted_by_reason() {
        let overall = BTreeMap::from([
            ("CA".to_string(), 400i64),
            ("TX".to_string(), 200),
            ("AK".to_string(), 5),
        ]);
        let denom = BTreeMap::from([
            (
                "CA".to_string(),
                Denom {
                    population: 40_000,
                    households: 10_000,
                    owner_occupied: 6_000,
                },
            ),
            // TX has an ACS row whose household base is a jam value → 0.
            (
                "TX".to_string(),
                Denom {
                    population: 30_000,
                    households: 0,
                    owner_occupied: 5_000,
                },
            ),
            // AK: no row at all.
        ]);
        let n = normalize_places(&overall, &denom, "households");
        assert_eq!(n.rows.len(), 1);
        assert_eq!(n.rows[0].0, "CA");
        assert_eq!(n.base_not_positive, 1);
        assert_eq!(n.no_denominator_row, 1);
        // Switching the denominator moves TX back in — the exclusion is about
        // the chosen base, not the place.
        let pop = normalize_places(&overall, &denom, "population");
        assert_eq!(pop.rows.len(), 2);
        assert_eq!(pop.base_not_positive, 0);
    }

    /// The anti-pattern: comparing a one-sided cell's per-10k with a complete
    /// cell's as if they measured the same thing. The value now travels with a
    /// machine-readable basis saying what entered the numerator.
    #[test]
    fn per_10k_carries_the_coverage_it_was_computed_over() {
        let bases = BTreeMap::from([
            ("CA".to_string(), test_base(10_000)),
            ("TX".to_string(), test_base(10_000)),
        ]);
        // A `both` cell: employer + solo over the base.
        let both = blend_market(
            &[emp("238220", "state", "CA", "06", 100)],
            &[solo("2382", "CA", "06", 300)],
            &bases,
            &[],
            &[],
        );
        assert_eq!(both[0].1["total_market_per_10k"], json!(400.0));
        assert_eq!(both[0].1["total_market_per_10k_basis"], "employer+solo");

        // An `employer_only` cell over the SAME base: the number is half a
        // market, and must say so rather than read as a thin state.
        let one_sided = blend_market(
            &[emp("561730", "state", "TX", "48", 80)],
            &[solo("2382", "CA", "06", 1)],
            &bases,
            &[],
            &[],
        );
        let tx = one_sided
            .iter()
            .find(|(k, _)| k == "5617:48")
            .expect("TX cell");
        assert_eq!(tx.1["coverage"], "employer_only");
        assert_eq!(tx.1["total_market_per_10k"], json!(80.0));
        assert_eq!(
            tx.1["total_market_per_10k_basis"],
            "employer_only — solo operators NOT counted"
        );
        // No base → no ratio AND no basis label (nothing to qualify).
        let none = blend_market(
            &[emp("238220", "state", "CA", "06", 100)],
            &[solo("2382", "CA", "06", 300)],
            &BTreeMap::new(),
            &[],
            &[],
        );
        assert!(none[0].1["total_market_per_10k"].is_null());
        assert!(none[0].1["total_market_per_10k_basis"].is_null());
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
        // The key carries its own grain: a `population` run no longer rewrites
        // the `households` ranking under the same keys.
        assert_eq!(key, "state|households|CA");
        assert_eq!(v["key_grain"], SATURATION_KEY_GRAIN);
        assert_eq!(v["place"], "CA");
        assert_eq!(v["combined_establishments"], 400);
        assert_eq!(v["base"], 10_000);
        assert_eq!(v["denominator_kind"], "households");
        assert_eq!(v["per_10k"], json!(400.0));
        assert_eq!(v["acs_year"], "2022");
    }

    fn test_base(base: i64) -> PlaceBase {
        PlaceBase {
            base,
            denominator_kind: "households".into(),
            acs_year: Some("2022".into()),
        }
    }

    fn emp(naics: &str, geo: &str, place: &str, st: &str, estab: i64) -> Value {
        json!({
            "naics": naics, "geo": geo, "place": place, "state_fips": st,
            "establishments": estab, "year": "2022",
        })
    }

    /// A county-grain employer row, exactly as the CBP loop writes one.
    fn county_emp(naics: &str, st: &str, county: &str, estab: i64) -> Value {
        json!({
            "naics": naics, "geo": "county",
            "place": format!("{}·{county}", census_common::state_abbr(st)),
            "state_fips": st, "county_fips": county,
            "establishments": estab, "year": "2022",
        })
    }

    /// A county-grain SOLO row — what `census-nonemp` writes if the probe ever
    /// answers `served`.
    fn county_solo(naics4: &str, st: &str, county: &str, nonemp: i64) -> Value {
        json!({
            "naics": naics4, "trade": "Building equipment contractors",
            "geo": "county", "state": census_common::state_abbr(st),
            "state_fips": st, "county_fips": county,
            "place": format!("{}·{county}", census_common::state_abbr(st)),
            "nonemployers": nonemp, "year": "2021",
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

    /// County employer rows used to be DROPPED here ("the solo side has no
    /// county grain"). They now form their own cell — and the anti-pattern this
    /// test pins is the one that replaced it: a county cell folding into, or
    /// overwriting, the state cell for the same trade. The state cell must be
    /// byte-identical to what it was before the geo dimension existed.
    #[test]
    fn county_employer_rows_get_their_own_cell_and_never_the_states() {
        let employers = vec![
            county_emp("238220", "06", "037", 40),
            emp("238220", "state", "CA", "06", 100),
        ];
        let solos = vec![solo("2382", "CA", "06", 10)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        let by_key: BTreeMap<_, _> = items.into_iter().collect();
        assert_eq!(by_key.len(), 2, "one state cell and one county cell");

        let state = &by_key["2382:06"];
        assert_eq!(state["employer_establishments"], 100, "not 140");
        assert_eq!(state["geo"], "state");
        assert_eq!(state["state_fips"], "06");
        assert_eq!(state["solo_grain"], "state");
        assert_eq!(state["solo_operators"], 10);

        let county = &by_key["2382:06037"];
        assert_eq!(county["employer_establishments"], 40);
        assert_eq!(county["geo"], "county");
        assert_eq!(county["geo_fips"], "06037");
        assert_eq!(county["county_fips"], "037");
        assert_eq!(county["parent_state_fips"], "06");
    }

    /// **The N35 gate.** A county cell with no NES row of its own carries
    /// `solo_grain: state_carried`, a NULL solo count, and a `total_market`
    /// that counts employers only — never the state's solo operators
    /// apportioned onto a county, and never a `0` that reads as "no solo
    /// operators here".
    #[test]
    fn a_county_with_no_nes_row_is_state_carried_not_a_fabricated_county_number() {
        let employers = vec![county_emp("238220", "06", "037", 40)];
        // Only a STATE solo row exists — the ASSUMED-unavailable county grain.
        let solos = vec![solo("2382", "CA", "06", 300)];
        let mut bases = BTreeMap::new();
        bases.insert("CA·037".to_string(), test_base(10_000));
        let items = blend_market(&employers, &solos, &bases, &[], &[]);
        let by_key: BTreeMap<_, _> = items.into_iter().collect();
        let c = &by_key["2382:06037"];
        assert_eq!(c["solo_grain"], "state_carried");
        assert!(
            c["solo_operators"].is_null(),
            "a county solo count nobody published must be Null, not 0 and not 300"
        );
        assert!(c["solo_share"].is_null());
        assert_eq!(c["total_market"], 40, "employers only");
        // The state's total rides along as labelled CONTEXT.
        assert_eq!(c["solo_state_operators"], 300);
        // 40 employers / 10,000 households * 10k = 40.0, and the basis says
        // out loud that it is half a market.
        assert_eq!(c["total_market_per_10k"], json!(40.0));
        let basis = c["total_market_per_10k_basis"].as_str().expect("a basis");
        assert!(basis.contains("state_carried"), "{basis}");

        // And when NES DOES serve the county, the same cell joins at county
        // grain with no code change — the probe's `served` verdict landing.
        let solos = vec![
            solo("2382", "CA", "06", 300),
            county_solo("2382", "06", "037", 25),
        ];
        let items = blend_market(&employers, &solos, &bases, &[], &[]);
        let by_key: BTreeMap<_, _> = items.into_iter().collect();
        let c = &by_key["2382:06037"];
        assert_eq!(c["solo_grain"], "county");
        assert_eq!(c["solo_operators"], 25);
        assert_eq!(c["total_market"], 65);
        assert!(c["solo_state_operators"].is_null());
    }

    /// The collision this row shape exists to prevent: `market/profile` indexes
    /// blend cells by `{naics4}:{state_fips}` off the record's own fields, so a
    /// county cell carrying `state_fips` would replace the state cell behind
    /// every state × trade profile row — a county's employer count published as
    /// the state's market.
    #[test]
    fn county_cells_cannot_displace_the_state_cell_in_the_profile_join() {
        let employers = vec![
            emp("238220", "state", "CA", "06", 100),
            county_emp("238220", "06", "037", 40),
        ];
        let solos = vec![solo("2382", "CA", "06", 300)];
        let blend: Vec<Value> = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[])
            .into_iter()
            .map(|(_, v)| v)
            .collect();
        let economics = vec![json!({ "trade": "Plumbing", "state": "CA" })];
        let naics4 = BTreeMap::from([("Plumbing".to_string(), "2382".to_string())]);
        let profiles = trades_common::market::build_profiles(&economics, &blend, &naics4);
        assert_eq!(profiles.len(), 1);
        let (key, p) = &profiles[0];
        assert_eq!(key, "CA:Plumbing");
        assert_eq!(
            p["density"]["total_market"], 400,
            "the STATE cell (100 + 300), never the county's 40"
        );
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
        bases.insert("CA".to_string(), test_base(10_000));
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

    /// N33, end to end through a real store: a census run is the LAST WRITER,
    /// so it republishes the cross-family `market/profile` — and the profile is
    /// honest about the half it does not have.
    ///
    /// The anti-pattern this closes is the zeroed half-profile: before the
    /// coverage marker, a trade whose 4-digit group the census publishes
    /// nothing for would have been indistinguishable from a state where nobody
    /// operates.
    #[tokio::test]
    async fn a_census_run_republishes_the_state_x_trade_profile_with_honest_coverage() {
        let (_store, ctx) = seeded_ctx("census-blend-profile").await;
        // The trades half. Plumbing and HVAC share NAICS 238220 → one cell;
        // Landscaping (5617) has no cell in this fixture.
        let econ = |trade: &str| {
            json!({
                "trade": trade, "state": "CA", "soc_code": "47-2152",
                "wage_band": { "median_hourly": 30.0 }, "wage_grain": "national",
                "pricing": Value::Null, "pricing_locality": "CA",
                "tax": { "federal": { "qbi_deduction_pct": 20.0 } },
                "compliance": Value::Null, "valuation": Value::Null,
            })
        };
        ctx.datasets
            .upsert_many(
                "trades",
                "operator_economics",
                &[
                    ("CA:Plumbing".to_string(), econ("Plumbing")),
                    ("CA:HVAC".to_string(), econ("HVAC")),
                    ("CA:Landscaping".to_string(), econ("Landscaping")),
                    // The national roll-up the trades layer also writes — it
                    // has no state FIPS and must not become a profile row.
                    (
                        "US:Plumbing".to_string(),
                        json!({ "trade": "Plumbing", "state": "US" }),
                    ),
                ],
            )
            .await
            .expect("seed economics");

        let out = sync_market_blend(&ctx).await.expect("blend");
        let profile = &out["market_profile"];
        assert_eq!(
            profile["profiled"], 3,
            "3 state rows, the US roll-up skipped"
        );
        assert_eq!(profile["with_density"], 2, "only 2382 has a cell");
        assert_eq!(profile["economics_only"], 1);
        assert_eq!(profile["density_grain"], "naics4");
        assert_eq!(profile["dataset"], "market/profile");

        async fn profile_row(ctx: &AppContext, key: &str) -> Value {
            ctx.datasets
                .get("market", "profile", key)
                .await
                .expect("read")
                .expect("record")
                .data
        }
        let plumbing = profile_row(&ctx, "CA:Plumbing").await;
        let hvac = profile_row(&ctx, "CA:HVAC").await;
        // Two distinct profiles, ONE density block, labeled.
        assert_eq!(plumbing["trade"], "Plumbing");
        assert_eq!(hvac["trade"], "HVAC");
        assert_eq!(plumbing["density_key"], "2382:06");
        assert_eq!(plumbing["density"], hvac["density"]);
        assert_eq!(plumbing["density_grain"], "naics4");
        assert_eq!(plumbing["density"]["total_market"], 400);
        assert_eq!(plumbing["coverage"], "both");
        assert_eq!(plumbing["state_fips"], "06");

        // The half profile: absent, never zeroed.
        let landscaping = profile_row(&ctx, "CA:Landscaping").await;
        assert_eq!(landscaping["coverage"], "economics_only");
        assert!(landscaping["density"].is_null());
        assert!(landscaping["total_market_per_10k"].is_null());
        assert_eq!(
            landscaping["economics"]["wage_band"]["median_hourly"], 30.0,
            "the half it HAS is whole"
        );
        assert!(
            ctx.datasets
                .get("market", "profile", "US:Plumbing")
                .await
                .expect("read")
                .is_none(),
            "the national roll-up is not a state profile"
        );

        // Provenance: a row in a namespace no app owns still names its job, its
        // inputs and when it was derived.
        let revs = ctx
            .datasets
            .history("market", "profile", "CA:Plumbing", 10)
            .await
            .expect("history");
        let prov = &revs.first().expect("one revision").provenance;
        assert_eq!(prov.job_id.as_deref(), Some(&*ctx.job_id.to_string()));
        let url = prov.source_url.as_deref().expect("derived source_url");
        assert!(url.starts_with("derived://market/profile?"), "{url}");
        assert!(url.contains("trades/operator_economics"), "{url}");
        assert!(url.contains("census/market_blend"), "{url}");
        assert!(!prov.replayable(), "a joined row has no body to replay");

        // Idempotent: a second run with nothing changed announces nothing.
        let again = sync_market_blend(&ctx).await.expect("blend");
        assert_eq!(again["market_profile"]["new"], 0);
        assert_eq!(again["market_profile"]["changed"], 0);
        assert_eq!(again["market_profile"]["unchanged"], 3);
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
            .history(MARKET_APP, SATURATION_DATASET, "state|households|CA", 10)
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

    /// The anti-pattern: a re-run with an older `year` rewriting current data
    /// backwards, and change detection publishing the regression as a FORWARD
    /// change — a `changed` revision, every watch and trigger on
    /// `establishments`, a search re-index, all saying "the market moved".
    #[tokio::test]
    async fn an_older_year_rerun_is_refused_before_it_rewrites_current_data() {
        let store = pumper_core::testing::TempStore::new("census-vintage").await;
        let ctx2022 = pumper_core::testing::TestContext::new(&store.storage, "census-density")
            .params(json!({ "year": "2022" }))
            .build();
        // First run of any vintage is always allowed.
        let first = census_common::guard_vintage(&ctx2022, "establishments", "2022")
            .await
            .expect("first run");
        assert_eq!(first["verdict"], "first_run");
        assert_eq!(first["held"], Value::Null);
        census_common::record_vintage(&ctx2022, "establishments", "2022")
            .await
            .expect("watermark");

        // The same vintage again — the ordinary scheduled re-run.
        let again = census_common::guard_vintage(&ctx2022, "establishments", "2022")
            .await
            .expect("rerun");
        assert_eq!(again["verdict"], "rerun");
        assert_eq!(again["held"], "2022");

        // An OLDER vintage: refused, with the escape hatch named.
        let ctx2019 = pumper_core::testing::TestContext::new(&store.storage, "census-density")
            .params(json!({ "year": "2019" }))
            .build();
        let err = census_common::guard_vintage(&ctx2019, "establishments", "2019")
            .await
            .expect_err("a rewind must be refused");
        let msg = err.to_string();
        assert!(msg.contains("holds vintage 2022"), "{msg}");
        assert!(msg.contains("allow_vintage_rewind"), "{msg}");

        // ...unless it is asked for explicitly, and then the watermark follows
        // the data rather than staying at a high-water mark of runs.
        let forced = pumper_core::testing::TestContext::new(&store.storage, "census-density")
            .params(json!({ "year": "2019", "allow_vintage_rewind": true }))
            .build();
        let ok = census_common::guard_vintage(&forced, "establishments", "2019")
            .await
            .expect("an approved rewind proceeds");
        assert_eq!(ok["verdict"], "rewind_allowed");
        census_common::record_vintage(&forced, "establishments", "2019")
            .await
            .expect("watermark");
        let after = census_common::guard_vintage(&ctx2022, "establishments", "2022")
            .await
            .expect("advance");
        assert_eq!(after["verdict"], "advance");
        assert_eq!(after["held"], "2019");
        // The guard is per DATASET — one app's other datasets are untouched.
        let other = census_common::guard_vintage(&ctx2019, "owner_age", "2019")
            .await
            .expect("independent watermark");
        assert_eq!(other["verdict"], "first_run");
    }

    /// The anti-pattern: two runs at different grains overwriting each other's
    /// saturation ranking under the same bare `{place}` keys, with change
    /// detection reporting the substitution as movement in the numbers.
    #[test]
    fn saturation_keys_separate_the_grains_that_used_to_overwrite_each_other() {
        let rows = vec![("CA".to_string(), 400i64, 10_000i64, 400.0)];
        let write = |geo, denom| {
            saturation_records(
                &rows,
                &SaturationWrite {
                    geo,
                    denom_kind: denom,
                    acs_dataset: "acs/acs5",
                    acs_year: "2022",
                    year: "2022",
                },
            )[0]
            .0
            .clone()
        };
        let households = write("state", "households");
        let population = write("state", "population");
        let county = write("county", "households");
        assert_eq!(households, "state|households|CA");
        assert_ne!(
            households, population,
            "two denominators are two rankings, not one row rewritten"
        );
        assert_ne!(households, county, "two geographies are two rankings");
        assert_eq!(
            saturation_key("state", "owner_occupied", "CA·037"),
            "state|owner_occupied|CA·037"
        );
    }

    /// The blend's base join must be deterministic now that a place can carry
    /// several saturation rows: state grain only, most recent first — which is
    /// also what keeps a LEGACY `{place}`-keyed row from shadowing a current one.
    #[test]
    fn the_base_join_takes_the_newest_state_row_per_place() {
        let sat = |geo: &str, kind: &str, base: i64, acs: &str| {
            json!({ "place": "CA", "geo": geo, "base": base,
                    "denominator_kind": kind, "acs_year": acs })
        };
        // `Datasets::list` is updated_at DESC, so index 0 is the newest write.
        let idx = base_index(&[
            sat("state", "population", 40_000, "2022"),
            sat("state", "households", 10_000, "2021"),
        ]);
        assert_eq!(
            idx["CA"],
            PlaceBase {
                base: 40_000,
                denominator_kind: "population".into(),
                acs_year: Some("2022".into()),
            }
        );
        // A county row never supplies a STATE cell's base — the guarantee the
        // old state-only filter gave, kept now that county rows ARE indexed:
        // state rows are indexed first, so a county row labelled with a state
        // place cannot shadow one.
        let both = base_index(&[
            sat("county", "households", 500, "2022"),
            sat("state", "households", 10_000, "2022"),
        ]);
        assert_eq!(both["CA"].base, 10_000);
        // A county row under its own place label IS indexed (N35): a county
        // cell's per-10k needs the county's base, not the state's.
        let county = base_index(&[json!({
            "place": "CA·037", "geo": "county", "base": 3_000,
            "denominator_kind": "households", "acs_year": "2022"
        })]);
        assert_eq!(county["CA·037"].base, 3_000);
        // A legacy row with no `geo` is read as state (the only grain that
        // existed when it was written) rather than dropped.
        let legacy = base_index(&[json!({ "place": "CA", "base": 9_000 })]);
        assert_eq!(legacy["CA"].base, 9_000);
        assert_eq!(legacy["CA"].acs_year, None);
    }

    /// The anti-pattern: a mixed-grain taxonomy (`"2382"` AND `"238220"`)
    /// double-summing the aggregate with a component of itself in the cell whose
    /// grain IS the aggregate — a state that looks like it has 50% more
    /// plumbers, with nothing anywhere saying why.
    #[test]
    fn a_covering_naics_is_not_double_summed_with_its_components() {
        let employers = vec![
            emp("2382", "state", "CA", "06", 150),   // the aggregate
            emp("238220", "state", "CA", "06", 100), // a component OF it
            emp("238210", "state", "CA", "06", 50),  // another component
        ];
        let solos = vec![solo("2382", "CA", "06", 300)];
        let items = blend_market(&employers, &solos, &BTreeMap::new(), &[], &[]);
        assert_eq!(items.len(), 1);
        let v = &items[0].1;
        // 150, not 300: the aggregate is the total for the cell.
        assert_eq!(v["employer_establishments"], 150);
        assert_eq!(v["employer_naics"], json!(["2382"]));
        assert_eq!(v["employer_naics_covered"], json!(["238210", "238220"]));
        assert_eq!(v["total_market"], 450);

        // Single-grain (the normal case) is untouched, and reports no coverage.
        let plain = blend_market(
            &[
                emp("238220", "state", "CA", "06", 100),
                emp("238210", "state", "CA", "06", 50),
            ],
            &solos,
            &BTreeMap::new(),
            &[],
            &[],
        );
        assert_eq!(plain[0].1["employer_establishments"], 150);
        assert_eq!(plain[0].1["employer_naics"], json!(["238210", "238220"]));
        assert_eq!(plain[0].1["employer_naics_covered"], json!([]));
    }

    /// The same anti-pattern as `a_covering_naics_is_not_double_summed_with_its_components`,
    /// but on the OTHER path: the cross-trade overall ranking. Requested trades
    /// span overlapping grains — sector `"23"` and its subgroup `"2382"`. CBP's
    /// request for `"23"` already contains `"2382"`'s establishments, so folding
    /// both into `overall` double-counts. `overall_ranking` reuses the blend's
    /// `covering_naics` guard: keep the covering code, drop the covered subset.
    #[test]
    fn overall_ranking_does_not_double_count_overlapping_naics_grains() {
        let sector = ("23".to_string(), vec![("Austin, TX".to_string(), 100i64)]);
        let subgroup = ("2382".to_string(), vec![("Austin, TX".to_string(), 40i64)]);
        let overall = overall_ranking(&[sector, subgroup]);
        // 100 (the covering sector's total), NOT 140 — the subgroup is subsumed.
        assert_eq!(overall.get("Austin, TX"), Some(&100));

        // Single-grain (the normal case) is untouched: two non-overlapping codes
        // both contribute to the combined total.
        let a = (
            "238210".to_string(),
            vec![("Austin, TX".to_string(), 30i64)],
        );
        let b = (
            "238220".to_string(),
            vec![("Austin, TX".to_string(), 40i64)],
        );
        let plain = overall_ranking(&[a, b]);
        assert_eq!(plain.get("Austin, TX"), Some(&70));
    }

    /// The anti-pattern: `updated_at` moving weekly (four apps re-derive the
    /// blend) over 2021/2022 stock data, so a consumer reading freshness off the
    /// envelope concludes the market numbers are current.
    #[test]
    fn blend_rows_name_the_vintage_of_every_input() {
        let bases = BTreeMap::from([("CA".to_string(), test_base(10_000))]);
        let ages = vec![
            band("23", "06", "55 to 64", 40),
            band("23", "06", "25 to 54", 60),
        ];
        let velocity = vec![json!({
            "sector": "NAICS23", "geo": "US", "t12m_applications": 1320.0,
            "as_of_period": "2026-06", "grain": "naics_sector_national",
        })];
        let items = blend_market(
            &[emp("238220", "state", "CA", "06", 100)],
            &[solo("2382", "CA", "06", 300)],
            &bases,
            &ages,
            &velocity,
        );
        assert_eq!(
            items[0].1["vintages"],
            json!({
                "employer_cbp_year": "2022",
                "solo_nes_year": "2021",
                "owner_age_nesd_year": "2021",
                "formation_bfs_as_of": "2026-06",
                "base_acs_year": "2022",
            })
        );
        // Absent inputs are Null in the block, never a fabricated vintage — and
        // the block itself is always present, so a reader cannot mistake "no
        // vintage recorded" for "no such field in this build".
        let bare = blend_market(
            &[emp("238220", "state", "CA", "06", 100)],
            &[solo("2382", "CA", "06", 300)],
            &BTreeMap::new(),
            &[],
            &[],
        );
        let v = &bare[0].1["vintages"];
        assert_eq!(v["employer_cbp_year"], "2022");
        assert!(v["owner_age_nesd_year"].is_null());
        assert!(v["formation_bfs_as_of"].is_null());
        assert!(v["base_acs_year"].is_null());
        // No derivation timestamp: it would enter the change-detection hash and
        // mark every row `changed` on every re-derive.
        assert!(v.get("derived_at").is_none() && bare[0].1.get("derived_at").is_none());
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

    // ── N35: the atlas ───────────────────────────────────────────────────────

    /// A blend over three states and one county each, with a base for every
    /// place so every cell gets a per-10k.
    fn atlas_fixture() -> (Vec<(String, Value)>, Vec<Value>) {
        // (state fips, county fips, state employers, county employers).
        let places = [
            ("06", "037", 1_000, 400), // CA — biggest state market
            ("48", "201", 800, 300),   // TX
            ("12", "086", 600, 200),   // FL
            ("56", "021", 5, 2),       // WY — outside any sane top-K
        ];
        let mut employers = Vec::new();
        let mut solos = Vec::new();
        let mut bases: BTreeMap<String, PlaceBase> = BTreeMap::new();
        for (st, county, state_estab, county_estab) in places {
            employers.push(emp(
                "238220",
                "state",
                census_common::state_abbr(st),
                st,
                state_estab,
            ));
            employers.push(county_emp("238220", st, county, county_estab));
            solos.push(solo("2382", census_common::state_abbr(st), st, state_estab));
            bases.insert(
                census_common::state_abbr(st).to_string(),
                test_base(100_000),
            );
            bases.insert(
                format!("{}·{county}", census_common::state_abbr(st)),
                test_base(10_000),
            );
        }
        let blend = blend_market(&employers, &solos, &bases, &[], &[]);
        // Saturation rows as `census/saturation` stores them.
        let sat = |place: &str, per_10k: f64| json!({ "place": place, "geo": "county", "per_10k": per_10k, "base": 10_000 });
        let saturation = vec![
            sat("CA·037", 40.0),
            sat("TX·201", 30.0),
            sat("FL·086", 20.0),
            sat("WY·021", 2.0),
        ];
        (blend, saturation)
    }

    /// **The N35 gate.** The atlas never contains a county outside the top-K
    /// states — the rail that bounds both the dataset and the CBP fan-out
    /// behind it. K=2 keeps CA and TX; FL's and WY's counties are absent, not
    /// ranked last.
    #[test]
    fn the_atlas_never_contains_a_county_outside_the_top_k_states() {
        let (blend, saturation) = atlas_fixture();
        let states = top_states_for_atlas(&blend, 2);
        assert_eq!(states.len(), 2);
        assert_eq!(states[0]["state_fips"], "06");
        assert_eq!(states[1]["state_fips"], "48");
        // The basis is published: this is not a formation ranking and says so.
        assert!(states[0]["ranked_by"]
            .as_str()
            .expect("a basis")
            .contains("NATIONAL-grain only"));

        let ranks: BTreeMap<String, usize> =
            BTreeMap::from([("06".to_string(), 1), ("48".to_string(), 2)]);
        let records = atlas_records(&blend, &county_saturation_index(&saturation), &ranks, 25);
        let keys: Vec<&str> = records.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["2382:06037", "2382:48201"]);
        assert!(
            !keys
                .iter()
                .any(|k| k.starts_with("2382:12") || k.starts_with("2382:56")),
            "a county outside the scoped states is not in the atlas at all"
        );

        // Every block is grain-labelled, and the solo half is the ASSUMED one.
        let (_, ca) = &records[0];
        assert_eq!(ca["grains"]["cell"], "naics4|county");
        assert_eq!(ca["grains"]["employer"], "county");
        assert_eq!(ca["grains"]["solo"], "state_carried");
        assert_eq!(ca["grains"]["saturation"], "county");
        assert_eq!(ca["rank_by_saturation"], 1);
        assert_eq!(ca["rank_by_total_market_per_10k"], 1);
        assert_eq!(ca["saturation_per_10k"], json!(40.0));
        assert_eq!(
            ca["metro"]["cbsa_title"],
            "Los Angeles-Long Beach-Anaheim, CA"
        );
        assert_eq!(ca["state"], "CA");
        assert!(
            ca["solo_operators"].is_null(),
            "no fabricated county solo count"
        );
    }

    /// `top_n` cuts each ranking independently: a county that made only ONE of
    /// the two rankings carries a Null for the other rather than a position it
    /// never reached.
    #[test]
    fn a_county_that_made_one_ranking_gets_a_null_for_the_other() {
        let (blend, saturation) = atlas_fixture();
        let ranks: BTreeMap<String, usize> = BTreeMap::from([
            ("06".to_string(), 1),
            ("48".to_string(), 2),
            ("12".to_string(), 3),
        ]);
        // Only the single best county per ranking survives; both rankings agree
        // here, so exactly one record is emitted.
        let top1 = atlas_records(&blend, &county_saturation_index(&saturation), &ranks, 1);
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].0, "2382:06037");

        // With no saturation rows at all, the saturation ranking is empty and
        // every surviving record says so — Null, not rank 0.
        let no_sat = atlas_records(&blend, &BTreeMap::new(), &ranks, 25);
        assert_eq!(no_sat.len(), 3);
        for (_, v) in &no_sat {
            assert!(v["rank_by_saturation"].is_null());
            assert!(v["saturation_per_10k"].is_null());
            assert!(!v["rank_by_total_market_per_10k"].is_null());
        }
    }

    /// **The N35 gate.** A fixture run's `metro_pricing` plan lists the expected
    /// CBSA names — one entry per METRO however many of its counties ranked,
    /// capped, with the counties it was built from on the record.
    #[test]
    fn the_metro_plan_lists_the_expected_cbsa_names_and_never_invents_one() {
        let (blend, saturation) = atlas_fixture();
        let ranks: BTreeMap<String, usize> = BTreeMap::from([
            ("06".to_string(), 1),
            ("48".to_string(), 2),
            ("12".to_string(), 3),
        ]);
        let records = atlas_records(&blend, &county_saturation_index(&saturation), &ranks, 25);
        let plan = metro_pricing_plan(&records, 5, "0 0 7 1 1,4,7,10 *", Some(2.0));
        let names: Vec<&str> = plan
            .entries
            .iter()
            .map(|e| e["cbsa_title"].as_str().expect("a title"))
            .collect();
        assert_eq!(
            names,
            vec![
                "Los Angeles-Long Beach-Anaheim, CA",
                "Houston-The Woodlands-Sugar Land, TX",
                "Miami-Fort Lauderdale-Pompano Beach, FL",
            ],
            "in atlas rank order"
        );
        assert_eq!(plan.unmapped_counties, 0);
        let la = &plan.entries[0];
        assert_eq!(la["cbsa_code"], "31080");
        assert_eq!(la["counties"], json!(["06037"]));
        assert_eq!(la["locality"], "Los Angeles-Long Beach-Anaheim, CA");
        // The schedule body is a `POST /schedules` body, budget rail included.
        assert_eq!(la["schedule"]["app"], "homewyse-pricing");
        assert_eq!(la["schedule"]["params"]["locality"], la["locality"]);
        assert_eq!(la["schedule"]["budget_usd"], json!(2.0));
        assert_eq!(la["schedule"]["cron"], "0 0 7 1 1,4,7,10 *");

        // The cap TRUNCATES and says so, rather than dropping metros silently.
        let capped = metro_pricing_plan(&records, 1, "0 0 7 1 1,4,7,10 *", None);
        assert_eq!(capped.entries.len(), 1);
        assert_eq!(capped.truncated, 2);
        assert!(
            capped.entries[0]["schedule"].get("budget_usd").is_none(),
            "no ceiling asked for means the key is absent, not 0"
        );

        // A county the crosswalk does not know is COUNTED, never substituted.
        let unknown = vec![(
            "2382:06003".to_string(),
            json!({ "geo_fips": "06003", "naics4": "2382", "metro": Value::Null,
                    "rank_by_saturation": 1 }),
        )];
        let plan = metro_pricing_plan(&unknown, 5, "* * * * * *", None);
        assert!(plan.entries.is_empty());
        assert_eq!(plan.unmapped_counties, 1);
    }

    /// `[census]` must actually BIND: the operator's section reaches the atlas,
    /// and a per-run param can still narrow it.
    #[test]
    fn the_census_section_binds_and_params_narrow_it() {
        let shipped = AtlasSettings::default();
        assert_eq!(shipped.states_k, 10, "[census] atlas_states_k default");
        assert!(
            !shipped.metro_pricing,
            "the metered driver ships OFF — the plan is reported, not bought"
        );
        let operator = AtlasSettings::from_config(&pumper_core::config::CensusConfig {
            atlas_states_k: 3,
            metro_pricing: true,
            ..pumper_core::config::CensusConfig::default()
        });
        assert_eq!(operator.states_k, 3);
        assert!(operator.metro_pricing);
        // A zero would scope the atlas to nothing and call it a ranking.
        let floored = AtlasSettings::from_config(&pumper_core::config::CensusConfig {
            atlas_states_k: 0,
            atlas_top_n: 0,
            ..pumper_core::config::CensusConfig::default()
        });
        assert_eq!((floored.states_k, floored.top_n), (1, 1));
    }

    /// End to end through a real store: the atlas is written into the `census`
    /// namespace with a derived stamp, and the pricing driver asks for nothing
    /// while it is off.
    #[tokio::test]
    async fn the_atlas_lands_in_the_census_namespace_and_buys_nothing_by_default() {
        let store = pumper_core::testing::TempStore::new("census-atlas").await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "census-density").build();
        let (blend, saturation) = atlas_fixture();
        let out = sync_atlas(&ctx, &blend, &saturation, &AtlasSettings::default())
            .await
            .expect("atlas");
        assert_eq!(out["dataset"], "census/atlas");
        assert_eq!(out["states_k"], 10);
        assert_eq!(out["counties_ranked"], 4);
        assert_eq!(out["metro_pricing"]["enabled"], false);
        assert_eq!(
            out["metro_pricing"]["requested"], 0,
            "the plan is computed; nothing is scheduled until an operator says so"
        );
        assert_eq!(
            out["metro_pricing"]["plan"].as_array().expect("plan").len(),
            4
        );
        assert!(ctx.take_schedule_requests().is_empty());

        let rec = ctx
            .datasets
            .get(MARKET_APP, ATLAS_DATASET, "2382:06037")
            .await
            .expect("read")
            .expect("record");
        assert_eq!(rec.data["grains"]["solo"], "state_carried");
        let revs = ctx
            .datasets
            .history(MARKET_APP, ATLAS_DATASET, "2382:06037", 10)
            .await
            .expect("history");
        let p = &revs.first().expect("one revision").provenance;
        assert_eq!(p.job_id.as_deref(), Some(&*ctx.job_id.to_string()));
        let url = p.source_url.as_deref().expect("derived source_url");
        assert!(url.starts_with("derived://census/atlas?"), "{url}");
        assert!(!p.replayable());

        // Driver ON: the run ASKS the runtime for one schedule per planned
        // metro, and says how many it asked for.
        let settings = AtlasSettings {
            metro_pricing: true,
            metros: 2,
            ..AtlasSettings::default()
        };
        let out = sync_atlas(&ctx, &blend, &saturation, &settings)
            .await
            .expect("atlas");
        assert_eq!(out["metro_pricing"]["requested"], 2);
        assert_eq!(out["metro_pricing"]["metros_truncated"], 2);
        let asked = ctx.take_schedule_requests();
        assert_eq!(asked.len(), 2);
        assert_eq!(asked[0]["app"], "homewyse-pricing");
    }
}
