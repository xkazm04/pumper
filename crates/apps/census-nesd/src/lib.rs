//! US solo-trades OWNER-AGE demographics via Census **Nonemployer Statistics by
//! Demographics (NES-D)** — the succession-wave input.
//!
//! NES-D publishes owner characteristics (age, sex, ethnicity, race, veteran
//! status) of nonemployer firms per NAICS × state — **2-digit sector NAICS
//! only**. We pull the **owner age band** composition for the sectors covering
//! the trades census-nonemp tracks (23 Construction, 56 Admin & support/waste),
//! so the density blend can compute where a retirement wave of solo trades
//! operators will hit (`pct_owners_55plus`, `succession_receipts` — sector
//! grain, labeled so). Upserted into the `owner_age` dataset. Fast path — GET
//! JSON API, no HTML, no browser.
//!
//! Data type: OWNER DEMOGRAPHICS (owners of nonemployer firms, by age band).
//! Access: FREE Census key (shared with census-density/census-nonemp;
//! `params.api_key` or env `CENSUS_API_KEY`). A Ledgerline trades-intelligence
//! consumer; cataloged in `catalog/data-sources.toml` because it is on the
//! scheduler (the drift gate cross-checks the cron).
//!
//! Contract notes (LIVE-verified 2026-07-30 with a real key): the NES-D *owner
//! characteristics* table lives at
//! `https://api.census.gov/data/{year}/absnesdo` (NOT `…/nesd` — that path
//! does not exist in the discovery doc). GRAIN: per-state data exists ONLY at
//! 2-digit sector NAICS — `NAICS2017=23&for=state:XX` returns real OWNRAGE
//! rows, while `NAICS2017=238` and `=2381` return HTTP 204 No Content (a
//! contract-VALID empty response meaning "not published at this grain", not
//! drift and not an error). Success is a JSON array-of-arrays, row 0 the
//! header, columns matched by NAME. Variables: `OWNNOPD` (number of owners of
//! nonemployer firms), `OWNNOPD_PCT`, question code `QDESC`/`QDESC_LABEL` (age
//! rows carry the OWNRAGE question), band code `OWNCHAR`/`OWNCHAR_LABEL`
//! ("Under 25" … "65 or over" plus structural "Total reporting"/"Item not
//! reported" rows), and per-demographic `OWNER_SEX/ETH/RACE/VET` (+`_LABEL`).
//! We keep only the all-demographics rows (each `OWNER_*_LABEL` = "Total…") of
//! the age question, and only *reported* age bands. The 2021 vintage exposes
//! `NAICS2017`; later vintages are expected to switch classification
//! (mirroring census-nonemp's year gate) — override with `params.naics_var` if
//! a release deviates. Suppressed cells are negative sentinels / blanks →
//! dropped, never counted as zero owners. Age bands are coarse and the grain
//! is a whole SECTOR — the derived index is a *wave-size indicator*, not a
//! per-business or per-trade prediction, and the dataset says so.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
    UpsertSummary,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub struct CensusNesd;

const DEFAULT_YEAR: &str = "2021";
/// Substring (case-insensitive) identifying the owner-age question among the
/// NES-D `QDESC_LABEL` values. Overridable via `params.age_question`.
const DEFAULT_AGE_QUESTION: &str = "OWNRAGE";

/// 2-digit NAICS sectors covering the trades census-nonemp tracks (2382 → 23,
/// 5617 → 56). NES-D publishes per-state owner demographics at sector grain
/// ONLY — 3/4-digit requests return HTTP 204 (not published at that grain).
/// The blend joins by the trade group's 2-digit sector prefix.
const DEFAULT_SECTORS: &[(&str, &str)] = &[
    (
        "23",
        "Construction (incl. plumbing, HVAC, electrical trades)",
    ),
    (
        "56",
        "Administrative & support and waste management services (incl. landscaping, pool)",
    ),
];

#[async_trait]
impl ScrapeApp for CensusNesd {
    fn name(&self) -> &'static str {
        "census-nesd"
    }

    fn description(&self) -> &'static str {
        "US solo-trades owner-age demographics from Census Nonemployer Statistics by \
         Demographics (NES-D, absnesdo JSON API). Owners of nonemployer firms per age \
         band × 2-digit NAICS SECTOR × state (NES-D publishes per-state data at \
         sector grain only; finer NAICS → HTTP 204 = not published), upserted into \
         the `owner_age` dataset — the sector-grain input for the succession-wave \
         fields on `census/market_blend`. Requires a FREE Census API key \
         (params.api_key or env CENSUS_API_KEY). Params: {\"year\": \"2021\", \
         \"states\": \"06,12,48\" (FIPS list; default all), \"naics\": \
         [\"23\",\"56\"] (2-digit sectors), \"naics_var\": \"NAICS2017\", \
         \"age_question\": \"OWNRAGE\", \"api_key\": \"...\"}"
    }

    // Shared free Census key; env var is the readiness signal for scheduled runs.
    fn requires(&self) -> &'static [pumper_core::Requirement] {
        &[pumper_core::Requirement::Env("CENSUS_API_KEY")]
    }

    // NES-D is an annual release (~mid-year, lagging ~2 years). Yearly refresh:
    // 06:00:00 on June 20 (sec min hour dom mon dow).
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 6 20 6 *")
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
                        "description": "NES-D vintage (lags ~2 years; 2021 is the live one). Also picks the classification predicate: >= 2022 → NAICS2022, else NAICS2017."
                    },
                    "states": {
                        "type": "string",
                        "description": "Comma-separated state FIPS list (e.g. \"06,12,48\"). Empty or \"*\" = all states."
                    },
                    "naics": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "2-DIGIT NAICS sectors. NES-D publishes per-state owner demographics at sector grain only — 3/4-digit codes return HTTP 204 (not published), which is counted, not an error."
                    },
                    "naics_var": {
                        "type": "string",
                        "description": "Override the classification predicate (NAICS2017 / NAICS2022) when a release deviates from the year rule."
                    },
                    "age_question": {
                        "type": "string",
                        "description": "QDESC_LABEL predicate selecting the owner-age question (default OWNRAGE). Pinning it is required — without it the API returns an unstable question subset."
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
                    description:
                        "Annual refresh: owner-age bands for both trade sectors, all states",
                    params: json!({ "year": DEFAULT_YEAR }),
                },
                ManifestExample {
                    description: "Construction sector only, three states",
                    params: json!({
                        "year": DEFAULT_YEAR,
                        "states": "06,48,12",
                        "naics": ["23"]
                    }),
                },
            ],
            output_shape: Some(
                "{source, year, grain: \"naics_sector\", sectors: [{sector, label, \
                 states_reported, age_band_records, suppressed: {owner_cells}, \
                 top_states_by_pct_owners_55plus} | {sector, label, note}], \
                 sectors_not_published, suppression, market_blend, index_datasets, records, \
                 new, changed, unchanged} — a sector with no published per-state data \
                 (HTTP 204) is counted in sectors_not_published, never an error; withheld \
                 owner cells are dropped (never 0 owners) and counted in `suppression`",
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
        let age_question = ctx
            .params
            .get("age_question")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_AGE_QUESTION)
            .to_string();

        // params.naics overrides everything; otherwise the governed
        // `trades/taxonomy` registry drives the list at NES-D's 2-digit sector
        // grain (an enabled trade's sector joins on the next run). Registry
        // absent/empty ⇒ compile-time DEFAULT_SECTORS, exactly as before.
        let label_for = |c: &str| -> String {
            DEFAULT_SECTORS
                .iter()
                .find(|(k, _)| *k == c)
                .map(|(_, l)| l.to_string())
                .unwrap_or_else(|| c.to_string())
        };
        let sectors: Vec<(String, String)> = match ctx.params.get("naics").and_then(Value::as_array)
        {
            Some(arr) => arr
                .iter()
                .filter_map(Value::as_str)
                .map(|c| (c.to_string(), label_for(c)))
                .collect(),
            None => match trades_common::taxonomy::registry_naics(&ctx, 2).await? {
                Some(codes) => codes
                    .into_iter()
                    .map(|c| {
                        let l = label_for(&c);
                        (c, l)
                    })
                    .collect(),
                None => DEFAULT_SECTORS
                    .iter()
                    .map(|(c, l)| (c.to_string(), l.to_string()))
                    .collect(),
            },
        };

        let api_key = census_common::api_key(&ctx, "census-nesd")?;

        let for_clause = if states.is_empty() || states == "*" {
            "for=state:*".to_string()
        } else {
            format!("for=state:{states}")
        };

        // Classification vintage gate, mirroring census-nonemp: the 2021 NES-D
        // dictionary exposes NAICS2017; 2022+ vintages are expected on the 2022
        // classification. params.naics_var overrides both.
        let naics_var = match ctx.params.get("naics_var").and_then(Value::as_str) {
            Some(v) => v.to_string(),
            None => match year.parse::<u32>() {
                Ok(y) if y >= 2022 => "NAICS2022".to_string(),
                _ => "NAICS2017".to_string(),
            },
        };

        // Provenance (M12) is per-request — one URL and one archived artifact
        // per sector — so each sector's rows carry their own stamp and the run
        // reports one merged rollup instead of one anonymous batch.
        let mut summary = UpsertSummary::default();
        let mut record_count = 0usize;
        let mut sector_summaries: Vec<Value> = Vec::new();
        let mut not_published: usize = 0;
        // Run-level suppression telemetry: what the API declined to tell us.
        let mut run_suppressed_owner_cells = 0usize;

        for (naics, label) in &sectors {
            let url = format!(
                // LIVE-VERIFIED 2026-07-30: without an explicit QDESC_LABEL
                // predicate the API returns an unstable subset of questions
                // (a live run saw only USBORN/USCITIZEN rows for a query that
                // moments earlier included OWNRAGE). Pinning the question as a
                // predicate is deterministic and shrinks the payload; the
                // echoed QDESC_LABEL column keeps the parse unchanged.
                "https://api.census.gov/data/{year}/absnesdo?get=OWNNOPD,OWNNOPD_PCT,OWNCHAR,OWNCHAR_LABEL,OWNER_SEX_LABEL,OWNER_ETH_LABEL,OWNER_RACE_LABEL,OWNER_VET_LABEL&{for_clause}&{naics_var}={naics}&QDESC_LABEL={age_question}&key={api_key}"
            );
            let resp = ctx
                .engines
                .http
                .fetch(HttpRequest::get(url.clone()))
                .await?;
            // HTTP 204 No Content is contract-VALID: NES-D per-state data only
            // exists at 2-digit sector grain, so a finer (or unpublished) code
            // is simply "not published at this grain" — a stat, never an error.
            // Shared with the three sibling apps (`is_empty_answer`) so the
            // guard cannot drift out of one of them again.
            if census_common::is_empty_answer(resp.status, &resp.body) {
                not_published += 1;
                sector_summaries.push(json!({
                    "sector": naics, "label": label,
                    "note": "not published at this grain (HTTP 204 — NES-D serves \
                             2-digit sector NAICS per state only)",
                }));
                continue;
            }
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "Census NES-D {year} NAICS {naics}: HTTP {} (body starts: {})",
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
                    "Census NES-D {year} NAICS {naics}: response was not JSON{hint} \
                     (starts: {})",
                    resp.body.chars().take(160).collect::<String>()
                )));
            }
            let rows: Vec<Vec<String>> = serde_json::from_str(&resp.body).map_err(|e| {
                Error::App(format!(
                    "Census NES-D {year} NAICS {naics}: bad JSON rows: {e}"
                ))
            })?;
            // Bind the archived bytes once: `artifact_sha` must hash exactly what
            // was stored, never a re-serialization of it.
            let artifact = serde_json::to_vec_pretty(&rows)?;
            ctx.save_artifact(&format!("nesd-{naics}.json"), &artifact)
                .await?;

            let header = rows.first().cloned().unwrap_or_default();
            let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
            let cols = AgeCols {
                owners: idx("OWNNOPD").ok_or_else(|| {
                    Error::App(format!(
                        "Census NES-D NAICS {naics}: no OWNNOPD column in {header:?}"
                    ))
                })?,
                owners_pct: idx("OWNNOPD_PCT"),
                qdesc_label: idx("QDESC_LABEL").ok_or_else(|| {
                    Error::App(format!(
                        "Census NES-D NAICS {naics}: no QDESC_LABEL column in {header:?}"
                    ))
                })?,
                band_code: idx("OWNCHAR").ok_or_else(|| {
                    Error::App(format!(
                        "Census NES-D NAICS {naics}: no OWNCHAR column in {header:?}"
                    ))
                })?,
                band_label: idx("OWNCHAR_LABEL").ok_or_else(|| {
                    Error::App(format!(
                        "Census NES-D NAICS {naics}: no OWNCHAR_LABEL column in {header:?}"
                    ))
                })?,
                demo_labels: [
                    "OWNER_SEX_LABEL",
                    "OWNER_ETH_LABEL",
                    "OWNER_RACE_LABEL",
                    "OWNER_VET_LABEL",
                ]
                .iter()
                .filter_map(|n| idx(n))
                .collect(),
                state: idx("state").ok_or_else(|| {
                    Error::App(format!(
                        "Census NES-D NAICS {naics}: no state column in {header:?}"
                    ))
                })?,
            };

            let rollup = map_age_rows(&rows, &cols, naics, label, &year, &age_question);
            // The question filter matching nothing while data rows exist means
            // the QDESC vocabulary shifted — fail loudly with what WAS seen so
            // the operator can set params.age_question, instead of silently
            // upserting an empty vintage.
            if rollup.records.is_empty() && rows.len() > 1 {
                return Err(Error::App(format!(
                    "Census NES-D {year} NAICS {naics}: no rows matched age question \
                     '{age_question}' — QDESC_LABEL values seen: {:?}",
                    rollup.questions_seen
                )));
            }

            // Per-state 55+ share for the trade summary (the durable share math
            // itself lives in the census/market_blend join).
            let mut by_share: Vec<(String, f64)> = rollup
                .bands_by_state
                .iter()
                .filter_map(|(state, bands)| {
                    census_common::owner_age_share_55plus(bands)
                        .map(|s| (state.clone(), (s * 10_000.0).round() / 10_000.0))
                })
                .collect();
            by_share.sort_by(|a, b| b.1.total_cmp(&a.1));

            sector_summaries.push(json!({
                "sector": naics,
                "label": label,
                "states_reported": rollup.bands_by_state.len(),
                "age_band_records": rollup.records.len(),
                // Age bands the API withheld — the share below is computed over
                // the bands that WERE reported, not over the whole sector.
                "suppressed": { "owner_cells": rollup.suppressed_owner_cells },
                "top_states_by_pct_owners_55plus": by_share.iter().take(5)
                    .map(|(s, share)| json!({ "state": s, "pct_owners_55plus": share }))
                    .collect::<Vec<_>>(),
            }));
            record_count += rollup.records.len();
            run_suppressed_owner_cells += rollup.suppressed_owner_cells;
            census_common::merge_summary(
                &mut summary,
                ctx.upsert_many_with_provenance(
                    "owner_age",
                    &rollup.records,
                    census_common::http_provenance(&url, &artifact),
                )
                .await?,
            );
        }

        // Re-derive the blended `census/market_blend` (adds/refreshes the
        // succession fields). Degrades gracefully when the other Census apps
        // have never run.
        let market_blend = match app_census_density::sync_market_blend(&ctx).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        // `with_product_index` puts `census/market_blend` + `census/saturation`
        // in the worker's index + hook scope for this run — see
        // `census_common::product_index_datasets`.
        Ok(census_common::with_product_index(json!({
            "source": format!("census/absnesdo/{year}"),
            "year": year,
            "grain": "naics_sector",
            "sectors": sector_summaries,
            "sectors_not_published": not_published,
            "suppression": { "owner_cells": run_suppressed_owner_cells },
            "market_blend": market_blend,
            "records": record_count,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        })))
    }
}

/// Pre-resolved column indices for the NES-D owner-characteristics payload.
struct AgeCols {
    owners: usize,
    owners_pct: Option<usize>,
    qdesc_label: usize,
    band_code: usize,
    band_label: usize,
    /// Whichever of the four OWNER_*_LABEL columns are present; a row must be
    /// the "Total" slice of each to count (all-demographics age composition).
    demo_labels: Vec<usize>,
    state: usize,
}

/// Per-sector rollup of the age rows: dataset records keyed
/// `{state_fips}|{sector}|{band_code}`, the per-state band lists the summary
/// share math runs on, and the distinct question labels seen (diagnostics).
struct AgeRollup {
    records: Vec<(String, Value)>,
    /// state abbr → (band label, owners) for reported bands.
    bands_by_state: BTreeMap<String, Vec<(String, i64)>>,
    questions_seen: BTreeSet<String>,
    /// Age-question rows of the all-demographics slice whose OWNNOPD cell was
    /// suppressed: the band exists, the owner count is withheld. Dropped (never
    /// stored as 0 owners) — and counted, because a share computed over five
    /// bands means something different when three more were withheld.
    suppressed_owner_cells: usize,
}

/// Map the NES-D array-of-arrays payload into per-state × age-band records for
/// one 2-digit NAICS sector. Keeps only: the owner-age question, the
/// all-demographics ("Total") slice, *reported* age bands, and unsuppressed
/// owner counts.
fn map_age_rows(
    rows: &[Vec<String>],
    cols: &AgeCols,
    naics: &str,
    label: &str,
    year: &str,
    age_question: &str,
) -> AgeRollup {
    let mut records: Vec<(String, Value)> = Vec::new();
    let mut bands_by_state: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
    let mut questions_seen: BTreeSet<String> = BTreeSet::new();
    let mut suppressed_owner_cells = 0usize;
    let want_q = age_question.to_uppercase();

    for row in rows.iter().skip(1) {
        let q = row.get(cols.qdesc_label).cloned().unwrap_or_default();
        if !q.to_uppercase().contains(&want_q) {
            questions_seen.insert(q);
            continue;
        }
        // Only the all-demographics slice: every present OWNER_*_LABEL must be
        // its roll-up value, otherwise the same owners are counted once per
        // sex/ethnicity/race/veteran cross-tab. LIVE-VERIFIED 2026-07-30: the
        // real roll-up label is "All owners of nonemployer firms" (not
        // "Total…" — that guess silently dropped every OWNRAGE row).
        let all_total = cols.demo_labels.iter().all(|&i| {
            row.get(i)
                .map(|v| {
                    let v = v.trim().to_lowercase();
                    v.starts_with("all") || v.starts_with("total")
                })
                .unwrap_or(true)
        });
        if !all_total {
            continue;
        }
        let band = row.get(cols.band_label).cloned().unwrap_or_default();
        if !census_common::is_reported_age_band(&band) {
            continue;
        }
        // Suppressed owner counts (negative sentinels / blanks / "D") are
        // dropped, never stored as zero owners.
        let Some(owners) = census_common::census_num(row.get(cols.owners)) else {
            suppressed_owner_cells += 1;
            continue;
        };
        let band_code = row.get(cols.band_code).cloned().unwrap_or_default();
        let owners_pct = cols
            .owners_pct
            .and_then(|i| row.get(i))
            .and_then(|s| s.trim().parse::<f64>().ok())
            .filter(|v| *v >= 0.0)
            .map(Value::from)
            .unwrap_or(Value::Null);
        let st_fips = row.get(cols.state).cloned().unwrap_or_default();
        let state = census_common::state_abbr(&st_fips).to_string();

        bands_by_state
            .entry(state.clone())
            .or_default()
            .push((band.clone(), owners));

        records.push((
            format!("{st_fips}|{naics}|{band_code}"),
            json!({
                "sector": naics,
                "sector_label": label,
                "state": state,
                "state_fips": st_fips,
                "age_band_code": band_code,
                "age_band": band,
                "owners": owners,
                "owners_pct": owners_pct,
                "basis": "owners_of_nonemployer_firms",
                // Coarse bands at whole-sector grain: a wave-size indicator,
                // not a per-business or per-trade prediction — consumers must
                // not present it as one.
                "grain": "naics_sector_owner_age_band",
                "year": year,
            }),
        ));
    }

    AgeRollup {
        records,
        bands_by_state,
        questions_seen,
        suppressed_owner_cells,
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
        let app = CensusNesd;
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
            "census-nesd's run() must wrap its result exactly once with {needle}"
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

    // Header shaped like the real absnesdo payload (columns addressed by name
    // in run(); the tests pre-resolve the same indices).
    const HEADER: [&str; 10] = [
        "OWNNOPD",
        "OWNNOPD_PCT",
        "QDESC_LABEL",
        "OWNCHAR",
        "OWNCHAR_LABEL",
        "OWNER_SEX_LABEL",
        "OWNER_ETH_LABEL",
        "OWNER_RACE_LABEL",
        "OWNER_VET_LABEL",
        "state",
    ];

    fn cols() -> AgeCols {
        AgeCols {
            owners: 0,
            owners_pct: Some(1),
            qdesc_label: 2,
            band_code: 3,
            band_label: 4,
            demo_labels: vec![5, 6, 7, 8],
            state: 9,
        }
    }

    fn rows(data: &[[&str; 10]]) -> Vec<Vec<String>> {
        let mut out = vec![HEADER.iter().map(|s| s.to_string()).collect::<Vec<_>>()];
        out.extend(
            data.iter()
                .map(|r| r.iter().map(|c| c.to_string()).collect::<Vec<_>>()),
        );
        out
    }

    fn total4() -> [&'static str; 4] {
        ["Total", "Total", "Total", "Total"]
    }

    fn row(
        owners: &'static str,
        q: &'static str,
        code: &'static str,
        band: &'static str,
        demo: [&'static str; 4],
        st: &'static str,
    ) -> [&'static str; 10] {
        [
            owners, "10.0", q, code, band, demo[0], demo[1], demo[2], demo[3], st,
        ]
    }

    fn rollup(data: &[[&str; 10]]) -> AgeRollup {
        map_age_rows(
            &rows(data),
            &cols(),
            "23",
            "Construction",
            "2021",
            "OWNRAGE",
        )
    }

    #[test]
    fn keeps_only_the_age_question_total_slice_and_reported_bands() {
        let r = rollup(&[
            row("100", "OWNRAGE", "AG04", "55 to 64", total4(), "06"),
            // Different question → out (but remembered for diagnostics).
            row("500", "OWNRVET", "V01", "Veteran", total4(), "06"),
            // Sex-sliced age row → out (would double-count owners).
            row(
                "40",
                "OWNRAGE",
                "AG04",
                "55 to 64",
                ["Female", "Total", "Total", "Total"],
                "06",
            ),
            // Structural band → out.
            row("999", "OWNRAGE", "AG00", "Total reporting", total4(), "06"),
        ]);
        assert_eq!(r.records.len(), 1);
        let (key, v) = &r.records[0];
        assert_eq!(key, "06|23|AG04");
        assert_eq!(v["sector"], "23");
        assert_eq!(v["owners"], 100);
        assert_eq!(v["age_band"], "55 to 64");
        assert_eq!(v["state"], "CA");
        assert_eq!(v["grain"], "naics_sector_owner_age_band");
        assert!(r.questions_seen.contains("OWNRVET"));
    }

    #[test]
    fn suppressed_owner_counts_are_dropped_not_stored_as_zero() {
        let r = rollup(&[
            row("-666666666", "OWNRAGE", "AG04", "55 to 64", total4(), "56"),
            row("", "OWNRAGE", "AG05", "65 or over", total4(), "56"),
            row("25", "OWNRAGE", "AG02", "25 to 34", total4(), "56"),
        ]);
        assert_eq!(r.records.len(), 1);
        assert_eq!(r.records[0].1["owners"], 25);
        // The suppressed 55+ bands must not appear as 0-owner bands in the
        // share input either.
        assert_eq!(r.bands_by_state["WY"], vec![("25 to 34".to_string(), 25)]);
    }

    #[test]
    fn share_math_over_the_rollup_matches_the_reported_bands() {
        let r = rollup(&[
            row("60", "OWNRAGE", "AG02", "25 to 54", total4(), "06"),
            row("30", "OWNRAGE", "AG04", "55 to 64", total4(), "06"),
            row("10", "OWNRAGE", "AG05", "65 or over", total4(), "06"),
        ]);
        let share = census_common::owner_age_share_55plus(&r.bands_by_state["CA"]).unwrap();
        assert!((share - 0.4).abs() < 1e-9);
    }
}
