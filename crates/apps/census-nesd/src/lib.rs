//! US solo-trades OWNER-AGE demographics via Census **Nonemployer Statistics by
//! Demographics (NES-D)** — the succession-wave input.
//!
//! NES-D publishes owner characteristics (age, sex, ethnicity, race, veteran
//! status) of nonemployer firms per NAICS × state. We pull the **owner age
//! band** composition for the same 4-digit trade codes census-nonemp tracks, so
//! the density blend can compute where a retirement wave of solo trades
//! operators will hit (`pct_owners_55plus`, `succession_receipts`). Upserted
//! into the `owner_age` dataset. Fast path — GET JSON API, no HTML, no browser.
//!
//! Data type: OWNER DEMOGRAPHICS (owners of nonemployer firms, by age band).
//! Access: FREE Census key (shared with census-density/census-nonemp;
//! `params.api_key` or env `CENSUS_API_KEY`). A Ledgerline trades-intelligence
//! consumer; cataloged in `catalog/data-sources.toml` because it is on the
//! scheduler (the drift gate cross-checks the cron).
//!
//! Contract notes (verified 2026-07-30 against api.census.gov/data.json and the
//! key-free variable dictionary; data rows are key-gated — a keyless request
//! 302s to missing_key.html exactly like CBP/NES, so the row shape is
//! re-verified on the first keyed run): the NES-D *owner characteristics* table
//! lives at `https://api.census.gov/data/{year}/absnesdo` (NOT `…/nesd` — that
//! path does not exist in the discovery doc). Success is a JSON
//! array-of-arrays, row 0 the header, columns matched by NAME. Variables:
//! `OWNNOPD` (number of owners of nonemployer firms), `OWNNOPD_PCT`, question
//! code `QDESC`/`QDESC_LABEL` (age rows carry the OWNRAGE question), band code
//! `OWNCHAR`/`OWNCHAR_LABEL` ("Under 25" … "65 or over" plus structural
//! "Total reporting"/"Item not reported" rows), and per-demographic
//! `OWNER_SEX/ETH/RACE/VET` (+`_LABEL`). We keep only the all-demographics
//! rows (each `OWNER_*_LABEL` = "Total…") of the age question, and only
//! *reported* age bands. The 2021 vintage exposes `NAICS2017`; later vintages
//! are expected to switch classification (mirroring census-nonemp's year gate)
//! — override with `params.naics_var` if a release deviates. Suppressed cells
//! are negative sentinels / blanks → dropped, never counted as zero owners.
//! Age bands are coarse — the derived index is a *wave-size indicator*, not a
//! per-business prediction, and the dataset says so.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

pub struct CensusNesd;

const DEFAULT_YEAR: &str = "2021";
/// Substring (case-insensitive) identifying the owner-age question among the
/// NES-D `QDESC_LABEL` values. Overridable via `params.age_question`.
const DEFAULT_AGE_QUESTION: &str = "OWNRAGE";

/// Same 4-digit trade codes as census-nonemp — the blend joins on them.
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
impl ScrapeApp for CensusNesd {
    fn name(&self) -> &'static str {
        "census-nesd"
    }

    fn description(&self) -> &'static str {
        "US solo-trades owner-age demographics from Census Nonemployer Statistics by \
         Demographics (NES-D, absnesdo JSON API). Owners of nonemployer firms per age \
         band × trade NAICS × state, upserted into the `owner_age` dataset — the input \
         for the succession-wave fields on `census/market_blend`. Requires a FREE \
         Census API key (params.api_key or env CENSUS_API_KEY). Params: {\"year\": \
         \"2021\", \"states\": \"06,12,48\" (FIPS list; default all), \"naics\": \
         [\"2382\",\"5617\"] (4-digit), \"naics_var\": \"NAICS2017\", \
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

        let trades: Vec<(String, String)> = match ctx.params.get("naics").and_then(Value::as_array)
        {
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

        let mut all_records: Vec<(String, Value)> = Vec::new();
        let mut trade_summaries: Vec<Value> = Vec::new();

        for (naics, label) in &trades {
            let url = format!(
                "https://api.census.gov/data/{year}/absnesdo?get=OWNNOPD,OWNNOPD_PCT,QDESC_LABEL,OWNCHAR,OWNCHAR_LABEL,OWNER_SEX_LABEL,OWNER_ETH_LABEL,OWNER_RACE_LABEL,OWNER_VET_LABEL&{for_clause}&{naics_var}={naics}&key={api_key}"
            );
            let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
            // 204 No Content (fully suppressed at this level) → a note, not a
            // failed run.
            if resp.status == 204 || resp.body.trim().is_empty() {
                trade_summaries.push(json!({
                    "naics": naics, "label": label,
                    "note": "no data — NES-D owner-age figures suppressed at this level",
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
            ctx.save_artifact(
                &format!("nesd-{naics}.json"),
                &serde_json::to_vec_pretty(&rows)?,
            )
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
                demo_labels: ["OWNER_SEX_LABEL", "OWNER_ETH_LABEL", "OWNER_RACE_LABEL",
                    "OWNER_VET_LABEL"]
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

            trade_summaries.push(json!({
                "naics": naics,
                "label": label,
                "states_reported": rollup.bands_by_state.len(),
                "age_band_records": rollup.records.len(),
                "top_states_by_pct_owners_55plus": by_share.iter().take(5)
                    .map(|(s, share)| json!({ "state": s, "pct_owners_55plus": share }))
                    .collect::<Vec<_>>(),
            }));
            all_records.extend(rollup.records);
        }

        let summary = ctx.upsert_many("owner_age", &all_records).await?;

        // Re-derive the blended `census/market_blend` (adds/refreshes the
        // succession fields). Degrades gracefully when the other Census apps
        // have never run.
        let market_blend = match app_census_density::sync_market_blend(&ctx).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        Ok(json!({
            "source": format!("census/absnesdo/{year}"),
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

/// Per-trade rollup of the age rows: dataset records keyed
/// `{naics}:{state_fips}:{band_code}`, the per-state band lists the summary
/// share math runs on, and the distinct question labels seen (diagnostics).
struct AgeRollup {
    records: Vec<(String, Value)>,
    /// state abbr → (band label, owners) for reported bands.
    bands_by_state: BTreeMap<String, Vec<(String, i64)>>,
    questions_seen: BTreeSet<String>,
}

/// Map the NES-D array-of-arrays payload into per-state × age-band records for
/// one trade NAICS. Keeps only: the owner-age question, the all-demographics
/// ("Total") slice, *reported* age bands, and unsuppressed owner counts.
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
    let want_q = age_question.to_uppercase();

    for row in rows.iter().skip(1) {
        let q = row.get(cols.qdesc_label).cloned().unwrap_or_default();
        if !q.to_uppercase().contains(&want_q) {
            questions_seen.insert(q);
            continue;
        }
        // Only the all-demographics slice: every present OWNER_*_LABEL must be
        // its "Total…" roll-up, otherwise the same owners are counted once per
        // sex/ethnicity/race/veteran cross-tab.
        let all_total = cols.demo_labels.iter().all(|&i| {
            row.get(i)
                .map(|v| v.trim().to_lowercase().starts_with("total"))
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
            format!("{naics}:{st_fips}:{band_code}"),
            json!({
                "naics": naics,
                "trade": label,
                "state": state,
                "state_fips": st_fips,
                "age_band_code": band_code,
                "age_band": band,
                "owners": owners,
                "owners_pct": owners_pct,
                "basis": "owners_of_nonemployer_firms",
                // Coarse bands: a wave-size indicator, not a per-business
                // prediction — consumers must not present it as one.
                "grain": "owner_age_band",
                "year": year,
            }),
        ));
    }

    AgeRollup {
        records,
        bands_by_state,
        questions_seen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        map_age_rows(&rows(data), &cols(), "2382", "Building equipment", "2021", "OWNRAGE")
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
        assert_eq!(key, "2382:06:AG04");
        assert_eq!(v["owners"], 100);
        assert_eq!(v["age_band"], "55 to 64");
        assert_eq!(v["state"], "CA");
        assert_eq!(v["grain"], "owner_age_band");
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
