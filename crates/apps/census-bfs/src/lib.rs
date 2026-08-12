//! US new-business FORMATION VELOCITY via Census **Business Formation
//! Statistics (BFS)** — the leading edge of competition.
//!
//! CBP/NES describe the trades market as it was ~2 years ago; BFS publishes
//! *current* new business applications per NAICS sector, **US-national only**.
//! This app ingests application counts (and the high-propensity subset) for
//! the construction/services sectors, upserts the raw national series into
//! `formations`, derives trailing-12-month velocity/acceleration per sector
//! into `formation_velocity`, and refreshes the density blend's formation
//! block — "how fast is new competition entering the market right now", on a
//! weekly scheduler. Fast path — GET JSON API, no HTML, no browser.
//!
//! Data type: LEADING INDICATOR (business applications). Access: FREE Census
//! key (`params.api_key` or env `CENSUS_API_KEY`, shared with the other Census
//! apps). Cataloged in `catalog/data-sources.toml` (scheduled → drift-gated).
//!
//! Contract notes (LIVE-verified 2026-07-30 with a real key): BFS is an EITS
//! timeseries at `https://api.census.gov/data/timeseries/eits/bfs` (NOT
//! `…/timeseries/bfs`, which 404s). Geography is **US-ONLY** — the dataset's
//! geography.json exposes fips = [us]; `for=state:XX` and `for=state:*` both
//! return HTTP 400 "unknown/unsupported geography hierarchy". A working
//! request REQUIRES `for=us:*` AND `time_slot_id=0`:
//! `get=cell_value,data_type_code,category_code,seasonally_adj&for=us:*`
//! `&time=from+{year}` (or `time=YYYY-MM`), `category_code={NAICS23|NAICS56}`,
//! `data_type_code=BA_BA` (all applications) / `BA_HBA` (high-propensity),
//! `seasonally_adj=no`, `time_slot_id=0`. `cell_value` arrives as a string.
//! QUIRK: predicate columns are DUPLICATED in the header row (e.g.
//! `["cell_value","data_type_code","category_code","seasonally_adj","time",
//! "category_code","data_type_code","seasonally_adj","time_slot_id","us"]`) —
//! columns must be resolved by FIRST occurrence of the name (or by position of
//! `cell_value`), never by unique-name assumptions.
//!
//! HONEST GRAIN: BFS is NAICS *sector* level (23 Construction, 56 Admin &
//! support/waste) at NATIONAL geography only. Every record carries
//! `grain: "naics_sector_national"` — state-level or trade-level (4/6-digit)
//! inference is deliberately impossible to read out of this dataset, and
//! consumers must keep it that way.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub struct CensusBfs;

/// First year requested from the timeseries (`time=from+…`): enough history
/// for a T12M window plus a full prior-year comparison from day one.
const DEFAULT_FROM_YEAR: &str = "2022";

/// (BFS category_code, friendly label) for the sectors covering Ledgerline's
/// trades: construction (plumbing/HVAC/electrical) and administrative &
/// support / waste (landscaping, pool, building services).
const DEFAULT_SECTORS: &[(&str, &str)] = &[
    ("NAICS23", "Construction"),
    (
        "NAICS56",
        "Administrative & support and waste management services",
    ),
];

#[async_trait]
impl ScrapeApp for CensusBfs {
    fn name(&self) -> &'static str {
        "census-bfs"
    }

    fn description(&self) -> &'static str {
        "US business-formation velocity from Census Business Formation Statistics \
         (EITS BFS timeseries JSON API). Monthly business applications + \
         high-propensity applications per NAICS sector, US-NATIONAL only — the BFS \
         API serves no state geography (`formations`), with derived trailing-12-month \
         velocity/YoY/acceleration (`formation_velocity`) feeding the \
         census/market_blend formation block. National sector grain — records are \
         labeled grain=naics_sector_national, no state or trade-level inference. \
         Requires a FREE Census API key (params.api_key or env CENSUS_API_KEY). \
         Params: {\"from_year\": \"2022\", \"sectors\": [\"NAICS23\",\"NAICS56\"], \
         \"api_key\": \"...\"}"
    }

    fn requires(&self) -> &'static [pumper_core::Requirement] {
        &[pumper_core::Requirement::Env("CENSUS_API_KEY")]
    }

    // BFS releases weekly (monthly series refreshed each Thursday); pull every
    // Friday at 11:00:00 so a new release is at most a day old when ingested.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 11 * * FRI")
    }

    fn default_params(&self) -> Value {
        json!({ "from_year": DEFAULT_FROM_YEAR })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "from_year": {
                        "type": "string",
                        "description": "First year of the requested series (`time=from+YYYY`). Needs >= 24 months of history for a T12M window plus its prior-year comparison."
                    },
                    "sectors": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "BFS `category_code` values — NAICS SECTOR grain only (e.g. NAICS23, NAICS56). BFS publishes no state geography and no finer NAICS."
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
                    description: "Weekly refresh of both trade sectors from the default start year",
                    params: json!({ "from_year": DEFAULT_FROM_YEAR }),
                },
                ManifestExample {
                    description: "Construction sector only, deeper history",
                    params: json!({ "from_year": "2018", "sectors": ["NAICS23"] }),
                },
            ],
            output_shape: Some(
                "{source, from_year, sectors: [{sector, label, monthly_cells, \
                 velocity_records}], empty_series, market_blend, index_datasets, \
                 formations: {records, new, changed, unchanged}, formation_velocity: \
                 {records, new, changed, unchanged}} — every record is US-national \
                 NAICS-sector grain (grain=naics_sector_national); t12m fields stay Null \
                 until 12 months exist; `empty_series` names the sector/measure requests \
                 the API served nothing for",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let from_year = ctx
            .params
            .get("from_year")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_FROM_YEAR)
            .to_string();
        let sectors: Vec<(String, String)> =
            match ctx.params.get("sectors").and_then(Value::as_array) {
                Some(arr) => arr
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|c| {
                        let label = DEFAULT_SECTORS
                            .iter()
                            .find(|(k, _)| *k == c)
                            .map(|(_, l)| l.to_string())
                            .unwrap_or_else(|| c.to_string());
                        (c.to_string(), label)
                    })
                    .collect(),
                None => DEFAULT_SECTORS
                    .iter()
                    .map(|(c, l)| (c.to_string(), l.to_string()))
                    .collect(),
            };

        let api_key = census_common::api_key(&ctx, "census-bfs")?;

        let mut formation_records: Vec<(String, Value)> = Vec::new();
        let mut velocity_records: Vec<(String, Value)> = Vec::new();
        let mut sector_summaries: Vec<Value> = Vec::new();
        // `{sector}/{data_type_code}` pairs the API served nothing for.
        let mut empty_series: Vec<String> = Vec::new();

        for (sector, label) in &sectors {
            // period → (applications, high-propensity). National series — the
            // BFS API serves no state geography (for=us:* is the only grain).
            let mut cells: BTreeMap<String, (Option<f64>, Option<f64>)> = BTreeMap::new();

            for (dt_code, slot) in [("BA_BA", 0usize), ("BA_HBA", 1usize)] {
                // `for=us:*` and `time_slot_id=0` are both REQUIRED — without
                // them the API 400s (state geographies) or returns nothing.
                let url = format!(
                    "https://api.census.gov/data/timeseries/eits/bfs?get=cell_value,data_type_code,category_code,seasonally_adj&for=us:*&time=from+{from_year}&category_code={sector}&data_type_code={dt_code}&seasonally_adj=no&time_slot_id=0&key={api_key}"
                );
                let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
                // An empty series for one sector/measure is a note, not a
                // failure — and now a COUNTED one: a silently skipped measure
                // used to look identical to a measure that returned zeros.
                // Shared with the three sibling apps (`is_empty_answer`).
                if census_common::is_empty_answer(resp.status, &resp.body) {
                    empty_series.push(format!("{sector}/{dt_code}"));
                    continue;
                }
                if !resp.is_success() {
                    return Err(Error::App(format!(
                        "Census BFS {sector} {dt_code}: HTTP {} (body starts: {})",
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
                        "Census BFS {sector} {dt_code}: response was not JSON{hint} \
                         (starts: {})",
                        resp.body.chars().take(160).collect::<String>()
                    )));
                }
                let rows: Vec<Vec<String>> = serde_json::from_str(&resp.body).map_err(|e| {
                    Error::App(format!("Census BFS {sector} {dt_code}: bad JSON rows: {e}"))
                })?;
                ctx.save_artifact(
                    &format!("bfs-{sector}-{dt_code}.json"),
                    &serde_json::to_vec_pretty(&rows)?,
                )
                .await?;

                // QUIRK: EITS duplicates the predicate columns in the header
                // row (each requested get= var appears again as an echoed
                // predicate) — `position` resolves the FIRST occurrence, which
                // is the contract here; never assume unique column names.
                let header = rows.first().cloned().unwrap_or_default();
                let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
                let i_val = idx("cell_value").ok_or_else(|| {
                    Error::App(format!(
                        "Census BFS {sector}: no cell_value column in {header:?}"
                    ))
                })?;
                let i_time = idx("time").ok_or_else(|| {
                    Error::App(format!("Census BFS {sector}: no time column in {header:?}"))
                })?;
                let i_sa = idx("seasonally_adj");

                for (period, v) in parse_series_rows(&rows, i_val, i_time, i_sa) {
                    let cell = cells.entry(period).or_default();
                    if slot == 0 {
                        cell.0 = Some(v);
                    } else {
                        cell.1 = Some(v);
                    }
                }
            }

            // Raw monthly records — one national series per sector.
            let mut months: Vec<(String, f64)> = Vec::new();
            let mut hp_months: Vec<(String, f64)> = Vec::new();
            for (period, (ba, hba)) in &cells {
                if let Some(v) = ba {
                    months.push((period.clone(), *v));
                }
                if let Some(v) = hba {
                    hp_months.push((period.clone(), *v));
                }
                formation_records.push((
                    format!("US|{sector}|{period}"),
                    json!({
                        "sector": sector,
                        "sector_label": label,
                        "geo": "US",
                        "period": period,
                        "applications": ba.map(Value::from).unwrap_or(Value::Null),
                        "high_propensity_applications":
                            hba.map(Value::from).unwrap_or(Value::Null),
                        "seasonally_adj": "no",
                        "grain": "naics_sector_national",
                        "source": "eits/bfs",
                    }),
                ));
            }

            // Derived velocity — national, per sector.
            let mut sector_velocity = 0usize;
            let v = compute_velocity(&months);
            if v.months_available > 0 {
                let hp_t12m = compute_velocity(&hp_months).t12m;
                sector_velocity = 1;
                velocity_records.push((
                    format!("US|{sector}"),
                    json!({
                        "sector": sector,
                        "sector_label": label,
                        "geo": "US",
                        "months_available": v.months_available,
                        "as_of_period": v.as_of,
                        "t12m_applications": v.t12m.map(Value::from).unwrap_or(Value::Null),
                        "prior12m_applications":
                            v.prior12m.map(Value::from).unwrap_or(Value::Null),
                        "yoy_delta_pct":
                            v.yoy_delta_pct.map(Value::from).unwrap_or(Value::Null),
                        "accel_pct": v.accel_pct.map(Value::from).unwrap_or(Value::Null),
                        "t12m_high_propensity":
                            hp_t12m.map(Value::from).unwrap_or(Value::Null),
                        "seasonally_adj": "no",
                        "grain": "naics_sector_national",
                    }),
                ));
            }

            sector_summaries.push(json!({
                "sector": sector,
                "label": label,
                "monthly_cells": cells.len(),
                "velocity_records": sector_velocity,
            }));
        }

        let formations = ctx.upsert_many("formations", &formation_records).await?;
        let velocity = ctx
            .upsert_many("formation_velocity", &velocity_records)
            .await?;

        // Refresh the blend so its `formation` block tracks this weekly pull.
        let market_blend = match app_census_density::sync_market_blend(&ctx).await {
            Ok(v) => v,
            Err(e) => json!({ "skipped": format!("{e}") }),
        };

        // `with_product_index` puts `census/market_blend` + `census/saturation`
        // in the worker's index + hook scope for this run — see
        // `census_common::product_index_datasets`.
        Ok(census_common::with_product_index(json!({
            "source": "census/eits-bfs",
            "from_year": from_year,
            "sectors": sector_summaries,
            // Sector × measure requests the API served nothing for — an empty
            // answer is a fact about the release, not an absence of formations.
            "empty_series": empty_series,
            "market_blend": market_blend,
            "formations": {
                "records": formation_records.len(),
                "new": formations.new.len(),
                "changed": formations.changed.len(),
                "unchanged": formations.unchanged,
            },
            "formation_velocity": {
                "records": velocity_records.len(),
                "new": velocity.new.len(),
                "changed": velocity.changed.len(),
                "unchanged": velocity.unchanged,
            },
        })))
    }
}

/// Parse EITS series rows into `(period, value)` tuples (national series —
/// there is no state column; BFS geography is `us` only). Rows with a
/// non-numeric or negative cell (EITS suppression/jam conventions) are
/// dropped, never recorded as zero applications; a seasonally-adjusted row
/// that leaks past the `seasonally_adj=no` predicate is dropped too. Column
/// indices must be resolved by FIRST occurrence of the header name — EITS
/// duplicates the predicate columns in the header row.
fn parse_series_rows(
    rows: &[Vec<String>],
    i_val: usize,
    i_time: usize,
    i_sa: Option<usize>,
) -> Vec<(String, f64)> {
    rows.iter()
        .skip(1)
        .filter_map(|row| {
            if let Some(i) = i_sa {
                if row.get(i).map(|s| s.trim().to_lowercase()) != Some("no".into()) {
                    return None;
                }
            }
            let v = row
                .get(i_val)?
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|v| *v >= 0.0)?;
            let period = row.get(i_time)?.trim().to_string();
            (!period.is_empty()).then_some((period, v))
        })
        .collect()
}

/// Trailing-window formation velocity for one national sector series.
#[derive(Debug, Default, PartialEq)]
pub struct Velocity {
    pub months_available: usize,
    /// Latest period in the series (`YYYY-MM`).
    pub as_of: Option<String>,
    /// Sum of the latest 12 monthly values; `None` until 12 months exist —
    /// a partial window must not masquerade as an annual rate.
    pub t12m: Option<f64>,
    /// Sum of months 13–24 back; `None` until 24 months exist.
    pub prior12m: Option<f64>,
    /// (t12m − prior12m) / prior12m, in % — `None` when the prior window is
    /// incomplete or zero.
    pub yoy_delta_pct: Option<f64>,
    /// Recent momentum vs the trailing year: (last-3-months annualized −
    /// t12m) / t12m, in % — positive = formations accelerating.
    pub accel_pct: Option<f64>,
}

/// Pure velocity math over `(period "YYYY-MM", value)` samples (any order;
/// sorted lexicographically, which is chronological for zero-padded periods).
pub fn compute_velocity(months: &[(String, f64)]) -> Velocity {
    let mut m: Vec<(String, f64)> = months.to_vec();
    m.sort_by(|a, b| a.0.cmp(&b.0));
    let n = m.len();
    let round1 = |x: f64| (x * 10.0).round() / 10.0;

    let mut v = Velocity {
        months_available: n,
        as_of: m.last().map(|(p, _)| p.clone()),
        ..Velocity::default()
    };
    if n >= 12 {
        let t12m: f64 = m[n - 12..].iter().map(|(_, x)| x).sum();
        v.t12m = Some(round1(t12m));
        let last3: f64 = m[n - 3..].iter().map(|(_, x)| x).sum();
        if t12m > 0.0 {
            v.accel_pct = Some(round1((last3 * 4.0 - t12m) / t12m * 100.0));
        }
        if n >= 24 {
            let prior: f64 = m[n - 24..n - 12].iter().map(|(_, x)| x).sum();
            v.prior12m = Some(round1(prior));
            if prior > 0.0 {
                v.yoy_delta_pct = Some(round1((t12m - prior) / prior * 100.0));
            }
        }
    }
    v
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
        let app = CensusBfs;
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
    /// `census/*` products this weekly run refreshes are invisible — no
    /// per-record search doc, and (worker `run_indexed_apps`) no watch, trigger
    /// or saved search scoped to app `census` can EVER fire for this run.
    ///
    /// The needle is split so this assertion cannot match itself.
    #[test]
    fn run_result_declares_the_census_product_datasets() {
        let needle = concat!("census_common::with_product_index", "(json!(");
        assert_eq!(
            include_str!("lib.rs").matches(needle).count(),
            1,
            "census-bfs's run() must wrap its result exactly once with {needle}"
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

    fn series(spec: &[(&str, f64)]) -> Vec<(String, f64)> {
        spec.iter().map(|(p, v)| (p.to_string(), *v)).collect()
    }

    /// 24 months: 100/mo in year one, 110/mo in year two.
    fn two_years() -> Vec<(String, f64)> {
        let mut s = Vec::new();
        for m in 1..=12 {
            s.push((format!("2023-{m:02}"), 100.0));
        }
        for m in 1..=12 {
            s.push((format!("2024-{m:02}"), 110.0));
        }
        s
    }

    #[test]
    fn full_windows_yield_t12m_yoy_and_flat_acceleration() {
        let v = compute_velocity(&two_years());
        assert_eq!(v.months_available, 24);
        assert_eq!(v.as_of.as_deref(), Some("2024-12"));
        assert_eq!(v.t12m, Some(1320.0));
        assert_eq!(v.prior12m, Some(1200.0));
        assert_eq!(v.yoy_delta_pct, Some(10.0));
        // Constant 110/mo: last 3 annualized == t12m → 0% acceleration.
        assert_eq!(v.accel_pct, Some(0.0));
    }

    #[test]
    fn recent_surge_shows_as_positive_acceleration() {
        let mut s = two_years();
        // Replace the last 3 months with a surge to 220/mo.
        for (p, v) in s.iter_mut().rev().take(3) {
            assert!(p.starts_with("2024"));
            *v = 220.0;
        }
        let v = compute_velocity(&s);
        // t12m = 9*110 + 3*220 = 1650; last3*4 = 2640 → +60%.
        assert_eq!(v.t12m, Some(1650.0));
        assert_eq!(v.accel_pct, Some(60.0));
    }

    #[test]
    fn partial_year_window_emits_no_t12m_not_a_scaled_guess() {
        let v = compute_velocity(&series(&[
            ("2024-01", 100.0),
            ("2024-02", 100.0),
            ("2024-03", 100.0),
        ]));
        assert_eq!(v.months_available, 3);
        assert_eq!(v.t12m, None);
        assert_eq!(v.yoy_delta_pct, None);
        assert_eq!(v.accel_pct, None);
        assert_eq!(v.as_of.as_deref(), Some("2024-03"));
    }

    #[test]
    fn incomplete_prior_window_emits_t12m_but_no_yoy() {
        let s: Vec<(String, f64)> = (0..18)
            .map(|i| (format!("{}-{:02}", 2023 + i / 12, i % 12 + 1), 100.0))
            .collect();
        let v = compute_velocity(&s);
        assert_eq!(v.months_available, 18);
        assert_eq!(v.t12m, Some(1200.0));
        assert_eq!(v.prior12m, None);
        assert_eq!(v.yoy_delta_pct, None);
    }

    #[test]
    fn unsorted_input_is_sorted_before_windowing() {
        let mut s = two_years();
        s.reverse();
        let v = compute_velocity(&s);
        assert_eq!(v.t12m, Some(1320.0));
        assert_eq!(v.as_of.as_deref(), Some("2024-12"));
    }

    #[test]
    fn suppressed_and_adjusted_series_rows_are_dropped() {
        let rows: Vec<Vec<String>> = vec![
            vec!["cell_value", "seasonally_adj", "time", "us"],
            vec!["120", "no", "2024-01", "1"],
            vec!["-999", "no", "2024-02", "1"], // jam sentinel
            vec!["abc", "no", "2024-03", "1"],  // non-numeric
            vec!["130", "yes", "2024-04", "1"], // adjusted leak
        ]
        .into_iter()
        .map(|r| r.into_iter().map(String::from).collect())
        .collect();
        let out = parse_series_rows(&rows, 0, 2, Some(1));
        assert_eq!(out, vec![("2024-01".to_string(), 120.0)]);
    }

    /// The LIVE header shape: predicate columns duplicated after the get= vars
    /// (`…,"time","category_code","data_type_code","seasonally_adj",
    /// "time_slot_id","us"]`). First-occurrence resolution must pick the real
    /// value columns and still parse the row.
    #[test]
    fn duplicated_predicate_header_columns_resolve_by_first_occurrence() {
        let rows: Vec<Vec<String>> = vec![
            vec![
                "cell_value",
                "data_type_code",
                "category_code",
                "seasonally_adj",
                "time",
                "category_code",
                "data_type_code",
                "seasonally_adj",
                "time_slot_id",
                "us",
            ],
            vec![
                "50183", "BA_BA", "NAICS23", "no", "2025-01", "NAICS23", "BA_BA", "no", "0", "1",
            ],
        ]
        .into_iter()
        .map(|r| r.into_iter().map(String::from).collect())
        .collect();
        let header = rows.first().cloned().unwrap();
        let idx = |name: &str| header.iter().position(|h| h.as_str() == name);
        let (i_val, i_time, i_sa) = (
            idx("cell_value").unwrap(),
            idx("time").unwrap(),
            idx("seasonally_adj"),
        );
        assert_eq!((i_val, i_time, i_sa), (0, 4, Some(3)));
        let out = parse_series_rows(&rows, i_val, i_time, i_sa);
        assert_eq!(out, vec![("2025-01".to_string(), 50183.0)]);
    }
}
