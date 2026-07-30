//! US new-business FORMATION VELOCITY via Census **Business Formation
//! Statistics (BFS)** — the leading edge of competition.
//!
//! CBP/NES describe the trades market as it was ~2 years ago; BFS publishes
//! *current* new business applications per NAICS sector × state. This app
//! ingests application counts (and the high-propensity subset) for the
//! construction/services sectors, upserts the raw series into `formations`,
//! derives trailing-12-month velocity/acceleration per state × sector into
//! `formation_velocity`, and refreshes the density blend's formation block —
//! "how fast is new competition entering this market right now", on a weekly
//! scheduler. Fast path — GET JSON API, no HTML, no browser.
//!
//! Data type: LEADING INDICATOR (business applications). Access: FREE Census
//! key (`params.api_key` or env `CENSUS_API_KEY`, shared with the other Census
//! apps). Cataloged in `catalog/data-sources.toml` (scheduled → drift-gated).
//!
//! Contract notes (verified 2026-07-30 against api.census.gov/data.json and the
//! key-free variable dictionary; data rows are key-gated — a keyless request
//! 302s to missing_key.html like CBP/NES, so the row shape is re-verified on
//! the first keyed run): BFS is an EITS timeseries at
//! `https://api.census.gov/data/timeseries/eits/bfs` (NOT `…/timeseries/bfs`,
//! which 404s). EITS conventions differ from the year-vintage apps:
//! `get=cell_value,data_type_code,category_code,seasonally_adj`, predicates
//! `time=from+{year}` (monthly periods `YYYY-MM` in the echoed `time` column),
//! `category_code={NAICS sector, e.g. NAICS23}`, `data_type_code=BA_BA`
//! (all business applications) / `BA_HBA` (high-propensity),
//! `seasonally_adj=no`, `for=state:*`. `cell_value` arrives as a string.
//!
//! HONEST GRAIN: BFS is NAICS *sector* level (23 Construction, 56 Admin &
//! support/waste). Every record carries `grain: "naics_sector"` — trade-level
//! (4/6-digit) inference is deliberately impossible to read out of this
//! dataset, and consumers must keep it that way.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
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
    ("NAICS56", "Administrative & support and waste management services"),
];

#[async_trait]
impl ScrapeApp for CensusBfs {
    fn name(&self) -> &'static str {
        "census-bfs"
    }

    fn description(&self) -> &'static str {
        "US business-formation velocity from Census Business Formation Statistics \
         (EITS BFS timeseries JSON API). Monthly business applications + \
         high-propensity applications per NAICS sector × state (`formations`), with \
         derived trailing-12-month velocity/YoY/acceleration (`formation_velocity`) \
         feeding the census/market_blend formation block. Sector grain — records are \
         labeled grain=naics_sector, no trade-level inference. Requires a FREE Census \
         API key (params.api_key or env CENSUS_API_KEY). Params: {\"from_year\": \
         \"2022\", \"states\": \"06,12,48\" (FIPS list; default all), \"sectors\": \
         [\"NAICS23\",\"NAICS56\"], \"api_key\": \"...\"}"
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

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let from_year = ctx
            .params
            .get("from_year")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_FROM_YEAR)
            .to_string();
        let states = ctx
            .params
            .get("states")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
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

        let for_clause = if states.is_empty() || states == "*" {
            "for=state:*".to_string()
        } else {
            format!("for=state:{states}")
        };

        let mut formation_records: Vec<(String, Value)> = Vec::new();
        let mut velocity_records: Vec<(String, Value)> = Vec::new();
        let mut sector_summaries: Vec<Value> = Vec::new();

        for (sector, label) in &sectors {
            // (state_fips, period) → (applications, high-propensity).
            let mut cells: BTreeMap<(String, String), (Option<f64>, Option<f64>)> =
                BTreeMap::new();

            for (dt_code, slot) in [("BA_BA", 0usize), ("BA_HBA", 1usize)] {
                let url = format!(
                    "https://api.census.gov/data/timeseries/eits/bfs?get=cell_value,data_type_code,category_code,seasonally_adj&{for_clause}&time=from+{from_year}&category_code={sector}&data_type_code={dt_code}&seasonally_adj=no&key={api_key}"
                );
                let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
                // An empty series for one sector/measure is a note, not a failure.
                if resp.status == 204 || resp.body.trim().is_empty() {
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
                let i_state = idx("state").ok_or_else(|| {
                    Error::App(format!("Census BFS {sector}: no state column in {header:?}"))
                })?;
                let i_sa = idx("seasonally_adj");

                for (st, period, v) in parse_series_rows(&rows, i_val, i_time, i_state, i_sa) {
                    let cell = cells.entry((st, period)).or_default();
                    if slot == 0 {
                        cell.0 = Some(v);
                    } else {
                        cell.1 = Some(v);
                    }
                }
            }

            // Raw monthly records.
            let mut months_by_state: BTreeMap<String, Vec<(String, f64)>> = BTreeMap::new();
            let mut hp_by_state: BTreeMap<String, Vec<(String, f64)>> = BTreeMap::new();
            for ((st_fips, period), (ba, hba)) in &cells {
                let state = census_common::state_abbr(st_fips).to_string();
                if let Some(v) = ba {
                    months_by_state
                        .entry(st_fips.clone())
                        .or_default()
                        .push((period.clone(), *v));
                }
                if let Some(v) = hba {
                    hp_by_state
                        .entry(st_fips.clone())
                        .or_default()
                        .push((period.clone(), *v));
                }
                formation_records.push((
                    format!("{sector}:{st_fips}:{period}"),
                    json!({
                        "sector": sector,
                        "sector_label": label,
                        "state": state,
                        "state_fips": st_fips,
                        "period": period,
                        "applications": ba.map(Value::from).unwrap_or(Value::Null),
                        "high_propensity_applications":
                            hba.map(Value::from).unwrap_or(Value::Null),
                        "seasonally_adj": "no",
                        "grain": "naics_sector",
                        "source": "eits/bfs",
                    }),
                ));
            }

            // Derived velocity per state.
            let mut sector_velocity = 0usize;
            for (st_fips, months) in &months_by_state {
                let v = compute_velocity(months);
                if v.months_available == 0 {
                    continue;
                }
                let hp_t12m = hp_by_state
                    .get(st_fips)
                    .map(|m| compute_velocity(m))
                    .and_then(|hv| hv.t12m);
                sector_velocity += 1;
                velocity_records.push((
                    format!("{sector}:{st_fips}"),
                    json!({
                        "sector": sector,
                        "sector_label": label,
                        "state": census_common::state_abbr(st_fips),
                        "state_fips": st_fips,
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
                        "grain": "naics_sector",
                    }),
                ));
            }

            sector_summaries.push(json!({
                "sector": sector,
                "label": label,
                "states_reported": months_by_state.len(),
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

        Ok(json!({
            "source": "census/eits-bfs",
            "from_year": from_year,
            "sectors": sector_summaries,
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
        }))
    }
}

/// Parse EITS series rows into `(state_fips, period, value)` tuples. Rows with
/// a non-numeric or negative cell (EITS suppression/jam conventions) are
/// dropped, never recorded as zero applications; a seasonally-adjusted row
/// that leaks past the `seasonally_adj=no` predicate is dropped too.
fn parse_series_rows(
    rows: &[Vec<String>],
    i_val: usize,
    i_time: usize,
    i_state: usize,
    i_sa: Option<usize>,
) -> Vec<(String, String, f64)> {
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
            let st = row.get(i_state)?.trim().to_string();
            (!period.is_empty() && !st.is_empty()).then_some((st, period, v))
        })
        .collect()
}

/// Trailing-window formation velocity for one state × sector series.
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
            vec!["cell_value", "seasonally_adj", "time", "state"],
            vec!["120", "no", "2024-01", "06"],
            vec!["-999", "no", "2024-02", "06"], // jam sentinel
            vec!["abc", "no", "2024-03", "06"],  // non-numeric
            vec!["130", "yes", "2024-04", "06"], // adjusted leak
        ]
        .into_iter()
        .map(|r| r.into_iter().map(String::from).collect())
        .collect();
        let out = parse_series_rows(&rows, 0, 2, 3, Some(1));
        assert_eq!(out, vec![("06".to_string(), "2024-01".to_string(), 120.0)]);
    }
}
