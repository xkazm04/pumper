//! MPSV ISPV average-earnings benchmarks by CZ-ISCO → `wages` dataset.
//!
//! The authoritative Czech salary-calibration table (Informační systém o
//! průměrném výdělku): median/mean plus the monthly decile spread
//! (D1/Q1/Q3/D9) per occupation × sphere (`MZDOVA` = wage sphere,
//! `PLATOVA` = salary/public sphere). Keyed `czIsco|sfera` into `wages`, it is
//! the trustworthy anchor used to derive seniority bands and to flag posted
//! salaries that fall outside the official distribution.
//!
//! Data type: LABOR-MARKET open data. Access: key-free, CC BY 4.0. Small file
//! (~320 KB), whole rows are kept as the record value. See
//! `catalog/data-sources.toml` (id `mpsv-ispv`).
//!
//! Source contract (verified 2026-07-05): `{ "polozky": [ {…row…} ] }`; each row
//! keys on `czIsco` ("CzIsco/1120") + `sfera`, with `medianMzda`, `mzdaPrumer`,
//! `diferenciaceD1M`/`Q1M`/`Q3M`/`D9M` (monthly) and the hourly analogues.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct MpsvIspv;

const URL: &str = "https://data.mpsv.cz/od/soubory/ispv-zamestnani/ispv-zamestnani.json";

#[async_trait]
impl ScrapeApp for MpsvIspv {
    fn name(&self) -> &'static str {
        "mpsv-ispv"
    }

    fn description(&self) -> &'static str {
        "Czech ISPV average-earnings benchmarks by CZ-ISCO occupation (MPSV open data, \
         key-free, CC BY 4.0). Median/mean + monthly decile spread (D1/Q1/Q3/D9) per \
         occupation × sphere, keyed `czIsco|sfera` into the `wages` dataset. No params."
    }

    /// Quarterly (the source refreshes on annual/quarterly cycles).
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 7 1 */3 *")
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let resp = ctx.engines.http.fetch(HttpRequest::get(URL)).await?;
        if !resp.is_success() {
            return Err(Error::App(format!(
                "mpsv-ispv: {URL} returned status {} (body starts: {})",
                resp.status,
                resp.body.chars().take(160).collect::<String>()
            )));
        }
        let parsed: Value = serde_json::from_str(&resp.body)
            .map_err(|e| Error::App(format!("mpsv-ispv: response was not JSON: {e}")))?;

        let rows = parsed
            .get("polozky")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
            .await?;

        // Key by occupation + sphere; both are needed to disambiguate a CZ-ISCO
        // row (wage vs salary sphere have different distributions).
        let items: Vec<(String, Value)> = rows
            .iter()
            .filter_map(|r| {
                let czisco = r.get("czIsco").and_then(Value::as_str)?;
                let sfera = r.get("sfera").and_then(Value::as_str).unwrap_or("");
                Some((format!("{czisco}|{sfera}"), r.clone()))
            })
            .collect();

        let summary = ctx.upsert_many("wages", &items).await?;

        Ok(json!({
            "source": "data.mpsv.cz/ispv-zamestnani",
            "rows": rows.len(),
            "stored": items.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
        }))
    }
}
