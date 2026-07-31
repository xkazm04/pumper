//! MPSV ISPV average-earnings benchmarks by CZ-ISCO → `wages` dataset.
//!
//! The authoritative Czech salary-calibration table (Informační systém o
//! průměrném výdělku): median/mean plus the monthly decile spread
//! (D1/Q1/Q3/D9) per occupation × sphere (`MZDOVA` = wage sphere,
//! `PLATOVA` = salary/public sphere). Keyed `czIsco|sfera` into `wages`, it is
//! the trustworthy anchor used to derive seniority bands and to flag posted
//! salaries that fall outside the official distribution.
//!
//! ISPV publishes with a quarterly-to-annual LAG. mpsv-vpm reads this `wages`
//! dataset as the anchor for `cz-labour/salary_gap` (posted-vs-official) and
//! the derived `cz-labour/salary_nowcast` (deterministic ratio-carry
//! projection of the current official-grade median); each stored record's
//! `updated_at` is the anchor vintage those products disclose as staleness.
//!
//! Data type: LABOR-MARKET open data. Access: key-free, CC BY 4.0. Small file
//! (~320 KB), whole rows are kept as the record value. See
//! `catalog/data-sources.toml` (id `mpsv-ispv`).
//!
//! Source contract (verified 2026-07-05): `{ "polozky": [ {…row…} ] }`; each row
//! keys on `czIsco` ("CzIsco/1120") + `sfera`, with `medianMzda`, `mzdaPrumer`,
//! `diferenciaceD1M`/`Q1M`/`Q3M`/`D9M` (monthly) and the hourly analogues.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Provenance, Result,
    ScrapeApp,
};
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

    /// The app reads NO params — the source is one fixed key-free document. The
    /// schema says exactly that (empty `properties`, permissive
    /// `additionalProperties` so a human can still pass a note), and the single
    /// worked example is the scheduled invocation. Inventing knobs the code
    /// never reads would be a lie an agent would act on.
    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "description": "No parameters: the ISPV endpoint, keying and dataset are fixed.",
                "properties": {},
                "additionalProperties": true
            })),
            examples: vec![ManifestExample {
                description: "Refresh the official ISPV wage anchor (the scheduled quarterly run; \
                              also the way to pull a just-published vintage on demand)",
                params: json!({}),
            }],
            output_shape: Some(
                "{source, rows, stored, new, changed, unchanged} — `rows` is the raw `polozky` \
                 count, `stored` the rows that carried a `czIsco` and were upserted into the \
                 `wages` dataset (key `<czIsco>|<sfera>`, value = the whole source row)",
            ),
            cost_class: CostClass::Free,
        }
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

        let items = keyed_rows(&rows);

        // Provenance (M12): every `wages` row is one object read out of THIS one
        // document, so the batch-level `source_url` is literally the URL each
        // record's content was fetched from — not an approximation. `rules_hash`
        // stays Null: the extraction is Rust code (`keyed_rows`), not a
        // registered RuleSet, so there is nothing replayable to pin.
        // `artifact_sha` stays Null too: the saved artifact is a re-serialized
        // pretty-print, not the byte-exact source body.
        let summary = ctx
            .upsert_many_with_provenance(
                "wages",
                &items,
                Provenance {
                    source_url: Some(URL.to_string()),
                    ..Default::default()
                },
            )
            .await?;

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

/// Key each ISPV row by occupation + sphere; both are needed to disambiguate a
/// CZ-ISCO row (wage vs salary sphere have different distributions). Rows
/// without a `czIsco` cannot be keyed and are dropped.
fn keyed_rows(rows: &[Value]) -> Vec<(String, Value)> {
    rows.iter()
        .filter_map(|r| {
            let czisco = r.get("czIsco").and_then(Value::as_str)?;
            let sfera = r.get("sfera").and_then(Value::as_str).unwrap_or("");
            Some((format!("{czisco}|{sfera}"), r.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Realistic slice of the ispv-zamestnani.json `polozky` array (source
    // contract verified 2026-07-05): same CZ-ISCO in both spheres, one row
    // without `sfera`, one malformed row without `czIsco`.
    const SAMPLE: &str = r#"[
        { "czIsco": "CzIsco/1120", "sfera": "MZDOVA",
          "medianMzda": 118706, "mzdaPrumer": 145861,
          "diferenciaceD1M": 55444, "diferenciaceD9M": 262714 },
        { "czIsco": "CzIsco/1120", "sfera": "PLATOVA",
          "medianMzda": 102371, "mzdaPrumer": 108990 },
        { "czIsco": "CzIsco/2512", "medianMzda": 75210 },
        { "sfera": "MZDOVA", "medianMzda": 41000 }
    ]"#;

    fn sample_rows() -> Vec<Value> {
        serde_json::from_str(SAMPLE).expect("sample parses")
    }

    /// The manifest claims this app takes no params. That claim must stay true:
    /// if a param is ever read from `ctx.params`, the schema has to grow with it.
    #[test]
    fn manifest_declares_a_paramless_schema_with_a_worked_example() {
        let m = MpsvIspv.manifest();
        let schema = m.params_schema.expect("schema declared");
        assert!(
            schema["properties"]
                .as_object()
                .expect("properties object")
                .is_empty(),
            "app reads no params — declaring one would mislead an agent"
        );
        assert_eq!(m.examples.len(), 1);
        assert_eq!(m.examples[0].params, json!({}));
        // Scheduled runs enqueue default_params, which must satisfy the schema.
        assert_eq!(MpsvIspv.default_params(), json!({}));
    }

    #[test]
    fn wage_and_salary_spheres_of_one_occupation_get_distinct_keys_not_a_collision() {
        let keys: Vec<String> = keyed_rows(&sample_rows())
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(keys.contains(&"CzIsco/1120|MZDOVA".to_string()));
        assert!(keys.contains(&"CzIsco/1120|PLATOVA".to_string()));
    }

    #[test]
    fn row_without_czisco_is_dropped_not_stored_under_an_empty_key() {
        let items = keyed_rows(&sample_rows());
        assert_eq!(items.len(), 3); // 4 rows, 1 unkeyable
        assert!(items.iter().all(|(k, _)| !k.starts_with('|')));
    }

    #[test]
    fn missing_sfera_still_keys_the_row_with_an_empty_suffix() {
        let items = keyed_rows(&sample_rows());
        let (key, row) = items
            .iter()
            .find(|(k, _)| k.starts_with("CzIsco/2512"))
            .expect("row kept");
        assert_eq!(key, "CzIsco/2512|");
        // The whole row is kept as the record value, not a projection.
        assert_eq!(row.get("medianMzda").and_then(Value::as_u64), Some(75210));
    }
}
