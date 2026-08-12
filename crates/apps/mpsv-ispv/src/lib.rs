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
//!
//! DRIFT IS LOUD. A document with no `polozky` array (renamed key, re-wrapped
//! envelope, error body that happens to parse) and a document carrying fewer
//! than [`MIN_PLAUSIBLE_ROWS`] rows both FAIL the run naming what arrived —
//! they used to be a clean `stored: 0` success. Nothing is written on either
//! path, and since this app upserts (never tombstones), the last good vintage
//! stays in place as mpsv-vpm's anchor.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Provenance, Result,
    ScrapeApp,
};
use serde_json::{json, Value};

pub struct MpsvIspv;

const URL: &str = "https://data.mpsv.cz/od/soubory/ispv-zamestnani/ispv-zamestnani.json";

/// Floor on the row count that may be published as an ISPV vintage.
///
/// ISPV is the national earnings table: median/mean plus a decile spread for
/// every CZ-ISCO occupation in BOTH spheres — hundreds of occupations, so the
/// ~320 KB document carries several hundred rows. Anything under 50 is not a
/// small quarter, it is a truncated download, a partial publication, or an
/// error envelope that happened to parse. Deliberately far below the real
/// count: the floor exists to catch collapse, not to police the source's size.
const MIN_PLAUSIBLE_ROWS: usize = 50;

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
                 `wages` dataset (key `<czIsco>|<sfera>`, value = the whole source row). A run \
                 whose document has no `polozky` array, or carries fewer than 50 rows, FAILS \
                 naming the drift instead of reporting `stored: 0` — nothing is written, so the \
                 last good vintage stays the anchor",
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

        // Archive the body BEFORE judging its shape: on drift the archived
        // document IS the evidence, and a run that fails on the check below
        // would otherwise leave nothing to look at.
        ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
            .await?;

        let rows = polozky_rows(&parsed).map_err(|why| {
            Error::App(format!("mpsv-ispv: source contract drift at {URL}: {why}"))
        })?;
        if implausibly_few_rows(rows.len()) {
            return Err(Error::App(format!(
                "mpsv-ispv: {URL} carried only {} rows (floor {MIN_PLAUSIBLE_ROWS}) — refusing to \
                 treat a collapsed document as a publishable vintage. The stored `wages` anchor is \
                 left untouched (this app upserts, it never tombstones), so mpsv-vpm keeps \
                 benchmarking against the last good vintage instead of an empty index.",
                rows.len()
            )));
        }

        let items = keyed_rows(rows);

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

/// The feed's `polozky` array, or the reason this document is SCHEMA DRIFT.
///
/// The anti-pattern this replaces: `get("polozky").and_then(as_array)
/// .unwrap_or_default()`. A renamed key, a re-wrapped envelope, or a `{}` body
/// all collapsed to an empty `Vec`, upserted nothing, and reported a clean
/// `stored: 0` **success**. Nothing was tombstoned either (this app upserts),
/// so there was no data-loss alarm on either side: the operator saw green and
/// mpsv-vpm kept benchmarking against a silently frozen anchor.
///
/// A **present-but-empty** `polozky: []` is a different claim from a missing
/// key — one says "the source published nothing", the other says "this is not
/// the document we contracted for" — so the two are distinguished here and only
/// the size floor judges the first.
fn polozky_rows(parsed: &Value) -> std::result::Result<&Vec<Value>, String> {
    match parsed.get("polozky") {
        Some(Value::Array(rows)) => Ok(rows),
        Some(other) => Err(format!(
            "`polozky` is present but is a {}, not an array",
            json_kind(other)
        )),
        None => Err(format!(
            "response has no `polozky` key (top-level keys: [{}])",
            top_level_keys(parsed)
        )),
    }
}

/// Whether a parsed row count is too small to be an ISPV vintage — see
/// [`MIN_PLAUSIBLE_ROWS`]. `polozky: []` is included on purpose: an empty
/// national earnings table is an outage, not a quarter with no earnings.
fn implausibly_few_rows(rows: usize) -> bool {
    rows < MIN_PLAUSIBLE_ROWS
}

/// JSON type name, for a drift message that says what actually arrived.
fn json_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// The document's top-level keys, comma-joined and capped — the one detail that
/// turns "the key is gone" into "the key was renamed to THIS".
fn top_level_keys(parsed: &Value) -> String {
    match parsed.as_object() {
        Some(map) => map.keys().take(12).cloned().collect::<Vec<_>>().join(", "),
        None => format!("<{}, not an object>", json_kind(parsed)),
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

    use std::sync::Arc;

    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::{HttpClient, HttpResponse};

    /// One scripted HTTP response for every request — the whole app makes
    /// exactly one fetch, so this is a complete stand-in for the source.
    struct StubHttp {
        status: u16,
        body: String,
    }

    #[async_trait]
    impl HttpClient for StubHttp {
        async fn fetch(&self, _: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: self.status,
                headers: Default::default(),
                body: self.body.clone(),
                final_url: URL.to_string(),
                cache_hit: false,
            })
        }
    }

    /// An `AppContext` whose only engine is a scripted HTTP response.
    fn ctx_serving(store: &TempStore, body: String) -> pumper_core::AppContext {
        let http = Arc::new(StubHttp { status: 200, body });
        TestContext::new(&store.storage, "mpsv-ispv")
            .engines(engines_with(http, Arc::new(Dead), Arc::new(Dead)))
            .build()
    }

    /// A document with `n` well-formed ISPV rows — enough to clear the size
    /// floor without hand-writing hundreds of fixtures.
    fn feed_of(n: usize) -> String {
        let rows: Vec<Value> = (0..n)
            .map(|i| {
                json!({
                    "czIsco": format!("CzIsco/{:04}", 1000 + i),
                    "sfera": "MZDOVA",
                    "medianMzda": 40_000 + i,
                })
            })
            .collect();
        json!({ "polozky": rows }).to_string()
    }

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

    // ── feed-drift honesty ──────────────────────────────────────────────────

    /// The anti-pattern: `unwrap_or_default()` turned every shape of drift into
    /// an empty Vec, so "the contract changed" and "the source published
    /// nothing" were the same observable — a clean `stored: 0`.
    #[test]
    fn missing_polozky_key_is_drift_not_an_empty_feed() {
        // Renamed key — the message must name what DID arrive.
        let renamed = json!({ "items": [], "meta": { "n": 0 } });
        let err = polozky_rows(&renamed).expect_err("drift");
        assert!(err.contains("no `polozky` key"), "{err}");
        assert!(err.contains("items"), "must name the keys present: {err}");
        // Re-wrapped envelope.
        assert!(polozky_rows(&json!({ "data": { "polozky": [] } })).is_err());
        // Right key, wrong type.
        let wrong = polozky_rows(&json!({ "polozky": { "0": {} } })).expect_err("drift");
        assert!(wrong.contains("object"), "{wrong}");
        // A body that is not even an object.
        assert!(polozky_rows(&json!([])).is_err());
        // And the honest empty feed is NOT drift — it is judged by the floor.
        assert_eq!(
            polozky_rows(&json!({ "polozky": [] })).expect("ok").len(),
            0
        );
    }

    #[test]
    fn row_floor_rejects_a_collapsed_document_but_not_a_full_vintage() {
        assert!(
            implausibly_few_rows(0),
            "an empty national table is an outage"
        );
        assert!(implausibly_few_rows(MIN_PLAUSIBLE_ROWS - 1));
        assert!(!implausibly_few_rows(MIN_PLAUSIBLE_ROWS));
        assert!(!implausibly_few_rows(800)); // a realistic vintage
    }

    // ── run() end-to-end over a stubbed HTTP engine ─────────────────────────

    #[tokio::test]
    async fn run_stores_every_keyable_row_from_a_healthy_feed() {
        let store = TempStore::new("mpsv-ispv-run").await;
        let out = MpsvIspv
            .run(ctx_serving(&store, feed_of(60)))
            .await
            .expect("healthy feed runs");
        assert_eq!(out["rows"], 60);
        assert_eq!(out["stored"], 60);
        assert_eq!(out["new"], 60);
        let stored = store
            .datasets()
            .list("mpsv-ispv", "wages", 1_000)
            .await
            .expect("read back");
        assert_eq!(stored.len(), 60);
    }

    /// The bug, at run level: a drifted document reported success and left the
    /// anchor silently frozen. It must now FAIL and write nothing.
    #[tokio::test]
    async fn run_fails_on_drift_instead_of_reporting_a_clean_stored_zero() {
        let store = TempStore::new("mpsv-ispv-drift").await;
        let body = json!({ "polozkyList": [] }).to_string();
        let err = MpsvIspv
            .run(ctx_serving(&store, body))
            .await
            .expect_err("drift must fail the run");
        let msg = err.to_string();
        assert!(msg.contains("source contract drift"), "{msg}");
        assert!(msg.contains("polozkyList"), "{msg}");
        assert!(
            store
                .datasets()
                .list("mpsv-ispv", "wages", 10)
                .await
                .expect("read back")
                .is_empty(),
            "a drifted run must write nothing"
        );
    }

    /// A present-but-collapsed feed is the second half of the same failure: the
    /// key is there, so the drift check passes, and only the floor catches it.
    #[tokio::test]
    async fn run_refuses_a_collapsed_feed_and_leaves_the_prior_vintage_in_place() {
        let store = TempStore::new("mpsv-ispv-floor").await;
        // A good vintage lands first...
        MpsvIspv
            .run(ctx_serving(&store, feed_of(60)))
            .await
            .expect("first run");
        // ...then the source collapses to three rows.
        let err = MpsvIspv
            .run(ctx_serving(&store, feed_of(3)))
            .await
            .expect_err("collapsed feed must fail the run");
        assert!(err.to_string().contains("floor"), "{err}");
        // The anchor mpsv-vpm reads is untouched — not emptied, not halved.
        assert_eq!(
            store
                .datasets()
                .list("mpsv-ispv", "wages", 1_000)
                .await
                .expect("read back")
                .len(),
            60
        );
    }

    #[tokio::test]
    async fn run_fails_on_a_non_success_status_before_parsing() {
        let store = TempStore::new("mpsv-ispv-503").await;
        let http = Arc::new(StubHttp {
            status: 503,
            body: "upstream unavailable".into(),
        });
        let ctx = TestContext::new(&store.storage, "mpsv-ispv")
            .engines(engines_with(http, Arc::new(Dead), Arc::new(Dead)))
            .build();
        let err = MpsvIspv.run(ctx).await.expect_err("503 fails");
        assert!(err.to_string().contains("503"), "{err}");
    }
}
