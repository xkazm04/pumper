//! MPSV číselníky (codebooks) → the label side of the Czech labour datasets.
//!
//! `mpsv-vpm` keys its skills, education, region and wage-type dimensions on
//! the opaque codebook URIs the vacancy feed publishes (`Dovednost/…`,
//! `Vzdelani/…`, `Kraj/116`, `TypMzdy/…`). Those URIs are identity, not text:
//! a `skill_demand` row said nothing a human or an agent could read, region
//! titles printed `kraj Kraj/108`, and both datasets were deliberately kept out
//! of the full-text index because there was no searchable TEXT in them.
//!
//! This app mirrors the four codebooks MPSV publishes as open data on the same
//! portal that serves the vacancy and ISPV feeds, one dataset per codebook,
//! keyed by the SAME URI the vacancy feed uses — so `mpsv-vpm` can resolve
//! `Dovednost/…` to `Programování v jazyce Java` at write time by an exact key
//! lookup, never a substring match.
//!
//! Data type: LABOR-MARKET reference data. Access: key-free, CC BY 4.0. Small
//! quarterly-stable JSON documents. See `catalog/data-sources.toml`
//! (ids `mpsv-ciselniky-*`).
//!
//! # The distribution URLs are ASSUMED, not verified
//!
//! The two live MPSV feeds this fleet already ingests both sit at
//! `https://data.mpsv.cz/od/soubory/<slug>/<slug>.json` (`volna-mista`,
//! `ispv-zamestnani`), and the catalog already records that a third MPSV
//! distribution's exact `/od/soubory` slug had to be guessed and 404'd
//! (`mpsv-vpm-prirustky`). The codebook slugs below follow the same doubled
//! -slug pattern but have **NOT been verified against the live portal** — this
//! app was built without network access. They are ASSUMED, exactly as
//! `grants-gov`'s attachment download-URL pattern is (see that crate's header):
//! the constant is named, the assumption is stated, and the run's own output
//! carries it.
//!
//! An operator therefore does not need a code change to correct a slug: every
//! codebook's URL is overridable per run through `params.urls`, and the result
//! reports `urlSource: "assumed" | "param"` per codebook. **First live run:**
//! run it once, and if a codebook 404s, re-run with the real URL in
//! `params.urls` and move that URL into [`CODEBOOKS`] in the same session.
//!
//! # Drift is loud, and the run is all-or-nothing
//!
//! Every codebook is fetched and judged BEFORE anything is written: a document
//! with no `polozky` array (renamed key, re-wrapped envelope, error body that
//! happens to parse) and a document carrying fewer rows than that codebook's
//! own floor both FAIL the run naming what arrived. Nothing is written on
//! either path, so the last good codebook vintage stays in place as the label
//! source and `mpsv-vpm` keeps stamping yesterday's labels instead of
//! silently un-labelling every row it writes.
//!
//! The floors are per codebook because the codebooks have wildly different
//! sizes (14 kraje vs thousands of skills) — the same reason `mpsv-ispv`'s
//! floor is 50 and `mpsv-vpm`'s is 1 000.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Provenance, Result,
    ScrapeApp,
};
use serde_json::{json, Map, Value};

pub struct MpsvCiselniky;

/// One mirrored codebook: the dataset it lands in, where it is fetched from,
/// and the row floor below which the document is a collapsed download.
#[derive(Debug)]
pub struct Codebook {
    /// Dataset name under this app's namespace, and the `params.codebooks` /
    /// `params.urls` selector.
    pub name: &'static str,
    /// ASSUMED distribution URL — see the crate header.
    pub url: &'static str,
    /// Floor on the publishable row count for THIS codebook.
    pub min_rows: usize,
    /// The URI prefix the vacancy feed uses for this dimension, and therefore
    /// the prefix `mpsv-vpm` looks rows up by. Documentation only: nothing here
    /// filters on it, because a codebook that renamed its prefix is drift the
    /// operator must see, not rows this app should quietly drop.
    pub uri_prefix: &'static str,
}

/// The four codebooks `mpsv-vpm` needs to make its datasets readable.
///
/// Floors are deliberately far below the real counts — they exist to catch a
/// truncated download or an error envelope, not to police the source's size:
///
/// * `kraj` — the Czech republic has 14 kraje (13 + Praha). A document with
///   fewer than 10 is not a redistricting, it is a broken download.
/// * `typ_mzdy` — a handful of wage types; 2 is the floor a real enumeration
///   cannot fall below.
/// * `vzdelani` — the KKOV education-level scale is a dozen-ish levels.
/// * `dovednost` — the skills codebook is the big one (thousands of entries);
///   100 is a collapse, not a quarter with few skills.
pub const CODEBOOKS: &[Codebook] = &[
    Codebook {
        name: "dovednost",
        url: "https://data.mpsv.cz/od/soubory/ciselnik-dovednost/ciselnik-dovednost.json",
        min_rows: 100,
        uri_prefix: "Dovednost/",
    },
    Codebook {
        name: "vzdelani",
        url: "https://data.mpsv.cz/od/soubory/ciselnik-vzdelani/ciselnik-vzdelani.json",
        min_rows: 5,
        uri_prefix: "Vzdelani/",
    },
    Codebook {
        name: "kraj",
        url: "https://data.mpsv.cz/od/soubory/ciselnik-kraj/ciselnik-kraj.json",
        min_rows: 10,
        uri_prefix: "Kraj/",
    },
    Codebook {
        name: "typ_mzdy",
        url: "https://data.mpsv.cz/od/soubory/ciselnik-typ-mzdy/ciselnik-typ-mzdy.json",
        min_rows: 2,
        uri_prefix: "TypMzdy/",
    },
];

/// This app's own namespace — the `app` half of the `(app, dataset)` pair
/// `mpsv-vpm` reads its labels from. Exported so the consumer names the same
/// constant instead of re-typing the string.
pub const CODEBOOK_APP: &str = "mpsv-ciselniky";

/// The `(app, dataset)` pairs offered to the full-text index.
///
/// All four, and unlike every other labour dataset this is cheap and obviously
/// right: a codebook is a few thousand rows of pure LABEL text that changes
/// quarterly at most, so the cost is O(a codebook revision) and the payoff is
/// that "which skills mention Java" becomes answerable at all.
pub fn index_datasets_spec() -> Value {
    Value::Array(
        CODEBOOKS
            .iter()
            .map(|c| json!({ "app": CODEBOOK_APP, "dataset": c.name }))
            .collect(),
    )
}

#[async_trait]
impl ScrapeApp for MpsvCiselniky {
    fn name(&self) -> &'static str {
        CODEBOOK_APP
    }

    fn description(&self) -> &'static str {
        "Czech MPSV codebooks (číselníky) mirrored as label tables: Dovednost (skills), \
         Vzdelani (education levels), Kraj (regions) and TypMzdy (wage types), one dataset \
         each, keyed by the SAME codebook URI the vacancy feed publishes so mpsv-vpm can \
         resolve skill/education/region ids to human-readable names. Key-free, CC BY 4.0. \
         Params: `codebooks` (subset to fetch), `urls` (per-codebook URL override — the \
         shipped URLs are ASSUMED, see the crate header)."
    }

    /// Quarterly, an hour before the ISPV pull on the same day — codebooks are
    /// reference data that changes at most a few times a year, and the labels
    /// want to be in place before the consumers run.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 6 1 */3 *")
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "description":
                    "All params are optional. `codebooks` restricts the run to a subset; \
                     `urls` overrides a codebook's distribution URL, which is how a wrong \
                     ASSUMED slug is corrected without a code change.",
                "properties": {
                    "codebooks": {
                        "type": "array",
                        "items": { "type": "string", "enum": ["dovednost", "vzdelani", "kraj", "typ_mzdy"] },
                        "description": "Codebooks to fetch (default: all four)."
                    },
                    "urls": {
                        "type": "object",
                        "description":
                            "Per-codebook distribution URL override, e.g. \
                             {\"kraj\": \"https://data.mpsv.cz/od/soubory/…json\"}.",
                        "additionalProperties": { "type": "string" }
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Refresh all four codebooks (the scheduled quarterly run)",
                    params: json!({}),
                },
                ManifestExample {
                    description: "Correct one ASSUMED distribution URL after a 404, without a \
                                  code change",
                    params: json!({
                        "codebooks": ["kraj"],
                        "urls": { "kraj": "https://data.mpsv.cz/od/soubory/ciselnik-kraj/ciselnik-kraj.json" }
                    }),
                },
            ],
            output_shape: Some(
                "{source, codebooks: [{name, url, urlSource, rows, stored, new, changed, \
                 unchanged, labelledPct}], stored, index_datasets} — one entry per fetched \
                 codebook. Every codebook is fetched and judged BEFORE anything is written: a \
                 document with no `polozky` array, or fewer rows than that codebook's floor, \
                 FAILS the run naming what arrived and writes nothing, so the last good label \
                 vintage stays in place. `urlSource` is `assumed` for the shipped constant and \
                 `param` when the operator supplied the URL.",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let selected = selected_codebooks(ctx.params.get("codebooks"))?;
        let overrides = ctx.params.get("urls");

        // Phase 1 — fetch, parse and JUDGE every codebook. Nothing is written
        // until all of them cleared, so a partially-drifted run cannot leave
        // half the label space refreshed and half frozen at an older vintage
        // (which is worse than either, because nothing says which is which).
        let mut fetched: Vec<(&Codebook, String, bool, Vec<(String, Value)>, usize)> = Vec::new();
        for book in selected {
            let (url, from_param) = resolve_url(book, overrides);
            let resp = ctx.engines.http.fetch(HttpRequest::get(&url)).await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "mpsv-ciselniky: codebook `{}` at {url} returned status {} (body starts: {}). \
                     The shipped URLs are ASSUMED (see the crate header) — if this is a 404, \
                     re-run with `params.urls.{}` set to the real distribution URL.",
                    book.name,
                    resp.status,
                    resp.body.chars().take(160).collect::<String>(),
                    book.name
                )));
            }
            let parsed: Value = serde_json::from_str(&resp.body).map_err(|e| {
                Error::App(format!(
                    "mpsv-ciselniky: codebook `{}` at {url} was not JSON: {e}",
                    book.name
                ))
            })?;

            // Archive BEFORE judging the shape: on drift the archived document
            // IS the evidence, and a run that fails below would otherwise leave
            // nothing to look at.
            ctx.save_artifact(
                &format!("{}.json", book.name),
                &serde_json::to_vec_pretty(&parsed)?,
            )
            .await?;

            let rows = polozky_rows(&parsed).map_err(|why| {
                Error::App(format!(
                    "mpsv-ciselniky: source contract drift in codebook `{}` at {url}: {why}",
                    book.name
                ))
            })?;
            if implausibly_few_rows(rows.len(), book.min_rows) {
                return Err(Error::App(format!(
                    "mpsv-ciselniky: codebook `{}` at {url} carried only {} rows (floor {}) — \
                     refusing to publish a collapsed codebook as a label vintage. Nothing was \
                     written for ANY codebook this run, so mpsv-vpm keeps stamping the labels it \
                     already has instead of silently un-labelling every row it writes.",
                    book.name,
                    rows.len(),
                    book.min_rows
                )));
            }
            let items = keyed_rows(book.name, rows);
            fetched.push((book, url, from_param, items, rows.len()));
        }

        // Phase 2 — write. Provenance: every row of a codebook dataset is one
        // object read out of THIS one document, so the batch-level `source_url`
        // is literally the URL each record's content came from. `rules_hash`
        // and `artifact_sha` stay Null: the extraction is Rust code, and the
        // saved artifact is a re-serialized pretty-print, not the source bytes.
        let mut books_out: Vec<Value> = Vec::new();
        let mut stored_total = 0usize;
        for (book, url, from_param, items, rows) in &fetched {
            let summary = ctx
                .upsert_many_with_provenance(
                    book.name,
                    items,
                    Provenance {
                        source_url: Some(url.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            stored_total += items.len();
            books_out.push(json!({
                "name": book.name,
                "url": url,
                "urlSource": if *from_param { "param" } else { "assumed" },
                "uriPrefix": book.uri_prefix,
                "rows": rows,
                "stored": items.len(),
                "new": summary.new.len(),
                "changed": summary.changed.len(),
                "unchanged": summary.unchanged,
                // The share of stored rows that carry a Czech label — the one
                // number that says whether this mirror is usable as a label
                // source at all. A codebook whose rows parse but carry no
                // readable name resolves every id to `label: null` downstream.
                "labelledPct": labelled_pct(items),
            }));
        }

        Ok(json!({
            "source": "data.mpsv.cz/ciselniky",
            "codebooks": books_out,
            "stored": stored_total,
            "index_datasets": index_datasets_spec(),
        }))
    }
}

/// The codebooks this run covers: all of them, or the subset `params.codebooks`
/// names.
///
/// An unknown name is a REFUSAL, not a silent skip: `{"codebooks": ["kraje"]}`
/// (one letter off) would otherwise be a green run that refreshed nothing, and
/// the operator would go on believing the region labels were current.
fn selected_codebooks(param: Option<&Value>) -> Result<Vec<&'static Codebook>> {
    let names = match param {
        None | Some(Value::Null) => return Ok(CODEBOOKS.iter().collect()),
        Some(Value::Array(a)) => a,
        Some(other) => {
            return Err(Error::App(format!(
                "mpsv-ciselniky: `codebooks` must be an array of codebook names, got {other}"
            )))
        }
    };
    if names.is_empty() {
        return Ok(CODEBOOKS.iter().collect());
    }
    let mut out = Vec::new();
    for n in names {
        let name = n.as_str().unwrap_or_default();
        match CODEBOOKS.iter().find(|c| c.name == name) {
            Some(c) => out.push(c),
            None => {
                return Err(Error::App(format!(
                    "mpsv-ciselniky: unknown codebook {n} — known codebooks are [{}]",
                    CODEBOOKS
                        .iter()
                        .map(|c| c.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        }
    }
    Ok(out)
}

/// This codebook's distribution URL and whether the operator supplied it.
///
/// The shipped constants are ASSUMED (crate header), so the override is the
/// documented way to correct a slug on the first live run; `from_param` is
/// reported per codebook so a result never claims a verified URL it did not
/// use.
fn resolve_url(book: &Codebook, overrides: Option<&Value>) -> (String, bool) {
    let over = overrides
        .and_then(Value::as_object)
        .and_then(|m| m.get(book.name))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match over {
        Some(u) => (u.to_string(), true),
        None => (book.url.to_string(), false),
    }
}

/// The feed's `polozky` array, or the reason this document is SCHEMA DRIFT.
///
/// Same discipline as `mpsv-ispv`: `get("polozky").and_then(as_array)
/// .unwrap_or_default()` would turn a renamed key, a re-wrapped envelope and an
/// error body that happens to parse into one indistinguishable empty `Vec`, and
/// a green `stored: 0`. A present-but-empty `polozky: []` is a DIFFERENT claim
/// ("the codebook is empty") and is judged by the floor, not here.
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

/// Whether a parsed row count is too small to be a publishable vintage of a
/// codebook whose floor is `floor` — see [`CODEBOOKS`].
fn implausibly_few_rows(rows: usize, floor: usize) -> bool {
    rows < floor
}

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

fn top_level_keys(parsed: &Value) -> String {
    match parsed.as_object() {
        Some(map) => map.keys().take(12).cloned().collect::<Vec<_>>().join(", "),
        None => format!("<{}, not an object>", json_kind(parsed)),
    }
}

/// The codebook URI a row is identified by — the SAME string the vacancy feed
/// puts in `profeseCzIsco.id` / `pozadovanaDovednost[].id` / `…kraj.id`, which
/// is why the lookup downstream is an exact key hit and never a substring
/// match. A row without one cannot be keyed and is dropped.
fn row_id(row: &Value) -> Option<&str> {
    for field in ["id", "@id", "uri", "kod"] {
        if let Some(s) = row.get(field).and_then(Value::as_str) {
            let s = s.trim();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

/// A row's `(cs, en)` labels.
///
/// MPSV's open data publishes a name either as a language object
/// (`{"nazev": {"cs": "…", "en": "…"}}` — the shape `mpsv-vpm` already
/// deserializes for `pozadovanaProfese`) or as a bare string, and different
/// codebooks on the same portal have used different field names. Reading
/// several candidates is not sloppiness: an unresolved label is the ONE failure
/// this app exists to prevent, and the alternative to tolerance here is a
/// silently un-labelled dimension downstream.
///
/// A label is never invented: a row whose name field is missing or blank yields
/// `None`, which becomes a stored `label_cs: null` and, downstream, a
/// `skillLabel: null` on a row that keeps its URI.
fn row_labels(row: &Value) -> (Option<String>, Option<String>) {
    for field in ["nazev", "název", "label", "name", "text", "popis"] {
        match row.get(field) {
            Some(Value::String(s)) => {
                if let Some(v) = non_blank(s) {
                    return (Some(v), None);
                }
            }
            Some(Value::Object(m)) => {
                let cs = lang(m, "cs");
                let en = lang(m, "en");
                if cs.is_some() || en.is_some() {
                    return (cs, en);
                }
            }
            _ => {}
        }
    }
    // Flat per-language siblings, the third shape seen on this portal.
    let cs = row
        .get("nazevCs")
        .and_then(Value::as_str)
        .and_then(non_blank);
    let en = row
        .get("nazevEn")
        .and_then(Value::as_str)
        .and_then(non_blank);
    (cs, en)
}

fn lang(m: &Map<String, Value>, key: &str) -> Option<String> {
    m.get(key).and_then(Value::as_str).and_then(non_blank)
}

fn non_blank(s: &str) -> Option<String> {
    let t = s.trim();
    (!t.is_empty()).then(|| t.to_string())
}

/// One stored codebook record per row, keyed by the codebook URI.
///
/// The record is a NORMALIZED label row, not the verbatim source row (the
/// `mpsv-ispv` "keep the whole row" contract does not apply: the whole point
/// here is a stable `{id, label_cs, label_en}` shape a consumer can resolve
/// against without knowing which of the portal's three name shapes this
/// codebook uses). The raw document is kept as the run's artifact.
fn keyed_rows(codebook: &str, rows: &[Value]) -> Vec<(String, Value)> {
    rows.iter()
        .filter_map(|r| {
            let id = row_id(r)?;
            let (cs, en) = row_labels(r);
            Some((
                id.to_string(),
                json!({
                    // The full-text index builds a doc's title from the record's
                    // own `title` field; the label IS the title here.
                    "title": cs.clone().or_else(|| en.clone()).unwrap_or_else(|| id.to_string()),
                    "id": id,
                    "codebook": codebook,
                    "label_cs": cs,
                    "label_en": en,
                }),
            ))
        })
        .collect()
}

/// Share of stored rows carrying a Czech label, one decimal. A codebook that
/// parses into rows but resolves no names is a mirror of nothing.
fn labelled_pct(items: &[(String, Value)]) -> f64 {
    if items.is_empty() {
        return 0.0;
    }
    let labelled = items
        .iter()
        .filter(|(_, v)| v.get("label_cs").and_then(Value::as_str).is_some())
        .count();
    (labelled as f64 / items.len() as f64 * 1000.0).round() / 10.0
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::{HttpClient, HttpResponse};

    /// One scripted response per URL — the app makes one fetch per codebook, so
    /// a URL→body map is a complete stand-in for the portal. An unscripted URL
    /// answers 404, which is exactly what a wrong ASSUMED slug does live.
    struct StubHttp {
        bodies: HashMap<String, (u16, String)>,
        seen: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HttpClient for StubHttp {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            self.seen.lock().expect("seen").push(req.url.clone());
            let (status, body) = self
                .bodies
                .get(&req.url)
                .cloned()
                .unwrap_or_else(|| (404, "not found".to_string()));
            Ok(HttpResponse {
                status,
                headers: Default::default(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn book(name: &str) -> &'static Codebook {
        CODEBOOKS.iter().find(|c| c.name == name).expect("codebook")
    }

    /// A codebook document of `n` well-formed rows, ids prefixed the way the
    /// vacancy feed publishes them.
    fn feed_of(prefix: &str, n: usize) -> String {
        let rows: Vec<Value> = (0..n)
            .map(|i| {
                json!({
                    "id": format!("{prefix}{}", 100 + i),
                    "nazev": { "cs": format!("polozka {i}") },
                })
            })
            .collect();
        json!({ "polozky": rows }).to_string()
    }

    /// Every codebook served at its ASSUMED URL, each just over its floor.
    fn healthy_portal() -> HashMap<String, (u16, String)> {
        CODEBOOKS
            .iter()
            .map(|c| {
                (
                    c.url.to_string(),
                    (200, feed_of(c.uri_prefix, c.min_rows + 2)),
                )
            })
            .collect()
    }

    fn ctx_serving(
        store: &TempStore,
        bodies: HashMap<String, (u16, String)>,
        params: Value,
    ) -> pumper_core::AppContext {
        let http = Arc::new(StubHttp {
            bodies,
            seen: Mutex::new(Vec::new()),
        });
        TestContext::new(&store.storage, CODEBOOK_APP)
            .params(params)
            .engines(engines_with(http, Arc::new(Dead), Arc::new(Dead)))
            .build()
    }

    // ── the shipped URLs are ASSUMED, and must say so ───────────────────────

    /// The one claim this app must never make silently. If a URL is ever
    /// VERIFIED live, this test is the place the verification date lands —
    /// deleting the assertion without recording the verification is the
    /// failure mode it guards.
    #[test]
    fn every_shipped_url_is_on_the_portal_that_serves_the_verified_feeds() {
        for c in CODEBOOKS {
            assert!(
                c.url.starts_with("https://data.mpsv.cz/od/soubory/"),
                "{} must sit under the /od/soubory root the two verified MPSV feeds use: {}",
                c.name,
                c.url
            );
            assert!(c.url.ends_with(".json"), "{}", c.url);
            assert!(c.min_rows >= 2, "every codebook needs a real floor");
            assert!(c.uri_prefix.ends_with('/'), "{}", c.uri_prefix);
        }
        // Dataset names are keys downstream — a duplicate would silently make
        // one codebook overwrite another.
        let mut names: Vec<&str> = CODEBOOKS.iter().map(|c| c.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), CODEBOOKS.len());
    }

    #[test]
    fn a_param_url_overrides_the_assumed_constant_and_is_reported_as_such() {
        let over = json!({ "kraj": "https://example.test/kraj.json" });
        let (url, from_param) = resolve_url(book("kraj"), Some(&over));
        assert_eq!(url, "https://example.test/kraj.json");
        assert!(
            from_param,
            "a run must not report an operator URL as ASSUMED"
        );
        // A blank override is not an override — it would otherwise fetch "".
        let blank = json!({ "kraj": "   " });
        let (url, from_param) = resolve_url(book("kraj"), Some(&blank));
        assert_eq!(url, book("kraj").url);
        assert!(!from_param);
        let (url, from_param) = resolve_url(book("kraj"), None);
        assert_eq!(url, book("kraj").url);
        assert!(!from_param);
    }

    #[test]
    fn a_misspelled_codebook_name_is_refused_not_silently_skipped() {
        let err = selected_codebooks(Some(&json!(["kraje"]))).expect_err("unknown name");
        let msg = err.to_string();
        assert!(msg.contains("unknown codebook"), "{msg}");
        assert!(
            msg.contains("kraj"),
            "the refusal must list what IS known: {msg}"
        );
        // The honest selections.
        assert_eq!(
            selected_codebooks(None).expect("all").len(),
            CODEBOOKS.len()
        );
        assert_eq!(
            selected_codebooks(Some(&json!([])))
                .expect("empty = all")
                .len(),
            CODEBOOKS.len()
        );
        let one = selected_codebooks(Some(&json!(["kraj"]))).expect("subset");
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].name, "kraj");
    }

    // ── row shape ───────────────────────────────────────────────────────────

    #[test]
    fn a_label_is_read_from_any_of_the_portals_name_shapes_but_never_invented() {
        assert_eq!(
            row_labels(&json!({ "nazev": { "cs": "Jihomoravský kraj", "en": "South Moravian" } })),
            (
                Some("Jihomoravský kraj".to_string()),
                Some("South Moravian".to_string())
            )
        );
        assert_eq!(
            row_labels(&json!({ "nazev": "Praha" })),
            (Some("Praha".to_string()), None)
        );
        assert_eq!(
            row_labels(&json!({ "nazevCs": "Praha", "nazevEn": "Prague" })),
            (Some("Praha".to_string()), Some("Prague".to_string()))
        );
        // Blank and absent are the same honest absence — never an empty label
        // that would render as a nameless hit downstream.
        assert_eq!(
            row_labels(&json!({ "nazev": { "cs": "  " } })),
            (None, None)
        );
        assert_eq!(row_labels(&json!({ "id": "Kraj/116" })), (None, None));
    }

    #[test]
    fn a_row_without_an_id_is_dropped_not_stored_under_an_empty_key() {
        let rows = vec![
            json!({ "id": "Kraj/116", "nazev": { "cs": "Jihomoravský kraj" } }),
            json!({ "nazev": { "cs": "no id" } }),
            json!({ "id": "   ", "nazev": { "cs": "blank id" } }),
        ];
        let items = keyed_rows("kraj", &rows);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0, "Kraj/116");
        assert_eq!(items[0].1["label_cs"], "Jihomoravský kraj");
        assert_eq!(items[0].1["label_en"], Value::Null);
        assert_eq!(items[0].1["title"], "Jihomoravský kraj");
        assert_eq!(items[0].1["codebook"], "kraj");
    }

    /// An unlabelled row is still stored under its URI — dropping it would make
    /// the codebook claim the id does not exist, which is a stronger and wrong
    /// claim than "this id has no name".
    #[test]
    fn an_unlabelled_row_is_stored_with_a_null_label_not_dropped() {
        let items = keyed_rows("kraj", &[json!({ "id": "Kraj/999" })]);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].1["label_cs"], Value::Null);
        // The title falls back to the id, so a search hit is at least locatable.
        assert_eq!(items[0].1["title"], "Kraj/999");
        assert_eq!(labelled_pct(&items), 0.0);
        let mixed = keyed_rows(
            "kraj",
            &[
                json!({ "id": "Kraj/1", "nazev": { "cs": "a" } }),
                json!({ "id": "Kraj/2" }),
            ],
        );
        assert_eq!(labelled_pct(&mixed), 50.0);
    }

    // ── drift honesty ───────────────────────────────────────────────────────

    #[test]
    fn missing_polozky_key_is_drift_not_an_empty_codebook() {
        let err = polozky_rows(&json!({ "items": [] })).expect_err("drift");
        assert!(err.contains("no `polozky` key"), "{err}");
        assert!(err.contains("items"), "{err}");
        assert!(polozky_rows(&json!({ "polozky": { "0": {} } })).is_err());
        assert!(polozky_rows(&json!([])).is_err());
        // Present-but-empty is a claim, judged by the floor instead.
        assert_eq!(
            polozky_rows(&json!({ "polozky": [] })).expect("ok").len(),
            0
        );
    }

    #[test]
    fn each_codebook_floor_rejects_a_collapse_but_not_its_real_size() {
        // The regions codebook: 14 kraje is a vintage, 3 is a broken download.
        let kraj = book("kraj");
        assert!(implausibly_few_rows(0, kraj.min_rows));
        assert!(implausibly_few_rows(3, kraj.min_rows));
        assert!(!implausibly_few_rows(14, kraj.min_rows));
        // The skills codebook is thousands of rows — 14 would be a collapse
        // there, which is exactly why the floor is per codebook.
        let dov = book("dovednost");
        assert!(implausibly_few_rows(14, dov.min_rows));
        assert!(!implausibly_few_rows(4_000, dov.min_rows));
    }

    // ── run() end to end ────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_mirrors_every_codebook_and_reports_the_url_it_assumed() {
        let store = TempStore::new("ciselniky-run").await;
        let out = MpsvCiselniky
            .run(ctx_serving(&store, healthy_portal(), json!({})))
            .await
            .expect("healthy portal runs");
        let books = out["codebooks"].as_array().expect("codebooks");
        assert_eq!(books.len(), CODEBOOKS.len());
        for b in books {
            assert_eq!(b["urlSource"], "assumed");
            assert_eq!(b["labelledPct"], 100.0);
            assert_eq!(b["stored"], b["rows"]);
        }
        assert_eq!(out["index_datasets"], index_datasets_spec());
        let kraj = store
            .datasets()
            .list(CODEBOOK_APP, "kraj", 100)
            .await
            .expect("read back");
        assert_eq!(kraj.len(), book("kraj").min_rows + 2);
    }

    /// The gate: a codebook below its floor is a drift REFUSAL, and — because
    /// judging happens before any write — it leaves the whole label vintage
    /// untouched rather than half-refreshed.
    #[tokio::test]
    async fn a_collapsed_codebook_refuses_and_leaves_every_prior_label_in_place() {
        let store = TempStore::new("ciselniky-floor").await;
        MpsvCiselniky
            .run(ctx_serving(&store, healthy_portal(), json!({})))
            .await
            .expect("first vintage lands");

        // The skills codebook collapses; the other three are healthy.
        let mut portal = healthy_portal();
        portal.insert(
            book("dovednost").url.to_string(),
            (200, feed_of("Dovednost/", 3)),
        );
        let err = MpsvCiselniky
            .run(ctx_serving(&store, portal, json!({})))
            .await
            .expect_err("a collapsed codebook must fail the run");
        let msg = err.to_string();
        assert!(msg.contains("floor"), "{msg}");
        assert!(msg.contains("dovednost"), "{msg}");
        // Untouched, not halved: the prior vintage is still the label source.
        assert_eq!(
            store
                .datasets()
                .list(CODEBOOK_APP, "dovednost", 1_000)
                .await
                .expect("read back")
                .len(),
            book("dovednost").min_rows + 2
        );
    }

    #[tokio::test]
    async fn run_fails_on_drift_instead_of_reporting_a_clean_stored_zero() {
        let store = TempStore::new("ciselniky-drift").await;
        let mut portal = healthy_portal();
        portal.insert(
            book("kraj").url.to_string(),
            (200, json!({ "polozkyList": [] }).to_string()),
        );
        let err = MpsvCiselniky
            .run(ctx_serving(
                &store,
                portal,
                json!({ "codebooks": ["kraj"] }),
            ))
            .await
            .expect_err("drift must fail the run");
        let msg = err.to_string();
        assert!(msg.contains("source contract drift"), "{msg}");
        assert!(msg.contains("polozkyList"), "{msg}");
        assert!(store
            .datasets()
            .list(CODEBOOK_APP, "kraj", 10)
            .await
            .expect("read back")
            .is_empty());
    }

    /// A wrong ASSUMED slug is the FIRST thing this app will hit live, so its
    /// failure has to point at the fix rather than at a bare 404.
    #[tokio::test]
    async fn a_404_names_the_params_urls_escape_hatch() {
        let store = TempStore::new("ciselniky-404").await;
        let err = MpsvCiselniky
            .run(ctx_serving(&store, HashMap::new(), json!({})))
            .await
            .expect_err("404 fails");
        let msg = err.to_string();
        assert!(msg.contains("404"), "{msg}");
        assert!(msg.contains("ASSUMED"), "{msg}");
        assert!(msg.contains("params.urls"), "{msg}");
    }

    #[tokio::test]
    async fn a_param_url_is_fetched_and_reported_as_operator_supplied() {
        let store = TempStore::new("ciselniky-override").await;
        let mut portal = HashMap::new();
        portal.insert(
            "https://example.test/kraj.json".to_string(),
            (200, feed_of("Kraj/", 14)),
        );
        let out = MpsvCiselniky
            .run(ctx_serving(
                &store,
                portal,
                json!({
                    "codebooks": ["kraj"],
                    "urls": { "kraj": "https://example.test/kraj.json" }
                }),
            ))
            .await
            .expect("override runs");
        let books = out["codebooks"].as_array().expect("codebooks");
        assert_eq!(books.len(), 1);
        assert_eq!(books[0]["urlSource"], "param");
        assert_eq!(books[0]["url"], "https://example.test/kraj.json");
        assert_eq!(books[0]["stored"], 14);
    }

    #[test]
    fn manifest_examples_satisfy_the_declared_schema_shape() {
        let m = MpsvCiselniky.manifest();
        let schema = m.params_schema.expect("schema declared");
        let props = schema["properties"].as_object().expect("properties");
        assert!(props.contains_key("codebooks") && props.contains_key("urls"));
        assert_eq!(m.examples.len(), 2);
        // Scheduled runs enqueue default_params, which must be a valid input.
        assert_eq!(MpsvCiselniky.default_params(), json!({}));
        assert!(selected_codebooks(MpsvCiselniky.default_params().get("codebooks")).is_ok());
    }
}
