//! EU Funding & Tenders Portal open calls via the SEDIA Search API — the pan-EU
//! open-calls feed (Horizon Europe, Erasmus+, CERV, LIFE, …), one source that
//! serves every EU member state. `http` engine.
//!
//! Data type: OPEN CALLS. Access: key-free (`apiKey=SEDIA` is a static public
//! key). Keyed by the topic `identifier` into the `opportunities` dataset. See
//! `catalog/data-sources.toml` (id `eu-sedia`) and the modeling note in the
//! grant-writing app's `docs/eu-market-deep-dive.md` (attach this as a shared
//! grant source on every EU member-state jurisdiction profile).
//!
//! Contract (verified 2026-07-03): POST-only, body is `multipart/form-data` with
//! a `query` part (Elasticsearch bool JSON) and a `languages` part (`["en"]`).
//! `text=***` (match-all) is REQUIRED in the query string; `pageSize` is hard-
//! capped at 100. Filter `type` in {1=grant topics, 2=PROSPECT} and
//! `status`=31094502 (open). Results are volatile (weight/checksum/highlights),
//! so we normalize each hit to a stable grant record before upserting.
//!
//! Win-intelligence (M31): Horizon topics additionally carry a `history` block
//! joined from the `cordis` app's `topic_stats` dataset — funded-outcome priors
//! (project count, EU contribution, top participant orgs) for the topic's
//! predecessor family, keyed by [`topic_lineage`]. Topics whose family has no
//! stats — or whose family has been **tombstoned** because it left the CORDIS
//! corpus — get no block. The block carries `as_of` (when cordis last confirmed
//! those numbers) and, inside `stats`, cordis's own `coverage`: the walk takes
//! ~46 weeks, so these are partial-corpus priors for most of a year and must
//! say so. Queryable via `?filter=history.stats.project_count:gte:1`.

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{
    html_to_markdown, AppContext, AppManifest, CostClass, Error, HttpMethod, HttpRequest,
    ManifestExample, Provenance, Record, Result, ScrapeApp,
};
use serde_json::{json, Value};

pub struct EuSedia;

const SEDIA_URL: &str = "https://api.tech.ec.europa.eu/search-api/prod/rest/search";
// Multipart boundary — a fixed token that never appears in the JSON parts.
const BOUNDARY: &str = "----PumperSediaBoundaryQ1W2E3R4T5Y6";

#[async_trait]
impl ScrapeApp for EuSedia {
    fn name(&self) -> &'static str {
        "eu-sedia"
    }

    fn description(&self) -> &'static str {
        "EU Funding & Tenders Portal open calls (SEDIA Search API, key-free). \
         Pan-EU grant topics keyed by identifier into the `opportunities` dataset. \
         Params: {\"types\": [\"1\",\"2\"] (1=grants,2=PROSPECT), \
         \"statuses\": [\"31094502\"] (open; 31094501=forthcoming), \
         \"pageSize\": 1-100, \"maxPages\": 1-50}"
    }

    /// Daily at 10:00 UTC.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 10 * * *")
    }

    fn default_params(&self) -> Value {
        // maxPages 50 (the clamp max) → up to 5000 topics at pageSize 100, so the
        // 1000-topic-plus Horizon corpus isn't silently cut. SEDIA's match-all is
        // server-relevance-ordered with no stable sort we can pin, so the window is
        // not deterministic across runs; covering the whole corpus is what keeps
        // topics from drifting in and out of it. `truncated` is the tripwire if
        // even 5000 isn't enough.
        json!({ "types": ["1", "2"], "statuses": ["31094502"], "pageSize": 100, "maxPages": 50 })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "types": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "SEDIA `type` facet codes as STRINGS: \"1\" = grant topics, \"2\" = PROSPECT."
                    },
                    "statuses": {
                        "type": "array",
                        "items": { "type": "string" },
                        "minItems": 1,
                        "description": "SEDIA `status` facet codes as STRINGS: \"31094502\" = open, \"31094501\" = forthcoming, \"31094503\" = closed."
                    },
                    "pageSize": {
                        "type": "integer", "minimum": 1, "maximum": 100,
                        "description": "Hard-capped at 100 by the SEDIA API."
                    },
                    "maxPages": {
                        "type": "integer", "minimum": 1, "maximum": 50,
                        "description": "Page cap. SEDIA's match-all window has no stable sort, so a truncated run's uncovered topics drift between runs — reported as `truncated` plus a warning."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Daily sweep of every open pan-EU topic (the scheduled default)",
                    params: json!({
                        "types": ["1", "2"], "statuses": ["31094502"],
                        "pageSize": 100, "maxPages": 50
                    }),
                },
                ManifestExample {
                    description: "Forward look: forthcoming grant topics only, first few pages",
                    params: json!({
                        "types": ["1"], "statuses": ["31094501"],
                        "pageSize": 100, "maxPages": 5
                    }),
                },
            ],
            output_shape: Some(
                "{source, types[], statuses[], totalResults, fetched, enriched, pages, new, \
                 changed, unchanged, historyJoined, truncated, unified: {new, changed, events}, \
                 swept, crossSourceDups, recurrenceLinks, \
                 corpusPass: {ran, cycle, batchSwept, corpusSwept}, \
                 warnings[], index_datasets[]} — the corpus-wide relation pass (sweep + \
                 duplicate/recurrence links) runs once per UTC-day cycle on whichever grant \
                 source gets there first, so a run that did not own it reports \
                 `crossSourceDups`/`recurrenceLinks` as null (not 0) — normalized topics in \
                 the `opportunities` dataset (keyed by topic identifier), Horizon topics \
                 carrying a `history` block joined from cordis/topic_stats \
                 (`{family, source, as_of, stats}`, where `stats.coverage` says how much of \
                 the ~23k-project CORDIS corpus those priors rest on; a tombstoned family \
                 yields no block)",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let types = string_array(&ctx.params, "types", &["1", "2"]);
        let statuses = string_array(&ctx.params, "statuses", &["31094502"]);
        let page_size = ctx
            .params
            .get("pageSize")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 100);
        let max_pages = ctx
            .params
            .get("maxPages")
            .and_then(Value::as_u64)
            .unwrap_or(50)
            .clamp(1, 50);

        // Elasticsearch-style bool query: open grant topics.
        let query = json!({
            "bool": { "must": [
                { "terms": { "type": types } },
                { "terms": { "status": statuses } },
            ] }
        })
        .to_string();
        let languages = json!(["en"]).to_string();
        let body = multipart_body(&query, &languages);

        let mut records: Vec<(String, Value)> = Vec::new();
        let mut enriched: u64 = 0;
        let mut total: u64 = 0;
        let mut page: u64 = 1;
        let mut pages_fetched: u64 = 0;

        loop {
            let url =
                format!("{SEDIA_URL}?apiKey=SEDIA&text=***&pageSize={page_size}&pageNumber={page}");
            let resp = ctx
                .engines
                .http
                .fetch(sedia_request(url, body.clone()))
                .await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "SEDIA returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }

            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("eu-sedia: response was not JSON: {e}")))?;
            if pages_fetched == 0 {
                total = parsed
                    .get("totalResults")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            // Borrow the results array rather than cloning it whole — each hit's
            // `descriptionByte` is tens of KB of topic HTML, and `normalize` clones
            // only the fields it keeps.
            let hits = parsed.get("results").and_then(Value::as_array);
            let got = hits.map_or(0, Vec::len) as u64;
            if pages_fetched == 0 && total > 0 && got == 0 {
                // Positive totalResults but zero parsed rows means the `results`
                // array was renamed/moved upstream. Refuse to report an empty run
                // as success (grants-gov guards this same drift).
                return Err(Error::App(format!(
                    "eu-sedia: API reported {total} results but parsed 0 rows from \
                     'results' — likely an upstream schema change"
                )));
            }
            if let Some(hits) = hits {
                for hit in hits {
                    let (key, record) = normalize(hit);
                    if record.get("description_text").is_some_and(|v| !v.is_null()) {
                        enriched += 1;
                    }
                    records.push((key, record));
                }
            }
            pages_fetched += 1;
            page += 1;

            if got < page_size || (pages_fetched * page_size) >= total || pages_fetched >= max_pages
            {
                break;
            }
        }

        // Honest coverage: hitting the page cap while topics remain is a
        // silently-partial, non-deterministic window (SEDIA match-all has no stable
        // sort), so a truncated run must not read as a clean sweep.
        let truncated = pages_fetched >= max_pages && (pages_fetched * page_size) < total;

        // Win-intelligence join (moonshot M31): annotate each open Horizon topic
        // with its predecessor-family funded-outcomes stats from the `cordis`
        // app's `topic_stats` dataset (read via the dataset store — no cross-app
        // fetch). One read per distinct family, not per record. A family with no
        // stats record gets NO `history` block — absence of evidence is never
        // rendered as a zero-project history.
        let mut family_stats: HashMap<String, Option<Value>> = HashMap::new();
        let mut history_joined: u64 = 0;
        for (key, record) in &mut records {
            let Some(family) = topic_lineage(key) else {
                continue;
            };
            let block = match family_stats.get(&family) {
                Some(cached) => cached.clone(),
                None => {
                    let fetched = ctx
                        .datasets
                        .get("cordis", "topic_stats", &family)
                        .await?
                        .and_then(|rec| history_block(&family, rec));
                    family_stats.insert(family.clone(), fetched.clone());
                    fetched
                }
            };
            if let (Some(block), Value::Object(map)) = (block, &mut *record) {
                map.insert(HISTORY_FIELD.into(), block);
                history_joined += 1;
            }
        }

        // Provenance (M12): every page hits the one SEDIA search endpoint (only
        // the paging query-string differs), so the batch-level `source_url` is
        // honest. `rules_hash` stays Null deliberately — `normalize` is Rust
        // code, not a registered RuleSet, so there is nothing replayable to pin.
        let summary = ctx
            .upsert_many_with_provenance(
                "opportunities",
                &records,
                Provenance {
                    source_url: Some(SEDIA_URL.to_string()),
                    ..Provenance::default()
                },
            )
            .await?;

        // Cross-source layer: publish the pan-EU corpus into grants/unified so it
        // joins GET /grants filtering, closing-soon, sweep_closed, cross-source
        // dedup, and per-opportunity search — the same tail grants-gov/ca-grants
        // run. Normalizes from the already-cleaned `opportunities` records.
        let unified_items: Vec<(String, Value)> = records
            .iter()
            .filter_map(|(_, rec)| grants_common::normalize_eu_sedia(rec))
            .collect();
        let cross = grants_common::finalize_unified(&ctx, &unified_items, Some(SEDIA_URL)).await?;

        let mut out = json!({
            "source": "ec.europa.eu/funding-tenders/sedia",
            "types": types,
            "statuses": statuses,
            "totalResults": total,
            "fetched": records.len(),
            "enriched": enriched,
            "pages": pages_fetched,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "historyJoined": history_joined,
            "truncated": truncated,
        });
        cross.merge_into(&mut out);
        if truncated {
            // After merge_into, which sets `warnings` to the drift warnings.
            if let Value::Object(map) = &mut out {
                let msg = format!(
                    "coverage truncated: stopped at maxPages={max_pages} after {} of \
                     {total} topics — the SEDIA match-all window is non-deterministic, \
                     so uncovered topics drift in and out between runs",
                    records.len()
                );
                match map.get_mut("warnings") {
                    Some(Value::Array(w)) => w.push(json!(msg)),
                    _ => {
                        map.insert("warnings".into(), json!([msg]));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// The record field the CORDIS win-intelligence join writes into. Named once
/// because two things have to agree about it: the join below, and the
/// derived-path declaration at the upsert.
const HISTORY_FIELD: &str = "history";

/// The `history` block for one open topic, or `None` when there is nothing
/// honest to attach.
///
/// A **tombstoned** stats record yields `None`. cordis's rollup is a complete
/// recompute, so a family whose projects left the corpus is tombstoned — and
/// `Datasets::get` returns tombstoned rows (only the filtered/list reads exclude
/// them). Joining one anyway is exactly how a ghost family outlives the corpus
/// it was computed from and keeps being served as a funded-outcome prior.
///
/// `as_of` comes off the record **envelope**, not out of the stats value: the
/// store refreshes `last_seen` on every rollup, unchanged families included, so
/// it is the honest "cordis last confirmed these numbers at" — and it costs no
/// weekly content churn on every family the way a stamped field would.
fn history_block(family: &str, rec: Record) -> Option<Value> {
    if rec.removed_at.is_some() {
        return None;
    }
    Some(json!({
        "family": family,
        "source": "cordis",
        "as_of": rec.last_seen.to_rfc3339(),
        // Carries cordis's own `coverage` block: how much of the ~23k-project
        // corpus these numbers rest on. A partial-walk prior must not read like
        // a complete one.
        "stats": rec.data,
    }))
}

/// Horizon topic-family key: the identifier with its call-year segment removed,
/// so successor topics across work programmes collapse onto one lineage key
/// (`HORIZON-CL4-2026-DATA-01` and `HORIZON-CL4-2024-DATA-01` →
/// `HORIZON-CL4-DATA-01`). This is the join key between open SEDIA topics and
/// funded CORDIS outcomes (the `cordis` app aggregates `topic_stats` per family).
///
/// **Horizon-only by design**: identifiers must start with `HORIZON-` and carry
/// exactly one plausible call-year segment (2020–2039). Other programmes
/// (Erasmus+, LIFE, CERV, …) use different, less regular grammars — returning
/// `None` there is honest; guessing a family would fabricate lineage. Counter
/// segments (`-01`, `-01-05`) are kept: they distinguish topics within a
/// destination, which is exactly the granularity predecessor matching needs.
/// If a second year-like segment appears, only the first is removed.
pub fn topic_lineage(identifier: &str) -> Option<String> {
    let id = identifier.trim();
    let segments: Vec<&str> = id.split('-').collect();
    if segments.first() != Some(&"HORIZON") || segments.len() < 3 {
        return None;
    }
    let is_year = |s: &str| {
        s.len() == 4
            && s.chars().all(|c| c.is_ascii_digit())
            && (2020..=2039).contains(&s.parse::<u32>().unwrap_or(0))
    };
    let year_pos = segments.iter().position(|s| is_year(s))?;
    let family: Vec<&str> = segments
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != year_pos)
        .map(|(_, s)| *s)
        .collect();
    // A family must still name something beyond the programme prefix.
    if family.len() < 2 {
        return None;
    }
    Some(family.join("-"))
}

/// Reads a params array of strings, or a fallback. Accepts `["1","2"]`.
fn string_array(params: &Value, key: &str, fallback: &[&str]) -> Vec<String> {
    params
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.iter().map(|s| s.to_string()).collect())
}

/// Normalize one SEDIA hit to a stable grant record (dropping volatile fields
/// like weight/checksum/highlightedFragments so change-detection is meaningful).
/// SEDIA metadata values are arrays — take the first, except deadlines (kept whole).
fn normalize(hit: &Value) -> (String, Value) {
    let m = hit.get("metadata").cloned().unwrap_or(Value::Null);
    let reference = hit.get("reference").and_then(Value::as_str).unwrap_or("");
    let identifier = first(&m, "identifier").unwrap_or(reference).to_string();

    let record = json!({
        "identifier": identifier,
        "reference": reference,
        // Titles come back entity-escaped (&amp;, &#8211;, …) — store the decoded
        // human-readable form; raw HTML lives only in descriptionByte anyway.
        "title": first(&m, "title").map(clean_inline),
        "summary": hit.get("summary").and_then(Value::as_str),
        // The REAL topic description (Expected Outcome / Scope / Specific challenge)
        // as HTML — the search `summary` is just a title echo, so this is what carries
        // the substance. Kept raw for fidelity... (data-hygiene P6b)
        "descriptionByte": first(&m, "descriptionByte"),
        // ...and enriched as capped plain text so stored records are readable and
        // search indexing isn't polluted by tag soup (idea 5c873722).
        "description_text": first(&m, "descriptionByte").and_then(clean_text),
        "url": hit.get("url").and_then(Value::as_str),
        "status": first(&m, "status"),
        "type": first(&m, "type"),
        "callIdentifier": first(&m, "callIdentifier"),
        "callTitle": first(&m, "callTitle").map(clean_inline),
        "frameworkProgramme": first(&m, "frameworkProgramme"),
        "programmePeriod": first(&m, "programmePeriod"),
        "typesOfAction": first(&m, "typesOfAction"),
        "startDate": first(&m, "startDate"),
        "deadlineDate": m.get("deadlineDate").cloned().unwrap_or(Value::Null),
        "deadlineModel": first(&m, "deadlineModel"),
        "budgetOverview": first(&m, "budgetOverview"),
    });
    (identifier.clone(), record)
}

/// First element of a SEDIA metadata array field, as a &str.
fn first<'a>(metadata: &'a Value, key: &str) -> Option<&'a str> {
    metadata.get(key)?.as_array()?.first()?.as_str()
}

/// Cap for `description_text` — enough for a full Expected Outcome / Scope intro
/// without bloating records (full HTML stays in `descriptionByte`).
const DESCRIPTION_TEXT_CAP: usize = 2000;

/// SEDIA `descriptionByte` HTML -> capped plain text. Reuses core's
/// `html_to_markdown` (entity decode + tag strip + whitespace collapse), then
/// drops the residual Markdown decoration so the field is genuinely plain.
fn clean_text(html: &str) -> Option<String> {
    let text = strip_md(&html_to_markdown(html));
    if text.is_empty() {
        return None;
    }
    // Truncate on a char boundary; mark the cut so consumers know it's partial.
    if text.chars().count() > DESCRIPTION_TEXT_CAP {
        let cut: String = text.chars().take(DESCRIPTION_TEXT_CAP).collect();
        Some(format!("{}…", cut.trim_end()))
    } else {
        Some(text)
    }
}

/// Single-line variant for titles: entity-escaped fragments -> decoded text
/// with all whitespace collapsed to single spaces.
fn clean_inline(s: &str) -> String {
    strip_md(&html_to_markdown(s))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Removes the Markdown decoration `html_to_markdown` emits (headings `#`,
/// bold `**`, italics `_`, code ticks); list dashes are kept — they read fine
/// as plain text. Dropping `_` is safe here: SEDIA identifiers are hyphenated
/// (HORIZON-CL4-…), so prose underscores don't occur.
fn strip_md(md: &str) -> String {
    md.lines()
        .map(|l| {
            l.trim_start_matches('#')
                .trim_start()
                .replace(['*', '`', '_'], "")
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn multipart_body(query: &str, languages: &str) -> String {
    let mut s = String::new();
    for (name, val) in [("query", query), ("languages", languages)] {
        s.push_str(&format!("--{BOUNDARY}\r\n"));
        s.push_str(&format!(
            "Content-Disposition: form-data; name=\"{name}\"\r\n"
        ));
        s.push_str("Content-Type: application/json\r\n\r\n");
        s.push_str(val);
        s.push_str("\r\n");
    }
    s.push_str(&format!("--{BOUNDARY}--\r\n"));
    s
}

fn sedia_request(url: String, body: String) -> HttpRequest {
    let mut headers = HashMap::new();
    headers.insert(
        "Content-Type".to_string(),
        format!("multipart/form-data; boundary={BOUNDARY}"),
    );
    headers.insert("Accept".to_string(), "application/json".to_string());
    HttpRequest {
        url,
        method: HttpMethod::Post,
        headers,
        body: Some(body),
        no_cache: false,
        ttl_override: None,
        etag: None,
        if_modified_since: None,
        max_body_bytes: None,
        timeout_secs: None,
        proxy: None,
        profile: None,
        archive_max_age: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{clean_inline, clean_text, normalize, topic_lineage, DESCRIPTION_TEXT_CAP};
    use serde_json::json;

    #[test]
    fn lineage_strips_year_and_keeps_counters() {
        assert_eq!(
            topic_lineage("HORIZON-CL4-2026-DATA-01").as_deref(),
            Some("HORIZON-CL4-DATA-01")
        );
        // Successor topics across programme years share one family.
        assert_eq!(
            topic_lineage("HORIZON-CL4-2024-DATA-01"),
            topic_lineage("HORIZON-CL4-2026-DATA-01")
        );
        // Double counters (destination + topic) are kept whole.
        assert_eq!(
            topic_lineage("HORIZON-MSCA-2024-PF-01-01").as_deref(),
            Some("HORIZON-MSCA-PF-01-01")
        );
        assert_eq!(
            topic_lineage("HORIZON-EIC-2025-PATHFINDEROPEN-01").as_deref(),
            Some("HORIZON-EIC-PATHFINDEROPEN-01")
        );
    }

    #[test]
    fn lineage_is_horizon_only_and_never_guesses() {
        // Other programmes have different grammars — no family, not a wrong one.
        assert_eq!(topic_lineage("ERASMUS-EDU-2026-PEX-TEACH-ACA"), None);
        assert_eq!(topic_lineage("LIFE-2026-SAP-NAT"), None);
        assert_eq!(topic_lineage("CERV-2025-CHILD"), None);
        // Horizon identifiers without a plausible call-year segment.
        assert_eq!(topic_lineage("HORIZON-JU-CLEANH2"), None);
        // 4-digit segment outside the 2020-2039 window is not a year.
        assert_eq!(topic_lineage("HORIZON-CL3-0142-X-01"), None);
        // Nothing left beyond the prefix once the year goes.
        assert_eq!(topic_lineage("HORIZON-2026"), None);
        assert_eq!(topic_lineage(""), None);
    }

    #[test]
    fn lineage_removes_only_the_first_year_like_segment() {
        assert_eq!(
            topic_lineage("HORIZON-CL5-2024-2030-VISION-01").as_deref(),
            Some("HORIZON-CL5-2030-VISION-01")
        );
    }

    /// Realistic SEDIA descriptionByte shape: entities, nested tags, boilerplate
    /// whitespace, list markup.
    const SEDIA_HTML: &str = "<p><strong>Expected Outcome:</strong>&nbsp;Projects are expected to \
         contribute to the following outcomes:</p>\n\n<ul>\n<li>Improved R&amp;I capacity \
         &#8211; including <em>SMEs</em>;</li>\n<li>Uptake of &lt;trustworthy&gt; AI \
         across the EU&rsquo;s single market.</li>\n</ul>\n<p>  Scope:   proposals should \
         address\u{a0}interoperability.</p>";

    #[test]
    fn cleans_sedia_html_to_plain_text() {
        let text = clean_text(SEDIA_HTML).expect("non-empty");
        // Entities decoded, tags gone, markdown decoration stripped.
        assert!(
            text.contains("Expected Outcome: Projects are expected"),
            "{text}"
        );
        assert!(
            text.contains("Improved R&I capacity – including SMEs;"),
            "{text}"
        );
        assert!(
            text.contains("Uptake of <trustworthy> AI across the EU’s single market."),
            "{text}"
        );
        assert!(
            text.contains("Scope: proposals should address interoperability."),
            "{text}"
        );
        assert!(
            !text.contains('<') || text.contains("<trustworthy>"),
            "tag soup leaked: {text}"
        );
        assert!(!text.contains("**") && !text.contains("&amp;"), "{text}");
    }

    #[test]
    fn caps_long_descriptions() {
        let html = format!("<p>{}</p>", "grant ".repeat(1000));
        let text = clean_text(&html).expect("non-empty");
        assert!(
            text.chars().count() <= DESCRIPTION_TEXT_CAP + 1,
            "len {}",
            text.chars().count()
        );
        assert!(text.ends_with('…'), "missing truncation marker: {text}");
        assert!(
            clean_text("  <p> </p> ").is_none(),
            "blank HTML should yield None"
        );
    }

    #[test]
    fn normalize_enriches_and_keeps_raw() {
        let hit = json!({
            "reference": "REF-1",
            "url": "https://ec.europa.eu/x",
            "summary": "echo",
            "metadata": {
                "identifier": ["HORIZON-CL4-2026-DATA-01"],
                "title": ["AI &amp; Robotics &#8211; Phase II"],
                "callTitle": ["Digital &amp; Industry"],
                "descriptionByte": [SEDIA_HTML],
            }
        });
        let (key, rec) = normalize(&hit);
        assert_eq!(key, "HORIZON-CL4-2026-DATA-01");
        // Raw HTML preserved, clean text added alongside.
        assert_eq!(rec["descriptionByte"].as_str().unwrap(), SEDIA_HTML);
        assert!(rec["description_text"]
            .as_str()
            .unwrap()
            .contains("Improved R&I capacity"));
        // Entity-escaped titles normalized.
        assert_eq!(rec["title"], "AI & Robotics – Phase II");
        assert_eq!(rec["callTitle"], "Digital & Industry");
        assert_eq!(clean_inline("Plain title"), "Plain title");
    }

    #[test]
    fn normalize_without_description_leaves_null() {
        let hit = json!({
            "reference": "REF-2",
            "metadata": { "identifier": ["ID-2"], "title": ["T"] }
        });
        let (_, rec) = normalize(&hit);
        assert!(rec["description_text"].is_null());
        assert!(rec["descriptionByte"].is_null());
    }
}

/// The CORDIS win-intelligence join, end to end against a real store — because
/// what it must NOT join (a tombstoned family) is a property of the store, not
/// of any pure function.
#[cfg(test)]
mod history_join_tests {
    use super::*;
    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::HttpResponse;
    use std::sync::Arc;

    /// Two open Horizon topics, one per family — a SEDIA response with nothing
    /// interesting in it except the two identifiers the join keys on.
    struct ScriptedSedia;

    #[async_trait]
    impl pumper_core::HttpClient for ScriptedSedia {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            let hit = |id: &str| {
                json!({
                    "reference": id, "url": "https://ec.europa.eu/x", "summary": "s",
                    "metadata": {
                        "identifier": [id], "title": ["T"], "status": ["31094502"],
                    }
                })
            };
            let body = json!({
                "totalResults": 2,
                "results": [
                    hit("HORIZON-CL4-2026-DATA-01"),
                    hit("HORIZON-CL4-2026-GHOST-01"),
                ]
            });
            Ok(HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body: body.to_string(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// A family that left the CORDIS corpus is tombstoned by the rollup — and
    /// `Datasets::get` still returns tombstoned rows, so the join used to keep
    /// serving its stats as a live funded-outcome prior forever.
    #[tokio::test]
    async fn a_tombstoned_family_yields_no_history_block() {
        let store = TempStore::new("eu-sedia-history").await;
        let ds = store.datasets();
        let stats = |family: &str| {
            json!({
                "family": family, "project_count": 3, "contribution_known": 3,
                "total_ec_contribution": 6_300_000.0, "mean_ec_contribution": 2_100_000.0,
                "top_participants": [], "coverage": {
                    "corpus_aggregated": 1_200, "corpus_total": 23_361, "corpus_swept": false
                }
            })
        };
        for family in ["HORIZON-CL4-DATA-01", "HORIZON-CL4-GHOST-01"] {
            ds.upsert("cordis", "topic_stats", family, &stats(family))
                .await
                .unwrap();
        }
        ds.tombstone_keys(
            "cordis",
            "topic_stats",
            &["HORIZON-CL4-GHOST-01".to_string()],
        )
        .await
        .unwrap();

        let engines = engines_with(Arc::new(ScriptedSedia), Arc::new(Dead), Arc::new(Dead));
        let ctx = TestContext::new(&store.storage, "eu-sedia")
            .params(json!({ "pageSize": 100, "maxPages": 1 }))
            .engines(engines)
            .build();
        let out = EuSedia.run(ctx).await.expect("run");
        assert_eq!(out["fetched"], 2);
        assert_eq!(out["historyJoined"], 1, "the ghost must not be joined");

        let live = ds
            .get("eu-sedia", "opportunities", "HORIZON-CL4-2026-DATA-01")
            .await
            .unwrap()
            .unwrap();
        let history = &live.data["history"];
        assert_eq!(history["family"], "HORIZON-CL4-DATA-01");
        assert_eq!(history["source"], "cordis");
        // Partial-corpus context rides through to the consumer.
        assert_eq!(history["stats"]["coverage"]["corpus_aggregated"], 1_200);
        assert_eq!(history["stats"]["coverage"]["corpus_swept"], false);
        assert!(
            history["as_of"].as_str().is_some_and(|s| s.contains('T')),
            "the block must say when cordis last confirmed these numbers: {history}"
        );

        let ghost = ds
            .get("eu-sedia", "opportunities", "HORIZON-CL4-2026-GHOST-01")
            .await
            .unwrap()
            .unwrap();
        assert!(
            ghost.data.get("history").is_none(),
            "a removed family must leave no history behind: {}",
            ghost.data
        );
    }
}
