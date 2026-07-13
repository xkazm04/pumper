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

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{html_to_markdown, AppContext, Error, HttpMethod, HttpRequest, Result, ScrapeApp};
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
        json!({ "types": ["1", "2"], "statuses": ["31094502"], "pageSize": 100, "maxPages": 10 })
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
            .unwrap_or(10)
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
            let url = format!(
                "{SEDIA_URL}?apiKey=SEDIA&text=***&pageSize={page_size}&pageNumber={page}"
            );
            let resp = ctx.engines.http.fetch(sedia_request(url, body.clone())).await?;
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
                total = parsed.get("totalResults").and_then(Value::as_u64).unwrap_or(0);
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            let hits = parsed
                .get("results")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = hits.len() as u64;
            for hit in &hits {
                let (key, record) = normalize(hit);
                if record.get("description_text").is_some_and(|v| !v.is_null()) {
                    enriched += 1;
                }
                records.push((key, record));
            }
            pages_fetched += 1;
            page += 1;

            if got < page_size || (pages_fetched * page_size) >= total || pages_fetched >= max_pages
            {
                break;
            }
        }

        let summary = ctx.upsert_many("opportunities", &records).await?;

        Ok(json!({
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
        }))
    }
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
    strip_md(&html_to_markdown(s)).split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Removes the Markdown decoration `html_to_markdown` emits (headings `#`,
/// bold `**`, italics `_`, code ticks); list dashes are kept — they read fine
/// as plain text. Dropping `_` is safe here: SEDIA identifiers are hyphenated
/// (HORIZON-CL4-…), so prose underscores don't occur.
fn strip_md(md: &str) -> String {
    md.lines()
        .map(|l| l.trim_start_matches('#').trim_start().replace(['*', '`', '_'], ""))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

fn multipart_body(query: &str, languages: &str) -> String {
    let mut s = String::new();
    for (name, val) in [("query", query), ("languages", languages)] {
        s.push_str(&format!("--{BOUNDARY}\r\n"));
        s.push_str(&format!("Content-Disposition: form-data; name=\"{name}\"\r\n"));
        s.push_str("Content-Type: application/json\r\n\r\n");
        s.push_str(val);
        s.push_str("\r\n");
    }
    s.push_str(&format!("--{BOUNDARY}--\r\n"));
    s
}

#[cfg(test)]
mod tests {
    use super::{clean_inline, clean_text, normalize, DESCRIPTION_TEXT_CAP};
    use serde_json::json;

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
        assert!(text.contains("Expected Outcome: Projects are expected"), "{text}");
        assert!(text.contains("Improved R&I capacity – including SMEs;"), "{text}");
        assert!(text.contains("Uptake of <trustworthy> AI across the EU’s single market."), "{text}");
        assert!(text.contains("Scope: proposals should address interoperability."), "{text}");
        assert!(!text.contains('<') || text.contains("<trustworthy>"), "tag soup leaked: {text}");
        assert!(!text.contains("**") && !text.contains("&amp;"), "{text}");
    }

    #[test]
    fn caps_long_descriptions() {
        let html = format!("<p>{}</p>", "grant ".repeat(1000));
        let text = clean_text(&html).expect("non-empty");
        assert!(text.chars().count() <= DESCRIPTION_TEXT_CAP + 1, "len {}", text.chars().count());
        assert!(text.ends_with('…'), "missing truncation marker: {text}");
        assert!(clean_text("  <p> </p> ").is_none(), "blank HTML should yield None");
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
        assert!(rec["description_text"].as_str().unwrap().contains("Improved R&I capacity"));
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
    }
}
