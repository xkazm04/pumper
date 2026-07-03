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
use pumper_core::{AppContext, Error, HttpMethod, HttpRequest, Result, ScrapeApp};
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
        "title": first(&m, "title"),
        "summary": hit.get("summary").and_then(Value::as_str),
        "url": hit.get("url").and_then(Value::as_str),
        "status": first(&m, "status"),
        "type": first(&m, "type"),
        "callIdentifier": first(&m, "callIdentifier"),
        "callTitle": first(&m, "callTitle"),
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

fn sedia_request(url: String, body: String) -> HttpRequest {
    let mut headers = HashMap::new();
    headers.insert(
        "Content-Type".to_string(),
        format!("multipart/form-data; boundary={BOUNDARY}"),
    );
    headers.insert("Accept".to_string(), "application/json".to_string());
    HttpRequest { url, method: HttpMethod::Post, headers, body: Some(body), no_cache: false }
}
