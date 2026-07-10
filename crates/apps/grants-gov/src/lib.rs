//! Grants.gov federal grant opportunities via the Search2 JSON API.
//!
//! The US-federal open-calls backbone: every federal grant opportunity, keyed by
//! its stable opportunity id, upserted into the `opportunities` dataset so a
//! scheduled daily run only surfaces what is genuinely new or changed. This is
//! the fast path — a POST-only JSON API, no HTML parsing, no browser.
//!
//! Data type: OPEN CALLS (posted + forecasted). Access: key-free. See
//! `catalog/data-sources.toml` (id `grants-gov`) for how this fits the pipeline map.
//!
//! Contract notes (verified 2026-07-03): `https://api.grants.gov/v1/api/search2`
//! is **POST-only** — a bare GET returns 403. The body is JSON; pagination is
//! `startRecordNum` + `rows`; results live under `data.oppHits[]` with
//! `data.hitCount` as the total.

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpMethod, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct GrantsGov;

const SEARCH2_URL: &str = "https://api.grants.gov/v1/api/search2";

#[async_trait]
impl ScrapeApp for GrantsGov {
    fn name(&self) -> &'static str {
        "grants-gov"
    }

    fn description(&self) -> &'static str {
        "US federal grant opportunities (Grants.gov Search2 API, key-free). \
         Open calls, keyed by opportunity id into the `opportunities` dataset. \
         Params: {\"oppStatuses\": \"posted|forecasted\", \"keyword\": \"\", \
         \"eligibilities\": \"\" (pipe-separated grants.gov codes, e.g. 12|13|25|99 \
         for nonprofits), \"rows\": 1-1000, \"maxPages\": 1-100}"
    }

    /// Daily full sync of open opportunities at 09:00 UTC. Scheduled runs use
    /// `default_params`, which are sufficient (posted+forecasted, 25×100 rows).
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 9 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "oppStatuses": "posted|forecasted", "rows": 100, "maxPages": 25 })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let statuses = ctx
            .params
            .get("oppStatuses")
            .and_then(Value::as_str)
            .unwrap_or("posted|forecasted")
            .to_string();
        let keyword = ctx
            .params
            .get("keyword")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let eligibilities = ctx
            .params
            .get("eligibilities")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let rows = ctx
            .params
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000);
        let max_pages = ctx
            .params
            .get("maxPages")
            .and_then(Value::as_u64)
            .unwrap_or(25)
            .clamp(1, 100);

        let mut hits: Vec<Value> = Vec::new();
        let mut hit_count: u64 = 0;
        let mut start: u64 = 0;
        let mut pages: u64 = 0;

        loop {
            let body = json!({
                "keyword": keyword,
                "oppNum": "",
                "eligibilities": eligibilities,
                "agencies": "",
                "oppStatuses": statuses,
                "aln": "",
                "fundingCategories": "",
                "rows": rows,
                "startRecordNum": start,
            })
            .to_string();

            let resp = ctx.engines.http.fetch(search2_request(body)).await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "grants.gov search2 returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }

            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("grants.gov: response was not JSON: {e}")))?;
            // errorcode 0 = success; anything else is an application-level error.
            if parsed.get("errorcode").and_then(Value::as_i64).unwrap_or(0) != 0 {
                return Err(Error::App(format!(
                    "grants.gov error: {}",
                    parsed.get("msg").and_then(Value::as_str).unwrap_or("unknown")
                )));
            }

            let data = parsed.get("data").cloned().unwrap_or(Value::Null);
            if pages == 0 {
                hit_count = data.get("hitCount").and_then(Value::as_u64).unwrap_or(0);
                // Keep the first raw page for debugging / schema drift checks.
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            let page_hits = data
                .get("oppHits")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = page_hits.len() as u64;
            hits.extend(page_hits);
            pages += 1;
            start += rows;

            // Stop when the page came back short, we've covered hitCount, or hit the cap.
            if got < rows || start >= hit_count || pages >= max_pages {
                break;
            }
        }

        // Dedup + change detection: key each opportunity by its stable id (falling
        // back to the opportunity number, then row index). A scheduled run reports
        // only new/changed opportunities — the substrate for deadline alerts.
        let items: Vec<(String, Value)> = hits
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let key = h
                    .get("id")
                    .and_then(Value::as_str)
                    .or_else(|| h.get("number").and_then(Value::as_str))
                    .map(String::from)
                    .unwrap_or_else(|| format!("row-{i}"));
                (key, h.clone())
            })
            .collect();

        let summary = ctx.upsert_many("opportunities", &items).await?;

        // Cross-source layer: normalize into grants/unified and link SimHash
        // near-duplicates syndicated across portals.
        let unified_items: Vec<(String, Value)> = hits
            .iter()
            .filter_map(grants_common::normalize_grants_gov)
            .collect();
        let unified = grants_common::sync_unified(&ctx, &unified_items).await?;
        let cross_source_dups = grants_common::link_duplicates(&ctx, 3).await?;

        // Closing-soon digest: posted opportunities whose closeDate falls within
        // the next `digestDays` days, soonest first — the deadline-alert surface
        // this dataset was always meant to feed.
        let digest_days = ctx
            .params
            .get("digestDays")
            .and_then(Value::as_u64)
            .unwrap_or(14)
            .clamp(1, 365) as i64;
        let closing_soon = closing_soon_digest(&hits, digest_days);
        ctx.save_artifact("closing_soon.json", &serde_json::to_vec_pretty(&closing_soon)?)
            .await?;

        Ok(json!({
            "source": "grants.gov/search2",
            "oppStatuses": statuses,
            "hitCount": hit_count,
            "fetched": hits.len(),
            "pages": pages,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "digestDays": digest_days,
            "closingSoonCount": closing_soon.len(),
            "closingSoon": closing_soon.iter().take(25).collect::<Vec<_>>(),
            "unified": { "new": unified.new.len(), "changed": unified.changed.len() },
            "crossSourceDups": cross_source_dups,
        }))
    }
}

/// Posted opportunities closing within `days` days, sorted soonest-first.
/// Each entry keeps just what an alert needs: id, number, title, agency,
/// close date, and days left.
fn closing_soon_digest(hits: &[Value], days: i64) -> Vec<Value> {
    let today = chrono::Utc::now().date_naive();
    let mut digest: Vec<(i64, Value)> = hits
        .iter()
        .filter(|h| {
            h.get("oppStatus")
                .and_then(Value::as_str)
                .map_or(true, |s| s.eq_ignore_ascii_case("posted"))
        })
        .filter_map(|h| {
            let close = h.get("closeDate").and_then(Value::as_str)?;
            let close = parse_close_date(close)?;
            let days_left = (close - today).num_days();
            (0..=days).contains(&days_left).then(|| {
                (
                    days_left,
                    json!({
                        "id": h.get("id"),
                        "number": h.get("number"),
                        "title": h.get("title"),
                        "agency": h.get("agency").or_else(|| h.get("agencyCode")),
                        "closeDate": close.to_string(),
                        "daysLeft": days_left,
                    }),
                )
            })
        })
        .collect();
    digest.sort_by_key(|(days_left, _)| *days_left);
    digest.into_iter().map(|(_, v)| v).collect()
}

/// Grants.gov emits US-style `MM/DD/YYYY`; tolerate ISO `YYYY-MM-DD` too so a
/// schema drift doesn't silently empty the digest.
fn parse_close_date(s: &str) -> Option<chrono::NaiveDate> {
    chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
        .or_else(|_| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .ok()
}

/// A POST request to the Search2 endpoint carrying a JSON body. The API is
/// POST-only (a bare GET is 403), so this can't use `HttpRequest::get`.
fn search2_request(body: String) -> HttpRequest {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("Accept".to_string(), "application/json".to_string());
    HttpRequest {
        url: SEARCH2_URL.to_string(),
        method: HttpMethod::Post,
        headers,
        body: Some(body),
        no_cache: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn digest_keeps_only_posted_opps_closing_within_window() {
        let today = chrono::Utc::now().date_naive();
        let soon = (today + chrono::Duration::days(3)).format("%m/%d/%Y").to_string();
        let far = (today + chrono::Duration::days(90)).format("%m/%d/%Y").to_string();
        let past = (today - chrono::Duration::days(1)).format("%m/%d/%Y").to_string();
        let hits = vec![
            json!({ "id": "1", "title": "in window", "oppStatus": "posted", "closeDate": soon }),
            json!({ "id": "2", "title": "too far", "oppStatus": "posted", "closeDate": far }),
            json!({ "id": "3", "title": "already closed", "oppStatus": "posted", "closeDate": past }),
            json!({ "id": "4", "title": "forecasted", "oppStatus": "forecasted", "closeDate": soon }),
            json!({ "id": "5", "title": "no close date", "oppStatus": "posted" }),
        ];
        let digest = closing_soon_digest(&hits, 14);
        assert_eq!(digest.len(), 1);
        assert_eq!(digest[0]["id"], "1");
        assert_eq!(digest[0]["daysLeft"], 3);
    }

    #[test]
    fn digest_sorts_soonest_first_and_tolerates_iso_dates() {
        let today = chrono::Utc::now().date_naive();
        let d = |n: i64, iso: bool| {
            let date = today + chrono::Duration::days(n);
            if iso { date.format("%Y-%m-%d").to_string() } else { date.format("%m/%d/%Y").to_string() }
        };
        let hits = vec![
            json!({ "id": "a", "closeDate": d(10, false) }),
            json!({ "id": "b", "closeDate": d(2, true) }),
            json!({ "id": "c", "closeDate": d(5, false) }),
        ];
        let digest = closing_soon_digest(&hits, 14);
        let ids: Vec<&str> = digest.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["b", "c", "a"]);
    }
}
