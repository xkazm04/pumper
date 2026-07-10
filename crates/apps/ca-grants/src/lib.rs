//! California grant opportunities via the California Grants Portal on data.ca.gov
//! (CKAN `datastore_search`). The only US state that publishes a true open-call
//! API, so this is a `http` fast-path like grants-gov — no browser needed.
//!
//! Data type: OPEN CALLS. Access: key-free CKAN. Keyed by the portal's stable
//! `PortalID` into the `opportunities` dataset. See `catalog/data-sources.toml`
//! (id `ca-grants`).
//!
//! Uses CKAN's POST+JSON form of `datastore_search` (avoids URL-encoding the
//! `filters` object) and paginates with `limit` + `offset`; `result.total` is the
//! full count. `Status` filters to currently-open grants (`active` by default).

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpMethod, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct CaGrants;

const CKAN_URL: &str = "https://data.ca.gov/api/3/action/datastore_search";
// California Grants Portal dataset on data.ca.gov (verified 2026-07-03).
const RESOURCE_ID: &str = "111c8c88-21f6-453c-ae2c-b4785a0624f5";

#[async_trait]
impl ScrapeApp for CaGrants {
    fn name(&self) -> &'static str {
        "ca-grants"
    }

    fn description(&self) -> &'static str {
        "California grant opportunities (California Grants Portal via data.ca.gov \
         CKAN, key-free). Open calls, keyed by PortalID into the `opportunities` \
         dataset. Params: {\"status\": \"active\" (\"\" = all statuses), \
         \"limit\": 1-1000, \"maxPages\": 1-100}"
    }

    /// Daily at 09:30 UTC — offset from grants-gov (09:00) to spread the load.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 30 9 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "status": "active", "limit": 100, "maxPages": 25 })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let status = ctx
            .params
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("active")
            .to_string();
        let limit = ctx
            .params
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000);
        let max_pages = ctx
            .params
            .get("maxPages")
            .and_then(Value::as_u64)
            .unwrap_or(25)
            .clamp(1, 100);

        let mut records: Vec<Value> = Vec::new();
        let mut total: u64 = 0;
        let mut offset: u64 = 0;
        let mut pages: u64 = 0;

        loop {
            let mut body = json!({
                "resource_id": RESOURCE_ID,
                "limit": limit,
                "offset": offset,
            });
            // Empty status = no filter (all statuses); otherwise filter server-side.
            if !status.is_empty() {
                body["filters"] = json!({ "Status": status });
            }

            let resp = ctx
                .engines
                .http
                .fetch(post_json(CKAN_URL, body.to_string()))
                .await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "data.ca.gov returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }

            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("ca-grants: response was not JSON: {e}")))?;
            if !parsed.get("success").and_then(Value::as_bool).unwrap_or(false) {
                return Err(Error::App(format!(
                    "ca-grants: CKAN reported failure: {}",
                    parsed.get("error").map(|e| e.to_string()).unwrap_or_default()
                )));
            }

            let result = parsed.get("result").cloned().unwrap_or(Value::Null);
            if pages == 0 {
                total = result.get("total").and_then(Value::as_u64).unwrap_or(0);
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            let page = result
                .get("records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = page.len() as u64;
            records.extend(page);
            pages += 1;
            offset += limit;

            if got < limit || offset >= total || pages >= max_pages {
                break;
            }
        }

        // Key by the portal's stable grant id (PortalID); the CKAN `_id` is a row
        // number that renumbers on dataset reload, so it must NOT be the key.
        let items: Vec<(String, Value)> = records
            .iter()
            .enumerate()
            .map(|(i, r)| (record_key(r, i), r.clone()))
            .collect();

        let summary = ctx.upsert_many("opportunities", &items).await?;

        // Cross-source layer: normalize into grants/unified and link SimHash
        // near-duplicates syndicated across portals.
        let unified_items: Vec<(String, Value)> = records
            .iter()
            .filter_map(grants_common::normalize_ca_grants)
            .collect();
        let unified = grants_common::sync_unified(&ctx, &unified_items).await?;
        let cross_source_dups = grants_common::link_duplicates(&ctx, 3).await?;

        Ok(json!({
            "source": "data.ca.gov/california-grants-portal",
            "status": status,
            "total": total,
            "fetched": records.len(),
            "pages": pages,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "unified": { "new": unified.new.len(), "changed": unified.changed.len() },
            "crossSourceDups": cross_source_dups,
        }))
    }
}

/// Stable key for a portal record: PortalID, then GrantID, then a fallback that
/// is never the raw `_id` (which renumbers on reload).
fn record_key(rec: &Value, i: usize) -> String {
    for field in ["PortalID", "GrantID"] {
        if let Some(s) = rec.get(field).and_then(Value::as_str) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    match rec.get("_id") {
        Some(Value::Number(n)) => format!("_id-{n}"),
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => format!("row-{i}"),
    }
}

fn post_json(url: &str, body: String) -> HttpRequest {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("Accept".to_string(), "application/json".to_string());
    HttpRequest {
        url: url.to_string(),
        method: HttpMethod::Post,
        headers,
        body: Some(body),
        no_cache: false,
    }
}
