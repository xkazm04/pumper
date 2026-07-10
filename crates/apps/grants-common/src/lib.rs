//! Shared grant-intelligence layer for the grant-source apps.
//!
//! Each source app keeps its raw records in its own `opportunities` dataset;
//! this crate additionally normalizes every opportunity into ONE canonical
//! schema and upserts it into the cross-source `grants/unified` dataset
//! (keyed `<source>:<source_id>`), so downstream consumers — search, exports,
//! deadline digests, dedup — see one shape regardless of origin. Cross-source
//! near-duplicates (the same grant syndicated on two portals) are linked via
//! SimHash into `grants/duplicate_links`.

use pumper_core::{AppContext, Result, UpsertSummary};
use serde_json::{json, Value};

/// Virtual app namespace holding the cross-source datasets.
pub const UNIFIED_APP: &str = "grants";
pub const UNIFIED_DATASET: &str = "unified";
pub const DUP_DATASET: &str = "duplicate_links";

/// Normalizes a grants.gov Search2 `oppHits[]` entry. Award amounts are not
/// present in Search2 results, so the money fields stay null for this source.
pub fn normalize_grants_gov(hit: &Value) -> Option<(String, Value)> {
    let id = str_of(hit, &["id", "number"])?;
    let unified = json!({
        "source": "grants-gov",
        "source_id": id,
        "title": str_of(hit, &["title"]),
        "agency": str_of(hit, &["agency", "agencyCode"]),
        "status": norm_status(str_of(hit, &["oppStatus"]).as_deref()),
        "open_date": str_of(hit, &["openDate"]).as_deref().and_then(norm_date),
        "close_date": str_of(hit, &["closeDate"]).as_deref().and_then(norm_date),
        "award_floor": Value::Null,
        "award_ceiling": Value::Null,
        "total_funding": Value::Null,
        "url": str_of(hit, &["number"])
            .map(|n| format!("https://www.grants.gov/search-results-detail/{id}?opp={n}"))
            .unwrap_or_else(|| format!("https://www.grants.gov/search-results-detail/{id}")),
        "description": Value::Null,
    });
    Some((format!("grants-gov:{id}"), unified))
}

/// Normalizes a California Grants Portal CKAN row. Column names are looked up
/// defensively (several candidates per field) so portal schema drift degrades
/// to nulls instead of breaking the run.
pub fn normalize_ca_grants(rec: &Value) -> Option<(String, Value)> {
    let id = str_of(rec, &["PortalID", "GrantID"])?;
    let unified = json!({
        "source": "ca-grants",
        "source_id": id,
        "title": str_of(rec, &["Title", "GrantTitle"]),
        "agency": str_of(rec, &["AgencyDept", "Agency", "Department"]),
        "status": norm_status(str_of(rec, &["Status"]).as_deref()),
        "open_date": str_of(rec, &["OpenDate", "ApplicationOpenDate"]).as_deref().and_then(norm_date),
        "close_date": str_of(rec, &["ApplicationDeadline", "CloseDate", "Deadline"])
            .as_deref()
            .and_then(norm_date),
        "award_floor": money_of(rec, &["EstAmountFloor", "AmountFloor"]),
        "award_ceiling": money_of(rec, &["EstAmounts", "EstAmountCeiling", "AmountCeiling"]),
        "total_funding": money_of(rec, &["EstAvailFunds", "TotalEstAvailFunds"]),
        "url": str_of(rec, &["GrantURL", "URL", "Link"]),
        "description": str_of(rec, &["Description", "Purpose"])
            .map(|d| d.chars().take(500).collect::<String>()),
    });
    Some((format!("ca-grants:{id}"), unified))
}

/// Upserts normalized grants into the cross-source unified dataset.
pub async fn sync_unified(
    ctx: &AppContext,
    items: &[(String, Value)],
) -> Result<UpsertSummary> {
    ctx.datasets.upsert_many(UNIFIED_APP, UNIFIED_DATASET, items).await
}

/// Links cross-source near-duplicates (SimHash Hamming ≤ `max_distance`) into
/// `grants/duplicate_links`, keyed `a|b`. Same-source pairs are skipped — the
/// interesting signal is one grant syndicated on two portals.
pub async fn link_duplicates(ctx: &AppContext, max_distance: u32) -> Result<usize> {
    let pairs = ctx
        .datasets
        .duplicate_pairs(UNIFIED_APP, UNIFIED_DATASET, max_distance)
        .await?;
    let items: Vec<(String, Value)> = pairs
        .into_iter()
        .filter(|p| source_of(&p.a) != source_of(&p.b))
        .map(|p| {
            (
                format!("{}|{}", p.a, p.b),
                json!({ "a": p.a, "b": p.b, "distance": p.distance }),
            )
        })
        .collect();
    if !items.is_empty() {
        ctx.datasets.upsert_many(UNIFIED_APP, DUP_DATASET, &items).await?;
    }
    Ok(items.len())
}

fn source_of(key: &str) -> &str {
    key.split(':').next().unwrap_or("")
}

/// First non-empty string among candidate field names.
fn str_of(rec: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .filter_map(|f| rec.get(*f).and_then(Value::as_str))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(String::from)
}

/// First parseable money value among candidates: numbers pass through,
/// strings tolerate `$`, thousands separators, and surrounding text.
fn money_of(rec: &Value, fields: &[&str]) -> Value {
    for f in fields {
        match rec.get(*f) {
            Some(Value::Number(n)) => return Value::from(n.as_f64().unwrap_or(0.0)),
            Some(Value::String(s)) => {
                let cleaned: String = s
                    .chars()
                    .filter(|c| c.is_ascii_digit() || *c == '.')
                    .collect();
                if let Ok(n) = cleaned.parse::<f64>() {
                    if n > 0.0 {
                        return Value::from(n);
                    }
                }
            }
            _ => {}
        }
    }
    Value::Null
}

/// Normalizes dates to `YYYY-MM-DD`; tolerates US `MM/DD/YYYY`, ISO, and ISO
/// datetime prefixes.
fn norm_date(s: &str) -> Option<String> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
        .or_else(|_| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .or_else(|_| chrono::NaiveDate::parse_from_str(&s[..s.len().min(10)], "%Y-%m-%d"))
        .ok()
        .map(|d| d.to_string())
}

/// Canonical status vocabulary: open | forecasted | closed (unknowns lowercase
/// through so nothing is silently lost).
fn norm_status(s: Option<&str>) -> Value {
    let Some(s) = s else { return Value::Null };
    let lower = s.trim().to_lowercase();
    let norm = match lower.as_str() {
        "posted" | "active" | "open" => "open",
        "forecasted" | "forecast" => "forecasted",
        "closed" | "archived" | "inactive" | "expired" => "closed",
        other => other,
    };
    Value::String(norm.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn grants_gov_normalizes_to_unified_schema() {
        let hit = json!({
            "id": "356037", "number": "TEST-24-001", "title": "Rural Health",
            "agency": "HHS", "oppStatus": "posted",
            "openDate": "07/01/2026", "closeDate": "08/15/2026"
        });
        let (key, v) = normalize_grants_gov(&hit).unwrap();
        assert_eq!(key, "grants-gov:356037");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-08-15");
        assert_eq!(v["award_ceiling"], Value::Null);
    }

    #[test]
    fn ca_grants_parses_money_dates_and_status() {
        let rec = json!({
            "PortalID": "CA-99", "Title": "Wildfire Prevention",
            "AgencyDept": "CAL FIRE", "Status": "active",
            "ApplicationDeadline": "2026-09-30",
            "EstAvailFunds": "$5,000,000", "GrantURL": "https://ca.gov/g/99"
        });
        let (key, v) = normalize_ca_grants(&rec).unwrap();
        assert_eq!(key, "ca-grants:CA-99");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-09-30");
        assert_eq!(v["total_funding"], json!(5_000_000.0));
    }

    #[test]
    fn unmappable_rows_are_skipped_not_fabricated() {
        assert!(normalize_ca_grants(&json!({ "Title": "no id" })).is_none());
        assert!(normalize_grants_gov(&json!({})).is_none());
    }
}
