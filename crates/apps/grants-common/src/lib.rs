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

/// Lifecycle sweep for the upsert-only unified dataset: these sources only
/// report currently-listed opportunities, so a grant that closes or is delisted
/// is simply absent from the next fetch — its `open`/`forecasted` row would
/// otherwise persist forever. After sync, mark every live unified row whose
/// status is `open`/`forecasted` and whose `close_date` is strictly before
/// today as `closed`. Written through the normal upsert path, so each transition
/// records a `changed` revision (the delisting signal `removed_at` can't give on
/// a partial-view source). Returns the number of rows swept to `closed`.
pub async fn sweep_closed(ctx: &AppContext) -> Result<usize> {
    let today = chrono::Utc::now().date_naive();
    // Local datasets are small (both sources cap well under this); one read.
    let rows = ctx.datasets.list(UNIFIED_APP, UNIFIED_DATASET, 1_000_000).await?;
    let mut updates: Vec<(String, Value)> = Vec::new();
    for rec in rows {
        if rec.removed_at.is_some() {
            continue;
        }
        let status = rec.data.get("status").and_then(Value::as_str);
        if !matches!(status, Some("open") | Some("forecasted")) {
            continue;
        }
        let past_due = rec
            .data
            .get("close_date")
            .and_then(Value::as_str)
            .and_then(parse_date)
            .is_some_and(|d| d < today);
        if !past_due {
            continue;
        }
        let mut updated = rec.data.clone();
        updated["status"] = Value::String("closed".to_string());
        updates.push((rec.key, updated));
    }
    if !updates.is_empty() {
        ctx.datasets.upsert_many(UNIFIED_APP, UNIFIED_DATASET, &updates).await?;
    }
    Ok(updates.len())
}

/// Fraction of a run's normalized opportunities missing their `title` above
/// which schema drift is likely (a renamed/dropped title column). Titles are
/// essentially always present, so a majority-null run is the signal; picked at
/// 0.5 to stay quiet on the odd genuinely-untitled record while catching a
/// wholesale column rename. `close_date`-null is intentionally NOT a drift
/// signal — forecasted grants legitimately have no close date.
pub const TITLE_NULL_DRIFT_THRESHOLD: f64 = 0.5;

/// Non-fatal schema-drift warnings over a run's normalized unified items. Empty
/// when nothing looks wrong; otherwise human-readable strings for the result's
/// `warnings` array. (The hard drift case — a positive server hitCount with zero
/// fetched rows — is a job failure, handled in each app.)
pub fn drift_warnings(items: &[(String, Value)]) -> Vec<String> {
    let mut warnings = Vec::new();
    let total = items.len();
    if total == 0 {
        return warnings;
    }
    let null_titles = items
        .iter()
        .filter(|(_, v)| v.get("title").and_then(Value::as_str).is_none())
        .count();
    let rate = null_titles as f64 / total as f64;
    if rate > TITLE_NULL_DRIFT_THRESHOLD {
        warnings.push(format!(
            "possible schema drift: {null_titles}/{total} ({:.0}%) normalized opportunities \
             have a null title — check the source's title field",
            rate * 100.0
        ));
    }
    warnings
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

/// The one date parser for the grant sources — used by normalization, the
/// close-date sweep, and the closing-soon digest so they can never diverge.
/// Tolerates the formats observed across grants.gov and the CA portal:
/// US `MM/DD/YYYY` (non-zero-padded ok, e.g. `7/1/2027`), ISO `YYYY-MM-DD`, and
/// ISO/space datetimes (`2026-11-02 23:59:00`, `2026-11-02T23:59:00Z`) whose
/// date prefix is taken. Empty/whitespace and unrecognized text yield `None`.
pub fn parse_date(s: &str) -> Option<chrono::NaiveDate> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
        .or_else(|_| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .or_else(|_| chrono::NaiveDate::parse_from_str(&s[..s.len().min(10)], "%Y-%m-%d"))
        .ok()
}

/// Normalizes a date string to canonical `YYYY-MM-DD`, or `None` if unparseable.
fn norm_date(s: &str) -> Option<String> {
    parse_date(s).map(|d| d.to_string())
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

    #[test]
    fn parse_date_handles_all_observed_formats() {
        // US MM/DD/YYYY (grants.gov), zero-padded and not.
        assert_eq!(parse_date("08/15/2026").unwrap().to_string(), "2026-08-15");
        assert_eq!(parse_date("7/1/2027").unwrap().to_string(), "2027-07-01");
        // ISO date.
        assert_eq!(parse_date("2026-09-30").unwrap().to_string(), "2026-09-30");
        // CA portal space-separated datetime + ISO 'T' datetime → date prefix.
        assert_eq!(parse_date("2026-11-02 23:59:00").unwrap().to_string(), "2026-11-02");
        assert_eq!(parse_date("2026-11-02T23:59:00Z").unwrap().to_string(), "2026-11-02");
        // Empty / unparseable → None.
        assert!(parse_date("").is_none());
        assert!(parse_date("   ").is_none());
        assert!(parse_date("not a date").is_none());
    }

    #[test]
    fn drift_warnings_fire_only_on_majority_null_titles() {
        let with_title = |t: Option<&str>| {
            ("k".to_string(), json!({ "title": t }))
        };
        // Mostly-present titles: no warning.
        let ok = vec![with_title(Some("A")), with_title(Some("B")), with_title(None)];
        assert!(drift_warnings(&ok).is_empty());
        // Majority null: warning.
        let bad = vec![with_title(None), with_title(None), with_title(Some("C"))];
        assert_eq!(drift_warnings(&bad).len(), 1);
        // Empty input: no warning (no data is not drift).
        assert!(drift_warnings(&[]).is_empty());
    }
}
