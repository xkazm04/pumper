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

/// SimHash Hamming distance for cross-source near-duplicate linking. One
/// constant so every source links identically — a per-app literal drifts.
pub const DUP_DISTANCE: u32 = 3;

/// What the shared cross-source finalize produced, for the source's result JSON.
pub struct UnifiedOutcome {
    pub unified: UpsertSummary,
    pub swept: usize,
    pub cross_source_dups: usize,
    pub warnings: Vec<String>,
}

impl UnifiedOutcome {
    /// Merges the cross-source fields into a source app's result object so every
    /// grant source reports the unified layer with one identical shape.
    pub fn merge_into(&self, out: &mut Value) {
        let Value::Object(map) = out else { return };
        map.insert(
            "unified".into(),
            json!({ "new": self.unified.new.len(), "changed": self.unified.changed.len() }),
        );
        map.insert("swept".into(), json!(self.swept));
        map.insert("warnings".into(), json!(self.warnings));
        map.insert("crossSourceDups".into(), json!(self.cross_source_dups));
        // Per-opportunity search docs come from the unified dataset (compact
        // result, one indexed doc per grant) — see worker `dataset_search_docs`.
        map.insert(
            "index_datasets".into(),
            json!([{ "app": UNIFIED_APP, "dataset": UNIFIED_DATASET }]),
        );
    }
}

/// The cross-source tail every grant source runs after storing its raw records:
/// publish the normalized batch into `grants/unified`, sweep past-due rows to
/// closed, link near-duplicates, and collect drift warnings.
///
/// Shared so the sources cannot drift apart — before this, each app hand-rolled
/// the same four calls, and one silently skipping the sweep (or linking at a
/// different distance) would be invisible.
pub async fn finalize_unified(
    ctx: &AppContext,
    unified_items: &[(String, Value)],
) -> Result<UnifiedOutcome> {
    let unified = sync_unified(ctx, unified_items).await?;
    // Lifecycle: flip past-due open/forecasted unified rows to closed — these
    // upsert-only sources never see a delisting otherwise.
    let swept = sweep_closed(ctx).await?;
    let cross_source_dups = link_duplicates(ctx, DUP_DISTANCE).await?;
    let warnings = drift_warnings(unified_items);
    Ok(UnifiedOutcome { unified, swept, cross_source_dups, warnings })
}

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
        // Search2 gives no per-opportunity category/eligibility facets (those are
        // search filters, not hit fields), so these stay empty for this source.
        "categories": Value::Array(vec![]),
        "eligibilities": Value::Array(vec![]),
        // ALN (Assistance Listing Number, formerly CFDA) lives in `cfdaList`.
        "aln": aln_from_array(hit.get("cfdaList")),
        "url": str_of(hit, &["number"])
            .map(|n| format!("https://www.grants.gov/search-results-detail/{id}?opp={n}"))
            .unwrap_or_else(|| format!("https://www.grants.gov/search-results-detail/{id}")),
        "description": Value::Null,
    });
    Some((format!("grants-gov:{id}"), unified))
}

/// Normalizes a California Grants Portal CKAN row. Column names were verified
/// against a live `datastore_search` sample (2026-07-13); a couple of legacy
/// candidates are kept as defensive fallbacks so a minor rename degrades to
/// nulls instead of breaking the run.
///
/// Per-award amount is a single `EstAmounts` **range** column ("Between
/// $100,000 and $10,000,000"), parsed into award_floor/ceiling; the earlier
/// `EstAmountFloor`/`EstAmountCeiling`/`AmountCeiling` candidates do not exist.
/// `EstAvailFunds` is the total-funding scalar ("$370,000,000").
pub fn normalize_ca_grants(rec: &Value) -> Option<(String, Value)> {
    let id = str_of(rec, &["PortalID", "GrantID"])?;
    let (award_floor, award_ceiling) = money_range(rec, &["EstAmounts"]);
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
        "award_floor": award_floor,
        "award_ceiling": award_ceiling,
        "total_funding": money_scalar(rec, &["EstAvailFunds"]),
        // Portal taxonomies are single "; "-separated string columns. Category
        // names themselves contain commas ("Housing, Community and Economic
        // Development"), so only ';' is a separator.
        "categories": str_list(rec, &["Categories"]),
        "eligibilities": str_list(rec, &["ApplicantType"]),
        // The CA portal publishes no ALN/CFDA number.
        "aln": Value::Null,
        "url": str_of(rec, &["GrantURL", "URL", "Link"]),
        "description": str_of(rec, &["Description", "Purpose"])
            .map(|d| d.chars().take(500).collect::<String>()),
    });
    Some((format!("ca-grants:{id}"), unified))
}

/// Normalizes an eu-sedia **already-normalized** `opportunities` record (the
/// output of the eu-sedia app's own `normalize`, so titles/descriptions are
/// already entity-decoded) into the unified schema.
///
/// Two SEDIA-specific traps are handled here so the pan-EU corpus doesn't
/// silently corrupt the shared query surface:
/// - **status** is a numeric code (`31094502`=open, `31094501`=forthcoming), not
///   a word. Passing it through `norm_status` would write the literal digits into
///   `status` and break every `?status=open` filter and the sweep predicate, so
///   the two real codes are mapped explicitly and anything else is left `Null`.
/// - **money** (`budgetOverview`) is EUR and the unified schema has no currency
///   dimension, so award/funding stay `Null` rather than filing euros as if they
///   were ca-grants dollars. (Revisit once unified gains a `currency` field.)
pub fn normalize_eu_sedia(rec: &Value) -> Option<(String, Value)> {
    let id = str_of(rec, &["identifier"])?;
    let today = chrono::Utc::now().date_naive();
    let unified = json!({
        "source": "eu-sedia",
        "source_id": id,
        "title": str_of(rec, &["title"]),
        "agency": sedia_agency(rec),
        "status": sedia_status(str_of(rec, &["status"]).as_deref()),
        "open_date": str_of(rec, &["startDate"]).as_deref().and_then(norm_date),
        "close_date": sedia_close_date(rec.get("deadlineDate").unwrap_or(&Value::Null), today),
        // EUR, and unified has no currency dimension — Null, never fabricated USD.
        "award_floor": Value::Null,
        "award_ceiling": Value::Null,
        "total_funding": Value::Null,
        "categories": sedia_categories(rec),
        // Not present in the SEDIA search hit.
        "eligibilities": Value::Array(vec![]),
        // ALN/CFDA is a US-only concept.
        "aln": Value::Null,
        "url": str_of(rec, &["url"]),
        // Already-cleaned plain text; match the 500-char cap the other sources use.
        "description": str_of(rec, &["description_text"])
            .map(|d| d.chars().take(500).collect::<String>()),
    });
    Some((format!("eu-sedia:{id}"), unified))
}

/// SEDIA has no agency column; the framework programme (e.g. "Horizon Europe")
/// is the honest analogue, qualified by the call identifier when present.
fn sedia_agency(rec: &Value) -> Value {
    match (str_of(rec, &["frameworkProgramme"]), str_of(rec, &["callIdentifier"])) {
        (Some(fp), Some(call)) => Value::String(format!("{fp} — {call}")),
        (Some(fp), None) => Value::String(fp),
        (None, Some(call)) => Value::String(call),
        (None, None) => Value::Null,
    }
}

/// typesOfAction + programmePeriod as the category axis — the SEDIA search hit
/// carries no topic taxonomy.
fn sedia_categories(rec: &Value) -> Value {
    let mut cats = Vec::new();
    if let Some(t) = str_of(rec, &["typesOfAction"]) {
        cats.push(Value::String(t));
    }
    if let Some(p) = str_of(rec, &["programmePeriod"]) {
        cats.push(Value::String(p));
    }
    Value::Array(cats)
}

/// Maps SEDIA numeric status codes to the canonical vocabulary. Only the two
/// codes the app queries are known; anything else is `Null` rather than passed
/// through `norm_status` (which would emit the literal code).
fn sedia_status(code: Option<&str>) -> Value {
    match code {
        Some("31094502") => Value::String("open".into()),
        Some("31094501") => Value::String("forecasted".into()),
        _ => Value::Null,
    }
}

/// The unified `close_date` for a SEDIA topic. `deadlineDate` is kept whole
/// because multi-stage/multi-cutoff calls carry several dates: the effective
/// deadline is the earliest cutoff still upcoming, and once every cutoff is past
/// the latest one (so `sweep_closed` can retire the topic). Taking `[0]` blindly
/// would flip a still-open two-stage call to `closed` the moment its first cutoff
/// passes. Accepts an array or a lone value; unparseable/absent → `None`.
fn sedia_close_date(deadline: &Value, today: chrono::NaiveDate) -> Option<String> {
    let mut dates: Vec<chrono::NaiveDate> = match deadline {
        Value::Array(a) => a.iter().filter_map(Value::as_str).filter_map(parse_date).collect(),
        Value::String(s) => parse_date(s).into_iter().collect(),
        _ => Vec::new(),
    };
    if dates.is_empty() {
        return None;
    }
    dates.sort_unstable();
    let chosen = dates
        .iter()
        .find(|d| **d >= today)
        .copied()
        .unwrap_or_else(|| *dates.last().unwrap());
    Some(chosen.to_string())
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
    use pumper_core::datasets::JsonFilter;
    let today = chrono::Utc::now().date_naive();
    // Load only the sweep candidates (status open/forecasted), not the whole
    // corpus. Over time most unified rows are already `closed` and can never flip
    // again, yet the old full read deserialized every one of them on every sync —
    // and `finalize_unified` runs this once per source, now three (grants-gov,
    // ca-grants, eu-sedia), so the wasted scan was paid 3×/day. `list_filtered`
    // also already excludes tombstoned rows. (Deduplicating the 3 invocations into
    // one per sync cycle is a separate, larger change; making each cheap is the
    // pragmatic win.)
    let mut rows = Vec::new();
    for status in ["open", "forecasted"] {
        let filter = [JsonFilter::Eq { path: "$.status".into(), value: status.into() }];
        rows.extend(
            ctx.datasets
                .list_filtered(UNIFIED_APP, UNIFIED_DATASET, &filter, None, 1_000_000)
                .await?,
        );
    }
    let mut updates: Vec<(String, Value)> = Vec::new();
    for rec in rows {
        let status = rec.data.get("status").and_then(Value::as_str);
        let close_date = rec.data.get("close_date").and_then(Value::as_str);
        if !is_past_due_open(status, close_date, today) {
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

/// The sweep decision for one row: an `open`/`forecasted` opportunity whose
/// `close_date` parses and is strictly before `today` should flip to `closed`.
/// A missing/unparseable close date, a future/today date, or any other status
/// is left untouched (a deadline that is exactly today has not passed).
fn is_past_due_open(status: Option<&str>, close_date: Option<&str>, today: chrono::NaiveDate) -> bool {
    matches!(status, Some("open") | Some("forecasted"))
        && close_date.and_then(parse_date).is_some_and(|d| d < today)
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

/// A "; "-separated taxonomy string column → a JSON array of trimmed,
/// non-empty values (empty array when absent/blank). Only ';' splits, because
/// the portal's category names contain commas.
fn str_list(rec: &Value, fields: &[&str]) -> Value {
    let items: Vec<Value> = str_of(rec, fields)
        .map(|s| {
            s.split(';')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(|p| Value::String(p.to_string()))
                .collect()
        })
        .unwrap_or_default();
    Value::Array(items)
}

/// Joins an ALN/CFDA list value (`["15.931", ...]`) into a single `", "`-joined
/// string, or Null when absent/empty. Tolerates a bare string too.
fn aln_from_array(v: Option<&Value>) -> Value {
    let parts: Vec<String> = match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        Some(Value::String(s)) if !s.trim().is_empty() => vec![s.trim().to_string()],
        _ => vec![],
    };
    if parts.is_empty() {
        Value::Null
    } else {
        Value::String(parts.join(", "))
    }
}

/// All money amounts found in a string, left-to-right. Handles currency symbols,
/// thousands separators, decimals, and K/M/B magnitude suffixes
/// ("$1.5M" → 1_500_000, "$100k" → 100_000). Zero and unparseable tokens are
/// dropped, so "$0" and prose ("Dependant on submissions") yield an empty vec.
fn scan_amounts(s: &str) -> Vec<f64> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b',' || bytes[i] == b'.') {
            i += 1;
        }
        let digits: String = s[start..i].chars().filter(|c| *c != ',').collect();
        // Optional single magnitude suffix immediately after the number.
        let mult = match bytes.get(i).map(|b| *b as char) {
            Some('k') | Some('K') => {
                i += 1;
                1_000.0
            }
            Some('m') | Some('M') => {
                i += 1;
                1_000_000.0
            }
            Some('b') | Some('B') => {
                i += 1;
                1_000_000_000.0
            }
            _ => 1.0,
        };
        if let Ok(v) = digits.trim_matches('.').parse::<f64>() {
            let v = v * mult;
            if v > 0.0 {
                out.push(v);
            }
        }
    }
    out
}

/// Single money value for a scalar field: the first parseable amount among the
/// candidate columns (JSON numbers pass through). Null when none is found.
fn money_scalar(rec: &Value, fields: &[&str]) -> Value {
    for f in fields {
        match rec.get(*f) {
            Some(Value::Number(n)) => {
                let v = n.as_f64().unwrap_or(0.0);
                if v > 0.0 {
                    return Value::from(v);
                }
            }
            Some(Value::String(s)) => {
                if let Some(v) = scan_amounts(s).into_iter().next() {
                    return Value::from(v);
                }
            }
            _ => {}
        }
    }
    Value::Null
}

/// (floor, ceiling) for a field that may express a range ("Between $100,000 and
/// $10,000,000", "$100k-$500k"): min and max of the amounts found. A lone value
/// collapses to (v, v); no amounts → (Null, Null).
fn money_range(rec: &Value, fields: &[&str]) -> (Value, Value) {
    for f in fields {
        let amounts = match rec.get(*f) {
            Some(Value::Number(n)) => {
                let v = n.as_f64().unwrap_or(0.0);
                if v > 0.0 { vec![v] } else { vec![] }
            }
            Some(Value::String(s)) => scan_amounts(s),
            _ => vec![],
        };
        if amounts.is_empty() {
            continue;
        }
        let min = amounts.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = amounts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        return (Value::from(min), Value::from(max));
    }
    (Value::Null, Value::Null)
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
        .or_else(|_| {
            // Datetime forms (`2026-11-02 23:59:00`, `2026-11-02T23:59:00Z`): take
            // the date part before the first space or `T`. Split on chars (not a
            // byte slice) so a non-ASCII value — e.g. an em-dash in "Deadline—see
            // website" — yields None instead of panicking on a non-char boundary.
            let date_part = s.split(['T', ' ']).next().unwrap_or(s);
            chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        })
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
            "openDate": "07/01/2026", "closeDate": "08/15/2026",
            "cfdaList": ["93.912", "93.913"]
        });
        let (key, v) = normalize_grants_gov(&hit).unwrap();
        assert_eq!(key, "grants-gov:356037");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-08-15");
        assert_eq!(v["award_ceiling"], Value::Null);
        // ALN joined from cfdaList; categories/eligibilities empty for this source.
        assert_eq!(v["aln"], "93.912, 93.913");
        assert_eq!(v["categories"], json!([]));
        assert_eq!(v["eligibilities"], json!([]));
    }

    #[test]
    fn ca_grants_parses_money_dates_and_status() {
        // Field values mirror the live portal sample (2026-07-13).
        let rec = json!({
            "PortalID": "CA-99", "Title": "Wildfire Prevention",
            "AgencyDept": "CAL FIRE", "Status": "active",
            "ApplicationDeadline": "2026-11-02 23:59:00",
            "EstAvailFunds": "$5,000,000",
            "EstAmounts": "Between $100,000 and $10,000,000",
            "Categories": "Environment & Water; Disadvantaged Communities",
            "ApplicantType": "Public Agency; Tribal Government",
            "GrantURL": "https://ca.gov/g/99"
        });
        let (key, v) = normalize_ca_grants(&rec).unwrap();
        assert_eq!(key, "ca-grants:CA-99");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-11-02");
        assert_eq!(v["total_funding"], json!(5_000_000.0));
        // EstAmounts range → floor/ceiling.
        assert_eq!(v["award_floor"], json!(100_000.0));
        assert_eq!(v["award_ceiling"], json!(10_000_000.0));
        // "; "-split taxonomies; CA has no ALN.
        assert_eq!(v["categories"], json!(["Environment & Water", "Disadvantaged Communities"]));
        assert_eq!(v["eligibilities"], json!(["Public Agency", "Tribal Government"]));
        assert_eq!(v["aln"], Value::Null);
    }

    #[test]
    fn money_parsing_handles_suffixes_ranges_and_prose() {
        let m = |rec: &Value| money_scalar(rec, &["v"]);
        // K/M/B suffixes.
        assert_eq!(m(&json!({ "v": "$1.5M" })), json!(1_500_000.0));
        assert_eq!(m(&json!({ "v": "$100k" })), json!(100_000.0));
        assert_eq!(m(&json!({ "v": "$2B" })), json!(2_000_000_000.0));
        // Thousands separators + currency symbol.
        assert_eq!(m(&json!({ "v": "$370,000,000" })), json!(370_000_000.0));
        // JSON number passes through.
        assert_eq!(m(&json!({ "v": 250000 })), json!(250_000.0));
        // Prose / zero → null.
        assert_eq!(m(&json!({ "v": "Dependant on submissions" })), Value::Null);
        assert_eq!(m(&json!({ "v": "$0" })), Value::Null);

        // Ranges (real EstAmounts strings).
        let r = |s: &str| money_range(&json!({ "v": s }), &["v"]);
        assert_eq!(r("Between $100,000 and $10,000,000"), (json!(100_000.0), json!(10_000_000.0)));
        assert_eq!(r("$100k-$500k"), (json!(100_000.0), json!(500_000.0)));
        // Lone value collapses to (v, v).
        assert_eq!(r("$250,000"), (json!(250_000.0), json!(250_000.0)));
        // No amount → (Null, Null).
        assert_eq!(r("Dependant on submissions"), (Value::Null, Value::Null));
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
        // Regression: a non-ASCII char straddling byte 10 must not panic on a
        // non-char-boundary slice — an em-dash close-date cell yields None.
        assert!(parse_date("Deadline—see website").is_none());
        assert!(parse_date("—").is_none());
    }

    #[test]
    fn sweep_predicate_flips_only_past_due_open_or_forecasted() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 7, 13).unwrap();
        // Past-due open / forecasted → flip.
        assert!(is_past_due_open(Some("open"), Some("2026-07-12"), today));
        assert!(is_past_due_open(Some("forecasted"), Some("07/12/2026"), today));
        // Deadline exactly today has not passed → leave.
        assert!(!is_past_due_open(Some("open"), Some("2026-07-13"), today));
        // Future, already-closed, missing/unparseable date → leave.
        assert!(!is_past_due_open(Some("open"), Some("2026-08-01"), today));
        assert!(!is_past_due_open(Some("closed"), Some("2026-01-01"), today));
        assert!(!is_past_due_open(Some("open"), None, today));
        assert!(!is_past_due_open(Some("open"), Some("n/a"), today));
    }

    #[test]
    fn eu_sedia_normalizes_with_status_codes_null_money_and_stage_deadline() {
        // A normalized eu-sedia record (the eu-sedia app's `normalize` output):
        // status is a numeric code, deadlineDate is a multi-stage array, budget
        // is EUR.
        let today = chrono::Utc::now().date_naive();
        let stage1 = (today - chrono::Duration::days(30))
            .format("%Y-%m-%dT17:00:00Z")
            .to_string();
        let stage2 = (today + chrono::Duration::days(60))
            .format("%Y-%m-%dT17:00:00Z")
            .to_string();
        let rec = json!({
            "identifier": "HORIZON-CL4-2026-DATA-01",
            "title": "AI & Robotics – Phase II",
            "status": "31094502",                               // as stored: a string code
            "frameworkProgramme": "Horizon Europe",
            "callIdentifier": "HORIZON-CL4-2026-01",
            "typesOfAction": "HORIZON-RIA",
            "programmePeriod": "2021-2027",
            "startDate": "2026-01-15",
            "deadlineDate": [stage1, stage2.clone()],
            "budgetOverview": "EUR 10 000 000",
            "url": "https://ec.europa.eu/x",
            "description_text": "Expected Outcome: projects contribute to a trustworthy AI single market.",
        });
        let (key, v) = normalize_eu_sedia(&rec).unwrap();
        assert_eq!(key, "eu-sedia:HORIZON-CL4-2026-DATA-01");
        assert_eq!(v["source"], "eu-sedia");
        // Numeric code mapped to the canonical word, NOT passed through literally.
        assert_eq!(v["status"], "open");
        // Money stays Null (EUR, no currency dimension) — never fabricated USD.
        assert_eq!(v["award_floor"], Value::Null);
        assert_eq!(v["award_ceiling"], Value::Null);
        assert_eq!(v["total_funding"], Value::Null);
        // Multi-stage deadline: the earliest cutoff still upcoming wins, not [0].
        assert_eq!(v["close_date"], stage2.split('T').next().unwrap());
        assert_eq!(v["agency"], "Horizon Europe — HORIZON-CL4-2026-01");
        assert_eq!(v["categories"], json!(["HORIZON-RIA", "2021-2027"]));
        assert_eq!(v["aln"], Value::Null);
        assert_eq!(v["eligibilities"], json!([]));
    }

    #[test]
    fn eu_sedia_unknown_status_code_is_null_not_passed_through() {
        // An unrecognized code must not leak into `status` (it would break
        // ?status=open and the sweep predicate).
        let rec = json!({ "identifier": "X", "status": "99999999" });
        let (_, v) = normalize_eu_sedia(&rec).unwrap();
        assert_eq!(v["status"], Value::Null);
    }

    #[test]
    fn eu_sedia_close_date_prefers_earliest_upcoming_else_latest_past() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();
        let d = |s: &str| Value::String(s.to_string());
        // All future → earliest.
        let all_future = json!([d("2026-09-01"), d("2026-08-01")]);
        assert_eq!(sedia_close_date(&all_future, today).as_deref(), Some("2026-08-01"));
        // Mixed → earliest that is >= today (a passed first cutoff is skipped).
        let mixed = json!([d("2026-03-01"), d("2026-09-01")]);
        assert_eq!(sedia_close_date(&mixed, today).as_deref(), Some("2026-09-01"));
        // All past → latest (so the sweep can retire it).
        let all_past = json!([d("2026-01-01"), d("2026-05-01")]);
        assert_eq!(sedia_close_date(&all_past, today).as_deref(), Some("2026-05-01"));
        // Absent / unparseable → None (forecasted topics legitimately lack one).
        assert_eq!(sedia_close_date(&Value::Null, today), None);
        assert_eq!(sedia_close_date(&json!([d("n/a")]), today), None);
    }

    #[test]
    fn eu_sedia_row_without_identifier_is_skipped() {
        assert!(normalize_eu_sedia(&json!({ "title": "no id" })).is_none());
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
