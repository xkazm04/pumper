//! Shared helpers for the Census API apps (`census-density`, `census-nonemp`).
//!
//! These were duplicated verbatim in both apps, which is precisely how they
//! drifted: the disclosure/jam-sentinel guard was applied in one parser and
//! forgotten in another, silently summing `-666666666` into national totals.
//! One definition each, used by both, so a fix can't land in only half the fleet.

use pumper_core::{AppContext, Error, Provenance, Result, UpsertSummary};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// The `census` product namespace — a VIRTUAL app (the `grants/unified` pattern).
//
// All four census apps re-derive these two datasets after their own upserts, so
// they belong to no single app's namespace. The names live here rather than in
// one of the apps because every app needs them to declare `index_datasets`.
// ---------------------------------------------------------------------------

/// Virtual app namespace holding the cross-app census products.
pub const MARKET_APP: &str = "census";
/// Blended employer + solo market view, keyed `{naics4}:{state_fips}`.
pub const MARKET_BLEND_DATASET: &str = "market_blend";
/// Per-place saturation (establishments per 10k of an ACS base).
pub const SATURATION_DATASET: &str = "saturation";

/// RFC-3339 UTC micros for *now* — the `as_of` a derived write is stamped with.
///
/// Deliberately a **provenance** value, never a record field: provenance lives
/// on the revision, outside the change-detection hash, so an as-of that moves
/// every run cannot mark every blended row `changed` (the churn trap the
/// `cordis/topic_stats` rollup documents).
pub fn as_of_now() -> String {
    pumper_core::datasets::ts(chrono::Utc::now())
}

/// The `index_datasets` specs a census run declares: the two PRODUCT datasets
/// under the virtual [`MARKET_APP`] namespace.
///
/// Two things ride on this, and neither works without it (worker.rs
/// `dataset_search_docs` / `run_indexed_apps`):
///  - per-record full-text search docs for the blend and the saturation
///    ranking, instead of one opaque `_job` snapshot per run;
///  - the `census` namespace entering the run's `indexed_apps`, which is what
///    lets a watch, trigger or saved search scoped to app `census` fire at all.
///
/// Both datasets are declared by every census app because every census app
/// re-derives the blend (and the blend reads saturation), so whichever ran last
/// is the one that must publish. A spec naming a dataset this particular run did
/// not touch costs one empty `changes_since` and yields no documents — the
/// honest no-op, not a fabricated one.
pub fn product_index_datasets() -> Value {
    json!([
        { "app": MARKET_APP, "dataset": MARKET_BLEND_DATASET },
        { "app": MARKET_APP, "dataset": SATURATION_DATASET },
    ])
}

/// Adds [`product_index_datasets`] to a census run result. A non-object result
/// passes through untouched (there is nowhere honest to put the key).
pub fn with_product_index(mut result: Value) -> Value {
    if let Value::Object(map) = &mut result {
        map.insert("index_datasets".into(), product_index_datasets());
    }
    result
}

/// Provenance for a write into the virtual [`MARKET_APP`] namespace.
///
/// These writes go straight through `ctx.datasets` (the namespace belongs to no
/// app), which bypasses `AppContext`'s automatic stamping — so before this every
/// blend and saturation revision carried `Provenance::default()`, i.e. nothing
/// at all. Three facts ARE known and are recorded: the producing job, the input
/// datasets the value was derived from, and when the derivation ran.
///
/// `artifact_sha`/`rules_hash` stay `None` on purpose: a derived row has no
/// archived body of its own and no RuleSet, so it is not replayable and must not
/// claim to be (`Provenance::replayable` stays false, and
/// `POST /provenance/.../rederive` keeps refusing).
pub fn derived_provenance(ctx: &AppContext, dataset: &str, inputs: &[&str]) -> Provenance {
    Provenance {
        job_id: Some(ctx.job_id.to_string()),
        // `derived://` — not an http(s) URL, because nothing was fetched to
        // produce this row. It names the join's inputs and when it ran.
        source_url: Some(format!(
            "derived://{MARKET_APP}/{dataset}?inputs={}&as_of={}",
            inputs.join(","),
            as_of_now()
        )),
        ..Provenance::default()
    }
}

// ---------------------------------------------------------------------------
// Vintage watermark — the guard against a backwards re-run.
//
// Every census source is a fixed annual VINTAGE (CBP 2022, NES 2021, NES-D
// 2021) keyed WITHOUT the year: `{naics}:{state_fips}`. So `params.year=2019`
// on an already-2022 store does not add history, it OVERWRITES current data
// with older data — and, because change detection only sees "the numbers
// moved", publishes the regression as a FORWARD change: a `changed` revision,
// a webhook, every watch and trigger on the dataset, and a search re-index.
//
// The design decision (2026-08-12), of the two the brief offered:
//   (a) put the vintage in the record KEY so vintages coexist — rejected. It
//       multiplies every dataset by its history, forces every reader (the
//       blend, `/datasets`, exports, the SDK) to learn a "pick the latest
//       vintage" rule, and there is no `sync_many` reachable for these
//       namespaces, so the old-keyed rows would linger anyway.
//   (b) keep one row per cell, pin the vintage as a FIELD (already true — every
//       record carries `year`) and add a per-dataset watermark that refuses an
//       older-year run. CHOSEN: it makes the dangerous case loud and costs one
//       row per dataset.
//
// A rewind stays POSSIBLE — `params.allow_vintage_rewind = true` — because
// re-pointing a store at an older vintage is a legitimate operator action. It
// just cannot happen by accident, which is how it happened before.
// ---------------------------------------------------------------------------

/// Per-app dataset holding one vintage watermark per guarded dataset, keyed by
/// the dataset name. Lives in the app's OWN namespace (not `census`), because a
/// watermark is a fact about that app's ingest.
pub const VINTAGE_DATASET: &str = "vintages";

/// How a requested vintage compares to the one already held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VintageVerdict {
    /// Nothing held yet — any vintage is the first.
    FirstRun,
    /// Newer than what is held: the ordinary annual refresh.
    Advance,
    /// The same vintage again: a re-run, which is exactly what a scheduled
    /// annual job does for most of the year.
    Rerun,
    /// OLDER than what is held — the dangerous one.
    Rewind,
    /// One of the two vintages is not a plain year, so they cannot be ordered.
    /// Never blocks: a guard that cannot judge must not refuse.
    Unorderable,
}

/// Compare a requested vintage with the stored watermark. Pure.
pub fn vintage_verdict(requested: &str, held: Option<&str>) -> VintageVerdict {
    let Some(held) = held else {
        return VintageVerdict::FirstRun;
    };
    match (requested.trim().parse::<u32>(), held.trim().parse::<u32>()) {
        (Ok(r), Ok(h)) if r > h => VintageVerdict::Advance,
        (Ok(r), Ok(h)) if r == h => VintageVerdict::Rerun,
        (Ok(_), Ok(_)) => VintageVerdict::Rewind,
        _ => VintageVerdict::Unorderable,
    }
}

/// Whether the run asked to be allowed to write an older vintage over a newer
/// one (`params.allow_vintage_rewind`).
pub fn rewind_allowed(ctx: &AppContext) -> bool {
    ctx.params
        .get("allow_vintage_rewind")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

/// Reads `dataset`'s vintage watermark and decides whether this run may write.
///
/// Returns the block the run result reports; **errors** on an unapproved
/// [`VintageVerdict::Rewind`] before a single row is written, naming the escape
/// hatch. Fail-open on a store read error is deliberately NOT offered: the
/// whole point is that the write does not happen when we cannot prove it is
/// safe — a read failure propagates like any other.
pub async fn guard_vintage(ctx: &AppContext, dataset: &str, year: &str) -> Result<Value> {
    let held = ctx
        .datasets
        .get(&ctx.app, VINTAGE_DATASET, dataset)
        .await?
        .and_then(|r| {
            r.data
                .get("year")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    let verdict = vintage_verdict(year, held.as_deref());
    let allowed = rewind_allowed(ctx);
    if verdict == VintageVerdict::Rewind && !allowed {
        return Err(Error::App(format!(
            "{}/{dataset} holds vintage {} and this run asked for {year}: writing it would \
             OVERWRITE current data with older data and publish the regression as a forward \
             change (a `changed` revision, every watch/trigger on the dataset, a search \
             re-index). Refused. Pass params.allow_vintage_rewind = true if re-pointing the \
             store at {year} is what you mean.",
            ctx.app,
            held.as_deref().unwrap_or("?"),
        )));
    }
    Ok(json!({
        "dataset": dataset,
        "requested": year,
        "held": held,
        "verdict": match verdict {
            VintageVerdict::FirstRun => "first_run",
            VintageVerdict::Advance => "advance",
            VintageVerdict::Rerun => "rerun",
            VintageVerdict::Rewind => "rewind_allowed",
            VintageVerdict::Unorderable => "unorderable",
        },
        "rewind_allowed": allowed,
    }))
}

/// Moves `dataset`'s watermark to `year` after a successful write.
///
/// Always set to the run's own vintage, including an APPROVED rewind: the store
/// now holds that vintage, so the watermark must describe the data rather than
/// the high-water mark of runs that once happened.
pub async fn record_vintage(ctx: &AppContext, dataset: &str, year: &str) -> Result<()> {
    ctx.upsert(
        VINTAGE_DATASET,
        dataset,
        &json!({ "dataset": dataset, "year": year }),
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Mixed-grain NAICS.
// ---------------------------------------------------------------------------

/// From the NAICS codes contributing to ONE roll-up cell, the codes that may be
/// **summed**: a code that is a strict prefix of another COVERS it, so the
/// covered (finer) codes are dropped.
///
/// The bug this kills: a `trades/taxonomy` registry entry listing both `"2382"`
/// and `"238220"` makes census-density fetch the 4-digit AGGREGATE and one of
/// its 6-digit COMPONENTS as two separate CBP requests, stored as two records
/// (`2382:06`, `238220:06`). The blend truncates both to naics4 `2382` and adds
/// them — the aggregate plus a part of itself, in the cell whose grain IS the
/// aggregate. Nothing in `trades-common` validates grain, and the double count
/// is invisible: it looks like a state with more plumbers.
///
/// **Keep the aggregate, drop the components** (the choice, recorded): the
/// roll-up cell's grain is the 4-digit group, and the 4-digit row IS the
/// complete total for it. Keeping the components instead would under-count
/// every sibling code that was not requested (2382 also covers 238290), i.e.
/// trade a visible double-count for an invisible shortfall.
///
/// Returns `(counted, dropped)`, both sorted — the dropped list is emitted on
/// the record, because a silently corrected input is its own kind of lie.
pub fn covering_naics(codes: &std::collections::BTreeSet<String>) -> (Vec<String>, Vec<String>) {
    let mut counted = Vec::new();
    let mut dropped = Vec::new();
    for code in codes {
        // Covered when some OTHER code in the set is a strict prefix of it.
        let covered = codes
            .iter()
            .any(|other| other != code && code.starts_with(other.as_str()));
        if covered {
            dropped.push(code.clone());
        } else {
            counted.push(code.clone());
        }
    }
    (counted, dropped)
}

// ---------------------------------------------------------------------------
// Month arithmetic for the BFS monthly series (`YYYY-MM`).
// ---------------------------------------------------------------------------

/// `YYYY-MM` → months since year 0, or `None` when the period is not a
/// well-formed month. The common scale every comparison below runs on.
pub fn month_index(period: &str) -> Option<i32> {
    let (y, m) = period.trim().split_once('-')?;
    let (y, m) = (y.parse::<i32>().ok()?, m.parse::<i32>().ok()?);
    (1..=12).contains(&m).then_some(y * 12 + (m - 1))
}

/// Whether `periods` are consecutive calendar months with no gap, in the order
/// given. An empty or single-element window is trivially contiguous; a period
/// that is not a well-formed month makes the window non-contiguous (we cannot
/// prove it is).
pub fn months_contiguous(periods: &[String]) -> bool {
    let mut prev: Option<i32> = None;
    for p in periods {
        let Some(i) = month_index(p) else {
            return false;
        };
        if let Some(prev) = prev {
            if i != prev + 1 {
                return false;
            }
        }
        prev = Some(i);
    }
    true
}

/// Whole months from `period` to `now_month`, or `None` when either is not a
/// well-formed month. Negative when the period is in the future.
pub fn months_between(period: &str, now_month: &str) -> Option<i32> {
    Some(month_index(now_month)? - month_index(period)?)
}

/// The current calendar month as `YYYY-MM` (UTC).
pub fn current_month() -> String {
    chrono::Utc::now().format("%Y-%m").to_string()
}

/// Parses a Census numeric cell.
///
/// Missing, non-numeric, and **negative** values are treated as suppressed
/// (`None`) rather than data: Census encodes disclosure suppression and jam
/// values as negative sentinels (e.g. `-666666666`), so parsing them as real
/// numbers corrupts every total they reach.
pub fn census_num(cell: Option<&String>) -> Option<i64> {
    cell.and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|v| *v >= 0)
}

/// Whether a Census response is an **empty answer** — the API's way of saying
/// "nothing is published at this grain", not a failure.
///
/// Census returns `204 No Content` (sometimes a `200` with an empty body) for a
/// NAICS × geography whose cells are fully disclosure-suppressed or simply not
/// published: NES at 6-digit × state, NES-D at 3/4-digit, a sector with no
/// series. That is a **contract-valid** answer about ONE request, and the run's
/// other requests are unaffected.
///
/// It has to be checked BEFORE the JSON-shape guard, because `204` is inside
/// `HttpResponse::is_success`'s 200..300: a bare 204 falls through to
/// "response was not JSON" and takes the whole multi-trade run down with it —
/// which is exactly what census-density did while its three siblings
/// skipped-and-noted (bughunt 2026-07-14 #3). One definition, used by all four,
/// so the guard cannot land in only part of the fleet again.
pub fn is_empty_answer(status: u16, body: &str) -> bool {
    // "Nothing published at this grain" is only a valid answer on a SUCCESS
    // status. A `204 No Content` says so on its own; a `2xx` with an empty body
    // says the same by another route. But an empty body on a `5xx` (or any
    // non-2xx) is a TRANSIENT FAILURE, not an empty answer — reading it as
    // "nothing published" would silently drop the request instead of letting the
    // non-success path fail it into the job's retry ladder. So the empty-body
    // case is gated on a success status; only a bare 204 is unconditional.
    status == 204 || ((200..300).contains(&status) && body.trim().is_empty())
}

/// Resolves the free Census API key: `params.api_key`, else env
/// `CENSUS_API_KEY`. `app` names the caller in the error so the operator knows
/// which app is asking.
pub fn api_key(ctx: &AppContext, app: &str) -> Result<String> {
    ctx.params
        .get("api_key")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| std::env::var("CENSUS_API_KEY").ok())
        .filter(|k| !k.trim().is_empty())
        .ok_or_else(|| {
            Error::App(format!(
                "{app} needs a free Census API key — set env CENSUS_API_KEY or pass \
                 params.api_key. Get one instantly at \
                 https://api.census.gov/data/key_signup.html"
            ))
        })
}

// ---------------------------------------------------------------------------
// Provenance (M12) for the keyed Census JSON APIs.
// ---------------------------------------------------------------------------

/// The Census request URL with the API key removed — what a provenance stamp is
/// allowed to record. The live URL carries `key=<secret>`, and `source_url` is
/// read back by anyone with dataset access (`GET /datasets/.../revisions`), so
/// stamping it verbatim would publish the shared credential into the ledger.
/// Everything else about the request (dataset, vintage, predicates) is kept —
/// that is precisely the part that makes the stamp useful.
pub fn redact_key(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    for (i, part) in url.split('&').enumerate() {
        if i > 0 {
            out.push('&');
        }
        match part.split_once("key=") {
            // Only a whole `key=` parameter (start of the query or of this
            // `&`-segment) — never a substring like `api_key=`/`&mykey=`.
            Some((head, _)) if head.is_empty() || head.ends_with('?') => {
                out.push_str(head);
                out.push_str("key=REDACTED");
            }
            _ => out.push_str(part),
        }
    }
    out
}

/// sha256 (hex) of the bytes actually written as the job artifact — the
/// `artifact_sha` half of a provenance stamp. Hash the exact bytes handed to
/// `ctx.save_artifact`, never a re-serialization, or the stamp points at a body
/// that was never stored.
pub fn artifact_sha(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Provenance for one Census API response: the key-redacted request URL plus
/// the sha of the artifact that response was archived as. `rules_hash` stays
/// `None` — these records are parsed by compiled code, not by a registered
/// RuleSet, and inventing a hash for one would be a lie.
pub fn http_provenance(url: &str, artifact_bytes: &[u8]) -> Provenance {
    Provenance {
        source_url: Some(redact_key(url)),
        artifact_sha: Some(artifact_sha(artifact_bytes)),
        ..Provenance::default()
    }
}

/// Folds one per-request upsert summary into a run-level total. Provenance is
/// per-request (one URL, one artifact), so these apps upsert per request rather
/// than once per run — the job result still reports a single new/changed/
/// unchanged rollup.
pub fn merge_summary(acc: &mut UpsertSummary, mut next: UpsertSummary) {
    acc.new.append(&mut next.new);
    acc.changed.append(&mut next.changed);
    acc.unchanged += next.unchanged;
    acc.removed.append(&mut next.removed);
}

/// USPS abbreviation for a state FIPS code; an unknown code passes through
/// unchanged so unexpected geographies stay traceable rather than becoming "??".
pub fn state_abbr(fips: &str) -> &str {
    match fips {
        "01" => "AL",
        "02" => "AK",
        "04" => "AZ",
        "05" => "AR",
        "06" => "CA",
        "08" => "CO",
        "09" => "CT",
        "10" => "DE",
        "11" => "DC",
        "12" => "FL",
        "13" => "GA",
        "15" => "HI",
        "16" => "ID",
        "17" => "IL",
        "18" => "IN",
        "19" => "IA",
        "20" => "KS",
        "21" => "KY",
        "22" => "LA",
        "23" => "ME",
        "24" => "MD",
        "25" => "MA",
        "26" => "MI",
        "27" => "MN",
        "28" => "MS",
        "29" => "MO",
        "30" => "MT",
        "31" => "NE",
        "32" => "NV",
        "33" => "NH",
        "34" => "NJ",
        "35" => "NM",
        "36" => "NY",
        "37" => "NC",
        "38" => "ND",
        "39" => "OH",
        "40" => "OK",
        "41" => "OR",
        "42" => "PA",
        "44" => "RI",
        "45" => "SC",
        "46" => "SD",
        "47" => "TN",
        "48" => "TX",
        "49" => "UT",
        "50" => "VT",
        "51" => "VA",
        "53" => "WA",
        "54" => "WV",
        "55" => "WI",
        "56" => "WY",
        "72" => "PR",
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Owner-age band helpers (NES-D succession math), shared between the
// census-nesd ingester and the census-density blend so the 55+ classification
// can't drift between the app that stores bands and the join that reads them.
// ---------------------------------------------------------------------------

/// Whether an NES-D `OWNCHAR_LABEL` names a *reported* age band — i.e. a band
/// that belongs in the share denominator. "Total reporting" / "Item not
/// reported" / "Don't know"-style rows are structural, not age data.
pub fn is_reported_age_band(label: &str) -> bool {
    let l = label.to_lowercase();
    !(l.is_empty()
        || l.contains("total")
        || l.contains("not report")
        || l.contains("don't know")
        || l.contains("dont know")
        || l.contains("unknown")
        || l.contains("all owners"))
}

/// Whether an age-band label is 55-or-older. Label-driven (the first integer in
/// the label, e.g. "55 to 64" → 55, "65 or over" → 65, "Under 25" → 25) so a
/// vintage that reshuffles band *codes* can't silently misclassify.
pub fn is_55_plus_age_band(label: &str) -> bool {
    let mut digits = String::new();
    for c in label.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
        } else if !digits.is_empty() {
            break;
        }
    }
    digits.parse::<u32>().map(|n| n >= 55).unwrap_or(false)
}

/// Share of owners aged 55+ across the *reported* age bands: `(band label,
/// owner count)` pairs in, raw fraction out. `None` (never a fabricated 0)
/// when no reported band carries a positive total — suppression must yield an
/// absent share, not a 0% wave.
pub fn owner_age_share_55plus(bands: &[(String, i64)]) -> Option<f64> {
    let mut total: i64 = 0;
    let mut older: i64 = 0;
    for (label, owners) in bands {
        if !is_reported_age_band(label) {
            continue;
        }
        total += owners;
        if is_55_plus_age_band(label) {
            older += owners;
        }
    }
    (total > 0).then(|| older as f64 / total as f64)
}

/// BFS `category_code` for a NAICS trade-group code: the 2-digit sector prefix
/// (`"2382"` → `"NAICS23"`). `None` when the code doesn't start with two
/// digits. BFS publishes at NAICS *sector* grain only — callers must label
/// anything joined through this as sector-grain, never trade-level.
pub fn bfs_sector_category(naics: &str) -> Option<String> {
    let prefix: String = naics.chars().take(2).collect();
    (prefix.len() == 2 && prefix.chars().all(|c| c.is_ascii_digit()))
        .then(|| format!("NAICS{prefix}"))
}

#[cfg(test)]
mod tests {
    use super::{
        artifact_sha, bfs_sector_category, census_num, covering_naics, http_provenance,
        is_55_plus_age_band, is_empty_answer, is_reported_age_band, merge_summary, months_between,
        months_contiguous, owner_age_share_55plus, product_index_datasets, redact_key, state_abbr,
        with_product_index, MARKET_APP, MARKET_BLEND_DATASET, SATURATION_DATASET,
    };
    use pumper_core::UpsertSummary;
    use serde_json::json;

    /// The anti-pattern: a re-run with an OLDER `year` rewriting current data
    /// backwards and publishing the regression as a forward change. The verdict
    /// must name that case distinctly from the two harmless ones.
    #[test]
    fn an_older_vintage_is_a_rewind_not_an_ordinary_change() {
        use super::{vintage_verdict, VintageVerdict as V};
        assert_eq!(vintage_verdict("2022", None), V::FirstRun);
        assert_eq!(vintage_verdict("2022", Some("2021")), V::Advance);
        assert_eq!(vintage_verdict("2022", Some("2022")), V::Rerun);
        // The dangerous one.
        assert_eq!(vintage_verdict("2019", Some("2022")), V::Rewind);
        assert_eq!(vintage_verdict("2021", Some("2022")), V::Rewind);
        // Years are compared as NUMBERS: a lexicographic compare would be right
        // here by luck and wrong the moment a vintage is not zero-padded.
        assert_eq!(vintage_verdict("999", Some("2022")), V::Rewind);
        assert_eq!(vintage_verdict("10000", Some("2022")), V::Advance);
        // Unorderable never blocks — a guard that cannot judge must not refuse.
        assert_eq!(vintage_verdict("2021Q3", Some("2022")), V::Unorderable);
        assert_eq!(vintage_verdict("2022", Some("latest")), V::Unorderable);
    }

    /// The anti-pattern: a mixed-grain registry entry (`"2382"` AND `"238220"`)
    /// summing an aggregate with a component of itself inside the naics4 cell
    /// whose grain IS the aggregate — a silent double count that reads as a
    /// state with more plumbers.
    #[test]
    fn a_covering_aggregate_drops_its_components_instead_of_double_summing() {
        let set = |v: &[&str]| -> std::collections::BTreeSet<String> {
            v.iter().map(|s| s.to_string()).collect()
        };
        // The literal case: 2382 covers 238220 and 238210.
        let (counted, dropped) = covering_naics(&set(&["2382", "238210", "238220"]));
        assert_eq!(counted, vec!["2382".to_string()]);
        assert_eq!(dropped, vec!["238210".to_string(), "238220".to_string()]);
        // No aggregate present → every component counts (the normal case).
        let (counted, dropped) = covering_naics(&set(&["238210", "238220"]));
        assert_eq!(counted, vec!["238210".to_string(), "238220".to_string()]);
        assert!(dropped.is_empty());
        // Three grains: only the coarsest survives.
        let (counted, _) = covering_naics(&set(&["23", "2382", "238220"]));
        assert_eq!(counted, vec!["23".to_string()]);
        // A shared prefix that is not a CODE in the set covers nothing.
        let (counted, dropped) = covering_naics(&set(&["238210", "238290"]));
        assert_eq!(counted.len(), 2);
        assert!(dropped.is_empty());
        assert_eq!(covering_naics(&set(&[])), (vec![], vec![]));
    }

    /// The anti-pattern: 12 values that happen to sit next to each other in a
    /// vector treated as "the trailing twelve months" when three of those
    /// months are missing from the series.
    #[test]
    fn a_gapped_window_is_not_contiguous_months() {
        let months = |v: &[&str]| -> Vec<String> { v.iter().map(|s| s.to_string()).collect() };
        assert!(months_contiguous(&months(&[
            "2024-11", "2024-12", "2025-01"
        ])));
        assert!(months_contiguous(&months(&["2025-01"])));
        assert!(months_contiguous(&[]));
        // A missing month.
        assert!(!months_contiguous(&months(&["2024-11", "2025-01"])));
        // A year boundary that skips December.
        assert!(!months_contiguous(&months(&["2024-11", "2025-02"])));
        // Out of order is not contiguous either — the caller must sort first.
        assert!(!months_contiguous(&months(&["2025-01", "2024-12"])));
        // Malformed periods cannot be proven contiguous.
        assert!(!months_contiguous(&months(&["2024-13", "2024-14"])));
        assert!(!months_contiguous(&months(&["2024", "2024-01"])));

        assert_eq!(months_between("2026-06", "2026-08"), Some(2));
        assert_eq!(months_between("2025-12", "2026-01"), Some(1));
        assert_eq!(months_between("2026-08", "2026-08"), Some(0));
        assert_eq!(months_between("2026-09", "2026-08"), Some(-1));
        assert_eq!(months_between("nope", "2026-08"), None);
    }

    /// The anti-pattern: a bare `204 No Content` — a contract-VALID "nothing
    /// published at this grain" — read as a broken payload, because 204 is
    /// inside `is_success` and falls through to the JSON-shape guard. That
    /// aborted a whole multi-trade census-density run on one suppressed cell.
    #[test]
    fn a_bare_204_is_an_empty_answer_not_a_json_parse_failure() {
        assert!(is_empty_answer(204, ""));
        assert!(is_empty_answer(204, "[[\"NAME\"]]"), "204 wins on its own");
        // A 200 with nothing in it is the same answer by another route.
        assert!(is_empty_answer(200, ""));
        assert!(is_empty_answer(200, "   \n "));
        // Real payloads — and real failures — are NOT empty answers: an HTML
        // missing-key page must still reach the loud "not JSON" error.
        assert!(!is_empty_answer(200, "[[\"NAME\",\"ESTAB\"]]"));
        assert!(!is_empty_answer(200, "<html>missing key</html>"));
        assert!(!is_empty_answer(400, "unknown predicate variable"));
    }

    /// Regression: an EMPTY body on a non-success status is a transient failure,
    /// not "nothing published". Reading a 5xx-with-no-body as an empty answer
    /// swallowed the request and dropped it from the run instead of letting the
    /// non-success path fail it into the retry ladder — silent data loss on a
    /// server hiccup. Only an empty body on a SUCCESS status is an empty answer.
    #[test]
    fn an_empty_body_on_a_non_success_status_is_a_failure_not_an_empty_answer() {
        // The bug: these must NOT be read as "nothing published".
        assert!(!is_empty_answer(500, ""), "500 with no body is a transient failure");
        assert!(!is_empty_answer(503, "   \n "), "503 with a blank body is a failure");
        assert!(!is_empty_answer(502, ""));
        assert!(!is_empty_answer(429, ""), "rate-limit with no body must retry, not skip");
        assert!(!is_empty_answer(400, ""), "a 4xx with no body is still not an empty answer");
        // The success cases are unchanged — an empty answer still means empty.
        assert!(is_empty_answer(200, ""));
        assert!(is_empty_answer(204, ""));
        assert!(is_empty_answer(204, "[[\"NAME\"]]"), "204 still wins on its own");
    }

    /// All four census apps must route their empty-answer check through
    /// `is_empty_answer` — the convention that a suppressed cell skips ONE
    /// request instead of failing the run, enforced as an inventory rather than
    /// as a sentence in a doc (the fleet already drifted here once).
    #[test]
    fn every_census_app_uses_the_shared_empty_answer_guard() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates/apps");
        let missing: Vec<&str> = [
            "census-density",
            "census-nonemp",
            "census-nesd",
            "census-bfs",
        ]
        .into_iter()
        .filter(|app| {
            let src = std::fs::read_to_string(root.join(app).join("src/lib.rs"))
                .unwrap_or_else(|e| panic!("read {app}/src/lib.rs: {e}"));
            !src.contains("census_common::is_empty_answer(")
        })
        .collect();
        assert!(
            missing.is_empty(),
            "these census apps hand-roll their empty-answer check (or dropped it): {missing:?}"
        );
    }

    /// The two product datasets are what a watch/trigger/saved search on app
    /// `census` can ever see (worker `run_indexed_apps`). Dropping either from
    /// the spec list silently un-hooks that dataset — pinned here.
    #[test]
    fn product_specs_name_both_census_products_under_the_virtual_app() {
        assert_eq!(
            product_index_datasets(),
            json!([
                { "app": "census", "dataset": "market_blend" },
                { "app": "census", "dataset": "saturation" },
            ])
        );
        assert_eq!(
            (MARKET_APP, MARKET_BLEND_DATASET, SATURATION_DATASET),
            ("census", "market_blend", "saturation")
        );
    }

    #[test]
    fn with_product_index_adds_the_specs_and_keeps_the_result() {
        let out = with_product_index(json!({ "source": "census/cbp/2022", "records": 3 }));
        assert_eq!(out["source"], "census/cbp/2022");
        assert_eq!(out["records"], 3);
        assert_eq!(out["index_datasets"], product_index_datasets());
        // A non-object result has nowhere honest to carry specs.
        assert_eq!(with_product_index(json!([1, 2])), json!([1, 2]));
    }

    /// The credential must never reach a provenance stamp — `source_url` is
    /// readable by every dataset consumer. Gutting `redact_key` to identity
    /// turns this red.
    #[test]
    fn redact_key_strips_the_census_credential_and_keeps_the_query() {
        let url = "https://api.census.gov/data/2021/nonemp?get=NESTAB&for=state:*&NAICS2017=2382&key=abc123secret";
        let red = redact_key(url);
        assert!(!red.contains("abc123secret"), "{red}");
        assert!(red.ends_with("key=REDACTED"));
        assert!(red.contains("NAICS2017=2382") && red.contains("for=state:*"));
        // Key first in the query string is redacted too.
        assert_eq!(
            redact_key("https://x/data?key=s3cret&get=A"),
            "https://x/data?key=REDACTED&get=A"
        );
        // A URL with no key is untouched.
        assert_eq!(redact_key("https://x/data?get=A"), "https://x/data?get=A");
    }

    #[test]
    fn artifact_sha_hashes_the_stored_bytes() {
        // sha256("") — pins that we hash the bytes, not a serde form of them.
        assert_eq!(
            artifact_sha(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_ne!(artifact_sha(b"a"), artifact_sha(b"b"));
    }

    #[test]
    fn http_provenance_stamps_url_and_sha_but_never_invents_a_rules_hash() {
        let p = http_provenance("https://x/data?get=A&key=s3cret", b"[[\"a\"]]");
        assert_eq!(
            p.source_url.as_deref(),
            Some("https://x/data?get=A&key=REDACTED")
        );
        assert_eq!(
            p.artifact_sha.as_deref(),
            Some(&*artifact_sha(b"[[\"a\"]]"))
        );
        // No RuleSet produced these records — a fabricated hash would claim
        // replayability the app cannot deliver.
        assert!(p.rules_hash.is_none());
        assert!(!p.replayable());
    }

    #[test]
    fn merge_summary_accumulates_every_bucket() {
        let mut acc = UpsertSummary {
            new: vec!["a".into()],
            changed: vec![],
            unchanged: 2,
            removed: vec![],
        };
        merge_summary(
            &mut acc,
            UpsertSummary {
                new: vec!["b".into()],
                changed: vec!["c".into()],
                unchanged: 3,
                removed: vec!["d".into()],
            },
        );
        assert_eq!(acc.new, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(acc.changed, vec!["c".to_string()]);
        assert_eq!(acc.unchanged, 5);
        assert_eq!(acc.removed, vec!["d".to_string()]);
    }

    #[test]
    fn census_num_rejects_suppression_sentinels() {
        assert_eq!(census_num(Some(&"1234".to_string())), Some(1234));
        assert_eq!(census_num(Some(&" 0 ".to_string())), Some(0));
        // Negative jam/annotation sentinels are suppression, not data.
        assert_eq!(census_num(Some(&"-666666666".to_string())), None);
        assert_eq!(census_num(Some(&"-1".to_string())), None);
        // Missing / non-numeric cells are suppressed too.
        assert_eq!(census_num(Some(&"".to_string())), None);
        assert_eq!(census_num(Some(&"D".to_string())), None);
        assert_eq!(census_num(None), None);
    }

    #[test]
    fn state_abbr_maps_fips_and_passes_unknown_through() {
        assert_eq!(state_abbr("06"), "CA");
        assert_eq!(state_abbr("72"), "PR");
        assert_eq!(state_abbr("99"), "99");
    }

    #[test]
    fn age_band_classifier_reads_the_first_integer_in_the_label() {
        assert!(is_55_plus_age_band("55 to 64"));
        assert!(is_55_plus_age_band("65 or over"));
        assert!(is_55_plus_age_band("Owners aged 65 years and older"));
        assert!(!is_55_plus_age_band("Under 25"));
        assert!(!is_55_plus_age_band("25 to 34"));
        assert!(!is_55_plus_age_band("45 to 54"));
        assert!(!is_55_plus_age_band("no digits here"));
    }

    #[test]
    fn structural_bands_are_not_reported_age_bands() {
        assert!(is_reported_age_band("55 to 64"));
        assert!(!is_reported_age_band("Total reporting"));
        assert!(!is_reported_age_band("Item not reported"));
        assert!(!is_reported_age_band("Don't know"));
        assert!(!is_reported_age_band(""));
    }

    #[test]
    fn share_55plus_sums_reported_bands_only_and_never_fabricates_zero() {
        let bands = |v: &[(&str, i64)]| -> Vec<(String, i64)> {
            v.iter().map(|(l, n)| (l.to_string(), *n)).collect()
        };
        // 30 + 10 of 100 reported owners are 55+; the "Total reporting" row and
        // the unreported row must not enter the denominator.
        let share = owner_age_share_55plus(&bands(&[
            ("Under 25", 5),
            ("25 to 54", 55),
            ("55 to 64", 30),
            ("65 or over", 10),
            ("Total reporting", 100),
            ("Item not reported", 40),
        ]))
        .expect("share");
        assert!((share - 0.4).abs() < 1e-9);
        // All bands suppressed/structural → None, not 0.0.
        assert_eq!(
            owner_age_share_55plus(&bands(&[("Total reporting", 90)])),
            None
        );
        assert_eq!(owner_age_share_55plus(&[]), None);
    }

    #[test]
    fn bfs_sector_category_takes_the_two_digit_sector_prefix() {
        assert_eq!(bfs_sector_category("2382"), Some("NAICS23".into()));
        assert_eq!(bfs_sector_category("5617"), Some("NAICS56".into()));
        assert_eq!(bfs_sector_category("56"), Some("NAICS56".into()));
        assert_eq!(bfs_sector_category("5"), None);
        assert_eq!(bfs_sector_category("ab12"), None);
    }
}
