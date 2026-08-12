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
        artifact_sha, bfs_sector_category, census_num, http_provenance, is_55_plus_age_band,
        is_reported_age_band, merge_summary, owner_age_share_55plus, product_index_datasets,
        redact_key, state_abbr, with_product_index, MARKET_APP, MARKET_BLEND_DATASET,
        SATURATION_DATASET,
    };
    use pumper_core::UpsertSummary;
    use serde_json::json;

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
