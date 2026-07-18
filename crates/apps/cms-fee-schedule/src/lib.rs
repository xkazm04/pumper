//! Pumper app: CMS fee-schedule release watcher.
//!
//! Keeps Counterbill's Medicare reference-price database fresh. Counterbill bakes
//! the CMS Physician Fee Schedule (PFS) into generated tables via
//! `scripts/ingest-cms-pfs.mjs`, pinned to one RVU release (e.g. `RVU26A`). CMS
//! republishes the Relative Value Files quarterly (RVU{YY}A/B/C/D) with an annual
//! conversion-factor change — so the baked data silently goes stale. This app is
//! the freshness signal: it detects the LATEST published release and reports
//! whether it is newer than what the caller currently has baked.
//!
//! It deliberately does NOT download the (binary, large) ZIP — the http engine
//! yields a `String` body, so instead it reads the server-rendered RVU index page
//! and detects the `rvuYYq` release tokens in it. The heavy parse/regeneration
//! stays in the Counterbill ingest script; this app only answers "is there a
//! newer release, and where is it?".
//!
//! Params: `{ "schedule": "pfs" }`
//!   · `schedule`      — only `"pfs"` is supported today (extensible to clfs/asp).
//!   · `known_release` — OPTIONAL explicit baseline (the release Counterbill has
//!                       baked). Omitted by default: the watcher **self-baselines**
//!                       off the release it stored last run, so `is_newer_than_known`
//!                       clears itself once a release is seen (`baseline_source`
//!                       reports `param`/`stored`/`none`).

use async_trait::async_trait;
use pumper_core::{AppContext, ChangeKind, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct CmsFeeSchedule;

/// CMS PFS Relative Value Files index — lists every `RVU{YY}{Q}` release.
const PFS_INDEX_URL: &str =
    "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files";

/// A parsed RVU release, e.g. `RVU26B` → year 2026, quarter `B` (Apr).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Release {
    year: u32,
    /// Uppercase quarter letter `A`..=`D` (A=Jan, B=Apr, C=Jul, D=Oct).
    quarter: char,
}

impl Release {
    /// Canonical id, e.g. `"RVU26B"`.
    fn id(&self) -> String {
        format!("RVU{:02}{}", self.year % 100, self.quarter)
    }

    /// Total-order key: a newer year, then a later quarter, sorts greater.
    fn ord_key(&self) -> (u32, char) {
        (self.year, self.quarter)
    }

    /// Quarter as 1..=4 (A→1 … D→4).
    fn quarter_num(&self) -> u32 {
        (self.quarter as u32) - ('A' as u32) + 1
    }

    /// The direct ZIP URL CMS publishes for this release (lowercase token) — the
    /// same convention `scripts/ingest-cms-pfs.mjs` fetches.
    fn zip_url(&self) -> String {
        format!(
            "https://www.cms.gov/files/zip/rvu{:02}{}.zip",
            self.year % 100,
            self.quarter.to_ascii_lowercase()
        )
    }

    /// The release's landing page under the index.
    fn source_url(&self) -> String {
        format!(
            "{PFS_INDEX_URL}/rvu{:02}{}",
            self.year % 100,
            self.quarter.to_ascii_lowercase()
        )
    }
}

/// Parse a single release id like `"RVU26A"` (case-insensitive). None if malformed
/// or the quarter is outside A–D.
fn parse_release(s: &str) -> Option<Release> {
    let up = s.trim().to_ascii_uppercase();
    let b = up.as_bytes();
    if b.len() < 6 || &b[0..3] != b"RVU" {
        return None;
    }
    let (d1, d2, q) = (b[3], b[4], b[5]);
    if !(d1.is_ascii_digit() && d2.is_ascii_digit()) || !(b'A'..=b'D').contains(&q) {
        return None;
    }
    let yy = ((d1 - b'0') as u32) * 10 + (d2 - b'0') as u32;
    Some(Release { year: 2000 + yy, quarter: q as char })
}

/// Scan an HTML/text blob for every distinct `rvuYYq` release token (in hrefs or
/// text), returned sorted oldest→newest. Pure — the unit-tested core.
fn detect_releases(html: &str) -> Vec<Release> {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(p) = lower[i..].find("rvu") {
        let pos = i + p;
        i = pos + 3; // advance past this match (monotonic → no infinite loop)
        if pos + 6 <= bytes.len() {
            let (d1, d2, q) = (bytes[pos + 3], bytes[pos + 4], bytes[pos + 5]);
            if d1.is_ascii_digit() && d2.is_ascii_digit() && (b'a'..=b'd').contains(&q) {
                let year = 2000 + ((d1 - b'0') as u32) * 10 + (d2 - b'0') as u32;
                let quarter = (q as char).to_ascii_uppercase();
                if seen.insert((year, quarter)) {
                    out.push(Release { year, quarter });
                }
            }
        }
    }
    out.sort_by_key(Release::ord_key);
    out
}

fn latest(releases: &[Release]) -> Option<Release> {
    releases.iter().max_by_key(|r| r.ord_key()).copied()
}

#[async_trait]
impl ScrapeApp for CmsFeeSchedule {
    fn name(&self) -> &'static str {
        "cms-fee-schedule"
    }

    fn description(&self) -> &'static str {
        "Watches CMS for the latest Physician Fee Schedule (PFS) RVU release and \
         reports whether it is newer than the caller's baked release — the freshness \
         signal behind Counterbill's scripts/ingest-cms-pfs.mjs regeneration. \
         Self-baselines off the last release it stored (so the staleness flag \
         clears itself); pass \"known_release\" to override. Params: \
         {\"schedule\":\"pfs\", \"known_release\": null (optional explicit baseline)}"
    }

    fn schedule(&self) -> Option<&'static str> {
        // 06:00:00 on the 1st of each month (sec min hour day month weekday).
        // CMS PFS releases are quarterly; a monthly check is a cheap, ample cadence.
        Some("0 0 6 1 * *")
    }

    fn default_params(&self) -> Value {
        // No hardcoded `known_release`: the watcher self-baselines off the last
        // release it stored (a stale literal would keep `is_newer_than_known`
        // permanently lit). A caller who knows what Counterbill has baked can still
        // pass `known_release` as an explicit override.
        json!({ "schedule": "pfs" })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let schedule = ctx
            .params
            .get("schedule")
            .and_then(Value::as_str)
            .unwrap_or("pfs");
        if schedule != "pfs" {
            return Err(Error::App(format!(
                "unsupported schedule '{schedule}' (only 'pfs' is supported today)"
            )));
        }

        let response = ctx.engines.http.fetch(HttpRequest::get(PFS_INDEX_URL)).await?;
        if !response.is_success() {
            return Err(Error::App(format!(
                "CMS PFS index returned status {}",
                response.status
            )));
        }
        ctx.save_artifact("pfs-index.html", response.body.as_bytes())
            .await?;

        let releases = detect_releases(&response.body);
        let latest = latest(&releases).ok_or_else(|| {
            Error::App(
                "no RVU release tokens found on the CMS PFS index — the page structure \
                 may have changed (consider the browser engine)"
                    .to_string(),
            )
        })?;

        // Self-baseline: read the release we stored last run BEFORE the upsert
        // below overwrites it. Baseline precedence: explicit `known_release` param
        // (what Counterbill has baked) > the stored release (self-baselining across
        // scheduled runs) > none (cold start). This clears the "permanently stale"
        // alarm — once RVU26B is stored, later runs baseline off it and stop
        // reporting `is_newer_than_known: true` until CMS actually ships RVU26C.
        let stored_prev = ctx
            .datasets
            .get(&ctx.app, "releases", schedule)
            .await?
            .and_then(|r| r.data.get("latest_release").and_then(Value::as_str).map(String::from));
        let param_known = ctx.params.get("known_release").and_then(Value::as_str).map(String::from);
        let (baseline, baseline_source) = match (&param_known, &stored_prev) {
            (Some(k), _) => (Some(k.clone()), "param"),
            (None, Some(s)) => (Some(s.clone()), "stored"),
            (None, None) => (None, "none"),
        };

        // Change detection across scheduled runs: keyed by `schedule`, so a run
        // reports `new`/`changed` only when CMS actually published a newer release.
        let record = json!({
            "latest_release": latest.id(),
            "year": latest.year,
            "quarter": latest.quarter.to_string(),
            "zip_url": latest.zip_url(),
            "source_url": latest.source_url(),
        });
        let change: ChangeKind = ctx.upsert("releases", schedule, &record).await?;

        // Is the detected latest newer than the effective baseline? A cold start
        // (no baseline) treats any detected release as actionable.
        let is_newer = match baseline.as_deref().and_then(parse_release) {
            Some(k) => latest.ord_key() > k.ord_key(),
            None => true,
        };

        Ok(json!({
            "schedule": schedule,
            "latest_release": latest.id(),
            "year": latest.year,
            "quarter": latest.quarter.to_string(),
            "quarter_num": latest.quarter_num(),
            "zip_url": latest.zip_url(),
            "source_url": latest.source_url(),
            "index_url": PFS_INDEX_URL,
            "known_release": param_known,          // the explicit override, if any
            "baseline": baseline,                  // the release we compared against
            "baseline_source": baseline_source,    // "param" | "stored" | "none"
            "is_newer_than_known": is_newer,
            "change_since_last_run": change,      // "new" | "changed" | "unchanged"
            "is_fresh": change.is_fresh(),         // new or changed since last run
            "releases_found": releases.iter().map(Release::id).collect::<Vec<_>>(),
            // Structured ingest target so a `dataset` trigger (on_change=fresh) can
            // fan out to an ingest job reading these keys from `_trigger`, instead
            // of a human reading the prose hint below.
            "ingest": {
                "release": latest.id(),
                "zip_url": latest.zip_url(),
                "source_url": latest.source_url(),
            },
            "ingest_hint": format!(
                "If newer: point scripts/ingest-cms-pfs.mjs at {} (update its ZIP_URL + RELEASE, \
                 or pass --zip a download of {}) and run `npm run ingest:pfs`.",
                latest.id(),
                latest.zip_url()
            ),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A realistic slice: releases appear in both hrefs (lowercase) and link text
    // (uppercase), with a duplicate and an older-year entry to exercise dedup+sort.
    const SAMPLE: &str = r#"
        <ul class="rvu-list">
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu26a">RVU26A (January 2026)</a></li>
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu26b">RVU26B (April 2026)</a></li>
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25d">RVU25D (October 2025)</a></li>
        </ul>
    "#;

    #[test]
    fn detects_dedupes_and_sorts_releases() {
        let ids: Vec<String> = detect_releases(SAMPLE).iter().map(Release::id).collect();
        // href + text mention the same release; deduped, sorted oldest→newest.
        assert_eq!(ids, vec!["RVU25D", "RVU26A", "RVU26B"]);
    }

    #[test]
    fn latest_prefers_newest_year_then_quarter() {
        assert_eq!(latest(&detect_releases(SAMPLE)).unwrap().id(), "RVU26B");
    }

    #[test]
    fn detects_nothing_in_unrelated_html() {
        assert!(detect_releases("<p>no releases here</p>").is_empty());
    }

    #[test]
    fn parse_release_validates_shape_and_quarter() {
        assert_eq!(parse_release("rvu26a").unwrap().id(), "RVU26A");
        assert_eq!(parse_release("RVU26D").unwrap().quarter, 'D');
        assert_eq!(parse_release(" RVU25C ").unwrap().year, 2025);
        assert!(parse_release("RVU26E").is_none()); // quarter out of A–D
        assert!(parse_release("RVU2A").is_none()); // too short
        assert!(parse_release("nope").is_none());
    }

    #[test]
    fn urls_and_ordering_match_cms_conventions() {
        let b = parse_release("RVU26B").unwrap();
        assert_eq!(b.zip_url(), "https://www.cms.gov/files/zip/rvu26b.zip");
        assert!(b.source_url().ends_with("/rvu26b"));
        assert_eq!(b.quarter_num(), 2);

        let a = parse_release("RVU26A").unwrap();
        let y = parse_release("RVU25D").unwrap();
        assert!(b.ord_key() > a.ord_key()); // later quarter, same year
        assert!(a.ord_key() > y.ord_key()); // newer year beats later quarter
    }
}
