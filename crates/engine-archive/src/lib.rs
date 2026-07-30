//! Tier-zero archive engine (M18): serves stored **Wayback Machine** snapshots
//! instead of hitting the live site. v1 speaks the Wayback CDX API only
//! (Common Crawl is deferred).
//!
//! ## How it works
//!
//! For a GET, the engine:
//! 1. queries the CDX index for the *newest* capture of the URL
//!    (`<base>/cdx/search/cdx?url=<u>&limit=1&sort=reverse&filter=statuscode:200`),
//! 2. checks that capture against the caller's freshness window
//!    (`HttpRequest.archive_max_age`, seconds) — an older-only capture is a
//!    typed miss so the tiered fetcher falls through to the live ladder,
//! 3. fetches the **raw, unrewritten** body via `<base>/web/<ts>id_/<url>`
//!    (the `id_` flag suppresses the Wayback toolbar/link rewriting),
//! 4. marks the response with provenance headers so stored records say where
//!    the body really came from: [`FETCHED_VIA_HEADER`]` = "archive"` and
//!    [`SNAPSHOT_TS_HEADER`]` = <capture time, RFC 3339 UTC>`.
//!
//! Both outbound requests run through the **inner** [`HttpClient`] (the real
//! HTTP engine), so archive.org gets the same per-host politeness governor,
//! retries, body caps, charset-aware capped body reader, and TTL cache as any
//! other host — nothing here talks to the network directly.
//!
//! ## Backfill (historical range enumeration)
//!
//! [`ArchiveEngine::list_snapshots`] enumerates CDX captures of a URL across a
//! date range — digest-deduped, oldest first, capped with an honest
//! `truncated` flag. The extractor app's `source.archive` mode builds on it:
//! each `(timestamp, original)` pair feeds [`snapshot_url`] for the raw body,
//! runs the ruleset, and lands as a `{natural_key}@{snapshot_date}` record
//! tagged `_fetched_via: "wayback"` (the M42 backfill key convention).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use pumper_core::config::ArchiveConfig;
use pumper_core::{
    Error, HttpClient, HttpMethod, HttpRequest, HttpResponse, Result, FETCHED_VIA_HEADER,
    SNAPSHOT_TS_HEADER,
};
use tracing::debug;

/// Wayback capture timestamps are exactly 14 ASCII digits: `YYYYMMDDhhmmss`.
const CDX_TS_LEN: usize = 14;

/// One CDX index row (the fields this engine reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdxSnapshot {
    /// Capture timestamp, 14-digit `YYYYMMDDhhmmss` (UTC).
    pub timestamp: String,
    /// The captured URL as archived (may differ from the request URL in
    /// scheme/canonicalization — snapshot fetches must use this one).
    pub original: String,
    /// Content digest from the CDX row (field 6 of the default order), used to
    /// skip byte-identical re-captures during range enumeration. `None` when
    /// the row carried too few fields to include one.
    pub digest: Option<String>,
}

/// Result of a CDX **range enumeration** ([`ArchiveEngine::list_snapshots`]):
/// digest-deduped captures, oldest first, plus an honest truncation flag —
/// `truncated: true` means the index held more captures than `max` and the
/// list is an incomplete prefix of the range, never a silent cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotList {
    pub snapshots: Vec<CdxSnapshot>,
    pub truncated: bool,
}

/// Tier-zero archive engine: an [`HttpClient`] over the Wayback Machine.
/// Construct with the real HTTP engine as `inner`; wire into the tiered
/// fetcher via `Fetcher::with_archive`.
pub struct ArchiveEngine {
    /// `[archive] base_url`, trailing-slash-trimmed.
    base_url: String,
    /// The real transport (politeness governor, retries, caps, cache).
    inner: Arc<dyn HttpClient>,
}

impl ArchiveEngine {
    pub fn new(cfg: &ArchiveConfig, inner: Arc<dyn HttpClient>) -> Self {
        Self {
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            inner,
        }
    }

    /// This deployment's Wayback base URL (trailing-slash-trimmed) — the base
    /// callers feed to [`snapshot_url`] for captures returned by
    /// [`list_snapshots`](Self::list_snapshots).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// CDX **range enumeration** (the backfill seam): every 200-status capture
    /// of `url` between `from` and `to` (14-digit timestamps or any digit
    /// prefix; `None` = unbounded), oldest first, deduped by content digest,
    /// capped at `max` with an honest `truncated` flag (`limit = max + 1` is
    /// requested so a full window is distinguishable from an overfull one).
    /// The CDX request runs through the **inner** governed transport, so
    /// archive.org keeps its politeness guarantees. `url` may use Wayback
    /// wildcard/prefix syntax (e.g. `example.com/products/*`).
    pub async fn list_snapshots(
        &self,
        url: &str,
        from: Option<&str>,
        to: Option<&str>,
        max: usize,
    ) -> Result<SnapshotList> {
        for (name, bound) in [("from", from), ("to", to)] {
            if let Some(b) = bound {
                if !valid_cdx_bound(b) {
                    return Err(Error::Http(format!(
                        "bad archive '{name}' bound '{b}': want 4-14 digits (YYYY[MMDDhhmmss])"
                    )));
                }
            }
        }
        let max = max.max(1);
        let req = HttpRequest::get(cdx_range_query_url(
            &self.base_url,
            url,
            from,
            to,
            max + 1,
        ));
        let resp = self.inner.fetch(req).await?;
        if !resp.is_success() {
            return Err(Error::Http(format!(
                "archive CDX range query for {url} failed: status {}",
                resp.status
            )));
        }
        let list = select_snapshots(parse_cdx_lines(&resp.body), max);
        debug!(
            url,
            snapshots = list.snapshots.len(),
            truncated = list.truncated,
            "enumerated archive captures"
        );
        Ok(list)
    }
}

/// The CDX query for the newest 200-status capture of `target`.
/// `to` (14-digit or any prefix, e.g. `2019`) bounds the capture time from
/// above — unused by the freshness path (which wants the newest capture and
/// window-checks it locally) but the seam the future backfill job builds on.
pub fn cdx_query_url(base_url: &str, target: &str, to: Option<&str>) -> String {
    let mut url = format!(
        "{}/cdx/search/cdx?url={}&limit=1&sort=reverse&filter=statuscode:200",
        base_url,
        urlencode(target)
    );
    if let Some(to) = to {
        url.push_str("&to=");
        url.push_str(&urlencode(to));
    }
    url
}

/// The CDX query for a **range enumeration** of `target`'s captures: ascending
/// (oldest first), 200-status only, server-side `collapse=digest` to thin
/// adjacent identical re-captures (client-side dedup still runs — collapse is
/// adjacency-only). `from`/`to` are 14-digit timestamps or any prefix
/// (e.g. `2019`); `limit` bounds the row count.
pub fn cdx_range_query_url(
    base_url: &str,
    target: &str,
    from: Option<&str>,
    to: Option<&str>,
    limit: usize,
) -> String {
    let mut url = format!(
        "{}/cdx/search/cdx?url={}&filter=statuscode:200&collapse=digest&limit={}",
        base_url,
        urlencode(target),
        limit
    );
    if let Some(from) = from {
        url.push_str("&from=");
        url.push_str(&urlencode(from));
    }
    if let Some(to) = to {
        url.push_str("&to=");
        url.push_str(&urlencode(to));
    }
    url
}

/// Whether `s` is a valid CDX time bound: 4–14 ASCII digits (a full
/// `YYYYMMDDhhmmss` timestamp or any prefix, e.g. `2019` or `201906`).
pub fn valid_cdx_bound(s: &str) -> bool {
    (4..=CDX_TS_LEN).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit())
}

/// Raw snapshot-body URL: the `id_` flag asks Wayback for the archived bytes
/// with no toolbar injection or link rewriting.
pub fn snapshot_url(base_url: &str, timestamp: &str, original: &str) -> String {
    format!("{base_url}/web/{timestamp}id_/{original}")
}

/// Percent-encodes a value for a query-string position.
fn urlencode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// Parses one plaintext CDX data line. The default CDX field order is
/// `urlkey timestamp original mimetype statuscode digest length`; a malformed
/// line (too few fields, bad timestamp) yields `None`.
pub fn parse_cdx_line(line: &str) -> Option<CdxSnapshot> {
    let mut fields = line.split_whitespace();
    let _urlkey = fields.next()?;
    let timestamp = fields.next()?;
    let original = fields.next()?;
    let _mimetype = fields.next();
    let _statuscode = fields.next();
    let digest = fields.next().map(str::to_string);
    if timestamp.len() != CDX_TS_LEN || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(CdxSnapshot {
        timestamp: timestamp.to_string(),
        original: original.to_string(),
        digest,
    })
}

/// Parses the first data line of a plaintext CDX response; an empty body (no
/// captures) or a malformed first line yields `None`.
pub fn parse_cdx_first_line(body: &str) -> Option<CdxSnapshot> {
    parse_cdx_line(body.lines().find(|l| !l.trim().is_empty())?)
}

/// Parses every well-formed data line of a plaintext CDX response, in order.
/// Malformed lines are skipped, matching [`parse_cdx_first_line`]'s tolerance.
pub fn parse_cdx_lines(body: &str) -> Vec<CdxSnapshot> {
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(parse_cdx_line)
        .collect()
}

/// Range-enumeration post-processing over rows fetched with `limit = max + 1`:
/// dedup by content digest (first — i.e. oldest — capture of each digest wins;
/// digest-less rows are kept as unique), cap the result at `max`, and flag
/// truncation honestly: `truncated` is true whenever the raw row count exceeded
/// `max` — the index held more captures than the caller allowed, even if
/// dedup shrank the returned list below the cap.
pub fn select_snapshots(rows: Vec<CdxSnapshot>, max: usize) -> SnapshotList {
    let truncated = rows.len() > max;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut snapshots: Vec<CdxSnapshot> = Vec::new();
    for row in rows {
        if let Some(d) = &row.digest {
            if !seen.insert(d.clone()) {
                continue;
            }
        }
        snapshots.push(row);
        if snapshots.len() == max {
            break;
        }
    }
    SnapshotList {
        snapshots,
        truncated,
    }
}

/// A 14-digit CDX capture timestamp as a UTC datetime.
pub fn snapshot_datetime(timestamp: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(timestamp, "%Y%m%d%H%M%S")
        .ok()
        .map(|naive| naive.and_utc())
}

/// Whether a capture taken at `captured` satisfies a freshness window of
/// `max_age_secs` seconds as of `now`. `None` = no window = any age serves
/// (a raw-engine caller that just wants "whatever the archive has").
pub fn within_window(
    captured: DateTime<Utc>,
    now: DateTime<Utc>,
    max_age_secs: Option<u64>,
) -> bool {
    let Some(max_age) = max_age_secs else {
        return true;
    };
    let age = now.signed_duration_since(captured).num_seconds();
    // A capture "from the future" (clock skew) is trivially fresh.
    age <= max_age.min(i64::MAX as u64) as i64
}

#[async_trait]
impl HttpClient for ArchiveEngine {
    /// Serves `req.url` from the newest archive capture inside
    /// `req.archive_max_age` (or the newest at all when `None`). Every miss is
    /// a typed [`Error::Http`] — the tiered fetcher treats any error here as
    /// "fall through to the live ladder".
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        if req.method != HttpMethod::Get || req.body.is_some() {
            return Err(Error::Http(
                "archive engine serves only bodyless GETs".into(),
            ));
        }

        // 1) Newest capture from the CDX index. The caller's cache preferences
        // carry over: a `no_cache` monitor-style fetch re-checks the index.
        let mut cdx_req = HttpRequest::get(cdx_query_url(&self.base_url, &req.url, None));
        cdx_req.no_cache = req.no_cache;
        cdx_req.ttl_override = req.ttl_override;
        cdx_req.timeout_secs = req.timeout_secs;
        let cdx_resp = self.inner.fetch(cdx_req).await?;
        if !cdx_resp.is_success() {
            return Err(Error::Http(format!(
                "archive CDX query for {} failed: status {}",
                req.url, cdx_resp.status
            )));
        }
        let snap = parse_cdx_first_line(&cdx_resp.body).ok_or_else(|| {
            Error::Http(format!("no archive snapshot recorded for {}", req.url))
        })?;
        let captured = snapshot_datetime(&snap.timestamp).ok_or_else(|| {
            Error::Http(format!(
                "unparseable archive capture timestamp '{}' for {}",
                snap.timestamp, req.url
            ))
        })?;

        // 2) Freshness window.
        if !within_window(captured, Utc::now(), req.archive_max_age) {
            return Err(Error::Http(format!(
                "newest archive snapshot of {} was captured {} — outside the {}s freshness window",
                req.url,
                captured.to_rfc3339(),
                req.archive_max_age.unwrap_or(0)
            )));
        }

        // 3) Raw body, through the same governed transport.
        let mut body_req = HttpRequest::get(snapshot_url(
            &self.base_url,
            &snap.timestamp,
            &snap.original,
        ));
        body_req.no_cache = req.no_cache;
        body_req.ttl_override = req.ttl_override;
        body_req.max_body_bytes = req.max_body_bytes;
        body_req.timeout_secs = req.timeout_secs;
        body_req.headers = req.headers.clone();
        let mut resp = self.inner.fetch(body_req).await?;
        debug!(
            url = %req.url,
            captured = %captured.to_rfc3339(),
            status = resp.status,
            "served from web archive"
        );

        // 4) Provenance: explicit in the header map, which is what stored
        // records keep. `final_url` stays the snapshot URL — honest about
        // where these bytes physically came from.
        resp.headers
            .insert(FETCHED_VIA_HEADER.to_string(), "archive".to_string());
        resp.headers
            .insert(SNAPSHOT_TS_HEADER.to_string(), captured.to_rfc3339());
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::TimeZone;

    use super::*;

    // --- pure helpers ---

    #[test]
    fn cdx_query_url_encodes_and_pins_the_contract() {
        let u = cdx_query_url(
            "https://web.archive.org",
            "https://example.com/a b?x=1&y=2",
            None,
        );
        assert!(u.starts_with("https://web.archive.org/cdx/search/cdx?url="));
        assert!(u.contains("limit=1"));
        assert!(u.contains("sort=reverse"));
        assert!(u.contains("filter=statuscode%3A200") || u.contains("filter=statuscode:200"));
        // The target's own query separators are escaped, not spliced.
        assert!(!u.contains("y=2&"), "target query must be percent-encoded");
        assert!(u.contains("%26y%3D2"));
        assert!(u.to_lowercase().contains("a+b") || u.contains("a%20b"));
    }

    #[test]
    fn cdx_query_url_appends_to_bound_when_asked() {
        let u = cdx_query_url("https://web.archive.org", "https://example.com/", Some("2019"));
        assert!(u.ends_with("&to=2019"));
        let u = cdx_query_url("https://web.archive.org", "https://example.com/", None);
        assert!(!u.contains("&to="));
    }

    #[test]
    fn snapshot_url_uses_the_raw_id_flag() {
        assert_eq!(
            snapshot_url(
                "https://web.archive.org",
                "20240102030405",
                "https://example.com/page"
            ),
            "https://web.archive.org/web/20240102030405id_/https://example.com/page"
        );
    }

    #[test]
    fn cdx_first_line_parses_the_default_field_order() {
        let body = "com,example)/ 20240102030405 https://example.com/ text/html 200 ABCDEF 1234\n\
                    com,example)/ 20230101000000 http://example.com/ text/html 200 ABCDEF 999\n";
        let snap = parse_cdx_first_line(body).unwrap();
        assert_eq!(snap.timestamp, "20240102030405");
        assert_eq!(snap.original, "https://example.com/");
    }

    #[test]
    fn cdx_empty_or_malformed_is_a_miss() {
        assert_eq!(parse_cdx_first_line(""), None);
        assert_eq!(parse_cdx_first_line("\n  \n"), None);
        // Too few fields.
        assert_eq!(parse_cdx_first_line("com,example)/ 20240102030405"), None);
        // A non-numeric or wrong-length timestamp is rejected, not served.
        assert_eq!(
            parse_cdx_first_line("com,example)/ 2024010203 https://example.com/"),
            None
        );
        assert_eq!(
            parse_cdx_first_line("com,example)/ 2024010203040X https://example.com/"),
            None
        );
    }

    #[test]
    fn cdx_range_query_url_pins_the_range_contract() {
        let u = cdx_range_query_url(
            "https://web.archive.org",
            "https://example.com/",
            Some("2019"),
            Some("20200630"),
            11,
        );
        assert!(u.starts_with("https://web.archive.org/cdx/search/cdx?url="));
        // Ascending enumeration: no limit=1, no sort=reverse.
        assert!(!u.contains("limit=1&"));
        assert!(!u.contains("sort=reverse"));
        assert!(u.contains("limit=11"));
        assert!(u.contains("collapse=digest"));
        assert!(u.contains("filter=statuscode%3A200") || u.contains("filter=statuscode:200"));
        assert!(u.contains("&from=2019"));
        assert!(u.contains("&to=20200630"));
        // Bounds are optional independently.
        let u = cdx_range_query_url("https://web.archive.org", "https://example.com/", None, None, 5);
        assert!(!u.contains("&from=") && !u.contains("&to="));
    }

    #[test]
    fn cdx_bounds_validate_digit_prefixes() {
        assert!(valid_cdx_bound("2019"));
        assert!(valid_cdx_bound("201906"));
        assert!(valid_cdx_bound("20190601123045"));
        assert!(!valid_cdx_bound("201")); // too short
        assert!(!valid_cdx_bound("201906011230456")); // too long
        assert!(!valid_cdx_bound("2019-06")); // non-digit
        assert!(!valid_cdx_bound(""));
    }

    #[test]
    fn cdx_lines_parse_in_order_and_skip_malformed() {
        let body = "com,example)/ 20190101000000 https://example.com/ text/html 200 AAA 10\n\
                    garbage-line\n\
                    com,example)/ 20200101000000 https://example.com/ text/html 200 BBB 11\n";
        let rows = parse_cdx_lines(body);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, "20190101000000");
        assert_eq!(rows[0].digest.as_deref(), Some("AAA"));
        assert_eq!(rows[1].digest.as_deref(), Some("BBB"));
        // A digest-less (short but valid) row parses with digest: None.
        let rows = parse_cdx_lines("com,example)/ 20190101000000 https://example.com/\n");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].digest, None);
    }

    fn snap(ts: &str, digest: Option<&str>) -> CdxSnapshot {
        CdxSnapshot {
            timestamp: ts.into(),
            original: "https://example.com/".into(),
            digest: digest.map(str::to_string),
        }
    }

    #[test]
    fn select_snapshots_dedups_by_digest_keeping_oldest() {
        let rows = vec![
            snap("20190101000000", Some("AAA")),
            snap("20190201000000", Some("AAA")), // re-capture, dropped
            snap("20190301000000", Some("BBB")),
            snap("20190401000000", None), // digest-less rows are unique
            snap("20190501000000", None),
        ];
        let list = select_snapshots(rows, 10);
        assert!(!list.truncated);
        let ts: Vec<&str> = list.snapshots.iter().map(|s| s.timestamp.as_str()).collect();
        assert_eq!(
            ts,
            ["20190101000000", "20190301000000", "20190401000000", "20190501000000"]
        );
    }

    #[test]
    fn select_snapshots_truncation_is_honest() {
        // Fetched with limit = max + 1: an overfull window flags truncation…
        let rows: Vec<CdxSnapshot> = (0..4)
            .map(|i| snap(&format!("2019010100000{i}"), Some(&format!("D{i}"))))
            .collect();
        let list = select_snapshots(rows.clone(), 3);
        assert!(list.truncated);
        assert_eq!(list.snapshots.len(), 3);
        // …even when dedup shrinks the result below the cap — the index still
        // held more rows than the caller allowed to be fetched.
        let dup: Vec<CdxSnapshot> = (0..4).map(|i| snap(&format!("2019010100000{i}"), Some("SAME"))).collect();
        let list = select_snapshots(dup, 3);
        assert!(list.truncated);
        assert_eq!(list.snapshots.len(), 1);
        // An exactly-full window is complete, not truncated.
        let list = select_snapshots(rows[..3].to_vec(), 3);
        assert!(!list.truncated);
        assert_eq!(list.snapshots.len(), 3);
    }

    #[tokio::test]
    async fn list_snapshots_enumerates_through_the_governed_inner() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: "com,example)/ 20190101000000 https://example.com/ text/html 200 AAA 1\n\
                       com,example)/ 20190201000000 https://example.com/ text/html 200 AAA 2\n\
                       com,example)/ 20200101000000 https://example.com/ text/html 200 BBB 3\n"
                .into(),
            page_body: "unused".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let list = engine
            .list_snapshots("https://example.com/", Some("2019"), Some("2020"), 10)
            .await
            .unwrap();
        assert_eq!(list.snapshots.len(), 2, "digest-deduped");
        assert!(!list.truncated);
        let seen = inner.seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "exactly one CDX request, via the inner transport");
        assert!(seen[0].contains("&from=2019") && seen[0].contains("&to=2020"));
        assert!(seen[0].contains("limit=11"), "requests max + 1 rows");
    }

    #[tokio::test]
    async fn list_snapshots_rejects_bad_bounds_without_fetching() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: String::new(),
            page_body: String::new(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let err = engine
            .list_snapshots("https://example.com/", Some("last-year"), None, 10)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("bad archive 'from' bound"), "{err}");
        assert!(inner.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn snapshot_datetime_parses_utc() {
        let dt = snapshot_datetime("20240102030405").unwrap();
        assert_eq!(dt, Utc.with_ymd_and_hms(2024, 1, 2, 3, 4, 5).unwrap());
        assert!(snapshot_datetime("not-a-ts").is_none());
        assert!(snapshot_datetime("20241399000000").is_none(), "month 13");
    }

    #[test]
    fn window_logic_gates_on_age() {
        let now = Utc.with_ymd_and_hms(2026, 7, 30, 12, 0, 0).unwrap();
        let hour_old = now - chrono::Duration::hours(1);
        // Inside the window serves; outside falls through.
        assert!(within_window(hour_old, now, Some(7200)));
        assert!(!within_window(hour_old, now, Some(600)));
        // Exactly at the boundary is still fresh.
        assert!(within_window(hour_old, now, Some(3600)));
        // No window = any age.
        assert!(within_window(now - chrono::Duration::days(3650), now, None));
        // Future capture (clock skew) never falls through.
        assert!(within_window(now + chrono::Duration::minutes(5), now, Some(60)));
    }

    // --- engine behavior over a scripted inner client ---

    /// Inner stub: serves a canned CDX body for `/cdx/` URLs and a canned page
    /// body for snapshot URLs; records every URL it was asked for.
    struct ScriptedInner {
        cdx_body: String,
        page_body: String,
        seen: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HttpClient for ScriptedInner {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            self.seen.lock().unwrap().push(req.url.clone());
            let body = if req.url.contains("/cdx/") {
                self.cdx_body.clone()
            } else {
                self.page_body.clone()
            };
            Ok(HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn engine_over(inner: ScriptedInner) -> (ArchiveEngine, Arc<ScriptedInner>) {
        let inner = Arc::new(inner);
        let cfg = ArchiveConfig {
            enabled: true,
            base_url: "https://web.archive.org".into(),
        };
        (ArchiveEngine::new(&cfg, inner.clone()), inner)
    }

    /// A CDX line whose capture time is `now`, so any window accepts it.
    fn fresh_cdx_line() -> String {
        let ts = Utc::now().format("%Y%m%d%H%M%S");
        format!("com,example)/ {ts} https://example.com/ text/html 200 DIGEST 100\n")
    }

    #[tokio::test]
    async fn hit_serves_body_with_provenance_headers() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: fresh_cdx_line(),
            page_body: "<html>archived body</html>".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let mut req = HttpRequest::get("https://example.com/");
        req.archive_max_age = Some(3600);
        let resp = engine.fetch(req).await.unwrap();
        assert_eq!(resp.body, "<html>archived body</html>");
        assert_eq!(
            resp.headers.get(FETCHED_VIA_HEADER).map(String::as_str),
            Some("archive")
        );
        let ts = resp.headers.get(SNAPSHOT_TS_HEADER).expect("snapshot ts set");
        chrono::DateTime::parse_from_rfc3339(ts).expect("RFC 3339 snapshot ts");
        // Exactly two inner requests: CDX index, then the raw id_ snapshot.
        let seen = inner.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].contains("/cdx/search/cdx?url="));
        assert!(seen[1].contains("id_/https://example.com/"));
    }

    #[tokio::test]
    async fn stale_snapshot_is_a_typed_miss() {
        let (engine, inner) = engine_over(ScriptedInner {
            // A 2019 capture against a 1-hour window.
            cdx_body: "com,example)/ 20190601120000 https://example.com/ text/html 200 D 1\n"
                .into(),
            page_body: "never served".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let mut req = HttpRequest::get("https://example.com/");
        req.archive_max_age = Some(3600);
        let err = engine.fetch(req).await.unwrap_err();
        assert!(matches!(err, Error::Http(_)));
        assert!(err.to_string().contains("freshness window"), "{err}");
        // The snapshot body was never fetched — only the index was consulted.
        assert_eq!(inner.seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn no_capture_at_all_is_a_typed_miss() {
        let (engine, _) = engine_over(ScriptedInner {
            cdx_body: String::new(),
            page_body: "never served".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let mut req = HttpRequest::get("https://example.com/");
        req.archive_max_age = Some(3600);
        let err = engine.fetch(req).await.unwrap_err();
        assert!(err.to_string().contains("no archive snapshot"), "{err}");
    }

    #[tokio::test]
    async fn post_requests_are_refused() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: fresh_cdx_line(),
            page_body: "x".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let mut req = HttpRequest::get("https://example.com/");
        req.method = HttpMethod::Post;
        assert!(engine.fetch(req).await.is_err());
        assert!(inner.seen.lock().unwrap().is_empty(), "nothing was fetched");
    }

    #[tokio::test]
    async fn no_window_serves_the_newest_capture_regardless_of_age() {
        let (engine, _) = engine_over(ScriptedInner {
            cdx_body: "com,example)/ 20150601120000 https://example.com/ text/html 200 D 1\n"
                .into(),
            page_body: "decade-old body".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        // archive_max_age: None — a raw-engine caller taking whatever exists.
        let resp = engine.fetch(HttpRequest::get("https://example.com/")).await.unwrap();
        assert_eq!(resp.body, "decade-old body");
        assert_eq!(
            resp.headers.get(SNAPSHOT_TS_HEADER).map(String::as_str),
            Some("2015-06-01T12:00:00+00:00")
        );
    }

    // --- live smoke test (network) ---

    /// Minimal reqwest-backed transport for the live test only. The real
    /// deployment always uses the governed HTTP engine as `inner`.
    struct PlainClient(reqwest::Client);

    #[async_trait]
    impl HttpClient for PlainClient {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            let resp = self
                .0
                .get(&req.url)
                .send()
                .await
                .map_err(|e| Error::Http(e.to_string()))?;
            let status = resp.status().as_u16();
            let final_url = resp.url().to_string();
            let body = resp.text().await.map_err(|e| Error::Http(e.to_string()))?;
            Ok(HttpResponse {
                status,
                headers: HashMap::new(),
                body,
                final_url,
                cache_hit: false,
            })
        }
    }

    /// Hits the real Wayback CDX + snapshot endpoints. Run explicitly with:
    /// `cargo test -p pumper-engine-archive -- --ignored live_wayback`
    #[tokio::test]
    #[ignore = "network: hits web.archive.org"]
    async fn live_wayback_snapshot_roundtrip() {
        let cfg = ArchiveConfig {
            enabled: true,
            base_url: "https://web.archive.org".into(),
        };
        let engine = ArchiveEngine::new(&cfg, Arc::new(PlainClient(reqwest::Client::new())));
        // example.com is captured constantly; a 10-year window can't flake.
        let mut req = HttpRequest::get("https://example.com/");
        req.archive_max_age = Some(10 * 365 * 24 * 3600);
        let resp = engine.fetch(req).await.expect("live archive fetch");
        assert!(resp.is_success());
        assert!(!resp.body.is_empty());
        assert_eq!(
            resp.headers.get(FETCHED_VIA_HEADER).map(String::as_str),
            Some("archive")
        );
        assert!(resp.headers.contains_key(SNAPSHOT_TS_HEADER));
    }

    /// Hits the real Wayback CDX range endpoint. Run explicitly with:
    /// `cargo test -p pumper-engine-archive -- --ignored live_wayback`
    #[tokio::test]
    #[ignore = "network: hits web.archive.org"]
    async fn live_wayback_list_snapshots_range() {
        let cfg = ArchiveConfig {
            enabled: true,
            base_url: "https://web.archive.org".into(),
        };
        let engine = ArchiveEngine::new(&cfg, Arc::new(PlainClient(reqwest::Client::new())));
        let list = engine
            .list_snapshots("https://example.com/", Some("2020"), Some("2021"), 5)
            .await
            .expect("live CDX range enumeration");
        assert!(!list.snapshots.is_empty());
        assert!(list.snapshots.len() <= 5);
        // example.com is captured near-daily; a 2-year window overflows max=5.
        assert!(list.truncated);
        for s in &list.snapshots {
            assert!(s.timestamp.starts_with("2020") || s.timestamp.starts_with("2021"));
        }
    }
}
