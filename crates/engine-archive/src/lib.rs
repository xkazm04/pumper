//! Tier-zero archive engine (M18): serves stored **Wayback Machine** snapshots
//! instead of hitting the live site. v1 speaks the Wayback CDX API only
//! (Common Crawl is deferred).
//!
//! ## How it works
//!
//! For a GET, the engine:
//! 1. queries the CDX index for the *newest* capture of the URL
//!    (`<base>/cdx/search/cdx?url=<u>&limit=1&sort=reverse&filter=statuscode:200`).
//!    LIVE-VERIFIED 2026-07-30: the CDX API returns **400 for requests without a
//!    User-Agent header** — any client fronting this engine must set one (the
//!    production HttpEngine always does via `[http] user_agent`),
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
        // An inverted window is refused rather than queried: CDX answers it with
        // an empty body, which is the same answer as "never archived here" (see
        // `inverted_cdx_range`).
        if inverted_cdx_range(from, to) {
            return Err(Error::Http(format!(
                "archive range for {url} is empty by construction: 'from' {} is after 'to' {} \
                 — narrow the window, do not invert it",
                from.unwrap_or_default(),
                to.unwrap_or_default()
            )));
        }
        let max = max.max(1);
        let req = HttpRequest::get(cdx_range_query_url(&self.base_url, url, from, to, max + 1));
        let resp = self.inner.fetch(req).await?;
        if !resp.is_success() {
            return Err(cdx_failure("range query", url, resp.status));
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
///
/// `to` (14-digit or any prefix, e.g. `2019`) bounds the capture time from
/// above: "the newest capture no later than T". The freshness path passes
/// `None` — it wants the newest capture outright and window-checks it locally,
/// which is one query instead of two and the reason the bound is unused there.
///
/// **It has no caller today, and the doc used to explain that by pointing at
/// "the future backfill job".** That job shipped: `list_snapshots` +
/// [`cdx_range_query_url`] enumerate a range, which is a different query
/// (ascending, `collapse=digest`, `limit = max + 1`) and does not go through
/// here. The bound is kept because point-in-time retrieval — one capture as of
/// a date, not every capture in a window — is a real and distinct shape that
/// range enumeration answers expensively. Naming the plan that superseded it is
/// the point: a parameter documented by a promise nobody kept reads as live.
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

/// Why the newest-capture lookup produced nothing, told apart by what the CDX
/// index actually sent back.
///
/// THE ANTI-PATTERN THIS CLOSES: one sentence, "no archive snapshot recorded
/// for <url>", for two different facts. `parse_cdx_first_line` returns `None`
/// both for an empty body (the archive genuinely holds no 200-status capture)
/// and for a body whose every line failed to parse (the archive answered and
/// this engine could not read it — a changed field order, an HTML error page
/// served with a 200, a truncated response). The first is a fact about the
/// world and the correct answer is "fall through to the live ladder and stop
/// asking". The second is a fact about this parser, and reporting it as the
/// first sends an operator to check a URL's archive coverage when what broke
/// is here. `copy-auditor`'s misdirection class, in a tier-zero miss that
/// nothing else in the stack will ever contradict.
///
/// Both remain typed [`Error::Http`] misses, because both must still fall
/// through — the tiered fetcher's behaviour is unchanged and only the sentence
/// differs.
fn no_snapshot_reason(body: &str, url: &str) -> Error {
    if body.trim().is_empty() {
        return Error::Http(format!("no archive snapshot recorded for {url}"));
    }
    let lines = body.lines().filter(|l| !l.trim().is_empty()).count();
    Error::Http(format!(
        "unreadable archive CDX index for {url}: {lines} row(s) came back and none parsed as \
         `urlkey timestamp original ...` with a 14-digit timestamp — the archive answered, this \
         engine could not read it"
    ))
}

/// The error one failed CDX request raises — the same sentence for both CDX
/// call sites, carrying the cause when the status has a known one.
///
/// THE ANTI-PATTERN THIS CLOSES: `failed: status 400`, and nothing else. This
/// module's own header records a live-verified fact — *the CDX API returns 400
/// for requests without a User-Agent header* — and that fact sat four hundred
/// lines away from the only place an operator ever meets the 400. The
/// production `HttpEngine` always sets one from `[http] user_agent`, so the
/// person who hits this is by definition the one who wired a different inner
/// client, which is exactly the reader who cannot know why.
///
/// Only the 400 carries a cause. Every other status gets the bare status
/// deliberately: this engine is tier zero and any error here means "fall
/// through to the live ladder", so guessing at causes it has not verified
/// would trade one unhelpful message for a misleading one.
fn cdx_failure(what: &str, url: &str, status: u16) -> Error {
    let cause = if status == 400 {
        " — the CDX API answers 400 to a request with no User-Agent header, \
         so the inner HttpClient must set one (the production HTTP engine \
         does, via `[http] user_agent`)"
    } else {
        ""
    };
    Error::Http(format!(
        "archive CDX {what} for {url} failed: status {status}{cause}"
    ))
}

/// Widens a digit-prefix bound to a full 14-digit timestamp at the extreme it
/// stands for: `filler` is `b'0'` for a lower bound (the earliest instant the
/// prefix admits) and `b'9'` for an upper one (the latest). The padded upper
/// bound is not a real datetime — it does not need to be, because two 14-digit
/// numeric strings order the same way the instants they denote do.
fn widen_cdx_bound(bound: &str, filler: u8) -> String {
    let mut s = String::with_capacity(CDX_TS_LEN);
    s.push_str(bound);
    while s.len() < CDX_TS_LEN {
        s.push(filler as char);
    }
    s
}

/// Whether a `from`/`to` pair names a window that is **empty by construction** —
/// `from` strictly after `to`.
///
/// THE ANTI-PATTERN THIS CLOSES: both bounds pass [`valid_cdx_bound`]
/// individually, nothing compared them, and CDX answers an inverted range with
/// an empty body. [`ArchiveEngine::list_snapshots`] then returned
/// `SnapshotList { snapshots: [], truncated: false }` — byte-identical to the
/// answer for a URL that was genuinely never archived in that window. The
/// caller cannot tell a typo from a fact.
///
/// It is not a hypothetical typo, either: the documented way to resume a
/// truncated enumeration (`extractor/src/lib.rs:31`) is to *narrow the
/// `from`/`to` window*, so the workflow that produces these pairs by hand is
/// exactly the workflow this engine ships for.
///
/// Bounds are prefixes of different lengths (`2019` against `20200630`), so
/// each is widened to the extreme it denotes before comparing: `from=2020,
/// to=2019` becomes `20200000000000 > 20199999999999` and is refused, while
/// `from=2019, to=2019` widens to a whole year and is not.
fn inverted_cdx_range(from: Option<&str>, to: Option<&str>) -> bool {
    let (Some(from), Some(to)) = (from, to) else {
        return false;
    };
    widen_cdx_bound(from, b'0') > widen_cdx_bound(to, b'9')
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
            return Err(cdx_failure("query", &req.url, cdx_resp.status));
        }
        let snap = parse_cdx_first_line(&cdx_resp.body)
            .ok_or_else(|| no_snapshot_reason(&cdx_resp.body, &req.url))?;
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

    /// **Deliberately unsupported, and it says so as itself.**
    ///
    /// A snapshot body *is* reachable as bytes (`/web/<ts>id_/<url>` serves the
    /// raw capture), but a binary archive fetch has semantics nobody has
    /// specified: which capture a caller means when they ask for "the bytes" and
    /// what a freshness window means for an artifact that is immutable by
    /// definition. Inventing that here would be worse than refusing.
    ///
    /// The point of overriding rather than inheriting is the *message*. The
    /// trait's default says "this engine does not support binary fetch_bytes",
    /// which is indistinguishable from a mock, a decorator that forgot to
    /// forward, or a genuine engine gap — so a capability hole in a wrapper and a
    /// deliberate refusal read identically. This one names the archive and the
    /// reason, and points at the surface that *does* enumerate captures.
    async fn fetch_bytes(&self, req: HttpRequest) -> Result<Vec<u8>> {
        Err(Error::Http(format!(
            "the archive engine deliberately does not serve binary bodies ({}): \
             a snapshot is a point in time, and which capture a byte fetch means \
             is unspecified. Enumerate captures with `ArchiveEngine::list_snapshots` \
             and fetch a chosen `snapshot_url` through the HTTP engine instead.",
            req.url
        )))
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
        let u = cdx_query_url(
            "https://web.archive.org",
            "https://example.com/",
            Some("2019"),
        );
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
        let u = cdx_range_query_url(
            "https://web.archive.org",
            "https://example.com/",
            None,
            None,
            5,
        );
        assert!(!u.contains("&from=") && !u.contains("&to="));
    }

    /// THE ANTI-PATTERN: two bounds each valid on their own, never compared to
    /// each other. CDX answers an inverted range with an empty body, so
    /// `list_snapshots` returned an empty, untruncated list — the exact same
    /// value it returns for a URL genuinely never archived in that window. The
    /// documented way to resume a truncated enumeration is to narrow `from`/`to`
    /// by hand, so this is the mistake the shipped workflow invites.
    #[test]
    fn an_inverted_range_is_not_the_same_as_no_captures() {
        assert!(inverted_cdx_range(Some("2020"), Some("2019")));
        assert!(inverted_cdx_range(
            Some("20200101000000"),
            Some("20190101000000")
        ));
        // Prefixes of different lengths widen to the extreme each denotes.
        assert!(
            inverted_cdx_range(Some("20200701"), Some("2019")),
            "a July 2020 lower bound is after all of 2019"
        );
        assert!(
            !inverted_cdx_range(Some("2019"), Some("20190101")),
            "a whole-year lower bound starts before 1 Jan of that year ends"
        );
        assert!(
            !inverted_cdx_range(Some("2019"), Some("2019")),
            "one year to itself is a real window, not an inversion"
        );
        assert!(!inverted_cdx_range(Some("2019"), Some("2020")));
        // An open-ended window cannot be inverted.
        assert!(!inverted_cdx_range(Some("2020"), None));
        assert!(!inverted_cdx_range(None, Some("2019")));
        assert!(!inverted_cdx_range(None, None));
    }

    /// THE ANTI-PATTERN: `failed: status 400` with the cause documented four
    /// hundred lines away in the module header. The 400 has exactly one known
    /// trigger (a CDX request with no User-Agent), and the only operator who
    /// can hit it is one who wired a non-production inner client — the reader
    /// least able to guess. Both CDX call sites now raise the one sentence.
    #[test]
    fn a_cdx_400_names_the_missing_user_agent_instead_of_only_its_status() {
        let msg = cdx_failure("query", "https://example.com/", 400).to_string();
        assert!(msg.contains("status 400"), "{msg}");
        assert!(
            msg.contains("User-Agent"),
            "the known cause is named: {msg}"
        );
        assert!(msg.contains("user_agent"), "and the setting that fixes it");

        // Every other status stays bare rather than guessing at a cause this
        // engine has not verified.
        for status in [403u16, 429, 500, 503] {
            let msg = cdx_failure("range query", "https://example.com/", status).to_string();
            assert!(msg.contains(&format!("status {status}")), "{msg}");
            assert!(
                !msg.contains("User-Agent"),
                "a {status} must not be blamed on the header: {msg}"
            );
        }

        // One sentence, two call sites - only the noun differs. (`Error::Http`
        // adds its own Display prefix, so this matches the body, not the head.)
        assert!(cdx_failure("query", "https://a/", 500)
            .to_string()
            .contains("archive CDX query for https://a/ failed: status 500"));
        assert!(cdx_failure("range query", "https://a/", 500)
            .to_string()
            .contains("archive CDX range query for https://a/ failed: status 500"));
    }

    /// THE ANTI-PATTERN: one sentence for two facts. "No archive snapshot
    /// recorded for X" was raised both when the index was empty (true, and the
    /// operator should stop asking) and when the index answered with rows this
    /// parser could not read (false, and the operator should look here). The
    /// second sent people to check a URL's archive coverage over a bug in the
    /// field-order assumption.
    #[test]
    fn an_unreadable_index_is_not_reported_as_an_unarchived_url() {
        let absent = no_snapshot_reason("", "https://example.com/").to_string();
        assert!(absent.contains("no archive snapshot recorded"), "{absent}");
        assert!(
            no_snapshot_reason("\n   \n", "https://example.com/")
                .to_string()
                .contains("no archive snapshot recorded"),
            "a whitespace-only body is still an empty index"
        );

        // Rows came back and none parsed: a changed field order, an HTML error
        // page served with a 200, a truncated response.
        let garbage = no_snapshot_reason(
            "<html><body>Server Error</body></html>\n",
            "https://example.com/",
        )
        .to_string();
        assert!(
            garbage.contains("unreadable archive CDX index"),
            "{garbage}"
        );
        assert!(
            !garbage.contains("no archive snapshot recorded"),
            "the two facts must not share a sentence: {garbage}"
        );
        assert!(garbage.contains("1 row"), "the count is shown: {garbage}");

        // A body of well-formed rows never reaches here (parse succeeds), so
        // the only multi-row case is the unreadable one - and it counts them.
        let two = no_snapshot_reason("bad one\nbad two\n", "https://example.com/").to_string();
        assert!(two.contains("2 row"), "{two}");
    }

    /// The same distinction, driven through the engine so the miss path is
    /// proved to REACH it rather than merely to have it available.
    #[tokio::test]
    async fn both_empty_and_unreadable_indexes_miss_but_say_which() {
        let (engine, _) = engine_over(ScriptedInner {
            cdx_body: String::new(),
            page_body: "never served".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let err = engine
            .fetch(HttpRequest::get("https://example.com/"))
            .await
            .expect_err("an empty index is a miss");
        assert!(err.to_string().contains("no archive snapshot"), "{err}");

        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: "<html>rate limited</html>\n".into(),
            page_body: "never served".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let err = engine
            .fetch(HttpRequest::get("https://example.com/"))
            .await
            .expect_err("an unreadable index is also a miss");
        assert!(err.to_string().contains("unreadable"), "{err}");
        assert!(
            matches!(err, Error::Http(_)),
            "both stay typed misses so the tiered fetcher still falls through"
        );
        assert_eq!(
            inner.seen.lock().unwrap().len(),
            1,
            "no snapshot body is fetched on either miss"
        );
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
        let ts: Vec<&str> = list
            .snapshots
            .iter()
            .map(|s| s.timestamp.as_str())
            .collect();
        assert_eq!(
            ts,
            [
                "20190101000000",
                "20190301000000",
                "20190401000000",
                "20190501000000"
            ]
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
        let dup: Vec<CdxSnapshot> = (0..4)
            .map(|i| snap(&format!("2019010100000{i}"), Some("SAME")))
            .collect();
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
        assert_eq!(
            seen.len(),
            1,
            "exactly one CDX request, via the inner transport"
        );
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
        assert!(
            err.to_string().contains("bad archive 'from' bound"),
            "{err}"
        );
        assert!(inner.seen.lock().unwrap().is_empty());
    }

    /// An inverted window is refused at the door, like a malformed bound — and
    /// for the same reason: the answer CDX would give (an empty body) is
    /// indistinguishable from the answer for a URL with no captures in range,
    /// so querying it would spend a request to buy an ambiguous result.
    #[tokio::test]
    async fn list_snapshots_refuses_an_inverted_window_instead_of_reporting_it_empty() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: String::new(),
            page_body: String::new(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let err = engine
            .list_snapshots("https://example.com/", Some("2020"), Some("2019"), 10)
            .await
            .expect_err("an inverted window is refused");
        let msg = err.to_string();
        assert!(msg.contains("empty by construction"), "{msg}");
        assert!(
            msg.contains("2020") && msg.contains("2019"),
            "the refusal must show the operator both bounds it compared: {msg}"
        );
        assert!(
            inner.seen.lock().unwrap().is_empty(),
            "refusing must cost no CDX request"
        );
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
        assert!(within_window(
            now + chrono::Duration::minutes(5),
            now,
            Some(60)
        ));
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

    /// Inner stub that answers every request with one non-success status —
    /// the shape an operator hits when their inner client sends no User-Agent.
    struct FailingInner(u16);

    #[async_trait]
    impl HttpClient for FailingInner {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: self.0,
                headers: HashMap::new(),
                body: String::new(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    fn engine_over_failing(status: u16) -> ArchiveEngine {
        let cfg = ArchiveConfig {
            enabled: true,
            base_url: "https://web.archive.org".into(),
        };
        ArchiveEngine::new(&cfg, Arc::new(FailingInner(status)))
    }

    /// Both CDX doors must REACH the shared sentence, not merely produce an
    /// equivalent one of their own — the point of `cdx_failure` existing. Gut
    /// it and this goes red for both `fetch` and `list_snapshots`.
    #[tokio::test]
    async fn both_cdx_doors_surface_the_cause_of_a_400() {
        let engine = engine_over_failing(400);

        let err = engine
            .fetch(HttpRequest::get("https://example.com/"))
            .await
            .expect_err("a 400 from CDX is a miss");
        assert!(err.to_string().contains("User-Agent"), "fetch: {err}");

        let err = engine
            .list_snapshots("https://example.com/", None, None, 5)
            .await
            .expect_err("a 400 from CDX is a failure");
        assert!(
            err.to_string().contains("User-Agent"),
            "list_snapshots: {err}"
        );
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
        let ts = resp
            .headers
            .get(SNAPSHOT_TS_HEADER)
            .expect("snapshot ts set");
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
        let resp = engine
            .fetch(HttpRequest::get("https://example.com/"))
            .await
            .unwrap();
        assert_eq!(resp.body, "decade-old body");
        assert_eq!(
            resp.headers.get(SNAPSHOT_TS_HEADER).map(String::as_str),
            Some("2015-06-01T12:00:00+00:00")
        );
    }

    /// The anti-pattern: **a capability hole that reads like a mock**.
    /// `fetch_bytes` is a default-bodied trait method, so an engine that never
    /// implements it still compiles and answers "this engine does not support
    /// binary fetch_bytes" — a sentence that is equally true of a forgetful
    /// decorator, a test stub, and a deliberate refusal. Only one of those three
    /// is a bug, and the caller could not tell them apart.
    ///
    /// The archive's refusal is deliberate (which capture would "the bytes"
    /// mean?), so it refuses as itself: naming the archive, the reason, and the
    /// surface that does enumerate captures. It must also never *fetch* anything
    /// on its way to refusing.
    #[tokio::test]
    async fn a_binary_archive_fetch_refuses_as_itself_not_as_an_anonymous_default() {
        let (engine, inner) = engine_over(ScriptedInner {
            cdx_body: fresh_cdx_line(),
            page_body: "unused".into(),
            seen: std::sync::Mutex::new(Vec::new()),
        });
        let err = engine
            .fetch_bytes(HttpRequest::get("https://example.com/a.zip"))
            .await
            .expect_err("the archive engine does not serve binary bodies");
        let msg = err.to_string();
        assert!(
            msg.contains("archive") && msg.contains("list_snapshots"),
            "the refusal must name itself and the alternative: {msg}"
        );
        assert!(
            inner.seen.lock().unwrap().is_empty(),
            "refusing must cost no CDX query and no snapshot fetch"
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
        let engine = ArchiveEngine::new(
            &cfg,
            Arc::new(PlainClient(
                reqwest::Client::builder()
                    .user_agent("pumper-live-test")
                    .build()
                    .unwrap(),
            )),
        );
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
        let engine = ArchiveEngine::new(
            &cfg,
            Arc::new(PlainClient(
                reqwest::Client::builder()
                    .user_agent("pumper-live-test")
                    .build()
                    .unwrap(),
            )),
        );
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
