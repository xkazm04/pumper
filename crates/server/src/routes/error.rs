//! Error type and cross-module helpers shared by the route handlers.
//!
//! `ApiError` (the HTTP error envelope) plus the small free helpers that more
//! than one domain module needs: the `since`/cursor parsers, the keyset-cursor
//! builder, the attempt cap, and the shared `{enabled}` request body. Everything
//! here is `pub(crate)` so each domain module can pull in exactly what it uses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use utoipa::ToSchema;

/// Upper bound on a client-supplied `max_attempts`, so a job/schedule/trigger
/// can't request a practically-non-terminating retry loop.
pub(crate) const MAX_ATTEMPTS_CAP: i64 = 20;

#[derive(Debug)]
pub(crate) struct ApiError(pub(crate) StatusCode, pub(crate) String);

/// Stable machine-readable code derived from the HTTP status, sent alongside the
/// human `error` string so consumers can branch without string-matching.
///
/// This map is kept honest by `every_status_a_handler_emits_has_a_code` below,
/// which scans the route sources for `StatusCode::` uses and diffs them against
/// an EXPECTED inventory — the doc sentence that used to claim "kept in lockstep"
/// was simply false, and 403/429/503 all shipped as `"internal"` for months.
///
/// One code is *not* the status's own name: `402` has exactly one producer in
/// this service — [`pumper_core::Error::BudgetExhausted`] — and `budget_exhausted`
/// is what a client can act on, where `payment_required` would suggest billing.
pub(crate) fn error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::PAYMENT_REQUIRED => "budget_exhausted",
        StatusCode::FORBIDDEN => "forbidden",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::CONFLICT => "conflict",
        StatusCode::PAYLOAD_TOO_LARGE => "too_large",
        StatusCode::UNPROCESSABLE_ENTITY => "unprocessable",
        StatusCode::TOO_MANY_REQUESTS => "rate_limited",
        StatusCode::BAD_GATEWAY => "bad_gateway",
        StatusCode::SERVICE_UNAVAILABLE => "unavailable",
        _ => "internal",
    }
}

/// The body a 500 shows the client. Deliberately says nothing: the detail is in
/// the log line the [`From`] impl writes, keyed by the same status.
pub(crate) const INTERNAL_MESSAGE: &str = "internal error";
/// The body a 502 shows the client. Naming the failing host or the upstream URL
/// (query string and all) is exactly the leak this replaces.
pub(crate) const UPSTREAM_MESSAGE: &str = "upstream engine failure";
/// Profile errors carry the profile *directory* they failed to open, so the
/// message is fixed and names the parameter instead.
pub(crate) const PROFILE_MESSAGE: &str =
    "invalid or unusable session profile — check the 'profile' parameter";
/// Replay misses carry the cassette path and the unmatched request.
pub(crate) const REPLAY_MISS_MESSAGE: &str =
    "the recorded cassette cannot serve this request (missing, unrecorded, or truncated)";

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = error_code(self.0);
        (self.0, Json(json!({ "error": self.1, "code": code }))).into_response()
    }
}

/// The status a core error deserves at the HTTP boundary, and the message the
/// **client** is allowed to see.
///
/// Pure, so the table is a thing you can read and test rather than a `_ => 500`.
/// Two independent decisions per variant:
///
/// **Status.** Client-fault and upstream-fault variants are named as such
/// instead of being flattened into 500, which told a caller "the server is
/// broken" for a refusal that was about their own request. Deliberately NOT
/// remapped: [`pumper_core::Error::Storage`], including its `RowNotFound` case.
/// Every 404 this API returns is raised explicitly by a handler that checked;
/// a `RowNotFound` arriving here means a `fetch_one` was used where
/// `fetch_optional` belonged, and dressing that bug as a tidy 404 would hide it
/// forever. It stays a 500, which is what a bug is.
///
/// **Disclosure.** A 4xx message describes the caller's own input, so it travels
/// verbatim. A 5xx message is this process describing its insides — raw
/// sqlx/SQLite text, absolute paths under the data dir, upstream URLs — and is
/// replaced by a fixed string; the `code` carries everything a client could
/// branch on anyway. `Profile` and `ReplayMiss` are 4xx *by cause* but their
/// messages are built from server-side paths, so they are redacted too.
pub(crate) fn client_facing(e: &pumper_core::Error) -> (StatusCode, String) {
    use pumper_core::Error as E;
    match e {
        // Definitionally the caller's input: a malformed query/filter/rule.
        E::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
        // An authored refusal ("we understood, and we decline to act"), not a
        // validation failure and not a breakage — 422 keeps the three apart.
        E::Transact(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
        // A fact about the job's own ledger. Terminal by construction (see
        // `Error::is_terminal_for_job`), so a client must be able to tell it
        // from a 500 they should retry. The message is about money, not internals.
        E::BudgetExhausted(msg) => (StatusCode::PAYMENT_REQUIRED, msg.clone()),
        // Caused by the caller's `profile` parameter; message names a directory.
        E::Profile(_) => (StatusCode::BAD_REQUEST, PROFILE_MESSAGE.into()),
        // The referenced recording cannot satisfy the request — a conflict
        // between stored state and what was asked of it, not a missing route.
        E::ReplayMiss(_) => (StatusCode::CONFLICT, REPLAY_MISS_MESSAGE.into()),
        // Somebody else's failure, reported as such rather than as ours.
        E::Http(_) | E::Browser(_) | E::Claude(_) => {
            (StatusCode::BAD_GATEWAY, UPSTREAM_MESSAGE.into())
        }
        // Genuinely unexpected here. Listed one by one rather than caught by a
        // wildcard so a new core variant has to be given a home on purpose.
        E::Storage(_)
        | E::Parse(_)
        | E::Config(_)
        | E::App(_)
        | E::Io(_)
        | E::Json(_)
        | E::Other(_) => (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_MESSAGE.into()),
    }
}

impl From<pumper_core::Error> for ApiError {
    fn from(e: pumper_core::Error) -> Self {
        let (status, message) = client_facing(&e);
        // The detail the client no longer receives has to go somewhere, or a
        // redacted 500 is an unfixable one. `error!` for 5xx (which is also what
        // reaches Sentry); `debug!` for the 4xx redactions, which are ordinary
        // client mistakes and would be pure noise at a higher level.
        if status.is_server_error() {
            tracing::error!(status = status.as_u16(), error = %e, "request failed");
        } else {
            tracing::debug!(status = status.as_u16(), error = %e, "request refused");
        }
        Self(status, message)
    }
}

/// The human message for a caught panic payload.
///
/// `panic!` produces exactly two payload shapes — a `&'static str` for a literal
/// message and a `String` for a formatted one — and anything else is an
/// explicit `panic_any`, which this service never does. Mirrors the worker's own
/// panic containment so a handler panic and an app panic read alike in the logs.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Locks an **advisory in-memory cache** that is shared with the worker,
/// recovering rather than propagating if a previous holder panicked while
/// holding it.
///
/// The anti-pattern this replaces: `.lock().unwrap()` on the request path. A
/// `std::sync::Mutex` is poisoned *permanently* by one panic anywhere — and
/// these mutexes are held by the worker too, so a single panicking worker task
/// turned `/sources`, `/catalog/health`, `/jobs/{id}/receipt`, `DELETE
/// /jobs/{id}` and `POST /ingest/{id}` into connection-reset generators for the
/// rest of the process's life. Poisoning is a *warning about the data*, and the
/// question is whether this particular data can be trusted after an interrupted
/// write. For every site that uses this helper, it can:
///
/// - `contract_verdicts` — per-run telemetry, a `HashMap<String, Value>`
///   rebuilt by the next run of each source. Worst case: one stale verdict, on
///   a surface that already documents itself as "null-absent before the first
///   contracted run since boot".
/// - `job_cancels` — advisory routing for `DELETE /jobs/{id}`. Worst case: a
///   stale token, and firing one is already harmless because the worker matches
///   the attempt number before honouring it. The fallback (cancelling a
///   `queued` job synchronously) is unaffected.
/// - the ingress rate-limit buckets — `(tokens, last_seen)` pairs. Worst case:
///   one source's bucket is off by a fraction of a refill window.
///
/// None of them can be left *half-written*: the guard makes each update atomic
/// with respect to readers, so recovery hands back a structurally sound map.
/// A lock protecting something where an interrupted write really is corrupting
/// must NOT use this — it should propagate, which is what poisoning is for.
pub(crate) fn lock_advisory<'a, T>(
    mutex: &'a std::sync::Mutex<T>,
    what: &'static str,
) -> std::sync::MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!(
                cache = what,
                "recovering a poisoned advisory cache lock — some earlier task panicked while \
                 holding it. The cached data is structurally sound and is reused; the panic \
                 itself was reported where it happened"
            );
            poisoned.into_inner()
        }
    }
}

/// Parses an optional RFC-3339 `since` query param. A malformed value is the
/// client's mistake, so it is a 400 — not the blanket 500 a bare `?` would give.
pub(crate) fn parse_since(
    since: Option<&str>,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    since
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&chrono::Utc))
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("invalid 'since': {e}")))
        })
        .transpose()
}

pub(crate) fn default_limit() -> i64 {
    50
}

/// Cursors are `<sort-timestamp>|<tiebreak-id>` — decode back to the pair.
pub(crate) fn parse_cursor(cursor: &str) -> Option<(String, String)> {
    let trimmed = cursor.trim();
    if trimmed.is_empty() {
        return None; // first page
    }
    trimmed
        .split_once('|')
        .map(|(t, k)| (t.to_string(), k.to_string()))
}

/// Strict `?cursor=` parse for the routes where silently ignoring a cursor is
/// **data loss** rather than a cosmetic reset — the change feed and record
/// history, i.e. the two surfaces `@pumper/sync` and the `peer` app walk.
///
/// Blank (`?cursor=`) still means "start at the first page": on those routes the
/// param's mere PRESENCE is what selects `{items, next_cursor}` mode, and the
/// peering puller sends it empty on every fresh walk. Anything non-blank that is
/// not a `<created_at>|<tiebreak>` pair is the caller's mistake and is a 400.
///
/// The anti-pattern this replaces: [`parse_cursor`] collapses "malformed" into
/// the same `None` as "absent", so a corrupted cursor restarted the walk at the
/// NEWEST revision with a 200 and no signal anywhere. For a mirror that is not a
/// reset, it is a livelock — every page dedupes against the already-applied key
/// set, the per-run budget burns, and the walk re-suspends near the top of the
/// feed forever while the run still reports `status:"ok"`.
///
/// Deliberately scoped: the other cursor routes (`/jobs`, `/watches`, …) are
/// browse surfaces where restarting at page 1 is visible to a human and costs
/// nothing, so they keep the lenient parse.
pub(crate) fn parse_cursor_arg(cursor: &str) -> Result<Option<(String, String)>, ApiError> {
    if cursor.trim().is_empty() {
        return Ok(None); // first page
    }
    parse_cursor(cursor)
        .map(Some)
        .ok_or_else(|| ApiError(StatusCode::BAD_REQUEST, bad_cursor_message(cursor)))
}

/// The one malformed-cursor 400 body, so the shape reads identically on every
/// route that parses a cursor strictly.
pub(crate) fn bad_cursor_message(cursor: &str) -> String {
    format!(
        "invalid 'cursor' {}: expected the opaque `<created_at>|<tiebreak>` token this API \
         returns in 'next_cursor' (e.g. `2026-07-26T00:00:00.000000Z|41`). Send the param empty \
         or omit it to start at the first page.",
        echo_arg(cursor)
    )
}

/// Quotes a caller-supplied value back into an error body without letting a
/// hostile (or merely enormous) query string become the response.
fn echo_arg(raw: &str) -> String {
    const MAX: usize = 64;
    let shown: String = raw.chars().take(MAX).filter(|c| !c.is_control()).collect();
    if raw.chars().count() > MAX {
        format!("'{shown}…'")
    } else {
        format!("'{shown}'")
    }
}

/// Next-page cursor for a keyset page: `Some` only when the page came back full
/// (so more rows may remain), built from the last item. Mirrors the inline
/// pattern on `/jobs` and `/datasets/...`.
pub(crate) fn keyset_cursor<T>(
    items: &[T],
    limit: i64,
    encode: impl Fn(&T) -> String,
) -> Option<String> {
    ((items.len() as i64) == limit)
        .then(|| items.last())
        .flatten()
        .map(encode)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct EnabledBody {
    pub(crate) enabled: bool,
}

#[cfg(test)]
mod contract_tests {
    use super::{
        client_facing, error_code, ApiError, INTERNAL_MESSAGE, PROFILE_MESSAGE,
        REPLAY_MISS_MESSAGE, UPSTREAM_MESSAGE,
    };
    use axum::http::StatusCode;
    use std::collections::BTreeSet;
    use std::path::Path;

    /// Every `StatusCode::…` constant the HTTP surface names, with the `code`
    /// clients branch on — `None` for the success statuses, which carry no
    /// error envelope.
    ///
    /// The EXPECTED-diff idiom (as in `routes::mod`'s spec-coverage test): the
    /// scan below reads the route sources and diffs the statuses actually in use
    /// against this list, so a handler that starts emitting a new status is
    /// *forced* to give it a code. The doc comment on `error_code` used to claim
    /// this was "kept in lockstep" by hand; it wasn't, and 403/429/503 shipped
    /// as `"internal"` — the exact three cases where a client most needs to tell
    /// "you were refused" from "we broke".
    const EXPECTED_STATUS_USE: &[(&str, u16, Option<&str>)] = &[
        ("OK", 200, None),
        ("CREATED", 201, None),
        ("ACCEPTED", 202, None),
        ("BAD_REQUEST", 400, Some("bad_request")),
        ("UNAUTHORIZED", 401, Some("unauthorized")),
        ("PAYMENT_REQUIRED", 402, Some("budget_exhausted")),
        ("FORBIDDEN", 403, Some("forbidden")),
        ("NOT_FOUND", 404, Some("not_found")),
        ("CONFLICT", 409, Some("conflict")),
        ("PAYLOAD_TOO_LARGE", 413, Some("too_large")),
        ("UNPROCESSABLE_ENTITY", 422, Some("unprocessable")),
        ("TOO_MANY_REQUESTS", 429, Some("rate_limited")),
        ("INTERNAL_SERVER_ERROR", 500, Some("internal")),
        ("BAD_GATEWAY", 502, Some("bad_gateway")),
        ("SERVICE_UNAVAILABLE", 503, Some("unavailable")),
    ];

    /// Collects the status constants named in every `.rs` file under the HTTP
    /// surface. Rooted at `CARGO_MANIFEST_DIR` (a compile-time absolute path)
    /// rather than the CWD, and walking the directories rather than an
    /// `include_str!` list, so a NEW route module is covered the moment it
    /// exists instead of when someone remembers to add it here.
    ///
    /// Comment lines are skipped: prose that merely *mentions* a status is not
    /// a handler emitting one, and without this the test fails on its own
    /// documentation (it did).
    fn statuses_in_use() -> BTreeSet<String> {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = BTreeSet::new();
        for area in ["routes", "mcp"] {
            scan(&src.join(area), &mut found);
        }
        assert!(
            found.len() > 5,
            "the scan found almost nothing — it is looking in the wrong place, and a test that \
             cannot see the handlers cannot police them"
        );
        found
    }

    fn scan(dir: &Path, found: &mut BTreeSet<String>) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                scan(&path, found);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read source");
            for line in source.lines().filter(|l| !l.trim_start().starts_with("//")) {
                for (at, marker) in line.match_indices("StatusCode::") {
                    let name: String = line[at + marker.len()..]
                        .chars()
                        .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                        .collect();
                    // Empty for method calls like `StatusCode::from_u16`.
                    if !name.is_empty() {
                        found.insert(name);
                    }
                }
            }
        }
    }

    #[test]
    fn every_status_a_handler_emits_has_a_code() {
        let listed: BTreeSet<String> = EXPECTED_STATUS_USE
            .iter()
            .map(|(name, ..)| name.to_string())
            .collect();
        let in_use = statuses_in_use();

        let unlisted: Vec<_> = in_use.difference(&listed).collect();
        assert!(
            unlisted.is_empty(),
            "handlers emit statuses this contract has never heard of (add them to \
             EXPECTED_STATUS_USE *and* to `error_code`, or they ship as \"internal\"): {unlisted:?}"
        );
        let stale: Vec<_> = listed.difference(&in_use).collect();
        assert!(
            stale.is_empty(),
            "EXPECTED_STATUS_USE lists statuses no handler emits any more — drop them: {stale:?}"
        );

        for (name, number, code) in EXPECTED_STATUS_USE {
            let status = StatusCode::from_u16(*number).expect("a real status");
            // Close the name↔number loop, so a mistyped pair can't silently
            // assert the wrong status's code.
            let derived = status
                .canonical_reason()
                .expect("a canonical reason")
                .to_ascii_uppercase()
                .replace(['-', ' '], "_");
            assert_eq!(&derived, name, "{number} is not {name}");
            if let Some(code) = code {
                assert_eq!(&error_code(status), code, "wrong code for {name}");
            }
        }
    }

    /// The three that shipped wrong, pinned by name rather than by "not
    /// internal": a rate-limited push, a disabled ingress source, and the
    /// detection-off 503 are precisely the refusals a client is supposed to
    /// handle differently from a server fault.
    #[test]
    fn refusals_are_not_reported_as_internal_errors() {
        assert_eq!(error_code(StatusCode::FORBIDDEN), "forbidden");
        assert_eq!(error_code(StatusCode::TOO_MANY_REQUESTS), "rate_limited");
        assert_eq!(error_code(StatusCode::SERVICE_UNAVAILABLE), "unavailable");
    }

    /// The anti-pattern: `other.to_string()` went verbatim into the response
    /// body, so a 500 handed an unauthenticated caller raw SQLite text, absolute
    /// paths inside the data dir, and upstream URLs with their query strings.
    #[test]
    fn internal_failures_are_generic_not_raw_store_or_path_text() {
        let leaky = [
            pumper_core::Error::Io(std::io::Error::other(
                "opening /srv/pumper/data/pumper.db: permission denied",
            )),
            pumper_core::Error::Other(anyhow::anyhow!(
                "GET https://api.vendor.example/v2/x?api_key=s3cret failed"
            )),
            pumper_core::Error::Storage(sqlx::Error::RowNotFound),
            pumper_core::Error::Config("missing key in /etc/pumper/config.toml".into()),
        ];
        for e in leaky {
            let raw = e.to_string();
            let err = ApiError::from(e);
            assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                err.1, INTERNAL_MESSAGE,
                "the body must not vary with the cause"
            );
            assert_ne!(err.1, raw);
            for leak in ["pumper.db", "api_key", "s3cret", "/srv/", "/etc/", "sqlx"] {
                assert!(
                    !err.1.contains(leak),
                    "{leak:?} reached the client in {:?}",
                    err.1
                );
            }
        }
    }

    /// Somebody else's outage is a 502, not a confession that this server is
    /// broken — and the body still names no host or URL.
    #[test]
    fn upstream_engine_failures_are_502_not_500() {
        for e in [
            pumper_core::Error::Http("connect https://slow.example/a?k=v: timed out".into()),
            pumper_core::Error::Browser("chrome crashed rendering https://x.example".into()),
            pumper_core::Error::Claude("cli exited 1".into()),
        ] {
            let err = ApiError::from(e);
            assert_eq!(err.0, StatusCode::BAD_GATEWAY);
            assert_eq!(error_code(err.0), "bad_gateway");
            assert_eq!(err.1, UPSTREAM_MESSAGE);
            assert!(!err.1.contains("example"), "no upstream host: {:?}", err.1);
        }
    }

    /// The client-fault variants: each one is a 4xx naming what the caller can
    /// do about it, rather than the blanket 500 that told them to file a bug.
    #[test]
    fn client_fault_variants_are_4xx_not_500() {
        let (status, msg) = client_facing(&pumper_core::Error::BadRequest("bad filter".into()));
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            msg, "bad filter",
            "a validation message is the caller's own"
        );

        let (status, msg) = client_facing(&pumper_core::Error::Transact(
            "live submit is refused in this slice".into(),
        ));
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(msg.contains("live submit"), "the refusal explains itself");

        let (status, msg) = client_facing(&pumper_core::Error::BudgetExhausted(
            "$0.50 ceiling reached".into(),
        ));
        assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
        assert_eq!(error_code(status), "budget_exhausted");
        assert!(msg.contains("0.50"), "the ceiling is the whole explanation");

        // 4xx by cause, but redacted: these two build their messages from
        // server-side paths (the profile dir, the cassette file).
        let (status, msg) = client_facing(&pumper_core::Error::Profile(
            "opening /srv/pumper/data/profiles/x/cookies.json: denied".into(),
        ));
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, PROFILE_MESSAGE);
        assert!(msg.contains("'profile'"), "it names the parameter at fault");

        let (status, msg) = client_facing(&pumper_core::Error::ReplayMiss(
            "no recorded response in /srv/pumper/data/cassettes/a.json".into(),
        ));
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(msg, REPLAY_MISS_MESSAGE);
        assert!(!msg.contains("/srv/"), "no cassette path: {msg:?}");
    }

    /// The anti-pattern, and the reason this helper exists: one panic anywhere
    /// poisons a `std::sync::Mutex` **permanently**, and these caches are held
    /// by the worker as well as the routes — so a single panicking worker task
    /// turned five endpoints into connection-reset generators for the rest of
    /// the process's life. Recovery must be per-lock and forever, not a retry.
    #[test]
    fn a_poisoned_advisory_lock_is_not_a_permanent_500() {
        use std::collections::HashMap;
        use std::sync::Mutex;

        let cache: Mutex<HashMap<&str, i32>> = Mutex::new(HashMap::new());
        super::lock_advisory(&cache, "test").insert("before", 1);

        // A holder dies mid-use, exactly as a panicking worker hook would.
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut guard = cache.lock().expect("not poisoned yet");
            guard.insert("during", 2);
            panic!("holder died");
        }));
        assert!(caught.is_err(), "the holder really did unwind");
        assert!(cache.is_poisoned(), "and the lock really is poisoned");
        assert!(
            cache.lock().is_err(),
            "so a bare `.lock().unwrap()` here would panic — the 500 generator"
        );

        // Every subsequent request still works, and the data is intact — both
        // the write from before the panic and the one that completed during it.
        for _ in 0..3 {
            let guard = super::lock_advisory(&cache, "test");
            assert_eq!(guard.get("before"), Some(&1));
            assert_eq!(guard.get("during"), Some(&2));
        }
        super::lock_advisory(&cache, "test").insert("after", 3);
        assert_eq!(super::lock_advisory(&cache, "test").get("after"), Some(&3));
    }

    /// Both payload shapes `panic!` can produce reach the log intact — a panic
    /// whose message is swallowed is barely better than the reset it replaced.
    #[test]
    fn panic_message_reads_both_payload_shapes_not_just_literals() {
        let literal: Box<dyn std::any::Any + Send> = Box::new("index out of bounds");
        assert_eq!(
            super::panic_message(literal.as_ref()),
            "index out of bounds"
        );

        let formatted: Box<dyn std::any::Any + Send> = Box::new(format!("row {} missing", 7));
        assert_eq!(super::panic_message(formatted.as_ref()), "row 7 missing");

        // Anything else is still described rather than dropped.
        let exotic: Box<dyn std::any::Any + Send> = Box::new(42u8);
        assert_eq!(
            super::panic_message(exotic.as_ref()),
            "non-string panic payload"
        );
    }

    /// The convention, enforced as an inventory rather than as a sentence:
    /// **no request-path code unwraps a lock result.** A single `.lock()
    /// .unwrap()` reintroduced anywhere under `src/routes` or `src/mcp` re-arms
    /// the permanent-500 failure for whatever endpoint owns it.
    ///
    /// `tokio::sync::Mutex` (`.lock().await`) is unaffected and deliberately not
    /// matched: it cannot be poisoned, because it does not unlock on unwind.
    #[test]
    fn no_route_unwraps_a_lock_result() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut offenders: Vec<String> = Vec::new();
        let mut scanned = 0usize;
        for area in ["routes", "mcp"] {
            scan_for_lock_unwraps(&src.join(area), &mut offenders, &mut scanned);
        }
        assert!(
            scanned > 20,
            "only {scanned} files had any production body — the truncation below has eaten the \
             surface this is supposed to police"
        );
        assert!(
            offenders.is_empty(),
            "request-path code unwraps a poisonable lock — route it through \
             `lock_advisory` (or, if an interrupted write really would corrupt the data, \
             propagate deliberately and say why): {offenders:#?}"
        );
    }

    fn scan_for_lock_unwraps(dir: &Path, offenders: &mut Vec<String>, scanned: &mut usize) {
        let entries =
            std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()));
        for entry in entries {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                scan_for_lock_unwraps(&path, offenders, scanned);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("read source");
            // Production code only. The rule is about the REQUEST PATH, and a
            // `#[cfg(test)]` block is not on it — this very file's poison
            // fixture has to lock-and-unwrap to create the poisoned state.
            // Every module in this tree puts its test block last (checked: no
            // route file defines a function after its first one), so truncating
            // at the first column-0 marker keeps all shipped code.
            let body: Vec<&str> = source
                .lines()
                .take_while(|l| *l != "#[cfg(test)]")
                .filter(|l| !l.trim_start().starts_with("//"))
                .map(str::trim)
                .collect();
            if body.is_empty() {
                continue;
            }
            *scanned += 1;
            // Whitespace-normalized, so the multi-line `.lock()\n.unwrap()`
            // shape rustfmt produces is caught just like the one-liner.
            let flat = body.join(" ").replace(" .", ".").replace(". ", ".");
            for bad in [".lock().unwrap()", ".lock().expect("] {
                if flat.contains(bad) {
                    offenders.push(format!("{} contains `{bad}`", path.display()));
                }
            }
        }
    }

    /// A reasoned NON-mapping, pinned so nobody "fixes" it later: a
    /// `RowNotFound` reaching this boundary means a handler used `fetch_one`
    /// where `fetch_optional` belonged. Every real 404 is raised explicitly by a
    /// handler that looked; dressing this one up as a tidy 404 would turn a bug
    /// into a plausible-looking answer and hide it permanently.
    #[test]
    fn row_not_found_stays_500_not_a_fabricated_404() {
        let err = ApiError::from(pumper_core::Error::Storage(sqlx::Error::RowNotFound));
        assert_eq!(err.0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_ne!(err.0, StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod tests {
    use super::{keyset_cursor, parse_cursor, parse_cursor_arg};
    use axum::http::StatusCode;

    /// ALL cursor encoding goes through `keyset_cursor` (9 call sites). Its
    /// contract: a cursor exists only when the page came back full, and it is
    /// built from the last item. Hand-rolling `len()==limit -> last() -> map`
    /// is the anti-pattern this pins down.
    #[test]
    fn keyset_cursor_only_on_a_full_page_from_the_last_item() {
        let encode = |s: &&str| s.to_string();
        // Full page => cursor from the LAST item.
        assert_eq!(keyset_cursor(&["a", "b", "c"], 3, encode), Some("c".into()));
        // Short page => no more rows => no cursor.
        assert_eq!(keyset_cursor(&["a", "b"], 3, encode), None);
        // Empty page => no cursor (and no panic on last()).
        assert_eq!(keyset_cursor(&[] as &[&str], 3, encode), None);
    }

    #[test]
    fn parse_cursor_round_trips_and_treats_blank_as_first_page() {
        assert_eq!(parse_cursor(""), None);
        assert_eq!(parse_cursor("   "), None);
        assert_eq!(
            parse_cursor("2026-07-26T00:00:00.000000Z|job-42"),
            Some(("2026-07-26T00:00:00.000000Z".into(), "job-42".into()))
        );
        // No separator => not a valid keyset cursor.
        assert_eq!(parse_cursor("garbage"), None);
    }

    /// The anti-pattern this defends: a malformed cursor used to decode to the
    /// same `None` as an absent one, so the feed answered 200 with PAGE ONE —
    /// the newest revisions — and no consumer could tell its walk had been
    /// silently rewound to the top.
    #[test]
    fn bad_cursor_400_not_page_one() {
        let err = parse_cursor_arg("garbage").expect_err("a separator-less cursor must not pass");
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(
            err.1.contains("next_cursor") && err.1.contains("<created_at>|<tiebreak>"),
            "the 400 must name the expected format, not just say 'invalid': {}",
            err.1
        );
        assert!(
            err.1.contains("'garbage'"),
            "and echo what was sent: {}",
            err.1
        );
    }

    /// Absent and blank are NOT the malformed case: `?cursor=` is the documented
    /// way to select paged mode from the first page, and the `peer` app sends
    /// exactly that on every fresh walk. Turning it into a 400 would break every
    /// mirror's first pull.
    #[test]
    fn blank_cursor_is_the_first_page_not_an_error() {
        assert_eq!(parse_cursor_arg("").unwrap(), None);
        assert_eq!(parse_cursor_arg("   ").unwrap(), None);
        assert_eq!(
            parse_cursor_arg("2026-07-26T00:00:00.000000Z|41").unwrap(),
            Some(("2026-07-26T00:00:00.000000Z".into(), "41".into()))
        );
    }

    /// A hostile cursor must not turn the error body into an echo chamber.
    #[test]
    fn bad_cursor_message_truncates_and_strips_control_chars() {
        let err = parse_cursor_arg(&"z".repeat(5_000)).expect_err("still malformed");
        assert!(
            err.1.len() < 400,
            "error body stayed bounded: {}",
            err.1.len()
        );
        let err = parse_cursor_arg("bad\ncursor\u{0}").expect_err("still malformed");
        assert!(!err.1.contains('\n') && !err.1.contains('\u{0}'));
    }
}
