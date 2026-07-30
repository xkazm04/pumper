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
/// human `error` string so consumers can branch without string-matching. Kept in
/// lockstep with the statuses the handlers actually emit.
pub(crate) fn error_code(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::BAD_GATEWAY => "bad_gateway",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::CONFLICT => "conflict",
        StatusCode::PAYLOAD_TOO_LARGE => "too_large",
        StatusCode::UNPROCESSABLE_ENTITY => "unprocessable",
        _ => "internal",
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = error_code(self.0);
        (self.0, Json(json!({ "error": self.1, "code": code }))).into_response()
    }
}

impl From<pumper_core::Error> for ApiError {
    fn from(e: pumper_core::Error) -> Self {
        // A BadRequest is the one core error that is definitionally the client's
        // fault (a malformed query/filter/rule) → 400. Everything else is
        // unexpected at the request boundary → 500; the client-distinguishable
        // outcomes (404/409/400) are otherwise raised explicitly by the handlers.
        match e {
            pumper_core::Error::BadRequest(msg) => Self(StatusCode::BAD_REQUEST, msg),
            other => Self(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
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
mod tests {
    use super::{keyset_cursor, parse_cursor};

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
}
