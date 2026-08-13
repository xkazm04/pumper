//! Engine capability traits. Apps depend only on these; concrete engines
//! (`engine-http`, `engine-browser`, `engine-claude`) implement them, and the
//! server wires everything together into an [`EngineSet`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

// ---- Session vault: named login profiles (phase 1) -------------------------
//
// A *profile* is a named, persistent identity a fetch can run under. It lives in
// its own directory under `[fetcher] profiles_dir` (default `data/profiles`),
// created on first use:
//
//   data/profiles/<name>/cookies.json   persistent HTTP cookie jar (engine-http)
//   data/profiles/<name>/browser/       Chrome user-data-dir      (engine-browser)
//
// The name is the only untrusted input in that path, so it is validated to a
// path-safe alphabet before it is ever joined onto a directory. Phase 1 stores
// session state only — there is no credential management or at-rest encryption.

/// Cookie-jar file inside a profile dir.
pub const PROFILE_COOKIES_FILE: &str = "cookies.json";
/// Chrome user-data-dir inside a profile dir.
pub const PROFILE_BROWSER_DIR: &str = "browser";
/// Max profile-name length (keeps paths sane on every platform).
pub const PROFILE_NAME_MAX_LEN: usize = 64;

/// Accepts only path-safe profile names: 1..=64 chars of ASCII alphanumerics,
/// `-`, or `_`. Everything else — separators, `.`/`..`, drive letters, spaces,
/// non-ASCII — is rejected with a typed [`Error::Profile`], so a name can never
/// escape `profiles_dir`.
pub fn validate_profile_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Profile("name must not be empty".into()));
    }
    if name.len() > PROFILE_NAME_MAX_LEN {
        return Err(Error::Profile(format!(
            "name '{name}' is longer than {PROFILE_NAME_MAX_LEN} chars"
        )));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '-' || *c == '_'))
    {
        return Err(Error::Profile(format!(
            "name '{name}' contains {bad:?}; only ASCII letters, digits, '-' and '_' are allowed"
        )));
    }
    Ok(())
}

/// `<profiles_dir>/<name>` for a validated name.
pub fn profile_dir(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    validate_profile_name(name)?;
    Ok(profiles_dir.join(name))
}

/// `<profiles_dir>/<name>/cookies.json` — the HTTP tier's persistent jar.
pub fn profile_cookies_path(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    Ok(profile_dir(profiles_dir, name)?.join(PROFILE_COOKIES_FILE))
}

/// `<profiles_dir>/<name>/browser` — the browser tier's Chrome user-data-dir.
pub fn profile_browser_dir(profiles_dir: &Path, name: &str) -> Result<PathBuf> {
    Ok(profile_dir(profiles_dir, name)?.join(PROFILE_BROWSER_DIR))
}

/// Provenance response header set by the archive engine: `"archive"` when the
/// body was served from a web archive snapshot rather than the live site.
///
/// **This header is a transport, not the consumer-facing field.** An
/// [`HttpClient`] that serves stored snapshots has exactly one channel back to
/// whoever wraps it — the [`HttpResponse`] — and header maps do **not** survive
/// a tiered fetch ([`crate::FetchOutcome`] has no header map, and never had
/// one). The wrapper lifts this header off with [`snapshot_provenance`] into
/// [`crate::FetchOutcome::snapshot`], and *that* typed field is what consumers,
/// receipts and records read. Before 2026-08 nothing lifted it, so both
/// constants had a single writer and zero readers, and an archived body was
/// indistinguishable from a live one everywhere past the engine boundary.
pub const FETCHED_VIA_HEADER: &str = "x-pumper-fetched-via";
/// Provenance response header set by the archive engine: the snapshot's capture
/// timestamp (RFC 3339 UTC). Present only alongside [`FETCHED_VIA_HEADER`], and
/// read through the same [`snapshot_provenance`] seam.
pub const SNAPSHOT_TS_HEADER: &str = "x-pumper-snapshot-ts";

/// Where a served body actually came from, when that is **not** the live site.
///
/// Present on a [`crate::FetchOutcome`] only for a body served out of a stored
/// capture, so a consumer branches on `Option::is_some` rather than parsing the
/// `escalations` prose. The freshness/availability trade the archive tier makes
/// is only safe if the consumer can see it was made.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotProvenance {
    /// The store that served the body — `"archive"` for the Wayback tier. A
    /// free string rather than an enum: the value travels over the wire from
    /// whatever engine set [`FETCHED_VIA_HEADER`], and a second snapshot source
    /// must not require a core enum variant to be legible.
    pub via: String,
    /// The snapshot's capture time, RFC 3339 UTC, exactly as the serving engine
    /// reported it. `None` when the engine marked provenance without one —
    /// "this came from a store" is still worth saying, but "this is what the
    /// page looked like on 2019-03-11" is the fact the tier actually trades.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured_at: Option<String>,
}

impl SnapshotProvenance {
    /// One human line for the places that render prose rather than branch on
    /// types — the fetch trace's `detail` and the job receipt's cost-event
    /// `detail`. The single renderer for both, so the two surfaces can never
    /// drift into describing the same fetch differently.
    ///
    /// This is a *rendering* of the struct, never the storage: nothing may
    /// classify a fetch by matching this string, which is why it is free to be
    /// reworded.
    pub fn note(&self) -> String {
        match &self.captured_at {
            Some(ts) => format!("served from {} snapshot captured {ts}", self.via),
            None => format!("served from {} snapshot (capture time unknown)", self.via),
        }
    }
}

/// Lifts snapshot provenance off a header map, or `None` when it carries no
/// [`FETCHED_VIA_HEADER`].
///
/// Generic over the map so the one reader serves both header maps in the repo:
/// [`HttpResponse::headers`] (a `HashMap`, the live seam) and the VCR
/// cassette entry's ordered map (the replay seam). A second copy of these two
/// header names is exactly how the constants got a writer and no reader.
///
/// Lookup is ASCII-case-insensitive — a header map that has round-tripped
/// through a real HTTP stack has no guaranteed casing — and an empty value is
/// treated as absent, because a marker that says nothing is not provenance.
///
/// **Call this only where a snapshot-serving engine is the one that answered.**
/// The header is trivially forgeable by any origin, so reading it off an
/// ordinary live response would let a hostile host stamp its own page
/// "archived". The tiered fetcher therefore reads it in the archive branch only.
pub fn snapshot_provenance<'a>(
    headers: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> Option<SnapshotProvenance> {
    let mut via: Option<&str> = None;
    let mut captured_at: Option<&str> = None;
    for (name, value) in headers {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        if name.eq_ignore_ascii_case(FETCHED_VIA_HEADER) {
            via = Some(value);
        } else if name.eq_ignore_ascii_case(SNAPSHOT_TS_HEADER) {
            captured_at = Some(value);
        }
    }
    Some(SnapshotProvenance {
        via: via?.to_string(),
        captured_at: captured_at.map(str::to_string),
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    #[default]
    Get,
    Post,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRequest {
    pub url: String,
    #[serde(default)]
    pub method: HttpMethod,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
    /// Skip the response cache for this request (always hit the network).
    #[serde(default)]
    pub no_cache: bool,
    /// Override the response-cache TTL (seconds). On write it sets how long the
    /// stored response stays fresh; on read it also caps accepted staleness, so a
    /// caller asking for `<=N`-second-old content is never served a longer-lived
    /// entry another caller wrote. `None` uses the configured `[cache] ttl_secs`.
    /// Not part of the cache key (it shapes freshness, not the answer) and ignored
    /// when uncacheable.
    #[serde(default)]
    pub ttl_override: Option<u64>,
    /// Conditional GET validator: sent as `If-None-Match` so the origin can
    /// answer `304 Not Modified` (empty body) when the resource is unchanged.
    /// Powers incremental recrawl / change-monitoring. Usually paired with
    /// `no_cache` so the request actually revalidates instead of being served
    /// from the local TTL cache.
    #[serde(default)]
    pub etag: Option<String>,
    /// Conditional GET validator: sent as `If-Modified-Since` (an HTTP-date
    /// string, typically the origin's prior `Last-Modified`). Same 304 contract
    /// as `etag`.
    #[serde(default)]
    pub if_modified_since: Option<String>,
    /// Per-request response body cap (bytes). Overrides `[http] max_body_bytes`.
    /// A response whose streamed body exceeds this is rejected with a typed error
    /// naming the cap and URL (guards against unbounded/hostile bodies). `None`
    /// uses the configured default.
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
    /// Per-request timeout (seconds) applied to each attempt. Overrides the
    /// client-global `[http] timeout_secs` for this request only. `None` uses the
    /// global timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Per-request proxy override (`http`/`https`/`socks5` URL, optional
    /// `user:pass@` auth). Routes just this request through the given proxy
    /// instead of `[http] proxy`. Served from a small bounded client pool since
    /// reqwest binds a proxy at client-build time. `None` uses the configured
    /// default (or no proxy).
    #[serde(default)]
    pub proxy: Option<String>,
    /// Session-vault profile to run this request under: it is served by a client
    /// bound to `<profiles_dir>/<name>/cookies.json`, a **persistent** cookie jar
    /// that survives restarts (the default client's jar is in-memory and dies
    /// with the process). `None` = exactly the previous behavior. An invalid name
    /// yields a typed [`Error::Profile`].
    #[serde(default)]
    pub profile: Option<String>,
    /// Archive freshness window (seconds). When set, an archive-capable client
    /// (the tier-zero archive engine) may serve a stored web-archive snapshot of
    /// this URL captured no longer than this many seconds ago; an older-only (or
    /// absent) snapshot is a typed miss, and the tiered fetcher falls through to
    /// the live ladder. Ignored by the plain HTTP engine. `None` = live-only,
    /// exactly the previous behavior.
    #[serde(default)]
    pub archive_max_age: Option<u64>,
}

impl HttpRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: HttpMethod::Get,
            headers: HashMap::new(),
            body: None,
            no_cache: false,
            ttl_override: None,
            etag: None,
            if_modified_since: None,
            max_body_bytes: None,
            timeout_secs: None,
            proxy: None,
            profile: None,
            archive_max_age: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
    pub final_url: String,
    /// Whether this response was served from the HTTP cache rather than the
    /// network. Set by the engine; surfaced in the tiered-fetch trace so callers
    /// can distinguish a cache hit from a live fetch.
    pub cache_hit: bool,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// A scripted interaction run against a rendered page before capture — the
/// escape hatch for infinite-scroll / "load more" / lazy-loaded listings that a
/// one-shot render only captures the first viewport of. Executed in order after
/// the settle wait and before `evaluate`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PageAction {
    /// Scroll to the bottom of the document (triggers most infinite-scroll).
    ScrollBottom,
    /// Scroll by a pixel delta (negative scrolls up).
    ScrollBy { pixels: i64 },
    /// Click the first element matching a CSS selector (e.g. a "Load more" button).
    Click { selector: String },
    /// Type text into the first element matching a selector (focus + set value).
    Type { selector: String, text: String },
    /// Wait until a selector appears, up to `timeout_ms` (falls back to the
    /// nav timeout).
    WaitForSelector {
        selector: String,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    /// Wait a fixed number of milliseconds (a settle pause between steps).
    WaitMs { ms: u64 },
    /// Repeat `steps` up to `times`. When `until_selector_count_stable` is set,
    /// stop early once that selector's match count stops growing between
    /// iterations — the "scroll until no new rows load" loop.
    Repeat {
        times: u32,
        #[serde(default)]
        steps: Vec<PageAction>,
        #[serde(default)]
        until_selector_count_stable: Option<String>,
    },
}

impl PageAction {
    /// The CSS selector this action targets, when it has one. Scrolls, fixed
    /// waits and `Repeat` target no element, so they have none — a transact
    /// whose `submit_action` is one of those has no submit target to assess.
    pub fn selector(&self) -> Option<&str> {
        match self {
            PageAction::Click { selector }
            | PageAction::Type { selector, .. }
            | PageAction::WaitForSelector { selector, .. } => Some(selector),
            PageAction::ScrollBottom
            | PageAction::ScrollBy { .. }
            | PageAction::WaitMs { .. }
            | PageAction::Repeat { .. } => None,
        }
    }
}

/// What one executed [`PageAction`] actually did.
///
/// The anti-pattern this exists to kill: the executor counted every action it
/// *reached* as "completed", so a flow whose three selectors all 404'd reported
/// `steps_completed: 3` — an evidence bundle that cannot distinguish a clean run
/// from a total miss is worse than no bundle, because a human approves off it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    /// The action ran and the page accepted it.
    Ok,
    /// The action's CSS selector matched nothing (a click/type target that is
    /// not on the page, or a `wait_for_selector` that never appeared).
    SelectorMissing,
    /// The element was found but the interaction itself failed (a click that
    /// CDP refused, a `type` that errored, a scroll `evaluate` that threw).
    ActionFailed,
    /// **Coarse, `Repeat`-only**: the block ran but not every inner step of
    /// every iteration succeeded (or an iteration was cut by the deadline).
    /// Inner outcomes are deliberately NOT rolled up per step — one outcome per
    /// block, so the evidence never claims granularity it does not have.
    Partial,
}

impl StepOutcome {
    pub fn is_ok(self) -> bool {
        matches!(self, StepOutcome::Ok)
    }
}

/// Outcome of an action that must first find its element and then act on it.
/// Keeps "the selector was not there" (a flow/site mismatch a reviewer must
/// see) distinct from "the element was there and the interaction failed".
pub fn interaction_outcome(found: bool, acted: bool) -> StepOutcome {
    match (found, acted) {
        (false, _) => StepOutcome::SelectorMissing,
        (true, false) => StepOutcome::ActionFailed,
        (true, true) => StepOutcome::Ok,
    }
}

/// Whether one pass over a step list fully succeeded: every requested step ran
/// (none skipped at the deadline) and every one of them reported `Ok`. The
/// rollup a `Repeat` block's single coarse outcome is built from.
pub fn pass_fully_succeeded(requested: usize, outcomes: &[StepOutcome]) -> bool {
    outcomes.len() == requested && outcomes.iter().all(|o| o.is_ok())
}

/// Requested-vs-attempted-vs-succeeded for one executed step list — all three
/// recoverable from the evidence bundle, so "we asked for 3, ran 2, one worked"
/// can never be reported as "3 completed".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSummary {
    /// Steps the caller asked for.
    pub requested: usize,
    /// Steps the executor actually reached (fewer than `requested` iff the
    /// flow's time budget ran out mid-list).
    pub attempted: usize,
    /// Steps that reported [`StepOutcome::Ok`] — the honest "completed" count.
    pub completed: usize,
    /// `true` when the deadline stopped the list before every step was reached.
    pub deadline_hit: bool,
}

/// Rolls a per-step outcome list into a [`StepSummary`]. `outcomes` carries one
/// entry per step the executor *attempted*, in order.
pub fn summarize_steps(requested: usize, outcomes: &[StepOutcome]) -> StepSummary {
    let attempted = outcomes.len();
    StepSummary {
        requested,
        attempted,
        completed: outcomes.iter().filter(|o| o.is_ok()).count(),
        deadline_hit: attempted < requested,
    }
}

// ---- Transact (M06, v1 slice: dry-run ONLY) --------------------------------
//
// A *transact flow* is a declarative multi-step interaction (navigate → fill →
// click → wait) executed by the browser engine up to — and never past — the
// final irreversible action. The steps reuse [`PageAction`] verbatim; the
// irreversible action lives in its OWN field (`submit_action`), which the v1
// executor has no code path to run: stop-before-submit is structural, not a
// flag check. `submit: true` is rejected with a typed [`Error::Transact`]
// because live submission requires the human-approval design (pending-approval
// jobs + `POST /transactions/{id}/approve`) documented as the next slice.

/// A declarative browser transaction, executed **dry-run only** in this slice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactRequest {
    /// Page the flow starts on.
    pub url: String,
    /// Session-vault profile to act under (logins/cookies). Same contract as
    /// [`RenderRequest::profile`]; validated by [`validate_profile_name`].
    #[serde(default)]
    pub profile: Option<String>,
    /// The reversible steps (fill/click/wait/scroll), executed in order up to
    /// the final confirmation state. Reuses [`PageAction`] verbatim.
    #[serde(default)]
    pub steps: Vec<PageAction>,
    /// The exact irreversible action the flow would perform (e.g. clicking the
    /// real submit button). **Never executed in v1** — it is captured verbatim
    /// into the evidence bundle as `would_submit` so a human can review it.
    pub submit_action: PageAction,
    /// Request live submission. `false` (default) = dry-run. `true` is
    /// REJECTED with a typed [`Error::Transact`]: releasing a live submit needs
    /// the human-approval slice (pending-approval jobs + an explicit approve
    /// endpoint), which does not exist yet.
    #[serde(default)]
    pub submit: bool,
    /// Caller-chosen idempotency key. Required non-empty; recorded in the
    /// evidence bundle now, and the dedup key of the future `transactions`
    /// table that will block double-submission once live submits exist.
    pub idempotency_key: String,
    /// Wait for this selector after navigation, before running steps.
    #[serde(default)]
    pub wait_for_selector: Option<String>,
    /// Extra settle time before steps; engine default when `None`.
    #[serde(default)]
    pub extra_wait_ms: Option<u64>,
    /// Cap on the captured DOM-snapshot size (bytes); engine default when `None`.
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
}

impl TransactRequest {
    /// Rejects flows this slice must not run: `submit: true` (typed
    /// [`Error::Transact`] pointing at the human-approval design), an empty
    /// idempotency key, and an invalid profile name. Engines call this before
    /// touching a browser; apps call it before touching an engine.
    pub fn validate(&self) -> Result<()> {
        if self.submit {
            return Err(Error::Transact(
                "live submission (submit: true) is not available: this slice executes flows \
                 dry-run only, stopping before the irreversible action. Releasing a live submit \
                 requires the human-approval design (pending-approval transactions + an explicit \
                 approve endpoint) — the documented next slice. Re-run with submit: false to get \
                 the evidence bundle for review."
                    .into(),
            ));
        }
        if self.idempotency_key.trim().is_empty() {
            return Err(Error::Transact(
                "idempotency_key must be a non-empty caller-chosen key: it is recorded with the \
                 evidence bundle and will dedup live submissions in the next slice"
                    .into(),
            ));
        }
        if let Some(profile) = &self.profile {
            validate_profile_name(profile)?;
        }
        Ok(())
    }

    /// Every selector the flow types into (recursing through `Repeat`), in
    /// order, deduplicated — the fields whose live DOM values the evidence
    /// bundle summarizes.
    pub fn fill_selectors(&self) -> Vec<String> {
        fn walk(steps: &[PageAction], out: &mut Vec<String>) {
            for step in steps {
                match step {
                    PageAction::Type { selector, .. } => {
                        if !out.iter().any(|s| s == selector) {
                            out.push(selector.clone());
                        }
                    }
                    PageAction::Repeat { steps, .. } => walk(steps, out),
                    _ => {}
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.steps, &mut out);
        out
    }
}

/// Top-level param keys a [`TransactRequest`] understands. Pinned against the
/// struct itself by `transact_fields_match_the_request_struct` (serde is the
/// source of truth), so a new field can't be added without landing here too.
pub const TRANSACT_FIELDS: &[&str] = &[
    "url",
    "profile",
    "steps",
    "submit_action",
    "submit",
    "idempotency_key",
    "wait_for_selector",
    "extra_wait_ms",
    "max_body_bytes",
];

/// Param keys a transact job carries that [`TransactRequest`] does not
/// understand.
///
/// The anti-pattern: serde silently drops unknown fields, so a typo'd `"step"`
/// (singular) passed the params schema, deserialized into an EMPTY step list,
/// and ran a zero-step flow that produced a perfectly plausible landing-page
/// evidence bundle — the worst failure mode for an app a human approves off.
///
/// `#[serde(deny_unknown_fields)]` is deliberately NOT used: the trigger runtime
/// injects a `_trigger` envelope into a target job's params, and trigger-fired
/// enqueues go straight to the queue without the enqueue-time schema validator,
/// so denying unknown fields at the struct would break every triggered transact.
/// Underscore-prefixed keys are host-owned and always allowed; everything else
/// must be a field the request declares.
pub fn unknown_transact_fields(params: &Value) -> Vec<String> {
    let Some(obj) = params.as_object() else {
        return Vec::new();
    };
    obj.keys()
        .filter(|k| !k.starts_with('_') && !TRANSACT_FIELDS.contains(&k.as_str()))
        .cloned()
        .collect()
}

/// Refuses a transact flow whose session profile the vault does not hold.
///
/// Renders **create** a profile dir on first use — that is the documented
/// onboarding path (run once with `[browser] headless = false`, log in by
/// hand). For a flow that ACTS, that default is a trap: a typo'd profile name
/// silently births an empty, logged-OUT Chrome profile, the flow runs against a
/// login wall, and the evidence bundle looks perfectly plausible while
/// describing entirely the wrong page. `exists` is whether the profile's Chrome
/// user-data-dir is already there (`GET /profiles` reports it as
/// `has_browser_dir`); "no profile at all" stays valid and unaffected.
///
/// Typed [`Error::Transact`], not [`Error::Profile`], on purpose: this is a
/// transact-flow policy rather than a generic profile problem (renders keep
/// create-on-first-use), and it makes the refusal terminal for the job — see
/// [`Error::is_terminal_for_job`] — instead of riding the retry ladder.
pub fn require_existing_profile(name: &str, exists: bool) -> Result<()> {
    if exists {
        return Ok(());
    }
    Err(Error::Transact(format!(
        "session profile '{name}' has no browser session in the vault, and a transact flow will \
         not create one: an empty profile is a LOGGED-OUT browser, so the flow would run against \
         a login wall and still emit a plausible-looking evidence bundle of the wrong page. \
         Check `GET /profiles` for the profiles you have (`has_browser_dir: true` is the one this \
         needs); establish this one by rendering under it once with `[browser] headless = false` \
         and logging in, then re-run the flow."
    )))
}

/// The live DOM value of one filled field at the moment the flow stopped —
/// what a reviewer checks before ever approving a live submit.
///
/// The three trailing fields are `#[serde(default)]`, so payloads written before
/// they existed (`{selector, value, found}`) still decode unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FilledField {
    /// The `Type` step's selector.
    pub selector: String,
    /// The element's current value (`.value`, falling back to text content),
    /// capped at [`FILLED_VALUE_MAX_CHARS`]. `None` when the element was not
    /// found, when it was empty, **or when it was redacted** (see `redacted` —
    /// a `None` value with `found: true` is never ambiguous because of it).
    pub value: Option<String>,
    /// Whether the element existed in the DOM at capture time.
    pub found: bool,
    /// `true` when the element is a secret input (see [`is_sensitive_input`])
    /// and its value was dropped **in the page**, before the result ever
    /// crossed into the job's evidence, result, SSE stream or webhooks.
    #[serde(default)]
    pub redacted: bool,
    /// Length (in JS characters) of the value as it stood on the page, before
    /// redaction or truncation — a non-reversible "the field was filled, with
    /// this much" hint. `None` when the element was not found.
    #[serde(default)]
    pub value_len: Option<usize>,
    /// `true` when `value` holds only the first [`FILLED_VALUE_MAX_CHARS`]
    /// characters (a textarea can't balloon the result payload).
    #[serde(default)]
    pub truncated: bool,
}

impl FilledField {
    /// The honest "we looked and the element was not there" row.
    pub fn not_found(selector: impl Into<String>) -> Self {
        Self {
            selector: selector.into(),
            value: None,
            found: false,
            redacted: false,
            value_len: None,
            truncated: false,
        }
    }
}

/// Max characters of a captured field value kept in the evidence bundle. A
/// filled `<textarea>` is unbounded page-controlled input, and the summary
/// exists to prove a field was filled — not to mirror its contents into
/// `jobs.result` (and thence every SSE subscriber and webhook payload).
pub const FILLED_VALUE_MAX_CHARS: usize = 512;

/// `autocomplete` tokens whose value is a credential or payment secret. Kept in
/// Rust and **compiled into the capture JS** by [`filled_fields_js`], so the
/// browser-side predicate and the Rust-side twin ([`is_sensitive_input`]) can
/// never drift apart.
pub const SENSITIVE_AUTOCOMPLETE_TOKENS: &[&str] = &[
    "current-password",
    "new-password",
    "one-time-code",
    "cc-number",
    "cc-csc",
    "cc-exp",
    "cc-exp-month",
    "cc-exp-year",
];

/// Whether an input's live value is a secret that must never be republished.
///
/// The anti-pattern this defends: a flow that types a password captured that
/// password's live value into `filled_fields[].value`, which lands in
/// `evidence.json` on disk, the persisted job result, every SSE subscriber and
/// every webhook/HMAC callback payload. The evidence must prove the field was
/// filled without republishing what was typed into it.
///
/// `input_type` is the element's `type` attribute; `autocomplete` its
/// `autocomplete` attribute (a whitespace/comma-separated token list). Both are
/// matched case-insensitively.
pub fn is_sensitive_input(input_type: &str, autocomplete: &str) -> bool {
    if input_type.trim().eq_ignore_ascii_case("password") {
        return true;
    }
    autocomplete
        .split([' ', '\t', '\n', '\r', ','])
        .filter(|t| !t.is_empty())
        .any(|token| {
            SENSITIVE_AUTOCOMPLETE_TOKENS
                .iter()
                .any(|s| token.eq_ignore_ascii_case(s))
        })
}

/// Enforces the capture contract on ONE decoded field, whatever the page (or a
/// drifting/hand-written evaluate result) claimed: a field marked `redacted`
/// carries no value, and every surviving value is capped at
/// [`FILLED_VALUE_MAX_CHARS`] with `value_len` preserving the real length.
///
/// Defense in depth, not the primary guard — the masking happens in the page
/// (see [`filled_fields_js`]) so the plaintext never reaches this process at
/// all. This is what makes that guarantee unbypassable from the decode side.
pub fn redact_field(mut field: FilledField) -> FilledField {
    if let Some(value) = field.value.take() {
        let len = value.chars().count();
        if field.value_len.is_none() {
            field.value_len = Some(len);
        }
        if field.redacted {
            // A redacted field NEVER carries a value, however it arrived.
            field.truncated = false;
        } else if len > FILLED_VALUE_MAX_CHARS {
            field.value = Some(value.chars().take(FILLED_VALUE_MAX_CHARS).collect());
            field.truncated = true;
        } else {
            field.value = Some(value);
        }
    }
    field
}

/// The evidence bundle a dry-run transact emits instead of acting: everything
/// a human needs to decide whether the flow, run again with approval, would do
/// the right thing.
#[derive(Debug, Clone, Serialize)]
pub struct TransactEvidence {
    /// Always `true` in this slice — no code path produces a live receipt.
    pub dry_run: bool,
    /// The caller's idempotency key, threaded through verbatim.
    pub idempotency_key: String,
    /// The session-vault profile the flow actually ran under, or `None` when it
    /// ran profile-less (the shared default Chrome). A reviewer approving a
    /// live submit is approving it *as this identity*, so the bundle names it.
    pub profile: Option<String>,
    /// Flow start URL and where the page actually ended up.
    pub url: String,
    pub final_url: Option<String>,
    /// Reversible steps the caller asked for (a `Repeat` counts as one).
    pub steps_requested: usize,
    /// Steps the executor actually reached; below `steps_requested` iff the
    /// flow's time budget ran out mid-list.
    pub steps_attempted: usize,
    /// Steps that **succeeded** — never a count of attempts. A flow whose
    /// selectors all missed reports `0` here with `steps_attempted` intact.
    pub steps_completed: usize,
    /// Per-step outcome, one entry per *attempted* step, in order.
    pub step_outcomes: Vec<StepOutcome>,
    /// `true` when the deadline stopped the step list before its end.
    pub steps_deadline_hit: bool,
    /// Outcome of the transact-level `wait_for_selector` (the confirmation
    /// state the flow was told to wait for): `Some(true)` it appeared,
    /// `Some(false)` it never did, `None` none was requested.
    pub wait_for_selector_found: Option<bool>,
    /// Live DOM values of every field the flow typed into.
    pub filled_fields: Vec<FilledField>,
    /// The exact irreversible action that was NOT performed.
    pub would_submit: PageAction,
    /// State of `would_submit`'s target element on the FINAL page — the one
    /// question a reviewer most needs answered before approving. `None` when
    /// the submit action targets no selector at all (a scroll/wait/repeat).
    pub submit_target: Option<SubmitTarget>,
    /// DOM snapshot at the stop point. Truncated (never dropped) when over the
    /// size cap — see `dom_truncated`.
    pub dom_html: String,
    /// Byte size of the DOM **as captured from the page**, before truncation.
    pub dom_bytes: usize,
    /// `true` when `dom_html` holds only a prefix of the captured DOM because
    /// it exceeded `max_body_bytes`. The flow already acted by capture time, so
    /// an over-cap DOM degrades the snapshot rather than destroying the bundle.
    pub dom_truncated: bool,
    /// Path to a screenshot of the stop state, when the engine can produce
    /// one. The current browser engine does not yet expose screenshot capture
    /// through its render path, so this is `None` — an honest gap, not a stub.
    pub screenshot_path: Option<String>,
    /// `true` when navigation timed out and the DOM was captured mid-load —
    /// the evidence may show a partial page.
    pub nav_timed_out: bool,
}

/// JS expression that reads the live values of `selectors` — the evidence
/// bundle's filled-field summary. Selectors are JSON-encoded into the script,
/// so quotes/backslashes in a CSS selector cannot break out of the literal.
///
/// **Secrets are masked here, in the page.** A password (or a credential/card
/// `autocomplete` field — see [`SENSITIVE_AUTOCOMPLETE_TOKENS`]) yields
/// `{value: null, redacted: true, value_len: <n>}`: the plaintext never leaves
/// the tab, so it cannot reach `evidence.json`, `jobs.result`, an SSE event or a
/// webhook payload even in principle. Everything else is capped at
/// [`FILLED_VALUE_MAX_CHARS`] with `truncated` set, so a filled `<textarea>`
/// cannot balloon the job's result payload.
pub fn filled_fields_js(selectors: &[String]) -> String {
    let sels = serde_json::to_string(selectors).unwrap_or_else(|_| "[]".into());
    // Compiled from the Rust const so the two predicates cannot drift.
    let sensitive =
        serde_json::to_string(SENSITIVE_AUTOCOMPLETE_TOKENS).unwrap_or_else(|_| "[]".into());
    let cap = FILLED_VALUE_MAX_CHARS;
    format!(
        "(() => {{ const sels = {sels}; const CAP = {cap}; const SENS = {sensitive}; \
           const secret = (el) => {{ \
             const t = String((el.getAttribute && el.getAttribute('type')) || el.type || '') \
               .toLowerCase(); \
             if (t === 'password') return true; \
             const ac = String((el.getAttribute && el.getAttribute('autocomplete')) || '') \
               .toLowerCase(); \
             return ac.split(/[\\s,]+/).some(tok => tok && SENS.indexOf(tok) >= 0); }}; \
           return sels.map(s => {{ \
             const el = document.querySelector(s); \
             if (!el) return {{selector: s, value: null, found: false, redacted: false, \
               value_len: null, truncated: false}}; \
             const raw = ('value' in el && el.value !== undefined && el.value !== null \
               && el.value !== '') ? String(el.value) : String(el.textContent || ''); \
             const len = raw.length; \
             if (secret(el)) return {{selector: s, value: null, found: true, redacted: true, \
               value_len: len, truncated: false}}; \
             if (len > CAP) return {{selector: s, value: raw.slice(0, CAP), found: true, \
               redacted: false, value_len: len, truncated: true}}; \
             return {{selector: s, value: (len === 0 ? null : raw), found: true, \
               redacted: false, value_len: len, truncated: false}}; }}); }})()"
    )
}

/// State of the irreversible action's target element on the page the flow
/// stopped at. "Does the button I would click actually exist, and could it be
/// clicked?" — the question a reviewer needs answered before approving.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmitTarget {
    /// `would_submit`'s CSS selector.
    pub selector: String,
    /// `Some(true)` the element was on the final page, `Some(false)` it was
    /// not, `None` the probe could not run (evaluate failed / page navigated
    /// away) — an honest "we don't know", never a fabricated "not found".
    pub found: Option<bool>,
    /// Rendered (non-zero box, not `display:none`/`visibility:hidden`/opacity 0).
    #[serde(default)]
    pub visible: Option<bool>,
    /// Not `disabled` and not `aria-disabled="true"`.
    #[serde(default)]
    pub enabled: Option<bool>,
    /// Lowercased tag name (`button`, `input`, …).
    #[serde(default)]
    pub tag: Option<String>,
    /// Trimmed visible label (inner text / value / `aria-label`), capped.
    #[serde(default)]
    pub label: Option<String>,
}

/// Max characters of a submit target's label kept in the evidence.
const SUBMIT_LABEL_MAX_CHARS: usize = 120;

/// JS expression that assesses one selector's element (exists / visible /
/// enabled / tag / label), or the literal `null` when there is no selector to
/// assess. The selector is JSON-encoded into the script, so quotes and
/// backslashes in a CSS selector cannot break out of the literal.
pub fn submit_target_js(selector: Option<&str>) -> String {
    let Some(selector) = selector else {
        return "null".to_string();
    };
    let sel = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    let cap = SUBMIT_LABEL_MAX_CHARS;
    format!(
        "(() => {{ const s = {sel}; const el = document.querySelector(s); \
           if (!el) return {{selector: s, found: false, visible: null, enabled: null, \
             tag: null, label: null}}; \
           const r = el.getBoundingClientRect(); const cs = window.getComputedStyle(el); \
           const visible = (r.width > 0 || r.height > 0) && cs.visibility !== 'hidden' \
             && cs.display !== 'none' && cs.opacity !== '0'; \
           const enabled = el.disabled !== true && el.getAttribute('aria-disabled') !== 'true'; \
           const label = String(el.innerText || el.value || el.getAttribute('aria-label') || '') \
             .trim().slice(0, {cap}); \
           return {{selector: s, found: true, visible: visible, enabled: enabled, \
             tag: String(el.tagName || '').toLowerCase(), label: label}}; }})()"
    )
}

/// The transact evidence probe: ONE `evaluate` expression that captures both
/// the filled-field summary and the submit target's state, since the render
/// path exposes a single evaluate slot. Shape:
/// `{fields: [FilledField...], submit_target: SubmitTarget|null}`.
pub fn transact_probe_js(fill_selectors: &[String], submit_selector: Option<&str>) -> String {
    let fields = filled_fields_js(fill_selectors);
    let target = submit_target_js(submit_selector);
    format!("(() => ({{ fields: {fields}, submit_target: {target} }}))()")
}

/// Decodes a [`transact_probe_js`] result into its typed halves. Every failure
/// mode degrades honestly instead of failing the bundle: fields fall back to
/// "nothing found" rows, and an unassessable target reports `found: None`
/// ("we could not look") rather than `found: false` ("it is not there").
pub fn parse_transact_probe(
    fill_selectors: &[String],
    submit_selector: Option<&str>,
    evaluated: Option<&Value>,
) -> (Vec<FilledField>, Option<SubmitTarget>) {
    let fields = parse_filled_fields(fill_selectors, evaluated.and_then(|v| v.get("fields")));
    let target = submit_selector.map(|selector| {
        evaluated
            .and_then(|v| v.get("submit_target"))
            .and_then(|t| serde_json::from_value::<SubmitTarget>(t.clone()).ok())
            .unwrap_or(SubmitTarget {
                selector: selector.to_string(),
                found: None,
                visible: None,
                enabled: None,
                tag: None,
                label: None,
            })
    });
    (fields, target)
}

/// Decodes the result of [`filled_fields_js`] back into typed fields. A missing
/// or malformed result (evaluate failed, page navigated away) degrades to
/// "nothing found" rows rather than failing the whole evidence bundle.
///
/// Every decoded row passes through [`redact_field`], so the capture contract
/// (a redacted field carries no value; values are length-capped) holds on the
/// decode side too — not only where the page happened to honor it.
pub fn parse_filled_fields(selectors: &[String], evaluated: Option<&Value>) -> Vec<FilledField> {
    if let Some(v) = evaluated {
        if let Ok(fields) = serde_json::from_value::<Vec<FilledField>>(v.clone()) {
            return fields.into_iter().map(redact_field).collect();
        }
    }
    selectors.iter().map(FilledField::not_found).collect()
}

/// One same-origin JSON response observed by the browser tier while rendering a
/// page with [`RenderRequest::capture_network`] set — the raw material of the
/// API X-ray (discovering the data API behind a SPA). Bodies are size-capped by
/// the engine (per response and in total), so a capture can never balloon a
/// render's memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedCall {
    /// Full request URL (with query string) as the page issued it.
    pub url: String,
    /// HTTP method (`GET`, `POST`, ...).
    pub method: String,
    /// Response status code.
    pub status: u16,
    /// Response `Content-Type` / MIME type as reported by CDP.
    pub content_type: String,
    /// Parsed JSON response body. Responses that fail to parse as JSON, exceed
    /// the per-body cap, or arrive after the total budget is spent are dropped
    /// (never truncated into invalid JSON).
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenderRequest {
    pub url: String,
    /// CSS selector to wait for before capturing the DOM.
    #[serde(default)]
    pub wait_for_selector: Option<String>,
    /// Scripted page actions (scroll/click/type/wait) run before capture — drives
    /// infinite-scroll and "load more" pages the one-shot render can't reach.
    /// Empty (default) = exactly the previous one-shot behavior.
    #[serde(default)]
    pub actions: Vec<PageAction>,
    /// Extra settle time; falls back to the configured default.
    #[serde(default)]
    pub extra_wait_ms: Option<u64>,
    /// JS expression evaluated after load; its JSON result lands in
    /// [`RenderedPage::evaluated`].
    #[serde(default)]
    pub evaluate: Option<String>,
    /// Opt this render out of resource blocking (`[browser] block_resources`):
    /// load images/fonts/media too. Ignored when blocking is disabled globally.
    #[serde(default)]
    pub load_all_resources: bool,
    /// Session-vault profile to render under: Chrome is acquired with
    /// `<profiles_dir>/<name>/browser` as its user-data-dir, so that profile's
    /// logins/cookies are in effect. `None` renders on the shared default
    /// instance (`[browser] user_data_dir`) — exactly the previous behavior.
    #[serde(default)]
    pub profile: Option<String>,
    /// Cap on the captured HTML size (bytes); over-cap renders fail instead of
    /// buffering an unbounded DOM. `None` falls back to `[browser] max_html_bytes`
    /// — the browser-tier mirror of `HttpRequest.max_body_bytes`.
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
    /// Capture same-origin JSON network responses observed during the render
    /// into [`RenderedPage::network`] (per-request opt-in; the API X-ray seam).
    /// `false` (default) = no CDP network capture — exactly the previous
    /// behavior.
    #[serde(default)]
    pub capture_network: bool,
}

impl RenderRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            wait_for_selector: None,
            actions: Vec::new(),
            extra_wait_ms: None,
            evaluate: None,
            load_all_resources: false,
            profile: None,
            max_body_bytes: None,
            capture_network: false,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RenderedPage {
    pub html: String,
    pub final_url: Option<String>,
    pub evaluated: Option<Value>,
    /// `true` when the navigation-wait deadline elapsed and the DOM was captured
    /// mid-load — the HTML may be partial. Distinguishes an honest timeout from a
    /// clean load.
    pub nav_timed_out: bool,
    /// Outcome of a `wait_for_selector`: `Some(true)` the selector appeared,
    /// `Some(false)` it never did before the deadline, `None` no selector was
    /// requested.
    pub selector_found: Option<bool>,
    /// Count of subresources (images/fonts/media) dropped by request interception
    /// for this render. `0` when blocking is off or the render opted out.
    pub blocked_resources: usize,
    /// Number of scripted [`PageAction`]s the executor **reached** before
    /// capture (a `Repeat` counts as one). `0` when none were requested — lets a
    /// caller see that an infinite-scroll script actually executed rather than
    /// silently no-op'd. Deliberately an *attempt* count: the render ladder has
    /// always read it that way. For "how many worked", see [`Self::action_outcomes`].
    pub actions_completed: usize,
    /// Per-action outcome, one entry per attempted action, in order (so
    /// `action_outcomes.len() == actions_completed`). Empty when no actions were
    /// requested. Renders may ignore it; the transact path turns it into the
    /// evidence bundle's honest requested/attempted/succeeded accounting.
    pub action_outcomes: Vec<StepOutcome>,
    /// Same-origin JSON responses captured during the render. Empty unless the
    /// request set [`RenderRequest::capture_network`]; size-capped by the engine.
    pub network: Vec<CapturedCall>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResearchRequest {
    pub prompt: String,
    #[serde(default)]
    pub append_system_prompt: Option<String>,
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Named preset from `[claude.roles]` — e.g. "research" or "compose".
    #[serde(default)]
    pub role: Option<String>,
    /// Explicit model id/alias; overrides the role and config default.
    #[serde(default)]
    pub model: Option<String>,
    /// Explicit reasoning effort (low|medium|high|xhigh|max); overrides role.
    #[serde(default)]
    pub effort: Option<String>,
    /// Hard spend ceiling for this run.
    #[serde(default)]
    pub max_budget_usd: Option<f64>,
    /// Resume a prior CLI session id for multi-step research pipelines.
    #[serde(default)]
    pub resume_session: Option<String>,
    /// Constrain the final answer to this JSON schema (`--json-schema`).
    #[serde(default)]
    pub json_schema: Option<Value>,
}

impl ResearchRequest {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt: prompt.into(),
            ..Default::default()
        }
    }

    /// Selects a named role preset (e.g. "research", "compose").
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchOutput {
    /// Final response text from the agent.
    pub text: String,
    /// Populated when the response parses as JSON (fenced JSON is unwrapped).
    pub json: Option<Value>,
    pub cost_usd: Option<f64>,
    pub duration_ms: Option<u64>,
    pub num_turns: Option<u64>,
    pub session_id: Option<String>,
}

/// Plain HTTP fetching — fast path for server-rendered pages and APIs.
#[async_trait]
pub trait HttpClient: Send + Sync {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse>;

    /// Fetches a **raw binary body** (ZIP/PDF/…) as bytes, hard-capped by the
    /// same `max_body_bytes` machinery as [`fetch`](Self::fetch).
    ///
    /// This is the deliberately minimal engine-traits#2-LITE seam: no charset
    /// decoding, no response cache (the cache stores decoded text), buffered in
    /// memory (NOT streamed to disk — the full streaming binary-body design
    /// stays deferred). Default: unsupported, so only engines that opt in
    /// (currently `pumper-engine-http`) carry binary fetching; wrappers/mocks
    /// keep compiling and fail loudly if a binary fetch reaches them.
    async fn fetch_bytes(&self, req: HttpRequest) -> Result<Vec<u8>> {
        Err(Error::Http(format!(
            "this engine does not support binary fetch_bytes ({})",
            req.url
        )))
    }
}

/// The refusal an engine with no flow support owes a caller — one producer, so
/// a wrapper that wants to refuse explicitly cannot accidentally mint a
/// *retryable* version of the same sentence.
///
/// Terminal by construction: see [`Browser::transact`]'s default body for why
/// [`Error::Transact`] is the honest variant here.
pub fn unsupported_transact(url: &str) -> Error {
    Error::Transact(format!(
        "this engine does not support transact flows ({url})"
    ))
}

/// Headless-browser rendering — JS-heavy pages, logged-in sessions.
#[async_trait]
pub trait Browser: Send + Sync {
    async fn render(&self, req: RenderRequest) -> Result<RenderedPage>;

    /// Executes a declarative [`TransactRequest`] **dry-run only**: the
    /// reversible steps run to the final confirmation state, the flow STOPS
    /// before the irreversible `submit_action`, and an evidence bundle comes
    /// back for human review. Default: unsupported — only engines that opt in
    /// (currently `pumper-engine-browser`) can execute flows; wrappers/mocks
    /// keep compiling and fail loudly if a transact reaches them.
    ///
    /// The refusal is [`Error::Transact`], not [`Error::Browser`], because it is
    /// **deterministic**: which engine sits behind the trait object is fixed for
    /// the life of the job, so every attempt reaches this identical refusal
    /// before touching a browser. As an `Error::Browser` it was retryable
    /// ([`Error::is_terminal_for_job`]), and a job that reached an engine
    /// without flow support burned its whole backoff ladder producing the same
    /// sentence four times. It sits exactly on the boundary that variant already
    /// documents: a failure *during* a flow stays an `Error::Browser`; only a
    /// pre-flight refusal is typed `Transact`.
    async fn transact(&self, req: TransactRequest) -> Result<TransactEvidence> {
        Err(unsupported_transact(&req.url))
    }
}

/// Agentic web research via Claude Code CLI.
#[async_trait]
pub trait Researcher: Send + Sync {
    async fn research(&self, req: ResearchRequest) -> Result<ResearchOutput>;
}

/// Everything an app can scrape with, handed over via [`crate::AppContext`].
///
/// **The `claude` engine is deliberately not public.** Every model call must go
/// through [`crate::AppContext::research`], which adds the research cache, the
/// per-job budget governor and cost metering; a direct
/// `ctx.engines.claude.research(...)` silently loses all three (it happened —
/// `connector-api-watch` summarized every doc diff off-ledger). Field privacy is
/// what makes the chokepoint structural rather than conventional: an app crate
/// cannot name the researcher, so it cannot bypass the wrapper. Construct with
/// [`EngineSet::new`]; core-internal consumers use [`EngineSet::researcher`].
pub struct EngineSet {
    pub http: Arc<dyn HttpClient>,
    pub browser: Arc<dyn Browser>,
    pub(crate) claude: Arc<dyn Researcher>,
    /// Tiered fetcher that picks/escalates engines automatically.
    pub fetch: crate::fetcher::Fetcher,
}

impl EngineSet {
    /// Assembles the engine set. The researcher is moved in and thereafter
    /// reachable only through the metered chokepoint.
    pub fn new(
        http: Arc<dyn HttpClient>,
        browser: Arc<dyn Browser>,
        claude: Arc<dyn Researcher>,
        fetch: crate::fetcher::Fetcher,
    ) -> Self {
        Self {
            http,
            browser,
            claude,
            fetch,
        }
    }

    /// The raw researcher — **core-internal**, for the one caller that is itself
    /// the chokepoint ([`crate::AppContext::research`]). Named rather than
    /// field-public so the bypass surface is a single greppable symbol.
    pub(crate) fn researcher(&self) -> &Arc<dyn Researcher> {
        &self.claude
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_request_actions_default_empty_and_deserialize() {
        // Absent `actions` = one-shot render (exactly the previous behavior).
        let r: RenderRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(r.actions.is_empty());

        // The documented infinite-scroll script deserializes into the enum.
        let r: RenderRequest = serde_json::from_str(
            r##"{"url":"https://x/","actions":[
                {"action":"repeat","times":5,
                 "steps":[{"action":"scroll_bottom"},{"action":"wait_ms","ms":800}],
                 "until_selector_count_stable":".row"},
                {"action":"click","selector":"#more"}
            ]}"##,
        )
        .unwrap();
        assert_eq!(r.actions.len(), 2);
        match &r.actions[0] {
            PageAction::Repeat {
                times,
                steps,
                until_selector_count_stable,
            } => {
                assert_eq!(*times, 5);
                assert_eq!(steps.len(), 2);
                assert!(matches!(steps[0], PageAction::ScrollBottom));
                assert_eq!(until_selector_count_stable.as_deref(), Some(".row"));
            }
            other => panic!("expected Repeat, got {other:?}"),
        }
        assert!(matches!(&r.actions[1], PageAction::Click { selector } if selector == "#more"));
    }

    #[test]
    fn capture_network_is_serde_defaulted_and_round_trips() {
        // Older payloads omit it => false => no capture (previous behavior).
        let r: RenderRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(!r.capture_network);
        assert!(!RenderRequest::new("https://x/").capture_network);
        // Present => round-trips; a captured call deserializes.
        let r: RenderRequest =
            serde_json::from_str(r#"{"url":"https://x/","capture_network":true}"#).unwrap();
        assert!(r.capture_network);
        let call: CapturedCall = serde_json::from_str(
            r#"{"url":"https://x/api?q=1","method":"GET","status":200,
                "content_type":"application/json","body":{"items":[1,2]}}"#,
        )
        .unwrap();
        assert_eq!(call.status, 200);
        assert_eq!(call.body["items"][0], 1);
        // RenderedPage default carries no captures.
        assert!(RenderedPage::default().network.is_empty());
    }

    #[test]
    fn http_request_conditional_validators_are_serde_defaulted() {
        // Older payloads (and the common case) omit the conditional fields.
        let req: HttpRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(req.etag.is_none());
        assert!(req.if_modified_since.is_none());
        // When present they round-trip.
        let req2: HttpRequest = serde_json::from_str(
            r#"{"url":"https://x/","etag":"\"abc\"","if_modified_since":"Wed, 21 Oct 2025 07:28:00 GMT"}"#,
        )
        .unwrap();
        assert_eq!(req2.etag.as_deref(), Some("\"abc\""));
        assert_eq!(
            req2.if_modified_since.as_deref(),
            Some("Wed, 21 Oct 2025 07:28:00 GMT")
        );
        // The convenience constructor leaves them unset.
        assert!(HttpRequest::get("https://x/").etag.is_none());
    }

    #[test]
    fn profile_is_serde_defaulted_on_every_request_type() {
        // None = today's behavior; omitted from older payloads.
        let h: HttpRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(h.profile.is_none());
        let r: RenderRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(r.profile.is_none());
        // Present => round-trips.
        let h2: HttpRequest =
            serde_json::from_str(r#"{"url":"https://x/","profile":"acme_login"}"#).unwrap();
        assert_eq!(h2.profile.as_deref(), Some("acme_login"));
        let r2: RenderRequest =
            serde_json::from_str(r#"{"url":"https://x/","profile":"acme_login"}"#).unwrap();
        assert_eq!(r2.profile.as_deref(), Some("acme_login"));
        assert!(HttpRequest::get("https://x/").profile.is_none());
        assert!(RenderRequest::new("https://x/").profile.is_none());
    }

    #[test]
    fn archive_max_age_is_serde_defaulted_and_round_trips() {
        // Older payloads omit it => None => live-only (previous behavior).
        let req: HttpRequest = serde_json::from_str(r#"{"url":"https://x/"}"#).unwrap();
        assert!(req.archive_max_age.is_none());
        assert!(HttpRequest::get("https://x/").archive_max_age.is_none());
        // Present => round-trips.
        let req: HttpRequest =
            serde_json::from_str(r#"{"url":"https://x/","archive_max_age":86400}"#).unwrap();
        assert_eq!(req.archive_max_age, Some(86_400));
    }

    #[test]
    fn profile_names_accept_only_the_path_safe_alphabet() {
        for ok in [
            "a",
            "acme",
            "acme-login",
            "acme_login_2",
            "A1",
            &"x".repeat(64),
        ] {
            assert!(
                validate_profile_name(ok).is_ok(),
                "{ok:?} should be accepted"
            );
        }
        // Traversal, separators, and anything else are typed errors.
        for bad in [
            "",
            "..",
            ".",
            "a/b",
            "a\\b",
            "a.b",
            "C:",
            "a b",
            "naïve",
            "a:b",
            "-*-",
            &"x".repeat(65),
        ] {
            let err = validate_profile_name(bad).unwrap_err();
            assert!(matches!(err, Error::Profile(_)), "{bad:?} => {err:?}");
        }
    }

    fn dry_run_flow() -> TransactRequest {
        serde_json::from_str(
            r##"{"url":"https://portal.example/signup",
                 "idempotency_key":"signup-2026-07-31",
                 "steps":[
                   {"action":"type","selector":"#email","text":"a@b.c"},
                   {"action":"click","selector":"#next"},
                   {"action":"wait_for_selector","selector":"#confirm"}],
                 "submit_action":{"action":"click","selector":"#confirm-submit"}}"##,
        )
        .unwrap()
    }

    #[test]
    fn transact_request_defaults_to_dry_run_and_validates() {
        let req = dry_run_flow();
        // Omitted `submit` => false => dry-run: the ONLY mode this slice runs.
        assert!(!req.submit);
        assert!(req.validate().is_ok());
        assert_eq!(req.steps.len(), 3);
        assert!(
            matches!(&req.submit_action, PageAction::Click { selector } if selector == "#confirm-submit")
        );
    }

    #[test]
    fn submit_true_is_rejected_with_a_typed_error_naming_the_next_slice() {
        let mut req = dry_run_flow();
        req.submit = true;
        let err = req.validate().unwrap_err();
        // Typed (not a generic App/Browser error), and the message points the
        // caller at the human-approval design rather than a dead end.
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("human-approval"),
            "message must name the next slice: {msg}"
        );
        assert!(
            msg.contains("dry-run"),
            "message must explain what v1 does: {msg}"
        );
    }

    #[test]
    fn empty_idempotency_key_and_bad_profile_are_typed_rejections() {
        let mut req = dry_run_flow();
        req.idempotency_key = "  ".into();
        assert!(matches!(req.validate().unwrap_err(), Error::Transact(_)));
        let mut req = dry_run_flow();
        req.profile = Some("../escape".into());
        assert!(matches!(req.validate().unwrap_err(), Error::Profile(_)));
        // A valid profile threads through fine.
        let mut req = dry_run_flow();
        req.profile = Some("acme_login".into());
        assert!(req.validate().is_ok());
    }

    /// The EXPECTED-diff idiom: serde is the source of truth for what a
    /// `TransactRequest` accepts, so the allowlist is compared against a
    /// serialized request's own keys. Adding a field without listing it here
    /// would make the new field itself "unknown" and rejected at the door.
    #[test]
    fn transact_fields_match_the_request_struct() {
        let req = dry_run_flow();
        let serialized = serde_json::to_value(&req).unwrap();
        let mut actual: Vec<String> = serialized
            .as_object()
            .expect("a request serializes to an object")
            .keys()
            .cloned()
            .collect();
        actual.sort();
        let mut expected: Vec<String> = TRANSACT_FIELDS.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(
            actual, expected,
            "TRANSACT_FIELDS drifted from TransactRequest — update the const"
        );
    }

    /// The anti-pattern: serde silently drops what it does not know, so a
    /// typo'd `"step"` passed the schema, deserialized into an EMPTY step list,
    /// and ran a zero-step flow that produced a plausible landing-page bundle.
    #[test]
    fn unknown_field_not_silently_dropped() {
        let params = serde_json::json!({
            "url": "https://x/", "idempotency_key": "k",
            "submit_action": {"action": "click", "selector": "#go"},
            "step": [{"action": "click", "selector": "#next"}],
            "sumbit": false
        });
        let mut unknown = unknown_transact_fields(&params);
        unknown.sort();
        assert_eq!(unknown, vec!["step".to_string(), "sumbit".to_string()]);
        // Proof of the harm: the typo'd flow really does deserialize to zero
        // steps, which is why "silently dropped" was never survivable here.
        let req: TransactRequest = serde_json::from_value(params).unwrap();
        assert!(req.steps.is_empty());

        // Host-injected envelopes are allowed: trigger-fired jobs carry
        // `_trigger` in their params and bypass the enqueue-time validator, so
        // denying unknown keys outright would break every triggered transact.
        let triggered = serde_json::json!({
            "url": "https://x/", "idempotency_key": "k",
            "submit_action": {"action": "click", "selector": "#go"},
            "_trigger": {"depth": 1, "chain": ["T1"]}
        });
        assert!(unknown_transact_fields(&triggered).is_empty());
        assert!(serde_json::from_value::<TransactRequest>(triggered).is_ok());
        // A non-object payload has no keys to judge (serde rejects it anyway).
        assert!(unknown_transact_fields(&serde_json::json!("nope")).is_empty());
    }

    /// The anti-pattern: `Some(profile)` went straight to `create_dir_all`, so a
    /// typo'd profile name silently created an empty, logged-OUT Chrome profile
    /// and the flow ran against a login wall — emitting a plausible evidence
    /// bundle of entirely the wrong page.
    #[test]
    fn missing_profile_not_silently_created() {
        assert!(require_existing_profile("portal_login", true).is_ok());
        let err = require_existing_profile("prtal_login", false).unwrap_err();
        // Typed Transact => terminal for the job: a refusal fails ONCE.
        assert!(matches!(err, Error::Transact(_)), "got {err:?}");
        assert!(err.is_terminal_for_job());
        let msg = err.to_string();
        assert!(msg.contains("prtal_login"), "names the profile: {msg}");
        assert!(msg.contains("/profiles"), "names the surface: {msg}");
    }

    #[test]
    fn fill_selectors_recurse_through_repeat_and_dedupe() {
        let mut req = dry_run_flow();
        req.steps.push(PageAction::Repeat {
            times: 2,
            steps: vec![
                PageAction::Type {
                    selector: "#email".into(), // duplicate of the top-level fill
                    text: "x".into(),
                },
                PageAction::Type {
                    selector: "#org".into(),
                    text: "Acme".into(),
                },
            ],
            until_selector_count_stable: None,
        });
        assert_eq!(
            req.fill_selectors(),
            vec!["#email".to_string(), "#org".to_string()]
        );
    }

    #[test]
    fn filled_fields_js_json_encodes_selectors_and_parse_round_trips() {
        // A hostile selector's quotes/backslashes stay inside the JSON literal.
        let sels = vec![r#"input[name="a\"b"]"#.to_string()];
        let js = filled_fields_js(&sels);
        assert!(
            js.contains(r#"\"a\\\"b\""#),
            "selector must be JSON-escaped: {js}"
        );
        // The evaluate result decodes into typed fields.
        let evaluated = serde_json::json!([
            {"selector": "#email", "value": "a@b.c", "found": true},
            {"selector": "#gone", "value": null, "found": false}
        ]);
        let fields = parse_filled_fields(&["#email".into(), "#gone".into()], Some(&evaluated));
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].value.as_deref(), Some("a@b.c"));
        assert!(!fields[1].found);
        // A failed/malformed evaluate degrades to not-found rows, never an error.
        let fields = parse_filled_fields(&["#email".into()], None);
        assert_eq!(fields, vec![FilledField::not_found("#email")]);
        let fields = parse_filled_fields(&["#email".into()], Some(&serde_json::json!("nonsense")));
        assert!(!fields[0].found);
    }

    /// The anti-pattern this exists to kill: a flow that types a password
    /// captured that password's live value into `filled_fields[].value`, which
    /// lands in `evidence.json` on disk, the persisted job result, every SSE
    /// subscriber, and every webhook/HMAC callback payload. The summary must
    /// prove the field was filled WITHOUT republishing what was typed into it.
    #[test]
    fn password_value_not_republished() {
        // The predicate: password inputs and credential/card autocomplete hints.
        assert!(is_sensitive_input("password", ""));
        assert!(is_sensitive_input("PASSWORD", ""));
        assert!(is_sensitive_input("text", "current-password"));
        assert!(is_sensitive_input("text", "section-a billing cc-number"));
        assert!(is_sensitive_input("tel", "ONE-TIME-CODE"));
        for benign in [("text", ""), ("email", "email"), ("text", "cc-type")] {
            assert!(
                !is_sensitive_input(benign.0, benign.1),
                "{benign:?} is not a secret — over-redacting blinds the reviewer"
            );
        }

        // The capture script masks in the PAGE: the plaintext never leaves it.
        let js = filled_fields_js(&["#pw".into()]);
        assert!(js.contains("'password'"), "type check compiled in: {js}");
        assert!(
            js.contains("current-password") && js.contains("cc-number"),
            "the token list is compiled from the Rust const, so it can't drift"
        );
        assert!(js.contains("redacted: true"));

        // Decode-side enforcement: even if a page (or a drifting script) hands
        // back a value ALONGSIDE redacted:true, the value is dropped.
        let evaluated = serde_json::json!([
            {"selector": "#pw", "value": "hunter2", "found": true,
             "redacted": true, "value_len": 7, "truncated": false}
        ]);
        let fields = parse_filled_fields(&["#pw".into()], Some(&evaluated));
        assert_eq!(fields[0].value, None, "the secret is never republished");
        assert!(
            fields[0].redacted,
            "but the reviewer still sees it was filled"
        );
        assert_eq!(fields[0].value_len, Some(7), "with a length-only hint");
        assert!(fields[0].found);
        assert!(
            !serde_json::to_string(&fields).unwrap().contains("hunter2"),
            "no serialization of the bundle may carry the plaintext"
        );
    }

    /// The sibling leak: a filled `<textarea>` is unbounded page-controlled
    /// input, and mirroring it whole into `jobs.result` balloons every payload
    /// derived from it.
    #[test]
    fn oversized_field_value_not_republished_whole() {
        let long = "x".repeat(FILLED_VALUE_MAX_CHARS + 100);
        let evaluated = serde_json::json!([
            {"selector": "#bio", "value": long, "found": true}
        ]);
        let fields = parse_filled_fields(&["#bio".into()], Some(&evaluated));
        assert_eq!(fields[0].value.as_deref().unwrap().chars().count(), 512);
        assert!(fields[0].truncated, "the cut is visible, not silent");
        assert_eq!(
            fields[0].value_len,
            Some(FILLED_VALUE_MAX_CHARS + 100),
            "the real length is still reported"
        );
        // Exactly at the cap is kept whole (strict-over, like every other cap).
        let at_cap = "y".repeat(FILLED_VALUE_MAX_CHARS);
        let evaluated = serde_json::json!([{"selector": "#bio", "value": at_cap, "found": true}]);
        let fields = parse_filled_fields(&["#bio".into()], Some(&evaluated));
        assert!(!fields[0].truncated);
        // Multi-byte values are cut on CHARACTERS, never mid-codepoint.
        let wide = "é".repeat(FILLED_VALUE_MAX_CHARS + 1);
        let evaluated = serde_json::json!([{"selector": "#bio", "value": wide, "found": true}]);
        let fields = parse_filled_fields(&["#bio".into()], Some(&evaluated));
        assert_eq!(fields[0].value.as_deref().unwrap().chars().count(), 512);
    }

    /// Old payloads (`{selector, value, found}`) predate the redaction fields
    /// and must keep decoding — the shape grew, it did not break.
    #[test]
    fn filled_field_stays_backward_compatible_on_the_wire() {
        let old: FilledField =
            serde_json::from_str(r##"{"selector":"#email","value":"a@b.c","found":true}"##)
                .unwrap();
        assert_eq!(old.value.as_deref(), Some("a@b.c"));
        assert!(!old.redacted && !old.truncated && old.value_len.is_none());
        // And the new shape round-trips through JSON unchanged.
        let round: FilledField =
            serde_json::from_value(serde_json::to_value(redact_field(old)).unwrap()).unwrap();
        assert_eq!(round.value_len, Some(5));
    }

    /// The anti-pattern: the executor counted every action it *reached* as
    /// completed, so three missed selectors read as "3 steps completed".
    /// Attempts, successes and the requested count are three different numbers.
    #[test]
    fn attempted_steps_not_counted_as_completed() {
        use StepOutcome::*;
        // Every step missed: attempted 3, completed 0 — never 3.
        let s = summarize_steps(3, &[SelectorMissing, SelectorMissing, SelectorMissing]);
        assert_eq!((s.requested, s.attempted, s.completed), (3, 3, 0));
        assert!(!s.deadline_hit, "the list ran to its end, it just failed");
        // Mixed run: only the Ok rows count.
        let s = summarize_steps(4, &[Ok, ActionFailed, Ok, Partial]);
        assert_eq!((s.requested, s.attempted, s.completed), (4, 4, 2));
        // Deadline cut the list short: attempted < requested is the ONLY signal
        // for that, so it is reported explicitly.
        let s = summarize_steps(5, &[Ok, Ok]);
        assert_eq!((s.requested, s.attempted, s.completed), (5, 2, 2));
        assert!(s.deadline_hit);
        // A clean run is unambiguous.
        let s = summarize_steps(2, &[Ok, Ok]);
        assert_eq!((s.requested, s.attempted, s.completed), (2, 2, 2));
        assert!(!s.deadline_hit);
        // No steps requested is not a failure.
        assert_eq!(summarize_steps(0, &[]).completed, 0);
        assert!(!summarize_steps(0, &[]).deadline_hit);
    }

    #[test]
    fn interaction_outcome_separates_a_missing_selector_from_a_failed_action() {
        assert_eq!(interaction_outcome(true, true), StepOutcome::Ok);
        assert_eq!(interaction_outcome(true, false), StepOutcome::ActionFailed);
        assert_eq!(
            interaction_outcome(false, false),
            StepOutcome::SelectorMissing
        );
        // "not found" wins even if the caller claims it acted — it cannot have.
        assert_eq!(
            interaction_outcome(false, true),
            StepOutcome::SelectorMissing
        );
        assert!(StepOutcome::Ok.is_ok());
        for not_ok in [
            StepOutcome::SelectorMissing,
            StepOutcome::ActionFailed,
            StepOutcome::Partial,
        ] {
            assert!(!not_ok.is_ok(), "{not_ok:?} is not a success");
        }
        // The taxonomy is a stable wire contract (evidence.json + job results).
        assert_eq!(
            serde_json::to_value(StepOutcome::SelectorMissing).unwrap(),
            serde_json::json!("selector_missing")
        );
    }

    #[test]
    fn a_repeat_pass_only_counts_clean_when_every_inner_step_succeeded() {
        use StepOutcome::*;
        assert!(pass_fully_succeeded(2, &[Ok, Ok]));
        assert!(pass_fully_succeeded(0, &[]));
        // One inner miss taints the whole pass (the block outcome is coarse).
        assert!(!pass_fully_succeeded(2, &[Ok, SelectorMissing]));
        // Deadline cut the pass short: fewer outcomes than requested steps.
        assert!(!pass_fully_succeeded(3, &[Ok, Ok]));
    }

    #[test]
    fn only_element_targeting_actions_expose_a_selector() {
        assert_eq!(
            PageAction::Click {
                selector: "#go".into()
            }
            .selector(),
            Some("#go")
        );
        assert_eq!(
            PageAction::Type {
                selector: "#email".into(),
                text: "a".into()
            }
            .selector(),
            Some("#email")
        );
        assert_eq!(
            PageAction::WaitForSelector {
                selector: "#panel".into(),
                timeout_ms: None
            }
            .selector(),
            Some("#panel")
        );
        assert_eq!(PageAction::ScrollBottom.selector(), None);
        assert_eq!(PageAction::ScrollBy { pixels: 10 }.selector(), None);
        assert_eq!(PageAction::WaitMs { ms: 5 }.selector(), None);
        assert_eq!(
            PageAction::Repeat {
                times: 2,
                steps: vec![],
                until_selector_count_stable: None
            }
            .selector(),
            None
        );
    }

    /// The anti-pattern: the evidence echoed `would_submit` back verbatim and
    /// never asked whether that button is on the final page — the single
    /// question a reviewer needs answered before approving a live submit.
    #[test]
    fn submit_target_probe_reports_unknown_not_missing_when_it_cannot_look() {
        let js = transact_probe_js(&["#email".into()], Some(r#"button[data-x="a\"b"]"#));
        assert!(
            js.contains("fields:"),
            "one evaluate carries both halves: {js}"
        );
        assert!(js.contains("submit_target:"));
        assert!(
            js.contains(r#"\"a\\\"b\""#),
            "the submit selector is JSON-escaped into the literal: {js}"
        );

        // A live probe result decodes into both halves.
        let evaluated = serde_json::json!({
            "fields": [{"selector": "#email", "value": "a@b.c", "found": true}],
            "submit_target": {"selector": "#submit", "found": true, "visible": true,
                              "enabled": false, "tag": "button", "label": "Confirm"}
        });
        let (fields, target) =
            parse_transact_probe(&["#email".into()], Some("#submit"), Some(&evaluated));
        assert_eq!(fields[0].value.as_deref(), Some("a@b.c"));
        let target = target.expect("a selector was assessed");
        assert_eq!(target.found, Some(true));
        assert_eq!(target.enabled, Some(false), "a disabled button is reported");
        assert_eq!(target.label.as_deref(), Some("Confirm"));

        // Probe failed (evaluate threw / page navigated): "we could not look",
        // NOT "the button is not there" — the two must never be conflated.
        let (fields, target) = parse_transact_probe(&["#email".into()], Some("#submit"), None);
        assert!(!fields[0].found);
        assert_eq!(target.expect("still reported").found, None);

        // No selector to assess at all (a scroll/wait submit action).
        let (_, target) = parse_transact_probe(&[], None, Some(&evaluated));
        assert!(target.is_none());
        assert_eq!(submit_target_js(None), "null");
    }

    #[test]
    fn profile_paths_stay_inside_the_profiles_dir() {
        let root = Path::new("data/profiles");
        assert_eq!(profile_dir(root, "acme").unwrap(), root.join("acme"));
        assert_eq!(
            profile_cookies_path(root, "acme").unwrap(),
            root.join("acme").join("cookies.json")
        );
        assert_eq!(
            profile_browser_dir(root, "acme").unwrap(),
            root.join("acme").join("browser")
        );
        // A traversal attempt never produces a path at all.
        assert!(profile_cookies_path(root, "../../etc").is_err());
        assert!(profile_browser_dir(root, "..").is_err());
    }

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The anti-pattern: `FETCHED_VIA_HEADER` and `SNAPSHOT_TS_HEADER` shipped
    /// with a doc comment promising provenance "survives into records", one
    /// writer, and **zero readers** anywhere in the workspace. A constant whose
    /// only reader is its own definition documents an intention, not a contract.
    ///
    /// This test is that reader's guard: the names the archive engine writes are
    /// the names this seam reads, and the capture time comes back with them.
    #[test]
    fn snapshot_provenance_is_not_a_constant_with_no_reader() {
        let got = snapshot_provenance(&headers(&[
            (FETCHED_VIA_HEADER, "archive"),
            (SNAPSHOT_TS_HEADER, "2019-03-11T09:15:00+00:00"),
            ("content-type", "text/html"),
        ]))
        .expect("a marked response carries provenance");
        assert_eq!(got.via, "archive");
        assert_eq!(
            got.captured_at.as_deref(),
            Some("2019-03-11T09:15:00+00:00")
        );
        // The whole point of the timestamp: the note names the day, not just
        // the fact that a store answered.
        assert!(
            got.note().contains("2019-03-11"),
            "note was {:?}",
            got.note()
        );
    }

    /// A live response carries neither header, and a marker with no value is
    /// not provenance — otherwise an empty string would read as a store name
    /// and every consumer would branch the wrong way on it.
    #[test]
    fn an_unmarked_or_blank_response_reports_no_provenance() {
        assert!(snapshot_provenance(&headers(&[("content-type", "text/html")])).is_none());
        assert!(snapshot_provenance(&headers(&[(FETCHED_VIA_HEADER, "   ")])).is_none());
        // A capture time with no `via` is not enough to claim a stored body.
        assert!(
            snapshot_provenance(&headers(&[(SNAPSHOT_TS_HEADER, "2019-03-11T00:00:00Z")]))
                .is_none()
        );
    }

    /// Header casing is not preserved by real HTTP stacks, so an exact-match
    /// lookup would silently report "live" for a genuinely archived body the
    /// moment the map round-tripped through one.
    #[test]
    fn provenance_survives_a_header_map_that_changed_casing() {
        let got = snapshot_provenance(&headers(&[
            ("X-Pumper-Fetched-Via", "archive"),
            ("X-PUMPER-SNAPSHOT-TS", "2019-03-11T00:00:00Z"),
        ]))
        .expect("casing must not decide provenance");
        assert_eq!(got.via, "archive");
        assert_eq!(got.captured_at.as_deref(), Some("2019-03-11T00:00:00Z"));
    }

    /// The engine may mark provenance without a capture time (a store that does
    /// not report one). That is still provenance — it just says less, and the
    /// note has to admit it rather than imply freshness.
    #[test]
    fn provenance_without_a_capture_time_says_so_instead_of_implying_freshness() {
        let got = snapshot_provenance(&headers(&[(FETCHED_VIA_HEADER, "archive")]))
            .expect("a marker alone is still provenance");
        assert!(got.captured_at.is_none());
        assert!(
            got.note().contains("capture time unknown"),
            "{}",
            got.note()
        );
    }
}
