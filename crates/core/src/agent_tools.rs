//! Job-scoped MCP access for the self-hosted agent loop (N15).
//!
//! The Claude tier is the only tier whose web access pumper does not own. The
//! subprocess is launched with the CLI's own `WebFetch`/`WebSearch`, so the
//! most expensive tier re-fetches the same URL the http and browser tiers
//! already tried — from the same IP, with no politeness spacing, no cookie
//! profile, no archive fallback, no response cache, no VCR cassette and no
//! ledger row. `[claude] self_hosted_tools = true` points the subprocess at
//! **pumper's own `/mcp`** instead: the CLI gets a `fetch` tool that runs the
//! calling job's metered [`crate::app::AppContext::fetch`], and everything the
//! rest of the ladder runs under applies to it.
//!
//! That tool needs to know *which job* is asking, because the whole point is
//! that the spend lands on that job's budget and ledger. This module is the
//! narrow answer: a process-local table of short-lived random tokens, each
//! bound to one job id, minted by the engine at spawn and revoked when the run
//! ends.
//!
//! **What a token is and is not.** It authorizes *attribution*, not access: it
//! says "the fetch this MCP call is making belongs to job X". It is not an API
//! key and does not satisfy `[auth] mode = "keys"` — the MCP route sits behind
//! the same identity layer as every other route, so in `keys` mode the config
//! also carries an operator-minted key (`[claude] self_hosted_key`). Tokens are
//! held in memory only (a restart invalidates every one, which is correct: the
//! jobs they named are not running either), never logged, and never written
//! anywhere but the per-run MCP config file the engine deletes on exit.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

/// The header a job token travels in. Deliberately NOT `Authorization`: that
/// header carries the API key in `keys` mode, and one header cannot mean two
/// credentials without the loop breaking in exactly one of the two auth modes.
pub const JOB_TOKEN_HEADER: &str = "x-pumper-job-token";

/// The MCP server name the engine writes into the per-run config, and therefore
/// the `mcp__<server>__<tool>` prefix the CLI's allow-list must name.
pub const MCP_SERVER_NAME: &str = "pumper";

/// Everything the subprocess needs to reach pumper's own MCP surface for one
/// run: where it is, which job the calls belong to, and (in `keys` mode) the
/// operator key that gets it past the identity layer.
#[derive(Debug, Clone)]
pub struct AgentTools {
    pub url: String,
    pub token: String,
    pub api_key: Option<String>,
    pub allowed_tools: Vec<String>,
}

/// One live grant: the job a token speaks for, and when it stops speaking.
#[derive(Debug, Clone, Copy)]
struct Grant {
    job_id: Uuid,
    expires_at: DateTime<Utc>,
}

/// What a presented token resolves to. Every non-`Valid` arm is a refusal the
/// tool reports as its own error rather than as a silent unattributed fetch —
/// an unattributed fetch is precisely the state this feature exists to end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenVerdict {
    /// No token was presented at all.
    Missing,
    /// A token was presented that this process never minted (or has revoked).
    Unknown,
    /// A token that was minted for a run which has since passed its deadline.
    Expired,
    /// Good for this job id.
    Valid(Uuid),
}

impl TokenVerdict {
    /// The refusal an agent reads, or `None` when the token is good.
    pub fn refusal(&self) -> Option<&'static str> {
        match self {
            Self::Missing => Some(
                "no job token: this tool runs a fetch on some job's budget, so it needs the \
                 x-pumper-job-token header pumper writes into the subprocess's MCP config. \
                 It is not callable from an ordinary MCP client.",
            ),
            Self::Unknown => Some(
                "unknown job token: it was never minted by this process, or the run it named \
                 has already ended and the token was revoked with it.",
            ),
            Self::Expired => Some(
                "expired job token: a token lives only as long as the research run it was \
                 minted for ([claude] self_hosted_token_ttl_secs).",
            ),
            Self::Valid(_) => None,
        }
    }
}

/// The verdict on one lookup, decided without a clock or a map.
///
/// Extracted so both refusals are reachable in a test: an expired grant is a
/// *different* fact from an unknown one (the first says the run is over, the
/// second says the caller is not pumper), and a seam that collapsed them would
/// make a leaked-token report indistinguishable from an ordinary late call.
/// The anti-pattern: `expired_token_not_read_as_valid`.
fn token_verdict(grant: Option<Grant>, now: DateTime<Utc>) -> TokenVerdict {
    match grant {
        None => TokenVerdict::Unknown,
        Some(g) if g.expires_at <= now => TokenVerdict::Expired,
        Some(g) => TokenVerdict::Valid(g.job_id),
    }
}

fn grants() -> &'static Mutex<HashMap<String, Grant>> {
    static GRANTS: OnceLock<Mutex<HashMap<String, Grant>>> = OnceLock::new();
    GRANTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A minted token, revoked when this guard drops.
///
/// The engine holds it beside the scratch files for exactly the life of the
/// subprocess, so "expires with the job" is enforced by ownership rather than
/// by a sweeper that may not run: a cancelled run drops the guard on the same
/// path that kills the process tree, and the token stops resolving before the
/// CLI has finished dying.
#[derive(Debug)]
pub struct JobToken {
    token: String,
    job_id: Uuid,
}

impl JobToken {
    /// The secret itself. Only ever written into the per-run MCP config file.
    pub fn secret(&self) -> &str {
        &self.token
    }

    pub fn job_id(&self) -> Uuid {
        self.job_id
    }
}

impl Drop for JobToken {
    fn drop(&mut self) {
        if let Ok(mut map) = grants().lock() {
            map.remove(&self.token);
        }
    }
}

/// Mints a token for `job_id`, good for `ttl`.
///
/// 256 bits of entropy in the same shape `auth::generate_key` mints its keys.
/// A zero `ttl` mints a token that is already expired — an honest way for an
/// operator to turn the loop off without turning the config key off.
pub fn mint(job_id: Uuid, ttl: Duration) -> JobToken {
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_at = Utc::now()
        + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(0));
    if let Ok(mut map) = grants().lock() {
        map.insert(token.clone(), Grant { job_id, expires_at });
    }
    JobToken { token, job_id }
}

/// Resolves a presented token to the job it speaks for.
pub fn resolve(token: Option<&str>) -> TokenVerdict {
    let Some(token) = token.map(str::trim).filter(|t| !t.is_empty()) else {
        return TokenVerdict::Missing;
    };
    let grant = grants().lock().ok().and_then(|map| map.get(token).copied());
    token_verdict(grant, Utc::now())
}

/// Live grants — telemetry for tests and diagnostics.
pub fn live_count() -> usize {
    grants().lock().map(|m| m.len()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The anti-pattern: a token whose run has ended still buying fetches on
    /// that job's budget. It must resolve to `Expired`, not to the job id.
    #[test]
    fn expired_token_not_read_as_valid() {
        let job = Uuid::new_v4();
        let now = Utc::now();
        let grant = Grant {
            job_id: job,
            expires_at: now - chrono::Duration::seconds(1),
        };
        assert_eq!(token_verdict(Some(grant), now), TokenVerdict::Expired);
        assert!(TokenVerdict::Expired.refusal().is_some());
    }

    /// A token this process never minted is `Unknown` — a distinct fact from
    /// `Expired`, because the two mean different things to whoever is reading
    /// the logs after a leak.
    #[test]
    fn unknown_token_not_confused_with_an_expired_one() {
        assert_eq!(token_verdict(None, Utc::now()), TokenVerdict::Unknown);
        assert_eq!(resolve(Some("not-a-token")), TokenVerdict::Unknown);
        assert_eq!(resolve(None), TokenVerdict::Missing);
        assert_eq!(resolve(Some("   ")), TokenVerdict::Missing);
    }

    /// A live grant resolves, and dropping the guard revokes it on the spot —
    /// "expires with the job" enforced by ownership, not by a sweeper.
    #[test]
    fn a_dropped_guard_not_left_resolvable() {
        let job = Uuid::new_v4();
        let secret = {
            let token = mint(job, Duration::from_secs(60));
            let secret = token.secret().to_string();
            assert_eq!(resolve(Some(&secret)), TokenVerdict::Valid(job));
            secret
        };
        assert_eq!(resolve(Some(&secret)), TokenVerdict::Unknown);
    }

    /// Two concurrent runs must never share a token — the whole attribution
    /// story collapses if one job can spend on another's budget.
    #[test]
    fn two_mints_not_sharing_a_token() {
        let a = mint(Uuid::new_v4(), Duration::from_secs(60));
        let b = mint(Uuid::new_v4(), Duration::from_secs(60));
        assert_ne!(a.secret(), b.secret());
        assert_eq!(a.secret().len(), 64, "256 bits, hex, no separators");
        assert_eq!(resolve(Some(a.secret())), TokenVerdict::Valid(a.job_id()));
        assert_eq!(resolve(Some(b.secret())), TokenVerdict::Valid(b.job_id()));
    }

    /// A zero TTL mints a token that never works — the honest shape of "on, but
    /// give it nothing", and proof the deadline is checked rather than assumed.
    #[test]
    fn a_zero_ttl_token_is_born_expired() {
        let token = mint(Uuid::new_v4(), Duration::from_secs(0));
        assert_eq!(resolve(Some(token.secret())), TokenVerdict::Expired);
    }
}
