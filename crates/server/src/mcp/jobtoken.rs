//! Server side of the N15 job token: turning the `x-pumper-job-token` header a
//! self-hosted Claude subprocess presents into the metered `AppContext` its
//! `fetch` calls run under.
//!
//! The token itself lives in [`pumper_core::agent_tools`] — minted by the
//! engine at spawn, revoked when the run's guard drops. This module is the
//! other half: it resolves one, refuses honestly when it cannot, and rebuilds
//! the calling job's context so `ctx.fetch(..)` goes through the *same* seam
//! the job's own app code would have used — governor, response cache, tier
//! router, session profile, archive tier, budget clamp and cost ledger all
//! apply, and the spend lands on the job that is paying for the model.
//!
//! Two deliberate differences from the worker's context, both because this is a
//! side call into a run that is already in flight rather than the run itself:
//! progress and checkpoints are no-ops (a fetch the model made is not a
//! resumable step of the app), and VCR is `Off` — recording into a cassette
//! another task owns would interleave two writers, and replay would have to
//! resolve against a cassette this call did not open. Both are named as gaps in
//! `docs/features/mcp.md` rather than faked.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use pumper_core::agent_tools::{self, TokenVerdict, JOB_TOKEN_HEADER};
use pumper_core::{AppContext, JobStatus};

use crate::state::AppState;

/// What one MCP request presented about itself. Anonymous for every ordinary
/// client — the `fetch` tool is the only surface that reads it.
#[derive(Debug, Clone, Default)]
pub(crate) struct McpCaller {
    pub(crate) job_token: Option<String>,
}

impl McpCaller {
    /// No credential at all — what a direct `handle_rpc` call carries.
    #[cfg(test)]
    pub(crate) fn anonymous() -> Self {
        Self::default()
    }

    pub(crate) fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            job_token: headers
                .get(JOB_TOKEN_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        }
    }

    /// A caller carrying a token literal — for tests and for the engine-side
    /// round trip.
    #[cfg(test)]
    pub(crate) fn with_token(token: impl Into<String>) -> Self {
        Self {
            job_token: Some(token.into()),
        }
    }
}

/// Whether a job in this state may still have money spent against it.
///
/// A token exists only while the engine is holding a subprocess open, so the
/// job it names is `running` — but a token could arrive late (a leaked config
/// file, a retried MCP call after the run ended), and the anti-pattern
/// `a_finished_job_not_billed_for_a_late_fetch` is what this closes: spending
/// on a job that has already been finalized writes a cost row nothing will ever
/// reconcile, past a `budget_usd` the receipt has already reported as final.
///
/// `waiting` is refused too. A parked job is not executing, so nothing of its
/// is legitimately fetching; a token that still resolves for one is a token
/// whose guard did not drop.
pub(crate) fn job_may_spend(status: JobStatus) -> bool {
    matches!(status, JobStatus::Running)
}

/// One refusal, in the tool-error shape, tagged with the HTTP code it would
/// have carried on the REST surface so the two surfaces agree about what
/// happened.
fn refuse(status: StatusCode, message: impl AsRef<str>) -> String {
    format!(
        "[{}] {}",
        crate::routes::error_code(status),
        message.as_ref()
    )
}

/// Resolves the caller's job token and rebuilds that job's metered context.
///
/// Every failure is a readable tool error, never a fetch: an MCP `fetch` that
/// could not identify its job would be exactly the ungoverned, unattributed
/// egress this whole item exists to remove.
pub(crate) async fn job_context(
    state: &AppState,
    caller: &McpCaller,
) -> Result<AppContext, String> {
    let verdict = agent_tools::resolve(caller.job_token.as_deref());
    let job_id = match verdict {
        TokenVerdict::Valid(id) => id,
        other => {
            return Err(refuse(
                StatusCode::UNAUTHORIZED,
                other.refusal().unwrap_or("job token refused"),
            ))
        }
    };
    let job = state
        .storage
        .get(job_id)
        .await
        .map_err(|e| refuse(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| {
            refuse(
                StatusCode::NOT_FOUND,
                format!("job '{job_id}' no longer exists"),
            )
        })?;
    if !job_may_spend(job.status) {
        return Err(refuse(
            StatusCode::CONFLICT,
            format!(
                "job '{job_id}' is {} — a fetch on this token would spend against a run that \
                 is not executing",
                job.status.as_str()
            ),
        ));
    }
    // Seeded from the ledger for the same reason the worker seeds it: this
    // job's earlier spend still counts toward its ceiling, and a sub-fetch that
    // started its accounting at zero could walk straight through a budget the
    // model has already exhausted.
    let spent = state.costs.job_total(job_id).await.unwrap_or(0.0);
    Ok(AppContext {
        job_id,
        app: job.app.clone(),
        params: job.params.clone(),
        engines: state.engines.clone(),
        datasets: state.datasets.clone(),
        costs: state.costs.clone(),
        // No post-run fan-out drains THIS context (it is a token-scoped
        // sub-fetch, not a job run), so `request_schedule` refuses rather than
        // collecting requests nobody would ever apply.
        schedule_requests: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        max_schedule_requests: 0,
        budget_usd: crate::datahub::effective_budget(state, &job.app, job.budget_usd),
        spent_usd: Arc::new(pumper_core::SpentTotal::new(spent)),
        research_cache: state.research_cache.clone(),
        tiers: state.tiers.clone(),
        health: state.health.clone(),
        recipes: Arc::new(state.storage.recipes()),
        plugins: state.plugins.clone(),
        progress: Arc::new(pumper_core::NoProgress),
        checkpoints: Arc::new(pumper_core::NoCheckpoints),
        restored: None,
        resumed_input: None,
        vcr: pumper_core::Vcr::Off,
        artifacts_dir: state
            .storage
            .artifacts_dir
            .join(&job.app)
            .join(job_id.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The anti-pattern: a token that outlived its run still buying fetches on
    /// a job whose receipt has already been written and whose budget has
    /// already been reported as final.
    #[test]
    fn a_finished_job_not_billed_for_a_late_fetch() {
        assert!(job_may_spend(JobStatus::Running));
        for done in [
            JobStatus::Queued,
            JobStatus::Waiting,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ] {
            assert!(!job_may_spend(done), "{done:?} must not be billable");
        }
    }

    /// A refusal names the HTTP code the REST surface would have used, so an
    /// operator reading a tool error and an operator reading an access log are
    /// looking at the same fact.
    #[test]
    fn a_refusal_carries_its_status_code() {
        assert!(refuse(StatusCode::UNAUTHORIZED, "no").starts_with("[unauthorized]"));
        assert!(refuse(StatusCode::CONFLICT, "no").starts_with("[conflict]"));
    }

    /// Header extraction is the whole trust boundary: only the one header, and
    /// an absent one is `None` rather than an empty-string token that could
    /// match an empty grant.
    #[test]
    fn only_the_job_token_header_is_read() {
        let mut headers = HeaderMap::new();
        assert!(McpCaller::from_headers(&headers).job_token.is_none());
        headers.insert("authorization", "Bearer somekey".parse().unwrap());
        assert!(
            McpCaller::from_headers(&headers).job_token.is_none(),
            "an API key is not a job token"
        );
        headers.insert(JOB_TOKEN_HEADER, "abc".parse().unwrap());
        assert_eq!(
            McpCaller::from_headers(&headers).job_token.as_deref(),
            Some("abc")
        );
        assert!(McpCaller::anonymous().job_token.is_none());
        assert_eq!(McpCaller::with_token("t").job_token.as_deref(), Some("t"));
    }
}
