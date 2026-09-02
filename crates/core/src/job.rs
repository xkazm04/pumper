use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    /// Parked on the outside world: the app called
    /// [`crate::AppContext::await_input`], its checkpoint was forced, and its
    /// worker permit was released. Non-terminal — `GET /jobs/{id}/stream` stays
    /// open, the schedule slot stays held — and it leaves the state only through
    /// `POST /jobs/{id}/resume` (back to `queued`, with attempt headroom) or the
    /// `waiting_expires_at` sweep (to `failed`).
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Waiting => "waiting",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(JobStatus::Queued),
            "running" => Some(JobStatus::Running),
            "waiting" => Some(JobStatus::Waiting),
            "succeeded" => Some(JobStatus::Succeeded),
            "failed" => Some(JobStatus::Failed),
            "cancelled" => Some(JobStatus::Cancelled),
            _ => None,
        }
    }

    /// The single authority for "is this job in a terminal state?". Every SSE
    /// self-termination check and trigger filter routes through here so adding a
    /// new terminal variant can never silently leave a stream open (or a trigger
    /// unfired) at one forgotten call site.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Succeeded | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub id: Uuid,
    pub app: String,
    pub params: Value,
    pub status: JobStatus,
    pub attempts: i64,
    pub max_attempts: i64,
    /// Higher runs sooner; ties break by creation order.
    pub priority: i64,
    /// On terminal state, the worker POSTs this job here (HMAC-signed).
    pub callback_url: Option<String>,
    #[serde(skip_serializing)]
    pub callback_secret: Option<String>,
    /// Spend ceiling for the whole job; metered Claude calls abort past it.
    pub budget_usd: Option<f64>,
    /// The schedule that fired this job, when it was a scheduled run.
    pub schedule_id: Option<String>,
    /// The trigger that fired this job, when it was a reactive-pipeline hop.
    pub trigger_id: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    /// What a `waiting` job asked the outside world for — the app's own JSON
    /// request, verbatim (`{kind, ...}` is the convention, nothing is imposed).
    /// `None` on a job that has never parked.
    pub input_request: Option<Value>,
    /// When this job entered `waiting`. Survives the resume, so "how long did
    /// the human take?" is answerable from the row.
    pub waiting_since: Option<DateTime<Utc>>,
    /// When an unresumed wait becomes a permanent failure. `None` = wait
    /// forever (the default: `[waiting] expiry_secs = 0`).
    pub waiting_expires_at: Option<DateTime<Utc>>,
    /// The input `POST /jobs/{id}/resume` stored, handed to the resumed attempt
    /// as `ctx.restore_input()`.
    ///
    /// **Never serialized**, for the same reason as `callback_secret`: the
    /// payloads this field exists to carry are approvals, one-time codes and
    /// credentials handed to a parked job, and `GET /jobs` is an unauthenticated
    /// read on a default install.
    #[serde(skip_serializing)]
    pub resumed_input: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::JobStatus;

    const ALL: [JobStatus; 6] = [
        JobStatus::Queued,
        JobStatus::Running,
        JobStatus::Waiting,
        JobStatus::Succeeded,
        JobStatus::Failed,
        JobStatus::Cancelled,
    ];

    #[test]
    fn is_terminal_matches_intended_set() {
        assert!(JobStatus::Succeeded.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
        assert!(!JobStatus::Queued.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(!JobStatus::Waiting.is_terminal());
    }

    /// The anti-pattern: `waiting` classified terminal because it is "not
    /// running". A parked job is *mid-run* — its checkpoint is live, its
    /// schedule slot is held, and `GET /jobs/{id}/stream` must stay open until
    /// it really ends. Terminality here would close the stream, fire the
    /// terminal triggers and dispatch the result callback on a job that has not
    /// produced a result.
    #[test]
    fn waiting_is_non_terminal_not_a_sixth_ending() {
        assert!(!JobStatus::Waiting.is_terminal());
        assert_eq!(JobStatus::parse("waiting"), Some(JobStatus::Waiting));
        assert_eq!(JobStatus::Waiting.as_str(), "waiting");
        let terminal: Vec<_> = ALL.iter().filter(|s| s.is_terminal()).collect();
        assert_eq!(terminal.len(), 3, "adding `waiting` must not add an ending");
    }

    /// Meta-test: the string-literal terminal predicate that the SSE and trigger
    /// call sites replaced (`matches!(s, "succeeded" | "failed" | "cancelled")`)
    /// must agree, variant for variant, with the enum authority — so routing a
    /// string through `parse(..).is_terminal()` cannot drift from the source of
    /// truth if a new terminal variant is ever added.
    #[test]
    fn string_sites_agree_with_enum_authority() {
        for status in ALL {
            let via_enum = status.is_terminal();
            let via_string = JobStatus::parse(status.as_str()).is_some_and(|j| j.is_terminal());
            assert_eq!(
                via_enum, via_string,
                "string predicate disagrees with enum for {:?}",
                status
            );
        }
    }
}
