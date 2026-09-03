use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Succeeded => "succeeded",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(JobStatus::Queued),
            "running" => Some(JobStatus::Running),
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
    /// What this job acts on, as its app declares it (`ScrapeApp::target_key`).
    /// Two jobs carrying the same key never run at the same time — the claim
    /// holds the second back until the first leaves `running`. `None` = the app
    /// names no target, and the job is neither held nor holding.
    ///
    /// Serialized on `GET /jobs`, which is where a queued job that is *held*
    /// becomes distinguishable from one the worker is merely behind on.
    pub target_key: Option<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::JobStatus;

    const ALL: [JobStatus; 5] = [
        JobStatus::Queued,
        JobStatus::Running,
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
