//! Live job events, broadcast to any SSE subscribers.

use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct JobEvent {
    pub job_id: Uuid,
    pub app: String,
    /// queued | running | succeeded | failed | cancelled
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl JobEvent {
    pub fn new(job_id: Uuid, app: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            job_id,
            app: app.into(),
            status: status.into(),
            result: None,
            error: None,
        }
    }
}
