//! DB-backed cron scheduler. Every tick it reconciles the `schedules` table:
//! for each enabled schedule whose next cron firing (relative to its last run)
//! is now due, it enqueues a job and records the run. Because schedules live in
//! the database, apps and callers can add, disable, or remove them at runtime
//! via the API without restarting the service. Paired with each app's dataset
//! dedup, this delivers periodic scrapes that only surface what changed.

use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use cron::Schedule as CronSchedule;
use pumper_core::{EnqueueOptions, Schedule};
use serde_json::Value;
use tracing::{error, info, warn};

use crate::state::AppState;

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.worker.schedule_tick_secs.max(1));
    info!(tick_secs = tick.as_secs(), "scheduler started");
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        if let Err(e) = reconcile(&state).await {
            error!("scheduler reconcile failed: {e}");
        }
        // Stop enqueuing new scheduled work as soon as shutdown is signalled.
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
    }
    info!("scheduler stopped");
}

async fn reconcile(state: &AppState) -> anyhow::Result<()> {
    let now = Utc::now();
    for schedule in state.storage.list_schedules().await? {
        if !schedule.enabled {
            continue;
        }
        let cron = match CronSchedule::from_str(&schedule.cron) {
            Ok(cron) => cron,
            Err(e) => {
                warn!(id = %schedule.id, cron = %schedule.cron, "invalid cron: {e}");
                continue;
            }
        };
        // Next firing after the last run (or after creation for a fresh schedule).
        let reference = schedule.last_run.unwrap_or(schedule.created_at);
        let Some(next) = cron.after(&reference).next() else {
            continue;
        };
        if next > now {
            continue; // not due yet
        }

        if !state.registry.contains_key(&schedule.app) {
            warn!(app = %schedule.app, "schedule references unregistered app; skipping");
            continue;
        }

        // Overlap guard: don't stack a second run while the previous one is
        // still queued/running. last_run is NOT touched, so the missed firing
        // stays due and fires on the first tick after the active run finishes.
        if state.storage.schedule_has_active_job(&schedule.id).await? {
            info!(id = %schedule.id, app = %schedule.app, "previous scheduled run still active; skipping tick");
            continue;
        }

        let params = resolve_params(state, &schedule);
        let opts = EnqueueOptions {
            params,
            max_attempts: 1,
            priority: schedule.priority,
            schedule_id: Some(schedule.id.clone()),
            ..Default::default()
        };
        match state.storage.enqueue(&schedule.app, opts).await {
            Ok(job) => {
                info!(id = %schedule.id, app = %schedule.app, job = %job.id, "scheduled run fired");
                state.storage.touch_schedule(&schedule.id, now).await?;
                state.notify.notify_one();
            }
            Err(e) => error!(id = %schedule.id, "failed to enqueue scheduled job: {e}"),
        }
    }
    Ok(())
}

/// Uses the schedule's own params, falling back to the app's defaults when none
/// were configured.
fn resolve_params(state: &AppState, schedule: &Schedule) -> Value {
    let empty = matches!(&schedule.params, Value::Null)
        || matches!(&schedule.params, Value::Object(m) if m.is_empty());
    if empty {
        state
            .registry
            .get(&schedule.app)
            .map(|app| app.default_params())
            .unwrap_or(Value::Null)
    } else {
        schedule.params.clone()
    }
}
