//! DB-backed cron scheduler. Every tick it reconciles the `schedules` table:
//! for each enabled schedule whose next cron firing (relative to its last run)
//! is now due, it enqueues a job and records the run. Because schedules live in
//! the database, apps and callers can add, disable, or remove them at runtime
//! via the API without restarting the service. Paired with each app's dataset
//! dedup, this delivers periodic scrapes that only surface what changed.
//!
//! Each schedule's cron is evaluated in its own timezone (`schedules.timezone`,
//! chrono-tz; `NULL` = UTC), so DST transitions are honoured. When the scheduler
//! was down across one or more firings, `misfire_policy` decides the catch-up:
//! `fire_once` runs a single job (the historical behaviour), `skip` runs none
//! and simply advances past the missed firings.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule as CronSchedule;
use pumper_core::{EnqueueOptions, Schedule};
use serde_json::Value;
use tracing::{error, info, warn};

use crate::state::AppState;

/// Attempt budget for scheduled jobs whose schedule leaves `max_attempts` unset.
/// Cron runs then retry transient failures with backoff exactly like a manual
/// job, instead of the historical single hardcoded attempt.
const DEFAULT_SCHEDULE_MAX_ATTEMPTS: i64 = 3;

/// Cap on how many missed firings are enumerated per schedule per tick when
/// sizing a backlog, so a frequent schedule that fell far behind can't spin.
/// Reported "missed" count (misfire-skip path) saturates at this bound. Walked at
/// most once per schedule, since Skip then advances `last_run` past the backlog.
const MAX_MISFIRE_SCAN: usize = 10_000;

/// The `Fire` path enumerates the pending backlog no further than this — enough to
/// log a meaningful `collapsed` count while keeping per-tick work O(1) even when
/// the overlap guard keeps a schedule due for hours. Realistic backlogs are exact;
/// larger ones saturate here (the value is diagnostic only).
const COLLAPSE_LOG_CAP: usize = 64;

pub async fn run(state: AppState) {
    let tick = Duration::from_secs(state.config.worker.schedule_tick_secs.max(1));
    info!(tick_secs = tick.as_secs(), "scheduler started");
    // Parsed crons cached across ticks, keyed by expression string, so we don't
    // re-parse every schedule's cron on every tick (an edited cron is a new key and
    // re-parses). Lives here so it outlives a single reconcile.
    let mut cron_cache: HashMap<String, CronSchedule> = HashMap::new();
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        if let Err(e) = reconcile(&state, &mut cron_cache).await {
            error!("scheduler reconcile failed: {e}");
        }
        // Piggyback the scheduler tick to run the stuck-job reaper: re-queue
        // running jobs whose heartbeat lease has gone stale (a hung task on a
        // live server). Cheap — one indexed scan of `running` jobs.
        crate::worker::reap_once(&state).await;
        // Also piggyback the webhook dead-letter drain: re-send failed deliveries
        // whose backoff is due, so a receiver outage longer than the in-process
        // retry loop doesn't mean permanent silent event loss.
        if state.config.webhooks.auto_retry {
            crate::webhook::drain_due(&state).await;
        }
        // Stop enqueuing new scheduled work as soon as shutdown is signalled.
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
    }
    info!("scheduler stopped");
}

async fn reconcile(
    state: &AppState,
    cron_cache: &mut HashMap<String, CronSchedule>,
) -> anyhow::Result<()> {
    let now = Utc::now();
    // A firing more than two ticks late was missed while the scheduler was down
    // (a healthy tick catches a due firing within one interval). This is the
    // grace window that separates an on-time run from a backlog misfire.
    let grace =
        chrono::Duration::seconds(state.config.worker.schedule_tick_secs.max(1) as i64 * 2);
    for schedule in state.storage.list_schedules().await? {
        if !schedule.enabled {
            continue;
        }
        let cron = if let Some(cron) = cron_cache.get(&schedule.cron) {
            cron
        } else {
            match CronSchedule::from_str(&schedule.cron) {
                Ok(cron) => cron_cache.entry(schedule.cron.clone()).or_insert(cron),
                Err(e) => {
                    warn!(id = %schedule.id, cron = %schedule.cron, "invalid cron: {e}");
                    continue;
                }
            }
        };
        let tz = parse_tz(schedule.timezone.as_deref());
        // Next firing after the last run (or after creation for a fresh schedule).
        let reference = schedule.last_run.unwrap_or(schedule.created_at);
        let misfire_skip = schedule.misfire_policy == "skip";

        match decide(cron, tz, reference, now, misfire_skip, grace) {
            Action::Idle => continue,
            Action::Skip { missed } => {
                info!(
                    id = %schedule.id, app = %schedule.app, missed,
                    "misfire policy 'skip': advancing past missed firings without enqueuing"
                );
                state.storage.touch_schedule(&schedule.id, now).await?;
            }
            Action::Fire { collapsed } => {
                if !state.registry.contains_key(&schedule.app) {
                    warn!(app = %schedule.app, "schedule references unregistered app; skipping");
                    continue;
                }
                // Overlap guard: don't stack a second run while the previous one
                // is still queued/running. last_run is NOT touched, so the missed
                // firing stays due and fires on the first tick after it finishes.
                if state.storage.schedule_has_active_job(&schedule.id).await? {
                    info!(id = %schedule.id, app = %schedule.app, "previous scheduled run still active; skipping tick");
                    continue;
                }

                let params = resolve_params(state, &schedule);
                let max_attempts = schedule.max_attempts.unwrap_or(DEFAULT_SCHEDULE_MAX_ATTEMPTS);
                let opts = EnqueueOptions {
                    params,
                    max_attempts,
                    priority: schedule.priority,
                    schedule_id: Some(schedule.id.clone()),
                    ..Default::default()
                };
                match state.storage.enqueue(&schedule.app, opts).await {
                    Ok(job) => {
                        if collapsed > 0 {
                            info!(id = %schedule.id, app = %schedule.app, job = %job.id, collapsed, "scheduled run fired (missed firings collapsed into one)");
                        } else {
                            info!(id = %schedule.id, app = %schedule.app, job = %job.id, "scheduled run fired");
                        }
                        state.storage.touch_schedule(&schedule.id, now).await?;
                        state.notify.notify_one();
                    }
                    Err(e) => error!(id = %schedule.id, "failed to enqueue scheduled job: {e}"),
                }
            }
        }
    }
    Ok(())
}

/// What a tick should do with one schedule.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    /// Nothing due yet.
    Idle,
    /// Enqueue one run. `collapsed` = extra missed firings folded into this run
    /// (0 when on-time) — for logging only.
    Fire { collapsed: usize },
    /// `misfire_policy = skip`: advance past `missed` firings without enqueuing.
    Skip { missed: usize },
}

/// Parses an IANA timezone name; unknown/absent names fall back to UTC. The API
/// validates the name at create time, so this only defends against manual edits.
fn parse_tz(name: Option<&str>) -> Tz {
    name.and_then(|n| n.parse().ok()).unwrap_or(Tz::UTC)
}

/// Projects a schedule's next firing (read-only, for the observability API),
/// using the exact reference rule the reconcile loop does: the first cron time
/// strictly after `last_run` (or `created_at` for a never-run schedule),
/// evaluated in the schedule's timezone. `None` if the cron is unparseable or has
/// no future firing — so the API can never disagree with the scheduler.
pub fn project_next_run(schedule: &Schedule) -> Option<DateTime<Utc>> {
    let cron = CronSchedule::from_str(&schedule.cron).ok()?;
    let tz = parse_tz(schedule.timezone.as_deref());
    let reference = schedule.last_run.unwrap_or(schedule.created_at);
    cron.after(&reference.with_timezone(&tz)).next().map(|t| t.with_timezone(&Utc))
}

/// Decides a schedule's action this tick — pure (no I/O), so it is unit-testable
/// against simulated downtime and DST boundaries.
///
/// The cron is evaluated in `tz` (a firing at a nonexistent local wall-clock time
/// — e.g. inside a spring-forward gap — is skipped by the cron iterator). `grace`
/// is how late the oldest pending firing may be and still count as on-time; older
/// than that means it was missed while the scheduler was down (a misfire).
fn decide(
    cron: &CronSchedule,
    tz: Tz,
    reference: DateTime<Utc>,
    now: DateTime<Utc>,
    misfire_skip: bool,
    grace: chrono::Duration,
) -> Action {
    let reference_tz = reference.with_timezone(&tz);
    let now_tz = now.with_timezone(&tz);

    let mut iter = cron.after(&reference_tz);
    // The earliest pending firing is one iterator step: firings come out
    // increasing, so if the first is still in the future nothing is due. This
    // avoids enumerating the whole backlog just to find the oldest one.
    let earliest = match iter.next() {
        Some(fire) if fire <= now_tz => fire,
        _ => return Action::Idle,
    };

    // Misfire = the oldest pending firing is more than `grace` behind now.
    let missed = now_tz.signed_duration_since(earliest) > grace;
    if missed && misfire_skip {
        // Skip advances past ALL missed firings, so it needs the exact count — but
        // this happens once (the tick then touches last_run), not every tick.
        let mut missed = 1usize;
        for fire in iter {
            if fire > now_tz {
                break;
            }
            missed += 1;
            if missed >= MAX_MISFIRE_SCAN {
                break;
            }
        }
        Action::Skip { missed }
    } else {
        // Fire enqueues ONE run no matter how many firings are pending, and the
        // overlap guard can keep this schedule "due" for many ticks — so bound the
        // enumeration to a small cap instead of re-walking the whole growing
        // backlog every tick. `collapsed` is a diagnostic log field: exact for
        // realistic backlogs, saturating at the cap for pathological ones.
        let mut collapsed = 0usize;
        for fire in iter {
            if fire > now_tz {
                break;
            }
            collapsed += 1;
            if collapsed >= COLLAPSE_LOG_CAP {
                break;
            }
        }
        Action::Fire { collapsed }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn cron(expr: &str) -> CronSchedule {
        CronSchedule::from_str(expr).unwrap()
    }

    /// Top of every hour.
    const HOURLY: &str = "0 0 * * * *";
    const GRACE: chrono::Duration = chrono::Duration::seconds(30);

    #[test]
    fn idle_when_next_firing_is_in_the_future() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        // Next hourly firing after 12:00 is 13:00 — not yet due.
        assert_eq!(decide(&cron(HOURLY), Tz::UTC, reference, now, false, GRACE), Action::Idle);
    }

    fn schedule(cron: &str, tz: Option<&str>, last_run: Option<DateTime<Utc>>) -> Schedule {
        Schedule {
            id: "s1".into(),
            app: "demo".into(),
            cron: cron.into(),
            params: Value::Null,
            enabled: true,
            priority: 0,
            timezone: tz.map(String::from),
            misfire_policy: "fire_once".into(),
            max_attempts: None,
            last_run,
            created_at: Utc.with_ymd_and_hms(2026, 7, 13, 9, 15, 0).unwrap(),
        }
    }

    #[test]
    fn project_next_run_uses_last_run_reference() {
        // Never run → projects from created_at (09:15) → next hourly is 10:00.
        let never = schedule(HOURLY, None, None);
        assert_eq!(
            project_next_run(&never),
            Some(Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap())
        );
        // Ran at 12:00 → next hourly firing after is 13:00.
        let ran = schedule(HOURLY, None, Some(Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap()));
        assert_eq!(
            project_next_run(&ran),
            Some(Utc.with_ymd_and_hms(2026, 7, 13, 13, 0, 0).unwrap())
        );
    }

    #[test]
    fn project_next_run_none_on_bad_cron() {
        assert_eq!(project_next_run(&schedule("not a cron", None, None)), None);
    }

    #[test]
    fn on_time_firing_runs_under_both_policies() {
        // Firing at 12:00:00 detected 30s later — within grace, so on-time.
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 11, 30, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        let grace = chrono::Duration::seconds(60);
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, false, grace),
            Action::Fire { collapsed: 0 }
        );
        // skip only skips *missed* firings; an on-time one still runs.
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, true, grace),
            Action::Fire { collapsed: 0 }
        );
    }

    #[test]
    fn fire_once_collapses_a_downtime_backlog_into_one_run() {
        // Simulated downtime: last run 08:00, back at 12:00:30 — 09/10/11/12 missed.
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, false, GRACE),
            Action::Fire { collapsed: 3 }
        );
    }

    #[test]
    fn skip_advances_past_a_downtime_backlog_without_running() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, true, GRACE),
            Action::Skip { missed: 4 }
        );
    }

    #[test]
    fn cron_is_evaluated_in_the_schedule_timezone_across_dst() {
        // US spring-forward 2026: DST begins Sun Mar 8 02:00 -> 03:00 (EST->EDT).
        let tz: Tz = "America/New_York".parse().unwrap();
        // Daily noon local. Reference just after Mar 7 noon (EST, UTC-5 => 17:00Z).
        let reference = Utc.with_ymd_and_hms(2026, 3, 7, 18, 0, 0).unwrap();
        let next = cron("0 0 12 * * *")
            .after(&reference.with_timezone(&tz))
            .next()
            .unwrap()
            .with_timezone(&Utc);
        // Mar 8 is already on EDT (UTC-4), so local noon = 16:00Z — NOT the 17:00Z
        // a naive-UTC evaluation would produce.
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 8, 16, 0, 0).unwrap());
    }

    #[test]
    fn firing_inside_a_spring_forward_gap_is_skipped() {
        // 02:30 local does not exist on Mar 8 2026 (clocks jump 02:00 -> 03:00).
        let tz: Tz = "America/New_York".parse().unwrap();
        let reference = Utc.with_ymd_and_hms(2026, 3, 8, 5, 0, 0).unwrap(); // Mar 8 00:00 EST
        let next = cron("0 30 2 * * *")
            .after(&reference.with_timezone(&tz))
            .next()
            .unwrap()
            .with_timezone(&Utc);
        // The nonexistent Mar 8 02:30 is skipped; next is Mar 9 02:30 EDT = 06:30Z.
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap());
    }
}
