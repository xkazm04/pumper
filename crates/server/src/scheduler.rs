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
use pumper_core::{Catalog, EnqueueOptions, ReconcilePlan, Schedule, CATALOG_MANAGED_BY};
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
    // Boot-time catalog reconcile: always plan and log drift loudly; only apply
    // when [catalog] auto_reconcile = true (default OFF). Failures are non-fatal
    // — a broken catalog must not stop the scheduler from serving existing rows.
    boot_reconcile(&state).await;
    // Parsed crons cached across ticks, keyed by expression string, so we don't
    // re-parse every schedule's cron on every tick (an edited cron is a new key and
    // re-parses). Lives here so it outlives a single reconcile.
    let mut cron_cache: HashMap<String, CronSchedule> = HashMap::new();
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        if let Err(e) = reconcile(&state, &mut cron_cache, Utc::now()).await {
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
        // And the cache refresher ([refresher], default OFF): revalidate cached
        // entries whose learned change cadence says a change is near — spawned
        // (non-blocking) and strictly idle-slot via Governor::try_acquire, so
        // it can neither delay this loop nor crowd out live traffic.
        crate::refresher::tick(&state);
        // And the DataHub governance pull ([datahub] govern, default OFF):
        // interval-gated and spawned — deprecations/tags/assertions in DataHub
        // become schedule disables, Claude-tier pauses, and immediate syncs.
        crate::datahub::govern_tick(&state);
        // Stop enqueuing new scheduled work as soon as shutdown is signalled.
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
    }
    info!("scheduler stopped");
}

/// `now` is a parameter (the same rule `decide` follows) so a test can drive a
/// reconcile pass deterministically without waiting for wall-clock time.
pub(crate) async fn reconcile(
    state: &AppState,
    cron_cache: &mut HashMap<String, CronSchedule>,
    now: chrono::DateTime<Utc>,
) -> anyhow::Result<()> {
    // A firing more than two ticks late was missed while the scheduler was down
    // (a healthy tick catches a due firing within one interval). This is the
    // grace window that separates an on-time run from a backlog misfire.
    let grace = chrono::Duration::seconds(state.config.worker.schedule_tick_secs.max(1) as i64 * 2);
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
        // Next firing after this schedule's cron reference (see the shared
        // `schedule_reference`, which `project_next_run` uses too).
        let reference = schedule_reference(&schedule);
        let misfire_skip = schedule.misfire_policy == "skip";

        match decide(cron, tz, reference, now, misfire_skip, grace) {
            Action::Idle => continue,
            Action::Skip { missed } => {
                info!(
                    id = %schedule.id, app = %schedule.app, missed,
                    "misfire policy 'skip': advancing past missed firings without enqueuing"
                );
                // NOT `touch_schedule`: nothing ran, so `last_run` must not move.
                // The advance is recorded as a skip (with its eaten-firing count),
                // which `schedule_reference` then honours — see migration 0039.
                state
                    .storage
                    .record_schedule_skip(&schedule.id, now, missed)
                    .await?;
            }
            Action::Fire { collapsed } => {
                if !state.registry.contains_key(&schedule.app) {
                    warn!(app = %schedule.app, "schedule references unregistered app; skipping");
                    continue;
                }
                // Overlap guard: don't stack a second run while the previous one
                // is still queued/running. last_run is NOT touched, so the missed
                // firing stays due and fires on the first tick after it finishes.
                // The SAME read + predicate `GET /schedules` reports as
                // `health: "overlapping"` — see `latest_run`.
                if latest_run(state, &schedule.id).await?.holds_slot {
                    info!(id = %schedule.id, app = %schedule.app, "previous scheduled run still active; skipping tick");
                    continue;
                }

                // Door parity: a schedule whose effective params fail the app's
                // declared schema is SKIPPED, not enqueued — the same 422 the
                // HTTP door answers, moved to the only place a legacy row can
                // still be caught. `last_run` is deliberately NOT touched (as
                // with an unregistered app), so fixing the row makes the
                // schedule fire again instead of having silently eaten firings.
                // Visible on `GET /schedules` as `health: "invalid_params"`.
                let params = match validate_schedule_params(
                    &state.registry,
                    &schedule.app,
                    &schedule.params,
                ) {
                    Ok(params) => params,
                    Err(msg) => {
                        warn!(
                            id = %schedule.id, app = %schedule.app,
                            "schedule params fail the app's declared schema; not enqueuing \
                             (GET /schedules shows health=invalid_params): {msg}"
                        );
                        continue;
                    }
                };
                let max_attempts = schedule
                    .max_attempts
                    .unwrap_or(DEFAULT_SCHEDULE_MAX_ATTEMPTS);
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

// ---- Catalog reconcile (M19) ----------------------------------------------
// The catalog (`catalog/data-sources.toml`) is desired state; the schedules
// table is actual state. The pure diff lives in `pumper_core::Catalog::
// reconcile_plan`; this section is the I/O around it: load + list (plan), and
// the tag-fenced writes (apply). Shared by boot and the /catalog/reconcile
// routes so all three paths cannot disagree.

/// Loads the catalog and diffs it against the schedules table. Dry-run: no writes.
pub(crate) async fn catalog_reconcile_plan(state: &AppState) -> anyhow::Result<ReconcilePlan> {
    let catalog = Catalog::load()?;
    let schedules = state.storage.list_schedules().await?;
    Ok(catalog.reconcile_plan(&schedules))
}

/// Applies a plan: creates/updates/disables **catalog-managed** schedules only
/// (every storage write is SQL-fenced on `managed_by = "catalog"`). Orphans are
/// never touched. Returns per-section applied counts plus any per-row errors —
/// a partial apply is reported honestly rather than rolled into one failure,
/// since re-running reconcile is idempotent and finishes the remainder.
pub(crate) async fn apply_reconcile_plan(
    state: &AppState,
    plan: &ReconcilePlan,
) -> serde_json::Value {
    let mut created = 0usize;
    let mut updated = 0usize;
    let mut disabled = 0usize;
    let mut errors: Vec<String> = Vec::new();
    for c in &plan.create {
        match state
            .storage
            .create_managed_schedule(&c.app, &c.cron, CATALOG_MANAGED_BY)
            .await
        {
            Ok(s) => {
                info!(id = %s.id, app = %c.app, cron = %c.cron, "catalog reconcile: schedule created");
                created += 1;
            }
            Err(e) => errors.push(format!("create {}: {e}", c.app)),
        }
    }
    for u in &plan.update {
        match state
            .storage
            .set_managed_schedule_cron(&u.schedule_id, &u.to_cron, CATALOG_MANAGED_BY)
            .await
        {
            Ok(true) => {
                info!(id = %u.schedule_id, app = %u.app, from = %u.from_cron, to = %u.to_cron,
                      re_enable = u.re_enable, "catalog reconcile: schedule updated");
                updated += 1;
            }
            Ok(false) => errors.push(format!(
                "update {}: row missing or not catalog-managed (fence)",
                u.schedule_id
            )),
            Err(e) => errors.push(format!("update {}: {e}", u.schedule_id)),
        }
    }
    for d in &plan.disable {
        match state
            .storage
            .set_managed_schedule_enabled(&d.schedule_id, false, CATALOG_MANAGED_BY)
            .await
        {
            Ok(true) => {
                warn!(id = %d.schedule_id, app = %d.app, reason = %d.reason,
                      "catalog reconcile: schedule DISABLED");
                disabled += 1;
            }
            Ok(false) => errors.push(format!(
                "disable {}: row missing or not catalog-managed (fence)",
                d.schedule_id
            )),
            Err(e) => errors.push(format!("disable {}: {e}", d.schedule_id)),
        }
    }
    serde_json::json!({
        "created": created,
        "updated": updated,
        "disabled": disabled,
        "orphans_untouched": plan.orphan.len(),
        "errors": errors,
    })
}

/// Boot pass: dry-run always; loud drift log; apply only under
/// `[catalog] auto_reconcile = true`.
async fn boot_reconcile(state: &AppState) {
    let plan = match catalog_reconcile_plan(state).await {
        Ok(plan) => plan,
        Err(e) => {
            warn!("catalog reconcile skipped (catalog unreadable): {e}");
            return;
        }
    };
    if plan.is_empty() {
        info!(
            covered_by_untagged = plan.covered_by_untagged,
            in_sync = plan.in_sync,
            "catalog reconcile: schedules in sync with catalog/data-sources.toml"
        );
        return;
    }
    // Loud on purpose: drift between the TOML and the live schedules table is
    // the exact condition this feature exists to surface.
    warn!(
        "CATALOG DRIFT: schedules disagree with catalog/data-sources.toml — {} \
         (dry-run: GET /catalog/reconcile; apply: POST /catalog/reconcile, or set \
         [catalog] auto_reconcile = true)",
        plan.summary()
    );
    for c in &plan.create {
        warn!(app = %c.app, cron = %c.cron, source = %c.source_id, "catalog drift: schedule missing");
    }
    for u in &plan.update {
        warn!(id = %u.schedule_id, app = %u.app, from = %u.from_cron, to = %u.to_cron, "catalog drift: cron/enabled mismatch");
    }
    for d in &plan.disable {
        warn!(id = %d.schedule_id, app = %d.app, reason = %d.reason, "catalog drift: schedule should be disabled");
    }
    for o in &plan.orphan {
        warn!(id = %o.schedule_id, app = %o.app, reason = %o.reason, "catalog drift: ORPHAN catalog-managed schedule (never auto-touched)");
    }
    if state.config.catalog.auto_reconcile {
        let applied = apply_reconcile_plan(state, &plan).await;
        info!(%applied, "catalog reconcile: auto-applied boot plan");
    }
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

/// The instant a schedule's cron is projected forward from: the most recent
/// point the scheduler has already accounted for.
///
/// That is the LATER of "when a job was last enqueued" (`last_run`) and "when
/// the `skip` misfire policy last advanced past missed firings"
/// (`last_skipped_at`), falling back to `created_at` for a schedule that has
/// done neither.
///
/// Both facts have to count. `last_run` alone would make a `skip` schedule
/// re-scan the same backlog on every tick forever (it advanced past those
/// firings precisely so it would not); `last_skipped_at` alone would forget
/// real runs. Before migration 0039 the skip path borrowed `last_run` to get
/// this effect, which is what made a row report a run that never happened.
///
/// Extracted so the reconcile loop and [`project_next_run`] cannot drift: the
/// projected `next_run` on `GET /schedules` is computed from the same reference
/// the next tick will use.
pub fn schedule_reference(schedule: &Schedule) -> DateTime<Utc> {
    [schedule.last_run, schedule.last_skipped_at]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(schedule.created_at)
}

/// Projects a schedule's next firing (read-only, for the observability API),
/// using the exact reference rule the reconcile loop does: the first cron time
/// strictly after [`schedule_reference`], evaluated in the schedule's timezone.
/// `None` if the cron is unparseable or has no future firing — so the API can
/// never disagree with the scheduler.
pub fn project_next_run(schedule: &Schedule) -> Option<DateTime<Utc>> {
    let cron = CronSchedule::from_str(&schedule.cron).ok()?;
    let tz = parse_tz(schedule.timezone.as_deref());
    let reference = schedule_reference(schedule);
    cron.after(&reference.with_timezone(&tz))
        .next()
        .map(|t| t.with_timezone(&Utc))
}

/// A schedule's most recent firing, read once and interpreted once.
///
/// The scheduler's overlap guard and the `GET /schedules` health derivation both
/// go through [`latest_run`] to build this, so there is no second place where
/// "is a run of this schedule still outstanding?" can be answered differently.
pub(crate) struct LatestRun {
    /// The most recent job this schedule enqueued, if any.
    pub job_id: Option<String>,
    /// That job's status.
    pub status: Option<String>,
    /// Whether that run still holds the schedule's firing slot — see
    /// [`run_holds_slot`].
    pub holds_slot: bool,
}

/// Whether a schedule's most recent run still holds its firing slot.
///
/// The anti-pattern this replaces: the guard was existential over ALL of the
/// schedule's jobs (`status IN ('queued','running')`) while health read only the
/// newest one. `POST /jobs/retry` on an OLD failed job of a schedule re-queues
/// it without touching `created_at`, and nothing ever clears `schedule_id` — so
/// the existential guard stayed true forever, the schedule silently stopped
/// firing, and `GET /schedules` answered `ok`. `POST /jobs/retry {app}` could
/// wedge every schedule of an app in one call.
///
/// Keying on the NEWEST firing keeps the guarantee the guard exists for — while
/// a scheduled run is queued/running it *is* the newest job, because the guard
/// itself prevented anything newer — and drops the wedge, since a retried older
/// job is by definition not the newest.
pub(crate) fn run_holds_slot(status: Option<&str>) -> bool {
    matches!(status, Some("queued") | Some("running"))
}

/// Reads a schedule's most recent firing and applies [`run_holds_slot`] to it.
pub(crate) async fn latest_run(state: &AppState, schedule_id: &str) -> anyhow::Result<LatestRun> {
    let latest = state.storage.latest_job_for_schedule(schedule_id).await?;
    let (job_id, status) = match latest {
        Some((id, status)) => (Some(id), Some(status)),
        None => (None, None),
    };
    Ok(LatestRun {
        holds_slot: run_holds_slot(status.as_deref()),
        job_id,
        status,
    })
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

/// The params a schedule's run would actually carry: the app's defaults with the
/// schedule's own params **shallow-merged over them** — byte-identical to what
/// `POST /apps/{name}/jobs` does with a request body (`routes::merge_params`).
///
/// Shared by three callers on purpose: the create door (`POST /schedules`
/// validates *this*, not the raw body), the fire path below, and the
/// `GET /schedules` health derivation. One resolution means the params the API
/// validated are the params the scheduler enqueues.
///
/// **This used to REPLACE rather than merge** (the schedule's params were used
/// verbatim whenever they were non-empty, defaults only when they were absent),
/// so the same app got different effective params depending on which door
/// created the work — while `routes::jobs`'s own doc comment claimed the two
/// agreed. Merging is the side that matches both that promise and
/// `ScrapeApp::default_params`'s contract ("params used for scheduled runs"):
/// the schedule's own keys still win, it only stops silently dropping the
/// defaults it didn't mention.
pub(crate) fn schedule_params(
    registry: &std::collections::HashMap<String, std::sync::Arc<dyn pumper_core::ScrapeApp>>,
    app: &str,
    params: &Value,
) -> Value {
    let defaults = registry
        .get(app)
        .map(|app| app.default_params())
        .unwrap_or(Value::Null);
    let empty = matches!(params, Value::Null) || matches!(params, Value::Object(m) if m.is_empty());
    let over = if empty { None } else { Some(params.clone()) };
    crate::routes::merge_params(defaults, over)
}

/// Whether a schedule's effective params pass the target app's declared schema
/// — the same check the enqueue door applies, so a schedule can never hold work
/// the door would refuse. `Err` carries the door's own pointer-path message.
///
/// Rows that predate the create-time check (or that were edited in SQL) are the
/// reason this runs on the FIRE path too, not just at the door.
pub(crate) fn validate_schedule_params(
    registry: &std::collections::HashMap<String, std::sync::Arc<dyn pumper_core::ScrapeApp>>,
    app: &str,
    params: &Value,
) -> Result<Value, String> {
    let effective = schedule_params(registry, app, params);
    crate::mcp::validate_app_params(registry, app, &effective)?;
    Ok(effective)
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
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, false, GRACE),
            Action::Idle
        );
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
            managed_by: None,
            last_run,
            last_skipped_at: None,
            skipped_count: 0,
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
        let ran = schedule(
            HOURLY,
            None,
            Some(Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap()),
        );
        assert_eq!(
            project_next_run(&ran),
            Some(Utc.with_ymd_and_hms(2026, 7, 13, 13, 0, 0).unwrap())
        );
    }

    #[test]
    fn project_next_run_none_on_bad_cron() {
        assert_eq!(project_next_run(&schedule("not a cron", None, None)), None);
    }

    /// `misfire_policy = "skip"` advances past missed firings; the reference has
    /// to move with it or the schedule re-scans the same backlog every tick
    /// forever. That advance used to be written to `last_run` — this pins that
    /// moving it to `last_skipped_at` did NOT break the projection.
    #[test]
    fn project_next_run_follows_a_skip_not_only_a_run() {
        let mut skipped = schedule(HOURLY, None, None);
        skipped.last_skipped_at = Some(Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap());
        assert_eq!(
            project_next_run(&skipped),
            Some(Utc.with_ymd_and_hms(2026, 7, 13, 13, 0, 0).unwrap()),
            "a skip advanced the schedule past 12:00, so the next firing is 13:00 — \
             not the 10:00 that projecting from created_at would give"
        );
    }

    /// The reference is the LATER of the two facts, whichever way round they
    /// happened — a schedule that ran at 12:00 and skipped at 09:00 is not
    /// dragged backwards, and vice versa.
    #[test]
    fn schedule_reference_takes_the_later_fact_not_a_fixed_column() {
        let created = Utc.with_ymd_and_hms(2026, 7, 13, 9, 15, 0).unwrap();
        let early = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
        let late = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();

        let mut s = schedule(HOURLY, None, None);
        assert_eq!(schedule_reference(&s), created, "neither fact yet");

        s.last_run = Some(late);
        s.last_skipped_at = Some(early);
        assert_eq!(schedule_reference(&s), late, "a real run is newer");

        s.last_run = Some(early);
        s.last_skipped_at = Some(late);
        assert_eq!(
            schedule_reference(&s),
            late,
            "a skip after the last run still moves the reference — otherwise the \
             skipped backlog is re-scanned on every tick, forever"
        );
    }

    /// The wedge, at predicate level: the guard used to be existential over ALL
    /// of a schedule's jobs, so ONE manually retried old job held the schedule's
    /// firing slot forever while `GET /schedules` read the newest job and said
    /// `ok`. One predicate over the newest run cannot produce that split.
    #[test]
    fn a_retried_older_job_does_not_hold_the_slot_only_the_newest_run_does() {
        // Newest run still going: the slot IS held — the guarantee the guard
        // exists for ("don't stack a second run on top of mine").
        assert!(run_holds_slot(Some("queued")));
        assert!(run_holds_slot(Some("running")));
        // Newest run finished: the slot is free, whatever any OLDER job of the
        // same schedule was manually retried into.
        for terminal in ["succeeded", "failed", "cancelled"] {
            assert!(
                !run_holds_slot(Some(terminal)),
                "a schedule whose newest run is '{terminal}' must fire again"
            );
        }
        // Never fired at all: nothing to overlap with.
        assert!(!run_holds_slot(None));
    }

    /// The EXPECTED-diff guard for "one predicate, two readers": the overlap
    /// question may be answered in exactly these places, and the health
    /// derivation must reach it through the shared read rather than
    /// re-deriving `queued`/`running` for itself. A second hand-rolled status
    /// match anywhere in the schedules surface is how the two drifted apart in
    /// the first place.
    #[test]
    fn health_and_guard_share_one_predicate() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let scheduler = std::fs::read_to_string(src.join("scheduler.rs")).expect("scheduler.rs");
        let route =
            std::fs::read_to_string(src.join("routes/schedules.rs")).expect("routes/schedules.rs");

        // Both readers go through the one read+interpret helper.
        assert!(
            scheduler.contains("latest_run(state, &schedule.id).await?.holds_slot"),
            "the scheduler's overlap guard must consult `latest_run`"
        );
        assert!(
            route.contains("crate::scheduler::latest_run("),
            "the health derivation must consult `latest_run`, not its own query"
        );

        // And only `run_holds_slot` itself decides what an active status is.
        let matches_active = |body: &str| {
            body.contains(r#"Some("queued") | Some("running")"#)
                || body.contains(r#"Some("running") | Some("queued")"#)
        };
        assert!(
            !matches_active(&route),
            "routes/schedules.rs re-derives the active-status set — the exact \
             divergence `run_holds_slot` exists to prevent"
        );
        let outside_predicate = scheduler
            .split("pub(crate) fn run_holds_slot")
            .next()
            .expect("split yields the text before the predicate");
        assert!(
            !matches_active(outside_predicate),
            "scheduler.rs answers the overlap question somewhere other than \
             `run_holds_slot`"
        );

        // The divergent existential twin must stay gone.
        assert!(
            !scheduler.contains("schedule_has_active_job"),
            "the existential overlap query is back; it is what wedged schedules \
             on `POST /jobs/retry`"
        );
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
