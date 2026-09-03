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
    // So a contained panic's `file:line:col` survives into the log line below.
    crate::worker::install_panic_location_hook();
    // Parsed crons cached across ticks, keyed by expression string, so we don't
    // re-parse every schedule's cron on every tick (an edited cron is a new key and
    // re-parses). Lives here so it outlives a single reconcile.
    let mut cron_cache: HashMap<String, CronSchedule> = HashMap::new();
    // When the previous reconcile pass ran. This is what lets "missed" mean "a
    // pass already had its chance at this firing" rather than "older than a
    // constant derived from the configured tick" — see [`misfire_cutoff`].
    let mut last_pass: Option<DateTime<Utc>> = None;
    loop {
        if state.shutdown.is_cancelled() {
            break;
        }
        let now = Utc::now();
        // Panic containment. This task IS the process heartbeat — cron, the
        // stuck-job reaper, the webhook dead-letter drain, the cache refresher
        // and the DataHub governance poll all ride it — and it is spawned
        // unjoined, so before this an unwind anywhere inside a tick killed all
        // five permanently while the HTTP server kept answering, with no log
        // line and no restart.
        //
        // Containment sits HERE rather than at the spawn boundary (supervised
        // respawn) on purpose: respawning `run` would re-run `boot_reconcile`
        // and throw away the cross-tick `cron_cache`, i.e. a panic would
        // silently change scheduling behaviour. Catching around the tick body
        // keeps every cross-tick fact and costs one `AssertUnwindSafe`.
        //
        // `AssertUnwindSafe` over the future: the state that outlives a tick is
        // `cron_cache` (a pure memo of parsed cron expressions mutated by one
        // non-async `entry().or_insert()` that no unwind can interleave) and
        // `last_pass` (advanced out here, never inside the tick). Everything
        // else is read fresh from `state` on the next tick.
        let ticked = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(tick_once(
            &state,
            &mut cron_cache,
            last_pass,
            now,
        )))
        .await;
        match ticked {
            // Only a pass that actually completed counts as "a pass had its
            // chance at this firing".
            Ok(true) => last_pass = Some(now),
            Ok(false) => {}
            Err(payload) => error!(
                "scheduler tick PANICKED and was contained; the loop continues (cron, the \
                 stuck-job reaper, the webhook dead-letter drain, the cache refresher and the \
                 DataHub governance poll all ride this tick): {}",
                crate::worker::panic_error(
                    &*payload,
                    crate::worker::take_panic_location().as_deref()
                )
            ),
        }
        // Stop enqueuing new scheduled work as soon as shutdown is signalled.
        tokio::select! {
            _ = state.shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
    }
    info!("scheduler stopped");
}

/// One tick: the cron reconcile plus the four periodic jobs that ride this loop
/// rather than owning a timer of their own.
///
/// Returns whether the reconcile *pass* completed — the only thing that may
/// advance `last_pass`. A pass that failed at `list_schedules` never looked at a
/// single schedule and so gave no firing its chance.
async fn tick_once(
    state: &AppState,
    cron_cache: &mut HashMap<String, CronSchedule>,
    last_pass: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    let completed = match reconcile(state, cron_cache, last_pass, now).await {
        Ok(tally) => {
            if tally.acted() {
                info!(?tally, "scheduler pass");
            }
            true
        }
        Err(e) => {
            error!("scheduler reconcile failed: {e}");
            false
        }
    };
    // Piggyback the scheduler tick to run the stuck-job reaper: re-queue
    // running jobs whose heartbeat lease has gone stale (a hung task on a
    // live server). Cheap — one indexed scan of `running` jobs. The hourly
    // trigger-decision-ledger prune is nested inside it.
    crate::worker::reap_once(state).await;
    // Also piggyback the webhook dead-letter drain: re-send failed deliveries
    // whose backoff is due, so a receiver outage longer than the in-process
    // retry loop doesn't mean permanent silent event loss.
    if state.config.webhooks.auto_retry {
        crate::webhook::drain_due(state).await;
    }
    // And the cache refresher ([refresher], default OFF): revalidate cached
    // entries whose learned change cadence says a change is near — spawned
    // (non-blocking) and strictly idle-slot via Governor::try_acquire, so
    // it can neither delay this loop nor crowd out live traffic.
    crate::refresher::tick(state);
    // And the DataHub governance pull ([datahub] govern, default OFF):
    // interval-gated and spawned — deprecations/tags/assertions in DataHub
    // become schedule disables, Claude-tier pauses, and immediate syncs.
    crate::datahub::govern_tick(state);
    completed
}

/// What one schedule's step did this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepOutcome {
    /// Nothing was due.
    Idle,
    /// A job was enqueued.
    Fired,
    /// `misfire_policy = "skip"` advanced past missed firings; nothing ran.
    Skipped,
    /// Both, in one tick: the genuinely-missed firings were advanced past AND
    /// the on-time firing that shared the tick was enqueued.
    SkippedAndFired,
    /// The overlap guard held the firing (a previous run still owns the slot).
    Held,
    /// A door gate refused the row (unparseable cron, unregistered app, invalid
    /// params, or a row disabled/removed since the pass read it). Nothing is
    /// recorded, so fixing the row makes the firing happen.
    Refused,
}

/// What one reconcile pass did, in aggregate.
///
/// Exists so per-schedule failure is *countable* rather than fatal: before this,
/// a single schedule's storage error propagated out of the loop with `?` and
/// every alphabetically-later schedule (rows come back `ORDER BY app`) silently
/// never fired, while `GET /schedules` still answered `health: "ok"`.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct PassTally {
    pub considered: usize,
    pub fired: usize,
    pub skipped: usize,
    pub held: usize,
    pub refused: usize,
    pub failed: usize,
    /// The shutdown token fired mid-pass, so the remaining schedules were left
    /// for the next boot rather than enqueued into a draining queue.
    pub stopped_early: bool,
}

impl PassTally {
    /// Folds ONE schedule's step result into the pass.
    ///
    /// The anti-pattern this replaces: `?` on a per-schedule storage call inside
    /// the reconcile loop. This function **cannot** propagate — an `Err` is
    /// logged with the schedule's id and counted, and the caller keeps looping —
    /// which is the whole isolation guarantee, expressed as a signature.
    fn absorb(&mut self, id: &str, app: &str, result: anyhow::Result<StepOutcome>) {
        self.considered += 1;
        match result {
            Ok(StepOutcome::Idle) => {}
            Ok(StepOutcome::Fired) => self.fired += 1,
            Ok(StepOutcome::Skipped) => self.skipped += 1,
            Ok(StepOutcome::SkippedAndFired) => {
                self.skipped += 1;
                self.fired += 1;
            }
            Ok(StepOutcome::Held) => self.held += 1,
            Ok(StepOutcome::Refused) => self.refused += 1,
            Err(e) => {
                self.failed += 1;
                error!(
                    id = %id, app = %app,
                    "schedule step failed; the pass CONTINUES with the remaining schedules: {e}"
                );
            }
        }
    }

    /// Whether this pass did anything worth a log line.
    fn acted(&self) -> bool {
        self.fired + self.skipped + self.held + self.refused + self.failed > 0 || self.stopped_early
    }
}

/// `now` is a parameter (the same rule `decide` follows) so a test can drive a
/// reconcile pass deterministically without waiting for wall-clock time.
/// `last_pass` is when the previous pass ran (`None` = first pass since boot) —
/// see [`misfire_cutoff`].
pub(crate) async fn reconcile(
    state: &AppState,
    cron_cache: &mut HashMap<String, CronSchedule>,
    last_pass: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> anyhow::Result<PassTally> {
    // Floor for the first pass after boot, when there is no previous pass to
    // measure against: a firing more than two configured ticks late was missed
    // while the scheduler was down.
    let grace = chrono::Duration::seconds(state.config.worker.schedule_tick_secs.max(1) as i64 * 2);
    let cutoff = misfire_cutoff(last_pass, now, grace);
    let mut tally = PassTally::default();
    for schedule in state.storage.list_schedules().await? {
        if !schedule.enabled {
            continue;
        }
        // Shutdown is honoured BETWEEN schedules: the token can fire while the
        // pass is half-way down the table, and enqueuing into a queue that is
        // already draining just re-queues work at the next boot with a
        // `last_run` that claims it ran.
        if state.shutdown.is_cancelled() {
            tally.stopped_early = true;
            info!(
                considered = tally.considered,
                "shutdown signalled mid-pass; enqueuing no further scheduled work"
            );
            break;
        }
        let cron = if let Some(cron) = cron_cache.get(&schedule.cron) {
            cron
        } else {
            match CronSchedule::from_str(&schedule.cron) {
                Ok(cron) => cron_cache.entry(schedule.cron.clone()).or_insert(cron),
                Err(e) => {
                    warn!(id = %schedule.id, cron = %schedule.cron, "invalid cron: {e}");
                    tally.absorb(&schedule.id, &schedule.app, Ok(StepOutcome::Refused));
                    continue;
                }
            }
        };
        // Per-schedule panic containment, inside the per-tick one: an app whose
        // `default_params()` unwinds is reached INLINE from here (through
        // `validate_schedule_params`), and one such app must not cost every
        // other schedule its pass.
        let step = reconcile_one(state, &schedule, cron, cutoff, now);
        let outcome =
            match futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(step)).await {
                Ok(outcome) => outcome,
                Err(payload) => Err(anyhow::anyhow!(
                    "{}",
                    crate::worker::panic_error(
                        &*payload,
                        crate::worker::take_panic_location().as_deref()
                    )
                )),
            };
        tally.absorb(&schedule.id, &schedule.app, outcome);
    }
    Ok(tally)
}

/// One schedule's whole step: decide, gate, act.
///
/// Extracted from the reconcile loop so the failure of *this* schedule is a
/// value the caller absorbs ([`PassTally::absorb`]) rather than a `?` that ends
/// the pass — and so the step is drivable directly from a test.
pub(crate) async fn reconcile_one(
    state: &AppState,
    schedule: &Schedule,
    cron: &CronSchedule,
    cutoff: DateTime<Utc>,
    now: DateTime<Utc>,
) -> anyhow::Result<StepOutcome> {
    let tz = parse_tz(schedule.timezone.as_deref());
    // Next firing after this schedule's cron reference (see the shared
    // `schedule_reference`, which `project_next_run` uses too).
    let reference = schedule_reference(schedule);
    let misfire_skip = schedule.misfire_policy == "skip";

    let (missed, collapsed, fires) = match decide(cron, tz, reference, now, misfire_skip, cutoff) {
        Action::Idle => return Ok(StepOutcome::Idle),
        Action::Fire { collapsed } => (0, collapsed, true),
        Action::Skip { missed } => (missed, 0, false),
        Action::SkipThenFire { missed } => (missed, 0, true),
    };

    // ---- Door gates ---------------------------------------------------------
    // These apply to EVERY acting branch, `skip` included. A `skip` schedule on
    // an unregistered app used to accrue `skipped_count` forever while
    // `GET /schedules` reported `health: "unregistered_app"` — the row "ate"
    // firings that could never have run in the first place, and the count of
    // eaten firings said work had been dropped when none was ever runnable.
    // Refusing without recording keeps ONE contract for both policies: fixing
    // the row makes it fire, because nothing advanced while it was broken.
    if !state.registry.contains_key(&schedule.app) {
        warn!(
            id = %schedule.id, app = %schedule.app,
            "schedule references unregistered app; neither firing nor recording a skip advance"
        );
        return Ok(StepOutcome::Refused);
    }
    // Door parity: a schedule whose effective params fail the app's declared
    // schema is REFUSED, not enqueued — the same 422 the HTTP door answers,
    // moved to the only place a legacy row can still be caught. Neither
    // `last_run` nor `last_skipped_at` is touched, so fixing the row makes the
    // schedule fire again instead of having silently eaten firings.
    // Visible on `GET /schedules` as `health: "invalid_params"`.
    let params = match validate_schedule_params(&state.registry, &schedule.app, &schedule.params) {
        Ok(params) => params,
        Err(msg) => {
            warn!(
                id = %schedule.id, app = %schedule.app,
                "schedule params fail the app's declared schema; neither firing nor recording a \
                 skip advance (GET /schedules shows health=invalid_params): {msg}"
            );
            return Ok(StepOutcome::Refused);
        }
    };

    if !fires {
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
        return Ok(StepOutcome::Skipped);
    }

    // Overlap guard: don't stack a second run while the previous one
    // is still queued/running. last_run is NOT touched, so the missed
    // firing stays due and fires on the first tick after it finishes.
    // The SAME read + predicate `GET /schedules` reports as
    // `health: "overlapping"` — see `latest_run`. Nothing is recorded here, the
    // skip advance included: recording it would move the reference past the very
    // firing the guard just promised to keep due.
    if latest_run(state, &schedule.id).await?.holds_slot {
        info!(id = %schedule.id, app = %schedule.app, "previous scheduled run still active; skipping tick");
        return Ok(StepOutcome::Held);
    }

    // Fire-time re-check against the LIVE row rather than the pass's snapshot —
    // see `still_firable`.
    let fresh = state.storage.get_schedule(&schedule.id).await?;
    if !still_firable(fresh.as_ref()) {
        info!(
            id = %schedule.id, app = %schedule.app,
            "schedule was disabled or removed after this pass read it; not enqueuing"
        );
        return Ok(StepOutcome::Refused);
    }

    let max_attempts = schedule
        .max_attempts
        .unwrap_or(DEFAULT_SCHEDULE_MAX_ATTEMPTS);
    let target_key = crate::mcp::target_key_for(&state.registry, &schedule.app, &params);
    let opts = EnqueueOptions {
        params,
        max_attempts,
        priority: schedule.priority,
        schedule_id: Some(schedule.id.clone()),
        target_key,
        // The schedule's own ceiling, off the live row — see `firing_budget`.
        // This field is why the fire path may not build its options from
        // `Default`: `budget_usd: None` is "no ceiling", so every scheduled run
        // of a Claude-tier app used to be unlimited.
        budget_usd: firing_budget(fresh.as_ref(), schedule),
        ..Default::default()
    };
    let job = state.storage.enqueue(&schedule.app, opts).await?;
    if collapsed > 0 {
        info!(id = %schedule.id, app = %schedule.app, job = %job.id, collapsed, "scheduled run fired (missed firings collapsed into one)");
    } else {
        info!(id = %schedule.id, app = %schedule.app, job = %job.id, "scheduled run fired");
    }
    state.storage.touch_schedule(&schedule.id, now).await?;
    state.notify.notify_one();
    if missed == 0 {
        return Ok(StepOutcome::Fired);
    }
    // Skip-then-fire: the enqueue happens FIRST on purpose. `record_schedule_skip`
    // stamps `last_skipped_at = now`, which advances the cron reference past
    // every pending firing — the on-time one included. Recording it ahead of a
    // failed enqueue would eat the very run this branch exists to preserve.
    info!(
        id = %schedule.id, app = %schedule.app, missed,
        "misfire policy 'skip': advanced past missed firings AND ran the on-time firing that \
         shared this tick"
    );
    state
        .storage
        .record_schedule_skip(&schedule.id, now, missed)
        .await?;
    Ok(StepOutcome::SkippedAndFired)
}

/// Whether a schedule the pass read earlier may still be fired *now*.
///
/// The anti-pattern this replaces: enqueuing from a snapshot. `list_schedules`
/// reads the whole table at the top of a pass; by the time an
/// alphabetically-later row is reached, `POST /schedules/{id}/enabled {false}`,
/// a catalog reconcile, or a DataHub governance disable may already have turned
/// it off — and the tick would still enqueue a paid run from the stale row and
/// stamp `last_run` on a schedule the operator had just stopped. A deleted row
/// (`None`) is refused for the same reason.
pub(crate) fn still_firable(fresh: Option<&Schedule>) -> bool {
    matches!(fresh, Some(s) if s.enabled)
}

/// The spend ceiling one firing must carry (migration 0040). `None` = no ceiling.
///
/// The anti-pattern this replaces: `EnqueueOptions { ..Default::default() }` on
/// the fire path, which handed every scheduled run `budget_usd: None`. Schedules
/// were then the last work-creator without a ceiling — an unattended, recurring,
/// paid standing order was the one work shape that could spend without limit,
/// while the same app enqueued at the jobs door honoured its cap.
///
/// The value is read off the row the fire step re-read for [`still_firable`],
/// not off the pass snapshot, because a ceiling is a money decision the operator
/// made *before* this enqueue: `POST /schedules/{id}/budget` landing while the
/// pass walks the table must bind this firing, not merely the next one. The
/// asymmetry with `params`/`priority` (which stay on the snapshot, where they
/// were validated) is deliberate — neither of those is a spend, and re-reading
/// costs nothing here since the row is already in hand. `fresh = None` cannot
/// fire at all, so the snapshot only backstops the signature.
pub(crate) fn firing_budget(fresh: Option<&Schedule>, snapshot: &Schedule) -> Option<f64> {
    fresh.map_or(snapshot.budget_usd, |live| live.budget_usd)
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
    /// `misfire_policy = skip`, and the pending firings are of BOTH kinds:
    /// advance past `missed` genuinely-missed ones **and** enqueue the on-time
    /// firing that shares this tick.
    ///
    /// This variant is why classification is per firing rather than per batch.
    /// `skip` used to classify the whole pending batch from its OLDEST member,
    /// so an hourly schedule that missed 11:00 while the process was down and
    /// came back at 12:00:05 produced `Skip { missed: 2 }` — and the
    /// legitimately-due 12:00 run was dropped along with the one really missed.
    SkipThenFire { missed: usize },
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

/// The instant that separates a **missed** firing from an **on-time** one.
///
/// "Missed" has to mean *a previous pass already had the chance to fire this and
/// didn't*, not *older than a constant*. Deriving the line from the CONFIGURED
/// tick (`grace = schedule_tick_secs × 2`) made it a statement about the config
/// rather than about what actually happened: any tick that ran long — and the
/// webhook dead-letter drain runs in-line on this loop, by design — reclassified
/// a firing the process was up for as a misfire, and under
/// `misfire_policy = "skip"` that ate a real, on-time run.
///
/// So when there IS a previous pass, its timestamp is the line exactly: a firing
/// after it has been seen by no pass and is on-time *by construction*, however
/// long the tick took. The configured grace survives as the floor for the first
/// pass after boot, where there is no previous pass and a genuine downtime
/// backlog is precisely what we expect to find.
pub(crate) fn misfire_cutoff(
    last_pass: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    grace: chrono::Duration,
) -> DateTime<Utc> {
    last_pass.unwrap_or(now - grace)
}

/// Pending firings beyond the first that this one run folds up — diagnostic only.
fn collapse_count(cron: &CronSchedule, reference: &DateTime<Tz>, now: &DateTime<Tz>) -> usize {
    // Fire enqueues ONE run no matter how many firings are pending, and the
    // overlap guard can keep this schedule "due" for many ticks — so bound the
    // enumeration to a small cap instead of re-walking the whole growing
    // backlog every tick. Exact for realistic backlogs, saturating at the cap.
    let mut collapsed = 0usize;
    for fire in cron.after(reference).skip(1) {
        if fire > *now {
            break;
        }
        collapsed += 1;
        if collapsed >= COLLAPSE_LOG_CAP {
            break;
        }
    }
    collapsed
}

/// Pending firings a previous pass already had its chance at (`fire <= cutoff`).
///
/// Bounded by `now` as well, so a cutoff in the future can never count a firing
/// that has not happened yet as missed.
fn missed_count(
    cron: &CronSchedule,
    reference: &DateTime<Tz>,
    cutoff: &DateTime<Tz>,
    now: &DateTime<Tz>,
) -> usize {
    let bound = std::cmp::min(cutoff, now);
    // Skip advances past ALL missed firings, so it needs the exact count — but
    // this happens once (the pass then advances the reference), not every tick.
    let mut missed = 0usize;
    for fire in cron.after(reference) {
        if fire > *bound {
            break;
        }
        missed += 1;
        if missed >= MAX_MISFIRE_SCAN {
            break;
        }
    }
    missed
}

/// Whether a firing **no pass has seen yet** is due in this tick.
///
/// O(1): one iterator step from the later of the schedule's own reference and
/// the cutoff. That matters because [`MAX_MISFIRE_SCAN`] bounds the missed
/// count — walking for the on-time firing instead would let a pathological
/// backlog hide it behind the cap, and the skip advance (which stamps
/// `last_skipped_at = now`) would then eat it.
fn has_due_on_time_firing(
    cron: &CronSchedule,
    reference: &DateTime<Tz>,
    cutoff: &DateTime<Tz>,
    now: &DateTime<Tz>,
) -> bool {
    let from = std::cmp::max(reference, cutoff);
    cron.after(from).next().is_some_and(|fire| fire <= *now)
}

/// Decides a schedule's action this tick — pure (no I/O), so it is unit-testable
/// against simulated downtime and DST boundaries.
///
/// The cron is evaluated in `tz` (a firing at a nonexistent local wall-clock time
/// — e.g. inside a spring-forward gap — is skipped by the cron iterator).
/// `cutoff` ([`misfire_cutoff`]) is the line between missed and on-time: a
/// pending firing at or before it was already a previous pass's to run.
///
/// Under `misfire_policy = "skip"` the classification is **per firing**, not per
/// batch: the missed ones are advanced past and an on-time firing sharing the
/// same tick still runs ([`Action::SkipThenFire`]). `fire_once` is unchanged —
/// it collapses the whole pending backlog into one run by definition, so it has
/// nothing to classify.
fn decide(
    cron: &CronSchedule,
    tz: Tz,
    reference: DateTime<Utc>,
    now: DateTime<Utc>,
    misfire_skip: bool,
    cutoff: DateTime<Utc>,
) -> Action {
    let reference_tz = reference.with_timezone(&tz);
    let now_tz = now.with_timezone(&tz);

    // Anything pending at all? One iterator step: firings come out increasing,
    // so if the first is still in the future nothing is due. This avoids
    // enumerating a backlog just to find that out.
    match cron.after(&reference_tz).next() {
        Some(fire) if fire <= now_tz => {}
        _ => return Action::Idle,
    }

    if !misfire_skip {
        return Action::Fire {
            collapsed: collapse_count(cron, &reference_tz, &now_tz),
        };
    }

    let cutoff_tz = cutoff.with_timezone(&tz);
    let missed = missed_count(cron, &reference_tz, &cutoff_tz, &now_tz);
    if missed == 0 {
        // Everything pending is on-time. `skip` skips *missed* firings, and
        // there are none — so this is an ordinary run.
        return Action::Fire {
            collapsed: collapse_count(cron, &reference_tz, &now_tz),
        };
    }
    if has_due_on_time_firing(cron, &reference_tz, &cutoff_tz, &now_tz) {
        Action::SkipThenFire { missed }
    } else {
        Action::Skip { missed }
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
    /// Every quarter hour — the cadence at which a slow tick and the configured
    /// grace window can actually disagree.
    const QUARTER_HOURLY: &str = "0 0,15,30,45 * * * *";
    const GRACE: chrono::Duration = chrono::Duration::seconds(30);

    /// The cutoff a FIRST pass after boot computes (no previous pass, so the
    /// configured grace is the floor). Spelled through the shipped
    /// [`misfire_cutoff`] so these cases stay tied to the real rule.
    fn boot_cutoff(now: DateTime<Utc>, grace: chrono::Duration) -> DateTime<Utc> {
        misfire_cutoff(None, now, grace)
    }

    #[test]
    fn idle_when_next_firing_is_in_the_future() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        // Next hourly firing after 12:00 is 13:00 — not yet due.
        assert_eq!(
            decide(
                &cron(HOURLY),
                Tz::UTC,
                reference,
                now,
                false,
                boot_cutoff(now, GRACE)
            ),
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
            budget_usd: None,
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
        // Only the NON-test half of each file is scanned. A source-scanning test
        // that reads its own module matches its own needles: every "this string
        // must be absent" assertion is then trivially false, and every "must be
        // present" one trivially true — an inventory guard that cannot fail is
        // worse than none, because it reads as coverage.
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let code = |name: &str| {
            let body =
                std::fs::read_to_string(src.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
            body.split("#[cfg(test)]")
                .next()
                .expect("split yields the pre-test-module source")
                .to_string()
        };
        let scheduler = code("scheduler.rs");
        let route = code("routes/schedules.rs");
        assert!(
            !scheduler.contains("fn health_and_guard_share_one_predicate"),
            "the scanned slice still includes this test module"
        );

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

        // The divergent existential twin must stay gone. Spelled in two pieces
        // so this line is not itself a match for the name it forbids.
        let existential_twin = concat!("schedule_has", "_active_job");
        assert!(
            !scheduler.contains(existential_twin),
            "the existential overlap query is back; it is what wedged schedules \
             on `POST /jobs/retry`"
        );
    }

    /// The guard above scans source text, so it has to be able to FAIL. This
    /// drives it against text that violates each rule and asserts the scan says
    /// so — the meta-test the self-matching version could never have passed.
    #[test]
    fn the_shared_predicate_guard_can_actually_fail() {
        let matches_active = |body: &str| {
            body.contains(r#"Some("queued") | Some("running")"#)
                || body.contains(r#"Some("running") | Some("queued")"#)
        };
        assert!(
            matches_active(r#"let active = matches!(s, Some("queued") | Some("running"));"#),
            "a hand-rolled active-status match must be detected"
        );
        assert!(
            matches_active(r#"matches!(s, Some("running") | Some("queued"))"#),
            "...in either order"
        );
        assert!(!matches_active("last.holds_slot"));
        let existential_twin = concat!("schedule_has", "_active_job");
        assert!("state.storage.schedule_has_active_job(&id)".contains(existential_twin));
    }

    /// The starvation bug, at the level it lived: one schedule's storage error
    /// propagated out of the reconcile loop with `?`, so the pass ended there
    /// and every alphabetically-later schedule (`list_schedules` orders by app)
    /// silently never fired — while `GET /schedules` still answered `ok`.
    ///
    /// [`PassTally::absorb`] cannot propagate: an `Err` is counted and the
    /// caller keeps looping. Driving the fold over an ordered mix proves the
    /// schedules *after* the failure still get their turn.
    #[test]
    fn one_bad_schedule_does_not_starve_the_rest() {
        let mut tally = PassTally::default();
        let steps: Vec<(&str, anyhow::Result<StepOutcome>)> = vec![
            ("a-early", Ok(StepOutcome::Fired)),
            (
                "m-broken",
                Err(anyhow::anyhow!("database is locked (5) on touch_schedule")),
            ),
            ("z-late", Ok(StepOutcome::Skipped)),
        ];
        for (id, result) in steps {
            tally.absorb(id, "demo", result);
        }
        assert_eq!(
            tally,
            PassTally {
                considered: 3,
                fired: 1,
                skipped: 1,
                failed: 1,
                ..PassTally::default()
            },
            "the schedule after the failing one must still have been stepped"
        );
        assert!(tally.acted(), "a pass with any outcome is worth a log line");
    }

    /// Every outcome the step can produce lands in exactly one counter — an
    /// `Idle` pass in particular must stay silent, or the log line the tally
    /// drives fires every tick for a table of not-yet-due schedules.
    #[test]
    fn an_idle_pass_is_silent_not_logged_every_tick() {
        let mut tally = PassTally::default();
        for _ in 0..3 {
            tally.absorb("s", "demo", Ok(StepOutcome::Idle));
        }
        assert_eq!(tally.considered, 3);
        assert!(!tally.acted());
        tally.absorb("s", "demo", Ok(StepOutcome::Held));
        assert!(tally.acted(), "a held firing IS worth saying out loud");
    }

    #[test]
    fn on_time_firing_runs_under_both_policies() {
        // Firing at 12:00:00 detected 30s later — within grace, so on-time.
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 11, 30, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        let cutoff = boot_cutoff(now, chrono::Duration::seconds(60));
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, false, cutoff),
            Action::Fire { collapsed: 0 }
        );
        // skip only skips *missed* firings; an on-time one still runs.
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, true, cutoff),
            Action::Fire { collapsed: 0 }
        );
    }

    /// The load-bearing invariant, in the case that used to break it: an on-time
    /// firing shares a tick with an older one that really was missed.
    ///
    /// Hourly schedule, last run 10:00, the process was down across 11:00 and is
    /// back at 12:00:05. Classifying the batch by its OLDEST member called BOTH
    /// pending firings misfires (`Skip { missed: 2 }`), so `skip` silently
    /// dropped the 12:00 run the process was up and due for — the policy is
    /// "don't catch up", not "don't run".
    #[test]
    fn a_shared_tick_skips_the_missed_firing_and_still_runs_the_on_time_one() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 10, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 5).unwrap();
        let cutoff = boot_cutoff(now, chrono::Duration::seconds(60));
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, true, cutoff),
            Action::SkipThenFire { missed: 1 },
            "11:00 was missed and is advanced past; the due 12:00 firing still runs"
        );
        // `fire_once` is untouched: it collapses the whole backlog into one run.
        assert_eq!(
            decide(&cron(HOURLY), Tz::UTC, reference, now, false, cutoff),
            Action::Fire { collapsed: 1 }
        );
    }

    /// A tick that ran long must not manufacture misfires. The webhook
    /// dead-letter drain runs in-line on this loop, so a tick's duration is not
    /// a property the config describes — and while grace was `tick × 2`, a
    /// 20-minute tick turned a firing the process was up for into an eaten one.
    #[test]
    fn a_slow_tick_does_not_manufacture_a_misfire() {
        // Quarter-hourly, fired at 11:00. The previous pass ran at 11:00:02;
        // this one at 11:20 because the tick took twenty minutes. 11:15 is
        // pending, and NO pass has ever seen it.
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 11, 0, 0).unwrap();
        let last_pass = Utc.with_ymd_and_hms(2026, 7, 13, 11, 0, 2).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 11, 20, 0).unwrap();
        let grace = chrono::Duration::seconds(60); // schedule_tick_secs = 30

        assert_eq!(
            decide(
                &cron(QUARTER_HOURLY),
                Tz::UTC,
                reference,
                now,
                true,
                misfire_cutoff(Some(last_pass), now, grace)
            ),
            Action::Fire { collapsed: 0 },
            "a firing due since the last pass is on-time by construction"
        );
        // The same instant under the OLD rule (grace floor only, i.e. what every
        // tick used to compute): 11:15 is more than 60s behind 11:20, so `skip`
        // ate a run the process was up for the whole time.
        assert_eq!(
            decide(
                &cron(QUARTER_HOURLY),
                Tz::UTC,
                reference,
                now,
                true,
                boot_cutoff(now, grace)
            ),
            Action::Skip { missed: 1 },
            "this is the behaviour threading the previous pass through fixes"
        );
    }

    #[test]
    fn fire_once_collapses_a_downtime_backlog_into_one_run() {
        // Simulated downtime: last run 08:00, back at 12:00:30 — 09/10/11/12 missed.
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        assert_eq!(
            decide(
                &cron(HOURLY),
                Tz::UTC,
                reference,
                now,
                false,
                boot_cutoff(now, GRACE)
            ),
            Action::Fire { collapsed: 3 }
        );
    }

    /// A genuine downtime backlog with nothing on-time in it still skips whole —
    /// the r11 behaviour the skip advance and `schedule_reference` are pinned to.
    /// The 12:00 firing IS the oldest-within-grace one here, so it counts as
    /// missed and the backlog is 4, exactly as before.
    #[test]
    fn skip_advances_past_a_downtime_backlog_without_running() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 8, 0, 0).unwrap();
        let now = Utc.with_ymd_and_hms(2026, 7, 13, 12, 0, 30).unwrap();
        assert_eq!(
            decide(
                &cron(HOURLY),
                Tz::UTC,
                reference,
                now,
                true,
                boot_cutoff(now, GRACE)
            ),
            Action::Skip { missed: 4 }
        );
    }

    /// The fire-time re-check, at predicate level. The pass works from a
    /// snapshot taken by one `list_schedules`, and a disable can land between
    /// that read and this row's enqueue — from the API, a catalog reconcile or a
    /// DataHub governance action. Firing anyway spends money on a schedule the
    /// operator has already stopped, and stamps `last_run` to prove it ran.
    #[test]
    fn a_disabled_row_is_not_fired_from_a_stale_snapshot() {
        let mut live = schedule(HOURLY, None, None);
        assert!(still_firable(Some(&live)), "an enabled row still fires");
        live.enabled = false;
        assert!(!still_firable(Some(&live)));
        // Deleted between the snapshot and the enqueue: same answer.
        assert!(!still_firable(None));
    }

    /// The money half of the same re-read. A ceiling set on a schedule while the
    /// pass is walking the table is an instruction about *this* firing — the
    /// operator placed it before the enqueue happened — so replaying the
    /// snapshot's ceiling would let exactly one more unbounded run out of the
    /// door after the call that was meant to stop it.
    #[test]
    fn the_firing_budget_is_the_live_rows_not_the_pass_snapshots() {
        let snapshot = schedule(HOURLY, None, None);
        assert_eq!(
            firing_budget(Some(&snapshot), &snapshot),
            None,
            "no ceiling anywhere stays no ceiling — this feature invents none"
        );

        // Set mid-pass: the firing this step is about must already honour it.
        let mut live = snapshot.clone();
        live.budget_usd = Some(2.0);
        assert_eq!(firing_budget(Some(&live), &snapshot), Some(2.0));

        // ...and cleared mid-pass, the other direction: the operator lifted the
        // ceiling, so the stale one must not keep capping the run.
        let mut capped = snapshot.clone();
        capped.budget_usd = Some(2.0);
        live.budget_usd = None;
        assert_eq!(firing_budget(Some(&live), &capped), None);
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
