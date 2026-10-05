-- Schedule decision ledger: every tick's verdict on a schedule, not just the
-- ones that fired.
--
-- Before this table a schedule's non-fires left either nothing at all or a
-- reason-free pair of columns. The overlap guard recorded NOTHING by design
-- (recording would have moved the cron reference past the firing the guard just
-- promised to keep due); a per-schedule storage error inside the pass was an
-- aggregate counter; and `schedules.last_skipped_at` + `skipped_count`
-- (migration 0039) could not tell a retirement skip from a misfire skip,
-- because they are byte-identical in the row. So "why did my nightly scrape not
-- run?" was answerable only from the server log, and for the most common
-- non-fire not even from there.
--
-- This is the schedules-shaped twin of `trigger_runs` (migration 0036): one row
-- per decision, negatives included, keyset-paged newest-first and bounded by age
-- from the worker's reaper tick. Diagnostic, not evidence — unlike the opt-in
-- `LEDGER_TABLES` whose deletion is data loss.
CREATE TABLE IF NOT EXISTS schedule_runs (
    id            TEXT PRIMARY KEY,
    -- The schedule the decision is about, or '*' for the pass heartbeat, which
    -- is a fact about the reconcile LOOP rather than about any one schedule
    -- (see `crate::storage::SCHEDULE_PASS_ID`).
    schedule_id   TEXT NOT NULL,
    -- What the tick did: one of `crate::storage::SCHEDULE_OUTCOMES`.
    outcome       TEXT NOT NULL,
    -- WHY, in the admission vocabulary the fire path and `GET /schedules`
    -- already share (`ScheduleAdmission::health`) — so the ledger adds no fourth
    -- authority to this decision. NULL when admission never ran (a storage
    -- failure inside the step), where `detail` carries the error instead.
    reason        TEXT,
    -- The firing this row is about: the first cron time after the schedule's
    -- reference. NULL for the heartbeat and for decisions taken before a firing
    -- could be projected.
    due_at        TEXT,
    -- The job enqueued ('fired'/'skipped_and_fired'), or the job still holding
    -- the slot ('held') — which is the one the operator has to go look at.
    job_id        TEXT,
    -- Firings advanced past by `misfire_policy = "skip"`. The number the log
    -- line used to compute, print and throw away while `skipped_count` moved
    -- by exactly +1 whatever its size.
    missed        INTEGER NOT NULL DEFAULT 0,
    -- Free-text context: an error message, the retired dataset, the collapsed
    -- backlog size.
    detail        TEXT,
    created_at    TEXT NOT NULL
);

-- The read path: one schedule's decisions, newest first
-- (GET /schedules/{id}/runs).
CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule
    ON schedule_runs (schedule_id, created_at DESC, id DESC);
-- The prune path: age-ordered sweep.
CREATE INDEX IF NOT EXISTS idx_schedule_runs_created ON schedule_runs (created_at);
