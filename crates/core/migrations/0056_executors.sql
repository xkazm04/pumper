-- N18: the elastic executor plane.
--
-- Before this, "which process is running this job?" had exactly one answer —
-- the process that owns the SQLite file — so `claim_next` needed no identity
-- column and the per-app running counts could live in an in-process HashMap.
-- An outbound executor breaks both assumptions: N processes drain the same
-- queue, and the coordinator has to be able to say *who* holds a lease.
--
-- `executor_id` is NULL for every job the coordinator claims locally, which is
-- the honest value and keeps the local claim path byte-for-byte as it was. It
-- is stamped by `claim_next_for_executor` and read back by the executor-facing
-- routes: a `finish`/`heartbeat`/`checkpoint` arriving for a job this executor
-- does not hold is refused, ON TOP of the `(status='running', attempts)` fence
-- that already makes a dead executor's late write harmless.
ALTER TABLE jobs ADD COLUMN executor_id TEXT;

-- The cluster-wide per-app cap ("how many jobs is each app running across every
-- executor?") and `GET /executors`'s running-jobs column are both this scan.
CREATE INDEX IF NOT EXISTS idx_jobs_executor ON jobs (executor_id, status, app);

-- One row per executor that has ever polled. Deliberately a DB table rather
-- than an in-memory map: the coordinator's caps are computed from the DB
-- precisely so a restart does not forget which executors exist, and so that
-- "last poll" survives long enough to be a diagnosis rather than a gap.
CREATE TABLE IF NOT EXISTS executors (
    id            TEXT PRIMARY KEY,
    -- JSON array of app names this executor declared it can run. Advisory: the
    -- coordinator intersects it with the executor-eligible app list, so an
    -- executor can never widen its own eligibility by claiming a capability.
    capabilities  TEXT NOT NULL DEFAULT '[]',
    first_seen_at TEXT NOT NULL,
    last_poll_at  TEXT NOT NULL,
    -- Monotonic count of jobs handed to this executor, so "polling but never
    -- claiming" and "not polling at all" stay two different observations.
    claimed_total INTEGER NOT NULL DEFAULT 0
);
