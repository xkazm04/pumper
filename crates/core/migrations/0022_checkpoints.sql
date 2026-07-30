-- Durable execution (M23): per-job checkpoint blobs so any app can suspend and
-- resume mid-run. Keyed by job id — one live checkpoint per job, overwritten in
-- place by the throttled `ctx.checkpoint(..)` seam and cleared when the job
-- reaches a terminal state.
--
--   state           the app's own JSON snapshot (advisory: apps must tolerate a
--                   stale or missing checkpoint and start fresh)
--   attempt         the attempt number that wrote the blob — writes are guarded
--                   by the same (status, attempts) lineage rule as `complete`,
--                   so a stale task whose job was reset/reaped can never
--                   overwrite the live attempt's checkpoint
--   resume_failures how many attempts have started from this blob without
--                   completing; past `[worker] max_resume_failures` the blob is
--                   treated as poisoned and discarded (fresh start)
--   updated_at      last write time (observability / debugging)
CREATE TABLE IF NOT EXISTS checkpoints (
    job_id          TEXT PRIMARY KEY,
    state           TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    resume_failures INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);
