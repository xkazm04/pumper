-- Overlap guard: scheduled jobs record which schedule fired them, so the
-- scheduler can skip a tick while the previous run is still queued/running.
ALTER TABLE jobs ADD COLUMN schedule_id TEXT;
CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON jobs (schedule_id, status);
