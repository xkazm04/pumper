-- Cron maturity: per-schedule timezone, misfire policy, and retry budget.
-- (feature: scheduled operations — see docs/features/runtime.md)
--
-- timezone:       IANA name (chrono-tz) the cron expression is evaluated in.
--                 NULL = UTC (the historical behaviour).
-- misfire_policy: how a backlog of firings missed while the scheduler was down
--                 is handled. 'fire_once' (default, = historical behaviour) runs
--                 a single catch-up; 'skip' runs none and advances past them.
-- max_attempts:   attempt budget for jobs this schedule enqueues. NULL = the
--                 server default (so cron runs retry transient failures like any
--                 manual job, instead of the old hardcoded single attempt).
ALTER TABLE schedules ADD COLUMN timezone TEXT;
ALTER TABLE schedules ADD COLUMN misfire_policy TEXT NOT NULL DEFAULT 'fire_once';
ALTER TABLE schedules ADD COLUMN max_attempts INTEGER;
