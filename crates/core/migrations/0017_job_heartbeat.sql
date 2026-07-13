-- Job heartbeat lease: the worker stamps `heartbeat_at` on each running job on
-- an interval, and the reaper re-queues running jobs whose heartbeat has gone
-- stale — so a hung task can't strand its job in `running` forever on a live
-- server (feature: runtime — stuck-job reaper). Fixed-width RFC-3339 µs, like
-- every other TEXT timestamp, so lexicographic/julianday comparison is chrono.
ALTER TABLE jobs ADD COLUMN heartbeat_at TEXT;
