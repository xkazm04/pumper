-- In-flight exclusion by target, not by app.
--
-- `blocked_apps` + `app NOT IN (…)` in the claim is a FAIRNESS device (one busy
-- app must not starve the others' queues) and stays exactly that. Nothing in the
-- tree was a mutual-exclusion device: within one app's budget, two jobs that
-- write the same dataset rows claimed two slots and ran at once — a scheduled
-- run and a manual re-run of the same source, two trigger hops onto one target,
-- a row re-queued by the lease reaper while its predecessor's task was still
-- finishing. `idempotency_key` cannot answer this: it refuses a second
-- *enqueue*, and says nothing about two rows already in the table.
--
-- NULL means UNKNOWN — the `trust` migration pattern (0020, 0030). Every job
-- written before this column existed, and every job of an app that has not
-- overridden `ScrapeApp::target_key`, is NULL and behaves exactly as it does
-- today: excluded from the exclusion. Nothing is backfilled and no key is
-- fabricated, because a fabricated key would serialise a legitimately parallel
-- shape silently.
ALTER TABLE jobs ADD COLUMN target_key TEXT;

-- The claim's added predicate is `target_key NOT IN (SELECT target_key FROM jobs
-- WHERE status = 'running' AND target_key IS NOT NULL)`, which runs on every
-- claim over a table that also holds every finished job. Without this index that
-- turns an indexed point claim into a scan under load, and the queue's own
-- throughput is the thing the exclusion exists to protect.
CREATE INDEX IF NOT EXISTS idx_jobs_status_target ON jobs (status, target_key);
