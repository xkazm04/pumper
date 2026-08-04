-- DataHub governance audit trail: every governance action this instance
-- actually EXECUTED, and why.
--
-- Before this table the only record of a governance action was
-- `GovernState.last` — the *last poll's* summary, in memory, erased by the next
-- poll and by every restart. "Why is this schedule disabled?" and "who zeroed
-- this app's budget last Tuesday?" were unanswerable the moment the process
-- restarted, even though the actions themselves (disabled schedules, enqueued
-- jobs) are durable.
--
-- Diagnostic, not evidence, like `trigger_runs`: rows are bounded by age (the
-- governance poll prunes them at most hourly), because the actions they
-- describe are visible in the schedules/jobs tables regardless.
CREATE TABLE IF NOT EXISTS datahub_govern_actions (
    id         TEXT PRIMARY KEY,
    -- What was done: see `crate::storage::DATAHUB_GOVERN_ACTIONS`.
    action     TEXT NOT NULL,
    -- The Pumper app the action acted on.
    target     TEXT NOT NULL,
    -- The dataset whose remote state was the evidence (NULL when the action is
    -- about the app as a whole, e.g. a staleness expiry).
    dataset    TEXT,
    -- The row the action produced or changed: a schedule id, a job id.
    subject    TEXT,
    -- The remote signal that justified it: 'deprecation' | 'cost:pause' |
    -- 'assertions' | 'stale' — what an operator needs to go look at in DataHub.
    evidence   TEXT NOT NULL,
    -- Free-text context (an idempotency key, an error, a human sentence).
    detail     TEXT,
    created_at TEXT NOT NULL
);

-- The read path (GET /datahub/status → govern.recent_actions) and the prune
-- path are the same order: newest first / age-ordered sweep.
CREATE INDEX IF NOT EXISTS idx_datahub_govern_actions_created
    ON datahub_govern_actions (created_at DESC, id DESC);
