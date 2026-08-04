-- Trigger decision ledger: every evaluation of a trigger edge, not just the
-- ones that fired.
--
-- Before this table the only observable trigger outcome was a job row
-- (`jobs.trigger_id`), so "why did my pipeline not fire?" was answerable only
-- from logs — and several negatives had no log line at all (a filter miss and a
-- dedup suppression were both a bare `continue`). This records one row per
-- (trigger, source event) decision with the reason.
--
-- Diagnostic, not evidence: rows are bounded by age (the worker's reaper tick
-- prunes them), unlike the opt-in `LEDGER_TABLES` whose deletion is data loss.
CREATE TABLE IF NOT EXISTS trigger_runs (
    id            TEXT PRIMARY KEY,
    -- The trigger that was evaluated, or '*' for a decision that belongs to the
    -- whole edge set rather than to one trigger (the evaluation set failing to
    -- load — the failure that used to drop every edge invisibly).
    trigger_id    TEXT NOT NULL,
    -- 'fired' or a skip reason (see `crate::storage::TRIGGER_OUTCOMES`).
    outcome       TEXT NOT NULL,
    -- 'dataset' | 'job' | 'external'
    source_kind   TEXT NOT NULL,
    -- The source job (dataset/job kinds); NULL for external events.
    source_job_id TEXT,
    -- The dataset of the batch this decision was about (dataset kind only).
    dataset       TEXT,
    -- The inbound event id (external kind only).
    event_id      TEXT,
    -- The hop that was enqueued, when the outcome is 'fired'.
    job_id        TEXT,
    -- Free-text context for the outcome (an error message, a plugin name).
    detail        TEXT,
    created_at    TEXT NOT NULL
);

-- The read path: one trigger's decisions, newest first (GET /triggers/{id}/runs).
CREATE INDEX IF NOT EXISTS idx_trigger_runs_trigger
    ON trigger_runs (trigger_id, created_at DESC, id DESC);
-- The prune path: age-ordered sweep.
CREATE INDEX IF NOT EXISTS idx_trigger_runs_created ON trigger_runs (created_at);
