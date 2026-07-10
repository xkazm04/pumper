-- Reactive triggers: a directed edge (source event) -> (enqueue target app).
-- The set of triggers IS the pipeline DAG (adjacency list); there is no separate
-- pipeline container. Modelled on `watches` (runtime-CRUD standing subscriptions).
CREATE TABLE IF NOT EXISTS triggers (
    id             TEXT PRIMARY KEY,
    name           TEXT,                          -- optional human label / future pipeline group
    source_kind    TEXT NOT NULL,                 -- 'dataset' | 'job'
    source_app     TEXT NOT NULL,
    source_dataset TEXT,                          -- dataset kind: '*' or name; NULL for job kind
    on_change      TEXT,                          -- 'new'|'changed'|'removed'|'fresh'|'any'; dataset only
    on_status      TEXT,                          -- 'succeeded'|'failed'|'any'; job only
    target_app     TEXT NOT NULL,
    params         TEXT NOT NULL DEFAULT '{}',    -- static template; _trigger merged over it
    budget_usd     REAL,                          -- target ceiling (NOT inherited from source)
    priority       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 1,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    CHECK (source_kind IN ('dataset','job'))
);
CREATE INDEX IF NOT EXISTS idx_triggers_source
    ON triggers (source_kind, source_app, enabled);

-- Lineage: which trigger fired this job (mirrors jobs.schedule_id).
ALTER TABLE jobs ADD COLUMN trigger_id TEXT;
CREATE INDEX IF NOT EXISTS idx_jobs_trigger ON jobs (trigger_id, created_at DESC);
