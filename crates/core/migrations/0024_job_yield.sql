-- Information economics (M04): per-job yield alongside per-job cost. The cost
-- ledger (0007) answers "what did this job spend"; this table answers "what did
-- that spend buy" — the new/changed/unchanged record counts apps already report
-- in their result JSON, parsed worker-side on completion (no app changes).
-- Joining the two over a trailing window gives $/new-record and $/changed-record
-- per app, the raw material for the advisory budget planner (GET /economics).
--
--   dataset          label of where in the result the summary sat: '' for the
--                    result root, else the JSON key path (e.g. 'datasets.velocity',
--                    'unified') — apps that write several datasets report one
--                    summary object per dataset and each becomes its own row
--   *_count          NULL means "the result did not report this number", which is
--                    deliberately distinct from 0 ("it reported zero"). SUM()
--                    ignores NULLs and returns NULL over all-NULL groups, so the
--                    economics rollup keeps unknown as unknown instead of $0.
CREATE TABLE IF NOT EXISTS job_yield (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id          TEXT NOT NULL,
    app             TEXT NOT NULL,
    dataset         TEXT NOT NULL DEFAULT '',
    new_count       INTEGER,
    changed_count   INTEGER,
    unchanged_count INTEGER,
    removed_count   INTEGER,
    created_at      TEXT NOT NULL
);

-- The economics window query: WHERE created_at > ? GROUP BY app, dataset.
CREATE INDEX IF NOT EXISTS idx_job_yield_created ON job_yield (created_at);
-- Per-job lookups (debugging, potential re-extraction).
CREATE INDEX IF NOT EXISTS idx_job_yield_job ON job_yield (job_id);
