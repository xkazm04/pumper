-- N03 "workflow runs": a declared multi-step DAG whose steps are ordinary jobs,
-- with fan-in join barriers, `{{steps.X.result.path}}` param templating, one
-- budget envelope and one rolled-up receipt.
--
-- The three tables are the forward declaration the queue never had. Lineage
-- today is backward-only (`jobs.source_job_id`, one hop at a time), so nothing
-- could say "these five jobs are one plan" before the plan ran. `workflow_steps`
-- is that statement, and it is also the barrier's state: a step becomes
-- enqueueable exactly when every name in its `depends_on` is `succeeded`.
--
--   workflow_defs    the plan. `spec_json` is the whole `{steps, budget_usd,
--                    on_failure}` document, validated at the door; `cron` is
--                    optional and makes the scheduler own the plan (a run per
--                    firing, overlap-guarded on "newest run still open").
--   workflow_runs    one execution. `root_id` equals the run id — the
--                    correlation key every step job carries — and `spent_usd`
--                    is the envelope's running total, refreshed from
--                    `cost_events` as each step ends.
--   workflow_steps   one (run, step) cell. `status` is the barrier input and
--                    the idempotence fence: every transition is a guarded
--                    UPDATE on the status it expects, so a join whose two
--                    upstreams finish concurrently still fires exactly once.
--
-- Everything is additive: the three `jobs` columns are nullable, so every
-- existing row and every query that does not mention them is unchanged.

CREATE TABLE IF NOT EXISTS workflow_defs (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    spec_json   TEXT NOT NULL,
    -- NULL = run only on demand. Non-NULL = the scheduler fires a run per
    -- cron firing (see `workflow::reconcile_scheduled`).
    cron        TEXT,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id              TEXT PRIMARY KEY,
    def_id          TEXT NOT NULL,
    -- running | succeeded | failed | cancelled
    status          TEXT NOT NULL,
    budget_usd      REAL,
    spent_usd       REAL NOT NULL DEFAULT 0,
    -- Client dedup key, exactly like `jobs.idempotency_key`: a replayed
    -- `POST /workflows/{id}/runs` returns the original run.
    idempotency_key TEXT UNIQUE,
    -- The caller that started the run (N20); every step job inherits it.
    principal_id    TEXT,
    -- Correlation id carried by every step job. Equal to `id` in v1; a separate
    -- column so a future chain that starts outside a workflow can share one.
    root_id         TEXT NOT NULL,
    error           TEXT,
    started_at      TEXT NOT NULL,
    finished_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_def
    ON workflow_runs (def_id, started_at DESC);

CREATE TABLE IF NOT EXISTS workflow_steps (
    run_id      TEXT NOT NULL,
    step        TEXT NOT NULL,
    job_id      TEXT,
    -- JSON array of step names: this step's join barrier.
    depends_on  TEXT NOT NULL,
    -- pending | queued | succeeded | failed | cancelled | skipped
    status      TEXT NOT NULL,
    result      TEXT,
    error       TEXT,
    finished_at TEXT,
    PRIMARY KEY (run_id, step)
);

-- The worker hook's only query: given a terminal job, which (run, step) is it?
CREATE INDEX IF NOT EXISTS idx_workflow_steps_job ON workflow_steps (job_id);

ALTER TABLE jobs ADD COLUMN workflow_run_id TEXT;
ALTER TABLE jobs ADD COLUMN workflow_step TEXT;
ALTER TABLE jobs ADD COLUMN root_id TEXT;

-- The run receipt's job set, and the correlation lookup.
CREATE INDEX IF NOT EXISTS idx_jobs_workflow_run ON jobs (workflow_run_id)
    WHERE workflow_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_root ON jobs (root_id)
    WHERE root_id IS NOT NULL;
