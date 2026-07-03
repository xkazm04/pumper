CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    app          TEXT NOT NULL,
    params       TEXT NOT NULL DEFAULT '{}',
    status       TEXT NOT NULL DEFAULT 'queued',
    attempts     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    result       TEXT,
    error        TEXT,
    created_at   TEXT NOT NULL,
    available_at TEXT NOT NULL,
    started_at   TEXT,
    finished_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_app ON jobs (app, created_at DESC);
