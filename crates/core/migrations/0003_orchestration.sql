-- Queue orchestration: priority + result-callback columns (features: queue, webhooks).
ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN callback_url TEXT;
ALTER TABLE jobs ADD COLUMN callback_secret TEXT;

-- Dynamic, DB-backed recurring schedules (feature: scheduled operations).
CREATE TABLE IF NOT EXISTS schedules (
    id         TEXT PRIMARY KEY,
    app        TEXT NOT NULL,
    cron       TEXT NOT NULL,
    params     TEXT NOT NULL DEFAULT '{}',
    enabled    INTEGER NOT NULL DEFAULT 1,
    priority   INTEGER NOT NULL DEFAULT 0,
    last_run   TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedules_enabled ON schedules (enabled);
