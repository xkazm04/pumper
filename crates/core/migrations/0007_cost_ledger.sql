-- Cost ledger: one event per metered engine call, attributed to the job that
-- spent it. Claude-tier events carry the CLI's actual total_cost_usd; http and
-- browser events default to 0.0 and exist for call-count / ROI accounting.
CREATE TABLE IF NOT EXISTS cost_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     TEXT NOT NULL,
    app        TEXT NOT NULL,
    engine     TEXT NOT NULL,             -- 'http' | 'browser' | 'claude'
    url        TEXT,
    cost_usd   REAL NOT NULL DEFAULT 0,
    detail     TEXT,                      -- e.g. escalation trail, 'cache_hit'
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cost_events_job ON cost_events (job_id);
CREATE INDEX IF NOT EXISTS idx_cost_events_app ON cost_events (app, created_at DESC);
