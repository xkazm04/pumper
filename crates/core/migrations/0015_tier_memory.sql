-- Self-learning tier router: per-host memory of whether the cheap HTTP tier
-- keeps failing/thinning out. After 3 consecutive strikes the fetcher starts
-- at the browser tier for that host; one HTTP win resets the record.
CREATE TABLE IF NOT EXISTS tier_memory (
    host         TEXT PRIMARY KEY,
    http_strikes INTEGER NOT NULL DEFAULT 0,
    preferred    TEXT,                          -- 'browser' once strikes >= 3
    updated_at   TEXT NOT NULL
);
