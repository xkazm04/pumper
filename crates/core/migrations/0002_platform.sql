-- Content-addressed HTTP response cache (feature: caching).
CREATE TABLE IF NOT EXISTS http_cache (
    key        TEXT PRIMARY KEY,
    url        TEXT NOT NULL,
    status     INTEGER NOT NULL,
    headers    TEXT NOT NULL DEFAULT '{}',
    body       TEXT NOT NULL,
    final_url  TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_http_cache_expiry ON http_cache (expires_at);

-- Persistent dataset store with change detection (features: dedup, datasets).
CREATE TABLE IF NOT EXISTS records (
    app        TEXT NOT NULL,
    dataset    TEXT NOT NULL,
    key        TEXT NOT NULL,
    hash       TEXT NOT NULL,
    data       TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen  TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (app, dataset, key)
);
CREATE INDEX IF NOT EXISTS idx_records_updated ON records (app, dataset, updated_at DESC);
