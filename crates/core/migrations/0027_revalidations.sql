-- Revalidation observation log (M02 self-refreshing mirror). Every conditional
-- revalidation of an http_cache entry — demand-path (engine 304/changed) or the
-- background refresher — appends one labeled observation: did the origin's body
-- change? The per-key change history feeds an EWMA inter-change estimator so the
-- refresher can revalidate each URL just before its predicted next change,
-- and GET /cache/freshness can report predicted staleness per key/host.
-- Append-only; pruned by the refresher tick on a retention window.
CREATE TABLE IF NOT EXISTS revalidations (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    key        TEXT NOT NULL,      -- http_cache.key (the entry revalidated)
    checked_at TEXT NOT NULL,      -- RFC 3339 time of the revalidation
    changed    INTEGER NOT NULL DEFAULT 0  -- 1 = new body, 0 = 304 unchanged
);
CREATE INDEX IF NOT EXISTS idx_revalidations_key_time ON revalidations (key, checked_at);
CREATE INDEX IF NOT EXISTS idx_revalidations_time ON revalidations (checked_at);
