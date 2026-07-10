-- Cost-aware research cache: Claude runs cost real money, so identical
-- research requests within the TTL are served from disk. Keyed by a hash of
-- every answer-shaping request field (prompt, system, role, model, effort,
-- turns, schema).
CREATE TABLE IF NOT EXISTS research_cache (
    key        TEXT PRIMARY KEY,
    text       TEXT NOT NULL,
    json       TEXT,
    cost_usd   REAL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_research_cache_expiry ON research_cache (expires_at);
