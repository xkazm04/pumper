-- Saved searches: standing full-text queries that fire a webhook when NEW
-- documents match. Seen doc-ids are tracked per search so each match alerts
-- exactly once (diff-aware alerts, not repeated result dumps).
CREATE TABLE IF NOT EXISTS saved_searches (
    id         TEXT PRIMARY KEY,
    query      TEXT NOT NULL,
    app        TEXT,                     -- optional scope filter
    dataset    TEXT,
    url        TEXT NOT NULL,            -- webhook target
    secret     TEXT,
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_search_seen (
    search_id  TEXT NOT NULL,
    doc_id     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (search_id, doc_id)
);
