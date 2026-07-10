-- Change intelligence: per-record revision history with field-level diffs.
-- Every New/Changed upsert appends a revision; removals (full-snapshot syncs)
-- append a 'removed' revision. This is the substrate for change feeds, diff
-- webhooks, and time-travel over datasets.
CREATE TABLE IF NOT EXISTS record_revisions (
    app        TEXT NOT NULL,
    dataset    TEXT NOT NULL,
    key        TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    change     TEXT NOT NULL,             -- 'new' | 'changed' | 'removed'
    data       TEXT,                      -- full record snapshot (NULL for removed)
    diff       TEXT,                      -- JSON field-level diff vs previous revision
    created_at TEXT NOT NULL,
    PRIMARY KEY (app, dataset, key, revision)
);
CREATE INDEX IF NOT EXISTS idx_revisions_feed
    ON record_revisions (app, dataset, created_at DESC);

-- Removal detection on the live records table: set when a full-snapshot sync
-- no longer contains the key; cleared if the record reappears.
ALTER TABLE records ADD COLUMN removed_at TEXT;
