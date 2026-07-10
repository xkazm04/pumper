-- Dataset watches: standing subscriptions that receive a webhook whenever a
-- job leaves new/changed/removed revisions in a watched dataset. dataset '*'
-- watches every dataset of the app.
CREATE TABLE IF NOT EXISTS watches (
    id         TEXT PRIMARY KEY,
    app        TEXT NOT NULL,
    dataset    TEXT NOT NULL DEFAULT '*',
    url        TEXT NOT NULL,
    secret     TEXT,
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watches_app ON watches (app, enabled);
