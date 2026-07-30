-- M21 inbound event ingress: HMAC-verified external webhooks become trigger
-- inputs. `ingress_sources` are the per-caller credentials for POST /ingest/{id};
-- the triggers table gains the 'external' source kind plus optional JSON-path
-- predicate filters evaluated against the inbound payload.

CREATE TABLE IF NOT EXISTS ingress_sources (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    secret     TEXT NOT NULL,          -- HMAC-SHA256 signing secret (required: ingress is a write surface)
    enabled    INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

-- Extend triggers with the 'external' source kind. SQLite cannot ALTER a CHECK
-- constraint, so the table is rebuilt in place (no FK references triggers;
-- jobs.trigger_id is a plain column). `filters` holds a JSON array of
-- '<path>:<op>:<value>' specs (the ?filter= grammar) ANDed against the payload.
CREATE TABLE triggers_new (
    id             TEXT PRIMARY KEY,
    name           TEXT,
    source_kind    TEXT NOT NULL,                 -- 'dataset' | 'job' | 'external'
    source_app     TEXT NOT NULL,                 -- external kind: ingress source id or '*'
    source_dataset TEXT,
    on_change      TEXT,
    on_status      TEXT,
    target_app     TEXT NOT NULL,
    params         TEXT NOT NULL DEFAULT '{}',
    budget_usd     REAL,
    priority       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 1,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    filters        TEXT,                          -- JSON array of filter specs; external kind only
    CHECK (source_kind IN ('dataset','job','external'))
);
INSERT INTO triggers_new (id, name, source_kind, source_app, source_dataset, on_change,
                          on_status, target_app, params, budget_usd, priority,
                          max_attempts, enabled, created_at)
    SELECT id, name, source_kind, source_app, source_dataset, on_change,
           on_status, target_app, params, budget_usd, priority,
           max_attempts, enabled, created_at
    FROM triggers;
DROP TABLE triggers;
ALTER TABLE triggers_new RENAME TO triggers;
CREATE INDEX IF NOT EXISTS idx_triggers_source
    ON triggers (source_kind, source_app, enabled);
