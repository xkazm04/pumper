-- N20 identity & tenancy plane: caller principals (scoped API keys), a durable
-- audit ledger, and the caller column the cost ledger never had.
--
-- Inert until `[auth] mode = "keys"`: in the default `open` mode no principal
-- row is ever consulted, every request resolves the synthetic `operator`, and
-- these tables stay empty. Adding them is additive — legacy `jobs` and
-- `cost_events` rows keep a NULL caller (unattributed, never invented).

CREATE TABLE IF NOT EXISTS principals (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    -- SHA-256 hex digest of the presented key. The key itself is returned once
    -- at creation/rotation and is never stored, so it cannot be re-read here.
    key_hash           TEXT NOT NULL,
    -- JSON array of scope strings: "read" | "enqueue:<app>" | "enqueue:*" | "admin".
    scopes             TEXT NOT NULL DEFAULT '[]',
    -- NULL = no ceiling (the same "omitted means unlimited" convention
    -- `budget_usd` uses on jobs/schedules/triggers).
    budget_usd_per_day REAL,
    -- NULL = no throttle.
    rate_limit_per_min INTEGER,
    enabled            INTEGER NOT NULL DEFAULT 1,
    created_at         TEXT NOT NULL
);

-- Key lookup is by digest, so it must be unique AND indexed: a rotation that
-- collided with a live principal would authenticate the wrong caller.
CREATE UNIQUE INDEX IF NOT EXISTS idx_principals_key_hash ON principals (key_hash);

CREATE TABLE IF NOT EXISTS audit_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    -- NULL for a request that never resolved a principal (refused before
    -- identification). Never back-filled with a guess.
    principal_id TEXT,
    action       TEXT NOT NULL,   -- '<METHOD> <path>'
    target       TEXT,            -- the request path
    at           TEXT NOT NULL,
    detail       TEXT             -- JSON: {status, mode, principal_name}
);
CREATE INDEX IF NOT EXISTS idx_audit_principal ON audit_log (principal_id, id);

-- The caller columns. Nullable by construction: every row written before this
-- migration, and every row written in `open` mode, has no principal.
ALTER TABLE jobs ADD COLUMN principal_id TEXT;
ALTER TABLE cost_events ADD COLUMN principal_id TEXT;
CREATE INDEX IF NOT EXISTS idx_cost_events_principal ON cost_events (principal_id);
