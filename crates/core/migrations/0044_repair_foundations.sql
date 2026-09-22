-- Repair-plane foundations (docs/design/self-repairing-extraction.md, Stages 1-3).
--
-- Additive only. Every new column is nullable and NULL means UNKNOWN or NONE —
-- the `trust` (0020) and provenance (0030) pattern: no backfill runs and no
-- value is ever fabricated. `records` and `record_revisions` gain nothing.
--
-- Deliberately NOT created here: field contracts, the assay ledger, repair
-- attempts, the commit-row receipt, accepted rows, and the licence / hold /
-- publication-epoch / cooldown columns on `sources`. They belong to Stages 4-6,
-- and an empty table reads as a feature that exists.

-- The ruleset POINTER per source. Bodies stay content-addressed in
-- `rules_versions` (0030); this table only says which one is active, where it
-- came from, and what it replaced. The head is the row with the greatest
-- `epoch` whose `retired_at` is NULL; `epoch` strictly increases per source.
CREATE TABLE IF NOT EXISTS source_rulesets (
    source_id       TEXT NOT NULL,
    rules_hash      TEXT NOT NULL,
    parent_hash     TEXT,                 -- NULL = first head for this source
    origin          TEXT NOT NULL,        -- import | human | rollback (inversion | split_half | induction reserved for Stage 4)
    incident_id     TEXT,                 -- NULL until the repair engine exists
    licence_at_move TEXT NOT NULL,        -- always 'none' before Stage 5
    epoch           INTEGER NOT NULL,
    activated_at    TEXT NOT NULL,
    retired_at      TEXT,
    PRIMARY KEY (source_id, activated_at)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_rulesets_epoch
    ON source_rulesets (source_id, epoch);

-- Golden documents: a frozen body plus the values it must still produce, two
-- generations deep so a slowly boiled baseline has to fool a reference about
-- two refresh periods old. Bodies live under data/golden/, outside artifact
-- retention.
CREATE TABLE IF NOT EXISTS golden_docs (
    source_id     TEXT NOT NULL,
    key           TEXT NOT NULL,
    generation    INTEGER NOT NULL,
    url           TEXT NOT NULL,
    body_sha      TEXT NOT NULL,
    expected      TEXT NOT NULL,          -- JSON: the record's typed values
    stable_fields TEXT NOT NULL,          -- JSON array: fields with churn < threshold
    rules_hash    TEXT NOT NULL,
    pinned_by     TEXT NOT NULL,          -- operator | refresh
    pinned_at     TEXT NOT NULL,
    PRIMARY KEY (source_id, key, generation)
);

-- Which body an observation was extracted from. Bytes are deduplicated by
-- `body_sha`; observations never are.
CREATE TABLE IF NOT EXISTS body_pins (
    body_sha       TEXT NOT NULL,
    observation_id TEXT PRIMARY KEY,
    source_id      TEXT NOT NULL,
    job_id         TEXT,
    document_key   TEXT,
    original_url   TEXT,
    captured_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_body_pins_source ON body_pins (source_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_body_pins_sha    ON body_pins (body_sha);

-- NULL = inline rules or a hardcoded app: not repairable.
ALTER TABLE sources ADD COLUMN active_rules_hash TEXT;

-- Retirement's own denominator (a run with failed fetches never reaches a
-- verdict, so retirement cannot sit behind the fetch gate). NULL = the run
-- predates this column or its app does not report a roster.
ALTER TABLE source_runs ADD COLUMN fetch_permanent INTEGER;
ALTER TABLE source_runs ADD COLUMN fetch_roster    INTEGER;
