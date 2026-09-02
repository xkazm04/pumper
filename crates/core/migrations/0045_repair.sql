-- N12 steps 4-5: the repair audit trail (resilient-extraction.md §5, §6.4, §8).
--
-- Every candidate and every verdict is persisted, so a REJECTED repair is
-- exactly as auditable as a promoted one. That is not bookkeeping for its own
-- sake: the design's falsifier (§12.3) is "of promoted repairs, the fraction
-- that reproduce the pre-mutation values", and it cannot be computed unless the
-- rejections are on record beside the promotions.
--
-- Nothing here is written unless `[resilience.repair] enabled = true`, which is
-- false by default. The tables exist so the seam has a home before it has a
-- caller.
CREATE TABLE IF NOT EXISTS repair_attempts (
    id               TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,
    job_id           TEXT,
    diagnosis        TEXT NOT NULL,
    -- Stable identity of WHAT is broken: diagnosis + the exact broken field
    -- set. Part of the idempotency key, so a source that breaks a second way is
    -- not blocked by the first attempt.
    diagnosis_hash   TEXT NOT NULL,
    -- inversion|claude|validating|shadow|promoted|rejected
    stage            TEXT NOT NULL,
    cost_usd         REAL NOT NULL DEFAULT 0,
    -- promoted|rejected|no_candidate|budget|inconclusive|shadow
    outcome          TEXT,
    promoted_version INTEGER,
    created_at       TEXT NOT NULL,
    finished_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_repair_source ON repair_attempts (source_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_repair_diagnosis ON repair_attempts (source_id, diagnosis_hash);

CREATE TABLE IF NOT EXISTS repair_candidates (
    attempt_id           TEXT NOT NULL,
    idx                  INTEGER NOT NULL,
    origin               TEXT NOT NULL,   -- inversion|claude
    rules                TEXT NOT NULL,   -- serialized RuleSet
    holdout_match_rate   REAL,
    golden_exact         INTEGER,
    golden_total         INTEGER,
    invariant_violations INTEGER,
    -- Candidates producing identical holdout OUTPUT share a group: two
    -- different selectors agreeing is stronger evidence than two identical
    -- proposals, which is why the group is over values and not over rules.
    agreement_group      INTEGER,
    lint                 TEXT,            -- JSON array of findings
    -- accepted | rejected:<gate>. The first gate that refused, so the reason is
    -- also the cheapest true explanation.
    verdict              TEXT NOT NULL,
    -- The whole score sheet, so a stored row explains itself without re-running
    -- anything.
    evidence             TEXT,
    profile_version      INTEGER,
    created_at           TEXT NOT NULL,
    PRIMARY KEY (attempt_id, idx)
);

-- Anti-oscillation state on the source row (§8.3). All NULL/0 defaults, all
-- inert while repair is disabled.
ALTER TABLE sources ADD COLUMN repair_blocked_until TEXT;
ALTER TABLE sources ADD COLUMN promotions_30d INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN last_promotion_at TEXT;
-- The profile a repair would write back to. NULL = not profile-backed, i.e.
-- `repairable: false` — a semantic default, not a sentinel, so every existing
-- row is correct without a backfill.
ALTER TABLE sources ADD COLUMN profile TEXT;
