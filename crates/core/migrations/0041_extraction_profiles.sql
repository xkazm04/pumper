-- N12 step 1: the profile registry (resilient-extraction.md §4).
--
-- Until now a `RuleSet` was a job parameter: no identity, no version, no home.
-- Repair is structurally impossible against that — there is nothing to promote,
-- nothing to roll back to, and nothing to stamp a record with. So rules become a
-- first-class versioned entity and job params reference them by name.
--
-- `profile_versions` is IMMUTABLE: a repair never edits a rule, it appends a
-- version and (maybe) moves `extraction_profiles.active_version`. Rollback is a
-- pointer move; history is never rewritten.
--
-- Inline `rules` keep working exactly as today and are explicitly unwarranted —
-- a job that passes rules inline can never be repaired, because there is nothing
-- to write back to. That is reported as `repairable: false` rather than
-- deprecated: inline rules are the right shape for `POST /extract/preview`.
CREATE TABLE IF NOT EXISTS extraction_profiles (
    name           TEXT PRIMARY KEY,
    app            TEXT NOT NULL,
    dataset        TEXT NOT NULL,
    active_version INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS profile_versions (
    profile        TEXT NOT NULL,
    version        INTEGER NOT NULL,
    rules          TEXT NOT NULL,        -- serialized RuleSet
    rules_hash     TEXT NOT NULL,        -- canonical hash, joins `rules_versions`
    origin         TEXT NOT NULL,        -- human|inversion|claude|rollback
    parent_version INTEGER,
    evidence       TEXT,                 -- JSON score sheet that justified it
    created_at     TEXT NOT NULL,
    PRIMARY KEY (profile, version)
);

CREATE INDEX IF NOT EXISTS idx_profile_versions_recent
    ON profile_versions (profile, version DESC);

-- Which rule version produced a run. NULL keeps meaning "not profile-backed"
-- (inline rules, or a run recorded before this migration) — a semantic default,
-- not a sentinel, so no backfill is required and pre-migration rows stay honest.
ALTER TABLE source_runs ADD COLUMN profile_version INTEGER;
