-- M12 reproducible records: stamp every revision with its derivation.
--
-- All four columns are nullable and NULL means UNKNOWN — the `trust` migration
-- pattern (0020): every revision written before this column existed is honest
-- by construction ("we don't know where it came from"), so no backfill runs and
-- no value is ever fabricated. Write paths populate only what they truly know:
--   job_id       producing job (AppContext stamps it on every app write)
--   source_url   URL the record's content was fetched from
--   artifact_sha sha256 (hex) of the archived source body on disk
--   rules_hash   sha256 (hex) of the canonical RuleSet JSON that extracted it
ALTER TABLE record_revisions ADD COLUMN job_id TEXT;
ALTER TABLE record_revisions ADD COLUMN source_url TEXT;
ALTER TABLE record_revisions ADD COLUMN artifact_sha TEXT;
ALTER TABLE record_revisions ADD COLUMN rules_hash TEXT;

-- Content-addressed RuleSet registry: re-derivation must replay the HISTORICAL
-- ruleset a revision was extracted with, not whatever the app's config says
-- today. A writer that stamps `rules_hash` registers the ruleset here first
-- (INSERT OR IGNORE — the hash IS the identity, so re-registration is free).
CREATE TABLE IF NOT EXISTS rules_versions (
    hash       TEXT PRIMARY KEY,
    rules      TEXT NOT NULL,
    created_at TEXT NOT NULL
);
