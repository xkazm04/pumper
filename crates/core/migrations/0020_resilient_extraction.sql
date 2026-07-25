-- Extraction health: per-source degradation detection.
--
-- Extraction rots silently — a selector that stops matching yields the same
-- `empty` as a genuinely absent field, and a selector that rebinds after a
-- redesign keeps every counter green. There is no ground truth to check
-- against, so the substrate here IS the past: per-run field sketches and
-- per-document fingerprints, compared against the rolling baseline of runs we
-- believed were healthy.
--
-- Timestamps are fixed-width RFC 3339 UTC micros (the enforced convention), so
-- lexicographic comparison is chronological.

-- ---------- the health unit: one row per (app, dataset) --------------------
CREATE TABLE IF NOT EXISTS sources (
    id                TEXT PRIMARY KEY,        -- '<app>/<dataset>'
    app               TEXT NOT NULL,
    dataset           TEXT NOT NULL,
    state             TEXT NOT NULL DEFAULT 'healthy',
    degradation_score REAL NOT NULL DEFAULT 0,
    state_since       TEXT NOT NULL,
    state_reason      TEXT,
    last_verdict      TEXT,
    last_verdict_at   TEXT,
    tripped_of_last3  INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    CHECK (state IN ('healthy','suspect','degraded','quarantined','probation','retired'))
);

-- ---------- one row per (source, run) --------------------------------------
CREATE TABLE IF NOT EXISTS source_runs (
    source_id     TEXT NOT NULL,
    job_id        TEXT NOT NULL,
    docs          INTEGER NOT NULL,
    fetch_ok_rate REAL NOT NULL,
    d_text        REAL,                        -- cohort median normalized drifts
    d_dom         REAL,
    d_val         REAL,
    compared      INTEGER NOT NULL DEFAULT 0,  -- keys present in both this run and the last
    verdict       TEXT NOT NULL,               -- ok|inconclusive|content_empty|suspect|broken
    diagnosis     TEXT,                        -- markup_drift|content_changed|field_loss|...
    score         REAL NOT NULL DEFAULT 0,
    reasons       TEXT,                        -- JSON array of {test, field, value, threshold}
    state_after   TEXT NOT NULL,
    build_id      TEXT,                        -- pumper build; disambiguates self-inflicted
    created_at    TEXT NOT NULL,
    PRIMARY KEY (source_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_source_runs_feed ON source_runs (source_id, created_at DESC);
-- The baseline read is "the last N runs of this source that were `ok`", so it
-- filters on verdict before ordering.
CREATE INDEX IF NOT EXISTS idx_source_runs_baseline
    ON source_runs (source_id, verdict, created_at DESC);

-- ---------- per-field sketches: the baseline substrate ----------------------
CREATE TABLE IF NOT EXISTS field_sketches (
    source_id       TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    field           TEXT NOT NULL,
    n               INTEGER NOT NULL,
    matched         INTEGER NOT NULL,
    empty           INTEGER NOT NULL,
    error           INTEGER NOT NULL,
    container_empty INTEGER NOT NULL DEFAULT 0,
    coerced         INTEGER NOT NULL DEFAULT 0,
    coercion_failed INTEGER NOT NULL DEFAULT 0,
    len_sum         REAL NOT NULL,
    len_sumsq       REAL NOT NULL,
    len_hist        BLOB NOT NULL,             -- 16 x u16 LE (log2 length buckets)
    cls             BLOB NOT NULL,             -- 4 x f32 LE (digit/alpha/space/punct)
    distinct_ratio  REAL NOT NULL,
    minhash         BLOB NOT NULL,             -- 64 x u64 LE
    created_at      TEXT NOT NULL,
    PRIMARY KEY (source_id, job_id, field)
);

-- ---------- invariants mined from the era we believed worked ---------------
CREATE TABLE IF NOT EXISTS field_invariants (
    source_id  TEXT NOT NULL,
    field      TEXT NOT NULL,
    kind       TEXT NOT NULL,                  -- type|regex|range|nonnull|distinctness
    spec       TEXT NOT NULL,                  -- JSON
    support    INTEGER NOT NULL,
    confidence REAL NOT NULL,
    learned_at TEXT NOT NULL,
    PRIMARY KEY (source_id, field, kind)
);

-- ---------- per-key document fingerprints (previous-run comparison) --------
-- One row per key per source, same order as `records`; written in chunked
-- transactions on one held connection, never per row.
CREATE TABLE IF NOT EXISTS doc_fingerprints (
    source_id    TEXT NOT NULL,
    key          TEXT NOT NULL,
    text_simhash INTEGER NOT NULL,             -- visible text: structure-blind
    dom_simhash  INTEGER NOT NULL,             -- markup shape: text-blind
    val_simhash  INTEGER NOT NULL,             -- extracted values: the output
    seen_at      TEXT NOT NULL,
    PRIMARY KEY (source_id, key)
);

-- ---------- trust stamping on the existing record tables -------------------
-- NULL *means* 'stable'. That is a semantic default, not a sentinel: every
-- pre-migration row is correct by construction and no backfill is required.
-- (0004_simhash.sql added a derived column with a `DEFAULT 0` sentinel and no
-- backfill, silently disabling near-dup detection for 3,367 rows; this is the
-- shape that avoids repeating it.) Readers must treat NULL and 'stable' as the
-- same value — `datasets::trust_label` is the one helper that does.
ALTER TABLE records          ADD COLUMN trust TEXT;
ALTER TABLE record_revisions ADD COLUMN trust TEXT;
