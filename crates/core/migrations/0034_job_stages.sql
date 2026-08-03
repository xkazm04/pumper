-- Per-job stage timings: where a job's wall-clock actually went.
--
-- Before this, a user looking at a three-minute job could not tell scraping
-- from fan-out — the run, the search index, the watch/trigger hooks and the
-- saved-search alerts all collapsed into one `started_at → finished_at` span.
-- The worker now stamps one row per completed run, written at the END of the
-- fan-out (so every stage it names is finished when the row lands).
--
--   attempt     which attempt these numbers describe; a retried job's row is
--               replaced by its winning attempt (PK is job_id — one job, one
--               current receipt).
--   *_ms        NULL means "this job never reached that stage" (e.g. a job that
--               failed before indexing), which is deliberately distinct from 0
--               ("the stage ran and took under a millisecond"). Readers must
--               render NULL as unknown, never as zero.
--   total_ms    claim → end of fan-out, i.e. the whole slot+fan-out span. It is
--               NOT the sum of the stages: the stages exclude queue-internal
--               work (completion write, checkpoint clear, yield record).
CREATE TABLE IF NOT EXISTS job_stages (
    job_id     TEXT PRIMARY KEY,
    app        TEXT NOT NULL,
    attempt    INTEGER NOT NULL,
    run_ms     INTEGER,
    index_ms   INTEGER,
    hooks_ms   INTEGER,
    alerts_ms  INTEGER,
    total_ms   INTEGER,
    created_at TEXT NOT NULL
);
