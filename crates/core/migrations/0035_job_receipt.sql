-- Job receipt (`GET /jobs/{id}/receipt`): make the four surfaces that describe
-- one run joinable BY JOB, in index time rather than by scanning the corpus.
--
-- Everything here is additive and nullable — a row written before this
-- migration keeps NULL, which the receipt renders as "unknown", never as zero
-- or as an inferred value.

-- Trigger lineage, the direction that was missing. `jobs.trigger_id` already
-- records WHICH trigger fired a job; this records WHICH JOB's outcome fired it,
-- so a receipt can list the hops one run caused. It was previously recoverable
-- only by suffix-matching `idempotency_key` ('trig:<trigger>:<source job>'),
-- i.e. a full scan of the jobs table.
ALTER TABLE jobs ADD COLUMN source_job_id TEXT;
CREATE INDEX IF NOT EXISTS idx_jobs_source_job ON jobs (source_job_id);

-- Revisions are already stamped with their producing job (0030), but only the
-- per-record provenance chain read them; a per-job rollup meant scanning
-- `record_revisions`.
CREATE INDEX IF NOT EXISTS idx_revisions_job ON record_revisions (job_id);

-- Extraction-health verdicts are keyed (source_id, job_id), so "the verdicts
-- THIS job produced" — the honest at-run-time health answer, as opposed to the
-- source's state right now — was a scan.
CREATE INDEX IF NOT EXISTS idx_source_runs_job ON source_runs (job_id);

-- Deliveries are looked up by status (0010) or by id; the receipt needs the
-- deliveries logged against one reference (a job id, for job callbacks and the
-- failure firehose).
CREATE INDEX IF NOT EXISTS idx_deliveries_ref ON webhook_deliveries (ref_id, created_at DESC);
