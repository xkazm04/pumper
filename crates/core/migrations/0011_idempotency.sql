-- Idempotent enqueue: a client-supplied key makes POST /apps/{name}/jobs safe
-- to retry — the same key returns the original job instead of a duplicate.
ALTER TABLE jobs ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;
