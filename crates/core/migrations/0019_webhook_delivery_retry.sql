-- Auto-drain the webhook dead-letter queue. A failed delivery now carries a
-- `retry_count` (drain-initiated retries so far) and a `next_retry_at` (when the
-- background drain may next re-send it). A periodic drain task re-sends
-- `status = 'failed'` rows whose `next_retry_at` is due, with exponential
-- backoff; past the retry cap a row becomes `'dead'` so the DLQ view stays
-- meaningful. Pre-existing rows default to retry_count 0 / no scheduled retry
-- (they predate the feature; replay them manually if needed).
ALTER TABLE webhook_deliveries ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE webhook_deliveries ADD COLUMN next_retry_at TEXT;

-- Drives the drain's due-scan: failed rows ordered by when they're next due.
CREATE INDEX IF NOT EXISTS idx_deliveries_retry
    ON webhook_deliveries (status, next_retry_at);
