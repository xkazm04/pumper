-- Webhook delivery log: every outbound delivery (job callbacks and dataset
-- watches) is recorded with its final status. Failed rows are the dead-letter
-- queue; POST /webhooks/deliveries/{id}/replay re-sends them.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id         TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,            -- 'job' | 'change'
    ref_id     TEXT NOT NULL,            -- job id or watch id
    url        TEXT NOT NULL,
    event      TEXT NOT NULL,            -- x-pumper-event header value
    body       TEXT NOT NULL,
    status     TEXT NOT NULL,            -- 'pending' | 'delivered' | 'failed'
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_deliveries_status
    ON webhook_deliveries (status, created_at DESC);
