-- Sink connectors (M22): a watch's delivery side becomes a typed connector.
-- `sink` selects how `dataset.changed` payloads are landed:
--   'webhook' (default) — POST JSON at `url`, HMAC-signed (existing behavior)
--   'file'              — NDJSON append under data/sinks/<watch_id>.ndjson
--   'slack'             — Slack incoming-webhook message POSTed at `url`
-- Every sink rides the same webhook_deliveries log / retry / DLQ machinery.
ALTER TABLE watches ADD COLUMN sink TEXT NOT NULL DEFAULT 'webhook';
