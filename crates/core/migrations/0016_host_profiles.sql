-- Host profiles: tier-memory v2. Extends `tier_memory` (the per-host learned
-- HTTP-vs-browser router) with the governor's learned politeness penalty, so
-- that penalty survives a restart (write-behind snapshot + restore on boot)
-- and every learned host signal is inspectable via GET /hosts.
--
-- Strike/pin aging reuses the existing `updated_at` (last tier-outcome change):
-- a host whose strikes are older than `[fetcher] host_memory_ttl_secs` is
-- treated as stale — the browser pin lapses and the next HTTP loss starts a
-- fresh strike count instead of re-pinning immediately.
--
-- `penalty_updated_at` is the write-behind snapshot time; it deliberately does
-- NOT touch `updated_at`, so persisting a penalty never resets strike aging.
ALTER TABLE tier_memory ADD COLUMN penalty_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tier_memory ADD COLUMN penalty_updated_at TEXT;
