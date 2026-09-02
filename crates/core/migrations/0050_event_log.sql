-- N05: the durable event log and its cursor subscriptions.
--
-- Before this, the event bus was an in-memory ring (1024 events / 32 MiB) whose
-- sequence restarted at 0 on boot, so a reconnecting client whose gap had been
-- evicted was told `reset` and had to rebuild its view — and an event nothing
-- was watching at the instant it happened left no trace at all.
--
-- `seq` is the SAME number the bus stamps on the wire (`Last-Event-ID`), not a
-- second identity: the bus seeds its counter from `MAX(seq)` at boot, so a
-- cursor survives a restart. It is an explicit INTEGER PRIMARY KEY (not
-- AUTOINCREMENT) because the writer supplies the value.
CREATE TABLE IF NOT EXISTS events (
    seq        INTEGER PRIMARY KEY,
    -- `job.queued` | `job.succeeded` | `dataset.changed` | `external` | … —
    -- the subscribable vocabulary. Derived from the bus event, never free text
    -- from a caller.
    kind       TEXT NOT NULL,
    -- The namespace the event belongs to: a job's app, a dataset's app, or an
    -- ingress source id for an inbound external event.
    app        TEXT NOT NULL,
    -- Job id, watch id, dataset name, transaction id — whatever the kind's
    -- subject is. Empty string when the kind has none.
    subject_id TEXT NOT NULL DEFAULT '',
    payload    TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- The retention janitor's scan (`created_at < cutoff`).
CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at);
-- The two filtered keyset pages `GET /events/log` and the outbox drain serve.
CREATE INDEX IF NOT EXISTS idx_events_kind ON events (kind, seq);
CREATE INDEX IF NOT EXISTS idx_events_app ON events (app, seq);

-- One subscription = (event selector, sink, cursor). The outbox drain reads
-- events past `cursor_seq`, dispatches each through the SAME `deliver` path a
-- webhook takes (so the delivery log, the DLQ ladder and manual replay are
-- unchanged), and advances the cursor.
CREATE TABLE IF NOT EXISTS subscriptions (
    id           TEXT PRIMARY KEY,
    -- Operator-facing label; not an identity.
    name         TEXT,
    -- JSON `{kinds: [..], app, dataset, filters: [{pointer, equals}]}`.
    selector     TEXT NOT NULL,
    -- `webhook` | `slack` | `file` | `plugin:<name>` — the same vocabulary a
    -- watch's sink has, resolved by the same transport switch.
    sink         TEXT NOT NULL DEFAULT 'webhook',
    url          TEXT NOT NULL DEFAULT '',
    secret       TEXT,
    -- The highest `events.seq` this subscription has a delivery row for.
    cursor_seq   INTEGER NOT NULL DEFAULT 0,
    enabled      INTEGER NOT NULL DEFAULT 1,
    principal_id TEXT,
    created_at   TEXT NOT NULL,
    last_delivered_at TEXT,
    last_error   TEXT
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled ON subscriptions (enabled);

-- A watch IS a subscription with a dataset selector (N05): the old table stays
-- the source of truth for watch rows and the old routes keep working, but the
-- fan-out now runs through the outbox, so a watch needs the same cursor.
ALTER TABLE watches ADD COLUMN cursor_seq INTEGER NOT NULL DEFAULT 0;
