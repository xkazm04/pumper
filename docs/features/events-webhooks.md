# Events & webhooks

## SSE

- `GET /events` — stream of all job status transitions (`queued/running/succeeded/failed/cancelled`).
- `GET /jobs/{id}/stream` — one job's transitions; replays the current state on connect and closes at terminal.

**Live progress events.** Besides the `queued/running/succeeded/failed/cancelled` status transitions, a long-running job also emits `status: "progress"` events carrying its latest snapshot in `result` (e.g. the crawler's `{crawled, kept, failed, frontier, hosts}`). These are throttled (≥ every 2s per job) and non-terminal, so the per-job stream stays open through them; the latest snapshot is also on `GET /jobs/{id}` (`progress` field). See [runtime.md § Live progress](runtime.md#live-progress).

**Resume with `Last-Event-ID`.** Every SSE event carries a process-global monotonic id (the `id:` field). Events are also kept in a bounded in-memory replay ring (last 1024). A client that reconnects with a `Last-Event-ID: <n>` header is replayed exactly the events it missed (`id > n`), filtered to the stream's scope. If the gap is older than the ring still holds, the server first emits a single `event: reset` (carrying the latest id as its `id:`) so the client knows to resync its view before live events resume. The same ring lets a live subscriber that falls behind the broadcast buffer recover the missed events instead of dropping them silently. The per-job stream's connect-time state snapshot has no id (it is a synthesized view, not a buffered transition); only real transitions are replayable.

## Outbound webhooks — one logged contract

All webhook kinds share one delivery loop (`webhook.rs::deliver`): POST JSON, 3 attempts with linear backoff, fire-and-forget (never blocks the worker). **New event kinds must go through `webhook::dispatch_event`** — never hand-roll a send. Every delivery carries these headers:
- `x-pumper-event` — the event name.
- `x-pumper-delivery-id` — a **stable idempotency key** for this delivery: the same id across all retries AND a manual `/replay`, so a receiver can dedup (at-least-once delivery means the same delivery can arrive more than once).
- `x-pumper-timestamp` — unix seconds, set per attempt; a receiver can reject stale timestamps.
- `x-pumper-signature: sha256=<hex>` (when a secret is configured) — `HMAC(secret, "{timestamp}.{delivery_id}." ++ body)`. The timestamp and delivery id are covered, so a captured signed request can't be replayed with a fresh timestamp, and the signature binds to the idempotency key. Recompute the base to verify: concatenate the `x-pumper-timestamp` and `x-pumper-delivery-id` header values as `"{ts}.{id}."` in front of the raw body.

Kinds:

- **`job.terminal`** — job set `callback_url` (+ optional `callback_secret`) at enqueue; the finished job JSON is delivered on terminal state.
- **`dataset.changed`** — dataset **watches** (`watches` table): standing subscriptions `{app, dataset|'*', url, secret?}`. After a successful run, revisions are grouped by dataset and each covering watch receives `{event, watch_id, job_id, app, dataset, count, changes[]}` (field-level diffs included). CRUD: `GET/POST /watches`, `DELETE /watches/{id}`, `POST /watches/{id}/enabled`.
- **`search.matched`** — saved-search alerts (see [search.md](search.md)).
- **`job.failed`** — global permanent-failure firehose. When `[webhooks] failure_url` is configured, every job that fails **permanently** (attempts exhausted — app error, timeout, or a reaped stale lease) POSTs `{event, job_id, app, error, attempts, schedule_id}` there, HMAC-signed with `[webhooks] failure_secret` if set. This is distinct from `job.terminal`: a job's own `callback_url` already receives the full terminal JSON on failure, so `job.failed` is the cross-app subscription for "any job failed" (which has no natural per-resource key), not a per-job duplicate. Retryable requeues do **not** fire it — permanent failures only.

## Delivery log & dead-letter queue

Every delivery is recorded in `webhook_deliveries` (kind, ref, url, event, body, status `pending|delivered|failed`, attempts, last_error). `GET /webhooks/deliveries?status=failed` is the DLQ view; `GET /webhooks/deliveries/{id}` includes the body; `POST /webhooks/deliveries/{id}/replay` re-sends, re-signing with the source's **current** secret (job callback secret / watch secret).

## Known gaps

- Delivery log has no retention/purge job yet.
