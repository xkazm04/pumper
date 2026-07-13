# Events & webhooks

## SSE

- `GET /events` — stream of all job status transitions (`queued/running/succeeded/failed/cancelled`).
- `GET /jobs/{id}/stream` — one job's transitions; replays the current state on connect and closes at terminal.

**Resume with `Last-Event-ID`.** Every SSE event carries a process-global monotonic id (the `id:` field). Events are also kept in a bounded in-memory replay ring (last 1024). A client that reconnects with a `Last-Event-ID: <n>` header is replayed exactly the events it missed (`id > n`), filtered to the stream's scope. If the gap is older than the ring still holds, the server first emits a single `event: reset` (carrying the latest id as its `id:`) so the client knows to resync its view before live events resume. The same ring lets a live subscriber that falls behind the broadcast buffer recover the missed events instead of dropping them silently. The per-job stream's connect-time state snapshot has no id (it is a synthesized view, not a buffered transition); only real transitions are replayable.

## Outbound webhooks — one logged contract

All webhook kinds share one delivery loop (`webhook.rs::deliver`): POST JSON, `x-pumper-event` header, optional HMAC-SHA256 body signature (`x-pumper-signature: sha256=<hex>`), 3 attempts with linear backoff, fire-and-forget (never blocks the worker). **New event kinds must go through `webhook::dispatch_event`** — never hand-roll a send.

Kinds:

- **`job.terminal`** — job set `callback_url` (+ optional `callback_secret`) at enqueue; the finished job JSON is delivered on terminal state.
- **`dataset.changed`** — dataset **watches** (`watches` table): standing subscriptions `{app, dataset|'*', url, secret?}`. After a successful run, revisions are grouped by dataset and each covering watch receives `{event, watch_id, job_id, app, dataset, count, changes[]}` (field-level diffs included). CRUD: `GET/POST /watches`, `DELETE /watches/{id}`, `POST /watches/{id}/enabled`.
- **`search.matched`** — saved-search alerts (see [search.md](search.md)).

## Delivery log & dead-letter queue

Every delivery is recorded in `webhook_deliveries` (kind, ref, url, event, body, status `pending|delivered|failed`, attempts, last_error). `GET /webhooks/deliveries?status=failed` is the DLQ view; `GET /webhooks/deliveries/{id}` includes the body; `POST /webhooks/deliveries/{id}/replay` re-sends, re-signing with the source's **current** secret (job callback secret / watch secret).

## Known gaps

- Delivery log has no retention/purge job yet.
