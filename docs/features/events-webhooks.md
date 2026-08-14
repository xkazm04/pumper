# Events & webhooks

## SSE

- `GET /events` — stream of all job status transitions (`queued/running/succeeded/failed/cancelled`).
- `GET /jobs/{id}/stream` — one job's transitions; replays the current state on connect and closes at terminal.

**Live progress events.** Besides the `queued/running/succeeded/failed/cancelled` status transitions, a long-running job also emits `status: "progress"` events carrying its latest snapshot in `result` (e.g. the crawler's `{crawled, kept, failed, frontier, hosts}`). These are throttled (≥ every 2s per job) and non-terminal, so the per-job stream stays open through them; the latest snapshot is also on `GET /jobs/{id}` (`progress` field). See [runtime.md § Live progress](runtime.md#live-progress).

**`checkpoint_failed` — the other non-terminal kind (2026-08-14).** A job whose durable checkpoint does not land also emits `status: "checkpoint_failed"`, carrying `{reason: "stale_lineage" | "storage_error", attempt}` in `result`. `stale_lineage` means the job was reset, reaped or re-claimed and another attempt now owns it, so this task's snapshot was fenced off; `storage_error` means the write itself failed (an oversized blob, a full disk, a locked DB). Only the **first** failure of each kind announces: a stale-lineage run fails *every* subsequent save, and one event per save would flood the bus with a single fact. A throttle-skipped save is not a failure and never announces. The running tally rides the stored result as `checkpoint_failures: {stale_lineage, storage_error, total}` — absent entirely when every save landed, never a fabricated zero. Like `progress`, this status is non-terminal, so the per-job stream stays open through it (only `succeeded`/`failed`/`cancelled` close a stream). **Known gap:** a run that never reaches a stored result — failed, timed out, or shutdown-suspended — carries its tally only on this event and in the log. See [runtime.md § Durable execution](runtime.md).

**Resume with `Last-Event-ID`.** Every SSE event carries a process-global monotonic id (the `id:` field). Events are also kept in a bounded in-memory replay ring (last 1024, **and** a 32 MiB byte budget — whichever binds first — so a burst of large-result jobs can't pin ~1 GB of RSS; the oldest events are evicted to stay under both). Each buffered event is held behind an `Arc`, so the ring, the broadcast slot, and every subscriber share one allocation instead of deep-cloning a multi-MB `result` per copy. A client that reconnects with a `Last-Event-ID: <n>` header is replayed exactly the events it missed (`id > n`), filtered to the stream's scope. If the gap is older than the ring still holds, the server first emits a single `event: reset` (carrying the latest id as its `id:`) so the client knows to resync its view before live events resume. The same ring lets a live subscriber that falls behind the broadcast buffer recover the missed events instead of dropping them silently. The per-job stream's connect-time state snapshot has no id (it is a synthesized view, not a buffered transition); only real transitions are replayable.

## Outbound webhooks — one logged contract

All webhook kinds share one delivery loop (`webhook.rs::deliver`): POST JSON, 3 attempts with linear backoff (0s / 2s / 4s), never blocking the worker. **New event kinds must go through `webhook::dispatch_event`** — never hand-roll a send. Every delivery carries these headers:
- `x-pumper-event` — the event name.
- `x-pumper-delivery-id` — a **stable idempotency key** for this delivery: the same id across all retries AND a manual `/replay`, so a receiver can dedup (at-least-once delivery means the same delivery can arrive more than once).
- `x-pumper-timestamp` — unix seconds, set per attempt; a receiver can reject stale timestamps.
- `x-pumper-signature: sha256=<hex>` (when a secret is configured) — `HMAC(secret, "{timestamp}.{delivery_id}." ++ body)`. The timestamp and delivery id are covered, so a captured signed request can't be replayed with a fresh timestamp, and the signature binds to the idempotency key. Recompute the base to verify: concatenate the `x-pumper-timestamp` and `x-pumper-delivery-id` header values as `"{ts}.{id}."` in front of the raw body.

Kinds (the `kind` column on a delivery row, and where its signing secret comes from):

| `kind` | Event | `ref_id` | Secret source |
| --- | --- | --- | --- |
| `job` | `job.terminal` | job id | the job's `callback_secret` |
| `change` | `dataset.changed` | watch id | the watch's `secret` |
| `search` | `search.matched` | saved-search id | the saved search's `secret` |
| `failure` | `job.failed` | job id | `[webhooks] failure_secret` (config, not a row) |

- **`job.terminal`** — job set `callback_url` (+ optional `callback_secret`) at enqueue; the finished job JSON is delivered on terminal state.
- **`dataset.changed`** — dataset **watches** (`watches` table): standing subscriptions `{app, dataset|'*', url, secret?, sink}`. After a successful run, revisions are grouped by dataset and each covering watch receives `{event, watch_id, job_id, app, dataset, count, changes[]}` (field-level diffs included). CRUD: `GET/POST /watches`, `DELETE /watches/{id}`, `POST /watches/{id}/enabled`, plus the per-watch delivery log `GET /watches/{id}/deliveries`. See [Watchable namespaces](#watch-namespaces) for what `app` may be, and [Sinks](#sinks) for the non-HTTP delivery targets.
- **`search.matched`** — saved-search alerts (see [search.md](search.md)).
- **`job.failed`** — global permanent-failure firehose. When `[webhooks] failure_url` is configured, every job that fails **permanently** (attempts exhausted — app error, timeout, or a reaped stale lease) POSTs `{event, job_id, app, error, attempts, schedule_id}` there, HMAC-signed with `[webhooks] failure_secret` if set. This is distinct from `job.terminal`: a job's own `callback_url` already receives the full terminal JSON on failure, so `job.failed` is the cross-app subscription for "any job failed" (which has no natural per-resource key), not a per-job duplicate. Retryable requeues do **not** fire it — permanent failures only.

Every kind above resolves its secret again on **every** retry and replay, from the source row (or config for `failure`), so a rotated secret takes effect on the next send. A source that was deleted resolves to "no secret" and the delivery is re-sent unsigned — exactly as it was first sent.

**A degrading source never pushes.** Before `dataset.changed` watches (and dataset triggers) are evaluated, the run's revision batch is filtered per **dataset** by extraction health: a dataset whose source state suppresses outbound pushes is dropped from the batch, so no watch and no trigger ever sees it. The filter runs *before* the hooks on purpose — a delivered webhook cannot be recalled, so the ordering is the enforcement. It is per-dataset rather than per-job, so a break in one of an app's datasets doesn't silence the others. **No-op unless `[resilience] enforce` is on** (default off — see [resilient-extraction.md](resilient-extraction.md)). Suppression is a silent drop plus a warn log, not a delivery-log row; the DLQ below is only about deliveries that were attempted.

<a id="watch-namespaces"></a>
## Watchable namespaces, and proving a watch is alive

A watch's `app` is the **namespace the records land under**, which is not always the app that produced them. The fan-out matches watches against the entry app of the run's change batch, and that batch spans every namespace the run declares in its result's `index_datasets` — so a `ca-grants` run publishes its unified records as app `grants`, and `grants` is the namespace to watch.

`POST /watches` gates `app` on the set of namespaces the fan-out can actually deliver under: every registered app, the declared virtual namespaces (`registry::VIRTUAL_NAMESPACES` — today just `grants`), every namespace that already holds records (this is what covers caller-named `app-peer` mirror namespaces), and every saved search's materialize target. Two refusals:

- **`404`** — the namespace is none of those. When the name is a source app that publishes elsewhere, the message says where (`records for 'ca-grants' land under app 'grants'`).
- **`400`** — the namespace is real but the named `dataset` demonstrably belongs to another one, e.g. `{app: "ca-grants", dataset: "unified"}`. A dataset nothing has written yet is accepted: a brand-new app's first dataset is indistinguishable from a typo until it runs.

> Before this, `POST /watches {app: "grants"}` answered **404** (the one namespace worth watching was the one you could not watch) while `{app: "ca-grants", dataset: "unified"}` was **accepted** and sat enabled forever without ever firing.

`?app=` on `GET /watches` and `GET /triggers` is validated against the same set (plus, for triggers, ingress source ids and `*`, since an `external` trigger's `source_app` is an ingress source; plus, for both, the values already stored, so tightening the create gate can never 400 a filter that returns rows). An unmatchable value is a **400** naming the accepted ones, not a `200` with an empty list — the same rule `?status=` follows, for the same reason.

**Is this watch alive?** `GET /watches` enriches each row with `last_delivery` — `{id, status, at}`, or an explicit `null` when it has never delivered — so a watch that has never fired is distinguishable from one firing into a dead receiver. `GET /watches/{id}/deliveries?status=&limit=&cursor=` is that watch's own delivery log (same four-state `status` vocabulary and validator as `GET /webhooks/deliveries`; bodies excluded — fetch one from `GET /webhooks/deliveries/{id}`). An unknown watch id is a **404**, not `{count: 0}`.

**Known gap:** "already holds records" proves records exist, not that the fan-out carries them. A namespace whose producers never declare `index_datasets` — today `trades`, written by the trades apps through `trades_common::unified` — is accepted and its watch will not fire. `last_delivery: null` is what surfaces that.

<a id="sinks"></a>
## Sinks — where a `dataset.changed` event is delivered

A watch's `sink` (a first-class `POST /watches` param, default `webhook`) selects the delivery *connector*. The body is shaped at dispatch time; only the transport branches. Every sink therefore rides the identical machinery: one delivery-log row, the same in-process retries, the same backed-off DLQ drain, the same manual replay.

| `sink` | Target | Body | Signed? |
| --- | --- | --- | --- |
| `webhook` (default) | the watch's `url` | the full `{event, watch_id, job_id, app, dataset, count, changes[]}` payload | yes, when `secret` is set |
| `slack` | the watch's `url` (a Slack incoming-webhook URL) | a compact `{"text": "pumper: \`app/dataset\` changed — N revisions (job J)"}` summary — **never** the raw revision batch | yes, when `secret` is set; Slack ignores the `x-pumper-*` headers |
| `file` | `data/sinks/<watch_id>.ndjson` (appended) | one NDJSON line `{delivery_id, event, delivered_at, payload}` | no — see below |

Two `file`-sink quirks worth knowing before you create one:

- **`url` is ignored.** `POST /watches` with `sink: "file"` accepts a missing or arbitrary `url` and stores the empty string; the destination is derived from the watch id alone. The delivery row's `url` is the pseudo-URL `file://<watch_id>.ndjson`, which the transport re-validates on every send (bare filename characters only, no `..`, no separators) so nothing in the delivery log — including a tampered row — can write outside `data/sinks/`. On the `webhook`/`slack` sinks `url` is required and must be `http(s)`, or the create is a 400.
- **`secret` is stored but never used.** A file append has no HTTP request to sign, so no signature is computed and the envelope carries none. The stable `delivery_id` in each line is the dedup key instead — a line re-appended by a drain retry or a manual replay repeats it.

WASM (`plugin:<name>`) sinks are deliberately out of scope; the seam for them is the transport branch in `webhook.rs::deliver`.

## Delivery lifecycle, log & dead-letter queue

Every attempted delivery is recorded in `webhook_deliveries` (kind, ref, url, event, body, status, attempts, retry_count, next_retry_at, last_error).

A delivery passes through one pre-persistence phase and four persisted states:

| State | Meaning | Next |
| --- | --- | --- |
| *(queued)* | Accepted onto the bounded delivery pool; **no row yet**. | → `pending` as soon as the log row is written |
| `pending` | In flight: a first send, a drain retry, or a manual replay owns it. | → `delivered` / `failed` / `dead` |
| `delivered` | The receiver accepted it (2xx), or the file sink appended the line. | terminal |
| `failed` | The send failed and the row is **still on the retry ladder**, with a `next_retry_at`. **Not the DLQ.** | → `pending` on the next due drain tick |
| `dead` | The ladder gave up (retry cap reached). **This is the dead-letter queue.** | only a manual `/replay` moves it |

**In-process attempts.** Each send tries up to 3 times with 0s/2s/4s linear backoff (~6s worst case at the 15s client timeout, 51s absolute worst case). Only after all three fail does the row leave `pending`.

**Auto-drain (`[webhooks] auto_retry`, default on).** A `failed` delivery is not lost until a human replays it. A background drain — piggybacked on the scheduler tick — re-sends `failed` deliveries whose backoff is due, in batches of 20, with exponential backoff **30s → 1m → 5m → 30m → 2h** (mild jitter to de-sync a herd that failed during the same outage). Each retry bumps `retry_count`; past the cap (5 retries) the row becomes **`dead`**. Set `auto_retry = false` to revert to manual-only replay.

**Stale-pending reclaim.** A process killed mid-send leaves a `pending` row that nothing would ever pick up (the drain scans `failed` only). At the head of every drain tick, rows that have sat `pending` for more than **10 minutes** are returned to the ladder as `failed`, marked immediately due, with `last_error` set to `interrupted: reclaimed after no delivery outcome was recorded`. `retry_count` is deliberately *not* bumped by the reclaim itself — the following drain claim bumps it — so a crash-looping row still reaches `dead` after the usual number of retries instead of cycling forever. Ten minutes is ~11.8× the 51s worst-case in-process send, so a merely-slow delivery is never double-sent.

**Shutdown drains deliveries.** Deliveries run on a bounded, drainable pool (16 concurrent, 1024 queued), not detached tasks. On graceful shutdown the worker drains the job fan-out and then the delivery pool within its `[worker] shutdown_drain_secs` budget, so a clean stop leaves every delivery either terminal or scheduled. Anything still walking its ladder at the deadline is counted and logged, and its `pending` row is picked up by the stale-pending reclaim on the next boot — late, never lost.

### API

- `GET /webhooks/deliveries?status=&limit=&cursor=` — the log, newest first. Bodies excluded. Dual-mode: `{count, deliveries}`, or `{items, next_cursor}` when `cursor` is present.
  - `status` accepts exactly `pending` | `delivered` | `failed` | `dead`. **`?status=dead` is the dead-letter view**; `?status=failed` is still-retrying. Anything else is a **400** naming the allowed values.
- `GET /webhooks/deliveries/{id}` — one delivery, body included.
- `POST /webhooks/deliveries/{id}/replay[?force=true]` — re-send, re-signing with the source's **current** secret. Guarded on two levels: the row must be in a replayable state, and the send only happens after the row is atomically *claimed*, so a manual replay can neither duplicate an in-flight delivery nor race the auto-drain for the same row. Answers **202** `{id, replaying: true}` on success, **404** if the row is gone, and **409** when:
  - the row is `pending` — a sender already owns it;
  - the row is `delivered` and `?force=true` was not passed — a duplicate the receiver never asked for is always a deliberate act;
  - the claim lost a race to the auto-drain between the status check and the claim.

  `failed` and `dead` replay without `force`. The drain's own claim covers `failed` only, so the auto-drain can never resurrect a `dead` row — that is what "dead" means — while an operator naming a row by id may.

### Metrics

`GET /metrics` exports delivery health (see [runtime.md § Metrics](runtime.md#metrics) for the full series list):

- `pumper_webhook_deliveries{status="pending|delivered|failed|dead"}` — counts by state.
- `pumper_webhook_oldest_undelivered_seconds` — age of the oldest delivery the receiver has not accepted (`pending` + `failed`; `0` when there is none). `dead` is excluded on purpose: it is terminal, so including it would pin the gauge at the age of the oldest dead row forever and the alert could never clear. **This is the series to alert on** — a climbing value means the receiver has been refusing for that long.
- `pumper_webhook_delivery_attempts_total` — send attempts across the log, retries included.
- `pumper_webhook_deliveries_succeeded_total` — deliveries the receiver accepted.

All four are read in one aggregate pass so they describe the same instant, and all are **DB-derived**: the retention sweep below can lower the `_total` series, exactly as `pumper_job_failures_total` already documents for jobs.

### Retention

Two opt-in knobs under `[storage]`, both `0` (off) by default:

| Key | Prunes | Why separate |
| --- | --- | --- |
| `webhook_delivery_retention_days` | `delivered` rows older than N days | successful history, safe to age out |
| `webhook_dead_letter_retention_days` | `dead` rows older than N days | a dead letter is failure *evidence*; deleting it is a deliberate, separate decision |

`pending` and `failed` rows are **never** pruned by either knob — they are the live retry queue. The sweep runs in the retention janitor every 6 hours and does nothing at all while both knobs are `0`. `GET /retention/preview` shows what a sweep would reclaim without deleting anything.

## Changes

- **`?status=` on `GET /webhooks/deliveries` now validates.** This is a **behavior change**: an unknown value (a typo, or `dead-letter`) used to bind straight into the query and answer `200 {"count": 0}` — indistinguishable from "you have no such deliveries" on the endpoint whose whole job is to surface undelivered webhooks. It is now a `400` naming `pending, delivered, failed, dead`. An absent or empty `?status=` still means "no filter".
- **The docs previously called `failed` the dead-letter view.** They were wrong from the commit that introduced `dead`: `failed` means *still retrying*, and `dead` is the DLQ. Fixed here, in the route's OpenAPI description, and in the storage doc comment.

## Known gaps

- No per-endpoint success-rate breakdown: the metrics are whole-log aggregates, so "which receiver is failing" still means reading `GET /webhooks/deliveries?status=dead` and looking at `url`.
- The delivery log is unauthenticated like the rest of the API, and `GET /webhooks/deliveries/{id}` returns the full body — which for `dataset.changed` is the revision batch. See [../deployment.md](../deployment.md) for the auth posture.
- Delivery-pool sizing (16 concurrent / 1024 queued) is a constant, not a config key.
