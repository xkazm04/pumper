# Live Events & Webhooks — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 3, Low: 1)
> Files scanned: `crates/server/src/events.rs`, `crates/server/src/webhook.rs` (confirmed against `crates/server/src/routes.rs`, `crates/server/src/worker.rs`, `crates/core/src/storage.rs`, `crates/core/migrations/0010_webhook_deliveries.sql`, `crates/server/src/state.rs`)

## 1. `emit` assigns the seq id outside the ring lock, so concurrent emits corrupt ring order and wire order
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: race-condition
- **File**: `crates/server/src/events.rs:89-100`
- **Scenario**: Two tasks call `emit` concurrently (guaranteed in practice: `worker.rs:19-48` spawns up to `concurrency` per-job tasks that each call `state.events.emit()` at `worker.rs:497`, and `routes.rs` emits `"queued"` from HTTP handler tasks at 692/746/774/814 in parallel). Thread A does `fetch_add` → seq=1; thread B does `fetch_add` → seq=2. B then takes the ring lock, pushes `(2, …)`, releases; A takes the lock, pushes `(1, …)`. The ring is now `[(2,…),(1,…)]` — not monotonic. `tx.send` is also outside the lock (line 98), so B can broadcast `2` before A broadcasts `1`.
- **Root cause**: The three steps that must be atomic to preserve the "monotonic id → append to ring in order → broadcast in order" invariant are split: `fetch_add` (line 90) is lock-free, the ring push is under the `Mutex` (91-97), and `tx.send` (98) is outside it. Nothing serializes the interleaving of id assignment vs. ring insertion vs. broadcast across callers.
- **Impact**: (a) Live client receives id 2 then id 1; the stream loop drops id 1 (`routes.rs:297` `if seq <= last_seq { continue; }`) → **event permanently lost to that live subscriber**. (b) `replay` treats `ring.front()` as the oldest id (`events.rs:106,112`); with an out-of-order front it can return `Reset` even though the requested id is still buffered → **false "resync everything" storms** to reconnecting clients. (c) `pop_front()` eviction (line 94) can drop a *newer* event while keeping an older one, corrupting the replay window.
- **Fix sketch**: Serialize the whole emit under one lock: take `ring.lock()`, then assign `seq` (a plain counter under the lock, or `fetch_add` while holding it), `push_back`, and `tx.send((seq, event))` — all before releasing. That guarantees ring order, wire order, and eviction order match the id order.

## 2. Adversarial `Last-Event-ID` overflows `after + 1` in `replay`
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case
- **File**: `crates/server/src/events.rs:112` (input from `crates/server/src/routes.rs:382-387`)
- **Scenario**: A client sends `Last-Event-ID: 18446744073709551615` (u64::MAX). `last_event_id` parses any `u64` with no bound check and passes it to `replay(after)`. Line 112 computes `after + 1`, which overflows.
- **Root cause**: `after` is fully client-controlled and used in unchecked arithmetic. The gap check assumes `after` is a real, in-range cursor the server once issued.
- **Impact**: Debug builds panic (arithmetic overflow) in the SSE handler task. Release builds (production) wrap to 0, so `*oldest > 0` is true and the client is wrongly told to `Reset` and resync its entire view — a wrong result driven purely by header input. Cheap DoS-flavored nuisance / incorrect behavior.
- **Fix sketch**: Use `after.saturating_add(1)` in the comparison (or early-return `Events(vec![])` when `after >= self.latest_seq()`), so an out-of-range cursor yields "nothing to replay" instead of overflow/false-reset.

## 3. `webhook_deliveries` grows without bound — no retention or purge
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: resource-leak
- **File**: `crates/server/src/webhook.rs:154-168` (row created every delivery via `storage.create_delivery`, `crates/core/src/storage.rs:1034`; table `crates/core/migrations/0010_webhook_deliveries.sql`)
- **Scenario**: Every outbound webhook — every terminal job with a `callback_url`, every `dataset.changed` watch event, and every `job.failed` firehose delivery — inserts one row and stores the full JSON `body`. On a production scraping platform firing on each job/dataset change, rows accumulate forever. There is no `DELETE`/TTL anywhere (confirmed: only create/finish/list/get exist in `storage.rs`, and `docs/features/events-webhooks.md:29` explicitly notes "Delivery log has no retention/purge job yet").
- **Root cause**: The delivery log doubles as the durable dead-letter queue, so nothing prunes it; the schema has no retention dimension and no background sweeper references the table.
- **Impact**: Unbounded SQLite table + `idx_deliveries_status` index growth (each row carries the full payload body) → disk bloat, slower DLQ list/keyset queries, eventual operational pain. Slow-burn resource exhaustion.
- **Fix sketch**: Add a periodic prune (e.g. `DELETE FROM webhook_deliveries WHERE status = 'delivered' AND created_at < now()-retention`, keeping `failed`/`pending` for the DLQ), driven by config, and/or drop the body for delivered rows after a grace window.

## 4. When the delivery-log INSERT fails, the webhook is sent but its outcome is neither logged nor replayable
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/server/src/webhook.rs:154-165`
- **Scenario**: In `spawn_logged`, if `create_delivery` returns `Err` (DB locked/full/contended — exactly when the system is under stress), the code warns and calls `deliver(...)` anyway but discards the result with `let _ =` (line 162), then returns without ever recording a row.
- **Root cause**: The "send anyway" fallback path bypasses `log_outcome`, so this delivery has no `webhook_deliveries` row at all — it can never enter the failed/DLQ view and can never be replayed.
- **Impact**: A webhook that matters (a job-result callback) can fail with only a transient `warn!` log and be permanently unrecoverable — the exact scenario the durable DLQ exists to prevent, defeated precisely when the DB is unhealthy. Silent, unreplayable lost delivery.
- **Fix sketch**: On `create_delivery` failure, either skip the send and surface a hard error, or (better) retry the log write / write a minimal fallback row so the outcome of the attempted `deliver` is still recorded and replayable. At minimum, capture and log the `deliver` result instead of `let _ =`. (Related: `log_outcome` at 183-188 leaves a row stuck `pending` if `finish_delivery` fails, which a later replay could resend as a duplicate.)

## 5. `dispatch` and `dispatch_change` duplicate the serialize-or-warn-and-return preamble instead of delegating to `dispatch_event`
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/server/src/webhook.rs:22-101`
- **Scenario**: Three entry points repeat the same shape: `serde_json::to_vec(...)` → on `Err` `warn!` + `return` → then a `spawn_logged(client, storage, kind, ref_id, url, event, body, secret)` call. The block appears at 26-32, 53-59, and 84-90; only the log fields and the pre-serialized value differ. `dispatch_event` (74-101) is already the generic form and `dispatch_failure` already funnels through it.
- **Root cause**: `dispatch` and `dispatch_change` predate/parallel the generic `dispatch_event` and were never collapsed onto it.
- **Impact**: Cleanup only — three copies of the serialize/warn/return logic drift independently (e.g. differing warn wording), and the 8-arg `spawn_logged` (`#[allow(clippy::too_many_arguments)]`) is invoked from three sites.
- **Fix sketch**: Have `dispatch` and `dispatch_change` build their `kind`/`ref_id`/`url`/`event`/`secret` and call `dispatch_event`, which owns the single serialize-or-warn-and-return + `spawn_logged` path.
