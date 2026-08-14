---
slug: grants-detail-delta-survives-restart
type: perfect/direction
context: "[[us-federal-grants]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-14
accepted: 2026-08-14
shipped: 2026-08-14
commit: a901707
---

## What & why

The federal grants detail harvest has a **loss window it was explicitly built to close, and does
not close.** The crate's own comment says the detail stage "is also the stage a restart would
silently ZERO — on a re-claim the listing re-syncs, every opportunity reads back `unchanged`, and
the delta collapses to empty. So the checkpoint carries the delta itself, not just a cursor."

That is precisely right about the danger and wrong about the remedy. The delta is computed at
`:457`; the **first** checkpoint that carries it is written at `:514`, *inside*
`if buffer.len() >= DETAIL_FLUSH` (25) — and it is the **throttled** `ctx.checkpoint`, not
`checkpoint_now`. So a crash, reaper re-queue, timeout or graceful-shutdown suspend anywhere in
the first 24 detail fetches loses the delta entirely.

The user moment: an operator restarts the server (or the reaper fires) while the daily federal
sync is 20 details into a 500-item harvest. On resume `restored_harvest` returns `None`, the
listing re-syncs, every opportunity reads back `unchanged`, `summary.new` and `summary.changed` are
empty, the delta collapses to `[]` — and the run **reports success having harvested nothing**.
The award-amount data the product exists to serve silently stops advancing, green the whole time.

## Evidence

- `crates/apps/grants-gov/src/lib.rs:445-452` — the doc comment stating exactly this failure mode
  as the checkpoint's reason for existing.
- `:457` — `capped_delta(...)` computes the delta. Nothing checkpoints it here.
- `:491` — the fetch loop begins. The first paid work happens with no durable state.
- `:508-516` — the first `ctx.checkpoint(harvest_state(..))`, gated on `buffer.len() >= DETAIL_FLUSH`
  and throttled (`CHECKPOINT_MIN_INTERVAL` = 5s, `crates/server/src/progress.rs:94`).
- `:1092` — `const DETAIL_FLUSH: usize = 25`.
- `:530` — the only unthrottled checkpoint is *after all work is done*, where it can no longer save
  anything that was at risk.
- `:1130-1152` — `restored_harvest` returns `None` on an empty `delta`, so an early snapshot with
  an empty delta is correctly ignored; the guard for criterion 2 already exists.
- Was **unassertable** until now: gated on [[harness-expresses-the-run]] shipping
  `RecordingCheckpoints`.

## Acceptance criteria

1. The delta is checkpointed **before the first detail fetch**, unthrottled (`checkpoint_now`), so
   the resumable unit exists before any work is paid for.
2. It is only written when there is something to resume — an empty `pending` writes nothing, and
   `restored_harvest`'s existing empty-delta guard (`:1150`) stays the safety net, not the
   mechanism.
3. A resumed run still restores exactly as it does today: `harvest_state_round_trips_and_resumes_where_it_stopped`
   (`:2010`) and `restored_harvest_treats_any_foreign_shape_as_start_fresh` (`:2030`) must pass
   **unchanged**. If your change requires editing either assertion, the change is wrong.
4. **A regression test using `RecordingCheckpoints` that fails on today's code**: drive the harvest,
   assert a checkpoint carrying a non-empty `delta` was saved *before* the first
   `fetch_detail`, and assert it was `force: true`. Run it against the unmodified app first and
   record that it fails — a test that passes before the fix has proved nothing.
5. No change to the detail stage's non-fatal-but-loud error contract (`:477-486`), the
   `detail_stage_is_broken` abort, or the `capped` semantics.

## Risks / non-goals

- **Non-goal:** the throttle itself. 5 s between mid-loop saves is a deliberate, documented
  trade-off; only the *first* save's absence is the defect.
- **Non-goal:** `row-key-positional-fallback` (rejected r20 as unverified against live payloads).
- **Risk:** an extra unthrottled write per run resets `JobCheckpointer`'s throttle clock, delaying
  the first in-loop save by up to 5 s. Harmless — the in-loop saves are strictly *additional*
  insurance over a snapshot that now always exists.
- **Refuted, do not chase:** an oversized checkpoint is not a live risk here.
  `MAX_CHECKPOINT_BYTES` is 8 MB (`crates/core/src/storage.rs:2490`) and `maxDetailsPerRun` caps
  the delta at 500 keys (`:175`), so `save_checkpoint`'s size rejection cannot fire for this app.

## Build record

(filled during build)
