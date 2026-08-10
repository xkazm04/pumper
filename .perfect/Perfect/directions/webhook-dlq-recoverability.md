---
slug: webhook-dlq-recoverability
type: perfect/direction
context: "[[webhook-delivery]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: 4d753fc
---

## What & why
The dead-letter queue exists so that a receiver outage never loses an event — but today
it structurally cannot recover two of the four delivery kinds it stores, re-sends rows
that are already delivered or in flight, and leaks crash-interrupted rows forever. This
direction makes the DLQ honest: every kind replays SIGNED, replay is claim-guarded, and
no row can get stuck in an unrecoverable state. User moment: "my alert webhook failed
while my endpoint was down; when it came back, the delivery arrived, signed, exactly
once."

## Evidence
- `crates/server/src/webhook.rs:366-384` — `resolve_secret` matches only `"job"` and
  `_ → get_watch`. Kind `"search"` (`worker.rs:1324`, ref_id = saved-search id) and kind
  `"failure"` (`webhook.rs:238`, ref_id = job id; the secret is
  `config.webhooks.failure_secret`) both fall through to `get_watch` → `None` → every
  retry/replay is UNSIGNED and a verifying receiver rejects it down the whole ladder.
  `storage.get_saved_search` exists (`storage.rs:1233`) and is never called here.
- `crates/server/src/routes/triggers.rs:542-563` — `replay_delivery` spawns
  unconditionally: no status check, no `begin_delivery_retry` claim; races the drain and
  itself; re-sends `delivered` and `dead` rows.
- `crates/core/src/storage.rs:1442` — `due_deliveries` scans `status='failed'` only; a
  crash between `create_delivery` (row `pending`, `storage.rs:1338`) and `log_outcome`
  leaves the row `pending` forever; prune (`storage.rs:1563-1578`) touches only
  `delivered`/`dead` → unbounded, un-prunable leak.

## Acceptance criteria
- [ ] `resolve_secret` resolves ALL four kinds: `job` → callback secret; `change` →
      watch secret; `search` → saved-search secret; `failure` → `[webhooks].failure_secret`
      from config (thread config access as needed). Unit tests per kind, including the
      deleted-source → None path. Test names follow the `x_not_y` idiom (e.g.
      `search_replay_signed_not_unsigned`).
- [ ] Manual `POST /webhooks/deliveries/{id}/replay` only replays rows in a replayable
      state (`failed`/`dead`; product call: `delivered` re-send allowed only with an
      explicit `?force=true`), claims atomically before sending (extend
      `begin_delivery_retry` or add an equivalent claim covering `dead`), and returns
      409 when the row is in-flight/claimed. OpenAPI + doc updated.
- [ ] Stale `pending` rows are reclaimed: a named, tested function (janitor or
      drain-adjacent) moves `pending` rows older than a threshold back into the retry
      ladder (`failed` + `next_retry_at`), so crash windows self-heal and the table is
      prunable end-to-end. Threshold generous (≥ 10× worst-case in-process delivery,
      i.e. > 51s·safety) to never reclaim a live row — state the margin in a comment.
- [ ] Storage-level test: crash-shaped `pending` row (old `updated_at`, no
      `next_retry_at`) → reclaimed → drain picks it up → ladder proceeds to
      `delivered`/`dead`. No live `pending` row (fresh `updated_at`) is reclaimed.
- [ ] Repo law: fixes land as extracted named functions with anti-pattern-named tests,
      wired at call sites after tests exist.

## Risks / non-goals
- Non-goal: drain throughput (batching, per-tick budget) — deferred direction.
- Non-goal: delivery ordering, event→delivery attribution.
- Risk: `failure`-kind secret lives in config not DB — resolving it inside
  `resolve_secret` needs the config (or the failure secret snapshot) reachable from the
  drain; keep the seam explicit, don't clone config wholesale into Storage.
- Risk: widening `begin_delivery_retry` to `dead` must not let the auto-drain resurrect
  `dead` rows — only the manual route may; keep the claims distinct.

## Build record
Lot W builder (opus) died before its first commit; complete work found dirty, Director-verified and committed as 4d753fc. resolve_secret now has all 4 arms (search/failure replay signed); replay_gate + separate begin_delivery_replay claim (409 on non-dead unless ?force=true); stale-pending reclaim at 600s (~11.8x worst-case delivery time). Review: keep. Gates: fmt/check/workspace --lib/clippy-calibrated green; smoke 17/17.
