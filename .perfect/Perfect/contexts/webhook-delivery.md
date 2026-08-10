---
name: webhook-delivery
type: perfect/context
group: Event Pipeline
category: lib
opportunity: 6
last_proposed: 2026-08-10
cooldown_until: round 10
directions: ["[[webhook-dlq-recoverability]]", "[[webhook-delivery-drain]]", "[[webhook-observability]]"]
---

## Current state (scout brief digest, 2026-08-10 — Director-verified key claims in code)

Event production is solid: EventBus with monotonic ids + dual-bounded replay ring
(`events.rs:163-189`, count 1024 / 32 MiB), SSE resume, lagged-subscriber recovery.
Delivery funnels 4 producers (job callback, watch, saved-search, `[webhooks].failure_url`
firehose) through `dispatch_event` → `spawn_logged` → `deliver` (`webhook.rs:187-327,427`)
with HMAC `{ts}.{id}.body` signing, a 5-step retry ladder 30s→2h then `dead`
(`storage.rs:1382-1433`), an atomically-claimed auto-drain on the scheduler tick, and
three sinks (webhook/slack/file NDJSON with a traversal guard).

**CONFIRMED defects (Director read the code):**
- `resolve_secret` (`webhook.rs:366-384`) has arms for `job` + `_→get_watch` only, but
  kinds `search` (ref_id = saved-search id, `worker.rs:1324`) and `failure` (ref_id =
  job id, secret lives in config, `webhook.rs:238`) are also written → every drain retry
  and manual replay of those kinds goes out UNSIGNED; a signature-requiring receiver
  401s it through the whole ladder to `dead`. The DLQ structurally cannot recover two of
  its four kinds. Zero tests on `resolve_secret`.
- Manual replay (`routes/triggers.rs:542-563`) has no status check and no claim — it
  re-sends `delivered`, `dead`, and in-flight `pending` rows, racing the auto-drain.
- Crash windows leak forever: `due_deliveries` scans only `status='failed'`
  (`storage.rs:1442`); a crash between `create_delivery` and `log_outcome` leaves
  `pending` rows that are never re-scanned and never pruned (prune touches only
  `delivered`/`dead`, `storage.rs:1563-1578`).
- Every delivery escapes the drainable FanoutPool via bare `tokio::spawn`
  (`webhook.rs:258,285`) → shutdown drops in-flight deliveries; e2e tests paper over it
  with 30s polling deadlines (the sink-delivery flake's structural root).
- Docs/OpenAPI/param say `failed` is the dead-letter view; the terminal state is `dead`
  since ebf0954. `?status=typo` → empty list, not 400. Sinks entirely undocumented in
  docs/features; retention keys absent from shipped config.toml.
- No webhook metrics whatsoever in `/metrics` (`routes/meta.rs:53-149`).

Also noted (not served this round): unvalidated `callback_url` at jobs enqueue incl. a
`file://` write-primitive into data/sinks (`routes/jobs.rs:123`, `webhook.rs:436`);
redirect policy re-sends signatures cross-host (`state.rs:152-154`); drain throughput 20
per tick = 80/min (`webhook.rs:52`); `fail_delivery` 2 round trips; `list_deliveries`
OR-NULL index defeat; no per-host circuit breaker; no event→delivery attribution
(`routes/receipt.rs:183-186`); plaintext secrets, no rotation path (auth itself is a
PARKED decision — do not re-propose).

## Direction history
- 2026-08-10 (round 8, autonomous, director-self-gated): proposed 5, accepted 3 —
  [[webhook-dlq-recoverability]] (robustness), [[webhook-delivery-drain]] (robustness),
  [[webhook-observability]] (feature).
  **REJECTED: webhook-egress-hardening** (callback_url validation + file:// closure +
  redirect policy) — on an unauthenticated localhost API the enqueue caller already has
  full API power, so the SSRF/write-primitive adds no real privilege; consistency
  polish, not outcome value. Redirect-policy fix folded as a note into
  [[webhook-delivery-drain]]'s non-goals for a future pass.
  **REJECTED (deferred): webhook-drain-throughput** (batch drain + single-statement
  fail_delivery + index-friendly list) — real numbers (10k backlog ≈ 2h to attempt) but
  behind the two correctness directions in value; banked as a seed for the next webhook
  pass. Seed-decay rule applies: re-verify before proposing.

## Shipped
- 2026-08-10 · [[webhook-dlq-recoverability]] → `4d753fc` — resolve_secret covers all
  4 written kinds (search/failure replays now signed), replay gated by status +
  atomic claim (409 / ?force=true), stale-`pending` reclaim at 600s. The DLQ can now
  recover everything it holds. (Builder died pre-commit; Director recovered + committed.)
- 2026-08-10 · [[webhook-delivery-drain]] → `614e7e3` — deliveries moved into a
  dedicated 16/1024 FanoutPool, shutdown drains in dependency order; the 4-session
  sink-delivery flake class is structurally dead (both e2es dropped deadline polls).
- 2026-08-10 · [[webhook-observability]] → `c3d1f52` — delivery_health aggregate,
  4 /metrics series, ?status 400-validation, events-webhooks.md rewritten (the
  "failed is the DLQ" doc bug dead), retention keys shipped in config.toml,
  smoke 15→17 checks.
