---
slug: ingress-replay-defense
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: f908903
---

## What & why
The bare/GitHub signature scheme has no timestamp and no skew gate; when the sender also
omits `x-pumper-delivery-id`, every replay of a captured signed body mints a fresh
`Uuid::new_v4()` event id → fresh idempotency key → a new job, forever, bounded only by the
60/min token bucket. Deriving the event id deterministically from the body (the
`derived_event_id` SHA-256 machinery already exists at `ingress.rs:53-59`) turns the
existing dedup into an actual replay defense. The handler's gate ordering (409/401/403/413/
429, skew-before-MAC, rate-limit-before-crypto) is entirely untested; ingress.md also
falsely claims UUIDv5 derivation.

## Evidence
- `ingress.rs:304` — bare scheme, `context = None`, no skew gate
- `ingress.rs:320` — `Uuid::new_v4()` when no delivery id
- `ingress.rs:53-59` — deterministic derivation exists (truncated SHA-256, not UUIDv5)
- `docs/features/ingress.md` — UUIDv5 claim; posture omits the replay gap

## Acceptance criteria
- No delivery-id → event id derived deterministically from body (+ source id), so an exact
  replay dedups instead of enqueuing.
- Handler gate tests: disabled ingress (409), unknown source (404), disabled source (403),
  oversized body (413), rate limit (429), bad signature (401), stale timestamp when the
  pumper scheme is used.
- ingress.md corrected: actual derivation scheme, honest statement of the bare scheme's
  replay bounds (deterministic-id dedup, no skew window).
- No behavior change for senders that DO supply delivery ids.

## Risks / non-goals
- Legit identical-body re-sends (e.g. unchanged cron pings) now dedup — that is the correct
  semantics for an event id, but docs must say so.
- Non-goal: mandatory timestamps on the bare scheme (would break GitHub compatibility).

## Build record
- Builder T1 (opus), wave 1 → master `f908903`. `body_event_id(source, body)` shares
  `digest_uuid` with the delivery-id derivation; NUL-delimited domain separator (`\0body\0`)
  a header value cannot contain, so no delivery id can impersonate a body-derived event.
  Exact replay → same event id → 202 with triggers_fired 0, no new job. Delivery-id senders
  unchanged. e2e gate tests: 409/404/403 refusal order, 413 before signature, 429, 401 bad
  sig + 401 stale timestamp with fresh-timestamp control, and the replay-dedup e2e.
  ingress.md corrected (UUIDv5 claim → truncated SHA-256; derivation table; replay
  consequences incl. identical-body legitimate re-sends collapsing, delivery-id as escape
  hatch).
- Gates: worktree 1050/0; master full-workspace green post-pick.
