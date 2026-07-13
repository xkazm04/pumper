---
slug: api-pagination-errors
type: perfect/direction
context: "[[HTTP API & Routes]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 0a91f46
---

## What & why
Repo has a documented cursor convention but most list endpoints ignore it: /schedules /watches /triggers /searches /plugins /webhooks/deliveries return unbounded full tables; /datasets changes+history clamp (1000/500) with NO cursor — rows past the clamp are unreachable. Every core error collapses to blanket 500; cancel/retry return 409 for not-found. Sweep for consistency.

## Evidence
- Unbounded lists: routes.rs:420-422, 713-719, 784-790, 1012-1021, 1115-1118, 1208-1210
- Cursorless clamps: routes.rs:653-677 (changes), 688-704 (history)
- Blanket 500: routes.rs:76-80; 409 conflation: routes.rs:335-356
- Convention: harness-learnings.md:44 (`cursor=` keyset → {items,next_cursor})

## Acceptance criteria
- [ ] Cursor keyset pagination on schedules/watches/triggers/searches/deliveries/changes/history (dual-mode like /jobs for back-compat).
- [ ] Errors: not-found → 404, wrong-state → 409, validation → 400; body gains stable `code` alongside `error`.
- [ ] Existing consumers unbroken (bare-array mode preserved when no cursor param).
- [ ] docs/features/http-api.md updated (remove "no pagination" gap note).

## Risks / non-goals
- Non-goal: changing envelope keys of already-conventional endpoints.

## Build record
(pending)
