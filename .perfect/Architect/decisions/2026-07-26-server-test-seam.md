---
date: 2026-07-26
slug: server-test-seam
status: shipped
type: structural-bug-class
reach: "~3,500 untested lines / 12 files in crates/server"
risk: 2
effort: l
payoff: 5
branch: "(committed to master)"
commits: [38f62f8, f7c7287]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# A test seam into the server composition layer

## Context
`AppState::init` (`crates/server/src/state.rs`) is monolithic: hardcodes `registry::apps()`,
constructs real HTTP/Browser/Claude engines, spawns background tasks. Consequence:
`worker.rs` (708 lines; the `suppress_unhealthy` → `notify_watches` →
`fire_dataset_triggers` ordering is enforced by a comment), `webhook.rs` (287; HMAC wire
contract untested), 9 of 12 `routes/*` files (~2,700 lines), and shutdown drain have zero
tests. `crates/server` has no `tests/` dir and no `[dev-dependencies]`.

## Decision
Split `AppState::from_parts(...)` out of `init` (additive; `init` delegates). Add
`crates/server/tests/` with: `test_state()` over TempStore + Dead engines + injectable
registry; a scriptable `FakeApp: ScrapeApp`; a `TestReceiver` local axum sink (template:
`engine-http/tests/profiles.rs:36`); router tests via `tower::ServiceExt::oneshot`.
Thread `now` through `scheduler::reconcile` (mirrors `decide`). Land the five e2e tests
from the scan: webhook signature contract; worker success fan-out; cursor paging + error
envelope; scheduler overlap guard; shutdown drain.

## Consequences
### Positive
- The joints between well-tested storage and the outside world get a regression net;
  Tier-2 guards from [[2026-07-26-prose-only-bug-class-guards]] land here.
### Negative / risks
- state.rs/worker.rs carry uncommitted in-flight datahub work — commits will layer on top.
- Worker loop refactor (single-pass seam) touches the hottest file in the repo.
### Mitigations
- Additive-only refactors; existing behavior asserted by the new tests before any cleanup;
  baseline-delta validation per commit.

## Rollout
1. `AppState::from_parts` + test-support plumbing + dev-deps (tower, tempfile) — cargo check
2. Webhook signature/headers/retry contract test (no AppState needed) — cargo test -p pumper-server
3. `FakeApp` + single worker-pass seam (`run_one`/drive `execute`+`finalize`) — cargo test
4. Worker success fan-out e2e (job → events → webhook+HMAC → trigger hop → index) — cargo test
5. Router oneshot: cursor paging, 404 envelope, extract/preview 400 — cargo test
6. Scheduler `now` injection + overlap-guard test — cargo test
7. Shutdown drain test (drain deadline → requeue survivor) — cargo test

## Acceptance criteria
- A reordering of finalize's side effects or suppress_unhealthy's gate makes a test red.
- A change to the HMAC base string makes a test red.
- Cursor paging over 5 rows/limit 2 yields 5 distinct items, null final cursor.

## Regression checklist
- [ ] Full workspace suite at baseline rate.
- [ ] Server boots (`cargo run -p pumper-server` smoke) — from_parts refactor is behavior-neutral.
