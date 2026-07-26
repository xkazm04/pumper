---
date: 2026-07-26
slug: prose-only-bug-class-guards
status: shipped
type: structural-bug-class
reach: "6 bug classes / ~15 files"
risk: 1
effort: m
payoff: 5
branch: "(committed to master)"
commits: [8885c96, a217596]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# Guard the six prose-only bug classes

## Context
Regression-guard audit of the 15-class catalog in harness-learnings.md: 8 guarded,
1 partial, 6 unguarded — (a) silent-success-on-empty (10 code guards, 0 tests),
(c) detect_removed empty-`present` no-op (`datasets.rs:522` deletable, suite stays green),
(e) `safe_path_segment` artifact traversal (`app.rs:459`, file has no test module; profile
variant of the same class has 3 tests), (f) sync_many discipline (comments only),
(k) `keyset_cursor` (`routes/error.rs:86`, no tests, 9 sites), (l) dispatch_event/HMAC
(`webhook.rs` zero tests). harness-learnings calls (k)/(l) "structural" — prose-only today.

## Decision
Land Tier-1 guards now (pure-unit, no new infra): empty-`present` assertions in the existing
datasets test; `safe_path_segment` test module; `keyset_cursor` test module; hackernews
`parse_front_page` test (guards parse + empty-is-error decision). Tier-2 guards ((l) HMAC
wire contract, (i) raw-engine metering) land with the server-test-seam decision. Tier-3
(EXPECTED-style allowlists for sync_many/cursor sites) queued as follow-up.

## Consequences
### Positive
- The two cheapest already-bit-once classes ((c), (e)) become revert-proof.
### Negative / risks
- Tier-1 only covers 4 of 6 classes; (f)/(l) remain prose until follow-ups.
### Mitigations
- Tier-2 explicitly folded into the server-seam ADR rollout; Tier-3 in backlog.

## Rollout
1. datasets: empty-`present` no-op assertions — validation: cargo test -p pumper-core
2. core/src/app.rs: test module for safe_path_segment — validation: cargo test -p pumper-core
3. routes/error.rs: keyset_cursor test module — validation: cargo test -p pumper-server
4. hackernews: parse_front_page test on inline HTML slice — validation: cargo test -p hackernews app crate

## Acceptance criteria
- Deleting the guard at datasets.rs:522 makes the suite red.
- Gutting safe_path_segment to Ok(()) makes the suite red.
- Hand-rolling a wrong cursor encode in keyset_cursor makes the suite red.

## Regression checklist
- [ ] Full workspace test run at baseline rate.
