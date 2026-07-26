---
date: 2026-07-26
slug: shared-test-harness
status: shipped
type: weak-pattern
reach: "13 setup sites / 41 manual teardowns / 4 stub sets / 9 files"
risk: 1
effort: m
payoff: 4
branch: "(committed to master)"
commits: [ed8674f, be7371b]
related_scan: "[[Architect/scans/2026-07-26-testing-strategy]]"
---

# Shared test harness in pumper-core

## Context
The ~10-line temp-SQLite `fresh_db` fixture exists 11× (5 named copies, 6 inlined —
`core/tests/tiers.rs` defines it at :8 and re-pastes it at :21). Panicking engine-stub
trios are re-invented 4× (`core/tests/resilience.rs:300`, `apps/extractor/tests/
source_mode.rs:22`, `core/src/fetcher.rs:713`, `core/src/crawl.rs:1413`). The 18-field
`AppContext` is hand-built in 2 tests + prod (`worker.rs:172`). 41 manual
`remove_dir_all` teardowns leak on failing asserts; `tempfile` is not a dependency
anywhere; no `tests/common/`, no test-util module, no dev-deps on core/server.

## Decision
Add `pumper_core::testing` behind a `test-support` cargo feature: `TempStore` (RAII
temp-dir Storage via `tempfile`), `AppContext::for_test`-style `TestContext` builder,
exported `DeadHttp`/`DeadBrowser`/`DeadResearcher` stubs. Migrate `crates/core/tests/*`
to it (existing tests passing unchanged = behavior preserved); other crates adopt
incrementally.

## Consequences
### Positive
- New DB/app-level tests cost ~3 lines; failing tests stop leaking temp dirs;
  `AppContext` field additions become 1-site edits in test code.
### Negative / risks
- A `test-support` feature in the prod crate ships stub code if misused.
### Mitigations
- Feature is additive and off by default; only dev-dependencies enable it.

## Rollout
1. `pumper_core::testing` module + `test-support` feature + tempfile optional dep — validation: cargo check --workspace
2. Migrate core/tests/* to TempStore/TestContext — validation: cargo test -p pumper-core, tests pass UNCHANGED
3. Migrate apps/extractor integration test — validation: cargo test -p extractor crate

## Acceptance criteria
- Zero `remove_dir_all` teardowns remain in crates/core/tests.
- One canonical engine-stub set; resilience + extractor tests consume it.

## Regression checklist
- [ ] All previously-passing tests pass without assertion edits.
