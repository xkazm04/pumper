# Strong Patterns

Load-bearing patterns identified by `/architect`.

## Inventory-as-test (bidirectional EXPECTED diff)
- Identified: 2026-07-26
- Reach: routes inventory, catalog↔registry (both ways), config-validates-shipped-file — 6 tests
- Why it works: drift physically cannot land; failure messages name the fix direction; allowlists are reviewable test data.
- Codification status: docs-written (harness-learnings "Testing conventions")
- Codified: 2026-07-26 · Codification ADR: [[Architect/decisions/2026-07-26-codify-testing-patterns]]
- Examples: `crates/server/src/routes/mod.rs:373`, `crates/core/src/config.rs:870`

## X-not-Y test naming + //! module docs
- Identified: 2026-07-26
- Reach: repo-wide; strongest in crates/core/tests
- Why it works: the guarded set is legible at a glance; the test name IS the anti-pattern statement.
- Codification status: docs-written · Codified: 2026-07-26 · ADR: [[Architect/decisions/2026-07-26-codify-testing-patterns]]
- Examples: `a_degrading_source_cannot_tombstone_its_own_dataset`, `unmappable_rows_are_skipped_not_fabricated`

## Deterministic time (now-as-param + SQL backdating, no sleeps)
- Identified: 2026-07-26
- Reach: all storage/scheduler tests; extended to `scheduler::reconcile` this run
- Why it works: zero flaky-timing tests in 349; the two prior violations are now #[ignore]-gated.
- Codification status: docs-written · Codified: 2026-07-26 · ADR: [[Architect/decisions/2026-07-26-codify-testing-patterns]]
- Examples: `scheduler::decide(.., now, ..)`, `core/tests/jobs.rs` insert_queued backdating

## Extract-then-test
- Identified: 2026-07-26
- Reach: explains guard status of all 15 catalog bug classes
- Why it works: an extracted named fn is trivially testable; an inline predicate needs the whole world. Guard coverage correlates ~perfectly with extraction.
- Codification status: docs-written (CLAUDE.md rule — loads every session)
- Codified: 2026-07-26 · ADR: [[Architect/decisions/2026-07-26-codify-testing-patterns]]
