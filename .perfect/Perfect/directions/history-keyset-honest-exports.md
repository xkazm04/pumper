---
slug: history-keyset-honest-exports
type: perfect/direction
context: "[[dataset-api]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 2aa150d
---

## What & why
Two silent-corruption bugs. (a) `history_page`'s keyset predicate leads on `created_at` but
the query orders by `revision DESC` — the predicate's leading column is not the ORDER BY's
leading column, so any revision whose `created_at` is out of order with its `revision`
(clock skew, backfill, import) silently skips or repeats rows across a page boundary.
`changes_page` gets the same pattern right. (b) The export streamer swallows mid-stream
store errors: logs a warn, `break`s, and for `format=json` still emits the closing `]` — a
truncated export is HTTP 200, valid JSON, indistinguishable from complete. Per-record
serialization failures also drop rows silently.

## Evidence
- `core/datasets.rs:669-674` (history predicate vs ORDER BY) vs `:722` (changes, correct)
- `routes/datasets.rs:396-399` (break on store error), `:430-431` (closing `]` anyway),
  `:409/:415` (silent row drops)

## Acceptance criteria
- History keyset predicate and ORDER BY aligned (order by `revision` → predicate leads on
  `revision`, or both lead on `created_at, revision`); boundary test with out-of-order
  `created_at` proves no skip/repeat.
- Mid-stream export failure produces detectably-truncated output: abort the stream without
  emitting the JSON closing terminator (and equivalent honesty for ndjson/csv), plus an
  error-level log.
- Per-row serialization failures are counted and logged, not silently dropped.
- Extracted, named fixes with anti-pattern-named tests (repo doctrine).

## Risks / non-goals
- Cursor format for /history may change (revision-led) — empty/garbage cursors already fall
  back to page 1, so old cursors degrade safely; document it.
- Non-goal: unifying all three cursor formats (note for a future direction).

## Build record
- Builder D1 (sonnet), wave 1 → master `2aa150d` (gate pending at write time).
  (a) history_page ORDER BY aligned to `(created_at DESC, revision DESC)` — cursor format
  unchanged, pure ordering fix; test-only `set_revision_created_at_for_test` behind
  `test-support` enables the clock-skew test (5 revisions, scrambled created_at, paged 1-at-
  a-time, no skip/repeat). (b) Export: mid-stream store error now yields a stream Err —
  hyper closes without the chunked terminator, so truncation is a detectable transfer
  failure, never a clean 200; extracted `format_batch`/`append_row`/
  `export_may_emit_terminator` with anti-pattern-named tests; per-row serde failures
  counted + logged.
- Worktree gates: 1044 tests 0 failed.
