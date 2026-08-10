---
slug: search-enrich-hardening
type: perfect/direction
context: "[[search-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: ed2c683
---

## What & why
Entity enrichment panics on non-ASCII input: `enrich.rs:108` slices
`lowered[start-120..start]` at a byte offset that need not be a char boundary — any accented
char/em-dash 1-119 bytes before a date match panics INSIDE the writer-lock closure, poisons
the lock, and drops the whole batch (recovered, lossy, near-silent). All 13 tests are
ASCII-only. Also: RFC3339 timestamps invisible to event_date (`\b` fails before `T`);
`$1.234,56` indexes as $1; enrichment (2 full-body lowercase allocs + 4 regex scans/doc)
runs inside the writer-lock critical section.

## Evidence
- `enrich.rs:108` (byte slice), `:42-43` (ISO regex + \b), `:34-39` (money regex)
- `lib.rs:338-347` (enrichment inside write_deferred closure under Mutex)
- `worker.rs:796` (batch dropped on write failure, warn only)

## Acceptance criteria
- Char-boundary-safe keyword window (extracted fn + non-ASCII tests incl. the exact
  boundary case).
- RFC3339/timestamp-suffixed dates recognized for event_date.
- European-decimal amounts skipped conservatively, never mis-indexed (test).
- Enrichment computed BEFORE the writer lock is taken; lock section = index ops only.
- Each fix anti-pattern-named per repo doctrine.

## Risks / non-goals
- Non-goal: non-USD currencies, doc-level max-amount semantics (note, don't change).

## Build record
- Builder SE1 (opus), wave 1 → master `ed2c683`. `keyword_window` snaps the lower bound
  FORWARD to a char boundary (shrink-only, panic-free) — builder caught that its own first
  test never hit a mid-codepoint offset and fixed the test. RFC3339 suffixes recognized
  (the `\b` genuinely never fired before `T`). `is_ambiguous_decimal_tail` drops
  separator+digit tails conservatively. Enrichment moved OUT of the writer lock (separate
  spawn_blocking stages; a panic now yields JoinError, not a poisoned mutex) and both
  extractions share ONE lowercase pass. 18 enrich tests.
- Also documented the entity-filter surface in search.md (didn't exist at all — SE2's
  parity direction extends it).
- Gates: worktree 1107/0; wave-1 integration gate green.
