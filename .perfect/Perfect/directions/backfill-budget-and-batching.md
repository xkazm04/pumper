---
slug: backfill-budget-and-batching
type: perfect/direction
context: "[[dataset-api]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 1fde4ac
---

## What & why
`POST /derived/{id}/backfill` loops the ENTIRE source dataset synchronously inside the HTTP
request — no time/row budget, no cancellation, no progress, restart-from-zero on client
timeout. Per source record the row path re-parses the spec's filter specs and issues one
point query per lookup join: backfilling 100k rows with a lookup = 100k parses + 100k extra
queries. The group path hoists the parse correctly; the row path doesn't.

## Evidence
- `routes/derived.rs:277` → `core/datasets.rs:2087-2122` (unbounded sync loop)
- `core/datasets.rs:2050` (per-record `parse_filter_specs`), `:2061` (per-record lookup get)
- Group path hoists: `core:2143/:2412`

## Acceptance criteria
- Filter specs parsed once per backfill (hoisted); lookup joins batched (chunk the keys, one
  query per chunk). Before/after measured on a large synthetic source and reported.
- Backfill is budgeted: time and/or row cap per request with a resumable cursor in the
  response, so a client drives it to completion incrementally; idempotent across resumes.
- A big-source test proves a single request no longer runs unbounded (budget respected).
- Doc-sync: derived docs describe the budgeted/resumable contract.

## Risks / non-goals
- Response shape of backfill changes (adds cursor/progress) — consumer-visible, document it.
- Non-goal: making backfill a queued job (bigger redesign; budget+resume gets the operational
  win without cross-context work).

## Build record
- Builder D2 (opus), wave 2 → master `1fde4ac` (gate in flight at write time). Filters
  parsed once/request; joins via deduped IN-chunks bounded by MAX_BIND_PARAMS
  (`live_records_by_key`, mirrors read_key_states); live recompute shares the same fn.
  **Measured: 50k rows + 500-key lookup 2.75s → 1.77s (−36%); 16,667 point queries → ~100
  chunked reads.** Budget: `{batch, max_rows, cursor}` → `{…, done, cursor?}`, default 50k;
  idempotent resume/restart/overlap. Aggregates: max_rows is a HARD CEILING (400, writes
  nothing) — builder decision I endorse: resumable-but-wrong partial totals refused;
  operators raise max_rows instead. Documented in code + datasets.md.
- Refuted: "restart-from-zero unpaged scan" was half right — it already keyset-paged
  internally; the missing thing was the cross-request budget/cursor.
- Honest: contract change verified at type/core level, not live HTTP; perf numbers one run,
  harness #[ignore]d asserting equality not timings.
- Gates: worktree full workspace 1072/0.
