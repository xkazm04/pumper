---
slug: extractor-records-echo
type: perfect/direction
context: "[[declarative-extractor]]"
lens: optimization
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
urls/source/archive modes serialize EVERY extracted record into the persisted job result —
a 10k-record source run writes a multi-megabyte job row into SQLite, rides every
`job.succeeded` webhook/SSE consumer, and lives in the jobs table forever. The write path
also deep-clones each record once purely to build this echo. The records are already
durably written to the dataset; the full echo is redundant transport.

**The load-bearing coupling (verified, do not skip):** `worker.rs:1689-1711 search_docs`
indexes search documents FROM `result["records"]` when the result declares no
`index_datasets`. A blind cap silently shrinks search coverage — the fix must move the
extractor onto the delta-driven `index_datasets` path (`worker.rs:1745+`, the r6/r7-hardened
route) FIRST, then bound the echo.

## Evidence
- `crates/apps/extractor/src/lib.rs:422-432` — `records.push(rec.clone())` (the extra clone).
- `lib.rs:1075`, `:1280`, `:1555` — `"records": out.records` in urls/source/archive results.
- `crates/server/src/worker.rs:1689-1711` — result-echo indexing path (`_records`/`_job`).
- `worker.rs:1745-1764` — `index_datasets` delta path, O(changes), already generic.
- `crates/server/src/e2e/app_fetch_chokepoint.rs:294`, `e2e/mcp.rs:281` — tests assert
  `records[0]` (must stay within any default cap).

## Acceptance criteria
- Extractor write-mode results declare `index_datasets` naming the dataset(s) actually
  written, so search indexing rides the change feed instead of the echo.
- The records echo is bounded (param, e.g. `records_echo`, with a sane default and hard
  ceiling) with an explicit `records_truncated: true` + total count when the bound bites.
- **No search-coverage regression**: prove (test) that a capped-echo run still gets its
  records indexed via the dataset path. If `search_docs` would now double-index the first
  N echoed records alongside the dataset path, REPORT the needed `worker.rs` adjustment
  (skip echo-indexing when `index_datasets` is present) as a Director-applied change —
  `worker.rs` is OUTSIDE your write set.
- The per-record clone for the echo is gone (build the echo from the bounded prefix only).
- `docs/features/extraction.md` documents the echo bound and `index_datasets` declaration.

## Risks / non-goals
- Risk: a consumer somewhere reads the full `result.records` (repo grep found only e2e
  tests + the search path). The bound must be overridable per-job for such a caller.
- Risk: `_records`/`_job` reserved-dataset semantics (snapshot sweep, ghost-doc GC) —
  understand `sweeps_prior_job_snapshot` + r7's `_job` sweep before changing what gets
  indexed where; state in the report what happens to previously-indexed `_records` docs.
- Non-goal: touching the dataset write path or upsert semantics.

## Build record
(pending)
