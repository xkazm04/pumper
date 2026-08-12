---
slug: extractor-records-echo
type: perfect/direction
context: "[[declarative-extractor]]"
lens: optimization
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 742cd44
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
- Builder (Lot X, opus) commit `742cd44`. Director review: **keep** — records_echo
  (default 100, ceiling 1000, 0 = counts-only, NO unbounded option by design); clone
  paid only for the echoed prefix; backfill echoes 0 → clone-free write path;
  `index_datasets` declared on all four write modes, GATED producer-side on the source's
  own verdict (`indexable: !state.skips_search_index()` — the worker's gate reads the
  spec pair's health, and `<dataset>@q` is a pair nothing judges, so producer-side is
  the only honest gate; same vocabulary as grants_common::indexable); coverage proof
  in-crate: capped echo (1 of 5) still yields 5 change-feed revisions with snapshots.
- **Director-applied follow-up LANDED as `e9c3c32`** (at Lot J quiescence): guard
  `search_docs`'s records/stories/items loop with `if result.get("index_datasets")
  .is_none()` — else the echoed prefix double-indexes (`_records` + dataset path). Keep
  the `docs.is_empty()` whole-result fallback OUTSIDE the guard (grants/peer runs keep
  their `_job` doc). Expected consequence: extractor runs mint a `_job` doc → one
  `delete_dataset(app, "_job")` sweep per run, same shape as other index_datasets apps.
  Rollout note: previously-indexed `extractor:_records` docs are never swept — one-call
  recovery `DELETE /search/datasets/extractor/_records`; reindex does NOT clear them.
- Banked (builder find): `versions_for` also reads with SOURCE_LIST_LIMIT — a URL with
  >10k archived versions truncates silently; same class as the sweep cap. And: an e2e
  "capped-echo run yields N dataset-scoped docs, zero _records docs" once the worker
  guard lands.
