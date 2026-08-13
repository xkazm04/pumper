---
slug: doctor-sees-search
type: perfect/direction
context: "[[maintenance-tooling]]"
lens: feature
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---
## What & why
`GET /datasets/doctor` is the store-integrity report — the surface an operator hits at 2am, and
the only one whose findings carry **named remediations**. It runs seven checks. **None of them is
about the search index**, and search is the subsystem with the repo's most manual recovery story:
`docs/features/search.md:107` concedes "recovery is the manual `search-backfill` bin, not an
automatic rebuild".

So the index can go empty — schema drift, a `[search] enabled = false` window, a corrupt-dir
quarantine — and `just doctor` reports `healthy: true` while `/search` returns nothing. The
wiped-index signal does exist (`index.degraded` on the `/search` response), but it lives on the
query path, which means the operator only learns about it when a **user** reports missing
results. The diagnostic that exists to be run *before* anyone complains is silent.

A second, smaller honesty defect in the same file ships with it. `records_without_simhash` fires
on `simhash IS NULL OR simhash = 0` and prescribes `just reindex` — but `reindex_simhashes`
rewrites only rows whose recomputed value **differs**, and a record with genuinely no textual
content recomputes to 0. The remediation text half-admits this. The endpoint's load-bearing
property — *"a clean store produces ZERO findings"* (`doctor.rs:16-18`, `datasets.md:155`) — is
therefore **permanently unreachable** on such a store, and the operator runs a whole-table rewrite
for nothing, forever.

The user moment: *"Search had been returning nothing for a week. `just doctor` said the store was
healthy the whole time."*

## Evidence (scout-supplied; verify each before building)
- `crates/core/src/doctor.rs:124-304` — seven checks, none about search.
- `crates/server/src/routes/doctor.rs:78-180` — the route; it does not touch `state.search`.
- `crates/server/src/state.rs:56` — `AppState.search` is available to the route.
- `docs/features/search.md:32` — `index.degraded` exists on the query path only; `:107` — manual
  recovery.
- `crates/core/src/doctor.rs:204-225` — `records_without_simhash` + its remediation string;
  `:216` half-admits the never-clears case.
- `crates/core/src/datasets.rs:1563-1564` — reindex rewrites only rows whose value **differs**.
- `crates/core/src/doctor.rs:16-18` and `docs/features/datasets.md:155` — the zero-findings
  invariant this breaks.

## Acceptance criteria
1. A **search-index finding** in the doctor report, in the existing finding shape (severity,
   evidence, named remediation pointing at `search-backfill`), firing when the index is
   meaningfully out of step with the store — e.g. `doc_count` against the live indexable record
   count. Choose the comparison deliberately: an exact-equality check will be noisy, since not
   every record is indexed. State the rule you chose and why it will not cry wolf.
2. **Search disabled is not a finding.** `[search] enabled = false` is a valid deployment and
   `NoSearch` answers every call with silent success (`search.rs:253-269`); a config-off store
   must stay `healthy: true`. Test it.
3. The zero-findings invariant survives: a clean store with search enabled and in step produces
   **no** findings. That property is load-bearing and already documented — do not break it to add
   a check.
4. `records_without_simhash` stops being permanently unclearable. Either it excludes records
   whose content genuinely hashes to 0 (so a clean store can reach zero findings), or the
   remediation stops prescribing a whole-table rewrite that provably will not help. Argue which,
   and pin it with a test named for the anti-pattern.
5. Route-level coverage: `/datasets/doctor` currently has **zero Rust tests** — only its pure core
   is unit-tested and the live smoke assertion is vacuous. Seed a store, drive the **route**, and
   assert the new finding fires and a clean store is silent.
6. `docs/features/datasets.md` (the doctor's finding table) and `docs/features/search.md` reflect
   the new check and the corrected remediation.

## Risks / non-goals
- The doctor is read-only and must stay so. No writes, no repair-on-read.
- Do not make the route's cost materially worse: it already runs five full scans plus an artifact
  walk. `doc_count()` is cheap; a per-record comparison would not be. If your check needs a count
  of indexable records, use an aggregate query, not a `list()`.
- Do not touch `crates/server/src/bin/search-backfill.rs` or `docs/features/search.md`'s backfill
  section beyond the remediation pointer — [[backfill-purges-ghosts]] owns that file this wave.
  Report any change you need there.
- Not an automatic rebuild. Detection and a named remediation only.

## Build record
(to fill during build)
