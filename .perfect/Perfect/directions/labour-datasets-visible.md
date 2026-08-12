---
slug: labour-datasets-visible
type: perfect/direction
context: "[[czech-labor-market]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: fdc8c3d
---
## What & why
Twelve datasets, zero discovery: neither mpsv app emits `index_datasets`, so no
per-record search, no saved-search alert, and no watch/trigger can ever fire on
the `cz-labour` namespace — the nowcast/gap/lifecycle trio is exactly the data
an operator would watch ("alert me when the projected median for my role
moves"). The consumers grep found ZERO production readers today; the discovery
path is the missing half of the product.

## Evidence
- Zero `index_datasets` matches in both app files.
- Consumers gate on it: worker.rs:1878-1946 (dataset_search_docs),
  :1509-1516 (run_indexed_apps → saved-search/watch scope).
- Pattern: grants-common/src/lib.rs:113-124, 204-209.
- Virtual namespaces are watchable since r11 (5ee2462, NamespaceIndex) — the
  seam exists; these apps just never announce their datasets to it.
- Mixed key grain (raw CZ-ISCO vs 4-digit unit group) undocumented as a join
  hazard — mpsv-vpm:1129-1141 vs :422-430.

## Acceptance criteria
1. Both apps emit `index_datasets` covering at least wages + the cz-labour trio
   (salary_gap, salary_nowcast, vacancy_lifecycle); per-dataset include/exclude
   judgment recorded for the other eight (volume vs value).
2. Namespace reachability proven at the strongest achievable level: emission
   unit tests required; consumption may rest on the worker's existing generic
   tests (name them); e2e only if achievable without live fetches — report
   honestly what was not driven.
3. Search doc text is sensible for nowcast/gap rows (role/region/values — not a
   raw JSON dump); builder judgment, pinned by test.
4. The key-grain split (raw CZ-ISCO vs unit-group truncation) documented where
   consumers look (apps.md or datasets.md) as an explicit join hazard.
5. NamespaceIndex/watch registration verified for `cz-labour` (r11 seam): a
   watch on the namespace is accepted and would-fire (test at whatever level
   the harness supports).

## Risks / non-goals
- No TS SDK changes, no new routes.
- Indexing volume: vacancy_samples/employers likely excluded — justify.

## Build record
(to fill during build)
