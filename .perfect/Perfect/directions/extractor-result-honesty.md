---
slug: extractor-result-honesty
type: perfect/direction
context: "[[declarative-extractor]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
The extractor's persisted result lies in five distinct ways, and each lie has a concrete
victim moment:
1. The manifest `output_shape` promises `{extracted, errors, dataset, new, changed,
   unchanged, removed?}` — no mode emits `extracted`, `errors`, `dataset`, or `removed`.
   An MCP agent building on the manifest parses keys that never come.
2. Source mode lists at most `SOURCE_LIST_LIMIT` (10k) live records and reports that
   count as `requested` with NO truncated signal — a 12k-record dataset silently
   extracts 10k and the caller believes it covered everything.
3. Backfill's result drops the health verdict and `worst_fields` entirely (tallies
   fields_matched/total but not the verdict the run computed).
4. `register_rules` failure is a `warn!` only — the run's revisions are permanently
   non-replayable (no rules_hash) and the result carries no trace of that degradation.
5. No mode echoes the dataset actually written (quarantine enforcement can redirect the
   write to `<dataset>@q`; the result never names the real target even in the normal case).

## Evidence
- `crates/apps/extractor/src/lib.rs:897-905` — output_shape text.
- `lib.rs:1155-1170` — source mode `list(.., SOURCE_LIST_LIMIT)` → `requested`.
- `lib.rs:1412-1428` — backfill result (no `health`, no `worst_fields`).
- `lib.rs:939-945` — registration failure → warn, `rules_hash = None`, silent.
- `lib.rs:1060-1076`, `:1265-1281` — result shapes (no `dataset` key).

## Acceptance criteria
- `output_shape` describes what each mode actually returns (or the modes emit the missing
  keys — builder's call per key; truthfulness is the criterion, direction of fix is not).
- Source mode result carries an explicit truncation signal when the 10k cap bit
  (e.g. `truncated: true` + the honest live-record total, or documented `requested`
  semantics + flag — pick one, test it with >cap fixtures at a small injected cap).
- Backfill result includes `health` and `worst_fields` like the other write modes.
- Registration failure surfaces in the result (e.g. `rules_hash: null` +
  `rules_registration_error: <reason>`); test named for the silent-degradation
  anti-pattern.
- Every write-mode result names the dataset it actually wrote to.
- `docs/features/extraction.md` result-shape section updated to match.

## Risks / non-goals
- Non-goal: changing extraction behavior or write semantics — only what the result claims.
- Risk: result-shape additions are additive (new keys), so existing consumers keep
  working; do NOT rename existing keys.

## Build record
(pending)
