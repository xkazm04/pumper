---
slug: extraction-quality-signal
type: perfect/direction
context: "[[Declarative Extraction Engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 70221c1
---

## What & why
Every miss is a silent Null — broken selector indistinguishable from absent field; failed fetch → all-null record still upserted; no matched/missed counts anywhere. Add an optional match-report channel (per-field: matched|empty|error), per-URL fetch attribution, fields_matched aggregates in results. The prerequisite signal for any future self-healing.

## Evidence
- Silent nulls: crates/core/src/extract.rs:290-316, :372 (css), :296-308 (regex/json/xpath)
- Fetch failure → empty string → all-null upsert: crates/apps/extractor/src/lib.rs:70-74
- Aggregate-only counts: extractor/lib.rs:76, 101-108

## Acceptance criteria
- [ ] extract_one/extract_batch gain an optional report mode (per-field status enum; zero-cost when not requested or measured-negligible).
- [ ] Extractor: failed fetches attributed per URL (not upserted as all-null records — skip + report), result gains fields_matched/fields_total aggregates + worst-fields list.
- [ ] Match-status enum serde-stable (consumed by [[ruleset-preview-endpoint]]).
- [ ] Unit tests per rule type (matched/empty/error); docs/features/extraction.md updated.

## Risks / non-goals
- Non-goal: auto-healing loop (rejected rules:auto direction covers drafting; healing is a future round IF the user wants it).

## Build record
(pending)
