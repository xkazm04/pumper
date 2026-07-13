---
slug: grants-schema-enrichment
type: perfect/direction
context: "[[US Grant Opportunities]]"
lens: optimization
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: d59b307
---

## What & why
The canonical unified schema has NO category/eligibility/ALN fields (sent to the federal API, never captured), and CA money parsing keeps the first number found ("$1.5M" → 1.5, ranges → garbage). Add taxonomy fields to grants/unified and make money parsing handle K/M suffixes, ranges (floor+ceiling), thousands separators.

## Evidence
- No taxonomy in canonical shape: grants-common/src/lib.rs:23-38
- Lossy money_of: grants-common:115-134; guessed CA field candidates :57-59
- Fields available upstream: grants-gov request :88-99 (fundingCategories/eligibilities), ca-grants Categories column

## Acceptance criteria
- [ ] unified schema gains categories[], eligibilities[], aln (federal) — captured from both sources where present, null where absent; existing keys unchanged.
- [ ] money parsing: K/M/B suffixes, ranges → (floor, ceiling), thousands separators, currency symbols; unit tests over real observed formats (pull samples from page1.json artifacts if available, else construct from API docs).
- [ ] Re-normalization: next run upserts enriched records (change detection shows changed — expected, note in result).
- [ ] docs/features/apps.md unified-schema row updated.

## Risks / non-goals
- CA column names are guesses — builder verifies against a live datastore_search sample (the API is key-free) and reports actual columns.

## Build record
(pending)
