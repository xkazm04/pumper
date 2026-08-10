---
slug: grant-recurrence-relation
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 49ca08c
---
## What & why
`link_duplicates` applies one fixed Hamming distance across every domain and excludes only same-
source pairs. Nothing distinguishes *"same program, different portal"* (a true duplicate) from
*"same program, next annual cycle"* (not a duplicate at all). Today the second case is a false link.
Typed as its own relation it becomes the most useful thing this dataset can tell a grant-seeker:
this program reopens annually, and the next cycle is due in roughly eleven months — turning a
false positive into the signal a user actually wants.

## Evidence
- One fixed distance, cross-source filter only: `crates/apps/grants-common/src/lib.rs:57,623-645`.
- Links written to `grants/duplicate_links` keyed `a|b`: `lib.rs:632-636`.
- Signals available in the canonical schema to discriminate with: `aln`, `agency`, `title`,
  `open_date`, `close_date` (`lib.rs:135-156`).
- Lifecycle events already model `Reopened` for the single-record case (`lib.rs:329-357`), so the
  vocabulary for recurrence exists conceptually but not across records.

## Acceptance criteria
- Recurrence is a DISTINCT link type from duplicate — not merely a tightened threshold.
- Discrimination uses real signals (ALN, agency, title stem, cycle year, prior close dates), each
  cited in the code where it is used.
- A recurrence link carries its observed period and the next expected window.
- Precision over recall: an uncertain pair stays an ordinary duplicate link rather than being
  promoted to a confident-sounding prediction.
- Tests over fixtures containing BOTH a genuine cross-portal duplicate AND a genuine annual cycle,
  asserting each is typed correctly.

## Risks / non-goals
Do not predict a next cycle from a single observation — a period needs evidence. Not an ML model:
deterministic signals only (the user has rejected LLM-driven features three times; this must stay
rule-based).

## Build record
(pending)
