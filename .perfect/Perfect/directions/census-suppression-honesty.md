---
slug: census-suppression-honesty
type: perfect/direction
context: "[[us-business-census]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---
## What & why
Suppressed Census cells currently FABRICATE data: a withheld `NRCPTOT` becomes
`$0` succession receipts end-to-end, directly contradicting the field's own doc
comment ("needs real receipts, not a defaulted 0") — with a flagged-not-fixed
test in the repo documenting it. Separately, census-density hard-fails a whole
multi-trade run on one bare 204 while its three siblings skip-and-note, and
nothing anywhere counts what was suppressed or dropped. The user moment: a $0
succession market means measured zero, not withheld data; a partially-suppressed
run says how partial it was; one empty API answer doesn't zero out the scrape.

## Evidence
- Fabrication chain: census-nonemp/src/lib.rs:368 `unwrap_or(0)` →
  census-density/src/lib.rs:827-829 (`if let Some` always true → Some(0)) →
  :892-895 emits 0; field doc :762-765 contradicted; flagged test
  nonemp:472-484 ("Flagged, not fixed").
- 204: density:268-288 aborts run (204 passes is_success, then non-JSON error);
  siblings guard first: nonemp:213-219, nesd:265-273, bfs:186-188. Bughunt
  2026-07-14 #3, still open.
- Silent drops: density:466-468 base<=0 → None, no counter; :516-526 summary has
  places_matched but no exclusions; suppressed-cell `continue`s count nothing
  (density:331-335, nonemp:363-366, nesd:481-483).
- per-10k mixes coverage classes silently: density:860-870 vs coverage field
  :843-847.

## Acceptance criteria
1. Suppressed receipts stay absent end-to-end: nonemp emits no numeric
   receipts_thousands on suppression, blend keeps solo_receipts_thousands
   `None`, succession_receipts `Null`; the flagged test is FLIPPED to assert the
   fix and renamed after the anti-pattern it now defends.
2. Empty-answer guard extracted as a named census-common function used by ALL
   FOUR apps (including density) — 204/empty body skips-with-note, never aborts;
   `x_not_y` test. Raw `ctx.engines.http` call sites must NOT move between files
   (the chokepoint inventory pins path+count).
3. Suppression/coverage telemetry: run results count suppressed cells per
   dataset and places excluded for base<=0; proven by test.
4. per-10k honesty: blend either segments by coverage or the cell carries an
   explicit machine-readable coverage caveat — builder judgment, but the silent
   mixed-coverage number must not survive unchanged.
5. Fixing this must not fabricate the other way: genuine zeros ("0" receipts)
   remain measured zeros (census_num already distinguishes — pin with a test).

## Risks / non-goals
- No API-surface schema changes; no catalog edits here.
- Downstream consumers of receipts_thousands: verify none besides blend
  (scout found none) before removing the field on suppression.

## Build record
(to fill during build)
