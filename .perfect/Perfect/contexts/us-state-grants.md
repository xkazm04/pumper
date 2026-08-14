---
name: us-state-grants
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 4
last_proposed: 2026-08-14
cooldown_until: r24 (mined r22)
directions: ["[[ca-grants-sweep-says-what-it-proved]]"]
alias_of_old_map: "[[us-grant-opportunities]] (round-3 grants pass covered CA)"
---

## Current state
Not yet scouted on the 46-map. Files: crates/apps/ca-grants/src/lib.rs. Round 3's
builder corrected the scout's guessed CA columns against the live API (d59b307).
Single-state coverage; expansion beyond CA was never proposed.

## Direction history
- (as us-grant-opportunities, round 3.)

## Shipped
- **2026-08-14 (r22) [[ca-grants-sweep-says-what-it-proved]] `4cb7415`** — grants-gov's four-arm
  sweep vocabulary carried across verbatim (`complete|capped|short_page|unknown_total`);
  `truncated` is now its boolean projection; `end` is declared uninitialised so the compiler
  proves the reported arm and the actual break cannot disagree; `output_shape` gained `sweep` and
  its `unified` block went from 3 to the 6 keys `merge_into` writes.
  **Observed effect:** a renamed `result.total` no longer caps California at one page indefinitely
  while reporting a complete sweep. **The builder found a SECOND lie in the same arm the scout did
  not name:** `offset >= total` counted rows *requested*, so a page that asked 1000 and delivered
  100 still advanced the offset by 1000 — on page 2 of a 1366-record corpus `2000 >= 1366` called
  1100 records complete. Coverage is now counted on records COLLECTED. Verified destructive:
  porting the old three-arm break back fails 3 of 12 tests.
- (inherited — see [[us-grant-opportunities]])
