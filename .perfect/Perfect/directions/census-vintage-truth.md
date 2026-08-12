---
slug: census-vintage-truth
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
The blend looks fresh weekly while resting on 2021/2022 vintages; a re-run with
an older `year` param rewrites current data backwards as a forward "change"
(poisoning change feeds/watches); `census/saturation` is keyed `{place}` only,
so county/state runs and denominator changes silently overwrite each other; a
mixed-grain NAICS registry entry double-sums blend cells (aggregate + its own
component); and BFS velocity trusts positional windows over a possibly-gappy
NSA series with a ×4 annualization. The aggregation layer must tell the truth
about WHAT it aggregates and WHEN it is from.

## Evidence
- Hardcoded vintages: density:38 ("2022"), nonemp:36 ("2021"), nesd:51 ("2021");
  density:77-79 schedule commented out, nonemp has none.
- Keys carry no vintage: density:366-369, nonemp:378, nesd:501; saturation key
  = place only (density:496). Model to copy: bfs formations `US|{sector}|{period}`
  (bfs:253-254).
- Weekly churn: bfs schedule :88-90 + sync_market_blend call :312 — formation
  block updates make every blend cell "changed" weekly while CBP/NES data is
  years old; nothing surfaces per-input vintages.
- Grain double-count: trades-common:752-763 naics_prefixes (no grain check),
  blend truncation density:812 `chars().take(4)`; both "2382" and "238220" from
  one registry entry double-sum cell 2382. (Fix census-side; trades-common is
  OUT of the write set — other contexts consume it.)
- Velocity: bfs:392-419 positional windows, no contiguity check; :408 ×4 on
  explicitly NSA data (:182); as_of never compared to now.

## Acceptance criteria
1. Backwards-rerun protection — DESIGN DECISION with options, builder chooses
   with recorded reasoning: (a) vintage in record keys + a coexistence/latest
   story for readers, or (b) vintage as a pinned field + per-dataset vintage
   watermark that refuses/flags an older-year overwrite. Either way: a re-run
   with an older year must not silently rewrite current data as a forward
   change — test proves it.
2. Saturation key carries its grain (place + geo + denominator, or refuses
   mixed-grain writes); a county run and a state run no longer overwrite each
   other — test.
3. Blend rows carry a vintages/as_of block naming each input's vintage (CBP
   year, NES year, NES-D year, BFS as_of) so updated_at churn stops implying
   fresh market data — test pins the block's presence and content.
4. Mixed-grain NAICS dedup: named pure function in census-common resolving
   covering-prefix overlaps (options: keep aggregate + drop covered components,
   or keep finest + drop the aggregate — builder decides, reasoning recorded);
   the literal 2382/238220 double-count killed by test. trades-common untouched.
5. Velocity honesty: non-contiguous months → no velocity emitted or an explicit
   flag; as_of staleness surfaced on the record; the ×4 NSA annualization
   labeled (e.g. accel_basis) — no silent seasonal artifact. Tests.

## Risks / non-goals
- Key changes create new rows; without sync_many, old-keyed rows linger — the
  builder must state the migration story for whichever option it picks (a
  one-time reconcile note is acceptable; silent duplication is not).
- No new schedules this round (vintage-bump automation noted as future work).
- No trades-common edits (write-set boundary).

## Build record
(to fill during build)
