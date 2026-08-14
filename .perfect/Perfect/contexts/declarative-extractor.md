---
name: declarative-extractor
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 6
last_proposed: 2026-08-14
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[targets-read-keys-truncated]]", "[[extractor-mode-door]]", "[[extractor-result-honesty]]", "[[extractor-records-echo]]"]
alias_of_old_map: "[[declarative-extraction-engine]] (round-3 pass; its app-side work lands here)"
---

## Current state
Not yet scouted on the 46-map (extraction-core's banked round-10 scout covered its core
seams from the other side). Files: crates/apps/extractor/src/{lib,induce,replay}.rs.
The extractor app: declarative RuleSet mode + stored-pages source mode (66b063f r3) +
induce param (M09) + replay. Known from the banked brief: induce has ONE consumer, no
HTTP route, zero integration tests; replay deltas are blind inside Each listings.

## Current state addendum (2026-08-12 — very-thorough scout brief BANKED, slate NOT drafted:
## round-11 cap. NOT yet covered; round-12 cursor candidate. Re-verify anchors first.)
Pre-verified anchors, strongest first:
1. **Mode mutual-exclusion unenforced**: schema PROSE claims replay/induce exclusive with
   rules/urls/source; run() returns early on replay then induce (lib.rs:798,:805), and source
   silently wins over urls (lib.rs:840) — a job with several modes runs ONE, returns 200, and
   the caller believes an extraction ran. Silent-wrong-result class.
2. **Result honesty bundle**: manifest output_shape names keys no mode emits (lib.rs:782-790);
   quarantine write-redirect never echoed (result omits the dataset actually written);
   source-mode requested capped at 10k with NO truncated flag; backfill result drops health +
   worst_fields entirely; register_rules failure = warn + permanently non-replayable records.
3. **Unbounded records echo**: urls/source/archive serialize EVERY extracted record into the
   persisted job result (lib.rs:959,:1163,:1433) — 10k records into one job row; missing/
   samples are capped but records is not.
4. **Schema-vs-code bounds**: parse_concurrency clamps lower only (schema caps 64); trigger/
   schedule enqueues bypass validation ([[enqueue-door-parity]] r11 closes the door side);
   strategy "auto_with_research" dead on the documented path, live on the undocumented one;
   unknown strategy falls through to Http silently.
5. **Zero coverage**: induce end-to-end (run_induce never executed by any test), source.archive
   end-to-end, urls-mode result correctness (chokepoint e2e asserts budget/VCR only);
   summarize_reports vs diff_fields "ContainerEmpty not a miss" convention unpinned.
6. **Perf**: N+1 sequential list_filtered per URL in versions modes; sequential artifact reads
   (fetch path is buffer_unordered, disk path is serial); double record clone on write path.
7. **Docs**: replay/induce/backfill/archive/as_of/versions all absent from docs/features (only
   design docs); extraction.md documents "exactly one of urls|source" which code doesn't enforce.

## Direction history
- (as declarative-extraction-engine, round 3): 4/4 shipped — see that note. rules:"auto"
  LLM drafting REJECTED (third LLM-feature rejection) — do not re-propose.
- 2026-08-12 (round 11): scouted, brief banked, no slate (cap). No cooldown.
- 2026-08-12 (round 12, director-self-gated, SWEEP — banked anchors re-verified inline
  against live code; all held with line shifts from r11's ee5d8e4/26fb0cc/1b6aebc; NEW
  decisive fact found: schema uses `anyOf` at lib.rs:654-658 while three descriptions
  claim exclusivity, and worker.rs:1689 consumes result["records"] for search indexing —
  the echo is a load-bearing contract, not dead weight). Slate of 5, ACCEPTED 3:
  - ACCEPTED [[extractor-mode-door]] (robustness M) — silent-wrong-result: multiple mode
    roots pass the door, run() first-match-wins, 200.
  - ACCEPTED [[extractor-result-honesty]] (robustness M) — five result lies: phantom
    output_shape keys, capped source read with no truncated flag, backfill drops
    health/worst_fields, silent registration failure, write target never named.
  - ACCEPTED [[extractor-records-echo]] (optimization M) — unbounded record echo into the
    job row + double clone; must move indexing to index_datasets path first.
  - REJECTED extractor-versions-nplus1 (optimization) — real N+1 in versions/as_of modes
    but those are low-traffic archival paths with no volume consumer today; same
    precedent as r9's fetch-hot-path-batching reject. BANKED as next anchor.
  - REJECTED-deferred extractor-e2e-coverage (robustness S) — induce/source.archive e2e
    gaps are real but the three accepted directions each carry tests into these paths;
    standalone harness doesn't clear the outcome-value bar this round. BANKED.

## Shipped
- (inherited — see [[declarative-extraction-engine]])
- 2026-08-12 (r12): [[extractor-mode-door]] → `aac6dd5` (one mode per job — refused at the
  door via oneOf AND in the app via resolve_run_mode; concurrency clamped to the schema's
  64) · [[extractor-result-honesty]] → `4c9092a` (truthful output_shape pinned by
  EXPECTED-diff; sweep truncation signal + source.limit; backfill gains health +
  worst_fields via poolable QualityRollup; registration failure surfaces; every write
  mode names the dataset actually written, @q included) · [[extractor-records-echo]] →
  `742cd44` (+ Director `e9c3c32` worker guard: echo bounded 100/1000/0, index_datasets
  on all write modes gated producer-side, double-index closed, clone gone).
- 2026-08-14 (r23): [[targets-read-keys-truncated]] — banked from Lot A's `DECISION NEEDED`,
  raised rather than reached for because both apps were outside its write set. r23 made the
  host honest (`keys_truncated` is emitted, warned about and unforgeable) but **no app reads
  it yet**, so a hop exceeding `key_cap` still processes the first 200 records and reports a
  clean run. `S`, mechanical, and it closes the loop r23 opened — recommended early for r24,
  with `extractor` + `plugin` as one natural lot.
