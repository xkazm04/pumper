---
name: declarative-extractor
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 6
last_proposed: never
cooldown_until: —
directions: []
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

## Shipped
- (inherited — see [[declarative-extraction-engine]])
