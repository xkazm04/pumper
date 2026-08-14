---
name: connector-api-watch
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 4
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[connector-watch-blank-predicate]]", "[[connector-watch-failures-are-not-success]]"]
---

## Current state
**Scouted 2026-08-14 (round 22), in the six-context "thin app + its catalog row" family brief —
COVERED.** 653 lines, 13 tests (10 in-file + 3 in `tests/adoption.rs`) — **the best-covered app in
its family**, and the coverage still misses both of its honesty holes.

- **Two real defects, one direction:** a failed upsert read as `Unchanged` and then cemented by the
  checkpoint (`:278-295`, `:341-342`), and an all-connectors-failed run returning `Ok`
  (`:251-257`, `:345-354`) on a **monthly** cadence. → [[connector-watch-failures-are-not-success]]
- **Catalog-vs-code drift, three items:** `engine = "http"` on the only `CostClass::Claude` app in
  the family (the vocabulary has a `claude` value); `max_row_delta_pct = 20.0` is **structurally
  inert** (upsert-only app; `removed` comes solely from `sync_many`) — the same inertness already
  deleted from grants-gov's row; `output_shape` omits `resumed_from_checkpoint`, which the run
  emits. Cron, dataset and `required_fields` all check out.
- **The most interesting finding is not in this app at all.** `catalog/data-sources.toml:789-792`
  states that `max_staleness_hours` was deliberately omitted because "a quiet month is normal and a
  staleness floor would fire false alarms" — but `cadence = "monthly"` alone makes
  `/catalog/health` monitor it at 62 days off `updated_at`, which moves only on a real change. **The
  documented intent is inexpressible in today's catalog fields**, so this is a
  [[data-pipeline-catalog]] defect surfaced through this app, and the two should be built together.
- Bounds: the sweep is unbounded by default (`limit: 0` = all, `:186`), the **whole document
  markdown is stored in the record** (`:272`) with no byte cap so every revision keeps a full copy,
  and the checkpoint blob is re-serialized in full after every connector (`:342`) — O(n²) writes.
  The *prompt* is properly capped (`MAX_DIFF_LINES` 200 / `MAX_DIFF_CHARS` 6000). Banked.
- Prior state: files: crates/apps/connector-api-watch/src/lib.rs.
JSON-API watch connector (old-map "Extraction, Crawl & API Watch" shipped the watch app
w1; this connector variant unswept). Likely shares seams with page-monitor.

## Direction history
- **2026-08-14 (round 22): PROPOSED — 1 direction, REJECTED on the 6-direction cap.**
  [[connector-watch-failures-are-not-success]] is real and cheap; it lost its slot to three
  directions with strictly larger blast radius (a partial parse that **tombstones live rows**, an
  alerting app that **fires false alerts at users**, a paginator that **caps a state corpus at one
  page while reporting completeness**). This app's failure mode is invisibility-of-a-failure — one
  step less severe than data loss or a false positive delivered to a human. Banked for r23 as a
  joint pass with [[data-pipeline-catalog]] (the `max_staleness_hours` expressiveness gap is the
  more interesting half, and it is a catalog defect, not an app defect).

## Shipped
- (none on this map)
- 2026-08-14 (r23): [[connector-watch-blank-predicate]] — **REFUTED on both halves.** r22
  banked "a third copy of `extracted_nothing`, one-line change to the core function". There is
  no third *copy* — `readable` and `watch` both import the core fn; connector-api-watch has an
  inline `markdown.trim().is_empty()` (`:260`), and `grep extracted_nothing` in that crate
  returns 0 hits. And the core fn needs **no change at all**; the work is two lines in the app.
  Below the bar (zero behavior change, zero user-visible symptom) — worth doing only as a rider
  on other work in that file. Note the handling divergence is deliberate and must be preserved:
  readable/watch fail the job, connector-api-watch skips one connector and continues.
