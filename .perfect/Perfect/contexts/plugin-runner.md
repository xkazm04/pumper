---
name: plugin-runner
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 4
last_proposed: 2026-08-13
cooldown_until: r21
directions: ["[[plugin-run-door-honest]]", "[[plugin-result-bounded-and-true]]", "[[observatory-signal-not-noise]]"]
---

## Current state (scouted very thorough, r19, 2026-08-13)

`crates/apps/plugin/src/lib.rs` (1154) + `observatory.rs` (719). Four modes — urls /
source / backfill / observatory — sharing one door (`lib.rs:481`) and one metering path.

**The theme: this app forked away from its declared sibling `extractor` and never took the
fixes.** `records_echo` (r12), `sweep_truncated` (r12) and the `1..=64` concurrency clamp
all exist in `crates/apps/extractor/` and have **zero** occurrences here — while
`docs/features/extraction.md:100` claims the clamp is "enforced twice" and names this app.

**The r14 banked anchor is CONFIRMED and SHARPER** (Director-verified in source):
- The door is still `ctx.require_str("plugin")?` (`lib.rs:481`) — a type check, nothing
  more (`app.rs:526-531`). No `ctx.plugins.has()` anywhere in the crate.
- **DECAYED in the loop's favour**: the check is now centralized at ONE site, not the three
  the r14 note recorded (`:492-498` dispatches after it). It is a one-line fix.
- Observatory *does* validate (`observatory.rs:259-264`), as does the trigger pipeline
  (`triggers.rs:237`). The asymmetry inside one app is the fix's shape.
- **REFUTED for the dataset, CONFIRMED for three other surfaces**: `{"error": …}` records
  never reach `plugin_out` — `upsert_items` drops them (`:1000`). They *do* reach the job
  result echo, the terminal SSE event (`worker.rs:1769`), the result webhook (`:1740`), and
  **one Tantivy doc each** (`:1837-1844`), unbounded.
- The proof nothing guards this is in the test suite: `app_fetch_chokepoint.rs:185, :236`
  run the app against `NoPlugins` with `plugin: "noop"` and **pass green on a run where
  every document failed**.

Observatory's headline promise (`observatory.rs:6-7`, "change detection + triggers …
surface extraction rot for free") is **structurally false**: every row embeds `run_at`,
`avg_elapsed_ms`, fuel/memory, `drift_score`, `prev_run_at` and is written via plain
`upsert_many` (`:503`), so every row is `changed` every run and `unchanged` is always 0 —
while `lib.rs:98-101` documents that exact anti-pattern as the reason cost lives on the
result. `DerivedPaths` (shipped by r14's own `derived-change-honesty`) is unused here;
`eu-sedia` is still its only adopter.

Test coverage: **21 tests, all pure functions, all in-file. Zero tests of any `run*` path**
and no `crates/apps/plugin/tests/` directory. Untested: `run`, `run_urls_mode`,
`run_source_mode`, `run_plugin_batch`, `run_backfill`, `run_observatory`, `upsert_items`,
`plugin_rules_hash`, `versions_for`, `gather_candidates`, `BackfillState::restore`.
Verified-good (do not re-litigate): `CostRollup::to_json` returning `None` for unmetered
runs; `rules_hash: None` for undescribed plugins; cost deliberately absent from
`BackfillState`; `single_source_url` returning `None` for mixed batches; `classify_outcome`'s
typed dispatch + its anti-regression test (`:571`); `buffered()` vs `buffer_unordered()`
(ordering is load-bearing for the positional zip); `read_source_artifact` path traversal
(hardened); fetch metering (genuinely through `ctx.fetch`, pinned twice).

## Direction history

- (round 9, via [[app-runtime]]): the fetch chokepoint covered its call sites (`6237cc8`).
- r14 (2026-08-12): plugin-app run-door validation REJECTED-deferred at the
  wasm-plugin-host gate — it is THIS context's door. Banked, and cashed in r19.
- **r19 (2026-08-13), 5 drafted → 3 accepted / 2 rejected**:
  - ACCEPTED [[plugin-run-door-honest]] · [[plugin-result-bounded-and-true]] ·
    [[observatory-signal-not-noise]]
  - REJECTED outright **plugin-params-redaction** (`plugin_params` go verbatim into the
    `rules_hash` pin → `register_rules` → the `rules_versions` table, hash-addressed and
    retrievable via `rules_by_hash`; `lib.rs:47-63`). The r10 `transact-secret-redaction`
    precedent is real, but that case had **actual passwords in the flow**; here no shipped
    plugin takes a secret, and the job `params` row stores them anyway, so redacting
    `register_rules` alone would be a partial fix on a hypothetical. **Banked**: re-open the
    moment any plugin takes a credential. (Note the scout's useful negative: `/provenance/replay`
    itself refuses — it parses the blob as a `RuleSet` and plugin records carry no
    `artifact_sha` — so the exposure is `rules_by_hash` + the job row, not the endpoint.)
  - REJECTED-deferred **plugin-search-identity** (in `versions:"all"`/`as_of` mode every
    record carries `_url = <natural key>` while the dataset key is `{url}@{date}`, so
    `record_doc` mints one id per URL and N archived revisions collapse to **one** search doc,
    last-write-wins; plus ~20 000 sequential SQLite round-trips + 10 000 sequential file reads
    in the serial resolution loop `:704-770`). Interacts with `index_datasets`, which the
    accepted echo direction deliberately does not touch. **Banked as the next anchor here.**
  - Folded as riders rather than slated: the concurrency upper clamp + its false "enforced
    twice" doc claim (→ result direction); the missing `maximum` on `sample_per_site`
    (→ observatory direction, one line if convenient).
  - Explicitly banked, not slated: **URL de-duplication** (`["https://a","https://a"]`
    double-fetches, double-runs, double-pays, and reports `new: 1, unchanged: 1`; no
    `uniqueItems` at `:353-358`).

## Banked (r19 — re-verify at proposal time, seeds decay)

1. **plugin-search-identity** — the anchor. See above.
2. URL de-duplication at the door. Small, confirmed, cheap.
3. **No ceiling on plugin invocations.** `{"observatory": true}` with no other params audits
   all loaded plugins × all sites; `sample_per_site` defaults to 25 with `minimum: 1` and no
   `maximum` (`lib.rs:403-407`). A 10 000-page corpus × 8 plugins = 80 000 wasm executions
   from one no-argument job. The host's semaphore caps *parallelism*, not *count*.
4. **A trapping plugin costs the job nothing it can see** — `core/src/plugin.rs:93-95`:
   "a call that trapped propagates the error, and the fuel it burned on the way is not
   carried". A plugin that burns its full budget then traps on every page reports
   `cost: null`. **Needs a host change (`crates/engine-wasm/`) — cross-context ask.**
5. Observatory's replay loop is serial (`:391`) with serial artifact reads (`:376-385`);
   `concurrency` is not read in that file at all.
6. `ctx.health` is unused here while `extractor` uses it at `:584`, `:637`, `:648`
   (honestly documented as a gap at `docs/features/extraction.md:191`).
7. Zero-reader measurement: **`plugin/observatory` and `plugin_out` both have 0 readers**
   workspace-wide, and `catalog/data-sources.toml` has 0 hits for "plugin". Not an argument
   for inventing consumers — an argument for the intended one (a watch/trigger) to work.

## Shipped

- **r19 (2026-08-13), 3/3 + 1 Director-decided follow-up** — landed on master via
  `perfect/2026-08-13-r19`:
  - [[plugin-run-door-honest]] → `7b6d5f1` — a plugin job that cannot run **fails instead of
    reporting success**. Refused via `ctx.plugins.has()` before any fetch; `Error::BadRequest`
    (terminal) after auditing construction sites rather than widening `Error::Plugin`, which would
    have made `trap` terminal. r14's typed `PluginFailure` now survives the fan-out, so failures
    report by class instead of as one opaque string, and a plugin's own `{"error": …}` output is
    finally distinguishable from a failed call. Policy decided and documented: partial failure
    succeeds, total failure fails. The e2e fixture that *proved* the bug (green on a 100%-failure
    run against `NoPlugins`) is fixed — and the same shape was found and fixed in its extractor arm.
  - [[plugin-result-bounded-and-true]] → `210fd3e` — `records_echo` 100/1000/0 with
    `records_total` + `records_truncated`; `sweep_truncated` + `source.limit`; `parse_concurrency`
    clamped at both ends so the doc's "enforced twice" claim is true; `output_shape` and the three
    result builders finally agree. Extractor's r12 fixes **copied, not shared** — an app→app edge
    would violate the dependency rule.
  - [[observatory-signal-not-noise]] → `3258bf4` — the drift dataset stops manufacturing the drift
    it measures. Seven volatile fields declared `DerivedPaths`, so a re-run over an unchanged corpus
    reports `unchanged` and appends **no** revision, while a real behaviour change still marks the
    row `changed` (both pinned by test — the pairing is what makes deriving telemetry safe).
    Plugins are replayed with their configured params instead of `null`; empty stored artifacts are
    no longer blamed on the plugin.
  - Director-decided follow-up → `fcc4249` — `index_datasets` on every write mode, because bounding
    the echo without it silently dropped a 10 000-output run to 100 search docs. Withheld on a
    quarantined source (the worker's gate would have read `plugin_out@q` as Healthy and indexed
    degraded rows); observatory deliberately does not declare, verified harmless because
    `run_indexed_apps` always includes the job's own app.
- (via [[app-runtime]] r9)
