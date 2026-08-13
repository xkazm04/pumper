---
slug: observatory-signal-not-noise
type: perfect/direction
context: "[[plugin-runner]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 3258bf4
---

## What & why

The observatory's headline promise is structurally impossible as built, and the same file
that documents the anti-pattern commits it.

`observatory.rs:1-7` sells the feature as: *"change detection + triggers on that dataset
surface extraction rot for free."* But every row it writes embeds `run_at` (`:465`, from
`Utc::now()` at `:361`), `avg_elapsed_ms` (`:485`), `avg_fuel_used` / `max_fuel_used` /
`max_memory_bytes` (`:491-494`), `drift_score` and `prev_run_at` (`:495`, `:497`) — and it
writes them through plain `ctx.upsert_many` (`:503`), i.e. `DerivedPaths::NONE`. Change
detection hashes the whole canonical value (`datasets.rs:215-217`). **Therefore every row is
`changed` on every run**: `summary.unchanged` is structurally always 0, a watch on
`plugin/observatory` fires on 100% of rows every run, and the `drift_score` signal the
feature exists to raise is buried in universal noise.

The irony is exact and local: `lib.rs:98-101` documents this precise anti-pattern — *"a
per-record `fuel_used` would make change detection mark every single record `changed` on
every re-run — the telemetry would destroy the very signal"* — as the stated reason cost
lives on the job result instead of on records. Observatory does it anyway. And the tool to
fix it already shipped: `DerivedPaths` + `upsert_many_with_derived` (`app.rs:598`) was built
by round 14's own `derived-change-honesty` direction for exactly this problem, and
`eu-sedia` (`:272`, `:348`) is still its only adopter.

Two more defects make the audit's verdicts untrustworthy:

**Every plugin is replayed with `params: null`.** `observatory.rs:396` calls
`run_metered(plugin, body, &Value::Null)`; there is no `plugin_params` support anywhere in
the file (grep: 0 hits). But `plugin_params` is the app's flagship feature — "one plugin be
configured per job … instead of recompiling a module per variation" (`lib.rs:27-29`), and
the reference plugin `title-extractor` reads `params.tag`. So a plugin that only produces
output under a configuration is classified `Empty` or `SchemaInvalid` at every site,
forever. Because the rate never *rises*, `empty_rate_rising` (`:134`) never flags it, and
`drift_score` compares two meaningless distributions — while the row reads
`low_confidence: false` and looks authoritative.

**Empty stored artifacts are blamed on the plugin.** `:381-383` pushes `String::new()` for
an artifact that reads fine but is empty; `:393-394` then short-circuits to
`Ok(Value::Null)` **without calling the plugin**; `classify_outcome` buckets it `Empty`
(`:80-84`). A crawl that stored zero-byte bodies therefore inflates the site's empty rate,
can trip `empty_rate_rising`, and inflates `drift_score` — a false positive on the exact
canary this feature exists to raise, attributed to the plugin rather than the corpus. Those
pages also count in `pages_replayed` (`:418`) though nothing was replayed. (`unreadable` is
tracked separately at `:384` and reported at `:466` — the author's intent is clear; empty
just fell through the wrong side.)

## Evidence

- `crates/apps/plugin/src/observatory.rs:1-7` — the promise
- `crates/apps/plugin/src/observatory.rs:465, 485, 491-497, 503` — the volatile fields and the `DerivedPaths::NONE` write
- `crates/core/src/datasets.rs:215-217` — change detection hashes the whole value
- `crates/apps/plugin/src/lib.rs:98-101` — the same anti-pattern, documented as a reason not to do it
- `crates/core/src/app.rs:598` — `upsert_many_with_derived`, shipped r14, one adopter
- `crates/apps/eu-sedia/src/lib.rs:272, 348` — the adoption pattern to copy
- `crates/apps/plugin/src/observatory.rs:396` — `&Value::Null` params
- `crates/apps/plugin/src/lib.rs:27-29` — `plugin_params` as the flagship feature
- `crates/apps/plugin/src/observatory.rs:381-384, 393-394, 418, 80-84` — empty-artifact misattribution
- Zero-reader measurement: workspace grep for the `plugin/observatory` dataset → writers = this file only, **readers = 0**; `catalog/data-sources.toml` has 0 hits for "plugin"

## Acceptance criteria

1. A re-run over an unchanged corpus with unchanged plugin behavior produces
   `unchanged > 0` — ideally all rows. Take the volatile fields out of the change identity
   via `DerivedPaths` (`/run_at`, `/avg_elapsed_ms`, `/avg_fuel_used`, `/max_fuel_used`,
   `/max_memory_bytes`, `/prev_run_at`, and consider `/drift_score` — argue it either way).
   Follow `eu-sedia`'s shape, including its `derived_paths()` + equality test.
2. A test proves it: two runs, same corpus, second run reports the rows unchanged. This is
   the criterion — do not accept "the fields are marked derived" as evidence on its own.
3. Observatory replays each plugin with the params it is actually configured with. Let
   `observatory.plugins` carry params (or read a default from the plugin's `describe()`
   manifest), and make the row key distinguish configurations so two configs of one plugin
   don't overwrite each other.
4. An empty stored artifact is no longer counted as a plugin `Empty`. Give it its own
   bucket or fold it into `unreadable`; exclude it from `classified`, `rates` and
   `pages_replayed`. Report the count so a rotting *corpus* is visible as a corpus problem.
5. `observatory.rs:1-7`'s promise is true as written, or the prose is corrected to match
   what the code does. Same for `docs/features/extraction.md:176`.

## Risks / non-goals

- **Do not touch `classify_outcome` (`:66-87`) or its anti-regression test (`:571`).** That
  is round 14's typed-failure fix landing correctly and is one of the better tests in the
  repo. Adding a bucket is fine; re-plumbing the classifier is not.
- The corpus reads are capped at 10 000 rows **globally across all sites** (`:307`, `:327`)
  while each row reports `total_pages: total` as if complete. Real and confirmed — but it is
  [[plugin-result-bounded-and-true]]'s truncation criterion. If a shared helper falls out,
  good; do not fix it twice in two commits.
- The replay loop is serial (`:391`) with serial artifact reads (`:376-385`) and
  `concurrency` is not read in this file at all. **Non-goal** — correctness first; note it
  for a later round. Likewise the missing `maximum` on `sample_per_site` (`lib.rs:403-407`):
  if you can add the schema bound in one line while you are there, do; do not build a
  replay-ceiling mechanism.
- This dataset has **zero readers today**. That is an argument for making its signal real
  (a watch/trigger on it is the intended consumer and is exactly what defect #1 breaks) —
  not an argument for building new consumers. Do not invent one.

## Build record

**Shipped `3258bf4`. Director verdict: KEEP.** All five criteria met.

Criteria 1–2: seven volatile fields declared via `derived_paths()` and written through
`upsert_many_with_derived`, following eu-sedia's shape **including its declaration-equality test**.
The criterion was behavioral and was met behaviorally:
`a_rerun_over_an_unchanged_corpus_reports_unchanged_not_every_row_changed` proves run 2 reports
`unchanged: 1, changed: 0` **and appends no second revision** — paired with
`a_real_behaviour_change_still_marks_the_row_changed` (a trapping plugin → `changed: 1`,
`drift_score > 0.9`). That pairing is what makes deriving telemetry *safe* rather than merely
quiet, and it was the builder's addition, not the criterion's.

Criterion 3: `AuditedPlugin` + `parse_audited_plugins` — an entry may be `"name"` (inheriting
job-level `plugin_params`) or `{name, params}`. `row_key` gives a configured replay
`plugin@<fp>|site` while an unconfigured one keeps the historic `plugin|site` key, so no orphaned
rows on deploy. Test: two configurations → two rows, and the bare host's params are all null.

Criterion 4: `classify_page` → `PageSource::{Replayable, Empty, Unreadable}`; empty stored
artifacts become `empty_artifacts`/`pages_empty` and are excluded from `classified`, `rates` and
`pages_replayed`. `classify_outcome` and its anti-regression test untouched, as instructed.
Criterion 5: the module prose and `extraction.md` now describe what the code does. Rider taken:
`sample_per_site` gains `maximum: 500` plus a matching code clamp.

**Two builder refutations of this note's criteria, both load-bearing and both Director-verified:**
1. **`DerivedPaths` takes dot-separated names, not JSON pointers.** Criterion 1 specified
   `/run_at`, `/avg_elapsed_ms`, …; `DerivedPaths::new` filters empty strings and resolves
   `.`-separated names through objects (`datasets.rs:3757-3768`), so a leading `/` is a **silent
   no-op**. The seam would have looked adopted and done nothing — precisely the failure class this
   direction existed to kill.
2. **`drift_score` must be derived, not "argued either way".** With it in the identity criterion 2
   *cannot* pass: run 1 writes `null`, run 2 writes `0.0`. The builder hit this as a real red test.
   Safe to derive because it is a pure function of `rates`, which stays in the identity.

Gates: `cargo test -p app-plugin --test observatory` 4/0.

**Builder-disclosed limits**: no wasm was executed (every test uses an in-process `StubPlugins`;
`run_metered` falls through to the unmetered default, so the fuel/memory derived paths are
exercised as `null` rather than as moving numbers — the real host is only reachable via
`--ignored` tests with built plugins). Existing live `plugin/observatory` rows will report
`changed` once on the first run after deploy (the documented one-time re-hash), and a plugin that
was audited bare and is later given params appears under a **new** key rather than migrating —
judged acceptable since the dataset has zero readers, but it is not migration-tested.
