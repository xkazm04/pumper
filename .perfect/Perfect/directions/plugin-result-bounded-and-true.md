---
slug: plugin-result-bounded-and-true
type: perfect/direction
context: "[[plugin-runner]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 210fd3e
---

## What & why

The plugin app's job result makes four claims it cannot keep — and its sibling `extractor`
already shipped the fix for three of them, so this is largely a port with tests, not
invention.

**1. `output_shape` promises fields no mode emits.** `AppManifest.output_shape`
(`lib.rs:460-465`) declares `{ran, errors, dataset, new, changed, unchanged}`. Grep the
three result builders (`:599-609`, `:780-794`, `:971-988`): **`errors` appears in none;
`dataset` appears in none.** An agent reading `GET /apps` or the MCP tool definitions
(`mcp/mod.rs:304`) is told to expect fields that never arrive. It also costs real money
downstream: `extract_yields` (`costs.rs:220-255`) keys a `YieldEntry` on the JSON path
where it finds `new`/`changed` — for this app that is the **root**, so the cost ledger
attributes every plugin run's yield to the empty-string dataset instead of `plugin_out`.

**2. The `records` echo is unbounded.** Source mode echoes up to `SOURCE_LIST_LIMIT =
10_000` full plugin outputs into the result (`:793`); URL mode echoes one per URL with **no
`maxItems` on the `urls` schema** (`:353-358`). That blob is stored in the SQLite
`jobs.result` column, streamed on the terminal SSE event (`worker.rs:1769`), POSTed to the
result webhook (`worker.rs:1740`), and turned into **one Tantivy doc per element**
(`worker.rs:1837-1844`) because this app never emits `index_datasets` (grep: 0 hits in
`crates/apps/plugin/`). The worker's own comment (`worker.rs:1822-1826`) assumes the echo
is bounded. The extractor solved this in r12: `parse_records_echo` (`extractor/src/lib.rs:138`),
default 100 / ceiling 1000 / `0` = counts-only, with `records_truncated` + `records_total`.
`crates/apps/plugin/` has **zero** occurrences of `records_echo`.

**3. The corpus truncates at 10 000 rows and says nothing.** Source mode's no-keys sweep
calls `ctx.datasets.list(app, dataset, SOURCE_LIST_LIMIT)` (`:689`). `Datasets::list`
(`datasets.rs:1622-1633`) does **not** filter tombstones in SQL — it takes the newest 10 000
by `updated_at DESC` and the app filters `removed_at`/`gone` afterwards (`:692-695`), so
tombstones consume slots. `requested` (`:698`) is the post-filter count; the result carries
no truncation signal and there is no `limit` param. The extractor's fix has a docstring
describing this precise bug: *"reported `requested: 10000` — a number indistinguishable
from a dataset that really does hold 10,000 rows, so nothing downstream could tell a
complete run from a silently partial one"* (`extractor/src/lib.rs:150-152`, with
`sweep_truncated` at `:157` and a `source.limit` param at `:1137`).

**4. A documented "enforced twice" guarantee is enforced once.**
`docs/features/extraction.md:100` says the concurrency ceiling is *"declared once and
enforced twice — the schema's `maximum` refuses `concurrency: 65` at the door, and the code
clamps it for any caller that reaches the app another way, so the two layers cannot
disagree"* — and names this app in the same sentence. The schema declares `"maximum": 64`
(`lib.rs:387`) but `parse_concurrency` (`:88-94`) clamps only `.max(1)`. No upper clamp.
The extractor does both (`extractor/src/lib.rs:235, 252`).

## Evidence

- `crates/apps/plugin/src/lib.rs:460-465` vs `:599-609`, `:780-794`, `:971-988` — the promised keys
- `crates/core/src/costs.rs:220-255` — root-keyed yield attribution
- `crates/apps/plugin/src/lib.rs:793`, `:353-358` — unbounded echo, no `maxItems`
- `crates/server/src/worker.rs:1740`, `:1769`, `:1822-1826`, `:1837-1844` — the four consumers of the echo
- `crates/apps/extractor/src/lib.rs:138`, `:150-152`, `:157`, `:1137`, `:235`, `:252` — every fix, already written
- `crates/apps/plugin/src/lib.rs:689`, `:692-698` — the silent sweep truncation
- `crates/core/src/datasets.rs:1622-1633` — `list` does not filter tombstones in SQL
- `crates/apps/plugin/src/lib.rs:88-94`, `:387` — the clamp that isn't
- `docs/features/extraction.md:100`, `:180` — the two doc claims

## Acceptance criteria

1. `output_shape` and the three result builders agree. Every declared key is emitted by
   every mode that declares it, or the declaration is corrected. Include `dataset` so
   `extract_yields` attributes yield to the real dataset — and add a test that pins the
   attribution, since that is the part a reader cannot see.
2. The `records` echo is bounded with the extractor's contract (`records_echo` param,
   `records_total`, `records_truncated`). **Prefer lifting/sharing `parse_records_echo`
   over copying it** — but if sharing means a new dependency edge that breaks the
   "apps depend only on core" rule (README §Architecture / `CLAUDE.md`), copy it and say so.
   Apps must not depend on other apps.
3. The sweep reports truncation (`sweep_truncated` or equivalent) and takes a `source.limit`.
   A partial run is distinguishable from a complete one at the result level.
4. `parse_concurrency` clamps both ends, with a test asserting the upper clamp — the
   existing test (`lib.rs:1145-1153`) covers only the lower one. The doc claim at
   `extraction.md:100` becomes true rather than being softened.
5. `docs/features/extraction.md` and `docs/features/apps.md:13` describe what this app
   actually does (four modes exist: urls / source / backfill / observatory; `apps.md:13`
   still describes a single-mode app).
6. Tests for the two behaviors that decide what becomes data: `upsert_items` and the `ran`
   predicate (`lib.rs:592-594`, `:995-1012`) currently have **zero** tests despite being the
   crux of this direction and [[plugin-run-door-honest]].

## Risks / non-goals

- **Coordinate with [[plugin-run-door-honest]] — same builder, same files.** That direction
  changes how failures are represented; this one changes what the result *reports*. Do the
  door first if the typed-failure seam makes the counting cleaner, and say which order you
  chose.
- Non-goal: URL de-duplication (`["https://a","https://a"]` double-fetches, double-runs and
  reports `new: 1, unchanged: 1`). Real, confirmed, **banked** — do not scope-creep into it.
- Non-goal: `versions: "all"` collapsing N revisions into one search doc via `_url`
  (`worker.rs:1990-2006`). Banked separately; it interacts with `index_datasets`, so if
  your echo work makes that fix easier, *note it* rather than doing it.
- Setting `index_datasets` would change the indexing path for this app — that is a bigger
  behavioral change than bounding the echo. If you want it, argue for it; don't assume it.

## Build record

**Shipped `210fd3e`, completed by `fcc4249`. Director verdict: KEEP.** All six criteria met, one
of them by refuting the criterion.

Criterion 1: `with_outcome_fields` / `with_records_echo` are now the single definition of the
write-mode contract; `dataset` names the real write target (`<name>@q` on quarantine, pinned by
test). **But the criterion's stated *reason* was wrong — see refutation below.**
Criterion 2: `records_echo` 100 / 1000 / `0`, with `records_total` + `records_truncated`.
**Copied, not shared** — an `app-plugin → app-extractor` edge would violate README §Architecture;
the doc comment says so. Criterion 3: `parse_source_limit` + `sweep_truncated`, judged on the
store's page **before** the removed/gone filter, which is the only place the count is honest;
source mode reports `limit` + `truncated`. Criterion 4: `parse_concurrency` clamps `1..=64`, with
a test asserting the clamp and the schema `maximum` are **the same number**. Criterion 5: both
docs updated. Criterion 6: `upsert_items` and the `ran` predicate got their first tests at unit
**and** `run()` level.

**Builder refutation — my criterion 1 was wrong, and this is the useful kind of wrong.**
`YieldEntry.dataset` is the dot-joined **JSON key path** (`costs.rs:189-193`), `""` at root — not
a dataset name. Adding a root-level `"dataset": "plugin_out"` changes attribution not at all, and
`extractor`, which has emitted that field since r12, is attributed to `""` too. Re-keying would
mean nesting the summary under a dataset-named object, and `walk_yields` **keeps descending below
a match**, so the run would then report its counts **twice**. Verified in source. They emitted
`dataset` anyway — it is what a human or agent reading `GET /jobs/{id}` needs — and pinned what is
actually true: exactly one yield entry, right counts, no double-count. So this was never a
plugin-app defect; it is a property of the yield convention, and the note above was wrong to call
it one.

### Director-decided follow-up: `fcc4249` — `index_datasets`

The direction made this a non-goal ("argue for it; don't assume it"). The builder argued for it in
its report rather than guessing, and it was right: bounding the echo **without** the delegation was
the one half-fix in the wave, silently dropping a 10 000-output run from 10 000 search docs to 100.
The decisive precedent it did not have: the extractor pairs the two — `extractor/src/lib.rs:779`
calls `index_datasets` "the load-bearing one" and sets it at `:802` *because* r12 bounded its echo
— and `worker.rs:1818-1830` documents the pairing at the guard. Re-briefed with a narrow scope.

What came back was better than the ask on two counts:
- **A real bug avoided in the quarantine path.** The worker's health gate reads the health of the
  *spec's* pair, and `("plugin", "plugin_out@q")` is a pair no `observe_extraction` ever judges —
  so it always reads `Healthy` and would have waved quarantined rows into the index that saved
  searches alert from. Gated in the producer instead (`write_target` now returns the verdict
  alongside the diverted name), which is where the extractor gates it for the same reason.
- **Backfill needed it most, and that was verified rather than assumed** against
  `echo_indexing_delegated`: backfill echoes nothing, so before this its written records had **no**
  per-record search coverage at all — only one whole-result `_job` document.
- **Observatory deliberately does not declare**, with the reasoning in a doc comment, and the
  load-bearing fact checked rather than asserted: `run_indexed_apps` always includes the job's own
  app, and observatory writes under `plugin`, so **watches and dataset triggers on
  `plugin/observatory` load their revisions either way** — the intended consumer costs nothing.
  Declaring would have been a new capability, not the other half of a fix, and would dilute
  `/search` with untitled telemetry rows (the failure `apps.md` already records for
  `mpsv-ispv/wages`).

**Director answer to the builder's one open "Director call"**: the disclosed gap — that its tests
pin the *inputs* to the indexing path (spec shape, change feed) but cannot call `search_docs` /
`echo_indexing_delegated`, which are private in `worker.rs` — needs no further work. **It is
already closed from the other end**: `worker.rs:2138` asserts "echo delegated, fallback kept". App
produces the right spec (pinned app-side), worker consumes it correctly (pinned worker-side). No
`pub(crate)` widening needed.

**Banked, re-scoped rather than closed** (builder's own honest framing): the `versions:"all"`
search-identity collapse is *largely dissolved on the forward path*, because delegation replaces
the `<app>:<url>` id with `<app>:<dataset>:<key>` and the key there **is** `{url}@{date}` — but it
is untested from the indexer side, and every plugin record indexed before `fcc4249` is now an
**orphan** under `<app>:<url>` in the reserved `_records` dataset that nothing updates or deletes
until a `just search-backfill` / `just reindex`. The residue is a verification test plus that
migration, and it applies to all modes, not just `versions:"all"`.

Gates: `cargo test -p app-plugin` 63/0 (42 lib + 4 observatory + 8 result_contract + 9 run_door).
Live smoke pins the door refusal and the declared bounds (checks 37 and 38).
