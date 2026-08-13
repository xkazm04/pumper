---
slug: plugin-result-bounded-and-true
type: perfect/direction
context: "[[plugin-runner]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
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

(filled during build)
