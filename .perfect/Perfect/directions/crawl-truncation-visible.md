---
slug: crawl-truncation-visible
type: perfect/direction
context: "[[web-crawler]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-13
commit: 3151f35
---
## What & why
**A truncated crawl is byte-identical to a complete one in the API response**, and the
feature doc promises otherwise.

Core computes two truncation counters honestly and says in its own doc comments that they
exist so a capped crawl is "reported honestly rather than silently dropping discovered URLs":
`frontier_dropped` (URLs refused at the 100k frontier cap) and `skipped_host_budget` (URLs
dumped when a host hit `max_pages_per_host`). The app's result builder emits thirty keys and
**neither of these**. `frontier_dropped` has no reader anywhere in the workspace — dead
honest-accounting, computed and tested in core, surfaced by nobody.

Meanwhile `docs/features/crawling.md:60` lists `skipped_host_budget` as a returned result
stat. It is not returned. That is a documented field that does not exist.

Two more result-honesty defects live in the same builder function and ship with it rather
than as separate directions:
- **`edges_written` over-reports.** The upsert's `UpsertSummary` is discarded and
  `edge_rows.len()` is added on `Ok(_)`, so unchanged/no-op rows count as writes — while
  `pages` two hundred lines earlier does it correctly with `summary.new/changed/unchanged`.
- **The manifest lies about its own output.** `output_shape` on `GET /apps` advertises
  `{pages, new, changed, unchanged, skipped, hosts, …}`; the result has no `pages` key and no
  `skipped` key. It also omits every field added by the last four milestones
  (`skipped_not_due`, `cadence_updates`, `versions_archived`, `reliability_hosts`,
  `edges_*`, `top_linked`).

The user moment: *"My crawl returned 50,000 pages and said nothing else. I had no way to
learn that it discarded 12,000 discovered URLs at the frontier cap and dumped another host's
entire backlog — so I treated a partial corpus as the whole site."*

## Evidence
- Core computes + documents them as honesty fields: `crates/core/src/crawl.rs:467-473`
  (fields + doc), `:558-560` (`dropped += 1`), `:598-604` (`skipped_host_budget += q.len()`),
  `:1128-1129` (assigned into `CrawlStats`). Tested in core at `:2346`.
- App result builder, every key read by the scout: `crates/apps/crawl/src/lib.rs:841-890` —
  `frontier_remaining` is emitted (`:857`), the two counters are not.
- Doc claims: `docs/features/crawling.md:60` (result stats list) and `:11` ("counted in
  `skipped_host_budget` (honest truncation, like `frontier_dropped`)").
- `edges_written`: `crates/apps/crawl/src/lib.rs:478-482` (discards the summary) vs the
  correct `pages` handling at `:417-426`.
- `output_shape` mismatch: manifest `:638-644` vs result `:841-890`.
- Doc also wrong on params (`:7` lists a `checkpoint` param that no longer exists — the app
  says so itself at `lib.rs:711-716`; omits `revisit_budget` + `min_due_score`, real and
  schema'd at `:722-733`/`:609-619`) and on the checkpoint mechanism (`:16` says "every 25
  kept pages" and a named artifact file; it is a 5s wall clock into the DB checkpoints table).

## Acceptance criteria
1. The result surfaces `frontier_dropped` and `skipped_host_budget`. Both already exist on
   `CrawlStats` — this is a pure app-side change, **no core edit** (a sibling builder owns
   `crates/core/src/crawl.rs` this wave).
2. Truncation is not just present but *legible*: a caller can tell "this crawl saw the whole
   discovered graph" from "this crawl was cut short", without comparing two numbers whose
   relationship is undocumented. Builder's judgment on the shape (a boolean/flag beside the
   counters is fine); a `warnings` entry when either is non-zero follows the fleet's existing
   idiom (cordis `aggregate_truncated`, r15 census `blend_complete`).
3. `edges_written` reports what the store actually did, from the `UpsertSummary`, exactly as
   the `pages` write does. Test.
4. `output_shape` matches the result the code emits — including the fields added by the last
   four milestones. An inventory-style test that fails when the two drift is strongly
   preferred over a one-time correction (this is the repo's EXPECTED-diff idiom; the manifest
   has now drifted through four milestones unnoticed).
5. `docs/features/crawling.md` corrected in the same session for every falsehood named in the
   Evidence block above: result stats, the param list, and the checkpoint mechanism. This
   direction OWNS that file for the wave — do not expect a sibling to touch it.
6. Proven by the first `run()`-level test in this app (see risks): a new
   `crates/apps/crawl/tests/` file that drives `Crawl::run()` against a `TempStore` with dead
   engines and asserts the result keys. Today nothing anywhere constructs the app and calls
   `run()`, which is the structural reason a documented field could go missing unnoticed.

## Risks / non-goals
- `run()` currently has **zero** coverage anywhere in the workspace (no `tests/` dir, no e2e,
  only `registry.rs:35` constructs the type). If a full `run()` drive proves infeasible
  without live fetches, report that honestly and pin what you can at the result-builder
  level — but say precisely what you could not verify.
- No new datasets, no schema changes, no core edits.

## Build record
(to fill during build)
