---
slug: crawl-memory-bounded
type: perfect/direction
context: "[[web-crawler]]"
lens: optimization
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---
## What & why
"Bounded memory" is one of this crawler's two headline architectural promises — page bodies
stream to disk, per-page metadata streams to the dataset, and the docs state that **only**
the frontier seen-set (capped at 100k) and the 8-byte SimHash fingerprints grow with the
crawl. The app layer breaks that promise with two unbounded structures, and the promise is
what makes a 100k-page crawl a supported operation rather than a gamble.

- **`EdgeGraph` grows without any cap for the whole run.** Its within-run dedup set is keyed
  `{from_url}|{to_url}` — two full URLs per entry — and its in-degree map holds one entry per
  distinct target URL. The only bound anywhere near it is the *per-page* `OUT_DEGREE_CAP`,
  which caps a page's contribution, not the total. At 200 edges per kept page across a crawl
  that the frontier alone permits to reach 100k pages, that is on the order of tens of
  millions of retained keys holding hundreds of millions of bytes of URL text — beside a
  frontier deliberately capped at 100k.
- **`SeedData` holds up to 10,000 full page records** for the entire run in revisit mode.

Both are in the app; core's own comment about bounded structures is true *of core*. The false
statement is the product-level one in the feature doc.

This is not a hypothetical: the frontier cap, the streaming sink, and `OUT_DEGREE_CAP` all
exist because someone already decided this crawler must survive a large crawl. These two
structures are the remaining holes in that decision.

The user moment: *"The crawler is documented as bounded-memory, so I pointed it at a large
site overnight and it ballooned until the machine started swapping."*

## Evidence
- `crates/apps/crawl/src/link_graph.rs:42-53` — `seen: HashSet<String>` (keys built at
  `:81-82`), `in_degree: HashMap<String, u64>` (`:91`). Only bound in the file is the
  per-page `OUT_DEGREE_CAP = 200` (`:33`, enforced `:77-80`).
- Lifetime: constructed `crates/apps/crawl/src/lib.rs:745`, read at `:833-836` — the whole run.
- `SeedData`: `crates/apps/crawl/src/lib.rs:148`, filled at `:536`, `REVISIT_SEED_LIMIT`
  = 10_000.
- The promise being broken: `docs/features/crawling.md:18` ("only the frontier seen-set
  (capped at 100k) and the kept-page SimHash fingerprints (8 bytes each) grow with the
  crawl"). The same wording in `crates/core/src/crawl.rs:10-15` is true of core and is NOT
  yours to edit this wave.
- Precedent for the shape of the fix: the frontier's own `MAX_FRONTIER` + `dropped` counter
  (`crates/core/src/crawl.rs:31`, `:523-526`) — cap, count, report. `link_graph.rs:10-16`
  already documents honest accounting for its two existing skip classes
  (`dropped_out_degree`, `deduped`), so the idiom is in the file.

## Acceptance criteria
1. `EdgeGraph`'s growth is bounded by an explicit, named cap in the same style as
   `MAX_FRONTIER` — chosen with a stated rationale about the memory it implies, not a round
   number pulled from the air.
2. Hitting the cap is **counted and surfaced**, never silent: a new skip class beside the
   existing `dropped_out_degree` / `deduped` tallies, reaching the run result. The file's own
   header already commits to this standard.
3. Degradation at the cap is a deliberate, documented choice (stop accepting new edges? stop
   in-degree tracking but keep emitting edges? evict?) — record the reasoning. `top_linked`
   remains meaningful, or the result says it is now partial.
4. The revisit `SeedData` retention is addressed or explicitly justified with numbers — if
   10k records is genuinely small enough to hold, say so with the measurement and leave it.
   An unexamined "it's fine" is not acceptable; this direction exists because of an
   unexamined "it's fine".
5. `docs/features/crawling.md:18` tells the truth about what grows with a crawl. Coordinate
   through the Director if [[crawl-truncation-visible]] has that file open — it owns the doc
   for this wave, so **report your doc delta in your final report** rather than editing it.
6. Tests: the cap engages, the counter reports, and normal-sized crawls are unaffected.

## Risks / non-goals
- No change to `OUT_DEGREE_CAP` semantics or to the `edges` dataset shape.
- Do NOT edit `crates/core/src/crawl.rs` (sibling builder owns it) or
  `docs/features/crawling.md` (sibling direction owns it).
- Not a perf optimization of the crawl loop — this is about a bound, not about speed.

## Build record
(to fill during build)
