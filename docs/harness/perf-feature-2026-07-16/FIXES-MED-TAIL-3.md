# Perf-Feature Scan — Medium Tail, Batch 3: content-extraction & research path

> 5 commits, 5 Medium findings closed (crawl→extract seam, research/readable cost,
> memory footprint).
> Baseline preserved: build clean, tests **227 → 230** (0 regressions).
> Branch `vibeman/tail-2026-07-17` (off master after PR #5).

## Commits

| # | Commit | Finding | What |
|---|---|---|---|
| 1 | `91c19b2` | czech #2 | drop the parsed feed after aggregation, not after the network phase |
| 2 | `c8b8fad` | web-research #3 | readable stores the document once (artifact), not twice |
| 3 | `4997b24` | web-research #2 | research salvages a fenced/prose-wrapped report (helper → core) |
| 4 | `acc51c4` | declarative-extraction #3 | scoped HTML→Markdown via `css html:true` + `to_markdown` |
| 5 | `d27c016` | extraction-crawl-api-watch #3 | plugin `source` mode over stored crawl bodies |

## What was fixed

1. **mpsv-vpm memory.** `drop(resp)` freed the 188 MB source string, but the typed
   `feed` (~300k Postings, 100–200 MB) stayed resident through the ISPV list and the
   ARES enrichment phase (up to 50 sequential governed fetches, minutes) — held
   across I/O while other apps run. Everything downstream uses the small derived
   collections, so an explicit `drop(feed)` after the aggregation loop cuts
   steady-state footprint to the aggregates.

2. **readable double-store.** The extracted Markdown was written to the `page.md`
   artifact *and* inlined into `jobs.result` (SQLite) — a 200 KB article made a
   200 KB job row. Now the result is compact by default (artifact pointer +
   `markdown_chars`); an interactive caller opts into inline with `inline: true`. Also
   `take()`s the string instead of double-cloning.

3. **research salvage.** A fenced/prose-wrapped report degraded to
   `structured: false` and a text blob, forcing an expensive re-run. Promoted
   `salvage_json` (the recovery the trades apps already use) to
   `pumper_core::json_salvage` (avoiding a backwards research→trades dep); research
   now salvages before giving up, `structured` still gated on the promised shape.

4. **scoped markdown rule.** Clean Markdown of one element was unreachable from the
   rule engine (`el.text()` fused headings/lists/tables; whole-page markdown kept
   chrome). Added `markdown::html_fragment_to_markdown`, a `css` `html: true` flag
   (yields `el.html()`), and a `to_markdown` transform — composing element-wise over
   `all: true`.

5. **plugin source mode.** Running a plugin over an already-crawled site re-fetched
   the whole site (1000 redundant fetches, governor delay, real money) because the
   crawl→extract seam was extractor-only, and reactive plugin pipelines were blocked
   (`_trigger.keys` unreadable). Promoted the read path to
   `AppContext::read_source_artifact` (one shared, hardened path-traversal guard —
   extractor's private copy removed), and gave `plugin` the same `source` mode +
   key-precedence ladder + `auto_with_research` parity.

## Verification

| Gate | Before | After |
|---|---|---|
| `cargo build --workspace` | clean | clean |
| `cargo test --workspace` | 227 | 230 |

New tests: `json_salvage` (bare/fenced/prose/brace-in-string, none-cases);
`css html:true + to_markdown` preserves structure. Extractor's `source_mode`
integration test passes unchanged against the moved `read_source_artifact` method.

## Patterns established (catalogue additions)

22. **Free the big parse when its readers are done, not at scope end.** A ~150 MB
    typed corpus held across a minutes-long network phase (while other apps run) is
    pure residency; `drop` it after the last read.
23. **Store big payloads once.** An app that both writes an artifact and inlines the
    same content into the result row doubles storage and bloats listings; keep the
    result compact, gate inline behind a param.
24. **A generic salvage/parse helper belongs in core, not in a domain crate.**
    `salvage_json` lived in `trades-common`; sharing it would have meant a backwards
    dep. Move to core beside the type it serves.
25. **One hardened guard, shared.** A path-traversal check duplicated per app drifts;
    promote it to a single `AppContext` method both callers use.

## Remaining Medium/Low tail (open, ~6)

app-registry #2/#3(Low), broad-crawler #3 (sitemap `<lastmod>`), config-catalog
#2/#3, census #3 (employer wage benchmark), trades #3 (vintage keys). Plus the larger
open **Highs** the themed waves didn't reach and the two deliberate deferrals (crawl
delta-journal; grants money-enrichment).
