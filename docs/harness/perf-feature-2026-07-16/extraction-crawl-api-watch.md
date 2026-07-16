# Extraction, Crawl & API Watch — perf-optimizer + feature-scout scan

> Total: 3
> Critical: 0 | High: 1 | Medium: 2 | Low: 0

## 1. Bound the fetch fan-out in extractor urls-mode and plugin

- **Severity**: High
- **Lens**: perf-optimizer
- **Category**: unbounded-concurrency
- **File**: crates/apps/extractor/src/lib.rs:236-251, crates/apps/plugin/src/lib.rs:50-69
- **Scenario**: An API user posts `{"urls": [...5000 product URLs across 800 hosts...], "rules": {...}}` to the `extractor` app (or the same list to `plugin`). Both apps build one future per URL and drive them all with a single `futures::future::join_all` — every URL is in flight at once, with no cap.
- **Root cause**: The only backpressure on the HTTP tier is `Governor::acquire(host)` (`crates/core/src/governor.rs:89`), which is explicitly a **per-host** token-bucket-of-capacity-one — it serializes requests *to the same host* but places no global limit on concurrent fetches. The browser tier has a real cap (`max_concurrent_renders` semaphore, `crates/engine-browser/src/lib.rs:107-114`), so `strategy: "browser"` degrades to 5000 parked futures rather than 5000 renders; the default `FetchStrategy::Http` path has no such guard. Notably the sibling `crawl` app *does* expose this knob — `CrawlConfig.concurrency`, default 16 (`crates/apps/crawl/src/lib.rs:184`) — so the asymmetry is an omission in the two URL-list apps, not a deliberate design.
- **Impact**: With N distinct hosts, N simultaneous sockets + TLS handshakes + N in-flight response buffers. At 5000 URLs this is thousands of concurrent connections from one job — enough to exhaust file descriptors, and it holds every response body resident at peak (compounding finding #2). Same-host lists are self-limiting via the governor but still allocate N live futures. A single API call can degrade every other concurrent job on the box.
- **Fix sketch**: Add a `concurrency` param (default 16, matching `CrawlConfig`) to both apps and replace `join_all` with `futures::stream::iter(...).buffer_unordered(concurrency).collect().await`. Both call sites already produce an independent future per URL that resolves to a `(url, Option<String>)` / `Value`, so the change is mechanical and preserves result ordering semantics in `plugin` (which `zip`s `urls` against `results`) only if `buffered` is used there instead of `buffer_unordered` — or better, have the plugin future return `(url, Value)` and drop the positional `zip` at lib.rs:72-75.

## 2. Stop cloning every document and DocReport in extract_and_upsert

- **Severity**: Medium
- **Lens**: perf-optimizer
- **Category**: allocation
- **File**: crates/apps/extractor/src/lib.rs:89-106
- **Scenario**: Any `extractor` run over a meaningful batch — e.g. source mode over the crawl's `pages` dataset, which defaults to all live records capped at `SOURCE_LIST_LIMIT` = 10,000 (lib.rs:19, 336-345).
- **Root cause**: Three avoidable copies of document-sized data on the shared tail path:
  1. `let docs: Vec<String> = keyed.iter().map(|(_, d)| d.clone()).collect();` (line 89) — deep-clones **every HTML body** purely to split keys from docs, while `keyed` is owned and dropped right after.
  2. `summarize_reports(&reported.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>())` (lines 91-92) — clones every `DocReport` into a throwaway `Vec` just to hand `summarize_reports` a slice, even though the function only reads `report.fields`.
  3. `records.push(rec.clone())` (line 103) — clones each extracted record so it can be both returned in `records` and upserted in `items`.
- **Impact**: (1) is the big one: it doubles peak RSS over the whole batch. 10,000 crawled pages averaging 200 KB ≈ 2 GB resident becomes ≈ 4 GB, right before `extract_batch_with_report` fans out across every core. (2) is pure waste proportional to fields × docs. (3) is smaller (extracted records, not bodies) but on the same hot path.
- **Fix sketch**: (1) `let (keys, docs): (Vec<String>, Vec<String>) = keyed.into_iter().unzip();` — zero copies — then `zip` `keys` back against `reported`. (2) Change `summarize_reports` to take `&[(Value, DocReport)]` (or an `impl Iterator<Item = &DocReport>`) and call it as `summarize_reports(&reported)`; its body only iterates `report.fields`, and the existing unit tests at lib.rs:386-422 adapt with a one-line map. (3) Leave as-is or return `items` and let the caller read `.1` — lowest priority of the three.

## 3. Give `plugin` the extractor's `source` mode so WASM plugins run over stored crawl bodies

- **Severity**: Medium
- **Lens**: feature-scout
- **Category**: feature-gap
- **File**: crates/apps/plugin/src/lib.rs:24-45, crates/apps/extractor/src/lib.rs:133-165
- **Scenario**: A user crawls a site (`crawl` streams every kept page body to `data/artifacts/crawl/<job_id>/page-NNNN.html` and fingerprints it into the `pages` dataset), then wants to run a custom `.wasm` extractor over those pages. Today the only way is `{"plugin": "x", "urls": [...every page URL...]}` — which **re-fetches the entire site** the crawl just downloaded.
- **Root cause**: The crawl→extract seam was built for `extractor` only. `extractor` gained `source: {app, dataset, keys?}` with `read_source_body` resolving `artifact_path` + `job_id` against the shared artifacts root (extractor lib.rs:133-165), documented as the crawl→extract seam in `docs/features/extraction.md:39`. `plugin` never got the parallel treatment: it reads only `urls` (lib.rs:26-34) and hard-errors on an empty list. `read_source_body` and `safe_segment` are private to the `extractor` crate, so there was no cheap path to share them. `plugin` is also missing `FetchStrategy::AutoWithResearch`, which `extractor` accepts (extractor lib.rs:232 vs plugin lib.rs:35-39) — the same parity drift.
- **Impact**: Running a plugin over an already-crawled 1,000-page site costs 1,000 redundant HTTP fetches — full re-download, full governor politeness delay (minutes to hours at default per-host spacing), and real money if `strategy` escalates to the browser or Claude tier. It also makes plugin results a *different point in time* than the crawl's `pages` records they logically annotate. Reactive `plugin` pipelines are effectively blocked: a dataset trigger hands `_trigger.keys`, which `extractor` honours (lib.rs:313-314) and `plugin` cannot read at all.
- **Fix sketch**: Promote `safe_segment` + `read_source_body` from `crates/apps/extractor/src/lib.rs` into `pumper_core` (they already take `&AppContext` and `&Record`, so the move is clean and lets both apps share one hardened path-traversal guard rather than forking it). Then mirror `run_source_mode`'s key-precedence ladder in `plugin` — explicit `source.keys` → `ctx.params.pointer("/_trigger/keys")` → live records capped at `SOURCE_LIST_LIMIT` — feeding bodies into the existing `p.run(&name, &doc)` fan-out in place of the fetch. Report `missing`/`missing_keys` the same way. Add `"auto_with_research"` to the plugin `strategy` match while touching it. Then update `docs/features/extraction.md` (the WASM plugin section at :66-68) and `apps.md`.
