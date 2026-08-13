# Full-text search & saved searches

Embedded Tantivy index (no external service), BM25-ranked over `title` + `body`. The worker indexes every successful job's result (elements of `records`/`stories`/`items` arrays, else the whole result) — id-keyed upserts.

### Job-result documents: reserved namespaces and their lifecycle

Documents minted from a job *result* are not stored dataset records, and they say so: they carry a **reserved dataset name**, never the app name (which previously advertised a dataset that does not exist in the store through `/search` facets and `?dataset=`).

| doc | id | `dataset` | lifecycle |
| --- | --- | --- | --- |
| array element with a url | `<app>:<url>` | `_records` | upserts on the url; accumulates across runs as a durable corpus |
| array element with no url | `<app>:<job_id>:<i>` | `_job` | **latest run per app** — swept |
| whole result (no arrays) | `<app>:<job_id>` | `_job` | **latest run per app** — swept |

`_job` ids embed the job id, so every run mints a fresh set that no upsert or delete would ever reclaim — the index grew with the number of runs forever. Before indexing a run that mints any `_job` doc, the worker sweeps the app's previous snapshot (`delete_dataset(app, "_job")`, issued *before* the adds so the new docs survive it). A run whose records are all url-keyed skips the sweep — nothing accumulated, and the sweep commits. Saved-search alerting is unaffected: `_job` ids are still unique per run, so `saved_search_seen` never re-alerts a swept doc (its claim row simply stops matching anything).

Query consequence: `?dataset=_job` / `?dataset=_records` scope to job-result documents, and `?dataset=<app>` no longer matches them.

### Indexing a dataset from a compact result (`index_datasets`)

An app whose result stays compact (counts, not arrays — the fleet convention) can still emit one search document per stored record by adding `"index_datasets": [{ "app", "dataset" }]` to its result. After indexing the result itself, the worker reads each named dataset's **revisions since the job started** (the change feed) and indexes only the records this run actually touched — `new`/`changed` keys are indexed from their revision snapshot, `removed` keys are deleted from the index. Doc id `"<app>:<dataset>:<key>"` — stable, so re-runs replace rather than duplicate; a key changed twice in one run is written once, from its final state. This is **delta-driven, cost O(changes) not O(corpus)**: the earlier version re-read and re-indexed the whole named dataset on every job completion. The trade-off is that a wiped index is refilled only as rows change — see [Maintenance](#maintenance). Load/index failures are logged, never fatal (search is a derived artifact). A dataset whose source health suppresses indexing is skipped for that run (see [resilient-extraction.md](resilient-extraction.md); inert while `[resilience] enforce = false`, the default). The grants apps use this to make every opportunity in `grants/unified` individually searchable (title/agency/status/url) without inlining thousands of records into the job result. These docs carry app `grants` (the virtual unified namespace), not the producing job's app — and **a saved search may scope to that virtual app**: saved-search scoping is evaluated against every namespace a run indexed under (the job's app *plus* each `index_datasets` app), so `app:"grants"` fires on a `grants-gov`/`ca-grants` run. Scoping is not widened beyond that — an unrelated `app` filter still skips the run, and a skip is logged at `debug` with the filter and the run's indexed apps.

## Query surface

`GET /search?q=&limit=&offset=&app=&dataset=&fuzzy=&sort=&since=&amount_gte=&amount_lte=&date_after=&date_before=` → `{query, total, count, hits, facets, index}`. The MCP `search` tool takes the same params through the same parser and renders through the same builder — see [below](#the-mcp-search-tool-same-surface-minus-facets).

- **Params.** `q` required (400 on empty). `limit` default 20, clamped 1–100. `offset` skips ranked hits before `limit` (page 2 = `offset=limit`), clamped to **10 000** — deep Tantivy offsets get progressively costlier. `sort` = `score` (BM25 relevance, the default) or `newest` (most recently indexed first — recency over relevance on a changing corpus); any other value is a 400. `since=<unix-seconds>` keeps only hits indexed at/after that instant (a "what's new" feed), backed by the `indexed_at` fast field.
- **Counts.** `total` is the **exact** number of matching documents, independent of `limit`/`offset` — the denominator for paging (it was previously the page size). `count` is the returned page size.
- **Hits** with highlighted `snippet` (matched terms in `<b>`, generated from the stored body). Hit fields are read directly off the stored doc (`get_first`), not via a full-doc JSON round-trip.
- **Facets**: `apps` + `datasets` counts over the top-1000 matches (honest sample), sorted by count. `app=`/`dataset=` params filter by exact term. **Computed only when requested** (`SearchRequest.facets`, which `GET /search` sets): facets sample ≥1000 docs and decode each, so a facet-less query (the saved-search runner, and any caller that reads only hit ids) ranks and decodes just the `offset+limit` page window — no facet-sampling overread.
- **Fuzzy** (`fuzzy=true`): edit-distance-1 on title+body (transposition = one edit). Quoted `"exact phrases"` parse as phrase queries in either mode.
- **`index`: the state the answer came from.** `{enabled, doc_count, degraded, reason}`, on every response, additive (no existing key changed). `degraded: true` means **an empty `hits` list is not evidence the records are missing** — the index is disabled, or enabled and holding **0 documents** (wiped by schema drift, quarantined as corrupt, or never populated; see [Maintenance](#maintenance)), and `reason` names the recovery. Each degradation is its own message, because the fixes differ: turn `[search] enabled` on, versus run `search-backfill`. A third state exists for honesty — `doc_count: null` when the count could not be read, reported as degraded rather than folded into `0`, which would slander a healthy index. This closes the trap that a wiped index answers `200 {total: 0, hits: []}` byte-identically to a genuine miss: the signal used to live only on `GET /search/status`, so a human (or an MCP agent) searching a wiped index concluded the data did not exist. `doc_count` is free to compute — Tantivy's is `searcher().num_docs()`, a sum over segment metadata the reader already holds — so the block is unconditional rather than appearing only on empty pages.

### The MCP `search` tool: same surface, minus facets

When `[mcp] enabled = true`, the `search` tool exposes **the same query surface** as `GET /search`: `q`, `limit`, `offset`, `app`, `dataset`, `fuzzy`, `sort`, `since`, `amount_gte`/`amount_lte`, `date_after`/`date_before`. It previously offered only `q`/`limit`/`app`/`dataset`, so an agent could not page, order by recency, or use the entity filters at all.

Parsing is not duplicated: both callers build their `SearchRequest` through one `build_search_request` in `crates/server/src/routes/search.rs`, so defaults (`limit` 20), clamps (`limit` 1–100, `offset` ≤ 10 000), and the `sort` vocabulary are identical — an unknown `sort` is refused on both surfaces rather than silently falling back to relevance (a 400 over HTTP, an `isError` tool result over MCP). The tool's JSON Schema advertises the same ceilings the builder enforces, and an inventory test fails if the two lists drift.

**The answer is not duplicated either**: both surfaces render through one `run_search` in the same file, so the `index` block above reaches the tool as well. That matters most for the agent — it is the caller most likely to read `total: 0` off a wiped index and report back that the data does not exist. The tool's own description says to read `index.reason` before concluding anything from zero hits.

The one deliberate difference: the tool returns `{query, total, count, hits, index}` with **no facets** (`facets: false` on the shared request is what omits the key). Facets sample ≥1000 documents and decode each, which is pure cost for a caller that reads hits.

### Entity-typed filters (`amount`, `event_date`)

At index time, `crates/engine-search/src/enrich.rs` extracts two optional fields from each doc's title+body with conservative regex rules, and `GET /search` filters on them: `amount_gte`/`amount_lte` (whole US dollars) and `date_after`/`date_before` (unix seconds). **No match = no field**: a doc with nothing extracted is *absent* from the field, so it never matches any range filter — filtering by amount implies "has an amount", never "amount is 0".

- **`amount`** — the largest amount carrying an explicit `$`/`usd` marker, whole dollars, scale suffixes (`k`/`m`/`mm`/`b`/`million`/`billion`) applied. Bare numbers are not money. Over $1T is treated as extraction noise and dropped. **Ambiguously formatted amounts are dropped, not guessed**: `$1.234,56` / `$1.234.567,89` / `$5.000.000` (European decimal or grouping) would be read as `$1` or `$5` by US-centric parsing, so the candidate is skipped entirely — other, unambiguous amounts in the same document still count.
- **`event_date`** — the earliest *upcoming* deadline-like date (UTC midnight), where "deadline-like" requires a keyword (`deadline`, `due`, `clos…`, `expir…`, `apply`, `submit`, `respond`, `end_date`) within the preceding 120 bytes. A bare publication date is not a deadline. Accepted shapes: `YYYY-MM-DD` **including RFC3339 timestamps** (`2026-09-01T00:00:00Z`, offsets and fractional seconds), `M/D/YYYY`, and `Month D, YYYY`. Upcoming means within `[now − 1 day, now + 10 years]`; invalid calendar dates are dropped.

Enrichment is computed **before** the index writer lock is taken (its own blocking task), so the locked section does index operations only, and both fields come from a single lowercased copy of the text rather than one per field.

`GET /search/status` → `{enabled, doc_count, disk_bytes, segment_count}`. `doc_count` is the logical document count; `disk_bytes` is the index directory's on-disk size (sum of its files, best-effort — an unreadable entry counts 0) and `segment_count` the searchable segments the reader currently sees. The physical pair exists because `doc_count` hides growth on an upserting corpus: flat `doc_count` with climbing bytes/segments means ghosts or merges falling behind. Both are `0` when `[search] enabled = false` (`NoSearch` measures nothing rather than guessing).

## Maintenance

`DELETE /search/docs {ids}` removes documents by id; `DELETE /search/datasets/{app}/{dataset}` removes an app's dataset (app AND dataset conjunction — dataset names repeat across apps). Trait: `Search::{index, query, delete_ids, delete_dataset, doc_count, index_stats, flush}`; `NoSearch` when `[search] enabled=false`. `index()` may defer its commit for throughput (a background committer flushes it), so a caller that must see its own writes — the saved-search runner, an offline backfill — calls `flush()` first.

### Opening the index: four states, three recoveries

`TantivyIndex::new` meets one of four directory states, and each has a defined outcome:

| on disk | outcome |
| --- | --- |
| opens, schema matches this build | used as-is |
| no `meta.json` (first boot, or a previously emptied dir) | fresh index created |
| opens, but the schema predates this build (a field was added, or `body` isn't stored) | **wiped and rebuilt EMPTY**, `warn` log |
| `meta.json` present but unreadable | the directory's **contents are moved aside** into the sibling `<dir>.corrupt.<n>` (a counter, not a timestamp), a fresh index is created in their place, `error` log naming the quarantined path — **boot proceeds** |

The last row used to be an unbootable process: `open_in_dir` fails on the bad manifest and `create_in_dir` refuses a directory that already has a `meta.json`, so there was no path forward. Quarantine also preserves the evidence instead of deleting it — inspect or delete `<dir>.corrupt.<n>` yourself; nothing reclaims it.

**Both destructive branches take the index directory's exclusive writer lock first.** That is Tantivy's own `INDEX_WRITER_LOCK` (`.tantivy-writer.lock`), the same lock a live `IndexWriter` holds for the life of the process — so a new-schema binary started while an old-schema server is running now **fails loudly, naming the lock and the directory**, instead of deleting the running server's index under it. What the lock can and cannot promise, honestly:

- **Can:** exclude any other Tantivy writer (a server, `search-backfill`, `reindex`) on the same directory, on every platform. On an `MmapDirectory` this is a *real OS advisory lock* — `try_lock_exclusive`, i.e. `flock` on Unix and `LockFileEx` on Windows, taken on an open handle to the lock file. Because the kernel owns it, a crashed or `SIGKILL`ed holder releases it automatically: there is **no stale lock to clear by hand**, and the lock file merely *existing* means nothing.
- **Cannot:** it is advisory — only processes that ask for it are excluded, so a stray `rm -rf`, a backup tool, or a second copy of the directory is unaffected. It excludes *writers* only: a peer holding just an `IndexReader` takes no lock, so a wipe can still pull files from under a reader-only process. And `flock`-family locks are unreliable over NFS/SMB; the guarantee is honest for the local-first deployment this service targets.
- **Platform asymmetry:** holding the lock means holding an open handle *inside* the index directory, and Windows refuses to rename or delete a directory containing an open handle. So both destructive steps drain the directory's **contents** in place rather than moving or removing the directory itself — on Unix either would work (an unlinked inode outlives its handles), but only draining works on both, and only draining keeps the lock held for the whole rebuild.

**The index can still go silently empty, and it does not self-heal.** A schema-drift wipe, a quarantined corrupt dir, or a spell of `[search] enabled = false` all leave an enabled index with nothing in it. Queries keep returning `200`, so nothing looks broken — except that **every `GET /search` and MCP `search` response now carries `index.degraded: true` with this recovery in `index.reason`** (see [Query surface](#query-surface)), so the state reaches the caller who is actually being misled rather than only the operator who thinks to check `GET /search/status` (`doc_count: 0` on an enabled index is the same signal). Rebuild from the stored dataset records — **with the server stopped**, since Tantivy holds that exclusive writer lock for the life of the process:

```bash
cargo run -p pumper-server --bin search-backfill -- --app grants --dataset unified
cargo run -p pumper-server --bin search-backfill -- --app grants   # all of an app's datasets
cargo run -p pumper-server --bin search-backfill -- --all          # every dataset
```

A scope is required so a broad rebuild is always deliberate. The backfill uses the same `SearchDoc::from_dataset_record` builder as the live path, so ids are stable and it upserts rather than duplicates — safe against a partially-populated index. **Tombstoned rows are purged, not skipped**: each removed record's doc id is deleted from the index, because it may already be indexed (indexed while live, then removed during a window the live delete path missed) — the stale-hit state a rebuild exists to repair. The completion line reports both counts: `N record(s) indexed, M tombstoned record(s) purged`. Note that backfilling a dataset no app names in `index_datasets` makes it searchable but nothing keeps it current.

**What each scope covers.** All three resolve targets over **every** stored record, tombstoned rows included — a dataset whose records are *all* tombstoned is the state a purge exists to repair, so it must be reachable. (`--all` previously resolved through the live-only dataset listing, which excluded exactly that dataset: its ghost documents survived every "full" rebuild while the tool reported `0 tombstoned record(s) purged` and exited 0.)

| scope | targets |
| --- | --- |
| `--app X --dataset Y` | that one dataset, live or fully tombstoned |
| `--app X` | every dataset `X` has ever written |
| `--all` | every `(app, dataset)` pair in `records` |

**A scope that matches nothing fails.** On every path, including `--app X --dataset Y`, which used to be taken on faith: a typo like `--dataset unifed` read zero rows and printed the same cheerful completion line with exit 0 as a real rebuild. It now exits non-zero naming the scope that matched nothing. `GET /datasets/{app}` lists an app's datasets if you need to check the spelling.

**No read ceiling.** The record read is keyset-paged (500/page) rather than a single `LIMIT 1000000`, so a dataset past a million rows no longer has its *oldest* records silently dropped from the rebuild — and memory stays flat at one page regardless of dataset size.

## Saved searches (standing alerts)

`saved_searches` + `saved_search_seen` tables. `GET/POST /searches`, `DELETE /searches/{id}`, `POST /searches/{id}/enabled`. Body: `{query, app?, dataset?, url, secret?, materialize?}` (400 on an empty `query` or a non-`http(s)` `url`). `GET /searches` is **dual-mode**: bare `{searches: [...]}` by default, or `{items, next_cursor}` when a `cursor` param is present (even empty) — an opaque keyset cursor. `limit` is clamped 1–500 in **both** modes, so an uncursored list can never stream the whole table. After each job's results are indexed, the worker runs enabled saved searches (scoped by their filters) and webhooks a **`search.matched`** event containing only never-before-seen matches — `INSERT OR IGNORE` claim on `(search_id, doc_id)` guarantees exactly-once alerting, including when several source apps publish into one virtual app and each of their runs re-evaluates the same search. **App scoping:** a search with `app` unset runs on every job; a search with `app` set runs when that app is among the run's indexed namespaces — `job.app` plus every `index_datasets` app (see [`index_datasets`](#indexing-a-dataset-from-a-compact-result-index_datasets)). A search that is skipped, matches nothing, or matches only already-alerted docs says so at `debug` rather than passing silently. Deliveries flow through the logged webhook path (DLQ + replay — see [events-webhooks.md](events-webhooks.md)).

### Materializing a saved search into a dataset (M13, "queries as datasets")

`materialize: {app, dataset}` on a saved search turns it into a **standing view**: after each run's alerting, the worker re-runs the query (facets off) and upserts its current result set into that dataset, one record per hit.

- **Record key** = the search doc id. **Value** = `{title, snippet, url, score, source: {app, dataset, key}}` — `score` rounded to one decimal so a BM25 wobble is not a change, and `source.key` is the hit's key inside its own dataset (the doc id minus its `<app>:<dataset>:` prefix).
- **Falling out of the results is a removal**: keys absent from this run's result set are tombstoned via the normal `detect_removed` path, so the view emits `new` / `changed` / `removed` deltas like any scraped dataset. An **empty** result set never wipes the view (removal guard).
- Those deltas then drive the same machinery as any dataset — watches, dataset triggers, `?filter=`, export — fired under the **view's** app, not the producing job's.
- **`[search] max_materialize_results`** (default `500`, `config.toml`) caps it on both axes: the query's `limit` and the per-run removal detection. A broad query stays a bounded view rather than an unbounded copy of the corpus.
- **Refused shape:** `materialize` targeting the same `app`+`dataset` the search is scoped to is a 400 — the view would re-materialize its own records if that dataset were ever indexed.
- Materialization is best-effort throughout: a failing view logs at `warn` and never touches the job outcome or the alert path.

## Known gaps

- No semantic/hybrid search and no autocomplete (backlog). Facets are a top-1000 sample, not exact counts (`total` is exact); the MCP `search` tool returns no facets at all.
- `offset` paging is capped at 10 000; there is no deep-paging cursor over search hits.
- A wiped or quarantined index refills only as records change — recovery is the manual `search-backfill` bin, not an automatic rebuild. Nothing reclaims a `<dir>.corrupt.<n>` quarantine. **Detection is no longer query-path-only**: `GET /datasets/doctor` (`just doctor`) raises a `search_index_empty` finding when search is enabled, the index holds 0 documents, and the store holds live records — so an operator can find the state before a user reports missing results. See [datasets.md § `datasets doctor`](datasets.md#datasets-doctor--store-integrity-report) for why the check is zero-versus-nonzero rather than a ratio.
- **`amount` is US-dollar only.** Extraction requires a `$`/`usd` marker, so `€`, `£`, `CZK`, and every other currency is invisible to `amount_gte`/`amount_lte` — and a non-USD figure is never converted, just skipped.
- **Both entity fields are document-level, not per-item**: one `amount` (the largest in the doc) and one `event_date` (the earliest upcoming deadline) per document. A document listing several awards or several deadlines is filterable only by its maximum amount and its nearest date; the others are not queryable.
- Materialized views carry the display fields above, not the source record's full payload — join back through `source.{app, dataset, key}` for that.
