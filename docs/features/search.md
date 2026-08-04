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

`GET /search?q=&limit=&offset=&app=&dataset=&fuzzy=&sort=&since=` → `{query, total, count, hits, facets}`.

- **Params.** `q` required (400 on empty). `limit` default 20, clamped 1–100. `offset` skips ranked hits before `limit` (page 2 = `offset=limit`), clamped to **10 000** — deep Tantivy offsets get progressively costlier. `sort` = `score` (BM25 relevance, the default) or `newest` (most recently indexed first — recency over relevance on a changing corpus); any other value is a 400. `since=<unix-seconds>` keeps only hits indexed at/after that instant (a "what's new" feed), backed by the `indexed_at` fast field.
- **Counts.** `total` is the **exact** number of matching documents, independent of `limit`/`offset` — the denominator for paging (it was previously the page size). `count` is the returned page size.
- **Hits** with highlighted `snippet` (matched terms in `<b>`, generated from the stored body). Hit fields are read directly off the stored doc (`get_first`), not via a full-doc JSON round-trip.
- **Facets**: `apps` + `datasets` counts over the top-1000 matches (honest sample), sorted by count. `app=`/`dataset=` params filter by exact term. **Computed only when requested** (`SearchRequest.facets`, which `GET /search` sets): facets sample ≥1000 docs and decode each, so a facet-less query (the saved-search runner, and any caller that reads only hit ids) ranks and decodes just the `offset+limit` page window — no facet-sampling overread.
- **Fuzzy** (`fuzzy=true`): edit-distance-1 on title+body (transposition = one edit). Quoted `"exact phrases"` parse as phrase queries in either mode.

### Entity-typed filters (`amount`, `event_date`)

At index time, `crates/engine-search/src/enrich.rs` extracts two optional fields from each doc's title+body with conservative regex rules, and `GET /search` filters on them: `amount_gte`/`amount_lte` (whole US dollars) and `date_after`/`date_before` (unix seconds). **No match = no field**: a doc with nothing extracted is *absent* from the field, so it never matches any range filter — filtering by amount implies "has an amount", never "amount is 0".

- **`amount`** — the largest amount carrying an explicit `$`/`usd` marker, whole dollars, scale suffixes (`k`/`m`/`mm`/`b`/`million`/`billion`) applied. Bare numbers are not money. Over $1T is treated as extraction noise and dropped. **Ambiguously formatted amounts are dropped, not guessed**: `$1.234,56` / `$1.234.567,89` / `$5.000.000` (European decimal or grouping) would be read as `$1` or `$5` by US-centric parsing, so the candidate is skipped entirely — other, unambiguous amounts in the same document still count.
- **`event_date`** — the earliest *upcoming* deadline-like date (UTC midnight), where "deadline-like" requires a keyword (`deadline`, `due`, `clos…`, `expir…`, `apply`, `submit`, `respond`, `end_date`) within the preceding 120 bytes. A bare publication date is not a deadline. Accepted shapes: `YYYY-MM-DD` **including RFC3339 timestamps** (`2026-09-01T00:00:00Z`, offsets and fractional seconds), `M/D/YYYY`, and `Month D, YYYY`. Upcoming means within `[now − 1 day, now + 10 years]`; invalid calendar dates are dropped.

Enrichment is computed **before** the index writer lock is taken (its own blocking task), so the locked section does index operations only, and both fields come from a single lowercased copy of the text rather than one per field.

`GET /search/status` → `{enabled, doc_count, disk_bytes, segment_count}`. `doc_count` is the logical document count; `disk_bytes` is the index directory's on-disk size (sum of its files, best-effort — an unreadable entry counts 0) and `segment_count` the searchable segments the reader currently sees. The physical pair exists because `doc_count` hides growth on an upserting corpus: flat `doc_count` with climbing bytes/segments means ghosts or merges falling behind. Both are `0` when `[search] enabled = false` (`NoSearch` measures nothing rather than guessing).

## Maintenance

`DELETE /search/docs {ids}` removes documents by id; `DELETE /search/datasets/{app}/{dataset}` removes an app's dataset (app AND dataset conjunction — dataset names repeat across apps). Trait: `Search::{index, query, delete_ids, delete_dataset, doc_count, index_stats, flush}`; `NoSearch` when `[search] enabled=false`. `index()` may defer its commit for throughput (a background committer flushes it), so a caller that must see its own writes — the saved-search runner, an offline backfill — calls `flush()` first.

**The index can go silently empty, and it does not self-heal.** On open, an index whose on-disk schema doesn't match this build's (a field was added, or `body` isn't stored) is **rebuilt EMPTY**; a lost/corrupt index dir or a spell of `[search] enabled = false` has the same effect. Queries keep returning `200` with fewer hits, so nothing looks broken. `GET /search/status` reporting `doc_count: 0` on an enabled index is the signal. Rebuild from the stored dataset records — **with the server stopped**, since Tantivy holds an exclusive writer lock on the index directory:

```bash
cargo run -p pumper-server --bin search-backfill -- --app grants --dataset unified
cargo run -p pumper-server --bin search-backfill -- --app grants   # all of an app's datasets
cargo run -p pumper-server --bin search-backfill -- --all          # every dataset
```

A scope is required so a broad rebuild is always deliberate. The backfill uses the same `SearchDoc::from_dataset_record` builder as the live path, so ids are stable and it upserts rather than duplicates — safe against a partially-populated index. Note that backfilling a dataset no app names in `index_datasets` makes it searchable but nothing keeps it current.

## Saved searches (standing alerts)

`saved_searches` + `saved_search_seen` tables. `GET/POST /searches`, `DELETE /searches/{id}`, `POST /searches/{id}/enabled`. Body: `{query, app?, dataset?, url, secret?}` (400 on an empty `query` or a non-`http(s)` `url`). `GET /searches` is **dual-mode**: bare `{searches: [...]}` by default, or `{items, next_cursor}` when a `cursor` param is present (even empty) — an opaque keyset cursor. `limit` is clamped 1–500 in **both** modes, so an uncursored list can never stream the whole table. After each job's results are indexed, the worker runs enabled saved searches (scoped by their filters) and webhooks a **`search.matched`** event containing only never-before-seen matches — `INSERT OR IGNORE` claim on `(search_id, doc_id)` guarantees exactly-once alerting, including when several source apps publish into one virtual app and each of their runs re-evaluates the same search. **App scoping:** a search with `app` unset runs on every job; a search with `app` set runs when that app is among the run's indexed namespaces — `job.app` plus every `index_datasets` app (see [`index_datasets`](#indexing-a-dataset-from-a-compact-result-index_datasets)). A search that is skipped, matches nothing, or matches only already-alerted docs says so at `debug` rather than passing silently. Deliveries flow through the logged webhook path (DLQ + replay — see [events-webhooks.md](events-webhooks.md)).

## Known gaps

- No semantic/hybrid search and no autocomplete (backlog). Facets are a top-1000 sample, not exact counts (`total` is exact).
- `offset` paging is capped at 10 000; there is no deep-paging cursor over search hits.
- A wiped index refills only as records change — recovery is the manual `search-backfill` bin, not an automatic rebuild.
