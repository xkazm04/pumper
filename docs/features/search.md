# Full-text search & saved searches

Embedded Tantivy index (no external service), BM25-ranked over `title` + `body`. The worker indexes every successful job's result (elements of `records`/`stories`/`items` arrays, else the whole result) — id-keyed upserts.

### Indexing a dataset from a compact result (`index_datasets`)

An app whose result stays compact (counts, not arrays — the fleet convention) can still emit one search document per stored record by adding `"index_datasets": [{ "app", "dataset" }]` to its result. After indexing the result itself, the worker loads each named dataset's **live** records (removed rows skipped) and indexes one doc per record, id `"<app>:<dataset>:<key>"` — stable, so re-runs replace rather than duplicate. Load/index failures are logged, never fatal (search is a derived artifact). The grants apps use this to make every opportunity in `grants/unified` individually searchable (title/agency/status/url) without inlining thousands of records into the job result. These docs carry app `grants` (the virtual unified namespace), not the producing job's app — so a saved-search alert on them should scope by `dataset:"unified"` and leave `app` unset (saved-search notification skips searches whose `app` filter differs from the finished job's app, which is `grants-gov`/`ca-grants`).

## Query surface

`GET /search?q=&limit=&app=&dataset=&fuzzy=` →

- **Hits** with highlighted `snippet` (matched terms in `<b>`, generated from the stored body; pre-snippet indexes are detected on open and rebuilt empty — the index is a derived artifact).
- **Facets**: `apps` + `datasets` counts over the top-1000 matches (honest sample), sorted by count. `app=`/`dataset=` params filter by exact term. **Computed only when requested** (`SearchRequest.facets`, which `GET /search` sets): facets sample ≥1000 docs and decode each, so a facet-less query (the saved-search runner, and any caller that reads only hit ids) ranks and decodes just the `offset+limit` page window — no facet-sampling overread. Hit fields are read directly off the stored doc (`get_first`), not via a full-doc JSON round-trip.
- **Fuzzy** (`fuzzy=true`): edit-distance-1 on title+body (transposition = one edit). Quoted `"exact phrases"` parse as phrase queries in either mode.

## Maintenance

`DELETE /search/docs {ids}` removes documents by id; `DELETE /search/datasets/{app}/{dataset}` removes an app's dataset (app AND dataset conjunction — dataset names repeat across apps). Trait: `Search::{index, query, delete_ids, delete_dataset}`; `NoSearch` when `[search] enabled=false`.

## Saved searches (standing alerts)

`saved_searches` + `saved_search_seen` tables. `GET/POST /searches`, `DELETE /searches/{id}`, `POST /searches/{id}/enabled`. Body: `{query, app?, dataset?, url, secret?}`. After each job's results are indexed, the worker runs enabled saved searches (scoped by their filters) and webhooks a **`search.matched`** event containing only never-before-seen matches — `INSERT OR IGNORE` claim on `(search_id, doc_id)` guarantees exactly-once alerting. Deliveries flow through the logged webhook path (DLQ + replay — see [events-webhooks.md](events-webhooks.md)).

## Known gaps

- No semantic/hybrid search, autocomplete, or Last-Event-ID SSE replay (backlog). Facets are a top-1000 sample, not exact counts.
