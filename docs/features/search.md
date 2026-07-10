# Full-text search & saved searches

Embedded Tantivy index (no external service), BM25-ranked over `title` + `body`. The worker indexes every successful job's result (elements of `records`/`stories`/`items` arrays, else the whole result) — id-keyed upserts.

## Query surface

`GET /search?q=&limit=&app=&dataset=&fuzzy=` →

- **Hits** with highlighted `snippet` (matched terms in `<b>`, generated from the stored body; pre-snippet indexes are detected on open and rebuilt empty — the index is a derived artifact).
- **Facets**: `apps` + `datasets` counts over the top-1000 matches (honest sample), sorted by count. `app=`/`dataset=` params filter by exact term.
- **Fuzzy** (`fuzzy=true`): edit-distance-1 on title+body (transposition = one edit). Quoted `"exact phrases"` parse as phrase queries in either mode.

## Maintenance

`DELETE /search/docs {ids}` removes documents by id; `DELETE /search/datasets/{app}/{dataset}` removes an app's dataset (app AND dataset conjunction — dataset names repeat across apps). Trait: `Search::{index, query, delete_ids, delete_dataset}`; `NoSearch` when `[search] enabled=false`.

## Saved searches (standing alerts)

`saved_searches` + `saved_search_seen` tables. `GET/POST /searches`, `DELETE /searches/{id}`, `POST /searches/{id}/enabled`. Body: `{query, app?, dataset?, url, secret?}`. After each job's results are indexed, the worker runs enabled saved searches (scoped by their filters) and webhooks a **`search.matched`** event containing only never-before-seen matches — `INSERT OR IGNORE` claim on `(search_id, doc_id)` guarantees exactly-once alerting. Deliveries flow through the logged webhook path (DLQ + replay — see [events-webhooks.md](events-webhooks.md)).

## Known gaps

- No semantic/hybrid search, autocomplete, or Last-Event-ID SSE replay (backlog). Facets are a top-1000 sample, not exact counts.
