# Vision Scan Fix Wave 5 — Search Activation

> 5 commits, 5 ideas closed + 1 duplicate absorbed (theme T4: the Tantivy index goes from barely-used to a product surface).
> Baseline preserved: build clean → build clean; tests 37 → 37, 0 failed.

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `5a03541` | 83280327 | Highlighted result snippets |
| 2 | `d1cdd8c` | 2c9df19f | Faceted filtering by app/dataset |
| 3 | `00f91e7` | 283d6999 | Delete-by-id / delete-by-dataset |
| 4 | `c610ce6` | 65c50b7a | Typo-tolerant fuzzy + phrase search |
| 5 | `cb79221` | a7f3e8b3 | Saved searches as standing alerts (absorbs b6edcd3e) |

## What was built

- **Snippets**: body field now STORED; hits carry a `<b>`-highlighted fragment via SnippetGenerator. Pre-snippet indexes are detected on open and rebuilt empty (derived artifact, refills as jobs run).
- **Facets + filters**: `Search::query` takes `SearchRequest {q, limit, app, dataset, fuzzy}` and returns `SearchResponse {hits, facets}`; filters are exact term clauses; facets counted over top-1000 sample, sorted by count.
- **Delete paths**: `delete_ids` (by term) and `delete_dataset` (app AND dataset `delete_query` — bare dataset term would over-delete across apps). Routes: `DELETE /search/docs {ids}`, `DELETE /search/datasets/{app}/{dataset}`.
- **Fuzzy/phrase**: `fuzzy=true` → edit-distance-1 on title+body; quoted phrases exact in either mode (positions were always indexed).
- **Saved searches** (migration 0013): standing queries with app/dataset scope + webhook target. Worker runs them after each job's indexing; `claim_unseen` (INSERT OR IGNORE on `saved_search_seen`) guarantees exactly-once alerts per (search, doc); deliveries flow through the logged webhook path (new generic `dispatch_event`) → DLQ + replay for free.

## Patterns established

13. **Schema-evolution guard on embedded indexes** — detect capability (body stored?) on open and rebuild derived indexes rather than limping with silent feature loss.
14. **Claim-then-alert dedup** — INSERT OR IGNORE + rows_affected as the atomic "have I alerted this before" primitive; no read-check race.
15. **Facets from a ranked sample** — top-N stored-field counting beats wiring fast-field aggregations at local scale; label it a sample and move on.

## What remains (INDEX themes)

T9 domain data products, T10 platform plays, plus deferred tails (T4: answer-engine RAG, hybrid semantic ×3 dups, multilingual, LTR, autocomplete; T7: API-key auth [product decision], OpenAPI, SSE replay; T5: LLM-assisted extraction).
