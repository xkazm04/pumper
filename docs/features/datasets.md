# Dataset store & change intelligence

Persistent, queryable record store (`records` table): apps upsert typed JSON records keyed `(app, dataset, key)`; the store hashes each value (sha256 canonical JSON + 64-bit SimHash) and reports `new | changed | unchanged`.

## Change intelligence

- **Revisions** (`record_revisions`): every New/Changed upsert appends a revision with a **field-level diff** vs the previous snapshot (dot-notation paths, `{"from":…,"to":…}`, root `$`; `diff_values` exported from core). 'Removed' revisions carry no data.
- **Removal detection**: `AppContext::sync_many` treats the batch as a **full snapshot** — previously-live keys absent from it get `records.removed_at` set + a `removed` revision; reappearing records are revived and reported Changed. `upsert_many` (partial batches) never marks removals — do not conflate them.
- **APIs**: `GET /datasets/{app}/{ds}/changes?since=&limit=` (change feed, newest first, diffs included), `GET /datasets/{app}/{ds}/history?key=` (per-record revision trail).

## Querying & export

- `GET /datasets/{app}/{ds}?limit=&cursor=` — records newest-updated first; `cursor=` (even empty) switches to `{items, next_cursor}` keyset pagination (`updated_at|key`); absent = legacy bare array. Removed records included with `removed_at` set.
- `GET /datasets/{app}/{ds}/export?format=json|ndjson|csv` — `json` buffered (100k cap); `ndjson`/`csv` **stream** in keyset-paged 1000-row batches with content-disposition (CSV: fixed columns key/timestamps/data-as-JSON, RFC-4180 quoted).
- `GET /apps/{name}/datasets` — dataset names per app. `GET /datasets/{app}/{ds}/duplicates?distance=` — SimHash near-duplicate pairs (O(n²), local scale).

## Conventions

- Keys are stable external ids (opportunity id, URL, `czisco|kraj|org`). Timestamps are fixed-width RFC 3339 UTC micros (`ts()` helpers) so lexicographic SQL comparison = chronological.
- **Virtual namespaces**: several apps may feed one cross-source dataset by passing an explicit app name to `ctx.datasets` (e.g. `grants/unified`, `census/market_blend`, `cz-labour/salary_gap`) with source-prefixed keys.
- Big payloads go to `ctx.save_artifact` (files under `data/artifacts/<app>/<job>/`); records and results stay compact.

## Known gaps

- SimHash duplicate scan is O(n²) (LSH banding is a backlog idea). No Parquet export. `changes_since` scans per app — fine for SQLite scale.
