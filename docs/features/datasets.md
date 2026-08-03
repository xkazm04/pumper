# Dataset store & change intelligence

Persistent, queryable record store (`records` table): apps upsert typed JSON records keyed `(app, dataset, key)`; the store hashes each value (sha256 canonical JSON + 64-bit SimHash) and reports `new | changed | unchanged`.

## Change intelligence

- **Revisions** (`record_revisions`): every New/Changed upsert appends a revision with a **field-level diff** vs the previous snapshot (dot-notation paths, `{"from":…,"to":…}`, root `$`; `diff_values` exported from core). 'Removed' revisions carry no data.
- **Removal detection**: `AppContext::sync_many` treats the batch as a **full snapshot** — previously-live keys absent from it get `records.removed_at` set + a `removed` revision; reappearing records are revived and reported Changed. `upsert_many` (partial batches) never marks removals — do not conflate them. `detect_removed` refuses an **empty** batch outright (a failed scrape must not tombstone everything), and `sync_many` downgrades itself to `upsert_many` when the source's health state suppresses removals — a *partial* batch is the case the empty-batch guard does not cover.
- **APIs**: `GET /datasets/{app}/{ds}/changes?since=&limit=&trust=` (change feed, newest first, diffs included), `GET /datasets/{app}/{ds}/history?key=` (per-record revision trail).

## Trust

Records and revisions carry a `trust` stamp recording how much the write is stood behind: `stable`, `provisional` (written while its source was degrading) or `quarantined`. Stamping comes from extraction health — see [resilient-extraction.md](resilient-extraction.md) — and only happens when `[resilience] enforce = true`.

Stored `NULL` **means** `stable`. That is a semantic default, not a sentinel: every row written before the column existed is correct by construction, so migration 0020 needs no backfill (the lesson from `0004_simhash.sql`, whose `DEFAULT 0` sentinel silently disabled near-dup detection for 3,367 rows). `datasets::trust_label` is the one place that decides the equivalence, and readers must not re-derive it.

Filtering follows push-versus-pull: **pushes suppress, pulls filter**. A webhook cannot be recalled, so watches/triggers are dropped at the source; a pull API is re-readable, so it filters and stays inspectable.

- `GET /datasets/{app}/{ds}/changes?trust=` defaults to **`stable`** — accepts `all`, `provisional`, `quarantined`.
- `GET /datasets/{app}/{ds}?trust=` defaults to **`all`**: each record carries its own stamp, so the raw dataset view stays complete.
- `/export` is never filtered (a complete copy by definition; the stamp rides in the payload).

A quarantined source writes to the shadow dataset `<ds>@q`, which is an ordinary dataset — listing, changes, export and duplicates all work on it unchanged.

## Querying & export

- `GET /datasets/{app}/{ds}?limit=&cursor=&trust=` — records newest-updated first; `cursor=` (even empty) switches to `{items, next_cursor}` keyset pagination (`updated_at|key`); absent = legacy bare array. Removed records included with `removed_at` set.
- `GET /datasets/{app}/{ds}/export?format=json|ndjson|csv` — `json` buffered (100k cap); `ndjson`/`csv` **stream** in keyset-paged 1000-row batches with content-disposition (CSV: fixed columns key/timestamps/data-as-JSON, RFC-4180 quoted).
- `GET /apps/{name}/datasets` — dataset names per app. `GET /datasets/{app}/{ds}/duplicates?distance=` — SimHash near-duplicate pairs (O(n²), local scale).

## Conventions

- Keys are stable external ids (opportunity id, URL, `czisco|kraj|org`). Timestamps are fixed-width RFC 3339 UTC micros (`ts()` helpers) so lexicographic SQL comparison = chronological.
- **Batch writes are set-shaped.** `upsert_many` commits in chunks of 500 on one held connection, and within a chunk issues a bounded number of statements (two batched reads + multi-row writes) rather than one triple per record — the statement count per chunk *is* the write-lock hold time other apps wait on. Consequence for consumers: **every record in one chunk shares one `last_seen`/`updated_at`/revision `created_at` stamp**. Ordered reads already tiebreak that — `/datasets/{app}/{ds}` by `key`, `/changes` by rowid — so paging stays stable; do not rely on records within a batch having distinct timestamps.
- **Virtual namespaces**: several apps may feed one cross-source dataset by passing an explicit app name to `ctx.datasets` (e.g. `grants/unified`, `census/market_blend`, `cz-labour/salary_gap`) with source-prefixed keys.
- Big payloads go to `ctx.save_artifact` (files under `data/artifacts/<app>/<job>/`); records and results stay compact.

## Known gaps

- SimHash duplicate scan is O(n²) (LSH banding is a backlog idea). No Parquet export. `changes_since` scans per app — fine for SQLite scale.
