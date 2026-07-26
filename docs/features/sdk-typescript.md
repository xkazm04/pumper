# TypeScript consumer SDK (`@pumper/sync`)

The shared client library downstream products use to mirror Pumper's canonical
datasets into their own store — instead of each product hand-rolling an
export→normalize→upsert loop over the HTTP API. Implementation:
`clients/typescript/` (zero runtime deps; global `fetch` + WebStreams, Node ≥ 20).
Full usage in [`clients/typescript/README.md`](../../clients/typescript/README.md).

## What it does

- **Consumes canonical datasets** (`GET /datasets/{app}/{ds}/export` and
  `.../changes`). Products point at a unified dataset (e.g. `grants/unified`) and
  drop their re-normalization — the canonical schema is already computed
  server-side.
- **Incremental, watermark-driven mirroring.** Cold start streams a filtered
  ndjson snapshot; every run after that pulls only the change-feed delta since a
  persisted watermark (an RFC3339 timestamp). No full-corpus re-pull per run.
- **Full lifecycle.** New/changed revisions carry the full post-image in the feed
  (`data`), applied directly with no follow-up read; `removed` revisions tombstone
  through the sink. An upsert-only mirror could not do the latter.
- **Filter pushdown.** The `filter=` predicate (`<path>:<op>:<value>`, ANDed) is
  passed straight through, so a product mirrors only its slice server-side.

## Design boundary (what it does NOT own)

Persistence and the product data model stay product-side. The consumer supplies:

- a **`sink`** — `upsert(records: {key,data}[])` + `tombstone(keys: string[])`,
  landing records into PGlite / Firestore / DuckDB / files as the product sees fit;
- a **`watermark`** store — `get`/`set` over any KV or settings row (`kvWatermark`
  / `memoryWatermark` helpers provided);
- an optional **`map(raw, ctx)`** for residual product-only massaging (identity by
  default for a straight canonical mirror).

The watermark advances only after the sink commits, so a mid-run crash
re-processes idempotently (upsert by key) rather than skipping.

## Public surface

- `createPumperSync(config) → { run(): Promise<SyncResult> }` — the mirror. Config:
  `dataset`, `filter?`, `watermark`, `sink`, `map?`, `batchSize?` (default 500),
  `baseUrl?` (default `$PUMPER_URL` → `http://127.0.0.1:8088`), `timeoutMs?`,
  `maxBytes?`, `onProgress?`, `signal?`.
- `PumperClient` — stateless low-level reads: `exportRecords(ds, filter?, signal?)`
  (async generator) and `changesPage(ds, since, cursor, limit?)`.
- `memoryWatermark()`, `kvWatermark(kv)` — `WatermarkStore` implementations.
- `PumperHttpError` — carries Pumper's `{error, code}` envelope; branch on `.code`.

`SyncResult` = `{ mode: "snapshot" | "incremental", upserted, tombstoned, watermark }`.

## Data model

Wire types (`PumperRecord`, `PumperRevision`, `RevisionChange`) are hand-written
to mirror `crates/core/src/datasets.rs` (`Record`, `Revision`). No persistence of
its own — the SDK is a client; the only durable state it touches is the product's
watermark row.

## Known gaps

- **Watermark boundary:** `since` is an exclusive micro-second lower bound; a
  revision landing at the exact stored instant could be skipped (negligible at job
  cadence; clear the watermark to force a fresh snapshot if suspected).
- **Types are hand-mirrored,** not generated — regenerate against `GET /openapi.json`
  if the record/revision shapes drift.
- **No built-in retry/backoff:** a failed page aborts the run with the watermark
  unadvanced; the next run resumes from the same point. Wrap `.run()` in the
  caller's scheduler retry.
- **TypeScript only** so far — a Rust/Python twin would generate off the same
  OpenAPI spec.
