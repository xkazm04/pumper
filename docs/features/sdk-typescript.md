# TypeScript consumer SDK (`@pumper/sync`)

The shared client library downstream products use to mirror Pumper's canonical
datasets into their own store — instead of each product hand-rolling an
export→normalize→upsert loop over the HTTP API. Implementation:
`clients/typescript/` (zero runtime deps; global `fetch` + WebStreams, Node ≥ 20).
Full usage in [`clients/typescript/README.md`](../../clients/typescript/README.md).

> **Restored.** This package was accidentally deleted by `27dba84`
> ("vibeman(moonshot): batch-7 integration + lockfile") while this doc, the
> `README.md`/`CLAUDE.md` references, and `context-map.json` kept describing
> it as shipped. It has been restored from `27dba84~1` and reconciled with the
> dataset-read surface as it stands today (`trust=`/`removed=` now apply
> uniformly to every read shape, `?removed=` defaults to `exclude` — see
> [datasets.md § Tombstones](datasets.md#tombstones-removed_at)). The fixes:
> - `PumperClient.exportRecords` now explicitly requests
>   `trust=all&removed=include`. Before this change, `/export` ignored both
>   params and always returned every trust tier and every tombstone; today it
>   honors them, and its `removed=` default flipped to `exclude`. Without the
>   explicit override, `PumperSync`'s cold-start snapshot would silently stop
>   seeing previously-removed keys and could never tombstone them through a
>   fresh sink — a correctness regression, not a build break, so nothing would
>   have caught it short of this fix.
> - `PumperClient.changesPage` now explicitly requests `trust=stable` (the
>   server's own default, unchanged by this reconciliation — stated for
>   parity with the export fix above, and to keep the wire request pinned by
>   the conformance test below rather than implicit).
> - `PumperRecord`/`PumperRevision` gained the `trust: string` field, present
>   on the wire since before the deletion but missing from the hand-written
>   types.
>
> A conformance test pins this contract:
> `clients/typescript/test/conformance.test.ts` (fixture-driven: shape
> assertions + the query params each client method sends) paired with
> `crates/server/src/routes/datasets.rs::sdk_fixture_conformance_tests`
> (asserts the server's actual `Record`/`Revision` serialization covers the
> same fixture fields). Both sides load
> `clients/typescript/test/fixtures/*.json`, so a field rename on either side
> fails its half of the pin — this does **not** prove live HTTP wire
> compatibility end-to-end (no server was booted for it); it proves both
> sides agree on the shape.

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
re-processes idempotently (upsert by key) rather than skipping. It advances
**per page**, not once per run: the incremental drain reads the feed
oldest-first (`order=asc`), applies one page of at most `batchSize` revisions
through the sink, and only then stores that page's position. A consumer whose
sink dies on page 7 keeps pages 1-6 and resumes at page 7 — it does not lose
the run.

### The watermark is a position, not a timestamp

The stored value is the opaque token `<created_at>|<rowid>`: the same tuple the
change feed pages by. **Never parse it, compare it, or synthesize one.**

It has to be a tuple because `upsert_many` commits in chunks of 500 and every
record in a chunk shares ONE revision `created_at`
([datasets.md § Conventions](datasets.md)). An instant therefore cannot name a
boundary *inside* a chunk, and resuming at `created_at > S` drops every sibling
of S the previous run had not read — up to 499 revisions, silently, with no way
to notice from the watermark's side.

**Upgrading needs no resync.** A store holding the old bare RFC3339 value is
read as the position `(that instant, rowid 0)`, which sits below every real
rowid and therefore *re-reads* the boundary stamp instead of skipping it;
re-delivery is idempotent through the sink. The first run rewrites the value as
a token. Nothing to migrate, no cold-start snapshot forced.

## Public surface

- `createPumperSync(config) → { run(): Promise<SyncResult> }` — the mirror. Config:
  `dataset`, `filter?`, `watermark`, `sink`, `map?`, `batchSize?` (default 500),
  `baseUrl?` (default `$PUMPER_URL` → `http://127.0.0.1:8088`), `timeoutMs?`,
  `maxBytes?`, `onProgress?`, `signal?`.
- `PumperClient` — stateless low-level reads: `exportRecords(ds, filter?, signal?)`
  (async generator, requests `trust=all&removed=include`) and
  `changesPage(ds, since, cursor, limit?, trust?, order?)` (defaults
  `trust="stable"`; `order` is omitted from the request entirely unless given,
  so the default call is byte-identical to before).
- `memoryWatermark()`, `kvWatermark(kv)` — `WatermarkStore` implementations.
- `parseWatermark(stored)` / `formatWatermark(pos)` / `normalizeStamp(iso)` —
  the position codec, exported for products that store the watermark in a typed
  column. `parseWatermark` accepts both the token and the legacy bare instant.
- `PumperHttpError` — carries Pumper's `{error, code}` envelope; branch on `.code`.

`SyncResult` = `{ mode: "snapshot" | "incremental", upserted, tombstoned,
watermark, anomalies?, stalled? }`. `anomalies` counts feed items the SDK
refused to apply and `stalled` is true when the drain stopped at one instead of
reaching the end of the feed — see § Known gaps.

## Data model

Wire types (`PumperRecord`, `PumperRevision`, `RevisionChange`) are hand-written
to mirror `crates/core/src/datasets.rs` (`Record`, `Revision`). No persistence of
its own — the SDK is a client; the only durable state it touches is the product's
watermark row.

## Known gaps

- ~~**Watermark boundary:** a revision landing at the exact stored instant could
  be skipped (negligible at job cadence).~~ **Fixed, and the dismissal was
  wrong.** The population at "the exact stored instant" is not one revision: it
  is an entire `upsert_many` chunk, up to 500 revisions sharing one stamp, so
  the blast radius was up to 499 silently-lost records per occurrence. The
  watermark is now the keyset position `<created_at>|<rowid>` and the drain
  resumes with the tuple comparison the feed orders by. Pinned end-to-end by
  `clients/typescript/test/conformance.test.ts` replaying
  `test/fixtures/feed-shared-stamp.json`, which is emitted from a real
  `Datasets` store by
  `crates/core/tests/change_feed_keyset.rs::shared_stamp_feed_fixture_is_emitted_from_a_real_store`
  rather than hand-authored.
- **A malformed revision stalls the drain rather than skipping it.** A
  non-`removed` revision with a null post-image is a contradiction in the feed;
  the SDK applies nothing from that page, leaves the watermark *before* it, and
  returns `stalled: true` with `anomalies > 0`. That is deliberate: the previous
  behaviour marked the key consumed and advanced past it, so the record was
  neither written nor ever reachable again. A stalled mirror is visible; a
  diverged one is not. Fixing the feed makes the next run proceed.
- **Types are hand-mirrored,** not generated — regenerate against `GET /openapi.json`
  if the record/revision shapes drift.
- **No built-in retry/backoff:** a failed page aborts the run; the pages already
  applied keep their checkpoint and the next run resumes at the failed page. Wrap `.run()` in the
  caller's scheduler retry.
- **TypeScript only** so far — a Rust/Python twin would generate off the same
  OpenAPI spec.
