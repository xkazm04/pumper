# @pumper/sync

The shared consumer SDK for [Pumper](../../README.md)'s canonical datasets. One
implementation of the export→upsert loop, versioned against the API it targets,
so the 10–20 products that mirror Pumper data stop each hand-rolling (and
independently drifting from) that loop.

## Why it exists

Before this package, every consumer re-implemented the same four things and got
at least one of them wrong:

- **Re-normalization.** Products pulled per-source `opportunities` and re-ran the
  same normalizer Pumper already runs. Consume the canonical `*_/unified`
  dataset instead and that code deletes itself.
- **Full-mirror transport.** Each product bulk-pulled the whole corpus every
  run. This syncs the change-feed **delta** since a persisted watermark.
- **No removals.** An upsert-only mirror can never tombstone a delisted record.
  The change feed carries `removed`; the SDK tombstones through the sink.
- **Silent shape drift.** A Pumper-side response-shape change (e.g. the export
  envelope becoming a bare array) broke a hand-rolled parser to *zero rows, no
  error*. One typed client turns that into one compile error in one place.

## Design boundary

The SDK owns **transport, canonical decode, the incremental watermark, and the
new/changed/removed lifecycle**. It does **not** own persistence or your product
model — you pass a `sink` (upsert + tombstone) and a `watermark` store, and the
SDK drives them. Cold start streams a filtered snapshot; every run after that
pulls only the delta.

```
Pumper /datasets/grants/unified ──▶ @pumper/sync ──▶ sink(yourStore)
     canonical, change-detected      delta + lifecycle    PGlite · Firestore · DuckDB · files
```

## Usage

```ts
import { createPumperSync, kvWatermark } from "@pumper/sync";

const sync = createPumperSync({
  baseUrl: process.env.PUMPER_URL,               // default http://127.0.0.1:8088
  dataset: { app: "grants", name: "unified" },   // canonical — no re-normalization
  filter: ["$.status:eq:open"],                  // optional server-side slice
  watermark: kvWatermark({
    get: (k) => store.getMeta(k),                // reuse any KV/settings row
    set: (k, v) => store.setMeta(k, v),
  }),
  sink: {
    upsert: (records) => store.upsertGrants(records),  // [{ key, data }]
    tombstone: (keys) => store.tombstone(keys),
  },
});

const { mode, upserted, tombstoned } = await sync.run();
// mode === "snapshot" on first run, "incremental" thereafter.
```

Run `.run()` on whatever cadence you already use (a cron route, a Pumper
trigger). It is safe to call concurrently-per-dataset only once; the watermark
advances after the sink commits, so a crash re-processes idempotently (upsert by
key) rather than skipping.

### Replacing Wellspring's `pumper.ts`

The current adapter (`src/features/grant-ingest/sources/pumper.ts`) mirrors three
per-source `opportunities` datasets, re-normalizes each with a product-side
normalizer, and pulls the full corpus every run — and today reads a `records`
field the export endpoint no longer returns, so it silently mirrors nothing.

The replacement is one `createPumperSync` over `grants/unified`: the three
`normalize*` calls go away (the data is already canonical), the `MAX_PER_DATASET`
backstop goes away (incremental deltas are small), and removals start working.
If any residual product-only massaging is needed (e.g. a legacy field), it moves
into the optional `map(raw, ctx)` hook instead of a bespoke fetch loop.

## Low-level client

For one-off reads without a watermark:

```ts
import { PumperClient } from "@pumper/sync";

const client = new PumperClient({ baseUrl: process.env.PUMPER_URL });
for await (const rec of client.exportRecords({ app: "grants", name: "unified" }, ["$.source:eq:ca-grants"])) {
  // rec: { key, data, first_seen, last_seen, updated_at, removed_at }
}
const page = await client.changesPage({ app: "grants", name: "unified" }, sinceIso, "");
```

## Build

```bash
npm install
npm run build      # → dist/ (ESM + .d.ts)
npm run typecheck
```

Zero runtime dependencies (global `fetch` + WebStreams; Node ≥ 20).

## Known gaps

- **Watermark boundary.** `since` is an exclusive lower bound at micro-second
  resolution; a new revision landing at the *exact* micro-second of the stored
  watermark could be skipped. Negligible at Pumper's job cadence, but real —
  re-run a snapshot (clear the watermark) if you ever suspect a gap.
- **Wire types are hand-written**, mirroring `crates/core/src/datasets.rs`. If
  the record/revision shapes change, regenerate against `GET /openapi.json`.
- **No built-in retry/backoff** on a failed page — a throw aborts the run and
  leaves the watermark unadvanced, so the next run resumes from the same point.
  Wrap `.run()` in your scheduler's retry if you want automatic recovery.
