# Consumer SDKs and CLI

Three clients, one contract. Every wire type below is **generated** from
`clients/openapi.json` — the OpenAPI document the server's own router produces —
so none of them is a hand mirror of the server's shapes, and a rename over there
fails a build over here.

| Artifact | Package | Path | What it is |
| --- | --- | --- | --- |
| TypeScript SDK | `@pumper/sync` | `clients/typescript/` | the mirror: watermark-driven incremental sync into any product store |
| Python SDK | `pumper-sync` | `clients/python/` | the same loop, step for step, stdlib only |
| CLI | `@pumper/cli` | `clients/cli/` | `pumper jobs ls`, `pumper datasets export`, `pumper triggers test` |

## The generation chain

```
crates/server/src/routes/**            the router + its response DTOs
        |   pinned by: cargo test, spec_snapshot_tests
clients/openapi.json                   the committed contract
        |   pinned by: CI, generate-clients.mjs --check
clients/typescript/src/generated.ts
clients/cli/src/generated.ts
clients/python/pumper_sync/generated.py
```

Both arrows are gates. `cargo test` fails if the committed document does not
match what the router serves; the `Consumer clients` CI job fails if the
generated files do not match the committed document. Between them there is no
step at which a shape change can land with the clients still describing the old
one — which is exactly what the hand-mirrored types allowed, and what the
fixture-conformance test existed to catch after the fact.

Regenerating, both steps in order:

```bash
just openapi     # router   -> clients/openapi.json   (needs cargo)
just clients     # document -> every client            (node only)
```

`just sdk` runs the drift check and all three test suites; `just ci` includes it.

## What is generated and what is not

Generated: every wire shape, in all three languages. Hand-written: the mirror
loop, the client transport, the retry policy, the CLI's commands, and the
protocols a product implements (`WatermarkStore`, `SyncSink`, `MapContext`).
None of those is a payload the server serves, so no generator knows about them.

The TypeScript SDK keeps its exported names and its generics — `PumperRecord<T>`,
`PumperRevision<T>`, `RevisionPage<T>`, `PumperEvent<T>` — as aliases into the
generated module. They differ from the raw generated types in exactly two ways,
both deliberate: `data`/`payload` stay the consumer's `T` (the server genuinely
does not type them), and `Required<>` re-imposes presence, because utoipa renders
a Rust `Option<T>` as a *non-required* property while the handlers' `json!`
literals always write the key as `null`. `PumperSchemas` is exported for any
response shape the SDK does not wrap yet.

## TypeScript: `@pumper/sync`

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
re-processes idempotently (upsert by key) rather than skipping.

## Public surface

- `createPumperSync(config) → { run(): Promise<SyncResult> }` — the mirror. Config:
  `dataset`, `filter?`, `watermark`, `sink`, `map?`, `batchSize?` (default 500),
  `baseUrl?` (default `$PUMPER_URL` → `http://127.0.0.1:8088`), `timeoutMs?`,
  `maxBytes?`, `onProgress?`, `signal?`.
- `PumperClient` — stateless low-level reads: `exportRecords(ds, filter?, signal?)`
  (async generator, requests `trust=all&removed=include`),
  `changesPage(ds, since, cursor, limit?, trust?)` (defaults `trust="stable"`),
  and the event-log cursor pair below.
- `client.eventsPage(after?, {kind?, app?, limit?})` / `client.subscribe({cursor?,
  kind?, app?, limit?})` — the durable event log (N05). `subscribe` is an async
  generator that walks `GET /events/log` forward from `cursor` and **returns**
  when the server says you are caught up (`next_after: null`); it does not tail,
  so poll it on your own interval. The consumer owns the cursor: persist
  `event.seq` after you have durably handled the event and pass it back as
  `cursor`. At-least-once is the server's log plus your commit — the SDK keeps
  no state and retries nothing.

  This is a different question from `changesPage`, not a faster version of it.
  The change feed answers *what records changed in this dataset*; the event log
  answers *what happened on this server*, including kinds a dataset has no
  opinion about (`job.failed`, `external`, `transaction.submitted`,
  `source.repair_promoted`). Mirroring stays on the watermark; reacting goes on
  the cursor. See [events-webhooks.md § The durable event
  log](events-webhooks.md#the-durable-event-log-n05).
- `memoryWatermark()`, `kvWatermark(kv)` — `WatermarkStore` implementations.
- `PumperHttpError` — carries Pumper's `{error, code}` envelope; branch on `.code`.

`SyncResult` = `{ mode: "snapshot" | "incremental", upserted, tombstoned, watermark }`.

## Data model

Wire types (`PumperRecord`, `PumperRevision`, `PumperEvent`, ...) are
**generated** from the served document; see
[The generation chain](#the-generation-chain). `RevisionChange`
(`"new" | "changed" | "removed"`) stays hand-narrowed — the server's field is a
plain `String` on the wire, so the spec cannot say more, but those three are the
whole vocabulary and a consumer should be able to `switch` on them exhaustively.
No persistence of its own — the SDK is a client; the only durable state it
touches is the product's watermark row.

## Python: `pumper-sync`

`clients/python/`, stdlib only (`urllib`, `json`), Python >= 3.11. The same
watermark loop as the TypeScript SDK, **step for step** including the
newest-first de-duplication in the incremental pass: two SDKs mirroring one feed
with subtly different loops are two different products, and the difference would
only ever surface as a divergent mirror in somebody's database.

```python
from pumper_sync import DatasetRef, PumperClient, PumperSync

result = PumperSync(
    DatasetRef("grants", "unified"),
    watermark=MyWatermarkTable(),   # get(ds) -> str | None; set(ds, str)
    sink=MySink(),                  # upsert([(key, data)]) -> int; tombstone([key]) -> int
    client=PumperClient("http://127.0.0.1:8088"),
    filters=["$.status:eq:open"],
).run()
```

`PumperClient.subscribe(cursor=...)` is the event-log walk, with the same
contract as the TypeScript one: it terminates when the server says you are
caught up, it does not tail, and the consumer owns the cursor.

Retries — the one policy in either SDK — are `rate_limited`, `unavailable` and
`bad_gateway` only, with exponential backoff. Every other code is a real refusal,
and `budget_exhausted` is retried **never**: a spend ceiling a client retries
around is not a ceiling. A test pins that.

Full usage in [`clients/python/README.md`](../../clients/python/README.md).

## CLI: `@pumper/cli`

`clients/cli/`, Node >= 20, typed off the same document.

```
pumper jobs ls [--app X] [--status queued|running|...] [--limit N] [--cursor C] [--json]
pumper datasets export <app> <dataset> [--filter '$.status:eq:open']... [--format ndjson|json|csv]
pumper triggers test <trigger-id> [--fire]
```

`--url` defaults to `$PUMPER_URL` then `http://127.0.0.1:8088`; `--key` to
`$PUMPER_API_KEY` (only needed with `[auth] mode = "keys"`).

Three things worth knowing:

- `jobs ls` handles **both** arms of the dual-mode `GET /jobs` — the bare array
  served without `?cursor=` and the keyset envelope served with it. That union
  is now a schema (`JobsResponse`), so the narrowing is typed rather than
  hopeful; a CLI that only understood the envelope would print nothing at all
  against a default call.
- `datasets export` streams — one row in memory at a time, so exporting a corpus
  is bounded by the terminal, not by RAM.
- `triggers test` is a **dry run** unless `--fire`, and `would_fire: false` exits
  `0`: that is the answer you asked for, not a failure.

Refusals print `error <status> <code>: <message>`. Branch on `code`; the message
is prose and is not a contract.

Full usage in [`clients/cli/README.md`](../../clients/cli/README.md).

## Known gaps

- **Watermark boundary:** `since` is an exclusive micro-second lower bound; a
  revision landing at the exact stored instant could be skipped (negligible at job
  cadence; clear the watermark to force a fresh snapshot if suspected).
- **No retry/backoff in the TypeScript SDK.** The Python SDK and the CLI both
  retry the three transient codes; `@pumper/sync` still aborts the run with the
  watermark unadvanced and resumes from the same point next time. Wrap `.run()`
  in the caller's scheduler retry until it catches up.
- **`Option<T>` is non-required in the document.** utoipa renders it that way,
  while the handlers' `json!` literals always write the key as `null`. The
  TypeScript aliases correct for it with `Required<>`; the generated Python
  TypedDicts and the raw generated TS types do not, so a field that is always
  present reads there as one that might be missing.
- **Python `__required_keys__` is unreliable at run time.** The generated
  TypedDicts quote their annotations (forward references across ~260 shapes), so
  `NotRequired` is invisible to run-time introspection. Static checkers resolve
  it correctly; do not branch on `__required_keys__`.
- **No SSE in the SDK.** `subscribe` polls `GET /events/log`; it does not open
  the `GET /events` stream. A batch mirror has no long-lived connection anywhere
  in it, and a durable cursor gives the same at-least-once guarantee without
  one — but a consumer that wants sub-second latency needs the SSE stream and an
  `EventSource` of its own.
- **No push half.** `POST /subscriptions` (the server pushing at a sink) has no
  SDK wrapper; `subscribe` is the pull side only.
- **No Rust client crate.** TypeScript, Python and the CLI generate off the
  document; a Rust twin would too, but the server's own types are already in the
  workspace, so it would serve an external consumer only.
- **The generated types are shapes, not validators.** Nothing at run time checks
  a response against its schema in any of the three clients; a server that lied
  would be believed. The gates catch drift between the router and the clients,
  not between the router and reality.
