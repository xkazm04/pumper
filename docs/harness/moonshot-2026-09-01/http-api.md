# HTTP API — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## HA1 — Identity & tenancy plane: scoped API keys, per-principal budgets, cost attribution

- deck item **N20**
- lens: `business-strategist` · size: **XL** · gate: **policy** · effort 8 / impact 9 / risk 6
- contexts: api-surface, automation-api, dataset-api, job-search-api
- extends: ingress per-source HMAC secrets (M21) + remote fabric shared secret (M17) + per-job/schedule/trigger budget_usd floors; 'new' as an identity concept

### Summary
Give pumper an identity concept: scoped API keys (principals) with per-principal spend ceilings, rate limits, dataset/app scopes and a durable audit ledger, so one node can serve the 10-20 downstream products and agent fleets the SDK/MCP work is aimed at without every caller being the operator.

### Description
Every surface in this group records the same gap and works around it locally. `docs/features/http-api.md:3` states "Local power mode: no auth - API-key auth is a parked decision"; `docs/deployment.md:103-113` spells out that the unauthenticated surface is fully mutating and can spend real money through the Claude engine. The two-step delete gate says it outright: "Authentication is a fourth rung this server cannot climb: it has no identity concept at all (the only credential anywhere is the ingress HMAC, which authenticates a webhook sender, not an operator)" (`crates/server/src/routes/datasets.rs:344-348`). Peering (`docs/features/peering.md:251-253`) and the delivery log (`docs/features/events-webhooks.md:276`, bodies returned in full) both defer to "the API-key story first".

The substrate for principals already exists in pieces: ingress sources are created with a secret that is returned exactly once and never listed again (`crates/server/src/routes/ingress.rs:118-162`); the remote fabric compares a shared secret as constant-shape SHA-256 digests (`crates/server/src/routes/remote.rs:71-73,158-167`); a pure token-bucket step and a per-source bucket map exist (`ingress.rs:40-48,88-101`); spend ceilings are enforced at every door through one `validate_budget_usd` (`crates/server/src/routes/jobs.rs:47-61`, `schedules.rs:253-269`, `triggers.rs:263-264`); MCP already has a one-bit authorization model (`enqueue_job` offered only under `[mcp] allow_enqueue`, `crates/server/src/mcp/mod.rs:140-141`); and the DataHub governance actuator keeps a durable audit trail with `{id, action, target, subject, evidence}` rows (`crates/server/src/routes/query.rs:531`). What is missing is the join: the cost ledger is keyed by `job_id`/`app`/`engine` with no caller column (`crates/core/migrations/0007_cost_ledger.sql:4-13`; `economics.rs:48-50` notes the ledger attributes cost to apps), so "which product spent the $40 this week" is unanswerable today. Registry subjects that constrain the design: `rate-limiting` (api-surface's top match), `operator-surfaces-for-llm-spend` (dataset-api), `billing-revenue-normalization` (automation-api).

This is a policy gate because auth is explicitly parked; the card proposes an opt-in `[auth] mode = "open" | "keys"` where `open` is byte-for-byte today's behaviour (the same posture `[ingress]`, `[remote]`, `[mcp]` already take), so the local-first default is untouched.

### Flow
- Migration: `principals` (id, name, key_hash, scopes JSON, budget_usd_per_day, rate_limit_per_min, enabled, created_at) and `audit_log` (principal_id, action, target, at, detail); `jobs.principal_id` and `cost_events.principal_id` (nullable - legacy rows stay unattributed, never invented)
- `POST /principals` (key shown once, like `create_ingress_source`), list/disable/rotate; key presented as `Authorization: Bearer` or `x-pumper-key`, compared as digests
- One tower layer (`with_middleware`, `routes/mod.rs:331-352`) that resolves the principal, enforces scope (read datasets / enqueue apps / admin), consumes the principal's token bucket (reuse `bucket_step`), and stamps the request extension the enqueue doors read; `open` mode resolves a synthetic `operator` principal
- Budget: per-principal daily ceiling checked at every door beside `validate_budget_usd`; `GET /costs?principal=` and a `by_principal` block on `/economics`
- Audit every mutating verb (enqueue/cancel/delete dataset/schedule+trigger CRUD/replay) into `audit_log`; `GET /audit?principal=&cursor=`
- MCP: a key per agent session; `allow_enqueue` becomes a scope rather than a global bit
- Docs: new `docs/features/auth.md`, `deployment.md` auth posture rewritten; `EXPECTED` route inventory updated

### Expected impact
Downstream products (SDK consumers, Ledgerline/Politicas-style apps) and agent fleets get their own keys, their own spend line and their own throttle; the operator gets an answer to "who spent what" and a log of who deleted what. Measured by: cost ledger rows carrying a principal (0% today), and the number of distinct principals active per week. What could break: any client that does not send a key once the operator flips `mode = keys` - hence the default stays `open` and `/health` stays unauthenticated.

### Evaluation
Claim: other - a caller identity with scope, throttle, spend ceiling and audit trail on every mutating route
Before: 0 routes authenticate an operator; cost_events has no caller column (migration 0007); ingress sources are the only principal-like row and they cannot call anything
After: every request resolves a principal; `cost_events.principal_id` populated for new jobs; `GET /audit` non-empty after the first mutating call
Method: probe - read every route module in this group for a credential check; only `/ingest/{id}` (sender HMAC) and `/fetch-proxy` (cluster secret) have one
Result: unmeasurable (the instrument is the share of cost_events rows with a principal, plus a smoke run that exercises `mode = keys` with a scoped key being refused on an out-of-scope route)
Gate: policy

### Evidence

```
docs/features/http-api.md:3 - 'Local power mode: no auth (API-key auth is a parked decision)'
docs/deployment.md:103-113 - unauthenticated AND fully mutating, spends money via Claude
crates/server/src/routes/datasets.rs:344-348 - 'it has no identity concept at all'
crates/server/src/routes/ingress.rs:118-162 - per-source secret, shown once (principal embryo)
crates/server/src/routes/remote.rs:71-73,158-167 - digest-compared shared secret
crates/server/src/routes/ingress.rs:40-48,88-101 - pure token bucket + per-source buckets
crates/server/src/routes/jobs.rs:47-61 - validate_budget_usd, the one budget door
crates/core/migrations/0007_cost_ledger.sql:4-13 - cost_events(job_id, app, engine, ...) no caller
crates/server/src/mcp/mod.rs:140-141 - enqueue_job gated by a single global allow_enqueue bit
crates/server/src/routes/query.rs:531 - durable governance audit trail precedent
docs/features/peering.md:251-253, docs/features/events-webhooks.md:276 - surfaces deferring to 'the API-key story'
```

## HA2 — Analytical query plane: read-only SQL/DuckDB over datasets, Parquet export, typed columns

- merged into **N08** (Analytical plane: SQL over datasets and revisions with cross-app derived joins)
- lens: `moonshot-architect` · size: **XL** · gate: **policy** · effort 8 / impact 9 / risk 5
- contexts: dataset-api, job-search-api, automation-api
- extends: M11 derived datasets + M20 declared data contracts + the `?filter=` grammar; 'new' as a query language

### Summary
Replace 'export everything and filter client-side' with a real analytical surface: a sandboxed read-only SQL endpoint (SQLite JSON views or an embedded DuckDB reader over the record store), Parquet/Arrow export, and typed generated columns + indexes derived from declared contracts - exposed over REST and as an MCP tool.

### Description
The dataset read surface is a five-operator predicate grammar: `eq | contains | gte | lte | numgte`, ANDed only, no OR, no sort, no projection, no joins (`crates/server/src/routes/datasets.rs:126-219`). The MCP `query_dataset` tool exposes exactly that grammar to agents (`crates/server/src/mcp/mod.rs:152-170`). Every richer question has been answered by hand-building a route: `/grants` exists because 'every consumer had to export the whole corpus and filter client-side' (`crates/server/src/routes/query.rs:16-25`), `/grants/closing-soon` needed a bespoke `list_filtered_ordered` (`query.rs:220-238`), and the doc admits the performance stance is a full-partition `json_extract` scan with 'generated column + index' as the escape hatch nobody has pulled (`docs/features/http-api.md:169`). Derived datasets aggregate only `count` and `sum($.path)` (`crates/server/src/routes/derived.rs:56-61`). Export speaks json/ndjson/csv (`datasets.rs:665-690`) and the feature doc lists 'No Parquet export' as a known gap (`docs/features/datasets.md:247`). Search carries document-level `amount`/`event_date` only (`docs/features/search.md:284-285`).

The substrate makes this cheap: records are one `(app, dataset, key, data JSON, updated_at, removed_at, trust)` table already queried through `JsonFilter` SQL pushdown; declared contracts (M20, `query.rs:356-375`) already name required fields and types per source, which is exactly the input a generated-column planner needs; provenance/trust columns exist for lineage-aware views. The user's sibling project measured DuckDB at 43x on OLAP over the same class of medallion data (session memory: Politicas DB architecture guide), so an embedded DuckDB reader over the SQLite file (or a Parquet mirror) is a known-good direction. Registry: `search` and `sync-replication` subjects touch this; the policy constraint is the classic one for a SQL door (`rate-limiting`): read-only connection, statement timeout, row/byte caps, allow-list of views, never the jobs/secrets tables.

### Flow
- `GET /datasets/{app}/{ds}/export?format=parquet` (Arrow writer streaming row groups; constant memory like the existing `stream_export`)
- Typed columns: from each `[source.contract]` field declaration, `ALTER TABLE records ADD COLUMN <app>__<field> GENERATED ALWAYS AS (json_extract(data,'$.<field>')) VIRTUAL` + index, planned by a `just reindex`-style bin and reported on `/datasets/doctor`
- `POST /query` `{sql, params, max_rows, timeout_ms}` on a dedicated read-only SQLite connection (`PRAGMA query_only`, authorizer callback allow-listing `records`/`record_revisions`/derived views, progress-handler deadline), returning `{columns, rows, truncated, plan}`; `EXPLAIN` free
- Optional `[query] engine = "duckdb"` feature: DuckDB with the sqlite scanner or the Parquet mirror for aggregations
- MCP tool `sql_query` sharing the same builder/clamps the way `run_search` is shared (`crates/server/src/routes/search.rs:166-207`)
- Saved SQL views as derived datasets (`/derived` accepts `sql` as an alternative to filters/project), so a view's deltas feed watches/triggers like M13's materialized searches
- Docs: `datasets.md` section Query plane; `http-api.md` route table; `EXPECTED` inventory

### Expected impact
Agents and products stop round-tripping whole corpora; questions like 'total award ceiling by agency for grants closing this quarter' become one call instead of a bespoke route. Measured by: export bytes per analytical question (whole-corpus today), p95 of `/query` under the row cap, and the count of hand-built curated routes that stop growing (`/grants` is the only one today). What could break: a runaway query holding the reader; the timeout + row cap + `query_only` connection are the containment.

### Evaluation
Claim: user - arbitrary read-only analytical questions over any dataset without exporting it
Before: 5 filter ops, AND-only, 2 aggregate kinds (count/sum), 3 export formats, 1 hand-built curated route (`/grants`) for the one corpus that needed more
After: SQL over allow-listed views with a deadline and row cap; Parquet export; typed columns for contracted fields
Method: probe - enumerated the filter grammar (datasets.rs:159-215), the derived aggregate vocabulary (derived.rs:59-61), the export formats (datasets.rs:665-690)
Result: unmeasurable (instrument: a benchmark comparing `/grants?min_award=` full-scan latency vs the same predicate over a generated column, and a `/query` smoke with an aggregate the filter grammar cannot express)
Gate: policy

### Evidence

```
crates/server/src/routes/datasets.rs:126-219 - parse_filters: eq|contains|gte|lte|numgte, ANDed
crates/server/src/mcp/mod.rs:152-170 - query_dataset tool = same grammar
crates/server/src/routes/query.rs:16-25 - /grants exists because consumers had to export+filter client-side
crates/server/src/routes/query.rs:220-238 - bespoke ordered read for closing-soon
docs/features/http-api.md:169 - full json_extract scan; 'generated column plus an index' escape hatch
crates/server/src/routes/derived.rs:56-61 - aggregates: count | sum($.path) only
crates/server/src/routes/datasets.rs:665-690 - ExportFormat {Json, Ndjson, Csv}
docs/features/datasets.md:247 - 'No Parquet export'
crates/server/src/routes/query.rs:356-375 - declared contracts per source (typed field input)
crates/server/src/routes/search.rs:166-207 - run_search shared REST/MCP renderer pattern to copy
```

## HA3 — Pumper Mesh: signed node identity, scheduled peer sync of datasets, host weather and recipes

- deck item **N16**
- lens: `integration-planner` · size: **XL** · gate: **contract** · effort 8 / impact 8 / risk 6
- contexts: job-search-api, dataset-api, automation-api
- extends: M01 host-weather v1 (export/import), M30 dataset peering (puller app), M17 remote fetch fabric, M05 API recipes - v1s are manual, unsigned, unscheduled and one-hop

### Summary
Turn three separate manual point-to-point features into one mesh: a node identity with signed bundles, a server-side `[[peer]]` config with scheduled pulls, and one sync protocol that moves dataset revisions, host weather AND discovered API recipes between nodes, with a reconcile pass that removes the ghosts hard deletes leave behind.

### Description
Each v1 names its own next slice and they are the same slice. Host weather: 'There is NO federation service and NO auto-sync; peer URLs, scheduled pulls, signatures, and faster decay of imported state are the documented next slice' (`crates/server/src/routes/host_weather.rs:5-12`); `node_id` is a hash of the database path and 'Not a security boundary (nothing is signed in v1)' (`host_weather.rs:48-56`); the bundle schema string is versioned precisely so a v2 can change it (`host_weather.rs:36-38`). Peering: 'No auth', 'Hard deletes leave ghosts... There is no reconcile pass', 'No scheduling of its own... A server-side `[[peer]]` config block is the documented next slice, not built', 'One direction, one hop' (`docs/features/peering.md:251-266`). The remote fabric is already a node-to-node surface with a shared secret and target policy (`crates/server/src/routes/remote.rs:10-28`, wired only when `[remote] enabled` and nodes are configured, `crates/server/src/state.rs:328-330`). Recipes are host-level intelligence exactly like weather but have no export/import at all and the table 'stays empty until a discovery caller ships' (`crates/server/src/routes/recipes.rs:6-10`) - a mesh lets one node's discovery feed every node's fetcher.

Why v1 is insufficient: three secrets (none for peering, one cluster secret for fabric, none for weather), three transports, no way to tell a bundle forged by a hostile peer from a real one, and a mirror that silently serves records the origin deleted. The wire shapes to reuse are already pinned: the revision feed's provenance fields are pinned by name for the peer app (`crates/server/src/routes/datasets.rs:1407-1458`), cursor-mode paging is the transport (`peering.md:36-39`), and the weather merge is conservative by construction (`host_weather.rs:143-150`). Registry: `sync-replication` (dataset-api) and `stream-proxy-hop` (job-search-api) are the governing subjects.

### Flow
- Node identity: ed25519 keypair generated at first boot under `data/`, public key on `GET /node`; `node_id` becomes the key fingerprint
- Signed envelopes: `{schema, node_id, generated_at, payload, sig}` for weather bundles (schema `pumper.host-weather/2`), recipe bundles (`GET/POST /recipes/export|import`, same dry-run-by-default shape as weather), and a feed manifest for datasets
- `[[peer]] url, public_key, pull = ["datasets:grants/*", "weather", "recipes"], every = "15m"` - the scheduler runs pulls as ordinary jobs (`peer` app + weather/recipe import), so budgets, receipts and the decision ledger apply unchanged
- Trust policy per peer: which namespaces it may write, max severity for imported penalties (reuse the 60s cap), faster decay of imported state
- Reconcile pass: origin publishes a keyed live-set digest per dataset (`GET /datasets/{app}/{ds}/manifest` = count + rolling hash over keys); a mirror whose digest differs runs a bounded key diff and tombstones ghosts - closes the hard-delete gap
- `GET /mesh` status: peers, last pull per stream, lag, signature failures; `pumper_mesh_*` series on `/metrics` emitted at zero like the egress counters (`crates/server/src/routes/health.rs:181-212`)
- Docs: `peering.md` rewritten as `mesh.md`; `fetching.md` remote + host weather cross-linked

### Expected impact
A fleet of pumper nodes (dev laptop, a VPS, a geo egress node) behaves as one learning system: politeness lessons, discovered JSON APIs and canonical datasets propagate on a schedule, and a mirror can be trusted to be complete. Measured by: cold-start fetch strikes on a fresh node before/after importing a signed bundle; mirror ghost count after a reconcile (today unbounded); hosts with recipes on a node that never rendered them (0 today). What could break: a peer with a compromised key can push bad weather - the severity caps and per-peer namespace allow-list bound the blast radius.

### Evaluation
Claim: resilience - every node shares what it learned, verifiably, without an operator curling bundles by hand
Before: 0 signed bundles; weather import is a manual POST; peering runs only when someone POSTs a peer job; ghosts after origin hard-delete are never reconciled; recipes never leave the node
After: scheduled signed pulls for three streams; ghost count converges to 0 after a reconcile tick
Method: probe - read the v1 seams and their own 'next slice' notes (host_weather.rs:5-12, peering.md:251-266, recipes.rs:6-10)
Result: unmeasurable (instrument: two-node e2e extending `crates/server/src/e2e/peer_mirror.rs` with a forged bundle refused and a hard delete reconciled)
Gate: contract

### Evidence

```
crates/server/src/routes/host_weather.rs:5-12 - 'NO federation service and NO auto-sync; peer URLs, scheduled pulls, signatures ... documented next slice'
crates/server/src/routes/host_weather.rs:48-56 - node_id = hash of DB path, 'nothing is signed in v1'
crates/server/src/routes/host_weather.rs:36-38 - versioned schema string reserved for v2
docs/features/peering.md:251-266 - no auth; ghosts, no reconcile; no [[peer]] config; one direction one hop
crates/server/src/routes/remote.rs:10-28 - fabric guardrails (secret, target policy, profile policy)
crates/server/src/state.rs:328-330 - remote engine wired only with enabled + nodes
crates/server/src/routes/recipes.rs:6-10 - recipes have no sharing; table empty until discovery ships
crates/server/src/routes/datasets.rs:1407-1458 - provenance fields pinned on the wire for the peer app
crates/server/src/routes/health.rs:181-212 - egress counters emitted at zero (pattern for mesh metrics)
```

## HA4 — Bitemporal dataset reads: as-of snapshots and interval diffs over the revision store

- merged into **N06** (Dataset time machine: as-of reads, interval diffs and lifespan analytics over revisions)
- lens: `feature-scout` · size: **L** · gate: **none** · effort 5 / impact 7 / risk 3
- contexts: dataset-api, automation-api
- extends: the change feed + per-record history (datasets change intelligence) and M12 provenance; M10/M42 time-travel the archived bodies and rules, this time-travels the records

### Summary
Let any consumer read a dataset as it was at instant T (`?as_of=`) and get the set difference between two instants (`/diff?from=&to=`) - full post-images are already stored per revision, so this is a query capability, not a new store.

### Description
Every revision already carries the full record snapshot: `Revision { data: Option<Value> /* Full record snapshot at this revision (None for 'removed') */, diff, created_at, trust, provenance }` (`crates/core/src/datasets.rs:142-161`). The read surface exposes that history only two ways: newest-first per key (`GET .../history?key=`, `crates/server/src/routes/datasets.rs:1067-1108`) and a newest-first feed of all revisions since an instant (`.../changes`, `datasets.rs:997-1054`). Neither answers 'what did this dataset look like last Tuesday' or 'what changed between the two grant sweeps' without a client replaying the whole feed. A whole-dataset revision walk already exists internally - `dataset_revisions_page` is used by the pre-delete export (`datasets.rs:557-578`, `crates/core/src/datasets.rs:1687`) - so the as-of reconstruction is one query: for each key, the latest revision with `created_at <= T` (then drop keys whose latest is `removed`). The horizon is bounded honestly by `[storage] revision_retention_days` (`crates/server/src/main.rs:594-600`), so the response must carry `horizon` and refuse an `as_of` older than the oldest retained revision rather than serve a partial world.

Who needs it: the SDK's cold start is 'current state only' and its watermark boundary caveat (`docs/features/sdk-typescript.md:294-296`) disappears when a product can request the snapshot at its own watermark; triggers could fire on 'diff since last successful run' rather than per-revision; the `/grants/closing-soon` view could be replayed for any past day for backtesting a Ledgerline/Politicas model; provenance (`crates/server/src/routes/provenance.rs:56-138`) gets a dataset-level counterpart. Registry subject: `public-claim-provenance` (dataset-api) - a claim made on day D should be reproducible against the corpus as of D.

### Flow
- Core: `Datasets::snapshot_as_of(app, ds, at, after, limit, trust, filters)` - keyset-paged, SQL over `record_revisions` taking max(revision) per key with `created_at <= ?`; `Datasets::diff_between(app, ds, from, to)` -> `{added, changed[{key, diff}], removed}` streamed
- Routes: `GET /datasets/{app}/{ds}?as_of=` (same dual-mode shape; `filter=`/`trust=` honoured via the existing JsonFilter pushdown against the reconstructed row), `GET /datasets/{app}/{ds}/export?as_of=`, `GET /datasets/{app}/{ds}/diff?from=&to=` (json/ndjson streaming like export)
- Honesty: response carries `{as_of, horizon: <oldest retained revision>, complete: bool}`; `as_of < horizon` -> 409 naming the retention key; `/datasets/doctor` gains an `as_of_horizon` per dataset
- SDK: `exportRecords({asOf})` and `diff()`; MCP `query_dataset` gains `as_of`
- Trigger source kind `dataset` gains `on_change: "diff"` with the interval since the trigger's last successful hop
- Docs: `datasets.md` section Time travel; `http-api.md` table; `EXPECTED` inventory

### Expected impact
Downstream products can backtest and audit ('what did we know when'), mirrors can start at any instant, and triggers can act on net change rather than revision noise. Measured by: the number of feed pages a consumer must walk to reconstruct T (today: all pages since T), and diff correctness against a replayed feed. What could break: revision pruning makes old `as_of` answers partial - the horizon field and 409 are the guard.

### Evaluation
Claim: user - point-in-time and interval reads of any dataset
Before: history is per-key newest-first (500 clamp uncursored) or a whole-feed walk; no as-of, no diff endpoint; the SDK replays the feed client-side
After: one paged call returns the dataset as of T with `complete: true`; one call returns added/changed/removed between two instants
Method: simulation - traced how a consumer would reconstruct T today: `/changes?cursor=` full walk then client-side max-by-key, O(revisions) per question
Result: unmeasurable (instrument: a property test that `snapshot_as_of(now)` equals `list_records_view` for a dataset with pruning off, and that `diff_between(a,b)` composes with `snapshot_as_of(a)` to give `snapshot_as_of(b)`)
Gate: none

### Evidence

```
crates/core/src/datasets.rs:142-161 - Revision carries full `data` snapshot, diff, created_at, trust, provenance
crates/server/src/routes/datasets.rs:1067-1108 - record_history: per-key only
crates/server/src/routes/datasets.rs:997-1054 - dataset_changes: feed since an instant, no as-of
crates/server/src/routes/datasets.rs:557-578 + crates/core/src/datasets.rs:1687 - whole-dataset revision walk exists (dataset_revisions_page)
crates/server/src/main.rs:594-600 - revision retention is opt-in; defines the horizon
docs/features/sdk-typescript.md:294-296 - watermark boundary caveat
crates/server/src/routes/provenance.rs:56-138 - per-record chain, the record-level counterpart
```

## HA5 — Consumer plane v2: typed response schemas, generated SDKs and CLI, live dataset stream

- deck item **N23**
- lens: `integration-planner` · size: **L** · gate: **contract** · effort 6 / impact 8 / risk 3
- contexts: api-surface, dataset-api, job-search-api
- extends: openapi-spec + M29 MCP + @pumper/sync (SDK Tier 1 of the 10-20-product plan); v1 spec types only request bodies and the SDK is hand-mirrored TypeScript

### Summary
Make the OpenAPI document the whole contract - typed response schemas for every route - and generate the TypeScript SDK, a Python twin, a Rust client and a `pumper` CLI from it in CI, plus one thing no generator can add: a resumable SSE stream of dataset revisions so UIs and agents get change intelligence without a webhook receiver.

### Description
The spec is generated from the router and gated by an EXPECTED inventory (`crates/server/src/routes/mod.rs:540-676`), but `docs/features/http-api.md:5` concedes 'response bodies are described inline; the ad-hoc JSON envelopes are documented in prose per endpoint'. Nearly every handler returns `Json<Value>` and describes its shape in a backtick string - `jobs.rs:193`, `datasets.rs:232`, `triggers.rs:519`, `health.rs:46-67` - with errors typed as `body = Object`. Consequences are recorded: the SDK's 'Types are hand-mirrored, not generated - regenerate against GET /openapi.json if the record/revision shapes drift', 'TypeScript only so far - a Rust/Python twin would generate off the same OpenAPI spec', 'No built-in retry/backoff' (`docs/features/sdk-typescript.md:297-303`), and the fixture-conformance test in `datasets.rs:1268-1279` exists precisely because nothing generates the types (it caught the regression class where the SDK 'silently mirrors nothing'). Agents already consume the derived tool definitions (`meta.rs:1001-1010` `?format=tools`; `docs/features/mcp.md:52-62`), so a typed spec also tightens MCP output contracts.

The live half: `/events` streams job status transitions only (`crates/server/src/routes/events.rs:16-28`); dataset changes reach a consumer only through watch sinks (webhook/slack/file, `watches.rs:290-298`), which needs a public receiver. The revision feed's cursor `<created_at>|<rowid>` (`datasets.rs:19-34`) is already a monotonic resume token - the same `Last-Event-ID` idea `/events` implements with a ring (`events.rs:177-225`) can be backed by the durable `record_revisions` table instead, so a stream resumes after any restart. Registry: `sync-replication` and `docs-sync` (the doc-sync hook already enforces spec/doc coupling for this group).

### Flow
- Typed DTOs: `#[derive(Serialize, ToSchema)]` structs for every response envelope (Job, Record page, Revision page, Schedule+health, Trigger, Delivery, Receipt, Economics, Sources, Doctor...) registered as `components.schemas`; a test asserts every 200 response in the spec references a schema (extend `spec_covers_exactly_the_registered_routes`)
- Generation in `just sdk`: openapi-typescript for `clients/typescript` (replacing the hand types, keeping `PumperSync`), a `clients/python` (`pumper-sync`) with the same watermark loop, a `clients/rust` crate, and `clients/cli` (`pumper jobs ls`, `pumper datasets export`, `pumper triggers test`) - all with retry/backoff on the shared error `code` map (`error.rs:35-52`)
- `GET /datasets/{app}/{ds}/stream` (SSE, `Last-Event-ID` = revision cursor, `trust=`/`filter=` honoured, ends cleanly on shutdown via `next_or_shutdown`, `events.rs:80-89`); an MCP `subscribe_dataset` on the live channel
- CI: generated clients diffed against committed output so a shape drift fails the build; the fixture conformance test becomes a generated-vs-served check
- Docs: `sdk-typescript.md` becomes `sdks.md`; `http-api.md` typed-envelope section

### Expected impact
Every downstream product and agent framework gets a typed, retrying client in its language from one artifact; shape drift becomes a build failure instead of a silent `undefined`; dashboards get live change intelligence without exposing a webhook URL. Measured by: number of hand-written type declarations in `clients/` (all of them today), languages served (1), and time-to-first-mirror for a new product. What could break: consumers of the legacy bare-array shapes if envelopes are tightened - keep dual-mode, only add schemas.

### Evaluation
Claim: quality - response contracts are machine-checked end to end and clients are generated, not mirrored
Before: 0 typed response schemas in the spec (responses are prose descriptions with `body = Object`); 1 hand-written TypeScript SDK; no dataset SSE
After: every 200 response references a component schema; TS/Python/Rust/CLI generated in CI; `/datasets/{app}/{ds}/stream` resumable across restarts
Method: probe - grepped handler signatures (Json<Value>) and utoipa `responses(` descriptions across the group; read sdk-typescript.md known gaps
Result: unmeasurable (instrument: spec test counting responses without a schema ref - must reach 0 - and a generated-client drift check in `just ci`)
Gate: contract

### Evidence

```
docs/features/http-api.md:5 - 'response bodies are described inline; the ad-hoc JSON envelopes are documented in prose'
crates/server/src/routes/jobs.rs:193, datasets.rs:232, triggers.rs:519, health.rs:46-67 - responses described as backtick strings, `body = Object`
docs/features/sdk-typescript.md:297-303 - hand-mirrored types, TS only, no retry
crates/server/src/routes/datasets.rs:1268-1279 - fixture conformance test standing in for generation
crates/server/src/routes/mod.rs:540-676 - EXPECTED route inventory + spec validity test to extend
crates/server/src/routes/events.rs:16-28,80-89,177-225 - SSE is job-events only; resume/shutdown plumbing reusable
crates/server/src/routes/watches.rs:290-298 - dataset changes reach consumers only via webhook/slack/file sinks
crates/server/src/routes/meta.rs:1001-1010 - ?format=tools derived definitions
```

## HA6 — Workflows: declarative multi-step pipelines with fan-in joins and run-level receipts

- merged into **N03** (Workflow runs: declared multi-step plans with join barriers, templating and one receipt)
- lens: `innovation-catalyst` · size: **L** · gate: **contract** · effort 6 / impact 7 / risk 4
- contexts: automation-api, job-search-api
- extends: triggers (reactive one-hop edges, decision ledger, chain depth/cycle guard) + job receipts + M23 durable execution; v1 has no join, no run identity, no whole-pipeline status

### Summary
Add a workflow layer over the job queue: a named DAG of steps with fan-out, fan-in (`after: all_of [...]`), per-step params templates and one `run_id` that rolls up status, cost and yield into a single receipt - so an agent or a product can ask for 'refresh grants, then enrich details, then rebuild the derived view' as one unit instead of wiring three triggers and reconstructing lineage by hand.

### Description
Today's automation is edges, not graphs. A trigger is one source kind (`dataset | job | external`) to one `target_app` (`crates/server/src/routes/triggers.rs:93-128`); the fire path guards cycles and depth over a chain (`triggers.rs:369-380`) and jobs carry `schedule_id`, `trigger_id`, `source_job_id` lineage (`crates/server/src/routes/jobs.rs:154-166`, `crates/core/src/job.rs:67-69`). There is no fan-in anywhere: nothing can fire 'when both the grants-gov and ca-grants sweeps have succeeded' (a grep of `triggers.rs` for join/fan-in/all_of finds nothing), so cross-source steps like the unified-corpus rebuild either run per upstream event (duplicated work) or on a cron guess. Observability is per node: `GET /jobs/{id}/receipt` is one run's cost + stage timings (`crates/server/src/routes/mod.rs:200`, `http-api.md:47`), `GET /triggers/{id}/runs` is one edge's fires and decisions (`triggers.rs:502-551`), and provenance joins one record back to one job (`crates/server/src/routes/provenance.rs:73-97`). Answering 'did last night's pipeline complete, what did it cost, what did it change' means walking `source_job_id` links by hand.

The substrate is ready: `EnqueueOptions` already carries lineage fields, `validate_app_params` is the single params door every creator uses (`jobs.rs:147`, `triggers.rs:474-477`), the decision ledger vocabulary (`triggers.rs:505-511`) is the right shape for step decisions, budgets replay per edge (`triggers.rs:108-115`), and M23 checkpoints make a step resumable. Registry: `job-coordination` (job-search-api) and `fleet-orchestration` govern the design; M25 (trigger DAG topology to DataHub) becomes richer when a real DAG exists.

### Flow
- Migration: `workflows` (id, name, spec JSON, enabled) and `workflow_runs` (run_id, workflow_id, status, started/finished, step states JSON); `jobs.workflow_run_id`, `jobs.step`
- Spec: `{steps: [{name, app, params (template with `{{steps.X.result...}}`), after: {all_of|any_of: [...]}, budget_usd, max_attempts, foreach?: "$.path"}]}`; validated at create against each app's schema through `validate_app_params` (422 with pointer paths, same as every door)
- `POST /workflows`, `POST /workflows/{id}/run` (idempotency key), `GET /workflows/{id}/runs`, `GET /workflow-runs/{run_id}` (step matrix + rolled-up receipt: cost from `cost_events` by job set, yield from `job_yield`), `DELETE /workflow-runs/{run_id}` (cancel remaining steps via the existing cancel tokens), `POST /workflow-runs/{run_id}/retry` (from the failed step)
- Engine: a step becomes enqueueable when its `after` set is satisfied - evaluated on the terminal-event fan-out the trigger engine already runs on; cycle/depth checks reuse `decide()`
- Triggers and schedules can target a workflow (`target_workflow`) as well as an app; MCP tools `run_workflow`/`wait_workflow` beside `wait_job`
- Docs: new `docs/features/workflows.md`; `triggers.md` cross-link; `EXPECTED` inventory

### Expected impact
Cross-source pipelines (grants unified corpus, trades join, Politicas-style medallion refreshes) get one run id, one status, one bill and one retry point; agents compose multi-step work without hand-wiring triggers. Measured by: triggers replaced by a workflow spec (edges per pipeline today), duplicate downstream runs per upstream batch (>1 today for fan-in shapes), and time to answer 'did the nightly pipeline finish' (manual lineage walk today). What could break: a workflow that re-enqueues on every partial fan-in satisfaction - the `all_of` gate must be evaluated exactly once per run via the ledger.

### Evaluation
Claim: user - multi-step pipelines with joins are first-class, with a single run identity and receipt
Before: 0 fan-in capability; lineage is `source_job_id` links; cost/yield receipts are per job; a 3-step cross-source refresh needs 2+ triggers and duplicate runs
After: one spec, one run_id, one rolled-up receipt; joins fire once
Method: probe - read trigger creation/fire/ledger code and job lineage fields; searched triggers.rs for any join semantics (none)
Result: unmeasurable (instrument: an e2e that runs a 3-step diamond workflow - two sources, one join - and asserts the join step ran exactly once with a receipt summing both upstream costs)
Gate: contract

### Evidence

```
crates/server/src/routes/triggers.rs:93-128 - CreateTriggerBody: one source kind -> one target_app
crates/server/src/routes/triggers.rs:369-380 - decide(): cycle/depth over a chain, no join
crates/server/src/routes/jobs.rs:154-166 + crates/core/src/job.rs:67-69 - schedule_id/trigger_id/source_job_id lineage on EnqueueOptions
crates/server/src/routes/triggers.rs:502-551 - per-trigger runs + decision ledger
crates/server/src/routes/mod.rs:200 + docs/features/http-api.md:47 - per-job receipt only
crates/server/src/routes/provenance.rs:73-97 - record -> single job lineage join
crates/server/src/routes/jobs.rs:147, triggers.rs:474-477 - validate_app_params, the one params door to reuse
```

