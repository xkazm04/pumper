# Event Pipeline — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## EP1 — Durable event log with cursor subscriptions: one outbox for every event kind

- deck item **N05**
- lens: `moonshot-architect` · size: **XL** · gate: **contract** · effort 8 / impact 9 / risk 5
- contexts: webhook-delivery, trigger-pipeline, datahub-bridge
- extends: M21 (ingress) + M22 (sinks) + the SSE replay ring (events.rs); v2 of the bus

### Summary
Persist the event bus to SQLite as an append-only, restart-stable log (`events` table, durable `seq`), and replace the four hand-wired push paths (per-job `callback_url`, `watches`, saved-search alerts, `[webhooks] failure_url`) plus the two pull surfaces (SSE ring, MCP live bridge) with ONE subscription model: a `subscriptions` row = (event selector, sink, cursor). Every consumer -- a receiver, a peer pumper, an agent, `@pumper/sync` -- reads or is pushed from a durable cursor, so nothing is ever `reset` and every event kind gets the DLQ/replay machinery that today only four kinds have.

### Description
The bus is in-memory only: `EventBus` keeps the last 1024 events under a 32 MiB byte budget and evicts past either bound (crates/server/src/events.rs:106-116, 163-189); `seq` restarts at 0 on boot (events.rs:141) and a client whose gap fell out of the ring is told `reset` (crates/server/src/routes/events.rs:185-189). Inbound ingress events are emitted onto that ring and nowhere else (crates/server/src/routes/ingress.rs:358-362) -- their only durable trace is a `trigger_runs` row if a trigger happened to match, and `datahub_govern` audit events ride the same volatile bus (crates/server/src/datahub.rs:1433-1449). Durable, at-least-once delivery exists, but only for four hand-wired kinds, each with its own secret-resolution branch (crates/server/src/webhook.rs:501-531) and its own trigger site in the worker (`notify_watches` crates/server/src/worker.rs:1633-1680; `dispatch_failure` webhook.rs:606-625). A `job.succeeded` for *any* app, a `trigger fired/skipped` decision, a contract verdict, a governance action, or an ingested external event cannot be subscribed to at all; the receipt route states outright that watch deliveries cannot even be attributed to a run (crates/server/src/routes/receipt.rs:24-26). The consumer SDK compensates by polling change feeds on a watermark (docs/features/sdk-typescript.md `changesPage`), and peering is 'puller only -- no push' (docs/features/peering.md:3-4). The MCP live stream inherits the same volatility (crates/server/src/mcp/live.rs:5-13).

The substrate already has every piece of an outbox: a stable `delivery_id` idempotency key, HMAC over `{ts}.{id}.body`, a claim-then-send drain, a `dead` state and manual replay (webhook.rs:596-631, 776-786), plus the ingress side that verifies the same base (webhook.rs:779-780). What is missing is the log and the cursor. Registry subjects that apply: `webhook-ingestion` and `delivery-guarantees` (both mapped to this context in .ai/registry-map.json) -- a durable outbox with consumer cursors is the golden path those subjects describe.

### Flow
- Migration: `events(seq INTEGER PRIMARY KEY, kind, app, subject_id, payload JSON, created_at)`; `EventBus::emit` writes the row under the ring lock and the ring becomes a read-through cache; `seq` is seeded from `MAX(seq)` at boot so `Last-Event-ID` survives a restart.
- `GET /events?after=<seq>&kind=&app=&limit=` (JSON keyset page) beside the SSE route; SSE/MCP resume from the table when the ring misses instead of emitting `reset`; retention knob `[storage] event_log_retention_days`.
- Migration: `subscriptions(id, selector JSON {kinds[], app, dataset, filters[]}, sink, url, secret, cursor_seq, enabled)`; port `watches`, job callbacks, saved-search alerts and `failure_url` onto it (keep the old routes as thin adapters for one release).
- A single outbox drain on the scheduler tick: for each enabled push subscription, read events past `cursor_seq`, dispatch through the existing `dispatch_event` -> `deliver` path (one delivery row per event), advance the cursor only on `delivered`.
- Reuse the ingress verify path so a peer pumper can subscribe with sink `pumper:<url>/ingest/<source>` -- push federation on top of M30's pull.
- `@pumper/sync` gains `subscribe(cursor)` beside the watermark poll; MCP gains `events` as a resource with a cursor argument.

### Expected impact
Operators stop losing events on restart and stop rebuilding state after a `reset`; every event kind becomes subscribable with the same DLQ semantics; a fleet of agents or downstream products can each hold an independent cursor. Measured by: zero `reset` events emitted across a restart under load, and the count of event kinds reachable by a durable subscription (4 -> all). What could break: the write amplification of one row per event on the hot path (must batch under WAL) and the migration of `watches` semantics onto selectors.

### Evaluation
Claim: resilience - every event survives a restart and is consumable from a durable cursor
Before: 1024-event / 32 MiB in-memory ring, seq resets to 0 on boot, `reset` emitted when the gap is evicted; 4 durable push kinds; ingress events have no durable record
After: N events retained per retention knob, `Last-Event-ID` valid across restarts, all kinds subscribable; instrument = count of `reset` SSE events and of subscription kinds
Method: probe - read events.rs, routes/events.rs, ingress.rs, webhook.rs dispatch sites and the receipt attribution gap
Result: unmeasurable (moonshot) - the instrument is a restart-under-load e2e counting `reset` events plus `SELECT COUNT(*) FROM events`
Gate: contract

### Evidence

```
crates/server/src/events.rs:106-116 (ring bounded by count AND bytes, in-memory), :141 (seq starts at 0), :163-189 (evict on emit)
crates/server/src/routes/events.rs:185-189 (Replay::Reset -> `reset` event)
crates/server/src/routes/ingress.rs:358-370 (external event emitted to bus only, then triggers)
crates/server/src/webhook.rs:501-531 (resolve_secret: exactly four kinds), :596-631 (drain_due claim/replay), :779-780 (sign shared with ingress verify)
crates/server/src/worker.rs:1633-1680 (notify_watches builds payload + dispatch_change per watch)
crates/server/src/routes/receipt.rs:24-26 (watch/saved-search deliveries cannot be attributed to a run)
crates/server/src/mcp/live.rs:5-13 (MCP live bridge inherits ring reset semantics)
docs/features/peering.md:3-4 (puller only, no push); docs/features/sdk-typescript.md (watermark polling)
```

## EP2 — Pipeline runs: root correlation id, fan-in barriers, end-to-end SLA across trigger hops

- merged into **N03** (Workflow runs: declared multi-step plans with join barriers, templating and one receipt)
- lens: `innovation-catalyst` · size: **XL** · gate: **contract** · effort 8 / impact 8 / risk 6
- contexts: trigger-pipeline, datahub-bridge, webhook-delivery
- extends: trigger-decision-ledger (trigger_runs) + M25 (DataHub flows) + M11 (derived DAGs); v2 of the reactive-pipeline model

### Summary
Give every reactive chain a first-class *run*: a host-owned `root_id` stamped at the chain's origin (schedule tick, manual enqueue, or ingested event) and carried through every hop, so the system can answer 'did the whole crawl -> extract -> research -> alert pipeline finish for root event E, how long did it take, and where did it stall?'. On that identity, add the one edge type the DAG lacks -- a fan-in barrier that fires once N upstream edges have completed under the same root -- and emit the run to DataHub as a `dataProcessInstance` so the lineage graph shows executions, not only topology.

### Description
Provenance today is a list of trigger ids plus a depth (`provenance`/`decide`, crates/server/src/triggers.rs:29-62) and a per-hop reverse pointer `source_job_id` (triggers.rs:1389-1392). Both identify *edges*, not the *execution*: two ingested events an hour apart produce identical `chain` values, so there is no key on which to group the jobs of one end-to-end run, and the receipt lists only the immediate `trigger_hops` of one job (crates/server/src/routes/receipt.rs:41-49). The design explicitly excludes fan-in/join barriers and named pipeline grouping (docs/features/triggers.md:88) -- reasonable for v1, but it means a `research` stage that needs BOTH `extract` outputs cannot be expressed, and no surface can state a pipeline's latency or its completion. The decision ledger already records every fire/skip with `source_job_id` and `job_id` (triggers.rs:852-887, 1400-1408) and is age-bounded at 14 days (docs/features/triggers.md:72); the DataHub emitter already models a run as a `dataJob` under a flow keyed by schedule/trigger (crates/server/src/datahub.rs:217-239, 595-650) but has no execution-instance entity to hang a duration or a status on. The host-owned key mechanism (`HOST_OWNED_KEYS`, triggers.rs:221-233) is exactly the seam for an unforgeable `root_id`: a transform plugin cannot drop or forge it, so barrier and SLA logic can trust it. Registry subject `agent-chaining` (strong match for this context) is the pattern: chains need a run identity to be observable and joinable.

### Flow
- Add `root_id` (and `root_kind`: schedule|manual|external|govern) to `_trigger`, to `HOST_OWNED_KEYS`, and as an indexed `jobs.root_id` column; origin points: scheduler enqueue, `POST /apps/{name}/jobs`, `fire_external_triggers` (triggers.rs:1204 where `decide` is called with `Value::Null`), governance `EnqueueSync` (datahub.rs:1839-1845).
- `GET /pipelines/runs/{root_id}`: the tree of jobs + ledger decisions under one root, with per-hop and end-to-end wall-clock from `job_stages` (migration 0034) and a derived status (complete | stalled | failed-at <trigger>).
- New `source_kind = "join"` trigger: `sources: [{app, on_status}]`, `quorum: N|all`, `window_secs`; a `join_state(root_id, trigger_id, seen JSON)` table updated from `fire_terminal_triggers`; fires once with idempotency key `trig:{id}:{root_id}` and a merged `_trigger.inputs[]`.
- SLA: optional `deadline_secs` per join/edge; a sweeper on the reaper tick emits a `pipeline.stalled` event (consumable by card 1) and a ledger row `sla_missed`.
- DataHub: emit `dataProcessInstance` per root with start/end/status and the participating `dataJob` URNs (extends `flow_entities`).
- Trigger dry-run (`POST /triggers/{id}/test`) learns to simulate a whole root: walk edges from a chosen origin and report the would-fire tree.

### Expected impact
Pipeline authors get completion and latency as facts rather than as log archaeology; fan-in unlocks the multi-source research pattern the grants apps already need (three source apps feed `grants/unified`, datahub.md:449). Measured by: share of triggered jobs carrying a `root_id`, and p50/p95 end-to-end latency per pipeline from the new route. What could break: the join table is new state that must be reaped on root expiry, and `root_id` on `jobs` is a schema change every enqueue door must stamp.

### Evaluation
Claim: user - a pipeline's completion, latency and stall point are queryable per execution
Before: provenance = trigger-id chain + depth (no execution key); receipt shows one job's immediate hops; no fan-in edge; no pipeline latency figure anywhere
After: `GET /pipelines/runs/{root_id}` returns tree + end-to-end ms + status; join triggers fire on quorum; instrument = the route itself and a `pumper_pipeline_latency_seconds` histogram
Method: probe - traced `decide`, `HOST_OWNED_KEYS`, ledger rows, receipt `trigger_hops`, and the DataHub flow model
Result: unmeasurable (moonshot) - measurable once the route exists; today the figure cannot be computed from any table
Gate: contract

### Evidence

```
crates/server/src/triggers.rs:29-62 (provenance = chain of trigger ids + depth), :221-233 (HOST_OWNED_KEYS seam), :1204 (external chains start from Value::Null), :1389-1392 (source_job_id reverse lineage), :852-887 (ledger row shape)
docs/features/triggers.md:88 (non-goals: fan-in/join barriers, named pipeline grouping), :72 (ledger retention 14 days)
crates/server/src/routes/receipt.rs:41-49 (receipt lists only one job's trigger_hops)
crates/server/src/datahub.rs:217-239 (flow_identity), :595-650 (flow_entities: dataFlow/dataJob only, no process instance), :1839-1845 (governance enqueue = another chain origin)
docs/features/datahub.md:449 (three source apps feed grants.unified — the fan-in case)
```

## EP3 — Capability-scoped host imports: WASM sinks and connectors as third-party plugins

- deck item **N10**
- lens: `integration-planner` · size: **XL** · gate: **policy** · effort 9 / impact 8 / risk 7
- contexts: wasm-plugin-examples, webhook-delivery, trigger-pipeline
- extends: M15 (WASM everywhere, predicate/transform slots) + M22 (sinks: `plugin:<name>` seam named but out of v1)

### Summary
Let a plugin *do* something with the outside world under a declared, host-enforced capability manifest: `describe()` states `capabilities: { http: { hosts: [...], methods: [...] }, kv: true }`, the host links only those imports, meters them like fuel, and the two blocked seams open at once -- `sink = "plugin:<name>"` on a subscription/watch (reverse-ETL to Notion, Airtable, Postgres-over-HTTP, a Discord bot) and a third `post_enqueue` hook slot on triggers. This turns the plugin directory into a connector marketplace that runs inside the sandbox rather than as a sidecar process.

### Description
Plugins today 'declare no imports, so they have no filesystem or network access' (docs/features/trigger-plugins.md:268-270); a hook may only answer `{pass}` or reshape the envelope (crates/server/src/triggers.rs:420-546), and 'there is no post-enqueue hook' (trigger-plugins.md:298). The sink transport is a `match` on `webhook | slack | file` (crates/server/src/webhook.rs:152-196, 643-653) with the comment that a future `plugin:<name>` sink 'would resolve through the plugin host with the same (delivery_id, event, body) contract and report (delivered, attempts, last_error, permanent)' (webhook.rs:34-37, 638-641) -- the return tuple and the DLQ ladder already exist, only the transport is missing. The four shipped plugins are pure functions over an envelope (plugins-src/trigger-gate/src/lib.rs:240-276; plugins-src/delta-slim/src/lib.rs:84-95), and the host already has the safety rails a capability model needs: per-call fuel and memory caps, a shared admission gate, typed failure classes and a ledger vocabulary (`hook_trap`, `hook_host_error`; triggers.rs:394-411; trigger-plugins.md:262-282). What blocks a connector ecosystem is a single design decision -- zero imports -- which is the right default but the wrong ceiling.

### Flow
- Manifest: extend `describe()` with `capabilities`; the loader records them and `GET /plugins` shows them; a plugin that imports a host function it did not declare fails to link (`hook_not_executable`).
- Host imports (wasmtime `Linker`): `pumper_http_request(ptr,len) -> u64` (JSON `{method,url,headers,body}` in, `{status,body}` out), bounded by an allowlist of hosts from the manifest AND an operator allowlist in `[plugins] allow_http_hosts`, routed through the existing metered `Fetcher` chokepoint so governor, host profiles and cost ledger see it; `pumper_kv_get/put` scoped to a per-plugin namespace table.
- Sink: `plugin:<name>` branch in `deliver` -> `plugins.run(name, envelope{delivery_id,event,body}, params)`; output contract `{delivered: bool, permanent?: bool, error?: string}` mapped onto the existing tuple so retries/DLQ/replay are unchanged.
- Trigger slot `post_enqueue` (fires after `enqueue_dedup` succeeds with the hop's job id) for side effects that must not gate the hop.
- Ship two reference connectors in plugins-src (e.g. `sink-notion`, `sink-postgrest`) with the same CI verification the trigger plugins have (trigger-plugins.md:109-117); document the capability threat model beside `[ingress]`'s.

### Expected impact
Reverse-ETL and notification targets stop being a Rust change per destination: a user drops a `.wasm` into `data/plugins/`, `POST /plugins/reload`, and creates a watch with `sink: "plugin:sink-notion"`. Measured by: number of sink kinds available without a server rebuild (3 -> unbounded) and connector deliveries flowing through the same DLQ metrics. What could break: this is the sandbox's first outbound capability, so SSRF, secret leakage into plugin memory and admission-gate starvation by slow HTTP calls are the risks; fail-closed on any undeclared import.

### Evaluation
Claim: other - destinations become plugins; the delivery machinery is reused unchanged
Before: 3 sinks hard-coded in `deliver`; plugins have zero imports; 2 hook slots; adding a destination = a Rust PR to webhook.rs
After: `plugin:<name>` sinks + `post_enqueue` slot; instrument = `GET /plugins` capability listing and `pumper_webhook_deliveries{sink="plugin:*"}`
Method: probe - read the sink match, the seam comments, the hook runner, the plugin ABI and the sandbox limits doc
Result: unmeasurable (moonshot) - a security review and a fuel/latency benchmark of one HTTP-calling plugin under the admission gate are the instruments
Gate: policy

### Evidence

```
crates/server/src/webhook.rs:34-37 (WASM sinks OUT of v1; seam described), :152-196 (dispatch_change sink match), :638-653 (deliver transport branch; `file://` only special case)
docs/features/trigger-plugins.md:268-270 (no imports => no fs/network), :298 (no post-enqueue hook), :262-282 (fuel/memory/admission + typed failure classes)
crates/server/src/triggers.rs:394-411 (hook_failure_outcome vocabulary), :420-546 (apply_plugin_hooks: predicate + transform only)
plugins-src/trigger-gate/src/lib.rs:240-276 and plugins-src/delta-slim/src/lib.rs:84-95 (pure envelope functions; no side effects possible)
docs/features/events-webhooks.md 'WASM (plugin:<name>) sinks are deliberately out of scope; the seam for them is the transport branch in webhook.rs::deliver'
```

## EP4 — Param binding and per-record fan-out: triggers an agent can author over MCP

- deck item **N04**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 6 / impact 7 / risk 4
- contexts: trigger-pipeline, wasm-plugin-examples, webhook-delivery
- extends: M21 (external triggers inline the payload) + M29/M03 (MCP server) — closes the gap between an ingested payload and a target's params

### Summary
Let a trigger *bind* target params from the event instead of only appending `_trigger`: a `bind` map of JSON pointers (`{"url": "/_trigger/payload/repository/html_url"}`) resolved by the host before the target-schema door, an optional `each` pointer that fans one event out into one job per array element (with per-element idempotency), and MCP tools (`create_trigger`, `test_trigger`, `list_trigger_decisions`, `create_subscription`) so an agent can wire a reactive pipeline end-to-end without a human writing curl.

### Description
`merged_params` inserts exactly one key, `_trigger`, over the static template (crates/server/src/triggers.rs:82-91); an external hop inlines the whole payload under `_trigger.payload` (triggers.rs:663-685). Apps read their own top-level params (`crawl` reads `url`), so the shipped GitHub-push example has to hard-code `"url": "https://acme.dev/docs"` (docs/features/ingress.md worked example, step 3) -- the event cannot steer the job. A transform plugin cannot fix this: its output *is* `_trigger` and is merged under that key only (triggers.rs:493-505, 1245), so it can shape the envelope but never lift a value into the target's params. `${...}` templating and per-record fan-out are listed as non-goals (docs/features/triggers.md:88), which is why every external integration today needs either a bespoke app that knows to read `_trigger.payload` or a fixed target. The enqueue door already validates the *resolved* params against the target's schema and records `bad_params` in the ledger (triggers.rs:1308-1342), so a binding failure would surface exactly where a bad template does. On the agent side, the MCP server exposes seven tools -- list_apps, query_dataset, search, wait_job, fetch_readable, deep_research, enqueue_job (crates/server/src/mcp/mod.rs:142-320) -- and none touch triggers, watches, ingress sources or the decision ledger; an agent can run a job but cannot make the system react. `POST /triggers/{id}/test` (docs/features/triggers.md:37-39) is the dry-run that makes agent authoring safe.

### Flow
- `Trigger.bind: Option<Map<String, String>>` (JSON-pointer into the resolved `{template, _trigger}` view); resolve in a pure `bind_params(template, trigger_obj, bind) -> Result<Value, BindError>` before `hop_params_pass_target_schema`; a missing pointer is a new ledger outcome `bind_miss`.
- `Trigger.each: Option<String>` (pointer to an array in the envelope): one hop per element, `_trigger.item` + `_trigger.item_index`, idempotency key `trig:{id}:{event}:i:{index}`, capped by `[triggers] fan_out_cap` with `fan_out_truncated` stated (the `keys_truncated` precedent, triggers.rs:93-105).
- Store both on the row (migration), validate at create (pointer syntax; `each` only where the kind can carry arrays), surface in dry-run output.
- MCP tools: `create_trigger` / `create_watch` (schema = the REST bodies), `test_trigger` (wraps the dry-run), `trigger_decisions` (ledger page), `create_ingress_source` (returns the secret once); resources: `pumper://triggers`, `pumper://triggers/{id}/runs`.
- Update `delta-slim`'s docs: shaping the envelope vs binding the params are now two distinct, documented powers.

### Expected impact
Any webhook-emitting system can drive any app with zero code: 'new row in Airtable -> research that company', 'GitHub push -> crawl the changed docs URL from the payload'. Agents become pipeline authors (create, dry-run, inspect why it didn't fire) instead of one-shot job runners. Measured by: external triggers whose params come from the payload (0 today, by construction) and MCP tool count (7 -> 12). What could break: a bind that resolves to a huge value lands in job params; cap bound sizes and keep the 256 KiB ingress cap as the ceiling.

### Evaluation
Claim: user - an event can steer the target job, and an agent can wire the edge
Before: `merged_params` inserts only `_trigger`; worked example hard-codes the crawl URL; 7 MCP tools, none for triggers/watches/ingress
After: `bind`/`each` on the row, `bind_miss` ledger outcome, +5 MCP tools; instrument = share of external triggers with a non-empty `bind`, dry-run success rate over MCP
Method: probe - read merged_params, external_trigger_obj, the schema door, the ingress example and the MCP tool table
Result: unmeasurable (moonshot) - the instrument is the ledger split (`fired` vs `bind_miss`) once binding exists
Gate: contract

### Evidence

```
crates/server/src/triggers.rs:82-91 (merged_params: template + `_trigger` only), :663-685 (external envelope inlines payload), :493-505 (transform output becomes `_trigger`), :1245 (external hop merge), :1308-1342 (schema door + `bad_params`), :93-105 (`keys_truncated` precedent for a stated cap)
docs/features/triggers.md:88 (non-goals: `${...}` templating, per-record fan-out), :37-39 (dry-run endpoint)
docs/features/ingress.md worked example step 3 (`"params": { "url": "https://acme.dev/docs" }` — static, payload cannot steer it)
crates/server/src/mcp/mod.rs:142-320 (seven tools; no trigger/watch/ingress tools)
```

## EP5 — Vendor-neutral lineage and quality push: OpenLineage runs plus assertions from Pumper's own verdicts

- deck item **N24**
- lens: `business-strategist` · size: **L** · gate: **none** · effort 6 / impact 6 / risk 3
- contexts: datahub-bridge, trigger-pipeline, webhook-delivery
- extends: M25 (DataHub topology) + M26 (governance pull) + M20 (data contracts) — the push half stops being DataHub-only and stops being freshness-only

### Summary
Split the emitter into a catalog-agnostic run/quality event model and two writers: the existing DataHub OpenAPI writer, and an OpenLineage `RunEvent` writer (START/COMPLETE/FAIL with input/output datasets and facets) that any Marquez, Dagster, Airflow, Atlan or OpenMetadata consumer accepts. At the same time push what Pumper already *knows* about quality -- contract verdicts, extraction-health state, trust tier, tombstone counts -- as assertion results and tags, so the governance loop (which today reads DataHub's assertions back) is closed on Pumper's own truth, and model the catalog's external upstream sources as entities so lineage no longer starts at Pumper.

### Description
Every entity is built for DataHub's v1 ingestion envelope (`entity`/`envelope`, crates/server/src/datahub.rs:66-74) and posted to `/openapi/entities/v1/` (datahub.rs:334-366) -- there is no intermediate model, so a second catalog means a second emitter. The run model is already OpenLineage-shaped: `flow_identity` (datahub.rs:217-239) is the OpenLineage *job*, `flow_entities` builds one run with input and output datasets (datahub.rs:595-650), `schema_metadata` and `dataset_profile` (datahub.rs:110-159) are the `schema` and `dataQualityMetrics` facets, and `rule_ops` (datahub.rs:267-278) is a `columnLineage` facet. What is pushed is freshness and shape only: contract verdicts are computed at the same fan-out choke point (crates/server/src/worker.rs:1558-1590) and held in memory (crates/server/src/routes/receipt.rs:18), extraction-health suppression drops datasets silently before hooks (worker.rs:1253-1259), and the emitter never mentions any of it. Meanwhile the governance poll reads `health[].type == ASSERTIONS` from DataHub (datahub.rs:1099-1105) -- assertions somebody else wrote. The docs list the gaps plainly: 'External upstream sources (catalog/data-sources.toml) are not modeled as DataHub entities -- lineage starts at Pumper's own datasets' and column lineage is `upstreamType: NONE` because 'the upstream is the fetched page, not a dataset' (docs/features/datahub.md:341, 458; datahub.rs:285-307). With source entities, that column lineage gets a real upstream.

### Flow
- Extract a `LineageEvent { run, job, inputs, outputs, facets }` built once in `on_job_success`/`full_sync`; the DataHub writer becomes a `From<LineageEvent>`.
- `[lineage] openlineage_url` (+ `namespace`, `api_key`): a writer posting OpenLineage 2.x `RunEvent`s (START at job start via the worker, COMPLETE/FAIL at terminal) with `schema`, `columnLineage`, `outputStatistics` (`rowCount`, new/changed/removed) and a custom `pumper` facet (job id, trigger chain, cost).
- Quality: contract verdicts -> `dataQualityAssertions` facet / DataHub `assertionRunEvent`; extraction-health state and trust tier -> `globalTags`; removals -> `operation` DELETE; governance audit rows -> DataHub incidents.
- Source entities: each `[[source]]` in catalog/data-sources.toml becomes `urn:li:dataset:(urn:li:dataPlatform:web,<slug>,<env>)` (and an OpenLineage input); `upstream_lineage_with_fields` points at it instead of `NONE`.
- `GET /datahub/status` gains `lineage.writers[]`; the governance poll can prefer Pumper-written assertions over remote ones.

### Expected impact
Pumper becomes a lineage-emitting node in whatever platform the buyer already runs, which is the difference between 'nice DataHub demo' and 'fits our data platform' in a procurement conversation. Measured by: catalogs supported (1 -> 2+) and the fraction of datasets carrying at least one Pumper-authored assertion result. What could break: two writers double the failure surface; keep the no-retry, self-healing posture (docs/features/datahub.md:454) and report each writer separately.

### Evaluation
Claim: quality - the catalog shows Pumper's verdicts, and any catalog can receive them
Before: one writer (DataHub OpenAPI); emitted aspects = properties, operation, profile, schema, lineage, flow/job; contract verdicts in memory only; external sources unmodeled; column lineage upstreamType NONE
After: OpenLineage RunEvents + assertion/tag facets from verdicts, health and trust; source entities; instrument = `lineage.writers[].ok/failed` counters and the assertion count per dataset in the catalog
Method: probe - read the entity builders, on_job_success, the govern_meta read path, enforce_contracts and the documented gaps
Result: unmeasurable (moonshot) - measurable against a Marquez quickstart the way M25 was verified against DataHub quickstart v1.6 (datahub.md:447-449)
Gate: none

### Evidence

```
crates/server/src/datahub.rs:66-74 (DataHub-only envelope), :110-159 (schema/profile aspects), :217-239 (flow_identity), :267-278 (rule_ops), :285-307 (fine-grained lineage with upstreamType NONE), :334-366 (post to /openapi/entities/v1/), :595-650 (flow_entities), :663-768 (on_job_success), :1099-1105 (governance reads remote assertions)
crates/server/src/worker.rs:1558-1590 (enforce_contracts computes verdicts at fan-out), :1253-1259 (suppress_unhealthy drops silently)
crates/server/src/routes/receipt.rs:18 ('the contract verdicts held in memory')
docs/features/datahub.md:341 (column lineage upstream is the page), :454 (no retry by design), :458 (external sources not modeled), :447-449 (verified against quickstart)
```

