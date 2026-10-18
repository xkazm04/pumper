# Wave 3 Design — Events, Consumers, Research (2026-09-02)

Five builders off `master` after wave 2 is merged. [DESIGN-WAVE-1.md](DESIGN-WAVE-1.md) §Shared
rules and §Shared surfaces apply unchanged. Wave-2 seams these build on: N10's `plugin:` sink +
capability manifest, N03's workflow runs and `root_id`, N09's component host. The merged code is
the contract — read `FIXES-WAVE-2.md` and the merged commits before designing.

## File-scope partition (HARD boundaries)

| Builder | Item | Owns | Must not touch |
| --- | --- | --- | --- |
| K | **N05** Durable event log | `crates/server/src/events.rs`, `crates/server/src/routes/events.rs`, `crates/server/src/subscriptions.rs` (new), `crates/server/src/routes/subscriptions.rs` (new), `crates/server/src/webhook.rs` (dispatch through subscriptions — keep N10's `plugin:` branch intact), `crates/server/src/worker.rs` (`notify_watches` → subscription outbox only), `crates/server/src/routes/watches.rs` (thin adapter), `crates/server/src/mcp/live.rs`, migration, `docs/features/events-webhooks.md`, `docs/features/sdk-typescript.md` (subscribe note), `clients/typescript` (subscribe(cursor) — additive) | `triggers.rs` (L owns), `datahub.rs` (N owns), `engine-*`, app crates |
| L | **N04** Param binding + fan-out | `crates/server/src/triggers.rs`, `crates/server/src/routes/triggers.rs`, `crates/server/src/mcp/mod.rs` (trigger/watch/ingress tools — append at END), migration, `docs/features/triggers.md`, `docs/features/ingress.md` (worked example uses `bind`), `docs/features/mcp.md`, `plugins-src/delta-slim` docs only | `worker.rs`, `webhook.rs`, `events.rs`, app crates |
| M | **N11** Index-time enricher hook | `crates/engine-search/src/**`, `crates/core/src/search.rs` (additive: entity field + enricher trait), `crates/core/src/plugin.rs` (an `enricher` plugin kind, additive), `crates/engine-wasm/src/**` (only the `enricher` export shape on the core-module host — NOT app_host.rs), `crates/server/src/bin/search-backfill.rs` (re-enrich flag), `plugins-src/enrich-money-date/` (new: the two shipped regexes as the reference enricher), `docs/features/search.md`, `docs/features/trigger-plugins.md` §kinds | `worker.rs`, `webhook.rs`, `triggers.rs`, app crates |
| N | **N24** OpenLineage + assertions | `crates/server/src/datahub.rs`, `crates/server/src/lineage/` (new: `LineageEvent` model + `datahub` + `openlineage` writers), `crates/server/src/routes/datahub.rs` (status gains `lineage.writers[]`), `crates/core/src/config.rs` (`[lineage]` appended), `crates/core/src/catalog.rs` (source URN helper, additive), `docs/features/datahub.md` | `worker.rs` beyond the two existing emitter call sites (touch only those lines), `webhook.rs`, `triggers.rs`, `events.rs` |
| O | **N25** Research as living KB | `crates/apps/research/**`, `crates/apps/connector-api-watch/src/lib.rs` (extract `summarize_change` into a core-facing helper ONLY if it can live in `crates/core/src/research_util.rs` — else leave it), `crates/core/src/app.rs` (additive helper only if unavoidable — say so), `catalog/data-sources.toml`, `docs/features/apps.md` §research, `docs/features/mcp.md` (`deep_research` returns dataset keys) | `worker.rs`, `triggers.rs`, `webhook.rs`, `mcp/mod.rs` beyond the `deep_research` result shape, `apps/extractor` |

Also assigned this wave (small carry-forwards from FIXES-WAVE-1 §Carry-forward):
- **K** dispatches `source.repair_promoted` / `source.rolled_back` from the repair app's result
  (item 5) through the new outbox.
- **M** adds `crates/core/src/recipes.rs` to the `fetching.md` globs in `feature-doc-map.json` (item 9).
- **O** wires the extractor `profile:` param door? **No** — `apps/extractor` is out of O's scope;
  that one goes to wave 4 with the extractor free.

## Item specs (v1 slices — do NOT exceed)

### K — N05 Durable event log with cursor subscriptions (XL, contract) — card EP1

**v1 slice.** `events(seq INTEGER PRIMARY KEY, kind, app, subject_id, payload JSON, created_at)`;
`EventBus::emit` writes the row under the ring lock (batched under WAL: one INSERT per emit is fine
at current volumes — measure and say so), the ring becomes a read-through cache, `seq` seeded from
`MAX(seq)` at boot so `Last-Event-ID` survives restarts; SSE/MCP-live resume from the table when the
ring misses instead of emitting `reset`. `[storage] event_log_retention_days` (default 7) pruned by
the existing janitor pattern. `GET /events?after=<seq>&kind=&app=&limit=` keyset page beside SSE.
`subscriptions(id, selector JSON {kinds[], app, dataset, filters[]}, sink, url, secret, cursor_seq,
enabled, principal_id)`; one outbox drain on the scheduler tick: for each enabled push subscription,
read events past `cursor_seq`, dispatch through the existing `deliver` path (one delivery row per
event, `plugin:` sinks included), advance the cursor only on `delivered`. Port `watches` onto it as a
thin adapter (a watch = a subscription with a dataset selector) — keep the old routes working for one
release, mark deprecated in the doc. Job `callback_url`, saved-search alerts and `failure_url` stay as
they are in v1 (state this). `POST/GET/DELETE /subscriptions`, `GET /subscriptions/{id}/deliveries`.
`@pumper/sync` gains `subscribe({cursor})` over `GET /events` polling (no SSE in the SDK yet).
**Out of v1:** push federation to peers (`pumper:` sink), migrating callbacks/failure_url, SSE in SDK.
**Gate to prove:** restart-under-emit e2e: zero `reset` events across a restart, `Last-Event-ID`
resumes from the table; a subscription with a `plugin:` sink delivers with the DLQ semantics of the
webhook sink.

### L — N04 Param binding and per-record fan-out (L, contract) — card EP4

**v1 slice.** `Trigger.bind: Option<Map<String, String>>` (JSON pointers into the resolved
`{template, _trigger}` view), resolved by a pure `bind_params(template, trigger_obj, bind) -> Result`
before `hop_params_pass_target_schema`; a missing pointer is the ledger outcome `bind_miss`.
`Trigger.each: Option<String>` (pointer to an array): one hop per element with `_trigger.item` +
`_trigger.item_index`, idempotency key `trig:{id}:{event}:i:{index}`, capped by `[triggers]
fan_out_cap` (default 50) with `fan_out_truncated` stated. Both on the row (migration), validated at
create (pointer syntax; `each` only where the kind can carry arrays), shown in dry-run output.
MCP tools: `create_trigger`, `create_watch`, `test_trigger` (wraps dry-run), `trigger_decisions`
(ledger page), `create_ingress_source` (returns the secret once) — all behind `[mcp] allow_enqueue`.
Update the ingress worked example to use `bind`.
**Out of v1:** `${...}` string templating, binding into nested arrays, MCP resources.
**Gate to prove:** `bind_miss_is_ledgered_not_enqueued`, `each_fans_out_capped_and_says_so`,
dry-run shows the bound params; an ingress e2e where the payload steers the target URL.

### M — N11 Index-time enrichment as a plugin hook (L, contract) — card SE5

**v1 slice.** A JSON `entities` field on the tantivy schema (stored, not indexed) plus typed fast
fields for the two shipped kinds (`amount`, `event_date`) — adding a new entity KIND must not change
the schema (so no wipe). An `Enricher` trait in core (`enrich(doc) -> Vec<Entity{kind, value,
span}>`), the built-in regex enricher moved behind it, and a `plugin:<name>` enricher kind that runs a
core-module plugin (`enrich` export, same fuel/memory caps, fail-open per doc with a counter).
`[search] enrichers = ["builtin", "plugin:enrich-money-date"]` ordered list; `search-backfill
--re-enrich` re-runs enrichment without a schema rebuild. `GET /search/status` shows enricher stats.
Reference plugin `plugins-src/enrich-money-date/` (core module — buildable here) reproducing the two
regexes, verified by the `plugins-verify` path.
**Out of v1:** LLM enrichers, faceting on arbitrary kinds, entity-typed query syntax beyond what
exists.
**Gate to prove:** `new_entity_kind_does_not_change_schema_hash`; a plugin enricher that traps is
counted and skipped, never fails the index; backfill `--re-enrich` on a fixture index.

### N — N24 OpenLineage runs + assertions (L, none) — card EP5

**v1 slice.** Extract `LineageEvent { run, job, inputs, outputs, facets }` built once in
`on_job_success`/`full_sync`; the DataHub writer becomes `From<LineageEvent>` (no behaviour change —
pin with the existing DataHub tests). `[lineage] openlineage_url` (+ `namespace`, `api_key`),
default unset: a writer posting OpenLineage 2.x `RunEvent`s (START at job start via the existing
emitter call site if one exists at start, else COMPLETE/FAIL only — say which) with `schema`,
`columnLineage`, `outputStatistics` (rowCount, new/changed/removed) and a `pumper` custom facet (job
id, trigger chain / workflow `root_id`, cost). Quality: contract verdicts → `dataQualityAssertions`
facet and a DataHub `assertionRunEvent`; extraction-health state + trust tier → `globalTags`.
Source entities: each catalog `[[source]]` becomes an OpenLineage input / DataHub dataset URN on
platform `web`, and `upstream_lineage_with_fields` points at it instead of `NONE`. `GET /datahub/status`
gains `lineage.writers[]`. Keep the no-retry posture per writer, report each separately.
**Out of v1:** governance preferring Pumper-written assertions, `dataProcessInstance` for workflow
runs (add only if trivial once `root_id` exists), Marquez verification (document how).
**Gate to prove:** golden-file tests for one RunEvent per outcome; DataHub emitter output
byte-identical before/after the extraction (pin); a source URN appears as upstream.

### O — N25 Research as a living knowledge base (XL, policy) — card CR3

**v1 slice.** After `final_parsed`, upsert `research/findings` (key `{slug(query)}#{i}`) and
`research/sources` (key = URL) with `Provenance{source_url, job_id}`, behind `persist: true` (default
true for structured runs); `index_datasets` covers both. `snapshot_sources: true` fetches each cited
URL through `ctx.fetch{to_markdown, archive_max_age}` (metered), saves `source-N.md`, stamps
`artifact_sha` on the source record (replayable). `watch_sources: true` creates one `watch` schedule
per cited URL under `research/sources` (capped by `[research] max_watched_sources`, default 20).
A documented trigger recipe (not auto-created): `research/sources` changed → enqueue `research`
with `{session_id, query: "Source <url> changed: <diff excerpt>. Update the findings.",
max_budget_usd}`. `deep_research` MCP result carries `session_id` + the dataset keys.
**Out of v1:** the `explain-diff` role extraction from connector-api-watch (do it only if it is a pure
move with tests; else leave a note), an `explain` read-only mode, auto-follow-up config.
**Gate to prove:** a scripted-researcher run produces N findings + M sources with provenance; a
second run with `session_id` updates rather than duplicates; `snapshot_sources` spend lands on the
job ledger and `artifact_sha` verifies.

## Carry-forward from wave 2 (assigned; see FIXES-WAVE-2.md §Carry-forward)

- **K (N05)** also dispatches `transaction.pending|submitted` (N01) and
  `source.repair_promoted|rolled_back` (N12) as first-class event kinds through the new log —
  the apps name them in their results; the worker's post-run fan-out is where they become events.
- **K (N05)** adds `Storage::upsert_managed_schedule(id, app, cron, params, enabled, tag)` to
  `crates/core/src/storage.rs` and ports `scheduler.rs`'s raw `sqlx` peer upsert onto it (K owns
  the outbox drain on the scheduler tick anyway). Keep `sqlx` as a real server dependency only if
  something else still needs it; say which.
- **Not this wave:** moving the mesh wire format from `apps/peer` to `core/src/mesh.rs` (wave 4,
  when nobody touches `apps/peer`), the N10 benchmark, real-Chrome transact runs.
- **Migrations:** master is at **0048**; take 0049+ and expect renumbering on collision.
- **Config sections:** append at the very end of `Config` and of `config.rs`, after
  `TransactConfig`.
- **MCP tools:** append at the end of the tool table and dispatch; `fetch` must stay last (the
  inventory test pins it). L adds five tools — put them before `fetch`.

## Merge order (coordinator)

N (N24) → M (N11) → O (N25) → L (N04) → K (N05). `cargo check --workspace` after each; full gates
after all.
