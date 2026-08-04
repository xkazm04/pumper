# DataHub bridge (metadata emitter + governance pull)

Two directions over one connection to a [DataHub](https://datahub.com) instance:

1. **Push** — a **metadata shadow** of the dataset store: dataset entities, inferred schema, table- and column-level lineage, pipeline topology (schedules/runs), and per-run freshness events. Record data never leaves the local store; only metadata (a few KB of JSON per run) is emitted, over DataHub's plain OpenAPI ingestion surface (`POST {gms}/openapi/entities/v1/`, bearer token; no Python SDK, no Kafka).
2. **Pull** — an opt-in **governance actuator**: DataHub state (deprecation, `cost:pause` tags, failing assertions) is read back and *acts on this Pumper instance* — disabling schedules, zeroing budgets, enqueueing syncs. This half writes to your job queue; read [Governance actuator](#governance-actuator-govern--true) before enabling it.

Implementation: `crates/server/src/datahub.rs`; config: `[datahub]` in `crates/core/src/config.rs`; wire tests: `crates/server/src/e2e/datahub_bridge.rs`.

## Config (`[datahub]`, disabled by default)

| key | default | what |
| --- | --- | --- |
| `enabled` | `false` | Master switch. Off ⇒ nothing is emitted, no poll runs, `POST /datahub/sync` is a 409, and job execution is byte-for-byte unchanged. The shipped `config.toml` ships it **off** on purpose: `true` against an absent localhost GMS makes every successful job do its metadata reads and then warn about a refused connection. |
| `gms_url` | `http://localhost:8080` | GMS base URL (DataHub Cloud: `https://<tenant>.acryl.io/gms`). |
| `token` | unset | Personal access token; falls back to `DATAHUB_TOKEN` from the environment / `.env`. The quickstart stack needs none. |
| `env` | `PROD` | Fabric segment of every URN. |
| `emit_schema` | `true` | Infer `schemaMetadata` from each dataset's newest record. |
| `emit_profile` | `true` | Emit `datasetProfile` (row counts). |
| `emit_flows` | `true` | Pipeline topology: schedules/triggers as `dataFlow`, runs as `dataJob` with in/out edges, trigger DAG as dataset lineage, and RuleSet-derived column lineage. |
| `govern` | `false` | The pull half — see [Governance actuator](#governance-actuator-govern--true). Remote state acting on an unattended box is opt-in. |
| `govern_interval_secs` | `300` | Seconds between governance polls, clamped to a 30s minimum. Measured from the previous poll's **completion**. |

## What gets emitted

Dataset URNs are `urn:li:dataset:(urn:li:dataPlatform:pumper,<app>.<dataset>,<env>)`; flows are `urn:li:dataFlow:(pumper,<flow_id>,<env>)`; runs are `urn:li:dataJob:(<flow_urn>,<job_id>)`.

Per dataset:

- **`datasetProperties`** — name `<app>/<dataset>`, description, and custom properties: `pumper_app`, `record_count`, and on job-driven emissions `last_job_id` + `last_run_new/changed/removed` (from the run's revision batch).
- **`operation`** (timeseries) — an `UPDATE` operation stamped at emission time; this is what DataHub freshness assertions and "last updated" read.
- **`datasetProfile`** (timeseries, `emit_profile`) — row count.
- **`schemaMetadata`** (`emit_schema`) — top-level fields of the dataset's newest record typed from their JSON values (string/number/boolean/array/object/null → DataHub logical types), with the truncated (4 KB) sample embedded as `OtherSchema.rawSchema`.
- **`upstreamLineage`** (table level) — two sources, both merged with what GMS already holds:
  - *cross-namespace run outputs*: when a run also writes datasets named in the result's `index_datasets` (e.g. `grants-gov` writing `grants/unified`), the derived dataset gets `TRANSFORMED` edges from every dataset under the job's namespace — lineage derived from actual run behavior, not configuration;
  - *trigger edges* (`emit_flows`, on `POST /datahub/sync`): every enabled dataset trigger becomes source-app → target-app dataset edges, so the reactive DAG renders as a graph.

  Because aspect upserts replace wholesale, the emitter first reads the dataset's existing upstreams back (`GET /openapi/v3/entity/dataset/{urn}?aspects=upstreamLineage`) and emits the union, so a multi-writer dataset accumulates one edge per source instead of each writer wiping the others.
- **`upstreamLineage.fineGrainedLineages`** (column level, `emit_flows`, job emissions) — emitted **only** when the job's params carry a declarative `rules` RuleSet, which makes field provenance mechanical: one entry per column with the rule as `transformOperation` (`css:h1`, `regex:\d+#1`, `json:/a/b`, `each:.card` + `parent.child` for nested fields) and `upstreamType: NONE` (the upstream is the fetched page, not a dataset — claiming otherwise would be a lie). Apps whose extraction is code, not rules, are honestly skipped.

Per pipeline (`emit_flows`):

- **`dataFlowInfo`** — one flow per schedule (`schedule.<app>.<id>`, with `cron`/`enabled`/`timezone`/`managed_by` as custom properties), per trigger (`trigger.<app>.<id>`), or the app's ad-hoc bucket (`adhoc.<app>`).
- **`dataJobInfo` + `dataJobInputOutput`** — each succeeded run as a job under its flow, with `job_id`/`attempts`, the firing trigger's source datasets as inputs, and everything the run wrote as outputs.

## When

- **On every succeeded job** (worker hook, after triggers/saved-search side effects): a fail-open emission of **all datasets under the job's namespace** — including on quiet runs with zero revisions, so the freshness signal never goes stale just because nothing changed — with this run's new/changed/removed counts where the revision feed has them, plus the `index_datasets` targets, the flow/run entities, and column lineage where rules allow. A down/misconfigured GMS is a warn log — never a job failure. Batches of 25 entities per POST on a dedicated 60s-timeout client (GMS cold-start ingestion was observed at ~18s, over the webhook client's 15s). The emission runs on the worker's **fan-out pool** (`crates/server/src/fanout.rs`), not a detached `tokio::spawn`: off the scrape permit but tracked, so shutdown drains it within `[worker] shutdown_drain_secs` or logs how many emissions it abandoned — never a silent metadata gap.
- **`POST /datahub/sync`** — one-shot backfill walking `list_all_datasets()` (entity + properties + profile/schema, plus schedule flows and trigger lineage under `emit_flows`). Returns `{kind: "sync", at, ok, datasets, flows, trigger_edges, entities?|error?}`. **409** while `[datahub]` is disabled, **and 409 while another full sync is already running** (one at a time: the backfill is idempotent, so rejecting beats queueing, and two parallel lineage read-merges can lose edges). Run once after connecting a fresh instance.
- **Governance poll** (`govern = true`), piggybacked on the scheduler tick — see below.
- **`GET /datahub/status`** — config view (`enabled`, `gms_url`, `env`, `token_set`, `emit_schema`, `emit_profile`, `emit_flows`) plus:
  - `emissions.ok` / `emissions.failed` — monotonic counters since boot, so a *flapping* bridge is visible without a log dive.
  - `emissions.last_success` / `emissions.last_error` — kept **separately**: a success seconds after a failure no longer erases it (the old single-slot `last_emission` did, which made a bridge failing half its emissions look healthy).
  - `emissions.last` — the newest entry of either kind; mirrored as the back-compat top-level `last_emission`.
  - `emissions.sync_running` — whether a full sync currently holds the single backfill slot.
  - Entries are `{kind: job|sync, at, ok, entities?|error?}`; a batch that fails mid-emission reports how many entities already landed (`(partial: 25 of 60 entities already ingested) …`) because there is no rollback and, deliberately, no retry.
  - `govern` — `{enabled, interval_secs, paused_apps, last_poll}`.
  - Everything here is in-memory: a restart zeroes the counters and clears the paused set.

## Governance actuator (`govern = true`)

Opt-in, default OFF, piggybacked on the scheduler tick. Each poll reads deprecation / tags / assertion health for **every** Pumper dataset URN over the GMS GraphQL surface (`POST {gms}/api/graphql`) and maps the answers to three actions. Every action is loud-logged and listed in `govern.last_poll.actions`.

**Poll mechanics.** Reads run 4 at a time on a dedicated **10s-timeout** client (not the emitter's 60s write client), so one poll's worst case is `ceil(datasets / 4) × 10s` — 50s for 20 datasets. Exceeding that budget is a warn with the measured duration; the summary carries `poll_ms` and `budget_secs`. The next poll is gated on the previous one's **completion**, so two polls can never overlap and race the paused-app set.

| DataHub signal | Action | Blast radius |
| --- | --- | --- |
| dataset **deprecated** | disable that dataset's app's schedules | **One deprecated dataset disables ALL of that app's catalog-managed schedules**, not just the ones feeding that dataset. Fenced to `managed_by = "catalog"` in SQL (M19) — hand-made schedules are never touched. **Not automatically reversible:** un-deprecating in DataHub does *not* re-enable them; re-enable via `POST /catalog/reconcile` (which sets `enabled = 1` for catalog-managed rows) or the schedule API. Conversely, with `[catalog] auto_reconcile = true` the **boot** reconcile re-enables them silently — the two actuators disagree, and the last one to run wins. |
| tag **`cost:pause`** on any of the app's datasets | force that app's job budget to `$0` for **new** jobs | The budget governor then runs free tiers only — the Claude tier is skipped. Nothing running is cancelled, no job fails. Fully reversible: the paused set is recomputed wholesale each poll, so removing the tag resumes the app on the next poll. |
| **failing assertions** (`health[].type == "ASSERTIONS" && status == "FAIL"`) | enqueue one immediate sync job for that dataset's app | `max_attempts = 2`, app default params, and an **hour-bucketed idempotency key** (`datahub-govern-sync:<app>:<dataset>:<YYYY-MM-DDTHH>`) so a persistently failing assertion enqueues at most one job per hour, not one per poll. Apps not in the registry are skipped with a warn. |

**Failure posture, as it behaves today.** The first read error aborts the entire poll *before any action is planned* — an unreachable or absent DataHub is a clean no-op, and datasets DataHub has never seen read as all-false (only explicit remote state acts). The consequence to know: because `paused_apps` is only recomputed on a **successful** poll, a pause set before an outage **stays frozen for the duration of the outage** — the tag cannot be un-read while GMS is down. A restart clears it (in-memory only), so a dead DataHub after a restart means "no pauses" — fail-open in that direction, fail-frozen in the other.

## Verified against a live instance (quickstart v1.6, 2026-07-23)

The v1 ingestion envelope accepts all emitted aspects **including the timeseries ones** (`operation`, `datasetProfile`) — no v3 route needed. Full backfill (15 datasets / 60 entities), per-run emission, and three-source accumulated lineage on `grants.unified` (`grants-gov` + `ca-grants` + `eu-sedia` → unified) all confirmed via GMS readback.

## Known gaps

- Schema inference is top-level-fields-only (no nested field paths), from a single sample record.
- **No retry/DLQ, by design** (unlike webhooks): a failed emission is recorded (`emissions.last_error` + the `failed` counter) and never re-sent. Metadata is idempotent and fully re-derived on every run, so the next run — or a manual `/datahub/sync` — self-heals it; a queue would only buy staleness insurance. The cost is honest: a batch that fails halfway leaves the earlier batches ingested and the rest missing until the next emission.
- Emitting all own-namespace datasets on success slightly over-claims freshness for apps whose runs deliberately touch only a subset of their datasets.
- The lineage read-merge is not concurrency-safe across *writers* (two jobs finishing at once could interleave read-then-write on the same derived dataset). The full-sync overlap guard removes the sync-vs-sync case only.
- Governance transitions are not previewed or audited: there is no dry-run of what a poll would do, and no history beyond the last poll summary. A deprecation-driven schedule disable is not undone by un-deprecating.
- External upstream sources (`catalog/data-sources.toml`) are not modeled as DataHub entities — lineage starts at Pumper's own datasets.
- Column lineage requires a declarative RuleSet in job params; code-driven apps get table-level lineage only.
