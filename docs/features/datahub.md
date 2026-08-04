# DataHub metadata emitter

Pushes a **metadata shadow** of the dataset store to a [DataHub](https://datahub.com) instance: dataset entities, schema inferred from stored records, table-level lineage, and per-run freshness events. Record data never leaves the local store — only metadata (a few KB of JSON per run) is emitted, over DataHub's plain OpenAPI ingestion surface (`POST {gms}/openapi/entities/v1/`, bearer token; no Python SDK, no Kafka). Implementation: `crates/server/src/datahub.rs`; config: `[datahub]` in `crates/core/src/config.rs`.

## Config (`[datahub]`, disabled by default)

`enabled` (false), `gms_url` (default `http://localhost:8080`; DataHub Cloud: `https://<tenant>.acryl.io/gms`), `token` (falls back to `DATAHUB_TOKEN` env / `.env`), `env` (URN fabric segment, default `PROD`), `emit_schema` (true), `emit_profile` (true). With `enabled = false` the emitter never runs and job execution is unchanged.

## What gets emitted

Dataset URNs are `urn:li:dataset:(urn:li:dataPlatform:pumper,<app>.<dataset>,<env>)`. Aspects per dataset:

- **`datasetProperties`** — name `<app>/<dataset>`, description, and custom properties: `pumper_app`, `record_count`, and on job-driven emissions `last_job_id` + `last_run_new/changed/removed` (from the run's revision batch).
- **`operation`** (timeseries) — an `UPDATE` operation stamped at emission time; this is what DataHub freshness assertions and "last updated" read.
- **`datasetProfile`** (timeseries, `emit_profile`) — row count.
- **`schemaMetadata`** (`emit_schema`) — top-level fields of the dataset's newest record typed from their JSON values (string/number/boolean/array/object/null → DataHub logical types), with the truncated (4 KB) sample embedded as `OtherSchema.rawSchema`.
- **`upstreamLineage`** — on job emissions only: when a run also writes cross-namespace datasets named in the result's `index_datasets` (e.g. `grants-gov` writing `grants/unified`), the derived dataset gets `TRANSFORMED` edges from every dataset under the job's namespace. Lineage is derived from actual run behavior, not configuration. Because aspect upserts replace wholesale, the emitter first reads the dataset's existing upstreams back from GMS (`/openapi/v3/entity/dataset/{urn}?aspects=upstreamLineage`) and emits the union — so a multi-writer dataset accumulates one edge per source app instead of each run wiping the others.

## When

- **On every succeeded job** (worker hook, after triggers/saved-search side effects): a fail-open emission of **all datasets under the job's namespace** — including on quiet runs with zero revisions, so the freshness signal never goes stale just because nothing changed — with this run's new/changed/removed counts where the revision feed has them, plus the `index_datasets` targets. A down/misconfigured GMS is a warn log — never a job failure. Batches of 25 entities per POST on a dedicated 60s-timeout client (GMS cold-start ingestion was observed at ~18s, over the webhook client's 15s). The emission runs on the worker's **fan-out pool** (`crates/server/src/fanout.rs`), not a detached `tokio::spawn`: off the scrape permit but tracked, so shutdown drains it within `[worker] shutdown_drain_secs` or logs how many emissions it abandoned — never a silent metadata gap.
- **`POST /datahub/sync`** — one-shot backfill walking `list_all_datasets()` (entity + properties + profile/schema, plus schedules/trigger lineage under `emit_flows`). 409 while `[datahub]` is disabled, **and 409 while another full sync is already running** (one at a time: the backfill is idempotent, so rejecting beats queueing, and two parallel lineage read-merges can lose edges). Run once after connecting a fresh instance.
- **Governance poll** (`govern = true`, piggybacked on the scheduler tick): reads DataHub state for every Pumper dataset URN. The reads run **4 at a time on a 10s-timeout client** (not the emitter's 60s write client), so one poll's worst case is `ceil(datasets / 4) × 10s` — 50s for 20 datasets, against ~20 minutes when the reads were serial on the write client. Exceeding that budget is a warn with the measured duration; the summary carries `poll_ms` and `budget_secs`. The next poll is gated on the previous one's **completion**, so two polls can never overlap and race the paused-app set. See the governance section below.
- **`GET /datahub/status`** — config view (`enabled`, `gms_url`, `env`, `token_set`, toggles) + `emissions`:
  - `ok` / `failed` — monotonic counters since boot, so a *flapping* bridge is visible without a log dive.
  - `last_success` / `last_error` — kept **separately**: a success seconds after a failure no longer erases it (the single-slot `last_emission` did, which made a bridge failing half its emissions look healthy).
  - `last` — the newest entry of either kind; also mirrored as the back-compat top-level `last_emission` field.
  - `sync_running` — whether a full sync currently holds the single backfill slot.
  - Entries are `{kind: job|sync, at, ok, entities?|error?}`; a batch that fails mid-emission reports how many entities already landed (`(partial: 25 of 60 entities already ingested) …`) because there is no rollback. All in-memory: a restart zeroes the counters.

## Verified against a live instance (quickstart v1.6, 2026-07-23)

The v1 ingestion envelope accepts all emitted aspects **including the timeseries ones** (`operation`, `datasetProfile`) — no v3 route needed. Full backfill (15 datasets / 60 entities), per-run emission, and three-source accumulated lineage on `grants.unified` (`grants-gov` + `ca-grants` + `eu-sedia` → unified) all confirmed via GMS readback.

## Known gaps

- Schema inference is top-level-fields-only (no nested field paths), from a single sample record.
- **No retry/DLQ, by design** (unlike webhooks): a failed emission is recorded (`emissions.last_error` + `failed` counter) and never re-sent. Metadata is idempotent and fully re-derived on every run, so the next run — or a manual `/datahub/sync` — self-heals it; a queue would only buy staleness insurance. The cost is honest: a batch that fails halfway leaves the earlier batches ingested and the rest missing until the next emission.
- Emitting all own-namespace datasets on success slightly over-claims freshness for apps whose runs deliberately touch only a subset of their datasets.
- The lineage read-merge is not concurrency-safe (two simultaneous writers could race); at Pumper's job cadence this is theoretical.
- Lineage from *trigger* edges (app→app) is not emitted — only same-run virtual-namespace joins. External upstream sources (`catalog/data-sources.toml`) are not yet modeled as DataHub entities.
