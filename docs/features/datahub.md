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

- **On every succeeded job** (worker hook, after triggers/saved-search side effects): a detached, fail-open task emits **all datasets under the job's namespace** — including on quiet runs with zero revisions, so the freshness signal never goes stale just because nothing changed — with this run's new/changed/removed counts where the revision feed has them, plus the `index_datasets` targets. A down/misconfigured GMS is a warn log — never a job failure. Batches of 25 entities per POST on a dedicated 60s-timeout client (GMS cold-start ingestion was observed at ~18s, over the webhook client's 15s).
- **`POST /datahub/sync`** — one-shot backfill walking `list_all_datasets()` (entity + properties + profile/schema; no lineage — that needs a run). 409 while `[datahub]` is disabled. Run once after connecting a fresh instance.
- **`GET /datahub/status`** — config view (`enabled`, `gms_url`, `env`, `token_set`, toggles) + `last_emission` (`{kind: job|sync, at, ok, entities?|error?}`, in-memory, null before the first emission).

## Verified against a live instance (quickstart v1.6, 2026-07-23)

The v1 ingestion envelope accepts all emitted aspects **including the timeseries ones** (`operation`, `datasetProfile`) — no v3 route needed. Full backfill (15 datasets / 60 entities), per-run emission, and three-source accumulated lineage on `grants.unified` (`grants-gov` + `ca-grants` + `eu-sedia` → unified) all confirmed via GMS readback.

## Known gaps

- Schema inference is top-level-fields-only (no nested field paths), from a single sample record.
- No retry/DLQ (unlike webhooks): a failed emission is dropped; the next run or a manual `/datahub/sync` heals it.
- Emitting all own-namespace datasets on success slightly over-claims freshness for apps whose runs deliberately touch only a subset of their datasets.
- The lineage read-merge is not concurrency-safe (two simultaneous writers could race); at Pumper's job cadence this is theoretical.
- Lineage from *trigger* edges (app→app) is not emitted — only same-run virtual-namespace joins. External upstream sources (`catalog/data-sources.toml`) are not yet modeled as DataHub entities.
