# Reactive pipelines (triggers)

Multi-stage flows (`crawl → extract → research → alert`) compose declaratively from **trigger edges**: *(source event) → (enqueue target app)*. The set of triggers IS the pipeline DAG — there is no pipeline container. Full design rationale: [`../harness/vision-scan-2026-07-10/DESIGN-reactive-pipelines.md`](../harness/vision-scan-2026-07-10/DESIGN-reactive-pipelines.md).

## Trigger row (`triggers` table)

`source_kind` = `dataset` (fires on a run's revision batch; filters `source_dataset` (`'*'` = any) + `on_change` ∈ `new|changed|removed|fresh|any`, default `fresh`) or `job` (fires on terminal state; `on_status` ∈ `succeeded|failed|any`, default `succeeded`). Target: `target_app` (must be registered; `source_app` may be a virtual namespace like `grants`), static `params` template, and the **trigger's own** `budget_usd` / `priority` / `max_attempts` (never inherited from the source).

## The `_trigger` contract (what the target app reads)

The target job's params = template with `_trigger` merged over it (injected wins): `{trigger_id, source_kind, app, dataset|status, kind, count, keys (capped at [triggers] key_cap, default 200), source_job_id, result_summary?, depth, chain}`. Full data is **never inlined** — fetch by key via the datasets API or by `source_job_id` via `GET /jobs/{id}`.

## Guarantees

- **At most once per source run *per dataset***. A run's revision batch is grouped by dataset and each group is evaluated separately, so a run writing three datasets fires **three** hops from one `source_dataset = '*'` trigger — each carrying only that dataset's keys. Keys:

  | Hop | Idempotency key |
  |---|---|
  | Dataset hop (run fan-out) | `trig:{trigger_id}:{source_job_id}:ds:{dataset}` |
  | Dataset hop (saved-search view materialization) | `trig:{trigger_id}:{source_job_id}:view:{saved_search_id}:ds:{dataset}` |
  | Terminal-job hop | `trig:{trigger_id}:{source_job_id}` |
  | External (ingress) hop | `trig:{trigger_id}:{event_id}` |

  A materialized [saved-search view](search.md) fires its dataset triggers under the *view's* app while keeping the **source job's id** for provenance, so its key is scoped by the saved search — otherwise a view feeding the source job's own app would collide with the run's own hop. Datasets are walked in sorted order, so hop ordering within a run is stable rather than hash-random.
- **Cycle guard**: the provenance `chain` (trigger ids) rides in `_trigger`; a repeated id skips the hop (warn log). `depth` capped by `[triggers] max_depth` (default 8).
- **Fail-open**: evaluation errors, unregistered targets, and guard skips warn-log and never affect the source job. A dedup suppression logs at `debug` with the key that suppressed it.
- Failed triggered jobs are ordinary failed jobs — `GET /jobs?status=failed` + `POST /jobs/{id}/retry` are the DLQ.
- **Degrading sources don't fire.** Dataset triggers and watches read the same run revision batch, and it is filtered per dataset by extraction health before either is evaluated — a dataset whose source state suppresses outbound pushes is dropped, so no hop is enqueued from it. Per-dataset, so a break in one of an app's datasets doesn't stop the others; **no-op unless `[resilience] enforce` is on** (default off). See [events-webhooks.md](events-webhooks.md) and [resilient-extraction.md](resilient-extraction.md).

## API

Handlers live in `crates/server/src/routes/triggers.rs`; the fire/decide logic in `crates/server/src/triggers.rs`.

`GET/POST /triggers` (kind-aware validation), `DELETE /triggers/{id}`, `POST /triggers/{id}/enabled`, `POST /triggers/{id}/test` (dry-run against the most recent source job → `would_fire` + resolved params + reason; `?fire=true` enqueues for real, idempotency-bypassed), `GET /triggers/{id}/runs` (lineage via `jobs.trigger_id`).

`GET /triggers` takes an optional `app` filter and is **dual-mode**: bare `{triggers: [...]}` by default, or `{items, next_cursor}` when a `cursor` param is present (even empty) — an opaque keyset cursor. `limit` is clamped 1–500 in **both** modes, so an uncursored list can never stream the whole table.

`POST /triggers` body: `{name?, source_kind, source_app, source_dataset?, on_change?, on_status?, target_app, params?, budget_usd?, priority?, max_attempts?}`.

## Non-goals (by design)

Fan-in/join barriers, `${…}` param templating, per-record fan-out, named pipeline grouping/UI, backfill on create.
