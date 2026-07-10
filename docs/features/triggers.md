# Reactive pipelines (triggers)

Multi-stage flows (`crawl → extract → research → alert`) compose declaratively from **trigger edges**: *(source event) → (enqueue target app)*. The set of triggers IS the pipeline DAG — there is no pipeline container. Full design rationale: [`../harness/vision-scan-2026-07-10/DESIGN-reactive-pipelines.md`](../harness/vision-scan-2026-07-10/DESIGN-reactive-pipelines.md).

## Trigger row (`triggers` table)

`source_kind` = `dataset` (fires on a run's revision batch; filters `source_dataset` (`'*'` = any) + `on_change` ∈ `new|changed|removed|fresh|any`, default `fresh`) or `job` (fires on terminal state; `on_status` ∈ `succeeded|failed|any`, default `succeeded`). Target: `target_app` (must be registered; `source_app` may be a virtual namespace like `grants`), static `params` template, and the **trigger's own** `budget_usd` / `priority` / `max_attempts` (never inherited from the source).

## The `_trigger` contract (what the target app reads)

The target job's params = template with `_trigger` merged over it (injected wins): `{trigger_id, source_kind, app, dataset|status, kind, count, keys (capped at [triggers] key_cap, default 200), source_job_id, result_summary?, depth, chain}`. Full data is **never inlined** — fetch by key via the datasets API or by `source_job_id` via `GET /jobs/{id}`.

## Guarantees

- **At most once per source run**: idempotency key `trig:{trigger_id}:{source_job_id}`.
- **Cycle guard**: the provenance `chain` (trigger ids) rides in `_trigger`; a repeated id skips the hop (warn log). `depth` capped by `[triggers] max_depth` (default 8).
- **Fail-open**: evaluation errors, unregistered targets, and guard skips warn-log and never affect the source job. Batch fan-out: one target job per trigger per source run, carrying the whole capped batch.
- Failed triggered jobs are ordinary failed jobs — `GET /jobs?status=failed` + `POST /jobs/{id}/retry` are the DLQ.

## API

`GET/POST /triggers` (kind-aware validation), `DELETE /triggers/{id}`, `POST /triggers/{id}/enabled`, `POST /triggers/{id}/test` (dry-run against the most recent source job → `would_fire` + resolved params + reason; `?fire=true` enqueues for real, idempotency-bypassed), `GET /triggers/{id}/runs` (lineage via `jobs.trigger_id`).

## Non-goals (by design)

Fan-in/join barriers, `${…}` param templating, per-record fan-out, named pipeline grouping/UI, backfill on create.
