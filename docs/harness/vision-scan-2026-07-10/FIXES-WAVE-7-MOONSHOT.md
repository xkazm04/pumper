# Vision Scan Wave 7 — Moonshot: Reactive Pipelines

> 7 commits (design + 6 implementation), 1 moonshot idea closed + 1 duplicate absorbed.
> Baseline preserved: build clean → build clean; tests 40 → 45 (+4 unit +1 integration, 0 failed).
> Design: Opus deep-think (`DESIGN-reactive-pipelines.md`); user decisions: fan-in deferred, fixed `_trigger` injection only, cycle-skips warn-logged, max_depth=8.

## Idea

**ca238cf5** — change-driven reactive pipelines on dataset deltas (absorbs 5011a7f6 — reactive DAGs on terminal events). Multi-stage flows (`crawl → extract → research → alert`) now compose declaratively inside pumper.

## Commits

1. schema+CRUD — `triggers` table (edge list = the DAG), `jobs.trigger_id` lineage, storage CRUD + `jobs_by_trigger`
2. fire helper — pure `decide()` (chain cycle-break + depth backstop), filters (`fresh` semantics), `merged_params`, capped `_trigger` builders, idempotency key; 4 unit tests; `[triggers]` config
3. dataset eval — worker computes the run's revision batch once (shared with watches), fires matching dataset triggers
4. terminal eval — `finalize()` fires job-kind triggers on final status
5. API — CRUD + `POST /triggers/{id}/test` (dry-run w/ resolved params + reasons; `?fire=true` real, idempotency-bypassed) + `GET /triggers/{id}/runs`
6. integration test — temp-DB round-trip: CRUD, evaluation-set scoping, idempotent double-fire dedup, lineage; + docs

## The contract (for app authors)

- Target reads `ctx.params["_trigger"]`: `{trigger_id, source_kind, app, dataset/status, kind, count, keys[≤200], source_job_id, depth, chain}` — full data fetched by key/id, never inlined.
- A trigger fires **at most once per source job run** (`trig:{t}:{src}` idempotency key).
- Budgets/priority/attempts are the **trigger's own**, never inherited.
- Cycles break at first chain repeat (warn log); depth capped at `[triggers] max_depth` (8).

## Non-goals (deferred by design)

Fan-in/join barriers, `${…}` param templating, per-record fan-out, named pipeline grouping/UI, backfill, trigger-fired SSE events.
