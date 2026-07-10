# DESIGN — Reactive pipelines / job DAGs (triggers)

**Moonshot:** dataset changes and terminal job events fire downstream jobs, so
multi-stage flows (`crawl → extract → research → alert`) compose declaratively
inside pumper instead of via external glue.
Vibeman idea **ca238cf5** (change-driven pipelines on dataset deltas), absorbing
**5011a7f6** (reactive DAGs on terminal events).

Status: DESIGN ONLY. Read-only survey of the current tree; no code changed.

---

## Summary

The whole feature is **one new concept — a `trigger`** — modelled as the codebase
already models `watches` and `schedules`: a durable, runtime-CRUD-able DB row that
the single-writer worker consults on a hook it already owns.

A trigger is a **directed edge**: *(when this fires) → (enqueue that app)*. There is
**no pipeline/steps/edges container**. A DAG is just the set of edges — the adjacency
list *is* the schema — because a triggered job's own dataset changes and terminal
event are themselves events that can match further triggers. Chains and fan-out DAGs
both fall out of one table for free, and each edge is independently enable/disable/
delete-able exactly like a watch. A named `pipeline` grouping is a later cosmetic label,
explicitly out of MVP.

Key decisions, up front:

| Question | Decision | Why |
|---|---|---|
| Trigger model | **Both** dataset-change and terminal-job events, one `triggers` table with a `source_kind` discriminator | The two Vibeman ideas are the two rows of one enum |
| Event → params | **Fixed injection field `params._trigger`** (compact: keys/counts/diff-summary/job-id), merged over a static template; NO template language | Matches "params is JSON `Value`" + "results stay compact, big payloads fetched by id" conventions |
| Schema | **One table** (edge list) + one `jobs.trigger_id` column | Minimal, non-corner-painting; DAG = edges |
| Cycle guard | **Visited trigger-id set in provenance chain (true break) + `max_depth` backstop** | Provenance rides in `_trigger`, propagates through `job.params` automatically |
| Fan-out | **Batch: one triggered job carries the whole change batch** (capped key list) | Mirrors `notify_watches` (one webhook per watch, `count`+`changes`); N-jobs would explode the queue/budget ledger |
| Idempotency | Enqueue key `trig:{trigger_id}:{source_job_id}` on the existing partial-unique index | A trigger fires **at most once per source job run**; dedupes double-eval |
| Where it runs | **Worker hooks**, beside `notify_watches` (dataset) and inside `finalize()` (terminal); no new reconciler | The hook already has the exact revision batch for free |
| Failure | Triggered job that fails permanently **is** the DLQ story (existing `/jobs?status=failed` + `/jobs/{id}/retry`); trigger eval is fail-open like watches | Established patterns, zero new surface |

---

## Trigger model

A trigger row answers three things: **what fires it**, **what it enqueues**, and
**what event data to pass in**.

### What fires it (`source_kind` + filters)

- **`dataset` triggers** — fire when a job run leaves revisions in a matching
  dataset. Columns: `source_app`, `source_dataset` (`"*"` = any dataset of the app,
  reusing `Watch::covers`), `on_change` ∈ `new | changed | removed | fresh | any`
  (`fresh` = new+changed; filters the batch by `Revision.change`).
- **`job` triggers** — fire when a job reaches a terminal state. Columns:
  `source_app`, `on_status` ∈ `succeeded | failed | any`.

`source_dataset` / `on_change` are NULL for `job` triggers; `on_status` is NULL for
`dataset` triggers. (SQLite is untyped; a `CHECK` documents the discriminator.)

### What it enqueues (target)

`target_app` (must be registered), plus reused `EnqueueOptions` fields as columns:
`params` (static JSON template), `budget_usd`, `priority`, `max_attempts`.

> **Budget is the trigger's, not inherited.** A source `http` crawl spends $0; its
> target `research` step needs its own ceiling. Inheriting would be wrong. Same for
> `max_attempts` (default 1, matching the scheduler).

### Event → params: fixed `_trigger` injection

The enqueued job's params = **`target.params` deep-merged with an injected
`_trigger` object** (injected key wins). Apps read `ctx.params["_trigger"]` to see
what fired them. Shape, deliberately **compact and bounded** (params is stored as
TEXT in `jobs`; the convention is small results + fetch big payloads by id):

```jsonc
// dataset trigger
"_trigger": {
  "trigger_id": "…", "source_kind": "dataset",
  "app": "grants", "dataset": "unified", "kind": "fresh",
  "count": 512, "keys": ["grants-gov:123", "…"],   // capped at N (config, e.g. 200) + count
  "source_job_id": "…",
  "depth": 1, "chain": ["trig-A"]                    // provenance (cycle guard)
}
// job trigger
"_trigger": {
  "trigger_id": "…", "source_kind": "job",
  "app": "crawl", "status": "succeeded",
  "source_job_id": "…",
  "result_summary": { "new": 40, "changed": 3 },     // if the result exposes them
  "depth": 2, "chain": ["trig-A", "trig-B"]
}
```

Full record data / full job result are **NOT** inlined — the target fetches them via
`GET /datasets/{app}/{dataset}/history?key=…` or `GET /jobs/{id}`. This bounds param
size regardless of a 500-key batch.

> **No template language in MVP.** Fixed-field injection covers `crawl→extract→
> research→alert` (each step reads `_trigger.keys` / `_trigger.source_job_id`).
> `${_trigger.keys}` substitution is an Open Question, not MVP.

---

## Schema (SQL) — migration `0014_triggers.sql`

```sql
-- Reactive triggers: a directed edge (source event) -> (enqueue target app).
-- The set of triggers IS the pipeline DAG (adjacency list); there is no separate
-- pipeline container. Modelled on `watches` (runtime-CRUD standing subscriptions).
CREATE TABLE IF NOT EXISTS triggers (
    id             TEXT PRIMARY KEY,
    name           TEXT,                         -- optional human label / future pipeline group
    source_kind    TEXT NOT NULL,                -- 'dataset' | 'job'
    source_app     TEXT NOT NULL,
    source_dataset TEXT,                          -- dataset kind: '*' or name; NULL for job kind
    on_change      TEXT,                          -- 'new'|'changed'|'removed'|'fresh'|'any'; dataset only
    on_status      TEXT,                          -- 'succeeded'|'failed'|'any'; job only
    target_app     TEXT NOT NULL,
    params         TEXT NOT NULL DEFAULT '{}',    -- static template; _trigger merged over it
    budget_usd     REAL,                          -- target ceiling (NOT inherited from source)
    priority       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 1,
    enabled        INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    CHECK (source_kind IN ('dataset','job'))
);
CREATE INDEX IF NOT EXISTS idx_triggers_source
    ON triggers (source_kind, source_app, enabled);

-- Lineage: which trigger fired this job (mirrors jobs.schedule_id).
ALTER TABLE jobs ADD COLUMN trigger_id TEXT;
CREATE INDEX IF NOT EXISTS idx_jobs_trigger ON jobs (trigger_id, created_at DESC);
```

That is the entire schema. `EnqueueOptions` gains `trigger_id: Option<String>`;
`Job` gains `trigger_id: Option<String>`; both threaded like `schedule_id`.

**Linear chains first or full DAG?** The edge-list model is a full DAG at zero extra
cost — fan-out (two triggers on one source) and chains (target's event matches
another trigger) are both just rows. The only thing NOT expressible is **fan-in /
join barriers** ("wait for A *and* B"), which is an explicit non-goal (see below).

---

## Execution semantics

### Where evaluation runs (worker hooks, no reconciler)

Both evaluations are best-effort side effects on the single-writer worker, exactly
like `notify_watches`:

- **Dataset triggers** — in `worker::execute` on the success path, folded into the
  existing changes computation. `notify_watches` already calls
  `changes_since(app, None, job.started_at, 1000)` and groups `by_dataset`. **Reuse
  that same batch**: pass the loaded `changes`/`by_dataset` to a new
  `fire_dataset_triggers(state, job, &by_dataset)` so there is no second query.
  Runs *after* watches + saved-searches (watches only notify; triggers create work —
  ordering is cosmetic since all three are fail-open).
- **Job/terminal triggers** — in `worker::finalize`, after `webhook::dispatch`. The
  job's terminal status is known there; call
  `fire_terminal_triggers(state, &job)`.

Rationale for hooks over a separate reconciler: the hook already holds the **exact**
revision batch this run produced (`started_at` window) — a reconciler would have to
persist and advance an "evaluated up to" cursor per trigger and risk gaps/dupes. The
worker is single-writer, giving natural ordering and no locking.

### Fan-out — batch, not N jobs

A batch of 500 fresh keys fires **one** target job carrying the capped key list +
count. Rationale: (1) mirrors `notify_watches`, which sends one webhook per watch
with a `changes` array; (2) N jobs would multiply queue rows, cost-ledger rows and
budget bookkeeping by 500 and defeat per-job budget ceilings; (3) scraper/extract
apps naturally accept a batch of keys as params. Per-record fan-out is a documented
non-goal (future `mode` column).

### Cycle / loop prevention — provenance chain + depth backstop

The hazard: a triggered job's own success matches another trigger; `A→B→A…`.

Two guards, both riding in `_trigger` (which propagates automatically because it
lives in `job.params`, and the worker reads `source_job.params._trigger` when firing
the next hop):

1. **Visited-set (true cycle break).** `_trigger.chain` is the ordered list of
   trigger-ids that led to this job. Before enqueuing via trigger `T`, if `T.id` is
   already in the incoming chain → **skip** (log `warn`, do not enqueue). This breaks
   any cycle at first repetition regardless of length.
2. **`max_depth` backstop** (config, default **8**). `_trigger.depth = source.depth +
   1`; refuse past `max_depth`. Catches long non-repeating chains and mis-config.

Skips are logged, never errored (fail-open). Surfacing skips for observability is an
Open Question.

### Idempotency — at most once per source job run

Enqueue with `idempotency_key = "trig:{trigger_id}:{source_job_id}"`, reusing the
existing partial-unique index and `enqueue_dedup`. Guarantees a trigger fires **at
most once per source job**, so a worker retry, or accidental double-evaluation
(dataset hook + any future path), collapses to one triggered job. Distinct source
job runs each fire (correct — each carried its own batch).

### Concurrency / overlap

**No overlap guard** (unlike schedules). Each source job run is a distinct event;
suppressing while a prior target runs would silently drop data. The idempotency key
already prevents duplication from the *same* source job. Rapid successive source runs
legitimately stack target jobs (each with its own batch); the worker's global +
per-app concurrency caps already bound throughput.

### Retry / budget

Target job carries the **trigger's** `budget_usd` / `max_attempts` / `priority`.
Standard job retry/backoff applies. No callback_url by default.

---

## API surface

CRUD modelled on `/watches`, plus a test endpoint:

- `GET  /triggers` — list; `?app=` filters `source_app`.
- `POST /triggers` — create. Validates `source_kind`, that `source_app` &
  `target_app` are registered, `on_change`/`on_status` legality for the kind.
- `DELETE /triggers/{id}`
- `POST /triggers/{id}/enabled` — `{ "enabled": bool }` (mirrors watches/schedules).
- `POST /triggers/{id}/test` — **dry-run**: resolves the target params (merged
  `_trigger`) against the trigger's most recent matching source job (or a supplied
  `sample`), returns `{ would_fire, target_app, resolved_params, reason }` **without
  enqueuing**. With `?fire=true`, actually enqueues once (bypassing the idempotency
  key so testing is repeatable) — the run-now path.
- `GET  /triggers/{id}/runs` — jobs where `trigger_id = id` (reuse `list_page`), the
  "what did this trigger produce" view.

`GET /jobs` gains an optional `trigger` filter (jobs fired by a trigger).

---

## Observability / lineage

"Pipeline X fired job Y because Z" is answered with **no new lineage table**:

- **`jobs.trigger_id`** → *which* trigger fired Y (join to trigger name/target).
- **`params._trigger`** on Y → *why*: source app/dataset/kind/`source_job_id`,
  `count`, `keys`, `chain`, `depth`. The full provenance chain is walkable by
  following `source_job_id` back through each job's `_trigger`.
- Y already emits `queued → running → terminal` SSE events and (if it fails) shows in
  `/jobs?status=failed`. No trigger-specific event stream in MVP.
- `GET /triggers/{id}/runs` gives the per-trigger job history and, via each job's
  cost events (`/jobs/{id}/costs`), the spend a trigger drove.

---

## Failure modes

- **Triggered job fails permanently** → it is an ordinary failed job: visible at
  `GET /jobs?status=failed`, retryable via `POST /jobs/{id}/retry`, provenance intact
  via `trigger_id` + `_trigger`. **The job queue's own DLQ is the DLQ.** If the target
  set a `callback_url`, the terminal webhook additionally lands in
  `webhook_deliveries` (existing DLQ/replay). No new machinery.
- **Trigger evaluation error must never fail the source job** — `fire_*` functions are
  fail-open exactly like `notify_watches`: load errors and per-trigger enqueue errors
  are logged (`warn`) and skipped; one bad trigger never blocks the others or the
  source job's completion.
- **Cycle / depth exceeded** → skip + `warn` log; source and prior hops are unaffected.
- **Unregistered `target_app`** (registered at create, later removed) → enqueue is
  skipped with a `warn`, mirroring the scheduler's unregistered-app guard.
- **Oversized batch** → key list is capped (`count` still exact); params stay bounded.

---

## MVP commit plan (6 commits, established conventions)

Each uses runtime `sqlx::query`, `ts()` timestamps, fail-open side effects, generic
`dispatch`-style patterns. No compile-time macros / DATABASE_URL.

1. **Schema + model + storage CRUD.**
   `crates/core/migrations/0014_triggers.sql` (table + `jobs.trigger_id`);
   `crates/core/src/storage.rs` — `Trigger` struct + `TriggerRow`/`TryFrom`,
   `create_trigger`, `list_triggers(app)`, `dataset_triggers(app)` /
   `job_triggers(app)` (enabled, by source_kind+app), `get_trigger`,
   `set_trigger_enabled`, `delete_trigger`; add `trigger_id` to `EnqueueOptions` +
   INSERT; `crates/core/src/job.rs` — `Job.trigger_id`; `lib.rs` export `Trigger`.

2. **Provenance + fire helper (pure, unit-tested).**
   `crates/server/src/triggers.rs` (new) — `build_trigger_params(template, injected)`
   deep-merge; `_trigger` construction for both kinds; `next_chain`/`depth` +
   cycle/`max_depth` decision; idempotency-key builder; key-list cap. Unit tests for
   merge precedence, cycle detection, depth cutoff, batch cap. Config: `max_depth`,
   `key_cap` (with `#[serde(default)]` + manual `Default`, per the ClaudeConfig lesson).

3. **Dataset-change evaluation.** `crates/server/src/worker.rs` — compute the
   `by_dataset` batch once (refactor `notify_watches` to accept it), add
   `fire_dataset_triggers(state, job, &by_dataset)`: match `dataset_triggers`, filter
   by `on_change`, build `_trigger`, guarded `enqueue_dedup` with `trigger_id`; wake
   worker. Fail-open throughout.

4. **Terminal-job evaluation.** `crates/server/src/worker.rs::finalize` — after
   `dispatch`, `fire_terminal_triggers(state, &job)`: match `job_triggers` on
   app+status, build `_trigger` (result summary), guarded enqueue.

5. **API routes.** `crates/server/src/routes.rs` — `GET/POST /triggers`,
   `DELETE /triggers/{id}`, `POST /triggers/{id}/enabled`, `POST /triggers/{id}/test`,
   `GET /triggers/{id}/runs`; `trigger` filter on `list_jobs`; register in `router()`.

6. **Integration test + docs.** End-to-end 2-step chain test (source job upserts →
   dataset trigger fires target carrying `_trigger.keys`; assert target enqueued once,
   idempotent on re-eval, cycle self-trigger skipped). Append a `harness-learnings.md`
   structural-facts note (triggers = edge list, worker-hook eval, `_trigger` contract).

---

## Non-goals (explicit)

- **Named pipeline container / visual DAG builder / graph UI.** Edges only; `name`
  column is the seed for a later grouping.
- **Fan-in / join barriers** ("run C after both A and B"). MVP is pure fan-out.
  `crawl→extract→research→alert` is linear, so unaffected.
- **Per-record fan-out** (N jobs from one batch). Batch only; future `mode` column.
- **Template / expression language** (`${…}`, conditionals on field *values*). Only
  `_trigger` fixed injection + change-kind/status filters.
- **Backfill** (firing on historical revisions at create time). Triggers act only on
  events after they exist.
- **Overlap suppression, scheduled+reactive hybrids, trigger-fired SSE events,
  retrying the *evaluation* itself.**

---

## Open questions for the user (max 3)

1. **Fan-in barriers** — confirm pure fan-out is enough for now. The target flow is
   linear so I believe yes, but a "wait for A and B" barrier is the one thing this
   edge-list model can't express and would need a small companion table later. OK to
   defer?
2. **Params templating** — MVP injects a fixed `_trigger` object and the target app
   reads it. Do you also want `${_trigger.keys}`-style substitution into the static
   `params` template now, or is fixed-field injection sufficient (my recommendation)?
3. **Cycle-skip visibility** — when a trigger is skipped for a cycle or `max_depth`,
   MVP just logs a `warn`. Do you want skips surfaced (a `trigger_skips` log row / SSE
   event) for observability, or is the log fine for v1? (And confirm `max_depth = 8`.)
