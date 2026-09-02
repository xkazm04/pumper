# Workflow runs

**Status: implemented.** Migration `0046`. No config key — workflows are inert
until one is declared, and a node with none behaves exactly as before.

A **workflow** is a *declared plan*: a named DAG of steps, each of which becomes
an ordinary job. It gives the queue three things it did not have — a **fan-in
join barrier**, `{{steps.X.result.path}}` **param templating**, and one **run
identity** that carries a single budget envelope and a single rolled-up receipt.

## Why this is not a trigger

[triggers.md](triggers.md) lists fan-in barriers, `${…}` templating and named
pipeline grouping as **non-goals by design**, and that is the right call for what
a trigger is: a *standing reactive edge* — "when X happens, also do Y" — one hop
at a time. What triggers cannot express is a **submitted plan**: crawl these
three seeds in parallel, and when all three are done, extract. Before workflows,
an agent drove that with N `enqueue_job`/`wait_job` round-trips and its own
bookkeeping, and a cron could not express it at all.

Workflows do not change the trigger engine. A workflow step is an ordinary job,
so its terminal event still fires whatever triggers watch it; the barrier is
evaluated in `finalize_with_stages` **beside** `fire_terminal_triggers`, never
inside it.

## The spec

```json
{
  "steps": {
    "crawl_a":  { "app": "crawl", "params": { "seed": "https://a.example" } },
    "crawl_b":  { "app": "crawl", "params": { "seed": "https://b.example" } },
    "extract": {
      "app": "extractor",
      "after": { "all_of": ["crawl_a", "crawl_b"] },
      "params": { "pages": "{{steps.crawl_a.result.pages}}" },
      "budget_usd": 0.25,
      "priority": 5,
      "max_attempts": 3
    }
  },
  "budget_usd": 1.00,
  "on_failure": "fail_fast"
}
```

| Field | Meaning |
| --- | --- |
| `steps.<name>.app` | A registered app. Checked at create — an unknown app is a 422, never a run that fails later. |
| `steps.<name>.params` | Params template. Shallow-merges over the app's `default_params` exactly like `POST /apps/{name}/jobs`. |
| `steps.<name>.after` | The join barrier: `{"all_of": [names]}` (a bare array is accepted). Absent/empty = a **root** step, enqueued when the run opens. Every named step must have **succeeded** before this one is enqueueable. |
| `steps.<name>.budget_usd` | Per-step ceiling. A **cap, never a grant**: it is clamped to what is left of the run envelope. |
| `steps.<name>.priority` / `max_attempts` | Passed straight to the step's job. Defaults `0` / `1`. |
| `budget_usd` | The run envelope. Every step's ceiling is clamped to `envelope − spent`, so the steps collectively can never exceed it. Refused if ≤ 0. |
| `on_failure` | `fail_fast` (default) — a step ending badly skips **every** unstarted step; `continue` — only steps whose barrier can no longer be satisfied are skipped, so independent branches finish. Either way the run ends `failed`. |

**Validated at create**, all of it, as one 422 listing every violation: an
`after` naming a step that does not exist, a dependency cycle, a self-dependency,
an unregistered app, a non-positive budget, declared step budgets summing past
the envelope, more than 100 steps, and `any_of` (see Known gaps — refused **by
name** rather than silently read as `all_of`, which would be a different plan).

Params are schema-checked at create only for steps whose params carry **no**
template — a template cannot be validated before it is rendered. The create
response says which is which, per step, as `params_validated: true|false`.
A templated step's rendered params are validated when it is enqueued, and a
failure there fails that step with the pointer-path message.

## Templating

`{{steps.<step>.result}}` and `{{steps.<step>.result.a.b.0}}` are the only forms.
Only steps that have **succeeded** are readable, which in practice means the
steps in this step's `after` set.

- A **whole-string** template substitutes the JSON value with its type intact —
  `"{{steps.crawl.result.urls}}"` renders as an array, not as text.
- An **embedded** token interpolates into the surrounding string; scalars render
  verbatim, containers as compact JSON.
- A **miss is a refusal.** An unresolvable path fails the step with a message
  naming the path. It is never rendered as `null` or left as the literal
  `{{…}}` — a step that runs with silently wrong params produces a plausible,
  wrong result, which is worse than a failure.

## Lifecycle

1. `POST /workflows/{id}/runs` opens a run: every step is seeded `pending` and
   the **root** steps are claimed and enqueued.
2. A step job ends. `finalize_with_stages` calls `workflow::on_step_terminal`,
   which lands that cell's outcome, refreshes the envelope's spend from
   `cost_events`, then re-evaluates the whole graph: cascade skips, enqueue
   everything whose barrier is now satisfied, and close the run if nothing is
   open.
3. The run ends `succeeded` (every step succeeded), `failed` (anything failed or
   was skipped — a plan that did not do what it declared did not succeed), or
   `cancelled`.

**Exactly-once is a guarded UPDATE, not a lock.** Two upstreams of a diamond can
finish on different worker tasks at the same instant and both compute the same
ready join step; both then run `UPDATE workflow_steps SET status='queued' WHERE
… AND status='pending'`, and only the one that changes a row enqueues. The same
idiom fences step completion and run completion, so a repeated terminal event or
a re-evaluation after a restart is a no-op rather than a second fan-out.

**Fail-open.** A storage error inside the hook is logged and swallowed: a
workflow that cannot advance must not also cost the job its callback, its
webhook or its triggers.

**Suspend/resume is free.** Steps are jobs, so a shutdown parks them at their
checkpoints and the reaper re-queues stale leases; the run itself is durable
rows, so a boot needs no recovery beyond the next terminal event re-evaluating
the barrier.

## API

| Method & path | What it does |
| --- | --- |
| `POST /workflows` | Declare a plan: `{name, spec, cron?}`. 201 with a per-step report; 409 if the name is taken; 422 on any spec violation. |
| `GET /workflows` | Every declared plan. |
| `GET /workflows/{id}` | One plan. `{id}` is the id **or** the name. |
| `DELETE /workflows/{id}` | Delete a plan. Open runs are **not** cancelled; they fail with "the workflow definition was deleted while this run was open" at their next advance. |
| `POST /workflows/{id}/runs` | Open a run: `{budget_usd?, idempotency_key?}`, `Idempotency-Key` header takes precedence. 202 on create, 200 on an idempotency replay (the original run). |
| `GET /workflows/{id}/runs?limit=` | This plan's runs, newest first. |
| `GET /workflow-runs/{run_id}` | The step matrix plus the rolled-up receipt. |
| `DELETE /workflow-runs/{run_id}` | Cancel: every open step is closed and each one that already had a job goes through the ordinary `DELETE /jobs/{id}` door. Returns `{cancelled, jobs_cancelled}`. |

### The run report

`GET /workflow-runs/{run_id}` returns `{run, workflow, steps, receipt, unknown}`:

- `steps[]` — `{step, status, job_id, depends_on, cost_usd, yield, error, finished_at}`.
- `receipt` — `{cost_usd, budget_usd, steps_total, steps_priced, yield}`. Cost is
  summed from `cost_events` over the run's whole job set; yield from `job_yield`.
- `unknown[]` — why the report is incomplete, in words. A step that never became
  a job (pending, skipped, refused at its own door) has `cost_usd: null`, **not**
  `$0`, and `unknown` says how many. A run whose apps report no
  `UpsertSummary`-shaped counts has an empty `yield` and `unknown` says so.

## Scheduling

A plan with a `cron` is fired by the scheduler, once per firing, on the same tick
the schedule reconcile runs on. The overlap guard is **"the newest run of this
plan is still open"** — the workflow analogue of the schedule guard. The first
tick after boot fires nothing (there is no previous pass to measure a missed
firing against), matching the schedule reconcile.

This is deliberately **not** a row in the `schedules` table: a schedule targets an
app and carries app params, and widening it to "app or workflow" would put a
nullable second target on every schedule read in the system. The cron lives on
the plan it schedules.

## MCP

Two tools, appended at the end of the tool list:

- `run_workflow {workflow, budget_usd?, idempotency_key?}` — behind
  `[mcp] allow_enqueue`, exactly like `enqueue_job`. `budget_usd` is clamped to
  `[mcp] max_job_budget_usd` and is the envelope for the **whole run**.
- `wait_workflow {run_id, timeout_secs?}` — settles on the run and returns the
  step matrix plus the rolled-up receipt. `timeout_secs` is clamped to
  `[mcp] wait_job_max_secs`, the same rail `wait_job` honours; hitting the
  deadline returns `timed_out: true` with the current matrix.

A crawl → extract → research pipeline is then **2 MCP calls and one receipt**
instead of six `enqueue_job`/`wait_job` round-trips plus the agent's own
bookkeeping.

## Events

Run lifecycle rides the ordinary event bus (`GET /events`, the MCP live stream)
as `status = "workflow.<running|succeeded|failed|cancelled>"`, with the **run id**
in the event's `job_id` slot and the **workflow name** in `app`. Step jobs emit
their own ordinary job events, unchanged.

## Data model

| Table | Columns |
| --- | --- |
| `workflow_defs` | `id, name (UNIQUE), spec_json, cron, enabled, created_at` |
| `workflow_runs` | `id, def_id, status, budget_usd, spent_usd, idempotency_key (UNIQUE), principal_id, root_id, error, started_at, finished_at` |
| `workflow_steps` | `(run_id, step) PK, job_id, depends_on JSON, status, result, error, finished_at` |

`jobs` gains three nullable columns: `workflow_run_id`, `workflow_step`, and
`root_id` (the chain correlation id; equal to the run id for a step job).
`EnqueueOptions` carries all three, so a step job is self-describing. Every step
job inherits the run's `principal_id`, so `GET /costs?principal=` prices a whole
plan to whoever asked for it. `Storage::job_chain_ids(id)` reads the pair back
(the in-memory `Job` does not carry it); the lineage bridge uses it to put
`workflowRunId`/`rootId` and an OpenLineage `parent` facet on every step's run
event, so a plan renders as one story downstream instead of N unrelated runs —
see [`datahub.md`](datahub.md#the-runevent).

Step-job dedup keys are `wf:{run_id}:{step}`, so a re-enqueue after a crash
returns the original job instead of doubling the work.

## Known gaps

- **`any_of` / quorum barriers** are not implemented. Refused by name at create.
- **`foreach` / per-record fan-out** is not implemented.
- **Retry-from-step** (`POST /workflow-runs/{id}/retry`) is not implemented; a
  failed run is re-run from the start as a new run.
- **Catalog `[[workflow]]`** reconciliation is not implemented — plans are
  created over HTTP or MCP only.
- **No SLA / deadline sweeper.** A run whose step job hangs is bounded by that
  job's own timeout and lease reaper, not by a run-level deadline.
- **No DataHub `dataProcessInstance`** emission for a run.
- **The barrier costs one indexed lookup per terminal job** (`workflow_steps` by
  `job_id`), including on nodes that have never declared a workflow. It is a
  single seek on a small table, not a cached answer like the trigger evaluation
  sets.
- **A step's `result` is stored on the cell** as well as on the job, so a very
  large step result is held twice. There is no cap on it beyond the job's own.
