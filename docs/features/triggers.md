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

`GET/POST /triggers` (kind-aware validation), `DELETE /triggers/{id}`, `POST /triggers/{id}/enabled`, `POST /triggers/{id}/test` (dry-run against the most recent source job → `would_fire` + resolved params + reason; `?fire=true` enqueues for real, idempotency-bypassed), `GET /triggers/{id}/runs` (lineage **and** the decision ledger — below).

## The decision ledger (`trigger_runs` table)

Every evaluation of a trigger against one source event is recorded, fires and **skips** alike — the negatives were previously log-only or entirely silent, so "why did my pipeline not fire?" was unanswerable from the API.

`GET /triggers/{id}/runs` → `{trigger_id, count, runs, decisions, next_cursor}`; **404** when the trigger does not exist (it used to answer `200 {count: 0}`, which reads as "never fired").

- `runs` — the jobs this trigger enqueued (`jobs.trigger_id`), newest first, as before.
- `decisions` — ledger rows, newest first, keyset-paged with `cursor` (`limit` clamped 1–500 for both). Each row: `{id, trigger_id, outcome, source_kind, source_job_id?, dataset?, event_id?, job_id?, detail?, created_at}`.

| `outcome` | Meaning |
|---|---|
| `fired` | The hop was enqueued (`job_id` names it; `detail` is the idempotency key) |
| `no_change_match` | No revision in that dataset's batch passed `on_change` |
| `status_mismatch` | The source job's terminal status did not pass `on_status` |
| `filter_miss` | The inbound payload did not satisfy the trigger's JSON-path filters |
| `bad_filters` | The trigger's stored filter specs no longer parse |
| `predicate_veto` | A predicate plugin returned `pass=false` (or failed with `on_error = "skip"`) |
| `plugin_missing` | A **configured** hook names a plugin the host has not loaded, so the hook did nothing — the predicate did not gate, the transform did not shape (`detail` = the plugin name). The hop still fired: fail-open is the contract. Usually means `just plugins-install` was never run — see [trigger-plugins.md](trigger-plugins.md) |
| `cycle` / `depth` | Provenance guards (`chain`, `[triggers] max_depth`) |
| `target_unregistered` | `target_app` is not a registered app (`detail` = the app) |
| `bad_params` | The resolved hop params (the trigger's `params` template with the `_trigger` envelope merged over it) fail the **target** app's declared `params_schema`, so the hop was not enqueued. `detail` is the same pointer-path message `POST /apps/{name}/jobs` answers with. Fix the template — a hop that cannot pass the enqueue door was only ever going to fail on the target app |
| `dedup` | A job already exists for this hop's idempotency key (`detail` = the key) |
| `enqueue_failed` | The enqueue itself errored (`detail` = the error) |
| `eval_set_error` | The evaluation set could not be loaded, dropping **every** edge of that source event. Recorded against the sentinel `trigger_id = "*"`, since no individual trigger was reached — read it with `GET /triggers/*/runs`… which 404s, so query the table directly for now (see gaps) |

Ledger writes are **fail-open**: a write that fails is logged loudly and the hop proceeds regardless — the observability table must never become a failure mode for the pipeline.

**Retention**: bounded by default at 14 days, swept at most hourly from the worker's reaper tick. Unlike the opt-in evidence ledgers (`cost_events`, `webhook_deliveries`, …) this one is diagnostic and high-write, so it bounds itself.

**Known gaps**: the retention window is a constant, not a config key; `eval_set_error` rows are not reachable through `GET /triggers/{id}/runs` (their `trigger_id` is the `"*"` sentinel and that id is not a trigger); there is no cross-trigger "all decisions" list endpoint.

`GET /triggers` takes an optional `app` filter and is **dual-mode**: bare `{triggers: [...]}` by default, or `{items, next_cursor}` when a `cursor` param is present (even empty) — an opaque keyset cursor. `limit` is clamped 1–500 in **both** modes, so an uncursored list can never stream the whole table.

`POST /triggers` body: `{name?, source_kind, source_app, source_dataset?, on_change?, on_status?, target_app, params?, budget_usd?, priority?, max_attempts?, filters?, plugins?}`. The `plugins` object attaches sandboxed WASM hooks — see [trigger-plugins.md](trigger-plugins.md).

## Evaluation-set caching

The set of enabled triggers for a scope — `(dataset|job, app)` or `(external, ingress source)` — is cached in memory with its filter specs already parsed, so a completion or inbound event with nothing configured performs **no** query. Coherence is a generation counter on the `triggers` table, bumped after every create / enable-toggle / delete: a CRUD operation is visible to the very next firing decision, and there is no stale window. The cache is per-process and in-memory only; nothing about it is configurable or observable through the API.

## Non-goals (by design)

Fan-in/join barriers, `${…}` param templating, per-record fan-out, named pipeline grouping/UI, backfill on create.
