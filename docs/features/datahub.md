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
| `govern_pause_max_stale_secs` | `900` | How long the `cost:pause` set may survive **without a successful poll** before it expires loudly (3× the default interval). `0` keeps the old behavior: pauses freeze until governance can see DataHub again. See [Outage staleness](#outage-staleness-governance-that-has-gone-blind). |

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
  - `govern` — `{enabled, interval_secs, paused_apps, last_poll, recent_actions}`.
  - Everything here is in-memory **except `govern.recent_actions`** (the durable audit trail, below): a restart zeroes the counters and clears the paused set.
- **`GET /datahub/governance/preview`** — what a governance poll would do **right now**, doing none of it. See [Preview](#preview-get-datahubgovernancepreview).

## Governance actuator (`govern = true`)

Opt-in, default OFF, piggybacked on the scheduler tick. Each poll reads deprecation / tags / assertion health for **every** Pumper dataset URN over the GMS GraphQL surface (`POST {gms}/api/graphql`) and maps the answers to three actions. Every action is loud-logged and listed in `govern.last_poll.actions`.

**Poll mechanics.** Reads run 4 at a time on a dedicated **10s-timeout** client (not the emitter's 60s write client), so one poll's worst case is `ceil(datasets / 4) × 10s` — 50s for 20 datasets. Exceeding that budget is a warn with the measured duration; the summary carries `poll_ms` and `budget_secs`. The next poll is gated on the previous one's **completion**, so two polls can never overlap and race the paused-app set.

| DataHub signal | Action | Blast radius |
| --- | --- | --- |
| dataset **deprecated** | disable that dataset's app's schedules, **once per transition** | **One deprecated dataset disables ALL of that app's catalog-managed schedules**, not just the ones feeding that dataset — the app is the unit, because the signal is read per dataset but schedules are not tied to datasets. Fenced to `managed_by = "catalog"` in SQL (M19) — hand-made schedules are never touched, and `GET /datahub/governance/preview` names the exact ids. **Acted on the change, not the level:** the disable fires when the deprecation appears; while the flag stands, a `POST /schedules/{id}/enabled` re-enable is **respected** (and survives a restart — see [Transition semantics](#transition-semantics-governance-acts-on-change-not-on-level)). **Not automatically reversible:** un-deprecating does *not* re-enable them; re-enable via `POST /catalog/reconcile` or the schedule API. With `[catalog] auto_reconcile = true` the **boot** reconcile re-enables them silently — the two actuators disagree, and the last one to run wins. |
| tag **`cost:pause`** on any of the app's datasets | force that app's job budget to `$0` for **new** jobs | The budget governor then runs free tiers only — the Claude tier is skipped. Nothing running is cancelled, no job fails. Fully reversible: the paused set is recomputed wholesale each poll, so removing the tag resumes the app on the next poll. |
| **failing assertions** (`health[].type == "ASSERTIONS" && status == "FAIL"`) | enqueue one immediate sync job for that dataset's app | `max_attempts = 2`, app default params, and an **hour-bucketed idempotency key** (`datahub-govern-sync:<app>:<dataset>:<YYYY-MM-DDTHH>`) so a persistently failing assertion enqueues at most one job per hour, not one per poll. Apps not in the registry are skipped with a warn. |

### Preview (`GET /datahub/governance/preview`)

The dry run. It reads the same remote state a poll reads and reports the plan that state maps to — and **writes nothing**: no schedule is disabled, no job enqueued, the paused set is untouched. It deliberately works with `govern = false`, because "what happens if I turn this on?" has to be answerable *before* the switch is flipped. `just enforcement-preview`'s sibling for the pull half.

```
GET /datahub/governance/preview
{
  "at": "...", "governing": false, "gms_url": "...", "env": "PROD",
  "datasets_polled": 15, "poll_ms": 412, "budget_secs": 40,
  "quiet": false,
  "would": {
    "disable_schedules": [{"app": "eu-sedia", "dataset": "calls", "evidence": "deprecation",
                           "schedule_ids": ["catalog-eu-sedia"], "note": "..."}],
    "pause_apps": ["ca-grants"],
    "resume_apps": [],
    "enqueue_syncs": [{"app": "grants-gov", "dataset": "opportunities", "evidence": "assertions",
                       "registered": true, "idempotency_key": "datahub-govern-sync:...:2026-08-04T09", "note": "..."}]
  },
  "paused_now": [], "read_errors": [], "poll_would_abort": false,
  "totals": {"schedules_disabled": 1, "apps_paused": 1, "syncs_enqueued": 1, "read_errors": 0}
}
```

- `quiet: true` means a poll right now would change nothing.
- `schedule_ids` names the **exact** catalog-managed rows a deprecation would disable. Hand-made schedules never appear, because they are never touched — a preview that guessed would be worse than no preview.
- `idempotency_key` is the key the real enqueue would use, so you can check whether this hour's sync already exists.
- Two honest differences from a real poll, both reported rather than hidden: a read error here is **collected** (`read_errors`) instead of aborting, and `poll_would_abort` says whether a real poll would therefore have done nothing at all. An empty plan with `poll_would_abort: true` is "blind", not "quiet".
- **409** while `[datahub] enabled = false` (there is no GMS to read). `govern` may stay `false`.

### Audit trail (durable)

Every **executed** governance action is recorded in the `datahub_govern_actions` table (migration 0037) and emitted on the event bus. The anti-pattern this closes: an action lived only in `govern.last_poll` — the last poll's in-memory summary, erased by the next poll and by every restart — while its *effect* (a disabled schedule, a zeroed budget) was durable and unexplained.

| column | meaning |
| --- | --- |
| `action` | `disable_schedule` · `enqueue_sync` · `pause_app` · `resume_app` |
| `target` | the Pumper app acted on |
| `dataset` | the dataset whose remote state was the evidence |
| `subject` | the row produced or changed: a schedule id, a job id |
| `evidence` | `deprecation` · `cost:pause` · `assertions` — what to go look at in DataHub |
| `detail` | free text: the schedule's cron, the sync's idempotency key |
| `created_at` | RFC 3339 UTC |

- **Read it** on `GET /datahub/status` → `govern.recent_actions` (newest 20).
- **Events**: each action is also emitted on `/events` as a `datahub_govern` status event whose `result` carries the same fields plus `audit_id`, so a remote-driven change shows up in the same stream as the job transitions it causes. The event is emitted even if the row could not be written — for a trail of things that are already true, visibility beats consistency.
- **Retention**: age-bounded at **90 days**, pruned from the governance poll itself at most hourly (the table only grows while governance runs). Diagnostic, not evidence, like `trigger_runs`: an aged-out row loses the explanation, not the state.
- Only *transitions* are audited for pauses: an app that was already paused is not news.

### Transition semantics: governance acts on CHANGE, not on level

The deprecation → disable action fires on the **transition** (not deprecated → deprecated), and the level it last acted on is persisted per app in `datahub_govern_levels` (migration 0038).

- **A manual re-enable is respected.** `POST /schedules/{id}/enabled` after a governance disable stands until DataHub changes its mind. Previously the still-standing flag re-disabled it on the next poll (≤ `govern_interval_secs`), forever, with no override.
- **A restart does not flap.** The memory is in SQLite, so a fresh process seeing a deprecation it has already acted on does nothing. That is why the level lives in its own table rather than in the age-pruned audit trail: pruning the memory would resurrect the flap.
- **Un-deprecating re-arms, it does not undo.** When the flag clears, the recorded level clears too (no action taken — the schedule stays as it is), so a *later* re-deprecation is a fresh transition and acts again.
- **Suppression is visible**: the poll summary counts `schedules_suppressed` and names each no-op in `actions`; the preview marks the entry `suppressed: true` with an empty `schedule_ids`.
- Fail-closed: if the level memory cannot be read, the poll disables **nothing** that cycle rather than assuming a clean slate and stomping a re-enable.
- The other two actions stay level-based on purpose. `cost:pause` is already reversible by construction (the set is recomputed wholesale, so removing the tag resumes), and a sync for a *still-failing* assertion is the healing attempt itself — bounded by the hour-bucketed idempotency key, not by a transition.

### Outage staleness: governance that has gone blind

The first read error aborts the entire poll *before any action is planned* — an unreachable or absent DataHub is a clean no-op, and datasets DataHub has never seen read as all-false (only explicit remote state acts).

That fail-closed read had one asymmetric consequence: because `paused_apps` could only be recomputed on a **successful** poll, an app paused just before an outage stayed at budget `$0` for the entire outage, with no way to un-read the tag — enforcement of state nobody could refute.

Now the pause set has a shelf life. When no poll has succeeded for `govern_pause_max_stale_secs` (default 900s = 3× the default interval), the set **expires loudly** on the next failed poll: a `warn` naming how long governance has been blind, an `expire_pause` audit row per app with `evidence: "stale"`, and the matching bus event. Normal budgets resume. Nothing is lost — the very next successful poll re-reads the tags and re-pauses whatever is still tagged.

- `GET /datahub/status` → `govern.secs_since_successful_poll` and `govern.pause_max_stale_secs` show how close the set is to expiring.
- A short blip changes nothing: expiry needs the *window*, not merely a failed poll.
- `govern_pause_max_stale_secs = 0` opts out (freeze until governance can see again).
- Direction of the trade: expiring is fail-open on **spend**, which is the same direction the module already leaned (a restart cleared the pause set anyway, in-memory as it is). Keeping a $0 budget indefinitely on the strength of a tag nobody can re-read is enforcement without observation; a paused app that resumes normal budgets still runs its own configured budget cap.

## Verified against a live instance (quickstart v1.6, 2026-07-23)

The v1 ingestion envelope accepts all emitted aspects **including the timeseries ones** (`operation`, `datasetProfile`) — no v3 route needed. Full backfill (15 datasets / 60 entities), per-run emission, and three-source accumulated lineage on `grants.unified` (`grants-gov` + `ca-grants` + `eu-sedia` → unified) all confirmed via GMS readback.

## Known gaps

- Schema inference is top-level-fields-only (no nested field paths), from a single sample record.
- **No retry/DLQ, by design** (unlike webhooks): a failed emission is recorded (`emissions.last_error` + the `failed` counter) and never re-sent. Metadata is idempotent and fully re-derived on every run, so the next run — or a manual `/datahub/sync` — self-heals it; a queue would only buy staleness insurance. The cost is honest: a batch that fails halfway leaves the earlier batches ingested and the rest missing until the next emission.
- Emitting all own-namespace datasets on success slightly over-claims freshness for apps whose runs deliberately touch only a subset of their datasets.
- The lineage read-merge is not concurrency-safe across *writers* (two jobs finishing at once could interleave read-then-write on the same derived dataset). The full-sync overlap guard removes the sync-vs-sync case only.
- A deprecation-driven schedule disable is not undone by un-deprecating (the preview and the audit trail make it visible; they do not reverse it).
- External upstream sources (`catalog/data-sources.toml`) are not modeled as DataHub entities — lineage starts at Pumper's own datasets.
- Column lineage requires a declarative RuleSet in job params; code-driven apps get table-level lineage only.
