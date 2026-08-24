# Observability — logging, error reporting, metrics

What this process tells you about itself. Three surfaces, all wired at the entry
point (`crates/server/src/main.rs`) except `/metrics`:

| Surface | Where it goes | Configured by |
| --- | --- | --- |
| Structured logs | stdout | `RUST_LOG` |
| Error reports | Sentry (optional) | `SENTRY_DSN`, `SENTRY_ENVIRONMENT`, `PUMPER_BUILD_ID` |
| Operational metrics | `GET /metrics` (Prometheus text) | always on |

## Logging

`tracing` + `tracing-subscriber`, initialized once in `main` before the tokio
runtime is built. The filter is `RUST_LOG` (`EnvFilter`), defaulting to `info`
when it is unset or unparseable. Output is the default `fmt` layer on stdout —
there is no log file, no rotation, and no JSON formatter; the process is expected
to be run under something that captures stdout.

HTTP requests are traced by `tower_http::trace::TraceLayer` on the router.

## Error reporting (Sentry)

`sentry` + `sentry-tracing`, composed **beside** the stdout layer rather than in
place of it — enabling Sentry does not change what is printed. With a client
initialized, the default `sentry-tracing` event filter maps:

- `tracing::error!` → a Sentry **event** (an issue),
- `tracing::warn!` / `info!` → **breadcrumbs** attached to the next event.

That is the whole scope. `traces_sample_rate` is **0.0** — no performance
tracing, no spans billed — and `send_default_pii` is **false**.

### Configuration

| Variable | Effect |
| --- | --- |
| `SENTRY_DSN` | The DSN to report to. **Unset, empty, or whitespace-only ⇒ reporting is off**, silently, and the process boots exactly as it did before Sentry existed. |
| `SENTRY_ENVIRONMENT` | Tags every event with the deployment (`production`, `staging`, …). Unset or blank ⇒ **`local`** — an unlabelled process is a local one until someone says otherwise, so unlabelled events never pool with production. |
| `PUMPER_BUILD_ID` | Reused verbatim as the Sentry **release**, so a Sentry release and an extraction-health run row name the same build. Unset or blank ⇒ the crate version (the same fallback `build_id()` uses). |

Names only — no credential value belongs in the repo. See
[`../deployment.md`](../deployment.md#environment-variables) for the full env
table and the `.env` loading rules.

### Degradation semantics

Error reporting is never allowed to be the reason the service is down:

- **No DSN** → reporting off, nothing logged. This is the default and the shipping
  posture for a local run.
- **Malformed DSN** → exactly **one `warn!` at boot** (`"SENTRY_DSN is set but
  unparseable (…); error reporting off"`), then the process continues with
  reporting off. A typo in a deploy env is an operator mistake, not an outage.
  The DSN itself is never echoed into the log.
- **Valid DSN** → reporting on, events tagged with environment + release.

The decision is an extracted pure function, `sentry_plan(dsn, environment,
release) -> SentryPlan` (`Disabled` | `Invalid(String)` | `Enabled(options)`),
which does no I/O, never panics, and returns no error a caller could `?` out of
`main`. It is guarded by `missing_dsn_disables_reporting_not_boot`,
`malformed_dsn_degrades_not_panics`,
`blank_environment_falls_back_to_local_not_untagged`, and
`release_tracks_build_id_not_empty`.

### Process shape this forces

`main` is **synchronous** and builds the tokio runtime itself, because the Sentry
transport owns a background thread that must not be born inside a runtime about
to be torn down (the final flush becomes unreliable). The client guard is bound
to a named local (`_sentry_guard`) so it lives for the whole process — binding it
to `_` would drop it immediately and silently stop reporting. Dropping the guard
flushes queued events.

## Metrics

`GET /metrics` serves Prometheus text (body cached ~5s so scrape bursts don't
re-run the aggregates): job counts by status, per-app permanent-failure counts,
job duration and queue-wait summaries, cost gauges, app and schedule counts,
webhook delivery health, remote egress split, and checkpoint failures by reason.
The full series list is in [runtime.md](runtime.md#metrics). `GET /health` is the
liveness probe.

**Three of those series are process counters** (`pumper_remote_egress_fetches`,
`pumper_checkpoint_failures_total`, `pumper_worker_claim_failures_total`) and reset on restart; the rest are DB-derived
and can therefore go *down* when retention prunes. Both contracts are stated in
each series' own `# HELP`, because a counter that silently resets and one that
silently shrinks fail an alert rule in different ways.

### The store's report on itself

The SQLite layer measures its own behaviour and exports it on the same endpoint.
Without it, "the store feels slow after a few weeks" has nothing to interrogate:
no monitoring agent watches an embedded database.

Keys are `(op, table, phase)` — an operation **family** and the table it touches,
never statement text (statements embed values, which is unbounded cardinality).
`phase` splits the wait for a pooled connection (`acquire`) from the statements
themselves (`execute`); they are never summed, because one indicts pool sizing
and the other indicts the query.

| Series | What it is |
| --- | --- |
| `pumper_store_ops_total{op,table,phase}` | Measured operations, process-lifetime |
| `pumper_store_slow_ops_total{op,table,phase}` | Operations at or past this key's slow line |
| `pumper_store_slow_line_seconds{op,table,phase}` | **The predicate** behind the count above — published on the same labels so "N slow ops" is never quoted without its N |
| `pumper_store_busy_total{op,table,phase}` | `SQLITE_BUSY` / `SQLITE_LOCKED` (or a pool timeout on `acquire`), classified by result code, never by message text |
| `pumper_store_errors_total{op,table,phase}` | Every other failure |
| `pumper_store_rows_total{op,table,phase}` | Rows touched — separates "the query got slower" from "the table got bigger" |
| `pumper_store_duration_p95_seconds{op,table,phase}` | Nearest-rank p95 over the key's 256-record window |
| `pumper_store_window_samples` / `pumper_store_window_seconds` | The window the p95 is derived over — n, and the wall-clock span it covers |
| `pumper_store_op_worst_seconds{op,table,phase}` | Worst single duration since start (lifetime; survives the ring wrapping) |
| `pumper_jobs_oldest_queued_age_seconds` / `..._running_age_seconds` | Whether the queue is **moving**, which `pumper_jobs{status}` cannot say |
| `pumper_store_bytes{part}` | `main` (page_count × page_size), `free` (freelist), `wal` (the sidecar on disk) |
| `pumper_store_pages{kind}`, `pumper_store_page_size_bytes` | The page accounting behind those bytes |
| `pumper_store_maintenance_passes_total{task,outcome}` | Maintenance passes — `ran` / `deferred` / `failed`, three different results |

Slow lines are **local-store** lines (2 ms for a pool handoff, 5 ms for an
indexed point write, 25 ms for the scanning families, 50 ms for a batch chunk
holding the writer lock). A server-derived 100 ms threshold would sit above every
pathology this exists to catch.

**The census is partial and stated.** Seven families are instrumented — job
enqueue, the atomic claim, the terminal verdicts, the recovery sweep, the status
aggregate, dataset writes, and maintenance passes. Schedules, watches, triggers,
deliveries, saved searches, the HTTP/research caches and the search index are
**not measured**, and a percentile here says nothing about them. The same
statement rides in the `# HELP` text and in `GET /datasets/doctor`'s `store`
block.

`GET /datasets/doctor` carries the on-demand version: p50 and p95 per key, the
window's sample count and span, the size facts, the recent maintenance passes,
and a `derived_by` block stating how every figure was recomputed.

## Boot-time logs that state what the process can observe

Two boot lines exist to close a gap between what is configured and what is
actually happening, and both are logs — neither blocks startup:

- **Extraction health** (`AppState::init`): one `info!` naming whether verdicts
  gate anything or the fleet is in soak mode.
- **Declared contracts** (`main::log_contract_observability`): `error!` when
  `catalog/data-sources.toml` will not parse — the worker's publish seam fails
  open per job, so an unparsable catalog means the whole fleet checks zero
  contracts while `/sources` used to keep rendering `contracts_enforce: true`.
  `info!` with the declared count otherwise. The same fact is served as
  `contracts.enforce_observed` on `GET /sources`; see
  [catalog.md](catalog.md).

Job-level live telemetry is a different surface: progress snapshots and terminal
outcomes stream over SSE (`GET /jobs/{id}/stream`, `GET /events` — see
[events-webhooks.md](events-webhooks.md)).

## Known gaps

- **Only the `pumper` server binary reports.** The one-shot maintenance binaries
  (`reindex`, `search-backfill`) have their own `main`s and do not initialize
  Sentry.
- **Errors only.** No distributed tracing, no span export, no profiling; the
  Sentry integration deliberately carries no request context beyond what
  `tracing` fields already put on the event.
- **No user/request identity on events** (`send_default_pii = false` and the API
  is unauthenticated, so there is no user to attach).
- **No alerting or dashboards checked in** — `/metrics` is served, nothing
  scrapes it in this repo, and there is no Alertmanager/Grafana config.
- Logs are stdout-only: no file sink, no rotation, no structured JSON mode.
