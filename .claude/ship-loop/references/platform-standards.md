# Dimension 10 — Platform standards (observability · docs sync · catalog & map parity)

Repo-wide requirements pumper must meet before its ship gate. Unlike dimensions 1–9 (discovered per audit), these are PRESCRIBED by the repo's own contracts (`.claude/CLAUDE.md`, `ONBOARDING.md` §9, `catalog/README.md`): the boot audit checks compliance and files a backlog item per gap, tagged `10-Plat`.

(The pumper replacement for the SaaS original's monitoring · i18n · BYOM trio. pumper has no UI to translate and no users to bill; what it does have is a fleet of agents reading its docs and catalog literally, so *keeping the written surface true* is the equivalent load-bearing standard.)

## 1. Observability

**Green means:**
- **Structured `tracing` at the right levels** across the job path: `info` for lifecycle (claimed, running, succeeded, failed), `warn` for retryable degradation (escalated a tier, rate-limited, retrying), `error` for terminal failures with enough context to identify the job, the app, and the URL. `debug` carries payload detail; `RUST_LOG=debug` must be genuinely useful, not a firehose of one module.
- **No secrets or credentials in any log line or job result** — config values from `[claude]`, cookie contents, and API keys in app params must never reach `tracing` or a webhook payload.
- **`/metrics` is meaningful**: jobs by status, per-app counters, schedules, and cost gauges that actually move when work happens. A gauge that is always zero is worse than no gauge.
- **`/events` SSE and `/jobs/{id}/stream`** report every transition, and both terminate cleanly on a terminal state.
- **Failures are discoverable after the fact** — a job that failed overnight is findable through `GET /jobs?status=failed`, counted in `/metrics`, and (if a webhook is configured) delivered with a signed payload recorded in `/webhooks/deliveries`.
- **Claude-tier spend is visible** — `/costs` and `/jobs/{id}/costs` report real numbers, and a budget refusal is logged as a refusal, not as a generic failure.

**Evidence:** a deliberately failed job traced end-to-end — its `error` field, its line in `/events`, its count in `/metrics`, its webhook delivery — plus a `grep` proving no secret-bearing field is logged.
**Audit prompt:** "Map observability: every `tracing::{info,warn,error,debug}` call on the job execution path (worker, scheduler, engines, fetcher) with its level and what context it carries; what `/metrics` exposes and where each gauge is updated; how `/events` and `/jobs/{id}/stream` are fed; where costs are recorded. Then grep for secret leakage: any log or serialized result that could carry an API key, a cookie, or a `config.toml` credential value. Return gaps, ranked by 'could an operator diagnose an overnight failure from this alone?'"

## 2. Documentation sync

**Green means:**
- Every mapped source glob in `scripts/docs/feature-doc-map.json` has a live feature doc under `docs/features/`, and every doc describes what IS (surface, params, data model, known gaps) rather than what might be.
- Running the checker is clean: `node scripts/docs/check-doc-sync.mjs` (the Stop hook in `.claude/settings.json` runs it per turn; run it explicitly at the gate).
- Every feature area has a map entry — a new crate, app, or server module without one is a gap by definition, because the hook cannot see it.
- `README.md`, `ONBOARDING.md`, and `config.toml`'s commented reference are true: no capability claimed that the code lacks, none present that the docs omit, no config key undocumented.
- The docs describe the *current* API: cross-check `docs/features/http-api.md` against the `EXPECTED` route inventory in `crates/server/src/routes/mod.rs`.

**Evidence:** the doc-sync checker output; the three-way route/SDK/docs diff from dimension 7; a spot-read of two feature docs against their source globs.
**Audit prompt:** "Read `scripts/docs/feature-doc-map.json` and `docs/features/README.md`. For each mapped glob: does the doc exist, and does its described surface (endpoints, params, datasets, config keys) match the current source? For each crate under `crates/` and each app under `crates/apps/`: is it covered by some map entry? Then check `README.md`, `ONBOARDING.md`, and `config.toml` for claims the code no longer supports or capabilities the docs omit. Return a gap list, one item per doc."

## 3. Catalog & context-map parity

**Green means:**
- **Catalog ↔ registry parity:** every app in `crates/server/src/registry.rs` has a row in `catalog/data-sources.toml` whose `app` field points at the right crate; every row with `status = "live"` corresponds to a registered app; every row with a `cron` matches a schedule the scheduler would actually fire. A `live` row for an unregistered app is a lie told to every downstream consumer.
- **Catalog field honesty:** `market`, `category`, `engine`, `access`, `cadence`, and `confidence` reflect what the app actually does (the `engine` field in particular drifts when an app is re-tiered).
- **Context-map parity:** every source file under `crates/`, `clients/`, and `catalog/` is claimed by exactly one context in `context-map.json`; no context lists a path that no longer exists. `.claude/CLAUDE.md` makes keeping it accurate a hard requirement for anyone who changes file ownership.
- The parity checks are cheap and should be re-run at every Verification Gate, not just at boot.

**Evidence:** the catalog↔registry diff at zero; the context-map↔filesystem diff at zero (unclaimed files and dangling paths both listed and empty).
**Audit prompt:** "Three parity diffs. (1) Registry vs catalog: list the app constructors in `crates/server/src/registry.rs`, list `[[source]]` entries in `catalog/data-sources.toml`, and report apps-without-rows, live-rows-without-apps, and rows whose `app` path doesn't exist. (2) Catalog field honesty: for each row, compare `engine` against which engine the app's code actually uses and `cron` against the schedules the scheduler registers. (3) Context map: collect every file path under `crates/`, `clients/`, `catalog/`, walk `context-map.json` `groups[].contexts[].filePaths`, and report unclaimed files and dangling paths. Return three tables."

## How it runs in the loop

- **Boot:** an extra audit lens runs the three audit prompts above; every gap becomes a backlog item tagged `10-Plat`.
- **Per milestone:** the doc-sync rule is not a dimension-10-only concern — Phase 2 step 4 requires the coupled doc update in the same commit as any user/API-visible change. Dimension 10 catches the *pre-existing* debt; the per-item rule prevents new debt.
- **Value case tie-in:** catalog honesty is where dimension 9's verdicts land. A `switch`/`cut`/`re-tier` verdict that hasn't reached `data-sources.toml` keeps dimension 10 amber.
- **Ship gate additions:** doc-sync checker clean; catalog↔registry and context-map parity at zero; a real failed job proven visible end-to-end through logs, `/metrics`, `/events`, and a webhook delivery.
