# Production Readiness Scorecard (pumper)

Ten dimensions. Each is 🔴 (ship-blocking gaps), 🟡 (works but unproven or rough), or 🟢 (machine evidence recorded this loop). The Ship Gate requires all 🟢 at two consecutive Verification Gates.

For each dimension: what green means, what evidence is required, and the audit prompt to hand a subagent at boot.

> pumper is a local-first service consumed over HTTP by other apps and agents on this machine. There is no UI, no auth, no billing, no market. Dimensions 5 (**Dataset value**), 7 (**API & client contract**), 9 (**Source & cost value**) and 10 (**Platform standards**) are the pumper-specific replacements for the SaaS originals — the method is identical, the subject is what changed.

## 1. Build, format & lint
- **Green means:** `just build` exits 0; `just fmt-check` clean; `just lint` clean (or only explicitly-waived lints with an `#[allow]` carrying a reason). `just ci` runs fmt-check + lint + test — the whole CI job — in one command.
- **Evidence:** the commands' exit codes from this loop, captured without pipes.
- **Audit:** run directly in the main loop (cheap, deterministic). Don't delegate.

## 2. Functional completeness
- **Green means:** every app in `crates/server/src/registry.rs` runs end-to-end and returns a real result; every route in the OpenAPI inventory behaves as documented; no `todo!()`, `unimplemented!()`, or silently-empty happy path reachable by a caller.
- **Evidence:** an app × route inventory in `state.md` with per-row status, verified by driving real jobs (dimension 4) — not by reading code.
- **Audit prompt:** "Inventory every app registered in `crates/server/src/registry.rs` and every route in the `EXPECTED` list in `crates/server/src/routes/mod.rs`. For each app: crate path, `name()`, `description()` param shape, which engine tier it uses, its cron schedule if any, and any `todo!`/`unimplemented!`/`TODO`/`FIXME` in its path. For each route: purpose, handler file:line. Return two tables: app | crate | engine | schedule | suspected state (works / stub / broken / unknown), and route | handler | suspected state."

## 3. Tests
- **Green means:** core logic (job queue state machine, dataset change detection, tiered fetch escalation, extraction rules, config parsing, route inventory) covered by unit/integration tests; `just test` green; `just test-ignored` compiles and its failures are understood and logged (they need real Chrome / wasm artifacts / wall-clock time, so they are not CI-gated).
- **Evidence:** full `just test` output from this loop with the test count, plus the `just test-ignored` run.
- **Audit prompt:** "Inventory the test setup: where tests live (in-file `#[cfg(test)]` vs `tests/`), what's covered vs not, which tests are `#[ignore]`d and why. List the 10 most important untested modules ranked by blast radius — queue/storage and dataset-store correctness first, then engines, then apps. Note any convention asserted in prose that has no inventory test guarding it (the EXPECTED-diff idiom in `crates/server/src/routes/mod.rs` is the canonical shape)."

## 4. Runtime acceptance
- **Green means:** every critical app journey (per the runtime-acceptance depth chosen at boot) has been driven as a **real job through a running server** (`just run`, i.e. `cargo run -p pumper-server --bin pumper`) and produced the expected `result` and artifacts, including the failure cases (upstream 404/403, timeout, retry, cancel). Spec in `references/dataset-and-runtime-acceptance.md`.
- **Evidence:** the job-run log per journey (job id, status, duration, result excerpt) recorded in `state.md`.
- **Audit prompt:** "Map how a job actually executes: `crates/server/src/worker.rs` (claim → run → timeout → retry → complete), `crates/server/src/scheduler.rs` (cron firing), crash recovery at startup in `crates/core/src/storage.rs`. List the observable states a job can reach and which are exercised by an existing test. Then list, per registered app, the minimal `params` payload that would exercise it, taken from its `description()`."

## 5. Dataset value & integrity
- **Green means:** every app that writes a dataset produces records with a **key that actually identifies the record**; change detection reports new/changed/unchanged correctly (a re-run of an unchanged source yields zero `changed`); migrations apply cleanly from an empty DB and from the current DB; exports and `/datasets/{app}/{ds}/changes|history|duplicates` return coherent data. Spec in `references/dataset-and-runtime-acceptance.md`.
- **Evidence:** the per-app dataset assertion table in `state.md` (idempotence re-run, key correctness, change-detection deltas), plus a from-scratch migration run.
- **Audit prompt:** "Map the dataset layer: `crates/core/src/dataset*`, the migrations in `crates/core/migrations/`, and every app that upserts records. For each dataset: the key fields, what a 'change' means, and whether a re-run against unchanged upstream data would report zero changes. Flag any key that is derived from a volatile field (timestamp, position, session id) — that is a silent-duplication bug. Return a table: app | dataset | key | change semantics | risk."

## 6. Resilience & safety posture
- **Green means:** retries with backoff bounded by `max_attempts`; per-job wall-clock timeouts actually cut work off; the per-domain governor and `robots.txt` handling are on the real fetch path; the WASM host enforces its fuel + memory caps; webhook HMAC signing verifies; no path-traversal or SQL-injection reachable from remote content; secrets never logged or echoed into job results.
- **NOT in scope:** the deliberate local-first trades in `ONBOARDING.md` §2 — no API auth, permissive CORS, `--dangerously-skip-permissions`, real cookies on disk, non-2xx bodies returned rather than raised. These are design decisions. Filing them is a false positive; if the loop believes one must change, that is a checkpoint question, not a backlog item.
- **Evidence:** a per-control probe table (control | how it was proven | result) — e.g. a job forced past its timeout, a plugin forced to exhaust fuel, a webhook delivered and its signature verified.
- **Audit prompt:** "Map the resilience controls: retry/backoff and `max_attempts` in the worker, per-job timeouts, the per-domain governor, robots handling in `core::crawl`, the wasmtime fuel/memory caps in `engine-wasm`, webhook HMAC signing. For each: where it is enforced (file:line) and whether a test proves it. Separately, grep for remote-input hazards: artifact/plugin paths built from remote strings, string-built SQL, unbounded body reads, unbounded redirect chains, secrets in `tracing` or in job results. Do NOT report the intentional trades listed in ONBOARDING §2."

## 7. API & client contract
- **Green means:** the OpenAPI spec matches the routing table (the `spec_covers_exactly_the_registered_routes` test is green), `clients/typescript` builds and its types match the live responses, and `docs/features/http-api.md` + `docs/features/sdk-typescript.md` describe the current surface with no drift.
- **Evidence:** the route-inventory test result, the SDK build/test output, and a diff-check of a live response against the documented shape for at least one route per doc section.
- **Audit prompt:** "Compare three surfaces: the `EXPECTED` route list in `crates/server/src/routes/mod.rs`, the exports/types in `clients/typescript/src/**`, and `docs/features/http-api.md` + `docs/features/sdk-typescript.md`. Return a three-way table: route | in spec | in SDK | in docs, and flag every row that isn't all three. Note any response shape the SDK types disagree with."

## 8. Ops readiness
- **Green means:** every key read by `crates/core/src/config.rs` appears in `config.toml`'s commented reference with its default; `README.md` covers run and configure; crash recovery re-queues in-flight jobs at startup (proven, not assumed); `/health` and `/metrics` answer and their gauges are meaningful; `RUST_LOG` produces useful output; `data/` layout (db, artifacts, browser profile, plugins) is documented and gitignored.
- **Evidence:** the pre-flight checklist below with per-item proof, including a kill-and-restart run showing an in-flight job re-queued.
- **Audit prompt:** "Inventory ops: every config key read in `crates/core/src/config.rs` vs documented in `config.toml`; the startup sequence in `crates/server/src/state.rs` (engine construction, migrations, crash recovery); what `/health` and `/metrics` actually report; the `data/` directory layout. Return gaps."

## 9. Source & cost value
- **Green means:** every scraping app earns its place — the upstream source is still the best available one for its data, the `confidence` and `cadence` recorded in `catalog/data-sources.toml` are honest, the cost per useful record (especially for `claude`-tier apps) is defensible against the alternatives, and a fresh consumer can get real value from the service in one session. Full spec: `references/value-validation.md`.
- **Evidence:** `value-case.md` (cited sources ≤30 days old) + a passing cold-start journey (fresh `data/`, service booted, one real dataset produced) + the user's explicit checkpoint sign-off on the source/cost claims.
- **Audit prompt (web-research lens):** "For each source in `catalog/data-sources.toml`, research with citations: (1) is this still the authoritative/most complete endpoint for that data, or has the publisher moved to a bulk download, an official API, or a successor portal? (2) does an official API or dataset exist that would replace a scrape? (3) what does the data cost elsewhere (paid API pricing, commercial dataset vendors) — the number that values what pumper produces. Return a table per source with URL + access date for every claim; label vendor marketing as claims."

## 10. Platform standards (observability · docs sync · catalog & map parity)
- **Green means:** errors and job outcomes are observable (`tracing` at the right levels, `/metrics`, `/events` SSE, webhook delivery log); `docs/features/*` is in sync with source per `scripts/docs/feature-doc-map.json` and the Stop hook; `catalog/data-sources.toml` matches the registry (no live-but-unregistered rows, no registered-but-uncatalogued apps); `context-map.json` accounts for every source file. Full spec: `references/platform-standards.md`.
- **Evidence:** a captured failing job visible in `/events` and counted in `/metrics`; the doc-sync checker run; the catalog↔registry and context-map↔filesystem parity diffs at zero.
- **Audit prompt:** run the three audit prompts in `references/platform-standards.md` (observability, docs sync, catalog & map parity) as one lens; file a backlog item per gap, tagged 10-Plat.

## Ship gate — pre-flight checklist

Run at Phase 4. Every line needs recorded proof (command output, file path, or job id):

- [ ] All 10 dimensions 🟢 at two consecutive Verification Gates
- [ ] Release build boots and serves (`cargo build --release -p pumper-server --bin pumper`, run it, hit `/health` and one job round-trip)
- [ ] From-scratch start: empty `data/`, migrations apply, service boots, one app produces a dataset
- [ ] Crash recovery: kill the process with a job in flight → restart → the job is re-queued, not lost
- [ ] Every config key read by code is documented in `config.toml`; no secret committed; `data/` gitignored
- [ ] Cron schedules: each enabled schedule's expression is a valid 6-field expr and its next fire time is what the catalog claims
- [ ] Webhooks: a delivery is signed, verified, and visible in `/webhooks/deliveries`; a replay works
- [ ] Error visibility: a forced app failure surfaces in the job `error`, in `/events`, and in `/metrics`
- [ ] Claude-tier cost: `max_budget_usd` enforced; `/costs` and `/jobs/{id}/costs` report real numbers
- [ ] API contract: route-inventory test green, SDK builds, `docs/features/http-api.md` matches
- [ ] Value case current (research ≤30 days), no weak-source verdict or ✗ reality-check unaddressed, user confirmed at a checkpoint
- [ ] Cold-start journey green: fresh checkout + empty `data/` → real dataset in the first session
- [ ] Docs: `docs/features/*` sync check clean; `README.md` and `ONBOARDING.md` true; `context-map.json` accounts for every file
- [ ] `SHIP_REPORT.md` written and committed
