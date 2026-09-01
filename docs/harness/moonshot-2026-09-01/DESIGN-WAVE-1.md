# Wave 1 Design — Foundations (2026-09-01)

Five builders, five worktrees, five branches off `master` (`63c78a2`), merged by the coordinator
in the order below once each reports. Cards: [INDEX.md](INDEX.md) + the group reports; the
`## Flow` of each card is the build order, this file trims it to a **v1 slice** and pins the
file scope so branches merge without stepping on each other.

## Shared rules (all builders)

1. **Read first**: `CLAUDE.md`, `.claude/CLAUDE.md` (doc-sync rule, "bug fixes ship as extracted,
   tested functions"), `ONBOARDING.md` §7–10, `MEMORY.md` (invariants), `docs/harness/harness-learnings.md`,
   your card in the group report, and the feature docs for every context you touch.
2. **Stay inside your file scope** (below). A needed change outside it is reported, not made —
   the coordinator resolves it at merge. The only files every builder may touch are listed under
   *Shared surfaces*, with the rule for each.
3. **Dependency rule holds**: apps depend only on `core` (+ parsing libs); engines only on `core`;
   the server wires. Never make an app depend on another app or an engine crate.
4. **Extract and test**: every new predicate/transform is a named pure function with an
   `x_not_y`-style test that fails before and passes after. Contract/inventory tests that pin the
   old expression are strengthened in the same commit, never loosened.
5. **Honest absence over fabricated values**: Null/`unknown` when a fact is missing; a cap or
   truncation is stated in the result (`*_truncated`, `complete: false`).
6. **Default OFF for anything with a policy or irreversible gate**: new config sections ship
   disabled/`open` so `master` behaves byte-for-byte as today until an operator flips the key.
7. **Migrations**: additive only, next free number in `crates/core/migrations/` (check the
   directory in YOUR worktree at start and again before commit — another builder may take the same
   number; if so, renumber yours and say so in the report).
8. **Docs same session**: every user/API-visible change updates the coupled `docs/features/*.md`
   (map: `scripts/docs/feature-doc-map.json`); a new feature area gets a new doc + map entry. Add
   new routes to the `EXPECTED` inventory in `crates/server/src/routes/mod.rs` and to
   `docs/features/http-api.md`'s route table. New apps: `crates/server/src/registry.rs` +
   `catalog/data-sources.toml` `[[source]]`.
9. **Gates, each in its own invocation, `&&`-chained, exit code asserted** — run from the
   worktree root: `cargo fmt --all` then `cargo check --workspace` then
   `cargo clippy --workspace --all-targets -- -D warnings` then `cargo test -p <every crate you
   touched>` then `cargo test --workspace` once at the end. Set `CARGO_BUILD_JOBS=3` (five builders
   share 12 cores). A gate you could not run is reported as **not passed**, never as passed.
10. **Commits**: small, atomic, on YOUR branch only, pathspec-staged (`git add <paths>` — never
    `-A`/`.`), message `feat(<context>): <what>` with a body naming the deck item (e.g. `N20`).
    Never touch `master`, never rebase, never `git stash`.
11. **Reply contract** (your final message): item id; what shipped (routes/params/tables/config
    keys); commits (sha + subject); gates run with pass/fail and the exact failing output if any;
    what you could NOT verify; what you deliberately left out of the v1 slice; any file outside
    your scope you needed and did not touch; migration numbers used.

## Shared surfaces

| File | Rule |
| --- | --- |
| `crates/server/src/routes/mod.rs` | Only: one `mod` line, one `.merge()`/nest line, your routes appended to `EXPECTED`. No reordering. |
| `crates/server/src/registry.rs` | Only: register a new app in the list; N09 owns the `DynamicApp` adapter and may change how the list is built. |
| `crates/core/src/config.rs` | Only: add your own `[section]` struct + default at the END of the file's section list; no edits to existing sections. |
| `crates/core/src/error.rs` | Only N02 (`AwaitingInput`) and N12 (`Repair*`) add variants; append at the end of the enum and of the HTTP status map (`routes/error.rs`). Adding a core `Error` variant is NOT purely additive — check the inventory test in `routes/error.rs`. |
| `crates/core/migrations/` | See rule 7. |
| `docs/features/http-api.md` | Append rows to the route table only. |
| `catalog/data-sources.toml` | Append `[[source]]` blocks only. |

## File-scope partition (HARD boundaries)

| Builder | Item | Owns (may edit) | Must not touch |
| --- | --- | --- | --- |
| A | **N20** Identity & tenancy | `crates/server/src/routes/principals.rs` (new), `crates/server/src/auth.rs` (new), `crates/server/src/state.rs`, `crates/server/src/main.rs` (wiring only), `crates/core/src/costs.rs`, `crates/core/src/storage.rs` (principal columns/queries only), migration, `docs/features/auth.md` (new), `docs/deployment.md` auth section, `scripts/docs/feature-doc-map.json` | `worker.rs`, `mcp/`, any app crate, `engine-*` |
| B | **N02** Jobs that wait | `crates/core/src/job.rs`, `crates/core/src/storage.rs` (job status/resume queries only), `crates/core/src/app.rs` (`await_input`/`restore_input`), `crates/core/src/error.rs` (one variant), `crates/server/src/worker.rs`, `crates/server/src/progress.rs`, `crates/server/src/scheduler.rs` (`run_holds_slot` only), `crates/server/src/routes/jobs.rs`, `crates/server/src/mcp/mod.rs` (`resume_job`, `wait_job` shape), `clients/typescript` types for the new status, `docs/features/runtime.md`, `docs/features/mcp.md` | `apps/transact` (N01 ports it in wave 2), `engine-*`, `routes/` other than jobs |
| C | **N09** WASM apps v2 | `crates/engine-wasm/**`, `crates/core/src/plugin.rs`, `crates/core/src/app.rs` (only a `DynamicAppContext`/host-facing facade if needed — additive), `crates/server/src/registry.rs`, `crates/server/src/routes/apps.rs` or wherever `GET /apps` lives (runnable flag), `crates/core/src/catalog.rs` (`engine = "wasm"`, `module_sha256`), `plugins-src/wasm-app-template/` (new), `justfile` (one recipe), `docs/features/apps.md` §dynamic, `docs/features/plugins.md` or equivalent | `worker.rs`, `resilience/`, `engine-browser`, `engine-http` |
| D | **N12** Self-healing extraction | `crates/core/src/resilience/**`, `crates/core/src/induce.rs`, `crates/core/src/extract.rs` (additive helpers only), `crates/core/src/datasets.rs` (profile registry queries only), `crates/apps/repair/` (new crate), `crates/server/src/bin/resilience-eval.rs` (new), `crates/server/src/routes/sources.rs` (or wherever `/sources` lives) for `reextract` + profile routes, migration(s), `docs/features/resilient-extraction.md`, `IMPLEMENTATION-NOTES.md` markers, `catalog/data-sources.toml` | `worker.rs`, `engine-*`, `apps/extractor` beyond reading |
| E | **N14** API X-ray loop | `crates/core/src/fetcher.rs`, `crates/core/src/tiers.rs`, `crates/core/src/recipes.rs` (or wherever recipes live in core), `crates/engine-browser/src/**` (capture on escalation only), `crates/apps/extractor/src/lib.rs` (discovery call site only), `crates/server/src/routes/recipes.rs`, `docs/features/fetching.md`, `docs/features/extraction.md` | `worker.rs`, `resilience/`, `engine-wasm`, `engine-claude` |

## Item specs (v1 slices — do NOT exceed)

### A — N20 Identity & tenancy plane (XL, policy) — group report: `http-api.md` HA1

**v1 slice.** `[auth] mode = "open" | "keys"` (default `open` = today's behaviour, byte for byte).
`principals` table (id, name, key_hash, scopes JSON, budget_usd_per_day, rate_limit_per_min,
enabled, created_at) + `audit_log` (principal_id, action, target, at, detail) + nullable
`jobs.principal_id` and `cost_events.principal_id`. `POST /principals` (key shown once, like
`create_ingress_source`), `GET /principals`, `POST /principals/{id}/disable|rotate`. One tower
layer in `with_middleware` resolving `Authorization: Bearer` / `x-pumper-key` by SHA-256 digest
compare, enforcing scope (`read` / `enqueue:<app|*>` / `admin`), consuming the principal's
token bucket (reuse `bucket_step` from `routes/ingress.rs`), stamping the principal into a request
extension the enqueue door reads. In `open` mode every request resolves a synthetic `operator`
principal with all scopes and no ceiling. Per-principal daily ceiling checked beside
`validate_budget_usd`. `GET /costs?principal=` and a `by_principal` block on `/economics`. Audit
every mutating verb through one `audit(action, target)` helper. `GET /audit?principal=&cursor=`.
`/health`, `/metrics`, `/openapi.json` stay unauthenticated in both modes.
**Out of v1:** MCP per-session keys (wave 2 items add MCP tools; leave `allow_enqueue` as is),
key expiry, org/tenant grouping, SDK changes.
**Gate to prove:** smoke-level test: `mode = keys`, a scoped key refused on an out-of-scope
route (403 with the error-code map), the operator path unchanged in `open`.

### B — N02 Jobs that wait (XL, contract) — group report: `job-orchestration.md` JO1

**v1 slice.** `JobStatus::Waiting`; columns `input_request JSON`, `waiting_since`,
`waiting_expires_at`, `resumed_input JSON`. `AppContext::await_input(request) -> Error::AwaitingInput`
(forces a checkpoint first, `checkpoint_now`); the worker's outcome arm sets `waiting` (not
failed), releases the permit, publishes a non-terminal `waiting` job event so `/jobs/{id}/stream`
stays open. `POST /jobs/{id}/resume {input}` → 409 unless `waiting`; stores input, re-queues via
`Storage::reset` semantics (attempt headroom, not a burned attempt); `ctx.restore_input()` returns
`Option<Value>`. `run_holds_slot` treats `waiting` as holding the slot. Reaper untouched (selects
`running` only); a separate expiry sweep on the reaper tick fails a waiting job past
`waiting_expires_at` through `finalize` with a distinguishable error so callbacks/triggers fire.
MCP: `wait_job` returns `{status: "waiting", input_request}`; add `resume_job`; `list_jobs?status=waiting`.
`pumper_jobs{status="waiting"}` gauge. SDK types learn the status.
**Out of v1:** porting transact (N01, wave 2) or research to use it — ship one **test app** in
`crates/core/src/testing.rs` or the server test seam that awaits and resumes, and an e2e
`park → resume → succeed with attempts unchanged`.
**Gate to prove:** that e2e, plus the `(status, attempts)` fence holding on a stale resume.

### C — N09 WASM apps v2 (XL, contract) — group reports: `core-platform.md` CP5 + `scraping-engines.md` SE3

**v1 slice.** A `pumper:app` WIT world in `crates/engine-wasm/wit/`: imports `fetch`, `upsert-many`,
`checkpoint`, `restore`, `save-artifact`, `progress`, `log`; exports `describe`, `run`. Component-model
host in `engine-wasm` (wasmtime component API, async host calls), per-job fuel budget re-armed per host
call, cancellation + job deadline checked at every import, `max_concurrent` admission shared with
plugins. `DynamicApp: ScrapeApp` adapter in `registry.rs`; manifests validated at load (examples
against schema, like the Rust apps' test); `GET /apps` shows `runnable: true`. Catalog:
`engine = "wasm"` + `module_sha256` on a `[[source]]`; loader refuses a hash mismatch. A template
crate `plugins-src/wasm-app-template/` that builds to a runnable app (a hackernews-shaped example)
and an `#[ignore]` e2e that runs it through the worker under VCR replay (`ReplayFidelity::Full`).
`just plugin-app <crate>` recipe. Provenance stamps the module hash as `rules_hash`.
**Out of v1:** `research` and `sync-many` imports, hot-reload of a running module, the
provisioner → module path, `observe-extraction`.
**Gate to prove:** the e2e (ignored, documented), plus a unit test that a module importing an
undeclared host function fails to link with a typed `PluginFailure`.

### D — N12 Self-healing extraction (XL, policy) — group report: `core-platform.md` CP1

**v1 slice, in this order, each its own commit:** (1) **profile registry** — `extraction_profiles`
+ immutable `profile_versions`; `profile: <name>` accepted wherever `rules` is (extractor reads it
through a core helper), `source_runs.profile_version` stamped; inline rules keep working and report
`repairable: false`. (2) **`resilience-eval` bin + fixture corpus** — mutation taxonomy over retained
bodies (rename classes, drop elements, rebind selectors), reporting recall / false-positive rate per
diagnosis; this number gates everything below. (3) **Tier-0 inversion** as a pure function in
`induce.rs`: `invert(old_values, new_docs) -> Vec<RuleSet>`, cross-document intersection, brittle-
selector lint. (4) **Seven validation gates** as extracted, tested predicates, persisted verdicts in
`repair_candidates`. (5) **`repair` app** (`crates/apps/repair`): Tier-0 only in v1, idempotency
key `repair:{source}:{diagnosis_hash}`, shadow mode (candidate extracts alongside live rules on the
same batch), promotion only after `[resilience.repair] probation_runs` clean runs, auto-rollback,
`max_promotions_30d`. `[resilience.repair] enabled = false` default. `source.repair_promoted` /
`source.rolled_back` webhooks via `dispatch_event`.
**Out of v1:** Tier-1 Claude candidates (design the seam: `ResearchRequest.json_schema = RuleSet`,
leave a TODO test), golden documents, `POST /sources/{id}/reextract` (ship only if (1)–(5) land
with green gates).
**Gate to prove:** the eval bin's fail-before on a deliberately broken fixture; the promotion
state machine's `stale_candidate_not_promoted` / `tripped_probation_rolls_back` tests.

### E — N14 API X-ray closes its own loop (L, contract) — group report: `scraping-engines.md` SE1

**v1 slice.** (1) Capture on escalation: when the tiered fetcher escalates a host to the browser
tier, set `capture_network` for that render (bounded by existing caps); (2) discovery at the
extractor's fetch call site: run the shipped discovery heuristic over captured requests and
`AppContext::xray` the result (today zero callers); (3) auto-validate: a discovered recipe is
replayed once through the HTTP tier and stored `validated: true|false` with the reason;
(4) router learns an `api_recipe` tier: `tiers.rs` prefers a validated recipe for that host before
`http`, with the same win/strike memory the browser pin has; (5) `GET /recipes` shows validation
state; `GET /hosts` shows the learned tier. `[fetcher] xray = false` default; everything above is
inert until flipped.
**Out of v1:** recipe sharing between nodes (N16), recipe editing routes, recipes for POST APIs.
**Gate to prove:** a fetcher test with a scripted browser that captures a JSON call, discovers a
recipe, validates it, and serves the second fetch from the `api_recipe` tier — and the negative:
an invalid recipe is stored `validated: false` and never preferred.

## Merge order (coordinator)

E (N14, smallest, isolated) → B (N02) → A (N20) → C (N09) → D (N12). After each merge:
`cargo check --workspace`; after all five: `just ci` on `master`. Conflicts in the shared
surfaces are resolved by the coordinator, never by re-running a builder.
