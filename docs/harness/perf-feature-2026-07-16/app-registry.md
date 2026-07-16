# App Registry — perf-optimizer + feature-scout scan

> Total: 3
> Critical: 0 | High: 1 | Medium: 1 | Low: 1

## 1. Publish `default_params()` through `GET /apps` — the registry already knows them and never says

- **Severity**: High
- **Lens**: feature-scout
- **Category**: introspection / half-implemented feature
- **File**: crates/server/src/registry.rs:9-32 (contract), crates/core/src/app.rs:335-337, crates/server/src/routes.rs:522-536, crates/server/src/routes.rs:585
- **Scenario**: An agent/CLI client calls `GET /apps`, gets `{name, description, schedule}`, and wants to run `census-nonemp` with a different `naics`. It has no machine-readable way to learn which keys the app accepts or what the defaults are — `docs/features/apps.md:3` concedes "each description documents its params", i.e. free text only. Worse, it POSTs `{"params": {"naics": "23"}}` and silently loses every other default, because `enqueue_job` does `params: body.params.unwrap_or_else(|| app.default_params())` (routes.rs:585) — a **wholesale replace, not a merge**. The scheduler (scheduler.rs:203) meanwhile runs the same app with the full default set, so a manual run and its scheduled twin quietly execute different configurations.
- **Root cause**: `ScrapeApp::default_params()` is a first-class trait method that the registry hands to two internal consumers (scheduler, enqueue fallback) but that no external surface ever emits. The registry's self-description stops at three fields. The replace-not-merge semantics are only safe if the caller can see what it is replacing — and it cannot.
- **Impact**: Every one of the 20 registered apps is param-opaque over HTTP. Concretely: a one-key override on `census-nonemp`/`grants-gov` drops the rest of the defaults and the job runs mis-configured but "successfully" — a silent wrong-data path, not a visible failure. Fix is ~3 lines of `json!` plus a merge decision; it is the cheapest large usability win in this context.
- **Fix sketch**: Add `"default_params": app.default_params()` to the `json!` in `list_apps` (routes.rs:528-532) and to the response description at routes.rs:520. Then make the enqueue path honest about semantics: either shallow-merge `body.params` over `app.default_params()` in `enqueue_job`, or keep replace and document it in the `EnqueueBody::params` doc comment (routes.rs:540) — but only after the defaults are visible. This is the zero-cost subset of recorded idea #181 ("Per-app parameter JSON Schema for discovery", `3b92f6cf`) and does not block it: `default_params` shipped today, full JSON Schema later.

## 2. Let apps declare their preconditions so the registry can report readiness, not just existence

- **Severity**: Medium
- **Lens**: feature-scout
- **Category**: feature-gap / capability reporting
- **File**: crates/server/src/registry.rs:18-19 (census entries), crates/core/src/app.rs:320-341, crates/apps/census-common/src/lib.rs:24-35, crates/server/src/routes.rs:292-293
- **Scenario**: `app_census_density::CensusDensity` and `app_census_nonemp::CensusNonemp` are registered like every other app, but both hard-require a free Census API key resolved via `params.api_key` → env `CENSUS_API_KEY` (`census-common::api_key`), erroring at run time if absent. `census-density/src/lib.rs:66` even carries the comment "enable a yearly refresh once `CENSUS_API_KEY` is set" — a shipped app that is inert until an operator does something the API never tells them about. `GET /apps` lists it as available; `/metrics` emits `pumper_apps 20` (routes.rs:293) as if all 20 are runnable. The operator learns only from a failed job.
- **Root cause**: `ScrapeApp` has no way to express preconditions (required env/credentials, required engines like browser or Claude). Requirements live inside `run()`, so the registry can only report *registration*, never *readiness*. The registry is a catalog of what compiles in, not of what will actually work here.
- **Impact**: Bounded but real — 2 of 20 apps (10%) are credential-gated and indistinguishable from the other 18 over the API. Scales badly: every credentialed connector added repeats the trap. A scheduled census job would fail on every cron tick with no signal beyond job records.
- **Fix sketch**: Add a defaulted trait method to `ScrapeApp` (app.rs, alongside `schedule()`), e.g. `fn requires(&self) -> &'static [Requirement] { &[] }` with `Requirement::Env("CENSUS_API_KEY")` / `Requirement::Engine(...)`. Both census apps return their key requirement; `list_apps` emits `requires` plus a resolved `ready: bool`; `/metrics` splits `pumper_apps{ready="true"|"false"}`. Distinct from recorded idea #274 ("App self-test / dry-run endpoint", `498faad5`), which *executes* the app — this is a static declaration checked at boot, and it is the cheap precondition #274 would otherwise re-derive.

## 3. Registry hard-links all 20 app crates with no cargo features — an honest build-cost note, not a runtime one

- **Severity**: Low
- **Lens**: perf-optimizer
- **Category**: startup-cost / build-cost
- **File**: crates/server/src/registry.rs:9-32, crates/server/Cargo.toml:17-36, crates/server/src/state.rs:148-157
- **Scenario**: Any build or deployment of `pumper` compiles and links all 20 app crates (`crates/apps/*` minus the 3 shared libs), including their transitive dependency trees, whether or not the deployment ever runs them.
- **Root cause**: `apps()` is an unconditional `vec![]` and `server/Cargo.toml` declares all 20 app crates as plain `workspace = true` dependencies — there is no `[features]` table in the server crate at all, so nothing can be trimmed.
- **Impact**: **Runtime cost here is genuinely negligible and should not be optimized.** Verified: every app struct is a unit ZST (`pub struct HackerNews;`, `pub struct Crawl;`, …), so `apps()` is 20 `Arc` allocations of zero-sized values, called exactly once from `AppState::new` (state.rs:148); the `Vec`→`HashMap` build is 20 inserts. That is microseconds at boot, once — the "repeated construction / lookup cost" concern does not apply, since `state.registry` is an `Arc<HashMap>` doing O(1) lookups thereafter. The only real cost is compile time and binary size, paid by developers and images, not by requests.
- **Fix sketch**: Only worth doing if trimmed deployment images or CI compile time become a stated goal — otherwise the trade-off (a feature matrix to maintain, `#[cfg]` noise in `apps()`, feature-combination build breakage) is worse than the cost. If pursued: add `[features]` to `server/Cargo.toml` with a `default = ["apps-all"]` and per-app or per-domain groups (e.g. `apps-grants`, `apps-census`, `apps-cz`), mark the app deps `optional = true`, and gate each `Arc::new(...)` line in `apps()` with `#[cfg(feature = "…")]`. Note this interacts with finding #2: a feature-trimmed binary changes which apps `GET /apps` reports, which is exactly why readiness reporting should land first. Recorded idea #229 (runtime app marketplace, `54208fbb`) would supersede compile-time gating entirely — do not invest here ahead of that decision.
