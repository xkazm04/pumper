# App Registry — refactor + bug-hunt findings

> Total: 3 findings (Critical: 0, High: 0, Medium: 2, Low: 1)
> Files scanned: `crates/server/src/registry.rs` (in full) + cross-checks: root `Cargo.toml`, `crates/server/Cargo.toml`, `crates/core/src/app.rs` (ScrapeApp trait), `crates/server/src/state.rs` (registry build), `crates/server/src/routes.rs`, `crates/server/src/worker.rs`, `crates/server/src/scheduler.rs`, `crates/server/src/main.rs`, and the `name()` id of all 20 registered app crates.

**Baseline (clean):** All 20 `app-*` crates present under `crates/apps/*` are wired in all three places (root `[workspace.dependencies]`, `crates/server/Cargo.toml`, and `registry.rs`), and every app's `name()` id is unique and matches its crate slug (`hackernews`→`"hackernews"`, `mpsv-vpm`→`"mpsv-vpm"`, etc.). No id collision, no misspelling, no missing/unreachable app *today*. The two non-`app-` crates (`grants-common`, `trades-common`) are shared libraries, correctly not registered. Every lookup-miss site is graceful — `routes.rs:537` returns 404, `worker.rs:149` fails the job permanently with a warn, `scheduler.rs:92/201` guard with `contains_key`/`unwrap_or` — so there is **no** panic-on-unknown-app bug. Findings below are latent/structural weaknesses in how this hand-maintained list is consumed.

## 1. Duplicate app id silently overwrites and permanently disables an app
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: id-collision / silent-failure
- **File**: `crates/server/src/registry.rs:9-32` (consumed at `crates/server/src/state.rs:144-147`)
- **Scenario**: The `Vec` built in `apps()` is folded into a `HashMap<String, Arc<dyn ScrapeApp>>` at `state.rs:144-147` via `.map(|app| (app.name().to_string(), app)).collect()`. `HashMap` collect keeps the **last** entry for a repeated key and silently drops the earlier one. If any two registered apps ever return the same `name()`, the earlier one vanishes: its route `POST /apps/<name>/jobs` (`routes.rs:537`) resolves to the survivor, and its cron schedule is never registered because scheduler wiring iterates `state.registry.values()` (`main.rs:36`) — the dropped app isn't in the map. Startup even logs `"registered scraping apps"` as if all is well (`state.rs:148-151`). The realistic trigger: the registry doc-comment says "add one line here," and there are copy-paste-prone id families — `mpsv-vpm`/`mpsv-ispv`, `census-density`/`census-nonemp`, `grants-gov`/`ca-grants`/`eu-sedia`. A new sibling crate copied from one of these that forgets to change the `name()` string collides with zero compile-time or startup error.
- **Root cause**: The registry list is deduplicated by *last-write-wins* HashMap semantics with no uniqueness assertion anywhere between the `Vec` in `registry.rs` and the `HashMap` in `state.rs`.
- **Impact**: A silently unreachable scraping app — dead API endpoint and missed scheduled scrapes — indistinguishable from success in logs; can ship to production undetected.
- **Fix sketch**: Assert uniqueness when constructing the map — e.g., in `apps()` or at the `state.rs` collect, detect a duplicate `name()` and hard-error (panic at startup) or at minimum `tracing::error!` the collision. A `debug_assert!` over the collected key count vs. `Vec` len catches it in tests/CI.

## 2. Declared-but-unregistered app compiles clean yet is invisible at runtime
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: unreachable-app / silent-failure
- **File**: `crates/server/src/registry.rs:9-32`
- **Scenario**: Integration requires three edits (root `Cargo.toml` dep, `crates/server/Cargo.toml` dep, one line in `registry.rs`). If the first two are done but the `registry.rs` line is forgotten, everything still compiles and links — an unused workspace dependency is not a Rust error. The app is simply absent from the registry `HashMap`, so every `POST /apps/<name>/jobs` returns 404 and the app never schedules. There is no test or startup check asserting that each `app-*` crate declared in `crates/server/Cargo.toml` appears in `apps()`. (This is the inverse of Finding 1: F1 = duplicate silently drops an app; F2 = omission silently never adds one.)
- **Root cause**: The three parallel hand-maintained lists have no cross-consistency check; the compiler cannot flag a workspace dep that is declared but never `use`d/registered.
- **Impact**: A finished, dependency-linked scraper is silently unreachable — wasted build weight plus a feature that "exists" in the tree but never runs.
- **Fix sketch**: Add a small test (in `crates/server/tests`) that parses `crates/server/Cargo.toml` for `app-*` deps and asserts each corresponds to a `name()` in `apps()`, or generate the registration list from a single source (build script / declarative macro) so the three lists cannot drift.

## 3. Registration order in registry.rs differs from both Cargo.toml orderings
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: boilerplate / consistency
- **File**: `crates/server/src/registry.rs:11-31`
- **Scenario**: The `apps()` vec order (`hackernews`, `research`, `connector-api-watch`, `readable`, `watch`, …) matches neither root `Cargo.toml` (`connector-api-watch` is listed first there) nor `crates/server/Cargo.toml` (`connector-api-watch` first, then `hackernews`, …). Because integration is a manual three-place edit, keeping the three lists in three different orders makes a "did I add it everywhere?" eyeball-diff harder — exactly the review step that would otherwise catch Finding 1 / Finding 2 before merge.
- **Root cause**: No canonical ordering convention across the three hand-maintained lists.
- **Impact**: Slower, more error-prone manual sync when adding/removing an app; higher chance of an unnoticed omission. Pure maintainability, no runtime effect.
- **Fix sketch**: Alphabetize all three lists identically (root `Cargo.toml` deps, server `Cargo.toml` deps, `registry.rs` vec) so a missing entry stands out on inspection. Keeps the intentional one-line-per-app pattern; only imposes a stable order.
