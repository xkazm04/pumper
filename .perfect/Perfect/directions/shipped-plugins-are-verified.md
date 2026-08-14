---
slug: shipped-plugins-are-verified
type: perfect/direction
context: "[[wasm-plugin-examples]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**Two production artifacts are never compiled by anything, and a break in them fails OPEN.**
`trigger-gate` and `delta-slim` are installed into `data/plugins/` by `just plugins-install`
(`justfile:160-166`) and gate/shape real trigger hops. Nothing checks they still compile or still
satisfy the host ABI: the four `plugins-src/*` crates are **workspace-detached** (`[workspace]` in
each `Cargo.toml`, root members are `crates/*` only, `Cargo.toml:3-14`), so
`cargo test --workspace` never touches them, and CI has no `wasm32-unknown-unknown` target and no
plugin build (`.github/workflows/ci.yml:25-38`).

A break does not go red — it goes **silently fail-open**: the plugin becomes an unknown plugin,
`predicate_fail_default` fires the hop anyway (`crates/server/src/triggers.rs:173-175, 394-398`),
predicates stop gating and transforms stop shaping. The tests that would catch it **already exist
and are good** (`crates/engine-wasm/tests/plugins.rs:36,64`;
`crates/server/src/e2e/trigger_plugins.rs:647,679`) — all four are `#[ignore]`d.

Worse, `just test-ignored` cannot pass on a clean machine: the title tests need
`data/plugins/**title**.wasm`, `just plugins-install` installs only the two hook plugins, and the
`title_extractor.wasm` → `title.wasm` rename exists solely as a README copy line
(`README.md:243`).

## Evidence

- `Cargo.toml:3-14` + `plugins-src/*/Cargo.toml` `[workspace]` — detached from the workspace.
- `.github/workflows/ci.yml:25-38` — fmt + clippy + `cargo test --workspace`; no wasm32 target,
  no `just plugins-install`, and `:36-37` explicitly parks the artifact tests as ignored.
- `crates/engine-wasm/tests/plugins.rs:14,28,36,64` — the ignored title tests and the artifact path
  they need.
- `crates/server/src/e2e/trigger_plugins.rs:647-711` — the shipped-plugin tests: they assert
  min_count/dataset gating and keep/max_keys shaping correctly, and never run unattended.
- `justfile:137-138, 160-166` — `just plugin <crate>` builds without copying; `plugins-install`
  covers only two of four.
- **Live state of this checkout:** `data/plugins/` holds only `busyloop.wasm` and `title.wasm` —
  neither hook plugin is installed, so this machine is *currently* on the fail-open path.

## Acceptance criteria (for whoever builds this)

1. CI compiles all four `plugins-src` crates for `wasm32-unknown-unknown`.
2. `just plugins-install` covers what the ignored tests actually need (incl. the `title.wasm`
   rename that lives only in the README today), so `just test-ignored` passes on a clean machine.
3. The four `#[ignore]`s come off for the targets CI can now build, or the ignore reason is
   narrowed to something still true.
4. A host-ABI break in either shipped hook plugin turns CI **red**, not fail-open.

## Risks / non-goals

- Adds a toolchain to CI (`rustup target add wasm32-unknown-unknown`) and a build step; measure the
  added CI minutes before committing to running it on every push rather than nightly.

## Why it was rejected in r22 (history — superseded by the r23 acceptance above)

Real, and the highest-payoff item in its context — **rejected only on the round's 6-direction cap**,
having lost the slot to three CONFIRMED live data-integrity defects (a partial parse that tombstones
live rows, an alerting app that fires false alerts, a paginator that caps a state corpus at one page
green). This is a **CI/toolchain** direction: its payoff is preventing a future silent regression
rather than closing one that is losing data today.

**Banked as the top r23 candidate for [[wasm-plugin-examples]], paired with
[[trigger-gate-honest-across-source-kinds]]** — which needs this harness to be provable, exactly the
D1/D2 relationship this round honoured for the checkpoint seam. Note the pairing precedent: r22
built a twice-deferred enabler *because* it unblocked a real fix. If r23 defers this pair again,
build it instead of a sweep context.

## r23 RE-VERIFICATION (Director, 2026-08-14) — CONFIRMED with two narrows

Re-scouted against HEAD `caf5e61`. Nothing in this area moved since banking (`ci.yml` last touched
`ecfd668`, both plugin sources `12401e0`). All structural facts hold: workspace-detached crates, no
wasm32 anywhere under `.github/`, `plugins-install` covers 2 of 4, `data/plugins/` holds only
`busyloop.wasm` + `title.wasm`, all four `#[ignore]`s verbatim as quoted.

**NARROW 1 — "never compiled by anything" is literally false.** `just plugin <crate>` and
`just plugins-install` do compile them. The defensible claim is: *nothing automated* — not CI, not
`cargo test --workspace`, no `build.rs`, no xtask, no script — only a manual `just` invocation.

**NARROW 2 — "silently" is wrong; it is loud, just never red.** Since `10fa27d` (2026-08-12) a
missing hook plugin emits `error!` on every evaluation (`triggers.rs:812-821`, naming
`just plugins-install`) and writes a `plugin_missing` ledger row asserted by a **non-ignored** e2e
test (`e2e/trigger_plugins.rs:288-349`). Note the row is recorded once per (trigger, plugin) then
suppressed (`is_static_hook_fact`, `:829-831`). Criterion 4 — *a break turns CI red* — remains
entirely unmet, which is the whole remaining point of this direction.

**New evidence the bank did not have:** `.gitignore:8` is `/data` and `git ls-files data/plugins`
returns nothing, so the artifacts are untracked **by design** — no clone ever starts with them.
Commit `8adfc91` ("activate the WASM trigger hooks — build, install, prove") added the recipe, the
tests and the docs but could not commit an artifact; that activation was local-only and has since
evaporated. This is not drift, it is a structural hole.

**Not broken, checked because it would have strengthened the claim:** `title-extractor/src/lib.rs:74`
returns `"version": "0.2.0"`, matching `engine-wasm/tests/plugins.rs:77`. No source/test drift.

**Scoping note for the builder:** adding `plugins-src/*` to the root workspace members is NOT the
cheap 80% — the crates are `cdylib`/wasm32 and each `Cargo.toml:21` carries a deliberate detachment
comment. CI is the right seam.
