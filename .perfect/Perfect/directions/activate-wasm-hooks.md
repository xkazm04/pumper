---
slug: activate-wasm-hooks
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 8adfc91
---

## What & why
The trigger plugin-hook feature is complete in code, tested against a stub, documented in the
ABI — and inert in every real deployment: `plugins-src/trigger-gate` (predicate) and
`plugins-src/delta-slim` (transform) are fully written but never built into `data/plugins/`,
so every configured hook takes the unknown-plugin fail-open path — predicates silently pass,
transforms silently no-op, with only a warn. Ship the build/deploy step, an e2e suite through
the REAL `WasmPluginHost` (all current hook tests use `StubPlugins`), make the unknown-plugin
path loud, and write the missing `docs/features/` entry for the whole feature.

## Evidence
- `plugins-src/{trigger-gate,delta-slim}` exist, ABI-correct; `data/plugins/` holds only
  busyloop.wasm + title.wasm
- `engine-wasm/src/lib.rs:108` → `triggers.rs:240` — unknown-plugin fail-open path
- `triggers.rs:881-1039` — hook tests are all StubPlugins; zero real-host coverage
- No justfile step builds trigger plugins into data/plugins; M15 feature absent from
  docs/features/

## Acceptance criteria
- `just` target builds both trigger plugins to wasm32 and installs them into `data/plugins/`
  (documented; CI-friendly).
- e2e tests through the real WasmPluginHost: predicate veto, transform reshape, provenance
  re-stamp, and ≥2 failure modes (fuel exhaustion / trap / bad output) — currently untested
  against the real host.
- Unknown-plugin on a CONFIGURED hook is loud (error-level + ledger/metric), not a quiet
  pass-through; fail-open behavior itself stays (availability) unless config says otherwise.
- New `docs/features/` entry covering hooks: ABI, config, failure semantics, build step.

## Risks / non-goals
- wasm32 target availability in CI/dev — the just target should fail with a clear message if
  the toolchain target is missing (`rustup target add` hint).
- Non-goal: new plugin capabilities; this is activation + verification of what exists.

## Build record
- Builder T2 (opus), wave 2 → master `8adfc91` (final gate in flight at write time).
  `just plugins-install` builds + installs both plugins (hyphenated file stems — cargo's
  underscored artifact name would be unaddressable by hooks; rustup hint on missing target).
  RAN for real; the two #[ignore]d shipped-plugin tests verified green after install.
  `plugin_missing` ledger outcome + error log via extracted `missing_hook_plugins`;
  `Plugins::has` added as defaulted trait method in core/plugin.rs (scope deviation, flagged
  not asked — additive, avoids per-event list() allocation; Director endorses). 6 real-host
  tests on inline wat fixtures run unconditionally (veto, reshape, provenance re-stamp vs a
  forging module, fuel/trap/malformed-output fail-open, on_error:skip fail-closed).
  New docs/features/trigger-plugins.md + doc-map entry.
- Honest: missing-target branch of plugins-install unexercised (wasm32 installed here).
- Gates: worktree full workspace 1072/0.
