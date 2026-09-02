# Wave 1 — Foundations (2026-09-02)

Five builders, five worktrees, all five items merged into `master`. Every builder reported all
five cargo gates green in its worktree; the coordinator re-ran check/clippy/tests on `master` after
each merge (see §Verification).

## Merges (in landing order)

| Item | Branch | Merge | Builder commits | Notes |
| --- | --- | --- | --- | --- |
| N20 Identity & tenancy | `moonshot/n20-identity` | `a077820` | 49a167d, 514f2ab, 837ffa9 | migration 0041 |
| N14 API X-ray loop | `moonshot/n14-xray` | `a76a310` | 5571eaa, d971d48, ed22455 | migration renumbered 0041→**0042** |
| N02 Jobs that wait | `moonshot/n02-waiting` | `f580d34` + `5bfad5d` | 28a19c0 … d027fb5 (7) | migration renumbered 0041→**0043**; `config.rs` both sections kept; a scripted conflict resolution dropped three closing braces, fixed in `5bfad5d` |
| N09 WASM apps v2 | `moonshot/n09-wasm-apps` | `eeea975` | 22306fc … abcf430 (7) | no migrations; `config.rs` both sections kept |
| N12 Self-healing extraction | `moonshot/n12-self-healing` | (this doc's commit) | d897483 … 16babba (5) | migrations renumbered 0041/0042→**0044/0045** |

**Migration collisions were the one predictable conflict**: four of five builders took 0041.
Rule 7 ("re-check before commit") cannot work across worktrees — the coordinator renumbers at merge,
and references in docs/tests are grepped and fixed in the merge commit.

## What master can do now (all default OFF unless stated)

- `[auth] mode = "keys"`: principals with scopes, per-principal daily ceiling + token bucket,
  audit log of every mutating verb (audit is ON in `open` mode too, with a NULL principal).
- `JobStatus::Waiting` + `ctx.await_input()` / `POST /jobs/{id}/resume` / MCP `resume_job`;
  `[waiting] expiry_secs = 0` = wait forever.
- `[wasm_apps] enabled = true`: `pumper:app@0.1.0` components in `[plugins] app_dir` register as
  runnable apps with fuel/wall-clock/host-call bounds; catalog `engine = "wasm"` + `module_sha256`.
- `[resilience.repair] enabled = true`: profile registry, `resilience-eval` bin, Tier-0 inversion,
  seven gates, `repair` app in shadow mode. **Nothing promotes today** — gate 4 refuses without
  golden docs, by design.
- `[fetcher] xray = true`: capture on escalation → discovery at the extractor → auto-validate →
  router learns `api_recipe`. Also fixed a real pre-existing bug: unvalidated junk recipes were
  replayed ahead of the live ladder forever.

## Measured

`resilience-eval` (default config, cohorts 5/30/200): hard-break recall **1.000**, silent-corruption
recall **0.500** (carried entirely by duplicate-node; sibling-swap undetected at 0.300 vs 0.6),
false-positive rate **0.000**. First number this subsystem has ever had.

## Carry-forward seams (coordinator follow-ups, not built by any builder)

Each was a file outside the builder's row. Small, all in the server or a wave-2 builder's scope:

1. **`jobs.principal_id` stamping** — `routes/jobs.rs::enqueue_job` must read the
   `CallerPrincipal` extension and call `enqueue_dedup_as`. Until then `cost_events.principal_id`
   stays NULL and `/principals/costs` is all `(unattributed)`. → wave-2 builder G touches
   `EnqueueOptions`; assign to G.
2. **`GET /costs?principal=`** + `by_principal` block on `/economics` — core aggregation exists.
3. **`api_recipe` pin has no routing effect in `AppContext::fetch`** (app.rs branches only on
   `browser`). → small core fix, wave 2 or 3.
4. **Extractor `profile:` param door** — core helper exists; `apps/extractor` `MODE_ROOTS` wiring
   missing. → wave-3 (extractor is free then).
5. **`source.repair_promoted` / `source.rolled_back` webhooks** — named in the app result, not
   dispatched (`dispatch_event` is server-side). → with N05 durable event log (wave 3).
6. **`RunReport.profile_version`** is stamped by a separate UPDATE, not a struct field.
7. **Template never compiled** — `wasm-tools` is not installed on this box, so
   `plugins-src/wasm-app-template` is unverified by compilation. Install `wasm-tools` and run
   `just plugin-app wasm-app-template` once.
8. `docs/features/http-api.md` has no row for the new `GET /apps` fields (documented in apps.md).
9. `scripts/docs/feature-doc-map.json`: add `crates/core/src/recipes.rs` to the `fetching.md` globs.

## Not verified anywhere

- No builder booted the binary; `scripts/smoke.ps1` and `just ci` were not run in the worktrees
  (the five cargo gates were). The coordinator's `just ci` on `master` is recorded in §Verification.
- The doc-sync Stop hook reports exit 3 (cannot check) when run standalone; docs were updated by
  hand in the same commits.

## Verification on master

After the fifth merge (`91674ad`), on `master`, each in its own invocation:

| gate | result |
| --- | --- |
| `cargo fmt --all -- --check` | exit 0 |
| `cargo check --workspace` | exit 0 |
| `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| `cargo test --workspace` | exit 0, no FAILED/panicked |

Not run: `just ci` (audit, plugins-verify, sdk, inventory, flake-check, harness-test, disk-check) and `scripts/smoke.ps1`. Both are owed before the campaign closes.
