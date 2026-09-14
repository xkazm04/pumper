# KPI baselines — the numbers the goals are judged against

This is the back-measure ledger. A goal whose only evidence is a commit message is
not measured; every row here is a number a command produced, with the command
written down next to it, so the next cycle can re-run it and diff rather than
re-describe.

The meters themselves live in the Personas project record (`dev_kpis` /
`dev_kpi_measurements`, project `512809db`); this file is the in-repo copy so a
clone can re-measure without the app. Keep them in sync — a reading recorded in
only one of the two is a reading nobody will compare against.

Rule: **a cannot-run is not a pass.** Rungs that could not be judged are listed as
such, not folded into the green column.

---

## Baseline — 2026-09-14, master `2d3b9d8`

Taken on win32, in a worktree with a linked `target/`. `audit` needs network and
`sdk` needs `clients/typescript/node_modules`, neither of which this environment
had; both are recorded as *not judged*, not as green and not as red.

### Goal `4ae7bafe` — keep every verify gate green on master

| meter | reading | target | source |
| --- | --- | --- | --- |
| Verify-gate rungs red | **3** of 9 judged | 0 | run each `just ci` rung, count non-zero exits |
| Undeclared/unregistered `#[ignore]`s | **3** | 0 | `node scripts/ci/flake-check.mjs`, count `[undeclared]` + `[unregistered-flake]` |

Rung by rung:

| rung | exit | reading |
| --- | --- | --- |
| `fmt-check` | 0 | clean |
| `lint` | 0 | `cargo clippy --workspace --all-targets` — 0 warnings, 50 s warm |
| `test` | 0 | `cargo test --workspace` — 2663 passed, 0 failed, 22 ignored, 159 suites |
| `plugins-verify` | 0 | clean |
| `inventory` | 0 | incl. `pin-check` (0 pinned, 33 registered, ceiling 33) and `protection-check` (7 required checks match) |
| `disk-check` | 0 | `target/` 6.99 GB of 40 GB, 3.86 GB stale |
| `flake-check` | **2** | 3 findings — see below |
| `harness-test` | **1** | 39/40; `flake-check.test.mjs:535 this_repos_own_register_reconciles_with_this_repos_own_tree`, `2 !== 0` — same root cause as the rung above |
| `supply-chain` | **1** | 7/8; `sbom.test.mjs:64 the_lockfile_parser_reads_packages_not_patch_tables`, `null !== 4` |
| `audit` | — | not judged (needs network) |
| `sdk` | — | not judged (needs `clients/typescript/node_modules`) |

Not a `just ci` rung, but a CI job and the `commit-msg` hook, and red:

- `just commit-lint` → exit 2, BLOCKED on master's own tip `2d3b9d8 ai: …` — `ai`
  is not in the type list, and the subject is 88 chars against a 72 advisory.
  Scope matters here and is easy to get wrong: bare `just commit-lint` judges
  **HEAD's subject only**, while the CI job passes `--range`, so master's history
  is not re-judged on every PR. The finding is therefore not "CI is red"; it is
  that a commit typed `ai:` reached master at all, which means the `commit-msg`
  hook was bypassed, and that every future autopilot commit under that type will
  be rejected by the hook and by CI.

And one gate that cannot report a cannot-run in its own convention:

- `just clients-check` → exit **1** with an unhandled `Error: openapi-typescript
  is not installed` out of `scripts/gen/generate-clients.mjs:63`. The justfile
  documents exit 1 as *drift*; here it means *could not check*. Those are the two
  results the 0/2/3 convention exists to keep apart.

The three flagged `#[ignore]`s:

- `crates/engine-wasm/tests/plugins.rs:189` `the_reference_enricher_answers_the_entity_contract` — undeclared
- `crates/engine-wasm/tests/plugins.rs:336` `the_reference_connector_loads_with_its_declared_capabilities` — undeclared
- `crates/server/src/routes/meta.rs:98` `#[ignore = "timing probe, not a gate"]` — reads as a flake, no register row

Register state at baseline: 2/4 quarantined, 17 environment-gated, 22 `#[ignore]`s
in tree, oldest entry 21 days old, **no run history under `.flake/history/runs`**
— so no test can be labelled yet, which is not the same as "no flakes".

### Goal `a3d5d45e` — retire the highest-cost scrape failures

| meter | reading | target | source |
| --- | --- | --- | --- |
| Scrape-engine guard tests | **260** | 290 | `cargo test -p <engine> --all-targets -- --list`, summed over the seven engine crates |

Per engine — tests (ignored) / `src` LOC:

| crate | tests | `#[ignore]` | src LOC |
| --- | --- | --- | --- |
| `pumper-engine-http` | 41 | 0 | 1690 |
| `pumper-engine-browser` | 35 | 4 | 2155 |
| `pumper-engine-claude` | 51 | 0 | 1527 |
| `pumper-engine-archive` | 30 | 2 | 1384 |
| `pumper-engine-remote` | 28 | 0 | 1451 |
| `pumper-engine-search` | 43 | 0 | 2242 |
| `pumper-engine-wasm` | 32 | 5 | 3294 |
| **total** | **260** | **11** | 13743 |

11 of the 260 are `#[ignore]`d, so **249 run on a default `cargo test --workspace`**.

Long lanes (`just lane-certify`): `browser-render` and `archive-wayback` are
CANNOT-RUN on every runner by their own declared reason — they need a live third
party, and "a red from example.com is not a finding about pumper". The two lanes
that would certify the browser engine's render path and the archive engine's
wayback path have therefore never certified anything, anywhere. The four
`datasets-*` / `derived-*` / `fingerprint-*` lanes read CANNOT-SEE here only
because `just lanes` had not been run in this worktree; that is an artifact of the
reading, not a finding about master.

### What was deliberately *not* metered

A blanket "panic sites in engine code" count was measured (29 across the seven
engines) and rejected as a meter: nearly all of them are const-regex compiles,
`Mutex` poison `expect`s, and `expect("admitted above")` after an explicit
admission check. A target over that number would buy churn on justified code, not
fewer scrape failures. Recorded here so the next cycle does not re-derive it.

Engine `#[ignore]` counts are likewise not a meter on their own — most are genuine
environment gates, and the actionable subset (the undeclared ones) is already
counted by the flake meter above.
