# ship-loop overlay - pumper

Project specifics for the `/ship-loop` lane skill (`ai-registry/skills/ship-loop` v2.1, linked
at `.claude/skills/ship-loop`). The lane body is the method; everything below is what pumper
is. Extracted 2026-08-24 from the project-owned copy the lane replaced.

The pumper-adapted reference material that copy carried lives beside this file in
`.claude/ship-loop/references/` and is **tracked**:

| File | What it is |
|---|---|
| `scorecard.md` | the 10 pumper dimensions - what green means, what evidence, the audit prompt per dimension, and the Ship-gate pre-flight checklist |
| `stack-pumper.md` | the stack profile (hints, not facts - believe the audit and update the file when they disagree) |
| `templates.md` | the `.claude/ship-loop/` state-file templates (`state.md`, `backlog.md`, `journal.md`, `decisions.md`, `SHIP_REPORT.md`) |
| `dataset-and-runtime-acceptance.md` | dimension 5 + the runtime-acceptance harness (the pumper replacement for the SaaS original's billing proof + Playwright UAT) |
| `value-validation.md` | dimension 9 - source & cost value, the three `value-case.md` artifacts |
| `platform-standards.md` | dimension 10 - observability, docs sync, catalog & context-map parity |

The loop's mutable state (`state.md`, `backlog.md`, `journal.md`, `decisions.md`,
`value-case.md`) sits in the same directory and is **gitignored** - never force-add it.

## Stack

Rust workspace at the repo root (`Cargo.toml` with `[workspace]`, crates under `crates/`) -
axum job server + durable SQLite job queue + tiered fetch engines (`http` -> `browser` ->
`claude`) + `ScrapeApp` app crates + a TypeScript SDK in `clients/typescript`. Not a Node app.
Full profile: `references/stack-pumper.md`.

**What "ship" means here.** pumper is a **local-first service**, not a SaaS: one binary, no
auth, no billing, no UI, consumed over HTTP by other apps and agents on this machine
(`ONBOARDING.md` section 4). The loop proves *the service does what it claims, keeps its data
honest, and costs what it should* - not *someone will pay for it*.

## Cadence

`milestone` (default; asked at CP0). The copy's own vocabulary: Milestone / Marathon
(check in only when blocked or every 4th milestone) / Tight (every 2-3 items).

## Ship bar (default answer at CP0)

**Unattended scheduled operation** - apps run on cron, unwatched. Alternatives offered at CP0:
on-demand service (other agents call it, a human is around) / local demo (correctness on the
golden path only). A fourth CP0 question is pumper-specific: **runtime-acceptance depth** -
critical apps only (`status = "live"` in `catalog/data-sources.toml`) vs all registered apps +
failure cases (recommended for unattended operation).

## Gates (ordered - run top to bottom, sequentially)

| step | command | ratchet | when / notes |
|---|---|---|---|
| fmt | `just fmt-check` | exits 0 | = `cargo fmt --check` |
| lint | `just lint` | 0 errors; `#[allow]`s carry a reason | = `cargo clippy --workspace --all-targets` |
| test | `just test` | 0 failed | = `cargo test --workspace` |
| build | `just build` | exits 0 | = `cargo build -p pumper-server`; add `--release` when the bar is unattended operation |
| ignored tests | `just test-ignored` | compiles; failures understood | Milestone cadence or better; env-dependent (real Chrome, wasm artifacts, wall-clock) so CI cannot see a compile break here |
| runtime acceptance | real jobs through a running server | per `references/dataset-and-runtime-acceptance.md` | full at Milestone/Marathon; touched apps + smoke at Tight |
| dataset value | dataset value assertions | per `references/dataset-and-runtime-acceptance.md` | |
| value-case freshness | `value-case.md` exists, research <= 30 days, no unaddressed weak verdict | per `references/value-validation.md` | |

Steps 1-3 are exactly `just ci`. The repo-root `justfile` is the canonical task runner
(`cargo install just`); raw-cargo equivalents are in `CLAUDE.md`'s table and are what
`.github/workflows/ci.yml` invokes. Run everything **from the repo root** - the `.env` loader
and the default `config.toml` path are both CWD-relative.

**`just run` = `cargo run -p pumper-server --bin pumper`.** The `--bin` is required:
`pumper-server` ships three binaries (`pumper`, `reindex`, `search-backfill`) with no
`default-run`.

**Capture exit codes so they cannot lie.** `cmd | tail` makes `$?` the tail's exit code -
redirect to a file and echo `$?` immediately instead. A "clippy passed" built on a piped exit
code is fabricated evidence.

**`just check` is not evidence that a scrape works.** `ONBOARDING.md` section 8 is explicit: a
change to an app or engine is not done until a real job has run through it.

## Dimensions

Ten, not nine - definitions in `references/scorecard.md`. Dimensions 5, 7, 9 and 10 are the
pumper replacements for the SaaS originals:

| # | name | what it means here |
|---|---|---|
| 5 | Dataset value | `references/dataset-and-runtime-acceptance.md` - every job that costs time, bandwidth or model spend returns records a consumer can use |
| 7 | API & client contract | route surface <-> OpenAPI <-> `clients/typescript` SDK <-> `docs/features/*` parity |
| 9 | Source & cost value | `references/value-validation.md` - the sources are still the right ones and the spend is defensible against web-researched alternatives |
| 10 | Platform standards | `references/platform-standards.md` - observability, docs sync, catalog & context-map parity |

## Conventions

- **Dependency rule:** `apps -> core <- engines`; only `server` depends on everything
  (`ONBOARDING.md` sections 3/7/9). The loop gets no exemption because it is in a hurry.
- **Bug fixes ship as extracted, tested functions** - a named function plus a test named after
  the anti-pattern it defends (`x_not_y`), never an inline patch in a `run()` body. A
  convention is enforced with an inventory test (the EXPECTED-diff idiom in
  `crates/server/src/routes/mod.rs`), never with a sentence in a doc.
- **Same-session doc sync:** any user/API-visible change (endpoint, param, dataset shape, app,
  trigger contract, config key, CLI-observable behavior) updates the coupled
  `docs/features/*.md` **in the same commit**; `scripts/docs/feature-doc-map.json` is the map
  and a Stop hook checks. Files moved between contexts update `context-map.json` too.
- **Scope with `context-map.json`** - find the context that owns the files and stay inside it
  unless the item says otherwise.
- **Read at boot, before the audit:** `CLAUDE.md` and `MEMORY.md` at the repo root first
  (`CLAUDE.md` is the shortest true summary; `MEMORY.md` indexes the durable state under
  `.perfect/`), then `.perfect/Architect/backlog.md` **Pending** so the loop does not duplicate
  structural work `/architect` has queued, then `Cargo.toml`, `justfile`, `README.md`,
  `ONBOARDING.md`, `docs/deployment.md`, `.claude/CLAUDE.md`, `context-map.json`,
  `.github/workflows/ci.yml`, and finally `references/stack-pumper.md`.
- **Git - shared checkout.** `/architect`, `/perfect`, `/explorer` and `/tiger` commit into
  this same tree concurrently. Commit prefix `ship:`; stage only explicit paths in one bash
  invocation (`git add <paths> && git diff --cached --stat`); never `git add -A`/`.`/`-u`;
  never `--amend`; never `git stash` / `reset --hard` / `clean -f`. On `index.lock`, wait 3-10s
  and retry up to 6 times; expect HEAD to advance mid-run.
- **State dir:** `.claude/ship-loop/` - the loop state files are gitignored; this overlay and
  `references/` are tracked.
- **Push policy:** never push unless the user asks; the local gate is primary.
- **Reuse installed skills as tools:** `/explorer` for a scoped quality sweep inside a
  milestone, `/architect` when a backlog item turns out to be structural, `/tiger` for anything
  touching the `claude` engine or its cost, `/simplify` during polish milestones.

## Lenses

Boot fans out one subagent per audit lens in `references/scorecard.md`: app/route inventory,
dataset + migration map, tests + tooling, resilience & safety posture, API <-> SDK <-> docs
contract, ops readiness, the **source & cost value** lens from `references/value-validation.md`
(give it `catalog/data-sources.toml` and demand cited sources), and the **platform-standards**
lens from `references/platform-standards.md`.

Scope: 22 contexts in `context-map.json`; ~24 app crates under `crates/apps/`.

## Product-call boundaries

The loop never green-lights its own value judgment. Checkpoint decisions the user owns:
dropping or re-tiering an app whose upstream source died, changing a dataset key, and every
dimension-9 verdict (`keep-as-is` / `re-tier` / `switch` / `cut` / `blocked`) whose honest
answer is `switch` or `cut`.

## History

- 2026-07-26: the pumper-adapted ship-loop copy was authored (10 dimensions, 6 reference files).
- 2026-08-24: retired in favour of the registry lane skill; specifics moved into this overlay.
