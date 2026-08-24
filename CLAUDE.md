# CLAUDE.md

Pumper is a **local-first scraping service**: one Rust binary that exposes an HTTP
API, runs a durable job queue on SQLite (WAL), and scrapes through pluggable
engines. Cargo workspace, edition 2021, `resolver = "2"`.

## Commands

The repo-root [`justfile`](justfile) is the **canonical task runner** — prefer it,
and keep it in sync with anything you change here. Install once with
`cargo install just`, then `just --list`.

Everything runs **from the repo root**: the `.env` loader and the default
`config.toml` path are both CWD-relative.

| `just` | raw cargo |
| --- | --- |
| `just check` | `cargo check --workspace` |
| `just test` | `cargo test --workspace` — what CI runs |
| `just test-ignored` | `cargo test --workspace -- --ignored` — env-dependent (real Chrome, built wasm, timing) |
| `just test-recorded` | `just test` through the flake recorder, so the run lands in the retained history detection queries over |
| `just lint` | `cargo clippy --workspace --all-targets` — CI gate |
| `just fmt` / `just fmt-check` | `cargo fmt` / `cargo fmt --check` — CI gate |
| `just flake-check` | the quarantine register as a gate — expiry, ceiling, and both reconciliation directions; 0 clean / 2 findings / **3 cannot check** |
| `just flake-report` | register size **with its trend**, age of the oldest entry, and the labelled set with its full predicate |
| `just harness-test` | the fixture suites that prove `flake-check` and `lane-certify` can still go red |
| `just lanes` | run every long lane runnable on this platform, then certify it — **minutes**, on its own clock (the nightly CI leg) |
| `just lane-certify` / `just lane-health` | judge the existing lane artifacts against the declared bounds / publish each lane's pass-rate history, with *never green* as its own category |
| `just ci` | every rung CI blocks on: `fmt-check lint test audit plugins-verify sdk inventory flake-check harness-test`. The long lanes are deliberately **absent** — a minutes-long certification hung off the pre-push habit is how the habit stops happening |
| `just build` | `cargo build -p pumper-server` |
| `just run` | `cargo run -p pumper-server --bin pumper` → http://127.0.0.1:8088 |
| `just dev` | same, with `RUST_LOG=debug` |
| `just reindex` | `cargo run -p pumper-server --bin reindex` |
| `just search-backfill <scope>` | `cargo run -p pumper-server --bin search-backfill -- <scope>` |
| `just plugin <crate>` | builds `plugins-src/<crate>` for `wasm32-unknown-unknown` (build only — does **not** install) |
| `just plugins-install` | builds **and installs** the two trigger-hook plugins (`trigger-gate`, `delta-slim`) into `data/plugins/`; without it every configured trigger hook takes the fail-open unknown-plugin path — see `docs/features/trigger-plugins.md` |
| `just doctor [port]` | `GET /datasets/doctor` — read-only store integrity report (server RUNNING) |
| `just retention-preview [days]` | `GET /retention/preview` — reclaimable bytes, deletes nothing (server RUNNING) |
| `just enforcement-preview [app]` | `GET /enforcement/preview` — what `[resilience] enforce = true` would have done, gates nothing (server RUNNING) |
| `just datahub-preview [port]` | `GET /datahub/governance/preview` — what the DataHub governance poll would do right now, writes nothing (server RUNNING) |
| `just smoke [args]` | `pwsh -File scripts/smoke.ps1` — boots the real binary against a scratch config, drives one real job end-to-end, and curls the doctor/retention/enforcement-preview/openapi/receipt surfaces; PASS/FAIL/SKIP per check, tears down after itself |

`--bin pumper` is **required**: the `pumper-server` package ships three binaries
(`pumper`, `reindex`, `search-backfill`) and has no `default-run`, so a bare
`cargo run -p pumper-server` errors with *"could not determine which binary to
run"*. The two maintenance binaries must run with the **server stopped** (Tantivy
holds an exclusive writer lock; `reindex` rewrites derived columns).

Config is `config.toml`, or `$PUMPER_CONFIG`; every key is optional (defaults in
`crates/core/src/config.rs`).

Doc-sync hook (Node, standalone-runnable; reads a hook payload on stdin):

```bash
node scripts/docs/check-doc-sync.mjs
```

## Architecture

```
crates/
  core/            traits + models everything else plugs into — ScrapeApp, engine
                   traits, tiered Fetcher, Storage (SQLite job queue), Datasets
                   (change detection), HttpCache, Governor, extract, simhash,
                   crawl, resilience, catalog, config
  engine-http/     reqwest + retries, fronted by cache + governor
  engine-browser/  chromiumoxide, lazy-launched persistent Chrome profile
  engine-claude/   `claude -p` subprocess with model/effort roles
  engine-wasm/     wasmtime plugin host (CPU fuel + memory cap)
  engine-search/   tantivy embedded full-text index
  apps/            one crate per scraping use case, plus `*-common` shared helpers
  server/          axum API (routes/ module tree) + worker pool + scheduler +
                   webhooks + triggers + SSE + datahub
plugins-src/       example WASM plugins (Rust -> wasm32), built separately
catalog/           data-sources.toml — the machine-readable pipeline registry
clients/typescript @pumper/sync consumer SDK
```

**Dependency rule (README.md §Architecture):** apps depend only on `core` (plus
parsing libs like `scraper`). Engines also depend only on `core`. The server wires
everything together. That keeps every new use case a self-contained crate — never
make an app depend on another app or on an engine crate.

Adding an app is a four-step contract (new crate → `impl ScrapeApp` → register in
`crates/server/src/registry.rs` → add its `[[source]]` to `catalog/data-sources.toml`).
Full instructions: README.md §"Adding a scraping use case" and ONBOARDING.md §5–7.

## Session memory

Read [MEMORY.md](MEMORY.md) at session start — it indexes the repo's durable state
(`.perfect/Architect/backlog.md`, decisions, coverage, pattern catalogues) and the
non-obvious invariants that are easy to violate. At session end, record durable
learnings there or in the `.perfect/` file that owns them (a new architectural
decision → `.perfect/Architect/decisions/`, a new invariant/gotcha → MEMORY.md).
Do not let a hard-won fact live only in the transcript.

## Where the depth is

- **[.claude/CLAUDE.md](.claude/CLAUDE.md)** — the policy file, and it is binding:
  the `context-map.json` protocol, the same-session documentation-sync rule, and
  the "bug fixes ship as extracted, tested functions" doctrine. Read it before
  editing code. Not duplicated here.
- **[ONBOARDING.md](ONBOARDING.md)** — the agent-facing contract: engine choice,
  extension seams, invariants (§7), verification loop (§8), the continuous-
  development charter (§9), the catalog rule (§10).
- **[README.md](README.md)** — human quickstart, the API surface by example, and
  the roadmap.
- **[docs/features/](docs/features/README.md)** — what each feature actually does
  today (API/params, data model, known gaps). `docs/harness/harness-learnings.md`
  holds structural facts and the pattern catalogue.
- **[docs/deployment.md](docs/deployment.md)** — build artifact, persistent state
  layout, environment variables, and the auth posture.

When docs disagree with each other, trust the code, then fix whichever doc is stale.

<!-- personas:context-map:start -->
## Project Context Map

This project is organized into **46 contexts** across **8 groups**. The full machine-readable map lives in `context-map.json` at the project root — read it at task start to scope your edits to the relevant context's files.

Taxonomy: each context has a `category` (ui · api · lib · data · test · config); each group has a `domain` (feature · infrastructure · shared · integration · data).

### Groups

- **Scraping Engines** _(domain: shared · 8 contexts)_
- **Core Platform** _(domain: shared · 8 contexts)_
- **HTTP API** _(domain: infrastructure · 4 contexts)_
- **Job Orchestration** _(domain: infrastructure · 3 contexts)_
- **Event Pipeline** _(domain: integration · 4 contexts)_
- **Content & Research Apps** _(domain: feature · 10 contexts)_
- **Grants Intelligence** _(domain: feature · 5 contexts)_
- **Market Data** _(domain: feature · 4 contexts)_

> Auto-generated by Personas on each context scan. Edits between the markers are overwritten on the next scan; edit `context-map.json` or rescan instead.
<!-- personas:context-map:end -->
