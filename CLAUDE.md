# CLAUDE.md

Pumper is a **local-first scraping service**: one Rust binary that exposes an HTTP
API, runs a durable job queue on SQLite (WAL), and scrapes through pluggable
engines. Cargo workspace, edition 2021, `resolver = "2"`.

## Commands

All from the repo root (the `.env` loader and the default `config.toml` path are
both **CWD-relative**).

```bash
cargo check                                  # fast type-check of the whole workspace
cargo test --workspace                       # what CI runs
cargo test -- --ignored                      # env-dependent tests (real Chrome, wasm artifact, wall-clock)
cargo fmt --check                            # CI gate
cargo clippy --workspace --all-targets       # CI gate
cargo build -p pumper-server                 # produce the binary
cargo run -p pumper-server --bin pumper      # boot it -> http://127.0.0.1:8088
```

`--bin pumper` is **required**: the `pumper-server` package ships three binaries
(`pumper`, `reindex`, `search-backfill`) and has no `default-run`, so a bare
`cargo run -p pumper-server` errors out. (README.md §Run and ONBOARDING.md §8 still
show the bare form — they are stale on this point.)

`RUST_LOG=debug` for verbose logs. Config is `config.toml`, or `$PUMPER_CONFIG`;
every key is optional (defaults in `crates/core/src/config.rs`).

Maintenance binaries — **run with the server stopped** (Tantivy holds an exclusive
writer lock; `reindex` rewrites derived columns):

```bash
cargo run -p pumper-server --bin reindex                      # recompute every record's SimHash
cargo run -p pumper-server --bin search-backfill -- --all     # rebuild the full-text index (or --app/--dataset)
```

WASM plugins build separately, to a different target:

```bash
cd plugins-src/title-extractor && cargo build --release --target wasm32-unknown-unknown
```

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

When docs disagree with each other, trust the code, then fix whichever doc is stale.
