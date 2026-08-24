# Stack profile: pumper (Rust workspace · axum + SQLite queue · pluggable scrape engines)

**Matches when:** the repo root has a `Cargo.toml` with `[workspace]`, crates under `crates/` including `core` + `server` + `engine-*`, and `crates/apps/` holding one crate per scraping use case.

**⚠️ Verify, don't trust:** this profile is hints, not facts. When the boot audit contradicts it, believe the audit and update this file so the correction sticks for the next loop.

## Shape

```
crates/
├─ core/          traits + models everything plugs into (ScrapeApp, AppContext, engine traits,
│                 storage/job queue, datasets, cache, governor, extract, simhash, crawl, config)
│    migrations/  SQL migrations (sqlx, applied automatically at startup)
├─ engine-http/     impl HttpClient  (reqwest + retries, fronted by cache + governor)
├─ engine-browser/  impl Browser     (chromiumoxide, persistent profile in data/browser-profile)
├─ engine-claude/   impl Researcher  (spawns the `claude` CLI — NOT an HTTP SDK)
├─ engine-wasm/     wasmtime plugin host (fuel + memory sandbox)
├─ engine-search/   tantivy full-text index
├─ apps/            ~24 crates, one per scraping use case
└─ server/          axum API + worker pool + cron scheduler + webhooks + SSE + search
                    src/registry.rs  ← the list of active apps
                    src/routes/      ← the HTTP surface (mod.rs holds the EXPECTED inventory test)
clients/typescript/  the SDK
catalog/             data-sources.toml — the authoritative list of what this machine scrapes
docs/features/       the implemented-product reference (doc-sync enforced by a Stop hook)
context-map.json     22 contexts in 7 groups — the feature map
data/                gitignored runtime: SQLite db, artifacts, browser profile, plugins
```

**The dependency rule (do not break it):** apps depend on `core` only (plus leaf parsing libs); engines depend on `core` only; only `server` depends on everything. An app that pulls in `pumper-engine-*` is a finding — it wants a trait from `core` handed over via `AppContext.engines`. Breaking `core` is the one move that hurts other agents' in-flight work: do it deliberately, update every impl in the same change, and run the full test suite.

## Commands

The repo-root **`justfile` is the canonical task runner** — `cargo install just`, then `just --list`. `CLAUDE.md` carries the same table. Prefer the `just` recipe; the raw cargo form is listed because that is what CI invokes. **Everything runs from the repo root** (the `.env` loader and the default `config.toml` path are both CWD-relative).

| `just` | raw cargo | notes |
|---|---|---|
| `just ci` | `fmt-check` + `lint` + `test` | the entire CI job in one command — the loop's gate ladder |
| `just fmt-check` | `cargo fmt --check` | CI gate |
| `just lint` | `cargo clippy --workspace --all-targets` | CI gate |
| `just test` | `cargo test --workspace` | CI gate |
| `just test-ignored` | `cargo test --workspace -- --ignored` | env-dependent (real Chrome, built wasm, timing) — **not** in CI |
| `just check` | `cargo check --workspace` | fast inner loop |
| `just build` | `cargo build -p pumper-server` | append `--release` yourself for an optimized build |
| `just run` | `cargo run -p pumper-server --bin pumper` | boots on `http://127.0.0.1:8088` |
| `just dev` | same with `RUST_LOG=debug` | verbose boot |
| `just reindex` | `cargo run -p pumper-server --bin reindex` | **server must be stopped** |
| `just search-backfill <scope>` | `cargo run -p pumper-server --bin search-backfill -- <scope>` | **server must be stopped**; e.g. `just search-backfill --all` |
| `just plugin <crate>` | builds `plugins-src/<crate>` for `wasm32-unknown-unknown` | detached workspace |

**`--bin pumper` is required.** The `pumper-server` package ships three binaries (`pumper`, `reindex`, `search-backfill`) and sets no `default-run`, so a bare `cargo run -p pumper-server` fails with *"could not determine which binary to run"*. The two maintenance binaries hold exclusive locks and must run with the server stopped.

- **Ignored tests:** environment-dependent tests are `#[ignore]`d and NOT in CI. Run `just test-ignored` at Milestone cadence — a compile break in an ignored test is invisible to CI.
- SDK: `npm run build` + `npm test` inside `clients/typescript`
- Doc sync: `node scripts/docs/check-doc-sync.mjs` (also wired as a Stop hook in `.claude/settings.json`)
- WASM example plugins in `plugins-src/` build in their own detached workspaces — not part of `cargo build --workspace`; use `just plugin <crate>`.
- **If the loop changes a command, update the `justfile` and `CLAUDE.md`'s table in the same commit** — they are what every other session reads.

## Runtime facts that shape the loop

- **Durable queue on SQLite (WAL).** Jobs survive restarts; in-flight jobs are re-queued by crash recovery at startup. Priorities, per-app fairness caps, a global concurrency cap, per-job wall-clock timeouts, and exponential-backoff retries bounded by `max_attempts` all live in the worker/storage path. Prove crash recovery by actually killing the process, not with a unit test.
- **Tiered fetcher** `http → browser → claude` with automatic escalation when a tier returns too little content. **Every escalation into the claude tier costs money** — the escalation threshold is a tunable with a direct line to dimension 9.
- **Dataset store with change detection.** Apps upsert records; the store reports new/changed/unchanged. Correctness here *is* the product — see dimension 5.
- **HTTP response cache** (content-addressed, TTL) fronts the http engine; a **per-domain governor** spaces requests per host. Code paths that bypass either are findings.
- **The `claude` engine is a CLI subprocess** — `claude -p --output-format json`, prompt over stdin, with `--model` / `--effort` / `--max-budget-usd` / `--json-schema` / `--resume` flags built from `ResearchRequest` and the `[claude.roles.*]` presets in `config.toml`. There is no API key and no HTTP SDK anywhere in the repo. Anything touching this surface should hand off to `/tiger`, which has the full pre-resolved map.
- **Config**: `config.toml` at the repo root, every key optional, defaults in `crates/core/src/config.rs`. Key drift between the two is a dimension-8 finding.
- **OpenAPI + route inventory**: `crates/server/src/routes/mod.rs` holds an `EXPECTED` route list and a test asserting the generated spec matches it exactly. This is the repo's canonical "convention enforced by an inventory test" pattern — reuse the idiom rather than inventing a new one, and expect the test to fail (correctly) whenever the loop adds a route.

## Deliberate trades — do NOT file these as findings

From `ONBOARDING.md` §2: no API auth and permissive CORS; the Claude CLI runs with `--dangerously-skip-permissions`; the browser keeps real login cookies on disk; non-2xx HTTP bodies are returned rather than raised (apps decide via `response.is_success()`). These are intentional local-first choices. If the loop believes one genuinely must change, that is a checkpoint question, never a silent backlog item.

## Repo etiquette

- **Shared checkout, concurrent sessions.** `/architect`, `/perfect`, `/explorer`, and `/tiger` commit into this same tree with their own prefixes. Stage only your explicit paths in one bash invocation; never `git add -A`/`.`/`-u`; never `--amend`; never `git stash`/`reset --hard`/`clean -f`. On `index.lock`, wait 3–10s and retry up to 6 times. Expect HEAD to advance mid-run.
- Commit per coherent change with a `ship:` prefix and a body explaining the why. Don't push unless asked — the local gate is primary.
- `.claude/` is gitignored except `CLAUDE.md` and `settings.json`, so the ship-loop state dir stays untracked. Never force-add it.
- **Bug fixes ship as extracted, tested functions** (`.claude/CLAUDE.md`): extract the predicate/transform into a named function, add a test named after the anti-pattern it defends (`x_not_y`), then wire it in. Conventions get an inventory test, not a sentence in a doc.
- **Docs sync in the same session** for any user/API-visible change; `scripts/docs/feature-doc-map.json` is the source→doc map.
- **Keep `context-map.json` true** when file ownership changes.
- `docs/harness/harness-learnings.md` holds structural facts and the pattern catalogue — read it before large changes.
- **Read `CLAUDE.md` and `MEMORY.md` at the repo root first.** `CLAUDE.md` is the shortest true summary (commands table, architecture, dependency rule, the four-step app-adding contract). `MEMORY.md` indexes the repo's durable state under `.perfect/` — including `.perfect/Architect/backlog.md`, whose **Pending** section must be checked before the loop files structural backlog items, or the loop will duplicate work `/architect` has already queued.
- **`docs/deployment.md`** is the authority on the run story: local-first operation, persistent state layout, and the auth posture. Dimensions 6 (resilience & safety) and 8 (ops readiness) are scored against it, not against generic SaaS deployment expectations.
