# ONBOARDING — for Claude Code CLIs working in this codebase

**You are reading this because you are an AI coding agent (Claude Code CLI), most
likely driving *another* app on this machine, and you need to either _use_ Pumper
to scrape something, or _extend_ Pumper with a new capability.**

This document is your map. Read the section that matches your goal, follow the
contracts exactly, and verify with the commands in [§8](#8-verification-loop--do-this-before-you-finish).
This codebase is **built to be extended by many agents over time** — see
[§9](#9-continuous-development-charter) for how to do that without breaking others' work.

> Human-facing quickstart lives in `README.md`. **This file is the agent-facing
> contract**: the invariants, the extension seams, and the guardrails. When the
> two disagree, trust the code, then fix whichever doc is stale.

---

## 1. What Pumper is

Pumper is a **local-first scraping service**: one Rust binary (`pumper`) that
exposes an HTTP API, runs a durable job queue on SQLite, and scrapes through
three interchangeable engines. Other apps on this machine enqueue scraping jobs
over HTTP and poll for results.

**Three scraping engines, pick per use case:**

| Engine    | Crate            | Reach for it when…                                                        |
|-----------|------------------|---------------------------------------------------------------------------|
| `http`    | `engine-http`    | The page is server-rendered or it's a JSON API. Fast path. reqwest + cookie jar + retries. |
| `browser` | `engine-browser` | The page needs JS, or you must be logged in. Headless Chrome (CDP) with a **persistent profile**. |
| `claude`  | `engine-claude`  | No fixed crawler works — you need judgement, multi-source synthesis, or open-ended research. Runs the Claude Code CLI headlessly with WebSearch/WebFetch. |

**Feature checklist (what already works):**

- Durable job queue (SQLite WAL) — jobs survive restarts; in-flight jobs are
  re-queued on crash recovery at startup.
- Worker pool with a **global** concurrency cap and **per-app** caps (fairness,
  so one busy app can't starve others), plus per-job wall-clock timeouts.
- Job **priorities** — higher-priority jobs claim ahead of others.
- Automatic retries with exponential backoff (`max_attempts` per job).
- **Tiered fetcher** — `http → browser → claude` with automatic escalation when
  a tier returns too little content.
- **HTML → clean Markdown** preprocessing (`ctx.engines`/`html_to_markdown`).
- **Dataset store with change detection** — apps upsert records; the store
  reports new/changed/unchanged (dedup + monitoring), queryable and exportable.
- **HTTP response cache** (content-addressed, TTL) fronting the http engine.
- **Per-domain politeness governor** (token-bucket spacing per host).
- **Claude model/effort roles** — `research` (Sonnet, normal reasoning) and
  `compose` (Opus, xhigh) presets, overridable per job.
- **Dynamic cron schedules** in the DB — create/enable/disable/delete via API.
- **Result webhooks** — POST the finished job to a caller URL (HMAC-signed).
- **Observability** — `/metrics` (Prometheus text) and SSE live job streams.
- **Multi-core SIMD extraction** — declarative rule sets (CSS/regex/JSON-pointer)
  run over document batches across all cores (`core::extract`, `extractor` app).
- **Sandboxed WASM plugins** — untrusted `.wasm` extractors run in-process with
  a CPU-fuel budget + memory cap (`engine-wasm`, `plugin` app; `/plugins`).
- **Embedded full-text search** (Tantivy, in-process) auto-indexed from job
  results (`/search`), plus **SimHash near-duplicate detection** over datasets
  (`/datasets/{app}/{ds}/duplicates`).
- **High-concurrency broad crawler** — bounded frontier, robots.txt, near-dup
  dropping, bodies streamed to disk (`core::crawl`, `crawl` app).
- Per-job artifact directory for raw dumps (HTML, JSON, screenshots).
- Claude runs report cost / turns / session id back in the job result.

---

## 2. Operating principles — read before you "improve" anything

Pumper runs **only on this machine** and deliberately trades security for power.
These are intentional design choices, **not** bugs to fix. Do not "harden" them
away without an explicit instruction:

- **No API auth, permissive CORS.** Any local app may call the API directly.
- **Claude engine runs with `--dangerously-skip-permissions`.** That is the point —
  headless research with no prompts.
- **Browser keeps real login cookies on disk** (`data/browser-profile`).
- **HTTP bodies for non-2xx are returned, not raised** — scrapers often need to
  read 403/404 pages. Apps decide via `response.is_success()`.

If you believe one of these genuinely needs to change, say so and ask first
([§9](#9-continuous-development-charter) covers coordination). Otherwise, build
_with_ the grain.

---

## 3. Codebase map & the one rule that keeps it clean

```
pumper/
├─ config.toml            all runtime config (every key optional; defaults in code)
├─ crates/
│  ├─ core/               the contract everything plugs into — depend on this, not on siblings
│  │   src/app.rs           ScrapeApp trait + AppContext (what a job receives)
│  │   src/engine.rs        engine capability traits + request/response types
│  │   src/storage.rs       SQLite job queue (enqueue/claim/complete/fail/recover)
│  │   src/config.rs        config.toml schema + loader
│  │   src/job.rs           Job / JobStatus models
│  │   src/error.rs         Error / Result
│  │   migrations/          SQL migrations (sqlx, run automatically at startup)
│  ├─ engine-http/         impl HttpClient  (reqwest)
│  ├─ engine-browser/      impl Browser     (chromiumoxide)
│  ├─ engine-claude/       impl Researcher  (claude CLI subprocess)
│  ├─ apps/                ← ONE CRATE PER SCRAPING USE CASE (this is where features live)
│  │   ├─ hackernews/        template: fetch-and-parse via http engine
│  │   └─ research/          template: agentic research via claude engine
│  └─ server/              axum API + worker pool + cron scheduler + registry
│      src/registry.rs       ← the list of active apps (you edit this to register)
│      src/routes.rs         HTTP surface
│      src/worker.rs         claims jobs, runs them, handles timeout/retry
│      src/scheduler.rs      fires cron-scheduled apps
│      src/state.rs          builds engines + registry at boot
└─ data/                   sqlite db + artifacts + browser profile (git-ignored)
```

### THE dependency rule (do not break this)

```
apps  ─depend on─►  core  ◄─depend on─  engines
                     ▲
                     └────── server wires apps + engines together
```

- **Apps depend on `core` only** (plus leaf parsing libs like `scraper`). An app
  MUST NOT depend on an engine crate or on another app.
- **Engines depend on `core` only.**
- **Only `server` depends on everything**, and only to wire it up.

This is what makes every use case a self-contained, independently-developable
crate. If you find yourself adding `pumper-engine-*` to an app's `Cargo.toml`,
stop — you want the trait from `core`, handed to you via `AppContext.engines`.

---

## 4. Path A — Just consume the service (you're scraping for another app)

Start it (from this directory): `cargo run -p pumper-server --bin pumper` (or
`just run`) → listens on `http://127.0.0.1:8088` (configurable in `config.toml`).
Naming the binary is required — see [§8](#8-verification-loop--do-this-before-you-finish).

| Method & path              | Purpose                                                        |
|----------------------------|----------------------------------------------------------------|
| `GET /health`              | Liveness.                                                      |
| `GET /metrics`             | Prometheus-style gauges (jobs by status, apps, schedules).    |
| `GET /apps`                | List registered apps (name, description, schedule).           |
| `POST /apps/{name}/jobs`   | Enqueue a job. Returns `202` + the `Job` (grab `id`).         |
| `GET /jobs/{id}`           | Poll one job — `status`, `result`, `error`.                   |
| `GET /jobs/{id}/stream`    | SSE stream of this job's transitions; closes when terminal.   |
| `GET /jobs?app=&status=&limit=` | List jobs (filters optional).                            |
| `DELETE /jobs/{id}`        | Cancel a job that is still `queued`.                          |
| `GET /events`              | SSE stream of **all** job transitions.                        |
| `GET /schedules`           | List cron schedules.                                          |
| `POST /schedules`          | Create one: `{app, cron, params?, priority?}`.               |
| `POST /schedules/{id}/enabled` | Enable/disable: `{enabled: bool}`.                       |
| `DELETE /schedules/{id}`   | Delete a schedule.                                            |
| `GET /apps/{name}/datasets`| List an app's dataset names.                                 |
| `GET /datasets/{app}/{dataset}?limit=` | Query stored records (change-detected).         |
| `GET /datasets/{app}/{dataset}/export` | Export the whole dataset as JSON.               |
| `GET /datasets/{app}/{dataset}/duplicates?distance=` | Near-duplicate record pairs (SimHash). |
| `GET /search?q=&limit=`    | Full-text search (BM25) over indexed job results.            |
| `GET /plugins`             | List loaded WASM plugins.                                     |
| `POST /plugins/reload`     | Hot-swap: rescan `data/plugins` for `.wasm` modules.         |

**Enqueue body** (all fields optional):

```json
{ "params": { "…app-specific…" }, "max_attempts": 3, "delay_secs": 0,
  "priority": 5, "callback_url": "https://…/hook", "callback_secret": "…" }
```

- `params` — passed verbatim to the app; omit to use the app's `default_params()`.
- `max_attempts` — default `1`; higher enables retry-with-backoff on failure.
- `delay_secs` — schedule the job to become runnable later.
- `priority` — higher runs sooner (default `0`).
- `callback_url` / `callback_secret` — on terminal state the worker POSTs the
  job JSON here; if a secret is set, the body is HMAC-SHA256 signed and sent as
  `X-Pumper-Signature: sha256=<hex>`. So you can push results instead of polling.

**Job lifecycle:** `queued → running → succeeded | failed | cancelled`. Poll
`GET /jobs/{id}` until `status` is terminal, then read `result` (or `error`).
Structured output for each app is under `result`; raw dumps are on disk at
`data/artifacts/<app>/<job_id>/`.

Example (PowerShell — this is a Windows machine):

```powershell
$job = irm -Method Post http://127.0.0.1:8088/apps/hackernews/jobs `
    -ContentType 'application/json' -Body '{"params":{"pages":2}}'
irm "http://127.0.0.1:8088/jobs/$($job.id)"
```

If the app you need doesn't exist yet, switch to Path B and build it.

---

## 5. Path B — Add a scraping use case (the primary extension path)

This is a **4-step contract**. Copy `crates/apps/hackernews` (http engine) or
`crates/apps/research` (claude engine) as your starting template.

### Step 1 — Create the crate `crates/apps/<name>/`

The workspace globs `crates/apps/*`, so a new folder is picked up automatically.

```toml
# crates/apps/<name>/Cargo.toml
[package]
name = "app-<name>"
version.workspace = true
edition.workspace = true

[dependencies]
pumper-core.workspace = true
async-trait.workspace = true
serde_json.workspace = true
scraper.workspace = true      # only if you parse HTML
serde = { workspace = true }  # only if you derive Serialize on output structs
```

### Step 2 — Implement `ScrapeApp`

The full trait (`crates/core/src/app.rs`):

```rust
#[async_trait]
pub trait ScrapeApp: Send + Sync {
    fn name(&self) -> &'static str;               // becomes the API path segment; must be unique
    fn description(&self) -> &'static str { "" }  // shown in GET /apps — document your params here
    fn schedule(&self) -> Option<&'static str> { None }   // 6-field cron w/ seconds; None = manual only
    fn requires(&self) -> &'static [Requirement] { &[] }  // preconditions (e.g. an API-key env var)
    fn default_params(&self) -> Value { Value::Object(Default::default()) } // scheduled + body-less runs
    fn manifest(&self) -> AppManifest { AppManifest::default() }            // agent-facing contract
    async fn run(&self, ctx: AppContext) -> Result<Value>;// returns JSON stored as the job result
}
```

`run()` is the only required method; the other six have defaults. Two are worth
overriding on purpose:

- **`manifest()`** — `AppManifest { params_schema, examples, output_shape,
  cost_class }`. Declaring `params_schema` (JSON Schema draft 2020-12) makes
  enqueue **enforce** it: a bad `params` body is rejected with **422** and
  JSON-pointer paths instead of failing halfway through a run. `examples` are
  worked invocations, and a test asserts each one validates against your own
  schema, so they cannot rot. This is what makes an app usable by an agent that
  has never read its source.
- **`requires()`** — `&[Requirement]`, surfaced by `GET /apps` as a resolved
  `ready` flag, so a credential-gated app is distinguishable from a working one
  *before* its first failed job.

Minimal implementation:

```rust
use async_trait::async_trait;
use pumper_core::{AppContext, FetchRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct MyApp;

#[async_trait]
impl ScrapeApp for MyApp {
    fn name(&self) -> &'static str { "myapp" }
    fn description(&self) -> &'static str { "What it scrapes. Params: {\"url\": \"…\"}" }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx.require_str("url")?;                    // typed access to ctx.params
        let out = ctx.fetch(FetchRequest::new(url)).await?;   // metered tiered fetch — §6
        let html = out.html.unwrap_or_default();
        ctx.save_artifact("page.html", html.as_bytes()).await?;
        // …parse html with `scraper`, build your output…
        Ok(json!({ "url": url, "items": [] }))
    }
}
```

**`AppContext` gives you** (`crates/core/src/app.rs`):
- `ctx.params: Value` — the enqueue `params`. `ctx.require_str("k")` for a
  required string (errors cleanly if missing).
- **`ctx.fetch(FetchRequest)`** and **`ctx.research(ResearchRequest)`** — the two
  **metered seams**, and the ones to reach for by default. They add cost
  attribution, the per-job budget clamp, the learned tier router and VCR
  record/replay. See §6.
- `ctx.engines` — `.http`, `.browser` and `.fetch` (the raw engines). Public on
  purpose, for the cases the metered seam cannot serve (a POST body, a
  conditional GET, a binary `fetch_bytes`, a crawler that owns its own
  frontier) — every raw call site in the workspace is inventoried by
  `crates/core/tests/fetch_chokepoint.rs`, so adding one is a reviewed decision.
  That decision has a **second half**: raw traffic is invisible to the VCR
  cassette in both directions, so an app that drives engines raw also needs a row
  in `REPLAY_BYPASS_APPS` (`crates/core/src/vcr.rs`) grading it `Partial` or
  `Unreplayable`. Without it the app is assumed replayable, and a `replay_of` job
  runs it live under a `vcr_replay_of` provenance stamp.
  **There is no `ctx.engines.claude`:** the researcher is `pub(crate)` so a model
  call cannot skip metering — use `ctx.research(...)`.
- `ctx.upsert(dataset, key, &value).await` → `ChangeKind` and
  `ctx.upsert_many(dataset, &items).await` →
  `UpsertSummary { new, changed, unchanged, removed }` — dedup + change
  detection, scoped to this app. (`removed` is only ever populated by
  `ctx.sync_many`, the full-snapshot variant that also tombstones.)
- `ctx.plugins.run(name, input, &params).await` → JSON — run a sandboxed WASM
  plugin. `params` is the per-call config envelope (pass `&json!({})` if the
  plugin needs none).
- `ctx.save_artifact(name, bytes).await` — writes under this job's artifact dir.
- `ctx.app` (this app's name) and `ctx.job_id` (UUID) for correlation.

Free functions in `core` for the Rust-leverage features:

- `extract_batch(&compiled, &docs)` — multi-core extraction; call inside
  `spawn_blocking`.
- `crawl(http, cfg, output_dir, sink, source, progress, checkpointer)` — the
  broad crawler. The last five are `Option`, so
  `crawl(http, cfg, Some(dir), None, None, None, None)` is the simple form.
- `simhash(&text)` / `hamming(a, b)` — near-duplicate detection.
- `html_to_markdown(&html)` — clean Markdown from a page.

### Step 3 — Register the crate in the workspace + server

Two `Cargo.toml` edits:

```toml
# root Cargo.toml → [workspace.dependencies]
app-<name> = { path = "crates/apps/<name>" }
```
```toml
# crates/server/Cargo.toml → [dependencies]
app-<name>.workspace = true
```

### Step 4 — Add ONE line to the registry

```rust
// crates/server/src/registry.rs
pub fn apps() -> Vec<Arc<dyn ScrapeApp>> {
    vec![
        Arc::new(app_hackernews::HackerNews),
        Arc::new(app_research::Research),
        Arc::new(app_<name>::MyApp),   // ← your line
    ]
}
```

That's the whole integration. `cargo run -p pumper-server --bin pumper` (or
`just run`), confirm your app appears in `GET /apps`, enqueue a job, verify the
result.

**Cron note:** `schedule()` returns a 6-field expression *with seconds*:
`sec min hour day month weekday`. `"0 0 */6 * * *"` = every 6 hours. Scheduled
runs use `default_params()`, so make sure those params are sufficient (e.g. the
`research` app needs a `query` and therefore should not be scheduled without one).

### Step 5 — add a catalog entry (required, not optional)

Append a `[[source]]` block to `catalog/data-sources.toml` describing what this app
scrapes — its market, data category, engine, cadence, access, and confidence. The
catalog is how any human or agent assesses the data-pipeline state without reading
every app; an app that isn't catalogued is invisible to that overview. Schema and
how-to live in `catalog/README.md` and [§10](#10-data-source-catalog).

---

## 6. Engine capabilities reference

Signatures live in `crates/core/src/engine.rs` and `fetcher.rs`. All return
`pumper_core::Result<_>`.

**Reach for the metered seams first** — `ctx.fetch(...)` and `ctx.research(...)`.
The raw engines under `ctx.engines.*` are public for the cases those cannot
serve, and every raw call site is inventoried (see §5).

### `ctx.engines.http` — `HttpClient::fetch(HttpRequest) -> HttpResponse`
```rust
let res = ctx.engines.http.fetch(HttpRequest::get("https://api.example.com")).await?;
// HttpRequest also supports .method (GET/POST), .headers, .body, .etag,
// .if_modified_since, .max_body_bytes, .timeout_secs, .proxy, .profile, .no_cache
// HttpResponse { status, headers, body: String, final_url, cache_hit }; res.is_success()
```
Retries the configured `[http] retryable_statuses` (default `429/502/503/504`)
with backoff automatically. Shares a cookie jar across calls within the process.

`HttpClient::fetch_bytes(HttpRequest) -> Vec<u8>` is the **binary** seam (no
charset decoding, no response cache, buffered in memory under `max_body_bytes`).
It is a default-bodied trait method, so not every engine implements it — the
one at `ctx.engines.http` does, and a conformance test pins that. See
[docs/features/fetching.md](docs/features/fetching.md#engine-capability-contract).

### `ctx.engines.browser` — `Browser::render(RenderRequest) -> RenderedPage`
```rust
let mut req = RenderRequest::new("https://spa.example.com");
req.wait_for_selector = Some(".results".into());  // wait for an element
req.extra_wait_ms = Some(1500);                    // extra settle time
req.evaluate = Some("document.title".into());      // JS → RenderedPage.evaluated (JSON)
let page = ctx.engines.browser.render(req).await?;
// RenderedPage { html, final_url, evaluated, nav_timed_out, selector_found,
//                blocked_resources, actions_completed, network }
```
Chrome launches lazily on first use and stays warm. **Logged-in scraping:** set
`headless = false` in `[browser]`, run a job, log in to the site in the window
that opens, then set `headless = true` — cookies persist in `data/browser-profile`.

`Browser::transact(TransactRequest) -> TransactEvidence` is the declarative
**dry-run flow** seam (form fill → evidence bundle, stopping before the
irreversible action). Like `fetch_bytes` it is default-bodied: an engine that
does not implement it refuses with a **terminal** `Error::Transact`, so the job
fails once rather than riding the retry ladder.

### `ctx.research(...)` — the ONLY way to reach the model
```rust
let mut req = ResearchRequest::new("Research X. Reply with ONLY JSON: {…schema…}")
    .with_role("compose");          // Opus @ xhigh; or "research" = Sonnet @ high
req.max_turns = Some(25);
req.effort = Some("max".into());    // per-job override of the role's effort
req.resume_session = Some(prev_id); // multi-step: continue a prior CLI session
let out = ctx.research(req).await?;
// ResearchOutput { text, json: Option<Value>, cost_usd, duration_ms, num_turns, session_id }
```
There is **no `ctx.engines.claude`**. The researcher behind `EngineSet` is
`pub(crate)`, so an app crate cannot name it — a direct call would silently lose
the research cache, the per-job budget governor and cost metering, and it once
did (`connector-api-watch` summarized every doc diff off-ledger). The privacy is
the guard; `crates/core/tests/llm_chokepoint.rs` bans the string in app crates as
the backstop. `ctx.research` is the metered wrapper: identical requests inside
the cache TTL are served from disk at $0, misses refuse to start once the job
budget is spent, and every call — including a failed one that already spent —
lands in `cost_events`.

Model + reasoning are chosen per job: pass a `role` (presets in `[claude.roles]`),
or set `model` / `effort` (`low|medium|high|xhigh|max`) directly — request fields
override the role, which overrides the config default, **per field independently**.
`out.json` is auto-populated when the reply parses as JSON (fenced and
prose-embedded JSON are both extracted; `json_schema` uses the CLI's validated
`--json-schema` output). **Instruct the agent to return strict JSON** for
structured output.

The engine refuses bad input rather than passing it to the subprocess: a `role`
no config defines is an error naming the configured roles (a *missing* role is
fine — it takes the defaults), a `model` outside `[A-Za-z0-9._:-]{1,128}` is
refused, and on the Windows shim path any value holding `% & | < > ^` or a
newline is refused instead of being mangled by cmd.exe's second parse. All of
these fail before spawning, so they cost nothing. `append_system_prompt` is
exempt — it travels by file, not argv — so prose may contain anything. The
subprocess runs in `<storage root>/claude-cwd`, not the server's CWD, so it does
not inherit your checkout's `CLAUDE.md`/hooks. See
[docs/features/fetching.md](docs/features/fetching.md#what-may-cross-the-cmdexe-shim).

### `ctx.fetch(...)` — `Fetcher::fetch(FetchRequest) -> FetchOutcome`, metered
```rust
let mut req = FetchRequest::new("https://…");
req.strategy = FetchStrategy::AutoWithResearch; // http → browser → claude
req.to_markdown = true;
let out = ctx.fetch(req).await?;
// FetchOutcome { url, engine, status, html, markdown, text,
//                escalations, trace, cost_usd, snapshot }
```
The fetcher starts on the cheapest tier and escalates when the extracted text is
below `min_content_chars` (default 250) **or the response is a bot-wall**.
`FetchStrategy` = `Http | Browser | Auto | AutoWithResearch`.

- `engine` is the winning tier: `"archive" | "api_recipe" | "http" | "browser" |
  "claude"` (the first two are opt-in pre-live tiers).
- `trace` is the **structured** per-tier record — branch on `TierTrace.verdict`
  (`ok | thin | blocked | error | skipped_by_router`) rather than parsing the
  human `escalations` lines, which are kept alongside for the trail.
- `snapshot` is `Some` only when the body came out of a stored capture rather
  than the live site, carrying its capture time.

`ctx.engines.fetch.fetch(...)` is the same call **unmetered** — no cost event, no
budget clamp, no tier learning, no VCR. Use `ctx.fetch`.

### HTML → Markdown — `pumper_core::html_to_markdown(&html) -> String`
Strips scripts/nav/footer chrome and serializes the meaningful content as clean
Markdown. Use it to store readable snapshots or to shrink a page before feeding
it to the Claude engine.

---

## 7. Invariants — the golden rules for anyone editing this repo

1. **Don't break the dependency rule** ([§3](#the-dependency-rule-do-not-break-this)).
   Apps and engines see `core` only.
2. **`core` is a stable contract.** Changing a trait in `engine.rs`/`app.rs`
   ripples to every engine and app. Prefer *adding* (new trait, new optional
   field with `#[serde(default)]`) over changing existing signatures. If you must
   change one, update all impls and call sites in the same change and re-run the
   full test suite.
3. **Config keys stay optional & defaulted.** Every `config.rs` struct uses
   `#[serde(default)]`. New keys must have a sensible default so existing
   `config.toml` files keep working.
4. **Migrations are append-only.** Add `crates/core/migrations/000N_*.sql`; never
   edit a migration that has already run against `data/pumper.db`.
5. **Timestamps are fixed-width RFC3339-UTC** so SQL string comparison matches
   chronological order. Use the `ts()`/`parse_ts()` helpers in `storage.rs`; don't
   invent a new format.
6. **Job `result` must be JSON-serializable** (it's stored as text). Put large
   raw payloads on disk via `save_artifact`, not in the result blob.
7. **Respect the local-power posture** ([§2](#2-operating-principles--read-before-you-improve-anything)).
   Don't silently add auth, sandboxing, or permission prompts.
8. **Keep the worker non-blocking.** `run()` is async — never block the runtime
   with sync I/O or `std::thread::sleep`; use `tokio` equivalents.

---

## 8. Verification loop — do this before you finish

```powershell
cargo check                             # fast type-check of the whole workspace
cargo test --workspace                  # unit + integration tests (what CI runs)
cargo test --workspace -- --ignored     # the env-dependent tests (real Chrome, built wasm, timing)
cargo fmt --check                       # CI gate
cargo clippy --workspace --all-targets  # CI gate
cargo build -p pumper-server            # produce the binary
cargo run -p pumper-server --bin pumper # boot it; RUST_LOG=debug for verbose logs
```

**`--bin pumper` is required.** The `pumper-server` package ships three binaries
(`pumper`, `reindex`, `search-backfill`) and sets no `default-run`, so a bare
`cargo run -p pumper-server` fails with *"could not determine which binary to
run"*. The two maintenance binaries must be run with the **server stopped**.

The repo-root `justfile` is the canonical task runner and wraps every command
above — `just check`, `just test`, `just test-ignored`, `just lint`, `just fmt`,
`just fmt-check`, `just build`, `just run`, `just dev`, plus `just ci` (the whole
CI job), `just reindex`, `just search-backfill <scope>` and `just plugin <crate>`,
plus two **read-only** operator recipes that need the server *running* rather than
stopped: `just doctor` (store integrity report — findings each carry their
remediation, empty means healthy) and `just retention-preview [days]` (reclaimable
artifact bytes per app; deletes nothing). Both perform full scans, so run them on
demand. See [docs/features/datasets.md](docs/features/datasets.md).
Install once with `cargo install just`; `just --list` shows them all. Keep the
recipes and this section in sync.

**Iterate narrow; sweep wide once, at the end.** The commands above are the
*finishing* loop, not the inner one. While you are still changing code, run
`cargo test -p <crate>` (or `cargo test -p <crate> --test <file>`) for the crate
you are actually touching. A full `cargo test --workspace` links ~200 separate
test binaries, and cargo garbage-collects `target/` **never** — every run leaves
another generation of artifacts behind forever. Reaching for the whole workspace
on every edit is how this repo put 280.8 GB in `target/` in a single month
(measured 2026-08-26), against 0.28 GB of actual scraped data. Run the wide
sweep when you are finishing, which is what `just ci` is for.

`just disk` shows where the disk went, `just disk-prune` reclaims superseded
generations safely, and `just disk-check` — a rung of `just ci` — prunes for you
and only goes red when pruning could not bring `target/` back under its ceiling.

Then exercise your change against the running server:

```powershell
irm http://127.0.0.1:8088/apps                      # your app is listed?
$j = irm -Method Post http://127.0.0.1:8088/apps/<name>/jobs `
     -ContentType 'application/json' -Body '{"params":{…}}'
irm "http://127.0.0.1:8088/jobs/$($j.id)"           # poll to `succeeded`, inspect result
```

A change to a scraping app or engine **is not done until you've watched a real
job run through it** and produce the expected `result` / artifacts. Don't rely on
`cargo check` alone — the interesting failures are at runtime (selectors that
don't match, JSON the agent didn't format as asked, a site that needs the browser
engine instead of http).

---

## 9. Continuous development charter

**This codebase is explicitly meant to grow.** Multiple apps and agents on this
machine are authorized to extend it — add scraping use cases, harden the engines,
sharpen the queue, add features. You do not need permission to make it better
within the contracts above. You are encouraged to:

- **Add apps** whenever a new app on this machine needs something scraped — that's
  the designed-for case (Path B). Prefer a new app crate over bolting logic onto
  an existing one.
- **Harden engines** — better retry/rate-limit logic in `engine-http`, screenshot
  capture or stealth tuning in `engine-browser`, session-resume (`--resume`) or
  streaming in `engine-claude`. These live behind the `core` traits, so improving
  an engine upgrades every app at once.
- **Strengthen the platform** — the queue now has priorities + per-app fairness,
  DB-backed dynamic schedules, result webhooks, an HTTP cache, a per-domain
  governor, `/metrics`, and SSE. Still open: running-job cancellation via a
  `CancellationToken` on `AppContext`, richer dataset querying/filtering, proxy
  rotation, and screenshot capture in `engine-browser`.

**To keep parallel development safe:**

- **Additive-first.** New crate, new trait, new optional field beats a breaking
  change every time. Breaking `core` is the one move that can hurt other agents'
  in-flight work — do it deliberately, update every impl in the same change, and
  run `cargo test`.
- **Leave the repo green.** Land changes that `cargo check` + `cargo test` pass.
  If you add an engine capability, add a test for it.
- **Document as you go.** New app → give it a real `description()` with its param
  shape. New engine capability or config key → update the relevant reference here
  and in `README.md`. New invariant others must respect → add it to [§7](#7-invariants--the-golden-rules-for-anyone-editing-this-repo).
- **Keep this file true.** If you change a contract, update ONBOARDING.md in the
  same change. The next agent trusts it literally.

The bar is simple: **the codebase should be a little more capable and no less
correct after you touch it than before.** Build accordingly.

---

## 10. Data source catalog

`catalog/data-sources.toml` is the machine-readable registry of **every data
pipeline on this machine** — one `[[source]]` per source. It answers, at a glance
and without reading any app: which **markets** we cover, by what **mechanism**
(web `http`/`browser` vs `claude` LLM vs `bulk` download), how **often** (one-time /
on-demand vs a cron), how **fresh**, and how **trustworthy** (a 1–5 confidence).
Both humans and other agents read it to assess the state of the data pipelines;
`catalog/README.md` is the schema plus a rendered overview table.

**Rule (part of the Path B contract):** adding or changing a scraping app **must**
add or update its `[[source]]` entry in the same change. A source you have only
researched (no app yet) still gets an entry with `status = "planned"` and
`app = ""` — so the catalog doubles as the roadmap, and "live vs planned" stays honest.

**Standardized fields** (defined in the `data-sources.toml` header and
`catalog/README.md`): `id, app, market, name, url, category, engine, access,
cadence, cron, status, confidence, dataset, notes`. The three `category` values —
`open-calls` · `awarded-history` · `registry` — encode the core data-strategy
insight that open-call feeds are the scarce, decisive resource.

The format is deliberately **app-agnostic**: any other app on this machine can copy
the schema and keep its own `data-sources.toml`, so "what data do we have, how
fresh, by what mechanism" has one uniform, greppable answer everywhere.
