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

Start it (from this directory): `cargo run -p pumper-server` → listens on
`http://127.0.0.1:8088` (configurable in `config.toml`).

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
    fn default_params(&self) -> Value { json!({}) }       // used for scheduled + body-less runs
    async fn run(&self, ctx: AppContext) -> Result<Value>;// returns JSON stored as the job result
}
```

Minimal implementation:

```rust
use async_trait::async_trait;
use pumper_core::{AppContext, RenderRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct MyApp;

#[async_trait]
impl ScrapeApp for MyApp {
    fn name(&self) -> &'static str { "myapp" }
    fn description(&self) -> &'static str { "What it scrapes. Params: {\"url\": \"…\"}" }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx.require_str("url")?;                    // typed access to ctx.params
        let page = ctx.engines.browser.render(RenderRequest::new(url)).await?;
        ctx.save_artifact("page.html", page.html.as_bytes()).await?;
        // …parse page.html with `scraper`, build your output…
        Ok(json!({ "url": url, "items": [] }))
    }
}
```

**`AppContext` gives you** (`crates/core/src/app.rs`):
- `ctx.params: Value` — the enqueue `params`. `ctx.require_str("k")` for a
  required string (errors cleanly if missing).
- `ctx.engines` — `.http`, `.browser`, `.claude`, and `.fetch` (the tiered
  fetcher). See §6.
- `ctx.upsert(dataset, key, &value).await` → `ChangeKind` and
  `ctx.upsert_many(dataset, &items).await` → `UpsertSummary{new,changed,unchanged}`
  — dedup + change detection, scoped to this app.
- `ctx.save_artifact(name, bytes).await` — writes under this job's artifact dir.
- `ctx.app` (this app's name) and `ctx.job_id` (UUID) for correlation.

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

That's the whole integration. `cargo run -p pumper-server`, confirm your app
appears in `GET /apps`, enqueue a job, verify the result.

**Cron note:** `schedule()` returns a 6-field expression *with seconds*:
`sec min hour day month weekday`. `"0 0 */6 * * *"` = every 6 hours. Scheduled
runs use `default_params()`, so make sure those params are sufficient (e.g. the
`research` app needs a `query` and therefore should not be scheduled without one).

---

## 6. Engine capabilities reference

You call these through `ctx.engines.*`. Signatures live in
`crates/core/src/engine.rs`. All return `pumper_core::Result<_>`.

### `ctx.engines.http` — `HttpClient::fetch(HttpRequest) -> HttpResponse`
```rust
let res = ctx.engines.http.fetch(HttpRequest::get("https://api.example.com")).await?;
// HttpRequest also supports .method (GET/POST), .headers, .body
// HttpResponse { status, headers, body: String, final_url }; res.is_success()
```
Retries `429/502/503/504` with backoff automatically. Shares a cookie jar across
calls within the process.

### `ctx.engines.browser` — `Browser::render(RenderRequest) -> RenderedPage`
```rust
let mut req = RenderRequest::new("https://spa.example.com");
req.wait_for_selector = Some(".results".into());  // wait for an element
req.extra_wait_ms = Some(1500);                    // extra settle time
req.evaluate = Some("document.title".into());      // JS → RenderedPage.evaluated (JSON)
let page = ctx.engines.browser.render(req).await?; // RenderedPage { html, final_url, evaluated }
```
Chrome launches lazily on first use and stays warm. **Logged-in scraping:** set
`headless = false` in `[browser]`, run a job, log in to the site in the window
that opens, then set `headless = true` — cookies persist in `data/browser-profile`.

### `ctx.engines.claude` — `Researcher::research(ResearchRequest) -> ResearchOutput`
```rust
let mut req = ResearchRequest::new("Research X. Reply with ONLY JSON: {…schema…}")
    .with_role("compose");          // Opus @ xhigh; or "research" = Sonnet @ high
req.max_turns = Some(25);
req.effort = Some("max".into());    // per-job override of the role's effort
req.resume_session = Some(prev_id); // multi-step: continue a prior CLI session
let out = ctx.engines.claude.research(req).await?;
// ResearchOutput { text, json: Option<Value>, cost_usd, duration_ms, num_turns, session_id }
```
Model + reasoning are chosen per job: pass a `role` (presets in `[claude.roles]`),
or set `model` / `effort` (`low|medium|high|xhigh|max`) directly — request fields
override the role, which overrides the config default. `out.json` is auto-populated
when the reply parses as JSON (fenced and prose-embedded JSON are both extracted;
`json_schema` uses the CLI's validated `--json-schema` output). **Instruct the
agent to return strict JSON** for structured output.

### `ctx.engines.fetch` — `Fetcher::fetch(FetchRequest) -> FetchOutcome`
```rust
let mut req = FetchRequest::new("https://…");
req.strategy = FetchStrategy::AutoWithResearch; // http → browser → claude
req.to_markdown = true;
let out = ctx.engines.fetch.fetch(req).await?;
// FetchOutcome { engine: "http"|"browser"|"claude", html, markdown, text, escalations, .. }
```
The fetcher starts on the cheapest tier and escalates when the extracted text is
below `min_content_chars` (default 250). `escalations` records why each hop
happened. `FetchStrategy` = `Http | Browser | Auto | AutoWithResearch`.

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
cargo check                       # fast type-check of the whole workspace
cargo test                        # unit + integration tests (browser test launches real Chrome)
cargo build -p pumper-server      # produce the binary
cargo run  -p pumper-server       # boot it; RUST_LOG=debug for verbose logs
```

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
