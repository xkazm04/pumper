# Pumper

Local-first scraping service. One Rust binary that exposes an HTTP API, runs a
durable job queue on SQLite, and scrapes through three pluggable engines:

| Engine | Crate | Use it for |
|---|---|---|
| `http` | `engine-http` | Server-rendered pages and APIs — reqwest + cookie jar, retries with backoff |
| `browser` | `engine-browser` | JS-heavy pages and logged-in sessions — headless Chrome (CDP) with a persistent profile |
| `claude` | `engine-claude` | Research-style scraping a crawler can't do — headless Claude Code CLI with WebSearch/WebFetch |

Designed to run **only on this machine**, so it deliberately trades security
for power: the API has no auth and permissive CORS (any local web app can call
it), the Claude CLI runs with permission prompts disabled, and the browser
profile keeps real login cookies on disk.

## Architecture

```
crates/
├─ core/             traits + models everything else plugs into
│    ScrapeApp       one scraping use case (name, schedule, run)
│    Engine traits   HttpClient / Browser / Researcher
│    Fetcher         tiered fetch with auto escalation (http→browser→claude)
│    Storage         SQLite job queue (WAL, priorities, retries, schedules)
│    Datasets        record store with change detection (dedup + monitoring)
│    HttpCache       content-addressed TTL response cache
│    Governor        per-domain politeness rate limiter
│    markdown        HTML → clean Markdown
│    Config          config.toml loader
│    extract         multi-core, SIMD extraction engine (rayon + simd-json)
│    simhash         near-duplicate detection (SimHash)
│    crawl           bounded-concurrency broad crawler (frontier + robots)
│    Plugins/Search  traits for the WASM host and full-text index
├─ engine-http/      reqwest + retries, fronted by cache + governor
├─ engine-browser/   chromiumoxide, lazy-launched persistent Chrome
├─ engine-claude/    `claude -p` subprocess with model/effort roles
├─ engine-wasm/      wasmtime plugin host — fuel + memory sandbox
├─ engine-search/    tantivy embedded full-text search index
├─ apps/             ← one crate per scraping use case
│  ├─ hackernews/    demo: fetch-and-parse + dedup via http engine
│  ├─ research/      demo: agentic research via claude engine (roles)
│  ├─ readable/      demo: URL → clean Markdown via the tiered fetcher
│  ├─ extractor/     demo: parallel rule-based extraction over many URLs
│  ├─ plugin/        demo: run a sandboxed WASM extractor over URLs
│  └─ crawl/         demo: high-concurrency broad crawl
└─ server/           axum API + worker pool + scheduler + webhooks + SSE + search
plugins-src/         example WASM plugins (Rust → wasm32), built separately
```

Dependency rule: **apps depend only on `core`** (plus parsing libs like
`scraper`). Engines also depend only on `core`. The server wires everything
together. That keeps every new use case a self-contained crate.

## Rust-specific capabilities

Features that lean on what Rust does well and Python can't (easily):

- **Multi-core, SIMD extraction** (`core::extract`, `extractor` app) — a
  declarative rule set (CSS / regex / JSON-pointer) compiled once and run over a
  batch of documents across **all cores** via rayon, with `simd-json` parsing.
  No GIL, no per-process pickling.
- **Sandboxed WASM plugins** (`engine-wasm`, `plugin` app) — run untrusted,
  hot-swappable `.wasm` extractors in-process with a CPU **fuel** budget and a
  memory cap. A runaway plugin traps instead of hanging the host — a guarantee
  Python's in-process code execution can't make.
- **Embedded full-text search + near-dup** (`engine-search`, `core::simhash`) —
  Tantivy (a Lucene-class engine) runs **in-process**, no Elasticsearch; SimHash
  flags near-duplicate records, not just exact changes.
- **High-concurrency broad crawler** (`core::crawl`, `crawl` app) — a bounded,
  deduplicated frontier feeds a concurrent fetch pool; page bodies stream to
  disk (memory bounded by concurrency, not crawl size), respecting robots.txt
  and the per-domain governor.

> Not built here: TLS/JA4 **fingerprint impersonation** (anti-bot fetching). It
> needs BoringSSL, which won't build on this ARM64 Windows box without a full
> C/C++ cross-toolchain. It builds cleanly on Linux/x64 (WSL or CI) — see the
> Roadmap.

## Run

```powershell
cargo run -p pumper-server
# listening on http://127.0.0.1:8088
```

Configuration lives in `config.toml` (all keys optional; see
`crates/core/src/config.rs` for defaults). `RUST_LOG=debug` for verbose logs.

## API

```powershell
# what's registered
irm http://127.0.0.1:8088/apps

# enqueue a scrape (body optional; params are app-specific)
irm -Method Post http://127.0.0.1:8088/apps/hackernews/jobs `
    -ContentType 'application/json' -Body '{"params": {"pages": 2}}'

# agentic research via Claude CLI
irm -Method Post http://127.0.0.1:8088/apps/research/jobs `
    -ContentType 'application/json' `
    -Body '{"params": {"query": "current state of Rust web scraping crates"}}'

# URL → clean Markdown via the tiered fetcher (http → browser → claude)
irm -Method Post http://127.0.0.1:8088/apps/readable/jobs `
    -ContentType 'application/json' `
    -Body '{"params": {"url": "https://example.com", "strategy": "auto"}}'

# poll job status / result
irm http://127.0.0.1:8088/jobs/<id>
irm 'http://127.0.0.1:8088/jobs?app=hackernews&status=succeeded&limit=10'

# cancel a queued job
irm -Method Delete http://127.0.0.1:8088/jobs/<id>

# observability
irm http://127.0.0.1:8088/metrics                       # Prometheus text
curl.exe -N http://127.0.0.1:8088/events                # live SSE of all jobs
curl.exe -N http://127.0.0.1:8088/jobs/<id>/stream      # one job, until terminal

# dynamic schedules (DB-backed; survives restart)
irm -Method Post http://127.0.0.1:8088/schedules -ContentType 'application/json' `
    -Body '{"app":"hackernews","cron":"0 0 */6 * * *","params":{"pages":2}}'
irm http://127.0.0.1:8088/schedules

# datasets (change-detected records) + near-duplicate detection
irm http://127.0.0.1:8088/apps/hackernews/datasets
irm 'http://127.0.0.1:8088/datasets/hackernews/stories?limit=20'
irm http://127.0.0.1:8088/datasets/hackernews/stories/export
irm 'http://127.0.0.1:8088/datasets/extractor/examples/duplicates?distance=8'

# parallel rule-based extraction over many URLs (multi-core + SIMD)
irm -Method Post http://127.0.0.1:8088/apps/extractor/jobs -ContentType 'application/json' `
    -Body '{"params":{"urls":["https://a","https://b"],"rules":{"title":{"type":"css","selector":"h1"},"stars":{"type":"json","pointer":"/stargazers_count"}}}}'

# run a sandboxed WASM plugin (fuel + memory limited)
irm http://127.0.0.1:8088/plugins                       # loaded modules
irm -Method Post http://127.0.0.1:8088/plugins/reload   # hot-swap from data/plugins
irm -Method Post http://127.0.0.1:8088/apps/plugin/jobs -ContentType 'application/json' `
    -Body '{"params":{"plugin":"title","urls":["https://example.com"]}}'

# high-concurrency broad crawl
irm -Method Post http://127.0.0.1:8088/apps/crawl/jobs -ContentType 'application/json' `
    -Body '{"params":{"seeds":["https://www.rust-lang.org/"],"max_pages":8,"max_depth":1,"concurrency":6}}'

# embedded full-text search across everything indexed from job results (BM25)
irm 'http://127.0.0.1:8088/search?q=rust&limit=10'
```

Enqueue body fields (all optional): `params` (JSON passed to the app),
`max_attempts` (default 1 — failed jobs retry with exponential backoff until
exhausted), `delay_secs` (delayed start), `priority` (higher runs sooner),
`callback_url` + `callback_secret` (POST the finished job here, HMAC-signed).

Job results are stored in SQLite (`data/pumper.db`); raw dumps written by apps
land in `data/artifacts/<app>/<job_id>/`.

## Adding a scraping use case

1. **Create the crate** `crates/apps/<name>/` (workspace globs pick it up):

   ```toml
   # crates/apps/myapp/Cargo.toml
   [package]
   name = "app-myapp"
   version.workspace = true
   edition.workspace = true

   [dependencies]
   pumper-core.workspace = true
   async-trait.workspace = true
   serde_json.workspace = true
   scraper.workspace = true   # if parsing HTML
   ```

2. **Implement `ScrapeApp`** — `hackernews` is the fetch-and-parse template,
   `research` the Claude-agent template:

   ```rust
   pub struct MyApp;

   #[async_trait]
   impl ScrapeApp for MyApp {
       fn name(&self) -> &'static str { "myapp" }
       fn schedule(&self) -> Option<&'static str> { Some("0 0 */6 * * *") } // optional
       async fn run(&self, ctx: AppContext) -> Result<Value> {
           let page = ctx.engines.browser.render(RenderRequest::new("https://…")).await?;
           // parse with `scraper`, save artifacts via ctx.save_artifact(...)
           Ok(json!({ "items": [] }))
       }
   }
   ```

3. **Register it** — add `app-myapp = { path = "crates/apps/myapp" }` to
   `[workspace.dependencies]`, add `app-myapp.workspace = true` to
   `crates/server/Cargo.toml`, and one line in `crates/server/src/registry.rs`.

Cron schedules use 6 fields with seconds: `sec min hour day month weekday`.

## Scraping behind logins

Set `headless = false` in `[browser]`, enqueue any browser-engine job, log in
to the target site in the Chrome window that opens, then flip back to
`headless = true`. Cookies persist in `data/browser-profile`.

## WASM plugins

Example plugins live in `plugins-src/` (Rust → `wasm32-unknown-unknown`). Build
and install one:

```powershell
cd plugins-src/title-extractor
cargo build --release --target wasm32-unknown-unknown
copy target\wasm32-unknown-unknown\release\title_extractor.wasm ..\..\data\plugins\title.wasm
# then: irm -Method Post http://127.0.0.1:8088/plugins/reload
```

A plugin exports `alloc(len)->ptr` and `extract(ptr,len)->u64` (output packed as
`ptr<<32 | len`, UTF-8 JSON). It runs with a CPU-fuel budget and memory cap and
has no ambient authority.

## Roadmap ideas

Delivered: tiered fetching, HTML→Markdown, dataset dedup/change-detection,
scheduled operations, Claude model/effort roles, queue priorities + per-app
fairness, result webhooks, HTTP caching, per-domain governor, `/metrics`, SSE,
Claude session resume; **multi-core SIMD extraction, sandboxed WASM plugins,
embedded full-text search + SimHash near-dup, and a high-concurrency crawler.**

Still open:

- **TLS/JA4 fingerprint impersonation** (`wreq`/BoringSSL) — blocked on this
  ARM64 Windows box (no C/C++ cross-toolchain); builds on Linux/x64 (WSL / CI).
- Per-page streaming download in the crawler (currently bounded by concurrency).
- Screenshot capture in `engine-browser`; cancellation of running jobs.
- Proxy / user-agent rotation for the http and browser engines.
