# Deployment & operations

How pumper is built, run, and kept alive. This describes what IS — the local-first
posture is a deliberate design decision (`ONBOARDING.md` §2), not an unfinished
migration to containers.

## The deployable artifact

One native binary, `pumper`, built from the `pumper-server` package:

```
cargo build -p pumper-server        # target/debug/pumper (+ reindex, search-backfill)
cargo build --release -p pumper-server
```

The package ships **three** binaries — `pumper` (the server, `src/main.rs`),
`reindex` and `search-backfill` (one-shot maintenance, run with the server
stopped). There is no `default-run`, so `cargo run -p pumper-server` is ambiguous;
name the binary: `cargo run -p pumper-server --bin pumper`. The repo-root
`justfile` wraps every command here (`just run`, `just ci`, `just reindex`, …).

The binary is a **long-running stateful process**: an axum HTTP API, a worker pool
claiming from a SQLite job queue, a cron scheduler, an SSE event bus, a Tantivy
index, a WASM host, and a lazily-launched Chrome. It is not a request-scoped
service and does not scale horizontally — two processes over one `data/` directory
would fight over the SQLite writer and the Tantivy exclusive writer lock.

## Local-first run story

```
cargo run -p pumper-server --bin pumper     # or: just run
# listening on http://127.0.0.1:8088
```

Run it **from the repo root** — `config.toml`, `catalog/data-sources.toml`, `.env`
and `data/` are all resolved relative to the working directory unless overridden.

Configuration is `config.toml` (every key optional, defaults compiled in — see
`crates/core/src/config.rs`); the bind address is `[server] host` / `[server] port`,
defaulting to `127.0.0.1:8088`. Migrations in `crates/core/migrations/` run
automatically at startup, and `recover_stuck` re-queues jobs orphaned by a crash.

Shutdown is graceful: Ctrl-C / SIGTERM (on Windows also Ctrl-Break, console close,
system shutdown) stops new claims, drains in-flight jobs for
`worker.shutdown_drain_secs`, then re-queues whatever is still running.

### Not containerized — on purpose

- **`engine-browser` drives a real Chrome** at `[browser] chrome_executable`, with
  a persistent user-data-dir. Headful login (`headless = false`) is part of the
  documented workflow.
- **`engine-claude` shells out to the `claude` CLI** on the host, with permission
  prompts disabled.
- **The state is a single SQLite file in WAL mode.** It must not be replicated,
  shared, or written by two processes.

A container would have to bind-mount all of it and still not get a browser you can
log in through. Reproducibility comes from the pinned `Cargo.lock` and the CI job,
not from an image.

## Persistent state — what must survive a restart or a machine move

Everything lives under `data/`, which is **gitignored**. Back it up as a unit,
with the server stopped (WAL files are only meaningful alongside their database).

| Path | Config key | What it holds | Loss impact |
| --- | --- | --- | --- |
| `data/pumper.db` (+ `-wal`, `-shm`) | `[storage] database_path` | Job queue, datasets + revision history, schedules, triggers, watches, cost ledger, HTTP cache, extraction-health rows | **Total.** This is the product's accrued value. |
| `data/browser-profile` | `[browser] user_data_dir` | Chrome profile with **real login cookies** for sites scraped behind a login | Every logged-in session must be re-established by hand (headful login). |
| `data/plugins` | `[plugins] dir` | Operator-supplied `.wasm` extractor modules, loaded at boot and by `POST /plugins/reload` | Plugin apps fail until the modules are rebuilt from `plugins-src/`. |
| `data/profiles` | `[fetcher] profiles_dir` | Session vault: per-profile `cookies.json` + `browser/` user-data-dirs | Named login profiles must be re-established. |
| `data/artifacts` | `[storage] artifacts_dir` | Per-job raw dumps (`<app>/<job_id>/`) | Job results in the DB survive; raw payloads do not. |
| `data/search-index` | `[search] dir` | Tantivy full-text index | Derived — rebuild with `search-backfill`. |

The two irreplaceable entries are `data/pumper.db` and `data/browser-profile`.
`data/plugins` is operator-supplied and rebuildable from source but not from the
DB. `data/search-index` is the only fully derived directory.

## Environment variables

Names only — values never live in the repo. See [`.env.example`](../.env.example)
for the annotated template with the exact code reference for each. Set them per
environment: local `.env` / CI secrets / host dashboard.

| Variable | Purpose |
| --- | --- |
| `CENSUS_API_KEY` | Census Bureau API key. Declared as a `Requirement::Env` by the `census-density` / `census-nonemp` apps, so `GET /apps` reports them not-ready without it. |
| `DATAHUB_TOKEN` | Bearer token for the DataHub GMS emitter, when `[datahub] token` is unset. |
| `PUMPER_CONFIG` | Override the config path (default `config.toml`). |
| `PUMPER_CATALOG` | Override the catalog path (default `catalog/data-sources.toml`). |
| `PUMPER_BUILD_ID` | Build identity stamped on extraction-health run rows **and reused as the Sentry release**, so both name the same build; set to the commit sha in CI. Defaults to the crate version. |
| `RUST_LOG` | `tracing` filter directive. Defaults to `info`. |
| `SENTRY_DSN` | Sentry DSN for error reporting. **Unset/blank ⇒ reporting off** and the process boots identically; a malformed value logs one warning at boot and stays off. Errors only (`traces_sample_rate = 0.0`, `send_default_pii = false`). |
| `SENTRY_ENVIRONMENT` | Deployment tag on reported events (`production`, `staging`, …). Defaults to `local`. |

`.env` loading is **hand-rolled and non-clobbering** (`load_dotenv` in
`crates/server/src/main.rs`): plain `KEY=VALUE` lines, `#` comments skipped,
surrounding quotes stripped, and a variable already present in the process
environment is never overwritten. Only the `pumper` binary loads it.

## Auth posture

**By default there is no inbound authentication on any route** — no API key, no
bearer token, no session. That default is unchanged and deliberate for a
local-first node, but it is now a *setting* rather than a fact about the server:

```toml
[auth]
mode = "keys"   # default: "open"
```

flips on scoped API keys (**principals**) with per-principal spend ceilings,
request throttles and a durable audit ledger in front of every route except
`/health`, `/metrics` and `/openapi.json`. `mode = "open"` — the default, and
what an absent `[auth]` section means — consults no table, reads no header and
throttles nothing, so a node that never edits its config behaves byte for byte
as it did before. Full surface, scope vocabulary, refusal codes and
bootstrapping: **[features/auth.md](features/auth.md)**.

Create the first principal while `mode = "open"`, then flip the key and restart:
minting a principal requires the `admin` scope, and there is deliberately no
bootstrap escape hatch.

**Everything below describes `mode = "open"`.** In `open` mode the surface is
**fully mutating**, not read-only, and unauthenticated callers can:

- enqueue and cancel/reset/retry jobs (`POST /apps/{name}/jobs`, `DELETE /jobs/{id}`,
  `POST /jobs/{id}/reset`, `POST /jobs/retry`) — which means driving the browser
  and the Claude CLI, and spending real money through the Claude engine;
- create, disable and delete cron schedules, triggers, and dataset watches
  (`POST/DELETE /schedules`, `/triggers`, `/watches`) — including outbound webhook
  targets;
- **hot-load operator-supplied WASM from disk** (`POST /plugins/reload` →
  `crates/server/src/routes/runtime.rs`), which rescans `data/plugins` and swaps in
  whatever `.wasm` modules are there;
- delete search indexes and saved searches, and override source state
  (`POST /sources/{id}/state` — the only exit from quarantine);
- read every stored record and export whole datasets.

The plugin sandbox (wasmtime fuel budget + memory cap, no ambient authority) bounds
what a *module* can do; it does not authenticate the *caller* who triggers the
reload — `[auth] mode = "keys"` is what does, and under it `POST /plugins/reload`
requires the `admin` scope like every other mutation.

Inbound bodies are bounded — **1 MiB on every route, 8 MiB on `POST /extract/preview`**,
over-limit ⇒ `413` before the handler runs (`docs/features/http-api.md` →
"Request body limits"). That caps how much memory one unauthenticated request can
make the process buffer; it is not a substitute for auth or rate limiting — for
those, see [features/auth.md](features/auth.md).

**This is defensible only while the bind stays on loopback.** The entire safety
argument is `[server] host = "127.0.0.1"` — the network, not the application, is
the access control.

**Precondition, stated plainly: binding to `0.0.0.0` (or any routable interface)
without first adding authentication is unsafe.** It would expose an unauthenticated,
money-spending, code-loading, data-exporting API to the network. If a remote
consumer is needed, terminate it in an authenticating reverse proxy that keeps the
pumper listener on loopback, or add auth to the server first.

### Remote fetch fabric (`[remote]`) — the precondition nobody wrote down

The distributed fetch fabric (`POST /fetch-proxy`, `crates/engine-remote`) is the
one feature that **contradicts the loopback argument above**, and until now nothing
said so — not `config.toml`, not `RemoteConfig`, not the route, not this file.

A peer has to be reachable by its coordinator. `[remote] nodes` therefore holds
routable addresses (the shipped example is `http://10.0.0.2:8088`), which means
every node in the cluster must bind off loopback — and binding off loopback
exposes *every other route on that node*: enqueue jobs that spend Claude money,
`POST /plugins/reload` to load WASM off disk, export every dataset. **The
`[remote]` shared secret authenticates `/fetch-proxy` and nothing else.** There is
no API-key auth on the rest of the surface and adding one is deliberately parked.

**So: enabling `[remote]` is a decision to put an unauthenticated pumper API on a
network, and it is only safe if you add the access control yourself, at the
network layer.** Pick one and actually do it:

| control | what to do |
| --- | --- |
| host firewall | bind `[server] host` to the cluster-facing interface and allow inbound `8088` **only** from the other nodes' addresses (`ufw allow from <peer> to any port 8088`, a cloud security group, or equivalent). The default-deny rule is the one doing the work |
| private overlay | put the nodes on a WireGuard / Tailscale / VPC-private network and bind to *that* interface only, never to `0.0.0.0` |
| authenticating reverse proxy | keep pumper on `127.0.0.1`, front it with nginx/Caddy requiring mTLS or an auth header, and point `[remote] nodes` at the proxy |

"It's on a private subnet" is only a control if something enforces it. A cloud
instance with a public IP and an open security group is not a private subnet.

Two guardrails ship *inside* the app; neither replaces the network control:

- **Target policy.** `/fetch-proxy` refuses to fetch loopback, link-local
  (incl. `169.254.169.254`, the cloud metadata service), RFC-1918 private and
  CGNAT addresses on a peer's behalf, plus any non-`http(s)` scheme — so holding
  the cluster secret does not by itself amount to driving each node's own API and
  LAN. `[remote] allow_private_targets = true` opts a deliberate LAN-scraping
  cluster back in (addresses only; the scheme refusal is not opt-outable). The
  predicate is pure and blocks every WHATWG spelling of an address literal
  (`127.0.0.1`, `127.1`, `2130706433`, `0x7f.0.0.1`, `[::ffff:127.0.0.1]`); what
  it does **not** catch is a *hostname that resolves* into a private range.
- **Profiled fetches never leave the coordinator.** A session profile is a cookie
  jar on one node's disk (`<profiles_dir>/<name>/cookies.json`) and nothing
  replicates it. The coordinator keeps profiled fetches local, and a node asked
  to serve one it does not hold answers `422` instead of fetching through an
  empty jar and returning the logged-out page with a `200`. The `422` costs the
  coordinator a fallback fetch — the right trade against silently storing a login
  wall as a dataset revision.

### Elastic executor plane (`[executors]`) — the same precondition, one direction further

N18 lets extra pumper processes drain the coordinator's queue: `pumper --executor
--coordinator <url>`. It inherits the `[remote]` warning above **and sharpens it**,
because an executor claims whole jobs rather than proxying one fetch.

What changes, and what does not:

- **Direction.** Executors dial *out*. They need no ingress, no port and no
  address of their own — which is the point: an executor can live behind NAT, on
  a laptop, or in a cheap VPS with a different IP and a real Chrome. Only the
  **coordinator** has to be reachable, so this adds exactly one exposed node, not
  N.
- **The exposure is the same one `[remote]` documents.** The coordinator must
  bind off loopback for executors to reach it, and `[executors] secret`
  authenticates the six executor routes **and nothing else**. Every control in
  the table above applies unchanged — host firewall, private overlay, or an
  authenticating reverse proxy. Prefer this one over `[remote]`'s network shape
  where you can: with `[auth] mode = "keys"` the executor routes additionally
  require an `admin` principal, so the plane is the one cross-host surface that
  can be put behind real authentication today.
- **What crosses the wire.** Job params and results in both directions, plus
  checkpoints and progress snapshots. Whatever an app puts in its params
  (credentials in a `params` object, say) reaches every executor allowed to claim
  that app. There is no TLS of pumper's own — put the coordinator behind a
  proxy that terminates it if the link is not already private.
- **What does not cross.** Session profiles (cookie jars are per-node disk, same
  as the fabric), the dataset store (an executor's `datasets` handle refuses —
  only result-only apps are eligible), and the queue itself.

**Executor state.** An executor keeps its own `[storage] database_path` — give it
one, do not point two processes at the same SQLite file. What lives there is
process-local derived state only (cost ledger, HTTP cache, host weather, recipes,
research cache) plus the artifact tree of the jobs it ran. Losing it costs cache
misses and the artifacts of past runs, never a record; it is **not** part of the
"must survive a machine move" set above.

**Operating it.** `GET /executors` on the coordinator is the fleet view (`busy` /
`idle` / `offline` per executor); `/metrics` carries `pumper_executors{state}`
while the plane is enabled. An executor that dies needs no intervention: its lease
goes stale, the existing reaper re-queues the job with its checkpoint, and the
dead process's late report is refused by the `(status, attempts, executor_id)`
fence. Full surface and the v1 boundaries:
[docs/features/runtime.md § Elastic executor plane](features/runtime.md#elastic-executor-plane-executors).

### CORS

CORS is **off by default** (`[server] cors_allowed_origins` empty → no CORS layer
is installed, so browsers enforce same-origin). The rationale is in the code at
`crates/server/src/routes/mod.rs` (the `router` builder): a permissive allow-all on an
unauthenticated, mutating, data-bearing API would let any site the operator merely
*visits* drive it cross-origin, and DNS-rebinding defeats the "it's only localhost"
assumption. A trusted local UI opts in by listing its exact origin (e.g.
`http://localhost:5173`); when the list is non-empty the layer allows any method
and any header for those origins.

## CI

`.github/workflows/ci.yml` runs on ubuntu-latest for pushes to `master` and all
PRs: `cargo fmt --check`, `cargo clippy --workspace --all-targets`,
`cargo test --workspace`. Environment-dependent tests (real Chrome, built wasm
artifacts, wall-clock timing) are `#[ignore]d` and run locally with
`cargo test --workspace -- --ignored` (`just test-ignored`). CI does not build or
publish an artifact — there is no release pipeline, by design.

Reproduce the full job locally with `just ci`.

## Known gaps

- No auth, no rate limiting, no TLS on the listener (see **Auth posture**).
- No release/packaging pipeline and no versioned artifact; deployment is
  "build from the checkout on the machine that will run it".
- No supervisor/service unit is checked in — the process is started by hand.
- Single-process only: no story for running two instances against one `data/`.
- Backup of `data/` is manual and unscripted.
