# explorer overlay - pumper

Project specifics for the `/explorer` lane skill (`ai-registry/skills/explorer`, linked at
`.claude/skills/explorer`). The lane body is the method and is repo-agnostic; everything
below is what pumper is. Extracted 2026-08-24 from the project-owned copy the lane replaced.

pumper is a **Rust workspace** — axum job server + SQLite queue + tiered fetch engines +
`ScrapeApp` plugin crates. It shares the repo vault with `/architect`, `/perfect` and
`/research` but owns its own `Explorer/` subtree.

## Vault

Resolve first hit; create the second if neither exists.

```bash
VAULT=""
for v in "C:/Users/mkdol/Documents/Obsidian/pumper" "C:/Users/mkdol/dolla/pumper/.perfect"; do
  [ -d "$v" ] && VAULT="$v" && break
done
[ -n "$VAULT" ] || { mkdir -p "C:/Users/mkdol/dolla/pumper/.perfect" && VAULT="C:/Users/mkdol/dolla/pumper/.perfect"; }
```

`.perfect/` is Obsidian-openable and **committed by default**, so loop state travels with the
repo. `Lessons/` and `Patterns/` are shared with the sibling skills - create the dir if
missing, never recreate an existing file, and append with `Edit`, never `Write`.

## Area taxonomy

The taxonomy source is repo-root `context-map.json` (7 groups / 22 contexts at the time of
writing; `filePaths` per context is the file authority). Menu:

```
  2. runtime      - Scraping Runtime Core (tiered fetcher, politeness, app/job model, capability traits)
  3. extraction   - Data Extraction & Storage (broad crawler, declarative extraction, dataset store)
  4. engines      - Scraping Engines (search index, WASM sandbox, http/browser/claude engines)
  5. server       - Job Server & API (config/catalog, events/webhooks, worker/cron, datahub, registry, routes)
  6. grants       - Public Funding & Grants Apps (EU/regulatory watchers, US grant opportunities)
  7. economic     - Economic & Labor Market Data Apps (US wages/tax/valuation, MPSV, Census density)
  8. content      - Content & Research Apps (extraction/crawl/api-watch, web research & readable)
  9. clients      - clients/typescript (the SDK) and its contract with the API
```

Option 9 is deliberately **outside** the context map - a real area with its own npm toolchain;
take its file list from `clients/typescript/src/**`. A free-text hint resolves by group name,
exact context name, path fragment, or **crate name** (contexts whose `filePaths` sit under it).

Cheap staleness cross-check: if `crates/apps/` holds a crate that appears in **no** context's
`filePaths`, the map is definitely stale - say so.

## Categories

`quality | dx | perf | bug | sec | docs | tests`

**No `ui` / `i18n` / `a11y`.** pumper has no user interface. The only surfaces are the HTTP
API, the TypeScript SDK, and CLI/log output; presentation-shaped concerns land under `dx`
(SDK/API ergonomics) or `docs`.

## Reference files (always loaded)

- `context-map.json` (repo root) - the area taxonomy and the file->context authority.
- `CLAUDE.md` (repo root) - the commands table (`just` <-> raw cargo), the architecture map,
  the dependency rule, the four-step app-adding contract. Shortest true summary of the repo.
- `MEMORY.md` (repo root) - the index into `.perfect/` durable state plus the expensive-to-
  rediscover invariants. Read at session start; it points at `.perfect/Architect/backlog.md`
  so a sweep item does not duplicate queued structural work.
- `docs/harness/harness-learnings.md` - structural facts, conventions, pattern catalogue.
  Read before proposing any change that spans crates.
- `.claude/CLAUDE.md` - session rules (context-map discipline, same-session doc sync,
  "bug fixes ship as extracted, tested functions").
- `docs/features/README.md` + the relevant `docs/features/*.md` - the implemented-product
  surface for the swept area. Faster than a wide grep.
- `docs/deployment.md` - the run story (local-first operation, persistent state layout, auth
  posture). Load for any `sec`, `docs` or ops-shaped sweep before calling something a gap.
- `ONBOARDING.md` sections 2 (operating principles) and 7 (invariants) - the deliberate
  design trades. Do not surface them as findings (see Exclusions).

## Gates / validation commands

The repo-root `justfile` is the canonical task runner (`cargo install just`, then
`just --list`). The raw-cargo form is what CI invokes. Everything runs **from the repo root**
- the `.env` loader and the default `config.toml` path are both CWD-relative.

| `just` | raw cargo | what it is |
|---|---|---|
| `just check` | `cargo check --workspace` | fast type-check; always, minimum bar |
| `just fmt-check` / `just fmt` | `cargo fmt --check` / `cargo fmt` | formatting gate (CI); always before committing Rust |
| `just lint` | `cargo clippy --workspace --all-targets` | lint gate (CI) |
| `just test` | `cargo test --workspace` | unit + integration (CI); whenever behavior changed |
| `just test-ignored` | `cargo test --workspace -- --ignored` | env-dependent (real Chrome, built wasm, timing) - not in CI |
| `just ci` | fmt-check + lint + test | the whole CI job; prefer it when a change spans crates |
| `just build` | `cargo build -p pumper-server` | build the binaries |
| `just run` / `just dev` | `cargo run -p pumper-server --bin pumper` (+/- `RUST_LOG=debug`) | boot the server -> `http://127.0.0.1:8088` |

**`--bin pumper` is required.** `pumper-server` ships three binaries (`pumper`, `reindex`,
`search-backfill`) and sets no `default-run`, so a bare `cargo run -p pumper-server` fails
with *"could not determine which binary to run"*. The two maintenance binaries
(`just reindex`, `just search-backfill <scope>`) must run with the **server stopped**.

`clients/typescript` only: `npm run build && npm test` inside that directory.

If a change alters a command, update the `justfile` in the same commit - it is the surface
every other session reads.

## Exclusions (never spend a slot on these)

**The deliberate local-first trades.** `ONBOARDING.md` section 2 declares these intentional,
not bugs: no API auth, permissive CORS, `--dangerously-skip-permissions` on the Claude CLI,
real login cookies on disk in `data/browser-profile`, non-2xx HTTP bodies returned rather
than raised. Do NOT surface "add auth to the API" or "harden the browser profile". A `sec`
item must be a defect *within* that model - a path traversal in an artifact path, a secret
written into a job result, an unbounded allocation from a remote body - never a
re-litigation of the model.

**Context-map bookkeeping.** "Context X's `filePaths` is missing file Y" is a map refresh,
not a sweep item. Fix it silently as part of an accepted item that touches that context.

## What to look for, by category

**quality** - dead code, `pub` items nobody outside the crate uses; duplicated logic across
app crates (3+ near-identical blocks) that belongs in `core` or a `*-common` crate
(`grants-common`, `census-common`, `trades-common` are the existing homes); leaking
abstraction across the `apps -> core <- engines` boundary; "what" comments; stale
commented-out code.

**dx** - test boilerplate that could be a fixture or a `#[cfg(test)]` helper in `core`;
repeated `map_err(|e| Error::...)` that should use an existing `From` impl or a helper in
`crates/core/src/error.rs`; app params parsed ad-hoc from `serde_json::Value` instead of a
typed `#[derive(Deserialize)]` struct; a `ScrapeApp::description()` that does not document
its param shape (`ONBOARDING.md` section 9); errors returned without enough context to tell
which URL / record failed; SDK ergonomics in `clients/typescript` (untyped responses,
missing exports, drift from the route surface).

**perf** - blocking work inside an async task (`std::fs`, `reqwest::blocking`, heavy parse)
without `spawn_blocking`; per-record DB round-trips where the dataset store offers a batch
upsert; rebuilding a `Regex` / `Selector` / rule set inside a loop (`core::extract` compiles
rules once by design - deviations are findings); whole-body `String` allocation where a
streaming read would do (the crawler streams bodies to disk by design); unbounded
concurrency (`join_all` over an unbounded frontier instead of a bounded pool/semaphore);
cache and governor bypasses (an engine call skipping `HttpCache` or the per-domain
`Governor`).

**bug** - races in the queue/worker path (claim-then-write without a transaction, missing
crash-recovery requeue); `unwrap()` on remote data, off-by-one on pagination cursors;
cancellation/timeout gaps (a job outliving its wall-clock timeout because the inner future
is not `select!`ed); retry logic that retries non-idempotent work or ignores `max_attempts`;
silently swallowed errors (`let _ =`, `.ok()` on a fallible write, an empty `Err(_) => {}`);
change-detection correctness - a dataset upsert whose key does not identify the record.

**sec** - path traversal into the artifact or plugin dir from remote-controlled input;
remote input interpolated into SQL instead of a bound `sqlx` parameter; secrets (API keys
from `config.toml`, cookies) logged via `tracing` or echoed into a job result / webhook
payload; WASM plugin execution without the fuel + memory caps `engine-wasm` promises;
webhook HMAC signing/verification gaps; unbounded remote input (no body size cap, no
redirect limit, a zip/JSON bomb).

**docs** - a route, param, config key, dataset shape or trigger contract in code but not in
the matching `docs/features/*.md`; `catalog/data-sources.toml` out of sync with
`crates/server/src/registry.rs`; `ONBOARDING.md` / `README.md` claiming a capability the
code no longer has; a config key in `crates/core/src/config.rs` with no mention in
`config.toml`'s commented reference.

**tests** - a bug fixed inline in a `run()` body with no extracted, named function and no
test (the exact anti-pattern `.claude/CLAUDE.md` names - surfacing one is always valid); a
convention asserted in prose but not enforced by an inventory test (the EXPECTED-diff idiom
in `crates/server/src/routes/mod.rs` is the canonical shape); `#[ignore]`d tests that have
silently stopped compiling; an engine capability added without a test (ONBOARDING section 9).

## Item fields

Replace the lane's `i18n_impact` with:

```yaml
doc_impact: "<none | docs/features/<file>.md needs the same-session update | catalog/data-sources.toml>"
```

`.claude/CLAUDE.md` requires that any user/API-visible change updates the coupled feature doc
**in the same session**; `scripts/docs/feature-doc-map.json` is the source->doc map. Internal-
only changes are `none` - say so explicitly so the Stop hook can be dismissed in one sentence.

## Severity rubric

- **critical** - data corruption in the dataset store; a queue state machine that can lose or
  double-run jobs; a remote-input path that can write outside `data/`.
- **high** - wrong behavior on the golden path (a scrape silently returns empty), a broken
  common edge case, regression risk if left.
- **medium** - paper cut, confusing API, small perf hit, latent risk.
- **low** - polish, nice-to-have, taste-level.

## Rust changes - non-negotiable

- **Honor the dependency rule.** `apps` depend on `core` only (plus leaf parsing libs);
  `engines` depend on `core` only; only `server` depends on everything. If a fix wants
  `pumper-engine-*` in an app's `Cargo.toml`, it wants a trait from `core` via
  `AppContext.engines` instead. This is the one rule that breaks other agents' in-flight work.
- **Bug fixes ship as extracted, tested functions.** Extract the predicate/transform into a
  named function, add a test named after the anti-pattern it defends (`x_not_y` style), then
  wire it into the call site. A fix buried inline in a `run()` body does not count as done.
- **Additive-first.** A new optional field or a trait method with a default beats a breaking
  `core` change. If `core` must break, update every impl in the same change and run `just test`.
- **Doc sync in the same session** (see `doc_impact` above).
- **Keep `context-map.json` true** - a file added or moved between contexts updates
  `filePaths` in the same commit.

## Runtime verification

If a change alters what a scrape actually does (an app's parse/selectors, an engine's fetch
behavior, tiering/escalation, the worker/scheduler), state explicitly that you have NOT run
it, OR run it:

```bash
just run
# then, in another shell:
curl -s -X POST http://127.0.0.1:8088/apps/<name>/jobs \
     -H 'content-type: application/json' -d '{"params":{}}'
curl -s http://127.0.0.1:8088/jobs/<id>          # poll to succeeded, inspect result
```

`ONBOARDING.md` section 8 is explicit: a change to a scraping app or engine **is not done
until you have watched a real job run through it**. `just check` alone proves nothing - the
interesting failures (selectors that stopped matching, a site that now needs the browser
tier) are runtime-only. If the site is unreachable or the run would burn Claude budget, say
so plainly rather than implying verification.

## Parallel-session conventions

pumper has no `.claude/active-runs.md` ledger; the vault `$VAULT/Explorer/state.md` claim IS
the coordination surface. The checkout **is** shared with concurrent CLI sessions
(`/architect`, `/perfect`, `/research`, `/tiger`), so:

- Before scanning, run `git status --porcelain -- <area paths>` and note any file in the area
  that already carries uncommitted work.
- Commit prefix `explorer:`. Stage only explicit paths in ONE bash invocation; never
  `git add -A` / `.` / `-u`; never `git commit --amend` (a sibling may already have built on
  your commit); never `git stash` / `reset --hard` / `clean -f`.
- Expect `HEAD` to advance mid-run. On `index.lock`, wait 3-10s and retry up to 6 times.
- An `architect:` or `perfect:` commit touching your area during the Phase 4d git-log dedupe
  is a strong signal the item is already handled.

## Sampling notes

`crates/core/src/app.rs`, `crates/core/src/storage.rs` and `crates/server/src/routes/*` are
the usual >500-line offenders - read with offset/limit. `crates/apps/*` are frequently 1-3
files and legitimately yield 3-5 items; widening to the sibling app in the same group is the
intended move. App crates are also the cheap wins: small, numerous, written by many sessions,
so they carry most of the duplication and most of the untyped-params debt.
