# research overlay - pumper

Project specifics for the `/research` lane skill (`ai-registry/skills/research`, linked at
`.claude/skills/research`). The lane body is the method; everything below is what pumper is.
Extracted 2026-08-24 from the project-owned copy the lane replaced.

pumper is a **Rust workspace** (axum job server + SQLite queue + tiered fetch engines +
`ScrapeApp` plugin crates), so relevance scoring runs against `context-map.json`, the buckets
map to this repo's real extension seams (`ONBOARDING.md` Path B), and validation is
cargo-based. It shares the repo vault with `/architect`, `/perfect` and `/explorer`.

## Focus hint

`code` / `apps` / `sources` / `all` (default `all`).

## Vault

```bash
VAULT=""
for v in "C:/Users/mkdol/Documents/Obsidian/pumper" "C:/Users/mkdol/dolla/pumper/.perfect"; do
  [ -d "$v" ] && VAULT="$v" && break
done
[ -n "$VAULT" ] || { mkdir -p "C:/Users/mkdol/dolla/pumper/.perfect" && VAULT="C:/Users/mkdol/dolla/pumper/.perfect"; }
```

Subtrees: `Research/` (one note per run, `YYYY-MM-DD-{slug}.md`), `Lessons/` (shared,
`YYYY-MM-DD-research.md`, append-only), `Patterns/user-preferences.md`,
`Patterns/descoped-reopenable.md`, `00 - Index.md`. Do not overwrite an index written by a
sibling skill.

## Reference files

- `CLAUDE.md` (repo root) - **always, first.** Commands table (`just` <-> raw cargo),
  architecture map, dependency rule, the four-step app-adding contract.
- `MEMORY.md` (repo root) - **always.** Index into `.perfect/` durable state + the invariants
  that are expensive to rediscover. Check `.perfect/Architect/backlog.md` **Pending** so a
  finding does not duplicate structural work `/architect` has already queued.
- `context-map.json` (repo root) - **always.** The relevance-scoring surface and the
  file->context authority (7 groups / 22 contexts, `filePaths`, one-line `index`).
- `docs/harness/harness-learnings.md` - **always.** pumper's hand-curated stack file: how the
  engines work, the shapes the repo has settled on, what has already been tried.
- `ONBOARDING.md` - **always.** Section 2 the deliberate trades, 3 the dependency rule, 4 the
  consumer API, 7 invariants, 8 the run-it-for-real rule, 9 the continuous-development
  charter, 10 the data-source catalog.
- `catalog/data-sources.toml` + `catalog/README.md` + `catalog/connector-docs.json` - loaded
  only when bucket B or C is in scope.
- `crates/server/src/registry.rs` - the registered-app list. Loaded with the catalog.
- `docs/deployment.md` - the run story (local-first operation, persistent state layout, auth
  posture). Load before scoring any ops-, security- or hosting-shaped idea; it is what makes
  most "containerize / authenticate / add a control plane" ideas out-of-scope rather than novel.
- `docs/features/` on demand in Phase 6 when `context-map.json`'s file lists are too coarse:
  `runtime.md`, `fetching.md`, `extraction.md`, `resilient-extraction.md`, `crawling.md`,
  `datasets.md`, `search.md`, `http-api.md`, `events-webhooks.md`, `triggers.md`, `apps.md`,
  `datahub.md`, `sdk-typescript.md`. The **known gaps** section is where a finding is most
  often already anticipated.

## Buckets (replace the lane's Code / Template / Credential)

**A - Code improvement.** A change to existing code. Output: target file(s) under `crates/**`
or `clients/**`, the function/trait name if known, evidence the gap exists, and which layer it
belongs to.

**B - New scraping app.** A use case fitting the `ScrapeApp` contract - a new crate under
`crates/apps/`. Output: kebab-case app name matching the catalog `id`, engine tier
(`http` / `browser` / `claude` / `bulk`), target URL(s), dataset shape and its **key**, and the
closest existing app in `crates/apps/` with why this is not a duplicate.

The scaffolding contract (`ONBOARDING.md` Path B), all five in the same change: new crate
under `crates/apps/<name>` implementing `ScrapeApp` (depending on `core` only) -> add to
`[workspace.dependencies]` and `crates/server/Cargo.toml` -> one line in
`crates/server/src/registry.rs` -> a `[[source]]` row in `catalog/data-sources.toml` -> a
mention in `docs/features/apps.md`.

**C - New data source.** A source not yet in `catalog/data-sources.toml`. Output: proposed
catalog `id`, `market`, `category` (`open-calls` / `awarded-history` / `registry` /
`labor-market`), `engine`, `access` (`key-free` / `api-key` / `bulk` / `scrape`), plausible
`cadence`, a `confidence` 1-5 with a reason, and why this machine needs it.

**Combo (B + C).** Present once, flag both; the **catalog row lands first** - it is the cheap,
reversible half and is useful even if the app is never built. Then scaffold the app, then flip
the row's `status` to `live` and fill `app` and `cron`.

## Relevance filter

Score against `context-map.json`. **Low** includes anything that contradicts an
`ONBOARDING.md` section 2 deliberate trade - drop it.

Bucket-specific evidence rules:

- **B**: first scan `catalog/data-sources.toml` for a row covering the same source /
  market / category (faster than reading crates), then `crates/apps/` names and
  `registry.rs`. A similar app exists -> drop as "duplicate of `{app}`". Boost when the
  proposed `category` or `market` is thin in the catalog (count the rows).
- **C**: first scan `catalog/data-sources.toml` for the domain/publisher. Found -> drop as
  "already catalogued as `{id}` (status: `{status}`)" - except a `planned` or `blocked` row,
  which is a **revival candidate**, not a drop. Boost `access = "bulk"` or an official API
  over a fragile `scrape`. Verify the proposed `engine` honestly: JS-only portal ->
  `browser`, not `http`; judgement required -> `claude`. Getting this wrong misleads every
  future reader.

Host-first grep table for the code bucket:

| Idea shape | Host-first grep |
|---|---|
| New HTTP endpoint | `Grep "route\|Router::new\|EXPECTED" crates/server/src/routes/` |
| Background/scheduled work | `Grep "tokio::spawn\|JoinHandle\|Scheduler\|cron" crates/server/src/` |
| Retry / backoff / rate limit | `Grep "max_attempts\|backoff\|Governor\|retry" crates/` |

## Gates / validation commands

The repo-root `justfile` is the canonical task runner (`cargo install just`); `CLAUDE.md`
carries the same table. Run everything **from the repo root** - the `.env` loader and the
default `config.toml` path are both CWD-relative.

| `just` | raw cargo |
|---|---|
| `just check` | `cargo check --workspace` |
| `just fmt-check` / `just fmt` | `cargo fmt --check` / `cargo fmt` |
| `just lint` | `cargo clippy --workspace --all-targets` |
| `just test` | `cargo test --workspace` |
| `just test-ignored` | `cargo test --workspace -- --ignored` (env-dependent; not in CI) |
| `just ci` | `fmt-check` + `lint` + `test` - the whole CI job |
| `just build` | `cargo build -p pumper-server` |
| `just run` / `just dev` | `cargo run -p pumper-server --bin pumper` (+/- `RUST_LOG=debug`) -> `http://127.0.0.1:8088` |

**`--bin pumper` is required** (three binaries, no `default-run`). The maintenance binaries
(`just reindex`, `just search-backfill <scope>`) must run with the server stopped.
`clients/typescript` only: `npm run build && npm test` in that directory. A finding that
changes a command updates the `justfile` and `CLAUDE.md`'s table in the same commit.

## Runtime verification (all buckets)

If accepted work changes what a scrape actually does - an app's parsing, an engine's fetch
behavior, tiering/escalation, the worker or scheduler - either run it or say plainly that you
did not:

```bash
just run
curl -s -X POST http://127.0.0.1:8088/apps/<name>/jobs \
     -H 'content-type: application/json' -d '{"params":{}}'
curl -s http://127.0.0.1:8088/jobs/<id>          # poll to succeeded, inspect result
```

`ONBOARDING.md` section 8: a change to a scraping app or engine **is not done until you have
watched a real job run through it**. Set `max_budget_usd` when a claude-tier job is involved.

## Docs & catalog sync (before the commit phase)

Skip entirely when zero findings were accepted, or every accepted finding was internal-only.

1. **Feature docs** - open `scripts/docs/feature-doc-map.json`, find the doc coupled to the
   source glob you changed, update it in the **same session** (the Stop hook checks). Describe
   what IS - surface, params, data model, known gaps; future-looking ideas belong in the vault.
   A new feature area needs a map entry **and** its feature doc in the same change; a new area
   with no map entry is invisible to the hook forever.
2. **Catalog** - update the `[[source]]` row (`app`, `engine`, `access`, `cadence`, `cron`,
   `status`, `confidence`); verify enums against `catalog/README.md` rather than guessing;
   sanity-check parity - every `status = "live"` row corresponds to an app in
   `crates/server/src/registry.rs` and vice versa.
3. **Context map** - files added, moved or removed update the owning context's `filePaths` in
   `context-map.json` (per `.claude/CLAUDE.md`). A new app crate needs a home in an app group.

Confirmation block:

```
Docs & catalog sync:
  - docs/features/{files}              ({N} updated)
  - scripts/docs/feature-doc-map.json  ({new entry | unchanged})
  - catalog/data-sources.toml          ({N} rows added/changed)
  - context-map.json                   ({updated | unchanged})
```

Nothing required -> print `Docs & catalog: no user-visible change - sync not required.` and
say so in one sentence when dismissing the Stop hook.

## Parallel-session conventions

pumper has no `.claude/active-runs.md` ledger and no `scripts/active-runs.mjs`; the vault is
the coordination surface. The checkout is shared with `/architect`, `/perfect`, `/explorer`
and `/tiger` - drift you did not author is expected, not alarming, and must NOT be swept in.
Stage only explicit paths in ONE bash invocation; never `git add -A` / `.` / `-u`; never
`--amend`; commit prefix `research:`.
