---
name: explorer
description: Wander one logical area of the pumper codebase, surface 10 items worth fixing, triage with the user, then execute the accepted ones in-session. Daily low-friction quality sweeps with per-context coverage memory. Pairs with /research (external sources) and /architect (heavy structural change).
category: Maintenance
memory: vault
contexts: tracked
---
# Explorer (pumper edition)

Wander a logical section of the pumper codebase, surface exactly **10 items** worth fixing, let the user triage, then execute the accepted ones in-session. Designed for frequent / low-friction use — daily wandering — and pairs with `/research` (external sources) and `/architect` (heavy structural change).

Adapted from the personas `/explorer` skill. pumper is a **Rust workspace** (axum job server + SQLite queue + tiered fetch engines + `ScrapeApp` plugin crates), so the area taxonomy comes from `context-map.json` (22 contexts in 7 groups), the structural facts from `docs/harness/harness-learnings.md`, and every validation is cargo-based. It shares the repo vault with `/architect` and `/perfect` but owns its own `Explorer/` subtree.

## Interaction conventions

Built for parallel CLI control — every user prompt is single-keystroke answerable.

- **Every prompt is a numbered menu.** Numeric input picks the option; **Enter** triggers the default; option `1. other → …` is the deviation lane (free text).
- **Every phase output (intermediate or final) ends with a `Next?` block** of 2–5 numbered next-step actions. Replying with a digit advances the run without typing prose.
- Long free-text answers are still accepted everywhere; the menu just makes the common case instant.

## Input

Ask **two** numbered-menu questions, in this order. Numeric input picks the option; **Enter** picks the default; option `1. other → …` is the deviation lane and accepts free text.

### Q1 — Area

```
Area? (Enter = pick for me)
  1. other → type a hint (path fragment, crate name, or context name)
  2. runtime      — Scraping Runtime Core (tiered fetcher, politeness, app/job model, capability traits)
  3. extraction   — Data Extraction & Storage (broad crawler, declarative extraction, dataset store)
  4. engines      — Scraping Engines (search index, WASM sandbox, http/browser/claude engines)
  5. server       — Job Server & API (config/catalog, events/webhooks, worker/cron, datahub, registry, routes)
  6. grants       — Public Funding & Grants Apps (EU/regulatory watchers, US grant opportunities)
  7. economic     — Economic & Labor Market Data Apps (US wages/tax/valuation, MPSV, Census density)
  8. content      — Content & Research Apps (extraction/crawl/api-watch, web research & readable)
  9. clients      — clients/typescript (the SDK) and its contract with the API
  10. pick for me   ← default
```

Numeric options 2–8 map 1:1 to the 7 groups in `context-map.json` (`Scraping Runtime Core`, `Data Extraction & Storage`, `Scraping Engines`, `Job Server & API`, `Public Funding & Grants Apps`, `Economic & Labor Market Data Apps`, `Content & Research Apps`). Option 9 (`clients/typescript`) is deliberately outside the context map — it is a real area with its own npm toolchain, so treat it as an eighth area with the file list taken from `clients/typescript/src/**`. Option 1's free text falls through to the Phase 2a resolver (path fragment / crate name / exact context name). Option 10 / Enter triggers Phase 2b auto-pick.

### Q2 — Category

```
Category? (Enter = any)
  1. other → describe (free-form intent; layered onto an auto-picked category)
  2. any            ← default
  3. quality
  4. dx
  5. perf
  6. bug
  7. sec
  8. docs
  9. tests
```

Wait for both answers. Don't ask anything else upfront — further questions only if a phase requires clarification.

If the user replies just "go" or "wander" or types `/explorer` with no arguments, treat as "pick for me" + "any" (Enter defaults for both).

> **Why no `ui` / `i18n` / `a11y`:** pumper has no user interface. The only surfaces are the HTTP API, the TypeScript SDK, and the CLI/log output. Presentation-shaped concerns land under `dx` (SDK/API ergonomics) or `docs`.

---

## Constants

- **Codebase reference files** (always loaded):
  - `context-map.json` (repo root) — the feature map: 7 groups, 22 contexts, `filePaths` per context, one-line `index`. The natural area taxonomy and the authority for which files a context owns.
  - `CLAUDE.md` (repo root) — the commands table (`just` ↔ raw cargo), the architecture map, the dependency rule, and the four-step app-adding contract. Read this before the harness docs; it is the shortest true summary of the repo.
  - `MEMORY.md` (repo root) — the index into `.perfect/` durable state (Architect backlog, decisions, coverage, pattern catalogues) plus the invariants that are expensive to rediscover. **Read at session start**; it will point you at the pending architectural queue so a sweep item doesn't duplicate queued structural work.
  - `docs/harness/harness-learnings.md` — structural facts, conventions, pattern catalogue. Read before proposing any change that spans crates.
  - `.claude/CLAUDE.md` — session rules (context-map discipline, same-session doc sync, "bug fixes ship as extracted, tested functions").
  - `docs/features/README.md` + the relevant `docs/features/*.md` — the implemented-product surface for the area you are sweeping. Faster than a wide grep when you need to know what a feature promises.
  - `docs/deployment.md` — the run story: local-first operation, persistent state layout, auth posture. Load it for any `sec`, `docs`, or ops-shaped sweep; it is the authority on what the deployment model actually is before you call something a gap.
  - `ONBOARDING.md` §2 (operating principles) and §7 (invariants) — the deliberate design trades. **Do not surface them as findings** (see 4b).
- **Vault root** (resolved at Phase 0):
  - `Explorer/sweeps/` — one note per run, the canonical artifact
  - `Explorer/state.md` — informational claim board (which areas are being explored *right now*)
  - `Explorer/coverage.md` — heatmap of last visit per area + yield density
  - `Explorer/passes.md` — per-area "already considered and rejected" memory; future passes skip these
  - `Patterns/explorer-preferences.md` — distilled rules across runs (promoted from Lessons)
  - `Lessons/{date}-explorer.md` — append-only self-reflection (shared `Lessons/` dir with `/architect` and `/research`)
- **Categories** — `quality | dx | perf | bug | sec | docs | tests`
- **Severities** — `critical | high | medium | low`
- **Effort buckets** — `xs (<15m) | s (15-60m) | m (1-3h) | l (>3h)`
- **Validation commands.** The repo-root `justfile` is the **canonical task runner** (`cargo install just`, then `just --list`); the raw cargo form is listed alongside because CI runs it directly. Everything runs **from the repo root** — the `.env` loader and the default `config.toml` path are both CWD-relative.

  | `just` | raw cargo | what it is |
  |---|---|---|
  | `just check` | `cargo check --workspace` | fast type-check |
  | `just fmt-check` | `cargo fmt --check` | formatting gate (CI) |
  | `just lint` | `cargo clippy --workspace --all-targets` | lint gate (CI) |
  | `just test` | `cargo test --workspace` | unit + integration (CI) |
  | `just test-ignored` | `cargo test --workspace -- --ignored` | environment-dependent tests (real Chrome, built wasm, timing) — not in CI |
  | `just ci` | `fmt-check` + `lint` + `test` | the whole CI job in one command |
  | `just build` | `cargo build -p pumper-server` | build the binaries |
  | `just run` | `cargo run -p pumper-server --bin pumper` | boot the server → `http://127.0.0.1:8088` |
  | `just dev` | same with `RUST_LOG=debug` | verbose boot |

  **`--bin pumper` is required.** The `pumper-server` package ships three binaries (`pumper`, `reindex`, `search-backfill`) and sets no `default-run`, so a bare `cargo run -p pumper-server` fails with *"could not determine which binary to run"*. The two maintenance binaries (`just reindex`, `just search-backfill <scope>`) must run with the **server stopped**.

  `clients/typescript` only: `npm run build` + `npm test` inside that directory.

  **If you change a command, update the `justfile` in the same commit** — it is the surface every other session reads.

---

## Phase 0: Resolve vault path

```bash
VAULT=""
for v in "C:/Users/mkdol/Documents/Obsidian/pumper" "C:/Users/mkdol/dolla/pumper/.perfect"; do
  [ -d "$v" ] && VAULT="$v" && break
done
# First run: neither exists → create C:/Users/mkdol/dolla/pumper/.perfect
# (Obsidian-openable folder, committed by default so the loop state travels with the repo).
[ -n "$VAULT" ] || { mkdir -p "C:/Users/mkdol/dolla/pumper/.perfect" && VAULT="C:/Users/mkdol/dolla/pumper/.perfect"; }
echo "$VAULT"
```

Record `$VAULT` for the rest of the run. This is the same vault `/architect` and `/perfect` use — share `Lessons/` and `Patterns/`, own `Explorer/`.

### Bootstrap (one-time per vault)

If any of these are missing, create them:

- `$VAULT/Explorer/` (directory)
- `$VAULT/Explorer/sweeps/` (directory)
- `$VAULT/Explorer/state.md` — header only:
  ```markdown
  # Explorer State

  Active claims by `/explorer` runs. Informational only — not a hard lock.
  Stale entries (>2h) are released automatically by the next run.

  ## Active

  _No active explorers._
  ```
- `$VAULT/Explorer/coverage.md` — header only:
  ```markdown
  # Explorer Coverage

  Heatmap of areas explored. Used by Phase 2 to pick the staleest, highest-yield area.

  ## Areas
  ```
- `$VAULT/Explorer/passes.md` — header only:
  ```markdown
  # Explorer Passes

  Per-area record of items that were surfaced and **rejected** in past runs.
  Future passes over the same area skip these. Accepted items don't appear here
  (their fix is in the codebase). Items that were not surfaced are also absent.

  ## Areas
  ```
- `$VAULT/Patterns/explorer-preferences.md` — header only:
  ```markdown
  # Explorer Preferences (distilled from /explorer runs)

  > Rules upgraded from `Lessons/` after 3+ observations. Loaded by Phase 1.

  _No patterns yet. Will be populated as runs accumulate._
  ```

Don't create `Lessons/` if it already exists (shared with `/architect` and `/research`).

---

## Phase 1: Load context & memory

### 1a. Required-file check

- `context-map.json` missing → stop and tell the user to run Vibeman's refresh (per `.claude/CLAUDE.md`); without it there is no area taxonomy.
- `docs/harness/harness-learnings.md` missing → warn and continue; the sweep is still valid, just less pattern-aware.

### 1b. Read in order

1. `CLAUDE.md` + `MEMORY.md` (repo root) — the commands table, the architecture map, and the index into `.perfect/` durable state. `MEMORY.md` points at `.perfect/Architect/backlog.md`; check its "Pending" section so a sweep item doesn't duplicate queued structural work.
2. `context-map.json` — to learn the area taxonomy (7 groups, 22 contexts, `filePaths`, the `index` one-liners).
3. `docs/harness/harness-learnings.md` — structural facts, conventions, the pattern catalogue.
4. `.claude/CLAUDE.md` — session rules (context-map discipline, doc-sync-in-same-session, bug-fixes-as-extracted-tested-functions).
5. `$VAULT/Architect/strong-patterns.md` (if present) — the canonical shapes this codebase has been observed to do well. When you propose a fix in Phase 5, **prefer the shape of an existing strong pattern** over inventing a new one. Reference it in the item's `strong_pattern_ref` field.
6. `$VAULT/Patterns/explorer-preferences.md` — to deprioritize finding shapes the user has rejected before.
7. `$VAULT/Explorer/state.md` — to know what *other* explorers are working on right now.
8. `$VAULT/Explorer/coverage.md` — last-visit dates and yield per area.
9. `$VAULT/Explorer/passes.md` — which items were already rejected per area.
10. The 3 most recent files in `$VAULT/Lessons/` matching `*-explorer.md` (sorted descending) — recent self-reflection.

### 1c. Stale-claim sweep

In `$VAULT/Explorer/state.md`, any entry whose `claimed_at` is older than 2 hours is **stale** — assume the run was abandoned. Remove stale entries before proceeding. This keeps the file honest without an explicit lock.

### 1d. Snapshot freshness

Parse `generatedAt` / `revision` in `context-map.json`. If it is >30 days old, OR `git rev-list --count HEAD` has advanced by >200 since the map was generated, warn but continue:
```
Warning: context-map.json may be stale ({N} commits / {D} days since generatedAt).
Consider a Vibeman refresh after this session.
```
Cross-check cheaply: if `crates/apps/` contains a crate that appears in **no** context's `filePaths`, the map is definitely stale — say so.

---

## Phase 2: Pick area

### 2a. If user gave a hint

Resolve the hint to one or more contexts in `context-map.json`:
- Exact group name (e.g. `Scraping Engines`) → all contexts under that group.
- Exact context name (e.g. `Dataset Store & Change Detection`) → that single context.
- Path fragment (e.g. `crates/apps/grants-gov`) → contexts whose `filePaths` overlap.
- Crate name (e.g. `engine-wasm`) → contexts whose `filePaths` sit under that crate.

If the resolution is ambiguous (>3 plausible areas), present a short numbered list and ask "which one?" before continuing.

### 2b. If user said "pick for me"

Score each context by:
- **Staleness** — days since last visit per `coverage.md` (more days = higher score). Never-visited = max staleness.
- **Past yield density** — items accepted / items surfaced in last 1–2 visits (higher = higher score). Tie-breaker.
- **Active claim penalty** — if the context appears in `state.md` Active section, score = 0 (skip it; pick a different area).

Pick the top-scored context. If multiple tie, pick the one with the smaller `filePaths` count (faster to scan, tighter feedback loop).

Tell the user which area you picked and why (one short sentence), then a `Next?` menu:

```
Next?
  1. other → name a different area or context name
  2. proceed with {picked-area}   ← default
  3. abort
```

### 2c. Category filter

If the user's category filter is not `any`, narrow the scan focus accordingly. The area stays the same; the filter only changes what kind of items count toward the 10-item budget.

---

## Phase 3: Claim the area

Append an entry to `$VAULT/Explorer/state.md` under the `## Active` section:

```markdown
- **{area-slug}** — claimed_at: {ISO timestamp}, run_id: {short random id}, category: {filter}
```

This is **informational, not a lock.** Other explorers reading this file will pick a different area.

pumper has no `.claude/active-runs.md` ledger; the vault `state.md` claim IS the coordination surface. But the checkout **is** shared with concurrent CLI sessions (see the git rules in Phase 7), so before scanning run `git status --porcelain -- <area paths>` and note any file in the area that already has uncommitted work — you will need it at commit time.

Print the claim line to the user so they know what's recorded.

---

## Phase 4: Wander the code

Read enough of the area to identify 10 items. Budget your tool calls — don't read every file in a 20-file area. Sample strategically.

### 4a. Sampling strategy

For an area with N files (from the context's `filePaths`):
- N ≤ 5: read all of them.
- 5 < N ≤ 20: read the crate's `lib.rs` / `mod.rs` entry points plus a sampling of the rest, capped at 10 file reads.
- N > 20: read all entry points + discover the largest files (`Glob` then sort by line count) + sample 5–8 of those.

Use `Read` with offset/limit when files are >500 lines — read top + bottom + a middle slice rather than the full file. In this repo `crates/core/src/app.rs`, `crates/core/src/storage.rs`, and `crates/server/src/routes/*` are the usual >500-line offenders.

### 4b. What to look for, by category

**Hard exclusion — the deliberate local-first trades.** `ONBOARDING.md` §2 declares these intentional, not bugs: no API auth, permissive CORS, `--dangerously-skip-permissions` on the Claude CLI, real login cookies on disk in `data/browser-profile`, non-2xx HTTP bodies returned rather than raised. Do NOT surface "add auth to the API" or "harden the browser profile" as items. A `sec` item must be a defect *within* that model (a path traversal in an artifact path, a secret written into a job result, an unbounded allocation from a remote body), not a re-litigation of the model.

**Second exclusion — context-map bookkeeping.** "Context X's `filePaths` is missing file Y" is a map refresh, not a sweep item. Fix it silently as part of an accepted item if you touch that context; never spend one of the 10 slots on it.

For `quality`:
- Dead code, unreachable branches, `pub` items nobody outside the crate uses.
- Duplicated logic across app crates (3+ near-identical blocks) that belongs in `core` or a `*-common` crate (`grants-common`, `census-common`, `trades-common` are the existing homes).
- Misleading names, unclear intent, leaking abstraction across the `apps → core ← engines` boundary.
- Comments explaining "what" instead of "why" — flag the comment, not just the code.
- Commented-out code older than the current branch.

For `dx`:
- Test setup boilerplate that could be a fixture or a `#[cfg(test)]` helper in `core`.
- Repeated `map_err(|e| Error::…)` boilerplate that should use an existing `From` impl or a helper in `crates/core/src/error.rs`.
- App params parsed ad-hoc from `serde_json::Value` instead of a typed `#[derive(Deserialize)]` params struct.
- A `ScrapeApp::description()` that doesn't document its param shape (the registry contract in `ONBOARDING.md` §9).
- Missing error context (errors returned without enough info to tell which URL / which record failed).
- SDK ergonomics in `clients/typescript` — untyped responses, missing exports, drift from the route surface.

For `perf`:
- Blocking work inside an async task (`std::fs`, `reqwest::blocking`, heavy parse) without `spawn_blocking`.
- Per-record DB round-trips where the dataset store offers a batch upsert (N+1 against SQLite).
- Rebuilding a `Regex`, `Selector`, or extraction rule set inside a loop instead of once (`core::extract` compiles rules once by design — deviations are findings).
- Whole-body `String` allocation where a streaming read would do (the crawler streams bodies to disk by design; new code that buffers is a regression).
- Unbounded concurrency — a `join_all` over an unbounded frontier instead of a bounded pool / semaphore.
- Cache and governor bypasses: an engine call that skips `HttpCache` / the per-domain `Governor`.

For `bug`:
- Race conditions in the queue/worker path (claim-then-write without a transaction, missing crash-recovery requeue).
- Unhandled edge cases (empty result sets, `unwrap()` on remote data, off-by-one on pagination cursors).
- Cancellation/timeout gaps — a job that can outlive its wall-clock timeout because the inner future isn't `select!`ed.
- Retry logic that retries non-idempotent work, or that doesn't respect `max_attempts`.
- Errors swallowed silently (`let _ =`, `.ok()` on a fallible write, an empty `Err(_) => {}` arm).
- Change-detection correctness — a dataset upsert whose key doesn't actually identify the record (silent dedup or silent duplication).

For `sec`:
- Path traversal into the artifact dir or the plugin dir from remote-controlled input.
- User/remote input interpolated into SQL instead of a bound `sqlx` parameter.
- Secrets (API keys from `config.toml`, cookies) logged via `tracing` or echoed into a job result / webhook payload.
- WASM plugin execution without the fuel + memory caps `engine-wasm` promises.
- Webhook HMAC signing/verification gaps.
- Unbounded remote input (a body read with no size cap, a redirect chain with no limit, a zip/JSON bomb).

For `docs`:
- A route, param, config key, dataset shape, or trigger contract that exists in code but not in the matching `docs/features/*.md` (the Stop hook enforces same-session sync — a pre-existing gap is a legitimate item).
- `catalog/data-sources.toml` out of sync with `crates/server/src/registry.rs` (an app registered but absent from the catalog, or a catalog `status = "live"` for an app that isn't registered).
- `ONBOARDING.md` / `README.md` claiming a capability the code no longer has, or missing one it now has.
- A config key in `crates/core/src/config.rs` with no mention in `config.toml`'s commented reference.

For `tests`:
- A bug fixed inline in a `run()` body with no extracted, named function and no test — the exact anti-pattern `.claude/CLAUDE.md` names. Surfacing one is always a valid item.
- A convention asserted in prose but not enforced by an inventory test (the EXPECTED-diff idiom in `crates/server/src/routes/mod.rs` is the canonical shape).
- `#[ignore]`d tests that have silently stopped compiling (they aren't in the CI path).
- An engine capability added without a test, contrary to the ONBOARDING §9 charter.

### 4c. Honor the deprioritization signals

- If `Patterns/explorer-preferences.md` contains a rule like "user rejects style-only findings without a measurable issue," skip those.
- If `Explorer/passes.md` for this area lists items by short fingerprint (file:line + 1-line summary), skip exact matches. A near-match is OK to surface — but note "previously passed; resurfacing because <reason>".
- Cross-check the area's previous sweep notes (`Explorer/sweeps/*-{area-slug}.md`) — don't resurface an item a past run already surfaced, unless its status changed.

### 4d. Dedupe against recent history (one command, seconds)

Before finalizing candidates, run **one** git log over the area's paths:

```bash
git log --oneline -20 -- crates/core/src/fetch.rs crates/core/src/governor.rs
```

Drop any candidate whose anchor was plausibly fixed or reworked by a recent commit (verify by reading the current code, not the commit message). Note that `/architect` and `/perfect` commit into this same repo with their own prefixes — an `architect:` commit touching your area is a strong signal the item is already handled. If a candidate survives despite recent activity, note "still present after <sha>" in its evidence.

### 4e. Stop conditions

- 10 items found → stop scanning, move to Phase 5.
- Exhausted the area without 10 items → widen scope by pulling in the *adjacent* context from the same group in `context-map.json`. Note the widening in the run record. If still <10 after widening twice, stop with what you have and explain the shortfall.
- Tool budget exceeded (>40 file reads) → stop with what you have.

**Do not pad the list** with low-value items just to hit 10. Quality over quota. If you stop short, the run record explains why. Small app crates (`crates/apps/*` are frequently 1–3 files) legitimately yield 3–5 items — widening to the sibling app in the same group is the intended move.

---

## Phase 5: Categorize and structure each item

### Premise verification (hard gate — no item ships without it)

Every item's `anchor` must be a `file:line` **you actually Read in this session**, and its `evidence` must quote or paraphrase the real code at that line. Before presenting, re-Read the anchor lines of any item whose premise came from a grep hit or a sampled slice, and confirm the defect is really there (the guard isn't elsewhere, the "dead" `pub fn` isn't used by another crate — one `Grep` over `crates/` settles it; the "missing" timeout isn't applied by the worker wrapper in `crates/server/src/worker.rs`). Pattern-matched suspicion ("this *usually* means…") is not an item. If verification kills a candidate, replace it or run short — never pad with unverified ones.

For each of the 10 (or fewer) items, capture:

```yaml
- id: 1
  title: "<short imperative phrase, ≤60 chars>"
  category: quality | dx | perf | bug | sec | docs | tests
  severity: critical | high | medium | low
  effort: xs | s | m | l
  anchor: "<file_path>:<line_number>"
  evidence: "<2-3 sentence explanation of the gap, with verbatim code snippet if helpful>"
  suggested_fix: "<1-2 sentence shape of the fix — not the fix itself>"
  strong_pattern_ref: "<wikilink to Architect/strong-patterns#... entry>" | null
  doc_impact: "<none | docs/features/<file>.md needs the same-session update | catalog/data-sources.toml>"
  cluster_hint: "<other ids that ship naturally with this one, or 'standalone'>"
```

**On `strong_pattern_ref`:** if the suggested fix matches the shape of an entry in `Architect/strong-patterns.md` (e.g. proposing a typed params struct when the strong pattern "typed app params + `serde` defaults" exists), set `strong_pattern_ref` to the wikilink. The fix should then **conform to the canonical example** in that entry, not invent a new shape. If no strong pattern applies, leave it null.

**On `doc_impact`:** `.claude/CLAUDE.md` requires that any user/API-visible change updates the coupled feature doc **in the same session**. `scripts/docs/feature-doc-map.json` is the source→doc map; consult it to fill this field. Internal-only changes are `none` — say so explicitly so the Stop hook can be dismissed in one sentence.

### Severity rubric (be honest)

- **critical** — data corruption in the dataset store, a queue state machine that can lose or double-run jobs, a remote-input path that can write outside `data/`. Drop everything and ship.
- **high** — wrong behavior on the golden path (a scrape silently returns empty), a broken common edge case, regression risk if left.
- **medium** — paper cut, confusing API, small perf hit, latent risk.
- **low** — polish, nice-to-have, taste-level.

If you find yourself rating most items "high," recalibrate downward. A 10-item list typically lands as 0–1 critical, 2–3 high, 4–6 medium, 1–3 low.

### Cluster detection

After categorizing, scan for items that should ship together:
- Same file → same commit.
- Type/function dependency → ship in order (a `core` trait change lands before the app that uses it).
- Same doc surface → one `docs/features/*.md` update covering both.

Note these in `cluster_hint`.

---

## Phase 6: Present to user

Print a summary table, then per-item detail.

### Summary table

```
#   Cat     Sev    Effort  Title                                              Anchor
─   ─────   ────   ──────  ─────────────────────────────────────────────────  ──────────────────────────
1   bug     high   s       Claimed job not requeued when worker panics        crates/server/src/worker.rs:118
2   perf    med    xs      Selector rebuilt per row in parse loop             crates/apps/hackernews/src/lib.rs:64
3   docs    med    s       /datasets/{app}/{ds}/duplicates undocumented       crates/server/src/routes/datasets.rs:212
...
```

### Per-item detail

For each row:
```
[N] {title}
    Category / Severity / Effort:  {cat} / {sev} / {effort}
    Anchor:    {file:line}
    Evidence:  {explanation + snippet}
    Suggested: {1-2 sentence fix shape}
    Follows:   {strong-pattern wikilink + canonical example, or "—" if none applies}
    Docs:      {none | docs/features/<file>.md | catalog/data-sources.toml}
    Cluster:   {standalone | ships with [a, b]}
```

If any items are clustered, end the section with a short "Clusters" block:
```
Clusters:
  - [2, 5, 8] — all in crates/apps/hackernews/src/lib.rs; ship in one commit. Order: 5 → 2 → 8.
  - [3] alone — routes doc fix, separate commit.
```

---

## Phase 7: Triage

Ask the user:
```
Which to action? Reply with item numbers (e.g. "1, 3, 4").

Shortcuts:
  all     — accept every surfaced item
  none    — accept nothing (still write the sweep note)
  ask     — guided walkthrough item-by-item
  Enter   — same as "none"   ← default
```

For each accepted item, execute it **in this same session**. Same default as `/research`: discover → decide → implement → commit, all in one context window.

### Execution rules

**Single accepted item with a clear anchor (Option A):**
1. Apply the edit at `anchor`.
2. Run validation, narrowest sufficient first:
   - `just check` — always, minimum bar.
   - `just test` — whenever behavior changed or a test was added.
   - `just lint` — whenever you touched Rust that CI will lint.
   - `just fmt` (then `just fmt-check`) — always before committing Rust.
   - `just ci` — the whole gate in one command; prefer it when the change spans crates.
   - `clients/typescript` touched → `npm run build && npm test` inside that directory.
3. **Stage scoped + verify + commit in ONE Bash invocation** (concurrent sessions rewrite the index between separate calls):
   ```bash
   git add crates/server/src/worker.rs crates/server/src/worker_tests.rs && git diff --cached --stat
   ```
   Never `git add -A`, `git add .`, or `git add -u`. If the cached stat lists **more files than you added**, the index held another session's pre-staged work — `git restore --staged <path>` each unrelated file, re-verify, THEN commit. Never trust the index.
4. Commit atomically: `explorer: <short title>` + Co-Authored-By footer + body explaining the why.

**2+ accepted items (Option B):**
1. Print the inline plan (one paragraph per item: file, change shape, validation).
2. Execute in **risk-ascending order** (xs effort first, l last; severity ties broken by category — `bug` before `perf` before `docs` before `quality`).
3. Atomic commit per item, validation per commit, same one-invocation stage-verify-commit discipline as Option A.
4. If validation fails → fix inline, do NOT stack failing commits. No `--no-verify`, no `--amend`.
5. If a downstream item turns out to be redundant after an upstream commit, drop it and note the drop in the run record.

**Item that needs more thought (Option D — escape hatch):**
Record it in the run record as `decided: deferred` with the reason. Do NOT write a handoff file. The run record is the future search target. Use sparingly — prefer A or B.

### Rust changes — non-negotiable

If any accepted item touches `crates/**`:
- **Honor the dependency rule.** `apps` depend on `core` only (plus leaf parsing libs); `engines` depend on `core` only; only `server` depends on everything. If your fix wants `pumper-engine-*` in an app's `Cargo.toml`, you want a trait from `core` via `AppContext.engines` instead. This is the one rule that breaks other agents' in-flight work.
- **Bug fixes ship as extracted, tested functions.** Per `.claude/CLAUDE.md`: extract the predicate/transform into a named function, add a test named after the anti-pattern it defends (`x_not_y` style), then wire it into the call site. A fix buried inline in a `run()` body is an unguarded fix and does not count as done.
- **Additive-first.** New optional field / new trait method with a default beats a breaking `core` change. If you must break `core`, update every impl in the same change and run `just test`.
- **Doc sync in the same session.** If the change is user/API-visible (endpoint, param, dataset shape, app, trigger contract, config key, CLI-observable behavior), update the coupled `docs/features/*.md` in the same commit — `scripts/docs/feature-doc-map.json` maps source globs to docs, and the Stop hook checks. If it's internal-only, say so in one sentence.
- **Keep `context-map.json` true.** If the change adds or moves a file between contexts, update the context's `filePaths` in the same commit.

If you can't honor these in the change, defer the item — don't ship it half-converted.

### Runtime verification

If a change alters what a scrape actually does (an app's parse/selectors, an engine's fetch behavior, the tiering/escalation logic, the worker/scheduler), state explicitly that you have NOT run it, OR run it:

```bash
just run                          # = cargo run -p pumper-server --bin pumper → http://127.0.0.1:8088
# then, in another shell:
curl -s -X POST http://127.0.0.1:8088/apps/<name>/jobs \
     -H 'content-type: application/json' -d '{"params":{…}}'
curl -s http://127.0.0.1:8088/jobs/<id>          # poll to succeeded, inspect result
```

ONBOARDING §8 is explicit: a change to a scraping app or engine **is not done until you've watched a real job run through it**. Don't claim "works" from `just check` alone — the interesting failures (selectors that stopped matching, a site that now needs the browser tier) are runtime-only. If the site is unreachable or the run would burn Claude budget, say so plainly rather than implying verification.

---

## Phase 8: Persist the sweep

Write `$VAULT/Explorer/sweeps/{YYYY-MM-DD}-{area-slug}.md`:

```markdown
---
date: 2026-07-26
run_id: {short id}
area: {context name or group}
files_sampled: {N}
category_filter: any | quality | ...
total_items: 10
accepted: [1, 3, 4]
declined: [2, 5, 6, 7, 8, 9, 10]
deferred: []
commits: [<sha1>, <sha2>]
widened: false
---

# {Area title} sweep — {date}

## Items

### [1] {title}  ✅ accepted → {commit sha} `{commit subject}`
**Category / Severity / Effort:** {cat} / {sev} / {effort}
**Anchor:** `{file:line}`
**Evidence:** {evidence}
**Fix shape:** {what was actually done; reference commit body for detail}
**Validation:** {just check / just test / just lint / live job run — with results}

### [2] {title}  ❌ declined
**Category / Severity / Effort:** ...
**Anchor:** ...
**Evidence:** ...
**Decline reason:** _filled in Phase 9_

### [3] {title}  ⏸ deferred
**Category / Severity / Effort:** ...
**Reason:** {why deferred — concrete blocker, not vague "later"}

...

## Cross-references
- Adjacent areas not yet swept: {list from coverage.md, optional}
- Related preferences: [[Patterns/explorer-preferences]]
```

---

## Phase 9: Self-reflection

### 9a. Ask why for declined items

Single batched question:
```
For the declined items, why did you skip them?

  [2] {title}
  [5] {title}
  ...

Reply per-item ("2: too vague, 5: already planned") or one overall reason.

Shortcuts:
  skip    — record "no reason given"
  Enter   — same as "skip"   ← default
```

### 9b. Append to Lessons

Write/append `$VAULT/Lessons/{YYYY-MM-DD}-explorer.md`. **Use `Edit` to append, never `Write` to replace** — the file is shared by date across runs (and `/architect` and `/research` write siblings in the same directory).

```markdown
## Run: {timestamp} — {area} ({category filter})

Sampled: {N} files
Surfaced: {M} items
Accepted: [list]
Declined: [list] (with reasons)
Deferred: [list] (with blockers)

### Self-reflection
- Categories that resonated: {pattern}
- Categories that didn't: {pattern}
- Calibration drift: {e.g. "rated 7 items 'high' but user accepted only 2; over-weighting severity"}
- Tools to use more / less next time: {observation}
```

### 9c. Backfill the sweep note

Add the decline reasons to the Phase 8 sweep note's `[N] declined` blocks.

### 9d. Update passes.md

For each declined item, append a fingerprint to `$VAULT/Explorer/passes.md` under the area's section (create section if missing):

```markdown
## {area}

- {file:line} — {1-line summary of the rejected suggestion} — pass {date}, run {id}, reason: {short reason}
```

The fingerprint matters — future passes over the same area skip these. Keep entries short.

### 9e. Pattern promotion check

Read all `$VAULT/Lessons/*-explorer.md`. If a decline reason has appeared in **3+ runs** (or close synonym), propose adding it to `$VAULT/Patterns/explorer-preferences.md`:

```
I've seen this 3+ times — promote to permanent rule?
  "{distilled rule}"

Source runs: [[2026-07-12-dataset-store]], [[2026-07-19-worker-scheduler]], [[2026-07-26-fetch-engines]]

Next?
  1. promote to Patterns/explorer-preferences.md   ← default
  2. snooze (re-ask after 3 more observations)
  3. drop (don't promote, reset the counter)
```

If the user picks 1, append to `Patterns/explorer-preferences.md`.

### 9f. Update coverage.md

Update or insert the row for this area:

```markdown
## Areas

### {area-slug}

- Last visited: {date}
- Last run: [[Explorer/sweeps/{date}-{area-slug}]]
- Items surfaced (last 3 runs): [10, 8, 10]
- Items accepted (last 3 runs): [3, 5, 4]
- Yield density: {accepted / surfaced average}
- Notes: {anything noteworthy across runs}
```

### 9g. Release the claim

Remove the entry written in Phase 3 from `$VAULT/Explorer/state.md`.

---

## Phase 10: Final summary

Print:
```
Explorer run complete.

  Area:           {name} (group: {group})
  Category:       {filter}
  Files sampled:  {N}
  Items surfaced: {M} / 10
  Accepted:       {K} → {commit shas}
  Declined:       {L}
  Deferred:       {D}

  Validation:     just check ✓ / just test ✓ (N tests) / just lint ✓ / just fmt-check ✓
                  live job run: {app} → succeeded | not run ({reason})

  Coverage update: last visit {date} → {today}, yield density {X}/{Y}

  Files updated:
    + {VAULT}/Explorer/sweeps/{date}-{slug}.md
    + {VAULT}/Lessons/{date}-explorer.md
    ~ {VAULT}/Explorer/coverage.md
    ~ {VAULT}/Explorer/passes.md  (if any declines)
    ~ {VAULT}/Explorer/state.md   (claim released)
    {if pattern promoted:}
    ~ {VAULT}/Patterns/explorer-preferences.md

  Next?
    1. /explorer {staleest adjacent area}                ← default
    2. /explorer {same area, different category}
    3. /research {area}    (external-source companion run)
    4. /architect resume   (drain backlog)
    5. done
```

If zero items were accepted, frame the run as a successful pass over a healthy area. The point is signal, not action.

---

## Notes on use

- **Pair with `/research`** — run `/explorer` after a research session that touched a specific area, to immediately surface adjacent gaps the research run didn't cover.
- **Cadence** — daily or every-other-day is a reasonable rhythm. `coverage.md` will tell you when the codebase is uniformly fresh and you should switch to `/architect` instead.
- **App crates are cheap wins.** `crates/apps/*` are small, numerous, and written by many sessions — they carry most of the duplication and most of the untyped-params debt. When coverage is even, prefer an app group over the runtime core for a fast, high-yield sweep.
- **Coexist with uncommitted work.** Multiple CLIs share this working tree. Explorer never stashes, resets, or discards anything it didn't author. Each commit stages **only the specific paths** the explorer touched; never `git add -A`, `git add .`, or `git add -u`. If an item's anchor file already has uncommitted changes from someone else, surface it: "this file already has changes — commit them first, or layer on top?" Default to layer-on-top if the user doesn't pick. Forbidden at all times: `git stash`, `git reset --hard`, worktree-touching `git restore` / `git checkout --` on paths the run didn't author, `git clean -f`. (`git restore --staged <path>` to unstage a foreign pre-staged file is allowed — it never touches the working tree.) Expect `HEAD` to advance mid-run; on `index.lock`, wait 3–10s and retry up to 6 times.
- **Never `git commit --amend`** — another session may already have built on your commit.
- **Drift signal** — if 3+ explorer runs in a row produce 0 accepted items, the calibration is off (severity bar too low, or area was wrong). Trigger a self-reflection: read the last 3 sweeps and ask the user "what shape would have actually been useful?"

## App context coverage (Personas-managed repos)

This skill declares `contexts: tracked` — the Personas app measures per-context memory coverage for it. When run inside a Personas-managed repo (a `.personas/` dir exists, or the app dispatched this run), record progress into the Project Memory Ledger so the Skills Manager shows honest coverage. Before finishing, append JSON lines to `.personas/memory-outbox.jsonl` at the repo root (append, never rewrite) — one node per context you meaningfully worked on:

```json
{"type":"node","kind":"progress","title":"<=200 chars: what you did in this context","body":"optional detail","context":"<exact context name from context-map.json>","skill":"explorer"}
```

Always set both `"skill":"explorer"` and `"context":"<name>"` — together they drive the per-skill context-coverage % (last 30 days). The app ingests and deletes the file when the session ends. Skip silently when the repo is not Personas-managed.
