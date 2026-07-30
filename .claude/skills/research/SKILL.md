---
name: research
description: Extract actionable improvements for pumper from external sources (video, blog, article, raw text). Scores ideas against the codebase, buckets into Code / New App / New Data Source, and persists findings to the repo memory vault.
argument-hint: "[source or question]"
category: Maintenance
memory: vault
---
# Research (pumper edition)

Extract actionable improvements for the pumper project from any external source (YouTube video, blog post, article, raw text). Score ideas against the codebase, bucket into **Code / New App / New Data Source**, execute the accepted ones in-session, and persist the run to the memory vault.

Adapted from the personas `/research` skill. pumper is a **Rust workspace** (axum job server + SQLite queue + tiered fetch engines + `ScrapeApp` plugin crates), so relevance scoring runs against `context-map.json`, the buckets map to this repo's real extension seams (`ONBOARDING.md` Path B), and validation is cargo-based. It shares the repo vault with `/architect`, `/perfect`, and `/explorer`.

## Input

Ask the user, in this order:

1. **"What is the source? Paste a YouTube URL, an article URL, or raw text."**
2. **"Any focus hint? (`code` / `apps` / `sources` / `all`) — defaults to `all`."**

Wait for both answers before proceeding. Do NOT ask anything else upfront — further questions only if a phase requires clarification.

---

## Constants

- **Codebase reference files:**
  - `CLAUDE.md` (repo root) — **always loaded, first.** The commands table (`just` ↔ raw cargo), the architecture map, the dependency rule, and the four-step app-adding contract. The shortest true summary of the repo.
  - `MEMORY.md` (repo root) — **always loaded.** The index into `.perfect/` durable state plus the invariants that are expensive to rediscover. It points at `.perfect/Architect/backlog.md`; check its **Pending** section so a finding doesn't duplicate structural work `/architect` has already queued.
  - `context-map.json` (repo root) — the feature map: 7 groups, 22 contexts, `filePaths` per context, one-line `index`. **Always loaded.** This is the relevance-scoring surface and the file→context authority.
  - `docs/harness/harness-learnings.md` — structural facts, conventions, pattern catalogue. **Always loaded.** This is pumper's equivalent of a hand-curated stack file: how the engines work, what shapes the repo has settled on, what has already been tried.
  - `ONBOARDING.md` — the agent-facing contract: §2 operating principles (the deliberate trades), §3 the dependency rule, §4 the consumer API, §7 invariants, §9 the continuous-development charter, §10 the data-source catalog. **Always loaded.**
  - `catalog/data-sources.toml` + `catalog/README.md` + `catalog/connector-docs.json` — what this machine already scrapes, with schema. Loaded only when bucket B or C is in scope.
  - `crates/server/src/registry.rs` — the list of registered apps. Loaded with the catalog.
  - `docs/deployment.md` — the run story: local-first operation, persistent state layout, auth posture. Load it before scoring any ops-, security-, or hosting-shaped idea; it is the authority on what the deployment model actually is, and it is what makes most "you should containerize / authenticate / add a control plane" ideas out-of-scope rather than novel.
- **Feature reference docs (`docs/features/`)** — the implemented-product reference, kept in sync with source by the Stop hook described in `.claude/CLAUDE.md` → "Documentation Sync". Use these on demand in Phase 6 when `context-map.json`'s file lists are too coarse to anchor a finding precisely. The README at `docs/features/README.md` indexes every area:
  - `runtime.md`, `fetching.md`, `extraction.md`, `resilient-extraction.md`, `crawling.md`, `datasets.md`, `search.md`, `http-api.md`, `events-webhooks.md`, `triggers.md`, `apps.md`, `datahub.md`, `sdk-typescript.md`
  - Read the relevant feature doc before deep-greping when a finding lands inside one of those areas — the doc's "what it does / API surface / data model / known gaps" sections often surface the exact attachment point faster than a wide grep. The **known gaps** section in particular is where a finding is most often already anticipated.
- **Vault** (resolved in Phase 0): `C:/Users/mkdol/Documents/Obsidian/pumper` if it exists, else `<repo>/.perfect` (shared with `/architect`, `/perfect`, `/explorer`; Obsidian-openable, committed by default).
  - `Research/` — one note per run
  - `Lessons/` — self-reflection notes (shared)
  - `Patterns/user-preferences.md` — distilled rules across runs
  - `Patterns/descoped-reopenable.md` — conditional waits
  - `00 - Index.md` — vault entry point
- **Validation commands.** The repo-root `justfile` is the **canonical task runner** (`cargo install just`, then `just --list`); `CLAUDE.md` carries the same table. Run everything **from the repo root** — the `.env` loader and the default `config.toml` path are both CWD-relative.

  | `just` | raw cargo |
  |---|---|
  | `just check` | `cargo check --workspace` |
  | `just fmt-check` / `just fmt` | `cargo fmt --check` / `cargo fmt` |
  | `just lint` | `cargo clippy --workspace --all-targets` |
  | `just test` | `cargo test --workspace` |
  | `just test-ignored` | `cargo test --workspace -- --ignored` (env-dependent; not in CI) |
  | `just ci` | `fmt-check` + `lint` + `test` — the whole CI job |
  | `just build` | `cargo build -p pumper-server` |
  | `just run` / `just dev` | `cargo run -p pumper-server --bin pumper` (± `RUST_LOG=debug`) → `http://127.0.0.1:8088` |

  **`--bin pumper` is required.** The `pumper-server` package ships three binaries (`pumper`, `reindex`, `search-backfill`) and sets no `default-run`, so a bare `cargo run -p pumper-server` fails with *"could not determine which binary to run"*. The maintenance binaries (`just reindex`, `just search-backfill <scope>`) must run with the server stopped.

  `clients/typescript` only: `npm run build && npm test` in that directory. If a finding changes a command, update the `justfile` and `CLAUDE.md`'s table in the same commit.

---

## Phase 0: Bootstrap vault (one-time)

Resolve the vault root:

```bash
VAULT=""
for v in "C:/Users/mkdol/Documents/Obsidian/pumper" "C:/Users/mkdol/dolla/pumper/.perfect"; do
  [ -d "$v" ] && VAULT="$v" && break
done
[ -n "$VAULT" ] || { mkdir -p "C:/Users/mkdol/dolla/pumper/.perfect" && VAULT="C:/Users/mkdol/dolla/pumper/.perfect"; }
```

If `$VAULT/00 - Index.md` doesn't exist, create the structure:

```
$VAULT/
  00 - Index.md
  Research/
  Lessons/
  Patterns/
    user-preferences.md
```

`00 - Index.md` content (only if absent — do not overwrite one written by `/architect` or `/perfect`):
```markdown
# pumper Memory Vault

Long-term memory for `/research`, `/explorer`, `/architect`, `/perfect`, and `/tiger`.

## Folders
- [[Research/]] — one note per `/research` run, source + extracted ideas + triage decisions
- [[Lessons/]] — self-reflection notes from each run (what was rejected and why)
- [[Patterns/]] — distilled rules across runs ([[Patterns/user-preferences|user preferences]])
- [[Architect/]] — ADRs, weak/strong patterns, backlog
- [[Explorer/]] — sweeps, coverage, passes

## Conventions
- Research notes: `YYYY-MM-DD-{slug}.md` with frontmatter (source, date, accepted, rejected)
- Lessons notes: `YYYY-MM-DD-research.md` — append-only, one block per run
- Patterns are upgraded from Lessons after a rule has been observed 3+ times
```

`Patterns/user-preferences.md` content:
```markdown
# User Preferences (distilled from /research runs)

> Rules upgraded from `Lessons/` after 3+ observations. Loaded by `/research` Phase 1.

_No patterns yet. Will be populated as runs accumulate._
```

---

## Phase 1: Load context & memory

### 1a. Determine which reference files to load

| Focus | Files loaded |
|---|---|
| any | `CLAUDE.md` + `MEMORY.md` (repo root) — always, first |
| `code` | the above + `context-map.json` + `docs/harness/harness-learnings.md` + `ONBOARDING.md` |
| `apps` | the above + `catalog/data-sources.toml` + `catalog/README.md` + `crates/server/src/registry.rs` |
| `sources` | the above + `catalog/connector-docs.json` |
| `all` (default) | everything, plus `docs/deployment.md` |

`CLAUDE.md`, `MEMORY.md`, `context-map.json`, `harness-learnings.md`, and `ONBOARDING.md` are **always required**. The catalog set is only required when bucket B or C is in scope. Load `docs/deployment.md` on demand for any ops/security/hosting-shaped idea regardless of focus.

### 1b. Verify required files exist

- `context-map.json` missing → stop; tell the user to run Vibeman's refresh (per `.claude/CLAUDE.md`). Without it there is no relevance-scoring surface.
- `docs/harness/harness-learnings.md` missing → warn and continue with `ONBOARDING.md` + `README.md` as the substitute; note the degradation in the run record.
- `catalog/data-sources.toml` missing AND focus needs it → stop; the catalog is the dedup surface for buckets B and C.

### 1c. Read and absorb the loaded files

- **`context-map.json`** — *where* code lives (7 groups, 22 contexts, `filePaths`, the `index` one-liners).
- **`docs/harness/harness-learnings.md`** — *how the machine works and what shapes it has settled on*.
- **`ONBOARDING.md`** — *the contract*: the dependency rule, the invariants, the deliberate trades, the extension seams.
- **`catalog/data-sources.toml`** — *what already exists* (per-source `app`, `market`, `category`, `engine`, `access`, `cadence`, `cron`, `status`, `confidence`).

**The single most important fact** — pumper's Claude engine literally spawns the `claude` CLI as a subprocess (`claude -p --output-format json`, `crates/engine-claude/src/lib.rs`), with `--model` / `--effort` / `--max-budget-usd` / `--json-schema` / `--resume` / `--allowedTools` built from `ResearchRequest` and the `[claude.roles.*]` presets in `config.toml`. Therefore **any idea about Claude Code CLI capabilities (flags, output formats, session resume, tool permissions, schemas, subagents, MCP, hooks) is highly relevant to this codebase, not out of scope.** There is no HTTP LLM SDK and no API key anywhere in the repo — never file a finding that assumes one. For anything deeper on this surface, hand off to `/tiger` (`.claude/skills/tiger/references/pumper-llm-surface.md` holds the full map).

### 1d. Check snapshot freshness

Parse `generatedAt` / `revision` in `context-map.json`. If it is >30 days old OR `git rev-list --count HEAD` has advanced by >200 since then, warn but continue:
```
Warning: context-map.json may be stale ({N} commits / {D} days since generatedAt).
Consider a Vibeman refresh after this session.
```
Cheap corroboration: a crate under `crates/apps/` that appears in no context's `filePaths` proves staleness.

### 1e. Load memory

Read in order:
1. `$VAULT/Patterns/user-preferences.md`
2. `$VAULT/Architect/strong-patterns.md` (if present) — the canonical shapes this codebase already does well. When a code-bucket finding's attachment point matches a strong pattern, prefer "extend the existing strong pattern" over "build something new" in Phase 6/7. Cite it under an `Aligns with:` line.
3. The 3 most recent files in `$VAULT/Lessons/` matching `*-research.md` (sorted descending).

These inform extraction priorities and what to deprioritize.

---

## Phase 1.5: Declare scope (shared checkout)

pumper has no `.claude/active-runs.md` ledger; the coordination surface is the working tree itself, shared with concurrent `/architect`, `/perfect`, `/explorer`, and `/tiger` sessions.

1. `git branch --show-current` — confirm the target branch. If it isn't `master`, do **not** switch the shared checkout; commit later via a temporary worktree pinned to `master`.
2. `git status --porcelain` — note every file that already has uncommitted work. Those are other sessions'. You will layer on top, never revert.
3. `git log --oneline -15` — see what landed recently; an `architect:`/`explorer:`/`perfect:` commit in the area your source is about is a strong dedup signal for Phase 6.

Record the three results in working memory; they feed the Phase 13 staging decision. Forbidden at all times: `git stash`, `git reset --hard`, `git clean -f`, `git checkout --`/`git restore` on paths this run didn't author, `git commit --amend`.

---

## Phase 2: Source ingestion

Detect source type from the user's first answer:

### 2a. YouTube URL
Patterns: `youtube.com/watch?v=`, `youtu.be/`, `youtube.com/shorts/`

Check `yt-dlp` is installed:
```bash
yt-dlp --version
```

If missing, abort with:
```
yt-dlp is not installed. Install it with one of:
  - winget install yt-dlp
  - pip install yt-dlp
  - Download from https://github.com/yt-dlp/yt-dlp/releases
Then re-run /research.
```

Otherwise, extract auto-generated subtitles:
```bash
mkdir -p .research-cache
yt-dlp \
  --skip-download \
  --write-auto-sub \
  --sub-lang en \
  --sub-format vtt \
  --output ".research-cache/%(id)s.%(ext)s" \
  "<url>"
```

Parse the resulting `.vtt` file:
- Strip WEBVTT header
- Strip cue settings and styling
- Collapse consecutive duplicate lines (auto-subs repeat heavily)
- Keep timestamps in `[HH:MM:SS]` format every ~30 seconds for citation

If no `.vtt` was produced (some videos have transcripts disabled), report the issue and ask the user to paste the transcript manually or provide an alternative source.

**Cleanup (MANDATORY, scoped to THIS run's video id):** as soon as the cleaned text is in working memory, delete the cache files this run created. Do this before Phase 3 starts — not at the end of the run, where a mid-run failure would leave strays.

```bash
# Replace <id> with the actual video id used in --output above.
rm -f .research-cache/<id>.* 2>/dev/null
```

Rules for the cleanup:
- **Scope strictly to this run's id.** Never sweep `.research-cache/*` blindly — that races with any parallel research run on the same machine.
- **Idempotent on failure.** If the rm fails, log it as a `cache_cleanup_skipped` note in the Lessons block but continue — leaving cache is not a run-blocking error.
- **Verify in Phase 11.** The final summary's "Files updated" block should include `Cache: cleaned` (or the residue path).
- **`.research-cache/` is not in this repo's `.gitignore`.** Either add it (a legitimate one-line change, mention it to the user) or be scrupulous about the scoped `rm` — otherwise strays show up in `git status` and risk being swept into someone else's commit.

### 2b. Other URL
Use `WebFetch` with a prompt asking for the article body, stripped of nav/footer/ads.

### 2c. Raw text
Use as-is.

**Sanity check:** if the resulting text is <300 words, report it's too thin to harvest meaningful ideas and stop.

> **Source-type agnosticism.** The transport (2a/2b/2c) does not change any downstream phase — same frontmatter, same Phase 6 rules, same output formats. Do not special-case downstream phases on how the text arrived.

---

## Phase 2.5: Web augmentation (technique/tooling lookup)

Transcripts and talks routinely name a tool or technique without explaining how it works. A speaker says "we use Firecrawl for the render step", "we JA3-spoof the TLS handshake", "we run the extraction as a WASM component" and moves on — leaving the text technique-shaped without enough depth for a clean Phase 6 evidence pass. This phase fills that gap with a **bounded** web round.

### 2.5a. Decide whether to run

Run web augmentation when **all** of these hold:
- The cleaned source text references at least one **named tool, framework, crate, protocol, technique, or workflow pattern** that is non-obvious from the transcript alone
- A correct Phase 6 evidence pass would benefit from knowing how that thing actually works (API shape, key concepts, integration points, licensing)
- The reference is not already deeply documented in `harness-learnings.md`, `docs/features/`, or present in `Cargo.toml`

**Skip the phase** when the source is fully self-contained. Don't run it on every source — it costs tool calls and drifts into rabbit-holes.

### 2.5b. Build the lookup list

From the cleaned text, list candidates — typically 1–5. For each, record `name` (exact spelling), `kind` (`crate` | `tool` | `service` | `protocol` | `technique` | `workflow_pattern`), and `why_useful` (one line on how a deeper definition changes Phase 6 framing).

Drop items that are:
- Already a dependency (check `Cargo.toml` / `Cargo.lock` / `clients/typescript/package.json`) — those are "already have it" catches, not augmentation candidates
- Already a catalogued source (`catalog/data-sources.toml`) — same
- Generic primitives (`HTTP`, `JSON`, `cron`, `SQLite`) — no augmentation value
- Commodity brand names the speaker only name-drops without using

### 2.5c. Run the lookup (bounded)

Cap at **3 web calls total** — this is augmentation, not full research.

- **First** try `WebSearch` with `<name> <kind> <year>`. One query usually surfaces the canonical docs URL or the crates.io page.
- **Then** `WebFetch` the single most authoritative result (docs.rs, crates.io, vendor docs, RFC, GitHub README) with a prompt like: *"Extract the core concept, API surface, licensing, and how it would integrate into a Rust async scraping service. Skip marketing copy."*
- For a Rust crate, also note: maintenance status, whether it builds on ARM64 Windows (a real constraint here — the README calls out BoringSSL/fingerprint impersonation as blocked for exactly this reason), and its transitive weight.

Stop early once the technique is understood. Do NOT fetch every result.

### 2.5d. Capture the augmentation note

For each looked-up item, write 2–4 sentences in working memory:
- **What it is** (one sentence)
- **How it works at a high level** (the load-bearing technical fact)
- **Integration shape** (which layer it would attach to — `core` trait, an engine crate, an app, the server)
- **Why it matters for pumper** (does it suggest a code change, a new app, or a new data source?)

These feed Phase 3 (better idea quality), Phase 5 (better bucket assignment), and Phase 6 (better grep terms — knowing the crate name lets you grep `Cargo.toml` for the right thing).

### 2.5e. Write the cited URLs into the Research note

Phase 9's frontmatter gets:
```yaml
web_augmentations:
  - { name: "CrateName", url: "https://docs.rs/...", kind: "crate" }
```
This makes the augmentation traceable and prevents re-fetching on later cross-checks.

### 2.5f. Anti-patterns

- **Don't run augmentation to validate the speaker's claims.** That's Phase 6, against the codebase.
- **Don't quote the augmentation source as a Phase 7 source anchor.** The anchor stays the original transcript/article.
- **Don't escalate a web-augmentation discovery into a finding on its own.** Add it as an extracted idea in Phase 3 with `source_anchor: "(web augmentation, not in transcript)"`.

---

## Phase 3: Raw idea extraction

From the source text, extract 5–15 distinct ideas. Each must be:
- A concrete technique, pattern, tool, or recommendation (not opinions or filler)
- Grounded in a specific quote or timestamp from the source
- Standalone enough to be evaluated independently

For each idea, capture:
- `title` — short imperative phrase (<60 chars)
- `summary` — 1–2 sentences
- `source_anchor` — quote (≤20 words) or `[HH:MM:SS]` for video sources
- `tentative_bucket` — initial guess: `code` / `app` / `source` / `unclear`

Apply memory-informed filtering: if `Patterns/user-preferences.md` says "user rejects dependency-adding ideas" or similar, deprioritize matching ideas (still extract, but mark `low_priority: true`).

**Also check `Patterns/descoped-reopenable.md`** (if it exists) for findings previously descoped that may now be viable. If any apply to the current source, surface them explicitly in Phase 7 as "previously descoped, reconsider?" items alongside the new findings.

### Source-type yield calibration

Different source types produce different finding profiles. **A "low" finding count is not a failure mode if it matches the source type's expected yield.** Don't force extraction past the natural limit just to hit a number.

| Source type | Expected yield | Typical pattern |
|---|---|---|
| **Technical interview / engineering talk** (scraping, crawler, queue, Rust async, anti-bot) | **densest** — 3–5 strong findings with concrete file anchors | Engineers describing a system they built reveal architectural critiques that map directly onto `core`/engine gaps. |
| **Crate / library release or deep-dive** | dense — 2–4, usually one dependency decision plus one pattern | Weigh against the ARM64-Windows build constraint and the additive-first rule before scoring High. |
| **Competitor product demo** (a hosted scraping API, an agent-scraper SaaS) | **low + many catches** — 1–3 real findings, 5–10 "already existed" | Every demonstrated feature is a "does pumper have this?" probe. Expect the catch count to exceed the finding count; that is a successful run. |
| **Data-publisher / open-data announcement** | low-medium, but bucket-C heavy — 1–3 new catalogable sources | The highest-value source type for buckets B/C. Check `catalog/data-sources.toml` before getting excited. |
| **Philosophical / forward-looking piece** | low — 1–2, mostly deltas against what's already built | Narrow deltas against existing implementations. |
| **Consumer web-app build tutorial** ("build X with Next.js + a DB + deploy") | **≈0 findings + 1–4 catches** | Near-zero overlap with a Rust scraping service. Do the full Phase 3 extraction to prove the drop, write a stub note, stop. Do not stretch into a weak finding. |
| **Blog post / raw text** | varies widely | Yield depends on content density, not transport. |

**If the finding count feels low, check the source type first.** Surface the catch count prominently in Phase 7 as the primary metric for low-finding runs.

---

## Phase 4: Relevance filter

For each idea, score relevance against `context-map.json`:

- **High** — clearly maps to a named context; specific files/crates are obvious anchors
- **Medium** — partial overlap with a group's subject, no clear file anchor
- **Low / drop** — no plausible attachment point in any context, or it contradicts an `ONBOARDING.md` §2 deliberate trade

**Drop all `Low` ideas.** Don't waste user attention on out-of-scope material.

**Scoring honesty — evidence caps the score.** Phase 4 scores are provisional; they become final only after Phase 6. A finding may carry `Relevance: High` into Phase 7 **only if** Phase 6 actually read or grepped the anchor file(s) in this session and the finding cites the resulting `file_path:line`. "Sounds applicable to pumper" without a code read caps the score at `Medium`, and the Evidence line must say `unverified — keyword match only`. Never present an unverified finding as High just because the source is compelling.

If the focus hint was `code` / `apps` / `sources`, drop ideas that don't match the chosen bucket (after Phase 5 reclassification).

---

## Phase 5: Bucket classification

Re-evaluate each surviving idea and assign a final bucket. An idea may belong to **multiple** buckets — present it once, flag all applicable.

### Bucket A — Code improvement
The idea suggests a change to existing code. Examples:
- "Cap the redirect chain and the body size in the http engine"
- "Compile the extraction rule set once and reuse it across the batch"
- "Add a cancellation token to `AppContext` so running jobs can be killed"

Required output: target file(s) under `crates/**` or `clients/**`, the function/trait name if known, evidence the gap exists, and **which layer it belongs to** (see the Phase 6 routing check).

### Bucket B — New scraping app
The idea describes a use case that fits the `ScrapeApp` contract — a new crate under `crates/apps/`. Indicators:
- A specific dataset someone on this machine would want on a schedule
- A clear "fetch → parse → upsert records" shape, or a research-shaped question the `claude` tier could answer
- It would replace a manual, repetitive lookup

Required output: proposed app name (kebab-case, matching the catalog `id`), the engine tier it needs (`http` / `browser` / `claude` / `bulk`), the target URL(s), the dataset shape and its **key**, and the closest existing app in `crates/apps/` (and why this isn't a duplicate).

The scaffolding contract (`ONBOARDING.md` Path B) is: new crate under `crates/apps/<name>` implementing `ScrapeApp` (depending on `core` only) → add to `[workspace.dependencies]` and `crates/server/Cargo.toml` → one line in `crates/server/src/registry.rs` → a `[[source]]` row in `catalog/data-sources.toml` → a mention in `docs/features/apps.md`. All five, in the same change.

### Bucket C — New data source
The idea references a data source not yet in `catalog/data-sources.toml`. Indicators:
- A named portal, API, bulk download, or registry that pumper doesn't track
- Cataloguing it would unlock app ideas in Bucket B

Required output: proposed catalog `id`, `market`, `category` (`open-calls` / `awarded-history` / `registry` / `labor-market`), `engine`, `access` (`key-free` / `api-key` / `bulk` / `scrape`), plausible `cadence`, a `confidence` 1–5 with a reason, and why this machine needs it.

If an idea is a `source + app` combo (a new app for a not-yet-catalogued source), present it once, flag both buckets, and note that the **catalog row lands first** — it is the cheap, reversible half, and it is useful even if the app is never built.

---

## Phase 6: Evidence gathering

For each surviving idea, gather concrete evidence to make the user's triage easy. Budget your tool calls.

### Code bucket

**Step 1 — Host infrastructure first.** Before searching for the specific feature, grep for the *category of host infrastructure* the idea would attach to. Examples:

| Idea shape | Host-first grep |
|---|---|
| New HTTP endpoint | `Grep "route\|Router::new\|EXPECTED" crates/server/src/routes/` |
| Background/scheduled work | `Grep "tokio::spawn\|JoinHandle\|Scheduler\|cron" crates/server/src/` |
| Retry / backoff / rate limit | `Grep "max_attempts\|backoff\|Governor\|retry" crates/` |
| Caching | `Grep "HttpCache\|ResearchCache\|cache_key\|ttl" crates/core/src/` |
| New table / column | `ls crates/core/migrations/` then `Grep "CREATE TABLE" crates/core/migrations/` |
| Extraction rule | `Grep "Rule\|Selector\|json_pointer" crates/core/src/extract*` |
| Engine capability | `Grep "trait HttpClient\|trait Browser\|trait Researcher" crates/core/src/engine.rs` |
| Claude CLI flag | `Grep "args.push" crates/engine-claude/src/lib.rs` |
| Dedup / change detection | `Grep "simhash\|upsert\|changed\|unchanged" crates/core/src/` |
| Concurrency / fairness | `Grep "Semaphore\|per_app\|concurrency" crates/server/src/worker.rs` |

This catches existing-but-undocumented surface area in one grep. **A single discovery here typically reframes 2–4 findings at once** — what looked like "build new infrastructure" becomes "add a field to the existing governor" / "add a variant to the existing rule enum". Reframing changes both effort estimates and anchors, so do it before deeper greps.

**Step 1b — Catalog vs runtime check.** Before scoring any quantitative architectural critique ("too many apps", "the prompt is huge", "N sources is unmanageable"), verify the catalog count is NOT the same as the per-execution count:
- ~24 app crates in the workspace ≠ 24 apps running at once. The worker has a **global** concurrency cap and **per-app** caps; a given job touches exactly one app.
- Rows in `catalog/data-sources.toml` ≠ registered apps. Rows may be `planned` or `blocked`; `crates/server/src/registry.rs` is the runtime truth.
- Engine crates compiled ≠ engines used per job. The tiered fetcher escalates `http → browser → claude` only on demand; most jobs never leave tier 1.
- Config keys defined ≠ config keys set. Every key in `config.toml` is optional with a default in `crates/core/src/config.rs`.

If a finding's premise depends on catalog count = runtime count, **the finding is wrong** — drop it or reframe before presenting.

**Step 1c — Layer routing (`core` vs `engine` vs `app` vs `server`).** Before deciding the file anchor, check which layer owns the change. This is pumper's hardest-won structural rule (`ONBOARDING.md` §3):

```
apps  ─depend on─►  core  ◄─depend on─  engines
                     ▲
                     └────── server wires apps + engines together
```

- Would **every** app benefit? → `core` (a trait, a helper, a model). Breaking change to `core` = update every impl in the same change.
- Is it about **how bytes are fetched or rendered**? → the relevant `engine-*` crate, behind the existing `core` trait. Improving an engine upgrades every app at once.
- Is it about **one data source's shape**? → an app crate. Prefer a **new** app crate over bolting logic onto an existing one.
- Is it about **wiring, routing, scheduling, or delivery**? → `server`.
- Shared logic between sibling apps → the existing `*-common` crates (`grants-common`, `census-common`, `trades-common`), not `core`.

When in doubt ask: "would an app that scrapes something completely different benefit from this?" If no → it's not `core`. A finding anchored at the wrong layer is worse than no finding, because it invites a dependency-rule violation.

**Step 2 — Then search for the specific feature.** Now grep for the actual thing the idea proposes (function name, config key, flag, table name, crate in `Cargo.toml`).

**Step 3 — Read the anchor file.** `Read` the most relevant file(s) — limit to ~100 lines. Identify the exact `file_path:line_number` where the change would land. **For host-infrastructure verification, read enough to confirm the public API (~30 lines), not the implementation (~500 lines)** — token efficiency matters.

**Step 3a — Consult `docs/features/` when the context map is too coarse.** `context-map.json` gives groups and file lists, not flow descriptions. When a finding lands inside a documented area, open the matching `docs/features/<area>.md` before wider greps. The doc names the surface, the params, the data model, and the **known gaps** — frequently the exact attachment point (or the reason the gap is deliberate) is stated in one sentence. The doc-sync Stop hook keeps these aligned with source within one session, so they're current; don't infer staleness without a `git log` check.

When the finding spans multiple areas (e.g. a fetcher change that surfaces in the dataset store), read both docs — the framing in one is rarely sufficient.

**Step 3b — Read the signature before proposing reuse.** When a finding proposes to reuse an existing function or trait, read that item's **signature** before assigning relevance. A name that sounds right can still be semantically wrong (a helper that takes a `&Job` cannot be called from an engine that never sees one). This check has repeatedly reshaped findings from "trivial hook" into "new sibling module."

**Step 4 — Drop if redundant.** If the gap doesn't actually exist (the codebase already does this), drop the idea and record it as an already-existed catch.

**Step 5 — Grounding check (per finding, before Phase 7).** Every code finding presented as `High` must carry at least one `file_path:line` citation produced by a Read or Grep **in this session** — the line that proves the gap exists (or the host surface the change attaches to). If you can't produce that within budget, downgrade to `Medium` + `unverified`; don't fabricate an anchor from the context map's file list.

**Security escalation rule.** When a grep against a file that handles **remote-controlled input** (a fetched body, a URL, a plugin name, an app param) returns **zero hits for the relevant defense** (`max_len|limit|truncate|canonicalize|bind|sqlx::query!|fuel|memory_limit|hmac|verify`), do NOT drop the finding as "no existing pattern". **Escalate it to severity `CRITICAL` and re-label it as a security gap, not a feature add.** The shapes that matter here: path traversal into `data/artifacts` or `data/plugins` from remote strings, SQL built by string concatenation, unbounded body/redirect reads, WASM execution without the fuel + memory caps, webhook payloads signed but not verified, secrets reaching `tracing` or a job result.

**Do NOT escalate the deliberate trades.** `ONBOARDING.md` §2 declares these intentional: no API auth, permissive CORS, `--dangerously-skip-permissions` on the Claude CLI, real login cookies on disk, non-2xx bodies returned rather than raised. A source that says "always authenticate your internal APIs" produces a *catch*, not a finding. If a source makes a genuinely new argument that one of these must change, present it as a question to the user, flagged as a departure from a documented invariant — never as a routine finding.

**Doc-impact check.** When a code finding would change anything user/API-visible (an endpoint, a param, a dataset shape, an app, a trigger/webhook contract, a config key, CLI-observable behavior), mark it `docs: required` and name the coupled file from `scripts/docs/feature-doc-map.json`. Add the effort note: "the coupled `docs/features/*.md` must be updated in the same session — the Stop hook checks." Internal-only findings are marked `docs: none` so the hook can be dismissed in one sentence. If the finding adds or moves files between contexts, also note `context-map.json` needs the same-session update.

### App bucket (B)
- **First** scan `catalog/data-sources.toml` for an existing row covering the same source/market/category — faster than reading crates.
- Then scan `crates/apps/` names and `crates/server/src/registry.rs`. If a similar app exists, drop the idea — note "duplicate of `{app}`".
- If unsure, `Read` the closest existing app crate (1 file max — most app crates are a single `src/lib.rs`) to confirm the overlap and to reuse its shape.
- **Boost priority** if the proposed app's `category` or `market` is thin in the catalog (count the rows).
- If the app's source isn't catalogued yet, mark it **combo** (source + app; the catalog row lands first).

### Source bucket (C)
- **First** scan `catalog/data-sources.toml` for the domain/publisher.
- If found, drop the idea — note "already catalogued as `{id}` (status: `{status}`)". A `planned` or `blocked` row is *not* a drop — it is a revival candidate; surface it as such.
- If not found, **boost priority** when the source is `access = "bulk"` or has an official API (cheap to acquire, high confidence) over a fragile `scrape`.
- Verify the proposed `engine` is honest: if the portal is JS-only it's `browser`, not `http`; if judgement is required it's `claude`. Getting this wrong in the catalog misleads every future reader.

---

## Phase 7: Present findings

Print a single summary table followed by numbered detail blocks. **Before printing, run cluster detection so the user sees natural bundles instead of a flat list.**

### Cluster detection

- **Same file anchor** — multiple findings touching the same file usually want one commit. Note the cluster.
- **Dependency edges** — finding B needs a field/trait/table finding A would create. Note `depends on [N]`.
- **Layer pairing** — a `core` trait addition paired with the engine impl that satisfies it. Neither ships alone (the trait won't compile without every impl updated). Always present these as a forced pair.
- **Catalog pairing** — a new source row paired with the app that consumes it (the row is the prerequisite). Always present as a natural pair.
- **Doc pairing** — a code change paired with its coupled `docs/features/*.md` update. Not a separate finding; fold it into the code finding's scope.

For each cluster, add a one-line note: `Cluster: ships with [N, M] — recommended order: M → N`.

### Summary table

```
#  Bucket       Title                                          Relevance  File / Source
─  ───────────  ─────────────────────────────────────────────  ─────────  ──────────────────────────────
1  code         Cap redirect chain + body size in http engine  High       crates/engine-http/src/lib.rs:88
2  app          Daily watch on the EU tenders successor portal High       (new crate: crates/apps/ted-watch)
3  source       Add ted.europa.eu bulk download                Medium     (new catalog row: ted-eu)
4  code+doc     Expose escalation reason in the job result     High       crates/core/src/fetcher.rs:485
...
```

### Per-idea detail

```
[N] {title}
    Bucket(s):    {bucket(s)}
    Source:       "{quote}" or [HH:MM:SS]
    Summary:      {2-3 sentences}
    Layer:        {core | engine-<x> | apps/<name> | server | clients — from Step 1c}
    Evidence:     {file_path:line actually read/grepped this session; or "unverified — keyword match only" (caps relevance at Medium)}
    Recommended:  {edit {file} | new app crate {name} | new catalog row {id}}
    Why it fits:  {which context from context-map.json it maps to}
    Docs:         {none | docs/features/<file>.md (+ context-map.json if files move)}
    Aligns with:  {strong-pattern wikilink + canonical example, if any — else omit line}
    Cluster:      {standalone | ships with [a, b]}
```

---

## Phase 8: User triage

Ask the user:
```
Which findings should I action? Reply with numbers (e.g., "1, 3, 4"),
"all", "none", or "ask" for a guided walkthrough.
```

For each accepted finding:

### Code bucket

**IN-SESSION EXECUTION IS THE DEFAULT.** Split sessions fragment the work: a handoff written at the end of session N accumulates amendments in session N+1 and finally gets executed in session N+2 — each handoff is a place where context is lost, scope drifts, and uncommitted work is vulnerable to a bad merge. **Execute in the same session that produced the findings, validate, and commit atomically per task.**

**When in-session execution is NOT possible** (pick the fallback shape):
- **Context is critically tight** and the remaining budget cannot accommodate the edits + validation + commits.
- **Work is genuinely exploratory or multi-day** — requires design that doesn't exist yet or research into unknown systems.
- **Dependency is unavailable** — a crate that won't build on this ARM64 Windows box (the BoringSSL/fingerprint-impersonation case the README names), a gated API, a source that needs credentials nobody has (Option C territory).
- **User explicitly requests planning-only.**

Do NOT fall back to a handoff because the work feels large. "Large" is a signal to break into smaller atomic commits. Cross-layer work (a `core` trait + two engine impls + an app + a doc) is still in-session-executable as long as validation passes per task.

**Option A — Single isolated finding → execute + commit (DEFAULT for one finding)**
Apply the edit at the anchor, run validation, commit with a `research:` prefix. Do NOT write the finding to the vault as a "noted but not implemented" item.

**Option B — Clustered findings → in-session execution with atomic commits (DEFAULT for 2+)**

1. **Present the full task plan inline** before executing, so the user sees what is about to happen.
2. **Execute in risk-ascending order** (trivial constants first; `core` breaking changes last, since they force every impl to move together).
3. **After each task, run validation:**
   - Always: `just check`
   - Behavior changed or a test added: `just test`
   - Any Rust touched: `just fmt` then `just lint` (or `just ci` for the whole gate)
   - `clients/typescript` touched: `npm run build && npm test` in that directory
   - App/engine/fetcher behavior changed: **run a real job** (see "Runtime verification" below) — `just check` is not evidence
4. **Commit atomically per task** with a `research: <short task title>` prefix, Co-Authored-By footer, and a body explaining the why.
5. **If validation fails**, fix inline before moving on. Do NOT stack failing commits. No `--no-verify`, no `--amend`.
6. **If a task genuinely can't be completed in-session**, commit the completed tasks, then write a handoff for the remainder — never discard completed work.

The inline task plan should include:
- **Why this matters** — one paragraph (what problem, what infrastructure already exists)
- **Goal** — numbered list of the bundled findings as deliverables
- **Non-goals** — explicit "do NOT do these" list (deferred findings, scope-creep traps, layers not to touch)
- **Dependency graph & order** — which tasks ship together, which depend on which
- **Per-task spec** — file path & line anchor, migration SQL, struct/trait definitions, function signatures, acceptance criteria
- **Cross-cutting concerns** — the dependency rule (`apps → core ← engines`), additive-first over breaking `core`, the extracted-tested-function rule for any bug fix, the same-session doc-sync obligation, and the `context-map.json` update if file ownership moves

Record the commit SHAs in the Research note frontmatter (`commits: [...]`) and in the Phase 11 summary.

**Option B-Design — Design-then-execute (when the shape requires exploration)**
Pick this when the user says "propose approaches", "design first", "what are the options", "three different approaches", or otherwise signals the shape is ambiguous. Shape: explore → user picks → concrete design doc → **immediately execute** in the same session.

1. **Scan once more.** A focused round of evidence gathering beyond Phase 6 to ground the approaches in real anchors. Without it, the approaches read as generic and the user can't distinguish them.
2. **Present 2–3 approaches** with tradeoff tables (✅ benefits / ⚠️ risks) and effort estimates. Each must name actual crates and existing infrastructure it would attach to. A generic approach that could apply to any codebase is a smell.
3. **Wait for the user's pick.**
4. **Write a co-located `DESIGN.md`** next to where the code will land (e.g. `crates/core/src/<area>/DESIGN.md` or the new app crate's root), NOT in a planning folder. Co-location matters: a future session reading the code finds the rationale next to it. If the change genuinely spans layers, put it in `docs/harness/` alongside the other durable design records.
5. **Continue IMMEDIATELY to execution.** The user approved the approach in step 3; the design doc is the implementation contract, not a second decision gate.
6. **Treat the design doc as a working artifact.** Amend inline when implementation reveals a constraint; only pause if the change is structural enough that the user would have picked differently.
7. **Atomic commits per rollout step**, validation per commit.

**Anti-pattern:** writing a design doc and stopping there ("design ready for review"). That fragments the work across sessions.

**Option B2 — Implementation-ready handoff plan (FALLBACK)**
Use ONLY when one of the "not possible" conditions above is met. Same structure as Option B; save to `docs/harness/handoffs/{YYYY-MM-DD}-{slug}.md` (create the dir if needed — `docs/harness/` is where this repo keeps durable cross-session records). The plan must be **self-contained**: readable without the conversation that produced it. Record the path in the Research note frontmatter and the Phase 11 summary.

**Do NOT default to Option B2.** Every handoff written instead of executed risks work that never lands or lands fragmented.

**Option C — Theoretical scaffolding handoff (blocked/gated dependencies)**
Same structure as Option B, with a much stricter non-goals section. Use when the finding depends on something unavailable: a crate that won't build on this box, a gated API, a portal requiring credentials nobody has.

- **Non-goals explicitly forbid any real integration attempts.** *"Do NOT make requests to {host}. Not in tests, not in examples, not in commented-out code."*
- **Scaffolding only:** stub structs/traits, config keys with no defaults, `Err(Error::…)` returns, an enum variant added with a dispatch point returning the not-implemented error. It compiles; no runtime behavior is exercised.
- **Every stub gets a `TODO({feature}-{reason})` marker** (e.g. `TODO(tls-impersonation-boringssl)`) so a future session can grep the breadcrumbs.
- **Tests cover only the deterministic stub path.** No integration tests, no fixtures implying a real API shape.
- **An out-of-band section** lists "what to do when the blocker clears" as a concrete checklist.
- **New dependencies only if already present for other reasons.** Do NOT add a crate that only the stub would use — that's a compile-time cost for nothing and it can break the build on this platform.

**Option D — Just record (escape hatch only)**
Write the finding into the Research note only. No handoff. The note is the future search target. Prefer B or C for anything concrete enough to have a file anchor.

### App bucket (B)
There is no `/add-app` skill here — scaffold it directly, following `ONBOARDING.md` Path B, and do all five steps in one change:
1. `crates/apps/<name>/` with `Cargo.toml` (depending on `pumper-core` only, plus leaf parsing libs) and `src/lib.rs` implementing `ScrapeApp` with a real `description()` documenting its param shape.
2. Add to the root `[workspace.members]` / `[workspace.dependencies]` and to `crates/server/Cargo.toml`.
3. One line in `crates/server/src/registry.rs`.
4. A `[[source]]` row in `catalog/data-sources.toml` with honest `engine`, `access`, `cadence`, `cron`, `status`, `confidence`.
5. A mention in `docs/features/apps.md` (same session — the Stop hook checks).

Then **run it**: `just run` (= `cargo run -p pumper-server --bin pumper`), `POST /apps/<name>/jobs`, poll to `succeeded`, inspect the result and the dataset. An app that has never run is not done. Model the new crate on the closest existing app rather than inventing a shape.

### Source bucket (C)
Add the `[[source]]` row to `catalog/data-sources.toml` per the schema in `catalog/README.md`, with `app = ""` and `status = "planned"` if no app exists yet. This is cheap, reversible, and useful on its own — the catalog is the artifact other agents read to answer "what data do we have?". Verify the schema fields against `catalog/README.md` rather than guessing enum values.

### Combo bucket
Add the catalog row first (Bucket C), then scaffold the app (Bucket B), then flip the row's `status` to `live` and fill its `app` and `cron`. Confirm with the user before chaining.

For each declined finding (in the user's reply or by omission), record the number for Phase 10.

### Runtime verification (all buckets)

If the accepted work changes what a scrape actually does — an app's parsing, an engine's fetch behavior, the tiering/escalation logic, the worker or scheduler — either run it or say plainly that you didn't:

```bash
just run                                         # = cargo run -p pumper-server --bin pumper
                                                 # → http://127.0.0.1:8088 (`just dev` for RUST_LOG=debug)
curl -s -X POST http://127.0.0.1:8088/apps/<name>/jobs \
     -H 'content-type: application/json' -d '{"params":{…}}'
curl -s http://127.0.0.1:8088/jobs/<id>          # poll to succeeded, inspect result
```

`ONBOARDING.md` §8 is explicit: a change to a scraping app or engine **is not done until you've watched a real job run through it**. The interesting failures — selectors that stopped matching, a portal that now needs the browser tier, JSON the model didn't format as asked — are runtime-only. If a claude-tier job is involved, set `max_budget_usd`.

---

## Phase 9: Persist to the Research note

Write `$VAULT/Research/{YYYY-MM-DD}-{slug}.md`, where `{slug}` is derived from the source (video title, article title, or first 4 words of raw text), kebab-case, max 40 chars.

### 9a. Duplicate defense (before writing)

The vault accumulates Research notes, and the same idea often arrives via multiple sources. Before writing, **Grep `Research/` and `Lessons/` for each surviving idea's key terms** (tool name, technique name, distinctive phrase — one grep with alternation is enough). For each hit, skim the matching note's frontmatter/headings:

- **Same idea, previously accepted/actioned** → do NOT re-present it as new. Record it under `## Prior art` with a wikilink (`covered in [[2026-06-02-crawler-frontiers]] — accepted, no delta`) and count it with the `already_existed` catches in Phase 11.
- **Same idea, previously declined/descoped** → surface the prior decision in Phase 7 ("previously declined in [[note]] because X — reconsider?") instead of presenting it fresh.
- **Related but with a real delta** → keep the finding, add the wikilink under `## Cross-references` naming the delta.

Also grep `$VAULT/Architect/` — a finding that duplicates an open ADR or a queued backlog item belongs to `/architect`, not here; link it and drop it.

Ideally run this check before Phase 7; at the latest, here.

### Frontmatter + body

```markdown
---
date: 2026-07-26
source_type: youtube|article|text
source_url: <url or "pasted">
source_title: "<video/article title>"
focus: all|code|apps|sources
total_extracted: 12
total_after_relevance: 7
accepted: [1, 3, 4]
declined: [2, 5, 6, 7]
buckets: { code: 4, app: 2, source: 1 }
commits: [<sha1>, <sha2>]
web_augmentations:        # Phase 2.5 — omit if the phase did not run
  - { name: "CrateName", url: "https://docs.rs/...", kind: "crate" }
---

# {Source title}

**Source:** [{title}]({url})
**Run:** {timestamp}

## Summary
{2-3 sentence overview of what this source covered}

## Extracted Ideas

### [1] {title}  ✅ accepted → {action taken}
**Bucket:** code
**Layer:** core
**Source anchor:** "{quote}" / [HH:MM:SS]
**Evidence:** `crates/core/src/fetcher.rs:485`
**Validation:** just test ✓ (212) / live job run: readable → succeeded
**Notes:** {anything from triage}

### [2] {title}  ❌ declined
**Bucket:** app
**Source anchor:** ...
**Evidence:** ...
**Decline reason:** _to be filled in Phase 10_

...

## Prior art
- {already-existed catches with wikilinks}

## Cross-references
- Related patterns: [[Patterns/user-preferences]]
- Prior runs touching the same area: {wikilinks}
```

---

## Phase 10: Self-reflection (the learning loop)

This phase makes the skill smarter over time. Do not skip it.

### 10a. Ask why

For declined findings, ask **once**, batched:
```
Help me improve. For these declined items, why did you skip them?

  [2] {title}
  [5] {title}

You can answer per-item ("2: too vague, 5: already planned") or with a
single reason that covers all of them. Type "skip" to move on.
```

If the user types `skip`, jump to 10c.

### 10b. Append to Lessons

Write/append `$VAULT/Lessons/{YYYY-MM-DD}-research.md`. **Use `Edit` to append, never `Write` to replace** — the file is shared by date across runs and across sibling skills writing into the same directory. If the file doesn't exist yet, `Write` is acceptable but must still be preceded by an existence check.

```markdown
## Run: {timestamp} — {source title}

Source: {url}
Accepted: [1, 3, 4]
Declined: [2, 5, 6, 7]

### Decline reasons
- [2] {reason}
- [5] {reason}

### Self-reflection
- What I extracted that resonated: {pattern}
- What I extracted that didn't: {pattern}
- Tools I should use more / less next time: {observation}
```

### 10c. Update the Research note
Backfill the Phase 9 note with the decline reasons.

### 10d. Pattern promotion check
Read all files in `Lessons/`. If the same decline reason (or a close synonym) has appeared in **3+** runs, propose adding it to `Patterns/user-preferences.md`: show the proposed rule and ask "I've seen this 3+ times — promote to permanent rule?" If yes, append with the date and source-run links.

### 10e. Durable-facts update check

Did this run discover a **structural fact about the codebase** that future runs would need? Examples:
- A misreading the user corrected (e.g. the catalog-vs-registry distinction)
- A crate, seam, or capability the skill didn't know existed
- An architectural boundary that determines where findings should be routed (the layer-routing rule)
- An invariant that affects threat assessment or the deliberate-trades list

If yes, **edit `docs/harness/harness-learnings.md`** with the new fact, tagged with the run date and source. That file is the repo's durable structural reference and is committed, so the correction reaches every future session and every sibling skill — not just `/research`. (This is a repo file, so it goes into the Phase 13 commit.)

If the fact is about *user preference* rather than the codebase, it belongs in `Patterns/user-preferences.md` instead (10d).

### 10f. Descoped-but-reopenable tracking

For each finding that was **descoped** (not declined, not accepted — blocked by something external), record it in `$VAULT/Patterns/descoped-reopenable.md`:

```markdown
### {YYYY-MM-DD} — {finding title}
- **Source run:** {research note wikilink}
- **Original descope reason:** {verbatim quote from the user or self-assessment}
- **Blocker:** {what needs to change for this to become viable}
- **Reconsider trigger:** {concrete signal — e.g. "a pure-Rust TLS impersonation crate ships", "the publisher exposes an official API", "the ARM64 toolchain lands"}
- **Related findings:** {wikilinks}
```

**When to add:** the decline reason names a specific external blocker. pumper's canonical case is the README's own note — TLS/JA4 fingerprint impersonation needs BoringSSL, which won't build on this ARM64 Windows box. Anything blocked that way is a textbook descoped-reopenable entry, not a rejection.

**When NOT to add:** descopes based on priority ("not now"), scope ("too big"), or permanent preference ("we don't want that pattern"). Those belong in Lessons or user-preferences.

**Cross-check on future runs (Phase 3):** check each entry's reconsider trigger against the current source; if the source describes a solution to the blocker, surface the entry in Phase 7 as a revived candidate.

**Cleanup:** when an entry is eventually accepted and actioned, remove it (or move it to a "resolved" section with the run date and commit).

---

## Phase 11: Final summary

Print:
```
Research run complete.

  Source:       {title} ({source_type})
  Extracted:    {N} ideas
  After filter: {M} relevant
  Accepted:     {K} ({list})
  Declined:     {L} ({list})

  Already existed:     {A} (caught by the host-first rule — see list)
  Descoped-reopenable: {D} (tracked in Patterns/descoped-reopenable.md)

  Actions taken:
    - Code findings executed: {N} ({crates touched})
    - New app crates scaffolded: {N} ({names}) — registered + catalogued + documented?
    - New catalog rows added: {N} ({ids})
    - Handoffs written: {N} ({paths})
    - Findings logged only: {N}

  Validation:   just check ✓ / just test ✓ (N) / just lint ✓ / just fmt-check ✓
                live job run: {app} → succeeded | not run ({reason})
  Docs sync:    {docs/features/*.md updated | not required (internal-only)}
  Context map:  {updated | unchanged}

  Already-existed catches:
    {one line each: "{candidate title} → already at {file:line}"}

  Files updated:
    + {VAULT}/Research/{date}-{slug}.md
    + {VAULT}/Lessons/{date}-research.md
    {if handoff written:} + docs/harness/handoffs/{date}-{slug}.md
    {if pattern promoted:} ~ {VAULT}/Patterns/user-preferences.md
    {if descoped entry added:} ~ {VAULT}/Patterns/descoped-reopenable.md
    {if 10e fired:} ~ docs/harness/harness-learnings.md

  Source-type yield:  {expected vs actual — see the Phase 3 calibration table}
  Snapshot freshness: {fresh | stale by N commits — consider a context-map refresh}
  Cache:              {cleaned | n/a (Phase 2b/c source) | residue at .research-cache/<id>.*}
  Commit: {filled in by Phase 13 — short SHA + subject, or skip reason}
```

**Surface `already_existed` prominently when the finding count is low.** A competitor-demo run that extracts 2 findings + 8 catches is a high-yield run — frame it that way. Do not let the user read "only 2 findings" as a failure when the real output is "8 existing capabilities confirmed + 2 real gaps found".

---

## Phase 12: Docs & catalog sync

After Phase 11, make sure the run's accepted work is reflected in the surfaces other agents read. This is pumper's equivalent of a user-facing release log: the consumers are the CLI agents and apps on this machine, and `docs/features/` + `catalog/data-sources.toml` are what they read literally.

**Skip the phase entirely** if zero findings were accepted, or if every accepted finding was internal-only.

### 12a. Feature docs
For each accepted finding marked `docs: required` in Phase 6:
1. Open `scripts/docs/feature-doc-map.json`, find the doc coupled to the source glob you changed.
2. Update it in the **same session** (the Stop hook checks and will otherwise nag). Describe what IS — the surface, the params, the data model, the known gaps. Keep future-looking ideas out; those belong in the vault backlog.
3. If the change created a **new feature area** (a new crate, a new app, a new server module), add a map entry **and** its feature doc in the same change. A new area with no map entry is invisible to the hook forever.

### 12b. Catalog
If any accepted finding added an app or a source, or changed a source's reality (a different engine tier, a changed cadence, a portal that died):
1. Update the `[[source]]` row in `catalog/data-sources.toml` — `app`, `engine`, `access`, `cadence`, `cron`, `status`, `confidence`.
2. Verify the enums against `catalog/README.md` rather than guessing.
3. Sanity-check parity: every `status = "live"` row must correspond to an app in `crates/server/src/registry.rs`, and vice versa.

### 12c. Context map
If the run added, moved, or removed source files, update the owning context's `filePaths` in `context-map.json` (per `.claude/CLAUDE.md`). A new app crate needs a home in one of the app groups.

### 12d. Confirm
```
Docs & catalog sync:
  - docs/features/{files}       ({N} updated)
  - scripts/docs/feature-doc-map.json  ({new entry | unchanged})
  - catalog/data-sources.toml   ({N} rows added/changed)
  - context-map.json            ({updated | unchanged})
```
If nothing was required, print `Docs & catalog: no user-visible change — sync not required.` and say so in one sentence when dismissing the Stop hook.

---

## Phase 13: Atomic commit (MANDATORY)

Every research run commits its own output at the end, so git is the recovery mechanism when anything else fails. This phase is **non-negotiable** except in the two explicit skip conditions below.

### 13a. Determine if there are changes to commit
`git status --porcelain`. If **empty**, skip Phase 13 entirely and print `No changes to commit.` in the final summary.

### 13b. Review what will be committed

`git status` and `git diff --stat`. **Look for unexpected files** — this checkout is shared with concurrent sessions, so drift you didn't author is expected, not alarming, and must NOT be swept in.

- **Expected scope for a research run:**
  - Files touched by accepted findings (`crates/**`, `clients/**`)
  - New app crate files + `Cargo.toml` + `crates/server/Cargo.toml` + `crates/server/src/registry.rs`
  - `catalog/data-sources.toml`, `docs/features/*.md`, `scripts/docs/feature-doc-map.json`, `context-map.json`
  - `docs/harness/harness-learnings.md` (if Phase 10e fired), `docs/harness/handoffs/*` (if a handoff was written)
  - The vault, **only if** it lives inside the repo at `.perfect/` — it is committed by default. If the vault resolved to the Obsidian path outside the repo, it won't appear in git status at all.
- **Unexpected — do not stage:**
  - Anything under `target/`, `data/`, `plugins-src/*/target/`
  - `.env`, `config.toml` if it now holds a real secret
  - `.research-cache/*` (Phase 2a should have removed it)
  - Files in areas unrelated to any accepted finding — those are another session's in-flight work

If unexpected files are present, **print them and ask** whether to include them. Don't auto-include anything suspicious, and never revert or stash another session's work.

### 13c. Stage only the in-scope files

**Explicit `git add <path>` per file, in ONE bash invocation with the verification**, because a concurrent session can rewrite the index between two separate calls:

```bash
git add crates/engine-http/src/lib.rs docs/features/fetching.md && git diff --cached --stat
```

Never `git add -A`, `git add .`, or `git add -u`. If the cached stat lists **more files than you added**, the index held another session's pre-staged work — `git restore --staged <path>` each unrelated file (this never touches the working tree), re-verify, THEN commit.

On `index.lock`: wait 3–10s and retry, up to 6 times. Expect HEAD to advance mid-run.

### 13d. Write the commit message

```bash
git commit -m "$(cat <<'EOF'
research: {short-title-of-source}

Source: {url-or-pasted}
Accepted: {N} finding(s) ({comma-separated-titles})

{optional 1-2 line summary of what was implemented}

{if a new app was scaffolded:}
App: crates/apps/{name} — registered, catalogued, documented

{if catalog rows changed:}
Catalog: {ids}

{if a handoff was written:}
Handoff: docs/harness/handoffs/{date}-{slug}.md

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

**Rules:**
- First line prefix **must be `research:`** — makes research commits filterable in `git log`, alongside this repo's `architect:`, `explorer:`, `perfect:`, `tiger:`, and `passport:` prefixes.
- Short title = the source title trimmed to ≤50 chars, lowercased.
- **Never include file paths** in the body — those are in `git diff`; the message is about *why*.
- **Never `--no-verify`.** If a hook fails, fix the issue, re-stage, and create a NEW commit.
- **Never `--amend`** — another session may already have built on your commit.
- **Never `git push`** unless the user asks.

If the branch is not `master`, do not switch the shared checkout — commit through a temporary worktree pinned to `master` instead.

### 13e. Handle commit failure

1. Print the failure reason.
2. Do NOT retry with `--no-verify`.
3. If fixable (a clippy error in code this run wrote), fix inline and create a new commit.
4. If not fixable in this session, print:
   ```
   ⚠️ Commit failed. Changes are staged but NOT committed.
   Research outputs are safe in the vault, but code changes are vulnerable
   until you commit manually. Run: git commit --message "research: <title>"
   ```
5. Still write the Research note — never sacrifice the learning loop because of a commit failure.

### 13f. Skip conditions

Exactly **two**. Everything else is non-negotiable.

**Skip 1 — No changes:** 13a found an empty `git status --porcelain`.
**Skip 2 — User explicitly opts out:** the user typed `--no-commit`, `no commit`, or `skip commit`. Print:
```
⚠️ Skipping commit per user request.
Changes are uncommitted and vulnerable until you commit manually.
```

**NOT a skip condition:** "I'll commit manually later." The whole point of Phase 13 is to make the commit happen in-session before context is lost — and in a checkout where four other skills commit concurrently, uncommitted work is the most fragile state there is.

### 13g. Update the Phase 11 summary

Append a `Commit:` line and re-print the summary so it stays canonical:
```
  Commit: {short-sha} — research: {short-title}
           | skipped (no changes)
           | skipped (user opted out)
           | ⚠️ commit failed — see above
```

---

## Error handling

| Failure | Response |
|---|---|
| `context-map.json` missing | Stop. Tell the user to run the Vibeman refresh. |
| `docs/harness/harness-learnings.md` missing | Warn; continue with `ONBOARDING.md` + `README.md`; note the degradation. |
| `catalog/data-sources.toml` missing and focus needs it | Stop — no dedup surface for buckets B/C. |
| `yt-dlp` missing | Stop with install instructions. |
| YouTube has no auto-subs | Ask for a manual transcript paste or an alternate source. |
| `WebFetch` returns paywall / 403 | Ask the user to paste the article text. |
| Source text <300 words | Report insufficient content. Stop. |
| Fewer than 2 ideas survive Phase 4 | Report "no relevant ideas for pumper in this source." Still write a stub Research note so the source isn't re-harvested. |
| Vault path missing | Run Phase 0 bootstrap; don't fail. |
| `cargo test --workspace` fails on code this run didn't touch | Do not "fix" it silently — it is likely another session's in-flight work. Report it, and validate your own change with a targeted `cargo test -p <crate>` instead. |
| A crate won't build on this platform | Descoped-reopenable (10f), not declined. The BoringSSL case is the precedent. |
| Phase 13 commit fails | Fix inline and re-commit. If unfixable, print the 13e warning and leave changes staged. Never `--no-verify`. |
| Phase 13 detects unexpected files | Ask before staging. Never auto-include, never revert another session's work. |

---

## Safety rules

- **Execute accepted findings; never edit code the user didn't accept.** Phase 8's triage answer is the consent boundary.
- **Never break the dependency rule** (`apps → core ← engines`; only `server` depends on everything) to make a finding fit. If the finding requires breaking it, the finding is mis-anchored — re-route it in Phase 6 Step 1c.
- **Additive-first.** A new optional field or a trait method with a default beats a breaking `core` change. If `core` must break, update every impl in the same change and run `just test`.
- **Bug fixes ship as extracted, tested functions** (`.claude/CLAUDE.md`): extract the predicate/transform into a named function, add a test named after the anti-pattern it defends (`x_not_y`), then wire it into the call site. A fix inline in a `run()` body is an unguarded fix.
- **Never file the deliberate trades as findings.** `ONBOARDING.md` §2 declares no-auth, permissive CORS, `--dangerously-skip-permissions`, on-disk cookies, and returned non-2xx bodies as intentional. Departing from one is a question for the user, not a routine finding.
- **Never skip Phase 10** unless the user typed `skip` — the learning loop is the whole point.
- **Never leave a scrape change unverified in silence.** Either run a real job or state plainly that you didn't and why.
- **Phase 12 is the only place** the skill writes to `catalog/data-sources.toml` and `scripts/docs/feature-doc-map.json`. Never write catalog rows the user didn't accept.
- **Phase 13 is mandatory** except for the two skip conditions. Git is the recovery mechanism in a shared checkout.
- **Phase 13 stages files explicitly**, in one bash invocation with `git diff --cached --stat` verification. Never `git add -A`/`.`/`-u`.
- **Phase 13 never bypasses hooks and never amends.**
- **Phase 2a cache cleanup is mandatory**, scoped to this run's id, before Phase 3 — never a blind sweep of `.research-cache/` (that races with parallel runs). Phase 11 reports `Cache: cleaned` or the residue path.
- **Forbidden at all times:** `git stash`, `git reset --hard`, `git clean -f`, `git checkout --` / `git restore` on working-tree paths this run didn't author, `git commit --amend`, `git push` without being asked.

---

## Inherited rule rationale

These rules were each added after a specific failure in the skill's original (personas) lineage. They are kept because the failure mode is structural, not project-specific. When a rule looks redundant on a future read, check here before removing it — the reason usually still applies, and the pumper analogue is named alongside.

- **Phase 6 host-infrastructure-first ordering.** A single second-order grep once reframed 4 of 7 findings from "build new infrastructure" to "extend what exists". Without it, anchors and effort estimates are wrong. Cost: one grep. In pumper the payoff is larger, because `core` already contains a governor, a cache, a crawler, an extraction engine, and a dataset store that findings routinely propose to reinvent.
- **Phase 6 security escalation rule.** A grep for auth patterns against an exposed surface once returned "no matches" — *and that was the finding*. The source never mentioned security; the codebase reality made it the most important item. The pumper adaptation adds an explicit exclusion list, because here several "missing defenses" are documented deliberate trades and escalating them would be noise.
- **Phase 6 catalog-vs-runtime check.** An architectural critique was once built on the wrong denominator (catalog count treated as per-execution count). The pumper analogue is exact: workspace crates ≠ concurrent apps, catalog rows ≠ registered apps, engines compiled ≠ engines used.
- **Phase 6 layer routing (Step 1c).** Two findings were once anchored in the wrong module because the skill didn't know the framework/plugin boundary. In pumper the equivalent boundary is the dependency rule, and violating it is worse than a mis-anchored finding — it breaks other agents' in-flight work.
- **Phase 6 Step 3b "read the signature before proposing reuse."** A finding framed as "reuse existing function X" was wrong: X's signature made it semantically incapable of the job. Reading the signature reshaped the work from a trivial hook into a new module.
- **Phase 7 cluster detection.** Findings that all landed in one file were presented flat, and the user had to re-cluster them manually. Automatic detection moved that work upstream.
- **Phase 8 in-session execution as the default.** A morning handoff → evening amendment → next-session execution cycle turned one session of work into three, and a handoff-era session's uncommitted code was once lost to a bad merge. Handoffs are now the fallback, with explicit escape criteria.
- **Phase 8 Option B-Design.** "Propose approaches first" is a real, recurring shape with no clear anchor; improvising it produced a different structure every time, and stopping at the design doc fragmented the work. Labeling it made the pattern reusable and made execution the default continuation.
- **Phase 8 Option C (theoretical scaffolding).** A finding once depended on a whitelist-gated product with no public API. Stubs + `TODO(marker)` breadcrumbs + strict non-goals captured the intent in compilable code instead of a prose analysis. pumper's standing case is the platform-blocked TLS-impersonation work named in the README.
- **Discovery briefs demoted.** Only one of three attempted analysis-only documents ever survived triage; users prefer concrete plans, even stubs. Not offered as a first-class option.
- **Phase 3 source-type yield calibration.** A run producing 2 findings + 8 catches read as a failure until the source type was considered; it was a high-signal run. The table exists so a run can self-assess before the user has to.
- **Phase 10b Edit-not-Write for shared-by-date Lessons files.** Three runs in one day, and the third overwrote the first two's Lessons blocks. Any vault path named `YYYY-MM-DD-*` without a per-run slug is shared and must be appended via `Edit`. Research notes carry a slug, so `Write` remains safe there.
- **Phase 10e durable-facts update.** Two consecutive runs discovered structural facts the skill needed but didn't have; capturing them in a committed reference prevents the same misframe next time. Routed here to `docs/harness/harness-learnings.md` so every sibling skill benefits, not just `/research`.
- **Phase 10f descoped-but-reopenable.** A finding was descoped for a hard external blocker; two runs later the blocker had been solved externally and nobody noticed. The cost of an unused entry is small; the cost of missing a reopen is a silently-lost finding.
- **Phase 13 atomic commit.** A bad merge once wiped an entire session's uncommitted work, recovered only by re-typing from the transcript. Explicit per-file staging, no `--no-verify`, no `--amend`, and a commit-SHA line in the summary are all consequences of that day. In pumper the risk is higher, not lower — four sibling skills commit into this same working tree.
- **Phase 2a scoped cache cleanup.** Twenty stray cache files accumulated over fourteen runs from a rule buried in one sentence. It is now a labeled block with an explicit scoped `rm`, and Phase 11 reports the outcome so an omission is visible.
- **Phase 2.5 web augmentation.** Sources name tools without explaining them, leaving Phase 6 unable to grep for the right concept. Bounded at 3 web calls so it sharpens framing without becoming a second research run.

## pumper run log

_(Append one block per notable run: what the source type was, what the yield was, and any rule this skill should change as a result. Keep it short — the durable structural facts go to `docs/harness/harness-learnings.md`, the user preferences to `Patterns/user-preferences.md`, and this section holds only changes to the skill itself.)_

- _No pumper-specific iterations yet — the skill was vendored and adapted on 2026-07-26._
