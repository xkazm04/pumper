---
name: perfect
description: Session-after-session product perfection loop for pumper. The strongest available model (Fable) directs — it walks context-map.json context-by-context, proposes 5 challenged, high-value directions per context (features, API/data-product elevations, significant optimizations), gates them with the user until 10 are accepted, then orchestrates builder subagents (Sonnet for routine S/M work, Opus for large or risk-flagged briefs) per context in isolated worktrees while making every review/merge decision itself. All state lives in the repo's .perfect/ vault so any future session resumes the loop exactly where the last one stopped. Invoke with `/perfect [init|propose|build|status|reflect] [context-name]`.
argument-hint: "[context]"
---

# Perfect — the direction-and-delivery loop (pumper edition)

> One model is best at *judgment* — seeing what would make a product excellent, challenging its own ideas, reviewing diffs ruthlessly. Cheaper strong models are great at *execution* inside a well-scoped brief. `/perfect` wires them together in a permanent loop: **Fable directs, the right-sized builder builds, the vault remembers.** Each session moves the product measurably closer to the best API surface, data quality, and architecture it can have; no session ever starts from zero.

## The product, in one line

pumper is a Rust scraping/data-product service: a tiered fetch runtime (http → browser → Claude), declarative extraction, a dataset store with change detection, embedded search, a job server (axum) with cron/triggers/webhooks, and a fleet of `ScrapeApp` crates producing domain datasets (grants, labour markets, trades economics). Its users are API consumers and CLI agents — "UX" here means API ergonomics, dataset quality, observability, and operational robustness, not pixels.

## Roles — Director and Builders

- **Director (the main session — Fable, or the strongest model available).** Owns everything that is judgment: opportunity-scoring contexts, drafting directions, adversarially challenging them before the user ever sees them, running the acceptance gate, writing builder briefs, answering builders' product questions mid-flight, reviewing every diff, deciding merge/redo/drop, running the repo gates, committing, and writing the vault. The Director **never delegates a decision** to a builder and never rubber-stamps a builder's diff.
- **Builders (subagents, one per context per wave; model chosen per brief — see *Builder model selection* below).** Each receives a tight brief (direction specs + acceptance criteria + the context's `filePaths` scope + repo-convention digest) and implements in its **own worktree**. Builders return a structured report; when they hit a genuine product ambiguity they **return the question instead of guessing** — the Director answers via `SendMessage` and the builder continues. The brief is identical regardless of model — a right-sized brief is what makes a cheaper model safe.
- **Scouts (Explore subagents, cheap).** Produce the per-context current-state brief the Director synthesizes directions from. Never used for judgment.

## The vault — durable loop state

Resolve the vault root (first hit wins), then use `$VAULT/Perfect/`:

```bash
for v in "C:/Users/mkdol/Documents/Obsidian/pumper" "C:/Users/mkdol/dolla/pumper/.perfect"; do
  [ -d "$v" ] && VAULT="$v" && break
done
# First run: neither exists → create C:/Users/mkdol/dolla/pumper/.perfect (Obsidian-openable folder; add `.perfect/` to .gitignore only if the user asks — default is COMMITTED so the loop state travels with the repo).
```

```
Perfect/
  Perfect.md               # HOME / Map-of-Content — always reflects current truth:
                           #   mission, the scored context QUEUE with the CURSOR,
                           #   the ACCEPTED POOL (n/10), shipped ledger headline, link to last session
  config.md                # per-repo overlay: gates to run, worktree recipe, wave size,
                           #   direction sizing rules, cooldown, ## User taste, + ## Skill improvement log
  contexts/<name>.md       # one per context-map context (long-lived, updated in place)
  directions/<slug>.md     # one per direction (long-lived; the atom of the whole loop)
  sessions/<YYYY-MM-DD[-n]>.md  # immutable run records, each ends with a `next:` pointer
```

**Context note** (`contexts/<name>.md`):
```markdown
---
name: <context-map name>        type: perfect/context
group: <group>                  category: api|lib|data|config
opportunity: <0-10>             # value reach × headroom × strategic fit (Director's judgment)
last_proposed: <YYYY-MM-DD|never>   cooldown_until: <date|—>
directions: ["[[<slug>]]", …]
---
## Current state   (scout brief digest + file:line evidence — refreshed each proposal pass)
## Direction history   (proposed / accepted / REJECTED-and-why — rejections are memory too)
## Shipped   (direction → commit SHA → observed effect)
```

**Direction note** (`directions/<slug>.md`):
```markdown
---
slug: <kebab, stable>           type: perfect/direction
context: "[[<context-name>]]"   lens: feature|api-ux|optimization|robustness|wildcard
status: proposed | accepted | building | shipped | failed | dropped | rejected
size: S|M|L                     # must fit ONE builder session (≲15 files, no cross-context schema break)
proposed: <date>  accepted: <date|—>  shipped: <date|—>  commit: <sha|—>
---
## What & why   (the user value, one paragraph, no fluff)
## Evidence   (file:line of the gap/opportunity in today's code)
## Acceptance criteria   (3-6 checkable bullets — the builder's contract AND the review checklist)
## Risks / non-goals
## Build record   (built by: sonnet|opus · builder report digest · review verdict · gate results — filled during build)
```

**Session note**: phases run, contexts covered, accept/reject tallies, build outcomes with SHAs, deltas, and **`next: <the exact resumption instruction for the following session>`**.

Vault hygiene: slugs are stable; **update notes, never duplicate**. Subagents may fail to write files in some harnesses — after any parallel phase the Director MUST `ls` the target dir and **backfill missing notes from the agents' returned content** before trusting "written".

## The loop — a vault-driven state machine

Every invocation starts the same way; the vault decides which phase runs.

### Phase 0 — Recall & register
1. Read `Perfect.md` (+ last session's `next:` pointer). If missing → run **init** (below).
2. Read `context-map.json`; diff against `contexts/*` — new contexts get notes + a queue slot, removed ones get archived (`status: retired` in frontmatter).
3. Repo rituals: read `docs/harness/harness-learnings.md` (structural facts, anti-patterns, **Open follow-ups** — follow-ups are pre-vetted direction seeds). Scan Claude memory (MEMORY.md) for signals that veto directions (e.g. "removed — don't re-suggest"). Note that `docs/harness/vision-scan-2026-07-10/` records prior waves — anything shipped there is NOT novel.
4. Announce the resumption point in one sentence, then go where the state machine points: pool < 10 → **Propose**; pool ≥ 10 (or user said `build`) → **Build**.

### Init (first run only)
1. Scaffold the vault tree + `config.md` (record: gates = `cargo check --workspace` and `cargo test --workspace --lib` (fast, no network); clippy calibration = *no NEW warnings in files the diff touched* — never `-D warnings` full-crate; wave size = 3; cooldown = 2 rounds).
2. Score every context 0-10 for **opportunity** = consumer-facing reach × headroom (distance from "perfect", judged from context-map metadata, `docs/features/*`, harness-learnings) × strategic fit (active arcs: data-product quality, trigger/pipeline maturity, API ergonomics). Write the ranked **queue** into `Perfect.md` with the cursor at the top. Don't deep-read code yet — scoring is refined per-context at proposal time.
3. Write session note; proceed straight into Propose.

### Phase P — Propose (context by context, until the pool holds 10)
Loop while `pool < 10` and the user hasn't said stop:

1. **Cursor** = highest-opportunity context not on cooldown. **Prefetch**: before presenting context *k*, launch the scout for context *k+1* in the background.
2. **Scout** (Explore, "very thorough", read-only): given the context's `filePaths` (+ its migrations, routes, and `docs/features/*` doc) → return a current-state brief: what exists, what's rough, dead ends, API seams, perf smells, data-quality gaps, with `file:line` evidence.
3. **Draft 5 directions** — one per lens by default: **feature** (new consumer value: endpoint, dataset, capability), **api-ux** (API ergonomics / dataset shape / docs-contract elevation), **optimization** (perf/cost/significant simplification), **robustness** (failure modes, observability, architecture), **wildcard** (the non-obvious idea a great PM would pitch). Each sized to ONE builder session; a bigger vision ships as its phase-1 slice.
   **Weight the slate by `config.md → ## User taste`** — the lens spread is a starting point, not a quota. Default depth is the *engine*, not the chrome: for any context with backend/algorithmic substance, most directions should be architecture-level (data model, algorithms, job lifecycle, fetch/extract paths, cost structure); surface polish appears at most once-twice unless the user steers otherwise. Scout prompts must match this depth (trace the full pipeline, not just the components).
4. **Challenge before presenting** (the Director argues against itself; a direction that fails any check is replaced, not presented):
   - Does it already exist in code? (scout evidence, not assumption)
   - Was it already proposed/rejected/shipped? (check `contexts/<name>.md` history + harness-learnings + vision-scan wave summaries)
   - Does it conflict with an active arc or a "removed / explicit non-goal" record (e.g. trigger fan-in barriers are a documented non-goal)?
   - Is the value claim concrete — can I name the consumer moment it improves?
   - Can ONE builder session genuinely ship it behind the acceptance criteria? (Size it S/M/L honestly here — the size + the nature of the criteria are what pick the builder's model at build time, so a sloppy size is a real cost mistake, not just bookkeeping.)
5. **Present** the 5 in chat — numbered, each: title · lens · size · one-paragraph why · evidence · acceptance criteria. Then gate with **AskUserQuestion (multiSelect)** — the tool caps options at 4 per question, so use TWO questions in one call: Q1 = directions 1–3, Q2 = directions 4–5 (labels = `N · short title`, description = one-line value claim + size). The user can annotate via "Other" (e.g. `edit 2: …`, `stop`); selecting nothing in both = none accepted.
6. Record outcomes in the vault (rejected ones too, with the user's implied reason — rejections steer future proposals). Accepted → `directions/<slug>.md` with `status: accepted`, pool counter++, context gets `cooldown_until`. Update `Perfect.md` after every context, not at session end — a killed session must lose nothing.
7. **A `none` gate that carries a steer** (the user says what they wanted instead) is a re-scout order, not a rejection of the context: promote the steer to `config.md → ## User taste` if it generalizes, re-scout at the steered depth/angle, and re-propose the SAME context once before advancing the cursor. Never re-present any rejected direction.

### Phase B — Build (right-sized builder per context, Fable decides everything)
1. **Wave plan**: group the pool's accepted directions by context → one builder per context, ≤ `config.wave_size` (default 3) concurrent, and **≤ 3 directions per builder brief** (a 4-direction brief exceeds one agent-session budget — split a bigger context into two sequential builders). **Assign a model to each brief (below)** and show it in the wave plan (`E1 · Fetch Engines · 2 dirs (M,M) · sonnet`) so the user sees the cost shape before go. Present the plan in one screen; on user go (or when invoked as `/perfect build`), execute.

   #### Builder model selection — Sonnet by default, Opus when the work earns it
   The unit of choice is the **brief**, not the direction: a brief runs on Opus if ANY of its directions trips a trigger.

   **Default → `model: "sonnet"`.** A brief whose directions are all **S** or **M**, with concrete acceptance criteria, inside one context's files. This is most work: adding config keys + threading a field, a new endpoint following an existing convention, counters/metrics/reporting, parsing fixes with unit tests, mechanical migrations of call sites.

   **Escalate to `model: "opus"`** when the brief contains any of:
   - a direction sized **L**, or 3 directions that each rewrite a core file;
   - **concurrency / correctness-critical** work — locking, cancellation, attempt fencing, cache coherence, crash recovery, anything where a subtle bug is silent (round 2's attempt-fenced job writes; round 3's session-vault cache-bypass catch);
   - a **new public seam other contexts will build on** — a trait, a core data structure, a cross-crate contract, a new crate;
   - an acceptance criterion that hands the builder a **design decision** ("design the trace shape yourself", "choose the seam and justify it") rather than a spec to implement;
   - a **schema/migration change** that other contexts read, or an algorithmic rewrite whose correctness must be *proven* (round 2's banded SimHash equivalence);
   - a **redo** after a Sonnet builder returned a diff the Director rejected on correctness (never re-run the same brief on the same model — escalate).

   **Honest bookkeeping:** record the model in each direction's `## Build record` (`built by: sonnet`). If a Sonnet brief is escalated mid-flight or its diff is rejected on review, log it in `config.md → ## Model policy` with what the trigger *should* have been — that log is how the trigger list gets sharper. The Director's review bar does NOT change with the model: a Sonnet diff gets read exactly as ruthlessly as an Opus one.
2. **Worktree per builder** — prepared by the Director, NOT via Agent-tool isolation. **Each concurrent builder gets its OWN target dir** — a shared `target/` across concurrent agent sessions produces stale-rlib linkage failures (round 2 lost time to this; round 3's per-builder dirs had zero incidents). The cold first build is worth it.
   ```bash
   git worktree add .claude/worktrees/perfect-<ctx> -b worktree-perfect-<ctx>
   # Builders run all cargo commands with: CARGO_TARGET_DIR=<repo>/target-<ctx>   (their own; NOT the shared target/)
   # Builders needing a live server use their own DB/port: copy config.toml, point [storage] at a scratch path, change the port.
   # At Wrap: rm -rf target-<ctx> for every builder dir.
   ```
   **Sequential > parallel within one context.** When a context has >3 directions, run its builders in waves (B1 → merge → B2 on a `reset --hard master` worktree) so the later builder BUILDS ON the merged earlier work instead of duplicating it (round 3: E3 generalized E2's client pool rather than adding a second pool). Parallelize ACROSS contexts, serialize WITHIN one.
3. **Brief** each builder (see template below); launch with `subagent_type: "general-purpose"` and the **model chosen in step 1** (`model: "sonnet"` | `"opus"`), all briefs of a wave in one message so they run concurrently.
4. **Mid-flight decisions**: a builder returning `DECISION NEEDED: …` gets an answer from the Director via `SendMessage` — product calls, trade-offs, and scope cuts are Fable's alone. A builder that stops without its final report gets one `SendMessage` nudge.
   **Builder-death recovery (session limits WILL kill builders):** the instant a builder dies, `git add -A && git commit --no-verify` a `wip(…)` snapshot **inside its worktree** (isolated tree — add-all is safe there; never-lose-work beats commit hygiene). Then the Director either finishes the work inline (review the WIP diff, complete gaps, split into per-direction commits along file boundaries — same-file hunks may share a commit if the message says so) or re-briefs a fresh builder after the limit resets with "continue from the WIP commit".
5. **Review — the Director earns its title here.** Per builder branch: `git diff master...worktree-perfect-<ctx>` and review against each direction's acceptance criteria, repo conventions (harness-learnings: `ts()` timestamps, stable record keys, compact job results + artifacts, `webhook::dispatch_event` for any event, cursor pagination convention, `#[serde(default)]` + manual `Default` on config structs), and taste. Verdict per direction: **merge** / **redo with notes** (SendMessage, builder fixes in place) / **drop** (`status: failed`, reason recorded). Never merge on "tests pass" alone — read the diff.
   **Docs-vs-code check:** when a diff documents a behavior (contract text, formula, doc comment, `docs/features/*` claim), grep for the code that implements it before merging — a contract describing behavior the code doesn't have is worse than nothing.
   **Migration check:** any new migration must be append-only (next number in `crates/core/migrations/`), and any new dataset/app must be registered end-to-end (workspace dep in root `Cargo.toml`, `crates/server/Cargo.toml`, `registry.rs`).
6. **Merge serially**: per direction, `git merge --squash` (or cherry-pick) → ONE atomic commit on master, message `feat(<context>): <direction title>` + `Co-Authored-By` footer. Stage per-file, verify `git diff --cached --stat` matches intent (foreign pre-staged files → `git restore --staged` them). Run the config gates on master after each merge; a red gate is fixed inline before the next merge.
7. **Doc-sync in the same turn**: consumer-visible changes update the mapped `docs/features/*` (see `scripts/docs/feature-doc-map.json`) — the Stop hook (`scripts/docs/check-doc-sync.mjs`) will demand it anyway. New feature area ⇒ new map entry + feature doc in the same change.
8. **Cleanup**: per worktree — `git worktree remove .claude/worktrees/perfect-<ctx>`, then delete the branch once its commits are on master.

### Phase W — Wrap (every session, even interrupted ones)
1. Update every touched vault note; write the session note with the **`next:` pointer** (e.g. `next: propose — cursor at http-api-routes, pool 7/10` or `next: build wave 2 — triggers + datasets remain`).
2. `Perfect.md` headline refreshed: pool count, queue cursor, shipped-total, last-session link.
3. Append durable structural learnings to `docs/harness/harness-learnings.md` (same style as existing entries, dated).
4. **Reflect on the skill itself**: 2-4 bullets in `config.md → ## Skill improvement log` — what dragged, what the user overrode, what the next round should change. This log is the input for the between-rounds skill revision.

## Direction quality bar (what earns a slot in the 5)

- **Value-first**: names the consumer moment it improves; "nice refactor" is not a direction unless it unlocks something.
- **Evidence-backed**: cites today's code (`file:line`), not vibes.
- **One-session-shippable**: ≲15 files, no cross-context schema breaks; else slice it.
- **Novel to the vault**: not shipped, not pending, not previously rejected, not already delivered by a vision-scan wave (unless the world changed — say so).
- **Lens-diverse**: default one per lens; substituting a second entry in one lens requires the Director to say why.

## Builder brief template

The brief is the same whatever model runs it — right-sizing the brief is what makes a cheaper model safe.

```
You are a builder for the `<context>` context of pumper, a Rust scraping/data-product
service (tokio + axum + sqlx/SQLite; apps are `ScrapeApp` trait crates under crates/apps/*).
Work ONLY in this worktree: <abs path>. Run every cargo command with
CARGO_TARGET_DIR=C:/Users/mkdol/dolla/pumper/target (shared build cache).
Your scope is this context's files: <filePaths from context-map.json>.
Touching other contexts requires DECISION NEEDED.

Implement these accepted directions, one atomic commit each, message `feat(<context>): <title>`:
<per direction: What & why · Acceptance criteria · Evidence file:line · Risks/non-goals>

COMMIT EACH DIRECTION THE MOMENT IT IS DONE AND VERIFIED — never batch commits
for the end of the session. An interrupted session must lose at most the
direction in progress, not everything.

Repo law (non-negotiable — digest of docs/harness/harness-learnings.md):
- Read docs/harness/harness-learnings.md first; its conventions and anti-patterns bind you.
- **VERIFY the brief's claims before building on them.** The file:line evidence and any external-API field names in this brief come from a read-only scout and MAY BE WRONG (round 3: a scout's guessed API column names did not exist; the builder checked the live API and corrected them). Check the code / the live source first; report any correction in your final report.
- TEXT timestamps: fixed-width RFC 3339 UTC micros via the `ts()` helpers (lexicographic = chronological).
- Record keys are stable external ids; never `sync_many` on filtered/partial scrapes.
- Job results stay compact JSON; large payloads via `ctx.save_artifact`.
- Every outbound webhook/event goes through `webhook::dispatch_event` — never hand-roll a reqwest send.
- New list endpoints follow the `cursor=` keyset pagination convention (`{items, next_cursor}`).
- Config struct fields: `#[serde(default)]` + manual `Default` impl (both, always).
- Migrations: append-only, next number in crates/core/migrations/, sqlx::migrate! picks them up.
- New app crate = workspace dep in root Cargo.toml + crates/server/Cargo.toml + registry.rs line.
- Apps meter LLM/fetch spend via ctx.fetch / ctx.research, not ctx.engines.* directly.
- Verify before claiming done: `cargo check --workspace`, targeted `cargo test -p <crate> --lib`,
  and drive the actual flow (spawn the server on a scratch DB/port, curl the endpoint, run the app
  via POST /jobs) when feasible; report what you COULD NOT verify honestly.
- Consumer-visible change ⇒ update the mapped docs/features/*.md in the SAME commit
  (map: scripts/docs/feature-doc-map.json).

If a product decision is ambiguous, STOP that direction and return `DECISION NEEDED: <question>`
with your recommendation — never guess. Final report format:
per direction → status (done|blocked|decision-needed), commits, files, verification evidence, open risks.
```

## Modes

- **`/perfect`** — resume the loop wherever the vault says it stopped (the default; covers init on first run).
- **`/perfect propose [context]`** — force a proposal pass (optionally jump the cursor to a named context).
- **`/perfect build`** — build now with the current pool even if < 10.
- **`/perfect status`** — read-only: queue, cursor, pool, in-flight builds, shipped ledger, last session. No agents.
- **`/perfect reflect`** — read `config.md → Skill improvement log` + last sessions and propose concrete edits to THIS skill file.

## Guardrails

- **Never stash, never `git add -A` on master** — per-file staging, staged-count check before every commit; other sessions' work is sacred (worktree WIP snapshots are the one exception).
- **Cost discipline**: scouts are Explore-tier; builders are **Sonnet by default and Opus only when a trigger fires** (Phase B step 1) — no model is spent on unaccepted work; the Director never re-runs a scout whose brief is < 1 round old (it's in the context note). Cheaper builders are a reason to build *more*, never a reason to review *less*.
- **Honest ledger**: a direction only reaches `shipped` with gates green AND the Director having read the diff; anything else is `failed` with a reason. No silent drops — every accepted direction's fate is recorded.
- **Interruptibility is a feature**: write the vault incrementally (after every context in P, after every merge in B) so a killed session resumes losslessly.
- **The user is the product owner**: the gate is theirs; the Director challenges but never overrides a rejection, and repeated rejections of a lens/context recalibrate the queue scores.
