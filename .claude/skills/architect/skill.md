---
name: architect
description: Heavy-hitter structural scan of the pumper codebase — parallel Explore agents sweep one theme or area for weak patterns to upgrade and strong patterns to codify, the user triages findings (execute now / queue / drop / rework), and a durable vault backlog of ADRs carries decisions across sessions. Invoke with `/architect [scan|area|resume]`. Pairs with /perfect (product directions); /architect handles structure, not features.
---

# Architect (pumper edition)

Heavy-hitter codebase scan for **structural patterns** — both weak ones to upgrade and strong ones to codify. Designed for rare, deliberate, high-effort sessions where the payoff is a class of bugs eliminated, a tech swap landed, or a convention promoted from "tribal knowledge" to "enforced rule."

Adapted from the personas `/architect` skill. pumper is a Rust workspace (axum job server, tiered fetch engines, declarative extraction, dataset store, `ScrapeApp` crates), so taxonomy comes from `context-map.json`, structural facts from `docs/harness/harness-learnings.md`, and validation is cargo-based. It shares the repo vault with `/perfect` but owns its own `Architect/` subtree.

## Interaction conventions

Built for parallel CLI control — every user prompt is single-keystroke answerable.

- **Every prompt is a numbered menu.** Numeric input picks the option; **Enter** triggers the default; option `1. other → …` is the deviation lane (free text).
- **Every phase output ends with a `Next?` block** of 2–5 numbered next-step actions.
- Multi-finding triages use `<id>=<verdict-number>` syntax (e.g. `1=2 2=1 3=3`); `all=<n>` and `ask` shortcuts are always accepted.
- Long free-text answers are accepted everywhere; the menu just makes the common case instant.
- In the interactive CLI, present menus via AskUserQuestion where it fits (≤4 options) or as printed menus otherwise.

## Input

### Q1 — Mode

```
Mode? (Enter = scan)
  1. scan      — pick a theme, parallel-agent sweep        ← default
  2. area      — bound the sweep to one area (context-map group)
  3. resume    — drain the backlog (skip scanning)
```

`resume` skips the rest of Input — jump straight to Phase 9.

### Q2a — Theme (scan mode)

```
Theme? (Enter = pick for me)
  1. other → describe (free-form theme; angles auto-picked in Phase 3a)
  2. error-handling      — Result/anyhow/thiserror discipline, error surfacing to API + logs
  3. async-patterns      — tokio usage, spawn/join discipline, cancellation, blocking-in-async
  4. trait-boundaries    — engine capability traits, ScrapeApp contract, framework-vs-app split
  5. data-modeling       — dataset shapes, store schema, migrations, change-detection contracts
  6. config-surface      — config.toml keys, defaults, validation, drift between docs and code
  7. api-surface         — route/handler consistency, status codes, params, response envelopes
  8. testing-strategy    — what's tested at which layer, fixture duplication, e2e gaps
  9. observability       — tracing/log consistency, health reporting, event/webhook telemetry
  10. pick for me   ← default (uses Architect/coverage.md staleness; first run = judgment call)
```

A one-word vague free-form theme yields shallow findings; if option 1's input is too thin, re-ask.

### Q2b — Area (area mode)

```
Area? (Enter = pick for me)
  1. other → type a hint (path fragment, crate name, or context name)
  2. runtime      — Scraping Runtime Core (fetcher, politeness, app/job model, capability traits)
  3. extraction   — Data Extraction & Storage (crawler, extraction engine, dataset store)
  4. engines      — Scraping Engines (search index, WASM sandbox, http/browser/claude engines)
  5. server       — Job Server & API (config/catalog, events/webhooks, worker/cron, datahub, registry, routes)
  6. apps         — the ScrapeApp fleet (funding, labor-market, content/research crates)
  7. clients      — clients/ (TypeScript SDK) and its contract with the API
  8. pick for me   ← default
```

Options 2–6 map to groups in `context-map.json` (apps = the three app groups combined). Resolve the area's file list from the matching groups' contexts' `filePaths`. Scan is bounded to that area but still cross-cutting within it; same parallel-agent shape as scan mode.

If the user's first message is ambiguous about mode (e.g. just `/architect`), present Q1; if they typed `resume` directly, skip to Phase 9.

---

## Constants

- **Codebase reference files:**
  - `context-map.json` (repo root) — feature/context map. Used to resolve area scope and target file lists (`groups[].contexts[].filePaths`, `index` for overview).
  - `docs/harness/harness-learnings.md` — structural facts, conventions, pattern catalogue. **Most important input** for architect; read in full.
  - `.claude/CLAUDE.md` — project rules (context-map discipline, docs-sync enforcement).
  - `docs/features/README.md` + relevant `docs/features/*.md` — the implemented-product surface.
- **Vault root** (resolved at Phase 0): `C:/Users/mkdol/Documents/Obsidian/pumper` if it exists, else `<repo>/.perfect` (shared with `/perfect`; Obsidian-openable, committed by default).
  - `Architect/scans/` — one note per scan run, the synthesis output
  - `Architect/decisions/` — one ADR per accepted decision
  - `Architect/backlog.md` — durable queue of accepted decisions with status
  - `Architect/strong-patterns.md` — load-bearing patterns, kept for codification
  - `Architect/weak-patterns.md` — anti-patterns identified, with affected files
  - `Architect/coverage.md` — themes/areas previously scanned, staleness
  - `Architect/architect-preferences.md` — distilled rules across runs (promoted from Lessons)
  - `Lessons/{date}-architect.md` — append-only self-reflection (shared `Lessons/` dir)
- **Categories of finding** — `weak-pattern | strong-pattern | tech-swap | structural-bug-class | convention-gap`
- **Risk** — 1 (low, isolated) … 5 (production-critical surface: worker loop, dataset store, fetch tiering)
- **Effort** — `s | m | l | xl`
- **Reach** — concrete number: "{N} files / {M} call sites / {K} crates" — never vague.
- **Payoff** — 1 (incremental) … 5 (eliminates a recurring bug class or unblocks a major future)

---

## Phase 0: Resolve vault path & bootstrap

```bash
if [ -d "C:/Users/mkdol/Documents/Obsidian/pumper" ]; then
  VAULT="C:/Users/mkdol/Documents/Obsidian/pumper"
else
  VAULT="<repo>/.perfect"   # create if missing
fi
```

If any of the `Architect/` files above are missing, create them with empty-state skeletons (backlog with `## Pending / ## Shipped / ## Abandoned` sections, patterns files with `## Patterns` heading, coverage with `## Themes / ## Areas`). `Lessons/` is shared — create the dir if missing, never recreate existing files.

---

## Phase 1: Load context & memory

### 1a. Required-file check

`context-map.json`, `docs/harness/harness-learnings.md`, `.claude/CLAUDE.md` must exist. If `context-map.json` is missing → stop; it's Vibeman-generated, the user must refresh it.

### 1b. Read in order

1. `docs/harness/harness-learnings.md` — in full. Conventions, engine internals, pattern catalogue.
2. `context-map.json` — group/context taxonomy, file paths, `index`.
3. `.claude/CLAUDE.md` — project rules (esp. docs-sync).
4. `$VAULT/Architect/strong-patterns.md` — what's already considered load-bearing (avoid re-flagging strengths as "discoveries").
5. `$VAULT/Architect/weak-patterns.md` — what's already on the radar.
6. `$VAULT/Architect/backlog.md` — pending/in-progress decisions.
7. `$VAULT/Architect/coverage.md` — staleness signals.
8. `$VAULT/Architect/architect-preferences.md` — deprioritize finding shapes the user has rejected before.
9. The 3 most recent `$VAULT/Lessons/*-architect.md` — recent self-reflection.
10. If `$VAULT/Perfect/` exists, skim `Perfect/Perfect.md` — avoid proposing structural work that collides with an in-flight /perfect direction.

### 1c. Snapshot freshness

If `context-map.json`'s `generatedAt` is >30 days old or `git log --oneline <generatedAt>.. | wc -l` > 200, warn that area scoping may be stale.

### 1d. Aging strong-patterns review

For each entry in `strong-patterns.md`: if `Codification status: noted` AND age > 60 days AND no `Last reviewed` within 30 days → mark **aging**; surface in Phase 5. Already-codified patterns (`docs-written`, `lint-configured`, `test-guard-added`) are never flagged.

---

## Phase 2: Mode dispatch

Scan mode → Phase 3. Area mode → Phase 3 with area scope applied to every sub-agent prompt. Resume mode → Phase 9.

---

## Phase 3: Parallel scan (scan + area modes)

Spawn **3–5 `Explore` sub-agents in parallel** (single message, multiple Agent calls), each looking at the theme/area from a different angle.

### 3a. Pick the angles

Default angle library:
1. **Usage map** — where does this concept appear? Count call sites, group by crate/context. Identify shape variation.
2. **Type/contract** — are types consistent? Trait boundaries respected? Leaky abstractions between core crates and apps?
3. **Failure mode** — what happens when this fails? Error propagation, retries, partial-failure handling, what the API/webhook consumer sees.
4. **Performance surface** — hot paths, blocking work on async runtimes, N+1 fetch/store patterns, unbounded growth (queues, tables, indexes).
5. **Test coverage** — tested at the right layer? Unit vs end-to-end; gaps that hide regressions.

Theme-specific swaps:
- `error-handling` → 1, 2, 3, 5.
- `async-patterns` → 1, 2, 3, 4.
- `trait-boundaries` → 1, 2, plus "framework-vs-app leakage" and "capability trait completeness".
- `data-modeling` → 1, 2, plus "migration history" and "schema-vs-struct drift".
- `config-surface` → 1, 2, plus "config-vs-docs drift" and "default/validation consistency".
- `api-surface` → 1, 2, 3, plus "docs/features parity" (the docs-sync rule makes drift a first-class smell).
- `testing-strategy` → 5 (deeply), plus "fixture duplication" and "harness reach".
- `observability` → 1, 3, plus "tracing span/field consistency" and "silent-failure audit".

In `area` mode, every angle is bounded to the area's `filePaths` from `context-map.json`.

### 3b. Sub-agent prompt template

Each sub-agent prompt is **self-contained**. Use `Explore` (read-only) for all.

```
You are scanning the pumper codebase (Rust workspace at <repo>) for {angle name}.

Theme: {theme}
{If area mode:} Scope: only files under {area paths}
Background: {1 paragraph from harness-learnings.md relevant to the theme}

Specific questions:
1. {question tailored to angle}
2. ...

Report format (Markdown):
- Files inspected: {list, capped at top 30 by relevance}
- Observed shapes: {distinct patterns found, with file:line examples}
- Inconsistencies: {where shapes diverge — specific files}
- Outliers: {any single file/crate doing it differently from the rest}
- Smell strength: 1-5 (1 = healthy, 5 = active drag)
- Cross-references: {where this angle interacts with other parts of the system}

Budget: 30-60 minutes of equivalent work. Sample strategically; report shape,
not exhaustive detail.
```

### 3c. Synthesize

Merge reports into one pattern model. Look for: **convergence** (multiple angles flag the same module → high confidence), **conflict** (strength in one angle, weakness in another → context-dependent), **surprise** (likely the most valuable finding), **reach quantification** (every weakness gets a concrete count).

If reports are thin (smell strengths all 1–2), the area is healthy in this theme — **say so explicitly** and offer a different theme or a no-findings passive scan. Don't manufacture findings.

### 3d. Output structure

- 0–8 **weak-pattern** findings with reach/risk/effort/payoff.
- 0–4 **strong-pattern** findings worth codifying.
- 0–2 **tech-swap** proposals — only when smell strength ≥4 AND swap unlocks payoff a refactor can't.
- 0–3 **structural-bug-class** findings — recurring bugs whose root is structural (fix the missing primitive, not the N call sites).

Cap total findings at **8**; rank by `(reach × payoff) / (risk × effort)` and drop the bottom.

---

## Phase 4: Surface against existing memory

Cross-check every finding against `strong-patterns.md` (flag conflicts explicitly — a "weakness" in something previously called strong is the most interesting outcome), `backlog.md` (merge duplicates: "previously proposed, re-confirming with new reach data"), and `weak-patterns.md` (update existing entries when reach/risk shifted rather than duplicating).

---

## Phase 5: Present findings

Summary table first:

```
#   Type                   Sev    R   E    Reach                     Title
─   ────────────────────   ────   ─   ──   ───────────────────────   ──────────────────────────────
1   weak-pattern           high   3   m    14 files / 6 crates       ...
```

Then per-finding detail — for weak-pattern / structural-bug-class / tech-swap: Type, Reach, Risk (with what-could-break + recovery), Effort (scan/migrate/test ratio), Payoff, **Current shape** (2–3 sentences + file:line examples), **Proposed shape** (with a canonical already-right example where one exists), **Migration plan** (3–7 independently-shippable numbered steps, breaking vs additive, ballpark commit count), **Risks** (with mitigations), **Already-on-radar** link.

For strong-pattern: Type, Reach, **Why it works**, **Codification** (which vehicle — see 7B), **Risk to losing it** (concrete drift-bug shape).

After new findings, print the **Aging** block from Phase 1d if non-empty.

---

## Phase 6: Triage

```
For each finding, pick a verdict:
  1. execute now    — implement this one in this session
  2. queue          — accept as backlog decision; defer       ← default
  3. drop           — not worth pursuing
  4. rework         — true gap, wrong proposed shape

Reply `<finding>=<verdict>` space-separated, `all=<n>`, `ask` for guided
walkthrough, or Enter for `all=2`.
```

- **execute now** → Phase 7. Recommend only one per session; if the user picks more, warn and ask them to pick the highest priority. Allow override.
- **queue** → Phase 8 (stub ADR + backlog entry).
- **drop** → record in scan note as `decided: dropped` with reason; pattern-track in Lessons.
- **rework** → ask "what shape would actually fit?", update, re-present; if no clear redo, queue as `proposed (needs reshape)`.

Strong patterns (new): `1. codify (→ 7B) | 2. note ← default | 3. drop (do NOT persist)`.
Aging strong patterns: `1. codify ← default | 2. snooze (Last reviewed = today, 30d) | 3. drop (remove entry)`.

---

## Phase 7: Execute (one decision, this session)

### 7a. Branch handling

Default is **commit on the current branch** — multiple CLI sessions coexist on this tree; restrictive branching fights that. Ask:

```
Branch handling:
  1. commit on current branch ({git branch --show-current})  ← default, recommended
  2. new branch architect/{slug}   — only when clean separation matters (risky migration reviewed as a unit)
```

Pick 2 only when the user explicitly asks. The ADR is the change's identity, not the branch.

### 7b. Write the ADR first

Before any code change, write `$VAULT/Architect/decisions/{YYYY-MM-DD}-{slug}.md` with frontmatter (`date, slug, status: in-progress, type, reach, risk, effort, payoff, branch, related_scan`) and sections: **Context** (today's reality with file:line), **Decision**, **Consequences** (positive / negative / mitigations), **Rollout** (numbered atomic commits, each with its validation command), **Acceptance criteria**, **Regression checklist**.

### 7c. Pre-flight checks

**Do NOT require a clean working tree.** Concurrent sessions share it. Inspect, classify, coexist:

1. `git status --short` — read every modified/untracked path.
2. Classify each: **in-flight by someone else** (unrelated to this decision — leave strictly alone), **pre-existing in your touch zone** (surface to user: commit theirs first / commit on top ← default / abort), **yours from this session** (normal).
3. Capture validation baselines and record them in the ADR:
   ```bash
   cargo check --workspace
   cargo clippy --workspace 2>&1 | tail -5    # baseline warning count
   cargo test --workspace                      # baseline pass/fail
   ```
   The metric for later commits is *delta vs baseline*, not absolute.

**Forbidden at every phase:** `git stash`, `git reset --hard/--merge`, `git restore` / `git checkout --` on any path, `git clean`, and `git add -A` / `git add .` / `git add -u` — always stage exact paths so you never claim someone else's work. If a conflicting path can't be resolved, abort and queue the decision back with `blocked: working-tree-conflict`.

### 7d. Atomic commits per rollout step

For each rollout step: apply → run that step's validation → compare to baseline (check errors must not increase, clippy warnings ≤ baseline + 5, tests at baseline rate) → fix regressions inline (never stack failing commits, no `--no-verify`, no `--amend`) → commit **with an explicit pathspec**: `git commit -m "architect: <step title>" -- <exact paths>`, body referencing the ADR → record the SHA in the ADR.

**Never bare `git commit` in this tree.** Concurrent sessions pre-stage their work in the shared index; a bare commit (even after `git add <your paths>`) sweeps their staged changes into your commit. The pathspec form commits only the named paths and leaves the rest of the index exactly as it was. (Learned the hard way, first run, 2026-07-26.)

### 7e. Docs-sync — non-negotiable

If any commit changes a user/API-visible surface (endpoint, param, dataset shape, config key, trigger/webhook contract, CLI-observable behavior), update the coupled `docs/features/*.md` **in the same session** per `.claude/CLAUDE.md` — the Stop hook enforces it. If a commit adds a new feature area, add the `scripts/docs/feature-doc-map.json` entry + feature doc in the same change. Internal-only refactors: dismiss the hook with one sentence.

### 7f. Final regression sweep

Re-run all validation fully (`cargo check`, `cargo clippy`, `cargo test`, all `--workspace`). Walk the ADR regression checklist; exercise real code paths where possible (run the server, hit the route, run the app). **Any unverified checklist item → ADR stays `in-progress` with a "needs verification" note** — never mark shipped on faith.

### 7g. Update ADR status

All steps committed + checklist passes → frontmatter `status: shipped`, `commits: [...]`; move the backlog entry Pending → Shipped. Partial → stays `in-progress` recording which steps remain.

---

## Phase 7B: Codify strong patterns

For every pattern marked `codify` (new or aging). Multiple codifications per session are fine — they're independent and low-risk.

### 7B.a. Pick the vehicle

```
How should "{pattern}" be codified? Pick one or more:
  1. lint-config    — workspace lints in Cargo.toml ([workspace.lints.clippy/rust]) or clippy.toml,
                      when a clippy/rustc lint (or lint level bump) mechanically catches the anti-shape
  2. docs-harness   — append a section to docs/harness/harness-learnings.md (read before large changes)
  3. docs-claude    — append a convention to .claude/CLAUDE.md (loaded into every session)
  4. test-guard     — a structural test: a Rust #[test] that walks the tree / asserts the invariant,
                      or a scripts/ check wired like scripts/docs/check-doc-sync.mjs (Stop hook)
  5. multiple       — combination (each vehicle = separate atomic commit)
```

Rule of thumb: mechanically-lintable code shape → `lint-config`; architectural boundary future sessions must know → `docs-harness`; project-wide convention → `docs-claude`; cross-file invariant detectable by scan → `test-guard`.

### 7B.b. Vehicle execution

- **lint-config**: add the lint to workspace lints, run `cargo clippy --workspace`, count new warnings. >200 new warnings → too noisy; pause for guidance. Commit `architect: codify <pattern> as workspace lint`.
- **docs-harness / docs-claude**: read the target, find the right section, write 10–25 concise lines: name, why load-bearing, canonical `file:line` example, anti-shape to avoid. Commit `architect: codify <pattern> in <file>`.
- **test-guard**: follow existing structural-test or `scripts/docs/check-doc-sync.mjs` conventions; clear failure message pointing at the strong-patterns entry. Verify it passes on current code. Commit `architect: codify <pattern> as test guard`.

### 7B.c. Bookkeeping

Update the `strong-patterns.md` entry (`Codification status`, `Codified: {date}`, vehicle pointers) and write a mini-ADR `decisions/{date}-codify-{slug}.md` (frontmatter: `type: codification`, `vehicle`, `parent_strong_pattern`, `commits`; body: Why now / Vehicle and rationale / Rollback).

Snoozed aging patterns: bump `Last reviewed` + `Snoozed until: {today+30d}` in `strong-patterns.md`. Dropped aging patterns: delete the entry (no tombstones) + one-line note in Lessons.

---

## Phase 8: Backlog the queued decisions

For every **queue** verdict:
- **Stub ADR** — Phase 7b template with `status: proposed`, sketchy Rollout allowed, no commits/branch.
- **Backlog entry** under `## Pending`:
  ```markdown
  - **[{date}] {Title}** — type: {type}, risk: {N}, effort: {s/m/l/xl}, payoff: {N}, reach: {concrete}
    ADR: [[Architect/decisions/{date}-{slug}]]
    Source scan: [[Architect/scans/{date}-{theme}]]
    Status: proposed
    Notes: {triage input}
  ```
  Sort Pending by `(reach × payoff) / (risk × effort)` descending.
- **weak-patterns.md** entry per weak finding (First/Last seen, Reach + trend, Backlog link, Examples).
- **strong-patterns.md** entry only for `note`/`codify` verdicts — **never for `drop`** (Identified, Reach, Why it works, Codification status, Last reviewed, Examples).

---

## Phase 9: Resume mode

1. Print `backlog.md`'s Pending section as a numbered table (Date / Title / Type / R/E/P / Reach).
2. Ask which to execute: number, `open N` (print the ADR, re-ask), `abort`, Enter = #1.
3. **Refresh the ADR** — re-verify file:line anchors, re-count reach, read recent git log on touched files. If anything material changed, present the delta and ask: proceed / reshape / abandon.
4. Jump to Phase 7c and run 7d–7g normally (branch question still applies).

---

## Phase 10: Self-reflection

1. Batched question: why were dropped findings dropped? (`skip`/Enter = "no reason given".)
2. Append `$VAULT/Lessons/{date}-architect.md`: run stats, triage outcome, drop reasons, and self-reflection (which angles produced signal vs noise, synthesis misses, calibration drift, one reusable insight).
3. Pattern promotion: after 3+ repeated drop-reason observations across Lessons, propose adding a rule to `Architect/architect-preferences.md`.
4. **harness-learnings.md update check** — if the run discovered a structural fact future sessions need, add it to `docs/harness/harness-learnings.md` tagged with the run date. Architect runs are especially prone to surfacing unmapped boundaries.
5. Update `coverage.md` (theme/area, last scanned, findings + actioned counts, yield density, notes).

---

## Phase 11: Persist the scan

Write `$VAULT/Architect/scans/{YYYY-MM-DD}-{theme-or-area-slug}.md` with frontmatter (mode, theme/area, sub_agents_spawned, findings by category, executed/queued/dropped/reworked lists, adrs_written, commits, branch) and body: per-angle 1–2 sentence summaries, per-finding verdict blocks, strong patterns observed, cross-references.

---

## Phase 12: Final summary

Print the run report: mode, theme/area, sub-agent count, findings by category, triage outcome (executed with commits/ADRs, queued, dropped, reworked), strong-pattern outcomes (codified with vehicles, noted, aging actioned), files updated in the vault, then:

```
Next?
  1. /architect resume     — execute next decision ({Q} pending)   ← default if Q > 0
  2. /architect scan       — fill the queue with a new theme
  3. /perfect              — product-direction companion loop
  4. done
```

---

## Notes on use

- **Cadence** — once a week is plenty. Alternate scan (fill the queue) and resume (drain it). 20+ pending items → next session should be resume.
- **Coexist with uncommitted work.** Never require a clean baseline; never stash/reset/clean; stage exact paths only.
- **Conflict signal** — a finding contradicting a vault `strong-pattern` is the most interesting finding of the run: either the entry is stale or the finding is wrong; both change the model.
- **Drift signal** — 3 consecutive scans producing backlog items with zero executed via resume → surface it: architect is being used as brainstorming, not shipping. Ask whether to lower the execution bar or accept the backlog as the artifact.
- **Tech swaps are the riskiest** — never propose a swap with reach ≥50 files unless smell strength is 5.
- **Division of labor with /perfect** — /perfect proposes product directions (features, API elevations); /architect proposes structural changes (conventions, bug classes, boundaries). If a finding is really a feature, hand it to the /perfect vault instead of the architect backlog.
