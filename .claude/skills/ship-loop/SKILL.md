---
name: ship-loop
category: Development
description: Long-horizon production-readiness loop for pumper. Audits the service into a scorecard + numbered backlog, then iterates milestone-by-milestone — the user steers via short select-based checkpoints — until every dimension is verifiably green; works end-to-end, tested, runtime-verified with real jobs, datasets proven to carry value, source/cost value-case validated against web-researched alternatives, code and docs polished. Persists state to disk so it survives compaction and resumes across sessions. Use when the user wants to drive pumper to ship-ready over a day-plus session.
argument-hint: "[resume | status | ship-check | <focus>]"
disable-model-invocation: true
---

# Ship Loop (pumper edition)

Mission: drive pumper to **ready to run in production**, with machine evidence for every claim. This loop is designed to run for a day or more, surviving context compaction and session restarts.

**Target repo:** this repo — a Rust workspace (`Cargo.toml` at the root, crates under `crates/`), not a Node app. If the user names a different directory when invoking, resolve it and state that explicitly in the first message. All state paths below are relative to the target repo.

**What "ship" means here.** pumper is a **local-first service**, not a SaaS: one binary, no auth, no billing, no UI, consumed over HTTP by other apps and agents on this machine (`ONBOARDING.md` §4). So the loop proves *the service does what it claims, keeps its data honest, and costs what it should* — not *someone will pay for it*. The scorecard in `references/scorecard.md` is adapted accordingly; dimensions 5 and 9 are the ones that changed most.

## Operating rules (non-negotiable)

1. **Disk is truth, context is cache.** All loop state lives in `.claude/ship-loop/` in the target repo. After boot, resume, or any sign your context was compacted, re-read `state.md` before acting. Update state files immediately after every completed item — never batch updates for later.
2. **Evidence or it isn't green.** A scorecard dimension turns 🟢 only on machine evidence produced by this loop (exit codes, test reports, real job runs, inspected dataset rows). Reading code is never sufficient. Record the evidence command + result next to each score. Capture exit codes so they can't lie: `cmd | tail` makes `$?` the tail's exit code — redirect to a file and echo `$?` immediately instead (a "clippy passed" built on a piped exit code is fabricated evidence).
3. **Questions only at checkpoints.** Between checkpoints run fully autonomously. When a decision comes up mid-milestone: make the smallest reversible call consistent with existing patterns, log it under "Auto-decided" in `decisions.md`, and flag it at the next checkpoint. Interrupt early only when genuinely blocked.
4. **Every user input is a short selection.** Use AskUserQuestion exclusively — never open-ended prose questions. Max 4 questions per checkpoint. For picks from lists longer than 4 options, print the numbered list as text first, then offer preset options ("Top 3 as listed (Recommended)", "All correctness items first", …) — the built-in *Other* lets the user type numbers like `1,4,7`.
5. **Never fake progress.** Failing tests are reported as failing. A dimension regresses to 🔴 the moment its evidence breaks. No "should work now" — verify by running. In this repo that specifically means: **`just check` (`cargo check --workspace`) is not evidence that a scrape works.** ONBOARDING §8 is explicit — a change to an app or engine is not done until a real job has run through it.
6. **Protect the main context.** Fan out audits, wide searches, and bulk mechanical work to subagents (Explore for read-only surveys, general-purpose for multi-step work). The main loop is the orchestrator: it owns the scorecard, decides, verifies, and talks to the user.
7. **Git discipline — shared checkout.** Other CLI sessions (`/architect`, `/perfect`, `/explorer`, `/tiger`) commit into this same working tree concurrently. Commit per coherent change with the prefix `ship:`. **Stage only explicit paths in one bash invocation** (`git add <paths> && git diff --cached --stat`); never `git add -A`/`.`/`-u`; never `--amend`; never `git stash`/`reset --hard`/`clean -f`. On `index.lock`, wait 3–10s and retry up to 6 times; expect HEAD to advance mid-run. `.claude/` is gitignored in this repo — the ship-loop state dir stays untracked; never force-add it. Never push unless the user asks; the local gate is primary.
8. **Reuse installed skills as tools** when available: `/explorer` for a scoped quality sweep inside a milestone, `/architect` when a backlog item turns out to be structural, `/tiger` for anything touching the `claude` engine or its cost, `/simplify` during polish milestones.
9. **Honor the repo's own contracts.** The dependency rule (`apps → core ← engines`, only `server` depends on everything), the same-session doc-sync rule, and "bug fixes ship as extracted, tested functions" all come from `.claude/CLAUDE.md` + `ONBOARDING.md` §3/§7/§9. The loop does not get an exemption because it is in a hurry.

## Arguments

- *(none)* — resume if `.claude/ship-loop/state.md` exists, else run Phase 0 boot.
- `status` — read state, print the scorecard + current milestone + next action, stop.
- `ship-check` — run the full Verification Gate and, if all green, the Ship Gate. Report.
- anything else — treat as a focus request: hold a checkpoint with that dimension/topic pre-prioritized.

## Phase 0 — Boot (first run only)

1. **Detect the stack.** Read `CLAUDE.md` and `MEMORY.md` at the repo root **first** — `CLAUDE.md` is the shortest true summary (commands table, architecture, dependency rule, the four-step app-adding contract), and `MEMORY.md` indexes the repo's durable state under `.perfect/`. Check `.perfect/Architect/backlog.md` **Pending** before building the backlog, so the loop doesn't duplicate structural work `/architect` has already queued. Then read `Cargo.toml` (workspace members), `justfile`, `README.md`, `ONBOARDING.md`, `docs/deployment.md` (the run story: local-first operation, persistent state, auth posture), `.claude/CLAUDE.md`, `context-map.json`, `.github/workflows/ci.yml`. Then read `references/stack-pumper.md` — but treat it as *hints, not facts*: profiles go stale. When the audit contradicts the profile, believe the audit, and update the profile file so the correction sticks.
2. **Create the state skeleton immediately** — `journal.md` + `decisions.md` in `.claude/ship-loop/` (templates in `references/templates.md`), and append journal lines as audit results land. Crash-safety beats tidy phase ordering.
3. **Fan out the audit** to parallel subagents — one per audit lens in `references/scorecard.md`: app/route inventory, dataset + migration map, tests + tooling, resilience & safety posture, API↔SDK↔docs contract, ops readiness, the **source & cost value lens** from `references/value-validation.md` (give it `catalog/data-sources.toml` and demand cited sources), and the **platform-standards lens** from `references/platform-standards.md` (observability, docs sync, catalog/context-map parity). While they run, execute directly in the main loop and capture real exit codes:
   ```bash
   # The repo-root justfile is the canonical task runner (`cargo install just`).
   # Raw cargo equivalents are in CLAUDE.md's table; run everything from the repo root.
   just fmt-check;   echo "fmt=$?"      # cargo fmt --check
   just lint;        echo "clippy=$?"   # cargo clippy --workspace --all-targets
   just test;        echo "test=$?"     # cargo test --workspace
   just build;       echo "build=$?"    # cargo build -p pumper-server
   # Past boot, `just ci` runs fmt-check + lint + test in one command.
   ```
4. **Build the scorecard** (10 dimensions, definitions in `references/scorecard.md`; dimension 9 in `references/value-validation.md`, dimension 10 in `references/platform-standards.md`) and a numbered, prioritized **backlog** (`state.md` + `backlog.md`): every gap = one item, tagged with its dimension, sized S/M/L, ordered by (ship-blocking first, then impact/effort). Write the dimension-9 artifacts to `value-case.md` in the state dir.
5. **Boot checkpoint** — print the scorecard and top backlog, then one AskUserQuestion call (≤4 questions). Defaults:
   - **Ship bar**: Unattended scheduled operation — apps run on cron, unwatched (Recommended) / On-demand service — other agents call it, a human is around / Local demo — correctness on the golden path only. This sets how strict the gates are.
   - **Cadence**: Milestone — check in after each milestone (Recommended) / Marathon — check in only when blocked or every 4th milestone (overnight mode) / Tight — every 2–3 items.
   - **First focus**: preset picks over the printed numbered backlog (+ Other for custom numbers).
   - **Runtime-acceptance depth**: Critical apps only (the ones marked `status = "live"` in `catalog/data-sources.toml`) / All registered apps + failure cases (Recommended for unattended operation).
   If the audit surfaced an **existential decision** (e.g. a registered app whose upstream source is dead, a dataset whose change detection has never actually fired), swap it in for the least-critical default question.
6. Record answers in `decisions.md`, define Milestone 1 in `state.md`, go to Phase 2.

**AFK protocol (any checkpoint):** if AskUserQuestion times out, do not block and do not guess the big calls. Apply recommended defaults to operational knobs (cadence, depth), explicitly DEFER product-shaping decisions (dropping an app, changing a dataset key), pick the largest decision-free backlog slice (correctness, resilience, test debt) as the next milestone, log all of it in `decisions.md` as provisional, and re-present the deferred questions at the next checkpoint.

## Phase 1 — Checkpoint (the only place the user is needed)

Triggers: milestone completed · genuinely blocked · ship gate reached · user invoked with a focus argument.

1. Bring state files current. Print, in this order: scorecard with per-dimension delta since the last checkpoint (⬆︎/⬇︎/=), milestone result (done / failed / deferred items), auto-decisions taken since last checkpoint, top ~8 backlog items as a numbered list.
2. One AskUserQuestion call, ≤4 questions, drawn from: next-milestone pick; flagged product decisions (e.g. "`eu-sedia`'s portal changed shape and now needs the browser tier — Rewrite / Mark blocked in the catalog / Cut the app"); **value-case verdicts** (a source whose cost or trustworthiness fails the bar: Re-tier it / Improve the extraction / Cut / Accept with a lowered `confidence` — the loop never green-lights its own value judgment); bar or cadence adjustments; confirmations of auto-decisions worth reversing.
3. Log answers in `decisions.md`, write the next milestone (3–10 items, always ends with a Verification Gate) into `state.md`, go to Phase 2.

## Phase 2 — Milestone execution

For each backlog item in the milestone:

1. Read the relevant code fresh (don't trust stale context), implement the change. Scope it with `context-map.json` — find the context that owns the files, and stay inside it unless the item says otherwise.
2. Add or extend tests that pin the behavior. Per `.claude/CLAUDE.md`, a bug fix ships as an **extracted, named function** plus a test named after the anti-pattern it defends (`x_not_y`), not as an inline patch in a `run()` body. A convention is enforced with an inventory test (the EXPECTED-diff idiom in `crates/server/src/routes/mod.rs`), never with a sentence in a doc.
3. Run targeted tests + `just check` on what you touched. For anything a user of the service can observe, verify end-to-end by driving a real job (see the runtime-acceptance spec in `references/dataset-and-runtime-acceptance.md`).
4. If the change is user/API-visible (endpoint, param, dataset shape, app, trigger contract, config key, CLI-observable behavior), update the coupled `docs/features/*.md` **in the same commit** — `scripts/docs/feature-doc-map.json` is the map, and a Stop hook checks. If the change moves files between contexts, update `context-map.json` too.
5. Update `backlog.md` status (☐→◐→☑), append one `journal.md` line, refresh the **Context refresher** block in `state.md`, commit (`ship:` prefix, explicit paths, one invocation).

An item is *done* only with code + test + evidence. If an item balloons past ~3× its size estimate: finish the smallest shippable core, file the remainder as new backlog items, move on.

## Phase 3 — Verification Gate (ends every milestone)

Run the full ladder in order, recording each result in `state.md`:

1. `just fmt-check` → 2. `just lint` → 3. `just test` → 4. `just build` (steps 1–3 are exactly `just ci`; add `--release` to the build if the ship bar is unattended operation) → 5. **Runtime acceptance suite** — real jobs driven through a running server (`just run` = `cargo run -p pumper-server --bin pumper`; full at Milestone/Marathon cadence; touched apps + smoke at Tight) → 6. **Dataset value assertions** (`references/dataset-and-runtime-acceptance.md`) → 7. Value-case freshness check (`value-case.md` exists, research ≤30 days old, no unaddressed weak verdict — see `references/value-validation.md`).

Note on step 3: environment-dependent tests (real Chrome, wasm artifacts, wall-clock timing) are `#[ignore]`d and excluded from CI. At Milestone cadence or better, also run `just test-ignored` and record it separately; a compile failure in an ignored test is a real regression that CI cannot see.

Regressions preempt the backlog: fix immediately if under ~30 min, otherwise file as a top-priority 🔴 item and surface it at the checkpoint. Update the scorecard with fresh evidence. Then Phase 1 — or Phase 4 if every dimension is 🟢.

## Phase 4 — Ship Gate

Entry condition: all 10 dimensions 🟢 at **two consecutive** Verification Gates (stability, not a lucky pass).

1. Run the pre-flight checklist in `references/scorecard.md` §Ship gate (config keys, data dirs, crash recovery, schedules, webhook signing, error visibility, docs, catalog current + user-confirmed).
2. Write `SHIP_REPORT.md` at the repo root (template in `references/templates.md`): scorecard with evidence, app/journey list, dataset value table, the value case (alternatives + cost + reality checklist), known limitations, run steps.
3. Final checkpoint: confirm ship readiness; offer the remaining nice-to-have backlog as an optional next loop.

## Compaction & resume protocol

`state.md` opens with a **Context refresher** block (≤15 lines) containing: service + stack, ship bar, cadence, one-line scorecard, current milestone with item statuses, and the *exact next action*. Keep it accurate after every item — a fresh context must be able to continue from that block alone. If you notice you're missing details you should know (compaction happened), stop and re-read all four state files before touching code.
