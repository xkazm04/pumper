---
name: passport-onboard
memory: project
category: Maintenance
description: Guided, select-driven onboarding of a repository against the Personas App Readiness Passport — assess every dimension, offer batched skip/path-A/path-B choices in the terminal, then execute accepted work with parallel subagents. Works for brand-new projects and as a completion checklist for developed ones. Invoked standalone in a repo, or dispatched by the Personas passport wall via Fleet.
---

# Passport Onboard

You are onboarding the CURRENT repository against the App Readiness Passport —
the per-dimension readiness scorecard the Personas desktop app maintains for
every managed project. Your job is NOT to blindly "improve everything": it is
to assess honestly, put every decision in the user's hands as a terse select
choice, and then execute exactly what they accepted — in parallel, because
this session can get large.

The same flow serves two audiences: a fresh repo getting its foundations, and
a mature repo using this as a completion checklist. The assessment phase makes
that distinction automatically — a dimension already at target gets reported
as ✓ and never wastes a question on it.

## Modes

**Dispatched (passport wall → Fleet).** The dispatch prompt carries a CONTEXT
BLOCK: project name/root, a passport snapshot (dimension → current level), env
slots (local/test/production per infra dimension), and the user's available
Personas connectors as `{name, service_type}` metadata — NEVER secrets. Trust
the snapshot for state; skip re-deriving what it already says.

**Standalone (CLI in any repo).** No context block. Phase 1 derives the state
itself (the deterministic checks are in `references/dimensions.md`), and
connector availability is a QUESTION for the user, not an assumption.

**Dimension-scoped (wall row → Fleet).** The dispatch names ONE dimension
(e.g. "SCOPED to a single dimension: Tests"). Run the same loop shrunk to it:
assess that dimension inline (no group assessors), present ONE decision round
of selects and WAIT — the operator is watching this terminal and answers the
way they would a full run; execute exactly what they accept (parallel
builders still allowed if the accepted path splits); re-assess; refresh ONLY
that dimension's entry in `app-passport.json`. Everything else — hard rules,
binding doctrine, honest levels, the report shape — applies unchanged.

**Prior manifest.** In EITHER mode, if `app-passport.json` exists at the repo
root, read it first: trust its levels as of `generatedAt` (re-verify only the
cheap checks), and NEVER re-ask a dimension it marks `skippedByChoice` —
surface it as "skipped on <date>, say the word to revisit" in the round intro
instead of a question. The manifest is how onboarding decisions survive
between runs.

## The loop: Assess → Decide (batched) → Execute (parallel) → Re-assess

### Phase 1 — Assess (parallel, read-only)

Spawn 3 read-only subagents IN PARALLEL, one per dimension group (Foundation /
Environments & Infra / Quality & Telemetry — the groups and their checks are
`references/dimensions.md`). Each returns, per dimension: current level,
the 1-2 line evidence for it, and the 2-3 realistic paths forward with ONE
recommendation. Assessors read; they never write.

While they run, read the context block (or probe `git`/files yourself) for
the environment picture: what local/test/production presence is already
observable per infra dimension.

### Phase 2 — Decide (batched selects, then hands off the keyboard)

Present decisions with the terminal select mechanism (the AskUserQuestion
tool; max 4 questions per call — if it is unavailable, print numbered options
and wait for typed input). Rules:

- **Batch by group, not per row.** Three-to-four decision rounds for the whole
  passport, in this order: Foundation → Environments & Infra → Quality &
  Telemetry → (only if reached) App cost confirmation. Never 14 sequential
  single questions; never one overwhelming mega-round.
- **Pipeline rounds behind assessors**: present a group's round as soon as ITS
  assessor returns — don't hold the user for the slowest group. (Field-proven:
  zero dead wait across three rounds.)
- **"Other" answers are first-class, not noise.** A user may name their OWN
  tool for a connector-flavored dimension. Then: check the Personas catalog
  for its service type first; if the tool's repo is local, read its client
  contract there (env var names, wire shape); prefer VENDORING a minimal
  client over a cross-repo path dependency that would break CI.
- Every question offers: **Skip** · concrete path A · concrete path B with
  **(Recommended)** on exactly one option · and the built-in Other lets the
  user type a custom direction. Options name OUTCOMES ("Wire Sentry to test +
  production"), not chores.
- Dimensions already at target are stated as ✓ in the round's intro text and
  get NO question.
- For env-scoped dimensions (hosting, database, auth, observability, LLM
  tracking): the choice is per-environment. Offer the environments in one
  question ("Which environments should X cover?" — multiSelect) or fold the
  env into the path options; either way the user can skip any environment.
- Connector-flavored options come from `references/connectors.md`: prefer an
  EXISTING user connector by name when the context block lists one, offer
  "add a new <type> connector in Personas Vault first" when none fits, and
  never invent credentials — wiring in code always reads env var NAMES.
- After the last round, echo a one-screen **decision ledger** (dimension →
  chosen path or skip) before any execution starts. This is the contract for
  the rest of the run.

### Phase 3 — Execute (parallel subagent waves)

Run accepted work as **parallel subagents — use the strongest available model
(Opus-class) for builders** since each task is a real engineering change.
Rules:

- **Waves respect dependencies, agents within a wave are parallel.** Wave 1:
  foundation dimensions (independent of infra). Wave 2: hosting/env
  resolution FIRST, then database + auth in parallel, then CI (its
  auto-deploy-to-test-env step needs hosting decided). Wave 3: tests, evals,
  observability, LLM tracking in parallel. App cost is composed LAST by the
  orchestrator itself from the round's connector decisions.
- One dimension = one subagent with a scoped brief: the accepted path, the
  repo conventions to follow, and the DONE criterion from
  `references/dimensions.md`. Skills first: each builder brief opens by
  telling the agent to check `.claude/skills/` in this repo and
  `~/.claude/skills/` for a matching skill and follow it over the brief.
- **MERGE briefs whose file scopes collide** (e.g. hosting + backup + auth all
  touch the deploy docs and env examples → one ops-hardening agent). Colliding
  scopes cost more in contention than a combined brief costs in focus.
- **Shared-checkout commit discipline** (every builder brief carries this):
  stage ONLY your explicit paths (never `-A`); verify the staged set matches
  your intent before committing; on `index.lock`, wait 3-10s and retry up to
  6×; expect HEAD to advance mid-run (rebasing onto it is fine); for
  co-mingled manifest files (Cargo.toml/lock, package.json) isolate your hunks
  via a temporary worktree + patch-staging rather than staging the whole file;
  NEVER `commit --amend` once concurrent agents may have committed — the amend
  lands on THEIR commit.
- **Never trust the checkout's branch.** A concurrent session may have
  switched the shared checkout to ITS branch mid-run — check
  `git branch --show-current` before any git op, and when the target branch
  (master unless the user said otherwise) isn't checked out, commit through a
  temporary worktree pinned to the target instead of switching the shared
  checkout. A commit that lands on a foreign branch gets cherry-picked to the
  target and surgically removed from the foreign branch ONLY if that removal
  cannot take concurrent work with it — otherwise leave the duplicate and say
  so (identical changes reconcile at merge).
- When another session owns a file you must extend (e.g. `.gitignore` for the
  app-cost rule), commit the change on the TARGET branch via a worktree and
  make at most a minimal unstaged append in the shared checkout — protective,
  merge-trivial, and their commit carries it forward.
- Pre-existing lint/format noise in a touched area gets REPORTED, never fixed
  silently (it may be another session's in-flight work or a toolchain
  artifact).
- Builders self-verify (build/test/lint as available) before reporting.
  Nothing is committed unless the user asked for commits; report a per-agent
  diffstat instead.
- A failed or blocked builder reports WHY and what remains — never silently
  drops its dimension.

### Phase 4 — Re-assess + report

Re-run the Phase-1 checks for every touched dimension. Close with a compact
table: dimension → before → after → what was skipped (user's choice vs
blocked), plus the exact follow-ups the user still owns. Connector-flavored
dimensions must ALSO show their `dev_projects` slot binding in the after
column — bind it yourself when the app is reachable (connectors.md § Binding
closes the loop); a report that hands the user `.env` homework for Sentry/
GitHub instead of a binding is a field-test failure we've already made once.
Before the report, write/refresh the **public-safe manifest**
`app-passport.json` at the repo root (commit it with the run when the run
commits — it is publishable by design):

```json
{
  "schemaVersion": 1,
  "generatedAt": "<ISO date>",
  "generatedBy": "personas passport-onboard",
  "dimensions": {
    "<dimension-key>": { "level": "<honest level>", "tool": "<tool NAME or null>", "skippedByChoice": false, "note": "<optional 1-liner>" }
  }
}
```

PUBLIC-SAFE means levels + tool *names* only — NEVER credential ids, URLs,
env values, costs (`app-cost.json` stays private/gitignored), or local paths.
Its jobs: (1) re-runs read it as the prior and honor `skippedByChoice`;
(2) any CLI agent in the repo gets instant maturity context without Personas
(the `context-map.json` pattern); (3) substrate for a future CI verify check.

If dispatched from the wall, end with
one line the wall can grep: `PASSPORT_ONBOARD_RESULT: <n> improved, <n>
skipped, <n> blocked`.

## Hard rules

- **Secrets never move.** Connector choices are names + service types;
  code wiring reads environment variable names; you never read, echo, or
  transfer a credential value. The Personas Vault is where credentials live —
  when a needed connector doesn't exist, the path is "user adds it in
  Personas → Vault", never "paste a key here".
- **Skip is always honored, at dimension AND environment granularity.** A
  skipped item appears in the final report as skipped-by-choice, not failure.
- **Additive, convention-following changes.** Read before writing; match the
  repo's stack and idioms; never invent commands, endpoints, or behavior the
  code doesn't have. Prefer several small verifiable changes over one rewrite.
- **The app-cost file is personal**: `app-cost.json` at the repo root, and it
  MUST be added to `.gitignore` in the same change that creates it. Shape and
  composition rules are in `references/dimensions.md` §App cost.
- **Honest levels only.** The passport's whole point is that every value is
  observed. Never claim a dimension improved without the Phase-4 re-check
  showing it.

## Memory outbox (Personas-managed repos)

If this repo is managed by the Personas app (dispatched runs say so; standalone: a `.personas/` dir or the operator confirms), leave durable notes for the next terminal before finishing: append 3–8 JSON lines to `.personas/memory-outbox.jsonl` (append, never rewrite) —
`{"type":"node","kind":"progress|decision|gotcha|fact","title":"≤200 chars","body":"optional detail","context":"optional context-map context name"}`
Record: dimensions assessed + outcomes, choices the operator made, gotchas hit. The app ingests and deletes the file when the session ends. Skip silently when the repo is not Personas-managed.
