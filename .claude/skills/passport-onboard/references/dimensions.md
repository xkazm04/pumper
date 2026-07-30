# Dimension playbook

Order matters: Foundation gives agents ground to stand on, Environments & Infra
resolves WHERE the app runs before anything depends on it (hosting before CI is
deliberate), Quality & Telemetry instruments what now exists, App cost composes
last from everything decided above.

Per dimension: **Assess** = the deterministic check (mirror of the passport's
own probes — same signals, so the wall agrees with you afterwards). **Offer** =
the realistic paths for the decision round. **Execute** = builder guidance.
**Done** = the passport level the re-check should show.

---

## Group 1 — Foundation

### 1. Context coverage
- **Assess**: does the repo have a Personas context map (`context-map.json` at
  root, or the app's snapshot says contexts exist)? Count of contexts: 0 =
  none, <20 partial, ≥20 full (the wall's GRAPH scale).
- **Offer**: Skip · Full context scan (recommended for new/unmapped) ·
  Incremental re-scan (mapped repos with drift).
- **Execute**: when dispatched by the wall the scan is the APP's job — report
  "request scan via the wall's Context coverage cell" rather than hand-rolling
  a map. Standalone: propose the map structure (groups → contexts → file
  globs) as `context-map.json` following the schema of an existing map.
- **Done**: partial→full per context count.

### 2. Agent instructions
- **Assess**: `CLAUDE.md` at root (and `.claude/CLAUDE.md`); does it carry the
  real commands (build/test/lint/run), architecture sketch, conventions,
  QUALITY STANDARDS (what "done" means here) and DESIGN STANDARDS (naming,
  layering, UI tokens if applicable)? A stub counts as `none`.
- **Offer**: Skip · Author/upgrade CLAUDE.md grounded in the code
  (recommended) · Minimal command-card only (fast path).
- **Execute**: read the codebase first; document only what exists; one screen,
  factual; include a short quality-standards and design-standards section the
  repo can actually enforce. Never invent commands.
- **Done**: instructions chip present (real CLAUDE.md).

### 3. Documentation
- **Assess**: README quality (real vs stub), `docs/` census, doc-map manifest
  (source→doc coupling), and — if the passport snapshot carries it — the
  doc-rot picture (dirty/unread counts).
- **Offer**: Skip · Baseline docs: real README + focused docs/ tree
  (recommended when none/readme) · Refresh stale docs against their changed
  sources (when structured but rotting) · Add a doc-map manifest so freshness
  becomes managed (mature repos).
- **Execute**: per the passport's docs doctrine — README with purpose,
  architecture, commands, key directories; a handful of focused pages, each
  naming the source paths it describes (that naming IS the coupling the rot
  scan reads). Refresh = update only what the changed sources made wrong.
- **Done**: none→readme→structured; synced needs the doc-map.

### 4. Agent memory
- **Assess**: root `MEMORY.md` / `.claude/memory/`; Claude Code auto-memory
  for this repo (`~/.claude/projects/<encoded-root>/memory/` — encoded root =
  the absolute path with every non-alphanumeric char as `-`): file count,
  index entries, freshness.
- **Offer**: Skip · Seed a MEMORY.md convention: root index + CLAUDE.md
  pointer telling agents to record durable learnings (recommended when none) ·
  Curate an existing ad-hoc store: index it, prune dead entries.
- **Execute**: seed with 3-5 REAL non-obvious facts learned reading this repo
  (gotchas, invariants, non-derivable decisions) — no filler; add the
  CLAUDE.md section (check at session start, record at session end).
- **Done**: none→adhoc→curated (indexed ≥5 entries, fresh).

### 5. Reusable skills
- **Assess**: `.claude/skills/` census; which skills from the user's global
  library (`~/.claude/skills/`) are missing here; per the snapshot, which are
  dormant elsewhere (don't recommend adopting a skill nobody uses).
- **Offer**: Skip · Adopt the recommended scope: the library skills that match
  this repo's stack and actual workflow, customized to its commands/layout
  (recommended) · Full library adoption (rarely right — say why if offered).
- **Execute**: for each adopted skill, copy then CUSTOMIZE: this repo's real
  commands, paths, idioms; keep the skill's method intact. List what was
  deliberately NOT adopted and why (stack mismatch, dormant at source).
- **Done**: skills present; shared count > 0.

---

## Group 2 — Environments & Infra

Env-scoped: each of these resolves per environment — **local / test /
production** — and the user may skip any environment. "Wire" always means:
config/SDK in code reading ENV VAR NAMES, a `.env.example` entry, and a
verification step (build passes; the tool's init path runs without a value
present — degrade, don't crash).

### 6. Hosting  *(deliberately before CI — CI's deploy step needs a target)*
- **Assess**: deploy configs present (vercel/netlify/fly/railway configs,
  Dockerfile, compose); which envs have a real target today (test_env_url in
  the snapshot = test exists).
- **Offer** (per env): Skip env · Use existing connector <name> (when the
  context block has a hosting-type credential) · Add hosting config for
  <detected-fit provider> (recommended by stack fit: static/Next→Vercel or
  Netlify or Cloudflare; container/service→Fly, Railway, DigitalOcean; VM/
  cloud→AWS) · Local env usually just needs a documented `dev` command — offer
  "document local run" not "host local".
- **Execute**: idiomatic config for the chosen provider + a DEPLOY note (env
  vars by NAME, deploy command); never hardcode secrets.
- **Done**: hosting present for the accepted envs.

### 7. Database & migrations
- **Assess**: persistence detected (engine, ORM), migrations framework
  (dir + tool), per-env database story (local file/container? test/prod =
  managed service?).
- **Offer** (per env): Skip · Keep current engine, add versioned migrations
  (recommended when tables exist and migrations = none) · Wire managed DB
  connector <name/type> for test/production (supabase/neon/postgres/convex/
  upstash from the catalog) · Local: document the local DB setup command.
- **Execute**: migrations = idiomatic tool for the stack, initial migration
  captures the EXISTING schema without data loss, up/migrate command
  documented. Connector wiring = connection via env var names + .env.example.
- **Done**: migrations scripted/versioned; env slots filled for accepted envs.

### 8. Auth
- **Assess**: auth method from dependencies (Clerk/Auth.js/Auth0/Supabase/
  Firebase/…), per-env configuration presence.
- **Offer** (per env): Skip · Keep detected <method>, complete its env story
  (missing env vars documented per environment) · Wire <catalog provider>
  (only when the app clearly needs auth and has none — recommend the
  stack-idiomatic one).
- **Execute**: config + env var names per accepted env; document the callback
  URLs per env WITHOUT inventing domains — use placeholders where the user
  must fill real hosts.
- **Done**: auth present.

### 9. CI  *(after hosting so auto-deploy has a target)*
- **Assess**: workflow files, which checks actually gate merges, deploy
  automation presence; the repo's remote (GitHub vs GitLab decides the
  connector and workflow dialect).
- **Offer**: Skip · Checks-only pipeline: build+test+lint on PR (recommended
  first rung) · Checks + auto-deploy to the TEST environment on main
  (needs a test hosting target from §6; uses the GitHub/GitLab connector) ·
  Full gated delivery (mature repos).
- **Execute**: idiomatic workflow files; secrets referenced by NAME as CI
  secrets, never inline; the deploy job targets the test env ONLY unless the
  user explicitly chose production.
- **Done**: CI level checks→gated→delivery; the GitHub/GitLab credential is
  bound as the project's `pr_credential_id` (connectors.md § Binding closes
  the loop).

---

## Group 3 — Quality & Telemetry

### 10. Tests
- **Assess**: framework, test-file count, what the suite actually protects;
  the 2-3 critical paths a regression would hurt most.
- **Offer**: Skip · Cover the top critical paths (recommended — name them in
  the option text) · Broad smoke layer (thin, fast, everywhere) · Stabilize an
  existing flaky suite.
- **Execute**: parallel builder MAY split per critical path; follow existing
  conventions; real behavior, no mocking away the subject; suite green before
  reporting.
- **Done**: tests none→smoke→partial by file count and coverage of the named
  paths.

### 11. Evals  *(the LLM-call-site dimension)*
- **Assess**: find the LLM call sites (grep providers/SDKs); is there an eval
  harness (evals/ dir, eval script) covering the core prompt behaviors?
- **Offer**: Skip · Eval harness over the top call sites: real cases, scored,
  one command (recommended when LLM calls exist and evals = none) · Extend
  existing harness to uncovered call sites. When the repo has NO LLM calls:
  report "not applicable" — no question.
- **Execute**: deterministic and CI-runnable; small REAL case set grounded in
  actual usage; print a pass/score summary.
- **Done**: evals none→partial.

### 12. Observability  *(one dimension, four wall rows — errors/logs/metrics/
tracing consolidate here; wiring one monitoring connector lights all four)*
- **Assess**: error-tracking SDK present? Which envs report today? Available
  monitoring-type connectors in the context block (sentry, datadog,
  betterstack, pagerduty, uptime_robot).
- **Offer** (per env, local usually skipped): Skip env · Wire existing
  <connector name> — SDK init + DSN env var name + release tagging
  (recommended when the user has one) · Add a new monitoring connector in
  Personas Vault first, then wire it · Logs-only lightweight path.
- **Execute**: init at entry point, unhandled errors + rejections captured,
  env-tagged (environment name in the SDK config), DSN from env var NAME; a
  missing DSN must degrade silently, never crash. Verification: build + a
  smoke that boots the app with no DSN set.
- **Done**: observability errors+ for accepted envs AND the monitoring
  credential is bound as `monitoring_credential_id` on the dev project — the
  wall's four tooling rows read ONLY from that binding, never from the repo's
  `.env` (connectors.md § Binding closes the loop). SDK wiring without the
  binding is not done.

### 13. LLM tracking
- **Assess**: LLM call sites (from §11); tracing SDK present (langfuse,
  helicone, langsmith, tracklight)? Which envs?
- **Offer** (per env): Skip · Wire existing <connector name> at the call-site
  chokepoint (recommended; name the chokepoint file if one exists) · Add new
  LLM-tracking connector in Personas Vault first. No LLM calls → "not
  applicable", no question.
- **Execute**: instrument at the chokepoint (one wrapper, not N call sites,
  creating the chokepoint if the repo lacks one — that refactor is part of
  the value); keys by env var name; env-tagged.
- **Done**: llm tracking present; the tracking credential is bound as
  `llm_tracking_credential_id` on the dev project (or the missing credential
  is the ONE named follow-up when it doesn't exist in the Vault yet).

### 14. Security  *(wall row; usually reached via dimension-scoped dispatch)*
- **Assess**: dependency audit story (npm audit / cargo-deny / Renovate-class
  bot), secret hygiene (secret-scan hook or CI job, no tracked .env files),
  security workflows (CodeQL/audit schedules), input-handling posture at
  trust boundaries (webhooks, file imports, IPC).
- **Offer**: Skip · Audit + secret-scan baseline: dep audit in CI + secret
  scan pre-commit (recommended first rung) · Scheduled scanning: CodeQL/audit
  workflows on a cadence · Findings pass: run the audit NOW and fix/waive
  what it surfaces (mature repos).
- **Execute**: wire real tools the stack already trusts; findings reported
  impact-first (severity = impact, confidence separate, file:line, root-cause
  fix at the sink, coverage disclosure); never bulk-suppress warnings to make
  a scan green.
- **Done**: security level none→audited→gated per what actually runs; open
  findings listed honestly in the report (and the manifest note), not hidden.

### 15. App cost  *(last — composes what the run decided)*
- **Assess**: `app-cost.json` at root? In `.gitignore`?
- **Offer**: Skip · Compose the cost file from this run's connector decisions
  (recommended) — hosting/db/monitoring/LLM services chosen above, each with
  `monthly: null` for the user to fill (never invent prices you don't know;
  a known free tier may be 0 with a note).
- **Execute**: write `app-cost.json`:
  ```json
  { "currency": "USD", "services": [ { "name": "Vercel", "monthly": null, "note": "hosting — test+prod" } ] }
  ```
  and add `app-cost.json` to `.gitignore` IN THE SAME CHANGE (the file is
  personal — the passport reads it locally; it never belongs in VCS).
- **Done**: the wall's App-cost row leaves `missing` for `empty`/`known`.
