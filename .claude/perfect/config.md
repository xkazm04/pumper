---
product: "pumper"
stack: "Rust workspace - tokio + axum + sqlx/SQLite; tiered fetch runtime (http -> browser -> claude); declarative extraction; dataset store with change detection; embedded tantivy search; ScrapeApp crates under crates/apps/*"
vault: ["C:/Users/mkdol/Documents/Obsidian/pumper", "C:/Users/mkdol/dolla/pumper/.perfect"]
vault_subdir: Perfect
base_branch: master
wave_size: 3
lot_caps: {rust: 2}
pool_target: 10
round_shape: pool
cooldown_rounds: 2
commit_format: "feat(<context>): <title>"
context_map: context-map.json
active_runs_ledger: ""
locale_count: 1
---

# Perfect overlay - pumper

Project specifics for the `/perfect` lane skill (`ai-registry/skills/perfect` v2.3, linked at
`.claude/skills/perfect`). Extracted 2026-08-24 from the project-owned copy the lane replaced,
merged with the vault overlay that preceded it.

**Historical `## Skill improvement log`** (rounds 1-24, ~600 lines of dated director lessons)
stays where it was written: `.perfect/Perfect/config.md`. Read it at Phase 0 alongside this
file; append **new** entries to the section at the bottom of THIS file.

## The product, in one line

pumper is a Rust scraping/data-product service: a tiered fetch runtime (http -> browser ->
claude), declarative extraction, a dataset store with change detection, embedded search, a job
server (axum) with cron/triggers/webhooks, and a fleet of `ScrapeApp` crates producing domain
datasets (grants, labour markets, trades economics). Its users are API consumers and CLI
agents - "UX" here means API ergonomics, dataset quality, observability and operational
robustness, not pixels.

## Gates

- `always:` `cargo check --workspace`, then `cargo test --workspace --lib` (fast, no network).
- `slow:` `just ci` (= `cargo fmt --check` + `cargo clippy --workspace --all-targets` +
  `cargo test --workspace`) - run `run_in_background`, read the output before the next
  state-changing action.
- `builder:` `cargo check --workspace` + targeted `cargo test -p <crate> --lib`, plus driving
  the real flow (spawn the server on a scratch DB/port, curl the endpoint, run the app via
  `POST /jobs`) when feasible. Builders report honestly what they could NOT verify.
- **Clippy calibration: no NEW warnings in files the diff touched** - never full-crate
  `-D warnings`. And compare a warning's **content**, never its line number: a wave that adds
  lines above an expression moves it, and a count-only check ("31 before, 31 after") is fooled
  by coincidence. `grep -F` the expression text against `git show master:<file>` is the only
  correct form. (Counting note: clippy reports lib+test duplicates of the same site - compare
  SITES, not counts.)
- **Never pipe a gate through `tail` / `head` / `grep`** - you keep the pipe's exit code and
  lose the evidence. Redirect to a file (`> log 2>&1; ec=$?`) and read the file.
- **`cargo test --workspace` fail-fasts on the first failing test binary**, so a red run's
  "N passed" is a partial count of an aborted run. `--no-fail-fast` is mandatory the moment
  anything goes red.
- Doc-sync: a consumer-visible change updates the mapped `docs/features/*.md`; the Stop hook
  (`scripts/docs/check-doc-sync.mjs`) demands it anyway.

## Class B

Nothing, by design. Four consecutive waves have shipped with **zero Class B files**, and the
standing rule is to treat "this wave needs a Class B file" as a signal to re-check the pairing
rather than a protocol to execute. `git commit --only` commits working-tree content, so on a
genuinely shared file each lot's commit carries the sibling's in-flight hunks - harmless to
correctness, fatal to per-commit attribution.

## Class C

Director-only:

- `scripts/docs/feature-doc-map.json` - the doc map. **Derive the wave partition FROM this
  file at plan time**, do not validate against it afterwards; every `crates/apps/**` edit
  drags `docs/features/apps.md` through it, so app-vs-app pairings must budget the shared
  feature doc as Director work from the start.
- `docs/features/apps.md` and any other feature doc two lots would both touch (builders report
  their doc text; the Director writes it).
- `context-map.json`, `catalog/data-sources.toml`, the git index.
- Applying a Class C item is **not bookkeeping - review it like a diff.**

## Repo law

Digest pasted verbatim into every builder brief:

- Read `docs/harness/harness-learnings.md` first; its conventions and anti-patterns bind you.
- **VERIFY the brief's claims before building on them.** The `file:line` evidence and any
  external-API field names come from a read-only scout and MAY BE WRONG. Check the code / the
  live source first; report any correction in your final report. A "brief claims I refuted"
  section is a **required** report section.
- TEXT timestamps: fixed-width RFC 3339 UTC micros via the `ts()` helpers (lexicographic =
  chronological).
- Record keys are stable external ids; never `sync_many` on filtered/partial scrapes.
- Job results stay compact JSON; large payloads via `ctx.save_artifact`.
- Every outbound webhook/event goes through `webhook::dispatch_event` - never hand-roll a
  reqwest send.
- New list endpoints follow the `cursor=` keyset pagination convention (`{items, next_cursor}`).
- Config struct fields: `#[serde(default)]` + a manual `Default` impl (both, always).
- Migrations are append-only, next number in `crates/core/migrations/`; `sqlx::migrate!` picks
  them up.
- A new app crate = workspace dep in root `Cargo.toml` + `crates/server/Cargo.toml` +
  a `registry.rs` line.
- Apps meter LLM/fetch spend via `ctx.fetch` / `ctx.research`, never `ctx.engines.*` directly.
- Dependency rule: `apps -> core <- engines`; only `server` depends on everything.
- Consumer-visible change => update the mapped `docs/features/*.md` in the SAME commit.
- e2e lives at `crates/server/src/e2e/`, **not** `tests/`.
- If a guard blocks you, report it - never rephrase around it; the row is the Director's to carry.
- Commit each direction the moment it is done and verified; never batch for the end.
- At most 2 files per turn, offset/limit over ~600 lines, never six large Reads in one message
  (the harness stream-watchdog kills builders at the 600s no-progress mark). Include a reading
  ORDER in any brief whose write set exceeds ~2,000 lines.
- If about to run out of room, commit a `wip(...)` whose message pastes the last error verbatim.

Out-of-scope walls: touching another context requires `DECISION NEEDED`. Declare a mechanical
ripple (a test fixture two lines wide in an out-of-set file) as an exception in the report
rather than contorting around the boundary.

## Context sources

`context-map.json` at the repo root is the queue AND the name source. **Verify provenance, not
shape** - read `project.root` and `project.id` and compare them to this checkout:

```bash
node -e 'const m=require("./context-map.json");console.log(JSON.stringify(m.project||{id:m.projectId,root:m.projectPath}))'
# must print root: C:\Users\mkdol\dolla\pumper  .  id: 512809db-ba9b-4a0e-80b6-5bfb7e3051e9
```

| Signal | Meaning | Use its names? |
|---|---|---|
| `project.root` = this checkout AND `project.id` = this repo's app project id | The app's export for this machine | **Yes** |
| App-export shape but `project.root` is some other path | A foreign machine's map committed here | **No** |
| `"$schema": "https://vibeman.dev/..."`, `"version": "2.0.0"` (string), `groups[].contexts[]` | A stale foreign Vibeman auto-map | **No** |

Shape alone is not enough - a sibling repo's map passes every shape test and points at a
different machine. As of 2026-08-03 the app has scanned this repo: 46 contexts across 8 groups,
kebab-case (`api-surface`, `browser-engine`, `claude-engine`, `archive-engine`, ...). Those
names are authoritative for both the outbox `context` field and the queue in `Perfect.md`.

If the provenance check fails, emit outbox nodes **without** the `context` field until a
re-scan and say so once in the session summary. Re-scan without the UI:

```bash
curl -s http://127.0.0.1:17400/dev-tools/projects
curl -s -X POST http://127.0.0.1:17400/dev-tools/scan-codebase \
  -H 'content-type: application/json' --data-binary @body.json   # {"project_id":"...","delta_mode":true}
curl -s http://127.0.0.1:17400/dev-tools/scan-status/<scan_id>
```

Port 17400 unless taken (the server scans upward). Send bodies as `--data-binary @file` - a
Windows path in a shell-quoted `-d` loses a backslash and the JSON parser rejects it.

The Personas app DB is `%APPDATA%/com.personas.desktop/personas.db`; project
`512809db-...` = pumper. The wrong DB **fails silently**
(`%LOCALAPPDATA%/personas-athena-test/personas.db` has the same `dev_contexts` schema, 89 rows,
and no pumper project). `dev_projects` has `root_path`, not `path`.

Other Phase 0 rituals: read `docs/harness/harness-learnings.md` (structural facts,
anti-patterns, **Open follow-ups** = pre-vetted direction seeds); scan `MEMORY.md` for vetoing
signals; `docs/harness/vision-scan-2026-07-10/` records prior waves - anything shipped there is
NOT novel. When the map's context COUNT or NAMES change wholesale, void cooldowns, re-score
from scratch, and record an old->new mapping in `Perfect.md`.

Coverage is recomputed by the committed script `.perfect/coverage.mjs` - never a snippet
retyped into a session note. The rule has **three** clauses (by direction, by
`last_proposed`, by explicit verdict); a number computed with fewer than all three is wrong,
and a disagreement with the shipped ledger is the computation's bug until proven otherwise.
Any vault metric computed by regex must normalize CRLF first.

## Smoke

Boot `just run` (= `cargo run -p pumper-server --bin pumper`; `--bin` is required - three
binaries, no `default-run`) on a **scratch config + port**: copy `config.toml`, point
`[storage]` at a scratch path, change the port. Drive one real job, curl the new endpoints,
tear down. Default port `http://127.0.0.1:8088`.

Smoke checks are code too - verify response shapes and state dependencies against the source
before asserting, exactly like a builder verifying a brief claim. Copy field names from the
route's request struct or the feature doc, never from the model/migration. Every build phase
ends with the smoke pass, and new API-surface directions add their endpoints to the checklist.

Known limit: Windows cannot deliver Ctrl-C to a detached process, so the harness cannot drive
the graceful-shutdown path; it stays e2e-proven only.

## Opportunity arcs

Data-product quality; trigger/pipeline maturity; API ergonomics. Opportunity =
consumer-facing reach x headroom x strategic fit against those arcs.

## Vetoes

- **API-key auth** - rejected explicitly 2026-07-13. Parked stays parked; do not re-propose
  unprompted.
- **LLM-driven features** - three rejections (most recently `rules:"auto"`, an LLM drafting a
  RuleSet). Deterministic engine work >> LLM features. Do not slate LLM directions unless the
  user asks.
- **Trigger fan-in barriers** - a documented non-goal.

## User taste

- **The deeper into the engine the slate goes, the higher the acceptance rate.** Three rounds
  of evidence, including two clean 10/10 sweeps on engine-level substrate (query batching,
  algorithmic complexity, capability tokens, retention, test harnesses). Keep consumer-facing
  / API-surface directions to **at most one per slate**.
- 2026-07-13: in the trades context the user kept substrate/data-correctness directions and
  rejected the consumer-facing ones. Exception: for the API context they took everything
  EXCEPT auth, including the wildcard (OpenAPI) - infra polish is welcome.
- Treat the pool target as a **soft** target and present full slates; the user accepted 4
  directions when told only 2 slots remained.
- Ask before resource-destructive actions even when obviously regenerable (a 100 GB cache
  deletion was approved without hesitation and was still right to ask).

## Model policy (builders)

Default **sonnet**; escalate a whole brief to **opus** if any direction trips a trigger: sized
L, or 3 directions each rewriting a core file - concurrency/correctness-critical work
(locking, fencing, cancellation, cache coherence, crash recovery) - a new public seam other
contexts build on (a trait, a core data structure, a cross-crate contract, a new crate) - an
acceptance criterion that hands over a *design decision* rather than a spec - a
schema/migration other contexts read, or an algorithmic rewrite needing a correctness proof -
a redo after a rejected diff (never re-run the same brief on the same model).

Watch item: a brief resting on **unverified external-source facts** is a candidate trigger -
`grants-schema-enrichment` looked Sonnet-shaped and its real value came from the builder
disbelieving the brief and checking the live API.

Record the model in each direction's `## Build record`. The Director's review bar does NOT
change with the model. Log every escalation, mid-flight upgrade or rejected diff here with the
trigger that should have caught it.

## Worktrees and build isolation

```bash
git worktree add .claude/worktrees/perfect-<ctx> -b worktree-perfect-<ctx>
# Each concurrent builder gets its OWN target dir:
#   CARGO_TARGET_DIR=C:/Users/mkdol/dolla/pumper/target-<ctx>
# FORWARD SLASHES - Bash mangles the backslash form into a literal `Usersmkdol...`
#   directory inside the worktree (measured 2026-08-04).
# Check free disk BEFORE launching a wave; rm -rf target-<ctx> for every builder at Wrap.
```

A shared `target/` across concurrent agent sessions produces stale-rlib linkage failures; the
cold first build is worth it (zero incidents across 7 concurrent builders once per-builder
dirs landed). **Sequential > parallel within one context** - run a >3-direction context in
waves (B1 -> merge -> B2 on a `reset --hard master` worktree) so the later builder builds on
the merged earlier work. Parallelize ACROSS contexts, serialize WITHIN one.

Run `git -C <path>` instead of `cd` chains in the Director shell - a leftover `cd` made a
cherry-pick silently run in the wrong repo.

## Guardrails (repo-specific)

- Never stash; never `git add -A` on master; per-file staging with a staged-count check before
  every commit. Worktree WIP snapshots are the one exception (isolated tree, add-all is safe).
- `git log <base>..master` before every merge - a sibling session landing a commit mid-wave
  turns an ff-merge into a cherry-pick. Cherry-pick in **chronological** order (`git log` is
  newest-first).
- Union-merge conflicts are not automatically safe: read every seam. A keep-both on
  `docs/features/runtime.md` would have shipped a duplicated, self-contradicting doc.
- Migration check at review: any new migration is append-only (next number in
  `crates/core/migrations/`), and any new dataset/app is registered end-to-end.
- Docs-vs-code check at review: when a diff documents a behavior, grep for the code that
  implements it before merging.
- When a diff removes or rewrites an existing test, the review must name what still enforces
  the old test's intent - if nothing does, that is a redo.
- The doc-sync Stop hook is satisfied by ANY `docs/features/*` edit, so it proves a doc was
  touched, never that the RIGHT docs were. When a diff adds a member of an enumerated set (an
  event kind, a status, an outcome, a trust level), grep the docs for the other members by
  name and update every place the set is listed.
- Fix recurring flakes as a Director commit rather than re-diagnosing them; if a test flakes in
  two consecutive rounds, end it.
- The vault is in-repo, so commit review-state per-lot rather than per-session - a session
  death otherwise loses ~176 lines of uncommitted review work.

## Skill improvement log

New entries go here; rounds 1-24 remain in `.perfect/Perfect/config.md`.

- 2026-08-24: the project-owned `/perfect` copy was retired in favour of the registry lane
  skill; every pumper-specific fact above was extracted from it into this overlay.
