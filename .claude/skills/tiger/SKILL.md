---
name: tiger
memory: vault
category: Testing
description: Hunts the highest-value surface of pumper — the LLM call sites — and drives them to their potential across three lenses. (1) Code quality of the AI plumbing (chokepoint, telemetry, caching/dedupe, schema+validation+self-repair). (2) Business value, using the Character method (representative consumers with jobs-to-be-done + a senior-quality bar + time-saved) but TESTING ONLY THE LLM PIECES — does each prompt's grounding and output clear the bar. (3) Model optimization — benchmark the same character inputs across models × effort levels to find degradation/upgrade vs cost/latency. Everything is memorized in a linked Obsidian vault (one note per call site / character / model / session). Pumper's LLM surface is the `claude` engine (a CLI subprocess, not an HTTP SDK). Invoke with `/tiger init|scan|run|benchmark|recall|backlog [args]`.
argument-hint: "[target]"
---

# Tiger — hunt the LLM value (pumper edition)

> In an LLM-powered app the model calls are the apex surface: they cost the most, vary the most, and carry the most business value. Tiger stalks exactly those — ignores the CRUD around them — and never lets a high-value call site sit under-wrapped, under-grounded, or running an over-priced model.

**Read `references/pumper-llm-surface.md` before Phase 1 of any mode.** It is the pre-resolved map of this repo's LLM surface (one engine, one chokepoint, four call sites, the exact benchmark recipe). It exists so a Tiger run does not waste a scan hunting for an OpenAI/Anthropic HTTP SDK — **there isn't one in this repo.**

## What Tiger is (and isn't)

**Is:** a periodic, deliberate pass that (a) builds a durable inventory of every LLM call site, (b) judges each on three lenses, and (c) emits one **session backlog** — the prioritized work to get the most out of the model engine. **Isn't:** a per-commit gate, a generic linter, or a test of non-AI code. If a finding isn't about a model call (or the plumbing/value/economics of one), it's out of scope — that's `/explorer` or `/architect`.

**Real model calls are the point** for lenses 2 and 3 — that's what catches what static reading can't (actual output quality, actual quality-vs-cost trade-off). So Tiger is **cost-aware**: it samples, caches in the vault, and never re-runs an identical (prompt, input, model) it already has a result for. In pumper this matters doubly, because a live call spends real money through the `claude` CLI and is governed by `max_budget_usd`.

## The Obsidian vault — durable, linked memory

Tiger's overlay **is an Obsidian vault** (a folder of markdown with YAML frontmatter and `[[wikilinks]]`). It is the memory: each run reads the prior vault, diffs against it, and writes back, so scan N+1 follows scan N.

**Vault root (pumper):** resolve in this order —
1. `C:/Users/mkdol/Documents/Obsidian/pumper/Tiger/` if the Obsidian vault exists,
2. else `<repo>/.perfect/Tiger/` — the repo vault already used by `/architect`, `/perfect`, and `/explorer` (Obsidian-openable, committed by default so the loop state travels with the repo).

Create it on first `init`. Share `Lessons/` with the sibling skills; own `Tiger/`.

```
Tiger/
  Tiger.md                 # home / Map-of-Content: headline state + links to everything
  config.md                # THE per-app file — seed it from
                           #   .claude/skills/tiger/references/pumper-llm-surface.md
  call-sites/<id>.md       # one note per LLM call site (the inventory — the core asset)
  characters/<name>.md     # durable LLM-focused Characters (JTBD + senior-bar + criteria)
  models/<model>.md        # per-model×effort benchmark rollups (quality/cost/latency)
  sessions/<date-slug>.md  # one note per run: scope, scores, backlog, deltas vs last run
```

Every note carries frontmatter and links. The vault is **append-and-update**: call-site notes are long-lived (status/score/recommended-model evolve); session notes are immutable run records; the home note always reflects current truth.

### Note schemas

**Call site** (`call-sites/<id>.md`) — the unit of value:
```markdown
---
id: <stable-slug, e.g. research-app | fetcher-tier3 | connector-api-watch | trades-common>
type: tiger/call-site
modality: text
file: <path:line of the model call>
wrapper: AppContext::research (metered chokepoint) | direct (engines.claude.research — finding)
provider: claude-cli                model: <role preset or explicit model id>
schema: <json_schema set? yes/no + where>   grounding: <n/m sources that reach the prompt>
quality_score: <0–5 senior-bar>     code_score: <0–5 plumbing>
recommended_model: <from the last benchmark, or "—">
status: discovered | assessed | benchmarked | improved
last_scanned: <YYYY-MM-DD>
characters: ["[[char-a]]", "[[char-b]]"]
---
## What it does
<the job this call performs, in one line, + the app/route that reaches it>
## Prompt & grounding
<prompt template summary; the REAL context that should feed it (target URL, prior dataset
records, catalog metadata, schema of the expected output) and how many actually reach it
→ grounding n/m, cite file:line>
## Code quality (chokepoint · telemetry · caching)
<does it go through AppContext::research (cache + budget governor + metering) or bypass it?
json_schema set + validated + self-repair on parse failure? cost/turns/session_id recorded
back into the job result? cache key correct? prompt bloat? timeout/turn caps? — cite file:line>
## Findings
<impact-scored items across the 3 lenses; link [[session]] where raised>
```

**Character** (`characters/<name>.md`) — representative *consumers of pumper's output*, narrowed to model outputs:
```markdown
---
name: <First role-tag>     type: tiger/character
maps_to: ["[[call-site]]", …]   # the LLM surfaces this Character exercises
references: [<url> — bar it sets]
---
## Who they are / Background / Voice   (authentic texture)
## Jobs to be done   (what they hire the MODEL OUTPUT for)
## Senior-quality bar   (the floor: output ≥ what they'd produce as a senior in role)
## Time-saved (motivation)   (manual-research minutes → with-pumper minutes, as a NUMBER)
## Scored acceptance criteria (judged identically every run, applied to the OUTPUT)
- [ ] grounded in MY real context (names the supplied URL/entity/dataset, no placeholders)
- [ ] senior-grade (specific, correct, citable, not generic)
- [ ] machine-usable (parses against the declared json_schema; keys stable across runs)
- [ ] worth the latency/cost (vs the http or browser tier doing it deterministically)
```

pumper's Characters are **other agents and apps on this machine** plus the humans behind them — the consumers named in `ONBOARDING.md` §4 and in `catalog/data-sources.toml` (`confidence`, `category`). Derive them from real consumers, not a generic roster.

**Model** (`models/<model>.md`): per call-site rows of `{quality, costUsd, latencyMs, verdict}` at each effort level, + the headline recommendation.

**Session** (`sessions/<date>.md`): scope, the inventory delta, per-lens findings, the impact-ranked backlog, the model-opt recommendations, and the **value ledger** (grounding + time-saved rolled up; what the engine *promises* vs *delivers*).

---

## The three lenses

### Lens 1 — Code quality of the AI plumbing (static, code-grounded)
For each call site, follow the call chain to the actual subprocess spawn and score, citing `file:line`:
- **Chokepoint:** does the call go through `AppContext::research()` (`crates/core/src/app.rs`) — which adds the disk research cache, the per-job budget governor, headroom clamping, and cost metering — or does it reach `ctx.engines.claude.research(...)` / `self.claude.research(...)` directly and skip all of it? **A direct call is a Lens-1 finding by default**; the only defensible exception is a call that must bypass the cache (e.g. `resume_session`), and it must say so.
- **Schema + validation + self-repair:** is `ResearchRequest::json_schema` set so the CLI constrains the answer (`--json-schema`)? Is the parsed output validated and normalized, and is there a re-prompt path when it doesn't parse? A `serde_json::from_str(...)?` with no repair is a finding on any call site whose output feeds a dataset.
- **Telemetry:** the engine reports cost / turns / session id back in the job result — is it actually propagated and stored, or dropped? Is `tracing` emitting model, effort, timeout, and outcome? Are failures observable through `/metrics` and `/events`?
- **Caching / efficiency:** is the research cache enabled and is its key right (`ResearchCache::key`)? Is the prompt **bloated** (whole page bodies where `html_to_markdown` + a digest would do)? Are `max_turns` / `timeout_secs` / `max_budget_usd` sensible? Is the escalation into the claude tier (`crates/core/src/fetcher.rs`) firing more often than the content actually warrants — every unnecessary escalation is a paid call.
- **Tier discipline:** is this call site doing something the `http` or `browser` engine could do deterministically? The most valuable Lens-1 finding in this repo is "this doesn't need the model at all."
- Emit code-quality findings + concrete fixes (route through the chokepoint, set a schema, add a repair path, tighten the escalation threshold).

### Lens 2 — Business value (Character method, scoped to the output)
1. **Characters** (durable in the vault) — representative consumers, each `maps_to` the call sites their JTBD hits.
2. **L1 (theoretical, mass-parallel):** per `character × call-site`, read the prompt + grounding and judge the *designed* output against the Character's senior-bar + scored criteria + time-saved. Score **grounding n/m** (how much of the consumer's real world reaches the prompt) — this is Tiger's highest-yield finding type, fully visible in code.
   **Grounding bar (hard rule):** every verdict must **quote the actual prompt text** (the real template string at `file:line`, not a paraphrase) **and at least one real sampled output.** Real outputs are available in this repo without spending money — look in `data/` artifacts, the SQLite `jobs` table (`result` column), dataset records via `GET /datasets/{app}/{ds}`, and the research cache on disk. Never judge from the call-site *name*. If no output sample exists anywhere, mark the verdict `ungrounded — needs L2 sample` instead of guessing.
3. **L2 (empirical, optional but ideal):** actually **run the job** with character-shaped params and judge the live output:
   ```bash
   just run     # = cargo run -p pumper-server --bin pumper (the --bin is required)
   # then, in another shell:
   curl -s -X POST http://127.0.0.1:8088/apps/research/jobs \
        -H 'content-type: application/json' \
        -d '{"params":{"query":"<character-shaped query>"}}'
   curl -s http://127.0.0.1:8088/jobs/<id>     # poll to succeeded; result carries cost + turns
   ```
   Assert the output names the supplied real entity / reflects the requested schema, not placeholders. One confirmed live finding beats ten theoretical ones.
- Emit business-value findings (grounding gaps, senior-bar misses, "the http tier would have been better") + suggested fixes.

### Lens 3 — Model optimization (the alternative scenario)
The Characters are the **consistent judgment harness** that makes cross-model comparison fair. For the selected call sites:
1. Hold the prompt + character input fixed; **run it across a model matrix** — models × effort levels. pumper's matrix axis is native: `[claude.roles.*]` in `config.toml` defines named presets (`research` = Sonnet @ `high`, `compose` = Opus @ `xhigh`), and `ResearchRequest` accepts `model`, `effort`, and `max_budget_usd` overrides per job, so a benchmark cell is one POST with an overridden param — no code change and no API key.
2. Have the **same Character criteria** judge each output (blind to which model). Use **forced ranking with a named separator per adjacent pair** — absolute 0–5 scoring saturates. Record **cost + latency** per cell; the job result already carries `cost_usd` and turns, and job timestamps give latency.
3. Find the frontier: the cheapest/fastest cell that still clears the senior-bar (a **downgrade** opportunity) and any call site where a **stronger** model/effort meaningfully lifts quality (an **upgrade** worth the spend). Watch for **degradation** (a model that silently drops grounding or hallucinates a citation).
- Emit a per-call-site model recommendation: `keep | downgrade to X | upgrade to Y` with the quality/cost/latency evidence, written to `models/*` and the call-site note's `recommended_model`. A recommendation that changes a role preset is a `config.toml` change — name the exact key (`[claude.roles.research].model`).

> **Judging rules (inherited, measured 2026-07):**
> Quality is judged by Characters, never by the model under test grading itself.
> - **Cross-family comparisons need ≥2 judge families or a human spot-check.** Judges rank their own model family first. Within-family (effort) comparisons from one judge are fine — and in this repo the matrix is mostly within-family (Sonnet vs Opus vs Haiku, all Claude), so single-judge effort comparisons are the well-supported case.
> - **More effort is not better.** On long-form output, quality *inverted* above medium effort. Length is not insight; never recommend an effort upgrade on prose evidence alone.
> - **A hard output cap collapses the effort axis.** Don't benchmark effort on a call site pinned by a tight `json_schema` or a low `max_turns` — you'd pay for reasoning you don't get.
> - **When every cell disappoints, suspect the prompt framing before the model.** Emit a *value* (Lens 2) finding, not an *upgrade* recommendation.
> - **pumper-specific:** when every cell disappoints AND the target is a structured page, suspect the *tier* before the prompt — the finding may be "use `engine-http` + a `core::extract` rule set," which costs nothing per run.

---

## Modes

- **`init`** — resolve the vault root; scaffold `Tiger/` + `config.md` **seeded from `references/pumper-llm-surface.md`** (do not re-derive the surface from scratch — verify the pre-resolved map against the current code and record any drift); run the first **inventory scan**; derive the consumer set from `catalog/data-sources.toml` + `ONBOARDING.md` §4 and **ask how many Characters (1/5/10)**; draft Characters; write `Tiger.md`. No lens runs yet.
- **`scan`** — re-inventory, **diff against the vault**: new / removed / changed call sites (prompt or schema drift vs the recorded fingerprint), update notes, flag regressions. Cheap; run often. The cheap inventory command is in the reference file.
- **`run [--lens code|value|model|all] [--chars N] [--live]`** — the full pass. Default `--lens all`, L1 only; `--live` adds Lens-2 real jobs and Lens-3 benchmark on the selected/highest-value call sites. **Mass-parallel:** one subagent per `call-site` (Lens 1) and per `character × call-site` (Lens 2). Writes a `sessions/<date>` note + refreshes call-site notes + the backlog.
- **`benchmark <call-site> [--models …] [--effort …]`** — Lens 3 only, deep, for one call site.
- **`recall`** — read the vault and summarize current state (top call sites by value, open backlog, last session, model recommendations) without re-scanning.
- **`backlog`** — (re)emit the impact-ranked backlog from current findings across all three lenses.

## The session backlog (the deliverable)

One impact-ranked list (frequency × reachability × value, not raw severity), each item tagged by lens and linking its `[[call-site]]`:
- **code** — chokepoint/telemetry/caching fix (e.g. "route `connector-api-watch` through `AppContext::research` — restores cache + budget governor").
- **value** — grounding/quality fix (e.g. "feed the prior dataset record into the watch prompt so the model diffs instead of re-describing — grounding 1/4 → 3/4").
- **model** — model/effort swap (e.g. "`research` role → Haiku @ medium: equal senior-bar on the gather step, −70% cost"; keep Opus for `compose`).
Plus a **value ledger** (grounding & time-saved rolled up; promised vs delivered) and the **strengths to protect**. The chat reply is the headline + sharpest findings, linking `file:line` and vault notes.

## Executing a backlog item

Tiger may fix what it finds, in-session, when the user accepts an item. Same discipline as the sibling skills:
- Validate with `just check` → `just test` → `just lint` → `just fmt-check` (or `just ci` for the whole gate). The repo-root `justfile` is the canonical task runner — `cargo install just`, then `just --list`; the raw cargo equivalents are in `CLAUDE.md`'s commands table.
- A prompt/schema/tier change is user-visible: update the coupled doc (`docs/features/fetching.md`, `docs/features/apps.md`, or `docs/features/runtime.md` — see `scripts/docs/feature-doc-map.json`) in the same session.
- Per `.claude/CLAUDE.md`, a fix ships as an **extracted, named function with a test** (`x_not_y` naming), not as an inline patch in a `run()` body. A prompt-builder or an output-validator is exactly the right shape to extract.
- Stage only your explicit paths in ONE bash invocation (`git add <paths> && git diff --cached --stat`), never `git add -A`; commit prefix `tiger:`; never `--amend`.

## Concurrency & trust
- **Mass-parallel** Lens 1 + Lens 2-L1 (no I/O to serialize — one subagent per unit). Lens 2-L2 and Lens 3 make real, **paid** calls → serial-ish and cost-bounded. Always set `max_budget_usd` on benchmark jobs; cache every result in `models/*` keyed by (call-site, model, effort, input-hash) so re-runs are free.
- **Evidence or it didn't happen:** every finding cites `file:line` (static) or a captured output/metric (live).
- **Adversarial judging** for value + model verdicts; default to "not better" unless the output earns it. Forced ranking over absolute scores.
- **Honest ceilings:** name what still isn't grounded/optimized after a fix.
- **Vault hygiene:** call-site `id`s are stable across runs; never duplicate a note — update it. Record the prompt/schema **fingerprint** so `scan` can detect drift.
- **Vault-write verification (inherited lesson, 2026-06-20):** a discovery/scan subagent may be unable to write files in some harnesses and will return the note bodies inline instead. After any parallel scan, the orchestrator MUST `ls` the target dir, diff against the expected `id` set, and **backfill** any missing notes from the agents' returned content — don't trust "wrote N notes" without checking.
- **Lens-3 recipe:** in this repo the cleanest matrix runner is the server itself (one POST per cell with `model`/`effort` overrides), because it exercises the real prompt, the real CLI, and reports real cost. The Agent-tool subagent recipe (one subagent per cell with `model`/`effort` params) remains available for judging and for dry cells. Judge the cells with a separate model, never the one under test. Note: reasoning content is redacted in transcripts, so effort can only be measured by output tokens + outcome.
