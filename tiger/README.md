---
type: tiger/home
app: pumper
skill: tiger (registry lane, linked at .claude/skills/tiger)
created: 2026-08-24
---

# Tiger - pumper

This directory **is** the overlay and the cross-session vault for the `/tiger` lane skill
(`ai-registry/skills/tiger` v2.1). The skill body is the stack-agnostic engine; everything
below is pumper.

Seeded 2026-08-24 from `.claude/skills/tiger/references/pumper-llm-surface.md` in the
project-owned copy the lane replaced. **Verify each anchor against the current code before
trusting a line number** - they drift - but do not re-derive the shape: it is a deliberate
architectural fact, not an accident of one scan.

> **The single most important fact:** pumper does **not** use an HTTP LLM SDK. There is no
> `anthropic`, no `openai`, no `reqwest` call to `api.anthropic.com`, no API key in
> `config.toml`, no `Bearer` header anywhere in the model path. The one and only `Researcher`
> implementation **shells out to the local `claude` CLI**. Do not spend a scan looking for an
> SDK. `grep -rn "api.anthropic\|openai\|OPENAI_API_KEY\|ANTHROPIC_API_KEY" crates/` returning
> nothing is the expected result, not a discovery gap.

## Layout

```
tiger/
  README.md              # this file - home + the per-app config
  models.md              # model x thinking matrix + the dated price/role snapshot
  characters/_roster.md  # who judges, their AI-surface angle and use_case binding
  call-sites/<id>.md     # one note per LLM call site (the inventory - the core asset)
  sessions/<date>.md     # immutable run records
  engine/_expected/*.md  # call sites the jobs imply but the code lacks
```

## Jobs / use cases

Not yet declared - runs use one `cross` frame until they are. The consumers to derive them
from are **other agents and apps on this machine** plus the humans behind them: those named in
`ONBOARDING.md` section 4 and in `catalog/data-sources.toml` (`confidence`, `category`).
Derive Characters from real consumers, not a generic roster. There is no `uat/` overlay in
this repo to reuse.

## The engine (the only provider)

- **Impl:** `crates/engine-claude/src/lib.rs` - `impl Researcher for ClaudeEngine`.
- **Mechanism:** spawns `claude -p --output-format json`, prompt over **stdin**, JSON parsed
  back out. On Windows it routes through `cmd /C` because npm installs `claude` as a
  `.ps1`/`.cmd` shim that `CreateProcess` cannot spawn directly.
- **Flags it can emit** (from `ResearchRequest` + `[claude]` config): `--model`, `--effort`,
  `--max-budget-usd`, `--bare`, `--dangerously-skip-permissions`, `--allowedTools`,
  `--max-turns`, `--resume <session>`, `--json-schema <schema>`, `--append-system-prompt`.

Consequences:

- The **model matrix is native** - `model` and `effort` are per-request params, so a benchmark
  cell is a param override: no code change, no new credential.
- The **cost signal is native** - the CLI's JSON output carries cost / turns / session id and
  they are propagated back into the job result.
- The **schema signal is native** - `--json-schema` constrains the answer. A call site shipping
  structured data into a dataset without `json_schema` is a Lens-A finding.
- **`--dangerously-skip-permissions` is deliberate** (`ONBOARDING.md` section 2, the local-first
  power trade). Never file it as a security finding.

## The chokepoint

**`AppContext::research()` - `crates/core/src/app.rs` (~line 319).** Every call site should go
through it. It adds, in order:

1. **Disk research cache** - `ResearchCache::key(&req)`; a hit is served at zero cost and
   metered as a `cache_hit` event with the dollar amount saved. `resume_session` requests
   deliberately bypass the cache.
2. **Budget governor** - `require_budget()` refuses to start once the job budget is exhausted
   and clamps the per-call ceiling to the remaining headroom (`clamp_to_headroom`).
3. **Cost metering** - every call, hit or miss, through `self.meter("claude", ...)`.
4. **Cache store** on miss, so the next caller is free.

`AppContext` also carries the tiered fetcher (`http -> browser -> claude`) with the same
metering, so escalation costs are visible too. **Bypassing the chokepoint loses all four** -
that is the default Lens-A finding shape in this repo.

## Discovery

The cheap inventory command, run on every `scan`:

```bash
grep -rn "\.research(" crates/ --include=*.rs
grep -rn "ResearchRequest::new\|json_schema\|append_system_prompt\|with_role" crates/ --include=*.rs
```

Known sites when this map was written (four, plus the chokepoint itself). Modality is `text`
for every site - there is no image, vision, embedding or audio call in this repo.

| id | file:line | route to the model | notes for the scan |
|---|---|---|---|
| `research-app` | `crates/apps/research/src/lib.rs:101` | `ctx.research(request)` - **through the chokepoint** | Two prompts pinned to the same shape; uses `with_role(role)` and a session id for the multi-step gather->compose pipeline (`--resume`). The reference implementation of a good call site; use it as the comparison bar. |
| `trades-common` | `crates/apps/trades-common/src/lib.rs:35` | `ctx.research(request)` - **through the chokepoint** | Shared helper behind several `trades-*` crates, so one prompt serves many datasets: high blast radius, high grounding leverage. Check whether each caller's real context (market, trade, prior record) reaches the prompt. |
| `connector-api-watch` | `crates/apps/connector-api-watch/src/lib.rs:301` | `ctx.engines.claude.research(req)` - **direct, bypasses the chokepoint** | Skips cache + budget governor + metering. Re-verify before filing; if still direct and not justified by `resume_session`, a ready-made Lens-A backlog item. |
| `fetcher-tier3` | `crates/core/src/fetcher.rs:485` | `self.claude.research(research)` - **inside the tiered fetcher** | The auto-escalation tier: any app using the tiered fetcher can reach the model implicitly. The highest-cost-surprise surface in the repo - a too-eager threshold turns free http fetches into paid model calls. It sits *below* `AppContext`, so confirm which governance it actually inherits rather than assuming. |

## Hard checks (Lens A must enforce)

1. **Bypassed chokepoint** - `engines.claude.research` / `self.claude.research` instead of
   `ctx.research`. Costs cache + budget + metering.
2. **Unschema'd structured output** - output feeds a dataset but `json_schema` is unset and
   there is no validate/repair path. Silent shape drift becomes silent dataset corruption.
3. **Over-eager tier-3 escalation** - the fetcher escalating to the model on thin content the
   `browser` tier would have rendered. Every escalation is a purchase.
4. **The model doing a parser's job** - a call site extracting fixed fields from a stable page.
   `core::extract` (CSS/regex/JSON-pointer rules, compiled once, run across all cores) does it
   deterministically, free and testably. **The highest-value finding class in this repo.**
5. **Grounding starvation** - the prompt gets a URL and a task but not the prior dataset
   record, the catalog metadata (`catalog/data-sources.toml`: `market`, `category`,
   `confidence`), or the schema of the expected output. Cheap to fix, large quality delta.
6. **Cost telemetry dropped** - the engine reports cost / turns / session id but the app
   discards them instead of returning them in the job result, so `/metrics` under-reports spend.
7. **Role mismatch** - a gather-shaped call running the `compose` (Opus @ xhigh) preset, or a
   synthesis-shaped call running `research`. One `with_role` change.
8. **Missing turn/timeout caps** - no `max_turns`, no `timeout_secs`, on a call that can loop.

## Expected kills

The highest-value outcome of a pumper Tiger run is **"this doesn't need the model at all"** -
a call site retired in favour of `engine-http` + a `core::extract` rule set. When every
benchmark cell disappoints AND the target is a structured page, suspect the **tier** before the
prompt.

## Fixtures - where real outputs live (free L2 evidence)

Never claim "no sample available" before checking these:

- **SQLite job rows** - `data/pumper.db`, `jobs.result` (JSON) and `error` for failures. Also
  over HTTP: `GET /jobs?app=research&status=succeeded&limit=10`, then `GET /jobs/{id}`.
- **Datasets** - `GET /datasets/{app}/{ds}` (and `/duplicates` for near-dup evidence). The
  store's change detection tells you whether the model's output is *stable* across runs - a
  quality signal static reading cannot produce.
- **Per-job artifacts** - `data/artifacts/<job-id>/` (raw dumps, HTML, JSON).
- **Research cache** - the chokepoint's on-disk cache holds prior `ResearchOutput`s verbatim.
- **`/events` SSE + `/metrics`** - live job transitions and Prometheus gauges.

**Grounding bar (hard rule):** every Lens-B verdict must quote the actual prompt text (the real
template string at `file:line`, not a paraphrase) **and** at least one real sampled output.
Never judge from the call-site name. If no sample exists anywhere, mark the verdict
`ungrounded - needs L2 sample` instead of guessing.

## Model-invocation recipe (Lens C / Lens B L2)

The cleanest matrix runner in this repo is the server itself - one POST per cell, exercising
the real prompt, the real CLI, and reporting real cost.

```bash
just run                            # = cargo run -p pumper-server --bin pumper -> http://127.0.0.1:8088
                                    # `--bin pumper` is REQUIRED: the package also ships
                                    # `reindex` and `search-backfill` and has no default-run.
                                    # `just dev` is the same with RUST_LOG=debug.

# one benchmark cell = one POST with model/effort/budget overrides
curl -s -X POST http://127.0.0.1:8088/apps/research/jobs \
     -H 'content-type: application/json' \
     -d '{"params":{"query":"<character input>","model":"claude-haiku-4-5","effort":"medium","max_budget_usd":0.25}}'

curl -s http://127.0.0.1:8088/jobs/<id>     # poll to succeeded; result carries cost + turns
```

Check the app's `description()` (or `GET /apps`) for the exact param names it forwards into
`ResearchRequest` - apps differ, and passing an unknown key silently does nothing.

**Always set `max_budget_usd` on benchmark jobs.** Cells are paid and the governor is the only
thing between a matrix run and a real bill. Cache every cell in `models.md` keyed by
(call-site, model, effort, input-hash) so a re-run is free.

The Agent-tool subagent recipe (one subagent per cell) remains available for judging and for
dry cells. Judge with a separate model, never the one under test. Reasoning content is
redacted in transcripts, so effort can only be measured by output tokens + outcome.

pumper-specific judging rider: the matrix here is mostly **within-family** (Sonnet vs Opus vs
Haiku, all Claude), which is the well-supported case for a single judge.

## Price basis

`models.md`, derived from `[claude.roles.*]` in `config.toml` (see below). There is no price
table in code - the CLI bills through the local `claude` install - so the snapshot is a dated
public list-price estimate plus the measured `cost_usd` the job results report.

## Executing a backlog item

- Validate with `just check` -> `just test` -> `just lint` -> `just fmt-check`, or `just ci`
  for the whole gate. The repo-root `justfile` is the canonical task runner; raw cargo
  equivalents are in `CLAUDE.md`'s commands table.
- A prompt / schema / tier change is user-visible: update the coupled doc
  (`docs/features/fetching.md`, `docs/features/apps.md` or `docs/features/runtime.md` - see
  `scripts/docs/feature-doc-map.json`) in the same session.
- Per `.claude/CLAUDE.md`, a fix ships as an **extracted, named function with a test**
  (`x_not_y` naming), not an inline patch in a `run()` body. A prompt-builder or an
  output-validator is exactly the right shape to extract.
- Stage only explicit paths in ONE bash invocation; never `git add -A`; commit prefix `tiger:`;
  never `--amend`. Expect HEAD to advance - the checkout is shared with `/architect`,
  `/perfect`, `/explorer` and `/ship-loop`.

## Backlog / drain homes

Backlog: `tiger/backlog.md` (created on first `backlog` run). Structural findings that outgrow
Tiger hand off to `.perfect/Architect/backlog.md`; product-shaped ones to the `/perfect` vault
at `.perfect/Perfect/`.
