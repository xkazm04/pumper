# pumper's LLM surface — the pre-resolved map

Seed `Tiger/config.md` from this file on `/tiger init`. **Verify each anchor against the
current code before trusting a line number** (they drift), but do not re-derive the shape:
the shape below is a deliberate architectural fact, not an accident of one scan.

> **The single most important fact:** pumper does **not** use an HTTP LLM SDK. There is no
> `anthropic`, `openai`, `reqwest` call to `api.anthropic.com`, no API key in `config.toml`,
> no `Bearer` header anywhere in the model path. The one and only `Researcher` implementation
> **shells out to the local `claude` CLI**. Do not spend a scan looking for an SDK.
> `grep -rn "api.anthropic\|openai\|OPENAI_API_KEY\|ANTHROPIC_API_KEY" crates/` returning
> nothing is the expected result, not a discovery gap.

---

## 1. The engine (the only provider)

- **Impl:** `crates/engine-claude/src/lib.rs` — `impl Researcher for ClaudeEngine`.
- **Mechanism:** spawns `claude -p --output-format json`, prompt over **stdin**, JSON parsed
  back out. On Windows it routes through `cmd /C` because npm installs `claude` as a
  `.ps1`/`.cmd` shim that `CreateProcess` cannot spawn directly.
- **Flags it can emit** (all from `ResearchRequest` + `[claude]` config):
  `--model`, `--effort`, `--max-budget-usd`, `--bare`, `--dangerously-skip-permissions`,
  `--allowedTools`, `--max-turns`, `--resume <session>`, `--json-schema <schema>`,
  `--append-system-prompt`.
- **Consequences for Tiger:**
  - The **model matrix is native** — `model` and `effort` are per-request params; a benchmark
    cell is a param override, never a code change and never a new credential.
  - The **cost signal is native** — the CLI's JSON output carries cost/turns/session id, and
    they are propagated back into the job result.
  - The **schema signal is native** — `--json-schema` constrains the answer. A call site that
    ships structured data into a dataset without setting `json_schema` is a Lens-1 finding.
  - **`--dangerously-skip-permissions` is deliberate** (`ONBOARDING.md` §2, local-first power
    trade). Never file it as a security finding.

### Config surface (`config.toml`)

```toml
[claude]
binary = "claude"
timeout_secs = 600
# effort = "high"          # global default reasoning effort
# max_budget_usd = 2.0     # global hard spend ceiling per run
bare = false

[claude.roles.research]
model  = "claude-sonnet-5"
effort = "high"

[claude.roles.compose]
model  = "claude-opus-4-8"
effort = "xhigh"
```

Roles are **named presets**, resolved per request in `ClaudeEngine::resolve`. Precedence:
explicit request field → role preset → `[claude]` global default. A Lens-3 recommendation
that changes a default is a one-key edit to `[claude.roles.<name>]` — name the exact key.

---

## 2. The chokepoint

**`AppContext::research()` — `crates/core/src/app.rs` (~line 319).**

This is the wrapper every call site should use. It adds, in order:
1. **Disk research cache** — `ResearchCache::key(&req)`; a hit is served at zero cost and
   metered as a `cache_hit` event (with the dollar amount saved). `resume_session` requests
   deliberately bypass the cache.
2. **Budget governor** — `require_budget()` refuses to start once the job budget is exhausted,
   and clamps the per-call ceiling to the remaining headroom (`clamp_to_headroom`).
3. **Cost metering** — every call (hit or miss) is metered through `self.meter("claude", …)`.
4. **Cache store** on miss, so the next caller is free.

`AppContext` also carries the tiered fetcher (`http → browser → claude`) with the same
metering, so escalation costs are visible too.

**Bypassing the chokepoint loses all four.** That is the default Lens-1 finding shape in
this repo.

---

## 3. Call-site inventory (verify, then diff)

The cheap inventory command — run it on every `scan`:

```bash
grep -rn "\.research(" crates/ --include=*.rs
grep -rn "ResearchRequest::new\|json_schema\|append_system_prompt\|with_role" crates/ --include=*.rs
```

Known sites at the time this reference was written (four, plus the chokepoint itself):

| id | file:line | route to the model | notes for the scan |
|---|---|---|---|
| `research-app` | `crates/apps/research/src/lib.rs:101` | `ctx.research(request)` — **through the chokepoint** | Two prompts pinned to the same shape; uses `with_role(role)` and a session id for the multi-step gather→compose pipeline (`--resume`). The reference implementation of a good call site; use it as the comparison bar for the others. |
| `trades-common` | `crates/apps/trades-common/src/lib.rs:35` | `ctx.research(request)` — **through the chokepoint** | Shared helper behind several `trades-*` app crates, so one prompt serves many datasets: high blast radius, high grounding leverage. Check whether each caller's real context (market, trade, prior record) reaches the prompt. |
| `connector-api-watch` | `crates/apps/connector-api-watch/src/lib.rs:301` | `ctx.engines.claude.research(req)` — **direct, bypasses the chokepoint** | Skips cache + budget governor + metering. Re-verify before filing (it may have been fixed); if still direct and not justified by `resume_session`, this is a ready-made Lens-1 backlog item. |
| `fetcher-tier3` | `crates/core/src/fetcher.rs:485` | `self.claude.research(research)` — **inside the tiered fetcher** | The auto-escalation tier: any app using the tiered fetcher can reach the model implicitly. This is the highest-cost-surprise surface in the repo — a too-eager escalation threshold turns free http fetches into paid model calls. Meter it, and treat the threshold itself as a tunable finding. (It sits *below* `AppContext`, so it has its own metering path — confirm which governance it actually inherits rather than assuming.) |

Modality is `text` for every site. There is no image, vision, embedding, or audio call in
this repo.

---

## 4. Where real outputs live (free L2 evidence)

Never claim "no sample available" before checking these:

- **SQLite job rows** — `data/pumper.db`, `jobs.result` (JSON), plus `error` for failures.
  Also reachable over HTTP: `GET /jobs?app=research&status=succeeded&limit=10`, then
  `GET /jobs/{id}`.
- **Datasets** — `GET /datasets/{app}/{ds}` (and `/duplicates` for near-dup evidence); the
  store's change detection tells you whether the model's output is *stable* across runs, which
  is a quality signal static reading cannot produce.
- **Per-job artifacts** — `data/artifacts/<job-id>/` (raw dumps, HTML, JSON).
- **Research cache** — the chokepoint's on-disk cache holds prior `ResearchOutput`s verbatim.
- **`/events` SSE + `/metrics`** — live job transitions and Prometheus gauges.

## 5. Running a live cell (Lens 2 L2 / Lens 3)

```bash
just run                            # = cargo run -p pumper-server --bin pumper → http://127.0.0.1:8088
                                    # `--bin pumper` is REQUIRED: the package also ships
                                    # `reindex` and `search-backfill` and has no default-run,
                                    # so a bare `cargo run -p pumper-server` errors out.
                                    # `just dev` is the same with RUST_LOG=debug.

# one benchmark cell = one POST with model/effort/budget overrides
curl -s -X POST http://127.0.0.1:8088/apps/research/jobs \
     -H 'content-type: application/json' \
     -d '{"params":{"query":"<character input>","model":"claude-haiku-4-5","effort":"medium","max_budget_usd":0.25}}'

curl -s http://127.0.0.1:8088/jobs/<id>     # poll to succeeded; result carries cost + turns
```

Check the app's `description()` (or `GET /apps`) for the exact param names it forwards into
`ResearchRequest` — apps differ, and passing an unknown key silently does nothing.

**Always set `max_budget_usd` on benchmark jobs.** Cells are paid, and the governor is the
only thing between a matrix run and a real bill. Cache every cell result in `models/*` keyed by
(call-site, model, effort, input-hash) so a re-run is free.

---

## 6. Pumper-specific finding shapes (start the scan here)

1. **Bypassed chokepoint** — `engines.claude.research` / `self.claude.research` instead of
   `ctx.research`. Costs cache + budget + metering.
2. **Unschema'd structured output** — output feeds a dataset but `json_schema` is unset, and
   there is no validate/repair path. Silent shape drift becomes silent dataset corruption.
3. **Over-eager tier-3 escalation** — the fetcher escalates to the model on thin content that
   the `browser` tier would have rendered. Every escalation is a purchase.
4. **The model doing a parser's job** — a call site extracting fixed fields from a stable page.
   `core::extract` (CSS/regex/JSON-pointer rules, compiled once, run across all cores) does it
   deterministically, free, and testably. This is the highest-value finding class in the repo.
5. **Grounding starvation** — the prompt gets a URL and a task but not the prior dataset record,
   the catalog metadata (`catalog/data-sources.toml`: `market`, `category`, `confidence`), or
   the schema of the expected output. Cheap to fix, large quality delta.
6. **Cost telemetry dropped** — the engine reports cost/turns/session id but the app discards
   them instead of returning them in the job result, so `/metrics` under-reports spend.
7. **Role mismatch** — a gather-shaped call running the `compose` (Opus @ xhigh) preset, or a
   synthesis-shaped call running `research`. One `with_role` change.
8. **Missing turn/timeout caps** — no `max_turns`, no `timeout_secs`, on a call that can loop.
