# Web Research & Readable Content — perf-optimizer + feature-scout scan

> Total: 3
> Critical: 0 | High: 1 | Medium: 2 | Low: 0

## 1. Expose `resume_session` so research can drill down instead of re-running from scratch

- **Severity**: High
- **Lens**: feature-scout
- **Category**: feature-gap
- **File**: crates/apps/research/src/lib.rs:23-54, 77-88
- **Scenario**: A caller runs `research {"query": "US pool-service margins"}`, gets a report, and wants one follow-up ("break the margin range down by state"). Today the only option is a second full `run` with a longer query — a fresh agent, fresh web searches, fresh cost. The app already returns `session_id` in its result JSON (line 84), so a caller can *see* the session handle but has no param to feed it back. It is dead output.
- **Root cause**: `ResearchRequest` (crates/core/src/engine.rs:256-258) carries `resume_session: Option<String>` — documented as "Resume a prior CLI session id for multi-step research pipelines" — and `AppContext::research` (crates/core/src/app.rs:235-236) explicitly handles it (resumes bypass the cache). The research app builds its `ResearchRequest` from `query`/`role`/`model`/`effort`/`max_turns` only and never reads a `session_id` param, so the one seam in core built for multi-step research has no app exposing it. This is the front door to the Claude engine; the four trades apps go through `trades_common::research_json`, so `research` is where interactive/API callers land.
- **Impact**: Every follow-up question costs a full agentic run (the whole search+fetch+synthesize loop, the dominant cost in this context) where a resume would reuse the accumulated context. Also blocks the natural next feature — a multi-step research pipeline (broad sweep → targeted drill-downs) — which core is already built for. Also unexposed: `max_budget_usd` (per-call ceiling), though job-level `budget_usd` already clamps headroom, so that is secondary.
- **Fix sketch**: Read `ctx.params.get("session_id").and_then(Value::as_str)` into `request.resume_session`. When set, use a follow-up prompt (the drill-down question + the same JSON shape contract) rather than the full "You are a web research agent…" preamble at line 42-49 — the agent already has the topic in session. Keep `json_schema` and `is_report_shaped` unchanged so resumed turns are held to the same shape. Document the param in `description()` and add a row to `docs/features/apps.md`.

## 2. Salvage a fenced/prose-wrapped report instead of degrading to `structured: false`

- **Severity**: Medium
- **Lens**: perf-optimizer
- **Category**: research-cost
- **File**: crates/apps/research/src/lib.rs:66-76
- **Scenario**: The agent returns the right report but wrapped in a ```json fence or with a leading sentence. `output.json` is `None`, so `structured` is false and `report` becomes `Value::String(output.text)` — a text blob where the caller expected `{summary, key_findings, sources}`. The caller's only recourse is to re-run the whole research job.
- **Root cause**: The exact fix already exists and is shipped next door: `trades_common::salvage_json` (crates/apps/trades-common/src/lib.rs:60-71) recovers a fenced or prose-wrapped object from raw text in one pass — no re-run, no cost, on text already paid for — and `trades_common::research_json` wires it into the metered path for all four trades apps. The research app predates that helper and never adopted it, so it drops answers the trades apps would recover. `json_schema` (line 57) reduces the frequency but does not eliminate it; that is precisely why the trades apps carry both.
- **Impact**: Each unsalvaged answer costs one wasted agentic run (the most expensive operation in this context) plus a duplicate one to retry — and the retry is cache-missed only if params changed, so a caller re-running identically just gets the same unparsed text from cache. Bounded by how often the model fences its output, hence Medium not High.
- **Fix sketch**: Add `trades-common` as a dep of the `research` crate and replace lines 71-76 with: try `output.json`, else `salvage_json(&output.text)`; run `is_report_shaped` over whichever object was obtained, so `structured` still means "matched the promised shape". Keep the text fallback for the genuinely-unparseable case. Consider whether the `research_json` seam (which also archives `research.json`) fits — the research app already saves its own `report.json` at line 86, so `salvage_json` alone is the lighter fit. Naming note: `trades-common` housing a generic helper is a smell; if this lands, the salvage/shape utilities arguably belong in `pumper_core` beside `ResearchOutput`.

## 3. Stop embedding full page Markdown in the readable job result

- **Severity**: Medium
- **Lens**: perf-optimizer
- **Category**: caching / payload-size
- **File**: crates/apps/readable/src/lib.rs:46-70
- **Scenario**: `readable` fetches a long article. The Markdown is written to the `page.md` artifact (line 61) and *also* inlined into the returned result JSON (line 69), which the worker persists into `jobs.result` (TEXT, crates/core/migrations/0001_init.sql:8). Every run stores the document twice — once on disk, once in SQLite — and every subsequent `GET /jobs` listing that hydrates results pays for it.
- **Root cause**: `docs/features/apps.md` states the convention explicitly: "big payloads to artifacts; compact result JSON with new/changed counts". `readable` is the app that documents the artifact pipeline and violates the rule it demonstrates — it returns the payload inline for caller convenience because there is no other same-response way to hand back the content. The clone chain at lines 48-52 (`outcome.markdown.clone().or_else(|| outcome.text.clone())`) adds a second document-sized copy in memory, minor beside the storage duplication.
- **Impact**: `jobs.result` grows unboundedly with page size — a 200KB article makes a 200KB row, and job rows are retained. Doubles storage per run and inflates any job-listing query that selects `result`. Bounded (readable is an example app, single URL per run), hence Medium; but it is the template other apps copy, so the anti-pattern propagates — and `watch`, which fingerprints rather than inlines, shows the intended shape.
- **Fix sketch**: Drop `"markdown"` from the result JSON at line 69, keeping `markdown_chars` plus the existing `url`/`engine`/`status`/`escalations` — the compact shape the convention asks for. The content stays available via the `page.md` artifact. If inline return is genuinely wanted for interactive callers, gate it behind an explicit `"inline": true` param (default off) so the scheduled path never pays. Also avoid the double clone: `outcome.markdown.take().or_else(|| outcome.text.take())` on a `mut outcome` moves the string instead of copying it.
