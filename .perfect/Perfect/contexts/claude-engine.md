---
name: claude-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 5
last_proposed: 2026-08-12
cooldown_until: 2026-08-14 (2 rounds)
directions: ["[[claude-kill-tree]]", "[[claude-cost-honesty]]", "[[claude-subprocess-hygiene]]"]
alias_of_old_map: "[[fetch-engines]] (old context named Claude, but no round-3 direction shipped here)"
---

## Current state (scouted 2026-08-12, r13 — full brief in scout report; Director
independently read lib.rs + config + app.rs seams and confirmed the top findings)
288-line engine, zero state, spawns `claude -p --output-format json` per call; prompt
via stdin; Windows DEFAULT path is `cmd /C claude` (binary "claude" is not .exe).
Chokepoint discipline is excellent (EngineSet.claude pub(crate), 2 call sites pinned by
core/tests/llm_chokepoint.rs; 8 apps all through ctx.research). Cost = parsed
total_cost_usd on success only. CONFIRMED-class gaps, Director-verified in source:
- Timeout orphans the real CLI on Windows (kill_on_drop kills cmd.exe only, lib.rs:
  94-100,117-120); default role budgets None → orphan spends unbounded; Error::Claude
  retryable. Writer task leaks on the timeout path (:142 unreachable).
- Cost discarded on EVERY failure path (is_error :160-165, non-zero exit discards
  stdout :145-152, chokepoint meters only Ok — app.rs:436-438). Documented 2026-07-14
  (bughunt fetch-engines.md:30-31), still live. Non-string result → text:"" → never
  cached (cache.rs:528) → schema calls can re-pay forever.
- cmd.exe arg channel: --json-schema raw JSON through cmd re-parsing (6 apps);
  model free-string from POST /jobs reaches --model; typo'd role silent fall-through
  (bughunt engine-capability-traits.md:16-21, still live); subprocess inherits repo
  CWD + full env with bare=false → repo CLAUDE.md/skills/Stop-hook loaded into every
  research call in dev.
- No tests of research()/command()/resolve() at all (6 in-crate tests = parse_loose_json
  only); nothing in the repo ever spawns the real CLI; no doctor check for the binary.
- No concurrency cap (worker.concurrency=4 is the only bound), no prompt/output size
  caps (http/browser/wasm all have theirs), no token telemetry (envelopes carry usage;
  discarded). Tier-3 fetcher research bypasses role/cache/VCR-research (tiered-fetcher
  context's seam). compose role pins claude-opus-4-8 — claude-opus-5 is same-priced and
  current. ONBOARDING.md:345-353 teaches `ctx.engines.claude` which no longer compiles.

## Direction history
- 2026-08-12 (r13, gate: director-self-gated, autonomous): slate of 5 drafted from the
  scout + Director first-hand read. ACCEPTED 3: [[claude-kill-tree]] (robustness M —
  the orphan money leak on the default Windows path), [[claude-cost-honesty]]
  (robustness M — failed spend metered, structured cost in errors, schema-result
  cacheability), [[claude-subprocess-hygiene]] (robustness M — cmd.exe arg guard,
  role/model door, CWD isolation). REJECTED 2, reasoning:
  - claude-concurrency-cap (robustness S): REJECTED — no volume consumer; worker
    concurrency (4) bounds subprocess count today and no app fans research out within
    a job. Same precedent as r9 governor-hot-path. Revisit only if a fan-out consumer
    appears.
  - claude-token-telemetry (feature M): REJECTED-deferred, banked — envelopes carry
    usage/cache-read tokens the repo discards, but /economics has no token consumer
    designed; shipping capture without a reader is inventory, not value. Banked as
    this context's next anchor alongside truncation-honesty (stop_reason/num_turns==
    max_turns surfaced as a partial-answer marker — scout gap #8) and the
    compose-role model refresh (opus-4-8 → opus-5, same price; Director-commit
    candidate with doc note).

## Shipped
- (r13 wave in flight)
