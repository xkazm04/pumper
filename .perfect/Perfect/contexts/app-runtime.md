---
name: app-runtime
type: perfect/context
group: Core Platform
category: lib
opportunity: 6
last_proposed: 2026-08-10
cooldown_until: round-11
directions: ["[[fetch-chokepoint]]", "[[budget-exhaustion-terminal]]", "[[vcr-attempt-integrity]]"]
---

## Current state (scout 2026-08-10, Director-verified highlights)

`crates/core/src/app.rs` (875 LOC): AppContext facade — 18 pub fields, no constructor;
one production construction site (worker.rs:482–509); TestContext builder in testing.rs.
Matured heavily 07-25→08-03 (provenance, VCR, checkpoints, removal-guard token, MCP
manifest). Untouched since 08-03.

**Verified defects (Director read the code, not just the scout):**
- **Raw-fetch bypass (the big one):** `EngineSet::fetch` is pub; extractor
  (lib.rs:875–909) and plugin (lib.rs:437–468) accept `strategy: "auto_with_research"`
  from job params and fan out via `ctx.engines.fetch.clone()` → raw `fetch(req)`:
  **no metering, no budget clamp (`max_budget_usd: None`), no tier learning, no VCR**.
  The research chokepoint is closed (llm_chokepoint.rs EXPECTED inventory + pub(crate)
  claude); the fetch path has no equivalent. 14 more app crates use raw http/browser
  ($0 tiers — visibility loss only; crawl + transact self-meter).
- **Budget exhaustion retried:** require_budget (app.rs:252–260) returns generic
  Error::App; worker fail path (worker.rs:663–680) does no classification → backoff
  retries that re-seed spend (worker.rs:470) and re-exhaust instantly, burning
  attempts. DataHub cost:pause surfaces as "budget of $0.00 exhausted" (datahub.rs
  effective_budget Some(0.0)).
- **VCR cross-attempt corruption:** cassette opened append (vcr.rs:321–330), loader is
  first-wins (vcr.rs:363–371), artifacts_dir has no attempt component (worker.rs:
  423–427) → a retried record job's attempt-1 partial shadows attempt-2's complete
  recording; 128 MiB cap counter resets per attempt (vcr.rs:280).

**Noted, not slated (evidence bank):**
- xray/M05 recipe discovery: zero callers; api_recipes has no writer; GET /recipes
  reads an empty table; capture_network never set. Slated decision deferred to the
  tiered-fetcher context (see [[tiered-fetcher]] banked anchor recipe-discovery-wiring).
- Progress snapshots leak on 4 of the execute() exit paths (clear only in
  finalize_with_stages, worker.rs:1551) — in-memory only, restart-bounded; S-sized.
- SkippedByRouter trace entries appended out of tier order (app.rs:334–359).
- Per-fetch DB chatter: tiers.preferred SELECT + learn_tier UPSERT + cost INSERT per
  ctx.fetch; double enforced_state per sync_many_with_provenance; double SHA-256 on
  research path; safe_path_segment duplicated inline in save_artifact.
- Poisoned checkpoint blob (serde-unreadable) is treated as absent but never cleared —
  row persists; escapes only via max_resume_failures when the blob parses but the app
  chokes.
- Architect backlog item "(i) raw-engine metering test" — SUBSUMED by
  [[fetch-chokepoint]]'s inventory-test criterion (verified still-open 2026-08-10).

## Direction history
- 2026-08-10 (round 9, director-self-gated): ACCEPTED [[fetch-chokepoint]] (robustness M),
  [[budget-exhaustion-terminal]] (robustness S/M), [[vcr-attempt-integrity]] (robustness S).
  REJECTED fetch-hot-path-batching (optimization) — no volume consumer: crawl bypasses
  ctx.fetch entirely, remaining callers are low-rate; a micro-opt with no nameable user
  moment. REJECTED mid-run-budget-visibility (feature) — cost_events are queryable
  mid-run and the round-4 job receipt answers the money question post-run; consistency
  polish, the taste log's own rejection class. All-robustness accepted slate is
  deliberate: this substrate's correctness debt (money, determinism, attempts)
  outranks new surface; lens spread is a default, not a quota.

## Shipped
- 2026-08-11 · [[fetch-chokepoint]] → `6237cc8` — the last paid-spend/determinism
  bypass closed; 16-site raw-engine inventory with counts is the standing guard
  (subsumes architect backlog item (i)).
- 2026-08-11 · [[budget-exhaustion-terminal]] → `f918006` — budgeted jobs fail once
  with attempts un-burned; governance pauses say so.
- 2026-08-11 · [[vcr-attempt-integrity]] → `4e3647a` — cassette survives retries via
  the checkpoint-coupled Fresh/Resume rule; suspend resumes the recording.
