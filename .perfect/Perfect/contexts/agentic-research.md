---
name: agentic-research
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 6
last_proposed: 2026-08-13
cooldown_until: after-round-23
directions: ["[[research-run-budget-is-real]]", "[[research-empty-is-not-success]]", "[[research-contract-is-what-it-emits]]"]
---

## Current state (scout brief digest, 2026-08-13, round 21 — first sweep on the 46-map)
Scouted "very thorough", read end to end: `crates/apps/research/src/lib.rs` (762 L) plus the whole
pipeline around it (`core/app.rs`, `engine-claude`, `core/error.rs`, `server/worker.rs`,
`server/registry.rs`, `core/cache.rs`, `core/testing.rs`, `core/vcr.rs`, `server/mcp/mod.rs`).
**Opportunity re-scored 4 → 6**: this is the fleet's most expensive per-job app and the only agentic
one whose spend ceiling does not bind.

**What is genuinely good.** The pure helpers (`plan_from_restore`, `fold_output`, `next_step_turns`,
`truncate_chars`, `is_report_shaped`) are cleanly factored and unit-tested; `StopReason` beats
inferring truncation from `structured: false`; `salvage_json` avoids a re-pay on text already bought;
the `json_schema` guardrail is actually wired (many apps declare shapes they never enforce); the
checkpoint is written *before* the artifact, which is the right order. There IS a `run()`-level test
module (`:656-761`) — better than most apps here.

**The intra-step loss window is genuinely closed** — the scout traced every `?` between the metered
call (`:417`) and the checkpoint write (`:444`) and there is none. Process-death-only. That part of
the design is sound and should not be "fixed".

**The headline defect (Director-verified in source, and SHARPER than the brief).**
`max_budget_usd` is a per-CLI-call ceiling sold as a per-run one. The param doc literally reads
`"max_budget_usd": 0.0 (per-run Claude spend ceiling)` (`:218`), the first manifest example is titled
*"Bounded web research run with a hard spend ceiling"* (`:250`), and the value is set on **every**
request (`:410`) across up to `MAX_STEPS = 12`. The between-steps wall (`:379-386`) is
`if let Some(..)` over `ctx.remaining_budget_usd()`, which returns `None` whenever the *job* has no
`budget_usd` (`app.rs:199-200`) — a no-op on exactly the shape the manifest models. Enforced ceiling
= **12×** the advertised one. The one safe entry path is MCP `deep_research`, which sets the clamped
value as both the job ceiling and the param (`mcp/mod.rs:508-513`) — the only caller that does not
trust the app's own knob.

**Contract drift is the worst measured in this repo**: `output_shape` declares 3 keys `run()` never
emits (`summary`/`key_findings`/`sources` — nested inside `report`, and unreachable at any depth when
`structured: false`) and omits 6 it does. Published over `/apps`, `/apps?format=tools` and `/mcp`.
No `tests/` directory at all, while grants-gov, plugin and crawl each ship the exact instrument.

**Correctly out of scope, recorded so no future round re-proposes it:** research has **no catalog
row and should not** — it is on the documented `CATALOG_EXEMPT` list (`routes/mod.rs:392-393, 417`)
and performs zero `ctx.upsert*` calls, so every catalog contract is structurally unfireable. Same for
`DerivedPaths`: not unadopted, **inapplicable** — the app writes no dataset records.

## Direction history
Round 21 (2026-08-13) — gate: director-self-gated (autonomous, Athena-dispatched). 3 accepted, 5
rejected-deferred. Every rejection is banked with its evidence, not dropped.

**ACCEPTED**
- [[research-run-budget-is-real]] — robustness · M. The 12× ceiling + `max_turns` not being a total.
- [[research-empty-is-not-success]] — robustness · M. **Narrowed by Director verification, and the
  narrowing IS the direction**: the scout read the whole "reports success on failure" class as
  broken; it is not. `budget_exhausted` carrying real partial text is *correct* — the user paid for
  those findings — and the test pinning it (`:738-760`) is a RIGHT test that must keep passing. The
  genuine lies are narrower: the **empty** case, and `is_report_shaped` stamping
  `{summary:"",key_findings:[],sources:[]}` as `completed`/`structured:true`. Rider: the `?` on the
  decorative `report.json` write, which at `max_resume_failures` costs a full re-research.
- [[research-contract-is-what-it-emits]] — robustness · M. The drift + the fourth `result_contract.rs`.

**REJECTED-DEFERRED (banked, with why now-is-wrong)**
- **research findings survive in search** — the strongest *user-value* item in the brief and the one
  I most wanted to slate. Every research run's search doc **deletes every prior run's**: the `_job`
  snapshot sweep (`worker.rs:1086-1094`) fires because `search_docs` falls through to the whole-result
  doc (`:1902-1913`), so the index holds exactly one research result at a time, app-wide.
  **Director verification partially REFUTED the framing**: the sweep is a *deliberate, documented*
  ghost-doc GC (`worker.rs:1918-1928` — "the index holds one run's snapshot per app instead of one
  per run forever"), correct for apps whose `_job` doc is a run summary with records indexed
  elsewhere. Research is the one app where that doc **is** the product. So the fix is app-side (write
  the report as a real identity-keyed dataset record) — which reopens the `CATALOG_EXEMPT` decision
  above and needs its own design pass. Deferred for that reason, not for lack of value.
- **the orphaned `claude` process on worker drop** — `worker.rs:737-752` drops the app future rather
  than awaiting it; `kill_process_tree` (`engine-claude:447`) is reachable only from the engine's own
  `abandon_run`. Reachable with shipped defaults (`claude 600s` steps vs `job 900s`), and the orphan
  keeps spending with **no cost event at all**. This is the remaining seam of r13's shipped
  `[[claude-kill-tree]]`. Write set is `worker.rs` + `engine-claude` — both outside this lot. **Bank
  on claude-engine.**
- **cancel destroys the checkpoint** — `cancel_running` → `clear_checkpoint` (`worker.rs:788-794`)
  with no result written, so a user cancel deletes the partial findings, the cumulative spend and the
  resumable `session_id`. Worker-side; **bank on job-worker.**
- **the VCR cassette key omits `resume_session`** — `cache.rs:479-502`; steps 2..N of a chunked run
  share the identical continue-prompt, so they all record under one key and a replay serves step 2's
  answer for every later step. research is graded `ReplayFidelity::Full` (absent from
  `REPLAY_BYPASS_APPS`), so `replay_of` is accepted today. **Bank on vcr-testing** (on cooldown to r22).
- **`checkpoint_now`'s `bool` is discarded at all 11 production call sites** — `progress.rs:141-152`
  returns `false` on stale lineage or storage error and nothing anywhere reads it, while this app's
  own comment (`:442`) claims *"the sink reports, the seam counts"*. Nothing counts. **Two independent
  scouts converged on this** (see [[trades-operator-economics]] L10) — that convergence is the
  evidence. It is a fleet-wide sweep across 11 crates + core, so it needs its own round.

## Shipped
(round 21 in flight — filled at wrap)
