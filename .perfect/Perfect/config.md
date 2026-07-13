---
type: perfect/config
repo: pumper
created: 2026-07-13
---

# Perfect — pumper config

## Gates
- `cargo check --workspace`
- `cargo test --workspace --lib` (fast, no network)
- Clippy calibration: no NEW warnings in files the diff touched (never full-crate `-D warnings`)
- Doc-sync: consumer-visible change ⇒ mapped `docs/features/*.md` updated (Stop hook enforces)

## Worktree recipe
```bash
git worktree add .claude/worktrees/perfect-<ctx> -b worktree-perfect-<ctx>
# builders: CARGO_TARGET_DIR=C:/Users/mkdol/dolla/pumper/target for every cargo command
# live-server verification: copy config.toml → scratch storage path + different port
```

## Sizing & pacing
- wave_size: 3 concurrent builders
- max 3 directions per builder brief
- direction size: ≲15 files, one builder session, no cross-context schema breaks
- cooldown: 2 proposal rounds per context

## Model policy (builders)

Default **sonnet**; escalate a whole brief to **opus** if any direction in it trips a trigger (full list in the skill, Phase B step 1): sized L · concurrency/correctness-critical (locking, fencing, cancellation, cache coherence, crash recovery) · new public seam other contexts build on · an acceptance criterion that hands over a *design decision* rather than a spec · schema/migration other contexts read, or an algorithmic rewrite needing a correctness proof · a redo after a rejected diff (never re-run the same brief on the same model).

**Retrospective calibration (rounds 1–3, all 35 shipped on Opus).** Classified against the triggers, roughly a third would have needed Opus and two thirds would have been fine on Sonnet:
- **Opus-worthy (trigger fired):** job-control-surface + stuck-job-reaper (attempt fencing, cancellation, crash recovery); structured-fetch-trace + governor-hot-path (new public seam; sharded locking); crawl-memory-bounds (banded-SimHash equivalence proof); crawl-pages-dataset + extract-from-stored-pages (new PageSink/source seams); session-vault (client-pool generalization + the cache-bypass correctness catch); sse-resume-graceful-shutdown (drain/requeue correctness); trades-common-unified (new crate + taxonomy other apps key on); extraction-quality-signal (new public report type).
- **Sonnet-shaped (spec, not design):** trades-meter-research, trades-output-guards, fetch-no-cache-ttl, api-pagination-errors, api-streaming-bounded, openapi-spec, priority-aging, cron-maturity, job-failed-webhooks, crawl-honest-errors, crawl-live-progress, browser-cheap-renders, http-request-controls, proxy-support, markdown-tables-tonumber, ruleset-preview-endpoint, and all four grants directions.
- Watch item: `grants-schema-enrichment` looked Sonnet-shaped but its real value came from the builder *disbelieving the brief* and checking the live API. If a Sonnet builder ever accepts a wrong brief claim uncritically, that's a trigger to add ("brief rests on unverified external-source facts → opus").

(Log every escalation, mid-flight upgrade, or rejected-diff here with the trigger that should have caught it — this is how the list gets sharper.)

## User taste
- 2026-07-13: In the trades context the user rejected the consumer-facing directions (provenance fields, exit-readiness endpoint) and kept substrate/data-correctness ones — weight future slates toward engine/data quality until steered otherwise. Exception: for the API context they took everything EXCEPT auth, including the wildcard (OpenAPI) — infra polish is welcome.
- 2026-07-13 (round 3): rules:"auto" (LLM drafts RuleSet) rejected — third LLM-feature rejection. Pattern: deterministic engine work ≫ LLM-driven features. Stop slating T5 LLM directions unless the user asks.
- 2026-07-13: API-key auth rejected explicitly — parked decision stays parked; don't re-propose unprompted.
- 2026-07-13: User accepted 4 directions when told only 2 slots remained — treat the 10-pool as a soft target, present full slates.

## Skill improvement log
- 2026-07-13 (round 1): Builders sometimes write doc edits to the MAIN checkout despite worktree instructions — the skill's brief template should add an explicit "NEVER touch <main path>" line (added to wave-2 briefs mid-session; F2/A2 complied). Consider a pre-flight `git status` check on main after each builder finishes.
- 2026-07-13: Sequential same-context builders (F1→F2, A1→A2) worked well with a worktree `reset --hard master` between waves — cheaper than fresh worktrees and keeps the branch name stable.
- 2026-07-13: Concurrent builders touching routes.rs produced one real integration task (OpenAPI × /hosts), not just textual conflicts — when a wave-2 builder adds a spec/coverage layer, later routes from OTHER branches must be folded into it at merge; budget Director time for that.
- 2026-07-13: Prefetching the NEXT context's scout during gating hid all scout latency; caching the unused worker/scheduler brief in its context note preserved the spend across rounds.
- 2026-07-13 (round 2): Shared CARGO_TARGET_DIR caused real contention between concurrent builders (stale rlibs, mtime skips) — next round give each builder `CARGO_TARGET_DIR=<main>/target-<ctx>` or stagger launches; disk cost beats flaky builds.
- 2026-07-13 (round 2): Cherry-pick builder commits in CHRONOLOGICAL order (git log is newest-first — reverse it); one out-of-order pick had to be aborted.
- 2026-07-13 (round 2): Scout briefs falsified the stale backlog twice (T6 items already shipped) — always challenge backlog claims against scout file:line evidence before proposing.
- 2026-07-13 (round 2): User accepted 10/10 — two clean sweeps. Slate quality is holding; keep engine-level depth as the default.
- 2026-07-13 (round 3): **Per-builder CARGO_TARGET_DIR (target-<ctx>) fixed the contention completely** — zero stale-rlib incidents across 7 concurrent builders. Make this the standing recipe (update the worktree recipe in the skill: each builder gets target-<ctx>, removed at wrap).
- 2026-07-13 (round 3): Scout briefs can be WRONG on details (G1 found the scout's CA column names didn't exist). Briefs should tell builders to VERIFY scout claims against live sources/code before building on them — worked well as an ad-hoc instruction, make it standing repo law in the brief template.
- 2026-07-13 (round 3): Sequential same-context builders (E1→E2→E3) with worktree `reset --hard master` between waves let later builders BUILD ON merged earlier work (E3 generalized E2's client pool instead of duplicating it). Strongly prefer this over parallel same-context builders.
- 2026-07-13 (round 3): Builders' honest "could not verify" reports are consistently the most valuable part of the report (E3's crash-loss window, G2's federal-money exclusion). Keep demanding them; they became round-4 seeds.
