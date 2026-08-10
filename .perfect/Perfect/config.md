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

## User taste (cont.)
- 2026-08-03 (round 4): **two clean sweeps, 10/10 accepted, zero rejections** across dataset-storage
  and job-worker. Both slates were engine-level substrate work (query batching, algorithmic
  complexity, capability tokens, retention, test harnesses) with at most one API surface per slate.
  This is now three rounds of evidence: **the deeper into the engine the slate goes, the higher the
  acceptance rate.** Keep UI/consumer-facing surfacing to at most one direction per slate.
- 2026-08-03: the user approved a 100 GB cache deletion to unblock the build without hesitation, but
  it was still right to ask — it was their disk and the rebuild cost was real. Ask on
  resource-destructive actions even when they are obviously regenerable.

## Skill improvement log
- 2026-08-10 (round 8): **Sequential lots on ONE shared branch worked exactly as the
  revised skill predicted** — zero conflicts, warm incremental rebuilds (post-wave
  `cargo check` in 1.9s), no cherry-picks, ff-merge available at the end. The honest
  "no disjoint partition → sequential" outcome cost wall-clock only, never correctness.
- 2026-08-10 (round 8): **Session death is the norm, not the exception — three
  interruptions in one round** (Lot W builder died pre-commit; the Director session
  itself died twice). Every recovery pattern held: dirty-tree work recovered and
  committed by the Director, same-day same-note continuation, and review-from-diffs
  when the Lot P builder's report was lost. Reading diffs against the DIRECTION NOTE'S
  ACCEPTANCE CRITERIA (not just the diff) found a real gap a green gate never would:
  the promised refusal→retry→apply scenario test didn't exist. Director closed it as
  `06b1deb`. Keep "review against the criteria checklist" as the explicit review step.
- 2026-08-10 (round 8): **First autonomous director-self-gated round.** 6 accepted /
  4 rejected across 2 slates; every decision + reasoning recorded in the context notes
  for Michal's audit. The taste filter transferred cleanly: both rejects-outright were
  "consistency polish without outcome value" calls the log's own precedents dictated.
- 2026-08-10 (round 8): **The wrap is the fragile phase — protect it.** Two of the
  three deaths lost only unwrapped state (a builder report, direction-note updates),
  never code, because commits are per-direction. From this round on the vault is
  git-committed at wrap (`docs(perfect): round-N wrap`, per-file staging) so a
  clobbered note is recoverable — rounds 4–7's vault lived only on disk.
- 2026-08-04 (round 7): **The smoke harness is now part of the loop's muscle memory** — the
  round's final smoke live-verified three of its own new surfaces, and extending the
  checklist is now a standing Director step (f079c48). Caught a checker bug (PowerShell
  unrolls empty JSON arrays) — raw-body assertions for endpoints that return bare arrays.
- 2026-08-04 (round 7): **Six briefs, five load-bearing refutations, zero wasted redos** —
  the verify-every-claim + refute-section discipline is doing the review's front line.
  Notably TWO refutations were of MY acceptance criteria (markdown-first, stable-id
  option). Keep writing options into briefs as OPTIONS with the tradeoff visible, not as
  prescriptions.
- 2026-08-04 (round 7): **fmt version skew is now the biggest mechanical debt** — three
  builders independently hit it; each reverts collateral, so drift keeps growing. Fix
  requires a QUIET moment (no in-flight worktrees): pin rustfmt via rust-toolchain.toml,
  one repo-wide `cargo fmt` commit, re-baseline. Scheduled as round-8 first item — do it
  BEFORE launching any builder.
- 2026-08-04 (round 7): **14 directions in one round is sustainable** with 3+3 builders and
  per-merge gates, but the Director's serial gate-wait is now the wall-clock bottleneck
  (~6 full workspace runs). Consider gating per-BUILDER (3 commits per gate) instead of
  per-direction when picks are from the same worktree — the per-direction atomicity is in
  the commits, not the gates.
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
- 2026-08-03 (round 4): **Re-registration is a real phase and the skill barely mentions it.** The
  context map had been rescanned from 21 → 46 contexts and ~9 moonshot batches had landed since
  round 3; the vault's queue, cursor and cooldowns were all stale. Phase 0 step 2 says "diff
  context-map against contexts/" but assumes small drift. Add explicit guidance: when the map's
  context COUNT or NAMES change wholesale, void cooldowns, re-score from scratch, and record an
  old→new mapping in Perfect.md so the shipped ledger stays meaningful.
- 2026-08-03: **Banked seeds decay.** Of four round-3 seeds carried forward, one was already fixed
  (`index_datasets` full re-index, fixed 2026-07-16) and one was half-obsolete. Challenge banked
  seeds against live code with the same rigour as scout claims — a seed is a hypothesis, not a fact.
- 2026-08-03: **Ask builders for a "brief claims I refuted" section explicitly.** All four did it
  unprompted this round and every one of them was load-bearing: worker.rs had 3 test files not 1;
  `rederive` reads the body from the RECORD not the revision (which reshaped the retention pin);
  `triggers_new` is a rebuild scaffold, not a stale table. This was an ad-hoc line in the brief —
  promote it to a required report section in the template.
- 2026-08-03: **The integration gate paid for itself in one round.** W2 honestly reported running
  only `--lib` + its own package; the full `cargo test --workspace` on master then failed two
  migration tests. Standing rule confirmed: after EVERY merge run the repo's full test command, not
  the builder's subset — and treat a builder's honest scope disclosure as a pointer to where to look.
- 2026-08-03: **Union-merge discipline earned its warning again.** Two of three cherry-pick conflicts
  were safe unions (complete re-export lists); the third (`docs/features/runtime.md`) was NOT — the
  incoming side carried a pre-fan-out version of a bullet the other side had already updated, so
  keep-both would have shipped a duplicated, self-contradicting doc. Read every seam.
- 2026-08-03: **This repo has no live-verification loop and it is now the single biggest gap.** All
  four builders independently reported the same honest limitation: no real server run (ONBOARDING §8
  asks for exactly that). Two migrations, a peer tombstone path and a saved-search fix all shipped
  proven-at-harness-level only. `/perfect smoke` exists in the skill but is written for the Personas
  desktop app (:17320 bridge). **Write a pumper-shaped smoke mode**: boot `just run` on a scratch
  config + port, drive one real job, curl the new endpoints, tear down.
- 2026-08-03: **Per-builder target dirs cost real disk.** Round 3's fix (target-<ctx>) is still right
  for correctness, but two concurrent builders plus a 209 GB main target dir exhausted the disk twice
  in one session. Add to the worktree recipe: check free space BEFORE launching a wave, and remove
  the per-builder target dirs at wrap (done — reclaimed to 64.9 GB).
- 2026-08-04 (round 5): **Requiring a "brief claims I refuted" section works — make it permanent.**
  All four builders produced one and every entry was load-bearing: one told me a direction was
  unimplementable as written (and shipped the right reinterpretation), one found my acceptance
  criterion self-contradictory (keeping the old sweep tests green meant keeping the bug), one
  inverted a fix (annual grant cycles were being DROPPED, not falsely linked, so the candidate set
  had to widen), one caught that a field I promised would appear in the job receipt structurally
  cannot. Three rounds of evidence now: builders catch Director errors reliably WHEN ASKED TO.
- 2026-08-04: **Banked seeds decayed for the third and fourth time.** BOTH grants seeds carried
  since round 3 were already fixed in code (one by round 4's own work). The vault's context note was
  the stale source, which is worse than no note. Standing rule, now proven twice: re-verify every
  banked seed against live code at proposal time and rewrite the context note when it is wrong.
- 2026-08-04: **A prefetched scout paid for itself twice** — it killed two dead seeds AND surfaced
  the round's best find (the unified layer bypassing health entirely), which no seed pointed at.
  Keep prefetching during the gate; it costs nothing in wall clock.
- 2026-08-04: **The Director must re-verify its own measurements before quoting them to builders.**
  I told wave-2 builders the box had ~45 GB free; a builder measured 394 GB and said so. Both were
  true — a concurrent session reclaimed ~360 GB in between. Stale environment facts in a brief are
  the same class of error as stale line numbers.
- 2026-08-04: **Concurrent master is now routine, not exceptional** — a sibling session landed a
  commit mid-wave, turning an ff-merge into a 5-commit cherry-pick (zero conflicts). Always
  `git log <base>..master` before merging, and never assume the ff will still be available.
- 2026-08-04: **Fix recurring flakes instead of re-diagnosing them.** The sink-delivery test flaked
  in three separate sessions and cost review time each round. Its 5s deadline measured machine load,
  not correctness. One commit ended it. If a test flakes in two consecutive rounds, fix it as a
  Director commit rather than noting it again.
- 2026-08-04 (round 6): **The smoke harness closed the loop's oldest gap in one direction** —
  and immediately became the round's own final gate (11/11 PASS on final master with a real
  job). Standing rule from now on: every build phase ends with `just smoke`, and new
  API-surface directions should add their endpoints to the smoke checklist.
- 2026-08-04 (round 6): **Never let a pipeline eat a gate's exit code.** `cargo test | grep
  | head` reported exit 0 while a test had FAILED; only the read-the-output rule caught it.
  Gate commands must capture the test's own exit (`> log; ec=$?`) — codified this round,
  keep it.
- 2026-08-04 (round 6): **Honest negative results are merge-worthy.** T2 measured its own
  assigned optimization (InstancePre) as a wash, said so, and shipped it as a regression
  gate + structural fix instead of a claimed speedup. Review should reward this, not punish
  it — the alternative was a fabricated win.
- 2026-08-04 (round 6): **Run `git -C <path>` instead of `cd` chains in the Director shell**
  — a leftover `cd` into a worktree made a cherry-pick silently run in the wrong repo (it
  reported "empty", the tell). Also: builders still violate FOREGROUND ONLY occasionally
  (D3 idled on a backgrounded test); the nudge protocol works, keep the caps-lock warning.
- 2026-08-04: **The live-verification gap is now the loop's biggest structural weakness.** EIGHT
  builders across rounds 4-5 all reported the same honest limitation: no real server run. The skill's
  `/perfect smoke` mode is written for the Personas desktop app (:17320 bridge) and does not apply
  here. Round 6 should build a pumper-shaped smoke: boot `just run` on a scratch config+port, drive
  one real job, curl the new endpoints, tear down. Four shipped surfaces are waiting on it.
