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
- 2026-08-12 (round 14): **A session note that records reviews AS THEY HAPPEN makes a
  death between review and gates nearly free.** The continuation found all 7 commits
  landed, both lots' KEEP verdicts written with file-level specifics, and an executable
  "if resuming" block — total recovery cost was one spot-verify pass (grep the recorded
  claims against the code) plus the gates. Keep writing reviews into the session note
  incrementally, not at wrap.
- 2026-08-12 (round 14): **A smoke check written from memory of the data model used the
  storage column name (`plugin_hooks`) where the API takes `plugins` — and the door's
  unknown-field tolerance turned that typo into a false FAIL that looked like a product
  bug.** Two lessons: (a) when writing a NEW check against an API, copy the field names
  from the route's request struct or the feature doc, never from the model/migration;
  (b) the door silently 201-ing a typo'd gate config is itself a finding — banked on
  trigger-pipeline as a candidate direction (unknown-body-field policy at the doors).
- 2026-08-12 (round 14): **The uncommitted-vault-death window is real: ~176 lines of
  review-state sat dirty in the working tree when the session died.** The continuation's
  first act was committing them before touching anything else. Consider committing vault
  review-state per-lot (not per-session) in future rounds — the vault is in-repo here, so
  a commit is cheap insurance.
- 2026-08-12 (round 13): **Builder death is the norm, and the recovery protocol has
  converged to near-zero cost.** Three of the last five rounds died mid-build; r13
  died TWICE (original + first continuation) and still shipped 6/6 with zero work
  lost. What made it cheap, in order of load-bearing-ness: (1) builders commit per
  direction the moment it verifies, (2) the session note's `next:` pointer names the
  exact remaining work, (3) the active-runs entry names the branch and the in-flight
  lots. A continuation session needs only those three to resume losslessly. Keep all
  three as non-negotiable steps.
- 2026-08-12 (round 13): **A dead sibling session's dirty file in your own wave's
  checkout is wave work, not foreign work.** The r13 smoke extension sat uncommitted
  in the tree across two session deaths; the parallel-safety rule ("leave dirty files
  you did not create alone") correctly protects OTHER sessions' work, but a file that
  is unambiguously this wave's planned output (named in the dead session's `next:`)
  should be reviewed like a builder diff and landed, not orphaned.
- 2026-08-12 (round 13): **Don't pipe a gate through `tail` — you keep the exit code
  and lose the evidence.** `just ci | tail -60` proved green but destroyed the test
  totals, costing a full re-run to recover the ledger number. Capture gate output to
  a file (`> log 2>&1`), then read the file; the round-6 exit-code rule and this are
  the two halves of the same discipline.
- 2026-08-12 (round 13): **An acceptance criterion can be wrong in the particulars and
  right in the principle — judge deviations against the principle.** The criteria
  said refuse `"` at the cmd.exe shim; the builder MEASURED cmd's actual re-parse and
  proved refusing quotes would break every schema-using app while a lone quote
  mangles nothing. The criterion's own words ("per actual cmd re-parsing rules")
  licensed the deviation. Review verdict: the measurement wins; record the deviation
  in the build record rather than forcing criteria-compliance.
- 2026-08-12 (round 12): **The report-don't-touch seam for out-of-set changes worked
  end-to-end for the first time at full depth**: Lot X predicted the worker.rs
  double-index its own change would create, specified the exact guard + the fallback
  that must stay outside it + the stale-doc rollout consequence, and the Director
  applied it as a reviewed commit at quiescence. The write-set boundary produced a
  BETTER analysis than shared ownership would have — the builder had to reason about
  the seam instead of just editing it. Keep worker.rs-style shared chokepoints
  Director-only when two lots orbit them.
- 2026-08-12 (round 12): **The doors-audit acceptance criterion caught a second
  instance of the bug being fixed** (triggers door, stored + replayed per hop — worse
  than the original). An audit criterion ("inventory EVERY surface that accepts X")
  turns one fix into a class kill; pair it with an extinction scan so the convention
  is enforced, not remembered. Also: my extinction scan failed twice on first runs
  (doc-comment quote, then its own needle literal) — a source-scanning guard needs a
  comment-stripped view AND a self-match check, now proven twice (r11 chokepoint, r12
  budget).
- 2026-08-12 (round 12): **First deathless round since r7** — both opus lots ran ~65min
  concurrently to final report. The concurrent two-lot partition is now 4-for-4 with
  zero collisions. Committing the propose-state BEFORE launching builders (b121216)
  cost nothing and would have made a death near-free; keep it as a standing step.
- 2026-08-12 (round 11): **Second mid-build death, and the recovery bill was ~30 minutes
  with zero work lost** — but only because both dead builders had COMMITTED nothing while
  having FINISHED their first direction in the working tree. The wip-snapshot →
  review-from-diff → clean re-commit path (soft reset on an unpushed branch) produced
  atomic history indistinguishable from a live builder's. Standing rule confirmed: the
  recovery order is snapshot FIRST (per-lot `--only` commits), analyze second.
- 2026-08-12 (round 11): **A builder self-caught shipping a red gate and named the cause:
  piping the gate through `head` truncated the failure line** — the exact anti-pattern the
  round-6 learning codified for the Director, now proven to need saying in BUILDER briefs
  too. Add to the brief template: capture the gate's own exit code; never pipe a gate
  through head/grep without checking `$?`.
- 2026-08-12 (round 11): **The Director's own smoke checks needed two fixes the builders'
  code didn't** (assumed the trap-400 fires on a fresh store; assumed a bare-array
  response shape). Smoke checks are code too: verify response shapes and state
  dependencies against the source before asserting, exactly like a builder verifying a
  brief claim.
- 2026-08-12 (round 11): **Write-set exceptions want a declared-and-judged path, not
  silence.** A2 needed +2 test-fixture lines in two out-of-set files after widening a
  core struct — a mechanical ripple no write-set can predict. It declared the exception
  with reasoning instead of contorting around the boundary, and the Director accepted at
  review. That is the right protocol; codify "declare the exception in your report" in
  the template rather than pretending ripples don't happen.
- 2026-08-11 (round 10): **The vault's in-flight session note made a mid-round death
  nearly free.** The proposing session died after gating but before any builder
  commit; the continuation session reconstructed the entire state from Perfect.md's
  wave plan + the 6 direction notes + `git log master..perfect/*` in under five
  minutes, re-verified four evidence anchors, and ran the recorded plan unchanged.
  The write-after-every-phase rule is doing exactly what it was designed to do —
  keep the "if resuming:" instructions in the session note's `next:` while a wave
  is in flight.
- 2026-08-11 (round 10): **A builder refutation flipped a criterion's THEORY, not
  just its letter**: the brief framed app-side unknown-field checking as defense in
  depth behind the schema; the builder proved trigger-fired enqueues never touch the
  validator, making the app-side check the ONLY guard on that path. Acceptance
  criteria that say "verify X first" keep paying — the verification WAS the design
  input. Also the second straight round where e2e-location knowledge mattered
  (crates/server has src/e2e/, not tests/) — put that in the next api brief up front.
- 2026-08-11 (round 10): **Windows can't deliver Ctrl-C to a detached process**, so
  the smoke harness cannot drive the graceful-shutdown path it most wants to prove;
  it stays e2e-proven only, and the smoke script now says so where a future round
  would look. If live proof is ever wanted: a conhost-attached child + 
  GenerateConsoleCtrlEvent shim is the only route — weigh it as its own S direction.
- 2026-08-11 (round 10): Second concurrent two-lot wave, zero collisions again; the
  partition rule is now 2-for-2 and cost nothing. Cross-lot Class-B rides didn't
  occur this round (e2e/mod.rs edits were both single-line appends, seconds apart).
- 2026-08-11 (round 9): **First CONCURRENT two-lot wave on the one-branch shape — the
  partition rule proved itself both ways.** Round 8 honestly went sequential (worker.rs
  overlap); round 9's write sets were genuinely disjoint and two opus lots ran
  concurrently with zero collisions, both finishing within ~1h of each other. The one
  cross-lot artifact was benign and structural: `git commit --only <Class-B file>`
  commits the WORKING-TREE content, so a sibling's just-added `mod` line rode along in
  the other lot's commit (a69420a doesn't build in isolation; the tip does). If
  commit-level bisectability ever matters, the fix is "stage Class-B hunks via a
  temp-index", not more isolation.
- 2026-08-11 (round 9): **Builder refutations of Director acceptance criteria are now
  the loop's highest-value review input** — two of six criteria were wrong as written
  (governance-pause message can't live in core; the save_penalties either/or was a
  false pair because of a partial-list caller the evidence missed) and both builders
  shipped the RIGHT thing with the reasoning recorded. Keep writing criteria as
  intent + options with tradeoffs, never prescriptions.
- 2026-08-11 (round 9): **A no-human round rejects honestly when the slate is
  correctness-shaped**: 6 accepts / 5 rejects across 2 contexts, all-robustness pool.
  The taste log's own precedents decided every reject (no volume consumer, existing
  surface answers it, unvalidatable heuristic churn). One reject was deferred-banked
  as the context's next anchor (recipe-discovery-wiring) — the bank is becoming the
  real feature pipeline; re-verify banked seeds at proposal time (decay rule).
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
- 2026-08-13 (round 17): **The Class C mechanism earned its keep for the first time as a
  collision-avoider rather than ceremony.** Both directions needed a row in
  `feature-doc-map.json` — the single true overlap in an otherwise clean partition. Making that
  one file Director-only cost nothing and removed the only reason the two lots could have
  fought. Rule to keep: when exactly one file straddles a partition, Class C it rather than
  re-planning the wave or accepting a Class B race.
- 2026-08-13 (round 17): **Applying Class C is not bookkeeping — review it like a diff.** The
  `ONBOARDING.md` map row I was about to apply would have been partially inert, because the
  hook hardcoded `docs/features/` as the only satisfying prefix; the reminder would have named a
  file that could not answer it. Lot E caught it *while reporting the row it needed*, which is
  an argument for asking builders to report Class C items with caveats rather than as bare data.
  I fixed the predicate and re-calibrated rather than shipping the row or dropping it.
- 2026-08-13 (round 17): **The coverage figure had drifted every round for four rounds because
  the stated rule was not computable.** Two independent causes: a stale note (`job-worker`) and —
  the nasty one — a naive `/^---\n([\s\S]*?)\n---/` frontmatter regex that silently skips every
  CRLF file in this repo, under-reporting coverage by ten contexts. My first computation said
  18/46 and I only caught it by disbelieving an output that listed `claude-engine` and
  `eu-grants` as never-proposed. **Any vault metric computed by regex must normalize CRLF first,
  and any number that disagrees with the shipped ledger is the metric's bug, not the ledger's.**
- 2026-08-13 (round 17): **"Measure it, don't read it" turned a filed-as-lesser seed into the
  round's best direction.** r11 scouted the same `check-doc-sync` scan window and banked it as
  anchor #4, "dev-loop only". r17's scout *replayed the algorithm over real transcripts* and
  found it had never fired in 1,136 edits. Same code, same reader-quality, opposite verdict —
  the difference was execution vs. inspection. Add to scout prompts: **for any claim of the form
  "X never happens" or "X is not reached", run it rather than reasoning about it.**
- 2026-08-13 (round 17): **Builder death is cheap now; builder *silence* is the residual cost.**
  The r16 D5 builder died leaving a compile error one line wide, and its wip message said
  "investigating a failing test" — which cost me a full read of a 540-line diff to discover the
  work was complete. A wip snapshot that names the *symptom it last saw* (`rustc says: second
  test attribute is supplied`) would have cut that to seconds. Add to the brief template: if you
  are about to run out of room, commit a wip whose message pastes the last error verbatim.
