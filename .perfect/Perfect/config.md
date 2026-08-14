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

- 2026-08-14 (round 22): **When a gate goes red, ask "is the failing file in the diff?" BEFORE
  re-running anything.** Two suites failed across two full-workspace runs and I burned two ~12-minute
  re-runs half-diagnosing them. One `git diff --name-only master..HEAD` answered it completely:
  neither `engine-search` nor `engine-claude` appeared anywhere in the wave's 32 files, so neither
  could be a regression **by construction** — stronger evidence than any re-run, and available in one
  second. The re-run is then only for confirming flakiness, not for establishing ownership.
  **Companion trap, and the more dangerous half: `cargo test --workspace` FAIL-FASTS on the first
  failing test binary.** The runs reported "1433 passed" and "1329 passed" against a 1919 baseline —
  which reads exactly like a catastrophic regression and is actually a *partial count of an aborted
  run*. `--no-fail-fast` is mandatory the moment anything goes red, or every number you have is a
  lie. Same family as r21's tail-truncated log and r17's CRLF trap: **the measurement apparatus
  fails more often than the thing being measured, and its failures are shaped like disasters.**
- 2026-08-14 (round 22): **Two runs, two DIFFERENT failures, both outside the diff = environment
  flakiness, not regression.** A single flaky failure is ambiguous; the *pattern across runs* is not.
  A real regression is deterministic and lands in the same place. Worth writing down because the
  instinct on seeing a second red run is "it's getting worse", when a second red run in a *different*
  untouched crate is actually the strongest available evidence that neither is yours.
- 2026-08-14 (round 22): **The r21 family-scout trick scales further than r21 claimed — two scouts
  covered SEVEN contexts and closed the campaign.** r21 said "prefer this wherever a `*-common` crate
  spans contexts". The generalisation is broader: the spine does not have to be a shared *library*,
  it can be a shared *registry*. `crates/core/src/catalog.rs` is the GitOps reconciler every thin app
  declares into, and scouting six apps against that one seam produced a finding no per-context scout
  could reach — that only 2 of 6 have a catalog row, the other four are exempt *by design*, and the
  real story is what the exemption COSTS (no contract tripwire, no freshness row) rather than the
  absence itself. **Look for the seam every member of a group touches, not just the code they share.**
- 2026-08-14 (round 22): **The twice-deferred enabler+fix pair finally shipped, and the deferral
  reason died the moment the enabler was scoped ADDITIVE-ONLY.** `harness-expresses-the-run` was
  rejected twice partly because `testing.rs` "ripples into 33-39 files". Written as *one new setter +
  one new double, no signature changes, and not one existing caller may need an edit*, the ripple was
  literally zero — the commit touches 2 files. **When a direction is deferred for write-set blast
  radius, the next brief's job is to make additivity an acceptance criterion, not to re-argue the
  value.** And the payoff was immediate and verifiable: the fix it unblocked shipped in the same wave
  with a regression test that was structurally impossible the day before.
- 2026-08-14 (round 22): **A rejected direction deserves a real note, not a verdict line — and this
  round that is what made 46/46 honest rather than a metric game.** Four contexts were covered by
  rejection. Each got a full direction note with `file:line` evidence, acceptance criteria for
  whoever builds it, and an explicit *why rejected* naming what it lost the slot to. Two of them
  ([[shipped-plugins-are-verified]] + [[trigger-gate-honest-across-source-kinds]]) turned out to be
  the *same enabler-blocks-fix pair shape* r22 had just paid off — which is only visible because the
  notes were written properly. **A bank note that a future round can build from directly is the
  difference between coverage and bookkeeping.**
- 2026-08-14 (round 22): **Also record what is SOUND, not only what is broken.** The catalog
  reconciler scout came back with "the `managed_by` fence is airtight in both directions, it cannot
  delete or overwrite a hand-made row, a missing catalog file is safe by design". I wrote all of that
  into the context note under an explicit heading, because "reconciler" *sounds* like a risk surface
  and a future round would otherwise re-scout it from scratch. **Confirmed-sound findings are cheap
  to record and expensive to re-derive.**

- 2026-08-13 (round 21): **A scout finding that survives verification but CHANGES SHAPE is worth more
  than one that is simply confirmed — and this round both of the shape-changes were load-bearing.**
  (a) The scout read research's whole "reports success on failure" class as broken. Verification showed
  a `budget_exhausted` run carrying real partial text is *correct* — the user paid for those findings —
  so the defect is only the **empty** case. That narrowing went into the criteria as "this existing test
  must keep passing, unchanged; if your change breaks it your definition of failure is too wide", and it
  is the single line that kept the builder from shipping a regression. (b) The search-eviction finding
  was partially refuted (a deliberate documented GC, not a bug), which kept a bad direction OUT of the
  slate. **Standing rule: when a scout generalizes a defect into a class, verify the class boundary, not
  just the exemplar — and write the boundary into the criteria as a test that must survive.**
- 2026-08-13 (round 21): **Scout a CRATE FAMILY as one brief when it spans map contexts — it covered
  three contexts in a two-lot wave.** `trades-operator-economics` and `trades-pricing` share
  `trades-common`; two per-context scouts would each have seen half of every defect (the numeric-string
  hole is *stored* in four apps and *dropped* in the shared join; the reference fix lives in a third).
  Cost: one context's directions get filed under a sibling note. Benefit: +2 coverage for one scout, and
  findings no per-context scout could have produced. **Prefer this wherever a `*-common` crate spans
  contexts** — the grants and census families are the obvious next candidates.
- 2026-08-13 (round 21): **The harness stream-watchdog kills builders that fan out many large Reads at
  once — three deaths this round, all at the 600s no-progress mark.** Two died before writing anything.
  The fix that worked was one paragraph in the brief: *at most 2 files per turn, offset/limit over ~600
  lines, do not issue six large Reads in one message*, plus a suggested reading ORDER matched to the
  direction sequence. **Put that paragraph in every brief whose write set exceeds ~2,000 lines**, and
  size the reading plan in the brief rather than leaving the builder to discover the ceiling.
- 2026-08-13 (round 21): **A builder-death snapshot must be scoped to the dead builder's OWN dirty files
  when a sibling is still live.** `git status` showed three modified files; two were Lot T's and one was
  the still-running Lot R's `research/src/lib.rs` mid-edit. `git commit --only <the two>` was correct and
  a wider snapshot would have committed a sibling's half-written source under a `wip(trades)` message.
  Also: **inspect the snapshot before re-briefing.** Reading it showed the shared-crate half was complete
  and good and only the wiring was broken (3 compile errors), which turned the continuation brief from
  "redo D4" into "finish these three errors, do not relitigate the lever choice" — and the continuation
  honoured it exactly.
- 2026-08-13 (round 21): **A parse that returns 0 is not a measurement of 0.** The entering test baseline
  summed to `passed=0 failed=0` because the background log had been tail-truncated to its last 5 lines,
  not because no tests ran. I caught it only because 0 was absurd. **Any gate figure that arrives as a
  suspiciously round or empty number is a parse bug until proven otherwise** — re-run capturing to a file
  you control. Same family as r17's CRLF trap and r19's dropped-clause: the measurement apparatus fails
  more often than the thing being measured.
- 2026-08-13 (round 21): **Making the one true straddler Class C beat sharing it, again.** Every
  `crates/apps/**` edit drags `docs/features/apps.md` through the doc-sync map, so two app-lot waves can
  never be zero-Class-B by pairing alone. Handing that ONE file to the Director (builders report their
  doc text) preserved zero Class B at a cost of ~15 minutes of Director doc-writing, and the builders'
  proposed text was good enough to use nearly verbatim. **For app-vs-app pairings, budget the shared
  feature doc as Director work from the start rather than treating it as a pairing failure.**
- 2026-08-13 (round 19): **The coverage rule has THREE clauses and dropping any one silently
  under-reports — this is the r17 CRLF trap in a new costume, and it caught me at the wrap.** My
  final spot-check recomputed 33/46 against a headline of 34, which for a moment looked like the
  headline was inflated. It was not: the spot-check had omitted the *verdict* clause
  (`nothing-clears-the-bar`), and `archive-engine` — covered by r11's explicit
  nothing-clears verdict rather than by a direction or a `last_proposed` date — is the single
  context that clause carries. Current split: **33 by direction, 1 by verdict, 0 by
  `last_proposed`-only.** Two rules follow. (a) **Any coverage number computed with fewer than all
  three clauses is wrong** — paste the rule, don't retype it from memory. (b) **A disagreement
  between a fresh computation and the ledger is the computation's bug until proven otherwise** —
  that was r17's finding and it held again, in the opposite direction this time (r17's bug
  under-reported by ten; this one by one). The `last_proposed`-only clause currently carries
  **zero** contexts, so it is untested in practice — do not delete it, but know that a bug in it
  would be invisible today.
- 2026-08-13 (round 19): **A wave with ZERO Class B files is achievable and it deletes a whole
  failure class.** r18's log recommended making every straddler Class C rather than sharing it;
  taken literally this round, the two lots shared no file at all — the four `crates/core/`
  straddlers went wholesale to one lot, the docs split by name, and neither lot was allowed to add
  a new e2e file (so `e2e/mod.rs` needed no append). The `git commit --only` bleed that r9 and r18
  both recorded **did not happen, because there was nothing to bleed through**. The cost is one
  constraint at wave-plan time: pick two contexts living in different crates. **Standing rule: aim
  for zero Class B, and treat "this wave needs a Class B file" as a signal to re-check the
  pairing** — not as a protocol to execute.
- 2026-08-13 (round 19): **Write thin findings into acceptance criteria as named riders instead of
  banking them.** Four findings too small to slate (`max_body_bytes = 0`, the retry-loop test gap,
  the concurrency upper clamp, a missing schema `maximum`) were named as riders inside the three
  directions whose files they touched. All four shipped, at near-zero marginal cost, because the
  builder was already in that code. Banking them would have cost a future round a re-verification
  pass each — and the decay rule (proven four times now) says at least one would have been stale.
  The bank is for things that need their own design; riders are for things that need a line.
- 2026-08-13 (round 19): **Three of four builder refutations were of my acceptance criteria's
  *stated mechanism*, not its intent — and each would have shipped a silent no-op.** `/run_at`
  instead of `run_at` for `DerivedPaths` (the seam would have *looked* adopted and done nothing —
  precisely the class the direction existed to kill); `drift_score` "argue it either way" when the
  behavioural test provably cannot pass with it in the identity; and "include `dataset` so
  `extract_yields` attributes correctly" when that field is a JSON *path*, so the fix changes
  nothing and the obvious re-keying would double-count. **Lesson: when a criterion names a
  mechanism (a path syntax, a field, an API), the criterion should say "verify this spelling
  against the implementation first" — I keep specifying mechanisms from a scout's prose rather
  than from the function signature.**
- 2026-08-13 (round 19): **A builder's honest "I did half of a known pair" is the highest-value
  line in a report, and it deserves a same-round decision rather than a bank entry.** Lot P bounded
  the `records` echo and reported, unprompted, that doing so dropped a 10 000-output run from
  10 000 search docs to 100 — then argued for the fix the direction had made a non-goal. It was
  right, and the decisive evidence was a precedent it did not have (the extractor pairs
  `records_echo` with `index_datasets` for exactly this reason, r12). Re-briefing cost ~10 minutes
  and the follow-up caught a bug neither of us had seen: the worker's health gate reads the
  *spec's* pair, and `plugin_out@q` is a pair nothing judges, so it always reads Healthy and would
  have indexed quarantined rows. **Standing rule: "non-goal" in a direction note means "do not
  assume", not "do not raise" — say so explicitly in the note, as this round's did.**
- 2026-08-13 (round 18): **Writing a known hazard into the acceptance criteria as a CHECK, not a
  prescription, produced the round's best outcome.** Criterion said "widening `is_terminal_for_job`
  to all of `Error::Profile` may capture a transient error — check every construction site before
  widening; if any is transient, take the other lever." The builder checked, found one
  (`ProfileJar::load` on a sharing violation), took the other lever, and left the reasoning in the
  doc comment so a future round cannot undo it. Compare the failure mode of prescribing the fix: I
  would have shipped a silent retry regression. **Standing rule: when a lever has a plausible
  hazard, name the hazard and the alternative lever in the criteria and let the builder decide.**
- 2026-08-13 (round 18): **A "known gap" pinned in an EXPECTED map is worth more than a gap
  mentioned in a report, because it names the remaining work precisely enough to finish.** Lot B
  could not reach the third profile-name seam (outside its write set) and recorded
  `("engine-http::fetch", false)` with a comment naming the exact edits. The Director closed it the
  same round in ~20 minutes. A gap in a report would have become a banked seed that decayed. Prefer
  "pin the gap as a failing-by-design row" over "mention it" whenever the guard shape allows.
- 2026-08-13 (round 18): **A builder refusing to game a guard is a signal to act on, not just
  praise.** Lot R declined to wire `/metrics` because the read tripped the raw-engine inventory, and
  explicitly declined both workarounds (rephrasing the expression to dodge the scanner; making the
  counters a process-global). That refusal is what let the Director apply a *reviewed* inventory row
  instead of discovering a laundered one later. Add to the brief template: "if a guard blocks you,
  report it — never rephrase around it; the row is the Director's to carry."
- 2026-08-13 (round 18): **The Class B shared-file bleed is now confirmed structural, twice
  independently, and should be in the brief rather than rediscovered.** `git commit --only` commits
  working-tree content, so on a genuinely shared file each lot's commit carries the sibling's
  in-flight hunks. Round 9 found it; both r18 lots reported it unprompted. It is harmless to
  correctness and fatal to per-commit attribution. Either say so in the brief up front, or stop
  putting *any* file in Class B and make every straddler Class C — r17's lesson already points that
  way and r18's one true straddler (`feature-doc-map.json`) cost nothing as Class C.
- 2026-08-13 (round 18): **Phase 0 reconciliation was a genuine no-op for the first time, and
  saying so with the numbers is the deliverable.** 0 notes to create, 0 to alias, 0 to retire, and
  a recomputed 30/46 matching the inherited headline after four rounds of drift. The dispatch
  demanded "expect real reconciliation work, not a no-op" — the honest answer was that r11/r16/r17
  had already done it. Report the diff table even when every cell is zero; a verified no-op and an
  unexamined one look identical otherwise.
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
- 2026-08-13 (round 20, continuation): **A `wip(...)` builder-death snapshot that actually holds a
  FINISHED direction is a ledger bug with a short shelf life.** Two of this wave's six directions
  (D2 `cassette-verified-or-refused`, D5's app half) shipped in their entirety under
  `wip(…): builder-death snapshot — Dn in flight`. The content was complete and reviewed; the
  messages undersell it, and the fix is only cheap until a sibling commit stacks on top — after
  that, rewording means rewriting history on a branch that `git rebase -i` cannot touch (it hangs
  this harness). **Rule for the Director: when recovering a dead builder, first check whether the
  snapshot is already complete, and if it is, `git commit --amend` it under its real message before
  briefing anything else.** Round 17 logged the mirror of this (a wip message naming the symptom
  saves a full diff read); this is the case where the wip message names *nothing*.
- 2026-08-13 (round 20, continuation): **Attribute snapshot commits by the symbols they introduce,
  never by their message.** Mapping the ten wave commits to six directions took one `git show |
  grep '^+(pub const|pub enum|fn )'` per commit and was unambiguous; reading the messages would have
  produced the wrong ledger for two of them and left D2 looking unshipped. Cheap, mechanical,
  and it is the only way the Shipped ledger stays true across a session death.
- 2026-08-13 (round 20, continuation): **The coverage script is now a committed file
  (`.perfect/coverage.mjs`), not a snippet re-typed into each session note.** Four rounds running,
  the note said "keep this script — it is the honest recomputation" and the next round re-typed a
  paraphrase of it. A metric the loop claims to *recompute* every round should be executable in one
  command; transcription is how the CRLF trap survived three rounds.
- 2026-08-13 (round 20, continuation): **Verify the ENTERING coverage figure, not just the exiting
  one.** Every round so far recomputed the number *after* its own directions existed, then compared
  it to an inherited "entering" figure — which cannot detect a drift that happened before the round
  started. This round checked the pre-propose commit directly (both target contexts read
  `last_proposed: never`, `directions: []`, and zero of the 152 then-existing direction notes pointed
  at either), so 34 → 36 is measured at both ends for the first time.
- 2026-08-13 (round 20, continuation): **Compare a warning's CONTENT against master, never its line
  number — and this round is the proof.** `grants-common:850` was flagged by clippy in a wave-touched
  file; on master the identical expression sits at **line 781**, because the wave added ~70 lines
  above it. A line-number check calls that a new warning and a count-only check ("31 before, 31
  after") can be fooled by a coincidence. The one warning that WAS new (`registry.rs:106`) was found
  the same way and fixed rather than allowed.
- 2026-08-13 (round 20, continuation): **The Personas app DB is
  `%APPDATA%/com.personas.desktop/personas.db`** — project `512809db-…` = Pumper, 46 contexts, and
  both r20 target names present verbatim (so the outbox anchors are validated, not assumed). Worth
  pinning because the wrong DB **fails silently**: `%LOCALAPPDATA%/personas-athena-test/personas.db`
  exists, has the same `dev_contexts` schema, holds 89 rows — and contains no pumper project at all,
  so every lookup returns zero rows with no error. `dev_projects` has `root_path`, not `path`.
  Query: `sqlite3 "file:<db>?mode=ro" "select c.name from dev_contexts c join dev_projects p on
  c.project_id=p.id where p.root_path like '%pumper%';"`
