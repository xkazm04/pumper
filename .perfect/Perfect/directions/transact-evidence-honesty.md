---
slug: transact-evidence-honesty  type: perfect/direction
context: "[[browser-transact]]"  lens: robustness
status: shipped                  size: M
proposed: 2026-08-11  accepted: 2026-08-11  shipped: 2026-08-11  commit: 428d2e9
---
## What & why
The transact app's entire product is an evidence bundle a human reviews before ever approving a
live submit — and today that bundle cannot distinguish a fully successful flow from one whose
every selector 404'd. `steps_completed` counts attempts; the confirmation-state signal
(`selector_found`) is computed and thrown away; nobody checks whether the submit target even
exists on the final page; the identity (profile) the flow ran under isn't recorded; and a DOM
over the size cap destroys ALL evidence after the flow already acted. Make the bundle tell the
truth a reviewer needs.

## Evidence
- engine-browser/src/lib.rs:831 — `completed += 1` outside every match arm; failed Click/Type
  (:783, :792) still count. A 3-selector-404 flow reports `steps_completed: 3`.
- engine-browser/src/lib.rs:706-719 — evidence_from_render discards `RenderedPage.selector_found`
  (computed at :543-551); `would_submit` echoed verbatim, never assessed (:713).
- engine-browser/src/lib.rs:645-652 — DOM over max_html_bytes → Err → whole job fails, zero
  evidence, AFTER navigation/fills/clicks already happened.
- engine-browser/src/lib.rs:562-567 — whole action list shares one nav_timeout; deadline break at
  :764-767 leaves only `steps_completed < len` as signal, which nothing compares.
- apps/transact/src/lib.rs:135-147 — evidence.json: no profile, no per-step outcomes, no DOM
  size/truncation marker, no page title.
- core/src/engine.rs:349-373 — TransactEvidence shape.

## Acceptance criteria
1. Executed actions produce per-step outcomes (e.g. ok / selector_missing / action_failed /
   deadline_hit — exact taxonomy is the builder's call), surfaced in evidence.json and the job
   result; `steps_completed` counts SUCCESSES, with requested-vs-attempted-vs-succeeded all
   recoverable from the bundle. The render path may ignore or trace outcomes — don't regress it.
2. Confirmation state is honest: the transact-level `wait_for_selector` outcome reaches the
   evidence (wire `selector_found` through, or an equivalent explicit signal).
3. The submit target is assessed on the final page (exists / visible / enabled — via the existing
   evaluate seam; depth is the builder's judgment) and recorded, so the one question a reviewer
   most needs answered is answered.
4. The evidence records the profile the flow ran under (or its absence) and the DOM snapshot's
   size + a truncation flag. For the TRANSACT path, an over-cap DOM is truncated-and-flagged
   instead of destroying the bundle; the read-only render path keeps fail-closed semantics.
5. A test proves a flow with failing selectors produces evidence DISTINGUISHABLE from a successful
   flow (name it after the anti-pattern), and execute_actions' outcome accounting gets direct
   tests (it currently has zero).

## Risks / non-goals
- execute_actions is shared with ordinary renders — changing its return type must not disturb the
  fetch ladder's use. Non-goals: screenshots, iframe/shadow-DOM support, live submit, URL policy.
- Per-step outcome for Repeat can be coarse (one outcome for the repeat block is fine) — say so
  in the evidence rather than pretending granularity.

## Build record
- Shipped `428d2e9` (Lot T, opus). All 5 criteria met. `execute_actions` returns
  `Vec<StepOutcome>` (ok/selector_missing/action_failed/partial); `RenderedPage.actions_completed`
  keeps attempt semantics for the ladder, outcomes ride in new `action_outcomes`. Evidence gets
  requested/attempted/completed (successes) + deadline_hit + outcomes + wait_for_selector_found +
  submit_target probe (found/visible/enabled/tag/label via one combined evaluate; found:null =
  "could not look") + profile + dom_bytes/dom_truncated. Transact path truncates over-cap DOM
  (render.max_body_bytes=Some(0) then truncate_to_cap at char boundary); render path stays
  fail-closed. Review: keep — extracted fns all tested, failed-vs-clean flow distinguishability
  proven end-to-end through the app.
- Honest gaps (recorded): no live Chrome run (probe JS shape unverified in a real browser —
  engine-browser/tests/render.rs is the home, needs a fixture page); execute_actions loop itself
  has no mock seam (accounting extracted+tested, loop by inspection); submit_target visibility
  heuristic is a signal, not a gate.
