---
slug: trades-partial-run-cannot-delete
type: perfect/direction
context: "[[trades-operator-economics]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: 74dac1f
---

## What & why
**A `state-tax` run in which the model returns 30 of 51 jurisdictions tombstones the other 21 and
reports SUCCESS.** This is the only destructive finding in the round: real user-visible data
disappears, the job is green, and the result carries no `removed` count at all.

`state-tax` is the one app in the family that writes through `sync_many_with_provenance`
(`state-tax:256`), so `detect_removed` tombstones every previously-live key absent from this run's
batch. Core's own doc names this exact hole: *"`detect_removed` already refuses an **empty** batch; a
partial batch is the case that guard does not cover"* (`app.rs:641-643`). The designed protection is
the source-health removal guard — and the app **deliberately routed through the sync path to get
it**, saying so in a comment: *"Carried through the sync path (rather than a hand-rolled upsert) so
the degrading-source removal guard still applies."*

**That protection structurally cannot engage.** Two independent reasons, both verified:
- `enforced_state` returns `Healthy` whenever `enforce` is off, and off is the shipping default
  (`resilience/store.rs:689-693, 721-728`).
- **No app in this family calls `observe_extraction`** (grep across all seven crates: 0 hits), so
  even with enforcement switched on there is no health history to enforce against.

So the comment claims a guarantee the code cannot deliver. The app already computes exactly the
signal that would prevent this — `missing_states` (`:237-241`) — and only *reports* it. The
completeness reporting that produced that field shipped in round 1 as `[[trades-output-guards]]`;
this direction is the unfinished half of it: report → **enforce**.

**Rider — near-total rejection is green everywhere in the family.** Only the fully-empty case errors
(`state-tax:243`, `trade-wages:199`, `valuation:176`, `homewyse:270`, `state-licensing:282`). One
surviving record of 51 is a SUCCESS with `rejected_count: 50`, no threshold and no `warnings[]`.

**Rider — tombstones leak back in as live data.** `Datasets::list` deliberately returns removed
records (`datasets.rs:1619-1633`). `sync_operator_economics` reads `state-tax/tax` through `list`
(`trades-common:988`), so a state this run just tombstoned still gets a live row in
`trades/operator_economics` and its rate still enters `median_state_rate` (`:995-1003`). The deletion
is invisible in the joined product and visible in the source dataset — the worst of both.

## Evidence
- `crates/apps/state-tax/src/lib.rs:237-241` — `missing_states` computed, only reported
- `crates/apps/state-tax/src/lib.rs:243-247` — only the fully-empty case errors
- `crates/apps/state-tax/src/lib.rs:250-258` — the comment claiming the removal guard applies
- `crates/core/src/app.rs:641-643` — "a partial batch is the case that guard does not cover"
- `crates/core/src/resilience/store.rs:689-693, 721-728` — `Healthy` whenever `enforce` is off (default)
- grep `observe_extraction` across the seven crates → **0 hits**; adopters are `extractor`,
  `grants-common`, `plugin`
- `crates/core/src/datasets.rs:1619-1633` — `list` returns tombstoned rows
- `crates/apps/trades-common/src/lib.rs:988, 995-1003` — the join reads via `list`, feeds `median_state_rate`
- `crates/apps/state-tax/src/lib.rs:264-280` — `output_shape`/result carry no `removed` count

## Acceptance criteria
1. A run that covers materially less than the expected roster **cannot tombstone the shortfall**.
   A test drives a scripted researcher returning ~30 of 51 states against a store already holding 51
   and proves the missing 21 survive — it must fail against today's code.
2. **Two levers exist and the choice is yours to make, not mine to prescribe.** (a) A completeness
   floor in the app that downgrades the write to a non-removing upsert (and says so in the result)
   when coverage is short; (b) adopting `observe_extraction` so the existing health guard has
   evidence to act on. Read both paths before choosing; if you take (a), say in a doc comment why
   (b) alone is insufficient — the default-off `enforce` is the reason, and a later round must not
   undo the floor believing the guard covers it.
3. The result reports removals honestly: a `removed` count reaches the result whenever the write path
   can tombstone, and a suppressed removal is visible as such rather than silently absent.
4. Near-total rejection is no longer a silent success across the family: a coverage shortfall is
   surfaced in the result (a threshold, a `warnings[]`, or the existing coverage block made
   load-bearing). Pick one shape and apply it consistently to the five agentic apps.
5. The tombstone leak is closed at the join: `sync_operator_economics` must not serve a removed
   `state-tax/tax` record as a live row, and must not count it into `median_state_rate`. A test pins it.
6. `state-tax`'s misleading comment is corrected in the same change — it currently asserts a
   protection that does not exist, which is worse than no comment.

## Risks / non-goals
- **Non-goal:** turning on `[resilience] enforce` by default, or changing core's `sync_many` /
  `detect_removed` semantics. This is fixed at the app/family layer.
- **Non-goal:** changing `Datasets::list`'s deliberate tombstone-returning behaviour — filter at the
  consumer.
- Risk: a completeness floor that is too strict makes a legitimately-shrinking roster unwritable
  forever. Provide the escape hatch the family already uses (`force`) and name it in the doc comment.

## Build record
**The round's highest-value direction: real user-visible data loss on a green job.**
Landed across two builder sessions — the first died to a harness stall mid-wiring and was snapshotted as
`8ac7f1f`; the continuation finished it and re-committed under a real message rather than leaving work
under a `wip` heading (r20's recorded lesson, applied).

The completeness floor (lever a) stands: `coverage::write_snapshot` downgrades to
`upsert_many_with_provenance` below a 0.9 floor and returns `removals_suppressed` carrying the reason.
`allow_shrink` is deliberately **separate from `force`** — `force` is the ordinary way to re-run a
vintage-gated app, so reusing it would switch the floor off on exactly the runs it protects. The doc
comment records why lever (b) alone was insufficient (default-off `enforce` + zero `observe_extraction`),
so a later round cannot undo the floor believing the health guard covers it.

**Made to fail first:** `a_thirty_of_fiftyone_run_does_not_tombstone_the_other_twentyone` failed
`left: 30, right: 51`; `a_tombstoned_state_does_not_reappear_as_a_live_joined_row` failed with
*"NM:Plumbing was tombstoned in state-tax/tax but came back live in the join"*. Also passing:
`a_complete_run_still_tombstones_and_reports_the_removal` — which matters, because it proves the floor is
not "never delete" — and `allow_shrink_lets_a_short_run_delete_but_force_alone_does_not`.

Criterion 4 is **enforced, not asserted in prose**: `coverage::RESULT_FIELDS` + `shape_declares_coverage`,
checked inside each of the five apps' own `manifest_declares_every_param_it_ships`. A sixth app cannot
join the family and quietly omit the coverage block.

**Director review: keep.** Verified `write_snapshot` genuinely downgrades the write rather than just
reporting.
