---
slug: provisioner-coherent-scoring
type: perfect/direction
context: "[[source-provisioner]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 6c16962
---

## What & why
The RuleSet draft is written against `bodies[0]` only but scored as a pooled majority over
up to 3 bodies from DIFFERENT sites — sample count silently changes the pass bar (3 samples:
a field perfect on its own site FAILS `1*2>=3`, and paid repair iterations burn on an
unfixable cross-site mismatch). Degenerate drafts sail through: `Const` rules always bind,
`ContainerEmpty` counts as a match, and the already-computed coercion report (the
wrong-element signal) is never consulted.

## Evidence
- `lib.rs:149-151, 183, 189-202` (pooled two-level majority over all bodies)
- `lib.rs:618` (draft prompt uses bodies[0] only)
- `extract.rs:516-518` (is_miss = Empty|Error → ContainerEmpty counts as match),
  `extract.rs:55` (Const always binds), `extract.rs:541-565` (CoercionStatus computed,
  unused here)

## Acceptance criteria
- Draft scored against its own document; other candidates reported as held-out evidence
  (per-candidate stats), never pooled into the accept bar.
- Deterministic pre-repair rejection of degenerate drafts: const-only rule sets, `each`
  fields with 0 items across all docs, coercion-failed fields — each with its own test,
  each rejected BEFORE a paid repair iteration.
- Confidence semantics redefined coherently (primary-document based) and documented in the
  proposal record.
- Existing pure-fn tests updated honestly (no keeping tests green that encode the old
  incoherent bar).

## Risks / non-goals
- Non-goal: changing the LLM prompts beyond what the new feedback requires; multi-site
  proposals stay single-proposal (splitting per-site is future work).

## Build record
- Builder P1 (opus), wave 1 → master `6c16962`. `dry_run(rules, primary, held_out)` —
  accept bar + confidence read the primary only; held-out candidates reported per-candidate.
  Degenerate rejections extracted (`const_only_rule_set`, `always_empty_each_fields` — with
  the reasoned distinction from the health detector's ContainerEmpty tolerance: a DRAFT has
  no track record — and `coercion_failed_fields`), all pre-repair, all free of metered
  calls. FieldStat now requires transforms to survive. Pooled-bar tests REWRITTEN with
  names saying why, not preserved.
- Gates: worktree 1101/0; master gate green post-pick.
