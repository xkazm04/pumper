---
slug: trigger-decision-ledger
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 5d99cc6
---

## What & why
"Why didn't my trigger fire" is unanswerable today. Every negative decision — filter
non-match, dedup suppression, cycle skip, depth skip, predicate veto, unregistered target,
eval-set load failure — is log-only or fully silent; the `/runs` view is `jobs WHERE
trigger_id`, i.e. successful fires only. The context map even lists a `trigger_runs` table
that exists nowhere. Ship a real per-decision ledger and surface it through the existing
`GET /triggers/{id}/runs`, so the 3-second operator question has one answer surface. Also
makes the fail-open evaluation drop (`:382/:435/:481`) visible instead of silent.

## Evidence
- No migration creates `trigger_runs`; only hits are context-map.json + handler fn name
- `triggers.rs:394/:441` (change/status non-match — no log), `:501` (filter miss — bare
  continue), `:587/:543` (dedup — silent), `:401/:405/:447/:451` (cycle/depth — warn only)
- `routes/triggers.rs:420-432` — /runs never loads the trigger, returns 200 on deleted id
- `ingress.rs:343` — `triggers_fired: 0` with no per-trigger breakdown

## Acceptance criteria
- New table (migration) recording per trigger per source event: outcome (fired | skip reason),
  source kind/job/dataset/event id, timestamp, and the enqueued job id when fired.
- Every decision path in `fire_dataset_triggers` / `fire_terminal_triggers` /
  `fire_external_triggers` / `enqueue_hop` writes a row — including eval-set load failure.
- `GET /triggers/{id}/runs` returns fires AND skips with reasons (paginated); 404 on unknown
  trigger id.
- Retention: bounded (cap or age-based prune wired into an existing maintenance tick).
- e2e test: a filter-miss and a dedup suppression are distinguishable via the API.
- Ledger writes are fail-open (never block the hop) but loud on failure.
- Doc-sync: triggers.md documents the ledger + skip reasons.

## Risks / non-goals
- Write amplification on wide fan-outs — acceptable at current scale; retention keeps it flat.
- Non-goal: retrying dropped evaluations (visibility first; durability is a future direction).

## Build record
- Builder T1 (opus), wave 1 → master `5d99cc6`. Migration `0036_trigger_runs.sql`; 12
  outcomes (fired, eval_set_error, no_change_match, status_mismatch, filter_miss,
  bad_filters, predicate_veto, cycle, depth, target_unregistered, dedup, enqueue_failed);
  every path in all three fire fns + enqueue_hop records; eval-set failure recorded under
  `'*'` sentinel (builder caught that my acceptance was internally incomplete — that failure
  belongs to no trigger; documented as known gap). `GET /triggers/{id}/runs` → jobs +
  decisions + cursor, 404 on unknown id (was 200 {count:0}). Retention: 14 days default-on
  as a constant (builder judgment I endorse — diagnostic table, not evidence ledger), pruned
  ≤hourly off `reap_once` BEFORE the stale==0 early return (reaper-disabled deployments
  still write decisions). Ledger writes fail-open + loud.
- e2e: `a_filter_miss_is_distinguishable_from_a_dedup_suppression`,
  `unknown_trigger_runs_is_404_not_an_empty_200`, cursor paging test.
- Known gaps: retention not config-keyed (config.rs out of scope); prune aging not tested
  past the window (no clock injection in storage).
- Gates: worktree 1050/0; master full-workspace green post-pick.
