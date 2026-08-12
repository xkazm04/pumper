---
slug: enqueue-door-parity
type: perfect/direction
context: "[[automation-api]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: 6c3f91a
---

## What & why
POST /apps/{name}/jobs validates merged params against the app's schema (422 with JSON
pointers). POST /schedules stores params VERBATIM — a schedule can be created whose every
run fails, signal arriving hours later as job failures. Worse, the scheduler REPLACES
defaults (resolve_params uses schedule.params wholesale unless null/empty) while HTTP
enqueue shallow-merges — same app, same key, two different effective param sets depending
on which door created the work. Trigger-fired enqueues bypass validation entirely (round
10 proved this class on transact; this closes the remaining doors). After this ships,
work that will fail is refused at the door that accepts it, on every door.

## Evidence
- crates/server/src/routes/schedules.rs:155-168 — create_schedule stores body.params raw.
- crates/server/src/scheduler.rs:398-410 — resolve_params replaces, contradicting
  jobs.rs:19-21's merge contract. Verified live 2026-08-12.
- crates/server/src/routes/jobs.rs:107-113 — the 422 gate the other doors lack.
- triggers.rs enqueue paths (:946, :1039 per scout) — no validate_params call in the file.
- Round-10 precedent: 8e17ca7 (trigger enqueues bypass the validator on transact).

## Acceptance criteria
- POST /schedules validates params against the target app's params_schema and 422s with
  the same pointer-error shape as the job door; e2e drives it over HTTP.
- The scheduler's fire path validates pre-existing/legacy schedule params before enqueue;
  an invalid row is recorded (health or log with a durable marker) and skipped, not
  enqueued-to-die — builder chooses the recording mechanism and states it.
- The merge-vs-replace divergence is resolved deliberately: either the scheduler merges
  like HTTP enqueue, or the difference is documented at both sites + schema-checked so it
  cannot produce invalid effective params. Builder verifies which existing schedules/tests
  depend on replace semantics FIRST — the choice is theirs with reasoning recorded.
- Trigger fire paths validate the target app's params (template + injected fields) before
  enqueue; an invalid template records a bad_params-class outcome in trigger_runs rather
  than silently enqueueing. Vocabulary addition follows TRIGGER_OUTCOMES conventions
  (storage.rs const + any doc listing updated together).
- Extracted, named validation helper shared by all doors (repo law: fixes ship as
  extracted tested functions); inventory-style test enumerating the doors that must call it.

## Risks / non-goals
- Non-goal: changing the params schema language or the job door's behavior.
- Risk: legacy schedules with now-invalid params — fire-path handling must not brick them
  silently; the recorded skip must be observable on GET /schedules.

## Build record
Built by the round-11 Lot A builder, which died pre-commit; the continuation Director
recovered the dirty tree as a wip snapshot, reviewed the full diff against the criteria,
and re-committed it as `6c3f91a`. All criteria met: one shared door
(`mcp::validate_app_params`) behind all six work-creating doors, inventory-enforced via
`EXPECTED_VALIDATING_DOORS` (+ `EXPECTED_EXEMPT_DOORS` for the datahub actuator, which
enqueues the app's own defaults); `POST /schedules` 422s on the MERGED effective params
with the job door's pointer shape; the fire path re-validates legacy rows and skips
WITHOUT touching last_run, surfaced as derived `health: "invalid_params"` (derived not
stored — fixing the row clears it the same instant); merge-vs-replace resolved to MERGE
(`scheduler::schedule_params` shallow-merges over defaults via `routes::merge_params` —
the side both jobs.rs's contract comment and `default_params`' doc promised; reasoning
in the fn doc); trigger hops validate via `hop_params_pass_target_schema` and record a
first-class `bad_params` TRIGGER_OUTCOMES entry with the door's message in detail;
`POST /triggers/{id}/test?fire=true` 422s. e2e (4 scenarios, real HTTP):
schedule_door_refuses_what_the_job_door_refuses_not_a_201,
scheduled_run_merges_over_defaults_not_replaces_them,
legacy_invalid_schedule_is_skipped_visibly_not_enqueued_or_silently_ok,
trigger_with_a_bad_template_records_bad_params_instead_of_firing.
Docs: http-api.md (schedules/triggers rows), runtime.md (health + params semantics),
triggers.md (bad_params row). Gates: check + lib 450/0 + e2e 4/4 green.
