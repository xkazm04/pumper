---
slug: enqueue-door-parity
type: perfect/direction
context: "[[automation-api]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
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
(pending)
