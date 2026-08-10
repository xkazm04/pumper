---
slug: worker-panic-containment
type: perfect/direction
context: "[[job-worker]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 4b80eb2
---
## What & why
There is no `catch_unwind` in the worker. A panic inside `app.run()` unwinds the per-job spawned
task; the job row stays `running` until the reaper notices it at least `stale_after_secs` (default
120s) later, and the user is then told `"lease expired (heartbeat stale)"` — which misdescribes what
happened. The panic message, the only thing that explains the failure, exists solely in the log.
Catch it, fail the job honestly through the normal fenced `fail()` path, and free the slot at once.

## Evidence
- No `catch_unwind` anywhere in `crates/server/src/worker.rs` (scout-verified).
- Reaper's error string: `crates/core/src/storage.rs:616` (`lease expired (heartbeat stale)`).
- Stale window default 120s: `crates/core/src/config.rs:874`.
- Heartbeat only ticks while the future yields: `crates/server/src/worker.rs:387-396`.

## Acceptance criteria
- A panicking app fails its job on the same tick, not after the stale window.
- `job.error` carries the panic payload (and location when available), distinguishable from
  app-returned errors, timeouts and reaped leases.
- Attempt fencing, backoff and retry semantics unchanged; the reaper remains the backstop for true
  wedges (non-yielding loops), which this cannot catch — say so in the doc comment.
- A deliberately panicking test app proves the path end-to-end.

## Risks / non-goals
Do not catch panics that indicate a poisoned global state as if they were routine; not an
`abort`-on-panic policy change. Out of scope: detecting non-yielding wedges.

## Build record
`catch_unwind(AssertUnwindSafe(app.run(ctx)))` → new `Outcome::Panicked` routed through the SAME
attempt-fenced `fail()` path; `job.error` = `panicked: <payload> (at file:line:col)`. Location comes
from a process-`Once` panic hook that **chains to the previously installed hook** (default backtrace
and any Sentry hook survive). Non-yielding-wedge limitation documented at the seam and in
`docs/features/runtime.md`; reaper remains the backstop. 3 unit tests + 2 e2e
(`a_panicking_app_fails_its_job_now_not_after_the_stale_window`, `a_panic_retries_like_any_other_failure`).
Director review: fencing/backoff semantics unchanged, thread-local location read on the same thread
that unwinds — correct. Known risk (builder-flagged, accepted): a host binary installing its own
panic hook after the first job loses the location (payload still captured).
