---
slug: job-race-catalog
type: perfect/direction
context: "[[job-worker]]"
lens: robustness
status: proposed
size: M
proposed: 2026-09-04
raised_by: ai-registry intake pi-02 (peer comparison, earendil-works/pi)
registry: software-engineering/concurrency-guards/race-catalog-with-two-histories
---

## What & why

The job lifecycle is deliberately concurrent — a claim races a recovery sweep, a
cancel races a completion write, a fan-out races a reset, a timeout races the
app future and a cancel token in one `select!`. Every one of those has two legal
durable outcomes, and this tree **already knows what they are**. The reasoning
is real and it is good; it is just scattered across source comments where no
test consumes it and no reader can enumerate it:

- `crates/server/src/worker.rs:1066-1070` — cancel-vs-timeout resolved "exactly
  once, here, because it CONSUMES this run's cancel intent (and records a
  suspend decision so a cancel arriving a microsecond later is told the truth)".
- `crates/server/src/worker.rs:1076-1079` — shutdown-suspend vs operator
  `DELETE`: "an operator's `DELETE /jobs/{id}` inside the drain window outranks
  the drain, because a suspend is a promise to run the work later".
- `crates/server/src/worker.rs:1174-1190` — `fanout_owns_outcome`, the staleness
  fence "that the inline version got for free from having just written the
  completion itself".
- `crates/core/src/storage.rs:1223-1231` — the boot sweep and the lease reaper
  were once *different policies for the same class of row*, unified into
  `issue_recovery_verdicts` citing the registry by name.

That is four two-history statements in prose. There is **one** race-shaped test
module in the tree (`worker.rs:2718 mod fanout_fence_tests`) and one "in either
order" assertion (`scheduler.rs:1137`). Nothing enumerates the set, so nothing
can tell you when a row goes missing.

## The row that is already missing

`finalize_fanout` runs **after** the completion write and fires irreversible
effects — `notify_watches`, `fire_dataset_triggers`, `notify_saved_searches` —
under a comment that says so plainly: *"A webhook is irreversible once sent"*
(`worker.rs:1262-1264`). Both recovery paths select `WHERE status = 'running'`
(`storage.rs:1234`, `storage.rs:1332`). A process that dies inside the fan-out
therefore leaves a row that is terminal, unleased, unreaped and **correct**,
beside side effects that are half delivered and that nothing will ever finish or
report. It produces no stuck row, no alert and no error — only an effect that
silently did not happen.

That is not a bug in any of the four mechanisms above. It is a race whose second
history nobody wrote down, which is exactly the class this direction exists to
surface.

## What to build

1. **`docs/features/job-races.md`** — one table, one row per durable race, each
   naming both legal outcomes. Seed it from the four comments above, plus:
   claim vs sweep, heartbeat vs `reap_stale`, requeue vs claim, cancel vs
   fan-out, and the post-terminal fan-out window. Each row cites the code that
   makes its outcome true.
2. **The two-history rule as an acceptance criterion.** A row with three
   outcomes means the window is wider than one commit boundary — narrow it. A
   row with one means either something serializes the pair (delete the row) or
   the second outcome exists and is unwritten (the fan-out case).
3. **A test module per row**, gating on the storage commit rather than sleeping,
   in the shape `fanout_fence_tests` already demonstrates. Rows that cannot be
   constructed are unfinished design, not untestable code.

Explicitly **not** in scope: changing any of the four existing resolutions. They
are correct. This writes them down and makes them testable.

## Measurable

**Durable races with both outcomes stated and both tested.** Today: 1 of ~8
(`fanout_fence_tests`), with 4 more stated in prose and untested and at least 1
neither stated nor tested. Target: every row in the table constructed in both
orders.

Secondary, and the one that pays for the work: **the post-terminal fan-out
window gains a mover.** Whether by moving reversible effects before the
completion write, giving the fan-out its own position and sweep, or accepting
the loss in writing with the class named and countable — the direction does not
choose, but the table makes the choice unavoidable rather than invisible.

## Gate

`cargo test -p pumper-server` and `cargo clippy --all-targets -D warnings`, both
of which the tree already runs. No new dependency: commit gating is achievable
with the existing storage seam, and `fanout_fence_tests` is the working
precedent.

## Falsifier

If enumerating the set produces rows that are all already fenced and all already
tested — i.e. the fan-out window is the only gap — then the catalog is
documentation rather than defect-finding, and the honest move is to fix that one
window and write a five-line note instead of a table. Run step 1 before step 3
and check: the count of rows that are *stated in prose but untested* is the
number that decides whether this is M or S.
