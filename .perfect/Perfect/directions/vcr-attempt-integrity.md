---
slug: vcr-attempt-integrity
type: perfect/direction
context: "[[app-runtime]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-11
commit: 4e3647a
---

## What & why
A `record: true` job that retries produces a corrupt cassette: the recorder opens the
file in append mode and the loader is first-wins, so attempt 1's partial recording
shadows attempt 2's complete one — replay then reproduces the FAILED attempt's data
while claiming determinism. The per-attempt `written` counter also defeats the 128 MiB
cassette cap across attempts. Retries are the norm in this queue (any app error
retries); the determinism feature must record the run that actually succeeded.

## Evidence
- `crates/core/src/vcr.rs:321–330` — OpenOptions append(true).
- `crates/core/src/vcr.rs:363–371` — first recording wins on load.
- `crates/server/src/worker.rs:423–427` — artifacts_dir = root/app/job_id, no attempt
  component; recorder constructed per attempt.
- `crates/core/src/vcr.rs:280` — written counter resets per Recorder.

## Acceptance criteria
- [ ] A new attempt in Record mode starts from a clean cassette. Default design:
      truncate/recreate at recorder construction (a failed attempt's partial recording
      is worthless); attempt-scoped naming is an acceptable alternative IF resolution
      is deterministic and documented — builder states the tradeoff either way.
- [ ] Anti-pattern test (e.g. retry_does_not_replay_failed_attempt): record attempt
      fails midway → retry succeeds → replay resolves only attempt-2 entries.
- [ ] The 128 MiB cap binds on the cassette's real size across attempts, not the
      in-memory counter.
- [ ] Replay-mode behavior unchanged; existing VCR tests green.
- [ ] Doc-sync: runtime.md VCR section if the documented contract text changes.

## Risks / non-goals
- Non-goal: multi-attempt cassette history/retention.
- Careful: the shutdown-suspend path re-queues WITHOUT burning an attempt — decide and
  document whether a suspended recording resumes (same attempt id) or restarts; the
  test must pin whichever is chosen.

## Build record
Shipped `4e3647a` (Lot A, opus, 2026-08-11). Design better than the brief's default:
CassetteStart::{Fresh,Resume} decided by whether a durable checkpoint was restored —
"the cassette records the job's work, and work survives exactly when a checkpoint
does." Fresh truncates; Resume appends AND seeds the cap from real file bytes. Policy
applied eagerly (Recorder::prepare at attempt start) and lazily (record()), so a
fetch-nothing attempt can't leave a stale cassette. Suspend therefore RESUMES the
recording (pinned by an e2e driving the real shutdown drain). Builder refutations:
retry-after-permanent-fail always clears the checkpoint → always Fresh; runtime.md had
NO VCR section at all — written new. Review: keep.
