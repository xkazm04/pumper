---
slug: render-cancel-safe
type: perfect/direction
context: "[[browser-engine]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

Every job cancel (`DELETE /jobs/{id}`) and every job timeout that lands mid-render
**permanently leaks a Chrome tab plus one or two detached tokio tasks**, and nothing
in the product can see it. The worker races the app future against cancel/timeout and
`break`s out of the select — which *drops* the pinned future. `BrowserEngine::render`
closes its page and aborts its drainer/capture tasks only on two success-shaped paths;
a dropped future runs neither. `JoinHandle` drop detaches, it does not abort, so the
tasks keep servicing a dead tab's CDP events.

The tabs accumulate on the shared Chrome until the 200-render recycle relaunches it,
so up to ~200 zombie tabs can co-reside. `--js-flags=--max-old-space-size=512` bounds
V8 heap per renderer, not tab count. On a queue with a tight `job_timeout_secs` this is
the dominant memory failure mode of the browser tier, and it is entirely invisible — no
metric, no log, no doctor check.

## Evidence

- `crates/server/src/worker.rs:673-675` — `catch_unwind(AssertUnwindSafe(app.run(ctx)))`,
  `tokio::pin!(run)`; the select at `:692-706` `break`s on `cancel.cancelled()` (`:693`)
  and on the wall-clock `sleep` (`:694`), dropping the future. **Director-verified.**
- `crates/engine-browser/src/lib.rs:408` `new_page(...)` … `:641` `page.close()` — the
  only close.
- `crates/engine-browser/src/lib.rs:424`, `:479` — `Some(tokio::spawn(...))` bound to
  plain locals. `.abort()` appears only at `:527`/`:530` (goto-error path) and
  `:636`/`:639` (happy path). **Director-verified by grep: 2 spawns, 4 aborts, none on drop.**
- chromiumoxide has no `impl Drop for Page` (scout verified against the vendored crate);
  only `Drop for Browser` reaps the process.
- Cancellation reaches this from `crates/server/src/routes/jobs.rs:398-414` and from the
  shutdown drain.

## Acceptance criteria

1. A render that is cancelled (future dropped) at **any** point between page creation
   and return closes its page and aborts both auxiliary tasks. Prefer an RAII scope
   guard owning the page handle + the `JoinHandle`s over adding cleanup to more paths —
   the bug is that cleanup lives on paths at all.
2. The guard's cleanup is **CI-testable without Chrome**: extract it as a named type over
   abortable handles + a closable resource, and test that dropping it aborts the handles
   and triggers the close exactly once. Name the test after the anti-pattern
   (`x_not_y` style, per `.claude/CLAUDE.md`).
3. Double-close/double-abort is impossible: the existing happy-path and goto-error
   cleanups either become the guard's job or are proven idempotent.
4. `Drop` cannot `.await`; whatever mechanism closes the page from a synchronous drop
   (detached task, best-effort) is documented at the call site with its failure mode.
5. No behavioral change on the success path — the same page is closed at the same point,
   verified by the existing ignored render tests still compiling and by unit tests.
6. If any leak remains structurally possible after the change, say so in the build record
   rather than claiming completeness.

## Risks / non-goals

- **Non-goal:** fixing the wedged-Chrome-hangs-forever class — that is [[render-has-a-budget]].
- Risk: a detached close task on a Chrome that is already dead must not panic or log
  scarily; treat close failure as expected.
- Risk: the guard must not extend the page's lifetime past the semaphore permit in a way
  that changes concurrency behavior.

## Build record

(filled during build)
