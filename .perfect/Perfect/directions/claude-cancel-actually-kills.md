---
slug: claude-cancel-actually-kills
type: perfect/direction
context: "[[claude-engine]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**Cancelling a Claude job returns `cancelled` and keeps spending money.**

The engine looks fully defended, and on its own paths it is: `kill_on_drop(true)` is set
(`engine-claude/src/lib.rs:232`), a deadline governs both `child.wait()` and the stdout drain
(`:213`, `:292`), and a real process-tree kill exists — `abandon_run` (`:409-423`) →
`kill_process_tree` (`:447-473`) → `taskkill /PID <pid> /T /F` (`:481-488`).

**But `kill_process_tree` is reachable only from `abandon_run`**, i.e. only from the engine's own
timeout / wait-error / drain-timeout branches (`:296`, `:299`, `:323`). It is never reached when the
`research()` future is simply **dropped**. On drop only `kill_on_drop` runs — and the file's own
comment says exactly what that is worth (`:229-232`):

> `// Backstop only. It kills the DIRECT child, which on the Windows shim path is cmd.exe and not`
> `// the process that spends money — see kill_process_tree.`

The drop path is the ordinary one. `worker.rs:741-755` races the app future against the cancel
token; `_ = cancel.cancelled() => break Outcome::Cancelled` leaves the loop without polling `run`
again, and `execute` returns at `:784` (shutdown suspend) or `:802` (user cancel), dropping the
pinned future at `:724` and with it the in-flight `research()` future and its `Child`.

And the **default config puts you on the shim**: `binary` defaults to `"claude"`
(`config.rs:1397`), so `via_shim = cfg!(windows) && !binary.ends_with(".exe")` (`lib.rs:149`) is
true — the direct child is `cmd.exe`, and the `claude`/node grandchild survives, keeps running its
agentic loop, and keeps spending. The user moment: an operator hits `DELETE /jobs/{id}`, the API
says `cancelled`, the job row says `cancelled`, and the bill keeps climbing.

Secondary leak on the same path: the three tasks spawned at `:263`, `:273`, `:278` are held as
`JoinHandle`s, and **dropping a `JoinHandle` detaches rather than aborts**. Their `AbortHandle`s
(`:283-287`) are consumed only by `abandon_run`, so a cancelled run also detaches three tasks
holding the pipe ends.

**The repo has already solved this exact bug class one crate over.**
`engine-browser/src/lib.rs:384-389` is `impl Drop for RenderScope` — it calls `abort_tasks()` then
reclaims the page — with the reasoning spelled out at `:1826-1828` ("dropping a `JoinHandle`
detaches its task instead of aborting it — so the tab and its one or two CDP tasks survived the
render that owned them, invisibly") and a regression test at `:1829-1834`,
`dropped_render_not_left_as_a_zombie_tab_with_detached_tasks`. Chrome is a real `.exe`, so
`kill_on_drop` sufficed there; engine-claude is the one with the shim indirection. **Layer on that
pattern; do not invent a second one.**

## Evidence

- `crates/engine-claude/src/lib.rs:229-232` — the comment naming the exact gap.
- `:409-423` `abandon_run`, `:447-473` `kill_process_tree`, `:481-488` the `taskkill /T /F`.
- `:296`, `:299`, `:323` — the only three callers of `abandon_run`, all engine-internal.
- `:263`, `:273`, `:278` — the three spawned tasks; `:283-287` their `AbortHandle`s.
- `:149` `via_shim`; `crates/core/src/config.rs:1397` `binary` default `"claude"`.
- `crates/server/src/worker.rs:741-755`, `:784`, `:802`, `:724` — the drop path.
- `crates/engine-browser/src/lib.rs:384-389`, `:1826-1834` — the pattern + its regression test.
- `crates/engine-claude/tests/fake_cli.rs:737-743` — asserts `"process tree killed"`, but only on
  the **timeout** path. No test drops the future mid-run. That gap is why this survived.

## Acceptance criteria

1. Dropping an in-flight `research()` future kills the **process tree**, not just the direct child —
   on the Windows shim path the grandchild that spends money must die.
2. The same drop aborts the three spawned tasks instead of detaching them.
3. A test proves it by **dropping the future mid-run** (not by timing out) and asserting the
   grandchild died — extend the existing `fake_cli.rs` marker-file technique (`:659`, `:737`) rather
   than inventing a new harness. Name it after the anti-pattern (`x_not_y` style).
4. The existing timeout-path behavior is unchanged and its test still passes.
5. The success path does not double-kill or pay any new cost.

## Risks / non-goals

- **The hard part is that tree-kill is async and `Drop` is not.** The browser crate shows the abort
  half; the kill half wants a detached `tokio::spawn` of the `taskkill` from `drop`. If the runtime
  is already shutting down, a spawn from `drop` can be refused — handle that explicitly rather than
  silently, and say in the report what happens in that case.
- **Non-goal:** changing the worker's cancellation shape (`worker.rs`). The worker is correct; it is
  the engine that leaks. `worker.rs` is NOT in the write set.
- **Non-goal:** the timeout path. It already does the right thing.

## Build record

(filled during build)
