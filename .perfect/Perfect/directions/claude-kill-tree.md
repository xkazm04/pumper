---
slug: claude-kill-tree
type: perfect/direction
context: "[[claude-engine]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
On Windows — the platform this repo targets, and the DEFAULT config path (`binary =
"claude"`, not an .exe) — the engine spawns `cmd.exe /C claude …` and relies on
`kill_on_drop(true)` for timeout cleanup. That kills **only cmd.exe**: the real
`claude`/node grandchild is orphaned and keeps running its whole agentic loop, keeps
its API connection, and keeps spending real money — invisibly, with the result
discarded. Default role budgets are `None`, so the orphan has no CLI-side ceiling
either. Every engine timeout (600s default), job timeout, and job cancel takes this
path. The user moment: "I cancelled/timed-out a research job and it still billed me
for the full run — and nothing anywhere shows that."

## Evidence
- `crates/engine-claude/src/lib.rs:94-100` — the `cmd /C` shim branch (default on
  Windows; npm installs `claude` as .ps1/.cmd shims).
- `crates/engine-claude/src/lib.rs:117-120` — `kill_on_drop(true)` is the only kill
  mechanism; `:138-142` — timeout drops the future (kills cmd.exe only).
- `crates/engine-claude/src/lib.rs:142` — `let _ = writer.await;` is unreachable on
  the timeout path (`?` at `:140` returns first): the stdin-writer task leaks.
- `crates/core/src/config.rs:1266` — default `binary: "claude"` → shim path.
- Scout: no Job Object / process-group handling anywhere in the workspace; zero
  tests exist for `research()` (in-crate tests cover only `parse_loose_json`).

## Acceptance criteria
- [ ] On timeout (and on any drop/cancel path), the ENTIRE process tree dies —
      on Windows including the grandchild behind the `cmd /C` shim. Implementation
      is the builder's call with tradeoffs weighed: explicit `child.kill()` plus a
      Windows tree-kill (`taskkill /PID <pid> /T /F` on the shim's pid is
      dependency-free; a Job Object via the `windows` crate is stronger but heavier).
      State in code why the chosen mechanism suffices.
- [ ] The timeout path also awaits/aborts the stdin-writer task — no leaked task
      blocked on a full pipe.
- [ ] An engine-level test proves it: spawn a FAKE cli (a temp .cmd/.bat on Windows,
      sh script elsewhere) that itself spawns a long-lived child and then sleeps;
      drive `research()` into timeout; assert the grandchild is gone (poll by pid).
      This is the first test of `research()` in the crate's history — the fake-CLI
      harness it introduces should also cover: non-zero exit, malformed stdout, and
      the happy path (fixture envelope). Mark env-dependent variants `#[ignore]`
      per repo convention ONLY if they genuinely need a real environment; a temp
      script needs none.
- [ ] A timeout error message names what was done ("process tree killed") so an
      operator reading logs can distinguish it from the old orphan behavior.
- [ ] No behavior change on the success path; POSIX path stays correct (direct
      child, no shim).

## Risks / non-goals
- Non-goal: changing worker-level cancel timing (`worker.rs:692-706` delays the drop
  until after storage writes — noted, out of scope).
- Non-goal: session pooling / warm processes.
- Risk: `taskkill` availability — it ships with every supported Windows; assert and
  fall back to `child.kill()` with a warn.
- Risk: test flakiness on process polling — use bounded retry loops, not fixed sleeps.

## Build record
(pending)
