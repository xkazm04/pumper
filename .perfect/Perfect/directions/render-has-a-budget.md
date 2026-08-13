---
slug: render-has-a-budget
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

`[browser] nav_timeout_secs` reads like a render budget and bounds **one of six** awaits.
A render can hold its semaphore permit — one of only four — for an unbounded time, and a
caller can request that directly: `extra_wait_ms` and the `wait_ms` action are raw `u64`
milliseconds with no clamp and no schema `maximum`. Four such jobs wedge the browser tier
for **every app on the box**, and a half-dead Chrome (alive enough that the liveness flag
stays true, wedged enough not to answer CDP) does the same with no caller error at all.

Worst case with defaults, counting only the *timed* waits, is already ≥ 91 s; the untimed
ones (`goto`, `content()`, `evaluate`, `find_element`) add an unbounded tail. The only
thing that eventually breaks the wedge is the job timeout — which then leaks a tab
(see [[render-cancel-safe]]).

## Evidence

- `crates/engine-browser/src/lib.rs:556-559` — `settle_ms = req.extra_wait_ms.unwrap_or(default_wait_ms)`,
  no clamp; `:888` — `sleep(Duration::from_millis(*ms))` for `PageAction::WaitMs`, no deadline clamp.
- `crates/engine-browser/src/lib.rs:830-833` — `execute_actions` checks the deadline
  *before* each action, never applies it *to* one.
- Untimed CDP awaits: `:525` `goto`, `:574` `evaluate`, `:599` `execute(GetResponseBodyParams)`,
  `:641` `close`, `:644` `content()`, `:788` `find_element`. Only `:537` `wait_for_navigation`
  is wrapped in `tokio::time::timeout`.
- `crates/engine-browser/src/lib.rs:782-796` — `wait_for_selector` checks its deadline only
  *after* `find_element` returns, so one hung call blows the deadline unboundedly.
- Reachable from job params: `crates/apps/transact/src/lib.rs:103` declares
  `"extra_wait_ms": {"type":"integer","minimum":0}` with **no `maximum`**; `steps` items are
  `{"type":"object","required":["action"]}` (`:86-90`), so any `wait_ms` magnitude passes the door.
- Permit held for the whole render: `:392` acquire, default `max_concurrent_renders = 4`
  (`crates/core/src/config.rs:1203`).

## Acceptance criteria

1. One **total render deadline** exists and every wait in the render path is bounded by it —
   including the four currently-untimed CDP awaits and `wait_for_selector`'s poll loop.
   A render cannot outlive its budget regardless of caller input or Chrome state.
2. The budget's source is stated and defensible: either a new `[browser]` key (add it to
   `config.toml`'s `[browser]` block **and** `docs/features/fetching.md`'s config list) or a
   derived multiple of `nav_timeout_secs`. Pick one, justify it, do not add two knobs.
3. `extra_wait_ms` and `wait_ms` are clamped to the remaining budget rather than rejected —
   a long wait should be truncated with a visible signal, not turn into a job failure.
   The truncation is observable (existing `nav_timed_out`/`actions_completed`-style honesty).
4. The clamp and the deadline arithmetic are **pure named functions with CI tests** (no Chrome).
   Include a test proving a pathological `extra_wait_ms` cannot exceed the budget.
5. Budget exhaustion produces a clear error/verdict naming the budget — not a generic
   `Error::Browser` an operator has to guess at.
6. `docs/features/fetching.md` states the render budget and the clamp in the browser-tier
   section. While you are in that config list, add the missing `max_html_bytes`
   (`crates/core/src/config.rs:1192`, honored at `lib.rs:649`) — it is documented nowhere.

## Risks / non-goals

- **Non-goal:** cancellation cleanup ([[render-cancel-safe]]) — same file, sibling direction,
  do them in order and keep the commits separate.
- Risk: clamping too aggressively breaks genuine slow-page renders. Default the budget
  generously (the current worst case is the floor, not the target).
- Risk: wrapping `page.close()` in a timeout on the cleanup path must not turn a slow close
  into a render failure.

## Build record

(filled during build)
