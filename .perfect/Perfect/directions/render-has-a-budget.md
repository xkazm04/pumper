---
slug: render-has-a-budget
type: perfect/direction
context: "[[browser-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: 2026-08-13
commit: ee4f4f4
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

**Shipped `ee4f4f4` · verdict KEEP.** `[browser] render_budget_secs` (default 180, `0` disables) is
one deadline for the whole render. Stage caps become `min(own cap, budget)`; the untimed CDP awaits
(`goto`, `evaluate`, `getResponseBody`, `content`, `page.url`, plus `new_page`/`event_listener`) are
wrapped; `wait_for_selector` probes **under** the deadline; each action runs under `timeout_at`.

**Criterion 2 decided against the alternative I offered, with a reason:** the budget is deliberately
**not** a multiple of `nav_timeout_secs`, because that key is per-navigation patience and multiplying
it would silently multiply the tier's worst case for an operator who raised it for one slow site. The
clock starts once the render owns a Chrome, so queueing behind a busy semaphore costs a render nothing.

**Builder's correction to the design:** *checking the clock between steps cannot cut a step that never
ends* — so each individual action is wrapped, not just the loop.

Waits are clamped, not rejected, with a 5s `CAPTURE_RESERVE` so a pathological wait cannot sleep the
budget away and then die at `content()`. A clamped `wait_ms` reports `partial` in `action_outcomes`.

**One considered deviation, accepted:** budget exhaustion stays **retryable** — "this page was slow
*this time*" is a fact about a live site, not about the request. Stated in the error text and the doc.

Pure functions `budget_deadline`/`stage_deadline`/`clamp_wait_ms`/`budget_exhausted` + 6 tests incl.
`pathological_wait_not_allowed_to_outlive_the_render_budget` (`u64::MAX` → clamped) and a paused-clock
proof that an endless await is cut. `max_html_bytes` documented for the first time.

**Refuted:** the brief said "four untimed CDP awaits" — there are **six** (`page.url()` unlisted).

**Not verified:** 180s is argued from the ~91s timed worst case, not measured against real slow pages.

**Banked (Director, deliberately not rushed at round end):** `RenderedPage.budget_truncated` would
make the *settle-wait* truncation in-band as the `wait_ms` truncation already is; today it is a
`warn!` only. Needs one `..Default::default()` at `crates/core/tests/eval_tier3_extraction.rs:363`.
