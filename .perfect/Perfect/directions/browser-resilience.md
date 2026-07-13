---
slug: browser-resilience
type: perfect/direction
context: "[[Fetch Engines (HTTP / Browser / Claude)]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: a57ee1c
---

## What & why
Chrome lazy-launches into a OnceCell and lives forever; on crash the cell stays populated with a dead browser — no relaunch possible, every future render fails until server restart. No render concurrency cap (N renders = N unbounded tabs). Wait timeouts are silent (partial DOM indistinguishable from success). Fix all three.

## Evidence
- Dead-browser trap: crates/engine-browser/src/lib.rs:27-64 (OnceCell, handler loop warn "chrome exited?" :60)
- No concurrency cap: lib.rs:74 (new_page per render, unbounded)
- Silent waits: lib.rs:82, 89-90 (warn-only timeout, break-and-continue selector poll)

## Acceptance criteria
- [ ] Relaunchable browser holder (health check on acquire; crashed browser detected → relaunch; OnceCell replaced).
- [ ] Render semaphore: `[browser] max_concurrent_renders` (config, #[serde(default)] + Default; sane default e.g. 4).
- [ ] RenderResult gains wait outcome flags (nav_timed_out, selector_found: Option<bool>) so callers can tell partial DOM from success.
- [ ] Crash-recovery test (kill the browser handle, next render succeeds) if feasible headless; else unit-test the holder logic and state honestly.
- [ ] docs/features/fetching.md updated.

## Risks / non-goals
- Non-goal: multi-browser pool; one relaunchable instance suffices.

## Build record
(pending)
