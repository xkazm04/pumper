---
slug: engine-conformance-suite
type: perfect/direction
context: "[[engine-contracts]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---
## What & why
Five capability traits define what an engine *is* in this repo, and **nothing anywhere runs a
fixed battery over every implementor.** Each engine tests only itself. Both existing guard rails
(`fetch_chokepoint.rs`, `llm_chokepoint.rs`) police **consumers**; no guard has ever policed an
**implementor**. The result is a set of silent capability holes, and one of them has the wrong
retry class.

**The retry-class bug is the sharp end.** `Browser::transact`'s default body returns
`Error::Browser` (`engine.rs:1018`). `Error::is_terminal_for_job` (`error.rs:311-313`) is
`matches!(self, Error::BudgetExhausted(_) | Error::Transact(_))` — so `Error::Browser` is
**retryable**. The `Transact` variant exists precisely so a flow refusal fails once. An engine
that never implemented `transact` therefore costs a job its entire backoff ladder producing
identical "this engine does not support transact flows" errors. The trait's own doc says it
should "fail loudly"; it fails loudly four times and bills for the privilege.

**The capability holes are the broad end.** `fetch_bytes` is a default-bodied method overridden
by exactly **one** of four production `HttpClient`s (`engine-http:728`). `ArchiveEngine`,
`RemoteEngine` and the crawl's `MeteringHttpClient` all inherit the "this engine does not support
binary fetch_bytes" error. `apps/cms-fee-schedule/src/lib.rs:518` calls
`ctx.engines.http.fetch_bytes(...)` and works only because `state.rs:307` happens to place the
raw `HttpEngine` there — one wiring change away from a runtime failure, with no type, test or
comment pinning the invariant.

And the shared test harness has the same asymmetry: `Dead::transact` was given an override
specifically to preserve its panic contract, with a doc comment naming the hazard
(`core/src/testing.rs:90-97`) — while **`Dead::fetch_bytes` was not** (`:80-84`), so a write-path
test that accidentally calls it gets a plausible `Err` instead of the intended panic.

The user moment: *"I added an engine, everything compiled, every test passed, and it silently
couldn't do binary fetches — we found out in production."*

## Evidence (Director-verified)
- `crates/core/src/error.rs:311-313` — `is_terminal_for_job` = `BudgetExhausted | Transact` only.
- `crates/core/src/engine.rs:1008-1023` — `Browser::transact` default returns `Error::Browser`.
- `crates/core/src/engine.rs:998` — `HttpClient::fetch_bytes` default returns `Error::Http`.
- Only override: `crates/engine-http/src/lib.rs:728`. Not overridden by
  `crates/engine-archive/src/lib.rs:293`, `crates/engine-remote/src/lib.rs:176`,
  `crates/apps/crawl/src/lib.rs:105`.
- `crates/core/src/testing.rs:80-84` (`Dead::fetch_bytes`, no panic) vs `:90-97`
  (`Dead::transact`, panics with the hazard documented).
- Consumer that depends on the unpinned invariant: `crates/apps/cms-fee-schedule/src/lib.rs:518`.
- Wiring that makes it true today: `crates/server/src/state.rs:298-307`.
- **Architectural constraint, verified:** `crates/core/Cargo.toml` depends on **no** engine crate
  (apps/engines depend on core, never the reverse). `crates/server/Cargo.toml:15-21` depends on
  all seven. A cross-engine conformance suite therefore **cannot live in core** — it belongs in
  `crates/server`.

## Acceptance criteria
1. A **conformance battery** that takes a `&dyn HttpClient` (and, where it makes sense, the other
   traits) and asserts the obligations every implementor owes, run against **every production
   implementor** — not a bespoke stub. It must live where the dependency rule permits
   (`crates/server`; see Evidence). If you conclude a different home is better, argue it.
2. The battery **fails today** against at least one real implementor, and you say which and why.
   A conformance suite that passes on first run has not been calibrated — find the holes it was
   written to find, then decide per hole: fix, or record as a deliberate documented exemption.
3. `Browser::transact`'s default returns a **terminal** error, so an unsupported flow fails once
   instead of burning the ladder. Check `Error::Transact` is the right variant and that nothing
   depends on the current retryable behavior before changing it.
4. `Dead::fetch_bytes` gets the same treatment its sibling `transact` already has, for the reason
   already written at `testing.rs:90-97`.
5. The `fetch_bytes` capability hole is **closed or pinned**: either the wrapping engines forward
   / explicitly refuse it, or an assertion pins "whatever sits at `EngineSet.http` is
   binary-capable". Silence is the one unacceptable outcome. Note `MeteringHttpClient` is a
   *decorator* — a decorator that drops a capability is a distinct bug from an engine that lacks
   one; treat them separately.
6. Each fix is an extracted, tested unit per `.claude/CLAUDE.md`, with anti-pattern test names.

## Risks / non-goals
- Do **not** implement `transact` for engines that lack it, or implement `fetch_bytes` on the
  archive tier by inventing semantics. This direction is about the **contract** — making holes
  visible, typed and guarded.
- Do not restructure `EngineSet` or change trait method signatures unless a criterion forces it;
  every app in the workspace consumes these.
- `crates/core/tests/fetch_chokepoint.rs` and `llm_chokepoint.rs` hold EXPECTED inventories — if
  your change moves a call site, update them in the same commit.
- Not a coverage push for `engine-browser` (its `#[ignore]`d render tests are a real gap, banked
  on [[browser-engine]] — do not fix it here).

## Build record
(to fill during build)
