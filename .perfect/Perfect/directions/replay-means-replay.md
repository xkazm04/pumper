---
slug: replay-means-replay
type: perfect/direction
context: "[[vcr-testing]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---

## What & why

`vcr.rs`'s module doc promises: *"Replay runs touch no engine, obey no politeness delay, and
spend $0."* The first clause is false, and the job's stored result claims otherwise.

`transact` calls `ctx.engines.browser.transact(req)` **raw** — not through the `AppContext::fetch`
chokepoint VCR hooks. A job enqueued with `replay_of: <id>` against the `transact` app therefore
launches a real Chrome session, navigates to the live site, and can submit a real form. No
cassette is consulted, no miss is raised, no marker is set — and then `worker.rs:759-761` stamps
`vcr_replay_of: <id>` into the stored result, because the stamp is applied unconditionally to any
job that carried the param.

That stamp is a provenance lie, and the doc comment beside it (*"a replayed job's output is
derived from recorded bytes, not the live web, and anyone reading it later must be able to tell"*)
states exactly the property being violated.

The blast radius is pinned but undisclosed. `crates/core/tests/fetch_chokepoint.rs` holds an
EXPECTED inventory of **32 raw-engine call sites** across the workspace — every one invisible to
record and live-executing on replay. The module doc's own limitations block names only *the
crawler*. `docs/features/runtime.md:103-112`, the operator's page for VCR, never mentions the
bypass at all.

**Director note on the scout's framing** (verified in source before gating): the brief claimed
these sites "meter real work during a replay run". That part is refuted — both raw meters pass
`0.0` (`transact/src/lib.rs:178`, `crawl/src/lib.rs:426`), so the "spend $0" clause holds. The
defect is engine contact and the false stamp, not cost. Build against the verified claim.

## Evidence

- `crates/core/src/vcr.rs:16-17` — "Replay runs touch no engine … (every metered seam records a
  `vcr_replay` cost event at 0.0)".
- `crates/core/src/vcr.rs:39-42` — the documented-limitations block, naming only the crawler.
- `crates/apps/transact/src/lib.rs:174` — `ctx.engines.browser.transact(req).await?`, raw, with no
  `ctx.vcr` consultation on any path.
- `crates/server/src/worker.rs:755-761` — the unconditional `vcr_replay_of` stamp and its doc.
- `crates/core/tests/fetch_chokepoint.rs:39-124` — the 32-row EXPECTED inventory; `:68-71` flags
  `hackernews::ctx.engines.http` as "Reviewed, not endorsed".
- `docs/features/runtime.md:103-112` — the operator VCR section; no bypass mention.

## Acceptance criteria

1. A job that asks to replay an app which **structurally cannot** replay does not silently run
   live under a replay stamp. Pick a lever and argue it:
   - **(a) Refuse at the door.** `Error::BadRequest` is already terminal-for-job and is
     semantically right here (client-supplied input the server understood and rejected), so this
     composes with `replay-miss-terminal` for free.
   - **(b) Stamp honestly** — the result declares partial/absent replay fidelity instead of a bare
     `vcr_replay_of`.
   - (a) is cleaner; (b) is weaker but non-breaking. **Check first whether any existing test, e2e
     or smoke check replays a bypassing app** — if one does, (a) breaks it and that fact is the
     design input, not an obstacle to route around.
2. Whatever classifies an app as replay-capable lives in **one place** and is **guarded**: a new
   app that drives engines raw cannot appear without that classification being a visible decision.
   The `EXPECTED_RAW_ENGINE_CALLS` inventory is the working precedent for the shape — read it, do
   not duplicate it. *(That file is Director-only this round: if you need it changed, report the
   exact edit.)*
3. `vcr.rs`'s module doc and `docs/features/runtime.md` both state what replay does and does not
   cover, naming the bypass class rather than one example of it. Do not write a contract the code
   does not implement — that failure mode has cost this repo a round before.
4. A test that fails today: a replay run against a raw-engine app must not reach the engine and
   claim a clean replay. Name it after the anti-pattern.
5. Report (do not apply) anything you need in `crates/server/src/registry.rs` or
   `crates/core/tests/fetch_chokepoint.rs`.

## Risks / non-goals

- **HARD WRITE-SET CONSTRAINT: do not edit any file under `crates/apps/`.** A sibling builder owns
  `docs/features/apps.md` this round, and every `crates/apps/**` edit drags that doc in through the
  doc-sync map. If your design genuinely requires an app-crate edit, **report it with the exact
  patch** and the Director applies it. This constraint is the wave's collision avoidance, not a
  style preference.
- **Non-goal**: converting `transact` or `crawl` to the `ctx.fetch` chokepoint. Both bypasses are
  reviewed and deliberate; the defect is that replay pretends they aren't.
- Hazard: the crawler's bypass IS disclosed today. Do not regress that disclosure while
  generalizing it.

## Build record

(filled during build)
