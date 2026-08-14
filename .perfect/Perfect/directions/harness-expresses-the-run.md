---
slug: harness-expresses-the-run
type: perfect/direction
context: "[[vcr-testing]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-14
accepted: 2026-08-14
shipped: 2026-08-14
commit: b5d500e
---

## What & why

The repo's durable-execution seam is **untestable**. `AppContext.checkpoints` is an
`Arc<dyn CheckpointSink>`, the trait's own doc says `save` "returns `false` when the write did not
land … **so apps can count it**" — and there is exactly **one** implementation in the entire
workspace (`NoCheckpoints`, `crates/core/src/app.rs:55-59`) which returns `true` unconditionally.
`TestContext` hardcodes it (`crates/core/src/testing.rs:358`) and exposes no setter among its eight.

So no test anywhere can assert *that* an app checkpointed, *when* it checkpointed, *what* it
checkpointed, or *what the app does when a save fails*. Every durable-execution claim in this repo
— across 11 call sites in 9 apps — rests on code no test has ever observed. This direction is the
blocker two independent scouts (r19, r20) converged on, and it has been deferred twice; the r21
ledger flagged a third deferral as a ledger smell.

**The deferral reason is now dead.** It was deferred partly because `testing.rs` ripples into 39
files. Scoped as **purely additive** — one new builder setter, one new double, no signature change
— the ripple is exactly zero.

## Evidence

- `crates/core/src/app.rs:46-59` — `CheckpointSink::save(state, force) -> bool`; doc promises the
  `false` is there "so apps can count it". `NoCheckpoints::save` returns `true` always.
- `crates/core/src/testing.rs:261-272` — `TestContext`'s 10 fields; no `checkpoints`.
- `crates/core/src/testing.rs:358` — `checkpoints: Arc::new(NoCheckpoints)`, hardcoded in `build()`.
- `grep -rn "impl CheckpointSink" crates/` → **one hit**, the no-op. Zero doubles workspace-wide.
- `crates/server/src/progress.rs:123-153` — the real sink, `JobCheckpointer`, whose two genuine
  `false` returns (stale attempt lineage; storage error) no test can reach through `TestContext`.
- Precedent for the shape: `ScriptedResearcher` (`testing.rs:115-218`) already does exactly this
  for the `Researcher` seam — records every call, replays scripted answers *and* scripted failures,
  and exposes `calls()`/`call_count()`. This direction is that pattern applied to `CheckpointSink`.

## Acceptance criteria

1. `TestContext` gains a `checkpoints(Arc<dyn CheckpointSink>)` setter. **Purely additive**: no
   existing field, signature or default changes, and **not one** of the 39 files that construct a
   `TestContext` today may need an edit. If your change forces an edit outside your write set, the
   design is wrong — stop and report, do not fix the caller.
2. A `RecordingCheckpoints` double lands in `crates/core/src/testing.rs`, modelled on
   `ScriptedResearcher`: it records every `(state, force)` pair in order, exposes them
   (`saves()`, `save_count()`, and the last state), and can be configured to **fail** — returning
   `false` always, or from the Nth save on — so the counting path the trait doc promises is
   reachable from a test for the first time.
3. **Ship a test that is structurally impossible today.** At minimum one that asserts an app
   checkpointed *before* doing the work it is meant to be able to resume, and one that drives the
   `false` return. A test that merely re-asserts something already provable is not acceptance —
   this is the criterion that kept this direction out of two prior slates.
4. `NoCheckpoints` keeps its current behavior and stays the default, so every existing test is
   byte-for-byte unaffected. The full `cargo test --workspace` count must not drop.
5. `docs/features/runtime.md` documents the checkpoint seam's failure contract and the harness
   that now exercises it — today the doc mentions checkpoints only in passing (`:27`, `:109-112`)
   and never states what a failed save means.

## Risks / non-goals

- **Non-goal:** a canonical `CannedHttp` or any wider harness generalization. The r19 framing
  ("TestContext has 8 setters against 18 AppContext fields") invites a sweep; a sweep is exactly
  the consistency polish the taste filter rejects. Only the checkpoint seam, because only it has a
  live correctness bug waiting on it.
- **Risk:** `pumper-core`'s `testing` module is feature-gated (`test-support`) and consumed via
  dev-dependencies. Adding a public type there is safe, but the module ships panicking stubs — keep
  `RecordingCheckpoints` non-panicking and honest.

## Build record

(filled during build)
