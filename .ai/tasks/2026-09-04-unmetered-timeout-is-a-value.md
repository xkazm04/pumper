# Task — an unmetered timeout is recorded as a value, not as an absence

- **Opened:** 2026-09-04 by registry run `pi-2026-09-04`
- **Registry subject:** `software-engineering/llm-agent/runtime-and-io/agent-runtime-assembly`
- **Technique:** `indeterminate-closure-on-interruption`, amended this run
- **Mode:** `task` (larger than a few readable lines; see Size)
- **Status:** proposed, first step not taken

## What the tree already gets right, and why this is narrow

`crates/core/src/error.rs:36-56` is not careless. It names the anti-pattern in
its own doc comment — "the anti-pattern is one silent `$0` that reads
identically to 'no call was made'" — distinguishes three cases with typed
classes rather than strings, and has a test at `:620-632` asserting that the
cost travels in a field and is not smuggled into the message. The engine's
non-zero-exit path at `crates/engine-claude/src/lib.rs:450-465` was already
fixed once for exactly this family: its comment records that discarding stdout
on a non-zero exit "threw that spend away", and it now recovers
`total_cost_usd` from an envelope printed before a failing exit.

So this task is not "the ledger lies about cost". It is two residues that the
tree's own reasoning implies and has not yet reached.

## Residue 1 — the unknown is a value, and the unknown-ness is a string

`crates/core/src/error.rs:52`

```rust
(None, ClaudeFailure::Timeout) => Some((0.0, "unmetered_timeout".to_string())),
```

The amount is `0.0` — a number an aggregator can sum — and the only thing
marking it as *not actually zero* is the `detail` string beside it. Every
consumer that totals `cost_usd` therefore understates spend by the whole
timeout population unless it independently knows to special-case one string
literal, and the first reword of that literal silently reclassifies history.

This is the same defect the file's own test guards one level up. That test
asserts the cost is a field rather than a message; here the *unknown-ness* is
the message. The registry rule is that an absence may not be rendered as a
value, and `0.0` is a value.

The fix is a representation change, not a policy change — the policy (write a
row, do not stay silent) is already right:

- make the ledger event's amount optional, or add a typed `metered: bool` /
  `Metering::{Reported, Unmetered, NotRun}` beside it, so a summing consumer
  must opt into treating an unmetered row as zero;
- keep `unmetered_timeout` as the human-readable detail, but stop letting it
  be the only carrier of the fact.

## Residue 2 — one of the two timeout branches follows an *exited* CLI

`crates/engine-claude/src/lib.rs:403-435`

`RunScope::abandon` (`:608-619`) states its premise plainly: "a killed run
produces no envelope, so what it spent is unknowable here." That is true of the
**wait** timeout at `:405-413` — the CLI is still running and has printed
nothing final.

It is not true of the **drain** timeout at `:424-434`. That branch is reached
only *after* `scope.wait()` returned a status, i.e. after the CLI process has
already exited; the drain hangs because an orphaned grandchild is still holding
the stdout pipe open. In that state the CLI has very likely already printed its
complete envelope — the exact situation the sibling branch 25 lines below was
fixed to stop discarding — and the bytes are thrown away, because
`abandon()` calls `abort_tasks()` and the `stdout_task`'s `buf` is a local that
dies with the aborted task.

The fix is to make the accumulated bytes survive an abort: read into a shared
buffer (`Arc<Mutex<Vec<u8>>>`, or a channel the parent drains) instead of a
task-local `Vec`, and on the drain timeout parse whatever arrived for
`total_cost_usd` before abandoning — falling back to `Unmetered` when it is
absent or unparseable.

## The measurable

**The count of ledger rows whose spend is unknown but recorded as a summable
zero, in a fixed replay of the engine's failure paths.** Today every timeout
contributes one. After residue 1, zero do — they carry an explicit unmetered
marker no aggregator can silently add up. After residue 2, the drain-timeout
subset contributes a real amount instead of an unmetered marker.

Second measurable, cheaper: **`cost_events` total over a replayed run set, with
and without the change.** The delta is the spend the ledger is currently
understating.

## The gate that will see it

`crates/core/tests/llm_chokepoint.rs` already asserts the current behaviour at
`:406` (`assert_eq!(events[0].detail.as_deref(), Some("unmetered_timeout"))`),
so that test **must be updated in the same change** — it is the test that pins
the string as the carrier. Add beside it:

- a case asserting an unmetered row cannot be summed as zero by the ordinary
  path (whatever shape residue 1 lands in);
- a drain-timeout case with a pre-written envelope in the pipe, asserting the
  cost is recovered rather than abandoned.

`cargo test -p pumper-core` is the cheapest gate that sees residue 1;
residue 2 needs the engine crate's own suite.

## Size

- Residue 1: 2 files (`crates/core/src/error.rs`, `crates/core/tests/llm_chokepoint.rs`)
  plus every `ledger_event()` consumer — grep first; estimated 3-5 files,
  40-80 lines.
- Residue 2: 1 file (`crates/engine-claude/src/lib.rs`), ~30 lines, plus a test.

Residue 1 is worth doing alone. Residue 2 depends on residue 1's vocabulary
existing (it needs somewhere to put "recovered after a drain timeout") and
should follow it.

## Falsifier — run on 2026-09-04, did NOT fire

If every consumer of `ledger_event()` already branched on `detail` before
summing, residue 1 would be cosmetic and this task would close as
`not-better`. It does not.

`ledger_event()` has exactly **one** production consumer —
`crates/core/src/app.rs:338-343`, `meter_failed_spend`:

```rust
let Some((cost, detail)) = e.claude_spend().and_then(|s| s.ledger_event()) else {
    return;
};
self.meter("claude", url, cost, Some(&detail)).await;
```

`cost` goes straight into the meter as an amount and `detail` rides along as an
opaque annotation. Nothing between here and the ledger inspects the string. The
other five hits are the unit tests in `error.rs` and one `.is_some()` filter in
`fetcher.rs:1282` that never reads either field. So a timeout does write a
summable `0.0` into `cost_events`, and any total over that column is understated
by the whole timeout population. Residue 1 is real.

This also **shrinks the size estimate**: one consumer, not "3-5 files". Residue
1 is `crates/core/src/error.rs`, `crates/core/src/app.rs`,
`crates/core/tests/llm_chokepoint.rs` — three files.

## First step

Not taken. `git status` on `master` at the time this was written showed
uncommitted work in `crates/server/src/main.rs`, `MEMORY.md` and
`.ai/registry-map.json` belonging to another session, so this task's branch was
not opened — the registry's cross-repo rule is that a tree holding a sibling's
in-flight work gets a branch or gets left alone, and the branch would still have
had to be reviewed against that work. Open it as
`direction/unmetered-timeout-metering` when that tree is clean. The consumer
grep above is done; start at `error.rs:52`.
