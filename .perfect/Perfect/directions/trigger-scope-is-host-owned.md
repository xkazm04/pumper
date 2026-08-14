---
slug: trigger-scope-is-host-owned
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
shipped: —
commit: —
---

## What & why

**The transform hook can silently rescope the target job's WORK, not just its payload — and the
configuration the docs tell operators to write turns a 3-record incremental extract into a
10,000-record full sweep.**

A transform plugin's output *is* the target job's params. Traced end to end:
`restamp_provenance` (`triggers.rs:195-211`) merges the plugin's output and re-stamps
`PROVENANCE_KEYS` — which is `trigger_id, source_kind, source_job_id, event_id, source_id, depth,
chain` (`:182-190`). **`keys` is not in that list**, so the sandbox owns it outright. The shaped
object becomes `verdict.obj` (`:1262`), goes through `merged_params` into `params["_trigger"]`
(`:1278`, `:84-91`), and is handed to `enqueue_dedup` (`:1295`).

And the target reads `_trigger.keys` as its **work scope**:
`extractor/src/lib.rs:1531-1532` — `str_array(source.get("keys")).or_else(|| str_array(ctx.params.pointer("/_trigger/keys")))`,
documented at `:1522-1523` as the key-precedence chain, consumed at `:1568-1570`.
`plugin/src/lib.rs:1062-1063` is byte-identical.

Two failure modes, and the second is the dangerous one:

1. **`max_keys` narrows silently.** Default 10 (`delta-slim/src/lib.rs:67-77`) against the host's
   `key_cap` of 200 — a 200-key hop extracts 10 records.
2. **`keep` mode *widens* catastrophically.** If `keys` is absent from the keep-list it vanishes
   entirely, the extractor's `.or_else(…)` yields `None`, and it falls through to
   `extractor/src/lib.rs:1571-1572` — *"No keys: every live (not removed, not gone) record — up to
   limit"* — bounded only by `SOURCE_LIST_LIMIT = 10_000`.

**And that is the documented example.** `docs/features/trigger-plugins.md:50-68` is one JSON block
pairing `"target_app": "extractor"` (`:55`) with
`"transform": {"plugin": "delta-slim", "params": {"keep": ["dataset", "count"]}}` (`:62-65`). The
same config is the first case in the shipped-plugin test, and `e2e/trigger_plugins.rs:696` asserts
`assert!(out.get("keys").is_none(), "keys were not kept")` — treating the disappearance of the work
scope as the desired outcome.

The vocabulary encodes the wrong model too: `:701` says *"the key list is capped, nothing else is
dropped"* and `:707-710` calls `keys` a **"sample"** whose shrinking is fine because *"count stays
exact"*. For an `extractor` or `plugin` target, `keys` is not a sample — it is the work list.

## Evidence

- `crates/server/src/triggers.rs:182-190` — `PROVENANCE_KEYS`, without `keys`.
- `:195-211` `restamp_provenance`; `:1262`, `:1278`, `:84-91`, `:1295` — the path to enqueue.
- `plugins-src/delta-slim/src/lib.rs:57-66` (`keep` drops everything unlisted), `:67-77`
  (`max_keys` default 10), `:79` (`slimmed: true`).
- `crates/apps/extractor/src/lib.rs:1531-1532`, `:1568-1572`, `:99-102`/`:111-117`
  (`SOURCE_LIST_LIMIT = 10_000`); `crates/apps/plugin/src/lib.rs:1062-1063`.
- `docs/features/trigger-plugins.md:50-68` — the example config that causes it.
- `crates/server/src/e2e/trigger_plugins.rs:696`, `:701`, `:707-710` — the assertions and the
  "sample" vocabulary.
- **Pre-existing sibling, do not miss it:** the host already truncates the same field —
  `triggers.rs:111-115` `revs.iter().take(cfg.key_cap)`, doc'd `:93-94` as *"Keys are capped at
  `cfg.key_cap`; `count` stays exact — targets fetch full data by key."* Same framing at
  `crates/core/src/config.rs:605`. **A 201-key change silently loses record #201 today, with no
  plugin involved at all.**
- **No test anywhere asserts downstream job scope after a transform.** Every `keys` assertion in the
  file (`:196`, `:696`, `:706`) is hook-level, on `hook_obj`, with no job enqueued.

## Acceptance criteria

1. A transform plugin **cannot** change the target job's work scope. The natural fix is to make
   `keys` host-owned by adding it to `PROVENANCE_KEYS` — decide and justify, but the outcome must be
   that a sandbox cannot narrow or delete the work list.
2. The pre-existing host-side truncation is made honest: when `key_cap` truncates, the target must
   not silently process a subset believing it has the whole delta. Either the target learns the list
   is partial, or the host stops passing a truncated list as if it were complete. **Fix the doctrine,
   not only the plugin** — otherwise a 201-key change still loses record #201.
3. **A test fires a real hop into a target and asserts what `params._trigger.keys` the enqueued job
   actually receives** — the assertion class that does not exist anywhere today. Hook-level
   assertions do not satisfy this criterion.
4. `docs/features/trigger-plugins.md:50-68`'s example no longer instructs operators into a
   10,000-record sweep. (Doc edits are **Class C — report them, do not make them.**)
5. The `"sample"` vocabulary at `e2e/trigger_plugins.rs:701`, `:707-710` is corrected to name what
   `keys` actually is.

## Risks / non-goals

- **Non-goal:** removing `delta-slim`'s payload-shaping. Shrinking what a *webhook* carries is
  legitimate; shrinking what a *job does* is not. Keep the former.
- **Risk:** protecting `keys` changes behavior for anyone relying on `max_keys` to throttle. That
  reliance is not a supported configuration — the throttle is `key_cap`, and a plugin silently
  halving a job's work is the defect. Say so in the commit message.
- Coordinate with [[shipped-plugins-are-verified]]: the shipped-plugin tests are `#[ignore]`d today.

## Build record

(filled during build)
