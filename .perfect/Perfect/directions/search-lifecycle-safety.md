---
slug: search-lifecycle-safety
type: perfect/direction
context: "[[search-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: f4a7d1b
---

## What & why
On schema drift the engine `remove_dir_all`s the index dir BEFORE acquiring the writer lock
— a new-schema binary started while an old-schema server runs deletes the live index under
it (Unix) or fails boot (Windows). A corrupt-but-present meta.json fails both open and
create → server won't boot, while search.md claims "a lost/corrupt index dir rebuilds
empty". The wipe branch has zero tests.

## Evidence
- engine-search/lib.rs:200-217 (schema_is_current → remove_dir_all pre-lock; create fallback
  fails on existing dir)
- state.rs:256-260 (boot fails hard); search-backfill.rs:15-16 (server-stopped convention)
- docs/features/search.md:25 (wrong recovery claim)

## Acceptance criteria
- Wipe/rebuild happens only under the writer lock (or an equivalent file-lock guard) —
  concurrent old-schema server cannot lose its index silently; the contested case fails
  LOUDLY on the newcomer's side.
- Corrupt-but-present index dir: quarantine (rename aside) + recreate empty + error-level
  log — boot proceeds; the quarantined dir named for manual inspection.
- search.md recovery claims match reality.
- Tests: schema-drift wipe branch, corrupt-dir recovery, and the contested-lock case (as
  far as testable).

## Risks / non-goals
- Tantivy lock semantics constrain the design — builder documents what the lock can and
  cannot guarantee cross-process.
- Non-goal: multi-process index sharing.

## Build record
- Builder SE2 (opus), wave 2, verdict merge (pick pending DH2 gate). `2c850a2`:
  `open_or_recover` + `classify_open_failure`; drift → locked wipe; corrupt meta.json →
  quarantine to `<dir>.corrupt.<n>` (counter, deterministic) + boot proceeds; both
  destructive branches take the REAL Tantivy INDEX_WRITER_LOCK first and fail naming the
  conflict. Destructive step = `drain_dir` (empties in place, keeps the lock file).
- **Load-bearing refutation**: MmapDirectory overrides acquire_lock with a real OS lock —
  a present lock FILE means nothing (builder's first test faked a holder via the file and
  wiped the index); no stale-lock problem exists (kernel releases on crash — the "delete by
  hand" doc advice was wrong and removed); a held lock blocks dir rename on Windows, hence
  drain-in-place. Also: create_in_dir refuses only dirs WITH meta.json — first-boot and
  stale-file cases skip quarantine entirely.
- Honest: contested case tested in-process (per-open-file-description locks make it valid);
  two-process race and Unix flock semantics reasoned, not run (Windows box).
- Gates: worktree 1137/0 at this commit.
