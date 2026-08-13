---
slug: doc-sync-hook-fires
type: perfect/direction
context: "[[maintenance-tooling]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---
## What & why
`.claude/CLAUDE.md` devotes its longest section to the **Documentation Sync** rule and calls it
*"per-session gap-prevention, not periodic catch-up"*, enforced by a Stop hook. `CLAUDE.md`
repeats it. That hook has **never fired — not once, in the entire life of the file.**

`scripts/docs/check-doc-sync.mjs:84` walks the transcript backwards and breaks on the first
`evt.type === 'user' && evt.message?.role === 'user'`, intending "stop at the user's message".
But Claude Code records **tool results** in exactly that shape — `type: 'user'`,
`message.role: 'user'`, carrying a `toolUseResult` key the script never checks. Since a turn's
last transcript entries are almost always tool results, the backward scan breaks immediately, the
edited-file set is empty, and `main()` exits 0 at line 103.

**Measured, not inferred.** I replayed the script's exact algorithm over this session's own
transcript: 58 `type:'user'` entries, of which **55 are tool results and 3 are genuine user
messages**; the file contains 3 `Edit`/`Write` calls; the hook's scan window saw **0**. The
scout independently replayed it across all 31 transcripts in this project — **1,128
`Edit`/`Write`/`MultiEdit` calls, 26 transcripts containing edits, zero detections in every
single one.**

So the repo's entire same-session doc-drift defense is inert, and `feature-doc-map.json`
(127 lines, 17 entries) has never been consulted for a real decision. This is the exact defect
class this repo names as highest-value — a documented promise with no implementing code — except
the promise is about the repo's own quality mechanism, which makes every other doc-drift finding
downstream of it. Two of this round's other five directions exist because docs drifted from code
unchecked.

The user moment: *"CLAUDE.md told me a hook would catch it if I changed an endpoint without
updating its doc. It never would have."*

## Evidence (Director-verified by replaying the algorithm, not by reading alone)
- `scripts/docs/check-doc-sync.mjs:81-96` — the backward walk; `:84` the faulty break condition;
  `:103` the `edited.size === 0 → exit 0` path.
- `.claude/settings.json` — registers it as the `Stop` hook.
- Replay on `~/.claude/projects/C--Users-mkdol-dolla-pumper/<this session>.jsonl`:
  58 `type:user`/`role:user` entries → 55 with a `toolUseResult` key, 3 genuine; 3 edit tool_uses
  in the file; **hook scan window: 0 edits**.
- `.github/workflows/ci.yml` — CI does not run it either, so the hook is the only path.
- `scripts/docs/feature-doc-map.json:3` — two glob groups are already self-documented `_inert`
  because `SKIP_PATTERNS` (`mjs:31,33`) is applied before the map lookup.
- `crates/server/src/bin/reindex.rs` has **no** entry in the map at all
  (`search-backfill.rs` does, at line 72) — so even a working hook would never couple it.

## Acceptance criteria
1. The hook **detects edits in a real turn**. The fix is to stop treating tool results as the
   user boundary — skip entries carrying `toolUseResult`, or find the boundary by session/prompt
   id instead of by `type`. Pick the one that survives a transcript-format change and say why.
2. **Prove it against real transcript fixtures**, not a hand-built object: check in a small
   redacted fixture (or generate one from the recorded shape) covering (a) a turn with edits
   after tool results — must detect; (b) a turn with no edits — must stay silent; (c) the
   `stop_hook_active` re-entry guard — must stay silent. A test is mandatory here: this script
   failed silently for its entire life precisely because nothing exercised it.
3. It is **runnable and provable outside the hook** — a `just doc-sync` recipe (the justfile is
   the canonical task runner per `CLAUDE.md`), so the next person can verify it in one command
   instead of inferring from a session that did not nag.
4. **Calibrate before declaring victory.** A hook that has never fired will, once fixed, start
   firing — possibly constantly. Run it against several real recent transcripts and report the
   hit rate. If it would fire on nearly every turn, the map or the skip patterns need tuning as
   part of this direction; a nag that is always wrong gets ignored and is worth less than nothing.
5. Fix the map gaps this exposes, at minimum an entry for `crates/server/src/bin/reindex.rs`.
   Coordinate with [[onboarding-compiles]] — that direction owns adding the `ONBOARDING.md`
   target entry; **you own the script and the recipe**, it owns that one map row.
6. `.claude/CLAUDE.md`'s description matches what the hook now actually does.

## Risks / non-goals
- Do **not** make the hook block or fail a turn. It exits 2 with a reminder by design; keep that.
- Do not expand the map's coverage broadly in this direction — a bigger map plus a
  newly-working hook is two variables at once. Fix the mechanism, close the named gaps, tune only
  as far as criterion 4 requires.
- Node script + justfile + fixtures. No Rust.

## Build record
(to fill during build)
