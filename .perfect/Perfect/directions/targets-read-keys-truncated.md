---
slug: targets-read-keys-truncated
type: perfect/direction
context: "[[declarative-extractor]]"
lens: robustness
status: proposed
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## What & why

**`keys_truncated` is emitted, warned about, and unforgeable — and no app reads it yet.**

r23's [[trigger-scope-is-host-owned]] made the host honest: `capped_keys` now returns
`(keys, truncated)`, `dataset_trigger_obj` always emits `keys_truncated`, and the value is
host-owned so a transform plugin cannot forge or delete it. The fire path warns when it is true.

But the two consumers that read `_trigger.keys` as their work list — `crates/apps/extractor` and
`crates/apps/plugin` — still do not look at the flag. So a hop whose delta exceeded
`[triggers] key_cap` (default 200) processes the first 200 records and **reports a clean run**. The
truncation is declared by the host and ignored by the target, which is one step better than r23's
starting state (declared nowhere) but is not yet the honest end-to-end story.

This is the direct follow-up Lot A raised as `DECISION NEEDED` rather than reaching outside its
write set — correctly, since both apps belonged to no lot this round.

## Evidence

- `crates/server/src/triggers.rs` — `capped_keys`, `HOST_OWNED_KEYS` (`keys`, `keys_truncated`),
  and the fire-path warning, all shipped in `fd88138`.
- `crates/apps/extractor/src/lib.rs:1531-1532` — reads `/_trigger/keys`, never `/_trigger/keys_truncated`.
- `crates/apps/plugin/src/lib.rs:1062-1063` — byte-identical.
- The idiom to layer on already exists in both apps' own result shapes: the `truncated` /
  `sweep_truncated` honesty fields (same doctrine as `ca-grants`, `census-density`, and cordis's
  `aggregate_truncated`).

## Acceptance criteria (for whoever builds this)

1. A hop whose key list was capped produces a result that **says so** — the run reports partial
   rather than clean, using each app's existing `truncated`-style field rather than a new vocabulary.
2. A hop that was NOT truncated is unchanged, byte for byte.
3. A test per app proving a capped hop is distinguishable from a complete one, named after the
   anti-pattern.
4. `docs/features/apps.md`'s extractor/plugin rows and `docs/features/triggers.md` say what the
   field means to a target.

## Risks / non-goals

- **Non-goal:** changing `key_cap` or the truncation itself. The host's behavior is correct and
  now declared; this is purely about the target believing it.
- **Non-goal:** the host side. `crates/server/src/triggers.rs` is done.
- Small and mechanical — `S`. Its value is that it closes the loop r23 opened; leaving it open means
  the honest flag exists and nothing acts on it, which is the shape that rots.

## Status

**Banked r23**, raised by Lot A as a `DECISION NEEDED` it declined to answer from outside its write
set. Recommended as an early r24 slate item: it is cheap, it finishes shipped work, and the two
apps are a natural single lot.
