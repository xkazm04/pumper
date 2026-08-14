---
slug: targets-read-keys-truncated
type: perfect/direction
context: "[[declarative-extractor]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-14
accepted: 2026-08-14
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

---

## r24 re-verification (2026-08-14) — CONFIRMED, and SHARPER than banked

**Host emission is universal.** `crates/server/src/triggers.rs:141-143` inserts `keys_truncated`
unconditionally in `dataset_trigger_obj`'s `json!` literal; the only two production callers
(`triggers.rs:1057` fire path, `routes/triggers.rs:404` dry-run) both route through it. `capped_keys`
(`:101-105`) is the workspace's only truncation site and returns `(keys, truncated)` as one tuple —
**the cap cannot be applied without the flag being computed.** No unflagged path exists.

**Readers: still zero.** Workspace grep for `keys_truncated` hits only `triggers.rs`, one e2e test,
`plugins-src/delta-slim` (a fixture the host overwrites), docs, and `.perfect/`. The only two
`_trigger` readers in `crates/apps/**` are `extractor/src/lib.rs:1532` and `plugin/src/lib.rs:1063`,
and they read `/_trigger/keys` and nothing else — not even `_trigger.count`.

### The sharpening that upgrades this from S to M

**Neither app merely ignores the flag — each emits a positive `truncated: false` that is FALSE on a
capped hop.**

- `extractor/src/lib.rs:1562` `let mut truncated = false;` → set only at `:1578`, inside the
  **no-keys sweep** branch. The trigger-keys branch (`:1568-1570`) leaves it false → emitted `:1694`.
- `plugin/src/lib.rs:1097` identical → set only at `:1111` → emitted `:1213`.

And **three artifacts assert the now-false invariant** and must move together:
- `plugin/src/lib.rs:1095-1096` — comment: *"`truncated` is always false when the caller named the
  key set: no cap applied to it."*
- `docs/features/extraction.md:104` — the **published contract**, same sentence.
- `docs/features/triggers.md:11` — says `keys_truncated` exists so the target need not infer
  truncation. **The two docs now contradict each other on the trigger path.**

**The seam:** `explicit_keys` (`extractor:1531-1532`, `plugin:1062-1063`) collapses two provenances
into one `Option<Vec<String>>` via `.or_else()` — caller-supplied `source.keys` (genuinely uncapped)
and host-supplied `_trigger.keys` (capped at `key_cap`). **The fix must preserve which one it got;
today `.or_else()` destroys exactly that information.** That, not the missing read, is the real work.

**Field to layer on: `truncated`** (siblings `requested`, `limit`, `missing`, `missing_keys`).
**Do NOT reuse `records_truncated`** (`extractor:796`) — that bounds the result's record *echo*, a
different axis.

**ACCEPTED r24.** Gate: director-self-gated (autonomous, Athena-dispatched).
