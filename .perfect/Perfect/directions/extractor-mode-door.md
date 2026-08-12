---
slug: extractor-mode-door
type: perfect/direction
context: "[[declarative-extractor]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: —
commit: —
---

## What & why
A params object can carry several mode roots at once — `replay`, `induce`, `rules`+`urls`,
`source` — and the extractor silently runs exactly ONE of them (replay > induce > rules,
and inside rules-mode source > urls) while returning 200. The schema's `anyOf` admits the
combinations its own prose calls "mutually exclusive", so the round-11 params door waves
them through. A caller who submits `{rules, urls, replay}` believes an extraction ran and
wrote records; nothing did but a read-only replay. Silent-wrong-result class — the job
completes green with a completely different effect than requested.

## Evidence
- `crates/apps/extractor/src/lib.rs:654-658` — schema `anyOf: [rules|replay|induce]`
  (anyOf permits any combination).
- `lib.rs:703`, `:738`, `:744` — three schema descriptions CLAIM mutual exclusion.
- `lib.rs:913-923` — replay then induce early-return, first-match-wins.
- `lib.rs:955-961` — `source` present silently beats `urls`.
- `parse_concurrency` `lib.rs:179-185` clamps `>= 1` only — no upper bound in code
  (fold: clamp to the schema's intended cap; schema currently declares no maximum either).
- docs/features/extraction.md documents "exactly one of urls|source" which code does not enforce.

## Acceptance criteria
- A named pure function (e.g. `resolve_run_mode(&params) -> Result<Mode, _>`) rejects any
  conflicting combination of mode roots with an error naming ALL conflicting keys; `run()`
  dispatches through it. Test named for the anti-pattern (e.g.
  `mode_conflict_rejected_not_first_match_win`), plus a case per pair.
- The params schema is tightened so the shared enqueue door 422s conflicting-mode jobs
  (JSON Schema `not`/`allOf` composition — verify the door's validator handles it; if the
  validator can't express it, the app-side check is the guard and the schema keeps prose).
- `concurrency` gets a code-side upper clamp with a schema `maximum` to match — one bound,
  both layers agree.
- `docs/features/extraction.md` documents the mode set and the enforced exclusivity.

## Risks / non-goals
- Risk: an existing caller relying on the silent precedence (e.g. always sending
  `source` alongside `urls`) starts getting 422s — that is the point; note it in the doc.
- Non-goal: changing any mode's behavior; this is purely the dispatch contract.

## Build record
(pending)
