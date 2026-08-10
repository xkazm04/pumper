---
slug: pumper-smoke-harness
type: perfect/direction
context: "[[trigger-pipeline]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: c4f3766
---

> Cross-context (repo harness). Filed under trigger-pipeline's round because it was gated
> there; it serves every context.

## What & why
Eight builders across rounds 4–5 all reported the same honest limitation: no live server run,
ever. Four shipped surfaces are proven only at test-harness level: `GET /enforcement/preview`,
`GET /datasets/doctor`, the federal `harvestDetails` stage on a real scheduled run, and the
`@q` divert path. Build the pumper-shaped smoke loop: one command that boots the server on a
scratch config + port, drives one real job end-to-end, curls the shipped endpoints, and
tears down — then use it to back-verify rounds 4–5 and as the standing `/perfect smoke` for
this repo.

## Evidence
- Session notes 2026-08-03 + 2026-08-04 "Carried forward — honestly unverified"
- config.md Skill improvement log 2026-08-03/-04: "live-verification gap is the loop's
  biggest structural weakness"
- `justfile` doctor/retention-preview/enforcement-preview targets require a RUNNING server —
  no target starts one on scratch state

## Acceptance criteria
- `just smoke` (or equiv script): scratch storage dir + scratch config + non-default port →
  boot `pumper` → wait for ready → enqueue + complete one real job → curl a checklist of
  endpoints (health, doctor, enforcement/preview, retention/preview, the job's receipt) →
  assert 200s + sane shapes → teardown (kill process, remove scratch state). Nonzero exit on
  any failure.
- Works on Windows (this box) — PowerShell or cross-platform script, no bashisms that break.
- The four waiting surfaces from rounds 4–5 verified live; results recorded in the vault.
- Recipe recorded in `.perfect/Perfect/config.md` so every future build phase runs it.
- Doc-sync: smoke target documented (justfile table in CLAUDE.md + relevant docs page).

## Risks / non-goals
- The federal harvest stage hits a live external API — the smoke must tolerate network
  absence (skip-with-notice, not fail) for external-dependent checks.
- Non-goal: full e2e regression suite; this is a boot-and-poke loop, minutes not hours.

## Build record
- Builder S1 (sonnet), wave 1. Shipped `scripts/smoke.ps1` (382 lines) + `just smoke` +
  CLAUDE.md table row + http-api.md "Smoke verification" section. Ran twice: **11 PASS /
  0 FAIL / 0 SKIP** both runs, exit 0, no stray processes or scratch dirs. First-ever live
  verification of `GET /enforcement/preview`, `GET /datasets/doctor`, `GET /jobs/{id}/receipt`
  (driven by a real hackernews job — the only credential-free app; no network-free app exists,
  so network checks SKIP not FAIL). Kill logic scoped by scratch-path command-line match.
  Honest gaps: federal `harvestDetails` stage needs a real grants.gov run (not exercised);
  `enforce=true` behavior itself unexercised (preview only).
- Builder refuted: no root `/` route (only /health, which lives in meta.rs — routes/health.rs
  is extraction-health despite the name); enqueue is per-app `POST /apps/{name}/jobs`; no
  network-free app exists in the repo.
- Director review: teardown/kill safety verified in diff; merge verdict on first review.
  Cherry-picked → master `c4f3766` (docs/scripts only, no Rust — full-suite gate deferred to
  the next code-carrying merge).
