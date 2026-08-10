---
slug: datahub-governance-reversible
type: perfect/direction
context: "[[datahub-bridge]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 93997ed
---

## What & why
Three sharp edges: (1) an operator re-enabling a schedule is re-disabled within ≤300s while
the remote deprecation flag stands — no override, governance acts on LEVELS not
transitions; (2) one deprecated dataset disables ALL the app's catalog schedules, one
cost:pause tag zeroes every job's budget — dataset-level signal, app-level blast; (3) the
pause set FREEZES during a DataHub outage (poll aborts before recompute) — an app paused
pre-outage stays $0 indefinitely despite the code's fail-open claim.

## Evidence
- `datahub.rs:1007-1030` (disable), routes/schedules.rs:204-218 (re-enable loses)
- `:887-897 → :799-809` (app-wide pause), `:958-970` (abort-before-recompute freeze),
  `:786-787` (fail-open claim that only holds across restart)

## Acceptance criteria
- Governance acts on TRANSITIONS: a manual re-enable is respected until the remote state
  changes (track last-acted remote state per target; extracted + tested).
- Pause-set staleness policy on outage: after N failed polls / T minutes, pauses expire
  loudly (or the choice is documented if kept) — the indefinite-freeze case is dead.
- Blast radius documented in code + docs; scoped tighter where cheap (builder proposes).
- Tests for all three edges (mock-HTTP where the poll is involved).

## Risks / non-goals
- Transition semantics must not resurrect a schedule the remote STILL wants off on
  restart (persisted last-acted state, coordinates with the audit trail from
  [[datahub-governance-preview]] — sequenced after it).

## Build record
- Builder DH2 (opus), wave 2, verdict merge (pick pending P2 gate). `c07bf5b`: transitions
  via migration 0038 `datahub_govern_levels` — SEPARATE from the age-pruned audit table
  (pruning the memory would resurrect the flap; bounded by signals×apps, needs no
  retention). `level_transitions` pure (act on false→true only, write on change only);
  fail-closed on unreadable memory; un-deprecating re-arms without re-enabling.
  Invariant test written first: `a_restart_does_not_reflap_an_unchanged_deprecation` +
  e2e restart simulation. Outage expiry: `govern_pause_max_stale_secs` (default 900, 0 =
  old freeze) — blind enforcement expires loudly (warn + expire_pause audit row + event);
  status exposes secs_since_successful_poll. Builder's judgment call (expiry fail-open on
  spend, restart-consistent, opt-out preserved) — endorsed.
- Out-of-scope touches all forced + flagged (config, EXPECTED_TABLES, justfile/CLAUDE.md).
- DH2 also found: record_govern was overwritten by FAILED polls too — even the last
  successful summary was lost on the next outage tick.
- Gates: worktree 1143/0/17, 114 suites.
