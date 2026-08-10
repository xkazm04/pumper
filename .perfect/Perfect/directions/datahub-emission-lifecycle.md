---
slug: datahub-emission-lifecycle
type: perfect/direction
context: "[[datahub-bridge]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 27c5131
---

## What & why
Every successful job bare-`tokio::spawn`s an emission: no shutdown token (drain silently
drops in-flight emissions), no retry, no overlap guard on the fully serial re-entrant
full_sync, and a single-slot `datahub_last` status where a success overwrites a failure
seconds later — a failing bridge is structurally invisible.

## Evidence
- `datahub.rs:558-658` (bare spawn, no handle/token), `:314-331` (first-error abort,
  partial batch), `:336-347` (single-slot status), `:663` (serial re-entrant full_sync)
- docs/features/datahub.md:32,34 (admits no-retry + the race)

## Acceptance criteria
- Emissions shutdown-aware (child token / tracked task) — drain waits bounded or cancels
  loudly, never silently drops.
- `full_sync` overlap-guarded: concurrent POST → 409 or queued (builder picks, documents).
- Status: last_error kept separately from last_success + monotonic counters
  (emissions_ok/failed) so a flapping bridge is visible.
- Mock-HTTP tests: batching, partial failure, overlap guard.

## Risks / non-goals
- Non-goal: retry policy (explicitly deferred; next-run self-heal is the design — say so
  in status docs).

## Build record
- Builder DH1 (opus), wave 1 → master `27c5131` (gate in flight at write). Emission moved
  onto the worker's existing FanoutPool (round-1 drain idiom — builder correctly LAYERED on
  it rather than inventing a tracker; the bare spawn was nested inside a fanout unit).
  SyncGuard (drop-released) → concurrent full_sync = 409. EmissionStatus keeps last_error
  separate + ok/failed counters + sync_running; last_emission kept as back-compat mirror.
  Partial-abort errors now say how many entities were already ingested. First non-pure
  tests in the module: MockGms = loopback axum server (NO new dependency — none exists in
  the workspace; follows fetch_proxy idiom). 4 mock tests + 3 pure.
- Honest: refined rather than closed the doc's "not concurrency-safe" lineage note —
  overlap guard fixes sync-vs-sync only, not writer-vs-writer; doc says so.
- Gates: worktree 1098/0.
