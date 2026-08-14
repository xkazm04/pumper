---
slug: reconciler-refuses-an-unregistered-app
type: perfect/direction
context: "[[data-pipeline-catalog]]"
lens: robustness
status: rejected
size: S
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## What & why

The catalog reconciler **never consults the app registry at runtime.** `reconcile_plan` reads
`src.app` as a plain string (`crates/core/src/catalog.rs:342-347`), so a `live` row with a typo'd
app name and a cron produces a `PlanCreate` (`:376-380`), and apply calls
`create_managed_schedule` (`crates/server/src/scheduler.rs:502`) — minting an **enabled schedule for
an app that does not exist**, which the scheduler then refuses every tick, forever.

The invariant is enforced **only at compile time**, by a test that reads an `include_str!` **copy**
of the TOML (`crates/server/src/routes/mod.rs:387, 436-503`). So `$PUMPER_CATALOG`
(`catalog.rs:266`) or any on-disk edit after build bypasses it entirely — and the catalog is a
GitOps surface explicitly designed to be edited.

**Second, smaller hole in the same function:** `desired.entry(app).or_insert(src)` (`:345`) — the
first live row per app wins, **silently**. Two live rows for one app with different crons: the
second's cron is dropped with no warning. `cordis` already has two live rows
(`catalog/data-sources.toml:696-705`, `:736-745`); their crons are identical today, so it is benign —
one edit from silent.

## Evidence

- `crates/core/src/catalog.rs:342-347` — app read as a string, registry never consulted.
- `:376-380` — the `PlanCreate` for an unregistered app.
- `crates/server/src/scheduler.rs:502` — apply mints the schedule.
- `crates/server/src/routes/mod.rs:387` — `include_str!`, i.e. a build-time copy, not the file the
  server loads.
- `crates/core/src/catalog.rs:266` — `$PUMPER_CATALOG` override.
- `:345` — the silent first-row-wins collapse; `catalog/data-sources.toml:696-705, 736-745` — the
  two `cordis` rows that make it live.

## What the same pass CONFIRMED as sound — recorded so a future round does not re-litigate it

The reconciler is the **best-built thing in its family**, and this is worth stating explicitly
because "reconciler" sounds like a risk surface:

- **The `managed_by` fence is airtight in both directions.** Untagged rows never appear in
  `update`/`disable`/`orphan` (`catalog.rs:319`, `:370-374`, test `:836-841`); a hand-made row with
  a drifting cron yields a `create` of a separate tagged row, never an `update` of the hand-made one
  (`:376-380`, test `:768-775`); every write is SQL-fenced (`scheduler.rs:502, 515, 533`) and a
  fence miss is reported as an error (`:523-526, 541-544`). **The reconciler cannot delete or
  overwrite a hand-made schedule row.**
- **Removal is conservative:** a source left `live` → `disable`, never delete (`:397-405`); no row
  at all → `orphan`, report-only (`:410-415`, test `:823-833`).
- **A missing catalog file is safe by design:** empty catalog → zero desired rows → every managed
  schedule becomes a report-only `orphan`, never a mass-disable.
- `auto_reconcile` defaults **false** (`crates/core/src/config.rs:196-200`); boot always plans and
  logs, and applies only on opt-in (`scheduler.rs:559-599`).

**One genuine fail-open worth banking separately:** malformed TOML at the worker's contract seam is
warn-and-skip (`crates/server/src/worker.rs:1430-1436`), so a single typo silently disables **every**
declared data contract fleet-wide behind one log line.

## Acceptance criteria (for whoever builds this)

1. A `live` row naming an unregistered app produces a refusal/`orphan`-style plan entry, not a
   `PlanCreate`, at **runtime**.
2. Two live rows for one app is a named plan warning, not a silent first-wins.
3. Malformed TOML at the worker contract seam is visible as more than a log line.

## Risks / non-goals

- `crates/core` must not depend on the server's registry — the check belongs at the apply seam or
  behind an injected predicate. That design question is why this is an S and not a triviality.

## Why REJECTED this round

Real gap, **low frequency**, and the compile-time test genuinely covers the committed TOML — which
is the only catalog in play on every deployment that does not set `$PUMPER_CATALOG`. Rejected on the
6-direction cap, having lost to three confirmed defects that are losing data or misleading users
*today* rather than on an edited-catalog path.

Banked for r23, paired with the `max_staleness_hours` expressiveness gap recorded in
[[connector-watch-failures-are-not-success]] — both are defects in what the **catalog can say**, so
they belong to one pass.

**Also noted, below the bar:** `Catalog::load()` does an fs read + full TOML parse at 5 call sites
including **every job completion** (`worker.rs:1430`), with no cached copy in `AppState`. Untidy,
measurably wasteful, not harmful — the kind of thing to fix while already in the file.
