---
slug: unified-health-inheritance
type: perfect/direction
context: "[[grants-unified-layer]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 916a38e
---
## What & why
`sync_unified` writes straight into `grants/unified` with `upsert_many_stamped(UNIFIED_APP, …)`,
bypassing `AppContext::write_target` — which only ever gates writes keyed to the SOURCE's own
app/dataset. So a quarantined or degrading `ca-grants` run still writes fresh, untrust-stamped rows
into the canonical layer every consumer reads. Worse, the search-index health gate on the virtual
pair `("grants","unified")` is structurally a no-op: no `observe_extraction` ever runs for it, so
`enforced_state` always resolves `Healthy`. The gate exists and can never fire.

## Evidence
- Bypass: `crates/apps/grants-common/src/lib.rs:309-323` (`sync_unified` → `upsert_many_stamped`).
- What it skips: `crates/core/src/app.rs:542-553,635-648` (`write_target` = `@q` + trust stamp).
- No-op index gate: `crates/server/src/worker.rs:1452-1459` against
  `crates/core/src/resilience/store.rs:673-678`.
- Producers use plain `upsert_many_with_provenance`, never `sync_many` — so the `RemovalGuard` work
  from round 4 is inert for this vertical (`ca-grants/src/lib.rs:205-214`); `sweep_closed` exists
  precisely because these are upsert-only sources (`grants-common/src/lib.rs:517-519`).

## Acceptance criteria
- A degrading/quarantined source's contribution to `grants/unified` is trust-stamped or diverted —
  never silently canonical.
- The design decision is explicit and documented: one source's verdict must NOT quarantine the whole
  shared dataset, so the unit of gating is the contribution, not the dataset.
- The index gate on the virtual pair either genuinely works or is honestly removed — not left as
  decoration.
- Test: a quarantined source's rows are distinguishable from healthy ones in the unified layer.
- Consumers reading `grants/unified` can still filter to trusted rows with the existing trust
  predicate (no new vocabulary).

## Risks / non-goals
Not a redesign of the unified schema. Do not make one bad source hide every other source's grants —
availability of good data matters more than purity here; say which way you chose and why.

## Build record
(pending)
