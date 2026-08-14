---
name: data-pipeline-catalog
type: perfect/context
group: Core Platform
category: config
opportunity: 3
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[reconciler-refuses-an-unregistered-app]]"]
---

## Current state
**Scouted THOROUGHLY 2026-08-14 (round 22) as the SPINE of the six-context "thin app + its catalog
row" family brief — COVERED.** 842 lines. Headline verdict: **the reconciler is the best-built
thing in that family** — the `managed_by` fence is genuinely airtight in both directions, and the
defects all live on the *other* side of the seam, in what the apps emit and what nobody checks.

**Confirmed sound (recorded so no future round re-litigates it):** untagged rows never appear in
`update`/`disable`/`orphan`; a hand-made row with a drifting cron yields a `create` of a *separate*
tagged row, never an update of the hand-made one; every write is SQL-fenced and a fence miss is
reported as an error. **The reconciler cannot delete or overwrite a hand-made schedule row.**
Removal is conservative (`live` → `disable`, never delete; no row → report-only `orphan`). A missing
catalog file is safe by design (empty catalog → every managed schedule becomes a report-only
orphan, never a mass-disable). `auto_reconcile` defaults false; boot always plans and logs loudly.

**Real gaps found:**
- The plan **never consults the app registry at runtime** (`:342-347`) — the invariant is enforced
  only by a compile-time test over an `include_str!` **copy** of the TOML
  (`routes/mod.rs:387`), so `$PUMPER_CATALOG` or any post-build edit bypasses it and a typo'd app
  mints an enabled schedule that fails every tick forever.
  → [[reconciler-refuses-an-unregistered-app]]
- `desired.entry(app).or_insert(src)` (`:345`) — first live row per app wins, **silently**.
  `cordis` already has two live rows; identical crons today, one edit from silent.
- **Malformed TOML at the worker contract seam is warn-and-skip** (`worker.rs:1430-1436`) — one
  typo silently disables **every** declared data contract fleet-wide behind a single log line.
  The strongest of the three; banked.
- **Expressiveness gap surfaced from a sibling context:** `max_staleness_hours` cannot express
  "monitor freshness but do not alarm on a legitimately quiet month" — `cadence` alone drives
  `/catalog/health`. See [[connector-watch-failures-are-not-success]]; build the two together.
- Two `max_row_delta_pct` clauses (ca-grants 40.0, connector 20.0) are **structurally inert** on
  upsert-only apps — precedent for deleting them already set when grants-gov's was removed.
- `Contract::evaluate` checks presence + non-null only, with **no non-blank notion anywhere**, so
  `required_fields = ["PortalID"]` passes on `PortalID: ""`.
- Below the bar: `Catalog::load()` does an fs read + full TOML parse at 5 call sites including
  **every job completion**, uncached.

Files: crates/core/src/catalog.rs (+ catalog/data-sources.toml it parses). The machine-readable pipeline registry; every new app must
register a [[source]] row (ONBOARDING §10). Thin surface; likely verdict-shaped unless
the scout finds drift between catalog and registry.

## Direction history
- **2026-08-14 (round 22): PROPOSED — 1 direction, REJECTED on the 6-direction cap.**
  [[reconciler-refuses-an-unregistered-app]] is a real gap but **low frequency**: the compile-time
  test genuinely covers the committed TOML, which is the only catalog in play on any deployment that
  does not set `$PUMPER_CATALOG`. It lost to three defects losing data or misleading users *today*.
  **The r11 `contract-volume-floor` anchor below was re-confirmed and sharpened** — two inert
  `max_row_delta_pct` clauses named with line numbers, plus the precedent that grants-gov's was
  already deleted for exactly this reason. Banked for r23 with
  [[connector-watch-failures-are-not-success]]; the malformed-TOML fleet-wide contract fail-open
  (`worker.rs:1430-1436`) is the strongest of the banked items and should lead that pass.
- 2026-08-12 (round 11): scouted (medium); candidates exist — banked, not slated (cap). NOT
  covered yet. The strongest finding of the thin sweep:
  1. **contract-volume-floor** (M): every max_row_delta_pct in the catalog is structurally
     INERT — Contract::evaluate's removed-count only exists via sync_many tombstones and all
     10 contracted apps are upsert-only, so the declared mass-delete tripwire can never fire.
     AND the resilience layer never runs for them either (observe_extraction has exactly one
     caller: the extractor app) — so a source dropping 2/3 of its rows reads green on every
     declared safety net while stale rows rot in the store. Fix shape: min_records floor +
     run-over-run volume-drop check at the existing worker.rs:1244 seam, kept pure in
     catalog.rs; then fix or remove the 10 inert declarations + docs.
  2. **catalog-dataset-parity** (S, rider): nothing checks a row's dataset matches what the
     app actually writes (contract_for silent-None + worker continue skips without a log);
     parity test iterates live() only, so planned/blocked rows naming dead apps never validate.
  Verified healthy: catalog↔registry parity exact (10 live + 18 exempt = 28), crons match,
  two enforcing tests, 13 unit tests.

## Shipped
- (none on this map)
