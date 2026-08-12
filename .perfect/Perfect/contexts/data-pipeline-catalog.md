---
name: data-pipeline-catalog
type: perfect/context
group: Core Platform
category: config
opportunity: 3
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/core/src/catalog.rs (+ catalog/
data-sources.toml it parses). The machine-readable pipeline registry; every new app must
register a [[source]] row (ONBOARDING §10). Thin surface; likely verdict-shaped unless
the scout finds drift between catalog and registry.

## Direction history
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
