---
slug: datasets-doctor
type: perfect/direction
context: "[[dataset-storage]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 5a2fa10
---
## What & why
Nothing tells an operator the store's actual health. One read-only audit surface reports: revisions
whose stamped artifact is missing on disk (a provenance claim the store cannot honour), provenance
coverage vs replayability, records with null simhash, per-table growth for the tables with no
retention, orphan derived specs, and the stale `triggers_new` table shadowing `triggers`. It is also
the measurement layer that makes [[artifact-retention-provenance-aware]]'s decisions inspectable
rather than blind.

## Evidence
- Provenance coverage primitive already exists: `crates/core/src/datasets.rs:1388+`.
- `replayable()` requires both pins (`datasets.rs:139-141`) — nothing checks the body still exists.
- Never-computed simhash backfill precedent: `b2e1da7`.
- Suspected stale table: `crates/core/migrations/0021_ingress.sql:18` creates `triggers_new` while
  CRUD targets `triggers` (`storage.rs:859,887`) — confirm before acting.
- No retention on `cost_events` / `webhook_deliveries` / `job_yield` (scout-verified, no prune callers).

## Acceptance criteria
- Read-only endpoint registered in the route inventory + OpenAPI, plus a `just` recipe.
- Each check names the concrete remediation (backfill / prune / migration), never a bare count.
- Zero findings on a clean fixture DB (no false positives) — test.
- Reports bytes on disk per app for the artifact tree.
- `docs/features/*` updated; the `triggers_new` question answered definitively in the report or a migration.

## Risks / non-goals
Read-only: it must not mutate or repair anything. Not O(corpus) on a hot path — it is an on-demand
audit, and any full scan is documented as such.

## Build record
Seven checks, each carrying a concrete remediation — asserted by an inventory test rather than
described in prose. `diagnose` is pure; the route only gathers facts.
`gathering_the_report_mutates_nothing` asserts the store is byte-identical after the full
fact-gathering set. Clean store → `findings: []`, proven both purely and against a real migrated
SQLite. `just doctor` + `just retention-preview` added, ONBOARDING §8 synced.
**The `triggers_new` suspicion was REFUTED empirically.** Migration 0021 rebuilds `triggers` through
a `triggers_new` scaffold (SQLite cannot ALTER a CHECK), then drops and renames — inside a
transaction, so the scaffold is never observable. The builder settled it by migrating a real DB and
asserting only `triggers` exists AND that its SQL carries the rebuilt `external` CHECK (which is
what proves the RENAME landed rather than the pre-0021 table surviving). It kept a
`stale_rebuild_tables` check for a rebuild that genuinely fails to land; empty on every correct store.
Director review: this is the standard — a scout suspicion answered with an experiment, not a reading.
Open item: the stat-based `missing_artifact_bodies` check is bounded at 5,000 revisions per report
(stated in the response), unmeasured against a large real archive. Cherry-picked as 5a2fa10.
