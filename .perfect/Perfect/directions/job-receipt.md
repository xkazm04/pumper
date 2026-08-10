---
slug: job-receipt
type: perfect/direction
context: "[[job-worker]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-03
accepted: 2026-08-03
shipped: 2026-08-03
commit: 7efdd53
---
## What & why
The pieces of "what did this job cost me and what did it actually change?" all exist but are
scattered across four surfaces: the cost ledger, M04 job yields, the run's revisions, contract and
health verdicts, artifacts written, deliveries fired and trigger hops. One receipt per job id
answers the operator's real question, and it is the natural home for
[[finalize-off-the-slot]]'s stage timings.

## Evidence
- Cost ledger: `crates/core/src/costs.rs:100-175`. Yields: `crates/core/src/storage.rs:1533-1558`,
  extracted at `crates/server/src/worker.rs:475`.
- Run changes already loaded once per run: `crates/server/src/worker.rs:644-659`.
- Contract/health verdicts: `worker.rs:704-776`, `suppress_unhealthy` at `worker.rs:510-522`.
- Artifacts written under `data/artifacts/<app>/<job_id>/`: `crates/core/src/app.rs:133-150`.
- Honest-null precedent to follow: `crates/server/src/routes/provenance.rs:166`.

## Acceptance criteria
- `GET /jobs/{id}/receipt` registered in the route inventory + OpenAPI.
- Joins cost, yield, per-dataset change counts, contract + health verdicts, artifacts and their
  bytes, webhook deliveries and trigger hops for that job.
- Honest nulls where a stamp is unknown — never an invented or inferred number.
- O(1)-ish per job: no corpus-scale scan on the request path.
- `docs/features/*` updated in the same change.

## Risks / non-goals
Read-only. Not a metrics/dashboard system; one job, one receipt.

## Build record
`GET /jobs/{id}/receipt` joins cost (by engine), stage timings, per-dataset change counts,
contract/health verdicts, artifacts + bytes, deliveries and trigger hops. Real receipt from the e2e
run: `cost.total_usd 0.25`, `stages {run_ms 4, index_ms 0, hooks_ms 4, alerts_ms 0, total_ms 13}`,
`artifacts {page.html, 18 bytes}`, `trigger_hops[1]`, plus three named `unknown[]` entries — honest
nulls, never inferred numbers (the rederive precedent). Migration 0035 makes it O(1)-ish: indexes on
`record_revisions.job_id`, `source_runs.job_id`, `webhook_deliveries.ref_id`, and a new
`jobs.source_job_id` column — the reverse trigger lineage that previously required suffix-matching
`idempotency_key` across the whole jobs table. Registered in the route inventory EXPECTED + OpenAPI.
Director review: migrations are additive and nullable; ALTER TABLE ADD COLUMN is instant on SQLite
and the pre-migration VACUUM INTO backup covers it. **Not verified: neither 0034 nor 0035 has been
applied to a real data/pumper.db** — tests use fresh temp stores only. Builder flagged this itself.
