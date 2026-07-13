---
slug: grants-query-surface
type: perfect/direction
context: "[[US Grant Opportunities]]"
lens: api-ux
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: c526d9f
---

## What & why
The unified corpus is reachable only via the generic dataset API — no filter by status, close_date range, agency, award band, or source; consumers export-and-filter client-side. Add GET /grants (filtered, cursor keyset, OpenAPI'd) over grants/unified + a persisted cross-source closing_soon view replacing the federal-only artifact digest.

## Evidence
- Generic surface only: docs/features/http-api.md:16,20; no filters on records data JSON
- closingSoon artifact-only + federal-only: grants-gov:174-196

## Acceptance criteria
- [ ] GET /grants?status=&agency=&source=&closing_before=&closing_after=&cursor=&limit= over grants/unified records (JSON-field filtering in SQL — json_extract on data — or a materialized read; state approach + index implications).
- [ ] GET /grants/closing-soon?days= cross-source (or a persisted grants/closing_soon dataset refreshed by both apps — choose, justify).
- [ ] Dual-mode/cursor per repo convention; OpenAPI + EXPECTED inventory; error codes.
- [ ] docs/features/http-api.md + apps.md updated.

## Risks / non-goals
- json_extract scans on large corpora — acceptable at current scale (≤ tens of thousands); note the future index option.
- Non-goal: metering/quotas (parked with auth).

## Build record
(pending)
