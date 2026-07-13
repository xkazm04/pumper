---
slug: api-streaming-bounded
type: perfect/direction
context: "[[HTTP API & Routes]]"
lens: optimization
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 268d271
---

## What & why
Buffered JSON export loads ≤100k rows into memory then SILENTLY truncates larger datasets while ndjson/csv already stream; dataset_duplicates runs pairwise SimHash synchronously unbounded; /metrics fans out aggregate queries per scrape and lacks duration/queue-wait metrics.

## Evidence
- JSON export buffer+truncate: routes.rs:535-542 (vs streamed ndjson/csv :544-599)
- Unbounded duplicates scan: routes.rs:626-639
- /metrics per-scrape queries, no histograms: routes.rs:89-115; started/finished cols unused for metrics: job.rs:63-64

## Acceptance criteria
- [ ] JSON export streams the array (no row cap, no truncation), constant memory.
- [ ] dataset_duplicates bounded (size guard or paged scan) with an explicit over-limit response.
- [ ] /metrics adds job duration + queue-wait summaries (histogram or quantile gauges) and avoids redundant per-scrape work (short cache or combined query).
- [ ] docs/features/http-api.md export + metrics sections updated.

## Risks / non-goals
- Non-goal: switching export to Accept-header negotiation.

## Build record
(pending)
