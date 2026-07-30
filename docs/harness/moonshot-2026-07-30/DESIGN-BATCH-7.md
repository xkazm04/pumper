# Batch 7 Design — Platform Power (2026-07-30)

> Branch: `vibeman/moonshot-batch7-2026-07-30` off merged master `9b626e3` (PR #26). Baseline: tests 724/0.
> Items: M15, M22, M24, M08, M32. Shared rules = DESIGN-BATCH-1.md §Shared rules. Agents read their FULL finding entry and follow its Path; this doc is coordination + scope trims.

## File-scope partition (HARD boundaries)

| Agent | Item | Finding | Owns |
|---|---|---|---|
| A7 | M15 WASM everywhere (v1: trigger predicates + delta transforms) | scraping-engines.md §"### 1. WASM everywhere" | `crates/engine-wasm/**` (host generalization), trigger predicate/transform hook sites (`crates/server/src/triggers.rs` + core trigger types), `plugins-src/**` example |
| B7 | M22 sink connectors (v1: builtin sinks only) | server-api.md §"### 2. Sink connectors" | `crates/server/src/webhook.rs` (delivery side), watches storage/routes sink field, migration 0031, NEW sink module |
| C7 | M24 VCR record/replay | server-api.md §"### 2. VCR mode" | `crates/core/src/fetcher.rs` (record/replay seam), app.rs fetch-path additive, worker enqueue params (`record`/`replay_of`), cassette artifact format |
| D7 | M08 persist the link graph | extraction-storage.md §"### 2. Persist the link graph" | `crates/apps/crawl/**` (edges sink; core crawl.rs ONLY if the edge data isn't reachable from the app sink — justify) |
| E7 | M32 Medicare price oracle | funding-grants.md §"### 2. Medicare price oracle" | `crates/apps/cms-fee-schedule/**`, MINIMAL additive binary-fetch seam in `crates/engine-http/**` (see guardrail), catalog row notes |

Migrations: 0031 = B7. Others claim 0032+ in reply if unavoidable (+ inventory test).

## Guardrails / scope trims

- **A7 M15**: v1 = two hook classes only: (1) trigger PREDICATE plugins — a trigger may name a plugin that receives the delta envelope and returns fire/skip (fuel/memory-capped, fail-open to configured default with loud log); (2) dataset delta TRANSFORM plugins on the trigger's `_trigger` param path (shape the payload before target-job params). Webhook payload shaping + record scoring OUT (that's B7's lane / future). Reuse extract_v2-style params envelope + describe(); example plugin in plugins-src (compiled artifact optional — tests may stub the host like plugin app tests do).
- **B7 M22**: builtin sinks only: `file` (NDJSON append under data/sinks/, path-traversal-guarded), `webhook` (existing behavior = default), `slack` (incoming-webhook URL JSON). WASM sinks OUT of v1 (compose with A7 later). `sink` config on watches (migration 0031); DLQ/backoff/delivery-log wrap ALL sinks uniformly (that's the point — reuse the existing delivery machinery, don't fork it).
- **C7 M24**: `record:true` enqueue flag → every fetch through AppContext persists {url, method, req-hash, status, headers-subset, body} into the job's artifacts as a cassette (size-capped per entry + total; over-cap = recorded-truncated marker). `replay_of:<job_id>` → fetches resolve from that job's cassette by req-hash; MISS = typed error (never silent live fetch — the whole value is determinism); replay jobs are marked and spend $0 (no engine calls). Browser-tier renders recorded as their final HttpResponse-equivalent only (document limitation).
- **D7 M08**: stream (from_url, to_url, depth, rel) edges into `crawl/edges` dataset per run (bounded per-page out-degree cap, default 200; report dropped). In-degree/PageRank OUT of v1 — the retained edges are the deliverable; add a simple `top_linked` per-run summary. Dedup edges within a run.
- **E7 M32**: the RVU ZIP needs bytes — add a MINIMAL `fetch_bytes(HttpRequest) -> Vec<u8>` (hard size cap via existing max_body machinery, no streaming, governor/cache-bypass documented) to engine-http as an additive seam (this is deliberately engine-traits#2-LITE; note the full streaming design stays deferred). Read how app-extractor already unzips (zip handling exists in the workspace) and reuse. Parse PPRRVU CSV → `cms/fee_schedule` keyed {hcpcs}:{modifier?} (work/PE/MP RVUs, conversion factor), release-over-release diff summary (counts + top movers) into `cms/fee_schedule_changes`. Existing release-watcher behavior stays; parsing fires only on new release or `force`. Contract honesty: pin the ZIP/CSV layout assumptions in the doc-header, loud error on drift — the file layout is NOT live-verified today (no download in CI).

## Orchestrator protocol
Dispatch A7–E7 parallel → gate+commit per item → full sweep → FIXES-BATCH-7.md → ledgers → merge → Batch 8 (M35, M38, M09, M25+M26).
