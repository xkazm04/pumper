---
type: perfect/home
repo: pumper
updated: 2026-08-10
pool: 0
pool_target: 10
shipped_total: 86
cursor: "app-runtime"
last_session: "[[sessions/2026-08-10]]"
---

# Perfect — pumper

**Mission**: make pumper the best possible scraping/data-product service — API ergonomics, dataset quality, runtime robustness, and cost efficiency — one gated, shipped direction at a time.

**State**: pool **0/10** · phase: **Propose (round 9)** · cursor: **app-runtime** (opp 6); webhook-delivery + dataset-peering on cooldown until round 10. Rounds 1–8: **86/86 accepted directions shipped**, zero failed, zero dropped (rejections recorded per round).

### Round-8 pool — ALL 6 SHIPPED (2026-08-10, gate: director-self-gated, Athena-dispatched)
webhook-delivery, 3/5 (REJECTED: egress-hardening — no privilege gain on an unauthenticated
local API; drain-throughput — deferred behind correctness, banked):
1. [[webhook-dlq-recoverability]] — robustness · M (CONFIRMED: DLQ can't re-sign 2 of 4 kinds)
2. [[webhook-delivery-drain]] — robustness · M (bare-spawn escape from FanoutPool; flake root)
3. [[webhook-observability]] — feature · M (zero webhook metrics; docs name the wrong DLQ state)

dataset-peering, 3/5 (REJECTED-deferred: tombstone-scale, mirror-reconcile — both banked in
the context note):
4. [[peer-feed-loss-windows]] — robustness · M (5 silent-loss/livelock classes in the walk)
5. [[peer-mirror-visibility]] — feature · M (mirrors invisible to watches/triggers/search)
6. [[peer-two-node-proof]] — robustness · M (first two-server e2e; provenance fields pinned)

### Wave plan (round 8) — ONE branch `perfect/2026-08-10`, main checkout, SEQUENTIAL lots
Both lots touch `worker.rs` (webhook dispatch call sites vs change-batch widening) → no
disjoint concurrent partition exists; the wave is two sequential builders on one branch:
**Lot W** (opus) = 1,2,3 in order → review/gates → **Lot P** (opus) = 4,5,6 in order.
Director-first fmt normalization landed as `1fa3b23` (rust-toolchain.toml pin + repo-wide fmt).

### Round-7 pool — ALL 14 SHIPPED (2026-08-04)
source-provisioner, 4/5 (budget-honesty REJECTED — first rejection since round 3):
1. [[provisioner-sample-stage-fix]] — robustness · S (confirmed showstopper)
2. [[provisioner-coherent-scoring]] — robustness · M
3. [[proposal-lifecycle]] — feature · M (sequenced after 4)
4. [[provisioner-record-honesty]] — robustness · S

search-engine, 5/5:
5. [[search-ghost-doc-gc]] — robustness · M
6. [[search-enrich-hardening]] — robustness · M (confirmed panic-class bug)
7. [[search-lifecycle-safety]] — robustness · M
8. [[search-incremental-proof]] — robustness · S
9. [[search-surface-parity]] — feature · S

datahub-bridge, 5/5:
10. [[datahub-governance-preview]] — feature · M
11. [[datahub-governance-reversible]] — robustness · M (sequenced after 10)
12. [[datahub-emission-lifecycle]] — robustness · M
13. [[datahub-poll-mechanics]] — optimization · S
14. [[datahub-config-honesty]] — robustness · S

### Wave plan (round 7)
- Wave 1 (3 builders): **P1** = provisioner (1, 4, 2 — fix first, honesty, then scoring) ·
  **SE1** = search (5, 6, 8) · **DH1** = datahub (12, 13, 14).
- Wave 2 (worktrees reset to merged master): **P2** = (3, sequenced after 4) ·
  **SE2** = (7, 9) · **DH2** = (10, 11 — sequenced pair inside one brief).
- Per-builder CARGO_TARGET_DIR + CARGO_INCREMENTAL=0; disk check before each wave;
  `just smoke` after final merge; gate commands must preserve exit codes.

### Round-6 pool — ALL 11 SHIPPED (2026-08-04)
trigger-pipeline, 6/6:
1. [[per-dataset-trigger-hops]] — robustness · S (confirmed bug)
2. [[trigger-decision-ledger]] — feature · M
3. [[ingress-replay-defense]] — robustness · S
4. [[trigger-hot-path]] — optimization · M
5. [[activate-wasm-hooks]] — wildcard · M
6. [[pumper-smoke-harness]] — robustness (cross-context, banked seed from rounds 4–5) · M

dataset-api, 5/5:
7. [[read-path-population-honesty]] — robustness · M (confirmed bug)
8. [[derived-trust-inheritance]] — robustness · M
9. [[backfill-budget-and-batching]] — optimization · M
10. [[history-keyset-honest-exports]] — robustness · S
11. [[resurrect-pumper-sync]] — wildcard · M (sequenced after 7)

### Wave plan (round 6)
- Wave 1 (3 concurrent): **T1** = triggers (1,2,3) on `worktree-perfect-triggers` ·
  **D1** = dataset-api (7,10) on `worktree-perfect-datasetapi` ·
  **S1** = smoke harness (6) on `worktree-perfect-smoke` (scripts/justfile, independent).
- Wave 2 (worktrees reset to merged master): **T2** = (4,5) · **D2** = (8,9) ·
  **D3** = (11, sequenced after 7 merges so the conformance pin captures the fixed contract).
- Per-builder `CARGO_TARGET_DIR=<main>/target-<id>` with `CARGO_INCREMENTAL=0`; check free
  disk BEFORE each wave; remove target dirs at wrap.

> **2026-08-03 re-registration.** The Personas context scan (`598ee37`) replaced the old 21-context
> map with **46 contexts / 8 groups**, and ~9 Vibeman moonshot batches (M01–M41) plus a per-client
> adoption pass landed between round 3 and now. The round-1–3 queue below is superseded; the new
> queue is scored against the current map. Old→new mapping of *completed* contexts: Tiered Fetcher→
> `tiered-fetcher`; Fetch Engines→`http-engine`/`browser-engine`/`claude-engine`; Extraction→
> `extraction-core`/`declarative-extractor`; Grants→`us-federal-grants`/`us-state-grants`/
> `grants-unified-layer`; Worker/Scheduler→`job-worker`/`cron-scheduler`; Broad Crawler→
> `web-crawler`/`crawler-core`; HTTP API→`api-surface`/`dataset-api`/`job-search-api`/
> `automation-api`; US Trades→`trades-operator-economics`/`trades-pricing`.
> **Cooldowns are void** — every mapped context's code has changed materially since it was served.

## Queue — round 4 (re-scored 2026-08-03 over the 46-context map)

Opportunity = consumer reach × headroom × strategic fit. The moonshot batches shipped breadth fast;
the headroom now sits in the load-bearing substrate they all write through, and in making the newest
(<3-week-old, largely untested) surfaces real.

| # | Context | Group | Opp | Why |
|---:|---|---|---:|---|
| 1 | dataset-storage | Core Platform | 9 | every app writes through it; zero artifact GC, O(n²) dup scan, 3-queries-per-record bulk path, atomicity fixes still fresh |
| 2 | job-worker | Job Orchestration | 9 | CONFIRMED saved-search scoping bug; whole file <3 weeks old with one e2e test; enforcement ordering unguarded |
| 3 | source-resilience | Core Platform | 8 | gates writes and suppresses removals platform-wide — highest blast radius of the new code |
| 4 | grants-unified-layer | Grants Intelligence | 8 | sweep_closed O(n) + link_duplicates O(n²) per run over the whole corpus (banked seed) |
| 5 | trigger-pipeline | Event Pipeline | 7 | reactive edges + ingress + WASM hooks, cycle guards untested at depth |
| 6 | tiered-fetcher | Scraping Engines | 7 | archive/remote tiers added since round 1; tier memory + recipes now carry more policy |
| 7 | dataset-api | HTTP API | 7 | trust semantics reimplemented per route; cursor formats diverge |
| 8 | source-provisioner | Content & Research | 7 | speaks sources into existence; proposals never verified end-to-end |
| 9 | search-engine | Scraping Engines | 7 | entity fast fields new; wiped-index backfill only rolls forward |
| 10 | datahub-bridge | Event Pipeline | 6 | detached spawn, governance signals can disable schedules — powerful and unproven |
| 11 | dataset-peering | Content & Research | 6 | real working consumer of the revision feed; the contract it pins has no conformance test |
| 12 | webhook-delivery | Event Pipeline | 6 | DLQ + sinks matured; replay/backoff surface |
| 13 | app-runtime | Core Platform | 6 | AppContext facade: budgets, VCR, checkpoints, artifacts all meet here |
| 14 | browser-transact | Content & Research | 6 | dry-run only by design — the evidence bundle is the whole product |
| 15 | api-surface | HTTP API | 6 | route inventory test is the model convention; body limits/CORS policy |
| 16 | extraction-core | Core Platform | 6 | induction + salvage added since round 3 |
| 17 | us-business-census | Market Data | 5 | live-verified recently; suppression handling is the risk |
| 18 | czech-labor-market | Market Data | 5 | nowcast is a projection — needs honesty guards |
| 19 | cron-scheduler | Job Orchestration | 5 | piggybacks reaping/DLQ/DataHub polling on one tick |
| 20 | vcr-testing | Core Platform | 5 | determinism harness, no worker-level round-trip test |
| — | (remaining 26 contexts) | — | ≤5 | thin surfaces, examples, single-source ingesters |

## Superseded queue (rounds 1–3, old 21-context map)

**Strong round-4 seeds already banked** (from round-3 builder findings, no scout needed):
- Saved-search app-scoping bug: worker scopes alerts by JOB app, but `index_datasets` docs carry the virtual app (`grants`) — alerts scoped by app are silently skipped. Worker-side fix. (Job Server context.)
- `index_datasets` re-indexes the FULL dataset every run — needs incremental indexing before large datasets adopt the seam. (Job Server / Search.)
- grants: sweep_closed O(n) + link_duplicates O(n²) run on every run of both apps over the whole corpus — real scaling cliff. (Grants, after cooldown.)
- No artifact retention/GC policy anywhere — bodies accumulate in per-job dirs forever; source-mode extraction depends on them. (Dataset Store / Runtime.)

## Queue (opportunity-ranked, 2026-07-13 init scoring)

Score = consumer reach × headroom (post waves 1–9) × strategic fit. Refined per-context at proposal time.

| # | Context | Group | Opp | Notes |
|---:|---|---|---:|---|
| 1 | Tiered Fetcher & Politeness | Scraping Runtime Core | 8 | every app benefits; `no_cache` follow-up open; self-learning tier routing unshipped |
| 2 | US Trades Wages, Tax & Valuation | Economic & Labor | 8 | unmetered Claude spend (follow-up); digital-twin / exit-readiness ideas unshipped |
| 3 | HTTP API & Routes | Job Server & API | 7 | T7 tail: auth, OpenAPI, SSE Last-Event-ID all deferred |
| 4 | Job Worker & Cron Scheduler | Job Server & API | 7 | T8: manual retry/requeue, misfire handling, adaptive cadence unshipped |
| 5 | Broad Crawler | Data Extraction & Storage | 7 | T6 maturity: sitemap discovery, crawl-delay, per-host tuning unshipped |
| 6 | Fetch Engines (HTTP/Browser/Claude) | Scraping Engines | 7 | learned rate governor, session management headroom |
| 7 | Declarative Extraction Engine | Data Extraction & Storage | 7 | T5 LLM-assisted / self-healing extraction remains |
| 8 | US Grant Opportunities | Public Funding | 6 | unified layer shipped (w6); agency behavior intel remains |
| 9 | Full-Text Search Index | Scraping Engines | 6 | fundamentals closed (w5); deferred: hybrid semantic, autocomplete, answer layer |
| 10 | Web Research & Readable Content | Content & Research | 6 | T3 provenance/citations unshipped; research digest |
| 11 | App & Job Model | Scraping Runtime Core | 6 | metering exists; migration of agentic apps incomplete |
| 12 | Engine Capability Traits | Scraping Runtime Core | 5 | schema-locked extraction, SDK crate ideas |
| 13 | Configuration & Data Source Catalog | Job Server & API | 5 | source-scout drafting catalog entries |
| 14 | WASM Plugin Sandbox | Scraping Engines | 5 | plugin manifest/versioning, polyglot SDK |
| 15 | Extraction, Crawl & API Watch | Content & Research | 5 | watch app shipped (w1); self-maintaining connectors moonshot |
| 16 | EU & Regulatory Funding Watchers | Public Funding | 5 | SEDIA clean-text shipped (w9); reopen prediction remains |
| 17 | Live Events & Webhooks | Job Server & API | 4 | mature after w4–5 (logged, signed, replayable) |
| 18 | Dataset Store & Change Detection | Data Extraction & Storage | 4 | heavily served w1–7 (revisions, removed_at, triggers) |
| 19 | Czech Labour Market (MPSV) | Economic & Labor | 4 | served w6+w9 (role_trends, salary gap) |
| 20 | US Trades Business Density | Economic & Labor | 4 | census blend shipped w9 |
| 21 | App Registry | Job Server & API | 3 | hot-reload deferred by choice; thin surface |

## Round-5 pool — ALL 10 SHIPPED (2026-08-04)

source-resilience, 5/5 accepted 2026-08-04 (clean sweep):
1. [[adaptive-cohort-floor]] — robustness · M
2. [[quarantine-recovery-ladder]] — feature · M
3. [[enforcement-preview]] — wildcard · M
4. [[single-parse-fingerprints]] — optimization · M
5. [[detector-false-positive-fixes]] — robustness · M

grants-unified-layer, 5/5 accepted 2026-08-04 (clean sweep; slate REBUILT after both banked seeds
were falsified — sweep_closed already narrowed by bee7854, link_duplicates already banded by 51ce092):
6. [[unified-health-inheritance]] — robustness · M
7. [[coalesce-unified-finalize]] — optimization · M
8. [[federal-award-amounts]] — feature · M
9. [[close-date-timezone-honesty]] — robustness · M
10. [[grant-recurrence-relation]] — wildcard · M

### Wave plan (round 5)
- Wave 1: **R1** = resilience (1,2,5 — all in detect.rs/sketch.rs/store.rs) on `worktree-perfect-resilience`
  · **G1** = grants (6,8,9 — all in grants-common/lib.rs) on `worktree-perfect-grants`.
- Wave 2 (same worktrees, reset to merged master): **R2** = (3,4) · **G2** = (7,10).
- Per-builder `CARGO_TARGET_DIR=<main>/target-<id>`, removed at wrap. Check free disk BEFORE launch.

## Round-4 pool — ALL 10 SHIPPED (2026-08-03)

dataset-storage, 5/5 accepted 2026-08-03 (clean sweep):
1. [[artifact-retention-provenance-aware]] — robustness · M
2. [[bulk-upsert-batching]] — optimization · M
3. [[banded-dup-index]] — optimization · M
4. [[removal-guard-in-store]] — robustness · M
5. [[datasets-doctor]] — wildcard · M

job-worker, 5/5 accepted 2026-08-03 (clean sweep; a sixth candidate — retry re-paying research
spend — was killed at the challenge gate, `worker.rs:528-545` already keeps the checkpoint):
6. [[saved-search-virtual-app-scoping]] — robustness · S (confirmed bug)
7. [[worker-panic-containment]] — robustness · M
8. [[finalize-off-the-slot]] — optimization · M
9. [[worker-lifecycle-harness]] — robustness · M
10. [[job-receipt]] — wildcard · M

### Wave record (executed 2026-08-03)
- Wave 1: **D1** = core/datasets (2,3,4) on `worktree-perfect-storage` · **W1** = server/worker
  (6,7,9) on `worktree-perfect-worker`. Different crates → low conflict.
- Wave 2 (sequential, same worktrees `reset --hard master`): **D2** = (1,5) · **W2** = (8,10).
- Per-builder `CARGO_TARGET_DIR=<main>/target-<id>` (round-3 learning: eliminates contention).

## Accepted pool — round 3 (shipped)

1. [[browser-resilience]] — Fetch Engines · robustness · M
2. [[browser-cheap-renders]] — Fetch Engines · optimization · M
3. [[proxy-support]] — Fetch Engines · feature · M
4. [[http-request-controls]] — Fetch Engines · api-ux · M
5. [[session-vault]] — Fetch Engines · wildcard · M
6. [[extract-from-stored-pages]] — Extraction · feature · M
7. [[ruleset-preview-endpoint]] — Extraction · api-ux · M
8. [[extraction-quality-signal]] — Extraction · robustness · M
9. [[markdown-tables-tonumber]] — Extraction · optimization · S
10. [[grants-searchable-alerts]] — Grants · feature · S
11. [[grants-lifecycle-honesty]] — Grants · robustness · M
12. [[grants-query-surface]] — Grants · api-ux · M
13. [[grants-schema-enrichment]] — Grants · optimization · M

(Round-1 and round-2 pools: all shipped — see ledger.)

## Round-1 pool (all shipped)

1. [[fetch-no-cache-ttl]] — Tiered Fetcher · feature · S
2. [[structured-fetch-trace]] — Tiered Fetcher · api-ux · M
3. [[governor-hot-path]] — Tiered Fetcher · optimization · S
4. [[fetch-tier-verdicts]] — Tiered Fetcher · robustness · M
5. [[host-profiles-api]] — Tiered Fetcher · wildcard · M
6. [[trades-common-unified]] — US Trades · feature · M
7. [[trades-meter-research]] — US Trades · optimization · S
8. [[trades-output-guards]] — US Trades · robustness · M
9. [[api-pagination-errors]] — HTTP API · api-ux · M
10. [[api-streaming-bounded]] — HTTP API · optimization · M
11. [[sse-resume-graceful-shutdown]] — HTTP API · robustness · M
12. [[openapi-spec]] — HTTP API · wildcard · M

## Shipped ledger

- 2026-08-10 · **Round 8 (6/6 + 2 Director commits, AUTONOMOUS director-self-gated)** — webhook-delivery 3/5 accepted, dataset-peering 3/5 accepted; 2 rejected outright, 2 rejected-deferred (banked). Master `f079c48` → `06b1deb` (sibling agentic-research commits in between; wave forked from Director fmt pin `1fa3b23`). Webhooks: 4d753fc (DLQ recoverable — all 4 kinds signed, replay gated+claimed, stale-pending reclaimed; builder died pre-commit, Director recovered), 614e7e3 (deliveries inside the FanoutPool lifecycle — 4-session flake class structurally dead), c3d1f52 (delivery_health + 4 /metrics series + honest DLQ docs + smoke 15→17). Peering: 54bf16a (five silent-loss windows closed; 1µs-exact inclusive resume; strict cursor 400s), 4ea11b7 (mirror behaves like local data — hook batch widened across run_indexed_apps, (app,dataset)-keyed, wildcard double-fire pinned), 5a7347f (first two-node e2e in repo history — 8 proofs over a live socket; provenance conformance pinned both ways; peering.md born), bb1c462 (chrono lock). Director: 1fa3b23 (toolchain pin + repo-wide fmt re-baseline), 06b1deb (refusal→retry→apply e2e the review found missing). **Final gate: 1219/0 full workspace + TS 7/7 + live smoke 17/17 on the merged master's own binary.** Cumulative: 86/86.
- 2026-07-13 · US Trades: d83edfd (metering), d95ba60 (output guards), a458c2a (trades-common unified) — gates green on master.
- 2026-07-13 · HTTP API wave 1: 0a91f46 (pagination + error codes, live-server verified), 268d271 (streamed JSON export, bounded dup scan, job-timing metrics) — gates green on master.
- 2026-07-13 · Tiered Fetcher wave 1: d6236d4 (no_cache + ttl_override, watch app live bodies), 1deadf9 (governor DashMap sharding + eviction, markdown once), 11ca817 (bot-wall verdicts, 2xx-only reward, Retry-After dates, [fetcher]/[governor] config) — gates green on master.
- 2026-07-13 · Tiered Fetcher wave 2: a2bcee2 (typed TierTrace, router keys on verdict enum — also fixed latent skip-note-counted-as-strike bug), 6fad704 (tier-memory aging + persisted penalties + /hosts API, migration 0016, live-verified restart restore) — gates green. Fetcher context COMPLETE: 5/5 shipped.
- 2026-07-13 · HTTP API wave 2: 5bdb7ae (EventBus monotonic ids + Last-Event-ID replay ring + graceful shutdown drain w/ requeue-at-deadline, verified live), 343341a (OpenAPI 3.1 at /openapi.json, router+spec single-source via utoipa-axum, coverage test; Director integrated F2's /hosts routes into the spec during merge) — merged-server smoke test green. HTTP API context COMPLETE: 4/4 shipped. **Round 1 total: 12/12.**
- 2026-07-13 · Broad Crawler wave 1 (round 2): 4c132df (crawl/pages dataset via PageSink), 525ed8a (honest errors + bot-wall skipping), 4b085c3 (banded SimHash, no per-page RAM, versioned checkpoint) — gates green, 37 core tests.
- 2026-07-13 · Worker wave 1 (round 2): 49e133c (priority aging), 5a6258a (bulk retry / reset / cancel-running, attempt-fenced writes, live-verified), f04e2a8 (heartbeat reaper, migration 0017, live-verified) — gates green, 51 core + 11 server tests.
- 2026-07-13 · Crawler wave 2 (round 2): 78ad7da (live progress seam + SSE), 1c3fe35 (incremental recrawl / sentinel mode, live-E2E-verified) — gates green, 55 core + 13 server tests. Crawler context COMPLETE: 5/5.
- 2026-07-13 · Worker wave 2 (round 2): c544db2 (cron tz + misfire policy + scheduled retries, migration 0018, live-verified misfire counts), 041055b (job.failed webhooks + failure metric, live-verified HMAC delivery) — gates green, 55 core + 19 server tests. Worker context COMPLETE: 5/5. **Round 2 total: 10/10. Cumulative: 22/22.**
- 2026-07-13 · **Round 3 (13/13)** — Fetch Engines 5/5: a57ee1c (browser relaunch/semaphore/honest waits), 8d3eda5 (resource blocking + recycle, live-proven), 709e84b (body cap + timeout + Retry-After retries), 9d2044f (proxy http/https/socks5), 50e03ba (session vault + cache-bypass correctness catch). Extraction 4/4: 70221c1 (per-field quality signal), ebe5f89 (markdown tables + number parsing), 66b063f (stored-pages source mode, no-double-fetch proven), 387a509 (POST /extract/preview). Grants 4/4: 94940a9 (per-record search via generic index_datasets seam + live search.matched webhook), 9d18132 (close-date sweep + drift guard), d59b307 (taxonomies + real money parsing — builder corrected the scout's guessed CA columns against the live API), c526d9f (GET /grants filters + closing-soon, verified vs SQL over 1,988 live records). Merged-server smoke test green (49 OpenAPI paths). **Cumulative: 35/35.**
- 2026-08-03 · **Round 4 (10/10)** — first round on the re-registered 46-context map. job-worker 5/5: 21c838d (saved-search scoping bug — alerts on the grants virtual app never fired), 4b80eb2 (panic containment; a panicking app no longer waits 120s to be mislabelled "lease expired"), ddebd66 (lifecycle harness over the REAL loop — 11 tests; negative control proved the ordering guards fail when the gate moves), a372209 (off-slot bounded fan-out, **3.6x** throughput, ordering guard strengthened not weakened), 7efdd53 (GET /jobs/{id}/receipt, migrations 0034+0035). dataset-storage 5/5: 879f9ab (batched chunk upserts — 50k sync 28.9s→12.1s, competing writer 1-3 starved → **zero starved**; hashing had been inside BEGIN IMMEDIATE), 51ce092 (banded dup index shared with the crawler, **27x** at the callers' distance), c21f630 (RemovalGuard capability token — bypass became a compile error), 92129d1 (provenance-pinned artifact + ledger retention), 5a2fa10 (datasets doctor; refuted the triggers_new suspicion by experiment). Director commits: 3efb2d9 (migration inventory), f003516 (CLAUDE.md recipes). **988 tests, 0 failed.** Cumulative: 45/45.
- 2026-08-04 · **Round 7 (14/14 + 2)** — source-provisioner 4/5 (first gate rejection since round 3: budget-honesty), search-engine 5/5, datahub-bridge 5/5. Provisioner: 0978626 (SHOWSTOPPER sample fix — the app could not complete a run; builder refuted markdown-first, html wins because selectors need a DOM), 99fe5cc (honest catalog rows, 1-5 scale), 6c16962 (primary-doc scoring + degenerate-draft rejection), 522219c (lifecycle: list/validate/promote/expire — proposals no longer rot). Search: dc03bd0 (ghost-doc GC via `_job` sweep — stable ids would have broken alerting), ed2c683 (non-ASCII panic + RFC3339 dates + enrichment out of the writer lock), 4ca9cc4 (incremental path proven + backfill tombstone purge), f4a7d1b (locked wipe + corrupt-dir quarantine — MmapDirectory lock model refuted and tested), 576a3d7 (MCP parity via shared build_search_request). DataHub: 27c5131 (tracked emissions + 409 sync guard + failure-visible status), 89a9b37 (bounded poll, 20min→50s worst case), 175ce65 (shipped OFF + actuator documented), b2a2e76 (governance preview + 0037 audit + bus events), 93997ed (transitions via 0038 level memory — re-enable respected, restarts don't flap, blind pauses expire). Director: b5aae81-style smoke extension f079c48 (round-7 surfaces in the checklist; caught a PS empty-array quirk live). **Final gate green + live smoke 14/14 on the round's own binary.** Cumulative: 80/80.
- 2026-08-04 · **Round 6 (11/11 + 1)** — trigger-pipeline 5/5 + smoke seed + dataset-api 5/5, two clean-sweep gates. c4f3766 (`just smoke` — first live verification in repo history; 11/11 twice), 48e7ade (per-dataset trigger hops — CONFIRMED multi-dataset hop-dropping bug dead), 5d99cc6 (trigger decision ledger, migration 0036 — "why didn't it fire" now answerable, 404 on unknown trigger), f908903 (ingress replay defense — body-derived event ids), fa26d29 (population honesty — trust honored on EVERY /datasets path, explicit `removed=`), 2aa150d (history keyset + honest exports — truncation now detectable), 78ff895 (@pumper/sync RESURRECTED + two-sided conformance pin; caught removed=exclude silently breaking mirror tombstones), e8ed5e9 (derived trust inheritance — laundering hole closed, weakest_trust floor), 1fde4ac (budgeted backfill, **−36%**, 16,667 point queries → ~100), b30bd47 (trigger eval cache — zero-trigger completions query nothing; honest InstancePre negative result kept as regression gate), 8adfc91 (WASM hooks ACTIVATED — `just plugins-install`, real-host e2e, plugin_missing ledger outcome). Director: b5aae81 (sink-delivery flake, 4th session, killed for real). **Final gate 1088 tests 0 failed + live smoke 11/11 PASS on final master.** Cumulative: 66/66.
- 2026-08-04 - **Round 5 (10/10 + 1)** - source-resilience 5/5: 107dc6b (per-source cohort adequacy + BelowCohort verdict; unjudged runs stop padding their own baseline), 0096a79 (evidence-based recovery quarantined->probation->healthy; quarantine was terminal), 6bbaf7d (two detector false-positive vectors closed + the untested Ambiguous cell), 42e8b37 (**one DOM per doc: 1.8x faster, 38% LESS memory** - the deep body clone, not the parse, was the real cost), 413bc6f (GET /enforcement/preview - what enforce=true would have done). grants-unified-layer 5/5 + 1: 916a38e (contribution-level health gating - a quarantined source no longer writes canonical rows), 86716d3 (timezone-honest sweep - a grant closing 23:59 local was being retired a day early), 230a766 (federal award amounts joined in, live-verified by curl: they are decimal strings containing "none"), 703aff1 (Director decision: harvest on, non-fatal but counted), 139b064 (corpus passes 3x/day -> 1x, **-66.7%** rows on a real 2637-row snapshot), 49ca08c (recurrence typed as its own relation - the builder found annual cycles were being DROPPED, not falsely linked, so the candidate set had to widen). Director: bffd088, 3ab2b4b. **1035 tests, 0 failed.** Cumulative: 55/55.
