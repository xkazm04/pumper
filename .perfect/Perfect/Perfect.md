---
type: perfect/home
repo: pumper
updated: 2026-08-12
pool: 0
pool_target: per-dispatch (r13 cap 6)
shipped_total: 116
coverage: "22/46 map contexts covered (proposal pass on the 46-map) — sweep campaign active; wasm-plugin-host banked slate-grade fronts r14; 24 others never proposed"
cursor: "round 14 PROPOSE: wasm-plugin-host first (banked slate-grade brief from the r13 scout — re-verify seeds), then eu-grants, us-business-census, czech-labor-market, web-crawler, crawler-core, engine-contracts (opp 5); never-proposed before re-mining"
last_session: "[[sessions/2026-08-12-5]]"
---

# Perfect — pumper

**Mission**: make pumper the best possible scraping/data-product service — API ergonomics, dataset quality, runtime robustness, and cost efficiency — one gated, shipped direction at a time.

**State**: pool **0** · phase: **round 13 SHIPPED — round 14 propose next** (gate: director-self-gated, autonomous, Athena-dispatched, SWEEP round).

### Round-13 pool — ALL 6 SHIPPED (2026-08-12, gate: director-self-gated, SWEEP round; original session died mid-build, TWO continuation sessions recovered + landed)
claude-engine, 3/5 (REJECTED: concurrency-cap — no volume consumer, r9 precedent;
REJECTED-deferred: token-telemetry — banked with truncation-honesty + compose-role
refresh as the context's anchors):
1. [[claude-kill-tree]] — robustness · M (Windows cmd /C shim orphans the real CLI on
   timeout; kill_on_drop reaps cmd.exe only; orphan spends with no ceiling)
2. [[claude-cost-honesty]] — robustness · M (cost discarded on every failure path —
   is_error/non-zero-exit/timeout; chokepoint meters Ok only; bughunt 2026-07-14 still
   live; non-string schema result → uncacheable, re-pays forever)
3. [[claude-subprocess-hygiene]] — robustness · M (cmd.exe arg mangling for schemas;
   model free-string + typo'd-role silent fallthrough; repo CWD/CLAUDE.md/Stop-hook
   loaded into every research call)

cron-scheduler, 3/5 (REJECTED-deferred: tick-serialization — fix lives in
webhook-delivery's write set, banked there; REJECTED: tick-telemetry — thin value,
doc-truth rides tick-isolation):
4. [[sched-tick-isolation]] — robustness · M (one bad schedule starves the rest via `?`;
   unjoined task + inline sync-mutex unwraps = silent total-scheduler death; no
   shutdown checks mid-tick; zero tick-loop coverage — SchedulerLoop harness)
5. [[sched-misfire-honesty]] — robustness · M (G4 CONFIRMED: skip batch-drops on-time
   firings sharing a tick with missed ones; grace vs observed tick; skip-branch gate
   parity; fire-time enabled re-check)
6. [[schedule-budget-door]] — feature · M (schedules are the last work-creator without
   the r12 budget contract; migration 0040 + door + fire-path plumb)

### Wave plan (round 13) — ONE branch `perfect/2026-08-12-r13`, main checkout, 2 CONCURRENT lots
Write sets disjoint: **Lot C** (opus) = 1,2,3 → crates/engine-claude/* (lib, Cargo.toml,
new tests/), core/src/{error,app,fetcher}.rs, core/tests (chokepoint/meter),
docs/features/{fetching,apps}.md, ONBOARDING.md §engines · **Lot S** (opus) = 4,5,6 →
server/src/{scheduler,main,worker,datahub,harness}.rs, server/routes/schedules.rs,
core/src/storage.rs, core/migrations/0040, core/tests/migrations.rs, server/e2e/*,
docs/features/{runtime,http-api}.md. No shared Class B beyond e2e/mod.rs (S only) —
truly disjoint. runtime.md belongs to Lot S; Lot C reports any need there.

### Round-12 pool — ALL 6 SHIPPED (2026-08-12, gate: director-self-gated, SWEEP round)
declarative-extractor, 3/5 (REJECTED: versions-nplus1 — banked; REJECTED-deferred:
e2e-coverage — banked):
1. [[extractor-mode-door]] — robustness · M (anyOf admits combined mode roots; run()
   first-match-wins silently; prose claims exclusivity nothing enforces)
2. [[extractor-result-honesty]] — robustness · M (five result lies: phantom output_shape
   keys, uncapped-claim on 10k source read, backfill drops health, silent registration
   failure, write target unnamed)
3. [[extractor-records-echo]] — optimization · M (unbounded record echo into the job row;
   worker indexes FROM the echo — move to index_datasets path first)

job-search-api, 3/5 (REJECTED-deferred: host-weather-import-atomicity — CONFIRMED, banked
as next anchor; REJECTED: recipes-surface-honesty — dead surface, real fix banked on
tiered-fetcher):
4. [[job-budget-floor]] — robustness · S (budget_usd 0/negative → silently unlimited on a
   paid path)
5. [[job-control-event-honesty]] — robustness · M (empty-app control events invisible to
   watchers; drain-window cancel says cancelled:true while the job resurrects)
6. [[search-degraded-honesty]] — robustness · S (wiped/disabled index answers 200 empty —
   indistinguishable from no-matches; MEMORY.md invariant #4)

### Wave plan (round 12) — ONE branch `perfect/2026-08-12-r12`, main checkout, 2 CONCURRENT lots
Write sets disjoint: **Lot X** (opus) = 1,2,3 → crates/apps/extractor/src/*, extractor
tests, docs/features/extraction.md — worker.rs is OUT of set (any search_docs adjustment
is REPORTED for a Director commit) · **Lot J** (opus) = 4,5,6 →
server/routes/{jobs,search}.rs, server/worker.rs, server/mcp/{mod,live}.rs,
core/storage.rs (retry_bulk), server/e2e/*, docs/features/{http-api,search}.md.
Class B both lots: routes/mod.rs EXPECTED inventory, e2e/mod.rs.

### Round-11 pool — ALL 6 SHIPPED (2026-08-12, gate: director-self-gated, Athena-dispatched, SWEEP round; original session died mid-build, continuation recovered + finished)
extraction-core, 3/5 (REJECTED-deferred: induce-surface — banked with the induce quality bundle;
markdown-fidelity — simhash-invalidation coupling needs its own guarded design):
1. [[each-field-reports]] — robustness · M (Each emits ONE FieldStatus; listing rot invisible to
   every honesty surface)
2. [[extract-honesty-sweep]] — robustness · S (XPath Debug garbage + Err→Empty; default dead on
   blanks; numeric coercion disagreement; 2 untested transforms)
3. [[url-absolutize]] — feature · M (extracted hrefs stay relative; no page-URL access in
   transforms; induce emits relative)

automation-api, 3/5 (REJECTED-deferred: trigger-ledger-completeness, schedule-runs-ledger,
automation-metrics — all banked in the context note):
4. [[enqueue-door-parity]] — robustness · M (schedules/triggers bypass the params-schema 422;
   scheduler replaces while HTTP merges)
5. [[schedule-truth]] — robustness · M (guard/health SQL divergence — retry wedges firing while
   health says ok; last_run lies under misfire-skip)
6. [[watch-honesty]] — robustness · M (virtual namespaces unwatchable while the fan-out looks
   for them; dead watches accepted silently; no watch→deliveries path)

### Wave plan (round 11) — ONE branch `perfect/2026-08-12`, main checkout, 2 CONCURRENT lots
Write sets disjoint: **Lot E** (opus) = 1,2,3 → core/{extract,induce}.rs, apps/extractor/*,
core extract tests, docs/features/extraction.md · **Lot A** (opus) = 4,5,6 →
server/routes/{schedules,watches,triggers,jobs}.rs, server/{scheduler,worker}.rs,
core/storage.rs, server/e2e/*, mcp/mod.rs (validate helper), docs/features/events-webhooks.md.
routes/mod.rs EXPECTED + e2e/mod.rs = Class B. Director commit first: fetch_chokepoint guard
multiline fix (9 invisible raw-engine sites; crates/core/tests/fetch_chokepoint.rs only). · round-11 cursor: **extraction-core** (scout brief banked in the context note — re-verify seeds first); app-runtime + tiered-fetcher off cooldown (fetcher anchor: recipe-discovery-wiring); webhook-delivery + dataset-peering off cooldown (anchors banked); browser-transact + api-surface on cooldown until round 12 (anchors: evidence-access endpoint · EventBus lock sweep / metrics-hot-path / doc-coverage test). Rounds 1–10: **98/98 accepted directions shipped**, zero failed, zero dropped (rejections recorded per round).

### Round-10 pool — ALL 6 SHIPPED (2026-08-11, gate: director-self-gated, Athena-dispatched)
browser-transact, 3/5 (REJECTED-deferred: evidence-access endpoint — banked anchor, write-set
collision with the api lot; REJECTED: flow-budget param — subsumed/no consumer):
1. [[transact-evidence-honesty]] — robustness · M (steps_completed counts attempts; selector_found
   discarded; submit target never assessed; DOM-cap destroys evidence post-act)
2. [[transact-secret-redaction]] — robustness · S (passwords republished into evidence/result/SSE/webhooks)
3. [[transact-retry-safety]] — robustness · M (deterministic refusals retried; door lets garbage through;
   typo'd profile runs logged-out)

api-surface, 3/5 (REJECTED-deferred: metrics-hot-path, doc-coverage test — both banked):
4. [[api-bounded-shutdown]] — robustness · M (one SSE tab = shutdown hangs to SIGKILL; 3 loops escape lifecycle)
5. [[api-error-contract]] — robustness · M (403/429/503 code "internal"; 500 bodies leak sqlx/path/URL text)
6. [[api-panic-containment]] — robustness · M (panic = connection reset; poisoned mutex = routes dead forever)

### Wave plan (round 10) — ONE branch `perfect/2026-08-11`, main checkout, 2 CONCURRENT lots
Write sets disjoint: **Lot T** (opus) = 1,2,3 → apps/transact, engine-browser/lib.rs,
core/{engine.rs,error.rs,testing.rs}, docs/features/apps.md · **Lot A** (opus) = 4,5,6 →
server/{main.rs,state.rs,refresher.rs,datahub.rs,mcp/live.rs}, server/routes/{mod,error,events,
health,ingress,jobs,query,receipt}.rs, root Cargo.toml (tower-http), docs/features/http-api.md.
Server e2e mod registrations = Class B for both lots (re-read + unique anchor).

### Round-9 pool — ALL 6 SHIPPED (2026-08-11, gate: director-self-gated, Athena-dispatched)
app-runtime, 3/5 (REJECTED: fetch-hot-path-batching — no volume consumer;
mid-run-budget-visibility — receipt + cost_events already answer it):
1. [[fetch-chokepoint]] — robustness · M (raw paid-tier fetch: unmetered, unclamped, un-VCR'd)
2. [[budget-exhaustion-terminal]] — robustness · S (deterministic error retried to attempt burn)
3. [[vcr-attempt-integrity]] — robustness · S (retry appends → failed attempt shadows replay)

tiered-fetcher, 3/6 (REJECTED-deferred: recipe-discovery-wiring — banked anchor;
REJECTED: governor-hot-path, quality-signal-expansion):
4. [[politeness-memory-honesty]] — robustness · M (zombie penalties resurrect on boot)
5. [[browser-down-ladder]] — robustness · M (dead Chrome + browser pin kills working http)
6. [[cache-growth-bounds]] — robustness · M (3 unbounded stores on default deployment)

### Wave plan (round 9) — ONE branch `perfect/2026-08-10-r9`, main checkout, 2 CONCURRENT lots
Write sets are disjoint: **Lot A** (opus) = 1,2,3 → apps/{extractor,plugin},
core/{app.rs,engine.rs,error.rs,vcr.rs}, server/worker.rs, core/tests(A) ·
**Lot F** (opus) = 4,5,6 → core/{fetcher.rs,governor.rs,tiers.rs,cache.rs,config.rs},
server/{state.rs,main.rs,refresher.rs,routes/runtime.rs}, config.toml, core/tests(F).
docs/features/{runtime,fetching}.md = Class B for this wave (both lots may append).
Director commits: smoke.ps1 stale-binary fix, recipes.rs:8 stale doc comment.

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

## Sweep campaign (round 11+, Michal via Athena 2026-08-12): cover ALL 46 map contexts

**Covered** = ≥1 proposal pass recorded on the 46-map (slate gated, or an explicit
nothing-clears-the-bar verdict). Scout-only-with-banked-seeds does NOT count (that is
queued work, not coverage). Cursor policy: never-proposed first, ties by opportunity.

**Reconciliation 2026-08-12** (map = committed `context-map.json`, generated 2026-08-03 by
`personas-context-scan` on THIS machine at commit 611360f, committed 598ee37; the local
Personas app DB's `dev_contexts` for project 512809db agrees EXACTLY — 46 identical names,
so unlike the Personas repo there is no partition disagreement here): 16 vault notes
matched map names; 30 map contexts had no note → created 2026-08-12; 8 vault-only
old-map notes got `superseded_by:` aliases (broad-crawler, declarative-extraction-engine,
fetch-engines, http-api-routes, job-worker-cron-scheduler, tiered-fetcher-politeness,
us-grant-opportunities, us-trades-wages-tax-valuation) — history preserved, none retired.

**Covered (22)**: dataset-storage r4 · job-worker r4 · source-resilience r5 ·
grants-unified-layer r5 · trigger-pipeline r6 · dataset-api r6 · source-provisioner r7 ·
search-engine r7 · datahub-bridge r7 · webhook-delivery r8 · dataset-peering r8 ·
app-runtime r9 · tiered-fetcher r9 · browser-transact r10 · api-surface r10 ·
extraction-core r11 (3 shipped) · automation-api r11 (3 shipped) ·
archive-engine r11 (nothing-clears-the-bar verdict recorded) ·
declarative-extractor r12 (3 shipped) · job-search-api r12 (3 shipped) ·
**claude-engine r13 (3 shipped)** · **cron-scheduler r13 (3 shipped)**.
(The r12 wrap updated the frontmatter count but not this list — drift fixed 2026-08-12-5.)

**Never-proposed queue (24 after r13's two proposals, opportunity-ranked)**:
| Opp | Contexts |
|---:|---|
| 6 | wasm-plugin-host (banked slate-grade brief, r13 scout — front of r14) |
| 5 | eu-grants · us-business-census · czech-labor-market · web-crawler · crawler-core · engine-contracts |
| 4 | vcr-testing · http-engine · browser-engine · remote-engine (banked anchor) · us-federal-grants · us-state-grants · czech-procurement · trades-operator-economics · trades-pricing · agentic-research · connector-api-watch · page-monitor · maintenance-tooling (banked CONFIRMED anchor) · plugin-runner |
| 3 | wasm-plugin-examples (banked anchor) · data-pipeline-catalog (banked anchor) |
| 2 | hackernews-example (banked anchor) |

(r13 in flight: claude-engine + cron-scheduler left this queue 2026-08-12.)

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

- 2026-08-12 · **Round 13 (6/6 + 1 Director commit, AUTONOMOUS director-self-gated, SWEEP round; original session died mid-build, two continuation sessions recovered + landed — zero work lost)** — claude-engine 3/5, cron-scheduler 3/5; 4 rejected (2 outright: concurrency-cap no-volume-consumer, tick-telemetry thin-value; 2 rejected-deferred banked: token-telemetry as the context anchor, tick-serialization on webhook-delivery's note where its write set lives). Master `59ab59a` → `a53e3ea` (ff; fifth concurrent two-lot wave, zero collisions). claude-engine: 1dcbbeb (a timeout kills the WHOLE process tree — taskkill /T on the shim pid; the orphan-spend class dead; first-ever tests of research() via the new fake-CLI harness), c7f5c37 (a failed call still reports what it spent — ClaudeSpend in Error::Claude, ledger_event pure decision, meter-before-propagate at both seams, cost_unreported on unpriced success, envelope_text kills the uncacheable empty-text class; original builder died with this COMPLETE, landed from its tree), b0d363c (check_shim_argv refuses measured cmd.exe hostiles + 8000-char line budget; system prompts travel by file not argv; unknown role/garbage model refused pre-spawn; subprocess runs in <storage root>/claude-cwd — the 35k-token repo-context leak closed). cron-scheduler: 4788dbb (tick isolated/contained/joined — reconcile_one + PassTally, two-level panic containment, lock_advisory at both inline mutex sites, SchedulerLoop harness = first tick-loop coverage), edf3041 (misfire skip eats only what was genuinely missed — per-firing SkipThenFire, misfire_cutoff = previous pass's timestamp, skip-branch gate parity, fire-time re-check), 85348d2+1f05e09 (a standing cron order can carry a spend ceiling — migration 0040, both doors through the shared validator, firing_budget off the LIVE row so a mid-pass ceiling binds that firing, catalog reconcile cannot wipe one; the last work-creator now under the r12 budget contract). Director: a53e3ea (smoke 29→32: budget floor 422, late door 404, ceiling round-trip). **Final gate: 1486/0 full workspace (`just ci` exit 0) + live smoke 32/32 on the wave tip (= merged master content, ff).** Coverage 20→22/46. Cumulative: 116/116.
- 2026-08-12 · **Round 12 (6/6 + 3 Director commits, AUTONOMOUS director-self-gated, SWEEP round)** — declarative-extractor 3/5, job-search-api 3/5; 4 rejected (2 banked, host-weather-import-atomicity CONFIRMED-banked as next anchor, recipes-surface rejected as dead-surface polish). Master `3385b8b` → wave tip (ff; fourth concurrent two-lot wave, zero collisions, both builders survived to final report — first deathless round since r7). extractor: aac6dd5 (one mode per job — oneOf at the door + resolve_run_mode in the app; the schema's anyOf admitted exactly the illegal combinations; concurrency clamped to the declared 64), 4c9092a (five result lies closed: EXPECTED-diff-pinned output_shape, sweep truncation + source.limit, backfill health/worst_fields via poolable QualityRollup, registration failure surfaces, actual write target named @q included), 742cd44 (records echo bounded 100/1000/0 + index_datasets on all write modes gated producer-side + clone gone). jobs/search: acba9f4 (budget_usd 0/neg/NaN/∞ refused, not silently unlimited), e638efc (control events carry the real app via RETURNING; user cancel outranks the drain via claim-under-mutex-before-fire; refuted "/events has an app filter"), 63db76f (index-state block on every search answer via shared run_search; 3 degraded states; MCP parity). Director: e9c3c32 (search_docs echo-indexing delegated when index_datasets present — double-index closed), 6f6efdb (trigger door budget floor — the audit found the same filter stored+replayed per hop; extinction scan), 0790fdd (smoke 25→29). **Final gate: 1419/0 full workspace + live smoke 29/29 on the wave tip (= merged master content, ff).** Coverage 18→20/46. Cumulative: 110/110.
- 2026-08-12 · **Round 11 (6/6 + 3 Director commits, AUTONOMOUS director-self-gated, SWEEP round, resumed)** — extraction-core 3/5, automation-api 3/5; 5 rejected-deferred (all banked), archive-engine covered with a nothing-clears verdict. Original session died mid-build with ~1,280 uncommitted lines; continuation snapshotted both lots' dirty trees, reviewed and landed the completed first directions, then two fresh opus continuation builders finished the rest — zero work lost. Master `d275fde` → `3216ec7` (ff; third concurrent two-lot wave, zero collisions). extraction: 1b6aebc (per-inner-field listing reports — rot vs sparse distinguishable, worst_fields + replay wired), 26fb0cc (four extraction lies closed: XPath Debug garbage, Err→Empty, default-on-blank via shared is_blank, to_int saturation), ee5d8e4 (url_absolute per-call base seam, every URL-bearing mode + preview + induce; base_url_missing honesty; FetchOutcome-drops-final_url found + banked). automation: 6c3f91a (one params door behind all six work-creators, inventory-enforced; replace→merge resolved; bad_params ledger outcome), 85bb5f9+fb78574 (existential guard twin deleted — retry-wedge dead; migration 0039 skip recording; schedule_reference shared by reconcile + projection; builder self-caught a red-guard ship via full-failure rescan), 5ee2462 (NamespaceIndex registry+seed+store+saved-search; virtual namespaces watchable; separate trigger filter set — ingress ids aren't apps; watch→deliveries route; explicit-null last_delivery; trades gap banked). Director: 1c43ca7 (chokepoint guard sees through rustfmt wrapping — 9 invisible raw-engine sites reviewed into EXPECTED), 84e4ac3+3216ec7 (smoke 21→25: schedules 422 door, grants watchable, bogus-filter 400, last_delivery). **Final gate: 1372/0 full workspace + live smoke 25/25 on the wave tip (= merged master content, ff).** Coverage 15→18/46. Cumulative: 104/104.
- 2026-08-11 · **Round 10 (6/6 + 2 Director commits, AUTONOMOUS director-self-gated, resumed)** — browser-transact 3/5, api-surface 3/5; 2 rejected outright, 3 rejected-deferred (banked). Session died post-gate pre-build; continuation session re-verified the evidence anchors and ran the recorded wave plan unchanged. Master `95713f0` → `c630a3f` (ff; second concurrent two-lot wave, disjoint write sets, zero collisions). transact: 428d2e9 (evidence bundle distinguishes a total miss from a clean run — per-step outcomes, successes-only count, submit-target probe, profile + DOM truncate-don't-destroy), f0ddec5 (secrets masked in-page + decode-side re-enforcement; end-to-end no-sentinel proof), 8e17ca7 (Error::Transact terminal; door tightened schema+app-side — builder discovered trigger enqueues bypass the validator entirely; missing profile refuses pre-Chrome). api: c9c2c68 (shutdown terminates bounded with the politeness snapshot flushed post-drain; all 3 SSE surfaces end on the token), 0cfc366 (error-code map complete + inventory-enforced; client_facing exhaustive table; 500 bodies redacted+logged; RowNotFound reasoned-stays-500), 4855fcd (CatchPanicLayer in the driven stack; lock_advisory ×5 + no-lock-unwrap sweep; preview clamp). Director: 684d2c7 (core prelude), c630a3f (smoke +4 checks). **Final gate: 1314/0 full workspace + live smoke 21/21 on the merged master's own binary.** Cumulative: 98/98.
- 2026-08-11 · **Round 9 (6/6 + 2 Director commits, AUTONOMOUS director-self-gated)** — app-runtime 3/5, tiered-fetcher 3/6; 4 rejected outright, 1 rejected-deferred (recipe-discovery-wiring banked as the fetcher anchor). Master `d80dc17` → `4e3647a` (ff; first CONCURRENT two-lot wave on the one-branch shape — disjoint write sets held, zero collisions). app-runtime: 6237cc8 (fetch chokepoint — the last paid-spend/determinism bypass closed; 16-site raw-engine inventory with counts), f918006 (budget exhaustion terminal — fails once, attempts un-burned; governance pauses say so), 4e3647a (VCR cassettes survive retries via the checkpoint-coupled Fresh/Resume rule). tiered-fetcher: a69420a (zombie penalties dead — authoritative snapshot, aged restore, locked reset, tier_memory GC), 65f893e (dead browser degrades the ladder; claude tier traces-and-exhausts), ca4bbe9 (revalidations/research_cache/http_cache all bounded by the always-on store_janitor). Director: cdcbc31 (smoke always builds — stale-binary class dead), 53c30c0 (recipes doc honesty). **Final gate: 1265/0 full workspace + live smoke 17/17 on the merged master's own binary.** Cumulative: 92/92.
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
