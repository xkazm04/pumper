# Wave 4 Design — Executors and Domain Products (2026-09-02)

Four builders off `master` after wave 3 is merged, plus one **carry-forward builder** (P) that
lands the structural debts the earlier waves could not reach. [DESIGN-WAVE-1.md](DESIGN-WAVE-1.md)
§Shared rules and §Shared surfaces apply unchanged. Read `FIXES-WAVE-3.md` first; the merged code
is the contract.

## File-scope partition (HARD boundaries)

| Builder | Item | Owns | Must not touch |
| --- | --- | --- | --- |
| P | **Carry-forwards** (structural) | `crates/core/src/mesh.rs` (new — the wire format moved verbatim from `crates/apps/peer/src/{envelope,mesh}.rs`; the app re-exports), `crates/apps/peer/src/**` (the move only), `crates/server/src/routes/{host_weather,recipes,mesh}.rs` (import path only), `crates/apps/extractor/src/lib.rs` (`profile:` param door via the core helper `resilience::profiles`), `docs/features/mesh.md`, `docs/features/resilient-extraction.md`, `docs/features/extraction.md` | everything else |
| Q | **N18** Elastic executor plane | `crates/server/src/executors.rs` (new), `crates/server/src/routes/executors.rs` (new), `crates/server/src/worker.rs` (claim path: `executor_id` + capability filter; the remote-report finish path), `crates/core/src/storage.rs` (claim/heartbeat/finish by executor; `jobs.executor_id`), `crates/core/src/job.rs`, `crates/server/src/main.rs` (`--executor` mode), `crates/server/src/executor_main.rs` (new), migration, `docs/features/runtime.md` §executors (new section) + `docs/features/deployment.md` | `triggers.rs`, `webhook.rs`, `events.rs`, app crates, `engine-*` |
| R | **N29** Program registry | `crates/apps/grants-common/src/**` (`program_key`, the corpus-pass rollup), `crates/server/src/routes/query.rs` (`GET /grants/programs`, `program=` on `/grants`), `catalog/data-sources.toml`, `docs/features/apps.md` §grants, `docs/features/http-api.md` row | `apps/grants-gov`, `apps/ca-grants`, `apps/eu-sedia`, `apps/cordis` beyond reading; wave-5's fit engine also lands in grants-common — keep your rollup in its own module |
| S | **N33** State × trade market profile | `crates/apps/trades-common/src/**` (crosswalk helpers, `sync_market_profile`), `crates/apps/census-common/src/**` (`state_fips_for_abbr`), `crates/apps/census-density/src/lib.rs` (call `sync_market_profile` at the end of `sync_market_blend` only), `crates/server/src/registry.rs` (seed the `market` virtual namespace — one list entry), `crates/server/src/mcp/mod.rs` (`market_profile` tool, before `fetch`), `crates/server/src/routes/query.rs` (`GET /market/profile/{state}/{trade}`) — coordinate with R: append your route below R's, `catalog/data-sources.toml`, `docs/features/apps.md` §market, `docs/features/mcp.md` | `apps/census-nonemp`, `apps/census-bfs`, `apps/census-nesd`, the other trades apps beyond reading |
| T | **N27** Corpus graph intelligence | `crates/apps/crawl/src/**` (`graph` mode, revisit importance term, structure changes), `crates/apps/plugin/src/observatory.rs` (`sample_by: rank`), `docs/features/crawling.md`, `justfile` (`graph-indegree` recipe) | `crates/core/src/crawl.rs`, `worker.rs`, `routes/` |

`crates/server/src/routes/query.rs` is touched by R and S: append handlers at the end of the file in
that order; the coordinator resolves the `EXPECTED` list.

## Item specs (v1 slices — do NOT exceed)

### P — Carry-forwards (M, contract) — FIXES-WAVE-1 §4, FIXES-WAVE-2 §1

1. **Mesh wire format to core.** Move `signing_bytes`, `open_envelope`, `fingerprint`,
   `manifest_digest`, `ghost_keys` and the bundle shapes into `crates/core/src/mesh.rs` **unchanged**
   (same tests moved with them); `apps/peer` and the server import from core; `ring` moves to
   core's dependencies if it must (say so). MEMORY.md invariant 9 is then rewritten to state the
   fact rather than the protest.
2. **Extractor `profile:` door.** `apps/extractor` accepts `profile: <name>` wherever `rules` is
   accepted, resolving through the core helper `resilience::profiles::rules_source`; inline `rules`
   keep working and the run reports `repairable: false`; `source_runs.profile_version` is stamped
   from the run. The `repair` app's shadow mode then has a real consumer.
**Gate to prove:** all moved tests pass unchanged; an extractor run under `profile:` stamps
`profile_version` and one under inline `rules` reports `repairable: false`.

### Q — N18 Elastic executor plane (XL, policy) — card JO5

**v1 slice.** Executors are **result-only**: an app is executor-eligible when its manifest declares
`executor: true` (default false) and its `AppContext` needs are fetch/research/artifact/progress/
checkpoint only (no `datasets`) — enforce with an inventory test over the app list.
Coordinator: `POST /executors/claim` (long-poll ≤ 30s, body `{executor_id, capabilities}`; 200 job
or 204), `POST /jobs/{id}/heartbeat`, `POST /jobs/{id}/checkpoint`, `POST /jobs/{id}/progress`,
`POST /jobs/{id}/finish {attempt, result | error}` — all fenced on `(status='running', attempts)`
and behind a `[executors] secret` (compared as digests, like the remote fabric) plus `admin` scope
under `[auth] keys`. `claim_next` gains `executor_id` and an app-eligibility filter; the reaper
already recovers a dead executor; `finalize_fanout` runs on the coordinator from the reported result.
Executor mode: `pumper --executor --coordinator <url>` builds engines only and loops
claim → execute → report with an `AppContext` whose `datasets` is a refusing stub. `GET /executors`
(last poll, running jobs, capabilities), `pumper_executors{state}`, receipts stamp `executor_id`.
Cluster-wide per-app caps computed from the DB, replacing the in-memory map **only for jobs claimed
by executors** (local claim path unchanged).
**Out of v1:** an RPC `Datasets` client, executor-side VCR, TLS/mTLS, executor autoscaling.
**Gate to prove:** an in-process e2e with two "executors" (tasks) draining a queue through the
routes: a job runs remotely and finalizes on the coordinator with its receipt; an executor that
stops heartbeating is reaped and the job re-claimed with its checkpoint; a late `finish` from the
dead executor is refused by the fence.

### R — N29 Program registry (L, contract) — card GI2

**v1 slice.** `program_key(unified_row) -> Option<String>`: federal `aln:<ALN>` when present, else
`<agency-norm>|<program_title>`; Horizon `family:<topic_lineage>`; tested like `classify_relation`.
In the once-per-cycle corpus pass, after `link_relations`, fold live unified rows +
`recurrence_links` + `grants/events` + `cordis/topic_stats` into `grants/programs` rows
`{program_key, title, agency, sources[], cycles_observed, period_days, next_expected_open/close,
prediction_basis, opportunities[], deadline_extended_count, closed_early_count, extension_rate,
last_award_ceiling, win_history?}`; `sync_many` only when the corpus read is complete (the
`rollup_is_complete` idiom), else upsert + warning. Stamp `program_key` on unified rows as a
`DerivedPath`. Routes: `GET /grants/programs?agency=&source=&next_expected_before=` and
`program=` on `GET /grants`; `grants/programs` in `index_datasets`. Catalog `[[source]]` +
contract (`program_key`, `cycles_observed`).
**Out of v1:** award_ceiling_trend, per-program document links, program merges across sources.
**Gate to prove:** two programs with the same stripped title but different ALNs stay distinct
(precision over recall); a program with 3 cycles gets a projection; the corpus pass with an
incomplete read does not `sync_many`.

### S — N33 State × trade market profile (L, contract) — card MD2

**v1 slice.** Helpers `state_fips_for_abbr` (census-common) and `naics4_for_trade(entry)`
(trades-common), inventory-pinned. `market/profile` keyed `<ST>:<trade>`: economics
(wage_band, pricing, tax, compliance, valuation) from `trades/operator_economics`; density
(employer/solo/total, per_10k + basis, coverage) from `census/market_blend`; succession + formation
blocks; `vintages` union; `density_grain: naics4` label; honest `coverage` (never zeros).
Written via `upsert_many_derived` with `DerivedPaths` on the replicated national blocks.
`sync_market_profile(ctx)` called at the end of both `sync_operator_economics` and
`sync_market_blend` (last writer publishes). Seed `market` in `VIRTUAL_NAMESPACES` (publishers:
the trades and census apps). MCP `market_profile {state, trade}` (read-only, before `fetch`) and
`GET /market/profile/{state}/{trade}`. Catalog contract for `market/profile`.
**Out of v1:** county grain (N35, wave 5), currency/inflation adjustment.
**Gate to prove:** Plumbing and HVAC (both NAICS 238220) get distinct profiles with the same
density block and `density_grain: naics4`; a trade with no NES row yields `coverage` partial, not
zeros; `catalog_tests` accepts the `market` namespace.

### T — N27 Corpus graph intelligence (L, none) — card CR6

**v1 slice.** (1) Document the in-degree derived-spec recipe (`{source: crawl/edges, group_by:
$.to_url, aggregates: count}`) + `just graph-indegree`. (2) `crawl` mode `graph`: paged PageRank
over `edges` (damping 0.85, N iterations, checkpoint per pass), writes `crawl/page_rank` keyed by
URL `{rank, in_degree, out_degree, run_at}` with `DerivedPaths` so an unchanged graph reports
`unchanged`. (3) Revisit: `RevisitSeed.importance` read from `page_rank` at seed load; due-score ×
rank under opt-in `importance_weight` (default 0 = today). (4) Observatory `sample_by: "rank"`.
(5) `graph` mode compares this run's per-hub out-edge set against the previous rollup and writes
`crawl/structure_changes` records.
**Out of v1:** HITS hub/authority, cross-run edge tombstoning in `edges` itself.
**Gate to prove:** PageRank on a 5-node fixture matches a hand-computed vector within 1e-3; a
revisit with `importance_weight = 0` orders exactly as today (pin); a hub losing 40% of out-links
produces one `structure_changes` record.

## Merge order (coordinator)

P → T (N27) → R (N29) → S (N33) → Q (N18). Migrations from whatever `FIXES-WAVE-3.md` reports as
the new ceiling. `cargo check --workspace` after each; full gates after all.
