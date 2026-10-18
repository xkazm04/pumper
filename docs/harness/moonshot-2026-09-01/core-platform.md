# Core Platform — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## CP1 — Self-healing extraction: closed-loop repair over the source's own history

- deck item **N12**
- lens: `moonshot-architect` · size: **XL** · gate: **policy** · effort 9 / impact 10 / risk 7
- contexts: source-resilience, extraction-core, dataset-storage, app-runtime, vcr-testing
- extends: M09 (induce.rs), M10 (extractor replay over stored bodies), M12 (rules_versions + rederive), M16 (observatory), resilience detector; the deliberately-unbuilt §4/§6/§8 of docs/features/resilient-extraction.md

### Summary
Today a degrading source is detected, quarantined and reported, and then it waits for a human (`IMPLEMENTATION-NOTES.md:105-120`). Every ingredient of an automatic repair already ships as a separate seam: the detector names the broken field and the diagnosis (`crates/core/src/resilience/detect.rs:1-18`, `mod.rs:245-269`), `record_revisions` holds the last-known-good values per key (`crates/core/src/datasets.rs:139-161`), stored bodies are addressable per record (`crates/core/src/app.rs:163-191`), a zero-LLM wrapper inducer already walks a page set and emits a compiled `RuleSet` (`crates/core/src/induce.rs:1-21,116-234`), mined invariants give a deterministic acceptance oracle (`crates/core/src/resilience/invariants.rs:62-77`), the content-addressed rules registry gives every candidate an identity (`datasets.rs:2146-2172`), and `ResearchRequest.json_schema` constrains a Claude proposal to a `RuleSet` shape (`crates/core/src/engine.rs:1145-1147`). Nothing joins them. The moonshot is the join: a `repair` loop that turns quarantine from a terminal state into a probation state.

### Description
The design is already written and honest about what is missing (`docs/features/resilient-extraction.md:600-640` profile registry, `:804-905` repair tiers and the seven validation gates, `:1009-1060` shadow promotion + rollback + anti-oscillation, `:1367-1482` the evaluation plan). What the code shows is that the substrate has moved since the notes were written, which is why this is now cheap rather than speculative:
- **Tier 0 is 80% built.** §6.2's value->selector inversion ("search the new markup for the old value string, derive the shortest stable path, intersect across >=5 documents") is the same machinery `induce::analyze_candidate` runs today for container/slot discovery (`induce.rs:247-300`), minus the seeding by known values. Old values come from `Datasets::history` (`datasets.rs:728-748`); new bodies from `read_source_artifact` (`app.rs:163-191`) or a live fetch through the metered seam.
- **Tier 1 is a constrained search, not a judgement.** `AppContext::research` is the only route to the model and is cache-aware + budget-clamped (`app.rs:422-484`); `json_schema` makes a malformed proposal a parse failure (`engine.rs:1145-1147`); failed-call spend is metered (`crates/core/src/error.rs:1-57`). Three candidates x different exemplar pairs, the model never sees the holdout.
- **Validation is deterministic and already partly implemented:** `RuleSet::compile` (`crates/core/src/extract.rs:141-149`), `extract_batch_with_report_at` for holdout match rates (`extract.rs:1136-1148`), `invariants::check` against the mined set, `DocReport.each` inner-field stats to catch listing rot (`extract.rs:681-744`), and `weakest_trust` semantics for anything a candidate touches.
- **Promotion has a home:** a candidate is `register_rules`'d (`datasets.rs:2146-2157`) and stamped as `rules_hash` provenance on every row it writes (`app.rs:624-629`), so the era a bad repair wrote is exactly identifiable and re-derivable (`crates/server/src/routes/provenance.rs:148-166`). Rollback = move the pointer; history is never rewritten.
- **The falsifier exists as a harness seam:** `ScriptedResearcher` replays transcripts offline (`crates/core/src/testing.rs:125`), VCR replay is $0 and deterministic (`crates/core/src/vcr.rs:1-17`), and `extract_and_fingerprint_batch` reuses one parse (`resilience/mod.rs:375-392`) — so §12.1's mutation harness (rename classes, drop elements, rebind selectors over retained bodies) is an integration test, not a research project. The notes say auto-promotion must not ship before this number exists (`IMPLEMENTATION-NOTES.md:107-116`); this card ships the number first.
Two prerequisites the design names are folded in: the **profile registry** (rules as a named, versioned entity rather than a job param — `resilient-extraction.md:600-640`) and **golden documents** (a handful of pinned bodies per source as the fixed anchor against boiling-frog baseline drift and as the only detection thin sources can have — `IMPLEMENTATION-NOTES.md:134-147`). Registry subjects: `structured-output`, `hitl-approval` (shadow mode is HITL-by-default), `judge-calibration-and-drift`, `quality-gates`, `time-travel-replay`.

### Flow
- Profile registry: `extraction_profiles` + immutable `profile_versions` (migration), `profile: <name>` accepted wherever `rules` is today; `source_runs.profile_version` stamped; inline rules keep working but report `repairable: false`.
- Golden docs: pin N bodies + expected values per profile under `data/golden/`, re-checked every run (exact for stable fields, shape for churny ones); this is also the thin-source detector.
- `resilience-eval` bin + fixture corpus: mutation taxonomy over retained bodies, report recall / false-positive rate per diagnosis; gate everything below on the numbers.
- Tier 0 inversion as a pure function in `induce.rs` (`invert(old_values, new_docs) -> Vec<RuleSet>`), cross-document intersection, brittle-selector lint.
- Tier 1 `repair` app: `ctx.research` with `json_schema = RuleSet`, three candidates, exemplar rotation, idempotency key `repair:{source}:{diagnosis_hash}`, budget from `[resilience.repair]`.
- Seven gates as extracted, tested predicates (`x_not_y` tests); every candidate + verdict persisted in `repair_candidates`.
- Shadow mode: candidate extracts alongside live rules on the same batch (free on rayon), promotes only after `probation_runs` clean; auto-rollback on a tripped probation run; `max_promotions_30d`.
- `POST /sources/{id}/reextract {from_version}` replays retained bodies through the active version, producing new revisions; `source.repair_promoted` / `source.rolled_back` webhooks via `dispatch_event`.

### Expected impact
Operators of the ~30-app fleet stop being the repair loop: a site redesign that today means a quarantined source until someone edits selectors becomes a probation window and a webhook. Measured by time-from-degradation-to-healthy on `source_runs`, share of `markup_drift` verdicts resolved at Tier 0 for $0, and the §12.3 blind-set number (promoted-but-wrong <= 5%). What could break: a repair that binds to a plausible wrong element passes match-rate gates — the distinctness invariant and golden docs are the defence, and shadow mode means it never writes live without a clean streak.

### Evaluation
Claim: quality - a degrading source heals itself with an auditable, reversible rule change instead of waiting on an operator
Before: 0 automatic repairs possible; `[resilience] enforce` is default-off (`crates/core/src/config.rs:524-530`) and the notes state no recall/FPR number has ever been measured (`IMPLEMENTATION-NOTES.md:182-186`); quarantine is a terminal state exited only by `POST /sources/{id}/state`
After: per-source mean time to recovery, Tier-0 resolution rate, and a measured promoted-but-wrong rate from the mutation harness; enforce becomes flippable on evidence
Method: probe - read the detector, inducer, provenance registry, research chokepoint and the design's own not-built markers; traced that every gate has an existing pure function to call
Result: unmeasurable (moonshot) - the instrument is the `resilience-eval` harness this card builds first
Gate: policy

### Evidence

```
IMPLEMENTATION-NOTES.md:105-120 (repair not built), :122-132 (profile registry not built), :134-147 (golden docs not built), :182-186 (no measured recall/FPR)
docs/features/resilient-extraction.md:600-640 (§4 profile registry), :804-905 (§6 repair tiers + gates), :1009-1060 (§8 promotion/rollback)
crates/core/src/induce.rs:1-21, 116-234, 247-300 (zero-LLM inducer already does cross-page slot alignment)
crates/core/src/resilience/invariants.rs:62-77, 104-140 (mined invariants = deterministic oracle)
crates/core/src/resilience/detect.rs:1-18; resilience/mod.rs:245-269 (diagnosis vocabulary incl. MarkupDrift/FieldLoss/SelfInflicted)
crates/core/src/datasets.rs:728-748 (history = old correct values), :2146-2172 (register_rules / rules_by_hash)
crates/core/src/app.rs:163-191 (read_source_artifact), :422-484 (research chokepoint, cache, budget), :624-629 (register_rules stamp)
crates/core/src/engine.rs:1145-1147 (json_schema-constrained research); crates/core/src/error.rs:1-57 (failed spend metered)
crates/core/src/extract.rs:141-149 (compile), :681-744 (InnerFieldStats), :1136-1148 (holdout batch with bases)
crates/core/src/testing.rs:125 (ScriptedResearcher), crates/core/src/vcr.rs:1-17 (deterministic $0 replay)
crates/core/src/config.rs:524-530 (enforce=false default)
```

## CP2 — Transact v2: approval-gated live actions with a transactions ledger and agent tools

- deck item **N01**
- lens: `innovation-catalyst` · size: **XL** · gate: **irreversible** · effort 8 / impact 9 / risk 8
- contexts: engine-contracts, app-runtime, dataset-storage
- extends: M06 Transact v1 (dry-run only) + M29/M03 MCP (read-mostly, default-off); v1 is structurally unable to act — `submit: true` is a typed refusal

### Summary
The web-acting capability shipped as a dry run by design: `TransactRequest::validate` rejects `submit: true` with a message that names the missing slice — "pending-approval transactions + an explicit approve endpoint" (`crates/core/src/engine.rs:511-520, 560-594`). The evidence bundle already answers every question a reviewer needs (filled fields with secrets redacted in-page, submit-target found/visible/enabled, honest step accounting — `engine.rs:806-862, 906-957`), and `idempotency_key` is already "the dedup key of the future `transactions` table" (`engine.rs:545-548`). The moonshot is that table and the state machine around it: pending -> approved -> submitted -> receipt, with the approve step exposed to humans over HTTP and to agents over MCP under the same double-opt-in that already guards enqueue (`crates/core/src/config.rs:206-240`).

### Description
What exists: `Browser::transact` runs steps up to the irreversible action and returns `TransactEvidence` (`engine.rs:1216-1234`); `require_existing_profile` refuses to act under a logged-out vault profile (`engine.rs:658-685`); every refusal is terminal-for-job so a bad flow fails once (`engine.rs:71-94`); the job queue already has an `idempotency_key` mechanism, checkpoints keyed by job+attempt lineage (`crates/core/src/storage.rs:2489-2515`), and HMAC-signed webhooks + a dead-letter drain for the receipt.
What is missing, and why v1 cannot be stretched into it: (1) a durable `transactions` row keyed by `idempotency_key` whose state gates whether the engine may ever run `submit_action` — today the executor has no code path for it at all, which is the right v1 shape and the wrong v2 one; (2) a **resume** of the browser session from the exact dry-run stop point: the evidence bundle proves the page, but a live submit that re-navigates and re-fills is a second flow, so v2 needs the engine to hold (or deterministically rebuild) the pre-submit tab, bounded by `render_budget_secs`; (3) an approval surface — `POST /transactions/{id}/approve` with the evidence sha pinned into the approval so a page that changed between review and submit is refused; (4) a post-submit **receipt** capture (final URL, DOM, network calls via the X-ray `capture_network` seam `engine.rs:1064-1070`) stored as an artifact and stamped with the profile identity; (5) MCP tools `list_pending_transactions` / `approve_transaction` behind a third opt-in (`[mcp] allow_approve`), so a fleet of agents can propose actions while a human (or a policy plugin — the trigger-hook WASM predicate seam, `storage.rs:3100-3135`) approves.
Policy plugins are the agent-native twist: a `TriggerPluginHooks`-style predicate over the evidence bundle (`{"pass": bool}`) can auto-approve low-risk flows (a form with no payment fields, a known submit label) and force HITL for the rest — sandboxed, fuel-bounded, fail-closed for approvals (unlike the trigger hooks which fail open).
Registry subjects: `hitl-approval` (the whole card is its golden path), `browser-credential-boundary`, `audit-logging`, `data-retention` (receipts and DOM snapshots must have a retention story).

### Flow
- Migration: `transactions(id, idempotency_key UNIQUE, app, job_id, profile, state, evidence_sha, approved_by, approved_at, submitted_at, receipt_path)`; state machine as an extracted pure function with `x_not_y` tests (approved-with-stale-evidence-not-submitted, duplicate-key-not-resubmitted).
- Engine: split `transact` into `stage` (v1 behaviour, returns evidence + a session handle) and `commit(handle, evidence_sha)`; commit refuses when the live DOM hash no longer matches the reviewed evidence.
- Routes: `GET /transactions?state=pending`, `POST /transactions/{id}/approve|reject`, `GET /transactions/{id}` with the receipt; approvals HMAC-audited.
- Worker: a `transact` job with `submit: true` becomes pending instead of failing; approval enqueues the commit hop with the same `idempotency_key`.
- Receipt capture through `capture_network` + final-DOM artifact; `transaction.submitted` webhook via `dispatch_event`.
- MCP: `list_pending_transactions`, `approve_transaction` behind `[mcp] allow_approve = false`; WASM policy predicate hook, fail-closed.

### Expected impact
Pumper crosses from reading the web to acting on it under audit: grant submissions, portal form filings, procurement registrations become jobs with a review queue instead of manual browser sessions. Measured by transactions/week reaching `submitted` with a receipt, approval latency, and zero duplicate submissions per idempotency key. What could break: a stale approval submitting a changed form — the evidence-sha pin is the guard, and fail-closed policy hooks mean a broken plugin blocks rather than approves.

### Evaluation
Claim: user - an operator or agent can safely approve and execute an irreversible web action with a receipt
Before: 0 live submissions possible; `submit: true` is a typed `Error::Transact` refusal (`engine.rs:570-580`); MCP ships `enabled=false, allow_enqueue=false` (`config.rs:231-239`)
After: count of transactions by state, approval-to-submit latency, duplicate-submit count (must be 0)
Method: probe - traced the v1 executor's structural stop, the evidence bundle fields, the idempotency-key comment naming the future table, and the MCP opt-in ladder
Result: unmeasurable (moonshot) - the `transactions` table is the instrument
Gate: irreversible

### Evidence

```
crates/core/src/engine.rs:511-520 (v1 = dry-run only, next slice named), :538-548 (submit refused; idempotency_key = future transactions table key), :560-594 (validate), :658-685 (require_existing_profile), :806-862 (TransactEvidence), :906-957 (SubmitTarget probe), :1064-1070 (capture_network), :1216-1234 (Browser::transact default)
crates/core/src/config.rs:206-240 (McpConfig default-off, allow_enqueue=false, per-call budget cap)
crates/core/src/storage.rs:2489-2515 (lineage-guarded checkpoint upsert pattern to reuse), :3100-3135 (PluginHook predicate/transform shape)
crates/core/src/error.rs:171-178 (Error::Transact typed refusal)
```

## CP3 — Dataset time machine: as-of reads, interval diffs and lifespan analytics over revisions

- deck item **N06**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 6 / impact 8 / risk 3
- contexts: dataset-storage, app-runtime
- extends: M12 (provenance ledger — every revision carries a full snapshot + job/rules stamps), M30 (peering feed), M37 (vacancy survival was built once, app-locally); no as-of read surface exists

### Summary
`record_revisions` stores a **full snapshot per revision**, append-only (`crates/core/src/datasets.rs:1715-1719`), with field-level diffs (`:139-161`), `first_seen`/`last_seen`/`removed_at` on every record (`:39-52`), and a provenance stamp per write (`:163-197`). The store therefore already holds a bitemporal history of every dataset in the fleet — and exposes it only as a newest-first change feed and a per-key history page (`:750-835, 866-914`). There is no `as_of` read anywhere (grep over `datasets.rs` and `routes/datasets.rs` finds only the delete receipt's `as_of` field). The moonshot is to make time a first-class query dimension: read any dataset as it stood at an instant, diff two instants as a net change set, and ask lifespan questions (how long do records live, what is the churn rate, which keys flip-flop) generically instead of per app.

### Description
- **As-of reads.** `GET /datasets/{app}/{ds}?as_of=<ts>` reconstructs the live set at `ts` from `record_revisions`: the latest revision per key with `created_at <= ts`, excluding keys whose latest such revision is `removed`. The keyset/trust/filter machinery already exists in `list_records_view` (`datasets.rs:1975-2030`) and `push_json_filters` (`:3187-3254`); the SQL twin is a correlated `MAX(revision)` per key, the same idiom `read_next_revisions` uses (`:1363-1391`). `JsonFilter` binds paths as parameters so filters compose with as-of unchanged (`:299-322`).
- **Interval diffs.** `GET /datasets/{app}/{ds}/diff?from=&to=` folds the revisions between two instants into one net verdict per key (added / removed / changed with a merged field diff / unchanged-after-flapping), reusing `diff_values` (`:629-630`). The peering SDK (`@pumper/sync`) becomes able to catch up from any instant, and triggers can be replayed against history.
- **Lifespan analytics.** Generic survival/churn views over `(first_seen, removed_at)` and revision cadence per key — the store-level version of what M37 built for one app — exposed as an aggregate endpoint and as a derived-spec aggregate kind (`Aggregate::Count|Sum` today, `datasets.rs:3289-3296`; add `lifespan_p50`, `churn_rate`).
- **Retention interplay.** `prune_revisions` keeps the newest N per key (`:1723-1748`), so as-of reads past the retention horizon must say so honestly: the response carries `history_floor` (the oldest revision retained for that dataset) and refuses an `as_of` below it rather than returning a partial world — the same honest-absence stance the doctor takes.
- **Provenance stays attached.** Every as-of row carries its revision's `job_id`/`rules_hash`, so a time-travel read of a grants dataset also answers "which run and which rules produced this value on that day" — directly composable with `rederive`.
Registry subjects: `time-travel-replay`, `versioning-snapshots`, `entity-lifecycle`, `analytics-time-windows`.

### Flow
- Extract `as_of_predicate(ts)` and `fold_interval(revisions) -> NetChange` as pure functions with tests (a key removed then revived inside the window reads as changed-not-added; flapping folds to unchanged; retention floor refuses).
- `Datasets::list_as_of` and `Datasets::diff_between` on the store, keyset-paged, trust-filtered.
- Routes + OpenAPI: `?as_of=`, `/diff`, `/lifespan`; SDK `sync({since})` gains `asOf` and `diff`.
- Derived-spec aggregates gain lifespan kinds; `/economics` can join yield against churn.
- Index: a covering index on `(app, dataset, key, created_at)` if the correlated-max plan proves slow at 1M revisions (measure with the store instrument).

### Expected impact
Every dataset becomes a queryable history rather than a current-state table: agents can ask "what was the deadline on 3 July", analysts can diff two dates, and the SDK can rebuild any point in time. Measured by as-of/diff request counts and the latency of an as-of read at 100k records / 1M revisions. What could break: a retention-pruned window silently returning a partial world — the history floor refusal is the guard.

### Evaluation
Claim: user - any dataset can be read as it stood at any retained instant, and two instants can be diffed
Before: 0 as-of read paths (grep `as_of|as-of|snapshot_at` in datasets.rs and routes/datasets.rs matches only the delete receipt); history is per-key newest-first only
After: p95 latency of an as-of read at 100k records, and diff correctness against a replayed change feed
Method: probe - read the revision schema, the keyset readers and the retention pruner; confirmed full snapshots per revision are stored
Result: unmeasurable (moonshot) - instrument is a store-instrument phase for the new read op plus a fixture with retention pruning
Gate: contract

### Evidence

```
crates/core/src/datasets.rs:39-52 (Record first_seen/last_seen/removed_at), :139-161 (Revision with data snapshot + diff), :163-197 (Provenance), :629-630 (diff_values), :750-835 (changes_since/history_page), :866-914 (changes_page), :1363-1391 (per-key MAX(revision) idiom), :1715-1748 (full snapshot per revision; prune keeps newest N), :1975-2030 (unified read view), :3187-3254 (json filters), :3289-3296 (Aggregate kinds)
grep as_of|as-of|time_travel|snapshot_at over crates/core/src/datasets.rs and crates/server/src/routes/datasets.rs: only DatasetDeletion.as_of (datasets.rs:237-250, routes/datasets.rs:315-466)
docs/features/datasets.md:276-279 (known gaps: no Parquet; changes_since scans per app)
```

## CP4 — Analytical plane: SQL over datasets and revisions with cross-app derived joins

- deck item **N08**
- lens: `business-strategist` · size: **XL** · gate: **contract** · effort 8 / impact 9 / risk 5
- contexts: dataset-storage, engine-contracts, extraction-core
- extends: M11 derived datasets (v1 filter/project/lookup, v2 group-by count/sum — same-app only) and M13/M14 search-as-dataset; the read side is `json_extract` full scans by design

### Summary
The store is a JSON-document table read through `json_extract` full scans, explicitly "the right trade while datasets are in the thousands" with an escape hatch named but not built (`crates/core/src/datasets.rs:1906-1916`). Derived datasets — the only compute-over-data layer — are single-app by construction: `DerivedSpec` keys on `(source_app, source_dataset)` and the lookup half joins "same app" (`datasets.rs:3261-3269, 3410-3432`); aggregates are `count` and `sum($.path)` only (`:3289-3319`). Meanwhile the fleet writes ~30 apps' datasets plus virtual namespaces written by several apps (`docs/features/catalog.md:44-51`), and the SDK's whole purpose is to mirror canonical datasets into consumers that then do the analysis elsewhere. The moonshot is an embedded analytical plane: Parquet snapshots of records and revisions, an in-process SQL engine (DataFusion, or DuckDB via its Rust binding) over them, cross-app derived specs whose recompute is a SQL view, and a `Query` capability trait beside `Search` so the server, MCP and SDK all speak one query language.

### Description
- **Why now:** the group-by recompute already re-reads whole groups from source truth with a `max_group_scan` honesty bound (`datasets.rs:2915-2977`), and aggregate backfills refuse to publish partial totals (`:3076-3184`) — both are workarounds for not having an analytical engine. A columnar snapshot makes the group recompute a query and removes the `stale: true` compromise.
- **Parquet is a named gap** (`docs/features/datasets.md:279`). The snapshot writer follows the janitor/quiet-window pattern (`crates/core/src/config.rs:40-123`) and is content-addressed per `(app, dataset, as_of)`, so the time-machine card and this one share the same artifact.
- **Cross-app derived specs:** lift `DerivedLookup.dataset` to `(app, dataset)` and add a `sql` spec kind whose target is materialized by the query engine; trust still folds through `weakest_trust` (`datasets.rs:97-103`) and provenance still stamps `rules_hash` = the registered spec fingerprint (`:2452-2471`), so a SQL-derived row is as auditable as a filter/project one. Cycle detection reuses `derived_would_cycle`.
- **A `Query` trait in core** mirroring `Search` (`crates/core/src/search.rs:210-247`): `NoQuery` fallback, `engine-query` crate holding the DataFusion dependency, so `core` stays light exactly as `engine-search` keeps Tantivy out of it. Read-only SQL with a row/byte/time budget, parameterized, no writes.
- **Surfaces:** `POST /query {sql}` (bounded, read-only), an MCP `query_datasets` tool under the existing read-mostly posture, `@pumper/sync` gaining a `query()` that runs against the mirror's own Parquet, and derived specs of kind `sql`.
- **Search + query converge:** `SearchDoc.body` is the record JSON (`search.rs:42-64`); a query engine over the same records makes M14's entity-typed fast fields a projection rather than a second index.
Registry subjects: `embedded-db` (measured DuckDB-vs-SQLite OLAP gap in the user's own Politicas work is the founding datum here), `sql-console`, `data-access`.

### Flow
- `Query` trait + `NoQuery` in core; `engine-query` crate (DataFusion first — pure Rust, no C toolchain, which matters on the ARM64 Windows box named in README.md:273-274).
- Parquet snapshot writer for `records` (live view) and `record_revisions`, scheduled on the quiet-window gate, content-addressed under `data/analytics/`.
- `POST /query` with sandbox limits (row cap, byte cap, deadline, read-only plan check), OpenAPI + MCP tool.
- Cross-app `DerivedLookup` + `sql` derived-spec kind; group recompute migrated to the engine; `stale: true` path retired when the engine is present.
- SDK: `query()` over the local mirror; Parquet export endpoint.
- Doctor findings for snapshot staleness and engine absence.

### Expected impact
Pumper stops being only a collector: the same binary answers cross-source analytical questions (grants x census x wages joins) for humans, agents and the SDK, and derived datasets become genuinely relational. Measured by query latency vs the `json_extract` scan on a 1M-revision store, and by the number of derived specs that cross app boundaries. What could break: a second copy of the data (Parquet) drifting from SQLite — snapshots are content-addressed and stamped `as_of`, and the doctor reports skew.

### Evaluation
Claim: performance - analytical reads (group-by, joins, as-of) run orders of magnitude faster than json_extract full scans, and cross-app joins become expressible at all
Before: filtered reads are full partition scans by design (`datasets.rs:1906-1916`); derived specs cannot cross apps (`:3261-3269`); aggregates limited to count/sum with a 10k scan bound that yields `stale: true` rows (`config.rs:661-683`)
After: same queries measured through the store instrument vs the query engine; count of cross-app derived specs
Method: probe - read the derived engine, the read paths, the search trait shape and the config defaults
Result: unmeasurable (moonshot) - instrument is a benchmark fixture at 100k records / 1M revisions run both ways
Gate: contract

### Evidence

```
crates/core/src/datasets.rs:1906-1916 (json_extract full scan, escape hatch not built), :2915-2977 (group recompute with stale bound), :3076-3184 (aggregate backfill refuses partial totals), :3261-3269 (lookup same-app), :3289-3319 (count/sum only), :3410-3432 (DerivedSpec keyed on one app), :97-103 (weakest_trust), :2452-2471 (derived provenance)
crates/core/src/search.rs:42-64, 210-247 (SearchDoc body = record JSON; trait + NoSearch pattern to mirror)
crates/core/src/config.rs:40-123 (quiet-window maintenance gate to schedule snapshots on), :661-683 (derived limits)
docs/features/datasets.md:279 (no Parquet export); docs/features/catalog.md:44-51 (virtual namespaces written by several apps)
README.md:273-274 (no C/C++ cross-toolchain on this box — favours a pure-Rust engine)
```

## CP5 — WASM apps v2: hot-loadable ScrapeApps with AppContext host imports

- deck item **N09**
- lens: `moonshot-architect` · size: **XL** · gate: **contract** · effort 9 / impact 8 / risk 6
- contexts: engine-contracts, app-runtime, vcr-testing, data-pipeline-catalog
- extends: M28 v1 (describe-only dynamic apps: listed `runnable: false`) + M15 (WASM hooks) + M19 (catalog reconciler); v1 cannot run anything because the plugin ABI is a pure document transformer

### Summary
The plugin host is a sandboxed pure function: `Plugins::run(name, input, params) -> Value` over one document, fuel- and memory-capped, with no host imports (`crates/core/src/plugin.rs:73-136`). M28 shipped exactly as far as that ABI allows — `[plugins] app_dir` modules that export `describe()` are listed in `GET /apps` as `dynamic: true, runnable: false`, with "executing dynamic apps needs the component-model host (next slice)" written into the config doc (`crates/core/src/config.rs:1673-1678`). Everything a real app needs is already a method on `AppContext` — metered `fetch`/`research`, `upsert_many`/`sync_many` with health gating, `checkpoint`/`restore`, `save_artifact`, `progress`, `observe_extraction` (`crates/core/src/app.rs:110-737`). The moonshot is to expose that facade as WIT host imports so a WASM module *is* a `ScrapeApp`: dropped into a directory, registered in the catalog, scheduled, budgeted, replayable, and health-monitored, with no Rust build.

### Description
- **Every invariant comes for free by construction.** The dependency rule says apps depend only on `core` (`CLAUDE.md` architecture section) — a WASM app can depend on nothing else. The research chokepoint is enforced by field privacy today (`crates/core/src/engine.rs:1243-1284`); a host import table enforces it absolutely. Raw-engine bypasses (17 apps in `REPLAY_BYPASS_APPS`, `crates/core/src/vcr.rs:216-313`) cannot exist in a WASM app unless the host exports them, so VCR replay is `Full` for the whole class by default (`vcr.rs:171-185`) and `crates/core/tests/fetch_chokepoint.rs`'s inventory never grows.
- **Cost and safety are already metered.** `PluginRunStats` reports fuel and memory per call (`plugin.rs:31-71`), `PluginFailure` classifies traps vs missing exports vs host errors (`crates/core/src/error.rs:107-151`), and `max_concurrent` bounds admission (`config.rs:1666-1672`). A long-running app needs a fuel budget per *job* and yields at host calls — the async host-call boundary is where fuel is re-armed and the job deadline/cancel are checked.
- **Manifest is the contract.** `AppManifest` (params JSON Schema, examples, output shape, cost class — `app.rs:890-914`) is what `describe()` already returns for v1; v2 validates examples against the schema at load, exactly as the server test does for Rust apps.
- **Catalog closes the loop.** A `[[source]]` row with `engine = "wasm"` and a module hash makes the reconciler (`crates/core/src/catalog.rs:514-631`) the deployment tool: desired state = TOML + module, actual = loaded module hash + schedule row; drift is a plan entry.
- **Agent-authored sources become deployable.** M44's provisioner produces proposals; with WASM apps a proposal can compile (Rust -> wasm32 in CI, or a tiny DSL -> wasm) into a runnable unit that the catalog reconciler installs — the missing last mile between "speak a source into existence" and "it runs nightly".
Registry subjects: `companion-runtime`, `module-design`, `usage-limit-governance`, `conformance-checking` (the engine-conformance suite already pins the Rust traits; the WIT world gets the same suite).

### Flow
- Define `pumper:app` WIT world: imports `fetch`, `research`, `upsert-many`, `sync-many`, `checkpoint`, `restore`, `save-artifact`, `progress`, `observe-extraction`, `require-str`; exports `describe`, `run`.
- Component-model host in `engine-wasm` with async host calls, per-job fuel budget re-armed per call, cancellation/deadline checked at every import, `max_concurrent` admission shared with plugins.
- `DynamicApp: ScrapeApp` adapter in the server registry; manifests validated at load; `runnable: true` in `GET /apps`.
- Catalog: `engine = "wasm"` + `module_sha256`; reconciler plans install/upgrade; `just plugin` gains an app target.
- Conformance suite for the WIT world (the existing engine-conformance pattern), plus VCR record/replay test proving a WASM app is `ReplayFidelity::Full`.
- SDK/provisioner: a `wasm-app` template crate under `plugins-src/` and a proposal -> module path.

### Expected impact
Adding a use case stops being a Rust crate + registry edit + rebuild + redeploy: a module and a TOML row. Every such app inherits budgets, health gating, checkpoints, VCR and the catalog for free, and cannot bypass the chokepoints. Measured by dynamic apps running on schedule, and by the raw-engine inventory staying flat while the app count grows. What could break: a hot-swapped module changing a dataset's shape mid-history — provenance stamps the module hash as `rules_hash`, so the era is identifiable, and contracts (M20) gate publish.

### Evaluation
Claim: other - a new scraping use case can be deployed without a Rust build, with every platform invariant enforced by the host boundary
Before: dynamic apps are `runnable: false` (`config.rs:1673-1678`); the plugin ABI has zero host imports (`plugin.rs:73-136`); 17 Rust apps bypass the fetch chokepoint (`vcr.rs:216-313`)
After: number of runnable dynamic apps; chokepoint bypass inventory size for the WASM class (must be 0); fuel/memory per job
Method: probe - read the plugin trait, the config's next-slice note, the AppContext facade and the replay-bypass table
Result: unmeasurable (moonshot) - instrument is the conformance suite plus `GET /apps` runnable counts
Gate: contract

### Evidence

```
crates/core/src/plugin.rs:31-71 (PluginRunStats fuel/memory), :73-136 (Plugins trait: run(name,input,params) only, no host imports)
crates/core/src/config.rs:1657-1692 (PluginConfig; app_dir = describe-only, 'next slice')
crates/core/src/app.rs:110-737 (the AppContext facade to export), :890-914 (AppManifest), :916-957 (ScrapeApp trait)
crates/core/src/engine.rs:1243-1284 (researcher chokepoint by field privacy)
crates/core/src/vcr.rs:171-185 (ReplayFidelity), :216-313 (17 raw-engine apps)
crates/core/src/error.rs:107-151 (PluginFailure classes)
crates/core/src/catalog.rs:514-631 (reconcile_plan as the deployment diff)
```

