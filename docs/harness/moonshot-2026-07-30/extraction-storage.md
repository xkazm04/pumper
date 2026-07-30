# Moonshot Scan — Data Extraction & Storage (2026-07-30)

> Total: 6 moonshots across 3 contexts.

## Context: Broad Crawler

### 1. Learned change-cadence scheduler: crawl the web at the rate it changes
- **Tier**: 1
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/core/src/crawl.rs (revisit mode, `RevisitSeed`, `unchanged_304`/`gone` stats, conditional-GET path)
- **What it is**: Every revisit already produces a per-URL observation stream: `304` (unchanged), changed body, or gone — with timestamps in the `pages` dataset revisions. Fit a per-URL change-rate estimator (Poisson / exponential-decay of observed change intervals) and replace the flat "revisit everything" frontier with a priority frontier that spends the `max_pages` budget on the URLs most likely to have changed since last visit. High-churn pages get hourly revisits; static pages get monthly ones — automatically, per URL, with zero configuration.
- **Why it's a moonshot**: This is the difference between a recrawler and a freshness engine. For a fixed fetch budget it can deliver an order of magnitude more *detected changes per fetch* — the metric every monitoring customer actually buys. It turns pumper's sentinel mode into the same class of system Google's scheduler is: budget allocation driven by learned per-document change probability.
- **Differentiation**: Prior idea #8 "Site-change sentinel: crawl + diff = monitoring product" proposed the sentinel itself — which has since shipped (revisit + conditional GET + gone markers). Nothing in the backlog learns *when* to revisit; the cadence model over the 304/changed history is entirely new ground.
- **Path**:
  1. Extend `RevisitSeed` with `last_change_at` / `observed_interval_secs` read from the existing `pages` revisions (the data is already stored; only the read query is new).
  2. Add a `due_score(now)` function (simple estimator: probability changed since last fetch) and sort/filter revisit seeds by it before `frontier.push`.
  3. Persist per-URL `(checks, changes, last_change_at)` counters on the `CrawlPageRecord` so the estimator improves every run without a new table.
  4. Expose `revisit_budget` + `min_due_score` in `CrawlConfig`; report `skipped_not_due` honestly in `CrawlStats`.
  5. Later: per-host cadence priors (new URLs inherit their host's rate) and a `/apps/crawler/freshness` report ranking hosts by churn.
- **Risks**:
  - Estimator cold-start: first two visits carry no interval signal — needs a sane prior (host-level default) or it degenerates to the flat schedule.
  - Sites that churn boilerplate (rotating ads) look hot; mitigate by scoring change on the existing SimHash distance, not raw body inequality.

### 2. Persist the link graph: turn every crawl into a queryable site atlas
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/core/src/crawl.rs (`extract_links`, `parse_page`, `PageSink` seam)
- **What it is**: The crawler already extracts, canonicalizes, and filters every outbound link — then throws the edges away after enqueueing. Stream `(from_url, to_url, depth)` edges through a second sink into an `edges` dataset alongside `pages`. That single retained byproduct unlocks a graph product: in-degree/PageRank-style importance per page, orphan and dead-link detection (edges pointing at `gone` pages), site-structure maps, and "what links to the page that just changed" blast-radius queries for the trigger DAG.
- **Why it's a moonshot**: Every competitor sells page content; almost none sell site *structure*. Link-graph queries (broken-link audits, internal-linking SEO reports, change blast-radius) are standalone SKUs that cost pumper nothing to acquire — the data is currently computed and discarded on every crawl. It converts one crawl into two datasets and three product surfaces.
- **Differentiation**: No prior idea touches link structure. The nearest, #260 "On-crawl semantic router to auto-file pages by topic," classifies page *content*; this persists page *relationships*. #8's sentinel diffs individual pages; blast-radius over edges is a graph question the backlog never asks.
- **Path**:
  1. Add an `EdgeSink` (or widen `PageSink` batches with an `edges` field) fed from `Fetched.links` before the depth-budget check — one struct + one buffer, same stride pattern as `PAGE_SINK_STRIDE`.
  2. App layer upserts edges into an `edges` dataset keyed `from|to` (dedup free via the store's change detection).
  3. Ship `GET /apps/crawler/graph?host=` returning top pages by in-degree + dead links (join edges against `gone` pages).
  4. Add a broken-link report: edges whose target is `gone` or never fetched with status ≥ 400.
  5. Later: feed in-degree back into the frontier as a best-first score (prior idea #211 wanted scoring but had no signal to score with — the graph supplies it).
- **Risks**:
  - Edge volume is O(links) not O(pages) — needs a per-crawl edge cap and `from|to` dedup in the buffer to keep dataset writes sane.
  - Cross-host edges on `same_domain` crawls are truncated views; report coverage honestly like `frontier_dropped` does.

## Context: Declarative Extraction Engine

### 1. Zero-shot wrapper induction: mine RuleSets from the corpus, no LLM, no demonstrations
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: quarters
- **Files**: crates/core/src/extract.rs (`Rule::Each`, compile pipeline), crates/core/src/simhash.rs (`dom_simhash` shape fingerprint)
- **What it is**: Given a cluster of same-template pages (already identifiable: `dom_simhash` groups pages by markup shape), statistically induce a `RuleSet` — find the repeating DOM container (the `Each` selector), then the slots inside it whose *text varies across items while structure stays fixed* (the fields). Classic RoadRunner-style wrapper induction, executed in Rust over the crawler's stored artifacts, emitting a plain declarative `RuleSet` a human can review and the existing engine runs at full speed.
- **Why it's a moonshot**: It removes the only remaining human step between "crawl a site" and "typed dataset." Point pumper at 50 crawled listing pages and it hands back a working extractor — for free, deterministically, with zero token cost, at corpus scale. That's a category shift: from an extraction engine you configure to a system that configures itself.
- **Differentiation**: The backlog attacks this problem only via LLMs or humans — #143/#27 "NL→RuleSet / AI rule-writer" (Claude drafts rules), #122 "extraction-by-demonstration" (human clicks examples), #123 "self-healing selectors" (repairs existing rules). No prior idea induces rules *statistically from the page corpus itself*; the mechanism (cross-page DOM alignment, no LLM, no demonstrations) is absent from all 54 rows.
- **Path**:
  1. Ship a `suggest` module that takes N documents and finds candidate `Each` containers: elements whose `(tag, classes)` signature repeats ≥ k times per page across most pages (the `dom_simhash` token stream already computes these signatures — reuse `build_hash_stem`).
  2. Within the winning container, enumerate leaf paths; keep paths present in most items whose text differs across items → field candidates, named by class/tag heuristics.
  3. Emit a `RuleSet` (Each + css fields) and validate it immediately with `extract_batch_with_report` over the same corpus — accept only if match-rate clears a threshold; attach the DocReport as evidence.
  4. Expose `POST /extract/suggest {app, dataset|artifact_dir}` returning the induced RuleSet + per-field match stats for human review.
  5. Later: type the fields automatically by trial transforms (`to_number` coercion success rate picks price-like fields — `CoercionStatus` already measures exactly this).
- **Risks**:
  - Messy real-world templates (interleaved ads, A/B variants) fragment the container signature; needs the cluster step to be strict and the acceptance threshold honest.
  - Field *naming* is heuristic; ship as reviewable suggestions, never silently deployed rules.

### 2. Extraction time machine: replay every rule edit against the archived web before it ships
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/core/src/extract.rs (`extract_batch_with_report`, `FieldStatus`, `CoercionStatus`), crates/core/src/crawl.rs (URL-addressed `artifact_name` body store)
- **What it is**: The crawler now stores page bodies URL-addressed on disk, and the engine already emits per-field match/coercion reports. Combine them into a CI-for-scrapers harness: any proposed RuleSet edit is replayed over the historical artifact corpus (thousands of real pages, all cores via rayon) and diffed field-by-field against the current rules' output *before* deployment. Bonus mode: when a field's live match-rate drops, auto-bisect across dated artifacts to answer "this selector broke between the 07-22 and 07-24 snapshots — here is the DOM diff."
- **Why it's a moonshot**: Scraper maintenance is the industry's dominant cost, and everyone tests rules against *one* live page. Deterministic regression replay against an archived corpus makes rule changes as safe as code changes with tests — a claim no scraping product makes. It also converts the artifact directory from dead weight into the engine's test fixture library.
- **Differentiation**: #123 "self-healing selectors" auto-*repairs* after breakage and #121 "per-field confidence" *scores* live extractions; neither validates a rule change pre-deploy nor replays history. No prior idea uses the stored artifacts as a regression corpus at all (URL-addressed artifacts only landed post-scan).
- **Path**:
  1. Add `POST /extract/replay {rules, rules_base?, app, sample?}`: load N artifacts, run both rule sets via `extract_batch_with_report`, return per-field deltas (match-rate, coercion-rate, value diffs) — everything needed already exists in `DocReport`.
  2. Wire it into the existing preview endpoint UX as a "validate against corpus" step.
  3. Persist replay verdicts per rules-hash so re-validations are incremental.
  4. Add the bisect mode: keep dated artifact generations on revisit (suffix by fetch date instead of overwriting), walk them for the first snapshot where `FieldStatus` flips to `Empty`.
  5. Gate app rule updates on a passing replay (opt-in `strict` flag).
- **Risks**:
  - Bisect requires retaining artifact history — disk growth needs a per-URL generation cap (keep last k bodies).
  - Corpus replay can bless a rule that only works on stale markup; always pair the verdict with a small live-fetch sample.

## Context: Dataset Store & Change Detection

### 1. Derived datasets: an incremental dataflow layer where datasets compute datasets
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: quarters
- **Files**: crates/core/src/datasets.rs (`UpsertSummary`, revisions/change feed, `JsonFilter`), crates/core/src/storage.rs (trigger lineage: `trigger_id` on jobs)
- **What it is**: Let a dataset be *declared* as a transformation of other datasets — filter/map/join/aggregate specs stored like RuleSets — and recomputed **incrementally** on each upstream delta by riding the shipped trigger DAG: an upstream `UpsertSummary`'s fresh keys drive recomputation of only the affected derived rows, which produce their own change events, which can feed further derivations. The store stops being a terminal sink and becomes a mini incremental dataflow engine (a Materialize-shaped core on SQLite).
- **Why it's a moonshot**: Today every domain app hand-writes its join/aggregate logic in Rust (census blend, trades operator-economics, skill-demand aggregations — all bespoke). A declarative derived-dataset layer collapses that entire class of code into config, and makes the pipeline composable by users, not just by developers: "grants closing this month joined with agency stats" becomes a POST, not a crate. That is the moment pumper turns from a scraping platform into a data platform.
- **Differentiation**: Nearest priors are #210 "Queryable dataset filtering" (read-time filters — shipped as `?filter=`) and #165 "NL querying" (query-time, LLM). Neither creates *persistent, incrementally-maintained* datasets from other datasets; nothing in the backlog composes datasets at all. The shipped trigger DAG fires *jobs* on deltas — this makes deltas produce *data*, a qualitatively different layer on the same substrate.
- **Path**:
  1. Define a `DerivedSpec` (source dataset(s), `JsonFilter` predicate, field projection/rename, optional group-by count/sum) stored in a `derived` table; v1 = filter+project of a single source.
  2. On `upsert_many`, after computing `UpsertSummary`, feed fresh keys through matching specs and upsert results into the derived dataset inside the same flow (change detection dedups no-op recomputes for free).
  3. Backfill command that materializes a new spec over the existing source rows.
  4. Add single-key join (`lookup` from a second dataset by key expression) — covers the census/trades enrichment shape.
  5. v2: group-by aggregates maintained via delta-recompute of only affected groups; cycle detection across specs (reuse the trigger DAG's guard).
- **Risks**:
  - Incremental aggregate maintenance on removals/changes is genuinely hard — v1 must stay in the filter/project/lookup subset where per-key recompute is exact.
  - Cascading specs amplify write load; depth cap and per-spec kill-switch from day one.

### 2. Reproducible records: a provenance ledger that can re-materialize any row from source
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: months
- **Files**: crates/core/src/datasets.rs (`Revision`, trust column), crates/core/src/storage.rs (jobs: `schedule_id`, `trigger_id` lineage already stored)
- **What it is**: Stamp every revision with its full derivation: producing `job_id` (which already carries schedule/trigger lineage), source URL + fetched-artifact hash, and the extraction rules-hash that produced the value. Then expose `GET /provenance/{app}/{dataset}/{key}`: the complete chain "this field value came from this URL, fetched at this time by this job, extracted by rules vX, body sha256 …" — and a `re-derive` action that replays the stored artifact through the stored rules and verifies the record reproduces bit-for-bit.
- **Why it's a moonshot**: Scraped data is unverifiable by reputation — pumper can make it verifiable by construction. "Every record in this feed is cryptographically traceable to an archived source document and reproducible on demand" is a claim worth an enterprise price tier (compliance, journalism, financial-data buyers), and no scraping vendor makes it. The trust column already grades records; provenance is what makes the grade *auditable*.
- **Differentiation**: #88 "Compliance-grade crawl audit trail" is host-level crawl *logs* (what was fetched when, robots compliance) in the crawler context; this is record-level *lineage inside the store* with replayable verification — a different table, a different query surface, and a reproducibility guarantee no prior row mentions. #121 "per-field confidence" scores plausibility; this proves origin.
- **Path**:
  1. Add nullable `job_id`, `source_ref`, `rules_hash`, `artifact_sha` columns to revisions (NULL-means-unknown, exactly the `trust` migration pattern already proven in this file — no backfill needed).
  2. Thread the producing job's id through `upsert_trusted` (the app context already knows it); apps that fetch pass URL + body hash.
  3. Ship the `/provenance` endpoint walking the revision chain with its stamps.
  4. Add `POST /provenance/verify`: load the artifact by hash, re-run the stored rules-hash's RuleSet, compare against the revision snapshot — emit `reproduced | diverged | source_missing`.
  5. Surface a per-dataset provenance-coverage metric (fraction of rows fully stamped) as the marketable number.
- **Risks**:
  - Only as strong as artifact retention — needs a retention policy that keeps bodies referenced by live revisions (the store's prune API must learn about artifact refs).
  - Rules evolve; verification must pin the *historical* RuleSet by hash, which requires storing rule versions, not just current app config.
