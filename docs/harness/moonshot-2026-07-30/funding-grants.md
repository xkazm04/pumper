# Moonshot Scan — Public Funding & Grants Apps (2026-07-30)

> Total: 4 moonshots across 2 contexts.

## Context: EU & Regulatory Funding Watchers

### 1. Win-intelligence layer: CORDIS funded-outcomes joined onto every open SEDIA topic
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/eu-sedia/src/lib.rs, crates/apps/grants-common/src/lib.rs
- **What it is**: eu-sedia already harvests the full pan-EU open-calls corpus (identifier, callIdentifier, frameworkProgramme, typesOfAction, budgetOverview). CORDIS publishes the *outcomes* side as key-free open data: every funded Horizon project with its topic identifier, EU contribution, coordinator, and all participants. A new `cordis` app ingests the bulk dumps, and a join layer annotates each open topic with its predecessor-topic history — how many projects were funded, at what average grant size, by what consortium shapes, led by which organisations. Horizon topic identifiers encode lineage (HORIZON-CL4-2026-… descends from …-2024-…), so predecessor matching is a string-grammar problem, not ML.
- **Why it's a moonshot**: it converts an open-calls mirror (a commodity — the portal itself shows open calls) into the only place where an applicant sees "topics like this funded 7 projects at ~€4.2M each; 60% of winning consortia had a Fraunhofer-class RTO". That evidence layer is what EU grant consultancies charge five figures for, and it makes every downstream play (fit-scoring, alerts, drafting) 10x more credible because recommendations carry priors, not just metadata.
- **Differentiation**: nearest priors are #239 "EU consortium partner matchmaking engine" (matches partners, no outcomes data) and US-context #178 "Past-winner enrichment via USAspending join" (US awards only). No prior EU-context idea touches CORDIS or any funded-outcomes source.
- **Path**:
  1. Add a `cordis` ScrapeApp fetching the CORDIS Horizon Europe projects JSON/CSV bulk dump (key-free HTTP, fits the existing http-engine fast-path pattern of grants-gov/ca-grants) into a `projects` dataset keyed by project RCN.
  2. Extract a pure `topic_lineage(identifier) -> family_key` function in eu-sedia (strip year + counter from the identifier grammar) with unit tests, mirroring the cms-fee-schedule `parse_release` style.
  3. Aggregate CORDIS projects per topic-family: count, total/mean EU contribution, participant-org frequency table — store as `topic_stats` records.
  4. In eu-sedia's run (or a dataset-trigger DAG on `opportunities` deltas — trigger pipelines shipped 2026-07), join `topic_stats` onto each normalized record as a `history` block; surface via the existing `?filter=` query surface.
  5. Add per-organisation rollups (org → topics won, total funding) as a second dataset — the seed of an EU funding league table product.
- **Risks**:
  - CORDIS bulk dumps are large (hundreds of MB CSV) — may need streamed/chunked ingest and the binary/large-body handling the http engine currently lacks (the ZIP variants do; the JSON REST extraction API is a fallback).
  - Topic-lineage grammar has exceptions across programmes (Erasmus+, LIFE differ from Horizon) — start Horizon-only, where identifiers are most regular.

### 2. Medicare price oracle: graduate cms-fee-schedule from release-watcher to reference-data owner
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/cms-fee-schedule/src/lib.rs
- **What it is**: today the app deliberately stops at "a newer RVU release exists, here is the zip_url" because the http engine yields a String body and the heavy parse lives in Counterbill's ingest script. The moonshot inverts ownership: Pumper downloads the RVU ZIP itself, parses the PPRRVU CSV in Rust (RVU components, conversion factor, GPCI), stores a per-HCPCS-code `fee_schedule` dataset, and serves lookups plus **release-over-release price diffs** ("RVU26B changed 1,842 codes; cardiology down 2.3%") through the existing dataset/`?filter=`/search surfaces and trigger DAGs.
- **Why it's a moonshot**: the platform stops being a doorbell for someone else's database and becomes the database. Any billing/pricing product (not just Counterbill) can consume Medicare reference prices as an API, and the diff feed — "what did CMS just change, by code and specialty" — is a publishable intelligence product the moment a quarterly release drops. One app upgrade turns a freshness signal into a healthcare-pricing data business.
- **Differentiation**: prior #258 "Fee-schedule change impact simulator" simulates impact against the *caller's* baked data and #199 "Self-updating reference tables via auto-regen loop" auto-drives Counterbill's regen script — both keep the parsed data outside Pumper. No prior idea makes Pumper itself parse, store, and serve the fee-schedule corpus with code-level diffs.
- **Path**:
  1. The watcher already emits a structured `ingest` block (release, zip_url) designed for dataset triggers — wire a trigger DAG from `releases` freshness to a new `ingest-pfs` job (scaffold exists today).
  2. Add bytes-body support to the http engine (the known engine-traits binary-body gap — this is its first concrete paying customer) or, interim, shell the download through the existing artifact store.
  3. Parse PPRRVU CSV inside the ZIP (zip + csv crates; pure function, golden-file tested like `detect_releases`) into `{hcpcs, modifier, work_rvu, pe_rvu, mp_rvu, conversion_factor}` records keyed by code+release.
  4. Upsert into a `fee_schedule` dataset; upsert_many change detection gives the per-code diff feed for free.
  5. Expose "what changed in RVU26C" as a filterable dataset + closing the loop: emit a webhook payload Counterbill can ingest directly instead of re-parsing.
- **Risks**:
  - Binary-body engine work is a deferred architectural item — the interim artifact-store route must not become permanent plumbing.
  - RVU file layout shifts between years (column reshuffles); pin per-release column maps and fail loudly on drift, as the app already does for the index page.

## Context: US Grant Opportunities

### 1. NOFO document intelligence: from opportunity listings to the full announcement corpus
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: quarters
- **Files**: crates/apps/grants-gov/src/lib.rs, crates/apps/grants-common/src/lib.rs
- **What it is**: Search2 returns listing stubs — title, agency, dates. The substance of a federal grant (eligibility fine print, cost-share/match requirements, evaluation criteria, award floors/ceilings, page limits) lives in the fetchOpportunity detail record and the attached NOFO PDFs, which nothing in the platform touches. This moonshot adds a detail-harvest stage: for each new/changed opportunity, call the (also key-free, POST-JSON) fetchOpportunity endpoint, store the full synopsis + attachment manifest, pull the NOFO documents, and extract a structured requirements block — making Pumper the only queryable corpus of what federal funding announcements *actually say*.
- **Why it's a moonshot**: every serious grants product (fit-scoring, drafting, alerts) dies on the gap between "an opportunity exists" and "here is what it takes to win it". A structured NOFO corpus is that bridge, and full-text search over announcement bodies ("cost sharing not required" AND "tribal") is a query no one — including Grants.gov — offers today. It is also the grounding layer the prior-backlog application-generator idea silently presupposes but nothing provides.
- **Differentiation**: #36 "Metered grants-corpus API" sells the *listing* corpus; #217 "Award-amount extraction" parses fields already present in hits; #120 "Autonomous first-draft application generator" consumes documents it has no source for. No prior idea harvests fetchOpportunity details or NOFO attachments. (A gated live-shape question on fetchOpportunity exists in the perf-campaign backlog — this is the product answer to why that shape matters.)
- **Path**:
  1. Add a `detail` stage to grants-gov gated on `summary.new`/`summary.changed` keys (already returned by upsert_many), calling fetchOpportunity for just the delta — tens of calls/day, not 25k.
  2. Store detail records in an `opportunity_details` dataset keyed by opportunity id; index synopsis text into tantivy search (recency + facets already shipped).
  3. Harvest attachment manifests (filenames, download ids) as structured data even before PDF handling lands.
  4. Add PDF text extraction behind the binary-body capability (shared blocker with the CMS oracle — one engine investment, two flagship consumers); use the Claude engine tier for born-scanned PDFs.
  5. Extract a typed requirements block (match %, award ceiling, eligibility codes, LOI required) with a declarative/LLM hybrid, validated against the listing fields as ground truth.
- **Risks**:
  - Per-opportunity detail calls multiply request volume — must stay delta-driven and politeness-governed or the daily sweep budget blows up.
  - NOFO PDFs are unstructured and agency-idiosyncratic; ship manifest + full-text first (already valuable), structured extraction second.

### 2. Amendment radar: typed lifecycle events for every grant, feeding the trigger mesh
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/apps/grants-gov/src/lib.rs, crates/apps/ca-grants/src/lib.rs, crates/apps/grants-common/src/lib.rs
- **What it is**: upsert_many already knows *that* a record changed; nothing knows *what the change means*. This adds a semantic diff layer over `grants/unified`: compare prior vs new record on load-bearing fields and emit **typed lifecycle events** — `deadline_extended` (closeDate moved later), `deadline_accelerated`, `forecast_posted` (oppStatus flip), `award_raised`, `reopened`, `closed_early` — persisted as an `events` dataset and fanned out through the shipped reactive trigger DAGs and webhook DLQ. Every grant becomes a versioned timeline; the corpus becomes an event stream.
- **Why it's a moonshot**: a deadline extension is the single highest-value signal in grants — it converts "we missed it" into "we can still win it" — and no US aggregator publishes extensions as a feed. Typed events also make the platform composable in a way raw diffs never are: "notify me on deadline_extended where agency=NSF" is a one-line trigger, and the event history compounds into agency-behavior data (who habitually extends) that raw snapshots cannot reconstruct.
- **Differentiation**: EU-context #107 "Field-level change diffs on watched items" proposes raw field diffs on *watched EU items*; US #80 "Natural-language funding alerts" is a query/alert UX over the change feed. Neither defines a semantic event taxonomy, a persisted per-opportunity timeline, or lifecycle state transitions — no prior US-context idea touches amendment semantics at all.
- **Path**:
  1. In `grants_common::finalize_unified`, fetch the prior unified record for each changed key (the upsert path already reads it for change detection) and pass old/new pairs to a pure `classify_events(old, new) -> Vec<GrantEvent>` — unit-testable like `closing_soon_digest`.
  2. Define the v1 taxonomy on fields the unified schema already normalizes: close date, status, award amount — 6 event types, each with `{opportunity_key, source, kind, before, after, observed_at}`.
  3. Upsert events into a `grants/events` dataset keyed by `{key}:{observed_at}:{kind}` — append-only timeline, queryable via `?filter=`.
  4. Wire a dataset trigger on `grants/events` freshness so existing webhook subscribers get typed payloads with zero new delivery code.
  5. Later: per-agency extension-rate rollups from the accumulated event history — the empirical feed for the prior "agency behavior" idea, built on observed events instead of forecast snapshots.
- **Risks**:
  - Source data glitches (date format hiccups, temporary field blanking) can fire false events — require both values parseable and debounce flip-flops within a run.
  - Event volume is unbounded over years; keyed append-only records need the dataset-store retention/prune API (already shipped) configured from day one.
