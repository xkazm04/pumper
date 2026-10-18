# Grants Intelligence — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## GI1 — NOFO document corpus: fetch attachments, extract text, type the requirements

- deck item **N28**
- lens: `feature-scout` · size: **XL** · gate: **contract** · effort 8 / impact 9 / risk 6
- contexts: us-federal-grants, grants-unified-layer, eu-grants
- extends: M33 (NOFO document intelligence, shipped as listing + detail record + attachment MANIFEST only) on top of the shipped HttpClient::fetch_bytes binary seam

### Summary
M33 shipped the detail record and an attachment manifest, then stopped: no NOFO document is ever fetched, the download URL is still marked ASSUMED, and the `requirements` block is built from synopsis fields only. The binary-body blocker that M33 named has since shipped (`HttpClient::fetch_bytes`), so the second half is now cheap: pull the PDFs behind the manifest into a `grants/documents` corpus, index their text, and extract a typed v2 requirements block (page limits, match %, LOI, evaluation criteria) with the synopsis as ground truth.

### Description
What the substrate already does: the detail stage stores full synopsis + manifest per new/changed opportunity (crates/apps/grants-gov/src/lib.rs:29-36), and `attachment_manifest` builds one entry per file with an ASSUMED `download_url` "stored alongside the raw metadata so the later PDF pass can verify" (lib.rs:75-80, 1099-1102, 1475-1528). `requirements_block` is explicitly "SYNOPSIS FIELDS ONLY (v1) - no PDF text is fetched or parsed" (lib.rs:1380-1383) and yields 8 fields (lib.rs:1396-1413). The M33 path step 4 was gated on a binary-body engine capability (docs/harness/moonshot-2026-07-30/funding-grants.md, US context #1, Path 4). That capability exists now: `HttpClient::fetch_bytes` is "the deliberately minimal engine-traits#2-LITE seam" for "ZIP/PDF", hard-capped by `max_body_bytes`, buffered in memory (crates/core/src/engine.rs:1182-1196). The detail harvest is already durable/resumable per key with a checkpointed delta (lib.rs:449-546), which is exactly the loop a document pass slots into. Full-text over documents rides the shipped `index_datasets` delta indexing (docs/features/search.md:21) and saved-search alerting (search.md:100-102). An app calling the Claude tier for born-scanned or prose-heavy PDFs has precedent (crates/apps/connector-api-watch/src/lib.rs:484 uses `engines.claude`). On the EU side, eu-sedia already carries the topic text (`descriptionByte`, capped `description_text` at 2000 chars, crates/apps/eu-sedia/src/lib.rs:534-537, 559-561), so the same `grants/documents` shape can hold the uncapped Horizon topic body and give the corpus one document surface across sources. Why v1 is insufficient: a fit-scoring or drafting consumer cannot answer "cost sharing not required AND tribal AND 15-page limit" from 8 synopsis fields, and the manifest URL has never been exercised, so the manifest is a promise with no verified path behind it.

### Flow
- Verify the ASSUMED `ATTACHMENT_DOWNLOAD_BASE` live against one stored manifest entry; keep the raw first response as an artifact (the crate's existing first-live-run discipline, lib.rs:57-58).
- Add a `harvestDocuments` stage after the detail flush: for each new/changed detail with a manifest, `fetch_bytes` each attachment under a per-file `max_body_bytes`, capped per run like `maxDetailsPerRun`, checkpointed per key the way `harvest_state` is (lib.rs:1123-1140).
- Extract text in Rust (pdf-extract / lopdf; DOCX via zip+xml) into `grants/documents` keyed `{unified_key}:{attachment_id}` with `{text, pages, sha256, mime, source_url, extracted_by}`; born-scanned PDFs (no text layer) route to the Claude tier behind a budget, or store `text: null` with the reason.
- Declare `grants/documents` in `index_datasets` so every document is a tantivy doc; add `document_id` facets so `GET /search` answers phrase queries over announcement bodies.
- `requirements_v2`: declarative regex/rule pass over document text for page limit, match percentage, LOI required, submission system, evaluation criteria headings; validate money/date fields against the synopsis block (ground truth) and record disagreement counts in the result.
- Mirror for EU: store the uncapped `descriptionByte` as a `grants/documents` row for Horizon topics so the document surface is cross-source.
- Feature doc + catalog `[[source]]` for `grants-documents`, contract `required_fields = [unified_key, sha256]`.

### Expected impact
Grant-writing consumers (the sibling grant-writing app the eu-sedia header names, lib.rs:7-9) stop at "an opportunity exists" today; this makes Pumper the only queryable corpus of what federal announcements actually say. Measured by: documents stored per day, share of new opportunities with at least one extracted document, and requirements_v2 field fill rates versus v1. What could break: per-run request volume and memory (in-memory fetch_bytes of multi-MB PDFs), and a wrong download-URL assumption discovered only live.

### Evaluation
Claim: user - answers eligibility/requirement questions from announcement text, not synopsis stubs
Before: 0 documents fetched; `requirements` = 8 synopsis-derived fields (grants-gov lib.rs:1396-1413); download URL ASSUMED (lib.rs:1099-1102)
After: `grants/documents` row count and requirements_v2 fill rate per field, reported in the run result like `amountsFilled`
Method: probe - read the detail stage, the manifest builder and the fetch_bytes seam; no live call made
Result: unmeasurable (instrument: `documentsHarvested`, `requirementsV2Filled` in the grants-gov result and a doc-count facet on /search)
Gate: contract

### Evidence

```
crates/apps/grants-gov/src/lib.rs:35-36 ("v1 does NO PDF fetching or parsing"), :75-80 and :1099-1102 (download URL ASSUMED, never fetched), :1380-1383 and :1396-1413 (requirements block = synopsis fields only), :449-546 (checkpointed detail harvest loop to reuse)
crates/core/src/engine.rs:1182-1196 (fetch_bytes: raw binary body for ZIP/PDF, max_body_bytes-capped, in-memory)
crates/apps/eu-sedia/src/lib.rs:534-537, :559-561 (topic HTML kept; plain text capped at 2000 chars)
docs/features/search.md:21 (index_datasets delta indexing), :100-102 (saved-search alerts)
crates/apps/connector-api-watch/src/lib.rs:484 (app-level Claude-tier call precedent)
docs/harness/moonshot-2026-07-30/funding-grants.md US #1 Path 4-5 (the unshipped half of M33)
```

## GI2 — Program registry: the funding program, not the posting, as the unit of intelligence

- deck item **N29**
- lens: `moonshot-architect` · size: **L** · gate: **contract** · effort 6 / impact 8 / risk 3
- contexts: grants-unified-layer, us-federal-grants, us-state-grants, eu-grants
- extends: M34 (amendment radar / grants/events), the shipped recurrence relation (grants/recurrence_links) and M31 (cordis/topic_stats)

### Summary
Three shipped seams each know something about a *program* but none of them stores the program: `recurrence_links` holds pairwise `a|b` rows with a period and a predicted next window, `grants/events` is an append-only timeline whose own doc-comment says the per-agency extension-rate history "IS the product", and cordis rolls Horizon outcomes up per topic family. There is no row anyone can `GET` for "Rural Health Network Development" or "HORIZON-CL4-DATA-01". This card materializes `grants/programs` in the once-per-cycle corpus pass and exposes it, turning three relations into one queryable entity with recurrence, amendment behaviour and win history on it.

### Description
Evidence the pieces exist and stop short: `link_relations` groups recurrence pairs by `program_title` into chains and computes a `RecurrenceProjection` (period, cycles, next_expected_open/close, basis) but writes it *per pair* into `grants/recurrence_links` keyed `a|b` (crates/apps/grants-common/src/lib.rs:1540-1591) - the program-level projection is recomputed and then thrown away as an entity. `EVENTS_DATASET` is documented as accumulating for years because "they ARE the product - per-agency extension-rate history" (lib.rs:51-52), yet the only occurrence of "extension-rate" in the crate is that comment; no rollup reads `grants/events` (grep over grants-common and routes/query.rs). M34's own path listed "per-agency extension-rate rollups from the accumulated event history" as step 5 (docs/harness/moonshot-2026-07-30/funding-grants.md US #2, Path 5) and it did not ship. On the EU side `aggregate_topic_stats` already IS a program rollup keyed by `topic_lineage` family (crates/apps/cordis/src/lib.rs:971-1057) with count, mean contribution, top participants and a `coverage` block - but it lives under `cordis/topic_stats`, is joined only onto eu-sedia rows (crates/apps/eu-sedia/src/lib.rs:311-333), and has no US sibling keyed by ALN. The schema already carries the identity signals: `aln` is described as "the one field explicitly designed to stay constant across a program's annual cycles" (grants-common lib.rs:1209-1213) and `program_title` strips year tokens (lib.rs:1341-1362). The corpus pass is claimed once per UTC day by whichever producer arrives first (lib.rs:283-291, 345-358), so a program rollup slots in beside `sweep_closed`/`link_relations` at no new scheduling cost. The query surface is one route file with `/grants` and `/grants/closing-soon` (crates/server/src/routes/query.rs:144-190).

### Flow
- Define program identity as a pure fn `program_key(unified_row) -> Option<String>`: federal `aln:<ALN>` when present, else `<agency-norm>|<program_title>`; Horizon `family:<topic_lineage>`; tested like `classify_relation`.
- In the corpus pass, after `link_relations`, fold the live unified corpus + `recurrence_links` + `grants/events` into `grants/programs` rows: `{program_key, title, agency, source(s), cycles_observed, period_days, next_expected_open/close, prediction_basis, opportunities[] (keys), deadline_extended_count, closed_early_count, extension_rate, last_award_ceiling, award_ceiling_trend, win_history (from cordis/topic_stats where family matches)}`; write with `sync_many` only when the corpus read is complete (the cordis `rollup_is_complete` idiom, cordis lib.rs:955-957), else upsert + warning.
- Stamp a `program_key` onto each unified row as a DerivedPath so a program change never reads as a source publication (the eu-sedia `history` precedent, eu-sedia lib.rs:432-434).
- Add `GET /grants/programs` (filter by agency/aln/source, `next_expected_before=`) and `program=` on `GET /grants`; declare `grants/programs` in `index_datasets` so saved searches can alert on "program X is expected to reopen in 60 days".
- Feature doc + catalog `[[source]]` for `grants-programs` with a contract on `program_key` and `cycles_observed`.

### Expected impact
A grant-seeker's real question is "does this program come back, when, and does this agency move deadlines" - today that requires joining three datasets by hand. The forward calendar (`next_expected_open`) becomes a first-class queryable, and the events timeline finally produces the agency-behaviour data it was justified by. Measured by: programs with >=3 cycles, share of open opportunities with a program row, extension_rate coverage. What could break: a wrong program key merges two programs; keep precision-over-recall (ALN veto, exact stripped-title match) exactly as `classify_relation` does.

### Evaluation
Claim: user - one row per funding program with recurrence, amendment behaviour and win history
Before: recurrence is pairwise `a|b` rows (grants-common lib.rs:1569-1589); events rollup does not exist (comment-only at lib.rs:51-52); EU stats live only on eu-sedia rows
After: `grants/programs` row count and `GET /grants/programs?next_expected_before=` results; extension_rate populated for agencies with events
Method: probe - read link_relations, record_events, aggregate_topic_stats and routes/query.rs
Result: unmeasurable (instrument: `programs: {rows, withProjection, withEvents}` in the corpus-pass block of the result)
Gate: contract

### Evidence

```
crates/apps/grants-common/src/lib.rs:51-52 (events "ARE the product - per-agency extension-rate history"; no rollup exists), :1416-1482 (project_recurrence computes next window), :1540-1591 (projection written per pair into recurrence_links, program entity discarded), :1209-1213 (aln as permanent program id), :283-291 and :345-358 (once-per-cycle corpus pass to host the rollup)
crates/apps/cordis/src/lib.rs:971-1057 (topic_stats = EU program rollup), :955-957 (rollup_is_complete idiom)
crates/apps/eu-sedia/src/lib.rs:311-333 (history join is eu-sedia-only), :432-434 (DerivedPaths precedent)
crates/server/src/routes/query.rs:144-190 (only /grants and /grants/closing-soon exist)
docs/harness/moonshot-2026-07-30/funding-grants.md US #2 Path 5 (rollups named, not shipped)
```

## GI3 — Awards layer: grants/awards + funder-recipient graph from USAspending and CORDIS

- deck item **N30**
- lens: `integration-planner` · size: **XL** · gate: **contract** · effort 9 / impact 9 / risk 5
- contexts: eu-grants, us-federal-grants, grants-unified-layer
- extends: M31 (win-intelligence: CORDIS outcomes joined onto SEDIA topics) - the US half never shipped, and the org side of CORDIS is a bounded leaderboard, not an entity

### Summary
The unified layer normalizes the *open-calls* side across three sources; the *awarded* side exists for the EU only (cordis) and is joined as a per-family stats block. The catalog has carried `usaspending` as a planned awarded-history source ("Funder intelligence: who each agency funded") since the pipeline map was written, and CORDIS already stores per-project participant rosters with per-org EU contribution. This card builds the awards mirror of `grants/unified`: a canonical `grants/awards` dataset (US via USAspending, EU via cordis projects), an entity-resolved `grants/orgs` dataset, and a US `history` block on federal opportunities keyed by ALN - the same product M31 made for Horizon topics, for the source that is ~1400 opportunities to SEDIA's ~600.

### Description
Substrate: cordis `normalize_detail` stores `participants: [{name, ec_contribution, role}]` (bounded to 50) and `coordinator` per project (crates/apps/cordis/src/lib.rs:744-763, 855-864); `aggregate_topic_stats` reduces that to a top-10 org leaderboard per family (lib.rs:1033-1039) - the org is a string, never an entity. eu-sedia joins the family block onto Horizon rows only (crates/apps/eu-sedia/src/lib.rs:311-333); `normalize_grants_gov` has no history and its money is Null until the detail corpus fills it (crates/apps/grants-common/src/lib.rs:360-394, 662-726). The catalog declares `usaspending` (`category = awarded-history`, `dataset = awards`, `status = planned`, key-free, catalog/data-sources.toml:176-186) and `au-grantconnect` with "forecast + current + awarded" (catalog:224-283 region). The federal join key already exists on both sides: unified `aln` from `cfdaList` (grants-common lib.rs:386-387) and USAspending awards carry CFDA/ALN per assistance award. Cross-source org matching can reuse the store's SimHash pairing (`duplicate_pairs`, grants-common lib.rs:1494-1497) over normalized legal names, the same way opportunities are paired today. Currency: unified deliberately leaves EU money Null because "unified has no currency dimension ... Revisit once unified gains a `currency` field" (lib.rs:447-450, 467-470); an awards schema is the natural place to introduce `currency` and then backfill it onto unified. The M31 report already named the org rollup as its step 5 ("per-organisation rollups ... the seed of an EU funding league table product") and it did not ship (docs/harness/moonshot-2026-07-30/funding-grants.md EU #1 Path 5).

### Flow
- New app `usaspending` (http, key-free, monthly + resume cursor like cordis, lib.rs:187-201): POST `/api/v2/search/spending_by_award/` filtered to assistance awards (grants), paged by `page`/`limit`, stored raw in `usaspending/awards` keyed by `generated_unique_award_id`, with a `SweepEnd`-style coverage verdict from `grants_common::walk_end`.
- `grants_common::normalize_award_usaspending` / `normalize_award_cordis` into `grants/awards` keyed `<source>:<award_id>`: `{funder_agency, program_key (aln|family), recipient_name, recipient_id (UEI|PIC), amount, currency, fiscal_year, start/end, place}` - honest-Null money exactly as unified.
- `grants/orgs`: entity resolution over recipient names (normalize legal-form suffixes, SimHash pairs, hard ids UEI/PIC as vetoes) -> one org row with `awards[]`, totals per funder, per program; precision-over-recall like `classify_relation`.
- Federal `history` block: in the corpus pass, aggregate `grants/awards` per ALN (count, mean, top recipients, years) and overlay onto unified rows with matching `aln` as a DerivedPath (the eu-sedia precedent, eu-sedia lib.rs:432-434) so award refreshes are not source news.
- Add `currency` to the unified schema, populate from source (USD/EUR), and lift the EU money that is currently Null.
- Routes: `GET /grants/orgs/{id}`, `GET /grants/awards?program=|recipient=`; feature doc + two catalog sources with contracts.

### Expected impact
Every US opportunity gains what Horizon topics have today: "this program funded N awards at a mean of $X to these recipients". Consultancies sell this; no aggregator ships it beside open calls. Second-order: the org graph is the substrate for consortium/partner discovery (EU) and competitor intelligence (US). Measured by: share of federal open opportunities with a non-empty history block; orgs resolved across both sources. What could break: USAspending volume (millions of assistance awards - must be ALN-scoped to the open corpus, not a full mirror) and false org merges.

### Evaluation
Claim: user - awarded-history priors and recipient graph on the US side, org entities on both
Before: history block exists only for Horizon rows (eu-sedia lib.rs:311-333); usaspending is `status = planned` with no app (catalog:176-186); orgs are strings in a top-10 list (cordis lib.rs:1033-1039)
After: `grants/awards` and `grants/orgs` counts; federal rows with `history.stats.award_count >= 1` queryable via `?filter=`
Method: probe - read cordis normalization/aggregation, the eu-sedia join, the unified normalizers and the catalog entry
Result: unmeasurable (instrument: `awardsJoined` in the corpus-pass result block, mirroring eu-sedia's `historyJoined`)
Gate: contract

### Evidence

```
catalog/data-sources.toml:176-186 (usaspending: awarded-history, dataset=awards, status=planned, "who each agency funded")
crates/apps/cordis/src/lib.rs:744-763 and :855-864 (per-project participant roster with per-org ec_contribution), :1033-1039 (orgs reduced to a top-10 string list)
crates/apps/eu-sedia/src/lib.rs:311-333 (history join scoped to Horizon families only), :432-434 (derived history path)
crates/apps/grants-common/src/lib.rs:360-394 (federal normalizer: no history), :386-387 (aln join key), :447-450 and :467-470 ("unified has no currency dimension"), :1494-1497 (duplicate_pairs SimHash seam)
docs/harness/moonshot-2026-07-30/funding-grants.md EU #1 Path 5 (org rollup named, unshipped)
```

## GI4 — Applicant fit engine: profile-driven eligibility matching that fires as an event

- deck item **N31**
- lens: `business-strategist` · size: **L** · gate: **contract** · effort 6 / impact 8 / risk 4
- contexts: grants-unified-layer, us-federal-grants, us-state-grants, eu-grants
- extends: M13 (queries as datasets / materialized saved searches) and the shipped saved-search alert path; new on the eligibility axis

### Summary
Today the only standing alert over the grants corpus is a full-text saved search: text in, `search.matched` out. But the corpus already carries structured eligibility - unified `eligibilities[]`/`categories[]`/money/`aln`, detail `applicant_types` (with an honest Null-vs-empty distinction), `cost_sharing`, `eligibility_text` - and the catalog names the IRS EO BMF as "the eligibility ground-truth". Nothing consumes any of it as a predicate. This card adds applicant profiles and a deterministic fit stage: `grants/profiles` -> `grants/fits` (one row per profile x opportunity with a scored, explained verdict), written in the corpus pass so the existing dataset triggers/webhooks/watches fan out "a grant you are eligible for just opened" with zero new delivery code.

### Description
What exists: `normalize_ca_grants` populates `categories` and `eligibilities` from `; `-split columns and parses `EstAmounts` into floor/ceiling (crates/apps/grants-common/src/lib.rs:405-436); the detail record's `requirements` carries `cost_sharing` (Bool|Null), `applicant_types` where "absent is not empty" (crates/apps/grants-gov/src/lib.rs:1444-1473), `eligibility_text`, `expected_awards` and money (lib.rs:1384-1414). Search2 itself accepts `eligibilities` codes as a *query* param (lib.rs:164-167) but they are never stored per hit (grants-common lib.rs:381-385). Saved searches are text queries with app/dataset scoping and a webhook URL (docs/features/search.md:100-102); materialize turns a query into a standing view dataset (search.md:104-113). The `/grants` filter surface is equality/date/money only (crates/server/src/routes/query.rs:43-66). `grants` is a registered virtual namespace that watches and dataset triggers can target (docs/features/events-webhooks.md:43-45, docs/features/triggers.md:7), so a new `grants/fits` dataset is deliverable the day it is written. The eligibility ground-truth sources are planned but unbuilt: `irs-eo-bmf` ("EIN valid, 501(c)(3), deductibility, auto-revocation") and `propublica-nonprofits` (catalog/data-sources.toml:148-172). A Claude-tier scoring pass over `eligibility_text` has an app-level precedent (crates/apps/connector-api-watch/src/lib.rs:484).

### Flow
- `grants/profiles`: `POST /grants/profiles` `{name, org_type (nonprofit|gov|tribal|smb|university|individual), country/state, ein?, uei?, ntee?, budget_band, focus_tags[], cost_share_capacity, programs_watched[]}`; validated JSON schema in the manifest style.
- Pure `fit(profile, unified, detail?) -> Fit {verdict: eligible|likely|blocked|unknown, score, reasons[], blockers[]}`: hard gates on `applicant_types`/`eligibilities` vocab maps, geography (source market), cost-share vs capacity, award band vs budget; `unknown` whenever the fields are Null (honest-Null rule, never a fabricated `eligible`); unit-tested `x_not_y` style.
- Corpus pass: for each profile, evaluate this cycle's new/changed unified keys (delta, not corpus), upsert `grants/fits` keyed `{profile}:{unified_key}`; `fresh` rows are exactly the alert set for a dataset trigger / watch.
- Optional Claude-tier refinement on `eligibility_text` for `likely` rows behind a per-profile budget and a `fit.method` field naming which arm decided.
- `GET /grants/fits?profile=&verdict=` and `GET /grants?profile=` (join); feature doc; when `irs-eo-bmf` ships, EIN-verified 501(c)(3) status hardens the nonprofit gate.

### Expected impact
The product moves from "here are all 2600 open grants" to "here are the 14 you can apply for, and why" - the pitch every grants SaaS sells, delivered as a dataset + webhook the fleet can consume. Measured by: fits per profile per day, share of `unknown` verdicts (drives which source fields to enrich next), alert precision reported by consumers. What could break: a wrong vocabulary map produces confident false `blocked` verdicts; keep `unknown` the default and log the mapping miss.

### Evaluation
Claim: user - standing eligibility-aware matches with reasons, delivered through existing triggers/webhooks
Before: alerts are text-only saved searches (search.md:100-102); `/grants` filters are equality/date/money (query.rs:43-66); `eligibilities[]`, `applicant_types`, `cost_sharing` are stored and read by nothing
After: `grants/fits` rows per profile with `verdict` distribution; `fresh` fits per cycle as the alert count
Method: probe - read the normalizers, the requirements block, the query route and the search/trigger docs
Result: unmeasurable (instrument: `fits: {profiles, evaluated, eligible, likely, blocked, unknown}` in the corpus-pass block)
Gate: contract

### Evidence

```
crates/apps/grants-common/src/lib.rs:405-436 (CA eligibilities/categories/money), :381-385 (federal eligibilities never stored)
crates/apps/grants-gov/src/lib.rs:1384-1414 (requirements: cost_sharing, eligibility_text, expected_awards), :1444-1473 (applicant_types, absent is not empty), :164-167 (eligibility codes accepted as query param only)
crates/server/src/routes/query.rs:43-66 (filter surface: source/status/dates/money/trust)
docs/features/search.md:100-113 (saved searches + materialize), docs/features/events-webhooks.md:43-45 and docs/features/triggers.md:7 (grants virtual namespace deliverable)
catalog/data-sources.toml:148-172 (irs-eo-bmf "the eligibility ground-truth", propublica - both planned)
```

## GI5 — Portal-grants: a recipe-driven state/national grant portal family (NY, TX, IL, OH, AU)

- deck item **N32**
- lens: `innovation-catalyst` · size: **XL** · gate: **policy** · effort 9 / impact 7 / risk 7
- contexts: us-state-grants, grants-unified-layer
- extends: M19 (catalog as control plane), M09 (wrapper induction) and M44 (research as compiler) applied to the grants unified layer; ca-grants is the single-source v1

### Summary
The `us-state-grants` context is one portal because California is "the only US state that publishes a true open-call API" - NY, TX, IL, OH and AU GrantConnect have sat in the catalog as `planned` since the pipeline map was written. Every other portal is HTML behind a browser or a paginated listing, which is exactly what the platform's crawl, declarative extraction, wrapper induction and research-as-compiler seams exist for. But the unified layer cannot absorb them: each source has a hand-written normalizer in grants-common and `finalize_unified` is called by three bespoke crates. This card makes the fourth-through-eighth sources cheap: one `portal-grants` app driven by a per-portal recipe (fetch plan + extraction rules + a field-map into the unified schema), a data-driven normalizer beside the three hand-written ones, and a conformance harness that proves each portal's output against the unified contract before it may publish.

### Description
Evidence: ca-grants' header states the one-portal reality (crates/apps/ca-grants/src/lib.rs:2-3); the catalog carries `ny-grants`, `tx-grants`, `il-grants`, `oh-grants` and `au-grantconnect` ("forecast + current + awarded") all `status = planned` (catalog/data-sources.toml:224-283). The unified layer has exactly three normalizers - `normalize_grants_gov`, `normalize_ca_grants`, `normalize_eu_sedia` - each a hand-written `json!` block over source field names (crates/apps/grants-common/src/lib.rs:365-482), and each source crate re-implements the same walk/drift/sweep plumbing (ca-grants lib.rs:132-195 and grants-common `walk_end`/`sweep_warning`/`empty_listing_is_drift` at lib.rs:1955-2057 exist precisely because that plumbing kept forking). `SOURCE_DATASET` health gating resolves the source's own `(app, opportunities)` pair (lib.rs:79-111), so a multi-portal app needs per-portal health identity, not per-crate. The contract machinery already exists for the target: catalog `[source.contract]` with `required_fields`/`types`/`max_row_delta_pct` (catalog:209-219) and M20 enforcement at publish time. The keys unified needs are few and stable (`source`, `source_id`, `title`, `agency`, `status`, `open_date`, `close_date`, `close_at`, money x3, `categories`, `eligibilities`, `aln`, `url`, `description`), and the shared parsers (`parse_date`, `money_scalar`, `money_range`, `norm_status`) are already public (lib.rs:1705-1874) - a field-map is a thin layer over them. Why the shipped seams are not enough alone: M44 can draft a source, M09 can induce a wrapper, M19 can reconcile a catalog entry - but none of them can write into `grants/unified`, because admission to the canonical layer is a Rust function per source.

### Flow
- `grants_common::normalize_mapped(rec, &FieldMap) -> Option<(key, unified)>`: a declarative map `{source, id: [paths], title: [paths], status: {path, vocab}, dates: {...}, money: {floor, ceiling, total, range?}, lists: {...}}` that reuses the public parsers; tested against the three existing normalizers as golden equivalents so the hand-written ones can be expressed by it.
- `portal-grants` app: params = a portal recipe id; the recipe (catalog-owned, GitOps-reconciled per M19) declares fetch plan (http/browser, paging, listing->detail), extraction rules (declarative RuleSet or induced wrapper), the FieldMap, and its own `opportunities` namespace `portal-grants/<portal>` so extraction health and `contribution_target` gate per portal.
- Conformance harness: a recorded-fixture suite per portal (VCR/M24) asserting unified-contract fields, null-title drift threshold (`drift_warnings`, lib.rs:1127-1146) and status vocab coverage; a portal that fails cannot be scheduled (catalog `status` stays `planned`).
- Onboard NY and IL first (structured listings), TX/OH via browser tier, AU GrantConnect last (it also carries awarded data - feeds the awards layer card).
- Feature doc; each portal a catalog `[[source]]` with contract; per-portal cron offset like the 09:00/09:30/10:00 stagger.

### Expected impact
Unified coverage goes from 1 US state to 5 + a national portal without five new crates; the corpus pass, dedup/recurrence, events radar, closing-soon and search all inherit the rows for free. Measured by: portals live, unified rows by source, per-portal drift/health verdicts. What could break: portal ToS and politeness on browser-tier portals (policy), and a field-map that silently maps the wrong column - the null-title drift guard and contract tripwires are the defense.

### Evaluation
Claim: user - five more open-call sources flowing into the canonical layer via recipes, not crates
Before: 1 state source (ca-grants); 3 hand-written normalizers (grants-common lib.rs:365-482); ny/tx/il/oh/au all `planned` (catalog:224-283)
After: `GET /grants?source=ny-grants` returns rows; per-portal `sweep`/`sourceState` in results
Method: probe - read the three normalizers, ca-grants' run loop, the catalog planned entries and the contract block
Result: unmeasurable (instrument: unified row count by `source` and the per-portal health ladder)
Gate: policy

### Evidence

```
crates/apps/ca-grants/src/lib.rs:2-3 ("the only US state that publishes a true open-call API"), :132-195 (per-crate walk plumbing)
catalog/data-sources.toml:224-283 (ny-grants, tx-grants, il-grants, oh-grants, au-grantconnect: status = planned), :209-219 ([source.contract] machinery)
crates/apps/grants-common/src/lib.rs:365-482 (three hand-written normalizers), :79-111 (health resolved per source pair), :1705-1874 (public shared parsers), :1955-2057 (shared walk/drift vocabulary lifted because it kept forking)
```

## Scout note

Read in full (non-test code): crates/apps/grants-common/src/lib.rs (all 3387 lines incl. tests), grants-gov/src/lib.rs 1-1552, eu-sedia/src/lib.rs 1-645, cordis/src/lib.rs 1-1059, ca-grants/src/lib.rs 1-464, smlouvy-dump-watch/src/lib.rs 1-541; plus docs/features/apps.md grants rows, search.md, events-webhooks.md, triggers.md, mcp.md, routes/query.rs headers, catalog/data-sources.toml grants + planned entries, moonshot INDEX + funding-grants.md, dirs.txt, core engine.rs fetch_bytes. Hypotheses traced and discarded: (a) a Czech public-money awards layer from Registr smluv dumps + CEDR - smlouvy-dump-watch is index-only by explicit design (lib.rs:16-18) and fetch_bytes is in-memory-capped (engine.rs:1187), so ~100 MB dumps need the still-open streaming download item; not proposed. (b) Adding a `currency` field to unified alone - M-sized, folded into the awards card. (c) Cross-source multi-stage deadline modelling (SEDIA cutoffs) - already handled by sedia_deadline (grants-common:535-558). (d) Grants as MCP tools - M03/M29 shipped and grants-gov already ships tool manifests (mcp.md:81). (e) Per-agency extension-rate rollup alone - M-sized; folded into the program registry card.
