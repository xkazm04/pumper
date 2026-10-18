# Market Data — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## MD1 — Longitudinal panel layer: stock datasets keep every vintage as a time series

- deck item **N07**
- lens: `moonshot-architect` · size: **XL** · gate: **contract** · effort 8 / impact 9 / risk 5
- contexts: czech-labor-market, us-business-census, trades-pricing, trades-operator-economics
- extends: M12 (provenance/revisions) + M11 (derived datasets); builds on the shipped census vintage watermark and the mpsv-vpm revision-mining products (M37/M38)

### Summary
Every market-data product in this group is a STOCK keyed without time, so each refresh overwrites the previous vintage and the only history is the revision trail — which is retention-pruned, scan-capped, and mined ad hoc by each app. A first-class panel layer (a `{key}|{period}` companion dataset materialised on every revision, with its own retention, a `/panel` read surface and SDK support) turns four contexts' throwaway history into the fleet's first honest time-series product: YoY census, ISPV release history, RVU/conversion-factor history, tax-year history, and the banked nowcast backtest all become queries instead of new apps.

### Description
The pattern is identical in all four contexts:
- Census keys `{naics}:{state_fips}` WITHOUT the year; the vintage watermark exists precisely because an older run overwrites current data (`crates/apps/census-common/src/lib.rs:97-113`), and the docs say the YoY layer 'awaits multi-vintage accumulation' (`docs/features/apps.md:146`) — accumulation that the key shape makes impossible.
- mpsv-vpm's `role_trends` is a 10-revision window mined from `changes_since` under a 50 000-row scan cap (`crates/apps/mpsv-vpm/src/lib.rs:217`, `:742-773`), and the nowcast's ratio window is 6 revisions of `salary_gap` under the same cap (`:106-110`, `:897-930`). The planned release-over-release backtest was never built and is banked as the context's anchor (`.perfect/Perfect/contexts/czech-labor-market.md` 'REJECTED-deferred ... nowcast backtest ... needs accumulated ISPV releases').
- mpsv-ispv stores rows verbatim under `czIsco|sfera` (`crates/apps/mpsv-ispv/src/lib.rs:232-240`); a new ISPV release overwrites the old one, so the anchor's own history — the thing a backtest needs — is only in `record_revisions`.
- cms-fee-schedule keys `fee_schedule` on `{hcpcs}` or `{hcpcs}:{modifier}` (`crates/apps/cms-fee-schedule/src/lib.rs:331-334`), overwrites per release, and keeps only counts + a 20-mover sample of the diff (`:76-79`, `:444-500`) — the per-code RVU history Counterbill would want is discarded.
- state-tax keys `state:{st}` and stamps `year` as a field (`crates/apps/state-tax/src/lib.rs:325-327`); a 2026 vintage erases 2025.
Revisions are not a panel: `revision_retention_days` prunes them (`docs/features/datasets.md:124`), `changes_since` is capped, and the diff is field-level, not row-level. The core already has the two halves needed: `DerivedPaths`/derived specs recompute on every upsert (M11, `docs/features/datasets.md:186-208`) and provenance stamps carry `as_of` (`census_common::derived_provenance`, `crates/apps/census-common/src/lib.rs:80-92`). A `snapshot` derived-spec kind — source dataset + period expression (`$.year`, `$.release`, run date) — appends `{key}|{period}` rows to `<app>/<ds>_panel` with `DerivedPaths::NONE` and never tombstones. Registry subject worth consulting: software-engineering data-modelling golden path on slowly-changing dimensions (type-2 rows vs overwrite).

### Flow
- Core: add a `snapshot` derived-spec kind (period expression + optional dedupe-on-unchanged) that runs at chain depth +1 like M11 aggregates; panel datasets get their own retention key (`panel_retention_periods`) independent of `revision_retention_days`.
- API/SDK: `GET /datasets/{app}/{ds}/panel?key=&from=&to=` (keyset, same cursor contract as `/changes`), `query_dataset` MCP tool gains `period` filters; `@pumper/sync` mirrors panels as ordinary datasets.
- Backfill: seed panels from surviving revisions (`Datasets::history`, `crates/core/src/datasets.rs:728`) so today's ~weeks of ISPV/salary_gap/formations history is not lost at cutover.
- Adopt in the four contexts via catalog rows: census `establishments`/`nonemployers`/`owner_age` by `$.year`, `mpsv-ispv/wages` by run date, `cz-labour/salary_gap` daily, `cms-fee-schedule/fee_schedule` by `$.release`, `state-tax/tax` by `$.year`.
- Consumers: census YoY (apps.md:146) as a derived group over the panel; the nowcast backtest as a pure function over `wages_panel` × `salary_gap_panel` (ratio at release T vs official at T+1), publishing per-group error into `cz-labour/nowcast_backtest` and suppressing groups whose error exceeds a threshold.

### Expected impact
Ledgerline, Counterbill and the `kp` consumer get trend, seasonality and revision-history questions answered from one surface instead of each product hand-mirroring snapshots; the nowcast stops being unvalidated. Measured by: number of panel-backed products shipped (target 4), backtest MAPE per ISCO group published, and zero history loss on the next census vintage advance. What could break: panel growth (mpsv-vpm writes ~tens of thousands of cells daily — the panel must be opt-in per dataset and the daily labour cells need a `period=week` rollup, not a daily row).

### Evaluation
Claim: quality - every stock dataset in the group gains a retention-proof, queryable vintage history; the nowcast becomes backtestable
Before: 0 datasets in the group carry a time dimension in their key; history lives only in `record_revisions` (capped scans of 50 000 rows, `mpsv-vpm/src/lib.rs:217,:110`; pruned by `revision_retention_days`); census YoY is documented as waiting (`apps.md:146`); nowcast backtest unbuilt (perfect context anchor)
After: `<ds>_panel` companions for 5+ datasets; `GET /panel` and MCP `period` filters; `cz-labour/nowcast_backtest` with per-group error — measured as panel row growth per day and backtest coverage once a second ISPV release lands
Method: probe - read every key construction in the four contexts and the revision/retention/derived docs; traced how each longitudinal product today re-mines revisions
Result: unmeasurable (moonshot) — the instrument is the panel itself: count of `(key, period)` pairs retained vs revisions pruned, and backtest MAPE
Gate: contract

### Evidence

```
census-common/src/lib.rs:97-113 — 'keyed WITHOUT the year ... OVERWRITES current data with older data'; vintage watermark chosen over vintage-in-key
docs/features/apps.md:146 — 'Census YoY trend layer awaits multi-vintage accumulation.'
mpsv-vpm/src/lib.rs:217 TRENDS_REVISION_SCAN=50_000; :106 NOWCAST_WINDOW_DEFAULT=6; :742-773 trend window = newest 10 revisions; :897-930 nowcast ratios mined from salary_gap revisions
mpsv-ispv/src/lib.rs:232-240 keyed_rows -> `{czIsco}|{sfera}` verbatim overwrite
cms-fee-schedule/src/lib.rs:331-334 key `{hcpcs}[:{modifier}]`; :76 TOP_MOVERS_CAP=20; :444-500 diff keeps counts + 20 movers only
state-tax/src/lib.rs:325-327 `rec["year"] = json!(year)` on key `state:{st}`
docs/features/datasets.md:124 revision_retention_days prunes record_revisions; :186-208 derived specs (M11) recompute on upsert — the seam to hang a snapshot kind on
.perfect/Perfect/contexts/czech-labor-market.md — nowcast backtest banked: 'needs accumulated ISPV releases'
```

## MD2 — State x trade market profile: trades economics joined to census density, one row, one MCP tool

- deck item **N33**
- lens: `integration-planner` · size: **L** · gate: **contract** · effort 5 / impact 8 / risk 3
- contexts: trades-operator-economics, trades-pricing, us-business-census
- extends: M35 (taxonomy-as-data carries NAICS per trade) + M39/M40 (market_blend) + M03/M29 (MCP query_dataset); v1 stops at two products with incompatible keys

### Summary
The two Ledgerline-facing products — `trades/operator_economics` (`<ST>:<trade>`, ~260 rows) and `census/market_blend` (`{naics4}:{state_fips}`, ~104 rows) — answer halves of the same question ('should I launch as a plumber in Texas, and what will it cost/earn?') under keys that cannot be joined by a consumer without a crosswalk the repo already holds. A `market/profile` virtual dataset keyed `<ST>:<trade>` that joins economics + compliance + density + saturation + succession + formation per state x trade, plus an MCP `market_profile` tool, is the product both consumers are currently reassembling by hand.

### Description
Evidence the join is one step away:
- The crosswalk exists in libraries both sides already depend on: `taxonomy::TradeEntry.naics` gives each trade its NAICS codes (`crates/apps/trades-common/src/lib.rs:1162-1174`, seeds at `:1085-1093`), and `census_common::state_abbr` maps FIPS to USPS (`crates/apps/census-common/src/lib.rs:429-485`). census-density already reads the taxonomy (`crates/apps/census-density/src/lib.rs:236`), so the dependency direction is legal.
- Blend cells are `{naics4}:{state_fips}` with `trade` label from the solo side (`census-density/src/lib.rs:1437`, `:1270-1272`); operator_economics per-state rows are `{code}:{label}` (`trades-common/src/lib.rs:1968-1988`). The docs already warn consumers about grain/key hazards for the labour trio (`apps.md:29` 'Join hazard'); the trades/census pair has the same hazard with no warning at all.
- Plumbing and HVAC share NAICS 238220 (`trades-common:1088-1089`) and NES is 4-digit (`census-nonemp/src/lib.rs:20-22`), so the profile must carry an honest `density_grain: naics4` label — the same discipline the blend applies to succession (`succession_grain`) and formation (`grain: naics_sector_national`, `census-density:1373-1374`).
- MCP already exposes `query_dataset` with `$.path:op:value` filters (`docs/features/mcp.md:57`), so a profile dataset is instantly agent-queryable; a purpose-built `market_profile {state, trade}` tool removes the two-call, two-key dance.
The row shape: economics (wage_band, pricing for that locality, tax, compliance, valuation) from operator_economics; density (employer/solo/total, per_10k + basis, coverage) from market_blend; succession + formation blocks; plus a `vintages` block union of both sides (the blend already computes one, `census-density:1429-1435`). Written via `upsert_many_derived` with `DerivedPaths` on the replicated national blocks (same trick as `STATE_ROW_DERIVED_PATHS`, `trades-common:1854`) so a national wage refresh does not mark 255 profile rows changed.

### Flow
- Add `market-common` helpers (or extend census-common) with `state_fips_for_abbr` and `naics4_for_trade(entry)`; pin them with the same inventory-test idiom used for `is_empty_answer`.
- New virtual namespace `market` (seed `registry::VIRTUAL_NAMESPACES`, publishers: the four trades apps + four census apps) — this also pays the seam the labour and census contexts both hit (`catalog_tests` refusing virtual namespaces).
- `sync_market_profile(ctx)` called at the end of `sync_operator_economics` and `sync_market_blend`, so whichever side refreshes last publishes; declare `market/profile` in both families' `index_datasets`.
- MCP tool `market_profile` (read-only, ungated) + `GET /market/profile/{state}/{trade}`; SDK example mirroring `market/profile` into Ledgerline.
- Catalog contract for `market/profile` (required fields, per_10k ranges, `density_grain` type).

### Expected impact
Ledgerline's launch ranking and an agent asked 'best state to start an HVAC business' get one row instead of two datasets and a hand crosswalk; the join hazard between families becomes impossible to hit. Measured by: one `query_dataset`/`market_profile` call replacing two plus client-side joining; profile coverage (states x trades with both halves non-null). What could break: a trade whose NAICS has no NES 4-digit row yields a half profile — must carry `coverage` exactly as the blend does, never zeros.

### Evaluation
Claim: user - one keyed row per state x trade with economics and density together, queryable by an agent in one call
Before: 2 products, 2 key grammars (`TX:Plumbing` vs `2382:48`), 0 crosswalk exposed; a consumer needs taxonomy NAICS + FIPS mapping + naics4 truncation to join, none of which is documented on the consumer side
After: `market/profile` rows = states x enabled trades (51 x 5 = 255 today), each carrying both halves with `coverage`/`density_grain`; 1 MCP call
Method: probe - traced key construction on both sides and the libraries each family already links
Result: unmeasurable (moonshot) — instrument: profile coverage ratio and MCP tool call count in the observability ledger
Gate: contract

### Evidence

```
trades-common/src/lib.rs:1968-1988 per-state row key `{code}:{label}`; :1085-1093 Trade::naics (Plumbing & HVAC both 238220); :1162-1174 TradeEntry.naics
census-density/src/lib.rs:1437 blend key `{naics4}:{st_fips}`; :236 census-density reads trades_common::taxonomy (dependency direction already legal); :1429-1435 vintages block; :1373-1374 grain labels
census-common/src/lib.rs:429-485 state_abbr(fips) — the FIPS->USPS half of the crosswalk
census-nonemp/src/lib.rs:20-22 NES is 4-digit only (grain label needed on the profile)
docs/features/mcp.md:57 query_dataset filter grammar; docs/features/apps.md:29 documented join hazard for the labour trio (none exists for trades x census)
crates/server/src/registry.rs:78-90 VIRTUAL_NAMESPACES seeds only `grants` — census/trades/cz-labour all missing, the shared seam three contexts report
```

## MD3 — Employer hiring scorecard: per-IČO time-to-close, repost rate and pay positioning from the survival ledger

- deck item **N34**
- lens: `business-strategist` · size: **L** · gate: **policy** · effort 5 / impact 8 / risk 4
- contexts: czech-labor-market
- extends: M37 (vacancy survival ledger) — v1 aggregates closures to unit group x kraj and throws the employer dimension away; ARES enrichment (employers dataset) exists but only for ~50 sampled IČOs per run

### Summary
The daily ledger already carries the employer IČO on every open and closed posting, and the ARES enrichment already resolves IČOs to legal entities with CZ-NACE codes — but nothing aggregates by employer. A `cz-labour/employer_scorecard` dataset (per IČO: live postings, median days-to-close, repost share, posted-median vs ISPV gap, occupation mix, hiring velocity) is the recruiter/sales lead list and the 'who is struggling to hire' signal that no Czech source publishes, derivable entirely from data the run already holds in memory.

### Description
- `ledger_today` captures `(czisco, kraj, band, ico)` per posting BEFORE the recency filter (`crates/apps/mpsv-vpm/src/lib.rs:497-522`); `ClosedEntry` keeps `ico`, `days_open`, `repost_id` (`:2158-2167`) and the repost matcher keys on IČO (`:2172-2205`). `aggregate_lifecycle` collapses all of it to `(unit group, kraj)` (`:2245-2307`) — the employer axis is discarded at the last step.
- Posted salary per posting is available at ingest (`monthly_salary_point`, `:2448-2456`) and the ISPV anchor index is already built in the same run (`official_wage_index`, `:1458-1478`), so a per-employer 'pays X% above/below official for this ISCO group' is one map away.
- `employers` is ARES-enriched but capped at 50 NEW lookups per run and only for IČOs in the sample reservoir (`:210`, `:997`, `:1042-1069`); a scorecard needs the enrichment to follow the scorecard's top-N employers instead of random samples.
- Privacy floor already exists (`minCount`, `:317-320`); the scorecard must apply it per employer cell and publish only employers above a posting floor (companies, not individuals — IČO is a public register identifier, and the feed is CC BY 4.0, `:43-44`).
- The `kg`/FollowTheMoney graph in the sibling Politicas project keys companies by IČO, so an employer scorecard is directly joinable to public-money trails — a cross-product no one else can assemble.
Registry subject to consult: llm-observability is irrelevant here; the software-engineering 'aggregation privacy floor' technique (k-anonymity on small cells) governs the per-employer floor.

### Flow
- Extend `TodayPosting`/`OpenEntry` with the salary point and unit group already computed; add `aggregate_employers(open, closed, official, min_count)` as a pure function beside `aggregate_lifecycle` with tests named after the anti-patterns (`employer_below_floor_is_not_published`, `repost_share_uses_closures_not_opens`).
- Publish `cz-labour/employer_scorecard` keyed `{ico}` (title = employer name from `employers` when enriched, else IČO), with `hiring_velocity_7d` derived from the ledger's `first_seen` distribution, and add it to `INDEXED_DATASETS` (`:187-192`) so it is searchable and watchable ('alert when employer X posts > N roles').
- Redirect ARES lookups: enrich the scorecard's top-N by live postings first, samples second, keeping the 50/run cap.
- Add the catalog contract and an SDK mirror example; expose a `GET /labour/employers?kraj=&isco4=&sort=days_to_close` curated route (the `query.rs` pattern, `crates/server/src/routes/query.rs:1-3`).

### Expected impact
Recruiters, HR-tech and B2B sales teams get a daily, sourced 'hardest-to-fill employers' list per region/occupation; the `kp` consumer gets employer pages. Measured by: employers published per run (expect thousands above a floor of 3 live postings out of ~300k postings), watch subscriptions on the dataset. What could break: employer-level cells are small — the floor must be enforced or the dataset leaks a one-person company's salary offer; repost share per employer amplifies the filled/withdrawn ambiguity the lifecycle doc already flags (`metric: time_to_close`, `:2289-2291`).

### Evaluation
Claim: user - a daily per-employer hiring scorecard exists for the Czech market
Before: 0 employer-level aggregates; `employers` holds ARES metadata only (max 50 new per run, `:210`); the ledger's IČO is used solely for repost matching (`:2172-2205`)
After: `cz-labour/employer_scorecard` with N employers >= floor per run, each carrying days-to-close, repost share, pay gap vs ISPV, live count — measured as rows published and enrichment coverage of the top-N
Method: probe - traced the IČO field from deserialisation (`:2365-2370`) through ledger, repost matcher and lifecycle aggregation
Result: unmeasurable (moonshot) — instrument: scorecard row count and top-N enrichment coverage per run
Gate: policy (publishing per-company hiring difficulty; k-floor and public-register-only identifiers)

### Evidence

```
mpsv-vpm/src/lib.rs:497-522 ledger_today keeps `ico` per posting before the recency filter
mpsv-vpm/src/lib.rs:2158-2167 ClosedEntry{ico, days_open, repost_id}; :2172-2205 repost index keyed on (ico, czisco, kraj, band)
mpsv-vpm/src/lib.rs:2245-2307 aggregate_lifecycle collapses to (unit group, kraj) only — employer axis dropped
mpsv-vpm/src/lib.rs:210 ARES_MAX_LOOKUPS_DEFAULT=50; :997 icos come from sample_items only
mpsv-vpm/src/lib.rs:1458-1478 official_wage_index built in the same run; :2448-2456 per-posting salary point
mpsv-vpm/src/lib.rs:187-192 INDEXED_DATASETS — where the new product must be declared to be watchable
```

## MD4 — Sub-state launch atlas: county/metro-grain blend, saturation and pricing

- deck item **N35**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 6 / impact 7 / risk 5
- contexts: us-business-census, trades-pricing
- extends: M39/M40 (market_blend + saturation) — v1 is state-grain because the blend refuses county rows; homewyse-pricing's `locality` param already prices metros but nothing drives it

### Summary
Ledgerline's stated job is a GEOGRAPHIC launch ranking, yet every product row in the census family is state-grain: the blend explicitly discards county rows, saturation is county-capable only for census-density, NES-D and BFS are coarser still, and homewyse-pricing can price a metro but only when a human passes `locality`. A county/metro atlas — county-grain CBP + ACS saturation (already fetchable), county-grain NES where published, metro pricing driven from the atlas's own top-N counties, and an honest per-field grain label — is the difference between 'Texas' and 'Travis County, TX'.

### Description
- census-density already supports `geo=county` with a mandatory `states` FIPS filter and a county ACS denominator (`crates/apps/census-density/src/lib.rs:93-99`, `:259-265`, `:1476-1503`), and `saturation` keys carry the grain (`SATURATION_KEY_GRAIN`, `:886`).
- The blend drops everything but state rows (`:972-991` filters `$.geo = state` in SQL; `:1239-1243` 'Only state rows: the solo side has no county grain'; `base_index` skips non-state, `:1139-1141`). The comment asserts NES is state-only (`:520-521`) — the app fetches `for=state:*` only (`census-nonemp/src/lib.rs:192-196`); whether the NES API serves `for=county:*` at 4-digit NAICS is NOT verified in-repo and is the first probe of the build.
- homewyse-pricing keys `{locality}:{trade}:{job}` and gates freshness per locality (`crates/apps/homewyse-pricing/src/lib.rs:237`, `:138-156`); the unified join filters pricing by locality and falls to Null on state rows with no priced locality (`trades-common/src/lib.rs:1966-1979`). Metro pricing therefore already has a home in the data model — it lacks a driver that chooses which metros to pay for.
- Sub-state rows must label what each block actually is: employer counts county-grain, solo counts state- or county-grain, succession sector/state-grain, formation national — the blend's existing `grain`/`scope`/`basis` discipline (`:1450-1456`, `:1373-1374`) extends naturally.

### Flow
- Probe NES county availability with one keyed request per default NAICS; record the verdict in the app doc header the way the BFS contract was pinned (`census-bfs/src/lib.rs:17-31`).
- Extend `blend_market` with a `geo` dimension: cells `{naics4}:{geo_fips}` for both state and county, solo side joined at county when available else state-carried with `solo_grain: state_carried` (never a per-county fabrication).
- `census/atlas`: top-N counties per trade by saturation and by total_market_per_10k, with the vintages block; a scheduled census-density county run over the top-K states by formation velocity.
- Pricing driver: a `metro_pricing` schedule that enqueues homewyse-pricing for the atlas's top-N metros (metro name from the county's CBSA crosswalk, a small static table), budget-capped through the existing `budget_usd` rail (`docs/features/apps.md:31`).
- Catalog contract + SDK mirror; `market_profile` (Card 2) gains an optional `county` argument.

### Expected impact
The launch ranking becomes actionable at the grain people actually open businesses in; pricing becomes local without a human choosing localities. Measured by: counties with a complete (both-sided) blend cell, metros priced per quarter within budget. What could break: county runs multiply CBP requests (~3 000 counties x 4 NAICS) and dataset size (50k read caps, `BLEND_READ_LIMIT`, `:558`) — must be top-K-state scoped and the read caps raised or paginated.

### Evaluation
Claim: user - a county/metro-grain launch atlas with localized pricing
Before: blend rows are state-only (max 52 per naics4); county saturation exists but is not joined; pricing localities = whatever a human passed (default 'United States', `homewyse-pricing:29`)
After: `census/atlas` rows per (naics4, county) for the top-K states; metro pricing rows for the atlas's top-N metros; each block grain-labelled
Method: probe - read the geo handling in census-density/nonemp and the locality plumbing in homewyse + unified
Result: unmeasurable (moonshot) — instrument: both-sided county cells and priced-metro count per quarter
Gate: contract

### Evidence

```
census-density/src/lib.rs:93-99,:259-265 geo=county supported (states filter required); :1476-1503 county for-clause + place_of
census-density/src/lib.rs:972-991 blend reads `$.geo = state` only; :1239-1243 'Only state rows'; :1139-1141 base_index skips county; :520-521 'NES is state-only' (assumption, unverified in-repo)
census-nonemp/src/lib.rs:192-196 for=state:* only
homewyse-pricing/src/lib.rs:237 key `{locality}:{trade}:{job}`; :138-156 per-locality freshness gate; :29 DEFAULT_LOCALITY
trades-common/src/lib.rs:1966-1979 per-state pricing looked up by locality == state code, Null otherwise
census-density/src/lib.rs:558 BLEND_READ_LIMIT=50_000 — the cap a county atlas would hit
```

## MD5 — Codebook-resolved skills demand: MPSV číselníky turn opaque URIs into a readable, ESCO-linkable product

- deck item **N36**
- lens: `innovation-catalyst` · size: **L** · gate: **none** · effort 4 / impact 7 / risk 3
- contexts: czech-labor-market
- extends: shipped `skill_demand` / `education_agg` datasets and the labour-datasets-visible direction — v1 excludes both from the index because their identity is an opaque codebook URI

### Summary
mpsv-vpm already computes skill demand and education premiums per CZ-ISCO unit group daily, but keys them on codebook URIs (`Dovednost/…`, education ids, `Kraj/108`, `TypMzdy/N`) it never resolves — so the datasets are deliberately excluded from search and watches ('no searchable TEXT in them'), titles read `kraj Kraj/108`, and no consumer can name a skill. MPSV publishes the codebooks as open data on the same portal. Ingesting them (a small `mpsv-ciselniky` app) and resolving every URI at write time turns skill_demand into a human-readable, searchable, ESCO-mappable skills-demand product — and gives every labour dataset real region and wage-type labels.

### Description
- Skills and education ids are deserialised as `IdRef{id}` only (`crates/apps/mpsv-vpm/src/lib.rs:2339-2349`, `:2358-2362`) and keyed as-is: 'Codebook ids are opaque URIs ("Dovednost/…"); key on them as-is, never substring-match' (`:575-594`).
- The index exclusion is explicit: 'their identity is an opaque codebook URI (`Dovednost/…`) — there is no searchable TEXT in them' (`:176-178`); `INDEXED_DATASETS` omits `skill_demand`/`education_agg` (`:187-192`).
- Region labels are the raw id: `kraj_label` renders `kraj Kraj/108` (`:1274-1280`; tests `:2877-2885`), so every lifecycle/region title and the search hit for a Czech region says a code, not 'Jihomoravský kraj'.
- The salary gate history shows the cost of unresolved codebooks: `typMzdy.id` was string-matched against a URI and silently discarded every salary (`:2443-2447`).
- The catalog already knows the portal serves more than the two feeds (`catalog/data-sources.toml:494-506` lists a planned increments feed on data.mpsv.cz); the codebook distributions live under the same `/od/soubory/` root and are small, quarterly-stable JSON — a `mpsv-ispv`-shaped app (one fetch, verbatim rows, drift-loud floor, `crates/apps/mpsv-ispv/src/lib.rs:100-171`) covers them.
- Once skill labels exist, an ESCO crosswalk (ESCO publishes CSV downloads with Czech labels) lets `skill_demand` join EU-wide skills taxonomies — the door to a `kp`-facing 'skills in demand for role X in region Y, in plain Czech and English' product.

### Flow
- New app `mpsv-ciselniky` (key-free, quarterly): fetch the Dovednost / Vzdelani / Kraj / TypMzdy codebooks, upsert `codebooks/{name}` keyed by URI with `label_cs`/`label_en`; drift-loud floors per codebook.
- mpsv-vpm: load the codebooks once per run (`ctx.datasets.list`, like the ISPV read at `:829`) and stamp `skillLabel`, `educationLabel`, `krajName`, and a `title` on skill_demand/education_agg rows; `kraj_label` renders the name.
- Add `skill_demand` and `education_agg` to `INDEXED_DATASETS` with a weekly rollup so daily churn does not index the same cell 365 times (the reason they were excluded); update the inventory test.
- ESCO crosswalk as a `codebooks/esco_map` dataset and a `skill_demand.esco_uri` field; catalog contracts for the new datasets; `docs/features/apps.md` row.

### Expected impact
The `kp` consumer and any agent can ask 'which skills are rising for ISCO 2512 in Praha' and get names, not URIs; regional products stop showing `Kraj/108`. Measured by: share of skill_demand rows with a resolved label (target > 99%), skill searches served. What could break: codebook drift (renamed URIs) would strand labels — the drift-loud pattern from mpsv-ispv must be reused, and unresolved ids must stay as raw URI with `label: null`, never dropped.

### Evaluation
Claim: quality - skills, education and region dimensions become human-readable and searchable
Before: 0 codebooks ingested; skill_demand/education_agg excluded from the index (`:187-192`); region titles print raw ids (`:2877-2885`)
After: 4 codebooks mirrored; > 99% of skill/education rows labelled; both datasets indexed; ESCO mapping coverage reported per run
Method: probe - traced every codebook URI from deserialisation to record key and to the index exclusion rationale
Result: unmeasurable (moonshot) — instrument: labelled-row share and search hit counts on skill labels
Gate: none

### Evidence

```
mpsv-vpm/src/lib.rs:176-178 'their identity is an opaque codebook URI (`Dovednost/…`) — there is no searchable TEXT in them'
mpsv-vpm/src/lib.rs:187-192 INDEXED_DATASETS omits skill_demand/education_agg
mpsv-vpm/src/lib.rs:575-594 skills/education keyed on raw codebook ids; :2339-2349,:2358-2362 IdRef{id} only
mpsv-vpm/src/lib.rs:1274-1280 kraj_label -> `kraj Kraj/108`; tests :2877-2885
mpsv-vpm/src/lib.rs:2443-2447 typMzdy URI string-match discarded every salary — the prior cost of unresolved codebooks
mpsv-ispv/src/lib.rs:100-171 the one-fetch verbatim-rows app shape to clone for codebooks
```

## MD6 — Localized Medicare price oracle: GPCI join gives a dollar price per HCPCS x locality per release

- deck item **N37**
- lens: `feature-scout` · size: **L** · gate: **contract** · effort 4 / impact 7 / risk 3
- contexts: trades-pricing
- extends: M32 (Medicare price oracle) — v1 owns the RVU parse and a release diff but publishes RVUs, not prices, and keeps only a 20-mover sample of what changed

### Summary
cms-fee-schedule now downloads the RVU ZIP and owns `fee_schedule` (work/PE/MP RVUs + conversion factor per HCPCS). Counterbill's reference-price database needs the number a provider is actually paid, which is `(work x GPCI_work + PE x GPCI_pe + MP x GPCI_mp) x CF` per Medicare locality — and the GPCI file ships in the same ZIP the app already extracts. Parsing it and materialising `fee_schedule_prices` (HCPCS x locality x release, facility and non-facility) plus a full per-code diff turns an RVU mirror into the price oracle M32 named.

### Description
- The ZIP extractor picks only the PPRRVU entry (`find_pprrvu_entry`, `crates/apps/cms-fee-schedule/src/lib.rs:367-379`); the test fixture itself lists `RVU26B/GPCI2026.csv` as a sibling entry (`:994-1000`) — the file is known and skipped.
- `fee_schedule` rows carry `work_rvu`, `pe_rvu_nonfac`, `pe_rvu_fac`, `mp_rvu`, `conversion_factor` (`:339-350`), i.e. every RVU input of the PFS formula; the total-RVU headline is already computed in the diff (`RvuRow::total_nonfac`, `:429-431`).
- The diff keeps `added/removed/changed` counts and 20 movers (`TOP_MOVERS_CAP`, `:76`; `:482-499`); Counterbill needs the full per-code delta to know which of its ~17k baked prices moved.
- The release watcher already emits a structured `ingest` block for a `dataset` trigger (`:806-813`) and self-baselines (`:704-729`); a price table keyed by release slots into the same lifecycle. The pinned-layout discipline (`:24-43`) extends to the GPCI header (`Medicare locality`, `PW GPCI`, `PE GPCI`, `MP GPCI`).
- Combined with Card 1's panel, per-locality price history across releases becomes a query; without it, `fee_schedule_prices` keyed `{hcpcs}:{locality}` still overwrites per release and the diff dataset carries the delta.

### Flow
- `extract_named_csv(zip, prefix)` generalising `extract_pprrvu_csv` (`:383-400`); `parse_gpci` pure function with drift-loud header pins and golden-file tests.
- `compute_prices(rvu_rows, gpci_rows, cf)` pure function producing `{hcpcs[:mod]}:{locality}` rows with `price_nonfac`, `price_fac`, `release`; store honest-Null when any component is Null (never a fabricated $0, the family rule at `:278-286`).
- `fee_schedule_price_changes/{release}`: full per-code delta as a chunked artifact + a bounded dataset of every code whose price moved more than a threshold, so consumers can `?filter=` for their own codes.
- Catalog contract for prices (ranges > 0, required fields); docs row; expose a `GET /medicare/price/{hcpcs}?locality=` curated route or rely on `query_dataset`.
- Add CLFS/ASP schedules behind the existing `schedule` enum (`:630-634`) once PFS prices are proven.

### Expected impact
Counterbill retires `scripts/ingest-cms-pfs.mjs` and reads localized prices directly; a patient-facing 'what Medicare pays for 99213 in Houston' becomes one query. Measured by: price rows per release (~17k codes x ~110 localities ≈ 1.9M rows — must be scoped to the consumer's localities or stored as a chunked artifact + on-demand compute), and codes whose price moved per release. What could break: the row count — a naive upsert of 1.9M rows through the per-row `upsert_many` path is the mpsv-vpm anti-pattern (`mpsv-vpm/src/lib.rs:38-41`); prices should be computed on read from RVU + GPCI, with only the consumer's locality set materialised.

### Evaluation
Claim: user - dollar prices per HCPCS x locality per release, with a complete per-code change list
Before: RVUs only (no GPCI parse, no price), diff limited to 20 movers (`:76`), Counterbill still runs its own ingest script (`:817-822` ingest_hint)
After: `fee_schedule_prices` for the configured localities + full change list per release; ingest_hint retired
Method: probe - read the ZIP/CSV pins, the record shape and the diff bounds
Result: unmeasurable (moonshot) — instrument: price rows per release and the count of Counterbill codes whose price moved
Gate: contract

### Evidence

```
cms-fee-schedule/src/lib.rs:367-379 find_pprrvu_entry selects only PPRRVU*.csv; :994-1000 fixture lists `RVU26B/GPCI2026.csv` in the same ZIP
cms-fee-schedule/src/lib.rs:339-350 record carries work/pe_nonfac/pe_fac/mp RVUs + conversion_factor; :429-431 total_nonfac
cms-fee-schedule/src/lib.rs:76 TOP_MOVERS_CAP=20; :482-499 diff output bounded
cms-fee-schedule/src/lib.rs:806-822 structured `ingest` block + ingest_hint still pointing Counterbill at its own script
cms-fee-schedule/src/lib.rs:630-634 `schedule` enum pinned to pfs only
```

