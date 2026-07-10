# Vision Scan — pumper, 2026-07-10

> Scanners: feature_scout (100) + business_visionary (84) + moonshot_architect (92) = **276 ideas**
> 21 group-scans across 7 context groups / 21 contexts. Ranked by value score = impact*3 − effort − risk.

## Totals by scanner × group

| Group | feat | busi | moon | total |
|---|---:|---:|---:|---:|
| Scraping Runtime Core | 0 | 12 | 15 | 27 |
| Data Extraction & Storage | 30 | 12 | 12 | 54 |
| Economic & Labor Market Data Apps | 15 | 15 | 15 | 45 |
| Content & Research Apps | 10 | 10 | 10 | 30 |
| Job Server & API | 20 | 17 | 17 | 54 |
| Scraping Engines | 15 | 10 | 15 | 40 |
| Public Funding & Grants Apps | 10 | 8 | 8 | 26 |

## Themes

| Theme | Count | Top score | Top idea |
|---|---:|---:|---|
| T11 Other | 68 | 19 | Onboarding aha: 'operators like you here earn $X' |
| T1 Change-feeds & diff products | 36 | 20 | Sell diff feeds: per-dataset webhooks, RSS, and alerts |
| T4 Search & answer layer | 35 | 16 | Answer engine: NL questions over scraped corpus |
| T9 Domain data products (apps) | 29 | 17 | 'Where to launch' market-selection report as a product |
| T2 Cost & budget governance | 24 | 21 | Cost ledger: meter every fetch tier for usage pricing |
| T6 Crawler maturity | 18 | 16 | Sitemap.xml discovery and crawl-delay from robots.txt |
| T7 API & integration surface | 18 | 15 | Manifest-driven no-code scraper authoring |
| T3 Provenance & trust | 17 | 17 | Capture source citations for agentic datasets |
| T5 AI-assisted & self-healing extraction | 13 | 17 | Claude-powered source scout drafts new catalog entries |
| T10 Platform & marketplace plays | 11 | 13 | Fleet rate governor that learns host limits from 429s |
| T8 Scheduler & worker robustness | 7 | 14 | Manual retry and requeue for failed jobs |

## Full ranked backlog (by theme)

### T1 Change-feeds & diff products (36)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 2 | business | Data Extraction & Storage | Dataset Store & Change Detection | Sell diff feeds: per-dataset webhooks, RSS, and alerts | E4/I9/R3 | 20 | cc719161-622a-451c-807c-d048d325ce91 |
| 4 | business | Data Extraction & Storage | Dataset Store & Change Detection | Record revision history: time-travel unlocks trend products | E5/I9/R3 | 19 | 7f19bae8-ba95-492a-9114-abfcb617d105 |
| 5 | feature | Content & Research Apps | Extraction, Crawl & API Watch | Generic scheduled change-watch app (Visualping-style) | E5/I9/R3 | 19 | 516e407d-de90-47e2-a819-68887f699408 |
| 8 | business | Data Extraction & Storage | Broad Crawler | Site-change sentinel: crawl + diff = monitoring product | E5/I9/R4 | 18 | a6356528-43b5-41d4-bb57-602819623782 |
| 9 | moonshot | Scraping Runtime Core | App & Job Model | Change-driven reactive pipelines on dataset deltas | E6/I9/R4 | 17 | ca238cf5-66ea-490b-97dd-bec223345979 |
| 14 | business | Scraping Runtime Core | App & Job Model | Change-feed subscriptions on dataset upserts | E6/I9/R4 | 17 | 2c1c5fe5-b821-49dc-ba1b-f6264aa823b4 |
| 17 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Versioned record history with field-level diffs | E6/I9/R4 | 17 | c738f8b5-27d4-4dc7-847e-502cf6ae1874 |
| 29 | business | Content & Research Apps | Extraction, Crawl & API Watch | Sell API-change intelligence as a subscription feed | E6/I9/R5 | 16 | 87cb4c4a-bc0b-4a89-9b43-daf6d6e17832 |
| 34 | business | Job Server & API | Live Events & Webhooks | Standing queries: diff-aware alerts on new records | E7/I9/R4 | 16 | b6edcd3e-9f1c-4f2f-ad3a-9027913a20b7 |
| 35 | business | Scraping Engines | Full-Text Search Index | Saved searches as standing watch alerts | E5/I8/R3 | 16 | a7f3e8b3-ea8a-4642-8b29-db264470f7bb |
| 36 | business | Public Funding & Grants Apps | US Grant Opportunities | Metered grants-corpus API with change-feed cursor | E7/I9/R4 | 16 | 64aa0da5-e13e-4d3f-b7b3-c055ade47369 |
| 45 | moonshot | Job Server & API | Live Events & Webhooks | Reactive job DAGs: terminal events trigger pipelines | E7/I9/R5 | 15 | 5011a7f6-d7bd-4bd3-b7d1-f10843dbd749 |
| 63 | business | Public Funding & Grants Apps | US Grant Opportunities | Agency behavior intelligence from forecast history | E6/I8/R3 | 15 | b6c5b45c-aaf2-4200-b9f2-7ed958aa06be |
| 65 | business | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Longitudinal call archive with reopen prediction | E6/I8/R3 | 15 | 0e2c918c-3375-458d-b8d3-0634dda2478d |
| 66 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Change-triggered webhooks for dataset monitoring | E5/I8/R4 | 15 | da32650c-296b-492e-80b1-711cab888bd3 |
| 73 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Change-triggered webhooks for fresh records | E5/I8/R4 | 15 | c1e8c4d7-e768-48fa-b538-d9c089adc18a |
| 78 | moonshot | Scraping Engines | Full-Text Search Index | Time-travel index: versioned docs with change feed | E6/I8/R4 | 14 | 8037258d-394e-4e24-9466-d6a8c0b207b5 |
| 80 | moonshot | Public Funding & Grants Apps | US Grant Opportunities | Natural-language funding alerts over change feed | E6/I8/R4 | 14 | 0040b8a4-82f7-4ebf-9082-e281ee9aed1b |
| 83 | moonshot | Scraping Runtime Core | App & Job Model | Time-travel dataset versioning and diff feed | E6/I8/R4 | 14 | 42a413b4-a1e5-431e-8f56-40d1cfea85d7 |
| 96 | business | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Productize the release-watcher as freshness-as-a-service | E6/I8/R4 | 14 | 6d5adf85-267d-4c26-ad77-88ed82bf57cd |
| 101 | feature | Content & Research Apps | Web Research & Readable Content | Scheduled research digest that diffs over time | E6/I8/R4 | 14 | d1df7a08-fadd-4d62-a075-b0213e338e00 |
| 105 | feature | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Trending vs fading roles from daily change detection | E6/I8/R4 | 14 | c5cd98fe-a795-4a77-8105-662de8a4b7cd |
| 107 | feature | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Field-level change diffs on watched items | E4/I7/R3 | 14 | 0e6ba2e5-741e-4a4c-87c7-c1ceae8dfef5 |
| 108 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Field-level diff history for changed records | E6/I8/R4 | 14 | 22a1651b-3781-4a5e-9e92-c9b893d350dc |
| 127 | moonshot | Content & Research Apps | Extraction, Crawl & API Watch | Self-maintaining connectors: doc-change to code patch | E8/I9/R6 | 13 | 38a3d9ea-2a7e-4306-9355-b88142e54f88 |
| 131 | business | Content & Research Apps | Extraction, Crawl & API Watch | Wayback-style time machine for watched API docs | E5/I7/R3 | 13 | 1e524acc-a5c3-4a0c-95ac-e29c6ec79102 |
| 132 | business | Content & Research Apps | Extraction, Crawl & API Watch | Delta-feed data products from change-detected datasets | E7/I8/R4 | 13 | 77c46dbe-21ed-4eed-acad-8164c7f064c5 |
| 151 | feature | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Materiality diff report across dataset refreshes | E5/I7/R3 | 13 | 593f2592-8bd8-4c00-95aa-870fb7e8a56a |
| 157 | feature | Scraping Engines | WASM Plugin Sandbox | Plugin manifest with version and MIME targeting | E5/I7/R3 | 13 | 1fb809b6-f692-4ce9-862a-06b33fba7bbb |
| 170 | business | Data Extraction & Storage | Declarative Extraction Engine | Community rule marketplace with versioned site templates | E7/I8/R5 | 12 | 3ab41699-fac4-4ffe-acc1-75a1062b5316 |
| 186 | feature | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Forthcoming-call watch with open transitions | E5/I7/R4 | 12 | c413b87d-62b4-47f1-8395-0a03a61e8c31 |
| 188 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Detect disappeared records (removed-listing signal) | E5/I7/R4 | 12 | 15724e65-e523-4afd-97bf-0243c32c2420 |
| 229 | moonshot | Job Server & API | App Registry | Runtime app marketplace: install, version, rollback | E8/I8/R6 | 10 | 54208fbb-402b-480c-b381-acc31f6f03b3 |
| 252 | feature | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | CMS multi-schedule watch (CLFS, ASP) | E5/I6/R3 | 10 | 55e919a0-5e6a-4d84-9943-b401506584e3 |
| 258 | moonshot | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Fee-schedule change impact simulator | E7/I7/R5 | 9 | b5c19ae9-1892-4f13-92cf-aed8f65a51c3 |
| 261 | business | Job Server & API | Job Worker & Cron Scheduler | Adaptive cadence: scheduler learns source change rates | E7/I7/R5 | 9 | f86bb78f-bc70-478a-9f50-deba8824722f |

### T10 Platform & marketplace plays (11)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 114 | moonshot | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Fleet rate governor that learns host limits from 429s | E6/I8/R5 | 13 | f50bc37e-9688-4e56-a79c-c5797f0f1187 |
| 118 | moonshot | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Operator financial digital-twin from all four datasets | E8/I9/R6 | 13 | efde836d-b2b2-4350-9c8a-79efa007016b |
| 139 | business | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Session vault: managed multi-profile login store | E6/I8/R5 | 13 | 81f0a334-0c65-42c5-b855-2be3d068cffc |
| 158 | moonshot | Scraping Engines | WASM Plugin Sandbox | Polyglot plugin SDK: JS/Python/Go to the WASM ABI | E7/I8/R5 | 12 | ab288e17-736d-4c81-b731-d9dbf3bf5082 |
| 168 | moonshot | Content & Research Apps | Web Research & Readable Content | Ambient personal intelligence briefing from all sources | E7/I8/R5 | 12 | 4cc75b14-885c-48a7-aa5b-92c2eb0dacaf |
| 171 | business | Scraping Runtime Core | Engine Capability Traits | Publish engine traits as an embeddable Rust SDK crate | E4/I6/R2 | 12 | bd8a8f09-6980-4669-b944-3ce1dfcdb984 |
| 200 | moonshot | Public Funding & Grants Apps | US Grant Opportunities | Global funding knowledge graph across programmes | E8/I8/R5 | 11 | cb209b32-4a64-4a86-8f83-8f94b99e017f |
| 203 | moonshot | Content & Research Apps | Web Research & Readable Content | Listen to any URL: readable-to-podcast audio feed | E6/I7/R4 | 11 | 6e25eaba-d108-4dbb-a99e-1af563fa1923 |
| 209 | business | Scraping Engines | WASM Plugin Sandbox | Bring-your-own-extractor hosted tenant lanes | E7/I8/R6 | 11 | 9bdf89e3-e32f-4e36-b513-b88baf331c05 |
| 232 | moonshot | Job Server & API | Live Events & Webhooks | Federated event mesh across Pumper nodes and apps | E8/I8/R6 | 10 | 0e370a98-af71-49e1-9403-ba24f7a6d4f9 |
| 239 | moonshot | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | EU consortium partner matchmaking engine | E8/I8/R6 | 10 | 21eee44a-61f0-4580-8f58-e02f1ed686bf |

### T11 Other (68)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 3 | business | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Onboarding aha: 'operators like you here earn $X' | E3/I8/R2 | 19 | 034c98e6-28b8-4bae-b1f9-7686272458eb |
| 6 | business | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Live 'What is my business worth?' exit-readiness score | E5/I9/R4 | 18 | c1dc9fb4-3480-4ae7-af63-3488ec46383b |
| 11 | moonshot | Scraping Runtime Core | Tiered Fetcher & Politeness | Self-learning tier router that skips dead tiers | E6/I9/R4 | 17 | 0e934fdb-ada1-4a60-b594-2be41d2fcbdb |
| 22 | moonshot | Scraping Runtime Core | Engine Capability Traits | Schema-locked structured extraction engine | E6/I9/R5 | 16 | f2e49774-211d-486a-8325-d28eec2c7122 |
| 26 | business | Data Extraction & Storage | Declarative Extraction Engine | Publish throughput benchmarks as the sales spearhead | E3/I7/R2 | 16 | 48fb2bf4-0e77-45b6-b3e2-fbbed3f160bf |
| 32 | business | Scraping Runtime Core | Tiered Fetcher & Politeness | Freshness SLA tiers built on the TTL cache | E3/I7/R2 | 16 | f6cf75f0-fa58-4886-beed-60214fa5b918 |
| 33 | business | Scraping Runtime Core | Tiered Fetcher & Politeness | Per-domain escalation memory that learns the winning tier | E5/I8/R3 | 16 | 988604cb-8d31-43b7-813f-53bad5bb81a0 |
| 37 | business | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | PROSPECT cascade-funding premium feed | E3/I7/R2 | 16 | ef1efb99-caf8-4ac3-aaaa-365325860453 |
| 41 | feature | Data Extraction & Storage | Declarative Extraction Engine | Field post-processing transform pipeline | E5/I8/R3 | 16 | e6f1b844-f4c0-45bc-89c9-379152dff746 |
| 43 | feature | Scraping Engines | WASM Plugin Sandbox | Return per-run plugin execution metrics | E3/I7/R2 | 16 | 2c6c4921-c3e9-43ce-b191-5ea02b4c54e8 |
| 54 | business | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Solo revenue percentile benchmark against own books | E5/I8/R4 | 15 | 5c82e1c2-865c-470a-a86c-74f9d8eaa471 |
| 61 | business | Job Server & API | Configuration & Data Source Catalog | Public data-freshness SLA page per source | E4/I7/R2 | 15 | bef45815-d9b2-4de8-89d5-51a3bac9aaee |
| 62 | business | Job Server & API | Configuration & Data Source Catalog | License the curated source catalog as a data product | E5/I8/R4 | 15 | 2e77e595-f8a7-409e-ae98-4d227a7fb06e |
| 67 | feature | Data Extraction & Storage | Declarative Extraction Engine | Repeating-container extraction for list pages | E5/I8/R4 | 15 | ae072557-491f-48b2-b91b-2eac4792dc96 |
| 77 | moonshot | Scraping Engines | Full-Text Search Index | Global multilingual index with per-language tokenizers | E6/I8/R4 | 14 | 77237a70-354f-4a6f-bd90-4b22479c2020 |
| 81 | moonshot | Data Extraction & Storage | Dataset Store & Change Detection | Data-drift guardian: statistical anomaly alerts | E6/I8/R4 | 14 | 83b1442f-4987-4b3b-9bce-149cbcbc9cf3 |
| 91 | business | Job Server & API | Live Events & Webhooks | Named durable subscriptions for the app ecosystem | E6/I8/R4 | 14 | cdc37477-ee5a-4433-b364-0462c81a3f02 |
| 93 | business | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Claude engine drives the logged-in browser session | E7/I9/R6 | 14 | 5255c67c-e9ab-4c9a-b205-72c7211b5f17 |
| 97 | feature | Data Extraction & Storage | Declarative Extraction Engine | Typed field coercion and post-extraction transforms | E4/I7/R3 | 14 | 0044d1cc-0327-4a60-9963-cfaf14e9dee6 |
| 102 | feature | Content & Research Apps | Extraction, Crawl & API Watch | Per-URL error report for extractor & plugin | E4/I7/R3 | 14 | db9e9c6c-1096-49e8-9b8a-2034cffd9686 |
| 106 | feature | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Apply json_schema + salvage guard to all agentic apps | E4/I7/R3 | 14 | 17d5f98a-202d-42a8-9c8e-135a5b948eb8 |
| 109 | feature | Data Extraction & Storage | Declarative Extraction Engine | Required-field validation and per-doc extract status | E4/I7/R3 | 14 | d01790c3-7d10-4361-9341-85f5f7d94195 |
| 111 | feature | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Smart engine router with HTTP-to-Browser fallback | E6/I8/R4 | 14 | a585e5d9-0330-4a38-af89-79225d0e882d |
| 115 | moonshot | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Recipe learning: auto-downtier browser scrapes to HTTP | E8/I9/R6 | 13 | 9c9a3dea-8805-422e-8153-901ddf6d5476 |
| 116 | moonshot | Scraping Engines | WASM Plugin Sandbox | Deterministic golden-output regression harness | E5/I7/R3 | 13 | c6886deb-5f37-487c-9df0-b11ecc2d8620 |
| 119 | moonshot | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Universal regulatory-data freshness fabric | E7/I8/R4 | 13 | 43675d13-974f-4dc1-b44b-4d24ae99eef9 |
| 120 | moonshot | Public Funding & Grants Apps | US Grant Opportunities | Autonomous first-draft application generator | E8/I9/R6 | 13 | 954a4f32-4684-49d6-ac30-488d476fddfa |
| 125 | moonshot | Scraping Runtime Core | Tiered Fetcher & Politeness | Adaptive politeness controller from response signals | E6/I8/R5 | 13 | 86eb2823-2e3a-4aea-9053-367376158588 |
| 129 | business | Content & Research Apps | Web Research & Readable Content | Position readable as a paid any-URL-to-Markdown API | E6/I8/R5 | 13 | b4aae27a-f097-45d0-81be-56267f21cce5 |
| 135 | business | Scraping Runtime Core | App & Job Model | Paid priority lanes on the job queue | E3/I6/R2 | 13 | 07141385-5c46-43ee-b9e1-34f8877fb9ca |
| 145 | feature | Job Server & API | Configuration & Data Source Catalog | Per-source credential health check endpoint | E5/I7/R3 | 13 | de8344db-ed85-48bd-8883-aed75f7eaaa8 |
| 146 | feature | Job Server & API | Configuration & Data Source Catalog | Catalog drift check API and dashboard endpoint | E5/I7/R3 | 13 | 8fff20ab-9454-4eb1-bb69-a4a93f5964c4 |
| 147 | feature | Content & Research Apps | Web Research & Readable Content | Keyword & score-threshold alerts for hackernews | E3/I6/R2 | 13 | 06c21a52-01a7-4c83-9d9d-8afb7b15720a |
| 148 | feature | Content & Research Apps | Web Research & Readable Content | Rich article metadata + reading time in readable | E5/I7/R3 | 13 | 55185a93-c854-441d-9caf-c2b355ed5998 |
| 150 | feature | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Skills-demand ranking per occupation | E5/I7/R3 | 13 | 08a7b266-68cb-4bb7-8fa2-046a39727e29 |
| 156 | feature | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Screenshot, PDF and resource-blocking in browser engine | E5/I7/R3 | 13 | 5c86a9b2-d2b3-44ae-826e-49a7ec060e24 |
| 162 | moonshot | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | National skills-gap early-warning radar | E7/I8/R5 | 12 | 81ec7214-f843-448d-8b63-d609f96afd46 |
| 167 | moonshot | Scraping Runtime Core | Tiered Fetcher & Politeness | Stale-while-revalidate cache across all tiers | E5/I7/R4 | 12 | 8a13626c-0997-4c9d-8f0e-4f27c6998afc |
| 169 | business | Data Extraction & Storage | Dataset Store & Change Detection | Data-quality dedup API on SimHash (scale past O(n2)) | E5/I7/R4 | 12 | d23f8ba1-84ad-4416-8e85-a5dd76a7c1d1 |
| 172 | business | Job Server & API | HTTP API & Routes | Embedded operator console served from the binary | E6/I7/R3 | 12 | b67bcf9f-f9fb-436b-99f6-d2b223c031a0 |
| 174 | business | Job Server & API | App Registry | Plugin connectors as first-class registry apps | E7/I8/R5 | 12 | 02404907-2022-4ffb-8ca4-8f4bf0991830 |
| 178 | business | Public Funding & Grants Apps | US Grant Opportunities | Past-winner enrichment via USAspending join | E7/I8/R5 | 12 | 0f9845ca-7651-4bd0-8065-dc9d21ded930 |
| 187 | feature | Public Funding & Grants Apps | US Grant Opportunities | Cross-source duplicate collapse via SimHash | E3/I6/R3 | 12 | d7dd9f0c-8ebb-4904-adf3-e6ea3e3db157 |
| 189 | feature | Data Extraction & Storage | Declarative Extraction Engine | LLM-assisted extraction fallback via Claude engine | E7/I8/R5 | 12 | 962a75c2-e8c8-4df3-9b87-942c11f18041 |
| 192 | moonshot | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Claude computer-use solves login and CAPTCHA walls | E9/I9/R7 | 11 | 54c3a9c6-c574-4b49-b2f6-c984a431397b |
| 194 | moonshot | Job Server & API | Configuration & Data Source Catalog | Planned-source autopilot: catalog rows self-build | E9/I9/R7 | 11 | b46cae09-88e6-47f9-8905-2644bbc23e37 |
| 199 | moonshot | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Self-updating reference tables via auto-regen loop | E7/I8/R6 | 11 | 791b22a2-6cba-4494-b73a-371bf8f73efe |
| 201 | moonshot | Data Extraction & Storage | Dataset Store & Change Detection | Cross-source entity resolution and record linkage | E9/I9/R7 | 11 | c4993fda-6a22-498b-9344-0b6229a70139 |
| 206 | business | Content & Research Apps | Web Research & Readable Content | Longitudinal tech-trend index from the stories dataset | E6/I7/R4 | 11 | 01675fb9-03b9-465f-a0cc-5894c6da0e11 |
| 207 | business | Job Server & API | HTTP API & Routes | Cross-dataset join queries for composite intelligence | E8/I8/R5 | 11 | d56b5cc8-9298-4115-bb60-103ea7e87f53 |
| 217 | feature | Public Funding & Grants Apps | US Grant Opportunities | Award-amount extraction and range filtering | E4/I6/R3 | 11 | 2e82222c-72c3-4487-9fb2-b996085095eb |
| 219 | feature | Data Extraction & Storage | Declarative Extraction Engine | Add XPath rule type to the extraction engine | E6/I7/R4 | 11 | 1bfb19ba-06f3-415a-ad29-5a8b5135d006 |
| 222 | feature | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Rotating proxy pool for the HTTP engine | E5/I7/R5 | 11 | aca3bdbb-0b07-43c7-9d47-690a4b9740c9 |
| 223 | feature | Scraping Engines | WASM Plugin Sandbox | Chain plugins into a transform pipeline | E4/I6/R3 | 11 | 8c46d058-ea15-4a6d-884e-872567a5458c |
| 228 | moonshot | Job Server & API | App Registry | Typed capability graph auto-composes app pipelines | E8/I8/R6 | 10 | 21df5aaf-5951-43bf-91fc-0a2cdb937900 |
| 237 | moonshot | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Occupation career-path and mobility recommender | E8/I8/R6 | 10 | ccc328dd-62ee-462f-8ff6-f51d7321c903 |
| 241 | moonshot | Scraping Runtime Core | Engine Capability Traits | Stealth anti-bot Browser engine tier | E8/I8/R6 | 10 | 8eb9cec8-24f1-4a5e-8639-df3e9b2f688b |
| 246 | business | Job Server & API | Live Events & Webhooks | Human-channel notifications: Slack/email digest sink | E5/I6/R3 | 10 | 5145b676-59d0-40b0-be71-aaf5fcde706c |
| 253 | feature | Data Extraction & Storage | Dataset Store & Change Detection | LSH bucketing to scale SimHash dedup beyond O(n2) | E6/I7/R5 | 10 | 6b6f7e65-ddbc-43e6-94a7-13ae5fc9d309 |
| 255 | moonshot | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Adaptive stealth: fingerprint and TLS rotation | E8/I8/R7 | 9 | 8e8572a4-25e3-4029-b5af-4f3db64861a5 |
| 256 | moonshot | Scraping Engines | WASM Plugin Sandbox | Governed host capabilities for multi-step plugins | E8/I8/R7 | 9 | aaeba0e5-1171-4c15-96b3-28b49b358a69 |
| 262 | feature | Data Extraction & Storage | Dataset Store & Change Detection | LSH banding to replace O(n2) duplicate scan | E5/I6/R4 | 9 | feca8f0e-2627-45df-853c-fe8abde0da07 |
| 263 | feature | Data Extraction & Storage | Declarative Extraction Engine | XPath rule support alongside CSS | E5/I6/R4 | 9 | 395b44dd-ca7b-4947-8109-850fdc196286 |
| 267 | feature | Scraping Engines | WASM Plugin Sandbox | Precompiled module cache for fast cold start | E5/I6/R4 | 9 | f013daa6-c21c-45ba-9d20-c6a11536833b |
| 268 | moonshot | Job Server & API | Configuration & Data Source Catalog | Self-tuning politeness: governor learns safe per-host RPS | E7/I7/R6 | 8 | d451cca2-481f-42cf-9ae3-df573b83e27f |
| 271 | moonshot | Scraping Runtime Core | Engine Capability Traits | Live streaming render capability for real-time pages | E7/I7/R6 | 8 | 0c6d127b-b2ec-44aa-b515-fbf4be3a59fe |
| 274 | feature | Job Server & API | App Registry | App self-test / dry-run endpoint | E6/I6/R4 | 8 | 498faad5-1c61-4430-b7d2-682fd91086c0 |
| 275 | feature | Scraping Engines | WASM Plugin Sandbox | Curated host imports for regex and logging | E5/I6/R5 | 8 | 36f2c33a-d460-450f-8ea9-f4cfd0010e2a |

### T2 Cost & budget governance (24)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 1 | business | Scraping Runtime Core | Tiered Fetcher & Politeness | Cost ledger: meter every fetch tier for usage pricing | E4/I9/R2 | 21 | 339c6e24-6741-45e0-b839-cc208d5779ba |
| 7 | business | Data Extraction & Storage | Declarative Extraction Engine | LLM-ready Markdown endpoint sold as token-cost saver | E3/I8/R3 | 18 | 505d9b9c-92a8-4ce7-a3cb-ed151012a5ed |
| 10 | moonshot | Scraping Runtime Core | Tiered Fetcher & Politeness | Budget-governed escalation to the Claude tier | E4/I8/R3 | 17 | 49ff07df-6d10-4ce1-a6c4-1417aafb3b6e |
| 13 | business | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Fair-Wage Hiring Report as Ledgerline premium upsell | E4/I8/R3 | 17 | 53ec3cd3-d525-4952-87d1-d64f2a4dbcc6 |
| 16 | business | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Provenance ledger: certified lineage for every record | E5/I8/R2 | 17 | fead488f-90c9-4b1b-a5be-5b6ef5cd8c45 |
| 28 | business | Data Extraction & Storage | Broad Crawler | Metered Crawl-as-a-Service API with per-page billing | E6/I9/R5 | 16 | cf2c5463-5cb4-4bee-9fe5-a1391b659857 |
| 50 | moonshot | Scraping Runtime Core | Tiered Fetcher & Politeness | Fetch provenance ledger with deterministic replay | E6/I8/R3 | 15 | 7c70a38d-ea8c-480a-930f-f13a3c4bc523 |
| 58 | business | Content & Research Apps | Web Research & Readable Content | Metered research API with cost-plus credit billing | E5/I8/R4 | 15 | d4bb702c-4c38-48ba-afcb-8a3189f2c4cc |
| 60 | business | Job Server & API | Job Worker & Cron Scheduler | Per-job cost and yield ledger (cost-per-record) | E6/I8/R3 | 15 | aa627187-2db7-462d-8600-3653ec0ae565 |
| 75 | feature | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Cost-aware caching for the Claude research engine | E5/I8/R4 | 15 | 35bff360-b301-4585-a254-822bbfc68a89 |
| 82 | moonshot | Data Extraction & Storage | Broad Crawler | Duplicate-density subtree pruning for smart budgets | E6/I8/R4 | 14 | 0290f330-52b7-45ca-967f-34650f2c8236 |
| 84 | moonshot | Content & Research Apps | Extraction, Crawl & API Watch | Tamper-evident provenance ledger for every fetch | E6/I8/R4 | 14 | e18c0be0-3327-48df-9783-ff0853471f05 |
| 90 | business | Scraping Runtime Core | Engine Capability Traits | Intelligence tiers: package Claude roles with cost margin | E4/I7/R3 | 14 | 24e12065-6d24-4613-8e41-4f0009767282 |
| 92 | business | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Cross-engine cost intelligence and ROI dashboard | E5/I7/R2 | 14 | 9a692cbb-69f9-4663-b7d8-1a2ccdbb9821 |
| 95 | business | Scraping Engines | Full-Text Search Index | Metered public search API over curated datasets | E6/I8/R4 | 14 | 45b981fa-008f-43c5-ab9f-444af5c69c1b |
| 138 | business | Job Server & API | HTTP API & Routes | Metered data API: keys, quotas, and usage accounting | E7/I8/R4 | 13 | 64273e00-5309-49ea-a7e6-c4e425eff5f1 |
| 140 | business | Scraping Engines | WASM Plugin Sandbox | Fuel-metered billing: sell compute by the instruction | E5/I7/R3 | 13 | d17c1526-391a-4b68-b869-dda58888b8ff |
| 141 | business | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | EU budget-landscape intelligence from budgetOverview | E5/I7/R3 | 13 | 2e0771d7-5338-4260-8d05-98309c5a3ce9 |
| 160 | moonshot | Job Server & API | Job Worker & Cron Scheduler | Budget-aware scheduler for Claude-powered jobs | E7/I8/R5 | 12 | c0b0740a-51d4-43c1-be87-242cc7776829 |
| 163 | moonshot | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Price-to-win quoting copilot for a specific bid | E7/I8/R5 | 12 | 242555c0-3d9f-44b9-a721-b47b28f93965 |
| 181 | feature | Job Server & API | App Registry | Per-app parameter JSON Schema for discovery | E6/I7/R3 | 12 | 3b92f6cf-d70a-4134-9275-01737bd5b292 |
| 185 | feature | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Metered-run cost budget ceiling with abort | E5/I7/R4 | 12 | 921ad081-9e3d-4baf-a971-5b69084bd9d1 |
| 231 | moonshot | Job Server & API | Live Events & Webhooks | Audit-grade event ledger with state rebuild | E7/I7/R4 | 10 | 7f019b78-4e79-470b-8bbf-3b0753cf090b |
| 269 | moonshot | Job Server & API | App Registry | Tenant-scoped registry views with quotas | E6/I6/R4 | 8 | 8584c1f8-8a9c-43c3-9b7d-95714fb7f38e |

### T3 Provenance & trust (17)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 19 | feature | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Capture source citations for agentic datasets | E4/I8/R3 | 17 | 1e8d4a95-12f9-400e-b99a-76b3e68d7d66 |
| 31 | business | Scraping Runtime Core | App & Job Model | Provenance vault: signed raw-artifact evidence per record | E5/I8/R3 | 16 | 53690cdb-3e70-42f1-8a63-ba519ec84b0a |
| 39 | feature | Content & Research Apps | Web Research & Readable Content | Research report exported as cited Markdown | E3/I7/R2 | 16 | 5a1054ac-dafb-47a4-aabb-dce6d07c613b |
| 46 | moonshot | Job Server & API | Configuration & Data Source Catalog | Signed data provenance passport on every record | E6/I8/R3 | 15 | 83101946-a742-48f9-a699-ed3f91a2d6fc |
| 71 | feature | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Per-figure confidence score on research outputs | E3/I7/R3 | 15 | 40a5d7b4-8fd4-481f-b288-a6d021352bc9 |
| 88 | business | Data Extraction & Storage | Broad Crawler | Compliance-grade crawl audit trail for enterprise buyers | E4/I7/R3 | 14 | 867adba4-bc41-4a62-bcda-0f6fb3a77cc4 |
| 121 | moonshot | Data Extraction & Storage | Declarative Extraction Engine | Per-field extraction confidence and provenance scores | E5/I7/R3 | 13 | 22b9ca83-7518-4ca1-94d7-7679c3087dd2 |
| 126 | moonshot | Content & Research Apps | Web Research & Readable Content | Multi-agent research swarm with cross-verification | E8/I9/R6 | 13 | 3e7cc2b2-6847-4fba-b9da-42bd569bce9c |
| 136 | business | Scraping Runtime Core | Tiered Fetcher & Politeness | Compliance-grade politeness: robots.txt + audit trail | E5/I7/R3 | 13 | c5b2aa51-8241-419d-aa92-716461627ec0 |
| 137 | business | Job Server & API | HTTP API & Routes | Ask-the-data: NL question endpoint with citations | E6/I8/R5 | 13 | f7fe8f89-3358-45b5-85c1-8ce70cb5ccb9 |
| 164 | moonshot | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Adversarial dual-agent verification of every figure | E7/I8/R5 | 12 | e33d0ce7-5c38-4f4f-be17-87aa6837148d |
| 176 | business | Job Server & API | Configuration & Data Source Catalog | Downstream quality feedback loop updates confidence | E6/I7/R3 | 12 | 15701128-57bf-47f7-b76f-50637fd7e75a |
| 205 | business | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Provenance and freshness metadata to resell reference data | E6/I7/R4 | 11 | aca8619c-6fdd-4673-b494-473c668c2d6f |
| 236 | moonshot | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Verifiable fair-pay certification for Czech employers | E6/I7/R5 | 10 | 0c7880b1-3aeb-4657-aa5b-2752c1ea9451 |
| 238 | moonshot | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Cited trades reference-data as an external B2B API | E8/I8/R6 | 10 | edea6bb9-c450-4ad5-b427-32575fc7d0c3 |
| 242 | moonshot | Scraping Runtime Core | Engine Capability Traits | Signed WASM plugin marketplace with capability grants | E8/I8/R6 | 10 | ab5c5a35-6712-490e-b18f-35c49e33deec |
| 243 | moonshot | Content & Research Apps | Web Research & Readable Content | Real-time claim-verification firewall for any page | E8/I8/R6 | 10 | 077b1080-fcf3-4706-8d8f-a25508553a84 |

### T4 Search & answer layer (35)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 21 | moonshot | Scraping Engines | Full-Text Search Index | Answer engine: NL questions over scraped corpus | E6/I9/R5 | 16 | 9de0c32c-cce0-43d5-beeb-f0673db5fc61 |
| 30 | business | Scraping Runtime Core | Engine Capability Traits | Corpus search API: sell queries over scraped datasets | E5/I8/R3 | 16 | 9a6a41fe-2f90-4e26-b237-1c615a52fd72 |
| 44 | feature | Scraping Engines | Full-Text Search Index | Highlighted result snippets for search hits | E3/I7/R2 | 16 | 83280327-3a64-445e-a578-19e6497b5714 |
| 48 | moonshot | Scraping Runtime Core | Engine Capability Traits | Hybrid semantic + BM25 search over scraped corpus | E7/I9/R5 | 15 | 617f51c7-b772-4d15-b119-fb803993fb91 |
| 51 | moonshot | Content & Research Apps | Web Research & Readable Content | Conversational RAG over everything you've scraped | E7/I9/R5 | 15 | 5442372c-f6fd-4169-960a-5b603a11210b |
| 52 | moonshot | Content & Research Apps | Extraction, Crawl & API Watch | Goal-directed semantic crawler steered by an LLM | E7/I9/R5 | 15 | a34b9316-48a1-4906-b013-549511360c6d |
| 57 | business | Content & Research Apps | Web Research & Readable Content | Chained recipes: research feeds crawl and extraction | E7/I9/R5 | 15 | 7ed376f9-e2fe-4fe1-a655-3b6c498d5554 |
| 64 | business | Public Funding & Grants Apps | US Grant Opportunities | 50-state grants corpus: the coverage flywheel | E8/I9/R4 | 15 | 1b9a93c6-9baf-4725-acf3-b818c6d141ca |
| 68 | feature | Content & Research Apps | Web Research & Readable Content | Read-later library with full-text search | E6/I8/R3 | 15 | 4f287888-f1aa-41a6-a3e4-495d7c851040 |
| 87 | business | Data Extraction & Storage | Dataset Store & Change Detection | Warehouse-native exports: Parquet, S3, BigQuery connectors | E6/I8/R4 | 14 | 5c93790c-511c-4c51-a96e-aea94623b65c |
| 104 | feature | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Average-wage benchmark derived from CBP payroll | E2/I6/R2 | 14 | 8f5aae29-a68b-430b-b2d5-2b9dde279736 |
| 112 | feature | Scraping Engines | Full-Text Search Index | Faceted filtering by app and dataset | E4/I7/R3 | 14 | 2c9df19f-08d1-4219-ba77-32a77b2527ca |
| 130 | business | Content & Research Apps | Web Research & Readable Content | Compounding research memory across runs and topics | E8/I9/R6 | 13 | c2572503-6d4b-41d1-bd02-d7d03e777539 |
| 143 | feature | Data Extraction & Storage | Declarative Extraction Engine | Natural-language to RuleSet generation via Claude | E6/I8/R5 | 13 | 92b8a11d-5cbf-426d-ba65-8a8d24ca3ef2 |
| 159 | moonshot | Job Server & API | HTTP API & Routes | Hybrid semantic + BM25 search over all records | E7/I8/R5 | 12 | 060d2ad1-fdbb-454a-9225-3d62afd02d80 |
| 165 | moonshot | Data Extraction & Storage | Dataset Store & Change Detection | Natural-language querying over accrued datasets | E7/I8/R5 | 12 | 368685c6-c7ba-41d2-b1b1-04744ee09638 |
| 173 | business | Job Server & API | App Registry | Coverage map: catalog-vs-registry gap as a roadmap | E4/I6/R2 | 12 | 66281144-d83f-426e-9d11-bb299d5a7951 |
| 177 | business | Scraping Engines | Full-Text Search Index | Hybrid BM25 + embedding semantic search layer | E7/I8/R5 | 12 | f5fbf3ca-7bae-4ac3-b5df-0a55c39ec6e4 |
| 179 | feature | Job Server & API | HTTP API & Routes | Cursor pagination for jobs, records, and search | E6/I7/R3 | 12 | 12694c51-44e2-4554-bd0e-eb73404029ae |
| 184 | feature | Job Server & API | Configuration & Data Source Catalog | Connector-docs catalog served as searchable API | E4/I6/R2 | 12 | 41950107-8a1f-4c65-acb6-2cf46a4e2f3d |
| 191 | feature | Scraping Engines | Full-Text Search Index | Delete-by-id and delete-by-dataset in Search | E3/I6/R3 | 12 | 283d6999-bbd7-4b75-ab6a-0cca9949386a |
| 193 | moonshot | Job Server & API | HTTP API & Routes | Temporal data lake: query any dataset as-of a date | E8/I8/R5 | 11 | 18a7f8c6-02ab-4baf-8a8b-fb125f500e4b |
| 196 | moonshot | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Roll-up acquisition target sourcing by fragmentation | E7/I8/R6 | 11 | f5559e05-a513-46b1-9dbd-55ae13e32d3a |
| 198 | moonshot | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Natural-language 'ask your market' assistant | E7/I8/R6 | 11 | a5a85812-ee29-4818-80bb-443afa59de02 |
| 210 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Queryable dataset filtering and field indexing | E6/I7/R4 | 11 | 3e9f46fe-3c27-4f67-be82-f0714b7505ae |
| 218 | feature | Public Funding & Grants Apps | US Grant Opportunities | Saved-search eligibility subscription profiles | E6/I7/R4 | 11 | dc0aef85-f088-4b8f-bc2b-e2e5d65f3129 |
| 224 | feature | Scraping Engines | Full-Text Search Index | Typo-tolerant fuzzy and phrase search | E4/I6/R3 | 11 | 65c50b7a-0869-444f-bf73-6040c5a610a2 |
| 225 | moonshot | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Autonomous research swarm with shared session memory | E8/I8/R6 | 10 | b78dbad0-e96f-4b62-a025-e50b210b6594 |
| 226 | moonshot | Scraping Engines | Full-Text Search Index | Sharded federated search for billion-doc scale | E8/I8/R6 | 10 | abb5bd42-22fc-47f3-9de4-2f00fad28f3b |
| 233 | moonshot | Job Server & API | Configuration & Data Source Catalog | Natural-language catalog discovery for apps | E5/I6/R3 | 10 | 8af31370-2332-40b5-ba05-04fd1a171d51 |
| 240 | moonshot | Data Extraction & Storage | Dataset Store & Change Detection | Vector-native semantic index over stored records | E8/I8/R6 | 10 | 29e490a4-0c64-45ef-8a4e-5d186090bd26 |
| 244 | moonshot | Content & Research Apps | Extraction, Crawl & API Watch | Compile natural-language extraction into cached WASM | E8/I8/R6 | 10 | 53c9b97c-a29f-4c7e-9015-280c7dec311e |
| 254 | feature | Scraping Engines | Full-Text Search Index | Autocomplete/typeahead suggestions endpoint | E5/I6/R3 | 10 | 567e0b36-49f6-4191-9b2c-bb597c91ea99 |
| 257 | moonshot | Scraping Engines | Full-Text Search Index | Learning-to-rank: usage-tuned relevance over BM25 | E7/I7/R5 | 9 | e6fad11b-aad5-4cfc-a1b4-176e83140b47 |
| 260 | moonshot | Data Extraction & Storage | Broad Crawler | On-crawl semantic router to auto-file pages by topic | E7/I7/R5 | 9 | edd171df-c877-4a7d-ba42-d60e90b858ab |

### T5 AI-assisted & self-healing extraction (13)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 15 | business | Job Server & API | Configuration & Data Source Catalog | Claude-powered source scout drafts new catalog entries | E6/I9/R4 | 17 | b0b6e72f-6527-42e7-9062-e41f2e2c4618 |
| 18 | feature | Data Extraction & Storage | Declarative Extraction Engine | Schema-less auto-extraction of article content | E6/I9/R4 | 17 | f4e82c6f-66c3-40e5-a3f9-4569b5d3b498 |
| 23 | moonshot | Scraping Runtime Core | App & Job Model | Self-healing scrapers via Claude repair loop | E8/I10/R6 | 16 | 43a385c0-10aa-442a-b586-a2c19959e9a4 |
| 27 | business | Data Extraction & Storage | Declarative Extraction Engine | AI rule-writer: Claude drafts RuleSets, Rust runs them free | E6/I9/R5 | 16 | eb9854a7-bbd1-4315-add6-0a16b2ee6576 |
| 76 | moonshot | Scraping Engines | WASM Plugin Sandbox | Prompt-to-plugin: Claude generates WASM extractors | E7/I9/R6 | 14 | 4551dc3a-2473-40bf-950f-a857db99e6f1 |
| 89 | business | Content & Research Apps | Extraction, Crawl & API Watch | Self-healing extraction rules via Claude repair loop | E7/I9/R6 | 14 | 633efd23-ab08-480a-9b6f-df9ac85fe257 |
| 113 | moonshot | Job Server & API | Job Worker & Cron Scheduler | Self-healing jobs: LLM-diagnosed engine fallback | E8/I9/R6 | 13 | a910de8d-566c-4363-97ad-bfefae428592 |
| 117 | moonshot | Scraping Engines | WASM Plugin Sandbox | Self-healing extractors that repair on site drift | E8/I9/R6 | 13 | 07f5ec90-e038-48c4-b628-409535094e2d |
| 122 | moonshot | Data Extraction & Storage | Declarative Extraction Engine | Extraction-by-demonstration: click examples to a RuleSet | E7/I8/R4 | 13 | d1a10671-c613-448f-be0e-1630f8a8eae3 |
| 123 | moonshot | Data Extraction & Storage | Declarative Extraction Engine | Self-healing selectors that auto-repair on redesigns | E8/I9/R6 | 13 | dd172fbc-cd39-4d2b-8f70-7fc2732c0a9d |
| 208 | business | Job Server & API | App Registry | Connector factory: Claude generates and tests extractors | E9/I9/R7 | 11 | f1e4c397-d35d-430b-90d4-49aab66f8f96 |
| 213 | feature | Content & Research Apps | Extraction, Crawl & API Watch | Auto-detect extraction schema from sample page | E8/I8/R5 | 11 | 1beb0327-2a50-4e7f-a015-9bcc7ac083a2 |
| 259 | moonshot | Data Extraction & Storage | Declarative Extraction Engine | Vision-model extraction from page screenshots | E8/I8/R7 | 9 | 69159f71-7ad0-458a-861c-9ed8a92e7288 |

### T6 Crawler maturity (18)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 38 | feature | Data Extraction & Storage | Broad Crawler | Sitemap.xml discovery and crawl-delay from robots.txt | E5/I8/R3 | 16 | f6edbeb7-6489-4cde-9f31-aa74159c66ec |
| 42 | feature | Data Extraction & Storage | Broad Crawler | Allow/deny URL pattern filters in CrawlConfig | E3/I7/R2 | 16 | 64cdbc45-9255-402a-9039-c66a706e1887 |
| 56 | business | Data Extraction & Storage | Broad Crawler | Vertical seed packs: prebuilt crawl configs as SKUs | E5/I8/R4 | 15 | 5923fa16-c9d9-4c3e-a3a6-68cd7dd4ce5a |
| 72 | feature | Public Funding & Grants Apps | US Grant Opportunities | Canonical unified grant schema across sources | E5/I8/R4 | 15 | 804037e7-a4d5-43e6-960b-2cb285c39546 |
| 74 | feature | Data Extraction & Storage | Broad Crawler | Sitemap.xml discovery for seed expansion | E4/I7/R2 | 15 | 84178e68-7967-4a1d-8edd-ebde5287763c |
| 98 | feature | Data Extraction & Storage | Broad Crawler | URL include/exclude patterns and canonicalization | E4/I7/R3 | 14 | aeef0195-3f23-4be9-8159-275a47f4f293 |
| 124 | moonshot | Data Extraction & Storage | Broad Crawler | Headless render bridge for JS-heavy SPA crawling | E8/I9/R6 | 13 | caa04ccc-b2a9-463f-9029-cc7e1f27d0a9 |
| 149 | feature | Content & Research Apps | Extraction, Crawl & API Watch | Sitemap seeding + include/exclude URL filters for crawl | E5/I7/R3 | 13 | eb7abeb5-e807-46cf-ad0d-f39640d2cbc0 |
| 155 | feature | Data Extraction & Storage | Broad Crawler | Canonical URL normalization to shrink frontier dupes | E3/I6/R2 | 13 | 89f4ad25-d7ad-4af9-9cce-c2a6906e6e88 |
| 161 | moonshot | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Territory white-space engine for expansion | E7/I8/R5 | 12 | 5b762302-349c-47be-80ce-7669e8a61488 |
| 190 | feature | Scraping Engines | Fetch Engines (HTTP / Browser / Claude) | Robots.txt and crawl-delay compliance | E5/I7/R4 | 12 | 9943f303-4d99-49f4-8495-fff7ae8f2001 |
| 202 | moonshot | Data Extraction & Storage | Broad Crawler | Distributed crawl swarm over a shared durable frontier | E9/I9/R7 | 11 | 8e37fb7e-04c6-4919-a751-25c67b74e6e7 |
| 211 | feature | Data Extraction & Storage | Broad Crawler | Best-first crawl scoring for relevance-guided crawls | E6/I7/R4 | 11 | 5a565d81-4e9e-49f9-85bc-81e404db2943 |
| 212 | feature | Data Extraction & Storage | Broad Crawler | Resumable crawl with persistent frontier checkpoint | E6/I7/R4 | 11 | ebc44974-db3d-4771-b22e-8b0d64651722 |
| 220 | feature | Data Extraction & Storage | Broad Crawler | Honor robots Crawl-delay and Sitemap directives | E4/I6/R3 | 11 | b83add58-d792-4d72-86bf-1a085f45b976 |
| 221 | feature | Data Extraction & Storage | Broad Crawler | Resumable crawl via persisted frontier state | E6/I7/R4 | 11 | 8dcb39cc-a4fd-41be-9e2d-46310bb0e7d9 |
| 247 | feature | Data Extraction & Storage | Broad Crawler | Live crawl progress stream and cancellation | E5/I6/R3 | 10 | c6e10372-ae7c-4071-aec3-109d9731bed2 |
| 272 | moonshot | Content & Research Apps | Extraction, Crawl & API Watch | Federated crawl fleet with shared frontier and dedup | E9/I8/R7 | 8 | 2011a323-b144-4f43-80bc-a2c5c32610fe |

### T7 API & integration surface (18)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 49 | moonshot | Scraping Runtime Core | App & Job Model | Manifest-driven no-code scraper authoring | E7/I9/R5 | 15 | 7a750de5-cdea-4090-b8bf-cc4a65cb5c7f |
| 100 | feature | Job Server & API | Live Events & Webhooks | Persistent webhook delivery log with replay | E6/I8/R4 | 14 | f44d36dc-1aaf-4fc3-b43c-aac9ee79cb13 |
| 103 | feature | Content & Research Apps | Extraction, Crawl & API Watch | Auto-follow pagination in extractor rule set | E6/I8/R4 | 14 | e88b00dd-9ded-4b44-919f-d9d22a621239 |
| 110 | feature | Data Extraction & Storage | Declarative Extraction Engine | Readability metadata extraction (title/author/date) | E6/I8/R4 | 14 | 57bd4d97-163e-48ad-9adb-0015967ad49d |
| 142 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Dataset export to CSV, JSON Lines, and Parquet | E5/I7/R3 | 13 | dbc5909f-c43b-474d-be0d-d3567db20b45 |
| 144 | feature | Job Server & API | HTTP API & Routes | Idempotency-Key on job enqueue | E5/I7/R3 | 13 | 0b61ffc5-0ef0-49f9-a688-2a3662b68962 |
| 152 | feature | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Actionable freshness webhook for stale reference data | E4/I7/R4 | 13 | 5a679625-a947-46e2-a30d-d931fcd9bcd3 |
| 154 | feature | Data Extraction & Storage | Dataset Store & Change Detection | Dataset export to CSV/JSONL/Parquet | E5/I7/R3 | 13 | 05a0c752-5df6-4425-a1e5-afd00ba8af86 |
| 180 | feature | Job Server & API | HTTP API & Routes | Streaming CSV and NDJSON dataset export | E5/I7/R4 | 12 | 83d77c80-9c05-42d7-b43f-fbe625c2f174 |
| 182 | feature | Job Server & API | Live Events & Webhooks | SSE event IDs with Last-Event-ID replay | E5/I7/R4 | 12 | 17a74710-3937-47b3-b137-8b73f25835ae |
| 183 | feature | Job Server & API | Live Events & Webhooks | Dead-letter queue for exhausted webhook deliveries | E5/I7/R4 | 12 | c83b4d3e-92f8-403d-baa1-fc1f1261a909 |
| 227 | moonshot | Job Server & API | HTTP API & Routes | Columnar lakehouse export (Parquet/Arrow) for BI | E7/I7/R4 | 10 | 7f661412-319a-476b-9500-ccd7be7c05cc |
| 230 | moonshot | Job Server & API | Job Worker & Cron Scheduler | Upstream-aware triggers replace fixed cron guesses | E6/I7/R5 | 10 | 5098b8ea-a20e-48bf-8656-23d940968057 |
| 250 | feature | Job Server & API | Live Events & Webhooks | Subscribable event types beyond terminal-only webhooks | E5/I6/R3 | 10 | e87e749e-1190-4453-b9cc-ba5ec212fcf1 |
| 251 | feature | Economic & Labor Market Data Apps | US Trades Business Density (Census) | GeoJSON export for market-density mapping | E5/I6/R3 | 10 | 03ab9ad2-be3c-4e18-a273-fe2747046a97 |
| 264 | feature | Job Server & API | HTTP API & Routes | Generated OpenAPI spec and Swagger UI | E6/I6/R3 | 9 | 6c051cfa-5a12-4a59-9ab0-721ca8a7833a |
| 273 | feature | Job Server & API | HTTP API & Routes | Optional API-key auth for non-localhost callers | E5/I6/R5 | 8 | 23152939-743d-4207-b4f6-c8e12e6d1bd5 |
| 276 | feature | Job Server & API | Configuration & Data Source Catalog | Hot-reload config without restarting the service | E6/I6/R5 | 7 | e4b95bbe-cc59-462c-8ff8-fc7768948f3e |

### T8 Scheduler & worker robustness (7)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 99 | feature | Job Server & API | Job Worker & Cron Scheduler | Manual retry and requeue for failed jobs | E4/I7/R3 | 14 | ead7219e-dcd8-4aa7-b6d1-9c27bc6c8450 |
| 166 | moonshot | Scraping Runtime Core | App & Job Model | Job DAG orchestration for multi-stage pipelines | E7/I8/R5 | 12 | 5dc7b7f2-444d-4e16-b2d3-789a7e12c23c |
| 175 | business | Job Server & API | Job Worker & Cron Scheduler | QoS lanes: reserved capacity for on-demand jobs | E5/I7/R4 | 12 | f41dd984-9755-4bde-a143-b7000666002c |
| 248 | feature | Job Server & API | Job Worker & Cron Scheduler | Prevent overlapping runs of the same schedule | E4/I6/R4 | 10 | cc245097-a00c-4d89-bbc2-e6b618ef0d9c |
| 249 | feature | Job Server & API | Job Worker & Cron Scheduler | Schedule misfire and catch-up policy | E6/I7/R5 | 10 | e2ae6ccb-1187-4b83-9e5f-b82c419814d5 |
| 266 | feature | Job Server & API | Job Worker & Cron Scheduler | Configurable retry backoff policy per job | E5/I6/R4 | 9 | 139bd7de-9a49-41a4-ba4f-a80466a7523a |
| 270 | moonshot | Job Server & API | Job Worker & Cron Scheduler | Elastic worker fleet with distributed job claim | E9/I8/R7 | 8 | 1a97273a-c7eb-4944-a969-6d9e6d206540 |

### T9 Domain data products (apps) (29)

| # | Scanner | Group | Context | Title | E/I/R | Score | Idea ID |
|---:|---|---|---|---|---|---:|---|
| 12 | business | Economic & Labor Market Data Apps | US Trades Business Density (Census) | 'Where to launch' market-selection report as a product | E4/I8/R3 | 17 | d7b6a23a-be19-443c-9533-695ed9a143b6 |
| 20 | feature | Public Funding & Grants Apps | US Grant Opportunities | Closing-soon deadline digest for open grants | E4/I8/R3 | 17 | 710594e3-5be4-44e6-ad83-aa14c8127d0b |
| 24 | business | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Posted-vs-official salary gap benchmarking API | E5/I8/R3 | 16 | b7285ec8-3778-45b2-8324-3fe2dccf7d2c |
| 25 | business | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Tax set-aside autopilot from the tax dataset | E6/I9/R5 | 16 | d51301d9-5f38-49a8-b4e2-4a163a987845 |
| 40 | feature | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Blended employer + solo total-market view | E5/I8/R3 | 16 | b3097e68-d2c3-43c8-a5ac-5e96427e0468 |
| 47 | moonshot | Public Funding & Grants Apps | US Grant Opportunities | Grant-fit copilot: org profile finds the money | E7/I9/R5 | 15 | 64552b20-6bb9-4b9e-8729-96a661365668 |
| 53 | business | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Fused Trades Market Opportunity Index across datasets | E7/I9/R5 | 15 | 799366fe-e726-4d75-a771-c6932dd32798 |
| 55 | business | Economic & Labor Market Data Apps | US Trades Wages, Tax & Valuation | Per-state wage bands to unlock localized pay guidance | E5/I8/R4 | 15 | e7f291e1-08ef-4757-b314-a74feb8ee198 |
| 59 | business | Scraping Runtime Core | Engine Capability Traits | WASM extractor marketplace on the Plugins sandbox | E7/I9/R5 | 15 | ddc8f0f8-13e2-4c83-b00c-eccdec1f5133 |
| 69 | feature | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Composite geographic launch score | E5/I8/R4 | 15 | ad2e4938-b1db-47c4-bcd2-aded35f1c5ae |
| 70 | feature | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Cross-source salary calibration vs ISPV distribution | E5/I8/R4 | 15 | 363076b2-62e4-4958-998d-57f5dd1f8ea6 |
| 79 | moonshot | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Consumer salary-negotiation copilot for job seekers | E6/I8/R4 | 14 | 20bd7fad-1715-451d-98e1-ce27a812971d |
| 85 | business | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Enable annual runs and build YoY market-trend layer | E4/I7/R3 | 14 | cfff9ead-6484-4a18-b2fb-93504c5de5b6 |
| 86 | business | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Czech labour-market pulse: trending and fading roles | E6/I8/R4 | 14 | a72fc621-64c3-4629-b2ab-ae102a523fee |
| 94 | business | Scraping Engines | WASM Plugin Sandbox | Community extractor marketplace on the WASM sandbox | E8/I9/R5 | 14 | 4f04b06d-7711-4357-8ad5-40e1a1d04544 |
| 128 | business | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | ARES employer enrichment for segment-level salaries | E5/I7/R3 | 13 | b43376ab-31ec-48e6-bbaf-a0d0a4941b68 |
| 133 | business | Content & Research Apps | Extraction, Crawl & API Watch | WASM extraction plugin marketplace with revenue share | E8/I9/R6 | 13 | 713f5011-5337-4f7b-9b91-617ee85b56c5 |
| 134 | business | Scraping Runtime Core | App & Job Model | App marketplace: registry becomes a distribution channel | E8/I9/R6 | 13 | ede2c273-af88-4e04-bd9a-b087d096382c |
| 153 | feature | Public Funding & Grants Apps | EU & Regulatory Funding Watchers | Clean-text enrichment of SEDIA descriptions | E5/I7/R3 | 13 | 5c873722-7d39-4611-a8b5-9513e46c34c6 |
| 195 | moonshot | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Trades demand forecast from housing-permit growth | E7/I8/R6 | 11 | f3ab0261-8426-4055-a072-bc50243ae83c |
| 197 | moonshot | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Interactive live map of the Czech labour market | E6/I7/R4 | 11 | f730dacf-22bd-4566-af03-44190e38868d |
| 204 | business | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Data-backed job-posting assistant for Czech SMB employers | E6/I7/R4 | 11 | da2bbfc6-d31c-4c39-88b3-fa507980ac04 |
| 214 | feature | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Year-over-year market growth trend ranking | E6/I7/R4 | 11 | a42f0fbf-03ea-4f63-84aa-81d081181e9d |
| 215 | feature | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Education-level salary premium dimension | E4/I6/R3 | 11 | 1ee9fe65-ce15-44b3-a065-3d55722f8ccc |
| 216 | feature | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | ARES employer-size enrichment for vacancy samples | E6/I7/R4 | 11 | adf4c395-3861-4eec-91f0-0a06b95b3076 |
| 234 | moonshot | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Solo-operator market-risk score for lenders | E8/I8/R6 | 10 | 5393af28-f9e4-4e5f-9379-244a45907714 |
| 235 | moonshot | Economic & Labor Market Data Apps | US Trades Business Density (Census) | Interactive nationwide trades market atlas | E7/I7/R4 | 10 | a0bc5034-3e55-4daa-bff7-aca6ab1b1897 |
| 245 | business | Economic & Labor Market Data Apps | Czech Labour Market (MPSV) | Kraj opportunity map licensed to boards and municipalities | E5/I6/R3 | 10 | 5bf3368f-59a5-4a4b-ad51-2c254ee9c914 |
| 265 | feature | Job Server & API | App Registry | Expose app tags and market for filtered discovery | E4/I5/R2 | 9 | 90c732c2-e2f8-407c-9e60-74dd8791f09f |
