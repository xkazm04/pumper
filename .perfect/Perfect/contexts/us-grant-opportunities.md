---
name: "US Grant Opportunities"
type: perfect/context
group: "Public Funding & Grants Apps"
category: lib
opportunity: 6
last_proposed: 2026-07-13
cooldown_until: —
directions: ["[[grants-searchable-alerts]]", "[[grants-lifecycle-honesty]]", "[[grants-query-surface]]", "[[grants-schema-enrichment]]"]
---

## Current state (scout brief digest, 2026-07-13)

- grants-gov (Search2 POST, daily 09:00 UTC, 2500-row cap, upsert_many, key id→number) + ca-grants (CKAN datastore, daily 09:30, same cap/pattern, key PortalID) → grants-common normalizes to grants/unified (`<source>:<id>`, 12-field canonical) + SimHash duplicate_links (cross-source only, distance 3).
- **Grants never reach full-text search**: worker indexes only records/stories/items result arrays; grants jobs index as ONE blob (worker.rs:496-517) → /search and the entire saved-search alert rail are useless for grants. THE consumer gap.
- **Lifecycle broken**: upsert-only, removed_at never fires — expired/delisted grants persist as `open` forever. 2500-cap makes sync_many unsafe (planned grants-gov-xml bulk extract would fix; catalog data-sources.toml:48-61, unbuilt).
- **Federal money fields always null** (Search2 doesn't return amounts); CA money_of lossy ("$1.5M"→1.5, ranges garbage). No category/eligibility/ALN in canonical schema.
- Silent schema drift: unwrap_or_default → fetched:0 as success; only page1.json artifact for forensics. No filters on unified (export-and-filter only); closingSoon is a federal-only artifact.
- Dataset change-feed cursor already generic (.../changes) — "metered corpus API" backlog item is mostly a filtered read surface.

## Direction history
- 2026-07-13 (round 3): 5 proposed, 4 accepted (searchable+alerts, lifecycle honesty, query surface, schema enrichment). **REJECTED**: grants-gov-xml bulk full-snapshot source (wildcard) — new-source acquisition deferred; implies: improve existing sources first. Note: agency-behavior-intelligence backlog item stays blocked on this rejection (needs amounts + true lifecycle).

## Shipped
- [[grants-searchable-alerts]] → 94940a9 — generic worker `index_datasets` seam (compact results + per-record docs, id `<app>:<dataset>:<key>`); live-verified: 20 grants searchable, search.matched webhook delivered. CAVEATS recorded: (1) full-dataset re-index per run — needs incremental indexing before large datasets adopt the seam; (2) saved-search app-scoping nuance — scope grant alerts by dataset:"unified" with NO app filter (worker scopes by job app, docs updated; a worker-side fix is a future direction candidate).
- [[grants-lifecycle-honesty]] → 9d18132 — sweep_closed (open|forecasted past close_date → closed), drift guard (hitCount>0 + fetched:0 → FAIL; title-null >50% → warnings), one shared date parser. Positive flip proven by unit test (live corpus had zero natural candidates — honest).
- [[grants-schema-enrichment]] → d59b307 — categories/eligibilities/aln; builder VERIFIED real CA columns against live sample (scout's guessed columns did not exist — real: EstAmounts range string "Between $X and $Y" → floor/ceiling, EstAvailFunds → total_funding). K/M/B + range money parser. Live: 80 CA rows enriched (41 with ranges), aln on all 20 federal.
- [[grants-query-surface]] → c526d9f — generic JsonFilter (Eq/Contains/Gte/Lte/NumGteAny with json_type guard; paths bound as params, not interpolated) + Datasets::list_filtered; GET /grants (status/agency/source/closing_before|after/min_award + cursor keyset) and GET /grants/closing-soon (cross-source, days 1–365). Live: every filter cross-checked against direct SQL over 1,988 real records; cursor walks at odd limits; tombstoned records excluded.
Context COMPLETE: 4/4 shipped.

## Known gaps / future direction seeds (surfaced during build)
- `min_award` excludes ALL federal records — grants.gov Search2 publishes no amounts; a detail-endpoint enrichment fetch would fix it (separate direction).
- `sweep_closed` (O(n) full read) + `link_duplicates` (O(n²) SimHash) run on EVERY run of BOTH apps over the whole unified corpus — the real scaling cliff, well before json_extract scans matter.
- Saved-search app-scoping: worker scopes by JOB app; unified docs carry app `grants` — alerts must use dataset:"unified" with no app filter until a worker-side fix.
- `index_datasets` re-indexes the full dataset each run — needs incremental indexing before very large datasets adopt the seam.
