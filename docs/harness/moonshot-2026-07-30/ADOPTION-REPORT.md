# Adoption Report — per-client propagation (2026-07-31)

> Branch `vibeman/adoption-2026-07-31` off master `582d20f`. 5 Opus agents, one per client group, disjoint crates. **Tests 862/0 → 901/0** (+39). No big-bang: every adoption is per-app, and every default-OFF flag stays OFF.

## Before → after

| Seam | Before | After |
|---|---|---|
| Provenance stamps (M12) | 0 apps | **17 apps** stamping something real; the rest deliberately Null |
| Data contracts (M20) | 2 of 10 live sources | **10 of 10** (warn-only) |
| Rich manifests (M27) | 9 of 28 | **24 of 28** (remainder: shared libs / no-param apps) |
| Checkpoints (M23) | 2 apps | **7 apps** (added: grants-gov details, cordis stage-2, mpsv-vpm ARES phase, connector-api-watch, state-licensing, provisioner discovery, extractor/plugin backfill) |
| Archive tier (M18) | 0 | 2 (readable opt-in param, provisioner sampling) — **and a structural reason for the rest, below** |
| Recipes (M05) | 0 | 2 (readable, provisioner) |

## Two structural findings (why "0 adopters" wasn't neglect)

1. **`archive_max_age` / `use_recipes` are unreachable from `ctx.engines.http.fetch`.** They only route through `ctx.fetch` (the tiered fetcher). Most apps drive the raw HTTP engine, so they *cannot* adopt M18/M05 without moving to `ctx.fetch` first. That — not app-level neglect — is the fleet-wide zero. Migrating an app is a behavior change (governor/tier-router/VCR all engage), so it belongs in its own pass. connector-api-watch was migrated as the pilot and is now metered, tier-training and VCR-recordable.
2. **`sync_many` had no provenance variant** — found independently by G2, G3 and G4. Full-snapshot syncers had to choose between stamping nothing and hand-rolling the upsert, which would bypass the degrading-source removal guard. Added centrally as `sync_many_with_provenance` (`5463c1f`) and the blocked callers wired.

## Judgement calls worth keeping (the "skipped" column is the valuable one)

- **watch refuses the archive tier and recipes**: a stale archived body would manufacture "no change" — the exact failure a change-detector must never have.
- **transact refuses both**: a dry-run must act on the live page.
- **peer's provenance was a real defect**, caught and fixed: v1 overwrote the origin's derivation with the peer feed URL. It now preserves origin `source_url` + `rules_hash`, forces local `job_id`, and **drops `artifact_sha`** — this node holds no such body, and mirroring it would make a non-re-derivable record report `replayable()`.
- **census provenance is key-redacted**: the live URL carries `CENSUS_API_KEY`; stamping it verbatim would have published the credential into every revision row. `redact_key` + a test enforcing it.
- **census-bfs skips provenance entirely**: each `formations` row fuses BA_BA + BA_HBA — two URLs, two artifacts, so any single stamp would lie.
- **plugin pins `rules_hash` only when the module self-describes a version** — a name-keyed pin would lie across a swapped `.wasm`.
- **Batch-level stamps are refused where a batch spans many sources** (mpsv `employers` has one ARES URL *per record*; crawl's hot paths key *by* URL already). Doing it per-record with today's API means one transaction per row — the write amplification the perf campaign removed.
- **grants-gov's detail checkpoint stores the delta, not a cursor**: on re-claim the listing re-syncs as `unchanged` and a cursor would collapse to empty.
- **Listing stages skipped for checkpoints** (grants-gov, eu-sedia): accumulate-then-upsert means a partial resume would publish a partial corpus.

## Commits

`7e50ab7` cz-labour · `75200a9` funding · `0be5112` crawl+platform · `b7188bb` content+research · `0b91f60` trades+census · `5463c1f` core sync_many_with_provenance · `90d6bf2` 8 contracts

## Open follow-ups (deliberately not done here)

1. **`upsert_many_stamped_each`** — per-record provenance on a multi-source batch without one txn per row. Would unlock crawl `pages`, mpsv `employers`, extractor/plugin multi-URL batches.
2. **Raw-engine → `ctx.fetch` migration** for the apps that would benefit from archive/recipes/VCR (census family, grants family, hackernews). One app at a time; each is a behavior change.
3. **cms-fee-schedule has no catalog row** — it can never get a contract or freshness monitoring until one exists.
4. **Flip `[contracts] enforce=true`** — only after real runs produce clean warn logs. That is the deliberate next step, not part of this PR.
