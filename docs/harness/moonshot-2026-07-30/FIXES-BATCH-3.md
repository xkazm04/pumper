# Moonshot Batch 3 — Core Substrate (2026-07-30)

> 5 commits, 5 M-ids / 4 backlog items, two sub-waves (3a: 3 parallel agents; 3b: 1 agent).
> Baseline preserved: tests 545/0 → **586/0**. Migrations 0025–0027 consumed, inventory green.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `b7c69cd` | M42 | Versioned crawl archive — crawl/page_versions on changed revisits, revision-suffixed artifacts, as_of/versions:all source mode, backfill over history keyed {url}@{date}. Zero core edits. |
| `897625e` | M05 | API X-ray — CDP capture of same-site JSON (capped), leaf-value-overlap discovery → api_recipes, GET /recipes, ctx.xray(); fetch-tier api-branch = documented seam |
| `b34472c` | M11 | Derived datasets v1 — filter/project/lookup specs, fail-open recompute on upsert_many, depth cap 3 + create-time cycle rejection, CRUD + backfill routes |
| `0cecd40` | — | 3a integration (shared storage/config/route surfaces) |
| `b0d1186` | M07+M02 | Change-cadence learning — EWMA due_score frontier (simhash-graded change, host priors, honest skipped_not_due) + cache revalidations log + idle-slot background refresher (Governor::try_acquire), GET /cache/freshness, default-OFF |

## Verification

`cargo test --workspace` 586/0. All new toggles default-OFF (`[refresher]`); recipes/derived/page_versions are inert until used.

## Deferred / open seams

- M05: fetch-tier api-branch (recipe → archive → live HTTP ordering documented in recipes.rs).
- M11: group-by aggregates (v2); target app fixed to source app.
- M42: retention uses the existing prune API — no archive-specific cap; M10 (replay-CI) and M16 (drift observatory) from the deferred list are now cheap consumers of page_versions.
- M07+M02 caveat: 304 cadence merges create `pages` revisions — triggers on `pages` see bookkeeping writes (documented; revisit if trigger noise matters).
