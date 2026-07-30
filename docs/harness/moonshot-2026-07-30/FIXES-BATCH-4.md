# Moonshot Batch 4 — Open Seams (2026-07-30)

> 6 commits on `vibeman/moonshot-seams-2026-07-30` (off merged master `f9a7bc9` / PR #16). One wave of 5 parallel agents.
> Baseline preserved: tests 586/0 → **626/0** (+40). Migration 0028 consumed, inventory green.

## Commits

| Commit | Seam closed | Summary |
|---|---|---|
| `81be008` | M23→research | research app checkpoints per budget-consuming step; re-claim resumes session, budget never double-spent |
| `1b05b6d` | M29 SSE + tools | GET /mcp SSE (Last-Event-ID replay, gap resets, app/kind filters, never blocks the bus) + fetch_readable / deep_research (gated enqueue) + wait_job (read-only, clamped timeout) |
| `8cf2da0` | M05 fetch branch | FetchTier::ApiRecipe ahead of archive/live, opt-in ([recipes] default-OFF), success/strike/un-validate state machine (0028) |
| `1883d44` | M18 backfill | list_snapshots CDX range (digest-dedup, honest truncation) + extractor source.archive → records keyed {key}@{date}, composes with M42 |
| `87139cd` | M11 v2 | group-by count/sum, affected-group recompute from source truth, stale:true over wrong numbers, no migration |
| `cffe92e` | — | integration (config/state/storage) + DESIGN-BATCH-4.md |

## Verification

`cargo test --workspace` 626/0; `cargo check` clean. New toggles default-OFF: `[recipes] enabled/auto_validate`. Agent P was blocked mid-wave by agent R's in-flight core edits — its edits were verified by the final workspace gate (planned behavior, not an incident).

## Remaining seams (all intentionally gated)

- M04 enforcement mode — needs accumulated live yield history.
- M29 → full MCP subscriptions handshake (current SSE is notification-only, which the spec permits).
- Deferred moonshot backlog: 25 rows in the vault (strongest: M33 NOFO corpus, M13 queries-as-datasets, M44 NL source compiler; M10 replay-CI / M16 drift observatory are now cheap on page_versions).

## Note on GitHub secret-scanning alert

`evals/tier3-extraction/fixtures/gitlab.html` (pre-existing fixture, commit `0004ba7`) contains GitLab.com's own public client-side Google API key (FS_IDENTIFIER in their page source) — real but GitLab's, public by design, nothing of ours to rotate. Dismiss as test-fixture; optionally neutralize the string to silence scanners.
