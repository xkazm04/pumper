# Moonshot Batch 1 — Domain Data Products + Archive Tier (2026-07-30)

> 7 commits, 6 M-ids / 5 backlog items shipped by 5 parallel Fable agents (no-git; orchestrator gated + committed).
> Baseline preserved: cargo check clean → clean; tests 422/0 → **488/0** (+66). Drift-gate green.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `966ab17` | M34 | Amendment radar — grants/events typed lifecycle events (6-type pure classifier, sweep flip-flop guard, 8 tests) |
| `98dbf1b` | M37 | Vacancy survival ledger — mpsv-vpm per-posting lifecycle diffing → cz-labour/vacancy_lifecycle (time-to-CLOSE labeling, gap tolerance, repost linking, 10 tests) |
| `5b0a38c` | M39+M40 | census-nesd (owner-age succession) + census-bfs (weekly formation velocity) + blend joins (16 tests) |
| `6a62a4e` | M31 | cordis app + topic_stats + eu-sedia history join (topic-lineage grammar, 11 tests) |
| `5e12d1d` | M18 | engine-archive Wayback pre-HTTP tier, default-OFF, governor-covered (21 tests) + HttpRequest initializer fixups |
| `9805540` | — | Integration: registrations + catalog rows (cordis, nesd, bfs) |
| + lockfile | — | Cargo.lock |

## Verification

- `cargo check --workspace` clean; `cargo test --workspace` 488 passed / 0 failed (baseline 422/0).
- Catalog drift-gate passes both directions with 3 new rows.

## Live-run follow-ups (not code)

1. **cordis**: API envelope was assumed from public docs (pinned in crate doc-header) — first live run must be watched; drift raises a loud error by design.
2. **census-nesd/bfs**: endpoints verified against data.json/variables.json but row shape re-verifies on first keyed run (CENSUS_API_KEY required — apps report `ready:false` without it).
3. **engine-archive**: `[archive] enabled=false` — flip on + set `archive_max_age` on a request to exercise; 1 `#[ignore]` live Wayback smoke test available.
4. **vacancy ledger**: needs ~60 days of daily runs before fill-time surfaces are meaningful — the moat clock starts at first deploy.

## Patterns observed

- **Additive-struct-field fallout**: a new field on a widely-constructed struct (HttpRequest.archive_max_age) broke sibling crates' literal initializers mid-batch; agents fixed in-scope ones, orchestrator swept the rest. Prefer `..Default::default()` in future initializers.
- **Design-doc endpoint guesses must be re-verified**: both census endpoints in the finding were wrong (absnesdo, timeseries/eits/bfs); the agent caught it via data.json — "verify the contract at the doc-header level" is a load-bearing instruction.

## Deferred within batch

- M18: Common Crawl index, backfill job type (worker seam doc-commented — pairs with Batch-3 M42).
- M23-adjacent: app_research checkpoint port (Batch 2 notes).
- Optional: freshness trigger example on grants/events.
