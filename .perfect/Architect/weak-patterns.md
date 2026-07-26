# Weak Patterns

Anti-patterns identified by `/architect`, with reach data.

## Guarded-in-prose-only bug classes
- First seen: 2026-07-26 / Last seen: 2026-07-26
- Reach: was 6 of 15 catalog classes unguarded; now 3 remain (raw-engine metering, sync_many allowlist, per-app silent-success predicates)
- Reach trend: shrinking (Tier-1 + HMAC contract shipped 2026-07-26)
- Backlog item: [[Architect/backlog]] → "Tier-2/3 bug-class guards"
- Root cause (codified): fixes inlined in `run()` bodies never get tests; extracted pure fns always do.

## Fixture duplication / no shared harness
- First seen: 2026-07-26 — RESOLVED same day by [[Architect/decisions/2026-07-26-shared-test-harness]]
- Was: fresh_db ×11, engine-stub trios ×4, AppContext literals ×3, 41 leak-prone teardowns.
- Residual: engine-search's `unique_dir`/`doc` builders still duplicated ×2 (minor; fold in when those files are next touched).

## Bimodal app test coverage
- First seen: 2026-07-26 / Last seen: 2026-07-26
- Reach: 11 of 24 app crates at zero tests, uncorrelated with size or parse risk
- Reach trend: stable (hackernews moved off the list this run)
- Backlog item: [[Architect/backlog]] → "App-coverage tail"
