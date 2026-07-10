# Vision Scan Fix Wave 9 — Domain Data Products II

> 4 commits, 4 ideas closed + 1 duplicate absorbed (T9 tail; implemented via parallel per-crate agents, orchestrator-verified + committed).
> Baseline preserved: build clean → build clean; tests 48 → 67 (+19, 0 failed).

## Commits

| # | Commit | Idea | Title |
|---|---|---|---|
| 1 | `09f25c4` | 5c873722 | Clean-text enrichment of SEDIA descriptions |
| 2 | `cd65f59` | b3097e68 | Blended employer + solo total-market view |
| 3 | `365d928` | b7285ec8 | Posted-vs-official salary gap benchmark |
| 4 | `d65fa1d` | b43376ab | ARES employer enrichment (absorbs adf4c395) |

## What was built

- **SEDIA**: `description_text` (plain text via core `html_to_markdown` + markdown strip, 2000-char cap) beside the raw HTML field; entity-escaped titles normalized.
- **Census blend** (`census/market_blend` virtual namespace): CBP employer counts × NES solo operators at the honest common grain `{naics4}:{state_fips}` — total_market, solo_share, per-side vintages, coverage flag; both apps re-derive so annual runs are order-independent.
- **CZ salary gap** (`cz-labour/salary_gap`): posted salary points pooled to CZ-ISCO 4-digit × sphere vs official ISPV medians/means; abs+pct gaps; thin/unmatched cells skipped.
- **ARES employers** (`employers` dataset in mpsv-vpm): key-free register lookups for this run's ICOs — name, legal form, founded, kraj, NACE; capped 50/run (drains across runs), skip-existing, fail-open.

## Patterns established

21. **Parallel per-crate implementation agents** — disjoint app crates can be built concurrently by agents that never touch git; the orchestrator verifies the workspace and makes per-idea commits. Sequence agents that share a crate (salary-gap → ARES both in mpsv-vpm).
22. **Honest common grain for cross-dataset joins** — join at the finest granularity BOTH sides genuinely support (naics4×state, isco4×sphere), rolling finer data up; never interpolate down.

## What remains

Deferred T9: census YoY trend layer (cfff9ead/a42f0fbf — needs multi-vintage accumulation), wage bands, skills-demand. Moonshots: answer engine/RAG, self-healing scrapers, source scout, session vault. Decisions: API-key auth, hybrid semantic. ~210 pending ideas.
