# State file templates

All files live in `.claude/ship-loop/` in the target repo. **In pumper, `.claude/` is gitignored** (except `CLAUDE.md` and `settings.json`), so these stay untracked — never force-add them. Keep formats exactly — resume depends on them.

## state.md

```markdown
# Ship Loop — state

## Context refresher  <!-- keep ≤15 lines, updated after EVERY item -->
- Service: pumper — local-first scraping service (axum + SQLite queue + http/browser/claude engines). Stack profile: stack-pumper.md.
- Ship bar: <unattended scheduled / on-demand service / local demo>. Cadence: <milestone/marathon/tight>. Runtime-acceptance depth: <choice>.
- Scorecard: 1.Build 🟢 2.Func 🟡 3.Tests 🔴 4.Runtime 🔴 5.Data 🟡 6.Resil 🟡 7.API 🟡 8.Ops 🔴 9.Value 🔴 10.Plat 🔴
- Milestone <n> "<name>": item 12 ☑, item 4 ◐ (writing change-detection test), items 7,9 ☐
- NEXT ACTION: <exact next step, e.g. "add dataset_key_is_stable_not_positional test in crates/core/src/dataset.rs, then just test">

## Scorecard
| # | Dimension | Score | Evidence (cmd → result, date) | Top gaps |
|---|-----------|-------|-------------------------------|----------|
| 1 | Build, format & lint | 🟡 | `cargo clippy --workspace --all-targets` → 3 warnings (2026-07-26) | ... |
| 2 | Functional completeness | | | |
| 3 | Tests | | | |
| 4 | Runtime acceptance | | | |
| 5 | Dataset value & integrity | | | |
| 6 | Resilience & safety posture | | | |
| 7 | API & client contract | | | |
| 8 | Ops readiness | | | |
| 9 | Source & cost value | | evidence = value-case.md + cold-start journey + user sign-off | |
| 10 | Platform standards | | | |

## App value inventory  <!-- from the app/dataset audit; see dataset-and-runtime-acceptance.md A1 -->
| App | Engine tier | Dataset(s) | Key fields | Promised value | Cost shape | A2 assertions |
|-----|-------------|------------|------------|----------------|------------|---------------|

## Route inventory  <!-- from the EXPECTED list in crates/server/src/routes/mod.rs -->
| Route | Handler | Exercised by journey | State |
|-------|---------|----------------------|-------|

## Current milestone
Milestone <n> — "<name>" — items: <numbers from backlog.md>
Gate results (this milestone): fmt – / clippy – / test – / build – / runtime – / datasets – / value-freshness –

## Checkpoint history
- CP0 (boot, <date>): bar=<>, cadence=<>, focus=<>
- CP1 (<date>): milestone 1 done, picked items 4,7,9 ...
```

## backlog.md

```markdown
# Backlog  (☐ todo · ◐ in progress · ☑ done · ✕ cut by user decision)

| # | S | Dim | Size | Item |
|---|---|-----|------|------|
| 1 | ☑ | 5-Data | M | Dataset key for grants-gov uses list position — make it the opportunity id |
| 2 | ◐ | 4-Runtime | L | Journey: cold start → boot → hackernews job → dataset records |
| 3 | ☐ | 10-Plat | S | catalog row `eu-sedia` says live but the app isn't registered |

Numbering is append-only and stable — never renumber (user decisions reference these numbers).
```

## decisions.md  (append-only)

```markdown
# Decisions log

## CP0 — boot (2026-07-26 10:12)
- Ship bar: unattended scheduled operation
- Cadence: milestone

## Auto-decided (pending user review at next CP)
- 2026-07-26 13:40 — Extracted the key builder into `dataset_key()` + test rather than patching inline (CLAUDE.md rule). Reversible.

## CP1 (2026-07-26 15:02)
- Approved auto-decisions above. `eu-sedia` portal changed shape: mark BLOCKED in the catalog for now (item 23 filed for the browser-tier rewrite).
```

## journal.md  (append-only, one line per event)

```markdown
2026-07-26 10:12 BOOT scorecard built: 2🟢 3🟡 5🔴, 31 backlog items
2026-07-26 11:03 ITEM 4 ☑ dataset key stability + test (commit a1b2c3)
2026-07-26 12:30 GATE M1: fmt ✓ clippy ✓ test ✓(212) build ✓ runtime 5/6 (journey 'crash-recovery' red → item 28)
```

## value-case.md  (dimension 9 artifacts; spec in value-validation.md)

```markdown
# Value case — pumper (researched <date>; stale after 30 days)

## Consumers & wedge
<who on this machine reads these datasets, in one line>
**Wedge:** <one honest sentence why scraping this ourselves beats every alternative — or "NONE FOUND (🔴)" for a specific source>

## Source alternatives map
| Source (catalog id) | Current mechanism | Best alternative | Alternative cost | Why we still scrape | Source (URL, date) | Verdict |

## Per-app cost/value
| App | Cost shape | Records/run | Cost/run (measured) | Value of the records | Basis (URL, date) | Multiple | Verdict |
Monthly aggregate: scheduled operation ≈ <n> runs ≈ $<x>/mo model spend + <y>h wall-clock vs cheapest credible alternative $<z>/mo → <statement>.

## Production-reality checklist
- Cold start: ✓/✗/⚠ — <evidence>
- Freshness honesty: …
- Confidence honesty: …
- Trust ladder: …
- Legal & politeness fit: …
- Schema drift exposure: …
- Month-2 rationale: …

## Verdict-driven backlog items filed
- item <n>: <weak app / failed check → action>
- catalog rows updated: <ids>

## User sign-off
<CP reference + date, or PENDING>
```

## SHIP_REPORT.md  (repo root, written at Ship Gate)

```markdown
# Ship report — pumper (<date>)

## Verdict: READY TO RUN (bar: <ship bar>)

## Scorecard (all evidence from final two gates)
<final scorecard table with evidence>

## What a consumer gets
<app value inventory with the promised-value column — the honest version>

## Journeys verified
<list of passing runtime-acceptance journeys, with job ids>

## Datasets proven
<per-dataset: key, idempotence re-run result, change-detection delta>

## Known limitations (accepted at checkpoints)
<list with decision references — include the deliberate ONBOARDING §2 trades, stated as choices>

## Run steps
<build, config keys that matter, data/ layout, how to boot, how to enqueue, where to look when it fails>
```
