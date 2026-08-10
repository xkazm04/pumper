---
slug: provisioner-sample-stage-fix
type: perfect/direction
context: "[[source-provisioner]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 0978626
---

## What & why
The provisioner cannot complete a run: its sample stage reads `outcome.text`/`.markdown`
but never `.html` and never sets `req.to_markdown` — while every normal fetch tier puts the
body in `html` and only fills `markdown` when asked. Every candidate yields an empty body,
all are skipped, and the run hard-errors AFTER the paid discovery call. Sibling apps set
`to_markdown = true`; the provisioner copied the read without the flag. No test covers the
sample stage, which is why it shipped.

## Evidence
- `crates/apps/provisioner/src/lib.rs:554-568` (text→markdown→"", no html read, no flag) —
  Director-verified by grep 2026-08-04
- `crates/core/src/fetcher.rs:885-906` (`markdown: if req.to_markdown`, `text: None`,
  `html: Some(html)`)
- `crates/apps/readable/src/lib.rs:104`, `crates/apps/watch/src/lib.rs:105` (correct flag)

## Acceptance criteria
- Sampler sets `to_markdown = true` and consumes markdown with an html fallback (extracted,
  named body-selection fn + anti-pattern-named test).
- Proposal records the REAL winning fetch engine + trace (replaces the hardcoded
  `engine: "http"` fiction) and sample byte counts.
- A `run()`-level test with the existing stubbed Researcher (core/testing.rs) + fake fetcher
  drives an end-to-end propose and would have caught this bug.
- Failure of all candidates still errors, but only after bodies were genuinely attempted.

## Risks / non-goals
- Non-goal: archive-tier enablement, api-recipe handling of JSON bodies (note if relevant).

## Build record
- Builder P1 (opus), wave 1 → master `0978626`. `select_sample_body` extracted — builder
  REFUTED my markdown-first spec: CSS/`each` selectors cannot bind flattened markdown;
  markdown-first would have swapped "empty body → error" for "full body → every field
  misses". Implemented html→markdown→text (markdown/text = honest claude-tier fallbacks).
  `catalog_engine` maps the real winning tier; samples[] with engine/body_field/bytes/tier
  trace; artifact names match body type. run()-level e2e over TestContext + stub http host
  + ScriptedResearcher — fails on old code with the exact production error. No core changes
  needed (TestContext seam sufficed).
- Gates: worktree 1101/0; master gate green post-pick.
