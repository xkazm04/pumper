---
slug: provisioner-record-honesty
type: perfect/direction
context: "[[source-provisioner]]"
lens: robustness
status: shipped
size: S
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 99fe5cc
---

## What & why
The emitted proposal record undermines its consumers: `accepted: false` proposals are
emitted shaped identically to good ones and the pasted `catalog_row` drops the accepted
flag; confidence is written 0-100 into a field the catalog documents as 1-5 (silent scale
corruption); `access: "public"` is not in the documented vocabulary; `row.dataset` names a
dataset nothing writes. And the app is entirely absent from docs/features/ — the feature
officially doesn't exist.

## Evidence
- `lib.rs:218-225, 263, 681-682` (0-100 confidence) vs docs/features/catalog.md:23 +
  ONBOARDING.md:498 (1-5 scale); catalog.rs:57-58 (bare u8, no validation)
- `lib.rs:266-281` (empty/invalid category/access/app; accepted flag absent from row;
  dataset names nonexistent dataset)
- docs/features/apps.md:9-29 — no provisioner row anywhere in docs/features/

## Acceptance criteria
- catalog_row carries the dry-run verdict (accepted + confidence on the DOCUMENTED 1-5
  scale, extracted named mapping fn + test) and valid vocabulary values (access, engine
  from the real fetch tier once [[provisioner-sample-stage-fix]] lands).
- Rejected (`accepted: false`) proposals visibly distinct in record AND row notes.
- `row.dataset` names something honest (the proposal's intended dataset, marked as such).
- docs/features/apps.md gains the provisioner row; a feature-doc-map entry guards the crate;
  known-gaps section states what the proposal does NOT automate (Path B steps).

## Risks / non-goals
- Non-goal: changing the proposal dataset schema beyond additive fields.

## Build record
- Builder P1 (opus), wave 1 → master `99fe5cc`. `confidence_1_to_5` (floor 1 — "bound
  nothing" is LEAST trustworthy, not unrated), `catalog_access()` = scrape (reasoned:
  the app only ever establishes scraping, never an API mechanism), `proposed:<key>`
  dataset marker, row_notes with ACCEPTED/REJECTED + UNPROVISIONED. apps.md row + Path B
  known-gaps. Builder refuted: doc-map already guards crates/apps/** (no-op entry avoided).
- Honest: `proposed:<key>` marker has no consumer yet; catalog doesn't validate
  vocabularies at parse time (conventions enforced by this app's tests only).
- Gates: worktree 1101/0; master gate green post-pick.
