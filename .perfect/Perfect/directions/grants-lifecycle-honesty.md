---
slug: grants-lifecycle-honesty
type: perfect/direction
context: "[[US Grant Opportunities]]"
lens: robustness
status: shipped
size: M
proposed: 2026-07-13
accepted: 2026-07-13
shipped: 2026-07-13
commit: 9d18132
---

## What & why
Both apps upsert-only, so removed_at/detect_removed never fire — delisted/expired opportunities persist as `open` forever in grants/unified. Schema drift is silent (unwrap_or_default → fetched:0 reported as success). Add a close-date sweep (posted past close_date → closed), a drift guard, and unify the duplicated date parsers.

## Evidence
- upsert_many only: grants-gov:161, ca-grants:135, grants-common:72; detect_removed unused (app.rs:278-295)
- Silent drift: grants-gov:128-132, ca-grants:112-116 (unwrap_or_default)
- Duplicate date logic: grants-common:138-148 vs grants-gov:240-244

## Acceptance criteria
- [ ] Post-sync sweep in grants-common: unified rows with status open|forecasted and close_date < today → status closed (revision recorded via normal upsert path; both apps call it).
- [ ] Drift guard: fetched==0 (or normalized-null rate > threshold) with hitCount>0 → job result carries an explicit `warnings` field; fetched==0 with no API error → job FAILS with a drift message (choose thresholds, justify).
- [ ] Single shared date parser in grants-common used by both call sites; unit tests over observed formats.
- [ ] docs/features/apps.md updated (lifecycle semantics: closed-by-sweep vs removed_at reserved for true snapshot sources).

## Risks / non-goals
- Non-goal: removed_at semantics for these partial-view sources (needs the bulk snapshot source — rejected this round).

## Build record
(pending)
