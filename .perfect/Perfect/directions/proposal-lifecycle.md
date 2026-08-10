---
slug: proposal-lifecycle
type: perfect/direction
context: "[[source-provisioner]]"
lens: feature
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 522219c
---

## What & why
Proposals rot: nothing reads `provisioner/proposals` — no list surface, no validation, no
promote path, no expiry. The manifest's "a human applies it via the catalog reconciler"
describes a path that does not exist. Ship the deterministic lifecycle so the app's output
has a consumer: list proposals with honesty fields, validate (re-run the dry-run), emit a
catalog-vocabulary-correct row, record status transitions.

## Evidence
- Repo-wide: only registry.rs + a catalog-exemption entry mention the app; no route/tool/
  doctor/e2e reads proposals
- `lib.rs:392` (manifest claims a reconciler path), ONBOARDING.md:195-315 (the real Path B)
- `catalog.rs:320` reconcile_plan operates on data-sources.toml only

## Acceptance criteria
- `GET /provisioner/proposals` (or equiv under an existing route family): list with status,
  accepted, confidence, engine, age.
- Validate endpoint: re-runs the compiled RuleSet dry-run against a fresh sample of the
  proposal's URL; updates status planned → validated | failed with the report.
- Promote emits a catalog-row (TOML fragment) that passes vocabulary validation —
  builds on [[provisioner-record-honesty]] (sequenced after it).
- Expiry: proposals past a configurable age markable/marked expired (piggyback an existing
  maintenance tick or make it lazy-on-list; builder chooses, documents).
- Status transitions recorded on the proposal record (revisions carry them); e2e test over
  the lifecycle; OpenAPI/inventory tests green; docs describe the REAL promotion contract
  end-to-end into ONBOARDING Path B.

## Risks / non-goals
- The app's "never writes the catalog" invariant STAYS — promote emits a row for human
  application, it does not write data-sources.toml.
- Non-goal: auto-generating app crates.

## Build record
- Builder P2 (sonnet), wave 2 → master `522219c` (gate in flight at write). Routes:
  GET /provisioner/proposals (paginated, lazy expiry via config
  `provisioner.proposal_max_age_secs`, default 30d), POST .../{key}/validate (fresh fetch
  via state.engines, pure `validate_sample` in the app crate — extract_preview pattern,
  dependency rule respected), POST .../{key}/promote (renders catalog_toml, never writes
  the catalog — invariant test untouched + asserted in e2e). `may_promote` gate blocks
  failed and never-validated-rejected proposals; re-promote idempotent. Discovered + fixed:
  the record had NO status field at all (brief presupposed it); added default "planned".
  Full lifecycle e2e over real router + stubbed engines.
- Gates: worktree 1141/0, run twice.
