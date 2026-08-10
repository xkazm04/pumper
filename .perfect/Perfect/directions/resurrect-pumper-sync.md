---
slug: resurrect-pumper-sync
type: perfect/direction
context: "[[dataset-api]]"
lens: wildcard
status: shipped
size: M
proposed: 2026-08-04
accepted: 2026-08-04
shipped: 2026-08-04
commit: 78ff895
---

## What & why
`clients/typescript` (`@pumper/sync`) was deleted in `27dba84` ("vibeman(moonshot): batch-7
integration + lockfile") — apparently accidentally: `docs/features/sdk-typescript.md`, the
features README, and CLAUDE.md all still advertise it, and the user's scaling plan marks it
Tier 1 (the shared canonical-dataset mirror for 10–20 products). Restore it from git
history, make it build/test again, and add what it never had: a conformance test pinning the
sync contract it consumes (`GET .../changes?since=` + `GET .../export?format=ndjson` with
watermark semantics) — exactly the endpoints whose trust/cursor/tombstone semantics
[[read-path-population-honesty]] is fixing. Build AFTER that direction merges so the pin
captures the corrected contract.

## Evidence
- `git ls-tree -r HEAD | grep clients/` empty; deletion commit `27dba84` (files:
  src/{client,http,index,sync,types,watermark}.ts, package.json, README.md, tsconfig.json)
- `docs/features/sdk-typescript.md`, `docs/features/README.md:22`, `CLAUDE.md:67` still
  claim it ships
- MEMORY.md: @pumper/sync is Tier 1 of the scaling plan

## Acceptance criteria
- SDK restored from history into `clients/typescript`, builds (`npm install && build`) and
  its own tests pass.
- Conformance test pinning the sync contract: cursor format, `since` watermark behavior,
  trust semantics, and tombstone representation — against fixtures or a live scratch server
  (the smoke harness, if merged, may provide it).
- Any drift between the restored SDK and the CURRENT API (post population-honesty) fixed in
  the SDK, with the differences listed in the report.
- Docs truthful again (sdk-typescript.md matches restored reality; note the restoration).

## Risks / non-goals
- The API moved since deletion — the conformance test is the point; expect and report drift.
- Non-goal: new SDK features (auth, retries beyond what existed); restoration + pin only.

## Build record
- Builder D3 (sonnet), wave 2 → master `78ff895` (gate in flight at write time). SDK restored
  from `27dba84~1` into clients/typescript; builds clean; 6/6 node:test conformance tests.
  Drift found + fixed: (1) export gained teeth on trust=/removed= with removed defaulting to
  EXCLUDE — old SDK would silently lose tombstones on cold-start snapshot; now sends
  `trust=all&removed=include` (verified against sync.ts's snapshot() which structurally
  needs removed_at); (2) TS types never declared `trust` — added; (3) changesPage
  trust=stable made explicit and pinned. Conformance = fixture-based, BOTH sides: shared
  JSON fixtures asserted by TS parsers + a Rust `sdk_fixture_conformance_tests` module in
  routes/datasets.rs (subset assertion — server dropping a field the SDK's types declare
  fails the Rust half). Honest limit: no live-HTTP wire test (no seed path for datasets
  without a real scrape; builder judged a flaky live harness worse than the fixture pin).
- Builder refuted: "trust honored on every path is new" was overstated (list already had it
  pre-deletion; new teeth are export trust= + removed= everywhere).
- Director note: builder initially idled on a backgrounded test run (FOREGROUND ONLY
  violation) — one nudge recovered it.
- Gates: cargo test --workspace exit 0 in worktree (197 server tests incl. 3 new).
