---
slug: peer-two-node-proof
type: perfect/direction
context: "[[dataset-peering]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-10
accepted: 2026-08-10
shipped: 2026-08-10
commit: 5a7347f
---

## What & why
The peer app has never once run above pure functions: no e2e, no test constructs its
AppContext, and "has never run against a real second server: everything" (scout). The
round-6 conformance pin never references the peer, and its fixtures omit exactly the
four provenance fields (`job_id`, `source_url`, `artifact_sha`, `rules_hash`) that
`mirror_provenance` consumes — a wire rename breaks every mirror silently with all
tests green. This direction builds the two-node proof: a real origin server, a real
mirror instance, a real pull over HTTP — the worker-lifecycle-harness idiom (round 4,
ddebd66) applied to peering. User moment: "peering is proven, not vibes — and the
contract it depends on cannot drift silently again."

## Evidence
- Exhaustive grep: `peer` appears in `crates/server/src/e2e/` zero times; no test
  anywhere constructs an `AppContext` for the app; `run()`/`pull_one()` (paging,
  suspension, resume, tombstone invocation) all unexercised
  (`crates/apps/peer/src/lib.rs:770-1035` is pure-fn only).
- `crates/server/src/routes/datasets.rs:907-1017` + 
  `clients/typescript/test/conformance.test.ts` — the "two-sided" pin is
  server↔TypeScript; `clients/typescript/test/fixtures/revision-page.json` carries no
  provenance fields, and `assert_covers` is one-way (fixture ⊆ actual), so the fields
  the peer needs are unpinned in both directions.
- DESIGN-BATCH-9.md:24 demanded "Tests: cursor advance, namespace mapping, cap,
  resume" — only namespace mapping shipped.
- No `docs/features/*peer*` exists; total public documentation is one sentence in
  `docs/features/datasets.md:10`.

## Acceptance criteria
- [ ] An e2e (`crates/server/src/e2e/peer_mirror.rs` or similar) boots a real origin
      server (existing e2e harness idiom), seeds records via the API, and runs the peer
      app on a second AppState/instance pulling the origin's real HTTP feed. Proves:
      initial pull lands under the namespace with mirrored provenance
      (`job_id` = local, `source_url`/`rules_hash` = origin's, `artifact_sha` absent);
      incremental second run picks up only new changes; budget cap suspends and a
      subsequent run resumes the walk to completion; an origin tombstone propagates to
      a mirror tombstone.
- [ ] The loss-window guards from [[peer-feed-loss-windows]] are exercised at this
      level where reachable (equal-stamp boundary pickup; malformed cursor → run
      error), and mirror visibility from [[peer-mirror-visibility]] gets one two-node
      assertion (a watch on the mirror namespace fires on pull).
- [ ] The conformance fixtures gain the provenance fields on both sides:
      `revision-page.json` items carry `job_id`/`source_url`/`rules_hash` (and one item
      with `artifact_sha`), the Rust `assert_covers` half now pins them, and the TS
      types/tests parse them. A comment names the consumer (the peer's
      `mirror_provenance`) so nobody prunes the fields as unused.
- [ ] `docs/features/` gains a peering doc (what it does, params, state model, what
      propagates downstream, known gaps incl. hard-delete ghosts + no auth) and
      `scripts/docs/feature-doc-map.json` maps `crates/apps/peer/**` to it.
- [ ] Honest-limitation section in the report: what a REAL two-process, two-machine
      deployment would still not have proven (clock skew, network partitions, auth).

## Risks / non-goals
- Non-goal: fixing the banked seeds (tombstone-scale, reconcile, trust propagation) —
  the harness may OBSERVE them; document, don't fix.
- Risk: two AppStates in one process may contend on ports/temp dirs — the e2e harness
  already allocates ephemeral ports; keep DBs in separate temp dirs.
- Risk: the peer uses the tiered fetcher for HTTP — ensure the e2e pins the HTTP tier
  (no browser/Claude fallback) so the test is hermetic.
- Sequencing: LAST in the lot — it proves the other two directions' work.

## Build record
e2e/peer_mirror.rs: real origin server on ephemeral loopback + real mirror (own AppState/SQLite/tempdir) pulling over live HttpEngine; browser/Claude pinned Dead (hermetic). 8 proofs after Director commit 06b1deb: provenance mirroring, incremental, cap/suspend/resume, tombstone propagation, equal-stamp boundary, corrupt-cursor run failure, watch-on-namespace fires once, refused-tombstones retried until applied. Regression proofs verified by reverting each fix (builder). Conformance: revision-page.json + Rust pin + TS types carry the 4 provenance fields both ways, consumer named. docs/features/peering.md created + feature-doc-map entry. Honest limits recorded in the doc (one process, shared clock, no partitions/auth). +bb1c462 (chrono lock). Review: keep. TS suite 7/7.
