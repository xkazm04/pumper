---
name: dataset-peering
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 6
last_proposed: 2026-08-10
cooldown_until: round 10
directions: ["[[peer-feed-loss-windows]]", "[[peer-mirror-visibility]]", "[[peer-two-node-proof]]"]
---

## Current state (scout brief digest, 2026-08-10 — Director-verified key claims)

The peer app (`crates/apps/peer/src/lib.rs`, 1036 lines, whole crate) is registered
(`registry.rs:39`), schema-enforced at the door, and its happy path is genuinely good:
newest-first first-revision-per-key-wins with a suspension-surviving `seen` set
(`lib.rs:563-611`), atomic name-based tombstones through `tombstone_keys` (the
RemovalGuard-respecting path), honest `mirror_provenance` (drops `artifact_sha` so
`replayable()` can't lie), and a namespace guard. 13 pure-fn unit tests.

**CONFIRMED gaps (Director verified the load-bearing ones in code):**
- **Loss windows in the walk/resume:** feed predicate is strict `created_at > ?3`
  (`core/datasets.rs:788-794`) while a whole upsert-chunk shares one stamp
  (`core/datasets.rs:699-701`) → same-stamp revisions written after page 1 are excluded
  forever. Malformed cursor → route silently serves page 1 with 200
  (`routes/datasets.rs:717-728`, `parse_cursor_i64` → None) → peer livelocks consuming
  budget (`ok/capped/new:0` forever). Schema drift: malformed items counted, walk
  completes, `since` advances past discarded revisions (`lib.rs:566-586`). Would-empty
  tombstone guard DROPS removals permanently (`lib.rs:405-410`). All-datasets-failed
  runs return `Ok` — green job history on a mirror that has failed for a week
  (`lib.rs:224-242`).
- **Mirror invisibility:** `job.app="peer"` but writes land under `peer_<x>`;
  `load_run_changes` scopes by `job.app` (`worker.rs:1025`) and the peer result
  declares no `index_datasets` → no watches, no dataset triggers, no search indexing
  on mirrored data. `lib.rs:36`'s claim that downstream triggers see removals is FALSE
  as built. The seam exists: `run_indexed_apps` (`worker.rs:1202`) already widens
  saved-search scoping to `index_datasets` virtual apps (round-6 fix 21c838d) — watches
  and triggers never got the same treatment.
- **Zero tests above pure functions**: no e2e, no two-node run ever; `run()`/`pull_one`
  untested. The round-6 conformance pin is server↔TypeScript only — it never references
  the peer, and its fixtures omit the four provenance fields (`job_id`, `source_url`,
  `artifact_sha`, `rules_hash`) that `mirror_provenance` consumes: a wire rename breaks
  mirrors silently with all tests green.
- Dead code: entire ETag/304 path unreachable (server emits no ETag anywhere);
  trust carry-through branch unreachable (feed default `stable`, never overridden).
- No feature doc; `peer` on `CATALOG_EXEMPT`; `[[peer]]` config never implemented
  (documented non-goal).

**Banked seeds (decay rule: re-verify at proposal time):**
- Tombstone-path scale: any tombstone → full-dataset `list()` materializing every data
  blob for a key set (`lib.rs:396-404`, `datasets.rs:1603-1616`); `peer/state` writes a
  full seen-array snapshot into record_revisions EVERY run (~1 MB/run on long walks).
- Mirror reconcile: hard deletes on origin (`delete_record`/`delete_dataset` erase
  revisions, no `removed` revision) leave permanent ghosts; no /export-based resync or
  staleness alarm. The wildcard anchor for the next peering round.
- Trust propagation: only `stable` crosses the wire; provisional-tail records freeze on
  the mirror invisibly. Design question — touches the round-6 anti-laundering floor;
  needs its own slate slot.

## Direction history
- 2026-08-10 (round 8, autonomous, director-self-gated): proposed 5, accepted 3 —
  [[peer-feed-loss-windows]] (robustness), [[peer-mirror-visibility]] (feature),
  [[peer-two-node-proof]] (robustness).
  **REJECTED (deferred): peer-tombstone-scale** — real perf numbers but behind the
  correctness cluster in value; banked above.
  **REJECTED (deferred): peer-mirror-reconcile** — ghost-record reconcile via /export
  is the right wildcard but larger than one session alongside the rest; banked as the
  anchor for the next peering pass.
  (ETag dead path: recorded, not slated — bandwidth polish on an on-demand app. Auth:
  PARKED decision, not proposed.)

## Shipped
- 2026-08-10 · [[peer-feed-loss-windows]] → `54bf16a` — all five loss windows closed
  (inclusive_since 1µs rewind, 400 on corrupt cursor scoped to /changes+/history,
  drift freezes the resume point, refused tombstones deferred in PeerState,
  all-errored fails the job / any-degraded = partial). +Director `06b1deb` pinning
  refusal→retry→apply end to end.
- 2026-08-10 · [[peer-mirror-visibility]] → `4ea11b7` — hook batch widened across
  `run_indexed_apps`, keyed (app, dataset); watches/triggers/search all see mirrored
  writes; saved-search re-badge hack deleted; `lib.rs` module-doc claim now true.
  Deliberate semantic widening for ALL virtual-app runs (grants-shaped case tested).
- 2026-08-10 · [[peer-two-node-proof]] → `5a7347f` (+`bb1c462` chrono lock) — first
  two-node e2e in repo history (8 proofs over a live socket, hermetic); provenance
  fields pinned in conformance fixtures both directions; docs/features/peering.md
  created + doc-map entry. Observed effect: peer went from zero-tests-above-pure-fns
  to the best-proven app in the fleet; the wire contract it depends on can no longer
  drift silently.
