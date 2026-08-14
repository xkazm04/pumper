---
name: source-resilience
type: perfect/context
group: Core Platform
category: lib
opportunity: 8
last_proposed: 2026-08-14
cooldown_until: —
directions: ["[[observe-extraction-is-vacuous]]", "[[adaptive-cohort-floor]]", "[[quarantine-recovery-ladder]]", "[[enforcement-preview]]", "[[single-parse-fingerprints]]", "[[detector-false-positive-fixes]]"]
---

## Current state (scout brief, 2026-08-04 — engine level, file:line verified)

**Files (5, map correct):** `crates/core/src/resilience/{mod,detect,sketch,invariants,store}.rs`.
Migration `0020_resilient_extraction.sql`. Config `config.rs:328-389`. **Map's table names are stale**:
real tables are `sources`, `source_runs`, `field_sketches`, `field_invariants`, `doc_fingerprints` —
there is no `source_health` or `source_sketches`.

**The whole subsystem is 9 days old** (`cd78c2d`, `62dc6b1`, `f28033d`, 2026-07-25/26). It has never
run against production data beyond soak mode: `[resilience] enforce` defaults **false**
(`config.rs:370`), and `enforced_state` (`store.rs:673-678`) returns `Healthy` unconditionally when
not enforcing — one clean choke point that makes soak a true no-op.

**Detection.** `DocSignals` = three SimHashes per doc from one parse: visible text (capped 200k),
DOM structure, extracted values (`mod.rs:258-283`) — the text-blind/structure-blind asymmetry the
detector runs on. `FieldSketch` (`sketch.rs:42-63`) is a fixed-size per-field-per-run summary
(len histogram, char-class profile, distinct ratio, minhash, miss/coercion counters). Baseline =
sketches of the last `window_runs` (20) runs whose verdict was `Ok` (`store.rs:211-264`);
`MIN_BASELINE_RUNS = 3` below which every distributional term is disabled (`sketch.rs:383`).
Rules in order (`detect.rs:212-338`): fetch gate (`fetch_ok_floor` 0.7) → total collapse (needs only
5 docs) → quiet-listing carve-out → cohort floor (`min_cohort_docs` 30) → five weighted terms
(missrate .30, distinctness .20, invariants .20, shape .15, divergence .15) → conclusive-rebind
override forcing 1.0. Trip at `degrade_score` 0.6, severe at `quarantine_score` 0.85. **The five
weights and the collapse/rebind constants are Rust `const`s, not config** (`detect.rs:31-60`).

**Ladder** (`detect.rs:680-733`): Healthy → Suspect → Degraded → Quarantined, stepping back one rung
per clean run. **`Quarantined` is terminal** — `next_state` returns it unconditionally; the only exit
is `POST /sources/{id}/state`. **`Probation` exists and behaves, but nothing ever promotes into it**
(`mod.rs:83-85` admits automated repair "is not built").

**Enforcement consequences**, each traced: `@q` quarantine dataset via `write_target`
(`app.rs:642-648`), trust stamping provisional/quarantined (`mod.rs:120-126`), removal suppression by
withholding `RemovalGuard` (`app.rs:613-626` — the only app-facing mint), no-index
(`worker.rs:1450-1459`), no-push (`suppress_unhealthy`, `worker.rs:970-992`).

**Holes found (all now proposed):**
- `min_cohort_docs` is fleet-wide; a chronically-thin source is never judged yet its runs still
  count `Ok` and pad its own baseline (`mod.rs:195-197` gates on verdict, not coverage).
- `robust_z`'s never-varying-baseline fallback returns ±∞ past tolerance (`sketch.rs:371-378`).
- Conclusive-rebind's only escape hatch is a `ContentChanged` divergence, which cannot be produced
  with no prior fingerprint (`detect.rs:379-384`, `624`) — worst exposure on brand-new sources.
- `Diagnosis::Ambiguous` (`detect.rs:641`) is reachable with **zero** test coverage.
- `observe` re-parses every body for fingerprinting (`extractor/src/lib.rs:387`, `mod.rs:273`) after
  extraction already parsed it.

**Cost:** genuinely bounded — capped sampling (`MINE_SAMPLE` 2000), chunked fingerprint writes (500),
correct indexes (`0020...sql:49,52-53`), retention validated so it can never prune the baseline.
No optimization headroom found beyond the double parse.

**Operator surface:** `GET /sources`, `/sources/{id}`, `/sources/{id}/runs` (every run carries
self-explaining `reasons`), `POST /sources/{id}/state` — the ONLY lever. No acknowledge-without-
forcing affordance, and no "I changed the ruleset, re-baseline me" path. (Banked seed.)

**Dead ends:** `Resilience::state` (raw, un-gated) has no external caller; `Probation` unreachable
automatically; `InvariantKind::Distinctness::holds()` always `None` by design (cohort-level only).

## Direction history
- 2026-08-04 (round 5): 5 proposed, **5/5 accepted**, zero rejections.

## Shipped
(pending build)
- 2026-08-14 (r23): [[observe-extraction-is-vacuous]] — **REFUTED, not deferred.** r22 banked
  "the health-based removal guard is structurally unreachable for the smlouvy/watch/ca-grants
  family". The grep fact is right (`extractor/src/lib.rs:843` is the ONLY `observe_extraction`
  call site in the repo) but the defect framing is wrong twice over: ca-grants, grants-gov,
  eu-sedia and both watch apps never call `sync_many` at all — the grants family retires rows
  by flipping `status` to "closed" — so the guard is **vacuous** for them, not gapped. And it
  no-ops for a second, independent reason: `enforce` defaults false, so `enforced_state`
  returns `Healthy` unconditionally, and `Healthy` never suppresses removals. Do not re-bank
  in this form. **Standing warning preserved:** the smlouvy and trades floors are load-bearing
  and a future round must not delete them believing the health guard covers it — this note is
  the evidence that it does not.
- Honest residue, banked separately: [[hackernews-snapshot-floor]] — the last unfloored
  `sync_many` in the fleet, in no banked claim before r23.
