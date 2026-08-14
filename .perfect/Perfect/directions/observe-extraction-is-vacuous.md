---
slug: observe-extraction-is-vacuous
type: perfect/direction
context: "[[source-resilience]]"
lens: robustness
status: rejected
size: —
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## The banked claim (r22), and why it is now REFUTED

r22 banked this as the strongest remaining fleet-wide finding:

> *"No app in the smlouvy/watch/ca-grants family calls `observe_extraction`, so core's health-based
> removal guard is structurally unreachable for all of them — the two app-layer floors shipped this
> round are the ONLY thing between a partial batch and a tombstone, and a later round must not
> delete them believing the health guard covers it."*

**The grep fact is exactly right and unchanged. The defect framing is wrong.** Re-scouted 2026-08-14
across all 31 app crates.

### Every `observe_extraction` call site in the repo

| file:line | kind |
|---|---|
| `crates/core/src/app.rs:719` | definition |
| `crates/core/src/resilience/store.rs:736` | `Resilience::observe`, the inner impl |
| `crates/apps/extractor/src/lib.rs:843` | **the only call site in the entire repo** |

Every other hit is a doc comment explaining the absence.

### Why the framing fails: the guard is VACUOUS for those apps, not GAPPED

**ca-grants, grants-gov, eu-sedia and watch/connector-api-watch never call `sync_many` at all**, so
there is no removal detection for a guard to protect. ca-grants writes via
`upsert_many_with_provenance` (`ca-grants/src/lib.rs:228-237`); the grants family retires rows by
flipping `status` to `"closed"` (`grants-common/src/lib.rs:1012-1062`), never by tombstoning. A guard
that cannot fire on a path that does not exist is not a gap.

And the guard no-ops for a **second, independent** reason the claim did not state: `[resilience]
enforce` defaults **false** (`config.rs:443`, comment *"Soak first."*), and
`Resilience::enforced_state` returns `SourceState::Healthy` unconditionally when not enforcing
(`store.rs:723-726`). `Healthy` never suppresses removals. So `observe_extraction` and `enforce` are
**redundant** — either alone disables the guard.

### The apps that CAN tombstone are already floored

1. **smlouvy-dump-watch** — `PARSE_FLOOR = 1.0` (`:104`), `is_partial()` (`:146-148`),
   `removal_suppression_reason()` (`:196-208`), the downgrade branch at `:401-410`. Pinned by
   `tests/partial_parse_cannot_tombstone.rs`. (r22)
2. **trades family** — `COVERAGE_FLOOR = 0.9` (`trades-common/src/lib.rs:700`), `may_tombstone()`
   (`:840-842`), `write_snapshot()` (`:884-918`), consumed by state-tax (`:357-365`). (r21)
3. **cordis** — `rollup_is_complete()` (`:435-447`) switches `sync_many` → `upsert_many` at the cap.
4. **peer** — avoids `detect_removed` entirely; has `tombstones_would_empty_the_mirror` (`:464-495`).

**Verdict: REJECTED — the claim is superseded, not deferred.** Do not re-bank it in this form.

## What survives, and it is smaller and different

1. **`hackernews` is the last unfloored `sync_many` in the fleet** — re-banked as
   [[hackernews-snapshot-floor]]. That is the real residue and it was never in any banked claim.
2. **The resilience ladder is decorative while exactly one app produces health evidence.** Flipping
   `[resilience] enforce = true` today would gate essentially nothing, because `extractor` is the
   only `observe_extraction` caller. That is a roadmap fact worth knowing before anyone plans an
   enforcement rollout — **not a defect**, and not a direction.

**The r22 warning still stands and must be carried forward:** the smlouvy and trades floors are
load-bearing and a future round must not delete them on the theory that the health guard covers it.
`trades-common/src/lib.rs:853-878` says so in the code itself. This note is the reason why —
the health guard does not cover it, and now cannot be *assumed* to.
