---
name: page-monitor
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 4
last_proposed: 2026-08-14
cooldown_until: r24 (mined r22)
directions: ["[[watch-empty-extraction-is-not-a-change]]"]
---

## Current state
Not yet scouted on the 46-map. Files: crates/apps/watch/src/lib.rs, crates/apps/readable/
src/lib.rs. Watch app shipped w1 (old map) + live bodies via no_cache (d6236d4 r1);
readable-content extraction w? — both unswept since the re-registration. Change-detection
fidelity + readability quality are the likely seams.

## Direction history
- (old map: watch app w1; fetch no_cache r1.)

## Shipped
- **2026-08-14 (r22) [[watch-empty-extraction-is-not-a-change]] `37d07f3`** — `watch` refuses an
  empty/whitespace-only extraction instead of fingerprinting it: no `pages` record, no revision,
  **no alert**. `extracted_nothing` lifted from `readable` to `pumper_core::extract` (apps may not
  depend on apps) and `is_blank` delegates its string arm to it, pinned by a test.
  **Observed effect:** the two false webhook alarms per incident are gone from the one app whose
  entire product is that its alarms mean something. Verified destructive: with the guard disabled
  3 of 4 new tests fail, incl. the acceptance one (the ABSENT revision, not the returned Err).
  `hex_sha256` NIST-vector test passes unchanged.
- (inherited, pre-46-map)
