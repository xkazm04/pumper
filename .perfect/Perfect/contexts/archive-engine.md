---
name: archive-engine
type: perfect/context
group: Scraping Engines
category: lib
opportunity: 4
last_proposed: never
cooldown_until: —
directions: []
---

## Current state
Not yet scouted on the 46-map. Files: crates/engine-archive/src/lib.rs. Wayback/archive
fetch tier added after round 3 (post-dates the old fetch-engines pass entirely — no
inherited history). Ladder integration hardened indirectly by round 9 (browser-down-ladder).

## Direction history
- 2026-08-12 (round 11, director-self-gated): **COVERED — nothing clears the bar.** Scout
  (medium) + Director review: thin, honest adapter; 20 in-file unit tests + 7 ladder tests;
  every failure mode is a typed error the ladder treats as fall-through. Checked and rejected:
  unvalidated snapshot-body status (both consumers gate on it upstream); caller-header forward
  to archive.org (unreachable from both consumers today — latent only). One open live-check
  (not a build session): whether CDX applies `limit` after filter+collapse — if not, range
  enumeration can report a partial decade as complete (truncated:false); the one live test
  cannot catch under-reporting. Nearest real gap (truncated backfill has no resume cursor)
  belongs to declarative-extractor and is recorded there-adjacent.

## Shipped
- (none on this map)
