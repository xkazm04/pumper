---
slug: crawl-write-path-is-sound
type: perfect/direction
context: "[[web-crawler]]"
lens: robustness
status: rejected
size: —
proposed: 2026-08-14
accepted: —
shipped: —
commit: —
---

## Recorded so a future round does not re-scout it — this is a CONFIRMED-SOUND finding, not a defect

r24's core/server scout surfaced, as its top non-claim finding, that **crawl `pages` records carry no
content address at all**: `crates/apps/crawl/src/lib.rs:583-587` (`job_prov()`) stamps `job_id` only —
`artifact_sha` None, `rules_hash` None, `source_url` dropped — on "the workspace's highest-volume
producer", while `page_versions` (`:669-678`) is at least half-stamped.

**REJECTED. The write path is deliberate, documented, and measured**, and the comment at `:575-582`
gives the whole argument:

- a batch spans many pages, so a batch-level `source_url` **would be a fabrication**;
- it would also be **redundant** — a `pages`/`edges` record's key *is* its URL (`{url}` /
  `{from}|{to}`), so the per-record source is already recoverable;
- per-record stamping would mean **one write transaction per crawled page** on the platform's highest-
  volume write path — the exact amplification `upsert_many`'s chunked commits exist to avoid;
- `rules_hash` stays `None` because a crawl runs no RuleSet: *"`None` = unknown, never a fabricated
  pin"* (`:675-677`).

Overturning a deliberate performance decision requires its own measurement, and **it is not needed**:
[[crawl-corpus-stays-addressable]] fixes the consequence (pinnability) from the *pin* side without
touching the writer at all, because `pages` records already carry the `artifact_path` + `job_id` pair
the pin actually needs (`:760`). Fixing the writer would have been the expensive wrong lever.

**Standing note for a future round:** if you find yourself proposing per-record provenance on the
crawl write path, read `crawl/src/lib.rs:575-587` first — the answer is there, and the correct seam is
`datasets.rs::pinned_artifact_refs`.
