---
slug: archive-provenance-visible
type: perfect/direction
context: "[[engine-contracts]]"
lens: robustness
status: accepted
size: M
proposed: 2026-08-13
accepted: 2026-08-13
shipped: —
commit: —
---
## What & why
The `[archive]` tier exists to trade **freshness for availability**: when a host is dead,
blocked, or rate-limiting, pumper serves a Wayback snapshot instead. That trade is only safe
if the consumer can tell it happened. Today it cannot.

`crates/core/src/engine.rs:78` declares `FETCHED_VIA_HEADER` with the doc comment *"Stored
with the response's header map, so provenance survives into records."* **No code path carries
it.** `FetchOutcome` (`fetcher.rs:226-246`) has no headers field at all, and the `outcome()`
builder drops `resp.headers` on every tier. Both constants have **zero readers workspace-wide**
outside engine-archive's own test module — verified by grep, not assumed.

So a record produced through the tiered fetcher's archive tier is **byte-indistinguishable from
a live fetch**. A dataset row extracted from a 2019 snapshot looks exactly like one extracted
from the page as it is today. The extractor's `_fetched_via: "wayback"` tag only works because
that app bypasses the fetcher and drives `ArchiveEngine` directly — every other consumer is
blind.

This is the highest-value defect class this repo recognizes: **a documented contract with no
implementing code.** The doc comment is not aspirational prose in a README; it is the type's
own declaration of what it guarantees.

The user moment: *"I built a price dataset off a crawl. Half of it came from archived snapshots
because the host started blocking us, and nothing in the data, the receipt, or the trace said
so."*

## Evidence (Director-verified, not scout-assumed)
- `crates/core/src/engine.rs:78` — the claim; `:81` `SNAPSHOT_TS_HEADER`.
- `crates/core/src/fetcher.rs:226-246` — `FetchOutcome`: `url, engine, status, html, markdown,
  text, escalations, trace, cost_usd`. **No headers, no provenance.**
- `crates/core/src/fetcher.rs:1041-1062` — `outcome()` never receives or stores `resp.headers`.
- Archive tier writes the headers at `crates/engine-archive/src/lib.rs:360,362`; the archive
  branch returns through `outcome("archive", …)` at `fetcher.rs:412` — headers dropped there.
- Zero readers: grep for both constants across `crates/` returns only the definitions
  (`engine.rs:78,81`), the re-exports (`lib.rs:116,118`) and engine-archive's own tests.
- `TierTrace` (`fetcher.rs:198-223`) has `tier, verdict, http_status, content_chars, cache_hit,
  latency_ms, cost_usd, detail` — no provenance channel either.

## Acceptance criteria
1. A fetch that was served from the archive says so **in a structured field a consumer can
   branch on** — not a substring of `escalations` prose. Put it where consumers already look
   (`FetchOutcome` and/or the winning `TierTrace` entry); the exact shape is yours, but justify
   it against how consumers read results today.
2. The snapshot's **capture timestamp** rides with it. "This came from the archive" is much less
   useful than "this is what the page looked like on 2019-03-11" — freshness is the whole
   variable being traded.
3. At least one real consumer reads it, so this is not another zero-reader constant. `AppContext::fetch`
   stamping record provenance is the natural candidate; pick one and wire it end to end.
4. `crates/core/src/engine.rs:78` and `:81` either become **true** or stop claiming what they
   claim. If the header mechanism is the wrong seam (it may well be — headers are an odd channel
   for this), say so in the doc and name the real one.
5. Guard it the way this repo guards conventions: a test named for the anti-pattern
   (`an_archived_fetch_is_not_indistinguishable_from_a_live_one` or similar) that fails against
   today's behavior. Confirm it fails first, and say so.
6. `docs/features/fetching.md` describes the new field where it documents the archive tier.

## Risks / non-goals
- **Do NOT** widen this to the remote/peer tier. `RemoteEngine` has the same class of gap (no
  field says which node served a fetch) but it is a different context — banked on
  [[remote-engine]]. Naming it as a follow-up in a comment is welcome; implementing it is scope
  creep.
- No new dataset, no schema migration.
- `FetchOutcome` is read by many consumers; adding a field is fine, changing existing field
  meanings is not.
- The `Fetcher` is a chokepoint pinned by `crates/core/tests/fetch_chokepoint.rs` — if your
  change touches the raw-engine inventory, update the EXPECTED list in the same commit.

## Build record
(to fill during build)
