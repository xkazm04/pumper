---
name: crawler-core
type: perfect/context
group: Core Platform
category: lib
opportunity: 7
last_proposed: 2026-08-12
cooldown_until: r18
directions: ["[[crawl-resume-loses-nothing]]", "[[crawl-revisit-dedup-freeze]]", "[[crawl-politeness-truth]]"]
alias_of_old_map: "[[broad-crawler]] (round-2 pass covered these files)"
---

## Current state (r16 scout 2026-08-12 — engine-depth, full read of both files)
Files: `crates/core/src/crawl.rs` (2639 lines), `crates/core/src/simhash.rs` (625).
Consumers: `crates/apps/crawl/**`; simhash also feeds `crates/core/src/resilience/`.
Opportunity raised 5 → 7: the scout read both files end to end and found **three severe
permanent-data-loss classes** the r15 prefetch brief did not have.

Findings, ranked (scout evidence, file:line verified):
1. **Kill between checkpoint save and sink flush loses kept pages permanently.** Order is
   body→disk (:1021), push to `sink_buf` (:1039, flushes only at `PAGE_SINK_STRIDE`=50,
   :1059-1063), then `save_checkpoint` (:1076-1083) which serializes `frontier.seen` (:1236)
   AND `dedup_index.hashes()` (:1237) — both already containing the buffered page (:562,
   :1009). On resume `seen` is authoritative (:654-659) and `push` early-returns (:555-557),
   so those URLs are NEVER re-fetched. Bodies orphaned in artifacts with no `pages` row, and
   the restored simhashes keep suppressing near-dups of pages that no longer exist.
   The code's own safety argument (:1073-1075, "the seen-set makes a resume idempotent")
   holds only for sink-then-checkpoint, which is not the order written.
2. **In-flight URLs are consumed and never returned.** `pop` (:892) removes from queue and
   inserts into `seen`; on `max_pages` break (:1114-1116) up to `concurrency-1` unresolved
   fetches are dropped, then the final save (:1132-1136) persists them as seen-but-not-queued.
   Unreachable forever. Bites the incremental `max_pages:50` walk pattern hardest.
3. **`frontier_dropped` + `skipped_host_budget` never surfaced; no `stopped_reason`.** Core
   computes both (:1128-1129) and its doc comments (:467-473) say they exist to report
   truncation honestly; the app's 30-field result (apps/crawl/lib.rs:841-890) emits neither.
   Nothing distinguishes "frontier drained" from "hit max_pages" from "all robots-blocked".
   CROSS-CRATE (core enum + app emit) — see the deferral note below.
4. **`MAX_FRONTIER` (100k, :31) is a LIFETIME discovered-URL cap that survives resume.**
   `push` tests `seen.len()` (:558) and `seen` is restored verbatim (:655), so once a
   long-lived checkpointed job has discovered 100k URLs every later resume enqueues NOTHING,
   drains, and exits looking successful (and `dropped` is unreported — see 3). Not in
   `CrawlConfig` (:289-338), not in the app schema: no knob at all.
5. **Revisit mode's cross-page dedup permanently freezes templated pages.** The gate (:1004)
   is unconditional and the app defaults `dedup_distance: 3` in ALL modes (apps/crawl:706).
   A known page within 3 bits of another page fetched this run bumps `revisited` (:999-1001)
   then returns at :1006-1007 without touching the sink: fresh `etag`/`last_modified`
   discarded, `RevisitCadence` never advances, record frozen with stale validators — so next
   run re-downloads and is dropped again. Permanent. Cross-page dedup is semantically wrong
   in a revisit (a sentinel recrawl compares a page to ITS OWN history, not to its siblings),
   and it targets exactly the paginated/templated sites `REVISIT_SEED_LIMIT`=10k implies.
6. **Intermediate checkpoints only fire inside the kept-page branch** (:1076-1083 sits in the
   `else` of `if duplicate`). Failed (:950), BotWall (:955), NotModified (:976), Gone (:993)
   and duplicates (:1006) all `continue` first. A revisit over 10k mostly-304 pages produces
   ZERO intermediate checkpoints; killed at 95%, it loses 95%.
7. **robots.txt fetched inline in the frontier top-up loop** (:897-899) inside
   `while in_flight.len() < concurrency`, and the only poll point is `in_flight.next()`
   (:939) — so every first touch of a new host serially stalls the entire pool. Filling 16
   slots with 16 new hosts costs 16 sequential round trips before one page fetch. Sitemap
   seeding is fully serial before the crawl (:870-884, :1682-1703).
8. **Crawl-delay drain re-parses the whole frontier every 200ms** (:891 rotations, :909
   requeue, :936 sleep) — each pop costs a full `Url::parse` in `enqueue` (:569) and
   `requeue` (:630) plus `rules.allowed` parsing again (:1553). 100k frontier on delayed
   hosts ≈ 200k parses + 100k robots evaluations per 200ms, producing zero fetches.
9. **robots.txt only ever fetched over `https`** (:1480, seed scheme ignored; cache key is
   the bare host). An http-only origin's robots fetch fails at transport and FAILS OPEN
   (:1487-1491): `respect_robots: true`, `skipped_robots: 0`, and the crawl took the
   `Disallow:` paths anyway. `robots_fetch_failures` counts it but nothing connects the
   counter to "these hosts were crawled without compliance".
10. **Per-host budgets reset on every resume** — `taken` is not in `Checkpoint` (:1196-1205),
    acknowledged at :653. `max_pages_per_host: 100` reaped and re-claimed 4× fetches 500
    from one host. This is a POLITENESS control, documented only in a private comment.
11. Sitemap seed allocation is nondeterministic (`seed_hosts` is a HashSet, :787/:871, shared
    2000 budget consumed first-come) and `sitemap_seeded` counts attempts not seeds (:1714).
12. (APP-side, disjoint) robots/sitemap probes flow through `MeteringHttpClient`, so a host
    with no robots.txt yields a 404 → `gone` → a fabricated "gone page" observation in the
    Web Reliability Index on every crawl.
13. (resilience/, different context) stored `val_simhash` carries no rules identity
    (store.rs:384-389; crawl leaves `rules_hash: None`), so editing a selector gives
    text≈0/dom≈0/value-high → `SelfInflicted` at score 1.0 (detect.rs:749,728,401) on the
    next run over unchanged pages. Self-heals at N+2. **Banked on [[source-resilience]]** —
    the detector's most confident verdict, the one that blames the user, fires by
    construction on every legitimate rules edit.
14. **Test gaps that explain why 1/2/5/6 survived:** `dedup_distance` is **0 in every
    end-to-end test** (`test_cfg` :1820 and :2433 are the only settings; production default
    is 3), so the whole dedup gate through `crawl()` is unexercised; the resume test (:2247)
    runs `concurrency = 1` with **no sink**, which structurally excludes findings 1 and 2;
    nothing asserts a checkpoint fires mid-run; nothing pushes past `MAX_FRONTIER`.

## What is already good (scout's explicit no-direction verdict)
- **`simhash.rs` — leave it alone.** Banding is proven, not argued: `band_widths` implements
  `b = d+1` pigeonhole incl. remainder distribution (:188-209); `MIN_BAND_BITS` makes
  "banding doesn't pay" a property of the index rather than a second code path (:179-187);
  `neighbor_walk_matches_the_linear_scan_across_both_banding_regimes` (:558-589) sweeps
  distances 0..=25 across the regime boundary asserting the exact slot list against the
  linear-scan definition. FNV-1a+splitmix64 chosen against a real failure (SipHash
  cross-version instability defeating dedup against stored records, :401-420).
- **Host fairness works** — the starvation question is answered NO (:592-624 rotation, budget
  refund :629-635, both tested). Findings 7/8 are throughput, not fairness.
- **Checkpoint versioning** (:1179-1220) is the right shape; `artifact_name` (:1157-1162) is
  URL-addressed, which is what makes resume-overwrite impossible.
- The `CrawlFetch` Failed/BotWall/NotModified/Gone vocabulary (:1251-1266) is the repo's
  "stop lying" pattern done right — findings 3 and 6 are where that discipline didn't reach,
  not a rebuttal of it.

## Direction history
- (as broad-crawler, round 2): 5/5 shipped — see [[broad-crawler]].
- **r16 (2026-08-12, director-self-gated): 3 accepted** — [[crawl-resume-loses-nothing]] (1+2+6
  +14), [[crawl-revisit-dedup-freeze]] (5), [[crawl-politeness-truth]] (9+10).
  REJECTED-deferred, with reasons: **stopped_reason + the two counters** — the reporting half
  is the app's (findings 3), and a cross-crate direction cannot be split across two concurrent
  lots; the app-side half that needs NO core change ships this round as
  [[crawl-truncation-visible]], and `stopped_reason` follows next round in whichever lot owns
  both files. **MAX_FRONTIER lifetime cap** (4) — real and severe, but the fix is a design
  decision (lifetime-seen vs queue cap vs configurable) entangled with the checkpoint format
  that [[crawl-resume-loses-nothing]] is already changing; sequencing it after that direction
  avoids two builders reshaping `Checkpoint` in one wave. Banked as this context's anchor.
  **Throughput pair** (7+8) — genuine, but this is a local-first service whose crawls are
  bounded by politeness rather than CPU; r9's governor-hot-path precedent (no volume consumer
  ⇒ no optimization direction) applies. Banked; promote when someone runs a broad crawl big
  enough to feel it. **Sitemap nondeterminism** (11) — thin; the over-report is one line and
  rides along with any future sitemap work.

## Shipped
- (inherited — see [[broad-crawler]])
