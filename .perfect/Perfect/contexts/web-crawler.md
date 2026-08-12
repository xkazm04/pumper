---
name: web-crawler
type: perfect/context
group: Content & Research Apps
category: lib
opportunity: 7
last_proposed: 2026-08-12
cooldown_until: r18
directions: ["[[crawl-truncation-visible]]", "[[crawl-reliability-survives-interruption]]", "[[crawl-memory-bounded]]"]
alias_of_old_map: "[[broad-crawler]] (round-2 pass covered the app side)"
---

## Current state (r16 — r15's banked brief RE-VERIFIED claim by claim, 2026-08-12)
App = `crates/apps/crawl/src/{lib,link_graph,reliability}.rs` over core `crawl.rs`/`simhash.rs`.
Fetch is raw `ctx.engines.http` pinned in the chokepoint inventory, wrapped by
`MeteringHttpClient`; the governor applies inside engine-http, so the crawler IS governed.

**Re-verification result: 6/6 banked claims CONFIRMED, four of them SHARPER.** The decay rule
paid again — not by refuting anything this time, but because every claim came back bigger:
1. **CONFIRMED.** `frontier_dropped` + `skipped_host_budget` computed in core (`crawl.rs:467-473`,
   `:1128-1129`) and absent from all 30 result keys (`apps/crawl/lib.rs:841-890`);
   `crawling.md:60` lists `skipped_host_budget` as returned. `frontier_dropped` has **no reader
   anywhere in the workspace**. APP-side fix, zero core change. → [[crawl-truncation-visible]]
2. **CONFIRMED, bigger.** The checkpoint/sink window also leaves an **orphan artifact file** no
   record points at (body written at `crawl.rs:1019-1024`, before the buffer push), and it
   swallows **gone + 304 markers** after their counters were already incremented — so a run can
   report `gone: 40` with zero `gone:true` rows. CORE-side. → [[crawl-resume-loses-nothing]]
3. **CONFIRMED, bigger.** The same early-return that loses reliability tallies also skips
   **`ctx.meter`** (cost ledger) and **`ctx.learn_tier`** (tier-router learning) —
   `lib.rs:795` `?` then the drain loop at `:804-823`. The shutdown-drain path
   (`worker.rs:194-203`) is exactly this. Resume state carries no tallies
   (`crawl.rs:1198-1205`). → [[crawl-reliability-survives-interruption]]
4. **CONFIRMED, much bigger — and NOT this context's to fix.** Crawl deliberately sets
   `rules_hash: None` ("never a fabricated pin") and an existing test *locks* it
   (`lib.rs:1103-1105`). The real finding: `KeepReason::Pinned` requires
   `artifact_sha AND rules_hash` (`datasets.rs:192-194`, SQL `:1951-1977`) and **no production
   write path in the workspace stamps both** — artifact_sha-only: watch, census-common,
   cms-fee-schedule, connector-api-watch, crawl; rules_hash-only: extractor, trades-common;
   both together **only in tests**. So the retention pin is dead code in production while
   `crawling.md:56` promises bodies are "reclaimed unless a replayable revision pins them".
   Banked on [[dataset-storage]] — see the rejection record below.
5. **CONFIRMED, sharpened.** Not "the app is untested" — `lib.rs:1032-1113` has real
   `tokio::test`s driving `DatasetPageSink::emit`. What is untested is exactly the result
   builder, param plumbing, the metering/reliability flush and the revisit wiring: i.e. every
   line where claims 1 and 3 live. No `tests/` dir, no e2e, only `registry.rs:35` constructs
   the app. Folded into the ACs of D1 and D2 rather than slated alone.
6. **CONFIRMED, invariant located elsewhere.** `link_graph.rs` never claims bounded memory; the
   broken promise is `crawling.md:18` and `crawl.rs:10-15`. Also violated by `SeedData`
   (`lib.rs:148`, `:536`, up to 10k full records). → [[crawl-memory-bounded]]

**Dataset reachability, settled (both scouts agree, contra the r15 brief's framing):** crawl
declares **no `index_datasets`**, but `pages`/`page_versions`/`edges` land under
`ctx.app == job.app == "crawl"`, and `run_indexed_apps` always includes the job's own app
(`worker.rs:1520-1528`) — so watches and dataset triggers on those three **do** fire; they are
only absent from full-text search, which `crawling.md:27,66` discloses correctly. The genuinely
unreachable one is **`web-reliability/*`**, a virtual namespace nothing declares, whose own
module docstring (`reliability.rs:5-7`) promises records are reachable through "triggers".
`edges` and `web-reliability/*` have **zero readers workspace-wide**; `page_versions` has real
ones (extractor + plugin).

**`docs/features/crawling.md` is wrong in six places** (all in D1's evidence): the checkpoint
mechanism (`:16` says every-25-pages into a named artifact file; it is a 5s wall clock into the
DB checkpoints table, and the `checkpoint` param it documents no longer exists — `lib.rs:711-716`
says so), the param list (`:7`, missing `revisit_budget`/`min_due_score`), result stats (`:60`),
bounded memory (`:18`), retention pinning (`:56`), and "up to 256 concurrent fetch tasks" (`:3`
— the app's own schema caps `concurrency` at 64 and the MCP path enforces it).

## Direction history
- (as broad-crawler, round 2): 5/5 shipped — see [[broad-crawler]].
- **r16 (2026-08-12, director-self-gated): 3 accepted** — [[crawl-truncation-visible]],
  [[crawl-reliability-survives-interruption]], [[crawl-memory-bounded]].
  **REJECTED-deferred, with reasons:**
  - **Retention pin is dead code in production** (claim 4 + the workspace-wide sweep). The
    single highest-severity finding of either scout, and deliberately NOT slated here: the
    honest fix redefines what `Pinned` means (the seam conflates two different questions —
    "can this revision be re-derived" vs "does a live revision still NAME this file"), it lives
    in `datasets.rs`/`retention.rs` which no direction in this wave touches, and an existing
    test deliberately locks crawl's non-replayable stamp. It deserves a scouted round on its
    own context, not a drive-by from a crawl sweep. **Banked on [[dataset-storage]] as its next
    anchor.** Mitigating fact that makes deferral safe: `artifact_retention_days` defaults to
    0, so nothing is being reclaimed today.
  - **`web-reliability` reachability** (`index_datasets` so its watches/triggers can fire).
    Real, and its module docstring promises it — but the dataset has **zero readers**, so
    making it watchable is building a door onto a room nobody has entered. Same reasoning that
    killed r15's in-repo ranking function. Promote when something reads the index.
  - **Crawl bypasses the `AppContext` write seam** (`DatasetPageSink` holds `Arc<Datasets>` and
    calls `upsert_many_stamped` directly with `trust = None`, skipping `write_target`, so
    `pages`/`edges`/`page_versions` are the only high-volume datasets immune to quarantine
    diversion; crawl also never calls `observe_extraction`). Genuinely interesting and exactly
    the fleet-wide `observe_extraction` gap r15 banked on [[source-resilience]] — merge it
    there rather than fixing one app in isolation.
  - **`stopped_reason` completeness contract** — the right idea, but it needs a core enum AND
    an app emit, and a cross-crate direction cannot be split across two concurrent lots. The
    half that needs no core change ships this round in D1; the enum follows in a round where
    one lot owns both files.

## Shipped
- (inherited — see [[broad-crawler]])
