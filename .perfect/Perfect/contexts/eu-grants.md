---
name: eu-grants
type: perfect/context
group: Grants Intelligence
category: lib
opportunity: 5
last_proposed: 2026-08-14
cooldown_until: r16
directions: ["[[sweep-end-lift-to-grants-common]]", "[[cordis-sweep-honesty]]", "[[topic-stats-honesty]]", "[[derived-change-honesty]]", "[[sedia-sweep-end-honest]]"]
---

## Current state (scouted 2026-08-12, r14 — engine-depth; full brief in r14 scout report)
eu-sedia (639 ln): daily 10:00 UTC, POST SEDIA search (multipart, fixed boundary),
paged ≤50×100, all records accumulated in memory, drift guard on empty page 1,
CORDIS join per topic_lineage family (ctx.datasets.get cordis/topic_stats, memoized),
writes opportunities (health-gated upsert_many_with_provenance) + grants/unified via
grants-common. cordis (1030 ln): weekly, two-stage (listing ids → per-id detail GET),
persisted cursor in cordis/state, per-project checkpoint, rollup aggregate_topic_stats
over datasets.list(projects, 200k) → upsert_many topic_stats. cordis does NOT depend
on grants-common (own parse_amount/start_year). Both bypass the fetch chokepoint
(inventoried; cordis's 2 GETs carry NO justification and meet none of the block's
criteria). No e2e; run() untested in both; multipart wire contract untested.

Direction-grade gaps (r14 scout, file:line in scout report):
1. Short/transient page ⇒ cordis exhausted=true ⇒ cursor reset to 1 + corpus_swept:
   true (cordis:251-254,:360,:384); total:0 drift ⇒ attempted==0 hole (:335) ⇒
   success + cursor reset. eu-sedia truncated only covers the maxPages cap (:215,:224).
2. topic_stats = partial-corpus aggregate with no as_of/corpus_size/coverage
   (:706-715); eu-sedia embeds verbatim as history.stats — "3 funded, mean €2.1M"
   indistinguishable from complete truth during the ~46-week walk of 23k projects.
3. history block written INTO opportunities before upsert (eu-sedia:250-256) ⇒ weekly
   cordis churn marks every joined topic "changed" daily; watches/revisions/yield
   ledger conflate derived churn with SEDIA publications.
4. Health/trust gating structurally inert twice over: enforce=false default AND no
   grant app calls observe_extraction (only extractor does) — plumbing true, effect
   false.
5. eu-sedia: no checkpoint, unbounded accumulation (~100s of MB at param ceiling);
   non-2xx on page 47 discards everything. cordis solved this for its own stage 2.
6. cordis re-sorts + re-serializes whole done-set per project (stage2_state at :329,
   :397-406) — ~5000 sorts/12.5M clones per full run for a few dozen throttled writes.
7. Money parsed divergently: cordis comma-decimal (:608-611) vs grants-common
   comma-stripping (:1576); repo ships both semantics for EU money. sedia_deadline
   calls Utc::now() per record.
8. Metadata-shape drift (array→scalar) silently rekeys whole corpus (first() at
   :411-413, fallback :378); ""-key collision for unkeyed hits; opportunities vs
   unified silently disagree.
9. maxProjects%pageSize≠0 ⇒ truncated tail skipped for a whole corpus cycle (:198,
   :256,:360).
10. topic_stats ghost families forever (complete recompute written with upsert_many
    not sync_many, :357); AGGREGATE_LIMIT 200k silent; list doesn't exclude tombstones.
Docs: cordis ENTIRELY absent from docs/features/*; apps.md:25-26 no mention of
history join/truncated; datahub.md:85-86 example uses nonexistent eu-sedia "calls";
registry.rs:82 lists cordis as grants publisher (publishes nothing into grants/*).
Catalog: eu-sedia row ok (dated notes); cordis row omits topic_stats (no contract on
the load-bearing joined output); max_row_delta_pct=10 questionable mid-walk.

## Direction history
- (old map w9: SEDIA clean-text.)
- r14 (2026-08-12, director-self-gated, SWEEP): slate of 5 → 3 ACCEPTED:
  [[cordis-sweep-honesty]] (robustness·M), [[topic-stats-honesty]] (robustness·M),
  [[derived-change-honesty]] (robustness·M — core seam + eu-sedia adoption).
  REJECTED-deferred (banked anchors): sedia-rekey-guard (gap 8 — dormant until SEDIA
  drifts; live sliver rare) · sedia-durable-sweep (gap 5 — default scale ~601 topics
  ≈ 7 pages, failure costs one free daily re-run; real at param ceiling only).
  Runner-up noted, not slated: eu-money parser unification (gap 7) — contained to
  cordis amounts; grants-common EU money is Null-by-design pending currency dimension.

## Shipped
- (inherited, pre-46-map)
- r14 (2026-08-12): [[cordis-sweep-honesty]] → 7f525e4 (only page arithmetic
  proves the corpus end; offset cursor — the 450/100 tail revisited; total:0
  drift loud with cursor untouched; checkpoint v2). [[topic-stats-honesty]] →
  a60b996 (coverage block on every stats record; sync_many with the window
  tripwire; ghosts die end-to-end incl. the eu-sedia join; cordis out of the
  grants publisher seed; topic_stats catalog contract). [[derived-change-honesty]]
  → 45dd84b (core DerivedPaths seam — derived paths out of the change hash,
  revisions keep the full value, refresh writes keep reads fresh; eu-sedia
  adopts for history: weekly cordis churn no longer fires watches/webhooks/
  revisions on every joined topic; cordis finally in the feature docs).
  One-time deploy note: first eu-sedia run reports changed≈historyJoined once
  (documented transition), then settles.
- 2026-08-14 (r23): [[sweep-end-lift-to-grants-common]] — **"mechanical" REFUTED, and rejected
  on taste.** r22 banked "SweepEnd exists in three apps with identical semantics". Two are
  identical; **cordis's is not** — 3 variants vs 4 (no `UnknownTotal`), a different `walk_end`
  arity, no `sweep_warning`, and no `grants-common` dependency. Correctly scoped the lift is
  ~45 non-test references whose payoff is "prevents the next divergence", which is the churn
  side of the config.md taste line. **The one outcome-value half is banked instead:** eu-sedia
  still has the collapsed boolean (`:230-231`) that the enum exists to kill — one bool cannot
  distinguish swept-everything / hit-the-cap / short-paged / never-told-the-total — and it is
  the grant app that already depends on `grants-common`. Build THAT, not the lift.
