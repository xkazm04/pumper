# Resilient extraction — implementation notes

Implementation of `docs/features/resilient-extraction.md` on branch
`impl-resilient-extraction`. This document is the honest account: what was built,
what was deliberately left out and why, every deviation from the design, and what
is unverified.

**Verified, by running them:** `cargo check --workspace --all-targets` is clean —
no errors, no warnings. `cargo test --workspace` passes: **326 tests, 0 failures**
(47 of them new on this branch). Three pre-existing assertions changed, each for a
reason recorded under *Deviations*: the `DocReport` wire shape, the extractor's
`worst_fields` treatment of `container_empty`, and the `record_revisions` SELECT
column lists.

**Migration 0020 verified against the real database**, on a copy — the repo's
`data/pumper.db` is 16 migrations behind (4 applied, 5 tables, 6,040 records), so
it exercises the whole chain including the two `ALTER TABLE … ADD COLUMN trust`
statements over real rows. After migrating: 20/20 applied, all five new tables
present, **6,040 of 6,040 records intact**, and all 6,040 carrying `trust IS NULL`
— which *means* `stable`, so the backfill-free claim holds in practice and not
just by argument. The live database was not touched.

---

## What was built

The design's own build order (§13) puts detection first and repair last, arguing
that steps 1–5 "deliver most of the value at none of the risk" and that "if step
5's numbers come back badly, steps 6–8 should not land at all". I followed that,
and stopped where the evidence stops.

### 1. Extraction prerequisites (§2.3, §2.6) — `5dfc608`

- **Post-transform coercion status.** `CoercionStatus { Coerced | CoercionFailed
  | NoTransforms }` on `DocReport`, orthogonal to `FieldStatus`. This is the
  wrong-element signature: `to_number` on `"Add to cart"` yields null while the
  field still reports `matched`.
- **`each` container split.** `Rule::Each` takes an optional `container`
  selector; an empty result then splits into `FieldStatus::ContainerEmpty` (the
  listing was found and held nothing) versus `Empty` (the listing is gone).
- **`dom_simhash`.** A markup-shape fingerprint (tag, sorted classes,
  id-presence) that is text-blind where the existing SimHash is structure-blind,
  plus `drift()` normalizing Hamming distance to `[0,1]`. Build digests fold to
  their stem so a webpack rebuild is not a redesign.
- **`markdown::visible_text_capped`** — the content fingerprint's input, sharing
  one walker with `text_len_capped` so "visible" cannot mean two things.

### 2. Schema and config (§5, §9) — `62dc6b1`

Migration `0020_resilient_extraction.sql`: `sources`, `source_runs`,
`field_sketches`, `field_invariants`, `doc_fingerprints`, and a `trust` column on
`records` / `record_revisions`. `[resilience]` config with `#[serde(default)]` +
a manual `Default`, and six `Config::validate()` rules.

### 3. The detection path (§2) — `cd78c2d`

`crates/core/src/resilience/`: `sketch.rs` (fixed-size per-field summaries and
the statistics on them), `detect.rs` (the pure verdict), `invariants.rs`
(mining + checking), `store.rs` (persistence and the `Resilience` service).

Seven signals, all computable from what the extraction pass already produced:

| Signal | What it discriminates |
|---|---|
| fetch gate | transient/fetch-layer, and it gates everything else |
| total collapse | "the selector is simply gone", without needing a cohort |
| Wilson-separated miss-rate rise | selector stopped matching |
| Wilson-separated coercion-failure rise | selector matched the *wrong* element |
| distinctness collapse | selector rebound to a template element |
| mined-invariant violation | a regularity the source held for its whole history broke |
| length / char-class shape drift | the values are no longer the same *kind* of thing |
| input–output divergence | **whose** change it was: redesign vs new content vs our own bug |

Scoring, diagnosis and the hysteresis ladder are pure functions with no I/O, no
clock and no model, so a verdict is reproducible from its stored `source_runs`
row.

### 4. Enforcement (§7) — `b0b98cc`

The three things the design says must never happen, each guarded at the seam
every caller reaches the resource through:

1. **A degrading source never tombstones its own dataset.** `sync_many`
   downgrades to `upsert_many` when the state suppresses removals. The check is
   inside `AppContext::sync_many`, not in each app — per the repo's own
   "guard-at-the-wrong-seam" anti-pattern.
2. **A degrading source never pushes.** Watches and triggers are dropped per
   dataset in the worker's post-run hook, *before* the hooks run.
3. **A degrading source never poisons the search index.** `index_datasets` skips
   a suppressed source.

Plus trust stamping, the `<ds>@q` shadow dataset, and the `trust=` filters on the
pull surfaces. All of it inert unless `enforce = true`.

### 5. API and app wiring — `b0b98cc`, `f28033d`

`GET /sources`, `GET /sources/{id}`, `GET /sources/{id}/runs`,
`POST /sources/{id}/state`. The `extractor` app fingerprints each document and
reports every run.

---

## What was deliberately NOT built

### Repair, in its entirety (§6, §8.1–8.3)

No candidate generation, no validation gates, no LLM call, no promotion, no
rollback, no money spent.

**Why:** the design's own §12.3 names the falsifier — "of *promoted* repairs, the
fraction that reproduce the pre-mutation values on a blind set … target ≥ 0.95.
*If promoted-but-wrong exceeds 5%, auto-promotion is not safe on this evidence
and `mode` must default to `off`*". That number is the output of the evaluation
harness (§12.1), which does not exist. Shipping auto-promotion before the
measurement that would justify it inverts the design's own logic, and the failure
mode is a dataset silently corrupted with full system confidence.

The honest consequence: a degrading source is detected, quarantined, and reported;
fixing it is an operator action followed by `POST /sources/{id}/state`. The design
calls a stuck source "an acceptable terminal state", and that is what it is.

### The profile registry (§4)

Rules stay a job parameter. It is a prerequisite for repair only — detection keys
on `(app, dataset)` and needs none of it.

**Cost:** `source_runs` cannot stamp a `profile_version`, so the
`self_inflicted` diagnosis narrows the affected era by `build_id` alone (which is
stamped). §3.3's recovery story — "`POST /sources/{id}/reextract` replays a
corrected ruleset over stored bodies" — is therefore also absent; the underlying
guarantee it rests on (append-only revisions, so the pre-breakage values are
still there) is intact and unchanged.

### Golden documents (§3.2, §6.3)

No `data/golden/` retention store, no pinned-document check.

**Cost, and it is the sharpest one:** the design gives golden docs two jobs, and
both are unfilled. (a) They are the **only** detection available to sources that
never form a cohort (§10.5: a single-URL `watch`, a handful of rows). Today those
sources have *no* detection rather than golden-doc-only detection — reported as
`statistical_coverage: false` on `GET /sources/{id}` rather than hidden. (b) They
are the **anchor** against baseline poisoning (§10.4): a rolling baseline can be
boiled-frogged by degradation slower than its own window, and without a fixed
reference nothing catches that. The design already calls this "the one failure
mode I consider genuinely incompletely solved"; without golden docs it is not
solved at all.

### Auto-retire (§2.7)

`retired` is a state an operator can set; nothing reaches it automatically. The
design's rule ("404/410/DNS on ≥ 90% of URLs across 3 runs") needs a per-run
permanent-failure rate that `FetchHealth` does not carry today. Detecting a dead
source is `/catalog/health`'s job — a source that stops producing is a freshness
question, not an extraction-quality one.

### A/B variant detection (§10.2), platform-change detection (§10.8)

Both are cross-cutting: `ab_variant` needs 2-means clustering over per-cohort DOM
fingerprints, and `platform_change` needs a fleet-wide epoch correlation across
sources. Neither is hard; both are worth having; both were below the line once
detection + enforcement + tests were done properly. Their absence is a
**recall/false-positive gap, not a safety gap**, with one exception worth stating
plainly: without §10.8, a pumper release that changes `to_number` or the markdown
converter would move every numeric field on every source in the same epoch and
read as thirty sites breaking at once. `build_id` is stamped on every run row, so
the correlation is one query — but nothing performs it automatically. With
`enforce = false` the consequence is a misleading `/sources` table; with
enforcement on it would be a fleet-wide false quarantine. **Do not enable
`enforce` fleet-wide across a release boundary without checking `build_id`
correlation by hand.**

### Health webhooks (§7.3), `/metrics` gauges (§8.4)

`source.degraded` / `source.quarantined` / etc. are not emitted, and the
`pumper_source_*` gauges are not exported. State transitions are `warn!`-logged
and every verdict is queryable. The design calls the alerts "what replaces the
human reviewer: nobody approves anything, but somebody is told" — today nobody is
told unless they look. This is the single highest-value remaining piece and the
cheapest (`webhook::dispatch_event` already exists and is the enforced path).

### The evaluation harness and canary (§10.9, §12)

No `resilience-eval` bin, no mutation taxonomy, no historical backtest, no
canary source. Consequently **no recall or false-positive-rate number in this
implementation has been measured** — see *Unverified* below.

---

## Deviations from the design

Each of these is a place the design said one thing and the implementation does
another, with the reason.

### 1. A distinctness collapse is conclusive, not just weighted

**Design (§2.7):** distinctness contributes `0.20` to the weighted score.
**Implementation:** a per-record field collapsing to near-constant sets the score
to `1.0` outright, under tight preconditions.

**Why:** the weights cannot express what §3.1 claims. A textbook silent rebind
produces `S_distinct ≈ 1.0`, `S_shape ≈ 1.0`, `S_divergence = 1.0` and *no*
miss-rate rise, which sums to `0.2 + 0.15 + 0.15 = 0.49` — below the `0.6`
degrade threshold. So the design's own "single highest-precision
silent-corruption signal in the design" could never trip a source, with every
other available signal corroborating it. Either the weights are wrong or the
claim is; I took the claim, which §3.1 argues for at length ("there is no benign
reason for a per-record field to become constant across 30 documents").

**Guards** (`detect::conclusive_rebind`): baseline distinctness ≥ 0.9 over ≥ 3
healthy runs, run distinctness ≤ 0.1, a full cohort, **and** the divergence
signal must not say `content_changed` — if the words on every page changed and
the markup did not, the site really did start saying the same thing everywhere,
and that is not our bug.

### 2. Wilson intervals do not make small runs inert

**Design (§2.4):** "This is what makes a 3-document run incapable of tripping
anything — its Wilson interval is enormous — without needing a special case."
**Reality:** 3 misses out of 3 against a 5% baseline has `p < 10⁻³`; the Wilson
lower bound is ≈ 0.30 against a baseline upper bound of ≈ 0.08, so it separates —
correctly. The claim is false as stated.

What actually makes small runs inert is the **cohort floor**, which the design
also specifies. The two are complementary, not redundant, and a test
(`wilson_separation_needs_evidence_not_just_a_big_ratio`) documents the real
behaviour rather than asserting the design's claim.

### 3. Build-digest class folding keeps the stem

**Design (§2.3):** hashed class tokens "are folded to a `#hash` placeholder".
**Implementation:** only the digest folds; the stem survives (`card-1a2b3c4d` →
`card-#`).

**Why:** folding the whole token also erases the stem, so renaming
`card-1a2b3c4d` to `tile-5e6f7a8b` becomes invisible — real signal thrown away to
suppress noise. Keeping the stem suppresses exactly the digest churn.

### 4. `d_val` is a fingerprint drift, not an exact-mismatch fraction

**Design (§2.3):** "exact-mismatch fraction for values".
**Implementation:** `drift(simhash_value(before), simhash_value(after))`, stored
as a third column on `doc_fingerprints`.

**Why:** two reasons. (a) The design aggregates each drift as the cohort
**median**, and the median of a 0/1 indicator is degenerate — it is 0 or 1 and
carries no magnitude. (b) An exact comparison needs the previous *values*, which
means reading `records` and couples the detector to the dataset write ordering; a
self-contained fingerprint column does not.

### 5. Diagnoses are recorded on untripped runs

**Design:** `source_runs.diagnosis` is implicitly for tripped runs.
**Implementation:** always recorded when any signal has an opinion.
`content_changed` on a healthy run is real information, and a run row that
explains itself is the point of keeping them. A genuinely clean run gets `NULL`,
not `ambiguous`.

### 6. Divergence names the cause but does not convict

At weight `0.15`, `markup_drift` alone cannot trip a run — and should not. "The
values moved and the markup moved, with every field still matching, staying
distinct and keeping its shape" is also what a content update delivered through a
template change looks like. The design's §2.3 table reads as though the
`low/high/high` cell is conclusive; it is corroborative. A redesign that actually
*broke* the extractor moves a field statistic too, and that is what trips.

### 7. `RunVerdict` has no `suspect` variant; `degraded` recovers one rung

The design lists `suspect` as both a run verdict and a source state. As a verdict
it would need a second threshold the design never specifies, so verdicts are
`ok | inconclusive | content_empty | broken | self_inflicted` and `suspect` is a
state only. The design's ladder also does not say how `degraded` recovers; a
clean run steps it back to `suspect` (one rung), not straight to `healthy`.

`degraded → quarantined` on "2 more tripped" is implemented as three *consecutive*
tripped runs, which is the unambiguous reading of a 3-run window.

### 8. `DocReport` is no longer serde-transparent (a wire change)

Carrying the orthogonal coercion map means `DocReport` gained a second field, so
`POST /extract/preview`'s `report` object nests the statuses one level down under
`report.fields`. Documented in `extraction.md`. The alternative (flattening
coercion into each field's status object) preserved the wire shape but broke
every `report.fields[x] == FieldStatus::Matched` comparison in the codebase; I
took the wire change and updated the doc, because this repo's convention is that
docs move with the surface.

### 9. Sub-cohort runs are `ok`, and they do enter the baseline

**Design (§2.4):** cohorts are formed by a sliding window over recent runs until
`min_cohort_docs` is met.
**Implementation:** a run below the floor is judged `ok` with score 0 and
`statistical_coverage: false` (only total collapse can fire), and its sketches
*do* join the baseline pool.

**Why:** the baseline is already a pool over `window_runs`, so pooling small runs
into it *is* the sliding window, with much less machinery. **The cost is real and
I want it stated:** a small run that is broken but unjudged pools its broken
sketches into the baseline, which is exactly the "a broken run must never be
absorbed into the baseline it is being judged against" rule the design states.
Total collapse (which fires at ≥ 5 documents) catches the worst case. For a
source whose runs are *always* below the floor this is a genuine weakness, and it
is the same population golden docs were supposed to serve.

### 10. Sketch fields added; some design columns omitted

`field_sketches` gained `container_empty` and `coerced` (the latter is the
coercion-rate denominator — without it the rate cannot be pooled across baseline
runs). `doc_fingerprints` gained `val_simhash` (deviation 4). The design's
`extraction_profiles`, `profile_versions`, `golden_docs`, `repair_attempts` and
`repair_candidates` tables are **not created**: nothing would write them, and an
empty table reads as a feature that exists.

### 11. Shape drift has an absolute floor as well as a z-score

A source whose baseline runs produce byte-identical length/char-class profiles has
zero scale, and the design's `|z| ≥ 3.5` rule then fires on a total-variation
distance of 0.03 (one value in thirty a character longer). `SHAPE_TOL = 0.10`
floors it. Found by a test, not by reading.

### 12. Enforcement is gated as a unit on `enforce`

The design says `enforce = false` "computes verdicts, gates nothing". I read
trust stamping as gating, so it too is off in soak mode. The effect is that a
default install's only observable changes are the new tables, the recorded
verdicts, and the `/sources` endpoints.

### 13. Fetch health from tier verdicts, approximately

`FetchHealth.ok` counts fetches where some tier's structured `TierVerdict` was
`Ok` with a 2xx (or no status). The design also wanted `content_chars` and
`cache_hit` folded in; they are available on `TierTrace` and unused. A
source-mode run over stored bodies reports `attempted: 0`, which reads as rate
1.0 — the fetch layer cannot explain a bad extraction when nothing was fetched.

---

## Honest limits

Things that are true about this implementation and are not going to be fixed by
reading the code more carefully.

1. **No measured recall, and no measured false-positive rate.** Every threshold
   in `[resilience]` is a starting guess. The design's binding constraint is
   "FPR ≤ 0.3% per run … FPR is the binding constraint and recall is tuned
   against it" — I have not measured either. This is why `enforce` ships `false`,
   and why flipping it should follow §12.6's soak on real data rather than
   confidence in this code.
2. **Only the `extractor` app reports runs.** `plugin` and every hardcoded-Rust
   app (grants-gov, eu-sedia, the census apps, …) are invisible to the detector —
   they never call `observe_extraction`, so they have no `sources` row and no
   verdict. The gating code paths *are* wired for them (`sync_many`,
   `upsert_many`, the worker hooks all consult health regardless of app), so
   they inherit enforcement the moment they report. `plugin` would be a ~30-line
   change mirroring the extractor; the hardcoded apps need a `DocReport` they do
   not currently produce.
3. **Fingerprinting parses each body a second time.** The design says
   `dom_simhash` "costs one extra walk of a tree we have already parsed". In
   practice extraction parses inside `extract_one_impl` and does not hand the
   tree back, so `doc_signals` parses again — roughly one extra HTML parse per
   document, on the rayon path, off the async runtime. On a workload dominated by
   network I/O this is very likely irrelevant; **I have not measured it.**
   Threading the parsed tree out of the extraction pass would remove it.
4. **Detection latency for small sources is unbounded.** A source producing
   fewer than `min_cohort_docs` documents per run is watched only by total
   collapse and, once its baseline exists, the conclusive-rebind rule. Slow
   degradation on such a source will not be caught. `statistical_coverage: false`
   is how it says so.
5. **Baseline poisoning is not solved.** With golden docs absent there is no
   fixed anchor, so degradation slower than the rolling window can walk the
   baseline with it. The design flags this as incompletely solved *with* the
   anchor; without it, it is open.
6. **`doc_fingerprints` grows monotonically.** One row per key per source,
   overwritten in place, never pruned — same order as `records`, but a source
   whose keys churn (URLs that change every run) accumulates dead rows.
   `HealthStore::prune` covers `source_runs` and `field_sketches` only.
7. **Sketch pruning has never been observed running.** The 6-hourly janitor in
   `main.rs` now calls `HealthStore::prune` whenever detection is enabled (and no
   longer early-returns when revision retention is off, which it previously did —
   so before this change `sketch_retention_runs` was purely advisory). `prune` is
   covered by an integration test; the *scheduling* of it is not, and I have not
   watched a live janitor tick.
8. **Nothing alerts.** See *Health webhooks* above.
9. **`Diagnosis::SelfInflicted` cannot be attributed to a rule change.** Without
   the profile registry there is no `profile_version` to diff, so the diagnosis
   says "this was us" and stops. `build_id` narrows it to a release.
10. **The change-feed default changed.** `GET /changes` now defaults to
    `trust=stable`. Because an unstamped revision *is* stable and stamping only
    happens under `enforce`, this is a no-op on any existing deployment — but it
    is a default-behaviour change on a shipped endpoint, and a client that starts
    seeing fewer rows after enabling enforcement is seeing this, not a bug.
11. **`SourceState::parse` fails open to `Healthy`.** An unrecognized state
    column reads as healthy rather than erroring, deliberately: this sits on the
    write path of every app and a health lookup that fails must never stop a
    working pipeline. The cost is that a corrupted state column silently disables
    gating for that source rather than announcing itself.

---

## Unverified

Claims I did not check. Listed separately because "I did not verify this" is a
correct answer and blurring it into the section above would not be.

- **Every performance claim.** No benchmark was run. The design's "+10–20% of
  extraction CPU" for fingerprinting, the "~2 MB/day of sketches at current fleet
  size", and my own assertion that the extra HTML parse is irrelevant are all
  unmeasured. The only thing I can say with confidence is the *shape*: per run
  the writes are `1 + fields` rows plus one upsert per key, chunked at 500 on one
  held connection.
- **Behaviour against a real degrading site.** Every test uses synthetic
  documents. The redesign scenario in `crates/core/tests/resilience.rs` is a
  class rename I wrote, not a redesign that happened. Whether the drift
  thresholds (`0.08` / `0.20`) bracket real-world redesigns correctly is exactly
  what §12.1's harness would answer.
- **The mined-invariant thresholds against real data.**
  `invariant_min_support = 500` at `0.99` confidence is the design's number. On
  the repo's actual datasets (5,196 records across ~15 datasets) I do not know
  how many fields clear it, or what gets mined. The integration test lowers
  support to 10 to exercise the path at all — which is itself evidence that the
  shipped default may mine nothing for most sources.
- **The server has not been run.** `cargo check`/`cargo test` pass; I did not
  start `pumper-server` and did not issue a request to `/sources`. The four
  endpoints are exercised only by the OpenAPI coverage test, which proves they are
  registered, not that they return what their annotations claim.
- **Whether `enforce = true` behaves well end-to-end under the worker.** The
  gating paths are unit- and integration-tested through `AppContext` and are
  read by the worker hooks, but no test drives a full job through the worker with
  a quarantined source. The push-suppression ordering is asserted by reading the
  code, not by a test.
- **OpenAPI shape of the new endpoints.** The coverage test proves the four
  routes are in the spec and in the inventory. I did not validate the response
  descriptions against actual responses beyond compiling them.
