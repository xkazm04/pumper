# Resilient extraction — degradation detection, quarantine & self-repair

> **Status: detection and enforcement are implemented; repair is not.** Sections
> 0–5 and 7–9 describe shipped surface. §4 (profile registry), §6 (repair), §8.1–8.3
> (promotion/rollback) and §12's evaluation harness are **not built** — see the
> per-section markers below and [`IMPLEMENTATION-NOTES.md`](../../IMPLEMENTATION-NOTES.md)
> at the repo root for what was built, what was deliberately left out and why,
> and every place the implementation deviates from this design.
>
> (Convention note: `docs/features/README.md` says deep design rationale belongs
> in `docs/harness/`. The brief specified this path, so the rationale still lives
> here rather than being split mid-flight; the sections below double as the
> reference for the signals the shipped detector computes.)

## What ships today

| Capability | Status | Where |
|---|---|---|
| Post-transform coercion status, `each` container split, `dom_simhash` (§2.3, §2.6) | **shipped** | `core::extract`, `core::simhash` |
| The seven detection signals, scoring, diagnosis (§2) | **shipped** | `core::resilience::detect` |
| Per-field sketches + the statistics on them (§2.4) | **shipped** | `core::resilience::sketch` |
| Mined invariants (§2.5) | **shipped** | `core::resilience::invariants` |
| Health ladder with hysteresis (§2.7) | **shipped**, minus `retired` (manual only) | `core::resilience::detect::next_state` |
| Automatic recovery `quarantined → probation → healthy` (§2.7) | **shipped** — `recovery_runs` consecutive clean *judged* runs per rung | `core::resilience::detect::{Recovery, next_state}` |
| Per-source cohort adequacy + honest `unmonitored` (§2.4) | **shipped** — verdict `below_cohort`, `cohort: full\|shrunken\|chronic`, `monitored` on `GET /sources` | `core::resilience::detect::cohort_adequacy` |
| Schema + persistence (§5) | **shipped** for the tables detection needs | migration `0020` |
| Trust stamping, `sync_many` downgrade, push suppression, index skip, quarantine dataset (§7) | **shipped**, gated on `enforce` | `core::app`, `server::worker` |
| `GET /sources`, `/sources/{id}`, `/sources/{id}/runs`, `POST /sources/{id}/state` (§8.4) | **shipped** | `server::routes` |
| Config surface + validation (§9) | **shipped** | `[resilience]` |
| Golden documents (§3.2, §6.3 retention store) | **not built** |  |
| Profile registry (§4) | **not built** |  |
| Repair: inversion, Claude proposals, validation gates, promotion, rollback (§6, §8.1–8.3) | **not built** |  |
| Health webhooks (§7.3), `/metrics` gauges (§8.4) | **not built** |  |
| `resilience-eval` mutation harness, canary source (§10.9, §12) | **not built** |  |

`[resilience] enforce = false` ships as the default: every verdict is computed and
stored, nothing is gated. That is §12.6's soak mode, and it is the state the
system is in until an operator reads `GET /sources` and decides otherwise.

---

## 0. The problem, stated precisely

Extraction rots silently. Three distinguishable failures, only one of which the
system currently notices:

| Failure | What pumper sees today | Damage |
|---|---|---|
| Selector stops matching | `FieldStatus::Empty` — identical to "this document genuinely lacks the field" | dataset quietly loses a column |
| Selector still matches, **wrong element** | `FieldStatus::Matched`, every counter green | dataset fills with plausible garbage; `record_revisions` records the garbage as a legitimate change; watches/webhooks push it downstream |
| Fetch degraded (bot wall, 5xx, thin body) | `TierTrace.verdict` = `blocked`/`thin` — *is* visible per fetch, but nothing aggregates it into a judgement about the dataset | mass false `removed` tombstones via `sync_many` |

There is no ground truth. Nobody labels the web. Any design that needs a label
that isn't already in this database is not buildable here.

**The central epistemic claim of this design:** *the past is the only ground
truth available, and it is enough.* pumper already stores, for every record,
every revision, with field-level diffs (`record_revisions`, migration 0005),
plus a content hash and a 64-bit SimHash. That history is a labelled corpus of
"what this extractor produced during the era in which we believed it worked."
Every detector, every learned invariant, and every repair validation below is
grounded in that corpus and nothing else. No LLM judges correctness anywhere in
an accept path, and no human reviews anything.

---

## 1. Assumptions resolved (the brief was ambiguous; these are the calls I made)

1. **The unit of health is a *source* = `(app, dataset)`**, optionally narrowed
   by extraction profile. Not a URL, not a field, not a job. `(app, dataset)` is
   the unit every existing surface already keys on — watches, triggers,
   `index_datasets`, `/changes`, the catalog — so gating consumers is a matter of
   consulting one row, not joining three.
2. **Detection is universal; repair is not.** Every dataset gets a health verdict,
   because the coarse signals (fetch verdicts, upsert new/changed/unchanged split,
   record SimHash drift) are produced by the runtime, not the app. Fine-grained
   signals (per-field sketches) need the app to report a `DocReport`, which today
   only `extractor`/`plugin` do. **Automated repair is restricted to sources
   backed by a declarative `RuleSet`.** A hardcoded-Rust app (grants-gov,
   eu-sedia, the census apps) can be detected, quarantined and alerted on; it
   cannot be repaired, because repairing it means editing Rust, and this system
   does not get to edit and rebuild itself unattended. Stated as a non-goal in §11.
3. **No human is in any loop.** Every "who authorizes this" answer below is a
   deterministic policy in code. Where a design would normally say "queue for
   review", this one says "stay quarantined and keep alerting" — a stuck source is
   an acceptable terminal state; a wrong auto-promotion is not.
4. **Trust is per-record, not per-field.** A per-field trust map would be more
   precise and would force every consumer to understand it. Records carry one
   `trust` value. The per-field detail lives in `source_runs`/`field_sketches` for
   anyone who wants it.
5. **Repair depends on retained page bodies.** Artifact retention exists now
   (`[storage] artifact_retention_days`, off by default, provenance-pinned — see
   `datasets.md`), but it is an *operator* policy over per-job dirs and nothing
   in it knows what repair needs. This design therefore keeps its own small,
   explicit retention store (`data/golden/`, §6.3) rather than depending on
   whatever window the deployment happens to configure.
6. **`/changes` defaults to trusted-only; push surfaces suppress.** Pull APIs are
   re-readable and therefore recoverable, so they filter but stay inspectable.
   Pushes (webhooks, triggers) are irreversible once sent, so they suppress. §7.3.
7. **SimHash stays as-is.** Its tokenizer is version-stable FNV-1a + splitmix64
   and there is a `reindex` bin; this design adds a *second, structural*
   fingerprint rather than changing the existing one (which would invalidate 5k+
   persisted fingerprints — see the 2026-07-15 reindex entry in
   `docs/harness/harness-learnings.md`).

---

## 2. Detection without ground truth

### 2.1 The signals, and why these

Everything below is computable from data pumper already has or already parses.
Nothing requires an extra fetch.

| Signal | Source | What it discriminates |
|---|---|---|
| `FieldStatus` counts (`matched`/`empty`/`error`) | `extract_batch_with_report` — exists | selector stopped matching |
| **post-transform coercion status** (new, §2.6) | `extract.rs` transform chain | selector matched the *wrong* element and the transform can't coerce it |
| `TierTrace.verdict`, `http_status`, `content_chars`, `cache_hit` | `FetchOutcome.trace` — exists | **transient/fetch-layer**, gates everything else |
| governor penalty, `tier_memory` strikes | exists | host-level trouble, corroborates the above |
| `UpsertSummary` new/changed/unchanged/removed | exists | run-level shape of the write |
| record `simhash` + `record_revisions` diffs | exists | how much the *output* moved |
| **document text SimHash** (`simhash(body_text)`) | primitive exists, not stored per-doc | how much the *input content* moved |
| **document DOM SimHash** (new, §2.3) | one pass over the HTML we already parse | how much the *input structure* moved |
| per-field distributional sketch (new, §2.4) | accumulated during the rayon extraction pass | value-domain drift, distinctness collapse |
| learned per-field invariants (new, §2.5) | mined from `record_revisions` | silent corruption |
| pinned golden documents (new, §6.3) | sampled bodies + their extractions | deterministic, statistics-free ground truth for a handful of pages |

### 2.2 The gating rule: fetch first, always

**A run whose fetch layer is unhealthy produces no extraction verdict.** If
`fetch_ok_rate = (fetches with a winning tier verdict `ok` and 2xx) / attempted`
is below `[resilience] fetch_ok_floor` (default `0.7`), the run is recorded with
verdict `inconclusive`: the source's state does not change, the run does **not**
enter the rolling baseline, and no repair is dispatched. You cannot judge an
extractor on documents you did not receive.

The same rule, generalised: **a run that could not be judged never becomes
evidence.** `inconclusive` (fetch), `content_empty` (the listing was there and
empty) and `below_cohort` (§2.4) all move neither the state nor the baseline.

This is the entire answer to "was the fetch transient?" and it is essentially
free, because `TierTrace` already carries a typed `verdict` enum and
`harness-learnings.md` is explicit that the trace — not the `escalations` prose —
is the machine-readable signal.

Corollary, and this one is load-bearing: **a source in `degraded` or
`quarantined` state has `sync_many` downgraded to `upsert_many`** (§7.2). A
half-broken run produces a short-but-nonempty batch, and `detect_removed`
tombstones every key missing from it. `detect_removed` already no-ops on an
*empty* `present` list (fixed 2026-07-14); a partially-broken run is the case
that guard does not cover, and it is the single most destructive thing a
degrading source can do. `detect_removed` now *requires* a `RemovalGuard`, which
only a non-degrading `SourceState` yields, so the downgrade cannot be bypassed by
a caller that reaches past `sync_many` (§7.2).

### 2.3 The input–output divergence test (the core idea)

The decisive observation is an asymmetry in what our two fingerprints see:

- **SimHash over text** is *structure-blind*. It moves when the words move.
- **Extraction** is *structure-bound*. It moves when the DOM moves.

So compute a second fingerprint that is text-blind and structure-bound:

```rust
/// SimHash over the document's *shape*: the pre-order sequence of
/// (tag, sorted class tokens, id-presence) triples, text nodes ignored.
/// Class tokens that look like build hashes (`^[A-Za-z]+[-_][0-9a-f]{6,}$`)
/// are folded to a `#hash` placeholder so a Tailwind/webpack rebuild that
/// changes nothing visible doesn't read as a redesign.
pub fn dom_simhash(html: &Html) -> u64;
```

It costs one extra walk of a tree we have already parsed — the same order of
work as `text_len_capped`, on the rayon path, and roughly 10–20% on top of
extraction CPU, which is not the bottleneck (fetching is).

Now, per key present in both this run and the previous one, define three
normalised drifts in `[0,1]` (Hamming distance ÷ 64, or exact-mismatch fraction
for values):

- `d_text` — how much the visible content changed
- `d_dom` — how much the markup structure changed
- `d_val` — how much the extracted record changed

Aggregate each as the cohort **median** (robust to a few genuinely-changed
records). The joint position in that 3-space is the diagnosis:

| `d_text` | `d_dom` | `d_val` | Diagnosis | Repairable? |
|---|---|---|---|---|
| low | low | low | `ok` | — |
| **high** | low | high | `content_changed` — words moved, markup didn't, extractor tracked it | no (healthy) |
| **low** | **high** | **high** | `markup_drift` — **the site was redesigned and the extractor broke** | **yes** |
| low | low | **high** | `self_inflicted` — neither input moved; *we* changed | no — **roll back**, don't repair |
| high | high | high | `ambiguous` — corroborate with §2.4/§2.5 before acting | only if corroborated |
| any | any | low, but miss-rate spiked | `field_loss` — some fields vanished without output churn | yes |

The `low / low / high` row is worth dwelling on: if neither the text nor the
structure of the input changed and the output did, extraction is no longer a
function of its input. The only explanations are a change in the ruleset, the
transform semantics, or the parser — all of which are *ours*. Every run row
stamps the `profile_version` and the pumper build id, so this diagnosis reduces
to a diff between two rule versions and the action is rollback, never a Claude
repair. Proposing a new selector for a regression we caused is how a system
learns to paper over its own bugs.

`markup_drift` — text unchanged, DOM changed, values changed — is exactly the
redesign case that the existing text SimHash cannot see and that no counter in
the system currently notices. It is the case this whole subsystem exists for.

### 2.4 Per-field sketches and the statistical shape of the decision

Per `(source, run, field)`, accumulate during the extraction pass:

```rust
pub struct FieldSketch {
    pub n: u32,                 // documents in the cohort
    pub matched: u32,
    pub empty: u32,
    pub error: u32,
    pub coercion_failed: u32,   // new, §2.6
    // length distribution: Welford moments + a 16-bucket log2 histogram
    pub len_sum: f64,
    pub len_sumsq: f64,
    pub len_hist: [u16; 16],
    // character-class profile of the concatenated values
    pub cls: [f32; 4],          // digit, alpha, space, punct fractions
    // distinctness within the cohort
    pub distinct_ratio: f32,    // distinct values / non-empty values
    // 64×u64 k-minhash over the value multiset → Jaccard vs any other run
    pub minhash: [u64; 64],
}
```

512 bytes of minhash + ~80 bytes of scalars per field per run. Thirty fields ×
four runs/day × thirty sources ≈ 2 MB/day before pruning; the retention janitor
keeps `window_runs × 3` runs (§9.4).

**The statistical shape is deliberately two different tests, because the two
kinds of signal have different natural variance:**

1. **Rate signals** (miss rate, error rate, coercion-failure rate) are
   proportions with a *known* sampling distribution, so compare them properly:
   the run's **Wilson 95% lower bound** must exceed the pooled baseline's Wilson
   95% upper bound before the rate counts as risen. This is what makes a 3-document
   run incapable of tripping anything — its Wilson interval is enormous — without
   needing a special case.
2. **Distributional signals** (length moments, char-class vector, distinct-ratio,
   Jaccard-vs-previous) are compared against the rolling baseline with a
   **robust z-score** — median and MAD across the last `window_runs` healthy runs,
   `z = 0.6745·(x − median)/MAD`, flagged at `|z| ≥ 3.5` (the standard
   Iglewicz–Hoaglin criterion). Median/MAD rather than mean/σ because miss-rate
   and length distributions are skewed and a single bad run would inflate σ enough
   to hide the next five. Length-histogram and char-class comparisons use total
   variation distance against the baseline median histogram, itself z-scored
   against the baseline's own run-to-run TV distances — i.e. "is today's drift
   large *relative to how much this source normally drifts*", which is the only
   scale-free way to threshold a source that is naturally noisy.

   **Zero-variance baselines.** A stable templated field produces a baseline that
   has never varied at all (`distinct_ratio` of exactly 1.0 on twenty consecutive
   runs is the common case), so there is no scale to divide by. The tolerance
   stands in as the scale — one tolerance of departure is one unit of
   significance, saturating at `ZERO_SCALE_Z_CAP` (25, far above any usable
   `mad_z`). It used to be ±∞, which reported the same maximal significance for
   three duplicate values in a cohort of thirty as for a total collapse.

**Cohort formation.** The unit of analysis is a cohort of at least
`[resilience] min_cohort_docs` (default 30) documents. *As built, a cohort is one
run* — the sliding multi-run cohort described below is **not** implemented; a run
below the floor is simply not judged.

A below-floor run is recorded with verdict **`below_cohort`**, which — like
`inconclusive` — moves neither the source's state nor its rolling baseline. That
last half is load-bearing: such a run used to be recorded as `ok`, and `ok` is
baseline material, so a source that never reached the floor assembled a baseline
entirely out of runs nobody had judged and then measured itself against it. A
self-referential history can never catch a silent rebind.

Which *kind* of small a run was is decided **per source**, not by the fleet-wide
constant alone (`detect::cohort_adequacy`), and rides on the verdict as
`cohort: full | shrunken | chronic`:

| `cohort` | Meaning | Consequence |
|---|---|---|
| `full` | at or above the floor | judged; every distributional test applies |
| `shrunken` | below the floor, but this source has cleared it before | not judged — the listing got smaller, which is itself worth seeing |
| `chronic` | below the floor and never above it in the retained window | not judged, and the source is **unmonitored** |

Nothing here *lowers* the bar: a thin source is not made easier to trip, it is
made honestly labelled. `GET /sources` carries `monitored` per row plus an
`unmonitored` count, where `monitored: false` means no retained run ever cleared
the floor — so `state: "healthy"` on that row means *unwatched*, not *verified*.
`GET /sources/{id}` keeps `statistical_coverage` for the latest run.

Sources that never reach the floor (e.g. `watch` on a single URL) were designed to
get **golden-doc detection only** (§6.3) — exact, deterministic, no statistics —
but golden docs are not built (§3.2), so today they are watched by the
assumption-free rules alone and say so.

Below the cohort floor exactly one rule still fires, because it needs no
distributional assumption: **total collapse** — `miss_rate ≥ 0.9` with a baseline
`≤ 0.1` over `n ≥ 5` documents and a healthy fetch layer. Under the baseline rate
that outcome has probability `< 10⁻³`; it is the "the selector is simply gone"
case and it should not wait a week.

### 2.5 Learned invariants (mined from history, not asserted by a human)

Once per `[resilience] invariant_refresh_days` (default 14), for each source,
sample up to 2000 live records and mine invariants that held with confidence
`≥ 0.99` over support `≥ 500` records:

- **type** — `field is always number` / `always string` / `always array`
- **regex class** — the value always matches a mined character-class pattern
  (`^\d{4}-\d{2}-\d{2}$`, `^https?://`, `^[A-Z]{2}-\d+$`); mined by generalising
  the character classes of observed values, not by an LLM
- **range** — numeric min/max with 1% tails trimmed
- **non-null** — the field is never empty
- **distinctness** — `distinct_ratio ≥ 0.9` (per-record field) or `≤ 0.05`
  (constant field); both are informative
- **pair ordering** — for the ≤ 20 most-populated numeric pairs, `a ≤ b` holds
  ≥ 99.9% (`price ≤ list_price`, `open ≤ total`)

An invariant is **violated** when a cohort breaks it in ≥ 20% of documents. A
violated `distinctness ≥ 0.9` invariant is the single highest-precision silent-
corruption signal in the design (§3.1).

Invariants are mined **only from runs that were `ok` at the time**, and are
stamped with the `profile_version` they were mined under. A promoted repair
invalidates nothing automatically — the invariants are the thing the repair is
checked *against* (§6.4) — but they are re-mined after probation clears.

### 2.6 Two small gaps in the existing extractor this design closes

Both are cheap and both are prerequisites, so they land first:

1. **Post-transform status.** `DocReport`'s `FieldStatus` is computed *before*
   transforms — deliberately, so it answers "did the selector find anything". But
   the wrong-element case is precisely "selector found something, and it is
   garbage": `to_number` on `"Add to cart"` yields null, and the field reports
   `matched`. Add a second, orthogonal status recorded alongside:
   `Coerced | CoercionFailed | NoTransforms`. A field whose coercion-failure rate
   jumps from 0% to 40% while its match rate stays at 100% is a wrong-element
   alarm with almost no other explanation.
2. **`each` container semantics.** `Rule::Each` yielding an empty array is
   currently indistinguishable from its container selector matching nothing —
   which conflates "the job board has no postings this week" with "the listing
   selector broke". Split it: `Empty { container_missing }` vs
   `Empty { container_matched_zero_items }`. The second, with `d_dom ≈ 0`, is the
   legitimate-empty case (§10.3) and must never quarantine a source.

### 2.7 Scoring and state transitions

Each enabled test yields `S ∈ [0,1]`. The degradation score is a weighted sum:

```
score = 0.30·S_missrate      (Wilson-separated rise in miss/error rate)
      + 0.20·S_distinct      (distinctness collapse vs baseline)
      + 0.20·S_invariant     (fraction of invariants violated, weighted by support)
      + 0.15·S_shape         (length/char-class TV distance, z-scored)
      + 0.15·S_divergence    (position in the (d_text,d_dom,d_val) plane)
```

with two overrides:

- **Gate:** `fetch_ok_rate < fetch_ok_floor` → verdict `inconclusive`, score not
  computed, baseline untouched, state unchanged.
- **Conclusive:** total collapse (§2.4) with a healthy fetch layer → `score = 1.0`
  regardless of the other terms.

Thresholds: `score ≥ degrade_score` (0.6) is a *tripped* run; `≥
quarantine_score` (0.85) is a *severe* run.

State machine, with hysteresis, because single-run anomalies are dominated by
transient causes:

```
healthy ──tripped──▶ suspect ──2 of last 3 tripped──▶ degraded
                        │                                │
                     1 ok run                     severe, or 2 more tripped
                        ▼                                ▼
                     healthy                        quarantined
                                                         │
                                    recovery_runs consecutive clean JUDGED runs
                                                         ▼
                                                    probation ──recovery_runs again──▶ healthy
                                                         │
                                              any tripped run
                                                         ▼
                                                    quarantined
```

**The up-path is implemented** (`[resilience] recovery_runs`, default 3). It was
not: `quarantined` used to be terminal without an operator, which on an
unattended box means a source that breaks at 03:00 and self-heals at 04:00 keeps
its writes diverted to `<ds>@q`, its pushes stopped and its revisions
unindexed until a person notices. That was the main reason `enforce = true` was
not adoptable.

Three properties make the release safe:

- **Only judged runs count.** `inconclusive`, `content_empty` and `below_cohort`
  are not evidence, so a source cannot heal on runs nobody looked at — in
  particular a source cannot shrink its cohort below the floor and quietly
  recover.
- **The streak is consecutive and per rung.** It is counted from the stored run
  rows since `state_since` (never a column that could drift out of sync), stops at
  the first tripped run, and each rung costs its own `recovery_runs` — so the full
  climb out of quarantine is two streaks.
- **Release is to `probation`, never straight to `healthy`.** Probation writes to
  the live dataset but stamps every record `provisional`, so a premature release
  is visible in the data and filtered out of the default `/changes` feed rather
  than silent. One tripped run in probation goes straight back to `quarantined`.

**The `<dataset>@q` shadow dataset is left exactly where it is on recovery** —
not merged, not renamed, not deleted. Its records were produced by an extractor
the system did not stand behind; merging them into the live dataset on recovery
would launder precisely the data quarantine exists to keep out, and renaming
would break the audit trail. It stays an ordinary dataset, so `GET
/datasets/{app}/{ds}@q`, `/changes`, `/export` and `duplicates` all keep working
on it, and an operator who wants that era back re-ingests it deliberately (the
§8.2 `reextract` path, when it exists).

`retired` is reached from any state when the fetch layer reports sustained
permanent failure (404/410/DNS on ≥ 90% of URLs across 3 runs) — a dead source,
not a broken extractor. Only runs with verdict `ok` update the rolling baseline;
`inconclusive` updates nothing; a `broken` run must never be absorbed into the
baseline it is being judged against.

### 2.8 What it costs at this scale

Per document: one extra tree walk (`dom_simhash`), plus O(fields) sketch
accumulation with a 64-hash minhash update per non-empty value. Both on the
existing rayon path, both allocation-free. Estimate: +10–20% of extraction CPU,
against a workload dominated by network I/O.

Per run: `1 + fields` row writes (`source_runs`, `field_sketches`) plus an upsert
per key into `doc_fingerprints`. The last one is O(keys) and is the only new
per-record write — batch it through the existing chunked-transaction pattern
(`UPSERT_CHUNK = 500` on one held connection), never per row. See the
"per-record transaction for a batch write" anti-pattern in
`harness-learnings.md`.

Evaluation itself is O(fields × window_runs) arithmetic on ≤ 20 cached rows —
microseconds. **Detection is free. Only repair costs money.**

---

## 3. The silent-corruption case — the real position

Everything green, values wrong. Here is the position, in three parts.

### 3.1 You cannot detect wrong-but-plausible values in general. Stop trying.

Deciding whether `$49.99` is the right price for this product requires knowing
the right price. Nothing on this machine knows it. Any design that claims
otherwise is either asking an LLM to hallucinate a judgement or assuming a label
source that does not exist.

So the design does not detect *wrongness*. It detects **the conditions under
which a selector silently rebinds**, which is a much narrower and entirely
checkable class. Three mechanisms, in descending order of yield:

**(a) Distinctness collapse — the dominant real failure.** After a redesign,
`.price` matches a template element that is identical on every page: a footer,
a nav item, a "Free shipping" banner. The values are plausible, the miss rate is
zero, and every record now carries the same value. `distinct_ratio` goes from
0.98 to 0.03 in one cohort. Fields that are *legitimately* constant already have
a baseline distinct-ratio near zero, so nothing fires on them. This one signal
catches, in my estimate, the majority of real silent rebinds, and its false-
positive rate on a genuinely per-record field is near zero because there is no
benign reason for a per-record field to become constant across 30 documents.

A collapse this severe overrides the weighted score to 1.0, so it is guarded: it
must be possible to **rule out** "the site legitimately started saying the same
thing everywhere". Normally that is a `content_changed` divergence — but
divergence needs a per-key fingerprint from the previous run, and a listing whose
keys rotate completely every run ("the 30 newest items") never has one, which
left exactly those sources unable to reach the guard that protects them. With no
divergence evidence the override now requires corroboration from the value domain
instead: the collapsed values must also have left the shape the field has always
produced. A same-shaped cohort-wide collapse on a key-rotating source is still
*scored* by the ordinary distinctness term — it just no longer convicts on its
own.

**(b) Value-domain drift.** A price field that becomes `"Add to cart"` moves the
char-class vector from 80% digit to 90% alpha. A date field that becomes a review
count moves the length histogram three buckets left and violates its mined
`^\d{4}-\d{2}-\d{2}$` invariant. Combined with the post-transform coercion
status (§2.6), this covers the case where the wrong element has the right
cardinality but the wrong *kind* of content.

**(c) Learned-invariant violation.** Type, range, regex class, pair ordering.
These are mined from the era we believed worked, over ≥ 500 records at ≥ 99%
confidence, so a violation in 20% of a fresh cohort is a genuine break in a
regularity the source has held for its entire history.

### 3.2 Golden documents: the only exact check, and it is cheap

> **Not built.** The `data/golden/` retention store and the pinned-document
> check do not exist. The consequence is stated honestly in §10.5: sources that
> never form a cohort (a single-URL `watch`, a handful of rows) have **no**
> detection at all today rather than golden-doc-only detection, and
> `GET /sources/{id}` reports `statistical_coverage: false` for them.

Statistics can be argued with. A pinned document cannot. For each source, keep
`golden_docs_per_source` (default 8) sampled documents: the **body**, copied out
of the per-job artifact dir into `data/golden/<source>/<key>.html` so a future
artifact GC cannot take it, plus the extracted values at pin time and the
`profile_version` that produced them.

Every N runs (or on any tripped run), re-fetch those exact URLs and extract. For
fields whose historical **churn rate** — the fraction of that field's revisions
in which it actually changed, derivable directly from `record_revisions.diff` —
is below 5%, the expected value is an **exact match**. For churnier fields, check
shape (type, length band, char class) instead. A mismatch on a stable field, with
a healthy fetch, is a deterministic break signal that needs no cohort, no
baseline, and no threshold. It is also the only detection available to
single-record sources (§2.4).

Golden docs are re-pinned only when a source is `healthy` and has been for
`golden_refresh_days` (default 30). That makes them an **anchor**: the rolling
baseline can be boiled-frogged by slow degradation, the anchor cannot (§10.4).

### 3.3 The residual, named honestly

A redesign in which the wrong element has the same cardinality, the same value
shape, the same distribution, and satisfies every mined invariant — the sale
price and the list price swapping places — is undetectable by this design, and I
do not believe it is detectable by any unattended design without an external
reference.

The answer is not detection. It is **recoverability**:

- every revision is stamped with the `profile_version` that produced it, so the
  affected era is exactly identifiable after the fact;
- `record_revisions` is append-only, so the pre-breakage values are still there;
- golden bodies and recent artifacts are retained, so
  `POST /sources/{id}/reextract` (§8) can replay a corrected ruleset over stored
  bodies and write corrected records as *new revisions* — never mutating history.

When someone eventually notices in three months, the fix is one API call and the
audit trail survives. That is the deliverable for this class, and pretending
otherwise would be the dishonest part of the design.

---

## 4. Where rules live: the profile registry (prerequisite for everything in §6)

> **Not built.** Rules remain a job parameter. This section is a prerequisite
> only for repair, which is also not built; detection keys on `(app, dataset)`
> and needs none of it. The cost is that `source_runs` cannot stamp a
> `profile_version`, so the `self_inflicted` diagnosis (§2.3) narrows the era by
> `build_id` alone.

Today a `RuleSet` is a **job parameter**. It has no identity, no version, and no
home — it arrives in `POST /apps/extractor/jobs` or sits inside a `schedules`
row's `params` blob. Repair is impossible against that: there is nothing to
promote, nothing to roll back to, and nothing to stamp a record with.

So: **rules become a first-class versioned entity, and job params reference them
by name.**

```jsonc
// before (still supported, see below)
{"urls": [...], "rules": {"title": {"type":"css","selector":"h1"}}}
// after
{"urls": [...], "profile": "acme-products"}
```

- `extraction_profiles` — one row per named profile, pointing at an
  `active_version`.
- `profile_versions` — immutable rows. A repair never edits a rule; it appends a
  version and (maybe) moves the pointer.

**Inline `rules` keep working and are explicitly unwarranted**: a job that passes
rules inline gets no health tracking beyond the coarse runtime signals and can
never be repaired, because there is nothing to write back to. `GET /sources`
reports `repairable: false, reason: "inline rules"`. This is the migration
incentive and it should be documented as such rather than deprecating the inline
form (it is the right shape for `POST /extract/preview` iteration).

---

## 5. Data model

Migration `0020_resilient_extraction.sql`. Timestamps are fixed-width RFC 3339
UTC micros per the enforced convention, so lexicographic comparison is
chronological.

```sql
-- ---------- versioned rule sets --------------------------------------------
CREATE TABLE IF NOT EXISTS extraction_profiles (
    name           TEXT PRIMARY KEY,
    app            TEXT NOT NULL,
    dataset        TEXT NOT NULL,
    active_version INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS profile_versions (
    profile        TEXT NOT NULL,
    version        INTEGER NOT NULL,
    rules          TEXT NOT NULL,        -- serialized RuleSet
    origin         TEXT NOT NULL,        -- 'human' | 'inversion' | 'claude' | 'rollback'
    parent_version INTEGER,
    evidence       TEXT,                 -- JSON: validation scores that justified it
    created_at     TEXT NOT NULL,
    PRIMARY KEY (profile, version)
);

-- ---------- the health unit -------------------------------------------------
CREATE TABLE IF NOT EXISTS sources (
    id                   TEXT PRIMARY KEY,   -- '<app>/<dataset>'
    app                  TEXT NOT NULL,
    dataset              TEXT NOT NULL,
    profile              TEXT,               -- NULL = not rule-backed
    state                TEXT NOT NULL DEFAULT 'healthy',
    degradation_score    REAL NOT NULL DEFAULT 0,
    state_since          TEXT NOT NULL,
    last_verdict_at      TEXT,
    tripped_of_last3     INTEGER NOT NULL DEFAULT 0,
    repair_blocked_until TEXT,
    promotions_30d       INTEGER NOT NULL DEFAULT 0,
    last_promotion_at    TEXT,
    updated_at           TEXT NOT NULL,
    CHECK (state IN ('healthy','suspect','degraded','quarantined','probation','retired'))
);

-- ---------- one row per (source, run) --------------------------------------
CREATE TABLE IF NOT EXISTS source_runs (
    source_id       TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    docs            INTEGER NOT NULL,
    fetch_ok_rate   REAL NOT NULL,
    d_text          REAL,               -- cohort median normalized drifts
    d_dom           REAL,
    d_val           REAL,
    verdict         TEXT NOT NULL,      -- ok|inconclusive|suspect|broken|self_inflicted|content_empty
    diagnosis       TEXT,               -- markup_drift|content_changed|field_loss|ab_variant|platform_change|...
    score           REAL NOT NULL DEFAULT 0,
    reasons         TEXT,               -- JSON array of {test, value, threshold}
    profile_version INTEGER,
    build_id        TEXT,               -- pumper build; disambiguates self-inflicted
    created_at      TEXT NOT NULL,
    PRIMARY KEY (source_id, job_id)
);
CREATE INDEX IF NOT EXISTS idx_source_runs_feed ON source_runs (source_id, created_at DESC);

-- ---------- per-field sketches (the baseline substrate) --------------------
CREATE TABLE IF NOT EXISTS field_sketches (
    source_id       TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    field           TEXT NOT NULL,
    n               INTEGER NOT NULL,
    matched         INTEGER NOT NULL,
    empty           INTEGER NOT NULL,
    error           INTEGER NOT NULL,
    coercion_failed INTEGER NOT NULL DEFAULT 0,
    len_sum         REAL NOT NULL,
    len_sumsq       REAL NOT NULL,
    len_hist        BLOB NOT NULL,      -- 16 × u16 LE
    cls             BLOB NOT NULL,      -- 4 × f32 LE
    distinct_ratio  REAL NOT NULL,
    minhash         BLOB NOT NULL,      -- 64 × u64 LE
    created_at      TEXT NOT NULL,
    PRIMARY KEY (source_id, job_id, field)
);

-- ---------- mined invariants -----------------------------------------------
CREATE TABLE IF NOT EXISTS field_invariants (
    source_id  TEXT NOT NULL,
    field      TEXT NOT NULL,
    kind       TEXT NOT NULL,           -- type|regex|range|nonnull|distinctness|pair_order
    spec       TEXT NOT NULL,           -- JSON
    support    INTEGER NOT NULL,
    confidence REAL NOT NULL,
    learned_at TEXT NOT NULL,
    PRIMARY KEY (source_id, field, kind)
);

-- ---------- per-key document fingerprints (previous-run comparison) --------
CREATE TABLE IF NOT EXISTS doc_fingerprints (
    source_id    TEXT NOT NULL,
    key          TEXT NOT NULL,
    text_simhash INTEGER NOT NULL,
    dom_simhash  INTEGER NOT NULL,
    seen_at      TEXT NOT NULL,
    PRIMARY KEY (source_id, key)
);

-- ---------- golden documents ------------------------------------------------
CREATE TABLE IF NOT EXISTS golden_docs (
    source_id       TEXT NOT NULL,
    key             TEXT NOT NULL,
    url             TEXT NOT NULL,
    body_path       TEXT NOT NULL,      -- data/golden/<source>/<sha>.html
    expected        TEXT NOT NULL,      -- JSON values at pin time
    stable_fields   TEXT NOT NULL,      -- JSON array: churn < 5%, exact-matched
    profile_version INTEGER,
    pinned_at       TEXT NOT NULL,
    PRIMARY KEY (source_id, key)
);

-- ---------- repair audit trail ---------------------------------------------
CREATE TABLE IF NOT EXISTS repair_attempts (
    id               TEXT PRIMARY KEY,
    source_id        TEXT NOT NULL,
    job_id           TEXT,
    diagnosis        TEXT NOT NULL,
    stage            TEXT NOT NULL,     -- inversion|claude|validating|shadow|promoted|rejected
    cost_usd         REAL NOT NULL DEFAULT 0,
    outcome          TEXT,              -- promoted|rejected|no_candidate|budget|inconclusive
    promoted_version INTEGER,
    created_at       TEXT NOT NULL,
    finished_at      TEXT
);
CREATE INDEX IF NOT EXISTS idx_repair_source ON repair_attempts (source_id, created_at DESC);

CREATE TABLE IF NOT EXISTS repair_candidates (
    attempt_id            TEXT NOT NULL,
    idx                   INTEGER NOT NULL,
    origin                TEXT NOT NULL, -- inversion|claude
    rules                 TEXT NOT NULL,
    holdout_match_rate    REAL,
    golden_exact          INTEGER,
    golden_total          INTEGER,
    invariant_violations  INTEGER,
    agreement_group       INTEGER,       -- candidates producing identical holdout output share a group
    lint                  TEXT,          -- JSON array of brittle-selector findings
    verdict               TEXT NOT NULL, -- accepted|rejected:<reason>
    PRIMARY KEY (attempt_id, idx)
);

-- ---------- trust stamping on existing tables ------------------------------
ALTER TABLE records           ADD COLUMN trust TEXT;            -- NULL = stable
ALTER TABLE record_revisions  ADD COLUMN trust TEXT;
ALTER TABLE record_revisions  ADD COLUMN profile_version INTEGER;
```

**On `records.trust` and the derived-column lesson.** `harness-learnings.md`
(2026-07-15) records that `0004_simhash.sql` added a derived column with a
`DEFAULT 0` sentinel and no backfill, silently disabling near-dup detection for
3,367 rows. That lesson is why `trust` is `NULL`-defaulted and `NULL` *means*
`stable`: it is a semantic default, not a sentinel, so pre-migration rows are
correct by construction and **no backfill is required**. Any code reading it must
treat `NULL` and `"stable"` as the same value; there is a helper for it and a
test asserting the equivalence.

---

## 6. Repair

> **Not built.** No candidate generation, no validation gates, no LLM call, no
> money spent. A degrading source is detected, quarantined and reported; fixing
> it is an operator action followed by `POST /sources/{id}/state`. §13's build
> order puts repair last for exactly this reason: steps 1-5 deliver most of the
> value at none of the risk, and if the evaluation numbers come back badly steps
> 6-8 should not land at all.

### 6.1 When repair is even attempted

All of these must hold:

1. Source state is `degraded` or `quarantined`.
2. Diagnosis is `markup_drift` or `field_loss` — **not** `self_inflicted`
   (→ rollback), **not** `content_changed`, **not** `ab_variant` without cluster
   coverage (§10.2), **not** `platform_change` (§10.8).
3. Source is profile-backed (§4). Inline-rules and hardcoded-Rust sources stop
   here and alert.
4. A validation corpus exists: ≥ `holdout_min_docs` (default 20) retained bodies
   for this source, of which ≥ 3 are golden docs with known expected values.
5. `now > repair_blocked_until`, `promotions_30d < max_promotions_30d`, and the
   daily repair budget has headroom.

Repair runs as a dedicated app, `repair`, so **every dollar it spends is
attributable by construction** through the existing `cost_events` ledger
(`AppContext::research` is the metered seam; nothing here touches
`ctx.engines.*` directly). Idempotency key `repair:{source_id}:{diagnosis_hash}`
gives at-most-one in-flight attempt per source per diagnosis, reusing the
existing `jobs.idempotency_key` mechanism.

### 6.2 Escalate by cost — try the free path first

This mirrors the fetcher's own philosophy: climb tiers only when the cheap one
loses.

**Tier 0 — value→selector inversion (free, deterministic).** We know the old
correct values: they are in `record_revisions`. For each broken field, search the
*new* markup for the old value string; for every element whose text (or attribute)
equals it, derive a candidate selector by walking up the tree and emitting the
shortest stable path — preferring semantic anchors (`id`, `itemprop`,
`data-*`, `aria-label`, non-hashed class tokens) over positional ones. Do this
across ≥ 5 documents and keep only selectors that work on **all** of them; that
cross-document intersection is what turns a per-page hack into a rule.

For the most common real redesign — the DOM keeps the same text but the class
names changed — this finds the answer for zero dollars and zero latency. My
expectation is that it resolves the majority of `markup_drift` cases; §12.3
makes that expectation falsifiable rather than an article of faith.

**Tier 1 — Claude proposal (paid).** Only when inversion yields nothing that
survives validation.

### 6.3 What the LLM is allowed to be

**A generator of candidates inside a constrained space. Never a judge.**

The prompt gets: the failing field names, the current (failing) rule, the *old
correct values* for those fields on specific documents, and a truncated window of
the new markup around plausible matches. Output is constrained by the existing
`ResearchRequest.json_schema` to a `RuleSet` fragment — so a malformed proposal
is a parse failure, not a surprise.

Crucially, the task posed is not "extract the price" (a judgement) but **"find
the rule that produces *these known values* from *this markup*"** — a search
problem with an answer key. That reframing is what makes the LLM's output
checkable by deterministic code, and it is only possible because revision history
exists.

`candidates = 3` independent runs (no `resume_session`, so the research cache
applies and a repeated identical attempt is free), each shown a **different pair
of exemplar documents**. Varying the exemplars across candidates is the primary
structural defence against overfitting: three candidates that overfit to three
different pages will disagree on the held-out set and be rejected by §6.4.5.

The model never sees the held-out documents, never sees the golden set, never
sees the invariants, and is never asked whether its own proposal is good.

### 6.4 Validation — the whole point

A candidate must pass every gate. All gates are deterministic code.

1. **Compiles.** `RuleSet::compile()` — already exists, catches bad CSS/regex/
   XPath at zero cost. Per the `POST /extract/preview` precedent, report all
   field errors at once.
2. **Brittle-selector lint.** Reject: selectors matching > 5% of document
   elements; `body`/`html`/`*`/bare `div`; `:nth-child` chains deeper than 3;
   any class token matching `^[A-Za-z]+[-_][0-9a-f]{6,}$` (build hashes churn on
   every deploy); `const` rules proposed for a previously-dynamic field (an LLM's
   favourite way to make a test pass).
3. **Held-out match rate.** The candidate is scored on the ≥ 20 documents it was
   never shown. Require `holdout_match_rate ≥ 0.9` **and** `≥ baseline_healthy_rate
   − 0.05`. Train/test separation is enforced structurally, by the corpus
   splitter, not by asking the model to behave.
4. **Golden-set check.** On the pinned documents, re-fetched now: **exact value
   equality** for `stable_fields` (historical churn < 5%), **shape equality**
   (type, length band, char class) for churnier ones. Any exact-field mismatch is
   an immediate reject.
5. **Output agreement, not proposal agreement.** Group candidates by the
   *values* they produce on the held-out set. Require an agreement group of size
   ≥ `agreement_min` (default 2). Two different selectors arriving at the same
   values is *stronger* evidence than two identical selectors — it means two
   independent searches found the same element. If all three candidates disagree,
   there is no repair; the source stays quarantined and alerts.
6. **Invariant re-check.** Candidate output must satisfy the source's mined
   invariants (§2.5) — including the distinctness invariant, which is what stops a
   "repair" that binds to a site-wide constant and scores 100% match rate.
7. **No-regression.** The candidate must not degrade any field that was healthy:
   for every non-broken field, its holdout match rate must be within 2 points of
   the live rule's.

Everything about the attempt — every candidate, every score, every reject reason
— lands in `repair_candidates`, so a rejected repair is as auditable as a
promoted one.

---

## 7. Trust, quarantine, and what consumers see

### 7.1 The lifecycle of a source that is degrading but not dead

| State | Writes | Trust stamp | Pushes (watches/triggers) | Search index | `/changes` | Repair |
|---|---|---|---|---|---|---|
| `healthy` | live dataset | `NULL` (stable) | fire | index | included | n/a |
| `suspect` | live dataset | `NULL` | fire | index | included | no |
| `degraded` | live dataset, **`upsert_many` only** | `provisional` | **suppressed** | **skipped** | filtered out by default | scheduled |
| `quarantined` | **shadow dataset `<ds>@q`** | `quarantined` | **suppressed** | **skipped** | not in live feed | active |
| `probation` | live dataset | `provisional` | fire (payload carries `trust`) | index | filtered out by default (`provisional`) | shadow compare |
| `retired` | none | — | — | — | — | no |

`suspect` deliberately changes nothing downstream. A single tripped run is
dominated by transient causes, and a system that quarantines on one bad run on an
unattended box will spend its life quarantining.

`quarantined` writes to `<dataset>@q` in the same app namespace. Using the
existing dataset mechanism rather than a new table means every tool already works
on it: `GET /datasets/{app}/{ds}@q`, `/changes`, `/export`, `duplicates`. When a
repair is promoted, the quarantine dataset is *not* merged automatically —
`POST /sources/{id}/reextract` replays the retained bodies through the new rules
into the live dataset, which is the same code path and produces proper revisions.

### 7.2 The three things that must never happen

1. **A degrading source must never tombstone its own dataset.** `sync_many` is
   downgraded to `upsert_many` in `degraded`/`quarantined`. `detect_removed`
   already no-ops on an empty batch; a *partial* batch is the dangerous case and
   this is the guard for it.

   The check lives **in the store**, as a precondition of removal detection:
   `Datasets::detect_removed` takes a `RemovalGuard`, and the only public way to
   mint one is `RemovalGuard::for_source_state(state)`, which returns `None` for
   a degrading source. It used to be a check inside `AppContext::sync_many` —
   one layer above — which is the "guard-at-the-wrong-seam" anti-pattern one step
   removed: it covered every caller that *went through* `sync_many`, and the
   `peer` app (hand-rolling upsert + `detect_removed`) simply did not. A token
   the store demands cannot be walked around, and
   `crates/core/tests/removal_guard.rs` holds the EXPECTED inventory of call
   sites. A caller that already knows which records disappeared uses
   `Datasets::tombstone_keys` — removal by name, no inference, no guard needed.
2. **A degrading source must never push.** Watches (`dataset.changed`), triggers
   (`fresh`/`changed`/`removed`), and saved-search alerts all fire from the
   worker's post-run hooks. Health evaluation runs **before** those hooks and they
   consult `sources.state`. This ordering is the entire enforcement mechanism; if
   it is wrong, the design does nothing.
3. **A degrading source must never poison the search index.** `index_datasets`
   handling skips a suppressed source. Because indexing is now delta-driven from
   the change feed, the skipped revisions are simply picked up by the next healthy
   run's window — or by `search-backfill` after a reextract.

### 7.3 Pull vs push, stated as a rule

Pushes suppress; pulls filter but stay inspectable. `GET /changes` defaults to
`trust=stable` and accepts `?trust=all|provisional|quarantined`. `GET /datasets/...`
returns records with their `trust` field populated and accepts the same filter.
`GET /events` (SSE) is unfiltered — it is operational telemetry about jobs, not
data delivery. A consumer that wants everything can always ask; a consumer that
asks for nothing gets only what we stand behind.

New webhook kinds, all through `webhook::dispatch_event` per the enforced
convention (never hand-roll a send): `source.degraded`, `source.quarantined`,
`source.repair_promoted`, `source.rolled_back`, `source.retired`, delivered to
`[webhooks] health_url` with `health_secret`. These are the alerts that replace
the human reviewer: nobody approves anything, but somebody is told.

---

## 8. Promotion, rollback, and the API

### 8.1 Promotion

> **Not built** (§8.1-8.3). There is nothing to promote or roll back without the
> profile registry and repair. `POST /sources/{id}/state` is the whole operator
> surface. It is no longer the *only* way out of `quarantined` — §2.7's
> evidence-based recovery is — but it is the shortcut for an operator who already
> knows the source is fixed.

`[resilience.repair] mode`:

- `off` — detect, quarantine, alert. Candidates are still generated and stored on
  an explicit `POST /sources/{id}/repair?dry_run=true`, never promoted.
- `shadow` — **the default.** A validated candidate is registered as a shadow
  version and runs *alongside* the live rules for `probation_runs` (default 3) or
  200 documents, whichever comes first. Both rule sets are extracted over the same
  batch — this is exactly what a multi-core extraction engine is for, and the
  second pass is free relative to the fetch that produced the documents. Only the
  live rules write. Auto-promotion happens when, across the shadow window, the
  candidate clears all §6.4 gates *every run* and the live rules fail at least one
  *every run*. Anything less decisive → no promotion, stay quarantined, alert.
- `on` — promote immediately on validation, then enter `probation`. For operators
  who accept the risk on a specific source; settable per profile.

Promotion writes a new `profile_versions` row (`origin='claude'|'inversion'`,
`parent_version` set, `evidence` = the full score sheet), moves
`extraction_profiles.active_version`, sets the source to `probation`, and emits
`source.repair_promoted`. **Nothing is edited; everything is appended.**

### 8.2 Rollback

- **Automatic:** any `probation` run that trips reverts `active_version` to
  `parent_version` instantly, sets state `quarantined`, sets
  `repair_blocked_until = now + repair_cooldown_secs`, emits
  `source.rolled_back`.
- **Manual:** `POST /profiles/extraction/{name}/rollback {to: <version>}`.
- **Three days later:** the records written by a bad version are exactly
  identifiable — `record_revisions.profile_version` narrows the era, and
  `record_revisions` is append-only so the pre-bad values are intact.
  `POST /sources/{id}/reextract {from_version|since}` replays the retained bodies
  through the current active version and upserts the results, producing *new*
  revisions. History is never rewritten; the correction is itself a change with a
  diff. This is the recovery story promised in §3.3, and it is the reason the
  golden/recent-body retention (§5, `data/golden/`) is not optional.

### 8.3 Anti-oscillation budget

Beyond the cooldown: `max_promotions_30d` (default 2) per source. A source that
exhausts it is pinned in `quarantined` with `reason: "repair budget exhausted"`
and keeps alerting. A stuck source is a bad outcome; a source oscillating between
two wrong rules while pushing garbage downstream is a worse one.

### 8.4 Endpoints

Registered through `openapi_router()` with `#[utoipa::path]` annotations **and**
an entry in the coverage test's `EXPECTED` inventory, or `cargo test -p
pumper-server` fails. Paginated lists follow the `cursor=` keyset convention.

```
GET    /sources                       # health table; ?state=&app=&cursor=
GET    /sources/{id}                  # state, score, last runs, per-field sketch vs baseline,
                                      #   invariants, active profile version, repairable+reason
GET    /sources/{id}/runs?cursor=     # verdict history with reasons
POST   /sources/{id}/state            # {state, reason} — manual override / unquarantine / retire
GET    /sources/{id}/golden           # pinned docs + expected values
POST   /sources/{id}/golden/pin       # {keys?} — pin now (else auto-sampled)
DELETE /sources/{id}/golden/{key}
POST   /sources/{id}/repair           # force an attempt; ?dry_run=true → candidates + scores only
POST   /sources/{id}/reextract        # {from_version?|since?} replay retained bodies

GET    /profiles/extraction                        # list, ?app=&cursor=
POST   /profiles/extraction                        # {name, app, dataset, rules}
GET    /profiles/extraction/{name}/versions
POST   /profiles/extraction/{name}/promote         # {version}
POST   /profiles/extraction/{name}/rollback        # {to}

GET    /repairs?source=&outcome=&cursor=           # attempt log incl. cost
GET    /repairs/{id}                               # candidates, scores, reject reasons
```

Metrics (cardinality-capped — per-field series limited to the worst 10 fields per
source, because `/metrics` is scraped and label explosion is a real cost):

```
pumper_source_state{app,dataset,state}                 gauge 0/1
pumper_source_degradation_score{app,dataset}           gauge
pumper_source_field_miss_rate{app,dataset,field}       gauge  (top-10 only)
pumper_repair_attempts_total{outcome}                  counter
pumper_repair_cost_usd_total                           counter
pumper_source_runs_total{verdict}                      counter
```

`/catalog/health` answers "did this source run recently". `/sources` answers "was
what it produced right". Both should link to each other in their responses; they
are the two halves of source liveness and neither subsumes the other.

---

## 9. Config surface

```toml
[resilience]
enabled                 = true
enforce                 = false    # false = compute verdicts, gate nothing (soak mode, §12.6)
min_cohort_docs         = 30
window_runs             = 20
degrade_score           = 0.6
quarantine_score        = 0.85
fetch_ok_floor          = 0.7
mad_z                   = 3.5
invariant_refresh_days  = 14
invariant_min_support   = 500
invariant_min_confidence= 0.99
golden_docs_per_source  = 8
golden_refresh_days     = 30
golden_check_every_runs = 5
sketch_retention_runs   = 60       # window_runs × 3; pruned by the retention janitor
recovery_runs           = 3        # consecutive clean JUDGED runs per rung back up (§2.7)
platform_change_ratio   = 0.5      # >half of sources tripping at once ⇒ it's us, §10.8

[resilience.repair]
mode                    = "shadow" # off | shadow | on
budget_usd_per_day      = 2.00
attempt_budget_usd      = 0.25
candidates              = 3
role                    = "research"   # a [claude.roles] preset
cooldown_secs           = 86400
promote_cooldown_secs   = 604800
max_promotions_30d      = 2
holdout_match_min       = 0.9
holdout_min_docs        = 20
agreement_min           = 2
probation_runs          = 3
```

Config structs use `#[serde(default)]` plus a manual `Default` impl (both are
required — see the `ClaudeConfig` note in `harness-learnings.md`), and
`Config::validate()` rejects semantically-broken combinations:
`degrade_score < quarantine_score`, `agreement_min ≤ candidates`,
`min_cohort_docs ≥ 5`, `holdout_min_docs ≥ 5`, `fetch_ok_floor ∈ (0,1]`,
`recovery_runs > 0` (zero would let a quarantined source release itself on the
first run that merely failed to trip).
`enabled = false` is a complete no-op; `enforce = false` computes everything and
gates nothing, which is the shipping default (§12.6).

### 9.1 Cost governance, summarised

- **Detection is free** — arithmetic over data already computed (§2.8).
- **Inversion is free** — string search over retained bodies (§6.2).
- **Only Claude proposals cost**, and they are bounded five ways: per-attempt job
  budget (existing `jobs.budget_usd` enforcement via `SpentTotal`), per-day global
  budget checked against `CostLedger::summary(app="repair", since=midnight)`,
  per-source cooldown, per-source 30-day promotion cap, and single-flight
  concurrency via the idempotency key. Repeated identical attempts are served free
  by the existing research cache.
- Worst case per source per day: 1 attempt × 3 candidates × `attempt_budget_usd`,
  hard-capped by the daily ceiling at $2.00 across the whole fleet. If the daily
  budget is exhausted, attempts are not queued — they are *declined* with outcome
  `budget`, because a queue of stale repairs against a site that has since changed
  again is worse than no repair.

---

## 10. Failure modes of this design

**10.1 Poisoned repair.** A candidate that binds to attacker-controlled text.
Mitigations: brittle-selector lint, invariant re-check (a promoted rule must
still produce values in the historical type/range/regex class), output agreement
across independently-prompted candidates, golden-set exact match, and shadow
before promotion. **Residual:** if the site itself is compromised, extraction is
*correct* and the data is wrong. Out of scope; no extraction-layer design can
distinguish that from a legitimate content change.

**10.2 A site that A/B tests its markup.** Two variants served per request. The
held-out match rate sits near 50% and never clears 0.9, so repair correctly
refuses — but the source then sits quarantined forever, which is a failure mode
of its own. Handling: detect **bimodality in `dom_simhash` within a cohort** (a
cheap 2-means on 64-bit Hamming distance; two clusters with inter-cluster
distance ≫ intra-cluster). Diagnosis becomes `ab_variant`, and the repair goal
changes: candidates are scored **per cluster** and accepted only if they clear
threshold on *both* — which usually means a union selector (`.price, .price-v2`),
a shape the existing CSS rule already supports. If no candidate covers both
clusters, the source stays quarantined and alerts, which is the honest outcome.

**10.3 A source that legitimately goes empty for a week.** A job board with no
postings. Handled by the `each`-container split (§2.6): container matched, zero
items, `d_dom ≈ 0`, fetch healthy → verdict `content_empty`, not `broken`. The
source stays `healthy` and its baseline is *not* updated from the empty run (so a
quiet week doesn't re-baseline the source into thinking zero is normal). If
emptiness persists past `window_runs`, it escalates to an informational
`source.quiet` alert rather than a quarantine.

**10.4 Baseline poisoning (boiling frog).** Slow degradation is absorbed by a
rolling baseline that moves with it. Two defences: (a) only `ok` runs update the
baseline, so once a source trips, its baseline freezes; (b) the golden docs are
an **anchor** re-pinned only from healthy states after 30 days, so anchor drift is
checked against a fixed reference regardless of what the rolling window believes.
This is the one failure mode I consider genuinely incompletely solved — a
degradation slower than 30 days per step could in principle walk the anchor too.
§12.1's mutation harness includes a gradual-drift class specifically to measure
that.

**10.5 Small sources never form a cohort.** `watch` on one URL, `cms-fee-schedule`
on a handful of rows. Golden-doc detection is not built, so today they are watched
by the assumption-free rules only (total collapse, the fetch gate). Their runs are
recorded `below_cohort` with `cohort: chronic`, they contribute nothing to a
baseline, and `GET /sources` reports them `monitored: false` and counts them in
`unmonitored`. Honest cost, surfaced in the API rather than hidden.

**10.6 Artifact retention.** Golden bodies live in `data/golden/`, outside the
per-job artifact dirs, precisely so that artifact retention
(`[storage] artifact_retention_days`, which walks only `data/artifacts/`) cannot
take them. Recent bodies for the holdout corpus are best-effort: if fewer than
`holdout_min_docs` survive, repair declines with outcome `no_corpus` rather than
validating against three pages.

**10.7 Write amplification.** ~2 MB/day of sketches at current fleet size, pruned
by the existing retention janitor (a `prune_sketches` sibling of
`prune_revisions`, keeping `sketch_retention_runs`). `doc_fingerprints` is one row
per key per source — same order as `records`, written in 500-row chunked
transactions on one held connection.

**10.8 We break ourselves and blame the sites.** A pumper upgrade that changes
`to_number`, the markdown converter, or a selector library moves every numeric
field on every source in the same epoch. Defence: if more than
`platform_change_ratio` (0.5) of *independent* sources trip in the same epoch,
the diagnosis is `platform_change`, **no repairs are dispatched fleet-wide**, and
a single `source.platform_change` alert fires. Every run row stamps `build_id`,
so the correlation with a deploy is one query. This matters more on an unattended
single machine than almost anything else here: without it, one bad release
triggers thirty simultaneous paid repairs against thirty innocent websites.

**10.9 The detector itself silently breaks.** The exact failure this subsystem
exists to prevent, applied to itself. Defence: a **canary source** — a `selftest`
app serving a fixture page from disk on a schedule, with a rule that is
deliberately broken on a rotation. It must produce a `broken` verdict every
cycle; if it stops, `pumper_source_runs_total{verdict="broken"}` for the canary
flatlines and an alert fires. Cheap, and it is the only thing standing between
this design and becoming the problem it was built to solve.

**10.10 Repair succeeds on the wrong field.** A candidate that fixes `price` by
binding it to `list_price` — every gate passes (values are plausible, distinct,
in range, correctly typed) except that it is the wrong number. The golden set
catches it *if* `price` is a stable field on the pinned docs. If it is churny, it
is not caught. This is the §3.3 residual reappearing inside repair, and it is the
strongest argument for `mode = "shadow"` being the default: shadow requires the
live rule to fail *and* the candidate to pass every run for three runs, which at
least prevents a coin-flip promotion.

---

## 11. Non-goals

- **No human review loop, and no UI.** By constraint. Every authorization is code.
- **No LLM-as-judge, anywhere in an accept path.** The Claude engine generates
  candidates inside a schema-constrained space; deterministic code decides.
- **No general "is this value correct" semantics.** §3.1.
- **No repair of hardcoded-Rust apps.** Detection, quarantine and alerting only.
  This system does not edit and rebuild its own source unattended.
- **No fetch/anti-bot work.** The governor, tier memory and host profiles own
  that; this subsystem *consumes* their verdicts and never second-guesses them.
- **No NL→RuleSet authoring or schema-less extraction** (separate backlog
  moonshots) — though the profile registry in §4 is the substrate they would need.
- **No cross-source semantic validation** (comparing two sites' prices for the
  same product). Interesting, out of scope, and a different subsystem.
- **No distributed anything.** Single machine, single SQLite writer, WAL,
  `busy_timeout` 5s, ≤ 8 connections — as today.
- **Not a freshness monitor.** `/catalog/health` already answers "did it run".

---

## 12. Evaluation plan — proving a design that detects the invisible

> **Not built.** The `resilience-eval` mutation harness, the historical
> backtest and the canary source do not exist, so the recall and
> false-positive-rate numbers below are **targets, not measurements**. Nothing
> in this document reports an observed FPR. That is precisely why `enforce`
> ships `false`: §12.6's soak is the only evidence currently available, and it
> accrues in `source_runs` as the fleet runs. Treat every threshold in §9 as a
> starting guess.

The thing this detects is by definition unobserved, so ground truth has to be
*manufactured*. Six measurements, each with a number that would falsify part of
the design.

### 12.1 Mutation testing on retained corpora (the primary experiment)

Build `--bin resilience-eval`. Take each source's retained bodies (crawl
artifacts, golden docs, `data/artifacts/`), apply **synthetic markup mutations**
drawn from a taxonomy of real breakages, and run the full detector over the
mutated corpus:

| Mutation class | Simulates | Expectation |
|---|---|---|
| class rename | CSS refactor | detect (`markup_drift`), repairable by inversion |
| build-hash class churn | webpack/Tailwind rebuild | **must NOT fire** (folded by `dom_simhash`) |
| tag change (`<span>`→`<div>`) | template rewrite | detect |
| wrapper insertion | layout change | detect if selectors were positional |
| sibling swap | **silent corruption** (two prices swap) | partial — measure honestly |
| duplicate-node insertion | **silent corruption** (selector binds to template) | detect via distinctness |
| attribute→text move | markup modernisation | detect |
| node deletion | field removed from site | detect (`field_loss`) |
| text-only change | **negative control** — genuine content change | **must NOT fire** |
| gradual drift (5%/run for 20 runs) | boiling frog (§10.4) | detect before run 20 |
| no mutation | **negative control** | **must NOT fire** |

Measured: **recall per class** and **false-positive rate on the two negative
controls**, at realistic cohort sizes (5/30/200 documents).

Targets and falsifiers:
- Hard-break recall (rename, tag, deletion, wrapper) **≥ 0.90** at cohort ≥ 30.
- Silent-corruption recall (duplicate-node, sibling-swap) **≥ 0.50**. *If this is
  below 0.5 at an acceptable FPR, §3's distinctness/invariant thesis is wrong and
  the design should be cut back to hard-break detection plus golden docs only* —
  which is still worth shipping, but the trust/quarantine machinery should then
  be far less aggressive.
- False-positive rate on negative controls **≤ 0.3% per run** — i.e. at most
  ~1 spurious quarantine per source-year on a daily source. On an unattended box a
  false quarantine that silently stops a working pipeline is worse than a
  detection that arrives a week late, so **FPR is the binding constraint and
  recall is tuned against it**, not the other way round.

### 12.2 Historical backtest

Replay the detector over the existing `record_revisions` history for all live
sources (5,196 records at last count) and every run of every app in `jobs`.
Outcomes and what they mean:

- Fires on nothing → either the fleet has genuinely been healthy, or the detector
  is inert. Disambiguated by §12.1: an inert detector fails the mutation harness.
- Fires on some runs → each is a *candidate past incident*. Hand-check a random
  sample of 50 against the stored diffs and report a precision estimate with a
  Wilson interval. Precision below ~0.5 on the backtest means the thresholds are
  tuned too hot for real data regardless of what the synthetic harness says.

This is also the cheapest way to discover the fleet's *natural* run-to-run
variance, which is what the MAD baselines will be made of.

### 12.3 Repair evaluation, blind by construction

Using the mutated corpora from §12.1 — where the pre-mutation values are known
exactly — measure:

- **Inversion hit rate** per mutation class. The design claims Tier 0 resolves
  most class-rename cases for free; if it resolves < 40%, the cost model in §9.1
  is wrong and the Claude budget must go up.
- **Proposal acceptance rate** (how often any candidate clears all seven gates).
- **The number that matters: of *promoted* repairs, the fraction that reproduce
  the pre-mutation values on a blind set excluded from training, holdout, and
  golden.** Target **≥ 0.95**. *If promoted-but-wrong exceeds 5%, auto-promotion
  is not safe on this evidence and `mode` must default to `off` — repair becomes
  "propose, validate, store, alert", and a promotion requires an explicit API
  call.* That is the design's most important falsifier, because an unattended
  wrong promotion silently corrupts a dataset with full system confidence.
- **Cost per successful repair**, in dollars and in wall-clock.

### 12.4 Detection latency

From the mutation harness at realistic per-source cadences and cohort sizes:
runs-to-detection and wall-clock-to-detection, per source. Publish the table; a
source whose latency exceeds its own cadence × `window_runs` is effectively
unmonitored and should be told so via `statistical_coverage: false`.

### 12.5 Oscillation and stability

Simulate an A/B source (alternating markup per fetch) for 30 runs. Assert: zero
promotions, exactly one transition into `quarantined`, no flapping between states,
`ab_variant` diagnosis emitted. Separately, simulate a repaired-then-regressed
source and assert the rollback fires on the first tripped probation run and the
cooldown blocks the immediate retry.

### 12.6 Production soak — the rollout gate

Ship with `[resilience] enforce = false` and `repair.mode = "shadow"`. The
detector computes and stores every verdict; **nothing is gated, nothing is
suppressed, nothing is promoted.** Run for four weeks or ~100 runs per source,
then read `source_runs`:

- Observed FPR (verdicts of `broken` on sources that a spot-check shows were
  fine) must meet the §12.1 target before `enforce = true`.
- Shadow candidates that *would* have been promoted are compared by hand for the
  first five before `mode` is left at `shadow` in earnest.

Enforcement flips on per-source, not fleet-wide, starting with the highest-volume
rule-backed sources where cohorts are large and the statistics are strongest.

### 12.7 Standing regression

The §10.9 canary, plus mutation-harness runs in CI over a fixed fixture corpus so
that a change to `dom_simhash`, the sketch, or the thresholds shows up as a
recall/FPR delta in a test, not as silence in production.

---

## 13. Build order

1. **Prerequisites** — post-transform coercion status; `each` container-empty
   split; `dom_simhash`; `doc_fingerprints`. All small, all independently useful,
   none of them gate anything.
2. **Profile registry** (§4) — `extraction_profiles`/`profile_versions`, the
   `profile` param on `extractor`/`plugin`, the CRUD endpoints. Nothing downstream
   is possible without it.
3. **Sketches + persistence** — `field_sketches`, `source_runs`, the worker
   post-run evaluation seam, `GET /sources`. Ship with `enforce = false`.
4. **Invariant mining + golden docs** — the silent-corruption layer and the
   `data/golden/` retention store.
5. **`resilience-eval` harness** (§12.1) — *before* enforcement, because the
   thresholds are its output, not an input.
6. **Enforcement** — trust stamping, `sync_many` downgrade, push suppression,
   quarantine datasets, health webhooks.
7. **Repair Tier 0** (inversion) with `mode = "shadow"`, dry-run endpoint first.
8. **Repair Tier 1** (Claude) + promotion/rollback + the cost governor.
9. **Canary source** and the CI mutation regression.

Steps 1–5 deliver most of the value (you find out your sources are rotting) at
none of the risk (nothing is gated, nothing is promoted, nothing is spent). If
step 8 never lands, the subsystem is still worth having; if step 5's numbers come
back badly, steps 6–8 should not land at all.
