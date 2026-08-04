//! The verdict: turning one run's sketches, drifts and invariant checks into a
//! degradation score, a diagnosis and a state transition.
//!
//! Everything here is a pure function of data the runtime already produced. No
//! I/O, no clock, no model — so the whole decision is reproducible from a stored
//! `source_runs` row, and the thresholds can be swept offline against a fixture
//! corpus.
//!
//! Two rules override the score, and both exist because a score computed on bad
//! evidence is worse than no score:
//!
//! - **The fetch gate.** A run whose fetch layer is unhealthy produces no
//!   extraction verdict at all. You cannot judge an extractor on documents you
//!   did not receive, and a half-delivered run is exactly what makes a healthy
//!   extractor look broken.
//! - **Total collapse.** A field that went from ~always matching to ~never
//!   matching, on a healthy fetch, needs no distribution: under the baseline rate
//!   that outcome has probability under 10^-3. It should not wait for a cohort.

use std::collections::BTreeMap;

use crate::config::ResilienceConfig;

use super::sketch::{
    self, median, median_distribution, robust_z, FieldSketch, CLS, MIN_BASELINE_RUNS,
};
use super::{CohortDrift, Diagnosis, RunVerdict, SourceState};

/// Weights of the five score terms. They sum to 1, so the score is directly
/// comparable to the `degrade_score` / `quarantine_score` thresholds.
const W_MISSRATE: f64 = 0.30;
const W_DISTINCT: f64 = 0.20;
const W_INVARIANT: f64 = 0.20;
const W_SHAPE: f64 = 0.15;
const W_DIVERGENCE: f64 = 0.15;

/// A field whose baseline distinctness is below this is legitimately repetitive
/// (a category, a currency), so its collapse carries no information.
const PER_RECORD_DISTINCT: f64 = 0.5;

/// Tolerance for "unchanged" on a ratio whose baseline has never varied.
const RATIO_TOL: f64 = 0.02;

/// Tolerance for "unchanged" on a *distribution* distance whose baseline has
/// never varied — much looser than the ratio tolerance, and deliberately so.
///
/// A source whose baseline runs happen to produce byte-identical length and
/// character-class profiles has zero scale, and with the ratio tolerance a total
/// variation of 0.03 (one value in thirty a character longer) would read as
/// infinitely significant. A TV distance under 0.10 is not a value-domain change
/// under any reading, so this is where the floor belongs.
const SHAPE_TOL: f64 = 0.10;

/// Documents a run needs before the total-collapse rule may fire. Below five,
/// "every document missed" is an unremarkable outcome.
const COLLAPSE_MIN_DOCS: u32 = 5;
/// Run miss rate at or above which a field has collapsed.
const COLLAPSE_RUN_RATE: f64 = 0.9;
/// Baseline miss rate at or below which the field was previously reliable.
const COLLAPSE_BASE_RATE: f64 = 0.1;

/// One field's healthy history — the sketches of the last `window_runs` runs
/// this source was judged `ok`, newest first.
#[derive(Debug, Default, Clone)]
pub struct Baseline {
    pub fields: BTreeMap<String, Vec<FieldSketch>>,
}

impl Baseline {
    /// Runs in the window for a field.
    pub fn runs(&self, field: &str) -> usize {
        self.fields.get(field).map_or(0, Vec::len)
    }

    /// Pooled `(misses, documents)` across the window — the reference a run's
    /// miss rate is separated against.
    pub fn pooled_misses(&self, field: &str) -> (u32, u32) {
        self.fold(field, |s| (s.misses(), s.n))
    }

    /// Pooled `(coercion failures, coercible documents)` across the window.
    pub fn pooled_coercion(&self, field: &str) -> (u32, u32) {
        self.fold(field, |s| {
            (s.coercion_failed, s.coerced + s.coercion_failed)
        })
    }

    fn fold(&self, field: &str, f: impl Fn(&FieldSketch) -> (u32, u32)) -> (u32, u32) {
        self.fields.get(field).map_or((0, 0), |runs| {
            runs.iter().fold((0, 0), |(a, b), s| {
                let (x, y) = f(s);
                (a + x, b + y)
            })
        })
    }

    /// A per-run series of some scalar, for the robust-z comparisons.
    pub fn series(&self, field: &str, f: impl Fn(&FieldSketch) -> f64) -> Vec<f64> {
        self.fields
            .get(field)
            .map_or_else(Vec::new, |runs| runs.iter().map(f).collect())
    }

    /// Whether this field has enough history for a distributional test.
    fn distributional(&self, field: &str) -> bool {
        self.runs(field) >= MIN_BASELINE_RUNS
    }

    /// The largest cohort any field in this window was sketched over — "how big
    /// does this source's listing actually get". The maximum rather than the
    /// median because the question is whether the source has *ever demonstrated*
    /// it can reach the floor, and one demonstration settles it.
    pub fn peak_docs(&self) -> u32 {
        self.fields
            .values()
            .flat_map(|runs| runs.iter().map(|s| s.n))
            .max()
            .unwrap_or(0)
    }
}

/// Whether a run's cohort was big enough to be judged, and — when it was not —
/// what kind of small it was. The distinction is per source, which is the point:
/// `min_cohort_docs` is one fleet-wide constant, and "12 documents" means
/// something entirely different on a source that has never produced more than 14
/// than on one that produced 4,000 yesterday.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CohortAdequacy {
    /// At or above the floor: every distributional test applies.
    Full,
    /// Below the floor, but this source has cleared it before — today's run
    /// shrank. Not judged, and worth looking at: the listing got smaller.
    Shrunken,
    /// Below the floor and it has never been above it in the whole baseline
    /// window. The source is structurally too small to be judged statistically —
    /// it is **unmonitored**, and `GET /sources` says so instead of showing it as
    /// healthy.
    Chronic,
}

impl CohortAdequacy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Shrunken => "shrunken",
            Self::Chronic => "chronic",
        }
    }

    /// Whether the distributional tests may run at all.
    pub fn covered(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Classifies this run's cohort against the source's own history.
///
/// Deliberately never *lowers* the bar: a thin source does not become easier to
/// trip by being chronically thin, it becomes honestly labelled as unmonitored.
/// What the per-source view buys is the opposite of leniency — a run that cannot
/// be judged now says so (`below_cohort`) instead of being recorded as a clean
/// `ok` run and quietly becoming baseline material.
pub fn cohort_adequacy(cfg: &ResilienceConfig, docs: u32, baseline: &Baseline) -> CohortAdequacy {
    if docs >= cfg.min_cohort_docs {
        CohortAdequacy::Full
    } else if baseline.peak_docs() >= cfg.min_cohort_docs {
        CohortAdequacy::Shrunken
    } else {
        CohortAdequacy::Chronic
    }
}

/// How a run's fetch layer went. `ok` counts fetches with a winning tier verdict
/// and a 2xx (or no status, for the tiers that have none) — the structured
/// `TierVerdict`, never the prose escalation trail.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FetchHealth {
    pub attempted: u32,
    pub ok: u32,
}

impl FetchHealth {
    pub fn rate(&self) -> f64 {
        if self.attempted == 0 {
            // Nothing was fetched (a source-mode run over stored bodies): the
            // fetch layer cannot be the explanation, so it does not gate.
            1.0
        } else {
            self.ok as f64 / self.attempted as f64
        }
    }
}

/// One invariant checked against this run's cohort.
#[derive(Debug, Clone)]
pub struct InvariantCheck {
    pub field: String,
    pub kind: String,
    /// Records the invariant was mined over — its weight in the score.
    pub support: u32,
    /// Documents in this cohort that broke it.
    pub broke: u32,
    /// Documents in this cohort where it was applicable.
    pub checked: u32,
}

impl InvariantCheck {
    fn violated(&self, ratio: f64) -> bool {
        self.checked > 0 && (self.broke as f64 / self.checked as f64) >= ratio
    }
}

/// Everything the verdict is computed from.
pub struct RunInput<'a> {
    pub docs: u32,
    pub fetch: FetchHealth,
    pub sketches: &'a BTreeMap<String, FieldSketch>,
    pub baseline: &'a Baseline,
    pub invariants: &'a [InvariantCheck],
    /// `None` on a source's first run, or when no key appeared in both this run
    /// and the last — there is nothing to compare against.
    pub drift: Option<CohortDrift>,
}

/// One contributing test, recorded whether or not it tripped, so a `source_runs`
/// row explains itself without re-running anything.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Reason {
    pub test: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub value: f64,
    pub threshold: f64,
}

impl Reason {
    fn new(test: &str, field: Option<&str>, value: f64, threshold: f64) -> Self {
        Self {
            test: test.to_string(),
            field: field.map(str::to_string),
            value: round4(value),
            threshold: round4(threshold),
        }
    }
}

/// The verdict for one run.
#[derive(Debug, Clone)]
pub struct RunEvaluation {
    pub verdict: RunVerdict,
    pub diagnosis: Option<Diagnosis>,
    pub score: f64,
    pub reasons: Vec<Reason>,
    pub drift: Option<CohortDrift>,
    /// Whether the run had enough documents for the distributional tests. A
    /// source that never reaches the cohort floor is effectively unmonitored,
    /// and says so on `GET /sources` rather than hiding it.
    pub statistical_coverage: bool,
    /// The same fact with its reason attached, decided per source.
    pub adequacy: CohortAdequacy,
}

impl RunEvaluation {
    /// Whether this run counts against the source's health.
    pub fn tripped(&self, cfg: &ResilienceConfig) -> bool {
        self.verdict.judged() && self.score >= cfg.degrade_score
    }

    /// Whether it counts as severe — enough to accelerate `degraded` to
    /// `quarantined`.
    pub fn severe(&self, cfg: &ResilienceConfig) -> bool {
        self.verdict.judged() && self.score >= cfg.quarantine_score
    }
}

/// Scores one run. See the module doc for the two overrides.
pub fn evaluate(cfg: &ResilienceConfig, input: &RunInput) -> RunEvaluation {
    let mut reasons = Vec::new();
    let adequacy = cohort_adequacy(cfg, input.docs, input.baseline);

    // ---- gate: fetch first, always ----------------------------------------
    let fetch_rate = input.fetch.rate();
    reasons.push(Reason::new(
        "fetch_ok_rate",
        None,
        fetch_rate,
        cfg.fetch_ok_floor,
    ));
    if fetch_rate < cfg.fetch_ok_floor {
        return RunEvaluation {
            verdict: RunVerdict::Inconclusive,
            diagnosis: None,
            score: 0.0,
            reasons,
            drift: input.drift,
            statistical_coverage: false,
            adequacy,
        };
    }

    // ---- conclusive: a field that simply vanished --------------------------
    if let Some((field, run_rate, base_rate)) = total_collapse(input) {
        reasons.push(Reason::new(
            "total_collapse",
            Some(&field),
            run_rate,
            base_rate,
        ));
        return RunEvaluation {
            verdict: RunVerdict::Broken,
            diagnosis: Some(Diagnosis::FieldLoss),
            score: 1.0,
            reasons,
            drift: input.drift,
            statistical_coverage: adequacy.covered(),
            adequacy,
        };
    }

    // ---- a legitimately quiet listing is not a break -----------------------
    // `each` with a container tells us the listing was found and held nothing.
    // Without the container split this is indistinguishable from a broken
    // listing selector, which is why the split exists.
    if let Some(field) = quiet_listing(input) {
        reasons.push(Reason::new("container_empty", Some(&field), 1.0, 1.0));
        return RunEvaluation {
            verdict: RunVerdict::ContentEmpty,
            diagnosis: None,
            score: 0.0,
            reasons,
            drift: input.drift,
            statistical_coverage: adequacy.covered(),
            adequacy,
        };
    }

    // ---- below the cohort floor, no distributional claim is honest ---------
    // And a claim that was never made must not become evidence: the run is
    // recorded as `below_cohort`, which neither moves the state nor enters the
    // baseline. Recording it as `ok` (what this did before) let a chronically
    // thin source assemble a baseline out of runs nobody had judged.
    let statistical_coverage = adequacy.covered();
    if !statistical_coverage {
        reasons.push(Reason::new(
            &format!("cohort_docs:{}", adequacy.as_str()),
            None,
            input.docs as f64,
            cfg.min_cohort_docs as f64,
        ));
        return RunEvaluation {
            verdict: RunVerdict::BelowCohort,
            diagnosis: None,
            score: 0.0,
            reasons,
            drift: input.drift,
            statistical_coverage,
            adequacy,
        };
    }

    // ---- the five weighted terms -------------------------------------------
    let s_missrate = score_missrate(input, &mut reasons);
    let s_distinct = score_distinctness(cfg, input, &mut reasons);
    let s_invariant = score_invariants(cfg, input, &mut reasons);
    let s_shape = score_shape(cfg, input, &mut reasons);
    let (s_divergence, divergence) = score_divergence(cfg, input, &mut reasons);

    let weighted = W_MISSRATE * s_missrate
        + W_DISTINCT * s_distinct
        + W_INVARIANT * s_invariant
        + W_SHAPE * s_shape
        + W_DIVERGENCE * s_divergence;
    // A per-record field that has gone near-constant is conclusive on its own.
    // The weighted sum cannot express that: distinctness carries 0.20, so even
    // with shape and divergence corroborating it lands at ~0.49 and never trips,
    // which would leave the design's own highest-precision silent-corruption
    // signal unable to act. See `conclusive_rebind`.
    let score = if let Some((field, base, run)) = conclusive_rebind(cfg, input) {
        reasons.push(Reason::new(
            "distinctness_collapse",
            Some(&field),
            run,
            base,
        ));
        1.0
    } else {
        weighted.clamp(0.0, 1.0)
    };
    reasons.push(Reason::new("score", None, score, cfg.degrade_score));

    let tripped = score >= cfg.degrade_score;
    // The diagnosis is recorded whether or not the run tripped: `content_changed`
    // on a healthy run is real information, and a stored run row that explains
    // itself is the whole point of keeping them.
    let diagnosis = diagnose(divergence, s_missrate, s_distinct, s_invariant, s_shape);
    let verdict = match (tripped, diagnosis) {
        (false, _) => RunVerdict::Ok,
        // Neither input moved and the output did: extraction is no longer a
        // function of its input, so the change is ours. Never a site problem.
        (true, Some(Diagnosis::SelfInflicted)) => RunVerdict::SelfInflicted,
        (true, _) => RunVerdict::Broken,
    };

    RunEvaluation {
        verdict,
        diagnosis,
        score: round4(score),
        reasons,
        drift: input.drift,
        statistical_coverage,
        adequacy,
    }
}

/// The assumption-free rule: a field that used to match and now never does.
/// Returns `(field, run miss rate, baseline miss rate)`.
fn total_collapse(input: &RunInput) -> Option<(String, f64, f64)> {
    if input.docs < COLLAPSE_MIN_DOCS {
        return None;
    }
    input.sketches.iter().find_map(|(field, sketch)| {
        if sketch.n < COLLAPSE_MIN_DOCS || sketch.miss_rate() < COLLAPSE_RUN_RATE {
            return None;
        }
        // Only against a history: a source's first run has never "worked", so
        // there is nothing for it to have stopped doing.
        let (base_misses, base_n) = input.baseline.pooled_misses(field);
        if base_n < COLLAPSE_MIN_DOCS {
            return None;
        }
        let base_rate = base_misses as f64 / base_n as f64;
        (base_rate <= COLLAPSE_BASE_RATE).then(|| (field.clone(), sketch.miss_rate(), base_rate))
    })
}

/// A per-record field that has collapsed to near-constant across a full cohort —
/// conclusive on its own. Returns `(field, baseline distinctness, run
/// distinctness)`.
///
/// The design's own argument for why this is safe: after a redesign a selector
/// often rebinds to a template element that is identical on every page, the
/// values stay plausible, the miss rate stays zero — and there is no benign
/// reason for a field that has been distinct-per-record over its whole history to
/// become constant across thirty documents. The preconditions are deliberately
/// tighter than the weighted term's: a near-1.0 baseline over enough healthy
/// runs, a near-0 run, a full cohort, and no content-change explanation. That
/// last guard is why the divergence signal exists — if the words on the page all
/// changed and the markup did not, the site really did start saying the same
/// thing everywhere, and that is not our bug.
fn conclusive_rebind(cfg: &ResilienceConfig, input: &RunInput) -> Option<(String, f64, f64)> {
    if input.docs < cfg.min_cohort_docs {
        return None;
    }
    if matches!(
        explain_divergence(cfg, input.drift),
        Some(Diagnosis::ContentChanged)
    ) {
        return None;
    }
    input.sketches.iter().find_map(|(field, sketch)| {
        if !input.baseline.distributional(field) {
            return None;
        }
        let series = input.baseline.series(field, |s| s.distinct_ratio as f64);
        let base = median(&series)?;
        let run = sketch.distinct_ratio as f64;
        (base >= CONCLUSIVE_BASE_DISTINCT && run <= CONCLUSIVE_RUN_DISTINCT)
            .then(|| (field.clone(), base, run))
    })
}

/// Baseline distinctness a field must have held for its collapse to be
/// conclusive.
const CONCLUSIVE_BASE_DISTINCT: f64 = 0.9;
/// Run distinctness at or below which the collapse is total.
const CONCLUSIVE_RUN_DISTINCT: f64 = 0.1;

/// A field whose container matched on every document and held no items, with no
/// other field missing — the job board with no postings this week.
fn quiet_listing(input: &RunInput) -> Option<String> {
    let quiet = input
        .sketches
        .iter()
        .find(|(_, s)| s.n > 0 && s.container_empty == s.n)
        .map(|(f, _)| f.clone())?;
    let anything_missing = input.sketches.values().any(|s| s.misses() > 0);
    (!anything_missing).then_some(quiet)
}

/// Rate signals: a miss rate or a coercion-failure rate whose Wilson interval is
/// separated upward from the baseline's. The score is the fraction of the
/// remaining headroom lost, so going 0.05 -> 0.50 scores lower than 0.05 -> 0.95.
fn score_missrate(input: &RunInput, reasons: &mut Vec<Reason>) -> f64 {
    let mut worst: f64 = 0.0;
    for (field, sketch) in input.sketches {
        for (test, run, base) in [
            (
                "miss_rate",
                (sketch.misses(), sketch.n),
                input.baseline.pooled_misses(field),
            ),
            (
                "coercion_failure_rate",
                (
                    sketch.coercion_failed,
                    sketch.coerced + sketch.coercion_failed,
                ),
                input.baseline.pooled_coercion(field),
            ),
        ] {
            if base.1 == 0 || run.1 == 0 {
                continue;
            }
            let run_rate = run.0 as f64 / run.1 as f64;
            let base_rate = base.0 as f64 / base.1 as f64;
            if !sketch::rate_rose(run, base) {
                continue;
            }
            let headroom = 1.0 - base_rate;
            let s = if headroom <= 0.0 {
                0.0
            } else {
                ((run_rate - base_rate) / headroom).clamp(0.0, 1.0)
            };
            if s > worst {
                worst = s;
            }
            reasons.push(Reason::new(test, Some(field), run_rate, base_rate));
        }
    }
    worst
}

/// Distinctness collapse: a field that carried a different value per record and
/// now carries the same one. After a redesign a selector often rebinds to a
/// template element — a footer, a nav item, a "Free shipping" banner — and the
/// values stay plausible while every record gets the same one. Fields that are
/// *legitimately* constant have a low baseline distinctness, so nothing fires.
fn score_distinctness(cfg: &ResilienceConfig, input: &RunInput, reasons: &mut Vec<Reason>) -> f64 {
    let mut worst: f64 = 0.0;
    for (field, sketch) in input.sketches {
        if !input.baseline.distributional(field) {
            continue;
        }
        let series = input.baseline.series(field, |s| s.distinct_ratio as f64);
        let Some(base) = median(&series) else {
            continue;
        };
        if base < PER_RECORD_DISTINCT {
            continue; // legitimately repetitive; a collapse says nothing
        }
        let run = sketch.distinct_ratio as f64;
        let Some(z) = robust_z(run, &series, RATIO_TOL) else {
            continue;
        };
        if z > -cfg.mad_z {
            continue;
        }
        let s = ((base - run) / base).clamp(0.0, 1.0);
        reasons.push(Reason::new("distinct_ratio", Some(field), run, base));
        if s > worst {
            worst = s;
        }
    }
    worst
}

/// Mined-invariant violations, weighted by the support they were mined over: a
/// regularity that held over 5,000 records counts for more than one that held
/// over 500.
fn score_invariants(cfg: &ResilienceConfig, input: &RunInput, reasons: &mut Vec<Reason>) -> f64 {
    let total: u64 = input.invariants.iter().map(|i| i.support as u64).sum();
    if total == 0 {
        return 0.0;
    }
    let mut violated_support: u64 = 0;
    for check in input.invariants {
        if !check.violated(cfg.invariant_violation_ratio) {
            continue;
        }
        violated_support += check.support as u64;
        reasons.push(Reason::new(
            &format!("invariant:{}", check.kind),
            Some(&check.field),
            check.broke as f64 / check.checked.max(1) as f64,
            cfg.invariant_violation_ratio,
        ));
    }
    violated_support as f64 / total as f64
}

/// Value-domain drift: the length distribution or character-class profile moving
/// far relative to how much this source's own history moves. A price field that
/// becomes "Add to cart" goes from 80% digits to 90% letters; a date that
/// becomes a review count moves three length buckets.
///
/// The comparison is scale-free by construction — today's distance from the
/// baseline shape, z-scored against the baseline's own run-to-run distances — so
/// a naturally noisy source is not punished for being noisy.
fn score_shape(cfg: &ResilienceConfig, input: &RunInput, reasons: &mut Vec<Reason>) -> f64 {
    let mut worst: f64 = 0.0;
    for (field, sketch) in input.sketches {
        if !input.baseline.distributional(field) {
            continue;
        }
        let Some(runs) = input.baseline.fields.get(field) else {
            continue;
        };
        // Two distributions per field, over different supports (16 length buckets
        // vs 4 character classes), so they are compared separately rather than
        // zipped into one loop.
        let len = shape_drift(
            &sketch.len_distribution(),
            &runs
                .iter()
                .map(|s| s.len_distribution())
                .collect::<Vec<_>>(),
        );
        let cls = shape_drift(
            &cls_f64(&sketch.cls),
            &runs.iter().map(|s| cls_f64(&s.cls)).collect::<Vec<_>>(),
        );
        for (test, (tv, history)) in [("len_shape", len), ("char_class_shape", cls)] {
            let Some(z) = robust_z(tv, &history, SHAPE_TOL) else {
                continue;
            };
            if z < cfg.mad_z || tv < SHAPE_TOL {
                continue;
            }
            reasons.push(Reason::new(
                test,
                Some(field),
                tv,
                median(&history).unwrap_or(0.0),
            ));
            if tv > worst {
                worst = tv;
            }
        }
    }
    worst
}

/// `(this run's distance from the baseline's typical shape, the baseline runs'
/// own distances from it)` — the scale-free comparison: is today's drift large
/// *relative to how much this source normally drifts*.
fn shape_drift<const N: usize>(run: &[f64; N], baseline: &[[f64; N]]) -> (f64, Vec<f64>) {
    let centre = median_distribution(baseline);
    let tv = sketch::total_variation(run, &centre);
    let history = baseline
        .iter()
        .map(|d| sketch::total_variation(d, &centre))
        .collect();
    (tv, history)
}

fn cls_f64(cls: &[f32; CLS]) -> [f64; CLS] {
    let mut out = [0.0; CLS];
    for (o, &f) in out.iter_mut().zip(cls.iter()) {
        *o = f as f64;
    }
    out
}

/// The input-output divergence test: where this run sits in the
/// `(d_text, d_dom, d_val)` space.
///
/// SimHash over text is structure-blind; the DOM fingerprint is text-blind;
/// extraction is structure-bound. So "the words held still, the markup moved,
/// and the output moved" is a redesign that broke the extractor — the case no
/// counter in the system otherwise notices — while "the words moved, the markup
/// held still" is a healthy source reporting new content.
fn score_divergence(
    cfg: &ResilienceConfig,
    input: &RunInput,
    reasons: &mut Vec<Reason>,
) -> (f64, Option<Diagnosis>) {
    if let Some(d) = input.drift {
        reasons.push(Reason::new("d_text", None, d.text, cfg.drift_high));
        reasons.push(Reason::new("d_dom", None, d.dom, cfg.drift_high));
        reasons.push(Reason::new("d_val", None, d.value, cfg.drift_high));
    }
    let diagnosis = explain_divergence(cfg, input.drift);
    // Divergence names *whose fault* a change is; it is corroborative about
    // whether anything is wrong. At 0.15 it cannot trip a run on its own, and it
    // should not: values that moved while the markup moved, with every field
    // still matching, distinct and well-shaped, is also what a content update
    // delivered through a template change looks like.
    let score = match diagnosis {
        Some(Diagnosis::MarkupDrift | Diagnosis::SelfInflicted) => 1.0,
        Some(Diagnosis::Ambiguous) => 0.5,
        _ => 0.0,
    };
    (score, diagnosis)
}

/// Which cell of the `(d_text, d_dom, d_val)` space this run sits in.
fn explain_divergence(cfg: &ResilienceConfig, drift: Option<CohortDrift>) -> Option<Diagnosis> {
    let d = drift?;
    let low = |x: f64| x <= cfg.drift_low;
    let high = |x: f64| x >= cfg.drift_high;
    if !high(d.value) {
        // The output held still. Whatever the inputs did, extraction tracked it.
        return None;
    }
    Some(match (low(d.text), high(d.dom), low(d.dom)) {
        // Text still, markup moved, output moved — the redesign case.
        (true, true, _) => Diagnosis::MarkupDrift,
        // Neither input moved and the output did. The only explanations are the
        // rules, the transforms or the parser, all of which are ours.
        (true, false, true) => Diagnosis::SelfInflicted,
        // Words moved, markup held: the extractor is tracking real new content.
        (false, false, _) => Diagnosis::ContentChanged,
        // Everything moved, or the drifts are in the band between the
        // thresholds: corroborate with the other terms before acting.
        _ => Diagnosis::Ambiguous,
    })
}

/// Names what happened. The divergence cell wins when it has an opinion about
/// fault, because it is the only test that distinguishes *whose* change this was;
/// otherwise the heaviest contributing term names the failure. `None` when
/// nothing has anything to say — a clean run is not "ambiguous".
fn diagnose(
    divergence: Option<Diagnosis>,
    s_missrate: f64,
    s_distinct: f64,
    s_invariant: f64,
    s_shape: f64,
) -> Option<Diagnosis> {
    if let Some(d @ (Diagnosis::MarkupDrift | Diagnosis::SelfInflicted)) = divergence {
        return Some(d);
    }
    let terms = [
        (W_MISSRATE * s_missrate, Diagnosis::FieldLoss),
        (W_DISTINCT * s_distinct, Diagnosis::SilentRebind),
        (W_INVARIANT * s_invariant, Diagnosis::InvariantBreak),
        (W_SHAPE * s_shape, Diagnosis::ValueDomainDrift),
    ];
    terms
        .into_iter()
        .filter(|(w, _)| *w > 0.0)
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(_, d)| d)
        .or(divergence)
}

/// The hysteresis ladder. A single tripped run is dominated by transient causes,
/// so nothing downstream changes until a source has tripped repeatedly — a
/// system that quarantines on one bad run on an unattended box will spend its
/// life quarantining.
///
/// `trips_of_last3` counts tripped runs among the last three *judged* runs,
/// including this one.
pub fn next_state(
    current: SourceState,
    tripped: bool,
    severe: bool,
    trips_of_last3: u32,
) -> SourceState {
    use SourceState::*;
    match current {
        Healthy => {
            if tripped {
                Suspect
            } else {
                Healthy
            }
        }
        // `suspect` deliberately changes nothing downstream, so it is cheap to
        // enter and one clean run leaves it.
        Suspect => {
            if !tripped {
                Healthy
            } else if trips_of_last3 >= 2 {
                Degraded
            } else {
                Suspect
            }
        }
        // Severe, or three consecutive tripped runs (the two that degraded it
        // plus two more), earns the quarantine. Recovery steps back one rung
        // rather than jumping to healthy.
        Degraded => {
            if !tripped {
                Suspect
            } else if severe || trips_of_last3 >= 3 {
                Quarantined
            } else {
                Degraded
            }
        }
        // Quarantine is terminal without an operator: a stuck source is an
        // acceptable outcome, a source that silently un-quarantines itself and
        // resumes pushing garbage downstream is not.
        Quarantined => Quarantined,
        // Reachable only by an explicit operator override today (repair, which
        // would promote into it, is not built). A tripped run drops it back.
        Probation => {
            if tripped {
                Quarantined
            } else {
                Probation
            }
        }
        Retired => Retired,
    }
}

fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::{CoercionStatus, FieldStatus};
    use crate::resilience::sketch::SketchBuilder;

    fn cfg() -> ResilienceConfig {
        ResilienceConfig {
            min_cohort_docs: 10,
            ..ResilienceConfig::default()
        }
    }

    /// `n` documents where the first `misses` missed and the rest carry a
    /// distinct, price-shaped value — the ordinary per-record field.
    fn prices(n: u32, misses: u32) -> FieldSketch {
        let mut b = SketchBuilder::new();
        for i in 0..n {
            if i < misses {
                b.push(&FieldStatus::Empty, None, &serde_json::Value::Null);
            } else {
                let v = format!("${}.{:02}", 10 + i, (i * 7) % 100);
                b.push(
                    &FieldStatus::Matched,
                    Some(CoercionStatus::NoTransforms),
                    &serde_json::json!(v),
                );
            }
        }
        b.finish()
    }

    /// `n` documents all carrying the same value — either a legitimately constant
    /// field or a rebound one, depending entirely on its history.
    fn constant(n: u32, value: &str) -> FieldSketch {
        let mut b = SketchBuilder::new();
        for _ in 0..n {
            b.push(
                &FieldStatus::Matched,
                Some(CoercionStatus::NoTransforms),
                &serde_json::json!(value),
            );
        }
        b.finish()
    }

    /// `n` documents carrying distinct prose — a field that kept its cardinality
    /// but changed kind.
    fn prose(n: u32) -> FieldSketch {
        let mut b = SketchBuilder::new();
        for i in 0..n {
            let v = format!("Add item {i} to your shopping cart today");
            b.push(
                &FieldStatus::Matched,
                Some(CoercionStatus::NoTransforms),
                &serde_json::json!(v),
            );
        }
        b.finish()
    }

    /// A healthy history: `runs` clean 30-document cohorts of a per-record field.
    fn healthy_baseline(field: &str, runs: usize) -> Baseline {
        let mut b = Baseline::default();
        b.fields.insert(
            field.to_string(),
            (0..runs).map(|_| prices(30, 0)).collect(),
        );
        b
    }

    fn one(field: &str, s: FieldSketch) -> BTreeMap<String, FieldSketch> {
        BTreeMap::from([(field.to_string(), s)])
    }

    fn input<'a>(
        sketches: &'a BTreeMap<String, FieldSketch>,
        baseline: &'a Baseline,
        invariants: &'a [InvariantCheck],
        drift: Option<CohortDrift>,
        docs: u32,
    ) -> RunInput<'a> {
        RunInput {
            docs,
            fetch: FetchHealth {
                attempted: docs,
                ok: docs,
            },
            sketches,
            baseline,
            invariants,
            drift,
        }
    }

    // ---- signal 1: the fetch gate ------------------------------------------

    #[test]
    fn a_broken_run_behind_a_broken_fetch_layer_is_inconclusive_not_broken() {
        let base = healthy_baseline("price", 5);
        let broken = one("price", prices(30, 30));

        // Healthy fetch: this run is unambiguously broken.
        let healthy = evaluate(&cfg(), &input(&broken, &base, &[], None, 30));
        assert_eq!(healthy.verdict, RunVerdict::Broken);
        assert_eq!(healthy.score, 1.0);

        // The identical extraction outcome behind a bot wall must NOT be judged:
        // you cannot blame an extractor for documents you did not receive.
        let gated = evaluate(
            &cfg(),
            &RunInput {
                fetch: FetchHealth {
                    attempted: 30,
                    ok: 9,
                },
                ..input(&broken, &base, &[], None, 30)
            },
        );
        assert_eq!(gated.verdict, RunVerdict::Inconclusive);
        assert_eq!(gated.score, 0.0);
        assert!(
            !gated.tripped(&cfg()),
            "an inconclusive run must not count against the source"
        );

        // Just above the floor it is judged again.
        let ok_fetch = evaluate(
            &cfg(),
            &RunInput {
                fetch: FetchHealth {
                    attempted: 30,
                    ok: 24,
                },
                ..input(&broken, &base, &[], None, 30)
            },
        );
        assert_eq!(ok_fetch.verdict, RunVerdict::Broken);

        // A run that fetched nothing at all (stored bodies) is not gated.
        assert_eq!(
            FetchHealth {
                attempted: 0,
                ok: 0
            }
            .rate(),
            1.0
        );
    }

    // ---- signal 2: total collapse ------------------------------------------

    #[test]
    fn total_collapse_fires_without_a_cohort_but_needs_a_history() {
        let base = healthy_baseline("price", 3);
        let gone = one("price", prices(6, 6));

        // Six documents is under the cohort floor, and it still fires: under a
        // 0% baseline this outcome is not something noise produces.
        let e = evaluate(&cfg(), &input(&gone, &base, &[], None, 6));
        assert_eq!(e.verdict, RunVerdict::Broken);
        assert_eq!(e.diagnosis, Some(Diagnosis::FieldLoss));
        assert!(
            !e.statistical_coverage,
            "still honest about the cohort size"
        );

        // Without a history there is nothing it stopped doing — a source's first
        // run must never trip. It is not a clean run either: nothing judged it.
        let e = evaluate(&cfg(), &input(&gone, &Baseline::default(), &[], None, 6));
        assert_eq!(e.verdict, RunVerdict::BelowCohort);
        assert!(!e.verdict.judged());

        // A field that always missed did not collapse.
        let mut always_missing = Baseline::default();
        always_missing
            .fields
            .insert("price".into(), (0..3).map(|_| prices(30, 30)).collect());
        let e = evaluate(&cfg(), &input(&gone, &always_missing, &[], None, 6));
        assert_eq!(e.verdict, RunVerdict::BelowCohort);

        // Four documents is too few for even this rule.
        let e = evaluate(
            &cfg(),
            &input(&one("price", prices(4, 4)), &base, &[], None, 4),
        );
        assert_eq!(e.verdict, RunVerdict::BelowCohort);
        assert!(!e.tripped(&cfg()));
    }

    // ---- cohort adequacy, decided per source --------------------------------

    #[test]
    fn a_thin_run_is_unjudged_not_a_clean_run() {
        // The self-referential-history bug this guards: a below-floor run used to
        // be recorded as `ok`, which made it baseline material. A source that
        // never reaches the floor then built its entire baseline out of runs
        // nobody had judged, and measured itself against that.
        let base = healthy_baseline("price", 5);
        let thin = one("price", prices(6, 0));
        let e = evaluate(&cfg(), &input(&thin, &base, &[], None, 6));
        assert_eq!(e.verdict, RunVerdict::BelowCohort);
        assert!(!e.verdict.baselines(), "an unjudged run is not evidence");
        assert!(!e.verdict.judged(), "nor may it move the ladder");
        assert_eq!(e.score, 0.0);
        assert!(!e.statistical_coverage);

        // A source that has cleared the floor before shrank; one that never has is
        // structurally unmonitored. Same non-verdict, different fact about the
        // source — and only the second belongs in an "unmonitored" list.
        assert_eq!(e.adequacy, CohortAdequacy::Shrunken);
        let never = evaluate(&cfg(), &input(&thin, &Baseline::default(), &[], None, 6));
        assert_eq!(never.adequacy, CohortAdequacy::Chronic);

        // A thin source is not made *easier* to trip by any of this: it is judged
        // by exactly the rules it was before, i.e. only the assumption-free ones.
        let collapsed = evaluate(
            &cfg(),
            &input(&one("price", prices(6, 6)), &base, &[], None, 6),
        );
        assert_eq!(
            collapsed.verdict,
            RunVerdict::Broken,
            "total collapse still fires"
        );

        // And a full cohort is untouched by the whole mechanism.
        let full = evaluate(
            &cfg(),
            &input(&one("price", prices(30, 0)), &base, &[], None, 30),
        );
        assert_eq!(full.adequacy, CohortAdequacy::Full);
        assert_eq!(full.verdict, RunVerdict::Ok);
        assert!(full.verdict.baselines());
        assert!(full.statistical_coverage);
    }

    // ---- signal 3: Wilson-separated miss-rate rise --------------------------

    #[test]
    fn miss_rate_rise_scores_by_evidence_not_by_ratio() {
        let base = healthy_baseline("price", 5);

        // Half the documents lost the field over a full cohort: separated, and a
        // real contribution to the score.
        let half = one("price", prices(30, 15));
        let e = evaluate(&cfg(), &input(&half, &base, &[], None, 30));
        assert!(e.score > 0.0, "a separated rise must contribute");
        assert!(e.reasons.iter().any(|r| r.test == "miss_rate"));

        // The same *rate* on a 2-document run is below the cohort floor, so no
        // distributional claim is made at all.
        let tiny = one("price", prices(2, 1));
        let e = evaluate(&cfg(), &input(&tiny, &base, &[], None, 2));
        assert_eq!(e.score, 0.0);
        assert!(!e.statistical_coverage);

        // A run identical to the baseline scores zero: the test cannot be
        // "always on" and still be a test.
        let clean = one("price", prices(30, 0));
        let e = evaluate(&cfg(), &input(&clean, &base, &[], None, 30));
        assert_eq!(e.score, 0.0);
        assert_eq!(e.verdict, RunVerdict::Ok);
        assert_eq!(e.diagnosis, None, "a clean run has no diagnosis");
    }

    #[test]
    fn coercion_failure_rise_is_visible_while_the_match_rate_stays_perfect() {
        // The wrong-element case: the selector matches every document, and the
        // transform can no longer coerce what it finds.
        let mut b = SketchBuilder::new();
        for _ in 0..30 {
            b.push(
                &FieldStatus::Matched,
                Some(CoercionStatus::CoercionFailed),
                &serde_json::Value::Null,
            );
        }
        let run = one("price", b.finish());
        assert_eq!(run["price"].miss_rate(), 0.0, "the match rate is untouched");

        let mut base = Baseline::default();
        base.fields.insert(
            "price".into(),
            (0..5)
                .map(|_| {
                    let mut b = SketchBuilder::new();
                    for i in 0..30 {
                        b.push(
                            &FieldStatus::Matched,
                            Some(CoercionStatus::Coerced),
                            &serde_json::json!(format!("{i}.50")),
                        );
                    }
                    b.finish()
                })
                .collect(),
        );

        let e = evaluate(&cfg(), &input(&run, &base, &[], None, 30));
        assert!(
            e.reasons.iter().any(|r| r.test == "coercion_failure_rate"),
            "a coercion collapse behind a perfect match rate must be visible: {:?}",
            e.reasons
        );
        assert!(e.score > 0.0);
    }

    // ---- signal 4: distinctness collapse -----------------------------------

    #[test]
    fn distinctness_collapse_fires_on_a_per_record_field_and_not_on_a_constant_one() {
        // A per-record price field rebinds to a site-wide banner: every record
        // now carries the same plausible value and the miss rate stays zero.
        let base = healthy_baseline("price", 5);
        let rebound = one("price", constant(30, "Free shipping"));
        let e = evaluate(&cfg(), &input(&rebound, &base, &[], None, 30));
        assert!(
            e.reasons.iter().any(|r| r.test == "distinctness_collapse"),
            "{:?}",
            e.reasons
        );
        assert_eq!(e.verdict, RunVerdict::Broken);
        assert_eq!(e.diagnosis, Some(Diagnosis::SilentRebind));
        assert_eq!(
            e.score, 1.0,
            "a total collapse of a per-record field is conclusive"
        );

        // A field that has ALWAYS been constant (a currency, a category) has a
        // low baseline distinctness, so its constancy says nothing.
        let mut const_base = Baseline::default();
        const_base.fields.insert(
            "currency".into(),
            (0..5).map(|_| constant(30, "USD")).collect(),
        );
        let still_const = one("currency", constant(30, "USD"));
        let e = evaluate(&cfg(), &input(&still_const, &const_base, &[], None, 30));
        assert_eq!(
            e.score, 0.0,
            "a legitimately constant field must never trip"
        );

        // Below the cohort floor the conclusive rule does not apply.
        let small = one("price", constant(6, "Free shipping"));
        let e = evaluate(&cfg(), &input(&small, &base, &[], None, 6));
        assert_eq!(e.score, 0.0);

        // And a real content change that made every page say the same thing is
        // not our bug: the words moved and the markup did not.
        let e = evaluate(
            &cfg(),
            &input(
                &rebound,
                &base,
                &[],
                Some(CohortDrift {
                    text: 0.4,
                    dom: 0.0,
                    value: 0.4,
                    compared: 30,
                }),
                30,
            ),
        );
        assert!(
            e.score < 1.0,
            "a content-change explanation blocks the conclusive rule"
        );
    }

    // ---- signal 5: mined invariants ----------------------------------------

    #[test]
    fn invariant_violations_score_by_support_and_need_a_real_violation_ratio() {
        let base = healthy_baseline("price", 5);
        let run = one("price", prices(30, 0));

        // Most of the cohort breaks a regularity that held over 2,000 records.
        let broken = [InvariantCheck {
            field: "price".into(),
            kind: "regex".into(),
            support: 2000,
            broke: 24,
            checked: 30,
        }];
        let e = evaluate(&cfg(), &input(&run, &base, &broken, None, 30));
        assert!(e.reasons.iter().any(|r| r.test == "invariant:regex"));
        assert!(e.score > 0.0);
        assert_eq!(e.diagnosis, Some(Diagnosis::InvariantBreak));

        // A handful of exceptions is normal in scraped data and must not fire.
        let noise = [InvariantCheck {
            field: "price".into(),
            kind: "regex".into(),
            support: 2000,
            broke: 2,
            checked: 30,
        }];
        let e = evaluate(&cfg(), &input(&run, &base, &noise, None, 30));
        assert_eq!(e.score, 0.0);
        assert_eq!(e.diagnosis, None);
    }

    // ---- signal 6: value-domain shape drift --------------------------------

    #[test]
    fn shape_drift_fires_when_a_price_field_turns_into_prose() {
        let base = healthy_baseline("price", 6);
        let e = evaluate(
            &cfg(),
            &input(&one("price", prose(30)), &base, &[], None, 30),
        );
        assert!(
            e.reasons
                .iter()
                .any(|r| r.test == "char_class_shape" || r.test == "len_shape"),
            "{:?}",
            e.reasons
        );

        // Different prices of the same shape are not drift — the field is doing
        // exactly what it always did with new values.
        let mut later = SketchBuilder::new();
        for i in 0..30 {
            later.push(
                &FieldStatus::Matched,
                Some(CoercionStatus::NoTransforms),
                &serde_json::json!(format!("${}.{:02}", 90 + i, (i * 3) % 100)),
            );
        }
        let e = evaluate(
            &cfg(),
            &input(&one("price", later.finish()), &base, &[], None, 30),
        );
        assert!(
            !e.reasons.iter().any(|r| r.test == "char_class_shape"),
            "new values of the same shape are not drift: {:?}",
            e.reasons
        );
        assert_eq!(e.verdict, RunVerdict::Ok);
    }

    // ---- signal 7: input-output divergence ---------------------------------

    #[test]
    fn divergence_separates_a_redesign_from_new_content_from_our_own_bug() {
        let base = healthy_baseline("price", 5);
        let run = one("price", prices(30, 0));
        let ev = |d: CohortDrift| evaluate(&cfg(), &input(&run, &base, &[], Some(d), 30));

        // Text still, markup moved, output moved: the site was redesigned and the
        // extractor followed the markup somewhere else.
        let redesign = ev(CohortDrift {
            text: 0.02,
            dom: 0.35,
            value: 0.40,
            compared: 30,
        });
        assert_eq!(redesign.diagnosis, Some(Diagnosis::MarkupDrift));
        assert!(redesign.score > 0.0);
        // Corroborative, not conclusive: every field still matches, stays distinct
        // and keeps its shape, which is also what a templated content update looks
        // like. Divergence names the cause; it does not convict on its own.
        assert!(!redesign.tripped(&cfg()));

        // Words moved, markup held: a healthy source reporting new content. This
        // is the negative control the whole design turns on.
        let content = ev(CohortDrift {
            text: 0.40,
            dom: 0.01,
            value: 0.40,
            compared: 30,
        });
        assert_eq!(content.diagnosis, Some(Diagnosis::ContentChanged));
        assert_eq!(content.score, 0.0, "a content change must never score");

        // Neither input moved and the output did: the change is ours.
        let ours = ev(CohortDrift {
            text: 0.01,
            dom: 0.01,
            value: 0.45,
            compared: 30,
        });
        assert_eq!(ours.diagnosis, Some(Diagnosis::SelfInflicted));
        assert!(ours.score > 0.0);

        // The output held still: whatever the inputs did, extraction tracked it.
        let tracking = ev(CohortDrift {
            text: 0.40,
            dom: 0.40,
            value: 0.01,
            compared: 30,
        });
        assert_eq!(tracking.score, 0.0);
        assert_eq!(tracking.diagnosis, None);

        // With no comparable keys there is no claim to make.
        let e = evaluate(&cfg(), &input(&run, &base, &[], None, 30));
        assert_eq!(e.score, 0.0);
    }

    #[test]
    fn a_self_inflicted_run_is_verdict_self_inflicted_not_broken() {
        // A rebind with neither input moving: it still counts against the source,
        // but it is named for what it is. Proposing a new selector for a
        // regression we caused is how a system learns to paper over its own bugs.
        let base = healthy_baseline("price", 5);
        let e = evaluate(
            &cfg(),
            &input(
                &one("price", constant(30, "Free shipping")),
                &base,
                &[],
                Some(CohortDrift {
                    text: 0.01,
                    dom: 0.01,
                    value: 0.5,
                    compared: 30,
                }),
                30,
            ),
        );
        assert_eq!(e.diagnosis, Some(Diagnosis::SelfInflicted));
        assert_eq!(e.verdict, RunVerdict::SelfInflicted);
        assert!(e.tripped(&cfg()));
        assert!(
            !e.verdict.baselines(),
            "a broken run must not become its own baseline"
        );
    }

    // ---- the quiet-listing carve-out ---------------------------------------

    #[test]
    fn a_listing_that_matched_and_held_nothing_is_content_empty_not_broken() {
        let mut quiet = SketchBuilder::new();
        for _ in 0..30 {
            quiet.push(&FieldStatus::ContainerEmpty, None, &serde_json::json!([]));
        }
        let run = one("jobs", quiet.finish());
        let mut base = Baseline::default();
        base.fields
            .insert("jobs".into(), (0..5).map(|_| prices(30, 0)).collect());

        let e = evaluate(&cfg(), &input(&run, &base, &[], None, 30));
        assert_eq!(e.verdict, RunVerdict::ContentEmpty);
        assert!(
            !e.verdict.judged(),
            "a quiet week must not move the source's state"
        );
        assert!(!e.verdict.baselines(), "nor teach it that zero is normal");
        assert!(!e.tripped(&cfg()));

        // The same field going *missing* (container gone) is a break.
        let e = evaluate(
            &cfg(),
            &input(&one("jobs", prices(30, 30)), &base, &[], None, 30),
        );
        assert_eq!(e.verdict, RunVerdict::Broken);
    }

    // ---- the ladder ---------------------------------------------------------

    #[test]
    fn one_bad_run_never_reaches_further_than_suspect() {
        use SourceState::*;
        // A single tripped run is dominated by transient causes, and `suspect`
        // changes nothing downstream.
        assert_eq!(next_state(Healthy, true, true, 1), Suspect);
        // One clean run leaves it.
        assert_eq!(next_state(Suspect, false, false, 0), Healthy);
        // Two of the last three trips it down a rung.
        assert_eq!(next_state(Suspect, true, false, 2), Degraded);
        // Recovery steps back one rung, not straight to healthy.
        assert_eq!(next_state(Degraded, false, false, 1), Suspect);
        // Severe accelerates; three consecutive also gets there.
        assert_eq!(next_state(Degraded, true, true, 2), Quarantined);
        assert_eq!(next_state(Degraded, true, false, 3), Quarantined);
        assert_eq!(next_state(Degraded, true, false, 2), Degraded);
        // Quarantine never un-sticks itself.
        assert_eq!(next_state(Quarantined, false, false, 0), Quarantined);
        assert_eq!(next_state(Retired, true, true, 3), Retired);
    }
}
