//! Manufactured ground truth: the mutation taxonomy and the offline evaluation
//! harness behind `--bin resilience-eval` (`resilient-extraction.md` §12.1).
//!
//! # Why this module exists before any repair does
//!
//! The thing the detector detects is by definition unobserved, so ground truth
//! has to be **manufactured**. Until this module there was no recall number and
//! no false-positive number anywhere in the project — `IMPLEMENTATION-NOTES.md`
//! says so in as many words — and the design's own rule is that auto-promotion
//! must not ship before the measurement that would justify it. So the number
//! comes first, and everything downstream is gated on it.
//!
//! # What a mutation is
//!
//! A [`Mutation`] is a **deterministic, string-level rewrite of a retained
//! body** drawn from a taxonomy of real breakages. String-level and not
//! DOM-level on purpose: a real redesign arrives as different bytes, and a
//! rewrite that round-trips through a parser would launder exactly the
//! malformedness that makes real drift hard.
//!
//! Two of the classes are **negative controls** and must NOT fire
//! ([`MutationClass::BuildHashChurn`], [`MutationClass::TextOnlyChange`], plus
//! [`MutationClass::None`]). They are the binding constraint: on an unattended
//! box a false quarantine that silently stops a working pipeline is worse than a
//! detection that arrives a week late.
//!
//! # What the harness is not
//!
//! It is not a database test and it touches no store. [`evaluate_corpus`] runs
//! the real detector — [`sketch_run`](super::sketch::sketch_run),
//! [`invariants::mine`], [`invariants::check`], [`detect::evaluate`] — over an
//! in-memory corpus, so a threshold change shows up as a recall/FPR delta in a
//! test rather than as silence in production.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::config::ResilienceConfig;
use crate::extract::{extract_batch_with_report, CompiledRuleSet};
use crate::simhash;

use super::detect::{self, Baseline, FetchHealth, RunInput};
use super::invariants;
use super::sketch::{self, sketch_run};
use super::{doc_signals, CohortDrift, Diagnosis, RunVerdict};

/// One class of synthetic breakage, and what real event it stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationClass {
    /// CSS refactor: the field's class token is renamed everywhere.
    ClassRename,
    /// webpack/Tailwind rebuild: per-build digest classes churn. **Negative
    /// control** — `dom_simhash` folds these, so nothing may fire.
    BuildHashChurn,
    /// Template rewrite: the field's element changes tag.
    TagChange,
    /// Layout change: an extra wrapper element around the field.
    WrapperInsertion,
    /// Silent corruption: two fields' values swap places.
    SiblingSwap,
    /// Silent corruption: an earlier, constant element with the same signature
    /// is inserted, so a first-match selector binds to the template.
    DuplicateNode,
    /// Markup modernisation: an attribute-borne value moves into element text.
    AttrToText,
    /// The field is removed from the site entirely.
    NodeDeletion,
    /// **Negative control**: a genuine content change, markup untouched.
    TextOnlyChange,
    /// **Negative control**: the corpus, unmodified.
    None,
}

impl MutationClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ClassRename => "class_rename",
            Self::BuildHashChurn => "build_hash_churn",
            Self::TagChange => "tag_change",
            Self::WrapperInsertion => "wrapper_insertion",
            Self::SiblingSwap => "sibling_swap",
            Self::DuplicateNode => "duplicate_node",
            Self::AttrToText => "attr_to_text",
            Self::NodeDeletion => "node_deletion",
            Self::TextOnlyChange => "text_only_change",
            Self::None => "none",
        }
    }

    /// Whether the detector is expected to fire on this class.
    ///
    /// `false` here is not "we do not mind either way" — it is the assertion
    /// that firing is a **false positive**, which is the number the design
    /// treats as binding.
    pub fn should_fire(self) -> bool {
        !matches!(
            self,
            Self::BuildHashChurn | Self::TextOnlyChange | Self::None
        )
    }

    /// Whether this class is a *silent corruption* — the field still extracts,
    /// it is just wrong. The design's own falsifier separates these from hard
    /// breaks because their recall target is much lower (0.50 vs 0.90).
    pub fn silent(self) -> bool {
        matches!(self, Self::SiblingSwap | Self::DuplicateNode)
    }

    /// Every class, in taxonomy order.
    pub fn all() -> [MutationClass; 10] {
        [
            Self::ClassRename,
            Self::BuildHashChurn,
            Self::TagChange,
            Self::WrapperInsertion,
            Self::SiblingSwap,
            Self::DuplicateNode,
            Self::AttrToText,
            Self::NodeDeletion,
            Self::TextOnlyChange,
            Self::None,
        ]
    }
}

/// A mutation bound to the markup it rewrites.
///
/// `field_class` is the class token the target field's selector is built on and
/// `partner_class` is a second field used by the swap classes; naming them
/// keeps the taxonomy applicable to a real retained body, not only to the
/// synthetic corpus.
#[derive(Debug, Clone)]
pub struct Mutation {
    pub class: MutationClass,
    pub field_class: String,
    pub partner_class: String,
    /// The attribute [`MutationClass::AttrToText`] moves into text.
    pub attr: String,
}

impl Mutation {
    pub fn new(class: MutationClass, field_class: &str, partner_class: &str) -> Self {
        Self {
            class,
            field_class: field_class.to_string(),
            partner_class: partner_class.to_string(),
            attr: "data-value".to_string(),
        }
    }

    /// Applies the mutation to one body. Deterministic: the same body and the
    /// same mutation always produce the same bytes, so a recall number is
    /// reproducible rather than a sample.
    pub fn apply(&self, doc: &str) -> String {
        let f = &self.field_class;
        let p = &self.partner_class;
        match self.class {
            MutationClass::None => doc.to_string(),
            MutationClass::ClassRename => doc.replace(&format!("\"{f}\""), &format!("\"{f}-v2\"")),
            MutationClass::BuildHashChurn => churn_build_hashes(doc),
            // `<span class="price">` → `<div class="price">`: the class survives,
            // the tag does not, so a tag-qualified rule loses its binding.
            MutationClass::TagChange => map_field(doc, f, |open, inner| {
                format!("{}{inner}</div>", open.replacen("<span", "<div", 1))
            }),
            // A layout wrapper around the PARTNER field, which is the one bound
            // by a child combinator — a descendant-bound rule survives a wrapper
            // and should, so wrapping the wrong field would measure nothing.
            MutationClass::WrapperInsertion => map_field(doc, p, |open, inner| {
                format!("<div class=\"shell\">{open}{inner}</span></div>")
            }),
            MutationClass::SiblingSwap => swap_classes(doc, f, p),
            // An earlier element with the same signature and CONSTANT text: a
            // first-match selector binds to the template on every document.
            MutationClass::DuplicateNode => map_field(doc, f, |open, inner| {
                format!("<span class=\"{FIELD_PLACEHOLDER}\">9999</span>{open}{inner}</span>")
                    .replace(
                        FIELD_PLACEHOLDER,
                        class_of(open).unwrap_or(FIELD_PLACEHOLDER),
                    )
            }),
            // Markup modernisation: the value stops being the element's text and
            // lives only in the attribute. Seen from a text-reading rule, that is
            // the attribute↔text move breaking the binding.
            MutationClass::AttrToText => map_field(doc, f, |open, _| format!("{open}</span>")),
            MutationClass::NodeDeletion => map_field(doc, f, |_, _| String::new()),
            // A new value in the same shape, DERIVED FROM THE OLD ONE. A
            // literal constant here would collapse the field to one value
            // across the cohort — a distinctness collapse, i.e. exactly the
            // corruption this class is the control for — and the harness would
            // then report the detector's correct catch as a false positive.
            MutationClass::TextOnlyChange => map_field(doc, f, |open, inner| {
                let next = match inner.trim().parse::<i64>() {
                    Ok(v) => (v + 500).to_string(),
                    Err(_) => format!("{} rev", inner.trim()),
                };
                format!("{open}{next}</span>")
            }),
        }
    }
}

/// Placeholder woven through [`Mutation::apply`]'s duplicate-node arm so the
/// inserted decoy carries the same class as the element it shadows.
const FIELD_PLACEHOLDER: &str = "\u{0}class\u{0}";

/// The first `class="…"` token of an open tag.
fn class_of(open: &str) -> Option<&str> {
    let i = open.find("class=\"")? + 7;
    let rest = &open[i..];
    let j = rest.find('"')?;
    Some(&rest[..j])
}

/// Rewrites every `<span class="{class}" …>…</span>` in `doc` through `f`,
/// which receives the element's full open tag and its inner text and returns
/// the replacement for the WHOLE element (open tag, text and closing tag).
///
/// String-level and sentinel-free: it walks the open tag to its `>` and takes
/// the next `</span>` as the element's end, which holds for the flat field
/// elements this taxonomy targets. A body it cannot parse this way is returned
/// unchanged, and [`Mutation`]'s own test asserts that every class actually
/// moved some bytes — a mutation that silently no-ops would report as a clean
/// negative control forever and quietly make the recall number a lie.
fn map_field(doc: &str, class: &str, mut f: impl FnMut(&str, &str) -> String) -> String {
    let key = format!("<span class=\"{class}\"");
    let mut out = String::with_capacity(doc.len() + 64);
    let mut rest = doc;
    while let Some(i) = rest.find(&key) {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        let Some(gt) = tail.find('>') else {
            out.push_str(tail);
            return out;
        };
        let open = &tail[..=gt];
        let body = &tail[gt + 1..];
        let Some(close) = body.find("</span>") else {
            out.push_str(tail);
            return out;
        };
        out.push_str(&f(open, &body[..close]));
        rest = &body[close + "</span>".len()..];
    }
    out.push_str(rest);
    out
}

/// Rewrites every per-build digest class to a different digest — the webpack
/// rebuild. Falls back to *adding* one when the body carries none, so the
/// negative control is still exercised on a corpus that has no digests.
fn churn_build_hashes(doc: &str) -> String {
    let mut out = String::with_capacity(doc.len() + 32);
    let mut rest = doc;
    let mut touched = false;
    while let Some(i) = rest.find("class=\"") {
        let (head, tail) = rest.split_at(i + 7);
        out.push_str(head);
        let Some(end) = tail.find('"') else {
            out.push_str(tail);
            return out;
        };
        let (classes, after) = tail.split_at(end);
        let rewritten: Vec<String> = classes
            .split_whitespace()
            .map(|c| match simhash::build_hash_stem(c) {
                Some(stem) => {
                    touched = true;
                    format!("{stem}-9f8e7d6c")
                }
                None => c.to_string(),
            })
            .collect();
        out.push_str(&rewritten.join(" "));
        rest = after;
    }
    out.push_str(rest);
    if touched {
        out
    } else {
        doc.replace("class=\"", "class=\"css-9f8e7d6c ")
    }
}

/// Swaps the two fields' class tokens, so each selector now reads the other's
/// value. Both fields still extract — this is the silent-corruption shape.
fn swap_classes(doc: &str, a: &str, b: &str) -> String {
    const TMP: &str = "\u{0}swap\u{0}";
    doc.replace(&format!("\"{a}\""), &format!("\"{TMP}\""))
        .replace(&format!("\"{b}\""), &format!("\"{a}\""))
        .replace(&format!("\"{TMP}\""), &format!("\"{b}\""))
}

// ── The synthetic corpus ────────────────────────────────────────────────────

/// A deterministic detail-page corpus: one body per record, three fields
/// (`title`, `price`, `sku`), stable across calls with the same `run`.
///
/// Synthetic rather than checked-in HTML so the harness runs in CI on a fresh
/// clone with no `data/` directory, and so cohort size is a parameter rather
/// than whatever the fleet happened to retain. `--corpus <dir>` on the bin
/// points the same taxonomy at real retained bodies.
/// Filler prose for [`fixture_corpus`] — see the comment at its use site for
/// why a realistic document size is load-bearing rather than cosmetic.
const PROSE: &str = "This component ships in a protective sleeve and is rated \
    for continuous indoor use. Specifications are indicative and may vary by \
    batch; consult the datasheet before ordering in volume. ";

pub fn fixture_corpus(records: usize, run: u64) -> Vec<(String, String)> {
    (0..records)
        .map(|i| {
            let key = format!("sku-{i:05}");
            let price = 1000 + (i as u64 * 37 + run) % 9000;
            // Surrounding prose, stable across runs and varied per document.
            //
            // Not decoration: without it the documents are ~150 characters, so
            // inserting a four-digit decoy moves the TEXT fingerprint by 23% and
            // the detector's content-change escape hatch (`d_text` high while
            // `d_dom` is low) fires and suppresses the very rebind signal the
            // duplicate-node class exists to measure. A corpus whose documents
            // are all body and no page is not a corpus — it measures the
            // fingerprint's sensitivity to document size, not the detector.
            let prose = PROSE.repeat(3 + i % 3);
            let body = format!(
                "<html><body><div class=\"page card-1a2b3c4d\">\
                 <h1 class=\"title\">Widget {i}</h1>\
                 <div class=\"card\">\
                 <span class=\"price\" data-value=\"{price}\">{price}</span>\
                 <span class=\"sku\">{key}</span>\
                 </div><div class=\"prose\">{prose}</div></div></body></html>"
            );
            (key, body)
        })
        .collect()
}

/// The rule set the fixture corpus is extracted with.
pub fn fixture_rules() -> crate::extract::RuleSet {
    // Deliberately a MIX of binding styles, because the taxonomy's expectations
    // are relative to how a rule is bound: a tag change only breaks a
    // tag-qualified rule, and a wrapper insertion only breaks a rule that
    // encodes the layout (here, the child combinator on `sku`). A corpus whose
    // rules were all `.class` selectors would be measuring a much easier
    // problem and would report a much better number for it.
    serde_json::from_value(serde_json::json!({
        "title": {"type": "css", "selector": "h1.title"},
        "price": {"type": "css", "selector": "span.price", "transforms": [{"op": "to_number"}]},
        "sku": {"type": "css", "selector": "div.card > span.sku"},
    }))
    .expect("fixture rules parse")
}

// ── The harness ─────────────────────────────────────────────────────────────

/// One run's worth of the detector's inputs, kept so the next run can compute
/// drift against it and so a baseline can be pooled from several.
struct RunObservation {
    sketches: BTreeMap<String, crate::resilience::sketch::FieldSketch>,
    values: Vec<Value>,
    signals: Vec<crate::resilience::DocSignals>,
}

fn observe_run(rules: &CompiledRuleSet, bodies: &[String]) -> RunObservation {
    let pairs = extract_batch_with_report(rules, bodies);
    let sketches = sketch_run(pairs.iter().map(|(v, r)| (v, r)));
    let signals = pairs
        .iter()
        .zip(bodies)
        .map(|((v, _), b)| doc_signals(b, v))
        .collect();
    RunObservation {
        sketches,
        values: pairs.into_iter().map(|(v, _)| v).collect(),
        signals,
    }
}

/// Median per-document drift between two runs of the SAME keys — the pure twin
/// of `HealthStore::cohort_drift`, which reads the fingerprints from SQLite.
fn drift_between(before: &RunObservation, after: &RunObservation) -> Option<CohortDrift> {
    if before.signals.len() != after.signals.len() || before.signals.is_empty() {
        return None;
    }
    let (mut text, mut dom, mut value) = (Vec::new(), Vec::new(), Vec::new());
    for (b, a) in before.signals.iter().zip(&after.signals) {
        text.push(simhash::drift(b.text_simhash, a.text_simhash));
        dom.push(simhash::drift(b.dom_simhash, a.dom_simhash));
        value.push(simhash::drift(b.val_simhash, a.val_simhash));
    }
    Some(CohortDrift {
        text: sketch::median(&text).unwrap_or(0.0),
        dom: sketch::median(&dom).unwrap_or(0.0),
        value: sketch::median(&value).unwrap_or(0.0),
        compared: text.len() as u32,
    })
}

/// What one mutation class scored at one cohort size.
#[derive(Debug, Clone, Serialize)]
pub struct ClassResult {
    pub class: MutationClass,
    pub cohort: usize,
    /// Whether the detector called the mutated run broken.
    pub fired: bool,
    pub score: f64,
    pub verdict: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnosis: Option<Diagnosis>,
    /// `true` when the outcome matches [`MutationClass::should_fire`].
    pub correct: bool,
}

/// The harness verdict over every class at every cohort size.
#[derive(Debug, Clone, Serialize)]
pub struct EvalReport {
    pub results: Vec<ClassResult>,
    /// Detection rate over the hard-break classes, **at judged cohorts only**.
    pub hard_recall: f64,
    /// Detection rate over the silent-corruption classes, at judged cohorts.
    pub silent_recall: f64,
    /// Share of negative-control runs that fired, over EVERY cohort. The
    /// binding constraint, and the one rate that is not restricted to judged
    /// cohorts: a spurious quarantine on a thin source is still a spurious
    /// quarantine.
    pub false_positive_rate: f64,
    /// Cohort sizes measured.
    pub cohorts: Vec<usize>,
    /// Cohort sizes at or above `min_cohort_docs`, i.e. the ones the
    /// distributional tests actually applied to. Recall is measured over these
    /// and only these, because the design's targets are stated "at cohort >= 30"
    /// — folding a below-floor cohort into the average would report a source
    /// that is honestly **unmonitored** as a detector that missed.
    pub judged_cohorts: Vec<usize>,
    /// Cohort sizes below the floor: measured, reported, and excluded from
    /// recall. Never silently dropped — "we did not judge this" is a result.
    pub unmonitored_cohorts: Vec<usize>,
    /// Baseline runs pooled before the mutation was applied.
    pub baseline_runs: usize,
}

impl EvalReport {
    /// Whether the report clears the design's §12.1 targets.
    ///
    /// Returns the failing lines rather than a bare bool: a harness that can
    /// only say "no" is a harness nobody uses to tune a threshold.
    pub fn findings(&self, targets: &EvalTargets) -> Vec<String> {
        let mut out = Vec::new();
        if self.hard_recall + f64::EPSILON < targets.hard_recall {
            out.push(format!(
                "hard-break recall {:.3} < target {:.3}",
                self.hard_recall, targets.hard_recall
            ));
        }
        if self.silent_recall + f64::EPSILON < targets.silent_recall {
            out.push(format!(
                "silent-corruption recall {:.3} < target {:.3}",
                self.silent_recall, targets.silent_recall
            ));
        }
        if self.false_positive_rate > targets.false_positive_rate + f64::EPSILON {
            out.push(format!(
                "false-positive rate {:.3} > ceiling {:.3}",
                self.false_positive_rate, targets.false_positive_rate
            ));
        }
        out
    }
}

/// The §12.1 targets, as data so the bin can print what it judged against.
#[derive(Debug, Clone, Copy)]
pub struct EvalTargets {
    pub hard_recall: f64,
    pub silent_recall: f64,
    pub false_positive_rate: f64,
}

impl Default for EvalTargets {
    fn default() -> Self {
        Self {
            hard_recall: 0.90,
            silent_recall: 0.50,
            // FPR is the binding constraint and recall is tuned against it: on
            // an unattended box a false quarantine that stops a working
            // pipeline is worse than a detection that arrives late.
            false_positive_rate: 0.0,
        }
    }
}

/// Runs the full mutation harness over the synthetic corpus.
///
/// For each cohort size: pool `baseline_runs` clean runs into a [`Baseline`],
/// mine invariants from their records, then apply each mutation class to a
/// fresh run of the same keys and score it with the real
/// [`detect::evaluate`].
pub fn evaluate_corpus(
    cfg: &ResilienceConfig,
    cohorts: &[usize],
    baseline_runs: usize,
) -> EvalReport {
    let rules = fixture_rules().compile().expect("fixture rules compile");
    let mutation = Mutation::new(MutationClass::None, "price", "sku");
    let mut results = Vec::new();

    for &cohort in cohorts {
        // Baseline: several clean runs of the same keys, exactly as the store
        // would have pooled them (only `ok` runs are baseline material).
        let mut baseline = Baseline::default();
        let mut previous: Option<RunObservation> = None;
        let mut baseline_records: Vec<Value> = Vec::new();
        for run in 0..baseline_runs as u64 {
            let bodies: Vec<String> = fixture_corpus(cohort, run)
                .into_iter()
                .map(|(_, b)| b)
                .collect();
            let obs = observe_run(&rules, &bodies);
            for (field, sketch) in &obs.sketches {
                baseline
                    .fields
                    .entry(field.clone())
                    .or_default()
                    .push(sketch.clone());
            }
            baseline_records.clone_from(&obs.values);
            previous = Some(obs);
        }
        let fields: Vec<String> = baseline.fields.keys().cloned().collect();
        let mined = invariants::mine(cfg, &baseline_records, &fields);
        let previous = previous.expect("at least one baseline run");

        for class in MutationClass::all() {
            let m = Mutation {
                class,
                ..mutation.clone()
            };
            // The mutated run replays the LAST BASELINE RUN's content, so the
            // only difference between the two runs is the mutation. Advancing
            // the content as well would move the words on every class at once
            // and quietly measure "mutation OR content change" — which is the
            // one confound the whole text-blind/structure-blind asymmetry
            // exists to separate.
            let bodies: Vec<String> =
                fixture_corpus(cohort, baseline_runs.saturating_sub(1) as u64)
                    .into_iter()
                    .map(|(_, b)| m.apply(&b))
                    .collect();
            let obs = observe_run(&rules, &bodies);
            let checks = invariants::check(&mined, obs.values.iter(), &obs.sketches);
            let eval = detect::evaluate(
                cfg,
                &RunInput {
                    docs: bodies.len() as u32,
                    fetch: FetchHealth {
                        attempted: bodies.len() as u32,
                        ok: bodies.len() as u32,
                    },
                    sketches: &obs.sketches,
                    baseline: &baseline,
                    invariants: &checks,
                    drift: drift_between(&previous, &obs),
                },
            );
            let fired = matches!(eval.verdict, RunVerdict::Broken | RunVerdict::SelfInflicted);
            results.push(ClassResult {
                class,
                cohort,
                fired,
                score: eval.score,
                verdict: eval.verdict.as_str(),
                diagnosis: eval.diagnosis,
                correct: fired == class.should_fire(),
            });
        }
    }

    let floor = cfg.min_cohort_docs as usize;
    let judged: Vec<usize> = cohorts.iter().copied().filter(|&c| c >= floor).collect();
    let unmonitored: Vec<usize> = cohorts.iter().copied().filter(|&c| c < floor).collect();
    let rate = |f: &dyn Fn(&ClassResult) -> bool| {
        let subset: Vec<&ClassResult> = results.iter().filter(|r| f(r)).collect();
        if subset.is_empty() {
            return 0.0;
        }
        subset.iter().filter(|r| r.fired).count() as f64 / subset.len() as f64
    };
    EvalReport {
        hard_recall: rate(&|r| r.class.should_fire() && !r.class.silent() && r.cohort >= floor),
        silent_recall: rate(&|r| r.class.silent() && r.cohort >= floor),
        // Every cohort, deliberately: see the field's doc.
        false_positive_rate: rate(&|r| !r.class.should_fire()),
        cohorts: cohorts.to_vec(),
        judged_cohorts: judged,
        unmonitored_cohorts: unmonitored,
        baseline_runs,
        results,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ResilienceConfig {
        ResilienceConfig {
            min_cohort_docs: 10,
            window_runs: 10,
            invariant_min_support: 10,
            ..ResilienceConfig::default()
        }
    }

    #[test]
    fn every_mutation_actually_changes_the_markup_except_the_identity() {
        // A "mutation" that silently no-ops would report as a clean negative
        // control forever and quietly make the recall number a lie.
        let (_, body) = fixture_corpus(1, 0).remove(0);
        for class in MutationClass::all() {
            let out = Mutation::new(class, "price", "sku").apply(&body);
            if class == MutationClass::None {
                assert_eq!(out, body);
            } else {
                assert_ne!(out, body, "{} did not change anything", class.as_str());
            }
        }
    }

    #[test]
    fn a_build_hash_churn_moves_only_the_digest_not_the_stable_class() {
        let (_, body) = fixture_corpus(1, 0).remove(0);
        let churned = Mutation::new(MutationClass::BuildHashChurn, "price", "sku").apply(&body);
        assert!(churned.contains("card-9f8e7d6c"), "{churned}");
        assert!(!churned.contains("card-1a2b3c4d"), "{churned}");
        // The classes the rules bind to are untouched — that is what makes this
        // a negative control rather than a rename.
        assert!(churned.contains("class=\"price\""), "{churned}");
        assert!(churned.contains("class=\"sku\""), "{churned}");
    }

    #[test]
    fn a_text_only_change_keeps_the_markup_byte_identical() {
        let (_, body) = fixture_corpus(1, 0).remove(0);
        let changed = Mutation::new(MutationClass::TextOnlyChange, "price", "sku").apply(&body);
        let strip = |s: &str| {
            s.split('>')
                .map(|seg| seg.split('<').next().unwrap_or("").to_string())
                .collect::<Vec<_>>()
                .join("|")
        };
        assert_ne!(strip(&body), strip(&changed), "the words must move");
        // Same tags, same classes, same order: only text nodes differ.
        let tags = |s: &str| {
            s.match_indices('<')
                .map(|(i, _)| s[i..].split('>').next().unwrap_or("").to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(tags(&body), tags(&changed));
    }

    #[test]
    fn a_deleted_field_stops_extracting_and_a_renamed_one_does_too() {
        let rules = fixture_rules().compile().unwrap();
        let (_, body) = fixture_corpus(1, 0).remove(0);
        for class in [MutationClass::NodeDeletion, MutationClass::ClassRename] {
            let out = Mutation::new(class, "price", "sku").apply(&body);
            let v = crate::extract::extract_one(&rules, &out);
            assert!(
                v["price"].is_null(),
                "{} left the field extracting: {v}",
                class.as_str()
            );
        }
    }

    #[test]
    fn a_duplicate_node_collapses_the_field_to_a_constant_not_to_nothing() {
        // The dangerous shape: the selector still matches, every counter green,
        // and the dataset fills with the same plausible value on every record.
        let rules = fixture_rules().compile().unwrap();
        let m = Mutation::new(MutationClass::DuplicateNode, "price", "sku");
        let values: Vec<Value> = fixture_corpus(5, 0)
            .into_iter()
            .map(|(_, b)| crate::extract::extract_one(&rules, &m.apply(&b)))
            .collect();
        let distinct: std::collections::HashSet<String> =
            values.iter().map(|v| v["price"].to_string()).collect();
        assert_eq!(distinct.len(), 1, "{values:?}");
        assert!(!values[0]["price"].is_null());
    }

    #[test]
    fn the_negative_controls_do_not_fire_and_the_hard_breaks_do() {
        // The measurement `IMPLEMENTATION-NOTES.md` says has never been taken.
        // A regression in `dom_simhash`, the sketch or a threshold shows up
        // here as a number, not as silence in production.
        let report = evaluate_corpus(&cfg(), &[30], 4);
        let sheet = || {
            report
                .results
                .iter()
                .map(|r| {
                    format!(
                        "{}@{} fired={} verdict={} score={:.3}",
                        r.class.as_str(),
                        r.cohort,
                        r.fired,
                        r.verdict,
                        r.score
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        // Per-class recall is measured and reported, NOT asserted class by
        // class: the design's own targets are aggregate and asymmetric (0.90
        // for hard breaks, 0.50 for silent corruption, because a swap of two
        // same-shaped fields is genuinely not always detectable). Asserting
        // every class fires would be asserting a claim the design explicitly
        // refuses to make.
        assert!(
            report.findings(&EvalTargets::default()).is_empty(),
            "{:?}\n{}",
            report.findings(&EvalTargets::default()),
            sheet()
        );
        // The one hard per-class assertion: a negative control that fires is a
        // false quarantine, and FPR is the binding constraint.
        assert_eq!(report.false_positive_rate, 0.0, "{}", sheet());
        for r in report.results.iter().filter(|r| !r.class.should_fire()) {
            assert!(!r.fired, "{} fired: {}", r.class.as_str(), sheet());
        }
        assert!(report.hard_recall >= 0.9, "{}", sheet());
    }

    #[test]
    fn a_below_floor_cohort_is_unmonitored_not_a_missed_detection() {
        // Cohort 5 is under `min_cohort_docs`, so every run there is
        // `below_cohort` — the detector said nothing, honestly. Folding those
        // rows into recall would report an UNMONITORED source as a detector
        // that missed, and would make the whole number a function of which
        // cohort sizes somebody happened to pass on the command line.
        let with_thin = evaluate_corpus(&cfg(), &[5, 30], 4);
        let judged_only = evaluate_corpus(&cfg(), &[30], 4);
        assert_eq!(with_thin.unmonitored_cohorts, vec![5]);
        assert_eq!(with_thin.judged_cohorts, vec![30]);
        assert_eq!(with_thin.hard_recall, judged_only.hard_recall);
        assert_eq!(with_thin.silent_recall, judged_only.silent_recall);
        // The thin rows are still reported, never dropped.
        assert!(with_thin.results.iter().any(|r| r.cohort == 5));
        // FPR, by contrast, spans every cohort: a spurious quarantine on a thin
        // source is still a spurious quarantine.
        assert_eq!(with_thin.false_positive_rate, 0.0);
    }

    #[test]
    fn a_harness_that_cannot_go_red_is_not_a_harness() {
        // The fail-before. With the trip threshold pushed past 1.0 the weighted
        // score can never reach it, so every signal that is *scored* goes
        // silent and the report SAYS SO. A gate that stays green under a
        // deliberately broken detector proves nothing.
        //
        // Note what survives: the assumption-free total-collapse override is
        // not score-gated, so the hard breaks that collapse a field outright
        // are still caught. That asymmetry is itself worth pinning — it is the
        // difference between the detector's floor and its statistics.
        let blind = ResilienceConfig {
            degrade_score: 2.0,
            quarantine_score: 2.0,
            ..cfg()
        };
        let report = evaluate_corpus(&blind, &[30], 4);
        let findings = report.findings(&EvalTargets::default());
        assert!(!findings.is_empty(), "a blinded detector reported clean");
        assert_eq!(report.silent_recall, 0.0, "{:?}", report.results);
        // …and the floor still holds: a field that vanishes entirely is caught
        // with no thresholds at all.
        assert!(report.hard_recall > 0.0, "{:?}", report.results);
    }
}
