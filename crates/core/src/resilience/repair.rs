//! The seven validation gates a repair candidate must pass
//! (`resilient-extraction.md` §6.4), each as an extracted, tested predicate.
//!
//! # Why gates and not a score
//!
//! A candidate rule set is a *search result*, not a judgement: the task posed is
//! "find the rule that produces THESE known values from THIS markup", which has
//! an answer key, so every check here is deterministic code and none of them
//! asks a model whether its own proposal is good.
//!
//! They are **gates, not weights**. A weighted score lets a candidate that fails
//! the distinctness invariant buy its way back with a high holdout match rate —
//! and "binds to a site-wide constant, matches 100%" is precisely the failure
//! shape the design is built to refuse. Every gate must pass.
//!
//! # The seven
//!
//! 1. [`gate_compiles`] — bad CSS/regex/XPath, all field errors at once.
//! 2. [`gate_lint`] — brittle selectors (`induce::lint_selector` + breadth).
//! 3. [`gate_holdout`] — match rate on documents the candidate never saw.
//! 4. [`gate_golden`] — exact on stable fields, shape on churny ones.
//! 5. [`gate_agreement`] — independent candidates agreeing on *output*.
//! 6. [`gate_invariants`] — the source's own mined regularities still hold.
//! 7. [`gate_no_regression`] — no healthy field is worse than it was.
//!
//! [`judge`] runs all seven and returns one [`CandidateVerdict`], which is what
//! `repair_candidates` stores — so a rejected repair is exactly as auditable as
//! a promoted one.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;

use crate::extract::{CompiledRuleSet, Rule, RuleSet};
use crate::induce::{lint_selector, lint_selector_breadth, LintFinding, MAX_SELECTOR_BREADTH};

use super::invariants::{check as check_invariants, Invariant};
use super::sketch::{value_text, FieldSketch};

/// Holdout match rate a candidate must reach on documents it never saw.
pub const MIN_HOLDOUT_MATCH_RATE: f64 = 0.9;
/// How far below the live rules' own healthy rate a candidate may sit.
pub const HOLDOUT_BASELINE_SLACK: f64 = 0.05;
/// Candidates that must produce identical holdout output before any of them is
/// trusted. Two *different* selectors arriving at the same values is stronger
/// evidence than two identical selectors: it means two independent searches
/// found the same element.
pub const DEFAULT_AGREEMENT_MIN: usize = 2;
/// Points a previously-healthy field's match rate may drop by.
pub const NO_REGRESSION_TOLERANCE: f64 = 0.02;

// ── Gate 1: compiles ────────────────────────────────────────────────────────

/// One field's compile error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldError {
    pub field: String,
    pub error: String,
}

/// Compiles the candidate, reporting **every** bad field rather than the first.
///
/// The one-error-at-a-time shape is what makes a repair loop iterate blind: a
/// candidate with three bad selectors takes three round trips to find out, and
/// the `POST /extract/preview` surface already set the precedent that a rule
/// set reports all of its field errors at once.
pub fn gate_compiles(rules: &RuleSet) -> Result<CompiledRuleSet, Vec<FieldError>> {
    let mut errors = Vec::new();
    for (field, rule) in &rules.fields {
        let single = RuleSet {
            fields: BTreeMap::from([(field.clone(), rule.clone())]),
        };
        if let Err(e) = single.compile() {
            errors.push(FieldError {
                field: field.clone(),
                error: e.to_string(),
            });
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    rules.compile().map_err(|e| {
        vec![FieldError {
            field: "*".into(),
            error: e.to_string(),
        }]
    })
}

// ── Gate 2: brittle-selector lint ───────────────────────────────────────────

/// Lints every CSS selector in the candidate — shape first, then breadth
/// against the documents it will actually run on.
///
/// A `const` rule for a field the source has always varied is included here
/// rather than in the invariant gate: it is a property of the *proposal*, and
/// it is a model's favourite way to make a test pass.
pub fn gate_lint(rules: &RuleSet, docs: &[String], dynamic_fields: &[String]) -> Vec<LintFinding> {
    let mut out = Vec::new();
    let dynamic: BTreeSet<&str> = dynamic_fields.iter().map(String::as_str).collect();
    for (field, fr) in &rules.fields {
        match &fr.rule {
            Rule::Css { selector, .. } | Rule::Each { selector, .. } => {
                out.extend(lint_selector(selector));
                out.extend(lint_selector_breadth(selector, docs, MAX_SELECTOR_BREADTH));
            }
            Rule::Const { .. } if dynamic.contains(field.as_str()) => {
                out.push(LintFinding {
                    selector: format!("<const {field}>"),
                    rule: "const_for_dynamic_field",
                    detail: format!(
                        "`{field}` has always varied on this source; a constant \
                         reproduces the sample and learns nothing"
                    ),
                });
            }
            _ => {}
        }
    }
    out
}

// ── Gate 3: held-out match rate ─────────────────────────────────────────────

/// The candidate's score on documents it was never shown.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct HoldoutScore {
    pub matched: usize,
    pub total: usize,
    pub rate: f64,
}

impl HoldoutScore {
    pub fn passes(&self, baseline_rate: f64) -> bool {
        self.total > 0
            && self.rate + f64::EPSILON >= MIN_HOLDOUT_MATCH_RATE
            && self.rate + HOLDOUT_BASELINE_SLACK + f64::EPSILON >= baseline_rate
    }
}

/// Scores `produced` against `expected` field by field, document by document.
///
/// **Positional**: `produced[i]` and `expected[i]` are the same document. A
/// document with no expected value for a field contributes nothing rather than
/// counting as a match — a candidate must not be able to score by being asked
/// fewer questions.
pub fn gate_holdout(
    produced: &[Value],
    expected: &[BTreeMap<String, String>],
    fields: &[String],
) -> HoldoutScore {
    let mut matched = 0usize;
    let mut total = 0usize;
    for (got, want) in produced.iter().zip(expected) {
        for field in fields {
            let Some(w) = want.get(field).filter(|v| !v.trim().is_empty()) else {
                continue;
            };
            total += 1;
            if value_text(got.get(field).unwrap_or(&Value::Null)) == *w {
                matched += 1;
            }
        }
    }
    HoldoutScore {
        matched,
        total,
        rate: if total == 0 {
            0.0
        } else {
            matched as f64 / total as f64
        },
    }
}

// ── Gate 4: golden documents ────────────────────────────────────────────────

/// One pinned document's expected values, and which of its fields are stable
/// enough to be matched exactly.
#[derive(Debug, Clone, Default)]
pub struct GoldenDoc {
    pub key: String,
    pub expected: BTreeMap<String, String>,
    /// Fields whose historical churn is under 5% — matched byte for byte.
    /// Everything else is matched by *shape*.
    pub stable_fields: BTreeSet<String>,
}

/// Golden-set outcome. `exact_mismatches` is an immediate reject; a shape
/// mismatch is reported and also rejects, but is distinguished so the audit
/// trail says which kind of wrong it was.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GoldenScore {
    pub checked: usize,
    pub exact_ok: usize,
    pub exact_mismatches: Vec<String>,
    pub shape_mismatches: Vec<String>,
}

impl GoldenScore {
    pub fn passes(&self) -> bool {
        self.exact_mismatches.is_empty() && self.shape_mismatches.is_empty()
    }
}

/// Checks the candidate's output on the pinned documents.
///
/// An **empty golden set does not pass** — it returns `checked: 0`, and
/// [`judge`] treats that as "this gate could not run", never as a pass. Golden
/// docs are the only anchor against baseline poisoning, so silently skipping
/// them would remove the one check that does not drift with the source.
pub fn gate_golden(produced: &[Value], golden: &[GoldenDoc]) -> GoldenScore {
    let mut score = GoldenScore {
        checked: 0,
        exact_ok: 0,
        exact_mismatches: Vec::new(),
        shape_mismatches: Vec::new(),
    };
    for (got, doc) in produced.iter().zip(golden) {
        for (field, want) in &doc.expected {
            score.checked += 1;
            let have = value_text(got.get(field).unwrap_or(&Value::Null));
            if doc.stable_fields.contains(field) {
                if have == *want {
                    score.exact_ok += 1;
                } else {
                    score
                        .exact_mismatches
                        .push(format!("{}/{field}: want `{want}`, got `{have}`", doc.key));
                }
            } else if !same_shape(&have, want) {
                score.shape_mismatches.push(format!(
                    "{}/{field}: shape moved (`{want}` → `{have}`)",
                    doc.key
                ));
            }
        }
    }
    score
}

/// Shape equality for a churny field: same emptiness, same length band, same
/// character-class profile. Deliberately coarse — it exists to catch "a price
/// became a paragraph", not to police a digit.
pub fn same_shape(a: &str, b: &str) -> bool {
    a.is_empty() == b.is_empty() && len_band(a) == len_band(b) && char_class(a) == char_class(b)
}

/// Log-ish length bands: 0, 1–8, 9–32, 33–128, 129–512, larger.
fn len_band(s: &str) -> u8 {
    match s.chars().count() {
        0 => 0,
        1..=8 => 1,
        9..=32 => 2,
        33..=128 => 3,
        129..=512 => 4,
        _ => 5,
    }
}

/// Which character classes the value uses at all: (digits, letters, other).
fn char_class(s: &str) -> (bool, bool, bool) {
    let mut out = (false, false, false);
    for c in s.chars() {
        if c.is_ascii_digit() {
            out.0 = true;
        } else if c.is_alphabetic() {
            out.1 = true;
        } else if !c.is_whitespace() {
            out.2 = true;
        }
    }
    out
}

// ── Gate 5: output agreement ────────────────────────────────────────────────

/// Agreement outcome: which group this candidate landed in and how big it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgreementScore {
    pub group: usize,
    pub group_size: usize,
    pub groups: usize,
}

impl AgreementScore {
    pub fn passes(&self, agreement_min: usize) -> bool {
        self.group_size >= agreement_min.max(1)
    }
}

/// Groups candidates by the **values they produce** on the holdout set, not by
/// the rules they propose.
///
/// This is the whole point of the gate: two different selectors arriving at the
/// same values means two independent searches found the same element, which is
/// stronger evidence than two identical proposals. Returns one score per
/// candidate, in input order; group ids are assigned in first-seen order so the
/// output is deterministic.
pub fn gate_agreement(candidate_outputs: &[Vec<Value>]) -> Vec<AgreementScore> {
    let mut order: Vec<String> = Vec::new();
    let mut assigned: Vec<usize> = Vec::with_capacity(candidate_outputs.len());
    for out in candidate_outputs {
        let key = serde_json::to_string(out).unwrap_or_default();
        let idx = match order.iter().position(|k| *k == key) {
            Some(i) => i,
            None => {
                order.push(key);
                order.len() - 1
            }
        };
        assigned.push(idx);
    }
    let mut sizes = vec![0usize; order.len()];
    for &g in &assigned {
        sizes[g] += 1;
    }
    assigned
        .into_iter()
        .map(|g| AgreementScore {
            group: g,
            group_size: sizes[g],
            groups: order.len(),
        })
        .collect()
}

// ── Gate 6: invariant re-check ──────────────────────────────────────────────

/// Invariants the candidate's own output breaks, as `field:kind` labels.
///
/// Includes the distinctness invariant, which is what stops a "repair" that
/// binds to a site-wide constant and scores a perfect match rate on a holdout
/// set whose expected values happen to be that constant.
pub fn gate_invariants(
    invariants: &[Invariant],
    produced: &[Value],
    sketches: &BTreeMap<String, FieldSketch>,
    violation_ratio: f64,
) -> Vec<String> {
    check_invariants(invariants, produced.iter(), sketches)
        .into_iter()
        .filter(|c| c.checked > 0 && (c.broke as f64 / c.checked as f64) >= violation_ratio)
        .map(|c| format!("{}:{}", c.field, c.kind))
        .collect()
}

// ── Gate 7: no regression ───────────────────────────────────────────────────

/// Fields that were healthy under the live rules and are worse under the
/// candidate. A repair for one field must not cost another.
pub fn gate_no_regression(
    candidate_rates: &BTreeMap<String, f64>,
    live_rates: &BTreeMap<String, f64>,
    broken_fields: &[String],
) -> Vec<String> {
    let broken: BTreeSet<&str> = broken_fields.iter().map(String::as_str).collect();
    let mut out = Vec::new();
    for (field, live) in live_rates {
        if broken.contains(field.as_str()) {
            continue;
        }
        let got = candidate_rates.get(field).copied().unwrap_or(0.0);
        if got + NO_REGRESSION_TOLERANCE + f64::EPSILON < *live {
            out.push(format!("{field}: {got:.3} < live {live:.3}"));
        }
    }
    out
}

// ── The verdict ─────────────────────────────────────────────────────────────

/// Everything the seven gates said about one candidate — the row
/// `repair_candidates` stores.
#[derive(Debug, Clone, Serialize)]
pub struct CandidateVerdict {
    pub accepted: bool,
    /// `None` when accepted; otherwise the FIRST gate that refused, as
    /// `rejected:<gate>`. Gates are ordered cheapest-first, so the reason is
    /// also the cheapest true explanation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejected_for: Option<String>,
    /// Every gate's own finding, kept whether or not it decided the verdict.
    pub compile_errors: Vec<FieldError>,
    pub lint: Vec<LintFinding>,
    pub holdout: HoldoutScore,
    pub golden: GoldenScore,
    pub agreement: AgreementScore,
    pub invariant_violations: Vec<String>,
    pub regressions: Vec<String>,
}

/// Everything [`judge`] needs about one candidate. Assembled by the caller
/// because producing it means running extractions, which is the caller's job.
pub struct CandidateEvidence<'a> {
    pub compile_errors: Vec<FieldError>,
    pub lint: Vec<LintFinding>,
    pub holdout: HoldoutScore,
    /// The live rules' own holdout rate on the same fields — the bar the
    /// candidate must not fall meaningfully below.
    pub baseline_rate: f64,
    pub golden: GoldenScore,
    pub agreement: AgreementScore,
    pub invariant_violations: Vec<String>,
    pub regressions: Vec<String>,
    pub agreement_min: usize,
    /// Whether a golden set existed at all. `false` means gate 4 could not run,
    /// which is a rejection, not a pass.
    pub golden_available: bool,
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Runs the seven gates in order and returns the verdict.
///
/// Order is cheapest-first, and the first refusal wins: a candidate that does
/// not compile has nothing to say about holdout rates, and reporting six
/// downstream failures caused by one broken selector buries the cause.
pub fn judge(e: CandidateEvidence<'_>) -> CandidateVerdict {
    let rejected_for = if !e.compile_errors.is_empty() {
        Some("rejected:compile".to_string())
    } else if !e.lint.is_empty() {
        Some(format!("rejected:lint:{}", e.lint[0].rule))
    } else if !e.holdout.passes(e.baseline_rate) {
        Some("rejected:holdout".to_string())
    } else if !e.golden_available {
        Some("rejected:golden_missing".to_string())
    } else if !e.golden.passes() {
        Some("rejected:golden".to_string())
    } else if !e.agreement.passes(e.agreement_min) {
        Some("rejected:agreement".to_string())
    } else if !e.invariant_violations.is_empty() {
        Some(format!("rejected:invariant:{}", e.invariant_violations[0]))
    } else if !e.regressions.is_empty() {
        Some("rejected:regression".to_string())
    } else {
        None
    };
    CandidateVerdict {
        accepted: rejected_for.is_none(),
        rejected_for,
        compile_errors: e.compile_errors,
        lint: e.lint,
        holdout: e.holdout,
        golden: e.golden,
        agreement: e.agreement,
        invariant_violations: e.invariant_violations,
        regressions: e.regressions,
    }
}

// ── Promotion, probation and rollback (§8.1–8.3) ────────────────────────────
//
// The state machine is pure and lives here rather than in the `repair` app,
// because "when may a rule set become the live one" is the single most
// dangerous decision in this subsystem and it must be testable without a job,
// a database or a clock.

/// The reason a source is or is not allowed another repair attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum PromotionDecision {
    /// Promote this version now.
    Promote { version: i64 },
    /// Keep shadowing: the candidate is clean but has not earned the streak.
    Wait { clean_runs: u32, needed: u32 },
    /// The candidate stopped being valid — drop it and start over.
    Drop { reason: String },
    /// Repair may not act at all right now (mode, cooldown, budget).
    Blocked { reason: String },
}

/// Everything [`decide_promotion`] reads. Assembled by the caller from the
/// source row, the candidate row and this run's gate verdict.
#[derive(Debug, Clone)]
pub struct PromotionInput {
    /// The candidate's `profile_versions` number.
    pub candidate_version: i64,
    /// The diagnosis the candidate was generated for.
    pub candidate_diagnosis_hash: String,
    /// The diagnosis the source has RIGHT NOW.
    pub current_diagnosis_hash: String,
    /// Consecutive clean shadow runs already banked, this run excluded.
    pub clean_runs: u32,
    /// Whether the candidate cleared all seven gates on THIS run.
    pub candidate_clean: bool,
    /// Whether the LIVE rules failed at least one gate on this run. Promotion
    /// requires this every run: replacing rules that are working is not a
    /// repair, it is a coin flip with the dataset.
    pub live_failing: bool,
    /// Promotions this source has already had inside the 30-day window.
    pub promotions_30d: u32,
    /// Whether `now` is still inside the post-rollback cooldown.
    pub blocked_by_cooldown: bool,
}

/// Decides what to do with a shadow candidate after one run.
///
/// The rule the design states and this encodes: promote only when, across the
/// whole shadow window, the candidate clears every gate **every run** and the
/// live rules fail at least one **every run**. Anything less decisive is not a
/// promotion — it is a source that stays quarantined and keeps alerting.
pub fn decide_promotion(
    cfg: &crate::config::RepairConfig,
    i: &PromotionInput,
) -> PromotionDecision {
    if !cfg.may_promote() {
        return PromotionDecision::Blocked {
            reason: if cfg.enabled {
                format!("repair mode `{}` never promotes", cfg.mode)
            } else {
                "repair disabled".to_string()
            },
        };
    }
    if i.blocked_by_cooldown {
        return PromotionDecision::Blocked {
            reason: "cooldown after a rollback".to_string(),
        };
    }
    if i.promotions_30d >= cfg.max_promotions_30d {
        return PromotionDecision::Blocked {
            reason: "repair budget exhausted".to_string(),
        };
    }
    // A candidate is an answer to ONE diagnosis. If the source has moved on —
    // a second redesign, a different broken field — the old candidate is a
    // solution to a problem nobody has any more, and promoting it would write
    // rules derived from markup that is already gone.
    if i.candidate_diagnosis_hash != i.current_diagnosis_hash {
        return PromotionDecision::Drop {
            reason: "stale candidate: the diagnosis moved".to_string(),
        };
    }
    if !i.candidate_clean {
        return PromotionDecision::Drop {
            reason: "candidate failed a gate during probation".to_string(),
        };
    }
    if !i.live_failing {
        return PromotionDecision::Drop {
            reason: "live rules are passing: nothing to repair".to_string(),
        };
    }
    let clean = i.clean_runs + 1;
    let needed = if cfg.promotes_immediately() {
        1
    } else {
        cfg.probation_runs.max(1)
    };
    if clean >= needed {
        PromotionDecision::Promote {
            version: i.candidate_version,
        }
    } else {
        PromotionDecision::Wait {
            clean_runs: clean,
            needed,
        }
    }
}

/// What a `probation` run does to the promoted version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ProbationOutcome {
    /// The run was clean: stay on the promoted version.
    Hold,
    /// The run tripped: revert the pointer immediately.
    Rollback { to_version: i64 },
    /// The run tripped and there is nowhere to go back to.
    CannotRollback { reason: String },
}

/// Auto-rollback (§8.2): **any** tripped probation run reverts
/// `active_version` to the promoted version's `parent_version`, instantly.
///
/// One tripped run, not a streak. The asymmetry with the ladder's
/// three-run hysteresis is deliberate and is the whole safety argument for
/// promoting at all: climbing costs evidence, falling back costs nothing, so
/// the expensive mistake (a bad version writing to the live dataset) has the
/// shortest possible life.
pub fn probation_outcome(run_tripped: bool, parent_version: Option<i64>) -> ProbationOutcome {
    if !run_tripped {
        return ProbationOutcome::Hold;
    }
    match parent_version {
        Some(v) => ProbationOutcome::Rollback { to_version: v },
        // A promoted version with no parent is a bug in whoever wrote it, and
        // reverting to "nothing" would leave the source with no rules at all.
        None => ProbationOutcome::CannotRollback {
            reason: "promoted version has no parent to revert to".to_string(),
        },
    }
}

/// The idempotency key for one repair attempt: at most one in-flight attempt
/// per source per diagnosis, reusing the job queue's existing mechanism.
///
/// The diagnosis is part of the key on purpose. Keying on the source alone
/// would make a source that breaks a second way un-repairable until the first
/// attempt aged out; keying on the job would make every scheduled run start a
/// duplicate attempt for the same break.
pub fn repair_idempotency_key(source_id: &str, diagnosis_hash: &str) -> String {
    format!("repair:{source_id}:{diagnosis_hash}")
}

/// A stable hash of what is currently wrong with a source — the diagnosis plus
/// the exact set of fields it names, order-independent.
///
/// The field set is in the hash because "markup_drift on `price`" and
/// "markup_drift on `price` and `sku`" are different problems needing different
/// candidates, and a key that could not tell them apart would let the first
/// attempt suppress the second.
pub fn diagnosis_hash(diagnosis: &str, broken_fields: &[String]) -> String {
    let mut fields: Vec<&str> = broken_fields.iter().map(String::as_str).collect();
    fields.sort_unstable();
    fields.dedup();
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(diagnosis.as_bytes());
    h.update(b"|");
    h.update(fields.join(",").as_bytes());
    format!("{:x}", h.finalize())[..16].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rules(v: Value) -> RuleSet {
        serde_json::from_value(v).unwrap()
    }

    fn evidence() -> CandidateEvidence<'static> {
        CandidateEvidence {
            compile_errors: Vec::new(),
            lint: Vec::new(),
            holdout: HoldoutScore {
                matched: 100,
                total: 100,
                rate: 1.0,
            },
            baseline_rate: 1.0,
            golden: GoldenScore {
                checked: 6,
                exact_ok: 6,
                exact_mismatches: Vec::new(),
                shape_mismatches: Vec::new(),
            },
            agreement: AgreementScore {
                group: 0,
                group_size: 2,
                groups: 2,
            },
            invariant_violations: Vec::new(),
            regressions: Vec::new(),
            agreement_min: DEFAULT_AGREEMENT_MIN,
            golden_available: true,
            _marker: std::marker::PhantomData,
        }
    }

    #[test]
    fn a_broken_ruleset_reports_every_bad_field_not_only_the_first() {
        let out = gate_compiles(&rules(json!({
            "a": {"type": "css", "selector": "div["},
            "b": {"type": "css", "selector": ".ok"},
            "c": {"type": "regex", "pattern": "([unclosed"},
        })));
        // `CompiledRuleSet` holds compiled selectors and is not `Debug`, so the
        // Ok arm is destructured rather than unwrapped.
        let Err(errs) = out else {
            panic!("a rule set with two bad fields must not compile");
        };
        let fields: Vec<&str> = errs.iter().map(|e| e.field.as_str()).collect();
        assert_eq!(fields, vec!["a", "c"], "{errs:?}");
    }

    #[test]
    fn a_const_rule_for_a_field_that_has_always_varied_is_linted_out() {
        // A model's favourite way to make a test pass: freeze the sample.
        let findings = gate_lint(
            &rules(json!({"price": {"type": "const", "value": "9.99"}})),
            &["<html><body><p>x</p></body></html>".to_string()],
            &["price".to_string()],
        );
        assert_eq!(
            findings.iter().map(|f| f.rule).collect::<Vec<_>>(),
            vec!["const_for_dynamic_field"]
        );
        // …and a const for a genuinely constant field is fine.
        assert!(gate_lint(
            &rules(json!({"currency": {"type": "const", "value": "USD"}})),
            &["<html/>".to_string()],
            &["price".to_string()],
        )
        .is_empty());
    }

    #[test]
    fn a_candidate_cannot_score_by_being_asked_fewer_questions() {
        // THE ANTI-PATTERN: counting "no expected value" as a match. A rule set
        // that produces nothing would then score 1.0 on a sparse holdout.
        let produced = vec![json!({"price": "10"}), json!({"price": null})];
        let expected = vec![
            BTreeMap::from([("price".to_string(), "10".to_string())]),
            BTreeMap::from([("price".to_string(), "20".to_string())]),
        ];
        let fields = vec!["price".to_string()];
        let s = gate_holdout(&produced, &expected, &fields);
        assert_eq!((s.matched, s.total), (1, 2));
        assert!(!s.passes(1.0));

        // A blank expected value asks nothing and counts as nothing.
        let sparse = vec![
            BTreeMap::from([("price".to_string(), "10".to_string())]),
            BTreeMap::from([("price".to_string(), "  ".to_string())]),
        ];
        let s = gate_holdout(&produced, &sparse, &fields);
        assert_eq!((s.matched, s.total), (1, 1));
        assert!(s.passes(1.0));
    }

    #[test]
    fn a_candidate_below_the_live_rules_own_rate_fails_even_at_ninety_percent() {
        let s = HoldoutScore {
            matched: 91,
            total: 100,
            rate: 0.91,
        };
        assert!(s.passes(0.95), "0.91 is within 0.05 of 0.95");
        assert!(!s.passes(0.99), "0.91 is not within 0.05 of 0.99");
        let low = HoldoutScore {
            matched: 89,
            total: 100,
            rate: 0.89,
        };
        assert!(!low.passes(0.5), "below the absolute floor regardless");
    }

    #[test]
    fn an_exact_field_mismatch_on_a_golden_doc_is_an_immediate_reject() {
        let golden = vec![GoldenDoc {
            key: "p1".into(),
            expected: BTreeMap::from([
                ("sku".to_string(), "ABC-1".to_string()),
                ("price".to_string(), "10.00".to_string()),
            ]),
            stable_fields: BTreeSet::from(["sku".to_string()]),
        }];
        // sku wrong → exact mismatch.
        let s = gate_golden(&[json!({"sku": "ABC-2", "price": "10.00"})], &golden);
        assert!(!s.passes());
        assert_eq!(s.exact_mismatches.len(), 1);
        // price moved but kept its shape → churn, not a break.
        let s = gate_golden(&[json!({"sku": "ABC-1", "price": "12.50"})], &golden);
        assert!(s.passes(), "{s:?}");
        // price became prose → shape mismatch.
        let s = gate_golden(
            &[json!({"sku": "ABC-1", "price": "Call us for a quote today"})],
            &golden,
        );
        assert!(!s.passes());
        assert_eq!(s.shape_mismatches.len(), 1);
    }

    #[test]
    fn agreement_groups_by_output_not_by_the_rules_that_produced_it() {
        // Two DIFFERENT selectors reaching the same values is the strong case,
        // and grouping by proposal instead of output would miss it entirely.
        let a = vec![json!({"price": "10"}), json!({"price": "20"})];
        let b = a.clone();
        let c = vec![json!({"price": "10"}), json!({"price": "99"})];
        let scores = gate_agreement(&[a, c, b]);
        assert_eq!(scores[0].group_size, 2);
        assert_eq!(scores[2].group_size, 2);
        assert_eq!(scores[1].group_size, 1);
        assert_eq!(scores[0].groups, 2);
        assert!(scores[0].passes(DEFAULT_AGREEMENT_MIN));
        assert!(!scores[1].passes(DEFAULT_AGREEMENT_MIN));
    }

    #[test]
    fn a_repair_that_costs_a_healthy_field_is_a_regression() {
        let live = BTreeMap::from([("price".into(), 0.2), ("title".into(), 0.99)]);
        let cand = BTreeMap::from([("price".into(), 0.98), ("title".into(), 0.80)]);
        let out = gate_no_regression(&cand, &live, &["price".to_string()]);
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].starts_with("title"), "{out:?}");
        // Within tolerance is not a regression.
        let cand = BTreeMap::from([("price".into(), 0.98), ("title".into(), 0.98)]);
        assert!(gate_no_regression(&cand, &live, &["price".to_string()]).is_empty());
    }

    #[test]
    fn a_missing_golden_set_is_not_a_pass() {
        // "Could not check" must never read as "fine": golden docs are the only
        // anchor that does not drift with the source, so skipping them silently
        // removes the defence against baseline poisoning.
        let v = judge(CandidateEvidence {
            golden_available: false,
            golden: GoldenScore {
                checked: 0,
                exact_ok: 0,
                exact_mismatches: Vec::new(),
                shape_mismatches: Vec::new(),
            },
            ..evidence()
        });
        assert!(!v.accepted);
        assert_eq!(v.rejected_for.as_deref(), Some("rejected:golden_missing"));
    }

    #[test]
    fn a_perfect_match_rate_does_not_buy_past_a_broken_invariant() {
        // The gates are gates, not weights. "Binds to a site-wide constant and
        // matches 100%" is exactly the shape a weighted score would promote.
        let v = judge(CandidateEvidence {
            invariant_violations: vec!["price:distinctness".into()],
            ..evidence()
        });
        assert!(!v.accepted);
        assert_eq!(
            v.rejected_for.as_deref(),
            Some("rejected:invariant:price:distinctness")
        );
    }

    #[test]
    fn a_clean_candidate_passes_all_seven() {
        let v = judge(evidence());
        assert!(v.accepted, "{v:?}");
        assert_eq!(v.rejected_for, None);
    }

    #[test]
    fn the_first_refusal_wins_so_the_reason_is_the_cause() {
        // A candidate that does not compile has nothing to say about holdout
        // rates; reporting six downstream failures buries the one cause.
        let v = judge(CandidateEvidence {
            compile_errors: vec![FieldError {
                field: "price".into(),
                error: "bad selector".into(),
            }],
            holdout: HoldoutScore {
                matched: 0,
                total: 100,
                rate: 0.0,
            },
            invariant_violations: vec!["price:nonnull".into()],
            ..evidence()
        });
        assert_eq!(v.rejected_for.as_deref(), Some("rejected:compile"));
        // …and the downstream evidence is still stored, not discarded.
        assert_eq!(v.holdout.rate, 0.0);
        assert_eq!(v.invariant_violations.len(), 1);
    }

    fn repair_cfg() -> crate::config::RepairConfig {
        crate::config::RepairConfig {
            enabled: true,
            mode: "shadow".into(),
            probation_runs: 3,
            max_promotions_30d: 2,
            ..crate::config::RepairConfig::default()
        }
    }

    fn promotion_input() -> PromotionInput {
        PromotionInput {
            candidate_version: 4,
            candidate_diagnosis_hash: "abc123".into(),
            current_diagnosis_hash: "abc123".into(),
            clean_runs: 2,
            candidate_clean: true,
            live_failing: true,
            promotions_30d: 0,
            blocked_by_cooldown: false,
        }
    }

    #[test]
    fn repair_ships_off_and_a_typo_in_mode_does_not_turn_it_on() {
        // The design's own rule: no auto-promotion before the blind-set number
        // exists. Default OFF, and an unrecognized mode reads as `off` rather
        // than as the permissive default.
        let off = crate::config::RepairConfig::default();
        assert!(!off.enabled);
        assert!(!off.may_promote());
        assert_eq!(
            decide_promotion(&off, &promotion_input()),
            PromotionDecision::Blocked {
                reason: "repair disabled".into()
            }
        );
        let typo = crate::config::RepairConfig {
            enabled: true,
            mode: "shadwo".into(),
            ..off
        };
        assert!(!typo.may_promote());
    }

    #[test]
    fn stale_candidate_not_promoted() {
        // The source broke a SECOND way while the candidate was shadowing. The
        // candidate answers a question nobody is asking any more, and its rules
        // were derived from markup that is already gone.
        let d = decide_promotion(
            &repair_cfg(),
            &PromotionInput {
                current_diagnosis_hash: "def456".into(),
                ..promotion_input()
            },
        );
        assert!(
            matches!(&d, PromotionDecision::Drop { reason } if reason.contains("stale")),
            "{d:?}"
        );
    }

    #[test]
    fn a_candidate_is_not_promoted_over_live_rules_that_are_working() {
        // Replacing rules that pass every gate is not a repair, it is a coin
        // flip with the dataset.
        let d = decide_promotion(
            &repair_cfg(),
            &PromotionInput {
                live_failing: false,
                ..promotion_input()
            },
        );
        assert!(
            matches!(&d, PromotionDecision::Drop { reason } if reason.contains("live rules")),
            "{d:?}"
        );
    }

    #[test]
    fn promotion_needs_the_whole_streak_and_one_bad_run_ends_it() {
        let cfg = repair_cfg();
        // Two banked + this one = 3 = probation_runs.
        assert_eq!(
            decide_promotion(&cfg, &promotion_input()),
            PromotionDecision::Promote { version: 4 }
        );
        // One short.
        assert_eq!(
            decide_promotion(
                &cfg,
                &PromotionInput {
                    clean_runs: 1,
                    ..promotion_input()
                }
            ),
            PromotionDecision::Wait {
                clean_runs: 2,
                needed: 3
            }
        );
        // A failed gate at run 3 of 3 does not "almost" promote.
        let d = decide_promotion(
            &cfg,
            &PromotionInput {
                candidate_clean: false,
                ..promotion_input()
            },
        );
        assert!(matches!(d, PromotionDecision::Drop { .. }), "{d:?}");
        // `on` promotes on the first clean run, by explicit operator choice.
        let eager = crate::config::RepairConfig {
            mode: "on".into(),
            ..repair_cfg()
        };
        assert_eq!(
            decide_promotion(
                &eager,
                &PromotionInput {
                    clean_runs: 0,
                    ..promotion_input()
                }
            ),
            PromotionDecision::Promote { version: 4 }
        );
    }

    #[test]
    fn an_exhausted_repair_budget_and_a_live_cooldown_both_block() {
        // A stuck source is a bad outcome; a source oscillating between two
        // wrong rules while pushing garbage downstream is a worse one.
        let d = decide_promotion(
            &repair_cfg(),
            &PromotionInput {
                promotions_30d: 2,
                ..promotion_input()
            },
        );
        assert_eq!(
            d,
            PromotionDecision::Blocked {
                reason: "repair budget exhausted".into()
            }
        );
        let d = decide_promotion(
            &repair_cfg(),
            &PromotionInput {
                blocked_by_cooldown: true,
                ..promotion_input()
            },
        );
        assert!(matches!(d, PromotionDecision::Blocked { .. }), "{d:?}");
    }

    #[test]
    fn tripped_probation_rolls_back() {
        // One tripped run, not a streak: climbing costs evidence, falling back
        // costs nothing, so a bad version has the shortest possible life.
        assert_eq!(
            probation_outcome(true, Some(3)),
            ProbationOutcome::Rollback { to_version: 3 }
        );
        assert_eq!(probation_outcome(false, Some(3)), ProbationOutcome::Hold);
        // Nowhere to go back to is reported, never silently held.
        assert!(matches!(
            probation_outcome(true, None),
            ProbationOutcome::CannotRollback { .. }
        ));
    }

    #[test]
    fn the_idempotency_key_separates_two_different_breaks_of_one_source() {
        let a = diagnosis_hash("markup_drift", &["price".into()]);
        let b = diagnosis_hash("markup_drift", &["price".into(), "sku".into()]);
        let c = diagnosis_hash("field_loss", &["price".into()]);
        assert_ne!(a, b, "different broken fields are different problems");
        assert_ne!(a, c, "different diagnoses are different problems");
        // Order-independent: the same break must key the same way every run.
        assert_eq!(
            b,
            diagnosis_hash("markup_drift", &["sku".into(), "price".into()])
        );
        assert_eq!(
            repair_idempotency_key("extractor/products", &a),
            format!("repair:extractor/products:{a}")
        );
    }
}
