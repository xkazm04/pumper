//! Invariants mined from a source's own history, and checked against a fresh
//! cohort.
//!
//! Nothing here is asserted by a human. An invariant is a regularity the source
//! has held over hundreds or thousands of its own records at high confidence —
//! "this field is always a number", "always matches `\d{4}-\d{2}-\d{2}`", "never
//! empty", "always distinct per record". A fresh cohort that breaks one is
//! breaking a rule the source itself wrote, which is a much stronger claim than
//! anything a threshold on a single run can make.
//!
//! Mining is deliberately dumb: character-class generalization over observed
//! values, min/max with trimmed tails, most-common JSON type. No model is
//! involved in producing them and none is involved in checking them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::ResilienceConfig;

use super::detect::InvariantCheck;
use super::sketch::{value_text, FieldSketch};

/// Longest mined regex we will store. A pattern generalized from prose is both
/// useless and enormous, so fields whose values are long text get no regex
/// invariant at all.
const MAX_PATTERN_CHARS: usize = 200;

/// Longest value a regex invariant is mined from.
const MAX_REGEX_VALUE_CHARS: usize = 64;

/// Distinctness at or above which a field counts as per-record.
const PER_RECORD_DISTINCT: f64 = 0.9;

/// Fraction trimmed from each tail before a numeric range is recorded, so one
/// bad parse does not widen the range to uselessness.
const RANGE_TRIM: f64 = 0.01;

/// The JSON shape of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonKind {
    Number,
    String,
    Bool,
    Array,
    Object,
}

impl JsonKind {
    fn of(value: &Value) -> Option<Self> {
        match value {
            Value::Null => None,
            Value::Bool(_) => Some(Self::Bool),
            Value::Number(_) => Some(Self::Number),
            Value::String(_) => Some(Self::String),
            Value::Array(_) => Some(Self::Array),
            Value::Object(_) => Some(Self::Object),
        }
    }
}

/// What a mined invariant asserts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum InvariantKind {
    /// The value is always this JSON type.
    Type { json_type: JsonKind },
    /// Its text always matches this character-class pattern.
    Regex { pattern: String },
    /// It is numeric and always within these bounds.
    Range { min: f64, max: f64 },
    /// It is never empty.
    NonNull,
    /// It is distinct per record. A violation of this one is the single
    /// highest-precision silent-corruption signal available.
    Distinctness { min: f64 },
}

impl InvariantKind {
    /// Stable name, used as part of the primary key so a source keeps at most
    /// one invariant of each kind per field.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Type { .. } => "type",
            Self::Regex { .. } => "regex",
            Self::Range { .. } => "range",
            Self::NonNull => "nonnull",
            Self::Distinctness { .. } => "distinctness",
        }
    }
}

/// One mined invariant with the evidence behind it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Invariant {
    pub field: String,
    #[serde(flatten)]
    pub kind: InvariantKind,
    /// Records it was mined over — its weight when a violation is scored.
    pub support: u32,
    pub confidence: f64,
}

/// Mines invariants from a sample of a source's live records.
///
/// `records` are whole record objects; each field is mined independently. Only
/// regularities holding at `invariant_min_confidence` over at least
/// `invariant_min_support` records survive, so a field that is merely *usually*
/// a number produces nothing.
pub fn mine(cfg: &ResilienceConfig, records: &[Value], fields: &[String]) -> Vec<Invariant> {
    let mut out = Vec::new();
    for field in fields {
        let values: Vec<&Value> = records.iter().filter_map(|r| r.get(field)).collect();
        if (values.len() as u32) < cfg.invariant_min_support {
            continue;
        }
        let support = values.len() as u32;
        let present: Vec<&Value> = values.iter().copied().filter(|v| !is_blank(v)).collect();
        let conf = |part: usize| part as f64 / support as f64;

        // never empty
        if conf(present.len()) >= cfg.invariant_min_confidence {
            out.push(Invariant {
                field: field.clone(),
                kind: InvariantKind::NonNull,
                support,
                confidence: conf(present.len()),
            });
        }
        if present.is_empty() {
            continue;
        }

        // always one JSON type
        if let Some((kind, count)) = dominant_kind(&present) {
            let confidence = count as f64 / present.len() as f64;
            if confidence >= cfg.invariant_min_confidence {
                out.push(Invariant {
                    field: field.clone(),
                    kind: InvariantKind::Type { json_type: kind },
                    support: present.len() as u32,
                    confidence,
                });
                // numeric range, tails trimmed
                if kind == JsonKind::Number {
                    if let Some((min, max)) = trimmed_range(&present) {
                        out.push(Invariant {
                            field: field.clone(),
                            kind: InvariantKind::Range { min, max },
                            support: present.len() as u32,
                            confidence: 1.0 - 2.0 * RANGE_TRIM,
                        });
                    }
                }
            }
        }

        // always the same character-class pattern
        if let Some((pattern, confidence)) = mine_pattern(cfg, &present) {
            out.push(Invariant {
                field: field.clone(),
                kind: InvariantKind::Regex { pattern },
                support: present.len() as u32,
                confidence,
            });
        }

        // distinct per record
        let distinct = distinct_ratio(&present);
        if distinct >= PER_RECORD_DISTINCT {
            out.push(Invariant {
                field: field.clone(),
                kind: InvariantKind::Distinctness {
                    min: PER_RECORD_DISTINCT,
                },
                support: present.len() as u32,
                confidence: distinct,
            });
        }
    }
    out
}

/// Checks a cohort against the source's invariants, returning one
/// [`InvariantCheck`] per invariant with how many documents broke it.
///
/// `docs` are this run's extracted values; `sketches` supply the cohort-level
/// properties (distinctness) that no single document can answer.
pub fn check<'a>(
    invariants: &[Invariant],
    docs: impl IntoIterator<Item = &'a Value> + Clone,
    sketches: &std::collections::BTreeMap<String, FieldSketch>,
) -> Vec<InvariantCheck> {
    let mut out = Vec::with_capacity(invariants.len());
    for inv in invariants {
        // Cohort-level: the run's distinctness against the mined floor. Counted
        // over the whole cohort so its weight matches a per-document check.
        if let InvariantKind::Distinctness { min } = inv.kind {
            let Some(sketch) = sketches.get(&inv.field) else {
                continue;
            };
            let broke = if (sketch.distinct_ratio as f64) < min {
                sketch.n
            } else {
                0
            };
            out.push(InvariantCheck {
                field: inv.field.clone(),
                kind: inv.kind.name().to_string(),
                support: inv.support,
                broke,
                checked: sketch.n,
            });
            continue;
        }
        // Per-document: compile once per invariant, not once per document.
        let regex = match &inv.kind {
            InvariantKind::Regex { pattern } => match regex::Regex::new(pattern) {
                Ok(re) => Some(re),
                // A pattern that no longer compiles is not evidence of anything.
                Err(_) => continue,
            },
            _ => None,
        };
        let mut checked = 0u32;
        let mut broke = 0u32;
        for values in docs.clone() {
            let Some(value) = values.get(&inv.field) else {
                continue;
            };
            match holds(&inv.kind, value, regex.as_ref()) {
                None => {}
                Some(true) => checked += 1,
                Some(false) => {
                    checked += 1;
                    broke += 1;
                }
            }
        }
        out.push(InvariantCheck {
            field: inv.field.clone(),
            kind: inv.kind.name().to_string(),
            support: inv.support,
            broke,
            checked,
        });
    }
    out
}

/// Whether one value satisfies one invariant. `None` when the invariant does not
/// apply to this value at all — a blank value is not a type violation, it is a
/// miss, and the miss-rate signal already owns it.
fn holds(kind: &InvariantKind, value: &Value, regex: Option<&regex::Regex>) -> Option<bool> {
    match kind {
        InvariantKind::NonNull => Some(!is_blank(value)),
        _ if is_blank(value) => None,
        InvariantKind::Type { json_type } => Some(JsonKind::of(value) == Some(*json_type)),
        InvariantKind::Range { min, max } => {
            let n = value.as_f64()?;
            Some(n >= *min && n <= *max)
        }
        InvariantKind::Regex { .. } => {
            let text = value_text(value);
            Some(regex?.is_match(&text))
        }
        InvariantKind::Distinctness { .. } => None, // handled at cohort level
    }
}

fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
        _ => false,
    }
}

fn dominant_kind(values: &[&Value]) -> Option<(JsonKind, usize)> {
    let mut counts: std::collections::BTreeMap<&str, (JsonKind, usize)> = Default::default();
    for value in values {
        if let Some(kind) = JsonKind::of(value) {
            let entry = counts.entry(kind_key(kind)).or_insert((kind, 0));
            entry.1 += 1;
        }
    }
    counts.into_values().max_by_key(|(_, n)| *n)
}

fn kind_key(kind: JsonKind) -> &'static str {
    match kind {
        JsonKind::Number => "number",
        JsonKind::String => "string",
        JsonKind::Bool => "bool",
        JsonKind::Array => "array",
        JsonKind::Object => "object",
    }
}

/// Numeric bounds with `RANGE_TRIM` of each tail dropped, so one mis-parsed
/// outlier does not widen the range until it can never be violated.
fn trimmed_range(values: &[&Value]) -> Option<(f64, f64)> {
    let mut numbers: Vec<f64> = values.iter().filter_map(|v| v.as_f64()).collect();
    if numbers.len() < 3 {
        return None;
    }
    numbers.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let trim = ((numbers.len() as f64) * RANGE_TRIM).floor() as usize;
    let lo = numbers[trim];
    let hi = numbers[numbers.len() - 1 - trim];
    (lo <= hi).then_some((lo, hi))
}

fn distinct_ratio(values: &[&Value]) -> f64 {
    let distinct: std::collections::HashSet<String> =
        values.iter().map(|v| value_text(v)).collect();
    distinct.len() as f64 / values.len() as f64
}

/// Generalizes observed values into one character-class pattern, preferring the
/// exact-length form (`^\d{4}-\d{2}-\d{2}$`) and falling back to the
/// variable-length form (`^\d+-\d+-\d+$`) when only the lengths differ.
fn mine_pattern(cfg: &ResilienceConfig, values: &[&Value]) -> Option<(String, f64)> {
    let texts: Vec<String> = values
        .iter()
        .map(|v| value_text(v))
        .filter(|t| !t.is_empty() && t.chars().count() <= MAX_REGEX_VALUE_CHARS)
        .collect();
    // Long/structured values are excluded, not approximated: a pattern mined
    // from prose would be enormous and satisfied by nothing.
    if texts.len() < values.len() / 2 || texts.is_empty() {
        return None;
    }
    for exact in [true, false] {
        let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
        for text in &texts {
            let pattern = class_pattern(text, exact);
            if pattern.chars().count() <= MAX_PATTERN_CHARS {
                *counts.entry(pattern).or_insert(0) += 1;
            }
        }
        if let Some((pattern, count)) = counts.into_iter().max_by_key(|(_, n)| *n) {
            let confidence = count as f64 / texts.len() as f64;
            if confidence >= cfg.invariant_min_confidence {
                return Some((pattern, confidence));
            }
        }
    }
    None
}

/// One value's character-class signature as an anchored regex.
fn class_pattern(text: &str, exact: bool) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::from("^");
    let mut i = 0;
    while i < chars.len() {
        let class = classify(chars[i]);
        let mut run = 1;
        while i + run < chars.len() && classify(chars[i + run]) == class {
            run += 1;
        }
        match class {
            Class::Digit | Class::Alpha | Class::Space => {
                out.push_str(match class {
                    Class::Digit => r"\d",
                    Class::Alpha => "[A-Za-z]",
                    _ => r"\s",
                });
                if class == Class::Space {
                    out.push('+');
                } else if exact && run > 1 {
                    out.push_str(&format!("{{{run}}}"));
                } else if !exact {
                    out.push('+');
                }
            }
            Class::Other => {
                // Punctuation is the skeleton of a format (`-`, `/`, `$`, `://`),
                // so it stays literal rather than being generalized away.
                for ch in &chars[i..i + run] {
                    out.push_str(&regex::escape(&ch.to_string()));
                }
            }
        }
        i += run;
    }
    out.push('$');
    out
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Class {
    Digit,
    Alpha,
    Space,
    Other,
}

fn classify(ch: char) -> Class {
    if ch.is_ascii_digit() {
        Class::Digit
    } else if ch.is_alphabetic() {
        Class::Alpha
    } else if ch.is_whitespace() {
        Class::Space
    } else {
        Class::Other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::FieldStatus;
    use crate::resilience::sketch::SketchBuilder;
    use serde_json::json;

    fn cfg() -> ResilienceConfig {
        ResilienceConfig {
            invariant_min_support: 5,
            invariant_min_confidence: 0.99,
            ..ResilienceConfig::default()
        }
    }

    fn records(values: &[Value]) -> Vec<Value> {
        values.iter().map(|v| json!({ "f": v })).collect()
    }

    fn mined(values: &[Value]) -> Vec<Invariant> {
        mine(&cfg(), &records(values), &["f".to_string()])
    }

    fn kinds(invs: &[Invariant]) -> Vec<&'static str> {
        invs.iter().map(|i| i.kind.name()).collect()
    }

    #[test]
    fn mines_a_date_pattern_and_rejects_prose() {
        let dates: Vec<Value> = (1..=12).map(|m| json!(format!("2026-{m:02}-01"))).collect();
        let invs = mined(&dates);
        let pattern = invs.iter().find_map(|i| match &i.kind {
            InvariantKind::Regex { pattern } => Some(pattern.clone()),
            _ => None,
        });
        // Punctuation is escaped, so the separator is a literal `\-`.
        assert_eq!(pattern.as_deref(), Some(r"^\d{4}\-\d{2}\-\d{2}$"));
        let re = regex::Regex::new(pattern.as_deref().unwrap()).unwrap();
        assert!(re.is_match("2027-01-31"));
        assert!(!re.is_match("17 reviews"));

        // Free text has no common signature, so no majority pattern emerges and
        // nothing is mined. (A *rigidly templated* string would mine a pattern,
        // and should — "always words then digits" is a real regularity.)
        let prose: Vec<Value> = [
            "Blue widget",
            "A larger red gadget, boxed",
            "Doohickey (spare)",
            "Sprocket 4mm - stainless",
            "Flange",
            "Gizmo v2; refurbished unit",
        ]
        .iter()
        .map(|s| json!(s))
        .collect();
        assert!(
            !kinds(&mined(&prose)).contains(&"regex"),
            "a pattern must not be mined from unstructured text"
        );
    }

    #[test]
    fn mines_variable_length_patterns_when_only_the_lengths_differ() {
        let ids: Vec<Value> = ["AB-1", "CDE-22", "F-333", "GHIJ-4", "KL-55555", "MN-6"]
            .iter()
            .map(|s| json!(s))
            .collect();
        let invs = mined(&ids);
        let pattern = invs.iter().find_map(|i| match &i.kind {
            InvariantKind::Regex { pattern } => Some(pattern.clone()),
            _ => None,
        });
        assert_eq!(pattern.as_deref(), Some(r"^[A-Za-z]+\-\d+$"));
        // And the mined pattern actually matches what it was mined from.
        let re = regex::Regex::new(pattern.as_deref().unwrap()).unwrap();
        assert!(re.is_match("XY-9"));
        assert!(!re.is_match("Add to cart"));
    }

    #[test]
    fn mines_type_range_nonnull_and_distinctness() {
        let prices: Vec<Value> = (1..=20).map(|i| json!(i as f64 * 1.5)).collect();
        let invs = mined(&prices);
        let names = kinds(&invs);
        assert!(names.contains(&"type"), "{names:?}");
        assert!(names.contains(&"range"), "{names:?}");
        assert!(names.contains(&"nonnull"), "{names:?}");
        assert!(names.contains(&"distinctness"), "{names:?}");
        let range = invs.iter().find_map(|i| match i.kind {
            InvariantKind::Range { min, max } => Some((min, max)),
            _ => None,
        });
        let (min, max) = range.unwrap();
        assert!(min >= 1.5 && max <= 30.0, "({min}, {max})");
    }

    #[test]
    fn a_field_that_is_only_usually_a_number_yields_no_type_invariant() {
        let mut mixed: Vec<Value> = (1..=10).map(|i| json!(i)).collect();
        mixed.push(json!("n/a"));
        mixed.push(json!("n/a"));
        assert!(
            !kinds(&mined(&mixed)).contains(&"type"),
            "99% confidence must not be satisfied by 83%"
        );
    }

    #[test]
    fn a_thin_sample_mines_nothing() {
        // Below min_support no regularity is trusted, however clean it looks.
        let few: Vec<Value> = (1..=3).map(|i| json!(i)).collect();
        assert!(mined(&few).is_empty());
    }

    #[test]
    fn check_counts_violations_and_ignores_inapplicable_values() {
        let invs = vec![
            Invariant {
                field: "date".into(),
                kind: InvariantKind::Regex {
                    pattern: r"^\d{4}-\d{2}-\d{2}$".into(),
                },
                support: 900,
                confidence: 1.0,
            },
            Invariant {
                field: "price".into(),
                kind: InvariantKind::Range {
                    min: 1.0,
                    max: 100.0,
                },
                support: 900,
                confidence: 1.0,
            },
        ];
        let docs = vec![
            json!({ "date": "2026-07-25", "price": 10.0 }),
            json!({ "date": "17 reviews", "price": 5000.0 }),
            // A blank value is a miss, not a violation — the miss-rate signal
            // owns it, and double-counting it would inflate the score.
            json!({ "date": null, "price": null }),
        ];
        let checks = check(&invs, docs.iter(), &Default::default());
        let date = checks.iter().find(|c| c.field == "date").unwrap();
        assert_eq!((date.checked, date.broke), (2, 1));
        let price = checks.iter().find(|c| c.field == "price").unwrap();
        assert_eq!((price.checked, price.broke), (2, 1));
    }

    #[test]
    fn distinctness_is_checked_against_the_cohort_not_a_document() {
        let invs = vec![Invariant {
            field: "price".into(),
            kind: InvariantKind::Distinctness { min: 0.9 },
            support: 900,
            confidence: 0.98,
        }];
        // A cohort where every record carries the same value breaks it wholesale.
        let mut collapsed = SketchBuilder::new();
        for _ in 0..10 {
            collapsed.push(&FieldStatus::Matched, None, &json!("Free shipping"));
        }
        let sketches =
            std::collections::BTreeMap::from([("price".to_string(), collapsed.finish())]);
        let checks = check(&invs, [].iter(), &sketches);
        assert_eq!((checks[0].checked, checks[0].broke), (10, 10));

        // A cohort that stayed distinct breaks nothing.
        let mut fine = SketchBuilder::new();
        for i in 0..10 {
            fine.push(&FieldStatus::Matched, None, &json!(format!("${i}.99")));
        }
        let sketches = std::collections::BTreeMap::from([("price".to_string(), fine.finish())]);
        let checks = check(&invs, [].iter(), &sketches);
        assert_eq!(checks[0].broke, 0);
    }
}
