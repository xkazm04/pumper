//! Multi-core, SIMD-accelerated extraction engine with a declarative rule set.
//!
//! A `RuleSet` maps output fields to extraction rules (CSS / regex / JSON
//! pointer / constant). Rules are compiled once, then `extract_batch` runs them
//! over a slice of documents across all CPU cores via rayon — no GIL, so a
//! whole batch is parsed and extracted in parallel in one process. JSON rules
//! parse with `simd-json` (SIMD, GB/s). This is the throughput path a Python
//! stack can't match in-process: the GIL serializes CPU-bound parsing, and
//! scaling out means `multiprocessing` with pickle overhead across processes.

use std::borrow::Cow;
use std::collections::BTreeMap;

use rayon::prelude::*;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{Error, Result};

/// One extraction rule for a field. Deserialized from app params, e.g.
/// `{"type": "css", "selector": "h1", "attr": null, "all": false}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Rule {
    /// CSS selector → text (or an attribute); `all` collects every match. Set
    /// `html: true` to yield the matched element's HTML instead of its flattened
    /// text — pair with a `to_markdown` transform to get clean scoped Markdown of
    /// e.g. `article.content` (the plain text path fuses headings/lists/tables).
    Css {
        selector: String,
        #[serde(default)]
        attr: Option<String>,
        #[serde(default)]
        all: bool,
        #[serde(default)]
        html: bool,
    },
    /// Regex over the raw document; captures `group` (0 = whole match).
    Regex {
        pattern: String,
        #[serde(default)]
        group: usize,
    },
    /// JSON Pointer (RFC 6901, e.g. `/data/0/name`) into a JSON body.
    Json { pointer: String },
    /// XPath expression over the HTML document (e.g. `//div[@id='x']//a/@href`);
    /// `all` collects every match.
    Xpath {
        xpath: String,
        #[serde(default)]
        all: bool,
    },
    /// A literal value.
    Const { value: Value },
    /// Repeating container: for every element matching `selector`, run `fields`
    /// **scoped to that element**, yielding one object per match. This is the
    /// list-page shape (50 product cards → 50 objects) — unlike `css` + `all`,
    /// which returns independent parallel arrays that mis-zip when an item is
    /// missing a field. Inner fields may be `css` (scoped to the element),
    /// `regex` (over the element's HTML), `const`, or a nested `each`; `json` and
    /// `xpath` inner rules are rejected at compile.
    ///
    /// `container` is an optional *enclosing* selector (the listing element that
    /// holds the items). When set, items are selected inside it and an empty
    /// result splits into two distinguishable statuses: the container matched but
    /// held no items ([`FieldStatus::ContainerEmpty`] — "the job board has no
    /// postings this week") versus the container itself is gone
    /// ([`FieldStatus::Empty`] — the listing selector broke). Without it both
    /// collapse into `Empty`, which is the conflation the health detector cannot
    /// undo after the fact.
    Each {
        selector: String,
        fields: BTreeMap<String, FieldRule>,
        #[serde(default)]
        container: Option<String>,
    },
}

/// A field's extraction rule plus an optional post-processing pipeline, e.g.
/// `{"type": "regex", "pattern": "\\$([0-9.]+)", "group": 1,
///   "transforms": [{"op": "to_number"}]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldRule {
    #[serde(flatten)]
    pub rule: Rule,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transforms: Vec<Transform>,
}

/// One post-extraction transform. Applied in order; element-wise over arrays
/// (except `default`, which replaces a null result wholesale).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Transform {
    /// Trim surrounding whitespace from strings.
    Trim,
    Lowercase,
    Uppercase,
    /// Parse strings to a number, tolerating `$ € £ % ,` and whitespace.
    ToNumber,
    /// Like `to_number` but truncated to an integer.
    ToInt,
    /// `true/yes/y/1` → true, `false/no/n/0` → false (case-insensitive).
    ToBool,
    /// Regex find/replace over string values ($1-style capture references).
    RegexReplace {
        pattern: String,
        replacement: String,
    },
    /// Split a string by `sep`; `index` picks one part (else keeps the array).
    Split {
        sep: String,
        #[serde(default)]
        index: Option<usize>,
    },
    /// HTML fragment → clean Markdown (pair with a `css` rule's `html: true`).
    ToMarkdown,
    /// Replace a **blank** result (`null`, a whitespace-only string, or an empty
    /// array — the same predicate [`FieldStatus`] calls empty) with this value.
    Default {
        value: Value,
    },
}

/// A set of fields to extract from each document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuleSet {
    pub fields: BTreeMap<String, FieldRule>,
}

impl RuleSet {
    /// Validates and pre-compiles selectors/regexes once for reuse across the
    /// whole batch.
    pub fn compile(&self) -> Result<CompiledRuleSet> {
        Ok(CompiledRuleSet {
            fields: compile_fields(&self.fields, false)?,
        })
    }
}

/// Compiles a field map into `(name, CompiledRule, transforms)` tuples. `scoped`
/// is true when compiling the inner fields of an [`Rule::Each`] — inside a
/// container, `json`/`xpath` have no meaningful root, so they are rejected here
/// with a clear compile error (v1 scope).
fn compile_fields(
    fields: &BTreeMap<String, FieldRule>,
    scoped: bool,
) -> Result<Vec<(String, CompiledRule, Vec<CompiledTransform>)>> {
    let mut out = Vec::with_capacity(fields.len());
    for (name, field) in fields {
        let compiled = compile_rule(&field.rule, scoped)?;
        let transforms = field
            .transforms
            .iter()
            .map(|t| CompiledTransform::compile(t.clone()))
            .collect::<Result<Vec<_>>>()?;
        out.push((name.clone(), compiled, transforms));
    }
    Ok(out)
}

fn compile_rule(rule: &Rule, scoped: bool) -> Result<CompiledRule> {
    Ok(match rule {
        Rule::Css {
            selector,
            attr,
            all,
            html,
        } => {
            let sel = Selector::parse(selector)
                .map_err(|e| Error::Parse(format!("bad css selector '{selector}': {e:?}")))?;
            CompiledRule::Css {
                selector: sel,
                attr: attr.clone(),
                all: *all,
                html: *html,
            }
        }
        Rule::Regex { pattern, group } => {
            let re = Regex::new(pattern)
                .map_err(|e| Error::Parse(format!("bad regex '{pattern}': {e}")))?;
            CompiledRule::Regex { re, group: *group }
        }
        Rule::Json { pointer } if !scoped => {
            // RFC 6901: a pointer is the empty string or begins with '/'.
            // Validate here like css/regex/xpath so a malformed pointer is
            // an Error at compile time, not an indistinguishable Empty miss
            // at extract time (which defeats the DocReport/FieldStatus signal).
            if !pointer.is_empty() && !pointer.starts_with('/') {
                return Err(Error::Parse(format!(
                    "bad json pointer '{pointer}': must be empty or start with '/'"
                )));
            }
            CompiledRule::Json {
                pointer: pointer.clone(),
            }
        }
        Rule::Xpath { xpath, all } if !scoped => {
            let parsed = skyscraper::xpath::parse(xpath)
                .map_err(|e| Error::Parse(format!("bad xpath '{xpath}': {e}")))?;
            CompiledRule::Xpath {
                xpath: parsed,
                all: *all,
            }
        }
        Rule::Json { .. } | Rule::Xpath { .. } => {
            return Err(Error::Parse(
                "'json'/'xpath' rules are not supported inside an 'each' container \
                 (use 'css'/'regex'/'const' or a nested 'each')"
                    .into(),
            ))
        }
        Rule::Const { value } => CompiledRule::Const {
            value: value.clone(),
        },
        Rule::Each {
            selector,
            fields,
            container,
        } => {
            let sel = Selector::parse(selector).map_err(|e| {
                Error::Parse(format!("bad css selector '{selector}' in 'each': {e:?}"))
            })?;
            let container = container
                .as_deref()
                .map(|c| {
                    Selector::parse(c).map_err(|e| {
                        Error::Parse(format!("bad container selector '{c}' in 'each': {e:?}"))
                    })
                })
                .transpose()?;
            CompiledRule::Each {
                selector: sel,
                fields: compile_fields(fields, true)?,
                container,
            }
        }
    })
}

enum CompiledRule {
    Css {
        selector: Selector,
        attr: Option<String>,
        all: bool,
        html: bool,
    },
    Regex {
        re: Regex,
        group: usize,
    },
    Json {
        pointer: String,
    },
    Xpath {
        xpath: skyscraper::xpath::Xpath,
        all: bool,
    },
    Const {
        value: Value,
    },
    Each {
        selector: Selector,
        fields: Vec<(String, CompiledRule, Vec<CompiledTransform>)>,
        container: Option<Selector>,
    },
}

/// A transform with its regex pre-compiled.
enum CompiledTransform {
    Trim,
    Lowercase,
    Uppercase,
    ToNumber,
    ToInt,
    ToBool,
    RegexReplace { re: Regex, replacement: String },
    Split { sep: String, index: Option<usize> },
    ToMarkdown,
    Default { value: Value },
}

impl CompiledTransform {
    fn compile(t: Transform) -> Result<Self> {
        Ok(match t {
            Transform::Trim => Self::Trim,
            Transform::Lowercase => Self::Lowercase,
            Transform::Uppercase => Self::Uppercase,
            Transform::ToNumber => Self::ToNumber,
            Transform::ToInt => Self::ToInt,
            Transform::ToBool => Self::ToBool,
            Transform::RegexReplace {
                pattern,
                replacement,
            } => Self::RegexReplace {
                re: Regex::new(&pattern)
                    .map_err(|e| Error::Parse(format!("bad transform regex '{pattern}': {e}")))?,
                replacement,
            },
            Transform::Split { sep, index } => Self::Split { sep, index },
            Transform::ToMarkdown => Self::ToMarkdown,
            Transform::Default { value } => Self::Default { value },
        })
    }

    /// Applies to one value; arrays are mapped element-wise (except `default`).
    ///
    /// `default` fires on [`is_blank`] — the SAME predicate that decides
    /// [`FieldStatus::Empty`] — so the two cannot disagree. It used to fire only
    /// on `Value::Null`, which left a matched-but-empty `""` (or an empty array)
    /// reported as `empty` while the declared default never applied: the status
    /// system said "this field produced nothing" and the value said `""`.
    fn apply(&self, value: Value) -> Value {
        match (self, value) {
            (Self::Default { value: d }, v) if is_blank(&v) => d.clone(),
            (Self::Default { .. }, v) => v,
            (t, Value::Array(items)) => {
                Value::Array(items.into_iter().map(|v| t.apply_scalar(v)).collect())
            }
            (t, v) => t.apply_scalar(v),
        }
    }

    fn apply_scalar(&self, value: Value) -> Value {
        match self {
            Self::Trim => map_str(value, |s| Value::String(s.trim().to_string())),
            Self::Lowercase => map_str(value, |s| Value::String(s.to_lowercase())),
            Self::Uppercase => map_str(value, |s| Value::String(s.to_uppercase())),
            Self::ToNumber => coerce_number(value, false),
            Self::ToInt => coerce_number(value, true),
            Self::ToBool => match value {
                Value::Bool(b) => Value::Bool(b),
                Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "yes" | "y" | "1" => Value::Bool(true),
                    "false" | "no" | "n" | "0" => Value::Bool(false),
                    _ => Value::Null,
                },
                Value::Number(n) => Value::Bool(n.as_f64() != Some(0.0)),
                _ => Value::Null,
            },
            Self::RegexReplace { re, replacement } => map_str(value, |s| {
                Value::String(re.replace_all(s, replacement.as_str()).into_owned())
            }),
            Self::Split { sep, index } => map_str(value, |s| {
                let parts: Vec<&str> = s.split(sep.as_str()).collect();
                match index {
                    Some(i) => parts
                        .get(*i)
                        .map(|p| Value::String(p.trim().to_string()))
                        .unwrap_or(Value::Null),
                    None => Value::Array(
                        parts
                            .into_iter()
                            .map(|p| Value::String(p.trim().to_string()))
                            .collect(),
                    ),
                }
            }),
            Self::ToMarkdown => map_str(value, |s| {
                Value::String(crate::markdown::html_fragment_to_markdown(s))
            }),
            Self::Default { .. } => value, // handled in apply()
        }
    }
}

/// Applies `f` when the value is a string; passes anything else through.
fn map_str(value: Value, f: impl Fn(&str) -> Value) -> Value {
    match value {
        Value::String(s) => f(&s),
        v => v,
    }
}

/// Parses strings to numbers, tolerating currency symbols, thousands
/// separators, and `%`. Numbers pass through; anything else becomes null.
fn coerce_number(value: Value, int: bool) -> Value {
    let num = match &value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => parse_first_number(s),
        _ => None,
    };
    match num {
        Some(n) => number_value(n, int),
        None => Value::Null,
    }
}

/// 2^63 — the exclusive upper bound of `i64` as an `f64`. `-2^63` is exactly
/// `i64::MIN`, so it is the *inclusive* lower bound.
const I64_LIMIT: f64 = 9_223_372_036_854_775_808.0;

/// One parsed number as JSON, or `Null` when it cannot be represented — the
/// single place `to_number` and `to_int` decide that, so they cannot disagree.
///
/// They used to: a 400-digit price string parses to `f64::INFINITY`, and
/// `to_number` correctly refused it (`serde_json::Number::from_f64` rejects
/// non-finite) while `to_int`'s `as i64` cast *saturated* it to
/// `9223372036854775807` — a fabricated number, indistinguishable from a real
/// one, in a field whose whole point is to be trustworthy. A value f64 cannot
/// hold, or one outside `i64` when an integer was asked for, is null at BOTH
/// precisions: the honest answer is "not a number", never a clamped stand-in.
fn number_value(n: f64, int: bool) -> Value {
    if !n.is_finite() {
        return Value::Null;
    }
    if int {
        let t = n.trunc();
        if !(-I64_LIMIT..I64_LIMIT).contains(&t) {
            return Value::Null;
        }
        return Value::from(t as i64);
    }
    serde_json::Number::from_f64(n)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Parses the FIRST valid decimal number found in a string, tolerating leading
/// currency symbols and `,` thousands separators. Unlike a naive
/// "strip every non-digit" pass, this does NOT concatenate digits across
/// separators: `"1-2"` → `1` (a range, not `-12`), `"$1,234.50"` → `1234.5`,
/// `"3.5%"` → `3.5`. A sign only binds when it directly precedes the digits
/// (`"-5"` → `-5`, but the `-` in `"1-2"` is a separator, not a sign).
fn parse_first_number(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    let n = b.len();
    let is_digit = |i: usize| b.get(i).is_some_and(u8::is_ascii_digit);
    let mut i = 0;
    while i < n {
        // Does a number token start at `i`?
        let starts = match b[i] {
            b'-' | b'+' => is_digit(i + 1) || (b.get(i + 1) == Some(&b'.') && is_digit(i + 2)),
            b'.' => is_digit(i + 1),
            c => c.is_ascii_digit(),
        };
        if !starts {
            i += 1;
            continue;
        }
        let mut buf = String::new();
        let mut j = i;
        if b[j] == b'-' || b[j] == b'+' {
            if b[j] == b'-' {
                buf.push('-');
            }
            j += 1;
        }
        let mut seen_dot = false;
        while j < n {
            match b[j] {
                d if d.is_ascii_digit() => {
                    buf.push(d as char);
                    j += 1;
                }
                // Thousands separator: only between digits.
                b',' if is_digit(j + 1) => j += 1,
                // Decimal point: only the first, and only if a digit follows
                // (so a sentence-ending period isn't swallowed).
                b'.' if !seen_dot && is_digit(j + 1) => {
                    seen_dot = true;
                    buf.push('.');
                    j += 1;
                }
                _ => break,
            }
        }
        return buf.parse::<f64>().ok();
    }
    None
}

/// Compiled, thread-shareable rule set. `Send + Sync` so a `&CompiledRuleSet`
/// can drive every rayon worker in parallel.
pub struct CompiledRuleSet {
    fields: Vec<(String, CompiledRule, Vec<CompiledTransform>)>,
}

impl CompiledRuleSet {
    fn needs_html(&self) -> bool {
        // `Each` always selects its container via a CSS selector on the document,
        // so it needs the parsed HTML just like a top-level `Css` rule.
        self.fields
            .iter()
            .any(|(_, r, _)| matches!(r, CompiledRule::Css { .. } | CompiledRule::Each { .. }))
    }

    fn needs_json(&self) -> bool {
        self.fields
            .iter()
            .any(|(_, r, _)| matches!(r, CompiledRule::Json { .. }))
    }

    fn needs_xpath(&self) -> bool {
        self.fields
            .iter()
            .any(|(_, r, _)| matches!(r, CompiledRule::Xpath { .. }))
    }
}

/// Per-field extraction outcome — the quality signal that separates a broken
/// selector's silent `Null` from a field that is genuinely absent. `serde`-stable
/// (a `status` tag): consumers (e.g. the extractor's aggregate result and the
/// preview endpoint) serialize this directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FieldStatus {
    /// The rule ran and produced a non-empty value.
    Matched,
    /// The rule ran but produced nothing (`null`, empty string, or empty
    /// array) — the field is absent in this document, not mis-configured.
    Empty,
    /// An `each` rule with a `container`: the container matched but held zero
    /// items. Distinct from `Empty` (the container itself is missing) because a
    /// legitimately quiet listing and a broken listing selector are otherwise
    /// indistinguishable, and only one of them means the extractor is broken.
    ContainerEmpty,
    /// The rule could not run: the document was not in the format the rule
    /// needs (e.g. a `json` rule over a body that is not JSON, or an `xpath`
    /// rule over unparseable HTML). Distinguishes a bad input from a real miss.
    Error { detail: String },
}

impl FieldStatus {
    /// Classifies a rule's raw (pre-transform) output. `ran` is false when the
    /// rule's required parse failed, so the rule never actually evaluated;
    /// `container_matched` is true only for an `each` rule whose `container`
    /// selector found its listing element.
    fn classify(ran: bool, raw: &Value, detail: &str, container_matched: bool) -> FieldStatus {
        if !ran {
            return FieldStatus::Error {
                detail: detail.to_string(),
            };
        }
        if !is_blank(raw) {
            return FieldStatus::Matched;
        }
        if container_matched {
            FieldStatus::ContainerEmpty
        } else {
            FieldStatus::Empty
        }
    }

    /// True when the rule found nothing — the miss signal the health detector
    /// counts. `ContainerEmpty` is deliberately NOT a miss: the container was
    /// there, so the selector still binds.
    pub fn is_miss(&self) -> bool {
        matches!(self, FieldStatus::Empty | FieldStatus::Error { .. })
    }
}

/// Whether a rule's output counts as "produced nothing": `null`, a
/// whitespace-only string, or an empty array. Shared by the pre-transform status
/// and the post-transform coercion check so the two can't disagree.
fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

/// What the transform pipeline did to a field that the selector *did* match —
/// the orthogonal companion to [`FieldStatus`], which is computed before
/// transforms and so cannot see this.
///
/// The wrong-element failure is precisely "the selector found something, and it
/// is garbage": `to_number` on `"Add to cart"` yields null while the field still
/// reports `matched`. A field whose coercion-failure rate jumps while its match
/// rate stays flat has almost no explanation other than a rebound selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoercionStatus {
    /// Transforms ran and left a non-empty value.
    Coerced,
    /// The selector matched, but the transform chain reduced it to nothing —
    /// the value was not of the kind the rule expects.
    CoercionFailed,
    /// The field has no transforms, so there is nothing to coerce.
    NoTransforms,
}

/// One inner field's outcome inside an [`Rule::Each`] listing, aggregated over
/// the listing's items.
///
/// A listing rule reports ONE [`FieldStatus`] for the whole array, so a card
/// whose `price` selector quietly stopped matching leaves the rule `Matched` —
/// the array is still full of objects, they just all carry `price: null`. These
/// counts are what make that visible: they are per inner field and per document,
/// aggregated ACROSS items (counts, never per-item lists), so the report stays
/// O(rule fields) on a listing of any width.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InnerFieldStats {
    /// Items the inner rule ran over — the denominator for every rate here.
    pub items: u32,
    pub matched: u32,
    pub empty: u32,
    /// Items where a NESTED `each` matched its container but held no items —
    /// a working selector over a quiet sub-listing, counted as a hit for the
    /// same reason [`FieldStatus::is_miss`] excludes `ContainerEmpty`.
    pub container_empty: u32,
    pub error: u32,
}

impl InnerFieldStats {
    /// Items where the inner rule found nothing. Mirrors
    /// [`FieldStatus::is_miss`] exactly, so an inner miss and a top-level miss
    /// are the same judgement applied at two scopes.
    pub fn misses(&self) -> u32 {
        self.empty + self.error
    }

    /// Items where the inner selector still bound (matched, or a nested
    /// container that matched but was quiet).
    pub fn hits(&self) -> u32 {
        self.matched + self.container_empty
    }

    /// The listing had items and the inner selector bound on NONE of them —
    /// listing rot, as opposed to a sparse field only some items carry.
    /// This is the distinction a single array-level `Matched` erases.
    pub fn is_dead(&self) -> bool {
        self.items > 0 && self.hits() == 0
    }

    /// Fraction of items where the inner rule found nothing (`0.0` for an
    /// empty listing — no items, no claim).
    pub fn miss_rate(&self) -> f64 {
        if self.items == 0 {
            0.0
        } else {
            self.misses() as f64 / self.items as f64
        }
    }

    /// Folds one item's outcome for this inner field.
    fn push(&mut self, status: &FieldStatus) {
        self.items += 1;
        match status {
            FieldStatus::Matched => self.matched += 1,
            FieldStatus::Empty => self.empty += 1,
            FieldStatus::ContainerEmpty => self.container_empty += 1,
            FieldStatus::Error { .. } => self.error += 1,
        }
    }
}

/// Per-document extraction report — the quality companion to an extracted
/// record. `fields` reflects the rule match (before transforms), so it answers
/// "did the selector find anything?"; `coercion` answers "and was it the right
/// kind of thing?" for the fields that have a transform chain; `each` answers
/// "and inside the listing, did every inner selector still bind?".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DocReport {
    pub fields: BTreeMap<String, FieldStatus>,
    /// Post-transform outcome per field, for fields the selector matched. Empty
    /// on reports built before this existed (and on rule sets with no
    /// transforms), so absence means "unknown", never "fine".
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub coercion: BTreeMap<String, CoercionStatus>,
    /// Per-inner-field outcomes inside [`Rule::Each`] listings, keyed by the
    /// dotted path from the top-level field (`products.price`, and
    /// `products.variants.sku` for a nested `each`). ADDITIVE: `fields` still
    /// carries the listing's own single status, unchanged. Empty on rule sets
    /// with no `each` rule and on reports built before this existed, so absence
    /// means "unknown", never "fine".
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub each: BTreeMap<String, InnerFieldStats>,
}

/// Extracts one document into a JSON object. HTML is parsed at most once (only
/// if any CSS rule needs it); the JSON body is parsed at most once with
/// simd-json (only if any JSON rule needs it).
pub fn extract_one(rules: &CompiledRuleSet, doc: &str) -> Value {
    extract_one_impl(rules, doc, false).0
}

/// Like [`extract_one`] but also returns a per-field [`DocReport`] classifying
/// each field as matched / empty / error.
pub fn extract_one_with_report(rules: &CompiledRuleSet, doc: &str) -> (Value, DocReport) {
    extract_one_impl(rules, doc, true)
}

fn extract_one_impl(rules: &CompiledRuleSet, doc: &str, want_report: bool) -> (Value, DocReport) {
    let html = rules.needs_html().then(|| Html::parse_document(doc));
    extract_one_parsed(rules, doc, want_report, html.as_ref())
}

/// Extracts one document against a DOM the caller already built.
///
/// This is the seam that lets a second consumer of the same document — today,
/// resilience fingerprinting — share the parse instead of building a second
/// identical `Html`. `html` may be `None` only when the rule set has no CSS rule
/// ([`CompiledRuleSet::needs_html`] is false); the CSS arms unwrap it, and that
/// unwrap is exactly the invariant `extract_one_impl` upholds by parsing
/// whenever `needs_html()` says a CSS rule exists.
///
/// Crate-internal: outside callers go through [`crate::resilience::extract_and_fingerprint_batch`],
/// which owns the DOM for the whole document and hands it to both consumers.
pub(crate) fn extract_one_parsed(
    rules: &CompiledRuleSet,
    doc: &str,
    want_report: bool,
    html: Option<&Html>,
) -> (Value, DocReport) {
    let json = if rules.needs_json() {
        let mut bytes = doc.as_bytes().to_vec();
        simd_json::serde::from_slice::<Value>(&mut bytes).ok()
    } else {
        None
    };
    let xpath_tree = if rules.needs_xpath() {
        skyscraper::html::parse(doc).ok()
    } else {
        None
    };

    let mut obj = Map::with_capacity(rules.fields.len());
    let mut report = DocReport::default();
    for (name, rule, transforms) in &rules.fields {
        // (raw value, whether the rule's required parse was available, error
        // detail, whether an `each` container selector matched). `detail` is a
        // `Cow` so a *runtime* failure (an xpath that parsed but could not
        // evaluate) can name itself without every healthy field paying for an
        // allocation.
        let (mut value, ran, detail, container_matched): (Value, bool, Cow<'static, str>, bool) =
            match rule {
                CompiledRule::Css {
                    selector,
                    attr,
                    all,
                    html: as_html,
                } => (
                    css_extract(html.unwrap(), selector, attr.as_deref(), *all, *as_html),
                    true,
                    Cow::Borrowed(""),
                    false,
                ),
                CompiledRule::Regex { re, group } => (
                    re.captures(doc)
                        .and_then(|c| c.get(*group))
                        .map(|m| Value::String(m.as_str().to_string()))
                        .unwrap_or(Value::Null),
                    true,
                    Cow::Borrowed(""),
                    false,
                ),
                CompiledRule::Json { pointer } => match json.as_ref() {
                    Some(j) => (
                        j.pointer(pointer).cloned().unwrap_or(Value::Null),
                        true,
                        Cow::Borrowed(""),
                        false,
                    ),
                    None => (
                        Value::Null,
                        false,
                        Cow::Borrowed("body did not parse as JSON"),
                        false,
                    ),
                },
                CompiledRule::Xpath { xpath, all } => match xpath_tree.as_ref() {
                    Some(tree) => match xpath_extract(tree, xpath, *all) {
                        Ok(v) => (v, true, Cow::Borrowed(""), false),
                        Err(detail) => (Value::Null, false, Cow::Owned(detail), false),
                    },
                    None => (
                        Value::Null,
                        false,
                        Cow::Borrowed("document did not parse as HTML for xpath"),
                        false,
                    ),
                },
                CompiledRule::Const { value } => (value.clone(), true, Cow::Borrowed(""), false),
                CompiledRule::Each {
                    selector,
                    fields,
                    container,
                } => {
                    // The listing's inner outcomes are accumulated only when a
                    // report was asked for — no report, no per-item bookkeeping.
                    let mut acc = want_report.then(EachAcc::default);
                    let (items, container_matched) = each_extract(
                        html.unwrap(),
                        selector,
                        fields,
                        container.as_ref(),
                        &mut acc,
                    );
                    if let Some(acc) = acc {
                        acc.flatten_into(name, fields, &mut report.each);
                    }
                    (
                        Value::Array(items),
                        true,
                        Cow::Borrowed(""),
                        container_matched,
                    )
                }
            };
        let status = FieldStatus::classify(ran, &value, &detail, container_matched);
        let matched = matches!(status, FieldStatus::Matched);
        if want_report {
            report.fields.insert(name.clone(), status);
        }
        for t in transforms {
            value = t.apply(value);
        }
        // Post-transform status: only meaningful where the selector matched and a
        // transform chain then ran. A field that matched nothing has nothing to
        // coerce, so it reports `coerced` rather than inventing a second failure.
        if want_report {
            let coercion = if transforms.is_empty() {
                CoercionStatus::NoTransforms
            } else if matched && is_blank(&value) {
                CoercionStatus::CoercionFailed
            } else {
                CoercionStatus::Coerced
            };
            report.coercion.insert(name.clone(), coercion);
        }
        obj.insert(name.clone(), value);
    }
    (Value::Object(obj), report)
}

/// Accumulates one `each` rule's per-inner-field outcomes across its items
/// WITHOUT allocating per item: slots are positional (parallel to the compiled
/// inner field list) and the dotted report paths are built once, at
/// [`EachAcc::flatten_into`] time. That is what keeps a report over a 5000-row
/// listing the same size as a report over a 5-row one.
#[derive(Default)]
struct EachAcc {
    slots: Vec<InnerSlot>,
}

#[derive(Default)]
struct InnerSlot {
    stats: InnerFieldStats,
    /// Present only when the inner rule is itself an `each`.
    nested: Option<EachAcc>,
}

impl EachAcc {
    /// Grows the slot vector to cover every inner field. Called once per item
    /// (cheap after the first) so an accumulator built for an empty listing
    /// still knows the shape it was measuring.
    fn fit(&mut self, n: usize) {
        if self.slots.len() < n {
            self.slots.resize_with(n, InnerSlot::default);
        }
    }

    /// Writes the accumulated counts into `out`, keyed `{prefix}.{field}`, and
    /// recurses into nested `each` rules. Iterates the RULE's fields rather
    /// than the filled slots, so an inner field of an empty listing still gets
    /// an honest `items: 0` row instead of vanishing from the report.
    fn flatten_into(
        mut self,
        prefix: &str,
        fields: &[(String, CompiledRule, Vec<CompiledTransform>)],
        out: &mut BTreeMap<String, InnerFieldStats>,
    ) {
        self.fit(fields.len());
        for (slot, (name, rule, _)) in self.slots.into_iter().zip(fields) {
            let path = format!("{prefix}.{name}");
            if let CompiledRule::Each {
                fields: inner_fields,
                ..
            } = rule
            {
                slot.nested
                    .unwrap_or_default()
                    .flatten_into(&path, inner_fields, out);
            }
            out.insert(path, slot.stats);
        }
    }
}

/// Runs an `each` rule, returning `(items, container_matched)`. With no
/// `container` the items are selected document-wide and `container_matched` is
/// false (an empty result is just `Empty`, as before). With one, items are
/// selected *inside* every matching container, and `container_matched` reports
/// whether the listing element was found at all — the split that separates a
/// quiet listing from a broken one.
///
/// `acc`, when `Some`, collects each item's per-inner-field outcome.
fn each_extract(
    html: &Html,
    selector: &Selector,
    fields: &[(String, CompiledRule, Vec<CompiledTransform>)],
    container: Option<&Selector>,
    acc: &mut Option<EachAcc>,
) -> (Vec<Value>, bool) {
    match container {
        None => (
            html.select(selector)
                .map(|el| extract_scoped(el, fields, acc))
                .collect(),
            false,
        ),
        Some(container) => {
            let mut items = Vec::new();
            let mut found = false;
            for root in html.select(container) {
                found = true;
                items.extend(
                    root.select(selector)
                        .map(|el| extract_scoped(el, fields, acc)),
                );
            }
            (items, found)
        }
    }
}

/// [`each_extract`] for a nested `each`: the same container split, resolved
/// inside one item element instead of the whole document, so a card's own
/// sub-listing reports exactly like the top level does.
fn each_scoped(
    root: ElementRef,
    selector: &Selector,
    fields: &[(String, CompiledRule, Vec<CompiledTransform>)],
    container: Option<&Selector>,
    acc: &mut Option<EachAcc>,
) -> (Vec<Value>, bool) {
    match container {
        None => (
            root.select(selector)
                .map(|el| extract_scoped(el, fields, acc))
                .collect(),
            false,
        ),
        Some(container) => {
            let mut items = Vec::new();
            let mut found = false;
            for inner in root.select(container) {
                found = true;
                items.extend(
                    inner
                        .select(selector)
                        .map(|el| extract_scoped(el, fields, acc)),
                );
            }
            (items, found)
        }
    }
}

/// Extracts a whole batch in parallel across all cores.
pub fn extract_batch(rules: &CompiledRuleSet, docs: &[String]) -> Vec<Value> {
    docs.par_iter().map(|doc| extract_one(rules, doc)).collect()
}

/// Extracts a whole batch in parallel, pairing each record with its
/// [`DocReport`]. Same ordering guarantees as [`extract_batch`].
pub fn extract_batch_with_report(
    rules: &CompiledRuleSet,
    docs: &[String],
) -> Vec<(Value, DocReport)> {
    docs.par_iter()
        .map(|doc| extract_one_with_report(rules, doc))
        .collect()
}

/// Runs one compiled XPath, or reports WHY it could not run.
///
/// An expression that parses but fails at evaluation (an undefined variable, an
/// unsupported function call, a type error mid-expression) used to return
/// `Value::Null` — which [`FieldStatus::classify`] then read as `Empty`, i.e.
/// "the site had nothing here". That is the one classification a broken rule
/// must never be able to claim: `Empty` is a fact about the *document*, and a
/// rule that never evaluated learned no facts about the document at all. The
/// error string travels to [`FieldStatus::Error`] instead.
fn xpath_extract(
    tree: &skyscraper::xpath::XpathItemTree,
    xpath: &skyscraper::xpath::Xpath,
    all: bool,
) -> std::result::Result<Value, String> {
    let items = xpath
        .apply(tree)
        .map_err(|e| format!("xpath failed to evaluate: {e}"))?;
    let mut values = items.iter().map(|item| xpath_item_value(item, tree));
    Ok(if all {
        Value::Array(values.collect())
    } else {
        values.next().unwrap_or(Value::Null)
    })
}

/// One XPath result as JSON: attribute nodes yield their value, text nodes
/// their content, elements their recursive text; atomic values keep their own
/// JSON type ([`xpath_atomic_value`]).
fn xpath_item_value(
    item: &skyscraper::xpath::grammar::data_model::XpathItem,
    tree: &skyscraper::xpath::XpathItemTree,
) -> Value {
    use skyscraper::xpath::grammar::data_model::XpathItem;
    use skyscraper::xpath::grammar::XpathItemTreeNode;
    match item {
        XpathItem::Node(node) => match node {
            XpathItemTreeNode::AttributeNode(a) => Value::String(a.value.clone()),
            XpathItemTreeNode::TextNode(t) => Value::String(t.content.trim().to_string()),
            n => Value::String(n.text_content(tree).trim().to_string()),
        },
        XpathItem::AnyAtomicType(atomic) => xpath_atomic_value(atomic),
        // A function item is a callable, not data: there is no honest JSON for
        // it, and its Debug rendering is engine internals, not an extraction.
        XpathItem::Function(_) => Value::Null,
    }
}

/// One XPath **atomic** result as JSON, by type.
///
/// `count(//li)`, `string(//h1)` and `not(//x)` are the XPath expressions CSS
/// cannot express, and they are exactly the ones that do not return nodes.
/// Every non-node result used to be rendered with `format!("{other:?}")` — so
/// `count(//li)` stored the string `"AnyAtomicType(Integer(3))"`, a Rust Debug
/// dump written into the dataset as if it were extracted data, `matched` in the
/// report and all. Numbers become JSON numbers, booleans booleans, strings
/// strings; a non-finite double is null rather than a fabricated value
/// ([`number_value`], the same rule `to_number` follows).
fn xpath_atomic_value(atomic: &skyscraper::xpath::grammar::data_model::AnyAtomicType) -> Value {
    use skyscraper::xpath::grammar::data_model::AnyAtomicType;
    match atomic {
        AnyAtomicType::Boolean(b) => Value::Bool(*b),
        AnyAtomicType::Integer(i) => Value::from(*i),
        AnyAtomicType::Float(f) => number_value(f64::from(f.0), false),
        AnyAtomicType::Double(d) => number_value(d.0, false),
        AnyAtomicType::String(s) => Value::String(s.clone()),
        // A QName is a *name*; its lexical form (`prefix:local`) is the value.
        q @ AnyAtomicType::QName { .. } => Value::String(q.to_string()),
    }
}

/// Renders one matched element to a value: an attribute, its serialized HTML
/// (`as_html`), or its flattened text.
fn render_css(el: ElementRef, attr: Option<&str>, as_html: bool) -> Value {
    match attr {
        // An attribute takes precedence over the html/text mode.
        Some(a) => el
            .value()
            .attr(a)
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        // `html: true` yields the matched element's serialized HTML (for a
        // `to_markdown` transform); otherwise its flattened text.
        None if as_html => Value::String(el.html()),
        None => Value::String(el.text().collect::<String>().trim().to_string()),
    }
}

/// Collects a CSS match iterator into a value (`all` = array of every match,
/// else the first or `Null`). Shared by the document-level and element-scoped
/// extractors so their semantics can't diverge.
fn collect_css<'a>(
    mut matches: impl Iterator<Item = ElementRef<'a>>,
    attr: Option<&str>,
    all: bool,
    as_html: bool,
) -> Value {
    if all {
        Value::Array(matches.map(|el| render_css(el, attr, as_html)).collect())
    } else {
        matches
            .next()
            .map(|el| render_css(el, attr, as_html))
            .unwrap_or(Value::Null)
    }
}

fn css_extract(
    html: &Html,
    selector: &Selector,
    attr: Option<&str>,
    all: bool,
    as_html: bool,
) -> Value {
    collect_css(html.select(selector), attr, all, as_html)
}

/// Extracts one repeating-container item: runs `fields` scoped to `root` (a
/// single matched element) and returns an object. CSS selects descendants of
/// `root`, regex runs over the element's own HTML, and a nested `each` recurses
/// into `root`'s subtree — so every item's fields stay bound together and a
/// missing field is a `null` on its own item, never a mis-zipped parallel array.
///
/// `acc`, when `Some`, records each inner field's [`FieldStatus`] for this item
/// — the same pre-transform classification the top level uses, folded into
/// per-field counts so listing rot is visible without echoing every item.
fn extract_scoped(
    root: ElementRef,
    fields: &[(String, CompiledRule, Vec<CompiledTransform>)],
    acc: &mut Option<EachAcc>,
) -> Value {
    if let Some(a) = acc.as_mut() {
        a.fit(fields.len());
    }
    let mut obj = Map::with_capacity(fields.len());
    for (i, (name, rule, transforms)) in fields.iter().enumerate() {
        // Same tuple shape as the document-level loop: (raw value, whether the
        // rule could run, error detail, whether a nested container matched).
        let (mut value, ran, detail, container_matched): (Value, bool, &str, bool) = match rule {
            CompiledRule::Css {
                selector,
                attr,
                all,
                html: as_html,
            } => (
                collect_css(root.select(selector), attr.as_deref(), *all, *as_html),
                true,
                "",
                false,
            ),
            CompiledRule::Regex { re, group } => (
                re.captures(&root.html())
                    .and_then(|c| c.get(*group))
                    .map(|m| Value::String(m.as_str().to_string()))
                    .unwrap_or(Value::Null),
                true,
                "",
                false,
            ),
            CompiledRule::Const { value } => (value.clone(), true, "", false),
            // A nested `each`'s container (when set) is resolved inside this item,
            // so a card's own sub-listing splits the same way the top level does.
            CompiledRule::Each {
                selector,
                fields: inner_fields,
                container,
            } => {
                let (items, matched) = match acc.as_mut() {
                    Some(a) => {
                        let nested = &mut a.slots[i].nested;
                        if nested.is_none() {
                            *nested = Some(EachAcc::default());
                        }
                        each_scoped(root, selector, inner_fields, container.as_ref(), nested)
                    }
                    None => {
                        each_scoped(root, selector, inner_fields, container.as_ref(), &mut None)
                    }
                };
                (Value::Array(items), true, "", matched)
            }
            // json/xpath are rejected at compile inside a container, so this arm
            // is unreachable — and if it ever became reachable it is a rule that
            // could not run, never a document that had nothing.
            CompiledRule::Json { .. } | CompiledRule::Xpath { .. } => (
                Value::Null,
                false,
                "'json'/'xpath' rules cannot run inside an 'each' container",
                false,
            ),
        };
        if let Some(a) = acc.as_mut() {
            a.slots[i].stats.push(&FieldStatus::classify(
                ran,
                &value,
                detail,
                container_matched,
            ));
        }
        for t in transforms {
            value = t.apply(value);
        }
        obj.insert(name.clone(), value);
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::{extract_batch, RuleSet};
    use serde_json::json;

    fn ruleset(v: serde_json::Value) -> super::CompiledRuleSet {
        serde_json::from_value::<RuleSet>(v)
            .unwrap()
            .compile()
            .unwrap()
    }

    #[test]
    fn css_html_mode_with_to_markdown_extracts_scoped_markdown() {
        let rules = ruleset(json!({
            "body": {
                "type": "css", "selector": "article", "html": true,
                "transforms": [{"op": "to_markdown"}]
            }
        }));
        let doc = "<div><nav>menu here</nav><article><h2>Title</h2>\
            <ul><li>one</li><li>two</li></ul></article></div>"
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        let md = out["body"].as_str().unwrap();
        // Structure preserved (heading + list items), unlike the flattening text path.
        assert!(md.contains("## Title"), "{md}");
        assert!(md.contains("- one") && md.contains("- two"), "{md}");
        // SKIP still applies inside the subtree (nav wasn't in <article> here, but
        // the point is html mode gives real structure): list items are delimited.
        assert!(!md.contains("onetwo"), "list items must not fuse: {md}");
    }

    #[test]
    fn each_yields_one_object_per_item_with_missing_fields_as_null() {
        // The list-page shape: 3 cards, the middle one missing its price. `each`
        // keeps each item's fields bound together — the 3rd card's price is NOT
        // silently shifted up (which parallel `all:true` arrays would do).
        let rules = ruleset(json!({
            "products": {
                "type": "each",
                "selector": ".card",
                "fields": {
                    "name": {"type": "css", "selector": "h3"},
                    "price": {"type": "css", "selector": ".price",
                              "transforms": [{"op": "to_number"}]}
                }
            }
        }));
        let doc = r#"
            <div class="card"><h3>A</h3><span class="price">$10</span></div>
            <div class="card"><h3>B</h3></div>
            <div class="card"><h3>C</h3><span class="price">$30</span></div>
        "#
        .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(
            out["products"],
            json!([
                {"name": "A", "price": 10.0},
                {"name": "B", "price": null},
                {"name": "C", "price": 30.0},
            ])
        );
    }

    #[test]
    fn each_rejects_json_and_xpath_inner_rules_at_compile() {
        let bad = serde_json::from_value::<RuleSet>(json!({
            "rows": {"type": "each", "selector": ".r",
                     "fields": {"x": {"type": "json", "pointer": "/a"}}}
        }))
        .unwrap()
        .compile();
        assert!(bad.is_err(), "json inner rule must be rejected inside each");
    }

    #[test]
    fn css_regex_and_const() {
        let rules = ruleset(json!({
            "title": {"type": "css", "selector": "h1"},
            "link":  {"type": "css", "selector": "a", "attr": "href"},
            "items": {"type": "css", "selector": "li", "all": true},
            "price": {"type": "regex", "pattern": "\\$([0-9]+)", "group": 1},
            "src":   {"type": "const", "value": "unit"}
        }));
        let doc =
            r#"<h1>Hi</h1><a href="/x">l</a><ul><li>a</li><li>b</li></ul> costs $42"#.to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["title"], json!("Hi"));
        assert_eq!(out["link"], json!("/x"));
        assert_eq!(out["items"], json!(["a", "b"]));
        assert_eq!(out["price"], json!("42"));
        assert_eq!(out["src"], json!("unit"));
    }

    #[test]
    fn json_pointer_via_simd() {
        let rules = ruleset(json!({
            "name": {"type": "json", "pointer": "/data/0/name"},
            "n":    {"type": "json", "pointer": "/count"}
        }));
        let doc = r#"{"count": 2, "data": [{"name": "Ada"}, {"name": "Bob"}]}"#.to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["name"], json!("Ada"));
        assert_eq!(out["n"], json!(2));
    }

    #[test]
    fn xpath_text_attribute_and_all() {
        let rules = ruleset(json!({
            "title": {"type": "xpath", "xpath": "//div[@class='main']/h2"},
            "href":  {"type": "xpath", "xpath": "//a/@href"},
            "items": {"type": "xpath", "xpath": "//li", "all": true},
            "none":  {"type": "xpath", "xpath": "//article"}
        }));
        let doc = r#"<html><body><div class="main"><h2> Deep Title </h2></div>
                     <a href="/next">n</a><ul><li>a</li><li>b</li></ul></body></html>"#
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["title"], json!("Deep Title"));
        assert_eq!(out["href"], json!("/next"));
        assert_eq!(out["items"], json!(["a", "b"]));
        assert_eq!(out["none"], json!(null));
        // Invalid XPath fails at compile time, not silently at extraction.
        assert!(serde_json::from_value::<RuleSet>(
            json!({ "x": {"type": "xpath", "xpath": "///"} })
        )
        .unwrap()
        .compile()
        .is_err());
    }

    #[test]
    fn transforms_coerce_and_chain() {
        let rules = ruleset(json!({
            "price": {"type": "regex", "pattern": "costs (\\$[0-9,.]+)", "group": 1,
                      "transforms": [{"op": "to_number"}]},
            "tags":  {"type": "css", "selector": "li", "all": true,
                      "transforms": [{"op": "lowercase"}, {"op": "trim"}]},
            "year":  {"type": "css", "selector": ".date",
                      "transforms": [{"op": "split", "sep": "-", "index": 0}, {"op": "to_int"}]},
            "missing": {"type": "css", "selector": ".nope",
                        "transforms": [{"op": "default", "value": "n/a"}]},
            "active": {"type": "css", "selector": ".flag",
                       "transforms": [{"op": "to_bool"}]}
        }));
        let doc = "<ul><li> Rust </li><li>WEB</li></ul><span class=\"date\">2026-07-10</span>\
                   <i class=\"flag\">Yes</i> costs $1,234.50"
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["price"], json!(1234.5));
        assert_eq!(out["tags"], json!(["rust", "web"]));
        assert_eq!(out["year"], json!(2026));
        assert_eq!(out["missing"], json!("n/a"));
        assert_eq!(out["active"], json!(true));
    }

    #[test]
    fn to_number_parses_first_valid_number() {
        // Drive coerce_number through a const rule + to_number transform.
        let cases = [
            ("1-2", json!(1.0)),              // range: not -12
            ("$1,234.50", json!(1234.5)),     // currency + thousands
            ("3.5%", json!(3.5)),             // trailing percent
            ("-5.5", json!(-5.5)),            // real negative
            ("2026-07-10", json!(2026.0)),    // date: first component only
            ("abc", json!(null)),             // no number -> null
            ("  42 ", json!(42.0)),           // surrounding whitespace
            ("Price: 9.99 USD", json!(9.99)), // embedded
        ];
        for (input, want) in cases {
            let rules = ruleset(json!({
                "n": {"type": "const", "value": input, "transforms": [{"op": "to_number"}]}
            }));
            let out = &extract_batch(&rules, std::slice::from_ref(&String::new()))[0];
            assert_eq!(out["n"], want, "input {input:?}");
        }
        // to_int truncates toward zero after the same parse.
        let rules = ruleset(json!({
            "n": {"type": "const", "value": "$1,234.90", "transforms": [{"op": "to_int"}]}
        }));
        let out = &extract_batch(&rules, std::slice::from_ref(&String::new()))[0];
        assert_eq!(out["n"], json!(1234));
    }

    #[test]
    fn report_statuses_per_rule_type() {
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "title":   {"type": "css", "selector": "h1"},        // matched
            "missing": {"type": "css", "selector": ".nope"},     // empty (absent)
            "blank":   {"type": "css", "selector": ".empty"},    // empty (whitespace only)
            "items":   {"type": "css", "selector": "li", "all": true}, // empty array
            "price":   {"type": "regex", "pattern": "\\$([0-9]+)", "group": 1}, // matched
            "noprice": {"type": "regex", "pattern": "€([0-9]+)", "group": 1},   // empty
            "name":    {"type": "json", "pointer": "/name"},     // error: body isn't JSON
            "lit":     {"type": "const", "value": "x"}           // matched
        }));
        let doc = r#"<h1>Hi</h1><span class="empty">   </span> costs $42"#.to_string();
        let (values, report) = extract_one_with_report(&rules, &doc);
        assert_eq!(report.fields["title"], FieldStatus::Matched);
        assert_eq!(report.fields["missing"], FieldStatus::Empty);
        assert_eq!(report.fields["blank"], FieldStatus::Empty);
        assert_eq!(report.fields["items"], FieldStatus::Empty);
        assert_eq!(report.fields["price"], FieldStatus::Matched);
        assert_eq!(report.fields["noprice"], FieldStatus::Empty);
        assert!(matches!(report.fields["name"], FieldStatus::Error { .. }));
        assert_eq!(report.fields["lit"], FieldStatus::Matched);
        // The value map still carries the extracted record alongside the report.
        assert_eq!(values["title"], json!("Hi"));

        // serde round-trips the tagged status enum (preview endpoint depends on it).
        let wire = serde_json::to_value(&report).unwrap();
        assert_eq!(wire["fields"]["title"], json!({"status": "matched"}));
        assert_eq!(wire["fields"]["name"]["status"], json!("error"));
        assert!(wire["fields"]["name"]["detail"].is_string());
    }

    #[test]
    fn coercion_status_separates_a_matched_selector_from_an_uncoercible_value() {
        use super::{extract_one_with_report, CoercionStatus, FieldStatus};
        let rules = ruleset(json!({
            // The wrong-element case: the selector matches, the value is prose.
            "price":  {"type": "css", "selector": ".p", "transforms": [{"op": "to_number"}]},
            // Same rule, a coercible value.
            "weight": {"type": "css", "selector": ".w", "transforms": [{"op": "to_number"}]},
            // No transforms → nothing to coerce.
            "title":  {"type": "css", "selector": "h1"},
            // A miss is not a coercion failure — there was nothing to coerce.
            "absent": {"type": "css", "selector": ".nope", "transforms": [{"op": "to_number"}]}
        }));
        let doc = r#"<h1>Hi</h1><span class="p">Add to cart</span><span class="w">2.5kg</span>"#
            .to_string();
        let (values, report) = extract_one_with_report(&rules, &doc);
        // The pre-transform status cannot see the problem: the selector matched.
        assert_eq!(report.fields["price"], FieldStatus::Matched);
        assert_eq!(values["price"], json!(null));
        // The post-transform status is what makes it visible.
        assert_eq!(report.coercion["price"], CoercionStatus::CoercionFailed);
        assert_eq!(report.coercion["weight"], CoercionStatus::Coerced);
        assert_eq!(report.coercion["title"], CoercionStatus::NoTransforms);
        assert_eq!(report.coercion["absent"], CoercionStatus::Coerced);
    }

    #[test]
    fn each_container_splits_a_quiet_listing_from_a_broken_selector() {
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "jobs": {"type": "each", "selector": ".job", "container": "#listing",
                     "fields": {"title": {"type": "css", "selector": "h3"}}}
        }));
        // Listing present with items → matched.
        let (_, full) = extract_one_with_report(
            &rules,
            r#"<div id="listing"><div class="job"><h3>A</h3></div></div>"#,
        );
        assert_eq!(full.fields["jobs"], FieldStatus::Matched);

        // Listing present, no postings this week → NOT a break.
        let (values, quiet) =
            extract_one_with_report(&rules, r#"<div id="listing"><p>No open roles</p></div>"#);
        assert_eq!(quiet.fields["jobs"], FieldStatus::ContainerEmpty);
        assert!(
            !quiet.fields["jobs"].is_miss(),
            "a quiet listing must not count as a miss"
        );
        assert_eq!(values["jobs"], json!([]));

        // Listing itself gone → the selector broke, and this IS a miss.
        let (_, broken) = extract_one_with_report(&rules, "<div id=\"other\"></div>");
        assert_eq!(broken.fields["jobs"], FieldStatus::Empty);
        assert!(broken.fields["jobs"].is_miss());

        // Without a container the two cases stay conflated (unchanged behaviour).
        let bare = ruleset(json!({
            "jobs": {"type": "each", "selector": ".job",
                     "fields": {"title": {"type": "css", "selector": "h3"}}}
        }));
        let (_, r) = extract_one_with_report(&bare, "<div id=\"listing\"></div>");
        assert_eq!(r.fields["jobs"], FieldStatus::Empty);
    }

    #[test]
    fn each_container_scopes_items_to_the_listing() {
        // Items outside the container are not the listing's items.
        let rules = ruleset(json!({
            "jobs": {"type": "each", "selector": ".job", "container": "#listing",
                     "fields": {"title": {"type": "css", "selector": "h3"}}}
        }));
        let doc = r#"<div id="listing"><div class="job"><h3>in</h3></div></div>
                     <div class="job"><h3>out</h3></div>"#
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["jobs"], json!([{"title": "in"}]));
    }

    #[test]
    fn report_error_vs_empty_for_json() {
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "present": {"type": "json", "pointer": "/a"},
            "absent":  {"type": "json", "pointer": "/missing"}
        }));
        // Valid JSON body: present matches, absent is a real miss (Empty, not Error).
        let (_, ok) = extract_one_with_report(&rules, r#"{"a": 1}"#);
        assert_eq!(ok.fields["present"], FieldStatus::Matched);
        assert_eq!(ok.fields["absent"], FieldStatus::Empty);
        // Non-JSON body: every json field is Error (bad input), not a silent miss.
        let (_, bad) = extract_one_with_report(&rules, "<html>not json</html>");
        assert!(matches!(bad.fields["present"], FieldStatus::Error { .. }));
        assert!(matches!(bad.fields["absent"], FieldStatus::Error { .. }));
    }

    #[test]
    fn compile_rejects_malformed_json_pointer() {
        // A pointer missing the leading '/' is invalid RFC 6901 — it must fail at
        // compile time, not become a silent Empty miss at extract time.
        let bad: RuleSet =
            serde_json::from_value(json!({ "bad": {"type": "json", "pointer": "a/b"} })).unwrap();
        assert!(
            bad.compile().is_err(),
            "malformed json pointer must fail compile"
        );
        // Valid pointers (empty or '/'-prefixed) still compile.
        let ok: RuleSet = serde_json::from_value(json!({
            "root": {"type": "json", "pointer": ""},
            "nested": {"type": "json", "pointer": "/a/b"}
        }))
        .unwrap();
        assert!(ok.compile().is_ok());
    }

    #[test]
    fn listing_rot_not_invisible() {
        // THE REFUTED BEHAVIOR: before per-inner-field reports, a listing whose
        // `price` selector had died still reported exactly one status for the
        // whole array — `FieldStatus::Matched`, because the array was full of
        // objects — and `report.fields` carried NO signal at all that every
        // single card's price was now null. `report.each` is that signal.
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "products": {
                "type": "each",
                "selector": ".card",
                "fields": {
                    "name":  {"type": "css", "selector": "h3"},
                    "price": {"type": "css", "selector": ".price"},
                    "badge": {"type": "css", "selector": ".badge"}
                }
            }
        }));
        // 3 cards: name always there, price NOWHERE (the site dropped the
        // class), badge on exactly one card (legitimately sparse).
        let healthy = r#"
            <div class="card"><h3>A</h3><span class="price">$10</span></div>
            <div class="card"><h3>B</h3><span class="price">$20</span></div>
            <div class="card"><h3>C</h3><span class="price">$30</span></div>
        "#;
        let rotted = r#"
            <div class="card"><h3>A</h3><span class="amount">$10</span><i class="badge">new</i></div>
            <div class="card"><h3>B</h3><span class="amount">$20</span></div>
            <div class="card"><h3>C</h3><span class="amount">$30</span></div>
        "#;

        let (_, before) = extract_one_with_report(&rules, healthy);
        assert_eq!(before.fields["products"], FieldStatus::Matched);
        assert_eq!(before.each["products.price"].items, 3);
        assert_eq!(before.each["products.price"].matched, 3);
        assert!(!before.each["products.price"].is_dead());

        let (values, after) = extract_one_with_report(&rules, rotted);
        // The listing rule itself STILL says Matched — that part is unchanged,
        // and it is exactly why the old report could not see the rot.
        assert_eq!(after.fields["products"], FieldStatus::Matched);
        assert!(!after.fields["products"].is_miss());
        assert_eq!(values["products"][0]["price"], json!(null));

        // ...but the inner report now shows it, and separates dead from sparse.
        let price = after.each["products.price"];
        assert_eq!((price.items, price.matched, price.empty), (3, 0, 3));
        assert_eq!(price.misses(), 3);
        assert_eq!(price.miss_rate(), 1.0);
        assert!(
            price.is_dead(),
            "a wholly-dead inner field must read as dead"
        );

        let badge = after.each["products.badge"];
        assert_eq!((badge.items, badge.matched, badge.misses()), (3, 1, 2));
        assert!(
            !badge.is_dead(),
            "a sparse field must NOT be confused with a dead one"
        );

        let name = after.each["products.name"];
        assert_eq!((name.items, name.matched, name.misses()), (3, 3, 0));

        // serde: the additive map rides the same report the preview endpoint
        // and the health detector already serialize.
        let wire = serde_json::to_value(&after).unwrap();
        assert_eq!(wire["each"]["products.price"]["items"], json!(3));
        assert_eq!(wire["each"]["products.price"]["empty"], json!(3));
        // Rule sets with no `each` rule carry no `each` key at all (skip_if_empty),
        // so an old consumer sees byte-identical JSON.
        let plain = ruleset(json!({"t": {"type": "css", "selector": "h1"}}));
        let (_, r) = extract_one_with_report(&plain, "<h1>x</h1>");
        assert!(r.each.is_empty());
        assert!(serde_json::to_value(&r).unwrap().get("each").is_none());
    }

    #[test]
    fn inner_reports_are_bounded_by_rule_width_not_listing_width() {
        // 5000 cards, 2 inner fields → 2 report entries. Counts, never lists.
        use super::extract_one_with_report;
        let rules = ruleset(json!({
            "rows": {"type": "each", "selector": ".r", "fields": {
                "a": {"type": "css", "selector": ".a"},
                "b": {"type": "css", "selector": ".b"}
            }}
        }));
        let doc: String = (0..5000)
            .map(|i| format!("<div class=\"r\"><span class=\"a\">{i}</span></div>"))
            .collect();
        let (_, report) = extract_one_with_report(&rules, &doc);
        assert_eq!(report.each.len(), 2);
        assert_eq!(report.each["rows.a"].items, 5000);
        assert_eq!(report.each["rows.a"].matched, 5000);
        assert!(report.each["rows.b"].is_dead());
    }

    #[test]
    fn nested_each_inner_fields_report_under_a_dotted_path() {
        use super::extract_one_with_report;
        let rules = ruleset(json!({
            "products": {"type": "each", "selector": ".card", "fields": {
                "name": {"type": "css", "selector": "h3"},
                "variants": {"type": "each", "selector": ".v", "container": ".variants",
                             "fields": {"sku": {"type": "css", "selector": ".sku"},
                                        "size": {"type": "css", "selector": ".size"}}}
            }}
        }));
        let doc = r#"
            <div class="card"><h3>A</h3><div class="variants">
              <div class="v"><span class="sku">a-1</span></div>
              <div class="v"><span class="sku">a-2</span></div>
            </div></div>
            <div class="card"><h3>B</h3><div class="variants"></div></div>
        "#;
        let (_, report) = extract_one_with_report(&rules, doc);
        // Two cards; the second's variants container matched but was quiet, so
        // the nested `each` field is a hit on that item, not a miss.
        let variants = report.each["products.variants"];
        assert_eq!((variants.items, variants.matched), (2, 1));
        assert_eq!(variants.container_empty, 1);
        assert_eq!(variants.misses(), 0);
        assert!(!variants.is_dead());
        // The nested inner fields report under the dotted path, over the ITEMS
        // of the nested listing (2 variants total, both on card A).
        let sku = report.each["products.variants.sku"];
        assert_eq!((sku.items, sku.matched), (2, 2));
        let size = report.each["products.variants.size"];
        assert_eq!((size.items, size.matched, size.empty), (2, 0, 2));
        assert!(size.is_dead());
    }

    #[test]
    fn empty_listing_reports_zero_items_not_a_dead_field() {
        // A container that matched but held nothing: every inner field is an
        // honest `items: 0` row — present in the report (so it is discoverable)
        // and NOT dead (there was nothing to extract from).
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "jobs": {"type": "each", "selector": ".job", "container": "#listing",
                     "fields": {"title": {"type": "css", "selector": "h3"}}}
        }));
        let (_, r) = extract_one_with_report(&rules, r#"<div id="listing"><p>none</p></div>"#);
        assert_eq!(r.fields["jobs"], FieldStatus::ContainerEmpty);
        let title = r.each["jobs.title"];
        assert_eq!((title.items, title.matched, title.misses()), (0, 0, 0));
        assert!(!title.is_dead());
        assert_eq!(title.miss_rate(), 0.0);
    }

    #[test]
    fn inner_stats_count_a_hit_exactly_like_field_status_is_miss() {
        // The two miss conventions must not drift: `InnerFieldStats::misses`
        // counts precisely the statuses `FieldStatus::is_miss` returns true for.
        use super::{FieldStatus, InnerFieldStats};
        for status in [
            FieldStatus::Matched,
            FieldStatus::Empty,
            FieldStatus::ContainerEmpty,
            FieldStatus::Error { detail: "x".into() },
        ] {
            let mut s = InnerFieldStats::default();
            s.push(&status);
            assert_eq!(
                s.misses() == 1,
                status.is_miss(),
                "miss convention diverged for {status:?}"
            );
            assert_eq!(s.hits() + s.misses(), s.items);
        }
    }

    #[test]
    fn xpath_atomics_are_typed_values_not_debug_strings() {
        // THE REFUTED BEHAVIOR: every non-node XPath result went through
        // `format!("{other:?}")`, so `count(//li)` stored the Rust Debug dump
        // `"AnyAtomicType(Integer(3))"` — engine internals written into the
        // dataset as if they were extracted data, and `matched` in the report.
        let rules = ruleset(json!({
            "n":     {"type": "xpath", "xpath": "count(//li)"},
            "title": {"type": "xpath", "xpath": "string(//h1)"},
            "none":  {"type": "xpath", "xpath": "not(//article)"},
            "num":   {"type": "xpath", "xpath": "number('3.5')"}
        }));
        let doc = "<html><body><h1>Hi</h1><ul><li>a</li><li>b</li><li>c</li></ul></body></html>"
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["n"], json!(3), "count() must be a JSON number");
        assert!(out["n"].is_number());
        assert_eq!(out["title"], json!("Hi"), "string() must be a JSON string");
        assert_eq!(out["none"], json!(true), "not() must be a JSON boolean");
        assert!(out["none"].is_boolean());
        assert_eq!(out["num"], json!(3.5));
        // No value anywhere carries the Debug shape of the engine's own enums.
        let wire = serde_json::to_string(out).unwrap();
        assert!(!wire.contains("AnyAtomicType"), "{wire}");
        assert!(!wire.contains("Integer("), "{wire}");
    }

    #[test]
    fn xpath_error_not_empty() {
        // An expression that PARSES but cannot evaluate (an unsupported
        // function, an undefined variable) used to return Null, which
        // classified as `Empty` — "the site had nothing here". That is a claim
        // about the document, and a rule that never evaluated learned nothing
        // about the document.
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "broken": {"type": "xpath", "xpath": "unknown-fn()"},
            "unbound": {"type": "xpath", "xpath": "//div[$missing]"},
            "fine":   {"type": "xpath", "xpath": "//h1"},
            "absent": {"type": "xpath", "xpath": "//article"}
        }));
        let (values, report) =
            extract_one_with_report(&rules, "<html><body><div><h1>Hi</h1></div></body></html>");
        let FieldStatus::Error { detail } = &report.fields["broken"] else {
            panic!(
                "a runtime xpath failure must be Error, got {:?}",
                report.fields["broken"]
            );
        };
        assert!(detail.contains("xpath"), "{detail}");
        assert!(report.fields["broken"].is_miss());
        assert!(matches!(
            report.fields["unbound"],
            FieldStatus::Error { .. }
        ));
        assert_eq!(values["broken"], json!(null));
        // A working rule is unaffected, and a rule that ran and found nothing
        // is STILL `Empty` — the honest miss keeps its own name.
        assert_eq!(report.fields["fine"], FieldStatus::Matched);
        assert_eq!(report.fields["absent"], FieldStatus::Empty);
    }

    #[test]
    fn default_fires_on_blank_not_just_null() {
        // `default` fired only on `Value::Null`, while the status system calls
        // null / whitespace-only string / empty array all "blank". A selector
        // that matched an empty <span> therefore reported `empty` AND kept the
        // `""` — the declared default silently never applied.
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "absent": {"type": "css", "selector": ".nope",
                       "transforms": [{"op": "default", "value": "n/a"}]},
            "blank":  {"type": "css", "selector": ".blank",
                       "transforms": [{"op": "default", "value": "n/a"}]},
            "spaces": {"type": "const", "value": "   ",
                       "transforms": [{"op": "default", "value": "n/a"}]},
            "list":   {"type": "css", "selector": ".none", "all": true,
                       "transforms": [{"op": "default", "value": []}]},
            "kept":   {"type": "css", "selector": "h1",
                       "transforms": [{"op": "default", "value": "n/a"}]},
            "zero":   {"type": "const", "value": 0,
                       "transforms": [{"op": "default", "value": "n/a"}]},
            "false":  {"type": "const", "value": false,
                       "transforms": [{"op": "default", "value": "n/a"}]}
        }));
        let doc = r#"<h1>Hi</h1><span class="blank">   </span>"#;
        let (values, report) = extract_one_with_report(&rules, doc);
        // The existing null case is preserved exactly (it is a subset of blank).
        assert_eq!(values["absent"], json!("n/a"));
        // ...and the cases the status system already called empty now agree.
        assert_eq!(report.fields["blank"], FieldStatus::Empty);
        assert_eq!(values["blank"], json!("n/a"));
        assert_eq!(values["spaces"], json!("n/a"));
        assert_eq!(values["list"], json!([]));
        // A real value is never replaced — including falsey ones, which are
        // data, not absence.
        assert_eq!(values["kept"], json!("Hi"));
        assert_eq!(values["zero"], json!(0));
        assert_eq!(values["false"], json!(false));
    }

    #[test]
    fn default_agrees_with_the_status_predicate_on_every_shape() {
        // The structural guard: `default` fires on EXACTLY the values the
        // report calls blank. One predicate, two consumers — they cannot drift.
        use super::{is_blank, CompiledTransform};
        let d = CompiledTransform::Default {
            value: json!("FILLED"),
        };
        for v in [
            json!(null),
            json!(""),
            json!("   "),
            json!([]),
            json!("x"),
            json!(0),
            json!(false),
            json!(["a"]),
            json!({}),
        ] {
            let blank = is_blank(&v);
            let filled = d.apply(v.clone()) == json!("FILLED");
            assert_eq!(blank, filled, "default/is_blank diverged on {v}");
        }
    }

    #[test]
    fn to_int_overflow_is_null_not_a_saturated_number() {
        // A 400-digit string parses to f64::INFINITY. `to_number` refused it
        // (null), while `to_int`'s `as i64` cast SATURATED it to
        // 9223372036854775807 — a fabricated number indistinguishable from a
        // real one. The two now give the same answer.
        let huge = format!("1{}", "0".repeat(400));
        let pair = |input: &str| {
            let rules = ruleset(json!({
                "i": {"type": "const", "value": input, "transforms": [{"op": "to_int"}]},
                "f": {"type": "const", "value": input, "transforms": [{"op": "to_number"}]}
            }));
            let out = extract_batch(&rules, std::slice::from_ref(&String::new()))[0].clone();
            (out["i"].clone(), out["f"].clone())
        };

        // NON-FINITE: the two agree, and the answer is null at both precisions.
        for input in [huge.as_str(), &format!("-{huge}")] {
            let (i, f) = pair(input);
            assert_eq!(i, json!(null), "to_int({input:?}) must not saturate");
            assert_eq!(f, json!(null), "to_number({input:?})");
        }

        // FINITE but outside i64: they legitimately differ, because an f64
        // holds 1e20 exactly and an i64 cannot hold it at all — so `to_int`
        // says null (it has no integer to give) rather than clamping to
        // i64::MAX, and `to_number` keeps the double it really parsed.
        for input in ["99999999999999999999", "-99999999999999999999"] {
            let (i, f) = pair(input);
            assert_eq!(i, json!(null), "to_int({input:?}) must not saturate");
            assert!(f.is_number(), "to_number({input:?}) = {f}");
        }

        // Ordinary values are untouched by the guard.
        assert_eq!(pair("42"), (json!(42), json!(42.0)));
        assert_eq!(pair("-5.9"), (json!(-5), json!(-5.9)));
        // No exponent parsing: the first number in "1e999" is 1, not infinity.
        assert_eq!(pair("1e999"), (json!(1), json!(1.0)));
    }

    #[test]
    fn uppercase_and_regex_replace_map_strings_and_pass_others_through() {
        // Both transforms shipped with zero coverage: the only two in the
        // catalogue nothing pinned.
        let rules = ruleset(json!({
            "up":    {"type": "css", "selector": "h1", "transforms": [{"op": "uppercase"}]},
            "each":  {"type": "css", "selector": "li", "all": true,
                      "transforms": [{"op": "uppercase"}]},
            "slug":  {"type": "css", "selector": ".t",
                      "transforms": [{"op": "regex_replace",
                                      "pattern": "\\s+", "replacement": "-"},
                                     {"op": "lowercase"}]},
            "caps":  {"type": "css", "selector": ".d",
                      "transforms": [{"op": "regex_replace",
                                      "pattern": "(\\d{4})-(\\d{2})",
                                      "replacement": "$2/$1"}]},
            "num":   {"type": "const", "value": 7, "transforms": [{"op": "uppercase"}]},
            "miss":  {"type": "css", "selector": ".nope",
                      "transforms": [{"op": "regex_replace", "pattern": "a", "replacement": "b"}]}
        }));
        let doc = r#"<h1>hi there</h1><ul><li>a</li><li>b</li></ul>
                     <span class="t">Rust  Is Fast</span><span class="d">2026-07</span>"#
            .to_string();
        let out = &extract_batch(&rules, std::slice::from_ref(&doc))[0];
        assert_eq!(out["up"], json!("HI THERE"));
        assert_eq!(out["each"], json!(["A", "B"]), "element-wise over arrays");
        assert_eq!(out["slug"], json!("rust-is-fast"));
        assert_eq!(out["caps"], json!("07/2026"), "$N capture references");
        // Non-strings pass through untouched; a miss stays a miss.
        assert_eq!(out["num"], json!(7));
        assert_eq!(out["miss"], json!(null));
    }

    #[test]
    fn parallel_batch_preserves_order() {
        let rules = ruleset(json!({ "h": {"type": "css", "selector": "h1"} }));
        let docs: Vec<String> = (0..500).map(|i| format!("<h1>{i}</h1>")).collect();
        let out = extract_batch(&rules, &docs);
        assert_eq!(out.len(), 500);
        assert_eq!(out[0]["h"], json!("0"));
        assert_eq!(out[499]["h"], json!("499"));
    }
}
