//! Multi-core, SIMD-accelerated extraction engine with a declarative rule set.
//!
//! A `RuleSet` maps output fields to extraction rules (CSS / regex / JSON
//! pointer / constant). Rules are compiled once, then `extract_batch` runs them
//! over a slice of documents across all CPU cores via rayon — no GIL, so a
//! whole batch is parsed and extracted in parallel in one process. JSON rules
//! parse with `simd-json` (SIMD, GB/s). This is the throughput path a Python
//! stack can't match in-process: the GIL serializes CPU-bound parsing, and
//! scaling out means `multiprocessing` with pickle overhead across processes.

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
    /// CSS selector → text (or an attribute); `all` collects every match.
    Css {
        selector: String,
        #[serde(default)]
        attr: Option<String>,
        #[serde(default)]
        all: bool,
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
    RegexReplace { pattern: String, replacement: String },
    /// Split a string by `sep`; `index` picks one part (else keeps the array).
    Split {
        sep: String,
        #[serde(default)]
        index: Option<usize>,
    },
    /// Replace a null result with this value.
    Default { value: Value },
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
        let mut fields = Vec::with_capacity(self.fields.len());
        for (name, field) in &self.fields {
            let compiled = match &field.rule {
                Rule::Css { selector, attr, all } => {
                    let sel = Selector::parse(selector).map_err(|e| {
                        Error::Parse(format!("bad css selector '{selector}': {e:?}"))
                    })?;
                    CompiledRule::Css { selector: sel, attr: attr.clone(), all: *all }
                }
                Rule::Regex { pattern, group } => {
                    let re = Regex::new(pattern)
                        .map_err(|e| Error::Parse(format!("bad regex '{pattern}': {e}")))?;
                    CompiledRule::Regex { re, group: *group }
                }
                Rule::Json { pointer } => {
                    // RFC 6901: a pointer is the empty string or begins with '/'.
                    // Validate here like css/regex/xpath so a malformed pointer is
                    // an Error at compile time, not an indistinguishable Empty miss
                    // at extract time (which defeats the DocReport/FieldStatus signal).
                    if !pointer.is_empty() && !pointer.starts_with('/') {
                        return Err(Error::Parse(format!(
                            "bad json pointer '{pointer}': must be empty or start with '/'"
                        )));
                    }
                    CompiledRule::Json { pointer: pointer.clone() }
                }
                Rule::Xpath { xpath, all } => {
                    let parsed = skyscraper::xpath::parse(xpath)
                        .map_err(|e| Error::Parse(format!("bad xpath '{xpath}': {e}")))?;
                    CompiledRule::Xpath { xpath: parsed, all: *all }
                }
                Rule::Const { value } => CompiledRule::Const { value: value.clone() },
            };
            let transforms = field
                .transforms
                .iter()
                .map(|t| CompiledTransform::compile(t.clone()))
                .collect::<Result<Vec<_>>>()?;
            fields.push((name.clone(), compiled, transforms));
        }
        Ok(CompiledRuleSet { fields })
    }
}

enum CompiledRule {
    Css { selector: Selector, attr: Option<String>, all: bool },
    Regex { re: Regex, group: usize },
    Json { pointer: String },
    Xpath { xpath: skyscraper::xpath::Xpath, all: bool },
    Const { value: Value },
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
            Transform::RegexReplace { pattern, replacement } => Self::RegexReplace {
                re: Regex::new(&pattern)
                    .map_err(|e| Error::Parse(format!("bad transform regex '{pattern}': {e}")))?,
                replacement,
            },
            Transform::Split { sep, index } => Self::Split { sep, index },
            Transform::Default { value } => Self::Default { value },
        })
    }

    /// Applies to one value; arrays are mapped element-wise (except `default`).
    fn apply(&self, value: Value) -> Value {
        match (self, value) {
            (Self::Default { value: d }, Value::Null) => d.clone(),
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
                        parts.into_iter().map(|p| Value::String(p.trim().to_string())).collect(),
                    ),
                }
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
        Some(n) if int => Value::from(n.trunc() as i64),
        Some(n) => serde_json::Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null),
        None => Value::Null,
    }
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
        self.fields.iter().any(|(_, r, _)| matches!(r, CompiledRule::Css { .. }))
    }

    fn needs_json(&self) -> bool {
        self.fields.iter().any(|(_, r, _)| matches!(r, CompiledRule::Json { .. }))
    }

    fn needs_xpath(&self) -> bool {
        self.fields.iter().any(|(_, r, _)| matches!(r, CompiledRule::Xpath { .. }))
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
    /// The rule could not run: the document was not in the format the rule
    /// needs (e.g. a `json` rule over a body that is not JSON, or an `xpath`
    /// rule over unparseable HTML). Distinguishes a bad input from a real miss.
    Error { detail: String },
}

impl FieldStatus {
    /// Classifies a rule's raw (pre-transform) output. `ran` is false when the
    /// rule's required parse failed, so the rule never actually evaluated.
    fn classify(ran: bool, raw: &Value, detail: &str) -> FieldStatus {
        if !ran {
            return FieldStatus::Error { detail: detail.to_string() };
        }
        match raw {
            Value::Null => FieldStatus::Empty,
            Value::String(s) if s.trim().is_empty() => FieldStatus::Empty,
            Value::Array(a) if a.is_empty() => FieldStatus::Empty,
            _ => FieldStatus::Matched,
        }
    }
}

/// Per-document field-status map — the report companion to an extracted record.
/// Status reflects the rule match (before transforms), so it answers "did the
/// selector find anything?" independent of downstream coercion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DocReport {
    pub fields: BTreeMap<String, FieldStatus>,
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
        // (raw value, whether the rule's required parse was available, error detail)
        let (mut value, ran, detail): (Value, bool, &str) = match rule {
            CompiledRule::Css { selector, attr, all } => {
                (css_extract(html.as_ref().unwrap(), selector, attr.as_deref(), *all), true, "")
            }
            CompiledRule::Regex { re, group } => (
                re.captures(doc)
                    .and_then(|c| c.get(*group))
                    .map(|m| Value::String(m.as_str().to_string()))
                    .unwrap_or(Value::Null),
                true,
                "",
            ),
            CompiledRule::Json { pointer } => match json.as_ref() {
                Some(j) => (j.pointer(pointer).cloned().unwrap_or(Value::Null), true, ""),
                None => (Value::Null, false, "body did not parse as JSON"),
            },
            CompiledRule::Xpath { xpath, all } => match xpath_tree.as_ref() {
                Some(tree) => (xpath_extract(tree, xpath, *all), true, ""),
                None => (Value::Null, false, "document did not parse as HTML for xpath"),
            },
            CompiledRule::Const { value } => (value.clone(), true, ""),
        };
        if want_report {
            report.fields.insert(name.clone(), FieldStatus::classify(ran, &value, detail));
        }
        for t in transforms {
            value = t.apply(value);
        }
        obj.insert(name.clone(), value);
    }
    (Value::Object(obj), report)
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
    docs.par_iter().map(|doc| extract_one_with_report(rules, doc)).collect()
}

fn xpath_extract(
    tree: &skyscraper::xpath::XpathItemTree,
    xpath: &skyscraper::xpath::Xpath,
    all: bool,
) -> Value {
    let Ok(items) = xpath.apply(tree) else {
        return Value::Null;
    };
    let mut values = items.iter().map(|item| xpath_item_value(item, tree));
    if all {
        Value::Array(values.collect())
    } else {
        values.next().unwrap_or(Value::Null)
    }
}

/// One XPath result as JSON: attribute nodes yield their value, text nodes
/// their content, elements their recursive text; atomics render as strings.
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
        other => Value::String(format!("{other:?}")),
    }
}

fn css_extract(html: &Html, selector: &Selector, attr: Option<&str>, all: bool) -> Value {
    let render = |el: ElementRef| -> Value {
        match attr {
            Some(a) => el
                .value()
                .attr(a)
                .map(|s| Value::String(s.to_string()))
                .unwrap_or(Value::Null),
            None => Value::String(el.text().collect::<String>().trim().to_string()),
        }
    };
    if all {
        Value::Array(html.select(selector).map(render).collect())
    } else {
        html.select(selector).next().map(render).unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::{extract_batch, RuleSet};
    use serde_json::json;

    fn ruleset(v: serde_json::Value) -> super::CompiledRuleSet {
        serde_json::from_value::<RuleSet>(v).unwrap().compile().unwrap()
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
        let doc = r#"<h1>Hi</h1><a href="/x">l</a><ul><li>a</li><li>b</li></ul> costs $42"#
            .to_string();
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
            ("1-2", json!(1.0)),          // range: not -12
            ("$1,234.50", json!(1234.5)), // currency + thousands
            ("3.5%", json!(3.5)),        // trailing percent
            ("-5.5", json!(-5.5)),       // real negative
            ("2026-07-10", json!(2026.0)), // date: first component only
            ("abc", json!(null)),        // no number -> null
            ("  42 ", json!(42.0)),      // surrounding whitespace
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
        assert_eq!(wire["title"], json!({"status": "matched"}));
        assert_eq!(wire["name"]["status"], json!("error"));
        assert!(wire["name"]["detail"].is_string());
    }

    #[test]
    fn report_error_vs_empty_for_json() {
        use super::{extract_one_with_report, FieldStatus};
        let rules = ruleset(json!({
            "present": {"type": "json", "pointer": "/a"},
            "absent":  {"type": "json", "pointer": "/missing"}
        }));
        // Valid JSON body: present matches, absent is a real miss (Empty, not Error).
        let (_, ok) = extract_one_with_report(&rules, &r#"{"a": 1}"#.to_string());
        assert_eq!(ok.fields["present"], FieldStatus::Matched);
        assert_eq!(ok.fields["absent"], FieldStatus::Empty);
        // Non-JSON body: every json field is Error (bad input), not a silent miss.
        let (_, bad) = extract_one_with_report(&rules, &"<html>not json</html>".to_string());
        assert!(matches!(bad.fields["present"], FieldStatus::Error { .. }));
        assert!(matches!(bad.fields["absent"], FieldStatus::Error { .. }));
    }

    #[test]
    fn compile_rejects_malformed_json_pointer() {
        // A pointer missing the leading '/' is invalid RFC 6901 — it must fail at
        // compile time, not become a silent Empty miss at extract time.
        let bad: RuleSet =
            serde_json::from_value(json!({ "bad": {"type": "json", "pointer": "a/b"} })).unwrap();
        assert!(bad.compile().is_err(), "malformed json pointer must fail compile");
        // Valid pointers (empty or '/'-prefixed) still compile.
        let ok: RuleSet = serde_json::from_value(json!({
            "root": {"type": "json", "pointer": ""},
            "nested": {"type": "json", "pointer": "/a/b"}
        }))
        .unwrap();
        assert!(ok.compile().is_ok());
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
