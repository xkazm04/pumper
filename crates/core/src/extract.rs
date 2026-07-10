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
                Rule::Json { pointer } => CompiledRule::Json { pointer: pointer.clone() },
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
        Value::String(s) => {
            let cleaned: String = s
                .chars()
                .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
                .collect();
            cleaned.parse::<f64>().ok()
        }
        _ => None,
    };
    match num {
        Some(n) if int => Value::from(n.trunc() as i64),
        Some(n) => serde_json::Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null),
        None => Value::Null,
    }
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
}

/// Extracts one document into a JSON object. HTML is parsed at most once (only
/// if any CSS rule needs it); the JSON body is parsed at most once with
/// simd-json (only if any JSON rule needs it).
pub fn extract_one(rules: &CompiledRuleSet, doc: &str) -> Value {
    let html = rules.needs_html().then(|| Html::parse_document(doc));
    let json = if rules.needs_json() {
        let mut bytes = doc.as_bytes().to_vec();
        simd_json::serde::from_slice::<Value>(&mut bytes).ok()
    } else {
        None
    };

    let mut obj = Map::with_capacity(rules.fields.len());
    for (name, rule, transforms) in &rules.fields {
        let mut value = match rule {
            CompiledRule::Css { selector, attr, all } => {
                css_extract(html.as_ref().unwrap(), selector, attr.as_deref(), *all)
            }
            CompiledRule::Regex { re, group } => re
                .captures(doc)
                .and_then(|c| c.get(*group))
                .map(|m| Value::String(m.as_str().to_string()))
                .unwrap_or(Value::Null),
            CompiledRule::Json { pointer } => json
                .as_ref()
                .and_then(|j| j.pointer(pointer).cloned())
                .unwrap_or(Value::Null),
            CompiledRule::Const { value } => value.clone(),
        };
        for t in transforms {
            value = t.apply(value);
        }
        obj.insert(name.clone(), value);
    }
    Value::Object(obj)
}

/// Extracts a whole batch in parallel across all cores.
pub fn extract_batch(rules: &CompiledRuleSet, docs: &[String]) -> Vec<Value> {
    docs.par_iter().map(|doc| extract_one(rules, doc)).collect()
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
    fn parallel_batch_preserves_order() {
        let rules = ruleset(json!({ "h": {"type": "css", "selector": "h1"} }));
        let docs: Vec<String> = (0..500).map(|i| format!("<h1>{i}</h1>")).collect();
        let out = extract_batch(&rules, &docs);
        assert_eq!(out.len(), 500);
        assert_eq!(out[0]["h"], json!("0"));
        assert_eq!(out[499]["h"], json!("499"));
    }
}
