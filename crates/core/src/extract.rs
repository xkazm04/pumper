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

/// A set of fields to extract from each document.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuleSet {
    pub fields: BTreeMap<String, Rule>,
}

impl RuleSet {
    /// Validates and pre-compiles selectors/regexes once for reuse across the
    /// whole batch.
    pub fn compile(&self) -> Result<CompiledRuleSet> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for (name, rule) in &self.fields {
            let compiled = match rule {
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
            fields.push((name.clone(), compiled));
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

/// Compiled, thread-shareable rule set. `Send + Sync` so a `&CompiledRuleSet`
/// can drive every rayon worker in parallel.
pub struct CompiledRuleSet {
    fields: Vec<(String, CompiledRule)>,
}

impl CompiledRuleSet {
    fn needs_html(&self) -> bool {
        self.fields.iter().any(|(_, r)| matches!(r, CompiledRule::Css { .. }))
    }

    fn needs_json(&self) -> bool {
        self.fields.iter().any(|(_, r)| matches!(r, CompiledRule::Json { .. }))
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
    for (name, rule) in &rules.fields {
        let value = match rule {
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
    fn parallel_batch_preserves_order() {
        let rules = ruleset(json!({ "h": {"type": "css", "selector": "h1"} }));
        let docs: Vec<String> = (0..500).map(|i| format!("<h1>{i}</h1>")).collect();
        let out = extract_batch(&rules, &docs);
        assert_eq!(out.len(), 500);
        assert_eq!(out[0]["h"], json!("0"));
        assert_eq!(out[499]["h"], json!("499"));
    }
}
