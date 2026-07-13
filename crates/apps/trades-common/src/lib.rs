//! Shared layer for the four agentic US-trades reference apps (trade-wages,
//! homewyse-pricing, state-tax, valuation-multiples).
//!
//! Two concerns live here so they stay consistent across all four apps:
//!   - [`salvage_json`]: recover a JSON object the agent emitted but the engine
//!     couldn't parse (markdown fence / surrounding prose). One pass, no re-run,
//!     no cost — it works on text already paid for.
//!   - [`validate`]: plausibility guards (monotone bands, rate ranges, positive
//!     magnitudes) so a nonsensical record is rejected with per-record detail
//!     instead of silently upserted.

use serde_json::Value;

/// Best-effort recovery of a JSON object the agent emitted but the engine
/// couldn't parse into `output.json` — the common failure is a markdown
/// ```json fence or a leading/trailing sentence. No re-run, no cost: it works
/// on the raw text we already paid for. Returns None only when there's no
/// parseable object at all.
pub fn salvage_json(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return Some(v);
    }
    let unfenced = strip_code_fence(trimmed);
    if let Ok(v) = serde_json::from_str::<Value>(unfenced.trim()) {
        return Some(v);
    }
    let span = first_balanced_object(unfenced)?;
    serde_json::from_str::<Value>(span).ok()
}

/// Strip a leading ```json (or bare ```) fence and its closing ``` if present.
fn strip_code_fence(text: &str) -> &str {
    let t = text.trim();
    let Some(rest) = t.strip_prefix("```") else {
        return t;
    };
    // drop the optional language tag on the fence's first line
    let rest = rest.split_once('\n').map(|(_, r)| r).unwrap_or(rest);
    rest.strip_suffix("```").unwrap_or(rest).trim()
}

/// The first brace-balanced `{...}` span in `text`, respecting quoted strings
/// and escapes so a `}` inside a string value doesn't close the object early.
fn first_balanced_object(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let start = text.find('{')?;
    let mut depth = 0usize;
    let mut in_str = false;
    let mut escaped = false;
    for i in start..bytes.len() {
        let c = bytes[i];
        if in_str {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_str = false;
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[start..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Plausibility validation for parsed trades records. These are cheap sanity
/// gates — NOT a re-run loop: a record that fails is rejected (with reasons)
/// and reported in the job result; valid siblings still upsert. The agent's
/// answer is already paid for, so there is no retry.
pub mod validate {
    use serde_json::Value;

    /// A rejected record: its dataset key and the plausibility reasons it failed.
    #[derive(Debug, Clone)]
    pub struct Rejection {
        pub key: String,
        pub reasons: Vec<String>,
    }

    impl Rejection {
        pub fn to_json(&self) -> Value {
            serde_json::json!({ "key": self.key, "reasons": self.reasons })
        }
    }

    /// Numeric field accessor tolerant of JSON numbers and numeric strings
    /// (the agent sometimes quotes a figure, e.g. `"30.10"`).
    pub fn num(rec: &Value, field: &str) -> Option<f64> {
        match rec.get(field) {
            Some(Value::Number(n)) => n.as_f64(),
            Some(Value::String(s)) => s.trim().replace([',', '$'], "").parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Push a violation if the ordering low ≤ median ≤ high is broken. Values
    /// that are absent are skipped — presence is a schema concern, not a
    /// plausibility one — but any present pair must be ordered.
    pub fn require_monotone(
        reasons: &mut Vec<String>,
        label: &str,
        low: Option<f64>,
        median: Option<f64>,
        high: Option<f64>,
    ) {
        if let (Some(l), Some(m)) = (low, median) {
            if l > m {
                reasons.push(format!("{label}: low {l} > median {m}"));
            }
        }
        if let (Some(m), Some(h)) = (median, high) {
            if m > h {
                reasons.push(format!("{label}: median {m} > high {h}"));
            }
        }
        if let (Some(l), Some(h)) = (low, high) {
            if l > h {
                reasons.push(format!("{label}: low {l} > high {h}"));
            }
        }
    }

    /// Push a violation if the value is present and not strictly positive.
    pub fn require_positive(reasons: &mut Vec<String>, label: &str, v: Option<f64>) {
        if let Some(v) = v {
            if v <= 0.0 {
                reasons.push(format!("{label}: {v} not > 0"));
            }
        }
    }

    /// Push a violation if the value is present and outside the percentage
    /// range [0, 100].
    pub fn require_rate(reasons: &mut Vec<String>, label: &str, v: Option<f64>) {
        if let Some(v) = v {
            if !(0.0..=100.0).contains(&v) {
                reasons.push(format!("{label}: rate {v} outside [0,100]"));
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[test]
        fn num_reads_numbers_and_numeric_strings() {
            let rec = json!({ "a": 30.1, "b": "1,200", "c": "$45.5", "d": "x" });
            assert_eq!(num(&rec, "a"), Some(30.1));
            assert_eq!(num(&rec, "b"), Some(1200.0));
            assert_eq!(num(&rec, "c"), Some(45.5));
            assert_eq!(num(&rec, "d"), None);
            assert_eq!(num(&rec, "missing"), None);
        }

        #[test]
        fn monotone_flags_out_of_order_bands() {
            let mut r = Vec::new();
            require_monotone(&mut r, "band", Some(1.0), Some(2.0), Some(3.0));
            assert!(r.is_empty());
            require_monotone(&mut r, "band", Some(5.0), Some(2.0), Some(3.0));
            assert_eq!(r.len(), 2); // low>median and low>high
        }

        #[test]
        fn monotone_skips_absent_values() {
            let mut r = Vec::new();
            require_monotone(&mut r, "band", None, Some(2.0), None);
            assert!(r.is_empty());
        }

        #[test]
        fn positive_flags_zero_and_negative() {
            let mut r = Vec::new();
            require_positive(&mut r, "wage", Some(10.0));
            assert!(r.is_empty());
            require_positive(&mut r, "wage", Some(0.0));
            require_positive(&mut r, "wage", Some(-1.0));
            assert_eq!(r.len(), 2);
        }

        #[test]
        fn rate_flags_out_of_range() {
            let mut r = Vec::new();
            require_rate(&mut r, "top", Some(0.0));
            require_rate(&mut r, "top", Some(13.3));
            require_rate(&mut r, "top", Some(100.0));
            assert!(r.is_empty());
            require_rate(&mut r, "top", Some(-1.0));
            require_rate(&mut r, "top", Some(133.0));
            assert_eq!(r.len(), 2);
        }
    }
}

#[cfg(test)]
mod salvage_tests {
    use super::*;

    #[test]
    fn salvages_a_clean_object() {
        let v = salvage_json(r#"{"locality":"Texas","trades":[]}"#).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_a_fenced_object() {
        let raw = "```json\n{\"locality\":\"Texas\",\"trades\":[]}\n```";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
    }

    #[test]
    fn salvages_an_object_wrapped_in_prose() {
        let raw = "Here is the pricing data you asked for:\n{\"locality\":\"Texas\",\
                   \"trades\":[{\"trade\":\"Plumbing\",\"jobs\":[]}]}\nHope that helps!";
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["locality"], "Texas");
        assert_eq!(v["trades"][0]["trade"], "Plumbing");
    }

    #[test]
    fn does_not_close_early_on_a_brace_inside_a_string() {
        let raw = r#"prefix {"note":"a } inside a string","ok":true} suffix"#;
        let v = salvage_json(raw).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["note"], "a } inside a string");
    }

    #[test]
    fn returns_none_when_there_is_no_object() {
        assert!(salvage_json("I could not find reliable pricing data.").is_none());
    }
}
