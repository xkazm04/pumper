//! Recover a JSON object an agent emitted but wrapped — in a ```` ```json ````
//! fence or behind a leading sentence — so a valid answer isn't discarded (and
//! re-paid for) just because it wasn't bare JSON. Lives beside [`ResearchOutput`]
//! since every agentic app faces the same "the model fenced its output" case.

use serde_json::Value;

/// Best-effort recovery of a JSON value from raw agent text: try it verbatim,
/// then unfenced, then the first balanced `{…}` span. `None` when nothing parses.
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

/// Strips a leading ```` ```lang ```` fence and its trailing ```` ``` ````.
fn strip_code_fence(text: &str) -> &str {
    let t = text.trim();
    let Some(rest) = t.strip_prefix("```") else {
        return t;
    };
    // drop the optional language tag on the fence's first line
    let rest = rest.split_once('\n').map(|(_, r)| r).unwrap_or(rest);
    rest.strip_suffix("```").unwrap_or(rest).trim()
}

/// The first brace-balanced `{…}` span (string-aware), or `None`.
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

#[cfg(test)]
mod tests {
    use super::salvage_json;

    #[test]
    fn recovers_bare_fenced_and_prose_wrapped() {
        // Bare JSON.
        assert_eq!(salvage_json(r#"{"a":1}"#).unwrap()["a"], 1);
        // Fenced with a language tag.
        assert_eq!(salvage_json("```json\n{\"a\":2}\n```").unwrap()["a"], 2);
        // Leading prose, then the object.
        assert_eq!(salvage_json("Here you go: {\"a\":3} — hope that helps").unwrap()["a"], 3);
        // A brace inside a string doesn't confuse the balancer.
        assert_eq!(salvage_json(r#"prefix {"a":"has } brace"} tail"#).unwrap()["a"], "has } brace");
    }

    #[test]
    fn none_when_no_object() {
        assert!(salvage_json("I could not find reliable data.").is_none());
        assert!(salvage_json("").is_none());
    }
}
