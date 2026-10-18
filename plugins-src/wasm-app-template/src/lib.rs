//! A runnable **dynamic app**, shaped after the compiled-in `hackernews` app:
//! fetch a listing through the metered host chokepoint, shape the rows, upsert
//! them into a dataset with change detection, and return a summary.
//!
//! Everything it can do is in `wit/pumper-app.wit`. There is no socket, no
//! filesystem and no clock here — a fetch that is not `host::fetch` does not
//! compile, let alone link.

wit_bindgen::generate!({
    path: "wit",
    world: "app",
});

use serde_json::{json, Value};

struct Component;

export!(Component);

/// The manifest `GET /apps` serves and the enqueue door enforces. Examples are
/// validated against `params_schema` when the module is loaded — an example
/// that fails its own schema keeps the app OUT of the registry, exactly as the
/// server's manifest test does for compiled-in apps.
const MANIFEST: &str = r#"{
  "description": "Template dynamic app: fetches a listing and upserts its rows",
  "schedule": null,
  "cost_class": "free",
  "output_shape": "{dataset, fetched, new, changed, unchanged}",
  "default_params": { "url": "https://news.ycombinator.com/", "dataset": "items" },
  "params_schema": {
    "type": "object",
    "properties": {
      "url": { "type": "string", "description": "listing URL to fetch" },
      "dataset": { "type": "string", "description": "dataset to upsert into" },
      "limit": { "type": "integer", "minimum": 1, "maximum": 200 }
    },
    "required": ["url"]
  },
  "examples": [
    {
      "description": "scrape the front page into the `items` dataset",
      "params": { "url": "https://news.ycombinator.com/", "dataset": "items", "limit": 30 }
    }
  ]
}"#;

impl Guest for Component {
    fn describe() -> String {
        MANIFEST.to_string()
    }

    fn run(params_json: String) -> Result<String, String> {
        let params: Value =
            serde_json::from_str(&params_json).map_err(|e| format!("bad params: {e}"))?;
        let url = params
            .get("url")
            .and_then(Value::as_str)
            .ok_or("missing required string param 'url'")?;
        let dataset = params
            .get("dataset")
            .and_then(Value::as_str)
            .unwrap_or("items");
        let limit = params
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(30)
            .min(200) as usize;

        // A resumable app reads its checkpoint first and tolerates any shape —
        // `None` and "something I do not recognise" are the same answer: start
        // fresh.
        let done: Vec<String> = pumper::app::host::restore()
            .and_then(|state| serde_json::from_str::<Value>(&state).ok())
            .and_then(|state| {
                state
                    .get("done")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            })
            .unwrap_or_default();
        pumper::app::host::log("info", &format!("starting at {url} ({} done)", done.len()));

        // The ONE way out to the network: tiered, governed, budgeted, recorded.
        let outcome_json = pumper::app::host::fetch(&json!({ "url": url }).to_string())
            .map_err(|e| format!("fetch {url} failed: {e}"))?;
        let outcome: Value =
            serde_json::from_str(&outcome_json).map_err(|e| format!("bad fetch outcome: {e}"))?;
        let html = outcome
            .get("html")
            .and_then(Value::as_str)
            .unwrap_or_default();

        // Replace this with the real extraction for your source. Kept
        // deliberately trivial: the template is about the ABI, and an example
        // parser would be the first thing a real app deletes.
        let items: Vec<Value> = extract_links(html)
            .into_iter()
            .take(limit)
            .map(|(href, title)| json!({ "url": href, "title": title }))
            .collect();

        pumper::app::host::progress(&json!({ "fetched": items.len() }).to_string());

        let batch: Vec<Value> = items
            .iter()
            .map(|item| json!({ "key": item["url"], "value": item }))
            .collect();
        let summary_json =
            pumper::app::host::upsert_many(dataset, &Value::Array(batch).to_string())
                .map_err(|e| format!("upsert into {dataset} failed: {e}"))?;
        let summary: Value = serde_json::from_str(&summary_json).unwrap_or(Value::Null);

        // Checkpointing is advisory: a refused write is never fatal.
        let _ = pumper::app::host::checkpoint(
            &json!({ "done": items.iter().map(|i| &i["url"]).collect::<Vec<_>>() }).to_string(),
        );

        Ok(json!({
            "dataset": dataset,
            "fetched": items.len(),
            "new": summary.get("new").and_then(Value::as_array).map(Vec::len).unwrap_or(0),
            "changed": summary.get("changed").and_then(Value::as_array).map(Vec::len).unwrap_or(0),
            "unchanged": summary.get("unchanged").cloned().unwrap_or(json!(0)),
            "index_datasets": [dataset],
        })
        .to_string())
    }
}

/// Dependency-free `<a href="…">text</a>` scrape — enough to make the template
/// produce real records, small enough that nobody mistakes it for an extractor.
fn extract_links(html: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for chunk in html.split("<a ").skip(1) {
        let Some(rest) = chunk.split_once("href=\"") else {
            continue;
        };
        let Some((href, after)) = rest.1.split_once('"') else {
            continue;
        };
        if !href.starts_with("http") {
            continue;
        }
        let title = after
            .split_once('>')
            .map(|(_, text)| text.split('<').next().unwrap_or("").trim().to_string())
            .unwrap_or_default();
        if title.is_empty() {
            continue;
        }
        out.push((href.to_string(), title));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{extract_links, MANIFEST};

    /// The manifest is a contract the HOST validates at load: if it is not an
    /// object, or an example fails the app's own schema, the module is listed
    /// and refused rather than registered. Catch that here, where the failure
    /// costs a test rather than a deployment.
    #[test]
    fn the_manifest_is_valid_json_with_a_schema_and_examples() {
        let manifest: serde_json::Value = serde_json::from_str(MANIFEST).expect("valid JSON");
        assert!(manifest["params_schema"]["properties"]["url"].is_object());
        assert!(manifest["examples"][0]["params"]["url"].is_string());
        assert_eq!(manifest["cost_class"], "free");
    }

    #[test]
    fn links_without_a_title_or_an_absolute_href_are_skipped() {
        let html = r#"<a href="https://a.test/1">One</a><a href="/rel">Two</a><a href="https://b.test/2"></a>"#;
        let links = extract_links(html);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].0, "https://a.test/1");
        assert_eq!(links[0].1, "One");
    }
}
