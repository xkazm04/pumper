//! DataHub metadata emitter. Pushes *metadata only* — dataset entities, schema
//! inferred from stored records, table-level lineage, and per-run operation
//! (freshness) events — to a DataHub GMS over its plain OpenAPI ingestion
//! surface (`POST /openapi/entities/v1/`). No Python SDK, no Kafka: just JSON
//! over the shared reqwest client. Record data never leaves the local store.
//!
//! Fail-open like webhooks/triggers: emission runs in a detached task after
//! the job outcome is persisted, and any failure is a warn log plus a status
//! entry on `GET /datahub/status` — never a job failure.

use std::sync::Arc;

use pumper_core::Job;
use serde_json::{json, Map, Value};
use tracing::{info, warn};

use crate::state::AppState;

/// Entities per ingestion POST — small batches so one oversized payload can't
/// take down the whole emission.
const BATCH: usize = 25;
/// Cap on the raw sample JSON embedded in `schemaMetadata.platformSchema`.
const RAW_SCHEMA_CAP: usize = 4096;

pub const PLATFORM_URN: &str = "urn:li:dataPlatform:pumper";
const ACTOR_URN: &str = "urn:li:corpuser:pumper";

/// `urn:li:dataset:(urn:li:dataPlatform:pumper,<app>.<dataset>,<env>)`
pub fn dataset_urn(env: &str, app: &str, dataset: &str) -> String {
    format!("urn:li:dataset:({PLATFORM_URN},{app}.{dataset},{env})")
}

/// Wraps one aspect into the v1 ingestion envelope.
fn envelope(urn: &str, aspect: Value) -> Value {
    json!({ "entityType": "dataset", "entityUrn": urn, "aspect": aspect })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// `datasetProperties`: display name, description, and Pumper's run metadata as
/// custom properties (string map per the aspect model).
fn dataset_properties(app: &str, dataset: &str, custom: &[(&str, String)]) -> Value {
    let props: Map<String, Value> = custom
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.clone())))
        .collect();
    json!({
        "__type": "DatasetProperties",
        "name": format!("{app}/{dataset}"),
        "description": format!(
            "Pumper dataset `{dataset}` maintained by app `{app}` (change-detected upserts; \
             per-record revision history and field diffs live in the Pumper API)."
        ),
        "customProperties": Value::Object(props),
    })
}

/// DataHub logical type + native label for a sample JSON value.
fn field_type(v: &Value) -> (&'static str, &'static str) {
    match v {
        Value::String(_) => ("StringType", "string"),
        Value::Number(_) => ("NumberType", "number"),
        Value::Bool(_) => ("BooleanType", "boolean"),
        Value::Array(_) => ("ArrayType", "array"),
        Value::Object(_) => ("RecordType", "object"),
        Value::Null => ("NullType", "null"),
    }
}

/// `schemaMetadata` inferred from one sample record: top-level fields typed by
/// their JSON value, the (truncated) sample embedded as an `OtherSchema`.
fn schema_metadata(app: &str, dataset: &str, sample: &Value) -> Value {
    let fields: Vec<Value> = sample
        .as_object()
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| {
                    let (logical, native) = field_type(v);
                    json!({
                        "fieldPath": k,
                        "nativeDataType": native,
                        "type": { "type": { "__type": logical } },
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut raw = sample.to_string();
    raw.truncate(RAW_SCHEMA_CAP);
    json!({
        "__type": "SchemaMetadata",
        "schemaName": format!("{app}.{dataset}"),
        "platform": PLATFORM_URN,
        "version": 0,
        "hash": "",
        "platformSchema": { "__type": "OtherSchema", "rawSchema": raw },
        "fields": fields,
    })
}

/// `operation` (timeseries): a run refreshed this dataset — feeds DataHub
/// freshness assertions and "last updated" in the UI.
fn operation(ms: i64) -> Value {
    json!({
        "__type": "Operation",
        "timestampMillis": ms,
        "lastUpdatedTimestamp": ms,
        "operationType": "UPDATE",
    })
}

/// `datasetProfile` (timeseries): row count at emission time.
fn dataset_profile(ms: i64, rows: i64) -> Value {
    json!({
        "__type": "DatasetProfile",
        "timestampMillis": ms,
        "rowCount": rows,
    })
}

/// `upstreamLineage`: table-level TRANSFORMED edges from each upstream URN.
fn upstream_lineage(upstreams: &[String], ms: i64) -> Value {
    let ups: Vec<Value> = upstreams
        .iter()
        .map(|urn| {
            json!({
                "auditStamp": { "time": ms, "actor": ACTOR_URN },
                "dataset": urn,
                "type": "TRANSFORMED",
            })
        })
        .collect();
    json!({ "__type": "UpstreamLineage", "upstreams": ups })
}

/// POSTs entity batches to `{gms}/openapi/entities/v1/`. Returns the entity
/// count on success, the first error otherwise.
/// Own client, not the 15s webhook one: GMS ingestion can take >15s on a
/// cold instance (first write observed at ~18s on the quickstart stack).
fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("datahub client")
    })
}

async fn post_entities(state: &AppState, entities: Vec<Value>) -> Result<usize, String> {
    let client = client();
    let cfg = &state.config.datahub;
    let url = format!("{}/openapi/entities/v1/", cfg.gms_url.trim_end_matches('/'));
    let token = cfg.resolve_token();
    let total = entities.len();
    for chunk in entities.chunks(BATCH) {
        let mut req = client.post(&url).json(&chunk);
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.map_err(|e| {
            let cause = std::error::Error::source(&e)
                .map(|s| format!(" ({s})"))
                .unwrap_or_default();
            format!("POST {url}: {e}{cause}")
        })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(500).collect::<String>();
            return Err(format!("POST {url}: {status}: {body}"));
        }
    }
    Ok(total)
}

/// Records the outcome of the most recent emission for `GET /datahub/status`.
fn record_status(state: &AppState, kind: &str, outcome: Result<usize, String>) -> Value {
    let entry = match &outcome {
        Ok(n) => {
            json!({ "kind": kind, "at": pumper_core::datasets::ts(chrono::Utc::now()), "ok": true, "entities": n })
        }
        Err(e) => {
            json!({ "kind": kind, "at": pumper_core::datasets::ts(chrono::Utc::now()), "ok": false, "error": e })
        }
    };
    *state.datahub_last.lock().unwrap() = Some(entry.clone());
    entry
}

/// Aspects for one dataset: properties (+run counts), operation, and — when
/// enabled — profile and inferred schema. Reads are fail-open (a failed count
/// or sample read just omits that aspect).
async fn dataset_entities(
    state: &AppState,
    app: &str,
    dataset: &str,
    run: Option<(&Job, usize, usize, usize)>,
) -> Vec<Value> {
    let cfg = &state.config.datahub;
    let urn = dataset_urn(&cfg.env, app, dataset);
    let ms = now_ms();
    let rows = state.datasets.record_count(app, dataset).await.ok();

    let mut custom: Vec<(&str, String)> = vec![("pumper_app", app.to_string())];
    if let Some(rows) = rows {
        custom.push(("record_count", rows.to_string()));
    }
    if let Some((job, new, changed, removed)) = run {
        custom.push(("last_job_id", job.id.to_string()));
        custom.push(("last_run_new", new.to_string()));
        custom.push(("last_run_changed", changed.to_string()));
        custom.push(("last_run_removed", removed.to_string()));
    }

    let mut out = vec![
        envelope(&urn, dataset_properties(app, dataset, &custom)),
        envelope(&urn, operation(ms)),
    ];
    if cfg.emit_profile {
        if let Some(rows) = rows {
            out.push(envelope(&urn, dataset_profile(ms, rows)));
        }
    }
    if cfg.emit_schema {
        match state.datasets.list(app, dataset, 1).await {
            Ok(recs) => {
                if let Some(rec) = recs.first() {
                    out.push(envelope(&urn, schema_metadata(app, dataset, &rec.data)));
                }
            }
            Err(e) => warn!("datahub: sample read {app}/{dataset} failed: {e}"),
        }
    }
    out
}

/// Datasets a result names in `index_datasets` (`[{app, dataset}]`) — the
/// cross-namespace outputs (e.g. `grants/unified`) this run also wrote.
pub fn index_dataset_specs(result: &Value) -> Vec<(String, String)> {
    result
        .get("index_datasets")
        .and_then(Value::as_array)
        .map(|specs| {
            specs
                .iter()
                .filter_map(|s| {
                    Some((
                        s.get("app")?.as_str()?.to_string(),
                        s.get("dataset")?.as_str()?.to_string(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Per-dataset new/changed/removed counts from this run's revisions in the
/// given namespace since the job started.
async fn run_counts(
    state: &AppState,
    app: &str,
    dataset: Option<&str>,
    job: &Job,
) -> Vec<(String, usize, usize, usize)> {
    // Unfiltered: this counts everything the run produced for the emission
    // summary, not just what a consumer would be shown by trust.
    let revs = match state
        .datasets
        .changes_since(app, dataset, job.started_at, 100_000, None)
        .await
    {
        Ok(revs) => revs,
        Err(e) => {
            warn!("datahub: changes for {app} failed: {e}");
            return Vec::new();
        }
    };
    let mut by: std::collections::HashMap<String, (usize, usize, usize)> = Default::default();
    for rev in revs {
        let entry = by.entry(rev.dataset).or_default();
        match rev.change.as_str() {
            "new" => entry.0 += 1,
            "changed" => entry.1 += 1,
            "removed" => entry.2 += 1,
            _ => {}
        }
    }
    by.into_iter()
        .map(|(ds, (n, c, r))| (ds, n, c, r))
        .collect()
}

/// Existing upstream URNs of a dataset, read back from GMS so a multi-source
/// derived dataset (e.g. `grants/unified`, fed by three apps) accumulates edges
/// instead of each writer overwriting the others (aspect upserts replace
/// wholesale). Fail-open: unreadable → empty (this writer's edges still land).
async fn existing_upstreams(state: &AppState, urn: &str) -> Vec<String> {
    let cfg = &state.config.datahub;
    let encoded: String = urn
        .chars()
        .map(|c| match c {
            ':' => "%3A".to_string(),
            ',' => "%2C".to_string(),
            '(' => "%28".to_string(),
            ')' => "%29".to_string(),
            c => c.to_string(),
        })
        .collect();
    let url = format!(
        "{}/openapi/v3/entity/dataset/{encoded}?aspects=upstreamLineage",
        cfg.gms_url.trim_end_matches('/')
    );
    let mut req = client().get(&url);
    if let Some(t) = cfg.resolve_token() {
        req = req.bearer_auth(t);
    }
    let Ok(resp) = req.send().await else {
        return Vec::new();
    };
    let Ok(body) = resp.json::<Value>().await else {
        return Vec::new();
    };
    body["upstreamLineage"]["value"]["upstreams"]
        .as_array()
        .map(|ups| {
            ups.iter()
                .filter_map(|u| u["dataset"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Fire-and-forget emission for a succeeded job: every dataset in the job's
/// namespace (a successful run refreshes them whether or not rows changed — the
/// freshness signal must not go stale on quiet runs), plus the cross-namespace
/// `index_datasets` outputs with lineage edges (own datasets → derived dataset)
/// merged into the edges other writers already registered. One-line hook in the
/// worker; everything (including the revision reads) happens off the hot path.
pub fn on_job_success(state: AppState, job: Job, index_specs: Vec<(String, String)>) {
    if !state.config.datahub.enabled {
        return;
    }
    tokio::spawn(async move {
        let env = state.config.datahub.env.clone();
        let mut entities = Vec::new();

        // All datasets under the job's namespace, with this run's revision
        // counts where the run changed anything (zeroes on a quiet run).
        let own = match state.datasets.datasets(&job.app).await {
            Ok(own) => own,
            Err(e) => {
                warn!("datahub: datasets for {} failed: {e}", job.app);
                Vec::new()
            }
        };
        let counts = run_counts(&state, &job.app, None, &job).await;
        for ds in &own {
            let (n, c, r) = counts
                .iter()
                .find(|(d, ..)| d == ds)
                .map(|(_, n, c, r)| (*n, *c, *r))
                .unwrap_or_default();
            entities.extend(dataset_entities(&state, &job.app, ds, Some((&job, n, c, r))).await);
        }
        let own_urns: Vec<String> = own
            .iter()
            .map(|ds| dataset_urn(&env, &job.app, ds))
            .collect();

        // Cross-namespace outputs (e.g. grants-gov → grants/unified).
        for (app, ds) in &index_specs {
            if *app == job.app {
                continue; // already covered above
            }
            let counts = run_counts(&state, app, Some(ds), &job).await;
            let (n, c, r) = counts
                .first()
                .map(|(_, n, c, r)| (*n, *c, *r))
                .unwrap_or_default();
            entities.extend(dataset_entities(&state, app, ds, Some((&job, n, c, r))).await);
            if !own_urns.is_empty() {
                let urn = dataset_urn(&env, app, ds);
                let mut merged = existing_upstreams(&state, &urn).await;
                for u in &own_urns {
                    if !merged.contains(u) {
                        merged.push(u.clone());
                    }
                }
                entities.push(envelope(&urn, upstream_lineage(&merged, now_ms())));
            }
        }

        if entities.is_empty() {
            return;
        }
        let count = entities.len();
        match post_entities(&state, entities).await {
            Ok(n) => {
                info!(job = %job.id, entities = n, "datahub: job metadata emitted");
                record_status(&state, "job", Ok(n));
            }
            Err(e) => {
                warn!(job = %job.id, "datahub: emission failed: {e}");
                record_status(&state, "job", Err(format!("({count} entities) {e}")));
            }
        }
    });
}

/// One-shot backfill: walk every stored dataset and push entity + properties
/// (+ profile/schema per config). The button to press right after connecting a
/// fresh DataHub instance. Returns a summary; also recorded on `/datahub/status`.
pub async fn full_sync(state: &AppState) -> Value {
    let all = match state.datasets.list_all_datasets().await {
        Ok(all) => all,
        Err(e) => return record_status(state, "sync", Err(format!("list datasets: {e}"))),
    };
    let mut entities = Vec::new();
    for (app, ds) in &all {
        entities.extend(dataset_entities(state, app, ds, None).await);
    }
    let count = entities.len();
    let outcome = post_entities(state, entities).await;
    let mut summary = record_status(
        state,
        "sync",
        outcome.map_err(|e| format!("({count} entities) {e}")),
    );
    if let Some(obj) = summary.as_object_mut() {
        obj.insert("datasets".into(), json!(all.len()));
    }
    summary
}

/// Config + last-emission view for `GET /datahub/status`.
pub fn status(state: &AppState) -> Value {
    let cfg = &state.config.datahub;
    json!({
        "enabled": cfg.enabled,
        "gms_url": cfg.gms_url,
        "env": cfg.env,
        "token_set": cfg.resolve_token().is_some(),
        "emit_schema": cfg.emit_schema,
        "emit_profile": cfg.emit_profile,
        "last_emission": *state.datahub_last.lock().unwrap(),
    })
}

pub type StatusCell = Arc<std::sync::Mutex<Option<Value>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urn_shape() {
        assert_eq!(
            dataset_urn("PROD", "grants", "unified"),
            "urn:li:dataset:(urn:li:dataPlatform:pumper,grants.unified,PROD)"
        );
    }

    #[test]
    fn schema_inference_types_fields() {
        let sample = json!({
            "title": "x", "amount": 3.5, "open": true,
            "tags": ["a"], "meta": {"k": 1}, "gone": null
        });
        let schema = schema_metadata("grants", "unified", &sample);
        assert_eq!(schema["platform"], PLATFORM_URN);
        let fields = schema["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 6);
        let ty = |name: &str| {
            fields
                .iter()
                .find(|f| f["fieldPath"] == name)
                .map(|f| f["type"]["type"]["__type"].as_str().unwrap().to_string())
                .unwrap()
        };
        assert_eq!(ty("title"), "StringType");
        assert_eq!(ty("amount"), "NumberType");
        assert_eq!(ty("open"), "BooleanType");
        assert_eq!(ty("tags"), "ArrayType");
        assert_eq!(ty("meta"), "RecordType");
        assert_eq!(ty("gone"), "NullType");
    }

    #[test]
    fn lineage_edges_carry_audit_and_type() {
        let up = vec![dataset_urn("PROD", "grants-gov", "opportunities")];
        let aspect = upstream_lineage(&up, 42);
        let edge = &aspect["upstreams"][0];
        assert_eq!(edge["dataset"], up[0]);
        assert_eq!(edge["type"], "TRANSFORMED");
        assert_eq!(edge["auditStamp"]["actor"], ACTOR_URN);
    }

    #[test]
    fn envelope_is_v1_ingestion_shape() {
        let urn = dataset_urn("PROD", "a", "b");
        let e = envelope(&urn, operation(7));
        assert_eq!(e["entityType"], "dataset");
        assert_eq!(e["entityUrn"], urn);
        assert_eq!(e["aspect"]["__type"], "Operation");
        assert_eq!(e["aspect"]["timestampMillis"], 7);
    }

    #[test]
    fn index_specs_parsed_and_malformed_skipped() {
        let result = json!({
            "index_datasets": [
                {"app": "grants", "dataset": "unified"},
                {"app": "grants"},
                "nonsense"
            ]
        });
        assert_eq!(
            index_dataset_specs(&result),
            vec![("grants".into(), "unified".into())]
        );
        assert!(index_dataset_specs(&json!({})).is_empty());
    }

    #[test]
    fn properties_customs_are_strings() {
        let p = dataset_properties("grants", "unified", &[("record_count", "12".into())]);
        assert_eq!(p["customProperties"]["record_count"], "12");
        assert_eq!(p["name"], "grants/unified");
    }
}
