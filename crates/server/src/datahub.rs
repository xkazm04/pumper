//! DataHub metadata emitter. Pushes *metadata only* — dataset entities, schema
//! inferred from stored records, table-level lineage, and per-run operation
//! (freshness) events — to a DataHub GMS over its plain OpenAPI ingestion
//! surface (`POST /openapi/entities/v1/`). No Python SDK, no Kafka: just JSON
//! over the shared reqwest client. Record data never leaves the local store.
//!
//! Fail-open like webhooks/triggers: emission runs in a detached task after
//! the job outcome is persisted, and any failure is a warn log plus a status
//! entry on `GET /datahub/status` — never a job failure.

use std::collections::HashSet;
use std::sync::Arc;

use pumper_core::extract::{Rule, RuleSet};
use pumper_core::{EnqueueOptions, Job, CATALOG_MANAGED_BY};
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

/// `urn:li:dataFlow:(pumper,<flow_id>,<env>)` — a pipeline (schedule, trigger,
/// or the app's ad-hoc bucket) in DataHub's process model.
pub fn dataflow_urn(env: &str, flow_id: &str) -> String {
    format!("urn:li:dataFlow:(pumper,{flow_id},{env})")
}

/// `urn:li:dataJob:(<flow_urn>,<job_id>)` — one run under a flow.
pub fn datajob_urn(flow_urn: &str, job_id: &str) -> String {
    format!("urn:li:dataJob:({flow_urn},{job_id})")
}

/// `urn:li:schemaField:(<dataset_urn>,<field>)` — a column, for fine-grained lineage.
fn schema_field_urn(dataset_urn: &str, field: &str) -> String {
    format!("urn:li:schemaField:({dataset_urn},{field})")
}

/// Wraps one aspect into the v1 ingestion envelope for any entity type.
fn entity(entity_type: &str, urn: &str, aspect: Value) -> Value {
    json!({ "entityType": entity_type, "entityUrn": urn, "aspect": aspect })
}

/// Dataset-entity envelope (the original emitter's shape).
fn envelope(urn: &str, aspect: Value) -> Value {
    entity("dataset", urn, aspect)
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

// ── M25: pipeline topology (dataFlow / dataJob / column lineage) ─────────────

/// `dataFlowInfo`: the flow's display name plus Pumper metadata as custom
/// properties (string map per the aspect model).
fn dataflow_info(name: &str, custom: &[(&str, String)]) -> Value {
    let props: Map<String, Value> = custom
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.clone())))
        .collect();
    json!({
        "__type": "DataFlowInfo",
        "name": name,
        "customProperties": Value::Object(props),
    })
}

/// `dataJobInfo`: one run's display name; `type` is the aspect-model union,
/// which the plain OpenAPI surface takes as `{"string": ...}`.
fn datajob_info(name: &str, custom: &[(&str, String)]) -> Value {
    let props: Map<String, Value> = custom
        .iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.clone())))
        .collect();
    json!({
        "__type": "DataJobInfo",
        "name": name,
        "type": { "string": "COMMAND" },
        "customProperties": Value::Object(props),
    })
}

/// `dataJobInputOutput`: the dataset edges that make a run render as a node in
/// DataHub's lineage graph.
fn datajob_io(inputs: &[String], outputs: &[String]) -> Value {
    json!({
        "__type": "DataJobInputOutput",
        "inputDatasets": inputs,
        "outputDatasets": outputs,
    })
}

/// Which flow a run belongs to: its schedule, the trigger that fired it, or the
/// app's ad-hoc bucket. Returns `(flow_id, display_name, kind)`.
fn flow_identity(
    app: &str,
    schedule_id: Option<&str>,
    trigger_id: Option<&str>,
) -> (String, String, &'static str) {
    if let Some(s) = schedule_id {
        (
            format!("schedule.{app}.{s}"),
            format!("{app} (schedule {s})"),
            "schedule",
        )
    } else if let Some(t) = trigger_id {
        (
            format!("trigger.{app}.{t}"),
            format!("{app} (trigger {t})"),
            "trigger",
        )
    } else {
        (format!("adhoc.{app}"), format!("{app} (ad-hoc)"), "adhoc")
    }
}

/// A declarative `RuleSet` in job params (`params.rules`), when present and
/// well-formed. Anything else — absent, or a shape only the app understands —
/// is `None`: column lineage is emitted ONLY where rules make it mechanical.
fn job_rule_set(params: &Value) -> Option<RuleSet> {
    serde_json::from_value(params.get("rules")?.clone()).ok()
}

/// One rule's provenance descriptor for `transformOperation` — the mechanical
/// "where this column comes from" a declarative rule states outright.
fn rule_op(rule: &Rule) -> String {
    match rule {
        Rule::Css { selector, attr, .. } => match attr {
            Some(a) => format!("css:{selector}@{a}"),
            None => format!("css:{selector}"),
        },
        Rule::Regex { pattern, group } => format!("regex:{pattern}#{group}"),
        Rule::Json { pointer } => format!("json:{pointer}"),
        Rule::Xpath { xpath, .. } => format!("xpath:{xpath}"),
        Rule::Const { .. } => "const".to_string(),
        Rule::Each { selector, .. } => format!("each:{selector}"),
    }
}

/// Flattens a RuleSet into `(column, provenance)` pairs. `each` containers
/// contribute the container itself plus `parent.child` entries for their inner
/// fields (one level — the same nesting the extractor compiles).
pub(crate) fn rule_ops(rules: &RuleSet) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (name, fr) in &rules.fields {
        out.push((name.clone(), rule_op(&fr.rule)));
        if let Rule::Each { fields, .. } = &fr.rule {
            for (inner, ifr) in fields {
                out.push((format!("{name}.{inner}"), rule_op(&ifr.rule)));
            }
        }
    }
    out
}

/// `upstreamLineage` carrying column-level provenance: fine-grained entries
/// with `upstreamType: NONE` (the upstream is the fetched page, not a dataset —
/// claiming a dataset-level source here would be a lie) and the rule descriptor
/// as `transformOperation`. Existing table-level upstreams are preserved so
/// this write cannot clobber edges other writers registered.
fn upstream_lineage_with_fields(
    dataset_urn: &str,
    upstreams: &[String],
    ops: &[(String, String)],
    ms: i64,
) -> Value {
    let mut aspect = upstream_lineage(upstreams, ms);
    let fine: Vec<Value> = ops
        .iter()
        .map(|(field, op)| {
            json!({
                "upstreamType": "NONE",
                "upstreams": [],
                "downstreamType": "FIELD",
                "downstreams": [schema_field_urn(dataset_urn, field)],
                "confidenceScore": 1.0,
                "transformOperation": op,
            })
        })
        .collect();
    aspect["fineGrainedLineages"] = Value::Array(fine);
    aspect
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

/// The dataFlow + dataJob entities for one succeeded run. Inputs are the
/// firing trigger's source datasets (the one upstream edge Pumper knows
/// mechanically); outputs are everything the run wrote. Reads are fail-open:
/// an unreadable trigger just means no input edges.
async fn flow_entities(state: &AppState, job: &Job, output_urns: &[String]) -> Vec<Value> {
    let env = &state.config.datahub.env;
    let (flow_id, flow_name, kind) = flow_identity(
        &job.app,
        job.schedule_id.as_deref(),
        job.trigger_id.as_deref(),
    );
    let flow_urn = dataflow_urn(env, &flow_id);
    let mut custom: Vec<(&str, String)> = vec![("pumper_app", job.app.clone()), ("kind", kind.into())];
    if let Some(s) = &job.schedule_id {
        custom.push(("schedule_id", s.clone()));
    }
    if let Some(t) = &job.trigger_id {
        custom.push(("trigger_id", t.clone()));
    }

    // Input edges: the datasets the firing trigger listens on.
    let mut inputs: Vec<String> = Vec::new();
    if let Some(tid) = &job.trigger_id {
        match state.storage.get_trigger(tid).await {
            Ok(Some(t)) => {
                let source_datasets = match (t.source_kind.as_str(), t.source_dataset.as_deref()) {
                    ("dataset", Some(ds)) if ds != "*" => vec![ds.to_string()],
                    _ => state
                        .datasets
                        .datasets(&t.source_app)
                        .await
                        .unwrap_or_default(),
                };
                for ds in source_datasets {
                    inputs.push(dataset_urn(env, &t.source_app, &ds));
                }
            }
            Ok(None) => {}
            Err(e) => warn!(trigger = %tid, "datahub: trigger read failed: {e}"),
        }
    }

    let job_id = job.id.to_string();
    let jurn = datajob_urn(&flow_urn, &job_id);
    let job_custom: Vec<(&str, String)> = vec![
        ("pumper_app", job.app.clone()),
        ("job_id", job_id.clone()),
        ("attempts", job.attempts.to_string()),
    ];
    vec![
        entity("dataFlow", &flow_urn, dataflow_info(&flow_name, &custom)),
        entity(
            "dataJob",
            &jurn,
            datajob_info(&format!("{} run {}", job.app, job_id), &job_custom),
        ),
        entity("dataJob", &jurn, datajob_io(&inputs, output_urns)),
    ]
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

        // M25: this run as a dataJob under its flow (schedule / trigger /
        // ad-hoc), with input/output dataset edges — the run renders as a node
        // in DataHub's lineage graph.
        if state.config.datahub.emit_flows {
            let mut output_urns = own_urns.clone();
            for (app, ds) in &index_specs {
                let urn = dataset_urn(&env, app, ds);
                if !output_urns.contains(&urn) {
                    output_urns.push(urn);
                }
            }
            entities.extend(flow_entities(&state, &job, &output_urns).await);

            // Column lineage — ONLY where a declarative RuleSet makes field
            // provenance mechanical. Apps whose extraction logic is code (not
            // rules) are honestly skipped: guessing would poison the graph.
            if let Some(rules) = job_rule_set(&job.params) {
                let ops = rule_ops(&rules);
                if !ops.is_empty() {
                    for (ds, ..) in counts.iter().filter(|(_, n, c, r)| n + c + r > 0) {
                        let urn = dataset_urn(&env, &job.app, ds);
                        let merged = existing_upstreams(&state, &urn).await;
                        entities.push(envelope(
                            &urn,
                            upstream_lineage_with_fields(&urn, &merged, &ops, now_ms()),
                        ));
                    }
                }
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

    // M25: the pipeline topology. Every schedule is a dataFlow (cron and
    // ownership in custom properties), and every enabled trigger becomes
    // dataset-level lineage edges source → target, so the reactive DAG renders
    // as an actual graph in DataHub. Both reads are fail-open.
    let mut schedules_emitted = 0usize;
    let mut trigger_edges = 0usize;
    if state.config.datahub.emit_flows {
        let env = &state.config.datahub.env;
        match state.storage.list_schedules().await {
            Ok(schedules) => {
                for s in &schedules {
                    let flow_id = format!("schedule.{}.{}", s.app, s.id);
                    let mut custom: Vec<(&str, String)> = vec![
                        ("pumper_app", s.app.clone()),
                        ("kind", "schedule".into()),
                        ("schedule_id", s.id.clone()),
                        ("cron", s.cron.clone()),
                        ("enabled", s.enabled.to_string()),
                    ];
                    if let Some(tz) = &s.timezone {
                        custom.push(("timezone", tz.clone()));
                    }
                    if let Some(m) = &s.managed_by {
                        custom.push(("managed_by", m.clone()));
                    }
                    entities.push(entity(
                        "dataFlow",
                        &dataflow_urn(env, &flow_id),
                        dataflow_info(&format!("{} @ {}", s.app, s.cron), &custom),
                    ));
                    schedules_emitted += 1;
                }
            }
            Err(e) => warn!("datahub: schedules read failed: {e}"),
        }
        match state.storage.list_triggers(None).await {
            Ok(triggers) => {
                for t in triggers.iter().filter(|t| t.enabled) {
                    let sources = match (t.source_kind.as_str(), t.source_dataset.as_deref()) {
                        ("dataset", Some(ds)) if ds != "*" => vec![ds.to_string()],
                        _ => state
                            .datasets
                            .datasets(&t.source_app)
                            .await
                            .unwrap_or_default(),
                    };
                    let source_urns: Vec<String> = sources
                        .iter()
                        .map(|ds| dataset_urn(env, &t.source_app, ds))
                        .collect();
                    if source_urns.is_empty() {
                        continue;
                    }
                    let targets = state
                        .datasets
                        .datasets(&t.target_app)
                        .await
                        .unwrap_or_default();
                    for ds in &targets {
                        let urn = dataset_urn(env, &t.target_app, ds);
                        let mut merged = existing_upstreams(state, &urn).await;
                        for u in &source_urns {
                            if !merged.contains(u) {
                                merged.push(u.clone());
                            }
                        }
                        entities.push(envelope(&urn, upstream_lineage(&merged, now_ms())));
                        trigger_edges += source_urns.len();
                    }
                }
            }
            Err(e) => warn!("datahub: triggers read failed: {e}"),
        }
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
        obj.insert("flows".into(), json!(schedules_emitted));
        obj.insert("trigger_edges".into(), json!(trigger_edges));
    }
    summary
}

// ── M26: governance pull loop (DataHub state drives Pumper) ──────────────────
//
// Opt-in (`[datahub] govern = true`, default OFF) and scheduler-piggybacked
// like the DLQ drain. Each poll reads deprecation / tags / assertion health for
// every Pumper dataset URN over the GMS GraphQL surface and maps them to three
// actions, each loud-logged and surfaced on `GET /datahub/status`:
//
//   deprecation        → disable that app's **catalog-managed** schedules only
//                        (M19's `managed_by = "catalog"` fence — hand-made
//                        schedules are sacred and never touched)
//   `cost:pause` tag   → force the app's job budget to $0, which the existing
//                        budget governor turns into "free tiers only" — the
//                        Claude tier is skipped, nothing is cancelled, and
//                        removing the tag restores normal budgets (reversible)
//   failing assertions → enqueue one immediate sync job for the dataset's app
//                        (hour-bucketed idempotency key, so a persistent
//                        failure can't enqueue a storm)
//
// Unreachable/absent DataHub = clean no-op: the FIRST read error aborts the
// whole poll before any action is planned, matching the emitter's posture.

/// Governance state shared with the worker (pause enforcement) and the status
/// route. In-memory only: a restart re-derives everything from DataHub on the
/// next poll, so a dead DataHub after a restart means "no pauses" — fail-open.
#[derive(Debug, Default)]
pub struct GovernState {
    last_poll: Option<std::time::Instant>,
    paused_apps: HashSet<String>,
    last: Option<Value>,
}

pub type GovernCell = Arc<std::sync::Mutex<GovernState>>;

/// The budget a job actually runs with: `cost:pause` (from the last governance
/// poll) forces `$0`, which [`pumper_core::AppContext`]'s budget governor turns
/// into free-tiers-only. One-line hook in the worker's `AppContext` build.
pub fn effective_budget(state: &AppState, app: &str, requested: Option<f64>) -> Option<f64> {
    if state.datahub_govern.lock().unwrap().paused_apps.contains(app) {
        warn!(
            app,
            "datahub govern: cost:pause tag active — Claude-tier budget forced to $0 for this job"
        );
        Some(0.0)
    } else {
        requested
    }
}

/// Scheduler-tick entry point: interval-gated, spawned (non-blocking), gated on
/// both `enabled` and `govern`.
pub fn govern_tick(state: &AppState) {
    let cfg = &state.config.datahub;
    if !cfg.enabled || !cfg.govern {
        return;
    }
    let interval = std::time::Duration::from_secs(cfg.govern_interval_secs.max(30));
    {
        let mut g = state.datahub_govern.lock().unwrap();
        if matches!(g.last_poll, Some(t) if t.elapsed() < interval) {
            return;
        }
        g.last_poll = Some(std::time::Instant::now());
    }
    let state = state.clone();
    tokio::spawn(async move { govern_poll(state).await });
}

/// What one poll observed for one dataset.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DatasetMeta {
    app: String,
    dataset: String,
    deprecated: bool,
    cost_pause: bool,
    assertions_failing: bool,
}

/// Parses one GraphQL `dataset` response into signals. Absent/null anything
/// (dataset not yet in DataHub, no tags, no health) = all-false: only explicit
/// remote state may cause an action.
pub(crate) fn govern_meta(app: &str, dataset: &str, body: &Value) -> DatasetMeta {
    let d = &body["data"]["dataset"];
    let deprecated = d["deprecation"]["deprecated"].as_bool().unwrap_or(false);
    let cost_pause = d["tags"]["tags"]
        .as_array()
        .map(|ts| {
            ts.iter().any(|t| {
                t["tag"]["urn"]
                    .as_str()
                    .is_some_and(|u| u.eq_ignore_ascii_case("urn:li:tag:cost:pause"))
            })
        })
        .unwrap_or(false);
    let assertions_failing = d["health"]
        .as_array()
        .map(|hs| {
            hs.iter()
                .any(|h| h["type"] == "ASSERTIONS" && h["status"] == "FAIL")
        })
        .unwrap_or(false);
    DatasetMeta {
        app: app.to_string(),
        dataset: dataset.to_string(),
        deprecated,
        cost_pause,
        assertions_failing,
    }
}

/// One planned governance action (pause is not an action — the paused set is
/// declarative and recomputed wholesale each poll, so tag removal resumes).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GovernAction {
    /// A dataset of this app is deprecated → disable catalog-managed schedules.
    DisableSchedules { app: String, dataset: String },
    /// A dataset of this app has failing assertions → enqueue immediate sync.
    EnqueueSync { app: String, dataset: String },
}

/// Pure mapping observations → (actions, paused apps). Deduped: one disable per
/// app (the first deprecated dataset names it), one sync per (app, dataset).
pub(crate) fn plan_govern_actions(metas: &[DatasetMeta]) -> (Vec<GovernAction>, HashSet<String>) {
    let mut actions = Vec::new();
    let mut disabled_apps: HashSet<&str> = HashSet::new();
    let mut paused: HashSet<String> = HashSet::new();
    for m in metas {
        if m.deprecated && disabled_apps.insert(&m.app) {
            actions.push(GovernAction::DisableSchedules {
                app: m.app.clone(),
                dataset: m.dataset.clone(),
            });
        }
        if m.cost_pause {
            paused.insert(m.app.clone());
        }
        if m.assertions_failing {
            actions.push(GovernAction::EnqueueSync {
                app: m.app.clone(),
                dataset: m.dataset.clone(),
            });
        }
    }
    (actions, paused)
}

/// One GraphQL read per dataset URN. Any transport / HTTP / GraphQL error is a
/// hard `Err` — the caller aborts the poll (no partial governance).
async fn fetch_govern_meta(state: &AppState, urn: &str) -> Result<Value, String> {
    let cfg = &state.config.datahub;
    let url = format!("{}/api/graphql", cfg.gms_url.trim_end_matches('/'));
    let query = "query($urn: String!) { dataset(urn: $urn) { \
                 deprecation { deprecated } \
                 tags { tags { tag { urn } } } \
                 health { type status } } }";
    let mut req = client()
        .post(&url)
        .json(&json!({ "query": query, "variables": { "urn": urn } }));
    if let Some(t) = cfg.resolve_token() {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await.map_err(|e| format!("POST {url}: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("POST {url}: {status}"));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| format!("POST {url}: bad json: {e}"))?;
    if body.get("errors").and_then(Value::as_array).is_some_and(|e| !e.is_empty()) {
        return Err(format!("graphql errors for {urn}"));
    }
    Ok(body)
}

/// Records the poll summary for `GET /datahub/status`.
fn record_govern(state: &AppState, summary: Value) {
    state.datahub_govern.lock().unwrap().last = Some(summary);
}

/// One governance poll: read remote state for every dataset URN, plan, apply.
async fn govern_poll(state: AppState) {
    let all = match state.datasets.list_all_datasets().await {
        Ok(all) => all,
        Err(e) => {
            warn!("datahub govern: dataset list failed, poll skipped: {e}");
            return;
        }
    };
    let env = state.config.datahub.env.clone();
    let mut metas = Vec::with_capacity(all.len());
    for (app, ds) in &all {
        let urn = dataset_urn(&env, app, ds);
        match fetch_govern_meta(&state, &urn).await {
            Ok(body) => metas.push(govern_meta(app, ds, &body)),
            Err(e) => {
                // Unreachable DataHub = clean no-op: abort before ANY action.
                warn!("datahub govern: poll aborted, no actions taken: {e}");
                record_govern(
                    &state,
                    json!({
                        "at": pumper_core::datasets::ts(chrono::Utc::now()),
                        "ok": false,
                        "error": e,
                    }),
                );
                return;
            }
        }
    }

    let (actions, paused) = plan_govern_actions(&metas);
    let mut log: Vec<String> = Vec::new();
    let mut disabled = 0usize;
    let mut syncs = 0usize;

    // Pause set: recomputed wholesale, so removing the tag resumes the app.
    {
        let mut g = state.datahub_govern.lock().unwrap();
        for app in paused.difference(&g.paused_apps) {
            warn!(app = %app, "datahub govern: cost:pause tag — Claude-tier PAUSED (budget $0) for new jobs");
            log.push(format!("paused {app} (cost:pause tag)"));
        }
        for app in g.paused_apps.difference(&paused) {
            info!(app = %app, "datahub govern: cost:pause tag removed — Claude-tier resumed");
            log.push(format!("resumed {app} (cost:pause tag removed)"));
        }
        g.paused_apps = paused.clone();
    }

    let schedules = if actions
        .iter()
        .any(|a| matches!(a, GovernAction::DisableSchedules { .. }))
    {
        state.storage.list_schedules().await.unwrap_or_else(|e| {
            warn!("datahub govern: schedules read failed: {e}");
            Vec::new()
        })
    } else {
        Vec::new()
    };

    for action in &actions {
        match action {
            GovernAction::DisableSchedules { app, dataset } => {
                // M19 fence: only rows tagged managed_by = "catalog" — the SQL
                // itself refuses anything else, hand-made schedules are sacred.
                for s in schedules.iter().filter(|s| {
                    s.app == *app && s.enabled && s.managed_by.as_deref() == Some(CATALOG_MANAGED_BY)
                }) {
                    match state
                        .storage
                        .set_managed_schedule_enabled(&s.id, false, CATALOG_MANAGED_BY)
                        .await
                    {
                        Ok(true) => {
                            warn!(id = %s.id, app = %app, dataset = %dataset,
                                  "datahub govern: dataset deprecated in DataHub — catalog-managed schedule DISABLED");
                            log.push(format!(
                                "disabled schedule {} ({app}, deprecation on {app}/{dataset})",
                                s.id
                            ));
                            disabled += 1;
                        }
                        Ok(false) => warn!(id = %s.id, "datahub govern: disable fenced off (not catalog-managed)"),
                        Err(e) => warn!(id = %s.id, "datahub govern: disable failed: {e}"),
                    }
                }
            }
            GovernAction::EnqueueSync { app, dataset } => {
                if !state.registry.contains_key(app.as_str()) {
                    warn!(app = %app, "datahub govern: failing assertion but app not registered; skipping sync");
                    continue;
                }
                let params = state
                    .registry
                    .get(app.as_str())
                    .map(|a| a.default_params())
                    .unwrap_or(Value::Null);
                // Hour-bucketed idempotency: a persistently failing assertion
                // enqueues at most one sync per hour, not one per poll.
                let key = format!(
                    "datahub-govern-sync:{app}:{dataset}:{}",
                    chrono::Utc::now().format("%Y-%m-%dT%H")
                );
                let opts = EnqueueOptions {
                    params,
                    max_attempts: 2,
                    idempotency_key: Some(key),
                    ..Default::default()
                };
                match state.storage.enqueue(app, opts).await {
                    Ok(job) => {
                        warn!(app = %app, dataset = %dataset, job = %job.id,
                              "datahub govern: failing assertion in DataHub — immediate sync enqueued");
                        log.push(format!(
                            "enqueued sync job {} ({app}, failing assertion on {app}/{dataset})",
                            job.id
                        ));
                        state.notify.notify_one();
                        syncs += 1;
                    }
                    Err(e) => warn!(app = %app, "datahub govern: sync enqueue failed: {e}"),
                }
            }
        }
    }

    let summary = json!({
        "at": pumper_core::datasets::ts(chrono::Utc::now()),
        "ok": true,
        "datasets_polled": all.len(),
        "schedules_disabled": disabled,
        "syncs_enqueued": syncs,
        "paused_apps": paused.iter().collect::<Vec<_>>(),
        "actions": log,
    });
    if disabled + syncs > 0 || !paused.is_empty() {
        info!(summary = %summary, "datahub govern: poll applied actions");
    }
    record_govern(&state, summary);
}

/// Config + last-emission view for `GET /datahub/status`.
pub fn status(state: &AppState) -> Value {
    let cfg = &state.config.datahub;
    let govern = {
        let g = state.datahub_govern.lock().unwrap();
        json!({
            "enabled": cfg.govern,
            "interval_secs": cfg.govern_interval_secs,
            "paused_apps": g.paused_apps.iter().collect::<Vec<_>>(),
            "last_poll": g.last,
        })
    };
    json!({
        "enabled": cfg.enabled,
        "gms_url": cfg.gms_url,
        "env": cfg.env,
        "token_set": cfg.resolve_token().is_some(),
        "emit_schema": cfg.emit_schema,
        "emit_profile": cfg.emit_profile,
        "emit_flows": cfg.emit_flows,
        "last_emission": *state.datahub_last.lock().unwrap(),
        "govern": govern,
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
    fn flow_and_job_urn_shapes() {
        let flow = dataflow_urn("PROD", "schedule.grants-gov.s1");
        assert_eq!(flow, "urn:li:dataFlow:(pumper,schedule.grants-gov.s1,PROD)");
        assert_eq!(
            datajob_urn(&flow, "j1"),
            "urn:li:dataJob:(urn:li:dataFlow:(pumper,schedule.grants-gov.s1,PROD),j1)"
        );
    }

    #[test]
    fn flow_identity_prefers_schedule_then_trigger_then_adhoc() {
        assert_eq!(flow_identity("a", Some("s"), Some("t")).0, "schedule.a.s");
        assert_eq!(flow_identity("a", None, Some("t")).0, "trigger.a.t");
        let (id, _, kind) = flow_identity("a", None, None);
        assert_eq!((id.as_str(), kind), ("adhoc.a", "adhoc"));
    }

    #[test]
    fn datajob_io_carries_dataset_edges() {
        let io = datajob_io(
            &["urn:in".to_string()],
            &["urn:out1".to_string(), "urn:out2".to_string()],
        );
        assert_eq!(io["__type"], "DataJobInputOutput");
        assert_eq!(io["inputDatasets"], json!(["urn:in"]));
        assert_eq!(io["outputDatasets"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn rule_ops_flatten_each_containers_and_describe_provenance() {
        let rules: RuleSet = serde_json::from_value(json!({
            "title": {"type": "css", "selector": "h1"},
            "price": {"type": "regex", "pattern": "\\d+", "group": 1},
            "items": {"type": "each", "selector": ".card", "fields": {
                "name": {"type": "css", "selector": ".n", "attr": "title"},
                "tag": {"type": "const", "value": "x"}
            }}
        }))
        .unwrap();
        let ops = rule_ops(&rules);
        let get = |k: &str| ops.iter().find(|(n, _)| n == k).map(|(_, o)| o.clone());
        assert_eq!(get("title").unwrap(), "css:h1");
        assert_eq!(get("price").unwrap(), "regex:\\d+#1");
        assert_eq!(get("items").unwrap(), "each:.card");
        assert_eq!(get("items.name").unwrap(), "css:.n@title");
        assert_eq!(get("items.tag").unwrap(), "const");
    }

    #[test]
    fn job_rule_set_only_parses_declarative_rules() {
        assert!(job_rule_set(&json!({"rules": {"t": {"type": "css", "selector": "h1"}}})).is_some());
        assert!(job_rule_set(&json!({})).is_none());
        assert!(job_rule_set(&json!({"rules": "not rules"})).is_none());
    }

    #[test]
    fn fine_grained_lineage_preserves_upstreams_and_names_columns() {
        let urn = dataset_urn("PROD", "shop", "products");
        let ups = vec!["urn:other".to_string()];
        let ops = vec![("price".to_string(), "css:.price".to_string())];
        let aspect = upstream_lineage_with_fields(&urn, &ups, &ops, 7);
        assert_eq!(aspect["upstreams"][0]["dataset"], "urn:other");
        let fg = &aspect["fineGrainedLineages"][0];
        assert_eq!(fg["upstreamType"], "NONE");
        assert_eq!(fg["transformOperation"], "css:.price");
        assert_eq!(fg["downstreams"][0], format!("urn:li:schemaField:({urn},price)"));
    }

    #[test]
    fn govern_meta_reads_deprecation_tags_and_health() {
        let body = json!({"data": {"dataset": {
            "deprecation": {"deprecated": true},
            "tags": {"tags": [{"tag": {"urn": "urn:li:tag:cost:pause"}}]},
            "health": [{"type": "ASSERTIONS", "status": "FAIL"}]
        }}});
        let m = govern_meta("grants-gov", "opportunities", &body);
        assert!(m.deprecated && m.cost_pause && m.assertions_failing);
        // Passing health / unrelated tags / absent dataset ⇒ all false.
        let quiet = govern_meta(
            "a",
            "b",
            &json!({"data": {"dataset": {
                "tags": {"tags": [{"tag": {"urn": "urn:li:tag:pii"}}]},
                "health": [{"type": "ASSERTIONS", "status": "PASS"}]
            }}}),
        );
        assert!(!quiet.deprecated && !quiet.cost_pause && !quiet.assertions_failing);
        let absent = govern_meta("a", "b", &json!({"data": {"dataset": null}}));
        assert!(!absent.deprecated && !absent.cost_pause && !absent.assertions_failing);
    }

    fn meta(app: &str, ds: &str, dep: bool, pause: bool, fail: bool) -> DatasetMeta {
        DatasetMeta {
            app: app.into(),
            dataset: ds.into(),
            deprecated: dep,
            cost_pause: pause,
            assertions_failing: fail,
        }
    }

    #[test]
    fn plan_dedupes_disables_per_app_and_computes_pause_set() {
        let metas = vec![
            meta("a", "d1", true, false, false),
            meta("a", "d2", true, false, true),
            meta("b", "d3", false, true, false),
        ];
        let (actions, paused) = plan_govern_actions(&metas);
        // One disable for app `a` (deduped), one sync for a/d2.
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], GovernAction::DisableSchedules { app, .. } if app == "a"));
        assert!(matches!(&actions[1], GovernAction::EnqueueSync { app, dataset } if app == "a" && dataset == "d2"));
        assert_eq!(paused, HashSet::from(["b".to_string()]));
    }

    #[test]
    fn plan_is_empty_when_datahub_is_quiet() {
        let metas = vec![meta("a", "d1", false, false, false)];
        let (actions, paused) = plan_govern_actions(&metas);
        assert!(actions.is_empty() && paused.is_empty());
    }

    #[test]
    fn properties_customs_are_strings() {
        let p = dataset_properties("grants", "unified", &[("record_count", "12".into())]);
        assert_eq!(p["customProperties"]["record_count"], "12");
        assert_eq!(p["name"], "grants/unified");
    }
}
