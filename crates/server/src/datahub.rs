//! DataHub metadata emitter. Pushes *metadata only* — dataset entities, schema
//! inferred from stored records, table-level lineage, and per-run operation
//! (freshness) events — to a DataHub GMS over its plain OpenAPI ingestion
//! surface (`POST /openapi/entities/v1/`). No Python SDK, no Kafka: just JSON
//! over the shared reqwest client. Record data never leaves the local store.
//!
//! Fail-open like webhooks/triggers: emission runs off the worker's scrape
//! permit after the job outcome is persisted, and any failure is a warn log
//! plus a status entry on `GET /datahub/status` — never a job failure.
//!
//! "Off the permit" is NOT "detached": job emission runs on the worker's
//! [`crate::fanout::FanoutPool`], the same tracked pool the rest of the
//! post-success fan-out uses, so a shutdown either drains it or *counts and
//! logs* what it abandoned. A bare `tokio::spawn` here would make a shutdown
//! during emission a silent metadata gap.
//!
//! There is deliberately **no retry**: a failed emission is recorded (see
//! [`EmissionStatus`], which keeps the last failure separately from the last
//! success so a flapping bridge is visible) and healed by the next run or a
//! manual `POST /datahub/sync`. Metadata is idempotent and re-derived every
//! run, so a queue/DLQ would buy staleness insurance nobody asked for.

use std::collections::HashSet;
use std::sync::Arc;

use futures::StreamExt;
use pumper_core::extract::{Rule, RuleSet};
use pumper_core::storage::NewDatahubGovernAction;
use pumper_core::{EnqueueOptions, Job, Schedule, CATALOG_MANAGED_BY};
use serde_json::{json, Map, Value};
use tracing::{info, warn};

use crate::events::JobEvent;
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

/// Prefix naming what a mid-batch abort already pushed. Batching means a
/// failure is never all-or-nothing: the batches before the failing one are
/// already ingested at GMS, and an error that says only "500" hides that.
/// There is no rollback and no retry — the next emission re-derives the whole
/// set — but the status entry must not pretend nothing landed.
pub(crate) fn partial_abort_note(sent: usize, total: usize) -> String {
    if sent == 0 {
        format!("(0 of {total} entities ingested) ")
    } else {
        format!("(partial: {sent} of {total} entities already ingested) ")
    }
}

async fn post_entities(state: &AppState, entities: Vec<Value>) -> Result<usize, String> {
    let client = client();
    let cfg = &state.config.datahub;
    let url = format!("{}/openapi/entities/v1/", cfg.gms_url.trim_end_matches('/'));
    let token = cfg.resolve_token();
    let total = entities.len();
    let mut sent = 0usize;
    for chunk in entities.chunks(BATCH) {
        let mut req = client.post(&url).json(&chunk);
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.map_err(|e| {
            let cause = std::error::Error::source(&e)
                .map(|s| format!(" ({s})"))
                .unwrap_or_default();
            format!("{}POST {url}: {e}{cause}", partial_abort_note(sent, total))
        })?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let body = body.chars().take(500).collect::<String>();
            return Err(format!(
                "{}POST {url}: {status}: {body}",
                partial_abort_note(sent, total)
            ));
        }
        sent += chunk.len();
    }
    Ok(total)
}

/// Emission history for `GET /datahub/status`.
///
/// The anti-pattern this replaces: ONE `last_emission` slot, where a success
/// seconds after a failure erased the failure. A bridge that fails half its
/// emissions then looked perfectly healthy on every poll. Successes and
/// failures are kept apart and both are counted, so flapping is visible
/// without a log dive.
#[derive(Debug, Default)]
pub struct EmissionStatus {
    /// Most recent entry of either kind (back-compat `last_emission`).
    pub last: Option<Value>,
    pub last_success: Option<Value>,
    pub last_error: Option<Value>,
    /// Monotonic since boot (in-memory, like the entries themselves).
    pub emissions_ok: u64,
    pub emissions_failed: u64,
    /// True while a `POST /datahub/sync` backfill is running — see
    /// [`try_begin_sync`].
    sync_running: bool,
}

impl EmissionStatus {
    /// Files one outcome into the right slot and bumps its counter. Pure
    /// (no state, no clock beyond the caller-supplied entry) so the
    /// "a success must not erase the last error" rule is directly testable.
    pub(crate) fn record(&mut self, entry: Value) {
        if entry["ok"] == Value::Bool(true) {
            self.emissions_ok += 1;
            self.last_success = Some(entry.clone());
        } else {
            self.emissions_failed += 1;
            self.last_error = Some(entry.clone());
        }
        self.last = Some(entry);
    }
}

/// Held for the duration of one `full_sync`; releases the flag on drop, so a
/// panic or an early return can't wedge the bridge into permanent 409.
pub struct SyncGuard(StatusCell);

impl Drop for SyncGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap().sync_running = false;
    }
}

/// Claims the single full-sync slot, or `None` when one is already running.
/// A backfill walks every dataset and read-merges lineage; two in parallel
/// double the GMS load and can interleave their read-merges into lost edges.
pub(crate) fn try_begin_sync(cell: &StatusCell) -> Option<SyncGuard> {
    let mut s = cell.lock().unwrap();
    if s.sync_running {
        return None;
    }
    s.sync_running = true;
    Some(SyncGuard(cell.clone()))
}

/// What one `POST /datahub/sync` did.
pub enum SyncOutcome {
    Ran(Value),
    /// Another backfill is already in flight; this call did nothing (409).
    Busy,
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
    state.datahub_last.lock().unwrap().record(entry.clone());
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
    let mut custom: Vec<(&str, String)> =
        vec![("pumper_app", job.app.clone()), ("kind", kind.into())];
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

/// Emission for a succeeded job: every dataset in the job's namespace (a
/// successful run refreshes them whether or not rows changed — the freshness
/// signal must not go stale on quiet runs), plus the cross-namespace
/// `index_datasets` outputs with lineage edges (own datasets → derived dataset)
/// merged into the edges other writers already registered. One-line hook in the
/// worker; everything (including the revision reads) happens off the hot path.
///
/// Runs on the worker's fan-out pool rather than a bare `tokio::spawn`: off the
/// scrape permit, but **tracked** — the shutdown drain waits for it, and says
/// out loud how many emissions it abandoned instead of dropping them silently.
/// Panics are contained by the pool for the same reason.
pub async fn on_job_success(state: &AppState, job: &Job, index_specs: Vec<(String, String)>) {
    if !state.config.datahub.enabled {
        return;
    }
    let job_id = job.id;
    let pool = state.fanout.clone();
    let state = state.clone();
    let job = job.clone();
    pool.run("datahub", job_id, async move {
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
    })
    .await;
}

/// One-shot backfill: walk every stored dataset and push entity + properties
/// (+ profile/schema per config). The button to press right after connecting a
/// fresh DataHub instance. Returns a summary; also recorded on `/datahub/status`.
///
/// Non-re-entrant by construction: a second concurrent call gets
/// [`SyncOutcome::Busy`] (HTTP 409) rather than queueing or racing the first
/// one's lineage read-merge. Rejecting beats queueing here — the backfill is
/// idempotent, so "come back when it's done" loses nothing.
pub async fn full_sync(state: &AppState) -> SyncOutcome {
    let Some(_guard) = try_begin_sync(&state.datahub_last) else {
        warn!("datahub: /datahub/sync rejected — a full sync is already running");
        return SyncOutcome::Busy;
    };
    SyncOutcome::Ran(full_sync_inner(state).await)
}

/// The backfill body, always under the [`SyncGuard`].
async fn full_sync_inner(state: &AppState) -> Value {
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
//
// The reads are bounded on BOTH axes, because a governance poll is the one path
// where a slow GMS turns into Pumper acting on stale state:
//
//   * `POLL_CONCURRENCY` reads in flight at once, each with the SHORT
//     `POLL_TIMEOUT` — not the emitter's 60s write timeout, which made 20
//     datasets a ~20-minute worst case.
//   * the next poll is gated on the previous one's COMPLETION (not on when it
//     started), so two polls can never race on `paused_apps`.

/// Governance reads in flight at once. Small on purpose: these are GraphQL
/// reads against someone's GMS, and the poll is background work.
const POLL_CONCURRENCY: usize = 4;
/// Per-request timeout for the governance READ path. Deliberately far shorter
/// than the emitter's 60s write client: a stalled read must not hold the poll,
/// and a missed poll self-heals on the next tick.
const POLL_TIMEOUT_SECS: u64 = 10;

/// Worst case for one poll: `datasets / POLL_CONCURRENCY` batches, each bounded
/// by [`POLL_TIMEOUT_SECS`]. Serial reads on the 60s client made this
/// `datasets × 60` — 20 minutes for 20 datasets, i.e. unbounded in practice.
pub(crate) fn worst_case_poll_secs(datasets: usize) -> u64 {
    datasets.div_ceil(POLL_CONCURRENCY) as u64 * POLL_TIMEOUT_SECS
}

/// Whether this tick should start a poll.
///
/// The anti-pattern: stamping `last_poll` when a poll *starts*, so a poll
/// slower than the interval overlapped the next one and two tasks raced to
/// write `paused_apps`. Completion gates the next poll instead — `in_flight`
/// makes overlap impossible by construction, and `since_last` is measured from
/// the previous poll's END.
pub(crate) fn poll_due(
    in_flight: bool,
    since_last: Option<std::time::Duration>,
    interval: std::time::Duration,
) -> bool {
    if in_flight {
        return false;
    }
    match since_last {
        Some(elapsed) => elapsed >= interval,
        None => true,
    }
}

/// Governance state shared with the worker (pause enforcement) and the status
/// route. In-memory only: a restart re-derives everything from DataHub on the
/// next poll, so a dead DataHub after a restart means "no pauses" — fail-open.
#[derive(Debug, Default)]
pub struct GovernState {
    /// When the last poll **finished** (see [`poll_due`]). `pub(crate)` so a
    /// test can age it and exercise the in-flight guard on its own, without
    /// waiting out the 30s minimum interval.
    pub(crate) last_poll: Option<std::time::Instant>,
    /// A poll is running right now; no other tick may start one.
    pub(crate) in_flight: bool,
    paused_apps: HashSet<String>,
    last: Option<Value>,
}

pub type GovernCell = Arc<std::sync::Mutex<GovernState>>;

/// Held for one poll. Releasing the in-flight flag and stamping the completion
/// time both happen on **drop**, so a panicking or early-returning poll can
/// neither wedge governance off forever nor re-fire on the very next tick.
struct PollGuard(GovernCell);

impl Drop for PollGuard {
    fn drop(&mut self) {
        let mut g = self.0.lock().unwrap();
        g.in_flight = false;
        g.last_poll = Some(std::time::Instant::now());
    }
}

/// The budget a job actually runs with: `cost:pause` (from the last governance
/// poll) forces `$0`, which [`pumper_core::AppContext`]'s budget governor turns
/// into free-tiers-only. One-line hook in the worker's `AppContext` build.
pub fn effective_budget(state: &AppState, app: &str, requested: Option<f64>) -> Option<f64> {
    if state
        .datahub_govern
        .lock()
        .unwrap()
        .paused_apps
        .contains(app)
    {
        warn!(
            app,
            "datahub govern: cost:pause tag active — Claude-tier budget forced to $0 for this job"
        );
        Some(0.0)
    } else {
        requested
    }
}

/// Scheduler-tick entry point: gated on `enabled` + `govern`, on the interval
/// **since the last poll finished**, and on nothing else being in flight.
/// Spawned, so a slow GMS never delays the scheduler loop.
pub fn govern_tick(state: &AppState) {
    let cfg = &state.config.datahub;
    if !cfg.enabled || !cfg.govern {
        return;
    }
    let interval = std::time::Duration::from_secs(cfg.govern_interval_secs.max(30));
    let guard = {
        let mut g = state.datahub_govern.lock().unwrap();
        if !poll_due(g.in_flight, g.last_poll.map(|t| t.elapsed()), interval) {
            return;
        }
        g.in_flight = true;
        PollGuard(state.datahub_govern.clone())
    };
    let state = state.clone();
    tokio::spawn(async move {
        // Moved in, so the flag clears (and completion is stamped) whenever the
        // poll ends — including on panic.
        let _guard = guard;
        govern_poll(state).await
    });
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

/// Read-path client: same shape as [`client`], but with the short
/// [`POLL_TIMEOUT_SECS`] instead of the 60s write timeout. Separate instance so
/// tuning the governance path can never lengthen an ingestion POST.
fn poll_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(POLL_TIMEOUT_SECS))
            .build()
            .expect("datahub poll client")
    })
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
    let mut req = poll_client()
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
    if body
        .get("errors")
        .and_then(Value::as_array)
        .is_some_and(|e| !e.is_empty())
    {
        return Err(format!("graphql errors for {urn}"));
    }
    Ok(body)
}

/// Records the poll summary for `GET /datahub/status`.
fn record_govern(state: &AppState, summary: Value) {
    state.datahub_govern.lock().unwrap().last = Some(summary);
}

/// What the read half of one poll observed.
///
/// Errors are **collected, not thrown**: the poll's fail-closed rule (the first
/// error means no action at all) is applied by its caller, so the same read
/// path can serve `GET /datahub/governance/preview`, which must report what it
/// could see AND what it could not rather than going dark on one bad URN.
pub(crate) struct GovernRead {
    pub(crate) metas: Vec<DatasetMeta>,
    pub(crate) errors: Vec<String>,
    pub(crate) datasets: usize,
    pub(crate) elapsed: std::time::Duration,
    pub(crate) budget_secs: u64,
}

/// Reads remote state for every Pumper dataset URN, bounded on both axes
/// ([`POLL_CONCURRENCY`] in flight, [`POLL_TIMEOUT_SECS`] each).
async fn read_govern_metas(state: &AppState) -> GovernRead {
    let started = std::time::Instant::now();
    let all = match state.datasets.list_all_datasets().await {
        Ok(all) => all,
        Err(e) => {
            return GovernRead {
                metas: Vec::new(),
                errors: vec![format!("dataset list failed: {e}")],
                datasets: 0,
                elapsed: started.elapsed(),
                budget_secs: 0,
            }
        }
    };
    let env = state.config.datahub.env.clone();
    // Bounded-concurrency reads on the short-timeout client. Serial reads on
    // the 60s write client made a slow GMS a ~20-minute poll for 20 datasets;
    // the ceiling is now `worst_case_poll_secs(datasets)`.
    let targets: Vec<(String, String, String)> = all
        .iter()
        .map(|(app, ds)| (app.clone(), ds.clone(), dataset_urn(&env, app, ds)))
        .collect();
    let reads = futures::stream::iter(targets.into_iter().map(|(app, ds, urn)| async move {
        fetch_govern_meta(state, &urn)
            .await
            .map(|body| govern_meta(&app, &ds, &body))
    }))
    .buffer_unordered(POLL_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut metas = Vec::with_capacity(all.len());
    let mut errors = Vec::new();
    for read in reads {
        match read {
            Ok(meta) => metas.push(meta),
            Err(e) => errors.push(e),
        }
    }
    // The bound is structural, not aspirational — say so when reality misses it
    // (a GMS answering just under the per-request timeout for every batch).
    let elapsed = started.elapsed();
    let budget_secs = worst_case_poll_secs(all.len());
    if elapsed.as_secs() > budget_secs {
        warn!(
            datasets = all.len(),
            elapsed_secs = elapsed.as_secs(),
            budget_secs,
            "datahub govern: poll exceeded its worst-case read budget"
        );
    }
    GovernRead {
        metas,
        errors,
        datasets: all.len(),
        elapsed,
        budget_secs,
    }
}

/// The schedules a deprecation disable would actually touch for one app: the
/// M19 fence (`managed_by = "catalog"`) plus "currently enabled".
///
/// The fence also lives in SQL (`set_managed_schedule_enabled`), which is what
/// makes it safe; this mirrors it so the **preview** can name the exact rows
/// without writing anything — a preview that guessed would be worse than none.
pub(crate) fn disable_targets<'a>(schedules: &'a [Schedule], app: &str) -> Vec<&'a Schedule> {
    schedules
        .iter()
        .filter(|s| {
            s.app == app && s.enabled && s.managed_by.as_deref() == Some(CATALOG_MANAGED_BY)
        })
        .collect()
}

/// SSE/`/events` status carried by every executed governance action, so the bus
/// shows remote-driven changes alongside the job transitions they cause.
pub(crate) const GOVERN_EVENT_STATUS: &str = "datahub_govern";

/// How long an audit row lives. Diagnostic, like the trigger decision ledger:
/// the *effects* (a disabled schedule, an enqueued job) are durable in their own
/// tables, so an aged-out row loses the explanation, not the state. Longer than
/// the trigger ledger's 14 days because the write rate is a handful of rows per
/// incident, not one per evaluated edge.
const AUDIT_RETENTION_DAYS: u64 = 90;
/// How often the audit prune actually runs — a `DELETE` per poll against a
/// table bounded in months would be pure write amplification.
const AUDIT_PRUNE_EVERY: std::time::Duration = std::time::Duration::from_secs(3600);
/// Last completed audit prune, process-local (the worker's ledger prune takes
/// the same posture: a restart re-arms it, and an extra sweep is free).
static LAST_AUDIT_PRUNE: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

/// Whether the audit prune is due. Mirrors `worker::prune_is_due` — the same
/// anti-pattern (a sweep on every tick against a table bounded in days) applies
/// to any age-bounded ledger.
pub(crate) fn audit_prune_due(
    last: Option<std::time::Instant>,
    now: std::time::Instant,
    every: std::time::Duration,
) -> bool {
    match last {
        None => true, // first poll after boot
        Some(t) => now.duration_since(t) >= every,
    }
}

/// Bounds `datahub_govern_actions` by age, at most once per
/// [`AUDIT_PRUNE_EVERY`]. Rides the governance poll rather than the reaper: the
/// table only grows while governance runs.
async fn prune_audit_trail(state: &AppState) {
    let now = std::time::Instant::now();
    {
        // Dropped before the await: a std Mutex must never be held across one.
        let mut last = LAST_AUDIT_PRUNE.lock().expect("audit prune clock poisoned");
        if !audit_prune_due(*last, now, AUDIT_PRUNE_EVERY) {
            return;
        }
        *last = Some(now);
    }
    match state
        .storage
        .prune_datahub_govern_actions(AUDIT_RETENTION_DAYS)
        .await
    {
        Ok(n) if n > 0 => info!(
            pruned = n,
            days = AUDIT_RETENTION_DAYS,
            "pruned old DataHub governance actions"
        ),
        Ok(_) => {}
        Err(e) => warn!("datahub govern: audit trail prune failed: {e}"),
    }
}

/// Records ONE **executed** governance action: a durable row (migration 0037)
/// plus an event on the same bus job transitions ride.
///
/// The anti-pattern this closes: governance actions existed only in
/// `GovernState.last` — the last poll's in-memory summary, erased by the next
/// poll and by every restart — while their effects (a disabled schedule, a
/// zeroed budget) were durable. "Why is this schedule off?" had no answer.
///
/// Fail-open: the action has already happened when this is called, so a failed
/// write is a warn, and the event is emitted either way (visibility beats
/// consistency for an audit trail of things that are already true).
async fn audit_action(state: &AppState, a: NewDatahubGovernAction<'_>) {
    let id = match state.storage.record_datahub_govern_action(&a).await {
        Ok(id) => Some(id),
        Err(e) => {
            warn!(
                action = a.action,
                target = a.target,
                "datahub govern: audit row not recorded (the action still happened): {e}"
            );
            None
        }
    };
    let mut event = JobEvent::new(
        id.as_deref()
            .and_then(|i| uuid::Uuid::parse_str(i).ok())
            .unwrap_or_else(uuid::Uuid::nil),
        a.target.to_string(),
        GOVERN_EVENT_STATUS,
    );
    event.result = Some(json!({
        "action": a.action,
        "target": a.target,
        "dataset": a.dataset,
        "subject": a.subject,
        "evidence": a.evidence,
        "detail": a.detail,
        "audit_id": id,
    }));
    state.events.emit(event);
}

/// The audit trail for `GET /datahub/status` — the durable half of the
/// governance view.
pub async fn recent_actions(state: &AppState, limit: i64) -> Value {
    match state.storage.list_datahub_govern_actions(limit).await {
        Ok(rows) => serde_json::to_value(rows).unwrap_or(Value::Null),
        Err(e) => json!({ "error": format!("audit trail read failed: {e}") }),
    }
}

/// **What a governance poll would do right now**, without doing any of it.
///
/// Read-only: it reads the same remote state a poll reads and reports the
/// actions that state maps to. It disables nothing, enqueues nothing, and does
/// not touch the paused set — which is why it deliberately works with
/// `govern = false`. That is the whole point: the answer to "what happens if I
/// turn this on?" must be available *before* turning it on.
///
/// Two honest differences from a real poll, both reported rather than hidden:
/// a read error here is collected (`read_errors`) instead of aborting, and
/// `poll_would_abort` says whether a real poll would therefore have done
/// nothing at all.
pub async fn governance_preview(state: &AppState) -> Value {
    let cfg = &state.config.datahub;
    let read = read_govern_metas(state).await;
    let (actions, paused) = plan_govern_actions(&read.metas);
    let schedules = match state.storage.list_schedules().await {
        Ok(s) => s,
        Err(e) => {
            warn!("datahub govern preview: schedules read failed: {e}");
            Vec::new()
        }
    };

    let mut disables: Vec<Value> = Vec::new();
    let mut syncs: Vec<Value> = Vec::new();
    for action in &actions {
        match action {
            GovernAction::DisableSchedules { app, dataset } => {
                let targets = disable_targets(&schedules, app);
                disables.push(json!({
                    "app": app,
                    "dataset": dataset,
                    "evidence": "deprecation",
                    "schedule_ids": targets.iter().map(|s| s.id.clone()).collect::<Vec<_>>(),
                    "note": if targets.is_empty() {
                        "no enabled catalog-managed schedule for this app — the disable would be a no-op"
                    } else {
                        "catalog-managed schedules only; hand-made schedules are never touched"
                    },
                }));
            }
            GovernAction::EnqueueSync { app, dataset } => {
                let registered = state.registry.contains_key(app.as_str());
                syncs.push(json!({
                    "app": app,
                    "dataset": dataset,
                    "evidence": "assertions",
                    "registered": registered,
                    "idempotency_key": govern_sync_key(app, dataset, chrono::Utc::now()),
                    "note": if registered {
                        "one sync job, hour-bucketed so a persistent failure cannot storm"
                    } else {
                        "app is not registered on this instance — the sync would be skipped"
                    },
                }));
            }
        }
    }

    let mut paused_now: Vec<String> = {
        let g = state.datahub_govern.lock().unwrap();
        g.paused_apps.iter().cloned().collect()
    };
    paused_now.sort();
    let mut would_pause: Vec<String> = paused.iter().cloned().collect();
    would_pause.sort();
    let mut would_resume: Vec<String> = paused_now
        .iter()
        .filter(|a| !paused.contains(*a))
        .cloned()
        .collect();
    would_resume.sort();
    let planned_disables = disables
        .iter()
        .filter(|d| !d["schedule_ids"].as_array().is_none_or(|a| a.is_empty()))
        .count();
    // "Quiet" is about CHANGE, not level: an app that is already paused and
    // would stay paused is not something a poll would do to you.
    let newly_paused = would_pause
        .iter()
        .filter(|a| !paused_now.contains(a))
        .count();
    let quiet =
        planned_disables == 0 && syncs.is_empty() && newly_paused == 0 && would_resume.is_empty();

    json!({
        "at": pumper_core::datasets::ts(chrono::Utc::now()),
        "governing": cfg.govern,
        "gms_url": cfg.gms_url,
        "env": cfg.env,
        "datasets_polled": read.datasets,
        "poll_ms": read.elapsed.as_millis() as u64,
        "budget_secs": read.budget_secs,
        "quiet": quiet,
        "would": {
            "disable_schedules": disables,
            "pause_apps": would_pause,
            "resume_apps": would_resume,
            "enqueue_syncs": syncs,
        },
        "paused_now": paused_now,
        "read_errors": read.errors,
        "poll_would_abort": !read.errors.is_empty(),
        "totals": {
            "schedules_disabled": planned_disables,
            "apps_paused": would_pause.len(),
            "syncs_enqueued": syncs.len(),
            "read_errors": read.errors.len(),
        },
    })
}

/// The hour-bucketed idempotency key of a governance-driven sync: a persistently
/// failing assertion enqueues at most one job per hour, not one per poll. Shared
/// with the preview so the previewed key IS the key that would be used.
pub(crate) fn govern_sync_key(
    app: &str,
    dataset: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> String {
    format!(
        "datahub-govern-sync:{app}:{dataset}:{}",
        now.format("%Y-%m-%dT%H")
    )
}

/// One governance poll: read remote state for every dataset URN, plan, apply.
async fn govern_poll(state: AppState) {
    prune_audit_trail(&state).await;
    let read = read_govern_metas(&state).await;
    if let Some(e) = read.errors.first() {
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
    let metas = read.metas;
    let elapsed = read.elapsed;
    let budget = read.budget_secs;

    let (actions, paused) = plan_govern_actions(&metas);
    let mut log: Vec<String> = Vec::new();
    let mut disabled = 0usize;
    let mut syncs = 0usize;

    // Pause set: recomputed wholesale, so removing the tag resumes the app.
    // The transitions (not the level) are what gets audited — an app that was
    // already paused is not news.
    let (newly_paused, newly_resumed) = {
        let mut g = state.datahub_govern.lock().unwrap();
        let entered: Vec<String> = paused.difference(&g.paused_apps).cloned().collect();
        let left: Vec<String> = g.paused_apps.difference(&paused).cloned().collect();
        g.paused_apps = paused.clone();
        (entered, left)
    };
    for app in &newly_paused {
        warn!(app = %app, "datahub govern: cost:pause tag — Claude-tier PAUSED (budget $0) for new jobs");
        log.push(format!("paused {app} (cost:pause tag)"));
        audit_action(
            &state,
            NewDatahubGovernAction {
                action: "pause_app",
                target: app,
                evidence: "cost:pause",
                detail: Some("Claude-tier budget forced to $0 for new jobs"),
                ..Default::default()
            },
        )
        .await;
    }
    for app in &newly_resumed {
        info!(app = %app, "datahub govern: cost:pause tag removed — Claude-tier resumed");
        log.push(format!("resumed {app} (cost:pause tag removed)"));
        audit_action(
            &state,
            NewDatahubGovernAction {
                action: "resume_app",
                target: app,
                evidence: "cost:pause",
                detail: Some("tag removed — normal budgets resume"),
                ..Default::default()
            },
        )
        .await;
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
                for s in disable_targets(&schedules, app) {
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
                            audit_action(
                                &state,
                                NewDatahubGovernAction {
                                    action: "disable_schedule",
                                    target: app,
                                    dataset: Some(dataset),
                                    subject: Some(&s.id),
                                    evidence: "deprecation",
                                    detail: Some(&s.cron),
                                },
                            )
                            .await;
                        }
                        Ok(false) => {
                            warn!(id = %s.id, "datahub govern: disable fenced off (not catalog-managed)")
                        }
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
                let key = govern_sync_key(app, dataset, chrono::Utc::now());
                let opts = EnqueueOptions {
                    params,
                    max_attempts: 2,
                    idempotency_key: Some(key.clone()),
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
                        let job_id = job.id.to_string();
                        audit_action(
                            &state,
                            NewDatahubGovernAction {
                                action: "enqueue_sync",
                                target: app,
                                dataset: Some(dataset),
                                subject: Some(&job_id),
                                evidence: "assertions",
                                detail: Some(&key),
                            },
                        )
                        .await;
                    }
                    Err(e) => warn!(app = %app, "datahub govern: sync enqueue failed: {e}"),
                }
            }
        }
    }

    let summary = json!({
        "at": pumper_core::datasets::ts(chrono::Utc::now()),
        "ok": true,
        "datasets_polled": read.datasets,
        "poll_ms": elapsed.as_millis() as u64,
        "budget_secs": budget,
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

/// Config + emission/governance view for `GET /datahub/status`.
pub fn status(state: &AppState) -> Value {
    let cfg = &state.config.datahub;
    let emissions = emission_status(state);
    let last = emissions["last"].clone();
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
        "last_emission": last,
        "emissions": emissions,
        "govern": govern,
    })
}

/// How many audit rows `GET /datahub/status` carries. The full trail is bounded
/// by age, not by this; the status view is a window, not the ledger.
const STATUS_ACTIONS: i64 = 20;

/// [`status`] plus the **durable** governance audit trail
/// (`govern.recent_actions`). The in-memory half is erased by a restart; the
/// actions it describes are not, which is exactly why they are stored.
pub async fn status_json(state: &AppState) -> Value {
    let mut out = status(state);
    let actions = recent_actions(state, STATUS_ACTIONS).await;
    if let Some(govern) = out.get_mut("govern").and_then(Value::as_object_mut) {
        govern.insert("recent_actions".into(), actions);
    }
    out
}

/// The emission half of [`status`]: counters plus the two independent slots.
/// `last_error` is NOT cleared by a later success — that is the point.
fn emission_status(state: &AppState) -> Value {
    let s = state.datahub_last.lock().unwrap();
    json!({
        "ok": s.emissions_ok,
        "failed": s.emissions_failed,
        "last": s.last,
        "last_success": s.last_success,
        "last_error": s.last_error,
        "sync_running": s.sync_running,
    })
}

pub type StatusCell = Arc<std::sync::Mutex<EmissionStatus>>;

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
        assert!(
            job_rule_set(&json!({"rules": {"t": {"type": "css", "selector": "h1"}}})).is_some()
        );
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
        assert_eq!(
            fg["downstreams"][0],
            format!("urn:li:schemaField:({urn},price)")
        );
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
        assert!(
            matches!(&actions[1], GovernAction::EnqueueSync { app, dataset } if app == "a" && dataset == "d2")
        );
        assert_eq!(paused, HashSet::from(["b".to_string()]));
    }

    #[test]
    fn plan_is_empty_when_datahub_is_quiet() {
        let metas = vec![meta("a", "d1", false, false, false)];
        let (actions, paused) = plan_govern_actions(&metas);
        assert!(actions.is_empty() && paused.is_empty());
    }

    /// The anti-pattern: one `last_emission` slot, where the success that
    /// followed a failure erased it and the bridge looked healthy while
    /// dropping half its emissions.
    #[test]
    fn a_success_does_not_erase_the_last_error() {
        let mut s = EmissionStatus::default();
        s.record(json!({"kind": "job", "ok": false, "error": "boom"}));
        s.record(json!({"kind": "job", "ok": true, "entities": 4}));
        assert_eq!(s.last_error.as_ref().unwrap()["error"], "boom");
        assert_eq!(s.last_success.as_ref().unwrap()["entities"], 4);
        assert_eq!((s.emissions_ok, s.emissions_failed), (1, 1));
        // `last` still tracks the newest of the two for the back-compat field.
        assert_eq!(s.last.as_ref().unwrap()["ok"], true);
    }

    /// A mid-batch abort must not read as "nothing was ingested": earlier
    /// batches are already at GMS and there is no rollback.
    #[test]
    fn partial_abort_note_names_what_already_landed_not_just_the_error() {
        assert_eq!(partial_abort_note(0, 60), "(0 of 60 entities ingested) ");
        assert_eq!(
            partial_abort_note(25, 60),
            "(partial: 25 of 60 entities already ingested) "
        );
    }

    /// Two backfills at once double GMS load and can interleave the lineage
    /// read-merge into lost edges. The slot is claimed, and released on drop
    /// so a panic can't wedge the bridge into permanent 409.
    #[test]
    fn concurrent_sync_is_rejected_and_the_slot_is_released_on_drop() {
        let cell: StatusCell = Arc::new(std::sync::Mutex::new(EmissionStatus::default()));
        let first = try_begin_sync(&cell).expect("first sync claims the slot");
        assert!(
            try_begin_sync(&cell).is_none(),
            "a second concurrent sync must be rejected, not run"
        );
        drop(first);
        assert!(
            try_begin_sync(&cell).is_some(),
            "the slot must be free again once the first sync finishes"
        );
    }

    /// The anti-pattern: `last_poll` stamped when a poll STARTS, so a poll
    /// slower than the interval overlapped the next one and two tasks raced to
    /// write `paused_apps`.
    #[test]
    fn a_hanging_poll_gates_the_next_tick_not_just_the_interval() {
        let interval = std::time::Duration::from_secs(300);
        // Never polled → due.
        assert!(poll_due(false, None, interval));
        // In flight → NOT due, no matter how long ago the last one finished.
        assert!(!poll_due(true, None, interval));
        assert!(!poll_due(
            true,
            Some(std::time::Duration::from_secs(9_999)),
            interval
        ));
        // Idle: the interval since COMPLETION decides.
        assert!(!poll_due(
            false,
            Some(std::time::Duration::from_secs(299)),
            interval
        ));
        assert!(poll_due(
            false,
            Some(std::time::Duration::from_secs(300)),
            interval
        ));
    }

    /// The anti-pattern: one serial read per dataset on the 60s write client,
    /// making the poll's worst case grow linearly at a minute a dataset.
    #[test]
    fn worst_case_poll_is_bounded_by_batches_not_dataset_count() {
        assert_eq!(worst_case_poll_secs(0), 0);
        assert_eq!(worst_case_poll_secs(1), POLL_TIMEOUT_SECS);
        assert_eq!(worst_case_poll_secs(4), POLL_TIMEOUT_SECS);
        // 20 datasets: 5 batches × 10s = 50s, against 20 × 60s = 20 minutes.
        assert_eq!(worst_case_poll_secs(20), 50);
        assert!(worst_case_poll_secs(20) < 20 * 60);
    }

    fn schedule(id: &str, app: &str, enabled: bool, managed_by: Option<&str>) -> Schedule {
        Schedule {
            id: id.into(),
            app: app.into(),
            cron: "0 * * * *".into(),
            params: json!({}),
            enabled,
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once".into(),
            max_attempts: None,
            managed_by: managed_by.map(str::to_string),
            last_run: None,
            created_at: chrono::Utc::now(),
        }
    }

    /// The anti-pattern a preview could reintroduce: naming rows the SQL fence
    /// would refuse anyway. A hand-made schedule is sacred — it must not appear
    /// in the disable set, previewed or executed.
    #[test]
    fn disable_targets_skip_hand_made_and_already_disabled_schedules() {
        let schedules = vec![
            schedule("catalog-a", "a", true, Some(CATALOG_MANAGED_BY)),
            schedule("hand-a", "a", true, None),
            schedule("catalog-a-off", "a", false, Some(CATALOG_MANAGED_BY)),
            schedule("catalog-b", "b", true, Some(CATALOG_MANAGED_BY)),
        ];
        let ids: Vec<&str> = disable_targets(&schedules, "a")
            .iter()
            .map(|s| s.id.as_str())
            .collect();
        assert_eq!(ids, vec!["catalog-a"]);
        assert!(disable_targets(&schedules, "c").is_empty());
    }

    /// The anti-pattern: one sync per poll for a persistently failing assertion
    /// — a 300s poll turning one broken dataset into 12 jobs an hour.
    #[test]
    fn sync_key_buckets_by_hour_not_by_poll() {
        let at = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap().to_utc();
        let a = govern_sync_key("app", "ds", at("2026-08-04T10:00:00Z"));
        let b = govern_sync_key("app", "ds", at("2026-08-04T10:59:59Z"));
        let c = govern_sync_key("app", "ds", at("2026-08-04T11:00:00Z"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.ends_with("2026-08-04T10"), "{a}");
    }

    /// The anti-pattern: a retention `DELETE` on every poll against a table
    /// bounded in months.
    #[test]
    fn audit_prune_runs_hourly_not_on_every_poll() {
        let now = std::time::Instant::now();
        let hour = std::time::Duration::from_secs(3600);
        assert!(audit_prune_due(None, now, hour), "first poll after boot");
        assert!(!audit_prune_due(Some(now), now, hour));
        assert!(audit_prune_due(
            now.checked_sub(hour),
            now,
            std::time::Duration::from_secs(3600)
        ));
    }

    #[test]
    fn properties_customs_are_strings() {
        let p = dataset_properties("grants", "unified", &[("record_count", "12".into())]);
        assert_eq!(p["customProperties"]["record_count"], "12");
        assert_eq!(p["name"], "grants/unified");
    }
}
