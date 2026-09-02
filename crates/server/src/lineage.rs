//! N24 — vendor-neutral lineage: the run/quality model both catalog writers
//! render, plus the OpenLineage writer itself.
//!
//! ## Why there is a model at all
//!
//! Before this module the emitter built DataHub's v1 ingestion envelope
//! *directly* at every call site ([`crate::datahub`]), so "support a second
//! catalog" meant "write a second emitter that re-derives the same facts from
//! the same tables". The facts were never DataHub-shaped to begin with: a run
//! with inputs, outputs, a schema per output, per-column provenance and
//! new/changed/removed counts is the OpenLineage model almost line for line.
//!
//! So the gather step now produces one [`LineageEvent`] — what this run did, in
//! nobody's vocabulary — and each writer is a pure rendering of it:
//!
//! * [`crate::datahub::DatahubEntities`] `From<&LineageEvent>` — the existing
//!   DataHub entity list, unchanged (pinned by the `golden_*` tests in
//!   `datahub.rs`, which is why they were written *before* the extraction).
//! * [`run_event`] — an OpenLineage 2.x `RunEvent`.
//!
//! ## What is honestly absent
//!
//! There is **no `START` emission wired**. The only metadata call site on the
//! success path is the fan-out's `on_job_success`, and the worker has no
//! emitter hook at job start; inventing one would put a network post on the
//! scrape permit's hot path. [`RunOutcome::Start`] exists and renders (a
//! consumer that receives only `COMPLETE`/`FAIL` still materialises the run),
//! and wiring it is a worker change, not a writer change.
//!
//! A backfill (`full_sync`) builds a [`LineageEvent`] with `run: None`. That is
//! not a run and the OpenLineage writer skips it rather than minting a
//! synthetic run id: OpenLineage runs are things that happened, and a catalog
//! sweep did not happen to any job.
//!
//! ## Posture
//!
//! Same as the DataHub writer and for the same reason: **no retry**. A failed
//! post is recorded on its own status slot (`GET /datahub/status` →
//! `lineage.writers[]`) and healed by the next run. Two writers double the
//! failure surface, so each is counted and reported separately — a green
//! DataHub must never make a dead Marquez look fine.

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use tracing::{info, warn};
use uuid::Uuid;

use crate::state::AppState;

/// `producer` on every event and facet: who wrote this, at what version.
pub const PRODUCER: &str = concat!(
    "https://github.com/pumper/pumper/tree/v",
    env!("CARGO_PKG_VERSION")
);

/// OpenLineage core spec version this writer emits.
const SPEC: &str = "2-0-2";

/// `_schemaURL` for a named facet at a named facet-spec version.
fn facet_url(name: &str, version: &str) -> String {
    format!("https://openlineage.io/spec/facets/{version}/{name}.json#/$defs/{name}")
}

/// Wraps a facet body with the two fields every OpenLineage facet must carry.
/// Extracted because a facet missing `_producer`/`_schemaURL` is accepted by
/// permissive receivers and dropped by strict ones — a divergence that shows up
/// as "Marquez shows the run but no schema", days later.
fn facet(name: &str, version: &str, mut body: Map<String, Value>) -> Value {
    body.insert("_producer".into(), json!(PRODUCER));
    body.insert("_schemaURL".into(), json!(facet_url(name, version)));
    Value::Object(body)
}

// ── the model ───────────────────────────────────────────────────────────────

/// Where a run stands when the event is emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The run began.
    ///
    /// Rendered but **not wired**: there is no emitter call site at job start
    /// (see the module docs), so nothing outside the tests constructs this
    /// today. It stays in the enum because wiring it is a worker change, not a
    /// writer change, and a consumer that receives only `COMPLETE`/`FAIL` still
    /// materialises the run.
    #[allow(dead_code)]
    Start,
    /// The run succeeded.
    Complete,
    /// The run failed permanently (attempts exhausted).
    Fail,
}

impl RunOutcome {
    /// OpenLineage `eventType`.
    pub fn as_openlineage(self) -> &'static str {
        match self {
            RunOutcome::Start => "START",
            RunOutcome::Complete => "COMPLETE",
            RunOutcome::Fail => "FAIL",
        }
    }
}

/// How a dataset relates to the run.
///
/// This classifies **outputs** for the DataHub render, which reproduces the
/// pre-N24 entity sequence by walking own-then-derived. Inputs are never
/// rendered as dataset aspects (Pumper did not write them) — only as edges,
/// and, for [`Source`](Self::Source), as an external entity of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetOrigin {
    /// Output, written under the job's own app namespace.
    Own,
    /// Output, written under another app's namespace (`index_datasets`), so it
    /// also carries table-level upstream edges back to the job's own datasets.
    Derived,
    /// Input: a Pumper dataset another app owns (the trigger's source).
    Upstream,
    /// Input: an external `[[source]]` from the catalog. Pumper reads it,
    /// nothing writes it.
    Source,
}

/// The catalog row behind an external upstream.
#[derive(Debug, Clone, Default)]
pub struct SourceRef {
    pub id: String,
    pub name: String,
    pub url: String,
    pub cadence: String,
    pub access: String,
    pub category: String,
}

/// What one run did to one dataset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutputStats {
    pub new: usize,
    pub changed: usize,
    pub removed: usize,
}

impl OutputStats {
    /// Whether this run touched the dataset at all. The column-lineage write is
    /// gated on it: re-asserting field provenance on a dataset a quiet run did
    /// not write is noise, and it was the pre-N24 behaviour too.
    pub fn touched(&self) -> bool {
        self.new + self.changed + self.removed > 0
    }
}

/// One quality judgement Pumper made about a dataset, in the vocabulary both
/// catalogs understand: a named check with a pass/fail and a reason.
#[derive(Debug, Clone)]
pub struct Assertion {
    pub name: String,
    pub passed: bool,
    /// Why it failed (or what it observed). `None` = nothing to add.
    pub message: Option<String>,
}

/// A dataset as it appears in one run's lineage.
#[derive(Debug, Clone)]
pub struct LineageDataset {
    pub app: String,
    pub dataset: String,
    pub origin: DatasetOrigin,
    /// Set only for [`DatasetOrigin::Source`].
    pub source: Option<SourceRef>,
    /// Total rows in the dataset now. `None` = the count read failed.
    pub rows: Option<i64>,
    /// Row count for the profile aspect — `None` when profile emission is off,
    /// which is how the render honours `[datahub] emit_profile` without the
    /// vendor-neutral model learning about a DataHub config key.
    pub profile_rows: Option<i64>,
    /// One stored record, for schema inference. `None` when schema emission is
    /// off or the dataset is empty.
    pub sample: Option<Value>,
    /// What this run wrote. `None` on a backfill (no run).
    pub stats: Option<OutputStats>,
    /// `(column, transform)` provenance, from a declarative `RuleSet` only.
    /// Empty when the app's extraction is code — guessing would poison the graph.
    pub column_ops: Vec<(String, String)>,
    /// Table-level upstream URNs, already merged with the ones the remote
    /// catalog holds. `None` = no upstream write for this dataset.
    pub upstreams: Option<Vec<String>>,
    /// The external source this dataset is extracted from, when the catalog
    /// names one and `[lineage] emit_sources` is on.
    pub source_upstream: Option<SourceRef>,
    pub assertions: Vec<Assertion>,
    /// `key:value` labels (`health:degraded`, `trust:provisional`).
    pub tags: Vec<String>,
}

impl LineageDataset {
    /// A bare output/input entry: everything optional starts absent, and the
    /// gather step fills in only what it actually read.
    pub fn new(app: impl Into<String>, dataset: impl Into<String>, origin: DatasetOrigin) -> Self {
        Self {
            app: app.into(),
            dataset: dataset.into(),
            origin,
            source: None,
            rows: None,
            profile_rows: None,
            sample: None,
            stats: None,
            column_ops: Vec::new(),
            upstreams: None,
            source_upstream: None,
            assertions: Vec::new(),
            tags: Vec::new(),
        }
    }

    /// `<app>.<dataset>` — the OpenLineage dataset name, and the middle segment
    /// of the DataHub URN.
    pub fn qualified_name(&self) -> String {
        match (&self.origin, &self.source) {
            (DatasetOrigin::Source, Some(s)) => s.id.clone(),
            _ => format!("{}.{}", self.app, self.dataset),
        }
    }
}

/// The pipeline a run belongs to: its schedule, the trigger that fired it, or
/// the app's ad-hoc bucket.
#[derive(Debug, Clone)]
pub struct FlowRef {
    pub flow_id: String,
    pub name: String,
    pub kind: &'static str,
    pub schedule_id: Option<String>,
    pub trigger_id: Option<String>,
}

/// The custom `pumper` run facet: the things no vendor schema has a slot for.
#[derive(Debug, Clone, Default)]
pub struct PumperFacet {
    pub job_id: String,
    pub app: String,
    pub attempts: i64,
    /// `schedule` | `trigger` | `adhoc`.
    pub flow_kind: String,
    pub schedule_id: Option<String>,
    pub trigger_id: Option<String>,
    /// N03 workflow run this job is a step of.
    pub workflow_run_id: Option<String>,
    /// The root of the chain this job hangs off (N03). This is what makes a
    /// fan-out of twenty jobs render as one lineage story instead of twenty.
    pub root_id: Option<String>,
    /// Metered spend for this job, in USD. `None` = the ledger read failed;
    /// `Some(0.0)` = it really cost nothing.
    pub cost_usd: Option<f64>,
}

/// One run's lineage, in nobody's vocabulary.
#[derive(Debug, Clone)]
pub struct LineageEvent {
    /// Deployment environment label (`PROD`/`DEV`) — DataHub's fabric segment,
    /// and a plain property on the OpenLineage side.
    pub env: String,
    /// One clock read for the whole event. Every aspect that carries a
    /// timestamp shares it, so all of a run's metadata lands on one instant
    /// instead of smeared across the emission.
    pub ms: i64,
    pub event_time: DateTime<Utc>,
    /// `None` on a backfill — see the module docs.
    pub run: Option<RunRef>,
    pub flow: Option<FlowRef>,
    pub inputs: Vec<LineageDataset>,
    /// Own-namespace datasets first, then derived ones. The DataHub render
    /// relies on that order to reproduce the pre-N24 entity sequence exactly.
    pub outputs: Vec<LineageDataset>,
}

/// The run half of a [`LineageEvent`].
#[derive(Debug, Clone)]
pub struct RunRef {
    pub job_id: Uuid,
    pub app: String,
    pub attempts: i64,
    pub outcome: RunOutcome,
    /// Set on [`RunOutcome::Fail`].
    pub error: Option<String>,
    pub pumper: PumperFacet,
}

impl LineageEvent {
    /// A backfill event: datasets, no run, no flow.
    pub fn backfill(env: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            env: env.into(),
            ms: now.timestamp_millis(),
            event_time: now,
            run: None,
            flow: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// A run event.
    pub fn for_run(env: impl Into<String>, run: RunRef) -> Self {
        let now = Utc::now();
        Self {
            env: env.into(),
            ms: now.timestamp_millis(),
            event_time: now,
            run: Some(run),
            flow: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// The OpenLineage job name: the flow when there is one, else the app.
    pub fn job_name(&self) -> String {
        match (&self.flow, &self.run) {
            (Some(f), _) => f.flow_id.clone(),
            (None, Some(r)) => format!("adhoc.{}", r.app),
            (None, None) => "sync".to_string(),
        }
    }
}

// ── the OpenLineage writer (pure) ───────────────────────────────────────────

/// `schema` dataset facet from one sample record. `None` when there is no
/// sample: an empty field list is a claim ("this dataset has no columns") that
/// a failed or skipped read is in no position to make.
fn schema_facet(sample: Option<&Value>) -> Option<Value> {
    let obj = sample?.as_object()?;
    let fields: Vec<Value> = obj
        .iter()
        .map(|(k, v)| json!({ "name": k, "type": ol_type(v) }))
        .collect();
    let mut body = Map::new();
    body.insert("fields".into(), Value::Array(fields));
    Some(facet("SchemaDatasetFacet", "1-1-1", body))
}

/// OpenLineage type name for a sample JSON value.
fn ol_type(v: &Value) -> &'static str {
    match v {
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Array(_) => "array",
        Value::Object(_) => "struct",
        Value::Null => "null",
    }
}

/// `columnLineage` dataset facet. Each column names its transform, and — when
/// the catalog knows the external source — the input dataset it came from.
///
/// The pre-N24 DataHub emitter had to write `upstreamType: NONE` here, because
/// the honest upstream (a fetched page) was not modelled as a dataset. With
/// `[lineage] emit_sources` on it is, and the field can finally point at it.
fn column_lineage_facet(d: &LineageDataset) -> Option<Value> {
    if d.column_ops.is_empty() {
        return None;
    }
    let input_fields = |field: &str| -> Vec<Value> {
        match &d.source_upstream {
            Some(s) => vec![json!({
                "namespace": SOURCE_NAMESPACE,
                "name": s.id,
                "field": field,
            })],
            None => Vec::new(),
        }
    };
    let mut fields = Map::new();
    for (col, op) in &d.column_ops {
        fields.insert(
            col.clone(),
            json!({
                "inputFields": input_fields(col),
                "transformationType": "INDIRECT",
                "transformationDescription": op,
            }),
        );
    }
    let mut body = Map::new();
    body.insert("fields".into(), Value::Object(fields));
    Some(facet("ColumnLineageDatasetFacet", "1-2-0", body))
}

/// `dataQualityAssertions` dataset facet from Pumper's own verdicts.
fn assertions_facet(assertions: &[Assertion]) -> Option<Value> {
    if assertions.is_empty() {
        return None;
    }
    let list: Vec<Value> = assertions
        .iter()
        .map(|a| {
            let mut e = json!({ "assertion": a.name, "success": a.passed });
            if let Some(m) = &a.message {
                e["column"] = Value::Null;
                e["message"] = json!(m);
            }
            e
        })
        .collect();
    let mut body = Map::new();
    body.insert("assertions".into(), Value::Array(list));
    Some(facet("DataQualityAssertionsDatasetFacet", "1-0-1", body))
}

/// `tags` dataset facet from `key:value` labels. A label without a colon is
/// carried as a key with an empty value rather than dropped.
fn tags_facet(tags: &[String]) -> Option<Value> {
    if tags.is_empty() {
        return None;
    }
    let list: Vec<Value> = tags
        .iter()
        .map(|t| {
            let (k, v) = t.split_once(':').unwrap_or((t.as_str(), ""));
            json!({ "key": k, "value": v, "source": "pumper" })
        })
        .collect();
    let mut body = Map::new();
    body.insert("tags".into(), Value::Array(list));
    Some(facet("TagsDatasetFacet", "1-0-0", body))
}

/// `outputStatistics` output facet: rows written and how they broke down.
fn output_statistics_facet(stats: &OutputStats, rows: Option<i64>) -> Value {
    let mut body = Map::new();
    body.insert("rowCount".into(), json!((stats.new + stats.changed) as i64));
    body.insert("size".into(), Value::Null);
    body.insert("newRows".into(), json!(stats.new));
    body.insert("changedRows".into(), json!(stats.changed));
    body.insert("removedRows".into(), json!(stats.removed));
    // Honest absence: `None` here means the count read failed, and a zero would
    // be indistinguishable from an empty dataset.
    body.insert(
        "datasetRowCount".into(),
        rows.map(|r| json!(r)).unwrap_or(Value::Null),
    );
    facet("OutputStatisticsOutputDatasetFacet", "1-0-1", body)
}

/// Namespace for external `[[source]]` inputs. `web` is the platform these
/// actually live on, and it is what DataHub's own convention calls them.
pub const SOURCE_NAMESPACE: &str = "web";

/// One dataset in an OpenLineage event. `role` selects the facet bucket name
/// (`inputFacets` / `outputFacets`), which the spec keeps separate from the
/// static `facets`.
fn ol_dataset(namespace: &str, d: &LineageDataset) -> Value {
    let ns = match d.origin {
        DatasetOrigin::Source => SOURCE_NAMESPACE.to_string(),
        _ => namespace.to_string(),
    };
    let mut facets = Map::new();
    if let Some(f) = schema_facet(d.sample.as_ref()) {
        facets.insert("schema".into(), f);
    }
    if let Some(f) = column_lineage_facet(d) {
        facets.insert("columnLineage".into(), f);
    }
    if let Some(f) = assertions_facet(&d.assertions) {
        facets.insert("dataQualityAssertions".into(), f);
    }
    if let Some(f) = tags_facet(&d.tags) {
        facets.insert("tags".into(), f);
    }
    if let Some(s) = &d.source {
        let mut body = Map::new();
        body.insert("name".into(), json!(s.name));
        body.insert("uri".into(), json!(s.url));
        facets.insert(
            "dataSource".into(),
            facet("DatasourceDatasetFacet", "1-0-1", body),
        );
    }
    let mut out = json!({ "namespace": ns, "name": d.qualified_name() });
    if !facets.is_empty() {
        out["facets"] = Value::Object(facets);
    }
    if let Some(stats) = &d.stats {
        out["outputFacets"] = json!({
            "outputStatistics": output_statistics_facet(stats, d.rows),
        });
    }
    out
}

/// The custom `pumper` run facet.
fn pumper_facet(p: &PumperFacet) -> Value {
    let mut body = Map::new();
    body.insert("_producer".into(), json!(PRODUCER));
    body.insert(
        "_schemaURL".into(),
        json!(format!("{PRODUCER}#/$defs/PumperRunFacet")),
    );
    body.insert("jobId".into(), json!(p.job_id));
    body.insert("app".into(), json!(p.app));
    body.insert("attempts".into(), json!(p.attempts));
    body.insert("flowKind".into(), json!(p.flow_kind));
    // Every one of these is Null-when-absent rather than omitted: a consumer
    // reading `rootId` must be able to tell "not part of a chain" from "this
    // build does not know about chains".
    let opt = |v: &Option<String>| v.clone().map(Value::String).unwrap_or(Value::Null);
    body.insert("scheduleId".into(), opt(&p.schedule_id));
    body.insert("triggerId".into(), opt(&p.trigger_id));
    body.insert("workflowRunId".into(), opt(&p.workflow_run_id));
    body.insert("rootId".into(), opt(&p.root_id));
    body.insert(
        "costUsd".into(),
        p.cost_usd
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
    );
    Value::Object(body)
}

/// Renders one [`LineageEvent`] as an OpenLineage 2.x `RunEvent`.
///
/// Pure: no clock, no config lookup, no I/O — every input is on the event. That
/// is what makes the golden tests able to pin one event per outcome.
pub fn run_event(namespace: &str, ev: &LineageEvent) -> Option<Value> {
    let run = ev.run.as_ref()?;
    let mut run_facets = Map::new();
    run_facets.insert("pumper".into(), pumper_facet(&run.pumper));
    if let Some(root) = &run.pumper.root_id {
        // `parent` is how OpenLineage says "this run belongs to that one";
        // without it a workflow's steps render as unrelated runs.
        let mut body = Map::new();
        body.insert(
            "run".into(),
            json!({ "runId": run.pumper.workflow_run_id.clone().unwrap_or_else(|| root.clone()) }),
        );
        body.insert(
            "job".into(),
            json!({ "namespace": namespace, "name": format!("workflow.{root}") }),
        );
        run_facets.insert("parent".into(), facet("ParentRunFacet", "1-1-0", body));
    }
    if let Some(err) = &run.error {
        let mut body = Map::new();
        body.insert("message".into(), json!(err));
        body.insert("programmingLanguage".into(), json!("RUST"));
        run_facets.insert(
            "errorMessage".into(),
            facet("ErrorMessageRunFacet", "1-0-0", body),
        );
    }

    let mut job_facets = Map::new();
    let mut jt = Map::new();
    jt.insert("processingType".into(), json!("BATCH"));
    jt.insert("integration".into(), json!("PUMPER"));
    jt.insert(
        "jobType".into(),
        json!(ev
            .flow
            .as_ref()
            .map(|f| f.kind)
            .unwrap_or("adhoc")
            .to_uppercase()),
    );
    job_facets.insert("jobType".into(), facet("JobTypeJobFacet", "2-0-3", jt));

    Some(json!({
        "eventTime": ev.event_time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "producer": PRODUCER,
        "schemaURL": format!("https://openlineage.io/spec/{SPEC}/OpenLineage.json#/$defs/RunEvent"),
        "eventType": run.outcome.as_openlineage(),
        "run": { "runId": run.job_id.to_string(), "facets": Value::Object(run_facets) },
        "job": { "namespace": namespace, "name": ev.job_name(), "facets": Value::Object(job_facets) },
        "inputs": ev.inputs.iter().map(|d| ol_dataset(namespace, d)).collect::<Vec<_>>(),
        "outputs": ev.outputs.iter().map(|d| ol_dataset(namespace, d)).collect::<Vec<_>>(),
    }))
}

// ── the OpenLineage writer (I/O) ────────────────────────────────────────────

/// Own client with the emitter's timeout: a cold Marquez behaves like a cold
/// GMS, and the 15s webhook client is not the right shape for either.
fn client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .expect("openlineage client")
    })
}

/// Posts one `RunEvent`. No retry, by the same argument as the DataHub writer:
/// lineage is re-derived every run, so a queue buys staleness insurance nobody
/// asked for.
pub async fn emit(state: &AppState, ev: &LineageEvent) {
    let cfg = &state.config.lineage;
    let Some(url) = cfg.endpoint() else {
        return;
    };
    let Some(body) = run_event(&cfg.namespace, ev) else {
        // A backfill is not a run. Deliberately silent: this is the documented
        // shape of the model, not a failure to report.
        return;
    };
    let outcome = post(&url, cfg.resolve_api_key().as_deref(), &body).await;
    let kind = ev
        .run
        .as_ref()
        .map(|r| r.outcome.as_openlineage())
        .unwrap_or("SYNC");
    let entry = match &outcome {
        Ok(()) => {
            info!(kind, "openlineage: run event emitted");
            json!({
                "kind": kind,
                "at": pumper_core::datasets::ts(chrono::Utc::now()),
                "ok": true,
                "entities": 1,
            })
        }
        Err(e) => {
            warn!(kind, "openlineage: emission failed: {e}");
            json!({
                "kind": kind,
                "at": pumper_core::datasets::ts(chrono::Utc::now()),
                "ok": false,
                "error": e,
            })
        }
    };
    state.lineage_last.lock().unwrap().record(entry);
}

async fn post(url: &str, key: Option<&str>, body: &Value) -> Result<(), String> {
    let mut req = client().post(url).json(body);
    if let Some(k) = key {
        req = req.bearer_auth(k);
    }
    let resp = req.send().await.map_err(|e| {
        let cause = std::error::Error::source(&e)
            .map(|s| format!(" ({s})"))
            .unwrap_or_default();
        format!("POST {url}: {e}{cause}")
    })?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        let text = text.chars().take(500).collect::<String>();
        return Err(format!("POST {url}: {status}: {text}"));
    }
    Ok(())
}

// ── quality inputs (pure) ───────────────────────────────────────────────────

/// The assertion a stored contract verdict describes, when the verdict belongs
/// to THIS run.
///
/// The staleness fence is the point. Verdicts live in one in-memory map keyed
/// by `<app>/<dataset>` and are overwritten by whichever run judged that pair
/// last. Pushing whatever is in the slot would stamp another job's verdict onto
/// this run's assertion result — a wrong fact, published to someone's catalog,
/// with this run's id on it.
pub fn verdict_assertion(verdict: &Value, job_id: Uuid) -> Option<Assertion> {
    if verdict.get("job_id")?.as_str()? != job_id.to_string() {
        return None;
    }
    let v = verdict.get("verdict")?.as_str()?;
    let violations: Vec<String> = verdict
        .get("violations")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Some(Assertion {
        name: "pumper.data_contract".into(),
        passed: v == "pass",
        message: (!violations.is_empty()).then(|| violations.join("; ")),
    })
}

/// `key:value` tags for a dataset's extraction health and trust tier.
///
/// `health` is always stamped (including `healthy` — "we looked and it is
/// fine" is a different fact from "nobody looked"), `trust` only where the
/// health ladder actually stamps one.
pub fn health_tags(state_label: &str, trust: Option<&str>) -> Vec<String> {
    let mut tags = vec![format!("health:{state_label}")];
    if let Some(t) = trust {
        tags.push(format!("trust:{t}"));
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ds() -> LineageDataset {
        let mut d = LineageDataset::new("hn", "stories", DatasetOrigin::Own);
        d.rows = Some(12);
        d.sample = Some(json!({ "title": "a", "url": "b" }));
        d.stats = Some(OutputStats {
            new: 2,
            changed: 1,
            removed: 0,
        });
        d.column_ops = vec![("title".into(), "css:h1".into())];
        d
    }

    fn run(outcome: RunOutcome) -> RunRef {
        RunRef {
            job_id: Uuid::nil(),
            app: "hn".into(),
            attempts: 1,
            outcome,
            error: (outcome == RunOutcome::Fail).then(|| "boom".to_string()),
            pumper: PumperFacet {
                job_id: Uuid::nil().to_string(),
                app: "hn".into(),
                attempts: 1,
                flow_kind: "schedule".into(),
                schedule_id: Some("nightly".into()),
                trigger_id: None,
                workflow_run_id: None,
                root_id: None,
                cost_usd: Some(0.25),
            },
        }
    }

    fn event(outcome: RunOutcome) -> LineageEvent {
        let mut ev = LineageEvent::for_run("PROD", run(outcome));
        ev.event_time = DateTime::parse_from_rfc3339("2026-09-01T12:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        ev.ms = ev.event_time.timestamp_millis();
        ev.flow = Some(FlowRef {
            flow_id: "schedule.hn.nightly".into(),
            name: "hn (schedule nightly)".into(),
            kind: "schedule",
            schedule_id: Some("nightly".into()),
            trigger_id: None,
        });
        ev.outputs = vec![ds()];
        ev
    }

    // ── golden: one RunEvent per outcome ────────────────────────────────────

    #[test]
    fn golden_complete_run_event() {
        let v = run_event("pumper", &event(RunOutcome::Complete)).unwrap();
        assert_eq!(v["eventType"], "COMPLETE");
        assert_eq!(v["eventTime"], "2026-09-01T12:00:00.000Z");
        assert_eq!(v["producer"], PRODUCER);
        assert_eq!(
            v["schemaURL"],
            "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
        );
        assert_eq!(v["run"]["runId"], Uuid::nil().to_string());
        assert_eq!(v["job"]["namespace"], "pumper");
        assert_eq!(v["job"]["name"], "schedule.hn.nightly");
        assert_eq!(v["job"]["facets"]["jobType"]["jobType"], "SCHEDULE");
        let out = &v["outputs"][0];
        assert_eq!(out["namespace"], "pumper");
        assert_eq!(out["name"], "hn.stories");
        assert_eq!(
            out["facets"]["schema"]["fields"],
            json!([{ "name": "title", "type": "string" }, { "name": "url", "type": "string" }])
        );
        assert_eq!(
            out["facets"]["columnLineage"]["fields"]["title"]["transformationDescription"],
            "css:h1"
        );
        let stats = &out["outputFacets"]["outputStatistics"];
        assert_eq!(stats["rowCount"], 3);
        assert_eq!(stats["newRows"], 2);
        assert_eq!(stats["changedRows"], 1);
        assert_eq!(stats["removedRows"], 0);
        assert_eq!(stats["datasetRowCount"], 12);
        let pf = &v["run"]["facets"]["pumper"];
        assert_eq!(pf["app"], "hn");
        assert_eq!(pf["flowKind"], "schedule");
        assert_eq!(pf["costUsd"], 0.25);
        assert_eq!(pf["rootId"], Value::Null);
        // No parent facet without a chain root.
        assert!(v["run"]["facets"].get("parent").is_none());
        assert!(v["run"]["facets"].get("errorMessage").is_none());
    }

    #[test]
    fn golden_start_run_event_carries_no_statistics_it_cannot_know() {
        let mut ev = event(RunOutcome::Start);
        for d in &mut ev.outputs {
            d.stats = None;
            d.sample = None;
        }
        let v = run_event("pumper", &ev).unwrap();
        assert_eq!(v["eventType"], "START");
        let out = &v["outputs"][0];
        assert!(out.get("outputFacets").is_none());
        assert!(out["facets"].get("schema").is_none());
    }

    #[test]
    fn golden_fail_run_event_carries_the_error_facet() {
        let v = run_event("pumper", &event(RunOutcome::Fail)).unwrap();
        assert_eq!(v["eventType"], "FAIL");
        assert_eq!(v["run"]["facets"]["errorMessage"]["message"], "boom");
        assert_eq!(
            v["run"]["facets"]["errorMessage"]["_schemaURL"],
            facet_url("ErrorMessageRunFacet", "1-0-0")
        );
    }

    #[test]
    fn a_backfill_is_not_a_run_and_renders_no_event() {
        let ev = LineageEvent::backfill("PROD");
        assert!(run_event("pumper", &ev).is_none());
    }

    #[test]
    fn a_workflow_step_names_its_parent_run_not_just_its_own() {
        let mut ev = event(RunOutcome::Complete);
        if let Some(r) = &mut ev.run {
            r.pumper.root_id = Some("root-7".into());
            r.pumper.workflow_run_id = Some("wf-3".into());
        }
        let v = run_event("pumper", &ev).unwrap();
        assert_eq!(v["run"]["facets"]["parent"]["run"]["runId"], "wf-3");
        assert_eq!(
            v["run"]["facets"]["parent"]["job"]["name"],
            "workflow.root-7"
        );
        assert_eq!(v["run"]["facets"]["pumper"]["rootId"], "root-7");
    }

    // ── the source-as-upstream claim ────────────────────────────────────────

    #[test]
    fn a_source_input_lands_in_the_web_namespace_not_pumpers() {
        let mut ev = event(RunOutcome::Complete);
        let mut src = LineageDataset::new("hn", "", DatasetOrigin::Source);
        src.source = Some(SourceRef {
            id: "hacker-news".into(),
            name: "Hacker News".into(),
            url: "https://news.ycombinator.com".into(),
            ..Default::default()
        });
        ev.inputs = vec![src];
        let v = run_event("pumper", &ev).unwrap();
        assert_eq!(v["inputs"][0]["namespace"], SOURCE_NAMESPACE);
        assert_eq!(v["inputs"][0]["name"], "hacker-news");
        assert_eq!(
            v["inputs"][0]["facets"]["dataSource"]["uri"],
            "https://news.ycombinator.com"
        );
    }

    #[test]
    fn column_lineage_names_the_source_not_nothing_when_the_catalog_knows_it() {
        let mut ev = event(RunOutcome::Complete);
        ev.outputs[0].source_upstream = Some(SourceRef {
            id: "hacker-news".into(),
            ..Default::default()
        });
        let v = run_event("pumper", &ev).unwrap();
        let f = &v["outputs"][0]["facets"]["columnLineage"]["fields"]["title"]["inputFields"][0];
        assert_eq!(f["namespace"], SOURCE_NAMESPACE);
        assert_eq!(f["name"], "hacker-news");
        assert_eq!(f["field"], "title");
    }

    #[test]
    fn column_lineage_claims_no_upstream_when_the_catalog_names_none() {
        let v = run_event("pumper", &event(RunOutcome::Complete)).unwrap();
        assert_eq!(
            v["outputs"][0]["facets"]["columnLineage"]["fields"]["title"]["inputFields"],
            json!([])
        );
    }

    // ── quality ─────────────────────────────────────────────────────────────

    #[test]
    fn a_verdict_from_another_run_is_not_published_as_this_runs_assertion() {
        let mine = Uuid::from_u128(1);
        let theirs = Uuid::from_u128(2);
        let verdict = json!({
            "verdict": "warn",
            "violations": ["row_count 0 < min 1"],
            "job_id": theirs.to_string(),
        });
        assert!(verdict_assertion(&verdict, mine).is_none());
        let a = verdict_assertion(&verdict, theirs).unwrap();
        assert!(!a.passed);
        assert_eq!(a.message.as_deref(), Some("row_count 0 < min 1"));
        assert_eq!(a.name, "pumper.data_contract");
    }

    #[test]
    fn a_passing_verdict_is_an_assertion_too_not_silence() {
        let id = Uuid::from_u128(3);
        let verdict = json!({ "verdict": "pass", "violations": [], "job_id": id.to_string() });
        let a = verdict_assertion(&verdict, id).unwrap();
        assert!(a.passed);
        assert_eq!(a.message, None);
    }

    #[test]
    fn assertions_and_tags_reach_the_event_as_facets() {
        let mut ev = event(RunOutcome::Complete);
        ev.outputs[0].assertions = vec![Assertion {
            name: "pumper.data_contract".into(),
            passed: false,
            message: Some("row_count 0 < min 1".into()),
        }];
        ev.outputs[0].tags = health_tags("degraded", Some("provisional"));
        let v = run_event("pumper", &ev).unwrap();
        let a = &v["outputs"][0]["facets"]["dataQualityAssertions"]["assertions"][0];
        assert_eq!(a["assertion"], "pumper.data_contract");
        assert_eq!(a["success"], false);
        assert_eq!(a["message"], "row_count 0 < min 1");
        let tags = v["outputs"][0]["facets"]["tags"]["tags"]
            .as_array()
            .unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(
            tags[0],
            json!({"key":"health","value":"degraded","source":"pumper"})
        );
        assert_eq!(
            tags[1],
            json!({"key":"trust","value":"provisional","source":"pumper"})
        );
    }

    #[test]
    fn health_tags_stamp_healthy_too_because_looked_and_fine_is_a_fact() {
        assert_eq!(health_tags("healthy", None), vec!["health:healthy"]);
    }

    #[test]
    fn every_facet_carries_the_producer_and_schema_url_a_strict_receiver_demands() {
        let mut ev = event(RunOutcome::Fail);
        ev.outputs[0].assertions = vec![Assertion {
            name: "x".into(),
            passed: true,
            message: None,
        }];
        ev.outputs[0].tags = vec!["health:healthy".into()];
        let v = run_event("pumper", &ev).unwrap();
        let mut checked = 0;
        for bucket in [
            &v["run"]["facets"],
            &v["job"]["facets"],
            &v["outputs"][0]["facets"],
            &v["outputs"][0]["outputFacets"],
        ] {
            for (_, f) in bucket.as_object().unwrap() {
                assert!(f.get("_producer").is_some(), "missing _producer in {f}");
                assert!(f.get("_schemaURL").is_some(), "missing _schemaURL in {f}");
                checked += 1;
            }
        }
        assert!(checked >= 6, "expected several facets, checked {checked}");
    }

    #[test]
    fn outcome_labels_are_the_openlineage_vocabulary() {
        assert_eq!(RunOutcome::Start.as_openlineage(), "START");
        assert_eq!(RunOutcome::Complete.as_openlineage(), "COMPLETE");
        assert_eq!(RunOutcome::Fail.as_openlineage(), "FAIL");
    }

    #[test]
    fn a_touched_dataset_is_one_this_run_actually_wrote() {
        assert!(!OutputStats::default().touched());
        assert!(OutputStats {
            new: 0,
            changed: 0,
            removed: 1
        }
        .touched());
    }
}
