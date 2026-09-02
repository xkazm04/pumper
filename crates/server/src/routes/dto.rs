//! Typed response envelopes for the OpenAPI document (N23).
//!
//! # Why these are schema-only
//!
//! Nearly every handler in this tree returns `Json<Value>` built from a `json!`
//! literal, and its shape was documented in a backtick string inside the
//! `#[utoipa::path]` annotation — prose a generator cannot read. So the spec
//! described *paths* precisely and *payloads* not at all, and every consumer
//! (`@pumper/sync`, the MCP tool definitions, any future product) had to
//! hand-mirror the shapes and re-mirror them by hand whenever they drifted.
//!
//! The structs below are the missing half of that contract. They are
//! **declarations, not the code path**: no handler constructs one, nothing here
//! runs at request time, and adding them changed no byte of any response. They
//! exist so `body = <Dto>` on a `responses(...)` entry lands a
//! `#/components/schemas/<Dto>` reference in the document, which is what the
//! client generators read. `spec_schema_tests` in `mod.rs` is the fence: every
//! 2xx response must reference a component schema or name itself in that
//! module's `SCHEMALESS_RESPONSES` allowlist, so a new route cannot join the
//! surface untyped and silent.
//!
//! # Rules these follow
//!
//! - **Describe what is served, not what would be nicer.** Where a handler is
//!   dual-mode (a bare array or a legacy envelope without `cursor`, a keyset
//!   envelope with it), BOTH shapes are typed and the operation declares the
//!   union — tightening the legacy shape would break consumers, which is
//!   exactly what this item was forbidden to do.
//! - **Timestamps are `String`.** Every one of them is RFC 3339 on the wire;
//!   modelling them as `chrono` types here would buy nothing and would make
//!   these mirrors look like the live structs, which they deliberately are not.
//! - **`Value` where the payload genuinely is free-form** — a record's `data`,
//!   an app's `params`, a plugin's self-declared manifest, a store-instrument
//!   report. Typing those would be fabricating a contract the server does not
//!   enforce. Each such field says so.
//! - **`Option<T>` means the key can be `null`**, which for a `json!` literal is
//!   nearly always the case: `json!` never omits a key, it writes `null`. A key
//!   that is genuinely *absent* is called out in its doc comment.
//!
//! Mirrors of `pumper_core` types (`Record`, `Job`, `Trigger`, …) live here
//! rather than as a `ToSchema` derive on the core struct on purpose: N23's file
//! scope is the server's HTTP surface, and the wire shape of a route is a
//! property of the route, not of the storage model it happens to reuse today.

// Every struct here is a DECLARATION, never a value: the schema is read off the
// type by `ToSchema`, and no code path constructs one. rustc counts only
// literals as construction, so without this the whole module reads as dead —
// which is exactly the warning you would want if these were meant to be built.
#![allow(dead_code)]

use serde::Serialize;
use serde_json::Value;
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Shared shapes
// ---------------------------------------------------------------------------

/// The error envelope every 4xx/5xx carries (`routes::error::ApiError`).
///
/// `code` is the stable machine token clients branch on; `error` is the human
/// sentence, which is NOT stable and must never be parsed.
#[derive(Serialize, ToSchema)]
pub(crate) struct ErrorEnvelope {
    /// Human-readable message. Not a contract — do not match on it.
    pub error: String,
    /// Stable code: `bad_request`, `unauthorized`, `budget_exhausted`,
    /// `forbidden`, `not_found`, `conflict`, `too_large`, `unprocessable`,
    /// `confirmation_required`, `rate_limited`, `bad_gateway`, `unavailable`,
    /// `internal`.
    pub code: String,
}

/// `{deleted: true}` — the answer every idempotent delete door gives.
#[derive(Serialize, ToSchema)]
pub(crate) struct DeletedResponse {
    pub deleted: bool,
}

/// `{id, enabled}` — the answer every `POST .../enabled` toggle gives.
#[derive(Serialize, ToSchema)]
pub(crate) struct EnabledResponse {
    pub id: String,
    pub enabled: bool,
}

/// One stored record (`pumper_core::datasets::Record`).
#[derive(Serialize, ToSchema)]
pub(crate) struct RecordDto {
    pub key: String,
    /// The canonical payload. App-defined, so genuinely free-form.
    #[schema(value_type = Object)]
    pub data: Value,
    pub first_seen: String,
    pub last_seen: String,
    pub updated_at: String,
    /// Set once a full-snapshot sync stopped containing this key; else null.
    pub removed_at: Option<String>,
    /// `stable` | `provisional` | `quarantined`.
    pub trust: String,
}

/// A keyset page of records (`?cursor=` mode of `GET /datasets/{app}/{ds}`).
#[derive(Serialize, ToSchema)]
pub(crate) struct RecordPage {
    pub items: Vec<RecordDto>,
    /// `null` when the page came back short — you are caught up.
    pub next_cursor: Option<String>,
}

/// One entry of the change feed (`pumper_core::datasets::Revision`), with the
/// `Provenance` block flattened onto it exactly as the server serializes it.
#[derive(Serialize, ToSchema)]
pub(crate) struct RevisionDto {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub revision: i64,
    /// `new` | `changed` | `removed`. The feed never emits `unchanged`.
    pub change: String,
    /// Full post-image for new/changed; null for removed.
    #[schema(value_type = Option<Object>)]
    pub data: Option<Value>,
    /// `{"$.path": {"from": …, "to": …}}`, or null.
    #[schema(value_type = Option<Object>)]
    pub diff: Option<Value>,
    pub created_at: String,
    pub trust: String,
    /// Provenance, flattened. Every field is honest-Null: `null` means UNKNOWN,
    /// never a fabricated value, and all four keys are always present.
    pub job_id: Option<String>,
    pub source_url: Option<String>,
    pub artifact_sha: Option<String>,
    pub rules_hash: Option<String>,
}

/// A keyset page of the change feed.
#[derive(Serialize, ToSchema)]
pub(crate) struct RevisionPageDto {
    pub items: Vec<RevisionDto>,
    pub next_cursor: Option<String>,
}

/// One queued/running/finished job (`pumper_core::Job`). `callback_secret` and
/// `resumed_input` are `skip_serializing` on the core struct and are absent from
/// the wire, so they are absent here.
#[derive(Serialize, ToSchema)]
pub(crate) struct JobDto {
    pub id: String,
    pub app: String,
    /// Merged params, app-defined.
    #[schema(value_type = Object)]
    pub params: Value,
    /// `queued` | `running` | `waiting` | `succeeded` | `failed` | `cancelled`.
    pub status: String,
    pub attempts: i64,
    pub max_attempts: i64,
    pub priority: i64,
    pub callback_url: Option<String>,
    pub budget_usd: Option<f64>,
    pub schedule_id: Option<String>,
    pub trigger_id: Option<String>,
    /// The app's `RunReport`, app-defined.
    #[schema(value_type = Option<Object>)]
    pub result: Option<Value>,
    pub error: Option<String>,
    /// What a `waiting` job is waiting for (N02).
    #[schema(value_type = Option<Object>)]
    pub input_request: Option<Value>,
    pub waiting_since: Option<String>,
    pub waiting_expires_at: Option<String>,
    pub executor_id: Option<String>,
    pub created_at: String,
    pub available_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

/// `GET /jobs/{id}`: a job plus, for a running one, its live progress snapshot.
#[derive(Serialize, ToSchema)]
pub(crate) struct JobDetail {
    #[serde(flatten)]
    pub job: JobDto,
    /// ABSENT (not null) unless the job is running and the worker has published
    /// a snapshot. In-memory only — it does not survive a restart.
    #[schema(value_type = Option<Object>)]
    pub progress: Option<Value>,
}

/// `GET /jobs` without `cursor` is a bare `[Job]`; with it, this envelope.
#[derive(Serialize, ToSchema)]
pub(crate) struct JobPage {
    pub items: Vec<JobDto>,
    pub next_cursor: Option<String>,
}

/// One metered engine call (`pumper_core::CostEvent`).
#[derive(Serialize, ToSchema)]
pub(crate) struct CostEventDto {
    pub job_id: String,
    pub app: String,
    pub engine: String,
    pub url: Option<String>,
    pub cost_usd: f64,
    pub detail: Option<String>,
    pub created_at: String,
}

/// One delivery attempt of a webhook / watch / subscription sink.
#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryDto {
    pub id: String,
    pub kind: String,
    pub ref_id: String,
    pub url: String,
    pub event: String,
    /// ABSENT on list rows (those queries select an empty body, which the core
    /// struct skips); populated on `GET /webhooks/deliveries/{id}`.
    pub body: Option<String>,
    pub status: String,
    pub attempts: i64,
    pub last_error: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// A keyset page of deliveries.
#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryPage {
    pub items: Vec<DeliveryDto>,
    pub next_cursor: Option<String>,
}

/// One dataset's freshness counters, as an app's `RunReport` reports them.
#[derive(Serialize, ToSchema)]
pub(crate) struct YieldEntryDto {
    pub dataset: String,
    pub new: Option<i64>,
    pub changed: Option<i64>,
    pub unchanged: Option<i64>,
    pub removed: Option<i64>,
}

// ---------------------------------------------------------------------------
// jobs.rs
// ---------------------------------------------------------------------------

/// `POST /jobs/retry` — the bulk re-queue door.
#[derive(Serialize, ToSchema)]
pub(crate) struct BulkRetryResponse {
    pub retried: i64,
    pub ids: Vec<String>,
}

/// `DELETE /jobs/{id}`.
///
/// Three shapes share this schema, which is why everything but `cancelled` is
/// optional: a queued job answers `{cancelled: true}`; a running one adds
/// `running: true`; and a job that lost the race with a graceful shutdown —
/// it had already committed to a checkpoint suspend — answers
/// `{cancelled: false, running: true, suspended: true, note}`. That last one is
/// not a cancellation at all, and saying so honestly is the point.
#[derive(Serialize, ToSchema)]
pub(crate) struct CancelJobResponse {
    pub cancelled: bool,
    /// ABSENT unless the job was in flight.
    pub running: Option<bool>,
    /// ABSENT unless the job was re-queued by the shutdown drain instead.
    pub suspended: Option<bool>,
    /// ABSENT unless `suspended`; explains what happened instead.
    pub note: Option<String>,
}

/// `GET /jobs/{id}/costs`.
#[derive(Serialize, ToSchema)]
pub(crate) struct JobCostsResponse {
    pub job_id: String,
    pub app: String,
    pub total_usd: f64,
    pub calls: i64,
    /// `null` when the job has no result yet — an unknown yield, never a
    /// fabricated zero.
    pub fresh_records: Option<i64>,
    /// `null` unless `fresh_records > 0`.
    pub cost_per_fresh_record_usd: Option<f64>,
    pub events: Vec<CostEventDto>,
}

/// One `(app, engine)` row of the spend ledger.
#[derive(Serialize, ToSchema)]
pub(crate) struct CostSummaryRow {
    pub app: String,
    pub engine: String,
    pub calls: i64,
    pub cost_usd: f64,
}

/// `GET /costs`.
#[derive(Serialize, ToSchema)]
pub(crate) struct CostSummaryResponse {
    pub total_usd: f64,
    /// Echo of `?principal=`; `null` when the ledger was not restricted.
    pub principal: Option<String>,
    pub by_app_engine: Vec<CostSummaryRow>,
}

// ---------------------------------------------------------------------------
// schedules.rs
// ---------------------------------------------------------------------------

/// One cron schedule, enriched with the four derived keys `GET /schedules`
/// adds (`POST /schedules` returns the bare row, so those four are optional).
#[derive(Serialize, ToSchema)]
pub(crate) struct ScheduleDto {
    pub id: String,
    pub app: String,
    pub cron: String,
    #[schema(value_type = Object)]
    pub params: Value,
    pub enabled: bool,
    pub priority: i64,
    pub timezone: Option<String>,
    pub misfire_policy: String,
    pub max_attempts: Option<i64>,
    pub budget_usd: Option<f64>,
    pub managed_by: Option<String>,
    pub last_run: Option<String>,
    pub last_skipped_at: Option<String>,
    pub skipped_count: i64,
    pub created_at: String,
    /// Listing only. `null` when the cron is unparseable or exhausted.
    pub next_run: Option<String>,
    /// Listing only.
    pub last_job_id: Option<String>,
    /// Listing only.
    pub last_status: Option<String>,
    /// Listing only: `ok` | `disabled` | `invalid_cron` | `unregistered_app` |
    /// `invalid_params` | `overlapping`.
    pub health: Option<String>,
}

/// Keyset page of schedules.
#[derive(Serialize, ToSchema)]
pub(crate) struct SchedulePage {
    pub items: Vec<ScheduleDto>,
    pub next_cursor: Option<String>,
}

/// `POST /schedules/{id}/budget`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ScheduleBudgetResponse {
    pub id: String,
    /// `null` when the ceiling was cleared.
    pub budget_usd: Option<f64>,
}

// ---------------------------------------------------------------------------
// receipt.rs
// ---------------------------------------------------------------------------

/// The job block of a receipt — a deliberate subset of `Job` plus `wall_ms`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptJob {
    pub id: String,
    pub app: String,
    pub status: String,
    pub attempts: i64,
    pub max_attempts: i64,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    /// `null` unless the job both started and finished.
    pub wall_ms: Option<i64>,
    pub schedule_id: Option<String>,
    pub trigger_id: Option<String>,
    pub executor_id: Option<String>,
    pub error: Option<String>,
}

/// Per-stage timings for one attempt.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptStages {
    pub attempt: i64,
    pub run_ms: Option<i64>,
    pub index_ms: Option<i64>,
    pub hooks_ms: Option<i64>,
    pub alerts_ms: Option<i64>,
    pub total_ms: Option<i64>,
}

/// Spend by engine within one job.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptEngineCost {
    pub engine: String,
    pub calls: i64,
    pub cost_usd: f64,
}

/// Fetches this job pushed out through a mesh peer.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptEgress {
    pub node: String,
    pub calls: i64,
}

/// The cost block of a receipt.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptCost {
    pub total_usd: f64,
    pub calls: i64,
    pub budget_usd: Option<f64>,
    pub by_engine: Vec<ReceiptEngineCost>,
    pub egress: Vec<ReceiptEgress>,
    /// Fetches the research subprocess made back through this node's own MCP
    /// `fetch` tool (N15).
    pub self_hosted_fetches: i64,
}

/// What one dataset actually changed during this job.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptChange {
    pub app: String,
    pub dataset: String,
    pub total: i64,
    /// Revision kind (`new` / `changed` / `removed`) to count. A dynamic map,
    /// so it is typed as an object rather than as a fixed field set.
    #[schema(value_type = Object)]
    pub by_change: Value,
}

/// One source's extraction-health verdict for this job.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptHealthVerdict {
    pub source_id: String,
    pub verdict: String,
    pub diagnosis: Option<String>,
    pub score: f64,
    pub state_after: String,
}

/// The verdict block of a receipt.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptVerdicts {
    pub health: Vec<ReceiptHealthVerdict>,
    /// Contract verdicts, as the worker wrote them — `{verdict, violations,
    /// records, removed, enforced, job_id, checked_at}` plus a `source` key the
    /// route injects. Free-form because the worker owns the shape.
    #[schema(value_type = Vec<Object>)]
    pub contracts: Vec<Value>,
}

/// One retained artifact file.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptArtifactFile {
    pub name: String,
    pub bytes: i64,
}

/// The artifacts block of a receipt; `null` when the directory is missing or
/// unreadable — stated rather than reported as an empty run.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptArtifacts {
    pub dir: String,
    pub files: Vec<ReceiptArtifactFile>,
    pub count: i64,
    pub total_bytes: i64,
    /// True when the listing hit its cap: `files` is a prefix, not the whole set.
    pub truncated: bool,
}

/// One downstream job this job's completion triggered.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReceiptTriggerHop {
    pub job_id: String,
    pub app: String,
    pub trigger_id: Option<String>,
    pub status: String,
    pub created_at: String,
}

/// `GET /jobs/{id}/receipt` — one job's whole story.
#[derive(Serialize, ToSchema)]
pub(crate) struct JobReceipt {
    pub job: ReceiptJob,
    /// `null` when no stage row was recorded for this attempt.
    pub stages: Option<ReceiptStages>,
    pub cost: ReceiptCost,
    #[serde(rename = "yield")]
    pub yield_: Vec<YieldEntryDto>,
    pub changes: Vec<ReceiptChange>,
    pub verdicts: ReceiptVerdicts,
    pub artifacts: Option<ReceiptArtifacts>,
    pub deliveries: Vec<DeliveryDto>,
    pub trigger_hops: Vec<ReceiptTriggerHop>,
    /// What this receipt could NOT establish, in prose. Never empty in
    /// practice — the honest-absence half of the contract.
    pub unknown: Vec<String>,
}

// ---------------------------------------------------------------------------
// economics.rs
// ---------------------------------------------------------------------------

/// Claude spend for one app in one window, and whether it paid for itself.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsClaude {
    pub cost_usd: f64,
    pub calls: i64,
    /// `null` together with `worth_it` when there was no Claude spend or the
    /// yield is unknown.
    pub records_per_dollar: Option<f64>,
    pub worth_it: Option<bool>,
}

/// One dataset's yield inside an economics window.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsDataset {
    pub dataset: String,
    pub jobs: i64,
    pub new: Option<i64>,
    pub changed: Option<i64>,
    pub unchanged: Option<i64>,
    pub removed: Option<i64>,
}

/// One app's economics inside one window.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsApp {
    pub app: String,
    pub weight: f64,
    pub jobs_with_yield: i64,
    pub engine_calls: i64,
    pub cost_usd: f64,
    pub new: Option<i64>,
    pub changed: Option<i64>,
    pub unchanged: Option<i64>,
    pub cost_per_new_usd: Option<f64>,
    pub cost_per_changed_usd: Option<f64>,
    pub weighted_fresh_per_dollar: Option<f64>,
    pub claude: EconomicsClaude,
    pub datasets: Vec<EconomicsDataset>,
}

/// One rolling window of the economics report.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsWindow {
    pub days: i64,
    pub apps: Vec<EconomicsApp>,
}

/// The two windows the report always carries.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsWindows {
    #[serde(rename = "7d")]
    pub d7: EconomicsWindow,
    #[serde(rename = "30d")]
    pub d30: EconomicsWindow,
}

/// One budget/cadence recommendation.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsAdvice {
    pub app: String,
    pub weight: f64,
    pub recommended_budget_usd: Option<f64>,
    /// `increase` | `keep` | `decrease`.
    pub cadence: String,
    pub reason: String,
}

/// One caller's spend.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalCostRow {
    /// `null` is the unattributed bucket — spend from before N20 stamped a
    /// principal, or from an `open`-mode operator.
    pub principal_id: Option<String>,
    /// The id, or the literal `(unattributed)`.
    pub principal: String,
    pub calls: i64,
    pub cost_usd: f64,
}

/// The `by_principal` block of `GET /economics`.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsByPrincipal {
    /// Always `30d`.
    pub window: String,
    pub rows: Vec<PrincipalCostRow>,
    pub all_time: Vec<PrincipalCostRow>,
}

/// `GET /economics`.
#[derive(Serialize, ToSchema)]
pub(crate) struct EconomicsReport {
    pub enforce: bool,
    pub windows: EconomicsWindows,
    pub advice: Vec<EconomicsAdvice>,
    pub by_principal: EconomicsByPrincipal,
}

// ---------------------------------------------------------------------------
// datasets.rs
// ---------------------------------------------------------------------------

/// `GET /apps/{name}/datasets`.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppDatasetsResponse {
    pub app: String,
    pub datasets: Vec<String>,
}

/// `DELETE /datasets/{app}/{dataset}` once confirmed.
///
/// The unconfirmed call answers 428 with a preview instead — deleting a dataset
/// is the one irreversible door here, so the confirmation is part of the
/// contract rather than a nicety.
#[derive(Serialize, ToSchema)]
pub(crate) struct DatasetDeletion {
    /// Always `false` on the 200 — the 428 preview is where `true` appears.
    pub preview: bool,
    pub app: String,
    pub dataset: String,
    /// Equal to `records`; kept for the consumers that read it before
    /// `records` existed.
    pub deleted: i64,
    pub records: i64,
    pub revisions: i64,
    /// Where the pre-delete NDJSON snapshot was written.
    pub export: String,
    pub as_of: String,
}

/// One near-duplicate pair and its SimHash Hamming distance.
#[derive(Serialize, ToSchema)]
pub(crate) struct DupPairDto {
    pub a: String,
    pub b: String,
    pub distance: i64,
}

/// `GET /datasets/{app}/{dataset}/duplicates`.
#[derive(Serialize, ToSchema)]
pub(crate) struct DuplicatesResponse {
    pub app: String,
    pub dataset: String,
    /// The CLAMPED distance actually used, which may be below what was asked.
    pub max_distance: i64,
    pub pairs: Vec<DupPairDto>,
}

/// `GET /datasets/{app}/{dataset}/changes` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct DatasetChangesResponse {
    pub app: String,
    pub dataset: String,
    pub count: i64,
    /// Echo of `?trust=` — which trust levels this feed was restricted to.
    pub trust: String,
    pub changes: Vec<RevisionDto>,
}

/// `GET /datasets/{app}/{dataset}/history` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct RecordHistoryResponse {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub count: i64,
    pub revisions: Vec<RevisionDto>,
}

/// `GET /datasets/{app}/{dataset}/manifest` — the digest a mesh peer reconciles
/// against.
#[derive(Serialize, ToSchema)]
pub(crate) struct DatasetManifest {
    pub app: String,
    pub dataset: String,
    /// Every row, tombstones included.
    pub count: i64,
    /// Live keys only — the set the digest covers.
    pub live_count: i64,
    /// SHA-256 over the sorted live keys.
    pub digest: String,
    /// `false` when the dataset is larger than `cap`: the digest then covers a
    /// prefix, and a peer must not treat a mismatch as a ghost.
    pub complete: bool,
    pub cap: i64,
    /// ABSENT unless `?keys=true`.
    pub keys: Option<Vec<String>>,
}

// ---------------------------------------------------------------------------
// derived.rs
// ---------------------------------------------------------------------------

/// The `lookup` clause of a derived spec.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedLookupDto {
    pub dataset: String,
    pub key_expr: String,
    pub merge_as: String,
}

/// The `group` clause of a derived spec.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedGroupDto {
    pub group_by: Vec<String>,
    /// `output field` to aggregate expression.
    #[schema(value_type = Object)]
    pub aggregates: Value,
}

/// One derived-dataset spec.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedSpecDto {
    pub id: String,
    pub source_app: String,
    pub source_dataset: String,
    pub target_dataset: String,
    pub filters: Vec<String>,
    /// `output field` to source JSON path.
    #[schema(value_type = Object)]
    pub project: Value,
    pub lookup: Option<DerivedLookupDto>,
    pub group: Option<DerivedGroupDto>,
    pub enabled: bool,
    pub created_at: String,
}

/// `GET /derived`.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedListResponse {
    pub specs: Vec<DerivedSpecDto>,
}

/// `DELETE /derived/{id}` — answers with the id, not a bool, unlike the other
/// delete doors. Kept as served.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedDeleted {
    pub deleted: String,
}

/// `POST /derived/{id}/backfill` — one bounded pass.
#[derive(Serialize, ToSchema)]
pub(crate) struct DerivedBackfillResponse {
    pub scanned: i64,
    pub matched: i64,
    pub new: i64,
    pub changed: i64,
    pub unchanged: i64,
    /// `false` means call again with `cursor`.
    pub done: bool,
    /// ABSENT when `done` — the resume token for the next pass.
    pub cursor: Option<String>,
}

// ---------------------------------------------------------------------------
// provenance.rs
// ---------------------------------------------------------------------------

/// How much of a record's history can be replayed.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProvenanceCoverage {
    pub revisions: i64,
    pub with_job: i64,
    /// Revisions holding BOTH an archived body and a rules hash — the ones
    /// `rederive` can actually re-run.
    pub replayable: i64,
}

/// The derivation stamp on one revision.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProvenanceStamp {
    pub job_id: Option<String>,
    pub source_url: Option<String>,
    pub artifact_sha: Option<String>,
    pub rules_hash: Option<String>,
    /// Derived: both `artifact_sha` and `rules_hash` are present.
    pub replayable: bool,
}

/// The job that wrote one revision; `null` when the id is unknown or unparseable.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProvenanceJob {
    pub app: String,
    pub status: String,
    pub schedule_id: Option<String>,
    pub trigger_id: Option<String>,
    pub created_at: String,
}

/// One link of a record's derivation chain.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProvenanceLink {
    pub revision: i64,
    pub change: String,
    pub created_at: String,
    pub trust: String,
    pub provenance: ProvenanceStamp,
    pub job: Option<ProvenanceJob>,
}

/// `GET /provenance/{app}/{dataset}/{key}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProvenanceResponse {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub trust: String,
    pub removed_at: Option<String>,
    pub coverage: ProvenanceCoverage,
    pub chain: Vec<ProvenanceLink>,
}

/// `POST /provenance/{app}/{dataset}/{key}/rederive` — a read-only replay of the
/// archived body through the recorded rules.
#[derive(Serialize, ToSchema)]
pub(crate) struct RederiveResponse {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub revision: i64,
    pub artifact_sha: String,
    pub rules_hash: String,
    /// Top-level `_`-prefixed keys stripped before comparing.
    pub ignored_meta_fields: Vec<String>,
    /// `reproduced` | `diverged`.
    pub verdict: String,
    /// ABSENT when reproduced; `{"$.path": {"from": …, "to": …}}` otherwise.
    #[schema(value_type = Option<Object>)]
    pub diff: Option<Value>,
}

// ---------------------------------------------------------------------------
// doctor.rs
// ---------------------------------------------------------------------------

/// One store-integrity finding.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorFinding {
    pub check: String,
    /// `warn` | `info`. The doctor never blocks, so there is no `error`.
    pub severity: String,
    pub summary: String,
    pub count: i64,
    pub remediation: String,
    /// A handful of offending rows, shape depending on the check.
    #[schema(value_type = Vec<Object>)]
    pub examples: Vec<Value>,
}

/// Retained artifact bytes for one app.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppReclaimDto {
    pub app: String,
    pub files: i64,
    pub bytes: i64,
    pub reclaimable_files: i64,
    pub reclaimable_bytes: i64,
    pub pinned_files: i64,
    pub pinned_bytes: i64,
    pub cassette_files: i64,
    pub cassette_bytes: i64,
    pub within_window_files: i64,
    pub within_window_bytes: i64,
}

/// The doctor's artifact scan; `{scanned: false}` and nothing else when
/// `?skip_artifacts=true`, so every other field is optional.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorArtifacts {
    pub scanned: bool,
    pub root: Option<String>,
    pub bodies_checked: Option<i64>,
    pub check_limit: Option<i64>,
    pub per_app: Option<Vec<AppReclaimDto>>,
    pub total_bytes: Option<i64>,
}

/// Whether the search index and the store agree about how much exists.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorSearch {
    pub enabled: bool,
    /// `null` when the index could not be read — unknown, not zero.
    pub doc_count: Option<i64>,
    pub live_records: i64,
}

/// One append-only table and the retention that is (or is not) bounding it.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorTable {
    pub table: String,
    pub rows: i64,
    pub oldest_days: Option<i64>,
    pub retention_days: i64,
    pub config_key: String,
}

/// Provenance coverage for one dataset.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorCoverage {
    pub app: String,
    pub dataset: String,
    pub revisions: i64,
    pub with_job_id: i64,
    pub replayable: i64,
}

/// The doctor's `thresholds` block: the constants its verdicts were judged by,
/// so a reader can tell a finding from a policy.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorThresholds {
    pub unbounded_growth_days: i64,
}

/// `GET /datasets/doctor` — read-only store integrity.
#[derive(Serialize, ToSchema)]
pub(crate) struct DoctorReport {
    /// Always `true`. This endpoint deletes nothing, ever.
    pub read_only: bool,
    pub generated_at: String,
    pub healthy: bool,
    pub findings: Vec<DoctorFinding>,
    pub artifacts: DoctorArtifacts,
    pub search: Option<DoctorSearch>,
    pub tables: Vec<DoctorTable>,
    pub coverage: Vec<DoctorCoverage>,
    /// The SQLite store instrument's own report — latency percentiles per
    /// operation, file sizes, pool saturation and the maintenance ledger. Its
    /// shape is owned by `pumper_core::store_instrument` and changes with the
    /// instrument, so it is deliberately not pinned here.
    #[schema(value_type = Object)]
    pub store: Value,
    pub thresholds: DoctorThresholds,
}

// ---------------------------------------------------------------------------
// retention.rs
// ---------------------------------------------------------------------------

/// The artifact half of the retention dry run.
#[derive(Serialize, ToSchema)]
pub(crate) struct RetentionArtifacts {
    pub root: String,
    pub cutoff_days: i64,
    pub enabled: bool,
    pub cassettes_protected: bool,
    pub total_files: i64,
    pub total_bytes: i64,
    pub reclaimable_files: i64,
    pub reclaimable_bytes: i64,
    pub pinned_files: i64,
    pub pinned_bytes: i64,
    pub cassette_files: i64,
    pub cassette_bytes: i64,
    pub within_window_files: i64,
    pub within_window_bytes: i64,
    pub per_app: Vec<AppReclaimDto>,
}

/// One append-only ledger's current size.
#[derive(Serialize, ToSchema)]
pub(crate) struct RetentionLedger {
    pub table: String,
    pub rows: i64,
}

/// The retention windows currently configured, in days.
#[derive(Serialize, ToSchema)]
pub(crate) struct RetentionConfig {
    pub revision_retention_days: i64,
    pub artifact_retention_days: i64,
    pub cost_event_retention_days: i64,
    pub webhook_delivery_retention_days: i64,
    pub webhook_dead_letter_retention_days: i64,
    pub job_yield_retention_days: i64,
    pub saved_search_seen_retention_days: i64,
}

/// `GET /retention/preview` — what the janitor WOULD reclaim. Deletes nothing.
#[derive(Serialize, ToSchema)]
pub(crate) struct RetentionPreview {
    /// Always `true`.
    pub dry_run: bool,
    pub artifacts: RetentionArtifacts,
    pub ledgers: Vec<RetentionLedger>,
    pub config: RetentionConfig,
}

// ---------------------------------------------------------------------------
// triggers.rs
// ---------------------------------------------------------------------------

/// One reactive edge: a source event kind to a target app.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerDto {
    pub id: String,
    pub name: Option<String>,
    /// `dataset` | `job` | `external`.
    pub source_kind: String,
    pub source_app: String,
    pub source_dataset: Option<String>,
    pub on_change: Option<String>,
    pub on_status: Option<String>,
    pub target_app: String,
    #[schema(value_type = Object)]
    pub params: Value,
    pub budget_usd: Option<f64>,
    pub priority: i64,
    pub max_attempts: i64,
    pub enabled: bool,
    pub created_at: String,
    /// ABSENT when the trigger declares none.
    pub filters: Option<Vec<String>>,
    /// ABSENT when none. `{predicate?, transform?, post_enqueue?}`, each a
    /// `{plugin, config?}` hook.
    #[schema(value_type = Option<Object>)]
    pub plugin_hooks: Option<Value>,
    /// ABSENT when none. Target param name to JSON pointer into the event (N04).
    #[schema(value_type = Option<Object>)]
    pub bind: Option<Value>,
    /// ABSENT when none. JSON pointer to the array this trigger fans out over.
    #[serde(rename = "each")]
    pub each_path: Option<String>,
}

/// `GET /triggers` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerListResponse {
    pub triggers: Vec<TriggerDto>,
}

/// `GET /triggers` with `cursor`.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerPage {
    pub items: Vec<TriggerDto>,
    pub next_cursor: Option<String>,
}

/// One plugin-hook incident on a hop: what the plugin did instead of deciding.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerHookIncident {
    /// `predicate` | `transform` | `post_enqueue`.
    pub slot: String,
    pub plugin: String,
    pub outcome: String,
    pub detail: String,
}

/// The hook block of a dry run: which configured plugins could not be used at
/// all, and what the ones that ran did. A hop whose gate plugin is missing
/// fails OPEN, so naming the unusable ones is the only way an operator learns
/// the gate they deployed is not gating.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerHooks {
    pub unusable_plugins: Vec<String>,
    pub incidents: Vec<TriggerHookIncident>,
}

/// The fan-out plan of a dry run (N04's `each`).
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerFanOut {
    /// The JSON pointer fanned out over.
    #[serde(rename = "each")]
    pub each_path: String,
    /// Hops this fan-out would actually enqueue (after the cap).
    pub hops: i64,
    /// Elements found before the cap; `null` when not computed.
    pub total: Option<i64>,
    /// `true` when `total` exceeded `cap` — the truncation is stated, never
    /// silent.
    pub truncated: Option<bool>,
    pub cap: i64,
}

/// `POST /triggers/{id}/test`.
///
/// One schema, six served shapes — a dry run that would not fire, a dry run
/// that would, and a real `?fire=true` — because a caller has to branch on
/// `would_fire` / `fired` anyway and splitting them into six components would
/// make the union harder to consume, not easier.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerTestResponse {
    /// Present on every DRY RUN. Absent on `?fire=true`.
    pub would_fire: Option<bool>,
    /// Why not, in prose. Present whenever `would_fire` is false.
    pub reason: Option<String>,
    /// `bind_miss` | `fan_out_empty` — the ledger outcome this hop would record.
    pub outcome: Option<String>,
    pub hooks: Option<TriggerHooks>,
    /// Dry run that would fire: the app that would be enqueued.
    pub target_app: Option<String>,
    pub source_job_id: Option<String>,
    /// The params the hop would carry, after defaults, binding and hooks.
    #[schema(value_type = Option<Object>)]
    pub resolved_params: Option<Value>,
    /// Which params came from `bind`; `null` when the trigger has no `bind`.
    pub bound_params: Option<Vec<String>>,
    /// `null` unless the trigger declares `each`.
    pub fan_out: Option<TriggerFanOut>,
    /// `?fire=true` only.
    pub fired: Option<bool>,
    /// `?fire=true` only: the first hop.
    pub job: Option<JobDto>,
    /// `?fire=true` only: every hop.
    pub jobs: Option<Vec<JobDto>>,
}

/// One row of the trigger decision ledger — why a hop did or did not happen.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerRunDto {
    pub id: String,
    pub trigger_id: String,
    pub outcome: String,
    pub source_kind: String,
    pub source_job_id: Option<String>,
    pub dataset: Option<String>,
    pub event_id: Option<String>,
    pub job_id: Option<String>,
    pub detail: Option<String>,
    pub created_at: String,
}

/// `GET /triggers/{id}/runs` — the hops AND the decisions, because a trigger
/// that never fired is the case you are usually debugging.
#[derive(Serialize, ToSchema)]
pub(crate) struct TriggerRunsResponse {
    pub trigger_id: String,
    pub count: i64,
    pub runs: Vec<JobDto>,
    pub decisions: Vec<TriggerRunDto>,
    pub next_cursor: Option<String>,
}

/// `GET /webhooks/deliveries` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryListResponse {
    pub count: i64,
    pub deliveries: Vec<DeliveryDto>,
}

/// `POST /webhooks/deliveries/{id}/replay`.
#[derive(Serialize, ToSchema)]
pub(crate) struct DeliveryReplayResponse {
    pub id: String,
    pub replaying: bool,
}

// ---------------------------------------------------------------------------
// watches.rs
// ---------------------------------------------------------------------------

/// The last delivery a watch made; `null` (never omitted) when it has never
/// delivered, so "never fired" and "fired and failed" cannot be confused.
#[derive(Serialize, ToSchema)]
pub(crate) struct WatchLastDelivery {
    pub id: String,
    pub status: String,
    pub at: String,
}

/// One dataset-change webhook. `secret` is never serialized.
#[derive(Serialize, ToSchema)]
pub(crate) struct WatchDto {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    /// `webhook` | `slack` | `file` | `plugin:<name>`.
    pub sink: String,
    pub enabled: bool,
    pub cursor_seq: i64,
    pub created_at: String,
    /// Listing only; `null` when the watch has never delivered.
    pub last_delivery: Option<WatchLastDelivery>,
}

/// `GET /watches` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct WatchListResponse {
    pub watches: Vec<WatchDto>,
}

/// `GET /watches` with `cursor`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WatchPage {
    pub items: Vec<WatchDto>,
    pub next_cursor: Option<String>,
}

/// `GET /watches/{id}/deliveries` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct WatchDeliveriesResponse {
    pub watch_id: String,
    pub count: i64,
    pub deliveries: Vec<DeliveryDto>,
}

// ---------------------------------------------------------------------------
// subscriptions.rs
// ---------------------------------------------------------------------------

/// One cursor subscription over the durable event log (N05). `secret` is never
/// serialized.
#[derive(Serialize, ToSchema)]
pub(crate) struct SubscriptionDto {
    pub id: String,
    pub name: Option<String>,
    /// Which events this subscription wants.
    #[schema(value_type = Object)]
    pub selector: Value,
    pub sink: String,
    pub url: String,
    /// How far this subscription has been ACKNOWLEDGED — advanced only on a
    /// delivered event, never on an attempted one.
    pub cursor_seq: i64,
    pub enabled: bool,
    pub principal_id: Option<String>,
    pub created_at: String,
    pub last_delivered_at: Option<String>,
    pub last_error: Option<String>,
}

/// `GET /subscriptions`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SubscriptionListResponse {
    pub count: i64,
    /// The log's head, so `latest_seq - cursor_seq` is a subscription's backlog.
    pub latest_seq: i64,
    pub subscriptions: Vec<SubscriptionDto>,
}

/// `GET /subscriptions/{id}/deliveries` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct SubscriptionDeliveriesResponse {
    pub subscription_id: String,
    pub cursor_seq: i64,
    pub latest_seq: i64,
    pub last_error: Option<String>,
    pub count: i64,
    pub deliveries: Vec<DeliveryDto>,
}

// ---------------------------------------------------------------------------
// events.rs
// ---------------------------------------------------------------------------

/// One row of the durable event log.
#[derive(Serialize, ToSchema)]
pub(crate) struct EventRecordDto {
    /// Monotonic sequence — the cursor. Survives a restart, and is the same
    /// number `Last-Event-ID` carries on the SSE streams.
    pub seq: i64,
    /// `job.succeeded` | `dataset.changed` | `external` |
    /// `transaction.submitted` | `source.repair_promoted` | …
    pub kind: String,
    pub app: String,
    /// Job id, dataset name, transaction id — whatever this kind is about.
    pub subject_id: String,
    #[schema(value_type = Object)]
    pub payload: Value,
    pub created_at: String,
}

/// `GET /events/log` — the pull page of the event log.
#[derive(Serialize, ToSchema)]
pub(crate) struct EventLogPage {
    pub count: i64,
    /// Send as `after` next time; `null` when the page did not fill, i.e. you
    /// are caught up and should back off rather than spin.
    pub next_after: Option<i64>,
    /// The log's head, so `latest_seq - after` is your backlog.
    pub latest_seq: i64,
    /// Rows still retained — the log is pruned past `[events]
    /// log_retention_days`, so this is not the number ever written.
    pub retained: i64,
    /// Events buffered but not yet drained to their sinks.
    pub pending: i64,
    pub retention_days: i64,
    pub events: Vec<EventRecordDto>,
}

// ---------------------------------------------------------------------------
// ingress.rs
// ---------------------------------------------------------------------------

/// One inbound webhook source. `secret` is returned exactly once, at creation.
#[derive(Serialize, ToSchema)]
pub(crate) struct IngressSourceDto {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub created_at: String,
}

/// `GET /ingress/sources`.
#[derive(Serialize, ToSchema)]
pub(crate) struct IngressSourceListResponse {
    pub count: i64,
    pub sources: Vec<IngressSourceDto>,
}

/// `POST /ingress/sources` — the only response that ever carries the secret.
#[derive(Serialize, ToSchema)]
pub(crate) struct IngressSourceCreated {
    pub source: IngressSourceDto,
    /// Shown ONCE. It is stored hashed and can never be read back.
    pub secret: String,
}

/// `POST /ingest/{id}` — one accepted external event.
#[derive(Serialize, ToSchema)]
pub(crate) struct IngestResponse {
    pub event_id: String,
    /// The log sequence this event landed at.
    pub seq: i64,
    pub triggers_fired: i64,
}

// ---------------------------------------------------------------------------
// meta.rs
// ---------------------------------------------------------------------------

/// `GET /health`.
#[derive(Serialize, ToSchema)]
pub(crate) struct HealthResponse {
    /// Always `ok` — the endpoint answering at all IS the liveness signal.
    pub status: String,
}

/// One registered app.
///
/// Compiled-in apps and discovered WASM apps (N09) share this listing, and the
/// two carry different keys: everything a dynamic app does not have is
/// `Option`, and `runnable: false` with a `reason` is how a discovered module
/// this build cannot execute says so instead of vanishing from the list.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppEntry {
    pub name: String,
    pub description: String,
    pub schedule: Option<String>,
    /// What this app needs before it can run, e.g. `env:CENSUS_API_KEY`.
    pub requires: Vec<String>,
    /// Every requirement is satisfied on this node.
    pub ready: bool,
    /// Compiled-in apps only.
    #[schema(value_type = Option<Object>)]
    pub default_params: Option<Value>,
    /// Compiled-in apps only: `free` | `metered` | `claude`.
    pub cost_class: Option<String>,
    /// Compiled-in apps only.
    pub output_shape: Option<String>,
    pub has_params_schema: bool,
    /// WASM apps only: `true`.
    pub dynamic: Option<bool>,
    /// WASM apps only. `false` means listed but not executable here; enqueueing
    /// it answers 409 with `reason`.
    pub runnable: Option<bool>,
    /// WASM apps only, and only when not runnable.
    pub reason: Option<String>,
    /// WASM apps only: the component world it exports.
    pub world: Option<String>,
    /// WASM apps only: the module digest, pinned in the catalog.
    pub module_sha256: Option<String>,
    /// WASM apps only: the catalog pins this digest.
    pub pinned: Option<bool>,
    /// WASM apps only.
    #[schema(value_type = Option<Object>)]
    pub params_schema: Option<Value>,
}

/// One worked example an agent can copy.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppToolExample {
    pub description: String,
    #[schema(value_type = Object)]
    pub params: Value,
}

/// One app as an agent-facing tool definition (`GET /apps?format=tools`).
#[derive(Serialize, ToSchema)]
pub(crate) struct AppToolDefinition {
    pub name: String,
    pub description: String,
    /// JSON Schema for `params`; `{"type":"object"}` when the app declares none.
    #[schema(value_type = Object)]
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
    pub cost_class: String,
    pub output_shape: Option<String>,
    pub examples: Vec<AppToolExample>,
    #[schema(value_type = Object)]
    pub default_params: Value,
    pub schedule: Option<String>,
    pub requires: Vec<String>,
    pub ready: bool,
}

/// `GET /apps` — the default listing.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppListResponse {
    pub apps: Vec<AppEntry>,
}

/// `GET /apps?format=tools` — the same registry as tool definitions.
#[derive(Serialize, ToSchema)]
pub(crate) struct AppToolsResponse {
    pub tools: Vec<AppToolDefinition>,
}

// ---------------------------------------------------------------------------
// health.rs (extraction health: /sources, /enforcement/preview)
// ---------------------------------------------------------------------------

/// Whether declared data contracts are being enforced, and whether the catalog
/// that declares them could even be read.
#[derive(Serialize, ToSchema)]
pub(crate) struct ContractsStatusDto {
    pub enforce_configured: bool,
    /// What the worker ACTUALLY did, which is the number that matters: a
    /// configured gate that never observed is not a gate.
    pub enforce_observed: bool,
    pub catalog_ok: bool,
    /// ABSENT when the catalog read succeeded.
    pub catalog_error: Option<String>,
    pub declared: i64,
    /// ABSENT unless there is one.
    pub reason: Option<String>,
}

/// One source's extraction health.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceHealthDto {
    pub id: String,
    pub app: String,
    pub dataset: String,
    /// `healthy` | `suspect` | `degraded` | `quarantined` | `probation` |
    /// `retired` | `unknown`.
    pub state: String,
    pub degradation_score: f64,
    pub state_since: String,
    /// ABSENT when unset.
    pub state_reason: Option<String>,
    /// ABSENT when unset.
    pub last_verdict: Option<String>,
    /// ABSENT when unset.
    pub last_verdict_at: Option<String>,
    pub tripped_of_last3: i64,
    pub monitored: bool,
    pub updated_at: String,
    /// Listing only, and only when a contract verdict exists: the worker's
    /// verdict blob plus `age_secs` / `stale` / `stale_reason`. Free-form —
    /// the worker owns the shape.
    #[schema(value_type = Option<Object>)]
    pub contract: Option<Value>,
}

/// `GET /sources`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceListResponse {
    /// Always `true` — the subsystem is compiled in.
    pub enabled: bool,
    /// Whether a degraded verdict actually withholds writes today.
    pub enforcing: bool,
    pub contracts_enforce: bool,
    pub contracts: ContractsStatusDto,
    pub count: i64,
    /// Sources with no monitoring at all — the blind spot, named.
    pub unmonitored: i64,
    pub sources: Vec<SourceHealthDto>,
}

/// One judged extraction run.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceRunDto {
    pub job_id: String,
    pub docs: i64,
    pub fetch_ok_rate: f64,
    /// Text / DOM / value drift against the baseline. ABSENT when not computed.
    pub d_text: Option<f64>,
    pub d_dom: Option<f64>,
    pub d_val: Option<f64>,
    pub compared: i64,
    pub verdict: String,
    /// ABSENT when the verdict named no diagnosis.
    pub diagnosis: Option<String>,
    pub score: f64,
    /// ABSENT when none. Free-form: the diagnosis's own evidence.
    #[schema(value_type = Option<Object>)]
    pub reasons: Option<Value>,
    pub state_after: String,
    /// ABSENT on runs recorded before build stamping.
    pub build_id: Option<String>,
    pub created_at: String,
}

/// One extracted field's observed statistics, against its baseline.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceFieldStats {
    pub field: String,
    pub docs: i64,
    pub miss_rate: f64,
    pub coercion_failure_rate: f64,
    pub distinct_ratio: f64,
    pub mean_len: f64,
    pub baseline_runs: i64,
    /// `null` when there is no baseline yet — unknown, not zero.
    pub baseline_miss_rate: Option<f64>,
    pub baseline_distinct_ratio: Option<f64>,
}

/// One learned invariant over a field. `kind` selects which of the bounds are
/// populated, so all of them are optional.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceInvariant {
    pub field: String,
    /// `type` | `regex` | `range` | `nonnull` | `distinctness`.
    pub kind: String,
    /// `type` only.
    pub json_type: Option<String>,
    /// `regex` only.
    pub pattern: Option<String>,
    /// `range` (both) and `distinctness` (`min` only).
    pub min: Option<f64>,
    pub max: Option<f64>,
    /// How many documents this invariant was learned from.
    pub support: i64,
    pub confidence: f64,
}

/// `GET /sources/{id}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceDetailResponse {
    pub source: SourceHealthDto,
    /// `null` when no contract verdict has been recorded since boot.
    #[schema(value_type = Option<Object>)]
    pub contract: Option<Value>,
    pub enforcing: bool,
    /// Whether enough runs exist for the drift statistics to mean anything.
    pub statistical_coverage: bool,
    pub runs: Vec<SourceRunDto>,
    pub fields: Vec<SourceFieldStats>,
    pub invariants: Vec<SourceInvariant>,
    /// Pointer to the sibling endpoint that answers the OTHER health question
    /// (did it run at all), because the two get confused constantly.
    pub see_also: String,
}

/// `GET /sources/{id}/runs`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceRunsResponse {
    pub id: String,
    pub count: i64,
    pub runs: Vec<SourceRunDto>,
}

/// `POST /sources/{id}/state` — a manual override of a source's state.
#[derive(Serialize, ToSchema)]
pub(crate) struct SourceStateResponse {
    pub id: String,
    pub state: String,
    pub reason: String,
}

/// Runs and documents one consequence would have touched.
#[derive(Serialize, ToSchema)]
pub(crate) struct PreviewRunCount {
    pub runs: i64,
    pub docs: i64,
}

/// What enforcement WOULD have done, counted per consequence.
#[derive(Serialize, ToSchema)]
pub(crate) struct PreviewConsequencesDto {
    pub diverted_writes: PreviewRunCount,
    pub withheld_removals: PreviewRunCount,
    pub suppressed_pushes: PreviewRunCount,
    pub skipped_index_writes: PreviewRunCount,
    pub trust_stamped: PreviewRunCount,
}

/// One state transition the replay would have made.
#[derive(Serialize, ToSchema)]
pub(crate) struct PreviewTransitionDto {
    pub job_id: String,
    pub at: String,
    pub from: String,
    pub to: String,
    /// `verdict` | `outside`.
    pub cause: String,
    pub verdict: String,
    pub score: f64,
    pub diagnosis: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub reasons: Option<Value>,
    pub gates: Vec<String>,
}

/// One source that is NOT ready for enforcement, and which gates it fails.
#[derive(Serialize, ToSchema)]
pub(crate) struct PreviewNotReady {
    pub id: String,
    pub state: String,
    pub gates: Vec<String>,
    /// ABSENT when there is no transition to point at.
    pub since: Option<PreviewTransitionDto>,
}

/// One source's enforcement preview.
#[derive(Serialize, ToSchema)]
pub(crate) struct PreviewSource {
    pub id: String,
    pub runs_replayed: i64,
    /// Runs the judge could not score — the honest denominator.
    pub unjudged_runs: i64,
    /// ABSENT when the window is already open.
    pub window_opens_at: Option<String>,
    pub window_opens_in: String,
    pub state: String,
    pub gates: Vec<String>,
    /// The state the LIVE system is in, as against the replayed one.
    pub live_state: String,
    pub monitored: bool,
    pub transitions: Vec<PreviewTransitionDto>,
    pub consequences: PreviewConsequencesDto,
}

/// `GET /enforcement/preview` — what `[resilience] enforce = true` would have
/// done. Gates nothing, writes nothing.
#[derive(Serialize, ToSchema)]
pub(crate) struct EnforcementPreview {
    pub enforcing: bool,
    pub runs_per_source: i64,
    pub sources_replayed: i64,
    pub ready: bool,
    pub not_ready: Vec<PreviewNotReady>,
    pub unmonitored: Vec<String>,
    pub totals: PreviewConsequencesDto,
    pub sources: Vec<PreviewSource>,
}

// ---------------------------------------------------------------------------
// runtime.rs
// ---------------------------------------------------------------------------

/// One host's learned tier memory and politeness penalty.
#[derive(Serialize, ToSchema)]
pub(crate) struct HostProfileDto {
    pub host: String,
    /// The tier the router pins for this host; `null` when it has learned none.
    pub preferred_tier: Option<String>,
    pub http_strikes: i64,
    /// The LIVE governor delay, not the stored one.
    pub penalty_ms: i64,
    /// ABSENT on the live-penalty-only answer for a host with no stored row.
    pub observations: Option<i64>,
    pub updated_at: Option<String>,
    pub penalty_updated_at: Option<String>,
}

/// `GET /hosts` with `cursor`.
#[derive(Serialize, ToSchema)]
pub(crate) struct HostPage {
    pub items: Vec<HostProfileDto>,
    pub next_cursor: Option<String>,
}

/// `GET /hosts` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct HostListResponse {
    pub hosts: Vec<HostProfileDto>,
}

/// `DELETE /hosts/{host}/memory`.
#[derive(Serialize, ToSchema)]
pub(crate) struct HostMemoryReset {
    pub host: String,
    pub reset: bool,
}

/// One cache key's learned change cadence.
#[derive(Serialize, ToSchema)]
pub(crate) struct CacheKeyFreshness {
    pub key: String,
    pub url: String,
    pub checks: i64,
    pub changes: i64,
    pub last_checked_at: String,
    /// `null` when it has never been seen to change.
    pub last_change_at: Option<String>,
    /// The learned interval; `null` while unknown.
    pub interval_secs: Option<f64>,
    pub predicted_next_change: String,
    pub due_in_secs: f64,
}

/// Per-host refresh pressure.
#[derive(Serialize, ToSchema)]
pub(crate) struct CacheHostFreshness {
    pub host: String,
    pub keys: i64,
    pub due_now: i64,
}

/// `GET /cache/freshness`.
#[derive(Serialize, ToSchema)]
pub(crate) struct CacheFreshnessResponse {
    pub refresher_enabled: bool,
    pub keys: Vec<CacheKeyFreshness>,
    pub hosts: Vec<CacheHostFreshness>,
}

/// `GET /profiles` — the session vault's named login profiles.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProfileListResponse {
    pub profiles: Vec<ProfileInfoDto>,
}

/// One named login profile. Cookies and browser state are never returned.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProfileInfoDto {
    pub name: String,
    pub has_cookies: bool,
    pub has_browser_dir: bool,
    pub last_used: Option<String>,
}

/// `GET /plugins`.
#[derive(Serialize, ToSchema)]
pub(crate) struct PluginListResponse {
    /// Each plugin's SELF-DECLARED manifest. The host does not impose a shape
    /// on it, so neither does this schema.
    #[schema(value_type = Vec<Object>)]
    pub plugins: Vec<Value>,
}

/// `POST /plugins/reload`.
#[derive(Serialize, ToSchema)]
pub(crate) struct PluginReloadResponse {
    pub loaded: i64,
}

/// `POST /extract/preview` — a RuleSet dry run against one document.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExtractPreviewResponse {
    /// The extracted record. Keys are the caller's own rule fields.
    #[schema(value_type = Object)]
    pub values: Value,
    /// Per-field extraction status, coercion outcomes and `each` sub-stats. Its
    /// shape follows the RuleSet the caller sent, so it is free-form here.
    #[schema(value_type = Object)]
    pub report: Value,
    pub fields_matched: i64,
    pub fields_total: i64,
}

// ---------------------------------------------------------------------------
// search.rs
// ---------------------------------------------------------------------------

/// One search hit.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchHitDto {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub url: String,
    pub title: String,
    pub score: f64,
    pub snippet: String,
}

/// Whether the index behind an answer is trustworthy.
///
/// This block is why `/search` cannot go silently-empty: an index that is off,
/// wiped or mid-rebuild says so here instead of returning zero hits that read
/// like "nothing matched".
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchIndexState {
    pub enabled: bool,
    /// `null` when the index could not be read.
    pub doc_count: Option<i64>,
    pub degraded: bool,
    /// `null` when not degraded.
    pub reason: Option<String>,
}

/// One facet value and its count.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchFacetCount {
    pub value: String,
    pub count: i64,
}

/// Facet counts over the whole match set, not just this page.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchFacetsDto {
    pub apps: Vec<SearchFacetCount>,
    pub datasets: Vec<SearchFacetCount>,
}

/// `GET /search`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchResponse {
    pub query: String,
    /// Full match count, which is not `count` unless the page held everything.
    pub total: i64,
    pub count: i64,
    pub hits: Vec<SearchHitDto>,
    pub index: SearchIndexState,
    /// ABSENT on the shared MCP surface, which does not ask for facets.
    pub facets: Option<SearchFacetsDto>,
}

/// One index-time enricher's throughput.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchEnricherStat {
    pub name: String,
    pub docs: i64,
    pub entities: i64,
    pub failures: i64,
}

/// `GET /search/status`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchStatusResponse {
    pub enabled: bool,
    pub doc_count: i64,
    pub disk_bytes: i64,
    pub segment_count: i64,
    pub enrichers: Vec<SearchEnricherStat>,
}

/// Where a saved search materializes its hits, when it does.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchMaterializeDto {
    pub app: String,
    pub dataset: String,
}

/// One saved search. `secret` is never serialized.
#[derive(Serialize, ToSchema)]
pub(crate) struct SavedSearchDto {
    pub id: String,
    pub query: String,
    pub app: Option<String>,
    pub dataset: Option<String>,
    pub url: String,
    pub enabled: bool,
    /// ABSENT unless the saved search materializes.
    pub materialize: Option<SearchMaterializeDto>,
    pub created_at: String,
}

/// `GET /searches` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct SavedSearchListResponse {
    pub searches: Vec<SavedSearchDto>,
}

/// `GET /searches` with `cursor`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SavedSearchPage {
    pub items: Vec<SavedSearchDto>,
    pub next_cursor: Option<String>,
}

/// `DELETE /search/docs` — the count REQUESTED, not an index-confirmed one.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchDocsDeleted {
    pub deleted: i64,
}

/// `DELETE /search/datasets/{app}/{dataset}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct SearchDatasetDeleted {
    pub app: String,
    pub dataset: String,
    pub deleted: bool,
}

// ---------------------------------------------------------------------------
// query.rs (grants, catalog, datahub, market)
// ---------------------------------------------------------------------------

/// `GET /grants` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct GrantListResponse {
    pub grants: Vec<RecordDto>,
}

/// `GET /grants/closing-soon`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ClosingSoonResponse {
    /// The CLAMPED horizon actually used (1..=365).
    pub days: i64,
    pub count: i64,
    /// The unified record's `data` spread inline, plus a guaranteed `key` and
    /// `days_left`. Rows whose `close_date` will not parse are DROPPED rather
    /// than given a fabricated deadline.
    #[schema(value_type = Vec<Object>)]
    pub grants: Vec<Value>,
}

/// `GET /grants/programs` without `cursor` (legacy shape).
#[derive(Serialize, ToSchema)]
pub(crate) struct ProgramListResponse {
    pub programs: Vec<RecordDto>,
}

/// One numeric bound of a declared data contract.
#[derive(Serialize, ToSchema)]
pub(crate) struct ContractRangeDto {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// A source's declared data contract.
#[derive(Serialize, ToSchema)]
pub(crate) struct ContractDto {
    pub required_fields: Vec<String>,
    /// Field name to expected JSON type.
    #[schema(value_type = Object)]
    pub types: Value,
    /// Field name to a `{min, max}` bound.
    #[schema(value_type = Object)]
    pub ranges: Value,
    pub max_row_delta_pct: Option<f64>,
    pub max_staleness_hours: Option<i64>,
}

/// One catalog `[[source]]` — the machine-readable pipeline registry row.
#[derive(Serialize, ToSchema)]
pub(crate) struct CatalogSourceDto {
    pub id: String,
    pub app: String,
    pub market: String,
    pub name: String,
    pub url: String,
    pub category: String,
    pub engine: String,
    pub access: String,
    pub cadence: String,
    pub cron: String,
    pub status: String,
    pub confidence: i64,
    pub dataset: String,
    pub notes: String,
    pub module_sha256: String,
    /// ABSENT when the source declares no contract.
    pub contract: Option<ContractDto>,
}

/// `GET /catalog/sources`.
#[derive(Serialize, ToSchema)]
pub(crate) struct CatalogSourcesResponse {
    pub count: i64,
    pub sources: Vec<CatalogSourceDto>,
}

/// One source's freshness verdict — did it RUN, as against did it run right.
#[derive(Serialize, ToSchema)]
pub(crate) struct CatalogHealthSource {
    pub id: String,
    pub app: String,
    pub dataset: String,
    pub cadence: String,
    /// `false` when this source has no freshness expectation to check against.
    pub monitored: bool,
    /// Why it is unmonitored, or why it is considered stale. ABSENT otherwise.
    pub reason: Option<String>,
    /// Monitored sources only.
    pub expected_max_age_secs: Option<i64>,
    /// `null` when the dataset has never been written.
    pub last_write_at: Option<String>,
    pub age_secs: Option<i64>,
    pub stale: Option<bool>,
    /// The declared contract plus its last verdict; ABSENT when none is
    /// declared.
    #[schema(value_type = Option<Object>)]
    pub contract: Option<Value>,
}

/// `GET /catalog/health`.
#[derive(Serialize, ToSchema)]
pub(crate) struct CatalogHealthResponse {
    pub checked: i64,
    pub stale: i64,
    pub contracts_enforce: bool,
    pub sources: Vec<CatalogHealthSource>,
    /// Pointer to the sibling endpoint answering the other health question.
    pub see_also: String,
}

/// A schedule the catalog says should exist and does not.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileCreate {
    pub source_id: String,
    pub app: String,
    pub cron: String,
}

/// A schedule whose cron drifted from the catalog's.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileUpdate {
    pub schedule_id: String,
    pub app: String,
    pub from_cron: String,
    pub to_cron: String,
    pub re_enable: bool,
}

/// A schedule the catalog no longer wants.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileDisable {
    pub schedule_id: String,
    pub app: String,
    pub reason: String,
}

/// A schedule reconcile will NOT touch, and why. Orphans are reported rather
/// than deleted: a schedule nobody claims may still be one somebody made.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileOrphan {
    pub schedule_id: String,
    pub app: String,
    pub reason: String,
}

/// `GET /catalog/reconcile` — the plan. Changes nothing.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcilePlanDto {
    pub create: Vec<ReconcileCreate>,
    pub update: Vec<ReconcileUpdate>,
    pub disable: Vec<ReconcileDisable>,
    pub orphan: Vec<ReconcileOrphan>,
    pub covered_by_untagged: i64,
    pub in_sync: i64,
    /// GET only: nothing to do.
    pub empty: Option<bool>,
    /// GET only: whether the scheduler applies this plan on its own.
    pub auto_reconcile: Option<bool>,
}

/// What applying the plan actually did.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileApplied {
    pub created: i64,
    pub updated: i64,
    pub disabled: i64,
    /// Orphans are counted, never deleted.
    pub orphans_untouched: i64,
    /// Per-item failures. The apply is not atomic, so a non-empty list here
    /// with non-zero counts above is a real, partial outcome.
    pub errors: Vec<String>,
}

/// `POST /catalog/reconcile`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ReconcileApplyResponse {
    pub applied: ReconcileApplied,
    pub plan: ReconcilePlanDto,
}

/// `GET /datahub/status` — DataHub and OpenLineage emission state.
///
/// Free-form on purpose: the block is assembled from the emitters' own
/// counters, the governance poller's last summary and per-writer lineage
/// status, all of which change with those subsystems rather than with this
/// route. Its keys are `enabled`, `gms_url`, `env`, `token_set`, `emit_schema`,
/// `emit_profile`, `emit_flows`, `last_emission`, `emissions`, `govern` and
/// `lineage`.
#[derive(Serialize, ToSchema)]
pub(crate) struct DatahubStatusResponse {
    pub enabled: bool,
    pub gms_url: String,
    pub env: String,
    pub token_set: bool,
    pub emit_schema: bool,
    pub emit_profile: bool,
    pub emit_flows: bool,
    #[schema(value_type = Option<Object>)]
    pub last_emission: Option<Value>,
    #[schema(value_type = Object)]
    pub emissions: Value,
    #[schema(value_type = Object)]
    pub govern: Value,
    #[schema(value_type = Object)]
    pub lineage: Value,
}

/// `POST /datahub/sync` — one emission's outcome. `entities` on success and
/// `error` on failure are mutually exclusive.
#[derive(Serialize, ToSchema)]
pub(crate) struct DatahubSyncResponse {
    /// Always `sync`.
    pub kind: String,
    pub at: String,
    pub ok: bool,
    /// Success only.
    pub entities: Option<i64>,
    /// Failure only.
    pub error: Option<String>,
    /// ABSENT when the run failed before listing datasets.
    pub datasets: Option<i64>,
    pub flows: Option<i64>,
    pub trigger_edges: Option<i64>,
}

/// A schedule the governance poll would disable.
#[derive(Serialize, ToSchema)]
pub(crate) struct GovernDisableSchedule {
    pub app: String,
    pub dataset: String,
    /// Always `deprecation`.
    pub evidence: String,
    pub schedule_ids: Vec<String>,
    /// True when the action is suppressed by policy and would NOT be taken.
    pub suppressed: bool,
    pub note: String,
}

/// A sync the governance poll would enqueue.
#[derive(Serialize, ToSchema)]
pub(crate) struct GovernEnqueueSync {
    pub app: String,
    pub dataset: String,
    /// Always `assertions`.
    pub evidence: String,
    /// False when no such app is registered here — the action would be a no-op.
    pub registered: bool,
    pub idempotency_key: String,
    pub note: String,
}

/// Everything the governance poll would do.
#[derive(Serialize, ToSchema)]
pub(crate) struct GovernWould {
    pub disable_schedules: Vec<GovernDisableSchedule>,
    pub pause_apps: Vec<String>,
    pub resume_apps: Vec<String>,
    pub enqueue_syncs: Vec<GovernEnqueueSync>,
}

/// Counts of what the poll would do.
#[derive(Serialize, ToSchema)]
pub(crate) struct GovernTotals {
    pub schedules_disabled: i64,
    pub apps_paused: i64,
    pub syncs_enqueued: i64,
    pub read_errors: i64,
}

/// `GET /datahub/governance/preview` — what the poll would do right now.
/// Writes nothing.
#[derive(Serialize, ToSchema)]
pub(crate) struct GovernancePreview {
    pub at: String,
    pub governing: bool,
    pub gms_url: String,
    pub env: String,
    pub datasets_polled: i64,
    pub poll_ms: i64,
    pub budget_secs: i64,
    /// True when the poll found nothing to say.
    pub quiet: bool,
    pub would: GovernWould,
    pub paused_now: Vec<String>,
    /// Datasets whose governance metadata could not be read. A read error is
    /// NOT an absence of policy, which is why `poll_would_abort` exists.
    pub read_errors: Vec<String>,
    pub poll_would_abort: bool,
    pub totals: GovernTotals,
}

/// `GET /market/profile/{state}/{trade}` — the record's `data`, unwrapped.
///
/// The row is an app-owned cross-family join (economics x density) whose blocks
/// evolve with the apps that write it, so the payload is free-form here rather
/// than a schema this route does not enforce. `coverage` is `both` or
/// `economics_only`, and `density` is `null` for the latter — an absent join,
/// never a fabricated density.
#[derive(Serialize, ToSchema)]
pub(crate) struct MarketProfileResponse {
    pub state: String,
    pub trade: String,
    /// `both` | `economics_only`.
    pub coverage: String,
    /// `null` when coverage is `economics_only`.
    #[schema(value_type = Option<Object>)]
    pub density: Option<Value>,
    #[schema(value_type = Object)]
    pub economics: Value,
}

// ---------------------------------------------------------------------------
// provisioner.rs
// ---------------------------------------------------------------------------

/// One compiled proposal awaiting validation or promotion.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProposalSummary {
    pub key: String,
    #[schema(value_type = Option<Object>)]
    pub prompt: Option<Value>,
    /// `planned` | `validated` | `failed` | `promoted`.
    pub status: String,
    /// A proposal past its freshness window: its sample no longer stands.
    pub expired: bool,
    #[schema(value_type = Option<Object>)]
    pub verdict: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub accepted: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub catalog_confidence: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub engine: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub url: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub intended_dataset: Option<Value>,
    pub first_seen: String,
    pub updated_at: String,
    pub age_secs: i64,
}

/// `GET /provisioner/proposals` with `cursor`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProposalPage {
    pub items: Vec<ProposalSummary>,
    pub next_cursor: Option<String>,
}

/// `POST /provisioner/proposals/{key}/validate` — a real fetch and a real dry
/// run, with the verdict written back onto the proposal.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProposalValidation {
    pub key: String,
    /// `validated` | `failed`.
    pub status: String,
    /// `{checked_at, sample, dry_run, accepted}` — the sample statistics and the
    /// held-out dry run, both owned by the provisioner app.
    #[schema(value_type = Object)]
    pub validation: Value,
}

/// `POST /provisioner/proposals/{key}/promote` — hands back the catalog block
/// to paste. This route NEVER writes `catalog/data-sources.toml` itself.
#[derive(Serialize, ToSchema)]
pub(crate) struct ProposalPromotion {
    pub key: String,
    /// Always `promoted`.
    pub status: String,
    /// A TOML `[[source]]` fragment, as a string.
    pub catalog_toml: String,
}

// ---------------------------------------------------------------------------
// recipes.rs
// ---------------------------------------------------------------------------

/// One discovered JSON-API endpoint behind a rendered page (N14).
#[derive(Serialize, ToSchema)]
pub(crate) struct RecipeDto {
    pub id: String,
    pub host: String,
    pub url_template: String,
    /// Parsed request parameters; `null` when unparseable.
    #[schema(value_type = Option<Object>)]
    pub params: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub json_paths: Option<Value>,
    pub score: f64,
    /// Only a validated recipe is ever preferred over the live ladder.
    pub validated: bool,
    pub validation_reason: Option<String>,
    pub validated_at: Option<String>,
    pub consecutive_failures: i64,
    pub discovered_at: String,
    pub last_seen_at: String,
}

/// `GET /recipes`.
#[derive(Serialize, ToSchema)]
pub(crate) struct RecipeListResponse {
    pub recipes: Vec<RecipeDto>,
}

/// A signed mesh bundle: an opaque `payload` under this node's signature.
///
/// The signature covers the payload as serialized, so a consumer verifies
/// BEFORE interpreting anything inside it.
#[derive(Serialize, ToSchema)]
pub(crate) struct SignedBundle {
    /// `pumper.recipes/1` | `pumper.host-weather/2`.
    pub schema: String,
    pub node_id: String,
    pub generated_at: String,
    /// The pre-ed25519 identifier, carried so an older peer can still match.
    pub legacy_id: String,
    #[schema(value_type = Object)]
    pub payload: Value,
    /// Hex ed25519 signature over the payload.
    pub sig: String,
}

/// `POST /recipes/import`.
#[derive(Serialize, ToSchema)]
pub(crate) struct RecipeImportResponse {
    pub applied: bool,
    /// `null` when the bundle named no node.
    pub source_node_id: Option<String>,
    /// Whether the signature checked out. An unverified bundle is only applied
    /// when the peer is configured `allow_unsigned`.
    pub verified: bool,
    pub considered: i64,
    pub imported: i64,
    pub skipped: i64,
    /// Capped at 20 — the truncation is deliberate, not a silent drop.
    pub notes: Vec<String>,
    /// Always `false`: an imported recipe is never trusted as validated. It
    /// must earn that here.
    pub validated: bool,
}

// ---------------------------------------------------------------------------
// remote.rs
// ---------------------------------------------------------------------------

/// `POST /fetch-proxy` — one fetch performed through this node's local stack on
/// a peer's behalf.
#[derive(Serialize, ToSchema)]
pub(crate) struct FetchProxyResponse {
    pub status: i64,
    /// Response headers, plus one this node adds naming the URL it was asked
    /// for — so a caller can tell which request an answer belongs to.
    #[schema(value_type = Object)]
    pub headers: Value,
    pub body: String,
    /// After redirects.
    pub final_url: String,
    pub cache_hit: bool,
}

// ---------------------------------------------------------------------------
// host_weather.rs
// ---------------------------------------------------------------------------

/// One host's weather, as exported to a peer.
#[derive(Serialize, ToSchema)]
pub(crate) struct WeatherEntryDto {
    pub host: String,
    pub preferred_tier: Option<String>,
    pub http_strikes: i64,
    pub penalty_ms: i64,
    pub observations: i64,
    pub challenge_fingerprints: Vec<String>,
    pub updated_at: Option<String>,
}

/// `GET /host-weather/export?schema=1` — the legacy flat bundle, which is
/// UNSIGNED. Kept so an older peer keeps working; new consumers should read the
/// signed `SignedBundle` the default answer carries.
#[derive(Serialize, ToSchema)]
pub(crate) struct HostWeatherLegacyBundle {
    /// Always `pumper.host-weather/1`.
    pub schema: String,
    pub generated_at: String,
    /// The LEGACY id, not the ed25519 node id.
    pub node_id: String,
    pub min_observations: i64,
    pub entries: Vec<WeatherEntryDto>,
}

/// What importing one host's weather would change locally.
#[derive(Serialize, ToSchema)]
pub(crate) struct WeatherPlanDto {
    pub host: String,
    pub adopt_pin: bool,
    /// `null` when the local value already covers the peer's.
    pub raise_strikes: Option<i64>,
    pub raise_penalty_ms: Option<i64>,
    pub notes: Vec<String>,
}

/// `POST /host-weather/import`. An import only ever RAISES local caution — it
/// cannot make this node less polite than it already decided to be.
#[derive(Serialize, ToSchema)]
pub(crate) struct HostWeatherImportResponse {
    pub applied: bool,
    pub source_node_id: Option<String>,
    pub verified: bool,
    /// Echoed from the bundle; empty string when it declared none.
    pub schema: String,
    pub considered: i64,
    pub changed: i64,
    pub noops: i64,
    pub actions: Vec<WeatherPlanDto>,
}

// ---------------------------------------------------------------------------
// mesh.rs
// ---------------------------------------------------------------------------

/// `GET /node` — this node's signing identity.
#[derive(Serialize, ToSchema)]
pub(crate) struct NodeResponse {
    pub node_id: String,
    pub legacy_id: String,
    /// Always `ed25519`.
    pub algo: String,
    pub public_key: String,
    pub key_path: String,
    /// True when this request is what created the key.
    pub key_created: bool,
}

/// One sync stream with one peer.
#[derive(Serialize, ToSchema)]
pub(crate) struct MeshStream {
    pub stream: String,
    pub schedule_id: String,
    /// False when the schedule for this stream does not exist — the stream is
    /// configured but nothing is pulling it.
    pub scheduled: bool,
    pub last_attempt_at: Option<String>,
    pub last_success_at: Option<String>,
    /// Seconds since the last SUCCESS; `null` when there has never been one.
    pub lag_secs: Option<i64>,
    pub ok: Option<bool>,
    pub verified: Option<bool>,
    pub pulls: i64,
    /// Non-zero here means a peer's bundles are being rejected, silently as far
    /// as the data is concerned.
    pub signature_failures: i64,
    pub ghosts_removed: i64,
    #[schema(value_type = Option<Object>)]
    pub detail: Option<Value>,
}

/// One configured peer.
#[derive(Serialize, ToSchema)]
pub(crate) struct MeshPeer {
    pub name: String,
    pub url: String,
    pub key_pinned: bool,
    pub allow_unsigned: bool,
    pub every: String,
    pub every_secs: i64,
    pub enabled: bool,
    pub streams: Vec<MeshStream>,
}

/// Fleet-wide mesh counters.
#[derive(Serialize, ToSchema)]
pub(crate) struct MeshTotalsDto {
    pub peers: i64,
    pub streams: i64,
    pub pulls: i64,
    pub signature_failures: i64,
    pub ghosts_removed: i64,
}

/// `GET /mesh`.
#[derive(Serialize, ToSchema)]
pub(crate) struct MeshResponse {
    /// Empty string when this node's identity could not be read.
    pub node_id: String,
    pub peers: Vec<MeshPeer>,
    pub totals: MeshTotalsDto,
}

// ---------------------------------------------------------------------------
// principals.rs
// ---------------------------------------------------------------------------

/// One API principal. `key_hash` is never serialized, and the plaintext key
/// exists in exactly two responses: creation and rotation.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalDto {
    pub id: String,
    pub name: String,
    /// `read` | `enqueue:<app|*>` | `admin`.
    pub scopes: Vec<String>,
    pub budget_usd_per_day: Option<f64>,
    pub rate_limit_per_min: Option<i64>,
    pub enabled: bool,
    pub created_at: String,
}

/// Who the server resolved this request as.
#[derive(Serialize, ToSchema)]
pub(crate) struct CallerPrincipalDto {
    pub id: String,
    pub name: String,
    pub scopes: Vec<String>,
    /// True in `[auth] mode = "open"`: the synthetic operator, not a stored
    /// principal. Saying so is what keeps "unauthenticated" from reading as
    /// "authenticated as somebody".
    pub synthetic: bool,
}

/// `GET /principals`.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalListResponse {
    pub count: i64,
    /// `open` | `keys`.
    pub mode: String,
    /// `null` when no principal was resolved for this request.
    pub caller: Option<CallerPrincipalDto>,
    pub principals: Vec<PrincipalDto>,
}

/// `POST /principals` — the only response that carries a new key.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalCreated {
    pub principal: PrincipalDto,
    /// Shown ONCE. Stored only as a digest and never readable again.
    pub key: String,
}

/// `POST /principals/{id}/disable`.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalDisabled {
    pub id: String,
    /// Always `false`.
    pub enabled: bool,
}

/// `POST /principals/{id}/rotate` — the old key stops working immediately.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalRotated {
    pub id: String,
    /// Shown ONCE.
    pub key: String,
}

/// One audit entry: a mutating verb, who did it, and to what.
#[derive(Serialize, ToSchema)]
pub(crate) struct AuditEntryDto {
    pub id: i64,
    /// `null` in `open` mode — the action is still logged, without a caller.
    pub principal_id: Option<String>,
    pub action: String,
    pub target: Option<String>,
    pub at: String,
    /// A raw JSON STRING, not an object.
    pub detail: Option<String>,
}

/// `GET /audit`.
#[derive(Serialize, ToSchema)]
pub(crate) struct AuditPage {
    pub items: Vec<AuditEntryDto>,
    pub next_cursor: Option<String>,
}

/// `GET /principals/costs`.
#[derive(Serialize, ToSchema)]
pub(crate) struct PrincipalCostsResponse {
    pub total_usd: f64,
    pub by_principal: Vec<PrincipalCostRow>,
}

// ---------------------------------------------------------------------------
// workflows.rs
// ---------------------------------------------------------------------------

/// One declared multi-step DAG.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowDefDto {
    pub id: String,
    pub name: String,
    /// The declared plan: steps, `after` barriers and param templates.
    #[schema(value_type = Object)]
    pub spec: Value,
    pub cron: Option<String>,
    pub enabled: bool,
    pub created_at: String,
}

/// One step as accepted at creation time.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowStepAccepted {
    pub step: String,
    pub app: String,
    /// The steps this one joins on — an `all_of` barrier.
    pub after: Vec<String>,
    /// False when the step's params could not be checked against the app's
    /// schema at declaration time (templated values are only known at run time).
    pub params_validated: bool,
}

/// `POST /workflows`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowCreated {
    pub workflow: WorkflowDefDto,
    pub steps: Vec<WorkflowStepAccepted>,
}

/// `GET /workflows`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowListResponse {
    pub workflows: Vec<WorkflowDefDto>,
}

/// `GET /workflows/{id}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowResponse {
    pub workflow: WorkflowDefDto,
}

/// One run of a workflow.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunDto {
    pub id: String,
    pub def_id: String,
    /// `running` | `succeeded` | `failed` | `cancelled`.
    pub status: String,
    pub budget_usd: Option<f64>,
    pub spent_usd: f64,
    pub idempotency_key: Option<String>,
    pub principal_id: Option<String>,
    /// The lineage root every step's job hangs off.
    pub root_id: String,
    pub error: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

/// `GET /workflows/{id}/runs`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunListResponse {
    pub runs: Vec<WorkflowRunDto>,
}

/// `POST /workflows/{id}/runs`. 202 when this call created the run, 200 when an
/// idempotency key replayed an existing one — `created` says which.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunStarted {
    pub run: WorkflowRunDto,
    pub created: bool,
}

/// The plan a run belongs to; `null` when it has since been deleted.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunPlan {
    pub id: String,
    pub name: String,
    pub cron: Option<String>,
}

/// One step's outcome inside a run.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunStep {
    pub step: String,
    /// `pending` | `queued` | `succeeded` | `failed` | `cancelled` | `skipped`.
    pub status: String,
    pub job_id: Option<String>,
    pub depends_on: Vec<String>,
    /// `null` — not `0` — when the step never became a job. A step that did not
    /// run did not cost nothing; its cost is unknown.
    pub cost_usd: Option<f64>,
    #[serde(rename = "yield")]
    pub yield_: Option<Vec<YieldEntryDto>>,
    pub error: Option<String>,
    pub finished_at: Option<String>,
}

/// The rolled-up receipt for a whole run.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunReceipt {
    pub cost_usd: f64,
    pub budget_usd: Option<f64>,
    pub steps_total: i64,
    /// Steps that actually carried a price. `steps_total - steps_priced` is how
    /// much of the run this number does NOT cover.
    pub steps_priced: i64,
    /// `new` / `changed` / `unchanged` / `removed`, each ABSENT when no step
    /// reported it. `{}` when nothing did.
    #[schema(value_type = Object)]
    #[serde(rename = "yield")]
    pub yield_: Value,
}

/// `GET /workflow-runs/{run_id}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunReport {
    pub run: WorkflowRunDto,
    pub workflow: Option<WorkflowRunPlan>,
    pub steps: Vec<WorkflowRunStep>,
    pub receipt: WorkflowRunReceipt,
    /// What this report could not establish, in prose.
    pub unknown: Vec<String>,
}

/// `DELETE /workflow-runs/{run_id}`.
#[derive(Serialize, ToSchema)]
pub(crate) struct WorkflowRunCancelled {
    pub cancelled: bool,
    pub jobs_cancelled: i64,
}

// ---------------------------------------------------------------------------
// transactions.rs
// ---------------------------------------------------------------------------

/// One staged irreversible browser action, awaiting (or past) approval.
#[derive(Serialize, ToSchema)]
pub(crate) struct TransactionDto {
    pub id: String,
    /// One key submits at most once, ever.
    pub idempotency_key: String,
    pub app: String,
    pub job_id: Option<String>,
    pub profile: Option<String>,
    /// `pending` | `approved` | `submitted` | `rejected` | `expired`.
    pub state: String,
    /// Digest of the evidence the approval was granted against. A commit whose
    /// live evidence no longer matches is REFUSED, not submitted anyway.
    pub evidence_sha: String,
    pub approved_by: Option<String>,
    pub approved_at: Option<String>,
    pub submitted_at: Option<String>,
    pub receipt_path: Option<String>,
    /// Derived; `null` when `approval_ttl_secs = 0` (no expiry).
    pub expires_at: Option<String>,
    /// Derived.
    pub expired: bool,
    pub created_at: String,
    pub updated_at: String,
    /// This node's `[transact] allow_live`, stamped on every row: without it a
    /// `pending` row looks the same whether or not submission is even possible.
    pub allow_live: bool,
}

/// `GET /transactions`.
#[derive(Serialize, ToSchema)]
pub(crate) struct TransactionListResponse {
    pub count: i64,
    pub allow_live: bool,
    pub transactions: Vec<TransactionDto>,
}

/// `POST /transactions/{id}/approve`.
#[derive(Serialize, ToSchema)]
pub(crate) struct TransactionApproved {
    pub transaction: TransactionDto,
    /// Whether the parked job was resumed by this call.
    pub resumed: bool,
    pub note: String,
}

/// `POST /transactions/{id}/reject`.
#[derive(Serialize, ToSchema)]
pub(crate) struct TransactionRejected {
    pub transaction: TransactionDto,
    pub job_cancelled: bool,
}

// ---------------------------------------------------------------------------
// executors.rs
// ---------------------------------------------------------------------------

/// `POST /jobs/{id}/heartbeat` — always `true`; a lost lease answers 409.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorOwned {
    pub owned: bool,
}

/// `POST /jobs/{id}/checkpoint`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorCheckpointResponse {
    /// `false` when the checkpoint was refused (e.g. the job moved on).
    pub saved: bool,
}

/// `POST /jobs/{id}/progress`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorProgressResponse {
    pub reported: bool,
}

/// `POST /jobs/{id}/finish` — what the coordinator did with the report.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorFinishResponse {
    /// `succeeded` | `queued` (retryable, put back) | `failed`.
    pub outcome: String,
}

/// One outbound executor.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorDto {
    pub id: String,
    /// `offline` | `busy` | `idle` — derived from `last_poll_age_secs`.
    pub state: String,
    pub capabilities: Vec<String>,
    pub running: i64,
    pub claimed_total: i64,
    pub last_poll_at: String,
    pub last_poll_age_secs: i64,
    pub first_seen_at: String,
}

/// `GET /executors`.
#[derive(Serialize, ToSchema)]
pub(crate) struct ExecutorListResponse {
    pub executors: Vec<ExecutorDto>,
    /// Which apps this plane may claim, so an executor that never gets work can
    /// tell a capability mismatch from an empty queue.
    pub eligible_apps: Vec<String>,
}

// ---------------------------------------------------------------------------
// Dual-mode unions
// ---------------------------------------------------------------------------
//
// A dozen list endpoints answer one shape without `?cursor=` and a keyset
// envelope with it, and one (`GET /apps`) switches on `?format=`. That is the
// legacy surface, and N23 was explicitly forbidden to tighten it — consumers of
// the bare-array shape must keep working. So the spec states the union instead
// of picking a winner: an untagged enum renders as `oneOf`, which is exactly
// what a generated client needs in order to narrow on the field it finds.
//
// Naming these is also the only way the dual mode is DISCOVERABLE. It used to
// live in a sentence inside a `description` string; now it is a schema, and a
// client that forgets to handle one arm fails to compile rather than reading
// `undefined` at run time.

/// `GET /jobs`: bare `[Job]`, or a keyset page with `?cursor=`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum JobsResponse {
    Legacy(Vec<JobDto>),
    Page(JobPage),
}

/// `GET /schedules`: bare array, or a keyset page with `?cursor=`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum SchedulesResponse {
    Legacy(Vec<ScheduleDto>),
    Page(SchedulePage),
}

/// `GET /datasets/{app}/{dataset}`: bare `[Record]`, or a keyset page.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum RecordsResponse {
    Legacy(Vec<RecordDto>),
    Page(RecordPage),
}

/// `GET /datasets/{app}/{dataset}/changes`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum ChangesResponse {
    Legacy(DatasetChangesResponse),
    Page(RevisionPageDto),
}

/// `GET /datasets/{app}/{dataset}/history`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum HistoryResponse {
    Legacy(RecordHistoryResponse),
    Page(RevisionPageDto),
}

/// `GET /triggers`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum TriggersResponse {
    Legacy(TriggerListResponse),
    Page(TriggerPage),
}

/// `GET /webhooks/deliveries`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum WebhookDeliveriesResponse {
    Legacy(DeliveryListResponse),
    Page(DeliveryPage),
}

/// `GET /watches`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum WatchesResponse {
    Legacy(WatchListResponse),
    Page(WatchPage),
}

/// `GET /watches/{id}/deliveries`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum WatchDeliveryFeed {
    Legacy(WatchDeliveriesResponse),
    Page(DeliveryPage),
}

/// `GET /subscriptions/{id}/deliveries`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum SubscriptionDeliveryFeed {
    Legacy(SubscriptionDeliveriesResponse),
    Page(DeliveryPage),
}

/// `GET /hosts`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum HostsResponse {
    Legacy(HostListResponse),
    Page(HostPage),
}

/// `GET /searches`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum SavedSearchesResponse {
    Legacy(SavedSearchListResponse),
    Page(SavedSearchPage),
}

/// `GET /grants`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum GrantsResponse {
    Legacy(GrantListResponse),
    Page(RecordPage),
}

/// `GET /grants/programs`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum ProgramsResponse {
    Legacy(ProgramListResponse),
    Page(RecordPage),
}

/// `GET /provisioner/proposals`: bare array, or a keyset page.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum ProposalsResponse {
    Legacy(Vec<ProposalSummary>),
    Page(ProposalPage),
}

/// `GET /apps`: the registry, or the same apps as agent tool definitions with
/// `?format=tools`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum AppsResponse {
    Registry(AppListResponse),
    Tools(AppToolsResponse),
}

/// `GET /host-weather/export`: the signed v2 envelope, or the unsigned legacy
/// flat bundle with `?schema=1`.
#[derive(Serialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum HostWeatherExport {
    Signed(SignedBundle),
    Legacy(HostWeatherLegacyBundle),
}
