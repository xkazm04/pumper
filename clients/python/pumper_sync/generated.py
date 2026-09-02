"""Wire types for the Pumper HTTP API — GENERATED, do not edit.

Emitted from clients/openapi.json by scripts/gen/generate-clients.mjs
(`just clients`). The spec is produced by the server's router and pinned to it
by a Rust test, so a rename over there lands here on the next regeneration and
CI fails if it does not.

Every shape is a functional-syntax TypedDict rather than the class syntax,
because the served document contains keys the class syntax cannot express: the
economics report is windowed under `"7d"` / `"30d"`, and job receipts carry a
`"yield"` block, which is a Python keyword.

Fields the server may omit are `NotRequired`. A field that is present-but-null
is typed `Optional` and stays required — the distinction is load-bearing here:
`null` is Pumper's honest-unknown, and a consumer that cannot tell it from an
absent key cannot tell "we do not know" from "we did not say".
"""

from __future__ import annotations

from typing import Any, NotRequired, Optional, TypedDict, Union

#: A payload the server does not type: a record's `data`, an app's `params`, a
#: plugin's self-declared manifest. Deliberately `Any` — narrowing it here would
#: be inventing a contract the server does not enforce.
Json = Any

AppDatasetsResponse = TypedDict(
    "AppDatasetsResponse",
    {
        "app": "str",
        "datasets": "list[str]",
    },
    total=True,
)
"""`GET /apps/{name}/datasets`."""

AppEntry = TypedDict(
    "AppEntry",
    {
        "cost_class": "NotRequired[Optional[str]]",
        "default_params": "NotRequired[Optional[Json]]",
        "description": "str",
        "dynamic": "NotRequired[Optional[bool]]",
        "has_params_schema": "bool",
        "module_sha256": "NotRequired[Optional[str]]",
        "name": "str",
        "output_shape": "NotRequired[Optional[str]]",
        "params_schema": "NotRequired[Optional[Json]]",
        "pinned": "NotRequired[Optional[bool]]",
        "ready": "bool",
        "reason": "NotRequired[Optional[str]]",
        "requires": "list[str]",
        "runnable": "NotRequired[Optional[bool]]",
        "schedule": "NotRequired[Optional[str]]",
        "world": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One registered app.

Compiled-in apps and discovered WASM apps (N09) share this listing, and the
two carry different keys: everything a dynamic app does not have is
`Option`, and `runnable: false` with a `reason` is how a discovered module
this build cannot execute says so instead of vanishing from the list."""

AppListResponse = TypedDict(
    "AppListResponse",
    {
        "apps": "list[AppEntry]",
    },
    total=True,
)
"""`GET /apps` — the default listing."""

AppReclaimDto = TypedDict(
    "AppReclaimDto",
    {
        "app": "str",
        "bytes": "int",
        "cassette_bytes": "int",
        "cassette_files": "int",
        "files": "int",
        "pinned_bytes": "int",
        "pinned_files": "int",
        "reclaimable_bytes": "int",
        "reclaimable_files": "int",
        "within_window_bytes": "int",
        "within_window_files": "int",
    },
    total=True,
)
"""Retained artifact bytes for one app."""

AppToolDefinition = TypedDict(
    "AppToolDefinition",
    {
        "cost_class": "str",
        "default_params": "Json",
        "description": "str",
        "examples": "list[AppToolExample]",
        "inputSchema": "Json",
        "name": "str",
        "output_shape": "NotRequired[Optional[str]]",
        "ready": "bool",
        "requires": "list[str]",
        "schedule": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One app as an agent-facing tool definition (`GET /apps?format=tools`)."""

AppToolExample = TypedDict(
    "AppToolExample",
    {
        "description": "str",
        "params": "Json",
    },
    total=True,
)
"""One worked example an agent can copy."""

AppToolsResponse = TypedDict(
    "AppToolsResponse",
    {
        "tools": "list[AppToolDefinition]",
    },
    total=True,
)
"""`GET /apps?format=tools` — the same registry as tool definitions."""

ApproveBody = TypedDict(
    "ApproveBody",
    {
        "evidence_sha": "NotRequired[Optional[str]]",
    },
    total=True,
)

AuditEntryDto = TypedDict(
    "AuditEntryDto",
    {
        "action": "str",
        "at": "str",
        "detail": "NotRequired[Optional[str]]",
        "id": "int",
        "principal_id": "NotRequired[Optional[str]]",
        "target": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One audit entry: a mutating verb, who did it, and to what."""

AuditPage = TypedDict(
    "AuditPage",
    {
        "items": "list[AuditEntryDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /audit`."""

BackfillBody = TypedDict(
    "BackfillBody",
    {
        "batch": "NotRequired[Optional[int]]",
        "cursor": "NotRequired[Optional[str]]",
        "max_rows": "NotRequired[Optional[int]]",
    },
    total=True,
)

BulkRetryBody = TypedDict(
    "BulkRetryBody",
    {
        "app": "NotRequired[Optional[str]]",
        "limit": "NotRequired[Optional[int]]",
        "status": "NotRequired[Optional[str]]",
    },
    total=True,
)

BulkRetryResponse = TypedDict(
    "BulkRetryResponse",
    {
        "ids": "list[str]",
        "retried": "int",
    },
    total=True,
)
"""`POST /jobs/retry` — the bulk re-queue door."""

CacheFreshnessResponse = TypedDict(
    "CacheFreshnessResponse",
    {
        "hosts": "list[CacheHostFreshness]",
        "keys": "list[CacheKeyFreshness]",
        "refresher_enabled": "bool",
    },
    total=True,
)
"""`GET /cache/freshness`."""

CacheHostFreshness = TypedDict(
    "CacheHostFreshness",
    {
        "due_now": "int",
        "host": "str",
        "keys": "int",
    },
    total=True,
)
"""Per-host refresh pressure."""

CacheKeyFreshness = TypedDict(
    "CacheKeyFreshness",
    {
        "changes": "int",
        "checks": "int",
        "due_in_secs": "float",
        "interval_secs": "NotRequired[Optional[float]]",
        "key": "str",
        "last_change_at": "NotRequired[Optional[str]]",
        "last_checked_at": "str",
        "predicted_next_change": "str",
        "url": "str",
    },
    total=True,
)
"""One cache key's learned change cadence."""

CallerPrincipalDto = TypedDict(
    "CallerPrincipalDto",
    {
        "id": "str",
        "name": "str",
        "scopes": "list[str]",
        "synthetic": "bool",
    },
    total=True,
)
"""Who the server resolved this request as."""

CancelJobResponse = TypedDict(
    "CancelJobResponse",
    {
        "cancelled": "bool",
        "note": "NotRequired[Optional[str]]",
        "running": "NotRequired[Optional[bool]]",
        "suspended": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""`DELETE /jobs/{id}`.

Three shapes share this schema, which is why everything but `cancelled` is
optional: a queued job answers `{cancelled: true}`; a running one adds
`running: true`; and a job that lost the race with a graceful shutdown —
it had already committed to a checkpoint suspend — answers
`{cancelled: false, running: true, suspended: true, note}`. That last one is
not a cancellation at all, and saying so honestly is the point."""

CatalogHealthResponse = TypedDict(
    "CatalogHealthResponse",
    {
        "checked": "int",
        "contracts_enforce": "bool",
        "see_also": "str",
        "sources": "list[CatalogHealthSource]",
        "stale": "int",
    },
    total=True,
)
"""`GET /catalog/health`."""

CatalogHealthSource = TypedDict(
    "CatalogHealthSource",
    {
        "age_secs": "NotRequired[Optional[int]]",
        "app": "str",
        "cadence": "str",
        "contract": "NotRequired[Optional[Json]]",
        "dataset": "str",
        "expected_max_age_secs": "NotRequired[Optional[int]]",
        "id": "str",
        "last_write_at": "NotRequired[Optional[str]]",
        "monitored": "bool",
        "reason": "NotRequired[Optional[str]]",
        "stale": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""One source's freshness verdict — did it RUN, as against did it run right."""

CatalogSourceDto = TypedDict(
    "CatalogSourceDto",
    {
        "access": "str",
        "app": "str",
        "cadence": "str",
        "category": "str",
        "confidence": "int",
        "contract": "NotRequired[Union[Json, ContractDto]]",
        "cron": "str",
        "dataset": "str",
        "engine": "str",
        "id": "str",
        "market": "str",
        "module_sha256": "str",
        "name": "str",
        "notes": "str",
        "status": "str",
        "url": "str",
    },
    total=True,
)
"""One catalog `[[source]]` — the machine-readable pipeline registry row."""

CatalogSourcesResponse = TypedDict(
    "CatalogSourcesResponse",
    {
        "count": "int",
        "sources": "list[CatalogSourceDto]",
    },
    total=True,
)
"""`GET /catalog/sources`."""

ClaimBody = TypedDict(
    "ClaimBody",
    {
        "capabilities": "NotRequired[list[str]]",
        "executor_id": "str",
    },
    total=True,
)

ClaimedJob = TypedDict(
    "ClaimedJob",
    {
        "app": "str",
        "attempt": "int",
        "budget_usd": "NotRequired[Optional[float]]",
        "job_id": "str",
        "params": "Json",
        "restored": "NotRequired[Json]",
        "resumed_input": "NotRequired[Json]",
    },
    total=True,
)
"""One claimed job, as the executor needs it: everything `AppContext`
construction takes, and nothing the coordinator keeps to itself (no callback
secret, no principal, no resumed-input on a job that never parked)."""

ClosingSoonResponse = TypedDict(
    "ClosingSoonResponse",
    {
        "count": "int",
        "days": "int",
        "grants": "list[Json]",
    },
    total=True,
)
"""`GET /grants/closing-soon`."""

ContractDto = TypedDict(
    "ContractDto",
    {
        "max_row_delta_pct": "NotRequired[Optional[float]]",
        "max_staleness_hours": "NotRequired[Optional[int]]",
        "ranges": "Json",
        "required_fields": "list[str]",
        "types": "Json",
    },
    total=True,
)
"""A source's declared data contract."""

ContractsStatusDto = TypedDict(
    "ContractsStatusDto",
    {
        "catalog_error": "NotRequired[Optional[str]]",
        "catalog_ok": "bool",
        "declared": "int",
        "enforce_configured": "bool",
        "enforce_observed": "bool",
        "reason": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""Whether declared data contracts are being enforced, and whether the catalog
that declares them could even be read."""

CostEventDto = TypedDict(
    "CostEventDto",
    {
        "app": "str",
        "cost_usd": "float",
        "created_at": "str",
        "detail": "NotRequired[Optional[str]]",
        "engine": "str",
        "job_id": "str",
        "url": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One metered engine call (`pumper_core::CostEvent`)."""

CostSummaryResponse = TypedDict(
    "CostSummaryResponse",
    {
        "by_app_engine": "list[CostSummaryRow]",
        "principal": "NotRequired[Optional[str]]",
        "total_usd": "float",
    },
    total=True,
)
"""`GET /costs`."""

CostSummaryRow = TypedDict(
    "CostSummaryRow",
    {
        "app": "str",
        "calls": "int",
        "cost_usd": "float",
        "engine": "str",
    },
    total=True,
)
"""One `(app, engine)` row of the spend ledger."""

CreateDerivedBody = TypedDict(
    "CreateDerivedBody",
    {
        "aggregates": "NotRequired[Optional[Json]]",
        "filters": "NotRequired[Optional[list[str]]]",
        "group_by": "NotRequired[Optional[list[str]]]",
        "lookup": "NotRequired[Union[Json, LookupBody]]",
        "project": "NotRequired[Optional[Json]]",
        "source_app": "str",
        "source_dataset": "str",
        "target_dataset": "str",
    },
    total=True,
)

CreateIngressSourceBody = TypedDict(
    "CreateIngressSourceBody",
    {
        "name": "str",
        "secret": "NotRequired[Optional[str]]",
    },
    total=True,
)

CreatePrincipalBody = TypedDict(
    "CreatePrincipalBody",
    {
        "budget_usd_per_day": "NotRequired[Optional[float]]",
        "name": "str",
        "rate_limit_per_min": "NotRequired[Optional[int]]",
        "scopes": "list[str]",
    },
    total=True,
)

CreateSavedSearchBody = TypedDict(
    "CreateSavedSearchBody",
    {
        "app": "NotRequired[Optional[str]]",
        "dataset": "NotRequired[Optional[str]]",
        "materialize": "NotRequired[Union[Json, MaterializeBody]]",
        "query": "str",
        "secret": "NotRequired[Optional[str]]",
        "url": "str",
    },
    total=True,
)

CreateScheduleBody = TypedDict(
    "CreateScheduleBody",
    {
        "app": "str",
        "budget_usd": "NotRequired[Optional[float]]",
        "cron": "str",
        "max_attempts": "NotRequired[Optional[int]]",
        "misfire_policy": "NotRequired[Optional[str]]",
        "params": "NotRequired[Json]",
        "priority": "NotRequired[Optional[int]]",
        "timezone": "NotRequired[Optional[str]]",
    },
    total=True,
)

CreateSubscriptionBody = TypedDict(
    "CreateSubscriptionBody",
    {
        "from_seq": "NotRequired[Optional[int]]",
        "name": "NotRequired[Optional[str]]",
        "secret": "NotRequired[Optional[str]]",
        "selector": "Json",
        "sink": "NotRequired[Optional[str]]",
        "url": "NotRequired[Optional[str]]",
    },
    total=True,
)

CreateTriggerBody = TypedDict(
    "CreateTriggerBody",
    {
        "bind": "Json",
        "budget_usd": "NotRequired[Optional[float]]",
        "each": "NotRequired[Optional[str]]",
        "filters": "NotRequired[Optional[list[str]]]",
        "max_attempts": "NotRequired[Optional[int]]",
        "name": "NotRequired[Optional[str]]",
        "on_change": "NotRequired[Optional[str]]",
        "on_status": "NotRequired[Optional[str]]",
        "params": "NotRequired[Json]",
        "plugins": "Json",
        "priority": "NotRequired[Optional[int]]",
        "source_app": "str",
        "source_dataset": "NotRequired[Optional[str]]",
        "source_kind": "str",
        "target_app": "str",
    },
    total=True,
)

CreateWatchBody = TypedDict(
    "CreateWatchBody",
    {
        "app": "str",
        "dataset": "NotRequired[Optional[str]]",
        "secret": "NotRequired[Optional[str]]",
        "sink": "NotRequired[Optional[str]]",
        "url": "NotRequired[Optional[str]]",
    },
    total=True,
)

CreateWorkflowBody = TypedDict(
    "CreateWorkflowBody",
    {
        "cron": "NotRequired[Optional[str]]",
        "name": "str",
        "spec": "Json",
    },
    total=True,
)

DatahubStatusResponse = TypedDict(
    "DatahubStatusResponse",
    {
        "emissions": "Json",
        "emit_flows": "bool",
        "emit_profile": "bool",
        "emit_schema": "bool",
        "enabled": "bool",
        "env": "str",
        "gms_url": "str",
        "govern": "Json",
        "last_emission": "NotRequired[Optional[Json]]",
        "lineage": "Json",
        "token_set": "bool",
    },
    total=True,
)
"""`GET /datahub/status` — DataHub and OpenLineage emission state.

Free-form on purpose: the block is assembled from the emitters' own
counters, the governance poller's last summary and per-writer lineage
status, all of which change with those subsystems rather than with this
route. Its keys are `enabled`, `gms_url`, `env`, `token_set`, `emit_schema`,
`emit_profile`, `emit_flows`, `last_emission`, `emissions`, `govern` and
`lineage`."""

DatahubSyncResponse = TypedDict(
    "DatahubSyncResponse",
    {
        "at": "str",
        "datasets": "NotRequired[Optional[int]]",
        "entities": "NotRequired[Optional[int]]",
        "error": "NotRequired[Optional[str]]",
        "flows": "NotRequired[Optional[int]]",
        "kind": "str",
        "ok": "bool",
        "trigger_edges": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""`POST /datahub/sync` — one emission's outcome. `entities` on success and
`error` on failure are mutually exclusive."""

DatasetChangesResponse = TypedDict(
    "DatasetChangesResponse",
    {
        "app": "str",
        "changes": "list[RevisionDto]",
        "count": "int",
        "dataset": "str",
        "trust": "str",
    },
    total=True,
)
"""`GET /datasets/{app}/{dataset}/changes` without `cursor` (legacy shape)."""

DatasetDeletion = TypedDict(
    "DatasetDeletion",
    {
        "app": "str",
        "as_of": "str",
        "dataset": "str",
        "deleted": "int",
        "export": "str",
        "preview": "bool",
        "records": "int",
        "revisions": "int",
    },
    total=True,
)
"""`DELETE /datasets/{app}/{dataset}` once confirmed.

The unconfirmed call answers 428 with a preview instead — deleting a dataset
is the one irreversible door here, so the confirmation is part of the
contract rather than a nicety."""

DatasetManifest = TypedDict(
    "DatasetManifest",
    {
        "app": "str",
        "cap": "int",
        "complete": "bool",
        "count": "int",
        "dataset": "str",
        "digest": "str",
        "keys": "NotRequired[Optional[list[str]]]",
        "live_count": "int",
    },
    total=True,
)
"""`GET /datasets/{app}/{dataset}/manifest` — the digest a mesh peer reconciles
against."""

DeleteDocsBody = TypedDict(
    "DeleteDocsBody",
    {
        "ids": "list[str]",
    },
    total=True,
)

DeletedResponse = TypedDict(
    "DeletedResponse",
    {
        "deleted": "bool",
    },
    total=True,
)
"""`{deleted: true}` — the answer every idempotent delete door gives."""

DeliveryDto = TypedDict(
    "DeliveryDto",
    {
        "attempts": "int",
        "body": "NotRequired[Optional[str]]",
        "created_at": "str",
        "event": "str",
        "id": "str",
        "kind": "str",
        "last_error": "NotRequired[Optional[str]]",
        "ref_id": "str",
        "status": "str",
        "updated_at": "str",
        "url": "str",
    },
    total=True,
)
"""One delivery attempt of a webhook / watch / subscription sink."""

DeliveryListResponse = TypedDict(
    "DeliveryListResponse",
    {
        "count": "int",
        "deliveries": "list[DeliveryDto]",
    },
    total=True,
)
"""`GET /webhooks/deliveries` without `cursor` (legacy shape)."""

DeliveryPage = TypedDict(
    "DeliveryPage",
    {
        "items": "list[DeliveryDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""A keyset page of deliveries."""

DeliveryReplayResponse = TypedDict(
    "DeliveryReplayResponse",
    {
        "id": "str",
        "replaying": "bool",
    },
    total=True,
)
"""`POST /webhooks/deliveries/{id}/replay`."""

DerivedBackfillResponse = TypedDict(
    "DerivedBackfillResponse",
    {
        "changed": "int",
        "cursor": "NotRequired[Optional[str]]",
        "done": "bool",
        "matched": "int",
        "new": "int",
        "scanned": "int",
        "unchanged": "int",
    },
    total=True,
)
"""`POST /derived/{id}/backfill` — one bounded pass."""

DerivedDeleted = TypedDict(
    "DerivedDeleted",
    {
        "deleted": "str",
    },
    total=True,
)
"""`DELETE /derived/{id}` — answers with the id, not a bool, unlike the other
delete doors. Kept as served."""

DerivedGroupDto = TypedDict(
    "DerivedGroupDto",
    {
        "aggregates": "Json",
        "group_by": "list[str]",
    },
    total=True,
)
"""The `group` clause of a derived spec."""

DerivedListResponse = TypedDict(
    "DerivedListResponse",
    {
        "specs": "list[DerivedSpecDto]",
    },
    total=True,
)
"""`GET /derived`."""

DerivedLookupDto = TypedDict(
    "DerivedLookupDto",
    {
        "dataset": "str",
        "key_expr": "str",
        "merge_as": "str",
    },
    total=True,
)
"""The `lookup` clause of a derived spec."""

DerivedSpecDto = TypedDict(
    "DerivedSpecDto",
    {
        "created_at": "str",
        "enabled": "bool",
        "filters": "list[str]",
        "group": "NotRequired[Union[Json, DerivedGroupDto]]",
        "id": "str",
        "lookup": "NotRequired[Union[Json, DerivedLookupDto]]",
        "project": "Json",
        "source_app": "str",
        "source_dataset": "str",
        "target_dataset": "str",
    },
    total=True,
)
"""One derived-dataset spec."""

DoctorArtifacts = TypedDict(
    "DoctorArtifacts",
    {
        "bodies_checked": "NotRequired[Optional[int]]",
        "check_limit": "NotRequired[Optional[int]]",
        "per_app": "NotRequired[Optional[list[AppReclaimDto]]]",
        "root": "NotRequired[Optional[str]]",
        "scanned": "bool",
        "total_bytes": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""The doctor's artifact scan; `{scanned: false}` and nothing else when
`?skip_artifacts=true`, so every other field is optional."""

DoctorCoverage = TypedDict(
    "DoctorCoverage",
    {
        "app": "str",
        "dataset": "str",
        "replayable": "int",
        "revisions": "int",
        "with_job_id": "int",
    },
    total=True,
)
"""Provenance coverage for one dataset."""

DoctorFinding = TypedDict(
    "DoctorFinding",
    {
        "check": "str",
        "count": "int",
        "examples": "list[Json]",
        "remediation": "str",
        "severity": "str",
        "summary": "str",
    },
    total=True,
)
"""One store-integrity finding."""

DoctorReport = TypedDict(
    "DoctorReport",
    {
        "artifacts": "DoctorArtifacts",
        "coverage": "list[DoctorCoverage]",
        "findings": "list[DoctorFinding]",
        "generated_at": "str",
        "healthy": "bool",
        "read_only": "bool",
        "search": "NotRequired[Union[Json, DoctorSearch]]",
        "store": "Json",
        "tables": "list[DoctorTable]",
        "thresholds": "DoctorThresholds",
    },
    total=True,
)
"""`GET /datasets/doctor` — read-only store integrity."""

DoctorSearch = TypedDict(
    "DoctorSearch",
    {
        "doc_count": "NotRequired[Optional[int]]",
        "enabled": "bool",
        "live_records": "int",
    },
    total=True,
)
"""Whether the search index and the store agree about how much exists."""

DoctorTable = TypedDict(
    "DoctorTable",
    {
        "config_key": "str",
        "oldest_days": "NotRequired[Optional[int]]",
        "retention_days": "int",
        "rows": "int",
        "table": "str",
    },
    total=True,
)
"""One append-only table and the retention that is (or is not) bounding it."""

DoctorThresholds = TypedDict(
    "DoctorThresholds",
    {
        "unbounded_growth_days": "int",
    },
    total=True,
)
"""The doctor's `thresholds` block: the constants its verdicts were judged by,
so a reader can tell a finding from a policy."""

DupPairDto = TypedDict(
    "DupPairDto",
    {
        "a": "str",
        "b": "str",
        "distance": "int",
    },
    total=True,
)
"""One near-duplicate pair and its SimHash Hamming distance."""

DuplicatesResponse = TypedDict(
    "DuplicatesResponse",
    {
        "app": "str",
        "dataset": "str",
        "max_distance": "int",
        "pairs": "list[DupPairDto]",
    },
    total=True,
)
"""`GET /datasets/{app}/{dataset}/duplicates`."""

EconomicsAdvice = TypedDict(
    "EconomicsAdvice",
    {
        "app": "str",
        "cadence": "str",
        "reason": "str",
        "recommended_budget_usd": "NotRequired[Optional[float]]",
        "weight": "float",
    },
    total=True,
)
"""One budget/cadence recommendation."""

EconomicsApp = TypedDict(
    "EconomicsApp",
    {
        "app": "str",
        "changed": "NotRequired[Optional[int]]",
        "claude": "EconomicsClaude",
        "cost_per_changed_usd": "NotRequired[Optional[float]]",
        "cost_per_new_usd": "NotRequired[Optional[float]]",
        "cost_usd": "float",
        "datasets": "list[EconomicsDataset]",
        "engine_calls": "int",
        "jobs_with_yield": "int",
        "new": "NotRequired[Optional[int]]",
        "unchanged": "NotRequired[Optional[int]]",
        "weight": "float",
        "weighted_fresh_per_dollar": "NotRequired[Optional[float]]",
    },
    total=True,
)
"""One app's economics inside one window."""

EconomicsByPrincipal = TypedDict(
    "EconomicsByPrincipal",
    {
        "all_time": "list[PrincipalCostRow]",
        "rows": "list[PrincipalCostRow]",
        "window": "str",
    },
    total=True,
)
"""The `by_principal` block of `GET /economics`."""

EconomicsClaude = TypedDict(
    "EconomicsClaude",
    {
        "calls": "int",
        "cost_usd": "float",
        "records_per_dollar": "NotRequired[Optional[float]]",
        "worth_it": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""Claude spend for one app in one window, and whether it paid for itself."""

EconomicsDataset = TypedDict(
    "EconomicsDataset",
    {
        "changed": "NotRequired[Optional[int]]",
        "dataset": "str",
        "jobs": "int",
        "new": "NotRequired[Optional[int]]",
        "removed": "NotRequired[Optional[int]]",
        "unchanged": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""One dataset's yield inside an economics window."""

EconomicsReport = TypedDict(
    "EconomicsReport",
    {
        "advice": "list[EconomicsAdvice]",
        "by_principal": "EconomicsByPrincipal",
        "enforce": "bool",
        "windows": "EconomicsWindows",
    },
    total=True,
)
"""`GET /economics`."""

EconomicsWindow = TypedDict(
    "EconomicsWindow",
    {
        "apps": "list[EconomicsApp]",
        "days": "int",
    },
    total=True,
)
"""One rolling window of the economics report."""

EconomicsWindows = TypedDict(
    "EconomicsWindows",
    {
        "30d": "EconomicsWindow",
        "7d": "EconomicsWindow",
    },
    total=True,
)
"""The two windows the report always carries."""

EnabledBody = TypedDict(
    "EnabledBody",
    {
        "enabled": "bool",
    },
    total=True,
)

EnabledResponse = TypedDict(
    "EnabledResponse",
    {
        "enabled": "bool",
        "id": "str",
    },
    total=True,
)
"""`{id, enabled}` — the answer every `POST .../enabled` toggle gives."""

EnforcementPreview = TypedDict(
    "EnforcementPreview",
    {
        "enforcing": "bool",
        "not_ready": "list[PreviewNotReady]",
        "ready": "bool",
        "runs_per_source": "int",
        "sources": "list[PreviewSource]",
        "sources_replayed": "int",
        "totals": "PreviewConsequencesDto",
        "unmonitored": "list[str]",
    },
    total=True,
)
"""`GET /enforcement/preview` — what `[resilience] enforce = true` would have
done. Gates nothing, writes nothing."""

EnqueueBody = TypedDict(
    "EnqueueBody",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "callback_secret": "NotRequired[Optional[str]]",
        "callback_url": "NotRequired[Optional[str]]",
        "delay_secs": "NotRequired[Optional[int]]",
        "idempotency_key": "NotRequired[Optional[str]]",
        "max_attempts": "NotRequired[Optional[int]]",
        "params": "NotRequired[Json]",
        "priority": "NotRequired[Optional[int]]",
    },
    total=True,
)

ErrorEnvelope = TypedDict(
    "ErrorEnvelope",
    {
        "code": "str",
        "error": "str",
    },
    total=True,
)
"""The error envelope every 4xx/5xx carries (`routes::error::ApiError`).

`code` is the stable machine token clients branch on; `error` is the human
sentence, which is NOT stable and must never be parsed."""

EventLogPage = TypedDict(
    "EventLogPage",
    {
        "count": "int",
        "events": "list[EventRecordDto]",
        "latest_seq": "int",
        "next_after": "NotRequired[Optional[int]]",
        "pending": "int",
        "retained": "int",
        "retention_days": "int",
    },
    total=True,
)
"""`GET /events/log` — the pull page of the event log."""

EventRecordDto = TypedDict(
    "EventRecordDto",
    {
        "app": "str",
        "created_at": "str",
        "kind": "str",
        "payload": "Json",
        "seq": "int",
        "subject_id": "str",
    },
    total=True,
)
"""One row of the durable event log."""

ExecutorCheckpointResponse = TypedDict(
    "ExecutorCheckpointResponse",
    {
        "saved": "bool",
    },
    total=True,
)
"""`POST /jobs/{id}/checkpoint`."""

ExecutorDto = TypedDict(
    "ExecutorDto",
    {
        "capabilities": "list[str]",
        "claimed_total": "int",
        "first_seen_at": "str",
        "id": "str",
        "last_poll_age_secs": "int",
        "last_poll_at": "str",
        "running": "int",
        "state": "str",
    },
    total=True,
)
"""One outbound executor."""

ExecutorFinishResponse = TypedDict(
    "ExecutorFinishResponse",
    {
        "outcome": "str",
    },
    total=True,
)
"""`POST /jobs/{id}/finish` — what the coordinator did with the report."""

ExecutorListResponse = TypedDict(
    "ExecutorListResponse",
    {
        "eligible_apps": "list[str]",
        "executors": "list[ExecutorDto]",
    },
    total=True,
)
"""`GET /executors`."""

ExecutorOwned = TypedDict(
    "ExecutorOwned",
    {
        "owned": "bool",
    },
    total=True,
)
"""`POST /jobs/{id}/heartbeat` — always `true`; a lost lease answers 409."""

ExecutorProgressResponse = TypedDict(
    "ExecutorProgressResponse",
    {
        "reported": "bool",
    },
    total=True,
)
"""`POST /jobs/{id}/progress`."""

ExecutorWrite = TypedDict(
    "ExecutorWrite",
    {
        "attempt": "int",
        "error": "NotRequired[Optional[str]]",
        "executor_id": "str",
        "result": "NotRequired[Json]",
        "run_ms": "NotRequired[Optional[int]]",
        "state": "NotRequired[Json]",
    },
    total=True,
)
"""What every executor-driven write names: the job's attempt and the executor
that claims to hold it. Both are required — the attempt is the fence the
local worker already lives under, and the executor id is what makes a
*reaped* process's late write refusable rather than merely unlucky."""

ExtractPreviewResponse = TypedDict(
    "ExtractPreviewResponse",
    {
        "fields_matched": "int",
        "fields_total": "int",
        "report": "Json",
        "values": "Json",
    },
    total=True,
)
"""`POST /extract/preview` — a RuleSet dry run against one document."""

FetchProxyResponse = TypedDict(
    "FetchProxyResponse",
    {
        "body": "str",
        "cache_hit": "bool",
        "final_url": "str",
        "headers": "Json",
        "status": "int",
    },
    total=True,
)
"""`POST /fetch-proxy` — one fetch performed through this node's local stack on
a peer's behalf."""

GovernDisableSchedule = TypedDict(
    "GovernDisableSchedule",
    {
        "app": "str",
        "dataset": "str",
        "evidence": "str",
        "note": "str",
        "schedule_ids": "list[str]",
        "suppressed": "bool",
    },
    total=True,
)
"""A schedule the governance poll would disable."""

GovernEnqueueSync = TypedDict(
    "GovernEnqueueSync",
    {
        "app": "str",
        "dataset": "str",
        "evidence": "str",
        "idempotency_key": "str",
        "note": "str",
        "registered": "bool",
    },
    total=True,
)
"""A sync the governance poll would enqueue."""

GovernTotals = TypedDict(
    "GovernTotals",
    {
        "apps_paused": "int",
        "read_errors": "int",
        "schedules_disabled": "int",
        "syncs_enqueued": "int",
    },
    total=True,
)
"""Counts of what the poll would do."""

GovernWould = TypedDict(
    "GovernWould",
    {
        "disable_schedules": "list[GovernDisableSchedule]",
        "enqueue_syncs": "list[GovernEnqueueSync]",
        "pause_apps": "list[str]",
        "resume_apps": "list[str]",
    },
    total=True,
)
"""Everything the governance poll would do."""

GovernancePreview = TypedDict(
    "GovernancePreview",
    {
        "at": "str",
        "budget_secs": "int",
        "datasets_polled": "int",
        "env": "str",
        "gms_url": "str",
        "governing": "bool",
        "paused_now": "list[str]",
        "poll_ms": "int",
        "poll_would_abort": "bool",
        "quiet": "bool",
        "read_errors": "list[str]",
        "totals": "GovernTotals",
        "would": "GovernWould",
    },
    total=True,
)
"""`GET /datahub/governance/preview` — what the poll would do right now.
Writes nothing."""

GrantListResponse = TypedDict(
    "GrantListResponse",
    {
        "grants": "list[RecordDto]",
    },
    total=True,
)
"""`GET /grants` without `cursor` (legacy shape)."""

HealthResponse = TypedDict(
    "HealthResponse",
    {
        "status": "str",
    },
    total=True,
)
"""`GET /health`."""

HostListResponse = TypedDict(
    "HostListResponse",
    {
        "hosts": "list[HostProfileDto]",
    },
    total=True,
)
"""`GET /hosts` without `cursor` (legacy shape)."""

HostMemoryReset = TypedDict(
    "HostMemoryReset",
    {
        "host": "str",
        "reset": "bool",
    },
    total=True,
)
"""`DELETE /hosts/{host}/memory`."""

HostPage = TypedDict(
    "HostPage",
    {
        "items": "list[HostProfileDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /hosts` with `cursor`."""

HostProfileDto = TypedDict(
    "HostProfileDto",
    {
        "host": "str",
        "http_strikes": "int",
        "observations": "NotRequired[Optional[int]]",
        "penalty_ms": "int",
        "penalty_updated_at": "NotRequired[Optional[str]]",
        "preferred_tier": "NotRequired[Optional[str]]",
        "updated_at": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One host's learned tier memory and politeness penalty."""

HostWeatherImportResponse = TypedDict(
    "HostWeatherImportResponse",
    {
        "actions": "list[WeatherPlanDto]",
        "applied": "bool",
        "changed": "int",
        "considered": "int",
        "noops": "int",
        "schema": "str",
        "source_node_id": "NotRequired[Optional[str]]",
        "verified": "bool",
    },
    total=True,
)
"""`POST /host-weather/import`. An import only ever RAISES local caution — it
cannot make this node less polite than it already decided to be."""

HostWeatherLegacyBundle = TypedDict(
    "HostWeatherLegacyBundle",
    {
        "entries": "list[WeatherEntryDto]",
        "generated_at": "str",
        "min_observations": "int",
        "node_id": "str",
        "schema": "str",
    },
    total=True,
)
"""`GET /host-weather/export?schema=1` — the legacy flat bundle, which is
UNSIGNED. Kept so an older peer keeps working; new consumers should read the
signed `SignedBundle` the default answer carries."""

IngestResponse = TypedDict(
    "IngestResponse",
    {
        "event_id": "str",
        "seq": "int",
        "triggers_fired": "int",
    },
    total=True,
)
"""`POST /ingest/{id}` — one accepted external event."""

IngressSourceCreated = TypedDict(
    "IngressSourceCreated",
    {
        "secret": "str",
        "source": "IngressSourceDto",
    },
    total=True,
)
"""`POST /ingress/sources` — the only response that ever carries the secret."""

IngressSourceDto = TypedDict(
    "IngressSourceDto",
    {
        "created_at": "str",
        "enabled": "bool",
        "id": "str",
        "name": "str",
    },
    total=True,
)
"""One inbound webhook source. `secret` is returned exactly once, at creation."""

IngressSourceListResponse = TypedDict(
    "IngressSourceListResponse",
    {
        "count": "int",
        "sources": "list[IngressSourceDto]",
    },
    total=True,
)
"""`GET /ingress/sources`."""

JobCostsResponse = TypedDict(
    "JobCostsResponse",
    {
        "app": "str",
        "calls": "int",
        "cost_per_fresh_record_usd": "NotRequired[Optional[float]]",
        "events": "list[CostEventDto]",
        "fresh_records": "NotRequired[Optional[int]]",
        "job_id": "str",
        "total_usd": "float",
    },
    total=True,
)
"""`GET /jobs/{id}/costs`."""

JobDto = TypedDict(
    "JobDto",
    {
        "app": "str",
        "attempts": "int",
        "available_at": "str",
        "budget_usd": "NotRequired[Optional[float]]",
        "callback_url": "NotRequired[Optional[str]]",
        "created_at": "str",
        "error": "NotRequired[Optional[str]]",
        "executor_id": "NotRequired[Optional[str]]",
        "finished_at": "NotRequired[Optional[str]]",
        "id": "str",
        "input_request": "NotRequired[Optional[Json]]",
        "max_attempts": "int",
        "params": "Json",
        "priority": "int",
        "result": "NotRequired[Optional[Json]]",
        "schedule_id": "NotRequired[Optional[str]]",
        "started_at": "NotRequired[Optional[str]]",
        "status": "str",
        "trigger_id": "NotRequired[Optional[str]]",
        "waiting_expires_at": "NotRequired[Optional[str]]",
        "waiting_since": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One queued/running/finished job (`pumper_core::Job`). `callback_secret` and
`resumed_input` are `skip_serializing` on the core struct and are absent from
the wire, so they are absent here."""

JobPage = TypedDict(
    "JobPage",
    {
        "items": "list[JobDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /jobs` without `cursor` is a bare `[Job]`; with it, this envelope."""

JobReceipt = TypedDict(
    "JobReceipt",
    {
        "artifacts": "NotRequired[Union[Json, ReceiptArtifacts]]",
        "changes": "list[ReceiptChange]",
        "cost": "ReceiptCost",
        "deliveries": "list[DeliveryDto]",
        "job": "ReceiptJob",
        "stages": "NotRequired[Union[Json, ReceiptStages]]",
        "trigger_hops": "list[ReceiptTriggerHop]",
        "unknown": "list[str]",
        "verdicts": "ReceiptVerdicts",
        "yield": "list[YieldEntryDto]",
    },
    total=True,
)
"""`GET /jobs/{id}/receipt` — one job's whole story."""

LookupBody = TypedDict(
    "LookupBody",
    {
        "dataset": "str",
        "key_expr": "str",
        "merge_as": "str",
    },
    total=True,
)
"""Body twin of core's [`DerivedLookup`] (which doesn't carry a utoipa schema)."""

MarketProfileResponse = TypedDict(
    "MarketProfileResponse",
    {
        "coverage": "str",
        "density": "NotRequired[Optional[Json]]",
        "economics": "Json",
        "state": "str",
        "trade": "str",
    },
    total=True,
)
"""`GET /market/profile/{state}/{trade}` — the record's `data`, unwrapped.

The row is an app-owned cross-family join (economics x density) whose blocks
evolve with the apps that write it, so the payload is free-form here rather
than a schema this route does not enforce. `coverage` is `both` or
`economics_only`, and `density` is `null` for the latter — an absent join,
never a fabricated density."""

MaterializeBody = TypedDict(
    "MaterializeBody",
    {
        "app": "str",
        "dataset": "str",
    },
    total=True,
)

MeshPeer = TypedDict(
    "MeshPeer",
    {
        "allow_unsigned": "bool",
        "enabled": "bool",
        "every": "str",
        "every_secs": "int",
        "key_pinned": "bool",
        "name": "str",
        "streams": "list[MeshStream]",
        "url": "str",
    },
    total=True,
)
"""One configured peer."""

MeshResponse = TypedDict(
    "MeshResponse",
    {
        "node_id": "str",
        "peers": "list[MeshPeer]",
        "totals": "MeshTotalsDto",
    },
    total=True,
)
"""`GET /mesh`."""

MeshStream = TypedDict(
    "MeshStream",
    {
        "detail": "NotRequired[Optional[Json]]",
        "ghosts_removed": "int",
        "lag_secs": "NotRequired[Optional[int]]",
        "last_attempt_at": "NotRequired[Optional[str]]",
        "last_success_at": "NotRequired[Optional[str]]",
        "ok": "NotRequired[Optional[bool]]",
        "pulls": "int",
        "schedule_id": "str",
        "scheduled": "bool",
        "signature_failures": "int",
        "stream": "str",
        "verified": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""One sync stream with one peer."""

MeshTotalsDto = TypedDict(
    "MeshTotalsDto",
    {
        "ghosts_removed": "int",
        "peers": "int",
        "pulls": "int",
        "signature_failures": "int",
        "streams": "int",
    },
    total=True,
)
"""Fleet-wide mesh counters."""

NodeResponse = TypedDict(
    "NodeResponse",
    {
        "algo": "str",
        "key_created": "bool",
        "key_path": "str",
        "legacy_id": "str",
        "node_id": "str",
        "public_key": "str",
    },
    total=True,
)
"""`GET /node` — this node's signing identity."""

PluginListResponse = TypedDict(
    "PluginListResponse",
    {
        "plugins": "list[Json]",
    },
    total=True,
)
"""`GET /plugins`."""

PluginReloadResponse = TypedDict(
    "PluginReloadResponse",
    {
        "loaded": "int",
    },
    total=True,
)
"""`POST /plugins/reload`."""

PreviewBody = TypedDict(
    "PreviewBody",
    {
        "base_url": "NotRequired[Optional[str]]",
        "html": "NotRequired[Optional[str]]",
        "rules": "Json",
        "url": "NotRequired[Optional[str]]",
    },
    total=True,
)

PreviewConsequencesDto = TypedDict(
    "PreviewConsequencesDto",
    {
        "diverted_writes": "PreviewRunCount",
        "skipped_index_writes": "PreviewRunCount",
        "suppressed_pushes": "PreviewRunCount",
        "trust_stamped": "PreviewRunCount",
        "withheld_removals": "PreviewRunCount",
    },
    total=True,
)
"""What enforcement WOULD have done, counted per consequence."""

PreviewNotReady = TypedDict(
    "PreviewNotReady",
    {
        "gates": "list[str]",
        "id": "str",
        "since": "NotRequired[Union[Json, PreviewTransitionDto]]",
        "state": "str",
    },
    total=True,
)
"""One source that is NOT ready for enforcement, and which gates it fails."""

PreviewRunCount = TypedDict(
    "PreviewRunCount",
    {
        "docs": "int",
        "runs": "int",
    },
    total=True,
)
"""Runs and documents one consequence would have touched."""

PreviewSource = TypedDict(
    "PreviewSource",
    {
        "consequences": "PreviewConsequencesDto",
        "gates": "list[str]",
        "id": "str",
        "live_state": "str",
        "monitored": "bool",
        "runs_replayed": "int",
        "state": "str",
        "transitions": "list[PreviewTransitionDto]",
        "unjudged_runs": "int",
        "window_opens_at": "NotRequired[Optional[str]]",
        "window_opens_in": "str",
    },
    total=True,
)
"""One source's enforcement preview."""

PreviewTransitionDto = TypedDict(
    "PreviewTransitionDto",
    {
        "at": "str",
        "cause": "str",
        "diagnosis": "NotRequired[Optional[str]]",
        "from": "str",
        "gates": "list[str]",
        "job_id": "str",
        "reasons": "NotRequired[Optional[Json]]",
        "score": "float",
        "to": "str",
        "verdict": "str",
    },
    total=True,
)
"""One state transition the replay would have made."""

PrincipalCostRow = TypedDict(
    "PrincipalCostRow",
    {
        "calls": "int",
        "cost_usd": "float",
        "principal": "str",
        "principal_id": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One caller's spend."""

PrincipalCostsResponse = TypedDict(
    "PrincipalCostsResponse",
    {
        "by_principal": "list[PrincipalCostRow]",
        "total_usd": "float",
    },
    total=True,
)
"""`GET /principals/costs`."""

PrincipalCreated = TypedDict(
    "PrincipalCreated",
    {
        "key": "str",
        "principal": "PrincipalDto",
    },
    total=True,
)
"""`POST /principals` — the only response that carries a new key."""

PrincipalDisabled = TypedDict(
    "PrincipalDisabled",
    {
        "enabled": "bool",
        "id": "str",
    },
    total=True,
)
"""`POST /principals/{id}/disable`."""

PrincipalDto = TypedDict(
    "PrincipalDto",
    {
        "budget_usd_per_day": "NotRequired[Optional[float]]",
        "created_at": "str",
        "enabled": "bool",
        "id": "str",
        "name": "str",
        "rate_limit_per_min": "NotRequired[Optional[int]]",
        "scopes": "list[str]",
    },
    total=True,
)
"""One API principal. `key_hash` is never serialized, and the plaintext key
exists in exactly two responses: creation and rotation."""

PrincipalListResponse = TypedDict(
    "PrincipalListResponse",
    {
        "caller": "NotRequired[Union[Json, CallerPrincipalDto]]",
        "count": "int",
        "mode": "str",
        "principals": "list[PrincipalDto]",
    },
    total=True,
)
"""`GET /principals`."""

PrincipalRotated = TypedDict(
    "PrincipalRotated",
    {
        "id": "str",
        "key": "str",
    },
    total=True,
)
"""`POST /principals/{id}/rotate` — the old key stops working immediately."""

ProfileInfoDto = TypedDict(
    "ProfileInfoDto",
    {
        "has_browser_dir": "bool",
        "has_cookies": "bool",
        "last_used": "NotRequired[Optional[str]]",
        "name": "str",
    },
    total=True,
)
"""One named login profile. Cookies and browser state are never returned."""

ProfileListResponse = TypedDict(
    "ProfileListResponse",
    {
        "profiles": "list[ProfileInfoDto]",
    },
    total=True,
)
"""`GET /profiles` — the session vault's named login profiles."""

ProgramListResponse = TypedDict(
    "ProgramListResponse",
    {
        "programs": "list[RecordDto]",
    },
    total=True,
)
"""`GET /grants/programs` without `cursor` (legacy shape)."""

ProposalPage = TypedDict(
    "ProposalPage",
    {
        "items": "list[ProposalSummary]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /provisioner/proposals` with `cursor`."""

ProposalPromotion = TypedDict(
    "ProposalPromotion",
    {
        "catalog_toml": "str",
        "key": "str",
        "status": "str",
    },
    total=True,
)
"""`POST /provisioner/proposals/{key}/promote` — hands back the catalog block
to paste. This route NEVER writes `catalog/data-sources.toml` itself."""

ProposalSummary = TypedDict(
    "ProposalSummary",
    {
        "accepted": "NotRequired[Optional[Json]]",
        "age_secs": "int",
        "catalog_confidence": "NotRequired[Optional[Json]]",
        "engine": "NotRequired[Optional[Json]]",
        "expired": "bool",
        "first_seen": "str",
        "intended_dataset": "NotRequired[Optional[Json]]",
        "key": "str",
        "prompt": "NotRequired[Optional[Json]]",
        "status": "str",
        "updated_at": "str",
        "url": "NotRequired[Optional[Json]]",
        "verdict": "NotRequired[Optional[Json]]",
    },
    total=True,
)
"""One compiled proposal awaiting validation or promotion."""

ProposalValidation = TypedDict(
    "ProposalValidation",
    {
        "key": "str",
        "status": "str",
        "validation": "Json",
    },
    total=True,
)
"""`POST /provisioner/proposals/{key}/validate` — a real fetch and a real dry
run, with the verdict written back onto the proposal."""

ProvenanceCoverage = TypedDict(
    "ProvenanceCoverage",
    {
        "replayable": "int",
        "revisions": "int",
        "with_job": "int",
    },
    total=True,
)
"""How much of a record's history can be replayed."""

ProvenanceJob = TypedDict(
    "ProvenanceJob",
    {
        "app": "str",
        "created_at": "str",
        "schedule_id": "NotRequired[Optional[str]]",
        "status": "str",
        "trigger_id": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""The job that wrote one revision; `null` when the id is unknown or unparseable."""

ProvenanceLink = TypedDict(
    "ProvenanceLink",
    {
        "change": "str",
        "created_at": "str",
        "job": "NotRequired[Union[Json, ProvenanceJob]]",
        "provenance": "ProvenanceStamp",
        "revision": "int",
        "trust": "str",
    },
    total=True,
)
"""One link of a record's derivation chain."""

ProvenanceResponse = TypedDict(
    "ProvenanceResponse",
    {
        "app": "str",
        "chain": "list[ProvenanceLink]",
        "coverage": "ProvenanceCoverage",
        "dataset": "str",
        "key": "str",
        "removed_at": "NotRequired[Optional[str]]",
        "trust": "str",
    },
    total=True,
)
"""`GET /provenance/{app}/{dataset}/{key}`."""

ProvenanceStamp = TypedDict(
    "ProvenanceStamp",
    {
        "artifact_sha": "NotRequired[Optional[str]]",
        "job_id": "NotRequired[Optional[str]]",
        "replayable": "bool",
        "rules_hash": "NotRequired[Optional[str]]",
        "source_url": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""The derivation stamp on one revision."""

ReceiptArtifactFile = TypedDict(
    "ReceiptArtifactFile",
    {
        "bytes": "int",
        "name": "str",
    },
    total=True,
)
"""One retained artifact file."""

ReceiptArtifacts = TypedDict(
    "ReceiptArtifacts",
    {
        "count": "int",
        "dir": "str",
        "files": "list[ReceiptArtifactFile]",
        "total_bytes": "int",
        "truncated": "bool",
    },
    total=True,
)
"""The artifacts block of a receipt; `null` when the directory is missing or
unreadable — stated rather than reported as an empty run."""

ReceiptChange = TypedDict(
    "ReceiptChange",
    {
        "app": "str",
        "by_change": "Json",
        "dataset": "str",
        "total": "int",
    },
    total=True,
)
"""What one dataset actually changed during this job."""

ReceiptCost = TypedDict(
    "ReceiptCost",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "by_engine": "list[ReceiptEngineCost]",
        "calls": "int",
        "egress": "list[ReceiptEgress]",
        "self_hosted_fetches": "int",
        "total_usd": "float",
    },
    total=True,
)
"""The cost block of a receipt."""

ReceiptEgress = TypedDict(
    "ReceiptEgress",
    {
        "calls": "int",
        "node": "str",
    },
    total=True,
)
"""Fetches this job pushed out through a mesh peer."""

ReceiptEngineCost = TypedDict(
    "ReceiptEngineCost",
    {
        "calls": "int",
        "cost_usd": "float",
        "engine": "str",
    },
    total=True,
)
"""Spend by engine within one job."""

ReceiptHealthVerdict = TypedDict(
    "ReceiptHealthVerdict",
    {
        "diagnosis": "NotRequired[Optional[str]]",
        "score": "float",
        "source_id": "str",
        "state_after": "str",
        "verdict": "str",
    },
    total=True,
)
"""One source's extraction-health verdict for this job."""

ReceiptJob = TypedDict(
    "ReceiptJob",
    {
        "app": "str",
        "attempts": "int",
        "created_at": "str",
        "error": "NotRequired[Optional[str]]",
        "executor_id": "NotRequired[Optional[str]]",
        "finished_at": "NotRequired[Optional[str]]",
        "id": "str",
        "max_attempts": "int",
        "schedule_id": "NotRequired[Optional[str]]",
        "started_at": "NotRequired[Optional[str]]",
        "status": "str",
        "trigger_id": "NotRequired[Optional[str]]",
        "wall_ms": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""The job block of a receipt — a deliberate subset of `Job` plus `wall_ms`."""

ReceiptStages = TypedDict(
    "ReceiptStages",
    {
        "alerts_ms": "NotRequired[Optional[int]]",
        "attempt": "int",
        "hooks_ms": "NotRequired[Optional[int]]",
        "index_ms": "NotRequired[Optional[int]]",
        "run_ms": "NotRequired[Optional[int]]",
        "total_ms": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""Per-stage timings for one attempt."""

ReceiptTriggerHop = TypedDict(
    "ReceiptTriggerHop",
    {
        "app": "str",
        "created_at": "str",
        "job_id": "str",
        "status": "str",
        "trigger_id": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One downstream job this job's completion triggered."""

ReceiptVerdicts = TypedDict(
    "ReceiptVerdicts",
    {
        "contracts": "list[Json]",
        "health": "list[ReceiptHealthVerdict]",
    },
    total=True,
)
"""The verdict block of a receipt."""

RecipeDto = TypedDict(
    "RecipeDto",
    {
        "consecutive_failures": "int",
        "discovered_at": "str",
        "host": "str",
        "id": "str",
        "json_paths": "NotRequired[Optional[Json]]",
        "last_seen_at": "str",
        "params": "NotRequired[Optional[Json]]",
        "score": "float",
        "url_template": "str",
        "validated": "bool",
        "validated_at": "NotRequired[Optional[str]]",
        "validation_reason": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One discovered JSON-API endpoint behind a rendered page (N14)."""

RecipeImportResponse = TypedDict(
    "RecipeImportResponse",
    {
        "applied": "bool",
        "considered": "int",
        "imported": "int",
        "notes": "list[str]",
        "skipped": "int",
        "source_node_id": "NotRequired[Optional[str]]",
        "validated": "bool",
        "verified": "bool",
    },
    total=True,
)
"""`POST /recipes/import`."""

RecipeListResponse = TypedDict(
    "RecipeListResponse",
    {
        "recipes": "list[RecipeDto]",
    },
    total=True,
)
"""`GET /recipes`."""

ReconcileApplied = TypedDict(
    "ReconcileApplied",
    {
        "created": "int",
        "disabled": "int",
        "errors": "list[str]",
        "orphans_untouched": "int",
        "updated": "int",
    },
    total=True,
)
"""What applying the plan actually did."""

ReconcileApplyResponse = TypedDict(
    "ReconcileApplyResponse",
    {
        "applied": "ReconcileApplied",
        "plan": "ReconcilePlanDto",
    },
    total=True,
)
"""`POST /catalog/reconcile`."""

ReconcileCreate = TypedDict(
    "ReconcileCreate",
    {
        "app": "str",
        "cron": "str",
        "source_id": "str",
    },
    total=True,
)
"""A schedule the catalog says should exist and does not."""

ReconcileDisable = TypedDict(
    "ReconcileDisable",
    {
        "app": "str",
        "reason": "str",
        "schedule_id": "str",
    },
    total=True,
)
"""A schedule the catalog no longer wants."""

ReconcileOrphan = TypedDict(
    "ReconcileOrphan",
    {
        "app": "str",
        "reason": "str",
        "schedule_id": "str",
    },
    total=True,
)
"""A schedule reconcile will NOT touch, and why. Orphans are reported rather
than deleted: a schedule nobody claims may still be one somebody made."""

ReconcilePlanDto = TypedDict(
    "ReconcilePlanDto",
    {
        "auto_reconcile": "NotRequired[Optional[bool]]",
        "covered_by_untagged": "int",
        "create": "list[ReconcileCreate]",
        "disable": "list[ReconcileDisable]",
        "empty": "NotRequired[Optional[bool]]",
        "in_sync": "int",
        "orphan": "list[ReconcileOrphan]",
        "update": "list[ReconcileUpdate]",
    },
    total=True,
)
"""`GET /catalog/reconcile` — the plan. Changes nothing."""

ReconcileUpdate = TypedDict(
    "ReconcileUpdate",
    {
        "app": "str",
        "from_cron": "str",
        "re_enable": "bool",
        "schedule_id": "str",
        "to_cron": "str",
    },
    total=True,
)
"""A schedule whose cron drifted from the catalog's."""

RecordDto = TypedDict(
    "RecordDto",
    {
        "data": "Json",
        "first_seen": "str",
        "key": "str",
        "last_seen": "str",
        "removed_at": "NotRequired[Optional[str]]",
        "trust": "str",
        "updated_at": "str",
    },
    total=True,
)
"""One stored record (`pumper_core::datasets::Record`)."""

RecordHistoryResponse = TypedDict(
    "RecordHistoryResponse",
    {
        "app": "str",
        "count": "int",
        "dataset": "str",
        "key": "str",
        "revisions": "list[RevisionDto]",
    },
    total=True,
)
"""`GET /datasets/{app}/{dataset}/history` without `cursor` (legacy shape)."""

RecordPage = TypedDict(
    "RecordPage",
    {
        "items": "list[RecordDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""A keyset page of records (`?cursor=` mode of `GET /datasets/{app}/{ds}`)."""

RederiveResponse = TypedDict(
    "RederiveResponse",
    {
        "app": "str",
        "artifact_sha": "str",
        "dataset": "str",
        "diff": "NotRequired[Optional[Json]]",
        "ignored_meta_fields": "list[str]",
        "key": "str",
        "revision": "int",
        "rules_hash": "str",
        "verdict": "str",
    },
    total=True,
)
"""`POST /provenance/{app}/{dataset}/{key}/rederive` — a read-only replay of the
archived body through the recorded rules."""

ResumeBody = TypedDict(
    "ResumeBody",
    {
        "input": "NotRequired[Json]",
    },
    total=True,
)

RetentionArtifacts = TypedDict(
    "RetentionArtifacts",
    {
        "cassette_bytes": "int",
        "cassette_files": "int",
        "cassettes_protected": "bool",
        "cutoff_days": "int",
        "enabled": "bool",
        "per_app": "list[AppReclaimDto]",
        "pinned_bytes": "int",
        "pinned_files": "int",
        "reclaimable_bytes": "int",
        "reclaimable_files": "int",
        "root": "str",
        "total_bytes": "int",
        "total_files": "int",
        "within_window_bytes": "int",
        "within_window_files": "int",
    },
    total=True,
)
"""The artifact half of the retention dry run."""

RetentionConfig = TypedDict(
    "RetentionConfig",
    {
        "artifact_retention_days": "int",
        "cost_event_retention_days": "int",
        "job_yield_retention_days": "int",
        "revision_retention_days": "int",
        "saved_search_seen_retention_days": "int",
        "webhook_dead_letter_retention_days": "int",
        "webhook_delivery_retention_days": "int",
    },
    total=True,
)
"""The retention windows currently configured, in days."""

RetentionLedger = TypedDict(
    "RetentionLedger",
    {
        "rows": "int",
        "table": "str",
    },
    total=True,
)
"""One append-only ledger's current size."""

RetentionPreview = TypedDict(
    "RetentionPreview",
    {
        "artifacts": "RetentionArtifacts",
        "config": "RetentionConfig",
        "dry_run": "bool",
        "ledgers": "list[RetentionLedger]",
    },
    total=True,
)
"""`GET /retention/preview` — what the janitor WOULD reclaim. Deletes nothing."""

RevisionDto = TypedDict(
    "RevisionDto",
    {
        "app": "str",
        "artifact_sha": "NotRequired[Optional[str]]",
        "change": "str",
        "created_at": "str",
        "data": "NotRequired[Optional[Json]]",
        "dataset": "str",
        "diff": "NotRequired[Optional[Json]]",
        "job_id": "NotRequired[Optional[str]]",
        "key": "str",
        "revision": "int",
        "rules_hash": "NotRequired[Optional[str]]",
        "source_url": "NotRequired[Optional[str]]",
        "trust": "str",
    },
    total=True,
)
"""One entry of the change feed (`pumper_core::datasets::Revision`), with the
`Provenance` block flattened onto it exactly as the server serializes it."""

RevisionPageDto = TypedDict(
    "RevisionPageDto",
    {
        "items": "list[RevisionDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""A keyset page of the change feed."""

SavedSearchDto = TypedDict(
    "SavedSearchDto",
    {
        "app": "NotRequired[Optional[str]]",
        "created_at": "str",
        "dataset": "NotRequired[Optional[str]]",
        "enabled": "bool",
        "id": "str",
        "materialize": "NotRequired[Union[Json, SearchMaterializeDto]]",
        "query": "str",
        "url": "str",
    },
    total=True,
)
"""One saved search. `secret` is never serialized."""

SavedSearchListResponse = TypedDict(
    "SavedSearchListResponse",
    {
        "searches": "list[SavedSearchDto]",
    },
    total=True,
)
"""`GET /searches` without `cursor` (legacy shape)."""

SavedSearchPage = TypedDict(
    "SavedSearchPage",
    {
        "items": "list[SavedSearchDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /searches` with `cursor`."""

ScheduleBudgetBody = TypedDict(
    "ScheduleBudgetBody",
    {
        "budget_usd": "NotRequired[Optional[float]]",
    },
    total=True,
)
"""Body of `POST /schedules/{id}/budget`. `null` clears the ceiling."""

ScheduleBudgetResponse = TypedDict(
    "ScheduleBudgetResponse",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "id": "str",
    },
    total=True,
)
"""`POST /schedules/{id}/budget`."""

ScheduleDto = TypedDict(
    "ScheduleDto",
    {
        "app": "str",
        "budget_usd": "NotRequired[Optional[float]]",
        "created_at": "str",
        "cron": "str",
        "enabled": "bool",
        "health": "NotRequired[Optional[str]]",
        "id": "str",
        "last_job_id": "NotRequired[Optional[str]]",
        "last_run": "NotRequired[Optional[str]]",
        "last_skipped_at": "NotRequired[Optional[str]]",
        "last_status": "NotRequired[Optional[str]]",
        "managed_by": "NotRequired[Optional[str]]",
        "max_attempts": "NotRequired[Optional[int]]",
        "misfire_policy": "str",
        "next_run": "NotRequired[Optional[str]]",
        "params": "Json",
        "priority": "int",
        "skipped_count": "int",
        "timezone": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One cron schedule, enriched with the four derived keys `GET /schedules`
adds (`POST /schedules` returns the bare row, so those four are optional)."""

SchedulePage = TypedDict(
    "SchedulePage",
    {
        "items": "list[ScheduleDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""Keyset page of schedules."""

SearchDatasetDeleted = TypedDict(
    "SearchDatasetDeleted",
    {
        "app": "str",
        "dataset": "str",
        "deleted": "bool",
    },
    total=True,
)
"""`DELETE /search/datasets/{app}/{dataset}`."""

SearchDocsDeleted = TypedDict(
    "SearchDocsDeleted",
    {
        "deleted": "int",
    },
    total=True,
)
"""`DELETE /search/docs` — the count REQUESTED, not an index-confirmed one."""

SearchEnricherStat = TypedDict(
    "SearchEnricherStat",
    {
        "docs": "int",
        "entities": "int",
        "failures": "int",
        "name": "str",
    },
    total=True,
)
"""One index-time enricher's throughput."""

SearchFacetCount = TypedDict(
    "SearchFacetCount",
    {
        "count": "int",
        "value": "str",
    },
    total=True,
)
"""One facet value and its count."""

SearchFacetsDto = TypedDict(
    "SearchFacetsDto",
    {
        "apps": "list[SearchFacetCount]",
        "datasets": "list[SearchFacetCount]",
    },
    total=True,
)
"""Facet counts over the whole match set, not just this page."""

SearchHitDto = TypedDict(
    "SearchHitDto",
    {
        "app": "str",
        "dataset": "str",
        "id": "str",
        "score": "float",
        "snippet": "str",
        "title": "str",
        "url": "str",
    },
    total=True,
)
"""One search hit."""

SearchIndexState = TypedDict(
    "SearchIndexState",
    {
        "degraded": "bool",
        "doc_count": "NotRequired[Optional[int]]",
        "enabled": "bool",
        "reason": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""Whether the index behind an answer is trustworthy.

This block is why `/search` cannot go silently-empty: an index that is off,
wiped or mid-rebuild says so here instead of returning zero hits that read
like 'nothing matched'."""

SearchMaterializeDto = TypedDict(
    "SearchMaterializeDto",
    {
        "app": "str",
        "dataset": "str",
    },
    total=True,
)
"""Where a saved search materializes its hits, when it does."""

SearchResponse = TypedDict(
    "SearchResponse",
    {
        "count": "int",
        "facets": "NotRequired[Union[Json, SearchFacetsDto]]",
        "hits": "list[SearchHitDto]",
        "index": "SearchIndexState",
        "query": "str",
        "total": "int",
    },
    total=True,
)
"""`GET /search`."""

SearchStatusResponse = TypedDict(
    "SearchStatusResponse",
    {
        "disk_bytes": "int",
        "doc_count": "int",
        "enabled": "bool",
        "enrichers": "list[SearchEnricherStat]",
        "segment_count": "int",
    },
    total=True,
)
"""`GET /search/status`."""

SignedBundle = TypedDict(
    "SignedBundle",
    {
        "generated_at": "str",
        "legacy_id": "str",
        "node_id": "str",
        "payload": "Json",
        "schema": "str",
        "sig": "str",
    },
    total=True,
)
"""A signed mesh bundle: an opaque `payload` under this node's signature.

The signature covers the payload as serialized, so a consumer verifies
BEFORE interpreting anything inside it."""

SourceDetailResponse = TypedDict(
    "SourceDetailResponse",
    {
        "contract": "NotRequired[Optional[Json]]",
        "enforcing": "bool",
        "fields": "list[SourceFieldStats]",
        "invariants": "list[SourceInvariant]",
        "runs": "list[SourceRunDto]",
        "see_also": "str",
        "source": "SourceHealthDto",
        "statistical_coverage": "bool",
    },
    total=True,
)
"""`GET /sources/{id}`."""

SourceFieldStats = TypedDict(
    "SourceFieldStats",
    {
        "baseline_distinct_ratio": "NotRequired[Optional[float]]",
        "baseline_miss_rate": "NotRequired[Optional[float]]",
        "baseline_runs": "int",
        "coercion_failure_rate": "float",
        "distinct_ratio": "float",
        "docs": "int",
        "field": "str",
        "mean_len": "float",
        "miss_rate": "float",
    },
    total=True,
)
"""One extracted field's observed statistics, against its baseline."""

SourceHealthDto = TypedDict(
    "SourceHealthDto",
    {
        "app": "str",
        "contract": "NotRequired[Optional[Json]]",
        "dataset": "str",
        "degradation_score": "float",
        "id": "str",
        "last_verdict": "NotRequired[Optional[str]]",
        "last_verdict_at": "NotRequired[Optional[str]]",
        "monitored": "bool",
        "state": "str",
        "state_reason": "NotRequired[Optional[str]]",
        "state_since": "str",
        "tripped_of_last3": "int",
        "updated_at": "str",
    },
    total=True,
)
"""One source's extraction health."""

SourceInvariant = TypedDict(
    "SourceInvariant",
    {
        "confidence": "float",
        "field": "str",
        "json_type": "NotRequired[Optional[str]]",
        "kind": "str",
        "max": "NotRequired[Optional[float]]",
        "min": "NotRequired[Optional[float]]",
        "pattern": "NotRequired[Optional[str]]",
        "support": "int",
    },
    total=True,
)
"""One learned invariant over a field. `kind` selects which of the bounds are
populated, so all of them are optional."""

SourceListResponse = TypedDict(
    "SourceListResponse",
    {
        "contracts": "ContractsStatusDto",
        "contracts_enforce": "bool",
        "count": "int",
        "enabled": "bool",
        "enforcing": "bool",
        "sources": "list[SourceHealthDto]",
        "unmonitored": "int",
    },
    total=True,
)
"""`GET /sources`."""

SourceRunDto = TypedDict(
    "SourceRunDto",
    {
        "build_id": "NotRequired[Optional[str]]",
        "compared": "int",
        "created_at": "str",
        "d_dom": "NotRequired[Optional[float]]",
        "d_text": "NotRequired[Optional[float]]",
        "d_val": "NotRequired[Optional[float]]",
        "diagnosis": "NotRequired[Optional[str]]",
        "docs": "int",
        "fetch_ok_rate": "float",
        "job_id": "str",
        "reasons": "NotRequired[Optional[Json]]",
        "score": "float",
        "state_after": "str",
        "verdict": "str",
    },
    total=True,
)
"""One judged extraction run."""

SourceRunsResponse = TypedDict(
    "SourceRunsResponse",
    {
        "count": "int",
        "id": "str",
        "runs": "list[SourceRunDto]",
    },
    total=True,
)
"""`GET /sources/{id}/runs`."""

SourceStateBody = TypedDict(
    "SourceStateBody",
    {
        "reason": "NotRequired[Optional[str]]",
        "state": "str",
    },
    total=True,
)

SourceStateResponse = TypedDict(
    "SourceStateResponse",
    {
        "id": "str",
        "reason": "str",
        "state": "str",
    },
    total=True,
)
"""`POST /sources/{id}/state` — a manual override of a source's state."""

StartRunBody = TypedDict(
    "StartRunBody",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "idempotency_key": "NotRequired[Optional[str]]",
    },
    total=True,
)

SubscriptionDeliveriesResponse = TypedDict(
    "SubscriptionDeliveriesResponse",
    {
        "count": "int",
        "cursor_seq": "int",
        "deliveries": "list[DeliveryDto]",
        "last_error": "NotRequired[Optional[str]]",
        "latest_seq": "int",
        "subscription_id": "str",
    },
    total=True,
)
"""`GET /subscriptions/{id}/deliveries` without `cursor` (legacy shape)."""

SubscriptionDto = TypedDict(
    "SubscriptionDto",
    {
        "created_at": "str",
        "cursor_seq": "int",
        "enabled": "bool",
        "id": "str",
        "last_delivered_at": "NotRequired[Optional[str]]",
        "last_error": "NotRequired[Optional[str]]",
        "name": "NotRequired[Optional[str]]",
        "principal_id": "NotRequired[Optional[str]]",
        "selector": "Json",
        "sink": "str",
        "url": "str",
    },
    total=True,
)
"""One cursor subscription over the durable event log (N05). `secret` is never
serialized."""

SubscriptionListResponse = TypedDict(
    "SubscriptionListResponse",
    {
        "count": "int",
        "latest_seq": "int",
        "subscriptions": "list[SubscriptionDto]",
    },
    total=True,
)
"""`GET /subscriptions`."""

TransactionApproved = TypedDict(
    "TransactionApproved",
    {
        "note": "str",
        "resumed": "bool",
        "transaction": "TransactionDto",
    },
    total=True,
)
"""`POST /transactions/{id}/approve`."""

TransactionDto = TypedDict(
    "TransactionDto",
    {
        "allow_live": "bool",
        "app": "str",
        "approved_at": "NotRequired[Optional[str]]",
        "approved_by": "NotRequired[Optional[str]]",
        "created_at": "str",
        "evidence_sha": "str",
        "expired": "bool",
        "expires_at": "NotRequired[Optional[str]]",
        "id": "str",
        "idempotency_key": "str",
        "job_id": "NotRequired[Optional[str]]",
        "profile": "NotRequired[Optional[str]]",
        "receipt_path": "NotRequired[Optional[str]]",
        "state": "str",
        "submitted_at": "NotRequired[Optional[str]]",
        "updated_at": "str",
    },
    total=True,
)
"""One staged irreversible browser action, awaiting (or past) approval."""

TransactionListResponse = TypedDict(
    "TransactionListResponse",
    {
        "allow_live": "bool",
        "count": "int",
        "transactions": "list[TransactionDto]",
    },
    total=True,
)
"""`GET /transactions`."""

TransactionRejected = TypedDict(
    "TransactionRejected",
    {
        "job_cancelled": "bool",
        "transaction": "TransactionDto",
    },
    total=True,
)
"""`POST /transactions/{id}/reject`."""

TriggerDto = TypedDict(
    "TriggerDto",
    {
        "bind": "NotRequired[Optional[Json]]",
        "budget_usd": "NotRequired[Optional[float]]",
        "created_at": "str",
        "each": "NotRequired[Optional[str]]",
        "enabled": "bool",
        "filters": "NotRequired[Optional[list[str]]]",
        "id": "str",
        "max_attempts": "int",
        "name": "NotRequired[Optional[str]]",
        "on_change": "NotRequired[Optional[str]]",
        "on_status": "NotRequired[Optional[str]]",
        "params": "Json",
        "plugin_hooks": "NotRequired[Optional[Json]]",
        "priority": "int",
        "source_app": "str",
        "source_dataset": "NotRequired[Optional[str]]",
        "source_kind": "str",
        "target_app": "str",
    },
    total=True,
)
"""One reactive edge: a source event kind to a target app."""

TriggerFanOut = TypedDict(
    "TriggerFanOut",
    {
        "cap": "int",
        "each": "str",
        "hops": "int",
        "total": "NotRequired[Optional[int]]",
        "truncated": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""The fan-out plan of a dry run (N04's `each`)."""

TriggerHookIncident = TypedDict(
    "TriggerHookIncident",
    {
        "detail": "str",
        "outcome": "str",
        "plugin": "str",
        "slot": "str",
    },
    total=True,
)
"""One plugin-hook incident on a hop: what the plugin did instead of deciding."""

TriggerHooks = TypedDict(
    "TriggerHooks",
    {
        "incidents": "list[TriggerHookIncident]",
        "unusable_plugins": "list[str]",
    },
    total=True,
)
"""The hook block of a dry run: which configured plugins could not be used at
all, and what the ones that ran did. A hop whose gate plugin is missing
fails OPEN, so naming the unusable ones is the only way an operator learns
the gate they deployed is not gating."""

TriggerListResponse = TypedDict(
    "TriggerListResponse",
    {
        "triggers": "list[TriggerDto]",
    },
    total=True,
)
"""`GET /triggers` without `cursor` (legacy shape)."""

TriggerPage = TypedDict(
    "TriggerPage",
    {
        "items": "list[TriggerDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /triggers` with `cursor`."""

TriggerRunDto = TypedDict(
    "TriggerRunDto",
    {
        "created_at": "str",
        "dataset": "NotRequired[Optional[str]]",
        "detail": "NotRequired[Optional[str]]",
        "event_id": "NotRequired[Optional[str]]",
        "id": "str",
        "job_id": "NotRequired[Optional[str]]",
        "outcome": "str",
        "source_job_id": "NotRequired[Optional[str]]",
        "source_kind": "str",
        "trigger_id": "str",
    },
    total=True,
)
"""One row of the trigger decision ledger — why a hop did or did not happen."""

TriggerRunsResponse = TypedDict(
    "TriggerRunsResponse",
    {
        "count": "int",
        "decisions": "list[TriggerRunDto]",
        "next_cursor": "NotRequired[Optional[str]]",
        "runs": "list[JobDto]",
        "trigger_id": "str",
    },
    total=True,
)
"""`GET /triggers/{id}/runs` — the hops AND the decisions, because a trigger
that never fired is the case you are usually debugging."""

TriggerTestResponse = TypedDict(
    "TriggerTestResponse",
    {
        "bound_params": "NotRequired[Optional[list[str]]]",
        "fan_out": "NotRequired[Union[Json, TriggerFanOut]]",
        "fired": "NotRequired[Optional[bool]]",
        "hooks": "NotRequired[Union[Json, TriggerHooks]]",
        "job": "NotRequired[Union[Json, JobDto]]",
        "jobs": "NotRequired[Optional[list[JobDto]]]",
        "outcome": "NotRequired[Optional[str]]",
        "reason": "NotRequired[Optional[str]]",
        "resolved_params": "NotRequired[Optional[Json]]",
        "source_job_id": "NotRequired[Optional[str]]",
        "target_app": "NotRequired[Optional[str]]",
        "would_fire": "NotRequired[Optional[bool]]",
    },
    total=True,
)
"""`POST /triggers/{id}/test`.

One schema, six served shapes — a dry run that would not fire, a dry run
that would, and a real `?fire=true` — because a caller has to branch on
`would_fire` / `fired` anyway and splitting them into six components would
make the union harder to consume, not easier."""

WatchDeliveriesResponse = TypedDict(
    "WatchDeliveriesResponse",
    {
        "count": "int",
        "deliveries": "list[DeliveryDto]",
        "watch_id": "str",
    },
    total=True,
)
"""`GET /watches/{id}/deliveries` without `cursor` (legacy shape)."""

WatchDto = TypedDict(
    "WatchDto",
    {
        "app": "str",
        "created_at": "str",
        "cursor_seq": "int",
        "dataset": "str",
        "enabled": "bool",
        "id": "str",
        "last_delivery": "NotRequired[Union[Json, WatchLastDelivery]]",
        "sink": "str",
        "url": "str",
    },
    total=True,
)
"""One dataset-change webhook. `secret` is never serialized."""

WatchLastDelivery = TypedDict(
    "WatchLastDelivery",
    {
        "at": "str",
        "id": "str",
        "status": "str",
    },
    total=True,
)
"""The last delivery a watch made; `null` (never omitted) when it has never
delivered, so 'never fired' and 'fired and failed' cannot be confused."""

WatchListResponse = TypedDict(
    "WatchListResponse",
    {
        "watches": "list[WatchDto]",
    },
    total=True,
)
"""`GET /watches` without `cursor` (legacy shape)."""

WatchPage = TypedDict(
    "WatchPage",
    {
        "items": "list[WatchDto]",
        "next_cursor": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""`GET /watches` with `cursor`."""

WeatherEntryDto = TypedDict(
    "WeatherEntryDto",
    {
        "challenge_fingerprints": "list[str]",
        "host": "str",
        "http_strikes": "int",
        "observations": "int",
        "penalty_ms": "int",
        "preferred_tier": "NotRequired[Optional[str]]",
        "updated_at": "NotRequired[Optional[str]]",
    },
    total=True,
)
"""One host's weather, as exported to a peer."""

WeatherPlanDto = TypedDict(
    "WeatherPlanDto",
    {
        "adopt_pin": "bool",
        "host": "str",
        "notes": "list[str]",
        "raise_penalty_ms": "NotRequired[Optional[int]]",
        "raise_strikes": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""What importing one host's weather would change locally."""

WorkflowCreated = TypedDict(
    "WorkflowCreated",
    {
        "steps": "list[WorkflowStepAccepted]",
        "workflow": "WorkflowDefDto",
    },
    total=True,
)
"""`POST /workflows`."""

WorkflowDefDto = TypedDict(
    "WorkflowDefDto",
    {
        "created_at": "str",
        "cron": "NotRequired[Optional[str]]",
        "enabled": "bool",
        "id": "str",
        "name": "str",
        "spec": "Json",
    },
    total=True,
)
"""One declared multi-step DAG."""

WorkflowListResponse = TypedDict(
    "WorkflowListResponse",
    {
        "workflows": "list[WorkflowDefDto]",
    },
    total=True,
)
"""`GET /workflows`."""

WorkflowResponse = TypedDict(
    "WorkflowResponse",
    {
        "workflow": "WorkflowDefDto",
    },
    total=True,
)
"""`GET /workflows/{id}`."""

WorkflowRunCancelled = TypedDict(
    "WorkflowRunCancelled",
    {
        "cancelled": "bool",
        "jobs_cancelled": "int",
    },
    total=True,
)
"""`DELETE /workflow-runs/{run_id}`."""

WorkflowRunDto = TypedDict(
    "WorkflowRunDto",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "def_id": "str",
        "error": "NotRequired[Optional[str]]",
        "finished_at": "NotRequired[Optional[str]]",
        "id": "str",
        "idempotency_key": "NotRequired[Optional[str]]",
        "principal_id": "NotRequired[Optional[str]]",
        "root_id": "str",
        "spent_usd": "float",
        "started_at": "str",
        "status": "str",
    },
    total=True,
)
"""One run of a workflow."""

WorkflowRunListResponse = TypedDict(
    "WorkflowRunListResponse",
    {
        "runs": "list[WorkflowRunDto]",
    },
    total=True,
)
"""`GET /workflows/{id}/runs`."""

WorkflowRunPlan = TypedDict(
    "WorkflowRunPlan",
    {
        "cron": "NotRequired[Optional[str]]",
        "id": "str",
        "name": "str",
    },
    total=True,
)
"""The plan a run belongs to; `null` when it has since been deleted."""

WorkflowRunReceipt = TypedDict(
    "WorkflowRunReceipt",
    {
        "budget_usd": "NotRequired[Optional[float]]",
        "cost_usd": "float",
        "steps_priced": "int",
        "steps_total": "int",
        "yield": "Json",
    },
    total=True,
)
"""The rolled-up receipt for a whole run."""

WorkflowRunReport = TypedDict(
    "WorkflowRunReport",
    {
        "receipt": "WorkflowRunReceipt",
        "run": "WorkflowRunDto",
        "steps": "list[WorkflowRunStep]",
        "unknown": "list[str]",
        "workflow": "NotRequired[Union[Json, WorkflowRunPlan]]",
    },
    total=True,
)
"""`GET /workflow-runs/{run_id}`."""

WorkflowRunStarted = TypedDict(
    "WorkflowRunStarted",
    {
        "created": "bool",
        "run": "WorkflowRunDto",
    },
    total=True,
)
"""`POST /workflows/{id}/runs`. 202 when this call created the run, 200 when an
idempotency key replayed an existing one — `created` says which."""

WorkflowRunStep = TypedDict(
    "WorkflowRunStep",
    {
        "cost_usd": "NotRequired[Optional[float]]",
        "depends_on": "list[str]",
        "error": "NotRequired[Optional[str]]",
        "finished_at": "NotRequired[Optional[str]]",
        "job_id": "NotRequired[Optional[str]]",
        "status": "str",
        "step": "str",
        "yield": "NotRequired[Optional[list[YieldEntryDto]]]",
    },
    total=True,
)
"""One step's outcome inside a run."""

WorkflowStepAccepted = TypedDict(
    "WorkflowStepAccepted",
    {
        "after": "list[str]",
        "app": "str",
        "params_validated": "bool",
        "step": "str",
    },
    total=True,
)
"""One step as accepted at creation time."""

YieldEntryDto = TypedDict(
    "YieldEntryDto",
    {
        "changed": "NotRequired[Optional[int]]",
        "dataset": "str",
        "new": "NotRequired[Optional[int]]",
        "removed": "NotRequired[Optional[int]]",
        "unchanged": "NotRequired[Optional[int]]",
    },
    total=True,
)
"""One dataset's freshness counters, as an app's `RunReport` reports them."""

#: `GET /apps`: the registry, or the same apps as agent tool definitions with
#: `?format=tools`.
AppsResponse = Union[AppListResponse, AppToolsResponse]

#: `GET /datasets/{app}/{dataset}/changes`.
ChangesResponse = Union[DatasetChangesResponse, RevisionPageDto]

#: `GET /grants`.
GrantsResponse = Union[GrantListResponse, RecordPage]

#: `GET /datasets/{app}/{dataset}/history`.
HistoryResponse = Union[RecordHistoryResponse, RevisionPageDto]

#: `GET /host-weather/export`: the signed v2 envelope, or the unsigned legacy
#: flat bundle with `?schema=1`.
HostWeatherExport = Union[SignedBundle, HostWeatherLegacyBundle]

#: `GET /hosts`.
HostsResponse = Union[HostListResponse, HostPage]

#: `GET /jobs/{id}`: a job plus, for a running one, its live progress snapshot.
JobDetail = Json

#: `GET /jobs`: bare `[Job]`, or a keyset page with `?cursor=`.
JobsResponse = Union[list[JobDto], JobPage]

#: `GET /grants/programs`.
ProgramsResponse = Union[ProgramListResponse, RecordPage]

#: `GET /provisioner/proposals`: bare array, or a keyset page.
ProposalsResponse = Union[list[ProposalSummary], ProposalPage]

#: `GET /datasets/{app}/{dataset}`: bare `[Record]`, or a keyset page.
RecordsResponse = Union[list[RecordDto], RecordPage]

#: `GET /searches`.
SavedSearchesResponse = Union[SavedSearchListResponse, SavedSearchPage]

#: `GET /schedules`: bare array, or a keyset page with `?cursor=`.
SchedulesResponse = Union[list[ScheduleDto], SchedulePage]

#: `GET /subscriptions/{id}/deliveries`.
SubscriptionDeliveryFeed = Union[SubscriptionDeliveriesResponse, DeliveryPage]

#: `GET /triggers`.
TriggersResponse = Union[TriggerListResponse, TriggerPage]

#: `GET /watches/{id}/deliveries`.
WatchDeliveryFeed = Union[WatchDeliveriesResponse, DeliveryPage]

#: `GET /watches`.
WatchesResponse = Union[WatchListResponse, WatchPage]

#: `GET /webhooks/deliveries`.
WebhookDeliveriesResponse = Union[DeliveryListResponse, DeliveryPage]

__all__ = [
    "AppDatasetsResponse",
    "AppEntry",
    "AppListResponse",
    "AppReclaimDto",
    "AppToolDefinition",
    "AppToolExample",
    "AppToolsResponse",
    "ApproveBody",
    "AppsResponse",
    "AuditEntryDto",
    "AuditPage",
    "BackfillBody",
    "BulkRetryBody",
    "BulkRetryResponse",
    "CacheFreshnessResponse",
    "CacheHostFreshness",
    "CacheKeyFreshness",
    "CallerPrincipalDto",
    "CancelJobResponse",
    "CatalogHealthResponse",
    "CatalogHealthSource",
    "CatalogSourceDto",
    "CatalogSourcesResponse",
    "ChangesResponse",
    "ClaimBody",
    "ClaimedJob",
    "ClosingSoonResponse",
    "ContractDto",
    "ContractsStatusDto",
    "CostEventDto",
    "CostSummaryResponse",
    "CostSummaryRow",
    "CreateDerivedBody",
    "CreateIngressSourceBody",
    "CreatePrincipalBody",
    "CreateSavedSearchBody",
    "CreateScheduleBody",
    "CreateSubscriptionBody",
    "CreateTriggerBody",
    "CreateWatchBody",
    "CreateWorkflowBody",
    "DatahubStatusResponse",
    "DatahubSyncResponse",
    "DatasetChangesResponse",
    "DatasetDeletion",
    "DatasetManifest",
    "DeleteDocsBody",
    "DeletedResponse",
    "DeliveryDto",
    "DeliveryListResponse",
    "DeliveryPage",
    "DeliveryReplayResponse",
    "DerivedBackfillResponse",
    "DerivedDeleted",
    "DerivedGroupDto",
    "DerivedListResponse",
    "DerivedLookupDto",
    "DerivedSpecDto",
    "DoctorArtifacts",
    "DoctorCoverage",
    "DoctorFinding",
    "DoctorReport",
    "DoctorSearch",
    "DoctorTable",
    "DoctorThresholds",
    "DupPairDto",
    "DuplicatesResponse",
    "EconomicsAdvice",
    "EconomicsApp",
    "EconomicsByPrincipal",
    "EconomicsClaude",
    "EconomicsDataset",
    "EconomicsReport",
    "EconomicsWindow",
    "EconomicsWindows",
    "EnabledBody",
    "EnabledResponse",
    "EnforcementPreview",
    "EnqueueBody",
    "ErrorEnvelope",
    "EventLogPage",
    "EventRecordDto",
    "ExecutorCheckpointResponse",
    "ExecutorDto",
    "ExecutorFinishResponse",
    "ExecutorListResponse",
    "ExecutorOwned",
    "ExecutorProgressResponse",
    "ExecutorWrite",
    "ExtractPreviewResponse",
    "FetchProxyResponse",
    "GovernDisableSchedule",
    "GovernEnqueueSync",
    "GovernTotals",
    "GovernWould",
    "GovernancePreview",
    "GrantListResponse",
    "GrantsResponse",
    "HealthResponse",
    "HistoryResponse",
    "HostListResponse",
    "HostMemoryReset",
    "HostPage",
    "HostProfileDto",
    "HostWeatherExport",
    "HostWeatherImportResponse",
    "HostWeatherLegacyBundle",
    "HostsResponse",
    "IngestResponse",
    "IngressSourceCreated",
    "IngressSourceDto",
    "IngressSourceListResponse",
    "JobCostsResponse",
    "JobDetail",
    "JobDto",
    "JobPage",
    "JobReceipt",
    "JobsResponse",
    "LookupBody",
    "MarketProfileResponse",
    "MaterializeBody",
    "MeshPeer",
    "MeshResponse",
    "MeshStream",
    "MeshTotalsDto",
    "NodeResponse",
    "PluginListResponse",
    "PluginReloadResponse",
    "PreviewBody",
    "PreviewConsequencesDto",
    "PreviewNotReady",
    "PreviewRunCount",
    "PreviewSource",
    "PreviewTransitionDto",
    "PrincipalCostRow",
    "PrincipalCostsResponse",
    "PrincipalCreated",
    "PrincipalDisabled",
    "PrincipalDto",
    "PrincipalListResponse",
    "PrincipalRotated",
    "ProfileInfoDto",
    "ProfileListResponse",
    "ProgramListResponse",
    "ProgramsResponse",
    "ProposalPage",
    "ProposalPromotion",
    "ProposalSummary",
    "ProposalValidation",
    "ProposalsResponse",
    "ProvenanceCoverage",
    "ProvenanceJob",
    "ProvenanceLink",
    "ProvenanceResponse",
    "ProvenanceStamp",
    "ReceiptArtifactFile",
    "ReceiptArtifacts",
    "ReceiptChange",
    "ReceiptCost",
    "ReceiptEgress",
    "ReceiptEngineCost",
    "ReceiptHealthVerdict",
    "ReceiptJob",
    "ReceiptStages",
    "ReceiptTriggerHop",
    "ReceiptVerdicts",
    "RecipeDto",
    "RecipeImportResponse",
    "RecipeListResponse",
    "ReconcileApplied",
    "ReconcileApplyResponse",
    "ReconcileCreate",
    "ReconcileDisable",
    "ReconcileOrphan",
    "ReconcilePlanDto",
    "ReconcileUpdate",
    "RecordDto",
    "RecordHistoryResponse",
    "RecordPage",
    "RecordsResponse",
    "RederiveResponse",
    "ResumeBody",
    "RetentionArtifacts",
    "RetentionConfig",
    "RetentionLedger",
    "RetentionPreview",
    "RevisionDto",
    "RevisionPageDto",
    "SavedSearchDto",
    "SavedSearchListResponse",
    "SavedSearchPage",
    "SavedSearchesResponse",
    "ScheduleBudgetBody",
    "ScheduleBudgetResponse",
    "ScheduleDto",
    "SchedulePage",
    "SchedulesResponse",
    "SearchDatasetDeleted",
    "SearchDocsDeleted",
    "SearchEnricherStat",
    "SearchFacetCount",
    "SearchFacetsDto",
    "SearchHitDto",
    "SearchIndexState",
    "SearchMaterializeDto",
    "SearchResponse",
    "SearchStatusResponse",
    "SignedBundle",
    "SourceDetailResponse",
    "SourceFieldStats",
    "SourceHealthDto",
    "SourceInvariant",
    "SourceListResponse",
    "SourceRunDto",
    "SourceRunsResponse",
    "SourceStateBody",
    "SourceStateResponse",
    "StartRunBody",
    "SubscriptionDeliveriesResponse",
    "SubscriptionDeliveryFeed",
    "SubscriptionDto",
    "SubscriptionListResponse",
    "TransactionApproved",
    "TransactionDto",
    "TransactionListResponse",
    "TransactionRejected",
    "TriggerDto",
    "TriggerFanOut",
    "TriggerHookIncident",
    "TriggerHooks",
    "TriggerListResponse",
    "TriggerPage",
    "TriggerRunDto",
    "TriggerRunsResponse",
    "TriggerTestResponse",
    "TriggersResponse",
    "WatchDeliveriesResponse",
    "WatchDeliveryFeed",
    "WatchDto",
    "WatchLastDelivery",
    "WatchListResponse",
    "WatchPage",
    "WatchesResponse",
    "WeatherEntryDto",
    "WeatherPlanDto",
    "WebhookDeliveriesResponse",
    "WorkflowCreated",
    "WorkflowDefDto",
    "WorkflowListResponse",
    "WorkflowResponse",
    "WorkflowRunCancelled",
    "WorkflowRunDto",
    "WorkflowRunListResponse",
    "WorkflowRunPlan",
    "WorkflowRunReceipt",
    "WorkflowRunReport",
    "WorkflowRunStarted",
    "WorkflowRunStep",
    "WorkflowStepAccepted",
    "YieldEntryDto",
    "Json",
]
