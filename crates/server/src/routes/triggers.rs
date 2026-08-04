//! Reactive pipelines and the outbound webhook delivery log: trigger CRUD,
//! enable/disable, dry-run/fire test, fired-run lineage, plus the delivery list,
//! single-delivery fetch, and replay.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use pumper_core::EnqueueOptions;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{
    default_limit, keyset_cursor, parse_cursor, ApiError, EnabledBody, MAX_ATTEMPTS_CAP,
};
use crate::state::AppState;

#[derive(Deserialize, IntoParams)]
pub(crate) struct TriggersQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/triggers",
    tag = "triggers",
    params(TriggersQuery),
    responses((status = 200, description = "Dual-mode: `{triggers: [Trigger]}`, or `{items, next_cursor}` when `cursor` is present."))
)]
pub(crate) async fn list_triggers(
    State(state): State<AppState>,
    Query(query): Query<TriggersQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let triggers = state
            .storage
            .list_triggers_page(query.app.as_deref(), None, limit)
            .await?;
        return Ok(Json(json!({ "triggers": triggers })));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_triggers_page(query.app.as_deref(), after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |t| {
        format!("{}|{}", pumper_core::datasets::ts(t.created_at), t.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateTriggerBody {
    name: Option<String>,
    /// 'dataset' (change-feed events) | 'job' (terminal events) |
    /// 'external' (inbound ingress events).
    source_kind: String,
    /// External kind: an ingress source id or '*' (any source).
    source_app: String,
    /// Dataset kind only: dataset name or '*' (default).
    source_dataset: Option<String>,
    /// Dataset kind only: new|changed|removed|fresh|any (default fresh).
    on_change: Option<String>,
    /// Job kind only: succeeded|failed|any (default succeeded).
    on_status: Option<String>,
    target_app: String,
    /// Static params template; `_trigger` is merged over it at fire time.
    params: Option<Value>,
    /// The TARGET's spend ceiling (never inherited from the source).
    budget_usd: Option<f64>,
    priority: Option<i64>,
    max_attempts: Option<i64>,
    /// External kind only: `$.path:op:value` predicate specs (the `?filter=`
    /// grammar) ANDed against the inbound payload.
    filters: Option<Vec<String>>,
    /// Sandboxed WASM hooks (M15): `predicate` returns fire/skip over the
    /// `_trigger` delta envelope (`{"pass": bool}`, fail-open per `on_error`),
    /// `transform` shapes the `_trigger` object before target params. Any
    /// source kind. The named plugin need not be loaded yet (hot reload);
    /// a missing plugin at fire time takes the fail-open path, loudly.
    #[schema(value_type = Object)]
    plugins: Option<pumper_core::TriggerPluginHooks>,
}

/// Create-time validation for one plugin hook: a non-empty plugin name, and
/// `on_error` (predicate only) limited to `fire` | `skip`.
fn validate_hook(
    hook: &pumper_core::PluginHook,
    kind: &str,
    allow_on_error: bool,
) -> Result<(), String> {
    if hook.plugin.trim().is_empty() {
        return Err(format!("{kind} hook needs a non-empty plugin name"));
    }
    match hook.on_error.as_deref() {
        None => Ok(()),
        Some(_) if !allow_on_error => Err(format!(
            "on_error is only valid on the predicate hook, not {kind}"
        )),
        Some("fire" | "skip") => Ok(()),
        Some(other) => Err(format!("invalid on_error '{other}' (fire | skip)")),
    }
}

#[utoipa::path(
    post,
    path = "/triggers",
    tag = "triggers",
    request_body = CreateTriggerBody,
    responses(
        (status = 201, description = "Created trigger", body = Object),
        (status = 400, description = "Invalid source_kind/on_change/on_status", body = Object),
        (status = 404, description = "Unknown target app", body = Object),
    )
)]
pub(crate) async fn create_trigger(
    State(state): State<AppState>,
    Json(body): Json<CreateTriggerBody>,
) -> Result<(StatusCode, Json<pumper_core::Trigger>), ApiError> {
    let bad = |msg: String| ApiError(StatusCode::BAD_REQUEST, msg);
    if !state.registry.contains_key(&body.target_app) {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("unknown target app '{}'", body.target_app),
        ));
    }
    // source_app may be a virtual namespace (e.g. cross-source 'grants'), so
    // only the target is required to be a registered app.
    let (source_dataset, on_change, on_status) = match body.source_kind.as_str() {
        "dataset" => {
            let on_change = body.on_change.as_deref().unwrap_or("fresh");
            if !matches!(on_change, "new" | "changed" | "removed" | "fresh" | "any") {
                return Err(bad(format!("invalid on_change '{on_change}'")));
            }
            if body.on_status.is_some() {
                return Err(bad("on_status is only valid for source_kind 'job'".into()));
            }
            (
                Some(body.source_dataset.as_deref().unwrap_or("*")),
                Some(on_change),
                None,
            )
        }
        "job" => {
            let on_status = body.on_status.as_deref().unwrap_or("succeeded");
            if !matches!(on_status, "succeeded" | "failed" | "any") {
                return Err(bad(format!("invalid on_status '{on_status}'")));
            }
            if body.source_dataset.is_some() || body.on_change.is_some() {
                return Err(bad(
                    "source_dataset/on_change are only valid for source_kind 'dataset'".into(),
                ));
            }
            (None, None, Some(on_status))
        }
        "external" => {
            // source_app = ingress source id or '*'; the dataset/job-only
            // fields have no meaning here and are rejected rather than ignored.
            if body.source_dataset.is_some() || body.on_change.is_some() || body.on_status.is_some()
            {
                return Err(bad(
                    "source_dataset/on_change/on_status are not valid for source_kind 'external'"
                        .into(),
                ));
            }
            (None, None, None)
        }
        other => {
            return Err(bad(format!(
                "invalid source_kind '{other}' (dataset | job | external)"
            )))
        }
    };
    // Payload predicates: external kind only, validated with the same parser
    // the fire path uses so an accepted trigger can always be evaluated.
    let filters = match (&body.filters, body.source_kind.as_str()) {
        (None, _) => None,
        (Some(f), _) if f.is_empty() => None,
        (Some(_), kind) if kind != "external" => {
            return Err(bad(
                "filters are only valid for source_kind 'external'".into()
            ))
        }
        (Some(f), _) => {
            super::datasets::parse_filters(f)?; // 400 with the malformed spec
            Some(f.as_slice())
        }
    };
    // Plugin hooks: validate shape now so an accepted trigger can always be
    // evaluated; an all-empty hooks object stores as no hooks.
    let plugin_hooks = match &body.plugins {
        None => None,
        Some(h) if h.predicate.is_none() && h.transform.is_none() => None,
        Some(h) => {
            if let Some(p) = &h.predicate {
                validate_hook(p, "predicate", true).map_err(bad)?;
            }
            if let Some(t) = &h.transform {
                validate_hook(t, "transform", false).map_err(bad)?;
            }
            Some(h)
        }
    };
    let params = body.params.unwrap_or_else(|| json!({}));
    let trigger = state
        .storage
        .create_trigger(&pumper_core::NewTrigger {
            name: body.name.as_deref(),
            source_kind: &body.source_kind,
            source_app: &body.source_app,
            source_dataset,
            on_change,
            on_status,
            target_app: &body.target_app,
            params: &params,
            budget_usd: body.budget_usd.filter(|b| *b > 0.0),
            priority: body.priority.unwrap_or(0),
            max_attempts: body.max_attempts.unwrap_or(1).clamp(1, MAX_ATTEMPTS_CAP),
            filters,
            plugin_hooks,
        })
        .await?;
    Ok((StatusCode::CREATED, Json(trigger)))
}

#[utoipa::path(
    delete,
    path = "/triggers/{id}",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
pub(crate) async fn delete_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_trigger(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()))
    }
}

#[utoipa::path(
    post,
    path = "/triggers/{id}/enabled",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
pub(crate) async fn set_trigger_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_trigger_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()))
    }
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct TestTriggerQuery {
    /// When true, actually enqueue the resolved hop (repeatable — the
    /// idempotency key is bypassed for testing). Default: dry-run only.
    #[serde(default)]
    fire: bool,
}

/// Dry-runs a trigger against its most recent matching source job: shows
/// whether it would fire, the resolved target params, and why not otherwise.
/// `?fire=true` enqueues the hop for real.
#[utoipa::path(
    post,
    path = "/triggers/{id}/test",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id"), TestTriggerQuery),
    responses(
        (status = 200, description = "Dry-run decision `{would_fire, ...}` or, with `?fire=true`, `{fired, job}`"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
pub(crate) async fn test_trigger(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<TestTriggerQuery>,
) -> Result<Json<Value>, ApiError> {
    let Some(trigger) = state.storage.get_trigger(&id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()));
    };
    let no_fire = |reason: &str| json!({ "would_fire": false, "reason": reason });

    // External triggers have no source *job* to dry-run against — their input
    // is an inbound event; exercise them by POSTing a signed body to /ingest.
    if trigger.source_kind == "external" {
        return Ok(Json(no_fire(
            "external triggers are exercised by POSTing a signed event to /ingest/{source}",
        )));
    }

    // Most recent source job of the trigger's source app.
    let Some(source) = state
        .storage
        .list(Some(&trigger.source_app), None, 1)
        .await?
        .into_iter()
        .next()
    else {
        return Ok(Json(no_fire("no source job of that app exists yet")));
    };

    let decision = crate::triggers::decide(&trigger.id, &source.params, &state.config.triggers);
    let (depth, chain) = match decision {
        crate::triggers::FireDecision::Fire { depth, chain } => (depth, chain),
        crate::triggers::FireDecision::SkipCycle => {
            return Ok(Json(no_fire(
                "cycle: trigger already in the source job's chain",
            )))
        }
        crate::triggers::FireDecision::SkipDepth => {
            return Ok(Json(no_fire("max chain depth reached")))
        }
    };

    let obj = if trigger.source_kind == "dataset" {
        // Unfiltered: this is a dry-run preview of what the trigger *would*
        // see, and the live path suppresses per source rather than by trust.
        let changes = state
            .datasets
            .changes_since(&trigger.source_app, None, source.started_at, 1000, None)
            .await?;
        let matching: Vec<&pumper_core::Revision> = changes
            .iter()
            .filter(|r| trigger.covers_dataset(&r.dataset))
            .filter(|r| crate::triggers::change_matches(trigger.on_change.as_deref(), &r.change))
            .collect();
        if matching.is_empty() {
            return Ok(Json(no_fire(
                "latest source run produced no matching changes",
            )));
        }
        let dataset = matching[0].dataset.clone();
        crate::triggers::dataset_trigger_obj(
            &trigger,
            &source,
            &dataset,
            &matching,
            depth,
            &chain,
            &state.config.triggers,
        )
    } else {
        if !crate::triggers::status_matches(trigger.on_status.as_deref(), source.status.as_str()) {
            return Ok(Json(no_fire(
                "latest source job's status does not match on_status",
            )));
        }
        crate::triggers::terminal_trigger_obj(&trigger, &source, depth, &chain)
    };
    // Run the plugin hooks the live fire path would, so the dry-run decision
    // and the previewed params are honest about predicate/transform effects.
    let Some(obj) =
        crate::triggers::apply_plugin_hooks(state.plugins.as_ref(), &trigger, obj).await
    else {
        return Ok(Json(no_fire("predicate plugin returned pass=false")));
    };
    let resolved_params = crate::triggers::merged_params(&trigger.params, obj);

    if !query.fire {
        return Ok(Json(json!({
            "would_fire": true,
            "target_app": trigger.target_app,
            "source_job_id": source.id,
            "resolved_params": resolved_params,
        })));
    }
    // Real fire: no idempotency key so tests are repeatable.
    let opts = EnqueueOptions {
        params: resolved_params,
        max_attempts: trigger.max_attempts,
        priority: trigger.priority,
        budget_usd: trigger.budget_usd,
        trigger_id: Some(trigger.id.clone()),
        ..Default::default()
    };
    let job = state.storage.enqueue(&trigger.target_app, opts).await?;
    state.notify.notify_one();
    Ok(Json(json!({ "fired": true, "job": job })))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct RunsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor over `decisions` (the `runs` array is always the
    /// newest `limit` jobs).
    cursor: Option<String>,
}

/// What this trigger did, fires and skips alike.
///
/// `runs` is the job lineage (`jobs.trigger_id`) — the jobs the trigger
/// actually enqueued. `decisions` is the ledger (`trigger_runs`): one row per
/// evaluation of this trigger against one source event, INCLUDING the negatives
/// (`no_change_match`, `filter_miss`, `dedup`, `cycle`, `depth`,
/// `target_unregistered`, `predicate_veto`, `bad_filters`, `enqueue_failed`),
/// which are otherwise invisible. Decisions page with `cursor`.
#[utoipa::path(
    get,
    path = "/triggers/{id}/runs",
    tag = "triggers",
    params(("id" = String, Path, description = "Trigger id"), RunsQuery),
    responses(
        (status = 200, description = "`{trigger_id, count, runs: [Job], decisions: [TriggerRun], next_cursor}`"),
        (status = 404, description = "Trigger not found", body = Object),
    )
)]
pub(crate) async fn trigger_runs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<RunsQuery>,
) -> Result<Json<Value>, ApiError> {
    // A deleted (or mistyped) trigger used to answer 200 `{count: 0}`, which
    // reads as "this trigger has never fired" — the exact wrong answer for the
    // question the endpoint exists to answer.
    if state.storage.get_trigger(&id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, "trigger not found".into()));
    }
    let limit = query.limit.clamp(1, 500);
    let jobs = state.storage.jobs_by_trigger(&id, limit).await?;
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let decisions = state
        .storage
        .list_trigger_runs_page(&id, after, limit)
        .await?;
    let next_cursor = keyset_cursor(&decisions, limit, |d| {
        format!("{}|{}", pumper_core::datasets::ts(d.created_at), d.id)
    });
    Ok(Json(json!({
        "trigger_id": id,
        "count": jobs.len(),
        "runs": jobs,
        "decisions": decisions,
        "next_cursor": next_cursor,
    })))
}

// ---- Webhook delivery log ----------------------------------------------------

#[derive(Deserialize, IntoParams)]
pub(crate) struct DeliveriesQuery {
    /// 'pending' | 'delivered' | 'failed' — `failed` is the dead-letter view.
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/webhooks/deliveries",
    tag = "webhooks",
    params(DeliveriesQuery),
    responses((status = 200, description = "Dual-mode: `{count, deliveries}`, or `{items, next_cursor}` when `cursor` is present. `?status=failed` is the dead-letter view."))
)]
pub(crate) async fn list_deliveries(
    State(state): State<AppState>,
    Query(query): Query<DeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let Some(cursor) = &query.cursor else {
        let deliveries = state
            .storage
            .list_deliveries(query.status.as_deref(), limit)
            .await?;
        return Ok(Json(
            json!({ "count": deliveries.len(), "deliveries": deliveries }),
        ));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_deliveries_page(query.status.as_deref(), after, limit)
        .await?;
    let next_cursor = keyset_cursor(&items, limit, |d| {
        format!("{}|{}", pumper_core::datasets::ts(d.created_at), d.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[utoipa::path(
    get,
    path = "/webhooks/deliveries/{id}",
    tag = "webhooks",
    params(("id" = String, Path, description = "Delivery id")),
    responses(
        (status = 200, description = "The delivery, including body", body = Object),
        (status = 404, description = "Delivery not found", body = Object),
    )
)]
pub(crate) async fn get_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<pumper_core::Delivery>, ApiError> {
    state
        .storage
        .get_delivery(&id)
        .await?
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "delivery not found".into()))
}

/// Re-sends a logged delivery, re-signing with the source's current secret
/// (job callback secret or watch secret) when it still exists.
#[utoipa::path(
    post,
    path = "/webhooks/deliveries/{id}/replay",
    tag = "webhooks",
    params(("id" = String, Path, description = "Delivery id")),
    responses(
        (status = 202, description = "Replay scheduled (`{id, replaying: true}`)"),
        (status = 404, description = "Delivery not found", body = Object),
    )
)]
pub(crate) async fn replay_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let Some(delivery) = state.storage.get_delivery(&id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "delivery not found".into()));
    };
    let secret = crate::webhook::resolve_secret(&state.storage, &delivery).await;
    crate::webhook::replay(
        state.webhook_client.clone(),
        state.storage.clone(),
        delivery.id.clone(),
        delivery.url.clone(),
        delivery.event.clone(),
        delivery.body.into_bytes(),
        secret,
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "id": id, "replaying": true })),
    ))
}
