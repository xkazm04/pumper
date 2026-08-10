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
        // The preview reads the trigger's own source app, so the revisions it
        // matched carry that app — take it from the revision rather than from
        // the source job, whose app may be a producer feeding this namespace.
        let app = matching[0].app.clone();
        let dataset = matching[0].dataset.clone();
        crate::triggers::dataset_trigger_obj(
            &trigger,
            &source,
            &app,
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
/// `target_unregistered`, `predicate_veto`, `plugin_missing`, `bad_filters`,
/// `enqueue_failed`), which are otherwise invisible. Decisions page with
/// `cursor`.
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

/// The delivery states that actually exist, in lifecycle order. The single
/// source of truth for the `?status=` filter, its error message, and the docs.
pub(crate) const DELIVERY_STATUSES: [&str; 4] = ["pending", "delivered", "failed", "dead"];

/// Validates the `?status=` filter against [`DELIVERY_STATUSES`].
///
/// The anti-pattern this closes: the filter was passed straight through to a
/// `WHERE status = ?` bind, so `?status=dead-letter` (or a typo, or the
/// long-documented-but-wrong value) answered `200 {"count": 0}` — which reads
/// as "you have no dead deliveries", the exact opposite of the truth, on the
/// endpoint whose whole job is to tell you otherwise. An unknown status is a
/// caller mistake and must say so.
pub(crate) fn validate_delivery_status(status: Option<&str>) -> Result<Option<&str>, String> {
    match status {
        // An explicitly empty `?status=` means "no filter", matching how the
        // other list routes treat an empty query value.
        None | Some("") => Ok(None),
        Some(s) if DELIVERY_STATUSES.contains(&s) => Ok(Some(s)),
        Some(s) => Err(format!(
            "unknown delivery status '{s}' (expected one of: {})",
            DELIVERY_STATUSES.join(", ")
        )),
    }
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct DeliveriesQuery {
    /// `pending` (in flight) | `delivered` (accepted) | `failed` (**still
    /// retrying** on the backoff ladder) | `dead` (the ladder gave up — this is
    /// the dead-letter view). Anything else is a 400.
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
    responses(
        (status = 200, description = "Dual-mode: `{count, deliveries}`, or `{items, next_cursor}` when `cursor` is present. `?status=dead` is the dead-letter view (the retry ladder gave up); `?status=failed` is still-retrying, NOT the DLQ."),
        (status = 400, description = "Unknown `status` (allowed: pending, delivered, failed, dead)", body = Object),
    )
)]
pub(crate) async fn list_deliveries(
    State(state): State<AppState>,
    Query(query): Query<DeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let status = validate_delivery_status(query.status.as_deref())
        .map_err(|msg| ApiError(StatusCode::BAD_REQUEST, msg))?;
    let Some(cursor) = &query.cursor else {
        let deliveries = state.storage.list_deliveries(status, limit).await?;
        return Ok(Json(
            json!({ "count": deliveries.len(), "deliveries": deliveries }),
        ));
    };
    let after = parse_cursor(cursor);
    let items = state
        .storage
        .list_deliveries_page(status, after, limit)
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

#[derive(Deserialize, IntoParams)]
pub(crate) struct ReplayQuery {
    /// Re-send a delivery that already reached `delivered`. Off by default so a
    /// duplicate the receiver never asked for is always a deliberate act.
    #[serde(default)]
    force: bool,
}

/// Re-sends a logged delivery, re-signing with the source's current secret
/// (job callback secret, watch secret, saved-search secret, or the configured
/// `[webhooks] failure_secret`) when it still exists.
///
/// Guarded on two levels — the row must be in a replayable state, and the send
/// only happens after the row is atomically **claimed**, so a manual replay can
/// neither duplicate an in-flight delivery nor race the auto-drain for the same
/// row.
#[utoipa::path(
    post,
    path = "/webhooks/deliveries/{id}/replay",
    tag = "webhooks",
    params(("id" = String, Path, description = "Delivery id"), ReplayQuery),
    responses(
        (status = 202, description = "Replay claimed and scheduled (`{id, replaying: true}`)"),
        (status = 404, description = "Delivery not found", body = Object),
        (status = 409, description = "Not replayable: the row is in flight (`pending`), already claimed by the auto-drain, or `delivered` without `?force=true`", body = Object),
    )
)]
pub(crate) async fn replay_delivery(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<ReplayQuery>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    use crate::webhook::ReplayGate;
    let Some(delivery) = state.storage.get_delivery(&id).await? else {
        return Err(ApiError(StatusCode::NOT_FOUND, "delivery not found".into()));
    };
    match crate::webhook::replay_gate(&delivery.status, query.force) {
        ReplayGate::Allowed => {}
        ReplayGate::InFlight => {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "delivery is in flight (status 'pending'); a sender already owns it".into(),
            ))
        }
        ReplayGate::NotReplayable => {
            return Err(ApiError(
                StatusCode::CONFLICT,
                format!(
                    "delivery status '{}' is not replayable (pass ?force=true to re-send a \
                     delivered one)",
                    delivery.status
                ),
            ))
        }
    }
    // The claim, not the check above, is the authority: it flips the row to
    // `pending` in one statement, so a drain tick that grabbed it between the
    // read and here loses this race and we answer 409 instead of double-sending.
    if !state
        .storage
        .begin_delivery_replay(&id, query.force)
        .await?
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "delivery was claimed by another replay or by the auto-drain".into(),
        ));
    }
    let secret =
        crate::webhook::resolve_secret(&state.storage, &state.config.webhooks, &delivery).await;
    crate::webhook::replay(
        &state,
        delivery.id.clone(),
        delivery.url.clone(),
        delivery.event.clone(),
        delivery.body.into_bytes(),
        secret,
    )
    .await;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "id": id, "replaying": true })),
    ))
}

#[cfg(test)]
mod delivery_status_tests {
    use super::*;

    /// The anti-pattern the validator exists for: an unknown `?status=` used to
    /// bind straight into `WHERE status = ?`, so a typo — or the value the docs
    /// wrongly named for months — answered `200 {"count": 0}`. "No rows" and
    /// "you asked for a state that doesn't exist" are opposite answers on the
    /// endpoint whose job is to surface undelivered webhooks.
    #[test]
    fn bogus_status_rejected_not_empty_list() {
        for bad in ["dead-letter", "DEAD", "faild", "queued", "succeeded", "'"] {
            let err = validate_delivery_status(Some(bad))
                .expect_err("an unknown status must be a 400, not an empty 200");
            assert!(err.contains(bad), "names what was rejected: {err}");
            // The message has to carry the way out, not just the complaint.
            for allowed in DELIVERY_STATUSES {
                assert!(
                    err.contains(allowed),
                    "names the allowed value {allowed}: {err}"
                );
            }
        }
    }

    #[test]
    fn every_real_state_passes_including_dead() {
        for good in DELIVERY_STATUSES {
            assert_eq!(validate_delivery_status(Some(good)), Ok(Some(good)));
        }
    }

    /// Absent and explicitly-empty both mean "no filter" — an empty `?status=`
    /// is what a form or a shell variable expansion sends, and turning that into
    /// a 400 would break callers who mean "everything".
    #[test]
    fn absent_and_empty_mean_unfiltered() {
        assert_eq!(validate_delivery_status(None), Ok(None));
        assert_eq!(validate_delivery_status(Some("")), Ok(None));
    }

    /// The state set is the contract shared by the filter, the error message,
    /// the OpenAPI description and docs/features/events-webhooks.md. Adding a
    /// state without updating them is the drift this pins.
    #[test]
    fn the_documented_state_set_is_the_one_the_filter_enforces() {
        assert_eq!(
            DELIVERY_STATUSES,
            ["pending", "delivered", "failed", "dead"],
            "storage writes exactly these four (create_delivery / finish_delivery / \
             fail_delivery / begin_delivery_retry)"
        );
    }
}
