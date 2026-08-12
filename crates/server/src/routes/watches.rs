//! Dataset change webhooks: list (delivery-enriched), create (namespace-gated),
//! delete, enable/disable, and the per-watch delivery log — the
//! `dataset.changed` delivery subscriptions.

use std::collections::{BTreeMap, BTreeSet};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use utoipa::{IntoParams, ToSchema};

use crate::routes::error::{default_limit, keyset_cursor, parse_cursor, ApiError, EnabledBody};
use crate::state::AppState;

// ---- Namespaces: what a watch may actually be created on ---------------------

/// The app namespaces the change fan-out can deliver under, plus where each
/// dataset name actually lives.
///
/// A watch is matched by `worker::notify_watches` against the **entry app** of
/// the run's change batch, and that batch spans every namespace
/// `worker::run_indexed_apps` names — the job's own app plus the virtual apps
/// its result declares in `index_datasets`. So the set of watchable namespaces
/// is genuinely wider than the registry (`grants` is where every grant revision
/// lands, and `POST /watches {app: "grants"}` used to answer 404) and is partly
/// decided at runtime (`app-peer` writes under whatever `params.namespace`
/// says).
///
/// Four sources, unioned:
/// 1. every registered app — its own writes always deliver;
/// 2. [`crate::registry::VIRTUAL_NAMESPACES`] — the bootstrap seed, for
///    namespaces that are structurally certain before their first run;
/// 3. every namespace that already holds records — a namespace with records is
///    one runs really write to (this is what covers caller-named peer mirrors);
/// 4. every saved search's materialize target — views deliver through the same
///    `notify_watches` call.
///
/// **Known gap, deliberately not papered over:** (3) proves records exist, not
/// that the fan-out carries them. A namespace whose producers never declare
/// `index_datasets` (today: `trades`, written by the trades apps through
/// `trades_common::unified`) is admitted here and its watch will never fire.
/// The `last_delivery` enrichment on `GET /watches` is what surfaces that, and
/// it is why "never fired" is rendered as an explicit `null` rather than
/// omitted.
pub(crate) struct NamespaceIndex {
    /// Every namespace a watch may name, sorted.
    known: BTreeSet<String>,
    /// Dataset name → the namespaces that actually hold it. Backs the "you
    /// watched the source app, but those records land somewhere else" hint.
    apps_by_dataset: BTreeMap<String, BTreeSet<String>>,
}

impl NamespaceIndex {
    pub(crate) fn contains(&self, app: &str) -> bool {
        self.known.contains(app)
    }

    /// The accepted values, for an error message that carries the way out.
    pub(crate) fn known_values(&self) -> String {
        self.known
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Builds the [`NamespaceIndex`] for this instant.
pub(crate) async fn namespace_index(state: &AppState) -> Result<NamespaceIndex, ApiError> {
    let mut known: BTreeSet<String> = state.registry.keys().cloned().collect();
    known.extend(
        crate::registry::VIRTUAL_NAMESPACES
            .iter()
            .map(|ns| ns.name.to_string()),
    );

    let mut apps_by_dataset: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (app, dataset) in state.datasets.list_all_datasets().await? {
        known.insert(app.clone());
        apps_by_dataset.entry(dataset).or_default().insert(app);
    }
    for search in state.storage.list_saved_searches(false).await? {
        if let Some(mat) = search.materialize {
            known.insert(mat.app.clone());
            apps_by_dataset
                .entry(mat.dataset)
                .or_default()
                .insert(mat.app);
        }
    }
    Ok(NamespaceIndex {
        known,
        apps_by_dataset,
    })
}

/// Why a `(app, dataset)` watch could never deliver, or `None` if it could.
///
/// Two refusals, and only two — a watch this cannot prove wrong is accepted,
/// because a brand-new app's first dataset is indistinguishable from a typo
/// until it runs.
///
/// 1. **Unknown namespace.** Nothing delivers under that name and nothing is
///    declared to. This is the `grants` 404 inverted: `grants` now passes,
///    `grnats` does not.
/// 2. **The dataset lives elsewhere.** The app is real but the named dataset
///    demonstrably belongs to a different namespace — `{app: "ca-grants",
///    dataset: "unified"}` is accepted-and-dead today, because `ca-grants`
///    publishes its unified records under `grants`. The refusal names the
///    namespace to use instead.
pub(crate) fn watch_target_refusal(
    app: &str,
    dataset: &str,
    index: &NamespaceIndex,
) -> Option<(StatusCode, String)> {
    if !index.contains(app) {
        let hint = match crate::registry::publishes_into(app) {
            Some(ns) => format!(
                " (records for '{app}' land under app '{}' — {})",
                ns.name, ns.note
            ),
            None => String::new(),
        };
        return Some((
            StatusCode::NOT_FOUND,
            format!(
                "unknown app '{app}'{hint} — a watch must name a namespace the change \
                 fan-out delivers under (expected one of: {})",
                index.known_values()
            ),
        ));
    }
    // "*" watches every dataset of the namespace: nothing to cross-check.
    if dataset == "*" {
        return None;
    }
    let Some(owners) = index.apps_by_dataset.get(dataset) else {
        // The dataset has never been written anywhere; it may simply be new.
        return None;
    };
    if owners.contains(app) {
        return None;
    }
    let elsewhere = owners
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    Some((
        StatusCode::BAD_REQUEST,
        format!(
            "watch would never fire: app '{app}' has no dataset '{dataset}' — those \
             records land under app '{elsewhere}', which is the namespace to watch"
        ),
    ))
}

/// Validates an `?app=` list filter against the values it could possibly match.
///
/// The anti-pattern, and it is the same one `validate_delivery_status` was built
/// to kill one file over: the value went straight into a `WHERE app = ?` bind,
/// so `?app=grnats` — or `?app=ca-grants` when the records live under `grants` —
/// answered `200` with an empty list. "You have no watches" and "that namespace
/// does not exist" are opposite answers on the surface whose job is to tell you
/// which subscriptions you have.
///
/// `known` must include the values already STORED on the filtered table, not
/// just the creatable ones: a filter that would have returned rows must never
/// be refused because the creation gate tightened later.
pub(crate) fn validate_app_filter<'a>(
    app: Option<&'a str>,
    known: &BTreeSet<String>,
) -> Result<Option<&'a str>, String> {
    match app {
        // An explicitly empty `?app=` means "no filter" — what a form or an
        // unset shell variable sends — exactly as `?status=` treats it.
        None | Some("") => Ok(None),
        Some(a) if known.contains(a) => Ok(Some(a)),
        Some(a) => Err(format!(
            "unknown app '{a}' (expected one of: {})",
            known
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

/// The accepted `?app=` values for `GET /watches`: everything watchable now,
/// plus every namespace a watch is already stored under (legacy rows created
/// before the gate).
pub(crate) async fn watch_filter_values(
    state: &AppState,
    index: &NamespaceIndex,
) -> Result<BTreeSet<String>, ApiError> {
    let mut values = index.known.clone();
    values.extend(state.storage.watch_apps().await?);
    Ok(values)
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct WatchesQuery {
    app: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor; presence (even empty) switches to `{items, next_cursor}`.
    cursor: Option<String>,
}

#[utoipa::path(
    get,
    path = "/watches",
    tag = "watches",
    params(WatchesQuery),
    responses(
        (status = 200, description = "Dual-mode: `{watches: [Watch]}`, or `{items, next_cursor}` when `cursor` is present. Each watch is enriched with `last_delivery` (`{id, status, at}`, or **explicit `null`** when it has never delivered) so a watch that has never fired is distinguishable from one that fires into a dead receiver."),
        (status = 400, description = "Unknown `app` (the filter names a namespace nothing delivers under; the message lists the accepted values)", body = Object),
    )
)]
pub(crate) async fn list_watches(
    State(state): State<AppState>,
    Query(query): Query<WatchesQuery>,
) -> Result<Json<Value>, ApiError> {
    let limit = query.limit.clamp(1, 500);
    let index = namespace_index(&state).await?;
    let values = watch_filter_values(&state, &index).await?;
    let app = validate_app_filter(query.app.as_deref(), &values)
        .map_err(|msg| ApiError(StatusCode::BAD_REQUEST, msg))?;
    let Some(cursor) = &query.cursor else {
        // Legacy bare-array mode is still capped: an uncursored list must not
        // stream an entire table.
        let watches = state.storage.list_watches_page(app, None, limit).await?;
        return Ok(Json(
            json!({ "watches": enrich_watches(&state, watches).await? }),
        ));
    };
    let after = parse_cursor(cursor);
    let items = state.storage.list_watches_page(app, after, limit).await?;
    let next_cursor = keyset_cursor(&items, limit, |w| {
        format!("{}|{}", pumper_core::datasets::ts(w.created_at), w.id)
    });
    let items = enrich_watches(&state, items).await?;
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

/// Attaches each watch's most recent delivery, or an explicit `null`.
///
/// The listing used to render an enabled watch that had never delivered
/// identically to one delivering fine — so the two failure modes this surface
/// exists to catch (a watch on a namespace the fan-out never reaches, and a
/// watch whose receiver has been dead for a week) both looked like health.
/// `null` is written rather than omitted so "never" is a value, not an absence.
async fn enrich_watches(
    state: &AppState,
    watches: Vec<pumper_core::Watch>,
) -> Result<Vec<Value>, ApiError> {
    let mut out = Vec::with_capacity(watches.len());
    for watch in watches {
        let last = state
            .storage
            .latest_delivery_for_ref(pumper_core::storage::DELIVERY_KIND_WATCH, &watch.id)
            .await?;
        let mut value = serde_json::to_value(&watch).unwrap_or_else(|_| json!({}));
        if let Value::Object(map) = &mut value {
            map.insert(
                "last_delivery".into(),
                match last {
                    Some((id, status, at)) => json!({ "id": id, "status": status, "at": at }),
                    None => Value::Null,
                },
            );
        }
        out.push(value);
    }
    Ok(out)
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateWatchBody {
    /// The app **namespace** the records land under — which is not always the
    /// app that produced them. Registered apps, plus the virtual namespaces the
    /// change fan-out delivers under (e.g. `grants`, which is where every grant
    /// source's unified records go).
    app: String,
    /// Dataset to watch; "*" (default) watches every dataset of the app.
    dataset: Option<String>,
    /// Delivery target. `webhook`/`slack` sinks: required, the URL POSTed at.
    /// `file` sink: ignored — the file is always `data/sinks/<watch_id>.ndjson`.
    url: Option<String>,
    /// If set, delivery bodies are HMAC-SHA256 signed with this secret
    /// (webhook sink; Slack ignores the signature header, file sinks are local).
    secret: Option<String>,
    /// Delivery connector: "webhook" (default), "file" (NDJSON append under
    /// data/sinks/), or "slack" (incoming-webhook message at `url`).
    sink: Option<String>,
}

#[utoipa::path(
    post,
    path = "/watches",
    tag = "watches",
    request_body = CreateWatchBody,
    responses(
        (status = 201, description = "Created watch", body = Object),
        (status = 400, description = "Invalid sink, url missing/not http(s), or an `(app, dataset)` pair that could never fire — the message names the namespace those records actually land under", body = Object),
        (status = 404, description = "Unknown app: the namespace is neither a registered app nor one the change fan-out delivers under", body = Object),
    )
)]
pub(crate) async fn create_watch(
    State(state): State<AppState>,
    Json(body): Json<CreateWatchBody>,
) -> Result<(StatusCode, Json<pumper_core::Watch>), ApiError> {
    // The namespace gate. This used to be `registry.contains_key`, which both
    // over- and under-refused: `{app: "grants"}` 404'd although `grants` is
    // exactly where the fan-out delivers every grant revision, while
    // `{app: "ca-grants", dataset: "unified"}` was accepted and could never
    // fire, because ca-grants publishes those records into `grants`.
    let dataset = body.dataset.as_deref().unwrap_or("*");
    let index = namespace_index(&state).await?;
    if let Some((status, msg)) = watch_target_refusal(&body.app, dataset, &index) {
        return Err(ApiError(status, msg));
    }
    let sink = body.sink.as_deref().unwrap_or("webhook");
    let url = match sink {
        // The file path derives from the watch id only (path-traversal guard
        // lives in the delivery layer); any supplied url is ignored.
        "file" => "",
        "webhook" | "slack" => {
            let url = body.url.as_deref().unwrap_or("");
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(ApiError(
                    StatusCode::BAD_REQUEST,
                    "url must be http(s)".into(),
                ));
            }
            url
        }
        other => {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                format!("unknown sink '{other}' (expected webhook, file, or slack)"),
            ));
        }
    };
    let watch = state
        .storage
        .create_watch(&body.app, dataset, url, body.secret.as_deref(), sink)
        .await?;
    Ok((StatusCode::CREATED, Json(watch)))
}

#[derive(Deserialize, IntoParams)]
pub(crate) struct WatchDeliveriesQuery {
    /// `pending` | `delivered` | `failed` (still retrying) | `dead` (the ladder
    /// gave up). Anything else is a 400 — same vocabulary and same validator as
    /// `GET /webhooks/deliveries`.
    status: Option<String>,
    #[serde(default = "default_limit")]
    limit: i64,
    /// Opaque keyset cursor over this watch's deliveries.
    cursor: Option<String>,
}

/// Every delivery this watch produced, newest first.
///
/// `webhook_deliveries` has always carried `(kind, ref_id)`, but `status` was
/// the only filter it was ever queried by — so "did watch X ever deliver?" had
/// no answer over the API. You could see that *some* delivery was dead without
/// being able to tell which subscription had gone quiet, and a watch that had
/// never fired at all was indistinguishable from a healthy one.
#[utoipa::path(
    get,
    path = "/watches/{id}/deliveries",
    tag = "watches",
    params(("id" = String, Path, description = "Watch id"), WatchDeliveriesQuery),
    responses(
        (status = 200, description = "Dual-mode: `{watch_id, count, deliveries}`, or `{items, next_cursor}` when `cursor` is present. Bodies excluded — fetch one from `GET /webhooks/deliveries/{id}`."),
        (status = 400, description = "Unknown `status` (allowed: pending, delivered, failed, dead)", body = Object),
        (status = 404, description = "Watch not found", body = Object),
    )
)]
pub(crate) async fn watch_deliveries(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<WatchDeliveriesQuery>,
) -> Result<Json<Value>, ApiError> {
    // A deleted or mistyped id must not answer `200 {count: 0}` — that reads as
    // "this watch has never delivered", the exact wrong answer here. Same rule
    // `GET /triggers/{id}/runs` follows.
    if state.storage.get_watch(&id).await?.is_none() {
        return Err(ApiError(StatusCode::NOT_FOUND, "watch not found".into()));
    }
    let limit = query.limit.clamp(1, 500);
    let status = super::triggers::validate_delivery_status(query.status.as_deref())
        .map_err(|msg| ApiError(StatusCode::BAD_REQUEST, msg))?;
    let after = query.cursor.as_deref().and_then(parse_cursor);
    let items = state
        .storage
        .list_deliveries_for_ref_page(
            pumper_core::storage::DELIVERY_KIND_WATCH,
            &id,
            status,
            after,
            limit,
        )
        .await?;
    if query.cursor.is_none() {
        return Ok(Json(
            json!({ "watch_id": id, "count": items.len(), "deliveries": items }),
        ));
    }
    let next_cursor = keyset_cursor(&items, limit, |d| {
        format!("{}|{}", pumper_core::datasets::ts(d.created_at), d.id)
    });
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

#[utoipa::path(
    delete,
    path = "/watches/{id}",
    tag = "watches",
    params(("id" = String, Path, description = "Watch id")),
    responses(
        (status = 200, description = "Deleted (`{deleted: true}`)"),
        (status = 404, description = "Watch not found", body = Object),
    )
)]
pub(crate) async fn delete_watch(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.delete_watch(&id).await? {
        Ok(Json(json!({ "deleted": true })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "watch not found".into()))
    }
}

#[utoipa::path(
    post,
    path = "/watches/{id}/enabled",
    tag = "watches",
    params(("id" = String, Path, description = "Watch id")),
    request_body = EnabledBody,
    responses(
        (status = 200, description = "`{id, enabled}`"),
        (status = 404, description = "Watch not found", body = Object),
    )
)]
pub(crate) async fn set_watch_enabled(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<EnabledBody>,
) -> Result<Json<Value>, ApiError> {
    if state.storage.set_watch_enabled(&id, body.enabled).await? {
        Ok(Json(json!({ "id": id, "enabled": body.enabled })))
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, "watch not found".into()))
    }
}

#[cfg(test)]
mod namespace_tests {
    use super::*;

    fn index(known: &[&str], datasets: &[(&str, &[&str])]) -> NamespaceIndex {
        NamespaceIndex {
            known: known.iter().map(|s| s.to_string()).collect(),
            apps_by_dataset: datasets
                .iter()
                .map(|(dataset, apps)| {
                    (
                        dataset.to_string(),
                        apps.iter().map(|a| a.to_string()).collect(),
                    )
                })
                .collect(),
        }
    }

    /// The headline refusal, inverted: `grants` is not a registered app, but it
    /// is where every grant revision lands and where the fan-out matches
    /// watches — so refusing it made the one namespace worth watching the one
    /// namespace you could not watch.
    #[test]
    fn a_virtual_namespace_is_watchable_not_a_404() {
        let index = index(&["ca-grants", "grants"], &[("unified", &["grants"])]);
        assert!(watch_target_refusal("grants", "unified", &index).is_none());
        assert!(watch_target_refusal("grants", "*", &index).is_none());
    }

    /// The inverse bug: `{app: "ca-grants", dataset: "unified"}` was accepted,
    /// sat enabled forever, and could never fire — ca-grants publishes those
    /// records into `grants`. A refusal that only said "no" would leave the
    /// caller exactly as stuck, so it has to name the namespace to use.
    #[test]
    fn a_dataset_that_lands_elsewhere_is_refused_with_the_namespace_to_use() {
        let index = index(
            &["ca-grants", "grants"],
            &[("unified", &["grants"]), ("opportunities", &["ca-grants"])],
        );
        let (status, msg) =
            watch_target_refusal("ca-grants", "unified", &index).expect("must be refused");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            msg.contains("ca-grants") && msg.contains("unified"),
            "{msg}"
        );
        assert!(
            msg.contains("under app 'grants'"),
            "the refusal has to carry the way out: {msg}"
        );
        // The app's OWN datasets are of course still watchable.
        assert!(watch_target_refusal("ca-grants", "opportunities", &index).is_none());
        // And `*` never cross-checks: it means "whatever this namespace writes".
        assert!(watch_target_refusal("ca-grants", "*", &index).is_none());
    }

    /// A dataset nothing has written yet cannot be proven wrong — a brand-new
    /// app's first dataset looks exactly like a typo until it runs, and
    /// refusing it would make the gate worse than the hole it closes.
    #[test]
    fn an_unwritten_dataset_is_accepted_not_guessed_at() {
        let index = index(&["hackernews"], &[("stories", &["hackernews"])]);
        assert!(watch_target_refusal("hackernews", "brand-new", &index).is_none());
    }

    /// A namespace nothing delivers under is a 404 that names both the mistake
    /// and the accepted values.
    #[test]
    fn an_unknown_namespace_is_refused_with_the_known_values() {
        let index = index(&["ca-grants", "grants"], &[]);
        let (status, msg) = watch_target_refusal("grnats", "*", &index).expect("must be refused");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(msg.contains("grnats"), "names what was rejected: {msg}");
        assert!(
            msg.contains("ca-grants") && msg.contains("grants"),
            "names the accepted values: {msg}"
        );
    }

    /// The same anti-pattern `validate_delivery_status` closes, one surface
    /// over: an unmatchable `?app=` used to bind straight into `WHERE app = ?`
    /// and answer `200` with an empty list — which reads as "you have no
    /// watches on that app", not "that app does not exist".
    #[test]
    fn a_bogus_app_filter_is_rejected_not_an_empty_list() {
        let known: BTreeSet<String> = ["hackernews", "grants"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for bad in ["grnats", "GRANTS", "hackernews ", "'"] {
            let err = validate_app_filter(Some(bad), &known)
                .expect_err("an unmatchable filter must be a 400, not an empty 200");
            assert!(err.contains(bad), "names what was rejected: {err}");
            assert!(err.contains("grants"), "names the way out: {err}");
        }
        for good in ["hackernews", "grants"] {
            assert_eq!(validate_app_filter(Some(good), &known), Ok(Some(good)));
        }
    }

    /// Absent and explicitly-empty both mean "no filter" — an empty `?app=` is
    /// what a form or an unset shell variable sends, and 400-ing that would
    /// break callers who mean "everything". Same rule as `?status=`.
    #[test]
    fn absent_and_empty_app_filters_mean_unfiltered() {
        let known: BTreeSet<String> = ["hackernews"].iter().map(|s| s.to_string()).collect();
        assert_eq!(validate_app_filter(None, &known), Ok(None));
        assert_eq!(validate_app_filter(Some(""), &known), Ok(None));
    }
}
