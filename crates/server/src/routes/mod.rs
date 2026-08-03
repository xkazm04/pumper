//! HTTP surface for the pumper service.
//!
//! Handlers live in cohesive per-domain submodules (`meta`, `events`, `jobs`,
//! `schedules`, `datasets`, `watches`, `triggers`, `search`, `runtime`,
//! `query`), each carrying its own `#[utoipa::path]` operations and DTOs;
//! `error` holds the shared `ApiError` and the cross-module helpers. This file
//! is the wiring: it registers every handler into one `OpenApiRouter` — the
//! single source the axum routing table and the OpenAPI document are both
//! generated from — and exposes [`router`] for `main.rs`.

use axum::extract::DefaultBodyLimit;
use axum::Router;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::state::AppState;

// The `?filter=` spec parser, re-exported for the external-trigger fire path
// (`crate::triggers` sits outside `routes` and cannot reach the private
// `datasets` submodule directly). One parser, both surfaces — no drift.
pub(crate) use datasets::parse_filters;
// The defaults-merge, re-exported for the MCP enqueue tool (`crate::mcp`) so a
// job enqueued by an agent gets byte-identical params to one POSTed over HTTP.
pub(crate) use jobs::merge_params;

mod datasets;
mod derived;
mod economics;
mod error;
mod events;
mod health;
mod host_weather;
mod ingress;
mod jobs;
mod meta;
mod provenance;
mod query;
mod receipt;
mod recipes;
mod remote;
mod runtime;
mod schedules;
mod search;
mod triggers;
mod watches;

// Bring every handler (and its utoipa-generated `__path_*` companion) into this
// module so the `routes!(...)` registrations below resolve. Each submodule
// exposes only its handlers as `pub(crate)`; DTOs and helpers stay private.
use datasets::*;
use derived::*;
use economics::*;
use events::*;
use health::*;
use host_weather::*;
use ingress::*;
use jobs::*;
use meta::*;
use provenance::*;
use query::*;
use receipt::*;
use recipes::*;
use remote::*;
use runtime::*;
use schedules::*;
use search::*;
use triggers::*;
use watches::*;

/// Top-level metadata for the generated OpenAPI document. Route operations are
/// collected from each `#[utoipa::path]` handler by `OpenApiRouter` (see
/// `openapi_router`), so this only carries document-level info and tags.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "pumper HTTP API",
        description = "Local scraping / data-product service. Machine-readable surface \
                       for the routes documented in docs/features/http-api.md.",
        version = "0.1.0",
    ),
    tags(
        (name = "health", description = "Liveness and Prometheus metrics"),
        (name = "apps", description = "Registered scraping apps and enqueue"),
        (name = "jobs", description = "Job queue lifecycle"),
        (name = "costs", description = "Engine spend ledger"),
        (name = "schedules", description = "Cron schedules"),
        (name = "datasets", description = "Change-detected dataset records, export, history"),
        (name = "grants", description = "Filtered query surface over the cross-source grants corpus"),
        (name = "derived", description = "Derived datasets: filter/project/lookup specs recomputed on upstream deltas"),
        (name = "watches", description = "Dataset change webhooks"),
        (name = "triggers", description = "Reactive pipelines"),
        (name = "ingress", description = "Inbound event ingress: signed external webhooks as trigger inputs"),
        (name = "webhooks", description = "Outbound delivery log"),
        (name = "search", description = "Full-text search and saved searches"),
        (name = "extract", description = "Declarative RuleSet preview / dry-run"),
        (name = "plugins", description = "WASM plugin host"),
        (name = "events", description = "Server-sent event streams"),
        (name = "hosts", description = "Learned per-host tier memory and politeness"),
        (name = "cache", description = "HTTP cache freshness model (learned change cadence)"),
        (name = "profiles", description = "Session vault: named login profiles"),
        (name = "recipes", description = "API X-ray: discovered JSON-API endpoints behind rendered pages"),
        (name = "remote", description = "Distributed fetch fabric: peer nodes proxy fetches through this node's local stack"),
        (name = "provenance", description = "Record-level derivation chains (M12): who wrote each revision from what, plus read-only re-derivation"),
        (name = "meta", description = "The OpenAPI document itself"),
        (name = "sources", description = "Extraction health: per-source degradation detection"),
    )
)]
struct ApiDoc;

/// Ceiling on an inbound request body, applied to every route.
///
/// Sized from what the POST surface actually accepts, all of which is
/// hand-authored JSON config: job `params`, a cron string, a webhook URL + HMAC
/// secret, a trigger definition, a saved search, a source-state flip.
/// `POST /jobs/retry` is the widest and it carries only `{app, status, limit}` —
/// the id list is server-generated and capped at 500. The largest realistic body
/// on this list is a trigger with an inline payload template, measured in
/// kilobytes; 1 MiB is three orders of magnitude of headroom, so no legitimate
/// request can hit it, while an unbounded body can no longer make the process
/// buffer arbitrary memory before a handler ever runs.
///
/// The one endpoint that genuinely takes a *document* rather than config
/// (`POST /extract/preview`) gets a scoped override — see
/// [`PREVIEW_BODY_LIMIT_BYTES`] — instead of this number being raised for
/// everyone.
pub(crate) const BODY_LIMIT_BYTES: usize = 1024 * 1024;

/// Ceiling for `POST /extract/preview`, whose `html` field is a whole web page.
///
/// Deliberately equal to `PREVIEW_MAX_BODY_BYTES` in `runtime.rs`, the budget the
/// same endpoint already enforces on its `url`-fetch path: the two ways to hand
/// a preview a document must accept the same document, or the same page would
/// preview through `url` and 413 through `html`.
pub(crate) const PREVIEW_BODY_LIMIT_BYTES: usize = 8 * 1024 * 1024;

/// An "override" that is not larger than the thing it overrides is a
/// footgun that reads as protection — reject it at compile time.
const _: () = assert!(PREVIEW_BODY_LIMIT_BYTES > BODY_LIMIT_BYTES);

/// The routes that take a document-sized body, carrying their own larger limit.
/// Kept as a separate `OpenApiRouter` so the override is scoped by construction
/// — a route only gets the bigger ceiling by being moved into this function.
fn large_body_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(extract_preview))
        .layer(DefaultBodyLimit::max(PREVIEW_BODY_LIMIT_BYTES))
}

/// Builds the router with every route registered through its `#[utoipa::path]`
/// annotation, so the axum routing table and the OpenAPI document are generated
/// from a single source and cannot drift. Registering a route without an
/// annotation fails to compile; the path-coverage test guards the inverse.
fn openapi_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(metrics))
        .routes(routes!(stream_events))
        .routes(routes!(list_apps))
        .routes(routes!(enqueue_job))
        .routes(routes!(list_datasets))
        .routes(routes!(list_jobs))
        .routes(routes!(get_job, cancel_job))
        .routes(routes!(retry_job))
        .routes(routes!(bulk_retry_jobs))
        .routes(routes!(reset_job))
        .routes(routes!(stream_job))
        .routes(routes!(job_costs))
        .routes(routes!(job_receipt))
        .routes(routes!(cost_summary))
        .routes(routes!(economics_report))
        .routes(routes!(list_schedules, create_schedule))
        .routes(routes!(delete_schedule))
        .routes(routes!(set_schedule_enabled))
        .routes(routes!(list_records, delete_dataset_route))
        .routes(routes!(delete_record_route))
        .routes(routes!(export_records))
        .routes(routes!(dataset_duplicates))
        .routes(routes!(dataset_changes))
        .routes(routes!(record_history))
        .routes(routes!(get_provenance))
        .routes(routes!(rederive_provenance))
        .routes(routes!(list_derived, create_derived))
        .routes(routes!(get_derived, delete_derived))
        .routes(routes!(set_derived_enabled))
        .routes(routes!(backfill_derived))
        .routes(routes!(list_watches, create_watch))
        .routes(routes!(delete_watch))
        .routes(routes!(set_watch_enabled))
        .routes(routes!(list_triggers, create_trigger))
        .routes(routes!(delete_trigger))
        .routes(routes!(set_trigger_enabled))
        .routes(routes!(test_trigger))
        .routes(routes!(trigger_runs))
        .routes(routes!(list_ingress_sources, create_ingress_source))
        .routes(routes!(delete_ingress_source))
        .routes(routes!(set_ingress_source_enabled))
        .routes(routes!(ingest))
        .routes(routes!(list_deliveries))
        .routes(routes!(get_delivery))
        .routes(routes!(replay_delivery))
        .routes(routes!(cache_freshness))
        .routes(routes!(list_hosts))
        .routes(routes!(get_host))
        .routes(routes!(delete_host_memory))
        .routes(routes!(export_host_weather))
        .routes(routes!(import_host_weather))
        .routes(routes!(list_profiles))
        .routes(routes!(list_recipes))
        .routes(routes!(fetch_proxy))
        .routes(routes!(list_plugins))
        .routes(routes!(reload_plugins))
        .routes(routes!(search))
        .routes(routes!(delete_search_docs))
        .routes(routes!(list_saved_searches, create_saved_search))
        .routes(routes!(delete_saved_search))
        .routes(routes!(set_saved_search_enabled))
        .routes(routes!(delete_search_dataset))
        .routes(routes!(search_status))
        .routes(routes!(list_grants))
        .routes(routes!(closing_soon))
        .routes(routes!(catalog_sources))
        .routes(routes!(catalog_health))
        .routes(routes!(catalog_reconcile, catalog_reconcile_apply))
        .routes(routes!(list_sources))
        .routes(routes!(get_source))
        .routes(routes!(source_runs))
        .routes(routes!(set_source_state))
        .routes(routes!(datahub_status))
        .routes(routes!(datahub_sync))
        .routes(routes!(openapi_json))
        // Document-bodied routes, with their own scoped body ceiling.
        .merge(large_body_router())
}

pub fn router(state: AppState) -> Router {
    let (router, _api) = openapi_router().split_for_parts();
    // MCP endpoint (`[mcp] enabled`, default OFF). Merged here — inside the
    // layer stack below (body limit, compression, trace) but deliberately
    // OUTSIDE the OpenAPI document: /mcp speaks JSON-RPC, not REST, and the
    // spec-coverage test's EXPECTED inventory documents the REST surface only.
    let router = if state.config.mcp.enabled {
        router.merge(crate::mcp::router())
    } else {
        router
    };
    // gzip/br responses when the client sends accept-encoding — record JSON is
    // highly repetitive (identical keys per row, ISO timestamps) so it compresses
    // ~5-10x, a real win for remote consumers and the streamed export path.
    // CompressionLayer's default predicate skips already-small and SSE
    // (`text/event-stream`) responses, so /events and /jobs/{id}/stream keep their
    // incremental KeepAlive delivery; it wraps the body stream (no full buffering),
    // so the export path stays constant-memory. Localhost clients that don't send
    // accept-encoding are unaffected.
    let router = router
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // Outermost of the three, so it is the first thing an inbound request
        // meets. `DefaultBodyLimit` works by putting the limit in the request
        // extensions for `Json`/`Bytes` to read, and the *innermost* layer wins
        // — which is precisely why `large_body_router`'s per-route override
        // survives this global one instead of being clobbered by it.
        .layer(DefaultBodyLimit::max(BODY_LIMIT_BYTES));
    // CORS is OFF by default (same-origin only). A permissive allow-all on an
    // unauthenticated, mutating, data-bearing API lets any site the operator
    // visits drive it cross-origin (DNS-rebinding defeats the localhost
    // assumption). A trusted local UI opts in via [server] cors_allowed_origins.
    let origins: Vec<axum::http::HeaderValue> = state
        .config
        .server
        .cors_allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();
    let router = if origins.is_empty() {
        router
    } else {
        router.layer(
            tower_http::cors::CorsLayer::new()
                .allow_origin(origins)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        )
    };
    router.with_state(state)
}

#[cfg(test)]
mod catalog_tests {
    use pumper_core::Catalog;
    use std::collections::BTreeSet;

    /// The catalog, embedded at compile time so the check doesn't depend on the
    /// test's working directory.
    const CATALOG_TOML: &str = include_str!("../../../../catalog/data-sources.toml");

    /// Registered apps deliberately absent from the catalog, each for a documented
    /// reason. Kept explicit so a NEW in-scope source app can't be silently omitted
    /// — adding one that isn't listed here fails the coverage check below. Reasons:
    ///  - generic tooling / engines, not a data *source*: `crawl`, `extractor`,
    ///    `plugin`, `readable`, `research`, `watch`, `transact` (an on-demand
    ///    dry-run browser-flow executor, not a data source), and `provisioner` (an
    ///    on-demand compiler that *proposes* catalog rows for human review —
    ///    it deliberately never appears in the catalog it writes proposals for,
    ///    has no schedule, and its output is always `status = "planned"`).
    ///  - `hackernews`: an example/template app, not a production pipeline.
    ///  - sibling-product consumers outside this catalog's grant/labor scope, same
    ///    rationale the `census-*` docs already state ("a separate Ledgerline
    ///    consumer from the grant pipeline"): the Ledgerline trades apps
    ///    (`census-density`, `census-nonemp`, `homewyse-pricing`, `state-tax`,
    ///    `trade-wages`, `valuation-multiples`), the Counterbill medical app
    ///    (`cms-fee-schedule`), and the tender-radar procurement app
    ///    (`smlouvy-dump-watch` — a Czech Registr smluv dump-index watcher).
    const CATALOG_EXEMPT: &[&str] = &[
        "census-density",
        "census-nonemp",
        "cms-fee-schedule",
        "crawl",
        "extractor",
        "hackernews",
        "homewyse-pricing",
        "plugin",
        "provisioner",
        "readable",
        "research",
        "smlouvy-dump-watch",
        "state-tax",
        "trade-wages",
        "transact",
        "peer",
        "valuation-multiples",
        "watch",
    ];

    fn catalog() -> Catalog {
        Catalog::parse(CATALOG_TOML).expect("catalog/data-sources.toml parses")
    }

    fn registered() -> Vec<std::sync::Arc<dyn pumper_core::ScrapeApp>> {
        crate::registry::apps()
    }

    #[test]
    fn live_catalog_entries_map_to_registered_apps_with_matching_cron() {
        let catalog = catalog();
        let apps = registered();
        for source in catalog.live() {
            assert!(
                !source.app.is_empty(),
                "live catalog source '{}' has no app — a live pipeline must name its serving app",
                source.id
            );
            let app = apps
                .iter()
                .find(|a| a.name() == source.app)
                .unwrap_or_else(|| {
                    panic!(
                        "live catalog source '{}' names unregistered app '{}'",
                        source.id, source.app
                    )
                });
            // Cron must agree in BOTH directions: "" here must mean the app has no
            // schedule, and a cron here must equal the app's schedule() exactly.
            let app_cron = app.schedule().unwrap_or("");
            assert_eq!(
                source.cron.trim(),
                app_cron,
                "catalog cron for '{}' ({:?}) disagrees with app '{}' schedule() ({:?})",
                source.id,
                source.cron,
                source.app,
                app_cron,
            );
        }
    }

    #[test]
    fn every_registered_data_source_app_is_in_the_catalog() {
        let catalog = catalog();
        let cataloged: BTreeSet<&str> = catalog
            .sources
            .iter()
            .map(|s| s.app.as_str())
            .filter(|a| !a.is_empty())
            .collect();
        let exempt: BTreeSet<&str> = CATALOG_EXEMPT.iter().copied().collect();
        let missing: Vec<&str> = registered()
            .iter()
            .map(|a| a.name())
            .filter(|name| !cataloged.contains(name) && !exempt.contains(name))
            .collect();
        assert!(
            missing.is_empty(),
            "registered apps missing from catalog/data-sources.toml (add an entry, or add to \
             CATALOG_EXEMPT if it isn't a data source): {missing:?}"
        );
    }
}

#[cfg(test)]
mod api_spec_tests {
    use std::collections::BTreeSet;

    /// Every `(METHOD, path)` operation the router serves. Because `router` and
    /// the OpenAPI document are both generated from `openapi_router`, this set is
    /// literally the routing table — so this list doubles as the canonical route
    /// inventory. Adding or removing a route changes the spec and fails this test
    /// until the list is updated, which is the point: drift can't land silently.
    const EXPECTED: &[&str] = &[
        "GET /health",
        "GET /metrics",
        "GET /events",
        "GET /apps",
        "POST /apps/{name}/jobs",
        "GET /apps/{name}/datasets",
        "GET /jobs",
        "GET /jobs/{id}",
        "DELETE /jobs/{id}",
        "POST /jobs/{id}/retry",
        "POST /jobs/retry",
        "POST /jobs/{id}/reset",
        "GET /jobs/{id}/stream",
        "GET /jobs/{id}/costs",
        "GET /jobs/{id}/receipt",
        "GET /costs",
        "GET /economics",
        "GET /schedules",
        "POST /schedules",
        "DELETE /schedules/{id}",
        "POST /schedules/{id}/enabled",
        "GET /datasets/{app}/{dataset}",
        "GET /datasets/{app}/{dataset}/export",
        "GET /datasets/{app}/{dataset}/duplicates",
        "DELETE /datasets/{app}/{dataset}",
        "DELETE /datasets/{app}/{dataset}/records/{key}",
        "GET /datasets/{app}/{dataset}/changes",
        "GET /datasets/{app}/{dataset}/history",
        "GET /provenance/{app}/{dataset}/{key}",
        "POST /provenance/{app}/{dataset}/{key}/rederive",
        "GET /derived",
        "POST /derived",
        "GET /derived/{id}",
        "DELETE /derived/{id}",
        "POST /derived/{id}/enabled",
        "POST /derived/{id}/backfill",
        "GET /watches",
        "POST /watches",
        "DELETE /watches/{id}",
        "POST /watches/{id}/enabled",
        "GET /triggers",
        "POST /triggers",
        "DELETE /triggers/{id}",
        "POST /triggers/{id}/enabled",
        "POST /triggers/{id}/test",
        "GET /triggers/{id}/runs",
        "GET /ingress/sources",
        "POST /ingress/sources",
        "DELETE /ingress/sources/{id}",
        "POST /ingress/sources/{id}/enabled",
        "POST /ingest/{id}",
        "GET /webhooks/deliveries",
        "GET /webhooks/deliveries/{id}",
        "POST /webhooks/deliveries/{id}/replay",
        "GET /cache/freshness",
        "GET /hosts",
        "GET /hosts/{host}",
        "DELETE /hosts/{host}/memory",
        "GET /host-weather/export",
        "POST /host-weather/import",
        "GET /profiles",
        "GET /recipes",
        "POST /fetch-proxy",
        "GET /plugins",
        "POST /plugins/reload",
        "GET /search",
        "GET /search/status",
        "DELETE /search/docs",
        "GET /searches",
        "POST /searches",
        "DELETE /searches/{id}",
        "POST /searches/{id}/enabled",
        "DELETE /search/datasets/{app}/{dataset}",
        "POST /extract/preview",
        "GET /grants",
        "GET /grants/closing-soon",
        "GET /catalog/sources",
        "GET /catalog/health",
        "GET /catalog/reconcile",
        "POST /catalog/reconcile",
        "GET /sources",
        "GET /sources/{id}",
        "GET /sources/{id}/runs",
        "POST /sources/{id}/state",
        "GET /datahub/status",
        "POST /datahub/sync",
        "GET /openapi.json",
    ];

    /// The `(METHOD, path)` operations actually present in the generated spec.
    fn spec_operations() -> BTreeSet<String> {
        let api = super::openapi_router().split_for_parts().1;
        let json = serde_json::to_value(&api).expect("spec serializes");
        let methods = [
            "get", "post", "put", "delete", "patch", "head", "options", "trace",
        ];
        let mut ops = BTreeSet::new();
        for (path, item) in json["paths"].as_object().expect("paths object") {
            for method in item.as_object().expect("path item object").keys() {
                if methods.contains(&method.as_str()) {
                    ops.insert(format!("{} {}", method.to_uppercase(), path));
                }
            }
        }
        ops
    }

    #[test]
    fn spec_covers_exactly_the_registered_routes() {
        let spec = spec_operations();
        let expected: BTreeSet<String> = EXPECTED.iter().map(|s| s.to_string()).collect();
        let missing: Vec<_> = expected.difference(&spec).collect();
        let undocumented: Vec<_> = spec.difference(&expected).collect();
        assert!(
            missing.is_empty(),
            "routes missing from the OpenAPI spec: {missing:?}"
        );
        assert!(
            undocumented.is_empty(),
            "spec has operations not in the expected inventory (update EXPECTED): {undocumented:?}"
        );
    }

    /// Routes exempted from the global [`super::BODY_LIMIT_BYTES`] ceiling by
    /// living in `large_body_router`. Every entry is a route that takes a
    /// *document* rather than JSON config; the EXPECTED-diff makes moving a
    /// route into the larger ceiling a visible, deliberate edit rather than a
    /// one-line drive-by. Raising the global limit to accommodate a new document
    /// route is the anti-pattern this list exists to prevent.
    const EXPECTED_LARGE_BODY: &[&str] = &["POST /extract/preview"];

    #[test]
    fn only_document_routes_get_the_larger_body_ceiling() {
        let api = super::large_body_router().split_for_parts().1;
        let json = serde_json::to_value(&api).expect("spec serializes");
        let mut ops = BTreeSet::new();
        for (path, item) in json["paths"].as_object().expect("paths object") {
            for method in item.as_object().expect("path item object").keys() {
                ops.insert(format!("{} {}", method.to_uppercase(), path));
            }
        }
        let expected: BTreeSet<String> =
            EXPECTED_LARGE_BODY.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            ops, expected,
            "the body-limit override's scope changed — update EXPECTED_LARGE_BODY only if the \
             new route genuinely accepts a document-sized body"
        );
    }

    #[test]
    fn spec_is_valid_openapi_document() {
        let api = super::openapi_router().split_for_parts().1;
        let json = serde_json::to_value(&api).unwrap();
        assert!(json["openapi"].as_str().unwrap().starts_with("3."));
        assert_eq!(json["info"]["title"], "pumper HTTP API");
        // Typed request bodies land in components.schemas (e.g. the enqueue body).
        assert!(json["components"]["schemas"]["EnqueueBody"].is_object());
    }
}

#[cfg(test)]
mod filter_tests {
    // `filter_specs` / `parse_filters` are dataset-query helpers (in `datasets`),
    // in scope here via `use datasets::*`.
    use super::{filter_specs, parse_filters};
    use pumper_core::datasets::JsonFilter;

    #[test]
    fn filter_specs_pulls_only_repeated_filter_keys() {
        let pairs = vec![
            ("limit".to_string(), "10".to_string()),
            ("filter".to_string(), "$.state:eq:CA".to_string()),
            ("cursor".to_string(), "x".to_string()),
            ("filter".to_string(), "$.n:numgte:5".to_string()),
        ];
        assert_eq!(filter_specs(&pairs), vec!["$.state:eq:CA", "$.n:numgte:5"]);
    }

    #[test]
    fn parses_each_op_and_preserves_colons_in_value() {
        let specs: Vec<String> = [
            "$.state:eq:CA",
            "$.name:contains:health",
            "$.close_date:gte:2026-01-01",
            "$.close_date:lte:2026-12-31",
            "$.seen:eq:2026-07-17T10:30:00Z", // value keeps its colons
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let out = parse_filters(&specs).expect("valid");
        assert!(
            matches!(&out[0], JsonFilter::Eq { path, value } if path == "$.state" && value == "CA")
        );
        assert!(matches!(&out[1], JsonFilter::Contains { .. }));
        assert!(matches!(&out[2], JsonFilter::Gte { .. }));
        assert!(matches!(&out[3], JsonFilter::Lte { .. }));
        assert!(
            matches!(&out[4], JsonFilter::Eq { value, .. } if value == "2026-07-17T10:30:00Z"),
            "value must keep colons after the op"
        );
    }

    #[test]
    fn numgte_parses_number_and_multiple_paths() {
        let out = parse_filters(&["$.award_ceiling,$.total_funding:numgte:1000".to_string()])
            .expect("valid");
        match &out[0] {
            JsonFilter::NumGteAny { paths, value } => {
                assert_eq!(
                    paths,
                    &vec!["$.award_ceiling".to_string(), "$.total_funding".to_string()]
                );
                assert_eq!(*value, 1000.0);
            }
            other => panic!("expected NumGteAny, got {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_specs() {
        // Missing op/value.
        assert!(parse_filters(&["$.state".to_string()]).is_err());
        assert!(parse_filters(&["$.state:eq".to_string()]).is_err());
        // Path not a JSON path.
        assert!(parse_filters(&["state:eq:CA".to_string()]).is_err());
        // Unknown op.
        assert!(parse_filters(&["$.state:like:CA".to_string()]).is_err());
        // numgte with a non-number.
        assert!(parse_filters(&["$.n:numgte:lots".to_string()]).is_err());
        // numgte with a bad sub-path.
        assert!(parse_filters(&["$.a,bad:numgte:1".to_string()]).is_err());
    }

    #[test]
    fn empty_specs_yield_no_filters() {
        assert!(parse_filters(&[]).expect("ok").is_empty());
    }
}
