use std::sync::Arc;

use pumper_core::ScrapeApp;
use serde_json::{json, Value};

/// Every scraping app the service knows about. Adding a use case:
///   1. create a crate under `crates/apps/<name>` implementing `ScrapeApp`
///   2. add it to `[workspace.dependencies]` and the server's Cargo.toml
///   3. add one line here
pub fn apps() -> Vec<Arc<dyn ScrapeApp>> {
    vec![
        Arc::new(app_hackernews::HackerNews),
        Arc::new(app_research::Research),
        Arc::new(app_connector_api_watch::ConnectorApiWatch),
        Arc::new(app_readable::Readable),
        Arc::new(app_watch::Watch),
        Arc::new(app_grants_gov::GrantsGov),
        Arc::new(app_cms_fee_schedule::CmsFeeSchedule),
        Arc::new(app_census_density::CensusDensity),
        Arc::new(app_census_nonemp::CensusNonemp),
        Arc::new(app_census_nesd::CensusNesd),
        Arc::new(app_census_bfs::CensusBfs),
        Arc::new(app_cordis::Cordis),
        Arc::new(app_homewyse_pricing::HomewysePricing),
        Arc::new(app_state_tax::StateTax),
        Arc::new(app_state_licensing::StateLicensing),
        Arc::new(app_valuation_multiples::ValuationMultiples),
        Arc::new(app_trade_wages::TradeWages),
        Arc::new(app_ca_grants::CaGrants),
        Arc::new(app_eu_sedia::EuSedia),
        Arc::new(app_mpsv_vpm::MpsvVpm),
        Arc::new(app_mpsv_ispv::MpsvIspv),
        Arc::new(app_extractor::Extractor),
        Arc::new(app_plugin::Plugin),
        Arc::new(app_crawl::Crawl),
        Arc::new(app_smlouvy_dump_watch::SmlouvyDumpWatch),
        Arc::new(app_provisioner::Provisioner),
        Arc::new(app_transact::Transact),
        Arc::new(app_peer::Peer),
    ]
}

// ---- Virtual namespaces ------------------------------------------------------
//
// Not every app namespace the change fan-out delivers under is a registered
// app. `worker::run_indexed_apps` widens each run to the job's own app PLUS the
// namespaces its result names in `index_datasets`, and the watch fan-out
// (`worker::notify_watches`) then matches watches against those namespaces — so
// `grants` is where every grant revision lands, and `POST /watches
// {app: "grants"}` used to 404 because `grants` is not in `apps()` above.
//
// Those namespaces are declared in a RESULT, at runtime, so they are not
// statically enumerable: `app-peer` writes under whatever `params.namespace`
// says (default `peer_<remote_app>`), which no compile-time list can predict.
// The running authority is therefore the store — a namespace that already holds
// records is one the fan-out demonstrably delivered under — and the list below
// is only the BOOTSTRAP SEED for namespaces that are structurally certain but
// may not have been written to yet on a fresh install. See
// `routes::watches::namespace_index`, which unions the three sources.

/// One virtual namespace that exists before any run has written to it.
pub(crate) struct VirtualNamespace {
    /// The `app` value revisions land under, and that a watch must name.
    pub name: &'static str,
    /// The registered apps that publish into it. Pinned by test: an entry whose
    /// publishers are no longer registered is stale.
    pub publishers: &'static [&'static str],
    /// Why it exists, quoted at operators in the refusal message.
    pub note: &'static str,
}

/// Virtual namespaces this build can deliver under before their first run.
///
/// Deliberately tiny, and deliberately not the only source: keep it to
/// namespaces a caller would reasonably watch on a fresh install. Anything else
/// becomes watchable the moment it holds a record.
pub(crate) const VIRTUAL_NAMESPACES: &[VirtualNamespace] = &[VirtualNamespace {
    // `grants_common::UNIFIED_APP`. Not imported: `pumper-server` depends on the
    // grant source apps, not on `grants-common`; `virtual_namespace_publishers_are_registered`
    // pins the entry against the registry instead.
    name: "grants",
    publishers: &["grants-gov", "ca-grants", "eu-sedia", "cordis"],
    note: "the cross-source unified grants namespace every grant source publishes into",
}];

/// The virtual namespace a registered app publishes into, if any — the hint
/// behind "you watched the source app, but the records land somewhere else".
pub(crate) fn publishes_into(app: &str) -> Option<&'static VirtualNamespace> {
    VIRTUAL_NAMESPACES
        .iter()
        .find(|ns| ns.publishers.contains(&app))
}

/// One app rendered as an MCP-compatible tool definition: `name`,
/// `description`, and `inputSchema` are the MCP tool-definition contract
/// (an app with no declared schema gets the permissive `{"type":"object"}`);
/// the remaining keys (`cost_class`, `examples`, `output_shape`,
/// `default_params`, `requires`, `ready`, `schedule`) are additive metadata an
/// MCP client ignores and an agent can still read.
///
/// Shared by `GET /apps?format=tools` and the `/mcp` endpoint's tool +
/// resource surfaces, so the two agent-facing views cannot drift.
pub(crate) fn tool_definition(app: &dyn ScrapeApp) -> Value {
    let manifest = app.manifest();
    let input_schema = manifest
        .params_schema
        .unwrap_or_else(|| json!({ "type": "object" }));
    let examples: Vec<Value> = manifest
        .examples
        .iter()
        .map(|e| json!({ "description": e.description, "params": e.params }))
        .collect();
    let requires: Vec<String> = app.requires().iter().map(|r| r.label()).collect();
    let ready = app.requires().iter().all(|r| r.is_satisfied());
    json!({
        "name": app.name(),
        "description": app.description(),
        "inputSchema": input_schema,
        "cost_class": manifest.cost_class.as_str(),
        "output_shape": manifest.output_shape,
        "examples": examples,
        "default_params": app.default_params(),
        "schedule": app.schedule(),
        "requires": requires,
        "ready": ready,
    })
}

// ---- Dynamic apps (M28 v1 slice: discovery + listing ONLY) ------------------
//
// Kept deliberately separate from the static `apps()` list above: static
// entries are compiled-in `ScrapeApp` impls added one line at a time; dynamic
// entries are `.wasm` modules discovered at boot from `[plugins] app_dir` and
// surfaced READ-ONLY. Nothing below ever produces something the worker can run.

/// Why every dynamic app is `runnable: false` in this build. Returned verbatim
/// in listings and in the enqueue rejection so the two surfaces cannot drift.
pub(crate) const DYNAMIC_NOT_RUNNABLE_REASON: &str =
    "dynamic WASM apps are discovery-only in this build: running one requires the \
     component-model host (typed WIT world, async host imports for fetch/storage, \
     fuel + wall-clock + spend budgets across the boundary) — the next slice. \
     Enqueue is rejected outright; no partial execution path exists.";

/// Discovers dynamic apps in `[plugins] app_dir` (feature OFF when unset) and
/// renders each as a read-only `GET /apps` listing entry. A dynamic app whose
/// name collides with a compiled-in app is skipped with a warning — static
/// registration always wins, and a file in a data dir must never shadow it.
pub(crate) fn dynamic_app_entries(
    cfg: &pumper_core::config::PluginConfig,
    static_apps: &std::collections::HashMap<String, Arc<dyn ScrapeApp>>,
) -> Vec<Value> {
    let Some(dir) = &cfg.app_dir else {
        return Vec::new();
    };
    pumper_engine_wasm::discover_dynamic_apps(dir)
        .into_iter()
        .filter(|d| {
            let clash = static_apps.contains_key(&d.name);
            if clash {
                tracing::warn!(
                    name = %d.name,
                    "dynamic app shadows a compiled-in app — skipped (static wins)"
                );
            }
            !clash
        })
        .map(|d| dynamic_entry(&d.name, &d.manifest))
        .collect()
}

/// Maps one discovered manifest to its listing entry. Mirrors the static-app
/// listing keys where they make sense (`name`, `description`, `schedule`,
/// `requires`, `ready`, `has_params_schema`) and adds the dynamic contract:
/// `dynamic: true`, `runnable: false`, and the reason string. The module's
/// filename is the authoritative name; a `name` key inside the manifest is
/// ignored, matching the plugin-manifest convention.
fn dynamic_entry(name: &str, manifest: &Value) -> Value {
    let description = manifest
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("(dynamic app: describe() provided no description)");
    let params_schema = manifest.get("params_schema").cloned();
    json!({
        "name": name,
        "description": description,
        "schedule": Value::Null,
        "requires": ["host:component-model"],
        "ready": false,
        "dynamic": true,
        "runnable": false,
        "reason": DYNAMIC_NOT_RUNNABLE_REASON,
        "has_params_schema": params_schema.is_some(),
        "params_schema": params_schema.unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod dynamic_tests {
    use super::{dynamic_entry, DYNAMIC_NOT_RUNNABLE_REASON};
    use serde_json::json;

    /// The invariant this slice exists to hold: whatever a module's describe()
    /// claims — including lying about being runnable or smuggling a name — the
    /// listing entry is read-only: `dynamic: true`, `runnable: false`, reason
    /// attached, filename-authoritative name.
    #[test]
    fn dynamic_entries_are_never_runnable_and_filename_named() {
        let manifests = [
            json!({ "description": "well-behaved", "params_schema": { "type": "object" } }),
            json!({ "name": "impostor", "runnable": true, "dynamic": false }),
            json!({}),
        ];
        for manifest in &manifests {
            let entry = dynamic_entry("disk_name", manifest);
            assert_eq!(entry["name"], "disk_name");
            assert_eq!(entry["dynamic"], true);
            assert_eq!(entry["runnable"], false);
            assert_eq!(entry["ready"], false);
            assert_eq!(entry["reason"], DYNAMIC_NOT_RUNNABLE_REASON);
        }
    }

    #[test]
    fn dynamic_entry_maps_manifest_description_and_schema() {
        let entry = dynamic_entry(
            "quotes",
            &json!({ "description": "scrapes quotes", "params_schema": { "type": "object" } }),
        );
        assert_eq!(entry["description"], "scrapes quotes");
        assert_eq!(entry["has_params_schema"], true);
        assert_eq!(entry["params_schema"]["type"], "object");
        // And the degraded shape: no description, no schema.
        let bare = dynamic_entry("bare", &json!({}));
        assert!(bare["description"]
            .as_str()
            .unwrap()
            .contains("no description"));
        assert_eq!(bare["has_params_schema"], false);
        assert!(bare["params_schema"].is_null());
    }
}

#[cfg(test)]
mod virtual_namespace_tests {
    use super::{apps, publishes_into, VIRTUAL_NAMESPACES};
    use std::collections::BTreeSet;

    fn registered() -> BTreeSet<&'static str> {
        apps().iter().map(|a| a.name()).collect()
    }

    /// The drift this pins: a seed entry survives a rename or a removal of the
    /// apps that feed it and quietly starts vouching for a namespace nothing
    /// writes to — which is how a hand-kept list becomes a lie.
    #[test]
    fn virtual_namespace_publishers_are_registered() {
        let registered = registered();
        for ns in VIRTUAL_NAMESPACES {
            assert!(
                !ns.publishers.is_empty(),
                "virtual namespace '{}' names no publisher, so nothing can ever \
                 deliver under it",
                ns.name
            );
            for publisher in ns.publishers {
                assert!(
                    registered.contains(publisher),
                    "virtual namespace '{}' claims publisher '{publisher}', which is not \
                     a registered app — the entry is stale",
                    ns.name
                );
            }
        }
    }

    /// A namespace that is also a registered app is not virtual; leaving it in
    /// the seed would mean two answers to "what is this name" and a refusal
    /// message that names the wrong one.
    #[test]
    fn a_virtual_namespace_is_not_also_a_registered_app() {
        let registered = registered();
        for ns in VIRTUAL_NAMESPACES {
            assert!(
                !registered.contains(ns.name),
                "'{}' is a registered app, so it is not a virtual namespace",
                ns.name
            );
        }
    }

    /// The ca-grants/unified trap, at the level of the hint that closes it: a
    /// grant source app has to be able to say where its unified records go.
    #[test]
    fn a_grant_source_names_the_namespace_its_records_land_under() {
        for source in ["ca-grants", "grants-gov"] {
            let ns = publishes_into(source).expect("a grant source redirects");
            assert_eq!(ns.name, "grants");
            assert!(!ns.note.is_empty(), "the redirect has to explain itself");
        }
        // An app that publishes only under its own name has no redirect to give.
        assert!(publishes_into("hackernews").is_none());
    }
}

#[cfg(test)]
mod manifest_tests {
    use super::apps;

    /// Every declared params schema must compile, and every worked example must
    /// validate against its own schema — the guard that keeps manifests honest:
    /// a schema that drifts from the examples (or an example that drifts from
    /// the schema) fails here, not in an agent's first enqueue.
    #[test]
    fn every_manifest_example_passes_its_own_schema() {
        let mut rich = 0;
        for app in apps() {
            let manifest = app.manifest();
            let Some(schema) = &manifest.params_schema else {
                assert!(
                    manifest.examples.is_empty(),
                    "app '{}' has examples but no schema to hold them to",
                    app.name()
                );
                continue;
            };
            rich += 1;
            let validator = jsonschema::validator_for(schema).unwrap_or_else(|e| {
                panic!("app '{}' params_schema does not compile: {e}", app.name())
            });
            assert!(
                !manifest.examples.is_empty(),
                "app '{}' declares a schema but no worked examples — agents need at least one",
                app.name()
            );
            for example in &manifest.examples {
                let errors: Vec<String> = validator
                    .iter_errors(&example.params)
                    .map(|e| format!("{}: {e}", e.instance_path))
                    .collect();
                assert!(
                    errors.is_empty(),
                    "app '{}' example '{}' fails its own schema: {}",
                    app.name(),
                    example.description,
                    errors.join("; ")
                );
            }
        }
        // The five most-used apps ship rich manifests; a refactor that silently
        // drops them back to the empty default should fail loudly.
        assert!(rich >= 5, "expected >= 5 rich manifests, found {rich}");
    }

    /// A scheduled app's `default_params` are what the scheduler enqueues, and
    /// enqueue now enforces the schema — so for scheduled apps the defaults
    /// must satisfy it, or the schedule breaks itself.
    #[test]
    fn scheduled_apps_default_params_pass_their_schema() {
        for app in apps() {
            if app.schedule().is_none() {
                continue;
            }
            let Some(schema) = app.manifest().params_schema else {
                continue;
            };
            let validator = jsonschema::validator_for(&schema).expect("schema compiles");
            let defaults = app.default_params();
            let errors: Vec<String> = validator
                .iter_errors(&defaults)
                .map(|e| format!("{}: {e}", e.instance_path))
                .collect();
            assert!(
                errors.is_empty(),
                "scheduled app '{}' default_params fail its schema: {}",
                app.name(),
                errors.join("; ")
            );
        }
    }
}
