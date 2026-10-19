use std::sync::Arc;

use pumper_core::app::{AppManifest, CostClass, ManifestExample};
use pumper_core::ScrapeApp;
use serde_json::{json, Value};

/// Every scraping app the service knows about. Adding a use case:
///   1. create a crate under `crates/apps/<name>` implementing `ScrapeApp`
///   2. add it to `[workspace.dependencies]` and the server's Cargo.toml
///   3. add one line here
///
/// `config` is the operator's loaded config, so an app that owns a `[section]`
/// is CONSTRUCTED with it. Before this, `apps()` took nothing, and an app could
/// only reach an operator key through a per-run job param — which is how
/// `[research] max_watched_sources` sat in `config.toml` binding nothing while
/// the run capped at the shipped default. Most apps take no section and are
/// registered exactly as before; the ones that do read theirs off `self` in
/// `run()`, never from a global.
pub fn apps(config: &pumper_core::Config) -> Vec<Arc<dyn ScrapeApp>> {
    vec![
        Arc::new(app_hackernews::HackerNews),
        Arc::new(app_research::Research::with_config(&config.research)),
        Arc::new(app_connector_api_watch::ConnectorApiWatch),
        Arc::new(app_readable::Readable),
        Arc::new(app_watch::Watch),
        Arc::new(app_grants_gov::GrantsGov),
        Arc::new(app_cms_fee_schedule::CmsFeeSchedule),
        Arc::new(app_census_density::CensusDensity::with_config(
            &config.census,
        )),
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
        Arc::new(app_mpsv_ciselniky::MpsvCiselniky),
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
    /// publishers are no longer registered — or that never wrote into the
    /// namespace at all — is stale.
    pub publishers: &'static [&'static str],
    /// Why it exists, quoted at operators in the refusal message.
    pub note: &'static str,
}

/// Virtual namespaces this build can deliver under before their first run.
///
/// Deliberately tiny, and deliberately not the only source: keep it to
/// namespaces a caller would reasonably watch on a fresh install. Anything else
/// becomes watchable the moment it holds a record.
pub(crate) const VIRTUAL_NAMESPACES: &[VirtualNamespace] = &[
    VirtualNamespace {
        // `grants_common::UNIFIED_APP`. Kept as a literal on purpose even though
        // the server now depends on `grants-common` (for `deadline_end_utc`):
        // `virtual_namespace_publishers_are_registered` pins the entry against
        // the registry, which is the guard that matters here.
        name: "grants",
        // `cordis` is deliberately NOT here. It is an EU-funding app, but it writes
        // only its own `cordis/projects` + `cordis/topic_stats` and never calls
        // `grants_common::finalize_unified` — so `publishes_into("cordis")` used to
        // redirect an operator who watched `cordis` to a namespace that will never
        // carry one of its revisions.
        publishers: &["grants-gov", "ca-grants", "eu-sedia"],
        note: "the cross-source unified grants namespace every grant source publishes into",
    },
    VirtualNamespace {
        // `trades_common::unified::UNIFIED_APP`. Not imported, for the same reason
        // `grants` is not: `pumper-server` depends on the trades source apps, not on
        // `trades-common`; `virtual_namespace_publishers_are_registered` pins it.
        //
        // `cms-fee-schedule` is deliberately NOT here — it is a Market Data app in
        // the same group, but it drives no LLM, writes only its own `releases` +
        // `fee_schedule*` datasets, and never calls `unified::sync_operator_economics`.
        // Listing it would redirect an operator who watched it to a namespace that
        // will never carry one of its revisions — the same trap `cordis` documents above.
        name: "trades",
        publishers: &[
            "state-tax",
            "state-licensing",
            "trade-wages",
            "homewyse-pricing",
            "valuation-multiples",
        ],
        note: "the cross-source trades namespace holding operator_economics + compliance, \
           which all five trades apps publish into",
    },
    VirtualNamespace {
        // `trades_common::market::MARKET_APP` (N33). The one namespace here fed
        // by TWO families: the state x trade profile is re-derived at the end of
        // both `unified::sync_operator_economics` (the five trades apps) and
        // `app_census_density::sync_market_blend` (the four census apps), so
        // whichever refreshed last publishes it. Watching either family's app
        // alone is exactly the mistake `publishes_into` exists to redirect.
        name: "market",
        publishers: &[
            "state-tax",
            "state-licensing",
            "trade-wages",
            "homewyse-pricing",
            "valuation-multiples",
            "census-density",
            "census-nonemp",
            "census-nesd",
            "census-bfs",
        ],
        note: "the cross-FAMILY market namespace holding profile (one row per state x trade), \
           which the five trades apps and the four census apps all publish into",
    },
    VirtualNamespace {
        // `census_common::MARKET_APP` (N35). The census family's own product
        // namespace — market_blend, saturation and the county atlas — which all
        // four census apps re-derive after their own upserts.
        //
        // APPENDED, deliberately: `publishes_into` returns the FIRST match, and
        // the four census apps already resolve to `market` above. Putting this
        // entry earlier would change where an operator watching `census-density`
        // is redirected, which is a behaviour change this row does not need to
        // make — the entry exists so a catalog `[[source]]` may NAME the
        // namespace its data actually lands under (the guard in
        // `routes::mod`'s `live_catalog_entries_map_to_registered_apps_with_
        // matching_cron`).
        name: "census",
        // `census-nesd` and `census-bfs` ALSO re-derive these products (all four
        // apps call `sync_market_blend`), and are deliberately absent: the guard
        // below asks a publisher's own manifest to name the namespace it writes
        // into, and those two `output_shape`s do not — a manifest change in
        // crates this work does not own. The seed is a bootstrap, not the
        // authority (the store is), so listing fewer publishers is honest and
        // listing them without evidence would not be.
        publishers: &["census-density", "census-nonemp"],
        note: "the census product namespace holding market_blend, saturation and the county \
           atlas, which all four census apps publish into",
    },
];

/// The virtual namespace a registered app publishes into, if any — the hint
/// behind "you watched the source app, but the records land somewhere else".
pub(crate) fn publishes_into(app: &str) -> Option<&'static VirtualNamespace> {
    VIRTUAL_NAMESPACES
        .iter()
        .find(|ns| ns.publishers.contains(&app))
}

/// The virtual namespace called `name`, if there is one — the lookup in the
/// other direction from [`publishes_into`].
///
/// A namespace is a legal `app` for a catalog row (that is the pair the data
/// lands under), so the catalog's registered-app guard consults this before
/// declaring a row's app unknown.
///
/// **`cfg(test)` on purpose**, and it is the same judgment this wave applied to
/// `max_row_delta_pct`: that guard is its only consumer today. Every *runtime*
/// path already keys on the `(app, dataset)` pair the data lands under and needs
/// no registry lookup at all — `/catalog/health` reads
/// `datasets.list(s.app, s.dataset)`, `enforce_contracts` reads the pair off each
/// revision, and `Catalog::reconcile` skips any row without a cron (which a
/// namespace row must not have). Compiling it into the binary as `pub(crate)`
/// dead code would advertise a seam nothing uses; when a runtime caller appears,
/// deleting one attribute is the whole change.
#[cfg(test)]
pub(crate) fn virtual_namespace(name: &str) -> Option<&'static VirtualNamespace> {
    VIRTUAL_NAMESPACES.iter().find(|ns| ns.name == name)
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

/// Why a **core module** in the app dir is `runnable: false`. It is not the
/// component-model host that is missing any more (N09 shipped it) — it is that
/// this file is the older describe-only shape, which has no `run` at all.
/// Returned verbatim in listings and in the enqueue rejection so the two
/// surfaces cannot drift.
pub(crate) const DYNAMIC_NOT_RUNNABLE_REASON: &str =
    "this module is a describe-only core module: it exports a manifest and no \
     `run`, so there is nothing to execute. A RUNNABLE dynamic app is a \
     component-model binary exporting the pumper:app@0.1.0 world (see \
     plugins-src/wasm-app-template) loaded with [wasm_apps] enabled = true. \
     Enqueue is rejected outright; no partial execution path exists.";

/// Why a component in the app dir is listed but not registered while
/// `[wasm_apps] enabled = false` — the default.
pub(crate) const WASM_APPS_DISABLED_REASON: &str =
    "this IS a pumper:app component, but [wasm_apps] enabled = false (the \
     default): running third-party code with fetch and dataset-write authority \
     is an operator decision. Set [wasm_apps] enabled = true to register it.";

/// Discovers dynamic apps in `[plugins] app_dir` (feature OFF when unset) and
/// renders each as a `GET /apps` listing entry, **registering** the ones that
/// are runnable: a component that links against the `pumper:app` world, whose
/// manifest validates, and whose bytes match any catalog pin.
///
/// A dynamic app whose name collides with a compiled-in app is skipped with a
/// warning — static registration always wins, and a file in a data dir must
/// never shadow it.
pub(crate) fn dynamic_app_entries(
    config: &pumper_core::config::Config,
    registry: &mut std::collections::HashMap<String, Arc<dyn ScrapeApp>>,
) -> Vec<Value> {
    let cfg = &config.plugins;
    let Some(dir) = &cfg.app_dir else {
        return Vec::new();
    };
    // Describe-only core modules: the M28 path, unchanged and still read-only.
    let mut entries: Vec<Value> = pumper_engine_wasm::discover_dynamic_apps_with(dir, cfg)
        .into_iter()
        .filter(|d| !shadows_static(&d.name, registry))
        .map(|d| dynamic_entry(&d.name, &d.manifest))
        .collect();
    entries.extend(component_entries(dir, config, registry));
    entries.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    entries
}

fn shadows_static(
    name: &str,
    registry: &std::collections::HashMap<String, Arc<dyn ScrapeApp>>,
) -> bool {
    let clash = registry.contains_key(name);
    if clash {
        tracing::warn!(
            name = %name,
            "dynamic app shadows a compiled-in app — skipped (static wins)"
        );
    }
    clash
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

// ---- Runnable dynamic apps (N09: the component-model host) ------------------

/// Scans `dir` for **components**, registering the runnable ones into
/// `registry` and returning one listing entry each.
///
/// Three ways a component is listed but NOT registered, each with its own
/// reason string, because "it did not run" is not an answer an operator can
/// act on: the feature is off, the catalog pins a different build, or the
/// manifest does not validate. None of them is a silent skip.
fn component_entries(
    dir: &std::path::Path,
    config: &pumper_core::config::Config,
    registry: &mut std::collections::HashMap<String, Arc<dyn ScrapeApp>>,
) -> Vec<Value> {
    if !config.wasm_apps.enabled {
        // The host does not exist when the switch is off, so components are
        // identified by their header alone — enough to say what the file is and
        // why it is inert, without compiling anything.
        return inert_component_entries(dir, registry);
    }
    let host = match pumper_engine_wasm::app_host::WasmAppHost::new(dir, &config.wasm_apps) {
        Ok(Some(host)) => Arc::new(host),
        Ok(None) => return Vec::new(),
        Err(e) => {
            tracing::warn!("dynamic-app host failed to start: {e}");
            return Vec::new();
        }
    };
    // One catalog read for the whole scan. A catalog that fails to load is not
    // fatal here — it means "nothing is pinned", which is reported per app in
    // the listing (`pinned: false`) rather than silently treated as a match.
    let catalog = pumper_core::Catalog::load()
        .map_err(|e| tracing::warn!("dynamic-app pin check: catalog unreadable: {e}"))
        .ok();
    let mut entries = Vec::new();
    for app in host.discovered() {
        if shadows_static(&app.name, registry) {
            continue;
        }
        let expected = catalog
            .as_ref()
            .and_then(|c| pinned_module_hash(c, &app.name));
        match pumper_core::catalog::module_pin(expected.as_deref(), &app.sha256) {
            pumper_core::catalog::ModulePin::Mismatch => {
                let expected = expected.unwrap_or_default();
                tracing::warn!(
                    name = %app.name,
                    "dynamic app refused: catalog pins {expected}, module on disk is {}",
                    app.sha256
                );
                entries.push(unrunnable_component_entry(
                    &app.name,
                    &app.manifest,
                    &app.sha256,
                    false,
                    &format!(
                        "refused: the catalog pins module_sha256 = {expected}, but the module in \
                         the app dir hashes to {}. Update the [[source]] row, or restore the \
                         pinned build.",
                        app.sha256
                    ),
                ));
                continue;
            }
            pin => {
                let pinned = pin == pumper_core::catalog::ModulePin::Match;
                match validated_manifest(&app.manifest) {
                    Err(why) => {
                        tracing::warn!(name = %app.name, "dynamic app manifest rejected: {why}");
                        entries.push(unrunnable_component_entry(
                            &app.name,
                            &app.manifest,
                            &app.sha256,
                            pinned,
                            &format!("refused: describe() manifest is not usable: {why}"),
                        ));
                    }
                    Ok(manifest) => {
                        let dynamic = DynamicApp::new(&app, manifest, host.clone());
                        entries.push(dynamic.listing_entry(pinned));
                        registry.insert(app.name.clone(), Arc::new(dynamic));
                    }
                }
            }
        }
    }
    entries
}

/// Listing entries for components found while `[wasm_apps] enabled = false`.
/// Their manifests are deliberately NOT read: reading one means instantiating
/// the module, which is exactly what the switch says not to do.
fn inert_component_entries(
    dir: &std::path::Path,
    registry: &std::collections::HashMap<String, Arc<dyn ScrapeApp>>,
) -> Vec<Value> {
    let Ok(read) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    for entry in read.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
            continue;
        };
        // Only the header is read, never the whole file.
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        if !pumper_engine_wasm::app_host::is_component(&bytes) || shadows_static(&name, registry) {
            continue;
        }
        entries.push(unrunnable_component_entry(
            &name,
            &Value::Null,
            &pumper_engine_wasm::app_host::module_sha256(&bytes),
            false,
            WASM_APPS_DISABLED_REASON,
        ));
    }
    entries
}

/// The `module_sha256` a catalog row pins for the app called `name`.
///
/// A row matches on `id` OR `app`, because both spellings occur in the shipped
/// catalog (`id` is the slug, `app` names the serving unit) and a pin that only
/// half the rows can express would be a pin nobody uses.
fn pinned_module_hash(catalog: &pumper_core::Catalog, name: &str) -> Option<String> {
    catalog
        .sources
        .iter()
        .find(|s| s.engine == pumper_core::catalog::WASM_ENGINE && (s.id == name || s.app == name))
        .map(|s| s.module_sha256.clone())
        .filter(|h| !h.is_empty())
}

/// Turns a `describe()` manifest into an [`AppManifest`], refusing the shapes a
/// compiled-in app could never ship.
///
/// The guard that matters is **examples against the schema** — the exact check
/// the server's own manifest test runs over every Rust app. Without it a
/// dynamic app could advertise a worked example that its own schema rejects,
/// which is worse than no example: an agent copies it and gets a 422 from the
/// enqueue door that validates the same schema.
fn validated_manifest(manifest: &Value) -> Result<AppManifest, String> {
    let Some(map) = manifest.as_object() else {
        return Err("describe() must return a JSON object".into());
    };
    let params_schema = map.get("params_schema").cloned().filter(|s| !s.is_null());
    if let Some(schema) = &params_schema {
        jsonschema::validator_for(schema)
            .map_err(|e| format!("params_schema is not a usable JSON Schema: {e}"))?;
    }
    let mut examples = Vec::new();
    for (i, example) in map
        .get("examples")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let description = example
            .get("description")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("examples[{i}] has no string \"description\""))?;
        let params = example
            .get("params")
            .cloned()
            .ok_or_else(|| format!("examples[{i}] has no \"params\""))?;
        if let Some(schema) = &params_schema {
            crate::mcp::validate_params(schema, &params)
                .map_err(|e| format!("examples[{i}] fails the app's own params_schema: {e}"))?;
        }
        examples.push(ManifestExample {
            description: leak(description),
            params,
        });
    }
    let cost_class = match map.get("cost_class").and_then(Value::as_str) {
        None | Some("free") => CostClass::Free,
        Some("metered") => CostClass::Metered,
        Some("claude") => CostClass::Claude,
        Some(other) => {
            return Err(format!(
                "cost_class = {other:?} is not one of free | metered | claude"
            ))
        }
    };
    Ok(AppManifest {
        params_schema,
        examples,
        output_shape: map
            .get("output_shape")
            .and_then(Value::as_str)
            .map(leak_static),
        cost_class,
    })
}

/// `ScrapeApp::name`/`description`/`schedule` are `&'static str`, and a dynamic
/// app's are read off disk at boot. Leaking is bounded by the number of modules
/// in the app dir, once per process — the alternative (widening the trait to
/// `String`) would touch every compiled-in app to serve the dynamic ones.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

fn leak_static(s: &str) -> &'static str {
    leak(s)
}

/// A component-model module presented to the rest of the server as an ordinary
/// [`ScrapeApp`]: the queue, scheduler, receipts, triggers, budgets and SSE
/// need no knowledge that this app is not compiled in.
struct DynamicApp {
    name: &'static str,
    description: &'static str,
    schedule: Option<&'static str>,
    default_params: Value,
    manifest: AppManifest,
    sha256: String,
    host: Arc<pumper_engine_wasm::app_host::WasmAppHost>,
}

impl DynamicApp {
    fn new(
        app: &pumper_engine_wasm::app_host::DiscoveredApp,
        manifest: AppManifest,
        host: Arc<pumper_engine_wasm::app_host::WasmAppHost>,
    ) -> Self {
        let m = &app.manifest;
        Self {
            // The FILENAME is the name, whatever the manifest claims — the same
            // rule plugin manifests follow, and the reason a module cannot
            // smuggle itself in under another app's identity.
            name: leak(&app.name),
            description: leak(
                m.get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("(dynamic app: describe() provided no description)"),
            ),
            schedule: m.get("schedule").and_then(Value::as_str).map(leak),
            default_params: m
                .get("default_params")
                .cloned()
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({})),
            manifest,
            sha256: app.sha256.clone(),
            host,
        }
    }

    /// This app's `GET /apps` entry: a runnable one, so it carries the same
    /// keys a compiled-in app's does plus the dynamic provenance (`world`,
    /// `module_sha256`, `pinned`) an operator needs to know WHICH build is
    /// answering.
    fn listing_entry(&self, pinned: bool) -> Value {
        json!({
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "requires": Vec::<String>::new(),
            "ready": true,
            "dynamic": true,
            "runnable": true,
            "world": pumper_engine_wasm::app_host::WORLD,
            "module_sha256": self.sha256,
            "pinned": pinned,
            "has_params_schema": self.manifest.params_schema.is_some(),
            "params_schema": self
                .manifest
                .params_schema
                .clone()
                .unwrap_or(Value::Null),
        })
    }
}

#[async_trait::async_trait]
impl ScrapeApp for DynamicApp {
    fn name(&self) -> &'static str {
        self.name
    }

    fn description(&self) -> &'static str {
        self.description
    }

    fn schedule(&self) -> Option<&'static str> {
        self.schedule
    }

    fn default_params(&self) -> Value {
        self.default_params.clone()
    }

    fn manifest(&self) -> AppManifest {
        self.manifest.clone()
    }

    async fn run(&self, ctx: pumper_core::app::AppContext) -> pumper_core::Result<Value> {
        let (mut result, stats) = self.host.run(self.name, ctx).await?;
        // The run's own cost, attached to the RESULT (never to the records) for
        // the same reason plugin fuel is: a per-record cost would mark every
        // record changed on every re-run.
        if let Value::Object(map) = &mut result {
            let cfg = self.host.config();
            map.entry("wasm")
                .or_insert_with(|| stats.to_json(cfg.fuel_per_job, cfg.max_memory_bytes()));
            map.entry("module_sha256")
                .or_insert_with(|| Value::String(self.sha256.clone()));
        }
        Ok(result)
    }
}

/// The listing entry for a component that was found and understood but NOT
/// registered. Deliberately shaped like [`dynamic_entry`]'s output — same keys,
/// same `runnable: false` — with the module's digest attached, because "which
/// build is this file" is the first question every one of these reasons raises.
fn unrunnable_component_entry(
    name: &str,
    manifest: &Value,
    sha256: &str,
    pinned: bool,
    reason: &str,
) -> Value {
    let description = manifest
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("(dynamic app: manifest not read)");
    let params_schema = manifest.get("params_schema").cloned();
    json!({
        "name": name,
        "description": description,
        "schedule": Value::Null,
        "requires": ["config:wasm_apps.enabled"],
        "ready": false,
        "dynamic": true,
        "runnable": false,
        "reason": reason,
        "world": pumper_engine_wasm::app_host::WORLD,
        "module_sha256": sha256,
        "pinned": pinned,
        "has_params_schema": params_schema.is_some(),
        "params_schema": params_schema.unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod component_tests {
    use super::{
        pinned_module_hash, unrunnable_component_entry, validated_manifest,
        WASM_APPS_DISABLED_REASON,
    };
    use serde_json::json;

    const SHA: &str = "aa11bb22cc33dd44ee55ff66aa77bb88cc99dd00ee11ff22aa33bb44cc55dd66";

    /// A pin is looked up by `id` OR by `app`, because both spellings occur in
    /// the shipped catalog — and never off a non-`wasm` row, whose hash nothing
    /// hashes. A lookup that only matched `id` would leave half the rows
    /// silently unpinned, which reads exactly like "no pin declared".
    #[test]
    fn a_pin_is_found_by_id_or_app_and_never_off_a_non_wasm_row() {
        let catalog = pumper_core::Catalog::parse(&format!(
            r#"
[[source]]
id = "quotes"
name = "Quotes"
status = "planned"
engine = "wasm"
module_sha256 = "{SHA}"

[[source]]
id = "quotes-2"
app = "echo"
name = "Echo"
status = "planned"
engine = "wasm"
module_sha256 = "{SHA}"

[[source]]
id = "grants-gov"
name = "Grants"
status = "live"
engine = "http"
"#
        ))
        .expect("catalog parses");
        assert_eq!(pinned_module_hash(&catalog, "quotes").as_deref(), Some(SHA));
        assert_eq!(pinned_module_hash(&catalog, "echo").as_deref(), Some(SHA));
        assert_eq!(pinned_module_hash(&catalog, "grants-gov"), None);
        assert_eq!(pinned_module_hash(&catalog, "unlisted"), None);
    }

    /// The guard the compiled-in apps get by test, applied to modules nobody
    /// reviewed: an example that its OWN schema rejects is worse than no
    /// example, because an agent copies it and the enqueue door — validating
    /// the same schema — answers 422.
    #[test]
    fn an_example_that_fails_its_own_schema_is_refused_not_listed() {
        let good = json!({
            "description": "quotes",
            "params_schema": {"type": "object", "required": ["page"],
                              "properties": {"page": {"type": "integer"}}},
            "examples": [{"description": "first page", "params": {"page": 1}}]
        });
        let manifest = validated_manifest(&good).expect("a coherent manifest loads");
        assert_eq!(manifest.examples.len(), 1);

        let bad = json!({
            "params_schema": {"type": "object", "required": ["page"],
                              "properties": {"page": {"type": "integer"}}},
            "examples": [{"description": "broken", "params": {"page": "one"}}]
        });
        let err = validated_manifest(&bad).expect_err("must refuse");
        assert!(err.contains("examples[0]"), "{err}");
    }

    /// The rest of the manifest contract, each refused by NAME rather than
    /// degraded to a default — a dynamic app that claims an unknown cost class
    /// would otherwise silently become `free`, which is the one claim that
    /// changes whether a caller sets a budget.
    #[test]
    fn a_manifest_defect_is_named_not_defaulted() {
        assert!(validated_manifest(&json!("not an object")).is_err());
        assert!(validated_manifest(&json!({"params_schema": {"type": 7}})).is_err());
        let err = validated_manifest(&json!({"cost_class": "cheap"})).expect_err("must refuse");
        assert!(err.contains("cost_class"), "{err}");
        let err =
            validated_manifest(&json!({"examples": [{"params": {}}]})).expect_err("must refuse");
        assert!(err.contains("description"), "{err}");
        // The empty manifest is legal: it declares nothing, exactly like the
        // default `AppManifest` a compiled-in app gets for free.
        assert!(validated_manifest(&json!({})).is_ok());
    }

    /// Every not-registered path stays visible and says why. A component that
    /// vanished from the listing because a hash did not match would look like a
    /// missing file, which is the one diagnosis that sends an operator to the
    /// wrong place.
    #[test]
    fn a_refused_component_is_listed_with_its_reason_not_hidden() {
        for (reason, pinned) in [(WASM_APPS_DISABLED_REASON, false), ("refused: pin", true)] {
            let entry = unrunnable_component_entry("quotes", &json!({}), "ab12", pinned, reason);
            assert_eq!(entry["name"], "quotes");
            assert_eq!(entry["dynamic"], true);
            assert_eq!(entry["runnable"], false);
            assert_eq!(entry["ready"], false);
            assert_eq!(entry["module_sha256"], "ab12");
            assert_eq!(entry["pinned"], pinned);
            assert_eq!(entry["reason"], reason);
        }
    }
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
        apps(&pumper_core::Config::default())
            .iter()
            .map(|a| a.name())
            .collect()
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

    /// Registration is not publication. The previous check only asked whether a
    /// declared publisher **exists**, which is why `cordis` — a registered EU
    /// funding app that writes only its own two datasets and never calls
    /// `finalize_unified` — sat in the `grants` publisher list vouching for
    /// records it will never write, and `publishes_into("cordis")` redirected
    /// operators to a namespace none of its revisions ever reach.
    ///
    /// The pin available from this crate is the app's own manifest: a
    /// cross-source publisher describes the shared layer's result block in its
    /// declared `output_shape`. (The converse is deliberately NOT asserted — a
    /// `unified` block is a shared idiom, and the trades apps have their own —
    /// and it does not need to be: this list is only a bootstrap seed, and the
    /// running authority for "what can be watched" is the store, which knows
    /// every namespace that actually holds a record.)
    #[test]
    fn a_namespace_never_names_a_publisher_that_writes_nothing_into_it() {
        let apps = apps(&pumper_core::Config::default());
        for ns in VIRTUAL_NAMESPACES {
            for publisher in ns.publishers {
                let app = apps
                    .iter()
                    .find(|a| a.name() == *publisher)
                    .expect("checked registered above");
                let shape = app.manifest().output_shape.unwrap_or("");
                // The evidence a publisher owes is that its declared result
                // NAMES the shared layer it publishes into. Two spellings exist
                // because two families do this: the grants and trades joins
                // report a `unified` block, and the census/market family reports
                // its shared writes under the namespace's own name
                // (`market_blend`, `market_profile`). Anything else is a manifest
                // that does not mention the shared write at all.
                assert!(
                    shape.contains("unified") || shape.contains(ns.name),
                    "virtual namespace '{}' claims publisher '{publisher}', whose manifest \
                     describes neither a cross-source unified block nor a '{}' \
                     one — either it never publishes there (drop it from the \
                     seed) or its manifest is stale",
                    ns.name,
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
        // An app that publishes only under its own name has no redirect to give
        // — including `cordis`, whose records land in `cordis/*` and nowhere
        // else, so a redirect would have sent an operator to the wrong place.
        assert!(publishes_into("hackernews").is_none());
        assert!(publishes_into("cordis").is_none());
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
        for app in apps(&pumper_core::Config::default()) {
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
                    .map(|e| format!("{}: {e}", e.instance_path()))
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
        for app in apps(&pumper_core::Config::default()) {
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
                .map(|e| format!("{}: {e}", e.instance_path()))
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
/// **The materiality inventory.** The sibling of
/// `completeness_inventory_tests`, one layer down: that one pins "did this run
/// get enough?", this one pins "is this change news?".
///
/// The defect it ratchets is identical in shape and was found in the same
/// context. Four monitors lived in `crates/apps/` with four different answers —
/// `watch` had none and reported the store's raw exact-hash `ChangeKind`,
/// `connector-api-watch` had two (an empty line diff plus a Claude severity
/// ladder), `plugin`'s observatory had a third, `hackernews` had none — and a
/// fifth, `pumper_core::crawl::change_weight`, was already correct in `core`
/// and reached by nobody. None of them could see the others, because apps may
/// not depend on apps.
#[cfg(test)]
mod materiality_inventory_tests {
    use super::apps;
    use std::collections::BTreeSet;

    /// The block a monitor puts in its declared `output_shape` once its answer
    /// to "is this change news?" is a VERDICT from `pumper_core::materiality`
    /// rather than a private rule. The brace is part of the marker: the word
    /// alone appears in prose, the block is a declaration.
    const VERDICT_MARKER: &str = "materiality {";

    /// **EXACTLY** the registered apps that declare a materiality verdict.
    /// Pinned as a set rather than a count, so a monitor that is added — or one
    /// that quietly stops declaring — fails BY NAME instead of drifting.
    const JUDGED_MONITORS: [&str; 2] = ["connector-api-watch", "watch"];

    /// The monitors that still answer privately. They are listed, not
    /// tolerated: `the_un_judged_monitor_list_only_ever_shrinks` fails the
    /// moment one adopts the judge, which forces the entry out rather than
    /// letting a permanent exemption accumulate.
    ///
    /// Out of this round's write set on purpose. `plugin`'s observatory already
    /// solved its own case correctly with `DerivedPaths`, and `hackernews`'s
    /// volatile `points`/`comments` is a *completeness* question entangled with
    /// a materiality one — bundling them is how a card stops being falsifiable.
    const MONITORS_AWAITING_THE_JUDGE: [&str; 2] = ["hackernews", "plugin"];

    fn shape_of(name: &str) -> String {
        apps(&pumper_core::Config::default())
            .iter()
            .find(|a| a.name() == name)
            .unwrap_or_else(|| panic!("'{name}' is not a registered app — the list is stale"))
            .manifest()
            .output_shape
            .unwrap_or("")
            .to_string()
    }

    /// The EXPECTED-diff, reported in both directions so neither a new monitor
    /// nor a silently-dropped verdict can slide past.
    #[test]
    fn the_monitors_that_declare_a_materiality_verdict_are_exactly_the_listed_ones() {
        let found: BTreeSet<String> = apps(&pumper_core::Config::default())
            .iter()
            .filter(|a| {
                a.manifest()
                    .output_shape
                    .unwrap_or("")
                    .contains(VERDICT_MARKER)
            })
            .map(|a| a.name().to_string())
            .collect();
        let expected: BTreeSet<String> = JUDGED_MONITORS.iter().map(|n| n.to_string()).collect();
        let added: Vec<&String> = found.difference(&expected).collect();
        let gone: Vec<&String> = expected.difference(&found).collect();
        assert!(
            added.is_empty() && gone.is_empty(),
            "the materiality inventory drifted. App(s) newly declaring a verdict: \
             {added:?} — add them to JUDGED_MONITORS. Listed app(s) that no longer \
             declare one: {gone:?} — a verdict that vanished means that monitor went \
             back to answering 'is this change news?' privately, which is exactly how \
             four monitors ended up with four answers."
        );
    }

    /// Every listed monitor must be a registered app with an `output_shape` —
    /// a hand-kept list that outlives its apps is a lie.
    #[test]
    fn every_listed_monitor_is_registered() {
        for name in JUDGED_MONITORS
            .iter()
            .chain(MONITORS_AWAITING_THE_JUDGE.iter())
        {
            assert!(
                !shape_of(name).is_empty(),
                "app '{name}' declares no output_shape at all, so nothing can be \
                 asserted about its verdict"
            );
        }
    }

    /// The exemption list is a countdown, not a carve-out, and it may not grow:
    /// a fifth private answer to "is this change news?" is the defect, not a
    /// new entry.
    #[test]
    fn the_un_judged_monitor_list_only_ever_shrinks() {
        for name in MONITORS_AWAITING_THE_JUDGE {
            assert!(
                !shape_of(name).contains(VERDICT_MARKER),
                "app '{name}' now declares a `{VERDICT_MARKER}` block, so it is no \
                 longer awaiting the judge — drop it from MONITORS_AWAITING_THE_JUDGE"
            );
        }
        assert!(
            MONITORS_AWAITING_THE_JUDGE.len() <= 2,
            "the un-judged monitor list grew; a new monitor routes through \
             `pumper_core::materiality`, it does not join the exemption"
        );
    }
}

#[cfg(test)]
mod completeness_inventory_tests {
    use super::apps;
    use std::collections::BTreeSet;

    /// The word an app puts in its declared `output_shape` once its
    /// completeness floor became a VERDICT rather than a private const.
    const VERDICT_MARKER: &str = "completeness";
    /// The legacy vocabulary: every floor written before the judge existed
    /// described itself as a `floor` in the block it reported.
    const LEGACY_FLOOR_MARKER: &str = "floor";

    /// **EXACTLY** the apps that route a completeness floor through
    /// `pumper_core::completeness`. Pinned as a set rather than a count, so a
    /// market-data app that is added — or one that quietly stops reporting its
    /// verdict — fails this test BY NAME instead of drifting.
    ///
    /// That naming is the durable change of this round: the silent-partial-
    /// success defect had been fixed four times by four authors, in four
    /// vocabularies, and generalised zero times. The guard that stops a fifth
    /// private fix is a list that cannot be out of date.
    const JUDGED: [&str; 9] = [
        "homewyse-pricing",
        "mpsv-ciselniky",
        "mpsv-ispv",
        "mpsv-vpm",
        "smlouvy-dump-watch",
        "state-licensing",
        "state-tax",
        "trade-wages",
        "valuation-multiples",
    ];

    /// The ONE app that still judges its floor privately. It is listed, not
    /// tolerated: `the_un_judged_list_only_ever_shrinks` fails the moment it
    /// routes through the judge, which forces the entry out rather than letting
    /// a permanent exemption accumulate.
    ///
    /// Out of this round's write set on purpose — `hackernews` is the generic
    /// template app, not a market-data one, and its parsed/seen share over a
    /// fixed-size front page moves with its own change.
    const AWAITING_THE_JUDGE: [&str; 1] = ["hackernews"];

    fn shape_of(name: &str) -> String {
        apps(&pumper_core::Config::default())
            .iter()
            .find(|a| a.name() == name)
            .unwrap_or_else(|| panic!("'{name}' is not a registered app — the list is stale"))
            .manifest()
            .output_shape
            .unwrap_or("")
            .to_string()
    }

    /// The EXPECTED-diff, reported in both directions so neither a new app nor
    /// a silently-dropped verdict can slide past.
    #[test]
    fn the_apps_that_declare_a_verdict_are_exactly_the_listed_ones() {
        let found: BTreeSet<String> = apps(&pumper_core::Config::default())
            .iter()
            .filter(|a| {
                a.manifest()
                    .output_shape
                    .unwrap_or("")
                    .contains(VERDICT_MARKER)
            })
            .map(|a| a.name().to_string())
            .collect();
        let expected: BTreeSet<String> = JUDGED.iter().map(|n| n.to_string()).collect();
        let added: Vec<&String> = found.difference(&expected).collect();
        let gone: Vec<&String> = expected.difference(&found).collect();
        assert!(
            added.is_empty() && gone.is_empty(),
            "the completeness inventory drifted. App(s) newly declaring a verdict: \
             {added:?} — add them to JUDGED. Listed app(s) that no longer declare one: \
             {gone:?} — a verdict that vanished is how the silent-partial-success \
             defect comes back."
        );
    }

    /// **The ratchet.** An app that still describes a `floor` in the legacy
    /// vocabulary must also report the verdict that floor produces. Computing
    /// the judgment and discarding it is the exact shape four of five apps
    /// shipped: a 1-of-5 run was a green job carrying a string in `warnings[]`
    /// that nothing reads.
    #[test]
    fn an_app_that_declares_a_floor_also_declares_its_verdict() {
        for app in apps(&pumper_core::Config::default()) {
            let name = app.name();
            if AWAITING_THE_JUDGE.contains(&name) {
                continue;
            }
            let shape = app.manifest().output_shape.unwrap_or("");
            if !shape.contains(LEGACY_FLOOR_MARKER) {
                continue;
            }
            assert!(
                shape.contains(VERDICT_MARKER),
                "app '{name}' declares a completeness floor but no `{VERDICT_MARKER}` \
                 block — it computes the judgment and throws it away. Judge it with \
                 `pumper_core::completeness` and report `completeness` in its \
                 output_shape."
            );
        }
    }

    /// Every judged app must be registered, and every app named in either list
    /// must exist — a hand-kept list that outlives its apps is a lie.
    #[test]
    fn every_listed_app_is_registered() {
        for name in JUDGED.iter().chain(AWAITING_THE_JUDGE.iter()) {
            assert!(
                !shape_of(name).is_empty(),
                "app '{name}' declares no output_shape at all, so nothing can be \
                 asserted about its verdict"
            );
        }
    }

    /// The exemption list is a countdown, not a carve-out: once an app routes
    /// through the judge, leaving it here is itself the failure.
    #[test]
    fn the_un_judged_list_only_ever_shrinks() {
        for name in AWAITING_THE_JUDGE {
            assert!(
                !shape_of(name).contains(VERDICT_MARKER),
                "app '{name}' now declares `{VERDICT_MARKER}`, so it is no longer \
                 awaiting the judge — drop it from AWAITING_THE_JUDGE"
            );
        }
        assert!(
            AWAITING_THE_JUDGE.len() <= 1,
            "the un-judged floor list grew; a new app must route through \
             `pumper_core::completeness`, not join the exemption"
        );
    }
}

// ---- The catalog denominator -------------------------------------------------
//
// `ONBOARDING.md` §10 says adding an app REQUIRES its `[[source]]` row in
// `catalog/data-sources.toml`. For most of this registry's life that rule was
// a sentence, and nine registered apps missed it. The roster below plus
// `the_registered_apps_are_exactly_the_sourced_ones_plus_the_roster` turn it
// into a gate, and `pumper_core::uncovered_apps` renders the same join on
// `GET /sources` so an operator never has to diff two files by hand.

/// A registered app that deliberately has no `[[source]]` row.
pub(crate) struct NonSourceApp {
    /// The app's `name()`.
    pub name: &'static str,
    /// Why it is not a catalog source. An exemption without a reason is how a
    /// roster becomes a dumping ground.
    pub reason: &'static str,
}

/// Registered apps that are NOT data sources, each with its reason.
///
/// The bar is narrow on purpose: an app qualifies only when there is no single
/// upstream to declare — generic tooling the caller points at a URL, or a
/// control-plane app that writes nothing scheduled. "This source belongs to a
/// sibling product" is NOT a reason; such an app still publishes a dataset on
/// a cadence, and leaving it out is exactly the drift that made `declared: 12`
/// unreadable. Every app here is one whose dataset no catalog row could ever
/// describe.
pub(crate) const NON_SOURCE_APPS: &[NonSourceApp] = &[
    NonSourceApp {
        name: "crawl",
        reason: "generic multi-page crawler; the caller supplies the seed URL",
    },
    NonSourceApp {
        name: "extractor",
        reason: "generic rule-driven extractor; the caller supplies the page and the rules",
    },
    NonSourceApp {
        name: "peer",
        reason: "pulls from another Pumper instance into a caller-named namespace — \
                 its upstream is whatever remote the job points at",
    },
    NonSourceApp {
        name: "plugin",
        reason: "runs a WASM guest from data/plugins/; the plugin, not the app, has the source",
    },
    NonSourceApp {
        name: "provisioner",
        reason: "an on-demand compiler that PROPOSES catalog rows for human review; it \
                 deliberately never appears in the catalog it writes proposals for",
    },
    NonSourceApp {
        name: "readable",
        reason: "generic article extractor; the caller supplies the URL",
    },
    NonSourceApp {
        name: "transact",
        reason: "an on-demand dry-run browser-flow executor that writes no dataset at all \
                 (no ctx.upsert/sync call in crates/apps/transact/src/lib.rs), so there is \
                 nothing for a contract or a staleness window to attach to",
    },
    NonSourceApp {
        name: "watch",
        reason: "generic change watcher; the caller supplies the URL",
    },
];

/// The roster as it is rendered on `GET /sources`: one `{app, reason}` object
/// per entry, in declaration order.
///
/// The reasons exist to be READ. A `uncovered_apps: []` beside a roster nobody
/// can see is an unexplained claim, and an exemption whose justification lives
/// only in a source comment is how the previous 18-entry list drifted into a
/// scope argument for nine publishing apps.
pub(crate) fn non_source_roster() -> Value {
    Value::Array(
        NON_SOURCE_APPS
            .iter()
            .map(|e| json!({ "app": e.name, "reason": e.reason }))
            .collect(),
    )
}

/// The names on [`NON_SOURCE_APPS`], for the catalog join.
pub(crate) fn non_source_app_names() -> Vec<&'static str> {
    NON_SOURCE_APPS.iter().map(|e| e.name).collect()
}

/// Every registered app's `name()`.
pub(crate) fn app_names() -> Vec<&'static str> {
    apps(&pumper_core::Config::default())
        .iter()
        .map(|a| a.name())
        .collect()
}

#[cfg(test)]
mod catalog_coverage_tests {
    use super::{app_names, non_source_app_names, NON_SOURCE_APPS};
    use pumper_core::Catalog;
    use std::collections::BTreeSet;

    /// Embedded at compile time so the check does not depend on the test's
    /// working directory — the same trick `routes::catalog_tests` uses.
    const CATALOG_TOML: &str = include_str!("../../../catalog/data-sources.toml");

    fn catalog() -> Catalog {
        Catalog::parse(CATALOG_TOML).expect("catalog/data-sources.toml parses")
    }

    /// **The gate.** ONBOARDING §10 as an EXPECTED-diff, reported in both
    /// directions: an app with neither a live `[[source]]` row nor a
    /// `NON_SOURCE_APPS` entry fails BY NAME, and a roster entry for an app
    /// that has since grown a row fails too, so the exemption list cannot
    /// outlive its reason.
    #[test]
    fn the_registered_apps_are_exactly_the_sourced_ones_plus_the_roster() {
        let catalog = catalog();
        let sourced = catalog.sourced_apps();
        let exempt = non_source_app_names();
        let registered = app_names();

        let uncovered = pumper_core::uncovered_apps(&registered, &exempt, &sourced);
        assert!(
            uncovered.is_empty(),
            "registered app(s) with no live [[source]] row and no NON_SOURCE_APPS entry: \
             {uncovered:?}. ONBOARDING §10: add the row to catalog/data-sources.toml, or \
             add the app to NON_SOURCE_APPS with the reason it can never have one."
        );

        let redundant: Vec<&str> = exempt
            .iter()
            .copied()
            .filter(|name| sourced.contains(name))
            .collect();
        assert!(
            redundant.is_empty(),
            "NON_SOURCE_APPS claims {redundant:?} can never have a catalog row, but the \
             catalog now names them. Drop the exemption — a stale carve-out is how the \
             roster stops meaning anything."
        );
    }

    /// An exemption for an app nobody registers is dead weight that hides the
    /// next real gap.
    #[test]
    fn every_roster_entry_is_a_registered_app_with_a_reason() {
        let registered: BTreeSet<&str> = app_names().into_iter().collect();
        for entry in NON_SOURCE_APPS {
            assert!(
                registered.contains(entry.name),
                "NON_SOURCE_APPS names '{}', which is not a registered app",
                entry.name
            );
            assert!(
                entry.reason.len() > 20,
                "NON_SOURCE_APPS entry '{}' has no real reason",
                entry.name
            );
        }
        let names: BTreeSet<&str> = NON_SOURCE_APPS.iter().map(|e| e.name).collect();
        assert_eq!(
            names.len(),
            NON_SOURCE_APPS.len(),
            "NON_SOURCE_APPS has a duplicate entry"
        );
    }

    /// The roster `GET /sources` renders must carry every entry WITH its
    /// reason — `transact` specifically, because it writes no dataset at all
    /// and is therefore the clearest case that the bar is "no upstream to
    /// declare", not "out of scope".
    #[test]
    fn the_rendered_roster_carries_a_reason_for_every_entry() {
        let rendered = super::non_source_roster();
        let rows = rendered.as_array().expect("an array of {app, reason}");
        assert_eq!(rows.len(), NON_SOURCE_APPS.len());
        for row in rows {
            assert!(row["app"].as_str().is_some_and(|a| !a.is_empty()), "{row}");
            assert!(
                row["reason"].as_str().is_some_and(|r| r.len() > 20),
                "an exemption without a readable reason: {row}"
            );
        }
        let transact = rows
            .iter()
            .find(|r| r["app"] == "transact")
            .expect("transact is on the roster");
        assert!(
            transact["reason"].as_str().unwrap().contains("no dataset"),
            "transact's reason must say it writes no dataset: {transact}"
        );
    }

    /// The roster is a carve-out for tooling, not a scope argument. It must
    /// never regrow into the "sibling-product consumer" exemption that hid
    /// nine publishing apps from governance.
    #[test]
    fn the_roster_never_exempts_an_app_that_publishes_a_dataset() {
        const NEVER_EXEMPT: &[&str] = &[
            "census-density",
            "census-nonemp",
            "cms-fee-schedule",
            "hackernews",
            "homewyse-pricing",
            "smlouvy-dump-watch",
            "state-tax",
            "trade-wages",
            "valuation-multiples",
        ];
        let names: BTreeSet<&str> = NON_SOURCE_APPS.iter().map(|e| e.name).collect();
        let regressed: Vec<&&str> = NEVER_EXEMPT
            .iter()
            .filter(|n| names.contains(**n))
            .collect();
        assert!(
            regressed.is_empty(),
            "{regressed:?} publish datasets and were exempted once already — they get a \
             [[source]] row, not an exemption"
        );
    }
}
