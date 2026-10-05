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
        // `grants_common::UNIFIED_APP`. Not imported: `pumper-server` depends on the
        // grant source apps, not on `grants-common`; `virtual_namespace_publishers_are_registered`
        // pins the entry against the registry instead.
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
    pumper_engine_wasm::discover_dynamic_apps_with(dir, cfg)
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
        let apps = apps();
        for ns in VIRTUAL_NAMESPACES {
            for publisher in ns.publishers {
                let app = apps
                    .iter()
                    .find(|a| a.name() == *publisher)
                    .expect("checked registered above");
                let shape = app.manifest().output_shape.unwrap_or("");
                assert!(
                    shape.contains("unified"),
                    "virtual namespace '{}' claims publisher '{publisher}', whose manifest \
                     describes no cross-source unified block — either it never publishes \
                     there (drop it from the seed) or its manifest is stale",
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
    const JUDGED: [&str; 8] = [
        "homewyse-pricing",
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
        apps()
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
        let found: BTreeSet<String> = apps()
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
        for app in apps() {
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
        name: "research",
        reason: "generic Claude research runner; the caller supplies the question",
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
    apps().iter().map(|a| a.name()).collect()
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
