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
    ]
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
