use std::sync::Arc;

use pumper_core::ScrapeApp;

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
        Arc::new(app_grants_gov::GrantsGov),
        Arc::new(app_cms_fee_schedule::CmsFeeSchedule),
        Arc::new(app_census_density::CensusDensity),
        Arc::new(app_census_nonemp::CensusNonemp),
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
    ]
}
