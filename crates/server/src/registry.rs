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
        Arc::new(app_readable::Readable),
        Arc::new(app_cms_fee_schedule::CmsFeeSchedule),
    ]
}
