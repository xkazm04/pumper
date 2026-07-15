//! One-shot maintenance: recompute every record's SimHash from its stored JSON.
//!
//! Run this after the SimHash token hash changes. Fingerprints from the old hash
//! are not comparable with new ones, so a `records` table holding a mix of both
//! produces meaningless Hamming distances — near-duplicate detection and
//! `/datasets/{app}/{dataset}/duplicates` silently return wrong answers until
//! every row is recomputed.
//!
//! Only the derived `simhash` column is rewritten; `data`, `hash` and the
//! timestamps are untouched, so the change-feed sees no spurious revisions.
//!
//! Usage (with the server stopped):
//!   cargo run -p pumper-server --bin reindex

use pumper_core::{Config, Datasets, Storage};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .init();

    let config = Config::load()?;
    tracing::info!(db = %config.storage.database_path.display(), "reindexing record simhashes");

    let storage = Storage::connect(&config.storage).await?;
    let datasets = Datasets::new(storage.pool());

    let changed = datasets.reindex_simhashes().await?;

    tracing::info!(changed, "simhash reindex complete");
    println!("simhash reindex complete: {changed} record fingerprint(s) rewritten");
    Ok(())
}
