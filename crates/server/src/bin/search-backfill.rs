//! Rebuild the full-text search index from stored dataset records.
//!
//! The search index is a derived artifact and can go silently empty — the schema
//! -drift branch in `TantivyIndex::new` wipes it, a spell of `[search] enabled =
//! false` leaves that window unindexed, and a lost/corrupt index dir rebuilds
//! empty. In every case queries keep returning `200` with fewer hits, and the
//! only refill was the worker's post-job `index()` call — so a dataset became
//! searchable again only when its app happened to run next (days for a weekly
//! schedule, never for a retired app). This walks the stored records and rebuilds
//! from them, using the SAME `SearchDoc::from_dataset_record` builder the live
//! path uses, so ids are stable (`<app>:<dataset>:<key>`) and it upserts rather
//! than duplicates — safe to run against a partially-populated index.
//!
//! Run with the server STOPPED — Tantivy holds an exclusive writer lock on the
//! index directory, so a running server (with search enabled) blocks this.
//!
//! A scope is required, so a broad rebuild is always deliberate — note that the
//! live worker path only incrementally maintains datasets an app names in its
//! result's `index_datasets` (today just `grants/unified`), so backfilling other
//! datasets makes them searchable but they won't be kept current by normal runs.
//!
//! Usage:
//!   cargo run -p pumper-server --bin search-backfill -- --app grants --dataset unified
//!   cargo run -p pumper-server --bin search-backfill -- --app grants   # all of an app's datasets
//!   cargo run -p pumper-server --bin search-backfill -- --all          # every dataset

use pumper_core::{Config, Datasets, Search, SearchDoc, Storage};
use pumper_engine_search::TantivyIndex;

/// Records indexed per commit — matches the batch shape of the live path.
const INDEX_CHUNK: usize = 500;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .init();

    let config = Config::load()?;
    if !config.search.enabled {
        anyhow::bail!(
            "search is disabled ([search] enabled = false); enable it before backfilling"
        );
    }

    let storage = Storage::connect(&config.storage).await?;
    let datasets = Datasets::new(storage.pool());
    let search = TantivyIndex::new(&config.search)?;

    let targets = resolve_targets(&datasets).await?;
    if targets.is_empty() {
        println!("no datasets to backfill");
        return Ok(());
    }
    tracing::info!(datasets = targets.len(), "backfilling search index");

    let mut total: u64 = 0;
    for (app, dataset) in targets {
        // Local datasets are small; one read, then index in commit-sized chunks.
        let records = datasets.list(&app, &dataset, 1_000_000).await?;
        let mut buf: Vec<SearchDoc> = Vec::with_capacity(INDEX_CHUNK);
        let mut indexed: u64 = 0;
        for rec in records {
            if rec.removed_at.is_some() {
                continue; // tombstoned rows are not searchable
            }
            buf.push(SearchDoc::from_dataset_record(
                &app,
                &dataset,
                &rec.key,
                &rec.data,
                rec.updated_at.timestamp(),
            ));
            indexed += 1;
            if buf.len() >= INDEX_CHUNK {
                search.index(std::mem::take(&mut buf)).await?;
            }
        }
        if !buf.is_empty() {
            search.index(buf).await?;
        }
        tracing::info!(%app, %dataset, indexed, "backfilled dataset");
        total += indexed;
    }

    // index() defers its commit to a background committer, but this process exits
    // right after — flush so the tail is durable and doc_count is accurate.
    search.flush().await?;
    let doc_count = search.doc_count().await?;
    println!(
        "search backfill complete: {total} record(s) indexed; index now holds {doc_count} document(s)"
    );
    Ok(())
}

/// `(app, dataset)` targets from the CLI scope. A scope is required so a
/// full-index rebuild is never accidental: `--app X --dataset Y` for one dataset,
/// `--app X` for all of an app's datasets, `--all` for every dataset.
async fn resolve_targets(datasets: &Datasets) -> anyhow::Result<Vec<(String, String)>> {
    let args: Vec<String> = std::env::args().collect();
    let has = |name: &str| args.iter().any(|a| a == name);
    let flag = |name: &str| {
        args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
    };
    match (flag("--app"), flag("--dataset"), has("--all")) {
        (Some(app), Some(dataset), _) => Ok(vec![(app, dataset)]),
        (Some(app), None, _) => Ok(datasets
            .datasets(&app)
            .await?
            .into_iter()
            .map(|d| (app.clone(), d))
            .collect()),
        (None, Some(_), _) => anyhow::bail!("--dataset requires --app"),
        (None, None, true) => Ok(datasets.list_all_datasets().await?),
        (None, None, false) => {
            anyhow::bail!("specify a scope: --all, --app <app>, or --app <app> --dataset <dataset>")
        }
    }
}
