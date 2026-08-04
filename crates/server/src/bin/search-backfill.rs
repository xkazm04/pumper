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
//! Tombstoned rows are **purged**, not skipped: their document may already be in
//! the index (indexed while live, then removed during a window the live delete
//! path missed, or removed into an index that was later wiped), and leaving it
//! there is exactly the stale-hit state a rebuild is run to repair.
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
    let mut total_purged: u64 = 0;
    for (app, dataset) in targets {
        // Local datasets are small; one read, then index in commit-sized chunks.
        let records = datasets.list(&app, &dataset, 1_000_000).await?;
        let mut buf: Vec<SearchDoc> = Vec::with_capacity(INDEX_CHUNK);
        let mut tombstoned: Vec<String> = Vec::new();
        let mut indexed: u64 = 0;
        for rec in records {
            match backfill_action(&app, &dataset, &rec) {
                BackfillAction::Index(doc) => {
                    buf.push(doc);
                    indexed += 1;
                    if buf.len() >= INDEX_CHUNK {
                        search.index(std::mem::take(&mut buf)).await?;
                    }
                }
                BackfillAction::Purge(id) => tombstoned.push(id),
            }
        }
        if !buf.is_empty() {
            search.index(buf).await?;
        }
        // A tombstoned row is not merely "not indexed": its document may ALREADY
        // be in the index (indexed while live, then removed during a window the
        // live delete path missed — or the delete landed in an index that was
        // later wiped). Skipping it left that ghost queryable forever, which is
        // exactly the state a backfill is run to repair.
        let purged = tombstoned.len() as u64;
        if !tombstoned.is_empty() {
            search.delete_ids(&tombstoned).await?;
        }
        tracing::info!(%app, %dataset, indexed, purged, "backfilled dataset");
        total += indexed;
        total_purged += purged;
    }

    // index() defers its commit to a background committer, but this process exits
    // right after — flush so the tail is durable and doc_count is accurate.
    search.flush().await?;
    let doc_count = search.doc_count().await?;
    println!(
        "search backfill complete: {total} record(s) indexed, {total_purged} tombstoned \
         record(s) purged; index now holds {doc_count} document(s)"
    );
    Ok(())
}

/// What a backfill does with one stored record. A tombstoned row is not a no-op:
/// its doc id must be purged, because it may already sit in the index.
enum BackfillAction {
    Index(SearchDoc),
    Purge(String),
}

/// Classifies one stored record. Uses the SAME id/doc builders as the live worker
/// path (`SearchDoc::{from_dataset_record, dataset_id}`), so an index and a purge
/// address exactly the same document.
fn backfill_action(app: &str, dataset: &str, rec: &pumper_core::Record) -> BackfillAction {
    if rec.removed_at.is_some() {
        return BackfillAction::Purge(SearchDoc::dataset_id(app, dataset, &rec.key));
    }
    BackfillAction::Index(SearchDoc::from_dataset_record(
        app,
        dataset,
        &rec.key,
        &rec.data,
        rec.updated_at.timestamp(),
    ))
}

/// `(app, dataset)` targets from the CLI scope. A scope is required so a
/// full-index rebuild is never accidental: `--app X --dataset Y` for one dataset,
/// `--app X` for all of an app's datasets, `--all` for every dataset.
async fn resolve_targets(datasets: &Datasets) -> anyhow::Result<Vec<(String, String)>> {
    let args: Vec<String> = std::env::args().collect();
    let has = |name: &str| args.iter().any(|a| a == name);
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
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

#[cfg(test)]
mod tests {
    use super::{backfill_action, BackfillAction};
    use chrono::{TimeZone, Utc};
    use pumper_core::config::SearchConfig;
    use pumper_core::{Record, Search, SearchDoc, SearchRequest};
    use pumper_engine_search::TantivyIndex;

    fn record(key: &str, removed: bool) -> Record {
        let t = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        Record {
            key: key.into(),
            data: serde_json::json!({"title": format!("Grant {key}"), "url": format!("https://x/{key}")}),
            first_seen: t,
            last_seen: t,
            updated_at: t,
            removed_at: removed.then_some(t),
            trust: "stable".into(),
        }
    }

    /// The anti-pattern: `if rec.removed_at.is_some() { continue }` — the
    /// tombstoned row is skipped, so its already-indexed document survives the
    /// very rebuild that was supposed to repair the index.
    #[test]
    fn tombstoned_records_are_purged_not_merely_skipped() {
        match backfill_action("grants", "unified", &record("gone", true)) {
            BackfillAction::Purge(id) => assert_eq!(id, "grants:unified:gone"),
            BackfillAction::Index(_) => panic!("a tombstoned record must never be indexed"),
        }
        match backfill_action("grants", "unified", &record("live", false)) {
            BackfillAction::Index(doc) => {
                assert_eq!(doc.id, "grants:unified:live");
                assert_eq!(doc.title, "Grant live");
                assert_eq!(doc.indexed_at, 1_700_000_000);
            }
            BackfillAction::Purge(_) => panic!("a live record must be indexed"),
        }
    }

    /// Purge ids and index ids must be produced by the same builder, or a purge
    /// silently addresses a document that does not exist.
    #[test]
    fn purge_id_matches_the_id_the_same_record_would_be_indexed_under() {
        let live = match backfill_action("a", "d", &record("k", false)) {
            BackfillAction::Index(doc) => doc.id,
            BackfillAction::Purge(_) => unreachable!(),
        };
        let purged = match backfill_action("a", "d", &record("k", true)) {
            BackfillAction::Purge(id) => id,
            BackfillAction::Index(_) => unreachable!(),
        };
        assert_eq!(live, purged);
        assert_eq!(live, SearchDoc::dataset_id("a", "d", "k"));
    }

    /// End-to-end over a real scratch index directory: index two live records,
    /// tombstone one, re-run the backfill's per-record classification, and the
    /// ghost is gone from an actual Tantivy query.
    #[tokio::test]
    async fn rerunning_backfill_after_a_tombstone_removes_the_ghost() {
        let dir = std::env::temp_dir().join(format!(
            "pumper-backfill-e2e-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let index = TantivyIndex::new(&SearchConfig {
            enabled: true,
            dir: dir.clone(),
            ..Default::default()
        })
        .unwrap();

        // Backfill #1: both rows live.
        run_backfill(&index, &[record("a", false), record("b", false)]).await;
        assert_eq!(index.doc_count().await.unwrap(), 2);

        // Row "b" is tombstoned; backfill #2 sees it removed.
        run_backfill(&index, &[record("a", false), record("b", true)]).await;

        let hits = index
            .query(SearchRequest::new("grant", 10))
            .await
            .unwrap()
            .hits;
        let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["grants:unified:a"],
            "the tombstoned row's document must not survive the rebuild"
        );

        drop(index);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The main loop's body, minus the storage read: classify, index, purge, flush.
    async fn run_backfill(index: &TantivyIndex, records: &[Record]) {
        let mut docs = Vec::new();
        let mut purge = Vec::new();
        for rec in records {
            match backfill_action("grants", "unified", rec) {
                BackfillAction::Index(doc) => docs.push(doc),
                BackfillAction::Purge(id) => purge.push(id),
            }
        }
        index.index(docs).await.unwrap();
        if !purge.is_empty() {
            index.delete_ids(&purge).await.unwrap();
        }
        index.flush().await.unwrap();
    }
}
