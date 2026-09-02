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
//!
//! `--re-enrich` (N11) is the same walk run for a different reason: after
//! installing or reordering `[search] enrichers`, the stored entity maps of
//! documents already in the index are stale. Re-indexing upserts them with the
//! CURRENT enrichers and no schema rebuild — the `entities` field is one JSON
//! object, so a new entity kind never moves the schema and never wipes the
//! corpus. It refuses to run against an EMPTY index, because re-enriching
//! nothing is a plain backfill the operator did not ask for.

use std::sync::Arc;

use pumper_core::{
    backfill_cursor, parse_backfill_cursor, Config, Datasets, NoPlugins, Plugins, Search,
    SearchDoc, Storage,
};
use pumper_engine_search::TantivyIndex;
use pumper_engine_wasm::WasmPluginHost;

/// Records indexed per commit — matches the batch shape of the live path, and
/// doubles as the keyset page size so nothing is read that isn't about to be
/// written.
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
    // The same plugin host the server runs enrichers on, so an offline
    // re-enrichment produces the same entities a live index() would. Without it
    // a `plugin:` enricher would fail open on every document here and quietly
    // strip the very fields the run exists to refresh.
    let plugins: Arc<dyn Plugins> = if config.plugins.enabled {
        Arc::new(WasmPluginHost::new(&config.plugins)?)
    } else {
        Arc::new(NoPlugins)
    };
    let search = TantivyIndex::with_plugins(&config.search, Some(plugins))?;

    let invocation = parse_invocation(&std::env::args().collect::<Vec<_>>())?;
    let scope = invocation.scope;
    if invocation.re_enrich {
        // Checked BEFORE the walk: an empty index means the operator asked to
        // refresh entities on a corpus that is not there, and silently doing a
        // full first-time build under that flag would report a re-enrichment
        // that never happened.
        re_enrich_precondition(search.doc_count().await?).map_err(anyhow::Error::msg)?;
    }
    let targets = resolve_targets(&datasets, &scope).await?;
    tracing::info!(
        datasets = targets.len(),
        re_enrich = invocation.re_enrich,
        "backfilling search index"
    );

    let mut total = DatasetReport::default();
    for (app, dataset) in targets {
        let report = backfill_dataset(&datasets, &search, &app, &dataset).await?;
        tracing::info!(
            %app,
            %dataset,
            indexed = report.indexed,
            purged = report.purged,
            "backfilled dataset"
        );
        total.indexed += report.indexed;
        total.purged += report.purged;
    }

    // index() defers its commit to a background committer, but this process exits
    // right after — flush so the tail is durable and doc_count is accurate.
    search.flush().await?;
    let doc_count = search.doc_count().await?;
    let what = if invocation.re_enrich {
        "search re-enrichment complete"
    } else {
        "search backfill complete"
    };
    println!(
        "{what}: {} record(s) indexed, {} tombstoned \
         record(s) purged; index now holds {doc_count} document(s)",
        total.indexed, total.purged
    );
    // Per-enricher counters, so a run that produced no entities says WHY: a
    // pass with failures > 0 could not run (a trapping plugin, a module nobody
    // installed), which otherwise looks exactly like one that found nothing.
    for stat in search.enricher_stats() {
        println!(
            "  enricher {}: {} doc(s), {} entities, {} failure(s)",
            stat.name, stat.docs, stat.entities, stat.failures
        );
    }
    Ok(())
}

/// What one dataset's rebuild did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DatasetReport {
    indexed: u64,
    purged: u64,
}

/// Rebuilds one dataset's documents from stored records.
///
/// Pages the read with the repo's keyset pager rather than reading the dataset
/// into one `Vec`. The previous `list(app, dataset, 1_000_000)` was
/// `ORDER BY updated_at DESC LIMIT 1000000`, so past a million rows the OLDEST
/// records were silently dropped from the "full" rebuild and the summary
/// reported the truncated count as success. A rebuild whose entire purpose is
/// completeness cannot have a ceiling, so the ceiling is gone rather than merely
/// reported: `list_page` is stable under concurrent writes (keyset, not OFFSET),
/// and indexing/purging per page keeps memory flat at one page regardless of
/// dataset size.
async fn backfill_dataset(
    datasets: &Datasets,
    search: &impl Search,
    app: &str,
    dataset: &str,
) -> anyhow::Result<DatasetReport> {
    let mut report = DatasetReport::default();
    let mut after: Option<(String, String)> = None;
    loop {
        // `list_page` includes tombstoned rows — required here, since a purge is
        // the whole point of re-walking a dataset.
        let page = datasets
            .list_page(app, dataset, after.clone(), INDEX_CHUNK as i64, None)
            .await?;
        let n = page.len();
        let mut docs: Vec<SearchDoc> = Vec::with_capacity(n);
        let mut tombstoned: Vec<String> = Vec::new();
        for rec in &page {
            match backfill_action(app, dataset, rec) {
                BackfillAction::Index(doc) => docs.push(doc),
                BackfillAction::Purge(id) => tombstoned.push(id),
            }
        }
        report.indexed += docs.len() as u64;
        if !docs.is_empty() {
            search.index(docs).await?;
        }
        // A tombstoned row is not merely "not indexed": its document may ALREADY
        // be in the index (indexed while live, then removed during a window the
        // live delete path missed — or the delete landed in an index that was
        // later wiped). Skipping it left that ghost queryable forever, which is
        // exactly the state a backfill is run to repair.
        report.purged += tombstoned.len() as u64;
        if !tombstoned.is_empty() {
            search.delete_ids(&tombstoned).await?;
        }
        // A short page means the dataset is exhausted.
        if n < INDEX_CHUNK {
            return Ok(report);
        }
        after = page
            .last()
            .map(backfill_cursor)
            .as_deref()
            .and_then(parse_backfill_cursor);
        if after.is_none() {
            return Ok(report);
        }
    }
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

/// The CLI scope. A scope is required so a full-index rebuild is never
/// accidental.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Scope {
    /// `--app X --dataset Y`
    Dataset { app: String, dataset: String },
    /// `--app X` — all of an app's datasets.
    App(String),
    /// `--all` — every dataset in the store.
    All,
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::Dataset { app, dataset } => write!(f, "--app {app} --dataset {dataset}"),
            Scope::App(app) => write!(f, "--app {app}"),
            Scope::All => write!(f, "--all"),
        }
    }
}

/// One run of the tool: what to walk, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Invocation {
    scope: Scope,
    /// `--re-enrich` (N11): the walk exists to refresh entity maps with the
    /// current `[search] enrichers`, not to repopulate a lost index.
    re_enrich: bool,
}

/// Parses argv into a run. Pure — no store access — so the flag grammar is
/// testable without a database.
///
/// `--re-enrich` is a MODIFIER, not a scope: it never implies one, because
/// "refresh the entities" and "refresh WHICH dataset's entities" are different
/// questions and defaulting the second to everything is exactly the accidental
/// full rebuild `parse_scope` exists to prevent.
fn parse_invocation(args: &[String]) -> anyhow::Result<Invocation> {
    Ok(Invocation {
        scope: parse_scope(args)?,
        re_enrich: args.iter().any(|a| a == "--re-enrich"),
    })
}

/// Whether a `--re-enrich` run has anything to re-enrich.
///
/// The anti-pattern: treating `--re-enrich` on an empty index as a plain
/// backfill. It succeeds, prints a completion line, and leaves the operator
/// believing a re-enrichment ran over documents that were in fact indexed for
/// the first time — so a broken enricher list reads as a working one.
fn re_enrich_precondition(doc_count: u64) -> std::result::Result<(), String> {
    if doc_count == 0 {
        return Err(
            "--re-enrich needs an index with documents in it, and this one holds 0. \
             Run the same scope WITHOUT --re-enrich to build it first (a first-time \
             build already enriches with the configured [search] enrichers)"
                .to_string(),
        );
    }
    Ok(())
}

/// Parses the scope out of argv. Pure — no store access — so the flag grammar is
/// testable without a database.
fn parse_scope(args: &[String]) -> anyhow::Result<Scope> {
    let has = |name: &str| args.iter().any(|a| a == name);
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    match (flag("--app"), flag("--dataset"), has("--all")) {
        (Some(app), Some(dataset), _) => Ok(Scope::Dataset { app, dataset }),
        (Some(app), None, _) => Ok(Scope::App(app)),
        (None, Some(_), _) => anyhow::bail!("--dataset requires --app"),
        (None, None, true) => Ok(Scope::All),
        (None, None, false) => {
            anyhow::bail!("specify a scope: --all, --app <app>, or --app <app> --dataset <dataset>")
        }
    }
}

/// A scope that resolved to nothing. Its own function because the honesty rule
/// is the point: an operator who typos `--dataset unifed` used to get the same
/// cheerful `search backfill complete: 0 record(s) indexed` line and exit 0 as a
/// real rebuild, so a silent no-op was indistinguishable from success.
fn empty_scope_error(scope: &Scope) -> anyhow::Error {
    match scope {
        Scope::All => anyhow::anyhow!("no datasets to backfill: the store holds no records"),
        _ => anyhow::anyhow!(
            "no datasets matched `{scope}`; nothing was indexed. Check the spelling — \
             `GET /datasets/{{app}}` lists an app's datasets"
        ),
    }
}

/// `(app, dataset)` targets for a scope. Errors when the scope matches nothing,
/// on every path — including `--app X --dataset Y`, which used to be taken on
/// faith and never validated at all.
///
/// Existence is judged over ALL records, tombstoned included ([`record_count`],
/// [`Datasets::datasets`], [`Datasets::list_all_datasets_including_removed`]).
/// A dataset whose every record is tombstoned is not a typo — it is the exact
/// state a purge exists to repair, so it must resolve as a target.
///
/// [`record_count`]: Datasets::record_count
async fn resolve_targets(
    datasets: &Datasets,
    scope: &Scope,
) -> anyhow::Result<Vec<(String, String)>> {
    let targets = match scope {
        Scope::Dataset { app, dataset } => {
            if datasets.record_count(app, dataset).await? == 0 {
                Vec::new()
            } else {
                vec![(app.clone(), dataset.clone())]
            }
        }
        Scope::App(app) => datasets
            .datasets(app)
            .await?
            .into_iter()
            .map(|d| (app.clone(), d))
            .collect(),
        Scope::All => datasets.list_all_datasets_including_removed().await?,
    };
    if targets.is_empty() {
        return Err(empty_scope_error(scope));
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        backfill_action, backfill_dataset, parse_invocation, parse_scope, re_enrich_precondition,
        resolve_targets, BackfillAction, DatasetReport, Invocation, Scope, INDEX_CHUNK,
    };
    use chrono::{TimeZone, Utc};
    use pumper_core::config::SearchConfig;
    use pumper_core::testing::TempStore;
    use pumper_core::{Datasets, Record, Search, SearchDoc, SearchRequest};
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

    // ── scope parsing (pure) ────────────────────────────────────────────────

    fn argv(rest: &[&str]) -> Vec<String> {
        std::iter::once("search-backfill".to_string())
            .chain(rest.iter().map(|s| s.to_string()))
            .collect()
    }

    #[test]
    fn a_scope_is_parsed_without_touching_the_store() {
        assert_eq!(
            parse_scope(&argv(&["--app", "grants", "--dataset", "unified"])).unwrap(),
            Scope::Dataset {
                app: "grants".into(),
                dataset: "unified".into()
            }
        );
        assert_eq!(
            parse_scope(&argv(&["--app", "grants"])).unwrap(),
            Scope::App("grants".into())
        );
        assert_eq!(parse_scope(&argv(&["--all"])).unwrap(), Scope::All);
        assert!(parse_scope(&argv(&["--dataset", "unified"])).is_err());
        assert!(parse_scope(&argv(&[])).is_err());
    }

    /// `--re-enrich` modifies a scope; it never SUPPLIES one. The anti-pattern:
    /// letting the flag stand in for `--all`, which turns "refresh the entities
    /// I just reconfigured" into an unrequested rebuild of every dataset in the
    /// store.
    #[test]
    fn re_enrich_is_a_modifier_and_never_supplies_a_scope() {
        assert_eq!(
            parse_invocation(&argv(&["--all", "--re-enrich"])).unwrap(),
            Invocation {
                scope: Scope::All,
                re_enrich: true
            }
        );
        assert_eq!(
            parse_invocation(&argv(&["--app", "grants"])).unwrap(),
            Invocation {
                scope: Scope::App("grants".into()),
                re_enrich: false
            }
        );
        assert!(
            parse_invocation(&argv(&["--re-enrich"])).is_err(),
            "the flag alone names no scope"
        );
    }

    /// Re-enriching an empty index is not a re-enrichment. Reporting it as one
    /// tells an operator their new enricher ran over the corpus when in fact
    /// the corpus was built for the first time by that very command.
    #[test]
    fn re_enrich_refuses_an_empty_index_instead_of_reporting_a_first_build_as_one() {
        let err = re_enrich_precondition(0).expect_err("nothing to re-enrich");
        assert!(err.contains("holds 0"), "{err}");
        assert!(err.contains("WITHOUT --re-enrich"), "{err}");
        assert_eq!(re_enrich_precondition(1), Ok(()));
    }

    // ── target resolution (the function both honesty defects lived in) ──────

    /// The anti-pattern: `--all` resolving through `list_all_datasets()`, whose
    /// SQL is `... WHERE removed_at IS NULL`. A dataset whose every record is
    /// tombstoned then never appears as a target — and that is exactly the
    /// dataset whose ghost documents the full rebuild exists to purge. The tool
    /// printed `0 tombstoned record(s) purged` and exited 0, forever.
    #[tokio::test]
    async fn a_fully_tombstoned_dataset_is_not_invisible_to_all() {
        let store = TempStore::new("backfill-all-tombstoned").await;
        let datasets = Datasets::new(store.storage.pool());
        seed(&datasets, "retired", "old", &["a", "b"]).await;
        tombstone_all(&datasets, "retired", "old").await;
        // A second, still-live dataset so this is not just "the only row".
        seed(&datasets, "grants", "unified", &["x"]).await;

        let targets = resolve_targets(&datasets, &Scope::All).await.unwrap();
        assert!(
            targets.contains(&("retired".to_string(), "old".to_string())),
            "--all must reach a fully tombstoned dataset, got {targets:?}"
        );
        assert!(targets.contains(&("grants".to_string(), "unified".to_string())));

        // ...while the live-only view stays as its other callers (the watch
        // registry, the DataHub poll) rely on it.
        let live = datasets.list_all_datasets().await.unwrap();
        assert!(
            !live.contains(&("retired".to_string(), "old".to_string())),
            "list_all_datasets must keep its live-only contract"
        );
    }

    /// The anti-pattern: `--app grants --dataset unifed` returning
    /// `vec![(app, dataset)]` unvalidated, reading zero rows, and printing the
    /// same cheerful completion line with exit 0 as a real rebuild.
    #[tokio::test]
    async fn a_typod_scope_is_not_reported_as_success() {
        let store = TempStore::new("backfill-typo").await;
        let datasets = Datasets::new(store.storage.pool());
        seed(&datasets, "grants", "unified", &["x"]).await;

        let err = resolve_targets(
            &datasets,
            &Scope::Dataset {
                app: "grants".into(),
                dataset: "unifed".into(),
            },
        )
        .await
        .expect_err("a dataset that does not exist must not resolve as a target");
        assert!(
            err.to_string().contains("unifed"),
            "the error must name the scope that matched nothing: {err}"
        );

        // The correctly spelled scope still resolves.
        assert_eq!(
            resolve_targets(
                &datasets,
                &Scope::Dataset {
                    app: "grants".into(),
                    dataset: "unified".into()
                }
            )
            .await
            .unwrap(),
            vec![("grants".to_string(), "unified".to_string())]
        );
    }

    /// A fully tombstoned dataset is a legitimate scope, not a typo — naming it
    /// explicitly is how an operator purges one ghost dataset without a
    /// whole-store rebuild.
    #[tokio::test]
    async fn a_fully_tombstoned_dataset_is_not_mistaken_for_a_typo() {
        let store = TempStore::new("backfill-tombstoned-scope").await;
        let datasets = Datasets::new(store.storage.pool());
        seed(&datasets, "retired", "old", &["a"]).await;
        tombstone_all(&datasets, "retired", "old").await;

        assert_eq!(
            resolve_targets(
                &datasets,
                &Scope::Dataset {
                    app: "retired".into(),
                    dataset: "old".into()
                }
            )
            .await
            .unwrap(),
            vec![("retired".to_string(), "old".to_string())]
        );
        assert!(resolve_targets(&datasets, &Scope::App("retired".into()))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn an_empty_scope_is_not_reported_as_success() {
        let store = TempStore::new("backfill-empty").await;
        let datasets = Datasets::new(store.storage.pool());

        // Every path must fail, not print "no datasets to backfill" and exit 0.
        assert!(resolve_targets(&datasets, &Scope::All).await.is_err());
        assert!(resolve_targets(&datasets, &Scope::App("ghost".into()))
            .await
            .is_err());
        assert!(resolve_targets(
            &datasets,
            &Scope::Dataset {
                app: "ghost".into(),
                dataset: "d".into()
            }
        )
        .await
        .is_err());
    }

    // ── the real loop, end to end over a real store and a real index ────────

    /// End-to-end over a real temp SQLite AND a real scratch Tantivy dir,
    /// driving `backfill_dataset` — the function `main` calls. The previous
    /// version of this test reimplemented the loop body against hand-built
    /// `Record`s, so it could not see a target-resolution or a read bug at all.
    #[tokio::test]
    async fn rerunning_backfill_after_a_tombstone_removes_the_ghost() {
        let store = TempStore::new("backfill-ghost").await;
        let datasets = Datasets::new(store.storage.pool());
        let index = scratch_index("ghost");
        seed(&datasets, "grants", "unified", &["a", "b"]).await;

        // Backfill #1: both rows live.
        let first = backfill_dataset(&datasets, &index.index, "grants", "unified")
            .await
            .unwrap();
        index.index.flush().await.unwrap();
        assert_eq!(
            first,
            DatasetReport {
                indexed: 2,
                purged: 0
            }
        );
        assert_eq!(index.index.doc_count().await.unwrap(), 2);

        // Row "b" is tombstoned; backfill #2 must purge its document.
        datasets
            .tombstone_keys("grants", "unified", &["b".to_string()])
            .await
            .unwrap();
        let second = backfill_dataset(&datasets, &index.index, "grants", "unified")
            .await
            .unwrap();
        index.index.flush().await.unwrap();
        assert_eq!(
            second,
            DatasetReport {
                indexed: 1,
                purged: 1
            }
        );

        let hits = index
            .index
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
    }

    /// The anti-pattern: `list(app, dataset, 1_000_000)` —
    /// `ORDER BY updated_at DESC LIMIT ?3`, so everything past the ceiling is
    /// silently dropped from a "full" rebuild and the truncated count is
    /// reported as success. A ceiling cannot be tested at a million rows, but
    /// the mechanism that removed it can: the read now pages, so nothing is lost
    /// at a page boundary either.
    #[tokio::test]
    async fn a_dataset_larger_than_one_page_is_not_half_indexed() {
        let store = TempStore::new("backfill-paging").await;
        let datasets = Datasets::new(store.storage.pool());
        let index = scratch_index("paging");

        // Two full pages plus a remainder, so a single-page read would truncate.
        let n = INDEX_CHUNK * 2 + 7;
        let items: Vec<(String, serde_json::Value)> = (0..n)
            .map(|i| {
                (
                    format!("k{i:05}"),
                    serde_json::json!({"title": format!("Grant {i}"), "url": format!("https://x/{i}")}),
                )
            })
            .collect();
        datasets
            .upsert_many("grants", "unified", &items)
            .await
            .unwrap();

        let report = backfill_dataset(&datasets, &index.index, "grants", "unified")
            .await
            .unwrap();
        index.index.flush().await.unwrap();
        assert_eq!(
            report.indexed, n as u64,
            "every record must be indexed, not just the first page"
        );
        assert_eq!(index.index.doc_count().await.unwrap(), n as u64);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    async fn seed(datasets: &Datasets, app: &str, dataset: &str, keys: &[&str]) {
        let items: Vec<(String, serde_json::Value)> = keys
            .iter()
            .map(|k| {
                (
                    (*k).to_string(),
                    serde_json::json!({"title": format!("Grant {k}"), "url": format!("https://x/{k}")}),
                )
            })
            .collect();
        datasets.upsert_many(app, dataset, &items).await.unwrap();
    }

    /// Tombstones every record in a dataset through the NAMED removal seam.
    /// `detect_removed` is off-limits outside `AppContext::sync_many`, and the
    /// inventory test in `crates/core/tests/removal_guard.rs` scans every
    /// `src/` file for it — a bin's `#[cfg(test)]` module counts.
    async fn tombstone_all(datasets: &Datasets, app: &str, dataset: &str) {
        let keys: Vec<String> = datasets
            .list(app, dataset, 10_000)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.key)
            .collect();
        datasets.tombstone_keys(app, dataset, &keys).await.unwrap();
        let remaining = datasets
            .list(app, dataset, 10_000)
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.removed_at.is_none())
            .count();
        assert_eq!(
            remaining, 0,
            "helper must leave the dataset fully tombstoned"
        );
    }

    /// An enricher standing in for an installed `.wasm`, so the re-enrichment
    /// path is provable on a machine with no wasm toolchain.
    struct StubEnricher;

    #[async_trait::async_trait]
    impl pumper_core::Enricher for StubEnricher {
        fn name(&self) -> &str {
            "plugin:stub"
        }
        async fn enrich(&self, _input: &pumper_core::EnrichInput) -> Vec<pumper_core::Entity> {
            vec![pumper_core::Entity::new("currency", "usd")]
        }
    }

    /// Reopens the index dir once the previous handle's background committer has
    /// released Tantivy's writer lock. Bounded and clock-free: the committer only
    /// needs the runtime to poll it, so yielding is enough — no sleep, no
    /// timing assumption.
    async fn reopen(
        dir: &std::path::Path,
        enrichers: Vec<Arc<dyn pumper_core::Enricher>>,
    ) -> TantivyIndex {
        let cfg = SearchConfig {
            enabled: true,
            dir: dir.to_path_buf(),
            ..Default::default()
        };
        for _ in 0..200 {
            match TantivyIndex::with_enrichers(&cfg, enrichers.clone()) {
                Ok(index) => return index,
                Err(_) => tokio::task::yield_now().await,
            }
        }
        panic!("the previous index handle never released its writer lock");
    }

    /// `--re-enrich` over a fixture index: a second walk with a NEW enricher
    /// installed refreshes the stored entity maps in place — same documents,
    /// same doc ids, no schema rebuild and no wipe.
    ///
    /// The anti-pattern this pins: needing a destructive rebuild to pick up a new
    /// entity kind. Before N11 every entity field was a schema field, so "add an
    /// enricher" meant wiping the corpus and rebuilding it from the store, with
    /// the index answering queries as an empty-but-healthy index in between.
    #[tokio::test]
    async fn re_enrich_refreshes_entities_in_place_without_wiping_the_index() {
        let store = TempStore::new("backfill-re-enrich").await;
        let datasets = Datasets::new(store.storage.pool());
        datasets
            .upsert_many(
                "grants",
                "unified",
                &[(
                    "a".to_string(),
                    serde_json::json!({
                        "title": "Award notice",
                        "body": "award up to $250,000",
                        "url": "https://example.test/a"
                    }),
                )],
            )
            .await
            .unwrap();

        // A scratch dir this test owns across TWO index handles, so the
        // re-enrichment runs against the SAME on-disk index the first walk built.
        let dir = std::env::temp_dir().join(format!(
            "pumper-backfill-re-enrich-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        // First build: the shipped rules only.
        let first = reopen(
            &dir,
            vec![Arc::new(pumper_engine_search::enrichers::BuiltinEnricher)],
        )
        .await;
        let report = backfill_dataset(&datasets, &first, "grants", "unified")
            .await
            .unwrap();
        assert_eq!(report.indexed, 1);
        first.flush().await.unwrap();
        let before = first
            .stored_entities("grants:unified:a")
            .await
            .unwrap()
            .expect("indexed");
        assert_eq!(before["amount"], serde_json::json!(250_000));
        assert!(before.get("currency").is_none(), "{before}");
        drop(first);

        // Second walk with an enricher "installed": the re-enrichment.
        let index = reopen(
            &dir,
            vec![
                Arc::new(pumper_engine_search::enrichers::BuiltinEnricher),
                Arc::new(StubEnricher),
            ],
        )
        .await;
        assert_eq!(
            index.doc_count().await.unwrap(),
            1,
            "reopening with a new enricher must not wipe the index"
        );
        let report = backfill_dataset(&datasets, &index, "grants", "unified")
            .await
            .unwrap();
        assert_eq!(report.indexed, 1);
        index.flush().await.unwrap();

        let after = index
            .stored_entities("grants:unified:a")
            .await
            .unwrap()
            .expect("still indexed");
        assert_eq!(after["amount"], serde_json::json!(250_000), "kept");
        assert_eq!(after["currency"], serde_json::json!("usd"), "added");
        assert_eq!(
            index.doc_count().await.unwrap(),
            1,
            "an upsert, not a duplicate"
        );
        let stats = index.enricher_stats();
        assert_eq!(stats.len(), 2);
        assert!(stats.iter().all(|s| s.failures == 0), "{stats:?}");

        drop(index);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// RAII scratch Tantivy index — the dir is removed when the test ends.
    struct ScratchIndex {
        index: TantivyIndex,
        dir: std::path::PathBuf,
    }

    impl Drop for ScratchIndex {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    fn scratch_index(tag: &str) -> ScratchIndex {
        let dir = std::env::temp_dir().join(format!(
            "pumper-backfill-{tag}-{}-{}",
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
        ScratchIndex { index, dir }
    }
}
