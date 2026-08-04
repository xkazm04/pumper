//! Ghost-doc GC: the worker sweeps an app's previous identity-less job-result
//! snapshot (`dataset = "_job"`) before indexing the current run's, so the index
//! holds one snapshot per app instead of one per run forever. This exercises the
//! index-side primitive that sweep is built on, including the sequencing the
//! worker uses (delete THEN add) and the blast radius (one app, one reserved
//! dataset — durable url-keyed docs and other apps are untouched).

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest};
use pumper_engine_search::TantivyIndex;

/// Mirrors the worker's reserved dataset names (`crates/server/src/worker.rs`).
const JOB: &str = "_job";
const RECORDS: &str = "_records";

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("pumper-search-gc-{tag}-{}-{n}", std::process::id()))
}

fn doc(id: &str, app: &str, dataset: &str) -> SearchDoc {
    SearchDoc {
        id: id.to_string(),
        app: app.into(),
        dataset: dataset.into(),
        url: String::new(),
        title: format!("Result {id}"),
        body: "a rural health grant opportunity".into(),
        indexed_at: 1,
    }
}

async fn open(tag: &str) -> (TantivyIndex, std::path::PathBuf) {
    let dir = unique_dir(tag);
    let index = TantivyIndex::new(&SearchConfig {
        enabled: true,
        dir: dir.clone(),
        ..Default::default()
    })
    .unwrap();
    (index, dir)
}

async fn ids_for(index: &TantivyIndex, q: &str) -> Vec<String> {
    let mut ids: Vec<String> = index
        .query(SearchRequest::new(q, 50))
        .await
        .unwrap()
        .hits
        .into_iter()
        .map(|h| h.id)
        .collect();
    ids.sort();
    ids
}

/// The anti-pattern: every run mints `<app>:<job_id>` docs that nothing ever
/// deletes, so the index grows monotonically with the number of runs.
#[tokio::test]
async fn prior_run_snapshot_is_swept_not_accumulated() {
    let (index, _dir) = open("sweep").await;

    // Run 1: two identity-less docs + one durable url-keyed doc.
    index
        .index(vec![
            doc("hn:run1", "hn", JOB),
            doc("hn:run1:0", "hn", JOB),
            doc("hn:https://x/1", "hn", RECORDS),
        ])
        .await
        .unwrap();
    index.flush().await.unwrap();
    assert_eq!(index.doc_count().await.unwrap(), 3);

    // Run 2, in the worker's order: sweep the app's prior snapshot, then add.
    index.delete_dataset("hn", JOB).await.unwrap();
    index
        .index(vec![doc("hn:run2", "hn", JOB), doc("hn:run2:0", "hn", JOB)])
        .await
        .unwrap();
    index.flush().await.unwrap();

    assert_eq!(
        ids_for(&index, "grant").await,
        vec![
            "hn:https://x/1".to_string(),
            "hn:run2".to_string(),
            "hn:run2:0".to_string()
        ],
        "run 1's ghosts are gone; run 2's docs survive their own sweep and the \
         url-keyed doc is untouched"
    );
    assert_eq!(
        index.doc_count().await.unwrap(),
        3,
        "doc count is bounded by one snapshot per app, not runs x docs"
    );
}

/// The anti-pattern: a sweep that reaches past its own app, or past the reserved
/// snapshot dataset into the durable corpus.
#[tokio::test]
async fn sweep_is_scoped_to_one_app_not_every_job_doc() {
    let (index, _dir) = open("scope").await;
    index
        .index(vec![
            doc("hn:run1", "hn", JOB),
            doc("other:run1", "other", JOB),
            doc("hn:https://x/1", "hn", RECORDS),
        ])
        .await
        .unwrap();
    index.flush().await.unwrap();

    index.delete_dataset("hn", JOB).await.unwrap();

    assert_eq!(
        ids_for(&index, "grant").await,
        vec!["hn:https://x/1".to_string(), "other:run1".to_string()],
        "another app's snapshot and this app's durable records are out of scope"
    );
}

/// The anti-pattern: stamping the app name as the dataset, so `/search` facets
/// advertised a dataset that does not exist in the store.
#[tokio::test]
async fn facets_report_reserved_namespaces_not_a_phantom_app_dataset() {
    let (index, _dir) = open("facets").await;
    index
        .index(vec![
            doc("hn:run1", "hn", JOB),
            doc("hn:https://x/1", "hn", RECORDS),
        ])
        .await
        .unwrap();
    index.flush().await.unwrap();

    let res = index
        .query(SearchRequest {
            facets: true,
            ..SearchRequest::new("grant", 10)
        })
        .await
        .unwrap();
    let datasets: Vec<&str> = res
        .facets
        .datasets
        .iter()
        .map(|f| f.value.as_str())
        .collect();
    assert!(
        !datasets.contains(&"hn"),
        "the app name must not appear as a dataset facet: {datasets:?}"
    );
    let mut sorted = datasets.clone();
    sorted.sort();
    assert_eq!(sorted, vec![JOB, RECORDS]);
    assert_eq!(res.facets.apps.len(), 1, "app facet is still the app");
    assert_eq!(res.facets.apps[0].value, "hn");
}

/// The anti-pattern: `doc_count` as the only telemetry — flat while the index's
/// bytes and segments climb.
#[tokio::test]
async fn index_stats_report_disk_and_segments_not_only_doc_count() {
    let (index, _dir) = open("stats").await;
    let empty = index.index_stats().await.unwrap();
    assert_eq!(empty.segment_count, 0, "a fresh index has no segments");

    index
        .index(
            (0..20)
                .map(|i| doc(&format!("hn:{i}"), "hn", JOB))
                .collect(),
        )
        .await
        .unwrap();
    index.flush().await.unwrap();

    let stats = index.index_stats().await.unwrap();
    assert!(stats.segment_count >= 1, "committed docs live in a segment");
    assert!(
        stats.disk_bytes > empty.disk_bytes,
        "on-disk bytes grow with indexed content ({} -> {})",
        empty.disk_bytes,
        stats.disk_bytes
    );
}
