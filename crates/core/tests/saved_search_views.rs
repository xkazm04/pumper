//! M13 "queries as datasets": saved-search materialization. A saved search with
//! a `materialize` target snapshots its result set into a dataset each run —
//! these tests pin the record shape (key = search doc id, source provenance,
//! bucketed score), the delta semantics (changed content churns, score jitter
//! doesn't), removal detection (fell-out-of-results tombstones; an empty result
//! set never wipes the view), and cap honesty.

use pumper_core::testing::TempStore;
use pumper_core::{Datasets, SearchHit, SearchMaterialize};

fn hit(key: &str, title: &str, score: f32) -> SearchHit {
    SearchHit {
        id: format!("grants:opportunities:{key}"),
        app: "grants".into(),
        dataset: "opportunities".into(),
        url: format!("https://example.test/{key}"),
        title: title.into(),
        score,
        snippet: format!("...{title}..."),
    }
}

async fn live_keys(pool: &sqlx::SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT key FROM records WHERE app = 'search' AND dataset = 'view' \
         AND removed_at IS NULL ORDER BY key",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn materialize_round_trip_writes_view_records_keyed_by_doc_id() {
    let store = TempStore::new("mat-roundtrip").await;
    let ds = Datasets::new(store.storage.pool());

    let (summary, removed) = ds
        .materialize_search_hits("search", "view", &[hit("k1", "Alpha grant", 1.2345)], 500)
        .await
        .unwrap();
    assert_eq!(summary.new, vec!["grants:opportunities:k1"]);
    assert!(removed.is_empty());

    let (key, data): (String, String) =
        sqlx::query_as("SELECT key, data FROM records WHERE app = 'search' AND dataset = 'view'")
            .fetch_one(&store.storage.pool())
            .await
            .unwrap();
    assert_eq!(
        key, "grants:opportunities:k1",
        "key must be the search doc id"
    );
    let v: serde_json::Value = serde_json::from_str(&data).unwrap();
    assert_eq!(v["title"], "Alpha grant");
    assert_eq!(v["snippet"], "...Alpha grant...");
    assert_eq!(v["url"], "https://example.test/k1");
    assert_eq!(v["score"], 1.2, "score must be bucketed to one decimal");
    assert_eq!(v["source"]["app"], "grants");
    assert_eq!(v["source"]["dataset"], "opportunities");
    assert_eq!(
        v["source"]["key"], "k1",
        "source key = doc id minus app:dataset:"
    );
}

#[tokio::test]
async fn saved_search_materialize_field_survives_storage_round_trip() {
    let store = TempStore::new("mat-storage").await;
    let created = store
        .storage
        .create_saved_search(
            "ai grants",
            None,
            None,
            "https://example.test/hook",
            None,
            Some(&SearchMaterialize {
                app: "search".into(),
                dataset: "view-ai".into(),
            }),
        )
        .await
        .unwrap();
    let got = store
        .storage
        .get_saved_search(&created.id)
        .await
        .unwrap()
        .expect("created search must be readable");
    let mat = got.materialize.expect("materialize must round-trip");
    assert_eq!(
        (mat.app.as_str(), mat.dataset.as_str()),
        ("search", "view-ai")
    );

    // A plain search stays plain.
    let plain = store
        .storage
        .create_saved_search("other", None, None, "https://example.test/h2", None, None)
        .await
        .unwrap();
    assert!(plain.materialize.is_none());
}

#[tokio::test]
async fn changed_content_emits_deltas_but_score_jitter_does_not() {
    let store = TempStore::new("mat-deltas").await;
    let ds = Datasets::new(store.storage.pool());

    ds.materialize_search_hits(
        "search",
        "view",
        &[hit("k1", "Alpha", 1.23), hit("k2", "Beta", 3.11)],
        500,
    )
    .await
    .unwrap();

    // Run 2: k1's content changed; k2 identical except BM25 jitter within the
    // same 0.1 bucket — the jitter must not manufacture a `changed` revision.
    let (summary, removed) = ds
        .materialize_search_hits(
            "search",
            "view",
            &[hit("k1", "Alpha (updated)", 1.21), hit("k2", "Beta", 3.13)],
            500,
        )
        .await
        .unwrap();
    assert_eq!(summary.changed, vec!["grants:opportunities:k1"]);
    assert_eq!(summary.unchanged, 1, "score jitter alone must be unchanged");
    assert!(summary.new.is_empty());
    assert!(removed.is_empty());

    let changes = ds
        .changes_since("search", Some("view"), None, 100, None)
        .await
        .unwrap();
    let k2_revs = changes
        .iter()
        .filter(|r| r.key == "grants:opportunities:k2")
        .count();
    assert_eq!(k2_revs, 1, "k2 must have only its initial `new` revision");
}

#[tokio::test]
async fn hits_that_fall_out_are_tombstoned_but_empty_results_never_wipe() {
    let store = TempStore::new("mat-removal").await;
    let ds = Datasets::new(store.storage.pool());
    let pool = store.storage.pool();

    ds.materialize_search_hits(
        "search",
        "view",
        &[hit("k1", "Alpha", 1.0), hit("k2", "Beta", 2.0)],
        500,
    )
    .await
    .unwrap();

    // k2 fell out of the results → tombstoned with a `removed` revision.
    let (_, removed) = ds
        .materialize_search_hits("search", "view", &[hit("k1", "Alpha", 1.0)], 500)
        .await
        .unwrap();
    assert_eq!(removed, vec!["grants:opportunities:k2"]);
    assert_eq!(live_keys(&pool).await, vec!["grants:opportunities:k1"]);
    let removed_revs = ds
        .changes_since("search", Some("view"), None, 100, None)
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.change == "removed")
        .count();
    assert_eq!(removed_revs, 1);

    // An empty result set (query gone quiet / wiped index) must not erase the view.
    let (summary, removed) = ds
        .materialize_search_hits("search", "view", &[], 500)
        .await
        .unwrap();
    assert!(removed.is_empty(), "empty snapshot must never tombstone");
    assert!(summary.new.is_empty() && summary.changed.is_empty());
    assert_eq!(live_keys(&pool).await, vec!["grants:opportunities:k1"]);
}

#[tokio::test]
async fn cap_bounds_both_writes_and_removal_detection() {
    let store = TempStore::new("mat-cap").await;
    let ds = Datasets::new(store.storage.pool());
    let pool = store.storage.pool();

    let five: Vec<SearchHit> = (1..=5).map(|i| hit(&format!("a{i}"), "A", 1.0)).collect();
    let (summary, _) = ds
        .materialize_search_hits("search", "view", &five, 3)
        .await
        .unwrap();
    assert_eq!(summary.new.len(), 3, "writes past the cap must be dropped");
    assert_eq!(live_keys(&pool).await.len(), 3);

    // Next run returns a disjoint set: the capped 3 replace the previous 3 —
    // removal detection runs against the capped snapshot, not the raw hits.
    let next: Vec<SearchHit> = (1..=5).map(|i| hit(&format!("b{i}"), "B", 1.0)).collect();
    let (summary, removed) = ds
        .materialize_search_hits("search", "view", &next, 3)
        .await
        .unwrap();
    assert_eq!(summary.new.len(), 3);
    assert_eq!(removed.len(), 3, "all previous view records fell out");
    let live = live_keys(&pool).await;
    assert_eq!(live.len(), 3);
    assert!(live.iter().all(|k| k.contains(":b")));
}
