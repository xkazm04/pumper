//! Recency sort + `since` filter over the search index (search finding #3):
//! `sort=newest` orders by the stored `indexed_at`, and `since` is a floor on it.

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest, SearchSort};
use pumper_engine_search::TantivyIndex;

fn unique_dir() -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("pumper-search-recency-{}-{nanos}", std::process::id()))
}

fn doc(id: &str, indexed_at: i64) -> SearchDoc {
    SearchDoc {
        id: id.to_string(),
        app: "grants".into(),
        dataset: "unified".into(),
        url: String::new(),
        title: format!("Grant {id}"),
        body: format!("a rural health grant opportunity {id}"),
        indexed_at,
    }
}

#[tokio::test]
async fn newest_sort_and_since_filter_use_indexed_at() {
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig { enabled: true, dir: dir.clone() }).unwrap();

    // Three matching docs with out-of-order timestamps.
    index
        .index(vec![doc("a", 100), doc("b", 300), doc("c", 200)])
        .await
        .unwrap();

    let query = |sort, since| SearchRequest {
        q: "grant".into(),
        limit: 10,
        sort,
        since,
        ..Default::default()
    };

    // Relevance sort returns all three (order is BM25, not asserted).
    let by_score = index.query(query(SearchSort::Score, None)).await.unwrap();
    assert_eq!(by_score.hits.len(), 3);

    // Newest sort orders by indexed_at descending: b(300), c(200), a(100).
    let by_newest = index.query(query(SearchSort::Newest, None)).await.unwrap();
    let ids: Vec<&str> = by_newest.hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["b", "c", "a"], "newest first by indexed_at");

    // `since` is an inclusive floor: since=200 keeps c(200) and b(300), drops a(100).
    let recent = index.query(query(SearchSort::Newest, Some(200))).await.unwrap();
    let recent_ids: Vec<&str> = recent.hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(recent_ids, vec!["b", "c"], "since floor excludes older docs");

    // `since` past every doc yields nothing.
    let none = index.query(query(SearchSort::Score, Some(1000))).await.unwrap();
    assert!(none.hits.is_empty());

    drop(index);
    std::fs::remove_dir_all(&dir).ok();
}
