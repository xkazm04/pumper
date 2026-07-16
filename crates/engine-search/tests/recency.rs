//! Recency sort + `since` filter over the search index (search finding #3):
//! `sort=newest` orders by the stored `indexed_at`, and `since` is a floor on it.

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest, SearchSort};
use pumper_engine_search::TantivyIndex;

fn unique_dir() -> std::path::PathBuf {
    // Per-process atomic counter so parallel tests never collide on a dir (two
    // TantivyIndex on the same dir would fight over Tantivy's exclusive writer lock).
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("pumper-search-recency-{}-{n}", std::process::id()))
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

    // Three matching docs with out-of-order timestamps. index() defers its commit,
    // so flush to make them queryable immediately.
    index
        .index(vec![doc("a", 100), doc("b", 300), doc("c", 200)])
        .await
        .unwrap();
    index.flush().await.unwrap();

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

#[tokio::test]
async fn index_defers_commit_and_flush_makes_it_visible() {
    // index() no longer commits synchronously (the per-job commit-storm fix), so
    // a doc isn't queryable until a commit — explicit flush or the background tick.
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig { enabled: true, dir: dir.clone() }).unwrap();

    index.index(vec![doc("x", 1)]).await.unwrap();
    assert_eq!(index.doc_count().await.unwrap(), 0, "index() defers its commit");

    index.flush().await.unwrap();
    assert_eq!(index.doc_count().await.unwrap(), 1, "flush commits and makes it visible");

    drop(index);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn background_committer_flushes_without_explicit_flush() {
    // Deferred writes still land: the background committer commits within the
    // interval even with no flush call.
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig { enabled: true, dir: dir.clone() }).unwrap();

    index.index(vec![doc("y", 1)]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(400)).await; // past COMMIT_INTERVAL
    assert_eq!(index.doc_count().await.unwrap(), 1, "background committer made it visible");

    drop(index);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn offset_pages_and_total_is_the_match_count() {
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig { enabled: true, dir: dir.clone() }).unwrap();
    // 5 matching docs, newest-first ids e(500)..a(100).
    index
        .index(vec![doc("a", 100), doc("b", 200), doc("c", 300), doc("d", 400), doc("e", 500)])
        .await
        .unwrap();
    index.flush().await.unwrap();

    let page = |offset, limit| SearchRequest {
        q: "grant".into(),
        limit,
        offset,
        sort: SearchSort::Newest,
        ..Default::default()
    };

    // Page 1 (newest 2): e, d. total is the full match count, not the page size.
    let p1 = index.query(page(0, 2)).await.unwrap();
    assert_eq!(p1.total, 5, "total is the match count, not the returned page size");
    assert_eq!(p1.hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["e", "d"]);

    // Page 2 (offset 2): c, b — distinct from page 1.
    let p2 = index.query(page(2, 2)).await.unwrap();
    assert_eq!(p2.total, 5);
    assert_eq!(p2.hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["c", "b"]);

    // Page 3 (offset 4): just a.
    let p3 = index.query(page(4, 2)).await.unwrap();
    assert_eq!(p3.hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["a"]);

    drop(index);
    std::fs::remove_dir_all(&dir).ok();
}
