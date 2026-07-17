//! Facet opt-out (search finding #1): facets are computed only when
//! `SearchRequest.facets` is set, so the saved-search runner and default UI page
//! don't pay for facet-sampling they never read.

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest};
use pumper_engine_search::TantivyIndex;

fn unique_dir() -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("pumper-search-facets-{}-{n}", std::process::id()))
}

fn doc(id: &str, app: &str, dataset: &str) -> SearchDoc {
    SearchDoc {
        id: id.to_string(),
        app: app.into(),
        dataset: dataset.into(),
        url: String::new(),
        title: format!("Grant {id}"),
        body: "a rural health grant opportunity".into(),
        indexed_at: 1,
    }
}

#[tokio::test]
async fn facets_are_computed_only_when_requested() {
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig { enabled: true, dir: dir.clone() }).unwrap();
    index
        .index(vec![
            doc("a", "grants", "unified"),
            doc("b", "grants", "unified"),
            doc("c", "census", "market_blend"),
        ])
        .await
        .unwrap();
    index.flush().await.unwrap();

    // Default (facets off): hits + total are correct, but no facet breakdown is
    // computed — the saved-search runner's shape.
    let off = index.query(SearchRequest::new("grant", 10)).await.unwrap();
    assert_eq!(off.total, 3, "total is the real match count regardless of facets");
    assert_eq!(off.hits.len(), 3);
    assert!(off.facets.apps.is_empty(), "facets are off by default");
    assert!(off.facets.datasets.is_empty());

    // Opt in (the /search route): facets are populated over the matching set.
    let on = index
        .query(SearchRequest { facets: true, ..SearchRequest::new("grant", 10) })
        .await
        .unwrap();
    assert_eq!(on.total, 3);
    let apps: std::collections::BTreeMap<&str, u64> =
        on.facets.apps.iter().map(|f| (f.value.as_str(), f.count)).collect();
    assert_eq!(apps.get("grants"), Some(&2));
    assert_eq!(apps.get("census"), Some(&1));
    // Hit fields still read correctly with the direct get_first path (no to_json).
    assert!(on.hits.iter().all(|h| h.title.starts_with("Grant ")));

    let _ = std::fs::remove_dir_all(&dir);
}
