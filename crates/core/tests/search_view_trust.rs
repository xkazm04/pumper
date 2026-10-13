//! A materialized saved-search view must not launder trust: each view row
//! carries the trust of the source record its hit points at. The search index
//! stores no trust, and `materialize_search_hits` used to write every row with
//! `None` — so a provisional source record surfaced in the view as a stable row
//! that `?trust=stable` would happily serve.

use pumper_core::testing::TempStore;
use pumper_core::SearchHit;
use serde_json::json;

fn hit(key: &str) -> SearchHit {
    SearchHit {
        id: format!("grants:opportunities:{key}"),
        app: "grants".into(),
        dataset: "opportunities".into(),
        url: format!("https://example.test/{key}"),
        title: key.into(),
        score: 1.0,
        snippet: String::new(),
    }
}

async fn view_trust(store: &TempStore, key: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT trust FROM records WHERE app = 'search' AND dataset = 'view' AND key = ?1",
    )
    .bind(format!("grants:opportunities:{key}"))
    .fetch_one(&store.storage.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn a_provisional_hit_stays_provisional_in_the_view_not_laundered_stable() {
    let store = TempStore::new("view-trust").await;
    let ds = store.datasets();
    ds.upsert_trusted(
        "grants",
        "opportunities",
        "p",
        &json!({"t": "p"}),
        Some("provisional"),
    )
    .await
    .unwrap();
    ds.upsert_trusted("grants", "opportunities", "s", &json!({"t": "s"}), None)
        .await
        .unwrap();

    let (summary, _) = ds
        .materialize_search_hits("search", "view", &[hit("p"), hit("s"), hit("gone")], 500)
        .await
        .unwrap();
    assert_eq!(summary.new.len(), 3, "every hit still lands: {summary:?}");

    assert_eq!(
        view_trust(&store, "p").await.as_deref(),
        Some("provisional")
    );
    // Stable stays the NULL that means stable; a hit with no source record has
    // no known trust to carry and writes NULL too.
    assert_eq!(view_trust(&store, "s").await, None);
    assert_eq!(view_trust(&store, "gone").await, None);

    // The revision the view row appended carries it too.
    let rev = ds
        .history("search", "view", "grants:opportunities:p", 1)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(rev.trust, "provisional");

    // When the source recovers, the next materialization re-stamps the row.
    ds.upsert_trusted("grants", "opportunities", "p", &json!({"t": "p"}), None)
        .await
        .unwrap();
    ds.materialize_search_hits("search", "view", &[hit("p"), hit("s")], 500)
        .await
        .unwrap();
    assert_eq!(view_trust(&store, "p").await, None);
}
