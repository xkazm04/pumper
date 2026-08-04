//! Entity-typed index fields (M14): index-time extraction of money amounts and
//! deadline-like dates into `amount`/`event_date` fast fields, filtered via
//! `amount_gte/lte` and `date_after/before`. Docs where extraction found
//! nothing have the field ABSENT and never match an entity filter.

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest};
use pumper_engine_search::TantivyIndex;

fn unique_dir() -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("pumper-search-entity-{}-{n}", std::process::id()))
}

// 2026-01-01T00:00:00Z — every doc's indexed_at, the "now" deadlines are
// judged against.
const NOW: i64 = 1_767_225_600;
// 2026-09-01 / 2026-12-01 UTC midnights.
const SEP_1: i64 = 1_788_220_800;
const DEC_1: i64 = 1_796_083_200;

fn doc(id: &str, body: &str) -> SearchDoc {
    SearchDoc {
        id: id.to_string(),
        app: "grants".into(),
        dataset: "unified".into(),
        url: String::new(),
        title: format!("Grant {id}"),
        body: body.to_string(),
        indexed_at: NOW,
    }
}

#[tokio::test]
async fn amount_and_date_filters_over_extracted_fields() {
    let dir = unique_dir();
    let index = TantivyIndex::new(&SearchConfig {
        enabled: true,
        dir: dir.clone(),
        ..Default::default()
    })
    .unwrap();

    index
        .index(vec![
            // amount 2_000_000, deadline Sep 1 2026.
            doc(
                "big",
                r#"grant award up to $2 million, close_date 2026-09-01"#,
            ),
            // amount 50_000, deadline Dec 1 2026.
            doc(
                "small",
                "grant award of $50,000, applications due 12/1/2026",
            ),
            // No currency marker, no deadline keyword → both fields ABSENT.
            doc(
                "bare",
                "grant program serving 3,000,000 residents since 2026-09-01",
            ),
        ])
        .await
        .unwrap();
    index.flush().await.unwrap();

    let query = |f: fn(&mut SearchRequest)| {
        let mut req = SearchRequest::new("grant", 10);
        f(&mut req);
        req
    };
    let ids = |resp: pumper_core::SearchResponse| {
        let mut v: Vec<String> = resp.hits.into_iter().map(|h| h.id).collect();
        v.sort();
        v
    };

    // No entity filters: all three match.
    let all = index.query(query(|_| {})).await.unwrap();
    assert_eq!(all.total, 3);

    // amount_gte: only the $2M doc. "bare"'s 3,000,000 has no marker — absent.
    let rich = index
        .query(query(|r| r.amount_gte = Some(1_000_000)))
        .await
        .unwrap();
    assert_eq!(ids(rich), vec!["big"]);

    // amount_lte: only the $50k doc (absent field never matches).
    let modest = index
        .query(query(|r| r.amount_lte = Some(100_000)))
        .await
        .unwrap();
    assert_eq!(ids(modest), vec!["small"]);

    // Inclusive band around $50k.
    let band = index
        .query(query(|r| {
            r.amount_gte = Some(50_000);
            r.amount_lte = Some(50_000);
        }))
        .await
        .unwrap();
    assert_eq!(ids(band), vec!["small"]);

    // date_before Sep 1 (inclusive): only "big". "bare"'s 2026-09-01 lacks a
    // deadline keyword → no event_date, excluded.
    let soon = index
        .query(query(|r| r.date_before = Some(SEP_1)))
        .await
        .unwrap();
    assert_eq!(ids(soon), vec!["big"]);

    // date_after just past Sep 1: only the Dec 1 deadline.
    let later = index
        .query(query(|r| r.date_after = Some(SEP_1 + 1)))
        .await
        .unwrap();
    assert_eq!(ids(later), vec!["small"]);

    // Combined: money + deadline window ("grants over $1M closing by Dec 1").
    let combined = index
        .query(query(|r| {
            r.amount_gte = Some(1_000_000);
            r.date_before = Some(DEC_1);
        }))
        .await
        .unwrap();
    assert_eq!(ids(combined), vec!["big"]);

    // total reflects the filtered match count, the paging denominator.
    let filtered_total = index
        .query(query(|r| r.amount_gte = Some(1)))
        .await
        .unwrap();
    assert_eq!(filtered_total.total, 2, "bare doc has no amount field");

    drop(index);
    let _ = std::fs::remove_dir_all(&dir);
}
