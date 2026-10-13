//! The change feed as a *position*, not a clock reading.
//!
//! `upsert_many` commits in chunks of 500 and every record in one chunk shares
//! ONE revision `created_at` (docs/features/datasets.md § Conventions). The
//! feed's keyset is the tuple `(created_at, rowid)`, but `since` is a bare
//! `created_at > ?` with no tiebreak — so a consumer that resumes from "the
//! newest stamp I saw" silently drops every unread stamp-sibling behind that
//! boundary: up to 499 revisions per occurrence, with no error anywhere.
//!
//! These tests pin the forward (ascending) keyset drain that fixes it, and —
//! as a guard — that the default newest-first page did not move.

use pumper_core::datasets::{ts, FeedOrder};
use pumper_core::testing::TempStore;
use pumper_core::Datasets;
use serde_json::{json, Value};

/// Replaces every distinct `created_at` instant with a deterministic synthetic
/// stamp (`2026-01-01T00:00:0N.000000Z`, in ascending order of first
/// appearance), in both items and cursor tokens. Rowids come from a fresh temp
/// database and are already deterministic (1..N), so after this the feed's
/// serialization is byte-stable across runs while every structural fact —
/// which rows share a stamp, their order, where the page boundaries fall, the
/// exact cursor tokens — is still whatever the real store produced.
fn canonicalize(pages: &[(Vec<pumper_core::datasets::Revision>, Option<String>)]) -> Value {
    let mut stamps: Vec<String> = Vec::new();
    for (items, _) in pages {
        for item in items {
            let raw = ts(item.created_at);
            if !stamps.contains(&raw) {
                stamps.push(raw);
            }
        }
    }
    stamps.sort();
    let synth = |raw: &str| -> String {
        let idx = stamps.iter().position(|s| s == raw).unwrap_or(usize::MAX);
        format!("2026-01-01T00:00:{idx:02}.000000Z")
    };
    let out: Vec<Value> = pages
        .iter()
        .map(|(items, cursor)| {
            let items: Vec<Value> = items
                .iter()
                .map(|r| {
                    let mut v = serde_json::to_value(r).unwrap();
                    let raw = ts(r.created_at);
                    v["created_at"] = Value::String(synth(&raw));
                    v
                })
                .collect();
            let cursor = cursor.as_ref().map(|c| {
                let (ts, tie) = c
                    .rsplit_once('|')
                    .expect("cursor is `<created_at>|<rowid>`");
                format!("{}|{}", synth(ts), tie)
            });
            json!({ "items": items, "next_cursor": cursor })
        })
        .collect();
    Value::Array(out)
}

/// Two `upsert_many` chunks, each sharing one revision stamp: 12 keys then 8.
/// Returns the store so the caller can page it.
async fn shared_stamp_store(tag: &str) -> (TempStore, Datasets) {
    let store = TempStore::new(tag).await;
    let ds = Datasets::new(store.storage.pool());
    let first: Vec<(String, Value)> = (0..12)
        .map(|i| (format!("k{i:02}"), json!({ "n": i, "wave": 1 })))
        .collect();
    ds.upsert_many("grants", "unified", &first).await.unwrap();
    let second: Vec<(String, Value)> = (12..20)
        .map(|i| (format!("k{i:02}"), json!({ "n": i, "wave": 2 })))
        .collect();
    ds.upsert_many("grants", "unified", &second).await.unwrap();
    (store, ds)
}

/// GUARD (acceptance case 3, green before the change by design): the default
/// newest-first page is byte-identical to what the pre-change `changes_page`
/// produced for the same store. The golden below was captured from the
/// handler at base sha c705c2f, before the ascending branch existed. If this
/// goes red, the feed's default contract moved and every consumer outside this
/// tree moved with it.
#[tokio::test]
async fn desc_feed_page_is_byte_identical_to_the_pre_change_handler() {
    let (_store, ds) = shared_stamp_store("feed-desc-guard").await;
    let mut pages = Vec::new();
    let mut after: Option<(String, i64)> = None;
    loop {
        let page = ds
            .changes_page("grants", Some("unified"), None, after.clone(), 7, None)
            .await
            .unwrap();
        let cursor = page.next_cursor.clone();
        pages.push((page.items, cursor.clone()));
        match cursor {
            Some(c) => {
                let (t, r) = c.rsplit_once('|').unwrap();
                after = Some((t.to_string(), r.parse().unwrap()));
            }
            None => break,
        }
    }
    let actual = canonicalize(&pages);
    assert_golden(
        "crates/core/tests/fixtures/changes_desc_golden.json",
        &actual,
        "the newest-first change feed changed shape — this is the wire format \
         products outside this tree consume",
    );
}

/// Golden-file compare, with `PUMPER_UPDATE_FIXTURES=1` to re-emit. The
/// committed file is the artifact; a test that silently rewrote it on every
/// run would pin nothing.
fn assert_golden(repo_rel: &str, actual: &Value, why: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(repo_rel);
    let rendered = serde_json::to_string_pretty(actual).unwrap() + "\n";
    if std::env::var("PUMPER_UPDATE_FIXTURES").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        return;
    }
    let committed = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{repo_rel}: {e} (re-emit with PUMPER_UPDATE_FIXTURES=1)"));
    assert_eq!(
        serde_json::from_str::<Value>(&committed).unwrap(),
        *actual,
        "{why} ({repo_rel}; re-emit with PUMPER_UPDATE_FIXTURES=1 only if the move is intended)"
    );
}

/// A store with 600 keys written in ONE `upsert_many`, which the store commits
/// as two chunks (500 + 100) — so there are exactly two distinct revision
/// stamps and the first is shared by 500 siblings.
async fn six_hundred_store(tag: &str) -> (TempStore, Datasets) {
    let store = TempStore::new(tag).await;
    let ds = Datasets::new(store.storage.pool());
    let items: Vec<(String, Value)> = (0..600)
        .map(|i| (format!("k{i:04}"), json!({ "n": i })))
        .collect();
    ds.upsert_many("grants", "unified", &items).await.unwrap();
    (store, ds)
}

/// Drains the whole feed forward with the ascending keyset, returning the keys
/// in the order they were delivered.
async fn drain_ascending(ds: &Datasets, limit: i64) -> Vec<String> {
    let mut keys = Vec::new();
    let mut after: Option<(String, i64)> = None;
    loop {
        let page = ds
            .changes_page_ordered(
                "grants",
                Some("unified"),
                None,
                after.clone(),
                limit,
                None,
                FeedOrder::Asc,
            )
            .await
            .unwrap();
        if page.items.is_empty() {
            break;
        }
        let full = (page.items.len() as i64) == limit;
        keys.extend(page.items.iter().map(|r| r.key.clone()));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        if !full {
            break;
        }
        let (t, r) = cursor.rsplit_once('|').unwrap();
        after = Some((t.to_string(), r.parse().unwrap()));
    }
    keys
}

/// Acceptance 1. The union of every ascending page is the whole feed — no row
/// delivered twice, none missing — across a stamp shared by 500 siblings and a
/// page size that cuts right through it.
#[tokio::test]
async fn ascending_drain_delivers_every_revision_exactly_once() {
    let (_store, ds) = six_hundred_store("feed-asc-drain").await;
    let keys = drain_ascending(&ds, 250).await;
    assert_eq!(keys.len(), 600, "every revision is delivered");
    let distinct: std::collections::BTreeSet<String> = keys.into_iter().collect();
    assert_eq!(distinct.len(), 600, "no revision is delivered twice");
    let expected: std::collections::BTreeSet<String> =
        (0..600).map(|i| format!("k{i:04}")).collect();
    assert_eq!(distinct, expected, "no revision is missing");
}

/// Acceptance 1 (ordering half): an ascending page is non-decreasing by
/// `(created_at, rowid)` and its `next_cursor` names its LAST row, so the
/// cursor a consumer stores is the position it actually reached.
#[tokio::test]
async fn ascending_page_is_ordered_and_its_cursor_names_the_last_row() {
    let (_store, ds) = six_hundred_store("feed-asc-order").await;
    let page = ds
        .changes_page_ordered(
            "grants",
            Some("unified"),
            None,
            None,
            50,
            None,
            FeedOrder::Asc,
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 50);
    for w in page.items.windows(2) {
        assert!(
            w[0].created_at <= w[1].created_at,
            "ascending page must be non-decreasing by created_at"
        );
    }
    let cursor = page
        .next_cursor
        .expect("an ascending page always checkpoints, so a drain can resume from its tail");
    let (cursor_ts, _rowid) = cursor.rsplit_once('|').unwrap();
    assert_eq!(
        cursor_ts,
        ts(page.items.last().unwrap().created_at),
        "next_cursor names the LAST (newest) row of the page, not the first"
    );
}

/// Acceptance 2, and the defect itself. Resume from a cursor whose
/// `created_at` is shared by 400 rows nobody has read yet: the keyset resume
/// returns all 400, while the timestamp resume (`created_at > S`, what the SDK
/// does today) returns NONE of them — it skips the rest of the chunk silently.
#[tokio::test]
async fn keyset_resume_returns_stamp_siblings_that_since_silently_drops() {
    let (_store, ds) = six_hundred_store("feed-stamp-siblings").await;

    // Read the first 100 rows of the 500-sibling chunk, then stop.
    let first = ds
        .changes_page_ordered(
            "grants",
            Some("unified"),
            None,
            None,
            100,
            None,
            FeedOrder::Asc,
        )
        .await
        .unwrap();
    let boundary = first.items.last().unwrap().created_at;
    assert!(
        first.items.iter().all(|r| r.created_at == boundary),
        "the whole first page sits inside one shared stamp — that is the premise"
    );
    let cursor = first.next_cursor.clone().unwrap();
    let (cursor_ts, rowid) = cursor.rsplit_once('|').unwrap();

    // Keyset resume: the 400 unread siblings come back.
    let resumed = ds
        .changes_page_ordered(
            "grants",
            Some("unified"),
            None,
            Some((cursor_ts.to_string(), rowid.parse().unwrap())),
            500,
            None,
            FeedOrder::Asc,
        )
        .await
        .unwrap();
    assert_eq!(
        resumed
            .items
            .iter()
            .filter(|r| r.created_at == boundary)
            .count(),
        400,
        "a keyset resume must return every unread sibling of the boundary stamp"
    );

    // Timestamp resume (today's SDK): every one of those 400 is gone.
    let by_timestamp = ds
        .changes_page("grants", Some("unified"), Some(boundary), None, 1000, None)
        .await
        .unwrap();
    assert_eq!(
        by_timestamp
            .items
            .iter()
            .filter(|r| r.created_at == boundary)
            .count(),
        0,
        "this is the bug the card exists for: `created_at > S` drops every \
         sibling of S, so those 400 revisions are unreachable from a timestamp \
         watermark — no error, no signal, permanent divergence"
    );
}

/// Emits the fixture the TypeScript conformance suite replays. It is NOT
/// hand-authored: every page here is what a real `Datasets` store answered for
/// a real two-chunk write, including where the page boundaries fall relative
/// to the shared stamps. Only the wall-clock instants are normalized (see
/// `canonicalize`) so the committed file is byte-stable.
#[tokio::test]
async fn shared_stamp_feed_fixture_is_emitted_from_a_real_store() {
    let (_store, ds) = shared_stamp_store("feed-shared-stamp-fixture").await;
    let mut pages = Vec::new();
    let mut after: Option<(String, i64)> = None;
    loop {
        let page = ds
            .changes_page_ordered(
                "grants",
                Some("unified"),
                None,
                after.clone(),
                7,
                None,
                FeedOrder::Asc,
            )
            .await
            .unwrap();
        if page.items.is_empty() {
            break;
        }
        let full = page.items.len() == 7;
        let cursor = page.next_cursor.clone();
        pages.push((page.items, cursor.clone()));
        if !full {
            break;
        }
        let (t, r) = cursor
            .unwrap()
            .rsplit_once('|')
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .unwrap();
        after = Some((t, r.parse().unwrap()));
    }
    let stamps: std::collections::BTreeSet<_> = pages
        .iter()
        .flat_map(|(items, _)| items.iter().map(|r| r.created_at))
        .collect();
    assert_eq!(stamps.len(), 2, "two chunks, two shared stamps");
    assert_eq!(
        pages.iter().map(|(i, _)| i.len()).sum::<usize>(),
        20,
        "every key reaches the fixture"
    );

    let fixture = json!({
        "generated_by": "crates/core/tests/change_feed_keyset.rs::shared_stamp_feed_fixture_is_emitted_from_a_real_store",
        "note": "Emitted from a real Datasets store: two upsert_many chunks, each sharing one revision created_at, paged ascending at limit 7 so the page boundaries cut through a stamp group. Only the wall-clock instants are normalized. Re-emit with PUMPER_UPDATE_FIXTURES=1.",
        "dataset": { "app": "grants", "name": "unified" },
        "distinct_keys": 20,
        "pages": canonicalize(&pages),
    });
    assert_golden(
        "clients/typescript/test/fixtures/feed-shared-stamp.json",
        &fixture,
        "the shared-stamp feed the TypeScript SDK replays no longer matches what the store emits",
    );
}
