//! Differential test for the batched bulk-upsert path.
//!
//! `Datasets::upsert_many` no longer issues SELECT + write + revision-insert per
//! record; it reads a whole chunk's state in one statement, decides every verdict
//! in memory, and writes the chunk as multi-row statements. That is only a
//! *performance* change if it produces byte-identical results to the per-record
//! sequence it replaced — so this file runs randomized batches through both and
//! compares everything the change feed and every consumer can observe:
//! new/changed/unchanged/removed verdicts, the record rows, and the full
//! revision chains (numbers, kinds, snapshots, diffs).
//!
//! `Datasets::upsert` IS the per-record reference implementation: the batch path
//! used to be a loop over exactly its transactional body.

use std::collections::HashMap;
use std::sync::Arc;

use pumper_core::datasets::{ChangeKind, RemovalGuard};
use pumper_core::resilience::SourceState;
use pumper_core::testing::TempStore;
use pumper_core::Datasets;
use serde_json::{json, Value};
use sqlx::SqlitePool;

/// Deterministic xorshift64* — a seeded generator so a failure is reproducible
/// and the suite never depends on a rand crate.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// A record shape drawn from the mixes that historically broke change detection:
/// explicit JSON nulls, fields that are absent rather than null, nested objects,
/// arrays, and an empty object (whose SimHash is 0).
fn value(rng: &mut Rng, n: usize) -> Value {
    match rng.below(6) {
        0 => json!({ "title": format!("t{n}"), "amount": n, "closed": Value::Null }),
        1 => json!({ "title": format!("t{n}") }), // `amount`/`closed` ABSENT, not null
        2 => json!({ "title": Value::Null, "amount": Value::Null }),
        3 => json!({ "title": format!("t{n}"), "nested": { "a": n, "b": [1, 2, n] } }),
        4 => json!({}),
        _ => json!({ "title": format!("t{n}"), "amount": n }),
    }
}

/// All observable record state, keyed by record key: (hash, data, is_tombstoned).
async fn records_of(pool: &SqlitePool) -> HashMap<String, (String, String, bool)> {
    let rows: Vec<(String, String, String, Option<String>)> =
        sqlx::query_as("SELECT key, hash, data, removed_at FROM records ORDER BY key")
            .fetch_all(pool)
            .await
            .unwrap();
    rows.into_iter()
        .map(|(k, h, d, r)| (k, (h, d, r.is_some())))
        .collect()
}

/// Every revision chain: key -> [(revision, change, data, diff)] in order.
#[allow(clippy::type_complexity)]
async fn revisions_of(
    pool: &SqlitePool,
) -> Vec<(String, i64, String, Option<String>, Option<String>)> {
    sqlx::query_as(
        "SELECT key, revision, change, data, diff FROM record_revisions ORDER BY key, revision",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn batched_upsert_many_matches_per_record_upserts_not_a_batched_approximation() {
    // Two stores fed identical inputs: one through the batched path, one record
    // by record. `chunk` sizes straddle UPSERT_CHUNK (500) so multi-chunk
    // batches, IN-list slicing and multi-row statement slicing are all exercised.
    let batched_store = TempStore::new("dsdiff-batched").await;
    let reference_store = TempStore::new("dsdiff-reference").await;
    let batched = Datasets::new(batched_store.storage.pool());
    let reference = Datasets::new(reference_store.storage.pool());

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    const KEY_SPACE: usize = 260;

    for round in 0..14 {
        // Batch size straddles the 500-record commit chunk on some rounds.
        let size = match round % 4 {
            0 => 1 + rng.below(40),
            1 => 480 + rng.below(60),
            2 => 1 + rng.below(200),
            _ => 900 + rng.below(120),
        };
        let items: Vec<(String, Value)> = (0..size)
            .map(|_| {
                // A small key space over a large batch guarantees DUPLICATE KEYS
                // WITHIN A BATCH — the case a batched read cannot see and the
                // per-record loop got right for free.
                let k = rng.below(KEY_SPACE);
                let n = rng.below(7);
                (format!("k{k:04}"), value(&mut rng, n))
            })
            .collect();

        let mut b = batched.upsert_many("app", "d", &items).await.unwrap();
        let mut r = per_record_upsert_many(&reference, "app", "d", &items).await;

        // Every third round is a full-snapshot sync: removals must agree too.
        if round % 3 == 2 {
            let present: Vec<String> = items
                .iter()
                .map(|(k, _)| k.clone())
                .filter(|_| rng.below(4) != 0)
                .collect();
            if !present.is_empty() {
                b.removed = batched
                    .detect_removed("app", "d", &present, ok_guard())
                    .await
                    .unwrap();
                r.removed = reference
                    .detect_removed("app", "d", &present, ok_guard())
                    .await
                    .unwrap();
                b.removed.sort();
                r.removed.sort();
            }
        }

        assert_eq!(b.new, r.new, "round {round}: `new` verdicts diverged");
        assert_eq!(
            b.changed, r.changed,
            "round {round}: `changed` verdicts diverged"
        );
        assert_eq!(
            b.unchanged, r.unchanged,
            "round {round}: `unchanged` count diverged"
        );
        assert_eq!(
            b.removed, r.removed,
            "round {round}: `removed` verdicts diverged"
        );

        assert_eq!(
            records_of(&batched_store.storage.pool()).await,
            records_of(&reference_store.storage.pool()).await,
            "round {round}: stored records diverged (hash/data/tombstone)"
        );
        assert_eq!(
            revisions_of(&batched_store.storage.pool()).await,
            revisions_of(&reference_store.storage.pool()).await,
            "round {round}: revision chains diverged (number/kind/snapshot/diff)"
        );
    }
}

/// A healthy source's removal guard — the only way to reach removal detection.
fn ok_guard() -> RemovalGuard {
    RemovalGuard::for_source_state(SourceState::Healthy).expect("a healthy source permits removals")
}

/// The reference implementation: the per-record loop the batch path replaced.
async fn per_record_upsert_many(
    ds: &Datasets,
    app: &str,
    dataset: &str,
    items: &[(String, Value)],
) -> pumper_core::datasets::UpsertSummary {
    let mut summary = pumper_core::datasets::UpsertSummary::default();
    for (key, value) in items {
        match ds.upsert(app, dataset, key, value).await.unwrap() {
            ChangeKind::New => summary.new.push(key.clone()),
            ChangeKind::Changed => summary.changed.push(key.clone()),
            ChangeKind::Unchanged => summary.unchanged += 1,
        }
    }
    summary
}

#[tokio::test]
async fn a_key_changed_then_reconfirmed_in_one_batch_keeps_the_new_content() {
    // The collapse trap: within one batch a key goes Changed (v1 -> v2) and then
    // appears again as Unchanged (v2). Collapsing the chunk's record writes by
    // LAST OCCURRENCE would let the trailing Unchanged win and leave v1 in the
    // row — while the revision chain already says it moved to v2. Collapsing by
    // last *content-bearing* write is what keeps the two agreeing.
    let store = TempStore::new("dsdiff-collapse").await;
    let ds = Datasets::new(store.storage.pool());
    let pool = store.storage.pool();

    ds.upsert("app", "d", "k", &json!({ "v": 1 }))
        .await
        .unwrap();
    let summary = ds
        .upsert_many(
            "app",
            "d",
            &[
                ("k".to_string(), json!({ "v": 2 })),
                ("k".to_string(), json!({ "v": 2 })),
            ],
        )
        .await
        .unwrap();
    assert_eq!(summary.changed, vec!["k".to_string()]);
    assert_eq!(summary.unchanged, 1);

    let data: String = sqlx::query_scalar("SELECT data FROM records WHERE key = 'k'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&data).unwrap(),
        json!({ "v": 2 }),
        "the row must hold the content the revision chain claims"
    );
    let chain: Vec<(i64, String)> =
        sqlx::query_as("SELECT revision, change FROM record_revisions ORDER BY revision")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        chain,
        vec![(1, "new".to_string()), (2, "changed".to_string())],
        "one revision per content-bearing occurrence, contiguous"
    );
}

#[tokio::test]
async fn a_key_repeated_in_one_batch_gets_a_contiguous_revision_chain() {
    // Same key three times with three values: New, Changed, Changed — revisions
    // 1..=3 with the diffs chained through the in-batch writes, not all three
    // diffed against the pre-batch snapshot.
    let store = TempStore::new("dsdiff-repeat").await;
    let ds = Datasets::new(store.storage.pool());
    let pool = store.storage.pool();

    let summary = ds
        .upsert_many(
            "app",
            "d",
            &[
                ("k".to_string(), json!({ "v": 1 })),
                ("k".to_string(), json!({ "v": 2 })),
                ("k".to_string(), json!({ "v": 3 })),
            ],
        )
        .await
        .unwrap();
    assert_eq!(summary.new, vec!["k".to_string()]);
    assert_eq!(summary.changed, vec!["k".to_string(), "k".to_string()]);

    let chain: Vec<(i64, String, Option<String>)> =
        sqlx::query_as("SELECT revision, change, diff FROM record_revisions ORDER BY revision")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[0].1, "new");
    assert_eq!(chain[1].1, "changed");
    assert_eq!(chain[2].1, "changed");
    // Revision 3 diffs against revision 2's value (2 -> 3), not against v1.
    let diff: Value = serde_json::from_str(chain[2].2.as_deref().unwrap()).unwrap();
    assert_eq!(diff, json!({ "v": { "from": 2, "to": 3 } }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_batch_writers_keep_the_revision_chain_intact() {
    // The batch path now reads MAX(revision) into memory instead of computing it
    // in a per-row subquery. That is only safe because the read happens inside
    // the chunk's own BEGIN IMMEDIATE, so a second batch writer waits for the
    // COMMIT. 20 concurrent same-key batches must still yield a contiguous
    // 1..=20 chain — two writers computing the same next revision would collide
    // on the primary key and abort a batch.
    let store = TempStore::new("dsdiff-concurrent-batch").await;
    let ds = Arc::new(Datasets::new(store.storage.pool()));
    let pool = store.storage.pool();

    const N: i64 = 20;
    let mut handles = Vec::new();
    for i in 0..N {
        let ds = Arc::clone(&ds);
        handles.push(tokio::spawn(async move {
            ds.upsert_many("app", "d", &[("k".to_string(), json!({ "v": i }))])
                .await
        }));
    }
    for h in handles {
        h.await.expect("task joined").expect("batch ok");
    }

    let revisions: Vec<i64> =
        sqlx::query_scalar("SELECT revision FROM record_revisions ORDER BY revision")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(revisions, (1..=N).collect::<Vec<_>>());
}
