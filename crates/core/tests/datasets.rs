//! Integration test for dataset upsert atomicity under concurrency, against a
//! real temp-dir SQLite (WAL + busy_timeout) with the full migration chain.
//! Proves that concurrent same-key writers do not corrupt the per-key revision
//! chain — the bug that motivated wrapping upsert in a BEGIN IMMEDIATE
//! transaction (SELECT + record write + revision append as one atomic unit).

use std::sync::Arc;

use pumper_core::Datasets;
use serde_json::json;

use pumper_core::testing::TempStore;

async fn fresh_db(tag: &str) -> TempStore {
    TempStore::new(tag).await
}

/// A healthy source's removal guard. Removal detection is only reachable with
/// one, and only a non-degrading `SourceState` yields one.
fn ok_guard() -> pumper_core::datasets::RemovalGuard {
    pumper_core::datasets::RemovalGuard::for_source_state(
        pumper_core::resilience::SourceState::Healthy,
    )
    .expect("a healthy source permits removals")
}

#[tokio::test]
async fn reindex_rewrites_stale_simhashes_without_touching_content() {
    let store = fresh_db("datasets-reindex").await;
    let storage = &store.storage;
    let pool = storage.pool();
    let ds = Datasets::new(storage.pool());

    ds.upsert(
        "app",
        "d",
        "k",
        &json!({ "title": "hello world simhash reindex" }),
    )
    .await
    .unwrap();

    // What the current hash should produce, plus the content fields that must NOT move.
    let (correct_sim, hash_before, updated_before): (i64, String, String) =
        sqlx::query_as("SELECT simhash, hash, updated_at FROM records WHERE key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();

    // Simulate a fingerprint left behind by an older token hash.
    sqlx::query("UPDATE records SET simhash = 12345 WHERE key = 'k'")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        ds.reindex_simhashes().await.unwrap(),
        1,
        "stale row must be rewritten"
    );

    let (sim_after, hash_after, updated_after): (i64, String, String) =
        sqlx::query_as("SELECT simhash, hash, updated_at FROM records WHERE key = 'k'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        sim_after, correct_sim,
        "simhash recomputed from the stored data"
    );
    // Content hash + timestamps untouched → the change-feed sees no fake revision.
    assert_eq!(hash_after, hash_before, "content hash must not move");
    assert_eq!(updated_after, updated_before, "updated_at must not move");

    // Idempotent: a second run finds nothing to rewrite.
    assert_eq!(
        ds.reindex_simhashes().await.unwrap(),
        0,
        "reindex must be idempotent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_key_upserts_keep_revision_chain_intact() {
    let store = fresh_db("datasets-concurrency").await;
    let storage = &store.storage;
    let pool = storage.pool();
    let ds = Arc::new(Datasets::new(storage.pool()));

    // 20 concurrent writers, each upserting the SAME key with a DISTINCT value.
    // Serialized correctly, each observes a different prior and appends exactly
    // one revision → a contiguous 1..=20 chain. The pre-fix non-atomic path let
    // two writers compute the same MAX(revision)+1 (duplicate/aborted revisions).
    const N: i64 = 20;
    let mut handles = Vec::new();
    for i in 0..N {
        let ds = ds.clone();
        handles.push(tokio::spawn(async move {
            ds.upsert("app", "d", "k", &json!({ "v": i })).await
        }));
    }
    for h in handles {
        h.await.expect("task joined").expect("upsert ok");
    }

    // Exactly one record for the key.
    let record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM records WHERE app = 'app' AND dataset = 'd' AND key = 'k'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(record_count, 1, "the key must resolve to a single record");

    // Revision numbers are exactly 1..=N — contiguous, unique, none lost.
    let revisions: Vec<i64> = sqlx::query_scalar(
        "SELECT revision FROM record_revisions \
         WHERE app = 'app' AND dataset = 'd' AND key = 'k' ORDER BY revision",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let expected: Vec<i64> = (1..=N).collect();
    assert_eq!(
        revisions, expected,
        "revision chain must be contiguous 1..={N} with no duplicates or gaps"
    );
}

#[tokio::test]
async fn list_filtered_ordered_returns_soonest_rows_past_the_cap() {
    // The closing-soon correctness bug: ordering by close_date must happen in SQL
    // *before* the LIMIT, or a small cap returns an arbitrary (updated_at) slice
    // that an in-memory sort only reorders — silently dropping a grant closing
    // tomorrow. Seed more matches than the cap, with close dates in shuffled
    // insert order, and assert the cap returns the genuinely soonest ones.
    use pumper_core::datasets::JsonFilter;

    let store = fresh_db("datasets-ordered").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());

    // Insert 10 open grants with close dates 2026-03-10 .. 2026-03-01 in an order
    // that is NOT close-date order (so updated_at order != close_date order).
    let order = [5, 9, 1, 7, 3, 10, 2, 8, 4, 6];
    for day in order {
        let key = format!("g{day:02}");
        let close = format!("2026-03-{day:02}");
        ds.upsert(
            "grants",
            "unified",
            &key,
            &json!({ "status": "open", "close_date": close }),
        )
        .await
        .unwrap();
    }

    let filters = vec![
        JsonFilter::Eq {
            path: "$.status".into(),
            value: "open".into(),
        },
        JsonFilter::Gte {
            path: "$.close_date".into(),
            value: "2026-01-01".into(),
        },
    ];

    // count_filtered reports the true total, independent of any cap.
    let count = ds
        .count_filtered("grants", "unified", &filters)
        .await
        .unwrap();
    assert_eq!(count, 10, "count is the full window, not the return cap");

    // A cap of 3 must return the three SOONEST (01, 02, 03), not an arbitrary slice.
    let top = ds
        .list_filtered_ordered("grants", "unified", &filters, "$.close_date", 3)
        .await
        .unwrap();
    let closes: Vec<String> = top
        .iter()
        .map(|r| {
            r.data
                .get("close_date")
                .and_then(|v| v.as_str())
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(closes, vec!["2026-03-01", "2026-03-02", "2026-03-03"]);
}

#[tokio::test]
async fn upsert_many_is_correct_across_chunk_boundaries() {
    // 600 records exceeds the 500-record commit chunk, so this exercises the
    // multi-transaction batch path. Correctness must be identical to per-record.
    let store = fresh_db("datasets-upsert-many").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());

    let items: Vec<(String, serde_json::Value)> = (0..600)
        .map(|i| (format!("k{i:04}"), json!({ "n": i })))
        .collect();

    // First run: all new.
    let s1 = ds.upsert_many("app", "d", &items).await.unwrap();
    assert_eq!(s1.new.len(), 600);
    assert_eq!(s1.changed.len(), 0);
    assert_eq!(s1.unchanged, 0);

    // Re-run identical: all unchanged (no new revisions).
    let s2 = ds.upsert_many("app", "d", &items).await.unwrap();
    assert_eq!(s2.unchanged, 600, "identical re-upsert is all unchanged");
    assert_eq!(s2.new.len(), 0);

    // Change one record on each side of the chunk boundary.
    let changed = vec![
        ("k0007".to_string(), json!({ "n": 7, "extra": true })),
        ("k0512".to_string(), json!({ "n": 512, "extra": true })),
    ];
    let s3 = ds.upsert_many("app", "d", &changed).await.unwrap();
    assert_eq!(s3.changed.len(), 2);

    // Every record resolves to exactly one row; the two changed keys have 2
    // revisions (new + changed), the rest have 1 — the chain stayed intact.
    let pool = storage.pool();
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app='app' AND dataset='d'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(total, 600);
    let revs_changed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM record_revisions WHERE app='app' AND dataset='d' AND key='k0512'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(revs_changed, 2, "new + changed revisions");
}

#[tokio::test]
async fn detect_removed_tombstones_with_matching_removed_revisions() {
    // Every tombstone must have its `removed` revision — the atomicity guarantee.
    // A tombstone without a revision is a permanently-lost removal signal.
    let store = fresh_db("datasets-detect-removed").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());
    let pool = storage.pool();

    // Seed 5 live records.
    let items: Vec<(String, serde_json::Value)> = (0..5)
        .map(|i| (format!("k{i}"), json!({ "n": i })))
        .collect();
    ds.upsert_many("app", "d", &items).await.unwrap();

    // Next full snapshot drops k1 and k3.
    let present: Vec<String> = vec!["k0".into(), "k2".into(), "k4".into()];
    let mut removed = ds
        .detect_removed("app", "d", &present, ok_guard())
        .await
        .unwrap();
    removed.sort();
    assert_eq!(removed, vec!["k1".to_string(), "k3".to_string()]);

    // Each removed key is tombstoned AND has a `removed` revision (they agree).
    for key in ["k1", "k3"] {
        let removed_at: Option<String> = sqlx::query_scalar(
            "SELECT removed_at FROM records WHERE app='app' AND dataset='d' AND key=?1",
        )
        .bind(key)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(removed_at.is_some(), "{key} must be tombstoned");
        let rev_changes: Vec<String> = sqlx::query_scalar(
            "SELECT change FROM record_revisions WHERE app='app' AND dataset='d' AND key=?1 ORDER BY revision",
        )
        .bind(key)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rev_changes,
            vec!["new", "removed"],
            "{key} revision chain: new then removed"
        );
    }

    // Idempotent: a second identical snapshot re-removes nothing (already tombstoned).
    let removed2 = ds
        .detect_removed("app", "d", &present, ok_guard())
        .await
        .unwrap();
    assert!(
        removed2.is_empty(),
        "already-removed keys are not re-removed"
    );
}

#[tokio::test]
async fn detect_removed_noops_on_an_empty_snapshot() {
    // A failed scrape hands sync an empty `present` set. That must be a no-op,
    // never "tombstone the whole dataset" — the empty-present guard was added
    // after exactly that bug and this test is what keeps it from reverting.
    let store = fresh_db("datasets-detect-removed-empty").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());
    let pool = storage.pool();

    let items: Vec<(String, serde_json::Value)> = (0..5)
        .map(|i| (format!("k{i}"), json!({ "n": i })))
        .collect();
    ds.upsert_many("app", "d", &items).await.unwrap();

    let removed = ds
        .detect_removed("app", "d", &[], ok_guard())
        .await
        .unwrap();
    assert!(removed.is_empty(), "empty snapshot must remove nothing");

    let live: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM records WHERE app='app' AND dataset='d' AND removed_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(live, 5, "all records still live after an empty snapshot");
}

#[tokio::test]
async fn delete_record_and_dataset_remove_rows_and_revisions() {
    let store = fresh_db("datasets-delete").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());
    let pool = storage.pool();

    // Seed 3 records, changing one so it has 2 revisions.
    ds.upsert("app", "d", "k1", &json!({ "n": 1 }))
        .await
        .unwrap();
    ds.upsert("app", "d", "k2", &json!({ "n": 2 }))
        .await
        .unwrap();
    ds.upsert("app", "d", "k2", &json!({ "n": 22 }))
        .await
        .unwrap();
    ds.upsert("app", "d", "k3", &json!({ "n": 3 }))
        .await
        .unwrap();

    // delete_record removes the row AND its whole revision history.
    assert!(ds.delete_record("app", "d", "k2").await.unwrap(), "existed");
    let rec_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM records WHERE app='app' AND dataset='d' AND key='k2'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let rev_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM record_revisions WHERE app='app' AND dataset='d' AND key='k2'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        (rec_rows, rev_rows),
        (0, 0),
        "record and its 2 revisions gone"
    );
    // Deleting a missing record reports false, doesn't error.
    assert!(
        !ds.delete_record("app", "d", "k2").await.unwrap(),
        "already gone"
    );

    // delete_dataset removes the remaining records + all revisions, returns count.
    let removed = ds.delete_dataset("app", "d").await.unwrap();
    assert_eq!(removed, 2, "k1 + k3 removed");
    let total_recs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE app='app' AND dataset='d'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let total_revs: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM record_revisions WHERE app='app' AND dataset='d'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((total_recs, total_revs), (0, 0), "dataset fully gone");
}

#[tokio::test]
async fn prune_revisions_keeps_the_newest_n_and_respects_the_cutoff() {
    let store = fresh_db("datasets-prune").await;
    let storage = &store.storage;
    let ds = Datasets::new(storage.pool());
    let pool = storage.pool();

    // 5 revisions for k (1 new + 4 changed), 3 for k2.
    for v in 1..=5 {
        ds.upsert("app", "d", "k", &json!({ "n": v }))
            .await
            .unwrap();
    }
    for v in 1..=3 {
        ds.upsert("app", "d", "k2", &json!({ "n": v }))
            .await
            .unwrap();
    }

    // Cutoff in the past: nothing is older, so nothing is pruned.
    let none = ds
        .prune_revisions(chrono::Utc::now() - chrono::Duration::days(1), 1)
        .await
        .unwrap();
    assert_eq!(none, 0, "no revision predates the cutoff");

    // Cutoff in the future (all revisions older), keep newest 2 per key:
    // k prunes 3 (5 -> 2), k2 prunes 1 (3 -> 2) = 4.
    let pruned = ds
        .prune_revisions(chrono::Utc::now() + chrono::Duration::days(1), 2)
        .await
        .unwrap();
    assert_eq!(pruned, 4);

    // The kept revisions are the newest 2 of each key (highest revision numbers).
    let kept_k: Vec<i64> = sqlx::query_scalar(
        "SELECT revision FROM record_revisions WHERE app='app' AND dataset='d' AND key='k' ORDER BY revision",
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(kept_k, vec![4, 5], "newest 2 of k survive");
    let kept_k2: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM record_revisions WHERE app='app' AND dataset='d' AND key='k2'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(kept_k2, 2);
}

// ── derived datasets (M11) ───────────────────────────────────────────────────

use std::collections::BTreeMap;

use pumper_core::{DerivedLookup, NewDerivedSpec, Storage};

/// Creates a derived spec with the given shape (source app fixed to "app").
async fn make_spec(
    storage: &Storage,
    source: &str,
    target: &str,
    filters: &[&str],
    project: &[(&str, &str)],
    lookup: Option<DerivedLookup>,
) -> pumper_core::DerivedSpec {
    let filters: Vec<String> = filters.iter().map(|s| s.to_string()).collect();
    let project: BTreeMap<String, String> = project
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: "app",
            source_dataset: source,
            target_dataset: target,
            filters: &filters,
            project: &project,
            lookup: lookup.as_ref(),
            group: None,
        })
        .await
        .expect("create derived spec")
}

/// Creates an aggregate (group_by) spec: source → target with the given group
/// paths and `{out: expr}` aggregates (source app fixed to "app").
async fn make_group_spec(
    storage: &Storage,
    source: &str,
    target: &str,
    filters: &[&str],
    group_by: &[&str],
    aggregates: &[(&str, &str)],
) -> pumper_core::Result<pumper_core::DerivedSpec> {
    let filters: Vec<String> = filters.iter().map(|s| s.to_string()).collect();
    let project = BTreeMap::new();
    let group = pumper_core::DerivedGroup {
        group_by: group_by.iter().map(|s| s.to_string()).collect(),
        aggregates: aggregates
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    };
    storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: "app",
            source_dataset: source,
            target_dataset: target,
            filters: &filters,
            project: &project,
            lookup: None,
            group: Some(&group),
        })
        .await
}

#[tokio::test]
async fn derived_spec_filters_and_projects_fresh_keys_on_upsert() {
    let store = fresh_db("derived-basic").await;
    let ds = store.datasets();
    make_spec(
        &store.storage,
        "grants",
        "ca_grants",
        &["$.state:eq:CA"],
        &[("name", "$.title"), ("state", "$.state")],
        None,
    )
    .await;

    let items = vec![
        (
            "g1".to_string(),
            json!({ "title": "Solar", "state": "CA", "noise": 1 }),
        ),
        ("g2".to_string(), json!({ "title": "Wind", "state": "NY" })),
        ("g3".to_string(), json!({ "title": "Hydro", "state": "CA" })),
    ];
    ds.upsert_many("app", "grants", &items).await.unwrap();

    // Matching rows land projected, keyed by the source key; NY is filtered out.
    let g1 = ds
        .get("app", "ca_grants", "g1")
        .await
        .unwrap()
        .expect("g1 derived");
    assert_eq!(g1.data, json!({ "name": "Solar", "state": "CA" }));
    assert!(ds.get("app", "ca_grants", "g3").await.unwrap().is_some());
    assert!(ds.get("app", "ca_grants", "g2").await.unwrap().is_none());

    // Re-upserting unchanged source rows recomputes nothing: fresh-keys-only,
    // and the target's change detection dedups no-ops.
    ds.upsert_many("app", "grants", &items).await.unwrap();
    let history = ds.history("app", "ca_grants", "g1", 10).await.unwrap();
    assert_eq!(history.len(), 1, "no spurious derived revisions");

    // A source CHANGE flows through as a derived change.
    ds.upsert_many(
        "app",
        "grants",
        &[(
            "g1".to_string(),
            json!({ "title": "Solar III", "state": "CA" }),
        )],
    )
    .await
    .unwrap();
    let g1 = ds.get("app", "ca_grants", "g1").await.unwrap().unwrap();
    assert_eq!(g1.data["name"], "Solar III");
}

#[tokio::test]
async fn derived_lookup_merges_the_sibling_dataset_record() {
    let store = fresh_db("derived-lookup").await;
    let ds = store.datasets();
    // Lookup side first (no spec on it, so no cascade).
    ds.upsert_many(
        "app",
        "agencies",
        &[(
            "doe".to_string(),
            json!({ "name": "Dept of Energy", "tier": 1 }),
        )],
    )
    .await
    .unwrap();
    make_spec(
        &store.storage,
        "grants",
        "grants_enriched",
        &[],
        &[("title", "$.title"), ("agency", "$.agency")],
        Some(DerivedLookup {
            dataset: "agencies".into(),
            key_expr: "$.agency".into(),
            merge_as: "agency_info".into(),
        }),
    )
    .await;

    ds.upsert_many(
        "app",
        "grants",
        &[
            (
                "g1".to_string(),
                json!({ "title": "Solar", "agency": "doe" }),
            ),
            (
                "g2".to_string(),
                json!({ "title": "Wind", "agency": "unknown" }),
            ),
        ],
    )
    .await
    .unwrap();

    let g1 = ds
        .get("app", "grants_enriched", "g1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(g1.data["agency_info"]["name"], "Dept of Energy");
    // A missing lookup record still lands the row — just unenriched.
    let g2 = ds
        .get("app", "grants_enriched", "g2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(g2.data["title"], "Wind");
    assert!(g2.data.get("agency_info").is_none());
}

#[tokio::test]
async fn derived_chain_stops_at_the_depth_cap() {
    let store = fresh_db("derived-depth").await;
    let ds = store.datasets().with_derived_max_depth(2);
    // a -> b -> c -> d (all passthrough). Cap 2 allows b (depth 1) and c
    // (depth 2); d would be depth 3 and must be skipped.
    make_spec(&store.storage, "a", "b", &[], &[], None).await;
    make_spec(&store.storage, "b", "c", &[], &[], None).await;
    make_spec(&store.storage, "c", "d", &[], &[], None).await;

    ds.upsert_many("app", "a", &[("k".to_string(), json!({ "v": 1 }))])
        .await
        .unwrap();

    assert!(
        ds.get("app", "b", "k").await.unwrap().is_some(),
        "depth 1 lands"
    );
    assert!(
        ds.get("app", "c", "k").await.unwrap().is_some(),
        "depth 2 lands"
    );
    assert!(
        ds.get("app", "d", "k").await.unwrap().is_none(),
        "depth 3 is past the cap and must be skipped"
    );
}

#[tokio::test]
async fn derived_kill_switch_makes_a_spec_fully_inert() {
    let store = fresh_db("derived-kill").await;
    let ds = store.datasets();
    let spec = make_spec(&store.storage, "src", "dst", &[], &[], None).await;
    store
        .storage
        .set_derived_enabled(&spec.id, false)
        .await
        .unwrap();

    ds.upsert_many("app", "src", &[("k1".to_string(), json!({ "v": 1 }))])
        .await
        .unwrap();
    assert!(
        ds.get("app", "dst", "k1").await.unwrap().is_none(),
        "disabled = inert"
    );

    store
        .storage
        .set_derived_enabled(&spec.id, true)
        .await
        .unwrap();
    ds.upsert_many("app", "src", &[("k2".to_string(), json!({ "v": 2 }))])
        .await
        .unwrap();
    assert!(
        ds.get("app", "dst", "k2").await.unwrap().is_some(),
        "re-enabled flows"
    );
}

#[tokio::test]
async fn derived_cycles_are_rejected_at_create_time() {
    let store = fresh_db("derived-cycle").await;
    make_spec(&store.storage, "a", "b", &[], &[], None).await;
    make_spec(&store.storage, "b", "c", &[], &[], None).await;

    async fn try_spec(
        storage: &Storage,
        source: &str,
        target: &str,
    ) -> pumper_core::Result<pumper_core::DerivedSpec> {
        let filters: Vec<String> = Vec::new();
        let project = BTreeMap::new();
        storage
            .create_derived_spec(&NewDerivedSpec {
                source_app: "app",
                source_dataset: source,
                target_dataset: target,
                filters: &filters,
                project: &project,
                lookup: None,
                group: None,
            })
            .await
    }
    // Self-loop, direct back-edge, transitive back-edge: all refused.
    assert!(matches!(
        try_spec(&store.storage, "x", "x").await,
        Err(pumper_core::Error::BadRequest(_))
    ));
    assert!(matches!(
        try_spec(&store.storage, "b", "a").await,
        Err(pumper_core::Error::BadRequest(_))
    ));
    assert!(matches!(
        try_spec(&store.storage, "c", "a").await,
        Err(pumper_core::Error::BadRequest(_))
    ));
    // Acyclic fan-out is fine.
    assert!(try_spec(&store.storage, "a", "d").await.is_ok());
}

#[tokio::test]
async fn backfill_materializes_existing_source_rows_in_bounded_batches() {
    let store = fresh_db("derived-backfill").await;
    let ds = store.datasets();
    // Rows exist BEFORE the spec: the live hook never saw them.
    let items: Vec<(String, serde_json::Value)> = (0..5)
        .map(|i| {
            (
                format!("k{i}"),
                json!({ "n": i, "state": if i % 2 == 0 { "CA" } else { "NY" } }),
            )
        })
        .collect();
    ds.upsert_many("app", "grants", &items).await.unwrap();

    let spec = make_spec(
        &store.storage,
        "grants",
        "ca_grants",
        &["$.state:eq:CA"],
        &[("n", "$.n")],
        None,
    )
    .await;

    // batch=2 forces multiple keyset pages over the 5 rows.
    let report = ds.backfill_derived(&spec, 2).await.unwrap();
    assert_eq!(report.scanned, 5);
    assert_eq!(report.matched, 3, "k0/k2/k4 pass the CA filter");
    assert_eq!(report.new, 3);
    for i in [0, 2, 4] {
        let rec = ds
            .get("app", "ca_grants", &format!("k{i}"))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("k{i} backfilled"));
        assert_eq!(rec.data, json!({ "n": i }));
    }
    assert!(ds.get("app", "ca_grants", "k1").await.unwrap().is_none());

    // Idempotent: a second backfill recomputes to all-unchanged.
    let again = ds.backfill_derived(&spec, 2).await.unwrap();
    assert_eq!(again.new, 0);
    assert_eq!(again.unchanged, 3);
}

// ── derived group_by + aggregates (M11 v2) ───────────────────────────────────

#[tokio::test]
async fn derived_group_counts_and_sums_track_add_change_remove() {
    let store = fresh_db("derived-group-basic").await;
    let ds = store.datasets();
    make_group_spec(
        &store.storage,
        "sales",
        "sales_by_state",
        &[],
        &["$.state"],
        &[("n", "count"), ("total", "sum($.amount)")],
    )
    .await
    .expect("create group spec");

    // Add: two CA rows, one NY row.
    ds.upsert_many(
        "app",
        "sales",
        &[
            ("s1".to_string(), json!({ "state": "CA", "amount": 10 })),
            ("s2".to_string(), json!({ "state": "CA", "amount": 5 })),
            ("s3".to_string(), json!({ "state": "NY", "amount": 7 })),
        ],
    )
    .await
    .unwrap();

    let ca = ds
        .get("app", "sales_by_state", "CA")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ca.data,
        json!({ "state": "CA", "stale": false, "n": 2, "total": 15 })
    );
    let ny = ds
        .get("app", "sales_by_state", "NY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ny.data["n"], json!(1));
    assert_eq!(ny.data["total"], json!(7));

    // Change: s2 MOVES CA -> NY. Both the old and the new group recompute.
    ds.upsert_many(
        "app",
        "sales",
        &[("s2".to_string(), json!({ "state": "NY", "amount": 5 }))],
    )
    .await
    .unwrap();
    let ca = ds
        .get("app", "sales_by_state", "CA")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ca.data["n"], json!(1), "CA lost the moved row");
    assert_eq!(ca.data["total"], json!(10));
    let ny = ds
        .get("app", "sales_by_state", "NY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ny.data["n"], json!(2), "NY gained the moved row");
    assert_eq!(ny.data["total"], json!(12));

    // Remove: a full snapshot without s3 tombstones it; NY shrinks exactly.
    let removed = ds
        .detect_removed(
            "app",
            "sales",
            &["s1".to_string(), "s2".to_string()],
            ok_guard(),
        )
        .await
        .unwrap();
    assert_eq!(removed, vec!["s3".to_string()]);
    let ny = ds
        .get("app", "sales_by_state", "NY")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ny.data["n"], json!(1));
    assert_eq!(ny.data["total"], json!(5));
}

#[tokio::test]
async fn derived_group_recompute_touches_only_affected_groups() {
    let store = fresh_db("derived-group-affected").await;
    let ds = store.datasets();
    make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();

    ds.upsert_many(
        "app",
        "sales",
        &[
            ("a".to_string(), json!({ "state": "CA" })),
            ("b".to_string(), json!({ "state": "NY" })),
        ],
    )
    .await
    .unwrap();

    // A new CA row must recompute CA only: NY's derived row gains no revision.
    ds.upsert_many(
        "app",
        "sales",
        &[("c".to_string(), json!({ "state": "CA" }))],
    )
    .await
    .unwrap();
    let ca = ds.get("app", "by_state", "CA").await.unwrap().unwrap();
    assert_eq!(ca.data["n"], json!(2));
    let ny_history = ds.history("app", "by_state", "NY", 10).await.unwrap();
    assert_eq!(
        ny_history.len(),
        1,
        "untouched group must not be recomputed (no spurious revisions)"
    );
    let ca_history = ds.history("app", "by_state", "CA", 10).await.unwrap();
    assert_eq!(ca_history.len(), 2, "affected group recomputed once more");
}

#[tokio::test]
async fn derived_group_oversized_group_goes_stale_never_wrong() {
    let store = fresh_db("derived-group-stale").await;
    // Any CA recompute may scan at most 2 rows; 3 CA rows exceed the bound.
    let ds = store.datasets().with_max_group_scan(2);
    let spec = make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count"), ("total", "sum($.amount)")],
    )
    .await
    .unwrap();

    ds.upsert_many(
        "app",
        "sales",
        &[
            ("a".to_string(), json!({ "state": "CA", "amount": 1 })),
            ("b".to_string(), json!({ "state": "CA", "amount": 2 })),
            ("c".to_string(), json!({ "state": "CA", "amount": 3 })),
            ("d".to_string(), json!({ "state": "NY", "amount": 9 })),
        ],
    )
    .await
    .unwrap();

    // Oversized group: stale marker, NO aggregate fields — absent, not wrong.
    let ca = ds.get("app", "by_state", "CA").await.unwrap().unwrap();
    assert_eq!(ca.data, json!({ "state": "CA", "stale": true }));
    // The small group is exact.
    let ny = ds.get("app", "by_state", "NY").await.unwrap().unwrap();
    assert_eq!(
        ny.data,
        json!({ "state": "NY", "stale": false, "n": 1, "total": 9 })
    );

    // Backfill pages the whole source, so it computes the oversized group
    // exactly and clears the flag.
    let report = ds.backfill_derived(&spec, 2).await.unwrap();
    assert_eq!(report.scanned, 4);
    assert_eq!(report.matched, 2, "two groups materialized");
    let ca = ds.get("app", "by_state", "CA").await.unwrap().unwrap();
    assert_eq!(
        ca.data,
        json!({ "state": "CA", "stale": false, "n": 3, "total": 6 })
    );
}

#[tokio::test]
async fn derived_group_backfill_covers_pre_existing_rows() {
    let store = fresh_db("derived-group-backfill").await;
    let ds = store.datasets();
    // Rows exist BEFORE the spec — the live hook never saw them — and the
    // spec's filters apply pre-aggregation.
    let items: Vec<(String, serde_json::Value)> = (0..5)
        .map(|i| {
            (
                format!("k{i}"),
                json!({ "amount": i, "kind": if i == 0 { "junk" } else { "sale" },
                        "state": if i % 2 == 0 { "CA" } else { "NY" } }),
            )
        })
        .collect();
    ds.upsert_many("app", "sales", &items).await.unwrap();

    let spec = make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &["$.kind:eq:sale"],
        &["$.state"],
        &[("n", "count"), ("total", "sum($.amount)")],
    )
    .await
    .unwrap();

    // batch=2 forces multiple keyset pages over the 5 rows.
    let report = ds.backfill_derived(&spec, 2).await.unwrap();
    assert_eq!(report.scanned, 5);
    assert_eq!(report.matched, 2);
    assert_eq!(report.new, 2);
    // k0 (junk) is filtered out: CA = k2+k4, NY = k1+k3.
    let ca = ds.get("app", "by_state", "CA").await.unwrap().unwrap();
    assert_eq!(
        ca.data,
        json!({ "state": "CA", "stale": false, "n": 2, "total": 6 })
    );
    let ny = ds.get("app", "by_state", "NY").await.unwrap().unwrap();
    assert_eq!(ny.data["n"], json!(2));
    assert_eq!(ny.data["total"], json!(4));

    // Idempotent: a second backfill recomputes to all-unchanged.
    let again = ds.backfill_derived(&spec, 2).await.unwrap();
    assert_eq!(again.new, 0);
    assert_eq!(again.unchanged, 2);
}

#[tokio::test]
async fn derived_group_validation_rejects_unsupported_shapes() {
    let store = fresh_db("derived-group-validation").await;
    let err = |r: pumper_core::Result<pumper_core::DerivedSpec>| {
        assert!(
            matches!(r, Err(pumper_core::Error::BadRequest(_))),
            "expected BadRequest"
        );
    };

    // Aggregates + lookup is not supported (a group row has no single source
    // record to resolve a key_expr against).
    let group = pumper_core::DerivedGroup {
        group_by: vec!["$.state".into()],
        aggregates: [("n".to_string(), "count".to_string())]
            .into_iter()
            .collect(),
    };
    let lookup = DerivedLookup {
        dataset: "other".into(),
        key_expr: "$.k".into(),
        merge_as: "extra".into(),
    };
    let filters: Vec<String> = Vec::new();
    let project = BTreeMap::new();
    err(store
        .storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: "app",
            source_dataset: "s",
            target_dataset: "t",
            filters: &filters,
            project: &project,
            lookup: Some(&lookup),
            group: Some(&group),
        })
        .await);

    // Aggregates + project is not supported (group rows are synthesized).
    let projected: BTreeMap<String, String> =
        [("x".to_string(), "$.x".to_string())].into_iter().collect();
    err(store
        .storage
        .create_derived_spec(&NewDerivedSpec {
            source_app: "app",
            source_dataset: "s",
            target_dataset: "t",
            filters: &filters,
            project: &projected,
            lookup: None,
            group: Some(&group),
        })
        .await);

    // Malformed group content: bad aggregate expr, non-$. path, empty halves,
    // and a collision with the reserved `stale` field.
    err(make_group_spec(
        &store.storage,
        "s",
        "t",
        &[],
        &["$.state"],
        &[("n", "avg($.x)")],
    )
    .await);
    err(make_group_spec(&store.storage, "s", "t", &[], &["state"], &[("n", "count")]).await);
    err(make_group_spec(&store.storage, "s", "t", &[], &[], &[("n", "count")]).await);
    err(make_group_spec(&store.storage, "s", "t", &[], &["$.state"], &[]).await);
    err(make_group_spec(
        &store.storage,
        "s",
        "t",
        &[],
        &["$.state"],
        &[("stale", "count")],
    )
    .await);

    // A well-formed aggregate spec is accepted and round-trips its group half.
    let spec = make_group_spec(
        &store.storage,
        "s",
        "t",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();
    let loaded = store
        .storage
        .get_derived_spec(&spec.id)
        .await
        .unwrap()
        .unwrap();
    let g = loaded.group.expect("group half round-trips");
    assert_eq!(g.group_by, vec!["$.state".to_string()]);
    assert!(loaded.lookup.is_none());
}

// ── provenance (M12) ─────────────────────────────────────────────────────────

#[tokio::test]
async fn provenance_stamps_round_trip_and_unstamped_writes_stay_honest_null() {
    use pumper_core::Provenance;
    let store = fresh_db("datasets-provenance").await;
    let ds = Datasets::new(store.storage.pool());

    // Fully stamped write.
    let prov = Provenance {
        job_id: Some("11111111-2222-3333-4444-555555555555".into()),
        source_url: Some("https://example.com/listing".into()),
        artifact_sha: Some("ab".repeat(32)),
        rules_hash: Some("cd".repeat(32)),
    };
    ds.upsert_stamped("app", "d", "k1", &json!({ "v": 1 }), None, Some(&prov))
        .await
        .unwrap();
    let rev = &ds.history("app", "d", "k1", 10).await.unwrap()[0];
    assert_eq!(rev.provenance.job_id, prov.job_id);
    assert_eq!(rev.provenance.source_url, prov.source_url);
    assert_eq!(rev.provenance.artifact_sha, prov.artifact_sha);
    assert_eq!(rev.provenance.rules_hash, prov.rules_hash);
    assert!(rev.provenance.replayable());

    // Legacy/unstamped write: every field NULL = unknown, nothing invented.
    ds.upsert("app", "d", "k2", &json!({ "v": 2 }))
        .await
        .unwrap();
    let rev = &ds.history("app", "d", "k2", 10).await.unwrap()[0];
    assert!(
        rev.provenance.is_empty(),
        "unstamped write must stamp nothing"
    );
    assert!(!rev.provenance.replayable());

    // Batch-level stamp lands on every revision of the batch.
    let batch_prov = Provenance {
        job_id: Some("job-batch".into()),
        rules_hash: Some("ef".repeat(32)),
        ..Default::default()
    };
    let items = vec![
        ("b1".to_string(), json!({ "v": 3 })),
        ("b2".to_string(), json!({ "v": 4 })),
    ];
    ds.upsert_many_stamped("app", "d", &items, None, Some(&batch_prov))
        .await
        .unwrap();
    for key in ["b1", "b2"] {
        let rev = &ds.history("app", "d", key, 10).await.unwrap()[0];
        assert_eq!(rev.provenance.job_id.as_deref(), Some("job-batch"));
        assert_eq!(rev.provenance.rules_hash, batch_prov.rules_hash);
        assert!(rev.provenance.source_url.is_none(), "unknown stays unknown");
        assert!(
            !rev.provenance.replayable(),
            "rules without a pinned artifact must not claim replayability"
        );
    }

    // Removal revisions carry no stamp (mirrors the no-trust-on-tombstone rule).
    ds.detect_removed(
        "app",
        "d",
        &["k1".into(), "k2".into(), "b1".into()],
        ok_guard(),
    )
    .await
    .unwrap();
    let rev = &ds.history("app", "d", "b2", 10).await.unwrap()[0];
    assert_eq!(rev.change, "removed");
    assert!(rev.provenance.is_empty());
}

#[tokio::test]
async fn rules_registry_is_content_addressed_and_idempotent() {
    let store = fresh_db("datasets-rules-registry").await;
    let ds = Datasets::new(store.storage.pool());

    let rules = json!({ "title": { "type": "css", "selector": "h1" } });
    let h1 = ds.register_rules(&rules).await.unwrap();
    assert_eq!(h1, pumper_core::datasets::rules_hash(&rules));
    // Idempotent: same rules, same hash, no error.
    assert_eq!(ds.register_rules(&rules).await.unwrap(), h1);
    // Round-trips byte-canonically.
    assert_eq!(ds.rules_by_hash(&h1).await.unwrap(), Some(rules.clone()));
    // Unknown hashes are None, not an error — stamped-but-unregistered is a
    // legitimate (refusable) state.
    assert_eq!(ds.rules_by_hash("nope").await.unwrap(), None);
    // Different rules hash apart.
    let other = json!({ "title": { "type": "css", "selector": "h2" } });
    assert_ne!(ds.register_rules(&other).await.unwrap(), h1);
}

#[tokio::test]
async fn provenance_coverage_counts_whole_chain() {
    use pumper_core::Provenance;
    let store = fresh_db("datasets-prov-coverage").await;
    let ds = Datasets::new(store.storage.pool());

    // rev 1: unstamped; rev 2: job only; rev 3: fully replayable.
    ds.upsert("app", "d", "k", &json!({ "v": 1 }))
        .await
        .unwrap();
    let job_only = Provenance {
        job_id: Some("j".into()),
        ..Default::default()
    };
    ds.upsert_stamped("app", "d", "k", &json!({ "v": 2 }), None, Some(&job_only))
        .await
        .unwrap();
    let full = Provenance {
        job_id: Some("j".into()),
        source_url: Some("https://x".into()),
        artifact_sha: Some("aa".into()),
        rules_hash: Some("bb".into()),
    };
    ds.upsert_stamped("app", "d", "k", &json!({ "v": 3 }), None, Some(&full))
        .await
        .unwrap();

    let (total, with_job, replayable) = ds.provenance_coverage("app", "d", "k").await.unwrap();
    assert_eq!((total, with_job, replayable), (3, 2, 1));
}

/// `list_records_view` is the one function the `/datasets/{app}/{ds}` read
/// surface (default, cursor, filtered, export) now shares. Trust and tombstone
/// inclusion are independent toggles — this pins that they don't leak into
/// each other: a `stable`-only, tombstone-excluding read must not return a
/// provisional row or a removed row, and `include_removed` must not silently
/// widen the trust filter either.
#[tokio::test]
async fn list_records_view_trust_and_removed_are_independent_toggles() {
    let store = fresh_db("datasets-list-records-view").await;
    let ds = Datasets::new(store.storage.pool());

    ds.upsert_trusted("app", "d", "stable-live", &json!({"v": 1}), None)
        .await
        .unwrap();
    ds.upsert_trusted(
        "app",
        "d",
        "provisional-live",
        &json!({"v": 2}),
        Some("provisional"),
    )
    .await
    .unwrap();
    ds.upsert_trusted("app", "d", "stable-removed", &json!({"v": 3}), None)
        .await
        .unwrap();
    ds.tombstone_keys("app", "d", &["stable-removed".to_string()])
        .await
        .unwrap();

    // Default view: stable trust only, tombstones excluded.
    let live_stable = ds
        .list_records_view("app", "d", &[], None, 10, Some("stable"), false)
        .await
        .unwrap();
    let keys: Vec<&str> = live_stable.iter().map(|r| r.key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["stable-live"],
        "trust=stable, removed=exclude must show neither the provisional nor the tombstoned row"
    );

    // include_removed=true widens tombstones back in, but must not touch trust.
    let with_removed = ds
        .list_records_view("app", "d", &[], None, 10, Some("stable"), true)
        .await
        .unwrap();
    let mut keys: Vec<&str> = with_removed.iter().map(|r| r.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["stable-live", "stable-removed"],
        "removed=include must surface the tombstone without pulling in the provisional row"
    );

    // trust=None (all) with tombstones excluded must not surface the removed row.
    let all_trust_live = ds
        .list_records_view("app", "d", &[], None, 10, None, false)
        .await
        .unwrap();
    let mut keys: Vec<&str> = all_trust_live.iter().map(|r| r.key.as_str()).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["provisional-live", "stable-live"],
        "trust=all must include the provisional row but removed=exclude still hides the tombstone"
    );
}

/// `history_page`'s keyset predicate leads on `created_at` (`created_at < ? OR
/// (created_at = ? AND revision < ?)`), so the ORDER BY must lead on
/// `created_at` too — otherwise a page cut by `revision` disagrees with a
/// predicate that excludes by `created_at` first, and a revision whose
/// timestamp is out of step with its revision number (clock skew, a backdating
/// import) gets skipped or repeated across the page boundary. This writes 5
/// revisions for one key, then deliberately scrambles their `created_at` so it
/// does NOT move in step with `revision`, and walks the history one row per
/// page — proving every revision appears exactly once regardless.
#[tokio::test]
async fn history_page_survives_clock_skew_without_skip_or_repeat() {
    let store = fresh_db("datasets-history-skew").await;
    let ds = Datasets::new(store.storage.pool());

    for i in 1..=5 {
        ds.upsert("app", "d", "k", &json!({ "v": i }))
            .await
            .unwrap();
    }

    // Scramble created_at so it is NOT monotonic with revision — revision 3
    // (the middle write) is stamped the EARLIEST, revision 1 the LATEST,
    // deliberately inverting the naive "revision order == time order"
    // assumption a bare `ORDER BY revision DESC` would rely on.
    let base = chrono::Utc::now();
    let skewed = [
        (1i64, base + chrono::Duration::seconds(50)),
        (2, base + chrono::Duration::seconds(10)),
        (3, base), // earliest
        (4, base + chrono::Duration::seconds(40)),
        (5, base + chrono::Duration::seconds(30)),
    ];
    for (revision, created_at) in skewed {
        ds.set_revision_created_at_for_test("app", "d", "k", revision, created_at)
            .await
            .unwrap();
    }

    // Page one row at a time and collect every revision number seen.
    let mut seen: Vec<i64> = Vec::new();
    let mut after = None;
    loop {
        let page = ds.history_page("app", "d", "k", after, 1).await.unwrap();
        seen.extend(page.items.iter().map(|r| r.revision));
        match page.next_cursor.as_deref() {
            Some(cursor) => {
                let (t, r) = cursor.split_once('|').expect("cursor shape ts|revision");
                after = Some((t.to_string(), r.parse().unwrap()));
            }
            None => break,
        }
        assert!(seen.len() <= 5, "paged past the known row count: {seen:?}");
    }

    let mut dedup = seen.clone();
    dedup.sort_unstable();
    dedup.dedup();
    assert_eq!(
        dedup,
        vec![1, 2, 3, 4, 5],
        "every skewed revision appears exactly once across page boundaries: {seen:?}"
    );
}

// ── derived: trust inheritance + provenance ──────────────────────────────────

/// The laundering hole: derived rows used to be written with `trust = None`,
/// and NULL trust *means* `stable` — so a provisional source produced a
/// stable-looking derived row, and `?trust=stable` served it as something we
/// stand behind.
#[tokio::test]
async fn provisional_source_derives_a_provisional_row_not_a_stable_one() {
    let store = fresh_db("derived-trust-source").await;
    let ds = store.datasets();
    make_spec(&store.storage, "src", "tgt", &[], &[("n", "$.n")], None).await;

    ds.upsert_many_trusted(
        "app",
        "src",
        &[("k1".to_string(), json!({ "n": 1 }))],
        Some("provisional"),
    )
    .await
    .unwrap();

    let derived = ds.get("app", "tgt", "k1").await.unwrap().unwrap();
    assert_eq!(derived.data, json!({ "n": 1 }));
    assert_eq!(
        derived.trust, "provisional",
        "a derived row may not be more trusted than the row it came from"
    );
    // And the revision carries the same stamp, so the era stays identifiable.
    let rev = ds.history("app", "tgt", "k1", 1).await.unwrap();
    assert_eq!(rev[0].trust, "provisional");

    // A stable source stays stable — the floor is inherited, not invented.
    ds.upsert_many("app", "src", &[("k2".to_string(), json!({ "n": 2 }))])
        .await
        .unwrap();
    assert_eq!(
        ds.get("app", "tgt", "k2").await.unwrap().unwrap().trust,
        "stable"
    );
}

/// A join is an input too: the derived row is as weak as the weakest side, so a
/// stable source joined to a quarantined lookup row is quarantined.
#[tokio::test]
async fn lookup_join_drags_the_derived_row_down_to_the_joined_trust() {
    let store = fresh_db("derived-trust-lookup").await;
    let ds = store.datasets();
    make_spec(
        &store.storage,
        "src",
        "tgt",
        &[],
        &[("n", "$.n")],
        Some(DerivedLookup {
            dataset: "meta".into(),
            key_expr: "$.meta".into(),
            merge_as: "meta".into(),
        }),
    )
    .await;

    ds.upsert_many_trusted(
        "app",
        "meta",
        &[("m1".to_string(), json!({ "label": "L" }))],
        Some("quarantined"),
    )
    .await
    .unwrap();
    // The source write is fully stable.
    ds.upsert_many(
        "app",
        "src",
        &[("k1".to_string(), json!({ "n": 1, "meta": "m1" }))],
    )
    .await
    .unwrap();

    let derived = ds.get("app", "tgt", "k1").await.unwrap().unwrap();
    assert_eq!(
        derived.data["meta"],
        json!({ "label": "L" }),
        "join happened"
    );
    assert_eq!(
        derived.trust, "quarantined",
        "the joined side's trust must not be dropped on the floor"
    );
}

/// Aggregates are a claim about a whole group, so one weak member makes the
/// number weak — and the untouched group stays stable.
#[tokio::test]
async fn group_row_inherits_the_weakest_member_not_the_last_write() {
    let store = fresh_db("derived-trust-group").await;
    let ds = store.datasets();
    make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();

    ds.upsert_many(
        "app",
        "sales",
        &[
            ("a".to_string(), json!({ "state": "CA" })),
            ("b".to_string(), json!({ "state": "NY" })),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        ds.get("app", "by_state", "CA")
            .await
            .unwrap()
            .unwrap()
            .trust,
        "stable"
    );

    // One provisional CA member arrives; the CA aggregate is now provisional
    // even though the *last* write to the group was the stable one before it.
    ds.upsert_many_trusted(
        "app",
        "sales",
        &[("c".to_string(), json!({ "state": "CA" }))],
        Some("provisional"),
    )
    .await
    .unwrap();
    let ca = ds.get("app", "by_state", "CA").await.unwrap().unwrap();
    assert_eq!(ca.data["n"], json!(2));
    assert_eq!(ca.trust, "provisional");
    assert_eq!(
        ds.get("app", "by_state", "NY")
            .await
            .unwrap()
            .unwrap()
            .trust,
        "stable",
        "an untouched group keeps its own trust"
    );
}

/// The backfill is the same derivation by another door: it must inherit trust
/// exactly like the live recompute, or "re-run the backfill" would be a way to
/// launder a provisional corpus into stable derived rows.
#[tokio::test]
async fn backfill_inherits_trust_like_the_live_path() {
    let store = fresh_db("derived-trust-backfill").await;
    let ds = store.datasets();
    // Rows exist BEFORE the spec, written provisional.
    ds.upsert_many_trusted(
        "app",
        "src",
        &[
            ("k1".to_string(), json!({ "n": 1, "state": "CA" })),
            ("k2".to_string(), json!({ "n": 2, "state": "CA" })),
        ],
        Some("provisional"),
    )
    .await
    .unwrap();
    ds.upsert_many(
        "app",
        "src",
        &[("k3".to_string(), json!({ "n": 3, "state": "NY" }))],
    )
    .await
    .unwrap();

    let row_spec = make_spec(&store.storage, "src", "tgt", &[], &[("n", "$.n")], None).await;
    let report = ds.backfill_derived(&row_spec, 2).await.unwrap();
    assert_eq!(report.matched, 3);
    assert_eq!(
        ds.get("app", "tgt", "k1").await.unwrap().unwrap().trust,
        "provisional"
    );
    assert_eq!(
        ds.get("app", "tgt", "k3").await.unwrap().unwrap().trust,
        "stable"
    );

    let group_spec = make_group_spec(
        &store.storage,
        "src",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();
    ds.backfill_derived(&group_spec, 2).await.unwrap();
    assert_eq!(
        ds.get("app", "by_state", "CA")
            .await
            .unwrap()
            .unwrap()
            .trust,
        "provisional",
        "group backfill accumulates the weakest member trust"
    );
    assert_eq!(
        ds.get("app", "by_state", "NY")
            .await
            .unwrap()
            .unwrap()
            .trust,
        "stable"
    );
}

/// Derived revisions used to carry NO provenance at all: nothing said which
/// spec shaped the row or which run fed it. They now stamp the registered spec
/// fingerprint (`rules_hash`, the 0030 idiom) and inherit the source write's
/// job — and still refuse to claim replayability they don't have.
#[tokio::test]
async fn derived_revisions_stamp_the_spec_and_inherit_the_source_job() {
    let store = fresh_db("derived-provenance").await;
    let ds = store.datasets();
    let spec = make_spec(&store.storage, "src", "tgt", &[], &[("n", "$.n")], None).await;

    ds.upsert_many_stamped(
        "app",
        "src",
        &[("k1".to_string(), json!({ "n": 1 }))],
        None,
        Some(&pumper_core::Provenance {
            job_id: Some("job-7".into()),
            source_url: Some("https://example.test/list".into()),
            artifact_sha: Some("deadbeef".into()),
            rules_hash: None,
        }),
    )
    .await
    .unwrap();

    let expected = pumper_core::datasets::rules_hash(&pumper_core::derived_spec_fingerprint(&spec));
    let rev = ds.history("app", "tgt", "k1", 1).await.unwrap();
    let prov = &rev[0].provenance;
    assert_eq!(prov.rules_hash.as_deref(), Some(expected.as_str()));
    assert_eq!(
        prov.job_id.as_deref(),
        Some("job-7"),
        "source job is lineage"
    );
    assert!(
        prov.source_url.is_none() && prov.artifact_sha.is_none(),
        "a derived row was not fetched and has no archived body — never borrow the source's"
    );
    assert!(!prov.replayable(), "no artifact means never a replay claim");
    // The fingerprint is registered, so the doctor's unregistered-hash finding
    // stays quiet and the derivation is inspectable after the fact.
    let registered = ds.rules_by_hash(&expected).await.unwrap().unwrap();
    assert_eq!(registered["kind"], json!("derived_spec"));
    assert_eq!(registered["id"], json!(spec.id));
}

/// The silent-degradation path: an unparseable `lookup` column used to parse as
/// `(None, None)`, turning a lookup/aggregate spec into a whole-record
/// PASSTHROUGH that kept writing wrong-shaped rows. It must be skipped loudly
/// instead — nothing written, nothing pretended.
#[tokio::test]
async fn corrupt_lookup_column_skips_the_spec_not_writes_a_passthrough() {
    let store = fresh_db("derived-corrupt-join").await;
    let ds = store.datasets();
    let pool = store.storage.pool();
    let spec = make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();

    sqlx::query(r#"UPDATE derived SET lookup = '{"group_by": ' WHERE id = ?1"#)
        .bind(&spec.id)
        .execute(&pool)
        .await
        .unwrap();

    ds.upsert_many(
        "app",
        "sales",
        &[("a".to_string(), json!({ "state": "CA", "amount": 3 }))],
    )
    .await
    .unwrap();

    assert!(
        ds.list("app", "by_state", 10).await.unwrap().is_empty(),
        "a spec we cannot read must write NOTHING — not a passthrough copy of the source row"
    );
    // Loud on every surface that touches it: skipped in the run set and in the
    // listing, and a hard error when asked for by id.
    assert!(store
        .storage
        .list_derived_specs(Some("app"))
        .await
        .unwrap()
        .is_empty());
    assert!(store.storage.get_derived_spec(&spec.id).await.is_err());
}

// ── derived: backfill budget + resume ────────────────────────────────────────

/// The backfill loops the ENTIRE source synchronously inside one HTTP request.
/// On a large corpus that is a request that never returns — and a client that
/// gives up restarts from zero. It must stop at the budget and hand back a
/// cursor instead of running to completion.
#[tokio::test]
async fn oversized_backfill_returns_a_cursor_not_a_full_pass() {
    let store = fresh_db("derived-backfill-budget").await;
    let ds = store.datasets();
    let items: Vec<(String, serde_json::Value)> = (0..25)
        .map(|i| (format!("k{i:02}"), json!({ "n": i })))
        .collect();
    ds.upsert_many("app", "src", &items).await.unwrap();
    let spec = make_spec(&store.storage, "src", "tgt", &[], &[("n", "$.n")], None).await;

    let first = ds
        .backfill_derived_budgeted(
            &spec,
            &pumper_core::BackfillOpts {
                batch: 5,
                max_rows: 10,
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.scanned, 10, "the budget is respected, not exceeded");
    assert!(!first.done, "25 rows do not fit in a 10-row budget");
    let cursor = first
        .cursor
        .clone()
        .expect("an unfinished pass hands back a cursor");
    assert_eq!(
        ds.list("app", "tgt", 100).await.unwrap().len(),
        10,
        "the slice it did scan is fully materialized"
    );

    // Resume: each call continues where the last stopped, no gap and no repeat.
    let mut scanned = first.scanned;
    let mut cursor = Some(cursor);
    let mut guard = 0;
    loop {
        let next = ds
            .backfill_derived_budgeted(
                &spec,
                &pumper_core::BackfillOpts {
                    batch: 5,
                    max_rows: 10,
                    cursor: cursor.clone(),
                },
            )
            .await
            .unwrap();
        scanned += next.scanned;
        cursor = next.cursor.clone();
        guard += 1;
        assert!(guard < 10, "resume must terminate");
        if next.done {
            assert!(next.cursor.is_none(), "a finished pass has no cursor");
            break;
        }
    }
    assert_eq!(scanned, 25, "every source row was scanned exactly once");
    assert_eq!(ds.list("app", "tgt", 100).await.unwrap().len(), 25);

    // Idempotent: a re-run from scratch recomputes to all-unchanged.
    let again = ds.backfill_derived(&spec, 5).await.unwrap();
    assert!(again.done);
    assert_eq!(again.new, 0);
    assert_eq!(again.unchanged, 25);
}

/// A group's members are spread across the whole scan order, so a partial pass
/// would publish partial totals. The budget is therefore a ceiling for
/// aggregate specs: refuse loudly, write nothing.
#[tokio::test]
async fn group_backfill_refuses_a_partial_pass_instead_of_writing_partial_totals() {
    let store = fresh_db("derived-backfill-group-budget").await;
    let ds = store.datasets();
    let items: Vec<(String, serde_json::Value)> = (0..20)
        .map(|i| {
            (
                format!("k{i:02}"),
                json!({ "state": if i % 2 == 0 { "CA" } else { "NY" }, "amount": 1 }),
            )
        })
        .collect();
    ds.upsert_many("app", "sales", &items).await.unwrap();
    let spec = make_group_spec(
        &store.storage,
        "sales",
        "by_state",
        &[],
        &["$.state"],
        &[("n", "count")],
    )
    .await
    .unwrap();
    // Wipe what the live hook wrote, so anything present afterwards came from
    // the refused backfill.
    ds.delete_dataset("app", "by_state").await.unwrap();

    let err = ds
        .backfill_derived_budgeted(
            &spec,
            &pumper_core::BackfillOpts {
                batch: 5,
                max_rows: 10,
                cursor: None,
            },
        )
        .await;
    assert!(
        matches!(err, Err(pumper_core::Error::BadRequest(_))),
        "an aggregate backfill over the ceiling must fail, not truncate"
    );
    assert!(
        ds.list("app", "by_state", 10).await.unwrap().is_empty(),
        "a refused aggregate pass writes NOTHING — never a partial total"
    );

    // With room for the whole corpus it completes exactly.
    let ok = ds
        .backfill_derived_budgeted(
            &spec,
            &pumper_core::BackfillOpts {
                batch: 5,
                max_rows: 1000,
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert!(ok.done);
    assert_eq!(
        ds.get("app", "by_state", "CA").await.unwrap().unwrap().data["n"],
        json!(10)
    );
}

/// A cursor is `updated_at|key`, and a key may itself contain a `|` — splitting
/// on the LAST separator (or on every one) would corrupt the resume point.
#[tokio::test]
async fn backfill_cursor_round_trips_a_key_containing_a_pipe() {
    let store = fresh_db("derived-backfill-cursor").await;
    let ds = store.datasets();
    ds.upsert_many(
        "app",
        "src",
        &[("czisco|kraj|org".to_string(), json!({ "n": 1 }))],
    )
    .await
    .unwrap();
    let rec = ds
        .get("app", "src", "czisco|kraj|org")
        .await
        .unwrap()
        .unwrap();
    let cursor = pumper_core::backfill_cursor(&rec);
    let (ts, key) = pumper_core::parse_backfill_cursor(&cursor).unwrap();
    assert_eq!(key, "czisco|kraj|org");
    assert_eq!(ts, pumper_core::datasets::ts(rec.updated_at));
    // Blank / separator-less cursors page from the top rather than failing.
    assert_eq!(pumper_core::parse_backfill_cursor("  "), None);
    assert_eq!(pumper_core::parse_backfill_cursor("garbage"), None);
}

/// The lookup join used to be one point query PER SOURCE RECORD. Batching it
/// must not change a single derived row — same merges, same misses, same
/// treatment of a tombstoned lookup row.
#[tokio::test]
async fn batched_lookup_join_matches_the_per_record_join() {
    let store = fresh_db("derived-lookup-batched").await;
    let ds = store.datasets();
    ds.upsert_many(
        "app",
        "meta",
        &[
            ("m1".to_string(), json!({ "label": "one" })),
            ("m2".to_string(), json!({ "label": "two" })),
            ("gone".to_string(), json!({ "label": "removed" })),
        ],
    )
    .await
    .unwrap();
    // `gone` is tombstoned: a removed lookup row merges nothing.
    ds.tombstone_keys("app", "meta", &["gone".to_string()])
        .await
        .unwrap();

    make_spec(
        &store.storage,
        "src",
        "tgt",
        &[],
        &[("n", "$.n")],
        Some(DerivedLookup {
            dataset: "meta".into(),
            key_expr: "$.meta".into(),
            merge_as: "meta".into(),
        }),
    )
    .await;

    ds.upsert_many(
        "app",
        "src",
        &[
            // Two rows sharing one lookup key (deduped into one join read),
            ("a".to_string(), json!({ "n": 1, "meta": "m1" })),
            ("b".to_string(), json!({ "n": 2, "meta": "m1" })),
            ("c".to_string(), json!({ "n": 3, "meta": "m2" })),
            // a key with no lookup row, a tombstoned one, and no key at all.
            ("d".to_string(), json!({ "n": 4, "meta": "missing" })),
            ("e".to_string(), json!({ "n": 5, "meta": "gone" })),
            ("f".to_string(), json!({ "n": 6 })),
        ],
    )
    .await
    .unwrap();

    let got = |k: &str| {
        let ds = &ds;
        let k = k.to_string();
        async move { ds.get("app", "tgt", &k).await.unwrap().unwrap().data }
    };
    assert_eq!(got("a").await["meta"], json!({ "label": "one" }));
    assert_eq!(got("b").await["meta"], json!({ "label": "one" }));
    assert_eq!(got("c").await["meta"], json!({ "label": "two" }));
    for k in ["d", "e", "f"] {
        assert!(
            got(k).await.get("meta").is_none(),
            "{k}: a missing/tombstoned lookup row merges nothing"
        );
    }
}

/// The anti-pattern: one `list_all_datasets()` serving both "what can I serve"
/// and "what must I clean up". Its SQL is `WHERE removed_at IS NULL`, so a
/// dataset whose every record is tombstoned vanished from it — and that dataset
/// is exactly the one `search-backfill --all` exists to purge ghosts from. The
/// live-only view is still correct for the watch registry and the DataHub poll,
/// so the fix is a second, explicit method rather than a changed contract.
#[tokio::test]
async fn a_fully_tombstoned_dataset_is_not_invisible_to_the_full_listing() {
    let store = fresh_db("datasets-list-all-tombstoned").await;
    let ds = Datasets::new(store.storage.pool());

    ds.upsert("retired", "old", "a", &json!({ "t": 1 }))
        .await
        .unwrap();
    ds.upsert("grants", "unified", "x", &json!({ "t": 2 }))
        .await
        .unwrap();

    // Tombstone every record of `retired/old` — a non-empty snapshot that names
    // none of its keys is what the removal path acts on.
    ds.detect_removed("retired", "old", &["__absent__".to_string()], ok_guard())
        .await
        .unwrap();
    assert!(
        ds.list("retired", "old", 10)
            .await
            .unwrap()
            .iter()
            .all(|r| r.removed_at.is_some()),
        "precondition: the dataset must be fully tombstoned"
    );

    let live = ds.list_all_datasets().await.unwrap();
    assert_eq!(
        live,
        vec![("grants".to_string(), "unified".to_string())],
        "list_all_datasets keeps its live-only contract for the watch registry \
         and the DataHub poll"
    );

    let all = ds.list_all_datasets_including_removed().await.unwrap();
    assert_eq!(
        all,
        vec![
            ("grants".to_string(), "unified".to_string()),
            ("retired".to_string(), "old".to_string()),
        ],
        "a fully tombstoned dataset must still be reachable for cleanup"
    );
}
