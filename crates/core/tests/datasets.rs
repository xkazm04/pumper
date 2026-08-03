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
