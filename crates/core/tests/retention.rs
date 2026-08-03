//! Provenance-aware retention, against a real SQLite + a real artifact tree.
//!
//! The pure planning rules are unit-tested inside `core::retention`. What needs a
//! database is the other half of the pinning rule: that the **veto list actually
//! comes out of the provenance graph**, and that the ledger prunes spare the rows
//! something is still using.

use std::collections::HashSet;

use pumper_core::retention::{
    delete_artifacts, plan_artifact_retention, scan_artifact_tree, ArtifactRef, CASSETTE_FILE,
};
use pumper_core::storage::{LedgerPruned, LedgerRetention};
use pumper_core::testing::TempStore;
use pumper_core::Provenance;
use serde_json::json;

/// A body archived the way the crawl archives one, plus the record fields that
/// let `rederive` find it again.
fn record(job_id: &str, artifact: &str) -> serde_json::Value {
    json!({ "job_id": job_id, "artifact_path": artifact, "url": "https://example.test/a" })
}

fn replayable(job_id: &str) -> Provenance {
    Provenance {
        job_id: Some(job_id.to_string()),
        source_url: Some("https://example.test/a".into()),
        artifact_sha: Some("a".repeat(64)),
        rules_hash: Some("b".repeat(64)),
    }
}

/// Stamped, but not replayable: `rederive` refuses these, so they pin nothing.
fn stamped_only(job_id: &str) -> Provenance {
    Provenance {
        job_id: Some(job_id.to_string()),
        source_url: Some("https://example.test/a".into()),
        artifact_sha: None,
        rules_hash: None,
    }
}

fn write_body(store: &TempStore, app: &str, job: &str, name: &str, bytes: &[u8]) {
    let dir = store.storage.artifacts_dir.join(app).join(job);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(name), bytes).unwrap();
}

/// A cutoff in the future, so *age* would condemn every body on disk. Anything
/// that survives, survives because it was pinned — never because it was young.
fn everything_is_old() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::days(1)
}

/// **The direction's central guarantee.** A body a replayable revision points at
/// survives past the age cutoff; an identically-aged body whose revision is
/// merely *stamped* (no `artifact_sha`/`rules_hash`, so `rederive` would refuse
/// it anyway) does not.
///
/// If the pin is removed from `pinned_artifact_refs` or from `keep_reason`, this
/// test fails — which is the whole point: `POST /provenance/.../rederive` would
/// otherwise start answering "archived body unavailable" for records the store
/// still advertises as reproducible.
#[tokio::test]
async fn a_body_pinned_by_a_replayable_revision_survives_the_age_cutoff() {
    let store = TempStore::new("retention-pin").await;
    let ds = store.datasets();

    ds.upsert_stamped(
        "crawl",
        "pages",
        "pinned",
        &record("job-pinned", "page.html"),
        None,
        Some(&replayable("job-pinned")),
    )
    .await
    .unwrap();
    ds.upsert_stamped(
        "crawl",
        "pages",
        "loose",
        &record("job-loose", "page.html"),
        None,
        Some(&stamped_only("job-loose")),
    )
    .await
    .unwrap();

    write_body(&store, "crawl", "job-pinned", "page.html", b"pinned body");
    write_body(&store, "crawl", "job-loose", "page.html", b"loose body");

    let pinned = ds.pinned_artifact_refs().await.unwrap();
    assert!(pinned.contains(&ArtifactRef {
        app: "crawl".into(),
        job_id: "job-pinned".into(),
        name: "page.html".into(),
    }));
    assert_eq!(
        pinned.len(),
        1,
        "only the replayable revision pins: {pinned:?}"
    );

    let files = scan_artifact_tree(&store.storage.artifacts_dir);
    let plan = plan_artifact_retention(&files, &pinned, everything_is_old(), true);
    assert_eq!(plan.delete.len(), 1);
    assert_eq!(plan.delete[0].job_id, "job-loose");

    let (n, _) = delete_artifacts(&store.storage.artifacts_dir, &files, &plan);
    assert_eq!(n, 1);
    assert!(store
        .storage
        .artifacts_dir
        .join("crawl")
        .join("job-pinned")
        .join("page.html")
        .exists());
    assert!(!store
        .storage
        .artifacts_dir
        .join("crawl")
        .join("job-loose")
        .join("page.html")
        .exists());
}

/// A crawl revisit writes a NEW `job_id` copy and abandons the old one. Both the
/// historical snapshot and the record's current location must be pinned: the old
/// body is what the old revision claims to reproduce, and the new body is where
/// `rederive` looks today. Pinning only one of the two would break one of them.
#[tokio::test]
async fn a_revisit_pins_both_the_old_snapshot_and_the_current_body() {
    let store = TempStore::new("retention-revisit").await;
    let ds = store.datasets();
    for job in ["job-v1", "job-v2"] {
        ds.upsert_stamped(
            "crawl",
            "pages",
            "k",
            &record(job, "page.html"),
            None,
            Some(&replayable(job)),
        )
        .await
        .unwrap();
    }
    let pinned = ds.pinned_artifact_refs().await.unwrap();
    let jobs: HashSet<String> = pinned.iter().map(|r| r.job_id.clone()).collect();
    assert_eq!(
        jobs,
        ["job-v1".to_string(), "job-v2".to_string()]
            .into_iter()
            .collect::<HashSet<_>>()
    );
}

/// Nothing is pinned by a record with no provenance at all — otherwise retention
/// could never reclaim anything on a store full of legacy writes, and the knob
/// would be decorative.
#[tokio::test]
async fn unstamped_records_pin_nothing() {
    let store = TempStore::new("retention-unstamped").await;
    let ds = store.datasets();
    ds.upsert("crawl", "pages", "k", &record("job-a", "page.html"))
        .await
        .unwrap();
    assert!(ds.pinned_artifact_refs().await.unwrap().is_empty());
}

/// Cassettes are protected by default even though no revision ever points at
/// one: `Vcr::Replay` reads them and a miss is a hard error.
#[tokio::test]
async fn a_cassette_is_kept_although_no_revision_pins_it() {
    let store = TempStore::new("retention-cassette").await;
    write_body(&store, "research", "job-a", CASSETTE_FILE, b"{}\n");
    let files = scan_artifact_tree(&store.storage.artifacts_dir);
    let plan = plan_artifact_retention(&files, &HashSet::new(), everything_is_old(), true);
    assert!(plan.delete.is_empty(), "{:?}", plan.delete);

    let opted_in = plan_artifact_retention(&files, &HashSet::new(), everything_is_old(), false);
    assert_eq!(opted_in.delete.len(), 1);
}

/// An unconfigured deployment must be a true no-op: every knob at 0 deletes
/// nothing, whatever is in the tables.
#[tokio::test]
async fn ledger_retention_off_by_default_is_a_no_op() {
    let store = TempStore::new("retention-ledger-off").await;
    let off = LedgerRetention::default();
    assert!(!off.any_enabled());
    assert_eq!(
        store.storage.prune_ledgers(&off).await.unwrap(),
        LedgerPruned::default()
    );
}

/// A running job's spend backs its budget ceiling. Pruning its cost events would
/// silently restore budget the operator never granted, so the prune is scoped to
/// jobs that already reached a terminal state.
#[tokio::test]
async fn prune_cost_events_spares_a_running_jobs_events() {
    let store = TempStore::new("retention-costs").await;
    let pool = store.storage.pool();
    let old = "2000-01-01T00:00:00.000000Z";

    for (id, status) in [("job-live", "running"), ("job-done", "succeeded")] {
        sqlx::query(
            "INSERT INTO jobs (id, app, params, status, attempts, max_attempts, \
             created_at, available_at) VALUES (?1, 'a', '{}', ?2, 0, 1, ?3, ?3)",
        )
        .bind(id)
        .bind(status)
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO cost_events (job_id, app, engine, cost_usd, created_at) \
             VALUES (?1, 'a', 'claude', 1.5, ?2)",
        )
        .bind(id)
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
    }

    let pruned = store
        .storage
        .prune_ledgers(&LedgerRetention {
            cost_event_days: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(pruned.cost_events, 1, "only the terminal job's event goes");

    let live: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM cost_events WHERE job_id = 'job-live'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(live, 1, "a running job's spend history must survive");
}

/// `pending` and `failed` deliveries are the live retry queue and the replayable
/// dead-letter queue. Retention bounds the terminal states only.
#[tokio::test]
async fn prune_deliveries_spares_the_pending_and_failed_retry_queue() {
    let store = TempStore::new("retention-deliveries").await;
    let pool = store.storage.pool();
    let old = "2000-01-01T00:00:00.000000Z";
    for status in ["delivered", "dead", "failed", "pending"] {
        sqlx::query(
            "INSERT INTO webhook_deliveries (id, kind, ref_id, url, event, body, status, \
             attempts, created_at, updated_at) \
             VALUES (?1, 'job', 'r', 'https://x.test', 'e', '{}', ?1, 0, ?2, ?2)",
        )
        .bind(status)
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Delivered only: the dead-letter tail has its own knob.
    let pruned = store
        .storage
        .prune_ledgers(&LedgerRetention {
            delivered_webhook_days: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(pruned.webhook_deliveries, 1);

    let left: Vec<String> =
        sqlx::query_scalar("SELECT status FROM webhook_deliveries ORDER BY status")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(left, vec!["dead", "failed", "pending"]);

    // Opting the DLQ tail in removes `dead` and still spares the live queue.
    store
        .storage
        .prune_ledgers(&LedgerRetention {
            dead_webhook_days: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let left: Vec<String> =
        sqlx::query_scalar("SELECT status FROM webhook_deliveries ORDER BY status")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(left, vec!["failed", "pending"]);
}

/// The remaining two ledgers are plain age windows, and the row counts the store
/// report reads must move with them.
#[tokio::test]
async fn job_yield_and_seen_ids_are_bounded_by_age() {
    let store = TempStore::new("retention-yield").await;
    let pool = store.storage.pool();
    let old = "2000-01-01T00:00:00.000000Z";
    sqlx::query(
        "INSERT INTO job_yield (job_id, app, dataset, new_count, created_at) \
         VALUES ('j', 'a', '', 1, ?1)",
    )
    .bind(old)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO saved_search_seen (search_id, doc_id, created_at) VALUES ('s', 'd', ?1)",
    )
    .bind(old)
    .execute(&pool)
    .await
    .unwrap();

    let before = store.storage.ledger_row_counts().await.unwrap();
    assert!(before.iter().any(|(t, n)| t == "job_yield" && *n == 1));

    let pruned = store
        .storage
        .prune_ledgers(&LedgerRetention {
            job_yield_days: 1,
            saved_search_seen_days: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!((pruned.job_yield, pruned.saved_search_seen), (1, 1));

    let after = store.storage.ledger_row_counts().await.unwrap();
    assert!(after.iter().all(|(_, n)| *n == 0), "{after:?}");
}
