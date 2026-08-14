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

/// **The direction's central guarantee, on the arm that answers to `rederive`.**
/// A **historical** body — one no live record addresses any more, because the
/// key has since been revisited — survives past the age cutoff when its revision
/// is replayable, and does not when that revision is merely *stamped* (no
/// `artifact_sha`/`rules_hash`, so `rederive` would refuse it anyway).
///
/// Both keys are revisited so the current location is out of the picture: since
/// round 24 a live record pins its own body unconditionally (see
/// `a_crawl_body_a_live_record_addresses_is_not_reclaimed_by_age_alone`), so the
/// only place the *replayability* gate is still observable is on the abandoned
/// snapshot. If that gate is removed from `pinned_artifact_refs` or from
/// `keep_reason`, this test fails — which is the whole point: `POST
/// /provenance/.../rederive` would otherwise start answering "archived body
/// unavailable" for records the store still advertises as reproducible.
#[tokio::test]
async fn a_body_pinned_by_a_replayable_revision_survives_the_age_cutoff() {
    let store = TempStore::new("retention-pin").await;
    let ds = store.datasets();

    // Two keys, identical shape, differing only in whether the stamp makes the
    // revision replayable. Each is written twice, so revision 1's body is
    // historical and revision 2's is the live location.
    for (key, is_replayable) in [("pinned", true), ("loose", false)] {
        for job in [format!("job-{key}"), format!("job-{key}-v2")] {
            let prov = if is_replayable {
                replayable(&job)
            } else {
                stamped_only(&job)
            };
            ds.upsert_stamped(
                "crawl",
                "pages",
                key,
                &record(&job, "page.html"),
                None,
                Some(&prov),
            )
            .await
            .unwrap();
            write_body(&store, "crawl", &job, "page.html", b"body");
        }
    }

    let pinned = ds.pinned_artifact_refs().await.unwrap();
    // The replayable key's abandoned snapshot is pinned…
    assert!(pinned.contains(&ArtifactRef {
        app: "crawl".into(),
        job_id: "job-pinned".into(),
        name: "page.html".into(),
    }));
    // …the merely-stamped key's is not: rederive would refuse it anyway.
    assert!(
        !pinned.contains(&ArtifactRef {
            app: "crawl".into(),
            job_id: "job-loose".into(),
            name: "page.html".into(),
        }),
        "a stamp rederive refuses must not pin a body nothing else addresses: {pinned:?}"
    );
    // Both live locations are pinned, whatever their stamp.
    assert_eq!(pinned.len(), 3, "{pinned:?}");

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

/// The crawl's own write shape, verbatim: `pages` stamps `job_id` and nothing
/// else (`DatasetPageSink::job_prov`), `page_versions` stamps `job_id` +
/// `artifact_sha` and deliberately leaves `rules_hash` as `None` ("unknown,
/// never a fabricated pin"). Neither has a `rules_hash`, because a crawl runs no
/// RuleSet — it archives whole bodies.
fn crawl_pages_prov(job_id: &str) -> Provenance {
    Provenance {
        job_id: Some(job_id.to_string()),
        ..Provenance::default()
    }
}

fn crawl_version_prov(job_id: &str) -> Provenance {
    Provenance {
        job_id: Some(job_id.to_string()),
        source_url: Some("https://example.test/a".into()),
        artifact_sha: Some("c".repeat(64)),
        rules_hash: None,
    }
}

/// **The anti-pattern this pin exists to kill.** `pinned_artifact_refs` gated
/// both arms on `artifact_sha AND rules_hash`, so *zero* crawl bodies were
/// pinnable at any age under any config — while 11 `read_source_artifact` call
/// sites across four apps read exactly that corpus. `rules_hash` answers "did a
/// RuleSet make a provenance claim about this record"; the pin has to answer "is
/// this body still addressable by a live record", which is `artifact_path` +
/// `job_id` — the triple `read_source_artifact` resolves, and the one crawl
/// records DO carry.
///
/// The failure was silent-green: the extractor pushes an unreadable body onto
/// `missing` and continues, finishing `Ok` with clean fetch health, so the run
/// moves neither state nor baseline and `/sources` still reports the source
/// healthy. Job green, source green, zero records.
#[tokio::test]
async fn a_crawl_body_a_live_record_addresses_is_not_reclaimed_by_age_alone() {
    let store = TempStore::new("retention-crawl-pin").await;
    let ds = store.datasets();
    // Exactly what the crawl writes.
    ds.upsert_stamped(
        "crawl",
        "pages",
        "https://example.test/a",
        &record("job-crawl", "page-ab12.html"),
        None,
        Some(&crawl_pages_prov("job-crawl")),
    )
    .await
    .unwrap();
    ds.upsert_stamped(
        "crawl",
        "page_versions",
        "https://example.test/a#2",
        &record("job-crawl", "page-ab12.r2.html"),
        None,
        Some(&crawl_version_prov("job-crawl")),
    )
    .await
    .unwrap();
    write_body(&store, "crawl", "job-crawl", "page-ab12.html", b"body");
    write_body(
        &store,
        "crawl",
        "job-crawl",
        "page-ab12.r2.html",
        b"archived",
    );

    let pinned = ds.pinned_artifact_refs().await.unwrap();
    for name in ["page-ab12.html", "page-ab12.r2.html"] {
        assert!(
            pinned.contains(&ArtifactRef {
                app: "crawl".into(),
                job_id: "job-crawl".into(),
                name: name.into(),
            }),
            "{name} is addressable by a live record and must be pinned: {pinned:?}"
        );
    }
    // …and the sweep respects it, with every body past the cutoff.
    let files = scan_artifact_tree(&store.storage.artifacts_dir);
    let plan = plan_artifact_retention(&files, &pinned, everything_is_old(), true);
    assert!(
        plan.delete.is_empty(),
        "a crawl body the extract seam still reads was condemned: {:?}",
        plan.delete
    );
}

/// **The counter-test: the guard must not turn retention into a no-op.** A body
/// nothing addresses is still reclaimable, whatever else is in the tables — a
/// record with no `artifact_path`, and an orphan body in an abandoned job
/// directory (what a crawl revisit leaves behind, and the growth driver this
/// module was written for).
#[tokio::test]
async fn a_body_no_live_record_addresses_is_still_reclaimable() {
    let store = TempStore::new("retention-orphan").await;
    let ds = store.datasets();
    // A record that addresses nothing: no artifact_path, so nothing to pin.
    ds.upsert(
        "crawl",
        "pages",
        "no-body",
        &json!({ "job_id": "job-old", "url": "https://example.test/a" }),
    )
    .await
    .unwrap();
    // The live record moved to job-new on a revisit; job-old's copy is abandoned.
    ds.upsert("crawl", "pages", "k", &record("job-new", "page.html"))
        .await
        .unwrap();
    // A tombstoned record addresses nothing either: no read surface returns it,
    // so `read_source_artifact` can never be handed one.
    ds.upsert("crawl", "pages", "gone", &record("job-gone", "page.html"))
        .await
        .unwrap();
    assert_eq!(
        ds.tombstone_keys("crawl", "pages", &["gone".to_string()])
            .await
            .unwrap()
            .len(),
        1
    );
    write_body(&store, "crawl", "job-old", "page.html", b"superseded");
    write_body(&store, "crawl", "job-new", "page.html", b"current");
    write_body(&store, "crawl", "job-gone", "page.html", b"tombstoned");

    let pinned = ds.pinned_artifact_refs().await.unwrap();
    assert_eq!(pinned.len(), 1, "only the live location pins: {pinned:?}");
    let files = scan_artifact_tree(&store.storage.artifacts_dir);
    let plan = plan_artifact_retention(&files, &pinned, everything_is_old(), true);
    let doomed: HashSet<String> = plan.delete.iter().map(|r| r.job_id.clone()).collect();
    assert_eq!(
        doomed,
        ["job-old".to_string(), "job-gone".to_string()]
            .into_iter()
            .collect::<HashSet<_>>(),
        "{:?}",
        plan.delete
    );
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
