//! `datasets doctor` against a real migrated SQLite.
//!
//! The pure diagnosis rules are unit-tested in `core::doctor`. What needs a
//! database is that the **queries feeding them** are silent on a healthy store
//! and speak up on a broken one — a check that mis-reads its own SQL produces
//! false positives that no amount of pure testing would catch.

use pumper_core::doctor::{diagnose, StoreFacts};
use pumper_core::testing::TempStore;
use pumper_core::Provenance;
use serde_json::json;

fn replayable() -> Provenance {
    Provenance {
        job_id: Some("job-a".into()),
        source_url: Some("https://example.test/a".into()),
        artifact_sha: Some("a".repeat(64)),
        rules_hash: Some("b".repeat(64)),
    }
}

/// **No false positives.** A store that has done real, correct work — stamped
/// revisions, registered rules, fingerprinted records, a derived spec over a
/// populated source — must produce an EMPTY findings list. A report that always
/// says something is a report nobody reads.
#[tokio::test]
async fn a_clean_store_produces_no_findings() {
    let store = TempStore::new("doctor-clean").await;
    let ds = store.datasets();
    let pool = store.storage.pool();

    // Register the ruleset the stamp names, the way a correct write path does.
    sqlx::query("INSERT INTO rules_versions (hash, rules, created_at) VALUES (?1, '{}', ?2)")
        .bind("b".repeat(64))
        .bind("2026-01-01T00:00:00.000000Z")
        .execute(&pool)
        .await
        .unwrap();

    ds.upsert_stamped(
        "crawl",
        "pages",
        "k",
        &json!({ "title": "a page with words in it", "job_id": "job-a",
                 "artifact_path": "page.html" }),
        None,
        Some(&replayable()),
    )
    .await
    .unwrap();

    // A derived spec whose source actually holds records.
    sqlx::query(
        "INSERT INTO derived (id, source_app, source_dataset, target_dataset, created_at) \
         VALUES ('d1', 'crawl', 'pages', 'titles', ?1)",
    )
    .bind("2026-01-01T00:00:00.000000Z")
    .execute(&pool)
    .await
    .unwrap();

    let facts = StoreFacts {
        half_stamped: ds.half_stamped_revisions().await.unwrap(),
        unregistered_rules: ds.unregistered_rules_hashes().await.unwrap(),
        null_simhash: ds.null_simhash_counts().await.unwrap(),
        orphan_derived: ds.orphan_derived_specs().await.unwrap(),
        stale_rebuild_tables: store.storage.stale_rebuild_tables().await.unwrap(),
        ..Default::default()
    };
    let findings = diagnose(&facts);
    assert!(findings.is_empty(), "clean store reported: {findings:?}");
}

/// The `triggers_new` question, settled against the migration chain rather than
/// by reading it: migration 0021 rebuilds `triggers` through a `triggers_new`
/// scaffold and `RENAME`s it into place, and each migration runs in a
/// transaction — so after a full migrate the scaffold does not exist, `triggers`
/// does, and it carries the rebuilt three-value CHECK. Not a stale table.
#[tokio::test]
async fn the_triggers_rebuild_scaffold_does_not_survive_migration() {
    let store = TempStore::new("doctor-triggers").await;
    let pool = store.storage.pool();

    assert!(
        store
            .storage
            .stale_rebuild_tables()
            .await
            .unwrap()
            .is_empty(),
        "a fully migrated database has no leftover rebuild scaffolds"
    );

    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'triggers%'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(names, vec!["triggers".to_string()]);

    // The rebuild is what added 'external' to the CHECK, so its presence proves
    // the RENAME landed rather than the pre-0021 table surviving.
    let sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'triggers'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(sql.contains("'external'"), "{sql}");
}

/// The scaffold check is not decorative: when a rebuild really is left behind,
/// it is named with the remediation.
#[tokio::test]
async fn a_leftover_rebuild_scaffold_is_reported() {
    let store = TempStore::new("doctor-scaffold").await;
    sqlx::query("CREATE TABLE widgets_new (id TEXT PRIMARY KEY)")
        .execute(&store.storage.pool())
        .await
        .unwrap();
    let stale = store.storage.stale_rebuild_tables().await.unwrap();
    assert_eq!(stale, vec!["widgets_new".to_string()]);

    let findings = diagnose(&StoreFacts {
        stale_rebuild_tables: stale,
        ..Default::default()
    });
    assert_eq!(findings.len(), 1);
    assert_eq!(findings[0].check, "stale_rebuild_tables");
    assert!(findings[0].remediation.contains("migrations"));
}

/// A revision stamping exactly one half of the replay pin is found, and an
/// honestly-unstamped one is NOT — the two are different states and conflating
/// them would make the check fire on every legacy store.
#[tokio::test]
async fn half_stamps_are_found_and_honest_nulls_are_not() {
    let store = TempStore::new("doctor-halfstamp").await;
    let ds = store.datasets();

    ds.upsert("crawl", "pages", "legacy", &json!({ "t": "words here" }))
        .await
        .unwrap();
    ds.upsert_stamped(
        "crawl",
        "pages",
        "half",
        &json!({ "t": "more words here" }),
        None,
        Some(&Provenance {
            job_id: Some("job-a".into()),
            artifact_sha: Some("a".repeat(64)),
            rules_hash: None,
            source_url: None,
        }),
    )
    .await
    .unwrap();

    let half = ds.half_stamped_revisions().await.unwrap();
    assert_eq!(half, vec![("crawl".to_string(), "pages".to_string(), 1)]);
}

/// A stamped-but-unregistered ruleset is exactly what `rederive` refuses with
/// "not in the rules_versions registry", so the doctor must surface it — and
/// must go quiet once the ruleset is registered.
#[tokio::test]
async fn unregistered_rulesets_are_reported_until_they_are_registered() {
    let store = TempStore::new("doctor-rules").await;
    let ds = store.datasets();
    ds.upsert_stamped(
        "crawl",
        "pages",
        "k",
        &json!({ "t": "some words" }),
        None,
        Some(&replayable()),
    )
    .await
    .unwrap();

    let missing = ds.unregistered_rules_hashes().await.unwrap();
    assert_eq!(missing, vec![("b".repeat(64), 1)]);

    sqlx::query("INSERT INTO rules_versions (hash, rules, created_at) VALUES (?1, '{}', ?2)")
        .bind("b".repeat(64))
        .bind("2026-01-01T00:00:00.000000Z")
        .execute(&store.storage.pool())
        .await
        .unwrap();
    assert!(ds.unregistered_rules_hashes().await.unwrap().is_empty());
}

/// Coverage is descriptive, not a finding: a dataset whose app cannot know its
/// source hash is honestly unstamped, and flagging it would be noise. The numbers
/// must still be right.
#[tokio::test]
async fn coverage_counts_the_whole_chain_per_dataset() {
    let store = TempStore::new("doctor-coverage").await;
    let ds = store.datasets();
    ds.upsert("crawl", "pages", "a", &json!({ "t": "one" }))
        .await
        .unwrap();
    ds.upsert_stamped(
        "crawl",
        "pages",
        "b",
        &json!({ "t": "two" }),
        None,
        Some(&replayable()),
    )
    .await
    .unwrap();

    let cov = ds.provenance_coverage_by_dataset().await.unwrap();
    assert_eq!(
        cov,
        vec![("crawl".to_string(), "pages".to_string(), 2, 1, 1)]
    );
    // And it agrees with the per-record view for the stamped key.
    assert_eq!(
        ds.provenance_coverage("crawl", "pages", "b").await.unwrap(),
        (1, 1, 1)
    );
}

/// An orphan derived spec is one whose source dataset holds nothing — it will
/// recompute forever over an empty set.
#[tokio::test]
async fn orphan_derived_specs_are_the_ones_with_no_source_records() {
    let store = TempStore::new("doctor-derived").await;
    let ds = store.datasets();
    let pool = store.storage.pool();
    ds.upsert("crawl", "pages", "a", &json!({ "t": "one" }))
        .await
        .unwrap();
    for (id, src) in [("live", "pages"), ("orphan", "ghosts")] {
        sqlx::query(
            "INSERT INTO derived (id, source_app, source_dataset, target_dataset, created_at) \
             VALUES (?1, 'crawl', ?2, 'out', ?3)",
        )
        .bind(id)
        .bind(src)
        .bind("2026-01-01T00:00:00.000000Z")
        .execute(&pool)
        .await
        .unwrap();
    }
    let orphans = ds.orphan_derived_specs().await.unwrap();
    assert_eq!(orphans.len(), 1);
    assert_eq!(orphans[0].0, "orphan");
    assert_eq!(orphans[0].1, "crawl/ghosts");
}

/// The report must never write. `datasets doctor` is read-only by construction
/// (every query is a SELECT), and this pins it: running the whole fact-gathering
/// set leaves the store byte-identical.
#[tokio::test]
async fn gathering_the_report_mutates_nothing() {
    let store = TempStore::new("doctor-readonly").await;
    let ds = store.datasets();
    ds.upsert_stamped(
        "crawl",
        "pages",
        "k",
        &json!({ "t": "words" }),
        None,
        Some(&replayable()),
    )
    .await
    .unwrap();

    let snapshot = |pool: sqlx::SqlitePool| async move {
        let recs: Vec<(String, String)> =
            sqlx::query_as("SELECT key, data FROM records ORDER BY key")
                .fetch_all(&pool)
                .await
                .unwrap();
        let revs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM record_revisions")
            .fetch_one(&pool)
            .await
            .unwrap();
        (recs, revs)
    };
    let before = snapshot(store.storage.pool()).await;

    let _ = ds.half_stamped_revisions().await.unwrap();
    let _ = ds.unregistered_rules_hashes().await.unwrap();
    let _ = ds.null_simhash_counts().await.unwrap();
    let _ = ds.orphan_derived_specs().await.unwrap();
    let _ = ds.provenance_coverage_by_dataset().await.unwrap();
    let _ = ds.replayable_revisions(100).await.unwrap();
    let _ = store.storage.stale_rebuild_tables().await.unwrap();
    let _ = store.storage.ledger_stats().await.unwrap();

    assert_eq!(before, snapshot(store.storage.pool()).await);
}
