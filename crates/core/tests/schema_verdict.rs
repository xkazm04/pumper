//! Boot asserts the schema it got.
//!
//! The pure classification lives in `pumper_core::schema` and is unit-tested
//! there. What needs a real database is the thing that was actually missing:
//! `Storage::connect` ran the migrator and returned `Ok` **without reading back
//! the schema it produced**, so a store this binary cannot read booted clean
//! and failed later, at the first query, with no hint of the cause.
//!
//! Every refusal below is reached by constructing the broken store state for
//! real — a dropped table, a ledger row from a newer binary — never by mocking
//! the check out.

use std::collections::BTreeSet;

use pumper_core::config::StorageConfig;
use pumper_core::schema::{self, LedgerVerdict};
use pumper_core::storage::{migrator, Storage};
use pumper_core::testing::TempStore;
use pumper_core::Error;
use sqlx::SqlitePool;

/// A second `StorageConfig` pointing at a store that already exists, which is
/// exactly what a restart is.
fn reboot_cfg(root: &std::path::Path) -> StorageConfig {
    StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts"),
        ..StorageConfig::default()
    }
}

async fn table_names(pool: &SqlitePool) -> BTreeSet<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(pool)
    .await
    .expect("read sqlite_master")
    .into_iter()
    .collect()
}

/// Writes a `_sqlx_migrations` row for a version this build does not carry —
/// what a newer pumper leaves behind after it migrates the store.
async fn forge_applied_version(pool: &SqlitePool, version: i64) {
    sqlx::query(
        "INSERT INTO _sqlx_migrations \
         (version, description, installed_on, success, checksum, execution_time) \
         VALUES (?1, 'from a newer binary', CURRENT_TIMESTAMP, 1, X'00', 0)",
    )
    .bind(version)
    .execute(pool)
    .await
    .expect("forge a newer ledger row");
}

fn schema_message(err: Error) -> String {
    match err {
        Error::Schema(msg) => msg,
        other => panic!("expected Error::Schema, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// GUARD — the store this binary CAN read must still boot
// ---------------------------------------------------------------------------

/// **GUARD, green before this change by design.** The whole test suite boots
/// through `Storage::connect`; a post-condition that refuses a healthy store
/// would take the entire workspace down with it. Declared as a guard before the
/// refusals were built, and asserted here on a *restart* (the second connect),
/// which is the path the new check actually runs on.
#[tokio::test]
async fn a_store_this_binary_can_read_still_boots() {
    let store = TempStore::new("schema-guard-boots").await;
    let root = store.path().to_path_buf();

    let again = Storage::connect(&reboot_cfg(&root))
        .await
        .expect("a healthy store must still boot on restart");
    let report = again.schema_report().await.expect("schema report");
    assert!(report.is_sound(), "a fresh replay is sound: {report:?}");
    assert_eq!(report.state, "current");
    assert!(report.pending.is_empty());
    assert!(report.ahead.is_empty());
    assert!(report.tables_missing.is_empty());
    assert!(
        report.tables_unexpected.is_empty(),
        "the chain creates tables not in EXPECTED_TABLES: {:?}",
        report.tables_unexpected
    );
}

/// **GUARD.** An operator's own table is not a schema fault. A post-condition
/// that refuses anything it did not create would make the store hostile to the
/// person who owns it.
#[tokio::test]
async fn an_operator_scratch_table_is_reported_not_refused() {
    let store = TempStore::new("schema-guard-scratch").await;
    let root = store.path().to_path_buf();
    sqlx::query("CREATE TABLE operator_scratch (id INTEGER PRIMARY KEY)")
        .execute(&store.storage.pool())
        .await
        .expect("create a scratch table");

    let again = Storage::connect(&reboot_cfg(&root))
        .await
        .expect("an extra table must never block a boot");
    let report = again.schema_report().await.expect("schema report");
    assert_eq!(
        report.tables_unexpected,
        vec!["operator_scratch".to_string()]
    );
    assert!(report.is_sound(), "reported, never fatal");
}

// ---------------------------------------------------------------------------
// the refusals
// ---------------------------------------------------------------------------

/// A table the ledger says exists and the store does not have. Before this
/// change `connect` returned `Ok` and the first `golden_docs` query failed at
/// runtime as a bare `Error::Storage`.
#[tokio::test]
async fn a_dropped_table_refuses_the_boot_and_names_it() {
    let store = TempStore::new("schema-dropped-table").await;
    let root = store.path().to_path_buf();
    sqlx::query("DROP TABLE golden_docs")
        .execute(&store.storage.pool())
        .await
        .expect("drop golden_docs");

    let err = Storage::connect(&reboot_cfg(&root))
        .await
        .err()
        .expect("boot must refuse a store missing a promised table");
    let msg = schema_message(err);
    assert!(msg.contains("golden_docs"), "names the table: {msg}");
    assert!(msg.contains("bak-premigrate"), "names the way back: {msg}");
}

/// The downgrade path. This repo has **zero** down migrations, so running an
/// older binary is the only way back — and it was the one path nothing checked.
#[tokio::test]
async fn a_store_ahead_of_this_binary_refuses_the_boot() {
    let store = TempStore::new("schema-ahead").await;
    let root = store.path().to_path_buf();
    forge_applied_version(&store.storage.pool(), 9999).await;

    let err = Storage::connect(&reboot_cfg(&root))
        .await
        .err()
        .expect("boot must refuse a store a newer binary already moved");
    let msg = schema_message(err);
    assert!(msg.contains("9999"), "names the version: {msg}");
    assert!(
        msg.contains("allow_schema_ahead"),
        "names the key that opens the door: {msg}"
    );
}

/// The ceiling defaults closed and says which key opens it — and opening it
/// really does boot, rather than merely changing the sentence.
#[tokio::test]
async fn allow_schema_ahead_opens_the_door_it_names() {
    let store = TempStore::new("schema-ahead-allowed").await;
    let root = store.path().to_path_buf();
    forge_applied_version(&store.storage.pool(), 9999).await;

    let cfg = StorageConfig {
        allow_schema_ahead: true,
        ..reboot_cfg(&root)
    };
    let again = Storage::connect(&cfg)
        .await
        .expect("allow_schema_ahead = true must boot the same store");
    let report = again.schema_report().await.expect("schema report");
    assert_eq!(
        report.ahead,
        vec![9999],
        "the ahead versions are still reported, just not fatal"
    );
    assert!(
        !report.is_sound(),
        "booting it does not make it sound — the doctor must still say so"
    );
    assert!(
        !StorageConfig::default().allow_schema_ahead,
        "a safety ceiling defaults closed"
    );
}

// ---------------------------------------------------------------------------
// the manifest is reachable from the boot path
// ---------------------------------------------------------------------------

/// `EXPECTED_TABLES` used to live in a test file, so the boot path could not
/// reach it. This asserts the live replay and the crate constant agree — the
/// same diff the CI replay test makes, now made against the authority a real
/// boot reads.
#[tokio::test]
async fn the_live_replay_matches_the_crate_manifest() {
    let store = TempStore::new("schema-manifest").await;
    let observed = table_names(&store.storage.pool()).await;
    let drift = schema::table_drift(&observed, schema::EXPECTED_TABLES);
    assert!(
        drift.is_clean(),
        "replay vs pumper_core::schema::EXPECTED_TABLES: missing {:?}, unexpected {:?}",
        drift.missing,
        drift.unexpected
    );
}

/// The embedded chain, classified against itself after a full replay: the
/// ledger must be `Current`, which is the input `classify` sees on every
/// ordinary restart.
#[tokio::test]
async fn a_replayed_store_classifies_as_current_against_the_embedded_chain() {
    let store = TempStore::new("schema-current").await;
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&store.storage.pool())
            .await
            .expect("read _sqlx_migrations");
    let available: Vec<i64> = migrator()
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| m.version)
        .collect();
    assert_eq!(
        schema::classify(&applied, &available),
        LedgerVerdict::Current
    );
}
