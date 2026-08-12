//! Migration-chain guards: a full 0001→NNNN replay against a fresh temp-dir
//! SQLite, and the automatic pre-migration backup that wraps it.
//!
//! The schema assertion follows this repo's EXPECTED-diff inventory idiom (see
//! `crates/server/src/routes/mod.rs`): a hardcoded set diffed against the
//! observed one, so a broken or misordered migration fails with the exact
//! tables that appeared or vanished — not a bare `assert_eq!` on a count.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use pumper_core::backup::{backup_before_migrations, BACKUPS_RETAINED, BACKUP_MARKER};
use pumper_core::storage::migrator;
use pumper_core::testing::TempStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

/// Every table the full migration chain must leave behind. Add the new table
/// here in the same change that adds the migration that creates it.
const EXPECTED_TABLES: &[&str] = &[
    "_sqlx_migrations",
    "api_recipes",
    "checkpoints",
    "cost_events",
    "datahub_govern_actions",
    "datahub_govern_levels",
    "derived",
    "doc_fingerprints",
    "field_invariants",
    "field_sketches",
    "job_stages",
    "job_yield",
    "http_cache",
    "ingress_sources",
    "jobs",
    "record_revisions",
    "records",
    "research_cache",
    "revalidations",
    "rules_versions",
    "saved_search_seen",
    "saved_searches",
    "schedules",
    "source_runs",
    "sources",
    "tier_memory",
    "trigger_runs",
    "triggers",
    "watches",
    "webhook_deliveries",
];

async fn table_names(pool: &SqlitePool) -> BTreeSet<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .fetch_all(pool)
    .await
    .expect("read sqlite_master")
    .into_iter()
    .collect()
}

async fn column_names(pool: &SqlitePool, table: &str) -> BTreeSet<String> {
    sqlx::query_scalar::<_, String>(&format!("SELECT name FROM pragma_table_info('{table}')"))
        .fetch_all(pool)
        .await
        .expect("pragma_table_info")
        .into_iter()
        .collect()
}

/// Replays the whole chain into a brand-new database and diffs the schema.
#[tokio::test]
async fn full_replay_produces_the_expected_table_inventory() {
    let store = TempStore::new("migration-replay").await;
    let observed = table_names(&store.storage.pool()).await;
    let expected: BTreeSet<String> = EXPECTED_TABLES.iter().map(|s| s.to_string()).collect();

    let missing: Vec<_> = expected.difference(&observed).collect();
    let unexpected: Vec<_> = observed.difference(&expected).collect();
    assert!(
        missing.is_empty(),
        "migration chain no longer creates these tables: {missing:?}"
    );
    assert!(
        unexpected.is_empty(),
        "chain creates tables not in the expected inventory (update EXPECTED_TABLES): {unexpected:?}"
    );
}

/// The chain is *ordered* schema evolution: later migrations `ALTER TABLE` what
/// earlier ones created. A replay that merely ends with the right table names
/// can still have lost columns, so pin the load-bearing ones.
#[tokio::test]
async fn replay_keeps_columns_added_by_later_migrations() {
    let store = TempStore::new("migration-columns").await;
    let pool = store.storage.pool();

    let jobs = column_names(&pool, "jobs").await;
    for col in [
        "id",
        "app",
        "status",
        "priority",        // 0003 orchestration
        "budget_usd",      // 0008 job budget
        "idempotency_key", // 0011 idempotency
        "schedule_id",     // 0012 schedule overlap
        "trigger_id",      // 0014 triggers
        "heartbeat_at",    // 0017 job heartbeat
    ] {
        assert!(jobs.contains(col), "jobs lost column `{col}`: {jobs:?}");
    }

    let records = column_names(&pool, "records").await;
    for col in ["app", "dataset", "key", "simhash"] {
        assert!(
            records.contains(col),
            "records lost column `{col}`: {records:?}"
        );
    }

    let revisions = column_names(&pool, "record_revisions").await;
    for col in [
        "trust",        // 0020 resilient extraction
        "job_id",       // 0030 provenance
        "source_url",   // 0030 provenance
        "artifact_sha", // 0030 provenance
        "rules_hash",   // 0030 provenance
    ] {
        assert!(
            revisions.contains(col),
            "record_revisions lost column `{col}`: {revisions:?}"
        );
    }

    let saved = column_names(&pool, "saved_searches").await;
    for col in ["materialize_app", "materialize_dataset"] {
        // 0029 queries-as-datasets
        assert!(
            saved.contains(col),
            "saved_searches lost column `{col}`: {saved:?}"
        );
    }

    let schedules = column_names(&pool, "schedules").await;
    for col in [
        "timezone",        // 0018 cron maturity
        "misfire_policy",  // 0018 cron maturity
        "max_attempts",    // 0018 cron maturity
        "managed_by",      // 0023 catalog reconcile fence
        "last_skipped_at", // 0039 schedule skips
        "skipped_count",   // 0039 schedule skips
        "budget_usd",      // 0040 schedule budget
    ] {
        assert!(
            schedules.contains(col),
            "schedules lost column `{col}`: {schedules:?}"
        );
    }

    let triggers = column_names(&pool, "triggers").await;
    for col in [
        "filters",      // 0021 external triggers
        "plugin_hooks", // 0032 trigger plugin hooks (M15)
    ] {
        assert!(
            triggers.contains(col),
            "triggers lost column `{col}`: {triggers:?}"
        );
    }

    let tier_memory = column_names(&pool, "tier_memory").await;
    for col in [
        "penalty_ms",   // 0016 host profiles
        "observations", // 0033 host weather (M01)
    ] {
        assert!(
            tier_memory.contains(col),
            "tier_memory lost column `{col}`: {tier_memory:?}"
        );
    }

    let watches = column_names(&pool, "watches").await;
    assert!(
        watches.contains("sink"), // 0031 watch sinks
        "watches lost column `sink`: {watches:?}"
    );

    let recipes = column_names(&pool, "api_recipes").await;
    assert!(
        recipes.contains("consecutive_failures"), // 0028 recipe strikes
        "api_recipes lost column `consecutive_failures`: {recipes:?}"
    );
}

/// Versions must be strictly increasing and unique — a duplicated `0007_` pair
/// or a file renamed backwards is a merge artifact the migrator would apply in
/// an order nobody tested.
#[tokio::test]
async fn migration_versions_are_unique_and_ascending() {
    let migrator = migrator();
    let versions: Vec<i64> = migrator
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| m.version)
        .collect();
    assert!(!versions.is_empty(), "no migrations embedded");
    for pair in versions.windows(2) {
        assert!(
            pair[0] < pair[1],
            "migration versions out of order: {pair:?} in {versions:?}"
        );
    }
    assert_eq!(versions[0], 1, "chain must start at 0001");
}

/// Every applied version is recorded, so a later boot has nothing pending.
#[tokio::test]
async fn replay_records_every_version_in_sqlx_migrations() {
    let store = TempStore::new("migration-ledger").await;
    let applied: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&store.storage.pool())
            .await
            .expect("read _sqlx_migrations");
    let embedded: Vec<i64> = migrator()
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| m.version)
        .collect();
    assert_eq!(applied, embedded);
}

// ---------------------------------------------------------------------------
// pre-migration backup
// ---------------------------------------------------------------------------

fn backups_beside(db_path: &Path) -> Vec<PathBuf> {
    let dir = db_path.parent().expect("db has a parent dir");
    let marker = format!(
        "{}{BACKUP_MARKER}",
        db_path.file_name().unwrap().to_string_lossy()
    );
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .expect("read data dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(&marker))
        })
        .collect();
    found.sort();
    found
}

/// The whole test suite boots through `Storage::connect`. A fresh database has
/// nothing to lose, so the harness must stay backup-free — otherwise every test
/// would leave a snapshot behind.
#[tokio::test]
async fn temp_store_boot_writes_no_backup() {
    let store = TempStore::new("migration-backup-skip").await;
    let db = store.path().join("pumper.db");
    assert!(
        backups_beside(&db).is_empty(),
        "TempStore wrote a pre-migration backup: {:?}",
        backups_beside(&db)
    );
}

/// Forces a pending migration (drops the newest ledger row) and asserts the
/// backup is a real, standalone, readable database — not a torn copy of the
/// main file with its WAL left behind.
#[tokio::test]
async fn pending_migration_writes_a_readable_standalone_snapshot() {
    let store = TempStore::new("migration-backup-take").await;
    let pool = store.storage.pool();
    let db = store.path().join("pumper.db");

    // Leave uncommitted-to-disk work in the WAL: a naive `fs::copy` of the main
    // file would miss this row, `VACUUM INTO` must not.
    sqlx::query("INSERT INTO watches (id, app, dataset, url, enabled, created_at) \
                 VALUES ('w1', 'demo', '*', 'https://example.test/hook', 1, '2026-07-26T00:00:00.000000Z')")
        .execute(&pool)
        .await
        .expect("insert watch");
    sqlx::query(
        "DELETE FROM _sqlx_migrations WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
    )
    .execute(&pool)
    .await
    .expect("simulate a pending migration");

    let dest = backup_before_migrations(&pool, &db, &migrator())
        .await
        .expect("pending migration must produce a backup");
    assert!(dest.exists(), "backup file missing: {}", dest.display());
    assert!(
        !dest.with_extension("db-wal").exists(),
        "VACUUM INTO must yield a single file, no WAL sidecar"
    );

    let copy = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(&dest).read_only(true))
        .await
        .expect("backup is not a valid SQLite database");
    let expected: BTreeSet<String> = EXPECTED_TABLES.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        table_names(&copy).await,
        expected,
        "backup lost tables from the live database"
    );
    let watches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM watches")
        .fetch_one(&copy)
        .await
        .expect("count watches in backup");
    assert_eq!(watches, 1, "WAL-resident row missing from the backup");
    copy.close().await;
}

/// Backups are bounded: a long-lived install must not accumulate one snapshot
/// per migration forever.
#[tokio::test]
async fn old_backups_are_pruned_to_the_retention_limit() {
    let store = TempStore::new("migration-backup-prune").await;
    let pool = store.storage.pool();
    let db = store.path().join("pumper.db");

    // Pre-existing snapshots from earlier upgrades, plus an operator's
    // hand-made copy that must survive untouched.
    for stamp in ["20260101-000000", "20260102-000000", "20260103-000000"] {
        std::fs::write(
            store
                .path()
                .join(format!("pumper.db{BACKUP_MARKER}{stamp}")),
            b"old",
        )
        .expect("seed backup");
    }
    let hand_made = store.path().join("pumper.db.bak-simhash-20260715-081548");
    std::fs::write(&hand_made, b"operator").expect("seed hand-made copy");

    sqlx::query(
        "DELETE FROM _sqlx_migrations WHERE version = (SELECT MAX(version) FROM _sqlx_migrations)",
    )
    .execute(&pool)
    .await
    .expect("simulate a pending migration");
    let dest = backup_before_migrations(&pool, &db, &migrator())
        .await
        .expect("backup taken");

    let remaining = backups_beside(&db);
    assert_eq!(
        remaining.len(),
        BACKUPS_RETAINED,
        "retention not enforced: {remaining:?}"
    );
    assert!(remaining.contains(&dest), "the new backup was pruned");
    assert!(
        !remaining
            .iter()
            .any(|p| p.to_string_lossy().contains("20260101")),
        "oldest backup survived pruning: {remaining:?}"
    );
    assert!(hand_made.exists(), "operator's hand-made copy was pruned");
}
