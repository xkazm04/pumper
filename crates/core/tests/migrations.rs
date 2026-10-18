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
use pumper_core::costs::{CostLedger, SpendCertainty};
use pumper_core::storage::migrator;
use pumper_core::testing::TempStore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

/// Re-exported from the crate, not re-declared here. `EXPECTED_TABLES` used
/// to live in this file, which meant the boot path could not reach the only
/// manifest of what the schema should be: CI knew, and the running binary did
/// not. It now lives in `pumper_core::schema` and `Storage::connect` refuses a
/// store that is missing one of these - so a migration that creates a table
/// without updating the constant fails this test AND a real boot.
use pumper_core::schema::EXPECTED_TABLES;

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
    sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM pragma_table_info('{table}')"
    )))
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

    let schedule_runs = column_names(&pool, "schedule_runs").await;
    for col in [
        "schedule_id", // 0047 schedule decision ledger
        "outcome",     // 0047 what the tick did
        "reason",      // 0047 WHY, in the admission vocabulary
        "due_at",      // 0047 the firing the row is about
        "job_id",      // 0047 the job fired, or the one holding the slot
        "missed",      // 0047 the misfire count the log line threw away
        "detail",      // 0047 free-text context
    ] {
        assert!(
            schedule_runs.contains(col),
            "schedule_runs lost column `{col}`: {schedule_runs:?}"
        );
    }

    let api_recipes = column_names(&pool, "api_recipes").await;
    for col in [
        "consecutive_failures", // 0028 recipe strikes
        "retired_at",           // 0046 recipe lifecycle
        "last_failure_at",      // 0046 recipe lifecycle
        "validation_reason",    // 0049 X-ray validation verdict
        "validated_at",         // 0049 X-ray validation verdict
    ] {
        assert!(
            api_recipes.contains(col),
            "api_recipes lost column `{col}`: {api_recipes:?}"
        );
    }

    let watches = column_names(&pool, "watches").await;
    assert!(
        watches.contains("cursor_seq"), // 0055 durable event log
        "watches lost column `cursor_seq`: {watches:?}"
    );

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
    for col in [
        "retired_at",      // 0046 recipe lifecycle
        "last_failure_at", // 0046 recipe lifecycle
    ] {
        assert!(
            recipes.contains(col),
            "api_recipes lost column `{col}`: {recipes:?}"
        );
    }

    let cost_events = column_names(&pool, "cost_events").await;
    assert!(
        cost_events.contains("spend_certainty"), // 0045 spend certainty
        "cost_events lost column `spend_certainty`: {cost_events:?}"
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

// ---------------------------------------------------------------------------
// 0045 — cost_events.spend_certainty and its back-fill
// ---------------------------------------------------------------------------

/// The whole 0045 migration, so the back-fill under test is the **real** one
/// and not a copy that can drift from it.
const MIGRATION_0045: &str = include_str!("../migrations/0045_cost_event_spend_certainty.sql");

/// Everything after the `-- BACKFILL --` sentinel: one `UPDATE`, runnable on
/// its own, which is what makes the idempotency assertion below mean anything.
fn backfill_0045() -> &'static str {
    MIGRATION_0045
        .split_once("-- BACKFILL --")
        .expect("0045 marks its back-fill section with `-- BACKFILL --`")
        .1
}

/// Every `detail` spelling the ledger has ever written, with the certainty the
/// back-fill must derive from it. These strings are produced by
/// `ClaudeSpend::ledger_event`, `AppContext::success_spend_event` and the
/// cache/VCR seams — if one is renamed there, history stops being classifiable
/// and this table is where that is noticed.
const LEGACY_ROWS: &[(&str, &str, f64, Option<&str>, &str)] = &[
    // (case name, engine, cost_usd, detail, expected spend_certainty)
    (
        "reported failure spend",
        "claude",
        0.42,
        Some("failed_spend (is_error)"),
        "billed",
    ),
    (
        "killed mid-run",
        "claude",
        0.0,
        Some("unmetered_timeout"),
        "unmetered",
    ),
    (
        "exit 0, stdout garbage",
        "claude",
        0.0,
        Some("failed_spend_unreported (unparseable)"),
        "unmetered",
    ),
    (
        "prompt never arrived",
        "claude",
        0.0,
        Some("failed_spend_unreported (prompt_undelivered)"),
        "unmetered",
    ),
    (
        "is_error with no amount",
        "claude",
        0.0,
        Some("failed_spend_unreported (is_error)"),
        "unmetered",
    ),
    (
        "wait() syscall failed",
        "claude",
        0.0,
        Some("failed_spend_unreported (wait_failed)"),
        "unknown",
    ),
    (
        "pipe broke on a live child",
        "claude",
        0.0,
        Some("failed_spend_unreported (pipe_failure)"),
        "unknown",
    ),
    (
        "bare non-zero exit",
        "claude",
        0.0,
        Some("failed_spend_unreported (nonzero_exit)"),
        "unknown",
    ),
    (
        "success, no total_cost_usd",
        "claude",
        0.0,
        Some("cost_unreported"),
        "unmetered",
    ),
    ("ordinary priced answer", "claude", 0.25, None, "billed"),
    ("the model answered for free", "claude", 0.0, None, "billed"),
    (
        "cache hit",
        "claude",
        0.0,
        Some("cache_hit (saved ~$0.1200)"),
        "free",
    ),
    ("vcr replay", "claude", 0.0, Some("vcr_replay"), "free"),
    ("plain http fetch", "http", 0.0, None, "free"),
    (
        "escalated http fetch",
        "http",
        0.0,
        Some("http tier blocked: 403"),
        "free",
    ),
];

async fn certainty_of(pool: &SqlitePool, job_id: &str) -> Vec<(String, String)> {
    sqlx::query_as::<_, (String, String)>(
        "SELECT COALESCE(detail, '<null>'), spend_certainty FROM cost_events \
         WHERE job_id = ?1 ORDER BY id",
    )
    .bind(job_id)
    .fetch_all(pool)
    .await
    .expect("read back spend_certainty")
}

/// THE case this column exists for: a `$0` row for a killed paid run and a `$0`
/// row for a free cache hit were byte-identical on every surface. After 0045
/// they differ in a column a consumer can `GROUP BY` and filter on — including
/// for the history that was written before the column existed.
///
/// The back-fill is derived purely from `detail` (never modified) plus
/// `engine`/`cost_usd`, so replaying it is a no-op. That matters: a migration
/// that runs once is still a migration somebody replays onto a copy.
#[tokio::test]
async fn migration_0045_backfills_spend_certainty_from_history_exactly_once() {
    let store = TempStore::new("migration-0045-backfill").await;
    let pool = store.storage.pool();
    let job_id = "11111111-1111-4111-8111-111111111111";

    // Rows as they existed BEFORE 0045: the ALTER's `DEFAULT 'free'` is what
    // every historical row carried the instant the column appeared, which is
    // precisely the wrong answer for most of them.
    for (i, (_, engine, cost_usd, detail, _)) in LEGACY_ROWS.iter().enumerate() {
        sqlx::query(
            "INSERT INTO cost_events \
             (job_id, app, engine, url, cost_usd, spend_certainty, detail, created_at) \
             VALUES (?1, 'demo', ?2, NULL, ?3, 'free', ?4, ?5)",
        )
        .bind(job_id)
        .bind(engine)
        .bind(cost_usd)
        .bind(*detail)
        .bind(format!("2026-10-05T00:00:{i:02}.000000Z"))
        .execute(&pool)
        .await
        .expect("seed a pre-0045 cost event");
    }

    sqlx::query(backfill_0045())
        .execute(&pool)
        .await
        .expect("the back-fill statement runs");

    let observed = certainty_of(&pool, job_id).await;
    let expected: Vec<(String, String)> = LEGACY_ROWS
        .iter()
        .map(|(_, _, _, detail, certainty)| {
            (
                detail.unwrap_or("<null>").to_string(),
                (*certainty).to_string(),
            )
        })
        .collect();
    for ((case, _, _, _, want), (_, got)) in LEGACY_ROWS.iter().zip(&observed) {
        assert_eq!(got, want, "0045 mis-classified the `{case}` row");
    }
    assert_eq!(observed, expected, "back-filled distribution drifted");

    // The headline pair, asserted as a pair: these two were indistinguishable.
    let killed = &observed[1];
    let cached = &observed[11];
    assert_eq!(killed.1, "unmetered");
    assert_eq!(cached.1, "free");
    assert_ne!(
        killed.1, cached.1,
        "a killed paid run and a free cache hit are the same row again"
    );

    // Exactly once: replaying the back-fill changes nothing.
    sqlx::query(backfill_0045())
        .execute(&pool)
        .await
        .expect("the back-fill replays");
    assert_eq!(
        certainty_of(&pool, job_id).await,
        observed,
        "the back-fill is not idempotent — a replay onto a copy would rewrite history"
    );
}

/// GUARD. 0045 is additive by construction: the budget ceiling still sums
/// `cost_usd` and only `cost_usd`, so an unmetered row still contributes `$0`
/// to enforcement exactly as it did before. The card labels the headroom; it
/// deliberately does **not** move the ceiling, and this test is what makes that
/// a promise rather than a sentence.
#[tokio::test]
async fn job_total_still_sums_cost_usd_and_ignores_certainty() {
    let store = TempStore::new("migration-0045-ceiling").await;
    let pool = store.storage.pool();
    let ledger = CostLedger::new(pool.clone());
    let job_id = uuid::Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap();

    for (cost, certainty) in [
        (0.30, SpendCertainty::Billed),
        (0.0, SpendCertainty::Unmetered),
        (0.0, SpendCertainty::Unknown),
        (0.0, SpendCertainty::Free),
        (0.12, SpendCertainty::Billed),
    ] {
        ledger
            .record(job_id, "demo", "claude", None, cost, certainty, None)
            .await
            .expect("record");
    }

    let total = ledger.job_total(job_id).await.expect("job_total");
    assert!(
        (total - 0.42).abs() < 1e-9,
        "job_total must still be the plain SUM(cost_usd): {total}"
    );

    // …and the certainty survives the round trip, per row.
    let events = ledger.job_events(job_id).await.expect("job_events");
    let certainties: Vec<&str> = events.iter().map(|e| e.spend_certainty.as_str()).collect();
    assert_eq!(
        certainties,
        vec!["billed", "unmetered", "unknown", "free", "billed"]
    );
}

/// `GET /costs?certainty=` is a `HAVING`, not a `WHERE`: an operator asking
/// where the unmetered spend is wants the affected group's whole picture, and
/// an unaffected group must not appear at all.
#[tokio::test]
async fn the_certainty_filter_selects_whole_groups_that_contain_one() {
    let store = TempStore::new("migration-0045-filter").await;
    let ledger = CostLedger::new(store.storage.pool());
    let dirty = uuid::Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap();
    let clean = uuid::Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();

    ledger
        .record(
            dirty,
            "noisy",
            "claude",
            None,
            0.30,
            SpendCertainty::Billed,
            None,
        )
        .await
        .unwrap();
    ledger
        .record(
            dirty,
            "noisy",
            "claude",
            None,
            0.0,
            SpendCertainty::Unmetered,
            None,
        )
        .await
        .unwrap();
    ledger
        .record(
            clean,
            "quiet",
            "claude",
            None,
            0.50,
            SpendCertainty::Billed,
            None,
        )
        .await
        .unwrap();

    let all = ledger.summary(None, None).await.expect("summary");
    assert_eq!(all.len(), 2, "the unfiltered rollup keeps every group");

    let unmetered = ledger
        .summary_filtered(None, None, Some(SpendCertainty::Unmetered), None)
        .await
        .expect("filtered summary");
    assert_eq!(unmetered.len(), 1, "only the affected group: {unmetered:?}");
    assert_eq!(unmetered[0].app, "noisy");
    assert_eq!(unmetered[0].unmetered_calls, 1);
    assert_eq!(
        unmetered[0].calls, 2,
        "the group's WHOLE picture, not just the bad row"
    );
    assert!(
        (unmetered[0].cost_usd - 0.30).abs() < 1e-9,
        "the billed money of the affected group travels with it"
    );
    assert_eq!(unmetered[0].total_certainty(), "at_least");

    // The clean group answers `exact`, and vanishes under the filter.
    let quiet = all.iter().find(|s| s.app == "quiet").expect("quiet group");
    assert_eq!(quiet.uncertain_calls(), 0);
    assert_eq!(quiet.total_certainty(), "exact");
    let unknown = ledger
        .summary_filtered(None, None, Some(SpendCertainty::Unknown), None)
        .await
        .expect("filtered summary");
    assert!(unknown.is_empty(), "nothing is `unknown` here: {unknown:?}");
}
