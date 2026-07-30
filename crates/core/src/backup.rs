//! Automatic pre-migration database backups.
//!
//! Codifies a ritual the operator used to perform by hand (the tree still holds
//! a `data/pumper.db.bak-simhash-20260715-081548` from before a risky simhash
//! migration): immediately *before* the migrator runs, take a timestamped copy
//! of the database so a bad migration is recoverable.
//!
//! Two properties matter and both are why this is not a `std::fs::copy`:
//!
//! - **WAL correctness.** The pool opens SQLite in WAL mode
//!   ([`crate::storage::Storage::connect`]), so at any instant the committed
//!   state is `pumper.db` *plus* whatever lives in `pumper.db-wal`. Copying the
//!   main file alone can yield a torn, older-than-committed snapshot. The copy
//!   is therefore taken with `VACUUM INTO`, which asks SQLite itself to write a
//!   consistent, single-file, already-compacted image of the *committed*
//!   database — no `-wal`/`-shm` sidecars to keep together.
//! - **Test safety.** Every storage test runs this exact path via
//!   [`crate::testing::TempStore`]. The decision is a named predicate
//!   ([`backup_decision`]) whose `SkipFreshDatabase` arm covers temp stores and
//!   first boots: a database with no applied migrations has nothing to lose, so
//!   no backup is written and the test harness stays byte-for-byte as it was.
//!
//! Retention is bounded: [`prunable_backups`] keeps the newest
//! [`BACKUPS_RETAINED`] and returns the rest for deletion, so a long-lived
//! install cannot fill its disk with pre-migration snapshots.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use sqlx::migrate::Migrator;
use sqlx::SqlitePool;

/// Filename infix marking a file as an automatic pre-migration backup:
/// `pumper.db.bak-premigrate-20260726-081548`.
pub const BACKUP_MARKER: &str = ".bak-premigrate-";

/// How many automatic backups are retained; older ones are pruned after a new
/// one is written.
pub const BACKUPS_RETAINED: usize = 3;

/// Why a pre-migration backup was (or was not) taken. A named outcome rather
/// than a bare `bool` so the log line — and the tests — can say *which* skip
/// applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupDecision {
    /// `pending` migrations are about to run against a populated database.
    Take { pending: usize },
    /// In-memory / URI-memory database: there is no file to copy.
    SkipInMemory,
    /// No migration has ever been applied — an empty database is not worth
    /// snapshotting. Covers `TempStore` and the very first server boot.
    SkipFreshDatabase,
    /// Every known migration is already applied; the migrator will be a no-op.
    SkipUpToDate,
}

impl BackupDecision {
    /// True only for [`BackupDecision::Take`].
    pub fn is_take(self) -> bool {
        matches!(self, BackupDecision::Take { .. })
    }
}

/// True when `db_path` names an in-memory SQLite database rather than a file on
/// disk (`:memory:`, `file::memory:`, or any URI carrying `mode=memory`).
pub fn is_in_memory(db_path: &Path) -> bool {
    let Some(raw) = db_path.to_str() else {
        return false;
    };
    raw.is_empty()
        || raw == ":memory:"
        || raw.starts_with("file::memory:")
        || raw.contains("mode=memory")
}

/// Decides whether to snapshot the database before running the migrator.
///
/// `applied` are the versions recorded in `_sqlx_migrations`; `available` are
/// the up-migration versions the embedded [`Migrator`] carries. Pure: all I/O
/// (reading `applied`, writing the copy) happens in
/// [`backup_before_migrations`].
pub fn backup_decision(db_path: &Path, applied: &[i64], available: &[i64]) -> BackupDecision {
    if is_in_memory(db_path) {
        return BackupDecision::SkipInMemory;
    }
    if applied.is_empty() {
        return BackupDecision::SkipFreshDatabase;
    }
    let pending = available.iter().filter(|v| !applied.contains(v)).count();
    if pending == 0 {
        BackupDecision::SkipUpToDate
    } else {
        BackupDecision::Take { pending }
    }
}

/// Backup destination for `db_path` at instant `at`: a sibling of the database
/// named `<db file>.bak-premigrate-<YYYYMMDD-HHMMSS>` (UTC).
///
/// The timestamp is fixed-width and big-endian, matching this repo's timestamp
/// convention, so lexicographic filename order *is* chronological order — which
/// is what makes [`prunable_backups`] a plain sort.
pub fn backup_path(db_path: &Path, at: DateTime<Utc>) -> PathBuf {
    let name = db_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("database.db");
    let stamp = at.format("%Y%m%d-%H%M%S");
    let backup = format!("{name}{BACKUP_MARKER}{stamp}");
    match db_path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.join(backup),
        _ => PathBuf::from(backup),
    }
}

/// True when `candidate` is an automatic backup of `db_path` (and not the
/// database itself, nor an operator's hand-made copy under a different name).
pub fn is_backup_of(db_path: &Path, candidate: &Path) -> bool {
    let (Some(db), Some(file)) = (
        db_path.file_name().and_then(|n| n.to_str()),
        candidate.file_name().and_then(|n| n.to_str()),
    ) else {
        return false;
    };
    file.starts_with(&format!("{db}{BACKUP_MARKER}"))
}

/// The backups to delete: everything past the newest `keep`.
///
/// Input order is irrelevant — the list is sorted by filename, which is
/// chronological (see [`backup_path`]). Returns oldest-first.
pub fn prunable_backups(existing: &[PathBuf], keep: usize) -> Vec<PathBuf> {
    let mut sorted: Vec<PathBuf> = existing.to_vec();
    sorted.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    let drop_count = sorted.len().saturating_sub(keep);
    sorted.into_iter().take(drop_count).collect()
}

/// Versions recorded in `_sqlx_migrations`, or an empty vec when the table does
/// not exist yet (a database no migrator has ever touched).
async fn applied_versions(pool: &SqlitePool) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .unwrap_or_default()
}

/// Up-migration versions carried by `migrator` (down-migrations are not
/// schema-advancing and would double-count).
fn available_versions(migrator: &Migrator) -> Vec<i64> {
    migrator
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| m.version)
        .collect()
}

/// Snapshots the database before `migrator` runs, when [`backup_decision`] says
/// to. Returns the backup path, or `None` when skipped.
///
/// Must be called on a live pool *before* the migrator: the copy is taken by
/// SQLite itself (`VACUUM INTO`) so it is WAL-consistent.
///
/// A backup failure is logged at error level but does **not** abort startup — a
/// read-only or full disk should not make the server unbootable, and the
/// migration it guards is usually additive. The partial output file, if any, is
/// removed so it cannot be mistaken for a usable snapshot.
pub async fn backup_before_migrations(
    pool: &SqlitePool,
    db_path: &Path,
    migrator: &Migrator,
) -> Option<PathBuf> {
    let applied = applied_versions(pool).await;
    let available = available_versions(migrator);
    let decision = backup_decision(db_path, &applied, &available);
    let BackupDecision::Take { pending } = decision else {
        tracing::debug!(?decision, db = %db_path.display(), "pre-migration backup skipped");
        return None;
    };

    let dest = backup_path(db_path, Utc::now());
    if let Err(e) = vacuum_into(pool, &dest).await {
        tracing::error!(
            error = %e,
            backup = %dest.display(),
            "pre-migration backup FAILED; migrating anyway"
        );
        let _ = std::fs::remove_file(&dest);
        return None;
    }
    tracing::info!(
        pending,
        backup = %dest.display(),
        "pre-migration backup written"
    );
    prune_backups(db_path, BACKUPS_RETAINED);
    Some(dest)
}

/// Writes a consistent single-file image of the committed database to `dest`.
async fn vacuum_into(pool: &SqlitePool, dest: &Path) -> sqlx::Result<()> {
    // `VACUUM INTO <expr>` takes the destination as an SQL expression, so the
    // path binds as a parameter — no quoting of operator-supplied paths.
    sqlx::query("VACUUM INTO ?1")
        .bind(dest.to_string_lossy().to_string())
        .execute(pool)
        .await
        .map(|_| ())
}

/// Deletes all but the newest `keep` automatic backups sitting next to
/// `db_path`. Best-effort: unreadable directories and undeletable files are
/// logged, never fatal.
fn prune_backups(db_path: &Path, keep: usize) {
    let Some(dir) = db_path.parent() else {
        return;
    };
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let found: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| is_backup_of(db_path, p))
        .collect();
    for stale in prunable_backups(&found, keep) {
        match std::fs::remove_file(&stale) {
            Ok(()) => tracing::debug!(backup = %stale.display(), "pruned old pre-migration backup"),
            Err(e) => tracing::warn!(error = %e, backup = %stale.display(), "prune failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 26, 8, 15, s).unwrap()
    }

    #[test]
    fn temp_and_memory_databases_are_skipped_not_copied() {
        let all = [1i64, 2, 3];
        assert_eq!(
            backup_decision(Path::new(":memory:"), &[1], &all),
            BackupDecision::SkipInMemory
        );
        assert_eq!(
            backup_decision(Path::new("file::memory:?cache=shared"), &[1], &all),
            BackupDecision::SkipInMemory
        );
        assert_eq!(
            backup_decision(Path::new("file:x.db?mode=memory"), &[1], &all),
            BackupDecision::SkipInMemory
        );
    }

    #[test]
    fn fresh_database_is_skipped_not_backed_up() {
        // A `TempStore`/first-boot database: the file exists (the pool created
        // it) but no migration has been applied, so there is nothing to lose.
        assert_eq!(
            backup_decision(Path::new("/data/pumper.db"), &[], &[1, 2, 3]),
            BackupDecision::SkipFreshDatabase
        );
    }

    #[test]
    fn up_to_date_database_is_skipped_not_backed_up_every_boot() {
        // Every restart re-runs the migrator; without this arm the server would
        // mint a snapshot on each boot.
        assert_eq!(
            backup_decision(Path::new("/data/pumper.db"), &[1, 2, 3], &[1, 2, 3]),
            BackupDecision::SkipUpToDate
        );
        // Extra applied rows (a rolled-back migration file) still count as
        // nothing pending.
        assert_eq!(
            backup_decision(Path::new("/data/pumper.db"), &[1, 2, 3, 4], &[1, 2, 3]),
            BackupDecision::SkipUpToDate
        );
    }

    #[test]
    fn pending_migration_on_populated_database_is_taken_not_skipped() {
        assert_eq!(
            backup_decision(Path::new("/data/pumper.db"), &[1, 2], &[1, 2, 3, 4]),
            BackupDecision::Take { pending: 2 }
        );
        // Out-of-order backfill (an older version arriving late) counts too.
        assert_eq!(
            backup_decision(Path::new("/data/pumper.db"), &[1, 3], &[1, 2, 3]),
            BackupDecision::Take { pending: 1 }
        );
        assert!(backup_decision(Path::new("/data/pumper.db"), &[1], &[1, 2]).is_take());
    }

    #[test]
    fn backup_path_is_a_sibling_not_a_sidecar_overwrite() {
        let p = backup_path(Path::new("/data/pumper.db"), at(48));
        assert_eq!(
            p,
            PathBuf::from("/data/pumper.db.bak-premigrate-20260726-081548")
        );
        // Never collides with the database or its WAL sidecars.
        assert_ne!(p, PathBuf::from("/data/pumper.db"));
        assert!(!p.to_string_lossy().ends_with("-wal"));
    }

    #[test]
    fn backup_path_of_bare_filename_stays_in_cwd_not_root() {
        assert_eq!(
            backup_path(Path::new("pumper.db"), at(48)),
            PathBuf::from("pumper.db.bak-premigrate-20260726-081548")
        );
    }

    #[test]
    fn backup_names_sort_chronologically_not_lexicographically_wrong() {
        let mut names: Vec<String> = [at(9), at(48), at(10)]
            .iter()
            .map(|t| {
                backup_path(Path::new("pumper.db"), *t)
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        names.sort();
        assert!(names[0].ends_with("081509"), "{names:?}");
        assert!(names[2].ends_with("081548"), "{names:?}");
    }

    #[test]
    fn is_backup_of_matches_our_marker_not_the_db_or_hand_made_copies() {
        let db = Path::new("/data/pumper.db");
        assert!(is_backup_of(
            db,
            Path::new("/data/pumper.db.bak-premigrate-20260726-081548")
        ));
        assert!(!is_backup_of(db, db), "the database is not its own backup");
        assert!(
            !is_backup_of(db, Path::new("/data/pumper.db-wal")),
            "WAL sidecar is not a backup"
        );
        assert!(
            !is_backup_of(db, Path::new("/data/pumper.db.bak-simhash-20260715-081548")),
            "operator's hand-made copy must never be pruned"
        );
        assert!(
            !is_backup_of(
                db,
                Path::new("/data/other.db.bak-premigrate-20260726-081548")
            ),
            "another database's backup"
        );
    }

    #[test]
    fn pruning_drops_oldest_not_newest() {
        let files: Vec<PathBuf> = [at(10), at(48), at(9), at(30)]
            .iter()
            .map(|t| backup_path(Path::new("/data/pumper.db"), *t))
            .collect();
        let doomed = prunable_backups(&files, 2);
        assert_eq!(doomed.len(), 2);
        assert!(doomed[0].to_string_lossy().ends_with("081509"));
        assert!(doomed[1].to_string_lossy().ends_with("081510"));
        // The newest two survive.
        assert!(!doomed.contains(&files[1]));
        assert!(!doomed.contains(&files[3]));
    }

    #[test]
    fn pruning_under_the_limit_deletes_nothing() {
        let files: Vec<PathBuf> = [at(10), at(48)]
            .iter()
            .map(|t| backup_path(Path::new("/data/pumper.db"), *t))
            .collect();
        assert!(prunable_backups(&files, BACKUPS_RETAINED).is_empty());
        assert!(prunable_backups(&[], BACKUPS_RETAINED).is_empty());
    }
}
