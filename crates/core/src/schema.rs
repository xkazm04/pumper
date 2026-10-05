//! What the schema SHOULD be, and how the schema a boot actually got compares
//! to it.
//!
//! The migration chain is replayed at every boot and then *believed*:
//! [`crate::storage::Storage::connect`] ran the migrator and returned `Ok`
//! without ever reading back the schema it produced. The only manifest of
//! "which tables must exist" lived in a test file, where the boot path could
//! not reach it. This module is the post-condition that closes both halves:
//!
//! - [`EXPECTED_TABLES`] is now the one authority, read by the CI replay test
//!   **and** by every real boot. A migration that creates a table without
//!   updating it fails the test *and* refuses the boot.
//! - [`classify`] compares the store's `_sqlx_migrations` ledger against the
//!   versions this binary carries **in both directions**. The old comparison
//!   counted only `available \ applied`, so a store migrated by a NEWER binary
//!   was indistinguishable from a store in sync — and this repo has **zero**
//!   down migrations, so running an older binary IS the only way back, and it
//!   was the one path nothing checked.
//!
//! Everything here is pure: no sqlx, no pool, no feature gate. The caller
//! gathers (`sqlite_master`, `_sqlx_migrations`) and this decides — the same
//! split that makes [`crate::doctor::diagnose`] testable without a database.

use std::collections::BTreeSet;

use serde::Serialize;

/// Every table the full migration chain must leave behind.
///
/// **Add the new table here in the same change that adds the migration that
/// creates it.** This list is load-bearing twice over: `crates/core/tests/
/// migrations.rs` diffs a full 0001→NNNN replay against it, and
/// [`crate::storage::Storage::connect`] refuses to boot a store that is missing
/// one of these. One hand-maintained list, two populations — which is the point
/// of moving it out of the test crate.
pub const EXPECTED_TABLES: &[&str] = &[
    "_sqlx_migrations",
    "api_recipes",
    "body_pins",
    "checkpoints",
    "cost_events",
    "datahub_govern_actions",
    "datahub_govern_levels",
    "derived",
    "doc_fingerprints",
    "field_invariants",
    "field_sketches",
    "golden_docs",
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
    "source_rulesets",
    "source_runs",
    "sources",
    "tier_memory",
    "trigger_runs",
    "triggers",
    "watches",
    "webhook_deliveries",
];

/// How a store's applied-migration ledger stands against the chain this binary
/// carries.
///
/// Five states, not two, because the subtraction runs **both ways**. `Pending`
/// and `Ahead` are the two directions and they have opposite remedies: pending
/// means *this binary has work to do*, ahead means *this binary is the wrong
/// binary*. Collapsing them (which is what counting only `available \ applied`
/// did) lets an old reader boot in front of a moved schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LedgerVerdict {
    /// Nothing applied: a brand-new database. The chain is about to create it
    /// from 0001, and there is nothing to lose or to disagree with.
    Fresh,
    /// The ledger and the binary agree exactly.
    Current,
    /// Versions this binary carries that the store has not applied — ordinary
    /// forward migration.
    Pending { versions: Vec<i64> },
    /// Versions the store has applied that this binary does not carry. A
    /// **downgrade**: a newer pumper moved this store past what this build
    /// knows how to read.
    Ahead { versions: Vec<i64> },
    /// Both at once — the store applied versions this binary lacks *and* lacks
    /// versions this binary carries. A branch merge, a hand-edited ledger, or a
    /// half-restored backup; strictly worse than either alone, and it cannot be
    /// expressed by the two single-direction arms, which is why it is its own.
    Diverged { pending: Vec<i64>, ahead: Vec<i64> },
}

impl LedgerVerdict {
    /// Versions this binary must still apply. Empty for every arm but
    /// `Pending`/`Diverged`.
    pub fn pending(&self) -> &[i64] {
        match self {
            LedgerVerdict::Pending { versions } => versions,
            LedgerVerdict::Diverged { pending, .. } => pending,
            _ => &[],
        }
    }

    /// Versions the store has that this binary does not. **Non-empty means this
    /// binary cannot fully read this store.**
    pub fn ahead(&self) -> &[i64] {
        match self {
            LedgerVerdict::Ahead { versions } => versions,
            LedgerVerdict::Diverged { ahead, .. } => ahead,
            _ => &[],
        }
    }

    /// Short, stable name for logs and the doctor — never parsed back.
    pub fn state(&self) -> &'static str {
        match self {
            LedgerVerdict::Fresh => "fresh",
            LedgerVerdict::Current => "current",
            LedgerVerdict::Pending { .. } => "pending",
            LedgerVerdict::Ahead { .. } => "ahead",
            LedgerVerdict::Diverged { .. } => "diverged",
        }
    }
}

/// Compares a store's applied versions against the versions this binary
/// carries, **in both directions**.
///
/// `applied` are the `_sqlx_migrations` rows; `available` are the embedded
/// up-migration versions. Order of either input is irrelevant; both output
/// lists come back sorted ascending so a log line and a test read the same.
pub fn classify(applied: &[i64], available: &[i64]) -> LedgerVerdict {
    if applied.is_empty() {
        return LedgerVerdict::Fresh;
    }
    let have: BTreeSet<i64> = applied.iter().copied().collect();
    let carries: BTreeSet<i64> = available.iter().copied().collect();
    let pending: Vec<i64> = carries.difference(&have).copied().collect();
    let ahead: Vec<i64> = have.difference(&carries).copied().collect();
    match (pending.is_empty(), ahead.is_empty()) {
        (true, true) => LedgerVerdict::Current,
        (false, true) => LedgerVerdict::Pending { versions: pending },
        (true, false) => LedgerVerdict::Ahead { versions: ahead },
        (false, false) => LedgerVerdict::Diverged { pending, ahead },
    }
}

/// The difference between the tables a store actually has and the ones the
/// chain promises.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TableDrift {
    /// Promised by the chain, absent from the store. **A fault**: the first
    /// query against one of these fails at runtime, far from the cause.
    pub missing: Vec<String>,
    /// Present in the store, not in [`EXPECTED_TABLES`]. **Never a fault** — an
    /// operator's scratch table, a half-finished rebuild scaffold, an index
    /// experiment. Reported, logged, never fatal.
    pub unexpected: Vec<String>,
}

impl TableDrift {
    /// True when the store carries exactly the promised inventory.
    pub fn is_clean(&self) -> bool {
        self.missing.is_empty() && self.unexpected.is_empty()
    }
}

/// Diffs the observed table inventory against `expected`.
///
/// `observed` is `sqlite_master`'s table names with the `sqlite_%` internals
/// already filtered out — the caller does that because it is one `WHERE`
/// clause and keeping it there keeps this function free of SQLite trivia.
pub fn table_drift(observed: &BTreeSet<String>, expected: &[&str]) -> TableDrift {
    let want: BTreeSet<String> = expected.iter().map(|s| (*s).to_string()).collect();
    TableDrift {
        missing: want.difference(observed).cloned().collect(),
        unexpected: observed.difference(&want).cloned().collect(),
    }
}

/// Everything a boot or the doctor can say about one store's schema, in one
/// serializable shape.
///
/// Carried verbatim into `GET /datasets/doctor`'s `schema` block, so the
/// operator-facing report and the boot refusal are derived from the same
/// numbers rather than from two walks of the same tables.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SchemaReport {
    /// Applied migration versions, ascending.
    pub applied: Vec<i64>,
    /// Versions this binary still has to apply.
    pub pending: Vec<i64>,
    /// Versions the store has that this binary does not.
    pub ahead: Vec<i64>,
    /// Promised tables the store does not have.
    pub tables_missing: Vec<String>,
    /// Tables the store has that the chain does not promise.
    pub tables_unexpected: Vec<String>,
    /// The ledger verdict's short name, so a consumer does not re-derive it
    /// from the two lists and get the `diverged` case wrong.
    pub state: &'static str,
}

impl SchemaReport {
    /// Builds the report from the three gathered facts.
    pub fn new(applied: &[i64], available: &[i64], observed: &BTreeSet<String>) -> Self {
        let verdict = classify(applied, available);
        let drift = table_drift(observed, EXPECTED_TABLES);
        let mut sorted = applied.to_vec();
        sorted.sort_unstable();
        Self {
            applied: sorted,
            pending: verdict.pending().to_vec(),
            ahead: verdict.ahead().to_vec(),
            tables_missing: drift.missing,
            tables_unexpected: drift.unexpected,
            state: verdict.state(),
        }
    }

    /// True when nothing here is a fault.
    ///
    /// `tables_unexpected` is deliberately **not** consulted: an extra table is
    /// not a promise the store is failing to keep. Pending versions are not a
    /// fault either — on a real boot they are about to be applied.
    pub fn is_sound(&self) -> bool {
        self.ahead.is_empty() && self.tables_missing.is_empty()
    }
}

/// The sentence a boot prints when the store is ahead of this binary.
///
/// Extracted so the refusal and its test assert the same text, and so the
/// remedy travels with the refusal: a refusal that names no reachable remedy is
/// half a refusal.
pub fn ahead_refusal(db: &str, ahead: &[i64], carries_max: Option<i64>) -> String {
    let versions = join_versions(ahead);
    let carried = match carries_max {
        Some(v) => format!("this build's chain ends at {v}"),
        None => "this build carries no migrations".to_string(),
    };
    format!(
        "the store at {db} was migrated by a NEWER pumper: _sqlx_migrations records \
         version(s) {versions} that this binary does not carry ({carried}). There are no down \
         migrations, so this build cannot undo them and would read tables and columns it does \
         not understand. Remedy: run the newer binary against this store, or restore the \
         pre-migration snapshot written beside it (`<database>.bak-premigrate-*`). To boot \
         anyway — accepting that newer columns are invisible to this build and that its writes \
         may violate constraints it cannot see — set `[storage] allow_schema_ahead = true`."
    )
}

/// The sentence a boot prints when the chain settled but a promised table is
/// gone.
pub fn missing_tables_refusal(db: &str, missing: &[String]) -> String {
    let names = missing.join(", ");
    format!(
        "the migration chain finished against {db} but the store is missing table(s) that \
         the chain promises: {names}. The ledger says the migration ran, so this is divergence \
         below the ledger — an interrupted migration, a hand-restored partial copy, or a table \
         dropped out of band — and the first query against it would have failed at runtime with \
         no hint of the cause. Remedy: restore the pre-migration snapshot written beside the \
         database (`<database>.bak-premigrate-*`) and re-run so the chain replays, or move the \
         database aside and let it rebuild from 0001. `GET /datasets/doctor` reports the full \
         inventory on a store that does boot."
    )
}

fn join_versions(versions: &[i64]) -> String {
    versions
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn fresh_store_is_not_pending_everything() {
        assert_eq!(classify(&[], &[1, 2, 3]), LedgerVerdict::Fresh);
        assert_eq!(classify(&[], &[]), LedgerVerdict::Fresh);
    }

    #[test]
    fn in_sync_store_is_current() {
        assert_eq!(classify(&[1, 2, 3], &[1, 2, 3]), LedgerVerdict::Current);
        // Input order must not matter — the ledger is read ORDER BY version,
        // but nothing in the type enforces that.
        assert_eq!(classify(&[3, 1, 2], &[2, 3, 1]), LedgerVerdict::Current);
    }

    #[test]
    fn pending_names_the_versions_the_store_lacks() {
        let applied: Vec<i64> = (1..=43).collect();
        let available: Vec<i64> = (1..=44).collect();
        assert_eq!(
            classify(&applied, &available),
            LedgerVerdict::Pending { versions: vec![44] }
        );
        assert_eq!(classify(&applied, &available).ahead(), &[] as &[i64]);
    }

    /// THE case. This exact input was asserted to be "up to date" — see the
    /// strengthened guard in `crate::backup`.
    #[test]
    fn applied_ahead_of_available_is_ahead_not_current() {
        let applied: Vec<i64> = (1..=44).collect();
        let available: Vec<i64> = (1..=43).collect();
        let verdict = classify(&applied, &available);
        assert_eq!(verdict, LedgerVerdict::Ahead { versions: vec![44] });
        assert_ne!(verdict, LedgerVerdict::Current);
        assert!(verdict.pending().is_empty(), "nothing to apply, and yet");
        assert_eq!(verdict.ahead(), &[44]);
    }

    #[test]
    fn both_directions_at_once_is_diverged_not_either_one() {
        let verdict = classify(&[1, 2, 4], &[1, 2, 3]);
        assert_eq!(
            verdict,
            LedgerVerdict::Diverged {
                pending: vec![3],
                ahead: vec![4],
            }
        );
        // Both halves survive — collapsing to one arm is what lost the Ahead
        // direction in the first place.
        assert_eq!(verdict.pending(), &[3]);
        assert_eq!(verdict.ahead(), &[4]);
    }

    #[test]
    fn missing_table_is_a_fault_and_an_extra_one_is_not() {
        let drift = table_drift(&tables(&["jobs", "records"]), &["jobs", "records"]);
        assert!(drift.is_clean());

        let drift = table_drift(&tables(&["jobs"]), &["jobs", "records"]);
        assert_eq!(drift.missing, vec!["records".to_string()]);
        assert!(drift.unexpected.is_empty());

        let drift = table_drift(&tables(&["jobs", "scratch"]), &["jobs"]);
        assert!(drift.missing.is_empty());
        assert_eq!(drift.unexpected, vec!["scratch".to_string()]);
    }

    /// An operator's scratch table must never make a store unbootable. The
    /// whole refusal hangs on `is_sound`, so this is where that promise lives.
    #[test]
    fn report_is_sound_despite_unexpected_tables() {
        let mut observed = tables(EXPECTED_TABLES);
        observed.insert("operator_scratch".into());
        let report = SchemaReport::new(&[1, 2], &[1, 2], &observed);
        assert_eq!(
            report.tables_unexpected,
            vec!["operator_scratch".to_string()]
        );
        assert!(report.tables_missing.is_empty());
        assert!(
            report.is_sound(),
            "an extra table is not a promise the store is failing to keep"
        );
        assert_eq!(report.state, "current");
    }

    #[test]
    fn report_is_unsound_when_ahead_or_missing() {
        let ahead = SchemaReport::new(&[1, 2, 3], &[1, 2], &tables(EXPECTED_TABLES));
        assert!(!ahead.is_sound());
        assert_eq!(ahead.ahead, vec![3]);
        assert_eq!(ahead.state, "ahead");

        let mut short: Vec<&str> = EXPECTED_TABLES.to_vec();
        short.retain(|t| *t != "golden_docs");
        let gone = SchemaReport::new(&[1], &[1], &tables(&short));
        assert!(!gone.is_sound());
        assert_eq!(gone.tables_missing, vec!["golden_docs".to_string()]);
    }

    /// Every refusal names a reachable remedy, or it is half a refusal.
    #[test]
    fn refusals_name_the_remedy_not_only_the_fault() {
        let ahead = ahead_refusal("data/pumper.db", &[9999], Some(45));
        assert!(ahead.contains("9999"), "names the offending version");
        assert!(ahead.contains("allow_schema_ahead"), "names the key");
        assert!(ahead.contains("bak-premigrate"), "names the way back");

        let missing = missing_tables_refusal("data/pumper.db", &["golden_docs".into()]);
        assert!(missing.contains("golden_docs"));
        assert!(missing.contains("bak-premigrate"));
        assert!(missing.contains("doctor"));
    }

    /// The manifest is hand-maintained, so the cheap structural mistakes (a
    /// duplicate after a merge, an unsorted insert that hides one) get a guard.
    #[test]
    fn expected_tables_is_sorted_and_unique() {
        let set: BTreeSet<&&str> = EXPECTED_TABLES.iter().collect();
        assert_eq!(
            set.len(),
            EXPECTED_TABLES.len(),
            "duplicate entry in EXPECTED_TABLES"
        );
        assert!(
            EXPECTED_TABLES.contains(&"_sqlx_migrations"),
            "the ledger table is part of the inventory a boot checks"
        );
    }
}
