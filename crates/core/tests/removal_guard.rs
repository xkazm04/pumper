//! The removal seam: who is allowed to tombstone records, and how.
//!
//! Two removal paths exist and they are not interchangeable:
//!
//! - `Datasets::detect_removed` **infers** removals from a full snapshot —
//!   everything live and absent from the batch is tombstoned. A degrading source
//!   producing a short batch therefore erases the tail of its own dataset, which
//!   is why it demands a `RemovalGuard` that only a non-degrading health state
//!   can produce.
//! - `Datasets::tombstone_keys` removes **named** keys. Nothing is inferred, so
//!   there is nothing for a guard to protect against.
//!
//! The guard used to live one layer up in `AppContext::sync_many_with_provenance`
//! and the peer app walked around it. These tests keep both the mechanism and the
//! inventory honest.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pumper_core::config::ResilienceConfig;
use pumper_core::datasets::RemovalGuard;
use pumper_core::resilience::store::Resilience;
use pumper_core::testing::{TempStore, TestContext};
use pumper_core::{AppContext, Datasets, SourceState, Storage};
use serde_json::{json, Value};

fn items(keys: &[&str]) -> Vec<(String, Value)> {
    keys.iter()
        .map(|k| (k.to_string(), json!({ "id": k })))
        .collect()
}

fn ctx(storage: &Storage, health: Arc<Resilience>) -> AppContext {
    TestContext::new(storage, "extractor")
        .health(health)
        .build()
}

// ── the guard itself ────────────────────────────────────────────────────────

#[test]
fn only_a_non_degrading_state_can_mint_a_removal_guard() {
    // The guard is the *only* way to reach removal detection, so this mapping is
    // the whole policy. "Degrading" is unchanged — it is still exactly
    // `SourceState::suppresses_removals`.
    for state in [
        SourceState::Healthy,
        SourceState::Suspect,
        SourceState::Probation,
        SourceState::Retired,
    ] {
        assert!(
            RemovalGuard::for_source_state(state).is_some(),
            "{state:?} does not suppress removals and must yield a guard"
        );
    }
    for state in [SourceState::Degraded, SourceState::Quarantined] {
        assert!(
            RemovalGuard::for_source_state(state).is_none(),
            "{state:?} suppresses removals and must NOT yield a guard"
        );
    }
}

#[tokio::test]
async fn a_suppressed_run_leaves_existing_tombstones_and_live_records_untouched() {
    // The suppressed path must be a true no-op on the record table, not merely
    // "adds no new tombstones": reviving an existing tombstone, or re-stamping
    // one, would fire the change feed for a removal that already happened and
    // would let a degrading source rewrite history it cannot see.
    let store = TempStore::new("removal-guard-suppressed").await;
    let storage = &store.storage;
    let health = Arc::new(Resilience::new(
        storage.pool(),
        &ResilienceConfig {
            enforce: true,
            ..ResilienceConfig::default()
        },
    ));
    let ctx = ctx(storage, Arc::clone(&health));
    let pool = storage.pool();

    // Healthy: three keys, then `c` genuinely disappears and is tombstoned.
    ctx.sync_many("products", &items(&["a", "b", "c"]))
        .await
        .unwrap();
    let removed = ctx
        .sync_many("products", &items(&["a", "b"]))
        .await
        .unwrap()
        .removed;
    assert_eq!(removed, vec!["c".to_string()]);

    let snapshot = |pool: sqlx::SqlitePool| async move {
        let records: Vec<(String, Option<String>, String)> = sqlx::query_as(
            "SELECT key, removed_at, updated_at FROM records \
             WHERE app = 'extractor' AND dataset = 'products' ORDER BY key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let revisions: Vec<(String, i64, String)> = sqlx::query_as(
            "SELECT key, revision, change FROM record_revisions \
             WHERE app = 'extractor' AND dataset = 'products' ORDER BY key, revision",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        (records, revisions)
    };
    let before = snapshot(pool.clone()).await;
    assert!(
        before.0.iter().any(|(k, r, _)| k == "c" && r.is_some()),
        "c must be tombstoned before the degrading run"
    );

    // The source degrades, then a half-broken run returns only `a`.
    let store_h = health.store().unwrap();
    store_h
        .ensure_source("extractor", "products")
        .await
        .unwrap();
    store_h
        .set_state_manual("extractor/products", SourceState::Degraded, "test")
        .await
        .unwrap();
    let summary = ctx.sync_many("products", &items(&["a"])).await.unwrap();
    assert!(summary.removed.is_empty(), "{summary:?}");

    let after = snapshot(pool.clone()).await;
    // `b` stayed live and untouched; `c` stayed tombstoned with its original
    // stamp; no revision was added by the removal path (the upsert of `a` does
    // not touch either key).
    let live_or_dead = |s: &(Vec<(String, Option<String>, String)>, _), key: &str| {
        s.0.iter()
            .find(|(k, _, _)| k == key)
            .map(|(_, r, u)| (r.clone(), u.clone()))
            .unwrap()
    };
    assert_eq!(live_or_dead(&before, "b"), live_or_dead(&after, "b"));
    assert_eq!(live_or_dead(&before, "c"), live_or_dead(&after, "c"));
    assert_eq!(
        before.1, after.1,
        "a suppressed run must append no revision at all"
    );
}

// ── removal by name ─────────────────────────────────────────────────────────

#[tokio::test]
async fn tombstone_keys_removes_only_the_named_live_keys() {
    let store = TempStore::new("removal-guard-by-name").await;
    let ds = Datasets::new(store.storage.pool());
    let pool = store.storage.pool();
    ds.upsert_many("mirror", "d", &items(&["a", "b", "c"]))
        .await
        .unwrap();

    // Named removal: `b` goes, everything else is untouched, and an unknown key
    // and a repeat of a named one are silently skipped rather than fabricating
    // rows or double-tombstoning.
    let removed = ds
        .tombstone_keys("mirror", "d", &["b".into(), "ghost".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(removed, vec!["b".to_string()]);

    let states: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT key, removed_at FROM records ORDER BY key")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(states[0].1, None, "a stays live");
    assert!(states[1].1.is_some(), "b is tombstoned");
    assert_eq!(states[2].1, None, "c stays live");

    // The tombstone carries its `removed` revision — the change-feed signal
    // consumers (watches, triggers, the peer mirror downstream) run on.
    let chain: Vec<String> =
        sqlx::query_scalar("SELECT change FROM record_revisions WHERE key = 'b' ORDER BY revision")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(chain, vec!["new".to_string(), "removed".to_string()]);

    // Idempotent: an already-tombstoned key is not removed twice.
    assert!(ds
        .tombstone_keys("mirror", "d", &["b".into()])
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn tombstone_keys_does_not_infer_anything_from_the_keys_it_is_not_given() {
    // The whole point of the named path: handing it one key must never make it
    // reason about the rest of the dataset the way detect_removed does.
    let store = TempStore::new("removal-guard-no-inference").await;
    let ds = Datasets::new(store.storage.pool());
    ds.upsert_many("mirror", "d", &items(&["a", "b", "c"]))
        .await
        .unwrap();
    assert_eq!(
        ds.tombstone_keys("mirror", "d", &["a".into()])
            .await
            .unwrap(),
        vec!["a".to_string()]
    );
    let live: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records WHERE removed_at IS NULL")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap();
    assert_eq!(live, 2, "b and c must be untouched");
}

// ── inventory ───────────────────────────────────────────────────────────────

/// Production (`src/`) files permitted to call `detect_removed`.
///
/// EXPECTED-diff idiom (see `crates/server/src/routes/mod.rs`): a convention is
/// enforced with a test, not a sentence in a doc. Adding a call site here is a
/// deliberate act that has to be argued for in a diff — which is exactly what
/// did NOT happen when the peer app grew its own upsert + `detect_removed` pair
/// and stepped around the degrading-source guard.
///
/// Test files are out of scope: a test constructing a `RemovalGuard` explicitly
/// is the point of the seam, not a violation of it.
const EXPECTED_DETECT_REMOVED_CALLERS: &[&str] = &[
    // The guarded seam. Every app reaches removal detection through this.
    "crates/core/src/app.rs",
    // The store's own materialized-search-view path, whose snapshot is the view
    // itself and has no external source health to consult.
    "crates/core/src/datasets.rs",
];

#[test]
fn no_crate_calls_detect_removed_outside_the_guarded_seam() {
    let root = workspace_root();
    let mut found: BTreeSet<String> = BTreeSet::new();
    let mut scanned = 0usize;
    for file in rust_sources_under(&root.join("crates")) {
        let relative = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        // `src/` only — tests legitimately mint guards to exercise the seam.
        if !relative.contains("/src/") {
            continue;
        }
        scanned += 1;
        let body = std::fs::read_to_string(&file).expect("read source");
        // A CALL, not a doc mention: `.detect_removed(`. The definition
        // (`fn detect_removed(`) does not match, so the store's own declaration
        // does not count as a call site.
        if body.contains(".detect_removed(") {
            found.insert(relative);
        }
    }
    assert!(
        scanned > 50,
        "inventory scanned only {scanned} files — the walk is broken, not the code"
    );

    let expected: BTreeSet<String> = EXPECTED_DETECT_REMOVED_CALLERS
        .iter()
        .map(|s| s.to_string())
        .collect();
    let unguarded: Vec<_> = found.difference(&expected).collect();
    let stale: Vec<_> = expected.difference(&found).collect();
    assert!(
        unguarded.is_empty(),
        "these files call `detect_removed` outside the guarded seam — route the \
         removal through `AppContext::sync_many` (inferred) or \
         `Datasets::tombstone_keys` (named): {unguarded:?}"
    );
    assert!(
        stale.is_empty(),
        "EXPECTED lists call sites that no longer exist (update it): {stale:?}"
    );
}

fn workspace_root() -> PathBuf {
    // crates/core -> crates -> workspace root
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root above crates/core")
        .to_path_buf()
}

fn rust_sources_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            out.extend(rust_sources_under(&path));
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
    out
}
