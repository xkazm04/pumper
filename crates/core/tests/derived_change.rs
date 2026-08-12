//! The derived-path change-detection seam (`DerivedPaths`).
//!
//! A producer may write a block **joined from another dataset** into its own
//! records — eu-sedia embeds cordis's `topic_stats` into every Horizon topic as
//! `history` — and hashing the whole value then made the *joined* dataset's
//! cadence look like a change at the source. Watches, triggers, webhooks, the
//! revision trail and the yield ledger all read that churn as a real
//! publication.
//!
//! The seam narrows the change-detection hash and nothing else. These tests pin
//! both halves: that a declared path stops firing change detection, and that a
//! producer declaring nothing is *byte-identical* to what the store did before —
//! which is the entire safety argument for touching a shared write path.

use pumper_core::testing::TempStore;
use pumper_core::{Datasets, DerivedPaths, Provenance};
use serde_json::{json, Value};

/// A SEDIA-shaped topic: real source fields, plus the derived join block.
fn topic(title: &str, funded: u64) -> Value {
    json!({
        "identifier": "HORIZON-CL4-2026-DATA-01",
        "title": title,
        "deadlineDate": ["2026-09-17T17:00:00Z"],
        "history": {
            "family": "HORIZON-CL4-DATA-01",
            "source": "cordis",
            "as_of": "2026-08-12T07:00:00Z",
            "stats": { "project_count": funded },
        },
    })
}

fn items(v: Value) -> Vec<(String, Value)> {
    vec![("HORIZON-CL4-2026-DATA-01".to_string(), v)]
}

fn history() -> DerivedPaths {
    DerivedPaths::new(["history"])
}

async fn write(ds: &Datasets, v: Value, derived: &DerivedPaths) -> pumper_core::UpsertSummary {
    ds.upsert_many_derived("eu-sedia", "opportunities", &items(v), None, None, derived)
        .await
        .expect("write")
}

async fn stored(ds: &Datasets) -> pumper_core::Record {
    ds.get("eu-sedia", "opportunities", "HORIZON-CL4-2026-DATA-01")
        .await
        .expect("read")
        .expect("record exists")
}

async fn revisions(ds: &Datasets) -> usize {
    ds.history("eu-sedia", "opportunities", "HORIZON-CL4-2026-DATA-01", 100)
        .await
        .expect("history")
        .len()
}

/// THE anti-pattern. cordis runs weekly and rewrites its rollup; eu-sedia runs
/// daily and re-embeds it. Without the seam, the next eu-sedia run reported
/// every joined Horizon topic `changed` — a webhook, a trigger and a yield-ledger
/// line for a SEDIA publication that never happened.
#[tokio::test]
async fn derived_churn_does_not_mark_the_source_record_changed() {
    let store = TempStore::new("derived-change-churn").await;
    let ds = store.datasets();

    let first = write(&ds, topic("AI & Robotics", 3), &history()).await;
    assert_eq!(first.new.len(), 1);

    // A weekly cordis rollup moves. Nothing at SEDIA moved.
    let second = write(&ds, topic("AI & Robotics", 4), &history()).await;
    assert_eq!(
        second.changed.len(),
        0,
        "derived-only movement is not a change"
    );
    assert_eq!(second.unchanged, 1);
    assert_eq!(
        revisions(&ds).await,
        1,
        "the change feed must not learn about a join's cadence"
    );

    // …and readers still get the CURRENT derived data: the record body is
    // refreshed even though no revision was appended. Serving a stale join
    // would trade one dishonesty for another.
    let rec = stored(&ds).await;
    assert_eq!(rec.data["history"]["stats"]["project_count"], 4);

    // A real SEDIA field moving still fires, with the full value in the revision.
    let third = write(&ds, topic("AI & Robotics — Phase II", 4), &history()).await;
    assert_eq!(
        third.changed.len(),
        1,
        "a real field change must still fire"
    );
    assert_eq!(revisions(&ds).await, 2);
    let latest = ds
        .history("eu-sedia", "opportunities", "HORIZON-CL4-2026-DATA-01", 1)
        .await
        .unwrap();
    assert_eq!(
        latest[0].data.as_ref().unwrap()["history"]["stats"]["project_count"],
        4,
        "revisions store the FULL value; this is a hash seam, not a projection"
    );
}

/// The safety argument, asserted rather than assumed: declaring nothing hashes
/// exactly what the store always hashed. If this ever diverges, every producer
/// in the fleet re-writes its whole corpus once.
#[tokio::test]
async fn declaring_no_derived_paths_is_byte_identical_to_the_plain_upsert() {
    let store = TempStore::new("derived-change-identity").await;
    let ds = store.datasets();
    let value = topic("AI & Robotics", 3);

    // Written through the plain path…
    ds.upsert_many("plain", "d", &items(value.clone()))
        .await
        .unwrap();
    // …and through the derived path declaring nothing.
    ds.upsert_many_derived(
        "declared",
        "d",
        &items(value.clone()),
        None,
        None,
        &DerivedPaths::NONE,
    )
    .await
    .unwrap();
    // …and through every other batch entry point.
    ds.upsert_many_trusted("trusted", "d", &items(value.clone()), None)
        .await
        .unwrap();
    ds.upsert_many_stamped(
        "stamped",
        "d",
        &items(value),
        None,
        Some(&Provenance::default()),
    )
    .await
    .unwrap();

    let hash_of = |app: &'static str| {
        let pool = store.storage.pool();
        async move {
            sqlx::query_scalar::<_, String>(
                "SELECT hash FROM records WHERE app = ?1 AND dataset = 'd'",
            )
            .bind(app)
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let plain = hash_of("plain").await;
    for app in ["declared", "trusted", "stamped"] {
        assert_eq!(
            hash_of(app).await,
            plain,
            "'{app}' must hash exactly what the plain batch upsert hashes"
        );
    }
}

/// Absence is not an error, and a declared path that never appears must not
/// change anything about how the record hashes.
#[tokio::test]
async fn an_absent_derived_path_is_a_no_op_not_a_different_hash() {
    let store = TempStore::new("derived-change-absent").await;
    let ds = store.datasets();
    let bare = json!({ "identifier": "X", "title": "T" });

    ds.upsert_many("plain", "d", &[("X".to_string(), bare.clone())])
        .await
        .unwrap();
    // Declaring paths that are absent, nested, and impossible (a scalar's
    // "field") — all no-ops.
    let derived = DerivedPaths::new(["history", "history.stats", "title.nope", "", "a.b.c"]);
    let out = ds
        .upsert_many_derived(
            "declared",
            "d",
            &[("X".to_string(), bare)],
            None,
            None,
            &derived,
        )
        .await
        .unwrap();
    assert_eq!(out.new.len(), 1);

    let pool = store.storage.pool();
    let hashes: Vec<String> =
        sqlx::query_scalar("SELECT hash FROM records WHERE dataset = 'd' ORDER BY app")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(hashes[0], hashes[1], "an absent path cannot move the hash");
}

/// Removal semantics are untouched: a derived-path producer still tombstones
/// through a full-snapshot sync, and the revived record is still `Changed` even
/// when its content matches — the tombstone, not the hash, decides that.
#[tokio::test]
async fn removal_and_revival_semantics_are_untouched_by_the_seam() {
    let store = TempStore::new("derived-change-removal").await;
    let ds = store.datasets();
    let guard = pumper_core::datasets::RemovalGuard::for_source_state(
        pumper_core::resilience::SourceState::Healthy,
    )
    .expect("healthy source permits removals");

    write(&ds, topic("T", 3), &history()).await;
    // A non-empty snapshot that no longer lists our key (`detect_removed`
    // refuses an EMPTY batch outright — that guard is unrelated to this seam).
    let removed = ds
        .detect_removed(
            "eu-sedia",
            "opportunities",
            &["SOME-OTHER-TOPIC".to_string()],
            guard,
        )
        .await
        .expect("detect");
    assert_eq!(removed.len(), 1, "a full-snapshot sync still tombstones");
    assert!(stored(&ds).await.removed_at.is_some());

    // Re-writing the SAME value (derived block included) revives it as Changed.
    let back = write(&ds, topic("T", 3), &history()).await;
    assert_eq!(
        back.changed.len(),
        1,
        "a revived tombstone is always Changed"
    );
    assert!(stored(&ds).await.removed_at.is_none());
}

/// The one-time transition: records whose stored hash was computed over the
/// full value (derived block included) re-hash once when the producer adopts the
/// seam, report `changed` that one time, and settle.
#[tokio::test]
async fn adopting_the_seam_costs_exactly_one_changed_run_then_settles() {
    let store = TempStore::new("derived-change-transition").await;
    let ds = store.datasets();

    // Pre-upgrade: hashed over everything.
    write(&ds, topic("T", 3), &DerivedPaths::NONE).await;
    // First post-upgrade run, same value: the hash input narrowed, so the record
    // re-hashes once.
    let upgrade = write(&ds, topic("T", 3), &history()).await;
    assert_eq!(upgrade.changed.len(), 1, "bounded, one-time re-hash");
    // Every run after that is quiet, derived movement included.
    let after = write(&ds, topic("T", 9), &history()).await;
    assert_eq!(after.changed.len(), 0);
    assert_eq!(after.unchanged, 1);
    assert_eq!(revisions(&ds).await, 2, "no third revision");
}

/// A key written twice inside ONE batch still resolves in order — the case the
/// batched planner exists to get right — with the derived seam on.
#[tokio::test]
async fn a_key_written_twice_in_one_batch_still_resolves_in_order() {
    let store = TempStore::new("derived-change-twice").await;
    let ds = store.datasets();
    let key = "HORIZON-CL4-2026-DATA-01".to_string();

    let out = ds
        .upsert_many_derived(
            "eu-sedia",
            "opportunities",
            &[
                (key.clone(), topic("first", 1)),
                // Second occurrence: only the derived block moved.
                (key.clone(), topic("first", 2)),
            ],
            None,
            None,
            &history(),
        )
        .await
        .unwrap();
    assert_eq!(out.new.len(), 1);
    assert_eq!(out.changed.len(), 0, "the second is derived-only");
    assert_eq!(out.unchanged, 1);
    assert_eq!(revisions(&ds).await, 1);
    // The LAST write's body wins, derived block included.
    assert_eq!(
        stored(&ds).await.data["history"]["stats"]["project_count"],
        2
    );
}
