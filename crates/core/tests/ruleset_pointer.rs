//! Stage 2 of the repair plane: the per-source ruleset POINTER
//! (`source_rulesets`), canonical ruleset identity, body pins, and the
//! per-row provenance write path an enrolled source needs.
//!
//! The pointer is the only mutable part — ruleset bodies stay content-addressed
//! in `rules_versions` — so these tests hold the pointer to its contract: one
//! row per move, strictly increasing epochs, `sources.active_rules_hash`
//! following the head, zero writes on every refusal, and never creating a
//! source row it was not given.

use pumper_core::datasets::{
    body_sha256, canonical_rules_hash, group_by_trust, next_activation_ts, pinned_body_path,
    pinned_body_ref, resolve_rules_prefix, rules_hash, shared_provenance, BodyPin, RulesetError,
    RulesetOrigin,
};
use pumper_core::testing::TempStore;
use pumper_core::{Datasets, HealthStore, Provenance, RuleSet};
use serde_json::{json, Value};

const SOURCE: &str = "extractor/d";

async fn with_source(store: &TempStore) -> Datasets {
    HealthStore::new(store.storage.pool(), 1)
        .ensure_source("extractor", "d")
        .await
        .unwrap();
    store.datasets()
}

fn ruleset(v: Value) -> RuleSet {
    serde_json::from_value(v).unwrap()
}

async fn pointer_rows(store: &TempStore) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM source_rulesets")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap()
}

async fn active_column(store: &TempStore) -> Option<String> {
    sqlx::query_scalar("SELECT active_rules_hash FROM sources WHERE id = ?1")
        .bind(SOURCE)
        .fetch_one(&store.storage.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn two_spellings_of_one_ruleset_share_a_hash() {
    // The same rule, spelled with and without the defaults the parser fills in.
    let terse = json!({"h": {"type": "css", "selector": "h1"}});
    let verbose = json!({"h": {"type": "css", "selector": "h1", "attr": null, "all": false,
                               "html": false, "transforms": []}});
    assert_ne!(
        rules_hash(&terse),
        rules_hash(&verbose),
        "control: the RAW hash does tell the spellings apart"
    );
    let a = canonical_rules_hash(&ruleset(terse.clone()));
    let b = canonical_rules_hash(&ruleset(verbose));
    assert_eq!(a, b, "one ruleset, one identity");
    // A different ruleset is still a different identity.
    assert_ne!(
        a,
        canonical_rules_hash(&ruleset(json!({"h": {"type": "css", "selector": "h2"}})))
    );

    // Registration stores the canonical JSON under the canonical hash, and a
    // raw hash registered the old way stays resolvable beside it.
    let store = TempStore::new("rules-canonical").await;
    let ds = store.datasets();
    let old = ds.register_rules(&terse).await.unwrap();
    let new = ds.register_ruleset(&ruleset(terse.clone())).await.unwrap();
    assert_eq!(new, a);
    assert_eq!(ds.rules_by_hash(&old).await.unwrap(), Some(terse));
    let stored = ds.rules_by_hash(&new).await.unwrap().expect("registered");
    assert_eq!(
        rules_hash(&stored),
        new,
        "stored JSON hashes to its own key"
    );
}

#[test]
fn a_short_or_ambiguous_prefix_is_refused_not_guessed() {
    let lineage = ["aaaaaaaa11112222", "aaaaaaaa33334444", "bbbbbbbbcccc"];
    // Unique prefix, and a full hash, resolve.
    assert_eq!(
        resolve_rules_prefix(&lineage, "bbbbbbbb").unwrap(),
        "bbbbbbbbcccc"
    );
    assert_eq!(
        resolve_rules_prefix(&lineage, "AAAAAAAA1111").unwrap(),
        "aaaaaaaa11112222",
        "matching is case-insensitive"
    );
    // < 8 hex → BadRef (400), even when it would be unique.
    assert!(matches!(
        resolve_rules_prefix(&lineage, "bbbb"),
        Err(RulesetError::BadRef(_))
    ));
    // Not hex → BadRef.
    assert!(matches!(
        resolve_rules_prefix(&lineage, "zzzzzzzzzz"),
        Err(RulesetError::BadRef(_))
    ));
    // Ambiguous → BadRef, never "the newest wins".
    assert!(matches!(
        resolve_rules_prefix(&lineage, "aaaaaaaa"),
        Err(RulesetError::BadRef(m)) if m.contains("ambiguous")
    ));
    // Unknown → Unknown (404).
    assert!(matches!(
        resolve_rules_prefix(&lineage, "cccccccc"),
        Err(RulesetError::Unknown(_))
    ));
    // A hash appearing twice in the lineage (activated, rolled away, rolled
    // back) is ONE ruleset, not an ambiguity.
    assert_eq!(
        resolve_rules_prefix(&["dddddddd01", "dddddddd01"], "dddddddd").unwrap(),
        "dddddddd01"
    );
}

#[test]
fn activation_timestamps_increase_even_when_the_clock_does_not() {
    let now = chrono::Utc::now();
    let same = pumper_core::datasets::ts(now);
    let next = next_activation_ts(now, Some(&same));
    assert!(next > same, "{next} must sort after {same}");
    // A clock that stepped backwards still yields a later stamp.
    let future = pumper_core::datasets::ts(now + chrono::Duration::seconds(5));
    assert!(next_activation_ts(now, Some(&future)) > future);
    // A normal clock is used as is.
    let past = pumper_core::datasets::ts(now - chrono::Duration::seconds(5));
    assert_eq!(next_activation_ts(now, Some(&past)), same);
    assert_eq!(next_activation_ts(now, None), same);
}

#[tokio::test]
async fn activation_appends_one_row_and_epochs_strictly_increase() {
    let store = TempStore::new("rules-pointer").await;
    let ds = with_source(&store).await;
    let r1 = ds
        .register_ruleset(&ruleset(json!({"h": {"type": "css", "selector": "h1"}})))
        .await
        .unwrap();
    let r2 = ds
        .register_ruleset(&ruleset(json!({"h": {"type": "css", "selector": "h2"}})))
        .await
        .unwrap();

    assert_eq!(ds.ruleset_head(SOURCE).await.unwrap(), None);
    assert_eq!(active_column(&store).await, None);

    // Import: the first head.
    let first = ds.import_ruleset(SOURCE, &r1).await.unwrap();
    assert_eq!((first.epoch, first.origin.as_str()), (1, "import"));
    assert_eq!(first.parent_hash, None);
    assert_eq!(first.licence_at_move, "none");
    assert_eq!(pointer_rows(&store).await, 1);
    assert_eq!(active_column(&store).await.as_deref(), Some(r1.as_str()));

    // A second import is a conflict, with zero writes.
    assert!(matches!(
        ds.import_ruleset(SOURCE, &r2).await,
        Err(RulesetError::Conflict(_))
    ));
    assert_eq!(pointer_rows(&store).await, 1);

    // Human activation: exactly one more row, parent = old head.
    let second = ds
        .activate_ruleset(SOURCE, &r2, RulesetOrigin::Human)
        .await
        .unwrap();
    assert_eq!(second.epoch, 2);
    assert_eq!(second.parent_hash.as_deref(), Some(r1.as_str()));
    assert_eq!(pointer_rows(&store).await, 2);
    assert_eq!(active_column(&store).await.as_deref(), Some(r2.as_str()));

    // Re-activating the head mints nothing.
    assert!(matches!(
        ds.activate_ruleset(SOURCE, &r2, RulesetOrigin::Human).await,
        Err(RulesetError::Conflict(_))
    ));
    assert_eq!(pointer_rows(&store).await, 2);

    // Rollback by prefix, resolved in the lineage: one more row.
    let to = ds.resolve_rules_ref(SOURCE, &r1[..10]).await.unwrap();
    assert_eq!(to, r1);
    let third = ds
        .activate_ruleset(SOURCE, &to, RulesetOrigin::Rollback)
        .await
        .unwrap();
    assert_eq!((third.epoch, third.origin.as_str()), (3, "rollback"));
    assert_eq!(pointer_rows(&store).await, 3);
    assert_eq!(active_column(&store).await.as_deref(), Some(r1.as_str()));
    assert_eq!(
        ds.resolve_rules_ref(SOURCE, "active").await.unwrap(),
        r1,
        "`active` follows the head"
    );

    // Lineage: newest first, epochs strictly decreasing, exactly one live row.
    let lineage = ds.ruleset_lineage(SOURCE).await.unwrap();
    let epochs: Vec<i64> = lineage.iter().map(|h| h.epoch).collect();
    assert_eq!(epochs, vec![3, 2, 1]);
    let live: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM source_rulesets WHERE source_id = ?1 AND retired_at IS NULL",
    )
    .bind(SOURCE)
    .fetch_one(&store.storage.pool())
    .await
    .unwrap();
    assert_eq!(live, 1, "every superseded head is retired");
    assert_eq!(ds.ruleset_head(SOURCE).await.unwrap(), Some(third));
}

#[tokio::test]
async fn activating_an_unknown_source_fails_and_never_creates_a_row() {
    let store = TempStore::new("rules-unknown-source").await;
    let ds = store.datasets();
    let r = ds
        .register_ruleset(&ruleset(json!({"h": {"type": "css", "selector": "h1"}})))
        .await
        .unwrap();
    for outcome in [
        ds.import_ruleset(SOURCE, &r).await,
        ds.activate_ruleset(SOURCE, &r, RulesetOrigin::Human).await,
    ] {
        assert!(matches!(outcome, Err(RulesetError::Unknown(m)) if m.contains("unknown source")));
    }
    let sources: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap();
    assert_eq!(sources, 0, "the pointer must never create a source row");
    assert_eq!(pointer_rows(&store).await, 0);

    // An unregistered hash is refused too, even for a known source.
    let ds = with_source(&store).await;
    assert!(matches!(
        ds.import_ruleset(SOURCE, &"f".repeat(64)).await,
        Err(RulesetError::Unknown(_))
    ));
    // `active` with no head is a named Unknown, not an empty string.
    assert!(matches!(
        ds.resolve_rules_ref(SOURCE, "active").await,
        Err(RulesetError::Unknown(_))
    ));
}

#[test]
fn pinned_body_path_refuses_unsafe_segments_not_just_odd_ones() {
    let root = std::path::Path::new("/data/artifacts");
    let sha = body_sha256("<h1>x</h1>");
    let p = pinned_body_path(root, "extractor", &sha).expect("safe address");
    assert!(
        p.ends_with(format!("extractor/bodies/{sha}.html"))
            || p.ends_with(format!("extractor\\bodies\\{sha}.html"))
    );
    assert_eq!(p, pinned_body_ref("extractor", &sha).path(root));
    // A stored sha that is not 64 hex could carry a path.
    assert_eq!(
        pinned_body_path(root, "extractor", "../../etc/passwd"),
        None
    );
    assert_eq!(pinned_body_path(root, "extractor", "abc"), None);
    // An app segment with a separator.
    assert_eq!(pinned_body_path(root, "../x", &sha), None);
    assert_eq!(pinned_body_path(root, "a/b", &sha), None);
}

fn pin(sha: &str, obs: &str, key: &str, at: &str) -> BodyPin {
    BodyPin {
        body_sha: sha.to_string(),
        observation_id: obs.to_string(),
        source_id: SOURCE.to_string(),
        job_id: Some("job".into()),
        document_key: Some(key.to_string()),
        original_url: Some(key.to_string()),
        captured_at: at.to_string(),
    }
}

#[tokio::test]
async fn a_pinned_body_is_kept_while_a_revision_cites_it_and_reclaimable_after() {
    let store = TempStore::new("rules-pin-retention").await;
    let ds = store.datasets();
    let cited = body_sha256("cited");
    let orphan = body_sha256("orphan");
    ds.pin_bodies(&[
        pin(&cited, "j:a", "https://a/", "2026-01-01T00:00:00.000000Z"),
        pin(&orphan, "j:b", "https://b/", "2026-01-01T00:00:00.000000Z"),
    ])
    .await
    .unwrap();
    // Only `cited` is referenced by a replayable revision.
    let prov = Provenance {
        job_id: Some("j".into()),
        source_url: Some("https://a/".into()),
        artifact_sha: Some(cited.clone()),
        rules_hash: Some("r".into()),
    };
    ds.upsert_stamped(
        "extractor",
        "d",
        "https://a/",
        &json!({"v": 1}),
        None,
        Some(&prov),
    )
    .await
    .unwrap();

    // The retention scanner addresses the on-disk file exactly like the veto.
    let root = store.path().join("artifacts");
    for sha in [&cited, &orphan] {
        let path = pinned_body_path(&root, "extractor", sha).unwrap();
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "x").unwrap();
    }
    let scanned: Vec<_> = pumper_core::retention::scan_artifact_tree(&root)
        .into_iter()
        .map(|f| f.reference)
        .collect();
    assert!(scanned.contains(&pinned_body_ref("extractor", &cited)));

    let pinned = ds.pinned_artifact_refs().await.unwrap();
    assert!(
        pinned.contains(&pinned_body_ref("extractor", &cited)),
        "a body a revision cites must be vetoed from reclamation"
    );
    assert!(
        !pinned.contains(&pinned_body_ref("extractor", &orphan)),
        "a pin nothing cites stays reclaimable — retention narrowed, not disabled"
    );
}

#[tokio::test]
async fn latest_pin_per_document_wins_and_identical_bodies_are_two_observations() {
    let store = TempStore::new("rules-pin-latest").await;
    let ds = store.datasets();
    let one = body_sha256("one");
    let two = body_sha256("two");
    ds.pin_bodies(&[
        pin(&one, "j1:a", "https://a/", "2026-01-01T00:00:00.000000Z"),
        pin(&two, "j2:a", "https://a/", "2026-01-02T00:00:00.000000Z"),
        // Same body as the first, a different document.
        pin(&one, "j1:b", "https://b/", "2026-01-01T00:00:00.000000Z"),
    ])
    .await
    .unwrap();
    let latest = ds.latest_body_pins(SOURCE, 100).await.unwrap();
    let got: Vec<(&str, &str)> = latest
        .iter()
        .map(|p| (p.document_key.as_deref().unwrap(), p.body_sha.as_str()))
        .collect();
    assert_eq!(
        got,
        vec![("https://a/", two.as_str()), ("https://b/", one.as_str())]
    );
    assert_eq!(ds.body_pin_count(SOURCE).await.unwrap(), 3);
    // Two documents share `one`; the revision's own URL picks its pin.
    let p = ds
        .body_pin(&one, Some("https://b/"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.observation_id, "j1:b");
    let p = ds
        .body_pin(&one, Some("https://a/"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.observation_id, "j1:a");
}

#[tokio::test]
async fn per_row_stamps_land_on_each_revision_not_one_shared_stamp() {
    let store = TempStore::new("rules-row-stamped").await;
    let ds = store.datasets();
    let stamp = |url: &str, sha: &str| Provenance {
        job_id: Some("job-1".into()),
        source_url: Some(url.into()),
        artifact_sha: Some(sha.into()),
        rules_hash: Some("rh".into()),
    };
    let rows = vec![
        ("k1".to_string(), json!({"v": 1}), stamp("https://a/", "sa")),
        ("k2".to_string(), json!({"v": 2}), stamp("https://b/", "sb")),
    ];
    let s = ds
        .upsert_many_row_stamped("extractor", "d", &rows, Some("provisional"))
        .await
        .unwrap();
    assert_eq!(s.new, vec!["k1", "k2"]);
    for (key, url, sha) in [("k1", "https://a/", "sa"), ("k2", "https://b/", "sb")] {
        let rev = ds
            .history("extractor", "d", key, 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(rev.provenance.source_url.as_deref(), Some(url));
        assert_eq!(rev.provenance.artifact_sha.as_deref(), Some(sha));
        assert_eq!(rev.provenance.rules_hash.as_deref(), Some("rh"));
        assert_eq!(rev.trust, "provisional");
    }
    // Same values again: unchanged, no revision appended (single-row
    // semantics), trust refreshed.
    let s = ds
        .upsert_many_row_stamped("extractor", "d", &rows, None)
        .await
        .unwrap();
    assert_eq!((s.new.len(), s.changed.len(), s.unchanged), (0, 0, 2));
    assert_eq!(
        ds.history("extractor", "d", "k1", 10).await.unwrap().len(),
        1
    );
    assert_eq!(
        ds.get("extractor", "d", "k1").await.unwrap().unwrap().trust,
        "stable"
    );

    // The derived-write stamp is only what every row agrees on.
    let shared = shared_provenance(&rows);
    assert_eq!(shared.job_id.as_deref(), Some("job-1"));
    assert_eq!(shared.rules_hash.as_deref(), Some("rh"));
    assert_eq!(
        shared.source_url, None,
        "rows disagree: unknown, not one row's URL"
    );
    assert_eq!(shared.artifact_sha, None);
}

#[test]
fn trust_groups_keep_order_and_fold_stable_into_null() {
    let items = vec![
        ("a".to_string(), json!(1)),
        ("b".to_string(), json!(2)),
        ("c".to_string(), json!(3)),
        ("d".to_string(), json!(4)),
    ];
    let trusts = vec![
        None,
        Some("provisional".to_string()),
        Some("stable".to_string()),
        Some("provisional".to_string()),
    ];
    let groups = group_by_trust(items, &trusts);
    let shape: Vec<(Option<&str>, Vec<&str>)> = groups
        .iter()
        .map(|(t, v)| (t.as_deref(), v.iter().map(|(k, _)| k.as_str()).collect()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (None, vec!["a", "c"]),
            (Some("provisional"), vec!["b", "d"])
        ]
    );
}
