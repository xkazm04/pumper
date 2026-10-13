//! `datasets doctor` against a real migrated SQLite.
//!
//! The pure diagnosis rules are unit-tested in `core::doctor`. What needs a
//! database is that the **queries feeding them** are silent on a healthy store
//! and speak up on a broken one — a check that mis-reads its own SQL produces
//! false positives that no amount of pure testing would catch.

use pumper_core::doctor::{
    diagnose, examine, ArtifactScan, Check, CheckOutcome, SearchFacts, StoreFacts, TableGrowth,
    Verdict,
};
use pumper_core::testing::TempStore;
use pumper_core::Provenance;
use serde_json::json;

async fn missing_simhash_total(ds: &pumper_core::Datasets) -> i64 {
    ds.missing_simhash_counts()
        .await
        .unwrap()
        .iter()
        .map(|(_, _, n)| n)
        .sum()
}

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
        null_simhash: ds.missing_simhash_counts().await.unwrap(),
        orphan_derived: ds.orphan_derived_specs().await.unwrap(),
        stale_rebuild_tables: store.storage.stale_rebuild_tables().await.unwrap(),
        ..Default::default()
    };
    let findings = diagnose(&facts);
    assert!(findings.is_empty(), "clean store reported: {findings:?}");

    // ...and the half this test used to assert as "clean" without having
    // gathered it. `..Default::default()` left `search`, `schema`, the table
    // growth and the artifact sweep ungathered, so the honest verdict over
    // these facts is INCOMPLETE: four of the nine checks could not run, and the
    // absence of a finding from a check that never ran is not evidence of
    // health. This is the anti-pattern the roster exists to make unwritable.
    let report = examine(&facts);
    assert_eq!(
        report.verdict,
        Verdict::Incomplete,
        "a facts struct that gathered five of nine checks must not report clean: {:?}",
        report.checks
    );
    assert!(!report.is_clean());
    for ungathered in [
        Check::MissingArtifactBodies,
        Check::UnboundedTableGrowth,
        Check::SchemaDrift,
        Check::SearchIndexEmpty,
    ] {
        assert!(
            matches!(
                report.outcome(ungathered),
                CheckOutcome::Unverifiable { .. }
            ),
            "{} was never gathered yet reports {:?}",
            ungathered.as_str(),
            report.outcome(ungathered)
        );
    }
}

/// The other half of the flagship property, and the one that was never
/// provable: a store whose facts were ALL gathered and which has nothing wrong
/// reports `clean` — nine checks, nine `clean` outcomes, `healthy: true`. The
/// load-bearing "a clean store says nothing" property survives the third state
/// rather than being replaced by permanent incompleteness.
#[tokio::test]
async fn a_fully_gathered_clean_store_is_clean() {
    let store = TempStore::new("doctor-clean-gathered").await;
    let ds = store.datasets();

    let facts = StoreFacts {
        half_stamped: ds.half_stamped_revisions().await.unwrap(),
        unregistered_rules: ds.unregistered_rules_hashes().await.unwrap(),
        null_simhash: ds.missing_simhash_counts().await.unwrap(),
        orphan_derived: ds.orphan_derived_specs().await.unwrap(),
        stale_rebuild_tables: store.storage.stale_rebuild_tables().await.unwrap(),
        schema: Some(store.storage.schema_report().await.unwrap()),
        search: Some(SearchFacts {
            enabled: true,
            doc_count: Some(7),
            live_records: 0,
        }),
        tables: store
            .storage
            .ledger_stats()
            .await
            .unwrap()
            .into_iter()
            .map(|st| TableGrowth {
                table: st.table,
                rows: st.rows,
                oldest_days: None,
                retention_days: 0,
                config_key: "cost_event_retention_days",
            })
            .collect(),
        artifacts: ArtifactScan::Complete,
        ..Default::default()
    };

    let report = examine(&facts);
    assert!(report.findings.is_empty(), "{:?}", report.findings);
    assert_eq!(report.verdict, Verdict::Clean, "{:?}", report.checks);
    assert!(report.is_clean());
    assert_eq!(
        report.checks.len(),
        Check::ALL.len(),
        "every check in the roster is reported, not only the ones that fired"
    );
    for c in &report.checks {
        assert_eq!(
            c.outcome,
            CheckOutcome::Clean,
            "{} is not clean on a fully gathered clean store",
            c.check.as_str()
        );
    }
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
    assert_eq!(findings[0].check, Check::StaleRebuildTables);
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
    let _ = ds.missing_simhash_counts().await.unwrap();
    let _ = ds.orphan_derived_specs().await.unwrap();
    let _ = ds.provenance_coverage_by_dataset().await.unwrap();
    let _ = ds.replayable_revisions(100).await.unwrap();
    let _ = store.storage.stale_rebuild_tables().await.unwrap();
    let _ = store.storage.ledger_stats().await.unwrap();

    assert_eq!(before, snapshot(store.storage.pool()).await);
}

/// The anti-pattern: `records_without_simhash` counting every `simhash = 0`
/// row and prescribing `just reindex`, which rewrites only rows whose
/// recomputed value DIFFERS. A record with no textual content hashes to 0
/// honestly, so it was counted forever and the whole-table rewrite provably
/// could not clear it — making the endpoint's load-bearing "a clean store
/// produces ZERO findings" property permanently unreachable on such a store.
///
/// Driven against the real SQL and the real `reindex_simhashes`, because the
/// defect lives in the disagreement between the two.
#[tokio::test]
async fn the_simhash_finding_is_not_permanently_unclearable() {
    let store = TempStore::new("doctor-simhash-clears").await;
    let ds = store.datasets();
    let pool = store.storage.pool();

    // One record with text, one with none at all. Both are stored with the
    // `DEFAULT 0` sentinel that migration 0004 left behind.
    ds.upsert(
        "app",
        "d",
        "has-text",
        &json!({ "title": "a grant for widgets" }),
    )
    .await
    .unwrap();
    ds.upsert(
        "app",
        "d",
        "no-text",
        &json!({ "flag": true, "nothing": null }),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE records SET simhash = 0")
        .execute(&pool)
        .await
        .unwrap();

    assert_eq!(
        missing_simhash_total(&ds).await,
        1,
        "only the record reindex will actually rewrite may be counted"
    );

    // The prescribed remediation, for real.
    assert_eq!(
        ds.reindex_simhashes().await.unwrap(),
        1,
        "reindex rewrites exactly the counted row"
    );

    assert_eq!(
        missing_simhash_total(&ds).await,
        0,
        "after the prescribed remediation the finding must CLEAR — the textless \
         record still stores 0, which is its honest hash"
    );
    let facts = StoreFacts {
        null_simhash: ds.missing_simhash_counts().await.unwrap(),
        ..Default::default()
    };
    assert!(diagnose(&facts).is_empty(), "{:?}", diagnose(&facts));
}

/// `live_record_count` is what the search-index check compares against, so it
/// must mean "records the index could be serving" — tombstoned rows are not.
#[tokio::test]
async fn a_tombstoned_record_is_not_counted_as_indexable() {
    let store = TempStore::new("doctor-live-count").await;
    let ds = store.datasets();

    ds.upsert("app", "d", "a", &json!({ "t": "one" }))
        .await
        .unwrap();
    ds.upsert("app", "d", "b", &json!({ "t": "two" }))
        .await
        .unwrap();
    assert_eq!(ds.live_record_count().await.unwrap(), 2);

    ds.tombstone_keys("app", "d", &["b".to_string()])
        .await
        .unwrap();
    assert_eq!(
        ds.live_record_count().await.unwrap(),
        1,
        "a tombstoned record is not something the index should be holding"
    );
}

// ── the roster and the three-state verdict ───────────────────────────────────
// Pure cases over hand-built facts (no database): `examine` is the decider, and
// the point of each is a shape `healthy: findings.is_empty()` could not
// express.

fn full_tables() -> std::collections::BTreeSet<String> {
    pumper_core::schema::EXPECTED_TABLES
        .iter()
        .map(|t| (*t).to_string())
        .collect()
}

fn one_table(rows: i64, oldest_days: Option<i64>) -> TableGrowth {
    TableGrowth {
        table: "cost_events".into(),
        rows,
        oldest_days,
        retention_days: 0,
        config_key: "cost_event_retention_days",
    }
}

/// SEEDED VIOLATION. Make every check that can fail to run fail to run, and
/// watch the rollup refuse to call it clean. `StoreFacts::default()` gathered
/// nothing at all, and under the old verdict it answered `healthy: true` — a
/// rollup that cannot tell "nothing wrong" from "nothing looked" reports
/// success in a voice indistinguishable from it.
#[test]
fn a_facts_struct_that_gathered_nothing_is_incomplete_not_clean() {
    let report = examine(&StoreFacts::default());
    assert!(
        report.findings.is_empty(),
        "nothing was gathered, so nothing can be a finding: {:?}",
        report.findings
    );
    assert_eq!(
        report.verdict,
        Verdict::Incomplete,
        "an all-defaults facts struct looked at nothing: {:?}",
        report.checks
    );
    assert!(
        !report.is_clean(),
        "`healthy` must be false when nothing was examined"
    );
}

/// The operator moment the card is named for: `?skip_artifacts=true` on an
/// otherwise clean store. The body sweep is the check that predicts a 409 from
/// `rederive`, and skipping it used to cost nothing on the verdict.
#[test]
fn a_skipped_artifact_sweep_is_unverifiable_and_names_the_flag() {
    let facts = StoreFacts {
        artifacts: ArtifactScan::Skipped,
        ..Default::default()
    };
    let report = examine(&facts);
    assert_eq!(report.verdict, Verdict::Incomplete);
    let reason = match report.outcome(Check::MissingArtifactBodies) {
        CheckOutcome::Unverifiable { reason } => reason.clone(),
        other => panic!("a skipped sweep reported {other:?}"),
    };
    assert!(
        reason.contains("skip_artifacts=true"),
        "the reason must name the flag that caused it: {reason}"
    );
    assert!(
        reason.contains("/datasets/doctor"),
        "the reason must name the request that would verify it: {reason}"
    );
}

/// The ceiling, which is the silence an operator cannot opt out of: a bounded
/// sweep that found nothing is not evidence about the revisions it never
/// stat'd — and the reason carries both halves of the fraction, so the operator
/// knows how partial the green is.
#[test]
fn a_truncated_body_sweep_names_its_denominator_rather_than_claiming_clean() {
    let truncated = StoreFacts {
        replayable_checked: 5_000,
        artifacts: ArtifactScan::Truncated {
            checked: 5_000,
            replayable: 6_000,
        },
        ..Default::default()
    };
    let report = examine(&truncated);
    assert_eq!(report.verdict, Verdict::Incomplete);
    let reason = match report.outcome(Check::MissingArtifactBodies) {
        CheckOutcome::Unverifiable { reason } => reason.clone(),
        other => panic!("a truncated sweep reported {other:?}"),
    };
    assert!(
        reason.contains("5000 of 6000"),
        "the count must carry its predicate: {reason}"
    );
    assert!(
        reason.contains("1000"),
        "the reason must say how much was not looked at: {reason}"
    );

    // The same store small enough to sweep whole: the check concludes.
    let complete = StoreFacts {
        replayable_checked: 100,
        artifacts: ArtifactScan::Complete,
        schema: Some(pumper_core::schema::SchemaReport::new(
            &[1],
            &[1],
            &full_tables(),
        )),
        search: Some(SearchFacts {
            enabled: false,
            doc_count: None,
            live_records: 9,
        }),
        tables: vec![one_table(0, None)],
        ..Default::default()
    };
    let report = examine(&complete);
    assert_eq!(
        report.outcome(Check::MissingArtifactBodies),
        &CheckOutcome::Clean
    );
    assert_eq!(report.verdict, Verdict::Clean, "{:?}", report.checks);
}

/// An index whose `doc_count` could not be read disarms `search_index_empty`
/// entirely: `doc_count == Some(0)` is simply false, so the check went quiet
/// and the report stayed green. That silence is the doctor's own failure to
/// measure, not the store's health.
#[test]
fn an_unreadable_doc_count_is_unverifiable_and_an_empty_index_is_a_finding() {
    let unreadable = StoreFacts {
        search: Some(SearchFacts {
            enabled: true,
            doc_count: None,
            live_records: 1,
        }),
        ..Default::default()
    };
    let report = examine(&unreadable);
    assert!(
        report.findings.is_empty(),
        "the doctor must not manufacture a finding out of its own blindness"
    );
    assert!(matches!(
        report.outcome(Check::SearchIndexEmpty),
        CheckOutcome::Unverifiable { .. }
    ));
    assert_eq!(report.verdict, Verdict::Incomplete);

    // Readable and empty over a populated store: a finding, and `findings`
    // outranks `incomplete` in the rollup.
    let empty = StoreFacts {
        search: Some(SearchFacts {
            enabled: true,
            doc_count: Some(0),
            live_records: 1,
        }),
        ..Default::default()
    };
    let report = examine(&empty);
    assert_eq!(
        report.outcome(Check::SearchIndexEmpty),
        &CheckOutcome::Findings
    );
    assert_eq!(report.verdict, Verdict::Findings);
}

/// GUARD, and the collapse in the other direction: `[search] enabled = false`
/// is a valid deployment, so its check is `clean` — not `unverifiable`.
/// Counting an intentionally absent subsystem as "could not look" would make
/// every search-less store permanently un-green, which is how an operator
/// learns to ignore the verdict.
#[test]
fn a_disabled_search_index_is_clean_not_incomplete() {
    let facts = StoreFacts {
        artifacts: ArtifactScan::Complete,
        schema: Some(pumper_core::schema::SchemaReport::new(
            &[1],
            &[1],
            &full_tables(),
        )),
        search: Some(SearchFacts {
            enabled: false,
            doc_count: Some(0),
            live_records: 12_345,
        }),
        tables: vec![one_table(1, Some(1))],
        ..Default::default()
    };
    let report = examine(&facts);
    assert_eq!(
        report.outcome(Check::SearchIndexEmpty),
        &CheckOutcome::Clean
    );
    assert_eq!(report.verdict, Verdict::Clean, "{:?}", report.checks);
    assert!(report.is_clean());
}

/// Worst-of, with the order that matters: a real finding and an unexamined
/// check in the same report resolve to `findings` — the rung that carries a
/// remediation — while the unverifiable member is still named in `checks[]`. A
/// rollup that swallowed it would be a wall, not a door.
#[test]
fn a_finding_outranks_an_unverifiable_check_but_does_not_hide_it() {
    let facts = StoreFacts {
        stale_rebuild_tables: vec!["triggers_new".into()],
        artifacts: ArtifactScan::Skipped,
        ..Default::default()
    };
    let report = examine(&facts);
    assert_eq!(report.verdict, Verdict::Findings);
    assert_eq!(
        report.outcome(Check::StaleRebuildTables),
        &CheckOutcome::Findings
    );
    assert!(
        matches!(
            report.outcome(Check::MissingArtifactBodies),
            CheckOutcome::Unverifiable { reason } if reason.contains("skip_artifacts")
        ),
        "the incomplete member must stay visible under a findings verdict: {:?}",
        report.outcome(Check::MissingArtifactBodies)
    );
}

/// INVENTORY (EXPECTED-diff idiom). The roster is the contract: `examine`
/// returns an outcome for every `Check::ALL` variant on every input, the wire
/// names are pinned, and `Finding.check` is typed — so a tenth check added
/// without a roster entry fails to compile, one added without an arm in
/// `unverifiable_reason` fails to compile, and one added without a wire name
/// fails here.
#[test]
fn every_check_reports_an_outcome() {
    let inputs = [
        StoreFacts::default(),
        StoreFacts {
            artifacts: ArtifactScan::Complete,
            ..Default::default()
        },
        StoreFacts {
            stale_rebuild_tables: vec!["x_new".into()],
            search: Some(SearchFacts {
                enabled: true,
                doc_count: Some(0),
                live_records: 3,
            }),
            ..Default::default()
        },
    ];
    for facts in &inputs {
        let report = examine(facts);
        let reported: Vec<Check> = report.checks.iter().map(|c| c.check).collect();
        assert_eq!(
            reported,
            Check::ALL.to_vec(),
            "every roster member is reported, in roster order"
        );
    }

    assert_eq!(
        Check::ALL.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
        vec![
            "missing_artifact_bodies",
            "half_stamped_provenance",
            "unregistered_rulesets",
            "records_without_simhash",
            "unbounded_table_growth",
            "orphan_derived_specs",
            "stale_rebuild_tables",
            "schema_drift",
            "search_index_empty",
        ],
        "the roster changed — update this list deliberately, and the two feature docs with it"
    );

    // The wire name a consumer reads IS the roster's name, in both directions.
    let report = examine(&StoreFacts::default());
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["verdict"], "incomplete");
    for (i, check) in Check::ALL.iter().enumerate() {
        assert_eq!(json["checks"][i]["check"], check.as_str());
        assert!(
            json["checks"][i]["outcome"].is_string(),
            "every entry publishes its outcome: {}",
            json["checks"][i]
        );
    }
}

/// The fraction the doctor publishes ("5000 of 42117 stat'd") must be one
/// population: the bounded listing and its denominator share one predicate, so
/// a revision that counts cannot be a revision the sweep would never list.
/// Driven against real SQL, because the defect would live in the disagreement
/// between two WHERE clauses.
#[tokio::test]
async fn the_replayable_count_is_the_same_population_as_the_listing() {
    let store = TempStore::new("doctor-replayable-count").await;
    let ds = store.datasets();

    for key in ["a", "b", "c"] {
        ds.upsert_stamped(
            "crawl",
            "pages",
            key,
            &json!({ "t": "words", "job_id": "job-a", "artifact_path": "page.html" }),
            None,
            Some(&replayable()),
        )
        .await
        .unwrap();
    }
    // Half-stamped: not replayable, so neither half of the fraction counts it.
    ds.upsert_stamped(
        "crawl",
        "pages",
        "half",
        &json!({ "t": "words", "job_id": "job-a", "artifact_path": "page.html" }),
        None,
        Some(&pumper_core::Provenance {
            job_id: Some("job-a".into()),
            source_url: None,
            artifact_sha: Some("a".repeat(64)),
            rules_hash: None,
        }),
    )
    .await
    .unwrap();
    // Replayable stamp but no body location in `data`: the sweep cannot stat it.
    ds.upsert_stamped(
        "crawl",
        "pages",
        "nowhere",
        &json!({ "t": "words" }),
        None,
        Some(&replayable()),
    )
    .await
    .unwrap();

    assert_eq!(ds.replayable_revision_count().await.unwrap(), 3);
    assert_eq!(
        ds.replayable_revisions(100).await.unwrap().len(),
        3,
        "an unbounded listing is exactly the counted population"
    );
    assert_eq!(
        ds.replayable_revisions(2).await.unwrap().len(),
        2,
        "the ceiling bounds the listing and NOT the denominator — which is what \
         makes the truncation reportable"
    );
}
