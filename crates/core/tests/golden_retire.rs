//! Stage 3 of the repair plane against a real temp-dir SQLite: retirement on its
//! own denominator, golden documents (pin, check, refresh), and cohort-less
//! detection through `observe`.
//!
//! The pure predicates are unit-tested beside their code
//! (`resilience::detect`, `golden`); this covers what only the store answers —
//! that `observe` reads the right history, persists the right columns, and that
//! the golden store's files and rows move together.

use chrono::{Duration, Utc};
use pumper_core::config::ResilienceConfig;
use pumper_core::datasets::{body_sha256, pinned_body_path, ts, BodyPin, Datasets};
use pumper_core::golden::{
    self, GoldenError, PINNED_BY_ENROL, PINNED_BY_OPERATOR, PINNED_BY_REFRESH,
};
use pumper_core::resilience::store::Resilience;
use pumper_core::testing::TempStore;
use pumper_core::{
    DocReport, DocSignals, FetchHealth, ObservedDoc, RuleSet, RunReport, RunVerdict, SourceState,
};
use serde_json::{json, Value};
use sqlx::SqlitePool;
use uuid::Uuid;

const APP: &str = "extractor";
const DATASET: &str = "hn-items";
const SOURCE: &str = "extractor/hn-items";

fn cfg(auto_retire: &[&str]) -> ResilienceConfig {
    ResilienceConfig {
        min_cohort_docs: 10,
        auto_retire: auto_retire.iter().map(|s| s.to_string()).collect(),
        ..ResilienceConfig::default()
    }
}

/// A run of a 40-URL roster where every fetch came back 404.
fn dead_fetch() -> FetchHealth {
    FetchHealth {
        attempted: 40,
        ok: 0,
        permanent: 40,
        roster: Some(40),
    }
}

/// A run of the same roster behind a bot wall: every fetch failed, none of
/// them as "gone".
fn walled_fetch() -> FetchHealth {
    FetchHealth {
        attempted: 40,
        ok: 0,
        permanent: 0,
        roster: Some(40),
    }
}

async fn observe(
    health: &Resilience,
    docs: &[ObservedDoc],
    fetch: FetchHealth,
) -> (Uuid, pumper_core::SourceVerdict) {
    let job_id = Uuid::new_v4();
    let v = health
        .observe(
            APP,
            &RunReport {
                job_id,
                dataset: DATASET,
                docs,
                fetch,
                build_id: None,
            },
        )
        .await
        .expect("observe")
        .expect("detection is enabled");
    (job_id, v)
}

/// Moves a recorded run `days_ago` into the past — the scheduler's cadence,
/// compressed into one test.
async fn backdate_run(pool: &SqlitePool, job: Uuid, days_ago: i64) {
    sqlx::query("UPDATE source_runs SET created_at = ?1 WHERE job_id = ?2")
        .bind(ts(Utc::now() - Duration::days(days_ago)))
        .bind(job.to_string())
        .execute(pool)
        .await
        .unwrap();
}

/// The source row was created at its first run, which in real time was weeks
/// before the third dead one.
async fn backdate_source(pool: &SqlitePool, days_ago: i64) {
    sqlx::query("UPDATE sources SET state_since = ?1 WHERE id = ?2")
        .bind(ts(Utc::now() - Duration::days(days_ago)))
        .bind(SOURCE)
        .execute(pool)
        .await
        .unwrap();
}

/// Three dead weekly runs — the first two backdated to 14 and 7 days ago.
async fn three_dead_weeks(
    health: &Resilience,
    pool: &SqlitePool,
    fetch: FetchHealth,
) -> pumper_core::SourceVerdict {
    let (first, _) = observe(health, &[], fetch).await;
    backdate_source(pool, 30).await;
    backdate_run(pool, first, 14).await;
    let (second, v) = observe(health, &[], fetch).await;
    assert_ne!(v.state, SourceState::Retired, "two dead runs do not retire");
    backdate_run(pool, second, 7).await;
    observe(health, &[], fetch).await.1
}

// ---- retirement ---------------------------------------------------------

#[tokio::test]
async fn three_dead_runs_retire_behind_an_inconclusive_fetch_gate() {
    let store = TempStore::new("retire-three").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[SOURCE]));

    let v = three_dead_weeks(&health, &pool, dead_fetch()).await;
    // The fetch gate made every run inconclusive — and retirement still fired,
    // because it is evaluated on its own denominator, not on the verdict.
    assert_eq!(v.verdict, RunVerdict::Inconclusive);
    assert_eq!(v.state, SourceState::Retired);
    assert_eq!(v.previous_state, SourceState::Healthy);

    let row = health
        .store()
        .unwrap()
        .source(SOURCE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, SourceState::Retired);
    assert_eq!(row.state_reason.as_deref(), Some("permanent fetch failure"));

    // The denominator is persisted on every run row.
    let cols: Vec<(Option<i64>, Option<i64>)> = sqlx::query_as(
        "SELECT fetch_permanent, fetch_roster FROM source_runs WHERE source_id = ?1",
    )
    .bind(SOURCE)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(cols, vec![(Some(40), Some(40)); 3]);
}

#[tokio::test]
async fn unlisted_source_never_retires_however_dead() {
    let store = TempStore::new("retire-unlisted").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&["extractor/some-other-source"]));

    let v = three_dead_weeks(&health, &pool, dead_fetch()).await;
    assert_eq!(v.state, SourceState::Healthy, "opt-in per source");
}

#[tokio::test]
async fn a_bot_wall_never_retires_a_source() {
    let store = TempStore::new("retire-walled").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[SOURCE]));

    // 403/429/timeouts reach the extractor as failed fetches with no permanent
    // count — three weeks of them is a blocked source, not a dead one.
    let v = three_dead_weeks(&health, &pool, walled_fetch()).await;
    assert_eq!(v.state, SourceState::Healthy);
}

#[tokio::test]
async fn an_operator_restore_is_not_undone_by_the_runs_that_retired_it() {
    let store = TempStore::new("retire-restore").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[SOURCE]));
    assert_eq!(
        three_dead_weeks(&health, &pool, dead_fetch()).await.state,
        SourceState::Retired
    );

    let hs = health.store().unwrap();
    assert!(hs
        .set_state_manual(SOURCE, SourceState::Healthy, "site is back")
        .await
        .unwrap());
    // The next run is still dead — but the two runs that retired it are behind
    // the operator's `state_since`, so it takes three NEW dead runs to retire
    // it again.
    let (_, v) = observe(&health, &[], dead_fetch()).await;
    assert_eq!(v.state, SourceState::Healthy);
}

#[tokio::test]
async fn a_run_with_no_roster_persists_null_not_zero() {
    let store = TempStore::new("retire-null").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[SOURCE]));
    observe(&health, &[], FetchHealth::default()).await;
    let cols: (Option<i64>, Option<i64>) =
        sqlx::query_as("SELECT fetch_permanent, fetch_roster FROM source_runs")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(cols, (None, None), "no roster reported = unknown, not zero");
}

// ---- cohort-less detection -------------------------------------------------

fn doc(key: &str, values: Value) -> ObservedDoc {
    ObservedDoc {
        key: key.into(),
        values,
        report: DocReport::default(),
        signals: DocSignals::default(),
    }
}

async fn insert_golden(
    pool: &SqlitePool,
    key: &str,
    generation: i64,
    expected: Value,
    pinned_by: &str,
    pinned_at: &str,
) {
    sqlx::query(
        "INSERT INTO golden_docs (source_id, key, generation, url, body_sha, expected, \
             stable_fields, rules_hash, pinned_by, pinned_at) \
         VALUES (?1, ?2, ?3, ?2, ?4, ?5, '[\"by\",\"title\"]', 'h', ?6, ?7)",
    )
    .bind(SOURCE)
    .bind(key)
    .bind(generation)
    .bind("0".repeat(64))
    .bind(expected.to_string())
    .bind(pinned_by)
    .bind(pinned_at)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn a_below_cohort_run_is_judged_by_its_goldens_and_never_baselined() {
    let store = TempStore::new("golden-cohortless").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[]));
    let now = ts(Utc::now());
    for i in 0..3 {
        insert_golden(
            &pool,
            &format!("k{i}"),
            0,
            json!({"title": format!("Story {i}"), "by": "pg", "points": 1}),
            PINNED_BY_OPERATOR,
            &now,
        )
        .await;
    }
    let good: Vec<ObservedDoc> = (0..3)
        .map(|i| {
            doc(
                &format!("k{i}"),
                json!({"title": format!("Story {i}"), "by": "pg", "points": 90 + i}),
            )
        })
        .collect();

    // Three documents is under the floor of ten: the distributions cannot judge
    // it. The goldens can — `points` churns and is not compared.
    let (_, v) = observe(&health, &good, FetchHealth::default()).await;
    assert_eq!(v.verdict, RunVerdict::GoldenOk);
    assert!(!v.statistical_coverage);
    assert_eq!(v.state, SourceState::Healthy);
    let baseline = health.store().unwrap().baseline(SOURCE, 10).await.unwrap();
    assert!(
        baseline.fields.is_empty(),
        "a golden-judged run must not become baseline material"
    );

    // Two of the three rebound onto the page chrome: broken, and it trips.
    let bad: Vec<ObservedDoc> = (0..3)
        .map(|i| {
            let title = if i < 2 {
                "Hacker News".to_string()
            } else {
                format!("Story {i}")
            };
            doc(&format!("k{i}"), json!({"title": title, "by": "pg"}))
        })
        .collect();
    let (_, v) = observe(&health, &bad, FetchHealth::default()).await;
    assert_eq!(v.verdict, RunVerdict::Broken);
    assert_eq!(v.state, SourceState::Suspect);
    assert!(v.reasons.iter().any(|r| r.test == golden::GOLDEN_REASON));
}

#[tokio::test]
async fn no_goldens_stays_below_cohort_through_observe() {
    let store = TempStore::new("golden-none").await;
    let health = Resilience::new(store.storage.pool(), &cfg(&[]));
    let docs = [doc("k0", json!({"title": "A"}))];
    let (_, v) = observe(&health, &docs, FetchHealth::default()).await;
    assert_eq!(v.verdict, RunVerdict::BelowCohort);
}

// ---- the golden store -------------------------------------------------------

const KEY: &str = "https://news.ycombinator.com/item?id=1";

fn hn_page(title: &str) -> String {
    format!(
        "<html><head><title>{title} | Hacker News</title></head><body>\
         <table class=\"fatitem\"><tr class=\"athing submission\" id=\"1\"><td class=\"title\">\
         <span class=\"titleline\"><a href=\"http://ycombinator.com\">{title}</a></span></td></tr>\
         <tr><td class=\"subtext\"><span class=\"subline\"><span class=\"score\">57 points</span> \
         by <a class=\"hnuser\" href=\"user?id=pg\">pg</a></span></td></tr></table></body></html>"
    )
}

fn rules(title_selector: &str) -> RuleSet {
    serde_json::from_value(json!({
        "title": {"type": "css", "selector": title_selector},
        "by": {"type": "css", "selector": ".fatitem .hnuser"},
    }))
    .unwrap()
}

/// An enrolled source with one record, one pinned body and an active ruleset.
async fn enrolled(store: &TempStore, health: &Resilience) -> Datasets {
    let datasets = store.datasets();
    health
        .store()
        .unwrap()
        .ensure_source(APP, DATASET)
        .await
        .unwrap();
    sqlx::query("UPDATE sources SET active_rules_hash = 'h' WHERE id = ?1")
        .bind(SOURCE)
        .execute(&store.storage.pool())
        .await
        .unwrap();
    pin_body(store, &datasets, &hn_page("Y Combinator"), "job-1").await;
    datasets
        .upsert(
            APP,
            DATASET,
            KEY,
            &json!({"title": "Y Combinator", "by": "pg", "_url": KEY}),
        )
        .await
        .unwrap();
    datasets
}

async fn pin_body(store: &TempStore, datasets: &Datasets, body: &str, job: &str) {
    let sha = body_sha256(body);
    let path = pinned_body_path(&store.storage.artifacts_dir, APP, &sha).unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, body).unwrap();
    datasets
        .pin_bodies(&[BodyPin {
            body_sha: sha,
            observation_id: format!("{job}:{KEY}"),
            source_id: SOURCE.into(),
            job_id: Some(job.into()),
            document_key: Some(KEY.into()),
            original_url: Some(KEY.into()),
            captured_at: ts(Utc::now()),
        }])
        .await
        .unwrap();
}

#[tokio::test]
async fn an_enrolment_pin_copies_the_body_out_of_retention_and_checks_clean() {
    let store = TempStore::new("golden-pin").await;
    let health = Resilience::new(store.storage.pool(), &cfg(&[]));
    let datasets = enrolled(&store, &health).await;
    let root = &store.storage.artifacts_dir;

    let pinned = golden::pin(
        &datasets,
        root,
        SOURCE,
        &[KEY.to_string()],
        false,
        Utc::now(),
    )
    .await
    .expect("pin");
    assert_eq!(pinned.len(), 1);
    let g = &pinned[0];
    assert_eq!(
        (g.generation, g.pinned_by.as_str()),
        (0, PINNED_BY_ENROL),
        "the default pin is a refreshable enrolment"
    );
    assert_eq!(
        g.expected,
        json!({"title": "Y Combinator", "by": "pg"}),
        "no `_url`"
    );
    assert_eq!(g.stable_fields, vec!["by".to_string(), "title".to_string()]);
    let copied = golden::golden_body_path(&golden::golden_root(root), SOURCE, &g.body_sha).unwrap();
    assert!(copied.exists(), "the body is copied to data/golden/");
    assert!(
        !copied.starts_with(root),
        "outside the artifacts tree retention walks"
    );

    let check = golden::check(&datasets, root, SOURCE, &rules(".fatitem .titleline > a"))
        .await
        .unwrap();
    assert_eq!((check.frozen.keys, check.frozen.exact), (1, 1));
    assert_eq!((check.live.keys, check.live.agree), (1, 1));
    assert!(check.mismatches.is_empty());
}

#[tokio::test]
async fn golden_check_reports_a_code_drift_fixture_as_inexact() {
    let store = TempStore::new("golden-drift").await;
    let health = Resilience::new(store.storage.pool(), &cfg(&[]));
    let datasets = enrolled(&store, &health).await;
    let root = &store.storage.artifacts_dir;
    golden::pin(
        &datasets,
        root,
        SOURCE,
        &[KEY.to_string()],
        false,
        Utc::now(),
    )
    .await
    .unwrap();

    // A rules change that rebinds `title` onto the page <title>: the frozen body
    // no longer reproduces its known result.
    let check = golden::check(&datasets, root, SOURCE, &rules("title"))
        .await
        .unwrap();
    assert!(check.frozen.exact < check.frozen.keys, "{check:?}");
    let m = check
        .mismatches
        .iter()
        .find(|m| m.kind == "frozen")
        .expect("a frozen mismatch");
    assert_eq!(m.field.as_deref(), Some("title"));
    assert_eq!(m.expected, json!("Y Combinator"));
    // The site did not move: the live side still agrees.
    assert_eq!(check.live.agree, 1);

    // Site drift is the other half: the live record's stable field moved.
    datasets
        .upsert(
            APP,
            DATASET,
            KEY,
            &json!({"title": "Hacker News", "by": "pg", "_url": KEY}),
        )
        .await
        .unwrap();
    let check = golden::check(&datasets, root, SOURCE, &rules(".fatitem .titleline > a"))
        .await
        .unwrap();
    assert_eq!(check.live.agree, 0);
    assert!(check.mismatches.iter().any(|m| m.kind == "live"));
}

#[tokio::test]
async fn a_key_with_no_pinned_body_refuses_the_whole_pin() {
    let store = TempStore::new("golden-nopin").await;
    let health = Resilience::new(store.storage.pool(), &cfg(&[]));
    let datasets = enrolled(&store, &health).await;
    datasets
        .upsert(
            APP,
            DATASET,
            "https://news.ycombinator.com/item?id=2",
            &json!({"title": "B"}),
        )
        .await
        .unwrap();
    let err = golden::pin(
        &datasets,
        &store.storage.artifacts_dir,
        SOURCE,
        &[
            KEY.to_string(),
            "https://news.ycombinator.com/item?id=2".to_string(),
        ],
        false,
        Utc::now(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, GoldenError::BadRequest(ref m) if m.contains("no pinned body")),
        "{err:?}"
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM golden_docs")
        .fetch_one(&store.storage.pool())
        .await
        .unwrap();
    assert_eq!(rows, 0, "nothing is pinned when any key is refused");
}

async fn generations(pool: &SqlitePool) -> Vec<(i64, String)> {
    sqlx::query_as(
        "SELECT generation, pinned_by FROM golden_docs WHERE key = ?1 ORDER BY generation",
    )
    .bind(KEY)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn an_enrol_pin_refreshes_end_to_end_keeping_two_generations_and_a_frozen_pin_never_does() {
    let store = TempStore::new("golden-refresh").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[]));
    let datasets = enrolled(&store, &health).await;
    let root = &store.storage.artifacts_dir;
    let long_ago = ts(Utc::now() - Duration::days(90));
    sqlx::query("UPDATE sources SET state_since = ?1")
        .bind(&long_ago)
        .execute(&pool)
        .await
        .unwrap();

    // A frozen operator pin: refresh never replaces it, however old.
    golden::pin(
        &datasets,
        root,
        SOURCE,
        &[KEY.to_string()],
        true,
        Utc::now() - Duration::days(60),
    )
    .await
    .unwrap();
    assert!(golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        generations(&pool).await,
        vec![(0, PINNED_BY_OPERATOR.to_string())]
    );

    // Re-pinned as an ENROLMENT (the default), 60 days healthy: the real
    // refresh pins generation 1 beside it — no hand-written rows.
    golden::pin(
        &datasets,
        root,
        SOURCE,
        &[KEY.to_string()],
        false,
        Utc::now() - Duration::days(60),
    )
    .await
    .unwrap();
    assert_eq!(
        generations(&pool).await,
        vec![(0, PINNED_BY_ENROL.to_string())]
    );
    assert_eq!(
        golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
            .await
            .unwrap(),
        vec![KEY.to_string()]
    );
    assert_eq!(
        generations(&pool).await,
        vec![
            (0, PINNED_BY_ENROL.to_string()),
            (1, PINNED_BY_REFRESH.to_string())
        ]
    );

    // Too soon for another.
    assert!(golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
        .await
        .unwrap()
        .is_empty());

    // 40 days on: generation 2, and generation 0 is dropped — two kept.
    sqlx::query("UPDATE golden_docs SET pinned_at = ?1 WHERE generation = 1")
        .bind(ts(Utc::now() - Duration::days(40)))
        .execute(&pool)
        .await
        .unwrap();
    golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        generations(&pool)
            .await
            .iter()
            .map(|g| g.0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    // A source that left healthy after the pin is not continuously healthy.
    sqlx::query("UPDATE golden_docs SET pinned_at = ?1 WHERE generation = 2")
        .bind(ts(Utc::now() - Duration::days(40)))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE sources SET state_since = ?1")
        .bind(ts(Utc::now() - Duration::days(1)))
        .execute(&pool)
        .await
        .unwrap();
    assert!(golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn boiled_value_that_fooled_n_but_not_n_minus_1_blocks_repin_in_the_store() {
    let store = TempStore::new("golden-boil").await;
    let pool = store.storage.pool();
    let health = Resilience::new(pool.clone(), &cfg(&[]));
    let datasets = enrolled(&store, &health).await;
    let root = &store.storage.artifacts_dir;
    sqlx::query("UPDATE sources SET state_since = ?1")
        .bind(ts(Utc::now() - Duration::days(200)))
        .execute(&pool)
        .await
        .unwrap();
    let old = ts(Utc::now() - Duration::days(100));
    let recent = ts(Utc::now() - Duration::days(40));
    // n−1 holds the original title; n was pinned after the value boiled.
    insert_golden(
        &pool,
        KEY,
        0,
        json!({"title": "Y Combinator", "by": "pg"}),
        PINNED_BY_REFRESH,
        &old,
    )
    .await;
    insert_golden(
        &pool,
        KEY,
        1,
        json!({"title": "Y Combinator!", "by": "pg"}),
        PINNED_BY_REFRESH,
        &recent,
    )
    .await;
    datasets
        .upsert(
            APP,
            DATASET,
            KEY,
            &json!({"title": "Y Combinator!", "by": "pg"}),
        )
        .await
        .unwrap();

    assert!(
        golden::refresh(&datasets, root, SOURCE, 30, Utc::now())
            .await
            .unwrap()
            .is_empty(),
        "the current value fooled n but not n−1"
    );
    assert_eq!(
        generations(&pool)
            .await
            .iter()
            .map(|g| g.0)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}
