//! Integration test for the enforcement preview against a real temp-dir SQLite.
//!
//! The unit tests in `resilience::preview` cover the replay arithmetic over
//! synthetic rows. This covers the two claims only a database can settle:
//!
//! 1. **Fidelity** — a preview run over a history the detector actually produced
//!    reports the same ladder the detector walked, with each transition carrying
//!    the stored `reasons` that caused it, and never re-judging anything.
//! 2. **Provably zero side effects** — the store is byte-identical afterwards.
//!    A dry run that mutates is worse than no dry run: it is the exact thing an
//!    operator ran it to avoid.

use pumper_core::config::ResilienceConfig;
use pumper_core::extract::{CoercionStatus, DocReport, FieldStatus};
use pumper_core::resilience::preview::{preview_fleet, TransitionCause};
use pumper_core::resilience::store::Resilience;
use pumper_core::testing::TempStore;
use pumper_core::{doc_signals, FetchHealth, ObservedDoc, RunReport, SourceState};
use serde_json::{json, Value};
use sqlx::{Row, SqlitePool};
use uuid::Uuid;

/// Same shape as `tests/resilience.rs` — detection on, thresholds scaled so a
/// test cohort is a real cohort.
fn cfg() -> ResilienceConfig {
    ResilienceConfig {
        min_cohort_docs: 10,
        window_runs: 10,
        sketch_retention_runs: 30,
        invariant_min_support: 10,
        ..ResilienceConfig::default()
    }
}

fn page(i: usize, price_class: &str, price: &str) -> String {
    format!(
        "<html><body><div id=\"main\"><div class=\"card\">\
         <h1 class=\"title\">Product {i}</h1>\
         <span class=\"{price_class}\">{price}</span>\
         <p class=\"desc\">A description of product number {i} for sale.</p>\
         </div></div></body></html>"
    )
}

fn extract(doc: &str, price_selector: &str) -> (Value, DocReport) {
    let html = scraper::Html::parse_document(doc);
    let pick = |selector: &str| -> Option<String> {
        let sel = scraper::Selector::parse(selector).unwrap();
        html.select(&sel)
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
    };
    let title = pick(".title");
    let price = pick(price_selector);
    let mut report = DocReport::default();
    report.fields.insert(
        "title".into(),
        title
            .as_ref()
            .map_or(FieldStatus::Empty, |_| FieldStatus::Matched),
    );
    report.fields.insert(
        "price".into(),
        price
            .as_ref()
            .map_or(FieldStatus::Empty, |_| FieldStatus::Matched),
    );
    report
        .coercion
        .insert("title".into(), CoercionStatus::NoTransforms);
    report
        .coercion
        .insert("price".into(), CoercionStatus::NoTransforms);
    (json!({ "title": title, "price": price }), report)
}

fn cohort(n: usize, price_class: &str, price_selector: &str) -> Vec<ObservedDoc> {
    (0..n)
        .map(|i| {
            let doc = page(i, price_class, &format!("${}.{:02}", 10 + i, (i * 7) % 100));
            let (values, report) = extract(&doc, price_selector);
            ObservedDoc {
                key: format!("http://shop.example/p/{i}"),
                signals: doc_signals(&doc, &values),
                values,
                report,
            }
        })
        .collect()
}

async fn observe(health: &Resilience, docs: &[ObservedDoc]) -> SourceState {
    health
        .observe(
            "extractor",
            &RunReport {
                job_id: Uuid::new_v4(),
                dataset: "products",
                docs,
                fetch: FetchHealth {
                    attempted: docs.len() as u32,
                    ok: docs.len() as u32,
                },
                build_id: Some("test".into()),
            },
        )
        .await
        .expect("observe")
        .expect("detection is enabled")
        .state
}

/// Every row of every health table, rendered deterministically. The content
/// half of "nothing changed" — a `PRAGMA`-level file comparison alone could not
/// distinguish "no writes" from "writes that happened to land in the same pages".
async fn dump(pool: &SqlitePool) -> String {
    let mut out = String::new();
    for (table, order) in [
        ("sources", "id"),
        ("source_runs", "source_id, job_id"),
        ("field_sketches", "source_id, job_id, field"),
        ("doc_fingerprints", "source_id, key"),
        ("field_invariants", "source_id, field"),
    ] {
        out.push_str(&format!("-- {table}\n"));
        let rows = sqlx::query(&format!("SELECT * FROM {table} ORDER BY {order}"))
            .fetch_all(pool)
            .await
            .expect("dump table");
        for row in rows {
            for i in 0..row.len() {
                // Read every column as raw bytes so BLOBs (the sketch encodings)
                // are compared exactly rather than lossily stringified.
                let raw: Option<Vec<u8>> = row.try_get(i).ok();
                out.push_str(&format!("{i}={raw:?};"));
            }
            out.push('\n');
        }
    }
    out
}

/// The main database file's bytes. WAL-mode SQLite keeps recent commits in
/// `-wal`, so both are compared; `-shm` is a volatile shared-memory index that
/// read locks legitimately touch, and is deliberately excluded.
fn db_bytes(store: &TempStore) -> (Vec<u8>, Vec<u8>) {
    let db = store.path().join("pumper.db");
    let wal = store.path().join("pumper.db-wal");
    (
        std::fs::read(&db).expect("read db"),
        std::fs::read(&wal).unwrap_or_default(),
    )
}

/// Drives a source down the ladder to quarantine and back up to probation, so
/// the preview has a real timeline (both directions) to replay.
async fn history(health: &Resilience) {
    let healthy = || cohort(30, "price", ".price");
    let broken = || cohort(30, "amount", ".price");
    let repaired = || cohort(30, "amount", ".amount");
    for _ in 0..4 {
        observe(health, &healthy()).await;
    }
    for _ in 0..3 {
        observe(health, &broken()).await;
    }
    // Three clean judged runs earn the first rung back.
    for _ in 0..3 {
        observe(health, &repaired()).await;
    }
}

#[tokio::test]
async fn a_preview_replays_the_ladder_the_detector_actually_walked() {
    let store = TempStore::new("enforcement-preview-fidelity").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    history(&health).await;

    let preview = preview_fleet(health.store().unwrap(), health.enforcing(), None, 60, 500)
        .await
        .expect("preview");

    assert!(!preview.enforcing, "the shipping default is soak mode");
    assert_eq!(preview.sources_replayed, 1);
    let src = &preview.sources[0];
    assert_eq!(src.id, "extractor/products");
    assert_eq!(src.runs_replayed, 10);
    assert_eq!(src.unjudged_runs, 0);
    assert_eq!(src.window_opens_in, SourceState::Healthy);
    // The replayed end state is the live row: nothing outside the run history
    // moved this source.
    assert_eq!(src.state, src.live_state);
    assert_eq!(src.state, SourceState::Probation);
    assert!(src.monitored);

    // healthy -> suspect -> degraded -> quarantined -> probation, each caused by
    // a judged run, each carrying the stored tests that produced the verdict.
    let walked: Vec<(SourceState, SourceState)> =
        src.transitions.iter().map(|t| (t.from, t.to)).collect();
    assert_eq!(
        walked,
        vec![
            (SourceState::Healthy, SourceState::Suspect),
            (SourceState::Suspect, SourceState::Degraded),
            (SourceState::Degraded, SourceState::Quarantined),
            (SourceState::Quarantined, SourceState::Probation),
        ],
        "{:?}",
        src.transitions
    );
    for t in &src.transitions {
        assert_eq!(t.cause, TransitionCause::Verdict);
        assert!(
            t.reasons.as_ref().and_then(Value::as_array).is_some(),
            "a would-be transition must name what triggered it: {t:?}"
        );
    }

    // Consequences, counted per run and per document — never once per source.
    // The window: 4 healthy, 1 suspect, 1 degraded, 1 quarantined, 2 quarantined
    // (the two clean runs that had not yet earned a rung), 1 probation.
    let c = src.consequences;
    assert_eq!(c.diverted_writes.runs, 3);
    assert_eq!(c.diverted_writes.docs, 90);
    assert_eq!(c.withheld_removals.runs, 4, "degraded + 3 quarantined");
    assert_eq!(c.suppressed_pushes.runs, 4);
    assert_eq!(c.skipped_index_writes.runs, 4);
    // `suspect` stamps nothing; `probation` stamps `provisional`.
    assert_eq!(c.trust_stamped.runs, 5);
    assert_eq!(preview.totals, c, "one source, so totals are its counts");

    // Probation gates nothing downstream, so this fleet IS ready to enforce —
    // and the answer is one field, not a table the operator has to read.
    assert!(preview.ready, "{:?}", preview.not_ready);
    assert!(preview.not_ready.is_empty());
    assert!(preview.unmonitored.is_empty());
}

#[tokio::test]
async fn a_fleet_that_is_not_ready_names_the_sources_that_are_not() {
    let store = TempStore::new("enforcement-preview-ready").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    let healthy = || cohort(30, "price", ".price");
    let broken = || cohort(30, "amount", ".price");
    for _ in 0..4 {
        observe(&health, &healthy()).await;
    }
    for _ in 0..3 {
        observe(&health, &broken()).await;
    }

    let preview = preview_fleet(health.store().unwrap(), false, None, 60, 500)
        .await
        .expect("preview");
    assert!(!preview.ready);
    assert_eq!(preview.not_ready.len(), 1);
    let nr = &preview.not_ready[0];
    assert_eq!(nr.id, "extractor/products");
    assert_eq!(nr.state, SourceState::Quarantined);
    // Named consequences, not a bare state string: the operator asked what would
    // break, not what the row says.
    assert_eq!(
        nr.gates,
        vec![
            "diverted_writes",
            "withheld_removals",
            "suppressed_pushes",
            "skipped_index_writes"
        ]
    );
    let since = nr.since.as_ref().expect("the transition that put it there");
    assert_eq!(since.from, SourceState::Degraded);
    assert_eq!(since.to, SourceState::Quarantined);
    assert!(since.reasons.is_some());
}

#[tokio::test]
async fn a_preview_leaves_the_store_byte_identical() {
    let store = TempStore::new("enforcement-preview-readonly").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    history(&health).await;
    // Force everything committed out of the pool's connections before the
    // snapshot, so the comparison is over settled bytes.
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&store.storage.pool())
        .await
        .expect("checkpoint");

    let before_rows = dump(&store.storage.pool()).await;
    let before_bytes = db_bytes(&store);

    let preview = preview_fleet(health.store().unwrap(), false, None, 1000, 500)
        .await
        .expect("preview");
    // The preview really did read something — a no-op that touched nothing would
    // pass this test vacuously.
    assert_eq!(preview.sources_replayed, 1);
    assert_eq!(preview.sources[0].runs_replayed, 10);
    assert!(!preview.sources[0].transitions.is_empty());

    let after_rows = dump(&store.storage.pool()).await;
    let after_bytes = db_bytes(&store);
    assert_eq!(
        before_rows, after_rows,
        "the preview wrote to a health table"
    );
    assert_eq!(
        before_bytes.0, after_bytes.0,
        "the database file changed under a read-only preview"
    );
    assert_eq!(
        before_bytes.1, after_bytes.1,
        "the WAL grew under a preview"
    );
}
