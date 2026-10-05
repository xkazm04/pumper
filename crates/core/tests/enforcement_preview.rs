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
//! 3. **The consequence inventory is complete** — a workspace scan proves that
//!    every consumer of `enforced_state` is accounted for by a row in
//!    `preview::CONSEQUENCES`. See `ENFORCED_STATE_CONSUMERS` below.

use pumper_core::config::ResilienceConfig;
use pumper_core::extract::{CoercionStatus, DocReport, FieldStatus};
use pumper_core::resilience::preview::{preview_fleet, ArmedScope, Arming, TransitionCause};
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
                    ..Default::default()
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

    let preview = preview_fleet(health.store().unwrap(), &health.arming(), None, 60, 500)
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

    let preview = preview_fleet(health.store().unwrap(), &Arming::default(), None, 60, 500)
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

    let preview = preview_fleet(health.store().unwrap(), &Arming::default(), None, 1000, 500)
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

// ---------------------------------------------------------------------------
// The consequence inventory, checked against the workspace rather than a copy
// ---------------------------------------------------------------------------

/// Every `enforced_state` call site in the workspace, as
/// `<repo-relative path>::<expr>` → (occurrences, the [`CONSEQUENCES`] names
/// that call site applies).
///
/// **Why a scan and not a list.** `preview::CONSEQUENCES` is the preview's claim
/// about what `[resilience] enforce = true` would change, and the way that claim
/// rots is textbook: a fifth consumer starts gating on `enforced_state`, nobody
/// adds a row, and the preview keeps reporting the old four. The failure is
/// silent and it is optimistic — the preview says *flipping the flag today would
/// change nothing about the next run* while a consequence it never heard of
/// waits behind the flag. That is the one direction a rollout gate may not be
/// wrong in.
///
/// The test this replaced compared `CONSEQUENCES` to a hand-copied literal
/// twelve lines below it: two copies of one list, agreeing with each other and
/// with nothing else. It could catch a typo and could not catch the new
/// consumer.
///
/// Counts are pinned so a *second* `enforced_state` read inside an already-listed
/// file is caught too — file granularity would wave it through.
///
/// **Every row is a decision.** Before adding one: does this call site apply a
/// consequence `CONSEQUENCES` already names, or a NEW one? A new one needs a
/// `CONSEQUENCES` entry, a `PreviewConsequences` counter, and a `record` arm, or
/// the preview under-reports it.
const ENFORCED_STATE_CONSUMERS: &[(&str, usize, &[&str])] = &[
    // ── The two core seams the inventory is written against ──────────────────
    // `AppContext::write_target` (diversion + trust stamp) and
    // `AppContext::sync_many_with_provenance` (the withheld `RemovalGuard`).
    (
        "crates/core/src/app.rs::self.health.enforced_state",
        2,
        &["diverted_writes", "withheld_removals"],
    ),
    // `suppress_unhealthy` (pushes) and `dataset_search_docs` (index writes),
    // plus `enforced_states`, the one shared read behind the per-source gates on
    // batches that mix sources: saved-search `search.matched` alerts and a
    // view's watch/trigger fan-out (pushes), and the job-result echo index
    // (index writes). Same two consequences, reached by more paths.
    (
        "crates/server/src/worker.rs::state.health.enforced_state",
        3,
        &["suppressed_pushes", "skipped_index_writes"],
    ),
    // `search-backfill`'s `HealthGate`: the offline rebuild skips pairs whose
    // state keeps them out of the index — the same consequence as
    // `dataset_search_docs`, applied to the one path that could undo it.
    (
        "crates/server/src/bin/search-backfill.rs::self.health.enforced_state",
        1,
        &["skipped_index_writes"],
    ),
    // ── Producer-side reads of the SAME consequences ─────────────────────────
    // These are apps resolving the state themselves, at the point the core seam
    // would resolve it, to report or pre-apply a consequence already inventoried
    // — not new consequences. Each is listed so that a call site which stops
    // being one of these has to be re-justified here.
    //
    // extractor: names which of `<dataset>` / `<dataset>@q` the batch landed in,
    // via `resilience::write_dataset` — the diversion, reported.
    (
        "crates/apps/extractor/src/lib.rs::ctx.health.enforced_state",
        1,
        &["diverted_writes"],
    ),
    // grants-common: `contribution_target` picks the shadow dataset and the trust
    // stamp, and `indexable` withholds the search spec for a degrading source —
    // the producer-side half of the index gate, because the worker's gate reads
    // the VIRTUAL `grants/unified` pair that no `observe_extraction` judges.
    (
        "crates/apps/grants-common/src/lib.rs::ctx.health.enforced_state",
        1,
        &["diverted_writes", "skipped_index_writes"],
    ),
    // plugin: same reasoning as grants-common, spelled as `WriteTarget`.
    (
        "crates/apps/plugin/src/lib.rs::ctx.health.enforced_state",
        1,
        &["diverted_writes", "skipped_index_writes"],
    ),
    // trades-common: `write_target` for the cross-source `trades` join. Nothing
    // calls `observe_extraction` for that namespace yet, so it resolves `Healthy`
    // and gates nothing today — the plumbing, present and correct, declared
    // anyway so it is not a surprise the day a producer starts judging it.
    (
        "crates/apps/trades-common/src/lib.rs::ctx.health.enforced_state",
        1,
        &["diverted_writes"],
    ),
];

/// This file — its string literals are full of `enforced_state` expressions (the
/// inventory above), so scanning it would make the guard report itself.
const SELF_PATH: &str = "crates/core/tests/enforcement_preview.rs";

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root")
}

fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            // `target/` holds generated + vendored code, not our call sites.
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// One source file as the scanner must see it: comment and doc lines dropped
/// (the split is *documented* in a dozen places and prose must never read as a
/// call site), the rest joined with no separator so a rustfmt-wrapped chain
/// (`state\n.health\n.enforced_state(..)`) is still one site.
fn scannable_source(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
        .collect()
}

/// The `<receiver>.health.enforced_state` expressions on one scannable line.
fn enforced_state_exprs(line: &str) -> Vec<String> {
    let needle = ".health.enforced_state";
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(rel) = line[from..].find(needle) {
        let at = from + rel;
        // Walk back over the receiver identifier (`ctx`, `state`, `self`).
        let recv_start = line[..at]
            .char_indices()
            .rev()
            .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
            .last()
            .map_or(at, |(i, _)| i);
        found.push(format!("{}{needle}", &line[recv_start..at]));
        from = at + needle.len();
    }
    found
}

fn enforced_state_calls() -> std::collections::BTreeMap<String, usize> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let mut found: std::collections::BTreeMap<String, usize> = Default::default();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        if rel == SELF_PATH {
            continue;
        }
        for expr in enforced_state_exprs(&scannable_source(&text)) {
            *found.entry(format!("{rel}::{expr}")).or_default() += 1;
        }
    }
    found
}

/// The guard the copied literal could not be: a NEW consumer of `enforced_state`
/// anywhere in the workspace fails this test, by construction.
#[test]
fn every_enforced_state_consumer_is_named_in_the_preview_inventory() {
    let found = enforced_state_calls();
    let expected: std::collections::BTreeMap<&str, usize> = ENFORCED_STATE_CONSUMERS
        .iter()
        .map(|(site, n, _)| (*site, *n))
        .collect();

    let added: Vec<String> = found
        .iter()
        .filter(|(site, n)| expected.get(site.as_str()).is_none_or(|e| *n > e))
        .map(|(site, n)| format!("{site} x{n} (declared: {:?})", expected.get(site.as_str())))
        .collect();
    assert!(
        added.is_empty(),
        "NEW consumer(s) of `enforced_state`: {added:?}. Every one of them is something \
         `[resilience] enforce = true` changes, and the enforcement preview reports only what \
         `preview::CONSEQUENCES` names — so an unlisted consumer makes \
         `GET /enforcement/preview` claim, optimistically and silently, that flipping the flag \
         would change less than it would. Decide which consequence this applies (adding one to \
         CONSEQUENCES, PreviewConsequences and `record` if it is new), then add a row to \
         ENFORCED_STATE_CONSUMERS."
    );

    let gone: Vec<String> = expected
        .iter()
        .filter(|(site, n)| found.get(**site).is_none_or(|f| f < n))
        .map(|(site, n)| format!("{site} x{n} (actual: {:?})", found.get(*site)))
        .collect();
    assert!(
        gone.is_empty(),
        "ENFORCED_STATE_CONSUMERS over-counts call sites that no longer exist — a consumer was \
         removed (fine) but the inventory still claims it: {gone:?}"
    );
}

/// The other half: the inventory's consequence labels must be the preview's own
/// vocabulary, and every consequence the preview reports must be applied by some
/// real call site. A `CONSEQUENCES` row nothing in the workspace applies is a
/// consequence the preview counts and enforcement never produces.
#[test]
fn the_inventory_and_the_previewed_consequences_name_the_same_things() {
    use pumper_core::resilience::preview::CONSEQUENCES;
    use std::collections::BTreeSet;

    let previewed: BTreeSet<&str> = CONSEQUENCES.iter().map(|(name, _)| *name).collect();
    let applied: BTreeSet<&str> = ENFORCED_STATE_CONSUMERS
        .iter()
        .flat_map(|(_, _, names)| names.iter().copied())
        .collect();

    let invented: Vec<&&str> = applied.difference(&previewed).collect();
    assert!(
        invented.is_empty(),
        "ENFORCED_STATE_CONSUMERS labels {invented:?}, which `preview::CONSEQUENCES` does not \
         name — a consequence the preview would never count"
    );
    let unapplied: Vec<&&str> = previewed.difference(&applied).collect();
    assert!(
        unapplied.is_empty(),
        "`preview::CONSEQUENCES` names {unapplied:?}, which no scanned call site applies — the \
         preview reports a consequence enforcement cannot produce"
    );
}

/// The scanner must not be fooled by the prose that documents the observe/enforce
/// split, and must not miss a rustfmt-wrapped chain — the two ways a source scan
/// silently degrades into a test that passes on everything.
#[test]
fn prose_is_not_a_call_site_and_a_wrapped_chain_still_is() {
    assert!(enforced_state_exprs(&scannable_source(
        "// `enforced_state` answers Healthy while soaking\n/// see ctx.health.enforced_state\n"
    ))
    .is_empty());
    assert_eq!(
        enforced_state_exprs(&scannable_source(
            "        let state = ctx.health.enforced_state(&ctx.app, dataset).await;\n"
        )),
        vec!["ctx.health.enforced_state".to_string()]
    );
    let wrapped = "let s = state\n            .health\n            .enforced_state(app, dataset)\n            .await;\n";
    assert_eq!(
        enforced_state_exprs(&scannable_source(wrapped)),
        vec!["state.health.enforced_state".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Arming has exactly one predicate
// ---------------------------------------------------------------------------

/// Every place in the workspace's *source* that reads the configured
/// `[resilience] enforce_sources` list, as `<repo-relative path>` →
/// occurrences.
///
/// **Why this guard and not a sentence in a doc.** The whole value of
/// `enforcement_armed` is that there is ONE expression between config and an
/// enforcement consequence. The way that rots is a second reader: some later
/// call site asks `cfg.enforce_sources.contains(..)` inline, with slightly
/// different matching (a prefix, a case fold, a trimmed entry), and arming now
/// means two things depending on which consumer you ask. That divergence is
/// silent and it is the optimistic direction again — a source the operator
/// believes is unarmed being gated by the reader that matched loosely.
///
/// So the readers are pinned, and the needle is a **config access**
/// ([`CONFIG_READ_NEEDLES`]) rather than the bare identifier. The first draft of
/// this guard matched `enforce_sources` anywhere and went red on the preview's
/// own response field and on the OpenAPI prose that documents it — a response
/// key named after a config key is not a second reader of it, and a matcher
/// that cannot tell those apart is a matcher nobody will keep.
///
/// What is left is the real claim: `Resilience` is the only type that reads the
/// list. `config.rs` declares it, `detect.rs` takes it as a parameter, and
/// everything else — the four worker gates, the health routes, `app.rs`, every
/// app — reaches arming through `enforced_state`/`enforcing_any`/`armed`/
/// `arming`. That is why
/// `every_enforced_state_consumer_is_named_in_the_preview_inventory` above did
/// not have to change for this feature at all.
///
/// Adding a row means adding a reader. Before you do: can the call site use
/// `Resilience::armed` instead? It almost certainly can.
const ENFORCE_SOURCES_READERS: &[(&str, usize)] = &[
    // The only consumer, and exactly three expressions: the cheap
    // short-circuit (`enforcing_any`), the per-source gate (`armed_source`),
    // and the posture the preview is rendered against (`arming`).
    ("crates/core/src/resilience/store.rs", 3),
];

/// What counts as reading the configured list: an access through the config
/// struct. A parameter named `enforce_sources` (that is `detect.rs`, which is
/// handed the list and is the one place allowed to interpret it) and a response
/// field of the same name are not reads of the config.
const CONFIG_READ_NEEDLES: &[&str] = &["cfg.enforce_sources", "resilience.enforce_sources"];

/// This file names the key in its own prose and literals.
const SELF_PATH_ARMING: &str = "crates/core/tests/enforcement_preview.rs";

/// Config reads of the arming list on one scannable (comment-free, joined)
/// source text.
fn config_reads(scannable: &str) -> usize {
    CONFIG_READ_NEEDLES
        .iter()
        .map(|needle| scannable.matches(needle).count())
        .sum()
}

fn enforce_sources_readers() -> std::collections::BTreeMap<String, usize> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let mut found: std::collections::BTreeMap<String, usize> = Default::default();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&root)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        // Tests are free to set the key; this pins who READS it in the product.
        if rel == SELF_PATH_ARMING || !rel.contains("/src/") {
            continue;
        }
        let n = config_reads(&scannable_source(&text));
        if n > 0 {
            found.insert(rel, n);
        }
    }
    found
}

#[test]
fn enforcement_armed_is_the_only_predicate_between_config_and_a_consequence() {
    let found = enforce_sources_readers();
    let expected: std::collections::BTreeMap<&str, usize> =
        ENFORCE_SOURCES_READERS.iter().copied().collect();

    let added: Vec<String> = found
        .iter()
        .filter(|(f, n)| expected.get(f.as_str()).is_none_or(|e| *n > e))
        .map(|(f, n)| format!("{f} x{n} (declared: {:?})", expected.get(f.as_str())))
        .collect();
    assert!(
        added.is_empty(),
        "NEW reader(s) of `[resilience] enforce_sources`: {added:?}. Arming must have exactly \
         one predicate — `resilience::detect::enforcement_armed`, fronted by `cfg.enabled` in \
         `Resilience::armed`. A second reader with its own matching rule makes `armed` mean two \
         different things depending on which consumer is asked, and the divergence is silent. \
         Use `Resilience::armed` / `armed_source` / `enforcing_any` instead; if you genuinely \
         need a new reader, add a row here and say why."
    );

    let gone: Vec<String> = expected
        .iter()
        .filter(|(f, n)| found.get(**f).is_none_or(|a| a < n))
        .map(|(f, n)| format!("{f} x{n} (actual: {:?})", found.get(*f)))
        .collect();
    assert!(
        gone.is_empty(),
        "ENFORCE_SOURCES_READERS over-counts: a reader was removed (fine) but the inventory \
         still claims it: {gone:?}"
    );
}

/// The scanner must not count the prose that documents arming — every file it
/// walks explains the rule in a doc comment directly over code that uses it —
/// nor a response field that merely echoes the key's name, nor the parameter
/// `detect.rs` is handed. It MUST count a rustfmt-wrapped config access.
#[test]
fn arming_prose_and_a_response_field_are_not_config_reads() {
    // Prose, including a commented-out config access.
    assert_eq!(
        config_reads(&scannable_source(
            "/// `enforce_sources` arms one source\n// cfg.enforce_sources.contains(id)\n"
        )),
        0
    );
    // The response field of the same name, and the struct literal that fills
    // it from an already-resolved `Arming`.
    assert_eq!(
        config_reads(&scannable_source(
            "    pub enforce_sources: Vec<String>,\n        enforce_sources: \
             arming.sources.clone(),\n"
        )),
        0
    );
    // The pure predicate's parameter: handed the list, not reading the config.
    assert_eq!(
        config_reads(&scannable_source(
            "pub fn enforcement_armed(enforce: bool, enforce_sources: &[String], id: &str) \
             -> bool {\n    enforce || enforce_sources.iter().any(|s| s == id)\n}\n"
        )),
        0
    );
    // A real read, wrapped by rustfmt across four lines, is still one read.
    assert_eq!(
        config_reads(&scannable_source(
            "        super::detect::enforcement_armed(\n            self.cfg.enforce,\n            \
             &self.cfg\n                .enforce_sources,\n        )\n"
        )),
        1
    );
    // And the server's route-level shape, which must never appear.
    assert_eq!(
        config_reads(&scannable_source(
            "    let n = state.config.resilience.enforce_sources.len();\n"
        )),
        1
    );
}

// ---------------------------------------------------------------------------
// Arming, as the preview reports it
// ---------------------------------------------------------------------------

/// `observe`, but into a named dataset, so a test can build a two-source fleet.
async fn observe_into(health: &Resilience, dataset: &'static str, docs: &[ObservedDoc]) {
    health
        .observe(
            "extractor",
            &RunReport {
                job_id: Uuid::new_v4(),
                dataset,
                docs,
                fetch: FetchHealth {
                    attempted: docs.len() as u32,
                    ok: docs.len() as u32,
                    ..Default::default()
                },
                build_id: Some("test".into()),
            },
        )
        .await
        .expect("observe")
        .expect("detection is enabled");
}

/// Two sources, both walked into quarantine, so arming is the only difference
/// between them.
async fn two_broken_sources(health: &Resilience) {
    for dataset in ["products", "other"] {
        for _ in 0..4 {
            observe_into(health, dataset, &cohort(30, "price", ".price")).await;
        }
        for _ in 0..3 {
            observe_into(health, dataset, &cohort(30, "amount", ".price")).await;
        }
    }
}

/// Case 6: arming one source moves exactly that source's counts from `soaking`
/// to `armed`, and changes no other number.
#[tokio::test]
async fn arming_one_source_moves_only_that_source_from_soaking_to_armed() {
    let store = TempStore::new("enforcement-preview-arm-one").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    two_broken_sources(&health).await;
    let hstore = health.store().unwrap();

    let soaking = preview_fleet(hstore, &Arming::default(), None, 60, 500)
        .await
        .expect("preview");
    assert_eq!(soaking.armed, ArmedScope::None);
    assert!(
        soaking.sources.iter().all(|s| !s.armed),
        "nothing armed: every source reports armed = false"
    );
    assert_eq!(
        soaking.consequences.armed,
        Default::default(),
        "nothing is being gated, so the armed column must be empty"
    );
    assert_eq!(
        soaking.consequences.soaking, soaking.totals,
        "with nothing armed, soaking IS the total"
    );
    assert!(
        soaking.totals.diverted_writes.runs > 0,
        "the fixture must actually have consequences to split"
    );

    let armed = preview_fleet(
        hstore,
        &Arming::only(&["extractor/products"]),
        None,
        60,
        500,
    )
    .await
    .expect("preview");
    assert_eq!(armed.armed, ArmedScope::Some);
    assert_eq!(
        armed.enforce_sources,
        vec!["extractor/products".to_string()]
    );
    assert!(armed.redundant_sources.is_empty());
    assert!(armed.armed_unseen.is_empty());
    assert!(!armed.enforcing, "the fleet-wide flip is still off");

    let products = armed
        .sources
        .iter()
        .find(|s| s.id == "extractor/products")
        .expect("products");
    let other = armed
        .sources
        .iter()
        .find(|s| s.id == "extractor/other")
        .expect("other");
    assert!(products.armed, "the named source is armed");
    assert!(!other.armed, "its neighbour is not");

    // The split is exactly the per-source numbers, and the total is unmoved:
    // arming changes who is being gated, never what the replay counted.
    assert_eq!(armed.consequences.armed, products.consequences);
    assert_eq!(armed.consequences.soaking, other.consequences);
    assert_eq!(
        armed.totals, soaking.totals,
        "arming a source must not change a single replayed count"
    );
}

/// Case 4: `enforce = true` beside a list arms everything and says the list is
/// doing nothing, rather than letting the list narrow a fleet that is already
/// enforced.
#[tokio::test]
async fn the_flip_arms_everything_and_names_the_list_as_redundant() {
    let store = TempStore::new("enforcement-preview-redundant").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    two_broken_sources(&health).await;

    let arming = Arming {
        enforce: true,
        sources: vec!["extractor/products".to_string()],
    };
    let preview = preview_fleet(health.store().unwrap(), &arming, None, 60, 500)
        .await
        .expect("preview");

    assert_eq!(preview.armed, ArmedScope::All);
    assert!(preview.enforcing);
    assert!(
        preview.sources.iter().all(|s| s.armed),
        "every source is armed under the flip, including the one the list omits"
    );
    assert_eq!(
        preview.redundant_sources,
        vec!["extractor/products".to_string()],
        "the list is redundant under the flip, and the operator is told so"
    );
    assert_eq!(
        preview.consequences.soaking,
        Default::default(),
        "nothing is soaking when everything is armed"
    );
    assert_eq!(preview.consequences.armed, preview.totals);
}

/// Case 5: a typo arms nothing, and arming nothing looks exactly like a fleet
/// that is already fine — so the unmatched entry is named.
#[tokio::test]
async fn a_typo_in_the_arming_list_is_named_not_silently_ignored() {
    let store = TempStore::new("enforcement-preview-typo").await;
    let health = Resilience::new(store.storage.pool(), &cfg());
    two_broken_sources(&health).await;

    let preview = preview_fleet(
        health.store().unwrap(),
        &Arming::only(&["extractr/products", "extractor/other"]),
        None,
        60,
        500,
    )
    .await
    .expect("preview");

    assert_eq!(
        preview.armed_unseen,
        vec!["extractr/products".to_string()],
        "the typo is named; the entry that matched is not"
    );
    // And the real entry still worked: a reported typo must not be a reason to
    // distrust the rest of the rollout.
    let other = preview
        .sources
        .iter()
        .find(|s| s.id == "extractor/other")
        .expect("other");
    assert!(other.armed);
    assert_eq!(preview.consequences.armed, other.consequences);
}
