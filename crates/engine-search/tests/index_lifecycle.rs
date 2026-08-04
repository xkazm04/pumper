//! Index lifecycle safety: what happens when `TantivyIndex::new` meets a
//! directory it cannot use as-is. Three states, three outcomes — rebuild
//! (schema drift), quarantine + boot (corrupt `meta.json`), and refuse loudly
//! (someone else holds the writer lock). The last one is the one that used to
//! delete a running server's index out from under it.

use std::path::{Path, PathBuf};

use pumper_core::config::SearchConfig;
use pumper_core::{Search, SearchDoc, SearchRequest};
use pumper_engine_search::TantivyIndex;
use tantivy::directory::{MmapDirectory, INDEX_WRITER_LOCK};
use tantivy::schema::{Schema, INDEXED, STORED, STRING, TEXT};
use tantivy::{Directory, Index};

/// Tantivy's writer-lock filename, spelled out so the assertion checks the
/// message an operator will actually read.
const WRITER_LOCK: &str = ".tantivy-writer.lock";

fn unique_dir(tag: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "pumper-search-life-{tag}-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(quarantine_of(&dir));
    dir
}

/// The first quarantine sibling `TantivyIndex::new` would pick for `dir`.
fn quarantine_of(dir: &Path) -> PathBuf {
    dir.with_file_name(format!(
        "{}.corrupt.0",
        dir.file_name().unwrap().to_str().unwrap()
    ))
}

fn open(dir: &Path) -> pumper_core::Result<TantivyIndex> {
    TantivyIndex::new(&SearchConfig {
        enabled: true,
        dir: dir.to_path_buf(),
        ..Default::default()
    })
}

/// An index built by a *previous* build of pumper: the pre-M14 schema, i.e.
/// everything `SCHEMA_FIELDS` lists except the entity-enrichment fields
/// (`amount`, `event_date`). That absence is exactly what `schema_is_current`
/// detects.
fn create_old_schema_index(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let mut b = Schema::builder();
    b.add_text_field("id", STRING | STORED);
    b.add_text_field("app", STRING | STORED);
    b.add_text_field("dataset", STRING | STORED);
    b.add_text_field("url", STRING | STORED);
    b.add_text_field("title", TEXT | STORED);
    b.add_text_field("body", TEXT | STORED);
    b.add_i64_field("indexed_at", INDEXED | STORED);
    Index::create_in_dir(dir, b.build()).expect("old-schema index");
}

fn doc(id: &str) -> SearchDoc {
    SearchDoc {
        id: id.to_string(),
        app: "hn".into(),
        dataset: "_job".into(),
        url: String::new(),
        title: format!("Result {id}"),
        body: "a rural health grant opportunity".into(),
        indexed_at: 1,
    }
}

/// The corrupt-dir anti-pattern: `open_in_dir` fails on the bad `meta.json`,
/// `create_in_dir` ALSO fails (it refuses a directory that already has one), so
/// the process cannot boot at all — while the docs promised "rebuilt empty".
#[tokio::test]
async fn corrupt_meta_is_quarantined_not_a_boot_failure() {
    let dir = unique_dir("corrupt");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("meta.json"), b"{ this is not json").unwrap();

    let index = open(&dir).expect("a corrupt index dir must not stop the process booting");

    // The bad directory is aside under a deterministic, counted name.
    let aside = quarantine_of(&dir);
    assert!(
        aside.is_dir(),
        "the unopenable dir is quarantined: {aside:?}"
    );
    assert_eq!(
        std::fs::read(aside.join("meta.json")).unwrap(),
        b"{ this is not json",
        "quarantine preserves the evidence rather than deleting it"
    );

    // And the fresh index in its place actually works.
    index.index(vec![doc("hn:1")]).await.unwrap();
    index.flush().await.unwrap();
    assert_eq!(index.doc_count().await.unwrap(), 1);

    drop(index);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&aside);
}

/// Schema drift still rebuilds empty — the guard must not turn a routine
/// version bump into a boot failure.
#[tokio::test]
async fn outdated_schema_rebuilds_empty_not_unopenable() {
    let dir = unique_dir("drift");
    create_old_schema_index(&dir);

    let index = open(&dir).expect("an outdated schema is rebuilt, not fatal");
    assert_eq!(index.doc_count().await.unwrap(), 0, "rebuilt EMPTY");
    index.index(vec![doc("hn:1")]).await.unwrap();
    index.flush().await.unwrap();
    assert_eq!(
        index
            .query(SearchRequest::new("grant", 10))
            .await
            .unwrap()
            .hits
            .len(),
        1,
        "the rebuilt index carries this build's schema and indexes normally"
    );
    // A wipe is a wipe: nothing is quarantined, so drift can't fill the disk
    // with carcasses.
    assert!(
        !quarantine_of(&dir).exists(),
        "drift wipes, it does not quarantine"
    );
    // The rebuild released the lock it took (the live writer holds it now, and
    // dropping the index releases that too).
    drop(index);
    let _ = std::fs::remove_dir_all(&dir);
}

/// **The race this direction exists for.** A new-schema binary starting while an
/// old-schema process is live used to `remove_dir_all` the index before any lock
/// was taken — on Unix that silently deletes the running server's index.
///
/// The holder here is the very lock a live `IndexWriter` takes
/// (`INDEX_WRITER_LOCK` on an `MmapDirectory` = `flock`/`LockFileEx` on a handle
/// to `.tantivy-writer.lock`). Those are per-open-file-description, so a second
/// handle inside *this* process conflicts exactly as another process's would —
/// the only thing a real two-process test would add is the process boundary,
/// which the OS lock does not distinguish. Spawning a second binary is beyond
/// this suite's harness.
#[tokio::test]
async fn contested_dir_fails_loudly_not_silently_wiped() {
    let dir = unique_dir("contested");
    create_old_schema_index(&dir);
    let before = std::fs::read(dir.join("meta.json")).unwrap();
    // Stand in for the live old-schema process's IndexWriter.
    let holder = MmapDirectory::open(&dir)
        .unwrap()
        .acquire_lock(&INDEX_WRITER_LOCK)
        .expect("nobody else holds it yet");

    let Err(err) = open(&dir) else {
        panic!("a contested wipe must fail, not proceed");
    };
    let msg = err.to_string();
    assert!(
        msg.contains(WRITER_LOCK),
        "the error names the conflicting lock: {msg}"
    );
    assert!(
        msg.contains(dir.to_str().unwrap()),
        "and the directory it refused to touch: {msg}"
    );
    assert!(
        msg.contains("Stop that process"),
        "and what the operator should do about it: {msg}"
    );
    assert_eq!(
        std::fs::read(dir.join("meta.json")).unwrap(),
        before,
        "the other process's index is untouched — no wipe happened"
    );

    // Once the holder is gone, the same call rebuilds as normal.
    drop(holder);
    let index = open(&dir).expect("uncontested, the rebuild proceeds");
    assert_eq!(index.doc_count().await.unwrap(), 0);
    drop(index);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A first boot must not trip any of the recovery paths: an empty (or absent)
/// directory is just created, with no lock dance and no quarantine.
#[tokio::test]
async fn first_boot_creates_without_quarantining_an_empty_dir() {
    let dir = unique_dir("fresh");
    let index = open(&dir).expect("first boot");
    assert_eq!(index.doc_count().await.unwrap(), 0);
    assert!(
        !quarantine_of(&dir).exists(),
        "nothing to quarantine on a fresh dir"
    );
    drop(index);
    let _ = std::fs::remove_dir_all(&dir);
}
