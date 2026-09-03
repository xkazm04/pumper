//! **A typed cause must not be consumed into prose at the moment it is most
//! structured.**
//!
//! `.map_err(|e| Error::config(format!("{}: {e}", path.display())))` is the
//! shape. At that line `e` is a `toml::de::Error` with a span, a line and a
//! column; one line later it is a sentence, and every consumer downstream — the
//! job row, the receipt, the trigger ledger, the doctor — has the same sentence
//! and no way back. The tree already found this out twice one layer down and
//! fixed it both times by adding a typed field (`PluginFailure`, `SourceDrift`);
//! the cause itself was the same smuggling, unfixed.
//!
//! The guard is the EXPECTED-diff idiom the terminal classification already uses
//! (`error.rs`'s `expected_terminal`): every site where one of the four
//! cause-carrying constructors is handed a `format!` that interpolates a cause
//! binding is pinned, with a reason, so a NEW one fails the build instead of
//! being noticed later.
//!
//! Note what this is not: it is not a ban on `format!` in an error message. A
//! message built from a path, a key or a count is exactly right. What must not
//! happen is that a `{e}` goes in and the `e` does not.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Sites that interpolate a cause into the message and do **not** attach it,
/// as `<repo-relative path>::<line>` → the reason it is allowed.
///
/// Every entry is a decision. Before adding one: can the cause be attached with
/// `Error::{http,browser,parse,config}_from`? The bar for a row here is that the
/// cause genuinely cannot be carried — not that carrying it was inconvenient.
const EXPECTED_FLATTENED: &[(&str, &str)] = &[
    // `scraper`'s `SelectorErrorKind` is neither `Send` nor `Sync` nor
    // `'static` (it holds an `Rc<String>` and a `NonNull`), and `Error` crosses
    // `tokio::spawn` everywhere, so it cannot be boxed into a cause. The `{e:?}`
    // rendering is all there is; the alternative is a `Send` wrapper type that
    // carries nothing the Debug string does not.
    (
        "crates/core/src/extract.rs::css-selector compile",
        "scraper::SelectorErrorKind is !Send + !Sync + !'static",
    ),
    (
        "crates/core/src/extract.rs::each-selector compile",
        "scraper::SelectorErrorKind is !Send + !Sync + !'static",
    ),
    (
        "crates/core/src/extract.rs::each-container compile",
        "scraper::SelectorErrorKind is !Send + !Sync + !'static",
    ),
];

/// The crates this guard governs: the ones where the classifiers live, and the
/// ones the first pass converted. `crates/apps/**` and the other engines are
/// deliberately out of scope — the pattern spreads into them deliberately, and
/// pinning them here would turn a landing into a workspace-wide rewrite.
const GOVERNED: &[&str] = &["crates/core", "crates/engine-http"];

/// Constructors that can carry a cause, and therefore must when one is
/// interpolated.
const CAUSE_CARRYING: &[&str] = &["http", "browser", "parse", "config"];

#[test]
fn no_governed_site_interpolates_a_cause_without_attaching_it() {
    let found = flattening_sites();
    let allowed = EXPECTED_FLATTENED.len();
    assert_eq!(
        found.len(),
        allowed,
        "flattening sites in {GOVERNED:?}: {found:#?}\n\nEach one interpolates a \
         typed cause into a message and drops the value. Attach it with \
         `Error::<ctor>_from(message, e)`, or — if the cause genuinely cannot be \
         carried — add a row to EXPECTED_FLATTENED naming the reason."
    );
}

/// The paired assertion, and the one that keeps the count above honest: the
/// four constructors ARE used with their causes, in quantity. A guard that
/// passes because nobody raises these variants any more measures nothing.
#[test]
fn the_governed_crates_actually_attach_causes() {
    let attached = attached_sites();
    assert!(
        attached >= 15,
        "only {attached} sites attach a typed cause; the conversion was undone \
         or the guard is measuring an empty tree"
    );
}

/// A cause survives as a value, not as text: the chain answers what the type
/// was, and a consumer can downcast to ask the cause its own questions.
#[test]
fn an_attached_cause_is_still_a_value_a_consumer_can_ask_questions_of() {
    let toml_err = toml::from_str::<toml::Value>("this is = = not toml").unwrap_err();
    let line_col = toml_err.to_string();
    let e = pumper_core::Error::config_from(format!("config.toml: {toml_err}"), toml_err);

    // The human sentence did not move: the cause's Display is still in it.
    assert!(e.to_string().starts_with("config: config.toml:"), "{e}");
    // And the value is still there, as itself.
    let cause = e.cause().expect("the cause is attached");
    assert!(
        cause.downcast_ref::<toml::de::Error>().is_some(),
        "the cause must survive as a `toml::de::Error`, not as its own Display"
    );
    assert_eq!(e.cause_chain().as_deref(), Some(line_col.as_str()));

    // A failure with no cause says so rather than inventing an empty link.
    assert!(pumper_core::Error::config("no [worker] section")
        .cause()
        .is_none());
    assert!(pumper_core::Error::config("no [worker] section")
        .cause_chain()
        .is_none());
}

// ---- the scanner -----------------------------------------------------------

/// Every `Error::<ctor>(format!(…{e}…))` site in the governed crates, as
/// `<repo-relative path>::<line>`.
fn flattening_sites() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (path, text) in governed_sources() {
        for (line_no, call) in constructor_calls(&text, false) {
            if interpolates_a_cause(&call) {
                out.insert(format!("{path}:{line_no}"));
            }
        }
    }
    out
}

/// How many sites use a `_from` constructor — the paired count.
fn attached_sites() -> usize {
    governed_sources()
        .into_values()
        .map(|text| constructor_calls(&text, true).len())
        .sum()
}

/// `(<repo-relative path>, source)` for every `.rs` file in the governed crates.
fn governed_sources() -> BTreeMap<String, String> {
    let root = workspace_root();
    let mut out = BTreeMap::new();
    for crate_dir in GOVERNED {
        let mut files = Vec::new();
        rust_sources(&root.join(crate_dir), &mut files);
        for file in files {
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            // This file names the shapes it bans; scanning itself would make it
            // its own violation.
            if rel.ends_with("tests/error_cause_chain.rs") {
                continue;
            }
            if let Ok(text) = std::fs::read_to_string(&file) {
                out.insert(rel, text);
            }
        }
    }
    out
}

/// Every cause-carrying constructor call in `text` that is handed a `format!`,
/// as `(1-based line, the call's source text)`. `from` selects the `_from`
/// spelling instead of the plain one.
fn constructor_calls(text: &str, from: bool) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    for ctor in CAUSE_CARRYING {
        let needle = if from {
            format!("Error::{ctor}_from(")
        } else {
            format!("Error::{ctor}(")
        };
        let mut at = 0;
        while let Some(i) = text[at..].find(&needle) {
            let start = at + i;
            at = start + needle.len();
            // `Error::http(` is a prefix of `Error::http_from(`; skip the
            // overlap so the plain scan does not count the attaching sites.
            if !from && text[start..].starts_with(&format!("Error::{ctor}_from(")) {
                continue;
            }
            // A comment or doc line naming the shape is prose, not a call site —
            // the same rule the fetch-chokepoint inventory applies.
            let line_start = text[..start].rfind('\n').map(|n| n + 1).unwrap_or(0);
            let indent = text[line_start..start].trim_start();
            if indent.starts_with("//") || indent.starts_with('*') || indent.starts_with("///") {
                continue;
            }
            let Some(call) = balanced_call(text, start + needle.len() - 1) else {
                continue;
            };
            out.push((text[..start].matches('\n').count() + 1, call));
        }
    }
    out
}

/// The text of a call whose opening paren is at `open`, up to its match.
fn balanced_call(text: &str, open: usize) -> Option<String> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    for (offset, b) in bytes[open..].iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(text[open..=open + offset].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// Whether a call's message was built **from a cause it did not keep**: either
/// a `format!` interpolating a binding that is conventionally an error (`{e}`,
/// `{err}`, `{e:?}`), or the barest form of the same thing, `e.to_string()`.
///
/// Both spellings lose exactly the same thing, so the guard has to see both —
/// catching only the `format!` half would leave the shortest way to flatten a
/// cause wide open.
fn interpolates_a_cause(call: &str) -> bool {
    ["e", "err", "error", "source", "cause"].iter().any(|name| {
        call.contains(&format!("{{{name}}}"))
            || call.contains(&format!("{{{name}:"))
            || call.contains(&format!("{name}.to_string()"))
    })
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root")
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The operator's half, end to end: a failed job's row records **what the
/// failure was caused by, as a type**.
///
/// `jobs.error` has always held the sentence, and grouping a week of failures by
/// cause meant matching substrings of prose anybody was free to reword. The
/// column is the same fix `PluginFailure` and `SourceDrift` each made one layer
/// down, applied to the cause itself.
#[tokio::test]
async fn a_failed_job_row_records_what_caused_it_as_a_type() {
    use pumper_core::storage::FailReason;
    use pumper_core::testing::TempStore;
    use pumper_core::{EnqueueOptions, Error};

    let store = TempStore::new("cause-kind").await;
    let storage = &store.storage;
    let job = storage
        .enqueue(
            "a",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();

    let toml_err = toml::from_str::<toml::Value>("nope = = nope").unwrap_err();
    let failure = Error::config_from(format!("config.toml: {toml_err}"), toml_err);
    storage
        .fail(claimed.id, claimed.attempts, FailReason::Typed(&failure))
        .await
        .unwrap()
        .expect("the failure landed");

    let row = storage.get(job.id).await.unwrap().unwrap();
    assert_eq!(
        row.cause_kind.as_deref(),
        Some("toml::de::error::Error"),
        "the row must name the cause's type; it is what a query can group by"
    );
    // The sentence is untouched — this adds a column, it does not reword one.
    assert!(row.error.unwrap().starts_with("config: config.toml:"));

    // And a failure with no typed cause says nothing rather than something
    // vague: NULL means "carried no cause", not "cause uninteresting".
    let untyped = storage
        .enqueue(
            "a",
            EnqueueOptions {
                max_attempts: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let claimed = storage.claim_next(&[], 0.0).await.unwrap().unwrap();
    assert_eq!(claimed.id, untyped.id, "the first job is still backing off");
    storage
        .fail(
            claimed.id,
            claimed.attempts,
            FailReason::Text("panicked: index out of bounds"),
        )
        .await
        .unwrap()
        .expect("the failure landed");
    assert_eq!(
        storage.get(untyped.id).await.unwrap().unwrap().cause_kind,
        None
    );
}
