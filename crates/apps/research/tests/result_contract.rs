//! `AppManifest::output_shape` is the contract a consumer codes against: it is
//! published verbatim by `GET /apps`, `GET /apps?format=tools` and the MCP tool
//! manifest (`server/src/registry.rs`), and an agent that has never read this
//! crate's source has nothing else to key on.
//!
//! Research's declaration had drifted further than any other app measured here.
//! It declared `summary`, `key_findings` and `sources` at the TOP level, where
//! `run()` never puts them — they live inside `report`, and only when
//! `structured` is true, so a consumer coding against the declaration read
//! `undefined` on every job — and it omitted six keys the run does emit
//! (`query`, `report`, `structured`, `resumed`, `duration_ms`, `num_turns`).
//!
//! Prose cannot hold that line: the declaration and the `json!` block that
//! builds the result live 300 lines apart. So this file derives the emitted
//! shape from REAL runs and the declared shape from the published string, and
//! diffs them — the inventory / EXPECTED-diff idiom the repo uses for
//! conventions (`crates/server/src/routes/mod.rs`), already built for
//! grants-gov, plugin and crawl.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use app_research::Research;
use pumper_core::testing::TestContext;
use pumper_core::testing::{engines_with, research_output, Dead, ScriptedResearcher, TempStore};
use pumper_core::ScrapeApp;
use serde_json::{json, Value};

/// A run whose agent answers in the promised shape: `report` is an object.
async fn structured_run() -> Value {
    let store = TempStore::new("research-contract-structured").await;
    let researcher = Arc::new(ScriptedResearcher::new().always_text(
        r#"{"summary":"Rust 1.80 stabilized LazyLock.","key_findings":["LazyLock is in std::sync"],"sources":[{"url":"https://blog.rust-lang.org","title":"Rust 1.80"}]}"#,
    ));
    let ctx = TestContext::new(&store.storage, "research")
        .params(json!({ "query": "what changed in rust 1.80" }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), researcher))
        .build();
    Research
        .run(ctx)
        .await
        .expect("a shaped reply completes the run")
}

/// A run whose agent never shapes a report but does return text: `report` is a
/// bare string. Same key set, different value type — the reason the contract
/// has to say so instead of hoisting the report's children.
async fn unstructured_run() -> Value {
    let store = TempStore::new("research-contract-unstructured").await;
    let researcher = Arc::new(ScriptedResearcher::new().on("", research_output("prose, not json")));
    let ctx = TestContext::new(&store.storage, "research")
        .params(json!({ "query": "q", "turns_per_step": 1 }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), researcher))
        .build();
    Research
        .run(ctx)
        .await
        .expect("an unstructured but non-empty reply still succeeds")
}

/// The zero-spend path: a re-claimed attempt whose checkpoint already carries
/// the finished result. It returns the STORED result, so its key set is the one
/// a prior attempt emitted plus `resumed_from_checkpoint`.
async fn restored_finished_run() -> Value {
    let store = TempStore::new("research-contract-restored").await;
    let finished = structured_run().await;
    let ctx = TestContext::new(&store.storage, "research")
        .params(json!({ "query": "q" }))
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), Arc::new(Dead)))
        .restored(json!({
            "v": 1,
            "session_id": "sess-1",
            "steps_done": 1,
            "spent_usd": 0.42,
            "result": finished,
        }))
        .build();
    Research
        .run(ctx)
        .await
        .expect("a finished checkpoint is returned without new spend")
}

// ---- the declared shape, parsed out of the published string ----------------

/// The inside of the leading `{…}` key list, brace-matched (so a nested block
/// like `report: {…}` does not terminate it early) and stopping before the
/// prose.
fn brace_body(decl: &str) -> &str {
    let start = decl
        .find('{')
        .expect("output_shape opens with its key list");
    let mut depth = 0usize;
    for (i, c) in decl[start..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &decl[start + 1..start + i];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces in output_shape: {decl}");
}

/// Splits on commas at nesting depth 0 only.
fn split_top(body: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut last = 0usize;
    for (i, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&body[last..i]);
                last = i + 1;
            }
            _ => {}
        }
    }
    out.push(&body[last..]);
    out.into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect()
}

/// `sources[]` → `sources`; `report: {…}` → `report`.
fn key_name(entry: &str) -> String {
    entry
        .split(':')
        .next()
        .unwrap_or(entry)
        .trim()
        .trim_end_matches(['[', ']', '?'])
        .trim()
        .to_string()
}

fn declaration() -> &'static str {
    Research
        .manifest()
        .output_shape
        .expect("research declares an output_shape")
}

/// Declared top-level key → its declared nested keys (empty for scalars).
fn declared_shape() -> BTreeMap<String, BTreeSet<String>> {
    split_top(brace_body(declaration()))
        .into_iter()
        .map(|entry| {
            let nested = if entry.contains('{') {
                split_top(brace_body(entry))
                    .into_iter()
                    .map(key_name)
                    .collect()
            } else {
                BTreeSet::new()
            };
            (key_name(entry), nested)
        })
        .collect()
}

/// Emitted top-level key → its emitted nested keys (empty for non-objects).
fn emitted_shape(out: &Value) -> BTreeMap<String, BTreeSet<String>> {
    out.as_object()
        .expect("the job result is a JSON object")
        .iter()
        .map(|(k, v)| {
            let nested = v
                .as_object()
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            (k.clone(), nested)
        })
        .collect()
}

fn missing_from(
    a: &BTreeMap<String, BTreeSet<String>>,
    b: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    a.keys().filter(|k| !b.contains_key(*k)).cloned().collect()
}

fn assert_same_keys(out: &Value, what: &str) {
    let declared = declared_shape();
    let emitted = emitted_shape(out);
    let declared_but_never_emitted = missing_from(&declared, &emitted);
    let emitted_but_undeclared = missing_from(&emitted, &declared);
    assert!(
        declared_but_never_emitted.is_empty(),
        "output_shape promises keys the {what} never emits — a consumer coding \
         against the manifest reads undefined: {declared_but_never_emitted:?}\nresult was {out:#}"
    );
    assert!(
        emitted_but_undeclared.is_empty(),
        "the {what} emits keys output_shape never declares, so no consumer can \
         discover them: {emitted_but_undeclared:?}"
    );
}

#[tokio::test]
async fn the_published_output_shape_is_what_a_structured_run_emits() {
    let out = structured_run().await;
    assert_same_keys(&out, "structured run");

    // …and one level down, where the whole drift lived: `summary`,
    // `key_findings` and `sources` are children of `report`, not top-level keys.
    let declared = declared_shape();
    let emitted = emitted_shape(&out);
    assert_eq!(
        declared["report"], emitted["report"],
        "the declared children of `report` disagree with what the run nests there"
    );
    for phantom in ["summary", "key_findings", "sources"] {
        assert!(
            !declared.contains_key(phantom),
            "`{phantom}` is declared at the top level, where run() never puts it"
        );
    }
}

#[tokio::test]
async fn an_unstructured_run_emits_the_same_keys_with_report_as_a_string() {
    // The value type of `report` changes with `structured`; the KEY SET does
    // not. A declaration that hoisted the report's children would be wrong for
    // this run at any depth, so it has to name the string case in prose.
    let out = unstructured_run().await;
    assert_same_keys(&out, "unstructured run");
    assert_eq!(out["structured"], json!(false));
    assert!(out["report"].is_string(), "{out:#}");
    assert!(
        declaration().contains("bare string"),
        "the declaration must say what `report` is when `structured` is false: {}",
        declaration()
    );
}

#[tokio::test]
async fn the_restored_finished_result_emits_the_same_keys() {
    // The zero-spend re-claim path returns a STORED result with
    // `resumed_from_checkpoint` stamped on it. If it could drop or add a key,
    // strict equality above would be unsafe to rely on.
    let out = restored_finished_run().await;
    assert_same_keys(&out, "restored finished run");
    assert_eq!(out["resumed_from_checkpoint"], json!(true));
}

#[tokio::test]
async fn every_stop_reason_the_declaration_lists_is_one_the_code_can_emit() {
    // The declaration enumerates `stop_reason`'s vocabulary; a value listed
    // there that no code path produces is the same lie as a phantom key.
    let decl = declaration();
    for reason in [
        "completed",
        "step_cap",
        "turns_exhausted",
        "budget_exhausted",
        "no_session",
        "single_call",
    ] {
        assert!(decl.contains(reason), "`{reason}` is missing from: {decl}");
    }
    assert_eq!(structured_run().await["stop_reason"], json!("completed"));
    assert_eq!(unstructured_run().await["stop_reason"], json!("no_session"));
}
