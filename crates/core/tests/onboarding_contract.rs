//! `ONBOARDING.md` is the **agent-facing contract** — the first thing a CLI
//! agent reads before adding an app, and it is trusted literally. Its engine
//! section shipped three consecutive snippets that were wrong, and the most
//! prominent one taught the exact bypass this repo has both a compiler guard
//! and a test guard against (`ctx.engines.claude.research(...)`, where
//! `EngineSet::claude` is `pub(crate)` and `llm_chokepoint.rs` bans the string
//! in app crates outright).
//!
//! **Why they all survived is mechanical.** `scripts/docs/feature-doc-map.json`
//! maps source globs to `docs/features/*` pages only, so `ONBOARDING.md` was the
//! target of no map entry at all and no doc-sync signal ever pointed at it.
//! Fixing the prose without fixing that leaves the next drift equally free.
//!
//! This is the other half: a guard that fails when the doc and the code
//! disagree. It deliberately pins **only what was actually wrong** — the
//! privacy of the model seam, the `ScrapeApp` method surface, and the arity of
//! the two calls the doc got wrong. Prose is free to be rewritten; a test that
//! breaks on every wording edit is worse than the drift it prevents.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root")
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn onboarding() -> String {
    read("ONBOARDING.md")
}

/// Every fenced code block in a Markdown document, fences excluded. Snippets are
/// what a reader *copies*; surrounding prose is what a reader *reads*, and only
/// the first has to compile.
fn code_blocks(md: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<Vec<&str>> = None;
    for line in md.lines() {
        if line.trim_start().starts_with("```") {
            match current.take() {
                Some(body) => blocks.push(body.join("\n")),
                None => current = Some(Vec::new()),
            }
            continue;
        }
        if let Some(body) = current.as_mut() {
            body.push(line);
        }
    }
    blocks
}

/// The anti-pattern this file exists for: the doc taught
/// `ctx.engines.claude.research(req)`, which **cannot compile from an app
/// crate** (`EngineSet::claude` is `pub(crate)`, `engine.rs`) and which
/// `crates/core/tests/llm_chokepoint.rs` bans as a string in app crates
/// regardless. An agent following the doc either hit a compile error or, worse,
/// "worked around it" by reaching past the metering seam.
///
/// Checked in **code blocks only**: the prose now says "there is no
/// `ctx.engines.claude`", and telling a reader the seam does not exist is the
/// opposite of teaching them to use it.
#[test]
fn the_onboarding_snippets_do_not_teach_the_bypass_the_chokepoint_test_bans() {
    let offenders: Vec<String> = code_blocks(&onboarding())
        .into_iter()
        .filter(|b| b.contains("engines.claude"))
        .collect();
    assert!(
        offenders.is_empty(),
        "ONBOARDING.md has {} snippet(s) reaching the researcher directly. \
         `EngineSet::claude` is pub(crate) so this cannot compile from an app \
         crate, and llm_chokepoint.rs bans the string there. The idiom is \
         `ctx.research(request)`. Offending block(s): {offenders:?}",
        offenders.len()
    );
    // The privacy this rests on is the actual guard — if it is ever relaxed,
    // the doc is not what needs changing.
    assert!(
        read("crates/core/src/engine.rs").contains("pub(crate) claude: Arc<dyn Researcher>"),
        "EngineSet::claude is no longer pub(crate) — the metering chokepoint was \
         structural precisely because an app crate could not name the researcher"
    );
}

/// Method names declared inside the `ScrapeApp` trait block of `text`, starting
/// at `header` and ending at the first column-0 `}`.
fn scrape_app_methods(text: &str, header: &str) -> BTreeSet<String> {
    let mut methods = BTreeSet::new();
    let mut inside = false;
    for line in text.lines() {
        if line.contains(header) {
            inside = true;
            continue;
        }
        if !inside {
            continue;
        }
        if line == "}" {
            break;
        }
        let trimmed = line.trim_start();
        let decl = trimmed
            .strip_prefix("async fn ")
            .or_else(|| trimmed.strip_prefix("fn "));
        if let Some(decl) = decl {
            if let Some(name) = decl.split('(').next() {
                methods.insert(name.trim().to_string());
            }
        }
    }
    methods
}

/// The doc presented `ScrapeApp` as **five** methods. It has seven, and the two
/// missing ones are the two an app author most needs to know exist:
/// `manifest()` (a declared `params_schema` makes enqueue enforce 422 instead of
/// failing mid-run, and `registry.rs` asserts at least five apps ship rich ones)
/// and `requires()` (what makes a credential-gated app distinguishable from a
/// broken one in `GET /apps`).
///
/// Names only — signatures and doc comments are free to change.
#[test]
fn the_documented_scrapeapp_surface_is_the_real_one() {
    let real = scrape_app_methods(
        &read("crates/core/src/app.rs"),
        "pub trait ScrapeApp: Send + Sync {",
    );
    assert_eq!(real.len(), 7, "the trait itself changed shape: {real:?}");
    let documented = scrape_app_methods(&onboarding(), "pub trait ScrapeApp: Send + Sync {");
    assert_eq!(
        documented,
        real,
        "ONBOARDING.md's ScrapeApp block no longer matches the trait. Missing \
         from the doc: {:?}; invented by the doc: {:?}",
        real.difference(&documented).collect::<Vec<_>>(),
        documented.difference(&real).collect::<Vec<_>>()
    );
}

/// Top-level argument count of the first `<needle>…)` call or declaration in
/// `text`, where `needle` ends at the opening paren (or just past a `&self` the
/// caller never passes).
///
/// Nested parens, brackets and generics do not split arguments, and a trailing
/// comma is not an eighth parameter — rustfmt writes one on every multi-line
/// signature, so a naive comma count reports every wrapped declaration one
/// argument too wide.
fn arity_after(text: &str, needle: &str) -> usize {
    let start = text
        .find(needle)
        .unwrap_or_else(|| panic!("{needle:?} not found"))
        + needle.len();
    let mut depth = 0usize;
    let mut segments: Vec<String> = vec![String::new()];
    for c in text[start..].chars() {
        match c {
            '(' | '[' | '<' => depth += 1,
            ')' if depth == 0 => break,
            ')' | ']' | '>' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                segments.push(String::new());
                continue;
            }
            _ => {}
        }
        segments
            .last_mut()
            .expect("one segment always exists")
            .push(c);
    }
    segments.iter().filter(|s| !s.trim().is_empty()).count()
}

/// The two arities the doc got wrong, pinned against the declarations they
/// describe.
///
/// `ctx.plugins.run(name, doc)` was shown with two arguments; the trait takes
/// three (`params` is the per-call config envelope that lets one module be
/// reused across jobs). `crawl(http, cfg, out_dir)` was shown with three; it
/// takes six. Both are copy-paste-and-it-fails-to-compile bugs, which is the
/// worst kind in a doc an agent is told to trust literally.
#[test]
fn the_documented_call_arities_match_the_declarations() {
    let doc = onboarding();

    // The needle stops just past `&self`, which a caller does not pass.
    let plugins_decl = arity_after(&read("crates/core/src/plugin.rs"), "async fn run(&self,");
    assert_eq!(
        arity_after(&doc, "ctx.plugins.run("),
        plugins_decl,
        "ONBOARDING.md calls ctx.plugins.run with the wrong number of arguments"
    );

    let crawl_decl = arity_after(&read("crates/core/src/crawl.rs"), "pub async fn crawl(");
    assert_eq!(
        arity_after(&doc, "`crawl("),
        crawl_decl,
        "ONBOARDING.md calls crawl() with the wrong number of arguments"
    );
}

/// Every symbol the fixed snippets name has to exist, with the shape the snippet
/// assumes. Pinned as `(what the doc shows, source file, declaration that must
/// be present)` — the EXPECTED-diff idiom applied to a document.
const DOC_CLAIMS: &[(&str, &str, &str)] = &[
    (
        "ctx.fetch(",
        "crates/core/src/app.rs",
        "pub async fn fetch(&self, mut req: FetchRequest) -> Result<FetchOutcome>",
    ),
    (
        "ctx.research(",
        "crates/core/src/app.rs",
        "pub async fn research(&self, mut req: ResearchRequest) -> Result<ResearchOutput>",
    ),
    (
        "ctx.require_str(",
        "crates/core/src/app.rs",
        "pub fn require_str(&self, key: &str) -> Result<&str>",
    ),
    (
        "ctx.save_artifact(",
        "crates/core/src/app.rs",
        "pub async fn save_artifact(&self, name: &str, bytes: &[u8]) -> Result<PathBuf>",
    ),
    (
        "AppManifest",
        "crates/core/src/app.rs",
        "pub struct AppManifest",
    ),
    (
        "Requirement",
        "crates/core/src/app.rs",
        "pub enum Requirement",
    ),
    (
        "UpsertSummary { new, changed, unchanged, removed }",
        "crates/core/src/datasets.rs",
        "pub struct UpsertSummary",
    ),
    (
        "ctx.upsert_many(",
        "crates/core/src/app.rs",
        "pub async fn upsert_many(",
    ),
    (
        "ctx.sync_many",
        "crates/core/src/app.rs",
        "pub async fn sync_many(",
    ),
    (
        "html_to_markdown(&html)",
        "crates/core/src/markdown.rs",
        "pub fn html_to_markdown(html: &str) -> String",
    ),
    (
        "extract_batch(&compiled,",
        "crates/core/src/extract.rs",
        "pub fn extract_batch(rules: &CompiledRuleSet, docs: &[String]) -> Vec<Value>",
    ),
    (
        "simhash(&text)",
        "crates/core/src/simhash.rs",
        "pub fn simhash(text: &str) -> u64",
    ),
    (
        "hamming(a, b)",
        "crates/core/src/simhash.rs",
        "pub fn hamming(a: u64, b: u64) -> u32",
    ),
    (
        "fetch_bytes(HttpRequest) -> Vec<u8>",
        "crates/core/src/engine.rs",
        "async fn fetch_bytes(&self, req: HttpRequest) -> Result<Vec<u8>>",
    ),
    (
        "transact(TransactRequest) -> TransactEvidence",
        "crates/core/src/engine.rs",
        "async fn transact(&self, req: TransactRequest) -> Result<TransactEvidence>",
    ),
];

/// Both directions at once: the doc still makes the claim, and the code still
/// backs it. A renamed method fails here instead of failing in the editor of
/// whoever copied the snippet.
#[test]
fn every_symbol_the_onboarding_doc_names_still_exists_with_that_shape() {
    let doc = onboarding();
    let mut broken = Vec::new();
    for (claim, source, decl) in DOC_CLAIMS {
        if !doc.contains(claim) {
            broken.push(format!(
                "ONBOARDING.md no longer shows {claim:?} — if the idiom moved, \
                 update this list in the same change"
            ));
            continue;
        }
        if !read(source).contains(decl) {
            broken.push(format!(
                "ONBOARDING.md shows {claim:?}, but {source} no longer declares \
                 {decl:?} — the doc is teaching something that will not compile"
            ));
        }
    }
    assert!(broken.is_empty(), "{}", broken.join("\n"));
}
