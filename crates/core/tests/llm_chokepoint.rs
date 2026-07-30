//! The model chokepoint: **every** Claude call must go through
//! [`AppContext::research`], which adds the research cache, the per-job budget
//! governor and cost metering.
//!
//! Two layers guard it, because one is not enough:
//!
//! 1. **Structural** — `EngineSet::claude` is `pub(crate)`, so an app crate
//!    literally cannot name the researcher. That is what actually prevents a
//!    bypass; it is enforced by the compiler on every build.
//! 2. **Inventory** (this file) — the structural guard only covers crates
//!    *outside* `pumper-core`. Inside core, a new `researcher().research(...)`
//!    would still compile, so the direct call sites are pinned to a hardcoded
//!    EXPECTED set (the EXPECTED-diff idiom from
//!    `crates/server/src/routes/mod.rs`): adding one fails this test with a
//!    message naming the new site.
//!
//! Plus behavioural tests that the wrapper's three guarantees are real, so
//! "route it through the chokepoint" is worth something.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use pumper_core::testing::TestContext;
use pumper_core::testing::{engines_with, research_output, Dead, ScriptedResearcher, TempStore};
use pumper_core::ResearchRequest;

// ---------------------------------------------------------------------------
// Layer 2 — the inventory
// ---------------------------------------------------------------------------

/// Every place in the workspace that calls `Researcher::research` **without**
/// going through [`AppContext::research`], as `<path>::<receiver>`.
///
/// This list is the bypass surface of the whole repo, and it must stay inside
/// `pumper-core`:
///
/// - `crates/core/src/app.rs` — `self.engines.researcher()` **is** the
///   chokepoint. It is the one legitimate direct caller.
/// - `crates/core/src/fetcher.rs` — tier 3 of the tiered fetcher. It sits
///   *below* `AppContext`, and `AppContext::fetch` meters its outcome (including
///   the Claude tier's `cost_usd`) and clamps the request's budget beforehand,
///   so it inherits the governance by construction rather than by calling the
///   wrapper (which would be a cycle).
///
/// **Adding an entry here is a design decision, not a formality.** If a new call
/// site needs the model, it almost certainly wants `ctx.research(...)`.
const EXPECTED_DIRECT_RESEARCH_CALLS: &[&str] = &[
    "crates/core/src/app.rs::self.engines.researcher()",
    "crates/core/src/fetcher.rs::self.claude",
];

/// Workspace root — `crates/core/../..`.
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

/// The receiver expression immediately before a `.research(` call — e.g.
/// `ctx`, `self.claude`, `ctx.engines.claude`. Returns `None` when the line is a
/// comment or doc line (the chokepoint is *documented* in several places, and a
/// prose mention must not read as a call site).
fn research_receiver(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") || trimmed.starts_with("*") {
        return None;
    }
    let head = &line[..line.find(".research(")?];
    let recv: String = head
        .chars()
        .rev()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.' || *c == '(' || *c == ')')
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let recv = recv.trim_matches('.').to_string();
    (!recv.is_empty()).then_some(recv)
}

/// Scans the workspace for `.research(` calls and returns the ones that are NOT
/// `AppContext::research` itself (`ctx.research(...)` / `self.research(...)`),
/// as `<repo-relative path>::<receiver>`.
fn direct_research_calls() -> BTreeSet<String> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let mut found = BTreeSet::new();
    for file in files {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in text.lines() {
            let Some(recv) = research_receiver(line) else {
                continue;
            };
            // The chokepoint's own callers.
            if recv == "ctx" || recv == "self" || recv.ends_with("context") {
                continue;
            }
            let rel = file
                .strip_prefix(&root)
                .unwrap_or(&file)
                .to_string_lossy()
                .replace('\\', "/");
            found.insert(format!("{rel}::{recv}"));
        }
    }
    found
}

#[test]
fn model_calls_go_through_the_chokepoint_not_around_it() {
    let found = direct_research_calls();
    let expected: BTreeSet<String> = EXPECTED_DIRECT_RESEARCH_CALLS
        .iter()
        .map(|s| s.to_string())
        .collect();

    let new_bypasses: Vec<_> = found.difference(&expected).collect();
    assert!(
        new_bypasses.is_empty(),
        "NEW direct Researcher call site(s) bypassing AppContext::research — they lose the \
         research cache, the per-job budget governor and cost metering. Use `ctx.research(req)`. \
         If the bypass is genuinely necessary, justify it and add it to \
         EXPECTED_DIRECT_RESEARCH_CALLS: {new_bypasses:?}"
    );
    let gone: Vec<_> = expected.difference(&found).collect();
    assert!(
        gone.is_empty(),
        "EXPECTED_DIRECT_RESEARCH_CALLS lists call sites that no longer exist — remove them: \
         {gone:?}"
    );
}

#[test]
fn no_app_crate_can_name_the_researcher_not_even_by_field() {
    // The compiler already enforces this (`EngineSet::claude` is pub(crate)),
    // but the assertion names the failure in one line instead of as a privacy
    // error two crates away, and it also catches a would-be re-export.
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates").join("apps"), &mut files);
    let offenders: Vec<String> = files
        .iter()
        .filter(|f| {
            std::fs::read_to_string(f)
                .map(|t| {
                    t.lines().any(|l| {
                        let t = l.trim_start();
                        !t.starts_with("//") && l.contains("engines.claude")
                    })
                })
                .unwrap_or(false)
        })
        .map(|f| {
            f.strip_prefix(&root)
                .unwrap_or(f)
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "app crate(s) reaching for the raw researcher instead of ctx.research(): {offenders:?}"
    );
}

// ---------------------------------------------------------------------------
// The three guarantees the chokepoint exists for
// ---------------------------------------------------------------------------

#[tokio::test]
async fn identical_requests_hit_the_cache_not_the_engine_twice() {
    let store = TempStore::new("chokepoint-cache").await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("summary v1"));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine.clone()))
        .research_cache_ttl_secs(3600)
        .build();

    let first = ctx
        .research(ResearchRequest::new("extract the endpoints"))
        .await
        .unwrap();
    let second = ctx
        .research(ResearchRequest::new("extract the endpoints"))
        .await
        .unwrap();

    assert_eq!(first.text, "summary v1");
    assert_eq!(second.text, "summary v1");
    assert_eq!(
        engine.call_count(),
        1,
        "the second identical request must be served from the research cache"
    );
    assert_eq!(
        second.cost_usd,
        Some(0.0),
        "a cache hit costs nothing and must report so"
    );
}

#[tokio::test]
async fn exhausted_job_budget_refuses_the_call_not_spends_anyway() {
    let store = TempStore::new("chokepoint-budget").await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("expensive answer"));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine.clone()))
        .budget_usd(0.10)
        .build();

    // Burn the whole job budget through the ledger the chokepoint consults.
    ctx.meter("claude", None, 0.10, Some("prior call")).await;

    let err = ctx
        .research(ResearchRequest::new("one more expensive question"))
        .await
        .expect_err("a budget-exhausted job must not reach the engine");
    assert!(
        err.to_string().contains("budget"),
        "unhelpful budget error: {err}"
    );
    assert_eq!(
        engine.call_count(),
        0,
        "the engine was invoked despite an exhausted budget"
    );
}

#[tokio::test]
async fn per_call_ceiling_is_clamped_to_remaining_headroom_not_left_wide_open() {
    let store = TempStore::new("chokepoint-clamp").await;
    let engine = Arc::new(ScriptedResearcher::new().always_text("answer"));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine.clone()))
        .budget_usd(1.00)
        .build();
    ctx.meter("claude", None, 0.80, Some("prior call")).await;

    let mut req = ResearchRequest::new("a question with a generous ceiling");
    req.max_budget_usd = Some(5.00); // caller asked for more than the job has left
    ctx.research(req).await.unwrap();

    let sent = engine.calls();
    assert_eq!(sent.len(), 1);
    let ceiling = sent[0].max_budget_usd.expect("a ceiling must be set");
    assert!(
        (ceiling - 0.20).abs() < 1e-9,
        "per-call ceiling {ceiling} was not clamped to the $0.20 of remaining job headroom"
    );
}

#[tokio::test]
async fn every_model_call_is_metered_not_invisible_to_the_ledger() {
    let store = TempStore::new("chokepoint-meter").await;
    let mut out = research_output("answer");
    out.cost_usd = Some(0.37);
    let engine = Arc::new(ScriptedResearcher::new().on("", out));
    let ctx = TestContext::new(&store.storage, "chokepoint")
        .engines(engines_with(Arc::new(Dead), Arc::new(Dead), engine.clone()))
        .budget_usd(10.0)
        .build();

    ctx.research(ResearchRequest::new("what changed?"))
        .await
        .unwrap();

    let events = ctx.costs.job_events(ctx.job_id).await.unwrap();
    let total = ctx.costs.job_total(ctx.job_id).await.unwrap();
    assert_eq!(events.len(), 1, "expected exactly one metered claude event");
    assert_eq!(events[0].engine, "claude");
    assert!(
        (total - 0.37).abs() < 1e-9,
        "model spend {total} never reached the cost ledger"
    );
}
