//! The **fetch** chokepoint: every fetch an app makes on behalf of a job must
//! go through [`AppContext::fetch`], which adds cost metering, the per-job
//! budget clamp (the soft Claude-tier downgrade), the learned tier router, and
//! the VCR cassette.
//!
//! This is the sibling of `llm_chokepoint.rs`, and it needs a different guard.
//! The model chokepoint has a *structural* layer — `EngineSet::claude` is
//! `pub(crate)`, so an app crate cannot even name the researcher. The fetch
//! seam has no such layer: `EngineSet::http`, `::browser` and `::fetch` are
//! public **on purpose**, because a dozen legitimate callers need a raw engine
//! (an HTTP-API app that wants conditional GET or a byte body; the crawler,
//! which owns its own frontier and meters itself; jobless server-side callers
//! that have no `AppContext` at all).
//!
//! So the whole guard is this inventory: every raw-engine call site in the
//! workspace is pinned with a count and a per-entry reason. Adding one fails
//! this test with a message naming the file — which forces the question "should
//! this have been `ctx.fetch`?" to be answered by a human, once, in this list,
//! instead of silently costing a job its metering and its determinism.
//!
//! Plus the behavioural tests: an app fan-out that reaches a raw fetcher spends
//! Claude money past an exhausted budget and hits the live network on replay.
//! Those are asserted through the real `extractor` and `plugin` runs in
//! `crates/server/src/e2e/app_fetch_chokepoint.rs` (the server crate is the one
//! that can depend on app crates).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Every raw-engine (`*.engines.fetch` / `.engines.http` / `.engines.browser`)
/// call site in the workspace, as `<repo-relative path>::<expr>` → occurrences.
///
/// **Every entry is a decision, not a formality.** A new one costs its job the
/// cost ledger, the budget clamp, the tier router and VCR record/replay. Before
/// adding: could this be `ctx.fetch(FetchRequest::new(url))`?
///
/// Counts are pinned so a *second* raw fetch inside an already-listed file is
/// caught too — file-level granularity would wave it through.
const EXPECTED_RAW_ENGINE_CALLS: &[(&str, usize)] = &[
    // ── The chokepoint itself ────────────────────────────────────────────────
    // `AppContext::fetch` IS the metered seam; it drives the tiered fetcher
    // after clamping the budget and consulting the tier router, and meters +
    // records the outcome afterwards.
    ("crates/core/src/app.rs::self.engines.fetch", 1),
    // ── Raw-HTTP apps: JSON/binary APIs, not pages ───────────────────────────
    // These call an API endpoint, not a rendered document. Tier escalation
    // (browser → Claude) is meaningless for a JSON payload, and each needs a
    // per-request knob the tiered `FetchRequest` deliberately does not carry:
    // ETag revalidation, `max_body_bytes`, a per-request timeout, a POST body,
    // or a bytes (non-UTF-8) response.
    // CKAN datastore API: POST with a JSON body.
    ("crates/apps/ca-grants/src/lib.rs::ctx.engines.http", 1),
    ("crates/apps/census-bfs/src/lib.rs::ctx.engines.http", 1),
    ("crates/apps/census-density/src/lib.rs::ctx.engines.http", 2),
    ("crates/apps/census-nesd/src/lib.rs::ctx.engines.http", 1),
    ("crates/apps/census-nonemp/src/lib.rs::ctx.engines.http", 1),
    // Release ZIP via `fetch_bytes` + `max_body_bytes` — binary, not text.
    (
        "crates/apps/cms-fee-schedule/src/lib.rs::ctx.engines.http",
        2,
    ),
    ("crates/apps/cordis/src/lib.rs::ctx.engines.http", 2),
    // SEDIA search API: POST with a JSON body.
    ("crates/apps/eu-sedia/src/lib.rs::ctx.engines.http", 1),
    // POST `search2` with a JSON body — the tiered fetcher only issues GETs.
    ("crates/apps/grants-gov/src/lib.rs::ctx.engines.http", 2),
    // The README walkthrough template. This one is a rendered PAGE, not an API
    // — it predates the chokepoint and arguably SHOULD be `ctx.fetch`; the
    // migration (with provenance stamping) is banked in the vault
    // (hackernews-teaches-current-idioms, r11). Reviewed, not endorsed.
    ("crates/apps/hackernews/src/lib.rs::ctx.engines.http", 1),
    ("crates/apps/mpsv-ispv/src/lib.rs::ctx.engines.http", 1),
    // ~188 MB bulk feed: `no_cache` + a per-request 300s timeout, plus an ARES
    // company lookup. Both are APIs.
    ("crates/apps/mpsv-vpm/src/lib.rs::ctx.engines.http", 2),
    // Dataset peering: conditional GET (`etag`) over another node's change
    // feed. The tiered `FetchRequest` carries no validator, so routing this
    // through `ctx.fetch` would silently drop the 304 path a mirror walks on.
    ("crates/apps/peer/src/lib.rs::ctx.engines.http", 1),
    (
        "crates/apps/smlouvy-dump-watch/src/lib.rs::ctx.engines.http",
        1,
    ),
    // ── Self-metered by design ───────────────────────────────────────────────
    // The crawler owns its own concurrency, robots and frontier control, so it
    // cannot route through `fetch`. It wraps the raw client in
    // `MeteringHttpClient` and flushes one cost event + one tier-router signal
    // per host after the crawl — i.e. it re-implements the chokepoint's
    // guarantees at crawl granularity rather than losing them.
    ("crates/apps/crawl/src/lib.rs::ctx.engines.http", 1),
    // Wayback historical backfill: constructs an `ArchiveEngine` over the raw
    // HTTP client (the engine is not a fetch tier here, it is the CDX index
    // client) and pulls each snapshot's raw body from web.archive.org. Not a
    // tiered fetch — escalating an archive snapshot to a live browser render
    // would defeat the point of reading history.
    ("crates/apps/extractor/src/lib.rs::ctx.engines.http", 2),
    // A transact flow is a browser *session* (form fill, evidence capture),
    // not a fetch; there is no `FetchOutcome` to meter or record.
    ("crates/apps/transact/src/lib.rs::ctx.engines.browser", 1),
    // ── Jobless server-side callers: no AppContext exists ────────────────────
    // Materialized-view refresher: a background server task, not a job run.
    ("crates/server/src/refresher.rs::state.engines.http", 1),
    // Provisioner proposal validation: a synchronous route-driven sample fetch
    // with no job attached — same class as the `/extract/preview` entry below.
    (
        "crates/server/src/routes/provisioner.rs::state.engines.fetch",
        1,
    ),
    // `/remote/*` proxy: forwards a caller's request to a peer node.
    ("crates/server/src/routes/remote.rs::state.engines.http", 1),
    // `GET /metrics`: reads the remote fabric's egress counters
    // (`Fetcher::egress_counters`) to emit `pumper_remote_egress_fetches`. Not a
    // fetch at all — no job, no budget, no cassette, no network — the scanner
    // matches it because it is a `state.engines.fetch` *field access*. Kept as a
    // reviewed row rather than an exemption, and deliberately NOT dodged by
    // rephrasing the expression: gaming a guard is worse than carrying a row.
    ("crates/server/src/routes/meta.rs::state.engines.fetch", 1),
    // `POST /extract/preview`: a synchronous, jobless rules try-out. There is
    // no job, so no budget, ledger, cassette or tier lineage to attach to.
    (
        "crates/server/src/routes/runtime.rs::state.engines.fetch",
        1,
    ),
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

/// The raw-engine field accesses on one source line, each as
/// `<receiver>.engines.<field>` — e.g. `ctx.engines.http`. Empty for a comment
/// or doc line: the chokepoint is *documented* in a dozen places and a prose
/// mention must never read as a call site.
fn raw_engine_exprs(line: &str) -> Vec<String> {
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") || trimmed.starts_with('*') {
        return Vec::new();
    }
    let mut found = Vec::new();
    for needle in [".engines.fetch", ".engines.http", ".engines.browser"] {
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
    }
    found
}

/// This file — the one source in the workspace whose *string literals* are full
/// of `.engines.*` expressions (the inventory above). Scanning it would make the
/// guard report itself, so it is excluded by path rather than by trying to teach
/// the line scanner about string literals.
const SELF_PATH: &str = "crates/core/tests/fetch_chokepoint.rs";

/// One source file as the scanner must see it: comment/doc lines dropped, the
/// rest joined with no separator. rustfmt wraps long method chains
/// (`ctx\n.engines\n.http\n.fetch(...)`), and a per-line scan reads the wrapped
/// form as no call site at all — nine real sites across six files were
/// invisible to this guard until the scan joined lines (found 2026-08-12).
/// Joining cannot invent a site: a statement can't end in an identifier while
/// the next begins with `.` unless they are one chain.
fn scannable_source(text: &str) -> String {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.starts_with("//") && !l.starts_with('*'))
        .collect()
}

/// Scans the workspace for raw-engine call sites, as
/// `<repo-relative path>::<expr>` → occurrences.
fn raw_engine_calls() -> BTreeMap<String, usize> {
    let root = workspace_root();
    let mut files = Vec::new();
    rust_sources(&root.join("crates"), &mut files);

    let mut found: BTreeMap<String, usize> = BTreeMap::new();
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
        for expr in raw_engine_exprs(&scannable_source(&text)) {
            *found.entry(format!("{rel}::{expr}")).or_default() += 1;
        }
    }
    found
}

#[test]
fn app_fetches_go_through_the_metered_chokepoint_not_around_it() {
    let found = raw_engine_calls();
    let expected: BTreeMap<String, usize> = EXPECTED_RAW_ENGINE_CALLS
        .iter()
        .map(|(k, n)| (k.to_string(), *n))
        .collect();

    let added: Vec<String> = found
        .iter()
        .filter(|(k, n)| expected.get(*k).is_none_or(|e| *n > e))
        .map(|(k, n)| format!("{k} x{n} (reviewed: {:?})", expected.get(k)))
        .collect();
    assert!(
        added.is_empty(),
        "NEW raw-engine call site(s) bypassing AppContext::fetch — they lose cost metering, the \
         per-job budget clamp (an `auto_with_research` fetch can then spend past the job's \
         ceiling), the learned tier router, and VCR record/replay (a recorded job silently hits \
         the live network). Use `ctx.fetch(FetchRequest::new(url))`. If the raw engine is \
         genuinely necessary, justify it and add it to EXPECTED_RAW_ENGINE_CALLS: {added:?}"
    );
    let gone: Vec<String> = expected
        .iter()
        .filter(|(k, n)| found.get(*k).is_none_or(|f| f < n))
        .map(|(k, n)| format!("{k} x{n} (actual: {:?})", found.get(k)))
        .collect();
    assert!(
        gone.is_empty(),
        "EXPECTED_RAW_ENGINE_CALLS over-counts call sites that no longer exist — a bypass was \
         removed (good) but the inventory still claims it: {gone:?}"
    );
}

/// The two URL fan-out apps are the ones this direction migrated, and the ones
/// most likely to regress: they take a caller-selectable `strategy` param that
/// includes `auto_with_research`, so a raw fetch there is a direct line from
/// job params to unbounded Claude spend. Named separately from the inventory so
/// the failure says *which* invariant broke.
#[test]
fn extractor_and_plugin_url_modes_hold_no_raw_fetcher() {
    let found = raw_engine_calls();
    for app in ["extractor", "plugin"] {
        let key = format!("crates/apps/{app}/src/lib.rs::ctx.engines.fetch");
        assert!(
            !found.contains_key(&key),
            "{app} reaches for the raw tiered fetcher again — its `strategy` param accepts \
             `auto_with_research`, so this is a job-params-to-Claude-spend path that skips the \
             budget clamp, and a recorded run of it would hit the live network on replay"
        );
    }
}

/// The **second half** of every row in the inventory above. A raw-engine call
/// site costs its app the cost ledger, the budget clamp and the tier router —
/// and the VCR cassette, in both directions: that traffic is never recorded,
/// and on replay nothing stops it running live.
///
/// What replay DOES about that is declared per app in `REPLAY_BYPASS_APPS`
/// (`crates/core/src/vcr.rs`), the one place the decision lives. It is matched
/// on the app's NAME at runtime, so an app that grows a raw-engine call and
/// never gets a row stays *assumed replayable*: a `replay_of` job runs it LIVE
/// and the worker stamps `vcr_replay_of` on the result — a provenance claim
/// that the output came from recorded bytes.
///
/// This is the direction the table's own guards cannot cover. `vcr.rs`'s
/// `every_replay_bypass_row_names_an_app_crate_that_exists` and the server's
/// `every_declared_replay_bypass_names_a_registered_app` both police rows that
/// EXIST (stale or misspelled); only the scanner knows about an app that
/// should have one and does not.
#[test]
fn every_raw_engine_app_declares_its_replay_fidelity() {
    let graded: BTreeSet<&str> = pumper_core::vcr::REPLAY_BYPASS_APPS
        .iter()
        .map(|(app, _, _)| *app)
        .collect();
    let found = raw_engine_calls();
    let ungraded: BTreeSet<&str> = found
        .keys()
        .filter_map(|site| site.strip_prefix("crates/apps/"))
        .filter_map(|rest| rest.split('/').next())
        .filter(|app| !graded.contains(app))
        .collect();
    assert!(
        ungraded.is_empty(),
        "app(s) driving an engine raw with no row in REPLAY_BYPASS_APPS \
         (crates/core/src/vcr.rs): {ungraded:?}. Without one the app is assumed replayable, so \
         a `replay_of` job runs it LIVE and its result is stamped `vcr_replay_of`. Grade it \
         `Partial` (some run modes go through `ctx.fetch`) or `Unreplayable` (none do)."
    );
}

/// The scanner must not be fooled by the prose that documents the chokepoint —
/// otherwise the inventory fills with doc lines and stops meaning anything.
#[test]
fn comment_mentions_are_not_call_sites() {
    assert!(raw_engine_exprs("// Apps call `ctx.engines.fetch.fetch(...)` and get").is_empty());
    assert!(raw_engine_exprs("//! not `engines.fetch` raw: this call").is_empty());
    assert!(raw_engine_exprs("/// app calls `ctx.engines.http` directly").is_empty());
    assert!(raw_engine_exprs("     * doc continuation ctx.engines.http").is_empty());
    assert_eq!(
        raw_engine_exprs("        let resp = ctx.engines.http.fetch(req).await?;"),
        vec!["ctx.engines.http".to_string()]
    );
    // Two sites on one line are two sites.
    assert_eq!(
        raw_engine_exprs("f(state.engines.http.clone(), state.engines.browser.clone())"),
        vec![
            "state.engines.http".to_string(),
            "state.engines.browser".to_string()
        ]
    );
}

/// The anti-pattern this scanner shipped with: rustfmt wraps a long chain onto
/// four lines and the per-line scan saw nothing. The joined view must see one
/// call site, and comment lines must still drop out before the join (a doc
/// line gluing onto code must not manufacture a receiver).
#[test]
fn rustfmt_wrapped_chains_are_still_call_sites() {
    let wrapped = "        let response = ctx\n            .engines\n            .http\n            .fetch(HttpRequest::get(url))\n            .await?;\n";
    assert_eq!(
        raw_engine_exprs(&scannable_source(wrapped)),
        vec!["ctx.engines.http".to_string()]
    );
    let commented = "// wrapped in prose: ctx\n// .engines.http is documented here\nlet x = 1;\n";
    assert!(raw_engine_exprs(&scannable_source(commented)).is_empty());
}
