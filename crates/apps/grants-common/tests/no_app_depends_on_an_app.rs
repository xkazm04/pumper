//! **Inventory guard for the layering rule** (the EXPECTED-diff idiom — a
//! convention is enforced with a test, never with a sentence in a doc).
//!
//! The repo's dependency rule, stated three times over (README.md
//! §Architecture, `.claude/CLAUDE.md`, ONBOARDING.md §7): *apps depend only on
//! `core` plus parsing libs; engines depend only on `core`; the server wires
//! everything together.* An app that reaches sideways into a sibling app — or
//! down into an engine — makes both crates un-extractable and turns "add a
//! scraping use case" from a four-step contract into an archaeology exercise.
//!
//! It lives in `grants-common` rather than in a single app because the thing it
//! guards is the SHAPE OF THE WHOLE DIRECTORY, not any one crate: it reads every
//! `crates/apps/*/Cargo.toml` off disk. `grants-common` is the one crate here
//! that exists to be depended upon by apps and depends on no app itself, so it
//! is the natural host.
//!
//! **Why an allow-list and not a flat ban.** Four app→app edges and one
//! app→engine edge exist today and predate the rule being enforced. Banning
//! them outright would mean a permanently red test, which is a test nobody
//! runs. Pinning the exact set means a new edge fails immediately and an edge
//! that gets closed fails too — until someone shrinks EXPECTED, which is the
//! paperwork that records the win.
//!
//! **The trap this test was written against.** The first cut of this inventory
//! (in the card that commissioned it) matched `^app-.*\.workspace` and would
//! have shipped an EXPECTED set of three — silently blind to
//! `crates/apps/extractor/Cargo.toml`'s `app-crawl = { path = "../crawl" }`,
//! because a `path` dependency carries no `.workspace` marker. A gate whose
//! EXPECTED omits a live violation is a rubber stamp on day one. Both forms are
//! parsed here, and [`parses_both_dependency_forms_and_ignores_comments`] proves
//! it against synthetic manifests rather than against the tree it guards.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// `crates/apps/` — the directory whose shape this test pins.
fn apps_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/apps/grants-common has a parent")
        .to_path_buf()
}

/// Every crate directory under `crates/apps/` that has a `Cargo.toml`, by its
/// `[package] name`.
fn app_crates() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for entry in fs::read_dir(apps_dir()).expect("crates/apps is readable") {
        let path = entry.expect("dir entry").path().join("Cargo.toml");
        if !path.is_file() {
            continue;
        }
        let src = fs::read_to_string(&path).expect("manifest is readable");
        let name = package_name(&src)
            .unwrap_or_else(|| panic!("{} has no [package] name", path.display()));
        out.push((name, src));
    }
    out.sort();
    out
}

/// The `name = "…"` of the manifest's `[package]` table.
fn package_name(src: &str) -> Option<String> {
    let mut in_package = false;
    for line in strip_comments(src).lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
            continue;
        }
        if in_package {
            if let Some(rest) = line.strip_prefix("name") {
                let rest = rest.trim_start().strip_prefix('=')?.trim();
                return Some(rest.trim_matches('"').to_string());
            }
        }
    }
    None
}

/// Drops `#` comments, preserving line structure.
///
/// Load-bearing: a manifest that *explains* the rule in a comment above the
/// line that breaks it (cordis's did, verbatim) must not launder the violation,
/// and a commented-OUT dependency must not be reported as a live edge. A
/// `#` inside a quoted string is left alone.
fn strip_comments(src: &str) -> String {
    src.lines()
        .map(|line| {
            let mut in_quotes = false;
            for (i, ch) in line.char_indices() {
                match ch {
                    '"' => in_quotes = !in_quotes,
                    '#' if !in_quotes => return &line[..i],
                    _ => {}
                }
            }
            line
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every dependency name declared in any `*dependencies*` table of `src`.
///
/// Recognises BOTH spellings this workspace uses — `name.workspace = true` and
/// `name = { path = "…" }` / `name = { workspace = true, features = […] }` — and
/// also the `name.path = "…"` dotted form, because the next person to add an
/// edge will not consult this parser first.
fn dependency_names(src: &str) -> BTreeSet<String> {
    let mut deps = BTreeSet::new();
    let mut in_deps = false;
    for line in strip_comments(src).lines() {
        let line = line.trim();
        if line.starts_with('[') {
            // `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`
            // and any `[target.'…'.dependencies]` all count: an edge is an edge.
            in_deps = line.trim_matches(['[', ']']).ends_with("dependencies");
            continue;
        }
        if !in_deps || line.is_empty() {
            continue;
        }
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        // `serde.workspace` and `serde` both name the crate `serde`.
        let name = key.trim().split('.').next().unwrap_or_default().trim();
        if !name.is_empty() {
            deps.insert(name.to_string());
        }
    }
    deps
}

/// Whether `name` is a shared app-layer helper rather than a scraping app.
///
/// CLAUDE.md §Architecture: `crates/apps/` holds "one crate per scraping use
/// case, plus `*-common` shared helpers". Depending on a helper is the
/// SANCTIONED way to share code between apps — it is the escape hatch that
/// makes this rule affordable. Depending on an app is not.
fn is_shared_helper(name: &str) -> bool {
    name.ends_with("-common")
}

/// `crate -> dependency` for every edge from an app crate into a sibling app
/// crate (helpers excluded).
fn app_to_app_edges() -> BTreeSet<String> {
    let crates = app_crates();
    let apps: BTreeSet<&str> = crates
        .iter()
        .map(|(name, _)| name.as_str())
        .filter(|name| !is_shared_helper(name))
        .collect();
    let mut edges = BTreeSet::new();
    for (name, src) in &crates {
        for dep in dependency_names(src) {
            if dep != *name && apps.contains(dep.as_str()) {
                edges.insert(format!("{name} -> {dep}"));
            }
        }
    }
    edges
}

/// `crate -> dependency` for every edge from an app crate into an engine crate.
fn app_to_engine_edges() -> BTreeSet<String> {
    let mut edges = BTreeSet::new();
    for (name, src) in &app_crates() {
        for dep in dependency_names(src) {
            if dep.starts_with("pumper-engine-") {
                edges.insert(format!("{name} -> {dep}"));
            }
        }
    }
    edges
}

/// The app→app edges that exist and are not yet closed.
///
/// Each line is a debt, not a licence. Adding one means arguing in review why
/// the shared code does not belong in a `*-common` crate; the four here have
/// never had that argument, they are just older than the test.
///
/// Shrunk 2026-10-05: `app-cordis -> app-eu-sedia` closed. cordis bought the
/// whole `app-eu-sedia` crate for one 23-line pure function (`topic_lineage`),
/// which now lives in `grants-common` where both sides of the join can reach
/// it. **This list may only ever shrink.**
const EXPECTED_APP_TO_APP: &[&str] = &[
    "app-census-bfs -> app-census-density",
    "app-census-nesd -> app-census-density",
    "app-census-nonemp -> app-census-density",
    "app-extractor -> app-crawl",
];

/// The app→engine edges that exist and are not yet closed.
///
/// Same rule, other half: an app that names an engine crate has picked its
/// transport instead of declaring it, so the server can no longer swap the
/// tier. One edge, pinned so it cannot become two.
const EXPECTED_APP_TO_ENGINE: &[&str] = &["app-extractor -> pumper-engine-archive"];

#[test]
fn no_app_depends_on_an_app() {
    let found = app_to_app_edges();
    let expected: BTreeSet<String> = EXPECTED_APP_TO_APP.iter().map(|s| s.to_string()).collect();
    let added: Vec<&String> = found.difference(&expected).collect();
    let closed: Vec<&String> = expected.difference(&found).collect();
    assert!(
        added.is_empty(),
        "a NEW app -> app edge was added. Apps depend only on `core` plus parsing \
         libs; shared code between apps belongs in a `*-common` crate under \
         crates/apps/. New edges: {added:#?}"
    );
    assert!(
        closed.is_empty(),
        "an app -> app edge was CLOSED without shrinking EXPECTED_APP_TO_APP — \
         record the win so the list cannot silently re-grow: {closed:#?}"
    );
}

#[test]
fn no_app_depends_on_an_engine() {
    let found = app_to_engine_edges();
    let expected: BTreeSet<String> = EXPECTED_APP_TO_ENGINE
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        found, expected,
        "the app -> engine edge set changed. Engines are wired by the server, not \
         chosen by an app; if one was closed, shrink EXPECTED_APP_TO_ENGINE in the \
         same commit"
    );
}

/// The matcher, proved against synthetic manifests rather than against the tree
/// it guards — because a scanner that silently sees nothing and a clean tree are
/// indistinguishable from the outside.
#[test]
fn parses_both_dependency_forms_and_ignores_comments() {
    let manifest = r#"
[package]
name = "app-seeded"

[dependencies]
pumper-core.workspace = true
# app-commented-out.workspace = true
app-dotted.workspace = true
app-inline = { path = "../inline" }
app-dotted-path.path = "../dotted"
serde = { workspace = true, features = ["derive"] }

[dev-dependencies]
app-devonly.workspace = true
"#;
    assert_eq!(package_name(manifest).as_deref(), Some("app-seeded"));
    let deps = dependency_names(manifest);
    for live in [
        "pumper-core",
        "app-dotted",
        "app-inline",
        "app-dotted-path",
        "serde",
        "app-devonly",
    ] {
        assert!(deps.contains(live), "the parser missed `{live}`: {deps:#?}");
    }
    assert!(
        !deps.contains("app-commented-out"),
        "a commented-out dependency was read as a live edge: {deps:#?}"
    );
}

/// The real tree, read once: the guard must actually be LOOKING at something.
/// A parser bug that returns an empty set makes both inventories pass vacuously.
#[test]
fn the_inventory_actually_reads_the_apps_directory() {
    let crates = app_crates();
    assert!(
        crates.len() > 10,
        "crates/apps/ should hold every scraping app; found {}",
        crates.len()
    );
    assert!(
        crates.iter().any(|(n, _)| n == "app-cordis"),
        "app-cordis is missing from the scan"
    );
    let (_, cordis) = crates
        .iter()
        .find(|(n, _)| n == "app-cordis")
        .expect("app-cordis");
    assert!(
        dependency_names(cordis).contains("grants-common"),
        "cordis must reach shared code through the helper crate, not sideways"
    );
}
