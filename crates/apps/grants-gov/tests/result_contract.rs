//! `AppManifest::output_shape` is the contract a consumer codes against: it is
//! published verbatim by `GET /apps` and by the MCP tool manifest, and an agent
//! that has never read this crate's source has nothing else to key on.
//!
//! grants-gov's declaration had drifted in BOTH directions at once. It declared
//! `hit_count` while the run emits `hitCount` (a consumer keying on the
//! declaration read `undefined`), it declared `removed?` — which is not merely
//! unemitted but structurally *unemittable*, since the listing is written with
//! `upsert_many_with_provenance` and only `sync_many` ever produces removals —
//! and it omitted twelve keys the run does emit, including `warnings[]`,
//! `truncated`'s companion `sweep`, the whole `unified` block and
//! `index_datasets[]`.
//!
//! Prose cannot hold that line: the declaration and the `json!` block live 350
//! lines apart and every new key silently widens the gap. So this file derives
//! the emitted shape from a REAL run and the declared shape from the published
//! string, and diffs them — the inventory / EXPECTED-diff idiom the repo uses
//! for conventions (`crates/server/src/routes/mod.rs`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use app_grants_gov::GrantsGov;
use async_trait::async_trait;
use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
use pumper_core::{HttpRequest, HttpResponse, Result, ScrapeApp};
use serde_json::{json, Value};

/// Both grants.gov endpoints, answering healthily — the widest result shape the
/// app can produce (details harvested, digest non-empty, corpus pass owned).
struct Healthy;

#[async_trait]
impl pumper_core::HttpClient for Healthy {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let body = if req.url.contains("search2") {
            // Far-future close dates keep the digest membership independent of
            // the wall clock; one near date exercises a populated `closingSoon`.
            let soon = (chrono::Utc::now().date_naive() + chrono::Duration::days(3))
                .format("%m/%d/%Y")
                .to_string();
            json!({
                "errorcode": 0,
                "data": {
                    "hitCount": 2,
                    "oppHits": [
                        { "id": "1", "number": "TEST-1", "title": "Rural Health",
                          "agency": "HHS", "oppStatus": "posted", "closeDate": soon },
                        { "id": "2", "number": "TEST-2", "title": "Vegetation",
                          "agency": "DOI", "oppStatus": "posted", "closeDate": "09/30/2099" }
                    ]
                }
            })
        } else {
            json!({
                "errorcode": 0,
                "data": {
                    "id": 1,
                    "opportunityNumber": "TEST-1",
                    "opportunityTitle": "Rural Health",
                    "agencyName": "Health and Human Services",
                    "synopsis": { "awardCeiling": "750000", "responseDate": "09/30/2099" }
                }
            })
        };
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: body.to_string(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

async fn run(params: Value) -> Value {
    let store = TempStore::new("grants-gov-result-contract").await;
    let engines = engines_with(Arc::new(Healthy), Arc::new(Dead), Arc::new(Dead));
    let ctx = TestContext::new(&store.storage, "grants-gov")
        .params(params)
        .engines(engines)
        .build();
    GrantsGov.run(ctx).await.expect("a healthy run succeeds")
}

// ---- the declared shape, parsed out of the published string ----------------

/// The inside of the leading `{…}` key list, brace-matched (so nested blocks
/// like `unified: {…}` do not terminate it early) and stopping before the prose.
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

/// `closingSoon[]` → `closingSoon`; `unified: {…}` → `unified`.
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

/// Declared top-level key → its declared nested keys (empty for scalars/arrays).
fn declared_shape() -> BTreeMap<String, BTreeSet<String>> {
    let decl = GrantsGov
        .manifest()
        .output_shape
        .expect("grants-gov declares an output_shape");
    split_top(brace_body(decl))
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

/// Emitted top-level key → its emitted nested keys (empty for scalars/arrays).
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

fn diff(
    a: &BTreeMap<String, BTreeSet<String>>,
    b: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<String> {
    a.keys().filter(|k| !b.contains_key(*k)).cloned().collect()
}

#[tokio::test]
async fn the_published_output_shape_is_what_the_run_emits() {
    // Scheduled defaults: the widest shape (details on, digest populated, this
    // run owns the corpus pass, the source is healthy so `index_datasets` is
    // published). Every conditional key is therefore present exactly once.
    let out = run(GrantsGov.default_params()).await;
    let declared = declared_shape();
    let emitted = emitted_shape(&out);

    let declared_but_never_emitted = diff(&declared, &emitted);
    let emitted_but_undeclared = diff(&emitted, &declared);
    assert!(
        declared_but_never_emitted.is_empty(),
        "output_shape promises keys the run never emits — a consumer coding \
         against the manifest reads undefined: {declared_but_never_emitted:?}\nresult was {out:#}"
    );
    assert!(
        emitted_but_undeclared.is_empty(),
        "the run emits keys output_shape never declares, so no consumer can \
         discover them: {emitted_but_undeclared:?}"
    );

    // …and one level down, where the drift is quietest: `unified` used to be
    // declared as three of the six keys `UnifiedOutcome::merge_into` writes.
    for (key, declared_nested) in &declared {
        assert_eq!(
            declared_nested, &emitted[key],
            "nested keys of `{key}` disagree between the declaration and the run"
        );
    }
}

#[tokio::test]
async fn removed_is_not_declared_because_this_app_cannot_emit_it() {
    // `removed?` was declared for the whole life of this app while being
    // structurally unemittable: the listing goes through
    // `upsert_many_with_provenance`, and only `sync_many` populates
    // `UpsertSummary.removed`. Declaring a key no code path can produce is worse
    // than omitting it — a consumer branches on a field that will never arrive.
    let decl = GrantsGov.manifest().output_shape.unwrap();
    assert!(
        !declared_shape().contains_key("removed"),
        "`removed` is back in the declaration: {decl}"
    );
    let out = run(GrantsGov.default_params()).await;
    assert!(out.get("removed").is_none(), "{out:#}");
    // The declaration says WHY, so the next reader does not re-add it.
    assert!(
        decl.contains("no `removed` key"),
        "the declaration must record why removals are impossible here: {decl}"
    );
}

#[tokio::test]
async fn turning_the_harvest_off_drops_only_the_key_declared_as_conditional() {
    // The one conditional key. If some other key ever became conditional the
    // declaration would have to say so too — this is what makes the strict
    // equality above safe to rely on.
    let out = run(json!({ "rows": 10, "maxPages": 5, "harvestDetails": false })).await;
    let emitted = emitted_shape(&out);
    let declared = declared_shape();
    assert!(!emitted.contains_key("details"), "{out:#}");
    let missing: Vec<String> = declared
        .keys()
        .filter(|k| k.as_str() != "details" && !emitted.contains_key(*k))
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "a run without the detail harvest dropped keys that are not declared \
         conditional: {missing:?}"
    );
    assert!(
        GrantsGov
            .manifest()
            .output_shape
            .unwrap()
            .contains("absent when `harvestDetails` is false"),
        "the declaration must name `details` as the conditional key"
    );
}
