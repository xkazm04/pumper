//! The api-recipe **lifecycle**: candidate → validated → candidate → retired →
//! (revive) candidate, against a real SQLite store.
//!
//! The defect this file pins: `record_failure` incremented
//! `consecutive_failures` and the counter's only consumer was guarded on
//! `validated != 0`. Once a recipe was unvalidated that branch was unreachable
//! forever, so the counter was write-only and `best_for_host` — which never
//! read it — kept returning the same dead candidate first for its host, firing
//! a live outbound replay ahead of the archive tier on *every* fetch.
//!
//! Two rungs now exist, not one: `max_failures` demotes a **validated** recipe
//! to candidate (unchanged), and `retire_after` retires a **candidate**. A
//! retired recipe is invisible to selection, and the only way back is an
//! operator revive — which returns it to *probation*, never straight to trust.
//!
//! There is deliberately **no destructive delete** here: retirement is the
//! terminal state and it is reversible. See `docs/features/fetching.md`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::{FetcherConfig, GovernorConfig, RecipesConfig};
use pumper_core::engine::{
    Browser, HttpClient, HttpRequest, HttpResponse, RenderRequest, RenderedPage, ResearchOutput,
    ResearchRequest, Researcher,
};
use pumper_core::error::{Error, Result};
use pumper_core::fetcher::{FetchRequest, Fetcher};
use pumper_core::recipes::{ApiRecipe, RecipeState, RecipeStore};
use pumper_core::testing::TempStore;
use pumper_core::{Governor, RecipeSource};
use serde_json::json;
use sqlx::SqlitePool;

const HOST: &str = "example.test";
const PAGE: &str = "https://example.test/catalog";

fn fixture() -> ApiRecipe {
    ApiRecipe {
        id: String::new(),
        host: HOST.into(),
        url_template: "https://example.test/api/items?page={page}".into(),
        params: json!({"page": "1"}),
        json_paths: vec!["$.results[*].title".into()],
        score: 0.9,
        validated: false,
    }
}

/// Reads the two lifecycle columns straight out of the table, so a test asserts
/// on stored state rather than on what an accessor chooses to surface.
async fn row_state(pool: &SqlitePool, id: &str) -> (i64, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT consecutive_failures, retired_at, last_failure_at FROM api_recipes WHERE id = ?1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("recipe row")
}

// ── case 1: the missing rung ─────────────────────────────────────────────────

/// THE defect. A candidate that keeps failing must stop being picked.
#[tokio::test]
async fn a_failing_candidate_retires_instead_of_being_picked_forever() {
    let store = TempStore::new("recipe-retire").await;
    let recipes = store.storage.recipes().with_retire_after(10);
    let id = recipes.upsert(&fixture()).await.unwrap();

    // Nine strikes: still a candidate, still selectable opportunistically.
    for _ in 0..9 {
        assert!(
            !recipes
                .record_failure(&id, 3, "replay failed")
                .await
                .unwrap(),
            "retirement is not a demotion signal"
        );
    }
    let (failures, retired_at, last_failure_at) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(failures, 9);
    assert!(retired_at.is_none(), "9 < retire_after must not retire");
    assert!(
        last_failure_at.is_some(),
        "every strike stamps last_failure_at"
    );
    assert!(
        recipes.best_for_host(HOST, true).await.unwrap().is_some(),
        "a candidate under the threshold is still a candidate"
    );

    // The tenth strike retires it.
    recipes
        .record_failure(&id, 3, "replay failed")
        .await
        .unwrap();
    let (failures, retired_at, _) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(
        failures, 10,
        "retirement keeps the evidence, it does not reset"
    );
    assert!(
        retired_at.is_some(),
        "the 10th strike must stamp retired_at"
    );

    // And the whole point: it is gone from selection, both ways.
    assert!(
        recipes.best_for_host(HOST, true).await.unwrap().is_none(),
        "a retired recipe must never be picked again — not even opportunistically"
    );
    assert!(recipes.best_for_host(HOST, false).await.unwrap().is_none());

    // Further strikes do not re-stamp or crash.
    recipes
        .record_failure(&id, 3, "replay failed")
        .await
        .unwrap();
    let (_, again, _) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(
        again, retired_at,
        "retired_at is stamped once, not rewritten"
    );
}

/// `retire_after = 0` disables the new rung entirely: today's behaviour, byte
/// for byte. This is the escape hatch the config key promises.
#[tokio::test]
async fn retire_after_zero_restores_the_immortal_candidate() {
    let store = TempStore::new("recipe-retire-off").await;
    let recipes = store.storage.recipes().with_retire_after(0);
    let id = recipes.upsert(&fixture()).await.unwrap();
    for _ in 0..50 {
        recipes
            .record_failure(&id, 3, "replay failed")
            .await
            .unwrap();
    }
    let (failures, retired_at, _) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(failures, 50);
    assert!(retired_at.is_none(), "0 disables the rung");
    assert!(recipes.best_for_host(HOST, true).await.unwrap().is_some());
}

// ── case 2: the GUARD on the rung that already existed ───────────────────────

/// Regression guard, declared as a guard before the change: the *validated*
/// rung demotes to candidate with the counter reset and `retired_at` still
/// NULL. The new rung applies only below validation.
#[tokio::test]
async fn a_validated_recipe_demotes_to_candidate_and_is_not_retired() {
    let store = TempStore::new("recipe-demote").await;
    let recipes = store.storage.recipes().with_retire_after(10);
    let id = recipes.upsert(&fixture()).await.unwrap();
    recipes.set_validated(&id, true).await.unwrap();

    assert!(!recipes
        .record_failure(&id, 3, "replay failed")
        .await
        .unwrap());
    assert!(!recipes
        .record_failure(&id, 3, "replay failed")
        .await
        .unwrap());
    assert!(
        recipes
            .record_failure(&id, 3, "replay failed")
            .await
            .unwrap(),
        "the third consecutive strike un-validates"
    );

    let (failures, retired_at, _) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(
        failures, 0,
        "demotion resets the counter, exactly as before"
    );
    assert!(
        retired_at.is_none(),
        "demotion is not retirement — the row stays a candidate"
    );
    let row = recipes.get(&id).await.unwrap().expect("row");
    assert_eq!(row["state"], json!("candidate"));
}

// ── case 3: one good replay clears the whole slate ───────────────────────────

#[tokio::test]
async fn a_success_clears_the_strikes_and_the_failure_stamp() {
    let store = TempStore::new("recipe-success").await;
    let recipes = store.storage.recipes().with_retire_after(10);
    let id = recipes.upsert(&fixture()).await.unwrap();
    for _ in 0..7 {
        recipes
            .record_failure(&id, 3, "replay failed")
            .await
            .unwrap();
    }
    // auto_validate's success path: resets AND promotes.
    recipes.record_success(&id, true).await.unwrap();

    let (failures, retired_at, last_failure_at) = row_state(&store.storage.pool(), &id).await;
    assert_eq!(failures, 0, "one good replay clears the whole slate");
    assert!(retired_at.is_none());
    assert!(
        last_failure_at.is_none(),
        "a clean recipe must not still advertise a last failure"
    );
    let row = recipes.get(&id).await.unwrap().expect("row");
    assert_eq!(row["state"], json!("validated"));
}

// ── case 4: the operator can SEE the strikes, and filter by state ────────────

#[tokio::test]
async fn list_carries_the_state_the_strikes_and_the_failure_stamp() {
    let store = TempStore::new("recipe-list").await;
    let recipes = store.storage.recipes().with_retire_after(2);

    let validated_id = recipes.upsert(&fixture()).await.unwrap();
    recipes.set_validated(&validated_id, true).await.unwrap();

    let mut candidate = fixture();
    candidate.url_template = "https://example.test/api/other?page={page}".into();
    candidate.score = 0.5;
    let candidate_id = recipes.upsert(&candidate).await.unwrap();

    let mut doomed = fixture();
    doomed.url_template = "https://example.test/api/dead?page={page}".into();
    doomed.score = 0.1;
    let doomed_id = recipes.upsert(&doomed).await.unwrap();
    recipes
        .record_failure(&doomed_id, 3, "replay failed")
        .await
        .unwrap();
    recipes
        .record_failure(&doomed_id, 3, "replay failed")
        .await
        .unwrap();

    let all = recipes.list(None, None, 100).await.unwrap();
    assert_eq!(all.len(), 3);
    for row in &all {
        assert!(row.get("state").is_some(), "every row names its state");
        assert!(row.get("consecutive_failures").is_some());
        assert!(
            row.as_object().unwrap().contains_key("last_failure_at"),
            "the key is always present, null when there is no failure"
        );
    }

    let states: HashMap<&str, &str> = all
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap(),
                r["state"].as_str().expect("state is a string"),
            )
        })
        .collect();
    assert_eq!(states[validated_id.as_str()], "validated");
    assert_eq!(states[candidate_id.as_str()], "candidate");
    assert_eq!(states[doomed_id.as_str()], "retired");

    // The filter.
    let retired = recipes
        .list(None, Some(RecipeState::Retired), 100)
        .await
        .unwrap();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0]["id"], json!(doomed_id));
    assert_eq!(retired[0]["consecutive_failures"], json!(2));
    assert!(retired[0]["last_failure_at"].is_string());

    assert_eq!(
        recipes
            .list(None, Some(RecipeState::Validated), 100)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        recipes
            .list(None, Some(RecipeState::Candidate), 100)
            .await
            .unwrap()
            .len(),
        1
    );

    // A bogus state is not an empty page — it never reaches the store.
    assert!(RecipeState::parse("bogus").is_none());
    assert_eq!(RecipeState::parse("retired"), Some(RecipeState::Retired));
}

// ── case 5: revive is probation, not trust ───────────────────────────────────

#[tokio::test]
async fn revive_returns_a_retired_recipe_to_probation_never_to_trust() {
    let store = TempStore::new("recipe-revive").await;
    let recipes = store.storage.recipes().with_retire_after(1);
    let id = recipes.upsert(&fixture()).await.unwrap();
    // A recipe that WAS trusted, then fell all the way to retired.
    recipes.set_validated(&id, true).await.unwrap();
    recipes.retire(&id).await.unwrap();
    assert_eq!(
        recipes.get(&id).await.unwrap().unwrap()["state"],
        json!("retired"),
        "retired outranks validated — it is the terminal state"
    );

    let row = recipes.revive(&id).await.unwrap().expect("revived row");
    assert_eq!(
        row["state"],
        json!("candidate"),
        "revival is probation: never straight back to validated"
    );
    assert_eq!(row["validated"], json!(false));
    assert_eq!(row["consecutive_failures"], json!(0));
    assert!(row["last_failure_at"].is_null());

    // Selectable again, opportunistically only.
    assert!(recipes.best_for_host(HOST, true).await.unwrap().is_some());
    assert!(recipes.best_for_host(HOST, false).await.unwrap().is_none());

    // Unknown id is a miss on both doors, not a silent success.
    assert!(recipes.revive("nope").await.unwrap().is_none());
    assert!(!recipes.retire("nope").await.unwrap());
}

/// Retirement is the terminal state **and it is reversible**. Nothing in this
/// crate destroys a recipe row: the irreversible door was deliberately not
/// built (see the module doc), so this test pins that the round trip is lossless
/// for everything except the strike evidence the revive explicitly clears.
#[tokio::test]
async fn retire_then_revive_is_a_reversible_round_trip() {
    let store = TempStore::new("recipe-roundtrip").await;
    let recipes = store.storage.recipes().with_retire_after(10);
    let id = recipes.upsert(&fixture()).await.unwrap();
    let before = recipes.get(&id).await.unwrap().unwrap();

    assert!(recipes.retire(&id).await.unwrap());
    assert!(recipes.best_for_host(HOST, true).await.unwrap().is_none());
    assert!(recipes.revive(&id).await.unwrap().is_some());
    let after = recipes.get(&id).await.unwrap().unwrap();

    for key in [
        "id",
        "host",
        "url_template",
        "params",
        "json_paths",
        "score",
        "discovered_at",
        "state",
    ] {
        assert_eq!(before[key], after[key], "retire/revive changed `{key}`");
    }
}

// ── case 7: the 0046 back-fill ───────────────────────────────────────────────

/// The shipped back-fill SQL, read out of the migration itself so the test
/// exercises what actually runs rather than a paraphrase of it.
fn backfill_sql() -> &'static str {
    const SRC: &str = include_str!("../migrations/0046_recipe_lifecycle.sql");
    let at = SRC
        .find("UPDATE api_recipes")
        .expect("0046 carries a back-fill UPDATE");
    SRC[at..].trim()
}

#[tokio::test]
async fn the_0046_backfill_retires_over_threshold_candidates_exactly_once() {
    let store = TempStore::new("recipe-backfill").await;
    let pool = store.storage.pool();
    let recipes = store.storage.recipes().with_retire_after(10);

    // A pre-0046 store: a candidate deep past the threshold, and a validated
    // recipe with a single strike that must be left completely alone.
    let mut old_candidate = fixture();
    old_candidate.url_template = "https://example.test/api/zombie?page={page}".into();
    let zombie = recipes.upsert(&old_candidate).await.unwrap();
    sqlx::query(
        "UPDATE api_recipes SET consecutive_failures = 50, retired_at = NULL WHERE id = ?1",
    )
    .bind(&zombie)
    .execute(&pool)
    .await
    .unwrap();

    let healthy = recipes.upsert(&fixture()).await.unwrap();
    recipes.set_validated(&healthy, true).await.unwrap();
    sqlx::query("UPDATE api_recipes SET consecutive_failures = 1, retired_at = NULL WHERE id = ?1")
        .bind(&healthy)
        .execute(&pool)
        .await
        .unwrap();

    // First pass.
    sqlx::query(backfill_sql()).execute(&pool).await.unwrap();
    let (zf, z1, _) = row_state(&pool, &zombie).await;
    let (hf, h1, _) = row_state(&pool, &healthy).await;
    assert_eq!(zf, 50, "the back-fill must not touch consecutive_failures");
    assert!(z1.is_some(), "an over-threshold candidate is retired");
    assert_eq!(hf, 1, "the validated row's counter is untouched");
    assert!(
        h1.is_none(),
        "a validated row is never retired by the back-fill"
    );

    // Second pass: idempotent, and it does not re-stamp.
    sqlx::query(backfill_sql()).execute(&pool).await.unwrap();
    let (zf2, z2, _) = row_state(&pool, &zombie).await;
    let (hf2, h2, _) = row_state(&pool, &healthy).await;
    assert_eq!((zf2, hf2), (50, 1));
    assert_eq!(z2, z1, "the second pass must not re-stamp retired_at");
    assert!(h2.is_none());
}

// ── case 8: the fetcher pays nothing for a retired recipe ────────────────────

struct CountingHttp {
    api_calls: AtomicUsize,
    other_calls: AtomicUsize,
}

#[async_trait]
impl HttpClient for CountingHttp {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        if req.url.starts_with("https://example.test/api/") {
            self.api_calls.fetch_add(1, Ordering::SeqCst);
        } else {
            self.other_calls.fetch_add(1, Ordering::SeqCst);
        }
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: "<html><body><article>".to_string()
                + &"a healthy page body with plenty of prose. ".repeat(40)
                + "</article></body></html>",
            final_url: req.url,
            cache_hit: false,
        })
    }
}

struct Dead;
#[async_trait]
impl Browser for Dead {
    async fn render(&self, _: RenderRequest) -> Result<RenderedPage> {
        Err(Error::http("no browser in this test"))
    }
}
#[async_trait]
impl Researcher for Dead {
    async fn research(&self, _: ResearchRequest) -> Result<ResearchOutput> {
        Err(Error::http("no researcher in this test"))
    }
}

async fn fetch_with(recipes: RecipeStore, http: Arc<CountingHttp>) -> pumper_core::FetchOutcome {
    let cfg = RecipesConfig {
        enabled: true,
        auto_validate: true,
        ..RecipesConfig::default()
    };
    let fetcher = Fetcher::new(
        http,
        Arc::new(Dead),
        Arc::new(Dead),
        Arc::new(Governor::new(&GovernorConfig::default())),
        &FetcherConfig::default(),
    )
    .with_recipes(Some(Arc::new(recipes) as Arc<dyn RecipeSource>), &cfg);
    let mut req = FetchRequest::new(PAGE);
    req.use_recipes = true;
    fetcher.fetch(req).await.expect("the live tier serves")
}

#[tokio::test]
async fn a_retired_recipe_costs_the_fetcher_no_outbound_request() {
    let store = TempStore::new("recipe-fetcher").await;

    // Control: the candidate is live, so the recipe tier DOES fire. Without
    // this half the assertion below passes for the wrong reason.
    let live = store.storage.recipes().with_retire_after(10);
    let id = live.upsert(&fixture()).await.unwrap();
    let http = Arc::new(CountingHttp {
        api_calls: AtomicUsize::new(0),
        other_calls: AtomicUsize::new(0),
    });
    let out = fetch_with(store.storage.recipes().with_retire_after(10), http.clone()).await;
    assert_eq!(
        http.api_calls.load(Ordering::SeqCst),
        1,
        "control: a live candidate is replayed at the api_recipe tier"
    );
    assert!(
        out.trace.iter().any(|t| t.tier.as_str() == "api_recipe"),
        "control: the tier traces, got {:?}",
        out.trace
            .iter()
            .map(|t| t.tier.as_str())
            .collect::<Vec<_>>()
    );

    // The change: retire it, and the tier costs nothing at all.
    live.retire(&id).await.unwrap();
    let http = Arc::new(CountingHttp {
        api_calls: AtomicUsize::new(0),
        other_calls: AtomicUsize::new(0),
    });
    let out = fetch_with(store.storage.recipes().with_retire_after(10), http.clone()).await;
    assert_eq!(
        http.api_calls.load(Ordering::SeqCst),
        0,
        "a retired recipe must fire ZERO outbound replays"
    );
    assert!(
        !out.trace.iter().any(|t| t.tier.as_str() == "api_recipe"),
        "a retired recipe must not even leave a tier trace: {:?}",
        out.trace
            .iter()
            .map(|t| t.tier.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        http.other_calls.load(Ordering::SeqCst),
        1,
        "the fetch proceeds straight to the live tier"
    );
}
