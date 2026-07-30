//! API X-ray (M05): discovering the JSON data API behind a JS-heavy page.
//!
//! A render with `RenderRequest.capture_network` set returns the same-origin
//! JSON responses the page loaded ([`crate::engine::CapturedCall`]). The
//! *discovery pass* here scores each captured payload by how many of the page's
//! extracted field values it contains — a payload that holds the very values
//! the extractor scraped out of the DOM is, with high confidence, the API that
//! rendered them. High-overlap calls are generalized into an [`ApiRecipe`]:
//! the parameterizable endpoint (`url_template` + observed `params`) plus the
//! JSON paths where matching values live, persisted `validated: false` until a
//! successful replay proves them.
//!
//! Everything in the discovery half is pure (no I/O), so it is unit-tested
//! without a browser. [`RecipeStore`] (feature `storage`) persists recipes into
//! the `api_recipes` table (migration 0025) and backs `GET /recipes`.
//!
//! ## The fetcher branch (step 4, now taken)
//!
//! A *validated* recipe backs the pre-archive `api_recipe` tier in
//! `fetcher.rs`, ordered api-recipe → archive → live HTTP: when a recipe
//! matches the request's host, the fetcher replays the recipe's API URL
//! ([`ApiRecipe::replay_url`], threading the same `profile` cookies) via the
//! HTTP engine and returns the structured JSON body with a `TierTrace` entry
//! of tier `api_recipe`. The branch is double-opt-in (`FetchRequest.use_recipes`
//! or `[recipes] enabled`, both default-OFF) and never terminal on failure: a
//! non-JSON / non-overlapping / errored replay records a strike
//! ([`RecipeSource::record_failure`]) and falls through to archive/live, and
//! `[recipes] max_failures` consecutive strikes un-validate the recipe. With
//! `[recipes] auto_validate` ON, an unvalidated recipe is tried
//! opportunistically and a successful replay whose payload still overlaps its
//! [`ApiRecipe::json_paths`] ([`payload_overlaps`]) marks it validated.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::engine::CapturedCall;

/// Minimum share of the page's distinct extracted leaf values that must appear
/// in a captured payload for it to become a recipe candidate.
pub const MIN_OVERLAP_SCORE: f64 = 0.25;
/// Minimum number of distinct extracted values that must match (guards tiny
/// extractions where one coincidental match would clear the ratio threshold).
pub const MIN_MATCHED_VALUES: usize = 3;
/// Leaf values shorter than this (after normalization) are ignored on both
/// sides — "1", "ok", "US" style tokens match everything and carry no signal.
const MIN_VALUE_LEN: usize = 3;
/// Ceiling on the leaf values walked out of one payload, so a pathological
/// multi-megabyte capture can't turn discovery into an O(huge) set build.
const MAX_PAYLOAD_LEAVES: usize = 20_000;

/// A discovered (or operator-curated) API endpoint behind a rendered page.
/// `validated` stays `false` until a replay of the template against the live
/// host has been proven to return the expected shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiRecipe {
    /// Stable id (UUID at first persist).
    pub id: String,
    /// Host the recipe applies to (lowercase, no port).
    pub host: String,
    /// The endpoint with query-parameter values replaced by `{name}`
    /// placeholders, e.g. `https://api.example.com/v2/search?q={q}&page={page}`.
    pub url_template: String,
    /// Observed example value per templated parameter (`{"q": "grants", ...}`).
    pub params: Value,
    /// Generalized JSON paths (numeric indices → `[*]`) whose leaf values
    /// overlapped the page's extracted fields — where the data lives.
    pub json_paths: Vec<String>,
    /// Share of the page's distinct extracted values found in this payload.
    pub score: f64,
    /// Proven by a successful replay. Discovery always writes `false`.
    pub validated: bool,
}

impl ApiRecipe {
    /// Fills the observed `params` back into `url_template`, yielding the
    /// concrete API URL a replay fetches. Values are form-encoded. Returns
    /// `None` when a placeholder has no observed value (a template that can't
    /// be replayed blind) — the fetcher then skips the recipe branch.
    pub fn replay_url(&self) -> Option<String> {
        let mut url = self.url_template.clone();
        if let Value::Object(map) = &self.params {
            for (k, v) in map {
                let raw = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let encoded: String =
                    url::form_urlencoded::byte_serialize(raw.as_bytes()).collect();
                url = url.replace(&format!("{{{k}}}"), &encoded);
            }
        }
        // Any placeholder left unfilled means the template needs a value we
        // never observed — refuse rather than fire a literal `{page}` upstream.
        (!url.contains('{')).then_some(url)
    }
}

/// Whether a replayed payload still carries at least one of the recipe's
/// expected field paths — the "same shape" half of validating a replay. Paths
/// are the generalized (`[*]`) forms discovery produced, so any row count
/// matches; an API that renamed or restructured its fields stops overlapping
/// and the replay counts as thin.
pub fn payload_overlaps(json_paths: &[String], payload: &Value) -> bool {
    if json_paths.is_empty() {
        return false;
    }
    let mut leaves = Vec::new();
    collect_payload_leaves(payload, "$", &mut leaves);
    let present: BTreeSet<&str> = leaves.iter().map(|(p, _)| p.as_str()).collect();
    json_paths.iter().any(|p| present.contains(p.as_str()))
}

/// Read side of the fetcher's `api_recipe` tier plus its strike/validation
/// state machine. Object-safe so the `Fetcher` (which compiles without the
/// `storage` feature) can hold `Option<Arc<dyn RecipeSource>>`; [`RecipeStore`]
/// is the real implementation.
#[async_trait::async_trait]
pub trait RecipeSource: Send + Sync {
    /// Best recipe for `host`: validated recipes first, then best score. With
    /// `include_unvalidated = false` (the `auto_validate`-OFF fetcher default)
    /// only validated recipes are returned.
    async fn best_for_host(
        &self,
        host: &str,
        include_unvalidated: bool,
    ) -> crate::Result<Option<ApiRecipe>>;

    /// A successful overlapping replay: resets the consecutive-failure counter,
    /// and with `validate = true` (the `auto_validate` path) also marks the
    /// recipe validated.
    async fn record_success(&self, id: &str, validate: bool) -> crate::Result<()>;

    /// A failed/thin replay: increments the consecutive-failure counter.
    /// Returns `true` when this strike crossed `unvalidate_after` and a
    /// validated recipe was demoted back to unvalidated (counter reset so the
    /// next validation attempt starts clean).
    async fn record_failure(&self, id: &str, unvalidate_after: u32) -> crate::Result<bool>;
}

/// Normalizes a leaf value for overlap comparison: strings are trimmed +
/// lowercased; numbers/bools rendered canonically; null/objects/arrays are not
/// leaves. Values that normalize too short are dropped (no signal).
fn normalize_leaf(v: &Value) -> Option<String> {
    let s = match v {
        Value::String(s) => s.trim().to_lowercase(),
        Value::Number(n) => n.to_string(),
        Value::Bool(_) | Value::Null | Value::Object(_) | Value::Array(_) => return None,
    };
    (s.len() >= MIN_VALUE_LEN).then_some(s)
}

/// Collects the normalized leaf values of `v` (strings + numbers) into `out`.
fn collect_extracted_leaves(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(map) => {
            for child in map.values() {
                collect_extracted_leaves(child, out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_extracted_leaves(child, out);
            }
        }
        leaf => {
            if let Some(s) = normalize_leaf(leaf) {
                out.insert(s);
            }
        }
    }
}

/// Walks a captured payload collecting `(generalized_path, normalized_value)`
/// pairs. Array indices generalize to `[*]` so every row of a listing folds
/// onto one path (`$.items[*].title`), bounded by [`MAX_PAYLOAD_LEAVES`].
fn collect_payload_leaves(v: &Value, path: &str, out: &mut Vec<(String, String)>) {
    if out.len() >= MAX_PAYLOAD_LEAVES {
        return;
    }
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                collect_payload_leaves(child, &format!("{path}.{k}"), out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_payload_leaves(child, &format!("{path}[*]"), out);
            }
        }
        leaf => {
            if let Some(s) = normalize_leaf(leaf) {
                out.push((path.to_string(), s));
            }
        }
    }
}

/// Lowercased host of a URL, if it parses.
fn host_of(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_lowercase))
}

/// Replaces every query-parameter value with a `{name}` placeholder and returns
/// the template plus the observed values. A URL with no query templates to
/// itself with empty params. Non-parsing URLs return `None`.
fn template_url(url: &str) -> Option<(String, Value)> {
    let parsed = url::Url::parse(url).ok()?;
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    let mut base = parsed.clone();
    base.set_query(None);
    base.set_fragment(None);
    if pairs.is_empty() {
        return Some((base.to_string(), Value::Object(Default::default())));
    }
    let query: Vec<String> = pairs.iter().map(|(k, _)| format!("{k}={{{k}}}")).collect();
    let mut params = serde_json::Map::new();
    for (k, v) in pairs {
        params.insert(k, Value::String(v));
    }
    Some((
        format!("{}?{}", base, query.join("&")),
        Value::Object(params),
    ))
}

/// The discovery pass: scores each captured call's payload by leaf-value
/// overlap with the page's `extracted` records, and generalizes the calls that
/// clear the thresholds into recipe candidates (`validated: false`, `id`
/// empty — assigned at persist time). Pure; call-order stable (best score
/// first). `extracted` is typically the record values an extractor produced
/// from the same rendered page.
pub fn discover_recipes(network: &[CapturedCall], extracted: &[Value]) -> Vec<ApiRecipe> {
    let mut wanted = BTreeSet::new();
    for v in extracted {
        collect_extracted_leaves(v, &mut wanted);
    }
    if wanted.is_empty() {
        return Vec::new();
    }
    let need = MIN_MATCHED_VALUES.min(wanted.len());

    let mut out = Vec::new();
    for call in network {
        let mut leaves = Vec::new();
        collect_payload_leaves(&call.body, "$", &mut leaves);
        // Distinct extracted values found in this payload, and the paths where.
        let mut matched: BTreeSet<&str> = BTreeSet::new();
        let mut paths: BTreeSet<String> = BTreeSet::new();
        for (path, value) in &leaves {
            if let Some(hit) = wanted.get(value.as_str()) {
                matched.insert(hit.as_str());
                paths.insert(path.clone());
            }
        }
        let score = matched.len() as f64 / wanted.len() as f64;
        if matched.len() < need || score < MIN_OVERLAP_SCORE {
            continue;
        }
        let Some(host) = host_of(&call.url) else {
            continue;
        };
        let Some((url_template, params)) = template_url(&call.url) else {
            continue;
        };
        out.push(ApiRecipe {
            id: String::new(),
            host,
            url_template,
            params,
            json_paths: paths.into_iter().collect(),
            score,
            validated: false,
        });
    }
    // Best candidate first; ties keep capture order (stable sort).
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    // One recipe per (host, template): keep the best-scoring occurrence.
    let mut seen: BTreeMap<(String, String), ()> = BTreeMap::new();
    out.retain(|r| {
        seen.insert((r.host.clone(), r.url_template.clone()), ())
            .is_none()
    });
    out
}

// ── persistence (feature `storage`) ─────────────────────────────────────────

/// SQLite-backed store for [`ApiRecipe`]s — the `api_recipes` table (migration
/// 0025). Follows the `TierMemory` pattern: a small pool-wrapping handle,
/// constructed via [`crate::storage::Storage::recipes`] and shared on
/// `AppContext`. Upserts key on `(host, url_template)`: re-discovery refreshes
/// score/params/paths and `last_seen_at` but never un-validates a recipe.
#[cfg(feature = "storage")]
pub struct RecipeStore {
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "storage")]
impl RecipeStore {
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }

    /// Inserts or refreshes one discovered recipe. Returns the stored id.
    pub async fn upsert(&self, recipe: &ApiRecipe) -> crate::Result<String> {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        let id = if recipe.id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            recipe.id.clone()
        };
        let params = serde_json::to_string(&recipe.params)?;
        let paths = serde_json::to_string(&recipe.json_paths)?;
        // On re-discovery keep the original id + validated flag; refresh the
        // learned shape and last_seen_at.
        let stored: (String,) = sqlx::query_as(
            "INSERT INTO api_recipes \
               (id, host, url_template, params, json_paths, score, validated, \
                discovered_at, last_seen_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7) \
             ON CONFLICT(host, url_template) DO UPDATE SET \
               params = excluded.params, \
               json_paths = excluded.json_paths, \
               score = excluded.score, \
               last_seen_at = excluded.last_seen_at \
             RETURNING id",
        )
        .bind(&id)
        .bind(&recipe.host)
        .bind(&recipe.url_template)
        .bind(&params)
        .bind(&paths)
        .bind(recipe.score)
        .bind(&now)
        .fetch_one(&self.pool)
        .await?;
        Ok(stored.0)
    }

    /// Lists recipes, best score first, optionally filtered by host.
    pub async fn list(&self, host: Option<&str>, limit: i64) -> crate::Result<Vec<Value>> {
        let rows: Vec<(
            String,
            String,
            String,
            String,
            String,
            f64,
            i64,
            String,
            String,
        )> = sqlx::query_as(
            "SELECT id, host, url_template, params, json_paths, score, validated, \
                        discovered_at, last_seen_at \
                 FROM api_recipes \
                 WHERE (?1 IS NULL OR host = ?1) \
                 ORDER BY score DESC, host, url_template \
                 LIMIT ?2",
        )
        .bind(host)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(id, host, url_template, params, json_paths, score, validated, disc, seen)| {
                    serde_json::json!({
                        "id": id,
                        "host": host,
                        "url_template": url_template,
                        "params": serde_json::from_str::<Value>(&params)
                            .unwrap_or(Value::Null),
                        "json_paths": serde_json::from_str::<Value>(&json_paths)
                            .unwrap_or(Value::Null),
                        "score": score,
                        "validated": validated != 0,
                        "discovered_at": disc,
                        "last_seen_at": seen,
                    })
                },
            )
            .collect())
    }

    /// Flips a recipe's `validated` flag (a successful replay proves it).
    /// Returns whether the recipe existed.
    pub async fn set_validated(&self, id: &str, validated: bool) -> crate::Result<bool> {
        let res = sqlx::query("UPDATE api_recipes SET validated = ?2 WHERE id = ?1")
            .bind(id)
            .bind(validated as i64)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }
}

/// The real [`RecipeSource`]: rows from `api_recipes`, strikes in its
/// `consecutive_failures` column (migration 0028).
#[cfg(feature = "storage")]
#[async_trait::async_trait]
impl RecipeSource for RecipeStore {
    async fn best_for_host(
        &self,
        host: &str,
        include_unvalidated: bool,
    ) -> crate::Result<Option<ApiRecipe>> {
        let row: Option<(String, String, String, String, String, f64, i64)> = sqlx::query_as(
            "SELECT id, host, url_template, params, json_paths, score, validated \
             FROM api_recipes \
             WHERE host = ?1 AND (validated = 1 OR ?2) \
             ORDER BY validated DESC, score DESC, url_template \
             LIMIT 1",
        )
        .bind(host)
        .bind(include_unvalidated)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(id, host, url_template, params, json_paths, score, validated)| ApiRecipe {
                id,
                host,
                url_template,
                params: serde_json::from_str(&params).unwrap_or(Value::Null),
                json_paths: serde_json::from_str(&json_paths).unwrap_or_default(),
                score,
                validated: validated != 0,
            },
        ))
    }

    async fn record_success(&self, id: &str, validate: bool) -> crate::Result<()> {
        sqlx::query(
            "UPDATE api_recipes \
             SET consecutive_failures = 0, \
                 validated = CASE WHEN ?2 THEN 1 ELSE validated END \
             WHERE id = ?1",
        )
        .bind(id)
        .bind(validate)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn record_failure(&self, id: &str, unvalidate_after: u32) -> crate::Result<bool> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "UPDATE api_recipes SET consecutive_failures = consecutive_failures + 1 \
             WHERE id = ?1 \
             RETURNING consecutive_failures, validated",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some((failures, validated)) = row else {
            return Ok(false); // recipe deleted since the fetch looked it up
        };
        if validated != 0 && failures >= i64::from(unvalidate_after) {
            sqlx::query(
                "UPDATE api_recipes SET validated = 0, consecutive_failures = 0 WHERE id = ?1",
            )
            .bind(id)
            .execute(&self.pool)
            .await?;
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn call(url: &str, body: Value) -> CapturedCall {
        CapturedCall {
            url: url.into(),
            method: "GET".into(),
            status: 200,
            content_type: "application/json".into(),
            body,
        }
    }

    #[test]
    fn template_url_replaces_query_values_and_records_params() {
        let (t, p) =
            template_url("https://api.example.com/v2/search?q=grants&page=2&size=50").unwrap();
        assert_eq!(
            t,
            "https://api.example.com/v2/search?q={q}&page={page}&size={size}"
        );
        assert_eq!(p, json!({"q": "grants", "page": "2", "size": "50"}));
        // No query → template is the bare URL, no params.
        let (t, p) = template_url("https://api.example.com/v2/feed").unwrap();
        assert_eq!(t, "https://api.example.com/v2/feed");
        assert_eq!(p, json!({}));
    }

    #[test]
    fn payload_paths_generalize_array_indices() {
        let mut leaves = Vec::new();
        collect_payload_leaves(
            &json!({"items": [{"title": "Alpha Grant"}, {"title": "Beta Grant"}]}),
            "$",
            &mut leaves,
        );
        assert_eq!(
            leaves,
            vec![
                ("$.items[*].title".to_string(), "alpha grant".to_string()),
                ("$.items[*].title".to_string(), "beta grant".to_string()),
            ]
        );
    }

    #[test]
    fn discovery_scores_overlap_and_emits_a_recipe() {
        let extracted = vec![
            json!({"title": "Alpha Grant", "amount": 50000, "agency": "Dept of Energy"}),
            json!({"title": "Beta Grant", "amount": 75000, "agency": "Dept of Labor"}),
        ];
        let network = vec![
            // The real data API: carries the extracted values.
            call(
                "https://example.com/api/search?q=grants&page=1",
                json!({"results": [
                    {"title": "Alpha Grant", "amount": 50000, "agency": "Dept of Energy"},
                    {"title": "Beta Grant", "amount": 75000, "agency": "Dept of Labor"}
                ]}),
            ),
            // Analytics noise: no overlap.
            call(
                "https://example.com/api/telemetry",
                json!({"session": "zzz-123", "events": 4}),
            ),
        ];
        let recipes = discover_recipes(&network, &extracted);
        assert_eq!(recipes.len(), 1, "only the overlapping call qualifies");
        let r = &recipes[0];
        assert_eq!(r.host, "example.com");
        assert_eq!(
            r.url_template,
            "https://example.com/api/search?q={q}&page={page}"
        );
        assert_eq!(r.params, json!({"q": "grants", "page": "1"}));
        assert!(r.json_paths.contains(&"$.results[*].title".to_string()));
        assert!(r.json_paths.contains(&"$.results[*].amount".to_string()));
        assert!(r.score > 0.9, "all extracted values present: {}", r.score);
        assert!(!r.validated, "discovery never validates");
    }

    #[test]
    fn discovery_requires_minimum_matches_and_score() {
        // 1 coincidental match out of many extracted values → rejected.
        let extracted: Vec<Value> = (0..10)
            .map(|i| json!({"name": format!("record number {i}")}))
            .collect();
        let network = vec![call(
            "https://example.com/api/x",
            json!({"name": "record number 3"}),
        )];
        assert!(discover_recipes(&network, &extracted).is_empty());
        // No extracted fields at all → nothing to score against.
        assert!(discover_recipes(&network, &[]).is_empty());
    }

    #[test]
    fn short_and_non_leaf_values_carry_no_signal() {
        assert_eq!(normalize_leaf(&json!("  Alpha  ")), Some("alpha".into()));
        assert_eq!(normalize_leaf(&json!(50000)), Some("50000".into()));
        assert_eq!(normalize_leaf(&json!("ok")), None, "too short");
        assert_eq!(
            normalize_leaf(&json!(true)),
            None,
            "booleans match anything"
        );
        assert_eq!(normalize_leaf(&json!(null)), None);
    }

    #[test]
    fn duplicate_templates_keep_the_best_scoring_occurrence() {
        let extracted = vec![json!({"a": "alpha grant", "b": "beta grant", "c": "gamma grant"})];
        let full =
            json!({"rows": [{"v": "alpha grant"}, {"v": "beta grant"}, {"v": "gamma grant"}]});
        let partial = json!({"rows": [{"v": "alpha grant"}, {"v": "beta grant"}, {"v": "gamma grant"}, {"x": "noise value"}]});
        let network = vec![
            call("https://example.com/api/list?page=1", partial),
            call("https://example.com/api/list?page=2", full),
        ];
        let recipes = discover_recipes(&network, &extracted);
        assert_eq!(
            recipes.len(),
            1,
            "same (host, template) folds to one recipe"
        );
        assert_eq!(
            recipes[0].url_template,
            "https://example.com/api/list?page={page}"
        );
    }

    fn recipe_fixture() -> ApiRecipe {
        ApiRecipe {
            id: String::new(),
            host: "example.com".into(),
            url_template: "https://example.com/api/search?q={q}&page={page}".into(),
            params: json!({"q": "grant writing", "page": "2"}),
            json_paths: vec!["$.results[*].title".into()],
            score: 0.9,
            validated: false,
        }
    }

    #[test]
    fn replay_url_fills_and_encodes_observed_params() {
        let r = recipe_fixture();
        assert_eq!(
            r.replay_url().unwrap(),
            "https://example.com/api/search?q=grant+writing&page=2"
        );
        // A template with no query replays as itself.
        let mut bare = recipe_fixture();
        bare.url_template = "https://example.com/api/feed".into();
        bare.params = json!({});
        assert_eq!(bare.replay_url().unwrap(), "https://example.com/api/feed");
    }

    #[test]
    fn replay_url_refuses_unfilled_placeholders() {
        // A placeholder with no observed value must not fire literally.
        let mut r = recipe_fixture();
        r.params = json!({"q": "grants"}); // {page} has no value
        assert!(r.replay_url().is_none());
    }

    #[test]
    fn payload_overlap_checks_the_generalized_paths() {
        let paths = vec!["$.results[*].title".to_string()];
        // Same shape, different rows → overlaps (paths generalize row indices).
        let good = json!({"results": [{"title": "Newly Posted Grant"}]});
        assert!(payload_overlaps(&paths, &good));
        // Restructured API → no overlap → the replay counts as thin.
        let renamed = json!({"items": [{"name": "Newly Posted Grant"}]});
        assert!(!payload_overlaps(&paths, &renamed));
        // No expected paths at all can never validate anything.
        assert!(!payload_overlaps(&[], &good));
    }

    // ── strike / validation state machine (storage-backed) ──────────────────

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn strike_machine_unvalidates_after_n_consecutive_failures() {
        let store = crate::testing::TempStore::new("recipe-strikes").await;
        let recipes = store.storage.recipes();
        let id = recipes.upsert(&recipe_fixture()).await.unwrap();
        recipes.set_validated(&id, true).await.unwrap();

        // Two strikes under a threshold of 3: still validated.
        assert!(!recipes.record_failure(&id, 3).await.unwrap());
        assert!(!recipes.record_failure(&id, 3).await.unwrap());
        let r = recipes.best_for_host("example.com", false).await.unwrap();
        assert!(r.is_some_and(|r| r.validated), "2 strikes must not demote");

        // A success in between resets the counter...
        recipes.record_success(&id, false).await.unwrap();
        assert!(!recipes.record_failure(&id, 3).await.unwrap());
        assert!(!recipes.record_failure(&id, 3).await.unwrap());
        // ...so it takes 3 fresh consecutive failures to demote.
        assert!(
            recipes.record_failure(&id, 3).await.unwrap(),
            "third consecutive strike must un-validate"
        );
        assert!(
            recipes
                .best_for_host("example.com", false)
                .await
                .unwrap()
                .is_none(),
            "a demoted recipe is invisible to validated-only lookups"
        );
        // Unknown id: no panic, no demotion signal.
        assert!(!recipes.record_failure("nope", 3).await.unwrap());
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn best_for_host_prefers_validated_and_gates_unvalidated() {
        let store = crate::testing::TempStore::new("recipe-lookup").await;
        let recipes = store.storage.recipes();

        let mut low = recipe_fixture();
        low.url_template = "https://example.com/api/old?page={page}".into();
        low.params = json!({"page": "1"});
        low.score = 0.5;
        let low_id = recipes.upsert(&low).await.unwrap();
        recipes.set_validated(&low_id, true).await.unwrap();
        let high_unvalidated = recipe_fixture(); // score 0.9, validated: false
        recipes.upsert(&high_unvalidated).await.unwrap();

        // Validated-only lookup: the lower-scoring validated recipe wins.
        let r = recipes
            .best_for_host("example.com", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.id, low_id);
        assert!(r.validated);
        // Opportunistic lookup still prefers validated over raw score.
        let r = recipes
            .best_for_host("example.com", true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            r.id, low_id,
            "validated outranks score even opportunistically"
        );
        // Host filter must hold.
        assert!(recipes
            .best_for_host("other.example", true)
            .await
            .unwrap()
            .is_none());

        // record_success(validate: true) promotes — the auto_validate path:
        // demote the validated one, promote the high-scorer, and the
        // validated-only lookup flips to it.
        recipes.set_validated(&low_id, false).await.unwrap();
        let hi = recipes
            .best_for_host("example.com", true)
            .await
            .unwrap()
            .unwrap();
        assert!(!hi.validated);
        recipes.record_success(&hi.id, true).await.unwrap();
        let r = recipes
            .best_for_host("example.com", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.id, hi.id, "a validating success must promote the recipe");
    }
}
