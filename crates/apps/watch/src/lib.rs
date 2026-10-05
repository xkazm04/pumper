//! Generic scheduled change-watch: point it at any URL and it tells you when
//! (and how) the page changed. Each run fetches the page via the tiered
//! fetcher, reduces it to a compact fingerprint record (title, char count,
//! content hash, excerpt), and upserts it keyed by the URL — so the dataset
//! store's change detection + revision history do the heavy lifting. Pair a
//! run with a cron schedule (`POST /schedules`) and a dataset watch
//! (`POST /watches`) for a Visualping-style monitor with webhook alerts.
//!
//! **Materiality.** The record is also judged: every run compares the body's
//! SimHash fingerprint against the previous run's through
//! [`pumper_core::materiality`], the one judge this fleet shares, and a
//! cosmetic verdict narrows the change-detection hash so the record body is
//! refreshed without appending a revision. That matters because a revision is
//! what a subscribed watch, trigger and webhook actually fire on: before the
//! judge this app had no materiality stage at all — the store's exact-hash
//! `ChangeKind` went straight to the result — so a reordered list or a reflowed
//! paragraph woke somebody at 03:00. The verdict rides in the result AND on the
//! record, so a consumer can read why a run did or did not alert.
//!
//! Note the fingerprint is sha256 for *identity* and SimHash for *magnitude*:
//! the exact hash answers "did any byte move", the SimHash answers "how far",
//! and only the second one can say no.

use async_trait::async_trait;
use pumper_core::datasets::{ChangeKind, UpsertSummary};
use pumper_core::extract::extracted_nothing;
use pumper_core::materiality::{self, Sensitivity, Signal, Verdict};
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, FetchRequest, FetchStrategy, ManifestExample,
    Provenance, Result, ScrapeApp,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct Watch;

/// Excerpt length stored in the record. Long enough that the field-level diff
/// shows *what* changed near the top of the page, short enough to keep
/// revisions compact (the full markdown is saved as a job artifact).
const EXCERPT_CHARS: usize = 600;

/// The record path carrying the SimHash fingerprint of the **whole** fetched
/// body — not of the record, and not of the 600-char excerpt. It is what makes
/// the next run's materiality verdict a measurement over the real document
/// while the stored record stays compact.
const BODY_SIMHASH: &str = "body_simhash";

/// Record paths **computed about** this record rather than observed at the
/// source: the fingerprint and the verdict about the change that produced it.
/// Declared derived on every write, whatever the verdict.
///
/// This is also the reason adopting the judge fires nothing: the two new paths
/// are outside the change-detection hash, so a record written before they
/// existed hashes to exactly what it hashed before and no stored page reports a
/// phantom `changed` on the first run after deploy.
const COMPUTED_PATHS: [&str; 2] = [BODY_SIMHASH, "materiality"];

#[async_trait]
impl ScrapeApp for Watch {
    fn name(&self) -> &'static str {
        "watch"
    }

    fn description(&self) -> &'static str {
        "Watch any URL for content changes. Fetches the page as Markdown, \
         fingerprints it into the `pages` dataset (keyed by URL), and reports \
         new/changed/unchanged with the field-level diff. Params: \
         {\"url\": \"...\", \"strategy\": \"http|browser|auto|auto_with_research\", \
         \"wait_for_selector\": \".main\", \"min_content_chars\": 250, \
         \"cache_ttl_secs\": 60}. Bypasses the HTTP cache by default so it sees \
         live bodies; set `cache_ttl_secs` to cap staleness instead. A fetch that \
         yields no readable content FAILS the run instead of recording a 0-char \
         page — an empty extraction is a failed fetch, not a page change, and \
         alerting on it would cry wolf twice. \
         Every change is judged by `pumper_core::materiality`: a cosmetic \
         delta (a reflow, a reordered list, a rotating ad slot) refreshes the \
         record without appending a revision, so nothing subscribed fires. \
         Schedule it via POST /schedules and subscribe via POST /watches."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url"],
                "properties": {
                    "url": {
                        "type": "string",
                        "pattern": "^https?://",
                        "description": "Page to monitor; it is also the record key in the `pages` dataset."
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["http", "browser", "auto", "auto_with_research"],
                        "description": "Fetch ladder entry point (default \"auto\")."
                    },
                    "wait_for_selector": {
                        "type": "string",
                        "description": "Browser tier only: CSS selector to await before capturing — use it to fingerprint the content region rather than a spinner."
                    },
                    "min_content_chars": { "type": "integer", "minimum": 1 },
                    "cache_ttl_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Cap cache staleness at N seconds instead of bypassing the HTTP cache entirely (the default). Useful when several watches share one hot endpoint."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Watch a release-notes page for changes (schedule this + a dataset watch for webhook alerts)",
                    params: json!({ "url": "https://example.com/releases" }),
                },
                ManifestExample {
                    description: "JS-rendered status page: render in the browser, fingerprint only the content region, and share one cached body across sibling watches",
                    params: json!({
                        "url": "https://status.example.com/",
                        "strategy": "browser",
                        "wait_for_selector": "main .incidents",
                        "cache_ttl_secs": 60
                    }),
                },
            ],
            output_shape: Some(
                "{url, engine, status, change: new|changed|unchanged, chars, diff, \
                 materiality {significance: cosmetic|material|structural, distance, weight, \
                 moved_paths, reason}} — the field-level diff of this run's `pages` record \
                 versus the previous revision, plus the deterministic change-materiality \
                 verdict from `pumper_core::materiality`. A cosmetic verdict rewrites the \
                 record without appending a revision, so no subscribed watch fires; the \
                 verdict is stored on the record too.",
            ),
            cost_class: CostClass::Metered,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx.require_str("url")?.to_string();
        let strategy = match ctx.params.get("strategy").and_then(Value::as_str) {
            Some("http") => FetchStrategy::Http,
            Some("browser") => FetchStrategy::Browser,
            Some("auto_with_research") => FetchStrategy::AutoWithResearch,
            _ => FetchStrategy::Auto,
        };

        let mut req = FetchRequest::new(&url);
        req.strategy = strategy;
        req.to_markdown = true;
        req.wait_for_selector = ctx
            .params
            .get("wait_for_selector")
            .and_then(Value::as_str)
            .map(String::from);
        req.min_content_chars = ctx
            .params
            .get("min_content_chars")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        // Monitors need live bodies, not up-to-TTL-stale cached ones. Default to
        // a full cache bypass; a `cache_ttl_secs` param instead caps staleness
        // to a short TTL (useful when several watches share one hot endpoint).
        match ctx.params.get("cache_ttl_secs").and_then(Value::as_u64) {
            Some(secs) => req.ttl_override = Some(secs),
            None => req.no_cache = true,
        }

        let outcome = ctx.fetch(req).await?;
        let markdown = outcome
            .markdown
            .clone()
            .or_else(|| outcome.text.clone())
            .unwrap_or_default();
        if extracted_nothing(&markdown) {
            // A successful fetch that yields no readable content is a failed
            // extraction, not an empty-but-valid result — and fingerprinting it
            // is worse here than anywhere else in the fleet: the empty body
            // hashes to e3b0c442…, the record upserts as `changed`, and every
            // subscribed webhook fires "the entire page vanished". The next
            // healthy run flips it back and fires a second false alarm. The
            // escalation that would rescue a thin body is best-effort (the
            // ladder returns the last tier's output however thin), so an
            // interstitial or an empty 200 reaches this line routinely. A failed
            // job is visible; a false alert is actively misleading.
            return Err(Error::App(format!(
                "watch: extracted no content from {} (engine {}, status {:?}) — refusing to \
                 fingerprint an empty body as a page change",
                outcome.url, outcome.engine, outcome.status
            )));
        }
        ctx.save_artifact("page.md", markdown.as_bytes()).await?;

        // **Is this change news?** Read the previous fingerprint BEFORE the
        // write replaces it, then let the one judge the fleet shares answer —
        // this app used to have no materiality stage at all, so the store's
        // exact-hash verdict went straight to the result and to every
        // subscribed webhook, and any moved byte was an alert.
        let previous = ctx
            .datasets
            .get(&ctx.app, "pages", &url)
            .await?
            .map(|rec| rec.data);
        let body_simhash = pumper_core::simhash(&markdown);
        let verdict = judge_page(previous.as_ref(), body_simhash);

        // Compact fingerprint: change detection runs on this record, so keep it
        // small but informative — the excerpt makes diffs human-readable.
        let body_sha = hex_sha256(markdown.as_bytes());
        let record = page_record(
            &outcome.url,
            &markdown,
            &body_sha,
            body_simhash,
            &verdict,
            previous.as_ref(),
        );
        // Provenance (M12): this app knows both derivation facts for the record
        // it is about to write — the exact URL the body came from (the post-
        // redirect `outcome.url`, not the requested one) and the sha256 of the
        // body it saved as `page.md`. No RuleSet is involved (the fingerprint is
        // computed in code), so `rules_hash` stays Null rather than invented.
        //
        // Through the DERIVED write path, because the verdict has to be a lever
        // and not a label: a cosmetic verdict narrows the change-detection hash
        // to what it already was, so the body is refreshed and no revision is
        // appended. One record, so the batch-level provenance stamp IS the
        // record-level one.
        let summary = ctx
            .upsert_many_with_derived(
                "pages",
                &[(url.clone(), record)],
                Provenance {
                    source_url: Some(outcome.url.clone()),
                    artifact_sha: Some(body_sha),
                    ..Provenance::default()
                },
                &sensitivity().derived_paths(),
            )
            .await?;
        let change = change_of(&summary);

        // Surface what actually changed straight in the job result.
        let diff = ctx
            .datasets
            .history(&ctx.app, "pages", &url, 1)
            .await?
            .into_iter()
            .next()
            .and_then(|rev| rev.diff);

        Ok(json!({
            "url": outcome.url,
            "engine": outcome.engine,
            "status": outcome.status,
            "change": change,
            "chars": markdown.chars().count(),
            "diff": diff,
            "materiality": verdict.to_value(),
        }))
    }
}

/// The record this run writes.
///
/// **A cosmetic verdict HOLDS the baseline.** The record becomes the previous
/// one with a refreshed `materiality` block and nothing else, which is what
/// makes the write append no revision: the change-detection hash is taken over
/// the record minus [`COMPUTED_PATHS`], so holding every other path means the
/// hash is byte-identical to the stored one and the store reports `unchanged`.
///
/// Two consequences, both deliberate:
///
/// * The stored record is **the last body worth telling you about**, not the
///   last body fetched. The live page is still described — `materiality` and
///   its distance are about this run's body — but the fingerprint the NEXT run
///   is measured against stays put, so a page drifting one bit per run
///   eventually crosses the bound instead of being cosmetic forever against a
///   baseline that creeps along with it.
/// * Adopting the judge fires nothing. [`COMPUTED_PATHS`] are declared derived,
///   so a record written before they existed hashes to exactly what it hashed
///   before; there is no one-time fleet-wide `changed` to apologise for.
fn page_record(
    url: &str,
    markdown: &str,
    body_sha: &str,
    body_simhash: u64,
    verdict: &Verdict,
    previous: Option<&Value>,
) -> Value {
    if let (false, Some(previous)) = (verdict.is_news(), previous) {
        let mut held = previous.clone();
        if let Some(obj) = held.as_object_mut() {
            obj.insert("materiality".into(), verdict.to_value());
            return held;
        }
    }
    json!({
        "url": url,
        "title": first_heading(markdown),
        "chars": markdown.chars().count(),
        "content_sha256": body_sha,
        "excerpt": markdown.chars().take(EXCERPT_CHARS).collect::<String>(),
        // Stored, not merely returned: `watch --preview`, a trigger and any
        // dataset consumer must be able to read WHY a run did or did not alert.
        // The store has computed a per-revision SimHash on every upsert since
        // migration 0004 and exposed it to nobody; this is the monitor's own,
        // over the whole body rather than the 600-char excerpt.
        BODY_SIMHASH: body_simhash as i64,
        "materiality": verdict.to_value(),
    })
}

/// How harshly this app grades its own churn, and what it declares noise.
///
/// The fleet default bounds — 3 bits or fewer is boilerplate churn, 16 or more
/// is not the same document — plus [`COMPUTED_PATHS`] as the declared noise,
/// which is the ONE list that both the judge strips before measuring and the
/// write narrows its change-detection hash by.
fn sensitivity() -> Sensitivity {
    Sensitivity::default().noise(COMPUTED_PATHS)
}

/// **The materiality verdict for this run.** The previous run's stored body
/// fingerprint against this run's, judged by
/// [`pumper_core::materiality`] — the same function `connector-api-watch` and
/// the crawler's `change_weight` now go through.
///
/// A record stored before this app adopted the judge carries no
/// [`BODY_SIMHASH`], so there is no prior fingerprint and the verdict is a
/// first sighting. That is deliberately conservative: with no evidence to
/// discount the change, the run reports news and the store's own exact-hash
/// detection decides whether anything is written at all. It cannot manufacture
/// an alert — a body that did not move is still `unchanged`.
fn judge_page(previous: Option<&Value>, body_simhash: u64) -> Verdict {
    let Some(previous) = previous else {
        return materiality::judge(&Signal::first_sighting(), &sensitivity());
    };
    let previous_simhash = previous
        .get(BODY_SIMHASH)
        .and_then(Value::as_i64)
        .map(|sim| sim as u64);
    materiality::judge(
        &Signal::fingerprints(previous_simhash, body_simhash),
        &sensitivity(),
    )
}

/// The one-record batch's verdict as the single-record [`ChangeKind`] this
/// app's result has always reported. Extracted and named because reading
/// "changed" off `summary.changed.is_empty()` inline is how a monitor starts
/// reporting `unchanged` for a brand-new page.
fn change_of(summary: &UpsertSummary) -> ChangeKind {
    if !summary.new.is_empty() {
        ChangeKind::New
    } else if !summary.changed.is_empty() {
        ChangeKind::Changed
    } else {
        ChangeKind::Unchanged
    }
}

/// First markdown heading, as a cheap page title.
fn first_heading(markdown: &str) -> Option<String> {
    markdown
        .lines()
        .find(|l| l.starts_with('#'))
        .map(|l| l.trim_start_matches('#').trim().to_string())
        .filter(|t| !t.is_empty())
}

fn hex_sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Realistic slice of a page fetched as markdown: nav/preamble text before
    // the first real heading, which is not an h1.
    const PAGE: &str =
        "Skip to content\n\nSign in | Register\n\n## Release Notes\n\nv2.1 shipped today.\n";

    #[test]
    fn first_heading_is_the_stripped_title_not_the_preamble() {
        assert_eq!(first_heading(PAGE).as_deref(), Some("Release Notes"));
    }

    #[test]
    fn hashes_only_line_yields_none_not_an_empty_title() {
        assert_eq!(first_heading("body text\n##\nmore body"), None);
    }

    #[test]
    fn page_without_headings_has_no_title() {
        assert_eq!(first_heading("just a paragraph of text"), None);
    }

    /// Realistic slices of an API docs page, shared verbatim with
    /// `connector-api-watch` and with `pumper_core::materiality`'s own
    /// fixtures. **This is the one-verdict-two-monitors case**: the sibling
    /// monitor had a private line-diff suppressor for exactly this pair and
    /// this app had no answer at all, so the same reflow was noise over there
    /// and a webhook over here.
    const OLD_DOC: &str = "# Payments API\n\n## Endpoints\n\n- POST /v1/charges\n- GET /v1/charges/:id\n\nRate limit: 100 req/min\n";
    const NEW_DOC: &str = "# Payments API\n\n## Endpoints\n\n- POST /v1/payment_intents\n- GET /v1/charges/:id\n\nRate limit: 100 req/min\n";
    const REFLOWED_DOC: &str = "  # Payments API\n## Endpoints\n- GET /v1/charges/:id\n- POST /v1/charges\n\nRate limit: 100 req/min\n";

    /// The record this app would have stored for `markdown`, as the previous
    /// run left it.
    fn stored(markdown: &str) -> Value {
        json!({
            "url": "https://x.test/payments",
            "title": first_heading(markdown),
            "chars": markdown.chars().count(),
            "content_sha256": hex_sha256(markdown.as_bytes()),
            "excerpt": markdown.chars().take(EXCERPT_CHARS).collect::<String>(),
            BODY_SIMHASH: pumper_core::simhash(markdown) as i64,
        })
    }

    #[test]
    fn a_reflow_of_the_siblings_own_fixture_is_cosmetic_here_too() {
        let verdict = judge_page(Some(&stored(OLD_DOC)), pumper_core::simhash(REFLOWED_DOC));
        assert_eq!(
            verdict.significance(),
            pumper_core::Significance::Cosmetic,
            "{}",
            verdict.reason()
        );
        assert!(!verdict.is_news());
    }

    #[test]
    fn a_replaced_endpoint_is_still_news_here() {
        let verdict = judge_page(Some(&stored(OLD_DOC)), pumper_core::simhash(NEW_DOC));
        assert!(verdict.is_news(), "{}", verdict.reason());
    }

    /// The lever, asserted without running a job: a cosmetic run writes the
    /// PREVIOUS record with only the verdict refreshed, so the hash input (the
    /// record minus [`COMPUTED_PATHS`]) is byte-identical to the stored one and
    /// the store cannot append a revision. A material run writes the live body.
    #[test]
    fn a_cosmetic_run_holds_the_baseline_and_a_material_one_replaces_it() {
        let before = stored(OLD_DOC);
        let cosmetic = judge_page(Some(&before), pumper_core::simhash(REFLOWED_DOC));
        let held = page_record(
            "https://x.test/payments",
            REFLOWED_DOC,
            &hex_sha256(REFLOWED_DOC.as_bytes()),
            pumper_core::simhash(REFLOWED_DOC),
            &cosmetic,
            Some(&before),
        );
        let narrow = sensitivity().derived_paths();
        for path in narrow.paths() {
            assert!(
                COMPUTED_PATHS.contains(&path.as_str()),
                "'{path}' is narrowed but is not a computed path"
            );
        }
        for (key, value) in before.as_object().expect("object") {
            if COMPUTED_PATHS.contains(&key.as_str()) {
                continue;
            }
            assert_eq!(
                held.get(key),
                Some(value),
                "'{key}' moved on a cosmetic run, so the write appends a revision"
            );
        }
        assert_eq!(held["materiality"]["significance"], "cosmetic");

        let material = judge_page(Some(&before), pumper_core::simhash(NEW_DOC));
        let fresh = page_record(
            "https://x.test/payments",
            NEW_DOC,
            &hex_sha256(NEW_DOC.as_bytes()),
            pumper_core::simhash(NEW_DOC),
            &material,
            Some(&before),
        );
        assert_ne!(
            fresh["content_sha256"], before["content_sha256"],
            "a material change must write the live body, not hold the baseline"
        );
    }

    /// A record written before this app adopted the judge carries no
    /// fingerprint. The verdict must be the conservative first-sighting one —
    /// never a fabricated "cosmetic", which would silently swallow the first
    /// real change after deploy.
    #[test]
    fn a_record_without_a_stored_fingerprint_is_not_judged_cosmetic() {
        let mut legacy = stored(OLD_DOC);
        legacy.as_object_mut().expect("object").remove(BODY_SIMHASH);
        let verdict = judge_page(Some(&legacy), pumper_core::simhash(REFLOWED_DOC));
        assert!(verdict.is_news(), "{}", verdict.reason());
        assert_eq!(verdict.distance(), None);
    }

    #[test]
    fn a_one_record_batch_reports_new_before_changed_not_unchanged() {
        assert_eq!(
            change_of(&UpsertSummary {
                new: vec!["k".into()],
                ..UpsertSummary::default()
            }),
            ChangeKind::New
        );
        assert_eq!(
            change_of(&UpsertSummary {
                changed: vec!["k".into()],
                ..UpsertSummary::default()
            }),
            ChangeKind::Changed
        );
        assert_eq!(
            change_of(&UpsertSummary {
                unchanged: 1,
                ..UpsertSummary::default()
            }),
            ChangeKind::Unchanged
        );
    }

    #[test]
    fn hex_sha256_matches_the_nist_vector_not_a_double_hash() {
        // NIST test vectors — guard against double-hashing or hex-casing drift,
        // which would flip every stored fingerprint into a false "changed".
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_sha256(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
