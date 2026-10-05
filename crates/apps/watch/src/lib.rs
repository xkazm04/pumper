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
    diff_values, AppContext, AppManifest, CostClass, DerivedPaths, Error, FetchRequest,
    FetchStrategy, ManifestExample, Provenance, Result, ScrapeApp,
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

/// How many stored revisions a preview reads to answer "how noisy is this
/// URL". Deep enough that a weekly monitor's season of history is in frame,
/// shallow enough that it is one indexed keyset read.
const NOISE_WINDOW: i64 = 20;

/// Fewer stored revisions than this and the preview reports **no rate at all**.
/// The failure mode being refused is a brand-new monitor with one `new`
/// revision reporting a 100% change rate and being dismissed as noisy; one
/// sample is not a rate, and a confident-looking number off one sample is worse
/// than an honest "unproven".
const MIN_REVISIONS_FOR_A_RATE: usize = 3;

/// At or above this share of recorded revisions being `changed`, subscribing to
/// this URL means being paged on most of its runs.
const NOISY_AT_OR_ABOVE: f64 = 0.5;

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
         Set `preview: true` to find out whether a monitor is worth having \
         BEFORE subscribing to it: the real fetch, the real refusal and the \
         real verdict, reported as `would_change` plus a `noise` rate over the \
         stored revision history — and nothing written at all (no record, no \
         revision, no artifact), so no watch, trigger or webhook fires. \
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
                    },
                    "preview": {
                        "type": "boolean",
                        "default": false,
                        "description": "Dry run: fetch live, judge for real, write NOTHING. Returns `would_change` instead of `change`, plus a `noise` rate over the stored history, and leaves no record, revision or artifact behind — so no watch fires. Forces a cache bypass and ignores `cache_ttl_secs`."
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
                ManifestExample {
                    description: "Is this URL worth monitoring at all? Dry run: judges the live page and reports its recorded noise rate, writing nothing",
                    params: json!({
                        "url": "https://status.example.com/",
                        "preview": true
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
                 verdict is stored on the record too. \
                 With `preview: true` the result is instead {url, engine, status, preview: \
                 true, would_change: new|changed|unchanged, chars, diff, materiality {…}, \
                 noise {runs, changed, rate, last_changed_at, verdict: \
                 quiet|noisy|unproven}, writes {record_written: false, revision_appended: \
                 false, artifact_saved: false}} — `would_change` is what `change` would have \
                 been, and the `writes` block is the dry run's own receipt that it wrote \
                 nothing. `noise.rate` is null until there are enough stored revisions to \
                 be a rate.",
            ),
            cost_class: CostClass::Metered,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx.require_str("url")?.to_string();
        let preview = ctx
            .params
            .get("preview")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let outcome = ctx.fetch(fetch_request(&url, &ctx.params, preview)).await?;
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
        // Not under a preview: an artifact left on disk is a side effect, and a
        // dry run with side effects is a run. The refusal above, by contrast, is
        // reached by BOTH paths on purpose — a preview that reported "looks
        // fine" where every scheduled run will fail would be the worst possible
        // answer to "is this monitor worth having".
        if !preview {
            ctx.save_artifact("page.md", markdown.as_bytes()).await?;
        }

        // **Is this change news?** Read the previous fingerprint BEFORE the
        // write replaces it, then let the one judge the fleet shares answer —
        // this app used to have no materiality stage at all, so the store's
        // exact-hash verdict went straight to the result and to every
        // subscribed webhook, and any moved byte was an alert.
        let stored = ctx.datasets.get(&ctx.app, "pages", &url).await?;
        // A tombstoned record still exists as a row, and the store calls a
        // revived one `changed` — so the preview needs the flag, not just the
        // body, or it would predict `new` where the run reports `changed`.
        let revived = stored.as_ref().is_some_and(|rec| rec.removed_at.is_some());
        let previous = stored.map(|rec| rec.data);
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

        // **The dry run stops here.** Everything above it is the production
        // path, verbatim: the same fetch, the same refusal, the same
        // `judge_page` verdict and the same `page_record`. Everything below it
        // writes. There is no second evaluator — `would_change` replays the
        // store's own change-detection predicate over the record this run would
        // have handed it, and the diff is `pumper_core::diff_values`, the very
        // function the upsert uses to build a revision diff. That is the point:
        // a preview that ran its own judge would be a gate watching a proxy.
        if preview {
            let would = would_change(
                previous.as_ref(),
                revived,
                &record,
                &sensitivity().derived_paths(),
            );
            let diff = (would == ChangeKind::Changed)
                .then(|| previous.as_ref().map(|old| diff_values(old, &record)))
                .flatten();
            let revisions = ctx
                .datasets
                .history(&ctx.app, "pages", &url, NOISE_WINDOW)
                .await?;
            let kinds: Vec<&str> = revisions.iter().map(|rev| rev.change.as_str()).collect();
            let last_changed_at = revisions
                .iter()
                .find(|rev| rev.change == "changed")
                .map(|rev| pumper_core::datasets::ts(rev.created_at));
            return Ok(json!({
                "url": outcome.url,
                "engine": outcome.engine,
                "status": outcome.status,
                "preview": true,
                "would_change": would,
                "chars": markdown.chars().count(),
                "diff": diff,
                "materiality": verdict.to_value(),
                "noise": noise_of(&kinds, last_changed_at),
                // The receipt. A dry run that merely *says* it is a dry run is
                // a claim; these three are what the acceptance asserts against
                // the store afterwards.
                "writes": {
                    "record_written": false,
                    "revision_appended": false,
                    "artifact_saved": false,
                },
            }));
        }

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

/// The fetch this run asks for.
///
/// Extracted because the one thing a preview must not inherit is a stale body.
/// A monitor already distinguishes "monitors need live bodies" from "cap
/// staleness"; a dry run is the strictest case of the former, so `preview`
/// forces the full cache bypass and **ignores `cache_ttl_secs` entirely**.
/// With the configured default TTL at 3600s, honouring it would let an operator
/// decide whether a monitor is worth having by looking at an hour-old page —
/// which is exactly what `dry-run-preview` forbids: the fetch that arms the
/// decision is live, or it is labelled with its age.
fn fetch_request(url: &str, params: &Value, preview: bool) -> FetchRequest {
    let mut req = FetchRequest::new(url);
    req.strategy = match params.get("strategy").and_then(Value::as_str) {
        Some("http") => FetchStrategy::Http,
        Some("browser") => FetchStrategy::Browser,
        Some("auto_with_research") => FetchStrategy::AutoWithResearch,
        _ => FetchStrategy::Auto,
    };
    req.to_markdown = true;
    req.wait_for_selector = params
        .get("wait_for_selector")
        .and_then(Value::as_str)
        .map(String::from);
    req.min_content_chars = params
        .get("min_content_chars")
        .and_then(Value::as_u64)
        .map(|n| n as usize);
    // Monitors need live bodies, not up-to-TTL-stale cached ones. Default to a
    // full cache bypass; a `cache_ttl_secs` param instead caps staleness to a
    // short TTL (useful when several watches share one hot endpoint) — but
    // never under a preview.
    match params.get("cache_ttl_secs").and_then(Value::as_u64) {
        Some(secs) if !preview => req.ttl_override = Some(secs),
        _ => req.no_cache = true,
    }
    req
}

/// **What the store would report, without asking it to write.**
///
/// Not a second change-detection rule — the store's own one, replayed. The
/// upsert compares a sha256 over the canonical JSON of the record *minus* its
/// declared [`DerivedPaths`]; canonical JSON is injective, so equal narrowed
/// values and equal hashes are the same statement, and comparing the values
/// directly needs no access to the store's private hasher. The revived-tombstone
/// rule is carried too: the store calls a reappearing record `changed` even when
/// its content matches.
///
/// The equality that actually guards this is in
/// `tests/preview_writes_nothing.rs`: seeded identically, a preview's
/// `would_change` must equal a real run's `change`. Prose cannot hold that; the
/// test can.
fn would_change(
    previous: Option<&Value>,
    revived: bool,
    candidate: &Value,
    derived: &DerivedPaths,
) -> ChangeKind {
    let Some(previous) = previous else {
        return ChangeKind::New;
    };
    if revived {
        return ChangeKind::Changed;
    }
    if narrowed(previous, derived.paths()) == narrowed(candidate, derived.paths()) {
        ChangeKind::Unchanged
    } else {
        ChangeKind::Changed
    }
}

/// `value` with every declared noise path removed — the change-detection input.
/// Mirrors the store's own dot-separated, objects-only walk, so a nested
/// declaration narrows here exactly as it narrows there.
fn narrowed(value: &Value, paths: &[String]) -> Value {
    let mut out = value.clone();
    for path in paths {
        let mut cursor = &mut out;
        let mut segments = path.split('.').peekable();
        while let Some(segment) = segments.next() {
            let Value::Object(map) = cursor else { break };
            if segments.peek().is_none() {
                map.remove(segment);
                break;
            }
            match map.get_mut(segment) {
                Some(next) => cursor = next,
                None => break,
            }
        }
    }
    out
}

/// **How noisy this URL has been**, from the revision history the app already
/// reads on every run — it just only ever asked for one.
///
/// `runs` counts **recorded revisions**, not scheduled executions, and that is
/// the honest denominator available: an unchanged run appends nothing, so the
/// store has no row for it. The rate is therefore "of the times this page moved
/// at all, how many were real changes" — which is the number that predicts
/// whether a subscription pages you, because a revision is precisely what a
/// watch fires on.
///
/// Three-valued on purpose. A two-revision history reporting `rate: 1.0` would
/// label every brand-new monitor noisy, so below
/// [`MIN_REVISIONS_FOR_A_RATE`] the verdict is `unproven` and the rate is
/// `null` — never a fabricated 0, and never a confident number off one sample.
fn noise_of(kinds: &[&str], last_changed_at: Option<String>) -> Value {
    let runs = kinds.len();
    let changed = kinds.iter().filter(|k| **k == "changed").count();
    let rate = (runs >= MIN_REVISIONS_FOR_A_RATE).then(|| changed as f64 / runs as f64);
    let verdict = match rate {
        None => "unproven",
        Some(r) if r >= NOISY_AT_OR_ABOVE => "noisy",
        Some(_) => "quiet",
    };
    json!({
        "runs": runs,
        "changed": changed,
        "rate": rate,
        "last_changed_at": last_changed_at,
        "verdict": verdict,
    })
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

    /// **A preview never evaluates a monitor against a cached body.** The
    /// default HTTP cache TTL is 3600s, so a preview that honoured
    /// `cache_ttl_secs` could tell an operator "this page has not moved" about a
    /// page as it stood an hour ago — the one thing `dry-run-preview` forbids.
    #[test]
    fn a_preview_bypasses_the_cache_even_when_a_ttl_was_asked_for() {
        let params = json!({ "cache_ttl_secs": 3600 });
        let req = fetch_request("https://x.test/", &params, true);
        assert!(req.no_cache, "a preview must fetch live");
        assert_eq!(
            req.ttl_override, None,
            "a preview must ignore cache_ttl_secs"
        );
    }

    /// ...and the non-preview path is byte-for-byte what it always was: the TTL
    /// is honoured, which is what lets sibling watches share one hot endpoint.
    #[test]
    fn a_real_run_still_honours_cache_ttl_secs() {
        let req = fetch_request("https://x.test/", &json!({ "cache_ttl_secs": 60 }), false);
        assert_eq!(req.ttl_override, Some(60));
        assert!(!req.no_cache);

        let bare = fetch_request("https://x.test/", &json!({}), false);
        assert!(bare.no_cache, "absent TTL still means a full bypass");
        assert_eq!(bare.ttl_override, None);
    }

    #[test]
    fn the_strategy_and_selector_are_read_the_same_either_way() {
        let params =
            json!({ "strategy": "browser", "wait_for_selector": ".x", "min_content_chars": 9 });
        for preview in [false, true] {
            let req = fetch_request("https://x.test/", &params, preview);
            assert!(matches!(req.strategy, pumper_core::FetchStrategy::Browser));
            assert_eq!(req.wait_for_selector.as_deref(), Some(".x"));
            assert_eq!(req.min_content_chars, Some(9));
            assert!(req.to_markdown);
        }
    }

    /// The card's own figure: 8 recorded revisions of which 7 are changes is
    /// 0.875, and 0.875 is a nightly page.
    #[test]
    fn seven_changes_in_eight_revisions_is_noisy() {
        let kinds = ["changed"; 7];
        let mut kinds: Vec<&str> = kinds.to_vec();
        kinds.push("new");
        let noise = noise_of(&kinds, Some("2026-01-01T00:00:00.000000Z".into()));
        assert_eq!(noise["runs"], 8);
        assert_eq!(noise["changed"], 7);
        assert_eq!(noise["rate"], 0.875);
        assert_eq!(noise["verdict"], "noisy");
        assert_eq!(noise["last_changed_at"], "2026-01-01T00:00:00.000000Z");
    }

    /// **The anti-pattern this helper exists to refuse.** One `new` revision is
    /// a 100% change rate arithmetically, and reporting it would label every
    /// brand-new monitor noisy on the strength of its own creation.
    #[test]
    fn one_revision_is_unproven_not_a_hundred_percent() {
        for thin in [vec![], vec!["new"], vec!["new", "changed"]] {
            let noise = noise_of(&thin, None);
            assert_eq!(noise["verdict"], "unproven", "{thin:?}");
            assert!(noise["rate"].is_null(), "{thin:?} produced a rate");
            assert_eq!(noise["runs"], thin.len());
        }
    }

    #[test]
    fn a_page_that_mostly_sits_still_is_quiet() {
        let noise = noise_of(&["changed", "new", "new", "new"], None);
        assert_eq!(noise["rate"], 0.25);
        assert_eq!(noise["verdict"], "quiet");
        assert!(
            noise["last_changed_at"].is_null(),
            "honest null, never a date"
        );
    }

    /// `would_change` must agree with the store's predicate, including the two
    /// cases a naive body-comparison gets wrong: a cosmetic verdict HOLDS the
    /// baseline (so the narrowed records are equal and nothing is written), and
    /// a legacy record with no fingerprint gets a news verdict yet an identical
    /// body (so the store would still say `unchanged`).
    #[test]
    fn would_change_is_unchanged_when_only_the_computed_paths_moved() {
        let before = stored(OLD_DOC);
        let derived = sensitivity().derived_paths();

        assert_eq!(
            would_change(None, false, &before, &derived),
            ChangeKind::New,
            "no stored record is `new`"
        );

        let cosmetic = judge_page(Some(&before), pumper_core::simhash(REFLOWED_DOC));
        let held = page_record(
            "https://x.test/payments",
            REFLOWED_DOC,
            &hex_sha256(REFLOWED_DOC.as_bytes()),
            pumper_core::simhash(REFLOWED_DOC),
            &cosmetic,
            Some(&before),
        );
        assert_eq!(
            would_change(Some(&before), false, &held, &derived),
            ChangeKind::Unchanged,
            "a cosmetic run writes no revision, so the preview must not promise one"
        );

        let material = judge_page(Some(&before), pumper_core::simhash(NEW_DOC));
        let fresh = page_record(
            "https://x.test/payments",
            NEW_DOC,
            &hex_sha256(NEW_DOC.as_bytes()),
            pumper_core::simhash(NEW_DOC),
            &material,
            Some(&before),
        );
        assert_eq!(
            would_change(Some(&before), false, &fresh, &derived),
            ChangeKind::Changed
        );
        assert_eq!(
            would_change(Some(&before), true, &held, &derived),
            ChangeKind::Changed,
            "a revived tombstone is `changed` even when the content matches"
        );
    }

    /// A legacy record carries no fingerprint, so the verdict is the
    /// conservative first-sighting one — but the BODY has not moved, and the
    /// store would say so. A preview reporting `changed` here would invent the
    /// alert the sibling card just stopped firing.
    #[test]
    fn a_news_verdict_over_an_unmoved_body_is_still_unchanged() {
        let mut legacy = stored(OLD_DOC);
        legacy.as_object_mut().expect("object").remove(BODY_SIMHASH);
        let verdict = judge_page(Some(&legacy), pumper_core::simhash(OLD_DOC));
        assert!(verdict.is_news(), "fixture: {}", verdict.reason());
        let record = page_record(
            "https://x.test/payments",
            OLD_DOC,
            &hex_sha256(OLD_DOC.as_bytes()),
            pumper_core::simhash(OLD_DOC),
            &verdict,
            Some(&legacy),
        );
        assert_eq!(
            would_change(
                Some(&legacy),
                false,
                &record,
                &sensitivity().derived_paths()
            ),
            ChangeKind::Unchanged
        );
    }

    #[test]
    fn narrowing_removes_exactly_the_declared_paths() {
        let value = json!({ "a": 1, BODY_SIMHASH: 2, "materiality": { "significance": "x" } });
        assert_eq!(
            narrowed(&value, sensitivity().derived_paths().paths()),
            json!({ "a": 1 })
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
