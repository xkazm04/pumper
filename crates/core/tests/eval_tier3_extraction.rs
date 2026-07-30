//! Golden-fixture eval over the **tier-3 extraction path** — the Claude tier of
//! the tiered fetcher (`crates/core/src/fetcher.rs`), the surface where a page
//! the HTTP and browser tiers could not read gets turned into Markdown by the
//! model.
//!
//! Run it:
//!
//! ```text
//! cargo test -p pumper-core --test eval_tier3_extraction -- --nocapture
//! ```
//!
//! (also runs, quietly, as part of `cargo test --workspace`). Fully offline: no
//! network, no `claude` CLI, no `#[ignore]`.
//!
//! # What is real and what is not
//!
//! - **Fixtures are real.** `evals/tier3-extraction/fixtures/*.html` are live
//!   HTML captures of pages this repo actually targets — the source URLs in
//!   `catalog/data-sources.toml` and the connector docs pages watched by
//!   `connector-api-watch` (`catalog/connector-docs.json`). Each is the whole
//!   response body, frozen on the date in the manifest.
//! - **Transcripts are real.** `evals/tier3-extraction/transcripts/*.json` are
//!   verbatim `claude -p --output-format json` envelopes recorded on this
//!   machine against those frozen fixtures. They are not hand-authored. The
//!   recording prompt is the production tier-3 prompt with the frozen snapshot
//!   appended, because the recorder must not depend on the live web; the
//!   manifest records this.
//! - **Ground truth is checked, not asserted by fiat.** Every expected term must
//!   genuinely occur in the frozen page or the eval fails on the *fixture*, not
//!   the model — three invented terms were caught that way while this was built.
//! - **The free-path reference is recomputed every run** — the repo's own
//!   `html_to_markdown` over the frozen fixture, reported per case as
//!   `free-path`. It is both the length yardstick and a standing tier-discipline
//!   signal: a case where the deterministic converter already recovers the whole
//!   ground truth is a case that did not need to be bought from the model.
//!
//! # How it can fail
//!
//! 1. **The request changed** — [`ScriptedResearcher`] keys the transcript on
//!    the URL and errors if the prompt it is handed doesn't carry it. A tier-3
//!    prompt that stops naming the target page fails here.
//! 2. **The pipeline mangled the answer** — the invariant block asserts the
//!    `FetchOutcome` schema: engine, trace tier + verdict, `content_chars`
//!    agreeing with the text, Markdown populated, the model's `cost_usd`
//!    reaching both the outcome and the job's cost ledger.
//! 3. **Extraction quality regressed** — every case is scored and ratcheted
//!    against the baseline in the manifest. The score is not a non-emptiness
//!    check: it weighs term coverage against the deterministic reference,
//!    commentary-freeness, chrome-freeness and length sanity, and
//!    `adversarial_mutations_score_below_the_recording_not_the_same` proves the
//!    scorer separates the recorded answers from truncated / prefaced / refused
//!    / chrome-laden mutations of them.
//!
//! # Honest ceiling
//!
//! With replayed transcripts the eval grades *the pipeline and the scorer*, not
//! today's live model. It is the harness that makes a re-recording (new model,
//! new prompt, new effort level) immediately gradeable — the scorer is validated
//! by the adversarial cases, so a re-record that scores below baseline is a real
//! regression signal rather than an opinion.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::testing::{engines_with, ScriptedResearcher, TempStore, TestContext};
use pumper_core::{
    html_to_markdown, Browser, FetchRequest, FetchStrategy, FetchTier, HttpClient, HttpRequest,
    HttpResponse, RenderRequest, RenderedPage, ResearchOutput, Result, TierVerdict,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// The eval corpus
// ---------------------------------------------------------------------------

fn evals_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("evals")
        .join("tier3-extraction")
}

struct Case {
    id: String,
    url: String,
    /// Terms that genuinely occur in the frozen page (asserted), which a correct
    /// extraction of it must carry through.
    must_include: Vec<String>,
    /// Ratchet: the score measured when the transcript was recorded, floored to
    /// two decimals. A change that scores below it is a regression.
    baseline: f64,
    html: String,
    /// The verbatim `claude -p --output-format json` envelope.
    transcript: Value,
}

impl Case {
    fn reference_markdown(&self) -> String {
        html_to_markdown(&self.html)
    }

    /// The recorded answer, parsed exactly the way `ClaudeEngine` parses a live
    /// one — same envelope keys, so a change to the CLI contract shows up here.
    fn recorded_output(&self) -> ResearchOutput {
        let env = &self.transcript;
        assert_ne!(
            env["is_error"].as_bool(),
            Some(true),
            "{}: recorded transcript is an error envelope",
            self.id
        );
        ResearchOutput {
            text: env["result"]
                .as_str()
                .unwrap_or_else(|| panic!("{}: transcript has no `result`", self.id))
                .to_string(),
            json: None,
            cost_usd: env["total_cost_usd"].as_f64(),
            duration_ms: env["duration_ms"].as_u64(),
            num_turns: env["num_turns"].as_u64(),
            session_id: env["session_id"].as_str().map(String::from),
        }
    }
}

fn load_cases() -> (Value, Vec<Case>) {
    let dir = evals_dir();
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("manifest.json")).expect("read manifest.json"),
    )
    .expect("parse manifest.json");

    let cases = manifest["cases"]
        .as_array()
        .expect("manifest.cases is an array")
        .iter()
        .map(|c| {
            let id = c["id"].as_str().expect("case.id").to_string();
            Case {
                html: std::fs::read_to_string(dir.join("fixtures").join(format!("{id}.html")))
                    .unwrap_or_else(|e| panic!("read fixture {id}: {e}")),
                transcript: serde_json::from_str(
                    &std::fs::read_to_string(dir.join("transcripts").join(format!("{id}.json")))
                        .unwrap_or_else(|e| panic!("read transcript {id}: {e}")),
                )
                .unwrap_or_else(|e| panic!("parse transcript {id}: {e}")),
                url: c["url"].as_str().expect("case.url").to_string(),
                must_include: c["must_include"]
                    .as_array()
                    .expect("case.must_include")
                    .iter()
                    .map(|v| v.as_str().expect("must_include entry").to_string())
                    .collect(),
                baseline: c["baseline_score"].as_f64().expect("case.baseline_score"),
                id,
            }
        })
        .collect::<Vec<_>>();
    (manifest, cases)
}

// ---------------------------------------------------------------------------
// The scorer — the part that has to be able to say "worse"
// ---------------------------------------------------------------------------

/// Lead-ins an extraction must not carry: the tier-3 prompt says "only the
/// content, no commentary", and a preamble is what a downstream consumer stores
/// verbatim into a dataset.
const COMMENTARY_MARKERS: &[&str] = &[
    "here is the",
    "here's the",
    "below is the",
    "i've extracted",
    "i have extracted",
    "the page has no",
    "the page contains",
    "this page is",
    "i cannot",
    "i can't",
    "i'm unable",
    "i am unable",
    "as an ai",
    "sorry",
    "note:",
];

/// Navigation/consent boilerplate the extraction is supposed to drop.
const CHROME_MARKERS: &[&str] = &[
    "skip to main content",
    "skip to content",
    "accept all cookies",
    "enable javascript",
    "your browser is not supported",
    "this site requires javascript",
];

/// The offending lead-in when the answer opens with commentary rather than
/// content, else `None`. Only the opening matters — the same words deep inside a
/// long extraction are usually quoted page text.
fn commentary_preamble(text: &str) -> Option<String> {
    let opening: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .take(2)
        .collect();
    for line in opening {
        let lower = line.to_lowercase();
        if let Some(marker) = COMMENTARY_MARKERS.iter().find(|m| lower.starts_with(**m)) {
            return Some(format!("{marker:?} in {line:?}"));
        }
        // A whole answer wrapped in a fence is not "only the content" either.
        if line.starts_with("```") {
            return Some(format!("fenced answer: {line:?}"));
        }
    }
    None
}

/// Chrome markers that survived into the extraction.
fn surviving_chrome(text: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    CHROME_MARKERS
        .iter()
        .filter(|m| lower.contains(**m))
        .map(|m| (*m).to_string())
        .collect()
}

/// Fraction of the expected terms that survived into the extraction.
fn term_coverage(text: &str, must_include: &[String]) -> (f64, Vec<String>) {
    let lower = text.to_lowercase();
    let missing: Vec<String> = must_include
        .iter()
        .filter(|t| !lower.contains(&t.to_lowercase()))
        .cloned()
        .collect();
    if must_include.is_empty() {
        return (0.0, missing);
    }
    let hit = must_include.len() - missing.len();
    (hit as f64 / must_include.len() as f64, missing)
}

/// Extraction length against the deterministic reference. Tier 3 legitimately
/// produces less than the full Markdown (it drops chrome), so the band is wide —
/// it is there to catch a truncated answer and a runaway one, not to police
/// style.
///
/// `None` when the reference is empty. That is not a bug in the fixture: an
/// ASP.NET WebForms page puts its whole body inside `<form>`, which
/// `html_to_markdown` strips as chrome, so the free path yields nothing at all —
/// which is precisely when tier 3 earns its cost. With no reference there is no
/// ratio to band, and only [`MIN_EXTRACTION_CHARS`] can be asserted.
fn length_ratio(text: &str, reference: &str) -> Option<f64> {
    (!reference.is_empty()).then(|| text.chars().count() as f64 / reference.chars().count() as f64)
}

const LENGTH_BAND: std::ops::RangeInclusive<f64> = 0.02..=3.0;
/// Below this an "extraction" is a stub or a refusal, whatever the reference says.
const MIN_EXTRACTION_CHARS: usize = 400;

/// Whether the extraction's size is defensible: long enough to be content, and —
/// when a deterministic reference exists — within a wide band around it.
fn length_is_sane(text: &str, ratio: Option<f64>) -> bool {
    text.chars().count() >= MIN_EXTRACTION_CHARS && ratio.is_none_or(|r| LENGTH_BAND.contains(&r))
}

#[derive(Debug)]
struct Score {
    coverage: f64,
    missing: Vec<String>,
    preamble: Option<String>,
    chrome: Vec<String>,
    ratio: Option<f64>,
    /// Diagnostic: how much of the ground truth the FREE deterministic path
    /// already recovers. Low here and high in `coverage` is tier 3 earning its
    /// cost; high here is a candidate for dropping to the http tier.
    reference_coverage: f64,
    total: f64,
}

impl Score {
    fn line(&self, id: &str, baseline: f64) -> String {
        format!(
            "{:<18} score {:.2} (baseline {:.2})  coverage {:.2}  free-path {:.2}  len {}  {}{}",
            id,
            self.total,
            baseline,
            self.coverage,
            self.reference_coverage,
            self.ratio
                .map_or_else(|| "n/a".to_string(), |r| format!("x{r:.2}")),
            if self.preamble.is_some() {
                "COMMENTARY "
            } else {
                ""
            },
            if self.chrome.is_empty() {
                String::new()
            } else {
                format!("CHROME{:?}", self.chrome)
            },
        )
    }
}

/// Grades one extraction. Weighted so term coverage dominates (that is the job),
/// but a prefaced or truncated answer cannot reach a passing score on coverage
/// alone.
fn score_extraction(text: &str, reference: &str, must_include: &[String]) -> Score {
    let (coverage, missing) = term_coverage(text, must_include);
    let (reference_coverage, _) = term_coverage(reference, must_include);
    let preamble = commentary_preamble(text);
    let chrome = surviving_chrome(text);
    let ratio = length_ratio(text, reference);

    let total = 0.55 * coverage
        + 0.20 * f64::from(u8::from(preamble.is_none()))
        + 0.10 * f64::from(u8::from(chrome.is_empty()))
        + 0.15 * f64::from(u8::from(length_is_sane(text, ratio)));

    Score {
        coverage,
        missing,
        preamble,
        chrome,
        ratio,
        reference_coverage,
        total,
    }
}

// ---------------------------------------------------------------------------
// Offline engines that force the escalation to tier 3
// ---------------------------------------------------------------------------

/// The condition tier 3 exists for: the HTTP tier is bot-walled.
struct Walled;

#[async_trait]
impl HttpClient for Walled {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status: 403,
            headers: Default::default(),
            body: "<html><body>Access denied</body></html>".into(),
            final_url: req.url,
            cache_hit: false,
        })
    }
}

/// …and the browser tier gets a shell with no readable content.
struct Empty;

#[async_trait]
impl Browser for Empty {
    async fn render(&self, _: RenderRequest) -> Result<RenderedPage> {
        Ok(RenderedPage {
            html: "<html><body><div id=\"app\"></div></body></html>".into(),
            final_url: None,
            evaluated: None,
            nav_timed_out: false,
            selector_found: None,
            blocked_resources: 0,
            actions_completed: 0,
            network: Vec::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// The eval
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tier3_extraction_eval() {
    let (manifest, cases) = load_cases();
    assert!(
        cases.len() >= 10,
        "the eval corpus shrank to {} cases",
        cases.len()
    );

    let store = TempStore::new("eval-tier3").await;
    let mut scores: BTreeMap<String, (Score, f64)> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();

    for case in &cases {
        let reference = case.reference_markdown();
        let recorded = case.recorded_output();

        // Ground truth is the page, not an opinion: every expected term must
        // genuinely occur in the frozen fixture. Checked against the raw HTML
        // rather than the derived Markdown, because `html_to_markdown`
        // legitimately drops whole regions (a WebForms `<form>` body, for one)
        // and the question here is "is this really on the page", not "does the
        // free path already find it" — that one is scored, as `free-path`.
        let (_, missing_from_page) = term_coverage(&case.html, &case.must_include);
        assert!(
            missing_from_page.is_empty(),
            "{}: expected term(s) {:?} do not occur in the frozen page at all — the ground \
             truth is wrong, not the model",
            case.id,
            missing_from_page,
        );

        // Replay tier 3 through the REAL tiered fetcher and the REAL metered
        // AppContext seam.
        let engine = Arc::new(ScriptedResearcher::new().on(&case.url, recorded.clone()));
        let ctx = TestContext::new(&store.storage, "eval-tier3")
            .engines(engines_with(
                Arc::new(Walled),
                Arc::new(Empty),
                engine.clone(),
            ))
            .budget_usd(100.0)
            .build();

        let outcome = ctx
            .fetch(FetchRequest {
                strategy: FetchStrategy::AutoWithResearch,
                to_markdown: true,
                ..FetchRequest::new(&case.url)
            })
            .await
            .unwrap_or_else(|e| panic!("{}: tier-3 fetch failed: {e}", case.id));

        // --- invariants: the pipeline must not mangle the answer -------------
        assert_eq!(outcome.engine, "claude", "{}: wrong tier served", case.id);
        let claude_trace = outcome
            .trace
            .iter()
            .find(|t| t.tier == FetchTier::Claude)
            .unwrap_or_else(|| panic!("{}: no Claude tier in the trace", case.id));
        assert_eq!(claude_trace.verdict, TierVerdict::Ok, "{}", case.id);
        let text = outcome
            .text
            .as_deref()
            .unwrap_or_else(|| panic!("{}: tier 3 returned no text", case.id));
        assert_eq!(
            claude_trace.content_chars,
            Some(text.chars().count()),
            "{}: trace content_chars disagrees with the text it describes",
            case.id
        );
        assert_eq!(
            outcome.markdown.as_deref(),
            Some(text),
            "{}: to_markdown was requested but not honored",
            case.id
        );
        assert_eq!(
            outcome.cost_usd, recorded.cost_usd,
            "{}: the model's cost did not reach the outcome",
            case.id
        );
        assert_eq!(
            engine.call_count(),
            1,
            "{}: tier 3 called the model {} times",
            case.id,
            engine.call_count()
        );
        // The prompt actually named the page (the ScriptedResearcher would have
        // errored otherwise, but assert it rather than rely on the harness).
        assert!(
            engine.calls()[0].prompt.contains(&case.url),
            "{}: the tier-3 prompt no longer carries the target URL",
            case.id
        );
        // Spend is on the ledger, not just in the return value.
        let ledger = ctx.costs.job_total(ctx.job_id).await.unwrap();
        assert!(
            (ledger - recorded.cost_usd.unwrap_or(0.0)).abs() < 1e-9,
            "{}: metered {ledger} but the call cost {:?}",
            case.id,
            recorded.cost_usd
        );

        // --- quality ---------------------------------------------------------
        let score = score_extraction(text, &reference, &case.must_include);
        if score.total + 1e-9 < case.baseline {
            failures.push(format!(
                "{}: score {:.2} below baseline {:.2} (missing {:?}, preamble {:?}, chrome {:?}, \
                 len {:?})",
                case.id,
                score.total,
                case.baseline,
                score.missing,
                score.preamble,
                score.chrome,
                score.ratio,
            ));
        }
        scores.insert(case.id.clone(), (score, case.baseline));
    }

    // --- the summary ---------------------------------------------------------
    let mean = scores.values().map(|(s, _)| s.total).sum::<f64>() / scores.len() as f64;
    println!("\n=== tier-3 extraction eval ===");
    println!(
        "corpus: {} real frozen pages, transcripts recorded {} via {}",
        scores.len(),
        manifest["recorded"]["recorded_at"].as_str().unwrap_or("?"),
        manifest["recorded"]["command"].as_str().unwrap_or("?"),
    );
    for (id, (score, baseline)) in &scores {
        println!("  {}", score.line(id, *baseline));
    }
    println!("  {:<20} mean {:.3}", "ALL", mean);
    println!(
        "=== {} pass / {} fail ===\n",
        scores.len() - failures.len(),
        failures.len()
    );

    assert!(
        failures.is_empty(),
        "tier-3 extraction quality regressed:\n  {}",
        failures.join("\n  ")
    );
}

/// The scorer is only worth something if it can say "worse". Each mutation of a
/// recorded answer must score strictly below the answer it was derived from, and
/// trip the specific signal it was built to trip.
#[test]
fn adversarial_mutations_score_below_the_recording_not_the_same() {
    let (_, cases) = load_cases();
    let mut checked = 0;

    for case in &cases {
        let reference = case.reference_markdown();
        let good = case.recorded_output().text;
        let base = score_extraction(&good, &reference, &case.must_include);

        // 1. Truncated — the classic silent tier-3 regression.
        let truncated: String = good.chars().take(good.chars().count() / 100).collect();
        let s = score_extraction(&truncated, &reference, &case.must_include);
        assert!(
            s.total < base.total,
            "{}: truncating to 1% did not lower the score ({:.2} vs {:.2})",
            case.id,
            s.total,
            base.total
        );

        // 2. Prefaced with commentary.
        let prefaced = format!("Here is the extracted content:\n\n{good}");
        let s = score_extraction(&prefaced, &reference, &case.must_include);
        assert!(
            s.preamble.is_some(),
            "{}: a commentary preamble went undetected",
            case.id
        );
        // A recording that already opens with commentary cannot be made worse by
        // adding more of it — oh-grants-portal is that case, and it is a real
        // prompt-adherence miss rather than a fixture defect.
        if base.preamble.is_none() {
            assert!(s.total < base.total, "{}: preamble did not cost", case.id);
        }

        // 3. Refusal.
        let s = score_extraction(
            "I cannot access that page, so I am unable to extract its content.",
            &reference,
            &case.must_include,
        );
        assert!(
            s.total < 0.4,
            "{}: a refusal scored {:.2}",
            case.id,
            s.total
        );

        // 4. Chrome that should have been stripped.
        let chromed = format!("Skip to main content\n\n{good}\n\nAccept all cookies");
        let s = score_extraction(&chromed, &reference, &case.must_include);
        assert!(
            !s.chrome.is_empty() && s.total < base.total,
            "{}: surviving nav chrome did not cost",
            case.id
        );

        // 5. Empty.
        let s = score_extraction("", &reference, &case.must_include);
        assert!(
            s.total < 0.4,
            "{}: an empty answer scored {:.2}",
            case.id,
            s.total
        );

        checked += 1;
    }
    assert!(checked >= 10, "only {checked} cases exercised the scorer");
}

#[tokio::test]
async fn a_changed_tier3_prompt_fails_the_eval_not_passes_silently() {
    // The transcripts are keyed on the target URL. A prompt that stops naming
    // the page (a "cleanup" that drops the interpolation, a template rewrite) is
    // exactly the regression a replay harness must not absorb silently.
    let (_, cases) = load_cases();
    let case = &cases[0];
    let store = TempStore::new("eval-tier3-prompt").await;
    let engine = Arc::new(ScriptedResearcher::new().on(&case.url, case.recorded_output()));
    let ctx = TestContext::new(&store.storage, "eval-tier3")
        .engines(engines_with(Arc::new(Walled), Arc::new(Empty), engine))
        .budget_usd(100.0)
        .build();

    let err = ctx
        .fetch(FetchRequest {
            strategy: FetchStrategy::AutoWithResearch,
            to_markdown: true,
            // A prompt that forgot the page it is about.
            research_prompt: Some("Extract the main content as Markdown.".into()),
            ..FetchRequest::new(&case.url)
        })
        .await
        .expect_err("a prompt with no target URL must not match a recorded transcript");
    assert!(
        err.to_string().contains("no recorded transcript"),
        "unhelpful replay error: {err}"
    );
}
