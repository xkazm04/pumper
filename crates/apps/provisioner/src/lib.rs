//! M44 "speak a data source into existence" — v1: a PROPOSAL COMPILER.
//!
//! From one sentence ("track Czech senior Rust salaries weekly") the app:
//!   1. runs a research session (the same Claude engine seam `app-research`
//!      uses — schema-guarded, budget-clamped, cached) to identify 1–3
//!      candidate source URLs;
//!   2. samples each candidate through the tiered fetcher (the readable path);
//!   3. drafts a declarative [`RuleSet`] against the PRIMARY sampled body and
//!      DRY-RUNS it through the real extraction engine
//!      ([`pumper_core::extract_one_with_report`]) **against that same
//!      document**, iterating up to `max_iterations` until a strict majority of
//!      its fields hold and no degenerate-draft rejection fires. The other
//!      candidates are held-out generalization evidence, reported per candidate
//!      and never pooled into the accept bar;
//!   4. emits a complete **provision proposal** record into
//!      `provisioner/proposals`: `{catalog_row (TOML-shaped Source), rule_set,
//!      seeds, samples, cadence, budget, sample_stats, confidence, verdict}`.
//!
//! The app NEVER writes `catalog/data-sources.toml` and NEVER creates
//! schedules: the emitted catalog row is always `status = "planned"` with an
//! empty `cron`, so even a human pasting it verbatim into the catalog cannot
//! make the reconciler start anything — going live is a deliberate human edit.
//! That invariant is enforced by [`build_catalog_row`] and pinned by test.
//!
//! **The row is a claim, so every field of it must be true.** It reports the
//! fetch tier that actually served the sample ([`catalog_engine`]), an `access`
//! value from the documented vocabulary ([`catalog_access`]), confidence on the
//! catalog's documented 1–5 scale ([`confidence_1_to_5`], not this app's
//! internal 0–100 score), a `dataset` marked as unwritten ([`proposed_dataset`])
//! and a `notes` line naming the dry-run verdict ([`row_notes`]) — because
//! `catalog_toml` is the only artifact a reviewer may ever read, and it carries
//! no record fields at all.

use std::collections::BTreeMap;

use async_trait::async_trait;
use pumper_core::{
    extract_one_with_report, salvage_json, AppContext, AppManifest, CoercionStatus, CostClass,
    DocReport, Error, FetchOutcome, FetchRequest, FetchStrategy, ManifestExample, ResearchRequest,
    Result, Rule, RuleSet, ScrapeApp, Source,
};
use pumper_core::datasets::Provenance;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Provisioner;

/// At most this many candidate URLs are researched and sampled.
const MAX_CANDIDATES: usize = 3;

/// Default repair iterations for the draft → dry-run loop.
const DEFAULT_MAX_ITERATIONS: u32 = 2;

/// Hard cap on repair iterations regardless of params.
const ITERATIONS_CAP: u32 = 5;

/// Max chars of a sampled body inlined into the drafting prompt.
const SAMPLE_PROMPT_CAP_CHARS: usize = 12_000;

/// Max chars of the proposal-record key derived from the prompt.
const KEY_CAP_CHARS: usize = 64;

/// Default archive-tier freshness window for candidate SAMPLING (seconds, 24h).
///
/// Sampling exists to learn a page's SHAPE well enough to draft selectors, not
/// to capture its current values — and layouts move on the scale of months. So
/// a day-old web-archive snapshot is a fully adequate sample, and taking it
/// spares an unknown third-party host the first-contact hit from a compile that
/// may be re-run several times over one prompt. The archive tier is
/// opportunistic, never terminal: an absent, older, or thin snapshot falls
/// straight through to the live ladder. `0` opts out (live-only).
const DEFAULT_SAMPLE_ARCHIVE_MAX_AGE_SECS: u64 = 86_400;

// ── discovery ───────────────────────────────────────────────────────────────

/// One candidate source URL the research stage proposed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub url: String,
    pub name: String,
    /// Proposed refresh cadence in catalog vocabulary
    /// (`daily|weekly|monthly|quarterly|annual|on-demand`).
    pub cadence: String,
    /// Fields the source is expected to yield (drives the rule draft).
    pub expected_fields: Vec<String>,
}

/// Parses the discovery report `{candidates: [{url, name, cadence,
/// expected_fields}]}` defensively: entries without an http(s) URL are
/// dropped, duplicates (by URL) collapse, and the list is capped at
/// [`MAX_CANDIDATES`]. Anything unparseable yields an empty list — the caller
/// decides that is a loud failure, not this parser.
fn parse_candidates(v: &Value) -> Vec<Candidate> {
    let Some(items) = v.get("candidates").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<Candidate> = Vec::new();
    for item in items {
        let Some(url) = item.get("url").and_then(Value::as_str) else {
            continue;
        };
        let url = url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            continue;
        }
        if out.iter().any(|c| c.url == url) {
            continue;
        }
        out.push(Candidate {
            url: url.to_string(),
            name: item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(url)
                .trim()
                .to_string(),
            cadence: normalize_cadence(item.get("cadence").and_then(Value::as_str)),
            expected_fields: item
                .get("expected_fields")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default(),
        });
        if out.len() == MAX_CANDIDATES {
            break;
        }
    }
    out
}

/// Maps a free-text cadence onto the catalog vocabulary; unknown → "weekly"
/// (a conservative default the human reviewer sees and can change).
fn normalize_cadence(s: Option<&str>) -> String {
    match s.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some(c @ ("daily" | "weekly" | "monthly" | "quarterly" | "annual" | "on-demand")) => {
            c.to_string()
        }
        _ => "weekly".to_string(),
    }
}

// ── sampling ────────────────────────────────────────────────────────────────

/// What one sampled candidate actually produced — recorded in the proposal so a
/// reviewer can see *how* the sample was obtained, not just that it was.
#[derive(Debug, Clone, Serialize)]
pub struct SampleStat {
    pub url: String,
    /// The fetch tier that actually WON, straight from `FetchOutcome.engine`
    /// (`archive` · `api_recipe` · `http` · `browser` · `claude`).
    pub engine: String,
    /// Which `FetchOutcome` field carried the body (`html` · `markdown` ·
    /// `text`). A non-`html` sample means the rules were drafted against a
    /// flattened body and CSS selectors are guesswork — worth seeing.
    pub body_field: &'static str,
    /// Size of the sampled body in bytes.
    pub bytes: usize,
    /// One `tier:verdict` token per attempted tier, in ladder order.
    pub tiers: Vec<String>,
}

/// Picks the sampled body out of a [`FetchOutcome`] and says which field it
/// came from, or `None` when the fetch produced nothing usable.
///
/// THE ORDER IS LOAD-BEARING, and it is the bug this function exists to guard.
/// The http / browser / archive / recipe tiers return the body in `html` and
/// leave `text` empty; only the claude tier fills `text`. A sampler that reads
/// `text` first and never reads `html` therefore sees an EMPTY body on every
/// normal fetch, skips every candidate, and hard-errors — after the metered
/// discovery call has already been paid for.
///
/// `html` is also the field this app actually needs: the sample is drafted into
/// CSS/`each` rules and dry-run through [`extract_one_with_report`], and a
/// selector cannot bind against flattened Markdown or plain text. `markdown`
/// and `text` are honest fallbacks for the claude tier (which has no DOM to
/// give), never the preference.
pub fn select_sample_body(outcome: &mut FetchOutcome) -> Option<(&'static str, String)> {
    fn take(slot: &mut Option<String>) -> Option<String> {
        slot.take().filter(|b| !b.trim().is_empty())
    }
    if let Some(b) = take(&mut outcome.html) {
        return Some(("html", b));
    }
    if let Some(b) = take(&mut outcome.markdown) {
        return Some(("markdown", b));
    }
    if let Some(b) = take(&mut outcome.text) {
        return Some(("text", b));
    }
    None
}

/// Serializes a serde enum to its wire string (`FetchTier::Http` → `"http"`),
/// so the trace summary speaks the same vocabulary as the fetch trace itself.
fn wire_str<T: Serialize>(v: &T) -> String {
    serde_json::to_value(v)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "?".into())
}

/// Compacts a fetch's per-tier trace into `tier:verdict` tokens.
fn tier_summary(outcome: &FetchOutcome) -> Vec<String> {
    outcome
        .trace
        .iter()
        .map(|t| format!("{}:{}", wire_str(&t.tier), wire_str(&t.verdict)))
        .collect()
}

/// Maps the fetcher's winning tier onto the catalog's documented `engine`
/// vocabulary (`http` · `browser` · `claude` · `bulk`).
///
/// The archive and API-recipe tiers have no catalog spelling of their own, and
/// both are ordinary HTTP GETs from the point of view of a reviewer wiring an
/// app for this row — so they report `http`. `bulk` is a human judgement about a
/// downloadable dump; a page sample can never prove it, so this never emits it.
pub fn catalog_engine(fetch_engine: &str) -> &'static str {
    match fetch_engine {
        "browser" => "browser",
        "claude" => "claude",
        _ => "http",
    }
}

/// The artifact filename for a sample: only an `html` body earns the `.html`
/// extension — a markdown/text body saved as `.html` would misrepresent it.
fn sample_artifact_name(i: usize, body_field: &str) -> String {
    match body_field {
        "html" => format!("sample-{i}.html"),
        "markdown" => format!("sample-{i}.md"),
        _ => format!("sample-{i}.txt"),
    }
}

// ── degenerate-draft rejection ──────────────────────────────────────────────
//
// Three ways a draft can pass a match-rate bar while extracting nothing of
// value. All three are decidable from the extraction the dry run already ran,
// so they cost nothing and are known BEFORE any metered repair iteration is
// spent — and before an acceptance can stop the loop on a worthless draft.

/// True when EVERY top-level rule is a `const`.
///
/// `Rule::Const` always binds — it is a literal, not a selector — so a rule set
/// of nothing but constants scores a perfect 100 against any document,
/// including an empty one. It is the shortest path to passing this app's dry
/// run while extracting zero facts from the page.
pub fn const_only_rule_set(rules: &RuleSet) -> bool {
    !rules.fields.is_empty()
        && rules
            .fields
            .values()
            .all(|f| matches!(f.rule, Rule::Const { .. }))
}

/// `each` fields that yielded ZERO items on **every** document examined.
///
/// `FieldStatus::ContainerEmpty` deliberately does not count as a miss — a job
/// board with no postings this week is a working selector over a quiet listing,
/// and the health detector must not cry wolf over it. But that reasoning needs
/// a selector with a track record, and a draft has none: a listing rule that is
/// empty on every sample we have is precisely the selector never shown to work.
///
/// `items_per_doc` holds one map per document, `each`-field name → item count.
pub fn always_empty_each_fields(items_per_doc: &[BTreeMap<String, usize>]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let Some(first) = items_per_doc.first() else {
        return names;
    };
    for name in first.keys() {
        if items_per_doc
            .iter()
            .all(|doc| doc.get(name).copied().unwrap_or(0) == 0)
        {
            names.push(name.clone());
        }
    }
    names
}

/// Fields whose selector matched but whose transform chain then reduced the
/// value to nothing — [`CoercionStatus::CoercionFailed`], the wrong-element
/// signature (`to_number` over `"Add to cart"`).
///
/// The extraction engine has always computed this alongside the match status,
/// and the dry run never read it: such a field reported `Matched` and counted
/// as a working selector.
pub fn coercion_failed_fields(report: &DocReport) -> Vec<String> {
    report
        .coercion
        .iter()
        .filter(|(_, s)| **s == CoercionStatus::CoercionFailed)
        .map(|(name, _)| name.clone())
        .collect()
}

// ── dry-run harness ─────────────────────────────────────────────────────────

/// One field's outcome on the PRIMARY document.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FieldStat {
    /// The rule bound: `Matched`, or `ContainerEmpty` — a present-but-quiet
    /// listing is not a broken selector.
    pub bound: bool,
    /// Post-transform outcome (`coerced` · `coercion_failed` · `no_transforms`).
    pub coercion: String,
    /// `each` rules only: items yielded on this document.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<usize>,
}

impl FieldStat {
    /// A field "holds" when it bound AND its transforms did not reduce it to
    /// nothing. A matched selector over the wrong element is not a hold.
    fn holds(&self) -> bool {
        self.bound && self.coercion != "coercion_failed"
    }
}

/// A candidate document the draft was NOT written against.
pub struct HeldOutDoc<'a> {
    pub url: &'a str,
    pub body: &'a str,
}

/// What one held-out candidate says about the drafted rules.
///
/// Reported per candidate and never pooled into the accept bar: these are
/// DIFFERENT SITES with different markup, so a field missing here is evidence
/// about generalization, not evidence that the draft is wrong for the page it
/// was written against.
#[derive(Debug, Clone, Serialize)]
pub struct HeldOutStat {
    pub url: String,
    pub fields_held: usize,
    pub fields_total: usize,
    /// Fields that did not hold on this document, alphabetically.
    pub fields_missing: Vec<String>,
}

/// The dry-run verdict for one drafted rule set.
#[derive(Debug, Clone, Serialize)]
pub struct DryRun {
    /// Per-field outcome on the PRIMARY document — the one the draft was
    /// written against, and the only one the accept bar reads.
    pub stats: BTreeMap<String, FieldStat>,
    /// Total documents examined (primary + held out).
    pub docs: usize,
    pub fields_total: usize,
    pub fields_held: usize,
    /// The STOP RULE: a strict majority of fields hold **on the primary
    /// document**, and the draft is not degenerate.
    pub accepted: bool,
    /// Fields that failed to hold on the primary, alphabetically — the
    /// repair-prompt feedback.
    pub worst_fields: Vec<String>,
    /// Per-candidate generalization evidence. Never part of `accepted`.
    pub held_out: Vec<HeldOutStat>,
    /// Deterministic reasons the draft is unusable whatever it scored. Any
    /// entry here forces `accepted = false`.
    pub rejections: Vec<String>,
}

/// Runs a drafted rule set through the REAL extraction engine and scores it
/// **against its own document**.
///
/// The draft is written against `primary` and nothing else (see the drafting
/// prompt), so `primary` is what the accept bar reads. Scoring it as a pooled
/// majority over up to three candidates from DIFFERENT SITES made the bar move
/// with how many candidates happened to fetch: with 1 sample a primary-only
/// field passed, with 3 it needed `1*2 >= 3` and failed, so repair iterations
/// burned on a cross-site mismatch no selector can fix. The other candidates are
/// now held-out evidence, reported per candidate.
///
/// A rule set that doesn't compile is an `Err` — the loop feeds the compile
/// error back to the model as repair feedback rather than scoring it a zero.
pub fn dry_run(rules: &RuleSet, primary: &str, held_out: &[HeldOutDoc<'_>]) -> Result<DryRun> {
    if rules.fields.is_empty() {
        return Err(Error::App("drafted rule set has no fields".into()));
    }
    let compiled = rules.compile()?;

    let (primary_values, primary_report) = extract_one_with_report(&compiled, primary);
    let stats = field_stats(rules, &primary_values, &primary_report);

    // Item counts across EVERY document, so an `each` rule is only condemned
    // when it is empty on all the evidence we have.
    let mut items_per_doc = vec![each_item_counts(rules, &primary_values)];
    let mut held: Vec<HeldOutStat> = Vec::new();
    for doc in held_out {
        let (values, report) = extract_one_with_report(&compiled, doc.body);
        items_per_doc.push(each_item_counts(rules, &values));
        let s = field_stats(rules, &values, &report);
        let missing: Vec<String> = s
            .iter()
            .filter(|(_, st)| !st.holds())
            .map(|(name, _)| name.clone())
            .collect();
        held.push(HeldOutStat {
            url: doc.url.to_string(),
            fields_held: s.len() - missing.len(),
            fields_total: s.len(),
            fields_missing: missing,
        });
    }

    let mut rejections: Vec<String> = Vec::new();
    if const_only_rule_set(rules) {
        rejections.push(
            "every rule is a `const`: constants always bind, so this draft would score \
             perfectly while extracting nothing from the page"
                .into(),
        );
    }
    let empty_each = always_empty_each_fields(&items_per_doc);
    if !empty_each.is_empty() {
        rejections.push(format!(
            "`each` field(s) yielded 0 items on every sampled document: {}",
            empty_each.join(", ")
        ));
    }
    let miscoerced = coercion_failed_fields(&primary_report);
    if !miscoerced.is_empty() {
        rejections.push(format!(
            "field(s) matched an element whose value the transforms could not coerce \
             (wrong element): {}",
            miscoerced.join(", ")
        ));
    }

    let fields_total = stats.len();
    let fields_held = stats.values().filter(|s| s.holds()).count();
    // Strict majority of the PRIMARY document's fields, and nothing degenerate.
    let accepted = fields_held * 2 > fields_total && rejections.is_empty();
    let worst_fields: Vec<String> = stats
        .iter()
        .filter(|(_, s)| !s.holds())
        .map(|(name, _)| name.clone())
        .collect();
    Ok(DryRun {
        stats,
        docs: 1 + held_out.len(),
        fields_total,
        fields_held,
        accepted,
        worst_fields,
        held_out: held,
        rejections,
    })
}

/// Per-field outcome on one document, from the extraction the caller already ran.
fn field_stats(rules: &RuleSet, values: &Value, report: &DocReport) -> BTreeMap<String, FieldStat> {
    rules
        .fields
        .iter()
        .map(|(name, field)| {
            let stat = FieldStat {
                bound: report.fields.get(name).is_some_and(|s| !s.is_miss()),
                coercion: report
                    .coercion
                    .get(name)
                    .map(wire_str)
                    .unwrap_or_else(|| "no_transforms".into()),
                items: matches!(field.rule, Rule::Each { .. }).then(|| {
                    values
                        .get(name)
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len)
                }),
            };
            (name.clone(), stat)
        })
        .collect()
}

/// Item counts for the rule set's `each` fields on one extracted document.
fn each_item_counts(rules: &RuleSet, values: &Value) -> BTreeMap<String, usize> {
    rules
        .fields
        .iter()
        .filter(|(_, f)| matches!(f.rule, Rule::Each { .. }))
        .map(|(name, _)| {
            (
                name.clone(),
                values.get(name).and_then(Value::as_array).map_or(0, Vec::len),
            )
        })
        .collect()
}

/// Overall confidence 0–100: the share of the **primary document's** fields
/// that hold — bound, and survived their own transforms.
///
/// The old definition averaged (field × doc) cells across up to three different
/// SITES, so the number moved with how many candidates happened to fetch rather
/// than with how good the draft was for the page it was written against. A
/// degenerate draft scores 0 whatever its match rate: the rejection is the
/// finding, and a number beside it would only argue with it.
pub fn confidence(dry: &DryRun) -> u8 {
    if !dry.rejections.is_empty() || dry.fields_total == 0 {
        return 0;
    }
    ((dry.fields_held as f64 / dry.fields_total as f64) * 100.0).round() as u8
}

// ── proposal record ─────────────────────────────────────────────────────────

/// Maps this app's internal 0–100 score onto the catalog's **documented 1–5**
/// confidence scale (`docs/features/catalog.md`, ONBOARDING §10).
///
/// `Source.confidence` is an unvalidated `u8`, so writing the raw 0–100 score
/// into it produced rows claiming `confidence = 87` in a column whose entire
/// vocabulary is 1–5 — a value no hand-written row in
/// `catalog/data-sources.toml` can be compared against, and one that reads as
/// "wildly more trustworthy than anything else here" to a human skimming.
///
/// The floor is 1, never 0: 0 is not a point on the scale. A proposal that
/// bound nothing is *least* trustworthy, not *unrated*.
pub fn confidence_1_to_5(score_0_100: u8) -> u8 {
    match score_0_100.min(100) {
        0..=19 => 1,
        20..=39 => 2,
        40..=59 => 3,
        60..=79 => 4,
        _ => 5,
    }
}

/// The catalog `access` value for a proposed source.
///
/// The documented vocabulary is `key-free · api-key · bulk · scrape`; the app
/// used to emit `"public"`, which is in none of it. Everything this app can
/// produce is by construction a web page it sampled and drafted CSS/`each`
/// rules against — that is `scrape`, whatever the page's licensing. A `key-free`
/// or `api-key` API and a `bulk` dump are human judgements about a *mechanism*
/// this app never established, so it must not claim them.
pub fn catalog_access() -> &'static str {
    "scrape"
}

/// The `dataset` value for a proposed row.
///
/// The app used to write the bare proposal key here, naming a dataset that
/// nothing writes and nothing will write until a human builds the app crate —
/// indistinguishable, in the catalog, from a live source's real dataset. The
/// `proposed:` prefix names the *intent* while being unmistakably not a
/// resolvable dataset name (real ones are `<app>/<name>`).
pub fn proposed_dataset(key: &str) -> String {
    format!("proposed:{key}")
}

/// The row's `notes`, carrying the dry-run verdict in plain words.
///
/// A rejected proposal used to be emitted byte-identically to an accepted one:
/// same `status`, same shape, the verdict living only in the record's
/// `accepted` boolean — which `catalog_toml`, the fragment a human actually
/// pastes, drops entirely. So the reviewer could not tell a rule set that bound
/// everything from one that bound nothing.
pub fn row_notes(prompt: &str, dry: &DryRun, score_0_100: u8) -> String {
    let verdict = if dry.accepted { "ACCEPTED" } else { "REJECTED" };
    let why = if dry.rejections.is_empty() {
        String::new()
    } else {
        format!(" DEGENERATE DRAFT: {}.", dry.rejections.join("; "))
    };
    format!(
        "provisioner proposal — dry run {verdict} ({}/{} fields held on the primary sampled \
         document, {} held-out candidate(s) examined; score {score_0_100}/100 = confidence \
         {}/5).{why} UNPROVISIONED: no app crate, no dataset, nothing scheduled. \
         Prompt: {prompt}",
        dry.fields_held,
        dry.fields_total,
        dry.held_out.len(),
        confidence_1_to_5(score_0_100),
    )
}

/// Stable, readable proposal key from the prompt: lowercase alnum runs joined
/// by dashes, capped. Same prompt → same key, so re-compiles upsert (change
/// detection sees a revised proposal, not a pile of near-duplicates).
pub fn proposal_key(prompt: &str) -> String {
    let mut out = String::new();
    let mut dash = false;
    for c in prompt.chars() {
        if out.len() >= KEY_CAP_CHARS {
            break;
        }
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            dash = false;
        } else if !dash && !out.is_empty() {
            out.push('-');
            dash = true;
        }
    }
    let out = out.trim_end_matches('-').to_string();
    if out.is_empty() {
        "proposal".to_string()
    } else {
        out
    }
}

/// The TOML-shaped catalog row for the proposal — the exact `[[source]]`
/// field set ([`pumper_core::Source`]), so a human can paste it into
/// `catalog/data-sources.toml` after review.
///
/// INVARIANT (the never-provisions rule): `status` is ALWAYS `"planned"` and
/// `cron` is ALWAYS empty, whatever the proposed cadence — the reconciler only
/// schedules `live` sources with a cron, so nothing this app emits can start a
/// pipeline. Going live is a human edit, never machine output.
///
/// `engine` is the tier that ACTUALLY served the primary sample, mapped through
/// [`catalog_engine`] — not a hardcoded guess. A row that says `http` for a page
/// only the browser tier could render sends the reviewer down a path we already
/// know fails.
///
/// The row also carries the DRY-RUN VERDICT, because the pasteable TOML
/// fragment is the only artifact a reviewer may ever look at: `confidence` on
/// the documented 1–5 scale ([`confidence_1_to_5`]) and an accepted/rejected
/// `notes` line ([`row_notes`]). `score_0_100` is this app's own score; `conf`
/// on the row is its 1–5 projection.
pub fn build_catalog_row(
    prompt: &str,
    primary: &Candidate,
    dry: &DryRun,
    score_0_100: u8,
    fetch_engine: &str,
) -> Source {
    let key = proposal_key(prompt);
    Source {
        id: key.clone(),
        // No app crate exists for a proposed source; the reviewer wires one
        // (typically crawl + extractor with the proposed rule set).
        app: String::new(),
        market: String::new(),
        name: primary.name.clone(),
        url: primary.url.clone(),
        category: String::new(),
        engine: catalog_engine(fetch_engine).into(),
        access: catalog_access().into(),
        cadence: primary.cadence.clone(),
        cron: String::new(),      // never scheduled by this app
        status: "planned".into(), // never live from machine output
        confidence: confidence_1_to_5(score_0_100),
        dataset: proposed_dataset(&key),
        notes: row_notes(prompt, dry, score_0_100),
        contract: None, // contracts are human-declared, never machine-proposed
    }
}

/// Renders the catalog row as a paste-ready `[[source]]` TOML fragment.
///
/// `pub` (not just crate-internal) because the promotion route
/// (`POST /provisioner/proposals/{key}/promote`) re-renders this same fragment
/// server-side from the stored `catalog_row` rather than trusting the
/// `catalog_toml` string a proposal happened to carry at compile time — one
/// renderer, so the two can never drift.
pub fn catalog_toml(row: &Source) -> String {
    #[derive(Serialize)]
    struct Frag<'a> {
        source: [&'a Source; 1],
    }
    toml::to_string(&Frag { source: [row] }).unwrap_or_default()
}

// ── proposal lifecycle ──────────────────────────────────────────────────────
//
// A proposal record's `status` is the LIFECYCLE state a human (or the
// validate/promote routes) drives it through — distinct from `accepted` /
// `verdict`, which are the frozen compile-time dry-run verdict and never
// change after `run()` writes the record.
//
//   planned -> validated | failed -> promoted
//
// `planned` is every proposal's starting state, whatever its compile-time
// verdict: a REJECTED proposal is still emitted (the misses are the useful
// part) and still starts `planned`, so [`may_promote`] is what actually stops
// a proposal that never demonstrated it binds anything from being promoted —
// not the status value alone.

/// The record's status immediately after `run()` writes it — nothing has
/// validated or promoted it yet, whatever its compile-time verdict.
pub const STATUS_PLANNED: &str = "planned";
/// `POST .../validate` re-ran the dry run against a fresh sample and it held.
pub const STATUS_VALIDATED: &str = "validated";
/// `POST .../validate` re-ran the dry run against a fresh sample and it did not.
pub const STATUS_FAILED: &str = "failed";
/// `POST .../promote` emitted the catalog-row TOML fragment for this proposal.
pub const STATUS_PROMOTED: &str = "promoted";

/// Whether a proposal in `status`, with the ORIGINAL compile-time verdict
/// `accepted`, may be promoted.
///
/// The catalog row is a claim ("this rule set binds this page"), so promoting
/// one whose best available evidence says it does NOT bind would hand a
/// reviewer a paste-ready fragment for a draft already known not to work:
///   - `failed`: the LATEST evidence (a fresh re-validation) says no — never
///     promotable until it validates clean.
///   - `planned` (never validated): the only evidence is the original
///     compile-time verdict, so it gates directly on `accepted`.
///   - `validated` / `promoted`: the latest evidence says yes; `promoted` stays
///     promotable so re-promoting (e.g. to re-render the fragment) is not an
///     error, just a repeat of an already-cleared gate.
pub fn may_promote(status: &str, accepted: bool) -> bool {
    match status {
        STATUS_FAILED => false,
        STATUS_PLANNED => accepted,
        STATUS_VALIDATED | STATUS_PROMOTED => true,
        _ => false,
    }
}

/// Whether a `planned` proposal aged `age_secs` past `max_age_secs` counts as
/// expired.
///
/// Gated to `planned` on purpose: expiry names proposals ROTTING while waiting
/// for a human to look at them. A `validated` or `promoted` proposal already
/// had that attention (and a `failed` one has its own loud signal); re-flagging
/// it as "expired" merely because it sat in the store past the window would
/// bury the actually-neglected proposals in noise. `max_age_secs == 0` opts
/// out (nothing ever expires).
pub fn proposal_is_expired(status: &str, age_secs: i64, max_age_secs: i64) -> bool {
    status == STATUS_PLANNED && max_age_secs > 0 && age_secs > max_age_secs
}

/// Re-runs a compiled proposal's `RuleSet` against a FRESHLY fetched sample —
/// the validate route's core: catch drift the original compile could not have
/// seen. Takes an already-fetched [`FetchOutcome`] rather than fetching itself
/// so this crate keeps depending only on `core` (the dependency rule in
/// README.md §Architecture) — the caller (the server route) owns the actual
/// network call, exactly as `run()`'s own sampling stage does the fetch and
/// hands this crate's pure helpers the outcome.
///
/// Scored with NO held-out documents: validation re-checks the one proposal it
/// was asked to validate, not generalization across candidates — that
/// evidence was already captured (and is not re-fetched) at compile time.
pub fn validate_sample(mut outcome: FetchOutcome, rules: &RuleSet) -> Result<(SampleStat, DryRun)> {
    let url = outcome.url.clone();
    let engine = outcome.engine.to_string();
    let tiers = tier_summary(&outcome);
    let Some((body_field, body)) = select_sample_body(&mut outcome) else {
        return Err(Error::App(format!(
            "validate: fresh sample of {url} yielded no body on any field"
        )));
    };
    let sample = SampleStat {
        url,
        engine,
        body_field,
        bytes: body.len(),
        tiers,
    };
    let dry = dry_run(rules, &body, &[])?;
    Ok((sample, dry))
}

/// Assembles the full proposal record written to `provisioner/proposals`.
#[allow(clippy::too_many_arguments)]
pub fn build_proposal(
    prompt: &str,
    row: &Source,
    rule_set: &Value,
    seeds: &[String],
    samples: &[SampleStat],
    budget_usd: Option<f64>,
    dry: &DryRun,
    iterations: u32,
    cost_usd: f64,
) -> Value {
    json!({
        "prompt": prompt,
        "catalog_row": serde_json::to_value(row).unwrap_or(Value::Null),
        "catalog_toml": catalog_toml(row),
        "rule_set": rule_set,
        "seeds": seeds,
        // How each seed was actually obtained: winning tier, body field, byte
        // count, per-tier trace. The reviewer's evidence that the rules were
        // drafted against a real page and not an empty string.
        "samples": samples,
        "cadence": row.cadence,
        "budget": budget_usd,
        "sample_stats": serde_json::to_value(dry).unwrap_or(Value::Null),
        "confidence": confidence(dry),
        // The two confidence scales, both named, so neither can be mistaken for
        // the other: this app scores 0–100, the catalog column is 1–5.
        "confidence_scale": "0-100; catalog_row.confidence is the same judgement \
                             on the catalog's documented 1-5 scale",
        "catalog_confidence": row.confidence,
        "accepted": dry.accepted,
        // A rejected proposal is emitted (the drafts and the misses are the
        // useful part), but it must never be mistakable for an accepted one.
        "verdict": if dry.accepted { "accepted" } else { "rejected" },
        "provisioned": false,
        // Lifecycle state (planned -> validated|failed -> promoted), driven by
        // the provisioner routes — see the "proposal lifecycle" section above.
        // Every proposal starts here regardless of `verdict`: `may_promote`,
        // not `status` alone, is what keeps a rejected draft from promoting.
        "status": STATUS_PLANNED,
        "intended_dataset": row.dataset,
        "iterations": iterations,
        "cost_usd": cost_usd,
    })
}

/// Truncates a sampled body for the drafting prompt (char-boundary safe).
fn excerpt(body: &str, max: usize) -> String {
    if body.chars().count() <= max {
        return body.to_string();
    }
    body.chars().take(max).collect()
}

// ── the app ─────────────────────────────────────────────────────────────────

// ── durable execution ───────────────────────────────────────────────────────

/// The resumable unit of a compile (M23): everything the METERED discovery
/// stage produced. Stages after it (sampling, drafting) are free-tier or
/// re-derivable, and their inputs (page bodies) are far larger than the state
/// they would save — so the checkpoint stops here deliberately.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryCheckpoint {
    /// The prompt's proposal key. A checkpoint is only honored for a run
    /// compiling the SAME prompt — restoring another prompt's candidates would
    /// silently compile the wrong source.
    proposal_key: String,
    candidates: Vec<Candidate>,
    /// Research session to resume, so the drafting calls keep the discovery
    /// context instead of paying to rebuild it.
    #[serde(default)]
    session_id: Option<String>,
}

impl DiscoveryCheckpoint {
    /// Best-effort persist — a checkpoint that fails to write costs a repeated
    /// research call on re-claim, never the run.
    async fn save(&self, ctx: &AppContext) {
        match serde_json::to_value(self) {
            Ok(v) => {
                ctx.checkpoint_now(v).await;
            }
            Err(e) => tracing::warn!("provisioner checkpoint serialize failed: {e}"),
        }
    }

    /// Restores a checkpoint for THIS prompt, or `None` (fresh run, foreign
    /// prompt, or an unreadable blob — all of which simply re-run discovery).
    fn restore(ctx: &AppContext, proposal_key: &str) -> Option<Self> {
        Self::from_blob(ctx.restore(), proposal_key)
    }

    /// The restore decision, pure so it is testable without a runtime: a blob
    /// is honored only when it parses AND belongs to this prompt AND actually
    /// carries candidates.
    fn from_blob(blob: Option<&Value>, proposal_key: &str) -> Option<Self> {
        let cp: Self = serde_json::from_value(blob?.clone()).ok()?;
        (cp.proposal_key == proposal_key && !cp.candidates.is_empty()).then_some(cp)
    }
}

#[async_trait]
impl ScrapeApp for Provisioner {
    fn name(&self) -> &'static str {
        "provisioner"
    }

    fn description(&self) -> &'static str {
        "Compile a natural-language prompt into a reviewed provisioning PROPOSAL: \
         research 1-3 candidate source URLs, sample them via the tiered fetcher, \
         draft a declarative rule set and dry-run it through the real extraction \
         engine against the page it was written for (iterating until a majority of \
         that page's fields hold; the other candidates are held-out evidence, and \
         const-only / always-empty-`each` / coercion-failed drafts are rejected \
         outright), then emit a \
         {catalog_row, rule_set, seeds, samples, cadence, budget, sample_stats, \
         confidence, verdict} \
         record into provisioner/proposals. NEVER writes data-sources.toml or \
         creates schedules — the emitted row is always status=planned with no cron; \
         a human applies it via the catalog reconciler. The metered discovery \
         stage is checkpointed, so a reaped/suspended compile resumes without \
         re-spending it. Params: {\"prompt\": \"...\", \"budget_usd\": 1.0, \
         \"max_iterations\": 2, \"sample_archive_max_age\": 86400}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["prompt"],
                "properties": {
                    "prompt": { "type": "string", "minLength": 1 },
                    "budget_usd": { "type": "number", "minimum": 0 },
                    "max_iterations": { "type": "integer", "minimum": 1, "maximum": 5 },
                    "sample_archive_max_age": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Archive-tier freshness window (seconds) for candidate \
                                        SAMPLING only; a snapshot no older than this may serve \
                                        the shape-learning fetch instead of hitting the origin. \
                                        Default 86400, 0 = live-only."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Compile a source proposal from one sentence, with a spend ceiling",
                    params: json!({
                        "prompt": "track Czech senior Rust developer salary listings weekly",
                        "budget_usd": 1.0
                    }),
                },
                ManifestExample {
                    description: "Allow an extra repair iteration for a tricky source",
                    params: json!({
                        "prompt": "monitor US federal fuel-price weekly averages",
                        "budget_usd": 2.0,
                        "max_iterations": 3
                    }),
                },
            ],
            output_shape: Some(
                "{proposal_key, candidates, seeds, samples, iterations, accepted, verdict, \
                 rejections, confidence, catalog_confidence, sample_stats, cost_usd, \
                 resumed_discovery} — `sample_stats` scores the draft against the PRIMARY \
                 sampled document and reports the other candidates under `held_out`, per \
                 candidate, never pooled; `rejections` names any degenerate-draft finding \
                 (const-only rule set, `each` field empty on every document, coercion-failed \
                 field) that vetoes acceptance whatever the match rate; \
                 `samples` records, per seed, the fetch tier that actually served it, which \
                 body field carried it, its byte count and the per-tier trace; `confidence` \
                 is 0-100 and `catalog_confidence` its 1-5 catalog-scale projection; \
                 a REJECTED dry run still emits a record, marked as such in \
                 `verdict` and in the catalog row's own notes; the full proposal record is \
                 upserted into provisioner/proposals (stamped with the primary sampled URL \
                 as its source) and saved as the proposal.json artifact; \
                 nothing is written to the catalog and no schedule is created",
            ),
            cost_class: CostClass::Claude,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let prompt = ctx.require_str("prompt")?.to_string();
        let budget_usd = ctx.params.get("budget_usd").and_then(Value::as_f64);
        let max_iterations = ctx
            .params
            .get("max_iterations")
            .and_then(Value::as_u64)
            .map(|n| (n as u32).clamp(1, ITERATIONS_CAP))
            .unwrap_or(DEFAULT_MAX_ITERATIONS);
        let sample_archive_max_age = ctx
            .params
            .get("sample_archive_max_age")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_SAMPLE_ARCHIVE_MAX_AGE_SECS);
        let mut cost_usd = 0.0f64;

        // Durable execution (M23): discovery is the one irreversibly METERED
        // stage of a compile — a reap or shutdown between it and the drafting
        // loop used to re-spend that research call from scratch. Its outcome
        // (the candidate list + the resumable research session) is checkpointed
        // and restored on re-claim; everything after it is free-tier work that
        // is cheaper to redo than to persist.
        let key = proposal_key(&prompt);
        let resumed = DiscoveryCheckpoint::restore(&ctx, &key);
        let resumed_discovery = resumed.is_some();

        // ── stage 1: discover candidate sources ─────────────────────────────
        let (mut session_id, candidates) = if let Some(cp) = resumed {
            tracing::info!(
                job = %ctx.job_id,
                candidates = cp.candidates.len(),
                "provisioner resumed from checkpoint: discovery research not re-spent"
            );
            (cp.session_id, cp.candidates)
        } else {
            let discovery_schema = json!({
            "type": "object",
            "required": ["candidates"],
            "properties": {
                "candidates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["url"],
                        "properties": {
                            "url": { "type": "string" },
                            "name": { "type": "string" },
                            "cadence": { "type": "string" },
                            "expected_fields": {
                                "type": "array", "items": { "type": "string" }
                            }
                        }
                    }
                }
            }
        });
        let mut discover = ResearchRequest::new(format!(
            "You are provisioning a web data source. For the goal below, identify \
             the 1-3 BEST public web pages that carry the data (listing/index pages \
             preferred over articles), what fields each yields, and how often the \
             data changes.\n\nGoal: {prompt}\n\nRespond with ONLY a JSON object: \
             {{\"candidates\": [{{\"url\": string, \"name\": string, \
             \"cadence\": \"daily|weekly|monthly|quarterly|annual|on-demand\", \
             \"expected_fields\": string[]}}]}}"
        ))
        .with_role("research");
        discover.max_budget_usd = budget_usd;
        discover.json_schema = Some(discovery_schema);
            let out = ctx.research(discover).await?;
            cost_usd += out.cost_usd.unwrap_or(0.0);
            let session_id = out.session_id.clone();
            let report = out
                .json
                .clone()
                .or_else(|| salvage_json(&out.text))
                .unwrap_or(Value::Null);
            let candidates = parse_candidates(&report);
            if candidates.is_empty() {
                return Err(Error::App(format!(
                    "provisioner: research produced no usable candidate URLs for prompt {prompt:?}"
                )));
            }
            // Checkpoint the metered stage's outcome BEFORE any further work.
            DiscoveryCheckpoint {
                proposal_key: key.clone(),
                candidates: candidates.clone(),
                session_id: session_id.clone(),
            }
            .save(&ctx)
            .await;
            (session_id, candidates)
        };

        // ── stage 2: sample candidates via the tiered fetcher ───────────────
        let mut seeds: Vec<String> = Vec::new();
        let mut bodies: Vec<String> = Vec::new();
        let mut samples: Vec<SampleStat> = Vec::new();
        for (i, cand) in candidates.iter().enumerate() {
            let mut req = FetchRequest::new(&cand.url);
            req.strategy = FetchStrategy::Auto;
            // The claude tier has no DOM to return; asking for Markdown is the
            // only way its answer reaches `select_sample_body`'s fallback at
            // all. Costs nothing on the html-bearing tiers.
            req.to_markdown = true;
            // Sampling is shape-learning against an UNKNOWN third-party host —
            // exactly the case both cheap seams were built for: a learned API
            // recipe (M05) can replace a heavy render outright, and a recent
            // archive snapshot (M18) is an adequate shape sample. Both are
            // opportunistic: neither can terminate the ladder, so a miss simply
            // fetches live as before.
            req.use_recipes = true;
            req.archive_max_age = (sample_archive_max_age > 0).then_some(sample_archive_max_age);
            match ctx.fetch(req).await {
                Ok(mut outcome) => {
                    let tiers = tier_summary(&outcome);
                    let engine = outcome.engine.to_string();
                    let Some((body_field, body)) = select_sample_body(&mut outcome) else {
                        tracing::warn!(
                            url = %cand.url,
                            engine = %engine,
                            "provisioner sample yielded no body on any field"
                        );
                        continue;
                    };
                    ctx.save_artifact(&sample_artifact_name(i, body_field), body.as_bytes())
                        .await?;
                    samples.push(SampleStat {
                        url: cand.url.clone(),
                        engine,
                        body_field,
                        bytes: body.len(),
                        tiers,
                    });
                    seeds.push(cand.url.clone());
                    bodies.push(body);
                }
                Err(e) => {
                    tracing::warn!(url = %cand.url, "provisioner sample fetch failed: {e}");
                }
            }
        }
        if bodies.is_empty() {
            return Err(Error::App(
                "provisioner: no candidate URL yielded a sampleable body".into(),
            ));
        }
        let primary = candidates
            .iter()
            .find(|c| Some(&c.url) == seeds.first())
            .unwrap_or(&candidates[0]);
        let expected = if primary.expected_fields.is_empty() {
            "fields you judge most valuable for the goal".to_string()
        } else {
            primary.expected_fields.join(", ")
        };

        // ── stage 3: draft rules and dry-run through the real engine ────────
        let rules_shape = "Respond with ONLY a JSON object mapping field names to \
             extraction rules. Rule shapes: {\"type\":\"css\",\"selector\":\"h1\"}, \
             {\"type\":\"css\",\"selector\":\"a\",\"attr\":\"href\"}, \
             {\"type\":\"regex\",\"pattern\":\"...\",\"group\":1}, \
             {\"type\":\"each\",\"selector\":\".card\",\"container\":\"#list\",\
             \"fields\":{...css/regex/const rules...}} (use `each` for repeating \
             listings), optional \"transforms\":[{\"op\":\"trim\"}|{\"op\":\"to_number\"}].";
        let mut last_rules_value: Option<Value> = None;
        let mut last_dry: Option<DryRun> = None;
        let mut feedback: Option<String> = None;
        let mut iterations = 0u32;
        while iterations < max_iterations {
            // Stop gracefully at the budget ceiling between metered calls; the
            // first call keeps the seam's own refusal behavior.
            if iterations > 0 {
                if let Some(remaining) = ctx.remaining_budget_usd().await? {
                    if remaining <= 0.0 {
                        break;
                    }
                }
            }
            iterations += 1;
            let draft_prompt = match &feedback {
                None => format!(
                    "Draft extraction rules for this goal: {prompt}\n\
                     Target fields: {expected}\n\n{rules_shape}\n\n\
                     Sampled page body from {url} (truncated):\n\n{body}",
                    url = primary.url,
                    body = excerpt(&bodies[0], SAMPLE_PROMPT_CAP_CHARS),
                ),
                Some(fb) => format!(
                    "Your drafted rules failed the dry run: {fb}\n\
                     Revise the rule set (same JSON shape, full object, all fields)."
                ),
            };
            let mut req = ResearchRequest::new(draft_prompt).with_role("research");
            req.max_budget_usd = budget_usd;
            req.resume_session = session_id.clone();
            let out = ctx.research(req).await?;
            cost_usd += out.cost_usd.unwrap_or(0.0);
            if out.session_id.is_some() {
                session_id = out.session_id.clone();
            }
            let Some(draft) = out.json.clone().or_else(|| salvage_json(&out.text)) else {
                feedback = Some("response was not a JSON object of rules".into());
                continue;
            };
            let rules: RuleSet = match serde_json::from_value(draft.clone()) {
                Ok(r) => r,
                Err(e) => {
                    feedback = Some(format!("rules did not parse: {e}"));
                    continue;
                }
            };
            // The draft is written against `bodies[0]`, so that is what it is
            // scored against; the other candidates are held-out evidence.
            let held: Vec<HeldOutDoc<'_>> = bodies
                .iter()
                .zip(seeds.iter())
                .skip(1)
                .map(|(body, url)| HeldOutDoc { url, body })
                .collect();
            match dry_run(&rules, &bodies[0], &held) {
                Ok(dry) => {
                    let accepted = dry.accepted;
                    feedback = (!accepted).then(|| {
                        // A degenerate draft gets its own reason, not a score:
                        // "3/4 fields matched" would be actively misleading
                        // feedback for a rule set of nothing but constants.
                        if !dry.rejections.is_empty() {
                            format!(
                                "the draft is unusable regardless of its match rate: {}",
                                dry.rejections.join("; ")
                            )
                        } else {
                            format!(
                                "{}/{} fields held on the page the draft was written against \
                                 ({}); failing fields: {}",
                                dry.fields_held,
                                dry.fields_total,
                                primary.url,
                                dry.worst_fields.join(", ")
                            )
                        }
                    });
                    last_rules_value = Some(draft);
                    last_dry = Some(dry);
                    if accepted {
                        break;
                    }
                }
                Err(e) => {
                    feedback = Some(format!("rules failed to compile: {e}"));
                    last_rules_value = Some(draft);
                }
            }
        }
        let (rules_value, dry) = match (last_rules_value, last_dry) {
            (Some(r), Some(d)) => (r, d),
            _ => {
                return Err(Error::App(format!(
                    "provisioner: no runnable rule set after {iterations} iteration(s) \
                     (last feedback: {})",
                    feedback.as_deref().unwrap_or("none")
                )))
            }
        };

        // ── stage 4: emit the proposal (and ONLY the proposal) ──────────────
        let conf = confidence(&dry);
        // The primary sample is `samples[0]` by construction: `samples`,
        // `seeds` and `bodies` are pushed together, and `primary` is resolved
        // from `seeds.first()`.
        let primary_engine = samples
            .first()
            .map(|s| s.engine.as_str())
            .unwrap_or("http")
            .to_string();
        let row = build_catalog_row(&prompt, primary, &dry, conf, &primary_engine);
        let proposal = build_proposal(
            &prompt,
            &row,
            &rules_value,
            &seeds,
            &samples,
            budget_usd,
            &dry,
            iterations,
            cost_usd,
        );
        // Provenance (M12): a proposal is derived from ONE page — the primary
        // candidate whose sampled body the rules were drafted and dry-run
        // against — so that URL is a real per-record fact, not a guess. The
        // other sampled seeds are listed inside the record itself.
        //
        // `rules_hash` stays None on purpose: the drafted RuleSet is this
        // record's PAYLOAD, not the thing that extracted it. Stamping it would
        // claim a derivation that never happened (and the field is what the
        // replay path resolves against). `artifact_sha` likewise — the sample
        // artifacts are the inputs, not an archived copy of this record.
        let prov = Provenance {
            source_url: Some(primary.url.clone()),
            ..Provenance::default()
        };
        let change = ctx
            .upsert_with_provenance("proposals", &key, &proposal, prov)
            .await?;
        ctx.save_artifact("proposal.json", &serde_json::to_vec_pretty(&proposal)?)
            .await?;

        Ok(json!({
            "proposal_key": key,
            "change": format!("{change:?}"),
            "candidates": candidates,
            "seeds": seeds,
            "samples": samples,
            "iterations": iterations,
            "accepted": dry.accepted,
            "verdict": if dry.accepted { "accepted" } else { "rejected" },
            "rejections": dry.rejections,
            "confidence": conf,
            "catalog_confidence": row.confidence,
            "sample_stats": serde_json::to_value(&dry)?,
            "cost_usd": cost_usd,
            "session_id": session_id,
            // Durable execution: true when this attempt reused a prior
            // attempt's discovery instead of re-spending the research call.
            "resumed_discovery": resumed_discovery,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pumper_core::Catalog;

    fn outcome_with(
        engine: &'static str,
        html: Option<&str>,
        markdown: Option<&str>,
        text: Option<&str>,
    ) -> FetchOutcome {
        FetchOutcome {
            url: "https://a.example/list".into(),
            engine,
            status: Some(200),
            html: html.map(str::to_string),
            markdown: markdown.map(str::to_string),
            text: text.map(str::to_string),
            escalations: Vec::new(),
            trace: Vec::new(),
            cost_usd: None,
        }
    }

    // ── sampling ────────────────────────────────────────────────────────────

    /// THE showstopper this app shipped with: the sampler read `text` then
    /// `markdown` and never `html` — but every non-claude tier returns the body
    /// in `html` — so a perfectly good fetch produced an empty body, every
    /// candidate was skipped, and the run hard-errored AFTER paying for
    /// discovery. HTML is also the only body a CSS rule can bind against.
    #[test]
    fn sample_body_prefers_html_not_only_text() {
        // The http / browser / archive / recipe shape: body in `html`.
        let mut http = outcome_with("http", Some("<h1>real page</h1>"), None, None);
        assert_eq!(
            select_sample_body(&mut http),
            Some(("html", "<h1>real page</h1>".to_string())),
            "an html-bearing fetch must never sample as empty"
        );

        // Even when a flattened body is ALSO present, the DOM wins — selectors
        // cannot bind against Markdown.
        let mut both = outcome_with("http", Some("<h1>x</h1>"), Some("# x"), Some("x"));
        assert_eq!(select_sample_body(&mut both).unwrap().0, "html");

        // The claude tier has no DOM; markdown (only present because the
        // sampler asks for it) then text are the honest fallbacks.
        let mut claude = outcome_with("claude", None, Some("# answer"), Some("answer"));
        assert_eq!(
            select_sample_body(&mut claude),
            Some(("markdown", "# answer".to_string()))
        );
        let mut text_only = outcome_with("claude", None, None, Some("answer"));
        assert_eq!(
            select_sample_body(&mut text_only),
            Some(("text", "answer".to_string()))
        );

        // Whitespace is not a body, and nothing is not a body.
        let mut blank = outcome_with("http", Some("   \n "), None, Some(""));
        assert!(select_sample_body(&mut blank).is_none());
        assert!(select_sample_body(&mut outcome_with("http", None, None, None)).is_none());
    }

    #[test]
    fn sample_artifacts_are_named_for_their_body_not_always_html() {
        assert_eq!(sample_artifact_name(0, "html"), "sample-0.html");
        assert_eq!(sample_artifact_name(1, "markdown"), "sample-1.md");
        assert_eq!(sample_artifact_name(2, "text"), "sample-2.txt");
    }

    #[test]
    fn catalog_engine_maps_the_real_tier_not_a_hardcoded_http() {
        assert_eq!(catalog_engine("browser"), "browser");
        assert_eq!(catalog_engine("claude"), "claude");
        assert_eq!(catalog_engine("http"), "http");
        // No catalog spelling of their own; both are HTTP GETs to a reviewer.
        assert_eq!(catalog_engine("archive"), "http");
        assert_eq!(catalog_engine("api_recipe"), "http");
        // And the row carries it, instead of always claiming "http".
        let row = build_catalog_row(
            "p",
            &cand("https://a.example"),
            &fixture_dry(),
            90,
            "browser",
        );
        assert_eq!(row.engine, "browser");
        // …still a valid value of the documented catalog vocabulary.
        for tier in ["archive", "api_recipe", "http", "browser", "claude", "??"] {
            assert!(
                ["http", "browser", "claude", "bulk"].contains(&catalog_engine(tier)),
                "{tier} mapped outside the documented engine vocabulary"
            );
        }
    }

    #[test]
    fn discovery_checkpoint_round_trips_and_is_bound_to_its_prompt() {
        let key = proposal_key("track czech rust salaries");
        let cp = DiscoveryCheckpoint {
            proposal_key: key.clone(),
            candidates: vec![cand("https://a.example/list")],
            session_id: Some("sess-1".into()),
        };
        let blob = serde_json::to_value(&cp).unwrap();

        let back = DiscoveryCheckpoint::from_blob(Some(&blob), &key).expect("same prompt resumes");
        assert_eq!(back.candidates, cp.candidates);
        assert_eq!(back.session_id.as_deref(), Some("sess-1"));

        // A checkpoint from a DIFFERENT prompt must never be adopted — it would
        // silently compile the wrong source.
        assert!(DiscoveryCheckpoint::from_blob(Some(&blob), &proposal_key("something else")).is_none());
    }

    #[test]
    fn unusable_checkpoints_fall_back_to_a_fresh_discovery() {
        let key = proposal_key("p");
        // Fresh run, corrupt blob, and an empty-candidate blob all mean "run
        // discovery" — a checkpoint must never be able to fail the job.
        assert!(DiscoveryCheckpoint::from_blob(None, &key).is_none());
        assert!(DiscoveryCheckpoint::from_blob(Some(&json!("garbage")), &key).is_none());
        assert!(DiscoveryCheckpoint::from_blob(
            Some(&json!({ "proposal_key": key, "candidates": [] })),
            &key
        )
        .is_none());
    }

    fn cand(url: &str) -> Candidate {
        Candidate {
            url: url.into(),
            name: "Test Source".into(),
            cadence: "weekly".into(),
            expected_fields: vec!["title".into(), "price".into()],
        }
    }

    /// A realistic listing-page fixture the dry-run loop iterates against.
    const FIXTURE: &str = r#"
        <html><body><h1>Widget Prices</h1>
        <div id="list">
            <div class="card"><h3>Alpha</h3><span class="price">$10</span></div>
            <div class="card"><h3>Beta</h3><span class="price">$20</span></div>
        </div></body></html>"#;

    fn ruleset(v: Value) -> RuleSet {
        serde_json::from_value(v).unwrap()
    }

    // ── discovery parsing ───────────────────────────────────────────────────

    #[test]
    fn candidates_parse_defensively_cap_dedupe_and_drop_bad_urls() {
        let v = json!({"candidates": [
            {"url": "https://a.example/jobs", "name": "A", "cadence": "daily",
             "expected_fields": ["title", " salary ", ""]},
            {"url": "https://a.example/jobs"},                  // duplicate
            {"url": "ftp://b.example"},                          // bad scheme
            {"name": "no url at all"},
            {"url": "https://c.example", "cadence": "hourly-ish"}, // unknown cadence
            {"url": "https://d.example"},
            {"url": "https://e.example"},                        // over the cap
        ]});
        let got = parse_candidates(&v);
        assert_eq!(got.len(), MAX_CANDIDATES);
        assert_eq!(got[0].url, "https://a.example/jobs");
        assert_eq!(got[0].cadence, "daily");
        assert_eq!(got[0].expected_fields, vec!["title", "salary"]);
        // Unknown cadence normalizes to the conservative default.
        assert_eq!(got[1].cadence, "weekly");
        assert_eq!(got[2].url, "https://d.example");
    }

    #[test]
    fn unparseable_discovery_yields_empty_not_panic() {
        assert!(parse_candidates(&json!("prose")).is_empty());
        assert!(parse_candidates(&json!({"candidates": "nope"})).is_empty());
        assert!(parse_candidates(&Value::Null).is_empty());
    }

    // ── dry-run harness (the LLM boundary stubbed: drafts are fixtures) ─────

    /// A DIFFERENT site's listing page — same data, entirely different markup.
    /// This is what a second or third candidate actually looks like, and why
    /// pooling it into the accept bar was incoherent.
    const OTHER_SITE: &str = r#"
        <html><body><header><span class="page-title">Prices</span></header>
        <ul class="results">
            <li class="row"><b>Delta</b><em>40 USD</em></li>
        </ul></body></html>"#;

    fn held<'a>(url: &'a str, body: &'a str) -> HeldOutDoc<'a> {
        HeldOutDoc { url, body }
    }

    #[test]
    fn good_draft_is_accepted_by_the_dry_run() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "items": {"type": "each", "selector": ".card", "container": "#list",
                      "fields": {"name": {"type": "css", "selector": "h3"},
                                 "price": {"type": "css", "selector": ".price",
                                           "transforms": [{"op": "to_number"}]}}}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert!(dry.accepted);
        assert_eq!(dry.fields_held, 2);
        assert!(dry.worst_fields.is_empty());
        assert!(dry.rejections.is_empty());
        assert_eq!(dry.stats["items"].items, Some(2));
        assert_eq!(confidence(&dry), 100);
    }

    #[test]
    fn majority_miss_is_rejected_and_names_the_worst_fields() {
        // 1 of 3 fields holds — no majority on the primary document.
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "author": {"type": "css", "selector": ".author"},
            "date": {"type": "css", "selector": "time"}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert!(!dry.accepted);
        assert_eq!(dry.fields_held, 1);
        assert_eq!(dry.worst_fields, vec!["author".to_string(), "date".into()]);
    }

    #[test]
    fn exactly_half_matched_is_not_a_majority() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "missing": {"type": "css", "selector": ".nope"}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert!(!dry.accepted, "strict majority: 1/2 must not pass");
    }

    #[test]
    fn uncompilable_draft_is_feedback_not_a_scored_zero() {
        let rules = ruleset(json!({"x": {"type": "css", "selector": ":::"}}));
        assert!(dry_run(&rules, FIXTURE, &[]).is_err());
        // And an empty rule object is equally a loud error.
        assert!(dry_run(&ruleset(json!({})), FIXTURE, &[]).is_err());
    }

    #[test]
    fn iteration_loop_converges_on_the_repaired_draft() {
        // The dry-run loop with the LLM stubbed as a fixture sequence, the way
        // research's tests stub its engine boundary: draft 1 fails (feedback
        // names the broken field), draft 2 — "repaired" — is accepted. Two
        // iterations, matching max_iterations' default.
        let draft1 = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "name": {"type": "css", "selector": ".product-name"}, // wrong
            "price": {"type": "css", "selector": ".cost"}         // wrong
        }));
        let first = dry_run(&draft1, FIXTURE, &[]).unwrap();
        assert!(!first.accepted);
        assert_eq!(first.worst_fields, vec!["name".to_string(), "price".into()]);

        let draft2 = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "name": {"type": "css", "selector": ".card h3", "all": true},
            "price": {"type": "css", "selector": ".card .price", "all": true}
        }));
        let second = dry_run(&draft2, FIXTURE, &[]).unwrap();
        assert!(second.accepted, "repaired draft must stop the loop");
        assert_eq!(second.fields_held, 3);
    }

    // ── scoring is per-document, not pooled across sites ────────────────────

    /// The incoherence this replaces: the draft was written against `bodies[0]`
    /// alone but scored as a pooled two-level majority over up to three bodies
    /// from DIFFERENT SITES. A field that binds only on its own page needed
    /// `1*2 >= 3` with three samples and failed — so the number of candidates
    /// that happened to fetch silently moved the pass bar, and repair
    /// iterations burned on a cross-site mismatch no selector can fix.
    #[test]
    fn the_accept_bar_reads_the_primary_document_not_a_pool_of_other_sites() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "items": {"type": "each", "selector": ".card", "container": "#list",
                      "fields": {"name": {"type": "css", "selector": "h3"}}}
        }));
        // Alone: a clean pass.
        let alone = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert!(alone.accepted);
        assert_eq!(confidence(&alone), 100);

        // With two unrelated sites added — the SAME draft against the SAME page
        // it was written for. Under the old pooled bar these selectors bound on
        // 1 of 3 docs and the draft was rejected.
        let with_others = dry_run(
            &rules,
            FIXTURE,
            &[
                held("https://b.example", OTHER_SITE),
                held("https://c.example", OTHER_SITE),
            ],
        )
        .unwrap();
        assert!(
            with_others.accepted,
            "adding candidates must not move the pass bar"
        );
        assert_eq!(
            confidence(&with_others),
            confidence(&alone),
            "confidence must not move with the sample count either"
        );

        // The held-out evidence is REPORTED, per candidate, never pooled.
        assert_eq!(with_others.docs, 3);
        assert_eq!(with_others.held_out.len(), 2);
        assert_eq!(with_others.held_out[0].url, "https://b.example");
        assert_eq!(with_others.held_out[0].fields_held, 0);
        assert_eq!(with_others.held_out[0].fields_total, 2);
        assert_eq!(
            with_others.held_out[0].fields_missing,
            vec!["heading".to_string(), "items".into()]
        );
        // …and it did not contaminate the primary's own stats.
        assert!(with_others.worst_fields.is_empty());
    }

    // ── degenerate drafts ───────────────────────────────────────────────────

    /// `Rule::Const` always binds, so a rule set of nothing but constants
    /// scored a perfect 100 against any document — including an empty one —
    /// and stopped the loop having extracted zero facts from the page.
    #[test]
    fn a_const_only_draft_is_rejected_not_scored_a_perfect_hundred() {
        let rules = ruleset(json!({
            "source": {"type": "const", "value": "widgets"},
            "currency": {"type": "const", "value": "USD"}
        }));
        assert!(const_only_rule_set(&rules));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert_eq!(dry.fields_held, 2, "constants do bind — that is the trap");
        assert!(!dry.accepted, "…but a draft that reads nothing is unusable");
        assert!(dry.rejections[0].contains("every rule is a `const`"));
        assert_eq!(confidence(&dry), 0, "a rejected draft has no confidence");
        // One const among real selectors is fine — that is a legitimate
        // provenance/constant field, not a degenerate draft.
        let mixed = ruleset(json!({
            "currency": {"type": "const", "value": "USD"},
            "heading": {"type": "css", "selector": "h1"}
        }));
        assert!(!const_only_rule_set(&mixed));
        assert!(dry_run(&mixed, FIXTURE, &[]).unwrap().accepted);
    }

    /// `ContainerEmpty` is deliberately not a miss (a quiet job board is not a
    /// broken selector) — but that reasoning needs a selector with a track
    /// record, and a draft has none. An `each` rule empty on EVERY sample is
    /// exactly the listing selector never shown to work, and it used to pass.
    #[test]
    fn an_each_field_empty_on_every_doc_is_rejected_not_counted_as_bound() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "items": {"type": "each", "selector": ".no-such-item", "container": "#list",
                      "fields": {"name": {"type": "css", "selector": "h3"}}}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        // The container matched, so the engine reports it bound, not missed.
        assert!(dry.stats["items"].bound);
        assert_eq!(dry.stats["items"].items, Some(0));
        assert!(!dry.accepted);
        assert!(dry.rejections[0].contains("0 items on every sampled document"));

        // The pure predicate: empty everywhere is a rejection, empty somewhere
        // is not — a genuinely quiet listing on one page must stay usable.
        let m = |n: usize| BTreeMap::from([("items".to_string(), n)]);
        assert_eq!(always_empty_each_fields(&[m(0), m(0)]), vec!["items"]);
        assert!(always_empty_each_fields(&[m(0), m(3)]).is_empty());
        assert!(always_empty_each_fields(&[]).is_empty());
    }

    /// `to_number` over `"Add to cart"` yields null while the field still
    /// reports `Matched`. The engine has always computed this
    /// (`CoercionStatus::CoercionFailed`) and the dry run never read it, so a
    /// selector pointing at the wrong element counted as a working one.
    #[test]
    fn a_coercion_failed_field_is_rejected_not_counted_as_matched() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            // Matches the <h3>, which is a product name, not a number.
            "price": {"type": "css", "selector": ".card h3",
                      "transforms": [{"op": "to_number"}]}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert!(
            dry.stats["price"].bound,
            "the selector did match — the trap"
        );
        assert_eq!(dry.stats["price"].coercion, "coercion_failed");
        assert!(!dry.stats["price"].holds(), "a wrong element is not a hold");
        assert!(!dry.accepted);
        assert!(dry.rejections[0].contains("price"));
        assert_eq!(confidence(&dry), 0);
    }

    /// Every rejection is decided from the extraction the dry run ALREADY ran,
    /// so it is known before the loop can spend a repair call — and a
    /// degenerate draft can never be the thing that stops the loop.
    #[test]
    fn degenerate_rejections_are_deterministic_and_need_no_metered_call() {
        for rules in [
            json!({"a": {"type": "const", "value": 1}}),
            json!({"a": {"type": "each", "selector": ".none", "container": "#list",
                         "fields": {"x": {"type": "css", "selector": "h3"}}}}),
            json!({"a": {"type": "css", "selector": ".card h3",
                         "transforms": [{"op": "to_number"}]}}),
        ] {
            let rs = ruleset(rules);
            let a = dry_run(&rs, FIXTURE, &[]).unwrap();
            let b = dry_run(&rs, FIXTURE, &[]).unwrap();
            assert!(!a.rejections.is_empty());
            assert_eq!(a.rejections, b.rejections, "must be deterministic");
            assert!(!a.accepted);
        }
    }

    #[test]
    fn confidence_reflects_partial_binding_of_the_primary_document() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "missing": {"type": "css", "selector": ".nope"}
        }));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        assert_eq!(confidence(&dry), 50);
        // Held-out candidates are evidence, not score: adding a site where
        // nothing binds must not change the number.
        let with_other =
            dry_run(&rules, FIXTURE, &[held("https://b.example", OTHER_SITE)]).unwrap();
        assert_eq!(confidence(&with_other), 50);
    }

    // ── proposal record honesty ─────────────────────────────────────────────

    /// An accepted dry run over [`FIXTURE`], for row-shaping tests.
    fn fixture_dry() -> DryRun {
        let rules = ruleset(json!({"heading": {"type": "css", "selector": "h1"}}));
        dry_run(&rules, FIXTURE, &[]).unwrap()
    }

    /// A rejected dry run over [`FIXTURE`] — nothing binds.
    fn rejected_dry() -> DryRun {
        let rules = ruleset(json!({
            "author": {"type": "css", "selector": ".author"},
            "date": {"type": "css", "selector": "time"}
        }));
        dry_run(&rules, FIXTURE, &[]).unwrap()
    }

    /// The row wrote this app's 0–100 score straight into a catalog column
    /// documented as 1–5, so a proposal that bound half its fields claimed
    /// `confidence = 50` next to hand-written rows scoring 1–5.
    #[test]
    fn row_confidence_is_the_catalog_1_to_5_scale_not_a_raw_0_to_100_score() {
        assert_eq!(confidence_1_to_5(0), 1, "0 is not a point on the scale");
        assert_eq!(confidence_1_to_5(19), 1);
        assert_eq!(confidence_1_to_5(20), 2);
        assert_eq!(confidence_1_to_5(50), 3);
        assert_eq!(confidence_1_to_5(60), 4);
        assert_eq!(confidence_1_to_5(80), 5);
        assert_eq!(confidence_1_to_5(100), 5);
        assert_eq!(
            confidence_1_to_5(255),
            5,
            "out-of-range clamps, never wraps"
        );
        // Monotone and in range for every possible input.
        for s in 0u8..=255 {
            let c = confidence_1_to_5(s);
            assert!((1..=5).contains(&c), "score {s} → out-of-vocabulary {c}");
            if s > 0 {
                assert!(confidence_1_to_5(s - 1) <= c, "not monotone at {s}");
            }
        }
        // …and the row uses it.
        let row = build_catalog_row("p", &cand("https://a.example"), &fixture_dry(), 100, "http");
        assert_eq!(row.confidence, 5);
    }

    /// `access = "public"` is in none of the documented vocabulary
    /// (`key-free · api-key · bulk · scrape`), so the field was unfilterable.
    #[test]
    fn row_access_is_documented_vocabulary_not_the_invented_public() {
        let row = build_catalog_row("p", &cand("https://a.example"), &fixture_dry(), 80, "http");
        assert_eq!(row.access, "scrape");
        assert!(["key-free", "api-key", "bulk", "scrape"].contains(&row.access.as_str()));
    }

    /// The row named a dataset nothing writes — indistinguishable, in the
    /// catalog, from a live source's real dataset.
    #[test]
    fn row_dataset_is_marked_unprovisioned_not_a_name_nothing_writes() {
        let row = build_catalog_row(
            "track widget prices",
            &cand("https://a.example"),
            &fixture_dry(),
            80,
            "http",
        );
        assert_eq!(row.dataset, "proposed:track-widget-prices");
        // Unmistakably not a resolvable `<app>/<name>` dataset path.
        assert!(!row.dataset.contains('/'));
        assert!(row.dataset.starts_with("proposed:"));
        assert!(row.notes.contains("UNPROVISIONED"));
    }

    /// A rejected proposal used to be emitted byte-identically to an accepted
    /// one, with the verdict living only in a record field the pasteable TOML
    /// fragment drops.
    #[test]
    fn a_rejected_proposal_is_visibly_rejected_not_shaped_like_an_accepted_one() {
        let good = fixture_dry();
        let bad = rejected_dry();
        assert!(good.accepted && !bad.accepted);

        let c = cand("https://a.example/widgets");
        let ok_row = build_catalog_row("p", &c, &good, confidence(&good), "http");
        let no_row = build_catalog_row("p", &c, &bad, confidence(&bad), "http");
        assert!(ok_row.notes.contains("ACCEPTED"));
        assert!(no_row.notes.contains("REJECTED"));
        assert_ne!(ok_row.notes, no_row.notes);

        // The verdict survives into the TOML fragment — the only artifact a
        // reviewer may ever read.
        assert!(catalog_toml(&no_row).contains("REJECTED"));

        // …and into the record, as a word, not just a boolean.
        let mk = |dry: &DryRun| {
            let row = build_catalog_row("p", &c, dry, confidence(dry), "http");
            build_proposal("p", &row, &json!({}), &[], &[], None, dry, 1, 0.0)
        };
        assert_eq!(mk(&good)["verdict"], json!("accepted"));
        assert_eq!(mk(&bad)["verdict"], json!("rejected"));
        assert_eq!(mk(&bad)["accepted"], json!(false));
        assert_eq!(mk(&bad)["provisioned"], json!(false));
        // Both confidence scales are present and labelled.
        assert_eq!(mk(&good)["confidence"], json!(100));
        assert_eq!(mk(&good)["catalog_confidence"], json!(5));
    }

    // ── proposal record ─────────────────────────────────────────────────────

    #[test]
    fn proposal_carries_the_full_promised_shape() {
        let rules = ruleset(json!({"heading": {"type": "css", "selector": "h1"}}));
        let dry = dry_run(&rules, FIXTURE, &[]).unwrap();
        let c = cand("https://a.example/widgets");
        let row = build_catalog_row(
            "track widget prices weekly",
            &c,
            &dry,
            confidence(&dry),
            "http",
        );
        let p = build_proposal(
            "track widget prices weekly",
            &row,
            &json!({"heading": {"type": "css", "selector": "h1"}}),
            &["https://a.example/widgets".to_string()],
            &[SampleStat {
                url: "https://a.example/widgets".into(),
                engine: "http".into(),
                body_field: "html",
                bytes: FIXTURE.len(),
                tiers: vec!["http:ok".into()],
            }],
            Some(1.0),
            &dry,
            2,
            0.42,
        );
        for key in [
            "prompt", "catalog_row", "catalog_toml", "rule_set", "seeds", "samples", "cadence",
            "budget", "sample_stats", "confidence", "confidence_scale", "catalog_confidence",
            "accepted", "verdict", "provisioned", "status", "intended_dataset", "iterations",
            "cost_usd",
        ] {
            assert!(p.get(key).is_some(), "proposal missing key {key}");
        }
        assert_eq!(p["cadence"], json!("weekly"));
        assert_eq!(p["confidence"], json!(100));
        assert_eq!(p["seeds"], json!(["https://a.example/widgets"]));
        assert_eq!(
            p["status"],
            json!("planned"),
            "every freshly compiled proposal starts planned, whatever its verdict"
        );
        // The catalog row is TOML-shaped: it round-trips through the real
        // catalog parser.
        let parsed = Catalog::parse(p["catalog_toml"].as_str().unwrap()).unwrap();
        assert_eq!(parsed.sources.len(), 1);
        assert_eq!(parsed.sources[0].url, "https://a.example/widgets");
    }

    #[test]
    fn never_writes_catalog_invariant_row_is_inert_and_app_is_unscheduled() {
        // The app itself is on-demand: no schedule, ever.
        assert!(Provisioner.schedule().is_none());
        // Whatever cadence the research proposes, the emitted row is planned
        // with no cron — parsed through the REAL catalog machinery, it is
        // invisible to `live()` and unschedulable, so even pasting the
        // fragment verbatim into data-sources.toml starts nothing.
        for cadence in ["daily", "weekly", "on-demand"] {
            let mut c = cand("https://a.example");
            c.cadence = cadence.into();
            let row = build_catalog_row("some prompt", &c, &fixture_dry(), 90, "http");
            assert_eq!(row.status, "planned");
            assert_eq!(row.cron, "");
            assert!(!row.is_scheduled());
            let toml = catalog_toml(&row);
            let parsed = Catalog::parse(&toml).unwrap();
            assert_eq!(parsed.live().count(), 0, "a proposal must never be live");
        }
    }

    // ── end-to-end run() over stubbed engines ───────────────────────────────

    /// A listing page long enough to clear the fetcher's 250-char escalation
    /// floor, so the http tier WINS and the (panicking) browser stub is never
    /// reached — the same shape a real sample has.
    const LISTING_PAGE: &str = r#"<html><head><title>Widget Price Index</title></head><body>
        <h1>Widget Prices</h1>
        <p>The widget price index is published every week and tracks the retail
        price of the most commonly traded widget models across the domestic
        market. Prices are collected from published retailer listings and are
        stated in United States dollars, inclusive of any listed discount but
        exclusive of shipping, handling and local sales tax. Figures are revised
        whenever a retailer restates a previously published price.</p>
        <div id="list">
            <div class="card"><h3>Alpha</h3><span class="price">$10</span></div>
            <div class="card"><h3>Beta</h3><span class="price">$20</span></div>
            <div class="card"><h3>Gamma</h3><span class="price">$30</span></div>
        </div></body></html>"#;

    /// An `HttpClient` that serves [`LISTING_PAGE`] — the http tier, i.e. the
    /// tier that puts the body in `html` and nowhere else.
    struct ListingHost;

    #[async_trait]
    impl pumper_core::HttpClient for ListingHost {
        async fn fetch(&self, req: pumper_core::HttpRequest) -> Result<pumper_core::HttpResponse> {
            Ok(pumper_core::HttpResponse {
                status: 200,
                headers: std::collections::HashMap::new(),
                body: LISTING_PAGE.to_string(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    /// The regression test the sample-stage bug slipped past: every unit test
    /// fed `dry_run` a fixture string directly, so nothing ever exercised the
    /// fetch → body → draft seam where the body was being dropped. This drives
    /// the REAL `run()` over a stubbed http tier and a scripted model.
    #[tokio::test]
    async fn propose_end_to_end_samples_a_real_body_not_an_empty_one() {
        use pumper_core::testing::{
            engines_with, research_output, Dead, ScriptedResearcher, TempStore, TestContext,
        };
        use std::sync::Arc;

        let researcher = Arc::new(
            ScriptedResearcher::new()
                .on(
                    "Goal:",
                    research_output(
                        json!({"candidates": [{
                            "url": "https://a.example/widgets",
                            "name": "Widget Price Index",
                            "cadence": "weekly",
                            "expected_fields": ["name", "price"]
                        }]})
                        .to_string(),
                    ),
                )
                .on(
                    "Draft extraction rules",
                    research_output(
                        json!({
                            "heading": {"type": "css", "selector": "h1"},
                            "items": {"type": "each", "selector": ".card", "container": "#list",
                                      "fields": {"name": {"type": "css", "selector": "h3"},
                                                 "price": {"type": "css", "selector": ".price"}}}
                        })
                        .to_string(),
                    ),
                ),
        );
        let store = TempStore::new("provisioner-e2e").await;
        let ctx = TestContext::new(&store.storage, "provisioner")
            .params(json!({ "prompt": "track widget prices weekly" }))
            .engines(engines_with(
                Arc::new(ListingHost),
                Arc::new(Dead),
                researcher.clone(),
            ))
            .build();

        let out = Provisioner
            .run(ctx)
            .await
            .expect("a reachable candidate must not hard-error the compile");

        // The bug's exact signature was "no candidate URL yielded a sampleable
        // body" — a seed and a sized sample are what refute it.
        assert_eq!(out["seeds"], json!(["https://a.example/widgets"]));
        let sample = &out["samples"][0];
        assert_eq!(sample["body_field"], json!("html"));
        assert_eq!(sample["engine"], json!("http"));
        assert_eq!(sample["bytes"].as_u64().unwrap(), LISTING_PAGE.len() as u64);
        assert!(!sample["tiers"].as_array().unwrap().is_empty());

        // …and the drafted rules really did bind against that body.
        assert_eq!(out["accepted"], json!(true));
        assert_eq!(out["iterations"], json!(1));

        // The draft prompt was handed the actual page, not an empty string.
        let calls = researcher.calls();
        assert_eq!(calls.len(), 2, "one discovery call, one draft call");
        assert!(
            calls[1].prompt.contains("class=\"card\""),
            "the drafting prompt must carry the sampled DOM"
        );
    }

    #[test]
    fn proposal_key_is_stable_and_sanitized() {
        assert_eq!(
            proposal_key("Track Czech senior Rust salaries, weekly!"),
            "track-czech-senior-rust-salaries-weekly"
        );
        assert_eq!(proposal_key("a"), "a");
        assert_eq!(proposal_key("???"), "proposal");
        // Deterministic: same prompt, same key (upsert = revision, not dupe).
        assert_eq!(proposal_key("x y"), proposal_key("x y"));
        assert!(proposal_key(&"long ".repeat(50)).len() <= KEY_CAP_CHARS);
    }

    // ── proposal lifecycle ──────────────────────────────────────────────────

    /// A `failed` proposal — the LATEST evidence says the rule set does not
    /// bind — must never be promotable regardless of what it scored at compile
    /// time; that is exactly the case a stale `accepted` flag would otherwise
    /// let through.
    #[test]
    fn a_failed_revalidation_blocks_promotion_whatever_the_original_verdict() {
        assert!(!may_promote(STATUS_FAILED, true));
        assert!(!may_promote(STATUS_FAILED, false));
    }

    /// A never-validated (`planned`) proposal has only its compile-time verdict
    /// as evidence, so promotion gates directly on it — a REJECTED proposal
    /// (still emitted, per the record-honesty contract) must not be promotable
    /// just because nothing has touched its status yet.
    #[test]
    fn a_never_validated_proposal_gates_on_its_compile_time_verdict() {
        assert!(may_promote(STATUS_PLANNED, true));
        assert!(!may_promote(STATUS_PLANNED, false));
    }

    /// Once the latest evidence (a fresh re-validation) says the rule set
    /// binds, promotion no longer depends on the stale compile-time verdict —
    /// and re-promoting an already-promoted proposal (re-rendering the
    /// fragment) is not an error.
    #[test]
    fn validated_and_promoted_proposals_promote_regardless_of_the_stale_verdict() {
        assert!(may_promote(STATUS_VALIDATED, false));
        assert!(may_promote(STATUS_PROMOTED, false));
    }

    /// Only a `planned` proposal can rot — one already `validated` or
    /// `promoted` had its attention, and `failed` has its own loud signal. A
    /// non-planned proposal sitting past the window must not be relabeled
    /// "expired" on top of its real status.
    #[test]
    fn only_a_planned_proposal_can_be_flagged_expired() {
        assert!(proposal_is_expired(STATUS_PLANNED, 1_000_000, 100));
        assert!(!proposal_is_expired(STATUS_VALIDATED, 1_000_000, 100));
        assert!(!proposal_is_expired(STATUS_FAILED, 1_000_000, 100));
        assert!(!proposal_is_expired(STATUS_PROMOTED, 1_000_000, 100));
        // Within the window: not expired.
        assert!(!proposal_is_expired(STATUS_PLANNED, 50, 100));
        // `max_age_secs == 0` opts out entirely.
        assert!(!proposal_is_expired(STATUS_PLANNED, 1_000_000, 0));
    }

    /// `validate_sample` is `dry_run` fed a FRESH fetch's body (not the stored
    /// sample) with no held-out documents — the same sample->dry-run seam
    /// `run()` uses for its primary candidate, factored out so the validate
    /// route can drive it from a fetch it performed itself.
    #[test]
    fn validate_sample_scores_a_freshly_fetched_body_not_the_stored_one() {
        let rules = ruleset(json!({
            "heading": {"type": "css", "selector": "h1"},
            "items": {"type": "each", "selector": ".card", "container": "#list",
                      "fields": {"name": {"type": "css", "selector": "h3"}}}
        }));
        let fresh = outcome_with("http", Some(FIXTURE), None, None);
        let (sample, dry) = validate_sample(fresh, &rules).expect("html body scores");
        assert_eq!(sample.body_field, "html");
        assert_eq!(sample.engine, "http");
        assert!(dry.accepted);
        assert_eq!(dry.docs, 1, "validation never carries held-out documents");
        assert!(dry.held_out.is_empty());

        // An empty fresh fetch (the source went dark since the compile) is a
        // loud error, not a silently-scored zero.
        let dead = outcome_with("http", Some("   "), None, None);
        assert!(validate_sample(dead, &rules).is_err());
    }
}
