//! Extraction health: detecting that a source has started producing wrong data,
//! without any ground truth to check against.
//!
//! # The problem
//!
//! Extraction rots silently. Three distinguishable failures, and until this
//! module the runtime noticed one of them:
//!
//! | Failure | What the runtime saw | Damage |
//! |---|---|---|
//! | Selector stops matching | `FieldStatus::Empty` — identical to "this document genuinely lacks the field" | the dataset quietly loses a column |
//! | Selector still matches, **wrong element** | `FieldStatus::Matched`, every counter green | the dataset fills with plausible garbage, revisions record it as a legitimate change, and watches push it downstream |
//! | Fetch degraded (bot wall, 5xx, thin body) | visible per fetch in `TierTrace`, but never aggregated into a judgement | mass false `removed` tombstones through `sync_many` |
//!
//! # Why the past is enough
//!
//! Nobody labels the web, and no label source exists on this machine. But the
//! store already holds, for every record, every revision, plus a content hash
//! and a SimHash. That history is a labelled corpus of *what this extractor
//! produced during the era in which we believed it worked* — and every detector
//! here is grounded in it and nothing else. No model judges correctness
//! anywhere, and nothing waits for a human.
//!
//! # What is actually detectable
//!
//! Deciding whether `$49.99` is the right price requires knowing the right
//! price, so this does not detect *wrongness*. It detects **the conditions under
//! which a selector silently rebinds**, which is narrower and entirely
//! checkable: distinctness collapse (the dominant real failure — a rebound
//! selector usually lands on a template element that is identical on every
//! page), value-domain drift, and violated invariants mined from the source's
//! own history. A redesign where the wrong element has the same cardinality, the
//! same shape and satisfies every invariant — a sale price and a list price
//! swapping places — is not detectable here, and the answer to that class is
//! recoverability, not detection: revisions are append-only, so the affected era
//! stays exactly identifiable and correctable after the fact.
//!
//! # Layout
//!
//! - [`sketch`] — the fixed-size per-field summaries and the statistics on them.
//! - [`detect`] — the pure verdict: score, diagnosis, state transition.
//! - [`store`] — persistence, the rolling baseline, and invariant mining.
//!
//! Detection is free: arithmetic over data the extraction pass already produced,
//! plus one row per field per run.

pub mod detect;
pub mod invariants;
pub mod sketch;
#[cfg(feature = "storage")]
pub mod store;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::extract::DocReport;
use crate::simhash;

pub use detect::{
    Baseline, CohortAdequacy, FetchHealth, InvariantCheck, Reason, Recovery, RunEvaluation,
};
pub use invariants::{Invariant, InvariantKind};
pub use sketch::FieldSketch;
#[cfg(feature = "storage")]
pub use store::{HealthStore, Resilience, SourceHealth, SourceRun};

/// Where a source sits on the health ladder. The unit is `(app, dataset)` —
/// every existing surface already keys on it (watches, triggers, the change
/// feed, the catalog), so gating a consumer is one row lookup rather than a
/// three-way join.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceState {
    /// Producing what it always did.
    Healthy,
    /// One tripped run. Deliberately changes nothing downstream: a single bad
    /// run is dominated by transient causes, and a system that quarantines on
    /// one bad run on an unattended box spends its life quarantining.
    Suspect,
    /// Repeatedly tripped. Writes still land in the live dataset but are stamped
    /// `provisional`, removals are no longer inferred, and pushes stop.
    Degraded,
    /// Writes are diverted to a shadow dataset. Left only by evidence:
    /// `[resilience] recovery_runs` consecutive judged clean runs promote it to
    /// `Probation`, or an operator overrides it.
    Quarantined,
    /// Recovering and being watched: writes are live again but stamped
    /// `provisional`. Another full clean streak earns `Healthy`; one tripped run
    /// drops straight back to `Quarantined`.
    Probation,
    /// A dead source (permanently gone URLs), not a broken extractor. Set
    /// manually; nothing auto-retires.
    Retired,
}

impl SourceState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Suspect => "suspect",
            Self::Degraded => "degraded",
            Self::Quarantined => "quarantined",
            Self::Probation => "probation",
            Self::Retired => "retired",
        }
    }

    /// Parses a stored state. An unrecognized value reads as `healthy` rather
    /// than failing the run: a state column that cannot be parsed must not be
    /// able to take the whole pipeline down.
    pub fn parse(s: &str) -> Self {
        match s {
            "suspect" => Self::Suspect,
            "degraded" => Self::Degraded,
            "quarantined" => Self::Quarantined,
            "probation" => Self::Probation,
            "retired" => Self::Retired,
            _ => Self::Healthy,
        }
    }

    /// The trust stamp records written in this state carry. `None` means
    /// `stable`, which is also what `NULL` in the column means.
    pub fn trust(self) -> Option<&'static str> {
        match self {
            Self::Healthy | Self::Suspect | Self::Retired => None,
            Self::Degraded | Self::Probation => Some("provisional"),
            Self::Quarantined => Some("quarantined"),
        }
    }

    /// Whether outbound pushes (watches, triggers, saved-search alerts) are
    /// suppressed. A push is irreversible once sent, so a source we no longer
    /// stand behind does not get to make one.
    pub fn suppresses_pushes(self) -> bool {
        matches!(self, Self::Degraded | Self::Quarantined | Self::Retired)
    }

    /// Whether full-snapshot removal detection is suppressed, downgrading
    /// `sync_many` to `upsert_many`.
    ///
    /// This is the single most destructive thing a degrading source can do: a
    /// half-broken run produces a short-but-nonempty batch, and removal
    /// detection then tombstones every key missing from it. The empty-batch
    /// guard in `detect_removed` does not cover a *partial* batch, and this is
    /// the guard for that case.
    pub fn suppresses_removals(self) -> bool {
        matches!(self, Self::Degraded | Self::Quarantined)
    }

    /// Whether the search index skips this source's revisions.
    pub fn skips_search_index(self) -> bool {
        matches!(self, Self::Degraded | Self::Quarantined)
    }

    /// Whether writes are diverted to the shadow dataset.
    pub fn diverts_writes(self) -> bool {
        matches!(self, Self::Quarantined)
    }
}

/// How one run was judged. Only `Ok` runs update the rolling baseline — a broken
/// run must never be absorbed into the baseline it is being judged against, and
/// a quiet week must not re-baseline a source into thinking zero is normal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunVerdict {
    Ok,
    /// The fetch layer was too unhealthy to judge the extractor. Changes
    /// nothing: not the state, not the baseline, not the fingerprints.
    Inconclusive,
    /// The listing was found and held nothing. Healthy, but not baseline
    /// material.
    ContentEmpty,
    /// The cohort was too small for this source to be judged. Says nothing about
    /// the extractor, so — like `inconclusive` — it moves neither the state nor
    /// the baseline.
    ///
    /// Before this variant existed a below-floor run was recorded as `ok`, which
    /// made it baseline material: a source that never reaches the floor built a
    /// baseline entirely out of runs nobody ever judged, and then measured itself
    /// against it. See [`detect::CohortAdequacy`].
    BelowCohort,
    Broken,
    /// Broken, and the cause is ours (rules, transforms or parser), not the
    /// site's.
    SelfInflicted,
}

impl RunVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Inconclusive => "inconclusive",
            Self::ContentEmpty => "content_empty",
            Self::BelowCohort => "below_cohort",
            Self::Broken => "broken",
            Self::SelfInflicted => "self_inflicted",
        }
    }

    /// Whether this run says anything about the extractor's health — i.e. may
    /// move the source's state.
    pub fn judged(self) -> bool {
        matches!(self, Self::Ok | Self::Broken | Self::SelfInflicted)
    }

    /// Whether this run may enter the rolling baseline.
    pub fn baselines(self) -> bool {
        matches!(self, Self::Ok)
    }
}

/// What kind of failure this looks like.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Diagnosis {
    /// The site was redesigned and the extractor broke: the words held still,
    /// the markup moved, the output moved.
    MarkupDrift,
    /// The words moved and the markup held still — a healthy source reporting
    /// new content.
    ContentChanged,
    /// Fields stopped matching.
    FieldLoss,
    /// A per-record field became constant across the cohort: the selector
    /// rebound to a template element.
    SilentRebind,
    /// The values no longer look like what this field has always produced.
    ValueDomainDrift,
    /// A regularity the source held over its entire history broke.
    InvariantBreak,
    /// Neither input moved and the output did, so the change is ours. Not
    /// something to repair against the site.
    SelfInflicted,
    /// Corroboration needed; every input moved at once.
    Ambiguous,
}

impl Diagnosis {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MarkupDrift => "markup_drift",
            Self::ContentChanged => "content_changed",
            Self::FieldLoss => "field_loss",
            Self::SilentRebind => "silent_rebind",
            Self::ValueDomainDrift => "value_domain_drift",
            Self::InvariantBreak => "invariant_break",
            Self::SelfInflicted => "self_inflicted",
            Self::Ambiguous => "ambiguous",
        }
    }
}

/// The three normalized drifts, aggregated as cohort **medians** so a handful of
/// genuinely-changed records cannot carry the verdict.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CohortDrift {
    /// How much the visible content moved.
    pub text: f64,
    /// How much the markup structure moved.
    pub dom: f64,
    /// How much the extracted record moved.
    pub value: f64,
    /// Keys present in both this run and the previous one.
    pub compared: u32,
}

/// The three fingerprints of one document. Computed together because they come
/// from one parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DocSignals {
    pub text_simhash: u64,
    pub dom_simhash: u64,
    pub val_simhash: u64,
}

/// Chars of visible text fingerprinted per document. Enough to characterize a
/// page; bounded so one pathological body cannot dominate the pass.
const TEXT_FINGERPRINT_CAP: usize = 200_000;

/// Fingerprints one document and its extracted values.
///
/// The content fingerprint is over *visible text*, not raw HTML: fingerprinting
/// markup would make every markup change look like a content change and destroy
/// the text-blind/structure-blind asymmetry the whole detector runs on.
pub fn doc_signals(doc: &str, values: &Value) -> DocSignals {
    doc_signals_parsed(&scraper::Html::parse_document(doc), values)
}

/// [`doc_signals`] over a DOM the caller already built.
///
/// The **only** implementation of the three fingerprints — [`doc_signals`] is
/// this function plus a parse. Sharing one body rather than duplicating the
/// three calls is what makes "same document in, same fingerprint out" true by
/// construction rather than by review: these values are persisted in
/// `doc_fingerprints` and diffed against the next run, so a fingerprint that
/// drifted with the parse strategy would silently corrupt every future
/// divergence verdict.
pub fn doc_signals_parsed(html: &scraper::Html, values: &Value) -> DocSignals {
    DocSignals {
        text_simhash: simhash::simhash(&crate::markdown::visible_text_capped(
            html,
            TEXT_FINGERPRINT_CAP,
        )),
        dom_simhash: simhash::dom_simhash(html),
        val_simhash: simhash::simhash_value(values),
    }
}

/// Fingerprints a whole batch across all cores — the same rayon path extraction
/// itself runs on.
///
/// Parses each body itself, so it is the right call only when nobody else has
/// the DOM. Runs that extract *and* fingerprint use
/// [`extract_and_fingerprint_batch`], which shares one parse between the two.
pub fn signals_batch(docs: &[String], values: &[Value]) -> Vec<DocSignals> {
    use rayon::prelude::*;
    docs.par_iter()
        .zip(values.par_iter())
        .map(|(doc, values)| doc_signals(doc, values))
        .collect()
}

/// Extracts a batch **and** fingerprints it from the same DOM: one
/// `Html::parse_document` per document per run, not two.
///
/// Extraction parses the body, drops the DOM, and fingerprinting used to build
/// an identical one immediately afterwards — a full second DOM per document on
/// the highest-volume path in the platform. Both consumers are per-document and
/// both run on rayon, so the honest fix is to fuse them into one closure: the
/// DOM is built once, borrowed by the extractor and then by the fingerprinter,
/// and dropped at the end of the same iteration. Peak DOM residency is unchanged
/// — still one DOM per rayon worker, never a batch of them — because the parse
/// never outlives its closure.
///
/// `scraper::Html` is `!Send` (its tendrils are non-atomic), which is why this
/// exists as a fused batch rather than as an "extract, return the DOM, hand it
/// to the fingerprinter" API: a DOM cannot leave the worker that built it.
///
/// Ordering matches [`extract_batch_with_report`](crate::extract::extract_batch_with_report),
/// and the fingerprints are byte-identical to [`doc_signals`] on the same body
/// (both go through [`doc_signals_parsed`]).
pub fn extract_and_fingerprint_batch(
    rules: &crate::extract::CompiledRuleSet,
    docs: &[String],
) -> Vec<(Value, DocReport, DocSignals)> {
    use rayon::prelude::*;
    docs.par_iter()
        .map(|doc| {
            // Unconditional: the fingerprint needs the DOM even when no rule
            // does (a JSON-only rule set still gets a text/DOM fingerprint, and
            // always did — this changes where the parse happens, not whether).
            let html = scraper::Html::parse_document(doc);
            let (values, report) =
                crate::extract::extract_one_parsed(rules, doc, true, Some(&html));
            let signals = doc_signals_parsed(&html, &values);
            (values, report, signals)
        })
        .collect()
}

/// One document as the detector sees it.
pub struct ObservedDoc {
    /// The record key — how this document is matched to its own past.
    pub key: String,
    pub values: Value,
    pub report: DocReport,
    pub signals: DocSignals,
}

/// One run, as an app reports it.
pub struct RunReport<'a> {
    pub job_id: uuid::Uuid,
    pub dataset: &'a str,
    pub docs: &'a [ObservedDoc],
    pub fetch: FetchHealth,
    /// Build identity, so a fleet-wide break correlates with a deploy in one
    /// query instead of looking like thirty sites changing at once.
    pub build_id: Option<String>,
}

/// The outcome of observing a run: the verdict, and where the source now sits.
#[derive(Debug, Clone, Serialize)]
pub struct SourceVerdict {
    pub source_id: String,
    pub verdict: RunVerdict,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnosis: Option<Diagnosis>,
    pub score: f64,
    pub state: SourceState,
    pub previous_state: SourceState,
    pub statistical_coverage: bool,
    /// Why the run did or did not get statistical coverage — decided against
    /// this source's own history, not against one fleet-wide constant.
    pub cohort: CohortAdequacy,
    pub reasons: Vec<Reason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drift: Option<CohortDrift>,
}

/// Composes a source id from its `(app, dataset)`.
pub fn source_id(app: &str, dataset: &str) -> String {
    format!("{app}/{dataset}")
}

/// Suffix of the shadow dataset a quarantined source writes to. Reusing the
/// existing dataset mechanism rather than a new table means every tool already
/// works on it — listing, export, changes, duplicates.
pub const QUARANTINE_SUFFIX: &str = "@q";

/// The dataset a source in `state` should write to.
pub fn write_dataset(dataset: &str, state: SourceState) -> String {
    if state.diverts_writes() && !dataset.ends_with(QUARANTINE_SUFFIX) {
        format!("{dataset}{QUARANTINE_SUFFIX}")
    } else {
        dataset.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_gating_matches_the_documented_lifecycle() {
        // suspect is deliberately inert downstream — that is its whole purpose.
        assert!(!SourceState::Suspect.suppresses_pushes());
        assert!(!SourceState::Suspect.suppresses_removals());
        assert_eq!(SourceState::Suspect.trust(), None);

        // degraded keeps writing to the live dataset, but stops inferring
        // removals and stops pushing.
        assert!(SourceState::Degraded.suppresses_pushes());
        assert!(SourceState::Degraded.suppresses_removals());
        assert!(!SourceState::Degraded.diverts_writes());
        assert_eq!(SourceState::Degraded.trust(), Some("provisional"));

        // quarantined diverts writes entirely.
        assert!(SourceState::Quarantined.diverts_writes());
        assert_eq!(SourceState::Quarantined.trust(), Some("quarantined"));

        // healthy gates nothing and stamps nothing.
        assert!(!SourceState::Healthy.suppresses_pushes());
        assert_eq!(SourceState::Healthy.trust(), None);
    }

    #[test]
    fn state_round_trips_and_an_unknown_value_reads_as_healthy() {
        for state in [
            SourceState::Healthy,
            SourceState::Suspect,
            SourceState::Degraded,
            SourceState::Quarantined,
            SourceState::Probation,
            SourceState::Retired,
        ] {
            assert_eq!(SourceState::parse(state.as_str()), state);
        }
        // Fail-open: an unparseable state must not be able to stop a pipeline.
        assert_eq!(SourceState::parse("nonsense"), SourceState::Healthy);
    }

    #[test]
    fn only_ok_runs_enter_the_baseline() {
        assert!(RunVerdict::Ok.baselines());
        // A broken run must never become the baseline it is judged against, and
        // a quiet week must not teach the source that zero is normal.
        assert!(!RunVerdict::Broken.baselines());
        assert!(!RunVerdict::ContentEmpty.baselines());
        assert!(!RunVerdict::Inconclusive.baselines());
        // An inconclusive run says nothing at all.
        assert!(!RunVerdict::Inconclusive.judged());
        assert!(!RunVerdict::ContentEmpty.judged());
        assert!(RunVerdict::Broken.judged());
    }

    #[test]
    fn a_run_that_could_not_be_judged_is_not_baseline_material() {
        // The self-referential-history bug: a cohort too small to judge used to
        // be recorded as `ok`, so it fed the very baseline it would later be
        // measured against. A run nobody judged teaches nothing.
        assert!(!RunVerdict::BelowCohort.baselines());
        assert!(!RunVerdict::BelowCohort.judged());
        assert_eq!(RunVerdict::BelowCohort.as_str(), "below_cohort");
    }

    #[test]
    fn quarantine_write_target_is_idempotent() {
        assert_eq!(write_dataset("products", SourceState::Healthy), "products");
        assert_eq!(write_dataset("products", SourceState::Degraded), "products");
        assert_eq!(
            write_dataset("products", SourceState::Quarantined),
            "products@q"
        );
        // Never double-suffixed: the shadow dataset's own runs stay in place.
        assert_eq!(
            write_dataset("products@q", SourceState::Quarantined),
            "products@q"
        );
    }

    #[test]
    fn doc_signals_move_independently_for_content_and_markup() {
        let values = serde_json::json!({ "title": "A" });
        let a = doc_signals(
            "<div class=\"card\"><h1>Hello world here</h1></div>",
            &values,
        );
        // Same markup, different words.
        let text_changed = doc_signals(
            "<div class=\"card\"><h1>Totally other words</h1></div>",
            &values,
        );
        // Same words, different markup.
        let dom_changed = doc_signals(
            "<section class=\"tile\"><h1>Hello world here</h1></section>",
            &values,
        );

        assert_eq!(
            a.dom_simhash, text_changed.dom_simhash,
            "markup fingerprint is text-blind"
        );
        assert_ne!(a.text_simhash, text_changed.text_simhash);
        assert_ne!(a.dom_simhash, dom_changed.dom_simhash);
        assert_eq!(
            a.text_simhash, dom_changed.text_simhash,
            "text fingerprint is structure-blind"
        );
    }
}
