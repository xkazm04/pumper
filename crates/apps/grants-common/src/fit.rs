//! Applicant fit engine (N31) — `grants/profiles` × `grants/unified` →
//! `grants/fits`.
//!
//! The corpus already carries structured eligibility — unified
//! `eligibilities[]`/`categories[]`/money/`aln`, and the detail record's
//! `requirements` block with `applicant_types` (honest Null-vs-empty),
//! `cost_sharing` and `eligibility_text` — and until now nothing consumed any
//! of it as a *predicate*. The only standing alert over the corpus was a
//! full-text saved search.
//!
//! This module turns those fields into one deterministic verdict per
//! (profile, opportunity) pair, written as a dataset row so the shipped
//! dataset triggers / watches / webhooks fan out "a grant you are eligible for
//! just opened" with **zero new delivery code** — `grants` is already a
//! registered virtual namespace.
//!
//! ## The rule this engine is built around
//!
//! *A wrong vocabulary map produces confident false `blocked` verdicts.* So the
//! honest-absence rule (shared rule 5) is applied one notch harder than
//! elsewhere:
//!
//! - a field the source publishes as Null makes its gate [`GateVerdict::Unknown`],
//!   never a decision;
//! - an applicant-type term the vocabulary map does not recognise makes the
//!   **whole** applicant gate `Unknown` — a miss is never silently dropped from
//!   the term set, because dropping it is how "3 of 4 terms matched nothing"
//!   becomes a confident `blocked`;
//! - `eligible` requires *positive published evidence* on every eligibility
//!   gate. It is never reached by absence.
//!
//! ## Which gates can say `unknown`, and which can only block
//!
//! Four gates are **eligibility** gates — status, geography, applicant type,
//! cost share. When one of them cannot be decided the pair is at best `likely`,
//! and when the applicant gate itself cannot be decided the verdict is
//! `unknown`. The award gate is a **filter**, not an eligibility fact: a grant
//! that publishes no award amount does not become an eligibility mystery, so
//! that gate either blocks (the smallest award is larger than the profile's own
//! stated ceiling) or passes, and says which side of the comparison was absent.
//!
//! ## Why `grants/profiles` is a dataset and not a table
//!
//! The fit pass runs **inside an app** ([`evaluate_fits`], called from
//! [`crate::finalize_unified`]), and an app reaches storage only through
//! `AppContext::datasets` — the dependency rule (apps depend on `core`, never on
//! the server). A `profiles` SQL table would therefore be invisible to the very
//! pass that exists to read it. Beside that: `grants/fits` must be a dataset
//! (triggers, watches, `index_datasets`), so keeping profiles next to it means
//! one storage plane, one export surface (`GET /datasets/grants/profiles`),
//! revisions, provenance, doctor and retention coverage for free — and a
//! profile is an open, operator-authored document (`focus_tags[]`,
//! `programs_watched[]`) whose field set would otherwise cost a migration each
//! time it grew. No migration number is consumed by this feature.

use std::collections::{BTreeSet, HashSet};

use pumper_core::datasets::DerivedPaths;
use pumper_core::{AppContext, Result};
use serde_json::{json, Map, Value};

use crate::{stamp, DETAILS_DATASET, UNIFIED_APP, UNIFIED_DATASET};

/// Operator-authored applicant profiles (`grants/profiles`), keyed by
/// [`profile_key`]. Written through `POST /grants/profiles` (admin scope — the
/// auth layer's default for a mutating route), read by [`evaluate_fits`].
pub const PROFILES_DATASET: &str = "profiles";

/// One scored, explained verdict per profile × opportunity (`grants/fits`),
/// keyed [`fit_key`]. `new`/`changed` rows of a run are exactly the alert set.
pub const FITS_DATASET: &str = "fits";

/// Cap on the profile read behind a pass. Profiles are hand-authored; 200 is far
/// past any plausible operator roster and keeps a runaway POST loop from making
/// every sync quadratic.
pub const PROFILE_LIMIT: i64 = 200;

/// Cap on how many changed unified keys one pass evaluates. Reaching it is
/// stated in `warnings` — the pass never silently drops the tail.
pub const FIT_DELTA_LIMIT: usize = 5_000;

/// The `method` stamp on every v1 row: which arm decided. Claude refinement over
/// `eligibility_text` is out of the v1 slice, so today there is exactly one arm
/// and the field exists so a second one cannot arrive un-named.
pub const METHOD_DETERMINISTIC: &str = "deterministic";

// ---------------------------------------------------------------------------
// Vocabulary
// ---------------------------------------------------------------------------

/// The applicant taxonomy a profile declares itself in. Closed vocabulary: a
/// profile naming anything else is refused at the door rather than silently
/// filed as "other" and then matched against nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrgType {
    Nonprofit,
    Gov,
    Tribal,
    Smb,
    University,
    Individual,
}

/// Every legal `org_type`, in the order the API documents them.
pub const ORG_TYPES: &[OrgType] = &[
    OrgType::Nonprofit,
    OrgType::Gov,
    OrgType::Tribal,
    OrgType::Smb,
    OrgType::University,
    OrgType::Individual,
];

impl OrgType {
    pub fn as_str(self) -> &'static str {
        match self {
            OrgType::Nonprofit => "nonprofit",
            OrgType::Gov => "gov",
            OrgType::Tribal => "tribal",
            OrgType::Smb => "smb",
            OrgType::University => "university",
            OrgType::Individual => "individual",
        }
    }

    pub fn parse(s: &str) -> Option<OrgType> {
        ORG_TYPES
            .iter()
            .copied()
            .find(|o| o.as_str() == s.trim().to_lowercase())
    }
}

/// Which [`OrgType`]s one published applicant-type / eligibility term admits,
/// or `None` when this map has never seen the term.
///
/// **`None` is load-bearing.** It does not mean "no org type" — it means "we do
/// not know what this term means", and [`applicant_gate`] turns that into
/// `Unknown` for the whole gate. The terms are the ones grants.gov publishes in
/// `applicantTypes[]` and the California portal in `ApplicantType`, matched as
/// case-insensitive substrings because both sources publish sentences
/// ("Nonprofits having a 501(c)(3) status with the IRS, other than institutions
/// of higher education"), not codes.
///
/// **Order is the correctness argument.** The 501(c)(3) sentence above *contains*
/// "institutions of higher education", so the nonprofit arm must be tested
/// before the university arm or every federal nonprofit row would be filed as a
/// university.
pub fn applicant_vocab(term: &str) -> Option<Vec<OrgType>> {
    let t = term.trim().to_lowercase();
    if t.is_empty() {
        return None;
    }
    let has = |needle: &str| t.contains(needle);
    if has("unrestricted") || has("any organization") || has("open to all") {
        return Some(ORG_TYPES.to_vec());
    }
    // Before the higher-education arm, on purpose — see the doc comment.
    if has("nonprofit") || has("non-profit") || has("not-for-profit") || has("501(c)") {
        return Some(vec![OrgType::Nonprofit]);
    }
    if has("tribal") || has("native american") || has("tribe") {
        return Some(vec![OrgType::Tribal]);
    }
    if has("higher education") || has("universit") || has("college") || has("school district") {
        return Some(vec![OrgType::University]);
    }
    if has("small business") {
        return Some(vec![OrgType::Smb]);
    }
    if has("for profit") || has("for-profit") || has("business") || has("private sector") {
        return Some(vec![OrgType::Smb]);
    }
    if has("government")
        || has("public agency")
        || has("state agency")
        || has("municipal")
        || has("county")
        || has("city or township")
        || has("special district")
    {
        return Some(vec![OrgType::Gov]);
    }
    if has("individual") {
        return Some(vec![OrgType::Individual]);
    }
    // Includes grants.gov's literal "Others (see text field entitled
    // \"Additional Information on Eligibility\" for clarification)", which is a
    // pointer at prose this engine deliberately does not read in v1.
    None
}

/// Where a source's money may be spent, as far as the corpus can honestly say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceGeo {
    /// A national programme: applicants must be established in this country.
    Country(&'static str),
    /// A sub-national programme: this country AND this state.
    SubState(&'static str, &'static str),
    /// An EU framework programme. Membership passes; everything else is
    /// `unknown`, because third-country participation is real and this engine
    /// has no field that decides it.
    EuFramework,
}

/// The geography of one source app, or `None` for a source this map has not
/// been taught — which becomes an `Unknown` gate, never a block.
pub fn source_geo(source: &str) -> Option<SourceGeo> {
    match source.trim() {
        "grants-gov" => Some(SourceGeo::Country("US")),
        "ca-grants" => Some(SourceGeo::SubState("US", "CA")),
        "eu-sedia" => Some(SourceGeo::EuFramework),
        _ => None,
    }
}

/// EU member states, as ISO-3166 alpha-2. Associated countries are deliberately
/// absent: an applicant from one of them lands in the `unknown` arm rather than
/// being blocked on a list this module cannot keep current.
const EU_MEMBERS: &[&str] = &[
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV",
    "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
];

// ---------------------------------------------------------------------------
// The profile
// ---------------------------------------------------------------------------

/// One applicant, parsed from a stored `grants/profiles` record.
#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
    pub key: String,
    pub name: String,
    pub org_type: OrgType,
    /// ISO-3166 alpha-2, upper-case.
    pub country: String,
    /// Sub-national code, upper-case. `None` = the profile did not say, which is
    /// a reason to answer `unknown` on a state programme, never to block.
    pub state: Option<String>,
    /// Largest award this applicant says it can absorb. `None` = not declared,
    /// so the award gate has nothing to test (see [`award_gate`]).
    pub budget_max: Option<f64>,
    pub focus_tags: Vec<String>,
    /// `None` = the profile did not say whether it can meet a match requirement.
    /// **Never defaulted to `false`** — that default is the single most likely
    /// source of a fabricated `blocked`.
    pub cost_share_capacity: Option<bool>,
    pub programs_watched: Vec<String>,
}

impl Profile {
    /// Parses a stored record, or `None` when it carries no usable identity.
    /// `None` is a *skip with a warning* in [`evaluate_fits`], never a fit row:
    /// a profile whose `org_type` cannot be read cannot be matched against
    /// anything, and inventing a default org type is how every row for that
    /// profile becomes a lie.
    pub fn from_record(key: &str, data: &Value) -> Option<Profile> {
        let org_type = OrgType::parse(data.get("org_type").and_then(Value::as_str)?)?;
        let country = data
            .get("country")
            .and_then(Value::as_str)
            .map(|c| c.trim().to_uppercase())
            .filter(|c| !c.is_empty())?;
        Some(Profile {
            key: key.to_string(),
            name: data
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or(key)
                .to_string(),
            org_type,
            country,
            state: data
                .get("state")
                .and_then(Value::as_str)
                .map(|s| s.trim().to_uppercase())
                .filter(|s| !s.is_empty()),
            budget_max: data
                .get("budget_band")
                .and_then(|b| b.get("max"))
                .and_then(Value::as_f64),
            focus_tags: string_list(data.get("focus_tags")),
            cost_share_capacity: data.get("cost_share_capacity").and_then(Value::as_bool),
            programs_watched: string_list(data.get("programs_watched")),
        })
    }
}

fn string_list(v: Option<&Value>) -> Vec<String> {
    let Some(Value::Array(items)) = v else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// The record key one profile is stored under: its name, slugged.
///
/// Pure, so the POST door and any future consumer spell the key exactly one way
/// — a second spelling is how two surfaces come to disagree about which profile
/// exists (`market_profile_key`'s lesson, restated).
pub fn profile_key(name: &str) -> String {
    let mut key = String::new();
    let mut pending_dash = false;
    for ch in name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_dash && !key.is_empty() {
                key.push('-');
            }
            pending_dash = false;
            key.push(ch);
        } else {
            pending_dash = true;
        }
    }
    key
}

/// The `grants/fits` key for one pair. `{profile}:{unified_key}` — the unified
/// key already carries its source prefix, so the grammar is stable across
/// portals.
pub fn fit_key(profile: &str, unified_key: &str) -> String {
    format!("{profile}:{unified_key}")
}

/// The `grants/opportunity_details` key for a unified key, or `None` when the
/// source publishes no detail corpus.
///
/// The detail dataset is keyed by the **opportunity id**, not the unified key
/// (`grants-gov:355099` → `355099`), and it is written by grants-gov alone.
/// Getting this wrong is silent: every lookup misses, `applicant_types` is never
/// read, and every federal verdict quietly degrades to `unknown`.
pub fn detail_key(unified_key: &str) -> Option<String> {
    unified_key
        .strip_prefix("grants-gov:")
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// Gates
// ---------------------------------------------------------------------------

/// What one gate concluded. Three states, and the third is not a failure mode —
/// it is the answer whenever the fields the gate reads are absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateVerdict {
    Pass,
    Block,
    Unknown,
}

/// One gate's verdict with the sentence that explains it. The sentence is the
/// product: a verdict a grant-seeker cannot audit is worth nothing.
#[derive(Debug, Clone, PartialEq)]
pub struct Gate {
    pub verdict: GateVerdict,
    pub reason: String,
    /// Published terms this gate could not map, if any. Surfaced so a pass can
    /// report the vocabulary's blind spots instead of hiding them in `unknown`.
    pub vocab_misses: Vec<String>,
}

impl Gate {
    fn pass(reason: impl Into<String>) -> Gate {
        Gate {
            verdict: GateVerdict::Pass,
            reason: reason.into(),
            vocab_misses: Vec::new(),
        }
    }
    fn block(reason: impl Into<String>) -> Gate {
        Gate {
            verdict: GateVerdict::Block,
            reason: reason.into(),
            vocab_misses: Vec::new(),
        }
    }
    fn unknown(reason: impl Into<String>) -> Gate {
        Gate {
            verdict: GateVerdict::Unknown,
            reason: reason.into(),
            vocab_misses: Vec::new(),
        }
    }
}

/// Is this opportunity still a thing anyone can apply to?
pub fn status_gate(status: Option<&str>) -> Gate {
    match status {
        Some("open") => Gate::pass("the opportunity is open"),
        Some("forecasted") => Gate::pass("the opportunity is forecasted (not yet accepting)"),
        Some("closed") => Gate::block("the opportunity is closed"),
        Some(other) => Gate::unknown(format!("unrecognized status '{other}'")),
        None => Gate::unknown("the source publishes no status"),
    }
}

/// May an applicant established where this profile says it is take this
/// source's money?
pub fn geography_gate(profile: &Profile, source: Option<&str>) -> Gate {
    let Some(source) = source else {
        return Gate::unknown("the record names no source");
    };
    let Some(geo) = source_geo(source) else {
        return Gate::unknown(format!("no geography rule for source '{source}'"));
    };
    match geo {
        SourceGeo::Country(c) if profile.country == c => {
            Gate::pass(format!("{source} funds {c} applicants"))
        }
        SourceGeo::Country(c) => Gate::block(format!(
            "{source} funds {c} applicants; the profile is established in {}",
            profile.country
        )),
        SourceGeo::SubState(c, _) if profile.country != c => Gate::block(format!(
            "{source} is a {c} sub-national programme; the profile is established in {}",
            profile.country
        )),
        SourceGeo::SubState(_, s) => match profile.state.as_deref() {
            Some(state) if state == s => Gate::pass(format!("{source} funds {s} applicants")),
            Some(state) => Gate::block(format!(
                "{source} funds {s} applicants; the profile is in {state}"
            )),
            // The profile simply did not say. Blocking here would be a verdict
            // about the profile's silence, not about the grant.
            None => Gate::unknown(format!(
                "{source} funds {s} applicants and the profile declares no state"
            )),
        },
        SourceGeo::EuFramework if EU_MEMBERS.contains(&profile.country.as_str()) => {
            Gate::pass(format!("{} is an EU member state", profile.country))
        }
        SourceGeo::EuFramework => Gate::unknown(format!(
            "{source} is an EU framework programme and {} is not an EU member state — \
             third-country participation is decided by the call text, which v1 does not read",
            profile.country
        )),
    }
}

/// Does the published applicant vocabulary admit this org type?
///
/// Reads the detail record's `requirements.applicant_types` first (the only
/// place federal eligibility exists at all) and falls back to the unified
/// `eligibilities[]` the California portal fills. Three absences are kept
/// distinct because they mean different things:
///
/// - **Null** — the source published no such field, or the field drifted away.
/// - **`[]`** — the agency published an empty list. "This NOFO lists no eligible
///   applicant types" is not "nobody is eligible", so it is still `unknown`.
/// - **a term we cannot map** — the map's blind spot, not the applicant's.
pub fn applicant_gate(
    org: OrgType,
    applicant_types: Option<&Value>,
    eligibilities: Option<&Value>,
) -> Gate {
    let terms = match (as_terms(applicant_types), as_terms(eligibilities)) {
        (Some(t), _) if !t.is_empty() => t,
        (_, Some(t)) if !t.is_empty() => t,
        (Some(_), _) | (_, Some(_)) => {
            return Gate::unknown(
                "the source published an EMPTY applicant list — an empty list is not \
                 'nobody is eligible'",
            )
        }
        _ => return Gate::unknown("the source publishes no applicant-type vocabulary"),
    };
    let mut misses: Vec<String> = Vec::new();
    let mut matched: Option<String> = None;
    for term in &terms {
        match applicant_vocab(term) {
            Some(types) => {
                if matched.is_none() && types.contains(&org) {
                    matched = Some(term.clone());
                }
            }
            None => misses.push(term.clone()),
        }
    }
    if let Some(term) = matched {
        // A positive match settles the gate even when a sibling term is
        // unmapped: the applicant IS named, and no unmapped term can un-name it.
        return Gate {
            verdict: GateVerdict::Pass,
            reason: format!("published applicant type '{term}' admits {}", org.as_str()),
            vocab_misses: misses,
        };
    }
    if !misses.is_empty() {
        // The whole gate, not just the unmapped terms. Dropping a miss from the
        // set is exactly how "3 of 4 terms matched nothing" becomes a confident
        // `blocked` about a term we never understood.
        return Gate {
            verdict: GateVerdict::Unknown,
            reason: format!(
                "{} published applicant term(s) are outside the vocabulary map (e.g. '{}')",
                misses.len(),
                misses[0]
            ),
            vocab_misses: misses,
        };
    }
    Gate::block(format!(
        "the published applicant types ({}) do not include {}",
        terms.join("; "),
        org.as_str()
    ))
}

/// `Some(terms)` when the field is a published array (possibly empty), `None`
/// when it is absent or Null — the "absent is not empty" distinction
/// `grants-gov::applicant_types` exists to preserve, honoured on the read side.
fn as_terms(v: Option<&Value>) -> Option<Vec<String>> {
    match v {
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

/// Can this applicant meet a match requirement?
///
/// The named gate of the v1 slice: an applicant that has not said whether it can
/// cost-share is `unknown`, **never** `blocked`. Defaulting the capacity to
/// `false` would block every match-requiring federal NOFO for every profile that
/// left the field out — the exact fabricated-verdict failure this engine exists
/// to avoid.
pub fn cost_share_gate(cost_sharing: Option<&Value>, capacity: Option<bool>) -> Gate {
    match (cost_sharing.and_then(Value::as_bool), capacity) {
        (Some(false), _) => Gate::pass("no cost share is required"),
        (Some(true), Some(true)) => {
            Gate::pass("a cost share is required and the profile can meet one")
        }
        (Some(true), Some(false)) => {
            Gate::block("a cost share is required and the profile declares no capacity for one")
        }
        (Some(true), None) => Gate::unknown(
            "a cost share is required and the profile does not say whether it can meet one",
        ),
        (None, _) => Gate::unknown("the source publishes no cost-sharing requirement"),
    }
}

/// Is the smallest award larger than what this applicant says it can absorb?
///
/// A **filter, not an eligibility fact**: it can block, and it can pass, but it
/// never returns `Unknown`. A grant that publishes no award amount is not an
/// eligibility mystery — it is a grant whose size we cannot filter on, and the
/// reason says so.
pub fn award_gate(unified: &Value, detail: Option<&Value>, budget_max: Option<f64>) -> Gate {
    let Some(max) = budget_max else {
        return Gate::pass("the profile declares no budget ceiling, so award size is not tested");
    };
    let floor = num(unified.get("award_floor")).or_else(|| {
        detail
            .and_then(|d| d.get("requirements"))
            .and_then(|r| r.get("award_floor"))
            .and_then(|v| num(Some(v)))
    });
    match floor {
        Some(floor) if floor > max => Gate::block(format!(
            "the smallest award ({floor:.0}) exceeds the profile's stated ceiling ({max:.0})"
        )),
        Some(floor) => Gate::pass(format!(
            "the smallest award ({floor:.0}) is within the profile's ceiling ({max:.0})"
        )),
        None => Gate::pass("the source publishes no award floor, so award size is not tested"),
    }
}

fn num(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_f64)
}

// ---------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Eligible,
    Likely,
    Blocked,
    Unknown,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Eligible => "eligible",
            Verdict::Likely => "likely",
            Verdict::Blocked => "blocked",
            Verdict::Unknown => "unknown",
        }
    }

    /// The closed vocabulary, for the query surface's `verdict=` validation.
    pub const ALL: &'static [Verdict] = &[
        Verdict::Eligible,
        Verdict::Likely,
        Verdict::Blocked,
        Verdict::Unknown,
    ];

    pub fn parse(s: &str) -> Option<Verdict> {
        Verdict::ALL
            .iter()
            .copied()
            .find(|v| v.as_str() == s.trim().to_lowercase())
    }
}

/// One scored, explained verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct Fit {
    pub verdict: Verdict,
    pub score: f64,
    pub reasons: Vec<String>,
    pub blockers: Vec<String>,
    /// The gates that could not be decided, each naming the absent field. This
    /// is the list that tells an operator which source field to enrich next.
    pub unknowns: Vec<String>,
    pub vocab_misses: Vec<String>,
}

/// The whole engine: five gates over one (profile, opportunity) pair.
///
/// Pure — no clock, no I/O, no randomness — so the same corpus and the same
/// profile always produce the same row, which is what lets change detection
/// treat a `changed` fit as real news rather than churn.
pub fn fit(profile: &Profile, unified: &Value, detail: Option<&Value>) -> Fit {
    let requirements = detail.and_then(|d| d.get("requirements"));
    let status = status_gate(unified.get("status").and_then(Value::as_str));
    let geography = geography_gate(profile, unified.get("source").and_then(Value::as_str));
    let applicant = applicant_gate(
        profile.org_type,
        requirements.and_then(|r| r.get("applicant_types")),
        unified.get("eligibilities"),
    );
    let cost_share = cost_share_gate(
        requirements.and_then(|r| r.get("cost_sharing")),
        profile.cost_share_capacity,
    );
    let award = award_gate(unified, detail, profile.budget_max);

    let gates = [&status, &geography, &applicant, &cost_share, &award];
    let mut reasons = Vec::new();
    let mut blockers = Vec::new();
    let mut unknowns = Vec::new();
    let mut vocab_misses = Vec::new();
    for gate in gates {
        vocab_misses.extend(gate.vocab_misses.iter().cloned());
        match gate.verdict {
            GateVerdict::Pass => reasons.push(gate.reason.clone()),
            GateVerdict::Block => blockers.push(gate.reason.clone()),
            GateVerdict::Unknown => unknowns.push(gate.reason.clone()),
        }
    }

    // `eligible` demands positive published evidence on every gate. It cannot be
    // reached by absence, which is the whole point of the three-state gate.
    let verdict = if !blockers.is_empty() {
        Verdict::Blocked
    } else if applicant.verdict != GateVerdict::Pass {
        Verdict::Unknown
    } else if unknowns.is_empty() {
        Verdict::Eligible
    } else {
        Verdict::Likely
    };

    let score = score_of(verdict, profile, unified);
    Fit {
        verdict,
        score,
        reasons,
        blockers,
        unknowns,
        vocab_misses,
    }
}

/// Deterministic 0.0–1.0 ranking within a verdict band, so a consumer can sort
/// one bucket without re-deriving anything. Bands never overlap: a `likely` can
/// never outrank an `eligible`.
fn score_of(verdict: Verdict, profile: &Profile, unified: &Value) -> f64 {
    let (base, headroom) = match verdict {
        Verdict::Eligible => (0.80, 0.20),
        Verdict::Likely => (0.50, 0.20),
        Verdict::Unknown => (0.20, 0.20),
        // A blocked pair is not ranked: every blocker is equally disqualifying.
        Verdict::Blocked => return 0.0,
    };
    let mut bonus: f64 = 0.0;
    if unified.get("status").and_then(Value::as_str) == Some("open") {
        bonus += 0.5;
    }
    if watched(profile, unified) {
        bonus += 0.3;
    }
    if focus_hit(profile, unified) {
        bonus += 0.2;
    }
    // Two decimals: a float that drifts in the 15th place would re-hash the row
    // and mint a `changed` revision — i.e. a false alert — on every pass.
    ((base + headroom * bonus.min(1.0)) * 100.0).round() / 100.0
}

/// Whether the opportunity belongs to a program this profile watches.
fn watched(profile: &Profile, unified: &Value) -> bool {
    let Some(key) = unified
        .get(crate::programs::PROGRAM_KEY_FIELD)
        .and_then(Value::as_str)
    else {
        return false;
    };
    profile.programs_watched.iter().any(|p| p == key)
}

/// Whether any focus tag appears in the title, categories or description.
fn focus_hit(profile: &Profile, unified: &Value) -> bool {
    if profile.focus_tags.is_empty() {
        return false;
    }
    let mut hay = String::new();
    for field in ["title", "description"] {
        if let Some(s) = unified.get(field).and_then(Value::as_str) {
            hay.push_str(&s.to_lowercase());
            hay.push(' ');
        }
    }
    if let Some(Value::Array(cats)) = unified.get("categories") {
        for c in cats.iter().filter_map(Value::as_str) {
            hay.push_str(&c.to_lowercase());
            hay.push(' ');
        }
    }
    profile
        .focus_tags
        .iter()
        .any(|t| hay.contains(&t.to_lowercase()))
}

/// The stored `grants/fits` row.
///
/// Deliberately **verdict-shaped**: it carries the decision and its reasons and
/// nothing denormalized from the opportunity. A row that copied the title would
/// be rewritten — and therefore re-alerted through every watch and trigger on
/// this dataset — every time an agency fixed a typo. `fresh` must mean "the fit
/// changed", not "the grant was touched".
pub fn fit_record(profile: &Profile, unified_key: &str, source: Option<&str>, fit: &Fit) -> Value {
    json!({
        "profile": profile.key,
        "unified_key": unified_key,
        "source": source,
        "verdict": fit.verdict.as_str(),
        "score": fit.score,
        "method": METHOD_DETERMINISTIC,
        "reasons": fit.reasons,
        "blockers": fit.blockers,
        "unknowns": fit.unknowns,
    })
}

// ---------------------------------------------------------------------------
// Validation (the POST door's half)
// ---------------------------------------------------------------------------

/// Validates and canonicalizes a `POST /grants/profiles` body.
///
/// Lives here, not in the route, so the door and the engine cannot disagree
/// about what a profile is: [`Profile::from_record`] reads exactly the shape
/// this function writes.
///
/// **Strict**: an unknown field is refused rather than dropped. A typo'd
/// `cost_share_capacty` that is silently discarded produces a profile that
/// blocks nothing and matches everything, and the operator never learns why.
///
/// Every optional field is written explicitly as `null` when absent, so a stored
/// profile states its own unknowns instead of leaving a reader to guess whether
/// the field was omitted or the key renamed.
pub fn validate_profile(body: &Value) -> std::result::Result<(String, Value), Vec<String>> {
    let mut errors: Vec<String> = Vec::new();
    let Some(obj) = body.as_object() else {
        return Err(vec!["body must be a JSON object".into()]);
    };
    const FIELDS: &[&str] = &[
        "name",
        "org_type",
        "country",
        "state",
        "ein",
        "uei",
        "ntee",
        "budget_band",
        "focus_tags",
        "cost_share_capacity",
        "programs_watched",
    ];
    for key in obj.keys() {
        if !FIELDS.contains(&key.as_str()) {
            errors.push(format!(
                "unknown field '{key}' (allowed: {})",
                FIELDS.join(", ")
            ));
        }
    }

    let name = obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let key = name.map(profile_key).unwrap_or_default();
    if name.is_none() {
        errors.push("'name' is required and must be a non-empty string".into());
    } else if key.is_empty() {
        errors.push("'name' must contain at least one alphanumeric character".into());
    }

    let org_type = obj
        .get("org_type")
        .and_then(Value::as_str)
        .and_then(OrgType::parse);
    if org_type.is_none() {
        errors.push(format!(
            "'org_type' is required and must be one of: {}",
            ORG_TYPES
                .iter()
                .map(|o| o.as_str())
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }

    let country = code(obj.get("country"));
    if country.is_none() {
        errors
            .push("'country' is required and must be a 2-letter ISO-3166 code (e.g. 'US')".into());
    }
    let state = match obj.get("state") {
        None | Some(Value::Null) => None,
        other => match code(other) {
            Some(s) => Some(s),
            None => {
                errors.push("'state' must be a 2-letter code (e.g. 'CA') or null".into());
                None
            }
        },
    };

    let budget_band = match obj.get("budget_band") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::Object(band)) => {
            let min = band.get("min").and_then(Value::as_f64);
            let max = band.get("max").and_then(Value::as_f64);
            for (label, v) in [("min", min), ("max", max)] {
                if band.contains_key(label) && v.is_none() {
                    errors.push(format!("'budget_band.{label}' must be a number or absent"));
                }
                if v.is_some_and(|n| n < 0.0) {
                    errors.push(format!("'budget_band.{label}' must not be negative"));
                }
            }
            if let (Some(min), Some(max)) = (min, max) {
                if min > max {
                    errors.push("'budget_band.min' must not exceed 'budget_band.max'".into());
                }
            }
            json!({ "min": min, "max": max })
        }
        Some(_) => {
            errors.push("'budget_band' must be an object {min?, max?} or null".into());
            Value::Null
        }
    };

    let focus_tags = tag_list(obj, "focus_tags", &mut errors, true);
    let programs_watched = tag_list(obj, "programs_watched", &mut errors, false);

    let cost_share_capacity = match obj.get("cost_share_capacity") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::Bool(b)) => Value::Bool(*b),
        Some(_) => {
            errors.push(
                "'cost_share_capacity' must be true, false or null — absent means UNKNOWN, and \
                 unknown never blocks"
                    .into(),
            );
            Value::Null
        }
    };

    if !errors.is_empty() {
        return Err(errors);
    }
    let profile = json!({
        "name": name.unwrap_or_default(),
        "org_type": org_type.map(OrgType::as_str),
        "country": country,
        "state": state,
        // Identifiers are STORED, not verified: EIN verification against the IRS
        // EO BMF is out of the v1 slice (the source is `planned` in the catalog),
        // so nothing downstream treats these as proof of anything.
        "ein": opt_str(obj.get("ein")),
        "uei": opt_str(obj.get("uei")),
        "ntee": opt_str(obj.get("ntee")),
        "budget_band": budget_band,
        "focus_tags": focus_tags,
        "cost_share_capacity": cost_share_capacity,
        "programs_watched": programs_watched,
    });
    Ok((key, profile))
}

fn code(v: Option<&Value>) -> Option<String> {
    v.and_then(Value::as_str)
        .map(|s| s.trim().to_uppercase())
        .filter(|s| s.len() == 2 && s.chars().all(|c| c.is_ascii_alphabetic()))
}

fn opt_str(v: Option<&Value>) -> Value {
    match v.and_then(Value::as_str).map(str::trim) {
        Some(s) if !s.is_empty() => Value::String(s.to_string()),
        _ => Value::Null,
    }
}

/// A string array field, deduplicated and order-stable. `lower` folds case for
/// free-text tags; program keys keep theirs (they must match `program_key`
/// byte for byte).
fn tag_list(obj: &Map<String, Value>, field: &str, errors: &mut Vec<String>, lower: bool) -> Value {
    match obj.get(field) {
        None | Some(Value::Null) => json!([]),
        Some(Value::Array(items)) => {
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for item in items {
                match item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    Some(s) => {
                        let s = if lower {
                            s.to_lowercase()
                        } else {
                            s.to_string()
                        };
                        if seen.insert(s.clone()) {
                            out.push(Value::String(s));
                        }
                    }
                    None => errors.push(format!("'{field}' must contain non-empty strings")),
                }
            }
            Value::Array(out)
        }
        Some(_) => {
            errors.push(format!("'{field}' must be an array of strings or null"));
            json!([])
        }
    }
}

// ---------------------------------------------------------------------------
// The pass (the I/O half)
// ---------------------------------------------------------------------------

/// What one fit pass produced, for the source's result JSON.
#[derive(Debug, Default, Clone)]
pub struct FitPass {
    pub profiles: usize,
    /// profile × opportunity evaluations performed this pass.
    pub evaluated: usize,
    pub eligible: usize,
    pub likely: usize,
    pub blocked: usize,
    pub unknown: usize,
    /// New or changed fit rows — **exactly the alert set** a dataset trigger or
    /// watch on `grants/fits` fires on.
    pub fresh: usize,
    pub warnings: Vec<String>,
}

impl FitPass {
    /// Whether this pass wrote anything, and therefore whether naming
    /// `grants/fits` in `index_datasets` would describe real revisions rather
    /// than claim coverage of an empty window (the `grants/programs` rule).
    pub fn wrote(&self) -> bool {
        self.fresh > 0
    }

    /// The `fits` block for the source result.
    pub fn block(&self) -> Value {
        json!({
            "profiles": self.profiles,
            "evaluated": self.evaluated,
            "eligible": self.eligible,
            "likely": self.likely,
            "blocked": self.blocked,
            "unknown": self.unknown,
            "fresh": self.fresh,
        })
    }
}

/// Evaluate every stored profile against **this run's delta** — the unified keys
/// it just published as new or changed — and upsert the verdicts into
/// `grants/fits`.
///
/// **Delta, not corpus, and this is a correctness argument, not an optimization.**
/// The engine is pure, so re-running it over an unchanged opportunity can only
/// produce the identical row; the store would report `unchanged` and no alert
/// would fire, but the pass would have paid `profiles × corpus` reads to learn
/// nothing. The delta is exactly the set whose verdict can have moved.
///
/// Runs on **every** producer against the canonical dataset, not only the run
/// that owns the once-per-cycle corpus pass: the corpus pass is a lease over
/// work derived from the *stored* corpus and holds no delta at all, so hanging
/// the fit pass off it would drop two of the three sources' new grants on the
/// floor every day.
pub async fn evaluate_fits(ctx: &AppContext, delta_keys: &[String]) -> Result<FitPass> {
    let mut pass = FitPass::default();
    let stored = ctx
        .datasets
        .list(UNIFIED_APP, PROFILES_DATASET, PROFILE_LIMIT)
        .await?;
    let mut profiles = Vec::new();
    for rec in stored.into_iter().filter(|r| r.removed_at.is_none()) {
        match Profile::from_record(&rec.key, &rec.data) {
            Some(p) => profiles.push(p),
            None => pass.warnings.push(format!(
                "grants/profiles row '{}' names no readable org_type/country and was SKIPPED — \
                 it produces no fits at all rather than fits against a guessed applicant",
                rec.key
            )),
        }
    }
    pass.profiles = profiles.len();
    if profiles.is_empty() || delta_keys.is_empty() {
        return Ok(pass);
    }

    let mut keys: Vec<&String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for key in delta_keys {
        if seen.insert(key.as_str()) {
            keys.push(key);
        }
    }
    if keys.len() > FIT_DELTA_LIMIT {
        pass.warnings.push(format!(
            "grants/fits evaluated only the first {FIT_DELTA_LIMIT} of {} changed unified keys \
             this run: the remainder keep their previous verdict and are re-evaluated the next \
             time the source republishes them",
            keys.len()
        ));
        keys.truncate(FIT_DELTA_LIMIT);
    }

    let mut items: Vec<(String, Value)> = Vec::new();
    let mut misses: BTreeSet<String> = BTreeSet::new();
    for key in keys {
        let Some(rec) = ctx.datasets.get(UNIFIED_APP, UNIFIED_DATASET, key).await? else {
            continue;
        };
        // A tombstoned opportunity is not an answer (the `find_market_profile`
        // rule): re-scoring one would publish a fit for a grant that is gone.
        if rec.removed_at.is_some() {
            continue;
        }
        let detail = match detail_key(key) {
            Some(id) => ctx
                .datasets
                .get(UNIFIED_APP, DETAILS_DATASET, &id)
                .await?
                .filter(|d| d.removed_at.is_none())
                .map(|d| d.data),
            None => None,
        };
        let source = rec
            .data
            .get("source")
            .and_then(Value::as_str)
            .map(String::from);
        for profile in &profiles {
            let verdict = fit(profile, &rec.data, detail.as_ref());
            pass.evaluated += 1;
            match verdict.verdict {
                Verdict::Eligible => pass.eligible += 1,
                Verdict::Likely => pass.likely += 1,
                Verdict::Blocked => pass.blocked += 1,
                Verdict::Unknown => pass.unknown += 1,
            }
            misses.extend(verdict.vocab_misses.iter().cloned());
            items.push((
                fit_key(&profile.key, key),
                fit_record(profile, key, source.as_deref(), &verdict),
            ));
        }
    }

    if !misses.is_empty() {
        // Loud, per the card: a vocabulary blind spot is what turns into a
        // confident false `blocked` the day someone "fixes" the unknown arm.
        let sample: Vec<&str> = misses.iter().take(5).map(String::as_str).collect();
        let warning = format!(
            "grants/fits: {} published applicant term(s) are outside the vocabulary map \
             (e.g. {}) — every row carrying one is `unknown`, never `blocked`",
            misses.len(),
            sample.join(" | ")
        );
        tracing::warn!(misses = misses.len(), sample = ?sample, "applicant vocabulary miss");
        pass.warnings.push(warning);
    }

    if items.is_empty() {
        return Ok(pass);
    }
    let summary = ctx
        .datasets
        .upsert_many_derived(
            UNIFIED_APP,
            FITS_DATASET,
            &items,
            None,
            Some(&stamp(ctx, None)),
            &DerivedPaths::NONE,
        )
        .await?;
    pass.fresh = summary.new.len() + summary.changed.len();
    Ok(pass)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(org: OrgType) -> Profile {
        Profile {
            key: "acme".into(),
            name: "Acme".into(),
            org_type: org,
            country: "US".into(),
            state: Some("CA".into()),
            budget_max: None,
            focus_tags: Vec::new(),
            cost_share_capacity: None,
            programs_watched: Vec::new(),
        }
    }

    fn unified(source: &str) -> Value {
        json!({
            "source": source,
            "source_id": "1",
            "title": "Rural Health Network",
            "status": "open",
            "eligibilities": [],
            "categories": [],
            "award_floor": Value::Null,
        })
    }

    fn detail(requirements: Value) -> Value {
        json!({ "requirements": requirements })
    }

    // ── the named gate of the v1 slice ──

    /// The single most likely fabricated verdict: reading "the profile did not
    /// say" as "the profile cannot", and blocking every match-requiring NOFO.
    #[test]
    fn unknown_cost_share_capacity_is_unknown_not_blocked() {
        let required = json!(true);
        assert_eq!(
            cost_share_gate(Some(&required), None).verdict,
            GateVerdict::Unknown
        );
        assert_eq!(
            cost_share_gate(Some(&required), Some(false)).verdict,
            GateVerdict::Block
        );
        assert_eq!(
            cost_share_gate(Some(&required), Some(true)).verdict,
            GateVerdict::Pass
        );
        // And the grant's own silence is equally unknown, never a pass.
        assert_eq!(
            cost_share_gate(Some(&Value::Null), Some(true)).verdict,
            GateVerdict::Unknown
        );
        assert_eq!(
            cost_share_gate(None, Some(true)).verdict,
            GateVerdict::Unknown
        );
    }

    /// The composed verdict must not launder an unknown gate into a block
    /// either: a match-requiring NOFO whose applicant vocabulary admits the
    /// profile is `likely`, and the missing capacity is named.
    #[test]
    fn a_missing_capacity_makes_the_pair_likely_not_blocked() {
        let p = profile(OrgType::Nonprofit);
        let d = detail(json!({
            "applicant_types": ["Nonprofits having a 501(c)(3) status with the IRS"],
            "cost_sharing": true,
        }));
        let out = fit(&p, &unified("grants-gov"), Some(&d));
        assert_eq!(out.verdict, Verdict::Likely);
        assert!(out.blockers.is_empty(), "{:?}", out.blockers);
        assert!(out
            .unknowns
            .iter()
            .any(|u| u.contains("does not say whether it can meet one")));
    }

    // ── the vocabulary ──

    /// A term the map has never seen makes the WHOLE gate unknown. Dropping the
    /// unmapped term instead — the tempting "filter_map" — turns "3 of 4 terms
    /// meant nothing to us" into a confident `blocked`.
    #[test]
    fn a_vocabulary_miss_is_unknown_not_blocked() {
        let types = json!(["Interplanetary consortia", "State governments"]);
        let gate = applicant_gate(OrgType::Nonprofit, Some(&types), None);
        assert_eq!(gate.verdict, GateVerdict::Unknown);
        assert_eq!(gate.vocab_misses, vec!["Interplanetary consortia"]);
        // And a fully-mapped list that excludes the org type DOES block — the
        // unknown arm must not swallow the real negative.
        let mapped = json!(["State governments", "County governments"]);
        assert_eq!(
            applicant_gate(OrgType::Nonprofit, Some(&mapped), None).verdict,
            GateVerdict::Block
        );
    }

    /// A positive match settles the gate even beside an unmapped sibling term:
    /// the applicant is named, and no term we failed to understand can un-name
    /// it. The miss is still reported so the map's blind spot is not hidden.
    #[test]
    fn a_named_applicant_beats_an_unmapped_sibling_term() {
        let types = json!(["Interplanetary consortia", "Small businesses"]);
        let gate = applicant_gate(OrgType::Smb, Some(&types), None);
        assert_eq!(gate.verdict, GateVerdict::Pass);
        assert_eq!(gate.vocab_misses, vec!["Interplanetary consortia"]);
    }

    /// `applicant_types: null` (the field is absent or drifted) and
    /// `applicant_types: []` (the agency published an empty list) are different
    /// facts, and NEITHER is "nobody is eligible". The grants-gov normalizer
    /// keeps them apart on the write side; this keeps them apart on the read
    /// side.
    #[test]
    fn an_empty_published_list_is_unknown_not_blocked() {
        let empty = json!([]);
        let gate = applicant_gate(OrgType::Gov, Some(&empty), None);
        assert_eq!(gate.verdict, GateVerdict::Unknown);
        assert!(gate.reason.contains("EMPTY"));
        assert_eq!(
            applicant_gate(OrgType::Gov, Some(&Value::Null), None).verdict,
            GateVerdict::Unknown
        );
        assert_eq!(
            applicant_gate(OrgType::Gov, None, None).verdict,
            GateVerdict::Unknown
        );
    }

    /// The order of the vocabulary arms IS the correctness argument: the federal
    /// 501(c)(3) sentence literally contains "institutions of higher education",
    /// so a university-first map files every federal nonprofit as a university.
    #[test]
    fn the_501c3_sentence_maps_to_nonprofit_not_university() {
        let term = "Nonprofits having a 501(c)(3) status with the IRS, other than institutions of higher education";
        assert_eq!(applicant_vocab(term), Some(vec![OrgType::Nonprofit]));
        assert_eq!(
            applicant_vocab("Public and State controlled institutions of higher education"),
            Some(vec![OrgType::University])
        );
        assert_eq!(
            applicant_vocab("Native American tribal governments (Federally recognized)"),
            Some(vec![OrgType::Tribal])
        );
        // The pointer-at-prose term is honestly unmapped, not "everyone".
        assert_eq!(
            applicant_vocab("Others (see text field entitled \"Additional Information\")"),
            None
        );
    }

    // ── geography ──

    /// A profile that did not name a state is a fact about the profile, not
    /// about the grant: the California portal gate is unknown, never blocked.
    #[test]
    fn a_stateless_profile_is_unknown_on_a_state_programme_not_blocked() {
        let mut p = profile(OrgType::Nonprofit);
        p.state = None;
        assert_eq!(
            geography_gate(&p, Some("ca-grants")).verdict,
            GateVerdict::Unknown
        );
        p.state = Some("TX".into());
        assert_eq!(
            geography_gate(&p, Some("ca-grants")).verdict,
            GateVerdict::Block
        );
        p.state = Some("CA".into());
        assert_eq!(
            geography_gate(&p, Some("ca-grants")).verdict,
            GateVerdict::Pass
        );
        // A source with no rule blocks nothing.
        assert_eq!(
            geography_gate(&p, Some("ny-grants")).verdict,
            GateVerdict::Unknown
        );
    }

    /// Third-country participation in Horizon Europe is real and this engine
    /// has no field that decides it — so a non-member is `unknown`, not
    /// `blocked`, while a country mismatch on a NATIONAL programme is a genuine
    /// hard block.
    #[test]
    fn a_non_eu_country_is_unknown_on_eu_sedia_but_blocked_on_a_national_programme() {
        let p = profile(OrgType::Nonprofit);
        assert_eq!(
            geography_gate(&p, Some("eu-sedia")).verdict,
            GateVerdict::Unknown
        );
        let mut cz = p.clone();
        cz.country = "CZ".into();
        assert_eq!(
            geography_gate(&cz, Some("eu-sedia")).verdict,
            GateVerdict::Pass
        );
        assert_eq!(
            geography_gate(&cz, Some("grants-gov")).verdict,
            GateVerdict::Block
        );
    }

    // ── the award filter ──

    /// The award gate can block and can pass, but never returns `Unknown`: a
    /// grant that publishes no amount is un-filterable, not an eligibility
    /// mystery. And a profile with no ceiling declared tests nothing at all.
    #[test]
    fn a_missing_award_amount_does_not_make_eligibility_unknown() {
        let u = unified("grants-gov");
        assert_eq!(
            award_gate(&u, None, Some(50_000.0)).verdict,
            GateVerdict::Pass
        );
        assert_eq!(award_gate(&u, None, None).verdict, GateVerdict::Pass);
        let big = json!({ "award_floor": 1_000_000 });
        assert_eq!(
            award_gate(&big, None, Some(50_000.0)).verdict,
            GateVerdict::Block
        );
        assert_eq!(
            award_gate(&big, None, Some(2_000_000.0)).verdict,
            GateVerdict::Pass
        );
        // The detail corpus is the only place federal amounts exist, so the
        // gate reads it when the unified row is null.
        let d = detail(json!({ "award_floor": 1_000_000 }));
        assert_eq!(
            award_gate(&u, Some(&d), Some(50_000.0)).verdict,
            GateVerdict::Block
        );
    }

    // ── composition ──

    /// `eligible` is reachable only with positive published evidence on every
    /// gate. The same pair with the eligibility fields stripped is `unknown` —
    /// never `eligible`, which is the fabrication this engine exists to refuse.
    #[test]
    fn eligible_needs_published_evidence_and_absence_yields_unknown() {
        let p = profile(OrgType::Nonprofit);
        let d = detail(json!({
            "applicant_types": ["Nonprofits having a 501(c)(3) status with the IRS"],
            "cost_sharing": false,
        }));
        let full = fit(&p, &unified("grants-gov"), Some(&d));
        assert_eq!(full.verdict, Verdict::Eligible);
        assert!(full.unknowns.is_empty(), "{:?}", full.unknowns);

        let bare = fit(&p, &unified("grants-gov"), None);
        assert_eq!(bare.verdict, Verdict::Unknown);
        assert!(bare.blockers.is_empty());
    }

    /// A closed opportunity is blocked whatever else is true, and a blocked pair
    /// scores 0.0 — bands never overlap, so a `likely` can never outrank an
    /// `eligible` in a consumer's sort.
    #[test]
    fn verdict_bands_do_not_overlap() {
        let p = profile(OrgType::Nonprofit);
        let mut closed = unified("grants-gov");
        closed["status"] = json!("closed");
        let out = fit(&p, &closed, None);
        assert_eq!(out.verdict, Verdict::Blocked);
        assert_eq!(out.score, 0.0);

        let d = detail(json!({
            "applicant_types": ["Nonprofits having a 501(c)(3) status with the IRS"],
            "cost_sharing": false,
        }));
        let eligible = fit(&p, &unified("grants-gov"), Some(&d));
        let likely = fit(
            &p,
            &unified("grants-gov"),
            Some(&detail(json!({
                "applicant_types": ["Nonprofits having a 501(c)(3) status with the IRS"],
                "cost_sharing": true,
            }))),
        );
        assert!(eligible.score > likely.score);
        assert!(likely.score >= 0.5 && eligible.score <= 1.0);
    }

    /// The score must be a stable function of the row, or every pass re-hashes
    /// the record and mints a `changed` revision — i.e. a false alert — for
    /// every fit that did not actually move.
    #[test]
    fn the_score_is_stable_to_two_decimals() {
        let mut p = profile(OrgType::Nonprofit);
        p.focus_tags = vec!["rural health".into()];
        p.programs_watched = vec!["aln:93.912".into()];
        let mut u = unified("grants-gov");
        u["program_key"] = json!("aln:93.912");
        let a = fit(&p, &u, None);
        let b = fit(&p, &u, None);
        assert_eq!(a.score, b.score);
        assert_eq!(a.score, (a.score * 100.0).round() / 100.0);
    }

    // ── keys ──

    /// The detail corpus is keyed by the OPPORTUNITY id, not the unified key.
    /// Getting this wrong is silent: every lookup misses and every federal
    /// verdict quietly degrades to `unknown`.
    #[test]
    fn the_detail_key_is_the_opportunity_id_not_the_unified_key() {
        assert_eq!(detail_key("grants-gov:355099").as_deref(), Some("355099"));
        assert_eq!(detail_key("ca-grants:42"), None);
        assert_eq!(detail_key("grants-gov:"), None);
        assert_eq!(
            fit_key("acme", "grants-gov:355099"),
            "acme:grants-gov:355099"
        );
    }

    #[test]
    fn the_profile_key_is_a_slug_not_the_raw_name() {
        assert_eq!(
            profile_key("Acme Health Coalition"),
            "acme-health-coalition"
        );
        assert_eq!(profile_key("  A.C.M.E.  "), "a-c-m-e");
        assert_eq!(profile_key("!!!"), "");
    }

    // ── validation ──

    /// A typo'd field that is silently dropped produces a profile that blocks
    /// nothing and matches everything, and the operator never learns why.
    #[test]
    fn an_unknown_field_is_refused_not_silently_dropped() {
        let body = json!({
            "name": "Acme",
            "org_type": "nonprofit",
            "country": "US",
            "cost_share_capacty": true,
        });
        let errors = validate_profile(&body).unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("cost_share_capacty")),
            "{errors:?}"
        );
    }

    #[test]
    fn validation_canonicalizes_and_states_every_absent_field() {
        let (key, profile) = validate_profile(&json!({
            "name": "Acme Health Coalition",
            "org_type": "NonProfit",
            "country": "us",
            "state": "ca",
            "focus_tags": ["Rural Health", "rural health"],
            "budget_band": { "max": 250000 },
        }))
        .unwrap();
        assert_eq!(key, "acme-health-coalition");
        assert_eq!(profile["org_type"], json!("nonprofit"));
        assert_eq!(profile["country"], json!("US"));
        assert_eq!(profile["state"], json!("CA"));
        assert_eq!(profile["focus_tags"], json!(["rural health"]));
        assert_eq!(
            profile["budget_band"],
            json!({ "min": null, "max": 250000.0 })
        );
        // Absent is written as an explicit null, not omitted: a reader can see
        // the field is unknown rather than guess whether the key was renamed.
        for field in ["ein", "uei", "ntee", "cost_share_capacity"] {
            assert_eq!(profile[field], Value::Null, "{field}");
        }
        // And the round trip lands back in the engine's own shape.
        let parsed = Profile::from_record(&key, &profile).unwrap();
        assert_eq!(parsed.org_type, OrgType::Nonprofit);
        assert_eq!(parsed.budget_max, Some(250_000.0));
        assert_eq!(parsed.cost_share_capacity, None);
    }

    /// A blank or non-string tag is refused, not quietly dropped: a focus list
    /// that silently lost an entry scores every grant slightly wrong forever,
    /// and nothing in the output says which entry went missing.
    #[test]
    fn a_blank_tag_is_refused_not_quietly_dropped() {
        let errors = validate_profile(&json!({
            "name": "Acme",
            "org_type": "nonprofit",
            "country": "US",
            "focus_tags": ["rural health", " "],
        }))
        .unwrap_err();
        assert_eq!(errors, vec!["'focus_tags' must contain non-empty strings"]);
    }

    #[test]
    fn a_bad_org_type_or_country_is_refused() {
        let errors =
            validate_profile(&json!({ "name": "A", "org_type": "charity", "country": "USA" }))
                .unwrap_err();
        assert_eq!(errors.len(), 2, "{errors:?}");
        assert!(errors[0].contains("org_type"));
        assert!(errors[1].contains("country"));
    }

    // ── the pass (I/O half) ──

    async fn seeded(name: &str) -> (pumper_core::testing::TempStore, AppContext) {
        let store = pumper_core::testing::TempStore::new(name).await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "grants-gov").build();
        let corpus: Vec<(String, Value)> = (0..3)
            .map(|i| (format!("grants-gov:{i}"), unified("grants-gov")))
            .collect();
        ctx.datasets
            .upsert_many(UNIFIED_APP, UNIFIED_DATASET, &corpus)
            .await
            .unwrap();
        let (key, profile) = validate_profile(&json!({
            "name": "Acme",
            "org_type": "nonprofit",
            "country": "US",
        }))
        .unwrap();
        ctx.datasets
            .upsert_many(UNIFIED_APP, PROFILES_DATASET, &[(key, profile)])
            .await
            .unwrap();
        (store, ctx)
    }

    /// The pass is delta-driven: handing it one key must evaluate one key. A
    /// pass that quietly widened to the corpus would still be *correct* (the
    /// engine is pure) and would cost `profiles × corpus` reads a day forever,
    /// which is exactly the kind of regression no assertion elsewhere catches.
    #[tokio::test]
    async fn the_delta_pass_does_not_re_evaluate_unchanged_keys() {
        let (_store, ctx) = seeded("grants-fits-delta").await;
        let out = evaluate_fits(&ctx, &["grants-gov:1".to_string()])
            .await
            .unwrap();
        assert_eq!(out.profiles, 1);
        assert_eq!(out.evaluated, 1, "one profile x one delta key");
        assert_eq!(
            out.unknown, 1,
            "no detail corpus, so no applicant vocabulary"
        );
        assert_eq!(out.fresh, 1);

        let rows = ctx
            .datasets
            .list(UNIFIED_APP, FITS_DATASET, 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "only the delta key got a row");
        assert_eq!(rows[0].key, "acme:grants-gov:1");
        assert_eq!(rows[0].data["verdict"], json!("unknown"));
        assert_eq!(rows[0].data["method"], json!(METHOD_DETERMINISTIC));

        // Re-running the same delta writes no news: `fresh` is the alert set,
        // and a re-run is not news.
        let again = evaluate_fits(&ctx, &["grants-gov:1".to_string()])
            .await
            .unwrap();
        assert_eq!(again.evaluated, 1);
        assert_eq!(again.fresh, 0, "an unchanged verdict must not re-alert");
    }

    /// A profile with no readable identity produces NO fits and says so, rather
    /// than fits against a guessed applicant type.
    #[tokio::test]
    async fn an_unreadable_profile_is_skipped_loudly() {
        let (_store, ctx) = seeded("grants-fits-bad-profile").await;
        ctx.datasets
            .upsert_many(
                UNIFIED_APP,
                PROFILES_DATASET,
                &[("broken".to_string(), json!({ "name": "Broken" }))],
            )
            .await
            .unwrap();
        let out = evaluate_fits(&ctx, &["grants-gov:1".to_string()])
            .await
            .unwrap();
        assert_eq!(out.profiles, 1, "only the readable one");
        assert_eq!(out.evaluated, 1);
        assert!(
            out.warnings.iter().any(|w| w.contains("SKIPPED")),
            "{:?}",
            out.warnings
        );
    }

    /// With no profiles stored the pass is free and writes nothing — the
    /// feature is inert until an operator POSTs a profile.
    #[tokio::test]
    async fn no_profiles_means_no_reads_and_no_rows() {
        let store = pumper_core::testing::TempStore::new("grants-fits-empty").await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "grants-gov").build();
        let out = evaluate_fits(&ctx, &["grants-gov:1".to_string()])
            .await
            .unwrap();
        assert_eq!(out.profiles, 0);
        assert_eq!(out.evaluated, 0);
        assert!(!out.wrote());
        assert!(ctx
            .datasets
            .list(UNIFIED_APP, FITS_DATASET, 10)
            .await
            .unwrap()
            .is_empty());
    }
}
