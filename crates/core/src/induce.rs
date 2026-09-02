//! Zero-shot wrapper induction (M09 v1): statistically induce a CANDIDATE
//! [`RuleSet`] from a set of same-template pages — no LLM, no demonstrations,
//! pure-Rust heuristics over the already-parsed `scraper`/ego-tree DOM.
//!
//! The v1 scope is single-page-set induction (the caller supplies the page
//! set; `dom_simhash` clustering is deliberately deferred):
//!
//! 1. **Container candidates** — element signatures (`tag` + stable classes,
//!    build-digest classes like `card-1a2b3c4d` excluded) that repeat at least
//!    `min_instances` times per page, on at least `min_support` of the pages.
//! 2. **Field slots** — descendant paths inside the winning container whose
//!    *structure is fixed* (the same relative tag/class path appears across
//!    instances) while their *text varies* (≥ 2 distinct values — a constant
//!    "Add to cart" is boilerplate, not a field). Anchor `href`s are slots too.
//! 3. **Emission** — a compiled-and-validated `RuleSet` with one top-level
//!    [`Rule::Each`] (`items`), plus per-field support statistics so a human
//!    can judge every slot before the rules are ever deployed.
//!
//! Induced rules are SUGGESTIONS: the caller is expected to review them and
//! validate against the stored corpus (the extractor's replay mode) — this
//! module never touches storage.

use std::collections::{BTreeMap, HashMap, HashSet};

use scraper::{ElementRef, Html, Selector};
use serde::Serialize;

use crate::extract::{FieldRule, Rule, RuleSet, Transform};
use crate::simhash::build_hash_stem;
use crate::Result;

/// Cap on container signatures analyzed in depth (highest instance counts
/// first) and on instances inspected per candidate — keeps induction bounded
/// on pathological pages without changing the verdict on sane ones.
const MAX_CANDIDATES: usize = 40;
const MAX_INSTANCES: usize = 500;

/// Max relative-path depth (item root → slot element) considered a field slot.
const MAX_SLOT_DEPTH: usize = 4;

/// Distinct sample values echoed per field (illustration, not the data).
const SAMPLE_LIMIT: usize = 5;

/// Bare (class-less) tags that may still anchor a repeating container — their
/// tag alone already implies "one item of a list".
const BARE_ITEM_TAGS: [&str; 5] = ["li", "tr", "article", "dd", "option"];

/// Induction thresholds. `min_support` applies both to container candidacy
/// (fraction of pages where the signature repeats) and to field slots
/// (fraction of instances where the slot yields text).
#[derive(Debug, Clone)]
pub struct InduceOptions {
    pub min_support: f64,
    pub min_instances: usize,
    pub max_fields: usize,
}

impl Default for InduceOptions {
    fn default() -> Self {
        Self {
            min_support: 0.6,
            min_instances: 3,
            max_fields: 12,
        }
    }
}

/// One induced field slot with its evidence.
#[derive(Debug, Clone, Serialize)]
pub struct FieldSupport {
    pub name: String,
    /// Relative CSS path from the item root (the `Each` scope).
    pub selector: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attr: Option<String>,
    /// Fraction of instances where the slot yielded a non-empty value.
    pub support: f64,
    /// Distinct values / present instances — 1.0 means every instance differs.
    pub distinct_ratio: f64,
    /// Instances where the slot was present.
    pub instances: usize,
    pub samples: Vec<String>,
}

/// The winning repeating container and its evidence.
#[derive(Debug, Clone, Serialize)]
pub struct ContainerStats {
    /// The `Each` item selector (e.g. `div.card`).
    pub selector: String,
    /// Enclosing listing selector, when one class-bearing parent dominates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub container: Option<String>,
    pub pages: usize,
    /// Pages where the signature repeated at least `min_instances` times.
    pub pages_supported: usize,
    pub support: f64,
    pub instances: usize,
    pub avg_instances: f64,
}

/// A full induction verdict: the candidate rule set plus its evidence.
#[derive(Debug, Clone, Serialize)]
pub struct Induction {
    /// Valid, compile-checked rule set with one top-level `each` field
    /// (`items`) — directly usable as the extractor's `rules` param.
    pub rules: RuleSet,
    pub container: ContainerStats,
    pub fields: Vec<FieldSupport>,
    /// Container signatures that cleared the support gate and were analyzed.
    pub candidates_considered: usize,
}

/// Induces a candidate rule set from `docs` (same-template pages). Returns
/// `Ok(None)` when no repeating container clears the thresholds — an honest
/// "nothing inducible here", never a fabricated guess.
pub fn induce(docs: &[String], opts: &InduceOptions) -> Result<Option<Induction>> {
    if docs.is_empty() {
        return Ok(None);
    }
    let min_support = opts.min_support.clamp(0.05, 1.0);
    let min_instances = opts.min_instances.max(2);
    let max_fields = opts.max_fields.clamp(1, 32);
    let pages: Vec<Html> = docs.iter().map(|d| Html::parse_document(d)).collect();
    let need_pages = ((min_support * pages.len() as f64).ceil() as usize).max(1);

    // Pass 1: signature census — which (tag + stable classes) signatures
    // repeat >= min_instances per page, on >= need_pages pages?
    let mut page_counts: Vec<HashMap<String, usize>> = Vec::with_capacity(pages.len());
    for page in &pages {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for node in page.root_element().descendants() {
            if let Some(el) = ElementRef::wrap(node) {
                if let Some(sig) = candidate_sig(el.value()) {
                    *counts.entry(sig).or_insert(0) += 1;
                }
            }
        }
        page_counts.push(counts);
    }
    let mut totals: HashMap<&str, (usize, usize)> = HashMap::new(); // sig -> (pages_supported, total)
    for counts in &page_counts {
        for (sig, &n) in counts {
            let e = totals.entry(sig.as_str()).or_default();
            e.1 += n;
            if n >= min_instances {
                e.0 += 1;
            }
        }
    }
    let mut candidates: Vec<(String, usize)> = totals
        .into_iter()
        .filter(|(_, (ps, _))| *ps >= need_pages)
        .map(|(sig, (_, total))| (sig.to_string(), total))
        .collect();
    // Most instances first; name breaks ties for deterministic output.
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    candidates.truncate(MAX_CANDIDATES);
    let candidates_considered = candidates.len();

    // Pass 2: analyze each candidate's field slots; keep the best.
    let mut best: Option<CandidateResult> = None;
    for (sig, _) in &candidates {
        let Some(c) = analyze_candidate(
            &pages,
            sig,
            min_instances,
            min_support,
            need_pages,
            max_fields,
        ) else {
            continue;
        };
        let better = match &best {
            None => true,
            // More fields > broader page support > more instances.
            Some(b) => {
                (c.fields.len(), c.pages_supported, c.instances)
                    > (b.fields.len(), b.pages_supported, b.instances)
            }
        };
        if better {
            best = Some(c);
        }
    }
    let Some(win) = best else { return Ok(None) };

    // Emit the rule set and compile it — an induced rule set that does not
    // compile is a bug here, never the caller's problem.
    let mut inner: BTreeMap<String, FieldRule> = BTreeMap::new();
    for f in &win.fields {
        inner.insert(
            f.name.clone(),
            FieldRule {
                rule: Rule::Css {
                    selector: f.selector.clone(),
                    attr: f.attr.clone(),
                    all: false,
                    html: false,
                },
                transforms: induced_transforms(f.attr.as_deref()),
            },
        );
    }
    let mut top: BTreeMap<String, FieldRule> = BTreeMap::new();
    top.insert(
        "items".into(),
        FieldRule {
            rule: Rule::Each {
                selector: win.sig.clone(),
                fields: inner,
                container: win.container.clone(),
            },
            transforms: Vec::new(),
        },
    );
    let rules = RuleSet { fields: top };
    rules.compile()?;

    let pages_n = pages.len();
    Ok(Some(Induction {
        rules,
        container: ContainerStats {
            selector: win.sig,
            container: win.container,
            pages: pages_n,
            pages_supported: win.pages_supported,
            support: round3(win.pages_supported as f64 / pages_n as f64),
            instances: win.instances,
            avg_instances: round3(win.instances as f64 / pages_n as f64),
        },
        fields: win.fields,
        candidates_considered,
    }))
}

struct CandidateResult {
    sig: String,
    container: Option<String>,
    pages_supported: usize,
    instances: usize,
    fields: Vec<FieldSupport>,
}

/// Analyzes one container signature: enumerates its instances, collects field
/// slots (fixed structure, varying text), and detects a dominant enclosing
/// container. `None` when the candidate has no usable field at all.
fn analyze_candidate(
    pages: &[Html],
    sig: &str,
    min_instances: usize,
    min_support: f64,
    need_pages: usize,
    max_fields: usize,
) -> Option<CandidateResult> {
    let sel = Selector::parse(sig).ok()?;
    let mut all: Vec<ElementRef> = Vec::new();
    let mut parent_sigs: HashMap<String, usize> = HashMap::new();
    let mut pages_supported = 0usize;
    let mut instances = 0usize;
    for page in pages {
        let found: Vec<ElementRef> = page.select(&sel).collect();
        if found.len() >= min_instances {
            pages_supported += 1;
        }
        instances += found.len();
        for el in found {
            if all.len() >= MAX_INSTANCES {
                break;
            }
            if let Some(parent) = el.parent().and_then(ElementRef::wrap) {
                if let Some(psig) = class_sig(parent.value()) {
                    *parent_sigs.entry(psig).or_insert(0) += 1;
                }
            }
            all.push(el);
        }
    }
    if pages_supported < need_pages || all.len() < min_instances {
        return None;
    }

    // Slot census: per instance, the FIRST occurrence of each relative path.
    #[derive(Default)]
    struct Slot {
        present: usize,
        distinct: HashSet<String>,
        samples: Vec<String>,
    }
    let mut slots: BTreeMap<(String, Option<String>), Slot> = BTreeMap::new();
    for root in &all {
        let mut seen: HashMap<(String, Option<String>), String> = HashMap::new();
        for node in root.descendants().skip(1) {
            let Some(el) = ElementRef::wrap(node) else {
                continue;
            };
            let Some(path) = rel_path(*root, el) else {
                continue;
            };
            let text = direct_text(el);
            if !text.is_empty() {
                seen.entry((path.clone(), None)).or_insert(text);
            }
            if el.value().name().eq_ignore_ascii_case("a") {
                if let Some(href) = el.value().attr("href") {
                    let href = href.trim();
                    if !href.is_empty() {
                        seen.entry((path, Some("href".into())))
                            .or_insert_with(|| href.to_string());
                    }
                }
            }
        }
        for (key, value) in seen {
            let slot = slots.entry(key).or_default();
            slot.present += 1;
            if slot.samples.len() < SAMPLE_LIMIT && !slot.samples.contains(&value) {
                slot.samples.push(value.clone());
            }
            slot.distinct.insert(value);
        }
    }

    let n = all.len() as f64;
    let mut fields: Vec<FieldSupport> = slots
        .into_iter()
        .filter_map(|((path, attr), slot)| {
            let support = slot.present as f64 / n;
            // Structure fixed (slot present on >= min_support of instances)
            // AND text varies (>= 2 distinct values — constants are chrome).
            if support + f64::EPSILON < min_support || slot.distinct.len() < 2 {
                return None;
            }
            Some(FieldSupport {
                name: String::new(), // assigned below
                selector: path,
                attr,
                support: round3(support),
                distinct_ratio: round3(slot.distinct.len() as f64 / slot.present as f64),
                instances: slot.present,
                samples: slot.samples,
            })
        })
        .collect();
    if fields.is_empty() {
        return None;
    }
    fields.sort_by(|a, b| {
        b.support
            .partial_cmp(&a.support)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.selector.cmp(&b.selector))
            .then_with(|| a.attr.cmp(&b.attr))
    });
    fields.truncate(max_fields);
    assign_names(&mut fields);

    // Enclosing container: a class-bearing parent signature covering
    // min_support of the instances (and distinct from the item itself).
    let total = all.len();
    let container = parent_sigs
        .into_iter()
        .filter(|(p, _)| p != sig)
        .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
        .filter(|(_, count)| *count as f64 / total as f64 + f64::EPSILON >= min_support)
        .map(|(p, _)| p);

    Some(CandidateResult {
        sig: sig.to_string(),
        container,
        pages_supported,
        instances,
        fields,
    })
}

/// Relative CSS path from the item root to `el` (` > `-joined signatures),
/// or `None` when deeper than [`MAX_SLOT_DEPTH`].
fn rel_path(root: ElementRef, el: ElementRef) -> Option<String> {
    let mut segs: Vec<String> = Vec::new();
    let mut cur = el;
    while cur.id() != root.id() {
        segs.push(path_sig(cur.value()));
        if segs.len() > MAX_SLOT_DEPTH {
            return None;
        }
        cur = cur.parent().and_then(ElementRef::wrap)?;
    }
    segs.reverse();
    Some(segs.join(" > "))
}

/// The element's own text (direct text-node children only, trimmed) — a slot's
/// value must be the element's, not a flattened subtree that double-counts
/// deeper slots.
fn direct_text(el: ElementRef) -> String {
    let mut out = String::new();
    for child in el.children() {
        if let Some(t) = child.value().as_text() {
            out.push_str(t);
        }
    }
    out.trim().to_string()
}

/// A class usable in an induced selector: a plain CSS identifier that is NOT a
/// build digest (`card-1a2b3c4d` churns per deploy — a selector built on it is
/// dead on the next build; [`build_hash_stem`] recognizes exactly that shape).
fn usable_class(class: &str) -> bool {
    !class.is_empty()
        && !class.starts_with(|c: char| c.is_ascii_digit() || c == '-')
        && class
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        && build_hash_stem(class).is_none()
}

/// `tag` + up to 2 stable classes, sorted — the path-segment signature.
fn path_sig(el: &scraper::node::Element) -> String {
    let mut classes: Vec<&str> = el.classes().filter(|c| usable_class(c)).collect();
    classes.sort_unstable();
    classes.dedup();
    classes.truncate(2);
    let mut out = el.name().to_ascii_lowercase();
    for c in classes {
        out.push('.');
        out.push_str(c);
    }
    out
}

/// Container-candidate signature: [`path_sig`], but only for elements selective
/// enough to anchor a repeating item — at least one stable class, or a tag
/// whose bare name already means "list item" ([`BARE_ITEM_TAGS`]).
fn candidate_sig(el: &scraper::node::Element) -> Option<String> {
    let sig = path_sig(el);
    let tag = el.name().to_ascii_lowercase();
    if sig.contains('.') || BARE_ITEM_TAGS.contains(&tag.as_str()) {
        Some(sig)
    } else {
        None
    }
}

/// [`path_sig`] restricted to class-bearing signatures — a bare `div` parent
/// is no listing landmark.
fn class_sig(el: &scraper::node::Element) -> Option<String> {
    let sig = path_sig(el);
    sig.contains('.').then_some(sig)
}

/// Heuristic field names from the slot's last path segment (class if present,
/// else a tag mapping); `href` slots get a `_url` suffix. Collisions dedup
/// with `_2`, `_3`, … — names are suggestions for the human to rename.
fn assign_names(fields: &mut [FieldSupport]) {
    let mut used: HashMap<String, usize> = HashMap::new();
    for f in fields {
        let last = f.selector.rsplit(" > ").next().unwrap_or(&f.selector);
        let base = match last.split_once('.') {
            Some((_, classes)) => classes
                .rsplit('.')
                .next()
                .unwrap_or(classes)
                .replace('-', "_"),
            None => match last {
                "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => "heading".into(),
                "a" => "link".into(),
                "time" => "date".into(),
                "img" => "image".into(),
                t => t.replace('-', "_"),
            },
        };
        let name = match f.attr.as_deref() {
            Some("href") if base == "link" => "url".to_string(),
            Some(attr) => format!("{base}_{attr}").replace("href", "url"),
            None => base,
        };
        let n = used.entry(name.clone()).or_insert(0);
        *n += 1;
        f.name = if *n == 1 { name } else { format!("{name}_{n}") };
    }
}

/// The transform chain an induced slot gets for free.
///
/// A link slot is induced from a *raw* `href`, which on most listings is
/// relative (`/item/123`) — a value that means nothing once it leaves the page
/// it was scraped from. Every induced rule set was therefore shipping a `_url`
/// field its user had to notice and fix by hand. URL-bearing attributes get a
/// `url_absolute` transform emitted with them; every other slot keeps the empty
/// chain, because induction suggests structure, not opinions.
fn induced_transforms(attr: Option<&str>) -> Vec<Transform> {
    match attr {
        Some(a) if URL_ATTRS.contains(&a) => vec![Transform::UrlAbsolute],
        _ => Vec::new(),
    }
}

/// Attributes whose value is a URL reference (RFC 3986) rather than free text.
/// `href` is the only one induction emits today ([`analyze_candidate`] collects
/// anchor hrefs); the list is the seam for `src`/`data-href` when it does.
const URL_ATTRS: [&str; 3] = ["href", "src", "poster"];

// ── Tier-0 repair: value → selector inversion (N12 §6.2) ────────────────────
//
// Induction above asks "what repeats on these pages?". Inversion asks a much
// narrower and much more answerable question: **which rule produces THESE
// known values from THIS markup?** The old correct values come from the
// source's own `record_revisions`, so the search has an answer key — which is
// the entire reason a deterministic, zero-dollar repair is possible at all.
//
// For the most common real redesign — the words held still, the class names
// moved — this finds the answer for free. It is deliberately incapable of
// inventing a value: every candidate it emits has already been checked to
// reproduce the known values on every document it was given.

/// Attributes inversion will search for an old value, in preference order.
/// Text is searched first and is not in this list.
const INVERT_ATTRS: [&str; 7] = [
    "href", "src", "content", "datetime", "value", "title", "alt",
];

/// Attributes whose value is a stable identity worth anchoring a selector on —
/// the "semantic anchors" §6.2 prefers over positional paths.
const ANCHOR_ATTRS: [&str; 6] = [
    "itemprop",
    "data-testid",
    "data-test",
    "data-qa",
    "data-field",
    "name",
];

/// Candidate selectors examined per (document, field) before giving up. Bounds
/// a pathological page; a real field has a handful of matches, not hundreds.
const MAX_MATCHES_PER_DOC: usize = 24;

/// Distinct surviving selectors kept per field.
const MAX_SELECTORS_PER_FIELD: usize = 8;

/// Knobs for [`invert`].
#[derive(Debug, Clone)]
pub struct InvertOptions {
    /// Documents a selector must reproduce the known value on before it is
    /// emitted. §6.2's "≥ 5 documents" — the cross-document intersection is
    /// what turns a per-page hack into a rule, so this is the load-bearing
    /// number and lowering it is how a repair overfits.
    pub min_docs: usize,
    /// Distinct rule sets emitted, best first.
    pub max_candidates: usize,
    /// Ancestor levels walked when building a scoped path.
    pub max_depth: usize,
}

impl Default for InvertOptions {
    fn default() -> Self {
        Self {
            min_docs: 5,
            max_candidates: 3,
            max_depth: 3,
        }
    }
}

/// One field's inversion evidence — why a selector was chosen, or why none was.
#[derive(Debug, Clone, Serialize)]
pub struct FieldInversion {
    pub field: String,
    /// Documents where a known old value was available to search for.
    pub docs_with_value: usize,
    /// Selectors that reproduced the known value on EVERY such document,
    /// best first.
    pub selectors: Vec<String>,
    /// Attribute the value was found in (`None` = element text).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attr: Option<String>,
}

/// The full inversion verdict: the candidate rule sets plus per-field evidence.
#[derive(Debug, Clone, Serialize)]
pub struct Inversion {
    /// Compile-checked candidate rule sets, best first. Empty is the honest
    /// answer when nothing survived the cross-document intersection — never a
    /// half-bound rule set that would quietly drop a field.
    pub candidates: Vec<RuleSet>,
    pub fields: Vec<FieldInversion>,
    /// Fields that had known values but no surviving selector.
    pub unresolved: Vec<String>,
}

/// Inverts known-good values into candidate rule sets (Tier 0 repair).
///
/// `old_values[i]` are the last-known-correct field values for `new_docs[i]` —
/// **positional**, so the caller pairs a revision with the body of the same
/// record. A field missing (or blank) for a document is simply not searched
/// there; a field must still clear `min_docs` documents overall.
///
/// Returns an [`Inversion`] whose `candidates` is empty when nothing survived.
/// Emitting nothing is a first-class outcome here: a repair candidate that
/// reproduces the known values on 4 of 9 documents is not a weaker repair, it
/// is a different rule.
pub fn invert(
    old_values: &[BTreeMap<String, String>],
    new_docs: &[String],
    opts: &InvertOptions,
) -> Inversion {
    let empty = Inversion {
        candidates: Vec::new(),
        fields: Vec::new(),
        unresolved: Vec::new(),
    };
    if old_values.len() != new_docs.len() || new_docs.is_empty() {
        return empty;
    }
    let min_docs = opts.min_docs.max(1);
    if new_docs.len() < min_docs {
        return empty;
    }
    let pages: Vec<Html> = new_docs.iter().map(|d| Html::parse_document(d)).collect();

    // Field census: every field with a non-blank known value somewhere.
    let mut field_names: Vec<String> = old_values
        .iter()
        .flat_map(|m| m.keys().cloned())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    field_names.sort();

    let mut evidence: Vec<FieldInversion> = Vec::new();
    let mut unresolved: Vec<String> = Vec::new();
    for field in &field_names {
        let wanted: Vec<Option<&str>> = old_values
            .iter()
            .map(|m| {
                m.get(field)
                    .map(String::as_str)
                    .filter(|v| !v.trim().is_empty())
            })
            .collect();
        let docs_with_value = wanted.iter().filter(|v| v.is_some()).count();
        if docs_with_value < min_docs {
            unresolved.push(field.clone());
            continue;
        }
        let (selectors, attr) = invert_field(&pages, &wanted, opts);
        if selectors.is_empty() {
            unresolved.push(field.clone());
        }
        evidence.push(FieldInversion {
            field: field.clone(),
            docs_with_value,
            selectors,
            attr,
        });
    }

    // A candidate rule set binds EVERY field that resolved. Candidate k takes
    // each field's k-th surviving selector (clamped), so the alternatives are
    // genuinely different bindings rather than the same rule three times.
    let resolved: Vec<&FieldInversion> = evidence
        .iter()
        .filter(|f| !f.selectors.is_empty())
        .collect();
    let mut candidates: Vec<RuleSet> = Vec::new();
    if !resolved.is_empty() {
        let depth = resolved
            .iter()
            .map(|f| f.selectors.len())
            .max()
            .unwrap_or(1)
            .min(opts.max_candidates.max(1));
        let mut seen: HashSet<String> = HashSet::new();
        for k in 0..depth {
            let mut fields: BTreeMap<String, FieldRule> = BTreeMap::new();
            for f in &resolved {
                let sel = f.selectors[k.min(f.selectors.len() - 1)].clone();
                fields.insert(
                    f.field.clone(),
                    FieldRule {
                        rule: Rule::Css {
                            selector: sel,
                            attr: f.attr.clone(),
                            all: false,
                            html: false,
                        },
                        // Deliberately empty: the candidate must reproduce the
                        // RECORDED values byte for byte, and a transform chain
                        // invented here would change them.
                        transforms: Vec::new(),
                    },
                );
            }
            let rules = RuleSet { fields };
            let fingerprint = serde_json::to_string(&rules).unwrap_or_default();
            if !seen.insert(fingerprint) {
                continue;
            }
            // A rule set inversion built and cannot compile is a bug here,
            // never the caller's problem — drop it rather than emit it.
            if rules.compile().is_ok() {
                candidates.push(rules);
            }
        }
    }
    Inversion {
        candidates,
        fields: evidence,
        unresolved,
    }
}

/// One field's inversion: the selectors that reproduce every known value.
fn invert_field(
    pages: &[Html],
    wanted: &[Option<&str>],
    opts: &InvertOptions,
) -> (Vec<String>, Option<String>) {
    // Proposals are seeded from the FIRST document that has a known value —
    // any selector that works everywhere necessarily works there, so seeding
    // from one page loses nothing and bounds the search.
    let Some(seed) = wanted.iter().position(Option::is_some) else {
        return (Vec::new(), None);
    };
    let seed_value = wanted[seed].unwrap();
    let mut proposals: Vec<(u8, String, Option<String>)> = Vec::new();
    for (attr, el) in locate(&pages[seed], seed_value) {
        for (tier, sel) in selector_paths(el, opts.max_depth) {
            proposals.push((tier, sel, attr.clone()));
        }
        if proposals.len() >= MAX_MATCHES_PER_DOC * 6 {
            break;
        }
    }
    // Brittle selectors never enter the intersection: rejecting them here is
    // cheaper than validating them and rejecting them at the gate, and it stops
    // a brittle selector from crowding out a stable one at the same tier.
    proposals.retain(|(_, sel, _)| lint_selector(sel).is_empty());
    // Prefer semantic anchors, then shorter paths, then alphabetical order so
    // the same corpus always yields the same candidate list.
    proposals.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.len().cmp(&b.1.len()))
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    proposals.dedup_by(|a, b| a.1 == b.1 && a.2 == b.2);

    let mut kept: Vec<String> = Vec::new();
    let mut kept_attr: Option<String> = None;
    for (_, sel, attr) in proposals {
        // Mixing text and attribute bindings inside one field is not a thing a
        // `css` rule can express, so the first surviving binding fixes the mode.
        if !kept.is_empty() && kept_attr != attr {
            continue;
        }
        let Ok(parsed) = Selector::parse(&sel) else {
            continue;
        };
        if reproduces_everywhere(pages, wanted, &parsed, attr.as_deref()) {
            kept_attr = attr;
            kept.push(sel);
            if kept.len() >= MAX_SELECTORS_PER_FIELD {
                break;
            }
        }
    }
    (kept, kept_attr)
}

/// Whether `selector` yields exactly the known value on EVERY document that has
/// one. This is the cross-document intersection, and it is the whole guard
/// against a per-page hack: one disagreeing document rejects the selector.
fn reproduces_everywhere(
    pages: &[Html],
    wanted: &[Option<&str>],
    selector: &Selector,
    attr: Option<&str>,
) -> bool {
    for (page, want) in pages.iter().zip(wanted) {
        let Some(want) = want else { continue };
        let Some(el) = page.select(selector).next() else {
            return false;
        };
        // Exactly the runtime's own `css` rendering — a selector validated by a
        // different reader than the one that will run it proves nothing.
        let got = match attr {
            Some(a) => match el.value().attr(a) {
                Some(v) => v.to_string(),
                None => return false,
            },
            None => el.text().collect::<String>().trim().to_string(),
        };
        if got != *want {
            return false;
        }
    }
    true
}

/// Every element in `page` whose text (or a searchable attribute) is exactly
/// `value`, paired with the attribute it was found in (`None` = text).
fn locate<'a>(page: &'a Html, value: &str) -> Vec<(Option<String>, ElementRef<'a>)> {
    let mut out = Vec::new();
    for node in page.root_element().descendants() {
        let Some(el) = ElementRef::wrap(node) else {
            continue;
        };
        if el.text().collect::<String>().trim() == value {
            out.push((None, el));
        } else {
            for a in INVERT_ATTRS {
                if el.value().attr(a) == Some(value) {
                    out.push((Some(a.to_string()), el));
                    break;
                }
            }
        }
        if out.len() >= MAX_MATCHES_PER_DOC {
            break;
        }
    }
    out
}

/// Candidate selectors that address `el`, each with its preference tier
/// (lower = more stable). Semantic anchors first, positional paths last.
fn selector_paths(el: ElementRef, max_depth: usize) -> Vec<(u8, String)> {
    let mut out: Vec<(u8, String)> = Vec::new();
    let e = el.value();
    if let Some(id) = e.id().filter(|id| usable_class(id)) {
        out.push((0, format!("#{id}")));
    }
    for a in ANCHOR_ATTRS {
        if let Some(v) = e.attr(a).filter(|v| quotable(v)) {
            out.push((1, format!("[{a}=\"{v}\"]")));
        }
    }
    let own = path_sig(e);
    if own.contains('.') {
        out.push((2, own.clone()));
    }
    // Scoped paths: anchor the element under a class-bearing ancestor. The
    // descendant combinator (not `>`) survives a wrapper `<div>` being inserted
    // between them, which is one of the mutation classes this must resist.
    let mut cur = el;
    for depth in 0..max_depth {
        let Some(parent) = cur.parent().and_then(ElementRef::wrap) else {
            break;
        };
        if let Some(psig) = class_sig(parent.value()) {
            out.push((3 + depth as u8, format!("{psig} {own}")));
        }
        cur = parent;
    }
    if out.is_empty() && SEMANTIC_TAGS.contains(&e.name()) {
        out.push((9, e.name().to_string()));
    }
    out
}

/// Tags whose bare name is specific enough to be worth trying when an element
/// carries no class, id or anchor attribute at all.
const SEMANTIC_TAGS: [&str; 6] = ["h1", "title", "time", "address", "caption", "figcaption"];

/// Whether an attribute value can be embedded in a `[attr="…"]` selector
/// literally — no quote, backslash or newline to escape.
fn quotable(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 64
        && !v.contains('"')
        && !v.contains('\\')
        && !v.contains(|c: char| c.is_control())
}

// ── Brittle-selector lint (N12 §6.4.2) ──────────────────────────────────────

/// One reason a selector should not be deployed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LintFinding {
    pub selector: String,
    /// Stable machine-readable rule id, e.g. `build_hash_class`.
    pub rule: &'static str,
    pub detail: String,
}

/// Selector shapes a repair candidate must never be built on.
///
/// Document-free by design: these are properties of the selector text, so they
/// can reject a proposal before any document is parsed. Breadth ("matches > 5%
/// of the document's elements") is the one gate that needs a document and lives
/// in [`lint_selector_breadth`].
pub fn lint_selector(selector: &str) -> Vec<LintFinding> {
    let mut out = Vec::new();
    let s = selector.trim();
    let finding = |rule: &'static str, detail: String| LintFinding {
        selector: s.to_string(),
        rule,
        detail,
    };
    if s.is_empty() {
        out.push(finding("empty", "selector is empty".into()));
        return out;
    }
    // Unanchored roots: `body`, `html`, `*`, or a bare structural tag. These
    // match on every page ever written, so a candidate built on one is not a
    // rule about this source at all.
    for part in s.split(',') {
        let last = part
            .trim()
            .rsplit([' ', '>', '+', '~'])
            .next()
            .unwrap_or("")
            .trim();
        if matches!(
            last,
            "*" | "body" | "html" | "div" | "span" | "p" | "li" | "td" | "tr"
        ) {
            out.push(finding(
                "unanchored",
                format!("`{last}` is not selective enough to be a rule"),
            ));
            break;
        }
    }
    // Build-digest class tokens churn on every deploy, so a selector built on
    // one is dead at the next release even though it validates perfectly today.
    for token in s.split(['.', '#', ' ', '>', '[', ']', '"', '=', ':', ',']) {
        if !token.is_empty() && build_hash_stem(token).is_some() {
            out.push(finding(
                "build_hash_class",
                format!("`{token}` looks like a per-build digest"),
            ));
            break;
        }
    }
    // A deep positional chain encodes the layout, not the meaning.
    let nth = s.matches(":nth-child").count() + s.matches(":nth-of-type").count();
    if nth > MAX_POSITIONAL_STEPS {
        out.push(finding(
            "positional_chain",
            format!("{nth} positional steps (max {MAX_POSITIONAL_STEPS})"),
        ));
    }
    out
}

/// `:nth-child`/`:nth-of-type` steps a selector may carry before it is judged
/// positional rather than semantic.
const MAX_POSITIONAL_STEPS: usize = 3;

/// Default breadth ceiling: a field selector matching more than this share of a
/// document's elements is binding to chrome, not to a field.
pub const MAX_SELECTOR_BREADTH: f64 = 0.05;

/// The one lint that needs a document: how much of the page a selector matches.
///
/// Returns a finding when the selector matches more than `max_ratio` of the
/// document's elements on ANY of `docs`. A selector that fails to parse is
/// reported rather than silently passing — an unparseable selector has not been
/// shown to be narrow, and "could not check" is not "fine".
pub fn lint_selector_breadth(selector: &str, docs: &[String], max_ratio: f64) -> Vec<LintFinding> {
    let Ok(parsed) = Selector::parse(selector) else {
        return vec![LintFinding {
            selector: selector.to_string(),
            rule: "unparseable",
            detail: "selector does not parse as CSS".into(),
        }];
    };
    for doc in docs {
        let page = Html::parse_document(doc);
        let total = page
            .root_element()
            .descendants()
            .filter(|n| ElementRef::wrap(*n).is_some())
            .count();
        if total == 0 {
            continue;
        }
        let hits = page.select(&parsed).count();
        let ratio = hits as f64 / total as f64;
        if ratio > max_ratio {
            return vec![LintFinding {
                selector: selector.to_string(),
                rule: "too_broad",
                detail: format!(
                    "matches {hits}/{total} elements ({:.1}% > {:.1}%)",
                    ratio * 100.0,
                    max_ratio * 100.0
                ),
            }];
        }
    }
    Vec::new()
}

fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::{induce, InduceOptions};
    use crate::extract::{extract_one, RuleSet};
    use serde_json::json;

    /// A listing page: `.card` items (name varies, price varies, anchor href
    /// varies but its text is the constant "Details", plus a constant button).
    fn page(cards: &[(&str, &str)]) -> String {
        let items: String = cards
            .iter()
            .map(|(name, price)| {
                format!(
                    "<div class=\"card\"><h3>{name}</h3><div class=\"info\">\
                     <span class=\"price\">{price}</span></div>\
                     <a class=\"more\" href=\"/item/{name}\">Details</a>\
                     <button class=\"buy\">Add to cart</button></div>"
                )
            })
            .collect();
        format!(
            "<html><body><nav><a href=\"/\">Home</a></nav>\
             <div class=\"listing\">{items}</div></body></html>"
        )
    }

    fn corpus() -> Vec<String> {
        vec![
            page(&[
                ("Alpha", "$10"),
                ("Beta", "$20"),
                ("Gamma", "$30"),
                ("Delta", "$40"),
            ]),
            page(&[("Epsilon", "$11"), ("Zeta", "$21"), ("Eta", "$31")]),
            page(&[
                ("Theta", "$12"),
                ("Iota", "$22"),
                ("Kappa", "$32"),
                ("Lambda", "$42"),
            ]),
        ]
    }

    #[test]
    fn induces_each_ruleset_with_container_fields_and_stats() {
        let ind = induce(&corpus(), &InduceOptions::default())
            .unwrap()
            .expect("corpus must induce");
        assert_eq!(ind.container.selector, "div.card");
        assert_eq!(ind.container.container.as_deref(), Some("div.listing"));
        assert_eq!(ind.container.pages, 3);
        assert_eq!(ind.container.pages_supported, 3);
        assert_eq!(ind.container.support, 1.0);
        assert_eq!(ind.container.instances, 11);

        let names: Vec<&str> = ind.fields.iter().map(|f| f.name.as_str()).collect();
        // Varying slots survive: heading text, nested price, anchor href.
        assert!(names.contains(&"heading"), "{names:?}");
        assert!(names.contains(&"price"), "{names:?}");
        assert!(names.contains(&"more_url"), "{names:?}");
        // Constant text is chrome, never a field: the anchor's "Details" and
        // the "Add to cart" button both fail the text-varies gate.
        assert!(!names.contains(&"more"), "{names:?}");
        assert!(!names.contains(&"buy"), "{names:?}");
        for f in &ind.fields {
            assert_eq!(f.support, 1.0, "{}", f.name);
            assert!(f.instances == 11);
            assert!(!f.samples.is_empty());
        }
        // Nested slot keeps its relative path.
        let price = ind.fields.iter().find(|f| f.name == "price").unwrap();
        assert_eq!(price.selector, "div.info > span.price");
    }

    #[test]
    fn induced_rules_round_trip_and_extract() {
        let ind = induce(&corpus(), &InduceOptions::default())
            .unwrap()
            .unwrap();
        // The emitted rule set survives serde (it is what the job result and
        // artifact carry) and runs on the very pages it was induced from.
        let wire = serde_json::to_value(&ind.rules).unwrap();
        let rules: RuleSet = serde_json::from_value(wire).unwrap();
        let compiled = rules.compile().unwrap();
        let out = extract_one(&compiled, &corpus()[0]);
        let items = out["items"].as_array().unwrap();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0]["heading"], json!("Alpha"));
        assert_eq!(items[0]["price"], json!("$10"));
        assert_eq!(items[0]["more_url"], json!("/item/Alpha"));
    }

    #[test]
    fn induced_href_fields_are_absolute_not_relative() {
        // An induced rule set used to hand back `"/item/Alpha"` — a link that
        // means nothing off the page it came from, which every user had to
        // notice and patch by hand. The href slot now carries `url_absolute`.
        use crate::extract::extract_one_with_report_at;
        let ind = induce(&corpus(), &InduceOptions::default())
            .unwrap()
            .unwrap();
        let wire = serde_json::to_value(&ind.rules).unwrap();
        assert_eq!(
            wire["items"]["fields"]["more_url"]["transforms"],
            json!([{"op": "url_absolute"}]),
            "{wire}"
        );
        // Text slots keep the empty chain — induction suggests structure, not
        // opinions about values.
        assert_eq!(wire["items"]["fields"]["heading"].get("transforms"), None);
        assert_eq!(wire["items"]["fields"]["price"].get("transforms"), None);

        // End to end: induce, then run against the very page it came from.
        let compiled = ind.rules.compile().unwrap();
        assert!(compiled.needs_doc_url());
        let (out, report) =
            extract_one_with_report_at(&compiled, &corpus()[0], Some("https://shop.test/list/p1"));
        assert_eq!(
            out["items"][0]["more_url"],
            json!("https://shop.test/item/Alpha")
        );
        assert!(!report.base_url_missing);
    }

    #[test]
    fn too_few_instances_yield_none() {
        // 2 cards per page < min_instances (3): honest None, not a guess.
        let docs = vec![
            page(&[("A", "$1"), ("B", "$2")]),
            page(&[("C", "$3"), ("D", "$4")]),
        ];
        assert!(induce(&docs, &InduceOptions::default()).unwrap().is_none());
    }

    #[test]
    fn low_page_support_yields_none() {
        // Cards repeat on only 1 of 3 pages: 0.33 < 0.6 support.
        let docs = vec![
            corpus().remove(0),
            "<html><body><p>about us</p></body></html>".to_string(),
            "<html><body><p>contact</p></body></html>".to_string(),
        ];
        assert!(induce(&docs, &InduceOptions::default()).unwrap().is_none());
    }

    #[test]
    fn rare_slot_is_filtered_by_min_support() {
        // A `.badge` on a single card out of 12: support 1/12 << 0.6.
        let mut docs = corpus();
        docs[0] = docs[0].replace(
            "<h3>Alpha</h3>",
            "<h3>Alpha</h3><span class=\"badge\">SALE</span>",
        );
        let ind = induce(&docs, &InduceOptions::default()).unwrap().unwrap();
        assert!(
            ind.fields.iter().all(|f| f.name != "badge"),
            "{:?}",
            ind.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
    }

    #[test]
    fn build_digest_classes_never_enter_selectors() {
        // `card-1a2b3c4d` is a build digest (churns per deploy); the stable
        // co-class anchors the selector instead.
        let items: String = (0..4)
            .map(|i| {
                format!(
                    "<div class=\"card-1a2b3c4d item\"><h3>N{i}</h3>\
                     <span class=\"price\">${i}</span></div>"
                )
            })
            .collect();
        let doc = format!("<html><body><div class=\"list\">{items}</div></body></html>");
        let docs = vec![doc.clone(), doc.clone(), doc];
        let ind = induce(&docs, &InduceOptions::default()).unwrap().unwrap();
        assert_eq!(ind.container.selector, "div.item");
        assert!(!serde_json::to_string(&ind.rules)
            .unwrap()
            .contains("1a2b3c4d"));
    }

    #[test]
    fn empty_corpus_yields_none() {
        assert!(induce(&[], &InduceOptions::default()).unwrap().is_none());
    }

    // ── Tier-0 inversion ────────────────────────────────────────────────────

    use super::{invert, lint_selector, lint_selector_breadth, InvertOptions, MAX_SELECTOR_BREADTH};
    use std::collections::BTreeMap;

    /// A detail page: the old markup binds `.price`/`.sku`; the new markup is
    /// the same page after a CSS refactor renamed both class tokens.
    fn detail(i: usize, renamed: bool) -> String {
        let (p, s) = if renamed {
            ("cost-v2", "code-v2")
        } else {
            ("price", "sku")
        };
        format!(
            "<html><body><nav class=\"top\"><span class=\"price\">Sale</span></nav>\
             <div class=\"card\"><h1 class=\"title\">Widget {i}</h1>\
             <span class=\"{p}\">${i}9.00</span>\
             <span class=\"{s}\">SKU-{i:03}</span>\
             <a class=\"more\" href=\"/item/{i}\">Details</a></div></body></html>"
        )
    }

    fn known(i: usize) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("price".to_string(), format!("${i}9.00")),
            ("sku".to_string(), format!("SKU-{i:03}")),
        ])
    }

    #[test]
    fn inversion_rebinds_a_renamed_class_and_reproduces_the_old_values() {
        // The commonest real redesign: the words held still, the class names
        // moved. Tier 0 must answer it for zero dollars.
        let olds: Vec<_> = (0..6).map(known).collect();
        let docs: Vec<String> = (0..6).map(|i| detail(i, true)).collect();
        let out = invert(&olds, &docs, &InvertOptions::default());
        assert!(!out.candidates.is_empty(), "{:?}", out.unresolved);
        assert!(out.unresolved.is_empty(), "{:?}", out.unresolved);

        let compiled = out.candidates[0].compile().unwrap();
        for (i, doc) in docs.iter().enumerate() {
            let v = crate::extract::extract_one(&compiled, doc);
            assert_eq!(v["price"], serde_json::json!(format!("${i}9.00")), "{v}");
            assert_eq!(v["sku"], serde_json::json!(format!("SKU-{i:03}")), "{v}");
        }
    }

    #[test]
    fn a_selector_that_only_works_on_one_page_is_not_a_rule() {
        // THE ANTI-PATTERN: deriving a selector from one document. The nav's
        // `.price` matches everywhere and happens to hold document 0's value,
        // so a single-page inversion would bind to site chrome. The
        // cross-document intersection is the only thing that rejects it.
        let mut docs: Vec<String> = (0..6).map(|i| detail(i, true)).collect();
        docs[0] = docs[0].replace(
            "<span class=\"price\">Sale</span>",
            "<span class=\"price\">$09.00</span>",
        );
        let olds: Vec<_> = (0..6).map(known).collect();
        let out = invert(&olds, &docs, &InvertOptions::default());
        for rules in &out.candidates {
            let wire = serde_json::to_string(rules).unwrap();
            assert!(
                !wire.contains("nav"),
                "bound to site chrome that only matched page 0: {wire}"
            );
        }
        // …and whatever it did bind to still reproduces every known value.
        let compiled = out.candidates[0].compile().unwrap();
        for (i, doc) in docs.iter().enumerate() {
            let v = crate::extract::extract_one(&compiled, doc);
            assert_eq!(v["price"], serde_json::json!(format!("${i}9.00")), "{v}");
        }
    }

    #[test]
    fn inversion_emits_nothing_rather_than_a_partial_binding() {
        // The field was deleted from the site: there is no rule that produces
        // the old values, and saying so is the answer. A candidate that binds
        // two of three fields would be promoted and quietly drop a column.
        let olds: Vec<_> = (0..6).map(known).collect();
        let docs: Vec<String> = (0..6)
            .map(|i| {
                detail(i, true)
                    .replace(&format!("<span class=\"cost-v2\">${i}9.00</span>"), "")
            })
            .collect();
        let out = invert(&olds, &docs, &InvertOptions::default());
        assert!(out.unresolved.contains(&"price".to_string()), "{out:?}");
        for rules in &out.candidates {
            assert!(
                !rules.fields.contains_key("price"),
                "a deleted field must never be bound: {rules:?}"
            );
        }
    }

    #[test]
    fn too_few_documents_yield_no_candidate_at_all() {
        // Below the intersection floor there is no evidence, only a guess.
        let olds: Vec<_> = (0..3).map(known).collect();
        let docs: Vec<String> = (0..3).map(|i| detail(i, true)).collect();
        let out = invert(&olds, &docs, &InvertOptions::default());
        assert!(out.candidates.is_empty());
        // Mispaired inputs are a caller bug, and answering them with a
        // confident rule set would be the worst possible response.
        let out = invert(&olds, &[detail(0, true)], &InvertOptions::default());
        assert!(out.candidates.is_empty());
    }

    #[test]
    fn brittle_selectors_are_linted_out_not_shipped() {
        // §6.4.2, as a predicate. Each of these validates perfectly on today's
        // corpus and is dead (or meaningless) tomorrow.
        for (sel, rule) in [
            ("body", "unanchored"),
            ("div", "unanchored"),
            ("*", "unanchored"),
            (".card > div", "unanchored"),
            (".card-1a2b3c4d .price", "build_hash_class"),
            (
                "div:nth-child(2) > div:nth-child(3) > span:nth-child(1) > b:nth-child(2)",
                "positional_chain",
            ),
        ] {
            let findings = lint_selector(sel);
            assert!(
                findings.iter().any(|f| f.rule == rule),
                "{sel} should trip {rule}, got {findings:?}"
            );
        }
        // A stable, anchored selector passes clean.
        assert!(lint_selector("div.card span.price").is_empty());
        assert!(lint_selector("[itemprop=\"price\"]").is_empty());
    }

    #[test]
    fn a_selector_matching_most_of_the_page_is_too_broad_to_be_a_field() {
        // Padded to a realistic element count: the breadth lint is a RATIO, so
        // on a 12-element toy page a single unique match is already 8% and
        // every selector reads as too broad. A test that "passed" on a page
        // that small would be measuring the fixture, not the lint.
        let filler: String = (0..60).map(|n| format!("<p>line {n}</p>")).collect();
        let docs: Vec<String> = (0..3)
            .map(|i| detail(i, false).replace("</body>", &format!("{filler}</body>")))
            .collect();
        // `p` covers most of the page; `span.price` covers one element in ~75.
        assert!(!lint_selector_breadth("p", &docs, MAX_SELECTOR_BREADTH).is_empty());
        assert!(lint_selector_breadth("span.price", &docs, MAX_SELECTOR_BREADTH).is_empty());
        // "Could not check" is not "fine": an unparseable selector is reported.
        let bad = lint_selector_breadth("span[", &docs, MAX_SELECTOR_BREADTH);
        assert_eq!(bad.first().map(|f| f.rule), Some("unparseable"));
    }
}
