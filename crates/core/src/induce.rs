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

use crate::extract::{FieldRule, Rule, RuleSet};
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
                transforms: Vec::new(),
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
}
