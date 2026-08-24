//! Differential + cost harness for the shared-DOM fingerprint path.
//!
//! Every document used to be HTML-parsed **twice** per run: once by extraction,
//! then again by `signals_batch` purely to fingerprint it.
//! `extract_and_fingerprint_batch` fuses the two into one rayon closure so the
//! DOM is built once and borrowed by both consumers.
//!
//! That is only a performance change if the fingerprints are **byte-identical**
//! to the ones the two-parse path produced. They are persisted in
//! `doc_fingerprints` and diffed against the next run's, so a single bit of
//! drift would not fail anything loudly — it would silently register as a
//! divergence on every key of every source, and corrupt every future verdict.
//! So this file runs a real fixture corpus through both paths and compares the
//! fingerprints, the extracted records and the per-field reports.
//!
//! The `#[ignore]`d tail is the `fingerprint-shared-dom` long lane: both paths
//! over the same corpus, each emitting its half of one lane artifact. The
//! criterion is a ratio (the fused path must not be slower than the two-parse
//! reference), pre-declared in `.lanes/criteria.json` and judged by the
//! certifier — the two halves are separate `#[test]`s, so each writes its own
//! `<lane>--<part>.json` and the certifier merges them. A run where only one
//! half emitted is therefore reported as cannot-see, not as a pass.

mod lane_artifact;

use lane_artifact::Lane;
use pumper_core::extract::{extract_batch_with_report, CompiledRuleSet, DocReport, RuleSet};
use pumper_core::{doc_signals, extract_and_fingerprint_batch, signals_batch, DocSignals};
use serde_json::Value;

/// The tier-3 extraction fixtures — ten real captured pages, 15 KB to 100 KB,
/// with the messy markup (inline scripts, SVG, tables, deep nesting) that a
/// synthetic corpus never has.
const FIXTURE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../evals/tier3-extraction/fixtures"
);

fn fixtures() -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = std::fs::read_dir(FIXTURE_DIR)
        .expect("tier3 fixture directory exists")
        .filter_map(|e| {
            let path = e.ok()?.path();
            (path.extension()? == "html").then_some(())?;
            let name = path.file_name()?.to_string_lossy().to_string();
            Some((name, std::fs::read_to_string(&path).ok()?))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    assert!(out.len() >= 10, "fixture corpus shrank: {}", out.len());
    out
}

/// The rule shapes that exercise every branch of `needs_html` / `needs_json` /
/// `needs_xpath`, so the equivalence claim is not made only for the CSS case
/// that happens to parse HTML anyway.
fn rule_sets() -> Vec<(&'static str, CompiledRuleSet)> {
    let compile = |json: Value| {
        serde_json::from_value::<RuleSet>(json)
            .expect("rule set deserializes")
            .compile()
            .expect("rule set compiles")
    };
    vec![
        (
            "css",
            compile(serde_json::json!({
                "title": { "type": "css", "selector": "title" },
                "headings": { "type": "css", "selector": "h1, h2", "all": true },
                "canonical": { "type": "css", "selector": "link[rel=canonical]", "attr": "href" },
            })),
        ),
        (
            "each",
            compile(serde_json::json!({
                "links": {
                    "type": "each",
                    "selector": "a[href]",
                    "container": "body",
                    "fields": {
                        "href": { "type": "css", "selector": ":scope", "attr": "href" },
                        "text": { "type": "css", "selector": ":scope" },
                    },
                },
            })),
        ),
        (
            // No CSS rule at all: extraction never parses HTML, so before the
            // fusion this shape paid exactly one parse (the fingerprint's) and
            // must still pay exactly one.
            "regex-only",
            compile(serde_json::json!({
                "charset": { "type": "regex", "pattern": "charset=\"?([A-Za-z0-9-]+)", "group": 1 },
            })),
        ),
        (
            "json-only",
            compile(serde_json::json!({
                "anything": { "type": "json", "pointer": "/data/0/name" },
            })),
        ),
    ]
}

/// The path the fusion replaced: extract the batch, then parse every body a
/// second time to fingerprint it. Kept here verbatim as the reference
/// implementation the fused path is diffed against.
fn two_parse_reference(
    rules: &CompiledRuleSet,
    docs: &[String],
) -> Vec<(Value, DocReport, DocSignals)> {
    let reported = extract_batch_with_report(rules, docs);
    let values: Vec<Value> = reported.iter().map(|(v, _)| v.clone()).collect();
    let signals = signals_batch(docs, &values);
    reported
        .into_iter()
        .zip(signals)
        .map(|((v, r), s)| (v, r, s))
        .collect()
}

#[test]
fn a_shared_dom_fingerprints_identically_to_a_second_parse() {
    let corpus = fixtures();
    let docs: Vec<String> = corpus.iter().map(|(_, body)| body.clone()).collect();
    for (label, rules) in rule_sets() {
        let before = two_parse_reference(&rules, &docs);
        let after = extract_and_fingerprint_batch(&rules, &docs);
        assert_eq!(before.len(), after.len(), "[{label}] batch length");
        for (i, ((b_val, b_rep, b_sig), (a_val, a_rep, a_sig))) in
            before.iter().zip(after.iter()).enumerate()
        {
            let name = &corpus[i].0;
            // The criterion that matters: these three u64s are persisted and
            // diffed across runs. Byte-identical, or every future divergence
            // verdict on this source is wrong.
            assert_eq!(b_sig, a_sig, "[{label}] fingerprint drifted on {name}");
            assert_eq!(b_val, a_val, "[{label}] record drifted on {name}");
            assert_eq!(
                serde_json::to_value(b_rep).unwrap(),
                serde_json::to_value(a_rep).unwrap(),
                "[{label}] doc report drifted on {name}"
            );
        }
    }
}

#[test]
fn the_fused_path_agrees_with_the_single_document_entry_point() {
    // `doc_signals` is the public per-document call (used by the health tests
    // and by any app that fingerprints outside a batch). It and the fused batch
    // must not be two implementations of "the fingerprint".
    let corpus = fixtures();
    let docs: Vec<String> = corpus.iter().map(|(_, body)| body.clone()).collect();
    let (_, rules) = rule_sets().remove(0);
    for (i, (values, _, signals)) in extract_and_fingerprint_batch(&rules, &docs)
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            doc_signals(&docs[i], &values),
            signals,
            "doc_signals disagrees with the fused batch on {}",
            corpus[i].0
        );
    }
}

#[test]
fn a_batch_that_shares_its_dom_keeps_its_ordering() {
    // rayon's `map` is order-preserving, and the whole pipeline zips
    // fingerprints back against keys positionally — a reordered batch would
    // attach every fingerprint to the wrong record's key.
    let corpus = fixtures();
    let docs: Vec<String> = corpus.iter().map(|(_, body)| body.clone()).collect();
    let (_, rules) = rule_sets().remove(0);
    let fused = extract_and_fingerprint_batch(&rules, &docs);
    for (i, (_, _, signals)) in fused.iter().enumerate() {
        let expected = doc_signals(&docs[i], &fused[i].0);
        assert_eq!(*signals, expected, "position {i} carries another doc's DOM");
    }
    // Distinct documents produce distinct DOM fingerprints, so an off-by-one
    // would have been caught above rather than passing vacuously.
    let doms: std::collections::HashSet<u64> =
        fused.iter().map(|(_, _, s)| s.dom_simhash).collect();
    assert!(
        doms.len() >= fused.len() - 1,
        "corpus is not discriminating"
    );
}

#[test]
fn an_empty_batch_fingerprints_to_an_empty_batch() {
    let (_, rules) = rule_sets().remove(0);
    assert!(extract_and_fingerprint_batch(&rules, &[]).is_empty());
}

// ---- cost harness (`just test-ignored`) -------------------------------------

/// Copies of the fixture corpus, so the batch is the size a real extractor run
/// fans out over rather than ten documents.
const PERF_REPEATS: usize = 200;

fn perf_corpus() -> Vec<String> {
    let base: Vec<String> = fixtures().into_iter().map(|(_, body)| body).collect();
    let mut docs = Vec::with_capacity(base.len() * PERF_REPEATS);
    for r in 0..PERF_REPEATS {
        for body in &base {
            // Perturbed per repeat so the corpus is not one document the
            // allocator can serve from a single hot arena.
            docs.push(body.replace("</body>", &format!("<!-- run {r} --></body>")));
        }
    }
    docs
}

fn report(label: &str, docs: usize, bytes: usize, elapsed: std::time::Duration) {
    println!(
        "{label}: {docs} docs / {:.1} MB in {:?} ({:.1} docs/s)",
        bytes as f64 / 1_048_576.0,
        elapsed,
        docs as f64 / elapsed.as_secs_f64()
    );
}

#[test]
#[ignore = "long lane `fingerprint-shared-dom` — needs the 2000-document perf corpus; criteria in .lanes/criteria.json, run by `just lanes` and the nightly CI leg"]
fn perf_two_parses_per_document() {
    let docs = perf_corpus();
    let bytes: usize = docs.iter().map(String::len).sum();
    let (_, rules) = rule_sets().remove(0);
    let started = std::time::Instant::now();
    // Mirrors what the extractor actually did, `docs.clone()` included: the
    // clone existed only so the bodies survived extraction for the second parse.
    let out = two_parse_reference(&rules, &docs.clone());
    let elapsed = started.elapsed();
    report("two parses", out.len(), bytes, elapsed);
    emit_half("two-parse", "two_parse_s", out.len(), bytes, elapsed);
}

#[test]
#[ignore = "long lane `fingerprint-shared-dom` — needs the 2000-document perf corpus; criteria in .lanes/criteria.json, run by `just lanes` and the nightly CI leg"]
fn perf_one_shared_parse_per_document() {
    let docs = perf_corpus();
    let bytes: usize = docs.iter().map(String::len).sum();
    let (_, rules) = rule_sets().remove(0);
    let started = std::time::Instant::now();
    let out = extract_and_fingerprint_batch(&rules, &docs);
    let elapsed = started.elapsed();
    report("shared parse", out.len(), bytes, elapsed);
    emit_half("shared-parse", "shared_parse_s", out.len(), bytes, elapsed);
}

/// One half of the `fingerprint-shared-dom` lane artifact.
///
/// Each half names the same workload, because the ratio the certifier judges is
/// only meaningful if both halves saw the same corpus — and a workload described
/// once per artifact is a workload that can silently differ between the two.
fn emit_half(
    part: &'static str,
    scalar: &str,
    docs: usize,
    bytes: usize,
    elapsed: std::time::Duration,
) {
    let mut lane = Lane::new(
        "fingerprint-shared-dom",
        serde_json::json!({
            "documents": docs,
            "bytes": bytes,
            "corpus": "the ten captured tier-3 extraction fixtures (15-100 KB of real, messy markup: inline scripts, SVG, tables, deep nesting), each repeated 200 times with a per-repeat perturbation so the allocator cannot serve the batch from one hot arena",
            "rules": "the `css` rule set — title, all h1/h2, canonical link",
            "shape_fidelity": "REAL for markup shape (captured pages, not synthetic HTML); DECLARED-APPROXIMATE for batch composition, since a live extractor run fans out over documents from one source rather than a round-robin of ten.",
        }),
    )
    .part(part);
    lane.secs(scalar, elapsed)
        .scalar("documents", docs as f64)
        .scalar("bytes", bytes as f64)
        .scalar(
            &format!("{}_docs_per_s", scalar.trim_end_matches("_s")),
            docs as f64 / elapsed.as_secs_f64(),
        );
    lane.emit();
}
