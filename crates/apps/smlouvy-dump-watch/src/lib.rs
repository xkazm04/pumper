//! Pumper app: Registr smluv (Czech contract register) dump-index watcher.
//!
//! `data.smlouvy.gov.cz/index.xml` is the authoritative listing of every monthly
//! bulk dump the Ministry publishes — one `<dump>` per (year, month), each with a
//! sha1 hash, byte size, generation timestamp, and the dump's download URL. The
//! Ministry re-generates a month's dump when late contracts land, which changes
//! its hash and size while its (year, month) stays the same.
//!
//! This app parses that index into one record per dump, keyed by the dump URL,
//! and syncs it as a FULL SNAPSHOT — so a consumer (e.g. tender-radar's Registr
//! smluv ingest) learns precisely **which dump is new or re-generated**, with its
//! sha1 and size, instead of the generic `watch` app's "the index page changed"
//! fingerprint. A `dataset` trigger on `on_change=fresh` can fan a re-download of
//! exactly the changed dumps.
//!
//! It deliberately does NOT download the (large, ~100 MB) dumps — the heavy
//! fetch + parse belongs in the consuming app; this only answers "what dumps
//! exist and which changed?".
//!
//! Params: `{ "index_url": "https://data.smlouvy.gov.cz/index.xml", "year_from": null }`
//!   · `index_url` — override the index location (default is the production URL).
//!   · `year_from` — OPTIONAL: keep only dumps whose `rok` ≥ this, so a consumer
//!                   that only cares about recent months doesn't churn on the full
//!                   2016→now history (~100+ dumps). Omitted → all dumps.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Provenance, Result,
    ScrapeApp,
};
use serde_json::{json, Value};

pub struct SmlouvyDumpWatch;

const DEFAULT_INDEX_URL: &str = "https://data.smlouvy.gov.cz/index.xml";

/// One `<dump>` entry parsed from the index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dump {
    year: u32,
    month: u32,
    /// sha1 hex of the dump file (the `hashDumpu` element text).
    hash: String,
    /// Dump size in bytes (`velikostDumpu`).
    size_bytes: u64,
    /// Generation timestamp as published (`casGenerovani`, RFC-3339-ish).
    generated_at: String,
    /// Absolute download URL (`odkaz`) — the stable natural key.
    url: String,
}

impl Dump {
    /// The record persisted for this dump.
    fn record(&self) -> Value {
        json!({
            "year": self.year,
            "month": self.month,
            "period": format!("{:04}-{:02}", self.year, self.month),
            "hash": self.hash,
            "hash_algo": "sha1",
            "size_bytes": self.size_bytes,
            "generated_at": self.generated_at,
            "url": self.url,
        })
    }
}

/// Text of the first `<tag>…</tag>` in `block` (attributes on the open tag are
/// tolerated, e.g. `<hashDumpu algoritmus="sha1">…`). Returns the trimmed inner
/// text, or `None` if the element is absent. Namespace-prefix agnostic because the
/// index uses a default namespace (no element prefixes).
fn tag_text<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}");
    let start = block.find(&open)?;
    // Skip to the end of the (possibly attribute-bearing) open tag.
    let after_open = start + block[start..].find('>')? + 1;
    let close = format!("</{tag}>");
    let end = block[after_open..].find(&close)? + after_open;
    Some(block[after_open..end].trim())
}

/// The share of the index's `<dump>` blocks that must parse into a record before
/// a run may write a FULL SNAPSHOT — whose removal detection tombstones every
/// key absent from the batch.
///
/// **1.0: every block, no exceptions.** The floor is deliberately at the tight
/// end of the range the direction allowed, because of the asymmetry between the
/// two ways of being wrong here:
///
/// - The index is small (~120 blocks), machine-generated from one government
///   schema that has been stable since 2016, and every block carries the same
///   six elements. A block that does not parse is therefore *schema drift or a
///   truncated document*, not the routine noise a model-authored roster has.
///   There is no "one dump legitimately has no `<odkaz>`" case to tolerate.
/// - Suppressing removal detection costs nothing but a stale pointer to a month
///   the Ministry retired (the upsert still refreshes everything present).
///   Tombstoning wrongly costs a downstream consumer its entire dump index.
///
/// **The tombstone path stays reachable**: a genuinely shrinking feed — the
/// Ministry retiring 2016 — publishes fewer blocks that all still parse, so the
/// share is 1.0, the snapshot write runs and the retired dumps are removed.
/// Only a *garbled* feed suppresses removals. Pinned by
/// `a_shrinking_but_clean_index_still_tombstones`.
const PARSE_FLOOR: f64 = 1.0;

/// What [`parse_dumps`] **saw**, not only what it kept.
///
/// The whole defect this type exists for was that the old signature returned
/// `Vec<Dump>`: a 30-of-51 parse and a 30-of-30 parse were indistinguishable to
/// every caller, so the destructive full-snapshot write could not tell them
/// apart and neither could the operator reading the result JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct IndexParse {
    /// The dumps that parsed, in document order.
    dumps: Vec<Dump>,
    /// `<dump>…</dump>` blocks found in the document, parsed or skipped.
    blocks_seen: usize,
    /// Blocks skipped because `<odkaz>` was absent or empty.
    skipped_missing_url: usize,
    /// Blocks skipped because `<rok>`/`<mesic>` were absent or unparseable.
    skipped_unparseable_date: usize,
}

impl IndexParse {
    fn parsed(&self) -> usize {
        self.dumps.len()
    }

    fn skipped(&self) -> usize {
        self.skipped_missing_url + self.skipped_unparseable_date
    }

    /// Parsed / seen. An index with no `<dump>` blocks at all is complete (1.0) —
    /// nothing was published, so nothing was lost. (That case is refused
    /// separately, by the empty guard in `run`.)
    fn share(&self) -> f64 {
        if self.blocks_seen == 0 {
            return 1.0;
        }
        self.parsed() as f64 / self.blocks_seen as f64
    }

    /// Whether this batch is a **subset** of the published index rather than the
    /// whole of it — i.e. whether a full-snapshot write would tombstone dumps
    /// that are still live upstream.
    fn is_partial(&self) -> bool {
        self.share() < PARSE_FLOOR
    }

    /// The `parse` block of the result: what the index held, what we read out of
    /// it, and why the rest was dropped.
    fn to_json(&self) -> Value {
        json!({
            "blocks_seen": self.blocks_seen,
            "parsed": self.parsed(),
            "skipped": self.skipped(),
            "skipped_missing_url": self.skipped_missing_url,
            "skipped_unparseable_date": self.skipped_unparseable_date,
            // 3 dp: enough to read, short of float noise in a stored result.
            "share": (self.share() * 1000.0).round() / 1000.0,
            "floor": PARSE_FLOOR,
            "partial": self.is_partial(),
        })
    }

    /// The one-line `warnings[]` entry a lossy parse contributes, or `None` when
    /// every block parsed. Separate from [`removal_suppression_reason`] so a
    /// future looser floor still *reports* the skips it tolerates.
    fn warning(&self) -> Option<String> {
        (self.skipped() > 0).then(|| {
            format!(
                "partial index parse: {} of {} <dump> blocks parsed ({} skipped: {} missing \
                 <odkaz>, {} with an unparseable <rok>/<mesic>) — the feed may be truncated or \
                 its schema changed",
                self.parsed(),
                self.blocks_seen,
                self.skipped(),
                self.skipped_missing_url,
                self.skipped_unparseable_date,
            )
        })
    }
}

/// **The floor on a full-snapshot write.** `Some(reason)` when removal detection
/// must be skipped because this batch is only part of the published index;
/// `None` when the parse earned the right to tombstone.
///
/// Pure, so the floor is testable without a store — and named, so the fix is
/// guarded rather than buried in `run()`. Core's own protection against a
/// partial batch cannot engage here: `sync_many`'s doc says `detect_removed`
/// "already refuses an *empty* batch; a partial batch is the case that guard
/// does not cover", the health downgrade needs `[resilience] enforce` (off by
/// default, documented inert) AND `observe_extraction` calls this app does not
/// make. So the floor lives at the app layer, where the block count is known.
fn removal_suppression_reason(parse: &IndexParse) -> Option<String> {
    parse.is_partial().then(|| {
        format!(
            "removal detection suppressed: only {} of {} <dump> blocks parsed ({:.0}% < {:.0}% \
             floor), so this batch is a SUBSET of the published index — the dumps missing from \
             it are kept rather than tombstoned",
            parse.parsed(),
            parse.blocks_seen,
            parse.share() * 100.0,
            PARSE_FLOOR * 100.0,
        )
    })
}

/// The subset of a parse this run **tracks**, and what the narrowing left out.
///
/// The exclusion count is the safety-relevant fact about a `year_from` run, and
/// it exists nowhere else: [`IndexParse`] is built *before* the window is
/// applied, so no field of it can ever carry this number (see
/// [`TrackedWindow::suppression_reason`]).
struct TrackedWindow<'a> {
    /// The dumps kept, in document order.
    dumps: Vec<&'a Dump>,
    /// The window this run was scoped to, or `None` for the whole history.
    year_from: Option<u32>,
    /// Parsed dumps the window left out. `0` with no window at all — and also
    /// when a window happens to exclude nothing, which is the case that keeps
    /// the tombstone path reachable.
    excluded: usize,
}

impl<'a> TrackedWindow<'a> {
    /// Applies `year_from` to a parse. Named, and returning the count rather
    /// than only the survivors, because "how many did we drop?" is precisely the
    /// question the write below has to answer and the old inline `filter` threw
    /// away.
    fn of(parse: &'a IndexParse, year_from: Option<u32>) -> Self {
        let dumps: Vec<&Dump> = match year_from {
            Some(y) => parse.dumps.iter().filter(|d| d.year >= y).collect(),
            None => parse.dumps.iter().collect(),
        };
        Self {
            excluded: parse.parsed().saturating_sub(dumps.len()),
            dumps,
            year_from,
        }
    }

    /// **The second floor on a full-snapshot write — the one the parse floor
    /// structurally cannot reach.** `Some(reason)` when `year_from` narrows this
    /// run to a SUBSET of the index it is about to write as a full snapshot.
    ///
    /// The two floors measure different things and neither can see the other.
    /// [`removal_suppression_reason`] is a *document-fidelity* measure ("did we
    /// read every block the feed published?"), computed from an [`IndexParse`]
    /// that is built before the window exists. `year_from` is a *request-scoping*
    /// measure ("which of the blocks we read do we want?"). A clean 120-of-120
    /// parse with `year_from: 2024` has `share() == 1.0`, so the parse floor
    /// passes it — and then the snapshot write tombstones the ~96 pre-2024 dumps
    /// that are still live upstream. That is not a gap in the floor to be
    /// widened; the two guards are orthogonal by construction.
    ///
    /// **Why this was worse than "rows go missing": they come back.** `dumps` is
    /// ONE shared dataset. The scheduled daily run (no window) and any consumer
    /// run with a `year_from` alternated tombstoning and resurrecting the
    /// excluded dumps, and every resurrection lands in `fresh_dumps` — which this
    /// app's own manifest tells a dataset trigger to fan out as a targeted
    /// re-download of ~100 MB files. Each flip is ~10 GB of downstream traffic,
    /// and two consumers with different `year_from` values is a supported
    /// configuration today.
    ///
    /// Keyed on what the window **actually excludes**, not on
    /// `year_from.is_some()`: a window that excludes nothing produces exactly the
    /// batch an unwindowed run would, so it keeps the right to tombstone. That
    /// matters — a consumer pinned at `year_from: 2016` must still see the
    /// Ministry retiring 2016.
    fn suppression_reason(&self) -> Option<String> {
        let year_from = self.year_from?;
        (self.excluded > 0).then(|| {
            format!(
                "removal detection suppressed: year_from={year_from} narrows this run to {} of \
                 the {} dumps parsed, so this batch is a SUBSET of the published index — the {} \
                 dumps outside the window are kept rather than tombstoned. A full-snapshot write \
                 would delete them, and the next run without a window would resurrect every one \
                 into fresh_dumps",
                self.dumps.len(),
                self.dumps.len() + self.excluded,
                self.excluded,
            )
        })
    }
}

/// Parse the dump index XML into dumps, in document order, **counting what it
/// skipped**. Pure + unit-tested: a flat, stable government schema, so a scoped
/// tag scan beats pulling in an XML dependency.
///
/// Entries missing a URL or with an unparseable year/month are still skipped
/// rather than failing the whole run (one malformed block must not cost the
/// other 120), but the skip is now *counted* by reason — silent skipping is what
/// let a partially-garbled feed reach a full-snapshot write and tombstone the
/// dumps it failed to read.
fn parse_dumps(xml: &str) -> IndexParse {
    let mut out = IndexParse::default();
    let mut i = 0usize;
    while let Some(rel) = xml[i..].find("<dump>") {
        let start = i + rel + "<dump>".len();
        let Some(rel_end) = xml[start..].find("</dump>") else {
            break;
        };
        let block = &xml[start..start + rel_end];
        i = start + rel_end + "</dump>".len();
        out.blocks_seen += 1;

        let url = match tag_text(block, "odkaz") {
            Some(u) if !u.is_empty() => u.to_string(),
            _ => {
                out.skipped_missing_url += 1;
                continue;
            }
        };
        let (Some(year), Some(month)) = (
            tag_text(block, "rok").and_then(|s| s.parse::<u32>().ok()),
            tag_text(block, "mesic").and_then(|s| s.parse::<u32>().ok()),
        ) else {
            out.skipped_unparseable_date += 1;
            continue;
        };
        out.dumps.push(Dump {
            year,
            month,
            hash: tag_text(block, "hashDumpu").unwrap_or("").to_string(),
            size_bytes: tag_text(block, "velikostDumpu")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
            generated_at: tag_text(block, "casGenerovani").unwrap_or("").to_string(),
            url,
        });
    }
    out
}

#[async_trait]
impl ScrapeApp for SmlouvyDumpWatch {
    fn name(&self) -> &'static str {
        "smlouvy-dump-watch"
    }

    fn description(&self) -> &'static str {
        "Watches the Registr smluv (Czech contract register) bulk-dump index at \
         data.smlouvy.gov.cz/index.xml. Parses it into one change-detected record \
         per monthly dump (year, month, sha1, size, download URL), keyed by dump \
         URL and synced as a full snapshot — so a consumer learns exactly which \
         dump is new or re-generated (hash change) and can fan out a targeted \
         re-download via a dataset trigger. Does not download the (~100 MB) dumps \
         themselves. Params: {\"index_url\": \"…/index.xml\", \"year_from\": null \
         (optional: keep only dumps with rok >= year_from)}"
    }

    fn schedule(&self) -> Option<&'static str> {
        // 05:30:00 daily. The Ministry regenerates dumps overnight; a daily check
        // is ample and cheap (one small XML fetch).
        Some("0 30 5 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "index_url": DEFAULT_INDEX_URL })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "index_url": {
                        "type": "string",
                        "description": "Dump-index location. Defaults to the production \
                                        https://data.smlouvy.gov.cz/index.xml."
                    },
                    "year_from": {
                        "type": "integer",
                        "minimum": 2016,
                        "description": "Keep only dumps whose `rok` >= this year, so a consumer \
                                        that only cares about recent months doesn't track the \
                                        full 2016→now history. Omitted = all dumps. A windowed \
                                        run NEVER tombstones: whenever the window actually \
                                        excludes something, the write is downgraded to \
                                        upsert-only (reported in `removals_suppressed`), so the \
                                        dumps outside the window are KEPT, not deleted. `dumps` \
                                        is one shared dataset, so this is what stops two \
                                        consumers with different windows from alternately \
                                        deleting and resurrecting each other's dumps. Only a run \
                                        without a window may tombstone."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Watch the whole published history (the scheduled daily run)",
                    params: json!({ "index_url": DEFAULT_INDEX_URL }),
                },
                ManifestExample {
                    description: "Track only dumps from 2024 onward — a consumer that re-ingests \
                                  recent months and doesn't want the 2016→2023 backlog",
                    params: json!({ "year_from": 2024 }),
                },
            ],
            output_shape: Some(
                "{index_url, dumps_in_index, dumps_parsed, dumps_tracked, year_from, \
                 parse: {blocks_seen, parsed, skipped, skipped_missing_url, \
                 skipped_unparseable_date, share, floor, partial}, warnings: [string], new, \
                 changed, unchanged, removed, removals_suppressed, fresh_dumps[], \
                 newest_period, newest_url} — full-snapshot sync of the `dumps` dataset keyed \
                 by dump URL; `dumps_in_index` is the number of <dump> blocks SEEN and \
                 `dumps_parsed` how many of them parsed, so a partial parse is visible; TWO \
                 orthogonal floors downgrade the write to upsert-only and each names itself in \
                 `removals_suppressed` — a partial parse (so it cannot tombstone the dumps it \
                 failed to read) and a `year_from` window that actually excludes dumps (so a \
                 request-scoped run cannot tombstone the dumps it merely scoped out, and cannot \
                 flip them against an unwindowed run); `dumps_tracked` vs `dumps_parsed` is what \
                 the window excluded; `fresh_dumps` are the new/re-generated dump URLs a dataset \
                 trigger should re-download",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let index_url = ctx
            .params
            .get("index_url")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_INDEX_URL)
            .to_string();
        let year_from = ctx
            .params
            .get("year_from")
            .and_then(Value::as_u64)
            .map(|y| y as u32);

        let response = ctx.engines.http.fetch(HttpRequest::get(&index_url)).await?;
        if !response.is_success() {
            return Err(Error::App(format!(
                "Registr smluv index {index_url} returned status {}",
                response.status
            )));
        }
        ctx.save_artifact("index.xml", response.body.as_bytes())
            .await?;

        let parse = parse_dumps(&response.body);
        if parse.dumps.is_empty() {
            return Err(Error::App(format!(
                "no <dump> entries parsed from {index_url} — the feed may be empty or its \
                 schema changed ({} <dump> blocks seen, {} skipped: {} missing <odkaz>, {} \
                 with an unparseable <rok>/<mesic>)",
                parse.blocks_seen,
                parse.skipped(),
                parse.skipped_missing_url,
                parse.skipped_unparseable_date,
            )));
        }
        let mut warnings: Vec<String> = Vec::new();
        if let Some(w) = parse.warning() {
            warnings.push(w);
        }

        // `year_from` filters what we TRACK; it never changes what the index was
        // seen to hold, so the parse floor is judged on the parse, not on this.
        // The window has its OWN floor — see `TrackedWindow::suppression_reason`.
        let window = TrackedWindow::of(&parse, year_from);

        // Full snapshot: the index IS the complete current listing, so a dump that
        // vanishes (the Ministry retiring a month) is a real `removed`. Keyed by the
        // dump URL — a re-generated month keeps its URL and surfaces as `changed`
        // because its hash/size differ.
        //
        // ...unless one of the two floors says this batch is only PART of that
        // listing, in which case removal detection would tombstone dumps that are
        // still live upstream — the ones we failed to read (the parse floor) or
        // the ones we deliberately scoped out (the window floor).
        let items: Vec<(String, Value)> = window
            .dumps
            .iter()
            .map(|d| (d.url.clone(), d.record()))
            .collect();
        // Provenance (M12): every record is parsed out of THIS index document,
        // so a batch-level `source_url` is a fact here, not an approximation.
        let prov = Provenance {
            source_url: Some(index_url.clone()),
            ..Provenance::default()
        };
        // Two ORTHOGONAL floors; either one alone downgrades the write to
        // upsert-only, and both are reported when both apply.
        let suppressions: Vec<String> = [
            removal_suppression_reason(&parse),
            window.suppression_reason(),
        ]
        .into_iter()
        .flatten()
        .collect();
        for reason in &suppressions {
            tracing::warn!(dataset = "dumps", "{reason}");
            warnings.push(reason.clone());
        }
        let removals_suppressed: Option<String> =
            (!suppressions.is_empty()).then(|| suppressions.join(" | "));
        let summary = match &removals_suppressed {
            Some(_) => {
                ctx.upsert_many_with_provenance("dumps", &items, prov)
                    .await?
            }
            None => ctx.sync_many_with_provenance("dumps", &items, prov).await?,
        };

        // The freshly-changed dumps are the actionable ingest targets — a dataset
        // trigger reads these keys from `_trigger` and re-downloads exactly them.
        let fresh_urls: Vec<&str> = summary.fresh_keys().map(String::as_str).collect();
        let newest = window.dumps.iter().max_by_key(|d| (d.year, d.month));

        Ok(json!({
            "index_url": index_url,
            // Blocks SEEN in the index — what its name has always promised. The
            // count of blocks we managed to read is `dumps_parsed`; before they
            // were split, a 30-of-51 run and a 30-of-30 run emitted identical JSON.
            "dumps_in_index": parse.blocks_seen,
            "dumps_parsed": parse.parsed(),
            "parse": parse.to_json(),
            "warnings": warnings,
            "removals_suppressed": removals_suppressed,
            "dumps_tracked": window.dumps.len(),
            "year_from": year_from,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "removed": summary.removed.len(),
            "fresh_dumps": fresh_urls,
            "newest_period": newest.map(|d| format!("{:04}-{:02}", d.year, d.month)),
            "newest_url": newest.map(|d| d.url.clone()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A realistic slice of the real feed: default namespace, hashDumpu with an
    // attribute, two months, one with a later generation time.
    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<index xmlns="http://portal.gov.cz/rejstriky/ISRS/1.2/">
  <dump>
    <mesic>6</mesic><rok>2026</rok>
    <hashDumpu algoritmus="sha1">aaaa1111bbbb2222cccc3333dddd4444eeee5555</hashDumpu>
    <velikostDumpu>84123456</velikostDumpu>
    <casGenerovani>2026-07-01T00:11:51+02:00</casGenerovani>
    <dokoncenyMesic>1</dokoncenyMesic>
    <odkaz>https://data.smlouvy.gov.cz/dump_2026_06.xml</odkaz>
  </dump>
  <dump>
    <mesic>1</mesic><rok>2017</rok>
    <hashDumpu algoritmus="sha1">66dc71395aaf41aa8563da9c895d678c7b3466b4</hashDumpu>
    <velikostDumpu>72499846</velikostDumpu>
    <casGenerovani>2026-04-22T00:07:04+02:00</casGenerovani>
    <dokoncenyMesic>1</dokoncenyMesic>
    <odkaz>https://data.smlouvy.gov.cz/dump_2017_01.xml</odkaz>
  </dump>
</index>"#;

    /// Both params in the schema are ones `run` actually reads, and the
    /// scheduled default is a valid instance of it.
    #[test]
    fn manifest_declares_the_params_run_reads_and_defaults_fit_it() {
        let m = SmlouvyDumpWatch.manifest();
        let schema = m.params_schema.expect("schema declared");
        let props = schema["properties"].as_object().expect("properties");
        assert!(props.contains_key("index_url") && props.contains_key("year_from"));
        assert_eq!(props.len(), 2, "no param the code never reads");
        assert_eq!(m.examples.len(), 2);
        for ex in &m.examples {
            for k in ex.params.as_object().expect("object").keys() {
                assert!(props.contains_key(k), "example uses undeclared param '{k}'");
            }
        }
        for k in SmlouvyDumpWatch
            .default_params()
            .as_object()
            .expect("object")
            .keys()
        {
            assert!(
                props.contains_key(k),
                "default_params param '{k}' undeclared"
            );
        }
    }

    #[test]
    fn parses_every_dump_with_fields() {
        let parse = parse_dumps(SAMPLE);
        let dumps = &parse.dumps;
        assert_eq!(dumps.len(), 2);
        assert_eq!(parse.blocks_seen, 2, "both blocks were seen");
        assert_eq!(parse.skipped(), 0);
        assert!(!parse.is_partial(), "a clean parse may write a snapshot");
        let d = &dumps[0];
        assert_eq!((d.year, d.month), (2026, 6));
        assert_eq!(d.hash, "aaaa1111bbbb2222cccc3333dddd4444eeee5555");
        assert_eq!(d.size_bytes, 84123456);
        assert_eq!(d.url, "https://data.smlouvy.gov.cz/dump_2026_06.xml");
        assert_eq!(d.generated_at, "2026-07-01T00:11:51+02:00");
    }

    #[test]
    fn record_shape_carries_period_and_key_fields() {
        let rec = parse_dumps(SAMPLE).dumps[0].record();
        assert_eq!(rec["period"], "2026-06");
        assert_eq!(rec["hash_algo"], "sha1");
        assert_eq!(rec["size_bytes"], 84123456);
        assert_eq!(rec["url"], "https://data.smlouvy.gov.cz/dump_2026_06.xml");
    }

    #[test]
    fn tag_text_tolerates_attributes_and_missing_tags() {
        let block = r#"<hashDumpu algoritmus="sha1">abc123</hashDumpu><rok>2026</rok>"#;
        assert_eq!(tag_text(block, "hashDumpu"), Some("abc123"));
        assert_eq!(tag_text(block, "rok"), Some("2026"));
        assert_eq!(tag_text(block, "velikostDumpu"), None);
    }

    /// Keeping only the complete entry is still right — but the skip must now be
    /// **counted by reason**. Silently dropping two of three blocks and returning
    /// a bare `Vec` is what let a garbled feed reach the full-snapshot write.
    #[test]
    fn skips_entries_missing_url_or_date() {
        let xml = r#"
          <dump><mesic>3</mesic><rok>2025</rok><odkaz>https://x/dump_2025_03.xml</odkaz></dump>
          <dump><mesic>4</mesic><rok>2025</rok></dump>
          <dump><rok>2025</rok><odkaz>https://x/no_month.xml</odkaz></dump>
        "#;
        let parse = parse_dumps(xml);
        assert_eq!(parse.dumps.len(), 1, "only the complete entry is kept");
        assert_eq!(parse.dumps[0].url, "https://x/dump_2025_03.xml");

        assert_eq!(parse.blocks_seen, 3, "the index published three blocks");
        assert_eq!(parse.skipped(), 2, "and two of them were skipped, not lost");
        assert_eq!(parse.skipped_missing_url, 1, "the one with no <odkaz>");
        assert_eq!(parse.skipped_unparseable_date, 1, "the one with no <mesic>");
        assert!(parse.warning().is_some(), "a lossy parse warns");
    }

    /// The anti-pattern this whole change exists for: a 1-of-3 parse must NOT be
    /// allowed to write a full snapshot, because removal detection would tombstone
    /// the two dumps we merely failed to read.
    #[test]
    fn a_partial_parse_is_not_a_snapshot() {
        let xml = r#"
          <dump><mesic>3</mesic><rok>2025</rok><odkaz>https://x/a.xml</odkaz></dump>
          <dump><mesic>4</mesic><rok>2025</rok></dump>
          <dump><rok>2025</rok><odkaz>https://x/no_month.xml</odkaz></dump>
        "#;
        let parse = parse_dumps(xml);
        assert!(parse.is_partial());
        assert!((parse.share() - 1.0 / 3.0).abs() < 1e-9);
        let reason = removal_suppression_reason(&parse).expect("suppressed");
        assert!(
            reason.contains("1 of 3"),
            "the reason names the shortfall: {reason}"
        );
        assert!(
            reason.contains("tombstoned"),
            "and what it prevented: {reason}"
        );
    }

    /// The floor must not become "never tombstone": a feed that publishes fewer
    /// blocks which ALL parse is a genuine shrink and keeps snapshot semantics.
    #[test]
    fn a_clean_parse_of_any_size_may_still_tombstone() {
        let xml = r#"
          <dump><mesic>3</mesic><rok>2025</rok><odkaz>https://x/a.xml</odkaz></dump>
        "#;
        let parse = parse_dumps(xml);
        assert_eq!((parse.blocks_seen, parse.parsed()), (1, 1));
        assert!(!parse.is_partial());
        assert!(removal_suppression_reason(&parse).is_none());
        assert!(parse.warning().is_none());
    }

    /// Three years of clean blocks, one per year — the smallest index that has
    /// something for a window to exclude.
    fn three_year_parse() -> IndexParse {
        parse_dumps(
            r#"
          <dump><mesic>1</mesic><rok>2023</rok><odkaz>https://x/2023.xml</odkaz></dump>
          <dump><mesic>1</mesic><rok>2024</rok><odkaz>https://x/2024.xml</odkaz></dump>
          <dump><mesic>1</mesic><rok>2025</rok><odkaz>https://x/2025.xml</odkaz></dump>
        "#,
        )
    }

    /// THE anti-pattern this guard exists for, and the one the parse floor
    /// structurally cannot see: a **clean** parse with a `year_from` window is a
    /// full-fidelity read of the document (`share() == 1.0`, `is_partial()`
    /// false, so the parse floor waves it through) and *still* a SUBSET of what
    /// the index published. A full-snapshot write of it tombstones every dump
    /// outside the window — on the real feed, ~96 of ~120.
    #[test]
    fn a_clean_parse_inside_a_year_window_is_still_not_a_snapshot() {
        let parse = three_year_parse();
        // The parse floor's verdict, unchanged and unhelpful here by design.
        assert_eq!((parse.blocks_seen, parse.parsed()), (3, 3));
        assert!(!parse.is_partial(), "the document was read perfectly");
        assert!(
            removal_suppression_reason(&parse).is_none(),
            "the parse floor cannot see a request-scoping problem, and must not \
             be widened to try"
        );

        let window = TrackedWindow::of(&parse, Some(2025));
        assert_eq!(window.dumps.len(), 1);
        assert_eq!(window.excluded, 2);
        let reason = window
            .suppression_reason()
            .expect("a window that drops 2 of 3 dumps must not write a snapshot");
        assert!(
            reason.contains("year_from=2025"),
            "the reason must name the window, so an operator can tell it from a \
             partial parse: {reason}"
        );
        assert!(
            reason.contains("tombstoned") && reason.contains("resurrect"),
            "the reason must name both halves of the cost — the delete AND the \
             flip back that re-triggers a ~100 MB re-download: {reason}"
        );
    }

    /// The counter-case that keeps the guard honest: **no window at all** is the
    /// scheduled daily run, and it must keep full-snapshot semantics.
    #[test]
    fn a_run_without_a_window_still_tombstones() {
        let parse = three_year_parse();
        let window = TrackedWindow::of(&parse, None);
        assert_eq!(window.dumps.len(), 3, "everything is tracked");
        assert_eq!(window.excluded, 0);
        assert!(
            window.suppression_reason().is_none(),
            "the daily unwindowed run is the one that MUST be able to tombstone a \
             month the Ministry retired"
        );
    }

    /// The guard is keyed on what the window *actually excludes*, not on
    /// `year_from.is_some()`. A consumer pinned at the first published year sees
    /// exactly the batch an unwindowed run would build, so it keeps the right to
    /// tombstone — otherwise setting `year_from` at all would silently turn the
    /// app into an append-only index forever.
    #[test]
    fn a_window_that_excludes_nothing_keeps_the_right_to_tombstone() {
        let parse = three_year_parse();
        for year in [2016, 2023] {
            let window = TrackedWindow::of(&parse, Some(year));
            assert_eq!(window.excluded, 0, "year_from={year} excludes nothing");
            assert!(
                window.suppression_reason().is_none(),
                "year_from={year} drops no dump, so this batch IS the full index"
            );
        }
    }

    /// Both floors can fire at once — a garbled feed read through a window — and
    /// neither may mask the other in the report.
    #[test]
    fn a_partial_parse_inside_a_window_reports_both_floors() {
        let xml = r#"
          <dump><mesic>1</mesic><rok>2023</rok><odkaz>https://x/2023.xml</odkaz></dump>
          <dump><mesic>1</mesic><rok>2025</rok><odkaz>https://x/2025.xml</odkaz></dump>
          <dump><mesic>2</mesic><rok>2025</rok></dump>
        "#;
        let parse = parse_dumps(xml);
        assert!(parse.is_partial(), "one block lost its <odkaz>");
        let window = TrackedWindow::of(&parse, Some(2025));
        assert!(removal_suppression_reason(&parse).is_some());
        assert!(window.suppression_reason().is_some());
    }

    #[test]
    fn empty_or_unrelated_xml_yields_no_dumps() {
        for xml in ["<index></index>", "not xml at all"] {
            let parse = parse_dumps(xml);
            assert!(parse.dumps.is_empty());
            assert_eq!(parse.blocks_seen, 0);
            // Nothing was published, so nothing was lost — the empty feed is
            // refused by `run`'s own guard, not by the completeness floor.
            assert!(!parse.is_partial());
        }
    }

    /// The result block a consumer reads to tell 30-of-51 from 30-of-30 apart.
    #[test]
    fn the_parse_block_reports_seen_and_parsed_separately() {
        let xml = r#"
          <dump><mesic>3</mesic><rok>2025</rok><odkaz>https://x/a.xml</odkaz></dump>
          <dump><mesic>4</mesic><rok>2025</rok></dump>
        "#;
        let block = parse_dumps(xml).to_json();
        assert_eq!(block["blocks_seen"], 2);
        assert_eq!(block["parsed"], 1);
        assert_eq!(block["skipped"], 1);
        assert_eq!(block["skipped_missing_url"], 1);
        assert_eq!(block["skipped_unparseable_date"], 0);
        assert_eq!(block["share"], 0.5);
        assert_eq!(block["partial"], true);
    }

    /// The result keys agents and consumers read are declared. A field that only
    /// exists in `run` is a field no caller knows to look for.
    #[test]
    fn output_shape_declares_the_partial_parse_fields() {
        let shape = SmlouvyDumpWatch
            .manifest()
            .output_shape
            .expect("agents need the result shape");
        for field in [
            "dumps_in_index",
            "dumps_parsed",
            "parse",
            "warnings",
            "removals_suppressed",
        ] {
            assert!(
                shape.contains(field),
                "output_shape must declare {field}: {shape}"
            );
        }
    }
}
