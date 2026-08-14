//! Example app: Hacker News front page via the plain-HTTP engine.
//! Serves as the template for classic fetch-and-parse use cases.

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
};
use scraper::{Html, Selector};
use serde::Serialize;
use serde_json::{json, Value};

pub struct HackerNews;

#[derive(Debug, Serialize)]
struct Story {
    /// Position in the listing HN served, counted over story ROWS (including
    /// rows this run could not read), so a skipped row does not shift the ranks
    /// of everything after it.
    rank: u32,
    /// The `tr.athing` `id` attribute — HN's own item id, and the dataset key.
    ///
    /// Not optional, and that is the fix: an id-less row used to be keyed
    /// `rank-{n}`, and rank is POSITIONAL, so each run's `rank-7` record
    /// overwrote a different story and manufactured a fake `changed` revision
    /// every run. With `pages > 1` the per-page rank offset made cross-page
    /// collisions likely too. A row with no id is now dropped and counted as
    /// unparsed, which composes with the parse floor below: enough of them and
    /// the run stops tombstoning instead of writing nonsense keys.
    id: String,
    title: String,
    url: Option<String>,
    points: Option<u32>,
    author: Option<String>,
    comments: Option<u32>,
}

/// Story rows HN serves per listing page. Used only to *report* a short page —
/// never to gate tombstoning; see [`removal_suppression_reason`].
const ROWS_PER_PAGE: usize = 30;

/// The share of SERVED story rows a run must have read before its full-snapshot
/// write may tombstone the stories it did not produce.
///
/// 1.0 — nothing below "every row the page served" is a snapshot. HN publishes no
/// total, so the denominator cannot be "what the page claimed to hold"; but the
/// rows themselves are the page's own claim about how many stories it is
/// serving, and that number IS available (`tr.athing` rows found).
///
/// **The tombstone path stays reachable**: a genuinely shrinking front page —
/// fewer stories, all of them parseable — has share 1.0, so the snapshot write
/// runs and departed stories are removed. Only a *garbled* page suppresses
/// removals. Pinned by `a_shrinking_but_clean_front_page_still_tombstones`.
const PARSE_FLOOR: f64 = 1.0;

/// What the listing **served**, not only what was kept.
///
/// THE ANTI-PATTERN THIS CLOSES: `parse_front_page` returned `Vec<Story>`, so a
/// 15-of-30 parse and a 30-of-30 parse were indistinguishable to every caller —
/// including the full-snapshot `sync_many` write, which tombstoned the 15 stories
/// it failed to read and reported success. The empty-parse guard was the only
/// protection and it only catches 0-of-30.
#[derive(Debug, Default)]
struct PageParse {
    /// The stories that parsed, in listing order.
    stories: Vec<Story>,
    /// `tr.athing` rows found across every fetched page — parsed or skipped.
    /// This is the denominator: the page's own statement of how many stories it
    /// is serving.
    rows_seen: usize,
    /// Rows skipped because `span.titleline > a` was absent (markup drift).
    skipped_no_title: usize,
    /// Rows skipped because the row carried no `id` attribute — unkeyable, see
    /// [`Story::id`].
    skipped_no_id: usize,
}

impl PageParse {
    fn parsed(&self) -> usize {
        self.stories.len()
    }

    fn skipped(&self) -> usize {
        self.skipped_no_title + self.skipped_no_id
    }

    /// Fold another page's parse into this one.
    fn absorb(&mut self, other: PageParse) {
        self.stories.extend(other.stories);
        self.rows_seen += other.rows_seen;
        self.skipped_no_title += other.skipped_no_title;
        self.skipped_no_id += other.skipped_no_id;
    }

    /// Parsed ÷ story rows served. A listing with no `tr.athing` rows at all is
    /// complete (1.0) — nothing was served, so nothing was lost. (That case is
    /// refused separately, by the empty guard in `run`.)
    fn share(&self) -> f64 {
        if self.rows_seen == 0 {
            return 1.0;
        }
        self.parsed() as f64 / self.rows_seen as f64
    }

    /// Whether this batch is a **subset** of the listing rather than the whole of
    /// it — i.e. whether a full-snapshot write would tombstone stories that are
    /// still on the front page.
    fn is_partial(&self) -> bool {
        self.share() < PARSE_FLOOR
    }

    /// The `parse` block of the result: what the listing served, what was read
    /// out of it, and why the rest was dropped.
    fn to_json(&self) -> Value {
        json!({
            "rows_seen": self.rows_seen,
            "parsed": self.parsed(),
            "skipped": self.skipped(),
            "skipped_no_title": self.skipped_no_title,
            "skipped_no_id": self.skipped_no_id,
            // 3 dp: enough to read, short of float noise in a stored result.
            "share": (self.share() * 1000.0).round() / 1000.0,
            "floor": PARSE_FLOOR,
            "partial": self.is_partial(),
        })
    }

    /// The one-line `warnings[]` entry a lossy parse contributes, or `None` when
    /// every served row parsed. Separate from [`removal_suppression_reason`] so a
    /// future looser floor still *reports* the skips it tolerates.
    fn warning(&self) -> Option<String> {
        (self.skipped() > 0).then(|| {
            format!(
                "partial listing parse: {} of {} story rows parsed ({} skipped: {} with no \
                 titleline link, {} with no item id) — the markup may have drifted",
                self.parsed(),
                self.rows_seen,
                self.skipped(),
                self.skipped_no_title,
                self.skipped_no_id,
            )
        })
    }
}

/// **The floor on a full-snapshot write.** `Some(reason)` when removal detection
/// must be skipped because this batch is only part of the listing HN served;
/// `None` when the parse earned the right to tombstone.
///
/// Pure, so the floor is testable without a store — and named, so the fix is
/// guarded rather than buried in `run()`. Deliberately judged on parsed ÷ served,
/// NOT on "did the page serve 30 rows": a short page is a legitimate upstream
/// state (a shrinking front page), and gating removals on it would turn this app
/// permanently upsert-only in exactly the case tombstoning exists for. The short
/// page is reported instead, by [`short_page_warning`].
fn removal_suppression_reason(parse: &PageParse) -> Option<String> {
    parse.is_partial().then(|| {
        format!(
            "removal detection suppressed: only {} of {} story rows parsed ({:.0}% < {:.0}% \
             floor), so this batch is a SUBSET of the listing — the stories missing from it are \
             kept rather than tombstoned",
            parse.parsed(),
            parse.rows_seen,
            parse.share() * 100.0,
            PARSE_FLOOR * 100.0,
        )
    })
}

/// A page that served materially fewer than [`ROWS_PER_PAGE`] story rows.
///
/// Reported, never gated on (see [`removal_suppression_reason`]): HN publishes no
/// total, so "fewer rows than usual" is ambiguous between a shrinking front page
/// and a truncated response, and only a human reading the result can tell.
fn short_page_warning(rows_seen: usize, pages: u64) -> Option<String> {
    let expected = ROWS_PER_PAGE * pages as usize;
    (rows_seen < expected).then(|| {
        format!(
            "short listing: {rows_seen} story rows served across {pages} page(s), expected \
             ~{expected} ({ROWS_PER_PAGE}/page) — either the front page shrank or the response \
             was truncated"
        )
    })
}

/// Every key [`HackerNews::run`] emits, and the single source the manifest's
/// `output_shape` is checked against.
///
/// Borrowed from `trades-common`'s `RESULT_FIELDS` trick: a result shape stated
/// only in prose drifts from the code the first time a key is added. This one
/// already had — `output_shape` promised the tombstoning while the result never
/// carried `removed`, so a run that tombstoned 15 stories and a run that
/// tombstoned none were byte-identical.
/// (Test-only: the assertion is the whole point, and a production reference to
/// it would be busywork.)
#[cfg(test)]
const RESULT_KEYS: [&str; 10] = [
    "count",
    "rows_seen",
    "parse",
    "warnings",
    "removals_suppressed",
    "new",
    "changed",
    "unchanged",
    "removed",
    "stories",
];

/// The counts a dataset write reports back. Split out so [`run_result`] is pure
/// and the manifest can be pinned against a real result object in a unit test.
#[derive(Debug, Default, Clone, Copy)]
struct WriteCounts {
    new: usize,
    changed: usize,
    unchanged: usize,
    /// Stories tombstoned by the full-snapshot write. `0` on a suppressed run —
    /// which `removals_suppressed` distinguishes from "nothing dropped off".
    removed: usize,
}

fn run_result(
    parse: &PageParse,
    counts: WriteCounts,
    warnings: &[String],
    removals_suppressed: Option<String>,
) -> Value {
    json!({
        "count": parse.parsed(),
        "rows_seen": parse.rows_seen,
        "parse": parse.to_json(),
        "warnings": warnings,
        "removals_suppressed": removals_suppressed,
        "new": counts.new,
        "changed": counts.changed,
        "unchanged": counts.unchanged,
        "removed": counts.removed,
        "stories": parse.stories,
    })
}

#[async_trait]
impl ScrapeApp for HackerNews {
    fn name(&self) -> &'static str {
        "hackernews"
    }

    fn description(&self) -> &'static str {
        "Hacker News front page stories (http engine demo). Params: {\"pages\": 1-5}"
    }

    // Uncomment for a recurring scrape every 6 hours:
    // fn schedule(&self) -> Option<&'static str> { Some("0 0 */6 * * *") }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "pages": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "Front-page listing pages to fetch (30 stories each); clamped to 1..=5."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Front page only (30 stories) — the default run",
                    params: json!({}),
                },
                ManifestExample {
                    description: "Top 90 stories: walk three listing pages in one snapshot",
                    params: json!({ "pages": 3 }),
                },
            ],
            output_shape: Some(
                "{count, rows_seen, parse {rows_seen, parsed, skipped, skipped_no_title, \
                 skipped_no_id, share, floor, partial}, warnings[], removals_suppressed|null, \
                 new, changed, unchanged, removed, stories: [{rank, id, title, url, points, \
                 author, comments}]} — a full-snapshot sync of the `stories` dataset (keyed by \
                 HN item id), so stories that fell off the listing are tombstoned. UNLESS the \
                 run read fewer stories than the listing served rows for: then the write is \
                 downgraded to an upsert, `removals_suppressed` says why, and `removed` is 0 \
                 because nothing was tombstoned rather than because nothing dropped off",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let pages = ctx
            .params
            .get("pages")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .clamp(1, 5);

        let mut parse = PageParse::default();
        for page in 1..=pages {
            let response = ctx
                .engines
                .http
                .fetch(HttpRequest::get(format!(
                    "https://news.ycombinator.com/news?p={page}"
                )))
                .await?;
            if !response.is_success() {
                return Err(Error::App(format!(
                    "HN returned status {}",
                    response.status
                )));
            }
            // Ranks continue from ROWS SEEN, not from stories kept: rank is the
            // listing position, so a skipped row on page 1 must not shift page 2
            // up by one.
            let offset = parse.rows_seen as u32;
            parse.absorb(parse_front_page(&response.body, offset));
        }

        if parse.stories.is_empty() {
            // The front page always lists stories; a 200 that parses to zero rows
            // means markup drift or a soft rate-limit page — fail rather than
            // silently record an empty run as success.
            return Err(Error::App(
                "HN: fetched pages but parsed 0 stories (markup drift or soft rate-limit)".into(),
            ));
        }

        ctx.save_artifact("stories.json", &serde_json::to_vec_pretty(&parse.stories)?)
            .await?;

        // Dedup + change detection: upsert each story keyed by its HN id, so a
        // scheduled run only surfaces stories that are new or whose score/
        // comment counts changed since last time.
        let items: Vec<(String, Value)> = parse
            .stories
            .iter()
            .map(|s| (s.id.clone(), serde_json::to_value(s).unwrap_or(Value::Null)))
            .collect();

        // The front page is a full snapshot, so the write is normally
        // `sync_many`: a story that has dropped off is marked removed rather than
        // lingering forever. But a run that read 15 of 30 served rows is NOT a
        // snapshot, and `sync_many` would tombstone the other 15 and report
        // success — so a partial parse downgrades the write to an upsert and says
        // so, exactly as `smlouvy-dump-watch` and `cordis` do.
        let removals_suppressed = removal_suppression_reason(&parse);
        let warnings: Vec<String> = [
            parse.warning(),
            short_page_warning(parse.rows_seen, pages),
            removals_suppressed.clone(),
        ]
        .into_iter()
        .flatten()
        .collect();
        let summary = match &removals_suppressed {
            Some(_) => ctx.upsert_many("stories", &items).await?,
            None => ctx.sync_many("stories", &items).await?,
        };

        Ok(run_result(
            &parse,
            WriteCounts {
                new: summary.new.len(),
                changed: summary.changed.len(),
                unchanged: summary.unchanged,
                removed: summary.removed.len(),
            },
            &warnings,
            removals_suppressed,
        ))
    }
}

fn sel(css: &str) -> Selector {
    Selector::parse(css).expect("valid selector")
}

/// Parse one listing page, reporting the rows it could NOT read alongside the
/// ones it could. `rank_offset` is the number of story rows earlier pages served.
fn parse_front_page(html: &str, rank_offset: u32) -> PageParse {
    let doc = Html::parse_document(html);
    let row_sel = sel("tr.athing");
    let title_sel = sel("span.titleline > a");
    let subtext_sel = sel("td.subtext");
    let score_sel = sel("span.score");
    let user_sel = sel("a.hnuser");
    let link_sel = sel("a");

    // Each story row is followed by a metadata row; the td.subtext cells come
    // in the same document order, so zipping by index pairs them up.
    let subtexts: Vec<_> = doc.select(&subtext_sel).collect();

    let mut parse = PageParse::default();
    for (i, row) in doc.select(&row_sel).enumerate() {
        parse.rows_seen += 1;
        // The two ways a served row is lost. Both used to be a silent
        // `.filter_map` drop, which is what made the denominator unknowable.
        let Some(title_link) = row.select(&title_sel).next() else {
            parse.skipped_no_title += 1;
            continue;
        };
        let Some(id) = row.value().attr("id").map(String::from) else {
            parse.skipped_no_id += 1;
            continue;
        };
        let title = title_link.text().collect::<String>();
        let url = title_link.value().attr("href").map(|href| {
            if href.starts_with("item?") {
                format!("https://news.ycombinator.com/{href}")
            } else {
                href.to_string()
            }
        });

        let subtext = subtexts.get(i);
        let points = subtext
            .and_then(|s| s.select(&score_sel).next())
            .and_then(|score| {
                let text = score.text().collect::<String>();
                text.split_whitespace().next()?.parse().ok()
            });
        let author = subtext
            .and_then(|s| s.select(&user_sel).next())
            .map(|a| a.text().collect::<String>());
        let comments = subtext.and_then(|s| {
            s.select(&link_sel)
                .filter_map(|a| {
                    let text = a.text().collect::<String>().replace('\u{a0}', " ");
                    if !text.contains("comment") {
                        return None;
                    }
                    text.split_whitespace().next()?.parse::<u32>().ok()
                })
                .last()
        });

        parse.stories.push(Story {
            rank: rank_offset + i as u32 + 1,
            id,
            title,
            url,
            points,
            author,
            comments,
        });
    }
    parse
}

#[cfg(test)]
mod tests {
    use super::{
        parse_front_page, removal_suppression_reason, run_result, short_page_warning, HackerNews,
        WriteCounts, RESULT_KEYS,
    };
    use pumper_core::ScrapeApp;
    use serde_json::Value;

    /// A realistic slice of the front page: story row + subtext row pairs,
    /// one external link and one internal `item?` link, points/author/comments
    /// in the real markup shapes (nbsp in "12 comments", score span).
    const SAMPLE: &str = r#"
        <table>
          <tr class="athing" id="1001"><td>
            <span class="titleline"><a href="https://example.com/post">Big News</a></span>
          </td></tr>
          <tr><td class="subtext">
            <span class="score">55 points</span> by <a class="hnuser">alice</a>
            <a href="item?id=1001">12&#160;comments</a>
          </td></tr>
          <tr class="athing" id="1002"><td>
            <span class="titleline"><a href="item?id=1002">Ask HN: Question</a></span>
          </td></tr>
          <tr><td class="subtext">
            <span class="score">7 points</span> by <a class="hnuser">bob</a>
            <a href="item?id=1002">discuss</a>
          </td></tr>
        </table>"#;

    #[test]
    fn parses_story_rows_with_ranks_links_and_metadata() {
        let parse = parse_front_page(SAMPLE, 30);
        let stories = &parse.stories;
        assert_eq!(stories.len(), 2);
        assert_eq!(parse.rows_seen, 2, "both rows were served and both parsed");
        assert_eq!(parse.skipped(), 0);

        let s = &stories[0];
        assert_eq!(s.rank, 31, "rank continues from the page offset");
        assert_eq!(s.id, "1001");
        assert_eq!(s.title, "Big News");
        assert_eq!(s.url.as_deref(), Some("https://example.com/post"));
        assert_eq!(s.points, Some(55));
        assert_eq!(s.author.as_deref(), Some("alice"));
        assert_eq!(s.comments, Some(12));

        // Internal links get the site prefix; "discuss" (no comment count) is None.
        let s = &stories[1];
        assert_eq!(
            s.url.as_deref(),
            Some("https://news.ycombinator.com/item?id=1002")
        );
        assert_eq!(s.comments, None);
    }

    /// The empty-parse-is-an-error guard in `run()` depends on this: markup
    /// drift (or a soft rate-limit page) must parse to ZERO stories, not to
    /// garbage rows — zero is what trips the silent-success guard.
    #[test]
    fn drifted_markup_parses_to_zero_stories_not_garbage() {
        let drifted =
            r#"<table><tr class="story"><td><a href="/x">Not the real shape</a></td></tr></table>"#;
        let parse = parse_front_page(drifted, 0);
        assert!(parse.stories.is_empty());
        assert_eq!(
            parse.rows_seen, 0,
            "fully drifted markup serves no rows at all — the empty guard, not the floor, \
             catches this"
        );
        assert!(parse_front_page("", 0).stories.is_empty());
    }

    /// A page whose rows are HN's real shape but half of which this parser cannot
    /// read: the denominator (`tr.athing` rows served) is known even though HN
    /// publishes no total, and that is what makes the partial detectable.
    const HALF_BROKEN: &str = r#"
        <table>
          <tr class="athing" id="2001"><td>
            <span class="titleline"><a href="https://example.com/a">Readable</a></span>
          </td></tr>
          <tr><td class="subtext"><span class="score">10 points</span></td></tr>
          <tr class="athing" id="2002"><td>
            <span class="headline"><a href="https://example.com/b">Renamed span</a></span>
          </td></tr>
          <tr><td class="subtext"><span class="score">20 points</span></td></tr>
        </table>"#;

    #[test]
    fn a_partial_parse_cannot_tombstone_the_rows_it_failed_to_read() {
        // THE REFUTED BEHAVIOR: `parse_front_page` returned `Vec<Story>`, so a
        // 1-of-2 parse looked exactly like a 1-of-1 one and `sync_many` — a full
        // snapshot write — tombstoned the story it never managed to read, then
        // reported success. The empty-parse guard only catches 0-of-N.
        let parse = parse_front_page(HALF_BROKEN, 0);
        assert_eq!(parse.parsed(), 1);
        assert_eq!(parse.rows_seen, 2, "the page SERVED two story rows");
        assert_eq!(parse.skipped_no_title, 1);
        assert!(parse.is_partial());

        let reason = removal_suppression_reason(&parse).expect("a subset must not tombstone");
        assert!(reason.contains("1 of 2"), "the reason states the shortfall");
        assert!(parse.warning().is_some());
    }

    #[test]
    fn a_shrinking_but_clean_front_page_still_tombstones() {
        // THE COUNTER-TEST, and the reason the floor is parsed ÷ SERVED rather
        // than "did the page serve 30 rows": a front page that genuinely gets
        // shorter serves fewer rows that ALL parse. That is a real snapshot, and
        // suppressing removals for it would make this app permanently
        // upsert-only — stories would linger on the dataset forever, which is
        // the failure mode the `sync_many` was chosen for.
        let parse = parse_front_page(SAMPLE, 0);
        assert_eq!(parse.parsed(), 2);
        assert_eq!(parse.rows_seen, 2);
        assert!(
            !parse.is_partial(),
            "share is 1.0 — every served row parsed"
        );
        assert!(
            removal_suppression_reason(&parse).is_none(),
            "a clean short page has earned the right to tombstone"
        );
        // It IS reported, though — a two-row front page is worth a human look.
        assert!(short_page_warning(parse.rows_seen, 1).is_some());
        assert!(short_page_warning(30, 1).is_none());
        assert!(short_page_warning(60, 2).is_none());
        assert!(short_page_warning(31, 2).is_some());
    }

    #[test]
    fn an_idless_row_is_unparsed_not_keyed_by_its_rank() {
        // THE REFUTED BEHAVIOR: `s.id.unwrap_or(format!("rank-{}", s.rank))`.
        // Rank is POSITIONAL, so `rank-7` named a different story on every run —
        // each run silently overwrote the last one's record and manufactured a
        // fake `changed` revision. Dropping the row instead keeps the key space
        // honest AND composes with the floor: the row still counts in
        // `rows_seen`, so enough of them suppress removals rather than writing
        // nonsense keys. The existing parser test covers fully-drifted markup;
        // this is an id-less row inside VALID markup.
        let idless = r#"
            <table>
              <tr class="athing"><td>
                <span class="titleline"><a href="https://example.com/x">No id attribute</a></span>
              </td></tr>
              <tr><td class="subtext"><span class="score">3 points</span></td></tr>
              <tr class="athing" id="3002"><td>
                <span class="titleline"><a href="https://example.com/y">Has one</a></span>
              </td></tr>
              <tr><td class="subtext"><span class="score">4 points</span></td></tr>
            </table>"#;
        let parse = parse_front_page(idless, 0);
        assert_eq!(parse.parsed(), 1);
        assert_eq!(parse.skipped_no_id, 1);
        assert_eq!(parse.rows_seen, 2);
        assert_eq!(parse.stories[0].id, "3002");
        assert_eq!(
            parse.stories[0].rank, 2,
            "rank is the LISTING position — a skipped row must not shift it"
        );
        assert!(
            removal_suppression_reason(&parse).is_some(),
            "an unkeyable row is a row we did not read: it must not tombstone"
        );
    }

    #[test]
    fn the_result_declares_the_removals_it_makes_not_only_the_writes() {
        // THE REFUTED BEHAVIOR: `output_shape` promised the tombstoning while
        // `run()` emitted only {count, new, changed, unchanged, stories} — so a
        // run that tombstoned 15 stories and a run that tombstoned none were
        // byte-identical to every consumer. `UpsertSummary` carried `removed`
        // the whole time (cordis reads it).
        let parse = parse_front_page(SAMPLE, 0);
        let counts = WriteCounts {
            new: 1,
            changed: 0,
            unchanged: 1,
            removed: 15,
        };
        let result = run_result(&parse, counts, &[], None);
        assert_eq!(result["removed"], 15);
        assert_eq!(result["removals_suppressed"], Value::Null);
        assert_eq!(result["count"], 2);

        // A suppressed run reports 0 removals AND why they are 0.
        let broken = parse_front_page(HALF_BROKEN, 0);
        let reason = removal_suppression_reason(&broken).unwrap();
        let warnings = vec![reason.clone()];
        let suppressed = run_result(&broken, WriteCounts::default(), &warnings, Some(reason));
        assert_eq!(suppressed["removed"], 0);
        assert!(suppressed["removals_suppressed"].is_string());
        assert_eq!(suppressed["parse"]["partial"], true);
        assert_ne!(
            suppressed["removed"], result["removed"],
            "a suppressed run and a tombstoning run must not read alike"
        );
    }

    /// The manifest is checked against a REAL result object, not against prose:
    /// `output_shape` drifted from `run()` once already (it promised
    /// `{extracted, errors, removed?}`-style keys the run never emitted), and
    /// `trades-common`'s `shape_declares_coverage` exists for the same reason.
    #[test]
    fn the_output_shape_names_every_key_the_run_emits() {
        let shape = HackerNews.manifest().output_shape.expect("shape declared");
        let result = run_result(
            &parse_front_page(SAMPLE, 0),
            WriteCounts::default(),
            &[],
            None,
        );
        let emitted: Vec<String> = result
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect();
        for key in &emitted {
            assert!(
                shape.contains(key.as_str()),
                "output_shape does not name `{key}`, which run() emits"
            );
        }
        let declared: Vec<&str> = RESULT_KEYS.to_vec();
        assert_eq!(
            emitted.iter().map(String::as_str).collect::<Vec<_>>().len(),
            declared.len(),
            "RESULT_KEYS is out of step with the result object"
        );
        for key in declared {
            assert!(emitted.iter().any(|e| e == key), "`{key}` is not emitted");
            assert!(shape.contains(key), "output_shape does not name `{key}`");
        }
        // The `parse` block's own keys are part of the contract too.
        for key in [
            "rows_seen",
            "parsed",
            "skipped_no_title",
            "skipped_no_id",
            "share",
            "floor",
            "partial",
        ] {
            assert!(
                shape.contains(key),
                "output_shape does not name parse.{key}"
            );
        }
    }
}
