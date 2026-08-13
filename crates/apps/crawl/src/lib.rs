//! Broad crawler app: seed a set of URLs and crawl outward with bounded
//! concurrency, depth, and page count — respecting robots.txt and the per-domain
//! governor, dropping near-duplicate pages, and streaming page bodies to the
//! job's artifact directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::datasets::Provenance;
use pumper_core::{
    crawl, AppContext, AppManifest, CostClass, CrawlConfig, CrawlPageRecord, Datasets, Error,
    HttpClient, HttpRequest, HttpResponse, ManifestExample, PageSink, PageSource, Result,
    RevisitCadence, RevisitSeed, ScrapeApp,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub mod link_graph;
pub mod reliability;

pub struct Crawl;

/// Per-host fetch tally accumulated by [`MeteringHttpClient`] over a crawl.
/// The same observation point that always fed `learn_tier`, kept as counters
/// instead of a single boolean so the run's fetch-layer telemetry can also be
/// persisted into the Web Reliability Index (see [`reliability`]) — richer
/// tallying of responses the crawl made anyway, never extra fetches.
#[derive(Default)]
struct HostTally {
    fetches: usize,
    /// 2xx responses.
    ok: usize,
    /// Bot-wall statuses (403/429/503) — same set as `fetcher::http_bot_wall`
    /// (which is crate-private).
    botwall: usize,
    /// Transport-layer failures (DNS/TLS/connection/timeout).
    transport_errors: usize,
    /// `304 Not Modified` answers — evidence the host honors conditional GETs.
    not_modified: usize,
    /// `404`/`410` answers — the gone lifecycle.
    gone: usize,
    /// Responses carrying an `ETag` or `Last-Modified` validator header.
    validators_seen: usize,
    /// `robots.txt` / sitemap fetches — counted, but folded into none of the
    /// scored counters above. See [`is_page_fetch`].
    probes: usize,
}

impl HostTally {
    /// The signal the tier router learns from: any bot-wall or transport loss.
    fn http_lost(&self) -> bool {
        self.botwall > 0 || self.transport_errors > 0
    }

    /// Folds one PROBE fetch into the tally. Counted so the fetches the crawl
    /// really made are not invisible, but kept out of every scored counter.
    fn record_probe(&mut self) {
        self.probes += 1;
    }

    /// Folds one PAGE fetch outcome into the tally (pure classification, kept
    /// out of the client so it is unit-testable).
    fn record(&mut self, result: &Result<HttpResponse>) {
        self.fetches += 1;
        match result {
            Ok(resp) => {
                match resp.status {
                    403 | 429 | 503 => self.botwall += 1,
                    304 => self.not_modified += 1,
                    404 | 410 => self.gone += 1,
                    s if (200..300).contains(&s) => self.ok += 1,
                    _ => {}
                }
                if resp.headers.keys().any(|k| {
                    k.eq_ignore_ascii_case("etag") || k.eq_ignore_ascii_case("last-modified")
                }) {
                    self.validators_seen += 1;
                }
            }
            Err(_) => self.transport_errors += 1,
        }
    }
}

/// Wraps the raw HTTP client the crawler drives so the crawl — the platform's
/// highest-volume fetch path — stops being invisible to the cost ledger and the
/// learned tier router. It cannot route through `AppContext::fetch` (it owns its
/// own concurrency/robots/frontier control), so instead it tallies per-host
/// outcomes here and the app flushes them through the metered seams *after* the
/// crawl, in O(hosts) writes rather than O(fetches) — deliberately not one DB
/// write per fetch, which would re-create the write contention that motivated
/// the budget-total change in this same wave.
struct MeteringHttpClient {
    inner: Arc<dyn HttpClient>,
    tallies: Arc<Mutex<HashMap<String, HostTally>>>,
    /// Whether this run enabled `sitemap_seeds`, so sitemap documents are only
    /// treated as probes on runs that actually fetch them (see [`is_page_fetch`]).
    sitemap_seeding: bool,
}

#[async_trait]
impl HttpClient for MeteringHttpClient {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let host = host_of(&req.url);
        let is_page = is_page_fetch(&req.url, self.sitemap_seeding);
        let result = self.inner.fetch(req).await;
        if let Some(host) = host {
            // std Mutex, no `.await` held across the guard.
            let mut tallies = self.tallies.lock().unwrap_or_else(|e| e.into_inner());
            let tally = tallies.entry(host).or_default();
            if is_page {
                tally.record(&result);
            } else {
                tally.record_probe();
            }
        }
        result
    }
}

/// Lowercased host of an http(s) URL — enough for crawl targets, without pulling
/// in the `url` crate. Strips scheme, any `userinfo@`, the path/query/fragment,
/// and the port. Public so the sibling extractor app can attribute its
/// reliability observations to the same host notion.
pub fn host_of(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let host = authority.split(':').next()?;
    (!host.is_empty()).then(|| host.to_lowercase())
}

/// Path component of an http(s) URL (leading `/`, query and fragment stripped),
/// or `"/"` when the URL names only an authority. Sibling of [`host_of`], same
/// no-`url`-crate policy.
fn path_of(url: &str) -> Option<&str> {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority_and_path = after_scheme.split(['?', '#']).next()?;
    Some(match authority_and_path.find('/') {
        Some(i) => &authority_and_path[i..],
        None => "/",
    })
}

/// Whether a path looks like a sitemap document. Consulted **only** on runs that
/// actually enabled `sitemap_seeds`, so an ordinary crawl of a page genuinely
/// named `/sitemap.xml` still measures it as a page.
fn looks_like_sitemap(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let stem = lower.strip_suffix(".gz").unwrap_or(lower.as_str());
    stem.ends_with(".xml") && stem.contains("sitemap")
}

/// Whether this fetch retrieved a **page of the site**, as opposed to a probe
/// the crawler makes to learn *how* to crawl the host (`robots.txt`, and — on
/// sitemap-seeding runs — sitemap documents).
///
/// Anti-pattern this defends: **a host with no `robots.txt` looked exactly like
/// a host serving dead pages.** [`MeteringHttpClient`] wraps the very client
/// core hands to `robots_for`, so a host without a `robots.txt` answered `404`,
/// which classified as `gone` and folded straight into the Web Reliability
/// Index. Every crawl therefore fabricated a gone-page observation for every
/// host lacking a robots.txt, and the index was not merely sparse — it was wrong
/// in a consistent direction (`availability` and `fetch_ok` both pushed down by
/// a fetch that was never about a page at all).
///
/// Known residual: a robots-declared sitemap at a URL that does not look like
/// one (say `/feeds/all.xml`) is still measured as a page. That is the
/// pre-existing behaviour for that URL, not a new error, and it only occurs on
/// `sitemap_seeds` runs — unlike the robots probe, which every crawl makes
/// against every host.
fn is_page_fetch(url: &str, sitemap_seeding: bool) -> bool {
    let Some(path) = path_of(url) else {
        return true; // unparseable — measure it rather than silently drop it
    };
    if path == "/robots.txt" {
        return false;
    }
    !(sitemap_seeding && looks_like_sitemap(path))
}

/// Whether this crawl saw the entire graph it discovered.
///
/// Anti-pattern this defends: core counts truncation honestly and documents both
/// counters as existing so a capped crawl is "reported honestly rather than
/// silently dropping discovered URLs" — and the app emitted **neither**, so a
/// crawl that refused thousands of discovered URLs at the 100k frontier cap, or
/// dumped a whole host's backlog at `max_pages_per_host`, returned a result
/// byte-identical to one that covered the whole site. Emitting the two raw
/// counters is still not legible on its own (a caller would have to know that
/// "both zero means complete"), so the verdict is named and reported beside them.
fn coverage_complete(frontier_dropped: usize, skipped_host_budget: usize) -> bool {
    frontier_dropped == 0 && skipped_host_budget == 0
}

/// The `warnings` entry for a truncated crawl — the fleet's idiom for "this
/// result describes a WINDOW, not the whole thing" (cordis `aggregate_truncated`,
/// census `blend_complete`). `None` when nothing was cut, so a complete crawl
/// carries no warning noise.
fn coverage_warning(frontier_dropped: usize, skipped_host_budget: usize) -> Option<String> {
    if coverage_complete(frontier_dropped, skipped_host_budget) {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    if frontier_dropped > 0 {
        parts.push(format!(
            "{frontier_dropped} discovered URLs were refused at the frontier cap"
        ));
    }
    if skipped_host_budget > 0 {
        parts.push(format!(
            "{skipped_host_budget} queued URLs were dropped when their host reached \
             max_pages_per_host"
        ));
    }
    Some(format!(
        "coverage is PARTIAL: {} — this run crawled a WINDOW of the graph it discovered, so the \
         page counts and `top_linked` describe what was reached, not what exists",
        parts.join(" and ")
    ))
}

/// The manifest's `output_shape`: the leading `{...}` block lists **every key
/// `run()` always returns**, and nothing else. [`output_shape_keys`] parses that
/// block so an inventory test can diff it against a real run in both directions.
///
/// Anti-pattern this defends: the previous shape advertised `pages`, `skipped`
/// and `unchanged` (keys no run has ever emitted) and omitted every field added
/// by the last four milestones. `output_shape` is what a consumer codes against,
/// so drift here is a broken contract, not a stale comment.
const OUTPUT_SHAPE: &str = "{crawled, kept, skipped_duplicates, skipped_robots, \
     skipped_filtered, frontier_dropped, skipped_host_budget, coverage_complete, \
     sitemap_seeded, failed, failed_by_host, skipped_botwall, robots_fetch_failures, \
     checkpoint_errors, resumed, checkpoint_reset, hosts, frontier_remaining, pages_dataset, \
     pages_new, pages_changed, pages_unchanged, revisit, revisited, unchanged_304, \
     skipped_not_due, cadence_updates, changed, new, gone, versions_archived, \
     reliability_hosts, edges_dataset, edges_written, edges_unchanged, \
     edges_dropped_out_degree, edges_deduped, top_linked} — crawl tallies plus the `pages` \
     dataset upsert summary. A truncated crawl (`coverage_complete: false`) additionally \
     carries a `warnings` array naming what was cut. Bodies land in the job's artifact dir, \
     changed revisions also as revision-suffixed copies recorded in `page_versions`, and the \
     link graph streams into the `edges` dataset (key `{from_url}|{to_url}`).";

/// The always-present result keys named by [`OUTPUT_SHAPE`] — everything inside
/// its leading brace block. Lives beside the string it parses so the inventory
/// test cannot drift from the format.
pub fn output_shape_keys() -> Vec<&'static str> {
    let Some(open) = OUTPUT_SHAPE.find('{') else {
        return Vec::new();
    };
    let Some(close) = OUTPUT_SHAPE[open..].find('}').map(|i| open + i) else {
        return Vec::new();
    };
    OUTPUT_SHAPE[open + 1..close]
        .split(',')
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .collect()
}

/// How often a running crawl commits what it has learned about the hosts it is
/// touching.
///
/// **Wall clock, not a page stride.** The app has no hook inside core's crawl
/// loop — the progress seam is a synchronous `Fn`, and hanging DB writes off it
/// would put them on the crawl's hot path — and a clock bounds the worst-case
/// loss by *time*, which is the right unit here: a reaped or shutdown-drained
/// job loses at most one interval no matter how fast it was crawling. Two
/// minutes is the compromise between that loss (an interrupted six-hour crawl
/// keeps ~99% of its telemetry) and fold volume (one read-modify-write per host
/// per interval, i.e. ~30/hour/host — emphatically not the per-page write the
/// tally exists to avoid). A crawl shorter than the interval flushes exactly
/// once, at the end, exactly as before.
const TELEMETRY_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(120);

/// Per-host bookkeeping that must survive across a run's telemetry flushes.
#[derive(Default)]
struct TelemetryProgress {
    /// Hosts whose run has already been counted into `runs`/`observations`, so a
    /// later flush continues that contribution instead of registering a new run.
    started: std::collections::HashSet<String>,
    /// Hosts that lost the HTTP tier at ANY point this run. Monotone on purpose:
    /// `learn_tier` **resets** a host's strikes on a win, so feeding it a clean
    /// mid-crawl delta would erase a bot-wall the run had already seen. With the
    /// cumulative bit, the last signal a completed run sends is identical to the
    /// single end-of-run signal it used to send — the periodic commits can only
    /// make the router learn *earlier*, never differently.
    lost: std::collections::HashSet<String>,
}

/// Runs `crawl_fut` to completion while committing the metering client's
/// per-host tallies every `interval` — through every seam that learns from a
/// crawl: the cost ledger (`ctx.meter`), the learned tier router
/// (`ctx.learn_tier`) and the Web Reliability Index.
///
/// `interval` is [`TELEMETRY_FLUSH_INTERVAL`] in production; it is a parameter
/// only so the crate's own tests can drive the loop at millisecond cadence
/// (a paused tokio clock is not an option here — auto-advance trips the SQLite
/// pool's acquire timeout).
///
/// Anti-pattern this defends: **an interrupted crawl contributed nothing.** All
/// three commits sat after the `?` on `crawl(...)`, so a reaped job, a shutdown
/// drain (`worker.rs` drops the pinned `app.run(ctx)` future on cancel or
/// timeout) or a propagating fetch error skipped the entire loop — and the
/// durable resume state carries no tallies, so the next attempt could not
/// recover them either. The runs whose telemetry is worth the most, the
/// multi-hour ones, are exactly the ones most likely to be interrupted, so the
/// observatory was missing precisely its best evidence.
///
/// All three seams are committed per flush, not just the cheap one:
/// - `meter` appends one $0 ledger event per host per flush. Repeating it is
///   safe (the sum is what it always was); only the row count changes, and only
///   for crawls that outlive one interval.
/// - `learn_tier` is **not** safely repeatable with a delta — a clean delta
///   resets strikes — so it is fed [`TelemetryProgress::lost`], the run's
///   cumulative verdict, which makes a completed run's final signal identical to
///   the one end-of-run call it replaced. The verdict also decides the reported
///   winner ([`tier_that_won`]); passing a hardcoded `"http"` reset the strikes
///   unconditionally, which is why the cumulative verdict alone was not enough.
/// - the reliability fold is additive, and `continues_run` / `run_complete` keep
///   `runs` counting runs rather than check-ins.
async fn crawl_flushing_telemetry(
    ctx: &AppContext,
    crawl_fut: impl std::future::Future<Output = Result<pumper_core::CrawlStats>>,
    tallies: &Mutex<HashMap<String, HostTally>>,
    telemetry: &mut TelemetryProgress,
    interval: std::time::Duration,
) -> Result<pumper_core::CrawlStats> {
    let mut crawl_fut = std::pin::pin!(crawl_fut);
    let crawl_result = loop {
        match tokio::time::timeout(interval, &mut crawl_fut).await {
            Ok(done) => break done,
            Err(_elapsed) => flush_host_telemetry(ctx, tallies, telemetry, false).await,
        }
    };
    // The final flush runs BEFORE the caller's `?`: a crawl that failed at the
    // fetch layer still learned which hosts fail, which is precisely the
    // observation the reliability index exists to keep.
    flush_host_telemetry(ctx, tallies, telemetry, true).await;
    crawl_result
}

/// Reported to `learn_tier` when no tier won a host's fetches. Any value other
/// than a tier name works — `TierMemory::record` branches on `winner == "http"`
/// and otherwise only reads `http_lost` — but naming it says what happened
/// rather than picking an arbitrary other tier the crawl never ran.
const NO_TIER_WON: &str = "none";

/// Which tier to report as having WON this host's fetches.
///
/// Anti-pattern this defends — *the crawl erasing the evidence it just
/// gathered.* `TierMemory::record` resets a host's strikes to 0 whenever the
/// winner is `"http"`, **before** it ever looks at `http_lost`: that branch IS
/// the shape of an HTTP win. The crawl passed a hardcoded `"http"` for every
/// host, so a host that bot-walled or transport-errored the entire run had its
/// strike count cleared by the very call whose stated purpose is to teach the
/// router "which hosts bot-wall the HTTP tier" — and cleared what OTHER apps had
/// learned about that host too, since `tier_memory` is keyed by host alone.
///
/// The crawl only ever runs the http tier, so there is no other winner to name:
/// either http carried the host or nothing did.
fn tier_that_won(http_lost: bool) -> &'static str {
    if http_lost {
        NO_TIER_WON
    } else {
        "http"
    }
}

/// One commit of the run's accumulated per-host telemetry. See
/// [`crawl_flushing_telemetry`] for why it is called during the crawl.
async fn flush_host_telemetry(
    ctx: &AppContext,
    tallies: &Mutex<HashMap<String, HostTally>>,
    progress: &mut TelemetryProgress,
    final_flush: bool,
) {
    // Drain: a flush commits only what accumulated since the last one.
    let drained = std::mem::take(&mut *tallies.lock().unwrap_or_else(|e| e.into_inner()));
    if drained.is_empty() && !final_flush {
        return;
    }
    let mut hosts: Vec<(String, HostTally)> = drained.into_iter().collect();
    hosts.sort_by(|a, b| a.0.cmp(&b.0)); // deterministic write order
    let mut deltas: Vec<(String, reliability::HostDelta)> = Vec::with_capacity(hosts.len());
    let mut flushed_now: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (host, tally) in hosts {
        let continues_run = !progress.started.insert(host.clone());
        if tally.http_lost() {
            progress.lost.insert(host.clone());
        }
        flushed_now.insert(host.clone());
        let url = format!("https://{host}/");
        let detail = format!(
            "crawl: {} http fetches ({} robots/sitemap probes)",
            tally.fetches, tally.probes
        );
        ctx.meter("http", Some(&url), 0.0, Some(&detail)).await;
        let http_lost = progress.lost.contains(&host);
        ctx.learn_tier(&host, tier_that_won(http_lost), http_lost)
            .await;
        deltas.push((
            host,
            reliability::HostDelta::Crawl(reliability::CrawlHostObs {
                fetches: tally.fetches as u64,
                ok: tally.ok as u64,
                botwall: tally.botwall as u64,
                transport_errors: tally.transport_errors as u64,
                not_modified: tally.not_modified as u64,
                gone: tally.gone as u64,
                validators_seen: tally.validators_seen as u64,
                probes: tally.probes as u64,
                continues_run,
                run_complete: final_flush,
            }),
        ));
    }
    if final_flush {
        // A host last touched before the previous flush has no new counters, but
        // its record still says the run is in progress. Close it out with an
        // empty delta, or a completed crawl would leave `partial: true` behind
        // on every host that went quiet early.
        let mut tail: Vec<String> = progress
            .started
            .difference(&flushed_now)
            .cloned()
            .collect();
        tail.sort();
        for host in tail {
            deltas.push((
                host,
                reliability::HostDelta::Crawl(reliability::CrawlHostObs {
                    continues_run: true,
                    run_complete: true,
                    ..reliability::CrawlHostObs::default()
                }),
            ));
        }
    }
    reliability::record_observations(&ctx.datasets, &ctx.job_id.to_string(), deltas).await;
}

/// Max existing `pages` records loaded as revisit seeds per run (bounds the
/// dataset read and the frontier). A larger known set is revisited across runs.
const REVISIT_SEED_LIMIT: i64 = 10_000;

/// Running new/changed/unchanged tallies shared between the [`DatasetPageSink`]
/// (which adds to them as batches upsert) and the app (which reads them into the
/// job result once the crawl returns). Atomics avoid holding a lock across the
/// sink's `.await`.
#[derive(Default)]
struct PageCounts {
    new: AtomicUsize,
    changed: AtomicUsize,
    unchanged: AtomicUsize,
    /// Page revisions archived into `page_versions` (with a revision-suffixed
    /// artifact copy) because a revisit found the body CHANGED.
    versions_archived: AtomicUsize,
    /// Cadence-only counter merges written for `304` check markers (M07). Kept
    /// separate from `changed` — a cadence bump is bookkeeping, not content.
    cadence_updates: AtomicUsize,
    /// Link-graph edges the store actually WROTE into the `edges` dataset (M08):
    /// `summary.new + summary.changed`, exactly as `pages` counts its own writes.
    edges_written: AtomicUsize,
    /// Edge rows the store found already present and identical — a no-op upsert.
    /// Kept beside `edges_written` so the batch total stays recoverable now that
    /// `edges_written` no longer counts no-ops as writes.
    edges_unchanged: AtomicUsize,
}

/// Full stored `pages` record per URL, captured at revisit-seed load so the
/// sink can merge a 304's cadence bump into the record WITHOUT re-reading the
/// dataset (the read already happened to build the seeds). std Mutex — quick
/// map ops only, never held across an `.await`.
type SeedData = Arc<Mutex<HashMap<String, Value>>>;

/// Dataset holding the versioned crawl archive: one record per CHANGED revision
/// of a page, keyed `{url}#{revision}`. Each record carries `artifact_path` +
/// `job_id` in the exact shape `AppContext::read_source_artifact` expects, so
/// extractor/plugin `source` mode can read historical bodies unchanged.
///
/// Retention: the archive is capped by the EXISTING dataset retention seams, no
/// new janitor — `Datasets::prune_revisions` / the dataset prune API bounds the
/// `page_versions` revision history like any other dataset, and the janitor
/// (OFF by default) plus per-job artifact cleanup bound the on-disk copies.
/// Storage grows only with real change (unchanged revisits archive nothing).
const VERSIONS_DATASET: &str = "page_versions";

/// Inserts a `.r{revision}` suffix before the artifact's `.html` extension
/// (`page-<hex>.html` → `page-<hex>.r3.html`), so each archived revision owns a
/// distinct file while the un-suffixed name keeps meaning "latest". Falls back
/// to appending for non-`.html` names. Stays a single safe path segment.
fn versioned_artifact_name(artifact_path: &str, revision: i64) -> String {
    match artifact_path.strip_suffix(".html") {
        Some(stem) => format!("{stem}.r{revision}.html"),
        None => format!("{artifact_path}.r{revision}"),
    }
}

/// sha256 (hex) of an archived page body — the value stamped as
/// [`Provenance::artifact_sha`], and the same digest form the rest of the
/// platform uses for content addressing.
fn body_sha(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// [`PageSink`] that upserts each batch of kept-page fingerprints into the
/// `pages` dataset (key = canonical URL). Uses `upsert_many` — partial-batch
/// semantics, never `sync_many` (a crawl is a partial view, not a full snapshot,
/// so absent keys must NOT be marked removed). Errors are logged, never fatal:
/// dataset side-effects must not fail the crawl.
struct DatasetPageSink {
    datasets: Arc<Datasets>,
    app: String,
    job_id: String,
    /// This job's artifact dir — where core wrote each kept page's latest body
    /// (URL-hash name). On `changed` the sink copies that body to a
    /// revision-suffixed file and records it in [`VERSIONS_DATASET`].
    artifacts_dir: PathBuf,
    counts: Arc<PageCounts>,
    /// Stored record data per seeded URL (revisit runs; empty otherwise) — the
    /// merge base for 304 cadence markers.
    seed_data: SeedData,
    /// Within-run link-graph state (M08): `(from, to)` dedup + in-degree tally
    /// shared with the app for the `top_linked` result summary. std Mutex —
    /// quick map ops only, never held across an `.await`.
    edges: Arc<Mutex<link_graph::EdgeGraph>>,
}

impl DatasetPageSink {
    /// The batch-level derivation stamp (M12) for this sink's writes: the
    /// producing job, and nothing else.
    ///
    /// A batch spans many pages, so a batch-level `source_url` would be a
    /// fabrication — and it would also be redundant: a `pages`/`edges` record's
    /// key IS its URL (`{url}` / `{from}|{to}`), so the per-record source is
    /// already recoverable without inventing one. Per-record stamping here
    /// would mean one write transaction per crawled page on the platform's
    /// highest-volume write path — the exact amplification `upsert_many`'s
    /// chunked commits exist to avoid. `rules_hash` stays `None`: a crawl runs
    /// no RuleSet, it fingerprints whole bodies.
    fn job_prov(&self) -> Provenance {
        Provenance {
            job_id: Some(self.job_id.clone()),
            ..Provenance::default()
        }
    }

    /// Archives the CHANGED keys of one upsert batch into the versioned crawl
    /// archive: copy `page-<hex>.html` → `page-<hex>.r{revision}.html` and upsert
    /// a `page_versions` record keyed `{url}#{revision}` (artifact path, simhash,
    /// fetched_at). New first-sightings and unchanged revisits archive nothing —
    /// the un-suffixed artifact already IS the latest body, so the archive grows
    /// only when a page actually changes. Best-effort like the rest of the sink:
    /// an archive failure warns and skips, never fails the crawl.
    async fn archive_changed(&self, changed: &[String], meta: &HashMap<String, (String, u64)>) {
        // (key, value, artifact sha) — the archive is the ONE crawl write path
        // whose per-record derivation facts are genuinely known and not already
        // in the key: the page URL behind `{url}#{revision}`, and the sha256 of
        // the exact body copy this record points at. Volume is changed pages
        // only, and this path already does a per-record history read + file
        // write, so per-record stamping adds no new write pattern.
        let mut versions: Vec<(String, Value, String)> = Vec::new();
        for url in changed {
            let Some((artifact_path, simhash)) = meta.get(url) else {
                continue; // not in this batch (shouldn't happen) — nothing to copy
            };
            if artifact_path.is_empty() {
                continue; // crawl ran without an output dir — no body to archive
            }
            // The revision number (and its authoritative timestamp) of the write
            // that just happened comes from the record's revision history.
            let rev = match self.datasets.history(&self.app, "pages", url, 1).await {
                Ok(revs) => match revs.into_iter().next() {
                    Some(r) => r,
                    None => continue, // changed key with no revision row — skip
                },
                Err(e) => {
                    tracing::warn!(url = %url, "crawl version archive: history read failed: {e}");
                    continue;
                }
            };
            let versioned = versioned_artifact_name(artifact_path, rev.revision);
            let src = self.artifacts_dir.join(artifact_path);
            let dst = self.artifacts_dir.join(&versioned);
            // Read → hash → write rather than `fs::copy`: the copy has to touch
            // every byte anyway, so the sha256 of the archived body is free here
            // and turns the revision into a content-addressed one (M12).
            let body = match tokio::fs::read(&src).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(url = %url, path = %src.display(),
                        "crawl version archive: artifact read failed: {e}");
                    continue;
                }
            };
            let sha = body_sha(&body);
            if let Err(e) = tokio::fs::write(&dst, &body).await {
                tracing::warn!(url = %url, path = %dst.display(),
                    "crawl version archive: artifact copy failed: {e}");
                continue;
            }
            versions.push((
                format!("{url}#{}", rev.revision),
                json!({
                    "url": url,
                    "revision": rev.revision,
                    // Same {artifact_path, job_id} contract as `pages` records, so
                    // read_source_artifact resolves historical bodies unchanged.
                    "artifact_path": versioned,
                    "job_id": self.job_id,
                    "simhash": simhash,
                    "fetched_at": rev.created_at.to_rfc3339(),
                    // Content address of the archived body this record points
                    // at — the same value stamped as the revision's
                    // `artifact_sha`, kept in the record too so a reader that
                    // has the row can verify the file without the ledger.
                    "artifact_sha": sha,
                }),
                sha,
            ));
        }
        if versions.is_empty() {
            return;
        }
        let mut archived = 0usize;
        for (key, value, sha) in &versions {
            let prov = Provenance {
                job_id: Some(self.job_id.clone()),
                // The page URL itself — a real per-record fact, and NOT the
                // record key here (the key carries the `#revision` suffix).
                source_url: value.get("url").and_then(Value::as_str).map(String::from),
                artifact_sha: Some(sha.clone()),
                // No RuleSet extracted this record: the crawl archives whole
                // bodies. `None` = unknown, never a fabricated pin.
                rules_hash: None,
            };
            match self
                .datasets
                .upsert_stamped(&self.app, VERSIONS_DATASET, key, value, None, Some(&prov))
                .await
            {
                Ok(_) => archived += 1,
                Err(e) => {
                    tracing::warn!(job = %self.job_id, key = %key,
                        "crawl page_versions upsert failed: {e}")
                }
            }
        }
        self.counts
            .versions_archived
            .fetch_add(archived, Ordering::Relaxed);
    }
}

#[async_trait]
impl PageSink for DatasetPageSink {
    async fn emit(&mut self, batch: Vec<CrawlPageRecord>) {
        // Split live fingerprints from revisit `gone` markers: gone records upsert
        // a `{gone: true}` value (an explicit per-key removal → a `changed`
        // revision that triggers/watches fire on) and are NOT counted as changed.
        let mut live: Vec<(String, Value)> = Vec::new();
        let mut gone: Vec<(String, Value)> = Vec::new();
        // 304 check markers: cadence counters merged onto the stored record.
        let mut checks: Vec<(String, Value)> = Vec::new();
        // url → (artifact_path, simhash) for this batch's live pages, kept so the
        // version archive can copy the just-written body of any CHANGED key.
        let mut live_meta: HashMap<String, (String, u64)> = HashMap::new();
        // Link-graph edge records for this batch (M08): capped, run-dedup'd.
        let mut edge_rows: Vec<(String, Value)> = Vec::new();
        for p in batch {
            if p.unchanged {
                // Merge the bumped cadence into the record as loaded at seed
                // time — everything else about the page is by definition
                // unchanged (the origin said 304). NOTE: this UPSERT is a
                // changed revision (the cadence moved), so watches/triggers on
                // `pages` see check bookkeeping; bounded per URL per run.
                let mut base = self
                    .seed_data
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&p.url)
                    .cloned()
                    .unwrap_or_else(|| json!({ "url": p.url }));
                if let Value::Object(map) = &mut base {
                    map.insert(
                        "cadence".into(),
                        serde_json::to_value(&p.cadence).unwrap_or(Value::Null),
                    );
                }
                checks.push((p.url.clone(), base));
            } else if p.gone {
                gone.push((
                    p.url.clone(),
                    json!({ "url": p.url, "status": p.status, "gone": true, "job_id": self.job_id }),
                ));
            } else {
                // Persist the link graph (M08): this kept page's outbound links
                // (canonicalized/filtered by core exactly as the frontier saw
                // them) become `edges` records. Lock scope is synchronous.
                if !p.links.is_empty() {
                    edge_rows.extend(
                        self.edges
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .page_edges(&p.url, p.depth, &p.links, &self.job_id),
                    );
                }
                live_meta.insert(p.url.clone(), (p.artifact_path.clone(), p.simhash));
                live.push((
                    p.url.clone(),
                    json!({
                        "url": p.url,
                        "title": p.title,
                        "status": p.status,
                        "content_chars": p.content_chars,
                        "simhash": p.simhash,
                        "excerpt": p.excerpt,
                        "artifact_path": p.artifact_path,
                        "depth": p.depth,
                        // Conditional-GET validators, stored so the next revisit
                        // can send If-None-Match / If-Modified-Since.
                        "etag": p.etag,
                        "last_modified": p.last_modified,
                        // Learned change-cadence counters (M07): checks/changes/
                        // last_change_at + EWMA interval, read back as revisit
                        // seeds to drive the due-score frontier.
                        "cadence": p.cadence,
                        "job_id": self.job_id,
                    }),
                ));
            }
        }
        // One derivation stamp per batch write (M12): the producing job. See
        // `job_prov` for why per-record source URLs are deliberately not
        // stamped on these paths.
        let prov = self.job_prov();
        if !live.is_empty() {
            match self
                .datasets
                .upsert_many_stamped(&self.app, "pages", &live, None, Some(&prov))
                .await
            {
                Ok(summary) => {
                    self.counts
                        .new
                        .fetch_add(summary.new.len(), Ordering::Relaxed);
                    self.counts
                        .changed
                        .fetch_add(summary.changed.len(), Ordering::Relaxed);
                    self.counts
                        .unchanged
                        .fetch_add(summary.unchanged, Ordering::Relaxed);
                    // Versioned crawl archive: every CHANGED key gets its new body
                    // copied to a revision-suffixed artifact + a page_versions row.
                    if !summary.changed.is_empty() {
                        self.archive_changed(&summary.changed, &live_meta).await;
                    }
                }
                Err(e) => tracing::warn!(job = %self.job_id, "crawl pages upsert failed: {e}"),
            }
        }
        if !gone.is_empty() {
            if let Err(e) = self
                .datasets
                .upsert_many_stamped(&self.app, "pages", &gone, None, Some(&prov))
                .await
            {
                tracing::warn!(job = %self.job_id, "crawl gone-marker upsert failed: {e}");
            }
        }
        if !checks.is_empty() {
            // Deliberately NOT folded into new/changed counts: a cadence bump is
            // estimator bookkeeping, not observed content change.
            match self
                .datasets
                .upsert_many_stamped(&self.app, "pages", &checks, None, Some(&prov))
                .await
            {
                Ok(_) => {
                    self.counts
                        .cadence_updates
                        .fetch_add(checks.len(), Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!(job = %self.job_id, "crawl cadence-marker upsert failed: {e}")
                }
            }
        }
        if !edge_rows.is_empty() {
            // Same partial-batch upsert semantics as `pages`: an edge absent
            // this run is NOT removed (a crawl is a partial view). Best-effort
            // like the rest of the sink — a failure warns, never fails the crawl.
            match self
                .datasets
                .upsert_many_stamped(
                    &self.app,
                    link_graph::EDGES_DATASET,
                    &edge_rows,
                    None,
                    Some(&prov),
                )
                .await
            {
                Ok(summary) => {
                    // What the STORE did, not what we handed it. Discarding the
                    // summary and adding `edge_rows.len()` counted every no-op
                    // upsert as a write, so a re-crawl of a stable site reported
                    // its whole link graph as freshly written — while `pages`
                    // two hundred lines above always used the summary.
                    self.counts
                        .edges_written
                        .fetch_add(summary.new.len() + summary.changed.len(), Ordering::Relaxed);
                    self.counts
                        .edges_unchanged
                        .fetch_add(summary.unchanged, Ordering::Relaxed);
                }
                Err(e) => tracing::warn!(job = %self.job_id, "crawl edges upsert failed: {e}"),
            }
        }
    }
}

/// [`PageSource`] that reads existing live `pages` records to seed a revisit —
/// the read-side mirror of [`DatasetPageSink`]. Skips already-removed and
/// already-`gone` records so a sentinel doesn't keep re-probing dead URLs.
struct DatasetPageSource {
    datasets: Arc<Datasets>,
    app: String,
    limit: i64,
    /// Populated during `seeds()` with each seeded URL's full record data — the
    /// sink's merge base for 304 cadence markers (no second dataset read).
    seed_data: SeedData,
}

#[async_trait]
impl PageSource for DatasetPageSource {
    async fn seeds(&self) -> Vec<RevisitSeed> {
        match self.datasets.list(&self.app, "pages", self.limit).await {
            Ok(records) => {
                let mut seeds = Vec::new();
                let mut data_map = HashMap::new();
                for r in records {
                    if r.removed_at.is_some()
                        || r.data.get("gone").and_then(Value::as_bool).unwrap_or(false)
                    {
                        continue;
                    }
                    // Learned cadence counters written by DatasetPageSink;
                    // records that predate them default to a cold-start seed.
                    let cadence = r
                        .data
                        .get("cadence")
                        .cloned()
                        .and_then(|v| serde_json::from_value::<RevisitCadence>(v).ok())
                        .unwrap_or_default();
                    data_map.insert(r.key.clone(), r.data.clone());
                    seeds.push(RevisitSeed {
                        etag: r.data.get("etag").and_then(Value::as_str).map(String::from),
                        last_modified: r
                            .data
                            .get("last_modified")
                            .and_then(Value::as_str)
                            .map(String::from),
                        simhash: r.data.get("simhash").and_then(Value::as_u64).unwrap_or(0),
                        cadence,
                        // The record key is the canonical URL (see DatasetPageSink).
                        url: r.key,
                    });
                }
                *self.seed_data.lock().unwrap_or_else(|e| e.into_inner()) = data_map;
                seeds
            }
            Err(e) => {
                tracing::warn!(app = %self.app, "crawl revisit seed load failed: {e}");
                Vec::new()
            }
        }
    }
}

#[async_trait]
impl ScrapeApp for Crawl {
    fn name(&self) -> &'static str {
        "crawl"
    }

    fn description(&self) -> &'static str {
        "High-concurrency broad crawler. Params: {\"seeds\": [..], \"max_pages\": 50, \
         \"max_depth\": 2, \"concurrency\": 16, \
         \"max_pages_per_host\": null (per-host page cap for host-fair multi-seed \
         crawls; 0/absent = unlimited), \"same_domain\": true, \
         \"dedup_distance\": 3, \"respect_robots\": true, \
         \"include_patterns\": [\"regex\", ..], \"exclude_patterns\": [\"regex\", ..], \
         \"sitemap_seeds\": false, \
         \"mode\": \"revisit\" (incremental recrawl of the `pages` dataset via \
         conditional GETs; \"discover\": true opts into link-following; \
         \"revisit_budget\" + \"min_due_score\" spend the budget on the URLs \
         most likely changed, per learned per-URL change cadence)}. \
         Frontier state is checkpointed durably per job: an interrupted, reaped, \
         or shutdown-suspended crawl resumes where it left off on its next attempt. \
         Changed pages are archived into the `page_versions` dataset (key \
         `{url}#{revision}`, revision-suffixed artifact copy) — retention is the \
         existing dataset prune API / janitor, no separate knob. Each kept \
         page's outbound links are persisted into the `edges` dataset (key \
         `{from_url}|{to_url}`; per-page out-degree cap 200, dropped counted) — \
         the run's most-linked-to pages are echoed as `top_linked`. Every write \
         is provenance-stamped with the producing job; archived `page_versions` \
         revisions additionally carry the page URL and the sha256 of the exact \
         archived body."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "seeds": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^https?://" },
                        "description": "Start URLs. Required for a fresh crawl; `mode: revisit` runs need none."
                    },
                    "max_pages": { "type": "integer", "minimum": 1 },
                    "max_depth": { "type": "integer", "minimum": 0 },
                    "concurrency": { "type": "integer", "minimum": 1, "maximum": 64 },
                    "max_pages_per_host": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "description": "Per-host page cap for host-fair multi-seed crawls; 0/null = unlimited."
                    },
                    "same_domain": { "type": "boolean" },
                    "dedup_distance": { "type": "integer", "minimum": 0, "maximum": 20 },
                    "respect_robots": { "type": "boolean" },
                    "include_patterns": { "type": "array", "items": { "type": "string" } },
                    "exclude_patterns": { "type": "array", "items": { "type": "string" } },
                    "sitemap_seeds": { "type": "boolean" },
                    "mode": {
                        "type": "string",
                        "enum": ["revisit"],
                        "description": "Incremental recrawl of the existing `pages` dataset via conditional GETs."
                    },
                    "discover": { "type": "boolean" },
                    "revisit_budget": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "description": "Revisit mode: max known pages fetched this run, spent on the highest due-score URLs (learned change cadence). Absent/0 = all seeds."
                    },
                    "min_due_score": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Revisit mode: skip seeds whose probability-changed-since-last-check falls below this (0 = fetch all; skipped seeds are counted in skipped_not_due)."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Shallow same-domain crawl of one site, robots-respecting",
                    params: json!({
                        "seeds": ["https://example.com/"],
                        "max_pages": 50,
                        "max_depth": 2,
                        "same_domain": true
                    }),
                },
                ManifestExample {
                    description: "Incremental revisit of already-crawled pages (conditional GETs)",
                    params: json!({ "mode": "revisit", "max_pages": 200 }),
                },
            ],
            // Every key `run()` always returns, in emit order. An inventory test
            // (`tests/result_contract.rs`) diffs this list against a real run's
            // keys in BOTH directions — the manifest drifted through four
            // milestones unnoticed because nothing compared the two.
            output_shape: Some(OUTPUT_SHAPE),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let str_array = |key: &str| -> Vec<String> {
            ctx.params
                .get(key)
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        };
        // Revisit mode seeds the frontier from the `pages` dataset, so `seeds` is
        // optional there (it stays required for a normal fresh crawl).
        let revisit = ctx.params.get("mode").and_then(Value::as_str) == Some("revisit");
        let seeds = str_array("seeds");
        if seeds.is_empty() && !revisit {
            return Err(Error::App(
                "param 'seeds' must be a non-empty array (or set mode:\"revisit\")".into(),
            ));
        }

        let usize_param = |key: &str, default: usize| {
            ctx.params
                .get(key)
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .unwrap_or(default)
        };
        let u32_param = |key: &str, default: u32| {
            ctx.params
                .get(key)
                .and_then(Value::as_u64)
                .map(|n| n as u32)
                .unwrap_or(default)
        };
        let bool_param = |key: &str, default: bool| {
            ctx.params
                .get(key)
                .and_then(Value::as_bool)
                .unwrap_or(default)
        };

        let cfg = CrawlConfig {
            seeds,
            max_pages: usize_param("max_pages", 50),
            max_depth: u32_param("max_depth", 2),
            concurrency: usize_param("concurrency", 16),
            // Optional per-host page cap (0/absent = unlimited): keeps one big
            // seed from consuming the whole max_pages budget across seeds.
            max_pages_per_host: ctx
                .params
                .get("max_pages_per_host")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .filter(|&n| n > 0),
            same_domain: bool_param("same_domain", true),
            dedup_distance: u32_param("dedup_distance", 3),
            respect_robots: bool_param("respect_robots", true),
            include_patterns: str_array("include_patterns"),
            exclude_patterns: str_array("exclude_patterns"),
            sitemap_seeds: bool_param("sitemap_seeds", false),
            // Durable execution: a prior attempt's frontier checkpoint (persisted
            // through `ctx.checkpoint` below) comes back here on re-claim, so a
            // crashed/reaped/suspended crawl resumes instead of restarting. The
            // old app-private named-file checkpoint path is gone — the platform
            // seam owns persistence, lineage-guarding, and the poisoned-blob
            // escape now.
            resume_state: ctx.restore().cloned(),
            revisit,
            discover: bool_param("discover", false),
            // Learned change-cadence frontier (M07): spend the revisit budget on
            // the URLs most likely to have changed since last check.
            revisit_budget: ctx
                .params
                .get("revisit_budget")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .filter(|&n| n > 0),
            min_due_score: ctx
                .params
                .get("min_due_score")
                .and_then(Value::as_f64)
                .map(|s| s.clamp(0.0, 1.0))
                .unwrap_or(0.0),
        };

        // Per-page fingerprints stream into the `pages` dataset as the crawl
        // runs (key = canonical URL), so crawled pages become queryable/diffable
        // and dataset triggers + watches fire per-page.
        let counts = Arc::new(PageCounts::default());
        // Shared between source (writer, at seed load) and sink (reader, on 304
        // markers): the stored record data per seeded URL.
        let seed_data: SeedData = Arc::new(Mutex::new(HashMap::new()));
        // Link-graph state (M08), shared with the sink so the run result can
        // report the within-run `top_linked` summary + drop/dedup tallies.
        let edges = Arc::new(Mutex::new(link_graph::EdgeGraph::default()));
        let sink: Box<dyn PageSink> = Box::new(DatasetPageSink {
            datasets: ctx.datasets.clone(),
            app: ctx.app.clone(),
            job_id: ctx.job_id.to_string(),
            artifacts_dir: ctx.artifacts_dir.clone(),
            counts: counts.clone(),
            seed_data: seed_data.clone(),
            edges: edges.clone(),
        });

        // Revisit mode reads existing page records to seed the frontier.
        let source: Option<Box<dyn PageSource>> = revisit.then(|| {
            Box::new(DatasetPageSource {
                datasets: ctx.datasets.clone(),
                app: ctx.app.clone(),
                limit: REVISIT_SEED_LIMIT,
                seed_data: seed_data.clone(),
            }) as Box<dyn PageSource>
        });

        // Bridge core's crawl progress seam to the runtime reporter: each live
        // snapshot is persisted (visible on GET /jobs/{id}) and emitted as a
        // `progress` SSE event. The runtime throttles; this closure is cheap.
        let reporter = ctx.progress.clone();
        let progress: pumper_core::ProgressFn = Arc::new(move |snap| {
            reporter.report(serde_json::to_value(snap).unwrap_or_default());
        });

        // Meter the crawl's fetches: wrap the raw HTTP client so per-host outcomes
        // are tallied, then attribute them to the job through the cost ledger and
        // tier router after the crawl (see MeteringHttpClient).
        let tallies = Arc::new(Mutex::new(HashMap::<String, HostTally>::new()));
        let metered_http: Arc<dyn HttpClient> = Arc::new(MeteringHttpClient {
            inner: ctx.engines.http.clone(),
            tallies: tallies.clone(),
            sitemap_seeding: cfg.sitemap_seeds,
        });

        let crawl_fut = crawl(
            metered_http,
            cfg,
            Some(ctx.artifacts_dir.clone()),
            Some(sink),
            source,
            Some(progress),
            // Durable-execution seam: the crawler streams its frontier state
            // through the job's checkpoint sink (runtime-throttled, lineage-
            // guarded), which is what `resume_state` restores on re-claim.
            Some(ctx.checkpoints.clone()),
        );
        // Commit host telemetry WHILE the crawl runs, not only after it returns.
        // The interruption paths that motivate this (reap, shutdown drain, hard
        // timeout) DROP this future — no unwind, no `Err` — so nothing after it
        // is guaranteed to execute, and everything the run learned used to die
        // with it. Driven by `timeout` on one task rather than a spawned one:
        // `AppContext` is not `Clone`, and a flush must never race itself.
        let mut telemetry = TelemetryProgress::default();
        let stats = crawl_flushing_telemetry(
            &ctx,
            crawl_fut,
            &tallies,
            &mut telemetry,
            TELEMETRY_FLUSH_INTERVAL,
        )
        .await?;
        // Distinct hosts whose fetch telemetry this run folded into the index
        // (a run may fold a host several times; it is still one host).
        let reliability_hosts = telemetry.started.len();

        // Snapshot the link-graph run state (dropped, deduped, top_linked). The
        // crawl has returned, so the sink holds no lock.
        let edge_summary = {
            let g = edges.lock().unwrap_or_else(|e| e.into_inner());
            (g.dropped_out_degree, g.deduped, g.top_linked())
        };

        let pages_new = counts.new.load(Ordering::Relaxed);
        let pages_changed = counts.changed.load(Ordering::Relaxed);
        let pages_unchanged = counts.unchanged.load(Ordering::Relaxed);
        let mut out = json!({
            "crawled": stats.crawled,
            "kept": stats.kept,
            "skipped_duplicates": stats.skipped_duplicates,
            "skipped_robots": stats.skipped_robots,
            "skipped_filtered": stats.skipped_filtered,
            // Honest truncation accounting: core computes both counters so a
            // capped crawl is reported rather than silently short, and until now
            // the app surfaced neither. `coverage_complete` is the legible
            // verdict — a caller should not have to know that two zeros mean
            // "this crawl saw the whole discovered graph".
            "frontier_dropped": stats.frontier_dropped,
            "skipped_host_budget": stats.skipped_host_budget,
            "coverage_complete": coverage_complete(
                stats.frontier_dropped,
                stats.skipped_host_budget,
            ),
            "sitemap_seeded": stats.sitemap_seeded,
            // Honest failure/bot-wall accounting (previously swallowed silently).
            "failed": stats.failed,
            "failed_by_host": stats.failed_by_host,
            "skipped_botwall": stats.skipped_botwall,
            "robots_fetch_failures": stats.robots_fetch_failures,
            "checkpoint_errors": stats.checkpoint_errors,
            "resumed": stats.resumed,
            "checkpoint_reset": stats.checkpoint_reset,
            "hosts": stats.hosts,
            "frontier_remaining": stats.frontier_remaining,
            // Per-page metadata lives in the `pages` dataset (streamed during the
            // crawl), not in the result — only the write outcome is echoed here.
            "pages_dataset": "pages",
            "pages_new": pages_new,
            "pages_changed": pages_changed,
            "pages_unchanged": pages_unchanged,
            // Incremental-recrawl accounting (all 0 for a normal fresh crawl).
            "revisit": revisit,
            "revisited": stats.revisited,
            "unchanged_304": stats.unchanged_304,
            // Learned-cadence frontier accounting (M07): seeds skipped as
            // not-due / over-budget, and 304 cadence-counter merges written.
            "skipped_not_due": stats.skipped_not_due,
            "cadence_updates": counts.cadence_updates.load(Ordering::Relaxed),
            // `changed`/`new` = live pages re-fingerprinted / first-seen this run.
            "changed": pages_changed,
            "new": pages_new,
            "gone": stats.gone,
            // Versioned crawl archive: changed revisions copied to
            // revision-suffixed artifacts + `page_versions` records.
            "versions_archived": counts.versions_archived.load(Ordering::Relaxed),
            // Web Reliability Index: hosts whose fetch telemetry was folded into
            // `web-reliability/host_observations` + `host_index` this run.
            "reliability_hosts": reliability_hosts,
            // Persisted link graph (M08): edges upserted into the `edges`
            // dataset (key `{from}|{to}`), plus honest cap/dedup accounting and
            // the within-run most-linked-to pages.
            "edges_dataset": link_graph::EDGES_DATASET,
            "edges_written": counts.edges_written.load(Ordering::Relaxed),
            "edges_unchanged": counts.edges_unchanged.load(Ordering::Relaxed),
            "edges_dropped_out_degree": edge_summary.0,
            "edges_deduped": edge_summary.1,
            "top_linked": edge_summary.2,
        });
        // Fleet idiom: a partial result says so in `warnings`, and a complete one
        // stays quiet (cordis `aggregate_truncated`, census `blend_complete`).
        if let (Some(warning), Value::Object(map)) = (
            coverage_warning(stats.frontier_dropped, stats.skipped_host_budget),
            &mut out,
        ) {
            map.insert("warnings".into(), json!([warning]));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::{host_of, versioned_artifact_name, HostTally};
    use pumper_core::testing::TempStore;
    use pumper_core::{CrawlPageRecord, Error, HttpResponse, Result};
    use std::collections::HashMap;

    fn resp(status: u16, headers: &[(&str, &str)]) -> Result<HttpResponse> {
        Ok(HttpResponse {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            body: String::new(),
            final_url: "https://example.com/".into(),
            cache_hit: false,
        })
    }

    #[test]
    fn tally_classifies_outcomes_and_validators() {
        let mut t = HostTally::default();
        t.record(&resp(200, &[("ETag", "\"abc\"")]));
        t.record(&resp(304, &[]));
        t.record(&resp(404, &[]));
        t.record(&resp(429, &[]));
        t.record(&resp(200, &[("last-modified", "yesterday")]));
        t.record(&Err(Error::App("dns".into())));
        assert_eq!(t.fetches, 6);
        assert_eq!(t.ok, 2);
        assert_eq!(t.not_modified, 1);
        assert_eq!(t.gone, 1);
        assert_eq!(t.botwall, 1);
        assert_eq!(t.transport_errors, 1);
        assert_eq!(t.validators_seen, 2);
        assert!(t.http_lost());
    }

    #[test]
    fn tally_clean_host_is_not_lost() {
        let mut t = HostTally::default();
        t.record(&resp(200, &[]));
        t.record(&resp(304, &[]));
        assert!(!t.http_lost());
    }

    #[test]
    fn versioned_name_inserts_revision_before_html_suffix() {
        assert_eq!(
            versioned_artifact_name("page-ab12.html", 3),
            "page-ab12.r3.html"
        );
        assert_eq!(
            versioned_artifact_name("page-ab12.html", 1),
            "page-ab12.r1.html"
        );
    }

    #[test]
    fn versioned_name_appends_for_non_html_names() {
        assert_eq!(versioned_artifact_name("body.bin", 2), "body.bin.r2");
    }

    #[test]
    fn extracts_lowercased_host() {
        assert_eq!(
            host_of("https://Example.COM/path?q=1"),
            Some("example.com".into())
        );
        assert_eq!(host_of("http://example.com"), Some("example.com".into()));
    }

    #[test]
    fn strips_port_userinfo_and_path() {
        assert_eq!(
            host_of("https://user:pw@host.example:8443/a/b"),
            Some("host.example".into())
        );
        assert_eq!(
            host_of("https://host.example:443/"),
            Some("host.example".into())
        );
        assert_eq!(
            host_of("https://host.example/a?x#y"),
            Some("host.example".into())
        );
    }

    #[test]
    fn rejects_empty_or_hostless() {
        assert_eq!(host_of("https:///just-a-path"), None);
        assert_eq!(host_of(""), None);
    }

    // ── probe vs page ───────────────────────────────────────────────────────

    #[test]
    fn a_missing_robots_txt_is_not_a_dead_page() {
        // THE REFUTED BEHAVIOR: the metering client wraps the client core hands
        // to `robots_for`, so a host with no robots.txt answered 404 → `gone` →
        // folded into the reliability index. Every crawl fabricated a gone-page
        // observation for every host that simply has no robots.txt.
        assert!(!is_page_fetch("https://example.com/robots.txt", false));
        assert!(!is_page_fetch("http://example.com/robots.txt?x=1", false));
        // ...while a real page is still measured, including one that merely
        // mentions robots.
        assert!(is_page_fetch("https://example.com/", false));
        assert!(is_page_fetch("https://example.com/docs/robots.txt.html", false));
        assert!(is_page_fetch("https://example.com/about/robots.txt", false));
    }

    #[test]
    fn sitemaps_count_as_probes_only_on_runs_that_fetch_them() {
        // A page genuinely named /sitemap.xml on an ordinary crawl is a page —
        // the crawl never probed for it, it was discovered like any other URL.
        assert!(is_page_fetch("https://example.com/sitemap.xml", false));
        assert!(!is_page_fetch("https://example.com/sitemap.xml", true));
        assert!(!is_page_fetch("https://example.com/sitemaps/products-1.xml", true));
        assert!(!is_page_fetch("https://example.com/sitemap.xml.gz", true));
        // Not every XML document is a sitemap.
        assert!(is_page_fetch("https://example.com/feed.xml", true));
    }

    #[test]
    fn path_of_ignores_authority_query_and_fragment() {
        assert_eq!(path_of("https://h.example/robots.txt?a=1#f"), Some("/robots.txt"));
        assert_eq!(path_of("https://user:pw@h.example:8443/a/b"), Some("/a/b"));
        assert_eq!(path_of("https://h.example"), Some("/"));
        assert_eq!(path_of("https://h.example?q=1"), Some("/"));
    }

    // ── truncation honesty ──────────────────────────────────────────────────

    #[test]
    fn a_complete_crawl_is_not_warned_about() {
        // The control arm: if a complete crawl warned, the flag would be noise
        // on every run and nobody would read it.
        assert!(coverage_complete(0, 0));
        assert_eq!(coverage_warning(0, 0), None);
    }

    #[test]
    fn a_truncated_crawl_is_not_silent() {
        // THE REFUTED BEHAVIOR: both counters existed, both were computed, and
        // a crawl that discarded 12,000 discovered URLs returned a result
        // byte-identical to one that saw the whole site.
        assert!(!coverage_complete(12_000, 0));
        let w = coverage_warning(12_000, 0).expect("a dropped frontier is truncation");
        assert!(w.contains("12000"), "{w}");
        assert!(w.contains("frontier cap"), "{w}");
        assert!(
            !w.contains("max_pages_per_host"),
            "no cause it didn't hit: {w}"
        );
    }

    #[test]
    fn a_host_budget_dump_is_not_silent() {
        assert!(!coverage_complete(0, 340));
        let w = coverage_warning(0, 340).expect("a dumped host backlog is truncation");
        assert!(w.contains("340"), "{w}");
        assert!(w.contains("max_pages_per_host"), "{w}");
    }

    #[test]
    fn both_truncation_causes_are_named_not_just_the_first() {
        let w = coverage_warning(7, 9).expect("truncated");
        assert!(w.contains('7') && w.contains('9'), "{w}");
        assert!(
            w.contains(" and "),
            "both causes belong in one warning: {w}"
        );
    }

    #[test]
    fn output_shape_key_block_parses_to_bare_keys() {
        let keys = output_shape_keys();
        assert!(keys.contains(&"crawled"), "{keys:?}");
        assert!(keys.contains(&"top_linked"), "{keys:?}");
        // The prose tail (which itself contains `{from_url}`) must not leak in.
        assert!(!keys.iter().any(|k| k.contains(' ')), "{keys:?}");
    }

    // ── provenance (M12) ────────────────────────────────────────────────────

    const JOB: &str = "11111111-2222-3333-4444-555555555555";

    fn page(url: &str, simhash: u64, artifact: &str) -> CrawlPageRecord {
        CrawlPageRecord {
            url: url.into(),
            title: Some("t".into()),
            status: 200,
            content_chars: 10,
            simhash,
            excerpt: "x".into(),
            artifact_path: artifact.into(),
            depth: 0,
            etag: None,
            last_modified: None,
            gone: false,
            unchanged: false,
            cadence: None,
            links: vec!["https://example.com/b".into()],
        }
    }

    /// Builds a sink over a temp store, with the page body already on disk
    /// exactly as core's crawl would have written it.
    async fn sink_over(store: &TempStore, body: &[u8], artifact: &str) -> DatasetPageSink {
        let artifacts_dir = store.path().join("job-artifacts");
        tokio::fs::create_dir_all(&artifacts_dir).await.unwrap();
        tokio::fs::write(artifacts_dir.join(artifact), body)
            .await
            .unwrap();
        DatasetPageSink {
            datasets: Arc::new(store.datasets()),
            app: "crawl".into(),
            job_id: JOB.into(),
            artifacts_dir,
            counts: Arc::new(PageCounts::default()),
            seed_data: Arc::new(Mutex::new(HashMap::new())),
            edges: Arc::new(Mutex::new(link_graph::EdgeGraph::default())),
        }
    }

    #[tokio::test]
    async fn page_and_edge_writes_are_stamped_with_the_producing_job_only() {
        let store = TempStore::new("crawl-prov-pages").await;
        let datasets = store.datasets();
        let mut sink = sink_over(&store, b"<html>one</html>", "page-a.html").await;
        sink.emit(vec![page("https://example.com/a", 7, "page-a.html")])
            .await;

        let rev = datasets
            .history("crawl", "pages", "https://example.com/a", 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(rev.provenance.job_id.as_deref(), Some(JOB));
        // Honest-Null on a batch path: a batch spans many pages, and the key
        // already IS the URL — a batch-level source_url would be invented.
        assert!(rev.provenance.source_url.is_none());
        assert!(rev.provenance.rules_hash.is_none());
        assert!(rev.provenance.artifact_sha.is_none());

        let edge = datasets
            .history(
                "crawl",
                link_graph::EDGES_DATASET,
                "https://example.com/a|https://example.com/b",
                1,
            )
            .await
            .unwrap()
            .remove(0);
        assert_eq!(edge.provenance.job_id.as_deref(), Some(JOB));
        assert!(edge.provenance.source_url.is_none());
    }

    // ── telemetry survives interruption ─────────────────────────────────────
    //
    // Driven at millisecond cadence against a real store. A paused tokio clock
    // is not usable here: auto-advance fires the SQLite pool's acquire timeout,
    // so `TempStore::new` itself fails. `interval` being a parameter of
    // `crawl_flushing_telemetry` is what makes these tests possible.

    use pumper_core::testing::TestContext;
    use std::time::Duration;

    const FAST: Duration = Duration::from_millis(25);

    /// One host's worth of successful page fetches.
    fn tally_of(fetches: usize) -> HostTally {
        let mut t = HostTally::default();
        for _ in 0..fetches {
            t.record(&resp(200, &[]));
        }
        t
    }

    async fn day_record(store: &TempStore, host: &str) -> Value {
        let key = reliability::obs_key(host, &chrono::Utc::now().format("%Y-%m-%d").to_string());
        store
            .datasets()
            .get(reliability::APP, reliability::OBS_DATASET, &key)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("no observation recorded for {host}"))
            .data
    }

    #[tokio::test]
    async fn an_abandoned_crawl_has_already_committed_what_it_learned() {
        // THE REFUTED BEHAVIOR: every commit sat after the `?` on `crawl(...)`,
        // so dropping the run future — exactly what the worker does on a cancel
        // or a hard timeout — threw away the whole run's telemetry. The resume
        // state carries no tallies either, so the next attempt could not
        // recover them.
        let store = TempStore::new("crawl-flush-abandoned").await;
        let ctx = TestContext::new(&store.storage, "crawl").build();
        let job_id = ctx.job_id;
        let tallies = Arc::new(Mutex::new(HashMap::new()));
        tallies
            .lock()
            .unwrap()
            .insert("example.com".to_string(), tally_of(7));
        let mut telemetry = TelemetryProgress::default();

        // A crawl that never returns, abandoned mid-flight.
        let never = std::future::pending::<Result<pumper_core::CrawlStats>>();
        let outcome = tokio::time::timeout(
            Duration::from_millis(400),
            crawl_flushing_telemetry(&ctx, never, &tallies, &mut telemetry, FAST),
        )
        .await;
        assert!(outcome.is_err(), "fixture is wrong: this crawl never ends");

        let obs = day_record(&store, "example.com").await;
        assert_eq!(obs["crawl"]["fetches"], 7, "{obs}");
        assert_eq!(obs["crawl"]["runs"], 1, "{obs}");
        // ...and it says so: a consumer can weight a partial contribution.
        assert_eq!(obs["crawl"]["runs_complete"], 0, "{obs}");
        assert_eq!(obs["crawl"]["partial"], true, "{obs}");

        // The two seams the end-of-run drain also used to skip.
        let events = pumper_core::costs::CostLedger::new(store.storage.pool())
            .job_events(job_id)
            .await
            .unwrap();
        assert!(!events.is_empty(), "the cost ledger lost the run");
        assert!(
            pumper_core::tiers::TierMemory::new(store.storage.pool(), 0)
                .get("example.com")
                .await
                .unwrap()
                .is_some(),
            "the tier router lost what the run learned about this host"
        );
    }

    #[tokio::test]
    async fn periodic_flushes_of_one_run_stay_one_run() {
        // The control arm: committing early must not turn one crawl into many.
        // `low_confidence` keys off `observations`, so an inflated count would
        // let a single long crawl manufacture confidence in its own numbers.
        let store = TempStore::new("crawl-flush-one-run").await;
        let ctx = TestContext::new(&store.storage, "crawl").build();
        let tallies = Arc::new(Mutex::new(HashMap::new()));
        let mut telemetry = TelemetryProgress::default();

        let feeding = {
            let tallies = tallies.clone();
            async move {
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    tallies
                        .lock()
                        .unwrap()
                        .entry("example.com".to_string())
                        .or_insert_with(HostTally::default)
                        .record(&resp(200, &[]));
                }
                Ok(pumper_core::CrawlStats::default())
            }
        };
        crawl_flushing_telemetry(&ctx, feeding, &tallies, &mut telemetry, FAST)
            .await
            .unwrap();

        let obs = day_record(&store, "example.com").await;
        assert_eq!(obs["crawl"]["fetches"], 5, "every fetch, counted once: {obs}");
        assert_eq!(obs["crawl"]["runs"], 1, "not one run per flush: {obs}");
        assert_eq!(obs["crawl"]["runs_complete"], 1, "{obs}");
        assert_eq!(obs["crawl"]["partial"], false, "{obs}");
        let idx = store
            .datasets()
            .get(reliability::APP, reliability::INDEX_DATASET, "example.com")
            .await
            .unwrap()
            .expect("index record")
            .data;
        assert_eq!(idx["observations"], 1, "{idx}");
    }

    #[tokio::test]
    async fn a_host_that_went_quiet_early_is_still_closed_out() {
        // A host touched only at the start is drained by an early flush and has
        // nothing left for the final one. Without an explicit completion delta
        // it would keep `partial: true` forever, so a finished crawl would look
        // interrupted on every host that stopped appearing.
        let store = TempStore::new("crawl-flush-quiet-host").await;
        let ctx = TestContext::new(&store.storage, "crawl").build();
        let tallies = Arc::new(Mutex::new(HashMap::new()));
        tallies
            .lock()
            .unwrap()
            .insert("early.example".to_string(), tally_of(3));
        let mut telemetry = TelemetryProgress::default();

        let later = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(pumper_core::CrawlStats::default())
        };
        crawl_flushing_telemetry(&ctx, later, &tallies, &mut telemetry, FAST)
            .await
            .unwrap();

        let obs = day_record(&store, "early.example").await;
        assert_eq!(obs["crawl"]["fetches"], 3, "{obs}");
        assert_eq!(obs["crawl"]["runs"], 1, "{obs}");
        assert_eq!(obs["crawl"]["runs_complete"], 1, "{obs}");
        assert_eq!(obs["crawl"]["partial"], false, "{obs}");
    }

    #[tokio::test]
    async fn a_failing_crawl_still_records_which_hosts_failed() {
        // The `?` used to swallow this: a crawl that dies at the fetch layer is
        // exactly when the reliability index most wants the observation.
        let store = TempStore::new("crawl-flush-on-error").await;
        let ctx = TestContext::new(&store.storage, "crawl").build();
        let tallies = Arc::new(Mutex::new(HashMap::new()));
        let mut t = HostTally::default();
        t.record(&Err(Error::App("dns".into())));
        tallies
            .lock()
            .unwrap()
            .insert("broken.example".to_string(), t);
        let mut telemetry = TelemetryProgress::default();

        let failing = async { Err(Error::App("crawl blew up".into())) };
        let err = crawl_flushing_telemetry(&ctx, failing, &tallies, &mut telemetry, FAST)
            .await
            .expect_err("the crawl's error still propagates");
        assert!(err.to_string().contains("blew up"));

        let obs = day_record(&store, "broken.example").await;
        assert_eq!(obs["crawl"]["transport_errors"], 1, "{obs}");
        assert_eq!(obs["crawl"]["runs_complete"], 1, "a finished-with-error run: {obs}");
    }

    #[tokio::test]
    /// `TierMemory::record` resets a host's strikes the moment the winner is
    /// `"http"`, before it reads `http_lost` at all. Reporting a hardcoded
    /// `"http"` therefore erased the bot-wall signal this call exists to teach —
    /// and, because `tier_memory` is keyed by host alone, erased what other apps
    /// had learned about that host too.
    #[test]
    fn a_lost_host_does_not_report_an_http_win_that_would_clear_its_strikes() {
        assert_eq!(tier_that_won(false), "http", "a clean host IS an http win");
        assert_ne!(
            tier_that_won(true),
            "http",
            "a bot-walled host must not be reported as an http win"
        );
        assert_eq!(tier_that_won(true), NO_TIER_WON);
    }

    #[tokio::test]
    async fn a_clean_late_flush_does_not_erase_a_bot_wall_the_run_already_hit() {
        // `learn_tier` RESETS a host's strikes on a win, so feeding it each
        // flush's own delta would let a quiet tail of the crawl wipe out a
        // bot-wall seen earlier. The cumulative verdict is what it gets.
        let store = TempStore::new("crawl-flush-cumulative-loss").await;
        let ctx = TestContext::new(&store.storage, "crawl").build();
        let tiers = pumper_core::tiers::TierMemory::new(store.storage.pool(), 0);
        let tallies = Arc::new(Mutex::new(HashMap::new()));
        let mut walled = HostTally::default();
        walled.record(&resp(429, &[]));
        tallies
            .lock()
            .unwrap()
            .insert("walled.example".to_string(), walled);
        let mut telemetry = TelemetryProgress::default();

        let clean_tail = {
            let tallies = tallies.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                tallies
                    .lock()
                    .unwrap()
                    .entry("walled.example".to_string())
                    .or_insert_with(HostTally::default)
                    .record(&resp(200, &[]));
                Ok(pumper_core::CrawlStats::default())
            }
        };
        crawl_flushing_telemetry(&ctx, clean_tail, &tallies, &mut telemetry, FAST)
            .await
            .unwrap();

        let profile = tiers
            .get("walled.example")
            .await
            .unwrap()
            .expect("the host is known");
        assert!(
            profile.http_strikes > 0,
            "the bot-wall this run hit must survive its clean tail: {profile:?}"
        );
    }

    #[tokio::test]
    async fn an_unchanged_edge_upsert_is_not_counted_as_a_write() {
        // THE REFUTED BEHAVIOR: `Ok(_) => edges_written += edge_rows.len()`.
        // The store's own summary was thrown away, so an edge it found already
        // present and identical — a no-op — was reported as a write.
        let store = TempStore::new("crawl-edges-noop").await;
        let mut first = sink_over(&store, b"<html>one</html>", "page-a.html").await;
        first
            .emit(vec![page("https://example.com/a", 7, "page-a.html")])
            .await;
        assert_eq!(first.counts.edges_written.load(Ordering::Relaxed), 1);
        assert_eq!(first.counts.edges_unchanged.load(Ordering::Relaxed), 0);

        // The same job re-offering the identical edge (a fresh within-run dedup
        // set, so the row really is handed to the store again): nothing is
        // written, and the result must not claim otherwise.
        let mut again = sink_over(&store, b"<html>one</html>", "page-a.html").await;
        again
            .emit(vec![page("https://example.com/a", 7, "page-a.html")])
            .await;
        assert_eq!(
            again.counts.edges_written.load(Ordering::Relaxed),
            0,
            "a no-op upsert is not a write"
        );
        assert_eq!(
            again.counts.edges_unchanged.load(Ordering::Relaxed),
            1,
            "...but it stays visible as an unchanged row"
        );
    }

    #[tokio::test]
    async fn archived_version_carries_source_url_and_the_body_sha() {
        let store = TempStore::new("crawl-prov-versions").await;
        let datasets = store.datasets();
        let mut sink = sink_over(&store, b"<html>one</html>", "page-a.html").await;
        sink.emit(vec![page("https://example.com/a", 7, "page-a.html")])
            .await;

        // Second sighting with a different fingerprint = a CHANGED page, which
        // is what the version archive records.
        let changed_body = b"<html>two</html>";
        tokio::fs::write(sink.artifacts_dir.join("page-a.html"), changed_body)
            .await
            .unwrap();
        sink.emit(vec![page("https://example.com/a", 9, "page-a.html")])
            .await;

        let versions = datasets.list("crawl", VERSIONS_DATASET, 10).await.unwrap();
        assert_eq!(versions.len(), 1, "one archived revision");
        let key = versions[0].key.clone();
        assert!(key.starts_with("https://example.com/a#"));
        let expected_sha = body_sha(changed_body);
        assert_eq!(versions[0].data["artifact_sha"], json!(expected_sha));

        let rev = datasets
            .history("crawl", VERSIONS_DATASET, &key, 1)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(rev.provenance.job_id.as_deref(), Some(JOB));
        // Per-record facts the archive genuinely knows: the page URL (NOT the
        // `#revision`-suffixed key) and the sha of the body on disk.
        assert_eq!(
            rev.provenance.source_url.as_deref(),
            Some("https://example.com/a")
        );
        assert_eq!(rev.provenance.artifact_sha.as_deref(), Some(&*expected_sha));
        // No RuleSet extracted it, so the stamp must NOT claim replayability.
        assert!(rev.provenance.rules_hash.is_none());
        assert!(!rev.provenance.replayable());

        // The archived copy is byte-identical to what was hashed.
        let archived = versions[0].data["artifact_path"].as_str().unwrap();
        let bytes = tokio::fs::read(sink.artifacts_dir.join(archived))
            .await
            .unwrap();
        assert_eq!(body_sha(&bytes), expected_sha);
    }
}
