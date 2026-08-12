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
}

impl HostTally {
    /// The signal the tier router learns from: any bot-wall or transport loss.
    fn http_lost(&self) -> bool {
        self.botwall > 0 || self.transport_errors > 0
    }

    /// Folds one fetch outcome into the tally (pure classification, kept out of
    /// the client so it is unit-testable).
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
}

#[async_trait]
impl HttpClient for MeteringHttpClient {
    async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
        let host = host_of(&req.url);
        let result = self.inner.fetch(req).await;
        if let Some(host) = host {
            // std Mutex, no `.await` held across the guard.
            let mut tallies = self.tallies.lock().unwrap_or_else(|e| e.into_inner());
            tallies.entry(host).or_default().record(&result);
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
        });

        let stats = crawl(
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
        )
        .await?;

        // Flush the tally: one cost event + one tier-router signal per host. HTTP
        // fetches cost $0 (only the Claude tier spends), so this feeds call-count /
        // ROI accounting and, crucially, teaches the router which hosts bot-wall
        // the HTTP tier — the crawl's richest signal, previously discarded.
        let tallies = std::mem::take(&mut *tallies.lock().unwrap_or_else(|e| e.into_inner()));
        let mut reliability_deltas: Vec<(String, reliability::HostDelta)> =
            Vec::with_capacity(tallies.len());
        for (host, tally) in tallies {
            let url = format!("https://{host}/");
            let detail = format!("crawl: {} http fetches", tally.fetches);
            ctx.meter("http", Some(&url), 0.0, Some(&detail)).await;
            ctx.learn_tier(&host, "http", tally.http_lost()).await;
            // Web Reliability Index (M41): the same per-host outcomes, persisted
            // instead of discarded after the router learns from them.
            reliability_deltas.push((
                host,
                reliability::HostDelta::Crawl(reliability::CrawlHostObs {
                    fetches: tally.fetches as u64,
                    ok: tally.ok as u64,
                    botwall: tally.botwall as u64,
                    transport_errors: tally.transport_errors as u64,
                    not_modified: tally.not_modified as u64,
                    gone: tally.gone as u64,
                    validators_seen: tally.validators_seen as u64,
                }),
            ));
        }
        let reliability_hosts = reliability::record_observations(
            &ctx.datasets,
            &ctx.job_id.to_string(),
            reliability_deltas,
        )
        .await;

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
