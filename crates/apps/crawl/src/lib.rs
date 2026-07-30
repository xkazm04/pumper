//! Broad crawler app: seed a set of URLs and crawl outward with bounded
//! concurrency, depth, and page count — respecting robots.txt and the per-domain
//! governor, dropping near-duplicate pages, and streaming page bodies to the
//! job's artifact directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pumper_core::{
    crawl, AppContext, AppManifest, CostClass, CrawlConfig, CrawlPageRecord, Datasets, Error,
    HttpClient, HttpRequest, HttpResponse, ManifestExample, PageSink, PageSource, Result,
    RevisitCadence, RevisitSeed, ScrapeApp,
};
use serde_json::{json, Value};

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
    /// Link-graph edges successfully upserted into the `edges` dataset (M08).
    /// Counted on write success, like `versions_archived`.
    edges_written: AtomicUsize,
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
    /// Archives the CHANGED keys of one upsert batch into the versioned crawl
    /// archive: copy `page-<hex>.html` → `page-<hex>.r{revision}.html` and upsert
    /// a `page_versions` record keyed `{url}#{revision}` (artifact path, simhash,
    /// fetched_at). New first-sightings and unchanged revisits archive nothing —
    /// the un-suffixed artifact already IS the latest body, so the archive grows
    /// only when a page actually changes. Best-effort like the rest of the sink:
    /// an archive failure warns and skips, never fails the crawl.
    async fn archive_changed(&self, changed: &[String], meta: &HashMap<String, (String, u64)>) {
        let mut versions: Vec<(String, Value)> = Vec::new();
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
            if let Err(e) = tokio::fs::copy(&src, &dst).await {
                tracing::warn!(url = %url, path = %src.display(),
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
                }),
            ));
        }
        if versions.is_empty() {
            return;
        }
        match self
            .datasets
            .upsert_many(&self.app, VERSIONS_DATASET, &versions)
            .await
        {
            Ok(_) => {
                self.counts
                    .versions_archived
                    .fetch_add(versions.len(), Ordering::Relaxed);
            }
            Err(e) => tracing::warn!(job = %self.job_id, "crawl page_versions upsert failed: {e}"),
        }
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
        if !live.is_empty() {
            match self.datasets.upsert_many(&self.app, "pages", &live).await {
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
            if let Err(e) = self.datasets.upsert_many(&self.app, "pages", &gone).await {
                tracing::warn!(job = %self.job_id, "crawl gone-marker upsert failed: {e}");
            }
        }
        if !checks.is_empty() {
            // Deliberately NOT folded into new/changed counts: a cadence bump is
            // estimator bookkeeping, not observed content change.
            match self.datasets.upsert_many(&self.app, "pages", &checks).await {
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
                .upsert_many(&self.app, link_graph::EDGES_DATASET, &edge_rows)
                .await
            {
                Ok(_) => {
                    self.counts
                        .edges_written
                        .fetch_add(edge_rows.len(), Ordering::Relaxed);
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
         the run's most-linked-to pages are echoed as `top_linked`."
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
            output_shape: Some(
                "{pages, new, changed, unchanged, skipped, hosts, versions_archived, \
                 edges_written, edges_dropped_out_degree, top_linked} — crawl tallies plus the \
                 `pages` dataset upsert summary; bodies land in the job's artifact dir, changed \
                 revisions also as revision-suffixed copies recorded in `page_versions`, and the \
                 link graph streams into the `edges` dataset (key `{from_url}|{to_url}`)",
            ),
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
        Ok(json!({
            "crawled": stats.crawled,
            "kept": stats.kept,
            "skipped_duplicates": stats.skipped_duplicates,
            "skipped_robots": stats.skipped_robots,
            "skipped_filtered": stats.skipped_filtered,
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
            "edges_dropped_out_degree": edge_summary.0,
            "edges_deduped": edge_summary.1,
            "top_linked": edge_summary.2,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{host_of, versioned_artifact_name, HostTally};
    use pumper_core::{Error, HttpResponse, Result};
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
}
