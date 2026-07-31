//! Dataset peering (M30 v1, puller only): replicate another pumper node's
//! datasets over its existing revision-feed HTTP API — no new server surface,
//! no push, no distributed system.
//!
//! ## The upstream contract this crate pins (verified against
//! `crates/server/src/routes/datasets.rs`)
//!
//! `GET {peer}/datasets/{app}/{dataset}/changes` with query params:
//! - `since`  — RFC 3339 lower bound, strict `created_at > since`
//! - `cursor` — opaque keyset cursor; its PRESENCE (even empty) switches the
//!   response to `{ "items": [...], "next_cursor": string|null }` paging the
//!   full feed (the legacy no-cursor mode is `{app,dataset,count,changes}`
//!   clamped to 1000). This puller always sends `cursor` so pagination is real.
//! - `limit`  — page size, clamped 1..=1000 server-side
//! - `trust`  — left at the server default (`stable`): a mirror replicates
//!   what the origin stands behind.
//!
//! Each item is a revision: `{app, dataset, key, revision, change:
//! "new"|"changed"|"removed", data: object|null (null for removed), diff,
//! created_at, trust, job_id, source_url, artifact_sha, rules_hash}`, ordered
//! **newest first** (`created_at DESC, rowid DESC`).
//!
//! ## What a run does
//!
//! Params: `{ "url": "http://peer:8877", "datasets": ["hackernews/stories"],
//! "namespace": "peer_hackernews" (optional), "max_records": 500 (optional) }`.
//!
//! For each `remote_app/dataset` it walks the feed from a stored cursor and
//! applies revisions locally under the namespace app (default
//! `peer_{remote_app}`, so a mirror can never clobber a local app's own
//! datasets by accident). Because the feed is newest-first, only the FIRST
//! revision seen per key within one walk is applied (it is the latest state);
//! older revisions of the same key are skipped. `removed` revisions ARE
//! carried by the feed and are applied as real local tombstones (via
//! `Datasets::detect_removed`, which writes `removed_at` + a `removed`
//! revision) — so downstream triggers on the mirror see removals too.
//!
//! ## Provenance of a mirrored record (M12)
//!
//! A mirror must not claim it scraped the origin. Each applied revision is
//! stamped with the LOCAL pulling job, the ORIGIN's own `source_url` and
//! `rules_hash` carried through verbatim (unknown stays unknown), and NO
//! `artifact_sha` — this node holds no archived body, so mirroring that field
//! would falsely mark the record replayable. See [`mirror_provenance`].
//!
//! ## Cursor state (`peer/state` dataset)
//!
//! One record per (peer URL, remote dataset, namespace), key
//! `{url}|{app}/{dataset}|{namespace}`, value:
//! `{ since, walk: {next_cursor, newest, seen[]}|null, etag, etag_since, ... }`.
//! - `since` advances to the newest `created_at` observed **only when a walk
//!   completes** (feed exhausted). A capped run persists `walk` instead and the
//!   next run resumes the same frozen walk mid-flight — `max_records` is a
//!   per-run budget, never a data-loss mechanism.
//! - `seen` (the applied-key set) persists across a resumed walk so an older
//!   revision fetched by a later run can't overwrite the newer state a
//!   previous run already applied. It is capped ([`SEEN_CAP`]); a walk too
//!   large to resume safely is abandoned WITHOUT advancing `since` and the
//!   result says so.
//! - `etag`: the feed's `ETag` response header (when the origin sends one) is
//!   stored and replayed as `If-None-Match` on the next fresh walk with the
//!   same `since`; a `304` ends that dataset's pull at zero transfer.
//!   Response compression needs nothing here — the HTTP engine already sends
//!   `Accept-Encoding: gzip` and decompresses transparently.
//!
//! ## Deliberate non-goals (v1)
//! Push, bidirectional sync, auth beyond what the peer URL embeds (a non-local
//! peer should sit behind the API-key story, prerequisite per the design), and
//! server-side `[[peer]]` scheduling — runs are on-demand jobs; a `[[peer]]`
//! config block that enqueues them on a cron is the documented next slice.

use std::collections::HashSet;

use async_trait::async_trait;
use pumper_core::datasets::Provenance;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Result, ScrapeApp,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Dataset (under app `peer`) holding one cursor-state record per peered feed.
const STATE_DATASET: &str = "state";
/// Default per-dataset revision budget per run.
const DEFAULT_MAX_RECORDS: u64 = 500;
/// Hard clamp on `max_records`.
const MAX_RECORDS_CAP: u64 = 5_000;
/// Max datasets one run may pull.
const MAX_DATASETS: usize = 20;
/// Largest applied-key set a suspended walk may persist. Past this the walk is
/// abandoned (since NOT advanced) rather than risking an older revision
/// overwriting newer applied state on resume.
const SEEN_CAP: usize = 20_000;

pub struct Peer;

#[async_trait]
impl ScrapeApp for Peer {
    fn name(&self) -> &'static str {
        "peer"
    }

    fn description(&self) -> &'static str {
        "Pulls another pumper node's dataset revision feeds and mirrors them \
         locally under a peer namespace (default peer_{app}), with a durable \
         resume cursor, per-run caps, ETag revalidation and real tombstones. \
         Params: {\"url\": \"http://peer:8877\", \"datasets\": \
         [\"hackernews/stories\"], \"namespace\": \"peer_hackernews\", \
         \"max_records\": 500}"
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["url", "datasets"],
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Base URL of the peer pumper node (http/https)."
                    },
                    "datasets": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^[^/]+/[^/]+$" },
                        "minItems": 1,
                        "maxItems": MAX_DATASETS,
                        "description": "Remote datasets as \"app/dataset\"."
                    },
                    "namespace": {
                        "type": "string",
                        "pattern": "^[A-Za-z0-9_-]{1,64}$",
                        "description": "Local app namespace mirrored records are written under. \
                                        Default: peer_{remote app}."
                    },
                    "max_records": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_RECORDS_CAP,
                        "description": "Per-dataset revision budget for this run (default 500). \
                                        A capped walk suspends and resumes next run."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Mirror a peer's hackernews stories into peer_hackernews",
                    params: json!({
                        "url": "http://localhost:8877",
                        "datasets": ["hackernews/stories"]
                    }),
                },
                ManifestExample {
                    description: "Mirror two grants feeds into an explicit namespace, capped",
                    params: json!({
                        "url": "http://edge-node:8877",
                        "datasets": ["grants-gov/opportunities", "eu-sedia/topics"],
                        "namespace": "edge_grants",
                        "max_records": 1000
                    }),
                },
            ],
            output_shape: Some(
                "{ peer, datasets: [{dataset, namespace, status: ok|not_modified|error, pulled, \
                 new, changed, unchanged, origin_provenance_kept, \
                 origin_artifact_sha_dropped, tombstones_applied, capped, walk_resumed, \
                 walk_completed, since, note?, error?}], tombstones: string }",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let base = normalize_base_url(ctx.require_str("url")?)?;
        let specs: Vec<String> = ctx
            .params
            .get("datasets")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if specs.is_empty() {
            return Err(Error::App(
                "missing required param 'datasets' (array of \"app/dataset\")".into(),
            ));
        }
        if specs.len() > MAX_DATASETS {
            return Err(Error::App(format!(
                "{} datasets requested; a run pulls at most {MAX_DATASETS}",
                specs.len()
            )));
        }
        let max_records = ctx
            .params
            .get("max_records")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_MAX_RECORDS)
            .clamp(1, MAX_RECORDS_CAP);
        let namespace_override = ctx
            .params
            .get("namespace")
            .and_then(Value::as_str)
            .map(str::to_string);

        let mut reports: Vec<Value> = Vec::new();
        for spec in &specs {
            let report = match pull_one(&ctx, &base, spec, namespace_override.as_deref(), max_records)
                .await
            {
                Ok(r) => r,
                Err(e) => json!({
                    "dataset": spec,
                    "status": "error",
                    "error": e.to_string(),
                }),
            };
            reports.push(report);
        }

        Ok(json!({
            "peer": base,
            "max_records": max_records,
            "datasets": reports,
            // Honesty note pinned in the result: the upstream feed DOES carry
            // tombstones ('removed' revisions), and this puller applies them
            // as real local tombstones (removed_at + a 'removed' revision).
            "tombstones": "applied from the feed's 'removed' revisions",
        }))
    }
}

/// Pulls one `remote_app/dataset` feed and applies it. Returns the per-dataset
/// report object.
async fn pull_one(
    ctx: &AppContext,
    base: &str,
    spec: &str,
    namespace_override: Option<&str>,
    max_records: u64,
) -> Result<Value> {
    let (remote_app, dataset) = parse_dataset_spec(spec)?;
    let namespace = resolve_namespace(namespace_override, &remote_app)?;

    // Cursor state: read raw (no upsert churn on the read path).
    let state_key = state_key(base, &remote_app, &dataset, &namespace);
    let stored = ctx
        .datasets
        .get(&ctx.app, STATE_DATASET, &state_key)
        .await?
        .map(|r| r.data);
    let mut st = PeerState::load(stored.as_ref());

    let feed_url = format!("{base}/datasets/{remote_app}/{dataset}/changes");

    let resumed = st.walk.is_some();
    let mut walk = st.walk.take().unwrap_or_default();
    let mut seen: HashSet<String> = walk.seen.iter().cloned().collect();
    let mut cursor: String = walk.next_cursor.clone();
    let mut newest: Option<String> = walk.newest.clone();

    let mut pulled: u64 = 0;
    let mut applied_new = 0usize;
    let mut applied_changed = 0usize;
    let mut applied_unchanged = 0usize;
    let mut skipped_dupe = 0usize;
    let mut skipped_malformed = 0usize;
    let mut origin_provenance_kept = 0usize;
    let mut origin_artifact_sha_dropped = 0usize;
    let mut tombstone_keys: Vec<String> = Vec::new();
    let mut completed = false;
    let mut not_modified = false;
    let mut first_page = true;
    let mut notes: Vec<String> = Vec::new();

    loop {
        let page_limit = (max_records - pulled).min(1000);
        if page_limit == 0 {
            break; // capped mid-walk; cursor points at the resume position
        }
        let url = build_changes_url(&feed_url, st.since.as_deref(), &cursor, page_limit);
        let mut req = HttpRequest::get(&url);
        // The feed is a live surface: the TTL response cache must not serve a
        // stale page of a cursor walk.
        req.no_cache = true;
        // ETag revalidation — only meaningful on the first page of a FRESH
        // walk whose `since` matches the one the stored ETag was captured
        // under (the URL is otherwise different and the validator is void).
        if first_page && !resumed && st.etag.is_some() && st.etag_since == st.since {
            req.etag = st.etag.clone();
        }
        let resp = ctx.engines.http.fetch(req).await?;
        if resp.status == 304 {
            not_modified = true;
            completed = true;
            break;
        }
        if !resp.is_success() {
            return Err(Error::App(format!(
                "peer feed {url} returned status {}",
                resp.status
            )));
        }
        let body: Value = serde_json::from_str(&resp.body)
            .map_err(|e| Error::App(format!("peer feed {url}: response is not JSON: {e}")))?;
        let page = parse_feed_page(&body)?;

        if first_page {
            // Origin ETag (if any) is only trustworthy for this exact since.
            let etag = resp
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
                .map(|(_, v)| v.clone());
            if !resumed {
                st.etag = etag;
                st.etag_since = st.since.clone();
            }
            first_page = false;
        }
        if newest.is_none() {
            newest = page
                .items
                .first()
                .and_then(|i| i.get("created_at"))
                .and_then(Value::as_str)
                .map(str::to_string);
        }

        pulled += page.items.len() as u64;
        let plan = plan_actions(&page.items, &mut seen);
        skipped_dupe += plan.skipped_dupe;
        skipped_malformed += plan.skipped_malformed;
        for up in &plan.upserts {
            let prov = mirror_provenance(up, &ctx.job_id.to_string());
            if up.source_url.is_some() {
                origin_provenance_kept += 1;
            }
            if up.origin_artifact_sha {
                origin_artifact_sha_dropped += 1;
            }
            let kind = ctx
                .datasets
                .upsert_stamped(
                    &namespace,
                    &dataset,
                    &up.key,
                    &up.data,
                    Some(up.trust.as_str()),
                    Some(&prov),
                )
                .await?;
            match kind {
                pumper_core::datasets::ChangeKind::New => applied_new += 1,
                pumper_core::datasets::ChangeKind::Changed => applied_changed += 1,
                pumper_core::datasets::ChangeKind::Unchanged => applied_unchanged += 1,
            }
        }
        tombstone_keys.extend(plan.tombstones);

        match page.next_cursor {
            Some(next) if !page.items.is_empty() => cursor = next,
            _ => {
                completed = true;
                break;
            }
        }
        if pulled >= max_records {
            break;
        }
    }

    // Tombstones: `detect_removed` is the one public seam that writes a real
    // tombstone (removed_at + 'removed' revision), driven by a full present
    // set — so hand it "every live key except the tombstoned ones".
    let mut tombstones_applied = 0usize;
    if !tombstone_keys.is_empty() {
        let count = ctx.datasets.record_count(&namespace, &dataset).await?;
        let live: Vec<String> = ctx
            .datasets
            .list(&namespace, &dataset, count.max(1))
            .await?
            .into_iter()
            .filter(|r| r.removed_at.is_none())
            .map(|r| r.key)
            .collect();
        let dead: HashSet<&str> = tombstone_keys.iter().map(String::as_str).collect();
        let present: Vec<String> = live.into_iter().filter(|k| !dead.contains(k.as_str())).collect();
        if present.is_empty() {
            // detect_removed refuses an empty present set by design; honor it.
            notes.push(format!(
                "{} tombstone(s) NOT applied: they would empty the entire local \
                 mirror, which the store refuses (delete explicitly if intended)",
                tombstone_keys.len()
            ));
        } else {
            tombstones_applied = ctx
                .datasets
                .detect_removed(&namespace, &dataset, &present)
                .await?
                .len();
        }
    }

    // Advance / persist cursor state.
    if completed {
        if let Some(n) = newest {
            st.since = Some(n);
        }
        st.walk = None;
    } else if seen.len() <= SEEN_CAP {
        walk.next_cursor = cursor;
        walk.newest = newest;
        walk.seen = seen.into_iter().collect();
        st.walk = Some(walk);
    } else {
        st.walk = None; // since NOT advanced — next run restarts the walk
        notes.push(format!(
            "walk abandoned: applied-key set exceeded {SEEN_CAP}; cursor not \
             advanced — raise max_records so a walk can complete"
        ));
    }
    st.peer_url = base.to_string();
    st.remote_app = remote_app.clone();
    st.dataset = dataset.clone();
    st.namespace = namespace.clone();
    st.last_job_id = Some(ctx.job_id.to_string());
    ctx.upsert(STATE_DATASET, &state_key, &st.to_value()?).await?;

    Ok(json!({
        "dataset": spec,
        "namespace": namespace,
        "status": if not_modified { "not_modified" } else { "ok" },
        "pulled": pulled,
        "new": applied_new,
        "changed": applied_changed,
        "unchanged": applied_unchanged,
        "skipped_older_revisions": skipped_dupe,
        "skipped_malformed": skipped_malformed,
        // Provenance honesty (M12), visible per run rather than implied: how
        // many mirrored records preserved the ORIGIN's source_url, and how many
        // carried an origin `artifact_sha` this node deliberately did not
        // mirror (it holds no such artifact — see `mirror_provenance`).
        "origin_provenance_kept": origin_provenance_kept,
        "origin_artifact_sha_dropped": origin_artifact_sha_dropped,
        "tombstones_applied": tombstones_applied,
        "capped": !completed,
        "walk_resumed": resumed,
        "walk_completed": completed,
        "since": st.since,
        "note": if notes.is_empty() { Value::Null } else { Value::String(notes.join("; ")) },
    }))
}

// ── Cursor state ────────────────────────────────────────────────────────────

/// Suspended mid-walk position: resume cursor, the walk's candidate `since`,
/// and the keys already applied in this walk (newest-revision-wins guard).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WalkState {
    #[serde(default)]
    next_cursor: String,
    #[serde(default)]
    newest: Option<String>,
    #[serde(default)]
    seen: Vec<String>,
}

/// The `peer/state` record for one peered feed. Tolerant on load: any
/// unexpected shape starts fresh (a corrupt cursor must never fail the job).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct PeerState {
    #[serde(default)]
    peer_url: String,
    #[serde(default)]
    remote_app: String,
    #[serde(default)]
    dataset: String,
    #[serde(default)]
    namespace: String,
    /// RFC 3339 `created_at` high-water mark of the last COMPLETED walk,
    /// passed verbatim as `?since=`.
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    walk: Option<WalkState>,
    /// Origin `ETag` of the fresh-walk first page, replayed as If-None-Match.
    #[serde(default)]
    etag: Option<String>,
    /// The `since` the stored ETag was captured under — it validates only that.
    #[serde(default)]
    etag_since: Option<String>,
    #[serde(default)]
    last_job_id: Option<String>,
}

impl PeerState {
    fn load(stored: Option<&Value>) -> Self {
        stored
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    fn to_value(&self) -> Result<Value> {
        Ok(serde_json::to_value(self)?)
    }
}

fn state_key(base: &str, remote_app: &str, dataset: &str, namespace: &str) -> String {
    format!("{base}|{remote_app}/{dataset}|{namespace}")
}

// ── Pure planning / parsing (unit-tested) ───────────────────────────────────

/// One record write planned from a feed revision.
#[derive(Debug, Clone)]
struct PlannedUpsert {
    key: String,
    data: Value,
    trust: String,
    /// The ORIGIN's `source_url` as carried by the feed revision — where the
    /// content actually came from, which is not this peer's feed URL. Carried
    /// through so the mirror preserves the origin's derivation rather than
    /// overwriting it (see [`mirror_provenance`]).
    source_url: Option<String>,
    /// The origin's `rules_hash`, carried through for the same reason.
    rules_hash: Option<String>,
    /// Whether the origin revision carried an `artifact_sha`. The sha is
    /// deliberately NOT mirrored (see [`mirror_provenance`]); this only feeds
    /// the run report so the drop is visible rather than silent.
    origin_artifact_sha: bool,
}

#[derive(Debug, Default)]
struct PullPlan {
    upserts: Vec<PlannedUpsert>,
    tombstones: Vec<String>,
    /// Older revisions of keys already handled this walk (feed is newest-first).
    skipped_dupe: usize,
    /// Items missing key/change, or new/changed items with no data snapshot.
    skipped_malformed: usize,
}

/// Turns one newest-first page of revisions into writes. First revision seen
/// per key (across the whole walk — `seen` persists) wins; `removed` becomes a
/// tombstone; new/changed carry their full `data` snapshot.
fn plan_actions(items: &[Value], seen: &mut HashSet<String>) -> PullPlan {
    let mut plan = PullPlan::default();
    for item in items {
        let (Some(key), Some(change)) = (
            item.get("key").and_then(Value::as_str),
            item.get("change").and_then(Value::as_str),
        ) else {
            plan.skipped_malformed += 1;
            continue;
        };
        if seen.contains(key) {
            plan.skipped_dupe += 1;
            continue;
        }
        match change {
            "removed" => {
                seen.insert(key.to_string());
                plan.tombstones.push(key.to_string());
            }
            "new" | "changed" => {
                let Some(data) = item.get("data").filter(|d| !d.is_null()) else {
                    plan.skipped_malformed += 1;
                    continue;
                };
                seen.insert(key.to_string());
                let str_field = |name: &str| {
                    item.get(name)
                        .and_then(Value::as_str)
                        .filter(|s| !s.trim().is_empty())
                        .map(str::to_string)
                };
                plan.upserts.push(PlannedUpsert {
                    key: key.to_string(),
                    data: data.clone(),
                    trust: item
                        .get("trust")
                        .and_then(Value::as_str)
                        .unwrap_or("stable")
                        .to_string(),
                    source_url: str_field("source_url"),
                    rules_hash: str_field("rules_hash"),
                    origin_artifact_sha: str_field("artifact_sha").is_some(),
                });
            }
            _ => plan.skipped_malformed += 1,
        }
    }
    plan
}

/// The derivation stamp a MIRRORED record gets (M12).
///
/// The honesty problem peering creates: this node did not fetch the origin
/// page, it copied someone else's record. Three deliberate choices:
///
/// - `job_id` — always the LOCAL pulling job. `AppContext` enforces this for
///   its own writes for the same reason: the producing job here is this pull,
///   and the remote's job id is meaningless against this node's `jobs` table.
/// - `source_url` — the ORIGIN's own `source_url`, carried through verbatim
///   when the feed supplied one. That is where the content genuinely came
///   from. Stamping the peer's *feed* URL instead (what v1 did) overwrote the
///   real provenance with a transport detail and made every mirrored record
///   look like it was scraped from the peer node. When the origin knew no
///   source URL, the mirror stays `None` = unknown rather than inventing the
///   feed URL; the pull job's params + the `peer/state` record are where the
///   transport path is recorded.
/// - `artifact_sha` — deliberately DROPPED. It means "sha256 of the archived
///   body **on disk**", and this node holds no such artifact. Mirroring it
///   would make [`Provenance::replayable`] answer true for a record this node
///   provably cannot re-derive. `rules_hash` is kept (a content-addressed
///   ruleset identity is still true off-node, and alone it cannot claim
///   replayability).
fn mirror_provenance(up: &PlannedUpsert, local_job_id: &str) -> Provenance {
    Provenance {
        job_id: Some(local_job_id.to_string()),
        source_url: up.source_url.clone(),
        artifact_sha: None,
        rules_hash: up.rules_hash.clone(),
    }
}

/// One page of the cursor-mode feed: `{items, next_cursor}`.
#[derive(Debug)]
struct FeedPage {
    items: Vec<Value>,
    next_cursor: Option<String>,
}

/// Parses a cursor-mode response. A legacy `{changes: [...]}` body means the
/// peer ignored our `cursor` param — an incompatible/ancient node — and is a
/// typed error rather than a silent unpaginated pull.
fn parse_feed_page(body: &Value) -> Result<FeedPage> {
    if let Some(items) = body.get("items").and_then(Value::as_array) {
        return Ok(FeedPage {
            items: items.clone(),
            next_cursor: body
                .get("next_cursor")
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    if body.get("changes").is_some() {
        return Err(Error::App(
            "peer answered in legacy no-cursor mode ({changes:[..]}); it ignored the \
             'cursor' param — peer node too old to page safely"
                .into(),
        ));
    }
    Err(Error::App(
        "peer feed response has neither 'items' nor 'changes'".into(),
    ))
}

/// `"app/dataset"` → (app, dataset). Exactly one slash, both halves non-empty.
fn parse_dataset_spec(spec: &str) -> Result<(String, String)> {
    match spec.split_once('/') {
        Some((app, ds))
            if !app.trim().is_empty() && !ds.trim().is_empty() && !ds.contains('/') =>
        {
            Ok((app.trim().to_string(), ds.trim().to_string()))
        }
        _ => Err(Error::App(format!(
            "invalid dataset spec {spec:?} — expected \"app/dataset\""
        ))),
    }
}

/// The namespace mirrored records are written under. Default `peer_{app}`.
/// A custom namespace must be a plain identifier, and may not equal the remote
/// app name — that would write remote records straight into the shape a local
/// app of the same name owns (the write-origin corruption the design warns
/// about).
fn resolve_namespace(explicit: Option<&str>, remote_app: &str) -> Result<String> {
    let ns = match explicit {
        Some(ns) => ns.to_string(),
        None => format!("peer_{remote_app}"),
    };
    if ns.is_empty()
        || ns.len() > 64
        || !ns
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(Error::App(format!(
            "invalid namespace {ns:?} — use 1-64 chars of [A-Za-z0-9_-]"
        )));
    }
    if ns == remote_app {
        return Err(Error::App(format!(
            "namespace {ns:?} equals the remote app name — a mirror must not \
             write into a namespace a local app may own (default is peer_{remote_app})"
        )));
    }
    Ok(ns)
}

/// Validates and normalizes the peer base URL (scheme required, no trailing /).
fn normalize_base_url(url: &str) -> Result<String> {
    let trimmed = url.trim().trim_end_matches('/');
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(Error::App(format!(
            "peer url {url:?} must start with http:// or https://"
        )));
    }
    Ok(trimmed.to_string())
}

/// Builds one page request. `cursor` is always present (even empty) — that is
/// what selects the paginated `{items, next_cursor}` mode upstream.
fn build_changes_url(feed_url: &str, since: Option<&str>, cursor: &str, limit: u64) -> String {
    let mut url = format!("{feed_url}?cursor={}&limit={limit}", urlencode(cursor));
    if let Some(since) = since {
        url.push_str("&since=");
        url.push_str(&urlencode(since));
    }
    url
}

/// Minimal percent-encoding for a query VALUE (RFC 3986 unreserved pass-through).
/// Cursors (`<rfc3339>|<rowid>`) and RFC 3339 stamps (`+`, `:`) both need it.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rev(key: &str, change: &str, data: Value, created_at: &str) -> Value {
        json!({
            "app": "hackernews", "dataset": "stories", "key": key,
            "revision": 3, "change": change,
            "data": if change == "removed" { Value::Null } else { data },
            "diff": null, "created_at": created_at, "trust": "stable",
            "job_id": "j", "source_url": null, "artifact_sha": null, "rules_hash": null,
        })
    }

    /// A feed revision carrying the origin's full derivation stamp.
    fn rev_with_provenance(key: &str) -> Value {
        json!({
            "app": "hackernews", "dataset": "stories", "key": key,
            "revision": 2, "change": "changed", "data": {"v": 1}, "diff": null,
            "created_at": "t2", "trust": "stable",
            "job_id": "remote-job-uuid",
            "source_url": "https://origin.example/item?id=1",
            "artifact_sha": "deadbeef",
            "rules_hash": "cafebabe",
        })
    }

    #[test]
    fn mirror_keeps_the_origin_source_url_and_never_the_peer_feed_url() {
        let mut seen = HashSet::new();
        let plan = plan_actions(&[rev_with_provenance("k1")], &mut seen);
        let prov = mirror_provenance(&plan.upserts[0], "local-job");
        // The producing job is THIS pull; the remote's job id is meaningless here.
        assert_eq!(prov.job_id.as_deref(), Some("local-job"));
        // The content came from the origin page — not from the peer's feed.
        assert_eq!(
            prov.source_url.as_deref(),
            Some("https://origin.example/item?id=1")
        );
        assert_eq!(prov.rules_hash.as_deref(), Some("cafebabe"));
        // This node holds no archived body: mirroring the sha would falsely
        // mark a record replayable that cannot be re-derived here.
        assert!(prov.artifact_sha.is_none());
        assert!(!prov.replayable());
        assert!(plan.upserts[0].origin_artifact_sha, "the drop is reported");
    }

    #[test]
    fn unknown_origin_provenance_stays_unknown() {
        // The origin knew no source URL. Substituting the feed URL would be a
        // fabrication — honest-Null instead.
        let mut seen = HashSet::new();
        let plan = plan_actions(&[rev("k1", "new", json!({"v": 1}), "t1")], &mut seen);
        let prov = mirror_provenance(&plan.upserts[0], "local-job");
        assert_eq!(prov.job_id.as_deref(), Some("local-job"));
        assert!(prov.source_url.is_none());
        assert!(prov.rules_hash.is_none());
        assert!(prov.artifact_sha.is_none());
        assert!(!plan.upserts[0].origin_artifact_sha);
    }

    #[test]
    fn dataset_spec_parses_app_slash_dataset_and_rejects_everything_else() {
        assert_eq!(
            parse_dataset_spec("hackernews/stories").unwrap(),
            ("hackernews".into(), "stories".into())
        );
        for bad in ["", "stories", "/stories", "app/", "a/b/c"] {
            assert!(parse_dataset_spec(bad).is_err(), "must reject {bad:?}");
        }
    }

    #[test]
    fn namespace_defaults_to_peer_prefixed_remote_app() {
        assert_eq!(resolve_namespace(None, "hackernews").unwrap(), "peer_hackernews");
        assert_eq!(resolve_namespace(Some("mirror-a"), "x").unwrap(), "mirror-a");
    }

    #[test]
    fn namespace_may_not_shadow_the_remote_app_or_carry_path_chars() {
        // Writing a mirror into the exact namespace a local app owns is the
        // write-origin corruption the design forbids.
        assert!(resolve_namespace(Some("hackernews"), "hackernews").is_err());
        for bad in ["", "a/b", "a b", "a\\b", "peer|x", &"x".repeat(65)] {
            assert!(resolve_namespace(Some(bad), "app").is_err(), "must reject {bad:?}");
        }
    }

    #[test]
    fn changes_url_always_selects_cursor_mode_and_encodes_the_cursor() {
        // `cursor` presence (even empty) is what flips the upstream route into
        // `{items, next_cursor}` paging — the contract this puller relies on.
        let fresh = build_changes_url("http://p/datasets/a/b/changes", None, "", 1000);
        assert_eq!(fresh, "http://p/datasets/a/b/changes?cursor=&limit=1000");

        let url = build_changes_url(
            "http://p/datasets/a/b/changes",
            Some("2026-07-31T10:00:00+02:00"),
            "2026-07-31T09:00:00Z|42",
            250,
        );
        assert!(url.contains("cursor=2026-07-31T09%3A00%3A00Z%7C42"));
        assert!(url.contains("since=2026-07-31T10%3A00%3A00%2B02%3A00"));
        assert!(url.contains("limit=250"));
    }

    #[test]
    fn feed_page_parses_cursor_mode_and_refuses_legacy_mode() {
        let page = parse_feed_page(&json!({
            "items": [rev("k1", "new", json!({"a": 1}), "t1")],
            "next_cursor": "t1|9",
        }))
        .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("t1|9"));

        let done = parse_feed_page(&json!({"items": [], "next_cursor": null})).unwrap();
        assert!(done.items.is_empty() && done.next_cursor.is_none());

        // A `{changes:[..]}` body means the peer ignored `cursor` — an old
        // node we cannot page against; silent unpaginated pulls are refused.
        assert!(parse_feed_page(&json!({"changes": [], "count": 0})).is_err());
        assert!(parse_feed_page(&json!({"weird": true})).is_err());
    }

    #[test]
    fn newest_revision_per_key_wins_and_older_ones_are_skipped() {
        // Feed is newest-first: k1 changed at t2 must be applied, its older
        // t1 revision skipped — applying in feed order without the guard
        // would end the walk with STALE data.
        let items = vec![
            rev("k1", "changed", json!({"v": 2}), "t2"),
            rev("k2", "new", json!({"v": 1}), "t2"),
            rev("k1", "new", json!({"v": 1}), "t1"),
        ];
        let mut seen = HashSet::new();
        let plan = plan_actions(&items, &mut seen);
        assert_eq!(plan.upserts.len(), 2);
        assert_eq!(plan.upserts[0].key, "k1");
        assert_eq!(plan.upserts[0].data, json!({"v": 2}));
        assert_eq!(plan.skipped_dupe, 1);
        assert!(plan.tombstones.is_empty());
    }

    #[test]
    fn removal_newer_than_the_last_write_tombstones_but_an_older_one_does_not() {
        // k1's latest state is 'removed' → tombstone. k2 was re-created AFTER
        // its removal (newest-first: 'new' appears before 'removed') → the
        // stale tombstone must NOT undo the live record.
        let items = vec![
            rev("k1", "removed", Value::Null, "t3"),
            rev("k2", "new", json!({"v": 9}), "t3"),
            rev("k1", "changed", json!({"v": 1}), "t2"),
            rev("k2", "removed", Value::Null, "t1"),
        ];
        let mut seen = HashSet::new();
        let plan = plan_actions(&items, &mut seen);
        assert_eq!(plan.tombstones, vec!["k1".to_string()]);
        assert_eq!(plan.upserts.len(), 1);
        assert_eq!(plan.upserts[0].key, "k2");
        assert_eq!(plan.skipped_dupe, 2);
    }

    #[test]
    fn seen_set_persists_across_pages_so_a_resumed_walk_cannot_regress() {
        // Page 1 applied k1@t3; a later page (possibly a later RUN of the same
        // suspended walk) carries k1@t1 — it must be skipped.
        let mut seen = HashSet::new();
        plan_actions(&[rev("k1", "changed", json!({"v": 3}), "t3")], &mut seen);
        let plan = plan_actions(&[rev("k1", "new", json!({"v": 1}), "t1")], &mut seen);
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.skipped_dupe, 1);
    }

    #[test]
    fn malformed_items_are_counted_never_applied_or_fatal() {
        let items = vec![
            json!({"change": "new"}),                       // no key
            json!({"key": "k", "change": "mystery"}),       // unknown change kind
            rev("k2", "new", Value::Null, "t1"),            // new without data
            json!({"key": "k3"}),                           // no change
        ];
        let mut seen = HashSet::new();
        let plan = plan_actions(&items, &mut seen);
        assert!(plan.upserts.is_empty() && plan.tombstones.is_empty());
        assert_eq!(plan.skipped_malformed, 4);
    }

    #[test]
    fn state_load_is_tolerant_and_roundtrips() {
        // Corrupt/foreign state must start fresh, never fail the job.
        assert!(PeerState::load(Some(&json!("garbage"))).since.is_none());
        assert!(PeerState::load(Some(&json!([1, 2]))).walk.is_none());
        assert!(PeerState::load(None).since.is_none());

        let mut st = PeerState::default();
        st.since = Some("2026-07-31T10:00:00Z".into());
        st.walk = Some(WalkState {
            next_cursor: "t|7".into(),
            newest: Some("t9".into()),
            seen: vec!["k1".into()],
        });
        st.etag = Some("\"abc\"".into());
        st.etag_since = st.since.clone();
        let back = PeerState::load(Some(&st.to_value().unwrap()));
        assert_eq!(back.since.as_deref(), Some("2026-07-31T10:00:00Z"));
        let walk = back.walk.unwrap();
        assert_eq!(walk.next_cursor, "t|7");
        assert_eq!(walk.seen, vec!["k1".to_string()]);
        assert_eq!(back.etag.as_deref(), Some("\"abc\""));
    }

    #[test]
    fn base_url_normalizes_trailing_slash_and_requires_a_scheme() {
        assert_eq!(
            normalize_base_url("http://peer:8877/").unwrap(),
            "http://peer:8877"
        );
        assert!(normalize_base_url("peer:8877").is_err());
        assert!(normalize_base_url("ftp://x").is_err());
    }

    #[test]
    fn state_key_is_scoped_by_peer_dataset_and_namespace() {
        // Two namespaces mirroring the same remote feed must keep independent
        // cursors — a shared key would make one mirror skip the other's data.
        let a = state_key("http://p", "app", "ds", "peer_app");
        let b = state_key("http://p", "app", "ds", "other_ns");
        assert_ne!(a, b);
        assert_eq!(a, "http://p|app/ds|peer_app");
    }
}
