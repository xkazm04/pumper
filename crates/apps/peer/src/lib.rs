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
//! `Datasets::tombstone_keys` — removal by NAME, which writes `removed_at` + a
//! `removed` revision) — so downstream triggers on the mirror see removals too.
//! It is deliberately NOT `detect_removed`: this app knows exactly which keys
//! died, so inferring them from a synthetic full snapshot (what v1 did) only
//! bought a way around the degrading-source removal guard.
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
//! `{ since, walk: {next_cursor, newest, seen[]}|null, pending_tombstones[],
//! etag, etag_since, ... }`.
//! - `since` advances to the newest `created_at` observed **only when a walk
//!   completes cleanly** (feed exhausted AND nothing skipped as unreadable —
//!   see [`walk_may_advance`]). A capped run persists `walk` instead and the
//!   next run resumes the same frozen walk mid-flight — `max_records` is a
//!   per-run budget, never a data-loss mechanism.
//! - `since` is stored as the honest observed maximum but sent on the wire
//!   rewound by one microsecond ([`inclusive_since`]), because the origin's
//!   predicate is strict `>` and a whole upsert-chunk shares one stamp. Without
//!   that, revisions committed at the boundary stamp after page 1 was served
//!   were excluded forever.
//! - `pending_tombstones` holds removals a run REFUSED to apply (they would have
//!   emptied the mirror). They are retried every run until they can be applied —
//!   a refusal is "not yet", never "never".
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
use chrono::{DateTime, Duration, Utc};
use pumper_core::datasets::{ts, Provenance};
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
/// Largest deferred-tombstone backlog a state record may carry. A refusal that
/// grows past this is not a transient origin hiccup any more; the overflow is
/// dropped from the backlog and said so in the note, rather than growing one
/// state record without bound.
const PENDING_TOMBSTONE_CAP: usize = 10_000;

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
                "{ peer, max_records, status: ok|partial, datasets: [{dataset, namespace, \
                 status: ok|not_modified|drift|error, pulled, new, changed, unchanged, \
                 skipped_older_revisions, skipped_malformed, origin_provenance_kept, \
                 origin_artifact_sha_dropped, tombstones_applied, tombstones_deferred, capped, \
                 walk_resumed, walk_completed, since, note?, error?}], tombstones: string }. \
                 A run where EVERY dataset errored fails the job instead of returning this.",
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
            let report = match pull_one(
                &ctx,
                &base,
                spec,
                namespace_override.as_deref(),
                max_records,
            )
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

        // Run honesty: a per-dataset failure used to be a `{"status":"error"}`
        // object inside an `Ok` result, so total failure read as a green job.
        let statuses: Vec<&str> = reports
            .iter()
            .map(|r| r.get("status").and_then(Value::as_str).unwrap_or("error"))
            .collect();
        let outcome = run_outcome(&statuses);
        if outcome == RunOutcome::Failed {
            let why: Vec<String> = reports
                .iter()
                .map(|r| {
                    format!(
                        "{}: {}",
                        r.get("dataset").and_then(Value::as_str).unwrap_or("?"),
                        r.get("error").and_then(Value::as_str).unwrap_or("unknown")
                    )
                })
                .collect();
            return Err(Error::App(format!(
                "every requested dataset failed to mirror from {base} — nothing was pulled: {}",
                why.join("; ")
            )));
        }

        Ok(json!({
            "peer": base,
            "max_records": max_records,
            // `ok` only when every dataset came back clean; `partial` the moment
            // one errored or froze on drift. All-errored never reaches here.
            "status": if outcome == RunOutcome::Partial { "partial" } else { "ok" },
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
        // The stored resume point is rewound one microsecond on the wire so the
        // origin's strict `created_at > since` includes revisions that share the
        // boundary stamp — see `inclusive_since`.
        let wire_since = st.since.as_deref().map(inclusive_since);
        let url = build_changes_url(&feed_url, wire_since.as_deref(), &cursor, page_limit);
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

    // Tombstones: the feed NAMED these keys as removed, so `tombstone_keys`
    // writes the same two rows (`removed_at` + a `removed` revision) directly.
    //
    // v1 drove `Datasets::detect_removed` instead, by listing every live record
    // and handing back "all of them except the dead ones" as a synthetic full
    // snapshot. It produced the right rows, but it re-entered full-snapshot
    // *inference* — which this app never needed — and in doing so reached past
    // the degrading-source removal guard that every `sync_many` caller gets.
    //
    // A refused batch is DEFERRED, not dropped: it merges with any backlog a
    // previous run left behind and, if still refused, is written back to
    // `PeerState` so the next run retries it. Before this, the refusal advanced
    // `since` past the removals and they were never revisited.
    let mut tombstones_applied = 0usize;
    let mut tombstones_deferred = 0usize;
    let candidates = merge_deferred_tombstones(&st.pending_tombstones, &tombstone_keys);
    if !candidates.is_empty() {
        let count = ctx.datasets.record_count(&namespace, &dataset).await?;
        let live: Vec<String> = ctx
            .datasets
            .list(&namespace, &dataset, count.max(1))
            .await?
            .into_iter()
            .filter(|r| r.removed_at.is_none())
            .map(|r| r.key)
            .collect();
        if tombstones_would_empty_the_mirror(&live, &candidates) {
            tombstones_deferred = candidates.len();
            notes.push(format!(
                "{tombstones_deferred} tombstone(s) NOT applied: they would empty the \
                 entire local mirror, which this app refuses (delete explicitly if \
                 intended). They are HELD and retried next run — removals are pending, \
                 the mirror is not converged"
            ));
            st.pending_tombstones = candidates;
        } else {
            tombstones_applied = ctx
                .datasets
                .tombstone_keys(&namespace, &dataset, &candidates)
                .await?
                .len();
            st.pending_tombstones.clear();
        }
    }

    // Advance / persist cursor state. `walk_may_advance` is the gate: a walk
    // that reached the end of the feed but skipped items it could not parse must
    // NOT move the resume point past them.
    let drifted = skipped_malformed > 0;
    if walk_may_advance(completed, skipped_malformed) {
        if let Some(n) = newest {
            st.since = Some(n);
        }
        st.walk = None;
    } else if drifted {
        // Abandon the suspended walk too: with `since` frozen, the next run
        // re-reads the whole window from the last good resume point, so a
        // half-finished cursor into a drifted feed is worse than useless.
        st.walk = None;
        notes.push(format!(
            "{skipped_malformed} feed item(s) could not be read (schema drift?); the resume \
             point is FROZEN at {} and this window will be re-read next run — no revision is \
             skipped, but the mirror is not converged until the shape is understood",
            st.since.as_deref().unwrap_or("<beginning of feed>")
        ));
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
    ctx.upsert(STATE_DATASET, &state_key, &st.to_value()?)
        .await?;

    Ok(json!({
        "dataset": spec,
        "namespace": namespace,
        // `drift` outranks `not_modified`/`ok`: a run that could not read part
        // of the feed has not converged, and the resume point says so by not
        // moving. It is not `error` — the revisions it COULD read did land.
        "status": if drifted {
            "drift"
        } else if not_modified {
            "not_modified"
        } else {
            "ok"
        },
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
        // Removals the mirror is holding rather than dropping (see
        // `merge_deferred_tombstones`). Non-zero means "not converged yet".
        "tombstones_deferred": tombstones_deferred,
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
    /// RFC 3339 `created_at` high-water mark of the last COMPLETED walk. Sent
    /// as `?since=` through [`inclusive_since`], NOT verbatim — the stored value
    /// stays the honest observed maximum (it is what the run report shows), and
    /// the one-microsecond rewind that makes the origin's strict `>` inclusive
    /// is applied at request time only.
    #[serde(default)]
    since: Option<String>,
    #[serde(default)]
    walk: Option<WalkState>,
    /// Tombstones a previous run REFUSED (they would have emptied the mirror),
    /// carried forward so the removals are retried instead of dropped. See
    /// [`merge_deferred_tombstones`].
    #[serde(default)]
    pending_tombstones: Vec<String>,
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

/// The `?since=` value to send so the origin's STRICT `created_at > since`
/// predicate behaves **inclusively** for `resume_point`.
///
/// The loss window this closes. The feed orders `(created_at DESC, rowid DESC)`
/// and a whole upsert-chunk shares ONE `created_at` (see
/// `docs/features/datasets.md` § Conventions). A walk that reads page 1 and
/// stores its newest stamp as the resume point therefore excludes — permanently
/// — every revision carrying that same stamp that was committed *after* the page
/// was served: `created_at > newest` can never return them, on this run or any
/// future one. A mirror's one promise is convergence, so a boundary that can
/// silently drop a whole chunk is the wrong boundary.
///
/// Stored stamps are fixed-width RFC 3339 micros ([`pumper_core::datasets::ts`]),
/// so rewinding the resume point by exactly one microsecond turns `> (t - 1µs)`
/// into `>= t`: no representable stamp can fall in the open interval, so this is
/// an exact inclusive boundary and not a fuzzy safety window.
///
/// **Bounded cost.** Re-including the boundary makes every run re-fetch the
/// revisions sharing that one stamp — at most one upsert-chunk per dataset per
/// run, never unbounded, and it shrinks to nothing as soon as the origin writes
/// a newer stamp. Re-applying them is free: identical content upserts as
/// `ChangeKind::Unchanged`, which writes no revision, so the mirror's own feed
/// does not grow and downstream watches do not re-fire.
///
/// An unparseable resume point is passed through verbatim rather than dropped:
/// the origin answers 400 on it, which is louder than silently restarting the
/// walk from the top.
fn inclusive_since(resume_point: &str) -> String {
    match DateTime::parse_from_rfc3339(resume_point) {
        Ok(dt) => ts(dt.with_timezone(&Utc) - Duration::microseconds(1)),
        Err(_) => resume_point.to_string(),
    }
}

/// Whether a finished walk may advance the durable resume point.
///
/// Schema drift must HALT the walk, not silently discard the items it could not
/// read. `plan_actions` counts an item it cannot parse (`key`/`change` missing,
/// an unknown change kind, a new/changed revision with no snapshot) and moves
/// on; that is right for one bad row, but if the walk then completes and `since`
/// advances past those revisions, a field rename on the origin (`key` →
/// `record_key`) is permanent, silent, total data loss with a green run.
///
/// Freezing the resume point instead makes the drift self-healing: nothing is
/// lost, the same window is re-read on the next run, and the moment the origin
/// (or this crate) is fixed the backlog applies. The cost is re-walking that
/// window every run while the drift persists — which is loud, cheap relative to
/// losing the data, and reported as `status:"drift"` with the count.
fn walk_may_advance(completed: bool, skipped_malformed: usize) -> bool {
    completed && skipped_malformed == 0
}

/// Merges the tombstones a previous run REFUSED to apply with this run's, in
/// stable order, de-duplicated and capped at [`PENDING_TOMBSTONE_CAP`].
///
/// The refusal ([`tombstones_would_empty_the_mirror`]) used to add a note and
/// then let the walk complete and `since` advance — so the removals it declined
/// were never seen again by any run. A refusal is a "not yet", not a "never":
/// carrying the keys in `PeerState` is what makes the next run (when the origin
/// has more live records again, or the operator has intervened) able to apply
/// them.
///
/// Deferred keys come FIRST so the oldest backlog survives the cap.
fn merge_deferred_tombstones(pending: &[String], fresh: &[String]) -> Vec<String> {
    let mut seen: HashSet<&str> = HashSet::new();
    pending
        .iter()
        .chain(fresh.iter())
        .filter(|k| seen.insert(k.as_str()))
        .take(PENDING_TOMBSTONE_CAP)
        .cloned()
        .collect()
}

/// The run-level verdict over the per-dataset statuses.
///
/// The anti-pattern: `pull_one`'s errors were caught into
/// `{"status":"error"}` objects inside an `Ok` result, so a peer whose origin
/// had been unreachable for a week showed a wall of green in the job history —
/// the one place an operator looks. A run is only `ok` when every dataset it was
/// asked for actually came back clean.
#[derive(Debug, PartialEq, Eq)]
enum RunOutcome {
    /// Every dataset is `ok`/`not_modified`.
    Ok,
    /// At least one dataset is degraded (errored, or drift-frozen), but not all
    /// errored — data did land, so the job succeeds and says `partial`.
    Partial,
    /// EVERY dataset errored: nothing was mirrored, so the job itself must fail.
    /// Deliberately not "all datasets degraded" — a drifted dataset still
    /// applied the revisions it could read, which is not a failed run.
    Failed,
}

fn run_outcome(statuses: &[&str]) -> RunOutcome {
    if statuses.is_empty() {
        return RunOutcome::Ok;
    }
    if statuses.iter().all(|s| *s == "error") {
        return RunOutcome::Failed;
    }
    if statuses
        .iter()
        .any(|s| !matches!(*s, "ok" | "not_modified"))
    {
        return RunOutcome::Partial;
    }
    RunOutcome::Ok
}

/// Whether applying `dead` would leave the local mirror with no live record.
///
/// A mirror that empties itself is almost always an origin problem (a feed that
/// replayed every removal, a wiped upstream index) rather than a genuine "this
/// dataset no longer exists", and a mirror is not the place to make that call.
/// The store used to refuse this for us as a side effect of the empty-`present`
/// guard on `detect_removed`; naming it here keeps the behavior after the switch
/// to `tombstone_keys`, which — being removal by name — has no such guard.
fn tombstones_would_empty_the_mirror(live: &[String], dead: &[String]) -> bool {
    if live.is_empty() {
        return false; // nothing live to lose
    }
    let dead: HashSet<&str> = dead.iter().map(String::as_str).collect();
    live.iter().all(|k| dead.contains(k.as_str()))
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
        Some((app, ds)) if !app.trim().is_empty() && !ds.trim().is_empty() && !ds.contains('/') => {
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
        assert_eq!(
            resolve_namespace(None, "hackernews").unwrap(),
            "peer_hackernews"
        );
        assert_eq!(
            resolve_namespace(Some("mirror-a"), "x").unwrap(),
            "mirror-a"
        );
    }

    #[test]
    fn namespace_may_not_shadow_the_remote_app_or_carry_path_chars() {
        // Writing a mirror into the exact namespace a local app owns is the
        // write-origin corruption the design forbids.
        assert!(resolve_namespace(Some("hackernews"), "hackernews").is_err());
        for bad in ["", "a/b", "a b", "a\\b", "peer|x", &"x".repeat(65)] {
            assert!(
                resolve_namespace(Some(bad), "app").is_err(),
                "must reject {bad:?}"
            );
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
            json!({"change": "new"}),                 // no key
            json!({"key": "k", "change": "mystery"}), // unknown change kind
            rev("k2", "new", Value::Null, "t1"),      // new without data
            json!({"key": "k3"}),                     // no change
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
    fn a_mirror_refuses_to_tombstone_itself_empty_but_not_a_partial_sweep() {
        // v1 got this for free: it drove `detect_removed` with a synthetic
        // "present" set, and the store refuses an EMPTY present set. Removal by
        // name has no such guard — it does exactly what it is told — so the
        // refusal has to be stated here or a feed replaying every removal wipes
        // the mirror silently.
        let live = |ks: &[&str]| ks.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(tombstones_would_empty_the_mirror(
            &live(&["a", "b"]),
            &live(&["b", "a"])
        ));
        // Extra dead keys we do not hold locally still count as "empties it".
        assert!(tombstones_would_empty_the_mirror(
            &live(&["a"]),
            &live(&["a", "ghost"])
        ));
        // A partial sweep is the normal case and must go through.
        assert!(!tombstones_would_empty_the_mirror(
            &live(&["a", "b", "c"]),
            &live(&["b"])
        ));
        // Nothing live: no data to lose, so nothing to refuse.
        assert!(!tombstones_would_empty_the_mirror(&[], &live(&["a"])));
    }

    // ── Loss windows (each test is named for the loss it defends against) ───

    /// THE loss window: the feed's `since` predicate is strict (`created_at >
    /// ?`), the feed is ordered by `created_at` and a whole upsert-chunk shares
    /// one stamp. Storing page-1's newest stamp as the resume point and sending
    /// it verbatim excludes, permanently, every revision committed at that same
    /// stamp after the page was served.
    #[test]
    fn equal_stamp_revisions_not_lost() {
        let boundary = "2026-07-31T10:00:00.123456Z";
        let wire = inclusive_since(boundary);
        // Exactly one microsecond back — an EXACT inclusive boundary, because
        // stored stamps are fixed-width micros and nothing can fall between.
        assert_eq!(wire, "2026-07-31T10:00:00.123455Z");
        // The origin compares stamps as fixed-width strings, so this ordering
        // IS the server-side predicate: a revision stamped exactly at the
        // boundary now passes `created_at > since`, where before it could not.
        assert!(
            boundary > wire.as_str(),
            "a same-stamp revision must now be returned, not skipped forever"
        );
        // ...and nothing older leaks back in: the previous representable
        // microsecond is still excluded.
        assert!("2026-07-31T10:00:00.123455Z" <= wire.as_str());

        // Rewinding across a second/minute/hour/day boundary is real arithmetic,
        // not string surgery.
        assert_eq!(
            inclusive_since("2026-08-01T00:00:00.000000Z"),
            "2026-07-31T23:59:59.999999Z"
        );
        // Offset stamps normalize to the stored UTC form on the way out.
        assert_eq!(
            inclusive_since("2026-07-31T12:00:00.000000+02:00"),
            "2026-07-31T09:59:59.999999Z"
        );
        // Garbage passes through so the origin answers 400 — louder than a
        // silent restart from the top of the feed.
        assert_eq!(inclusive_since("not-a-stamp"), "not-a-stamp");
    }

    /// Schema drift used to DISCARD: malformed items were counted, the walk
    /// completed anyway, and `since` advanced past the revisions that were
    /// never applied. A `key` → `record_key` rename on the origin was therefore
    /// total, permanent, silent data loss with a green run.
    #[test]
    fn drift_freezes_cursor_not_advances() {
        // The clean case still advances — freezing on nothing would strand
        // every mirror.
        assert!(walk_may_advance(true, 0));
        // Anything unreadable freezes the resume point, however small.
        assert!(!walk_may_advance(true, 1));
        assert!(!walk_may_advance(true, 900));
        // A capped (suspended) walk never advanced in the first place.
        assert!(!walk_may_advance(false, 0));
        assert!(!walk_may_advance(false, 3));
    }

    /// A refused tombstone batch used to vanish: the note said "not applied",
    /// then `since` advanced past those `removed` revisions and no later run
    /// ever saw them again. The mirror kept records the origin had deleted,
    /// forever, and said `status:"ok"` about it.
    #[test]
    fn deferred_tombstones_retried_not_dropped() {
        let v = |ks: &[&str]| ks.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        // A backlog carried into a run merges with that run's own removals.
        assert_eq!(
            merge_deferred_tombstones(&v(&["a", "b"]), &v(&["c"])),
            v(&["a", "b", "c"])
        );
        // Re-seeing a deferred key does not duplicate it.
        assert_eq!(
            merge_deferred_tombstones(&v(&["a", "b"]), &v(&["b", "c"])),
            v(&["a", "b", "c"])
        );
        // Nothing pending is the ordinary case: this run's list, untouched.
        assert_eq!(merge_deferred_tombstones(&[], &v(&["x"])), v(&["x"]));
        assert!(merge_deferred_tombstones(&[], &[]).is_empty());

        // The backlog is capped, and the OLDEST deferrals survive the cap —
        // dropping the long-pending ones is what would make the loss permanent.
        let pending: Vec<String> = (0..PENDING_TOMBSTONE_CAP)
            .map(|i| format!("p{i}"))
            .collect();
        let merged = merge_deferred_tombstones(&pending, &v(&["fresh"]));
        assert_eq!(merged.len(), PENDING_TOMBSTONE_CAP);
        assert_eq!(merged[0], "p0", "the oldest deferral is kept, not evicted");
        assert!(!merged.contains(&"fresh".to_string()));
    }

    /// A week of total failure used to be a wall of green in the job history:
    /// `pull_one`'s errors became `{"status":"error"}` objects inside an `Ok`.
    #[test]
    fn all_datasets_errored_fails_the_job_not_reports_ok() {
        assert_eq!(run_outcome(&["error"]), RunOutcome::Failed);
        assert_eq!(run_outcome(&["error", "error"]), RunOutcome::Failed);
        // One survivor means data DID land — the job succeeds, degraded.
        assert_eq!(run_outcome(&["error", "ok"]), RunOutcome::Partial);
    }

    /// The other half: one bad dataset must never be reported as a clean run.
    #[test]
    fn one_degraded_dataset_is_partial_not_ok() {
        assert_eq!(run_outcome(&["ok", "error"]), RunOutcome::Partial);
        assert_eq!(run_outcome(&["ok", "drift"]), RunOutcome::Partial);
        assert_eq!(run_outcome(&["not_modified", "drift"]), RunOutcome::Partial);
        // Only genuinely-clean statuses make a clean run.
        assert_eq!(run_outcome(&["ok", "not_modified"]), RunOutcome::Ok);
        assert_eq!(run_outcome(&["ok"]), RunOutcome::Ok);
        // No datasets at all is not a failure (`run` rejects an empty list
        // earlier) — and must not divide-by-zero into `Failed`.
        assert_eq!(run_outcome(&[]), RunOutcome::Ok);
    }

    /// Drift is NOT an error: the revisions the run could read did land, so a
    /// wholly-drifted run must not fail the job — it must freeze and say so.
    #[test]
    fn drift_alone_does_not_fail_the_job() {
        assert_eq!(run_outcome(&["drift"]), RunOutcome::Partial);
        assert_eq!(run_outcome(&["drift", "drift"]), RunOutcome::Partial);
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
