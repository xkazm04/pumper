//! Mesh streams (N16): the pulls that are not dataset revisions — host weather
//! and API recipes — plus the ghost reconcile that closes the hard-delete gap,
//! and the per-(peer, stream) status record `GET /mesh` reads.
//!
//! ## Why these are the `peer` app and not a new job kind
//!
//! The design left the choice open. Three streams, one app, selected by a
//! `stream` param, because everything a scheduled pull needs already exists for
//! an app run and nothing of it exists for a bespoke job kind: budgets and the
//! cost ledger, receipts, retries and attempt semantics, dedup, cancel, the
//! `/jobs` surface and its SSE stream, `validate_app_params` at the enqueue
//! door, and VCR. A `mesh-pull` kind would have re-implemented all of that to
//! avoid one match arm. The three streams also share the pieces that actually
//! matter here — the peer URL, the trust policy, the auth header and the status
//! record — so splitting them would have duplicated the security-relevant half.
//!
//! ## What each stream trusts
//!
//! Everything arrives through [`pumper_core::mesh::open_envelope`] with the trust
//! built from the job's own params ([`trust_from_params`]): the scheduler copies
//! the peer's pinned key and `allow_unsigned` onto every job it enqueues, so the
//! app never needs config access and an operator can reproduce a scheduled pull
//! exactly by POSTing the same params by hand.
//!
//! ## What a pull may NOT do
//!
//! - Adopt a peer's recipe validation verdict ([`importable_recipe`]): that was
//!   a replay from the peer's egress IP, not this node's.
//! - Import a penalty above the per-peer ceiling ([`cap_imported_penalty`]).
//! - Tombstone against an incomplete origin manifest ([`ghosts_to_tombstone`]).

use std::collections::HashMap;

use pumper_core::{plan_weather_import, AppContext, Error, HttpRequest, Result, WeatherPlan};
use serde_json::{json, Value};

use crate::tombstones_would_empty_the_mirror;
/// The bundle shapes moved to `pumper_core::mesh` with the envelope they travel
/// in; re-exported here so the paths the server and this app already use keep
/// resolving to the one implementation.
pub use pumper_core::mesh::{exportable_recipe, importable_recipe, weather_entries};
use pumper_core::mesh::{
    ghost_keys, manifest_digest, open_envelope, PeerTrust, SCHEMA_RECIPES_V1, SCHEMA_WEATHER_V1,
    SCHEMA_WEATHER_V2,
};

/// Dataset (under app `peer`) holding one status record per (peer, stream).
/// Read by `GET /mesh`; see `crates/server/src/routes/mesh.rs`.
pub const MESH_DATASET: &str = "mesh";
/// Ceiling on entries accepted from one weather bundle — host-level intel, not
/// a bulk data channel. Matches the server's import route.
pub const MAX_WEATHER_ENTRIES: usize = 10_000;
/// Ceiling on recipes accepted from one bundle.
pub const MAX_RECIPE_ENTRIES: usize = 5_000;
/// Ceiling on ghosts one reconcile pass tombstones. A pass that would remove
/// more says so and removes none: at that scale the honest diagnosis is "the
/// origin and this mirror have diverged", not "delete 40,000 records".
pub const MAX_GHOSTS_PER_PASS: usize = 5_000;

// ── trust and auth, from the job's own params ───────────────────────────────

/// The trust rule for this pull, read off the job params the scheduler wrote.
pub fn trust_from_params(params: &Value) -> PeerTrust {
    PeerTrust {
        public_key: params
            .get("public_key")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .map(str::to_string),
        allow_unsigned: params
            .get("allow_unsigned")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

/// Resolves the `api_key` param.
///
/// `env:VAR` reads the environment at run time, which is the recommended form:
/// the schedule row and every job it enqueues are readable on `GET /schedules`
/// and `GET /jobs/{id}`, so a literal key there is a key on the wire of every
/// operator's console. A literal is still accepted — an operator who wrote one
/// has chosen that — but `env:` keeps only the variable NAME in the row.
///
/// A named variable that is not set returns `None`: a pull that then gets a 401
/// says so loudly, which is a better failure than sending the literal string
/// `env:PUMPER_VPS_KEY` as a credential.
pub fn resolve_api_key(raw: Option<&str>) -> Option<String> {
    let raw = raw.map(str::trim).filter(|s| !s.is_empty())?;
    match raw.strip_prefix("env:") {
        Some(var) => std::env::var(var.trim()).ok().filter(|v| !v.is_empty()),
        None => Some(raw.to_string()),
    }
}

/// Headers a pull presents to the peer. Empty when no key is configured, which
/// is exactly right for a peer running `[auth] mode = "open"`.
///
/// `x-pumper-key` rather than `Authorization: Bearer` because the vendor header
/// is the one the peer's own auth layer reads first-class and it cannot be
/// confused with a proxy's credentials in transit.
pub fn auth_headers(api_key: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    if let Some(key) = resolve_api_key(api_key) {
        headers.insert("x-pumper-key".to_string(), key);
    }
    headers
}

/// Applies the per-peer politeness ceiling on top of core's import cap.
///
/// The blast radius of a leaked peer key, stated as a number: the worst a
/// compromised peer can do to this node's politeness is slow it down by
/// `max_penalty_secs`. `0` means "no extra ceiling" — core's own import cap
/// still applies, so this can never *widen* what core allowed.
pub fn cap_imported_penalty(plan_ms: Option<u64>, max_penalty_secs: u64) -> Option<u64> {
    let ms = plan_ms?;
    if max_penalty_secs == 0 {
        return Some(ms);
    }
    let ceiling = max_penalty_secs.saturating_mul(1000);
    Some(ms.min(ceiling))
}

// ── ghost reconcile ─────────────────────────────────────────────────────────

/// What a reconcile pass decided, before anything is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GhostVerdict {
    /// Digests agree: nothing to do, and no key list needed.
    InSync,
    /// These keys exist here and not at the origin — tombstone them.
    Tombstone(Vec<String>),
    /// A difference was seen but must NOT be acted on, and why.
    Refused(String),
}

/// Decides what a reconcile pass may remove. Pure: two digests, two key sets,
/// one verdict — so every refusal below is testable without two nodes.
///
/// The refusals are the point. A reconcile that tombstones on a bad premise
/// deletes live records, which is strictly worse than the ghosts it exists to
/// remove, so each of these is a *stop*, not a warning:
/// - the origin's manifest was truncated (`complete: false`) — the keys it did
///   not list are indistinguishable from keys it does not have;
/// - the diff would empty the mirror entirely (the puller's existing rule);
/// - the diff is larger than [`MAX_GHOSTS_PER_PASS`] — at that scale the honest
///   diagnosis is divergence, not deletion.
pub fn ghosts_to_tombstone(
    local_digest: &str,
    origin_digest: &str,
    origin_complete: bool,
    local_live: &[String],
    origin_live: &[String],
) -> GhostVerdict {
    if local_digest == origin_digest {
        return GhostVerdict::InSync;
    }
    if !origin_complete {
        return GhostVerdict::Refused(
            "the origin's manifest is TRUNCATED (complete: false); keys it did not list are \
             indistinguishable from keys it does not have, so nothing is tombstoned"
                .into(),
        );
    }
    let ghosts = ghost_keys(local_live, origin_live);
    if ghosts.is_empty() {
        // Digests differ but this mirror holds no extra keys: the origin has
        // records this mirror has not pulled yet. That is a pull to make, not a
        // deletion to perform.
        return GhostVerdict::InSync;
    }
    if ghosts.len() > MAX_GHOSTS_PER_PASS {
        return GhostVerdict::Refused(format!(
            "{} ghost(s) is past the {MAX_GHOSTS_PER_PASS} per-pass ceiling — this looks like \
             divergence, not a hard delete; nothing was tombstoned",
            ghosts.len()
        ));
    }
    if tombstones_would_empty_the_mirror(local_live, &ghosts) {
        return GhostVerdict::Refused(
            "the reconcile would tombstone the ENTIRE local mirror, which this app refuses \
             (delete the namespace explicitly if that is intended)"
                .into(),
        );
    }
    GhostVerdict::Tombstone(ghosts)
}

// ── the mesh status record ──────────────────────────────────────────────────

/// One pull's contribution to the (peer, stream) status record.
#[derive(Debug, Clone, Default)]
pub struct MeshOutcome {
    pub ok: bool,
    pub verified: bool,
    pub signature_failure: bool,
    pub ghosts_removed: i64,
    pub node_id: Option<String>,
    pub detail: Option<String>,
}

/// Folds this pull's outcome into the stored status record.
///
/// Counters accumulate across runs (`pulls`, `signature_failures`,
/// `ghosts_removed`) because they are what a mesh dashboard trends;
/// `last_success_at` moves ONLY on a successful pull, so a peer that broke a
/// week ago reports a week of lag rather than a fresh timestamp from its
/// most recent failure. That distinction is the whole value of the record.
pub fn mesh_record(
    prev: Option<&Value>,
    url: &str,
    stream: &str,
    out: &MeshOutcome,
    now: &str,
) -> Value {
    let prior = |k: &str| {
        prev.and_then(|p| p.get(k))
            .and_then(Value::as_i64)
            .unwrap_or(0)
    };
    let last_success = if out.ok {
        Some(now.to_string())
    } else {
        prev.and_then(|p| p.get("last_success_at"))
            .and_then(Value::as_str)
            .map(str::to_string)
    };
    json!({
        "url": url,
        "stream": stream,
        "last_attempt_at": now,
        "last_success_at": last_success,
        "ok": out.ok,
        "verified": out.verified,
        "node_id": out.node_id,
        "pulls": prior("pulls") + 1,
        "signature_failures": prior("signature_failures") + i64::from(out.signature_failure),
        "ghosts_removed": prior("ghosts_removed") + out.ghosts_removed,
        "detail": out.detail,
    })
}

/// Persists the status record for one (peer, stream).
pub async fn write_mesh_status(
    ctx: &AppContext,
    peer_label: &str,
    stream_slug: &str,
    url: &str,
    out: &MeshOutcome,
) -> Result<()> {
    let key = format!("{peer_label}|{stream_slug}");
    let prev = ctx
        .datasets
        .get(&ctx.app, MESH_DATASET, &key)
        .await?
        .map(|r| r.data);
    let now = chrono::Utc::now().to_rfc3339();
    let record = mesh_record(prev.as_ref(), url, stream_slug, out, &now);
    ctx.upsert(MESH_DATASET, &key, &record).await?;
    Ok(())
}

// ── the pulls ───────────────────────────────────────────────────────────────

/// Fetches and opens one signed bundle from a peer.
async fn fetch_bundle(
    ctx: &AppContext,
    url: &str,
    api_key: Option<&str>,
    schema: &str,
    legacy: Option<&str>,
    trust: &PeerTrust,
) -> Result<pumper_core::mesh::Opened> {
    let mut req = HttpRequest::get(url);
    // A bundle is live intelligence; the TTL cache must not serve yesterday's.
    req.no_cache = true;
    req.headers = auth_headers(api_key);
    let resp = ctx.engines.http.fetch(req).await?;
    if !resp.is_success() {
        return Err(Error::App(format!(
            "peer bundle {url} returned status {}",
            resp.status
        )));
    }
    let body: Value = serde_json::from_str(&resp.body)
        .map_err(|e| Error::App(format!("peer bundle {url}: response is not JSON: {e}")))?;
    open_envelope(&body, schema, legacy, trust)
        .map_err(|e| Error::App(format!("peer bundle {url} REFUSED: {e}")))
}

/// Pulls a peer's host-weather bundle and merges it, conservatively.
///
/// The merge itself is core's (`plan_weather_import`) — raise-only, count-
/// weighted, never downgrading a locally-observed pin. Two things are this
/// function's own: the signature check above it, and the per-peer penalty
/// ceiling below it.
///
/// **Known gap, stated rather than hidden:** an app has no handle on the LIVE
/// governor (it is server state), so an imported penalty lands in tier memory
/// and in the persisted penalty snapshot, and the in-process governor adopts it
/// at the next boot. The manual `POST /host-weather/import?apply=true` route,
/// which runs inside the server, still raises the live governor immediately.
pub async fn pull_weather(
    ctx: &AppContext,
    base: &str,
    api_key: Option<&str>,
    trust: &PeerTrust,
    max_penalty_secs: u64,
) -> Result<(Value, MeshOutcome)> {
    let url = format!("{base}/host-weather/export");
    let opened = fetch_bundle(
        ctx,
        &url,
        api_key,
        SCHEMA_WEATHER_V2,
        Some(SCHEMA_WEATHER_V1),
        trust,
    )
    .await?;
    let entries = weather_entries(&opened.payload).map_err(Error::App)?;
    if entries.len() > MAX_WEATHER_ENTRIES {
        return Err(Error::App(format!(
            "peer weather bundle has {} entries; the ceiling is {MAX_WEATHER_ENTRIES}",
            entries.len()
        )));
    }
    let mut plans: Vec<WeatherPlan> = Vec::new();
    let mut noops = 0usize;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for entry in &entries {
        let host = entry.host.trim().to_lowercase();
        if host.is_empty() || !seen.insert(host.clone()) {
            noops += 1;
            continue;
        }
        let local = ctx.tiers.get(&host).await?;
        // The persisted snapshot stands in for the live governor value here —
        // see the function docs.
        let local_ms = local
            .as_ref()
            .map(|l| l.penalty_ms.max(0) as u64)
            .unwrap_or(0);
        let mut plan = plan_weather_import(local.as_ref(), local_ms, entry);
        plan.raise_penalty_ms = cap_imported_penalty(plan.raise_penalty_ms, max_penalty_secs);
        if plan.is_noop() {
            noops += 1;
            continue;
        }
        plans.push(plan);
    }
    let mut penalties: Vec<(String, u64)> = Vec::new();
    for plan in &plans {
        ctx.tiers.apply_weather(plan).await?;
        if let Some(ms) = plan.raise_penalty_ms {
            penalties.push((plan.host.clone(), ms));
        }
    }
    ctx.tiers.save_penalties(&penalties).await?;

    let report = json!({
        "stream": "weather",
        "status": "ok",
        "verified": opened.verified,
        "source_node_id": opened.node_id,
        "considered": entries.len(),
        "changed": plans.len(),
        "noops": noops,
        "penalties_raised": penalties.len(),
        "max_penalty_secs": max_penalty_secs,
        // The gap, in the result rather than only in a doc.
        "note": "imported penalties land in tier memory and the persisted snapshot; the live \
                 in-process governor adopts them at the next restart",
    });
    let outcome = MeshOutcome {
        ok: true,
        verified: opened.verified,
        node_id: opened.node_id.clone(),
        ..Default::default()
    };
    Ok((report, outcome))
}

/// Pulls a peer's recipe bundle and stores the entries as LOCAL candidates.
pub async fn pull_recipes(
    ctx: &AppContext,
    base: &str,
    api_key: Option<&str>,
    trust: &PeerTrust,
) -> Result<(Value, MeshOutcome)> {
    let url = format!("{base}/recipes/export");
    let opened = fetch_bundle(ctx, &url, api_key, SCHEMA_RECIPES_V1, None, trust).await?;
    let entries = opened
        .payload
        .get("entries")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if entries.len() > MAX_RECIPE_ENTRIES {
        return Err(Error::App(format!(
            "peer recipe bundle has {} entries; the ceiling is {MAX_RECIPE_ENTRIES}",
            entries.len()
        )));
    }
    let mut imported = 0usize;
    let mut skipped = 0usize;
    let mut notes: Vec<String> = Vec::new();
    for entry in &entries {
        match importable_recipe(entry) {
            Ok(recipe) => {
                ctx.recipes.upsert(&recipe).await?;
                imported += 1;
            }
            Err(why) => {
                skipped += 1;
                if notes.len() < 20 {
                    notes.push(why);
                }
            }
        }
    }
    let report = json!({
        "stream": "recipes",
        "status": "ok",
        "verified": opened.verified,
        "source_node_id": opened.node_id,
        "considered": entries.len(),
        "imported": imported,
        "skipped": skipped,
        "notes": notes,
        "note": "every imported recipe lands validated: false — a peer's replay verdict was \
                 earned from ITS egress IP, and this node's validator proves it here",
    });
    let outcome = MeshOutcome {
        ok: true,
        verified: opened.verified,
        node_id: opened.node_id.clone(),
        ..Default::default()
    };
    Ok((report, outcome))
}

/// Reconciles one mirrored dataset against the origin's live-set manifest and
/// tombstones the ghosts a hard delete left behind.
///
/// Runs AFTER the revision walk, so the mirror is as caught up as this run is
/// going to make it before the sets are compared — reconciling first would
/// diagnose "not pulled yet" as "ghost" every single run.
pub async fn reconcile_ghosts(
    ctx: &AppContext,
    base: &str,
    api_key: Option<&str>,
    remote_app: &str,
    dataset: &str,
    namespace: &str,
) -> Result<Value> {
    let count = ctx.datasets.record_count(namespace, dataset).await?;
    let local_live: Vec<String> = ctx
        .datasets
        .list(namespace, dataset, count.max(1))
        .await?
        .into_iter()
        .filter(|r| r.removed_at.is_none())
        .map(|r| r.key)
        .collect();
    let local_digest = manifest_digest(&local_live);

    let url = format!("{base}/datasets/{remote_app}/{dataset}/manifest?keys=true");
    let mut req = HttpRequest::get(&url);
    req.no_cache = true;
    req.headers = auth_headers(api_key);
    let resp = ctx.engines.http.fetch(req).await?;
    if resp.status == 404 {
        // An origin on a build without `/manifest` is not an error — it is a
        // node that cannot be reconciled yet, and saying so is the honest
        // answer rather than a silent skip.
        return Ok(json!({
            "reconciled": false,
            "reason": "the origin does not serve /manifest (pre-N16 build); ghosts from a hard \
                       delete cannot be detected against it",
            "ghosts_removed": 0,
        }));
    }
    if !resp.is_success() {
        return Err(Error::App(format!(
            "peer manifest {url} returned status {}",
            resp.status
        )));
    }
    let body: Value = serde_json::from_str(&resp.body)
        .map_err(|e| Error::App(format!("peer manifest {url}: response is not JSON: {e}")))?;
    let origin_digest = body
        .get("digest")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let origin_complete = body
        .get("complete")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let origin_live: Vec<String> = body
        .get("keys")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();

    let verdict = ghosts_to_tombstone(
        &local_digest,
        &origin_digest,
        origin_complete,
        &local_live,
        &origin_live,
    );
    match verdict {
        GhostVerdict::InSync => Ok(json!({
            "reconciled": true,
            "in_sync": true,
            "local_digest": local_digest,
            "origin_digest": origin_digest,
            "ghosts_removed": 0,
        })),
        GhostVerdict::Refused(why) => Ok(json!({
            "reconciled": false,
            "in_sync": false,
            "local_digest": local_digest,
            "origin_digest": origin_digest,
            "reason": why,
            "ghosts_removed": 0,
        })),
        GhostVerdict::Tombstone(ghosts) => {
            let removed = ctx
                .datasets
                .tombstone_keys(namespace, dataset, &ghosts)
                .await?;
            Ok(json!({
                "reconciled": true,
                "in_sync": false,
                "local_digest": local_digest,
                "origin_digest": origin_digest,
                "ghosts_removed": removed.len(),
                "ghost_keys": removed,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trust_comes_from_the_jobs_own_params_so_a_pull_is_reproducible_by_hand() {
        let params = json!({"public_key": "  aabb  ", "allow_unsigned": true});
        let t = trust_from_params(&params);
        assert_eq!(t.public_key.as_deref(), Some("aabb"));
        assert!(t.allow_unsigned);
        // The safe defaults: no key, and NOT allowed to be unverifiable.
        let bare = trust_from_params(&json!({}));
        assert!(bare.public_key.is_none());
        assert!(!bare.allow_unsigned, "unsigned must never be the default");
        assert!(trust_from_params(&json!({"public_key": "   "}))
            .public_key
            .is_none());
    }

    #[test]
    fn an_env_api_key_is_resolved_at_run_time_and_a_missing_one_is_absent_not_literal() {
        std::env::set_var("PUMPER_TEST_MESH_KEY", "secret-value");
        assert_eq!(
            resolve_api_key(Some("env:PUMPER_TEST_MESH_KEY")).as_deref(),
            Some("secret-value")
        );
        assert_eq!(resolve_api_key(Some("literal")).as_deref(), Some("literal"));
        assert_eq!(
            resolve_api_key(Some("env:PUMPER_TEST_MESH_UNSET")),
            None,
            "an unset variable must not be sent as the literal string 'env:...'"
        );
        assert_eq!(resolve_api_key(Some("  ")), None);
        assert_eq!(resolve_api_key(None), None);
        std::env::remove_var("PUMPER_TEST_MESH_KEY");
    }

    #[test]
    fn no_key_means_no_header_not_an_empty_one() {
        assert!(auth_headers(None).is_empty());
        let h = auth_headers(Some("k"));
        assert_eq!(h.get("x-pumper-key").map(String::as_str), Some("k"));
    }

    #[test]
    fn the_per_peer_penalty_ceiling_clamps_and_never_widens() {
        assert_eq!(cap_imported_penalty(Some(90_000), 60), Some(60_000));
        assert_eq!(cap_imported_penalty(Some(10_000), 60), Some(10_000));
        // 0 = no extra ceiling; core's own cap already applied upstream.
        assert_eq!(cap_imported_penalty(Some(90_000), 0), Some(90_000));
        assert_eq!(cap_imported_penalty(None, 60), None);
    }

    fn keys(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn matching_digests_short_circuit_without_reading_a_key_list() {
        assert_eq!(
            ghosts_to_tombstone("same", "same", true, &keys(&["a"]), &[]),
            GhostVerdict::InSync,
            "equal digests must not consult the key sets at all"
        );
    }

    #[test]
    fn a_hard_delete_on_the_origin_becomes_a_tombstone_on_the_mirror() {
        let local = keys(&["a", "b", "ghost"]);
        let origin = keys(&["a", "b"]);
        assert_eq!(
            ghosts_to_tombstone(
                &manifest_digest(&local),
                &manifest_digest(&origin),
                true,
                &local,
                &origin
            ),
            GhostVerdict::Tombstone(keys(&["ghost"]))
        );
    }

    #[test]
    fn a_truncated_origin_manifest_tombstones_nothing() {
        let local = keys(&["a", "b", "ghost"]);
        let origin = keys(&["a", "b"]);
        match ghosts_to_tombstone(
            &manifest_digest(&local),
            &manifest_digest(&origin),
            false,
            &local,
            &origin,
        ) {
            GhostVerdict::Refused(why) => assert!(why.contains("TRUNCATED"), "{why}"),
            other => panic!("a partial origin view must never delete: {other:?}"),
        }
    }

    #[test]
    fn keys_the_mirror_has_not_pulled_yet_are_not_ghosts() {
        let local = keys(&["a"]);
        let origin = keys(&["a", "fresh"]);
        assert_eq!(
            ghosts_to_tombstone(
                &manifest_digest(&local),
                &manifest_digest(&origin),
                true,
                &local,
                &origin
            ),
            GhostVerdict::InSync,
            "a differing digest with no local-only key is a pull to make, not a delete"
        );
    }

    #[test]
    fn a_reconcile_refuses_to_empty_the_mirror_or_to_delete_at_divergence_scale() {
        let local = keys(&["a", "b"]);
        let origin: Vec<String> = Vec::new();
        match ghosts_to_tombstone(
            &manifest_digest(&local),
            &manifest_digest(&origin),
            true,
            &local,
            &origin,
        ) {
            GhostVerdict::Refused(why) => assert!(why.contains("ENTIRE"), "{why}"),
            other => panic!("emptying the mirror must be refused: {other:?}"),
        }
        let many: Vec<String> = (0..MAX_GHOSTS_PER_PASS + 2)
            .map(|i| format!("k{i}"))
            .collect();
        let kept = keys(&["survivor"]);
        let mut local_many = many.clone();
        local_many.extend(kept.clone());
        match ghosts_to_tombstone(
            &manifest_digest(&local_many),
            &manifest_digest(&kept),
            true,
            &local_many,
            &kept,
        ) {
            GhostVerdict::Refused(why) => assert!(why.contains("per-pass ceiling"), "{why}"),
            other => panic!("divergence-scale diffs must be refused: {other:?}"),
        }
    }

    #[test]
    fn a_failed_pull_does_not_move_last_success_at() {
        let ok = mesh_record(
            None,
            "https://a",
            "weather",
            &MeshOutcome {
                ok: true,
                verified: true,
                ..Default::default()
            },
            "2026-09-01T00:00:00Z",
        );
        assert_eq!(ok["last_success_at"], "2026-09-01T00:00:00Z");
        assert_eq!(ok["pulls"], 1);

        let failed = mesh_record(
            Some(&ok),
            "https://a",
            "weather",
            &MeshOutcome {
                ok: false,
                signature_failure: true,
                detail: Some("forged".into()),
                ..Default::default()
            },
            "2026-09-08T00:00:00Z",
        );
        assert_eq!(
            failed["last_success_at"], "2026-09-01T00:00:00Z",
            "a week of lag must stay visible, not be reset by the failure that caused it"
        );
        assert_eq!(failed["last_attempt_at"], "2026-09-08T00:00:00Z");
        assert_eq!(failed["ok"], false);
        assert_eq!(failed["pulls"], 2, "counters accumulate across runs");
        assert_eq!(failed["signature_failures"], 1);
        assert_eq!(failed["detail"], "forged");
    }

    #[test]
    fn ghosts_removed_accumulates_rather_than_reporting_only_the_last_pass() {
        let first = mesh_record(
            None,
            "https://a",
            "datasets-hn-stories",
            &MeshOutcome {
                ok: true,
                ghosts_removed: 3,
                ..Default::default()
            },
            "t1",
        );
        let second = mesh_record(
            Some(&first),
            "https://a",
            "datasets-hn-stories",
            &MeshOutcome {
                ok: true,
                ghosts_removed: 2,
                ..Default::default()
            },
            "t2",
        );
        assert_eq!(second["ghosts_removed"], 5);
    }
}
