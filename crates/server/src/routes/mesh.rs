//! The mesh (N16): node identity, the trust policy every bundle import runs
//! through, and `GET /mesh` — the one page that answers "is my fleet actually
//! syncing".
//!
//! ## What a mesh is here
//!
//! A list of `[[peer]]` rows in this node's config, each naming a peer URL, its
//! ed25519 public key, and the streams to pull from it (`weather`, `recipes`,
//! `datasets:<app>/<dataset>`). The scheduler turns each (peer, stream) into an
//! ordinary schedule that runs the `peer` app; the app pulls, verifies and
//! applies. There is no push, no discovery, no gossip and no leader — federation
//! in the direction that needs no inbound port and no shared secret.
//!
//! ## Where the status comes from
//!
//! Nowhere new. `[[peer]]` is the desired state, the `schedules` table is what
//! the scheduler made of it, and the `peer/mesh` dataset is what the pulls
//! recorded (one record per peer+stream, written by the app at the end of every
//! run). `GET /mesh` joins the three. That is deliberate: a mesh whose status
//! lived in its own table could disagree with the jobs that actually ran, and
//! the failure mode of a sync feature is precisely a dashboard that says green
//! while nothing has moved for a week.

use axum::extract::State;
use axum::Json;
use pumper_core::config::{PeerConfig, PullStream};
use serde_json::{json, Value};

use crate::routes::error::ApiError;
use crate::state::AppState;

/// App the mesh's own bookkeeping records live under (alongside `peer/state`,
/// the puller's cursor records).
pub(crate) const MESH_APP: &str = "peer";
/// Dataset holding one status record per (peer, stream).
pub(crate) const MESH_DATASET: &str = "mesh";
/// `managed_by` tag on schedules the `[[peer]]` reconcile owns. Every write is
/// SQL-fenced on it, exactly as the catalog reconcile is fenced on `catalog`,
/// so a hand-made schedule can never be rewritten by a peer row.
pub(crate) const PEER_MANAGED_BY: &str = "peer";

/// Key of the `peer/mesh` status record for one (peer, stream).
///
/// Extracted because BOTH sides use it — the app writes it, this route reads it
/// — and a status page that silently reads a key nothing writes is the exact
/// "green dashboard, dead mesh" failure this module is trying not to have.
pub(crate) fn mesh_state_key(peer_label: &str, stream_slug: &str) -> String {
    format!("{peer_label}|{stream_slug}")
}

/// Schedule id for one (peer, stream). Stable across restarts and config
/// reorders — it is derived from the peer's LABEL, not its index, so inserting
/// a peer at the top of the file does not re-point every other schedule.
pub(crate) fn peer_schedule_id(peer_label: &str, stream_slug: &str) -> String {
    format!("peer-{peer_label}-{stream_slug}")
}

/// The trust rule applied to a bundle offered to this node.
///
/// Precedence:
/// 1. **No `[[peer]]` rows at all** → the node is not in a mesh. Imports behave
///    exactly as they did before N16: an operator curling a bundle in by hand
///    is trusted, because the only way it got here was through an authenticated
///    admin call they made themselves.
/// 2. **A peer whose pinned key fingerprints to this bundle's `node_id`** → that
///    peer's rule. This is the strong path and the one the scheduler always
///    takes.
/// 3. **Anything else, once peers exist** → unsigned/unverifiable, allowed only
///    if some peer row has explicitly said `allow_unsigned = true`. An operator
///    who has pinned keys for a fleet has said, by doing so, that anonymous
///    bundles are not welcome.
pub(crate) fn trust_for(peers: &[PeerConfig], node_id: Option<&str>) -> app_peer::envelope::PeerTrust {
    use app_peer::envelope::{fingerprint_hex, PeerTrust};
    if peers.is_empty() {
        return PeerTrust {
            public_key: None,
            allow_unsigned: true,
        };
    }
    if let Some(id) = node_id.filter(|s| !s.is_empty()) {
        for p in peers {
            let key = p.public_key.trim();
            if key.is_empty() {
                continue;
            }
            if fingerprint_hex(key).as_deref() == Some(id) {
                return PeerTrust {
                    public_key: Some(key.to_string()),
                    allow_unsigned: p.allow_unsigned,
                };
            }
        }
    }
    PeerTrust {
        public_key: None,
        allow_unsigned: peers.iter().any(|p| p.allow_unsigned),
    }
}

/// This node's identity on the mesh.
#[utoipa::path(
    get,
    path = "/node",
    tag = "mesh",
    responses((status = 200, description = "`{node_id, legacy_id, algo: \"ed25519\", public_key, \
        key_path, key_created}` — `node_id` is the key fingerprint a peer pins as `public_key`'s \
        owner; `legacy_id` is the pre-N16 database-path hash, kept for one release."))
)]
pub(crate) async fn get_node(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let id = crate::node::identity(&state).map_err(|e| {
        ApiError(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("node identity unavailable: {e}"),
        )
    })?;
    Ok(Json(json!({
        "node_id": id.node_id(),
        "legacy_id": id.legacy_id(),
        "algo": "ed25519",
        "public_key": id.public_key_hex(),
        "key_path": id.key_path().display().to_string(),
        "key_created": id.created(),
    })))
}

/// Mesh status: every configured peer, every stream, and what the last pull did.
#[utoipa::path(
    get,
    path = "/mesh",
    tag = "mesh",
    responses((status = 200, description = "`{node_id, peers: [{name, url, key_pinned, \
        allow_unsigned, every, every_secs, enabled, streams: [{stream, schedule_id, \
        scheduled, last_attempt_at, last_success_at, lag_secs, ok, verified, pulls, \
        signature_failures, ghosts_removed, detail}]}], totals: {...}}` — a peer with no \
        recorded pull reports nulls, never a fabricated zero timestamp."))
)]
pub(crate) async fn get_mesh(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let node_id = crate::node::identity(&state)
        .map(|i| i.node_id().to_string())
        .unwrap_or_default();
    let schedules: std::collections::HashSet<String> = state
        .storage
        .list_schedules()
        .await?
        .into_iter()
        .map(|s| s.id)
        .collect();
    let now = chrono::Utc::now();

    let mut peers = Vec::new();
    let mut totals = MeshTotals::default();
    for (i, peer) in state.config.peer.iter().enumerate() {
        let label = peer.label(i);
        let streams: Vec<PullStream> = peer.streams().unwrap_or_default();
        let mut stream_rows = Vec::new();
        for stream in &streams {
            let slug = stream.slug();
            let schedule_id = peer_schedule_id(&label, &slug);
            let record = state
                .datasets
                .get(MESH_APP, MESH_DATASET, &mesh_state_key(&label, &slug))
                .await?
                .map(|r| r.data);
            let last_success = record
                .as_ref()
                .and_then(|d| d.get("last_success_at"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let lag_secs = last_success
                .as_deref()
                .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
                .map(|t| (now - t.with_timezone(&chrono::Utc)).num_seconds().max(0));
            let num = |k: &str| {
                record
                    .as_ref()
                    .and_then(|d| d.get(k))
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
            };
            totals.pulls += num("pulls");
            totals.signature_failures += num("signature_failures");
            totals.ghosts_removed += num("ghosts_removed");
            totals.streams += 1;
            stream_rows.push(json!({
                "stream": slug,
                "schedule_id": schedule_id,
                // Whether the reconcile pass has actually made the row. False
                // with `enabled: true` above means the scheduler has not run a
                // reconcile yet (or refused the row) — a real, findable state.
                "scheduled": schedules.contains(&schedule_id),
                "last_attempt_at": record.as_ref().and_then(|d| d.get("last_attempt_at")).cloned(),
                "last_success_at": last_success,
                // Null, not 0: "never pulled" is not "pulled just now".
                "lag_secs": lag_secs,
                "ok": record.as_ref().and_then(|d| d.get("ok")).cloned(),
                "verified": record.as_ref().and_then(|d| d.get("verified")).cloned(),
                "pulls": num("pulls"),
                "signature_failures": num("signature_failures"),
                "ghosts_removed": num("ghosts_removed"),
                "detail": record.as_ref().and_then(|d| d.get("detail")).cloned(),
            }));
        }
        totals.peers += 1;
        peers.push(json!({
            "name": label,
            "url": peer.url,
            "key_pinned": !peer.public_key.trim().is_empty(),
            "allow_unsigned": peer.allow_unsigned,
            "every": peer.every,
            "every_secs": peer.every_secs(),
            "enabled": peer.enabled,
            "streams": stream_rows,
        }));
    }

    Ok(Json(json!({
        "node_id": node_id,
        "peers": peers,
        "totals": {
            "peers": totals.peers,
            "streams": totals.streams,
            "pulls": totals.pulls,
            "signature_failures": totals.signature_failures,
            "ghosts_removed": totals.ghosts_removed,
        },
    })))
}

/// The `pumper_mesh_*` counters, aggregated across every peer and stream.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct MeshTotals {
    pub peers: i64,
    pub streams: i64,
    pub pulls: i64,
    pub signature_failures: i64,
    pub ghosts_removed: i64,
}

impl MeshTotals {
    /// Reads the totals straight off the `peer/mesh` records, for `/metrics`.
    ///
    /// Infallible on purpose. A per-stream read failure contributes nothing and
    /// the pass continues: `/metrics` is scraped by something that alerts, and
    /// one unreadable record must not take every other series down with it. The
    /// CONFIGURED counts (`peers`, `streams`) are read from config and are
    /// therefore always right even when the recorded counters are not.
    pub(crate) async fn collect(state: &AppState) -> Self {
        let mut totals = MeshTotals {
            peers: state.config.peer.len() as i64,
            ..Default::default()
        };
        for (i, peer) in state.config.peer.iter().enumerate() {
            let label = peer.label(i);
            for stream in peer.streams().unwrap_or_default() {
                totals.streams += 1;
                let record = state
                    .datasets
                    .get(MESH_APP, MESH_DATASET, &mesh_state_key(&label, &stream.slug()))
                    .await
                    .ok()
                    .flatten()
                    .map(|r| r.data);
                let num = |k: &str| {
                    record
                        .as_ref()
                        .and_then(|d| d.get(k))
                        .and_then(Value::as_i64)
                        .unwrap_or(0)
                };
                totals.pulls += num("pulls");
                totals.signature_failures += num("signature_failures");
                totals.ghosts_removed += num("ghosts_removed");
            }
        }
        totals
    }
}

/// Prometheus lines for the mesh, emitted **at zero** on a node with no peers —
/// the same rule the egress counters follow, so a dashboard panel exists before
/// the first peer is configured rather than appearing (and alerting) the moment
/// one is.
pub(crate) fn mesh_metrics_lines(totals: &MeshTotals) -> String {
    let mut out = String::new();
    let series = [
        (
            "pumper_mesh_peers",
            "gauge",
            "Configured [[peer]] rows.",
            totals.peers,
        ),
        (
            "pumper_mesh_streams",
            "gauge",
            "Configured peer streams (peer x pull entry).",
            totals.streams,
        ),
        (
            "pumper_mesh_pulls_total",
            "counter",
            "Peer pulls recorded across all streams.",
            totals.pulls,
        ),
        (
            "pumper_mesh_signature_failures_total",
            "counter",
            "Bundles refused because they were unsigned, unverifiable or forged.",
            totals.signature_failures,
        ),
        (
            "pumper_mesh_ghosts_removed_total",
            "counter",
            "Mirrored records tombstoned by a reconcile because the origin no longer has them.",
            totals.ghosts_removed,
        ),
    ];
    for (name, kind, help, value) in series {
        out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use app_peer::envelope::fingerprint;

    fn peer_with(key: Option<&str>, allow_unsigned: bool) -> PeerConfig {
        PeerConfig {
            url: "https://a.example".into(),
            public_key: key.unwrap_or_default().to_string(),
            pull: vec!["weather".into()],
            every: "15m".into(),
            allow_unsigned,
            enabled: true,
            ..Default::default()
        }
    }

    #[test]
    fn a_node_with_no_peers_keeps_the_pre_mesh_import_behaviour() {
        let trust = trust_for(&[], Some("whatever"));
        assert!(
            trust.allow_unsigned,
            "a node that is not in a mesh must still accept a hand-curled bundle"
        );
        assert!(trust.public_key.is_none());
    }

    #[test]
    fn a_pinned_peer_is_matched_by_its_key_fingerprint_not_by_url_or_order() {
        let key = hex::encode([9u8; 32]);
        let id = fingerprint(&[9u8; 32]);
        let peers = vec![peer_with(Some(&hex::encode([1u8; 32])), false), peer_with(Some(&key), true)];
        let trust = trust_for(&peers, Some(&id));
        assert_eq!(trust.public_key.as_deref(), Some(key.as_str()));
        assert!(trust.allow_unsigned, "the matched peer's own rule applies");
    }

    #[test]
    fn once_keys_are_pinned_an_anonymous_bundle_is_refused_not_waved_through() {
        let peers = vec![peer_with(Some(&hex::encode([1u8; 32])), false)];
        let trust = trust_for(&peers, None);
        assert!(
            !trust.allow_unsigned,
            "an operator who pinned keys has said anonymous bundles are unwelcome"
        );
        // An unknown node id is the same case.
        assert!(!trust_for(&peers, Some("deadbeef")).allow_unsigned);
    }

    #[test]
    fn one_peer_opting_into_unsigned_keeps_legacy_imports_working() {
        let peers = vec![
            peer_with(Some(&hex::encode([1u8; 32])), false),
            peer_with(None, true),
        ];
        assert!(trust_for(&peers, Some("legacy-hash-id")).allow_unsigned);
    }

    #[test]
    fn mesh_series_are_emitted_at_zero_not_omitted() {
        let lines = mesh_metrics_lines(&MeshTotals::default());
        for name in [
            "pumper_mesh_peers",
            "pumper_mesh_streams",
            "pumper_mesh_pulls_total",
            "pumper_mesh_signature_failures_total",
            "pumper_mesh_ghosts_removed_total",
        ] {
            assert!(
                lines.contains(&format!("{name} 0\n")),
                "{name} must be present at zero on a node with no peers:\n{lines}"
            );
            assert!(lines.contains(&format!("# TYPE {name} ")));
        }
    }

    #[test]
    fn keys_and_schedule_ids_are_stable_and_stream_scoped() {
        assert_eq!(mesh_state_key("vps", "weather"), "vps|weather");
        assert_eq!(peer_schedule_id("vps", "weather"), "peer-vps-weather");
        assert_ne!(
            peer_schedule_id("vps", "weather"),
            peer_schedule_id("vps", "recipes"),
            "one peer's streams must not share a schedule row"
        );
    }
}
