//! Two-node dataset peering, end to end.
//!
//! Every other peer test is a pure-function test. This one runs the app: a REAL
//! origin pumper serving its revision feed over a real socket, and a REAL mirror
//! pumper — separate `AppState`, separate SQLite file, separate temp dir —
//! running the `peer` app against it through the live HTTP engine. Nothing here
//! is stubbed between the two nodes except the clock.
//!
//! What it proves: the initial pull lands under the namespace with mirrored
//! provenance, an incremental run picks up only what is new, a budget cap
//! suspends and a later run resumes the same walk to completion, an origin
//! tombstone becomes a mirror tombstone, a same-stamp revision at the resume
//! boundary is not lost, a corrupt cursor fails the run loudly instead of
//! silently walking from the top, and a watch on the mirror namespace fires.
//!
//! N16 adds the mesh proofs to the same two nodes: a bundle signed by the wrong
//! key is REFUSED (and counted on the mirror's status record), an unverifiable
//! one is refused unless explicitly allowed, and a HARD delete on the origin —
//! which emits no revision at all, so no walk can ever find it — is reconciled
//! away by the live-set digest pass.
//!
//! What it does NOT prove — see `docs/features/mesh.md` § Known gaps:
//! two nodes in one process share a clock and a loopback interface, so clock
//! skew between origin and mirror, network partitions mid-walk, and any
//! authentication story are all out of reach here.

use std::collections::HashMap;
use std::sync::Arc;

use pumper_core::config::Config;
use pumper_core::datasets::Provenance;
use pumper_core::testing::{engines_with, Dead, TempStore};
use pumper_core::{
    EnqueueOptions, Governor, HttpCache, Job, JobStatus, NoPlugins, NoSearch, Revision, ScrapeApp,
};
use pumper_engine_http::HttpEngine;
use serde_json::{json, Value};

use super::harness::{test_state, FakeApp, TestReceiver};
use crate::state::{AppState, AppStateParts};
use crate::{routes, worker};

/// The origin's app/dataset, and the namespace the mirror writes them under
/// (`peer_{remote app}` is the peer app's default).
const ORIGIN_APP: &str = "fake";
const DATASET: &str = "d";
const SPEC: &str = "fake/d";
const NAMESPACE: &str = "peer_fake";

// ── the two nodes ───────────────────────────────────────────────────────────

/// A real origin pumper with its router bound to an ephemeral loopback port.
/// Returns its state, its `TempStore` (KEEP IT BOUND — dropping it deletes the
/// database mid-test) and the base URL a peer job should be pointed at.
async fn origin_node() -> (AppState, TempStore, String) {
    let (state, store) = test_state(vec![Arc::new(FakeApp)]).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let router = routes::router(state.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (state, store, format!("http://{addr}"))
}

/// A real mirror pumper: its own store, and an engine set carrying a LIVE
/// `HttpEngine` so the pull is genuine HTTP over a socket rather than a stub.
///
/// Browser and Claude are pinned to `Dead`, which panics if reached. The peer
/// app calls `ctx.engines.http` directly and never the tiered fetcher, so those
/// tiers are structurally unreachable — the `Dead` pins turn "structurally"
/// into "provably", and keep the test hermetic (no Chrome launch, no
/// subprocess) if that ever changes.
async fn mirror_node() -> (AppState, TempStore) {
    let store = TempStore::new("peer-mirror-e2e").await;
    let mut config = Config::default();
    config.storage.database_path = store.path().join("pumper.db");
    config.storage.artifacts_dir = store.path().join("artifacts");
    config.fetcher.profiles_dir = store.path().join("profiles");
    // Politeness towards a loopback fixture is not what this test proves, and
    // the default 2 rps + 250 ms jitter would dominate a multi-page walk.
    config.governor.enabled = false;
    let governor = Arc::new(Governor::new(&config.governor));
    let cache = Arc::new(HttpCache::new(store.storage.pool(), &config.cache));
    let http = Arc::new(
        HttpEngine::new(
            &config.http,
            governor.clone(),
            cache,
            config.fetcher.profiles_dir.clone(),
        )
        .expect("build the mirror's HTTP engine"),
    );
    let registry: HashMap<String, Arc<dyn ScrapeApp>> = HashMap::from([(
        "peer".to_string(),
        Arc::new(app_peer::Peer) as Arc<dyn ScrapeApp>,
    )]);
    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor,
        engines: engines_with(http, Arc::new(Dead), Arc::new(Dead)),
        plugins: Arc::new(NoPlugins),
        search: Arc::new(NoSearch),
        registry,
    })
    .expect("assemble mirror state");
    (state, store)
}

// ── driving ─────────────────────────────────────────────────────────────────

/// Writes `keys` as the origin's COMPLETE snapshot of the dataset through a real
/// job (`FakeApp` → `ctx.sync_many`), so keys absent from `keys` are tombstoned
/// exactly as a real full-snapshot syncer would tombstone them.
async fn origin_sync(origin: &AppState, keys: &[(&str, Value)]) {
    let items: Vec<Value> = keys
        .iter()
        .map(|(k, d)| json!({ "key": k, "data": d }))
        .collect();
    origin
        .storage
        .enqueue(
            ORIGIN_APP,
            EnqueueOptions {
                params: json!({ "dataset": DATASET, "sync": items }),
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue origin sync");
    assert!(
        worker::run_one(origin).await,
        "the origin's sync job must be claimed and run"
    );
}

/// Runs ONE peer pull on the mirror and returns the finished job row.
///
/// `worker::run_one` drains `fanout` then `deliveries` before returning, so on
/// return every downstream effect of the pull — search indexing, watches,
/// dataset triggers, webhook deliveries — has already completed. That is the
/// synchronization point; nothing below needs to poll.
async fn pull(mirror: &AppState, base: &str, extra: Value) -> Job {
    let mut params = json!({ "url": base, "datasets": [SPEC] });
    if let (Some(p), Some(e)) = (params.as_object_mut(), extra.as_object()) {
        for (k, v) in e {
            p.insert(k.clone(), v.clone());
        }
    }
    let job = mirror
        .storage
        .enqueue(
            "peer",
            EnqueueOptions {
                params,
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue peer job");
    assert!(
        worker::run_one(mirror).await,
        "the queued peer job must be claimed and run"
    );
    mirror
        .storage
        .get(job.id)
        .await
        .expect("read the peer job back")
        .expect("the peer job row exists")
}

/// The single dataset report of a peer run's result.
fn report(job: &Job) -> Value {
    job.result
        .as_ref()
        .and_then(|r| r.get("datasets"))
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .cloned()
        .unwrap_or_else(|| panic!("peer result carries a dataset report: {:?}", job.result))
}

/// Live (non-tombstoned) mirrored keys, sorted.
async fn mirrored_keys(mirror: &AppState) -> Vec<String> {
    let mut keys: Vec<String> = mirror
        .datasets
        .list(NAMESPACE, DATASET, 1000)
        .await
        .expect("list mirrored records")
        .into_iter()
        .filter(|r| r.removed_at.is_none())
        .map(|r| r.key)
        .collect();
    keys.sort();
    keys
}

async fn origin_revisions(origin: &AppState) -> Vec<Revision> {
    origin
        .datasets
        .changes_since(ORIGIN_APP, Some(DATASET), None, 1000, None)
        .await
        .expect("read the origin's own feed")
}

// ── the proofs ──────────────────────────────────────────────────────────────

/// The whole point of the app, over a real socket: records land under the peer
/// NAMESPACE (never the origin's app name), carrying the provenance
/// `mirror_provenance` promises — the LOCAL pulling job, the ORIGIN's
/// `source_url`/`rules_hash` verbatim, and NO `artifact_sha`.
#[tokio::test]
async fn initial_pull_lands_under_the_namespace_with_mirrored_provenance() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    // Seeded directly so the origin's revision carries a FULL derivation stamp —
    // this is what a real scraping origin publishes, and it is exactly the four
    // fields `mirror_provenance` reads off the wire.
    origin
        .datasets
        .upsert_stamped(
            ORIGIN_APP,
            DATASET,
            "k1",
            &json!({ "v": 1 }),
            None,
            Some(&Provenance {
                job_id: Some("origin-job-uuid".into()),
                source_url: Some("https://origin.example/item/1".into()),
                artifact_sha: Some("deadbeef".into()),
                rules_hash: Some("cafebabe".into()),
            }),
        )
        .await
        .expect("seed the origin");

    let job = pull(&mirror, &base, json!({})).await;
    assert_eq!(job.status, JobStatus::Succeeded, "result: {:?}", job.result);
    let rep = report(&job);
    assert_eq!(rep["status"], "ok", "report: {rep}");
    assert_eq!(rep["namespace"], NAMESPACE);
    assert_eq!(rep["new"], 1);

    // The record is under the NAMESPACE, and nothing was written under the
    // origin's own app name (the write-origin corruption the design forbids).
    assert_eq!(mirrored_keys(&mirror).await, vec!["k1".to_string()]);
    assert!(mirror
        .datasets
        .list(ORIGIN_APP, DATASET, 10)
        .await
        .expect("list")
        .is_empty());

    let mirrored = mirror
        .datasets
        .changes_since(NAMESPACE, Some(DATASET), None, 10, None)
        .await
        .expect("read the mirror's own feed");
    let prov = &mirrored[0].provenance;
    assert_eq!(
        prov.job_id.as_deref(),
        Some(job.id.to_string().as_str()),
        "the producing job is THIS pull, not the origin's job"
    );
    assert_eq!(
        prov.source_url.as_deref(),
        Some("https://origin.example/item/1"),
        "the origin's source_url is carried through, never the peer's feed URL"
    );
    assert_eq!(prov.rules_hash.as_deref(), Some("cafebabe"));
    assert!(
        prov.artifact_sha.is_none() && !prov.replayable(),
        "this node holds no archived body, so mirroring the sha would mark a \
         record replayable that cannot be re-derived here"
    );
    assert_eq!(
        rep["origin_artifact_sha_dropped"], 1,
        "and the drop is reported rather than silent"
    );
}

/// A second run must transfer only what changed since the first — that is the
/// entire value of a resume point.
#[tokio::test]
async fn an_incremental_run_pulls_only_what_is_new() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(&origin, &[("a", json!({"v": 1})), ("b", json!({"v": 1}))]).await;
    let first = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(first["new"], 2);
    assert_eq!(first["walk_completed"], true);

    // Nothing changed upstream: the run is a no-op, not a re-pull.
    let idle = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(idle["new"], 0);
    assert_eq!(idle["changed"], 0);

    origin_sync(
        &origin,
        &[
            ("a", json!({"v": 1})),
            ("b", json!({"v": 2})),
            ("c", json!({"v": 1})),
        ],
    )
    .await;
    let third = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(third["new"], 1, "only c is new");
    assert_eq!(third["changed"], 1, "only b changed");
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["a".to_string(), "b".into(), "c".into()]
    );
}

/// `max_records` is a per-run BUDGET, never a data-loss mechanism: a capped run
/// suspends mid-walk and a later run resumes the same walk to completion.
#[tokio::test]
async fn a_capped_walk_suspends_and_a_later_run_resumes_it_to_completion() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    let seed: Vec<(&str, Value)> = vec![
        ("k1", json!({"v": 1})),
        ("k2", json!({"v": 1})),
        ("k3", json!({"v": 1})),
        ("k4", json!({"v": 1})),
        ("k5", json!({"v": 1})),
    ];
    origin_sync(&origin, &seed).await;

    let mut runs = 0;
    loop {
        runs += 1;
        assert!(
            runs <= 5,
            "a 5-record feed must finish within 5 capped runs"
        );
        let rep = report(&pull(&mirror, &base, json!({ "max_records": 2 })).await);
        assert_eq!(rep["status"], "ok", "report: {rep}");
        if rep["walk_completed"] == json!(true) {
            break;
        }
        assert_eq!(rep["capped"], true, "an unfinished walk reports capped");
    }
    assert!(
        runs > 1,
        "a budget of 2 over 5 records must actually suspend"
    );
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec![
            "k1".to_string(),
            "k2".into(),
            "k3".into(),
            "k4".into(),
            "k5".into()
        ],
        "every record arrives across the resumed walk — the budget cost time, not data"
    );
}

/// A removal on the origin must become a real removal on the mirror: the feed
/// carries `removed` revisions and the peer applies them as local tombstones.
#[tokio::test]
async fn an_origin_tombstone_propagates_to_a_mirror_tombstone() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(
        &origin,
        &[("keep", json!({"v": 1})), ("doomed", json!({"v": 1}))],
    )
    .await;
    pull(&mirror, &base, json!({})).await;
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["doomed".to_string(), "keep".into()]
    );

    // A full snapshot that no longer contains `doomed` tombstones it upstream.
    origin_sync(&origin, &[("keep", json!({"v": 1}))]).await;
    let rep = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(rep["tombstones_applied"], 1, "report: {rep}");
    assert_eq!(rep["tombstones_deferred"], 0);
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["keep".to_string()],
        "the mirror stopped serving a record the origin deleted"
    );
    // Not merely absent — a real tombstone, so the mirror's OWN feed carries the
    // removal for anything downstream of it.
    let removed = mirror
        .datasets
        .changes_since(NAMESPACE, Some(DATASET), None, 50, None)
        .await
        .expect("mirror feed")
        .into_iter()
        .filter(|r| r.change == "removed")
        .map(|r| r.key)
        .collect::<Vec<_>>();
    assert_eq!(removed, vec!["doomed".to_string()]);
}

/// The refusal path, end to end: tombstones that would empty the mirror are
/// HELD in `pending_tombstones` and applied by a later run once the origin has
/// live data again. The refusal used to add a note while `since` advanced past
/// the removals — no run ever saw them again, so the mirror kept serving
/// records the origin had deleted, forever, with `status:"ok"`.
#[tokio::test]
async fn refused_tombstones_are_retried_until_they_can_apply() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(&origin, &[("a", json!({"v": 1})), ("b", json!({"v": 1}))]).await;
    pull(&mirror, &base, json!({})).await;
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["a".to_string(), "b".into()]
    );

    // The origin removes EVERYTHING — by name, the one path that can genuinely
    // empty a feed (a full-snapshot sync refuses an empty batch outright).
    origin
        .datasets
        .tombstone_keys(ORIGIN_APP, DATASET, &["a".to_string(), "b".to_string()])
        .await
        .expect("tombstone the whole origin dataset");

    let refusing = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(refusing["tombstones_applied"], 0, "report: {refusing}");
    assert_eq!(
        refusing["tombstones_deferred"], 2,
        "refused, but HELD for retry — not dropped: {refusing}"
    );
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["a".to_string(), "b".into()],
        "the mirror refuses to empty itself on a feed that removes everything"
    );

    // The origin recovers with a live record. This run's window carries no
    // `removed` revisions at all — the walk advanced past them last run — so
    // ONLY the deferred backlog in `PeerState` can still deliver these removals.
    origin_sync(&origin, &[("c", json!({"v": 1}))]).await;
    let recovered = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(
        recovered["tombstones_applied"], 2,
        "the deferred removals applied once they no longer empty the mirror: {recovered}"
    );
    assert_eq!(recovered["tombstones_deferred"], 0);
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["c".to_string()],
        "converged: the mirror matches the origin again"
    );
}

/// The loss window `inclusive_since` closes, at the level where it actually
/// bites. A revision stamped EXACTLY at the mirror's resume point but committed
/// after that point was recorded is excluded forever by the origin's strict
/// `created_at > since` — the mirror would never see it again on any run.
///
/// The shared stamp is forced rather than raced (`set_revision_created_at_for_test`),
/// because the natural version of this bug needs two writers hitting one
/// microsecond.
#[tokio::test]
async fn equal_stamp_revisions_not_lost_across_runs() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(&origin, &[("first", json!({"v": 1}))]).await;
    let first = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(first["new"], 1);
    let boundary = origin_revisions(&origin).await[0].created_at;

    // A late arrival that shares the resume point's exact stamp — the chunk-mate
    // that was committed just after the mirror's page was served.
    origin_sync(
        &origin,
        &[("first", json!({"v": 1})), ("late", json!({"v": 1}))],
    )
    .await;
    origin
        .datasets
        .set_revision_created_at_for_test(ORIGIN_APP, DATASET, "late", 1, boundary)
        .await
        .expect("backdate the late revision onto the boundary stamp");

    let second = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(
        second["new"], 1,
        "a revision stamped at the resume point must still be delivered — with an \
         exclusive boundary it is silently lost forever: {second}"
    );
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["first".to_string(), "late".into()]
    );
}

/// A corrupt resume cursor must fail the run, loudly. Silently restarting at the
/// newest revision is not a reset for a mirror — it is a livelock: every page
/// re-dedupes against the applied-key set, the budget burns, the walk
/// re-suspends near the top, and the run still says `ok`.
#[tokio::test]
async fn a_corrupt_cursor_fails_the_run_instead_of_walking_from_the_top() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(
        &origin,
        &[
            ("k1", json!({"v": 1})),
            ("k2", json!({"v": 1})),
            ("k3", json!({"v": 1})),
        ],
    )
    .await;
    // Suspend a walk so there is a stored cursor to corrupt.
    let capped = report(&pull(&mirror, &base, json!({ "max_records": 1 })).await);
    assert_eq!(capped["capped"], true, "report: {capped}");

    let state_records = mirror
        .datasets
        .list("peer", "state", 10)
        .await
        .expect("peer state");
    let rec = state_records.first().expect("one peer/state record");
    let mut poisoned = rec.data.clone();
    poisoned["walk"]["next_cursor"] = json!("not-a-cursor");
    mirror
        .datasets
        .upsert_trusted("peer", "state", &rec.key, &poisoned, None)
        .await
        .expect("poison the stored cursor");

    let job = pull(&mirror, &base, json!({ "max_records": 1 })).await;
    assert_eq!(
        job.status,
        JobStatus::Failed,
        "every dataset errored, so the JOB fails — a green run here is the bug: {:?}",
        job.result
    );
    let err = job.error.unwrap_or_default();
    assert!(
        err.contains("400"),
        "the failure names the origin's 400 rather than some generic parse error: {err}"
    );
}

/// Mirror visibility, proved across the two nodes: a watch on the mirror's
/// NAMESPACE fires when the origin's change arrives. Before the run batch was
/// widened past `job.app`, this watch could never fire — the writes landed under
/// `peer_fake` while the batch was scoped to `peer`.
#[tokio::test]
async fn a_watch_on_the_mirror_namespace_fires_on_a_pull() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;
    let rx = TestReceiver::spawn(vec![]).await;

    mirror
        .storage
        .create_watch(NAMESPACE, DATASET, &rx.url(), Some("s3cr3t"), "webhook")
        .await
        .expect("watch the mirror namespace");

    origin_sync(&origin, &[("k1", json!({"v": 1})), ("k2", json!({"v": 1}))]).await;
    let job = pull(&mirror, &base, json!({})).await;
    assert_eq!(job.status, JobStatus::Succeeded);

    // `pull` returned only after `run_one` drained fanout THEN deliveries, so
    // this is a fact, not a deadline.
    let hits = rx.hits_so_far();
    assert_eq!(
        hits.len(),
        1,
        "exactly one dataset.changed for one run — not zero (invisible mirror) \
         and not one per namespace of the batch"
    );
    let (headers, body) = &hits[0];
    assert_eq!(headers["x-pumper-event"], "dataset.changed");
    let payload: Value = serde_json::from_slice(body).expect("payload is JSON");
    assert_eq!(
        payload["app"], NAMESPACE,
        "the payload names the namespace the records actually live under, which \
         is the only app they can be read back from — not the job's app"
    );
    assert_eq!(payload["dataset"], DATASET);
    assert_eq!(payload["count"], 2);
}

// ── the mesh (N16) ──────────────────────────────────────────────────────────

/// Runs one `peer` job with arbitrary params (no dataset defaults) and returns
/// the finished row — the bundle streams take no `datasets` list.
async fn run_peer(mirror: &AppState, params: Value) -> Job {
    let job = mirror
        .storage
        .enqueue(
            "peer",
            EnqueueOptions {
                params,
                max_attempts: 1,
                ..Default::default()
            },
        )
        .await
        .expect("enqueue peer job");
    assert!(
        worker::run_one(mirror).await,
        "the queued peer job must be claimed and run"
    );
    mirror
        .storage
        .get(job.id)
        .await
        .expect("read the peer job back")
        .expect("the peer job row exists")
}

/// The mirror's `peer/mesh` status record for one (peer label, stream).
async fn mesh_status(mirror: &AppState, label: &str, stream: &str) -> Value {
    mirror
        .datasets
        .get("peer", "mesh", &format!("{label}|{stream}"))
        .await
        .expect("read the mesh status record")
        .map(|r| r.data)
        .unwrap_or(Value::Null)
}

/// The whole point of signing, over a real socket: a bundle that did not come
/// from the key this mirror pinned is REFUSED, the job fails, and the refusal
/// is countable on the mirror's own status page rather than only in a log line.
///
/// The two nodes hold genuinely different keypairs (identity is memoised per
/// key-file path, and each node has its own temp dir), so "the wrong key" here
/// is a real other key, not a fixture constant.
#[tokio::test]
async fn a_bundle_signed_by_the_wrong_key_is_refused_and_counted() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    // Teach the origin something worth exporting: 3 losses pin the host.
    for _ in 0..3 {
        origin
            .tiers
            .record("pinned.example", "browser", true)
            .await
            .expect("record a tier outcome");
    }

    let origin_key = crate::node::identity(&origin)
        .expect("origin identity")
        .public_key_hex();
    let impostor_key = crate::node::identity(&mirror)
        .expect("mirror identity")
        .public_key_hex();
    assert_ne!(
        origin_key, impostor_key,
        "two nodes in one process must not share a keypair, or this test proves nothing"
    );

    // Pinned to the WRONG key: the bundle is real, the key is not the one this
    // peer row trusts, so it must not be applied.
    let job = run_peer(
        &mirror,
        json!({
            "url": base,
            "stream": "weather",
            "peer_name": "origin",
            "public_key": impostor_key,
            "allow_unsigned": false,
        }),
    )
    .await;
    assert_eq!(
        job.status,
        JobStatus::Failed,
        "a refused bundle must fail the job, not degrade it: {:?}",
        job.result
    );
    assert!(
        job.error.as_deref().unwrap_or_default().contains("REFUSED"),
        "the failure must name the refusal: {:?}",
        job.error
    );
    assert!(
        mirror
            .tiers
            .get("pinned.example")
            .await
            .expect("read tier memory")
            .is_none(),
        "nothing from an unverified bundle may reach tier memory"
    );
    let status = mesh_status(&mirror, "origin", "weather").await;
    assert_eq!(status["ok"], false);
    assert_eq!(
        status["signature_failures"], 1,
        "the refusal is countable on GET /mesh: {status}"
    );
    assert!(
        status["last_success_at"].is_null(),
        "a peer that never succeeded must not report a success time"
    );

    // The same pull with the RIGHT key verifies and applies.
    let job = run_peer(
        &mirror,
        json!({
            "url": base,
            "stream": "weather",
            "peer_name": "origin",
            "public_key": origin_key,
            "allow_unsigned": false,
        }),
    )
    .await;
    assert_eq!(job.status, JobStatus::Succeeded, "result: {:?}", job.result);
    let result = job.result.expect("weather result");
    assert_eq!(result["verified"], true);
    assert_eq!(result["changed"], 1, "result: {result}");
    let adopted = mirror
        .tiers
        .get("pinned.example")
        .await
        .expect("read tier memory")
        .expect("the verified bundle's pin was adopted");
    assert_eq!(adopted.preferred_tier.as_deref(), Some("browser"));
    assert_eq!(
        adopted.observations, 0,
        "an import never fabricates local evidence"
    );

    let status = mesh_status(&mirror, "origin", "weather").await;
    assert_eq!(status["ok"], true);
    assert_eq!(status["verified"], true);
    assert_eq!(status["pulls"], 2, "counters accumulate across runs");
    assert_eq!(
        status["signature_failures"], 1,
        "the earlier refusal is not erased by a later success"
    );
    assert!(status["last_success_at"].is_string());
}

/// An UNVERIFIABLE bundle is refused by default. The trap this closes: a
/// signature nobody can check reading as "signed, therefore fine".
#[tokio::test]
async fn a_bundle_nobody_can_verify_is_refused_unless_explicitly_allowed() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;
    for _ in 0..3 {
        origin
            .tiers
            .record("pinned.example", "browser", true)
            .await
            .expect("record a tier outcome");
    }

    // No key pinned, allow_unsigned absent → the safe default refuses.
    let job = run_peer(
        &mirror,
        json!({ "url": base, "stream": "weather", "peer_name": "origin" }),
    )
    .await;
    assert_eq!(job.status, JobStatus::Failed, "result: {:?}", job.result);

    // Explicit opt-in accepts it — and says `verified: false` about it.
    let job = run_peer(
        &mirror,
        json!({
            "url": base, "stream": "weather", "peer_name": "origin",
            "allow_unsigned": true,
        }),
    )
    .await;
    assert_eq!(job.status, JobStatus::Succeeded, "result: {:?}", job.result);
    assert_eq!(
        job.result.expect("result")["verified"],
        false,
        "an unverifiable bundle must never claim to be verified"
    );
}

/// The hard-delete gap, closed. An outright `DELETE` on the origin emits NO
/// revision, so no walk of the change feed can ever learn of it — before the
/// reconcile pass the mirror served that record forever, with a green run.
#[tokio::test]
async fn a_hard_delete_on_the_origin_is_reconciled_away_on_the_mirror() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(
        &origin,
        &[("keep", json!({"v": 1})), ("vanished", json!({"v": 1}))],
    )
    .await;
    pull(&mirror, &base, json!({ "peer_name": "origin" })).await;
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["keep".to_string(), "vanished".into()]
    );

    // The hard delete: the row is gone from the origin outright.
    assert!(
        origin
            .datasets
            .delete_record(ORIGIN_APP, DATASET, "vanished")
            .await
            .expect("hard-delete on the origin"),
        "the record existed before it was deleted"
    );
    let feed_after = origin_revisions(&origin).await;
    assert!(
        !feed_after
            .iter()
            .any(|r| r.key == "vanished" && r.change == "removed"),
        "the premise: a hard delete leaves NO removed revision for a puller to find"
    );

    // A plain walk therefore changes nothing — the ghost survives it.
    let rep = report(
        &pull(
            &mirror,
            &base,
            json!({ "reconcile": false, "peer_name": "origin" }),
        )
        .await,
    );
    assert_eq!(rep["tombstones_applied"], 0);
    assert!(
        rep.get("reconcile").is_none(),
        "reconcile: false must not run the pass at all"
    );
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["keep".to_string(), "vanished".into()],
        "without the reconcile the mirror keeps serving a record the origin deleted"
    );

    // With the reconcile on, the digest mismatch is found and the ghost dies.
    let rep = report(&pull(&mirror, &base, json!({ "peer_name": "origin" })).await);
    let rec = &rep["reconcile"];
    assert_eq!(rec["reconciled"], true, "report: {rep}");
    assert_eq!(rec["in_sync"], false);
    assert_ne!(rec["local_digest"], rec["origin_digest"]);
    assert_eq!(rec["ghosts_removed"], 1);
    assert_eq!(rec["ghost_keys"], json!(["vanished"]));
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["keep".to_string()],
        "the ghost is gone"
    );
    // A real tombstone, so the mirror's own feed carries the removal downstream.
    let removed: Vec<String> = mirror
        .datasets
        .changes_since(NAMESPACE, Some(DATASET), None, 50, None)
        .await
        .expect("mirror feed")
        .into_iter()
        .filter(|r| r.change == "removed")
        .map(|r| r.key)
        .collect();
    assert_eq!(removed, vec!["vanished".to_string()]);

    // And the pass is idempotent: once converged it removes nothing more.
    let rep = report(&pull(&mirror, &base, json!({ "peer_name": "origin" })).await);
    assert_eq!(rep["reconcile"]["in_sync"], true, "report: {rep}");
    assert_eq!(rep["reconcile"]["ghosts_removed"], 0);
    let status = mesh_status(&mirror, "origin", "datasets-fake-d").await;
    assert_eq!(
        status["ghosts_removed"], 1,
        "the reconcile's work is countable on GET /mesh: {status}"
    );
}

/// A converged mirror must not be told it has ghosts, and the pass must not
/// consult the origin's key list when the digests already agree.
#[tokio::test]
async fn a_converged_mirror_reports_in_sync_and_removes_nothing() {
    let (origin, _origin_store, base) = origin_node().await;
    let (mirror, _mirror_store) = mirror_node().await;

    origin_sync(&origin, &[("a", json!({"v": 1})), ("b", json!({"v": 1}))]).await;
    let rep = report(&pull(&mirror, &base, json!({})).await);
    assert_eq!(rep["reconcile"]["in_sync"], true, "report: {rep}");
    assert_eq!(
        rep["reconcile"]["local_digest"],
        rep["reconcile"]["origin_digest"]
    );
    assert_eq!(rep["reconcile"]["ghosts_removed"], 0);
    assert_eq!(
        mirrored_keys(&mirror).await,
        vec!["a".to_string(), "b".into()]
    );
}
