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
//! What it does NOT prove — see `docs/features/peering.md` § Known gaps:
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
