//! The server test harness: a headless `AppState` over a temp store with fake
//! engines, a scriptable `FakeApp`, and a loopback `TestReceiver` for outbound
//! webhooks.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pumper_core::config::{Config, GovernorConfig};
use pumper_core::testing::{dead_engines, TempStore};
use pumper_core::{AppContext, Error, Governor, NoPlugins, NoSearch, Result, ScrapeApp};
use serde_json::{json, Value};

use crate::state::{AppState, AppStateParts};

/// A headless `AppState`: temp SQLite, Dead engines, NoSearch/NoPlugins, only
/// the supplied apps registered, and worker knobs tuned for fast tests. The
/// returned `TempStore` must be kept alive for the state's lifetime.
pub async fn test_state(apps: Vec<Arc<dyn ScrapeApp>>) -> (AppState, TempStore) {
    test_state_with(apps, |_| {}).await
}

/// [`test_state`] with a config hook applied after the standard test tuning —
/// for tests exercising config-gated surfaces (e.g. `[plugins] app_dir`).
pub async fn test_state_with(
    apps: Vec<Arc<dyn ScrapeApp>>,
    tweak: impl FnOnce(&mut Config),
) -> (AppState, TempStore) {
    test_state_indexed(apps, Arc::new(NoSearch), tweak).await
}

/// [`test_state_with`] with the search index supplied by the caller — the seam
/// for a *slow* index fixture, which is the only way to measure what the
/// post-completion fan-out costs a worker slot.
pub async fn test_state_indexed(
    apps: Vec<Arc<dyn ScrapeApp>>,
    search: Arc<dyn pumper_core::Search>,
    tweak: impl FnOnce(&mut Config),
) -> (AppState, TempStore) {
    test_state_parts(apps, search, Arc::new(NoPlugins), dead_engines(), tweak).await
}

/// [`test_state`] with a REAL plugin host — the seam the trigger-hook tests
/// need, since `NoPlugins` makes every hook take the unknown-plugin path and so
/// can never exercise a predicate veto or a transform.
pub async fn test_state_with_plugins(
    apps: Vec<Arc<dyn ScrapeApp>>,
    plugins: Arc<dyn pumper_core::Plugins>,
) -> (AppState, TempStore) {
    test_state_parts(apps, Arc::new(NoSearch), plugins, dead_engines(), |_| {}).await
}

/// [`test_state`] with the engine set supplied by the caller — the seam for a
/// scripted `HttpClient`, which is how an app-level transport failure is staged
/// without a network.
pub async fn test_state_engines(
    apps: Vec<Arc<dyn ScrapeApp>>,
    engines: Arc<pumper_core::EngineSet>,
) -> (AppState, TempStore) {
    test_state_parts(
        apps,
        Arc::new(NoSearch),
        Arc::new(NoPlugins),
        engines,
        |_| {},
    )
    .await
}

/// The one assembly path; every `test_state*` helper is a preset over it.
async fn test_state_parts(
    apps: Vec<Arc<dyn ScrapeApp>>,
    search: Arc<dyn pumper_core::Search>,
    plugins: Arc<dyn pumper_core::Plugins>,
    engines: Arc<pumper_core::EngineSet>,
    tweak: impl FnOnce(&mut Config),
) -> (AppState, TempStore) {
    let store = TempStore::new("server-e2e").await;
    let mut config = Config::default();
    config.storage.database_path = store.path().join("pumper.db");
    config.storage.artifacts_dir = store.path().join("artifacts");
    config.worker.poll_interval_secs = 1;
    config.worker.shutdown_drain_secs = 1;
    config.worker.job_timeout_secs = 30;
    config.worker.heartbeat_secs = 0;
    tweak(&mut config);

    let mut registry: HashMap<String, Arc<dyn ScrapeApp>> = HashMap::new();
    for app in apps {
        registry.insert(app.name().to_string(), app);
    }

    let state = AppState::from_parts(AppStateParts {
        config,
        storage: Arc::new(store.storage.clone()),
        governor: Arc::new(Governor::new(&GovernorConfig::default())),
        engines,
        plugins,
        search,
        registry,
    })
    .expect("assemble test AppState");
    (state, store)
}

/// The **real** worker loop (`worker::run`) running in the background, with a
/// deterministic shutdown. Used by the lifecycle tests, which must exercise the
/// loop itself — claim/permit handling, per-app caps, the two-phase drain — not
/// just the `run_one` seam.
pub struct WorkerLoop {
    handle: tokio::task::JoinHandle<()>,
}

impl WorkerLoop {
    /// Spawns `worker::run` against this state. Nothing test-only is injected:
    /// this is the same entry point `main` uses.
    pub fn start(state: &AppState) -> Self {
        Self {
            handle: tokio::spawn(crate::worker::run(state.clone())),
        }
    }

    /// Fires the shutdown token and waits for the loop (including its drain) to
    /// exit. Panics if it doesn't, because a loop that won't stop is a bug.
    pub async fn shutdown(self, state: &AppState, timeout: Duration) {
        state.shutdown.cancel();
        tokio::time::timeout(timeout, self.handle)
            .await
            .expect("worker loop must exit within the drain deadline")
            .expect("worker loop task must not panic");
    }
}

/// The **real** scheduler loop (`scheduler::run`) running in the background,
/// with a deterministic shutdown — the mirror of [`WorkerLoop`].
///
/// The tick loop had no harness at all: every scheduler test drove `reconcile`
/// directly, so the loop *around* it (panic containment, the shutdown check
/// between phases, the piggybacked reaper/DLQ/refresher/DataHub calls, the exit)
/// was untested. That loop is the process heartbeat, so "untested" meant a
/// contained-or-not panic, a missed cancellation, or a loop that never exits
/// would all have shipped silently.
pub struct SchedulerLoop {
    handle: tokio::task::JoinHandle<()>,
}

impl SchedulerLoop {
    /// Spawns `scheduler::run` against this state. Nothing test-only is
    /// injected: this is the same entry point `main` uses.
    pub fn start(state: &AppState) -> Self {
        Self {
            handle: tokio::spawn(crate::scheduler::run(state.clone())),
        }
    }

    /// Fires the shutdown token and waits for the loop to exit. Panics if it
    /// doesn't — `main` now JOINS this task, so a loop that won't stop is a
    /// hung shutdown, not a detached background thread nobody waits on.
    pub async fn shutdown(self, state: &AppState, timeout: Duration) {
        state.shutdown.cancel();
        tokio::time::timeout(timeout, self.handle)
            .await
            .expect("scheduler loop must exit within the deadline")
            .expect("scheduler loop task must not panic");
    }
}

/// Polls `check` every 10ms until it returns true, then returns. Panics with
/// `what` on timeout. Condition-driven rather than a fixed sleep, so the tests
/// are neither flaky nor slower than the thing they wait for.
pub async fn wait_for<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until a job reaches `want` and returns the row.
pub async fn wait_status(
    state: &AppState,
    id: uuid::Uuid,
    want: pumper_core::JobStatus,
    timeout: Duration,
) -> pumper_core::Job {
    wait_for(
        &format!("job {id} to reach {}", want.as_str()),
        timeout,
        || async { matches!(state.storage.get(id).await, Ok(Some(j)) if j.status == want) },
    )
    .await;
    state
        .storage
        .get(id)
        .await
        .expect("read job")
        .expect("job row")
}

/// A scriptable `ScrapeApp`, driven entirely by job params:
/// - `{"sleep_ms": N}` — hold the job open (drain/cancel tests)
/// - `{"fail": "msg"}` — return an app error
/// - `{"dataset": "d", "sync": [{"key": "...", "data": {...}}, ...]}` — write
///   records through the real `ctx.sync_many` path
pub struct FakeApp;

#[async_trait::async_trait]
impl ScrapeApp for FakeApp {
    fn name(&self) -> &'static str {
        "fake"
    }
    fn description(&self) -> &'static str {
        "scriptable test app"
    }
    async fn run(&self, ctx: AppContext) -> Result<Value> {
        if let Some(ms) = ctx.params.get("sleep_ms").and_then(Value::as_u64) {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if let Some(msg) = ctx.params.get("fail").and_then(Value::as_str) {
            return Err(Error::App(msg.to_string()));
        }
        let mut synced = 0;
        if let Some(items) = ctx.params.get("sync").and_then(Value::as_array) {
            let dataset = ctx
                .params
                .get("dataset")
                .and_then(Value::as_str)
                .unwrap_or("d")
                .to_string();
            let items: Vec<(String, Value)> = items
                .iter()
                .map(|it| {
                    (
                        it.get("key")
                            .and_then(Value::as_str)
                            .expect("sync item key")
                            .to_string(),
                        it.get("data").cloned().unwrap_or(json!({})),
                    )
                })
                .collect();
            let summary = ctx.sync_many(&dataset, &items).await?;
            synced = summary.new.len() + summary.changed.len();
        }
        Ok(json!({ "ok": true, "synced": synced }))
    }
}

/// One captured webhook delivery: headers (lowercased names) + raw body.
pub type Hit = (HashMap<String, String>, Vec<u8>);

/// A loopback HTTP receiver. Responds with the next scripted status (then 200
/// once the script runs out) and records every request.
pub struct TestReceiver {
    addr: SocketAddr,
    hits: Arc<Mutex<Vec<Hit>>>,
}

impl TestReceiver {
    pub async fn spawn(statuses: Vec<u16>) -> Self {
        Self::spawn_slow(statuses, Duration::ZERO).await
    }

    /// A receiver that holds each request open for `delay` before recording it
    /// and answering — the fixture for "shutdown happened while POSTs were in
    /// flight". The hit is recorded *after* the delay, so a recorded hit proves
    /// the handler ran to completion rather than merely being entered.
    pub async fn spawn_slow(statuses: Vec<u16>, delay: Duration) -> Self {
        let hits: Arc<Mutex<Vec<Hit>>> = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(VecDeque::from(statuses)));

        let hits_h = hits.clone();
        let handler = move |req: axum::extract::Request| {
            let hits = hits_h.clone();
            let script = script.clone();
            async move {
                let headers: HashMap<String, String> = req
                    .headers()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.as_str().to_lowercase(),
                            v.to_str().unwrap_or("").to_string(),
                        )
                    })
                    .collect();
                let body = axum::body::to_bytes(req.into_body(), 1 << 20)
                    .await
                    .unwrap_or_default()
                    .to_vec();
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                hits.lock().unwrap().push((headers, body));
                let status = script.lock().unwrap().pop_front().unwrap_or(200);
                axum::http::StatusCode::from_u16(status).unwrap()
            }
        };
        let app = axum::Router::new().fallback(axum::routing::any(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, hits }
    }

    pub fn url(&self) -> String {
        format!("http://{}/hook", self.addr)
    }

    /// Everything received so far, without waiting — for asserting that a
    /// delivery did NOT happen.
    pub fn hits_so_far(&self) -> Vec<Hit> {
        self.hits.lock().unwrap().clone()
    }

    /// Polls until at least `n` requests arrived, then returns them all.
    ///
    /// Prefer [`TestReceiver::hits_so_far`] after a drain: deliveries run on
    /// `AppState::deliveries`, so `worker::run_one` (or an explicit
    /// `state.deliveries.drain(..)`) is a real synchronization point. This poll
    /// is only for the paths that have no such point — a raw dispatch whose
    /// arrival is genuinely concurrent with the assertion.
    pub async fn wait_hits(&self, n: usize, timeout: Duration) -> Vec<Hit> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            {
                let hits = self.hits.lock().unwrap();
                if hits.len() >= n {
                    return hits.clone();
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {n} webhook deliveries"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}
