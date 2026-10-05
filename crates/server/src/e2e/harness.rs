//! The server test harness: a headless `AppState` over a temp store with fake
//! engines, a scriptable `FakeApp`, and a loopback `TestReceiver` for outbound
//! webhooks.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Once};
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
    // Every state-building test arms the failure footer for the process; the
    // five modules that build no state call arm() themselves, and the census
    // below holds the whole directory to one or the other.
    arm();
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

// ───────────────────────── failure provenance ─────────────────────────────
//
// Two questions decide an engineer's next hour in front of a red
// `cargo test --workspace`: *how do I run just this one?* and *is this already
// known to flake?* Before this section the suite answered neither — no failure
// message in the whole e2e tree named a command, and `pumper-server` is
// binary-only, so the invocation is `-p pumper-server --bin pumper`, which
// nobody guesses (`cargo test --lib` and `--test` both miss it entirely; see
// CLAUDE.md on the same trap for `cargo run`).
//
// So a failure introduces itself. [`arm`] chains a panic hook onto the default
// one that appends a footer naming:
//
//   * the exact `--exact` repro command for the test that panicked;
//   * its status in `.flake/register.json` — quarantined (with owner and
//     expiry), not registered, or **cannot check**, which is not a pass;
//   * for a wall-clock claim made through [`timed_claim`], both measured
//     figures and the achieved-vs-required margin, because the margin is the
//     whole flake-vs-regression decision: `1.97x of a required 2.00x` is a
//     loaded box and `0.4x` is a regression, and a bare `assert!` renders
//     those two identically.
//
// Nothing here ever writes the register: `.flake/README.md`'s first rule is
// that a quarantine row requires a human author.

/// The register id prefix for every test in this suite.
///
/// `pumper-server` is binary-only, so its whole e2e tree compiles into
/// `bin:pumper` — the identity shape `scripts/ci/flake-id.mjs` writes and
/// `.flake/README.md` documents. Calling it `lib` would make every id here
/// match nothing at all, which is the failure mode that is invisible.
pub const REGISTER_TARGET: &str = "pumper-server::bin:pumper";

/// The register, relative to the repo root.
pub const REGISTER_FILE: &str = ".flake/register.json";

/// Where a failing test sits in the quarantine register. Three outcomes, not
/// two — the same 0/2/3 doctrine `scripts/ci/gates.json` and
/// `scripts/docs/check-doc-sync.mjs` are built on: a cannot-check is not a
/// pass, and it must not be able to read like one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterStatus {
    /// A `quarantine` row names this test.
    Quarantined {
        owner: String,
        entered: String,
        expires: String,
        cause: String,
    },
    /// The register was read and holds no row for this test.
    NotRegistered,
    /// The register could not be consulted. Says why, names the file.
    CannotCheck { why: String },
}

/// The full register id for a libtest module path inside this suite.
pub fn register_id(test_path: &str) -> String {
    format!("{REGISTER_TARGET}::{test_path}")
}

/// `<package>::<target>::<fn>` — the reduced key `scripts/ci/flake-id.mjs`'s
/// `registerKey()` computes, so a row written against a different intermediate
/// module path still reconciles. Mirrors that function exactly, including the
/// `< 3 parts` rejection.
pub fn register_key(id: &str) -> Option<String> {
    let parts: Vec<&str> = id.split("::").filter(|p| !p.is_empty()).collect();
    if parts.len() < 3 {
        return None;
    }
    Some(format!(
        "{}::{}::{}",
        parts[0],
        parts[1],
        parts[parts.len() - 1]
    ))
}

/// The repo root, from this crate's manifest directory.
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// The register lookup, over register text already in hand. Pure, so the
/// cannot-check branch is testable without making the real file unreadable.
pub fn status_in_register(register: &str, id: &str) -> RegisterStatus {
    let parsed: Value = match serde_json::from_str(register) {
        Ok(v) => v,
        Err(e) => {
            return RegisterStatus::CannotCheck {
                why: format!("{REGISTER_FILE} did not parse as JSON: {e}"),
            }
        }
    };
    let Some(rows) = parsed.get("quarantine").and_then(Value::as_array) else {
        return RegisterStatus::CannotCheck {
            why: format!("{REGISTER_FILE} has no `quarantine` array"),
        };
    };
    let key = register_key(id);
    for row in rows {
        let row_id = row.get("id").and_then(Value::as_str).unwrap_or_default();
        let same = row_id == id || (key.is_some() && register_key(row_id) == key);
        if !same {
            continue;
        }
        let field = |k: &str| {
            row.get(k)
                .and_then(Value::as_str)
                .unwrap_or("(absent)")
                .to_string()
        };
        return RegisterStatus::Quarantined {
            owner: field("owner"),
            entered: field("entered"),
            expires: field("expires"),
            cause: field("cause"),
        };
    }
    RegisterStatus::NotRegistered
}

/// [`status_in_register`] against the real register on disk.
pub fn register_status(id: &str) -> RegisterStatus {
    let path = repo_root().join(REGISTER_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => status_in_register(&text, id),
        Err(e) => RegisterStatus::CannotCheck {
            why: format!("{} could not be read: {e}", path.display()),
        },
    }
}

/// The one command that reproduces a single e2e test.
pub fn repro_command(test_path: &str) -> String {
    format!("cargo test -p pumper-server --bin pumper -- --exact {test_path} --nocapture")
}

/// A wall-clock claim's shape. Both variants carry the measured figures, which
/// is the point: the predicate alone cannot say how close it came.
#[derive(Debug, Clone, PartialEq)]
pub enum Claim {
    /// `measured_ms` must stay at or under `limit_ms` — a ceiling.
    AtMost { measured_ms: f64, limit_ms: f64 },
    /// `measured_ms` must reach `floor_ms` — a floor. The direction a stage
    /// accounting claim needs: a stage that reports LESS time than the latency
    /// it contains has lost the measurement, not gained speed.
    AtLeast { measured_ms: f64, floor_ms: f64 },
    /// `fast_ms * factor` must stay under `slow_ms` — a speedup claim, the
    /// RATIO form `.lanes/criteria.json`'s `boundPolicy` prefers, because a
    /// ratio survives a machine change and an absolute millisecond does not.
    SpeedupAtLeast {
        fast_ms: f64,
        slow_ms: f64,
        factor: f64,
    },
}

impl Claim {
    /// What the run actually achieved, on the claim's own scale.
    pub fn achieved(&self) -> f64 {
        match self {
            Claim::AtMost {
                measured_ms,
                limit_ms,
            } => measured_ms / limit_ms.max(f64::MIN_POSITIVE),
            Claim::AtLeast {
                measured_ms,
                floor_ms,
            } => measured_ms / floor_ms.max(f64::MIN_POSITIVE),
            Claim::SpeedupAtLeast {
                fast_ms, slow_ms, ..
            } => slow_ms / fast_ms.max(f64::MIN_POSITIVE),
        }
    }

    /// What the bound demanded, on the same scale.
    pub fn required(&self) -> f64 {
        match self {
            Claim::AtMost { .. } | Claim::AtLeast { .. } => 1.0,
            Claim::SpeedupAtLeast { factor, .. } => *factor,
        }
    }

    /// Whether the claim holds. The ONLY predicate an e2e wall-clock
    /// assertion may use, so that no site can assert one thing and report
    /// another.
    pub fn holds(&self) -> bool {
        match self {
            Claim::AtMost {
                measured_ms,
                limit_ms,
            } => measured_ms <= limit_ms,
            Claim::AtLeast {
                measured_ms,
                floor_ms,
            } => measured_ms >= floor_ms,
            Claim::SpeedupAtLeast {
                fast_ms,
                slow_ms,
                factor,
            } => fast_ms * factor < *slow_ms,
        }
    }

    /// The two measured figures, in the claim's own words.
    pub fn figures(&self) -> String {
        match self {
            Claim::AtMost {
                measured_ms,
                limit_ms,
            } => format!("measured {measured_ms:.0}ms against a {limit_ms:.0}ms ceiling"),
            Claim::AtLeast {
                measured_ms,
                floor_ms,
            } => format!("measured {measured_ms:.0}ms against a {floor_ms:.0}ms floor"),
            Claim::SpeedupAtLeast {
                fast_ms, slow_ms, ..
            } => format!("fast arm {fast_ms:.0}ms vs slow arm {slow_ms:.0}ms"),
        }
    }

    /// `1.13x achieved, 2.00x required` — the one line that decides
    /// flake-vs-regression, plus which of the two it reads like.
    pub fn margin(&self) -> String {
        let (achieved, required) = (self.achieved(), self.required());
        let direction = match self {
            Claim::AtMost { .. } => "of the ceiling used, 1.00x is the bound",
            Claim::AtLeast { .. } => "of the floor reached, 1.00x is the bound",
            Claim::SpeedupAtLeast { .. } => "achieved",
        };
        let reading = if self.holds() {
            "holds"
        } else if near_miss(achieved, required) {
            "MISSED BY A HAIR — within 25% of the bound, which reads as a loaded box \
             before it reads as a regression"
        } else {
            "MISSED WIDE — nowhere near the bound, so read this as a regression, not load"
        };
        match self {
            Claim::AtMost { .. } | Claim::AtLeast { .. } => format!(
                "{achieved:.2}x {direction} ({}) — {reading}",
                self.figures()
            ),
            Claim::SpeedupAtLeast { .. } => format!(
                "{achieved:.2}x {direction}, {required:.2}x required ({}) — {reading}",
                self.figures()
            ),
        }
    }
}

/// Within 25% of the bound, on the claim's own scale. Extracted so the
/// threshold is one named thing rather than a literal inside a format string.
pub fn near_miss(achieved: f64, required: f64) -> bool {
    let (lo, hi) = if achieved <= required {
        (achieved, required)
    } else {
        (required, achieved)
    };
    hi <= lo * 1.25
}

/// A claim made during this test, kept for the footer.
#[derive(Debug, Clone, PartialEq)]
pub struct TimedClaim {
    pub name: String,
    pub claim: Claim,
    pub held: bool,
}

thread_local! {
    /// Claims made on THIS test's thread. libtest gives each test its own
    /// thread, which is also what names it, so a thread-local is exactly the
    /// right scope — no cross-test bleed even under the default parallel run.
    static CLAIMS: RefCell<Vec<TimedClaim>> = const { RefCell::new(Vec::new()) };
}

/// Records a claim and returns whether it held, WITHOUT asserting — the form a
/// per-case budget counter needs (`trigger_hook_stages` counts over-budget
/// cases and asserts the count), so that even a counted breach still carries
/// its figures into the footer.
pub fn timed_claim_holds(name: &str, claim: Claim) -> bool {
    let held = claim.holds();
    let recorded = TimedClaim {
        name: name.to_string(),
        claim,
        held,
    };
    let _ = CLAIMS.try_with(|c| {
        let mut c = c.borrow_mut();
        if c.len() >= 8 {
            c.remove(0);
        }
        c.push(recorded);
    });
    held
}

/// The one way an e2e test may assert on wall clock. Carries both figures and
/// the margin into the panic message AND into the footer; the census test
/// below holds every e2e file to it.
pub fn timed_claim(name: &str, claim: Claim) {
    let margin = claim.margin();
    if !timed_claim_holds(name, claim) {
        panic!("wall-clock claim failed: {name}\n  {margin}");
    }
}

/// The claims recorded on this thread, oldest first.
pub fn recorded_claims() -> Vec<TimedClaim> {
    CLAIMS.try_with(|c| c.borrow().clone()).unwrap_or_default()
}

/// The footer text. Pure — the hook only prints what this returns, so every
/// branch (named test / unnamed thread, each register outcome, with and
/// without claims) is testable without actually panicking.
pub fn failure_footer(
    test_path: Option<&str>,
    status: Option<&RegisterStatus>,
    claims: &[TimedClaim],
) -> String {
    let rule = "─".repeat(78);
    let mut out = String::new();
    out.push_str(&format!("\n{rule}\n"));
    match test_path {
        Some(path) => {
            out.push_str(&format!("e2e failure: {path}\n"));
            out.push_str(&format!("reproduce:   {}\n", repro_command(path)));
        }
        None => {
            out.push_str("e2e failure: on a thread libtest did not name (a spawned task, or\n");
            out.push_str("             a run with --test-threads=1), so the test cannot be\n");
            out.push_str("             named from here. Re-run without --test-threads=1 for\n");
            out.push_str("             the exact repro command.\n");
        }
    }
    match status {
        Some(RegisterStatus::Quarantined {
            owner,
            entered,
            expires,
            cause,
        }) => {
            out.push_str(&format!(
                "flake register: QUARANTINED owner={owner} entered={entered} expires={expires} \
                 cause={cause}\n"
            ));
            out.push_str("                history and trend: `just flake-report`\n");
        }
        Some(RegisterStatus::NotRegistered) => {
            out.push_str("flake register: NOT REGISTERED — no row in .flake/register.json.\n");
            out.push_str(
                "                \"no row\" is NOT \"known good\": the register's population is\n",
            );
            out.push_str(
                "                the set of #[ignore]s, so a test that flakes without being\n",
            );
            out.push_str(
                "                ignored can never appear in it. If this one flakes, that is a\n",
            );
            out.push_str("                finding — see .flake/README.md.\n");
        }
        Some(RegisterStatus::CannotCheck { why }) => {
            out.push_str(&format!("flake register: CANNOT CHECK — {why}\n"));
            out.push_str(
                "                A cannot-check is not a pass; nothing about this test's flake\n",
            );
            out.push_str(
                "                status was verified (0/2/3 doctrine, scripts/ci/gates.json).\n",
            );
        }
        None => {}
    }
    for c in claims {
        let verdict = if c.held { "held" } else { "FAILED" };
        out.push_str(&format!("timing claim [{verdict}]: {}\n", c.name));
        out.push_str(&format!("                {}\n", c.claim.margin()));
    }
    out.push_str(&format!("{rule}\n"));
    out
}

/// Installs the footer hook, once per process. Chains the default hook rather
/// than replacing it, so the normal panic output is byte-identical and the
/// footer is strictly additive. Called by every `test_state*` constructor, and
/// explicitly by the handful of modules that build no state.
pub fn arm() {
    static ARMED: Once = Once::new();
    ARMED.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default(info);
            let thread = std::thread::current();
            let name = thread.name().map(str::to_string);
            // libtest names a test's own thread after its module path. Anything
            // else (a tokio worker, `main` under --test-threads=1) cannot be
            // resolved to a test, and saying so is better than guessing.
            let test_path = name.as_deref().filter(|n| n.starts_with("e2e::"));
            let status = test_path.map(|p| register_status(&register_id(p)));
            eprint!(
                "{}",
                failure_footer(test_path, status.as_ref(), &recorded_claims())
            );
        }));
    });
}

// ───────────────────── the census over this directory ─────────────────────
//
// Three derived inventories over `crates/server/src/e2e/`, all from
// `read_dir` rather than a hand list, because a hand list is the failure mode
// this section exists to close:
//
//   1. every sibling `.rs` is declared in `mod.rs`. rustc never warns about an
//      uncompiled sibling file, so a new e2e module nobody declares is a suite
//      member that silently never runs — a test that cannot fail.
//   2. every module that declares a test reaches the footer, so a red run in
//      it names its repro command.
//   3. no assertion compares a wall-clock quantity against a bound except
//      through [`timed_claim`] — the population `.flake/register.json` cannot
//      see, because its own population is the set of `#[ignore]`s and a test
//      that flakes without being ignored can never appear in it.
//
// The matchers are pure functions over source text, tested below against the
// two shapes that have silently defeated a source scanner in this repo before:
// a **comment** that talks about the rule above code that breaks it, and
// **CRLF** (this is a CRLF checkout). Each inventory also carries a nonzero
// floor, so a scan that reads nothing goes red instead of reporting a clean
// tree in a voice indistinguishable from success.

/// Comments and string-literal *contents* blanked to spaces, with every byte
/// offset and newline preserved so line numbers still line up.
///
/// Blanking strings is load-bearing twice over: a URL in a string
/// (`"https://example.com/slow"` — real, in `mcp_live.rs`) must not start a
/// line comment and swallow the rest of the line, and an assertion *message*
/// that happens to read `"must be < 5ms"` must not count as a comparison.
pub fn code_mask(src: &str) -> String {
    let b = src.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0usize;
    fn blank(out: &mut Vec<u8>, c: u8) {
        out.push(if c == b'\n' || c == b'\r' { c } else { b' ' });
    }
    while i < b.len() {
        // line comment
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'/' {
            while i < b.len() && b[i] != b'\n' {
                blank(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // block comment, nested as rustc allows
        if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
            let mut depth = 0usize;
            while i < b.len() {
                if b[i] == b'/' && i + 1 < b.len() && b[i + 1] == b'*' {
                    depth += 1;
                    blank(&mut out, b[i]);
                    blank(&mut out, b[i + 1]);
                    i += 2;
                    continue;
                }
                if b[i] == b'*' && i + 1 < b.len() && b[i + 1] == b'/' {
                    depth -= 1;
                    blank(&mut out, b[i]);
                    blank(&mut out, b[i + 1]);
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                    continue;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // raw string: r"..." / r#"..."# / r##"..."##
        if b[i] == b'r' && i + 1 < b.len() && (b[i + 1] == b'"' || b[i + 1] == b'#') {
            let mut j = i + 1;
            let mut hashes = 0usize;
            while j < b.len() && b[j] == b'#' {
                hashes += 1;
                j += 1;
            }
            if j < b.len() && b[j] == b'"' {
                out.push(b'r');
                out.resize(out.len() + hashes, b' ');
                out.push(b'"');
                j += 1;
                while j < b.len() {
                    if b[j] == b'"' && (1..=hashes).all(|k| j + k < b.len() && b[j + k] == b'#') {
                        out.push(b'"');
                        out.resize(out.len() + hashes, b' ');
                        j += 1 + hashes;
                        break;
                    }
                    blank(&mut out, b[j]);
                    j += 1;
                }
                i = j;
                continue;
            }
        }
        // ordinary string
        if b[i] == b'"' {
            out.push(b'"');
            i += 1;
            while i < b.len() {
                if b[i] == b'\\' && i + 1 < b.len() {
                    blank(&mut out, b[i]);
                    blank(&mut out, b[i + 1]);
                    i += 2;
                    continue;
                }
                if b[i] == b'"' {
                    out.push(b'"');
                    i += 1;
                    break;
                }
                blank(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // char literal, but NOT a lifetime: `'a'` closes, `'a,` does not
        if b[i] == b'\'' {
            if i + 2 < b.len() && b[i + 2] == b'\'' && b[i + 1] != b'\\' {
                out.push(b'\'');
                blank(&mut out, b[i + 1]);
                out.push(b'\'');
                i += 3;
                continue;
            }
            if i + 1 < b.len() && b[i + 1] == b'\\' {
                let mut j = i + 2;
                while j < b.len() && b[j] != b'\'' && b[j] != b'\n' {
                    j += 1;
                }
                if j < b.len() && b[j] == b'\'' {
                    out.push(b'\'');
                    for c in &b[i + 1..j] {
                        blank(&mut out, *c);
                    }
                    out.push(b'\'');
                    i = j + 1;
                    continue;
                }
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether a code region names a *measured* duration.
///
/// Deliberately NOT counting `Instant::now()` on its own: a `now() < deadline`
/// poll inside a `wait_*` helper is a timeout guard, not a performance claim,
/// and folding the two together would make the census unlandable rather than
/// true.
pub fn names_a_measured_duration(region: &str) -> bool {
    const TOKENS: [&str; 4] = [
        ".elapsed()",
        ".as_millis()",
        ".as_secs_f64()",
        ".as_secs_f32()",
    ];
    if TOKENS.iter().any(|t| region.contains(t)) {
        return true;
    }
    identifiers(region).iter().any(|id| {
        let lower = id.to_ascii_lowercase();
        lower == "elapsed" || lower.ends_with("_ms") || lower.ends_with("_millis")
    })
}

/// Rust identifiers in a code region.
fn identifiers(region: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for c in region.chars() {
        if c.is_alphanumeric() || c == '_' {
            cur.push(c);
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Whether a region compares two quantities. rustfmt spaces every binary
/// comparison and never spaces a generic's angle brackets, which is what makes
/// this cheap test sound on a formatted tree — and `cargo fmt --check` is a
/// gate, so the tree is always formatted.
pub fn compares(region: &str) -> bool {
    [" < ", " > ", " <= ", " >= "]
        .iter()
        .any(|op| region.contains(op))
}

/// The region of an assertion macro or `if` condition starting at `from`, as
/// (end offset, text). A macro region is paren-balanced; an `if` condition runs
/// to its opening brace.
fn region_at(mask: &[u8], from: usize, macro_call: bool) -> (usize, String) {
    let mut depth = 0i32;
    let mut i = from;
    let start = from;
    while i < mask.len() {
        let c = mask[i];
        if macro_call {
            if c == b'(' {
                depth += 1;
            } else if c == b')' {
                depth -= 1;
                if depth == 0 {
                    return (
                        i + 1,
                        String::from_utf8_lossy(&mask[start..=i]).into_owned(),
                    );
                }
            }
        } else if c == b'{' && depth == 0 {
            return (i + 1, String::from_utf8_lossy(&mask[start..i]).into_owned());
        } else if c == b'(' || c == b'[' {
            depth += 1;
        } else if c == b')' || c == b']' {
            depth -= 1;
        } else if c == b';' && depth == 0 {
            // a bare `if` that never reached a brace — bail rather than run on
            return (i + 1, String::from_utf8_lossy(&mask[start..i]).into_owned());
        }
        i += 1;
    }
    (
        mask.len(),
        String::from_utf8_lossy(&mask[start..]).into_owned(),
    )
}

/// Every site in one file that compares a wall-clock quantity against a bound
/// WITHOUT routing through [`timed_claim`], as 1-based line numbers.
pub fn raw_wall_clock_sites(src: &str) -> Vec<usize> {
    let masked = code_mask(src);
    let mask = masked.as_bytes();
    const STARTS: [&str; 5] = ["assert!(", "assert_eq!(", "assert_ne!(", "panic!(", "if "];
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < mask.len() {
        let Some(start) = STARTS
            .iter()
            .find(|s| mask[i..].starts_with(s.as_bytes()))
            .copied()
        else {
            i += 1;
            continue;
        };
        let is_macro = start != "if ";
        let open = if is_macro {
            i + start.len() - 1
        } else {
            i + start.len()
        };
        let (end, region) = region_at(mask, open, is_macro);
        if names_a_measured_duration(&region)
            && compares(&region)
            && !region.contains("timed_claim")
        {
            out.push(line_at(mask, i));
        }
        i = end.max(i + 1);
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// How many `timed_claim` call sites a file has — the census's own liveness
/// measure, so "found no violations" can be told apart from "read nothing".
pub fn timed_claim_sites(src: &str) -> usize {
    code_mask(src).matches("timed_claim").count()
}

/// Whether a module declares at least one test.
pub fn declares_a_test(src: &str) -> bool {
    let mask = code_mask(src);
    mask.contains("#[tokio::test]") || mask.contains("#[test]")
}

/// Whether a module reaches the footer: a `harness::test_state*` constructor
/// (each one arms it) or an explicit `harness::arm()`.
pub fn reaches_the_footer(src: &str) -> bool {
    let mask = code_mask(src);
    // `use super::harness::{test_state, FakeApp}` is the dominant import shape
    // in this directory (16 of 54 modules), and it does NOT contain the literal
    // `harness::test_state` — a matcher that only looked for that reported 41
    // unarmed modules out of 49, which is a broken matcher reporting a broken
    // tree. Reach is: it calls arm(), or it names the harness AND a state
    // constructor.
    mask.contains("harness::arm") || (mask.contains("harness::") && mask.contains("test_state"))
}

fn line_at(mask: &[u8], byte: usize) -> usize {
    mask[..byte.min(mask.len())]
        .iter()
        .filter(|c| **c == b'\n')
        .count()
        + 1
}

#[cfg(test)]
mod provenance_tests {
    use super::*;

    const REGISTER: &str = r#"{
      "ceiling": 4,
      "quarantine": [
        {"id": "pumper-core::lib::governor::tests::distinct_hosts_run_in_parallel_but_each_host_spaces",
         "owner": "xkazm04", "entered": "2026-08-24", "expires": "2026-10-26", "cause": "test"}
      ],
      "exempt": []
    }"#;

    #[test]
    fn a_failing_test_names_the_binary_only_repro_command_not_lib_or_test() {
        let cmd =
            repro_command("e2e::fanout_offslot::a_slow_index_no_longer_holds_a_scrape_permit");
        assert_eq!(
            cmd,
            "cargo test -p pumper-server --bin pumper -- --exact \
             e2e::fanout_offslot::a_slow_index_no_longer_holds_a_scrape_permit --nocapture"
        );
        // The trap CLAUDE.md already documents for `cargo run`: this crate ships
        // three binaries and has no default-run, so an invocation without
        // `--bin pumper` does not resolve.
        assert!(
            !cmd.contains("--lib"),
            "the suite is in a binary, not a lib"
        );
        assert!(
            !cmd.contains("--test "),
            "the suite is in src/, not tests/ — `--test` names an integration target"
        );
    }

    #[test]
    fn a_quarantined_test_reads_its_owner_and_expiry_not_just_quarantined() {
        let status = status_in_register(
            REGISTER,
            "pumper-core::lib::governor::tests::distinct_hosts_run_in_parallel_but_each_host_spaces",
        );
        let footer = failure_footer(Some("governor::tests::x"), Some(&status), &[]);
        assert!(footer.contains("QUARANTINED"), "{footer}");
        assert!(footer.contains("owner=xkazm04"), "{footer}");
        assert!(footer.contains("expires=2026-10-26"), "{footer}");
        assert!(footer.contains("cause=test"), "{footer}");
        assert!(
            footer.contains("just flake-report"),
            "the footer must hand the reader the history query, not just the row: {footer}"
        );
    }

    #[test]
    fn an_unregistered_test_says_no_row_is_not_known_good() {
        let status = status_in_register(
            REGISTER,
            register_id("e2e::fanout_offslot::whatever").as_str(),
        );
        assert_eq!(status, RegisterStatus::NotRegistered);
        let footer = failure_footer(Some("e2e::fanout_offslot::whatever"), Some(&status), &[]);
        assert!(footer.contains("NOT REGISTERED"), "{footer}");
        assert!(
            footer.contains("NOT \"known good\""),
            "silence about a flake must not read as a clean bill: {footer}"
        );
    }

    #[test]
    fn an_unreadable_register_is_cannot_check_and_never_reads_as_a_pass() {
        let status = status_in_register("{ not json", "pumper-server::bin:pumper::e2e::x::y");
        let RegisterStatus::CannotCheck { ref why } = status else {
            panic!("a register that does not parse must be CANNOT CHECK, got {status:?}")
        };
        assert!(why.contains(REGISTER_FILE), "it must name the file: {why}");
        let footer = failure_footer(Some("e2e::x::y"), Some(&status), &[]);
        assert!(footer.contains("CANNOT CHECK"), "{footer}");
        assert!(
            footer.contains("not a pass"),
            "0/2/3: a cannot-check that reads like a pass is the whole failure mode: {footer}"
        );
        // A register with no quarantine array is equally un-consultable.
        assert!(matches!(
            status_in_register("{\"ceiling\": 4}", "a::b::c"),
            RegisterStatus::CannotCheck { .. }
        ));
    }

    #[test]
    fn a_failed_timing_claim_prints_both_figures_and_the_margin() {
        let claim = Claim::SpeedupAtLeast {
            fast_ms: 900.0,
            slow_ms: 1020.0,
            factor: 2.0,
        };
        assert!(!claim.holds());
        let footer = failure_footer(
            Some("e2e::fanout_offslot::a_slow_index_no_longer_holds_a_scrape_permit"),
            Some(&RegisterStatus::NotRegistered),
            &[TimedClaim {
                name: "moving the fan-out off the permit must at least halve the drain".into(),
                claim,
                held: false,
            }],
        );
        assert!(footer.contains("1.13x achieved"), "{footer}");
        assert!(footer.contains("2.00x required"), "{footer}");
        assert!(
            footer.contains("900ms"),
            "the measured figures travel: {footer}"
        );
        assert!(footer.contains("1020ms"), "{footer}");
    }

    #[test]
    fn a_near_miss_and_a_wide_miss_do_not_read_the_same() {
        let hair = Claim::SpeedupAtLeast {
            fast_ms: 520.0,
            slow_ms: 1020.0,
            factor: 2.0,
        };
        let wide = Claim::SpeedupAtLeast {
            fast_ms: 2550.0,
            slow_ms: 1020.0,
            factor: 2.0,
        };
        assert!(!hair.holds() && !wide.holds());
        assert!(
            hair.margin().contains("MISSED BY A HAIR"),
            "{}",
            hair.margin()
        );
        assert!(wide.margin().contains("MISSED WIDE"), "{}", wide.margin());
        // The decision this whole section exists to make possible.
        assert!(near_miss(1.96, 2.0));
        assert!(!near_miss(0.4, 2.0));
    }

    #[test]
    fn an_at_most_claim_reports_the_ceiling_it_used() {
        let claim = Claim::AtMost {
            measured_ms: 4200.0,
            limit_ms: 5000.0,
        };
        assert!(claim.holds());
        assert!(claim.margin().contains("0.84x"), "{}", claim.margin());
        assert!(
            claim.margin().contains("5000ms ceiling"),
            "{}",
            claim.margin()
        );
        assert!(!Claim::AtMost {
            measured_ms: 5001.0,
            limit_ms: 5000.0
        }
        .holds());
    }

    #[test]
    fn the_register_key_matches_the_shape_flake_id_mjs_writes() {
        // scripts/ci/flake-id.mjs registerKey(): first, second, last.
        assert_eq!(
            register_key("pumper-server::bin:pumper::e2e::fanout_offslot::a_slow_index"),
            Some("pumper-server::bin:pumper::a_slow_index".to_string())
        );
        assert_eq!(register_key("too::short"), None);
        assert_eq!(
            register_id("e2e::x::y"),
            "pumper-server::bin:pumper::e2e::x::y"
        );
        // A row written against a different intermediate path still reconciles.
        let reg = r#"{"quarantine":[{"id":"pumper-server::bin:pumper::moved::y","owner":"a",
            "entered":"b","expires":"c","cause":"test"}]}"#;
        assert!(matches!(
            status_in_register(reg, "pumper-server::bin:pumper::e2e::x::y"),
            RegisterStatus::Quarantined { .. }
        ));
    }

    #[test]
    fn an_unnamed_thread_says_so_instead_of_guessing_a_command() {
        let footer = failure_footer(None, None, &[]);
        assert!(footer.contains("did not name"), "{footer}");
        assert!(
            !footer.contains("--exact"),
            "a guessed repro command is worse than none: {footer}"
        );
    }

    #[test]
    fn the_real_register_on_disk_is_readable_from_the_test_process() {
        // The path resolution itself is a thing that can rot; if it does, every
        // footer silently degrades to CANNOT CHECK. So check it here, loudly.
        let status = register_status(&register_id("e2e::no_such_module::no_such_test"));
        assert_eq!(
            status,
            RegisterStatus::NotRegistered,
            "the real .flake/register.json must be reachable from the test process \
             (repo root resolved as {})",
            repo_root().display()
        );
        assert!(matches!(
            register_status(
                "pumper-engine-wasm::lib::tests::\
                 instance_pre_instantiation_is_never_slower_than_relinking_per_call"
            ),
            RegisterStatus::Quarantined { .. }
        ));
    }

    #[test]
    fn a_recorded_claim_reaches_the_footer_from_the_test_that_made_it() {
        assert!(timed_claim_holds(
            "a probe claim",
            Claim::AtMost {
                measured_ms: 1.0,
                limit_ms: 1000.0
            }
        ));
        let claims = recorded_claims();
        assert!(
            claims.iter().any(|c| c.name == "a probe claim"),
            "timed_claim_holds must record even when the claim holds, or a counted \
             breach never reaches the footer"
        );
    }
}

#[cfg(test)]
mod census_tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    fn e2e_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/e2e")
    }

    /// Every `<name>.rs` beside `mod.rs`, from the filesystem.
    fn sibling_modules() -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for entry in std::fs::read_dir(e2e_dir()).expect("read_dir over crates/server/src/e2e") {
            let path = entry.expect("dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let stem = path.file_stem().unwrap().to_str().unwrap().to_string();
            if stem != "mod" {
                out.insert(stem);
            }
        }
        out
    }

    fn source_of(module: &str) -> String {
        std::fs::read_to_string(e2e_dir().join(format!("{module}.rs"))).expect("module source")
    }

    /// The floor every scan here asserts first. A scan that reads nothing must
    /// be RED, not green — "found no violations" and "found no files" are the
    /// same sentence otherwise, and this repo has been bitten by that twice.
    const MODULE_FLOOR: usize = 40;

    #[test]
    fn every_e2e_module_file_is_declared_in_mod_rs() {
        let files = sibling_modules();
        assert!(
            files.len() >= MODULE_FLOOR,
            "the e2e scan found only {} modules — the scan is broken, not the tree",
            files.len()
        );
        let mod_rs = std::fs::read_to_string(e2e_dir().join("mod.rs")).expect("mod.rs");
        let declared: BTreeSet<String> = code_mask(&mod_rs)
            .lines()
            .filter_map(|l| l.trim().strip_prefix("mod "))
            .filter_map(|l| l.split(';').next())
            .map(|s| s.trim().to_string())
            .collect();
        let undeclared: Vec<&String> = files.difference(&declared).collect();
        assert!(
            undeclared.is_empty(),
            "e2e module file(s) with no `mod` line in mod.rs: {undeclared:?}. rustc never \
             warns about an uncompiled sibling, so these are suite members that silently \
             never run — a test that cannot fail."
        );
        let orphaned: Vec<&String> = declared.difference(&files).collect();
        assert!(
            orphaned.is_empty(),
            "mod.rs declares module(s) with no sibling file: {orphaned:?}"
        );
    }

    #[test]
    fn every_e2e_module_with_a_test_reaches_the_failure_footer() {
        let files = sibling_modules();
        assert!(files.len() >= MODULE_FLOOR, "scan floor: {}", files.len());
        let mut unarmed = Vec::new();
        let mut with_tests = 0usize;
        for module in &files {
            if module == "harness" {
                continue;
            }
            let src = source_of(module);
            if !declares_a_test(&src) {
                continue;
            }
            with_tests += 1;
            if !reaches_the_footer(&src) {
                unarmed.push(module.clone());
            }
        }
        assert!(
            with_tests >= MODULE_FLOOR,
            "only {with_tests} e2e modules declare a test — the scan is broken"
        );
        assert!(
            unarmed.is_empty(),
            "e2e module(s) whose tests panic with no repro footer: {unarmed:?}. Each needs a \
             `harness::test_state*` constructor or an explicit `harness::arm()`."
        );
    }

    #[test]
    fn no_e2e_assertion_reads_the_clock_outside_timed_claim() {
        let files = sibling_modules();
        assert!(files.len() >= MODULE_FLOOR, "scan floor: {}", files.len());
        let mut raw = Vec::new();
        let mut routed = 0usize;
        for module in &files {
            let src = source_of(module);
            routed += timed_claim_sites(&src);
            for line in raw_wall_clock_sites(&src) {
                raw.push(format!("crates/server/src/e2e/{module}.rs:{line}"));
            }
        }
        // Liveness: the census must be able to SEE the sites it governs. If the
        // converted call sites stop matching, this scan has gone blind and its
        // silence means nothing.
        assert!(
            routed >= 6,
            "the census found only {routed} timed_claim call sites across the e2e tree — \
             it is reading the wrong thing, so its clean verdict is worthless"
        );
        assert!(
            raw.is_empty(),
            "wall-clock comparison(s) outside harness::timed_claim: {raw:#?}\n\
             A raw timing assert carries no figures and no margin, so a reader cannot tell a \
             loaded box from a regression — and it is invisible to .flake/register.json, whose \
             population is the set of #[ignore]s."
        );
    }

    // ── the matchers' own guards: a census that cannot find what it counts
    // reports a clean codebase in a voice indistinguishable from success ──

    #[test]
    fn the_census_finds_a_seeded_raw_wall_clock_assert() {
        let seeded = "#[tokio::test]\nasync fn x() {\n    assert!(a_ms * 2 < b_ms, \"nope\");\n}\n";
        assert_eq!(raw_wall_clock_sites(seeded), vec![3]);
        let seeded_if = "fn x() {\n    if started.elapsed().as_millis() > CEILING {\n        n += 1;\n    }\n}\n";
        assert_eq!(raw_wall_clock_sites(seeded_if), vec![2]);
    }

    #[test]
    fn the_census_is_not_satisfied_by_a_comment_that_talks_about_the_rule() {
        // The exact shape that defeated a matcher in this repo once already.
        let commented = "fn x() {\n    // every timing claim goes through timed_claim\n    assert!(a_ms < b_ms);\n}\n";
        assert_eq!(
            raw_wall_clock_sites(commented),
            vec![3],
            "a comment naming the rule must not launder the code below it"
        );
        let only_a_comment =
            "fn x() {\n    // assert!(a_ms < b_ms) — the thing we must not do\n    ok();\n}\n";
        assert!(
            raw_wall_clock_sites(only_a_comment).is_empty(),
            "a violation that exists ONLY in a comment is not a violation"
        );
    }

    #[test]
    fn the_census_survives_crlf_because_this_is_a_crlf_checkout() {
        let crlf = "fn x() {\r\n    assert!(a_ms < b_ms);\r\n}\r\n";
        assert_eq!(
            raw_wall_clock_sites(crlf),
            vec![2],
            "`.` does not match `\\r`, and every file in this checkout has CRLF endings"
        );
        let crlf_comment = "fn x() {\r\n    // assert!(a_ms < b_ms)\r\n    ok();\r\n}\r\n";
        assert!(
            raw_wall_clock_sites(crlf_comment).is_empty(),
            "a CRLF line comment must still end at the line, not run on"
        );
        assert_eq!(code_mask(crlf).lines().count(), crlf.lines().count());
    }

    #[test]
    fn a_url_in_a_string_does_not_blind_the_scanner() {
        // `//` inside a string literal is not a comment. mcp_live.rs really does
        // carry `"https://example.com/slow"`; a naive strip eats the rest of the
        // line and every violation after it on that line.
        let src = "fn x() {\n    let u = \"https://example.com/slow\"; assert!(a_ms < b_ms);\n}\n";
        assert_eq!(raw_wall_clock_sites(src), vec![2]);
        // And a message that merely READS like a comparison is not one.
        let message_only = "fn x() {\n    assert!(ok, \"wanted a_ms < b_ms\");\n}\n";
        assert!(raw_wall_clock_sites(message_only).is_empty());
    }

    #[test]
    fn routing_through_timed_claim_clears_the_census() {
        let routed = "fn x() {\n    harness::timed_claim(\"n\", Claim::AtMost { measured_ms: a_ms, limit_ms: 5.0 });\n}\n";
        assert!(raw_wall_clock_sites(routed).is_empty());
        assert_eq!(timed_claim_sites(routed), 1);
        // A deadline poll is a timeout guard, not a performance claim, and the
        // census says so by construction rather than by an allowlist.
        let poll = "fn x() {\n    assert!(Instant::now() < deadline, \"timed out\");\n}\n";
        assert!(raw_wall_clock_sites(poll).is_empty());
    }

    #[test]
    fn the_footer_reach_matcher_finds_a_seeded_unarmed_module() {
        let unarmed = "#[tokio::test]\nasync fn x() {\n    let s = my_own_state().await;\n}\n";
        assert!(declares_a_test(unarmed));
        assert!(!reaches_the_footer(unarmed));
        let armed = "#[tokio::test]\nasync fn x() {\n    super::harness::arm();\n}\n";
        assert!(reaches_the_footer(armed));
        let via_state = "use super::harness::test_state;\n#[tokio::test]\nasync fn x() {}\n";
        assert!(reaches_the_footer(via_state));
        // A test attribute that exists only in a comment declares nothing.
        assert!(!declares_a_test("// #[tokio::test]\nfn not_a_test() {}\n"));
    }
}
