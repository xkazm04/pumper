//! VCR record/replay (M24): deterministic re-execution of job runs.
//!
//! **Record** (`record: true` on enqueue): every fetch and research call that
//! goes through [`crate::AppContext`] appends a [`CassetteEntry`]
//! `{url, method, req_hash, status, headers, engine, body}` to the job's
//! `artifacts_dir/cassette.ndjson`. Entries are size-capped per entry and per
//! cassette; an over-cap entry keeps its request identity but drops its body
//! and carries `recorded_truncated: true` — an honest marker, never a silent
//! gap. Recording is best-effort telemetry: a write failure warn-logs and
//! never fails the job.
//!
//! **Replay** (`replay_of: <job_id>` on enqueue): every fetch resolves from
//! that job's cassette by `req_hash`. A MISS is a **typed error**
//! ([`crate::Error::ReplayMiss`]) — never a silent live fetch, because
//! determinism is the whole value of replay. Replay runs touch no engine, obey
//! no politeness delay, and spend $0 (every metered seam records a
//! `vcr_replay` cost event at 0.0).
//!
//! ## Attempts: one cassette, the attempt that actually did the work
//!
//! A job can run more than once (retry, reaper re-queue, shutdown suspend), and
//! all its attempts share ONE cassette path. Which attempt's recording survives
//! is decided by [`CassetteStart`], on one rule: **the cassette records the
//! job's work, and work survives exactly when a durable checkpoint does.**
//!
//! - An attempt that starts fresh (no checkpoint restored) discards the earlier
//!   cassette. Otherwise a failed attempt's *partial* recording shadows the
//!   successful attempt's complete one (entries load first-wins), and the replay
//!   reproduces the run that FAILED while claiming determinism.
//! - An attempt that resumes from a checkpoint appends to it, because it will
//!   not re-fetch what the earlier attempt already recorded. This is also the
//!   shutdown-suspend path, which re-queues without even burning an attempt: a
//!   suspended recording **resumes**, it does not restart.
//!
//! The total-size cap is seeded from the cassette actually on disk, so it binds
//! on real bytes rather than resetting to zero on every attempt.
//!
//! ## Documented limitations
//! - The seam is [`crate::AppContext::fetch`] / [`crate::AppContext::research`]
//!   — the choke point every well-behaved app uses. Apps that drive engines
//!   raw (the crawler owns its own frontier and calls engines directly) bypass
//!   the cassette and cannot be recorded or replayed.
//! - The AppContext seam sits **above** header granularity: a fetch returns a
//!   [`FetchOutcome`], not an `HttpResponse`, so `headers` is populated only
//!   with the subset the outcome exposes — today the two archive-provenance
//!   headers, so a replayed snapshot does not come back looking live. `engine` +
//!   `status` are the rest of the recorded response metadata.
//! - **Browser-tier renders replay as their final response equivalent**: the
//!   recorded outcome's `html` is the post-render document. Replay does not
//!   re-run JS, `actions`, or `wait_for_selector` — it hands back the bytes the
//!   original run saw, which is exactly the deterministic contract.
//! - Replay is byte-deterministic for the http/browser/archive/recipe tiers;
//!   a recorded Claude answer replays verbatim (its original sampling is
//!   frozen into the cassette).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::engine::{ResearchOutput, ResearchRequest};
use crate::fetcher::{FetchOutcome, FetchTier, TierTrace, TierVerdict};
use crate::{Error, Result};

/// Cassette file name inside a job's artifacts dir (NDJSON, one entry per line).
pub const CASSETTE_FILE: &str = "cassette.ndjson";

/// Per-entry size cap (serialized line bytes). Over it, the entry's body is
/// dropped and `recorded_truncated` set.
pub const ENTRY_CAP_BYTES: usize = 4 * 1024 * 1024;

/// Whole-cassette size cap. Once cumulative written bytes would exceed it,
/// further entries are recorded as truncated markers (identity only).
pub const TOTAL_CAP_BYTES: usize = 128 * 1024 * 1024;

/// Method tag for tiered fetches (the fetcher only issues GETs).
pub const METHOD_GET: &str = "GET";
/// Method tag for Claude research calls (hashed over the canonical request
/// key, not a URL).
pub const METHOD_RESEARCH: &str = "RESEARCH";

/// The VCR mode a job runs under. `Off` is the default and changes nothing.
#[derive(Clone, Default)]
pub enum Vcr {
    #[default]
    Off,
    /// Persist every AppContext fetch/research into this job's cassette.
    Record(Arc<Recorder>),
    /// Serve every AppContext fetch/research from a prior job's cassette.
    Replay(Arc<Cassette>),
}

/// Canonical request hash: `sha256(method \0 key)` hex. For fetches `key` is
/// the URL; for research it is [`crate::ResearchCache::key`] (prompt, system
/// prompt, role, model, effort, turns, schema — deliberately excluding budget
/// clamps, so a replay with a different budget still resolves).
pub fn req_hash(method: &str, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(method.as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// One recorded request/response pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassetteEntry {
    /// The fetched URL (empty for research entries — the prompt is not a URL;
    /// `detail` carries its leading window instead).
    pub url: String,
    /// `GET` (tiered fetch) or `RESEARCH` (Claude call).
    pub method: String,
    /// [`req_hash`] of this request — the replay lookup key.
    pub req_hash: String,
    /// HTTP status when the winning tier had one (http/archive/recipe).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    /// Response-header subset. The AppContext seam returns a `FetchOutcome`,
    /// which exposes no header map (see module docs), so this carries only what
    /// the outcome *does* surface: the two archive-provenance headers, when the
    /// body came out of a stored snapshot. Empty for a live fetch.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Winning engine: `http`/`browser`/`archive`/`api_recipe`/`claude`.
    pub engine: String,
    /// Recorded payload. Fetch: `{html?, markdown?, text?}`. Research:
    /// `{text, json?}`. `None` when truncated at record time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// Human hint for research entries (leading window of the prompt).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// The body was over a size cap and dropped; replaying this entry is a
    /// typed miss.
    #[serde(default)]
    pub recorded_truncated: bool,
}

/// Builds the cassette entry for one completed tiered fetch.
pub fn fetch_entry(outcome: &FetchOutcome) -> CassetteEntry {
    let mut body = serde_json::Map::new();
    if let Some(html) = &outcome.html {
        body.insert("html".into(), Value::String(html.clone()));
    }
    if let Some(md) = &outcome.markdown {
        body.insert("markdown".into(), Value::String(md.clone()));
    }
    if let Some(text) = &outcome.text {
        body.insert("text".into(), Value::String(text.clone()));
    }
    // Snapshot provenance rides out on the entry's header map (until now an
    // always-empty field) using the same two header names the archive engine
    // writes — so a replayed archive fetch does not come back looking live.
    let mut headers = BTreeMap::new();
    if let Some(snapshot) = &outcome.snapshot {
        headers.insert(
            crate::engine::FETCHED_VIA_HEADER.to_string(),
            snapshot.via.clone(),
        );
        if let Some(captured_at) = &snapshot.captured_at {
            headers.insert(
                crate::engine::SNAPSHOT_TS_HEADER.to_string(),
                captured_at.clone(),
            );
        }
    }
    CassetteEntry {
        url: outcome.url.clone(),
        method: METHOD_GET.into(),
        req_hash: req_hash(METHOD_GET, &outcome.url),
        status: outcome.status,
        headers,
        engine: outcome.engine.to_string(),
        body: Some(Value::Object(body)),
        detail: None,
        recorded_truncated: false,
    }
}

/// Builds the cassette entry for one completed research call. `key` is the
/// canonical request key ([`crate::ResearchCache::key`]).
pub fn research_entry(key: &str, req: &ResearchRequest, out: &ResearchOutput) -> CassetteEntry {
    let mut body = serde_json::Map::new();
    body.insert("text".into(), Value::String(out.text.clone()));
    if let Some(json) = &out.json {
        body.insert("json".into(), json.clone());
    }
    CassetteEntry {
        url: String::new(),
        method: METHOD_RESEARCH.into(),
        req_hash: req_hash(METHOD_RESEARCH, key),
        status: None,
        headers: BTreeMap::new(),
        engine: "claude".into(),
        body: Some(Value::Object(body)),
        detail: Some(req.prompt.chars().take(120).collect()),
        recorded_truncated: false,
    }
}

/// Reconstructs the [`FetchOutcome`] a replay hands back for a recorded fetch.
/// The trace carries a single `vcr replay` entry for the recorded tier so
/// consumers can see (and cost events can mark) that nothing live ran.
pub fn to_fetch_outcome(entry: &CassetteEntry, replay_of: Uuid) -> Result<FetchOutcome> {
    let (engine, tier) = engine_tier(&entry.engine).ok_or_else(|| {
        Error::ReplayMiss(format!(
            "cassette entry for {} names unknown engine {:?}",
            entry.url, entry.engine
        ))
    })?;
    let Some(body) = &entry.body else {
        return Err(truncated_miss(entry, replay_of));
    };
    let field = |k: &str| body.get(k).and_then(Value::as_str).map(str::to_string);
    Ok(FetchOutcome {
        url: entry.url.clone(),
        engine,
        status: entry.status,
        html: field("html"),
        markdown: field("markdown"),
        text: field("text"),
        escalations: Vec::new(),
        trace: vec![TierTrace {
            tier,
            verdict: TierVerdict::Ok,
            http_status: entry.status,
            content_chars: None,
            cache_hit: None,
            latency_ms: 0,
            cost_usd: None,
            detail: Some(format!("vcr replay of job {replay_of}")),
        }],
        // Replay spends nothing, whatever the original tier cost.
        cost_usd: None,
        // A recorded archive win replays as an archive win, capture time and
        // all — read back through the same seam that wrote it.
        snapshot: crate::engine::snapshot_provenance(&entry.headers),
    })
}

/// Reconstructs the [`ResearchOutput`] a replay hands back. `cost_usd` is 0 —
/// the recorded answer is served, the model is never invoked.
pub fn to_research_output(entry: &CassetteEntry, replay_of: Uuid) -> Result<ResearchOutput> {
    let Some(body) = &entry.body else {
        return Err(truncated_miss(entry, replay_of));
    };
    let text = body
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok(ResearchOutput {
        text,
        json: body.get("json").cloned(),
        cost_usd: Some(0.0),
        duration_ms: Some(0),
        num_turns: None,
        session_id: None,
    })
}

fn truncated_miss(entry: &CassetteEntry, replay_of: Uuid) -> Error {
    Error::ReplayMiss(format!(
        "recorded response for {} {} in job {replay_of}'s cassette was truncated at \
         record time (over the size cap) — its body is gone and cannot be replayed",
        entry.method,
        display_of(entry),
    ))
}

fn display_of(entry: &CassetteEntry) -> String {
    if entry.url.is_empty() {
        entry.detail.clone().unwrap_or_else(|| "<request>".into())
    } else {
        entry.url.clone()
    }
}

/// Maps a recorded engine string back to the fetcher's `&'static str` +
/// [`FetchTier`] pair. `None` for a string no shipped tier produces.
fn engine_tier(s: &str) -> Option<(&'static str, FetchTier)> {
    Some(match s {
        "api_recipe" => ("api_recipe", FetchTier::ApiRecipe),
        "archive" => ("archive", FetchTier::Archive),
        "http" => ("http", FetchTier::Http),
        "browser" => ("browser", FetchTier::Browser),
        "claude" => ("claude", FetchTier::Claude),
        _ => return None,
    })
}

// ── Recording ───────────────────────────────────────────────────────────────

/// What a new [`Recorder`] does with the cassette an EARLIER attempt of the
/// same job left behind — the whole of the retry-poisoning fix.
///
/// The cassette records a job's **work**, and work survives a new attempt
/// exactly when a durable checkpoint does. That is the rule, and it decides
/// both cases:
///
/// - No checkpoint restored → the attempt re-does everything, so the earlier
///   recording is dead work. Keeping it was the bug: append-mode plus
///   first-recording-wins loading meant a failed attempt 1's *partial* entries
///   shadowed attempt 2's complete ones, and the replay reproduced the failed
///   run's data while claiming determinism.
/// - A checkpoint restored (a retry that resumes, or a shutdown-suspend
///   re-queue, which does not even burn an attempt) → the attempt deliberately
///   SKIPS the work already done, so wiping its recordings would punch holes in
///   the cassette for fetches the job really made. Keep and append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CassetteStart {
    /// Discard any earlier cassette for this job and start empty.
    Fresh,
    /// Keep the earlier cassette and append, seeding the size cap from the
    /// bytes already on disk.
    Resume,
}

/// Appends cassette entries to a job's `artifacts_dir/cassette.ndjson`,
/// enforcing the per-entry and total size caps. Best-effort: failures warn-log
/// and never fail the job (recording is telemetry; the job's real work is not).
///
/// One recorder is built per **attempt**, and [`CassetteStart`] decides what it
/// inherits from the previous one.
pub struct Recorder {
    dir: PathBuf,
    entry_cap: usize,
    total_cap: usize,
    start: CassetteStart,
    /// Bytes in this job's cassette. `None` until the [`CassetteStart`] policy
    /// has been applied — that lazy init is what makes the cap bind on the
    /// file's REAL size instead of a fresh in-memory zero on every attempt.
    written: tokio::sync::Mutex<Option<u64>>,
}

impl Recorder {
    /// A recorder for a fresh attempt: any cassette an earlier attempt left is
    /// discarded (see [`CassetteStart`]).
    pub fn new(artifacts_dir: PathBuf) -> Self {
        Self::with_caps(artifacts_dir, ENTRY_CAP_BYTES, TOTAL_CAP_BYTES)
    }

    /// A recorder for an attempt that RESUMED from a durable checkpoint: the
    /// earlier attempt's entries are kept and appended to, because the resumed
    /// run will not re-fetch what they recorded.
    pub fn resuming(artifacts_dir: PathBuf) -> Self {
        Self::with_caps_starting(
            artifacts_dir,
            ENTRY_CAP_BYTES,
            TOTAL_CAP_BYTES,
            CassetteStart::Resume,
        )
    }

    /// Caps override — tests exercise the truncation paths with tiny caps.
    pub fn with_caps(artifacts_dir: PathBuf, entry_cap: usize, total_cap: usize) -> Self {
        Self::with_caps_starting(artifacts_dir, entry_cap, total_cap, CassetteStart::Fresh)
    }

    /// Caps + start policy — the full constructor the presets above cover.
    pub fn with_caps_starting(
        artifacts_dir: PathBuf,
        entry_cap: usize,
        total_cap: usize,
        start: CassetteStart,
    ) -> Self {
        Self {
            dir: artifacts_dir,
            entry_cap: entry_cap.max(1),
            total_cap: total_cap.max(1),
            start,
            written: tokio::sync::Mutex::new(None),
        }
    }

    pub fn cassette_path(&self) -> PathBuf {
        self.dir.join(CASSETTE_FILE)
    }

    /// Applies the [`CassetteStart`] policy now, before the run makes its first
    /// fetch. [`record`](Self::record) applies it lazily anyway (so a recorder
    /// used without this call is still correct — the guarantee must not depend
    /// on a caller remembering), but a run that ends up fetching NOTHING would
    /// then leave the previous attempt's cassette in place and let a replay
    /// reproduce a run this attempt never made. Call it once at attempt start.
    pub async fn prepare(&self) {
        let mut written = self.written.lock().await;
        self.ensure_started(&mut written).await;
    }

    /// The one place the start policy is applied. Idempotent: once `written` is
    /// `Some`, this attempt's cassette is already in the state it asked for.
    async fn ensure_started(&self, written: &mut Option<u64>) {
        if written.is_some() {
            return;
        }
        let path = self.cassette_path();
        let bytes = match self.start {
            CassetteStart::Fresh => {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => tracing::debug!(
                        "vcr: discarded a previous attempt's cassette at {}",
                        path.display()
                    ),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    // Fail open, like every other recording failure: an
                    // undeletable cassette must not fail the job. The append
                    // below then adds to it, which is the OLD behaviour — worse
                    // than a clean start, but never worse than losing the run.
                    Err(e) => tracing::warn!(
                        "vcr: could not clear the previous attempt's cassette at {}: {e}",
                        path.display()
                    ),
                }
                0
            }
            // The cap must bind on what is actually on disk. Seeding from a
            // fresh zero was how the 128 MiB total cap was defeated across
            // attempts: each attempt believed it had the whole budget again.
            CassetteStart::Resume => tokio::fs::metadata(&path).await.map_or(0, |m| m.len()),
        };
        *written = Some(bytes);
    }

    /// Appends one entry, applying the caps. An over-cap entry (or any entry
    /// once the total cap is reached) is written as a truncated marker — the
    /// request identity survives so a replay MISS on it is diagnosable, but
    /// the body is gone. A truncated marker is always written (markers are
    /// ~200 bytes; strict-cap purity is not worth a silent hole).
    pub async fn record(&self, mut entry: CassetteEntry) {
        let mut line = match serde_json::to_string(&entry) {
            Ok(line) => line,
            Err(e) => {
                tracing::warn!("vcr: cassette entry serialize failed: {e}");
                return;
            }
        };
        let mut written = self.written.lock().await;
        self.ensure_started(&mut written).await;
        let so_far = written.unwrap_or(0);
        if line.len() > self.entry_cap || so_far + line.len() as u64 > self.total_cap as u64 {
            entry.body = None;
            entry.recorded_truncated = true;
            line = match serde_json::to_string(&entry) {
                Ok(line) => line,
                Err(e) => {
                    tracing::warn!("vcr: cassette marker serialize failed: {e}");
                    return;
                }
            };
        }
        line.push('\n');
        if let Err(e) = self.append(&line).await {
            tracing::warn!("vcr: cassette write failed: {e}");
            return;
        }
        *written = Some(so_far + line.len() as u64);
    }

    async fn append(&self, line: &str) -> std::io::Result<()> {
        tokio::fs::create_dir_all(&self.dir).await?;
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.cassette_path())
            .await?;
        file.write_all(line.as_bytes()).await
    }
}

// ── Replay ──────────────────────────────────────────────────────────────────

/// A loaded cassette: the recorded entries of one prior job, keyed by
/// `req_hash`. When the same request was recorded more than once in a run the
/// **first** recording wins (deterministic; a mid-run source change is folded
/// to the first observation).
#[derive(Debug)]
pub struct Cassette {
    replay_of: Uuid,
    entries: HashMap<String, CassetteEntry>,
}

impl Cassette {
    /// Loads `<artifacts_dir>/cassette.ndjson`. A missing file or a cassette
    /// with zero readable entries is a typed [`Error::ReplayMiss`] — the job
    /// being replayed was not recorded (or its cassette is gone), and running
    /// live instead would silently defeat the point.
    pub async fn load(artifacts_dir: &Path, replay_of: Uuid) -> Result<Self> {
        let path = artifacts_dir.join(CASSETTE_FILE);
        let raw = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw,
            Err(e) => {
                return Err(Error::ReplayMiss(format!(
                    "job {replay_of} has no cassette at {} ({e}) — was it enqueued with \
                     record: true?",
                    path.display()
                )))
            }
        };
        let mut entries = HashMap::new();
        for line in raw.lines().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str::<CassetteEntry>(line) {
                // First recording of a request wins.
                Ok(entry) => {
                    entries.entry(entry.req_hash.clone()).or_insert(entry);
                }
                Err(e) => tracing::warn!("vcr: skipping unreadable cassette line: {e}"),
            }
        }
        if entries.is_empty() {
            return Err(Error::ReplayMiss(format!(
                "job {replay_of}'s cassette at {} holds no readable entries",
                path.display()
            )));
        }
        Ok(Self { replay_of, entries })
    }

    /// The job this cassette was recorded by.
    pub fn replay_of(&self) -> Uuid {
        self.replay_of
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolves one request by `(method, key)` — `key` is the URL for fetches,
    /// the canonical research key for research. A MISS (or a truncated
    /// recording) is a typed [`Error::ReplayMiss`]; `display` names the
    /// request in the message (URL / prompt head).
    pub fn resolve(&self, method: &str, key: &str, display: &str) -> Result<&CassetteEntry> {
        let hash = req_hash(method, key);
        let entry = self.entries.get(&hash).ok_or_else(|| {
            Error::ReplayMiss(format!(
                "no recorded response for {method} {display} in job {}'s cassette \
                 ({} entries) — replay never falls through to a live fetch",
                self.replay_of,
                self.entries.len()
            ))
        })?;
        if entry.recorded_truncated || entry.body.is_none() {
            return Err(truncated_miss(entry, self.replay_of));
        }
        Ok(entry)
    }

    /// Test/tooling constructor: a cassette from already-parsed entries.
    pub fn from_entries(replay_of: Uuid, list: Vec<CassetteEntry>) -> Self {
        let mut entries = HashMap::new();
        for entry in list {
            entries.entry(entry.req_hash.clone()).or_insert(entry);
        }
        Self { replay_of, entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(url: &str, engine: &'static str, html: &str) -> FetchOutcome {
        FetchOutcome {
            url: url.into(),
            engine,
            status: Some(200),
            html: Some(html.into()),
            markdown: None,
            text: None,
            escalations: Vec::new(),
            trace: Vec::new(),
            cost_usd: None,
            snapshot: None,
        }
    }

    #[test]
    fn req_hash_is_stable_and_distinct() {
        let a = req_hash(METHOD_GET, "https://x/a");
        assert_eq!(a, req_hash(METHOD_GET, "https://x/a"), "stable");
        assert_ne!(a, req_hash(METHOD_GET, "https://x/b"), "url-distinct");
        assert_ne!(
            a,
            req_hash(METHOD_RESEARCH, "https://x/a"),
            "method-distinct"
        );
    }

    #[tokio::test]
    async fn record_load_resolve_roundtrip_reconstructs_the_outcome() {
        let dir = tempfile::tempdir().unwrap();
        let rec = Recorder::new(dir.path().to_path_buf());
        rec.record(fetch_entry(&outcome(
            "https://x/a",
            "browser",
            "<p>rendered</p>",
        )))
        .await;
        let job = Uuid::new_v4();
        let cassette = Cassette::load(dir.path(), job).await.unwrap();
        assert_eq!(cassette.len(), 1);
        let entry = cassette
            .resolve(METHOD_GET, "https://x/a", "https://x/a")
            .unwrap();
        let out = to_fetch_outcome(entry, job).unwrap();
        // Browser render replays as its final response equivalent: the
        // recorded post-render html, engine preserved, $0.
        assert_eq!(out.engine, "browser");
        assert_eq!(out.html.as_deref(), Some("<p>rendered</p>"));
        assert_eq!(out.status, Some(200));
        assert!(out.cost_usd.is_none());
        assert_eq!(out.trace.len(), 1);
        assert_eq!(out.trace[0].tier, FetchTier::Browser);
        assert!(out.trace[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("vcr replay"));
    }

    #[tokio::test]
    async fn replay_miss_is_a_typed_error_not_a_fallthrough() {
        let cassette = Cassette::from_entries(
            Uuid::new_v4(),
            vec![fetch_entry(&outcome("https://x/a", "http", "<p>a</p>"))],
        );
        let err = cassette
            .resolve(METHOD_GET, "https://x/UNRECORDED", "https://x/UNRECORDED")
            .unwrap_err();
        assert!(
            matches!(err, Error::ReplayMiss(_)),
            "miss must be the typed variant, got: {err}"
        );
        assert!(err.to_string().contains("https://x/UNRECORDED"));
    }

    #[tokio::test]
    async fn over_entry_cap_records_a_truncated_marker_that_misses_on_replay() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny per-entry cap: the body must be dropped, identity kept.
        let rec = Recorder::with_caps(dir.path().to_path_buf(), 200, TOTAL_CAP_BYTES);
        let big = "x".repeat(4096);
        rec.record(fetch_entry(&outcome("https://x/big", "http", &big)))
            .await;
        let cassette = Cassette::load(dir.path(), Uuid::new_v4()).await.unwrap();
        assert_eq!(cassette.len(), 1, "the marker itself is recorded");
        let err = cassette
            .resolve(METHOD_GET, "https://x/big", "https://x/big")
            .unwrap_err();
        assert!(matches!(err, Error::ReplayMiss(_)));
        assert!(err.to_string().contains("truncated"));
    }

    #[tokio::test]
    async fn total_cap_truncates_later_entries_but_keeps_their_identity() {
        let dir = tempfile::tempdir().unwrap();
        // First entry fits; the second would push the cassette over the total.
        let rec = Recorder::with_caps(dir.path().to_path_buf(), ENTRY_CAP_BYTES, 400);
        rec.record(fetch_entry(&outcome("https://x/1", "http", "<p>fits</p>")))
            .await;
        rec.record(fetch_entry(&outcome(
            "https://x/2",
            "http",
            &"y".repeat(600),
        )))
        .await;
        let cassette = Cassette::load(dir.path(), Uuid::new_v4()).await.unwrap();
        assert_eq!(cassette.len(), 2);
        assert!(cassette
            .resolve(METHOD_GET, "https://x/1", "https://x/1")
            .is_ok());
        let err = cassette
            .resolve(METHOD_GET, "https://x/2", "https://x/2")
            .unwrap_err();
        assert!(
            matches!(err, Error::ReplayMiss(_)),
            "over-total = truncated marker"
        );
    }

    #[tokio::test]
    async fn duplicate_requests_replay_the_first_recording() {
        let dir = tempfile::tempdir().unwrap();
        let rec = Recorder::new(dir.path().to_path_buf());
        rec.record(fetch_entry(&outcome("https://x/a", "http", "<p>first</p>")))
            .await;
        rec.record(fetch_entry(&outcome(
            "https://x/a",
            "http",
            "<p>second</p>",
        )))
        .await;
        let cassette = Cassette::load(dir.path(), Uuid::new_v4()).await.unwrap();
        let entry = cassette
            .resolve(METHOD_GET, "https://x/a", "https://x/a")
            .unwrap();
        let out = to_fetch_outcome(entry, cassette.replay_of()).unwrap();
        assert_eq!(out.html.as_deref(), Some("<p>first</p>"));
    }

    #[tokio::test]
    async fn missing_cassette_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let job = Uuid::new_v4();
        let err = Cassette::load(dir.path(), job).await.unwrap_err();
        assert!(matches!(err, Error::ReplayMiss(_)));
        assert!(err.to_string().contains(&job.to_string()));
    }

    #[test]
    fn research_entry_roundtrips_at_zero_cost() {
        let req = ResearchRequest::new("summarize https://x/a into fields");
        let out = ResearchOutput {
            text: "{\"a\":1}".into(),
            json: Some(serde_json::json!({"a": 1})),
            cost_usd: Some(0.42),
            duration_ms: Some(900),
            num_turns: Some(3),
            session_id: Some("s".into()),
        };
        let entry = research_entry("canonical-key", &req, &out);
        assert_eq!(entry.method, METHOD_RESEARCH);
        assert_eq!(entry.req_hash, req_hash(METHOD_RESEARCH, "canonical-key"));
        let replayed = to_research_output(&entry, Uuid::new_v4()).unwrap();
        assert_eq!(replayed.text, out.text);
        assert_eq!(replayed.json, out.json);
        assert_eq!(
            replayed.cost_usd,
            Some(0.0),
            "replay is $0, whatever the original cost"
        );
    }

    #[test]
    fn unknown_engine_string_is_a_typed_error() {
        let mut entry = fetch_entry(&outcome("https://x/a", "http", "<p>a</p>"));
        entry.engine = "warp_drive".into();
        let err = to_fetch_outcome(&entry, Uuid::new_v4()).unwrap_err();
        assert!(matches!(err, Error::ReplayMiss(_)));
    }
}
