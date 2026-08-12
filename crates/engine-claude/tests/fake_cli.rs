//! The engine's first tests of `research()` itself, driven by a **fake CLI**: a
//! temp script that plays the part of `claude` (prints an envelope, exits
//! non-zero, emits garbage, or hangs while leaking a child process). No network,
//! no API key, no spend — so none of these are `#[ignore]`d.
//!
//! The script goes through the *same* launch path the real binary does: on
//! Windows a `.cmd` is not a PE image, so it is launched through the `cmd.exe`
//! shim — which is precisely the path the process-tree bug lived on.

use std::path::{Path, PathBuf};
use std::time::Duration;

use pumper_core::config::ClaudeConfig;
use pumper_core::{ResearchRequest, Researcher};
use pumper_engine_claude::ClaudeEngine;

/// A temp-dir fake `claude`, removed on drop.
struct FakeCli {
    /// Held for its `Drop`: the script (and any fixture beside it) must outlive
    /// the engine call, and the directory goes away with this value.
    _dir: tempfile::TempDir,
    binary: PathBuf,
}

impl FakeCli {
    fn temp_dir(tag: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("pumper-fakecli-{tag}-"))
            .tempdir()
            .expect("temp dir")
    }

    /// Writes a fake CLI whose body is `body` (shell lines, no shebang/`@echo
    /// off` — those are added per platform).
    fn new(tag: &str, body: &str) -> Self {
        Self::in_dir(Self::temp_dir(tag), body)
    }

    fn in_dir(dir: tempfile::TempDir, body: &str) -> Self {
        let binary = dir.path().join(if cfg!(windows) {
            "fake-claude.cmd"
        } else {
            "fake-claude.sh"
        });
        let script = if cfg!(windows) {
            format!("@echo off\r\n{}\r\n", body.replace('\n', "\r\n"))
        } else {
            format!("#!/bin/sh\n{body}\n")
        };
        std::fs::write(&binary, script).expect("write fake cli");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
                .expect("chmod fake cli");
        }
        Self { _dir: dir, binary }
    }

    /// A fake CLI that prints `envelope` on stdout and exits 0.
    fn printing(tag: &str, envelope: &serde_json::Value) -> Self {
        let dir = Self::temp_dir(tag);
        let payload = dir.path().join("envelope.json");
        std::fs::write(
            &payload,
            serde_json::to_vec_pretty(envelope).expect("envelope"),
        )
        .expect("write envelope");
        // `type`/`cat` of a file keeps the JSON out of the script's own quoting
        // rules — the point is to test the engine, not batch escaping.
        let body = if cfg!(windows) {
            format!("type \"{}\"", payload.display())
        } else {
            format!("cat \"{}\"", payload.display())
        };
        Self::in_dir(dir, &body)
    }

    fn config(&self, timeout_secs: u64) -> ClaudeConfig {
        ClaudeConfig {
            binary: self.binary.to_string_lossy().into_owned(),
            timeout_secs,
            ..ClaudeConfig::default()
        }
    }

    fn engine(&self, timeout_secs: u64) -> ClaudeEngine {
        ClaudeEngine::new(&self.config(timeout_secs))
    }
}

#[tokio::test]
async fn happy_path_envelope_becomes_a_research_output() {
    let cli = FakeCli::printing(
        "happy",
        &serde_json::json!({
            "result": "{\"answer\": 42}",
            "total_cost_usd": 0.1234,
            "duration_ms": 3210,
            "num_turns": 5,
            "session_id": "sess-abc",
            "is_error": false,
        }),
    );

    let out = cli
        .engine(30)
        .research(ResearchRequest::new("what is the answer?"))
        .await
        .expect("the envelope is well-formed");

    assert_eq!(out.text, "{\"answer\": 42}");
    assert_eq!(out.json, Some(serde_json::json!({"answer": 42})));
    assert_eq!(out.cost_usd, Some(0.1234));
    assert_eq!(out.duration_ms, Some(3210));
    assert_eq!(out.num_turns, Some(5));
    assert_eq!(out.session_id.as_deref(), Some("sess-abc"));
}

#[tokio::test]
async fn non_zero_exit_reports_the_status_and_stderr() {
    let body = if cfg!(windows) {
        "echo boom: model unavailable 1>&2\r\nexit /b 3"
    } else {
        "echo 'boom: model unavailable' 1>&2\nexit 3"
    };
    let cli = FakeCli::new("exit3", body);

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("anything"))
        .await
        .expect_err("a non-zero exit is a failure");
    let msg = err.to_string();
    assert!(msg.contains("exited with"), "no exit status in: {msg}");
    assert!(
        msg.contains("model unavailable"),
        "stderr was dropped from: {msg}"
    );
}

#[tokio::test]
async fn malformed_stdout_names_itself_instead_of_looking_like_an_empty_answer() {
    let cli = FakeCli::new("garbage", "echo not json at all");

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("anything"))
        .await
        .expect_err("garbage on stdout is a failure, not an empty answer");
    let msg = err.to_string();
    assert!(msg.contains("unparseable"), "unhelpful message: {msg}");
    assert!(
        msg.contains("not json"),
        "the raw output is not shown: {msg}"
    );
}

// ---------------------------------------------------------------------------
// The process-tree kill
// ---------------------------------------------------------------------------

/// Kills anything still running under the marker image name when the test ends —
/// a failed assertion must not leave a 300-second process behind.
#[cfg(windows)]
struct OrphanReaper(String);

#[cfg(windows)]
impl Drop for OrphanReaper {
    fn drop(&mut self) {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/IM", &self.0])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// Whether a process with this image name is currently running.
#[cfg(windows)]
fn image_running(image: &str) -> bool {
    let out = std::process::Command::new("tasklist")
        .args(["/FI", &format!("IMAGENAME eq {image}"), "/NH"])
        .output()
        .expect("tasklist");
    String::from_utf8_lossy(&out.stdout).contains(image)
}

/// Polls until `image` reaches `want`, up to `tries` × 200ms. Bounded retries,
/// not a fixed sleep: the kill is asynchronous, and a fixed sleep either flakes
/// or wastes wall-clock.
#[cfg(windows)]
async fn poll_image(image: &str, want: bool, tries: u32) -> bool {
    for _ in 0..tries {
        if image_running(image) == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

/// THE anti-pattern this direction exists for: on the default Windows config
/// (`binary = "claude"`, an npm shim) the engine spawns `cmd.exe /C claude …`,
/// and `kill_on_drop` kills only that `cmd.exe`. The real CLI behind it — the
/// process holding the API key — was re-parented and kept running its whole
/// agentic loop, spending real money that nothing would ever meter or notice.
///
/// The fake CLI stands in for that: it launches a uniquely-named long-lived
/// process (a renamed copy of `ping.exe`, so the test can find it by image name
/// without racing on pids) and then blocks past the engine's deadline.
#[cfg(windows)]
#[tokio::test]
async fn timeout_kills_the_whole_process_tree_not_just_the_shim() {
    let marker_name = format!("pumper-orphan-{}.exe", std::process::id());
    let _reaper = OrphanReaper(marker_name.clone());

    let dir = tempfile::Builder::new()
        .prefix("pumper-fakecli-tree-")
        .tempdir()
        .expect("temp dir");
    let marker = dir.path().join(&marker_name);
    let system_root = std::env::var("SystemRoot").unwrap_or_else(|_| "C:\\Windows".into());
    std::fs::copy(
        Path::new(&system_root).join("System32").join("ping.exe"),
        &marker,
    )
    .expect("copy the marker exe");

    // One backgrounded grandchild (the orphan the old code leaked) plus one
    // foreground child that keeps the shim alive past the deadline.
    let body = format!(
        "start \"\" /B \"{m}\" -n 300 127.0.0.1 >nul 2>&1\r\n\"{m}\" -n 300 127.0.0.1 >nul 2>&1",
        m = marker.display()
    );
    let cli = FakeCli::new("tree", &body);
    let engine = cli.engine(2);

    let run = tokio::spawn(async move { engine.research(ResearchRequest::new("hang")).await });

    assert!(
        poll_image(&marker_name, true, 25).await,
        "the fake cli never started its child — the test would pass vacuously"
    );
    let err = run.await.expect("join").expect_err("the run must time out");

    assert!(
        poll_image(&marker_name, false, 25).await,
        "{marker_name} survived the timeout — the process tree was orphaned, \
         which on the real CLI means an agentic loop still spending money"
    );
    let msg = err.to_string();
    assert!(msg.contains("timed out"), "not a timeout error: {msg}");
    assert!(
        msg.contains("process tree killed"),
        "the error must name what was done to the tree, so an operator can tell \
         this from the old orphan behaviour: {msg}"
    );
}
