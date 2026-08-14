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
        Self::printing_then_exit(tag, envelope, 0)
    }

    /// A fake CLI that prints `envelope` on stdout and then exits `code` — the
    /// real CLI's "complete envelope, non-zero exit" shape.
    fn printing_then_exit(tag: &str, envelope: &serde_json::Value, code: i32) -> Self {
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
            format!("type \"{}\"\r\nexit /b {code}", payload.display())
        } else {
            format!("cat \"{}\"\nexit {code}", payload.display())
        };
        Self::in_dir(dir, &body)
    }

    /// A fake CLI that records what it was actually handed — its working
    /// directory in `cwd.txt`, plus argv two ways — beside the envelope it then
    /// prints. This is the only way to assert what crossed the `cmd.exe` shim,
    /// which is where the mangling lives.
    ///
    /// **Two recordings, because on Windows neither alone is honest.**
    /// `argv.txt` holds batch's own tokens (`%~1`), which are convenient for
    /// "was this flag passed" but are *not* what the real CLI sees: batch splits
    /// tokens on commas, so a JSON schema arrives in `argv.txt` in pieces even
    /// when it crossed the shim perfectly. `cmdline.txt` holds the raw remainder
    /// (`%*`) — byte-for-byte what the npm shim forwards to `node`, which then
    /// applies MSVCRT parsing — so it is the fidelity oracle. Assert *content*
    /// against `cmdline.txt` and *presence* against `argv.txt`.
    fn recording(tag: &str, envelope: &serde_json::Value) -> Self {
        let dir = Self::temp_dir(tag);
        let payload = dir.path().join("envelope.json");
        std::fs::write(
            &payload,
            serde_json::to_vec_pretty(envelope).expect("envelope"),
        )
        .expect("write envelope");
        let argv = dir.path().join("argv.txt");
        let cmdline = dir.path().join("cmdline.txt");
        let cwd = dir.path().join("cwd.txt");
        // Redirections lead each line: `echo %*>"f"` would read as a *stderr*
        // redirect whenever the value happens to end in a digit.
        let body = if cfg!(windows) {
            format!(
                "> \"{cwd}\" cd\n\
                 > \"{cmdline}\" echo %*\n\
                 break > \"{argv}\"\n\
                 :next\n\
                 if \"%~1\"==\"\" goto done\n\
                 >> \"{argv}\" echo %~1\n\
                 shift\n\
                 goto next\n\
                 :done\n\
                 type \"{payload}\"",
                cwd = cwd.display(),
                cmdline = cmdline.display(),
                argv = argv.display(),
                payload = payload.display()
            )
        } else {
            format!(
                "pwd > \"{cwd}\"\n\
                 printf '%s\\n' \"$*\" > \"{cmdline}\"\n\
                 : > \"{argv}\"\n\
                 for a in \"$@\"; do printf '%s\\n' \"$a\" >> \"{argv}\"; done\n\
                 cat \"{payload}\"",
                cwd = cwd.display(),
                cmdline = cmdline.display(),
                argv = argv.display(),
                payload = payload.display()
            )
        };
        Self::in_dir(dir, &body)
    }

    /// Absolute path of a file the recording fake wrote beside itself.
    fn artifact(&self, name: &str) -> PathBuf {
        self.binary.parent().expect("script dir").join(name)
    }

    /// The argv the fake CLI actually received, one value per line.
    fn recorded_argv(&self) -> Vec<String> {
        let raw = std::fs::read_to_string(self.artifact("argv.txt"))
            .expect("the fake cli did not record its argv — did it run at all?");
        raw.lines().map(|l| l.trim_end().to_string()).collect()
    }

    /// The raw command tail the CLI was handed, with the shim's `\"` escaping
    /// undone — i.e. the string an MSVCRT argv parser reconstructs. On POSIX
    /// there is no escaping and the unescape is a no-op.
    fn recorded_command_line(&self) -> String {
        std::fs::read_to_string(self.artifact("cmdline.txt"))
            .expect("the fake cli did not record its command line — did it run at all?")
            .trim()
            .replace("\\\"", "\"")
    }

    fn recorded_cwd(&self) -> PathBuf {
        let raw = std::fs::read_to_string(self.artifact("cwd.txt")).expect("cwd.txt");
        PathBuf::from(raw.trim())
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

/// An envelope the engine accepts, for tests whose subject is the *input* side.
fn ok_envelope() -> serde_json::Value {
    serde_json::json!({"is_error": false, "result": "ok", "total_cost_usd": 0.01})
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
// Cost honesty: money the CLI reports on a failure path is real money
// ---------------------------------------------------------------------------

/// THE anti-pattern: the `is_error` envelope carries `total_cost_usd` in the
/// very same object the engine threw away, so the runs that spent the most were
/// the ones the ledger never heard about.
#[tokio::test]
async fn an_is_error_envelope_carries_its_cost_out_of_the_failure() {
    let cli = FakeCli::printing(
        "is-error",
        &serde_json::json!({
            "is_error": true,
            "result": "hit the model's own budget ceiling",
            "total_cost_usd": 0.87,
        }),
    );

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("an expensive question"))
        .await
        .expect_err("an is_error envelope is a failure");
    let spend = err.claude_spend().expect("a claude failure carries spend");
    assert_eq!(
        spend.cost_usd,
        Some(0.87),
        "the money the CLI reported burning was discarded with the envelope"
    );
    assert_eq!(spend.class, pumper_core::error::ClaudeFailure::CliError);
}

/// A non-zero exit does not mean stdout was worthless: the CLI routinely prints
/// a complete envelope and *then* exits non-zero. Discarding stdout discarded
/// the cost with it.
#[tokio::test]
async fn a_non_zero_exit_keeps_the_cost_its_envelope_reported() {
    let cli = FakeCli::printing_then_exit(
        "exit-with-envelope",
        &serde_json::json!({
            "is_error": true,
            "result": "crashed after spending",
            "total_cost_usd": 0.31,
        }),
        7,
    );

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("a question"))
        .await
        .expect_err("a non-zero exit is a failure");
    let spend = err.claude_spend().expect("a claude failure carries spend");
    assert_eq!(
        spend.cost_usd,
        Some(0.31),
        "stdout was discarded on the non-zero-exit path, and the cost with it"
    );
    assert_eq!(spend.class, pumper_core::error::ClaudeFailure::NonZeroExit);
}

/// The whole failure surface must be able to answer "how much did that cost?" —
/// including with "nothing was reported", which is a different answer from `$0`.
#[tokio::test]
async fn a_failure_without_an_envelope_reports_unknown_spend_not_zero() {
    let cli = FakeCli::new("no-envelope", "echo not json at all");

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("a question"))
        .await
        .expect_err("garbage on stdout is a failure");
    let spend = err.claude_spend().expect("a claude failure carries spend");
    assert_eq!(
        spend.cost_usd, None,
        "an unreadable envelope reports an UNKNOWN spend, never a metered $0"
    );
    assert_eq!(spend.class, pumper_core::error::ClaudeFailure::Unparseable);
}

/// THE anti-pattern for schema-constrained calls: `result.as_str()` on an
/// object yields `""`, and the research cache refuses to store an empty answer —
/// so the call re-paid the model on every repeat, with nothing anywhere saying
/// why. A non-string result must still produce cacheable text.
#[tokio::test]
async fn schema_result_is_not_silently_empty_and_uncacheable() {
    let cli = FakeCli::printing(
        "schema",
        &serde_json::json!({
            "is_error": false,
            "result": {"state": "CA", "rate": 13.3},
            "structured_output": {"state": "CA", "rate": 13.3},
            "total_cost_usd": 0.05,
        }),
    );

    let mut req = ResearchRequest::new("top marginal rate?");
    req.json_schema = Some(serde_json::json!({"type": "object"}));
    let out = cli
        .engine(30)
        .research(req)
        .await
        .expect("a valid envelope");

    assert!(
        !out.text.is_empty(),
        "a schema-constrained answer came back as empty text — the research \
         cache refuses to store that, so the call re-pays the model forever"
    );
    assert_eq!(
        out.json,
        Some(serde_json::json!({"state": "CA", "rate": 13.3})),
        "the validated structured output is still the parsed answer"
    );
}

// ---------------------------------------------------------------------------
// Subprocess hygiene: what the engine hands the CLI
// ---------------------------------------------------------------------------

/// THE anti-pattern: free-form prose went out as `--append-system-prompt <text>`,
/// straight into cmd.exe's re-parse. "R&D" truncated the value and ran the rest
/// as a command; "100% of" expanded an environment variable into it. The text now
/// travels by file, so no amount of prose can reach the command line.
#[tokio::test]
async fn the_system_prompt_travels_by_file_not_argv() {
    let cli = FakeCli::recording("sysprompt", &ok_envelope());
    let hostile = "Cite R&D spend >50% of revenue | flag ^outliers^";

    let mut req = ResearchRequest::new("q");
    req.append_system_prompt = Some(hostile.to_string());
    cli.engine(30)
        .research(req)
        .await
        .expect("prose full of shell metacharacters must not fail the run");

    let argv = cli.recorded_argv();
    assert!(
        !argv.iter().any(|a| a == "--append-system-prompt"),
        "the prose is back on the command line: {argv:?}"
    );
    let at = argv
        .iter()
        .position(|a| a == "--append-system-prompt-file")
        .expect("the system prompt must be passed by file");
    let path = argv.get(at + 1).expect("a path follows the flag");
    assert!(
        !cli.recorded_command_line().contains("R&D"),
        "the prose itself reached the command line: {}",
        cli.recorded_command_line()
    );
    // The engine deletes the file when the run ends, so the assertion that it
    // carried the *exact* text is on the path it handed over.
    assert!(
        path.ends_with(".txt") && path.contains("sysprompt"),
        "not a scratch file path: {path}"
    );
}

/// The scratch file is not litter: it holds an operator's prompt text, and one
/// per research call would accumulate forever under the storage root.
#[tokio::test]
async fn the_system_prompt_file_is_deleted_after_the_run() {
    let workdir = tempfile::Builder::new()
        .prefix("pumper-claude-cwd-")
        .tempdir()
        .expect("temp dir");
    let cli = FakeCli::recording("sysprompt-cleanup", &ok_envelope());
    let cfg = ClaudeConfig {
        isolation_dir: Some(workdir.path().to_path_buf()),
        ..cli.config(30)
    };

    let mut req = ResearchRequest::new("q");
    req.append_system_prompt = Some("some instructions".into());
    ClaudeEngine::new(&cfg).research(req).await.expect("a run");

    let leftovers: Vec<_> = std::fs::read_dir(workdir.path())
        .expect("read workdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("pumper-sysprompt-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "the system-prompt file outlived its run: {leftovers:?}"
    );
}

/// THE anti-pattern: the subprocess inherited the server's CWD, so in dev every
/// scraping research call discovered *this repo's* CLAUDE.md, skills and Stop
/// hooks — paid context that has nothing to do with the job. An explicit,
/// dedicated working directory is what stops that discovery starting here.
#[tokio::test]
async fn the_subprocess_runs_in_its_own_dir_not_the_servers_cwd() {
    let workdir = tempfile::Builder::new()
        .prefix("pumper-claude-cwd-")
        .tempdir()
        .expect("temp dir");
    // A dir the engine must create itself — the storage root exists long before
    // anything asks Claude a question.
    let nested = workdir.path().join("claude-cwd");
    let cli = FakeCli::recording("cwd", &ok_envelope());
    let cfg = ClaudeConfig {
        isolation_dir: Some(nested.clone()),
        ..cli.config(30)
    };

    ClaudeEngine::new(&cfg)
        .research(ResearchRequest::new("q"))
        .await
        .expect("a run");

    assert!(nested.is_dir(), "the working dir was not created");
    let seen = cli.recorded_cwd();
    assert_eq!(
        seen.canonicalize().expect("canonicalize seen"),
        nested.canonicalize().expect("canonicalize expected"),
        "the subprocess did not run in its isolation dir"
    );
    assert_ne!(
        seen.canonicalize().ok(),
        std::env::current_dir()
            .ok()
            .and_then(|d| d.canonicalize().ok()),
        "the subprocess inherited the server's CWD"
    );
}

/// Unset isolation dir = today's behaviour, which is what every other test in
/// this file (and every ClaudeConfig built by hand) relies on.
#[tokio::test]
async fn no_isolation_dir_keeps_the_inherited_cwd() {
    let cli = FakeCli::recording("cwd-inherited", &ok_envelope());
    cli.engine(30)
        .research(ResearchRequest::new("q"))
        .await
        .expect("a run");

    assert_eq!(
        cli.recorded_cwd().canonicalize().ok(),
        std::env::current_dir()
            .ok()
            .and_then(|d| d.canonicalize().ok()),
        "an unset isolation dir must not move the subprocess"
    );
}

/// A schema cmd.exe would corrupt must be refused *before* anything spawns — a
/// mangled schema still costs a full run, and the answer would be validated
/// against a schema the app never wrote.
#[cfg(windows)]
#[tokio::test]
async fn a_hostile_schema_is_refused_before_the_cli_runs() {
    let cli = FakeCli::recording("hostile-schema", &ok_envelope());

    let mut req = ResearchRequest::new("q");
    req.json_schema = Some(serde_json::json!({
        "type": "object",
        "description": "R&D > 50% of revenue",
    }));
    let err = cli
        .engine(30)
        .research(req)
        .await
        .expect_err("a schema cmd.exe would mangle must be refused");

    let msg = err.to_string();
    assert!(
        msg.contains("--json-schema"),
        "the flag is not named: {msg}"
    );
    assert!(
        !cli.artifact("argv.txt").exists(),
        "the CLI ran anyway — a refusal that still spawns still spends"
    );
    assert_eq!(
        err.claude_spend().expect("a claude failure").class,
        pumper_core::error::ClaudeFailure::Spawn,
        "nothing ran, so the ledger must record no spend at all"
    );
}

/// An ordinary schema must still reach the CLI intact: the guard's failure mode
/// with the highest blast radius is refusing the six apps that send one.
#[tokio::test]
async fn an_ordinary_schema_still_reaches_the_cli() {
    let cli = FakeCli::recording("ok-schema", &ok_envelope());

    let schema = serde_json::json!({
        "type": "object",
        "properties": {"state": {"type": "string"}},
        "required": ["state"],
    });
    let mut req = ResearchRequest::new("q");
    req.json_schema = Some(schema.clone());
    cli.engine(30).research(req).await.expect("a valid schema");

    assert!(
        cli.recorded_argv().iter().any(|a| a == "--json-schema"),
        "the schema flag never reached the CLI"
    );
    // Content fidelity is asserted against the raw tail, not batch's tokens —
    // see `FakeCli::recording`. The schema must arrive character-for-character:
    // a schema that survives "mostly" still constrains the answer to something
    // the app did not ask for.
    assert!(
        cli.recorded_command_line().contains(&schema.to_string()),
        "the schema was mangled crossing the shim: {}",
        cli.recorded_command_line()
    );
}

/// THE anti-pattern: a typo'd role resolved to `None` and fell through to the
/// config defaults, so the job succeeded having bought a different model at a
/// different effort than the caller asked for.
#[tokio::test]
async fn an_unknown_role_is_an_error_not_a_silent_default() {
    let cli = FakeCli::recording("bad-role", &ok_envelope());

    let err = cli
        .engine(30)
        .research(ResearchRequest::new("q").with_role("reserch"))
        .await
        .expect_err("an unconfigured role must not resolve to the defaults");

    let msg = err.to_string();
    assert!(msg.contains("reserch"), "the typo is not echoed: {msg}");
    assert!(
        msg.contains("research") && msg.contains("compose"),
        "the message must name the configured roles: {msg}"
    );
    assert!(
        !cli.artifact("argv.txt").exists(),
        "the CLI ran with the wrong preset anyway"
    );
}

/// The compatibility half of the same door: a request with no role at all is the
/// common case and keeps working, on the config defaults.
#[tokio::test]
async fn a_request_with_no_role_still_runs() {
    let cli = FakeCli::recording("no-role", &ok_envelope());
    let out = cli
        .engine(30)
        .research(ResearchRequest::new("q"))
        .await
        .expect("a request without a role is valid");
    assert_eq!(out.text, "ok");
}

/// A configured role resolves and reaches the CLI as the model it names.
#[tokio::test]
async fn a_configured_role_reaches_the_cli_as_its_model() {
    let cli = FakeCli::recording("good-role", &ok_envelope());
    cli.engine(30)
        .research(ResearchRequest::new("q").with_role("compose"))
        .await
        .expect("a configured role resolves");

    let argv = cli.recorded_argv();
    let at = argv.iter().position(|a| a == "--model").expect("--model");
    assert_eq!(
        argv.get(at + 1).map(String::as_str),
        Some("claude-opus-4-8"),
        "the compose role's model did not reach the CLI: {argv:?}"
    );
}

/// `model` is a free string from the `POST /jobs` body all the way to `--model`.
/// A garbage value must be refused at the door, not become a subprocess parse
/// error (or worse) after a process has already started.
#[tokio::test]
async fn a_garbage_model_is_refused_before_spawning() {
    let cli = FakeCli::recording("bad-model", &ok_envelope());

    let mut req = ResearchRequest::new("q");
    req.model = Some("opus --dangerously-skip-permissions".into());
    let err = cli
        .engine(30)
        .research(req)
        .await
        .expect_err("a free-string model must be validated at the engine door");

    assert!(err.to_string().contains("refusing model"), "{err}");
    assert!(
        !cli.artifact("argv.txt").exists(),
        "the CLI was spawned with an unvalidated model"
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
    let marker = marker_exe(dir.path(), &marker_name);

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

/// Copies `ping.exe` under a unique image name, so a test can find *its own*
/// long-lived process by name without racing on pids.
#[cfg(windows)]
fn marker_exe(dir: &Path, name: &str) -> PathBuf {
    let marker = dir.join(name);
    let system_root = std::env::var("SystemRoot").unwrap_or_else(|_| "C:\\Windows".into());
    std::fs::copy(
        Path::new(&system_root).join("System32").join("ping.exe"),
        &marker,
    )
    .expect("copy the marker exe");
    marker
}

/// THE bug this direction exists for, and the one no test anywhere reached: a
/// cancelled job does not time out — its future is simply **dropped**.
/// `crates/server/src/worker.rs` races the app future against the cancel token
/// and `break`s out of its `select!` without polling the run again, so every
/// path the engine defended (timeout, wait error, drain timeout) is skipped and
/// the future's `Drop` is all that runs. `kill_on_drop` alone kills the `cmd.exe`
/// shim; the `claude`/node grandchild behind it — the process holding the API
/// key — was re-parented and kept running its whole agentic loop. `DELETE
/// /jobs/{id}` answered `cancelled`, the job row said `cancelled`, and the bill
/// kept climbing.
///
/// The timeout is deliberately far longer than the test: if the deadline is what
/// kills the tree, this test is not testing the drop.
#[cfg(windows)]
#[tokio::test]
async fn dropping_the_run_kills_the_tree_not_only_the_shim() {
    let marker_name = format!("pumper-dropped-{}.exe", std::process::id());
    let _reaper = OrphanReaper(marker_name.clone());

    let dir = tempfile::Builder::new()
        .prefix("pumper-fakecli-drop-")
        .tempdir()
        .expect("temp dir");
    let marker = marker_exe(dir.path(), &marker_name);

    // Same shape as the timeout test: one backgrounded grandchild (the orphan
    // that keeps spending) plus one foreground child holding the shim open.
    let body = format!(
        "start \"\" /B \"{m}\" -n 300 127.0.0.1 >nul 2>&1\r\n\"{m}\" -n 300 127.0.0.1 >nul 2>&1",
        m = marker.display()
    );
    let cli = FakeCli::new("dropped", &body);
    // 600s: an eternity next to this test, so nothing here can be the deadline
    // path in disguise.
    let engine = cli.engine(600);

    let mut run = Box::pin(engine.research(ResearchRequest::new("hang")));
    // Poll the future far enough to have spawned the CLI *and* its grandchild,
    // racing it against the marker appearing — exactly the state a job is in
    // when a cancel lands.
    let started = tokio::select! {
        _ = &mut run => false,
        ok = poll_image(&marker_name, true, 50) => ok,
    };
    assert!(
        started,
        "the fake cli never started its child — the test would pass vacuously"
    );

    // THE MOMENT UNDER TEST. No timeout, no error path, no `?`: the future is
    // dropped and never polled again, which is what the worker's `select!` does.
    drop(run);

    assert!(
        poll_image(&marker_name, false, 50).await,
        "{marker_name} survived the DROP of the research future — on the real CLI \
         that is an agentic loop still running, and still spending, after the API \
         answered `cancelled`"
    );
}
