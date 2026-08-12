//! Claude Code CLI as a scraping engine: spawns `claude -p --output-format json`
//! headlessly and returns the agent's research result. The prompt is piped via
//! stdin, which sidesteps Windows command-line length limits and cmd.exe
//! quoting entirely.

use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use pumper_core::config::ClaudeConfig;
use pumper_core::error::ClaudeFailure;
use pumper_core::{Error, ResearchOutput, ResearchRequest, Researcher, Result};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

pub struct ClaudeEngine {
    cfg: ClaudeConfig,
}

impl ClaudeEngine {
    pub fn new(cfg: &ClaudeConfig) -> Self {
        Self { cfg: cfg.clone() }
    }

    /// Resolves model/effort/budget from the request's explicit fields, then
    /// its role preset, then the config defaults (in that precedence).
    ///
    /// Fails on a role nobody configured. That used to resolve to `None` and
    /// fall straight through to the config defaults, so a typo'd `role` in a
    /// `POST /jobs` body silently bought a *different model at a different
    /// effort* than the caller asked for — the job succeeded, the bill was
    /// wrong, and nothing anywhere said so.
    fn resolve(&self, req: &ResearchRequest) -> Result<Resolved> {
        let role = match req.role.as_deref() {
            None => None,
            Some(name) => Some(self.cfg.roles.get(name).ok_or_else(|| {
                Error::claude(
                    ClaudeFailure::Spawn,
                    unknown_role_message(name, self.cfg.roles.keys().map(String::as_str)),
                )
            })?),
        };
        let model = req
            .model
            .clone()
            .or_else(|| role.and_then(|r| r.model.clone()))
            .or_else(|| self.cfg.model.clone());
        if let Some(model) = model.as_deref().filter(|m| !is_plain_model_id(m)) {
            return Err(Error::claude(
                ClaudeFailure::Spawn,
                format!(
                    "refusing model {model:?}: a model id may only contain letters, digits, \
                     '.', '_', ':' and '-' (at most {MAX_MODEL_ID_CHARS} of them). It reaches \
                     the CLI as `--model <value>`, so anything else is a subprocess parse \
                     error at best"
                ),
            ));
        }
        Ok(Resolved {
            model,
            effort: req
                .effort
                .clone()
                .or_else(|| role.and_then(|r| r.effort.clone()))
                .or_else(|| self.cfg.effort.clone()),
            max_budget_usd: req
                .max_budget_usd
                .or_else(|| role.and_then(|r| r.max_budget_usd))
                .or(self.cfg.max_budget_usd),
        })
    }

    /// The directory the subprocess runs in, and the scratch root for files the
    /// engine hands it. Created on demand; `None` keeps the server's own CWD.
    fn workdir(&self) -> Result<Option<PathBuf>> {
        let Some(dir) = self.cfg.isolation_dir.as_ref() else {
            return Ok(None);
        };
        std::fs::create_dir_all(dir).map_err(|e| {
            Error::claude(
                ClaudeFailure::Spawn,
                format!(
                    "could not create the claude working dir {}: {e}",
                    dir.display()
                ),
            )
        })?;
        Ok(Some(dir.clone()))
    }

    fn command(&self, req: &ResearchRequest, resolved: &Resolved) -> Result<Launch> {
        let workdir = self.workdir()?;
        let scratch_root = workdir.clone().unwrap_or_else(std::env::temp_dir);
        let mut scratch: Vec<ScratchFile> = Vec::new();
        let mut args: Vec<String> = vec!["-p".into(), "--output-format".into(), "json".into()];
        if let Some(model) = &resolved.model {
            args.push("--model".into());
            args.push(model.clone());
        }
        if let Some(effort) = &resolved.effort {
            args.push("--effort".into());
            args.push(effort.clone());
        }
        if let Some(budget) = resolved.max_budget_usd {
            args.push("--max-budget-usd".into());
            args.push(format!("{budget}"));
        }
        if self.cfg.bare {
            args.push("--bare".into());
        }
        if self.cfg.skip_permissions {
            args.push("--dangerously-skip-permissions".into());
        }
        if !self.cfg.allowed_tools.is_empty() {
            args.push("--allowedTools".into());
            args.push(self.cfg.allowed_tools.join(","));
        }
        if let Some(turns) = req.max_turns {
            args.push("--max-turns".into());
            args.push(turns.to_string());
        }
        if let Some(session) = &req.resume_session {
            args.push("--resume".into());
            args.push(session.clone());
        }
        if let Some(schema) = &req.json_schema {
            args.push("--json-schema".into());
            args.push(schema.to_string());
        }
        // Free-form operator prose is the single most cmd.exe-hostile thing the
        // engine handles ("R&D", "100% of", "cost > value" all mangle or inject),
        // so it does not travel on argv at all: `--append-system-prompt-file`
        // takes a path, and the CLI reads the text itself. Only the path is left
        // for the guard below to vet.
        if let Some(system) = &req.append_system_prompt {
            let file = ScratchFile::write(&scratch_root, "sysprompt", system)?;
            args.push("--append-system-prompt-file".into());
            args.push(file.path().to_string_lossy().into_owned());
            scratch.push(file);
        }

        // npm installs `claude` as .ps1/.cmd shims, which CreateProcess cannot
        // spawn directly — route through cmd.exe unless pointed at a real .exe.
        let via_shim = cfg!(windows) && !self.cfg.binary.to_lowercase().ends_with(".exe");
        let mut cmd = if via_shim {
            // Only this path re-parses. Vetting argv unconditionally would refuse
            // schemas that a direct `execve`/`CreateProcess` delivers byte-exact.
            check_shim_argv(&self.cfg.binary, &args)
                .map_err(|refusal| Error::claude(ClaudeFailure::Spawn, refusal.to_string()))?;
            let mut c = Command::new("cmd");
            c.arg("/C").arg(&self.cfg.binary);
            c
        } else {
            Command::new(&self.cfg.binary)
        };
        cmd.args(&args);
        if let Some(dir) = &workdir {
            cmd.current_dir(dir);
        }
        Ok(Launch { cmd, scratch })
    }
}

/// A command plus the scratch files that must outlive the process reading them.
struct Launch {
    cmd: Command,
    scratch: Vec<ScratchFile>,
}

/// A file handed to the subprocess by path, removed when the run ends —
/// including on every early-return path, which is the point of the `Drop`.
struct ScratchFile(PathBuf);

impl ScratchFile {
    fn write(dir: &Path, kind: &str, body: &str) -> Result<Self> {
        // pid + counter: two concurrent research jobs in one process must not
        // hand the CLI the same path, and a stale file from a previous run of a
        // recycled pid must not be readable as this run's prompt.
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = dir.join(format!(
            "pumper-{kind}-{}-{}.txt",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, body).map_err(|e| {
            Error::claude(
                ClaudeFailure::Spawn,
                format!("could not write {kind} file {}: {e}", path.display()),
            )
        })?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[async_trait]
impl Researcher for ClaudeEngine {
    async fn research(&self, req: ResearchRequest) -> Result<ResearchOutput> {
        let timeout = Duration::from_secs(req.timeout_secs.unwrap_or(self.cfg.timeout_secs));
        let resolved = self.resolve(&req)?;
        debug!(
            model = resolved.model.as_deref().unwrap_or("<default>"),
            effort = resolved.effort.as_deref().unwrap_or("<default>"),
            "resolved claude run"
        );
        // Held for the whole call: the CLI reads the system-prompt file at
        // startup, and dropping this is what deletes it afterwards.
        let Launch {
            mut cmd,
            scratch: _scratch,
        } = self.command(&req, &resolved)?;
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // Backstop only. It kills the DIRECT child, which on the Windows
            // shim path is `cmd.exe` and not the process that spends money —
            // see `kill_process_tree`.
            .kill_on_drop(true);

        debug!(timeout_secs = timeout.as_secs(), "spawning claude cli");
        let mut child = cmd.spawn().map_err(|e| {
            Error::claude(
                ClaudeFailure::Spawn,
                format!("failed to spawn '{}': {e}", self.cfg.binary),
            )
        })?;
        // Captured BEFORE anything can reap the process: `Child::id` returns
        // `None` once the child has been waited on, and the tree kill needs the
        // shim's pid. The live `Child` handle is also what keeps Windows from
        // recycling that pid — killing a *recycled* pid's tree would be far
        // worse than the leak this fixes — so the kill must run while the handle
        // is still held.
        let pid = child.id();

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::claude(ClaudeFailure::Spawn, "no stdin handle"))?;
        let mut stdout_pipe = child
            .stdout
            .take()
            .ok_or_else(|| Error::claude(ClaudeFailure::Spawn, "no stdout handle"))?;
        let mut stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| Error::claude(ClaudeFailure::Spawn, "no stderr handle"))?;

        let prompt = req.prompt.clone();
        let writer = tokio::spawn(async move {
            let _ = stdin.write_all(prompt.as_bytes()).await;
            let _ = stdin.shutdown().await;
        });
        // Drained concurrently with the wait, because a child that fills its
        // stdout pipe blocks forever if nobody reads it. `wait_with_output` used
        // to do this for us — but it CONSUMES the child, which left
        // `kill_on_drop` as the only possible cleanup. Reading the pipes by hand
        // is the price of keeping `child` borrowable, and a borrowable child is
        // what makes an explicit process-tree kill possible at all.
        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            let _ = stdout_pipe.read_to_end(&mut buf).await;
            buf
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            let _ = stderr_pipe.read_to_end(&mut buf).await;
            buf
        });
        let side = [
            writer.abort_handle(),
            stdout_task.abort_handle(),
            stderr_task.abort_handle(),
        ];

        // ONE deadline governs the whole run — the wait AND the drain — so an
        // orphan holding the stdout pipe open cannot park this call past the
        // caller's timeout after the CLI itself is gone.
        let deadline = tokio::time::Instant::now() + timeout;
        let status = match tokio::time::timeout_at(deadline, child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(e)) => {
                return Err(abandon_run(&mut child, pid, &side, format!("cli failed: {e}")).await)
            }
            Err(_) => {
                return Err(abandon_run(
                    &mut child,
                    pid,
                    &side,
                    format!(
                        "timed out after {}s waiting for the cli to exit",
                        timeout.as_secs()
                    ),
                )
                .await)
            }
        };
        // The child is gone, so the writer's pipe is closed and the task is
        // finished or about to be — this cannot hang.
        let _ = writer.await;
        let stdout_bytes = match tokio::time::timeout_at(deadline, stdout_task).await {
            Ok(Ok(buf)) => buf,
            Ok(Err(e)) => {
                return Err(Error::claude(
                    ClaudeFailure::Unparseable,
                    format!("stdout reader failed: {e}"),
                ))
            }
            Err(_) => {
                return Err(abandon_run(
                    &mut child,
                    pid,
                    &side,
                    format!(
                        "timed out after {}s draining cli stdout — a spawned process is still \
                         holding the pipe open",
                        timeout.as_secs()
                    ),
                )
                .await)
            }
        };
        // stderr only decorates an error message; never fail the run over it.
        let stderr_bytes = match tokio::time::timeout_at(deadline, stderr_task).await {
            Ok(Ok(buf)) => buf,
            _ => Vec::new(),
        };

        let stdout = String::from_utf8_lossy(&stdout_bytes);
        // A non-zero exit does NOT mean stdout is worthless: the CLI routinely
        // prints a complete envelope — cost and all — and then exits non-zero.
        // Discarding stdout here threw that spend away.
        let parsed = serde_json::from_str::<Value>(stdout.trim());
        let reported_cost = parsed
            .as_ref()
            .ok()
            .and_then(|e| e["total_cost_usd"].as_f64());
        if !status.success() {
            let stderr = String::from_utf8_lossy(&stderr_bytes);
            return Err(Error::claude_spent(
                ClaudeFailure::NonZeroExit,
                reported_cost,
                format!("exited with {}: {}", status, truncate(&stderr, 2000)),
            ));
        }

        let envelope = parsed.map_err(|e| {
            Error::claude(
                ClaudeFailure::Unparseable,
                format!("unparseable cli output: {e}: {}", truncate(&stdout, 500)),
            )
        })?;
        if envelope["is_error"].as_bool() == Some(true) {
            // The most expensive failure shape there is: the run *happened*, so
            // the envelope's cost is money already spent. It rides out on the
            // error and the chokepoint meters it.
            return Err(Error::claude_spent(
                ClaudeFailure::CliError,
                reported_cost,
                format!("cli reported error: {}", envelope["result"]),
            ));
        }

        let text = envelope_text(&envelope);
        // Prefer the CLI's validated structured output when a schema was set;
        // otherwise best-effort parse JSON out of the free-form result.
        let json = match envelope.get("structured_output") {
            Some(value) if !value.is_null() => Some(value.clone()),
            _ => parse_loose_json(&text),
        };
        info!(
            cost_usd = envelope["total_cost_usd"].as_f64(),
            num_turns = envelope["num_turns"].as_u64(),
            structured = json.is_some(),
            "claude research finished"
        );

        Ok(ResearchOutput {
            text,
            json,
            cost_usd: envelope["total_cost_usd"].as_f64(),
            duration_ms: envelope["duration_ms"].as_u64(),
            num_turns: envelope["num_turns"].as_u64(),
            session_id: envelope["session_id"].as_str().map(String::from),
        })
    }
}

/// Ends a run that will not be waited on: aborts the side tasks, kills the whole
/// process tree, and mints the error that says so.
///
/// The stdin writer is aborted rather than left behind: it is parked on
/// `write_all` into a pipe nobody will ever read again, and dropping the future
/// (the old behaviour) never even reached the `writer.await` below it — `?`
/// returned first, so the task leaked for the life of the process.
async fn abandon_run(
    child: &mut Child,
    pid: Option<u32>,
    side: &[tokio::task::AbortHandle],
    why: String,
) -> Error {
    for task in side {
        task.abort();
    }
    let outcome = kill_process_tree(child, pid).await;
    // Unreported by construction: a killed run produces no envelope, so what it
    // spent is unknowable here. The chokepoint still records THAT it happened
    // (`unmetered_timeout`) rather than leaving the ledger silent.
    Error::claude(ClaudeFailure::Timeout, format!("{why}; {outcome}"))
}

/// Kills the child **and everything it spawned**, returning what was done for
/// the error message (an operator has to be able to tell a killed tree from the
/// orphan-and-hope behaviour this replaces).
///
/// On the Windows shim path the direct child is `cmd.exe`, and the process that
/// holds the API key and burns money is its `claude`/node grandchild.
/// `TerminateProcess` — which is all `Child::kill` and `kill_on_drop` do — kills
/// only the shim: the grandchild is re-parented, keeps running its whole agentic
/// loop, keeps spending, and has nobody left to reap it. `taskkill /T` walks the
/// live parent/child snapshot and kills the tree from the root down, which is
/// exactly the shape this engine creates (shim → cli → tool subprocesses, all
/// alive and still linked at kill time).
///
/// **Why not a Job Object.** `CreateJobObject` +
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is stronger — it survives re-parenting
/// and needs no snapshot walk — but it costs a native `windows` dependency and
/// unsafe handle plumbing in an engine whose entire job is spawning ONE
/// cooperative CLI that does not try to escape. `taskkill` covers every tree
/// this engine creates, at zero dependency cost.
///
/// On POSIX there is no shim: the child IS the CLI, so killing it stops the
/// spend at the source, and `Child::kill` is the whole mechanism.
async fn kill_process_tree(child: &mut Child, pid: Option<u32>) -> String {
    let mut outcome = "direct child killed".to_string();
    if let Some((program, args)) = pid.and_then(tree_kill_argv) {
        let killed = Command::new(program)
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .status()
            .await;
        outcome = match killed {
            Ok(status) if status.success() => "process tree killed".to_string(),
            // Exit 128 = "no such process": it had already exited on its own.
            Ok(status) => format!("process tree kill reported {status}"),
            Err(e) => {
                warn!(?pid, "process tree kill failed: {e}");
                format!("process tree kill FAILED ({e}) — a spawned process may still be running")
            }
        };
    }
    let _ = child.start_kill();
    // Reap, so the child is not left a zombie. Bounded: a kill that somehow did
    // not land must not hang the caller past its own deadline.
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
    outcome
}

/// The command that kills the whole process tree rooted at `pid`, or `None`
/// where killing the direct child already IS the whole tree (POSIX: no shim).
///
/// Extracted as a pure function so the flags are asserted in a test instead of
/// being reviewed once — dropping `/T` here silently restores the orphan bug,
/// and nothing else in the system would notice.
fn tree_kill_argv(pid: u32) -> Option<(&'static str, Vec<String>)> {
    #[cfg(windows)]
    {
        Some((
            "taskkill",
            vec!["/PID".into(), pid.to_string(), "/T".into(), "/F".into()],
        ))
    }
    #[cfg(not(windows))]
    {
        let _ = pid;
        None
    }
}

// ---------------------------------------------------------------------------
// What may cross the cmd.exe shim
// ---------------------------------------------------------------------------

/// Characters cmd.exe re-interprets when it re-parses the line on the shim path.
///
/// **Measured** (Windows 11, `cmd /C <shim>` with argv echoed back by node), not
/// taken from a table — the results differ from the obvious guess in both
/// directions:
///
/// - `&` truncates the value and runs the remainder as a *second command*.
/// - `|`, `>` hijack the invocation into a pipe/redirect; the CLI never runs.
/// - `^` is cmd's escape character and is **silently eaten** (`a ^ b` arrives as
///   `a  b`) — the worst shape here: no error, a corrupted schema, money spent.
/// - `%` expands: `%TEMP%` arrived as `C:\Users\…\Temp`, and `%PATH%` inlined the
///   entire PATH into the argument. Mangling *and* an environment leak into a
///   value that reaches the model.
/// - `<` survived one probe and broke another (it depends on cmd's quote state at
///   that point, which `\"`-escaped JSON desynchronises) — unreliable is refused.
/// - A newline truncates the value; a carriage return is dropped silently.
///
/// Deliberately **absent: the double quote**. Refusing `"` was the tempting rule
/// and it would have broken every schema-using app: a real JSON schema — quotes,
/// braces, brackets, colons, commas, backslashes, non-ASCII — round-trips
/// byte-exact through the shim, and six production apps depend on that. A lone
/// `"` cannot mangle anything by itself; it only desynchronises quote state,
/// which matters solely for the characters already refused above.
const CMD_HOSTILE: &[char] = &['%', '&', '|', '<', '>', '^', '\n', '\r'];

/// Budget for the whole rendered `cmd /C …` line.
///
/// Measured cliff: a line built from an 8008-character value went through, 8108
/// failed with "The command line is too long" — consistent with cmd.exe's
/// documented 8191 ceiling. The budget keeps ~190 characters of headroom for
/// quoting the probe could not exercise.
const MAX_SHIM_COMMAND_LINE: usize = 8000;

/// Longest model id accepted. Real ids are ~20 characters; this only has to be
/// short enough that a runaway string is refused before it reaches argv.
const MAX_MODEL_ID_CHARS: usize = 128;

/// Why an argument cannot cross the shim. Typed so the refusal names the flag —
/// "argument rejected" would leave an operator guessing which of nine it was.
#[derive(Debug, PartialEq, Eq)]
enum ShimRefusal {
    Metacharacter { flag: String, ch: char },
    TooLong { flag: String, width: usize },
}

impl fmt::Display for ShimRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metacharacter { flag, ch } => write!(
                f,
                "refusing to run: the value for `{flag}` contains {ch:?}, which cmd.exe \
                 re-interprets when it re-parses the command line for the `claude` shim \
                 (it would be mangled, or run as a separate command). Fold the text into \
                 the prompt, which is piped over stdin, or point `[claude] binary` at a \
                 real .exe to bypass the shim"
            ),
            Self::TooLong { flag, width } => write!(
                f,
                "refusing to run: the command line reaches {width} characters by `{flag}`, \
                 over the {MAX_SHIM_COMMAND_LINE}-character budget for the cmd.exe shim \
                 (cmd.exe truncates at 8191). Shrink the schema, or point `[claude] binary` \
                 at a real .exe to bypass the shim"
            ),
        }
    }
}

/// Conservative rendered width of one argv value once Rust has quoted it for
/// `CreateProcess`, plus the separating space. Over-estimating is the safe
/// direction: a refusal is loud, a truncated command line is not.
fn shim_arg_width(value: &str) -> usize {
    let escaped = value.chars().filter(|c| matches!(c, '"' | '\\')).count();
    value.chars().count() + escaped + 3
}

/// The offending character, if this value cannot survive cmd.exe's re-parse.
fn cmd_hostile_char(value: &str) -> Option<char> {
    value.chars().find(|c| CMD_HOSTILE.contains(c))
}

/// Vets everything destined for the shim, naming the flag that owns the offending
/// value. **Refuses; never sanitises.** Stripping or escaping would hand the model
/// a schema that is not the one the app wrote, and a wrong answer that looks right
/// is worse than a failed job.
fn check_shim_argv(binary: &str, args: &[String]) -> std::result::Result<(), ShimRefusal> {
    // `cmd /C ` plus the shim path are on the line too, and a binary path is as
    // capable of holding a `&` as any other value.
    let mut flag = "[claude] binary".to_string();
    let mut width = "cmd /C ".len() + shim_arg_width(binary);
    if let Some(ch) = cmd_hostile_char(binary) {
        return Err(ShimRefusal::Metacharacter { flag, ch });
    }
    for arg in args {
        // Values follow their flag, so the most recent flag names the offender.
        if arg.starts_with('-') {
            flag = arg.clone();
        }
        if let Some(ch) = cmd_hostile_char(arg) {
            return Err(ShimRefusal::Metacharacter { flag, ch });
        }
        width += shim_arg_width(arg);
        if width > MAX_SHIM_COMMAND_LINE {
            return Err(ShimRefusal::TooLong { flag, width });
        }
    }
    Ok(())
}

/// Whether a model id is plain enough to hand to `--model` unquoted-in-spirit.
/// A pattern check, deliberately **not** a catalogue of known ids: new models
/// ship constantly and an allowlist would reject them for a week each time.
fn is_plain_model_id(model: &str) -> bool {
    !model.is_empty()
        && model.chars().count() <= MAX_MODEL_ID_CHARS
        && model
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

/// Names what the operator could have meant. A bare "unknown role" is useless
/// when the roles live in a config file the caller may never have read.
fn unknown_role_message<'a>(name: &str, known: impl Iterator<Item = &'a str>) -> String {
    let mut names: Vec<&str> = known.collect();
    names.sort_unstable();
    if names.is_empty() {
        format!("unknown claude role {name:?}: no roles are configured under [claude.roles]")
    } else {
        format!(
            "unknown claude role {name:?}; configured roles are: {}",
            names.join(", ")
        )
    }
}

/// Effective model/effort/budget after merging request, role, and config.
#[derive(Debug)]
struct Resolved {
    model: Option<String>,
    effort: Option<String>,
    max_budget_usd: Option<f64>,
}

/// The answer text for an envelope whose `result` is **not necessarily a
/// string**.
///
/// Under `--json-schema` the CLI may return `result` as an object/array. The old
/// `as_str().unwrap_or_default()` turned exactly those answers into `""` — and
/// an empty text is what the research cache refuses to store, so every repeat of
/// a schema-constrained call re-paid the model and nothing anywhere said why.
/// A non-string result falls back to the validated `structured_output`, then to
/// the raw `result` value, serialized.
fn envelope_text(envelope: &Value) -> String {
    if let Some(text) = envelope["result"].as_str() {
        return text.to_string();
    }
    let fallback = match envelope.get("structured_output") {
        Some(value) if !value.is_null() => Some(value),
        _ => envelope.get("result").filter(|v| !v.is_null()),
    };
    fallback
        .and_then(|v| serde_json::to_string(v).ok())
        .unwrap_or_default()
}

/// Accepts raw JSON, JSON in markdown fences, or a JSON object/array embedded
/// in surrounding prose — agents love to add a lead-in sentence.
fn parse_loose_json(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    if let Ok(value) = serde_json::from_str(trimmed) {
        return Some(value);
    }
    if let Some(inner) = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|rest| rest.strip_suffix("```"))
    {
        if let Ok(value) = serde_json::from_str(inner.trim()) {
            return Some(value);
        }
    }
    extract_embedded_json(trimmed, '{', '}').or_else(|| extract_embedded_json(trimmed, '[', ']'))
}

/// Tries the outermost `open`..`close` span, then shrinks from the right —
/// handles both "prose then JSON" and "JSON then prose".
fn extract_embedded_json(text: &str, open: char, close: char) -> Option<Value> {
    let start = text.find(open)?;
    let mut end = text.len();
    loop {
        let slice = &text[start..end];
        let candidate_end = slice.rfind(close)?;
        let candidate = &slice[..=candidate_end];
        if let Ok(value) = serde_json::from_str(candidate) {
            return Some(value);
        }
        end = start + candidate_end;
    }
}

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        check_shim_argv, envelope_text, is_plain_model_id, parse_loose_json, tree_kill_argv,
        unknown_role_message, ClaudeEngine, ShimRefusal, MAX_SHIM_COMMAND_LINE,
    };
    use pumper_core::config::{ClaudeConfig, ClaudeRole};
    use pumper_core::ResearchRequest;
    use serde_json::json;

    fn argv(pairs: &[&str]) -> Vec<String> {
        pairs.iter().map(|s| s.to_string()).collect()
    }

    /// THE anti-pattern this guard exists for: cmd.exe re-parses the shim line,
    /// so these characters do not arrive as written. Each was *measured* through
    /// a real `cmd /C` round-trip — `&` splits the value and runs the tail as a
    /// command, `^` is eaten in silence, `%TEMP%` expands to a path. Mangling is
    /// worse than failing, because a corrupted schema still costs a full run.
    #[test]
    fn cmd_args_refused_not_mangled() {
        for ch in ['%', '&', '|', '<', '>', '^', '\n', '\r'] {
            let schema = format!("{{\"d\":\"a{ch}b\"}}");
            let err = check_shim_argv("claude", &argv(&["--json-schema", &schema]))
                .expect_err("this character does not survive cmd.exe and must be refused");
            assert_eq!(
                err,
                ShimRefusal::Metacharacter {
                    flag: "--json-schema".into(),
                    ch
                },
                "the refusal must name the offending flag and character"
            );
            assert!(
                err.to_string().contains("--json-schema"),
                "an operator cannot act on a refusal that hides the flag: {err}"
            );
        }
    }

    /// The over-refusal that would have been worse than the bug: a real JSON
    /// schema is *made of* double quotes, and six production apps send one on
    /// every call. Measured byte-exact through `cmd /C`, so it must pass.
    #[test]
    fn a_real_schema_still_crosses_the_shim() {
        let schema = r#"{"type":"object","properties":{"state":{"type":"string"},"rate":{"type":"number"}},"required":["state","rate"],"note":"C:\\x — café (a=b; y!)"}"#;
        assert!(
            check_shim_argv("claude", &argv(&["--json-schema", schema])).is_ok(),
            "refusing quotes/braces/backslashes would break every schema-using app"
        );
    }

    /// A binary path is an argv value like any other — and it is the one value an
    /// operator sets by hand in config.
    #[test]
    fn a_hostile_binary_path_is_refused_too() {
        let err = check_shim_argv(r"C:\tools\claude&calc\claude.cmd", &argv(&["-p"]))
            .expect_err("a binary path holding '&' runs a second command");
        assert!(err.to_string().contains("[claude] binary"), "{err}");
    }

    /// Over the ceiling cmd.exe silently truncates at, so the CLI would receive
    /// half a schema. Refusing names a number an operator can act on.
    #[test]
    fn an_oversized_command_line_is_refused_not_truncated() {
        let huge = "a".repeat(MAX_SHIM_COMMAND_LINE + 10);
        let err = check_shim_argv("claude", &argv(&["--json-schema", &huge]))
            .expect_err("cmd.exe cannot carry a line this long");
        assert!(
            matches!(err, ShimRefusal::TooLong { .. }),
            "wrong refusal: {err:?}"
        );
        assert!(err.to_string().contains("--json-schema"), "{err}");
    }

    /// The budget is a *line* budget, not a per-argument one: many medium
    /// arguments overflow cmd.exe exactly as one huge argument does.
    #[test]
    fn the_budget_counts_the_whole_line_not_one_arg() {
        let chunk = "b".repeat(1000);
        let mut args = Vec::new();
        for _ in 0..9 {
            args.push("--append".to_string());
            args.push(chunk.clone());
        }
        assert!(
            check_shim_argv("claude", &args).is_err(),
            "nine 1000-char values fit under no 8000-character line"
        );
    }

    #[test]
    fn garbage_model_is_refused_not_handed_to_the_subprocess() {
        assert!(is_plain_model_id("claude-sonnet-5"));
        assert!(is_plain_model_id("us.anthropic.claude-opus-4-8:0"));
        assert!(!is_plain_model_id(""), "an empty --model value is garbage");
        assert!(
            !is_plain_model_id("sonnet 5"),
            "a space splits the argument"
        );
        assert!(!is_plain_model_id("sonnet&calc"), "shell metacharacter");
        assert!(!is_plain_model_id("--dangerously-skip-permissions x"));
        assert!(!is_plain_model_id(&"a".repeat(500)));
    }

    #[test]
    fn unknown_role_message_names_the_known_roles() {
        let msg = unknown_role_message("reserch", ["compose", "research"].into_iter());
        assert!(msg.contains("reserch"), "the typo is not echoed: {msg}");
        assert!(msg.contains("compose") && msg.contains("research"), "{msg}");
        let empty = unknown_role_message("research", std::iter::empty());
        assert!(empty.contains("[claude.roles]"), "{empty}");
    }

    // -----------------------------------------------------------------------
    // resolve() precedence — request > role > config, per field independently
    // -----------------------------------------------------------------------

    fn engine_with_roles() -> ClaudeEngine {
        let mut cfg = ClaudeConfig {
            model: Some("config-model".into()),
            effort: Some("config-effort".into()),
            max_budget_usd: Some(1.0),
            ..ClaudeConfig::default()
        };
        cfg.roles.clear();
        cfg.roles.insert(
            "compose".into(),
            ClaudeRole {
                model: Some("role-model".into()),
                effort: Some("role-effort".into()),
                max_budget_usd: Some(2.0),
            },
        );
        ClaudeEngine::new(&cfg)
    }

    #[test]
    fn config_defaults_apply_when_nothing_overrides_them() {
        let resolved = engine_with_roles()
            .resolve(&ResearchRequest::new("q"))
            .expect("a request with no role is valid");
        assert_eq!(resolved.model.as_deref(), Some("config-model"));
        assert_eq!(resolved.effort.as_deref(), Some("config-effort"));
        assert_eq!(resolved.max_budget_usd, Some(1.0));
    }

    #[test]
    fn a_role_overrides_the_config_defaults() {
        let resolved = engine_with_roles()
            .resolve(&ResearchRequest::new("q").with_role("compose"))
            .expect("a configured role resolves");
        assert_eq!(resolved.model.as_deref(), Some("role-model"));
        assert_eq!(resolved.effort.as_deref(), Some("role-effort"));
        assert_eq!(resolved.max_budget_usd, Some(2.0));
    }

    /// Per field *independently*: a request that sets only `model` must keep the
    /// role's effort and budget, not reset them to the config defaults.
    #[test]
    fn a_request_field_overrides_only_its_own_field() {
        let mut req = ResearchRequest::new("q").with_role("compose");
        req.model = Some("request-model".into());
        let resolved = engine_with_roles().resolve(&req).expect("valid");
        assert_eq!(resolved.model.as_deref(), Some("request-model"));
        assert_eq!(
            resolved.effort.as_deref(),
            Some("role-effort"),
            "an explicit model must not discard the role's effort"
        );
        assert_eq!(resolved.max_budget_usd, Some(2.0));
    }

    /// A request field beats the role even when the role sets it too — and the
    /// other two fields still come from the role.
    #[test]
    fn a_request_beats_the_role_on_every_field() {
        let mut req = ResearchRequest::new("q").with_role("compose");
        req.effort = Some("request-effort".into());
        req.max_budget_usd = Some(3.0);
        let resolved = engine_with_roles().resolve(&req).expect("valid");
        assert_eq!(resolved.model.as_deref(), Some("role-model"));
        assert_eq!(resolved.effort.as_deref(), Some("request-effort"));
        assert_eq!(resolved.max_budget_usd, Some(3.0));
    }

    /// THE anti-pattern: an unknown role resolved to `None` and fell through to
    /// the config defaults, so a typo bought a different model at a different
    /// effort and the job *succeeded* with the wrong bill.
    #[test]
    fn an_unknown_role_is_refused_not_silently_defaulted() {
        let err = engine_with_roles()
            .resolve(&ResearchRequest::new("q").with_role("compoze"))
            .expect_err("a typo'd role must not resolve to the config defaults");
        let msg = err.to_string();
        assert!(msg.contains("compoze"), "{msg}");
        assert!(
            msg.contains("compose"),
            "the message must list what IS configured: {msg}"
        );
    }

    #[test]
    fn a_garbage_model_is_refused_at_the_engine_door() {
        let mut req = ResearchRequest::new("q");
        req.model = Some("sonnet 5; rm -rf /".into());
        let err = engine_with_roles()
            .resolve(&req)
            .expect_err("a free-string model must not reach --model");
        assert!(err.to_string().contains("refusing model"), "{err}");
    }

    /// The ordinary shape: a string result travels verbatim.
    #[test]
    fn string_result_is_the_text() {
        assert_eq!(
            envelope_text(&json!({"result": "plain prose"})),
            "plain prose"
        );
    }

    /// THE anti-pattern: `result.as_str().unwrap_or_default()` on a
    /// schema-constrained object produced `""` — which the research cache
    /// refuses to store, so the call re-paid the model on every repeat with no
    /// signal anywhere.
    #[test]
    fn schema_result_is_not_silently_empty_and_uncacheable() {
        let text = envelope_text(&json!({
            "result": {"state": "CA", "rate": 13.3},
            "structured_output": {"state": "CA", "rate": 13.3},
        }));
        assert!(!text.is_empty(), "an object result became empty text");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap(),
            json!({"state": "CA", "rate": 13.3})
        );
    }

    /// Same failure without a `structured_output` to fall back on: the raw
    /// result value is still an answer, and still better than `""`.
    #[test]
    fn non_string_result_without_structured_output_still_has_text() {
        let text = envelope_text(&json!({"result": [1, 2, 3]}));
        assert_eq!(text, "[1,2,3]");
    }

    /// The honest empty case stays empty — no answer is not an answer.
    #[test]
    fn a_missing_result_is_still_empty() {
        assert_eq!(envelope_text(&json!({"num_turns": 3})), "");
        assert_eq!(envelope_text(&json!({"result": null})), "");
    }

    /// The anti-pattern: killing the `cmd.exe` shim and calling it done. The
    /// grandchild behind the shim is the process holding the API key, and it
    /// keeps running its agentic loop — and keeps spending — unless the kill
    /// walks the tree. `/T` is that walk; `/F` is what makes it unconditional.
    #[cfg(windows)]
    #[test]
    fn tree_kill_targets_the_tree_not_only_the_shim() {
        let (program, args) = tree_kill_argv(4242).expect("windows kills the tree explicitly");
        assert_eq!(program, "taskkill");
        assert!(args.contains(&"/T".to_string()), "no tree walk: {args:?}");
        assert!(args.contains(&"/F".to_string()), "not forced: {args:?}");
        let pid_at = args.iter().position(|a| a == "/PID").expect("/PID flag");
        assert_eq!(
            args.get(pid_at + 1).map(String::as_str),
            Some("4242"),
            "the pid must follow /PID: {args:?}"
        );
    }

    /// POSIX spawns the CLI directly — no shim, so the direct child IS the
    /// process that spends money and `Child::kill` stops it at the source.
    /// Asserting `None` keeps that reasoning explicit rather than accidental.
    #[cfg(not(windows))]
    #[test]
    fn posix_needs_no_tree_walk_because_there_is_no_shim() {
        assert!(tree_kill_argv(4242).is_none());
    }

    #[test]
    fn raw_json() {
        assert_eq!(parse_loose_json(r#"{"a": 1}"#), Some(json!({"a": 1})));
    }

    #[test]
    fn fenced_json() {
        assert_eq!(
            parse_loose_json("```json\n{\"a\": 1}\n```"),
            Some(json!({"a": 1}))
        );
    }

    #[test]
    fn json_after_prose() {
        assert_eq!(
            parse_loose_json(r#"Both sources agree. {"summary": "x", "n": 2}"#),
            Some(json!({"summary": "x", "n": 2}))
        );
    }

    #[test]
    fn json_before_prose() {
        assert_eq!(
            parse_loose_json(r#"{"a": [1, 2]} Hope that helps!"#),
            Some(json!({"a": [1, 2]}))
        );
    }

    #[test]
    fn nested_braces_in_strings() {
        assert_eq!(
            parse_loose_json(r#"Result: {"code": "if (x) { y() }"} done"#),
            Some(json!({"code": "if (x) { y() }"}))
        );
    }

    #[test]
    fn plain_prose_is_none() {
        assert_eq!(parse_loose_json("No structured data here."), None);
    }
}
