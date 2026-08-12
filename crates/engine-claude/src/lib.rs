//! Claude Code CLI as a scraping engine: spawns `claude -p --output-format json`
//! headlessly and returns the agent's research result. The prompt is piped via
//! stdin, which sidesteps Windows command-line length limits and cmd.exe
//! quoting entirely.

use std::process::Stdio;
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
    fn resolve(&self, req: &ResearchRequest) -> Resolved {
        let role = req.role.as_deref().and_then(|r| self.cfg.roles.get(r));
        Resolved {
            model: req
                .model
                .clone()
                .or_else(|| role.and_then(|r| r.model.clone()))
                .or_else(|| self.cfg.model.clone()),
            effort: req
                .effort
                .clone()
                .or_else(|| role.and_then(|r| r.effort.clone()))
                .or_else(|| self.cfg.effort.clone()),
            max_budget_usd: req
                .max_budget_usd
                .or_else(|| role.and_then(|r| r.max_budget_usd))
                .or(self.cfg.max_budget_usd),
        }
    }

    fn command(&self, req: &ResearchRequest, resolved: &Resolved) -> Command {
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
        // Caveat: these travel as cmd.exe arguments on Windows; exotic shell
        // metacharacters may be mangled. Prefer folding instructions into the
        // prompt itself, which goes over stdin.
        if let Some(system) = &req.append_system_prompt {
            args.push("--append-system-prompt".into());
            args.push(system.clone());
        }

        // npm installs `claude` as .ps1/.cmd shims, which CreateProcess cannot
        // spawn directly — route through cmd.exe unless pointed at a real .exe.
        let mut cmd = if cfg!(windows) && !self.cfg.binary.to_lowercase().ends_with(".exe") {
            let mut c = Command::new("cmd");
            c.arg("/C").arg(&self.cfg.binary);
            c
        } else {
            Command::new(&self.cfg.binary)
        };
        cmd.args(&args);
        cmd
    }
}

#[async_trait]
impl Researcher for ClaudeEngine {
    async fn research(&self, req: ResearchRequest) -> Result<ResearchOutput> {
        let timeout = Duration::from_secs(req.timeout_secs.unwrap_or(self.cfg.timeout_secs));
        let resolved = self.resolve(&req);
        debug!(
            model = resolved.model.as_deref().unwrap_or("<default>"),
            effort = resolved.effort.as_deref().unwrap_or("<default>"),
            "resolved claude run"
        );
        let mut cmd = self.command(&req, &resolved);
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

/// Effective model/effort/budget after merging request, role, and config.
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
    use super::{envelope_text, parse_loose_json, tree_kill_argv};
    use serde_json::json;

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
