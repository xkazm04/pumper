//! Claude Code CLI as a scraping engine: spawns `claude -p --output-format json`
//! headlessly and returns the agent's research result. The prompt is piped via
//! stdin, which sidesteps Windows command-line length limits and cmd.exe
//! quoting entirely.

use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use pumper_core::config::ClaudeConfig;
use pumper_core::{Error, Researcher, ResearchOutput, ResearchRequest, Result};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, info};

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
        let mut args: Vec<String> =
            vec!["-p".into(), "--output-format".into(), "json".into()];
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
            .kill_on_drop(true);

        debug!(timeout_secs = timeout.as_secs(), "spawning claude cli");
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::Claude(format!("failed to spawn '{}': {e}", self.cfg.binary)))?;

        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| Error::Claude("no stdin handle".into()))?;
        let prompt = req.prompt.clone();
        let writer = tokio::spawn(async move {
            let _ = stdin.write_all(prompt.as_bytes()).await;
            let _ = stdin.shutdown().await;
        });

        // On timeout the future is dropped and kill_on_drop reaps the child.
        let output = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| Error::Claude(format!("timed out after {}s", timeout.as_secs())))?
            .map_err(|e| Error::Claude(format!("cli failed: {e}")))?;
        let _ = writer.await;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Claude(format!(
                "exited with {}: {}",
                output.status,
                truncate(&stderr, 2000)
            )));
        }

        let envelope: Value = serde_json::from_str(stdout.trim()).map_err(|e| {
            Error::Claude(format!("unparseable cli output: {e}: {}", truncate(&stdout, 500)))
        })?;
        if envelope["is_error"].as_bool() == Some(true) {
            return Err(Error::Claude(format!("cli reported error: {}", envelope["result"])));
        }

        let text = envelope["result"].as_str().unwrap_or_default().to_string();
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

/// Effective model/effort/budget after merging request, role, and config.
struct Resolved {
    model: Option<String>,
    effort: Option<String>,
    max_budget_usd: Option<f64>,
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

#[cfg(test)]
mod tests {
    use super::parse_loose_json;
    use serde_json::json;

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

fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        s.to_string()
    } else {
        s.chars().take(max_chars).collect()
    }
}
