//! Sanitized environments for child processes that run over **untrusted content**.
//!
//! The engine crates spawn children — the Claude CLI (`engine-claude`), and the
//! headless browser (`engine-browser`) — whose whole job is to process scraped
//! web pages. A child that inherits pumper's full process environment inherits
//! **every secret in it**: the Anthropic auth, `SENTRY_DSN`, per-app keys like
//! `CENSUS_API_KEY`, and any webhook signing secret loaded from `.env`
//! (`server/src/main.rs` `load_dotenv`). An indirect prompt-injection that
//! induces such a child (or a tool it calls) to read and echo its `env` is a
//! realistic exfiltration path.
//!
//! So a child's environment is **built, not inherited**: clear everything, then
//! re-add only the allowlist below. The allowlist is the guard — it is
//! deliberately small (the OS essentials a process needs to start, plus
//! `PATH`/home/locale/tmp) and carries **no application secret**. A child that
//! legitimately needs one secret (the Claude CLI needs its own Anthropic auth
//! when the operator authenticates by key rather than by `claude login`) is
//! handed **that one** by name via `extra` — never the rest.
//!
//! This is the single source of truth for that policy; the engines apply it to
//! their own `Command` (`cmd.env_clear(); cmd.envs(allowed_env(..))`). Keeping
//! the allowlist here — and pinned by [`tests`] — means "which env a child over
//! untrusted content may see" is one reviewable decision, not two drifting ones.

/// Platform baseline: the variables any child needs to start and find its way
/// around the machine. **No application secret appears here.**
///
/// Windows: the loader needs `SYSTEMROOT`/`WINDIR`, the cmd.exe shim path needs
/// `COMSPEC`/`PATHEXT`, and the Claude CLI reads its config/credentials under
/// `%USERPROFILE%\.claude` (hence `USERPROFILE`/`APPDATA`/`LOCALAPPDATA`).
#[cfg(windows)]
const PLATFORM_ALLOWED: &[&str] = &[
    "PATH",
    "PATHEXT",
    "COMSPEC",
    "SYSTEMROOT",
    "SYSTEMDRIVE",
    "WINDIR",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "PROGRAMFILES",
    "PROGRAMFILES(X86)",
    "PROCESSOR_ARCHITECTURE",
    "NUMBER_OF_PROCESSORS",
    "LANG",
    "TZ",
];

/// Platform baseline (Unix): `PATH` to spawn, `HOME` for `~/.claude`, a scratch
/// dir, plus the usual identity/locale vars a well-behaved CLI expects.
#[cfg(not(windows))]
const PLATFORM_ALLOWED: &[&str] = &[
    "PATH", "HOME", "TMPDIR", "USER", "LOGNAME", "SHELL", "TERM", "TZ", "LANG", "LANGUAGE",
    "LC_ALL", "LC_CTYPE",
];

/// The Anthropic/Claude auth the Claude CLI legitimately needs when the operator
/// authenticates by key rather than by `claude login` (which stores its token
/// under `HOME` instead, already covered by the baseline). Handed **only** to the
/// Claude child — never to the browser.
pub const CLAUDE_EXTRA_ENV: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
];

/// What the headless browser needs beyond the baseline: the X display handles on
/// Unix. The proxy is passed as a `--proxy-server` flag, not via env, so it does
/// not appear here. Carries no secret.
pub const BROWSER_EXTRA_ENV: &[&str] = &["DISPLAY", "XAUTHORITY"];

/// The allowlisted `(name, value)` pairs read from the **current** process
/// environment: the platform baseline plus any `extra` names, skipping those
/// that are unset. Everything not named is dropped.
///
/// Apply it as `cmd.env_clear(); cmd.envs(allowed_env(extra));` — the
/// `env_clear` is what removes the inherited secrets, and this is what adds back
/// only what the child needs.
pub fn allowed_env(extra: &[&str]) -> Vec<(String, String)> {
    PLATFORM_ALLOWED
        .iter()
        .chain(extra.iter())
        .filter_map(|&key| std::env::var(key).ok().map(|val| (key.to_string(), val)))
        .collect()
}

/// Whether `name` is on the platform baseline or in `extra` — i.e. whether
/// [`allowed_env`] would carry it. The inventory predicate the regression tests
/// assert against.
pub fn is_allowed(name: &str, extra: &[&str]) -> bool {
    PLATFORM_ALLOWED.contains(&name) || extra.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// THE invariant this module exists for: a secret the parent process holds
    /// (from `.env` or the ambient shell) must **not** cross into a child that
    /// runs over untrusted scraped content. Set representative secrets on the
    /// parent, build the sanitized env, and prove none survive — while the
    /// essentials the child needs to start do.
    #[test]
    fn a_sanitized_env_drops_secrets_the_parent_holds() {
        // Simulate pumper's own process holding secrets (the shapes `load_dotenv`
        // puts on the environment). Names chosen not to collide with any other
        // test's env mutations.
        std::env::set_var("SENTRY_DSN", "https://public@o0.ingest.sentry.io/0");
        std::env::set_var("CENSUS_API_KEY", "super-secret-census-key");
        std::env::set_var(
            "PUMPER_WEBHOOK_SIGNING_SECRET",
            "whsec_live_should_not_leak",
        );

        let env = allowed_env(CLAUDE_EXTRA_ENV);
        let names: HashSet<&str> = env.iter().map(|(k, _)| k.as_str()).collect();

        // None of the parent's secrets crossed into the child's environment.
        for forbidden in [
            "SENTRY_DSN",
            "CENSUS_API_KEY",
            "PUMPER_WEBHOOK_SIGNING_SECRET",
        ] {
            assert!(
                !names.contains(forbidden),
                "{forbidden} leaked into a child that runs over untrusted content"
            );
        }

        // Every key that IS present is on the allowlist — the child's env is the
        // allowlist and nothing else.
        for (key, _) in &env {
            assert!(
                is_allowed(key, CLAUDE_EXTRA_ENV),
                "{key} appeared in the sanitized env but is not on the allowlist"
            );
        }

        // And the one baseline the child cannot even spawn without survived.
        assert!(names.contains("PATH"), "a child with no PATH cannot spawn");
    }

    /// The allowlist is a closed set: no application secret is ever on it, for
    /// either child. This is a pure inventory check (no process-env mutation),
    /// so it states the policy independent of the machine it runs on.
    #[test]
    fn no_application_secret_is_ever_allowlisted() {
        for secret in [
            "SENTRY_DSN",
            "CENSUS_API_KEY",
            "PUMPER_WEBHOOK_SIGNING_SECRET",
            "AWS_SECRET_ACCESS_KEY",
            "DATABASE_URL",
        ] {
            assert!(
                !is_allowed(secret, CLAUDE_EXTRA_ENV),
                "{secret} must never reach the Claude child"
            );
            assert!(
                !is_allowed(secret, BROWSER_EXTRA_ENV),
                "{secret} must never reach the browser child"
            );
        }
    }

    /// The one secret the Claude CLI legitimately needs is allowlisted for Claude
    /// and ONLY for Claude — the browser never needs Anthropic auth, so it must
    /// not receive it.
    #[test]
    fn anthropic_auth_is_claude_only() {
        assert!(
            is_allowed("ANTHROPIC_API_KEY", CLAUDE_EXTRA_ENV),
            "the Claude CLI must still be able to authenticate by key"
        );
        assert!(
            !is_allowed("ANTHROPIC_API_KEY", BROWSER_EXTRA_ENV),
            "the browser has no business holding the Anthropic key"
        );
    }
}
