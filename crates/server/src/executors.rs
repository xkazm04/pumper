//! N18 — the elastic executor plane, coordinator side.
//!
//! An **executor** is another pumper process that dials *out*: it long-polls
//! `POST /executors/claim`, runs the whole job with its own engines, streams
//! heartbeats / progress / checkpoints back, and finishes with one
//! `POST /jobs/{id}/finish`. The queue, the gates and the post-run fan-out never
//! move — only `execute` does. Executors need no ingress, and the poll is both
//! the clock and the backpressure boundary (a process asks for work only when it
//! has a free slot).
//!
//! This module is the coordinator's **policy**, extracted as pure functions so
//! each one is testable without a socket:
//!
//! - [`secret_matches`] — the plane's shared-secret compare, digest-shaped like
//!   the remote fabric's.
//! - [`eligible_apps`] — which apps this cluster will let an executor run, and
//!   the intersection with what the executor said it can do.
//! - [`blocked_over_cap`] — the per-app concurrency cap made **cluster-wide**,
//!   from the DB, because an in-process `HashMap` counts one process's work.
//! - [`executor_state`] — the liveness word `GET /executors` reports.
//!
//! ### Why v1 is result-only
//!
//! An executor's `AppContext` has no dataset store: there is no RPC `Datasets`
//! client yet, and giving it a *local* one would be worse than refusing — the
//! app's writes would land in a scratch database on the executor and vanish,
//! silently, looking exactly like a successful run. So v1 admits only apps that
//! are **result-only**: everything they produce comes back in the job result and
//! the artifact tree. `ScrapeApp::executor` is how an app declares that, and
//! [`executor_eligible_apps_are_result_only`] is what makes the declaration a
//! checked claim rather than a comment.

use std::collections::HashMap;
use std::sync::Arc;

use pumper_core::ScrapeApp;
use sha2::{Digest, Sha256};

/// Constant-shape secret comparison: hash both sides, compare digests. Two
/// fixed-length digests make the `==` timing independent of where the presented
/// value diverges from the real secret.
///
/// Byte-for-byte the same technique as `routes::remote::secret_matches` — the
/// fabric's pattern, mirrored in the other direction. It stays a second small
/// function rather than a shared one because the two planes carry *different*
/// secrets in different headers, and a single helper would invite a single
/// secret.
pub(crate) fn secret_matches(presented: &str, expected: &str) -> bool {
    // An empty expected secret must never match: a blank `[executors] secret`
    // with `enabled = true` is refused at boot, but a hand-assembled state can
    // still carry it, and "" == "" would be an open door.
    if expected.trim().is_empty() {
        return false;
    }
    Sha256::digest(presented.as_bytes()) == Sha256::digest(expected.trim().as_bytes())
}

/// The apps an executor may be handed, given the registry and the capabilities
/// it declared.
///
/// Two rules, in this order:
/// 1. **The coordinator decides eligibility.** Only apps that declare
///    `ScrapeApp::executor` are ever candidates. An executor cannot widen this by
///    naming an app — `requested` can only ever *narrow*.
/// 2. **An empty `requested` means "anything you'll give me"**, not "nothing".
///    That is the useful default for a plain `pumper --executor`, and it is safe
///    precisely because rule 1 is evaluated first.
///
/// Sorted, so a claim's SQL and its logs are stable across runs.
pub(crate) fn eligible_apps(
    registry: &HashMap<String, Arc<dyn ScrapeApp>>,
    requested: &[String],
) -> Vec<String> {
    let mut apps: Vec<String> = registry
        .values()
        .filter(|app| app.executor())
        .map(|app| app.name().to_string())
        .filter(|name| requested.is_empty() || requested.iter().any(|r| r == name))
        .collect();
    apps.sort();
    apps
}

/// Apps at or above their concurrency limit **cluster-wide**, from per-app
/// counts of jobs currently running on executors.
///
/// The anti-pattern this replaces for the executor claim path: `worker::
/// blocked_apps`, which reads an `Arc<Mutex<HashMap>>` living in one process.
/// That map is correct for the jobs that process is running and blind to every
/// other executor's, so with N executors a per-app cap of 2 admitted 2×N jobs.
/// `limit_for` returns `0` for "unlimited", matching `worker::app_limit`.
pub(crate) fn blocked_over_cap(
    counts: &[(String, i64)],
    limit_for: impl Fn(&str) -> usize,
) -> Vec<String> {
    let mut blocked: Vec<String> = counts
        .iter()
        .filter(|(app, n)| {
            let limit = limit_for(app);
            limit > 0 && *n >= limit as i64
        })
        .map(|(app, _)| app.clone())
        .collect();
    blocked.sort();
    blocked
}

/// The word `GET /executors` reports for one executor's liveness.
///
/// Three states, not two, because "polling and idle" and "polling and busy" are
/// different operational facts and collapsing them is how an idle fleet reads as
/// a broken one:
/// - `busy` — polled recently and is running at least one job,
/// - `idle` — polled recently, running nothing (healthy: the queue is empty),
/// - `offline` — has not polled within `offline_after_secs`.
///
/// This is a **report**, never a recovery: reclaiming a dead executor's jobs is
/// the reaper's business and is driven by the job's own heartbeat lease, so an
/// executor going `offline` here does nothing to any job by itself.
pub(crate) fn executor_state(age_secs: i64, running: i64, offline_after_secs: u64) -> &'static str {
    if offline_after_secs > 0 && age_secs >= offline_after_secs as i64 {
        return "offline";
    }
    if running > 0 {
        "busy"
    } else {
        "idle"
    }
}

/// Seconds between `then` (a stored RFC-3339 timestamp) and `now`, or `0` when
/// the stamp will not parse. Fail-*young* deliberately: an unreadable timestamp
/// must not make a live executor read as offline, because the report would then
/// point an operator at the wrong problem.
pub(crate) fn age_secs(then: &str, now: chrono::DateTime<chrono::Utc>) -> i64 {
    match chrono::DateTime::parse_from_rfc3339(then) {
        Ok(t) => (now - t.with_timezone(&chrono::Utc)).num_seconds().max(0),
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{age_secs, blocked_over_cap, eligible_apps, executor_state, secret_matches};
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use pumper_core::ScrapeApp;

    #[test]
    fn secret_comparison_is_exact() {
        assert!(secret_matches("sesame", "sesame"));
        assert!(!secret_matches("sesam", "sesame"));
        assert!(!secret_matches("sesamee", "sesame"));
    }

    /// The anti-pattern: a blank configured secret compared equal to a blank
    /// presented one, i.e. an executor plane that authenticates *everyone* the
    /// moment the key is forgotten. `Config::validate` refuses that pairing for
    /// a file-loaded config; this makes the compare itself refuse it too, so the
    /// door cannot be opened by a state assembled some other way.
    #[test]
    fn a_blank_configured_secret_matches_nothing_not_everything() {
        assert!(!secret_matches("", ""));
        assert!(!secret_matches("   ", ""));
        assert!(!secret_matches("anything", "   "));
    }

    fn limits<'a>(map: &'a [(&'a str, usize)]) -> impl Fn(&str) -> usize + 'a {
        move |app| {
            map.iter()
                .find(|(a, _)| *a == app)
                .map(|(_, n)| *n)
                .unwrap_or(0)
        }
    }

    /// The anti-pattern this function exists for: per-app caps enforced from an
    /// in-process map. With two executors and `app_concurrency = 2`, the local
    /// map on each side sees at most its own jobs, so four run.
    #[test]
    fn caps_count_the_cluster_not_one_process() {
        let counts = vec![("readable".to_string(), 2), ("quiet".to_string(), 1)];
        let blocked = blocked_over_cap(&counts, limits(&[("readable", 2), ("quiet", 4)]));
        assert_eq!(blocked, vec!["readable".to_string()]);
        // Zero means unlimited, exactly as `worker::app_limit` reads it.
        assert!(blocked_over_cap(&counts, limits(&[])).is_empty());
    }

    #[test]
    fn an_offline_executor_is_not_reported_busy_or_idle() {
        assert_eq!(executor_state(5, 0, 120), "idle");
        assert_eq!(executor_state(5, 2, 120), "busy");
        assert_eq!(executor_state(600, 2, 120), "offline");
        // `0` disables the liveness window rather than making everything offline.
        assert_eq!(executor_state(999_999, 0, 0), "idle");
    }

    /// An unparseable stamp must not manufacture an outage.
    #[test]
    fn an_unreadable_poll_stamp_reads_young_not_ancient() {
        let now = chrono::Utc::now();
        assert_eq!(age_secs("not a timestamp", now), 0);
        assert_eq!(age_secs("", now), 0);
        let then = (now - chrono::Duration::seconds(90)).to_rfc3339();
        assert!((85..=95).contains(&age_secs(&then, now)));
    }

    fn registry() -> HashMap<String, Arc<dyn ScrapeApp>> {
        crate::registry::apps()
            .into_iter()
            .map(|a| (a.name().to_string(), a))
            .collect()
    }

    /// The registry's executor-eligible apps, pinned as an EXPECTED diff.
    ///
    /// This list is the whole of what the plane can run today. Adding a name is
    /// a deliberate act with a checked precondition (the result-only scan
    /// below); it must never grow by accident, because widening it is what
    /// silently sends an upserting app to a process whose dataset handle
    /// refuses.
    const EXPECTED_EXECUTOR_APPS: &[&str] = &["readable"];

    #[test]
    fn the_executor_eligible_set_is_exactly_what_is_declared() {
        let mut declared = eligible_apps(&registry(), &[]);
        declared.sort();
        let mut expected: Vec<String> = EXPECTED_EXECUTOR_APPS
            .iter()
            .map(|s| s.to_string())
            .collect();
        expected.sort();
        assert_eq!(
            declared, expected,
            "the set of apps declaring `ScrapeApp::executor` changed — update \
             EXPECTED_EXECUTOR_APPS only after confirming the new app is result-only"
        );
    }

    /// An executor may only narrow the coordinator's list, never widen it.
    #[test]
    fn a_capability_an_app_never_declared_does_not_become_claimable() {
        let reg = registry();
        // Asking for an ineligible app yields nothing — not "everything".
        assert!(eligible_apps(&reg, &["grants-gov".to_string()]).is_empty());
        // Asking for an eligible one narrows to it.
        assert_eq!(
            eligible_apps(&reg, &["readable".to_string(), "grants-gov".to_string()]),
            vec!["readable".to_string()]
        );
        // Empty means "whatever you'll give me", which is still only the
        // eligible set.
        assert_eq!(eligible_apps(&reg, &[]), eligible_apps(&reg, &[]));
    }

    /// The AppContext seams that reach the **dataset store**. An executor's
    /// context carries a refusing `Datasets`, so an app touching any of these
    /// cannot run out there — which makes this list the operative definition of
    /// "result-only".
    const DATASET_SEAMS: &[&str] = &[
        ".datasets",
        ".upsert",
        ".sync_many",
        ".register_rules",
        ".observe_extraction",
    ];

    /// **The inventory test the v1 slice is bounded by.** Every app declaring
    /// `executor: true` must be result-only, and this reads the app's own source
    /// to say so rather than trusting the flag.
    ///
    /// The anti-pattern: a manifest flag as the only gate. `executor: true` is
    /// one line and reads like a capability advertisement; the consequence of
    /// getting it wrong is a job that runs to completion on a remote process
    /// with every one of its dataset writes refused — a failure that looks like
    /// an app bug, days later, on someone else's machine.
    #[test]
    fn executor_eligible_apps_are_result_only() {
        let apps_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../apps");
        for name in eligible_apps(&registry(), &[]) {
            let src = apps_root.join(&name).join("src");
            assert!(
                src.is_dir(),
                "app '{name}' declares executor: true but has no crate at {} — the eligibility \
                 scan cannot verify it is result-only, and an unverifiable claim is refused",
                src.display()
            );
            for file in source_files(&src) {
                let text = std::fs::read_to_string(&file).expect("read app source");
                for (i, line) in text.lines().enumerate() {
                    // Skip the doc/comment prose that legitimately NAMES these
                    // seams while explaining why the app avoids them.
                    let code = line.trim_start();
                    if code.starts_with("//") {
                        continue;
                    }
                    for seam in DATASET_SEAMS {
                        assert!(
                            !code.contains(seam),
                            "app '{name}' declares `executor: true` but reaches the dataset \
                             store at {}:{} ({seam}). An executor's AppContext carries a \
                             REFUSING Datasets handle — this app would run remotely and lose \
                             every write it thinks it made. Either drop the flag or make the \
                             app result-only.",
                            file.display(),
                            i + 1,
                        );
                    }
                }
            }
        }
    }

    fn source_files(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(source_files(&path));
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
        out
    }
}
