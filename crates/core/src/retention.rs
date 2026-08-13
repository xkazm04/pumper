//! Bounded growth for the archive on disk.
//!
//! Every other store in this repo has a shape that bounds it — jobs terminate,
//! the cache expires, health sketches roll. The **artifact tree**
//! (`<storage.artifacts_dir>/<app>/<job_id>/<name>`) has none: nothing in the
//! workspace has ever deleted a body. A crawl revisit even writes a fresh
//! `job_id` copy and abandons the old one, so a scheduled crawl grows the tree
//! monotonically for as long as it runs.
//!
//! **But bodies are not disposable.** Three live readers address them:
//!
//! - `POST /provenance/{app}/{dataset}/{key}/rederive` replays the archived body
//!   through the ruleset pinned in the revision's stamp, and verifies the file
//!   IS the stamped body by sha256 before believing it. Deleting that body turns
//!   a reproducible record into a permanently unreproducible one.
//! - `AppContext::read_source_artifact` reads bodies **cross-job** (the
//!   crawl → extract/plugin seam), addressed by the record's own
//!   `job_id` + `artifact_path`.
//! - VCR cassettes (`cassette.ndjson`) live in the same tree and are the whole
//!   substrate of `Vcr::Replay`.
//!
//! So this module is not a delete-cron. It is a **pinning** calculation: an age
//! cutoff proposes, and the provenance graph vetoes. The one rule that matters:
//!
//! > A body a *replayable* revision still points at is never reclaimed, however
//! > old it is. Age alone is not permission.
//!
//! Everything here is pure except [`scan_artifact_tree`] (a documented, on-demand
//! full walk — never on a hot path) and [`delete_artifacts`]. The plan is built,
//! reported and only then — if the caller asked for it — executed, so
//! `GET /retention/preview` and the janitor run the identical calculation.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::Serialize;

/// The VCR cassette filename. Cassettes sit in the same per-job directory as
/// scraped bodies but answer to a different reader, so retention treats them as
/// their own class.
///
/// Re-exported from [`crate::vcr`] rather than re-declared: this was a second
/// `pub const` with the same literal, kept in sync by a comment claiming
/// `vcr::CASSETTE_FILE` was "private to that module" — it is `pub`, and
/// re-exported from the crate root. The retention sweep decides whether to
/// delete a file by NAME, so the day the recorder's filename changed, the two
/// constants would have disagreed and the sweep would have reclaimed live
/// cassettes.
pub use crate::vcr::CASSETTE_FILE;

/// One archived body, addressed exactly the way re-derivation addresses it:
/// `<artifacts_root>/<app>/<job_id>/<name>`.
///
/// `name` is the path *relative to the job directory*. `rederive` and
/// `read_source_artifact` both reject any segment containing a separator, so a
/// nested file can never be the target of a provenance claim — it is therefore
/// never pinned, and is governed by age alone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct ArtifactRef {
    pub app: String,
    pub job_id: String,
    pub name: String,
}

impl ArtifactRef {
    /// Absolute path of this body under an artifacts root.
    pub fn path(&self, root: &Path) -> PathBuf {
        let mut p = root.join(&self.app).join(&self.job_id);
        for seg in self.name.split('/') {
            p.push(seg);
        }
        p
    }
}

/// A body found on disk, with the two facts retention needs about it.
#[derive(Debug, Clone)]
pub struct ArtifactFile {
    pub reference: ArtifactRef,
    pub bytes: u64,
    /// Filesystem mtime. Deliberately *not* the job's `created_at`: a body that
    /// was rewritten is young again, and the job row may be long gone.
    pub modified: DateTime<Utc>,
}

/// Why a body survived the cutoff — reported per app so the operator can see
/// whether the tree is large because of pins or because retention is off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeepReason {
    /// A replayable revision still points at it. Never reclaimed.
    Pinned,
    /// A VCR cassette, protected unless the operator opted in.
    Cassette,
    /// Younger than the cutoff.
    WithinWindow,
}

/// Per-app reclaim accounting. `bytes` is the whole app subtree; the three
/// `*_bytes` breakdowns partition it exactly.
#[derive(Debug, Clone, Default, Serialize)]
pub struct AppReclaim {
    pub app: String,
    pub files: u64,
    pub bytes: u64,
    pub reclaimable_files: u64,
    pub reclaimable_bytes: u64,
    /// Kept because a replayable revision points at it — the pinning rule.
    pub pinned_files: u64,
    pub pinned_bytes: u64,
    /// Kept because it is a protected VCR cassette.
    pub cassette_files: u64,
    pub cassette_bytes: u64,
}

/// What retention *would* do. Building it never touches the filesystem, so the
/// dry-run endpoint and the janitor share one calculation and cannot diverge.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RetentionPlan {
    /// Per-app rollup, sorted by app name.
    pub apps: Vec<AppReclaim>,
    /// Bodies the plan would delete. Empty in a preview that reclaims nothing.
    #[serde(skip)]
    pub delete: Vec<ArtifactRef>,
    pub total_files: u64,
    pub total_bytes: u64,
    pub reclaimable_files: u64,
    pub reclaimable_bytes: u64,
    pub pinned_files: u64,
    pub pinned_bytes: u64,
}

/// **The pinning rule.** A body is reclaimable only when every reader that could
/// address it has let go:
///
/// 1. no *replayable* revision (`artifact_sha` AND `rules_hash` both stamped)
///    points at it — this veto is absolute and outranks age, because deleting a
///    pinned body silently downgrades a reproducible provenance claim into a
///    permanent "archived body unavailable";
/// 2. it is not a protected VCR cassette;
/// 3. and only then, it is older than the cutoff.
///
/// Named and pure so the veto has a test of its own
/// (`a_pinned_body_is_not_reclaimed_by_age_alone`): if the pin check is ever
/// dropped from the ordering, that test fails rather than a crawl quietly
/// losing its provenance.
pub fn artifact_is_reclaimable(
    file: &ArtifactFile,
    pinned: &HashSet<ArtifactRef>,
    older_than: DateTime<Utc>,
    protect_cassettes: bool,
) -> bool {
    keep_reason(file, pinned, older_than, protect_cassettes).is_none()
}

/// The reason a body is kept, or `None` when it is reclaimable. Same ordering as
/// [`artifact_is_reclaimable`]; that function is defined in terms of this one so
/// the two can never disagree about the pin.
pub fn keep_reason(
    file: &ArtifactFile,
    pinned: &HashSet<ArtifactRef>,
    older_than: DateTime<Utc>,
    protect_cassettes: bool,
) -> Option<KeepReason> {
    if pinned.contains(&file.reference) {
        return Some(KeepReason::Pinned);
    }
    if protect_cassettes && file.reference.name == CASSETTE_FILE {
        return Some(KeepReason::Cassette);
    }
    (file.modified >= older_than).then_some(KeepReason::WithinWindow)
}

/// Builds the plan: what would be deleted, and what each app's tree is made of.
/// Pure — this is the dry run, and executing it is a separate, explicit step.
pub fn plan_artifact_retention(
    files: &[ArtifactFile],
    pinned: &HashSet<ArtifactRef>,
    older_than: DateTime<Utc>,
    protect_cassettes: bool,
) -> RetentionPlan {
    let mut per_app: BTreeMap<&str, AppReclaim> = BTreeMap::new();
    let mut plan = RetentionPlan::default();
    for f in files {
        let entry = per_app
            .entry(f.reference.app.as_str())
            .or_insert_with(|| AppReclaim {
                app: f.reference.app.clone(),
                ..Default::default()
            });
        entry.files += 1;
        entry.bytes += f.bytes;
        match keep_reason(f, pinned, older_than, protect_cassettes) {
            Some(KeepReason::Pinned) => {
                entry.pinned_files += 1;
                entry.pinned_bytes += f.bytes;
            }
            Some(KeepReason::Cassette) => {
                entry.cassette_files += 1;
                entry.cassette_bytes += f.bytes;
            }
            Some(KeepReason::WithinWindow) => {}
            None => {
                entry.reclaimable_files += 1;
                entry.reclaimable_bytes += f.bytes;
                plan.delete.push(f.reference.clone());
            }
        }
    }
    plan.apps = per_app.into_values().collect();
    for a in &plan.apps {
        plan.total_files += a.files;
        plan.total_bytes += a.bytes;
        plan.reclaimable_files += a.reclaimable_files;
        plan.reclaimable_bytes += a.reclaimable_bytes;
        plan.pinned_files += a.pinned_files;
        plan.pinned_bytes += a.pinned_bytes;
    }
    plan
}

/// Per-app disk usage with no cutoff and nothing to delete — the shape
/// `datasets doctor` reports so D-D's decisions are inspectable.
pub fn artifact_usage(files: &[ArtifactFile], pinned: &HashSet<ArtifactRef>) -> Vec<AppReclaim> {
    // A cutoff at the beginning of time puts every body inside the window, so
    // the plan's delete list is necessarily empty however old the tree is —
    // reporting cannot become deleting by way of a stray argument.
    plan_artifact_retention(files, pinned, DateTime::<Utc>::MIN_UTC, true).apps
}

/// Walks the artifact tree and sizes every body.
///
/// **This is a full filesystem scan** of `<root>/<app>/<job_id>/…` — O(files),
/// one `stat` each. It is on-demand only: the retention janitor runs it every
/// few hours and the report endpoints run it per request. It must never be put
/// on a request hot path or inside the worker loop.
///
/// Unreadable entries are skipped rather than failing the walk: a partially
/// readable tree still yields a usable report, and retention must not be
/// blockable by one bad file.
pub fn scan_artifact_tree(root: &Path) -> Vec<ArtifactFile> {
    let mut out = Vec::new();
    let Ok(apps) = std::fs::read_dir(root) else {
        return out; // no artifacts written yet
    };
    for app_entry in apps.flatten() {
        if !app_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let app = app_entry.file_name().to_string_lossy().into_owned();
        let Ok(jobs) = std::fs::read_dir(app_entry.path()) else {
            continue;
        };
        for job_entry in jobs.flatten() {
            if !job_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let job_id = job_entry.file_name().to_string_lossy().into_owned();
            collect_bodies(&job_entry.path(), &app, &job_id, "", &mut out);
        }
    }
    out
}

fn collect_bodies(dir: &Path, app: &str, job_id: &str, prefix: &str, out: &mut Vec<ArtifactFile>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let rel = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            collect_bodies(&entry.path(), app, job_id, &rel, out);
            continue;
        }
        let modified = meta
            .modified()
            .ok()
            .map(DateTime::<Utc>::from)
            .unwrap_or_else(Utc::now);
        out.push(ArtifactFile {
            reference: ArtifactRef {
                app: app.to_string(),
                job_id: job_id.to_string(),
                name: rel,
            },
            bytes: meta.len(),
            modified,
        });
    }
}

/// Executes a plan's delete list. Returns `(files removed, bytes removed)`.
///
/// Job directories left empty are removed too (an empty dir is not addressable
/// by anything), but an app directory is never removed — its existence is the
/// only cheap record that the app ever ran.
pub fn delete_artifacts(root: &Path, files: &[ArtifactFile], plan: &RetentionPlan) -> (u64, u64) {
    let sizes: BTreeMap<&ArtifactRef, u64> =
        files.iter().map(|f| (&f.reference, f.bytes)).collect();
    let mut removed_files = 0;
    let mut removed_bytes = 0;
    let mut job_dirs: HashSet<PathBuf> = HashSet::new();
    for reference in &plan.delete {
        let path = reference.path(root);
        match std::fs::remove_file(&path) {
            Ok(()) => {
                removed_files += 1;
                removed_bytes += sizes.get(reference).copied().unwrap_or(0);
                job_dirs.insert(root.join(&reference.app).join(&reference.job_id));
            }
            Err(e) => tracing::warn!("retention: could not remove {}: {e}", path.display()),
        }
    }
    for dir in job_dirs {
        // Fails harmlessly when the directory still holds a pinned body.
        let _ = std::fs::remove_dir(&dir);
    }
    (removed_files, removed_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(app: &str, job: &str, name: &str, bytes: u64, age_days: i64) -> ArtifactFile {
        ArtifactFile {
            reference: ArtifactRef {
                app: app.into(),
                job_id: job.into(),
                name: name.into(),
            },
            bytes,
            modified: Utc::now() - chrono::Duration::days(age_days),
        }
    }

    fn cutoff(days: i64) -> DateTime<Utc> {
        Utc::now() - chrono::Duration::days(days)
    }

    /// THE rule of this direction. An ancient body that a replayable revision
    /// still points at survives the age cutoff; an equally ancient unpinned
    /// sibling does not. Delete the `pinned.contains` branch from `keep_reason`
    /// and this test fails — which is the point: the pin is not a comment.
    #[test]
    fn a_pinned_body_is_not_reclaimed_by_age_alone() {
        let kept = file("crawl", "job-a", "page.html", 100, 400);
        let doomed = file("crawl", "job-b", "page.html", 100, 400);
        let pinned: HashSet<ArtifactRef> = [kept.reference.clone()].into_iter().collect();

        assert!(!artifact_is_reclaimable(&kept, &pinned, cutoff(30), true));
        assert_eq!(
            keep_reason(&kept, &pinned, cutoff(30), true),
            Some(KeepReason::Pinned)
        );
        assert!(artifact_is_reclaimable(&doomed, &pinned, cutoff(30), true));

        let plan = plan_artifact_retention(&[kept, doomed], &pinned, cutoff(30), true);
        assert_eq!(plan.delete.len(), 1);
        assert_eq!(plan.delete[0].job_id, "job-b");
        assert_eq!(plan.pinned_bytes, 100);
        assert_eq!(plan.reclaimable_bytes, 100);
    }

    /// The pin is per-body, not per-job: a job directory holding one pinned body
    /// does not shelter its unpinned siblings, and a pinned body does not drag
    /// its siblings' bytes into the "pinned" column of the report.
    #[test]
    fn pinning_is_per_body_not_per_job_directory() {
        let pinned_body = file("crawl", "job-a", "a.html", 10, 400);
        let sibling = file("crawl", "job-a", "b.html", 7, 400);
        let pinned: HashSet<ArtifactRef> = [pinned_body.reference.clone()].into_iter().collect();
        let plan = plan_artifact_retention(&[pinned_body, sibling], &pinned, cutoff(30), true);
        assert_eq!(plan.reclaimable_files, 1);
        assert_eq!(plan.reclaimable_bytes, 7);
        assert_eq!(plan.pinned_bytes, 10);
    }

    /// Cassettes are the substrate of `Vcr::Replay` and nothing records which
    /// job will be replayed, so they are protected by default and only an
    /// explicit opt-in reclaims them.
    #[test]
    fn cassettes_are_protected_unless_the_operator_opts_in() {
        let cassette = file("research", "job-a", CASSETTE_FILE, 500, 400);
        let empty = HashSet::new();
        assert_eq!(
            keep_reason(&cassette, &empty, cutoff(30), true),
            Some(KeepReason::Cassette)
        );
        assert!(artifact_is_reclaimable(
            &cassette,
            &empty,
            cutoff(30),
            false
        ));
    }

    /// A young body is kept whatever else is true — the cutoff is the last
    /// check, not the first.
    #[test]
    fn a_body_inside_the_window_is_never_proposed_for_deletion() {
        let young = file("crawl", "job-a", "page.html", 100, 1);
        let plan = plan_artifact_retention(&[young], &HashSet::new(), cutoff(30), true);
        assert!(plan.delete.is_empty());
        assert_eq!(plan.total_bytes, 100);
        assert_eq!(plan.reclaimable_bytes, 0);
    }

    /// Per-app accounting partitions each subtree exactly: the three keep/reclaim
    /// columns sum back to the app's total bytes, so a report can never imply
    /// there is more (or less) to reclaim than there is.
    #[test]
    fn per_app_reclaim_columns_partition_the_subtree() {
        let files = vec![
            file("crawl", "j1", "old.html", 100, 400),
            file("crawl", "j2", "pinned.html", 40, 400),
            file("crawl", "j3", CASSETTE_FILE, 5, 400),
            file("crawl", "j4", "young.html", 3, 1),
            file("census", "j5", "cbp.json", 9, 400),
        ];
        let pinned: HashSet<ArtifactRef> = [files[1].reference.clone()].into_iter().collect();
        let plan = plan_artifact_retention(&files, &pinned, cutoff(30), true);
        assert_eq!(plan.apps.len(), 2);
        let crawl = plan.apps.iter().find(|a| a.app == "crawl").unwrap();
        assert_eq!(crawl.bytes, 148);
        assert_eq!(
            crawl.reclaimable_bytes + crawl.pinned_bytes + crawl.cassette_bytes + 3,
            crawl.bytes,
            "reclaimable + pinned + cassette + within-window must equal the subtree"
        );
        assert_eq!(plan.total_bytes, 157);
    }

    /// `artifact_usage` is the read-only shape: it reports the tree without ever
    /// proposing a deletion, whatever the ages involved.
    #[test]
    fn usage_reports_never_propose_deletions() {
        let files = vec![file("crawl", "j1", "ancient.html", 100, 9999)];
        let usage = artifact_usage(&files, &HashSet::new());
        assert_eq!(usage.len(), 1);
        assert_eq!(usage[0].bytes, 100);
        assert_eq!(usage[0].reclaimable_bytes, 0);
    }

    #[test]
    fn scan_walks_app_job_body_and_sizes_it() {
        let dir = tempfile::tempdir().unwrap();
        let job = dir.path().join("crawl").join("job-a");
        std::fs::create_dir_all(job.join("nested")).unwrap();
        std::fs::write(job.join("page.html"), b"hello").unwrap();
        std::fs::write(job.join("nested").join("deep.html"), b"xy").unwrap();
        let mut found = scan_artifact_tree(dir.path());
        found.sort_by(|a, b| a.reference.cmp(&b.reference));
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].reference.name, "nested/deep.html");
        assert_eq!(found[0].bytes, 2);
        assert_eq!(found[1].reference.name, "page.html");
        assert_eq!(found[1].bytes, 5);
        assert_eq!(found[1].reference.app, "crawl");
        assert_eq!(found[1].reference.job_id, "job-a");
    }

    /// Executing a plan removes exactly the planned bodies and leaves everything
    /// else — including the pinned sibling and its job directory — on disk.
    #[test]
    fn delete_removes_only_the_planned_bodies() {
        let dir = tempfile::tempdir().unwrap();
        let job = dir.path().join("crawl").join("job-a");
        std::fs::create_dir_all(&job).unwrap();
        std::fs::write(job.join("keep.html"), b"keep").unwrap();
        std::fs::write(job.join("drop.html"), b"drop!").unwrap();
        let files = scan_artifact_tree(dir.path());
        let keep_ref = ArtifactRef {
            app: "crawl".into(),
            job_id: "job-a".into(),
            name: "keep.html".into(),
        };
        let pinned: HashSet<ArtifactRef> = [keep_ref].into_iter().collect();
        // Cutoff in the future so everything unpinned is past it.
        let plan = plan_artifact_retention(
            &files,
            &pinned,
            Utc::now() + chrono::Duration::days(1),
            true,
        );
        let (n, bytes) = delete_artifacts(dir.path(), &files, &plan);
        assert_eq!((n, bytes), (1, 5));
        assert!(job.join("keep.html").exists());
        assert!(!job.join("drop.html").exists());
        assert!(
            job.exists(),
            "a job dir with a survivor must not be removed"
        );
    }

    #[test]
    fn artifact_ref_path_joins_nested_names_by_segment() {
        let r = ArtifactRef {
            app: "crawl".into(),
            job_id: "j".into(),
            name: "a/b.html".into(),
        };
        let p = r.path(Path::new("root"));
        assert!(p.ends_with(Path::new("crawl").join("j").join("a").join("b.html")));
    }
}
