//! The data-source catalog (`catalog/data-sources.toml`) as a load-bearing,
//! machine-readable artifact rather than hand-maintained prose.
//!
//! Each `[[source]]` entry describes one data pipeline: what it scrapes, which
//! app serves it, how fresh, how trustworthy. The file is declared "the single
//! source of truth" in `ONBOARDING.md` and `catalog/README.md`, but until this
//! module it had no reader — so it drifted from the registry silently. Now it is
//! parsed here, served over `GET /catalog/sources`, and cross-checked against the
//! live `AppRegistry` by a server-crate test that fails on drift (a `live` entry
//! whose app isn't registered, or whose `cron` disagrees with the app's
//! `schedule()`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
#[cfg(feature = "storage")]
use crate::storage::Schedule;

/// The `schedules.managed_by` tag the reconciler stamps on every schedule it
/// creates or mutates. Rows without this tag (hand-made via the API, or the
/// code-seeded `static-<app>` rows) are sacred — the reconciler only ever
/// *reads* them, and every storage write is SQL-fenced on this tag.
pub const CATALOG_MANAGED_BY: &str = "catalog";

/// One data pipeline in the catalog. Field docs live in `catalog/README.md` and
/// the TOML header; kept in lockstep with the `[[source]]` schema.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    /// Stable kebab-case slug; equals the Pumper app `name()` when 1:1.
    pub id: String,
    /// App crate serving it (`crates/apps/<app>`); empty when not built yet.
    #[serde(default)]
    pub app: String,
    /// Jurisdiction id in the app's scheme (`us`, `us-ca`, `eu`, `cz`, …).
    #[serde(default)]
    pub market: String,
    pub name: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub engine: String,
    #[serde(default)]
    pub access: String,
    #[serde(default)]
    pub cadence: String,
    /// Exact 6-field cron when on the scheduler; empty otherwise.
    #[serde(default)]
    pub cron: String,
    /// `live` | `planned` | `blocked`.
    pub status: String,
    #[serde(default)]
    pub confidence: u8,
    /// Dataset name it writes; empty if n/a.
    #[serde(default)]
    pub dataset: String,
    #[serde(default)]
    pub notes: String,
}

impl Source {
    /// A source is on the scheduler iff it declares a non-empty cron.
    pub fn is_scheduled(&self) -> bool {
        !self.cron.trim().is_empty()
    }

    /// Max-expected age (seconds) between writes for this source's `cadence`, or
    /// `None` for cadences that carry no freshness expectation
    /// (`on-demand`/`one-time`, or an unknown value). Drives the freshness monitor:
    /// a `live` source whose dataset hasn't been written within this window (times
    /// a grace multiplier) is stale.
    pub fn cadence_secs(&self) -> Option<i64> {
        const DAY: i64 = 86_400;
        match self.cadence.trim() {
            "daily" => Some(DAY),
            "weekly" => Some(7 * DAY),
            "monthly" => Some(31 * DAY),
            "quarterly" => Some(93 * DAY),
            "annual" => Some(366 * DAY),
            _ => None, // on-demand | one-time | unknown → no freshness expectation
        }
    }
}

/// The parsed catalog — a list of `[[source]]` entries.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Catalog {
    #[serde(default, rename = "source")]
    pub sources: Vec<Source>,
}

impl Catalog {
    /// Loads from `$PUMPER_CATALOG` or `./catalog/data-sources.toml`. A missing
    /// file is an empty catalog (not an error) so a deployment without the file
    /// still boots; a malformed file IS an error.
    pub fn load() -> Result<Catalog> {
        let path = PathBuf::from(
            std::env::var("PUMPER_CATALOG")
                .unwrap_or_else(|_| "catalog/data-sources.toml".to_string()),
        );
        if !path.exists() {
            tracing::warn!(
                "catalog file {} not found, using empty catalog",
                path.display()
            );
            return Ok(Catalog::default());
        }
        let raw = std::fs::read_to_string(&path)?;
        Self::parse(&raw).map_err(|e| Error::Config(format!("{}: {e}", path.display())))
    }

    /// Parses catalog TOML from a string (the testable core of [`load`]).
    pub fn parse(raw: &str) -> Result<Catalog> {
        toml::from_str(raw).map_err(|e| Error::Config(e.to_string()))
    }

    /// Sources with `status == "live"` — the pipelines actually running.
    pub fn live(&self) -> impl Iterator<Item = &Source> {
        self.sources.iter().filter(|s| s.status == "live")
    }
}

// Diffing needs the `Schedule` row type, which lives behind the sqlx-backed
// `storage` feature; the plan DTOs above stay unconditional (pure data).
#[cfg(feature = "storage")]
impl Catalog {
    /// Diffs the catalog (desired state) against the live `schedules` table
    /// (actual state) into a [`ReconcilePlan`]. Pure — no I/O — so the same
    /// function backs the boot-time drift log, `GET /catalog/reconcile`
    /// (dry-run), and `POST /catalog/reconcile` (apply), and is unit-testable.
    ///
    /// Semantics (GitOps, scoped by the `managed_by` tag):
    /// - Desired = every `live` source naming an `app` and a non-empty `cron`.
    /// - A desired row already served by an **untagged** enabled schedule with
    ///   the exact app+cron (hand-made, or the code-seeded `static-<app>`) is
    ///   *covered* — reported, never touched, never duplicated.
    /// - Otherwise the catalog-managed schedule for the app is created
    ///   (`create`), or its cron/enabled corrected (`update`).
    /// - A **tagged** schedule whose source is no longer `live` (or dropped its
    ///   cron) is `disable`d; one with *no catalog row at all* is an `orphan` —
    ///   reported loudly, never auto-touched (a human deleted the TOML row;
    ///   deciding what that means is theirs).
    /// - Untagged schedules NEVER appear in `update`/`disable`/`orphan`.
    pub fn reconcile_plan(&self, schedules: &[Schedule]) -> ReconcilePlan {
        let mut plan = ReconcilePlan::default();

        // Tagged schedules, first-per-app is the managed row; extras are
        // duplicates something else created — surface them as orphans.
        let mut managed: BTreeMap<&str, &Schedule> = BTreeMap::new();
        for s in schedules {
            if s.managed_by.as_deref() != Some(CATALOG_MANAGED_BY) {
                continue;
            }
            if managed.contains_key(s.app.as_str()) {
                plan.orphan.push(PlanOrphan {
                    schedule_id: s.id.clone(),
                    app: s.app.clone(),
                    reason: "duplicate catalog-managed schedule for this app".into(),
                });
            } else {
                managed.insert(s.app.as_str(), s);
            }
        }

        // Desired: first live source per app that declares a cron.
        let mut desired: BTreeMap<&str, &Source> = BTreeMap::new();
        for src in self.live() {
            if !src.app.is_empty() && src.is_scheduled() {
                desired.entry(src.app.as_str()).or_insert(src);
            }
        }
        // Any catalog row per app (any status), for the disable-vs-orphan call.
        let cataloged_apps: BTreeSet<&str> = self
            .sources
            .iter()
            .map(|s| s.app.as_str())
            .filter(|a| !a.is_empty())
            .collect();

        for (app, src) in &desired {
            let want_cron = src.cron.trim();
            if let Some(m) = managed.get(app) {
                if m.cron.trim() == want_cron && m.enabled {
                    plan.in_sync += 1;
                } else {
                    plan.update.push(PlanUpdate {
                        schedule_id: m.id.clone(),
                        app: (*app).to_string(),
                        from_cron: m.cron.clone(),
                        to_cron: want_cron.to_string(),
                        re_enable: !m.enabled,
                    });
                }
            } else if schedules.iter().any(|s| {
                s.managed_by.is_none() && s.enabled && s.app == *app && s.cron.trim() == want_cron
            }) {
                // Served by a hand-made or code-seeded row — never duplicated.
                plan.covered_by_untagged += 1;
            } else {
                plan.create.push(PlanCreate {
                    source_id: src.id.clone(),
                    app: (*app).to_string(),
                    cron: want_cron.to_string(),
                });
            }
        }

        // Tagged schedules the catalog no longer wants running.
        for (app, m) in &managed {
            if desired.contains_key(app) {
                continue;
            }
            if cataloged_apps.contains(app) {
                if m.enabled {
                    let status = self
                        .sources
                        .iter()
                        .find(|s| s.app == *app)
                        .map(|s| s.status.as_str())
                        .unwrap_or("?");
                    plan.disable.push(PlanDisable {
                        schedule_id: m.id.clone(),
                        app: (*app).to_string(),
                        reason: if status == "live" {
                            "source no longer declares a cron".into()
                        } else {
                            format!("catalog status is '{status}'")
                        },
                    });
                } else {
                    plan.in_sync += 1; // already off, as desired
                }
            } else {
                plan.orphan.push(PlanOrphan {
                    schedule_id: m.id.clone(),
                    app: (*app).to_string(),
                    reason: "no catalog row for this app (row deleted?)".into(),
                });
            }
        }
        plan
    }
}

/// One schedule to create for a live catalog source with no serving schedule.
#[derive(Debug, Clone, Serialize)]
pub struct PlanCreate {
    pub source_id: String,
    pub app: String,
    pub cron: String,
}

/// A catalog-managed schedule whose cron (or enabled flag) drifted from the TOML.
#[derive(Debug, Clone, Serialize)]
pub struct PlanUpdate {
    pub schedule_id: String,
    pub app: String,
    pub from_cron: String,
    pub to_cron: String,
    /// True when the row was disabled and the catalog wants it running again.
    pub re_enable: bool,
}

/// A catalog-managed schedule whose source flipped away from `live` (or dropped
/// its cron) — applying the plan sets `enabled = false`.
#[derive(Debug, Clone, Serialize)]
pub struct PlanDisable {
    pub schedule_id: String,
    pub app: String,
    pub reason: String,
}

/// A catalog-managed schedule with no catalog row backing it. Reported loudly,
/// NEVER auto-touched: a deleted TOML row is a human decision the machine
/// shouldn't finish on its own.
#[derive(Debug, Clone, Serialize)]
pub struct PlanOrphan {
    pub schedule_id: String,
    pub app: String,
    pub reason: String,
}

/// The reconciler's diff of catalog (desired) vs schedules table (actual).
/// `create`/`update`/`disable` are actionable; `orphan` is report-only.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ReconcilePlan {
    pub create: Vec<PlanCreate>,
    pub update: Vec<PlanUpdate>,
    pub disable: Vec<PlanDisable>,
    pub orphan: Vec<PlanOrphan>,
    /// Desired rows served by an untagged (hand-made / code-seeded) schedule —
    /// satisfied, and deliberately left alone.
    pub covered_by_untagged: usize,
    /// Desired rows whose catalog-managed schedule already matches.
    pub in_sync: usize,
}

impl ReconcilePlan {
    /// No drift: nothing to create, fix, disable, and no orphans to report.
    pub fn is_empty(&self) -> bool {
        self.create.is_empty()
            && self.update.is_empty()
            && self.disable.is_empty()
            && self.orphan.is_empty()
    }

    /// One-line summary for the boot drift log.
    pub fn summary(&self) -> String {
        format!(
            "create={} update={} disable={} orphan={} covered_by_untagged={} in_sync={}",
            self.create.len(),
            self.update.len(),
            self.disable.len(),
            self.orphan.len(),
            self.covered_by_untagged,
            self.in_sync,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sources_and_defaults_missing_fields() {
        let toml = r#"
            [[source]]
            id = "grants-gov"
            app = "grants-gov"
            market = "us"
            name = "Grants.gov"
            status = "live"
            cron = "0 0 9 * * *"

            [[source]]
            id = "future-thing"
            name = "Not built yet"
            status = "planned"
        "#;
        let cat = Catalog::parse(toml).expect("valid");
        assert_eq!(cat.sources.len(), 2);
        assert_eq!(cat.live().count(), 1);
        assert!(cat.sources[0].is_scheduled());
        // Missing optional fields default rather than failing the parse.
        assert_eq!(cat.sources[1].app, "");
        assert!(!cat.sources[1].is_scheduled());
    }

    #[test]
    fn cadence_secs_maps_known_cadences_and_exempts_the_rest() {
        let src = |cadence: &str| Source {
            id: "x".into(),
            app: String::new(),
            market: String::new(),
            name: "x".into(),
            url: String::new(),
            category: String::new(),
            engine: String::new(),
            access: String::new(),
            cadence: cadence.into(),
            cron: String::new(),
            status: "live".into(),
            confidence: 0,
            dataset: String::new(),
            notes: String::new(),
        };
        assert_eq!(src("daily").cadence_secs(), Some(86_400));
        assert_eq!(src("annual").cadence_secs(), Some(366 * 86_400));
        // No freshness expectation for these.
        assert_eq!(src("on-demand").cadence_secs(), None);
        assert_eq!(src("one-time").cadence_secs(), None);
        assert_eq!(src("").cadence_secs(), None);
    }

    // ---- reconcile plan ---------------------------------------------------

    fn sched(id: &str, app: &str, cron: &str, enabled: bool, managed: bool) -> Schedule {
        Schedule {
            id: id.into(),
            app: app.into(),
            cron: cron.into(),
            params: serde_json::Value::Null,
            enabled,
            priority: 0,
            timezone: None,
            misfire_policy: "fire_once".into(),
            max_attempts: None,
            managed_by: managed.then(|| CATALOG_MANAGED_BY.to_string()),
            last_run: None,
            created_at: chrono::Utc::now(),
        }
    }

    fn cat(toml: &str) -> Catalog {
        Catalog::parse(toml).expect("valid catalog")
    }

    const LIVE_DAILY: &str = r#"
        [[source]]
        id = "grants-gov"
        app = "grants-gov"
        name = "Grants.gov"
        status = "live"
        cron = "0 0 9 * * *"
    "#;

    #[test]
    fn plan_creates_for_unserved_live_source() {
        let plan = cat(LIVE_DAILY).reconcile_plan(&[]);
        assert_eq!(plan.create.len(), 1);
        assert_eq!(plan.create[0].app, "grants-gov");
        assert_eq!(plan.create[0].cron, "0 0 9 * * *");
        assert!(!plan.is_empty());
    }

    #[test]
    fn plan_is_empty_when_managed_schedule_matches() {
        let s = sched("catalog-grants-gov", "grants-gov", "0 0 9 * * *", true, true);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[s]);
        assert!(plan.is_empty(), "expected empty plan, got: {}", plan.summary());
        assert_eq!(plan.in_sync, 1);
    }

    #[test]
    fn untagged_schedule_covers_the_source_and_is_never_touched() {
        // The code-seeded static row already serves this source exactly.
        let s = sched("static-grants-gov", "grants-gov", "0 0 9 * * *", true, false);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[s]);
        assert!(plan.is_empty(), "untagged coverage must not produce actions");
        assert_eq!(plan.covered_by_untagged, 1);
    }

    #[test]
    fn untagged_drift_yields_create_not_update() {
        // A hand-made schedule with the wrong cron is sacred: the reconciler
        // creates its own tagged row rather than editing the hand-made one.
        let s = sched("hand", "grants-gov", "0 0 4 * * *", true, false);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[s]);
        assert_eq!(plan.create.len(), 1);
        assert!(plan.update.is_empty() && plan.disable.is_empty() && plan.orphan.is_empty());
    }

    #[test]
    fn managed_cron_drift_and_disabled_row_yield_update() {
        let drifted = sched("catalog-grants-gov", "grants-gov", "0 0 4 * * *", true, true);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[drifted]);
        assert_eq!(plan.update.len(), 1);
        assert_eq!(plan.update[0].to_cron, "0 0 9 * * *");
        assert!(!plan.update[0].re_enable);

        let off = sched("catalog-grants-gov", "grants-gov", "0 0 9 * * *", false, true);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[off]);
        assert_eq!(plan.update.len(), 1);
        assert!(plan.update[0].re_enable);
    }

    #[test]
    fn managed_schedule_for_blocked_source_is_disabled() {
        let toml = r#"
            [[source]]
            id = "jobs-cz"
            app = "jobs-cz"
            name = "Jobs.cz"
            status = "blocked"
            cron = "0 0 9 * * *"
        "#;
        let s = sched("catalog-jobs-cz", "jobs-cz", "0 0 9 * * *", true, true);
        let plan = cat(toml).reconcile_plan(&[s]);
        assert_eq!(plan.disable.len(), 1);
        assert!(plan.disable[0].reason.contains("blocked"));
        // Already-disabled = desired state, nothing to do.
        let s = sched("catalog-jobs-cz", "jobs-cz", "0 0 9 * * *", false, true);
        assert!(cat(toml).reconcile_plan(&[s]).is_empty());
    }

    #[test]
    fn managed_schedule_without_catalog_row_is_orphan_only() {
        let s = sched("catalog-gone", "gone-app", "0 0 9 * * *", true, true);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[s]);
        assert_eq!(plan.orphan.len(), 1);
        assert!(plan.disable.is_empty(), "orphans are report-only, never disabled");
        // The live source is still unserved, so a create is also planned.
        assert_eq!(plan.create.len(), 1);
    }

    #[test]
    fn untagged_schedules_never_enter_disable_or_orphan() {
        // Hand-made schedule for an app the catalog knows nothing about.
        let s = sched("hand", "my-experiment", "0 0 1 * * *", true, false);
        let plan = cat(LIVE_DAILY).reconcile_plan(&[s]);
        assert!(plan.disable.is_empty() && plan.orphan.is_empty() && plan.update.is_empty());
    }
}
