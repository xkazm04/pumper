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
    /// Declared data contract (`[source.contract]`) — the producer-side floor
    /// this source's output must clear at publish time. `None` = no contract,
    /// nothing checked. See [`Contract`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<Contract>,
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

/// A declared data contract for one source (`[source.contract]` in the catalog
/// TOML): the explicit, human/LLM-authored floor the source's output must clear
/// at publish time. The resilience system *infers* degradation statistically;
/// this is the *declared* complement — Great Expectations built into the
/// catalog. Evaluated in the worker at the same choke point where
/// `suppress_unhealthy` gates pushes; verdicts surface on `/catalog/health` and
/// `/sources`.
///
/// All checks are honest about absence: a field named only in `types`/`ranges`
/// is checked *when present and non-null* — requiring presence is exclusively
/// `required_fields`' job. Field names are top-level record keys (no nested
/// paths in v1).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Contract {
    /// Fields that must be present and non-null on every new/changed record.
    pub required_fields: Vec<String>,
    /// `field -> expected JSON type` (`string` | `number` | `bool` | `array` |
    /// `object`), checked only when the field is present and non-null.
    pub types: BTreeMap<String, String>,
    /// `field -> inclusive numeric bounds`, checked only when the field is
    /// present and numeric (a wrong *type* is `types`' job).
    pub ranges: BTreeMap<String, ContractRange>,
    /// Max share (0–100) of this run's revisions allowed to be removals — a
    /// mass-delete tripwire (a source suddenly dropping half its rows is a
    /// break, not a refresh).
    pub max_row_delta_pct: Option<f64>,
    /// Max age of the newest dataset write, in hours. Not evaluated at publish
    /// time (the run just wrote); it *tightens* the cadence-derived freshness
    /// window on `/catalog/health`.
    pub max_staleness_hours: Option<i64>,
}

/// Inclusive numeric bounds for one field in a [`Contract`].
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ContractRange {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// What a contract evaluation concluded, given the enforcement flag.
/// `Pass` = no violations. `Warn` = violations recorded, nothing gated
/// (`[contracts] enforce = false`, the default — soak mode, like resilience
/// started). `Block` = violations and enforcement on: the dataset's pushes are
/// suppressed at the worker seam before any webhook/trigger fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ContractVerdict {
    Pass,
    Warn,
    Block,
}

impl ContractVerdict {
    /// Maps an evaluation outcome to its verdict under the given enforce flag.
    pub fn from_violations(violations: &[String], enforce: bool) -> Self {
        match (violations.is_empty(), enforce) {
            (true, _) => ContractVerdict::Pass,
            (false, true) => ContractVerdict::Block,
            (false, false) => ContractVerdict::Warn,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ContractVerdict::Pass => "pass",
            ContractVerdict::Warn => "warn",
            ContractVerdict::Block => "block",
        }
    }
}

impl Contract {
    /// Evaluates one run's output against the contract. Pure — no clock, no IO —
    /// so the worker seam, tests, and any future backfill audit share it.
    ///
    /// `records` are the run's surviving (new/changed) record snapshots;
    /// `removed` is how many revisions in the run were removals. Returns the
    /// violation list — empty means the contract holds. Staleness is
    /// deliberately not checked here (see [`Contract::max_staleness_hours`]).
    pub fn evaluate(&self, records: &[&serde_json::Value], removed: usize) -> Vec<String> {
        let mut violations = Vec::new();

        for field in &self.required_fields {
            let missing = records
                .iter()
                .filter(|r| r.get(field).is_none_or(serde_json::Value::is_null))
                .count();
            if missing > 0 {
                violations.push(format!(
                    "required field '{field}' missing or null on {missing}/{} records",
                    records.len()
                ));
            }
        }

        for (field, expected) in &self.types {
            let bad = records
                .iter()
                .filter_map(|r| r.get(field))
                .filter(|v| !v.is_null() && !json_type_matches(v, expected))
                .count();
            if bad > 0 {
                violations.push(format!(
                    "field '{field}' is not of type '{expected}' on {bad}/{} records",
                    records.len()
                ));
            }
        }

        for (field, range) in &self.ranges {
            let out = records
                .iter()
                .filter_map(|r| r.get(field).and_then(serde_json::Value::as_f64))
                .filter(|n| {
                    range.min.is_some_and(|min| *n < min) || range.max.is_some_and(|max| *n > max)
                })
                .count();
            if out > 0 {
                violations.push(format!(
                    "field '{field}' out of range [{:?}, {:?}] on {out}/{} records",
                    range.min,
                    range.max,
                    records.len()
                ));
            }
        }

        if let Some(max_pct) = self.max_row_delta_pct {
            let total = records.len() + removed;
            if removed > 0 && total > 0 {
                let pct = removed as f64 / total as f64 * 100.0;
                if pct > max_pct {
                    violations.push(format!(
                        "removals are {pct:.1}% of this run's {total} revisions \
                         (max {max_pct}%) — possible mass-delete"
                    ));
                }
            }
        }

        violations
    }
}

/// `serde_json` type check for a contract's declared type name. Unknown names
/// match nothing — a typo'd contract fails loudly rather than silently passing.
fn json_type_matches(v: &serde_json::Value, expected: &str) -> bool {
    match expected {
        "string" => v.is_string(),
        "number" => v.is_number(),
        "bool" | "boolean" => v.is_boolean(),
        "array" => v.is_array(),
        "object" => v.is_object(),
        _ => false,
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

    /// The declared contract covering `(app, dataset)`, if any live source
    /// declares one. First match wins (app+dataset pairs are expected unique).
    pub fn contract_for(&self, app: &str, dataset: &str) -> Option<(&Source, &Contract)> {
        self.live()
            .filter(|s| s.app == app && s.dataset == dataset)
            .find_map(|s| s.contract.as_ref().map(|c| (s, c)))
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
            contract: None,
        };
        assert_eq!(src("daily").cadence_secs(), Some(86_400));
        assert_eq!(src("annual").cadence_secs(), Some(366 * 86_400));
        // No freshness expectation for these.
        assert_eq!(src("on-demand").cadence_secs(), None);
        assert_eq!(src("one-time").cadence_secs(), None);
        assert_eq!(src("").cadence_secs(), None);
    }

    // ---- data contracts ---------------------------------------------------

    #[test]
    fn parses_contract_block_and_absence_means_none() {
        let toml = r#"
            [[source]]
            id = "grants-gov"
            app = "grants-gov"
            name = "Grants.gov"
            status = "live"
            dataset = "opportunities"

            [source.contract]
            required_fields = ["id", "title"]
            max_row_delta_pct = 50.0
            max_staleness_hours = 48

            [source.contract.types]
            title = "string"

            [source.contract.ranges]
            award = { min = 0.0 }

            [[source]]
            id = "plain"
            name = "No contract"
            status = "live"
        "#;
        let cat = Catalog::parse(toml).expect("valid");
        let c = cat.sources[0].contract.as_ref().expect("contract parsed");
        assert_eq!(c.required_fields, vec!["id", "title"]);
        assert_eq!(c.types.get("title").map(String::as_str), Some("string"));
        assert_eq!(c.ranges["award"].min, Some(0.0));
        assert_eq!(c.max_row_delta_pct, Some(50.0));
        assert_eq!(c.max_staleness_hours, Some(48));
        assert!(cat.sources[1].contract.is_none());
        // Lookup goes through (app, dataset), live-only.
        assert!(cat.contract_for("grants-gov", "opportunities").is_some());
        assert!(cat.contract_for("grants-gov", "other").is_none());
    }

    fn contract(toml: &str) -> Contract {
        toml::from_str(toml).expect("valid contract")
    }

    #[test]
    fn evaluate_flags_missing_required_fields() {
        let c = contract(r#"required_fields = ["id", "title"]"#);
        let good = serde_json::json!({ "id": "1", "title": "ok" });
        let null_title = serde_json::json!({ "id": "2", "title": null });
        let missing = serde_json::json!({ "id": "3" });
        let v = c.evaluate(&[&good, &null_title, &missing], 0);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("'title'") && v[0].contains("2/3"), "{v:?}");
        assert!(c.evaluate(&[&good], 0).is_empty());
    }

    #[test]
    fn evaluate_checks_types_only_when_present_and_nonnull() {
        let c = contract(r#"[types]
            title = "string""#);
        let absent = serde_json::json!({ "id": "1" });
        let null = serde_json::json!({ "title": null });
        let wrong = serde_json::json!({ "title": 42 });
        // Absence and null are required_fields' job, not a type violation.
        assert!(c.evaluate(&[&absent, &null], 0).is_empty());
        let v = c.evaluate(&[&wrong], 0);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("'title'"), "{v:?}");
        // Unknown type names never match — a typo'd contract fails loudly.
        let typo = contract(r#"[types]
            title = "strnig""#);
        assert_eq!(typo.evaluate(&[&serde_json::json!({"title": "x"})], 0).len(), 1);
    }

    #[test]
    fn evaluate_checks_ranges_only_on_numeric_values() {
        let c = contract(r#"[ranges]
            applications = { min = 0.0, max = 100.0 }"#);
        let ok = serde_json::json!({ "applications": 50 });
        let non_numeric = serde_json::json!({ "applications": "n/a" });
        let low = serde_json::json!({ "applications": -1 });
        let high = serde_json::json!({ "applications": 200 });
        assert!(c.evaluate(&[&ok, &non_numeric], 0).is_empty());
        let v = c.evaluate(&[&low, &high], 0);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("2/2"), "{v:?}");
    }

    #[test]
    fn evaluate_trips_row_delta_on_mass_removal() {
        let c = contract("max_row_delta_pct = 50.0");
        let r = serde_json::json!({ "id": "1" });
        // 3 removed of 4 revisions = 75% > 50% — tripped.
        let v = c.evaluate(&[&r], 3);
        assert_eq!(v.len(), 1);
        assert!(v[0].contains("mass-delete"), "{v:?}");
        // 1 of 4 = 25% — fine; zero removals never trip.
        assert!(c.evaluate(&[&r, &r, &r], 1).is_empty());
        assert!(c.evaluate(&[], 0).is_empty());
    }

    #[test]
    fn verdict_maps_violations_and_enforce_flag() {
        assert_eq!(ContractVerdict::from_violations(&[], true), ContractVerdict::Pass);
        assert_eq!(ContractVerdict::from_violations(&[], false), ContractVerdict::Pass);
        let v = vec!["boom".to_string()];
        assert_eq!(ContractVerdict::from_violations(&v, false), ContractVerdict::Warn);
        assert_eq!(ContractVerdict::from_violations(&v, true), ContractVerdict::Block);
        assert_eq!(ContractVerdict::Block.as_str(), "block");
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
