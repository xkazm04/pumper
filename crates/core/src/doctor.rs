//! `datasets doctor` — a read-only integrity report on the store.
//!
//! Nothing told an operator the store's actual state. Provenance says a record
//! is reproducible; retention decides which bodies survive; `reindex` decides
//! which records have a fingerprint — and no surface joined those up and said
//! whether the promises still hold. This module is that join.
//!
//! Two properties are load-bearing:
//!
//! - **It repairs nothing.** Every query behind it is a `SELECT` and every
//!   filesystem touch is a `stat`. A doctor that fixes things silently is worse
//!   than no doctor, because you stop being able to tell whether the store was
//!   healthy or merely healed. Each finding therefore carries the *concrete*
//!   remediation instead — the binary to run, the config key to set, the route to
//!   call — never a bare count.
//! - **A clean store produces ZERO findings.** A report that always says
//!   something gets ignored within a week, so every check here is written to be
//!   silent on a healthy database (proved by `a_clean_store_reports_nothing`).
//!   Descriptive numbers that are not problems (per-dataset coverage, per-app
//!   artifact bytes, per-table growth) live outside `findings`.
//!
//! - **"We did not look" is never published as "it is fine."** The report is a
//!   roster ([`Check::ALL`]) of three-valued outcomes
//!   ([`CheckOutcome`]) rolled up worst-of into a [`Verdict`], so a check that
//!   could not run reports [`Verdict::Incomplete`] with the reason it could not,
//!   instead of an empty `findings` list that reads exactly like a clean store.
//!   `healthy` is [`DoctorReport::is_clean`], which is `verdict == Clean`.
//!
//! [`diagnose`] is pure: the caller gathers [`StoreFacts`] (full scans, on-demand
//! only) and this decides what is worth saying about them. [`examine`] wraps it
//! with the roster and the rollup.

use serde::Serialize;
use serde_json::{json, Value};

/// A table whose rows are accruing at least this long with retention disabled is
/// worth mentioning. Below it, "old rows exist" is just a store doing its job;
/// half a year of unbounded accrual is a trend.
pub const UNBOUNDED_GROWTH_DAYS: i64 = 180;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    /// A promise the store currently cannot keep.
    Warn,
    /// Accruing debt or a degraded capability — worth acting on, not broken.
    Info,
}

/// Every check the doctor is capable of running.
///
/// The roster is the load-bearing part. The checks used to exist only as nine
/// inline `&'static str` literals inside [`diagnose`], so nothing — not the
/// response, not a test, not an operator — could enumerate what the doctor is
/// *able* to check, which means nothing could notice when one of them did not
/// run. A verdict computed from the absence of findings then conflates the two
/// meanings of silence: "looked, nothing wrong" and "never looked".
///
/// Declared in the order [`diagnose`] evaluates them, which is the order
/// `findings` and `checks` are published in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Check {
    MissingArtifactBodies,
    HalfStampedProvenance,
    UnregisteredRulesets,
    RecordsWithoutSimhash,
    UnboundedTableGrowth,
    OrphanDerivedSpecs,
    StaleRebuildTables,
    SchemaDrift,
    SearchIndexEmpty,
}

impl Check {
    /// The whole roster. A tenth check cannot be added without appearing here
    /// (and without an arm in [`unverifiable_reason`], which is an exhaustive
    /// `match`), so "the doctor can check this" is a compile-time fact.
    pub const ALL: [Check; 9] = [
        Check::MissingArtifactBodies,
        Check::HalfStampedProvenance,
        Check::UnregisteredRulesets,
        Check::RecordsWithoutSimhash,
        Check::UnboundedTableGrowth,
        Check::OrphanDerivedSpecs,
        Check::StaleRebuildTables,
        Check::SchemaDrift,
        Check::SearchIndexEmpty,
    ];

    /// The wire name, identical to the string literal each check used to carry,
    /// so `findings[].check` is unchanged for every existing consumer.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MissingArtifactBodies => "missing_artifact_bodies",
            Self::HalfStampedProvenance => "half_stamped_provenance",
            Self::UnregisteredRulesets => "unregistered_rulesets",
            Self::RecordsWithoutSimhash => "records_without_simhash",
            Self::UnboundedTableGrowth => "unbounded_table_growth",
            Self::OrphanDerivedSpecs => "orphan_derived_specs",
            Self::StaleRebuildTables => "stale_rebuild_tables",
            Self::SchemaDrift => "schema_drift",
            Self::SearchIndexEmpty => "search_index_empty",
        }
    }
}

/// What one check concluded — **three-valued on purpose**.
///
/// A check that can only say "clean" or "finding" is forced to lie whenever the
/// honest answer is "I could not find out", and the doctor's checks reach that
/// answer routinely: `skip_artifacts=true` drops the body sweep, a ceiling
/// truncates it, Tantivy's `doc_count` can fail to read, and a [`StoreFacts`]
/// built with `..Default::default()` consulted neither the index nor the schema.
///
/// This repo already litigated this class one module over:
/// [`crate::resilience::SourceState::Unknown`] exists because a failed health
/// read resolving to `Healthy` published "a confident green about a check that
/// had not run — the exact shape of 'unknown rendering as a definite value' the
/// enforcement design is built to avoid". The doctor rendered it anyway, until
/// this type.
///
/// `Unverifiable` carries the reason the check could not run, because that is a
/// fact about the *prober* and routes to a different fixer than a finding does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CheckOutcome {
    /// Ran, concluded, nothing wrong.
    Clean,
    /// Ran and raised at least one entry in `findings`.
    ///
    /// A check that found something while its input was *also* incomplete
    /// reports `Findings`, not `Unverifiable`: it concluded something
    /// actionable, the rollup is already at its worst rung, and the partiality
    /// stays visible in the finding's own summary (which states `N of M`) and in
    /// the response's `artifacts` block.
    Findings,
    /// Did not run, or ran over an input that cannot support a clean claim. No
    /// claim about the store is being made at all.
    Unverifiable { reason: String },
}

/// One roster entry, as published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckReport {
    pub check: Check,
    #[serde(flatten)]
    pub outcome: CheckOutcome,
}

/// The rollup, by **worst-of** — because `healthy` is a gate question ("may I
/// trust this store?"), and for a gate question ninety-nine greens do not
/// offset one member that nobody looked at.
///
/// Ordered `Clean < Incomplete < Findings`, so the aggregation is `max` and an
/// actionable finding outranks incompleteness: both are "not clean", and the
/// one with a remediation attached is the one worth reporting first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Every check in the roster ran and found nothing.
    Clean,
    /// Nothing wrong was found, and at least one check could not run. **Not
    /// `Clean`** — that collapse is the whole reason this enum exists.
    Incomplete,
    /// At least one check raised a finding.
    Findings,
}

/// How far the per-revision artifact body sweep actually got.
///
/// The sweep is the one check whose completeness the pure core cannot infer
/// from its own inputs: an empty `missing_bodies` means "none broken" after a
/// full walk and "nobody looked" after a skipped one, and those were
/// indistinguishable. The caller states which it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArtifactScan {
    /// The default: nothing stat'd anything, so the facts carry no claim.
    #[default]
    NotAttempted,
    /// Deliberately skipped — `GET /datasets/doctor?skip_artifacts=true`.
    Skipped,
    /// Ran, but bounded: `checked` of `replayable` revisions were stat'd.
    Truncated { checked: i64, replayable: i64 },
    /// Every replayable revision in the store was stat'd.
    Complete,
}

impl ArtifactScan {
    /// Why this sweep cannot support a clean claim, or `None` when it examined
    /// every replayable revision.
    pub fn unverifiable_reason(self) -> Option<String> {
        match self {
            Self::Complete => None,
            Self::NotAttempted => Some(
                "the artifact sweep did not run, so no replayable revision's body was stat'd \
                 and a missing archived body cannot be ruled out"
                    .into(),
            ),
            Self::Skipped => Some(
                "skip_artifacts=true dropped the artifact tree walk, so no replayable \
                 revision's body was stat'd — re-request GET /datasets/doctor without \
                 skip_artifacts to verify this check"
                    .into(),
            ),
            Self::Truncated {
                checked,
                replayable,
            } => Some(format!(
                "{checked} of {replayable} replayable revisions stat'd (check_limit) — the \
                 remaining {} were not looked at, so a missing archived body among them \
                 cannot be ruled out",
                replayable.saturating_sub(checked)
            )),
        }
    }
}

/// One thing worth saying, with what to do about it.
#[derive(Debug, Clone, Serialize)]
pub struct Finding {
    pub check: Check,
    pub severity: Severity,
    pub summary: String,
    pub count: i64,
    /// The concrete next action: a binary, a config key, a route. Never advice.
    pub remediation: String,
    /// A bounded sample of what triggered it, so the finding is actionable
    /// without a second query.
    pub examples: Vec<Value>,
}

/// Cap on the examples attached to any one finding — the report is for reading.
const MAX_EXAMPLES: usize = 10;

/// A replayable revision whose stamped body was not found on disk.
#[derive(Debug, Clone, Serialize)]
pub struct MissingBody {
    pub app: String,
    pub dataset: String,
    pub key: String,
    pub revision: i64,
    pub path: String,
}

/// Stamp coverage of one dataset's revision chain.
#[derive(Debug, Clone, Serialize)]
pub struct DatasetCoverage {
    pub app: String,
    pub dataset: String,
    pub revisions: i64,
    pub with_job_id: i64,
    pub replayable: i64,
}

/// The search index's state, joined to what the store could put in it.
///
/// The doctor is the surface an operator runs *before* anyone complains, and
/// search was the one subsystem it could not see: the wiped-index signal
/// (`index.degraded`) lives on the `/search` response, so it only reaches
/// someone after a user reports missing results.
#[derive(Debug, Clone, Serialize)]
pub struct SearchFacts {
    /// `[search] enabled`. A disabled index is a valid deployment, never a
    /// defect — `NoSearch` answers every call with silent success.
    pub enabled: bool,
    /// Documents currently in the index. `None` when the count could not be
    /// read, which is reported descriptively rather than folded into `0` — that
    /// would slander a healthy index exactly as it would on the query path.
    pub doc_count: Option<u64>,
    /// Live (non-tombstoned) records in the whole store.
    pub live_records: i64,
}

/// Growth of one append-only table, joined to whether anything bounds it.
#[derive(Debug, Clone, Serialize)]
pub struct TableGrowth {
    pub table: String,
    pub rows: i64,
    /// Age of the oldest row in days; `None` when the table is empty.
    pub oldest_days: Option<i64>,
    /// The configured retention window for this table. `0` = unbounded.
    pub retention_days: u64,
    /// The `[storage]` key that would bound it — named so the remediation is
    /// copy-pasteable rather than a pointer at a doc.
    pub config_key: &'static str,
}

/// Everything the report is derived from. Gathered by the caller (the server
/// route), because half of it is SQL and half of it is the filesystem, and
/// keeping [`diagnose`] pure is what makes the "clean store is silent" property
/// testable without a database.
#[derive(Debug, Clone, Default)]
pub struct StoreFacts {
    /// Replayable revisions whose stamped body is not on disk.
    pub missing_bodies: Vec<MissingBody>,
    /// How many replayable revisions were checked to find them.
    pub replayable_checked: i64,
    /// How far that sweep got — and therefore whether an empty
    /// `missing_bodies` is a clean result or an unexamined one. Defaults to
    /// [`ArtifactScan::NotAttempted`], so a facts struct that never gathered
    /// the artifact side cannot read as clean.
    pub artifacts: ArtifactScan,
    /// `(app, dataset, count)` of revisions stamping exactly one of
    /// `artifact_sha` / `rules_hash`.
    pub half_stamped: Vec<(String, String, i64)>,
    /// `(rules_hash, revisions)` for hashes missing from `rules_versions`.
    pub unregistered_rules: Vec<(String, i64)>,
    /// `(app, dataset, count)` of live records with no SimHash fingerprint.
    pub null_simhash: Vec<(String, String, i64)>,
    /// The search index, or `None` when the report did not consult it.
    pub search: Option<SearchFacts>,
    /// `(id, source, target)` of derived specs whose source holds no records.
    pub orphan_derived: Vec<(String, String, String)>,
    /// Leftover `*_new` table-rebuild scaffolds.
    pub stale_rebuild_tables: Vec<String>,
    /// The schema this store actually has, against the chain this binary
    /// carries. `None` when the report did not consult it.
    ///
    /// On a booting server the unsound cases are mostly unreachable —
    /// `Storage::connect` refuses them — with one deliberate exception:
    /// `[storage] allow_schema_ahead = true` lets an ahead store through, and
    /// this is the surface that keeps saying so afterwards.
    pub schema: Option<crate::schema::SchemaReport>,
    /// Descriptive; only the unbounded-and-old ones become findings.
    pub tables: Vec<TableGrowth>,
    /// Descriptive; never a finding on its own — a dataset written by an app
    /// that cannot know its source hash is honestly unstamped, not broken.
    pub coverage: Vec<DatasetCoverage>,
}

/// Turns facts into findings. Pure, and deliberately conservative: a check that
/// cannot distinguish "wrong" from "differently shaped" stays out of `findings`
/// and lives in the descriptive sections instead.
pub fn diagnose(facts: &StoreFacts) -> Vec<Finding> {
    let mut out = Vec::new();

    // A provenance claim the store cannot honour: `rederive` will answer 409
    // "archived body unavailable" for exactly these revisions.
    if !facts.missing_bodies.is_empty() {
        out.push(Finding {
            check: Check::MissingArtifactBodies,
            severity: Severity::Warn,
            summary: format!(
                "{} of {} replayable revisions stamp an archived body that is not on disk",
                facts.missing_bodies.len(),
                facts.replayable_checked
            ),
            count: facts.missing_bodies.len() as i64,
            remediation: "re-run the producing job to re-archive the body; until then \
                          POST /provenance/{app}/{dataset}/{key}/rederive answers 409 for these \
                          keys. Retention pins bodies that replayable revisions point at, so \
                          these were removed by something else — check for manual deletion, and \
                          confirm [storage] artifact_retention_days against \
                          GET /retention/preview before enabling it further."
                .into(),
            examples: sample(facts.missing_bodies.iter().map(|m| json!(m))),
        });
    }

    // Half a stamp is not replayable: `rederive` refuses it exactly as it refuses
    // an unstamped revision. But "not replayable" is not the same as "wrong", and
    // this check cannot tell the two shapes apart — see the remediation.
    let half: i64 = facts.half_stamped.iter().map(|(_, _, n)| n).sum();
    if half > 0 {
        out.push(Finding {
            check: Check::HalfStampedProvenance,
            severity: Severity::Info,
            summary: format!(
                "{half} revisions stamp exactly one of artifact_sha / rules_hash, so \
                 POST .../rederive refuses them — which is correct for a whole-body archive and \
                 a defect for an extraction"
            ),
            count: half,
            remediation: "Read the shape before changing anything, because the two halves mean \
                          opposite things. artifact_sha WITHOUT rules_hash is the archives-whole- \
                          bodies shape and is usually DELIBERATE: the crawl's page_versions \
                          content-addresses each archived body and leaves rules_hash None on \
                          purpose ('None = unknown, never a fabricated pin'), because no RuleSet \
                          extracted the record — there is nothing to stamp and stamping one would \
                          be a fabrication. Those revisions are also fully protected by artifact \
                          retention (the pin addresses bodies by job_id + artifact_path, not by \
                          rules_hash), so this costs nothing; every healthy crawl run adds to this \
                          count and that is expected. rules_hash WITHOUT artifact_sha is the \
                          shape worth fixing: a ruleset claim with no body to verify it against, \
                          so make the write path stamp both (see \
                          census-common::http_provenance). Existing revisions are left alone \
                          either way: provenance stamps are never rewritten after the fact."
                .into(),
            examples: sample(
                facts
                    .half_stamped
                    .iter()
                    .map(|(app, ds, n)| json!({ "app": app, "dataset": ds, "revisions": n })),
            ),
        });
    }

    // The ruleset was stamped but never registered, so the historical rules the
    // value was extracted with are simply gone.
    let unregistered: i64 = facts.unregistered_rules.iter().map(|(_, n)| n).sum();
    if unregistered > 0 {
        out.push(Finding {
            check: Check::UnregisteredRulesets,
            severity: Severity::Warn,
            summary: format!(
                "{unregistered} revisions stamp a rules_hash that is not in the rules_versions \
                 registry"
            ),
            count: unregistered,
            remediation: "register the ruleset at write time (INSERT OR IGNORE into \
                          rules_versions — the hash IS the identity, so re-registration is free). \
                          Rulesets never captured cannot be recovered; those revisions stay \
                          non-replayable rather than being replayed against today's rules."
                .into(),
            examples: sample(
                facts
                    .unregistered_rules
                    .iter()
                    .map(|(h, n)| json!({ "rules_hash": h, "revisions": n })),
            ),
        });
    }

    // Duplicate detection skips simhash-0 rows, so this is a silently incomplete
    // report rather than an empty one. Counts only rows `reindex` would actually
    // rewrite — see `simhash_zero_is_a_missing_fingerprint` for why the raw
    // `simhash = 0` predicate made this finding permanently unclearable.
    let null_sim: i64 = facts.null_simhash.iter().map(|(_, _, n)| n).sum();
    if null_sim > 0 {
        out.push(Finding {
            check: Check::RecordsWithoutSimhash,
            severity: Severity::Info,
            summary: format!(
                "{null_sim} live records have textual content but no SimHash fingerprint, and are \
                 skipped by near-duplicate detection"
            ),
            count: null_sim,
            remediation: "run `just reindex` with the server stopped — it recomputes simhash from \
                          the stored JSON and rewrites only the rows that change. Every record \
                          counted here is one it WILL rewrite: records with genuinely no textual \
                          content hash to 0 honestly and are excluded, so this finding clears."
                .into(),
            examples: sample(
                facts
                    .null_simhash
                    .iter()
                    .map(|(app, ds, n)| json!({ "app": app, "dataset": ds, "records": n })),
            ),
        });
    }

    // Accruing tables that nothing bounds.
    let unbounded: Vec<&TableGrowth> = facts
        .tables
        .iter()
        .filter(|t| table_is_accruing_unbounded(t))
        .collect();
    if !unbounded.is_empty() {
        let rows: i64 = unbounded.iter().map(|t| t.rows).sum();
        out.push(Finding {
            check: Check::UnboundedTableGrowth,
            severity: Severity::Info,
            summary: format!(
                "{} append-only table(s) hold {rows} rows going back over {UNBOUNDED_GROWTH_DAYS} \
                 days with retention disabled",
                unbounded.len()
            ),
            count: rows,
            remediation: format!(
                "set {} under [storage] (see docs/features/datasets.md § Retention for what each \
                 prune spares), then confirm with GET /retention/preview before the janitor's \
                 next 6h tick.",
                unbounded
                    .iter()
                    .map(|t| t.config_key)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            examples: sample(unbounded.iter().map(|t| json!(t))),
        });
    }

    // A spec that recomputes forever over nothing.
    if !facts.orphan_derived.is_empty() {
        out.push(Finding {
            check: Check::OrphanDerivedSpecs,
            severity: Severity::Info,
            summary: format!(
                "{} derived spec(s) read a source dataset that holds no records",
                facts.orphan_derived.len()
            ),
            count: facts.orphan_derived.len() as i64,
            remediation: "either backfill the source (POST /derived/{id}/backfill once it has \
                          records) or remove the spec (DELETE /derived/{id}). Until then the \
                          target dataset it advertises will never fill."
                .into(),
            examples: sample(
                facts
                    .orphan_derived
                    .iter()
                    .map(|(id, src, tgt)| json!({ "id": id, "source": src, "target": tgt })),
            ),
        });
    }

    // SQLite cannot ALTER a CHECK constraint, so migrations rebuild the table
    // through a `*_new` scaffold and RENAME it into place (0021 does this to
    // `triggers`). Each migration runs in a transaction, so the scaffold is
    // never observable afterwards — a leftover means a rebuild did not land.
    if !facts.stale_rebuild_tables.is_empty() {
        out.push(Finding {
            check: Check::StaleRebuildTables,
            severity: Severity::Warn,
            summary: format!(
                "leftover table-rebuild scaffold(s): {}",
                facts.stale_rebuild_tables.join(", ")
            ),
            count: facts.stale_rebuild_tables.len() as i64,
            remediation: "a migration's CREATE x_new → copy → DROP x → RENAME sequence did not \
                          complete, so the live table may pre-date the rebuild and lack its \
                          constraint. Restore data/pumper.db from backup and re-run the server so \
                          migrations replay; do not drop the scaffold by hand, it may hold the \
                          only copy of the rows."
                .into(),
            examples: sample(facts.stale_rebuild_tables.iter().map(|t| json!(t))),
        });
    }

    // The schema the store actually has, against the chain this binary carries.
    // `Storage::connect` refuses both of these at boot, so reaching them here
    // means either `[storage] allow_schema_ahead = true` or a table that
    // vanished while the process was up.
    if let Some(schema) = facts.schema.as_ref().filter(|s| !s.is_sound()) {
        let mut parts = Vec::new();
        if !schema.ahead.is_empty() {
            parts.push(format!(
                "the store has applied migration version(s) {} that this binary does not carry",
                schema
                    .ahead
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !schema.tables_missing.is_empty() {
            parts.push(format!(
                "table(s) the migration chain promises are absent: {}",
                schema.tables_missing.join(", ")
            ));
        }
        out.push(Finding {
            check: Check::SchemaDrift,
            severity: Severity::Warn,
            summary: parts.join("; "),
            count: (schema.ahead.len() + schema.tables_missing.len()) as i64,
            remediation: "this build cannot fully read this store. If versions are AHEAD, run                           the newer pumper against it (there are no down migrations, so this                           build cannot undo them) or restore the pre-migration snapshot beside                           the database (`<database>.bak-premigrate-*`); `[storage]                           allow_schema_ahead = true` is what let this boot happen and should be                           turned back off once the right binary is running. If TABLES are                           missing, the ledger says the migration ran but the object is gone —                           restore that same snapshot and re-run so the chain replays, or move                           the database aside and let it rebuild from 0001. Do not create the                           table by hand: the chain is the only authority for its shape."
                .into(),
            examples: sample(
                schema
                    .ahead
                    .iter()
                    .map(|v| json!({ "ahead_version": v }))
                    .chain(
                        schema
                            .tables_missing
                            .iter()
                            .map(|t| json!({ "missing_table": t })),
                    ),
            ),
        });
    }

    // The index went empty and nothing on this surface would ever have said so.
    if facts
        .search
        .as_ref()
        .is_some_and(search_index_is_empty_but_store_is_not)
    {
        let live = facts.search.as_ref().map_or(0, |s| s.live_records);
        out.push(Finding {
            check: Check::SearchIndexEmpty,
            severity: Severity::Warn,
            summary: format!(
                "search is enabled but the index holds 0 documents while the store holds {live} \
                 live record(s) — /search answers 200 with no hits"
            ),
            count: live,
            remediation:
                "run `cargo run -p pumper-server --bin search-backfill -- --all` with the \
                          server STOPPED (Tantivy holds an exclusive writer lock). A wiped index \
                          does not self-heal: the live path only indexes records as they change, \
                          so a retired or weekly app's records never come back on their own."
                    .into(),
            examples: vec![json!({ "doc_count": 0, "live_records": live })],
        });
    }

    out
}

/// The whole report: the findings, the roster of every check with what it
/// concluded, and the worst-of rollup over the two.
///
/// The summary is a door, not a wall: `verdict` never travels without `checks`,
/// so "incomplete" always names the member that could not run and the reason it
/// could not.
#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    pub findings: Vec<Finding>,
    pub checks: Vec<CheckReport>,
    pub verdict: Verdict,
}

impl DoctorReport {
    /// The boolean an operator and `just doctor` still read — now meaning "no
    /// findings **and** nothing unexamined" rather than merely "no findings".
    pub fn is_clean(&self) -> bool {
        self.verdict == Verdict::Clean
    }

    /// What one roster member concluded. Total: every [`Check::ALL`] variant is
    /// present in every report.
    pub fn outcome(&self, check: Check) -> &CheckOutcome {
        &self
            .checks
            .iter()
            .find(|c| c.check == check)
            .expect("every Check::ALL variant is reported")
            .outcome
    }
}

/// Why `check` cannot support a clean claim over these facts, or `None` when it
/// was genuinely able to run.
///
/// This is where the three-state verdict is decided, and it is an exhaustive
/// `match` on purpose: a tenth check cannot be added without stating, here,
/// whether and how it can fail to run.
///
/// **The residue, stated.** Five checks (`half_stamped_provenance`,
/// `unregistered_rulesets`, `records_without_simhash`, `orphan_derived_specs`,
/// `stale_rebuild_tables`) read `Vec` fields whose empty value is ambiguous in
/// exactly the same way `missing_bodies` was — but their only caller gathers
/// all five unconditionally in one block, so no reachable request produces an
/// ungathered one. They report `Clean`; the four that *can* be ungathered
/// (`missing_artifact_bodies`, `unbounded_table_growth`, `schema_drift`,
/// `search_index_empty`) are typed so that they cannot.
pub fn unverifiable_reason(check: Check, facts: &StoreFacts) -> Option<String> {
    match check {
        Check::MissingArtifactBodies => facts.artifacts.unverifiable_reason(),
        Check::SchemaDrift => facts.schema.is_none().then(|| {
            "the report did not consult the schema ledger, so drift against the migration \
             chain this binary carries cannot be ruled out"
                .to_string()
        }),
        Check::UnboundedTableGrowth => facts.tables.is_empty().then(|| {
            "no append-only table's growth was gathered, so an unbounded table cannot be \
             ruled out"
                .to_string()
        }),
        Check::SearchIndexEmpty => match facts.search.as_ref() {
            None => Some(
                "the report did not consult the search index, so a wiped index cannot be \
                 ruled out"
                    .into(),
            ),
            // A disabled index is a valid deployment, never an unexamined one:
            // `NoSearch` answers every call with silent success, so there is
            // nothing to look at and `clean` is the honest outcome. Sweeping it
            // into `incomplete` would make every search-less store permanently
            // un-green, which is the unverifiable-to-failed collapse.
            Some(f) if !f.enabled => None,
            Some(f) if f.doc_count.is_none() => Some(
                "search is enabled but the index's doc_count could not be read, so an empty \
                 index over a populated store cannot be ruled out"
                    .into(),
            ),
            Some(_) => None,
        },
        Check::HalfStampedProvenance
        | Check::UnregisteredRulesets
        | Check::RecordsWithoutSimhash
        | Check::OrphanDerivedSpecs
        | Check::StaleRebuildTables => None,
    }
}

/// [`diagnose`] plus the roster and the rollup: the report an operator actually
/// reads.
///
/// `diagnose` answers "what is wrong"; this answers "and how much of the store
/// did we actually look at", which is the question `healthy: findings.is_empty()`
/// could not ask.
pub fn examine(facts: &StoreFacts) -> DoctorReport {
    let findings = diagnose(facts);
    let checks: Vec<CheckReport> = Check::ALL
        .iter()
        .map(|&check| {
            let outcome = if findings.iter().any(|f| f.check == check) {
                CheckOutcome::Findings
            } else if let Some(reason) = unverifiable_reason(check, facts) {
                CheckOutcome::Unverifiable { reason }
            } else {
                CheckOutcome::Clean
            };
            CheckReport { check, outcome }
        })
        .collect();
    let verdict = checks
        .iter()
        .map(|c| match c.outcome {
            CheckOutcome::Clean => Verdict::Clean,
            CheckOutcome::Findings => Verdict::Findings,
            CheckOutcome::Unverifiable { .. } => Verdict::Incomplete,
        })
        .max()
        .unwrap_or(Verdict::Clean);
    DoctorReport {
        findings,
        checks,
        verdict,
    }
}

/// True when the search index is empty while the store holds records it could
/// be serving — the one comparison between `doc_count` and the store that
/// cannot cry wolf.
///
/// **Why not a ratio or an equality check.** `doc_count` and the live record
/// count are not comparable quantities in either direction. The index holds
/// documents that are not stored records at all (job-result docs under the
/// reserved `_job` / `_records` datasets), and it legitimately omits nearly
/// every stored record — the live path only maintains datasets an app names in
/// its result's `index_datasets`, which today is just `grants/unified`. So a
/// healthy store routinely has millions of records and a few thousand
/// documents, and any threshold over that ratio would fire on a correct
/// deployment forever. Zero-versus-nonzero is the only step that means
/// something: an enabled index holding **nothing** while records exist is never
/// a correct state, and it is precisely the state (`schema drift wipe`, corrupt
/// -dir quarantine, an `enabled = false` window) whose recovery is
/// `search-backfill`.
///
/// A disabled index is not a finding: `[search] enabled = false` is a valid
/// deployment and `NoSearch::doc_count` reports 0 by design, so gating on
/// `enabled` is what keeps a config-off store `healthy: true`.
pub fn search_index_is_empty_but_store_is_not(f: &SearchFacts) -> bool {
    f.enabled && f.doc_count == Some(0) && f.live_records > 0
}

/// True when a stored SimHash of `0` means "never fingerprinted" rather than
/// "genuinely has no text".
///
/// `records.simhash` is `INTEGER NOT NULL DEFAULT 0` (migration 0004 added it
/// with no backfill), so `0` is both the un-fingerprinted sentinel AND the
/// honest hash of a record with no textual leaves — `simhash("")` returns 0 by
/// construction. `reindex_simhashes` rewrites only rows whose recomputed value
/// *differs*, so a textless record recomputes to 0, matches, and is never
/// touched. Counting it made `records_without_simhash` permanently unclearable:
/// the finding stayed forever, the operator re-ran a whole-table rewrite that
/// provably could not help, and the endpoint's load-bearing "a clean store
/// produces ZERO findings" property was unreachable on such a store.
///
/// Recomputing is what makes the finding *predictive of its own remediation*:
/// what it counts is exactly what `just reindex` will rewrite.
pub fn simhash_zero_is_a_missing_fingerprint(data: &Value) -> bool {
    crate::simhash::simhash_value(data) != 0
}

/// A table is worth flagging only when nothing bounds it AND it has actually been
/// accruing — a busy table with a week of rows is not a problem, and saying so
/// would train the operator to skip the report.
pub fn table_is_accruing_unbounded(t: &TableGrowth) -> bool {
    t.retention_days == 0 && t.rows > 0 && t.oldest_days.is_some_and(|d| d >= UNBOUNDED_GROWTH_DAYS)
}

fn sample(items: impl Iterator<Item = Value>) -> Vec<Value> {
    items.take(MAX_EXAMPLES).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn growth(
        table: &'static str,
        rows: i64,
        oldest_days: Option<i64>,
        retention: u64,
    ) -> TableGrowth {
        TableGrowth {
            table: table.into(),
            rows,
            oldest_days,
            retention_days: retention,
            config_key: "cost_event_retention_days",
        }
    }

    fn search(enabled: bool, doc_count: Option<u64>, live_records: i64) -> SearchFacts {
        SearchFacts {
            enabled,
            doc_count,
            live_records,
        }
    }

    /// The property the whole feature depends on: a healthy store says NOTHING.
    /// A report that always finds something is a report nobody reads, so an
    /// empty-but-present store — descriptive coverage rows, descriptive table
    /// rows, no defects — must produce an empty `findings` list.
    #[test]
    fn a_clean_store_reports_nothing() {
        let facts = StoreFacts {
            replayable_checked: 42,
            // Search enabled, populated, and in step.
            search: Some(search(true, Some(4_200), 100_000)),
            coverage: vec![DatasetCoverage {
                app: "crawl".into(),
                dataset: "pages".into(),
                revisions: 100,
                with_job_id: 100,
                replayable: 100,
            }],
            tables: vec![
                // Busy but young, retention off — normal.
                growth("cost_events", 5_000, Some(3), 0),
                // Old but bounded — the operator already decided.
                growth("job_yield", 900_000, Some(900), 30),
                // Empty.
                growth("saved_search_seen", 0, None, 0),
            ],
            ..Default::default()
        };
        assert!(diagnose(&facts).is_empty(), "{:?}", diagnose(&facts));
    }

    /// GUARD. A sound schema — and an extra table the chain never created —
    /// must stay out of `findings`. The doctor's whole contract is that a
    /// healthy store is silent, and the schema block is descriptive until
    /// something is actually unsound.
    #[test]
    fn a_sound_schema_says_nothing_even_with_an_extra_table() {
        let mut observed: std::collections::BTreeSet<String> = crate::schema::EXPECTED_TABLES
            .iter()
            .map(|t| (*t).to_string())
            .collect();
        observed.insert("operator_scratch".into());
        let facts = StoreFacts {
            schema: Some(crate::schema::SchemaReport::new(
                &[1, 2],
                &[1, 2],
                &observed,
            )),
            ..Default::default()
        };
        assert!(diagnose(&facts).is_empty(), "{:?}", diagnose(&facts));
    }

    /// The two unsound shapes, each naming the object and a reachable remedy.
    #[test]
    fn an_unsound_schema_names_the_object_and_the_way_back() {
        let full: std::collections::BTreeSet<String> = crate::schema::EXPECTED_TABLES
            .iter()
            .map(|t| (*t).to_string())
            .collect();
        let ahead = StoreFacts {
            schema: Some(crate::schema::SchemaReport::new(&[1, 2, 3], &[1, 2], &full)),
            ..Default::default()
        };
        let findings = diagnose(&ahead);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].check, Check::SchemaDrift);
        assert_eq!(findings[0].severity, Severity::Warn);
        assert!(findings[0].summary.contains('3'), "names the version");
        assert!(findings[0].remediation.contains("allow_schema_ahead"));

        let mut short = full.clone();
        short.remove("golden_docs");
        let gone = StoreFacts {
            schema: Some(crate::schema::SchemaReport::new(&[1], &[1], &short)),
            ..Default::default()
        };
        let findings = diagnose(&gone);
        assert_eq!(findings.len(), 1);
        assert!(findings[0].summary.contains("golden_docs"));
        assert!(findings[0].remediation.contains("bak-premigrate"));
        assert_eq!(findings[0].count, 1);
    }

    /// Growth is flagged on the conjunction, never on size alone — otherwise a
    /// legitimately large table nags forever.
    #[test]
    fn growth_is_flagged_only_when_unbounded_and_actually_old() {
        assert!(!table_is_accruing_unbounded(&growth(
            "t",
            10_000_000,
            Some(3),
            0
        )));
        assert!(!table_is_accruing_unbounded(&growth(
            "t",
            10_000_000,
            Some(999),
            30
        )));
        assert!(!table_is_accruing_unbounded(&growth("t", 0, None, 0)));
        assert!(table_is_accruing_unbounded(&growth(
            "t",
            1,
            Some(UNBOUNDED_GROWTH_DAYS),
            0
        )));
    }

    /// The anti-pattern: the finding's remediation told the operator to stamp
    /// both halves or neither — contradicting a *deliberate, commented* decision
    /// in the crawl (`page_versions` stamps `artifact_sha` and leaves
    /// `rules_hash` None because no RuleSet extracted the record, and a
    /// fabricated hash would be worse than an honest unknown). Every healthy
    /// crawl run grows this count, so the advice was permanent and wrong for the
    /// dataset that dominates it.
    #[test]
    fn half_stamped_advice_does_not_tell_the_operator_to_fabricate_a_stamp() {
        let facts = StoreFacts {
            half_stamped: vec![("crawl".into(), "page_versions".into(), 400)],
            ..Default::default()
        };
        let finding = diagnose(&facts)
            .into_iter()
            .find(|f| f.check == Check::HalfStampedProvenance)
            .expect("finding raised");
        // It names the legitimate shape, and says which half is the real defect.
        assert!(
            finding
                .remediation
                .contains("artifact_sha WITHOUT rules_hash"),
            "{}",
            finding.remediation
        );
        assert!(
            finding
                .remediation
                .contains("rules_hash WITHOUT artifact_sha"),
            "{}",
            finding.remediation
        );
        assert!(
            finding.remediation.contains("DELIBERATE"),
            "the archives-whole-bodies case must be named as intended, not as debt: {}",
            finding.remediation
        );
        // And it no longer claims the retention consequence that used to justify
        // the advice: the pin addresses bodies, not rulesets.
        assert!(
            finding.remediation.contains("job_id + artifact_path"),
            "{}",
            finding.remediation
        );
        assert_eq!(finding.severity, Severity::Info);
    }

    /// Every finding must name a concrete action. A bare count is the failure
    /// mode this check exists to prevent, so the inventory is asserted rather
    /// than described.
    #[test]
    fn every_finding_carries_a_remediation_not_a_bare_count() {
        let facts = StoreFacts {
            missing_bodies: vec![MissingBody {
                app: "crawl".into(),
                dataset: "pages".into(),
                key: "k".into(),
                revision: 3,
                path: "data/artifacts/crawl/j/page.html".into(),
            }],
            replayable_checked: 1,
            half_stamped: vec![("crawl".into(), "pages".into(), 2)],
            unregistered_rules: vec![("deadbeef".into(), 4)],
            null_simhash: vec![("crawl".into(), "pages".into(), 7)],
            orphan_derived: vec![("d1".into(), "crawl/pages".into(), "crawl/titles".into())],
            stale_rebuild_tables: vec!["triggers_new".into()],
            tables: vec![growth("cost_events", 10, Some(400), 0)],
            search: Some(search(true, Some(0), 5)),
            ..Default::default()
        };
        let findings = diagnose(&facts);
        let checks: Vec<&str> = findings.iter().map(|f| f.check.as_str()).collect();
        assert_eq!(
            checks,
            vec![
                "missing_artifact_bodies",
                "half_stamped_provenance",
                "unregistered_rulesets",
                "records_without_simhash",
                "unbounded_table_growth",
                "orphan_derived_specs",
                "stale_rebuild_tables",
                "search_index_empty",
            ],
            "the check inventory changed — update this list deliberately"
        );
        for f in &findings {
            assert!(f.count > 0, "{}: zero-count finding", f.check.as_str());
            assert!(
                f.remediation.len() > 40,
                "{}: remediation must name a concrete action, got {:?}",
                f.check.as_str(),
                f.remediation
            );
            assert!(!f.examples.is_empty(), "{}: no examples", f.check.as_str());
        }
    }

    /// The anti-pattern: `just doctor` reporting `healthy: true` for a week
    /// while `/search` returned nothing. The wiped-index signal existed, but
    /// only on the query path — so the operator learned about it from a user.
    #[test]
    fn an_empty_index_over_a_full_store_is_not_reported_as_healthy() {
        let facts = StoreFacts {
            search: Some(search(true, Some(0), 12_345)),
            ..Default::default()
        };
        let findings = diagnose(&facts);
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].check, Check::SearchIndexEmpty);
        assert_eq!(findings[0].severity, Severity::Warn);
        assert_eq!(findings[0].count, 12_345);
        assert!(
            findings[0].remediation.contains("search-backfill"),
            "the remediation must name the binary: {:?}",
            findings[0].remediation
        );
    }

    /// `[search] enabled = false` is a valid deployment — `NoSearch` reports 0
    /// documents by design, so keying on `doc_count == 0` alone would make every
    /// search-less store permanently unhealthy.
    #[test]
    fn a_disabled_search_index_is_not_a_finding() {
        let facts = StoreFacts {
            search: Some(search(false, Some(0), 12_345)),
            ..Default::default()
        };
        assert!(diagnose(&facts).is_empty(), "{:?}", diagnose(&facts));
        assert!(!search_index_is_empty_but_store_is_not(&search(
            false,
            Some(0),
            12_345
        )));
    }

    /// The check must not fire on states it cannot honestly judge: an empty
    /// store has nothing to index, and a `doc_count` that could not be read is
    /// the doctor's own failure to measure, not the store's defect.
    #[test]
    fn an_index_the_doctor_cannot_measure_is_not_a_finding() {
        // Empty store, empty index — correct, and correctly silent.
        assert!(!search_index_is_empty_but_store_is_not(&search(
            true,
            Some(0),
            0
        )));
        // Count unreadable — no claim either way.
        assert!(!search_index_is_empty_but_store_is_not(&search(
            true, None, 500
        )));
        // Populated index over a big store — the normal case; the index is
        // ALWAYS far smaller than the store and that is not a defect.
        assert!(!search_index_is_empty_but_store_is_not(&search(
            true,
            Some(12),
            9_000_000
        )));
        // A report that never consulted the index says nothing about it.
        assert!(diagnose(&StoreFacts::default()).is_empty());
    }

    /// The anti-pattern: counting every `simhash = 0` row, then prescribing
    /// `just reindex` — which rewrites only rows whose recomputed value DIFFERS.
    /// A record with no textual content hashes to 0 honestly, so it is counted
    /// forever, the rewrite provably cannot help, and the load-bearing
    /// "a clean store produces ZERO findings" property becomes unreachable.
    #[test]
    fn a_record_with_no_text_is_not_a_missing_fingerprint() {
        // Genuinely textless: numbers-free, string-free — hashes to 0 honestly.
        assert!(!simhash_zero_is_a_missing_fingerprint(&json!({})));
        assert!(!simhash_zero_is_a_missing_fingerprint(&json!({
            "flag": true, "nothing": null, "empty": [], "nested": { "also": {} }
        })));
        assert!(!simhash_zero_is_a_missing_fingerprint(&Value::Null));

        // Has text, so a stored 0 is the un-fingerprinted sentinel and reindex
        // WILL rewrite it.
        assert!(simhash_zero_is_a_missing_fingerprint(
            &json!({ "title": "a grant for widgets" })
        ));
        assert!(simhash_zero_is_a_missing_fingerprint(&json!({ "n": 42 })));
    }

    /// Examples are a sample, not a dump: a store with a million broken rows must
    /// still produce a readable report.
    #[test]
    fn examples_are_capped_rather_than_dumped() {
        let facts = StoreFacts {
            null_simhash: (0..1000)
                .map(|i| ("app".to_string(), format!("d{i}"), 1))
                .collect(),
            ..Default::default()
        };
        let findings = diagnose(&facts);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].examples.len(), MAX_EXAMPLES);
        assert_eq!(
            findings[0].count, 1000,
            "the count is the truth, not the sample"
        );
    }
}
