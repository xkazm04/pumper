//! MPSV / Úřad práce ČR open vacancy feed → labor-market aggregates.
//!
//! Ingests the Czech national job-vacancy register ("Volná místa za celou ČR") —
//! a key-free, daily, CC BY 4.0 JSON file — and turns the ~300k live postings
//! into two small, downstream-facing datasets:
//!   * `role_region_agg` — per (CZ-ISCO occupation × kraj × org type): posting
//!     count + the monthly-salary distribution (min/p25/median/p75/max). The
//!     substrate for reference salaries, the locality map, and — via change
//!     detection across daily runs — trending/fading positions. A `kraj = "ALL"`
//!     cell per (occupation × org type) carries the national roll-up.
//!   * `vacancy_samples` — a bounded reservoir of representative postings per
//!     CZ-ISCO unit group, for job-description references.
//!   * vacancy survival ledger — a compact per-posting lifecycle ledger
//!     (`vacancy-ledger.json` artifact + `vacancy_ledger` pointer record) diffed
//!     against the prior run to derive `cz-labour/vacancy_lifecycle`: per
//!     CZ-ISCO unit group × kraj, the distribution of days a posting stays
//!     listed before it disappears. HONEST LABELING: disappearance conflates
//!     filled / withdrawn / expired — the metric is **time-to-CLOSE**, NOT
//!     time-to-fill. Repost detection (same IČO + occupation + kraj + salary
//!     band reappearing within `repostWindowDays`) partially de-noises it. A
//!     run gap larger than `maxGapDays` closes nothing (carry-forward), so one
//!     outage can't mark ~300k postings closed.
//!   * `cz-labour/salary_nowcast` — a deterministic RATIO-CARRY nowcast (NOT a
//!     model): per CZ-ISCO unit group × sphere, the median posted-vs-official
//!     ratio from `salary_gap`'s stored revision history (newest
//!     `nowcastWindow` observations, default 6) is applied to today's posted
//!     median to project the current official-grade median, closing the
//!     quarterly-to-annual ISPV publication lag. Each row carries the ratio
//!     used, observation count, dispersion, a high|med|low confidence, and the
//!     ISPV anchor date + staleness. A group with no stored gap history emits
//!     no row — never extrapolated from nothing. HONESTY FLOOR: a cell whose
//!     ratio or projected median is implausible, or that rests on a single
//!     observation, ships with `nowcast_median: null` + `withheld: <reason>` +
//!     `confidence: "none"` rather than a number; and an ISPV anchor older than
//!     `NOWCAST_ANCHOR_STALE_DAYS` sets `anchor_stale` and costs one confidence
//!     level.
//!
//! The raw 188 MB feed is parsed into a typed subset (bounded memory) and
//! aggregated in-process; only the small aggregates are persisted. A full
//! per-posting upsert is deliberately avoided — `Datasets::upsert_many` is a
//! sequential per-row SELECT+write, so ~300k rows would be ~600k round-trips.
//!
//! Data type: LABOR-MARKET open data. Access: key-free. See
//! `catalog/data-sources.toml` (id `mpsv-vpm`).
//!
//! Source contract (verified 2026-07-05): a single JSON document
//! `{ "polozky": [ {…posting…} ] }`, replaced once daily; each posting carries
//! `profeseCzIsco.id` ("CzIsco/93291"), `mesicniMzdaOd`/`Do`,
//! `zamestnavatel.{ico,nazev}`, `mistoVykonuPrace.pracoviste[].adresa.kraj.id`
//! ("Kraj/108"), and the `statniSpravaSamosprava` / `souhlasAgentury*` flags used
//! to derive org type. The ~188 MB full-file fetch sets a per-request
//! `HttpRequest.timeout_secs` (the client-global `[http] timeout_secs` stays at
//! its 30s default for the rest of the fleet).
//!
//! DRIFT IS LOUD. A document with no `polozky` array fails the run naming the
//! drift ([`feed_postings`]) instead of aggregating zero postings into a clean
//! `feedRecords: 0` success, and the DEFAULT feed at full width carrying fewer
//! than [`MIN_PLAUSIBLE_POSTINGS`] postings fails before any aggregation
//! ([`implausibly_small_feed`]) so a collapsed feed cannot overwrite every
//! national cell with near-empty ones. Both paths write nothing, and since
//! every dataset here is upsert-only, yesterday's values stay in place.

#![allow(non_snake_case)]

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, NaiveDate, Utc};
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpRequest, ManifestExample, Provenance, Result,
    ScrapeApp,
};
use serde::Deserialize;
use serde_json::{json, Value};

pub struct MpsvVpm;

/// Full national vacancy register (~188 MB, replaced daily).
const FULL_URL: &str = "https://data.mpsv.cz/od/soubory/volna-mista/volna-mista.json";
/// Salary sanity band (CZK monthly) — drops hourly-mislabeled rows and errors.
const SALARY_MIN: f64 = 5_000.0;
const SALARY_MAX: f64 = 2_000_000.0;
/// Floor on the posting count that may recompute NATIONAL aggregates.
///
/// "Volná místa za celou ČR" is the whole country's live vacancy register —
/// ~300k postings, replaced daily. 1 000 is 0.3% of that: no holiday, no
/// seasonal trough and no real labour-market event produces it, only a
/// truncated download or a partial publication. Applies to the default feed at
/// full width only — see [`implausibly_small_feed`].
const MIN_PLAUSIBLE_POSTINGS: usize = 1_000;

/// Official ISPV salary statistics — read cross-app from the store.
const ISPV_APP: &str = "mpsv-ispv";
const ISPV_DATASET: &str = "wages";
/// Virtual shared namespace for the posted-vs-official join (grants-common
/// pattern: cross-source products live in a namespace no single app owns).
const GAP_APP: &str = "cz-labour";
const GAP_DATASET: &str = "salary_gap";
/// Salary nowcast — a deterministic RATIO-CARRY projection, not a model: per
/// (CZ-ISCO unit group × sphere), the median posted-vs-official ratio observed
/// in `salary_gap`'s stored revision history is carried onto today's posted
/// median to project the current official-grade (ISPV-quality) median. Groups
/// with no stored gap history emit NO row — never extrapolated from nothing.
const NOWCAST_DATASET: &str = "salary_nowcast";
/// Default ratio window: the newest N stored `salary_gap` revisions per cell.
const NOWCAST_WINDOW_DEFAULT: u64 = 6;
/// Bulk-read cap for the salary_gap revision scan (same posture as
/// [`TRENDS_REVISION_SCAN`]); a hit is logged, since silent truncation would
/// shorten some cells' ratio windows.
const NOWCAST_REVISION_SCAN: i64 = 50_000;
/// Confidence thresholds (documented contract):
///   high — ≥ [`NOWCAST_HIGH_MIN_OBS`] ratio observations AND relative ratio
///          spread (max−min)/median ≤ [`NOWCAST_HIGH_MAX_SPREAD`] (10%);
///   med  — ≥ [`NOWCAST_MED_MIN_OBS`] observations AND spread ≤
///          [`NOWCAST_MED_MAX_SPREAD`] (25%);
///   low  — everything else that still has ≥ 1 observation.
const NOWCAST_HIGH_MIN_OBS: usize = 6;
const NOWCAST_HIGH_MAX_SPREAD: f64 = 0.10;
const NOWCAST_MED_MIN_OBS: usize = 3;
const NOWCAST_MED_MAX_SPREAD: f64 = 0.25;
/// Evidence floor for PUBLISHING a nowcast number. A single stored ratio
/// observation is one day's posted-vs-official reading — the ratio's median is
/// then that day, and the "projection" is a rename of one sample. Cells below
/// this floor still emit a row (so the cell's absence of evidence is legible,
/// and so a previously-published number is actively overwritten rather than
/// left lingering), but with `nowcast_median: null` — see [`nowcast_withheld`].
const NOWCAST_MIN_OBSERVATIONS: usize = 2;
/// Plausible band for the ratio-carry divisor (`posted median ÷ official
/// median`). Czech posted salaries advertise a base against ISPV's gross
/// earnings, so the real relationship lives around 0.6–1.3; a cell whose stored
/// history claims postings pay a QUARTER (or four times) the official median is
/// not describing a wage relationship — it is a mis-join, a unit slip, or a
/// corrupted revision. Dividing by such a ratio mints a "projected median" that
/// is wrong by the same factor, with full numeric authority.
const NOWCAST_RATIO_MIN: f64 = 0.25;
const NOWCAST_RATIO_MAX: f64 = 4.0;
/// Beyond this anchor age the nowcast's official-grade claim is degraded.
/// `updated_at` on a `wages` row moves only when the official number CHANGES
/// (an unchanged upsert bumps `last_seen`, not `updated_at`), so this measures
/// "the official figure has not moved since", not "we have not fetched". ISPV
/// revises on a quarterly-to-annual cycle: 400 days is a full annual revision
/// plus a quarter of slack — an anchor that has missed that has missed the only
/// event that could have corrected it.
const NOWCAST_ANCHOR_STALE_DAYS: i64 = 400;

/// ARES business register — key-free JSON REST lookup of one economic subject
/// by IČO. Enriches the employers behind this run's vacancy samples.
const ARES_URL: &str = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty";
/// Per-run cap on NEW ARES lookups — this is enrichment, not a crawl; the
/// backlog drains across daily runs (already-enriched IČOs are skipped).
const ARES_MAX_LOOKUPS_DEFAULT: u64 = 50;
/// Cap on CZ-NACE activity codes kept per employer record.
const ARES_NACE_CAP: usize = 12;
/// Bulk-read cap for the trends revision scan — one `changes_since` query
/// replaces a per-cell `history()` round-trip. Comfortably above ~10 days of
/// `|ALL|` revisions at default cardinality; a hit is logged, since a silent
/// truncation would shorten some cells' trend windows.
const TRENDS_REVISION_SCAN: i64 = 50_000;
/// Vacancy survival ledger: one compact artifact per run + a pointer record so
/// the next run can locate and diff it (`read_source_artifact` needs the
/// writing job's id, which changes every run).
const LEDGER_DATASET: &str = "vacancy_ledger";
const LEDGER_ARTIFACT: &str = "vacancy-ledger.json";
/// Lifecycle aggregate — lives in the shared `cz-labour` namespace (same
/// pattern as `salary_gap`): a labour-market product, not app-internal state.
const LIFECYCLE_DATASET: &str = "vacancy_lifecycle";
/// If more days than this passed since the prior ledger run, close NOTHING —
/// carry the ledger forward. A missed run must not mark ~300k postings closed.
const MAX_GAP_DAYS_DEFAULT: i64 = 3;
/// A closed posting whose (IČO, czIsco, kraj, salary band) reappears within
/// this many days is a repost, not a fill — link it and de-noise the metric.
const REPOST_WINDOW_DAYS_DEFAULT: i64 = 30;
/// CZK width of the salary bands used for repost matching.
const SALARY_BAND_CZK: f64 = 5_000.0;
/// Bulk-read cap for the ARES skip-set — one `list()` replaces a per-IČO `get()`.
/// Above the `employers` dataset's realistic size; a hit is logged, since a
/// truncated set would re-fetch already-known IČOs.
const EMPLOYERS_SCAN: i64 = 100_000;

#[async_trait]
impl ScrapeApp for MpsvVpm {
    fn name(&self) -> &'static str {
        "mpsv-vpm"
    }

    fn description(&self) -> &'static str {
        "Czech national job-vacancy register (MPSV / ÚP ČR open data, key-free, CC BY 4.0). \
         Aggregates the ~300k live postings into `role_region_agg` (CZ-ISCO × kraj × orgType: \
         count + monthly-salary distribution; kraj `ALL` = national) and `vacancy_samples` \
         (JD references). Also derives `skill_demand` (per CZ-ISCO unit group × skill id: \
         posting count, share of the group, salary distribution) and `education_agg` \
         (per unit group × education level: salary median + premium vs the group median). \
         Also joins posted salaries against mpsv-ispv official ISPV \
         statistics into `cz-labour/salary_gap` (per CZ-ISCO unit group × sphere), \
         and nowcasts the current official-grade median into \
         `cz-labour/salary_nowcast` (deterministic ratio-carry: median \
         posted-vs-ISPV ratio over the last `nowcastWindow` stored gap \
         observations applied to today's posted median, with confidence \
         high|med|low and ISPV-anchor staleness; no gap history = no row, and \
         an implausible ratio/median or a single-observation cell ships \
         `nowcast_median: null` + `withheld` + `confidence: \"none\"` instead of \
         a number), \
         and enriches sampled employers from the key-free ARES business register \
         into `employers` (keyed by IČO: name, legal form, founded, kraj, CZ-NACE). \
         Keeps a per-posting survival ledger diffed daily into \
         `cz-labour/vacancy_lifecycle` (unit group × kraj: median/p75 days to \
         CLOSE — disappearance conflates filled/withdrawn/expired — plus repost \
         share and churn). \
         Drops stale relics: postings first posted more than \
         `maxPostedAgeDays` before the feed date are excluded (0 = keep all). \
         Params: {\"url\": endpoint override, \"maxRecords\": 0=all, \
         \"minCount\": 3 (min postings per aggregate cell), \"samplesPerGroup\": 4, \
         \"maxPostedAgeDays\": 730 (0 = keep all ages), \
         \"aresMaxLookups\": 50 (new ARES lookups per run, 0 = disable), \
         \"maxGapDays\": 3 (run gap beyond which the ledger closes nothing), \
         \"repostWindowDays\": 30 (repost matching window), \
         \"nowcastWindow\": 6 (newest salary_gap observations per cell for the ratio)}"
    }

    /// Daily full sync at 06:00 UTC. Change detection makes the output meaningful
    /// even on a full re-fetch (only new/changed aggregate cells are reported).
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 6 * * *")
    }

    fn default_params(&self) -> Value {
        json!({
            "maxRecords": 0,
            "minCount": 3,
            "samplesPerGroup": 4,
            "maxPostedAgeDays": 730,
            "aresMaxLookups": ARES_MAX_LOOKUPS_DEFAULT,
            "maxGapDays": MAX_GAP_DAYS_DEFAULT,
            "repostWindowDays": REPOST_WINDOW_DAYS_DEFAULT,
            "nowcastWindow": NOWCAST_WINDOW_DEFAULT,
        })
    }

    /// Every property below is a param the `run` body actually reads, with the
    /// bounds the code actually enforces (`clamp`/`max`/`min`), so an agent that
    /// respects the schema never has a value silently rewritten under it.
    /// `default_params` above is a valid instance of this schema — the registry
    /// test enforces that for scheduled apps.
    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "Feed override (default: the ~188 MB national volna-mista.json). Point at a trimmed mirror for a cheap smoke run."
                    },
                    "maxRecords": {
                        "type": "integer", "minimum": 0,
                        "description": "Cap on postings considered; 0 = the whole feed. A cap makes the run cheap but the aggregates unrepresentative."
                    },
                    "minCount": {
                        "type": "integer", "minimum": 1,
                        "description": "Minimum postings (or closures) per cell before it is published — the statistical/privacy floor on every aggregate. Default 3; lowering it publishes thin cells."
                    },
                    "samplesPerGroup": {
                        "type": "integer", "minimum": 1, "maximum": 50,
                        "description": "Vacancy samples kept per CZ-ISCO unit group."
                    },
                    "maxPostedAgeDays": {
                        "type": "integer", "minimum": 0,
                        "description": "Drop postings first posted more than this many days before the feed date; 0 = keep every age. Does NOT affect the survival ledger, which sees every live posting."
                    },
                    "aresMaxLookups": {
                        "type": "integer", "minimum": 0, "maximum": 500,
                        "description": "New ARES employer lookups this run (0 disables enrichment). The backlog drains across daily runs."
                    },
                    "maxGapDays": {
                        "type": "integer", "minimum": 1,
                        "description": "Run gap beyond which the vacancy ledger closes NOTHING and carries forward — the guard against one outage marking ~300k postings closed."
                    },
                    "repostWindowDays": {
                        "type": "integer", "minimum": 1,
                        "description": "Window in which a reappearing (IČO, occupation, kraj, salary band) counts as a repost rather than a fill, and how long closures stay in the lifecycle aggregate."
                    },
                    "nowcastWindow": {
                        "type": "integer", "minimum": 1, "maximum": 24,
                        "description": "Newest stored salary_gap observations per cell used for the ratio-carry nowcast."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Full daily national sync — every aggregate, the ledger diff, \
                                  the nowcast and 50 ARES lookups (the scheduled default)",
                    params: json!({
                        "maxRecords": 0,
                        "minCount": 3,
                        "samplesPerGroup": 4,
                        "maxPostedAgeDays": 730,
                        "aresMaxLookups": ARES_MAX_LOOKUPS_DEFAULT,
                        "maxGapDays": MAX_GAP_DAYS_DEFAULT,
                        "repostWindowDays": REPOST_WINDOW_DAYS_DEFAULT,
                        "nowcastWindow": NOWCAST_WINDOW_DEFAULT,
                    }),
                },
                ManifestExample {
                    description: "Cheap smoke run: first 20k postings, no ARES enrichment — \
                                  exercises the whole pipeline in seconds. Aggregates from a \
                                  truncated feed are NOT representative; don't publish them.",
                    params: json!({ "maxRecords": 20_000, "aresMaxLookups": 0 }),
                },
                ManifestExample {
                    description: "Current-market view: only postings from the last 180 days, \
                                  strict cell floor of 10 for tighter statistics",
                    params: json!({ "maxPostedAgeDays": 180, "minCount": 10 }),
                },
            ],
            output_shape: Some(
                "{feedRecords, considered, kept, filteredOld, agg*/region*/samples*/skill*/\
                 education*/trend* tallies, trendingTop[], fadingTop[], salaryGap{…}, \
                 salaryNowcast{…}, employers{…}, vacancyLedger{…}, freshness{…}} — writes \
                 mpsv-vpm/{role_region_agg, region_agg, vacancy_samples, skill_demand, \
                 education_agg, role_trends, employers, freshness, vacancy_ledger} and the \
                 shared cz-labour/{salary_gap, salary_nowcast, vacancy_lifecycle}",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx
            .params
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or(FULL_URL)
            .to_string();
        let max_records = ctx
            .params
            .get("maxRecords")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize;
        let min_count = ctx
            .params
            .get("minCount")
            .and_then(Value::as_u64)
            .unwrap_or(3)
            .max(1) as usize;
        let samples_per_group = ctx
            .params
            .get("samplesPerGroup")
            .and_then(Value::as_u64)
            .unwrap_or(4)
            .clamp(1, 50) as usize;
        let max_posted_age_days = ctx
            .params
            .get("maxPostedAgeDays")
            .and_then(Value::as_i64)
            .unwrap_or(730);

        // Bulk download — skip the response cache (188 MB) and always hit network.
        // Raise the timeout for THIS request only (the ~188 MB feed needs more than
        // the 30s client-global default) instead of degrading the fleet-wide
        // timeout, which would let any hung host hold a worker slot for 300s.
        let mut req = HttpRequest::get(&url);
        req.no_cache = true;
        req.timeout_secs = Some(300);
        let resp = ctx.engines.http.fetch(req).await?;
        if !resp.is_success() {
            return Err(Error::App(format!(
                "mpsv-vpm: {url} returned status {} (body starts: {})",
                resp.status,
                resp.body.chars().take(180).collect::<String>()
            )));
        }
        let feed: Feed = serde_json::from_str(&resp.body).map_err(|e| {
            Error::App(format!("mpsv-vpm: response was not the expected JSON: {e}"))
        })?;
        drop(resp); // free the ~188 MB source string before aggregating

        // Schema drift and an empty publication are DIFFERENT claims, and
        // `#[serde(default)]` used to erase the difference into a clean
        // `feedRecords: 0` success (see `feed_postings`).
        let postings = feed_postings(feed).map_err(|why| {
            Error::App(format!("mpsv-vpm: source contract drift at {url}: {why}"))
        })?;

        let total = postings.len();
        // The size floor: only the DEFAULT national feed at full width is judged
        // (see `implausibly_small_feed`). Checked before ANY aggregation, so a
        // collapsed feed republishes nothing at all.
        if implausibly_small_feed(total, url == FULL_URL, max_records) {
            return Err(Error::App(format!(
                "mpsv-vpm: the national vacancy register carried only {total} postings (floor \
                 {MIN_PLAUSIBLE_POSTINGS}, normal ~300k) — refusing to recompute national \
                 aggregates from a collapsed feed. Nothing was written, so `role_region_agg`, \
                 `region_agg` and the `cz-labour` products keep yesterday's values instead of \
                 being overwritten with near-empty cells."
            )));
        }
        let considered = if max_records == 0 {
            total
        } else {
            total.min(max_records)
        };

        // Reference "today" = the most recent change date in the feed (≈ its
        // publish date); posting age and the recency cutoff are measured from it.
        let ref_date: Option<NaiveDate> = postings
            .iter()
            .take(considered)
            .filter_map(|p| p.changed_date())
            .max();
        let posted_cutoff: Option<NaiveDate> = match (max_posted_age_days > 0, ref_date) {
            (true, Some(rd)) => Some(rd - Duration::days(max_posted_age_days)),
            _ => None,
        };
        let mut filtered_old = 0usize; // dropped as relics (posted before the cutoff)
        let mut kept = 0usize;
        let mut posted_ages: Vec<i64> = Vec::new();

        // --- aggregate in memory ---
        let mut cells: HashMap<(String, String, String), Cell> = HashMap::new();
        // region rollups over ALL occupations: (krajId, orgType) — the true
        // regional salary distribution powering the locality map headline.
        let mut regions: HashMap<(String, String), Cell> = HashMap::new();
        let mut groups: HashMap<String, Vec<Sample>> = HashMap::new();
        // posted-salary distribution per (CZ-ISCO unit group × ISPV sphere) —
        // the join side for the posted-vs-official gap benchmark. ISPV publishes
        // at the 4-digit unit-group level only, so posted salaries are pooled
        // there from the raw points (medians can't be recombined from finer cells).
        let mut gap_cells: HashMap<(String, String), Cell> = HashMap::new();
        // Skills-demand + education aggregates — the two non-salary dimensions the
        // app already parses off every posting but never persisted. Keyed by
        // (unit group, codebook id); `group_all` is the per-group total (postings
        // + salary distribution) that is the denominator for a skill's demand
        // share and the baseline for an education level's salary premium.
        let mut skill_demand: HashMap<(String, String), Cell> = HashMap::new();
        let mut education_agg: HashMap<(String, String), Cell> = HashMap::new();
        let mut group_all: HashMap<String, Cell> = HashMap::new();
        // Survival-ledger view of TODAY's feed: portalId → the compact tuple the
        // diff needs. Collected BEFORE the recency filter — a stale relic is
        // still a live posting; dropping it here would falsely close it.
        let mut ledger_today: HashMap<String, TodayPosting> = HashMap::new();
        // gather a few extra candidates per group, then keep only the richest N
        let gather_cap = samples_per_group.saturating_mul(6).max(samples_per_group);

        for p in postings.iter().take(considered) {
            // Survival ledger: track every classifiable posting with a stable id.
            if let (Some(pid), Some(cz)) = (p.portalId, p.czisco()) {
                ledger_today.insert(
                    pid.to_string(),
                    TodayPosting {
                        czisco: cz,
                        kraj: p.kraj(),
                        band: salary_band(p.monthly_salary_point()),
                        ico: p
                            .zamestnavatel
                            .as_ref()
                            .and_then(|z| z.ico.as_deref())
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string),
                    },
                );
            }
            // Recency filter: drop ancient relics (posted before the cutoff). A
            // posting with no posting date can't be aged, so it is kept.
            let posted = p.posted_date();
            if let (Some(cut), Some(pd)) = (posted_cutoff, posted) {
                if pd < cut {
                    filtered_old += 1;
                    continue;
                }
            }
            kept += 1;
            if let (Some(rd), Some(pd)) = (ref_date, posted) {
                posted_ages.push((rd - pd).num_days().max(0));
            }

            let org = p.org_type();
            let kraj = p.kraj();
            let salary = p.monthly_salary_point();

            // Region roll-ups FIRST — they key on (kraj, orgType) only and need
            // no occupation code, so they must not sit behind the CZ-ISCO gate
            // below. They used to, which silently excluded every unclassified
            // posting from the dataset documented as "the true regional salary
            // distribution" (bughunt 2026-07-14 #2).
            for key in region_rollup_keys(kraj.as_deref(), &org) {
                regions.entry(key).or_default().add(salary);
            }

            let czisco = match p.czisco() {
                Some(c) => c,
                None => continue, // unclassifiable postings can't feed the OCCUPATION products
            };

            // regional cell (when kraj known) + national ALL cell
            if let Some(k) = &kraj {
                cells
                    .entry((czisco.clone(), k.clone(), org.clone()))
                    .or_default()
                    .add(salary);
            }
            cells
                .entry((czisco.clone(), "ALL".to_string(), org.clone()))
                .or_default()
                .add(salary);

            let ug = unit_group(&czisco);

            // gap-benchmark pool: unit group × sphere (only rows with a salary count)
            gap_cells
                .entry((ug.clone(), sphere_for_org(&org).to_string()))
                .or_default()
                .add(salary);

            // Skills demand + education aggregates: zero extra fetch/parse — the ids
            // are already deserialized on `p`. Codebook ids are opaque URIs
            // ("Dovednost/…"); key on them as-is, never substring-match.
            group_all.entry(ug.clone()).or_default().add(salary);
            if let Some(skills) = &p.pozadovanaDovednost {
                for sref in skills {
                    if let Some(id) = &sref.id {
                        skill_demand
                            .entry((ug.clone(), id.clone()))
                            .or_default()
                            .add(salary);
                    }
                }
            }
            if let Some(id) = p.minPozadovaneVzdelani.as_ref().and_then(|e| e.id.clone()) {
                education_agg
                    .entry((ug.clone(), id))
                    .or_default()
                    .add(salary);
            }

            // sample reservoir per CZ-ISCO unit group
            let bucket = groups.entry(ug).or_default();
            if bucket.len() < gather_cap {
                if let Some(s) = p.as_sample(&czisco, &org, kraj.as_deref(), salary) {
                    bucket.push(s);
                }
            }
        }
        // Free the ~300k typed postings (100-200 MB) now that everything downstream
        // works off the small derived collections (cells/groups/gap_cells/…). The
        // ISPV `list` and the ARES enrichment phase below do sequential governed
        // HTTP fetches taking minutes; without this the whole corpus stays resident
        // across those network waits while other apps run concurrently. (Mirrors the
        // existing `drop(resp)` — extended to the larger, longer-lived parsed side.)
        drop(postings);

        // aggregate cells that clear the min-count threshold (statistically usable)
        let mut agg_items: Vec<(String, Value)> = Vec::new();
        for ((czisco, kraj, org), cell) in &cells {
            if cell.count < min_count {
                continue;
            }
            agg_items.push((
                format!("{czisco}|{kraj}|{org}"),
                cell.to_value(czisco, kraj, org),
            ));
        }

        // region rollups (all clear min_count except never-empty national)
        let mut region_items: Vec<(String, Value)> = Vec::new();
        for ((kraj, org), cell) in &regions {
            if cell.count < min_count {
                continue;
            }
            region_items.push((format!("{kraj}|{org}"), cell.to_region_value(kraj, org)));
        }

        // keep the richest N samples per group
        let mut sample_items: Vec<(String, Value)> = Vec::new();
        for (group, mut list) in groups {
            // richest first, then most-recently posted (undated last)
            list.sort_by(|a, b| {
                b.richness
                    .cmp(&a.richness)
                    .then_with(|| b.posted.cmp(&a.posted))
            });
            for (i, s) in list.into_iter().take(samples_per_group).enumerate() {
                sample_items.push((format!("{group}|{i}"), s.value));
            }
        }

        // freshness summary over the kept (recency-filtered) corpus
        posted_ages.sort_unstable();
        let median_posted_age = posted_ages.get(posted_ages.len() / 2).copied();
        let n = posted_ages.len().max(1);
        let within = |d: i64| (posted_ages.iter().filter(|&&a| a <= d).count() * 100 / n) as i64;
        let freshness = json!({
            "refDate": ref_date.map(|d| d.to_string()),
            "kept": kept,
            "filteredOld": filtered_old,
            "withPostedDate": posted_ages.len(),
            "medianPostedAgeDays": median_posted_age,
            "postedWithin30dPct": within(30),
            "postedWithin90dPct": within(90),
            "postedWithin180dPct": within(180),
            "maxPostedAgeDays": max_posted_age_days,
        });

        // Provenance (M12). Honest-Null discipline for a feed-aggregating app:
        //   * `feed_prov` — every posting behind these rows was read out of THIS
        //     one document, so its URL IS the source URL of each record. That the
        //     row is an aggregate of many postings does not make the URL a guess:
        //     there is exactly one URL, and it is this one.
        //   * `source_url` is left Null wherever a record is a JOIN or a
        //     time-series over the store (role_trends, salary_gap, salary_nowcast,
        //     vacancy_lifecycle) or an aggregate of many fetched URLs (employers) —
        //     naming one of them would be a fabrication.
        //   * `rules_hash`/`artifact_sha` stay Null everywhere: extraction is Rust
        //     code, not a registered RuleSet, and the ~188 MB body is never
        //     archived (it is dropped as soon as it is parsed).
        let feed_prov = || Provenance {
            source_url: Some(url.clone()),
            ..Default::default()
        };
        // The `cz-labour` products are written straight through `ctx.datasets`
        // (they belong to a shared namespace no app owns), which bypasses the
        // context's automatic stamping — so today they carry NO provenance at
        // all. The producing job is a fact we do know; stamp it explicitly.
        let job_prov = Provenance {
            job_id: Some(ctx.job_id.to_string()),
            ..Default::default()
        };
        let agg = ctx
            .upsert_many_with_provenance("role_region_agg", &agg_items, feed_prov())
            .await?;
        let region = ctx
            .upsert_many_with_provenance("region_agg", &region_items, feed_prov())
            .await?;
        let samples = ctx
            .upsert_many_with_provenance("vacancy_samples", &sample_items, feed_prov())
            .await?;
        ctx.upsert_with_provenance("freshness", "current", &freshness, feed_prov())
            .await?;

        // Skills demand + education aggregates (same min-count gate as the salary
        // cells). Persisting them turns the salary table into labour-market
        // intelligence — and once stored, role_trends' revision technique applies
        // verbatim to give rising/fading skills next run.
        let mut skill_items: Vec<(String, Value)> = Vec::new();
        for ((ug, skill_id), cell) in &skill_demand {
            if cell.count < min_count {
                continue;
            }
            let group_total = group_all.get(ug).map(|c| c.count).unwrap_or(0);
            skill_items.push((
                format!("{ug}|{skill_id}"),
                cell.to_skill_value(ug, skill_id, group_total),
            ));
        }
        let mut education_items: Vec<(String, Value)> = Vec::new();
        for ((ug, edu_id), cell) in &education_agg {
            if cell.count < min_count {
                continue;
            }
            let group_median = group_all.get(ug).and_then(Cell::median);
            education_items.push((
                format!("{ug}|{edu_id}"),
                cell.to_education_value(ug, edu_id, group_median),
            ));
        }
        let skill = ctx
            .upsert_many_with_provenance("skill_demand", &skill_items, feed_prov())
            .await?;
        let education = ctx
            .upsert_many_with_provenance("education_agg", &education_items, feed_prov())
            .await?;

        // Trending vs fading roles: national posting-count trajectories from
        // role_region_agg's revision history (the change-intelligence
        // substrate). Window = the cell's last 10 revisions, i.e. roughly its
        // last 10 *changed* days; unchanged days write no revision.
        //
        // ONE bulk `changes_since` read (newest-first, grouped by key) replaces the
        // per-cell `history()` round-trip — ~1.5–4k sequential SQLite queries per
        // run collapse to a single scan. Each key's window is truncated to 10 to
        // preserve the prior `history(key, 10)` semantics exactly.
        let all_revs = ctx
            .datasets
            // Unfiltered by trust: the trend window is this app's own history and
            // must not silently shorten because a run was written while the source
            // was degrading — a short window is a wrong trend, not a safe one.
            .changes_since(
                &ctx.app,
                Some("role_region_agg"),
                None,
                TRENDS_REVISION_SCAN,
                None,
            )
            .await?;
        if all_revs.len() as i64 >= TRENDS_REVISION_SCAN {
            tracing::warn!(
                scanned = all_revs.len(),
                "mpsv-vpm: role_region_agg revision scan hit the cap — some trend windows may be short"
            );
        }
        let mut revs_by_key: std::collections::HashMap<String, Vec<pumper_core::Revision>> =
            std::collections::HashMap::new();
        for rev in all_revs {
            revs_by_key.entry(rev.key.clone()).or_default().push(rev);
        }

        let mut trend_items: Vec<(String, Value)> = Vec::new();
        for (key, _) in agg_items.iter().filter(|(k, _)| k.contains("|ALL|")) {
            // The cell's newest ≤10 revisions — the same window `history(key, 10)`
            // returned (changes_since arrives newest-first, so no re-sort needed).
            let window: &[pumper_core::Revision] = revs_by_key
                .get(key)
                .map_or(&[][..], |v| &v[..v.len().min(10)]);
            let count_of = |rev: &pumper_core::Revision| {
                rev.data
                    .as_ref()
                    .and_then(|d| d.get("count"))
                    .and_then(Value::as_i64)
            };
            let Some(latest) = window.first().and_then(count_of) else {
                continue;
            };
            // Oldest snapshot within the window; None = the cell is brand new.
            let prev = window.iter().skip(1).filter_map(count_of).next_back();
            let (prev_count, delta, trend) = match prev {
                Some(p) if latest > p => (p, latest - p, "rising"),
                Some(p) if latest < p => (p, latest - p, "falling"),
                Some(p) => (p, 0, "flat"),
                None => (0, latest, "new"),
            };
            let mut parts = key.split('|');
            let czisco = parts.next().unwrap_or_default();
            let org = parts.nth(1).unwrap_or_default();
            trend_items.push((
                format!("{czisco}|{org}"),
                json!({
                    "czIsco": czisco,
                    "orgType": org,
                    "count": latest,
                    "prevCount": prev_count,
                    "delta": delta,
                    "pctChange": (prev_count > 0)
                        .then(|| (delta as f64 / prev_count as f64 * 100.0).round()),
                    "revisions": window.len(),
                    "trend": trend,
                }),
            ));
        }
        let trends = ctx.upsert_many("role_trends", &trend_items).await?;
        let top = |dir: i64| -> Vec<&Value> {
            let mut movers: Vec<&(String, Value)> = trend_items
                .iter()
                .filter(|(_, v)| v["delta"].as_i64().unwrap_or(0) * dir > 0)
                .collect();
            movers.sort_by_key(|(_, v)| -(v["delta"].as_i64().unwrap_or(0) * dir));
            movers.into_iter().take(15).map(|(_, v)| v).collect()
        };
        let trending_top = top(1);
        let fading_top = top(-1);

        // ── Posted-vs-official salary gap benchmark ─────────────────────────
        // Joins this run's POSTED distribution against mpsv-ispv's OFFICIAL
        // (ISPV) `wages` dataset, read cross-app from the store. Computed HERE
        // (not in mpsv-ispv) because this app runs daily with the raw posted
        // salary points in memory — the honest unit-group median needs them —
        // while ISPV refreshes only quarterly and its rows persist in the
        // store between runs. Output goes to the virtual shared namespace
        // `cz-labour` (grants-common pattern) so neither app owns the join.
        let official_rows = ctx.datasets.list(ISPV_APP, ISPV_DATASET, 5_000).await?;
        let official = official_wage_index(official_rows.iter().map(|r| &r.data));
        let salary_gap = if official.is_empty() {
            json!({ "skipped": "no official ISPV wages in store (run mpsv-ispv first)" })
        } else {
            let gap_items = compute_salary_gaps(&gap_cells, &official, min_count);
            let matched_groups: std::collections::HashSet<&str> =
                gap_items.iter().map(|(k, _)| k.as_str()).collect();
            let unmatched_posted = gap_cells
                .iter()
                .filter(|((g, s), c)| {
                    c.salaries.len() >= min_count
                        && !matched_groups.contains(format!("{g}|{s}").as_str())
                })
                .count();
            let gap_sum = ctx
                .datasets
                .upsert_many_stamped(GAP_APP, GAP_DATASET, &gap_items, None, Some(&job_prov))
                .await?;
            let top_gaps = |dir: f64| -> Vec<Value> {
                let mut v: Vec<&(String, Value)> = gap_items
                    .iter()
                    .filter(|(_, r)| r["gapPct"].as_f64().unwrap_or(0.0) * dir > 0.0)
                    .collect();
                v.sort_by(|a, b| {
                    let f = |r: &Value| r["gapPct"].as_f64().unwrap_or(0.0) * dir;
                    f(&b.1).total_cmp(&f(&a.1))
                });
                v.into_iter()
                    .take(10)
                    .map(|(_, r)| {
                        json!({
                            "czIscoGroup": r["czIscoGroup"],
                            "sfera": r["sfera"],
                            "postedMedian": r["postedMedian"],
                            "officialMedian": r["officialMedian"],
                            "gapPct": r["gapPct"],
                        })
                    })
                    .collect()
            };
            json!({
                "cells": gap_items.len(),
                "new": gap_sum.new.len(),
                "changed": gap_sum.changed.len(),
                "unchanged": gap_sum.unchanged,
                "officialRows": official.len(),
                "unmatchedPostedGroups": unmatched_posted,
                "topPostedAboveOfficial": top_gaps(1.0),
                "topPostedBelowOfficial": top_gaps(-1.0),
            })
        };

        // ── Salary nowcast (deterministic ratio-carry, NOT a model) ─────────
        // Per (unit group × sphere): median posted-vs-official ratio over the
        // newest `nowcastWindow` stored `salary_gap` revisions (this run's just
        // written revision included), applied to today's posted median →
        // projected official-grade median. Runs AFTER the gap upsert so the
        // freshest observed ratio participates. No stored history ⇒ no row.
        let salary_nowcast = if official.is_empty() {
            json!({ "skipped": "no official ISPV wages in store (run mpsv-ispv first)" })
        } else {
            let nowcast_window = ctx
                .params
                .get("nowcastWindow")
                .and_then(Value::as_u64)
                .unwrap_or(NOWCAST_WINDOW_DEFAULT)
                .clamp(1, 24) as usize;
            let gap_revs = ctx
                .datasets
                // Unfiltered by trust — same posture as the trends scan: a
                // silently shortened ratio window is a wrong nowcast, not a
                // safe one.
                .changes_since(
                    GAP_APP,
                    Some(GAP_DATASET),
                    None,
                    NOWCAST_REVISION_SCAN,
                    None,
                )
                .await?;
            if gap_revs.len() as i64 >= NOWCAST_REVISION_SCAN {
                tracing::warn!(
                    scanned = gap_revs.len(),
                    "mpsv-vpm: salary_gap revision scan hit the cap — some nowcast ratio windows may be short"
                );
            }
            // Group newest-first (changes_since order) per cell key, then
            // reduce each cell to its windowed ratio observations.
            let mut revs_by_key: HashMap<String, Vec<pumper_core::Revision>> = HashMap::new();
            for rev in gap_revs {
                revs_by_key.entry(rev.key.clone()).or_default().push(rev);
            }
            let ratios_by_key: HashMap<String, Vec<f64>> = revs_by_key
                .iter()
                .map(|(k, revs)| {
                    (
                        k.clone(),
                        ratio_observations(revs.iter().map(|r| r.data.as_ref()), nowcast_window),
                    )
                })
                .collect();
            let anchors = ispv_anchor_dates(&official_rows);
            let nowcast_items = compute_salary_nowcast(
                &gap_cells,
                &ratios_by_key,
                &anchors,
                Utc::now().date_naive(),
                min_count,
            );
            let nc_sum = ctx
                .datasets
                .upsert_many_stamped(
                    GAP_APP,
                    NOWCAST_DATASET,
                    &nowcast_items,
                    None,
                    Some(&job_prov),
                )
                .await?;
            let confidence_count = |level: &str| {
                nowcast_items
                    .iter()
                    .filter(|(_, v)| v["confidence"] == level)
                    .count()
            };
            let withheld_count = |reason: &str| {
                nowcast_items
                    .iter()
                    .filter(|(_, v)| v["withheld"] == reason)
                    .count()
            };
            json!({
                "method": "ratio_carry",
                "cells": nowcast_items.len(),
                "new": nc_sum.new.len(),
                "changed": nc_sum.changed.len(),
                "unchanged": nc_sum.unchanged,
                "window": nowcast_window,
                "confidenceHigh": confidence_count("high"),
                "confidenceMed": confidence_count("med"),
                "confidenceLow": confidence_count("low"),
                // Rows that shipped WITHOUT a number, by the guard that refused
                // it — a run whose nowcast quietly emptied is legible here, not
                // only as a drop in `confidenceHigh`.
                "withheldThinEvidence": withheld_count("thin_evidence"),
                "withheldImplausibleRatio": withheld_count("implausible_ratio"),
                "withheldOutOfBand": withheld_count("out_of_band"),
                "anchorStale": nowcast_items
                    .iter()
                    .filter(|(_, v)| v["anchor_stale"] == true)
                    .count(),
            })
        };

        // ── ARES employer enrichment ────────────────────────────────────────
        // The persisted vacancy samples carry the employer IČO; look the new
        // ones up in the key-free ARES business register and persist a compact
        // `employers` record (keyed by IČO). Capped per run — enrichment, not
        // a crawl; the engine's politeness governor + TTL cache handle
        // rate/duplication, and the backlog drains across daily runs. A
        // malformed/404 response skips that IČO with a warn, never fails the run.
        let ares_max = ctx
            .params
            .get("aresMaxLookups")
            .and_then(Value::as_u64)
            .unwrap_or(ARES_MAX_LOOKUPS_DEFAULT)
            .min(500) as usize;
        let icos = distinct_icos(sample_items.iter().map(|(_, v)| v));
        // ONE bulk read of the employers dataset into a skip-set, instead of a
        // `get()` per IČO (n+1 → 1). Read before the lookup loop, so it reflects
        // prior runs' enrichment (same as the per-key gets did).
        let known_employers: std::collections::HashSet<String> = ctx
            .datasets
            .list(&ctx.app, "employers", EMPLOYERS_SCAN)
            .await?
            .into_iter()
            .map(|r| r.key)
            .collect();
        if known_employers.len() as i64 >= EMPLOYERS_SCAN {
            tracing::warn!(
                known = known_employers.len(),
                "mpsv-vpm: employers skip-set hit the cap — some known IČOs may be re-fetched"
            );
        }
        // Durable execution (M23). This loop is the one genuinely resumable unit
        // of work in the job: up to `aresMaxLookups` SEQUENTIAL, politeness-
        // governed HTTP lookups against a third party (minutes of wall clock),
        // whose results were persisted only after the last one — so a reap,
        // timeout or shutdown mid-phase discarded every lookup already paid for,
        // and the retry re-fetched them all. Checkpointing the per-item progress
        // makes a re-claim resume where it stopped.
        //
        // Everything BEFORE this phase is deliberately not checkpointed: the
        // ~188 MB feed arrives as one document (no page cursor to save), and the
        // in-memory aggregation over ~300k postings has no intermediate state
        // worth persisting — a snapshot of it would be larger than the work it
        // saves, and it is re-derived in seconds once the body is in hand. The
        // ledger diff is likewise a pure function of the feed plus the PRIOR
        // run's artifact, so a retry recomputes it identically.
        let resumed = ctx
            .restore()
            .and_then(AresCheckpoint::from_value)
            .unwrap_or_default();
        let mut employer_items: Vec<(String, Value)> = resumed.records;
        let resumed_count = employer_items.len();
        let already_fetched: std::collections::HashSet<String> =
            employer_items.iter().map(|(k, _)| k.clone()).collect();
        let mut ares_skipped = 0usize; // already enriched in a prior run
        let mut ares_failed = resumed.failed; // transport / 404 / malformed
        let mut ares_capped = 0usize; // left for a later run (per-run cap)
        let mut ares_looked_up = resumed.looked_up;
        let lookups_before = ares_looked_up;
        for ico in &icos {
            // a prior attempt of THIS job already fetched it → don't pay twice
            if already_fetched.contains(ico) {
                continue;
            }
            // already in the employers dataset → nothing to fetch
            if known_employers.contains(ico) {
                ares_skipped += 1;
                continue;
            }
            if ares_looked_up >= ares_max {
                ares_capped += 1;
                continue;
            }
            ares_looked_up += 1;
            match fetch_ares_employer(&ctx, ico).await {
                Some(rec) => employer_items.push((ico.clone(), rec)),
                None => ares_failed += 1,
            }
            // Runtime-throttled, so calling it every lookup is cheap; the state
            // is at most `aresMaxLookups` compact employer records.
            ctx.checkpoint(AresCheckpoint::to_value(
                &employer_items,
                ares_looked_up,
                ares_failed,
            ))
            .await;
        }
        if ares_looked_up > lookups_before {
            // Unthrottled final snapshot: losing the tail of the phase to the
            // throttle would re-spend real lookups on the next attempt.
            ctx.checkpoint_now(AresCheckpoint::to_value(
                &employer_items,
                ares_looked_up,
                ares_failed,
            ))
            .await;
        }
        // No `source_url`: this batch is an aggregate of one ARES URL PER RECORD,
        // and stamping any single one of them batch-wide would be a fabrication.
        // (Per-record stamping would mean leaving `upsert_many`, which is also
        // the derived-spec seam — not worth losing for one field.)
        let employers = ctx.upsert_many("employers", &employer_items).await?;
        let employer_summary = json!({
            "distinctIcos": icos.len(),
            "enriched": employer_items.len(),
            "resumedFromCheckpoint": resumed_count,
            "new": employers.new.len(),
            "changed": employers.changed.len(),
            "unchanged": employers.unchanged,
            "skippedExisting": ares_skipped,
            "capped": ares_capped,
            "failed": ares_failed,
            "maxLookups": ares_max,
        });

        // ── Vacancy survival ledger ─────────────────────────────────────────
        // Diff today's per-posting view against the prior run's ledger artifact
        // to learn which postings disappeared (time-to-CLOSE — filled, withdrawn
        // or expired; the feed can't tell which), detect reposts, and aggregate
        // the rolling closed window into `cz-labour/vacancy_lifecycle`. The
        // ledger itself is ONE compact artifact per run; a `vacancy_ledger`
        // pointer record (artifact_path + job_id) lets the next run find it via
        // `read_source_artifact`. Every day not captured is lost forever, so a
        // broken/missing prior ledger restarts the ledger with a warn — it must
        // never fail the run.
        let max_gap_days = ctx
            .params
            .get("maxGapDays")
            .and_then(Value::as_i64)
            .unwrap_or(MAX_GAP_DAYS_DEFAULT)
            .max(1);
        let repost_window_days = ctx
            .params
            .get("repostWindowDays")
            .and_then(Value::as_i64)
            .unwrap_or(REPOST_WINDOW_DAYS_DEFAULT)
            .max(1);
        let ledger_date = ref_date.unwrap_or_else(|| chrono::Utc::now().date_naive());
        let prior_ledger: Option<Ledger> = match ctx
            .datasets
            .get(&ctx.app, LEDGER_DATASET, "current")
            .await?
        {
            Some(rec) => match ctx.read_source_artifact(&ctx.app, &rec).await {
                Ok(body) => match serde_json::from_str::<Ledger>(&body) {
                    Ok(l) => Some(l),
                    Err(e) => {
                        tracing::warn!(
                            "mpsv-vpm: prior vacancy ledger unparseable ({e}) — restarting ledger"
                        );
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(
                        "mpsv-vpm: prior vacancy ledger unreadable ({e}) — restarting ledger"
                    );
                    None
                }
            },
            None => None,
        };
        let had_prior = prior_ledger.is_some();
        let diff = diff_ledger(
            prior_ledger,
            &ledger_today,
            ledger_date,
            max_gap_days,
            repost_window_days,
        );
        if diff.carried && had_prior {
            tracing::warn!(
                gap_days = diff.gap_days,
                max_gap_days,
                "mpsv-vpm: ledger run gap outside tolerance — carried forward, no closures recorded"
            );
        }
        // Live posting counts per (unit group × kraj) — the churn denominator.
        let mut live_counts: HashMap<(String, String), usize> = HashMap::new();
        for t in ledger_today.values() {
            let ug = unit_group(&t.czisco);
            if let Some(k) = &t.kraj {
                *live_counts.entry((ug.clone(), k.clone())).or_default() += 1;
            }
            *live_counts.entry((ug, "ALL".to_string())).or_default() += 1;
        }
        let lifecycle_items = aggregate_lifecycle(
            &diff.ledger.closed,
            &live_counts,
            min_count,
            repost_window_days,
        );
        let lifecycle = ctx
            .datasets
            .upsert_many_stamped(
                GAP_APP,
                LIFECYCLE_DATASET,
                &lifecycle_items,
                None,
                Some(&job_prov),
            )
            .await?;
        let ledger_bytes = serde_json::to_vec(&diff.ledger)?;
        let ledger_len = ledger_bytes.len();
        ctx.save_artifact(LEDGER_ARTIFACT, &ledger_bytes).await?;
        // Pointer LAST — only after the artifact is durably written, so a crash
        // between the two can't leave the pointer at a nonexistent file.
        ctx.upsert(
            LEDGER_DATASET,
            "current",
            &json!({
                "artifact_path": LEDGER_ARTIFACT,
                "job_id": ctx.job_id.to_string(),
                "run_date": ledger_date.to_string(),
                "open": diff.ledger.open.len(),
                "closedInWindow": diff.ledger.closed.len(),
                "bytes": ledger_len,
            }),
        )
        .await?;
        let vacancy_ledger = json!({
            "runDate": ledger_date.to_string(),
            "open": diff.ledger.open.len(),
            "new": diff.new_now,
            "ongoing": diff.ongoing,
            "closed": diff.closed_now,
            "reposts": diff.reposts_now,
            "closedInWindow": diff.ledger.closed.len(),
            "gapDays": diff.gap_days,
            "carriedForward": diff.carried,
            "lifecycleCells": lifecycle_items.len(),
            "lifecycleNew": lifecycle.new.len(),
            "lifecycleChanged": lifecycle.changed.len(),
            "artifactBytes": ledger_len,
        });

        let out = json!({
            "source": "data.mpsv.cz/volna-mista",
            "feedRecords": total,
            "considered": considered,
            "kept": kept,
            "filteredOld": filtered_old,
            "aggCells": agg_items.len(),
            "aggNew": agg.new.len(),
            "aggChanged": agg.changed.len(),
            "aggUnchanged": agg.unchanged,
            "regionCells": region_items.len(),
            "regionNew": region.new.len(),
            "regionChanged": region.changed.len(),
            "samples": sample_items.len(),
            "samplesNew": samples.new.len(),
            "samplesChanged": samples.changed.len(),
            "skillCells": skill_items.len(),
            "skillNew": skill.new.len(),
            "skillChanged": skill.changed.len(),
            "educationCells": education_items.len(),
            "educationNew": education.new.len(),
            "educationChanged": education.changed.len(),
            "trendCells": trend_items.len(),
            "trendsChanged": trends.new.len() + trends.changed.len(),
            "trendingTop": trending_top,
            "fadingTop": fading_top,
            "salaryGap": salary_gap,
            "salaryNowcast": salary_nowcast,
            "employers": employer_summary,
            "vacancyLedger": vacancy_ledger,
            "freshness": freshness,
        });
        ctx.save_artifact("summary.json", &serde_json::to_vec_pretty(&out)?)
            .await?;
        Ok(out)
    }
}

/// The postings behind a parsed feed, or the reason the document is SCHEMA
/// DRIFT and must not be reported as a clean `feedRecords: 0` success.
///
/// `Feed::polozky` used to be `#[serde(default)]`, which erased the whole
/// distinction: a renamed key, a re-wrapped envelope and an error document that
/// happens to be valid JSON all deserialized to an empty `Vec`, aggregated to
/// nothing, wrote nothing, and reported success. On a ~300k-posting national
/// feed that is doubly invisible — `upsert_many` is a partial upsert, so no row
/// is tombstoned either and there is no data-loss alarm to notice. The only
/// observable was a `0` in a field nobody alerts on.
///
/// A present-but-EMPTY `polozky: []` is a different claim ("the register is
/// empty today") and is judged by [`implausibly_small_feed`], not here.
fn feed_postings(feed: Feed) -> std::result::Result<Vec<Posting>, &'static str> {
    feed.polozky.ok_or(
        "the document carries no `polozky` array — the source contract changed \
         (renamed/re-wrapped key, or an error envelope that parsed as JSON). This is NOT an \
         empty feed, and nothing was aggregated or written",
    )
}

/// Whether this run may publish national aggregates from a feed of `total`
/// postings.
///
/// The register carries ~300k live postings every day; [`MIN_PLAUSIBLE_POSTINGS`]
/// is 0.3% of that, so only a truncated download or a partial publication can
/// reach it. Deliberately scoped to the DEFAULT feed at full width:
///
/// * a `url` override points at a trimmed mirror **on purpose** — the manifest's
///   own smoke example does exactly that;
/// * `maxRecords` truncates **on purpose**.
///
/// A floor that judged those would refuse the runs it exists to allow. This is
/// a per-feed floor, not a global one, for the same reason mpsv-ispv's floor is
/// 50 and not 1000: the number is a property of the source, not of the fleet.
fn implausibly_small_feed(total: usize, is_default_feed: bool, max_records: usize) -> bool {
    is_default_feed && max_records == 0 && total < MIN_PLAUSIBLE_POSTINGS
}

/// The region roll-up cells one posting contributes to: its own kraj (per org
/// type and pooled `all`), plus the national `ALL` pair.
///
/// Extracted so it can be called ABOVE the CZ-ISCO gate in the aggregation loop.
/// None of these keys uses the occupation code, yet the roll-up used to sit
/// after `czisco`'s early `continue` — so every posting the feed leaves
/// unclassified was silently missing from `region_agg`, the dataset whose whole
/// claim is being "the true regional salary distribution".
fn region_rollup_keys(kraj: Option<&str>, org: &str) -> Vec<(String, String)> {
    let mut keys = Vec::with_capacity(4);
    if let Some(k) = kraj {
        keys.push((k.to_string(), org.to_string()));
        keys.push((k.to_string(), "all".to_string()));
    }
    keys.push(("ALL".to_string(), org.to_string()));
    keys.push(("ALL".to_string(), "all".to_string()));
    keys
}

/// Numeric CZ-ISCO unit group: `"CzIsco/93291"` → `"9329"` (first 4 digits of the
/// bare code). Buckets JD samples at the ISCO unit-group level.
fn unit_group(czisco: &str) -> String {
    let code = czisco.rsplit('/').next().unwrap_or(czisco);
    let digits: String = code
        .chars()
        .filter(|c| c.is_ascii_digit())
        .take(4)
        .collect();
    if digits.is_empty() {
        czisco.to_string()
    } else {
        digits
    }
}

/// ISPV sphere for a posted org type: public administration reports into the
/// salary (PLATOVA) sphere; private employers and temp agencies into the wage
/// (MZDOVA) sphere.
fn sphere_for_org(org: &str) -> &'static str {
    if org == "public" {
        "PLATOVA"
    } else {
        "MZDOVA"
    }
}

/// Reads a numeric field the ISPV feed may deliver as a JSON number OR a quoted
/// (possibly Czech-formatted) string; `as_f64` alone silently dropped the whole
/// row when a stat arrived string-encoded. Strips whitespace/NBSP thousands
/// separators and accepts a decimal comma.
fn wage_num(v: &Value, key: &str) -> Option<f64> {
    match v.get(key) {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => {
            let cleaned: String = s.chars().filter(|c| !c.is_whitespace()).collect();
            cleaned
                .parse::<f64>()
                .ok()
                .or_else(|| cleaned.replacen(',', ".", 1).parse::<f64>().ok())
        }
        _ => None,
    }
}

/// Index of official ISPV rows: (CZ-ISCO unit group, sfera) → (medianMzda,
/// mzdaPrumer). Rows without a positive monthly median are dropped — no
/// benchmark can honestly be computed against them.
fn official_wage_index<'a>(
    rows: impl Iterator<Item = &'a Value>,
) -> HashMap<(String, String), (f64, Option<f64>)> {
    let mut index = HashMap::new();
    for r in rows {
        let Some(czisco) = r.get("czIsco").and_then(Value::as_str) else {
            continue;
        };
        let sfera = r
            .get("sfera")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let Some(median) = wage_num(r, "medianMzda").filter(|m| *m > 0.0) else {
            continue;
        };
        let mean = wage_num(r, "mzdaPrumer").filter(|m| *m > 0.0);
        index.insert((unit_group(czisco), sfera), (median, mean));
    }
    index
}

/// Joins posted salary pools against the official ISPV index at their shared
/// granularity — (CZ-ISCO 4-digit unit group × sphere), the finest level ISPV
/// publishes — and computes the gap. Posted cells need `min_salaries` actual
/// salary points to be statistically usable; occupations absent from either
/// side are skipped, never estimated. Keys are `{unitGroup}|{sfera}`, sorted
/// for deterministic upserts.
fn compute_salary_gaps(
    posted: &HashMap<(String, String), Cell>,
    official: &HashMap<(String, String), (f64, Option<f64>)>,
    min_salaries: usize,
) -> Vec<(String, Value)> {
    let mut items: Vec<(String, Value)> = Vec::new();
    for ((group, sfera), cell) in posted {
        if cell.salaries.len() < min_salaries.max(1) {
            continue;
        }
        let Some((official_median, official_mean)) = official.get(&(group.clone(), sfera.clone()))
        else {
            continue; // no official row at this granularity — skip, don't fabricate
        };
        let (_, pct) = cell.stats();
        let Some(posted_median) = pct(0.5) else {
            continue;
        };
        let gap = |official: f64| -> (i64, f64) {
            let abs = posted_median as f64 - official;
            (
                abs.round() as i64,
                (abs / official * 100.0 * 10.0).round() / 10.0,
            )
        };
        let (gap_abs, gap_pct) = gap(*official_median);
        let vs_mean = official_mean.map(gap);
        items.push((
            format!("{group}|{sfera}"),
            json!({
                "czIscoGroup": group,
                "sfera": sfera,
                "postedMedian": posted_median,
                "postedSalaryCount": cell.salaries.len(),
                "postedCount": cell.count,
                "officialMedian": official_median.round() as i64,
                "officialMean": official_mean.map(|m| m.round() as i64),
                "gapAbs": gap_abs,
                "gapPct": gap_pct,
                "gapVsMeanAbs": vs_mean.map(|(a, _)| a),
                "gapVsMeanPct": vs_mean.map(|(_, p)| p),
            }),
        ));
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

/// Posted-vs-official ratio observations for one `salary_gap` cell, from its
/// revision snapshots (newest-first, as `changes_since` delivers them),
/// windowed to the newest `n`. A snapshot missing either median (or carrying a
/// non-positive one, or a 'removed' revision with no data) contributes nothing
/// — it is skipped, never guessed.
fn ratio_observations<'a>(datas: impl Iterator<Item = Option<&'a Value>>, n: usize) -> Vec<f64> {
    datas
        .filter_map(|d| {
            let d = d?;
            let posted = d
                .get("postedMedian")
                .and_then(Value::as_f64)
                .filter(|v| *v > 0.0)?;
            let official = d
                .get("officialMedian")
                .and_then(Value::as_f64)
                .filter(|v| *v > 0.0)?;
            Some(posted / official)
        })
        .take(n)
        .collect()
}

/// Median of a pre-sorted slice (even counts average the middle pair).
fn median_f64(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Confidence of a ratio-carry nowcast, from observation count + dispersion.
/// `spread` = (max − min) / median of the ratio observations. Thresholds (the
/// documented contract — see the `NOWCAST_*` constants):
///   high: ≥ 6 observations and spread ≤ 0.10
///   med:  ≥ 3 observations and spread ≤ 0.25
///   low:  anything else with ≥ 1 observation.
fn nowcast_confidence(observations: usize, spread: f64) -> &'static str {
    if observations >= NOWCAST_HIGH_MIN_OBS && spread <= NOWCAST_HIGH_MAX_SPREAD {
        "high"
    } else if observations >= NOWCAST_MED_MIN_OBS && spread <= NOWCAST_MED_MAX_SPREAD {
        "med"
    } else {
        "low"
    }
}

/// Why a computed nowcast must NOT ship its number, or `None` when it may.
///
/// The nowcast is a DIVISION (`posted median ÷ ratio_used`) whose only
/// output-side guard used to be `ratio_used <= 0.0`. Every other way the ratio
/// can be garbage — a corrupted or mis-joined revision history producing 0.02 or
/// 40 — sailed through and minted an arbitrarily implausible "projected median"
/// that then persisted with the same numeric authority as a good one. These are
/// the three ways the number is not publishable, in causal order:
///
/// 1. `implausible_ratio` — the divisor is outside
///    [`NOWCAST_RATIO_MIN`]..=[`NOWCAST_RATIO_MAX`] (or non-finite). The CAUSE.
/// 2. `out_of_band` — the projected median falls outside the same
///    [`SALARY_MIN`]..=[`SALARY_MAX`] admission band every posted salary point
///    had to clear to be counted at all. A projected "salary" the app would have
///    refused to ingest cannot be a salary it publishes.
/// 3. `thin_evidence` — fewer than [`NOWCAST_MIN_OBSERVATIONS`] stored ratio
///    observations back the divisor.
///
/// Withholding is `nowcast_median: null` on a row that still ships, NOT a
/// dropped row: these datasets are written with `upsert_many` (partial upsert,
/// no tombstoning), so a dropped key would LINGER at yesterday's published
/// number — silently keeping exactly the value the guard exists to retract.
fn nowcast_withheld(
    observations: usize,
    ratio_used: f64,
    nowcast_median: f64,
) -> Option<&'static str> {
    if !ratio_used.is_finite() || !(NOWCAST_RATIO_MIN..=NOWCAST_RATIO_MAX).contains(&ratio_used) {
        return Some("implausible_ratio");
    }
    if !nowcast_median.is_finite() || !(SALARY_MIN..=SALARY_MAX).contains(&nowcast_median) {
        return Some("out_of_band");
    }
    if observations < NOWCAST_MIN_OBSERVATIONS {
        return Some("thin_evidence");
    }
    None
}

/// Whether the ISPV anchor behind a nowcast is stale enough to judge, not merely
/// to stamp (see [`NOWCAST_ANCHOR_STALE_DAYS`]).
fn anchor_is_stale(staleness_days: i64) -> bool {
    staleness_days > NOWCAST_ANCHOR_STALE_DAYS
}

/// Confidence after judging the anchor's age: a stale anchor costs exactly one
/// level (`high` → `med`, `med` → `low`), never more. The ratio history can be
/// long and tight — that is what [`nowcast_confidence`] measures — and still be
/// carrying today's posted median onto an official figure nobody has restated
/// within a year. Dispersion cannot see that; only the anchor date can.
fn degrade_for_stale_anchor(confidence: &'static str, staleness_days: i64) -> &'static str {
    if !anchor_is_stale(staleness_days) {
        return confidence;
    }
    match confidence {
        "high" => "med",
        _ => "low",
    }
}

/// Newest ISPV anchor date per (unit group × sphere): when the official row the
/// nowcast leans on was last (re)written to the store — the vintage whose
/// staleness the nowcast record must disclose.
fn ispv_anchor_dates(rows: &[pumper_core::Record]) -> HashMap<(String, String), NaiveDate> {
    let mut anchors: HashMap<(String, String), NaiveDate> = HashMap::new();
    for r in rows {
        let Some(cz) = r.data.get("czIsco").and_then(Value::as_str) else {
            continue;
        };
        let sfera = r
            .data
            .get("sfera")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let d = r.updated_at.date_naive();
        anchors
            .entry((unit_group(cz), sfera))
            .and_modify(|e| *e = (*e).max(d))
            .or_insert(d);
    }
    anchors
}

/// Deterministic RATIO-CARRY salary nowcast — explicitly NOT a model: per
/// (CZ-ISCO unit group × sphere), `ratio_used` = median of the stored
/// posted-vs-official ratio observations, and
/// `nowcast_median` = today's posted median ÷ `ratio_used` — i.e. today's
/// posted signal deflated/inflated by the historically observed posted-vs-ISPV
/// relationship for that exact cell. Emits NO row when the cell has no stored
/// ratio history, no ISPV anchor row, or fewer than `min_salaries` posted
/// points today — a missing nowcast is honest; a fabricated one is not.
/// Keys `{unitGroup}|{sfera}`, sorted for deterministic upserts.
///
/// HONESTY FLOOR (see [`nowcast_withheld`]): a row whose divisor or projected
/// median is implausible, or whose evidence is a single observation, still ships
/// — with `nowcast_median: null`, `withheld: "<reason>"` and
/// `confidence: "none"`. The row is the retraction: dropping the key instead
/// would leave yesterday's published number in place, since this dataset is
/// written with a partial `upsert_many` and nothing tombstones it.
/// `anchor_stale` judges the ISPV vintage the projection leans on, and a stale
/// anchor costs one confidence level ([`degrade_for_stale_anchor`]).
fn compute_salary_nowcast(
    posted: &HashMap<(String, String), Cell>,
    ratios_by_key: &HashMap<String, Vec<f64>>,
    anchors: &HashMap<(String, String), NaiveDate>,
    today: NaiveDate,
    min_salaries: usize,
) -> Vec<(String, Value)> {
    let mut items: Vec<(String, Value)> = Vec::new();
    for ((group, sfera), cell) in posted {
        if cell.salaries.len() < min_salaries.max(1) {
            continue;
        }
        let key = format!("{group}|{sfera}");
        // No stored gap history ⇒ no row — never extrapolate from nothing.
        let Some(ratios) = ratios_by_key.get(&key).filter(|r| !r.is_empty()) else {
            continue;
        };
        // No official anchor row ⇒ the staleness disclosure can't be made
        // honestly ⇒ no row.
        let Some(anchor) = anchors.get(&(group.clone(), sfera.clone())) else {
            continue;
        };
        let (_, pct) = cell.stats();
        let Some(posted_median) = pct(0.5) else {
            continue;
        };
        let mut sorted = ratios.clone();
        sorted.sort_by(f64::total_cmp);
        let ratio_used = median_f64(&sorted);
        let projected = posted_median as f64 / ratio_used;
        let withheld = nowcast_withheld(ratios.len(), ratio_used, projected);
        // `spread` divides by the ratio, so it is only meaningful once the ratio
        // itself is; a withheld row reports no dispersion rather than a ratio of
        // a garbage number.
        let spread = (sorted[sorted.len() - 1] - sorted[0]) / ratio_used;
        let staleness_days = (today - *anchor).num_days();
        // A withheld row has no number, so it has no confidence IN a number —
        // reporting "high" beside a null would be the contradiction this guard
        // exists to prevent.
        let confidence = match withheld {
            Some(_) => "none",
            None => {
                degrade_for_stale_anchor(nowcast_confidence(ratios.len(), spread), staleness_days)
            }
        };
        items.push((
            key,
            json!({
                "isco4": group,
                "sfera": sfera,
                "posted_median": posted_median,
                "nowcast_median": withheld.is_none().then(|| projected.round() as i64),
                // Null when there is no number, naming which guard refused it.
                "withheld": withheld,
                "ratio_used": ratio_used
                    .is_finite()
                    .then(|| (ratio_used * 10_000.0).round() / 10_000.0),
                "observations": ratios.len(),
                "dispersion": (withheld.is_none() && spread.is_finite())
                    .then(|| (spread * 1_000.0).round() / 1_000.0),
                "confidence": confidence,
                "ispv_anchor_date": anchor.to_string(),
                "staleness_days": staleness_days,
                "anchor_stale": anchor_is_stale(staleness_days),
                "method": "ratio_carry",
            }),
        ));
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

/// Distinct valid employer IČOs from this run's persisted vacancy samples,
/// zero-padded to the canonical 8 digits (ARES's path format), in first-seen
/// order for deterministic capping.
fn distinct_icos<'a>(samples: impl Iterator<Item = &'a Value>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut icos = Vec::new();
    for v in samples {
        let Some(raw) = v.get("employerIco").and_then(Value::as_str) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() || raw.len() > 8 || !raw.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let ico = format!("{raw:0>8}");
        if seen.insert(ico.clone()) {
            icos.push(ico);
        }
    }
    icos
}

/// Non-empty trimmed string or number rendered as a string — ARES codes drift
/// between the two (e.g. `pravniForma: "121"` vs `kodKraje: 19`).
fn json_scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => {
            let s = s.trim();
            (!s.is_empty()).then(|| s.to_string())
        }
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// CZ-NACE activity codes from an ARES subject, defensively: an array of
/// strings/numbers, or of objects carrying the code under a known key.
/// Returns (codes capped at [`ARES_NACE_CAP`], total present).
fn ares_nace_codes(v: &Value) -> (Vec<String>, usize) {
    let Some(arr) = v.get("czNace").and_then(Value::as_array) else {
        return (Vec::new(), 0);
    };
    let codes: Vec<String> = arr
        .iter()
        .filter_map(|n| match n {
            Value::Object(_) => ["kodNace", "kod", "id", "value"]
                .iter()
                .find_map(|k| n.get(k).and_then(json_scalar_string)),
            scalar => json_scalar_string(scalar),
        })
        .take(ARES_NACE_CAP)
        .collect();
    (codes, arr.len())
}

/// Resumable state of the ARES enrichment phase (M23 checkpoint): the employer
/// records this job has already fetched, and how much of the per-run lookup
/// budget it has already spent. Lookups are the expensive, externally-paced part;
/// everything else in the phase is recomputed for free.
#[derive(Default)]
struct AresCheckpoint {
    /// IČO → normalized employer record, already fetched by a prior attempt.
    records: Vec<(String, Value)>,
    /// Lookups charged against `aresMaxLookups` so far (successes AND failures —
    /// a retry must not get a fresh budget by failing).
    looked_up: usize,
    /// Failures already counted, so the summary stays truthful across a resume.
    failed: usize,
}

impl AresCheckpoint {
    /// Advisory decode: ANY unexpected shape means "start this phase fresh",
    /// never an error. A stored snapshot from a different app version, a
    /// truncated write, or a future phase tag all fall back to `None`.
    fn from_value(v: &Value) -> Option<Self> {
        if v.get("phase").and_then(Value::as_str) != Some("ares") {
            return None;
        }
        let records: Vec<(String, Value)> = v
            .get("records")
            .and_then(Value::as_object)?
            .iter()
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        Some(Self {
            looked_up: v
                .get("looked_up")
                .and_then(Value::as_u64)
                .unwrap_or(records.len() as u64) as usize,
            failed: v.get("failed").and_then(Value::as_u64).unwrap_or(0) as usize,
            records,
        })
    }

    /// The snapshot handed to the checkpoint sink.
    fn to_value(records: &[(String, Value)], looked_up: usize, failed: usize) -> Value {
        json!({
            "phase": "ares",
            "records": records.iter().cloned().collect::<serde_json::Map<String, Value>>(),
            "looked_up": looked_up,
            "failed": failed,
        })
    }
}

/// One ARES lookup: fetch → parse → normalize. EVERY failure mode (transport,
/// non-2xx, non-JSON, no usable business name) warns and yields `None` —
/// enrichment is a side quest and must never fail the run.
async fn fetch_ares_employer(ctx: &AppContext, ico: &str) -> Option<Value> {
    let ares_url = format!("{ARES_URL}/{ico}");
    let resp = match ctx.engines.http.fetch(HttpRequest::get(&ares_url)).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("mpsv-vpm: ARES fetch failed for IČO {ico}: {e}");
            return None;
        }
    };
    if !resp.is_success() {
        tracing::warn!(
            "mpsv-vpm: ARES returned status {} for IČO {ico} — skipping",
            resp.status
        );
        return None;
    }
    let subject: Value = match serde_json::from_str(&resp.body) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("mpsv-vpm: ARES body for IČO {ico} was not JSON: {e}");
            return None;
        }
    };
    let rec = normalize_ares_employer(ico, &subject);
    if rec.is_none() {
        tracing::warn!("mpsv-vpm: ARES subject for IČO {ico} had no usable name");
    }
    rec
}

/// Compact normalized employer record from one ARES economic-subject response.
/// Inspects the payload defensively (the exact shape may drift); returns `None`
/// when there is no usable business name — nothing honest to persist.
fn normalize_ares_employer(ico: &str, v: &Value) -> Option<Value> {
    let name = v
        .get("obchodniJmeno")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let sidlo = v.get("sidlo");
    let (nace, nace_total) = ares_nace_codes(v);
    Some(json!({
        "ico": ico,
        "name": name,
        "legalForm": v.get("pravniForma").and_then(json_scalar_string),
        "founded": v.get("datumVzniku").and_then(Value::as_str),
        "krajId": sidlo.and_then(|s| s.get("kodKraje")).and_then(json_scalar_string),
        "krajName": sidlo.and_then(|s| s.get("nazevKraje")).and_then(Value::as_str),
        "nace": nace,
        "naceCount": nace_total,
    }))
}

// ── vacancy survival ledger ─────────────────────────────────────────────────
//
// Compact per-posting lifecycle state carried run-to-run as ONE JSON artifact.
// Rows serialize as tuples (arrays), not objects — at ~300k open postings the
// field names would dominate the file; tuple form keeps a full national ledger
// around ~25 MB open + a rolling closed window (bounded by `repostWindowDays`).
//
// METRIC HONESTY: a posting that stops appearing in the feed has CLOSED —
// filled, withdrawn by the employer, or expired; the feed cannot distinguish.
// Everything derived here is therefore time-to-CLOSE, never "time-to-fill".

/// One open posting: `(id, czIsco, kraj, salaryBand, ico, firstSeen, lastSeen,
/// seenCount)`; dates are `YYYY-MM-DD`.
type OpenTuple = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
    u32,
);

#[derive(Clone, Debug, PartialEq, serde::Serialize, Deserialize)]
#[serde(from = "OpenTuple", into = "OpenTuple")]
struct OpenEntry {
    id: String,
    czisco: String,
    kraj: Option<String>,
    band: Option<String>,
    ico: Option<String>,
    first_seen: String,
    last_seen: String,
    seen_count: u32,
}

impl From<OpenTuple> for OpenEntry {
    fn from((id, czisco, kraj, band, ico, first_seen, last_seen, seen_count): OpenTuple) -> Self {
        Self {
            id,
            czisco,
            kraj,
            band,
            ico,
            first_seen,
            last_seen,
            seen_count,
        }
    }
}
impl From<OpenEntry> for OpenTuple {
    fn from(e: OpenEntry) -> Self {
        (
            e.id,
            e.czisco,
            e.kraj,
            e.band,
            e.ico,
            e.first_seen,
            e.last_seen,
            e.seen_count,
        )
    }
}

/// One closed posting kept for the repost window: `(id, czIsco, kraj,
/// salaryBand, ico, closedAt, daysOpen, repostId)`. `days_open` = closedAt −
/// firstSeen (days-to-CLOSE). `repost_id` links the posting that reappeared
/// with the same (IČO, czIsco, kraj, band) within the window.
type ClosedTuple = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    i64,
    Option<String>,
);

#[derive(Clone, Debug, PartialEq, serde::Serialize, Deserialize)]
#[serde(from = "ClosedTuple", into = "ClosedTuple")]
struct ClosedEntry {
    id: String,
    czisco: String,
    kraj: Option<String>,
    band: Option<String>,
    ico: Option<String>,
    closed_at: String,
    days_open: i64,
    repost_id: Option<String>,
}

impl From<ClosedTuple> for ClosedEntry {
    fn from((id, czisco, kraj, band, ico, closed_at, days_open, repost_id): ClosedTuple) -> Self {
        Self {
            id,
            czisco,
            kraj,
            band,
            ico,
            closed_at,
            days_open,
            repost_id,
        }
    }
}
impl From<ClosedEntry> for ClosedTuple {
    fn from(e: ClosedEntry) -> Self {
        (
            e.id,
            e.czisco,
            e.kraj,
            e.band,
            e.ico,
            e.closed_at,
            e.days_open,
            e.repost_id,
        )
    }
}

/// The whole run-to-run ledger artifact: the run it describes, all open
/// postings, and the closures still inside the repost window.
#[derive(Default, serde::Serialize, Deserialize)]
struct Ledger {
    run_date: String,
    #[serde(default)]
    open: Vec<OpenEntry>,
    #[serde(default)]
    closed: Vec<ClosedEntry>,
}

/// Today's view of one posting — everything the diff and repost matcher need.
struct TodayPosting {
    czisco: String,
    kraj: Option<String>,
    band: Option<String>,
    ico: Option<String>,
}

struct LedgerDiff {
    ledger: Ledger,
    new_now: usize,
    ongoing: usize,
    closed_now: usize,
    reposts_now: usize,
    /// True when nothing was closed because the run gap was outside tolerance
    /// (or the prior run date didn't parse — never mass-close on bad data).
    carried: bool,
    gap_days: i64,
}

/// Salary band for repost matching: [`SALARY_BAND_CZK`]-wide buckets of the
/// monthly midpoint, labeled by their lower bound ("40k" = 40 000–44 999 CZK).
fn salary_band(salary: Option<f64>) -> Option<String> {
    salary.map(|s| {
        let lo = (s / SALARY_BAND_CZK).floor() as i64 * (SALARY_BAND_CZK as i64 / 1_000);
        format!("{lo}k")
    })
}

/// Diffs the prior ledger against today's feed view. Pure — all persistence
/// happens in the caller.
///
/// * First run (`prior` None): everything is new; nothing can close.
/// * Gap tolerance: if `today − prior.run_date` is outside `1..=max_gap_days`
///   (missed runs, same-day re-run, or an unparseable prior date), NOTHING is
///   closed — prior entries absent today are carried forward unchanged, since
///   their absence spans an unobserved window.
/// * Normal day: absent → closed (`days_open` = today − first_seen), present →
///   `last_seen`/`seen_count` advance, unknown → new (first_seen = today).
/// * Reposts: a new posting whose (IČO, czIsco, kraj, band) matches a
///   still-unmatched closure inside `repost_window_days` links to it 1:1
///   (newest closure first); postings without an IČO never match.
fn diff_ledger(
    prior: Option<Ledger>,
    today_map: &HashMap<String, TodayPosting>,
    today: NaiveDate,
    max_gap_days: i64,
    repost_window_days: i64,
) -> LedgerDiff {
    let today_s = today.to_string();
    let (prior_open, prior_closed, gap_days) = match prior {
        Some(l) => {
            let gap = NaiveDate::parse_from_str(&l.run_date, "%Y-%m-%d")
                .map(|d| (today - d).num_days())
                .unwrap_or(i64::MAX);
            (l.open, l.closed, gap)
        }
        None => (Vec::new(), Vec::new(), 0),
    };
    let carried = !prior_open.is_empty() && !(1..=max_gap_days).contains(&gap_days);

    // Closures still inside the repost window survive; older ones age out.
    let window_start = today - Duration::days(repost_window_days);
    let mut closed: Vec<ClosedEntry> = prior_closed
        .into_iter()
        .filter(|c| {
            NaiveDate::parse_from_str(&c.closed_at, "%Y-%m-%d").is_ok_and(|d| d >= window_start)
        })
        .collect();

    let mut known: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut open: Vec<OpenEntry> = Vec::with_capacity(today_map.len());
    let (mut ongoing, mut closed_now) = (0usize, 0usize);
    for mut e in prior_open {
        known.insert(e.id.clone());
        if today_map.contains_key(&e.id) {
            e.last_seen = today_s.clone();
            e.seen_count += 1;
            ongoing += 1;
            open.push(e);
        } else if carried {
            open.push(e); // absence unobservable across the gap — keep it open
        } else {
            let days_open = NaiveDate::parse_from_str(&e.first_seen, "%Y-%m-%d")
                .map(|f| (today - f).num_days().max(1))
                .unwrap_or(1);
            closed.push(ClosedEntry {
                id: e.id,
                czisco: e.czisco,
                kraj: e.kraj,
                band: e.band,
                ico: e.ico,
                closed_at: today_s.clone(),
                days_open,
                repost_id: None,
            });
            closed_now += 1;
        }
    }

    // Unmatched closures indexed by the repost key; last index = newest closure.
    let mut repost_index: HashMap<(String, String, String, String), Vec<usize>> = HashMap::new();
    for (i, c) in closed.iter().enumerate() {
        if c.repost_id.is_none() {
            if let Some(ico) = &c.ico {
                repost_index
                    .entry((
                        ico.clone(),
                        c.czisco.clone(),
                        c.kraj.clone().unwrap_or_default(),
                        c.band.clone().unwrap_or_default(),
                    ))
                    .or_default()
                    .push(i);
            }
        }
    }
    let mut new_ids: Vec<&String> = today_map.keys().filter(|id| !known.contains(*id)).collect();
    new_ids.sort(); // deterministic 1:1 matching
    let (mut new_now, mut reposts_now) = (0usize, 0usize);
    for id in new_ids {
        let t = &today_map[id];
        if let Some(ico) = &t.ico {
            let key = (
                ico.clone(),
                t.czisco.clone(),
                t.kraj.clone().unwrap_or_default(),
                t.band.clone().unwrap_or_default(),
            );
            if let Some(slot) = repost_index.get_mut(&key).and_then(Vec::pop) {
                closed[slot].repost_id = Some(id.clone());
                reposts_now += 1;
            }
        }
        open.push(OpenEntry {
            id: id.clone(),
            czisco: t.czisco.clone(),
            kraj: t.kraj.clone(),
            band: t.band.clone(),
            ico: t.ico.clone(),
            first_seen: today_s.clone(),
            last_seen: today_s.clone(),
            seen_count: 1,
        });
        new_now += 1;
    }

    LedgerDiff {
        ledger: Ledger {
            run_date: today_s,
            open,
            closed,
        },
        new_now,
        ongoing,
        closed_now,
        reposts_now,
        carried,
        gap_days,
    }
}

/// Aggregates the rolling closed window into `cz-labour/vacancy_lifecycle`
/// rows: per (CZ-ISCO unit group × kraj, plus a kraj `ALL` roll-up), the
/// days-to-CLOSE distribution (median/p75, nearest-rank), repost share, and
/// churn (window closures vs currently-live postings in the cell). Cells below
/// `min_count` closures are suppressed (the same privacy/statistical floor as
/// the salary aggregates). Keys `{unitGroup}|{krajId}`, sorted for
/// deterministic upserts.
///
/// The `metric` field says `time_to_close` on every record on purpose:
/// disappearance conflates filled / withdrawn / expired, so this must never be
/// presented as time-to-fill.
fn aggregate_lifecycle(
    closed: &[ClosedEntry],
    live_counts: &HashMap<(String, String), usize>,
    min_count: usize,
    window_days: i64,
) -> Vec<(String, Value)> {
    #[derive(Default)]
    struct LifeCell {
        days: Vec<i64>,
        reposts: usize,
    }
    let mut cells: HashMap<(String, String), LifeCell> = HashMap::new();
    let mut add = |ug: String, kraj: String, c: &ClosedEntry| {
        let cell = cells.entry((ug, kraj)).or_default();
        cell.days.push(c.days_open);
        cell.reposts += c.repost_id.is_some() as usize;
    };
    for c in closed {
        let ug = unit_group(&c.czisco);
        if let Some(k) = &c.kraj {
            add(ug.clone(), k.clone(), c);
        }
        add(ug, "ALL".to_string(), c);
    }
    let mut items: Vec<(String, Value)> = Vec::new();
    for ((ug, kraj), mut cell) in cells {
        if cell.days.len() < min_count.max(1) {
            continue;
        }
        cell.days.sort_unstable();
        let pct = |p: f64| -> i64 {
            let idx = (((cell.days.len() - 1) as f64) * p).round() as usize;
            cell.days[idx.min(cell.days.len() - 1)]
        };
        let n = cell.days.len();
        let live = live_counts.get(&(ug.clone(), kraj.clone())).copied();
        items.push((
            format!("{ug}|{kraj}"),
            json!({
                "czIscoGroup": ug,
                "krajId": kraj,
                // Time from first observation to disappearance from the feed.
                // NOT time-to-fill: closure = filled OR withdrawn OR expired.
                "metric": "time_to_close",
                "windowDays": window_days,
                "closedCount": n,
                "medianDaysToClose": pct(0.5),
                "p75DaysToClose": pct(0.75),
                "repostCount": cell.reposts,
                "repostSharePct": (cell.reposts as f64 / n as f64 * 100.0 * 10.0).round() / 10.0,
                "liveCount": live,
                "churnPct": live.filter(|&l| l > 0).map(|l| {
                    (n as f64 / l as f64 * 100.0 * 10.0).round() / 10.0
                }),
            }),
        ));
    }
    items.sort_by(|a, b| a.0.cmp(&b.0));
    items
}

// ── typed subset of the feed (unknown fields are ignored, bounding memory) ──

#[derive(Deserialize)]
struct Feed {
    /// `Option`, deliberately NOT `#[serde(default)]`: absence has to survive
    /// deserialization for [`feed_postings`] to tell drift from an empty feed.
    polozky: Option<Vec<Posting>>,
}

#[derive(Deserialize)]
struct Posting {
    #[serde(default)]
    portalId: Option<i64>,
    #[serde(default)]
    datumVlozeni: Option<String>,
    #[serde(default)]
    datumZmeny: Option<String>,
    #[serde(default)]
    mesicniMzdaOd: Option<f64>,
    #[serde(default)]
    mesicniMzdaDo: Option<f64>,
    #[serde(default)]
    statniSpravaSamosprava: Option<bool>,
    #[serde(default)]
    souhlasAgenturyAgentura: Option<bool>,
    #[serde(default)]
    souhlasAgenturyUzivatel: Option<bool>,
    #[serde(default)]
    urlAdresa: Option<String>,
    #[serde(default)]
    pozadovanaProfese: Option<LangText>,
    #[serde(default)]
    minPozadovaneVzdelani: Option<IdRef>,
    #[serde(default)]
    profeseCzIsco: Option<IdRef>,
    #[serde(default)]
    zamestnavatel: Option<Zamestnavatel>,
    #[serde(default)]
    mistoVykonuPrace: Option<Misto>,
    #[serde(default)]
    pozadovanaDovednost: Option<Vec<IdRef>>,
}

#[derive(Deserialize)]
struct LangText {
    #[serde(default)]
    cs: Option<String>,
}

#[derive(Deserialize)]
struct IdRef {
    #[serde(default)]
    id: Option<String>,
}

#[derive(Deserialize)]
struct Zamestnavatel {
    #[serde(default)]
    ico: Option<String>,
    #[serde(default)]
    nazev: Option<String>,
}

#[derive(Deserialize)]
struct Misto {
    #[serde(default)]
    pracoviste: Option<Vec<Pracoviste>>,
}

#[derive(Deserialize)]
struct Pracoviste {
    #[serde(default)]
    adresa: Option<Adresa>,
}

#[derive(Deserialize)]
struct Adresa {
    #[serde(default)]
    kraj: Option<IdRef>,
}

/// Parse the `YYYY-MM-DD` prefix of an MPSV RFC3339 datetime into a date.
fn parse_day(s: &Option<String>) -> Option<NaiveDate> {
    s.as_deref()
        .and_then(|d| d.get(0..10))
        .and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
}

impl Posting {
    fn posted_date(&self) -> Option<NaiveDate> {
        parse_day(&self.datumVlozeni)
    }
    fn changed_date(&self) -> Option<NaiveDate> {
        parse_day(&self.datumZmeny)
    }

    fn czisco(&self) -> Option<String> {
        self.profeseCzIsco
            .as_ref()
            .and_then(|r| r.id.clone())
            .filter(|s| !s.is_empty())
    }

    /// public (state/self-gov) → agency (temp-work agency) → private, in order.
    fn org_type(&self) -> String {
        if self.statniSpravaSamosprava == Some(true) {
            return "public".to_string();
        }
        if self.souhlasAgenturyAgentura == Some(true) || self.souhlasAgenturyUzivatel == Some(true)
        {
            return "agency".to_string();
        }
        "private".to_string()
    }

    fn kraj(&self) -> Option<String> {
        self.mistoVykonuPrace
            .as_ref()?
            .pracoviste
            .as_ref()?
            .iter()
            .find_map(|pr| {
                pr.adresa
                    .as_ref()
                    .and_then(|a| a.kraj.as_ref())
                    .and_then(|k| k.id.clone())
            })
            .filter(|s| !s.is_empty())
    }

    /// A single representative CZK monthly figure: midpoint of the band when both
    /// ends are given, else whichever end is present; `None` if the value isn't a
    /// sane monthly salary.
    ///
    /// The presence of `mesicniMzda*` ("monthly wage") within the monthly band IS
    /// the monthly signal — the API exposes no hourly wage fields, and `typMzdy.id`
    /// is a codebook URI (`"TypMzdy/N"`, like `CzIsco/93291`), not a substring-
    /// matchable label, so the old `id.contains("mesic")` gate matched nothing and
    /// silently discarded every salary in the distribution.
    fn monthly_salary_point(&self) -> Option<f64> {
        let point = match (self.mesicniMzdaOd, self.mesicniMzdaDo) {
            (Some(a), Some(b)) if a > 0.0 && b > 0.0 => (a + b) / 2.0,
            (Some(a), _) if a > 0.0 => a,
            (_, Some(b)) if b > 0.0 => b,
            _ => return None,
        };
        (SALARY_MIN..=SALARY_MAX).contains(&point).then_some(point)
    }

    fn as_sample(
        &self,
        czisco: &str,
        org: &str,
        kraj: Option<&str>,
        salary: Option<f64>,
    ) -> Option<Sample> {
        let title = self
            .pozadovanaProfese
            .as_ref()
            .and_then(|t| t.cs.clone())
            .filter(|s| !s.is_empty())?;
        let skills: Vec<String> = self
            .pozadovanaDovednost
            .as_ref()
            .map(|v| v.iter().filter_map(|r| r.id.clone()).collect())
            .unwrap_or_default();
        let employer = self.zamestnavatel.as_ref().and_then(|z| z.nazev.clone());
        // IČO → the join key for the ARES enrichment into the `employers` dataset.
        let employer_ico = self.zamestnavatel.as_ref().and_then(|z| z.ico.clone());
        let education = self
            .minPozadovaneVzdelani
            .as_ref()
            .and_then(|e| e.id.clone());
        // richer postings (salary + skills + a descriptive title) make better refs
        let richness = (salary.is_some() as u32) * 2
            + ((!skills.is_empty()) as u32)
            + (title.len().min(60) as u32 / 20);
        let posted = self.posted_date().map(|d| d.to_string());
        let value = json!({
            "portalId": self.portalId,
            "title": title,
            "czIsco": czisco,
            "orgType": org,
            "krajId": kraj,
            "salaryMin": self.mesicniMzdaOd,
            "salaryMax": self.mesicniMzdaDo,
            "salaryPoint": salary,
            "employer": employer,
            "employerIco": employer_ico,
            "education": education,
            "skills": skills,
            "postedAt": posted,
            "url": self.urlAdresa,
        });
        Some(Sample {
            richness,
            posted,
            value,
        })
    }
}

/// Accumulator for one (occupation × kraj × orgType) cell.
#[derive(Default)]
struct Cell {
    count: usize,
    salaries: Vec<f64>,
}

impl Cell {
    fn add(&mut self, salary: Option<f64>) {
        self.count += 1;
        if let Some(s) = salary {
            self.salaries.push(s);
        }
    }

    /// Sorted salaries + a percentile accessor (nearest-rank).
    fn stats(&self) -> (Vec<f64>, impl Fn(f64) -> Option<i64> + '_) {
        let mut s = self.salaries.clone();
        s.sort_by(f64::total_cmp);
        let s2 = s.clone();
        let pct = move |p: f64| -> Option<i64> {
            if s2.is_empty() {
                return None;
            }
            let idx = (((s2.len() - 1) as f64) * p).round() as usize;
            Some(s2[idx.min(s2.len() - 1)].round() as i64)
        };
        (s, pct)
    }

    fn to_value(&self, czisco: &str, kraj: &str, org: &str) -> Value {
        let (s, pct) = self.stats();
        json!({
            "czIsco": czisco,
            "krajId": kraj,
            "orgType": org,
            "count": self.count,
            "salaryCount": s.len(),
            "salaryMin": s.first().map(|v| v.round() as i64),
            "salaryP25": pct(0.25),
            "salaryMedian": pct(0.5),
            "salaryP75": pct(0.75),
            "salaryMax": s.last().map(|v| v.round() as i64),
        })
    }

    /// Nearest-rank median of this cell's salaries (`None` when no posting in the
    /// cell reported a salary).
    fn median(&self) -> Option<i64> {
        let (_, pct) = self.stats();
        pct(0.5)
    }

    /// A skill-demand row: how many postings in `unit_group` demand `skill_id`,
    /// its share of the group's postings, and the salary distribution for those
    /// postings.
    fn to_skill_value(&self, unit_group: &str, skill_id: &str, group_total: usize) -> Value {
        let (s, pct) = self.stats();
        json!({
            "unitGroup": unit_group,
            "skillId": skill_id,
            "count": self.count,
            "groupPostings": group_total,
            "sharePct": (group_total > 0)
                .then(|| (self.count as f64 / group_total as f64 * 100.0).round()),
            "salaryCount": s.len(),
            "salaryMedian": pct(0.5),
            "salaryP25": pct(0.25),
            "salaryP75": pct(0.75),
        })
    }

    /// An education-level row: the salary distribution for postings in
    /// `unit_group` that require `education_id`, plus the group's overall median
    /// so the premium is an honest median-vs-median read (never a fabricated
    /// delta). `premiumVsGroup` is emitted only when BOTH medians exist.
    fn to_education_value(
        &self,
        unit_group: &str,
        education_id: &str,
        group_median: Option<i64>,
    ) -> Value {
        let (s, pct) = self.stats();
        let median = pct(0.5);
        let premium = match (median, group_median) {
            (Some(m), Some(g)) => Some(m - g),
            _ => None,
        };
        json!({
            "unitGroup": unit_group,
            "educationId": education_id,
            "count": self.count,
            "salaryCount": s.len(),
            "salaryMedian": median,
            "groupMedian": group_median,
            "premiumVsGroup": premium,
        })
    }

    fn to_region_value(&self, kraj: &str, org: &str) -> Value {
        let (s, pct) = self.stats();
        json!({
            "krajId": kraj,
            "orgType": org,
            "count": self.count,
            "salaryCount": s.len(),
            "salaryMin": s.first().map(|v| v.round() as i64),
            "salaryP25": pct(0.25),
            "salaryMedian": pct(0.5),
            "salaryP75": pct(0.75),
            "salaryMax": s.last().map(|v| v.round() as i64),
        })
    }
}

struct Sample {
    richness: u32,
    /// `YYYY-MM-DD` posting date (for recency-preferring sample selection).
    posted: Option<String>,
    value: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};

    /// The manifest must describe the params the code actually reads. The server
    /// registry test validates examples against the schema; this one catches the
    /// drift that a validator cannot see — an example or a scheduled default
    /// naming a key the schema never declares.
    #[test]
    fn manifest_examples_and_defaults_only_use_declared_params() {
        let m = MpsvVpm.manifest();
        let schema = m.params_schema.expect("schema declared");
        let props = schema["properties"].as_object().expect("properties");
        for key in [
            "url",
            "maxRecords",
            "minCount",
            "aresMaxLookups",
            "nowcastWindow",
        ] {
            assert!(props.contains_key(key), "schema must declare '{key}'");
        }
        let declared = |params: &Value, what: &str| {
            for k in params.as_object().expect("object params").keys() {
                assert!(props.contains_key(k), "{what} uses undeclared param '{k}'");
            }
        };
        declared(&MpsvVpm.default_params(), "default_params");
        assert!(!m.examples.is_empty(), "agents need at least one example");
        for ex in &m.examples {
            declared(&ex.params, ex.description);
        }
    }

    #[test]
    fn ares_checkpoint_round_trips_records_and_budget() {
        let records = vec![
            (
                "00000001".to_string(),
                json!({ "ico": "00000001", "name": "A" }),
            ),
            (
                "00000002".to_string(),
                json!({ "ico": "00000002", "name": "B" }),
            ),
        ];
        let snapshot = AresCheckpoint::to_value(&records, 5, 3);
        let restored = AresCheckpoint::from_value(&snapshot).expect("decodes");
        let mut got = restored.records.clone();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got, records);
        // Failures count against the budget too — a retry must not win a fresh
        // lookup allowance by having failed.
        assert_eq!((restored.looked_up, restored.failed), (5, 3));
    }

    #[test]
    fn ares_checkpoint_treats_any_foreign_or_broken_shape_as_start_fresh() {
        for bad in [
            json!({}),
            json!({ "phase": "crawl", "records": {} }),
            json!({ "phase": "ares" }),                // no records object
            json!({ "phase": "ares", "records": [] }), // wrong records type
            json!("nonsense"),
        ] {
            assert!(
                AresCheckpoint::from_value(&bad).is_none(),
                "must start fresh on {bad}"
            );
        }
        // A snapshot from an older writer without the counters still resumes:
        // the records themselves are the lookups already paid for.
        let partial = json!({ "phase": "ares", "records": { "00000001": { "name": "A" } } });
        let c = AresCheckpoint::from_value(&partial).expect("decodes");
        assert_eq!((c.records.len(), c.looked_up, c.failed), (1, 1, 0));
    }

    fn cell(salaries: &[f64]) -> Cell {
        let mut c = Cell::default();
        for &s in salaries {
            c.add(Some(s));
        }
        c
    }

    #[test]
    fn skill_value_reports_count_share_and_salary() {
        // 3 postings demand this skill out of a 12-posting group → 25% share.
        let c = cell(&[40_000.0, 50_000.0, 60_000.0]);
        let v = c.to_skill_value("2512", "Dovednost/rust", 12);
        assert_eq!(v["unitGroup"], "2512");
        assert_eq!(v["skillId"], "Dovednost/rust");
        assert_eq!(v["count"], 3);
        assert_eq!(v["groupPostings"], 12);
        assert_eq!(v["sharePct"], 25.0);
        assert_eq!(v["salaryMedian"], 50_000);
    }

    #[test]
    fn education_value_premium_is_median_vs_median_never_fabricated() {
        // A degree-required cell median 60k against the group median 45k → +15k.
        let c = cell(&[55_000.0, 60_000.0, 65_000.0]);
        let v = c.to_education_value("2512", "Vzdelani/vs", Some(45_000));
        assert_eq!(v["salaryMedian"], 60_000);
        assert_eq!(v["groupMedian"], 45_000);
        assert_eq!(v["premiumVsGroup"], 15_000);
        // No group median → no fabricated premium.
        let v2 = c.to_education_value("2512", "Vzdelani/vs", None);
        assert!(v2["premiumVsGroup"].is_null());
    }

    fn posted_map(entries: Vec<((&str, &str), Cell)>) -> HashMap<(String, String), Cell> {
        entries
            .into_iter()
            .map(|((g, s), c)| ((g.to_string(), s.to_string()), c))
            .collect()
    }

    #[test]
    fn monthly_salary_extracted_without_relying_on_type_code() {
        // Regression: the salary distribution was silently emptied because the old
        // `is_monthly()` gate string-matched "mesic" against the codebook-URI
        // `typMzdy.id` ("TypMzdy/1"), which never contains it. The presence of the
        // monthly-wage fields within the sane band is the signal.
        let p: Posting = serde_json::from_value(json!({
            "mesicniMzdaOd": 40000.0,
            "mesicniMzdaDo": 60000.0,
            "typMzdy": { "id": "TypMzdy/1" }
        }))
        .unwrap();
        assert_eq!(p.monthly_salary_point(), Some(50_000.0));

        // Sub-band (hourly-looking) and absent values yield None, never fabricated.
        let hourly: Posting = serde_json::from_value(json!({ "mesicniMzdaOd": 150.0 })).unwrap();
        assert_eq!(hourly.monthly_salary_point(), None);
        let empty: Posting = serde_json::from_value(json!({})).unwrap();
        assert_eq!(empty.monthly_salary_point(), None);
    }

    // ── feed-drift honesty ──────────────────────────────────────────────────

    /// The anti-pattern: `#[serde(default)]` on `polozky` made a renamed key,
    /// a re-wrapped envelope and an error document indistinguishable from an
    /// empty feed — all four aggregated to nothing and reported success.
    #[test]
    fn missing_polozky_is_drift_not_an_empty_feed() {
        let drift = |body: Value| {
            let feed: Feed = serde_json::from_value(body).expect("parses as Feed");
            feed_postings(feed)
        };
        // Renamed key.
        assert!(drift(json!({ "items": [] })).is_err());
        // Re-wrapped envelope.
        assert!(drift(json!({ "data": { "polozky": [] } })).is_err());
        // An error document that happens to be valid JSON.
        assert!(drift(json!({ "error": "service unavailable" })).is_err());
        // The honest empty feed is NOT drift — the size floor judges it instead.
        assert_eq!(drift(json!({ "polozky": [] })).expect("ok").len(), 0);
        // And a real feed passes through untouched.
        let ok = drift(json!({ "polozky": [{ "mesicniMzdaOd": 40000.0 }] })).expect("ok");
        assert_eq!(ok.len(), 1);
    }

    #[test]
    fn size_floor_judges_the_national_feed_only_never_a_mirror_or_a_capped_run() {
        // The collapse the floor exists for: default feed, full width.
        assert!(implausibly_small_feed(0, true, 0));
        assert!(implausibly_small_feed(MIN_PLAUSIBLE_POSTINGS - 1, true, 0));
        assert!(!implausibly_small_feed(MIN_PLAUSIBLE_POSTINGS, true, 0));
        assert!(!implausibly_small_feed(300_000, true, 0));
        // A `url` override is a deliberately trimmed mirror — the manifest's own
        // smoke example. Refusing it would break the runs the floor exists to allow.
        assert!(!implausibly_small_feed(20, false, 0));
        // `maxRecords` truncates on purpose.
        assert!(!implausibly_small_feed(20, true, 20_000));
    }

    /// Bughunt 2026-07-14 #2: the region roll-up sat behind the CZ-ISCO early
    /// `continue`, so `region_agg` — "the true regional salary distribution" —
    /// silently omitted every posting the feed leaves unclassified.
    #[test]
    fn region_rollup_keys_need_no_occupation_code_and_cover_kraj_and_national() {
        let keys = region_rollup_keys(Some("Kraj/108"), "private");
        assert_eq!(
            keys,
            vec![
                ("Kraj/108".to_string(), "private".to_string()),
                ("Kraj/108".to_string(), "all".to_string()),
                ("ALL".to_string(), "private".to_string()),
                ("ALL".to_string(), "all".to_string()),
            ]
        );
        // A posting with no kraj still counts nationally — it just has no region.
        assert_eq!(
            region_rollup_keys(None, "public"),
            vec![
                ("ALL".to_string(), "public".to_string()),
                ("ALL".to_string(), "all".to_string()),
            ]
        );
    }

    // ── run() end-to-end over a stubbed HTTP engine ─────────────────────────

    /// One scripted HTTP response for every request. `aresMaxLookups: 0` in the
    /// test params keeps the ARES leg out, so the feed fetch is the only call.
    struct StubHttp {
        body: String,
    }

    #[async_trait]
    impl pumper_core::HttpClient for StubHttp {
        async fn fetch(&self, _: HttpRequest) -> Result<pumper_core::HttpResponse> {
            Ok(pumper_core::HttpResponse {
                status: 200,
                headers: Default::default(),
                body: self.body.clone(),
                final_url: FULL_URL.to_string(),
                cache_hit: false,
            })
        }
    }

    fn vpm_ctx(store: &TempStore, body: String, params: Value) -> AppContext {
        let http = std::sync::Arc::new(StubHttp { body });
        TestContext::new(&store.storage, "mpsv-vpm")
            .params(params)
            .engines(engines_with(
                http,
                std::sync::Arc::new(Dead),
                std::sync::Arc::new(Dead),
            ))
            .build()
    }

    /// One posting, with an optional occupation code — the fixture the region
    /// bias turns on.
    fn posting(id: i64, czisco: Option<&str>, kraj: &str, salary: f64) -> Value {
        let mut p = json!({
            "portalId": id,
            "datumVlozeni": "2026-08-01T00:00:00Z",
            "datumZmeny": "2026-08-10T00:00:00Z",
            "mesicniMzdaOd": salary,
            "mesicniMzdaDo": salary,
            "pozadovanaProfese": { "cs": "Pracovník" },
            "zamestnavatel": { "ico": "27074358", "nazev": "Alza.cz a.s." },
            "mistoVykonuPrace": { "pracoviste": [{ "adresa": { "kraj": { "id": kraj } } }] },
        });
        if let Some(cz) = czisco {
            p["profeseCzIsco"] = json!({ "id": cz });
        }
        p
    }

    /// The bughunt bug, proven at run level: with three unclassified postings in
    /// the same kraj as three classified ones, `region_agg` must count SIX.
    #[tokio::test]
    async fn run_region_agg_counts_unclassified_postings_not_only_czisco_ones() {
        let store = TempStore::new("mpsv-vpm-region").await;
        let mut rows: Vec<Value> = (0..3)
            .map(|i| posting(i, Some("CzIsco/52231"), "Kraj/108", 40_000.0))
            .collect();
        rows.extend((10..13).map(|i| posting(i, None, "Kraj/108", 60_000.0)));
        let body = json!({ "polozky": rows }).to_string();
        // `url` override keeps the national size floor out of the way.
        let params = json!({
            "url": "https://example.test/mirror.json",
            "minCount": 1,
            "aresMaxLookups": 0,
        });
        let out = MpsvVpm
            .run(vpm_ctx(&store, body, params))
            .await
            .expect("run");
        assert_eq!(out["feedRecords"], 6);
        let regions = store
            .datasets()
            .list("mpsv-vpm", "region_agg", 100)
            .await
            .expect("read back");
        let kraj_all = regions
            .iter()
            .find(|r| r.key == "Kraj/108|all")
            .expect("the kraj's pooled cell");
        assert_eq!(
            kraj_all.data["count"], 6,
            "unclassified postings belong in the REGIONAL distribution — they \
             only lack an occupation, not a region"
        );
        // Their salaries too: median of [40k,40k,40k,60k,60k,60k] is 40k at
        // nearest rank, and the max proves the 60k rows are in the pool.
        assert_eq!(kraj_all.data["salaryCount"], 6);
        assert_eq!(kraj_all.data["salaryMax"], 60_000);
        // The occupation-keyed table is unchanged: only the 3 classified rows.
        let cells = store
            .datasets()
            .list("mpsv-vpm", "role_region_agg", 100)
            .await
            .expect("read back");
        assert_eq!(
            cells
                .iter()
                .find(|r| r.key == "CzIsco/52231|Kraj/108|private")
                .expect("occupation cell")
                .data["count"],
            3
        );
    }

    #[tokio::test]
    async fn run_fails_on_drift_instead_of_reporting_a_clean_zero_record_success() {
        let store = TempStore::new("mpsv-vpm-drift").await;
        let body = json!({ "polozkyVolnychMist": [] }).to_string();
        let err = MpsvVpm
            .run(vpm_ctx(
                &store,
                body,
                json!({ "url": "https://example.test/mirror.json" }),
            ))
            .await
            .expect_err("drift must fail the run");
        assert!(err.to_string().contains("source contract drift"), "{err}");
        assert!(store
            .datasets()
            .list("mpsv-vpm", "region_agg", 10)
            .await
            .expect("read back")
            .is_empty());
    }

    /// The near-empty national feed: the key is present, so the drift check
    /// passes and only the floor stands between a collapsed download and every
    /// national cell being recomputed from nothing.
    #[tokio::test]
    async fn run_refuses_a_collapsed_national_feed_before_touching_any_aggregate() {
        let store = TempStore::new("mpsv-vpm-floor").await;
        let rows: Vec<Value> = (0..5)
            .map(|i| posting(i, Some("CzIsco/52231"), "Kraj/108", 40_000.0))
            .collect();
        let body = json!({ "polozky": rows }).to_string();
        // No `url` param → the DEFAULT national feed, at full width.
        let err = MpsvVpm
            .run(vpm_ctx(&store, body, json!({ "aresMaxLookups": 0 })))
            .await
            .expect_err("a collapsed national feed must fail the run");
        let msg = err.to_string();
        assert!(msg.contains("collapsed feed"), "{msg}");
        assert!(store
            .datasets()
            .list("mpsv-vpm", "region_agg", 10)
            .await
            .expect("read back")
            .is_empty());
    }

    #[test]
    fn sphere_mapping_public_vs_rest() {
        assert_eq!(sphere_for_org("public"), "PLATOVA");
        assert_eq!(sphere_for_org("private"), "MZDOVA");
        assert_eq!(sphere_for_org("agency"), "MZDOVA");
    }

    #[test]
    fn wage_num_accepts_number_and_czech_string_forms() {
        assert_eq!(wage_num(&json!({ "m": 111959.0 }), "m"), Some(111959.0));
        assert_eq!(wage_num(&json!({ "m": "111959" }), "m"), Some(111959.0));
        assert_eq!(wage_num(&json!({ "m": "111 959" }), "m"), Some(111959.0)); // space thousands
        assert_eq!(wage_num(&json!({ "m": "40000,50" }), "m"), Some(40000.5)); // Czech decimal comma
        assert_eq!(wage_num(&json!({ "m": "n/a" }), "m"), None);
        assert_eq!(wage_num(&json!({}), "m"), None);
    }

    #[test]
    fn official_index_reads_string_encoded_stats() {
        // Regression: as_f64-only dropped rows whose stats arrived as strings.
        let rows = [
            json!({"czIsco": "CzIsco/1120", "sfera": "MZDOVA", "medianMzda": "111959", "mzdaPrumer": "190185"}),
        ];
        let idx = official_wage_index(rows.iter());
        assert_eq!(idx.len(), 1);
        let (median, mean) = idx[&("1120".to_string(), "MZDOVA".to_string())];
        assert_eq!(median, 111959.0);
        assert_eq!(mean, Some(190185.0));
    }

    #[test]
    fn official_index_keys_by_unit_group_and_drops_medianless_rows() {
        let rows = [
            json!({"czIsco": "CzIsco/1120", "sfera": "MZDOVA", "medianMzda": 111959.0, "mzdaPrumer": 190185.0}),
            json!({"czIsco": "CzIsco/2433", "sfera": "PLATOVA"}), // no median → dropped
            json!({"sfera": "MZDOVA", "medianMzda": 40000.0}),    // no code → dropped
            json!({"czIsco": "CzIsco/5223", "sfera": "MZDOVA", "medianMzda": 0.0}), // zero → dropped
        ];
        let idx = official_wage_index(rows.iter());
        assert_eq!(idx.len(), 1);
        let (median, mean) = idx[&("1120".to_string(), "MZDOVA".to_string())];
        assert_eq!(median, 111959.0);
        assert_eq!(mean, Some(190185.0));
    }

    #[test]
    fn gap_joins_at_unit_group_and_computes_abs_and_pct() {
        // posted median of [40k, 50k, 60k] = 50k vs official 40k → +10k = +25%
        let posted = posted_map(vec![(
            ("5223", "MZDOVA"),
            cell(&[40_000.0, 50_000.0, 60_000.0]),
        )]);
        let mut official = HashMap::new();
        official.insert(
            ("5223".to_string(), "MZDOVA".to_string()),
            (40_000.0, Some(44_000.0)),
        );
        let items = compute_salary_gaps(&posted, &official, 3);
        assert_eq!(items.len(), 1);
        let (key, v) = &items[0];
        assert_eq!(key, "5223|MZDOVA");
        assert_eq!(v["postedMedian"], 50_000);
        assert_eq!(v["officialMedian"], 40_000);
        assert_eq!(v["gapAbs"], 10_000);
        assert_eq!(v["gapPct"], 25.0);
        assert_eq!(v["gapVsMeanAbs"], 6_000);
        // 6000/44000 = 13.636…% → 13.6 at one decimal
        assert_eq!(v["gapVsMeanPct"], 13.6);
        assert_eq!(v["postedSalaryCount"], 3);
    }

    #[test]
    fn gap_skips_unmatched_and_thin_cells_never_fabricates() {
        let posted = posted_map(vec![
            // no official row for this (group, sphere) → skipped
            (("9999", "MZDOVA"), cell(&[30_000.0, 32_000.0, 34_000.0])),
            // sphere mismatch: official only has PLATOVA → skipped
            (("2433", "MZDOVA"), cell(&[50_000.0, 52_000.0, 54_000.0])),
            // matched but only 2 salary points < min 3 → skipped
            (("5223", "MZDOVA"), cell(&[40_000.0, 42_000.0])),
        ]);
        let mut official = HashMap::new();
        official.insert(
            ("2433".to_string(), "PLATOVA".to_string()),
            (45_000.0, None),
        );
        official.insert(("5223".to_string(), "MZDOVA".to_string()), (40_000.0, None));
        assert!(compute_salary_gaps(&posted, &official, 3).is_empty());
    }

    #[test]
    fn gap_handles_negative_gap_and_missing_official_mean() {
        let posted = posted_map(vec![(
            ("5223", "PLATOVA"),
            cell(&[30_000.0, 30_000.0, 30_000.0]),
        )]);
        let mut official = HashMap::new();
        official.insert(
            ("5223".to_string(), "PLATOVA".to_string()),
            (40_000.0, None),
        );
        let items = compute_salary_gaps(&posted, &official, 1);
        assert_eq!(items.len(), 1);
        let v = &items[0].1;
        assert_eq!(v["gapAbs"], -10_000);
        assert_eq!(v["gapPct"], -25.0);
        assert_eq!(v["officialMean"], Value::Null);
        assert_eq!(v["gapVsMeanAbs"], Value::Null);
        assert_eq!(v["gapVsMeanPct"], Value::Null);
    }

    #[test]
    fn gap_output_is_sorted_by_key_for_deterministic_upserts() {
        let posted = posted_map(vec![
            (("9329", "MZDOVA"), cell(&[30_000.0])),
            (("1120", "MZDOVA"), cell(&[100_000.0])),
            (("5223", "MZDOVA"), cell(&[40_000.0])),
        ]);
        let mut official = HashMap::new();
        for g in ["9329", "1120", "5223"] {
            official.insert((g.to_string(), "MZDOVA".to_string()), (35_000.0, None));
        }
        let keys: Vec<String> = compute_salary_gaps(&posted, &official, 1)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["1120|MZDOVA", "5223|MZDOVA", "9329|MZDOVA"]);
    }

    // ── salary nowcast (ratio-carry) ────────────────────────────────────────

    fn anchor_map(
        entries: &[((&str, &str), (i32, u32, u32))],
    ) -> HashMap<(String, String), NaiveDate> {
        entries
            .iter()
            .map(|((g, s), (y, m, d))| {
                (
                    (g.to_string(), s.to_string()),
                    NaiveDate::from_ymd_opt(*y, *m, *d).unwrap(),
                )
            })
            .collect()
    }

    fn ratios_map(entries: &[(&str, &[f64])]) -> HashMap<String, Vec<f64>> {
        entries
            .iter()
            .map(|(k, r)| (k.to_string(), r.to_vec()))
            .collect()
    }

    #[test]
    fn nowcast_carries_the_median_ratio_onto_todays_posted_median() {
        // posted median 50k; observed posted/official ratios median = 1.25
        // → nowcast official-grade median = 50k / 1.25 = 40k.
        let posted = posted_map(vec![(
            ("5223", "MZDOVA"),
            cell(&[40_000.0, 50_000.0, 60_000.0]),
        )]);
        let ratios = ratios_map(&[("5223|MZDOVA", &[1.30, 1.25, 1.20])]);
        let anchors = anchor_map(&[(("5223", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 3);
        assert_eq!(items.len(), 1);
        let (key, v) = &items[0];
        assert_eq!(key, "5223|MZDOVA");
        assert_eq!(v["isco4"], "5223");
        assert_eq!(v["sfera"], "MZDOVA");
        assert_eq!(v["posted_median"], 50_000);
        assert_eq!(v["nowcast_median"], 40_000);
        assert_eq!(v["ratio_used"], 1.25);
        assert_eq!(v["observations"], 3);
        assert_eq!(v["ispv_anchor_date"], "2026-07-01");
        assert_eq!(v["staleness_days"], 30);
        assert_eq!(v["method"], "ratio_carry");
        // spread = (1.30 − 1.20) / 1.25 = 0.08 ≤ 0.25 with 3 obs → med
        assert_eq!(v["dispersion"], 0.08);
        assert_eq!(v["confidence"], "med");
    }

    #[test]
    fn nowcast_without_history_or_anchor_emits_no_row_never_extrapolates() {
        let posted = posted_map(vec![
            (("5223", "MZDOVA"), cell(&[40_000.0, 50_000.0, 60_000.0])), // no ratio history
            (("2433", "MZDOVA"), cell(&[50_000.0, 52_000.0, 54_000.0])), // history, no anchor
            (("9329", "MZDOVA"), cell(&[30_000.0, 31_000.0])),           // thin: 2 < min 3
        ]);
        let ratios = ratios_map(&[
            ("2433|MZDOVA", &[1.1, 1.1]),
            ("9329|MZDOVA", &[1.0, 1.0, 1.0]),
        ]);
        let anchors = anchor_map(&[(("9329", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        assert!(compute_salary_nowcast(&posted, &ratios, &anchors, today, 3).is_empty());
    }

    #[test]
    fn nowcast_confidence_thresholds_by_count_and_dispersion() {
        // high: ≥6 obs, spread ≤ 0.10
        assert_eq!(nowcast_confidence(6, 0.10), "high");
        // 6 obs but too dispersed → falls through; ≤0.25 keeps med
        assert_eq!(nowcast_confidence(6, 0.11), "med");
        // med: ≥3 obs, spread ≤ 0.25
        assert_eq!(nowcast_confidence(3, 0.25), "med");
        // wide dispersion → low regardless of count
        assert_eq!(nowcast_confidence(8, 0.26), "low");
        // thin history → low even when perfectly tight
        assert_eq!(nowcast_confidence(2, 0.0), "low");
        assert_eq!(nowcast_confidence(1, 0.0), "low");
    }

    #[test]
    fn ratio_observations_window_and_skip_unparseable_snapshots() {
        // Newest-first snapshots, as changes_since delivers them.
        let snaps = [
            Some(json!({"postedMedian": 50_000, "officialMedian": 40_000})), // 1.25
            None,                                                            // removed revision
            Some(json!({"postedMedian": 48_000})),                           // no official → skip
            Some(json!({"postedMedian": 0, "officialMedian": 40_000})),      // non-positive → skip
            Some(json!({"postedMedian": 44_000, "officialMedian": 40_000})), // 1.10
            Some(json!({"postedMedian": 42_000, "officialMedian": 40_000})), // 1.05 — beyond window
        ];
        let ratios = ratio_observations(snaps.iter().map(|s| s.as_ref()), 2);
        assert_eq!(ratios, vec![1.25, 1.10]);
        // window larger than the parseable set keeps everything parseable
        let all = ratio_observations(snaps.iter().map(|s| s.as_ref()), 10);
        assert_eq!(all, vec![1.25, 1.10, 1.05]);
    }

    #[test]
    fn nowcast_even_observation_count_uses_middle_pair_average() {
        let posted = posted_map(vec![(("5223", "MZDOVA"), cell(&[44_000.0]))]);
        // sorted ratios [1.0, 1.1] → median 1.05; 44k / 1.05 ≈ 41_905
        let ratios = ratios_map(&[("5223|MZDOVA", &[1.1, 1.0])]);
        let anchors = anchor_map(&[(("5223", "MZDOVA"), (2026, 6, 30))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1);
        assert_eq!(items.len(), 1);
        let v = &items[0].1;
        assert_eq!(v["ratio_used"], 1.05);
        assert_eq!(v["nowcast_median"], 41_905);
        assert_eq!(v["observations"], 2);
        assert_eq!(v["confidence"], "low"); // 2 obs, however tight
        assert_eq!(v["staleness_days"], 31);
    }

    #[test]
    fn nowcast_output_is_sorted_by_key_for_deterministic_upserts() {
        let posted = posted_map(vec![
            (("9329", "MZDOVA"), cell(&[30_000.0])),
            (("1120", "MZDOVA"), cell(&[100_000.0])),
        ]);
        let ratios = ratios_map(&[
            ("9329|MZDOVA", &[1.0, 1.0, 1.0]),
            ("1120|MZDOVA", &[1.2, 1.2, 1.2]),
        ]);
        let anchors = anchor_map(&[
            (("9329", "MZDOVA"), (2026, 7, 1)),
            (("1120", "MZDOVA"), (2026, 7, 1)),
        ]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let keys: Vec<String> = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["1120|MZDOVA", "9329|MZDOVA"]);
    }

    // ── nowcast honesty floor ───────────────────────────────────────────────

    /// The anti-pattern: `ratio_used <= 0.0` was the ONLY output-side guard, so
    /// any positive-but-garbage divisor surviving the window minted an
    /// arbitrarily implausible "projected median" that then persisted with full
    /// numeric authority.
    #[test]
    fn garbage_ratio_withholds_the_number_instead_of_minting_an_implausible_median() {
        let posted = posted_map(vec![(
            ("5223", "MZDOVA"),
            cell(&[40_000.0, 50_000.0, 60_000.0]),
        )]);
        // A corrupted history: postings supposedly pay 2% of the official median.
        // Unguarded, 50 000 / 0.02 = 2 500 000 CZK/month ships as a "projection".
        let ratios = ratios_map(&[("5223|MZDOVA", &[0.02, 0.02, 0.02])]);
        let anchors = anchor_map(&[(("5223", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 3);
        assert_eq!(items.len(), 1, "the row must still ship as the retraction");
        let v = &items[0].1;
        assert!(v["nowcast_median"].is_null());
        assert_eq!(v["withheld"], "implausible_ratio");
        assert_eq!(v["confidence"], "none");
        // The evidence for the refusal stays readable; the dispersion of a
        // garbage ratio does not.
        assert_eq!(v["ratio_used"], 0.02);
        assert_eq!(v["observations"], 3);
        assert!(v["dispersion"].is_null());
        // And the honest half of the row survives, so the cell is still legible.
        assert_eq!(v["posted_median"], 50_000);
    }

    /// A plausible-looking ratio can still project outside the very band every
    /// posted salary point had to clear to be counted (`SALARY_MIN..=SALARY_MAX`).
    #[test]
    fn projection_outside_the_salary_admission_band_is_withheld_not_published() {
        // posted median 5 000 (the band floor) ÷ ratio 4.0 → 1 250 CZK/month.
        let posted = posted_map(vec![(("9329", "MZDOVA"), cell(&[5_000.0, 5_000.0]))]);
        let ratios = ratios_map(&[("9329|MZDOVA", &[4.0, 4.0, 4.0])]);
        let anchors = anchor_map(&[(("9329", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1);
        assert_eq!(items.len(), 1);
        let v = &items[0].1;
        assert!(v["nowcast_median"].is_null());
        assert_eq!(v["withheld"], "out_of_band");
        assert_eq!(v["confidence"], "none");
    }

    /// A 1-observation cell used to ship a number indistinguishable in effect
    /// from a 6-observation one — same field, same authority, only a `low` label
    /// and an `observations` count no consumer is forced to read.
    #[test]
    fn one_observation_cell_withholds_its_number_not_ships_it_as_merely_low() {
        let posted = posted_map(vec![
            (("5223", "MZDOVA"), cell(&[50_000.0])),
            (("1120", "MZDOVA"), cell(&[100_000.0])),
        ]);
        let ratios = ratios_map(&[
            ("5223|MZDOVA", &[1.25]),                         // one reading
            ("1120|MZDOVA", &[1.2, 1.2, 1.2, 1.2, 1.2, 1.2]), // six
        ]);
        let anchors = anchor_map(&[
            (("5223", "MZDOVA"), (2026, 7, 1)),
            (("1120", "MZDOVA"), (2026, 7, 1)),
        ]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1);
        let by_key: HashMap<&str, &Value> = items.iter().map(|(k, v)| (k.as_str(), v)).collect();
        let thin = by_key["5223|MZDOVA"];
        assert!(thin["nowcast_median"].is_null());
        assert_eq!(thin["withheld"], "thin_evidence");
        assert_eq!(thin["confidence"], "none");
        // The well-evidenced cell is unaffected — the floor is a floor, not a mute.
        let thick = by_key["1120|MZDOVA"];
        assert_eq!(thick["nowcast_median"], 83_333);
        assert!(thick["withheld"].is_null());
        assert_eq!(thick["confidence"], "high");
    }

    /// The withheld row must still be EMITTED. `salary_nowcast` is written with
    /// `upsert_many` (partial upsert, no tombstoning), so a suppressed key that
    /// simply stopped being emitted would linger at yesterday's number — keeping
    /// exactly the value the guard exists to retract.
    #[test]
    fn withheld_cell_emits_its_key_so_a_prior_number_is_overwritten_not_left_lingering() {
        let posted = posted_map(vec![(("5223", "MZDOVA"), cell(&[50_000.0]))]);
        let ratios = ratios_map(&[("5223|MZDOVA", &[0.001])]);
        let anchors = anchor_map(&[(("5223", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let keys: Vec<String> = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(keys, vec!["5223|MZDOVA"]);
    }

    #[test]
    fn nowcast_withheld_names_the_ratio_cause_before_the_out_of_band_symptom() {
        // A garbage ratio ALSO throws the median out of band; the cause is the
        // useful answer, so it must win.
        assert_eq!(
            nowcast_withheld(6, 0.02, 2_500_000.0),
            Some("implausible_ratio")
        );
        // Non-finite divisors (a zero ratio survived as `inf` before) are the
        // same class, not a panic and not a published `inf`.
        assert_eq!(
            nowcast_withheld(6, f64::INFINITY, 0.0),
            Some("implausible_ratio")
        );
        assert_eq!(
            nowcast_withheld(6, 0.0, f64::INFINITY),
            Some("implausible_ratio")
        );
        // Plausible ratio, impossible projection.
        assert_eq!(nowcast_withheld(6, 4.0, 1_250.0), Some("out_of_band"));
        // Plausible everything, but one reading behind it.
        assert_eq!(nowcast_withheld(1, 1.25, 40_000.0), Some("thin_evidence"));
        // The publishable case.
        assert_eq!(nowcast_withheld(2, 1.25, 40_000.0), None);
        // Band edges are inclusive on both guards.
        assert_eq!(nowcast_withheld(2, NOWCAST_RATIO_MIN, SALARY_MAX), None);
        assert_eq!(nowcast_withheld(2, NOWCAST_RATIO_MAX, SALARY_MIN), None);
    }

    #[test]
    fn stale_ispv_anchor_degrades_confidence_and_flags_the_row_not_only_stamps_it() {
        let posted = posted_map(vec![(("1120", "MZDOVA"), cell(&[100_000.0]))]);
        // Six perfectly tight observations — `nowcast_confidence` alone says high.
        let ratios = ratios_map(&[("1120|MZDOVA", &[1.2, 1.2, 1.2, 1.2, 1.2, 1.2])]);
        let anchors = anchor_map(&[(("1120", "MZDOVA"), (2024, 6, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let items = compute_salary_nowcast(&posted, &ratios, &anchors, today, 1);
        let v = &items[0].1;
        assert_eq!(v["nowcast_median"], 83_333);
        assert_eq!(v["anchor_stale"], true);
        assert_eq!(v["confidence"], "med", "a stale anchor costs one level");
        assert!(v["staleness_days"].as_i64().unwrap() > NOWCAST_ANCHOR_STALE_DAYS);
    }

    #[test]
    fn degrade_for_stale_anchor_costs_exactly_one_level_and_only_when_stale() {
        // Fresh anchor: untouched.
        assert_eq!(degrade_for_stale_anchor("high", 0), "high");
        assert_eq!(
            degrade_for_stale_anchor("high", NOWCAST_ANCHOR_STALE_DAYS),
            "high",
            "the threshold itself is not yet stale"
        );
        // Stale: one level, and no further.
        let stale = NOWCAST_ANCHOR_STALE_DAYS + 1;
        assert_eq!(degrade_for_stale_anchor("high", stale), "med");
        assert_eq!(degrade_for_stale_anchor("med", stale), "low");
        assert_eq!(degrade_for_stale_anchor("low", stale), "low");
        assert!(!anchor_is_stale(NOWCAST_ANCHOR_STALE_DAYS));
        assert!(anchor_is_stale(stale));
    }

    /// A fresh anchor must not silently gain the flag — the negative half of the
    /// staleness judgment, which is what keeps `anchor_stale` meaningful.
    #[test]
    fn fresh_anchor_keeps_full_confidence_and_is_not_flagged() {
        let posted = posted_map(vec![(("1120", "MZDOVA"), cell(&[100_000.0]))]);
        let ratios = ratios_map(&[("1120|MZDOVA", &[1.2, 1.2, 1.2, 1.2, 1.2, 1.2])]);
        let anchors = anchor_map(&[(("1120", "MZDOVA"), (2026, 7, 1))]);
        let today = NaiveDate::from_ymd_opt(2026, 7, 31).unwrap();
        let v = &compute_salary_nowcast(&posted, &ratios, &anchors, today, 1)[0].1;
        assert_eq!(v["anchor_stale"], false);
        assert_eq!(v["confidence"], "high");
    }

    #[test]
    fn unit_group_truncates_to_four_digits() {
        assert_eq!(unit_group("CzIsco/93291"), "9329");
        assert_eq!(unit_group("CzIsco/1120"), "1120");
    }

    #[test]
    fn ares_normalize_extracts_compact_employer_record() {
        // realistic ARES economic-subject shape (subset; extra fields ignored)
        let subject = json!({
            "ico": "27074358",
            "obchodniJmeno": "Alza.cz a.s.",
            "pravniForma": "121",
            "datumVzniku": "2003-08-26",
            "financniUrad": "007",
            "sidlo": {
                "kodStatu": "CZ",
                "kodKraje": 19,
                "nazevKraje": "Hlavní město Praha",
                "textovaAdresa": "Jankovcova 1522/53, Holešovice, 17000 Praha 7"
            },
            "czNace": ["46900", "620", "471"]
        });
        let rec = normalize_ares_employer("27074358", &subject).expect("record");
        assert_eq!(rec["ico"], "27074358");
        assert_eq!(rec["name"], "Alza.cz a.s.");
        assert_eq!(rec["legalForm"], "121");
        assert_eq!(rec["founded"], "2003-08-26");
        assert_eq!(rec["krajId"], "19"); // numeric kodKraje → string
        assert_eq!(rec["krajName"], "Hlavní město Praha");
        assert_eq!(rec["nace"], json!(["46900", "620", "471"]));
        assert_eq!(rec["naceCount"], 3);
    }

    #[test]
    fn ares_normalize_rejects_nameless_and_tolerates_drifted_shapes() {
        // no usable name → nothing honest to persist
        assert!(normalize_ares_employer("123", &json!({"ico": "123"})).is_none());
        assert!(normalize_ares_employer("123", &json!({"obchodniJmeno": "  "})).is_none());
        // NACE as objects, string kodKraje, missing sidlo/dates still normalize
        let subject = json!({
            "obchodniJmeno": "Obec Horní Lhota",
            "sidlo": {"kodKraje": "141"},
            "czNace": [{"kodNace": "84110"}, {"kod": "0161"}, {"nazev": "codeless"}]
        });
        let rec = normalize_ares_employer("00000001", &subject).expect("record");
        assert_eq!(rec["krajId"], "141");
        assert_eq!(rec["krajName"], Value::Null);
        assert_eq!(rec["legalForm"], Value::Null);
        assert_eq!(rec["founded"], Value::Null);
        assert_eq!(rec["nace"], json!(["84110", "0161"]));
        assert_eq!(rec["naceCount"], 3); // total present, codeless entry included
    }

    #[test]
    fn ares_nace_list_is_capped() {
        let many: Vec<String> = (0..30).map(|i| format!("{i:05}")).collect();
        let subject = json!({"obchodniJmeno": "Big s.r.o.", "czNace": many});
        let rec = normalize_ares_employer("00000002", &subject).expect("record");
        assert_eq!(rec["nace"].as_array().unwrap().len(), ARES_NACE_CAP);
        assert_eq!(rec["naceCount"], 30);
    }

    #[test]
    fn distinct_icos_dedupes_pads_and_drops_invalid() {
        let samples = [
            json!({"employerIco": "27074358"}),
            json!({"employerIco": "27074358"}),  // duplicate
            json!({"employerIco": "45274649 "}), // trimmed
            json!({"employerIco": "1234567"}),   // 7 digits → zero-padded
            json!({"employerIco": "12a45678"}),  // non-numeric → dropped
            json!({"employerIco": "123456789"}), // too long → dropped
            json!({"employerIco": ""}),          // empty → dropped
            json!({"title": "no ico"}),          // absent → dropped
        ];
        assert_eq!(
            distinct_icos(samples.iter()),
            vec!["27074358", "45274649", "01234567"]
        );
    }

    // ── vacancy survival ledger ─────────────────────────────────────────────

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn tp(isco: &str, kraj: Option<&str>, band: Option<&str>, ico: Option<&str>) -> TodayPosting {
        TodayPosting {
            czisco: isco.to_string(),
            kraj: kraj.map(str::to_string),
            band: band.map(str::to_string),
            ico: ico.map(str::to_string),
        }
    }

    fn today_map(entries: Vec<(&str, TodayPosting)>) -> HashMap<String, TodayPosting> {
        entries
            .into_iter()
            .map(|(id, t)| (id.to_string(), t))
            .collect()
    }

    fn open_e(
        id: &str,
        isco: &str,
        ico: Option<&str>,
        first_seen: &str,
        last_seen: &str,
    ) -> OpenEntry {
        OpenEntry {
            id: id.to_string(),
            czisco: isco.to_string(),
            kraj: Some("Kraj/108".to_string()),
            band: Some("40k".to_string()),
            ico: ico.map(str::to_string),
            first_seen: first_seen.to_string(),
            last_seen: last_seen.to_string(),
            seen_count: 1,
        }
    }

    fn ledger(run_date: &str, open: Vec<OpenEntry>, closed: Vec<ClosedEntry>) -> Ledger {
        Ledger {
            run_date: run_date.to_string(),
            open,
            closed,
        }
    }

    #[test]
    fn salary_band_buckets_by_5k_and_never_fabricates() {
        assert_eq!(salary_band(Some(40_000.0)), Some("40k".to_string()));
        assert_eq!(salary_band(Some(44_999.0)), Some("40k".to_string()));
        assert_eq!(salary_band(Some(45_000.0)), Some("45k".to_string()));
        assert_eq!(salary_band(None), None);
    }

    #[test]
    fn diff_first_run_everything_new_nothing_closed() {
        let today = today_map(vec![
            (
                "1",
                tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), Some("123")),
            ),
            ("2", tp("CzIsco/9329", None, None, None)),
        ]);
        let r = diff_ledger(None, &today, d("2026-07-30"), 3, 30);
        assert_eq!(r.new_now, 2);
        assert_eq!((r.ongoing, r.closed_now, r.reposts_now), (0, 0, 0));
        assert!(!r.carried);
        assert_eq!(r.ledger.open.len(), 2);
        assert!(r.ledger.closed.is_empty());
        assert!(r
            .ledger
            .open
            .iter()
            .all(|e| e.first_seen == "2026-07-30" && e.seen_count == 1));
    }

    #[test]
    fn diff_normal_day_ongoing_advances_and_missing_closes_with_days_open() {
        let prior = ledger(
            "2026-07-29",
            vec![
                open_e("1", "CzIsco/5223", None, "2026-07-20", "2026-07-29"),
                open_e("2", "CzIsco/9329", None, "2026-07-25", "2026-07-29"),
            ],
            vec![],
        );
        let today = today_map(vec![(
            "1",
            tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), None),
        )]);
        let r = diff_ledger(Some(prior), &today, d("2026-07-30"), 3, 30);
        assert_eq!((r.new_now, r.ongoing, r.closed_now), (0, 1, 1));
        let kept = &r.ledger.open[0];
        assert_eq!(kept.id, "1");
        assert_eq!(kept.last_seen, "2026-07-30");
        assert_eq!(kept.seen_count, 2);
        assert_eq!(kept.first_seen, "2026-07-20"); // never reset
        let closed = &r.ledger.closed[0];
        assert_eq!(closed.id, "2");
        assert_eq!(closed.closed_at, "2026-07-30");
        assert_eq!(closed.days_open, 5); // 07-25 → 07-30
        assert!(closed.repost_id.is_none());
    }

    #[test]
    fn diff_gap_beyond_tolerance_closes_nothing_and_carries_forward() {
        let prior = ledger(
            "2026-07-20", // 10-day outage > maxGapDays 3
            vec![
                open_e("1", "CzIsco/5223", None, "2026-07-10", "2026-07-20"),
                open_e("2", "CzIsco/9329", None, "2026-07-15", "2026-07-20"),
            ],
            vec![],
        );
        let today = today_map(vec![
            ("1", tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), None)),
            ("3", tp("CzIsco/7112", None, None, None)),
        ]);
        let r = diff_ledger(Some(prior), &today, d("2026-07-30"), 3, 30);
        assert!(r.carried);
        assert_eq!(r.gap_days, 10);
        assert_eq!(r.closed_now, 0);
        assert!(r.ledger.closed.is_empty());
        // "2" was absent but survives untouched; "1" advances; "3" is new.
        assert_eq!(r.ledger.open.len(), 3);
        let e2 = r.ledger.open.iter().find(|e| e.id == "2").unwrap();
        assert_eq!(e2.last_seen, "2026-07-20");
        assert_eq!(r.new_now, 1);
    }

    #[test]
    fn diff_same_day_rerun_and_bad_prior_date_never_close() {
        let prior_open = vec![open_e("1", "CzIsco/5223", None, "2026-07-25", "2026-07-30")];
        let today = today_map(vec![]);
        // Same-day re-run: gap 0 is outside 1..=max, so the absent posting stays.
        let r = diff_ledger(
            Some(ledger("2026-07-30", prior_open.clone(), vec![])),
            &today,
            d("2026-07-30"),
            3,
            30,
        );
        assert!(r.carried && r.closed_now == 0 && r.ledger.open.len() == 1);
        // Unparseable prior run_date: never mass-close on bad data.
        let r = diff_ledger(
            Some(ledger("garbage", prior_open, vec![])),
            &today,
            d("2026-07-30"),
            3,
            30,
        );
        assert!(r.carried && r.closed_now == 0 && r.ledger.open.len() == 1);
    }

    #[test]
    fn repost_matches_on_ico_isco_kraj_band_within_window_and_links_ids() {
        let prior = ledger(
            "2026-07-29",
            vec![open_e(
                "old",
                "CzIsco/5223",
                Some("123"),
                "2026-07-01",
                "2026-07-29",
            )],
            vec![],
        );
        // Day 1: "old" disappears → closed.
        let r1 = diff_ledger(Some(prior), &today_map(vec![]), d("2026-07-30"), 3, 30);
        assert_eq!(r1.closed_now, 1);
        // Day 2: same employer + occupation + kraj + band reappears under a new id.
        let today = today_map(vec![(
            "new",
            tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), Some("123")),
        )]);
        let r2 = diff_ledger(Some(r1.ledger), &today, d("2026-07-31"), 3, 30);
        assert_eq!(r2.reposts_now, 1);
        let c = &r2.ledger.closed[0];
        assert_eq!(c.id, "old");
        assert_eq!(c.repost_id.as_deref(), Some("new"));
    }

    #[test]
    fn repost_requires_ico_and_exact_cell_match() {
        let closed = ClosedEntry {
            id: "old".to_string(),
            czisco: "CzIsco/5223".to_string(),
            kraj: Some("Kraj/108".to_string()),
            band: Some("40k".to_string()),
            ico: None, // no employer id → can never be repost-linked
            closed_at: "2026-07-29".to_string(),
            days_open: 4,
            repost_id: None,
        };
        let today = today_map(vec![(
            "new",
            tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), Some("123")),
        )]);
        let r = diff_ledger(
            Some(ledger("2026-07-29", vec![], vec![closed.clone()])),
            &today,
            d("2026-07-30"),
            3,
            30,
        );
        assert_eq!(r.reposts_now, 0);
        // With an IČO but a different salary band, still no match.
        let mut with_ico = closed;
        with_ico.ico = Some("123".to_string());
        with_ico.band = Some("60k".to_string());
        let r = diff_ledger(
            Some(ledger("2026-07-29", vec![], vec![with_ico])),
            &today,
            d("2026-07-30"),
            3,
            30,
        );
        assert_eq!(r.reposts_now, 0);
    }

    #[test]
    fn closed_entries_age_out_of_the_repost_window() {
        let stale = ClosedEntry {
            id: "old".to_string(),
            czisco: "CzIsco/5223".to_string(),
            kraj: Some("Kraj/108".to_string()),
            band: Some("40k".to_string()),
            ico: Some("123".to_string()),
            closed_at: "2026-06-01".to_string(), // far outside a 30-day window
            days_open: 10,
            repost_id: None,
        };
        let today = today_map(vec![(
            "new",
            tp("CzIsco/5223", Some("Kraj/108"), Some("40k"), Some("123")),
        )]);
        let r = diff_ledger(
            Some(ledger("2026-07-29", vec![], vec![stale])),
            &today,
            d("2026-07-30"),
            3,
            30,
        );
        // Pruned before matching: no repost link, and the window stays bounded.
        assert_eq!(r.reposts_now, 0);
        assert!(r.ledger.closed.is_empty());
    }

    #[test]
    fn lifecycle_aggregate_percentiles_repost_share_churn_and_min_count() {
        let mk = |days: i64, kraj: Option<&str>, repost: bool| ClosedEntry {
            id: format!("c{days}"),
            czisco: "CzIsco/52230".to_string(), // unit group 5223
            kraj: kraj.map(str::to_string),
            band: None,
            ico: None,
            closed_at: "2026-07-30".to_string(),
            days_open: days,
            repost_id: repost.then(|| "r".to_string()),
        };
        let closed = vec![
            mk(2, Some("Kraj/108"), false),
            mk(4, Some("Kraj/108"), true),
            mk(6, Some("Kraj/108"), false),
            mk(30, Some("Kraj/116"), false), // below minCount as a regional cell
        ];
        let mut live = HashMap::new();
        live.insert(("5223".to_string(), "Kraj/108".to_string()), 30usize);
        live.insert(("5223".to_string(), "ALL".to_string()), 40usize);
        let items = aggregate_lifecycle(&closed, &live, 3, 30);
        // Kraj/116 (1 closure) suppressed; Kraj/108 + ALL survive, sorted.
        let keys: Vec<&str> = items.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["5223|ALL", "5223|Kraj/108"]);
        let regional = &items[1].1;
        assert_eq!(regional["metric"], "time_to_close");
        assert_eq!(regional["closedCount"], 3);
        assert_eq!(regional["medianDaysToClose"], 4);
        assert_eq!(regional["p75DaysToClose"], 6);
        assert_eq!(regional["repostCount"], 1);
        assert_eq!(regional["repostSharePct"], 33.3);
        assert_eq!(regional["liveCount"], 30);
        assert_eq!(regional["churnPct"], 10.0); // 3 closed vs 30 live
        let all = &items[0].1;
        assert_eq!(all["closedCount"], 4); // Kraj/116 closure still counts here
        assert_eq!(all["churnPct"], 10.0); // 4 vs 40
                                           // Unknown live cell → no fabricated churn.
        let no_live = aggregate_lifecycle(&closed, &HashMap::new(), 3, 30);
        assert!(no_live[0].1["churnPct"].is_null());
        assert!(no_live[0].1["liveCount"].is_null());
    }

    #[test]
    fn ledger_serializes_rows_as_compact_tuples_and_round_trips() {
        let l = ledger(
            "2026-07-30",
            vec![open_e(
                "1",
                "CzIsco/5223",
                Some("123"),
                "2026-07-20",
                "2026-07-30",
            )],
            vec![ClosedEntry {
                id: "2".to_string(),
                czisco: "CzIsco/9329".to_string(),
                kraj: None,
                band: None,
                ico: None,
                closed_at: "2026-07-30".to_string(),
                days_open: 3,
                repost_id: None,
            }],
        );
        let s = serde_json::to_string(&l).unwrap();
        // Rows are arrays, not objects — field names must not repeat 300k times.
        assert!(
            s.contains(r#"["1","CzIsco/5223","Kraj/108","40k","123","2026-07-20","2026-07-30",1]"#)
        );
        assert!(s.contains(r#"["2","CzIsco/9329",null,null,null,"2026-07-30",3,null]"#));
        let back: Ledger = serde_json::from_str(&s).unwrap();
        assert_eq!(back.open, l.open);
        assert_eq!(back.closed, l.closed);
        assert_eq!(back.run_date, "2026-07-30");
    }
}
