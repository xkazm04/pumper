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
//! to derive org type. The full file needs a raised `[http] timeout_secs`.

#![allow(non_snake_case)]

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{Duration, NaiveDate};
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde::Deserialize;
use serde_json::{json, Value};

pub struct MpsvVpm;

/// Full national vacancy register (~188 MB, replaced daily).
const FULL_URL: &str = "https://data.mpsv.cz/od/soubory/volna-mista/volna-mista.json";
/// Salary sanity band (CZK monthly) — drops hourly-mislabeled rows and errors.
const SALARY_MIN: f64 = 5_000.0;
const SALARY_MAX: f64 = 2_000_000.0;

/// Official ISPV salary statistics — read cross-app from the store.
const ISPV_APP: &str = "mpsv-ispv";
const ISPV_DATASET: &str = "wages";
/// Virtual shared namespace for the posted-vs-official join (grants-common
/// pattern: cross-source products live in a namespace no single app owns).
const GAP_APP: &str = "cz-labour";
const GAP_DATASET: &str = "salary_gap";

/// ARES business register — key-free JSON REST lookup of one economic subject
/// by IČO. Enriches the employers behind this run's vacancy samples.
const ARES_URL: &str = "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty";
/// Per-run cap on NEW ARES lookups — this is enrichment, not a crawl; the
/// backlog drains across daily runs (already-enriched IČOs are skipped).
const ARES_MAX_LOOKUPS_DEFAULT: u64 = 50;
/// Cap on CZ-NACE activity codes kept per employer record.
const ARES_NACE_CAP: usize = 12;

#[async_trait]
impl ScrapeApp for MpsvVpm {
    fn name(&self) -> &'static str {
        "mpsv-vpm"
    }

    fn description(&self) -> &'static str {
        "Czech national job-vacancy register (MPSV / ÚP ČR open data, key-free, CC BY 4.0). \
         Aggregates the ~300k live postings into `role_region_agg` (CZ-ISCO × kraj × orgType: \
         count + monthly-salary distribution; kraj `ALL` = national) and `vacancy_samples` \
         (JD references). Also joins posted salaries against mpsv-ispv official ISPV \
         statistics into `cz-labour/salary_gap` (per CZ-ISCO unit group × sphere), \
         and enriches sampled employers from the key-free ARES business register \
         into `employers` (keyed by IČO: name, legal form, founded, kraj, CZ-NACE). \
         Drops stale relics: postings first posted more than \
         `maxPostedAgeDays` before the feed date are excluded (0 = keep all). \
         Params: {\"url\": endpoint override, \"maxRecords\": 0=all, \
         \"minCount\": 3 (min postings per aggregate cell), \"samplesPerGroup\": 4, \
         \"maxPostedAgeDays\": 730 (0 = keep all ages), \
         \"aresMaxLookups\": 50 (new ARES lookups per run, 0 = disable)}"
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
        })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let url = ctx
            .params
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or(FULL_URL)
            .to_string();
        let max_records = ctx.params.get("maxRecords").and_then(Value::as_u64).unwrap_or(0) as usize;
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
        let mut req = HttpRequest::get(&url);
        req.no_cache = true;
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

        let total = feed.polozky.len();
        let considered = if max_records == 0 { total } else { total.min(max_records) };

        // Reference "today" = the most recent change date in the feed (≈ its
        // publish date); posting age and the recency cutoff are measured from it.
        let ref_date: Option<NaiveDate> = feed
            .polozky
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
        // gather a few extra candidates per group, then keep only the richest N
        let gather_cap = samples_per_group.saturating_mul(6).max(samples_per_group);

        for p in feed.polozky.iter().take(considered) {
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

            let czisco = match p.czisco() {
                Some(c) => c,
                None => continue, // unclassifiable postings can't feed the products
            };
            let org = p.org_type();
            let kraj = p.kraj();
            let salary = p.monthly_salary_point();

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

            // region rollups (all occupations): per (kraj, orgType), per (kraj, all),
            // and national (ALL, orgType) + (ALL, all).
            if let Some(k) = &kraj {
                regions.entry((k.clone(), org.clone())).or_default().add(salary);
                regions.entry((k.clone(), "all".to_string())).or_default().add(salary);
            }
            regions.entry(("ALL".to_string(), org.clone())).or_default().add(salary);
            regions
                .entry(("ALL".to_string(), "all".to_string()))
                .or_default()
                .add(salary);

            // gap-benchmark pool: unit group × sphere (only rows with a salary count)
            gap_cells
                .entry((unit_group(&czisco), sphere_for_org(&org).to_string()))
                .or_default()
                .add(salary);

            // sample reservoir per CZ-ISCO unit group
            let bucket = groups.entry(unit_group(&czisco)).or_default();
            if bucket.len() < gather_cap {
                if let Some(s) = p.as_sample(&czisco, &org, kraj.as_deref(), salary) {
                    bucket.push(s);
                }
            }
        }

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
            list.sort_by(|a, b| b.richness.cmp(&a.richness).then_with(|| b.posted.cmp(&a.posted)));
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

        let agg = ctx.upsert_many("role_region_agg", &agg_items).await?;
        let region = ctx.upsert_many("region_agg", &region_items).await?;
        let samples = ctx.upsert_many("vacancy_samples", &sample_items).await?;
        ctx.upsert("freshness", "current", &freshness).await?;

        // Trending vs fading roles: national posting-count trajectories from
        // role_region_agg's revision history (the change-intelligence
        // substrate). Window = the cell's last 10 revisions, i.e. roughly its
        // last 10 *changed* days; unchanged days write no revision.
        let mut trend_items: Vec<(String, Value)> = Vec::new();
        for (key, _) in agg_items.iter().filter(|(k, _)| k.contains("|ALL|")) {
            let revs = ctx.datasets.history(&ctx.app, "role_region_agg", key, 10).await?;
            let count_of = |rev: &pumper_core::Revision| {
                rev.data.as_ref().and_then(|d| d.get("count")).and_then(Value::as_i64)
            };
            let Some(latest) = revs.first().and_then(count_of) else { continue };
            // Oldest snapshot within the window; None = the cell is brand new.
            let prev = revs.iter().skip(1).filter_map(count_of).next_back();
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
                    "revisions": revs.len(),
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
                .upsert_many(GAP_APP, GAP_DATASET, &gap_items)
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
        let mut employer_items: Vec<(String, Value)> = Vec::new();
        let mut ares_skipped = 0usize; // already enriched in a prior run
        let mut ares_failed = 0usize; // transport / 404 / malformed
        let mut ares_capped = 0usize; // left for a later run (per-run cap)
        let mut ares_looked_up = 0usize;
        for ico in &icos {
            // already in the employers dataset → nothing to fetch
            if ctx.datasets.get(&ctx.app, "employers", ico).await?.is_some() {
                ares_skipped += 1;
                continue;
            }
            if ares_looked_up >= ares_max {
                ares_capped += 1;
                continue;
            }
            ares_looked_up += 1;
            let ares_url = format!("{ARES_URL}/{ico}");
            let resp = match ctx.engines.http.fetch(HttpRequest::get(&ares_url)).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("mpsv-vpm: ARES fetch failed for IČO {ico}: {e}");
                    ares_failed += 1;
                    continue;
                }
            };
            if !resp.is_success() {
                tracing::warn!(
                    "mpsv-vpm: ARES returned status {} for IČO {ico} — skipping",
                    resp.status
                );
                ares_failed += 1;
                continue;
            }
            let subject: Value = match serde_json::from_str(&resp.body) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("mpsv-vpm: ARES body for IČO {ico} was not JSON: {e}");
                    ares_failed += 1;
                    continue;
                }
            };
            match normalize_ares_employer(ico, &subject) {
                Some(rec) => employer_items.push((ico.clone(), rec)),
                None => {
                    tracing::warn!("mpsv-vpm: ARES subject for IČO {ico} had no usable name");
                    ares_failed += 1;
                }
            }
        }
        let employers = ctx.upsert_many("employers", &employer_items).await?;
        let employer_summary = json!({
            "distinctIcos": icos.len(),
            "enriched": employer_items.len(),
            "new": employers.new.len(),
            "changed": employers.changed.len(),
            "unchanged": employers.unchanged,
            "skippedExisting": ares_skipped,
            "capped": ares_capped,
            "failed": ares_failed,
            "maxLookups": ares_max,
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
            "trendCells": trend_items.len(),
            "trendsChanged": trends.new.len() + trends.changed.len(),
            "trendingTop": trending_top,
            "fadingTop": fading_top,
            "salaryGap": salary_gap,
            "employers": employer_summary,
            "freshness": freshness,
        });
        ctx.save_artifact("summary.json", &serde_json::to_vec_pretty(&out)?)
            .await?;
        Ok(out)
    }
}

/// Numeric CZ-ISCO unit group: `"CzIsco/93291"` → `"9329"` (first 4 digits of the
/// bare code). Buckets JD samples at the ISCO unit-group level.
fn unit_group(czisco: &str) -> String {
    let code = czisco.rsplit('/').next().unwrap_or(czisco);
    let digits: String = code.chars().filter(|c| c.is_ascii_digit()).take(4).collect();
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

/// Index of official ISPV rows: (CZ-ISCO unit group, sfera) → (medianMzda,
/// mzdaPrumer). Rows without a positive monthly median are dropped — no
/// benchmark can honestly be computed against them.
fn official_wage_index<'a>(
    rows: impl Iterator<Item = &'a Value>,
) -> HashMap<(String, String), (f64, Option<f64>)> {
    let mut index = HashMap::new();
    for r in rows {
        let Some(czisco) = r.get("czIsco").and_then(Value::as_str) else { continue };
        let sfera = r.get("sfera").and_then(Value::as_str).unwrap_or("").to_string();
        let Some(median) = r.get("medianMzda").and_then(Value::as_f64).filter(|m| *m > 0.0)
        else {
            continue;
        };
        let mean = r.get("mzdaPrumer").and_then(Value::as_f64).filter(|m| *m > 0.0);
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
        let Some(posted_median) = pct(0.5) else { continue };
        let gap = |official: f64| -> (i64, f64) {
            let abs = posted_median as f64 - official;
            (abs.round() as i64, (abs / official * 100.0 * 10.0).round() / 10.0)
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

/// Distinct valid employer IČOs from this run's persisted vacancy samples,
/// zero-padded to the canonical 8 digits (ARES's path format), in first-seen
/// order for deterministic capping.
fn distinct_icos<'a>(samples: impl Iterator<Item = &'a Value>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut icos = Vec::new();
    for v in samples {
        let Some(raw) = v.get("employerIco").and_then(Value::as_str) else { continue };
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

// ── typed subset of the feed (unknown fields are ignored, bounding memory) ──

#[derive(Deserialize)]
struct Feed {
    #[serde(default)]
    polozky: Vec<Posting>,
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
        if self.souhlasAgenturyAgentura == Some(true) || self.souhlasAgenturyUzivatel == Some(true) {
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
        let education = self.minPozadovaneVzdelani.as_ref().and_then(|e| e.id.clone());
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
        Some(Sample { richness, posted, value })
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

    fn cell(salaries: &[f64]) -> Cell {
        let mut c = Cell::default();
        for &s in salaries {
            c.add(Some(s));
        }
        c
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

    #[test]
    fn sphere_mapping_public_vs_rest() {
        assert_eq!(sphere_for_org("public"), "PLATOVA");
        assert_eq!(sphere_for_org("private"), "MZDOVA");
        assert_eq!(sphere_for_org("agency"), "MZDOVA");
    }

    #[test]
    fn official_index_keys_by_unit_group_and_drops_medianless_rows() {
        let rows = vec![
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
        let posted = posted_map(vec![(("5223", "MZDOVA"), cell(&[40_000.0, 50_000.0, 60_000.0]))]);
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
        official.insert(("2433".to_string(), "PLATOVA".to_string()), (45_000.0, None));
        official.insert(("5223".to_string(), "MZDOVA".to_string()), (40_000.0, None));
        assert!(compute_salary_gaps(&posted, &official, 3).is_empty());
    }

    #[test]
    fn gap_handles_negative_gap_and_missing_official_mean() {
        let posted = posted_map(vec![(("5223", "PLATOVA"), cell(&[30_000.0, 30_000.0, 30_000.0]))]);
        let mut official = HashMap::new();
        official.insert(("5223".to_string(), "PLATOVA".to_string()), (40_000.0, None));
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
        let samples = vec![
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
}
