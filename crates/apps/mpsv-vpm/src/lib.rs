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

#[async_trait]
impl ScrapeApp for MpsvVpm {
    fn name(&self) -> &'static str {
        "mpsv-vpm"
    }

    fn description(&self) -> &'static str {
        "Czech national job-vacancy register (MPSV / ÚP ČR open data, key-free, CC BY 4.0). \
         Aggregates the ~300k live postings into `role_region_agg` (CZ-ISCO × kraj × orgType: \
         count + monthly-salary distribution; kraj `ALL` = national) and `vacancy_samples` \
         (JD references). Drops stale relics: postings first posted more than \
         `maxPostedAgeDays` before the feed date are excluded (0 = keep all). \
         Params: {\"url\": endpoint override, \"maxRecords\": 0=all, \
         \"minCount\": 3 (min postings per aggregate cell), \"samplesPerGroup\": 4, \
         \"maxPostedAgeDays\": 730 (0 = keep all ages)}"
    }

    /// Daily full sync at 06:00 UTC. Change detection makes the output meaningful
    /// even on a full re-fetch (only new/changed aggregate cells are reported).
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 6 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "maxRecords": 0, "minCount": 3, "samplesPerGroup": 4, "maxPostedAgeDays": 730 })
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
    typMzdy: Option<IdRef>,
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

    fn is_monthly(&self) -> bool {
        self.typMzdy
            .as_ref()
            .and_then(|t| t.id.as_deref())
            .map(|id| id.contains("mesic"))
            .unwrap_or(false)
    }

    /// A single representative CZK monthly figure: midpoint of the band when both
    /// ends are given, else whichever end is present; `None` if not a sane monthly.
    fn monthly_salary_point(&self) -> Option<f64> {
        if !self.is_monthly() {
            return None;
        }
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
        // IČO → the join key for a future ARES org-size enrichment (startup vs corporate).
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
