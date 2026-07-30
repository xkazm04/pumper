//! CORDIS funded Horizon projects via the CORDIS Search API — the *outcomes*
//! side of the EU funding corpus (eu-sedia carries the open-calls side). Every
//! funded project with its topic identifier, EU contribution, coordinator and
//! participants, keyed by RCN into `projects`; per-topic-family rollups into
//! `topic_stats`, which eu-sedia joins onto open topics as a `history` block.
//! `http` engine.
//!
//! Data type: AWARDED HISTORY. Access: key-free.
//!
//! Contract (ASSUMED, pinned 2026-07-30 — NOT yet verified live; the first
//! defensive fetch below is the tripwire):
//!   GET https://cordis.europa.eu/api/search/results
//!       ?q=<CORDIS query language>&format=json&p=<page,1-based>&num=<size≤100>
//!   default q: `contenttype='project' AND /project/frameworkProgramme='HORIZON'`
//!   Response envelope: `{"payload": {"total": N, "hits": [...]}}`, with the
//!   hits array possibly named `results` and each hit possibly nested under a
//!   `"hit"`/`"project"` wrapper (the API has shipped both shapes over time).
//!   Project fields mirror the public bulk-CSV columns: rcn, id (grant
//!   agreement), acronym, title, topics, frameworkProgramme, ecMaxContribution,
//!   totalCost, startDate, coordinator, participants (arrays or `;`-joined
//!   strings; EU numbers may use comma decimals).
//! We deliberately use the REST/JSON extraction API and NOT the bulk CSV/ZIP
//! dumps: the http engine has no binary/streaming body support yet (deferred
//! engine-traits#2). If the envelope drifts (positive total, zero parseable
//! hits) the run FAILS loudly instead of reporting a successful empty sweep.
//!
//! Coverage: `max_projects` caps each run (politeness — the governor paces
//! requests, the cap bounds them). A resume cursor persisted in the `state`
//! dataset advances the page window across scheduled runs and wraps at the end,
//! so the ~15k-project Horizon corpus is covered over successive weekly runs
//! without ever hammering the API in one go.

use std::collections::HashMap;

use app_eu_sedia::topic_lineage;
use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Cordis;

const SEARCH_URL: &str = "https://cordis.europa.eu/api/search/results";
const DEFAULT_QUERY: &str = "contenttype='project' AND /project/frameworkProgramme='HORIZON'";
/// Upper bound when aggregating the stored corpus (Horizon Europe is ~15k
/// projects; this leaves an order of magnitude of headroom).
const AGGREGATE_LIMIT: i64 = 200_000;
/// Bounded participant-org leaderboard per family.
const TOP_PARTICIPANTS: usize = 10;

#[async_trait]
impl ScrapeApp for Cordis {
    fn name(&self) -> &'static str {
        "cordis"
    }

    fn description(&self) -> &'static str {
        "CORDIS funded Horizon projects (Search API, key-free). Awarded-history \
         corpus keyed by RCN into `projects`, plus per-topic-family win stats in \
         `topic_stats` (joined by eu-sedia onto open topics). \
         Params: {\"query\": CORDIS query override, \"pageSize\": 1-100, \
         \"maxProjects\": 1-5000 per-run cap (default 500), \
         \"startPage\": override the persisted resume cursor}"
    }

    /// Weekly, Mondays 07:00 UTC — outcomes data moves slowly, and the resume
    /// cursor sweeps the whole corpus across runs.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 7 * * 1")
    }

    fn default_params(&self) -> Value {
        // Conservative per-run cap: 500 projects = 5 pages. Full-corpus coverage
        // comes from the resume cursor across scheduled runs, not one big sweep.
        json!({ "pageSize": 100, "maxProjects": 500 })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let query = ctx
            .params
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_QUERY)
            .to_string();
        let page_size = ctx
            .params
            .get("pageSize")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 100);
        let max_projects = ctx
            .params
            .get("maxProjects")
            .and_then(Value::as_u64)
            .unwrap_or(500)
            .clamp(1, 5000);

        // Resume cursor: continue where the last run stopped so the corpus is
        // covered over successive runs. An explicit `startPage` param overrides.
        let cursor_start = match ctx.datasets.get(&ctx.app, "state", "cursor").await? {
            Some(rec) => rec.data.get("next_page").and_then(Value::as_u64).unwrap_or(1),
            None => 1,
        };
        let start_page = ctx
            .params
            .get("startPage")
            .and_then(Value::as_u64)
            .unwrap_or(cursor_start)
            .max(1);

        let mut records: Vec<(String, Value)> = Vec::new();
        let mut skipped: u64 = 0;
        let mut total: u64 = 0;
        let mut page = start_page;
        let mut pages_fetched: u64 = 0;
        let mut exhausted = false;

        while (records.len() as u64) < max_projects {
            let url = url::Url::parse_with_params(
                SEARCH_URL,
                &[
                    ("q", query.as_str()),
                    ("format", "json"),
                    ("p", &page.to_string()),
                    ("num", &page_size.to_string()),
                ],
            )
            .map_err(|e| Error::App(format!("cordis: bad search url: {e}")))?;
            let resp = ctx.engines.http.fetch(HttpRequest::get(url)).await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "cordis search returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }
            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("cordis: response was not JSON: {e}")))?;
            if pages_fetched == 0 {
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            // Defensive envelope handling: the contract above is ASSUMED. A
            // positive total with zero parseable hits means the envelope drifted
            // from every shape we accept — refuse to report an empty success.
            let (page_total, hits) = extract_hits(&parsed).ok_or_else(|| {
                Error::App(
                    "cordis: could not locate total+hits in the response envelope — \
                     the assumed Search API contract does not match (see crate doc \
                     header; page1.json artifact holds the raw body)"
                        .to_string(),
                )
            })?;
            total = page_total;
            if total > 0 && hits.is_empty() && pages_fetched == 0 && start_page == 1 {
                return Err(Error::App(format!(
                    "cordis: API reported {total} results but page 1 parsed 0 hits — \
                     likely an upstream schema change"
                )));
            }

            let got = hits.len() as u64;
            for hit in &hits {
                match normalize_project(hit) {
                    Some((key, record)) => records.push((key, record)),
                    None => skipped += 1,
                }
            }
            pages_fetched += 1;
            page += 1;
            if got < page_size || ((page - 1) * page_size) >= total {
                exhausted = true;
                break;
            }
        }

        let summary = ctx.upsert_many("projects", &records).await?;

        // Re-aggregate topic families over the WHOLE stored corpus (not just
        // this run's window) so stats stay consistent while the cursor sweeps.
        // Change detection makes untouched families free.
        let corpus = ctx.datasets.list(&ctx.app, "projects", AGGREGATE_LIMIT).await?;
        let corpus_values: Vec<&Value> = corpus.iter().map(|r| &r.data).collect();
        let stats = aggregate_topic_stats(&corpus_values);
        let families = stats.len();
        let stats_summary = ctx.upsert_many("topic_stats", &stats).await?;

        // Persist the resume cursor: wrap to page 1 once the corpus is covered.
        let next_page = if exhausted { 1 } else { page };
        ctx.upsert("state", "cursor", &json!({ "next_page": next_page }))
            .await?;

        Ok(json!({
            "source": "cordis.europa.eu/api/search",
            "query": query,
            "totalResults": total,
            "startPage": start_page,
            "pages": pages_fetched,
            "fetched": records.len(),
            "skipped_unkeyed": skipped,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "corpus": corpus.len(),
            "families": families,
            "stats_new": stats_summary.new.len(),
            "stats_changed": stats_summary.changed.len(),
            "cursor_next_page": next_page,
            "corpus_swept": exhausted,
        }))
    }
}

/// Locates `(total, hits)` in the response envelope, tolerating the shapes the
/// Search API has shipped: `payload.{total,hits}`, `payload.{total,results}`,
/// and the same pair at the top level; each hit may be wrapped in a
/// `"hit"`/`"project"` object. Returns `None` when no accepted shape matches —
/// the caller turns that into a loud contract-drift error, never an empty run.
fn extract_hits(parsed: &Value) -> Option<(u64, Vec<Value>)> {
    let envelope = parsed.get("payload").unwrap_or(parsed);
    let total = envelope
        .get("total")
        .or_else(|| envelope.get("totalHits"))
        .and_then(Value::as_u64)?;
    let arr = envelope
        .get("hits")
        .or_else(|| envelope.get("results"))
        .and_then(Value::as_array)?;
    let hits = arr
        .iter()
        .map(|h| {
            h.get("hit")
                .or_else(|| h.get("project"))
                .unwrap_or(h)
                .clone()
        })
        .collect();
    Some((total, hits))
}

/// Normalizes one CORDIS project hit to a stable record keyed by RCN (falling
/// back to the grant-agreement id). `None` when neither key exists — an
/// unkeyable hit is counted, not silently invented.
fn normalize_project(hit: &Value) -> Option<(String, Value)> {
    let key = scalar_string(hit.get("rcn"))
        .or_else(|| scalar_string(hit.get("id")))
        .filter(|s| !s.is_empty())?;

    let topics = name_list(hit.get("topics"));
    let topic = topics.first().cloned();
    let coordinator = name_list(hit.get("coordinator")).first().cloned();
    let mut participants = name_list(hit.get("participants"));
    participants.truncate(50); // bounded — mega-consortia exist

    let record = json!({
        "rcn": scalar_string(hit.get("rcn")),
        "project_id": scalar_string(hit.get("id")),
        "acronym": hit.get("acronym").and_then(Value::as_str),
        "title": hit.get("title").and_then(Value::as_str),
        "topic": topic,
        "framework_programme": scalar_string(hit.get("frameworkProgramme")),
        // Honest money: unparseable ⇒ Null, never 0.
        "ec_contribution": hit
            .get("ecMaxContribution")
            .or_else(|| hit.get("ecContribution"))
            .and_then(parse_amount),
        "total_cost": hit.get("totalCost").and_then(parse_amount),
        "coordinator": coordinator,
        "participants": participants,
        "start_year": hit
            .get("startDate")
            .and_then(Value::as_str)
            .and_then(start_year),
        "status": hit.get("status").and_then(Value::as_str),
    });
    Some((key, record))
}

/// A scalar as an owned string: strings pass through, numbers are formatted
/// (RCNs/ids come back as either). Objects/arrays yield `None`.
fn scalar_string(v: Option<&Value>) -> Option<String> {
    match v? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// A name-ish field as a list: JSON arrays of strings pass through; a single
/// string is split on `;` (the bulk-dump convention for multi-value cells).
fn name_list(v: Option<&Value>) -> Vec<String> {
    match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(|x| scalar_string(Some(x)))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        Some(Value::String(s)) => s
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        _ => Vec::new(),
    }
}

/// Parses an EU money amount: JSON numbers pass through; strings tolerate the
/// comma-decimal convention (`"1234567,89"`) and thousands separators. Anything
/// ambiguous or non-numeric ⇒ `None` (a fabricated €0 would poison the means).
fn parse_amount(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            let normalized = if s.contains(',') && !s.contains('.') {
                // Comma is the decimal separator.
                s.replace(',', ".")
            } else {
                // Dot-decimal; commas (if any) are thousands separators.
                s.replace(',', "")
            };
            normalized.parse::<f64>().ok().filter(|n| n.is_finite())
        }
        _ => None,
    }
}

/// `"2024-01-01"` (or any string starting with a 4-digit year) → 2024.
fn start_year(s: &str) -> Option<u64> {
    let y = s.get(..4)?.parse::<u64>().ok()?;
    (1980..=2100).contains(&y).then_some(y)
}

/// Per-topic-family win stats over the project corpus. Only projects whose
/// `topic` yields a Horizon lineage family participate (non-Horizon topics have
/// no family — see [`app_eu_sedia::topic_lineage`]). Contribution stats are
/// computed over the projects whose contribution parsed; when NONE parsed the
/// totals are `Null`, never a fabricated zero, and `contribution_known` says
/// how many the money numbers actually rest on. Participant leaderboard counts
/// coordinator + participants once per project, bounded to the top 10
/// (count-desc, then name for determinism).
fn aggregate_topic_stats(projects: &[&Value]) -> Vec<(String, Value)> {
    struct Family {
        count: u64,
        known: Vec<f64>,
        orgs: HashMap<String, u64>,
        years: Vec<u64>,
    }
    let mut families: HashMap<String, Family> = HashMap::new();

    for p in projects {
        let Some(topic) = p.get("topic").and_then(Value::as_str) else {
            continue;
        };
        let Some(family) = topic_lineage(topic) else {
            continue;
        };
        let entry = families.entry(family).or_insert_with(|| Family {
            count: 0,
            known: Vec::new(),
            orgs: HashMap::new(),
            years: Vec::new(),
        });
        entry.count += 1;
        if let Some(c) = p.get("ec_contribution").and_then(Value::as_f64) {
            entry.known.push(c);
        }
        if let Some(y) = p.get("start_year").and_then(Value::as_u64) {
            entry.years.push(y);
        }
        let mut seen: Vec<String> = Vec::new();
        if let Some(coord) = p.get("coordinator").and_then(Value::as_str) {
            seen.push(coord.to_string());
        }
        if let Some(parts) = p.get("participants").and_then(Value::as_array) {
            for org in parts.iter().filter_map(Value::as_str) {
                if !seen.iter().any(|s| s == org) {
                    seen.push(org.to_string());
                }
            }
        }
        for org in seen {
            *entry.orgs.entry(org).or_insert(0) += 1;
        }
    }

    let mut out: Vec<(String, Value)> = families
        .into_iter()
        .map(|(family, f)| {
            let known = f.known.len() as u64;
            let (total, mean) = if known == 0 {
                (Value::Null, Value::Null)
            } else {
                let sum: f64 = f.known.iter().sum();
                (json!(sum), json!(sum / known as f64))
            };
            let mut orgs: Vec<(String, u64)> = f.orgs.into_iter().collect();
            orgs.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            orgs.truncate(TOP_PARTICIPANTS);
            let top: Vec<Value> = orgs
                .into_iter()
                .map(|(org, n)| json!({ "org": org, "projects": n }))
                .collect();
            let stats = json!({
                "family": family,
                "project_count": f.count,
                "contribution_known": known,
                "total_ec_contribution": total,
                "mean_ec_contribution": mean,
                "top_participants": top,
                "first_start_year": f.years.iter().min(),
                "last_start_year": f.years.iter().max(),
            });
            (family, stats)
        })
        .collect();
    // Deterministic output order (map iteration is not).
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_hits_accepts_payload_hits_envelope() {
        let resp = json!({ "payload": { "total": 2, "hits": [
            { "rcn": "1" }, { "rcn": "2" }
        ] } });
        let (total, hits) = extract_hits(&resp).expect("envelope");
        assert_eq!(total, 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["rcn"], "1");
    }

    #[test]
    fn extract_hits_accepts_results_with_nested_project_wrapper() {
        let resp = json!({ "payload": { "totalHits": 1, "results": [
            { "project": { "rcn": 101, "acronym": "AI4EU" } }
        ] } });
        let (total, hits) = extract_hits(&resp).expect("envelope");
        assert_eq!(total, 1);
        assert_eq!(hits[0]["acronym"], "AI4EU");
    }

    #[test]
    fn extract_hits_refuses_unknown_envelopes() {
        // Drifted envelope ⇒ None ⇒ the run errors loudly, never an empty success.
        assert!(extract_hits(&json!({ "data": { "items": [] } })).is_none());
        assert!(extract_hits(&json!({ "payload": { "total": 5 } })).is_none());
        assert!(extract_hits(&json!({ "payload": { "hits": [] } })).is_none());
    }

    #[test]
    fn normalize_keys_by_rcn_with_id_fallback_and_refuses_unkeyed() {
        let (key, rec) = normalize_project(&json!({
            "rcn": 12345, "id": "101070522", "acronym": "X",
            "topics": "HORIZON-CL4-2022-DATA-01;secondary",
            "ecMaxContribution": "4200000,50",
            "coordinator": "FRAUNHOFER",
            "participants": ["A", "B", ""],
            "startDate": "2023-01-01",
        }))
        .expect("keyed");
        assert_eq!(key, "12345");
        assert_eq!(rec["topic"], "HORIZON-CL4-2022-DATA-01");
        assert_eq!(rec["ec_contribution"], 4_200_000.5);
        assert_eq!(rec["coordinator"], "FRAUNHOFER");
        assert_eq!(rec["participants"], json!(["A", "B"]));
        assert_eq!(rec["start_year"], 2023);

        let (key2, _) = normalize_project(&json!({ "id": "999" })).expect("id fallback");
        assert_eq!(key2, "999");
        assert!(normalize_project(&json!({ "acronym": "NOPE" })).is_none());
    }

    #[test]
    fn parse_amount_is_honest_about_garbage() {
        assert_eq!(parse_amount(&json!(1500.5)), Some(1500.5));
        assert_eq!(parse_amount(&json!("1500.5")), Some(1500.5));
        // EU comma-decimal convention.
        assert_eq!(parse_amount(&json!("1234567,89")), Some(1_234_567.89));
        // Dot-decimal with thousands commas.
        assert_eq!(parse_amount(&json!("1,234,567.89")), Some(1_234_567.89));
        // Garbage/empty/non-scalar ⇒ None, never 0.
        assert_eq!(parse_amount(&json!("n/a")), None);
        assert_eq!(parse_amount(&json!("")), None);
        assert_eq!(parse_amount(&json!(["1"])), None);
    }

    fn proj(topic: &str, contribution: Option<f64>, coord: &str, parts: &[&str]) -> Value {
        json!({
            "topic": topic,
            "ec_contribution": contribution,
            "coordinator": coord,
            "participants": parts,
            "start_year": 2023,
        })
    }

    #[test]
    fn aggregate_groups_years_into_one_family_and_averages_known_only() {
        let a = proj("HORIZON-CL4-2022-DATA-01", Some(4_000_000.0), "FHG", &["TNO"]);
        let b = proj("HORIZON-CL4-2024-DATA-01", Some(2_000_000.0), "TNO", &["FHG"]);
        let c = proj("HORIZON-CL4-2024-DATA-01", None, "VTT", &[]);
        let d = proj("ERASMUS-EDU-2024-X", Some(1.0), "NOPE", &[]); // no family
        let refs: Vec<&Value> = vec![&a, &b, &c, &d];
        let stats = aggregate_topic_stats(&refs);
        assert_eq!(stats.len(), 1);
        let (family, s) = &stats[0];
        assert_eq!(family, "HORIZON-CL4-DATA-01");
        assert_eq!(s["project_count"], 3);
        // Mean over the 2 parseable contributions only — the third is absent,
        // not a zero dragging the mean down.
        assert_eq!(s["contribution_known"], 2);
        assert_eq!(s["total_ec_contribution"], 6_000_000.0);
        assert_eq!(s["mean_ec_contribution"], 3_000_000.0);
        // FHG and TNO each touched 2 projects (coordinator or participant).
        assert_eq!(s["top_participants"][0]["projects"], 2);
    }

    #[test]
    fn aggregate_with_no_known_contributions_reports_null_not_zero() {
        let a = proj("HORIZON-EIC-2025-PATHFINDEROPEN-01", None, "ETH", &[]);
        let refs: Vec<&Value> = vec![&a];
        let stats = aggregate_topic_stats(&refs);
        let s = &stats[0].1;
        assert_eq!(s["project_count"], 1);
        assert!(s["total_ec_contribution"].is_null());
        assert!(s["mean_ec_contribution"].is_null());
    }

    #[test]
    fn aggregate_bounds_and_orders_the_participant_leaderboard() {
        let orgs: Vec<String> = (0..15).map(|i| format!("ORG-{i:02}")).collect();
        let org_refs: Vec<&str> = orgs.iter().map(String::as_str).collect();
        let a = proj("HORIZON-CL5-2024-D3-01", Some(1.0), "ZZZ-COORD", &org_refs);
        let b = proj("HORIZON-CL5-2022-D3-01", Some(1.0), "ORG-03", &[]);
        let refs: Vec<&Value> = vec![&a, &b];
        let stats = aggregate_topic_stats(&refs);
        let top = stats[0].1["top_participants"].as_array().unwrap();
        assert_eq!(top.len(), 10, "leaderboard must stay bounded");
        // ORG-03 appears in both projects → count 2, ranked first.
        assert_eq!(top[0]["org"], "ORG-03");
        assert_eq!(top[0]["projects"], 2);
        // Ties broken by name for determinism.
        assert_eq!(top[1]["org"], "ORG-00");
    }
}
