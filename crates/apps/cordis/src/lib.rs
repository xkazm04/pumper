//! CORDIS funded Horizon projects — the *outcomes* side of the EU funding
//! corpus (eu-sedia carries the open-calls side). Two-stage: the Search API
//! enumerates project ids, then a per-project detail fetch supplies the real
//! data (money, orgs, topic identifiers). Records keyed by RCN into
//! `projects`; per-topic-family rollups into `topic_stats`, which eu-sedia
//! joins onto open topics as a `history` block. `http` engine.
//!
//! Data type: AWARDED HISTORY. Access: key-free.
//!
//! Contract (VERIFIED 2026-07-30 with live requests):
//!
//! Stage 1 — search listing (id enumeration ONLY):
//!   GET https://cordis.europa.eu/api/search/results
//!       ?q=<CORDIS query>&format=json&p=<page,1-based>&num=<size≤100>
//!   Working query grammar: `contenttype='project' AND programme/code='HORIZON'`
//!   (the older `/project/frameworkProgramme='HORIZON'` grammar silently
//!   returns total:0 — do NOT regress to it).
//!   Envelope: `{"status":true,"payload":{"total":N,"page":p,"nItems":n,
//!   "searchAfter":"<base64 cursor>","results":[...]}}`. Each result is a
//!   LISTING stub: reference, id, acronym, programme[], startDate, endDate,
//!   teaser, rcn, contentType, title — NO money, NO orgs, NO topic identifier,
//!   and listing dates carry template junk (`"1 {{month_06}} 2022"`), so we
//!   parse NOTHING from the listing except the project id. `searchAfter` is a
//!   deep-paging cursor for the future; p/num paging works at the moderate
//!   depths our resume cursor uses, so we keep the stored page cursor.
//!
//! Stage 2 — per-project detail (the real data):
//!   GET https://cordis.europa.eu/project/id/{id}?format=json
//!   Flat object: rcn, id, acronym, title, objective, totalCost,
//!   ecMaxContribution (plain numeric strings), startDate/endDate (clean ISO
//!   here), status, plus `relations.associations` — a numeric-keyed map of
//!   typed entries distinguished by `attributes.type`:
//!     - `relatedMasterCall` / `relatedSubCall`:
//!       `{identifier: "HORIZON-EURATOM-2021-NRT-01", title, rcn}` — THE
//!       topic/call identifiers used for lineage.
//!     - `participant` / coordinator-typed entries (detect coordinator by
//!       `attributes.type` containing "coordinator"; org entries in general
//!       carry `legalName`): `{legalName, shortName, attributes:
//!       {ecContribution, type, order, sme, terminated}}`.
//!     - Other entries (programme, result, article, …): ignored.
//!
//! Drift rules stay loud: a positive total with zero parseable listing hits,
//! or a run where every attempted detail fetch failed to normalize, FAILS the
//! run instead of reporting a successful empty sweep.
//!
//! Coverage: `max_projects` caps the detail fetches per run (each is a
//! governor-paced request). A resume cursor persisted in the `state` dataset
//! advances the page window across scheduled runs and wraps at the end, so the
//! ~23k-project Horizon corpus is covered over successive weekly runs without
//! hammering the API in one go.

use std::collections::{HashMap, HashSet};

use app_eu_sedia::topic_lineage;
use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, ChangeKind, CostClass, Error, HttpRequest, ManifestExample,
    Provenance, Result, ScrapeApp,
};
use serde_json::{json, Value};

pub struct Cordis;

const SEARCH_URL: &str = "https://cordis.europa.eu/api/search/results";
const DETAIL_URL: &str = "https://cordis.europa.eu/project/id";
/// Verified query grammar (2026-07-30). `programme/code`, NOT
/// `/project/frameworkProgramme` — the latter returns total:0.
const DEFAULT_QUERY: &str = "contenttype='project' AND programme/code='HORIZON'";
/// Upper bound when aggregating the stored corpus (Horizon Europe is ~23k
/// projects; this leaves an order of magnitude of headroom).
const AGGREGATE_LIMIT: i64 = 200_000;
/// Bounded participant-org leaderboard per family.
const TOP_PARTICIPANTS: usize = 10;
/// Bounded per-project org list — mega-consortia exist.
const MAX_ORGS: usize = 50;

#[async_trait]
impl ScrapeApp for Cordis {
    fn name(&self) -> &'static str {
        "cordis"
    }

    fn description(&self) -> &'static str {
        "CORDIS funded Horizon projects (two-stage: Search API listing \
         enumerates ids, per-project detail fetch supplies money/orgs/topics; \
         key-free). Awarded-history corpus keyed by RCN into `projects`, plus \
         per-topic-family win stats in `topic_stats` (joined by eu-sedia onto \
         open topics). \
         Params: {\"query\": CORDIS query override, \"pageSize\": 1-100, \
         \"maxProjects\": 1-5000 detail fetches per run (default 500), \
         \"startPage\": override the persisted resume cursor}"
    }

    /// Weekly, Mondays 07:00 UTC — outcomes data moves slowly, and the resume
    /// cursor sweeps the whole corpus across runs.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 7 * * 1")
    }

    fn default_params(&self) -> Value {
        // Conservative per-run cap: 500 detail fetches (~5 listing pages).
        // Full-corpus coverage comes from the resume cursor across scheduled
        // runs, not one big sweep.
        json!({ "pageSize": 100, "maxProjects": 500 })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "CORDIS query override. The VERIFIED working grammar is `contenttype='project' AND programme/code='HORIZON'`; the older `/project/frameworkProgramme=` grammar silently returns total:0."
                    },
                    "pageSize": {
                        "type": "integer", "minimum": 1, "maximum": 100,
                        "description": "Listing page size (`num`) for the id-enumeration stage."
                    },
                    "maxProjects": {
                        "type": "integer", "minimum": 1, "maximum": 5000,
                        "description": "Cap on per-project DETAIL fetches this run — the expensive, governor-paced stage. Full-corpus coverage comes from the resume cursor across scheduled runs, not one big sweep."
                    },
                    "startPage": {
                        "type": "integer", "minimum": 1,
                        "description": "Overrides the persisted resume cursor in cordis/state (1-based listing page)."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Weekly cursor advance: next 500 Horizon projects from where the last run stopped (the scheduled default)",
                    params: json!({ "pageSize": 100, "maxProjects": 500 }),
                },
                ManifestExample {
                    description: "Re-sweep from the top of the corpus, ignoring the persisted cursor",
                    params: json!({ "pageSize": 100, "maxProjects": 1000, "startPage": 1 }),
                },
            ],
            output_shape: Some(
                "{source, query, totalResults, startPage, pages, ids_enumerated, \
                 skipped_unlisted, detail_failed, skipped_unkeyed, fetched, resumed_from, new, \
                 changed, unchanged, corpus, families, stats_new, stats_changed, \
                 cursor_next_page, corpus_swept} — RCN-keyed projects in `projects`, \
                 per-topic-family rollups in `topic_stats` (read by eu-sedia)",
            ),
            cost_class: CostClass::Free,
        }
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
            Some(rec) => rec
                .data
                .get("next_page")
                .and_then(Value::as_u64)
                .unwrap_or(1),
            None => 1,
        };
        let start_page = ctx
            .params
            .get("startPage")
            .and_then(Value::as_u64)
            .unwrap_or(cursor_start)
            .max(1);

        // ── Stage 1: listing sweep — enumerate project ids only. ──
        let mut ids: Vec<String> = Vec::new();
        let mut unlisted: u64 = 0; // listing hits without an id
        let mut total: u64 = 0;
        let mut page = start_page;
        let mut pages_fetched: u64 = 0;
        let mut exhausted = false;

        while (ids.len() as u64) < max_projects {
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

            // Envelope per the verified contract; a positive total with zero
            // parseable hits means drift — refuse to report an empty success.
            let (page_total, hits) = extract_hits(&parsed).ok_or_else(|| {
                Error::App(
                    "cordis: could not locate payload.total+results in the search \
                     envelope — the verified contract drifted (see crate doc \
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
                match scalar_string(hit.get("id")).filter(|s| !s.is_empty()) {
                    Some(id) => ids.push(id),
                    None => unlisted += 1,
                }
            }
            pages_fetched += 1;
            page += 1;
            if got < page_size || ((page - 1) * page_size) >= total {
                exhausted = true;
                break;
            }
        }
        ids.truncate(max_projects as usize);

        // ── Stage 2: per-project detail fetch (the real data). ──
        //
        // Durable execution (M23): this stage is up to `maxProjects` separate
        // governor-paced requests — by far the longest thing this app does, and
        // the reason a reap used to cost the whole run. Each project is written
        // the moment it normalizes, and the ids written so far are checkpointed,
        // so a re-claim skips them instead of re-fetching. The listing stage is
        // deliberately NOT checkpointed: it is ~5 cheap calls and re-running it
        // is what regenerates `ids` for the resume to filter.
        let mut done: HashSet<String> = restored_done(ctx.restore(), start_page);
        let resumed_from = ids.iter().filter(|id| done.contains(*id)).count() as u64;
        let mut new_keys: u64 = 0;
        let mut changed_keys: u64 = 0;
        let mut unchanged_keys: u64 = 0;
        let mut normalized: u64 = 0;
        let mut attempted: u64 = 0;
        let mut detail_failed: u64 = 0;
        let mut skipped: u64 = 0; // detail parsed but unkeyable
        let mut first_detail = resumed_from == 0;
        let pending: Vec<String> = ids
            .iter()
            .filter(|id| !done.contains(*id))
            .cloned()
            .collect();
        for id in &pending {
            attempted += 1;
            let url = format!("{DETAIL_URL}/{id}?format=json");
            let resp = ctx.engines.http.fetch(HttpRequest::get(&url)).await?;
            if !resp.is_success() {
                // NOT marked done — a transient failure must be retried on the
                // next attempt, not silently skipped forever.
                detail_failed += 1;
                continue;
            }
            let Ok(detail) = serde_json::from_str::<Value>(&resp.body) else {
                detail_failed += 1;
                continue;
            };
            if first_detail {
                first_detail = false;
                ctx.save_artifact("detail1.json", &serde_json::to_vec_pretty(&detail)?)
                    .await?;
            }
            match normalize_detail(&detail) {
                Some((key, record)) => {
                    // Provenance (M12): this record came from exactly one URL and
                    // we know it — the per-record case the batch path cannot
                    // express. `rules_hash`/`artifact_sha` stay Null: extraction
                    // is Rust code, not a registered RuleSet, and only the first
                    // detail body is archived.
                    let kind = ctx
                        .upsert_with_provenance(
                            "projects",
                            &key,
                            &record,
                            Provenance {
                                source_url: Some(url.clone()),
                                ..Provenance::default()
                            },
                        )
                        .await?;
                    match kind {
                        ChangeKind::New => new_keys += 1,
                        ChangeKind::Changed => changed_keys += 1,
                        ChangeKind::Unchanged => unchanged_keys += 1,
                    }
                    normalized += 1;
                }
                None => skipped += 1,
            }
            done.insert(id.clone());
            ctx.checkpoint(stage2_state(start_page, &done)).await;
        }
        // Drift stays loud: attempting detail fetches and normalizing NONE of
        // them is a contract break, never a clean empty sweep. Gated on what
        // this attempt actually tried — a fully-resumed run legitimately
        // normalizes nothing new.
        if attempted > 0 && normalized == 0 {
            return Err(Error::App(format!(
                "cordis: {attempted} project detail fetches attempted but 0 records \
                 normalized ({detail_failed} fetch/parse failures, {skipped} unkeyable) \
                 — the detail contract drifted (detail1.json artifact holds a raw body)"
            )));
        }
        ctx.checkpoint_now(stage2_state(start_page, &done)).await;

        // Re-aggregate topic families over the WHOLE stored corpus (not just
        // this run's window) so stats stay consistent while the cursor sweeps.
        // Change detection makes untouched families free.
        let corpus = ctx
            .datasets
            .list(&ctx.app, "projects", AGGREGATE_LIMIT)
            .await?;
        let corpus_values: Vec<&Value> = corpus.iter().map(|r| &r.data).collect();
        let stats = aggregate_topic_stats(&corpus_values);
        let families = stats.len();
        // A rollup over thousands of stored projects from as many URLs: only job
        // lineage is knowable, so `upsert_many`'s automatic job_id stamp is the
        // whole honest provenance here. Naming one source_url would be a lie.
        let stats_summary = ctx.upsert_many("topic_stats", &stats).await?;

        // Persist the resume cursor: wrap to page 1 once the corpus is covered.
        let next_page = if exhausted { 1 } else { page };
        ctx.upsert("state", "cursor", &json!({ "next_page": next_page }))
            .await?;

        Ok(json!({
            "source": "cordis.europa.eu (search listing + project detail)",
            "query": query,
            "totalResults": total,
            "startPage": start_page,
            "pages": pages_fetched,
            "ids_enumerated": ids.len(),
            "skipped_unlisted": unlisted,
            "detail_failed": detail_failed,
            "skipped_unkeyed": skipped,
            "fetched": normalized,
            "resumed_from": resumed_from,
            "new": new_keys,
            "changed": changed_keys,
            "unchanged": unchanged_keys,
            "corpus": corpus.len(),
            "families": families,
            "stats_new": stats_summary.new.len(),
            "stats_changed": stats_summary.changed.len(),
            "cursor_next_page": next_page,
            "corpus_swept": exhausted,
        }))
    }
}

/// Checkpoint schema version — a snapshot in any other shape means "start
/// fresh", never a failed run (the sink is advisory by contract).
const STAGE2_STATE_VERSION: u64 = 1;

/// The stage-2 checkpoint payload: which listing window this attempt is working
/// (so a run that resumes under a *different* `startPage` cannot inherit a
/// stale done-set) and the project ids already written to `projects`. Ids only —
/// never record bodies, so the snapshot stays small at 5000 projects.
fn stage2_state(start_page: u64, done: &HashSet<String>) -> Value {
    let mut ids: Vec<&String> = done.iter().collect();
    ids.sort(); // a HashSet has no order; a stable snapshot diffs cleanly
    json!({
        "v": STAGE2_STATE_VERSION,
        "stage": "details",
        "start_page": start_page,
        "done": ids,
    })
}

/// The already-written project ids from a prior attempt of this job, or an
/// empty set. Tolerates any stored shape; a snapshot taken against a different
/// `start_page` is discarded rather than misapplied to a different window.
fn restored_done(state: Option<&Value>, start_page: u64) -> HashSet<String> {
    let empty = HashSet::new();
    let Some(state) = state else { return empty };
    if state.get("v").and_then(Value::as_u64) != Some(STAGE2_STATE_VERSION)
        || state.get("stage").and_then(Value::as_str) != Some("details")
        || state.get("start_page").and_then(Value::as_u64) != Some(start_page)
    {
        return empty;
    }
    state
        .get("done")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or(empty)
}

/// Locates `(total, results)` in the verified search envelope:
/// `payload.{total,results}` (tolerating the same pair at the top level).
/// Returns `None` when the shape does not match — the caller turns that into a
/// loud contract-drift error, never an empty run.
fn extract_hits(parsed: &Value) -> Option<(u64, Vec<Value>)> {
    let envelope = parsed.get("payload").unwrap_or(parsed);
    let total = envelope.get("total").and_then(Value::as_u64)?;
    let arr = envelope.get("results").and_then(Value::as_array)?;
    Some((total, arr.to_vec()))
}

/// Normalizes one project DETAIL object (from `/project/id/{id}?format=json`)
/// to a stable record keyed by RCN (falling back to the grant-agreement id).
/// `None` when neither key exists — an unkeyable detail is counted, not
/// silently invented. Topic identifiers, coordinator and participants come
/// from `relations.associations` (see crate doc-header).
fn normalize_detail(detail: &Value) -> Option<(String, Value)> {
    // Tolerate a `payload` wrapper defensively; verified shape is flat.
    let d = detail.get("payload").unwrap_or(detail);
    let key = scalar_string(d.get("rcn"))
        .or_else(|| scalar_string(d.get("id")))
        .filter(|s| !s.is_empty())?;

    let assoc = extract_associations(d);
    // The sub-call is the concrete topic; the master call is the umbrella.
    // `topic` prefers the sub-call — that is the identifier grammar lineage
    // keys on (e.g. HORIZON-EURATOM-2021-NRT-01).
    let topic = assoc.sub_call.clone().or_else(|| assoc.master_call.clone());

    let record = json!({
        "rcn": scalar_string(d.get("rcn")),
        "project_id": scalar_string(d.get("id")),
        "acronym": d.get("acronym").and_then(Value::as_str),
        "title": d.get("title").and_then(Value::as_str),
        "topic": topic,
        "master_call": assoc.master_call,
        "sub_call": assoc.sub_call,
        // Honest money: unparseable ⇒ Null, never 0.
        "ec_contribution": d.get("ecMaxContribution").and_then(parse_amount),
        "total_cost": d.get("totalCost").and_then(parse_amount),
        "coordinator": assoc.coordinator,
        "participants": assoc.participants,
        "start_year": d
            .get("startDate")
            .and_then(Value::as_str)
            .and_then(start_year),
        "status": d.get("status").and_then(Value::as_str),
    });
    Some((key, record))
}

/// What we pull out of `relations.associations`.
struct Associations {
    master_call: Option<String>,
    sub_call: Option<String>,
    coordinator: Option<String>,
    /// `[{name, ec_contribution, role}]`, coordinator included, order-sorted,
    /// bounded to [`MAX_ORGS`].
    participants: Vec<Value>,
}

/// Walks the numeric-keyed `relations.associations` map, classifying entries
/// by `attributes.type`: relatedMasterCall/relatedSubCall carry the topic
/// identifiers; org entries (have `legalName`) become participants, with the
/// coordinator detected by a type containing "coordinator". Everything else
/// (programme, result, article, …) is ignored.
fn extract_associations(detail: &Value) -> Associations {
    let mut out = Associations {
        master_call: None,
        sub_call: None,
        coordinator: None,
        participants: Vec::new(),
    };
    let Some(map) = detail
        .get("relations")
        .and_then(|r| r.get("associations"))
        .and_then(Value::as_object)
    else {
        return out;
    };
    // Numeric-keyed map — sort keys numerically for deterministic order.
    let mut entries: Vec<(&String, &Value)> = map.iter().collect();
    entries.sort_by_key(|(k, _)| k.parse::<u64>().unwrap_or(u64::MAX));

    let mut orgs: Vec<(u64, Value)> = Vec::new();
    for (i, (_, entry)) in entries.into_iter().enumerate() {
        let atype = entry
            .get("attributes")
            .and_then(|a| a.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("");
        match atype {
            "relatedMasterCall" => {
                if out.master_call.is_none() {
                    out.master_call = entry
                        .get("identifier")
                        .and_then(Value::as_str)
                        .map(String::from);
                }
            }
            "relatedSubCall" => {
                if out.sub_call.is_none() {
                    out.sub_call = entry
                        .get("identifier")
                        .and_then(Value::as_str)
                        .map(String::from);
                }
            }
            _ => {
                // Org entries carry legalName (participant, coordinator, …).
                let Some(name) = entry
                    .get("legalName")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    continue;
                };
                let is_coordinator = atype.to_ascii_lowercase().contains("coordinator");
                let role = if is_coordinator {
                    "coordinator"
                } else {
                    "participant"
                };
                if is_coordinator && out.coordinator.is_none() {
                    out.coordinator = Some(name.to_string());
                }
                let order = entry
                    .get("attributes")
                    .and_then(|a| a.get("order"))
                    .and_then(|o| match o {
                        Value::Number(n) => n.as_u64(),
                        Value::String(s) => s.parse::<u64>().ok(),
                        _ => None,
                    })
                    .unwrap_or(i as u64 + 1_000_000);
                let ec = entry
                    .get("attributes")
                    .and_then(|a| a.get("ecContribution"))
                    .and_then(parse_amount);
                orgs.push((
                    order,
                    json!({ "name": name, "ec_contribution": ec, "role": role }),
                ));
            }
        }
    }
    orgs.sort_by_key(|(order, _)| *order);
    orgs.truncate(MAX_ORGS);
    out.participants = orgs.into_iter().map(|(_, v)| v).collect();
    out
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

/// Parses an EU money amount: JSON numbers pass through; strings tolerate the
/// comma-decimal convention (`"1234567,89"`) and thousands separators (the
/// verified detail endpoint ships plain dot-decimal numeric strings, but the
/// tolerance costs nothing). Anything ambiguous or non-numeric ⇒ `None` (a
/// fabricated €0 would poison the means).
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

/// `"2024-01-01"` (or any string starting with a 4-digit year) → 2024. Detail
/// dates are clean ISO; listing dates are template junk and are never parsed.
fn start_year(s: &str) -> Option<u64> {
    let y = s.get(..4)?.parse::<u64>().ok()?;
    (1980..=2100).contains(&y).then_some(y)
}

/// Per-topic-family win stats over the project corpus. Only projects whose
/// `topic` (sub-call, falling back to master-call identifier) yields a Horizon
/// lineage family participate (non-Horizon topics have no family — see
/// [`app_eu_sedia::topic_lineage`]). Contribution stats are computed over the
/// projects whose contribution parsed; when NONE parsed the totals are `Null`,
/// never a fabricated zero, and `contribution_known` says how many the money
/// numbers actually rest on. Participant leaderboard counts each org (any
/// role, coordinator included) once per project, bounded to the top 10
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
        // Orgs once per project. Participants are `{name, ec_contribution,
        // role}` objects (coordinator included); the flat `coordinator` string
        // is also accepted so pre-rework corpus rows keep counting.
        let mut seen: Vec<String> = Vec::new();
        if let Some(coord) = p.get("coordinator").and_then(Value::as_str) {
            seen.push(coord.to_string());
        }
        if let Some(parts) = p.get("participants").and_then(Value::as_array) {
            for org in parts.iter().filter_map(|x| match x {
                Value::Object(_) => x.get("name").and_then(Value::as_str),
                Value::String(s) => Some(s.as_str()),
                _ => None,
            }) {
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

    // ── Stage 1: verified search envelope ──

    #[test]
    fn extract_hits_accepts_the_verified_payload_results_envelope() {
        // Real shape (2026-07-30): payload.{total,page,nItems,searchAfter,results}.
        let resp = json!({ "status": true, "payload": {
            "total": 23361, "page": 1, "nItems": 2,
            "searchAfter": "WzE3MDAwMDAwMDBd",
            "results": [
                { "reference": "101070522", "id": "101070522", "acronym": "X",
                  "programme": ["HORIZON"], "rcn": "241234",
                  "startDate": "1 {{month_06}} 2022", "contentType": "project",
                  "title": "T" },
                { "id": 101059379, "rcn": 240001, "contentType": "project" }
            ]
        } });
        let (total, hits) = extract_hits(&resp).expect("envelope");
        assert_eq!(total, 23361);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0]["id"], "101070522");
        // Numeric ids must enumerate too.
        assert_eq!(
            scalar_string(hits[1].get("id")).as_deref(),
            Some("101059379")
        );
    }

    #[test]
    fn extract_hits_refuses_drifted_envelopes() {
        // Drifted envelope ⇒ None ⇒ the run errors loudly, never an empty success.
        assert!(extract_hits(&json!({ "data": { "items": [] } })).is_none());
        assert!(extract_hits(&json!({ "payload": { "total": 5 } })).is_none());
        assert!(extract_hits(&json!({ "payload": { "results": [] } })).is_none());
        // The old assumed `hits` key is drift now, not an accepted alias.
        assert!(extract_hits(&json!({ "payload": { "total": 5, "hits": [] } })).is_none());
    }

    // ── Durable execution: stage-2 resume state ──

    #[test]
    fn stage2_state_round_trips_and_skips_written_projects() {
        let done: HashSet<String> = ["101070522".to_string(), "101059379".to_string()]
            .into_iter()
            .collect();
        let snap = stage2_state(7, &done);
        assert_eq!(restored_done(Some(&snap), 7), done);
        // The remaining work is the enumerated ids minus what already landed.
        let ids = vec![
            "101070522".to_string(),
            "101059379".to_string(),
            "101099999".to_string(),
        ];
        let restored = restored_done(Some(&snap), 7);
        let pending: Vec<&String> = ids.iter().filter(|i| !restored.contains(*i)).collect();
        assert_eq!(pending, vec!["101099999"]);
    }

    #[test]
    fn restored_done_discards_snapshots_it_cannot_trust() {
        let done: HashSet<String> = ["a".to_string()].into_iter().collect();
        let snap = stage2_state(7, &done);
        // A different listing window enumerates different ids — the done-set
        // from another window must NOT suppress fetches in this one.
        assert!(restored_done(Some(&snap), 8).is_empty());
        assert!(restored_done(None, 7).is_empty());
        // Foreign / versioned-out shapes start fresh instead of erroring.
        assert!(restored_done(Some(&json!({ "frontier": ["x"] })), 7).is_empty());
        assert!(restored_done(
            Some(&json!({ "v": 99, "stage": "details", "start_page": 7, "done": ["a"] })),
            7
        )
        .is_empty());
        // A well-formed snapshot with no done ids is simply an empty resume.
        assert!(restored_done(
            Some(&json!({ "v": 1, "stage": "details", "start_page": 7 })),
            7
        )
        .is_empty());
    }

    // ── Stage 2: verified detail shape ──

    /// A detail fixture mirroring the live 2026-07-30 ground truth.
    fn detail_fixture() -> Value {
        json!({
            "rcn": 241234,
            "id": "101070522",
            "acronym": "SAFEG2",
            "title": "Safe nuclear thing",
            "objective": "…",
            "totalCost": "4123456.50",
            "ecMaxContribution": "3999999.75",
            "startDate": "2022-06-01",
            "endDate": "2026-05-31",
            "status": "SIGNED",
            "relations": { "associations": {
                "0": { "attributes": { "type": "relatedMasterCall" },
                       "identifier": "HORIZON-EURATOM-2021-NRT",
                       "title": "Euratom NRT call", "rcn": 900001 },
                "1": { "attributes": { "type": "relatedSubCall" },
                       "identifier": "HORIZON-EURATOM-2021-NRT-01",
                       "title": "Euratom NRT topic", "rcn": 900002 },
                "2": { "legalName": "FRAUNHOFER GESELLSCHAFT",
                       "shortName": "FHG",
                       "attributes": { "type": "coordinator",
                           "ecContribution": "1500000", "order": 1,
                           "sme": false, "terminated": false } },
                "3": { "legalName": "TNO", "shortName": "TNO",
                       "attributes": { "type": "participant",
                           "ecContribution": "1200000.25", "order": 2,
                           "sme": false, "terminated": false } },
                "10": { "legalName": "VTT", "shortName": "VTT",
                       "attributes": { "type": "participant",
                           "ecContribution": "", "order": 3,
                           "sme": true, "terminated": false } },
                "4": { "attributes": { "type": "programme" },
                       "code": "HORIZON.1.1", "frameworkProgramme": "HORIZON" },
                "5": { "attributes": { "type": "result" }, "title": "deliverable" }
            } }
        })
    }

    #[test]
    fn normalize_detail_maps_the_verified_shape() {
        let (key, rec) = normalize_detail(&detail_fixture()).expect("keyed");
        assert_eq!(key, "241234"); // RCN-keyed
        assert_eq!(rec["project_id"], "101070522");
        assert_eq!(rec["acronym"], "SAFEG2");
        // Topic = sub-call identifier (the lineage-grade grammar).
        assert_eq!(rec["topic"], "HORIZON-EURATOM-2021-NRT-01");
        assert_eq!(rec["master_call"], "HORIZON-EURATOM-2021-NRT");
        assert_eq!(rec["sub_call"], "HORIZON-EURATOM-2021-NRT-01");
        assert_eq!(rec["ec_contribution"], 3_999_999.75);
        assert_eq!(rec["total_cost"], 4_123_456.5);
        assert_eq!(rec["start_year"], 2022); // clean ISO detail date
        assert_eq!(rec["status"], "SIGNED");
        assert_eq!(rec["coordinator"], "FRAUNHOFER GESELLSCHAFT");
        // Participants: order-sorted org objects, coordinator included,
        // per-org contribution honest-Null when empty.
        let parts = rec["participants"].as_array().unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0]["name"], "FRAUNHOFER GESELLSCHAFT");
        assert_eq!(parts[0]["role"], "coordinator");
        assert_eq!(parts[0]["ec_contribution"], 1_500_000.0);
        assert_eq!(parts[1]["name"], "TNO");
        assert_eq!(parts[1]["role"], "participant");
        assert_eq!(parts[1]["ec_contribution"], 1_200_000.25);
        assert_eq!(parts[2]["name"], "VTT");
        assert!(parts[2]["ec_contribution"].is_null());
    }

    #[test]
    fn normalize_detail_id_fallback_and_unkeyed_refusal() {
        let (key, _) = normalize_detail(&json!({ "id": "999" })).expect("id fallback");
        assert_eq!(key, "999");
        assert!(normalize_detail(&json!({ "acronym": "NOPE" })).is_none());
    }

    #[test]
    fn normalize_detail_without_associations_yields_null_topic_and_orgs() {
        let (_, rec) = normalize_detail(&json!({
            "rcn": 1, "id": "2", "ecMaxContribution": "100.5",
            "startDate": "2023-01-01"
        }))
        .expect("keyed");
        assert!(rec["topic"].is_null());
        assert!(rec["coordinator"].is_null());
        assert_eq!(rec["participants"], json!([]));
        assert_eq!(rec["ec_contribution"], 100.5);
    }

    #[test]
    fn parse_amount_is_honest_about_garbage() {
        assert_eq!(parse_amount(&json!(1500.5)), Some(1500.5));
        // Verified detail shape: plain dot-decimal numeric strings.
        assert_eq!(parse_amount(&json!("3999999.75")), Some(3_999_999.75));
        // EU comma-decimal convention (tolerated).
        assert_eq!(parse_amount(&json!("1234567,89")), Some(1_234_567.89));
        // Dot-decimal with thousands commas.
        assert_eq!(parse_amount(&json!("1,234,567.89")), Some(1_234_567.89));
        // Garbage/empty/non-scalar ⇒ None, never 0.
        assert_eq!(parse_amount(&json!("n/a")), None);
        assert_eq!(parse_amount(&json!("")), None);
        assert_eq!(parse_amount(&json!(["1"])), None);
    }

    // ── Lineage against the real identifier grammar ──

    #[test]
    fn topic_lineage_matches_the_verified_identifier_grammar() {
        // Real identifier seen live 2026-07-30.
        assert_eq!(
            topic_lineage("HORIZON-EURATOM-2021-NRT-01").as_deref(),
            Some("HORIZON-EURATOM-NRT-01")
        );
        // Successor topic collapses onto the same family key.
        assert_eq!(
            topic_lineage("HORIZON-EURATOM-2023-NRT-01").as_deref(),
            Some("HORIZON-EURATOM-NRT-01")
        );
    }

    // ── Aggregation over normalized records ──

    fn proj(topic: &str, contribution: Option<f64>, coord: &str, parts: &[&str]) -> Value {
        let participants: Vec<Value> = std::iter::once(coord)
            .chain(parts.iter().copied())
            .enumerate()
            .map(|(i, name)| {
                json!({
                    "name": name,
                    "ec_contribution": Value::Null,
                    "role": if i == 0 { "coordinator" } else { "participant" },
                })
            })
            .collect();
        json!({
            "topic": topic,
            "ec_contribution": contribution,
            "coordinator": coord,
            "participants": participants,
            "start_year": 2023,
        })
    }

    #[test]
    fn aggregate_groups_years_into_one_family_and_averages_known_only() {
        let a = proj(
            "HORIZON-CL4-2022-DATA-01",
            Some(4_000_000.0),
            "FHG",
            &["TNO"],
        );
        let b = proj(
            "HORIZON-CL4-2024-DATA-01",
            Some(2_000_000.0),
            "TNO",
            &["FHG"],
        );
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
        // FHG and TNO each touched 2 projects (coordinator or participant),
        // each counted ONCE per project despite appearing in both the
        // `coordinator` field and the participants list.
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

    #[test]
    fn aggregate_still_counts_legacy_string_participants() {
        // Pre-rework corpus rows stored participants as plain strings.
        let legacy = json!({
            "topic": "HORIZON-CL4-2022-DATA-01",
            "ec_contribution": 1.0,
            "coordinator": "FHG",
            "participants": ["TNO"],
            "start_year": 2022,
        });
        let refs: Vec<&Value> = vec![&legacy];
        let stats = aggregate_topic_stats(&refs);
        let top = stats[0].1["top_participants"].as_array().unwrap();
        assert_eq!(top.len(), 2);
    }
}
