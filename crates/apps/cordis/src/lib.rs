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
//! advances the window across scheduled runs and wraps at the end, so the
//! ~23k-project Horizon corpus is covered over successive weekly runs without
//! hammering the API in one go.
//!
//! **Sweep honesty.** The cursor is an *offset into the listing*, not a page
//! number, and only a walk that provably reached the end wraps it. See
//! [`SweepEnd`]: a page shorter than `pageSize` while the listing's own reported
//! total says more remains is a truncation, not the end of the corpus — treating
//! it as the end used to reset ~46 weeks of accumulated progress to page 1 while
//! reporting `corpus_swept: true`. And because the cursor counts *consumed ids*,
//! a `maxProjects` that is not a multiple of `pageSize` re-visits the truncated
//! tail on the next run instead of stepping over it for a whole corpus cycle.

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
                        "description": "Overrides the persisted resume cursor in cordis/state (1-based listing page; resumes at the top of that page, i.e. offset (page-1)*pageSize)."
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
                "{source, query, totalResults, startPage, start_offset, skip_in_page, pages, \
                 ids_enumerated, skipped_unlisted, detail_failed, skipped_unkeyed, fetched, \
                 resumed_from, new, changed, unchanged, corpus, families, stats_new, \
                 stats_changed, cursor_next_page, cursor_next_offset, sweep, corpus_swept, \
                 warnings[]} — RCN-keyed projects in `projects`, per-topic-family rollups in \
                 `topic_stats` (read by eu-sedia). `sweep` is `complete` (page arithmetic \
                 proved the listing's end — only then does `corpus_swept` hold and the cursor \
                 wrap), `capped` (stopped at maxProjects with corpus left) or `short_page` (a \
                 truncated page while the reported total says more remains: NOT the end, so \
                 the cursor keeps its place and a warning is reported)",
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
        // covered over successive runs. An explicit `startPage` param overrides
        // (resuming at the TOP of that page — a human asking for a page means
        // the page, not some offset inside it).
        let stored_cursor = ctx
            .datasets
            .get(&ctx.app, "state", "cursor")
            .await?
            .map(|r| r.data);
        let start_offset = match ctx.params.get("startPage").and_then(Value::as_u64) {
            Some(p) => (p.max(1) - 1) * page_size,
            None => cursor_offset(stored_cursor.as_ref(), page_size),
        };
        let start_page = start_offset / page_size + 1;
        let skip_in_page = start_offset % page_size;

        // ── Stage 1: listing sweep — enumerate project ids only. ──
        let mut ids: Vec<String> = Vec::new();
        let mut unlisted: u64 = 0; // listing hits without an id
        let mut total: u64;
        let mut page = start_page;
        let mut pages_fetched: u64 = 0;
        // Listing positions this run consumed, counted from `start_offset`. The
        // cursor advances by THIS, not by pages fetched: a `maxProjects` that
        // truncates mid-page must leave the tail for the next run, not step over
        // it (which skipped those projects for a whole ~46-week corpus cycle).
        let mut consumed: u64 = 0;
        let end: SweepEnd;

        loop {
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
                // `SourceDrift`, not `App`: terminal for the job. The envelope
                // shape is a pure parse of a response fetched from params frozen
                // at enqueue, so every attempt loses the contract in the same
                // place.
                Error::SourceDrift(
                    "cordis: could not locate payload.total+results in the search \
                     envelope — the verified contract drifted (see crate doc \
                     header; page1.json artifact holds the raw body)"
                        .to_string(),
                )
            })?;
            total = page_total;
            let got = hits.len() as u64;
            if pages_fetched == 0 && empty_first_page_is_drift(page, page_size, total, got) {
                // `SourceDrift`, not `App`: terminal for the job.
                return Err(Error::SourceDrift(format!(
                    "cordis: API reported {total} results but listing page {page} parsed 0 \
                     hits — likely an upstream schema change (page1.json artifact holds the \
                     raw body). The resume cursor is left untouched."
                )));
            }

            // The first page of a resumed run re-fetches the page the cursor sits
            // inside; the ids before the cursor were already consumed last run.
            let skip_here = if pages_fetched == 0 {
                skip_in_page.min(got)
            } else {
                0
            };
            let mut taken: u64 = 0;
            for hit in hits.iter().skip(skip_here as usize) {
                if ids.len() as u64 >= max_projects {
                    break;
                }
                taken += 1;
                match scalar_string(hit.get("id")).filter(|s| !s.is_empty()) {
                    Some(id) => ids.push(id),
                    None => unlisted += 1,
                }
            }
            consumed += taken;
            pages_fetched += 1;
            let leftover = got.saturating_sub(skip_here).saturating_sub(taken);
            if let Some(reason) = walk_end(
                page,
                page_size,
                total,
                got,
                leftover,
                ids.len() as u64 >= max_projects,
            ) {
                end = reason;
                break;
            }
            page += 1;
        }

        // A listing that suddenly reports nothing while a corpus is already
        // stored is drift, not a clean sweep — and it must NOT wrap the cursor.
        // Gated on the cheap half first so the count query only runs in the
        // suspicious case. This is the hole the stage-2 `attempted > 0` guard
        // structurally cannot see: a total:0 listing attempts no detail fetch.
        if total == 0 && ids.is_empty() {
            let stored_corpus = ctx
                .datasets
                .count_filtered(&ctx.app, "projects", &[])
                .await?;
            if empty_listing_is_drift(total, ids.len(), stored_corpus) {
                // `SourceDrift`, not `App`: terminal for the job — the drifted
                // query grammar is fixed for the life of the job.
                return Err(Error::SourceDrift(format!(
                    "cordis: the search listing reported total:0 while {stored_corpus} projects \
                     are already stored — the query grammar drifted (the older \
                     `/project/frameworkProgramme=` grammar silently returns total:0; the \
                     VERIFIED one is `contenttype='project' AND programme/code='HORIZON'`). \
                     The resume cursor is left untouched."
                )));
            }
        }

        // ── Stage 2: per-project detail fetch (the real data). ──
        //
        // Durable execution (M23): this stage is up to `maxProjects` separate
        // governor-paced requests — by far the longest thing this app does, and
        // the reason a reap used to cost the whole run. Each project is written
        // the moment it normalizes, and the ids written so far are checkpointed,
        // so a re-claim skips them instead of re-fetching. The listing stage is
        // deliberately NOT checkpointed: it is ~5 cheap calls and re-running it
        // is what regenerates `ids` for the resume to filter.
        let mut done: HashSet<String> = restored_done(ctx.restore(), start_offset);
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
            ctx.checkpoint(stage2_state(start_offset, &done)).await;
        }
        // Drift stays loud: attempting detail fetches and normalizing NONE of
        // them is a contract break, never a clean empty sweep. Gated on what
        // this attempt actually tried — a fully-resumed run legitimately
        // normalizes nothing new.
        if attempted > 0 && normalized == 0 {
            // `SourceDrift`, not `App`: terminal for the job. This is a
            // pre-write refusal like the listing guards above — `normalized ==
            // 0` means nothing reached a dataset — and the detail contract it
            // reports on is the same shape on every attempt. (Contrast the
            // per-item degradation paths, which are warn-only and stay
            // retryable.)
            return Err(Error::SourceDrift(format!(
                "cordis: {attempted} project detail fetches attempted but 0 records \
                 normalized ({detail_failed} fetch/parse failures, {skipped} unkeyable) \
                 — the detail contract drifted (detail1.json artifact holds a raw body)"
            )));
        }
        ctx.checkpoint_now(stage2_state(start_offset, &done)).await;

        // Re-aggregate topic families over the WHOLE stored corpus (not just
        // this run's window) so stats stay consistent while the cursor sweeps.
        // Change detection makes untouched families free. Tombstoned rows are
        // excluded — `list` returns them, and a removed project must not keep
        // counting toward a family's funded-outcome numbers.
        let corpus = ctx
            .datasets
            .list_filtered(&ctx.app, "projects", &[], None, AGGREGATE_LIMIT)
            .await?;
        let corpus_values: Vec<&Value> = corpus.iter().map(|r| &r.data).collect();
        let coverage = Coverage {
            aggregated: corpus.len(),
            listing_total: Some(total),
            swept: end == SweepEnd::Complete,
        };
        let stats = aggregate_topic_stats(&corpus_values, coverage);
        let families = stats.len();
        let mut warnings: Vec<String> = Vec::new();
        // A rollup over thousands of stored projects from as many URLs: only job
        // lineage is knowable, so the automatic job_id stamp is the whole honest
        // provenance here. Naming one source_url would be a lie.
        //
        // The rollup is a COMPLETE recompute over the stored corpus, so the batch
        // IS this dataset's whole current state and a family that left the corpus
        // has to disappear — `sync_many`, not `upsert_many` (which left the ghost
        // row behind forever, and eu-sedia kept joining it onto open topics).
        // The one precondition: the corpus read must not itself have been
        // truncated. A read that came back at the cap is a WINDOW, and syncing a
        // window would tombstone every family whose projects fell outside it — so
        // the cap doubles as the switch that turns removal detection off, and it
        // is never silent.
        let complete_read = rollup_is_complete(corpus.len(), AGGREGATE_LIMIT);
        let stats_summary = if complete_read {
            ctx.sync_many("topic_stats", &stats).await?
        } else {
            warnings.push(format!(
                "topic_stats rollup aggregated only the newest {} stored projects \
                 (AGGREGATE_LIMIT = {AGGREGATE_LIMIT}): the family stats are PARTIAL, and \
                 removal detection is switched off for this run so families outside the \
                 window are not tombstoned",
                corpus.len()
            ));
            ctx.upsert_many("topic_stats", &stats).await?
        };

        // Persist the resume cursor. It wraps to the top of the corpus ONLY on a
        // proven-complete walk; every other ending keeps the place this run
        // reached, counted in consumed listing positions.
        let next_offset = if end == SweepEnd::Complete {
            0
        } else {
            start_offset + consumed
        };
        ctx.upsert("state", "cursor", &cursor_record(next_offset, page_size))
            .await?;

        let mut out = json!({
            "source": "cordis.europa.eu (search listing + project detail)",
            "query": query,
            "totalResults": total,
            "startPage": start_page,
            "start_offset": start_offset,
            "skip_in_page": skip_in_page,
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
            "stats_removed": stats_summary.removed.len(),
            "aggregate_truncated": !complete_read,
            "cursor_next_page": next_offset / page_size + 1,
            "cursor_next_offset": next_offset,
            "sweep": end.as_str(),
            "corpus_swept": end == SweepEnd::Complete,
        });
        if end == SweepEnd::ShortPage {
            // Loud, because the silent version of this cost ~46 weeks of walk.
            warnings.push(format!(
                "listing page {page} returned {} of {page_size} results while the API reports \
                 {total} total — treated as a TRUNCATED page, not the end of the corpus: the \
                 resume cursor keeps its place ({}) and `corpus_swept` is false",
                consumed.min(page_size),
                start_offset + consumed
            ));
        }
        if !warnings.is_empty() {
            if let Value::Object(map) = &mut out {
                map.insert("warnings".into(), json!(warnings));
            }
        }
        Ok(out)
    }
}

/// How the listing walk ended — the three-way distinction the single
/// `exhausted` flag used to collapse into one lie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepEnd {
    /// Page arithmetic against the listing's OWN reported total proves the walk
    /// reached the end of the corpus. The only ending that wraps the resume
    /// cursor and reports `corpus_swept: true`.
    Complete,
    /// Stopped at the per-run `maxProjects` cap with corpus left to walk.
    Capped,
    /// A page came back shorter than `pageSize` while the reported total says
    /// more remains. A transient truncation, NOT the end of the corpus.
    ShortPage,
}

impl SweepEnd {
    fn as_str(self) -> &'static str {
        match self {
            SweepEnd::Complete => "complete",
            SweepEnd::Capped => "capped",
            SweepEnd::ShortPage => "short_page",
        }
    }
}

/// Whether page arithmetic proves the walk reached the listing's end: the
/// 1-based `page` just fetched covers the listing's own reported `total`.
///
/// This — and ONLY this — is proof of the end. A short page is evidence of
/// nothing: the same shape arrives from a rate-limited or half-broken upstream.
fn reached_listing_end(page: u64, page_size: u64, total: u64) -> bool {
    page.saturating_mul(page_size) >= total
}

/// How the walk ends after fetching `page`, or `None` to keep walking.
///
/// - `got` — hits the page returned.
/// - `leftover` — hits on this page the per-run cap left unconsumed (>0 can only
///   happen when the cap cut the page mid-way, which is never the corpus end).
/// - `full` — the run has collected its `maxProjects` ids.
fn walk_end(
    page: u64,
    page_size: u64,
    total: u64,
    got: u64,
    leftover: u64,
    full: bool,
) -> Option<SweepEnd> {
    if leftover > 0 {
        // The cap truncated this page: whatever the arithmetic says about the
        // corpus, THIS run did not consume the tail it is standing on.
        Some(SweepEnd::Capped)
    } else if reached_listing_end(page, page_size, total) {
        Some(SweepEnd::Complete)
    } else if full {
        Some(SweepEnd::Capped)
    } else if got < page_size {
        Some(SweepEnd::ShortPage)
    } else {
        None
    }
}

/// Whether the FIRST page of this run coming back empty is contract drift.
///
/// It is drift when the listing says there are results AND the arithmetic puts
/// this page inside the corpus. A resume cursor that has run past a shrunken
/// listing produces the same empty page and is NOT drift — it is the ordinary
/// end of a sweep, which [`reached_listing_end`] then wraps.
fn empty_first_page_is_drift(page: u64, page_size: u64, total: u64, got: u64) -> bool {
    total > 0 && got == 0 && page.saturating_sub(1).saturating_mul(page_size) < total
}

/// Whether an empty listing result is contract drift rather than an honest
/// empty corpus: the API reported nothing at all while projects are already
/// stored locally. The verified-then-drifted query grammar (`total: 0` for a
/// syntactically valid but wrong query) lands exactly here, and it used to walk
/// straight through the stage-2 drift guard — that guard is gated on
/// `attempted > 0`, and a listing with no ids attempts no detail fetch.
fn empty_listing_is_drift(total: u64, enumerated: usize, stored_corpus: i64) -> bool {
    total == 0 && enumerated == 0 && stored_corpus > 0
}

/// The persisted resume cursor. `next_offset` is the canonical field — an
/// absolute 0-based position in the listing, so it survives a change of
/// `pageSize`; `next_page`/`skip_in_page` are its readable projection at the
/// current page size, and `next_page` alone is what a pre-offset reader saw.
fn cursor_record(next_offset: u64, page_size: u64) -> Value {
    json!({
        "next_offset": next_offset,
        "next_page": next_offset / page_size + 1,
        "skip_in_page": next_offset % page_size,
        "page_size": page_size,
    })
}

/// Reads the stored resume cursor as a listing offset, tolerating every shape
/// the `cordis/state` row has ever had. A legacy row carries only `next_page`,
/// which is converted at THIS run's page size (start-from-current, the honest
/// reading when the page size it was written at is unknown). Anything
/// unrecognizable starts from the top — never a panic, never an error.
fn cursor_offset(state: Option<&Value>, page_size: u64) -> u64 {
    let Some(state) = state else { return 0 };
    if let Some(offset) = state.get("next_offset").and_then(Value::as_u64) {
        return offset;
    }
    match state.get("next_page").and_then(Value::as_u64) {
        Some(page) => page.max(1).saturating_sub(1).saturating_mul(page_size),
        None => 0,
    }
}

/// Checkpoint schema version — a snapshot in any other shape means "start
/// fresh", never a failed run (the sink is advisory by contract).
///
/// **v2** keys the window on the listing OFFSET rather than the page number: a
/// v1 `start_page` is not comparable to a v2 `start_offset` (the same page
/// number now means a different set of ids once a mid-page resume exists), so a
/// v1 snapshot is discarded. The cost is bounded — one in-flight job re-fetches
/// details it already wrote, which change detection reports `unchanged`.
const STAGE2_STATE_VERSION: u64 = 2;

/// The stage-2 checkpoint payload: which listing window this attempt is working
/// (so a run that resumes under a *different* start offset cannot inherit a
/// stale done-set) and the project ids already written to `projects`. Ids only —
/// never record bodies, so the snapshot stays small at 5000 projects.
fn stage2_state(start_offset: u64, done: &HashSet<String>) -> Value {
    let mut ids: Vec<&String> = done.iter().collect();
    ids.sort(); // a HashSet has no order; a stable snapshot diffs cleanly
    json!({
        "v": STAGE2_STATE_VERSION,
        "stage": "details",
        "start_offset": start_offset,
        "done": ids,
    })
}

/// The already-written project ids from a prior attempt of this job, or an
/// empty set. Tolerates any stored shape; a snapshot taken against a different
/// `start_offset` is discarded rather than misapplied to a different window.
fn restored_done(state: Option<&Value>, start_offset: u64) -> HashSet<String> {
    let empty = HashSet::new();
    let Some(state) = state else { return empty };
    if state.get("v").and_then(Value::as_u64) != Some(STAGE2_STATE_VERSION)
        || state.get("stage").and_then(Value::as_str) != Some("details")
        || state.get("start_offset").and_then(Value::as_u64) != Some(start_offset)
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

/// What a `topic_stats` rollup's numbers actually rest on.
///
/// The walk takes ~46 weeks, so for most of a year these are **partial-corpus**
/// aggregates — and eu-sedia embeds them verbatim into every open Horizon topic
/// as a funded-outcome prior. Without this block a reader cannot tell "3
/// projects funded, out of the 3 that exist in this family" from "3 so far, out
/// of a corpus we have walked 5% of", and the second presented as the first is
/// the difference between a prior and a lie.
#[derive(Debug, Clone, Copy)]
struct Coverage {
    /// Stored project records this rollup aggregated.
    aggregated: usize,
    /// The listing's own reported corpus size, when stage 1 knows it. `None`
    /// writes `Null` — never a fabricated 0.
    listing_total: Option<u64>,
    /// Whether the walk has provably covered the whole corpus ([`SweepEnd`]).
    swept: bool,
}

impl Coverage {
    /// The `coverage` block every stats record carries.
    ///
    /// Deliberately carries **no timestamp**. The store already stamps
    /// `last_seen` on every rollup — including one that changed nothing — so a
    /// stamped `as_of` would buy nothing except a content change on every
    /// family every week, which eu-sedia would then propagate onto every joined
    /// Horizon topic. The as-of is read off the record envelope instead
    /// (eu-sedia surfaces it as `history.as_of`).
    fn block(self) -> Value {
        json!({
            "corpus_aggregated": self.aggregated,
            "corpus_total": self.listing_total,
            "corpus_swept": self.swept,
        })
    }
}

/// Whether the rollup's batch may be treated as the COMPLETE current state of
/// `topic_stats` — the precondition for removal detection.
///
/// The corpus read is capped at [`AGGREGATE_LIMIT`]. A read that came back AT
/// the cap is a window over the corpus, not the corpus: syncing it would
/// tombstone every family whose projects fell outside the window. So the cap is
/// also the switch that turns removals off — and the caller reports it.
fn rollup_is_complete(corpus_rows: usize, limit: i64) -> bool {
    (corpus_rows as i64) < limit
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
///
/// Every record carries the [`Coverage`] block: these numbers are a claim about
/// whatever share of the corpus has been walked so far, and they must say so.
fn aggregate_topic_stats(projects: &[&Value], coverage: Coverage) -> Vec<(String, Value)> {
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
                "coverage": coverage.block(),
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

    // ── Stage 1: sweep honesty (what may claim the corpus was swept) ──

    /// THE anti-pattern this whole seam exists for: one page that comes back
    /// short — rate limiting, a half-broken upstream, a partial index — used to
    /// set `exhausted`, which both claimed `corpus_swept: true` and reset the
    /// resume cursor to page 1, wiping up to ~46 weeks of accumulated walk.
    #[test]
    fn a_short_page_is_not_proof_the_corpus_was_swept() {
        // Page 3 of 234 comes back with 40 of 100 while the API still reports
        // 23,361 results. Nothing about the end of the corpus is proven.
        assert_eq!(
            walk_end(3, 100, 23_361, 40, 0, false),
            Some(SweepEnd::ShortPage)
        );
        assert!(!reached_listing_end(3, 100, 23_361));
        // An EMPTY page in the middle of the corpus is the same story.
        assert_eq!(
            walk_end(3, 100, 23_361, 0, 0, false),
            Some(SweepEnd::ShortPage)
        );
    }

    #[test]
    fn only_page_arithmetic_proves_the_end_of_the_corpus() {
        // The real last page: 234 * 100 = 23,400 >= 23,361. Short AND proven.
        assert!(reached_listing_end(234, 100, 23_361));
        assert_eq!(
            walk_end(234, 100, 23_361, 61, 0, false),
            Some(SweepEnd::Complete)
        );
        // A cursor that ran past a shrunken listing is also an honest end.
        assert_eq!(
            walk_end(300, 100, 23_361, 0, 0, false),
            Some(SweepEnd::Complete)
        );
        // Mid-corpus with a full page: keep walking, decide nothing.
        assert_eq!(walk_end(3, 100, 23_361, 100, 0, false), None);
    }

    #[test]
    fn a_capped_run_does_not_claim_the_corpus_was_swept() {
        // Cap hit mid-page: `leftover` hits are still sitting on the page we
        // are standing on, so this can never be the end — even if the page
        // arithmetic would otherwise say so.
        assert_eq!(
            walk_end(5, 100, 450, 100, 50, true),
            Some(SweepEnd::Capped),
            "an untaken tail must never read as a complete sweep"
        );
        // Cap hit exactly on a page boundary, mid-corpus.
        assert_eq!(
            walk_end(5, 100, 23_361, 100, 0, true),
            Some(SweepEnd::Capped)
        );
        // But a page that BOTH finishes the corpus and was fully consumed is
        // complete, cap or no cap.
        assert_eq!(walk_end(5, 100, 450, 50, 0, true), Some(SweepEnd::Complete));
    }

    #[test]
    fn only_a_complete_sweep_wraps_the_cursor() {
        // The wrap rule, stated once: Complete → back to the top; everything
        // else keeps its place. (The run body encodes this as
        // `if end == SweepEnd::Complete { 0 } else { start_offset + consumed }`.)
        for end in [SweepEnd::Capped, SweepEnd::ShortPage] {
            assert_ne!(end, SweepEnd::Complete, "{} must not wrap", end.as_str());
        }
        assert_eq!(SweepEnd::Complete.as_str(), "complete");
        assert_eq!(SweepEnd::Capped.as_str(), "capped");
        assert_eq!(SweepEnd::ShortPage.as_str(), "short_page");
    }

    #[test]
    fn an_empty_first_page_inside_the_corpus_is_drift_but_past_the_end_is_not() {
        // Page 1 empty while 23,361 results are claimed: drift (the old guard).
        assert!(empty_first_page_is_drift(1, 100, 23_361, 0));
        // …and now ALSO drift when the run resumed mid-corpus, which the old
        // `start_page == 1` gate silently exempted — i.e. every scheduled run
        // after the first.
        assert!(empty_first_page_is_drift(50, 100, 23_361, 0));
        // A cursor past the end of a shrunken listing is an ordinary sweep end.
        assert!(!empty_first_page_is_drift(300, 100, 23_361, 0));
        // A page with hits is never this kind of drift, and neither is an
        // honestly empty corpus.
        assert!(!empty_first_page_is_drift(1, 100, 23_361, 100));
        assert!(!empty_first_page_is_drift(1, 100, 0, 0));
    }

    /// The `attempted == 0` hole: the stage-2 drift guard only fires once a
    /// detail fetch has been attempted, and a listing that returns `total: 0`
    /// attempts none — so the documented query-grammar drift (the
    /// `/project/frameworkProgramme=` grammar returns total:0 against a live
    /// API) walked through every guard and reported a clean, empty, cursor-
    /// wrapping sweep.
    #[test]
    fn a_zero_total_listing_against_a_stored_corpus_is_drift_not_an_empty_sweep() {
        assert!(empty_listing_is_drift(0, 0, 8_412));
        // A genuinely empty store is a first run, not drift.
        assert!(!empty_listing_is_drift(0, 0, 0));
        // A listing that DID enumerate ids is not this failure.
        assert!(!empty_listing_is_drift(0, 12, 8_412));
        assert!(!empty_listing_is_drift(23_361, 0, 8_412));
    }

    // ── Stage 1: the resume cursor ──

    #[test]
    fn the_cursor_counts_consumed_ids_not_pages() {
        // maxProjects=450 at pageSize=100 truncates page 5 after 50 ids. The
        // anti-pattern: persisting "next page = 6", which steps over the other
        // 50 ids of page 5 for a whole corpus cycle.
        let rec = cursor_record(450, 100);
        assert_eq!(rec["next_offset"], 450);
        assert_eq!(rec["next_page"], 5, "the tail's own page, not the next one");
        assert_eq!(rec["skip_in_page"], 50, "…resuming after what was consumed");
        // Round-trips back through the reader at the same page size.
        assert_eq!(cursor_offset(Some(&rec), 100), 450);
        // …and at a DIFFERENT page size, because the offset is absolute.
        assert_eq!(cursor_offset(Some(&rec), 50), 450);
    }

    #[test]
    fn cursor_offset_tolerates_legacy_and_unknown_state_rows() {
        // The pre-offset row shape: page-only. Converted at this run's page
        // size — start-from-current, never a panic.
        assert_eq!(cursor_offset(Some(&json!({ "next_page": 7 })), 100), 600);
        assert_eq!(cursor_offset(Some(&json!({ "next_page": 1 })), 100), 0);
        // Nonsense values degrade to the top of the corpus.
        assert_eq!(cursor_offset(Some(&json!({ "next_page": 0 })), 100), 0);
        assert_eq!(cursor_offset(Some(&json!({ "next_page": "7" })), 100), 0);
        assert_eq!(cursor_offset(Some(&json!({ "frontier": 3 })), 100), 0);
        assert_eq!(cursor_offset(Some(&json!("nonsense")), 100), 0);
        assert_eq!(cursor_offset(None, 100), 0);
        // A complete sweep wraps to the very top.
        assert_eq!(cursor_offset(Some(&cursor_record(0, 100)), 100), 0);
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
            Some(&json!({ "v": 99, "stage": "details", "start_offset": 7, "done": ["a"] })),
            7
        )
        .is_empty());
        // A pre-offset (v1) snapshot keyed on `start_page` is not comparable to
        // an offset window — discarded, not misread as "page 7 == offset 7".
        assert!(restored_done(
            Some(&json!({ "v": 1, "stage": "details", "start_page": 7, "done": ["a"] })),
            7
        )
        .is_empty());
        // A well-formed snapshot with no done ids is simply an empty resume.
        assert!(restored_done(
            Some(&json!({ "v": STAGE2_STATE_VERSION, "stage": "details", "start_offset": 7 })),
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

    /// A mid-walk rollup: `n` projects aggregated out of a ~23k corpus that has
    /// NOT been swept — the state cordis is in for ~46 weeks of every year.
    fn cov(n: usize) -> Coverage {
        Coverage {
            aggregated: n,
            listing_total: Some(23_361),
            swept: false,
        }
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
        let stats = aggregate_topic_stats(&refs, cov(refs.len()));
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
        let stats = aggregate_topic_stats(&refs, cov(refs.len()));
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
        let stats = aggregate_topic_stats(&refs, cov(refs.len()));
        let top = stats[0].1["top_participants"].as_array().unwrap();
        assert_eq!(top.len(), 10, "leaderboard must stay bounded");
        // ORG-03 appears in both projects → count 2, ranked first.
        assert_eq!(top[0]["org"], "ORG-03");
        assert_eq!(top[0]["projects"], 2);
        // Ties broken by name for determinism.
        assert_eq!(top[1]["org"], "ORG-00");
    }

    // ── Rollup honesty: what the numbers rest on ──

    /// The anti-pattern: "3 projects funded, mean €2.1M" published with no way
    /// to tell whether that is the whole family or the 5% of the corpus walked
    /// so far — and eu-sedia embedding exactly that into every Horizon topic.
    #[test]
    fn partial_corpus_stats_say_so_instead_of_reading_as_the_whole_truth() {
        let a = proj("HORIZON-CL4-2022-DATA-01", Some(2_100_000.0), "FHG", &[]);
        let refs: Vec<&Value> = vec![&a];
        let mid_walk = aggregate_topic_stats(&refs, cov(1_200));
        let c = &mid_walk[0].1["coverage"];
        assert_eq!(c["corpus_aggregated"], 1_200);
        assert_eq!(c["corpus_total"], 23_361);
        assert_eq!(c["corpus_swept"], false, "3 of ~23k walked, and it says so");

        // …and the same family after a proven-complete sweep is a different
        // claim entirely, even with identical numbers.
        let swept = aggregate_topic_stats(
            &refs,
            Coverage {
                aggregated: 23_361,
                listing_total: Some(23_361),
                swept: true,
            },
        );
        assert_eq!(swept[0].1["coverage"]["corpus_swept"], true);
        assert_eq!(swept[0].1["project_count"], mid_walk[0].1["project_count"]);
        assert_ne!(swept[0].1["coverage"], mid_walk[0].1["coverage"]);
    }

    #[test]
    fn an_unknown_listing_total_is_null_not_a_fabricated_zero() {
        let a = proj("HORIZON-CL4-2022-DATA-01", Some(1.0), "FHG", &[]);
        let refs: Vec<&Value> = vec![&a];
        let stats = aggregate_topic_stats(
            &refs,
            Coverage {
                aggregated: 1,
                listing_total: None,
                swept: false,
            },
        );
        assert!(stats[0].1["coverage"]["corpus_total"].is_null());
    }

    /// The tripwire on [`AGGREGATE_LIMIT`]. A corpus read that came back AT the
    /// cap is a window, and the anti-pattern is syncing a window as if it were
    /// the whole dataset — which tombstones every family outside it.
    #[test]
    fn a_truncated_corpus_read_is_not_a_complete_state_to_sync_from() {
        assert!(rollup_is_complete(199_999, AGGREGATE_LIMIT));
        assert!(!rollup_is_complete(200_000, AGGREGATE_LIMIT));
        assert!(!rollup_is_complete(200_001, AGGREGATE_LIMIT));
        // An empty corpus is a complete (if uninteresting) state — and
        // `detect_removed` refuses an empty batch anyway.
        assert!(rollup_is_complete(0, AGGREGATE_LIMIT));
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
        let stats = aggregate_topic_stats(&refs, cov(refs.len()));
        let top = stats[0].1["top_participants"].as_array().unwrap();
        assert_eq!(top.len(), 2);
    }
}

/// End-to-end walk tests against a scripted CORDIS API and a real temp store —
/// the cursor is *persisted state*, so the arithmetic has to be proven where it
/// actually lands, not only in the predicate.
#[cfg(test)]
mod walk_tests {
    use super::*;
    use pumper_core::testing::{engines_with, Dead, TempStore, TestContext};
    use pumper_core::{HttpResponse, ScrapeApp};
    use std::collections::HashMap;
    use std::sync::Arc;

    /// A CORDIS API with a fixed corpus of ids, plus the two failure shapes
    /// this seam exists for: a page truncated mid-corpus, and a listing that
    /// reports `total: 0`.
    struct ScriptedCordis {
        corpus: Vec<String>,
        /// Reported as `payload.total` (defaults to the corpus size).
        total_override: Option<u64>,
        /// (1-based page, hits it returns) — a truncated page mid-corpus.
        short_page: Option<(u64, usize)>,
        /// How many distinct topic families the detail responses spread over.
        families: usize,
    }

    impl ScriptedCordis {
        fn of(n: usize) -> Self {
            Self {
                corpus: (0..n).map(id_at).collect(),
                total_override: None,
                short_page: None,
                families: 1,
            }
        }
    }

    /// The listing id at corpus position `i`.
    fn id_at(i: usize) -> String {
        format!("{:06}", 100_000 + i)
    }

    /// The family key the scripted detail for `id` rolls up into.
    fn family_at(i: usize, families: usize) -> String {
        format!("HORIZON-CL4-DATA-{:02}", i % families + 1)
    }

    #[async_trait]
    impl pumper_core::HttpClient for ScriptedCordis {
        async fn fetch(&self, req: HttpRequest) -> Result<HttpResponse> {
            let parsed = url::Url::parse(&req.url).expect("test url parses");
            let body = if req.url.contains("/api/search/results") {
                let q: HashMap<_, _> = parsed.query_pairs().into_owned().collect();
                let page: u64 = q["p"].parse().unwrap();
                let num: usize = q["num"].parse().unwrap();
                let from = ((page - 1) as usize).saturating_mul(num);
                let mut window: Vec<Value> = self
                    .corpus
                    .iter()
                    .skip(from)
                    .take(num)
                    .map(|id| json!({ "id": id, "contentType": "project" }))
                    .collect();
                if let Some((short, keep)) = self.short_page {
                    if short == page {
                        window.truncate(keep);
                    }
                }
                json!({ "status": true, "payload": {
                    "total": self.total_override.unwrap_or(self.corpus.len() as u64),
                    "page": page,
                    "nItems": window.len(),
                    "results": window,
                } })
            } else {
                let id = parsed.path().rsplit('/').next().unwrap().to_string();
                let idx: usize = id.parse::<usize>().unwrap() - 100_000;
                // Same lineage family as `family_at`, with a call year the
                // lineage grammar strips.
                let topic = format!("HORIZON-CL4-2022-DATA-{:02}", idx % self.families + 1);
                json!({
                    "rcn": id, "id": id, "acronym": "ACR", "title": "T",
                    "ecMaxContribution": "1000000", "totalCost": "2000000",
                    "startDate": "2022-06-01", "status": "SIGNED",
                    "relations": { "associations": {
                        "1": { "attributes": { "type": "relatedSubCall" },
                               "identifier": topic },
                        "2": { "legalName": "ORG",
                               "attributes": { "type": "coordinator",
                                               "ecContribution": "500000", "order": 1 } }
                    } }
                })
            };
            Ok(HttpResponse {
                status: 200,
                headers: HashMap::new(),
                body: body.to_string(),
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    async fn run(store: &TempStore, api: ScriptedCordis, params: Value) -> Result<Value> {
        let engines = engines_with(Arc::new(api), Arc::new(Dead), Arc::new(Dead));
        let ctx = TestContext::new(&store.storage, "cordis")
            .params(params)
            .engines(engines)
            .build();
        Cordis.run(ctx).await
    }

    /// THE tail-skip bug, end to end: `maxProjects` 450 at `pageSize` 100
    /// truncates page 5 after 50 ids, and the old cursor persisted "next page =
    /// 6" — stepping over the other 50 for a whole ~46-week corpus cycle.
    #[tokio::test]
    async fn the_truncated_tail_is_revisited_not_skipped_for_a_cycle() {
        let store = TempStore::new("cordis-tail").await;
        let params = json!({ "pageSize": 100, "maxProjects": 450 });

        let first = run(&store, ScriptedCordis::of(500), params.clone())
            .await
            .expect("run 1");
        assert_eq!(first["fetched"], 450);
        assert_eq!(first["sweep"], "capped");
        assert_eq!(first["corpus_swept"], false, "the cap is not the end");
        assert_eq!(first["cursor_next_offset"], 450);
        assert_eq!(first["cursor_next_page"], 5, "page 5, not page 6");

        // Run 2 resumes INSIDE page 5 and picks up exactly the 50 skipped ids.
        let second = run(&store, ScriptedCordis::of(500), params)
            .await
            .expect("run 2");
        assert_eq!(second["start_offset"], 450);
        assert_eq!(second["skip_in_page"], 50);
        assert_eq!(second["ids_enumerated"], 50);
        assert_eq!(second["new"], 50, "the skipped tail, not a re-fetch");
        assert_eq!(second["corpus"], 500, "the whole corpus, in two runs");
        // …and the walk is now provably done, so the cursor wraps.
        assert_eq!(second["sweep"], "complete");
        assert_eq!(second["corpus_swept"], true);
        assert_eq!(second["cursor_next_offset"], 0);
    }

    /// The wipe: one truncated page used to claim `corpus_swept: true` AND
    /// reset the cursor to page 1, throwing away every week of walk so far.
    #[tokio::test]
    async fn a_short_page_neither_claims_a_sweep_nor_resets_the_cursor() {
        let store = TempStore::new("cordis-short").await;
        let params = json!({ "pageSize": 10, "maxProjects": 100 });
        let api = ScriptedCordis {
            short_page: Some((3, 4)), // page 3 of 20 comes back 4-of-10
            ..ScriptedCordis::of(200)
        };
        let out = run(&store, api, params).await.expect("run");

        assert_eq!(out["sweep"], "short_page");
        assert_eq!(out["corpus_swept"], false);
        // 10 + 10 + 4 consumed; the cursor keeps its place instead of wrapping.
        assert_eq!(out["cursor_next_offset"], 24);
        assert_ne!(out["cursor_next_offset"], json!(0), "no wipe");
        assert!(
            out["warnings"][0]
                .as_str()
                .expect("a truncated page is reported, not swallowed")
                .contains("TRUNCATED"),
            "{out}"
        );
        // The persisted row agrees with the reported cursor.
        let ds = store.datasets();
        let state = ds.get("cordis", "state", "cursor").await.unwrap().unwrap();
        assert_eq!(state.data["next_offset"], 24);
    }

    /// The query-grammar drift the crate doc-header warns about, arriving as a
    /// clean `total: 0` — which every existing guard let through.
    #[tokio::test]
    async fn a_total_zero_listing_fails_loudly_and_leaves_the_cursor_alone() {
        let store = TempStore::new("cordis-drift").await;
        let params = json!({ "pageSize": 10, "maxProjects": 50 });

        // A healthy run first, so there IS a corpus and a cursor to protect.
        let good = run(&store, ScriptedCordis::of(200), params.clone())
            .await
            .expect("healthy run");
        assert_eq!(good["cursor_next_offset"], 50);

        // Now the drifted query: syntactically fine, semantically empty.
        let drifted = ScriptedCordis {
            total_override: Some(0),
            ..ScriptedCordis::of(0)
        };
        let err = run(&store, drifted, params)
            .await
            .expect_err("total:0 over a stored corpus must not be a clean sweep");
        assert!(err.to_string().contains("total:0"), "{err}");

        let ds = store.datasets();
        let state = ds.get("cordis", "state", "cursor").await.unwrap().unwrap();
        assert_eq!(
            state.data["next_offset"], 50,
            "a drifted listing must not move — let alone wrap — the cursor"
        );
    }

    /// The ghost: the rollup is a complete recompute, but it used to be written
    /// with `upsert_many` — so a family whose projects left the corpus kept its
    /// stale row forever, and eu-sedia kept joining that row onto open topics as
    /// a funded-outcome prior. It has to disappear.
    #[tokio::test]
    async fn a_family_that_leaves_the_corpus_is_tombstoned_not_left_as_a_ghost() {
        let store = TempStore::new("cordis-ghost").await;
        let ds = store.datasets();
        let params = json!({ "pageSize": 10, "maxProjects": 100 });

        let first = run(
            &store,
            ScriptedCordis {
                families: 2,
                ..ScriptedCordis::of(10)
            },
            params.clone(),
        )
        .await
        .expect("run 1");
        assert_eq!(first["families"], 2);
        assert_eq!(first["stats_new"], 2);
        assert_eq!(first["stats_removed"], 0);
        assert_eq!(first["aggregate_truncated"], false);
        // The partial-walk coverage rode along into the stored stats.
        let ghost_key = family_at(1, 2);
        let ghost = ds
            .get("cordis", "topic_stats", &ghost_key)
            .await
            .unwrap()
            .expect("family 02 exists");
        assert_eq!(ghost.data["coverage"]["corpus_aggregated"], 10);
        assert_eq!(ghost.data["coverage"]["corpus_swept"], true);
        assert!(ghost.removed_at.is_none());

        // Family 02's projects leave the corpus (a purge, a delisting, a
        // re-scoped query) and the listing no longer offers them.
        for i in (1..10).step_by(2) {
            assert!(ds
                .delete_record("cordis", "projects", &id_at(i))
                .await
                .unwrap());
        }
        let survivors: Vec<String> = (0..10).step_by(2).map(id_at).collect();
        let second = run(
            &store,
            ScriptedCordis {
                corpus: survivors,
                families: 2,
                ..ScriptedCordis::of(0)
            },
            params,
        )
        .await
        .expect("run 2");

        assert_eq!(second["families"], 1, "only family 01 is left");
        assert_eq!(second["stats_removed"], 1, "the ghost has to die");
        let dead = ds
            .get("cordis", "topic_stats", &ghost_key)
            .await
            .unwrap()
            .expect("the row is tombstoned, not deleted");
        assert!(
            dead.removed_at.is_some(),
            "a family that left the corpus must not keep serving stale stats"
        );
    }

    /// The honest complete case still works: a walk that reaches the listing's
    /// arithmetic end wraps, and says so.
    #[tokio::test]
    async fn a_walk_that_reaches_the_end_wraps_and_says_so() {
        let store = TempStore::new("cordis-complete").await;
        let out = run(
            &store,
            ScriptedCordis::of(25),
            json!({ "pageSize": 10, "maxProjects": 100 }),
        )
        .await
        .expect("run");
        assert_eq!(out["ids_enumerated"], 25);
        assert_eq!(out["pages"], 3);
        assert_eq!(out["sweep"], "complete");
        assert_eq!(out["corpus_swept"], true);
        assert_eq!(out["cursor_next_offset"], 0);
        assert!(out.get("warnings").is_none(), "nothing to warn about");
    }
}

/// **Inventory guard for the drift-refusal classification** (the EXPECTED-diff
/// idiom — a convention is enforced with a test, never with a sentence in a
/// doc).
///
/// A pre-write drift refusal must be [`pumper_core::Error::SourceDrift`], which
/// is terminal for the job. Raised as `Error::App` it is *retryable*, so a
/// permanent upstream rename burns three identical attempts plus backoff on
/// every scheduled run, indefinitely — and reads in the job log exactly like the
/// source being down. The next drift guard anyone adds here will be copy-pasted
/// from an existing one, so the classification is pinned rather than trusted.
#[cfg(test)]
mod drift_inventory {
    /// This file's production source, with its test modules removed — the
    /// inventory counts call sites, not the literals in these tests.
    fn production_source() -> &'static str {
        include_str!("lib.rs")
            .split(
                "
#[cfg(test)]",
            )
            .next()
            .expect("source")
    }

    /// The message literal of every `Error::App(...)` construction: the text
    /// between the first pair of quotes after the constructor.
    ///
    /// Bounded to the literal on purpose. The first cut of this guard scanned a
    /// fixed 400-character window instead, which reached past the end of one
    /// construction into the *next* guard's explanatory comment and reported a
    /// straggler that did not exist.
    fn app_error_messages(src: &str) -> Vec<&str> {
        src.split("Error::App(")
            .skip(1)
            .filter_map(|rest| {
                // Bounded lookahead: an `Error::App(v)` built from a variable
                // must not borrow a later site's literal and answer for it.
                let head: String = rest.chars().take(200).collect();
                let start = head.find('"')? + 1;
                let tail = &rest[start..];
                Some(&tail[..tail.find('"')?])
            })
            .collect()
    }

    /// Every pre-write drift refusal in this app, by a stable fragment of its
    /// message. Adding or removing one fails this test until it is classified.
    const EXPECTED_TERMINAL: &[&str] = &[
        "cordis: could not locate payload.total+results",
        "cordis: API reported {total} results but listing page",
        "cordis: the search listing reported total:0",
        "cordis: {attempted} project detail fetches attempted but 0 records",
    ];

    /// Drift this app reports **without** failing the job.
    /// None here: every drift signal cordis raises is a pre-write refusal (the
    /// stage-2 guard fires only when NOTHING normalized, so nothing was written).
    /// Per-item detail failures are counted into `detail_failed`, not raised.
    const EXPECTED_RETRYABLE: &[&str] = &[];

    #[test]
    fn every_pre_write_drift_refusal_is_terminal_not_retryable() {
        let src = production_source();
        assert_eq!(
            src.matches("Error::SourceDrift(").count(),
            EXPECTED_TERMINAL.len(),
            "a drift refusal was added or removed without updating EXPECTED_TERMINAL"
        );
        for needle in EXPECTED_TERMINAL {
            assert!(
                src.contains(needle),
                "an EXPECTED terminal drift refusal is gone or reworded: {needle}"
            );
        }
        let app_drift: Vec<&str> = app_error_messages(src)
            .into_iter()
            .filter(|message| message.contains("drift"))
            .collect();
        assert_eq!(
            app_drift.len(),
            EXPECTED_RETRYABLE.len(),
            "a drift refusal is still an Error::App, so it rides the retry ladder              and a permanent rename fails three times a day forever: {app_drift:#?}"
        );
    }
}
