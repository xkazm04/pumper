//! Grants.gov federal grant opportunities via the Search2 JSON API.
//!
//! The US-federal open-calls backbone: every federal grant opportunity, keyed by
//! its stable opportunity id, upserted into the `opportunities` dataset so a
//! scheduled daily run only surfaces what is genuinely new or changed. This is
//! the fast path — a POST-only JSON API, no HTML parsing, no browser.
//!
//! Data type: OPEN CALLS (posted + forecasted). Access: key-free. See
//! `catalog/data-sources.toml` (id `grants-gov`) for how this fits the pipeline map.
//!
//! Contract notes (verified 2026-07-03): `https://api.grants.gov/v1/api/search2`
//! is **POST-only** — a bare GET returns 403. The body is JSON; pagination is
//! `startRecordNum` + `rows`; results live under `data.oppHits[]` with
//! `data.hitCount` as the total. A hit carries EXACTLY
//! `id, number, title, agencyCode, agency, openDate, closeDate, oppStatus,
//! docType, cfdaList` (re-verified live 2026-08-04) — **no award amounts**, which
//! is why federal money is joined in from the detail corpus below rather than
//! read from the listing.
//!
//! **Sweep honesty.** The walk stops for four different reasons and only one of
//! them proves the corpus was covered — see [`SweepEnd`]. `sweep` names the arm
//! (`complete` | `capped` | `short_page` | `unknown_total`) and `truncated` is
//! its boolean projection. Every non-complete arm also lands in `warnings[]`.
//! A page that the server's own `hitCount` places inside the corpus but that
//! comes back empty is contract drift on EVERY page (not just the first), and a
//! `hitCount:0` answer to an unfiltered query while opportunities are already
//! stored is drift rather than a clean sweep.
//!
//! Detail harvest (`harvestDetails`, default **ON** since 2026-08-04 — the one
//! statement of that default is [`HARVEST_DETAILS_DEFAULT`], which both the
//! absent-param path and `default_params` read): for opportunities the sync just
//! reported NEW or CHANGED (never the whole corpus), fetch the full announcement
//! record and store it into `grants/opportunity_details` keyed by opportunity id,
//! with a structured `requirements` block extracted from the synopsis fields and
//! the NOFO attachment manifest (URLs + metadata only — v1 does NO PDF fetching
//! or parsing; a later pass can pull the documents).
//!
//! **Absent is not empty**, in the detail record as well as in the money fields:
//! [`applicant_types`] and [`attachment_manifest`] answer `Null` when the source
//! did not publish the field at all, and `[]` only when it published an empty
//! one. A consumer can therefore tell "this NOFO lists no eligible applicant
//! types" from "`applicantTypes` was renamed".
//!
//! The stage is **non-fatal to the listing sync but never silent**: the daily
//! federal sync's primary obligation is the listing, so a fetchOpportunity
//! outage or contract drift is COUNTED into the result's `detailsFailed` (plus
//! a `details.errors[]` sample and a `warnings` entry) instead of failing the
//! job, and a run of `DETAIL_CONSECUTIVE_FAILURE_ABORT` back-to-back failures
//! stops the stage early rather than burning the whole cap on a dead endpoint.
//! Because coverage is exactly what the harvest has SEEN, federal `min_award`
//! coverage grows forward from the day the harvest was switched on — a
//! corpus-wide backfill is a deliberate non-goal.
//!
//! fetchOpportunity contract (pinned 2026-07-30; the envelope, `data` shape and
//! the money/count fields were VERIFIED LIVE 2026-08-04 against opportunities
//! 357305 and 141593 — the attachment block and its download-URL pattern remain
//! ASSUMED. The defensive parse in `extract_detail` is the tripwire, and the raw
//! first response is kept as the `detail1.json` artifact):
//!   POST https://api.grants.gov/v1/api/fetchOpportunity
//!   body: `{"opportunityId": <the Search2 hit id, sent as a JSON number when
//!          it parses as an integer, else as the raw string>}`
//!   envelope: `{"errorcode": 0, "msg": ..., "data": {...}}` — the same wrapper
//!   as search2. `data` carries id / opportunityNumber / opportunityTitle,
//!   agency fields, a `synopsis` object (posted) or `forecast` object
//!   (forecasted) with applicantTypes[] (objects with `description` or bare
//!   strings), applicantEligibilityDesc, costSharing (bool or "Yes"/"No"),
//!   awardFloor / awardCeiling / estimatedFunding (LIVE-VERIFIED 2026-08-04: they
//!   are decimal STRINGS — `"55746"` — with the literal `"none"` where the agency
//!   published no figure, alongside `*Formatted` siblings like `"55,746"`. The
//!   shared `money_scalar` maps `"none"` to Null, never 0),
//!   numberOfAwards (LIVE-VERIFIED 2026-07-30; `expectedNumberOfAwards` does not
//!   appear in real payloads — read as fallback only), responseDate (may be null
//!   on already-awarded listings whose prose lives in responseDateDesc) — and
//!   attachment folders under
//!   `synopsisAttachmentFolders[]` (each with `synopsisAttachments[]` carrying
//!   id / fileName / fileDescription / mimeType / fileLobSize), with a flat
//!   `attachments[]` tolerated as fallback. The attachment download-URL
//!   pattern is likewise ASSUMED
//!   (`https://apply07.grants.gov/grantsws/rest/opportunity/att/download/{id}`)
//!   and stored alongside the raw metadata so the later PDF pass can verify.
//! Drift discipline (cordis-style): if the envelope parses but `data` is not a
//! recognizable detail object, the run FAILS loudly — never a silent empty
//! detail corpus. Money fields follow the shared honest-Null rule ($0, prose,
//! absent → Null, never a fabricated zero).

//! The `closing_soon` digest judges "is this still claimable?" with
//! [`grants_common::deadline_end_utc`] — the SAME instant the unified sweep and
//! `GET /grants/closing-soon` use — so the three surfaces cannot disagree about
//! a grant that is past its printed date but not yet past its
//! anywhere-on-Earth deadline.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, DerivedPaths, Error, HttpMethod, HttpRequest,
    ManifestExample, Provenance, Result, ScrapeApp,
};
use serde_json::{json, Value};

pub struct GrantsGov;

const SEARCH2_URL: &str = "https://api.grants.gov/v1/api/search2";

#[async_trait]
impl ScrapeApp for GrantsGov {
    fn name(&self) -> &'static str {
        "grants-gov"
    }

    fn description(&self) -> &'static str {
        "US federal grant opportunities (Grants.gov Search2 API, key-free). \
         Open calls, keyed by opportunity id into the `opportunities` dataset. \
         Params: {\"oppStatuses\": \"posted|forecasted\", \"keyword\": \"\", \
         \"eligibilities\": \"\" (pipe-separated grants.gov codes, e.g. 12|13|25|99 \
         for nonprofits), \"rows\": 1-1000, \"maxPages\": 1-100, \
         \"harvestDetails\": true (fetchOpportunity details for new/changed \
         opps into grants/opportunity_details; non-fatal — failures are counted \
         in `detailsFailed`), \"maxDetailsPerRun\": 1-500}"
    }

    /// Daily full sync of open opportunities at 09:00 UTC. Scheduled runs use
    /// `default_params`: posted+forecasted at the API's max 1000-row page size, so
    /// the corpus is covered in ~3 round-trips and the ceiling is 25k, not 2.5k.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 9 * * *")
    }

    /// Scheduled runs harvest details too (`harvestDetails: true`).
    ///
    /// The listing is this job's primary obligation; the detail harvest is a
    /// secondary enrichment stage that exists so `grants/unified` (and therefore
    /// `GET /grants?min_award=`) can see federal award amounts at all — Search2
    /// publishes none. Turning it on in the default params is what makes that
    /// coverage grow at all: the harvest is delta-only (new/changed keys) and
    /// capped, so a daily run is tens of `fetchOpportunity` calls, never 25k.
    ///
    /// It is safe to schedule ONLY because the stage is non-fatal to the listing
    /// (see [`detail_stage_is_broken`] and the harvest block in `run`): a
    /// fetchOpportunity outage or contract drift is COUNTED into `detailsFailed`
    /// and named in `warnings`, never allowed to take the federal listing sync
    /// down with it — and never silently swallowed either.
    fn default_params(&self) -> Value {
        json!({
            "oppStatuses": "posted|forecasted",
            "rows": 1000,
            "maxPages": 25,
            "harvestDetails": true,
            "maxDetailsPerRun": 50
        })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "oppStatuses": {
                        "type": "string",
                        "description": "Pipe-separated grants.gov statuses, e.g. \"posted|forecasted\"."
                    },
                    "keyword": { "type": "string" },
                    "eligibilities": {
                        "type": "string",
                        "description": "Pipe-separated grants.gov eligibility codes, e.g. \"12|13|25|99\" for nonprofits."
                    },
                    "rows": { "type": "integer", "minimum": 1, "maximum": 1000 },
                    "maxPages": { "type": "integer", "minimum": 1, "maximum": 100 },
                    "digestDays": { "type": "integer", "minimum": 1 },
                    "harvestDetails": {
                        "type": "boolean",
                        "description": "Fetch full opportunity details (fetchOpportunity) for opportunities this sync reported new/changed, into grants/opportunity_details. Default TRUE (scheduled runs harvest). The stage is non-fatal to the listing sync: failures are counted in `detailsFailed` and named in `warnings`."
                    },
                    "maxDetailsPerRun": {
                        "type": "integer", "minimum": 1, "maximum": 500,
                        "description": "Cap on detail fetches per run (default 50); a capped run says so honestly in the result."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Full daily sync: every posted + forecasted opportunity (the scheduled default)",
                    params: json!({ "oppStatuses": "posted|forecasted", "rows": 1000, "maxPages": 25 }),
                },
                ManifestExample {
                    description: "Targeted pull: posted nonprofit-eligible opportunities matching a keyword",
                    params: json!({
                        "oppStatuses": "posted",
                        "keyword": "rural health",
                        "eligibilities": "12|13|25|99",
                        "rows": 500,
                        "maxPages": 5
                    }),
                },
                ManifestExample {
                    description: "Sync + NOFO detail harvest: fetch full announcement details for new/changed opportunities (capped)",
                    params: json!({
                        "oppStatuses": "posted|forecasted",
                        "rows": 1000,
                        "maxPages": 25,
                        "harvestDetails": true,
                        "maxDetailsPerRun": 50
                    }),
                },
            ],
            // Pinned against a real run by
            // `tests/result_contract.rs::the_published_output_shape_is_what_the_run_emits`
            // — the declaration and the `json!` block must agree in BOTH
            // directions. It previously declared `hit_count` (emitted:
            // `hitCount`) and `removed?` (emitted: never, and structurally
            // unemittable — see `OPPORTUNITIES_DATASET`'s upsert-only write),
            // while omitting twelve keys the run does emit.
            output_shape: Some(
                "{source, oppStatuses, hitCount, fetched, pages, new, changed, unchanged, \
                 digestDays, closingSoonCount, closingSoon[], amountsFilled, \
                 detailCorpus: {read, truncated}, detailsFailed, sweep, truncated, \
                 details: {harvested, deltaTotal, capped, resumedFrom, attempted, failed, \
                 abortedAfterConsecutiveFailures, errors[]}, \
                 unified: {new, changed, events, dataset, trust, sourceState}, swept, \
                 crossSourceDups, recurrenceLinks, \
                 corpusPass: {ran, cycle, batchSwept, corpusSwept}, warnings[], \
                 index_datasets[]} — Search2 sync tallies over the `opportunities` dataset \
                 (keyed by opportunity id). `sweep` names how the walk ended \
                 (`complete` | `capped` | `short_page` | `unknown_total`) and `truncated` is \
                 its boolean projection; every non-complete arm also lands in `warnings[]`. \
                 `closingSoon[]` is the FEDERAL-only deadline digest over this run's own hits \
                 (`digestDays`, default 14), judged at the same anywhere-on-Earth instant the \
                 unified sweep uses; `GET /grants/closing-soon` is the cross-source view. \
                 The detail harvest writes grants/opportunity_details and reports `details` \
                 (**absent when `harvestDetails` is false**); `amountsFilled` counts unified \
                 rows that got award amounts joined in from that detail corpus (Search2 itself \
                 publishes none) and `detailCorpus` says how much of it the join read; \
                 `detailsFailed` counts detail-stage failures, which degrade the enrichment \
                 without failing the listing sync. There is deliberately no `removed` key: the \
                 listing is written with `upsert_many_with_provenance`, which never tombstones. \
                 The cross-source tail reports `unified`, `swept`, `crossSourceDups`, \
                 `recurrenceLinks` and `corpusPass`: the corpus-wide relation pass (sweep + \
                 duplicate/recurrence links) runs once per UTC-day cycle on whichever grant \
                 source gets there first, so a run that did not own it reports both link counts \
                 as null. `index_datasets[]` is **withheld entirely** when this source's \
                 extraction health says its rows must not reach the search index",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let statuses = ctx
            .params
            .get("oppStatuses")
            .and_then(Value::as_str)
            .unwrap_or("posted|forecasted")
            .to_string();
        let keyword = ctx
            .params
            .get("keyword")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let eligibilities = ctx
            .params
            .get("eligibilities")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let rows = ctx
            .params
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000);
        let max_pages = ctx
            .params
            .get("maxPages")
            .and_then(Value::as_u64)
            .unwrap_or(25)
            .clamp(1, 100);

        let mut hits: Vec<Value> = Vec::new();
        let mut hit_count: u64 = 0;
        let mut pages: u64 = 0;
        let end: SweepEnd;

        loop {
            // Derived from the page counter rather than accumulated, so the
            // request offset and the arithmetic `walk_end` reasons about can
            // never drift apart.
            let start = pages.saturating_mul(rows);
            let body = json!({
                "keyword": keyword,
                "oppNum": "",
                "eligibilities": eligibilities,
                "agencies": "",
                "oppStatuses": statuses,
                "aln": "",
                "fundingCategories": "",
                "rows": rows,
                "startRecordNum": start,
            })
            .to_string();

            let resp = ctx.engines.http.fetch(search2_request(body)).await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "grants.gov search2 returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }

            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("grants.gov: response was not JSON: {e}")))?;
            if let Some(reason) = envelope_error(&parsed) {
                return Err(Error::App(format!("grants.gov search2 {reason}")));
            }

            let data = parsed.get("data").cloned().unwrap_or(Value::Null);
            if pages == 0 {
                hit_count = data.get("hitCount").and_then(Value::as_u64).unwrap_or(0);
                // Keep the first raw page for debugging / schema drift checks.
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            let page_hits = data
                .get("oppHits")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = page_hits.len() as u64;
            pages += 1;
            // Drift guard, now per PAGE: a page that the server's own hitCount
            // places inside the corpus came back with nothing in it, i.e.
            // `data.oppHits` was renamed/moved and `unwrap_or_default` emptied
            // it. This subsumes the old post-loop `hits.is_empty()` check (page
            // 1 evaluates identically) and additionally catches the mid-sweep
            // rename, which used to read as an ordinary short page and end the
            // walk reporting `truncated: false`.
            if empty_page_is_drift(pages, rows, hit_count, got) {
                return Err(Error::App(format!(
                    "grants.gov schema drift: hitCount={hit_count} but page {pages} \
                     (startRecordNum={start}) parsed 0 oppHits (data.oppHits missing \
                     or not an array)"
                )));
            }
            hits.extend(page_hits);

            if let Some(reason) =
                walk_end(pages, rows, hit_count, got, hits.len() as u64, max_pages)
            {
                end = reason;
                break;
            }
        }

        // A listing that reports nothing at all while opportunities are already
        // stored is drift, not a clean sweep. Gated on the cheap half first so
        // the count query only runs in the suspicious case — and only for a
        // query that selects the WHOLE corpus, because a narrowed pull
        // (keyword/eligibilities) can legitimately match nothing.
        if hit_count == 0 && hits.is_empty() {
            let stored_corpus = ctx
                .datasets
                .count_filtered(&ctx.app, OPPORTUNITIES_DATASET, &[])
                .await?;
            if empty_listing_is_drift(
                hit_count,
                hits.len(),
                stored_corpus,
                whole_corpus_query(&keyword, &eligibilities),
            ) {
                return Err(Error::App(format!(
                    "grants.gov schema drift: search2 reported hitCount:0 with no oppHits \
                     for an unfiltered {statuses} query while {stored_corpus} opportunities \
                     are already stored — the query grammar or the response shape drifted. \
                     Nothing was swept; the stored corpus is left untouched."
                )));
            }
        }

        // Honest coverage: only the arm that PROVES the corpus was covered reads
        // as complete. The prior flag was computed from the `maxPages` arm alone
        // (`pages >= max_pages && start < hit_count`) above a comment claiming
        // this whole class was closed, so a short page, a mid-sweep drop and a
        // renamed `hitCount` each returned Ok identically to a genuine full
        // sweep. See [`SweepEnd`].
        let truncated = end != SweepEnd::Complete;

        // Dedup + change detection: key each opportunity by its stable id (falling
        // back to the opportunity number, then row index). A scheduled run reports
        // only new/changed opportunities — the substrate for deadline alerts.
        let items: Vec<(String, Value)> = hits
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let key = h
                    .get("id")
                    .and_then(Value::as_str)
                    .or_else(|| h.get("number").and_then(Value::as_str))
                    .map(String::from)
                    .unwrap_or_else(|| format!("row-{i}"));
                (key, h.clone())
            })
            .collect();

        // Provenance (M12): every page of this batch was fetched from the one
        // Search2 endpoint, so the batch-level `source_url` is a fact, not a
        // guess. `artifact_sha`/`rules_hash` stay Null — the hits are stored
        // verbatim, no RuleSet extracted them, and the saved artifact is only
        // page 1, not the body behind each record.
        let summary = ctx
            .upsert_many_with_provenance(
                OPPORTUNITIES_DATASET,
                &items,
                Provenance {
                    source_url: Some(SEARCH2_URL.to_string()),
                    ..Provenance::default()
                },
            )
            .await?;

        // NOFO detail harvest (default ON — see `HARVEST_DETAILS_DEFAULT`): only
        // the delta this sync surfaced — new + changed keys — ever triggers a
        // fetchOpportunity call, capped per run, so the daily sweep stays tens
        // of calls, never 25k.
        let harvest_details = harvest_details_enabled(&ctx.params);
        let max_details = ctx
            .params
            .get("maxDetailsPerRun")
            .and_then(Value::as_u64)
            .unwrap_or(50)
            .clamp(1, 500) as usize;
        let mut details_out: Option<Value> = None;
        let mut details_warning: Option<String> = None;
        // Detail-stage failures, counted (never swallowed) so a broken secondary
        // stage is visible in the result rather than only in a log line. Zero is
        // a real claim here: the stage ran and nothing failed.
        let mut details_failed: usize = 0;
        // Appended to the result's `warnings` AFTER the unified merge (which
        // sets that array), so a degradation is never overwritten by it.
        let mut degradation_warnings: Vec<String> = Vec::new();
        if harvest_details {
            // Durable execution (M23). The harvest is the only genuinely long,
            // resumable unit here: one governor-paced fetchOpportunity call per
            // delta key, up to `maxDetailsPerRun`. It is also the stage a
            // restart would silently ZERO — on a re-claim the listing re-syncs,
            // every opportunity reads back `unchanged`, and the delta collapses
            // to empty. So the checkpoint carries the delta itself, not just a
            // cursor, and progress is flushed to the dataset before it is
            // recorded as done.
            let resumed = restored_harvest(ctx.restore());
            let (delta, capped, delta_total) = match &resumed {
                Some(state) => (state.delta.clone(), state.capped, state.delta_total),
                None => {
                    let (delta, capped) = capped_delta(&summary.new, &summary.changed, max_details);
                    (delta, capped, summary.new.len() + summary.changed.len())
                }
            };
            let mut done: HashSet<String> = resumed
                .map(|s| s.done)
                .unwrap_or_default()
                .into_iter()
                .collect();
            let resumed_count = done.len();

            let by_key: HashMap<&str, &Value> =
                items.iter().map(|(k, v)| (k.as_str(), v)).collect();
            let mut buffer: Vec<(String, Value)> = Vec::new();
            let mut first_fetch = resumed_count == 0;
            let pending: Vec<String> = delta
                .iter()
                .filter(|k| !done.contains(k.as_str()))
                .cloned()
                .collect();
            // NON-FATAL, LOUD. The detail stage may not take the listing sync
            // down: the daily federal sync's primary obligation is the listing,
            // and by this point it is already stored. But "non-fatal" must not
            // become "invisible" — this repo runs on honest nulls and visible
            // gaps, so every failure is counted into `details_failed`, the first
            // few are named verbatim, and the run carries a `warnings` entry.
            // The strict drift check inside `fetch_detail`/`extract_detail` is
            // deliberately UNCHANGED: contract drift still produces a named
            // error, it is now a visible degradation signal instead of a
            // whole-job failure.
            let mut errors: Vec<String> = Vec::new();
            let mut consecutive = 0usize;
            let mut aborted = false;
            let mut attempted = 0usize;
            for key in &pending {
                attempted += 1;
                match fetch_detail(&ctx, key, first_fetch).await {
                    Ok(detail) => {
                        consecutive = 0;
                        buffer.push((
                            key.clone(),
                            detail_record(key, by_key.get(key.as_str()).copied(), &detail),
                        ));
                    }
                    Err(e) => {
                        details_failed += 1;
                        consecutive += 1;
                        record_stage_error(&mut errors, format!("{key}: {e}"));
                    }
                }
                first_fetch = false;
                if buffer.len() >= DETAIL_FLUSH {
                    if let Err(e) = flush_details(&ctx, &mut buffer, &mut done).await {
                        details_failed += buffer.len();
                        record_stage_error(&mut errors, format!("detail flush failed: {e}"));
                        buffer.clear();
                    }
                    ctx.checkpoint(harvest_state(&delta, &done, capped, delta_total))
                        .await;
                }
                // A run of consecutive failures is contract drift or an outage,
                // not flakiness: stop paying for it, but say so. Burning the
                // whole cap against a dead endpoint would be its own waste.
                if detail_stage_is_broken(consecutive) {
                    aborted = true;
                    break;
                }
            }
            if let Err(e) = flush_details(&ctx, &mut buffer, &mut done).await {
                details_failed += buffer.len();
                record_stage_error(&mut errors, format!("detail flush failed: {e}"));
            }
            // Final snapshot is unthrottled: losing it costs a whole re-harvest.
            ctx.checkpoint_now(harvest_state(&delta, &done, capped, delta_total))
                .await;

            if capped {
                details_warning = Some(format!(
                    "detail harvest capped: fetched {} of {delta_total} new/changed \
                     opportunities (maxDetailsPerRun={max_details}) — the rest will be \
                     picked up as they change, or raise the cap",
                    done.len()
                ));
            }
            details_out = Some(json!({
                "harvested": done.len(),
                "deltaTotal": delta_total,
                "capped": capped,
                "resumedFrom": resumed_count,
                // The degradation block: what the stage tried, what broke, and
                // the first few reasons in the endpoint's own words.
                "attempted": attempted,
                "failed": details_failed,
                "abortedAfterConsecutiveFailures": aborted,
                "errors": errors,
            }));
            if let Some(msg) = detail_stage_degradation(details_failed, attempted, aborted, &errors)
            {
                degradation_warnings.push(msg);
            }
        }

        // Cross-source layer: normalize into grants/unified, sweep past-due rows
        // closed, and link SimHash near-duplicates syndicated across portals.
        let mut unified_items: Vec<(String, Value)> = hits
            .iter()
            .filter_map(grants_common::normalize_grants_gov)
            .collect();
        // Money join: Search2 publishes no award amounts (live-verified), so a
        // federal unified row is permanently null on all three money fields and
        // `GET /grants?min_award=` can never match it. The figures live in the
        // `synopsis` block of the fetchOpportunity detail records this machine
        // already stores, so overlay them from the store — no extra fetch, and
        // an opportunity with no stored detail keeps its honest Null.
        let amounts = grants_common::enrich_with_detail_amounts(&ctx, &mut unified_items).await?;
        if let Some(msg) = amounts.warning() {
            degradation_warnings.push(msg);
        }
        let cross =
            grants_common::finalize_unified(&ctx, &unified_items, Some(SEARCH2_URL)).await?;

        // Closing-soon digest: posted opportunities whose closeDate falls within
        // the next `digestDays` days, soonest first — the deadline-alert surface
        // this dataset was always meant to feed.
        let digest_days = ctx
            .params
            .get("digestDays")
            .and_then(Value::as_u64)
            .unwrap_or(14)
            .clamp(1, 365) as i64;
        let closing_soon = closing_soon_digest(&hits, digest_days, chrono::Utc::now());
        ctx.save_artifact(
            "closing_soon.json",
            &serde_json::to_vec_pretty(&closing_soon)?,
        )
        .await?;
        // The digest's status filter refuses to read an ABSENT `oppStatus` as
        // `posted`, which is only safe while the blind case is loud: a renamed
        // status field would otherwise turn the digest silently empty, and a
        // quiet fortnight looks identical.
        if let Some(msg) = digest_status_drift(&hits) {
            degradation_warnings.push(msg);
        }

        let mut out = json!({
            "source": "grants.gov/search2",
            "oppStatuses": statuses,
            "hitCount": hit_count,
            "fetched": hits.len(),
            "pages": pages,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "digestDays": digest_days,
            "closingSoonCount": closing_soon.len(),
            "closingSoon": closing_soon.iter().take(25).collect::<Vec<_>>(),
            // How many unified rows got award amounts joined in from the stored
            // detail corpus this run — i.e. how much of the federal corpus
            // `min_award` can actually see.
            "amountsFilled": amounts.filled,
            // What that number rests on: how much of the stored detail corpus
            // the join actually read, and whether that read was a WINDOW. A
            // silently-windowed join reports a lower `amountsFilled` and is
            // otherwise indistinguishable from agencies publishing no money.
            "detailCorpus": {
                "read": amounts.read,
                "truncated": amounts.truncated,
            },
            // Flat, greppable count of detail-stage failures this run. The stage
            // is non-fatal to the listing, so this is the ONLY place a caller
            // learns the enrichment degraded — it is never allowed to be absent.
            "detailsFailed": details_failed,
            // How the walk ended, named — see [`SweepEnd`]. `truncated` is its
            // boolean projection ("anything but complete"), kept because it is
            // the key consumers already read.
            "sweep": end.as_str(),
            "truncated": truncated,
        });
        cross.merge_into(&mut out);
        if let Some(details) = details_out {
            if let Value::Object(map) = &mut out {
                map.insert("details".into(), details);
            }
        }
        if let Some(msg) = details_warning {
            append_warning(&mut out, msg);
        }
        for msg in degradation_warnings {
            append_warning(&mut out, msg);
        }
        if let Some(msg) = sweep_warning(end, pages, max_pages, rows, hit_count, hits.len()) {
            append_warning(&mut out, msg);
        }
        Ok(out)
    }
}

/// The app's own listing dataset. Named so the sweep guards and the write
/// cannot drift apart — the drift guard counts the corpus this write produced.
const OPPORTUNITIES_DATASET: &str = "opportunities";

/// How the Search2 walk ended.
///
/// The single `truncated` boolean collapsed four different endings into one
/// claim, and three of them read as a complete corpus. This is cordis's
/// [`SweepEnd`](../../cordis/src/lib.rs) vocabulary — `Complete` / `Capped` /
/// `ShortPage`, same names, same meanings — deliberately reused rather than
/// re-invented.
///
/// **The one divergence**: cordis's listing always publishes a usable total, so
/// it has no equivalent of [`SweepEnd::UnknownTotal`]. grants.gov's `hitCount`
/// is read with `unwrap_or(0)`, so a rename of that one field yields
/// `hit_count = 0`, and `start >= hit_count` (`1000 >= 0`) then broke the walk
/// after page 1 while the drift guard — gated on `hit_count > 0` — stayed
/// silent. The corpus capped at one page, green, indefinitely. That arm needs a
/// name of its own because the remedy is different: nothing is wrong with the
/// walk, the *proof* is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepEnd {
    /// The records actually COLLECTED reach the listing's own reported
    /// `hitCount`. The only ending that reads as complete.
    Complete,
    /// Stopped at `maxPages` with records left to walk.
    Capped,
    /// A page came back shorter than `rows` while the reported `hitCount` says
    /// more remains. A transient truncation (a rate-limited or partially-served
    /// page), NOT the end of the corpus.
    ShortPage,
    /// The response served hits under a `hitCount` of 0 — absent, renamed, or
    /// zero. The total is unusable, so no arithmetic can prove the end; the walk
    /// runs on until a short page or the cap and reports coverage as unproven.
    UnknownTotal,
}

impl SweepEnd {
    fn as_str(self) -> &'static str {
        match self {
            SweepEnd::Complete => "complete",
            SweepEnd::Capped => "capped",
            SweepEnd::ShortPage => "short_page",
            SweepEnd::UnknownTotal => "unknown_total",
        }
    }
}

/// How the walk ends after fetching the 1-based `page`, or `None` to keep going.
///
/// `collected` is every hit gathered so far **including this page**, and it —
/// not page arithmetic — is what proves coverage. cordis's equivalent asks
/// `page * page_size >= total`, which counts the listing positions *requested*;
/// the short-page bug this function exists to kill is precisely a page that
/// asked for 1000 and delivered 100, so on the second page of a 1366-record
/// corpus that test reads `2000 >= 1366` and calls 1100 records a complete
/// sweep. Counting what actually arrived is the only proof that survives a
/// partially-served page.
///
/// The cost of the stricter test: an upstream whose `hitCount` is racy (the
/// corpus shrank mid-walk) ends `ShortPage` with a warning instead of
/// `Complete`. A false "coverage unproven" is recoverable; a false "corpus
/// covered" is the failure that hides money.
///
/// The ordering is load-bearing and mirrors cordis's: proof of coverage first,
/// then the per-run cap, then the short page — because a short page is
/// **evidence of nothing** (a rate-limited upstream produces exactly the same
/// shape as a genuine tail) and must never outrank the proof.
fn walk_end(
    page: u64,
    rows: u64,
    hit_count: u64,
    got: u64,
    collected: u64,
    max_pages: u64,
) -> Option<SweepEnd> {
    if hit_count == 0 {
        // No usable total. Two sub-cases, told apart by what the SAME response
        // served — which is the only evidence available here.
        if got == 0 {
            // Self-consistent: no total, no hits. An honestly empty result set
            // (a narrowed pull matching nothing) IS fully swept. Whether it is
            // instead drift is decided against the STORED corpus by
            // [`empty_listing_is_drift`], never against this same response.
            return Some(SweepEnd::Complete);
        }
        // Self-contradictory: hits served under a zero total. Keep walking —
        // a short page or the cap is the only end signal left — but never
        // report complete.
        return (got < rows || page >= max_pages).then_some(SweepEnd::UnknownTotal);
    }
    if collected >= hit_count {
        return Some(SweepEnd::Complete);
    }
    if page >= max_pages {
        return Some(SweepEnd::Capped);
    }
    if got < rows {
        return Some(SweepEnd::ShortPage);
    }
    None
}

/// Whether a page that returned nothing is contract drift.
///
/// It is drift when the listing's own `hitCount` places this page **inside** the
/// corpus: the records are there, `data.oppHits` did not deliver them. A page
/// past the end of a shrunken listing produces the same empty shape and is not
/// drift — the arithmetic tells them apart. cordis's `empty_first_page_is_drift`
/// generalized to every page, because grants-gov re-walks from 0 every run and
/// therefore has no "first page of this run" that is special.
fn empty_page_is_drift(page: u64, rows: u64, hit_count: u64, got: u64) -> bool {
    hit_count > 0 && got == 0 && page.saturating_sub(1).saturating_mul(rows) < hit_count
}

/// Whether the query selects the whole corpus, i.e. whether an empty answer is
/// allowed to be judged against the stored corpus at all.
///
/// `oppStatuses` is deliberately NOT part of this: it defines *which* corpus,
/// and every value of it should still return something while rows are stored.
/// `keyword` / `eligibilities` narrow *within* a corpus and can legitimately
/// match nothing — the manifest's own "targeted pull" example does exactly that.
fn whole_corpus_query(keyword: &str, eligibilities: &str) -> bool {
    keyword.trim().is_empty() && eligibilities.trim().is_empty()
}

/// Whether an empty listing is contract drift rather than an honestly empty
/// result: the API reported nothing at all for a whole-corpus query while
/// opportunities are already stored locally.
///
/// This is cordis's `empty_listing_is_drift` reasoning, with one addition it
/// does not need (cordis runs a single fixed query): the whole-corpus gate. The
/// count must come from the STORED corpus, never from the same response being
/// doubted.
///
/// Tombstones: the count comes from `Datasets::count_filtered`, which is
/// `removed_at IS NULL`. That is deliberate in both directions — this app only
/// ever upserts, so live == all today, and if some other path ever tombstones an
/// opportunity, a tombstoned row is not evidence that the source still has a
/// corpus to serve.
fn empty_listing_is_drift(
    hit_count: u64,
    fetched: usize,
    stored_corpus: i64,
    whole_corpus_query: bool,
) -> bool {
    hit_count == 0 && fetched == 0 && stored_corpus > 0 && whole_corpus_query
}

/// Why a grants.gov envelope is NOT an application-level success, or `None` when
/// it is one.
///
/// The anti-pattern: `errorcode` was read as
/// `.and_then(Value::as_i64).unwrap_or(0)`, so an envelope with **no**
/// `errorcode`, a null one, or a JSON-type-drifted `"0"` all defaulted to
/// *success* — the one value a status field must never default to. Both
/// endpoints already publish numbers as strings (`awardFloor: "55746"`), so a
/// stringified code is a live possibility, not a hypothetical; it is accepted as
/// the same integer, while anything unreadable is a named failure.
fn envelope_error(parsed: &Value) -> Option<String> {
    let raw = parsed.get("errorcode");
    let code = match raw {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    };
    match code {
        Some(0) => None,
        Some(code) => Some(format!(
            "error code {code}: {}",
            parsed
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        )),
        None => Some(format!(
            "carries no readable `errorcode` (found {}) — an unreadable status is \
             never success",
            match raw {
                None => "no such field".to_string(),
                Some(v) => v.to_string().chars().take(60).collect::<String>(),
            }
        )),
    }
}

/// The human-readable warning for a walk that did not prove its coverage, or
/// `None` for a complete sweep.
///
/// Every non-complete arm reaches the caller through `warnings[]` as well as
/// through `sweep`/`truncated`, because a consumer reading only the warnings
/// channel is exactly the consumer who would otherwise never learn that the
/// federal corpus is short.
fn sweep_warning(
    end: SweepEnd,
    pages: u64,
    max_pages: u64,
    rows: u64,
    hit_count: u64,
    fetched: usize,
) -> Option<String> {
    match end {
        SweepEnd::Complete => None,
        SweepEnd::Capped => Some(format!(
            "coverage truncated: stopped at maxPages={max_pages} after {fetched} of \
             {hit_count} records — raise rows/maxPages to cover the full corpus"
        )),
        SweepEnd::ShortPage => Some(format!(
            "coverage truncated: page {pages} returned fewer than rows={rows} while \
             search2 reports {hit_count} total, so the walk stopped at {fetched} records \
             — treated as a TRUNCATED page, not the end of the corpus (a rate-limited or \
             partially-served page looks exactly like a genuine tail)"
        )),
        SweepEnd::UnknownTotal => Some(format!(
            "coverage unproven: search2 served {fetched} records over {pages} page(s) while \
             reporting hitCount:0 — the total is missing or renamed, so nothing can prove \
             the corpus was covered. The walk ran to a short page or maxPages={max_pages} \
             instead of trusting the total"
        )),
    }
}

/// Appends a warning to a result's `warnings` array (creating it if absent).
/// `UnifiedOutcome::merge_into` sets `warnings` to the drift warnings, so any
/// coverage warning must be pushed *after* the merge to survive.
fn append_warning(out: &mut Value, msg: String) {
    if let Value::Object(map) = out {
        match map.get_mut("warnings") {
            Some(Value::Array(w)) => w.push(json!(msg)),
            _ => {
                map.insert("warnings".into(), json!([msg]));
            }
        }
    }
}

/// Consecutive detail-stage failures after which the stage is treated as broken
/// rather than flaky. Five in a row is not bad luck: it is a fetchOpportunity
/// outage or a contract drift, and every remaining key would fail identically.
const DETAIL_CONSECUTIVE_FAILURE_ABORT: usize = 5;

/// How many failure reasons the result keeps verbatim. Enough to diagnose (the
/// message names the opportunity id and the drift), bounded so a wholly-broken
/// endpoint cannot turn the job result into a 50-entry error dump.
const MAX_STAGE_ERRORS: usize = 3;

/// Whether a run of back-to-back detail failures has proven the stage broken.
///
/// The anti-pattern this defends: burning the whole `maxDetailsPerRun` cap
/// against a dead endpoint, one governor-paced round-trip at a time, because
/// each individual failure was "non-fatal".
fn detail_stage_is_broken(consecutive_failures: usize) -> bool {
    consecutive_failures >= DETAIL_CONSECUTIVE_FAILURE_ABORT
}

/// Records a stage error, keeping at most [`MAX_STAGE_ERRORS`] verbatim.
/// The COUNT is kept separately and is never truncated — losing the tally is
/// what would turn a non-fatal stage into a silent one.
fn record_stage_error(errors: &mut Vec<String>, msg: String) {
    if errors.len() < MAX_STAGE_ERRORS {
        errors.push(msg);
    }
}

/// The human-readable warning for a degraded detail stage, or `None` when
/// nothing failed.
///
/// Non-fatal is not the same as fine: a caller reading only `warnings` must
/// still learn that the federal money join is running on a thinner corpus than
/// it should be.
fn detail_stage_degradation(
    failed: usize,
    attempted: usize,
    aborted: bool,
    errors: &[String],
) -> Option<String> {
    if failed == 0 {
        return None;
    }
    let tail = if aborted {
        format!(
            " — stopped after {DETAIL_CONSECUTIVE_FAILURE_ABORT} consecutive failures \
             (treated as an outage or contract drift, not flakiness)"
        )
    } else {
        String::new()
    };
    Some(format!(
        "detail harvest degraded: {failed} of {attempted} fetchOpportunity calls failed{tail}. \
         The listing sync is unaffected; federal award amounts will be missing for those \
         opportunities until a later run picks them up. First reasons: {}",
        errors.join(" | ")
    ))
}

/// The default for [`harvest_details_enabled`] when the param is absent.
///
/// **Stated once.** The crate used to state this default three times and
/// contradict itself twice: the module header and the manifest both said ON,
/// while the harvest site said `// default OFF` above an `unwrap_or(false)`.
/// The runtime default was therefore *false* and only `default_params` supplied
/// true, so a caller who built params by hand — the documented way to narrow a
/// pull — silently got a different pipeline from the one the scheduler runs,
/// with no warning and no field in the result to notice it by.
///
/// ON is the correct value: `grants/opportunity_details` is the only source of
/// federal award amounts in the product, the stage is delta-only, capped and
/// non-fatal, and the scheduled run has harvested since 2026-08-04.
const HARVEST_DETAILS_DEFAULT: bool = true;

/// Whether this run harvests NOFO details. The absent-param answer is
/// [`HARVEST_DETAILS_DEFAULT`], i.e. exactly what the scheduler's
/// `default_params` asks for.
fn harvest_details_enabled(params: &Value) -> bool {
    params
        .get("harvestDetails")
        .and_then(Value::as_bool)
        .unwrap_or(HARVEST_DETAILS_DEFAULT)
}

/// Whether a Search2 hit is a POSTED opportunity.
///
/// The anti-pattern: `is_none_or(|s| s == "posted")` read a hit carrying **no**
/// `oppStatus` as posted. The pinned Search2 contract says every hit carries one
/// (`id, number, title, agencyCode, agency, openDate, closeDate, oppStatus,
/// docType, cfdaList`), so an absent status is drift — and under a wholesale
/// rename the "posted-only" digest would have published the entire *forecasted*
/// corpus as closing-soon alerts. Absence is not a posting; it is handled by
/// [`digest_status_drift`].
fn is_posted_hit(hit: &Value) -> bool {
    hit.get("oppStatus")
        .and_then(Value::as_str)
        .is_some_and(|s| s.trim().eq_ignore_ascii_case("posted"))
}

/// The warning for a digest whose status filter has gone blind: hits were served
/// and not one of them carries an `oppStatus`.
///
/// Refusing to read an absent status as `posted` is only safe while the blind
/// case is LOUD — otherwise a renamed field turns the digest silently empty, and
/// a quiet fortnight of deadlines looks exactly the same.
fn digest_status_drift(hits: &[Value]) -> Option<String> {
    let blind = !hits.is_empty()
        && hits
            .iter()
            .all(|h| h.get("oppStatus").and_then(Value::as_str).is_none());
    blind.then(|| {
        format!(
            "closing-soon digest is blind: none of the {} hits carries an `oppStatus`, so the \
             posted-only filter matched nothing. The listing sync is unaffected; treat the \
             empty digest as a renamed status field, not as a fortnight with no deadlines",
            hits.len()
        )
    })
}

/// Days left for a hit that belongs in the closing-soon digest, or `None` when
/// it does not.
///
/// The anti-pattern this closes: the digest filtered on
/// `chrono::Utc::now().date_naive()` while `is_past_due_open` (the unified
/// sweep) and `GET /grants/closing-soon` judge the SAME row against
/// [`grants_common::deadline_end_utc`] — `D+1T12:00:00Z`, the moment the printed
/// date is over anywhere on Earth. For the ~12 hours between those two instants
/// a grant was open in `grants/unified` and on the cross-source closing-soon
/// view while being absent from this job's own digest. `grants-common` names
/// that exact class as a bug it already fixed for the sweep; the digest was
/// never brought along.
///
/// `days_left` is floored at 0: a grant in the anywhere-on-Earth tail has a
/// printed date in the past and a deadline that has not lapsed, so it is
/// claimable TODAY — which is what an alert consumer acts on. The raw
/// `closeDate` travels in the same entry, so nothing is hidden.
fn digest_days_left(
    close_raw: &str,
    window_days: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<i64> {
    // Search2 publishes `MM/DD/YYYY` with no timezone, so `close_at` is always
    // absent here and this always takes the conservative anywhere-on-Earth arm —
    // the SAME arm the sweep takes for these rows.
    if now > grants_common::deadline_end_utc(Some(close_raw), None)? {
        return None;
    }
    let close = grants_common::parse_date(close_raw)?;
    let days_left = (close - now.date_naive()).num_days().max(0);
    (days_left <= window_days).then_some(days_left)
}

/// Posted opportunities closing within `days` days, sorted soonest-first.
/// Each entry keeps just what an alert needs: id, number, title, agency,
/// close date, and days left. `now` is a parameter so the two boundary classes
/// this digest gets wrong when it drifts — the anywhere-on-Earth tail and the
/// far edge of the window — are testable without waiting for a clock.
fn closing_soon_digest(
    hits: &[Value],
    days: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> Vec<Value> {
    let mut digest: Vec<(i64, Value)> = hits
        .iter()
        .filter(|h| is_posted_hit(h))
        .filter_map(|h| {
            let close_raw = h.get("closeDate").and_then(Value::as_str)?;
            let days_left = digest_days_left(close_raw, days, now)?;
            let close = grants_common::parse_date(close_raw)?;
            Some((
                days_left,
                json!({
                    "id": h.get("id"),
                    "number": h.get("number"),
                    "title": h.get("title"),
                    "agency": h.get("agency").or_else(|| h.get("agencyCode")),
                    "closeDate": close.to_string(),
                    "daysLeft": days_left,
                }),
            ))
        })
        .collect();
    digest.sort_by_key(|(days_left, _)| *days_left);
    digest.into_iter().map(|(_, v)| v).collect()
}

/// A POST request to the Search2 endpoint carrying a JSON body. The API is
/// POST-only (a bare GET is 403), so this can't use `HttpRequest::get`.
fn search2_request(body: String) -> HttpRequest {
    post_json(SEARCH2_URL, body)
}

const FETCH_OPPORTUNITY_URL: &str = "https://api.grants.gov/v1/api/fetchOpportunity";
/// ASSUMED attachment download endpoint (see crate doc header) — stored as
/// metadata for a later fetch pass, never fetched in v1.
const ATTACHMENT_DOWNLOAD_BASE: &str =
    "https://apply07.grants.gov/grantsws/rest/opportunity/att/download";

/// Detail records buffered before a dataset flush. The flush is what makes a
/// key durable, so this also bounds how much work a crash can cost (one
/// governor-paced fetch each).
const DETAIL_FLUSH: usize = 25;

/// Checkpoint schema version — a stored snapshot from a different shape is
/// treated as "start fresh", never as an error (the sink is advisory).
const HARVEST_STATE_VERSION: u64 = 1;

/// A prior attempt's detail-harvest progress, as restored from the checkpoint.
struct HarvestState {
    delta: Vec<String>,
    done: Vec<String>,
    capped: bool,
    delta_total: usize,
}

/// The checkpoint payload: the delta this run committed to harvesting, and how
/// far it got. Small by construction — ids only, never record bodies.
fn harvest_state(
    delta: &[String],
    done: &HashSet<String>,
    capped: bool,
    delta_total: usize,
) -> Value {
    // Sorted so the snapshot is stable across flushes (a HashSet is not).
    let mut done: Vec<&String> = done.iter().collect();
    done.sort();
    json!({
        "v": HARVEST_STATE_VERSION,
        "stage": "details",
        "delta": delta,
        "done": done,
        "capped": capped,
        "deltaTotal": delta_total,
    })
}

/// Reads a restored checkpoint back, tolerating ANY stored shape: a version
/// bump, a missing field, or a snapshot with no remaining delta all mean
/// "start fresh" rather than a failed run.
fn restored_harvest(state: Option<&Value>) -> Option<HarvestState> {
    let state = state?;
    if state.get("v").and_then(Value::as_u64) != Some(HARVEST_STATE_VERSION)
        || state.get("stage").and_then(Value::as_str) != Some("details")
    {
        return None;
    }
    let strings = |key: &str| -> Vec<String> {
        state
            .get(key)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default()
    };
    let delta = strings("delta");
    if delta.is_empty() {
        return None;
    }
    Some(HarvestState {
        done: strings("done"),
        capped: state
            .get("capped")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        delta_total: state
            .get("deltaTotal")
            .and_then(Value::as_u64)
            .unwrap_or(delta.len() as u64) as usize,
        delta,
    })
}

/// The one field of a detail record that is a fact about THIS MACHINE rather
/// than about the opportunity: when we last fetched it. Declared derived, so it
/// is stored and readable but excluded from the change-detection hash.
const HARVESTED_AT_FIELD: &str = "harvested_at";

/// Record paths the detail write declares **derived** — see [`DerivedPaths`].
///
/// The anti-pattern this closes: `detail_record` stamps
/// `harvested_at: ts(Utc::now())` into every record, and change detection
/// hashes the whole value, so re-harvesting a **byte-identical**
/// `fetchOpportunity` body wrote a new revision and read `changed`.
/// `grants/opportunity_details` is the only source of federal award amounts in
/// the product and it is genuinely watchable (`grants` is a registered virtual
/// namespace with grants-gov as a publisher), so every notification it could
/// ever send was noise about our own clock.
///
/// Declaring it excludes it from the hash **only**: the stored record and every
/// revision still carry the timestamp, and a derived-only movement still
/// rewrites the record body (so "when did we last touch this" stays fresh) —
/// it just appends no revision.
///
/// One-time cost: the first run after deploy re-hashes every stored detail
/// record, so up to the whole detail corpus reports `changed` once and then
/// settles. Given they reported `changed` on every harvest before, that is a
/// strict improvement from run two onwards.
fn derived_paths() -> DerivedPaths {
    DerivedPaths::new([HARVESTED_AT_FIELD])
}

/// Writes the buffered detail records and marks their keys done. Stamped with
/// the fetchOpportunity endpoint the whole buffer was fetched from (M12) —
/// per-record bodies differ only by the POSTed id, so the URL is shared and
/// honest — and declaring [`derived_paths`] so our own harvest clock is not
/// mistaken for news about the opportunity.
async fn flush_details(
    ctx: &AppContext,
    buffer: &mut Vec<(String, Value)>,
    done: &mut HashSet<String>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    ctx.datasets
        .upsert_many_derived(
            grants_common::UNIFIED_APP,
            grants_common::DETAILS_DATASET,
            buffer,
            None,
            Some(&Provenance {
                job_id: Some(ctx.job_id.to_string()),
                source_url: Some(FETCH_OPPORTUNITY_URL.to_string()),
                ..Provenance::default()
            }),
            &derived_paths(),
        )
        .await?;
    for (key, _) in buffer.drain(..) {
        done.insert(key);
    }
    Ok(())
}

/// The new-then-changed delta keys the detail harvest will fetch, honestly
/// capped: returns (keys to fetch, whether the cap truncated the delta).
/// New opportunities go first — a brand-new NOFO is worth more than a
/// re-touched one when the cap forces a choice.
fn capped_delta(new: &[String], changed: &[String], cap: usize) -> (Vec<String>, bool) {
    let total = new.len() + changed.len();
    let keys: Vec<String> = new
        .iter()
        .chain(changed.iter())
        .take(cap)
        .cloned()
        .collect();
    (keys, total > cap)
}

/// One fetchOpportunity call, defensively parsed against the ASSUMED contract
/// in the crate doc header. Any drift — HTTP failure, non-JSON, application
/// errorcode, or an unrecognizable `data` object — is a loud run failure,
/// never a silently-empty detail record. The first raw response is kept as the
/// `detail1.json` artifact for exactly that first-live-run verification.
async fn fetch_detail(ctx: &AppContext, opp_id: &str, keep_artifact: bool) -> Result<Value> {
    // The Search2 hit id looks numeric; send it as a JSON number when it is,
    // raw string otherwise (contract assumption — see doc header).
    let id_value = opp_id
        .parse::<u64>()
        .map(Value::from)
        .unwrap_or_else(|_| Value::from(opp_id));
    let body = json!({ "opportunityId": id_value }).to_string();
    let resp = ctx
        .engines
        .http
        .fetch(post_json(FETCH_OPPORTUNITY_URL, body))
        .await?;
    if !resp.is_success() {
        return Err(Error::App(format!(
            "grants.gov fetchOpportunity({opp_id}) returned status {} (body starts: {})",
            resp.status,
            resp.body.chars().take(180).collect::<String>()
        )));
    }
    let parsed: Value = serde_json::from_str(&resp.body).map_err(|e| {
        Error::App(format!(
            "grants.gov fetchOpportunity({opp_id}): response was not JSON: {e}"
        ))
    })?;
    if keep_artifact {
        ctx.save_artifact("detail1.json", &serde_json::to_vec_pretty(&parsed)?)
            .await?;
    }
    if let Some(reason) = envelope_error(&parsed) {
        return Err(Error::App(format!(
            "grants.gov fetchOpportunity({opp_id}) {reason}"
        )));
    }
    extract_detail(&parsed).cloned().ok_or_else(|| {
        Error::App(format!(
            "grants.gov fetchOpportunity({opp_id}) schema drift: `data` is missing or \
             not a recognizable opportunity-detail object — the ASSUMED contract in the \
             crate doc header does not match (detail1.json artifact holds the raw body)"
        ))
    })
}

/// Locates the detail object in a fetchOpportunity envelope, or `None` when the
/// shape drifted. A recognizable detail is an object under `data` carrying a
/// `synopsis` or `forecast` block, or at least its own id/opportunityNumber.
fn extract_detail(parsed: &Value) -> Option<&Value> {
    let data = parsed.get("data")?;
    if !data.is_object() {
        return None;
    }
    let recognizable = data.get("synopsis").is_some_and(Value::is_object)
        || data.get("forecast").is_some_and(Value::is_object)
        || data.get("id").is_some()
        || data.get("opportunityNumber").is_some();
    recognizable.then_some(data)
}

/// The stored `grants/opportunity_details` record: full synopsis/forecast,
/// attachment manifest (URLs + metadata, NO document bodies in v1), and the
/// structured requirements block — plus enough identity (number, title, agency,
/// unified_key) to join against `opportunities` and `grants/unified`.
fn detail_record(opp_id: &str, hit: Option<&Value>, detail: &Value) -> Value {
    let from_hit = |f: &str| hit.and_then(|h| h.get(f)).cloned().unwrap_or(Value::Null);
    let pick = |detail_field: &str, hit_field: &str| {
        detail
            .get(detail_field)
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| from_hit(hit_field))
    };
    let synopsis = detail
        .get("synopsis")
        .or_else(|| detail.get("forecast"))
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "opportunity_id": opp_id,
        "unified_key": format!("grants-gov:{opp_id}"),
        "number": pick("opportunityNumber", "number"),
        "title": pick("opportunityTitle", "title"),
        "agency": pick("agencyName", "agency"),
        "synopsis": synopsis,
        "attachments": attachment_manifest(detail),
        "requirements": requirements_block(detail),
        // Declared DERIVED (see `derived_paths`): stored and readable, but out
        // of the change-detection hash — our fetch clock is not news about the
        // opportunity.
        HARVESTED_AT_FIELD: pumper_core::datasets::ts(chrono::Utc::now()),
    })
}

/// Structured requirements block from the SYNOPSIS FIELDS ONLY (v1) — no PDF
/// text is fetched or parsed. Money follows the shared honest-Null rule via
/// `grants_common::money_scalar` ($0/prose/absent → Null, never fabricated);
/// forecasted opportunities read the same fields from their `forecast` block.
fn requirements_block(detail: &Value) -> Value {
    let null = Value::Null;
    let syn = detail
        .get("synopsis")
        .or_else(|| detail.get("forecast"))
        .unwrap_or(&null);
    let close_date = syn
        .get("responseDate")
        .and_then(Value::as_str)
        .and_then(grants_common::parse_date)
        .map(|d| Value::String(d.to_string()))
        .unwrap_or(Value::Null);
    json!({
        "cost_sharing": cost_sharing_flag(syn.get("costSharing")),
        "award_floor": grants_common::money_scalar(syn, &["awardFloor"]),
        "award_ceiling": grants_common::money_scalar(syn, &["awardCeiling"]),
        "estimated_total_funding": grants_common::money_scalar(syn, &["estimatedFunding"]),
        // Live-verified 2026-07-30: the real synopsis key is `numberOfAwards`;
        // `expectedNumberOfAwards` never appears (kept as fallback for drift).
        "expected_awards": count_value(syn.get("numberOfAwards").or_else(|| syn.get("expectedNumberOfAwards"))),
        "eligibility_text": syn
            .get("applicantEligibilityDesc")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Value::String(s.to_string()))
            .unwrap_or(Value::Null),
        "applicant_types": applicant_types(syn.get("applicantTypes")),
        "close_date": close_date,
    })
}

/// costSharing arrives as a bool or a "Yes"/"No" string; anything else is
/// honestly unknown (Null), never defaulted.
fn cost_sharing_flag(v: Option<&Value>) -> Value {
    match v {
        Some(Value::Bool(b)) => Value::Bool(*b),
        Some(Value::String(s)) => match s.trim().to_lowercase().as_str() {
            "yes" | "y" | "true" => Value::Bool(true),
            "no" | "n" | "false" => Value::Bool(false),
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

/// A count field that may be a number or a numeric string → integer, else Null.
fn count_value(v: Option<&Value>) -> Value {
    match v {
        Some(Value::Number(n)) => n.as_u64().map(Value::from).unwrap_or(Value::Null),
        Some(Value::String(s)) => s
            .trim()
            .replace(',', "")
            .parse::<u64>()
            .map(Value::from)
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// applicantTypes[] entries are objects with a `description` (or `value`) or
/// bare strings → a flat array of non-empty strings.
///
/// **Absent is not empty.** A payload that carries no `applicantTypes` (or a
/// drifted non-array one) yields `Null`, exactly as the money fields in the same
/// synopsis block do via `money_scalar`. `[]` is reserved for the agency
/// genuinely publishing an empty list, so a consumer can tell "this NOFO lists
/// no eligible applicant types" from "the field was renamed" — which the old
/// `_ => Vec::new()` arm made indistinguishable, on the only dataset that
/// carries federal eligibility at all.
fn applicant_types(v: Option<&Value>) -> Value {
    let Some(Value::Array(a)) = v else {
        return Value::Null;
    };
    Value::Array(
        a.iter()
            .filter_map(|e| match e {
                Value::String(s) => Some(s.trim().to_string()),
                Value::Object(_) => e
                    .get("description")
                    .or_else(|| e.get("value"))
                    .and_then(Value::as_str)
                    .map(|s| s.trim().to_string()),
                _ => None,
            })
            .filter(|s| !s.is_empty())
            .map(Value::String)
            .collect::<Vec<Value>>(),
    )
}

/// Flattens the NOFO attachment manifest: `synopsisAttachmentFolders[]` (each
/// with nested `synopsisAttachments[]`) plus a flat `attachments[]` fallback.
/// Every entry keeps id, file name/description, mime type, size, its folder,
/// and the ASSUMED download URL (constructed only when an id exists) — enough
/// for a later PDF pass to fetch, nothing fetched now.
///
/// **Absent is not empty**, same rule as [`applicant_types`]: when the payload
/// carries NEITHER attachment block the answer is `Null`, because "this
/// announcement published no documents" and "the attachment block we only ever
/// ASSUMED was renamed" are different facts — and the whole point of storing the
/// manifest is that a later pass can fetch those documents. `[]` means the block
/// was there and held nothing.
fn attachment_manifest(detail: &Value) -> Value {
    let folders = detail
        .get("synopsisAttachmentFolders")
        .and_then(Value::as_array);
    let flat = detail.get("attachments").and_then(Value::as_array);
    if folders.is_none() && flat.is_none() {
        return Value::Null;
    }
    let mut out = Vec::new();
    let mut push = |att: &Value, folder: Option<&str>| {
        let id = att.get("id").cloned().unwrap_or(Value::Null);
        let download_url = match &id {
            Value::Null => Value::Null,
            Value::String(s) => Value::String(format!("{ATTACHMENT_DOWNLOAD_BASE}/{s}")),
            other => Value::String(format!("{ATTACHMENT_DOWNLOAD_BASE}/{other}")),
        };
        out.push(json!({
            "id": id,
            "file_name": att.get("fileName").cloned().unwrap_or(Value::Null),
            "description": att.get("fileDescription").cloned().unwrap_or(Value::Null),
            "mime_type": att.get("mimeType").cloned().unwrap_or(Value::Null),
            "size_bytes": att.get("fileLobSize").cloned().unwrap_or(Value::Null),
            "folder": folder.map(|f| Value::String(f.to_string())).unwrap_or(Value::Null),
            "download_url": download_url,
        }));
    };
    for folder in folders.into_iter().flatten() {
        let name = folder
            .get("folderName")
            .or_else(|| folder.get("folderType"))
            .and_then(Value::as_str);
        if let Some(atts) = folder.get("synopsisAttachments").and_then(Value::as_array) {
            for att in atts {
                push(att, name);
            }
        }
    }
    for att in flat.into_iter().flatten() {
        push(att, None);
    }
    Value::Array(out)
}

/// A POST request carrying a JSON body to a grants.gov API endpoint.
fn post_json(url: &str, body: String) -> HttpRequest {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("Accept".to_string(), "application/json".to_string());
    HttpRequest {
        url: url.to_string(),
        method: HttpMethod::Post,
        headers,
        body: Some(body),
        no_cache: false,
        ttl_override: None,
        etag: None,
        if_modified_since: None,
        max_body_bytes: None,
        timeout_secs: None,
        proxy: None,
        profile: None,
        archive_max_age: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A UTC instant on a fixed day, so the digest's two boundary classes are
    /// testable without waiting for a clock.
    fn at(date: &str, hour: u32) -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;
        chrono::Utc.from_utc_datetime(
            &chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
                .unwrap()
                .and_hms_opt(hour, 0, 0)
                .unwrap(),
        )
    }

    #[test]
    fn digest_keeps_only_posted_opps_closing_within_window() {
        let now = at("2026-08-13", 9);
        let today = now.date_naive();
        let fmt = |n: i64| {
            (today + chrono::Duration::days(n))
                .format("%m/%d/%Y")
                .to_string()
        };
        let (soon, far, long_past) = (fmt(3), fmt(90), fmt(-5));
        let hits = vec![
            json!({ "id": "1", "title": "in window", "oppStatus": "posted", "closeDate": soon }),
            json!({ "id": "2", "title": "too far", "oppStatus": "posted", "closeDate": far }),
            json!({ "id": "3", "title": "lapsed", "oppStatus": "posted", "closeDate": long_past }),
            json!({ "id": "4", "title": "forecasted", "oppStatus": "forecasted", "closeDate": soon }),
            json!({ "id": "5", "title": "no close date", "oppStatus": "posted" }),
        ];
        let digest = closing_soon_digest(&hits, 14, now);
        assert_eq!(digest.len(), 1);
        assert_eq!(digest[0]["id"], "1");
        assert_eq!(digest[0]["daysLeft"], 3);
    }

    #[test]
    fn digest_sorts_soonest_first_and_tolerates_iso_dates() {
        let now = at("2026-08-13", 9);
        let today = now.date_naive();
        let d = |n: i64, iso: bool| {
            let date = today + chrono::Duration::days(n);
            if iso {
                date.format("%Y-%m-%d").to_string()
            } else {
                date.format("%m/%d/%Y").to_string()
            }
        };
        let hits = vec![
            json!({ "id": "a", "oppStatus": "posted", "closeDate": d(10, false) }),
            json!({ "id": "b", "oppStatus": "POSTED", "closeDate": d(2, true) }),
            json!({ "id": "c", "oppStatus": " posted ", "closeDate": d(5, false) }),
        ];
        let digest = closing_soon_digest(&hits, 14, now);
        let ids: Vec<&str> = digest.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["b", "c", "a"]);
    }

    #[test]
    fn a_grant_open_in_unified_is_not_missing_from_this_jobs_own_digest() {
        // THE anti-pattern (D6/C6). `closeDate: 08/12/2026` lapses at
        // 2026-08-13T12:00:00Z (anywhere-on-Earth), which is what the unified
        // sweep and `GET /grants/closing-soon` both use. The digest compared the
        // date against `Utc::now().date_naive()`, so from 00:00Z to 12:00Z on
        // the 13th the grant was OPEN in `grants/unified` and absent here.
        let hit = json!({ "id": "1", "oppStatus": "posted", "closeDate": "08/12/2026" });
        let hits = vec![hit];

        let morning = at("2026-08-13", 9);
        assert!(
            grants_common::deadline_end_utc(Some("08/12/2026"), None)
                .is_some_and(|end| morning < end),
            "the sweep still considers this row open at 09:00Z"
        );
        let digest = closing_soon_digest(&hits, 14, morning);
        assert_eq!(digest.len(), 1, "and so must the digest: {digest:?}");
        // Floored at 0 rather than reported as -1: it is claimable TODAY, which
        // is the only thing an alert consumer can act on. The printed date
        // travels alongside.
        assert_eq!(digest[0]["daysLeft"], json!(0));
        assert_eq!(digest[0]["closeDate"], json!("2026-08-12"));

        // Past the anywhere-on-Earth end, both surfaces agree it is over.
        let afternoon = at("2026-08-13", 13);
        assert!(closing_soon_digest(&hits, 14, afternoon).is_empty());
    }

    #[test]
    fn a_hit_with_no_opp_status_is_not_read_as_posted_and_the_blindness_is_loud() {
        let now = at("2026-08-13", 9);
        let soon = (now.date_naive() + chrono::Duration::days(3))
            .format("%m/%d/%Y")
            .to_string();
        assert!(is_posted_hit(&json!({ "oppStatus": "posted" })));
        assert!(is_posted_hit(&json!({ "oppStatus": " Posted " })));
        assert!(!is_posted_hit(&json!({ "oppStatus": "forecasted" })));
        // The whole point: absent is NOT posted. Under a wholesale rename the
        // old `is_none_or` would have published the forecasted corpus as
        // closing-soon alerts.
        assert!(!is_posted_hit(&json!({ "id": "1" })));
        assert!(!is_posted_hit(&json!({ "oppStatus": Value::Null })));

        let blind = vec![json!({ "id": "1", "closeDate": soon })];
        assert!(closing_soon_digest(&blind, 14, now).is_empty());
        // …and refusing it is only safe because the blind case says so.
        let msg = digest_status_drift(&blind).expect("a blind digest warns");
        assert!(msg.contains("closing-soon digest is blind"), "{msg}");
        assert!(msg.contains("renamed status field"), "{msg}");
        // A healthy batch is silent, and so is an empty one (nothing was served,
        // so nothing went blind).
        assert!(digest_status_drift(&[json!({ "oppStatus": "forecasted" })]).is_none());
        assert!(digest_status_drift(&[]).is_none());
    }

    #[test]
    fn an_absent_harvest_details_is_the_scheduled_default_not_off() {
        // The anti-pattern: the runtime default (`unwrap_or(false)`) disagreed
        // with the declared one (`default_params` → true), so a caller who built
        // params by hand silently ran a different pipeline from the scheduler's.
        assert!(harvest_details_enabled(&json!({})));
        assert!(harvest_details_enabled(
            &json!({ "keyword": "rural health" })
        ));
        // An explicit value still wins, in both directions.
        assert!(harvest_details_enabled(&json!({ "harvestDetails": true })));
        assert!(!harvest_details_enabled(
            &json!({ "harvestDetails": false })
        ));
        // A non-boolean is not a decision — it falls back to the one default.
        assert_eq!(
            harvest_details_enabled(&json!({ "harvestDetails": "yes" })),
            HARVEST_DETAILS_DEFAULT
        );
        // The default is stated ONCE: the scheduler's params and the
        // absent-param path read the same constant.
        assert_eq!(
            GrantsGov.default_params()["harvestDetails"],
            json!(HARVEST_DETAILS_DEFAULT)
        );
    }

    // ---- sweep honesty: only a proven walk reads as a complete corpus ----

    #[test]
    fn a_renamed_hit_count_does_not_prove_a_one_page_corpus() {
        // THE anti-pattern. `hitCount` renamed → `unwrap_or(0)` → the old
        // `start >= hit_count` was `1000 >= 0` after page 1, so the walk broke
        // immediately, `truncated` was false, and the drift guard (gated on
        // `hit_count > 0`) never fired: the corpus capped at one page, green,
        // indefinitely.
        let full_page = walk_end(1, 1000, 0, 1000, 1000, 25);
        assert_eq!(
            full_page, None,
            "a full page under a zero total keeps walking"
        );
        // …and when the walk does end, it ends UNPROVEN, never complete.
        assert_eq!(
            walk_end(2, 1000, 0, 400, 1400, 25),
            Some(SweepEnd::UnknownTotal)
        );
        assert_eq!(
            walk_end(25, 1000, 0, 1000, 25_000, 25),
            Some(SweepEnd::UnknownTotal)
        );
        // A zero total with zero hits is self-consistent — an honestly empty
        // result set IS fully swept (drift is judged against the STORED corpus,
        // not against this same response).
        assert_eq!(walk_end(1, 1000, 0, 0, 0, 25), Some(SweepEnd::Complete));
    }

    #[test]
    fn a_short_page_is_not_the_end_of_the_corpus() {
        // 1366 records, 1000-row pages: page 2 rate-limited to 100 hits used to
        // stop the walk at 1100 records with `truncated: false`. Note that
        // cordis's position arithmetic (`2 * 1000 >= 1366`) calls this COMPLETE
        // — counting records collected is what makes the arm visible.
        assert_eq!(
            walk_end(2, 1000, 1366, 100, 1100, 25),
            Some(SweepEnd::ShortPage)
        );
        // The genuine tail — the records collected cover the total — is
        // complete even though that page is also short.
        assert_eq!(
            walk_end(2, 1000, 1366, 366, 1366, 25),
            Some(SweepEnd::Complete)
        );
        // Mid-sweep `oppHits` drop on a page the arithmetic puts inside the
        // corpus is drift, judged before the walk ever classifies it.
        assert!(empty_page_is_drift(2, 1000, 1366, 0));
        // Page 1 evaluates exactly as the old post-loop `hits.is_empty()` guard.
        assert!(empty_page_is_drift(1, 1000, 1366, 0));
        // A page past the end of a shrunken listing is the ordinary end, not
        // drift — and neither is an empty page with no total to contradict.
        assert!(!empty_page_is_drift(3, 1000, 1366, 0));
        assert!(!empty_page_is_drift(1, 1000, 0, 0));
    }

    #[test]
    fn the_page_cap_and_the_proven_end_are_different_endings() {
        // Cap reached with records left: capped, never complete.
        assert_eq!(
            walk_end(25, 1000, 100_000, 1000, 25_000, 25),
            Some(SweepEnd::Capped)
        );
        // Cap reached exactly ON the proven end: proof outranks the cap.
        assert_eq!(
            walk_end(2, 1000, 1500, 500, 1500, 2),
            Some(SweepEnd::Complete)
        );
        // Mid-walk with everything healthy: keep going.
        assert_eq!(walk_end(1, 1000, 5000, 1000, 1000, 25), None);
    }

    #[test]
    fn sweep_warnings_name_the_arm_they_came_from() {
        assert!(sweep_warning(SweepEnd::Complete, 2, 25, 1000, 1366, 1366).is_none());
        let capped = sweep_warning(SweepEnd::Capped, 25, 25, 1000, 100_000, 25_000).unwrap();
        assert!(capped.contains("maxPages=25"), "{capped}");
        let short = sweep_warning(SweepEnd::ShortPage, 2, 25, 1000, 1366, 1100).unwrap();
        assert!(short.contains("TRUNCATED page"), "{short}");
        assert!(short.contains("not the end of the corpus"), "{short}");
        let unproven = sweep_warning(SweepEnd::UnknownTotal, 3, 25, 1000, 0, 2400).unwrap();
        assert!(unproven.contains("hitCount:0"), "{unproven}");
        assert!(unproven.contains("coverage unproven"), "{unproven}");
    }

    #[test]
    fn an_empty_listing_over_a_stored_corpus_is_drift_but_a_narrowed_pull_is_not() {
        // Query-grammar drift: nothing returned for the whole corpus while 1366
        // rows are stored. This used to be a perfect run.
        assert!(empty_listing_is_drift(0, 0, 1366, true));
        // First run ever — nothing stored, nothing returned: honestly empty.
        assert!(!empty_listing_is_drift(0, 0, 0, true));
        // A targeted pull (the manifest's own second example) may legitimately
        // match nothing while the corpus is full. Failing those would make the
        // guard unusable.
        assert!(!empty_listing_is_drift(0, 0, 1366, false));
        // Anything actually fetched is not an empty listing.
        assert!(!empty_listing_is_drift(0, 5, 1366, true));
        assert!(whole_corpus_query("", ""));
        assert!(whole_corpus_query("  ", " "));
        assert!(!whole_corpus_query("rural health", ""));
        assert!(!whole_corpus_query("", "12|13"));
    }

    #[test]
    fn an_unreadable_errorcode_is_not_success() {
        // The anti-pattern: `as_i64().unwrap_or(0)` made every unreadable
        // status a pass.
        assert!(envelope_error(&json!({ "errorcode": 0, "data": {} })).is_none());
        // JSON-type drift only — the same integer, sent as a string.
        assert!(envelope_error(&json!({ "errorcode": "0" })).is_none());
        assert!(envelope_error(&json!({ "errorcode": " 0 " })).is_none());
        // A real application error still names itself.
        let err = envelope_error(&json!({ "errorcode": 1, "msg": "bad query" })).unwrap();
        assert!(err.contains("error code 1"), "{err}");
        assert!(err.contains("bad query"), "{err}");
        assert!(envelope_error(&json!({ "errorcode": "12" })).is_some());
        // Absent / null / unreadable: refused, and the reason says what arrived.
        for envelope in [
            json!({ "data": {} }),
            json!({ "errorcode": Value::Null }),
            json!({ "errorcode": { "code": 0 } }),
            json!({ "errorcode": "ok" }),
        ] {
            let err = envelope_error(&envelope)
                .unwrap_or_else(|| panic!("{envelope} must not read as success"));
            assert!(err.contains("no readable `errorcode`"), "{err}");
        }
    }

    // ---- NOFO detail harvest ----

    /// A representative fetchOpportunity `data` object per the ASSUMED contract.
    fn sample_detail() -> serde_json::Value {
        json!({
            "id": 356037,
            "opportunityNumber": "TEST-24-001",
            "opportunityTitle": "Rural Health Detail",
            "agencyName": "Health and Human Services",
            "synopsis": {
                "synopsisDesc": "<p>Long announcement body…</p>",
                "applicantTypes": [
                    { "id": "12", "description": "Nonprofits with 501(c)(3)" },
                    "Tribal Governments"
                ],
                "applicantEligibilityDesc": "  Applicants must serve rural counties.  ",
                "costSharing": "No",
                "awardFloor": "$100,000",
                "awardCeiling": 750000,
                "estimatedFunding": "$5,000,000",
                "expectedNumberOfAwards": "7",
                "responseDate": "08/15/2026"
            },
            "synopsisAttachmentFolders": [
                {
                    "folderName": "Full Announcement",
                    "synopsisAttachments": [
                        {
                            "id": 999001,
                            "fileName": "NOFO.pdf",
                            "fileDescription": "Full announcement",
                            "mimeType": "application/pdf",
                            "fileLobSize": 123456
                        }
                    ]
                }
            ]
        })
    }

    #[test]
    fn requirements_block_extracts_structured_fields_from_synopsis() {
        let req = requirements_block(&sample_detail());
        assert_eq!(req["cost_sharing"], json!(false));
        // Money via the shared parser: "$"-string and JSON number both land.
        assert_eq!(req["award_floor"], json!(100_000.0));
        assert_eq!(req["award_ceiling"], json!(750_000.0));
        assert_eq!(req["estimated_total_funding"], json!(5_000_000.0));
        assert_eq!(req["expected_awards"], json!(7));
        assert_eq!(
            req["eligibility_text"],
            json!("Applicants must serve rural counties.")
        );
        assert_eq!(
            req["applicant_types"],
            json!(["Nonprofits with 501(c)(3)", "Tribal Governments"])
        );
        // responseDate normalized to canonical ISO.
        assert_eq!(req["close_date"], json!("2026-08-15"));
    }

    #[test]
    fn requirements_block_is_honest_null_when_fields_absent_or_prose() {
        // Forecast fallback: a forecasted opportunity carries `forecast`, and
        // its money/count fields may be prose, $0, or missing → all Null.
        let detail = json!({
            "id": 1,
            "forecast": {
                "costSharing": "To be determined",
                "awardFloor": "$0",
                "estimatedFunding": "Dependent on appropriations",
                "expectedNumberOfAwards": "several",
                "responseDate": "TBD"
            }
        });
        let req = requirements_block(&detail);
        assert_eq!(req["cost_sharing"], Value::Null);
        assert_eq!(req["award_floor"], Value::Null);
        assert_eq!(req["award_ceiling"], Value::Null);
        assert_eq!(req["estimated_total_funding"], Value::Null);
        assert_eq!(req["expected_awards"], Value::Null);
        assert_eq!(req["eligibility_text"], Value::Null);
        // Absent is Null, not a fabricated empty list — the same rule the money
        // fields two lines up already follow.
        assert_eq!(req["applicant_types"], Value::Null);
        assert_eq!(req["close_date"], Value::Null);
        // No synopsis/forecast at all: still a fully-Null block, never a panic.
        let req = requirements_block(&json!({ "id": 2 }));
        assert_eq!(req["award_ceiling"], Value::Null);
        assert_eq!(req["cost_sharing"], Value::Null);
    }

    #[test]
    fn attachment_manifest_flattens_folders_with_urls_and_metadata() {
        let atts = attachment_manifest(&sample_detail());
        let atts = atts.as_array().expect("a published block is an array");
        assert_eq!(atts.len(), 1);
        assert_eq!(atts[0]["file_name"], json!("NOFO.pdf"));
        assert_eq!(atts[0]["folder"], json!("Full Announcement"));
        assert_eq!(atts[0]["mime_type"], json!("application/pdf"));
        assert_eq!(atts[0]["size_bytes"], json!(123456));
        assert_eq!(
            atts[0]["download_url"],
            json!("https://apply07.grants.gov/grantsws/rest/opportunity/att/download/999001")
        );
        // The flat `attachments[]` fallback works, and a missing id means no
        // fabricated download URL.
        let flat = attachment_manifest(&json!({
            "attachments": [{ "fileName": "guide.docx" }]
        }));
        let flat = flat.as_array().unwrap();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0]["file_name"], json!("guide.docx"));
        assert_eq!(flat[0]["download_url"], Value::Null);
        assert_eq!(flat[0]["folder"], Value::Null);
    }

    #[test]
    fn an_absent_attachment_block_is_null_not_an_empty_manifest() {
        // The anti-pattern (D6/C5): `[]` for "no attachment block at all" made
        // "this announcement published no documents" indistinguishable from
        // "the block we only ever ASSUMED was renamed" — on the one field whose
        // whole purpose is to let a later pass fetch those documents.
        assert_eq!(attachment_manifest(&json!({ "id": 1 })), Value::Null);
        // A drifted non-array block is likewise unknown, never empty.
        assert_eq!(
            attachment_manifest(&json!({ "synopsisAttachmentFolders": { "a": 1 } })),
            Value::Null
        );
        // …but a block that IS there and holds nothing is honestly empty.
        assert_eq!(
            attachment_manifest(&json!({ "synopsisAttachmentFolders": [] })),
            json!([])
        );
        assert_eq!(
            attachment_manifest(&json!({ "attachments": [] })),
            json!([])
        );
    }

    #[test]
    fn an_absent_applicant_types_is_null_not_an_empty_list() {
        // Same anti-pattern on the sibling field: a consumer must be able to
        // tell "no eligible applicant types are listed" from "`applicantTypes`
        // was renamed".
        assert_eq!(applicant_types(None), Value::Null);
        assert_eq!(applicant_types(Some(&Value::Null)), Value::Null);
        assert_eq!(applicant_types(Some(&json!("Nonprofits"))), Value::Null);
        // A published empty list stays an empty list.
        assert_eq!(applicant_types(Some(&json!([]))), json!([]));
        // …and the flattening itself is unchanged.
        assert_eq!(
            applicant_types(Some(&json!([
                { "id": "12", "description": " Nonprofits " },
                { "value": "Tribal" },
                "  ",
                42
            ]))),
            json!(["Nonprofits", "Tribal"])
        );
    }

    #[test]
    fn extract_detail_refuses_drifted_envelopes() {
        // Good: data with a synopsis.
        let ok = json!({ "errorcode": 0, "data": sample_detail() });
        assert!(extract_detail(&ok).is_some());
        // Good: forecast-only and id-only shapes are still recognizable.
        assert!(extract_detail(&json!({ "data": { "forecast": {} } })).is_some());
        assert!(extract_detail(&json!({ "data": { "id": 5 } })).is_some());
        // Drift: no data / null data / non-object data / unrecognizable object.
        assert!(extract_detail(&json!({ "errorcode": 0 })).is_none());
        assert!(extract_detail(&json!({ "data": Value::Null })).is_none());
        assert!(extract_detail(&json!({ "data": [1, 2] })).is_none());
        assert!(extract_detail(&json!({ "data": { "unexpected": true } })).is_none());
    }

    #[test]
    fn capped_delta_is_new_first_and_reports_truncation_honestly() {
        let new = vec!["n1".to_string(), "n2".to_string()];
        let changed = vec!["c1".to_string(), "c2".to_string()];
        // Under the cap: everything, new first, not capped.
        let (keys, capped) = capped_delta(&new, &changed, 10);
        assert_eq!(keys, vec!["n1", "n2", "c1", "c2"]);
        assert!(!capped);
        // Over the cap: truncated AND flagged — never a silent partial harvest.
        let (keys, capped) = capped_delta(&new, &changed, 3);
        assert_eq!(keys, vec!["n1", "n2", "c1"]);
        assert!(capped);
        // Exactly at the cap is not a truncation.
        let (_, capped) = capped_delta(&new, &changed, 4);
        assert!(!capped);
    }

    /// The declaration names exactly the volatile field the record builder
    /// writes. If those two ever drift apart the seam silently stops working,
    /// which is indistinguishable from it never having been added.
    #[test]
    fn the_derived_declaration_names_the_only_field_that_is_our_clock() {
        assert_eq!(derived_paths(), DerivedPaths::new([HARVESTED_AT_FIELD]));
        assert!(!derived_paths().is_empty());
        let rec = detail_record("356037", None, &sample_detail());
        assert!(
            rec.get(HARVESTED_AT_FIELD).is_some(),
            "declaring a path that is not written is a no-op: {rec}"
        );
        // Nothing that carries the SOURCE's own facts may be derived — deriving
        // `requirements` or `synopsis` would silence the award-amount signal
        // instead of the noise, which is the opposite failure and a far worse
        // one (this dataset is the only source of federal money).
        for source_fact in ["synopsis", "requirements", "attachments", "number", "title"] {
            assert_ne!(
                derived_paths(),
                DerivedPaths::new([HARVESTED_AT_FIELD, source_fact]),
                "'{source_fact}' is the source's news, never ours"
            );
        }
    }

    // ---- durable detail harvest (M23) ----

    #[test]
    fn harvest_state_round_trips_and_resumes_where_it_stopped() {
        let delta = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let done: HashSet<String> = ["b".to_string()].into_iter().collect();
        let snap = harvest_state(&delta, &done, true, 9);
        let restored = restored_harvest(Some(&snap)).expect("round-trips");
        assert_eq!(restored.delta, delta);
        assert_eq!(restored.done, vec!["b".to_string()]);
        assert!(restored.capped);
        assert_eq!(restored.delta_total, 9);
        // The remaining work is the delta minus what already landed — the whole
        // point: a re-claimed run must not re-fetch `b`.
        let remaining: Vec<&String> = restored
            .delta
            .iter()
            .filter(|k| !restored.done.contains(k))
            .collect();
        assert_eq!(remaining, vec!["a", "c"]);
    }

    #[test]
    fn restored_harvest_treats_any_foreign_shape_as_start_fresh() {
        assert!(restored_harvest(None).is_none());
        // Another app's / another version's snapshot.
        assert!(restored_harvest(Some(&json!({ "frontier": [] }))).is_none());
        assert!(restored_harvest(Some(
            &json!({ "v": 99, "stage": "details", "delta": ["a"] })
        ))
        .is_none());
        assert!(
            restored_harvest(Some(&json!({ "v": 1, "stage": "listing", "delta": ["a"] })))
                .is_none()
        );
        // A snapshot with nothing left to do is not a resume.
        assert!(
            restored_harvest(Some(&json!({ "v": 1, "stage": "details", "delta": [] }))).is_none()
        );
        // Missing optional fields default rather than failing.
        let partial = json!({ "v": 1, "stage": "details", "delta": ["a", "b"] });
        let restored = restored_harvest(Some(&partial)).expect("tolerated");
        assert!(restored.done.is_empty());
        assert!(!restored.capped);
        assert_eq!(restored.delta_total, 2);
    }

    #[test]
    fn detail_record_joins_identity_and_falls_back_to_the_hit() {
        let hit = json!({ "id": "356037", "number": "TEST-24-001", "title": "Hit Title",
                          "agency": "HHS" });
        let rec = detail_record("356037", Some(&hit), &sample_detail());
        assert_eq!(rec["opportunity_id"], json!("356037"));
        assert_eq!(rec["unified_key"], json!("grants-gov:356037"));
        // Detail fields win when present…
        assert_eq!(rec["title"], json!("Rural Health Detail"));
        assert_eq!(rec["agency"], json!("Health and Human Services"));
        assert!(rec["synopsis"].is_object());
        assert_eq!(rec["requirements"]["award_ceiling"], json!(750_000.0));
        assert_eq!(rec["attachments"].as_array().unwrap().len(), 1);
        // …and the Search2 hit fills the gaps when the detail omits them.
        let sparse = json!({ "id": 356037, "synopsis": {} });
        let rec = detail_record("356037", Some(&hit), &sparse);
        assert_eq!(rec["title"], json!("Hit Title"));
        assert_eq!(rec["number"], json!("TEST-24-001"));
        assert_eq!(rec["agency"], json!("HHS"));
    }

    // ---- the detail stage is non-fatal to the listing, but never silent (G-F) ----

    #[test]
    fn scheduled_run_harvests_details_so_federal_amounts_can_ever_appear() {
        // The overlay that fills `award_*` on federal unified rows reads the
        // detail corpus; with the harvest off by default that corpus never
        // grows on a scheduled run and `min_award` stays permanently empty over
        // the largest source. The schedule uses `default_params` verbatim.
        let p = GrantsGov.default_params();
        assert_eq!(p["harvestDetails"], json!(true));
        // Still delta-only and capped — enabling it must not turn the daily
        // sweep into 25k fetchOpportunity calls.
        assert_eq!(p["maxDetailsPerRun"], json!(50));
        assert!(GrantsGov.schedule().is_some());
    }

    #[test]
    fn drifted_detail_stage_degrades_loudly_it_does_not_fail_the_listing_sync() {
        // The anti-pattern in the name is the pair of failures this guards:
        // (a) a secondary enrichment stage taking the primary listing sync down
        // with it, and (b) "non-fatal" quietly becoming "invisible".

        // Nothing failed → no warning at all (silence is only correct here).
        assert!(detail_stage_degradation(0, 50, false, &[]).is_none());

        // A partial failure names the count, the denominator and the reason,
        // and says in words that the listing is unaffected.
        let errs = vec!["141593: grants.gov fetchOpportunity(141593) schema drift".to_string()];
        let msg = detail_stage_degradation(3, 50, false, &errs).expect("degradation is reported");
        assert!(msg.contains("3 of 50"), "{msg}");
        assert!(msg.contains("listing sync is unaffected"), "{msg}");
        assert!(msg.contains("schema drift"), "{msg}");
        assert!(!msg.contains("stopped after"), "{msg}");

        // A wholly-broken endpoint additionally says it stopped early, so a
        // short `attempted` is never mistaken for a small delta.
        let msg = detail_stage_degradation(5, 5, true, &errs).expect("degradation is reported");
        assert!(
            msg.contains("stopped after 5 consecutive failures"),
            "{msg}"
        );
    }

    #[test]
    fn broken_detail_stage_stops_early_instead_of_burning_the_whole_cap() {
        // Flaky is tolerated; broken is not paid for 50 times.
        assert!(!detail_stage_is_broken(0));
        assert!(!detail_stage_is_broken(4));
        assert!(detail_stage_is_broken(5));
        assert!(detail_stage_is_broken(50));

        // The verbatim-reason list is bounded, but the tally that drives
        // `detailsFailed` is counted separately and is never truncated.
        let mut errors = Vec::new();
        for i in 0..10 {
            record_stage_error(&mut errors, format!("e{i}"));
        }
        assert_eq!(errors.len(), MAX_STAGE_ERRORS);
        assert_eq!(errors[0], "e0");
    }

    #[test]
    fn detail_drift_check_stays_strict_under_the_non_fatal_stage() {
        // Making the STAGE non-fatal must not soften the CONTRACT check: a
        // drifted envelope is still an error (now counted, not swallowed), and
        // `extract_detail` still refuses to hand back an unrecognizable object.
        assert!(extract_detail(&json!({ "data": { "unexpected": true } })).is_none());
        assert!(extract_detail(&json!({ "data": Value::Null })).is_none());
        // …while a real envelope still passes, so the strictness is not blanket.
        assert!(extract_detail(&json!({ "errorcode": 0, "data": sample_detail() })).is_some());
    }

    /// A Search2 endpoint that always answers, paired with a fetchOpportunity
    /// endpoint that is broken in the given way — the shape of the outage this
    /// stage now has to survive.
    struct ScriptedGrantsGov {
        detail: DetailFailure,
    }

    #[derive(Clone, Copy)]
    enum DetailFailure {
        /// The envelope parses but `data` is unrecognizable — exactly what the
        /// UNCHANGED strict drift check in `extract_detail` refuses.
        Drift,
        /// The endpoint is down.
        Http500,
    }

    #[async_trait]
    impl pumper_core::HttpClient for ScriptedGrantsGov {
        async fn fetch(&self, req: HttpRequest) -> Result<pumper_core::HttpResponse> {
            let (status, body) = if req.url.contains("search2") {
                (
                    200,
                    json!({
                        "errorcode": 0,
                        "data": {
                            "hitCount": 2,
                            "oppHits": [
                                { "id": "141593", "number": "P12AC10113", "title": "Vegetation interns",
                                  "agency": "DOI", "oppStatus": "posted", "closeDate": "08/15/2099" },
                                { "id": "357305", "number": "TEST-24-002", "title": "Rural Health",
                                  "agency": "HHS", "oppStatus": "posted", "closeDate": "09/30/2099" }
                            ]
                        }
                    })
                    .to_string(),
                )
            } else {
                match self.detail {
                    DetailFailure::Drift => (
                        200,
                        json!({ "errorcode": 0, "data": { "renamed": true } }).to_string(),
                    ),
                    DetailFailure::Http500 => (500, "gateway is down".to_string()),
                }
            };
            Ok(pumper_core::HttpResponse {
                status,
                headers: HashMap::new(),
                body,
                final_url: req.url,
                cache_hit: false,
            })
        }
    }

    async fn run_with_broken_details(detail: DetailFailure) -> Value {
        let store = pumper_core::testing::TempStore::new("grants-gov-details").await;
        let engines = pumper_core::testing::engines_with(
            std::sync::Arc::new(ScriptedGrantsGov { detail }),
            std::sync::Arc::new(pumper_core::testing::Dead),
            std::sync::Arc::new(pumper_core::testing::Dead),
        );
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "grants-gov")
            .params(GrantsGov.default_params())
            .engines(engines)
            .build();
        GrantsGov
            .run(ctx)
            .await
            .expect("a broken DETAIL stage must never fail the LISTING sync")
    }

    #[tokio::test]
    async fn a_failing_detail_stage_leaves_the_listing_green_and_the_failure_counted() {
        for (label, mode) in [
            ("contract drift", DetailFailure::Drift),
            ("endpoint down", DetailFailure::Http500),
        ] {
            let out = run_with_broken_details(mode).await;

            // The listing — this job's primary obligation — completed in full.
            assert_eq!(out["fetched"], json!(2), "{label}");
            assert_eq!(out["new"], json!(2), "{label}");
            assert_eq!(out["hitCount"], json!(2), "{label}");
            assert_eq!(out["truncated"], json!(false), "{label}");
            assert_eq!(out["unified"]["new"], json!(2), "{label}");

            // …and the secondary stage's failure is COUNTED, not swallowed.
            assert_eq!(out["detailsFailed"], json!(2), "{label}");
            assert_eq!(out["details"]["attempted"], json!(2), "{label}");
            assert_eq!(out["details"]["failed"], json!(2), "{label}");
            assert_eq!(out["details"]["harvested"], json!(0), "{label}");
            // …and named, so a reader of `warnings` alone still learns of it.
            let warnings = out["warnings"].as_array().expect("warnings array");
            assert!(
                warnings.iter().any(|w| w
                    .as_str()
                    .is_some_and(|s| s.contains("detail harvest degraded"))),
                "{label}: {warnings:?}"
            );
            // The drift case must still surface the strict check's own words —
            // that check is unchanged, only its blast radius is.
            if matches!(mode, DetailFailure::Drift) {
                let errors = out["details"]["errors"].as_array().expect("errors array");
                assert!(
                    errors
                        .iter()
                        .any(|e| e.as_str().is_some_and(|s| s.contains("schema drift"))),
                    "{errors:?}"
                );
            }
        }
    }

    #[test]
    fn cost_sharing_and_count_parse_defensively() {
        assert_eq!(cost_sharing_flag(Some(&json!(true))), json!(true));
        assert_eq!(cost_sharing_flag(Some(&json!("Yes"))), json!(true));
        assert_eq!(cost_sharing_flag(Some(&json!(" no "))), json!(false));
        assert_eq!(cost_sharing_flag(Some(&json!("maybe"))), Value::Null);
        assert_eq!(cost_sharing_flag(None), Value::Null);
        assert_eq!(count_value(Some(&json!(12))), json!(12));
        assert_eq!(count_value(Some(&json!("1,200"))), json!(1200));
        assert_eq!(count_value(Some(&json!("several"))), Value::Null);
        assert_eq!(count_value(None), Value::Null);
    }
}
