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
//! `data.hitCount` as the total.
//!
//! Detail harvest (`harvestDetails: true`, default OFF): for opportunities the
//! sync just reported NEW or CHANGED (never the whole corpus), fetch the full
//! announcement record and store it into `grants/opportunity_details` keyed by
//! opportunity id, with a structured `requirements` block extracted from the
//! synopsis fields and the NOFO attachment manifest (URLs + metadata only —
//! v1 does NO PDF fetching or parsing; a later pass can pull the documents).
//!
//! fetchOpportunity contract (ASSUMED, pinned 2026-07-30 — NOT yet verified
//! live; the defensive parse in `extract_detail` is the tripwire, and the raw
//! first response is kept as the `detail1.json` artifact):
//!   POST https://api.grants.gov/v1/api/fetchOpportunity
//!   body: `{"opportunityId": <the Search2 hit id, sent as a JSON number when
//!          it parses as an integer, else as the raw string>}`
//!   envelope: `{"errorcode": 0, "msg": ..., "data": {...}}` — the same wrapper
//!   as search2. `data` carries id / opportunityNumber / opportunityTitle,
//!   agency fields, a `synopsis` object (posted) or `forecast` object
//!   (forecasted) with applicantTypes[] (objects with `description` or bare
//!   strings), applicantEligibilityDesc, costSharing (bool or "Yes"/"No"),
//!   awardFloor / awardCeiling / estimatedFunding (numbers or "$"-strings),
//!   expectedNumberOfAwards, responseDate — and attachment folders under
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

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpMethod, HttpRequest, ManifestExample, Result,
    ScrapeApp,
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
         \"harvestDetails\": false (fetchOpportunity details for new/changed \
         opps into grants/opportunity_details), \"maxDetailsPerRun\": 1-500}"
    }

    /// Daily full sync of open opportunities at 09:00 UTC. Scheduled runs use
    /// `default_params`: posted+forecasted at the API's max 1000-row page size, so
    /// the corpus is covered in ~3 round-trips and the ceiling is 25k, not 2.5k.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 0 9 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "oppStatuses": "posted|forecasted", "rows": 1000, "maxPages": 25 })
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
                        "description": "Fetch full opportunity details (fetchOpportunity) for opportunities this sync reported new/changed, into grants/opportunity_details. Default false."
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
            output_shape: Some(
                "{hit_count, fetched, new, changed, unchanged, removed?, details?: {harvested, \
                 deltaTotal, capped}} — Search2 sync tallies over the `opportunities` dataset \
                 (keyed by opportunity id); detail harvest writes grants/opportunity_details",
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
        let mut start: u64 = 0;
        let mut pages: u64 = 0;

        loop {
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
            // errorcode 0 = success; anything else is an application-level error.
            if parsed.get("errorcode").and_then(Value::as_i64).unwrap_or(0) != 0 {
                return Err(Error::App(format!(
                    "grants.gov error: {}",
                    parsed
                        .get("msg")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                )));
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
            hits.extend(page_hits);
            pages += 1;
            start += rows;

            // Stop when the page came back short, we've covered hitCount, or hit the cap.
            if got < rows || start >= hit_count || pages >= max_pages {
                break;
            }
        }

        // Honest coverage: stopping on the page cap while records remain is a
        // silently-partial corpus. The prior code returned Ok identically to a
        // genuine full sweep, so a truncated run was indistinguishable from a
        // complete one — same failure the drift guard below already refuses for
        // the empty case.
        let truncated = pages >= max_pages && start < hit_count;

        // Drift guard: the server reported a positive hitCount but we parsed zero
        // opportunities out of `data.oppHits` — the array was renamed/moved and
        // `unwrap_or_default` silently emptied it. Fail loudly instead of
        // reporting a successful empty run.
        if hit_count > 0 && hits.is_empty() {
            return Err(Error::App(format!(
                "grants.gov schema drift: hitCount={hit_count} but parsed 0 oppHits \
                 (data.oppHits missing or not an array)"
            )));
        }

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

        let summary = ctx.upsert_many("opportunities", &items).await?;

        // NOFO detail harvest (default OFF): only the delta this sync surfaced —
        // new + changed keys — ever triggers a fetchOpportunity call, capped per
        // run, so the daily sweep stays tens of calls, never 25k.
        let harvest_details = ctx
            .params
            .get("harvestDetails")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let max_details = ctx
            .params
            .get("maxDetailsPerRun")
            .and_then(Value::as_u64)
            .unwrap_or(50)
            .clamp(1, 500) as usize;
        let mut details_out: Option<Value> = None;
        let mut details_warning: Option<String> = None;
        if harvest_details {
            let (delta, capped) = capped_delta(&summary.new, &summary.changed, max_details);
            let delta_total = summary.new.len() + summary.changed.len();
            let by_key: HashMap<&str, &Value> =
                items.iter().map(|(k, v)| (k.as_str(), v)).collect();
            let mut detail_items: Vec<(String, Value)> = Vec::new();
            for (i, key) in delta.iter().enumerate() {
                let detail = fetch_detail(&ctx, key, i == 0).await?;
                detail_items.push((
                    key.clone(),
                    detail_record(key, by_key.get(key.as_str()).copied(), &detail),
                ));
            }
            if !detail_items.is_empty() {
                ctx.datasets
                    .upsert_many(
                        grants_common::UNIFIED_APP,
                        grants_common::DETAILS_DATASET,
                        &detail_items,
                    )
                    .await?;
            }
            if capped {
                details_warning = Some(format!(
                    "detail harvest capped: fetched {} of {delta_total} new/changed \
                     opportunities (maxDetailsPerRun={max_details}) — the rest will be \
                     picked up as they change, or raise the cap",
                    detail_items.len()
                ));
            }
            details_out = Some(json!({
                "harvested": detail_items.len(),
                "deltaTotal": delta_total,
                "capped": capped,
            }));
        }

        // Cross-source layer: normalize into grants/unified, sweep past-due rows
        // closed, and link SimHash near-duplicates syndicated across portals.
        let unified_items: Vec<(String, Value)> = hits
            .iter()
            .filter_map(grants_common::normalize_grants_gov)
            .collect();
        let cross = grants_common::finalize_unified(&ctx, &unified_items).await?;

        // Closing-soon digest: posted opportunities whose closeDate falls within
        // the next `digestDays` days, soonest first — the deadline-alert surface
        // this dataset was always meant to feed.
        let digest_days = ctx
            .params
            .get("digestDays")
            .and_then(Value::as_u64)
            .unwrap_or(14)
            .clamp(1, 365) as i64;
        let closing_soon = closing_soon_digest(&hits, digest_days);
        ctx.save_artifact(
            "closing_soon.json",
            &serde_json::to_vec_pretty(&closing_soon)?,
        )
        .await?;

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
        if truncated {
            append_warning(
                &mut out,
                format!(
                    "coverage truncated: stopped at maxPages={max_pages} after {} of \
                     {hit_count} records — raise rows/maxPages to cover the full corpus",
                    hits.len()
                ),
            );
        }
        Ok(out)
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

/// Posted opportunities closing within `days` days, sorted soonest-first.
/// Each entry keeps just what an alert needs: id, number, title, agency,
/// close date, and days left.
fn closing_soon_digest(hits: &[Value], days: i64) -> Vec<Value> {
    let today = chrono::Utc::now().date_naive();
    let mut digest: Vec<(i64, Value)> = hits
        .iter()
        .filter(|h| {
            h.get("oppStatus")
                .and_then(Value::as_str)
                .is_none_or(|s| s.eq_ignore_ascii_case("posted"))
        })
        .filter_map(|h| {
            let close = h.get("closeDate").and_then(Value::as_str)?;
            let close = grants_common::parse_date(close)?;
            let days_left = (close - today).num_days();
            (0..=days).contains(&days_left).then(|| {
                (
                    days_left,
                    json!({
                        "id": h.get("id"),
                        "number": h.get("number"),
                        "title": h.get("title"),
                        "agency": h.get("agency").or_else(|| h.get("agencyCode")),
                        "closeDate": close.to_string(),
                        "daysLeft": days_left,
                    }),
                )
            })
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
    if parsed.get("errorcode").and_then(Value::as_i64).unwrap_or(0) != 0 {
        return Err(Error::App(format!(
            "grants.gov fetchOpportunity({opp_id}) error: {}",
            parsed
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
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
        "harvested_at": pumper_core::datasets::ts(chrono::Utc::now()),
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
        "expected_awards": count_value(syn.get("expectedNumberOfAwards")),
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
fn applicant_types(v: Option<&Value>) -> Value {
    let items: Vec<Value> = match v {
        Some(Value::Array(a)) => a
            .iter()
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
            .collect(),
        _ => Vec::new(),
    };
    Value::Array(items)
}

/// Flattens the NOFO attachment manifest: `synopsisAttachmentFolders[]` (each
/// with nested `synopsisAttachments[]`) plus a flat `attachments[]` fallback.
/// Every entry keeps id, file name/description, mime type, size, its folder,
/// and the ASSUMED download URL (constructed only when an id exists) — enough
/// for a later PDF pass to fetch, nothing fetched now.
fn attachment_manifest(detail: &Value) -> Vec<Value> {
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
    if let Some(folders) = detail
        .get("synopsisAttachmentFolders")
        .and_then(Value::as_array)
    {
        for folder in folders {
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
    }
    if let Some(flat) = detail.get("attachments").and_then(Value::as_array) {
        for att in flat {
            push(att, None);
        }
    }
    out
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

    #[test]
    fn digest_keeps_only_posted_opps_closing_within_window() {
        let today = chrono::Utc::now().date_naive();
        let soon = (today + chrono::Duration::days(3))
            .format("%m/%d/%Y")
            .to_string();
        let far = (today + chrono::Duration::days(90))
            .format("%m/%d/%Y")
            .to_string();
        let past = (today - chrono::Duration::days(1))
            .format("%m/%d/%Y")
            .to_string();
        let hits = vec![
            json!({ "id": "1", "title": "in window", "oppStatus": "posted", "closeDate": soon }),
            json!({ "id": "2", "title": "too far", "oppStatus": "posted", "closeDate": far }),
            json!({ "id": "3", "title": "already closed", "oppStatus": "posted", "closeDate": past }),
            json!({ "id": "4", "title": "forecasted", "oppStatus": "forecasted", "closeDate": soon }),
            json!({ "id": "5", "title": "no close date", "oppStatus": "posted" }),
        ];
        let digest = closing_soon_digest(&hits, 14);
        assert_eq!(digest.len(), 1);
        assert_eq!(digest[0]["id"], "1");
        assert_eq!(digest[0]["daysLeft"], 3);
    }

    #[test]
    fn digest_sorts_soonest_first_and_tolerates_iso_dates() {
        let today = chrono::Utc::now().date_naive();
        let d = |n: i64, iso: bool| {
            let date = today + chrono::Duration::days(n);
            if iso {
                date.format("%Y-%m-%d").to_string()
            } else {
                date.format("%m/%d/%Y").to_string()
            }
        };
        let hits = vec![
            json!({ "id": "a", "closeDate": d(10, false) }),
            json!({ "id": "b", "closeDate": d(2, true) }),
            json!({ "id": "c", "closeDate": d(5, false) }),
        ];
        let digest = closing_soon_digest(&hits, 14);
        let ids: Vec<&str> = digest.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["b", "c", "a"]);
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
        assert_eq!(req["applicant_types"], json!([]));
        assert_eq!(req["close_date"], Value::Null);
        // No synopsis/forecast at all: still a fully-Null block, never a panic.
        let req = requirements_block(&json!({ "id": 2 }));
        assert_eq!(req["award_ceiling"], Value::Null);
        assert_eq!(req["cost_sharing"], Value::Null);
    }

    #[test]
    fn attachment_manifest_flattens_folders_with_urls_and_metadata() {
        let atts = attachment_manifest(&sample_detail());
        assert_eq!(atts.len(), 1);
        assert_eq!(atts[0]["file_name"], json!("NOFO.pdf"));
        assert_eq!(atts[0]["folder"], json!("Full Announcement"));
        assert_eq!(atts[0]["mime_type"], json!("application/pdf"));
        assert_eq!(atts[0]["size_bytes"], json!(123456));
        assert_eq!(
            atts[0]["download_url"],
            json!("https://apply07.grants.gov/grantsws/rest/opportunity/att/download/999001")
        );
        // No attachments → empty manifest; flat `attachments[]` fallback works,
        // and a missing id means no fabricated download URL.
        assert!(attachment_manifest(&json!({ "id": 1 })).is_empty());
        let flat = attachment_manifest(&json!({
            "attachments": [{ "fileName": "guide.docx" }]
        }));
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0]["file_name"], json!("guide.docx"));
        assert_eq!(flat[0]["download_url"], Value::Null);
        assert_eq!(flat[0]["folder"], Value::Null);
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
