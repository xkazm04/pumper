//! California grant opportunities via the California Grants Portal on data.ca.gov
//! (CKAN `datastore_search`). The only US state that publishes a true open-call
//! API, so this is a `http` fast-path like grants-gov — no browser needed.
//!
//! Data type: OPEN CALLS. Access: key-free CKAN. Keyed by the portal's stable
//! `PortalID` into the `opportunities` dataset. See `catalog/data-sources.toml`
//! (id `ca-grants`).
//!
//! Uses CKAN's POST+JSON form of `datastore_search` (avoids URL-encoding the
//! `filters` object) and paginates with `limit` + `offset`; `result.total` is the
//! full count. `Status` filters to currently-open grants (`active` by default).

use std::collections::HashMap;

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, CostClass, Error, HttpMethod, HttpRequest, ManifestExample,
    Provenance, Result, ScrapeApp,
};
use serde_json::{json, Value};

pub struct CaGrants;

const CKAN_URL: &str = "https://data.ca.gov/api/3/action/datastore_search";
// California Grants Portal dataset on data.ca.gov (verified 2026-07-03).
const RESOURCE_ID: &str = "111c8c88-21f6-453c-ae2c-b4785a0624f5";

#[async_trait]
impl ScrapeApp for CaGrants {
    fn name(&self) -> &'static str {
        "ca-grants"
    }

    fn description(&self) -> &'static str {
        "California grant opportunities (California Grants Portal via data.ca.gov \
         CKAN, key-free). Open calls, keyed by PortalID into the `opportunities` \
         dataset. Params: {\"status\": \"active\" (\"\" = all statuses), \
         \"limit\": 1-1000, \"maxPages\": 1-100}"
    }

    /// Daily at 09:30 UTC — offset from grants-gov (09:00) to spread the load.
    fn schedule(&self) -> Option<&'static str> {
        Some("0 30 9 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "status": "active", "limit": 1000, "maxPages": 25 })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "description": "Portal `Status` value to filter on server-side (\"active\" = currently open). An EMPTY string means no filter — every status."
                    },
                    "limit": {
                        "type": "integer", "minimum": 1, "maximum": 1000,
                        "description": "CKAN page size (rows per datastore_search call)."
                    },
                    "maxPages": {
                        "type": "integer", "minimum": 1, "maximum": 100,
                        "description": "Page cap. Stopping on the cap with records left is reported as `sweep: \"capped\"` (`truncated: true`) plus a warning, never a silent partial sweep. It also bounds the walk when CKAN publishes no usable `result.total`."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description:
                        "Daily sync of currently-open California grants (the scheduled default)",
                    params: json!({ "status": "active", "limit": 1000, "maxPages": 25 }),
                },
                ManifestExample {
                    description: "Full backfill: every grant in the portal regardless of status",
                    params: json!({ "status": "", "limit": 1000, "maxPages": 100 }),
                },
            ],
            output_shape: Some(
                "{source, status, total, fetched, pages, new, changed, unchanged, sweep, \
                 truncated, unified: {new, changed, events, dataset, trust, sourceState}, \
                 swept, crossSourceDups, recurrenceLinks, \
                 corpusPass: {ran, cycle, batchSwept, corpusSwept}, warnings[], \
                 index_datasets[]} — CKAN sync tallies over the `opportunities` dataset \
                 (keyed by PortalID) plus the shared grants/unified cross-source layer. \
                 `sweep` names how the walk ended — complete|capped|short_page|\
                 unknown_total, the same vocabulary and meanings grants-gov reports — and \
                 `truncated` is its boolean projection (anything but `complete`); every \
                 non-complete arm also lands in `warnings[]`. \
                 The corpus-wide relation pass (sweep + duplicate/recurrence links) runs \
                 once per UTC-day cycle, on whichever grant source gets there first; a run \
                 that did not own it reports `crossSourceDups`/`recurrenceLinks` as null \
                 (not 0) and `corpusPass.ran: false`",
            ),
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let status = ctx
            .params
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("active")
            .to_string();
        let limit = ctx
            .params
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .clamp(1, 1000);
        let max_pages = ctx
            .params
            .get("maxPages")
            .and_then(Value::as_u64)
            .unwrap_or(25)
            .clamp(1, 100);

        let mut records: Vec<Value> = Vec::new();
        let mut total: u64 = 0;
        let mut offset: u64 = 0;
        let mut pages: u64 = 0;
        // Deliberately uninitialised: the loop's ONLY exit is the `break` that
        // assigns it from `walk_end`, so the compiler proves the reported arm and
        // the actual stop can never disagree. A default here would be a second,
        // silent opinion about how the walk ended.
        let end;

        loop {
            let mut body = json!({
                "resource_id": RESOURCE_ID,
                "limit": limit,
                "offset": offset,
            });
            // Empty status = no filter (all statuses); otherwise filter server-side.
            if !status.is_empty() {
                body["filters"] = json!({ "Status": status });
            }

            let resp = ctx
                .engines
                .http
                .fetch(post_json(CKAN_URL, body.to_string()))
                .await?;
            if !resp.is_success() {
                return Err(Error::App(format!(
                    "data.ca.gov returned status {} (body starts: {})",
                    resp.status,
                    resp.body.chars().take(180).collect::<String>()
                )));
            }

            let parsed: Value = serde_json::from_str(&resp.body)
                .map_err(|e| Error::App(format!("ca-grants: response was not JSON: {e}")))?;
            if !parsed
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(Error::App(format!(
                    "ca-grants: CKAN reported failure: {}",
                    parsed
                        .get("error")
                        .map(|e| e.to_string())
                        .unwrap_or_default()
                )));
            }

            let result = parsed.get("result").cloned().unwrap_or(Value::Null);
            if pages == 0 {
                total = result.get("total").and_then(Value::as_u64).unwrap_or(0);
                ctx.save_artifact("page1.json", &serde_json::to_vec_pretty(&parsed)?)
                    .await?;
            }

            let page = result
                .get("records")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = page.len() as u64;
            records.extend(page);
            pages += 1;
            offset += limit;

            if let Some(reason) =
                walk_end(pages, limit, total, got, records.len() as u64, max_pages)
            {
                end = reason;
                break;
            }
        }

        // Honest coverage: only the arm that PROVES the corpus was covered reads
        // as complete. `truncated` was computed from the `maxPages` arm alone
        // (`pages >= max_pages && offset < total`), so a short page and a
        // renamed/dropped `result.total` each returned Ok identically to a full
        // sweep — and the latter capped California at one page indefinitely.
        // See [`SweepEnd`].
        let truncated = end != SweepEnd::Complete;

        // Drift guard: CKAN reported a positive `result.total` but we parsed zero
        // records — the `result.records` array was renamed/moved and
        // `unwrap_or_default` silently emptied it. Fail loudly instead of
        // reporting a successful empty run.
        if total > 0 && records.is_empty() {
            // `SourceDrift`, not `App`: terminal for the job. A rename does not
            // un-rename itself between attempts, and the params are frozen at
            // enqueue — so retrying re-issues the identical request and
            // re-refuses here three times a day, forever, while reading in the
            // job log exactly like the portal being down.
            return Err(Error::SourceDrift(format!(
                "ca-grants schema drift: result.total={total} but parsed 0 records \
                 (result.records missing or not an array)"
            )));
        }

        // Key by the portal's stable grant id (PortalID); the CKAN `_id` is a row
        // number that renumbers on dataset reload, so it must NOT be the key.
        let items: Vec<(String, Value)> = records
            .iter()
            .enumerate()
            .map(|(i, r)| (record_key(r, i), r.clone()))
            .collect();

        // Provenance (M12): every page came from the one CKAN datastore_search
        // endpoint, so the batch-level `source_url` is a fact. Records are stored
        // as CKAN returned them — no RuleSet, no per-record archived body — so
        // `rules_hash`/`artifact_sha` stay honestly Null.
        let summary = ctx
            .upsert_many_with_provenance(
                "opportunities",
                &items,
                Provenance {
                    source_url: Some(CKAN_URL.to_string()),
                    ..Provenance::default()
                },
            )
            .await?;

        // Cross-source layer: normalize into grants/unified, sweep past-due rows
        // closed, and link SimHash near-duplicates syndicated across portals.
        let unified_items: Vec<(String, Value)> = records
            .iter()
            .filter_map(grants_common::normalize_ca_grants)
            .collect();
        let cross = grants_common::finalize_unified(&ctx, &unified_items, Some(CKAN_URL)).await?;

        let mut out = json!({
            "source": "data.ca.gov/california-grants-portal",
            "status": status,
            "total": total,
            "fetched": records.len(),
            "pages": pages,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            // How the walk ended, named — see [`SweepEnd`]. `truncated` is its
            // boolean projection ("anything but complete"), kept because it is
            // the key consumers already read.
            "sweep": end.as_str(),
            "truncated": truncated,
        });
        cross.merge_into(&mut out);
        // Pushed after the merge, which sets `warnings` to the drift warnings.
        if let Some(msg) = sweep_warning(end, pages, max_pages, limit, total, records.len()) {
            append_warning(&mut out, msg);
        }
        Ok(out)
    }
}

/// How the CKAN walk ended. Four endings, and **only one proves the corpus was
/// covered** — the single `truncated` boolean collapsed them into one claim and
/// three of them read as a complete corpus.
///
/// This is grants-gov's vocabulary — same four names, same meanings, same
/// ordering — deliberately reused rather than re-invented, because a second
/// dialect of "how much did this sweep prove" across two grant sources would be
/// worse than the bug. (grants-gov in turn took `Complete`/`Capped`/`ShortPage`
/// from cordis and added `UnknownTotal`.) It lives here rather than in
/// `grants-common` only because apps may not depend on apps and the shared crate
/// was out of this change's write set; lifting all three copies into
/// `grants_common` is the follow-up.
///
/// The arm this app was missing entirely is [`SweepEnd::UnknownTotal`], and the
/// ledger records what it was worth on the federal side: *the renamed-`hitCount`
/// case capped the corpus at one page indefinitely*. `ca-grants` read
/// `result.total` once, from page 1, with `unwrap_or(0)`, and then broke the walk
/// on `offset >= total` — so `1000 >= 0` after page 1. California capped at one
/// page, green, with `truncated: false` and a drift guard (gated on `total > 0`)
/// that could never fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepEnd {
    /// The records actually COLLECTED reach CKAN's own reported `result.total`.
    /// The only ending that reads as complete.
    Complete,
    /// Stopped at `maxPages` with records left to walk.
    Capped,
    /// A page came back shorter than `limit` while the reported `total` says more
    /// remains. A transient truncation (a rate-limited or partially-served page),
    /// NOT the end of the corpus.
    ShortPage,
    /// Records served under a `result.total` of 0 — absent, renamed, or moved.
    /// The total is unusable, so no arithmetic can prove the end; the walk runs
    /// on until a short page or the cap and reports coverage as unproven.
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
/// `collected` is every record gathered so far **including this page**, and it —
/// not offset arithmetic — is what proves coverage. The old break asked
/// `offset >= total`, which counts the rows *requested*: a page that asked for
/// 1000 and delivered 100 still advanced the offset by 1000, so on the second
/// page of a 1366-record corpus that test read `2000 >= 1366` and called 1100
/// records a complete sweep. Counting what actually arrived is the only proof
/// that survives a partially-served page.
///
/// The ordering is load-bearing and mirrors grants-gov's: proof of coverage
/// first, then the per-run cap, then the short page — because a short page is
/// **evidence of nothing** (a rate-limited upstream produces exactly the same
/// shape as a genuine tail) and must never outrank the proof.
///
/// Termination: every path either returns `Some` or leaves `page < max_pages`,
/// so `maxPages` still bounds the walk — including on an `unknown_total` feed,
/// where there is no total left to bound it.
fn walk_end(
    page: u64,
    limit: u64,
    total: u64,
    got: u64,
    collected: u64,
    max_pages: u64,
) -> Option<SweepEnd> {
    if total == 0 {
        // No usable total. Two sub-cases, told apart by what the SAME response
        // served — which is the only evidence available here.
        if got == 0 {
            // Self-consistent: no total, no records. An honestly empty result
            // set (a `Status` filter matching nothing) IS fully swept. This is
            // the boundary that must not be reported as drift.
            return Some(SweepEnd::Complete);
        }
        // Self-contradictory: records served under a zero total. Keep walking —
        // a short page or the cap is the only end signal left — but never report
        // complete.
        return (got < limit || page >= max_pages).then_some(SweepEnd::UnknownTotal);
    }
    if collected >= total {
        return Some(SweepEnd::Complete);
    }
    if page >= max_pages {
        return Some(SweepEnd::Capped);
    }
    if got < limit {
        return Some(SweepEnd::ShortPage);
    }
    None
}

/// The human-readable warning for a walk that did not prove its coverage, or
/// `None` for a complete sweep.
///
/// Every non-complete arm reaches the caller through `warnings[]` as well as
/// through `sweep`/`truncated`, because a consumer reading only the warnings
/// channel is exactly the consumer who would otherwise never learn that the
/// California corpus is short.
fn sweep_warning(
    end: SweepEnd,
    pages: u64,
    max_pages: u64,
    limit: u64,
    total: u64,
    fetched: usize,
) -> Option<String> {
    match end {
        SweepEnd::Complete => None,
        SweepEnd::Capped => Some(format!(
            "coverage truncated: stopped at maxPages={max_pages} after {fetched} of \
             {total} records — raise limit/maxPages to cover the full corpus"
        )),
        SweepEnd::ShortPage => Some(format!(
            "coverage truncated: page {pages} returned fewer than limit={limit} while \
             CKAN reports {total} total, so the walk stopped at {fetched} records — \
             treated as a TRUNCATED page, not the end of the corpus (a rate-limited or \
             partially-served page looks exactly like a genuine tail)"
        )),
        SweepEnd::UnknownTotal => Some(format!(
            "coverage unproven: CKAN served {fetched} records over {pages} page(s) while \
             reporting result.total:0 — the total is missing or renamed, so nothing can \
             prove the corpus was covered. The walk ran to a short page or \
             maxPages={max_pages} instead of trusting the total"
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

/// Stable key for a portal record: PortalID, then GrantID, then a fallback that
/// is never the raw `_id` (which renumbers on reload).
fn record_key(rec: &Value, i: usize) -> String {
    for field in ["PortalID", "GrantID"] {
        if let Some(s) = rec.get(field).and_then(Value::as_str) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    match rec.get("_id") {
        Some(Value::Number(n)) => format!("_id-{n}"),
        Some(Value::String(s)) if !s.is_empty() => s.clone(),
        _ => format!("row-{i}"),
    }
}

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

    // Records shaped like real CKAN `opportunities` rows (see the page1.json
    // artifact): PortalID/GrantID are strings, `_id` is CKAN's row number.
    #[test]
    fn portal_id_wins_over_grant_id_and_the_ckan_row_number() {
        let rec = json!({ "PortalID": "P0501", "GrantID": "G-77", "_id": 42 });
        assert_eq!(record_key(&rec, 0), "P0501");
    }

    #[test]
    fn key_falls_back_through_the_ladder_skipping_empty_strings() {
        let rec = json!({ "PortalID": "", "GrantID": "G-77", "_id": 42 });
        assert_eq!(record_key(&rec, 0), "G-77");
        let rec = json!({ "PortalID": "", "GrantID": "", "_id": 42 });
        assert_eq!(record_key(&rec, 0), "_id-42");
    }

    #[test]
    fn renumbering_ckan_row_id_is_prefixed_never_the_bare_key() {
        // A numeric `_id` renumbers on dataset reload, so it is namespaced —
        // it must never collide with a portal-issued id like "42".
        let rec = json!({ "_id": 42 });
        assert_eq!(record_key(&rec, 3), "_id-42");
        // Non-string PortalID and an empty `_id` fall through to the row index.
        let rec = json!({ "PortalID": 123, "_id": "" });
        assert_eq!(record_key(&rec, 3), "row-3");
    }

    // ---- sweep honesty: only a proven walk reads as a complete corpus ----

    /// **THE anti-pattern**, and the one the sibling app already paid for.
    /// `result.total` renamed → `unwrap_or(0)` → the old `offset >= total` was
    /// `1000 >= 0` after page 1, so the walk broke immediately, `truncated` was
    /// false, and the drift guard (gated on `total > 0`) never fired: California
    /// capped at one page, green, indefinitely.
    #[test]
    fn a_renamed_total_does_not_prove_a_one_page_corpus() {
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
    }

    /// The boundary criterion 4 pins: a genuinely empty answer is not drift.
    /// CKAN reporting `total: 0` while serving no records is self-consistent —
    /// a `Status` filter that matches nothing IS fully swept.
    #[test]
    fn a_genuinely_empty_answer_is_complete_not_unproven() {
        assert_eq!(walk_end(1, 1000, 0, 0, 0, 25), Some(SweepEnd::Complete));
    }

    /// Coverage is proven by records COLLECTED, never by offsets requested — the
    /// short-page case is exactly a page that asked for 1000 and delivered 100.
    #[test]
    fn a_short_page_is_a_truncated_page_not_the_end_of_the_corpus() {
        assert_eq!(
            walk_end(2, 1000, 1366, 100, 1100, 25),
            Some(SweepEnd::ShortPage),
            "1100 of 1366 collected is not a complete sweep, however far the offset ran"
        );
    }

    #[test]
    fn the_page_cap_reads_as_capped_not_as_a_finished_walk() {
        assert_eq!(
            walk_end(25, 1000, 100_000, 1000, 25_000, 25),
            Some(SweepEnd::Capped)
        );
    }

    #[test]
    fn a_walk_that_collected_the_reported_total_is_complete() {
        assert_eq!(
            walk_end(2, 1000, 1366, 366, 1366, 25),
            Some(SweepEnd::Complete)
        );
    }

    /// The risk the direction named: an `unknown_total` feed must not walk
    /// forever. `maxPages` is the only bound left once the total is unusable.
    #[test]
    fn an_unknown_total_walk_is_still_bounded_by_max_pages() {
        // Full pages, no usable total: keeps going while under the cap...
        assert_eq!(walk_end(4, 100, 0, 100, 400, 5), None);
        // ...and stops AT the cap, reported as unproven rather than complete.
        assert_eq!(
            walk_end(5, 100, 0, 100, 500, 5),
            Some(SweepEnd::UnknownTotal)
        );
    }

    #[test]
    fn sweep_warnings_name_the_arm_they_came_from() {
        assert!(sweep_warning(SweepEnd::Complete, 2, 25, 1000, 1366, 1366).is_none());
        let capped = sweep_warning(SweepEnd::Capped, 25, 25, 1000, 100_000, 25_000).unwrap();
        assert!(capped.contains("maxPages=25"), "{capped}");
        let short = sweep_warning(SweepEnd::ShortPage, 2, 25, 1000, 1366, 1100).unwrap();
        assert!(
            short.contains("limit=1000") && short.contains("1366"),
            "{short}"
        );
        let unproven = sweep_warning(SweepEnd::UnknownTotal, 3, 25, 1000, 0, 2400).unwrap();
        assert!(
            unproven.contains("coverage unproven") && unproven.contains("result.total:0"),
            "{unproven}"
        );
    }

    /// `truncated` must stay the boolean projection of `sweep`, so a consumer
    /// reading either key gets the same answer.
    #[test]
    fn truncated_is_the_projection_of_sweep_not_a_second_opinion() {
        for (end, name, truncated) in [
            (SweepEnd::Complete, "complete", false),
            (SweepEnd::Capped, "capped", true),
            (SweepEnd::ShortPage, "short_page", true),
            (SweepEnd::UnknownTotal, "unknown_total", true),
        ] {
            assert_eq!(end.as_str(), name);
            assert_eq!(end != SweepEnd::Complete, truncated, "{name}");
            assert_eq!(
                sweep_warning(end, 1, 25, 1000, 10, 5).is_some(),
                truncated,
                "every non-complete arm also warns: {name}"
            );
        }
    }

    /// The declaration is the only thing an agent or consumer reads before
    /// calling. `unified` declared three keys where `grants_common::merge_into`
    /// writes six — the exact drift grants-gov's declaration was corrected for.
    #[test]
    fn output_shape_declares_sweep_and_every_key_merge_into_writes() {
        let shape = CaGrants.manifest().output_shape.expect("output shape");
        assert!(shape.contains("sweep"), "{shape}");
        for key in [
            "new",
            "changed",
            "events",
            "dataset",
            "trust",
            "sourceState",
        ] {
            assert!(
                shape.contains(key),
                "output_shape must declare unified.{key}: {shape}"
            );
        }
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
            .split("\n#[cfg(test)]")
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
    const EXPECTED_TERMINAL: &[&str] = &["ca-grants schema drift: result.total="];

    /// Drift this app reports **without** failing the job. None here: ca-grants
    /// has one stage, so its only drift signal is the pre-write listing refusal.
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
            "a drift refusal is still an Error::App, so it rides the retry ladder \
             and a permanent rename fails three times a day forever: {app_drift:#?}"
        );
    }
}
