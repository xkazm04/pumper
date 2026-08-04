//! Shared grant-intelligence layer for the grant-source apps.
//!
//! Each source app keeps its raw records in its own `opportunities` dataset;
//! this crate additionally normalizes every opportunity into ONE canonical
//! schema and upserts it into the cross-source `grants/unified` dataset
//! (keyed `<source>:<source_id>`), so downstream consumers — search, exports,
//! deadline digests, dedup — see one shape regardless of origin. Cross-source
//! near-duplicates (the same grant syndicated on two portals) are linked via
//! SimHash into `grants/duplicate_links`.

use pumper_core::resilience::{write_dataset, SourceState};
use pumper_core::{AppContext, Provenance, Result, UpsertSummary};
use serde_json::{json, Value};

/// Derivation stamp (M12) for a cross-source write. `job_id` is the one fact
/// the runtime always knows; `source_url` is passed ONLY where the whole batch
/// genuinely came from that one endpoint (a source app's listing URL). Derived
/// writes — lifecycle events, the past-due sweep, duplicate links — are
/// computed from many records, so they stamp job lineage and nothing else:
/// naming one URL there would be a fabrication.
fn stamp(ctx: &AppContext, source_url: Option<&str>) -> Provenance {
    Provenance {
        job_id: Some(ctx.job_id.to_string()),
        source_url: source_url.map(str::to_string),
        ..Provenance::default()
    }
}

/// Virtual app namespace holding the cross-source datasets.
pub const UNIFIED_APP: &str = "grants";
pub const UNIFIED_DATASET: &str = "unified";
pub const DUP_DATASET: &str = "duplicate_links";

/// Append-only lifecycle-event timeline (`grants/events`), keyed
/// `{opportunity_key}:{observed_at_date}:{kind}` — one event per opportunity ×
/// day × kind, so an intra-day re-observation refines the same row instead of
/// duplicating it. Queryable via the generic `?filter=` surface
/// (e.g. `$.kind:eq:deadline_extended`).
///
/// Retention: events are meant to accumulate for years (they ARE the product —
/// per-agency extension-rate history), so no sweep deletes them. The unified
/// dataset's *revisions* that feed classification are subject to the normal
/// retention janitor (`[retention]`, OFF by default); classification only ever
/// needs the latest two revisions per key, so any revision-prune window ≥ 1
/// prior revision is safe for this feature.
pub const EVENTS_DATASET: &str = "events";

/// Per-opportunity detail corpus (`grants/opportunity_details`), keyed by the
/// opportunity id — full synopsis + attachment manifest + structured
/// requirements block harvested by a source app's detail stage (grants-gov
/// `harvestDetails` today; other sources may join). Lives in the shared
/// namespace so downstream consumers (search, `?filter=`, a future
/// application-drafting layer) see one dataset regardless of origin.
pub const DETAILS_DATASET: &str = "opportunity_details";

/// SimHash Hamming distance for cross-source near-duplicate linking. One
/// constant so every source links identically — a per-app literal drifts.
pub const DUP_DISTANCE: u32 = 3;

/// The dataset every grant source keeps its OWN raw records in, and therefore
/// the `(app, dataset)` pair the extraction-health ladder judges it on. All
/// three sources use the same name; one constant so the health lookup below
/// cannot drift from the datasets the apps actually write.
pub const SOURCE_DATASET: &str = "opportunities";

/// Where one source's contribution to the SHARED `grants/unified` dataset goes,
/// and what trust stamp it carries, given that source's own health state.
///
/// **Design decision — the unit of gating is the contribution, not the dataset.**
/// `grants/unified` is written by three independent sources. Gating the dataset
/// would mean one broken source (say ca-grants) quarantining the whole canonical
/// layer and taking grants-gov and eu-sedia down with it, which is strictly
/// worse than the disease. So health is resolved for the SOURCE's own pair
/// (`<source-app>/opportunities`) and the standard ladder is then applied to the
/// shared dataset: `degraded` keeps writing to `grants/unified` but stamps the
/// rows `provisional`; `quarantined` diverts them to the shadow dataset
/// `grants/unified@q` and leaves the canonical layer holding that source's last
/// healthy rows.
///
/// This cannot be delegated to [`AppContext::upsert_many`], which resolves health
/// for the write's own `(app, dataset)`: `("grants", "unified")` is a VIRTUAL
/// pair that no `observe_extraction` ever judges, so it always reads `Healthy`
/// and gates nothing. Resolving the source pair here is the whole point.
///
/// Vocabulary is the existing one — [`write_dataset`] for the shadow-dataset
/// name and [`SourceState::trust`] for the stamp — so consumers filter these
/// rows with the same `trust` predicate (`stable` | `provisional` |
/// `quarantined`) they use everywhere else. There is deliberately no second
/// grants-specific trust vocabulary.
pub fn contribution_target(state: SourceState) -> (String, Option<&'static str>) {
    (write_dataset(UNIFIED_DATASET, state), state.trust())
}

/// Whether this run's unified contribution may be offered to the full-text
/// search index.
///
/// The worker gates `index_datasets` on the health of the spec's own
/// `(app, dataset)` — which for the virtual `("grants", "unified")` pair is
/// structurally inert (see [`contribution_target`]). The honest place for the
/// gate is therefore the producer, against the source's own verdict: a degrading
/// or quarantined source does not get its rows into the index that saved-search
/// alerts fire from.
fn indexable(state: SourceState) -> bool {
    !state.skips_search_index()
}

/// What the shared cross-source finalize produced, for the source's result JSON.
pub struct UnifiedOutcome {
    pub unified: UpsertSummary,
    pub swept: usize,
    pub cross_source_dups: usize,
    pub warnings: Vec<String>,
    /// Lifecycle events written to `grants/events` this run.
    pub events: usize,
    /// The source's own extraction-health state this run was gated on.
    pub state: SourceState,
    /// The dataset this run's contribution actually landed in — `unified`, or
    /// the shadow `unified@q` when the source is quarantined.
    pub dataset: String,
    /// The trust stamp the contribution carries (`None` = `stable`).
    pub trust: Option<&'static str>,
}

impl UnifiedOutcome {
    /// Merges the cross-source fields into a source app's result object so every
    /// grant source reports the unified layer with one identical shape.
    pub fn merge_into(&self, out: &mut Value) {
        let Value::Object(map) = out else { return };
        map.insert(
            "unified".into(),
            json!({
                "new": self.unified.new.len(),
                "changed": self.unified.changed.len(),
                "events": self.events,
                // Where this contribution landed and how much it is stood behind
                // — so a diverted or provisional run is legible in the job
                // result, not only in the store.
                "dataset": self.dataset,
                "trust": self.trust.unwrap_or(pumper_core::TRUST_STABLE),
                "sourceState": self.state.as_str(),
            }),
        );
        map.insert("swept".into(), json!(self.swept));
        map.insert("warnings".into(), json!(self.warnings));
        map.insert("crossSourceDups".into(), json!(self.cross_source_dups));
        // Per-opportunity search docs come from the unified dataset (compact
        // result, one indexed doc per grant) — see worker `dataset_search_docs`.
        // Withheld entirely when the source's health says so: the worker's own
        // gate on ("grants","unified") can never fire (see `indexable`).
        if indexable(self.state) {
            map.insert(
                "index_datasets".into(),
                json!([{ "app": UNIFIED_APP, "dataset": self.dataset }]),
            );
        }
    }
}

/// The cross-source tail every grant source runs after storing its raw records:
/// publish the normalized batch into `grants/unified`, sweep past-due rows to
/// closed, link near-duplicates, and collect drift warnings.
///
/// Shared so the sources cannot drift apart — before this, each app hand-rolled
/// the same four calls, and one silently skipping the sweep (or linking at a
/// different distance) would be invisible.
///
/// `source_url` is the source app's listing endpoint — the one URL this whole
/// normalized batch was fetched from — stamped as provenance on the unified
/// rows. Pass `None` when a caller cannot name a single honest URL.
pub async fn finalize_unified(
    ctx: &AppContext,
    unified_items: &[(String, Value)],
    source_url: Option<&str>,
) -> Result<UnifiedOutcome> {
    // Resolve THIS source's health once, before any write, and gate the whole
    // contribution on it (see `contribution_target`).
    let state = ctx.health.enforced_state(&ctx.app, SOURCE_DATASET).await;
    let (dataset, trust) = contribution_target(state);
    let unified = sync_unified(ctx, unified_items, source_url, state).await?;
    // Amendment radar: classify source-observed changes into typed lifecycle
    // events. Runs BEFORE the sweep so the two newest revisions per changed key
    // are guaranteed to be (prior source snapshot, new source snapshot) — a
    // sweep write in between would make "old" our own inferred closure instead
    // of what the source last published. Reads history from the dataset the
    // contribution actually landed in, so a quarantined run's radar diffs its
    // shadow rows rather than mixing shadow and canonical snapshots.
    let events = record_events(ctx, &unified, &dataset).await?;
    // Lifecycle: flip past-due open/forecasted unified rows to closed — these
    // upsert-only sources never see a delisting otherwise.
    //
    // Deliberately NOT gated on this source's health, and deliberately always
    // against the canonical dataset: the sweep is derived from rows already
    // stored for ALL sources, not from this run's fetch, so a broken ca-grants
    // run must not stop grants-gov's expired rows being retired — nor write its
    // corrections into a shadow dataset it did not read from.
    let swept = sweep_closed(ctx).await?;
    let cross_source_dups = link_duplicates(ctx, DUP_DISTANCE).await?;
    let warnings = drift_warnings(unified_items);
    Ok(UnifiedOutcome {
        unified,
        swept,
        cross_source_dups,
        warnings,
        events,
        state,
        dataset,
        trust,
    })
}

/// Normalizes a grants.gov Search2 `oppHits[]` entry. Award amounts are not
/// present in Search2 results (live-verified 2026-08-04 — see
/// [`detail_amounts`]), so the money fields start null here and are filled from
/// the detail corpus by [`enrich_with_detail_amounts`] where a detail record
/// exists.
pub fn normalize_grants_gov(hit: &Value) -> Option<(String, Value)> {
    let id = str_of(hit, &["id", "number"])?;
    // Search2 publishes `MM/DD/YYYY` — a bare date with no timezone, so
    // `close_at` stays Null and the sweep takes its conservative path.
    let close_raw = str_of(hit, &["closeDate"]);
    let unified = json!({
        "source": "grants-gov",
        "source_id": id,
        "title": str_of(hit, &["title"]),
        "agency": str_of(hit, &["agency", "agencyCode"]),
        "status": norm_status(str_of(hit, &["oppStatus"]).as_deref()),
        "open_date": str_of(hit, &["openDate"]).as_deref().and_then(norm_date),
        "close_date": close_raw.as_deref().and_then(norm_date),
        "close_at": norm_instant(close_raw.as_deref()),
        "award_floor": Value::Null,
        "award_ceiling": Value::Null,
        "total_funding": Value::Null,
        // Search2 gives no per-opportunity category/eligibility facets (those are
        // search filters, not hit fields), so these stay empty for this source.
        "categories": Value::Array(vec![]),
        "eligibilities": Value::Array(vec![]),
        // ALN (Assistance Listing Number, formerly CFDA) lives in `cfdaList`.
        "aln": aln_from_array(hit.get("cfdaList")),
        "url": str_of(hit, &["number"])
            .map(|n| format!("https://www.grants.gov/search-results-detail/{id}?opp={n}"))
            .unwrap_or_else(|| format!("https://www.grants.gov/search-results-detail/{id}")),
        "description": Value::Null,
    });
    Some((format!("grants-gov:{id}"), unified))
}

/// Normalizes a California Grants Portal CKAN row. Column names were verified
/// against a live `datastore_search` sample (2026-07-13); a couple of legacy
/// candidates are kept as defensive fallbacks so a minor rename degrades to
/// nulls instead of breaking the run.
///
/// Per-award amount is a single `EstAmounts` **range** column ("Between
/// $100,000 and $10,000,000"), parsed into award_floor/ceiling; the earlier
/// `EstAmountFloor`/`EstAmountCeiling`/`AmountCeiling` candidates do not exist.
/// `EstAvailFunds` is the total-funding scalar ("$370,000,000").
pub fn normalize_ca_grants(rec: &Value) -> Option<(String, Value)> {
    let id = str_of(rec, &["PortalID", "GrantID"])?;
    let (award_floor, award_ceiling) = money_range(rec, &["EstAmounts"]);
    // The portal publishes `2026-11-02 23:59:00` — a wall clock with NO offset
    // (it is Pacific, but the feed never says so), so `close_at` stays Null
    // rather than being read as UTC. See `deadline_end_utc`.
    let close_raw = str_of(rec, &["ApplicationDeadline", "CloseDate", "Deadline"]);
    let unified = json!({
        "source": "ca-grants",
        "source_id": id,
        "title": str_of(rec, &["Title", "GrantTitle"]),
        "agency": str_of(rec, &["AgencyDept", "Agency", "Department"]),
        "status": norm_status(str_of(rec, &["Status"]).as_deref()),
        "open_date": str_of(rec, &["OpenDate", "ApplicationOpenDate"]).as_deref().and_then(norm_date),
        "close_date": close_raw.as_deref().and_then(norm_date),
        "close_at": norm_instant(close_raw.as_deref()),
        "award_floor": award_floor,
        "award_ceiling": award_ceiling,
        "total_funding": money_scalar(rec, &["EstAvailFunds"]),
        // Portal taxonomies are single "; "-separated string columns. Category
        // names themselves contain commas ("Housing, Community and Economic
        // Development"), so only ';' is a separator.
        "categories": str_list(rec, &["Categories"]),
        "eligibilities": str_list(rec, &["ApplicantType"]),
        // The CA portal publishes no ALN/CFDA number.
        "aln": Value::Null,
        "url": str_of(rec, &["GrantURL", "URL", "Link"]),
        "description": str_of(rec, &["Description", "Purpose"])
            .map(|d| d.chars().take(500).collect::<String>()),
    });
    Some((format!("ca-grants:{id}"), unified))
}

/// Normalizes an eu-sedia **already-normalized** `opportunities` record (the
/// output of the eu-sedia app's own `normalize`, so titles/descriptions are
/// already entity-decoded) into the unified schema.
///
/// Two SEDIA-specific traps are handled here so the pan-EU corpus doesn't
/// silently corrupt the shared query surface:
/// - **status** is a numeric code (`31094502`=open, `31094501`=forthcoming), not
///   a word. Passing it through `norm_status` would write the literal digits into
///   `status` and break every `?status=open` filter and the sweep predicate, so
///   the two real codes are mapped explicitly and anything else is left `Null`.
/// - **money** (`budgetOverview`) is EUR and the unified schema has no currency
///   dimension, so award/funding stay `Null` rather than filing euros as if they
///   were ca-grants dollars. (Revisit once unified gains a `currency` field.)
pub fn normalize_eu_sedia(rec: &Value) -> Option<(String, Value)> {
    let id = str_of(rec, &["identifier"])?;
    let now = chrono::Utc::now();
    let (close_date, close_at) =
        sedia_deadline(rec.get("deadlineDate").unwrap_or(&Value::Null), now);
    let unified = json!({
        "source": "eu-sedia",
        "source_id": id,
        "title": str_of(rec, &["title"]),
        "agency": sedia_agency(rec),
        "status": sedia_status(str_of(rec, &["status"]).as_deref()),
        "open_date": str_of(rec, &["startDate"]).as_deref().and_then(norm_date),
        "close_date": close_date,
        // The only source that publishes a timezone today — kept verbatim so the
        // sweep retires the topic at its real instant, not at UTC midnight.
        "close_at": close_at,
        // EUR, and unified has no currency dimension — Null, never fabricated USD.
        "award_floor": Value::Null,
        "award_ceiling": Value::Null,
        "total_funding": Value::Null,
        "categories": sedia_categories(rec),
        // Not present in the SEDIA search hit.
        "eligibilities": Value::Array(vec![]),
        // ALN/CFDA is a US-only concept.
        "aln": Value::Null,
        "url": str_of(rec, &["url"]),
        // Already-cleaned plain text; match the 500-char cap the other sources use.
        "description": str_of(rec, &["description_text"])
            .map(|d| d.chars().take(500).collect::<String>()),
    });
    Some((format!("eu-sedia:{id}"), unified))
}

/// SEDIA has no agency column; the framework programme (e.g. "Horizon Europe")
/// is the honest analogue, qualified by the call identifier when present.
fn sedia_agency(rec: &Value) -> Value {
    match (
        str_of(rec, &["frameworkProgramme"]),
        str_of(rec, &["callIdentifier"]),
    ) {
        (Some(fp), Some(call)) => Value::String(format!("{fp} — {call}")),
        (Some(fp), None) => Value::String(fp),
        (None, Some(call)) => Value::String(call),
        (None, None) => Value::Null,
    }
}

/// typesOfAction + programmePeriod as the category axis — the SEDIA search hit
/// carries no topic taxonomy.
fn sedia_categories(rec: &Value) -> Value {
    let mut cats = Vec::new();
    if let Some(t) = str_of(rec, &["typesOfAction"]) {
        cats.push(Value::String(t));
    }
    if let Some(p) = str_of(rec, &["programmePeriod"]) {
        cats.push(Value::String(p));
    }
    Value::Array(cats)
}

/// Maps SEDIA numeric status codes to the canonical vocabulary. Only the two
/// codes the app queries are known; anything else is `Null` rather than passed
/// through `norm_status` (which would emit the literal code).
fn sedia_status(code: Option<&str>) -> Value {
    match code {
        Some("31094502") => Value::String("open".into()),
        Some("31094501") => Value::String("forecasted".into()),
        _ => Value::Null,
    }
}

/// The unified deadline for a SEDIA topic: `(close_date, close_at)`.
///
/// `deadlineDate` is kept whole because multi-stage/multi-cutoff calls carry
/// several dates: the effective deadline is the earliest cutoff still upcoming,
/// and once every cutoff is past the latest one (so `sweep_closed` can retire the
/// topic). Taking `[0]` blindly would flip a still-open two-stage call to
/// `closed` the moment its first cutoff passes.
///
/// SEDIA publishes each cutoff as a **zoned** timestamp (`…T17:00:00Z`), so the
/// selection is made on real instants and the chosen one is kept verbatim in
/// `close_at` rather than being truncated to a date. That is the difference
/// between retiring a 17:00Z topic at 17:00Z and retiring it at midnight.
/// Accepts an array or a lone value; unparseable/absent → `(None, Null)`.
fn sedia_deadline(deadline: &Value, now: chrono::DateTime<chrono::Utc>) -> (Option<String>, Value) {
    let raw: Vec<&str> = match deadline {
        Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
        Value::String(s) => vec![s.as_str()],
        _ => Vec::new(),
    };
    // (lapse instant, original string) — the instant orders, the string is what
    // we keep, so no precision is lost by the sort.
    let mut cutoffs: Vec<(chrono::DateTime<chrono::Utc>, &str)> = raw
        .into_iter()
        .filter_map(|s| lapses_at(s).map(|end| (end, s)))
        .collect();
    if cutoffs.is_empty() {
        return (None, Value::Null);
    }
    cutoffs.sort_unstable_by_key(|(end, _)| *end);
    let chosen = cutoffs
        .iter()
        .find(|(end, _)| *end >= now)
        .copied()
        .unwrap_or_else(|| *cutoffs.last().unwrap())
        .1;
    (norm_date(chosen), norm_instant(Some(chosen)))
}

/// Upserts normalized grants into the cross-source unified dataset, stamping
/// each revision with this job's id and (when the caller can name one honestly)
/// the source listing URL the batch was fetched from.
///
/// `state` is the CALLING SOURCE's extraction-health state; it decides the
/// target dataset and the trust stamp via [`contribution_target`]. Callers must
/// resolve it before the write — judging afterwards would stamp trust from a
/// verdict that did not exist yet.
pub async fn sync_unified(
    ctx: &AppContext,
    items: &[(String, Value)],
    source_url: Option<&str>,
    state: SourceState,
) -> Result<UpsertSummary> {
    let (dataset, trust) = contribution_target(state);
    if state != SourceState::Healthy {
        tracing::warn!(
            app = %ctx.app,
            state = state.as_str(),
            %dataset,
            trust = trust.unwrap_or(pumper_core::TRUST_STABLE),
            "grants/unified contribution gated on source health"
        );
    }
    ctx.datasets
        .upsert_many_stamped(
            UNIFIED_APP,
            &dataset,
            items,
            trust,
            Some(&stamp(ctx, source_url)),
        )
        .await
}

/// The award amounts a stored `grants/opportunity_details` record carries, as
/// `(award_floor, award_ceiling, total_funding)`.
///
/// **Live-verified 2026-08-04** against `api.grants.gov/v1/api/fetchOpportunity`:
/// the Search2 hit really does carry no money (its keys are exactly
/// `id, number, title, agencyCode, agency, openDate, closeDate, oppStatus,
/// docType, cfdaList`), but the detail record's `synopsis` block carries
/// `awardFloor`, `awardCeiling` and `estimatedFunding` — as **strings**, with the
/// literal `"none"` where the agency published no figure (opportunity 357305 =
/// `"none"`/`"none"`; opportunity 141593 = `awardCeiling "55746"`,
/// `estimatedFunding "55746"`, `awardFloor "none"`).
///
/// The detail app already parses those three through
/// [`money_scalar`] into `requirements.{award_floor, award_ceiling,
/// estimated_total_funding}`, so `"none"`, `"$0"` and prose all arrive here as
/// `Null` — this function only reads what that parse produced and never
/// re-implements it.
pub fn detail_amounts(detail: &Value) -> (Value, Value, Value) {
    let req = detail.get("requirements").unwrap_or(&Value::Null);
    let num = |f: &str| match req.get(f) {
        Some(Value::Number(n)) if n.as_f64().is_some_and(|v| v > 0.0) => req[f].clone(),
        _ => Value::Null,
    };
    (
        num("award_floor"),
        num("award_ceiling"),
        num("estimated_total_funding"),
    )
}

/// Fills a normalized unified record's money fields from its detail record.
/// Returns whether anything landed.
///
/// **Fill-only, never overwrite.** A field the normalizer already populated
/// (ca-grants publishes amounts in the listing itself) is left alone, and a
/// detail record with no figure leaves the field `Null` — the honest-Null rule
/// holds end to end, so a federal opportunity whose agency published no ceiling
/// stays unmatched by `min_award` rather than matching a fabricated 0.
pub fn overlay_amounts(unified: &mut Value, detail: &Value) -> bool {
    let (floor, ceiling, total) = detail_amounts(detail);
    let mut filled = false;
    for (field, value) in [
        ("award_floor", floor),
        ("award_ceiling", ceiling),
        ("total_funding", total),
    ] {
        if value.is_null() || !unified.get(field).map(Value::is_null).unwrap_or(true) {
            continue;
        }
        unified[field] = value;
        filled = true;
    }
    filled
}

/// Overlays award amounts from the shared `grants/opportunity_details` corpus
/// onto a run's normalized unified items, in place. Returns how many items
/// gained at least one amount.
///
/// This is what makes `GET /grants?min_award=` reach the federal corpus at all:
/// Grants.gov Search2 publishes no money, so every grants-gov unified row was
/// permanently `Null` on all three money fields and the filter — over the
/// largest source — could never match. The amounts exist one endpoint away, in
/// detail records this machine already stores; this reads them from the store
/// (no re-fetch) and joins on the detail record's own `unified_key`.
///
/// Coverage is therefore exactly the detail corpus: opportunities the detail
/// harvest has seen. Everything else keeps `Null`, honestly.
pub async fn enrich_with_detail_amounts(
    ctx: &AppContext,
    items: &mut [(String, Value)],
) -> Result<usize> {
    if items.is_empty() {
        return Ok(0);
    }
    let details = ctx
        .datasets
        .list(UNIFIED_APP, DETAILS_DATASET, 1_000_000)
        .await?;
    if details.is_empty() {
        return Ok(0);
    }
    let by_key: std::collections::HashMap<&str, &Value> = details
        .iter()
        .filter_map(|r| {
            r.data
                .get("unified_key")
                .and_then(Value::as_str)
                .map(|k| (k, &r.data))
        })
        .collect();
    let mut filled = 0;
    for (key, unified) in items.iter_mut() {
        if let Some(detail) = by_key.get(key.as_str()) {
            if overlay_amounts(unified, detail) {
                filled += 1;
            }
        }
    }
    Ok(filled)
}

/// The v1 amendment-radar taxonomy: semantic lifecycle transitions on fields
/// every source normalizes (close_date, status, award amounts). Each variant is
/// something a subscriber would act on — not a raw field diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// `close_date` moved later.
    DeadlineExtended,
    /// `close_date` moved earlier (while the grant is not being closed early —
    /// that transition is `ClosedEarly`).
    DeadlineAccelerated,
    /// `status` forecasted → open: a forecast became a real, applicable posting.
    ForecastPosted,
    /// `award_ceiling` (or, failing that, `total_funding`) increased.
    AwardRaised,
    /// `status` closed → open with a deadline that is not already past.
    Reopened,
    /// `status` open → closed while the published deadline is still in the
    /// future — the source retired it before its own close date.
    ClosedEarly,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::DeadlineExtended => "deadline_extended",
            EventKind::DeadlineAccelerated => "deadline_accelerated",
            EventKind::ForecastPosted => "forecast_posted",
            EventKind::AwardRaised => "award_raised",
            EventKind::Reopened => "reopened",
            EventKind::ClosedEarly => "closed_early",
        }
    }
}

/// One classified lifecycle event: which transition fired, on which unified
/// field, with the before/after values (canonical forms — dates as
/// `YYYY-MM-DD`, money as numbers).
#[derive(Debug, Clone)]
pub struct GrantEvent {
    pub kind: EventKind,
    pub field: &'static str,
    pub before: Value,
    pub after: Value,
}

/// Classifies the transition from one unified snapshot (`old`) to the next
/// (`new`) into zero or more typed lifecycle events. PURE — all I/O stays in
/// [`record_events`], so the taxonomy is unit-testable like the closing-soon
/// digest.
///
/// Honesty rules (all deliberate, all tested):
/// - **Both values must parse** for any comparison — an unparseable or missing
///   date/number on either side yields no event, never a guess. A source
///   temporarily blanking a field must not fire the radar.
/// - **Equal values yield nothing**, so a field that flip-flops A→B→A within a
///   run (the stored snapshot ends where it started) emits no event.
/// - **Sweep flip-flop guard**: `Reopened` requires the new record's deadline
///   to not already be past. Our own `sweep_closed` flips past-due rows to
///   closed; when the source still lists the row as open with the same stale
///   deadline, the next sync writes closed→open — a real transition in the
///   revision chain but not a real reopening, and without this guard it would
///   fire daily.
/// - `ClosedEarly` requires a still-future deadline for the same reason in
///   mirror image: closing at/after the deadline is normal expiry, not news.
pub fn classify_events(
    old: &Value,
    new: &Value,
    observed_on: chrono::NaiveDate,
) -> Vec<GrantEvent> {
    let mut events = Vec::new();
    let str_field = |v: &Value, f: &str| -> Option<String> {
        v.get(f).and_then(Value::as_str).map(String::from)
    };
    let date_field = |v: &Value, f: &str| v.get(f).and_then(Value::as_str).and_then(parse_date);

    // Deadline movement — both sides must parse.
    let old_close = date_field(old, "close_date");
    let new_close = date_field(new, "close_date");
    if let (Some(o), Some(n)) = (old_close, new_close) {
        if n != o {
            events.push(GrantEvent {
                kind: if n > o {
                    EventKind::DeadlineExtended
                } else {
                    EventKind::DeadlineAccelerated
                },
                field: "close_date",
                before: Value::String(o.to_string()),
                after: Value::String(n.to_string()),
            });
        }
    }

    // Status transitions — at most one per change.
    let old_status = str_field(old, "status");
    let new_status = str_field(new, "status");
    let status_event = match (old_status.as_deref(), new_status.as_deref()) {
        (Some("forecasted"), Some("open")) => Some(EventKind::ForecastPosted),
        // Guard: only a reopening whose deadline is not already past (or has no
        // deadline yet) is real — see the sweep flip-flop note above.
        (Some("closed"), Some("open")) if !new_close.is_some_and(|d| d < observed_on) => {
            Some(EventKind::Reopened)
        }
        // Closed while the published deadline is still ahead of us.
        (Some("open"), Some("closed"))
            if new_close.or(old_close).is_some_and(|d| d > observed_on) =>
        {
            Some(EventKind::ClosedEarly)
        }
        _ => None,
    };
    if let Some(kind) = status_event {
        events.push(GrantEvent {
            kind,
            field: "status",
            before: old_status.map(Value::String).unwrap_or(Value::Null),
            after: new_status.map(Value::String).unwrap_or(Value::Null),
        });
    }

    // Award raised — award_ceiling preferred, total_funding as fallback; both
    // sides must be numbers (Null→number is "posted", not "raised").
    let num_field = |v: &Value, f: &str| v.get(f).and_then(Value::as_f64);
    for field in ["award_ceiling", "total_funding"] {
        if let (Some(o), Some(n)) = (num_field(old, field), num_field(new, field)) {
            if n > o {
                events.push(GrantEvent {
                    kind: EventKind::AwardRaised,
                    field,
                    before: Value::from(o),
                    after: Value::from(n),
                });
                break;
            }
        }
    }

    events
}

/// The I/O half of the amendment radar: for every key the sync reported as
/// changed, load its two newest unified revisions (new snapshot + the prior
/// one the upsert diffed against), classify, and append the typed events into
/// `grants/events`. Brand-new keys have no prior snapshot and are skipped —
/// first sight is not an amendment. Returns the number of events written.
async fn record_events(ctx: &AppContext, summary: &UpsertSummary, dataset: &str) -> Result<usize> {
    let now = chrono::Utc::now();
    let today = now.date_naive();
    let mut items: Vec<(String, Value)> = Vec::new();
    for key in &summary.changed {
        let revs = ctx.datasets.history(UNIFIED_APP, dataset, key, 2).await?;
        let (Some(newest), Some(prior)) = (revs.first(), revs.get(1)) else {
            continue;
        };
        let (Some(new), Some(old)) = (newest.data.as_ref(), prior.data.as_ref()) else {
            continue; // a 'removed' prior revision has no snapshot to diff against
        };
        let source = new
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or_else(|| source_of(key))
            .to_string();
        for ev in classify_events(old, new, today) {
            items.push((
                format!("{key}:{today}:{}", ev.kind.as_str()),
                json!({
                    "opportunity_key": key,
                    "source": source,
                    "kind": ev.kind.as_str(),
                    "field": ev.field,
                    "before": ev.before,
                    "after": ev.after,
                    "observed_at": pumper_core::datasets::ts(now),
                }),
            ));
        }
    }
    if !items.is_empty() {
        // Derived from revision history, not one fetched URL — job lineage only.
        ctx.datasets
            .upsert_many_stamped(
                UNIFIED_APP,
                EVENTS_DATASET,
                &items,
                None,
                Some(&stamp(ctx, None)),
            )
            .await?;
    }
    Ok(items.len())
}

/// Lifecycle sweep for the upsert-only unified dataset: these sources only
/// report currently-listed opportunities, so a grant that closes or is delisted
/// is simply absent from the next fetch — its `open`/`forecasted` row would
/// otherwise persist forever. After sync, mark every live unified row whose
/// status is `open`/`forecasted` and whose `close_date` is strictly before
/// today as `closed`. Written through the normal upsert path, so each transition
/// records a `changed` revision (the delisting signal `removed_at` can't give on
/// a partial-view source). Returns the number of rows swept to `closed`.
pub async fn sweep_closed(ctx: &AppContext) -> Result<usize> {
    use pumper_core::datasets::JsonFilter;
    let now = chrono::Utc::now();
    // Load only the sweep candidates (status open/forecasted), not the whole
    // corpus. Over time most unified rows are already `closed` and can never flip
    // again, yet the old full read deserialized every one of them on every sync —
    // and `finalize_unified` runs this once per source, now three (grants-gov,
    // ca-grants, eu-sedia), so the wasted scan was paid 3×/day. `list_filtered`
    // also already excludes tombstoned rows. (Deduplicating the 3 invocations into
    // one per sync cycle is a separate, larger change; making each cheap is the
    // pragmatic win.)
    let mut rows = Vec::new();
    for status in ["open", "forecasted"] {
        let filter = [JsonFilter::Eq {
            path: "$.status".into(),
            value: status.into(),
        }];
        rows.extend(
            ctx.datasets
                .list_filtered(UNIFIED_APP, UNIFIED_DATASET, &filter, None, 1_000_000)
                .await?,
        );
    }
    let mut updates: Vec<(String, Value)> = Vec::new();
    for rec in rows {
        let status = rec.data.get("status").and_then(Value::as_str);
        let close_date = rec.data.get("close_date").and_then(Value::as_str);
        // `close_at` is the zoned deadline where the source published one; rows
        // predating the field (and sources that publish no timezone) simply have
        // None and take the conservative date-only path.
        let close_at = rec.data.get("close_at").and_then(Value::as_str);
        if !is_past_due_open(status, close_date, close_at, now) {
            continue;
        }
        let mut updated = rec.data.clone();
        updated["status"] = Value::String("closed".to_string());
        updates.push((rec.key, updated));
    }
    if !updates.is_empty() {
        // An inferred closure, not a source publish: no source_url is honest here.
        ctx.datasets
            .upsert_many_stamped(
                UNIFIED_APP,
                UNIFIED_DATASET,
                &updates,
                None,
                Some(&stamp(ctx, None)),
            )
            .await?;
    }
    Ok(updates.len())
}

/// The sweep decision for one row: an `open`/`forecasted` opportunity whose
/// deadline has **provably lapsed** should flip to `closed`.
///
/// "Provably" is the fix. The old predicate compared `close_date` against
/// `Utc::now().date_naive()`, so a grant closing 23:59 America/Los_Angeles was
/// retired at 16:59 local — still open in its own source's timezone, and gone
/// from `GET /grants?status=open` and `closing-soon`. Now the row is judged
/// against a real instant from [`deadline_end_utc`]: exact where the source
/// published a timezone (`close_at`), and end-of-day-anywhere-on-Earth where it
/// did not.
///
/// A missing/unparseable deadline, a not-yet-lapsed one, or any other status is
/// left untouched.
fn is_past_due_open(
    status: Option<&str>,
    close_date: Option<&str>,
    close_at: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    matches!(status, Some("open") | Some("forecasted"))
        && deadline_end_utc(close_date, close_at).is_some_and(|end| now > end)
}

/// Fraction of a run's normalized opportunities missing their `title` above
/// which schema drift is likely (a renamed/dropped title column). Titles are
/// essentially always present, so a majority-null run is the signal; picked at
/// 0.5 to stay quiet on the odd genuinely-untitled record while catching a
/// wholesale column rename. `close_date`-null is intentionally NOT a drift
/// signal — forecasted grants legitimately have no close date.
pub const TITLE_NULL_DRIFT_THRESHOLD: f64 = 0.5;

/// Non-fatal schema-drift warnings over a run's normalized unified items. Empty
/// when nothing looks wrong; otherwise human-readable strings for the result's
/// `warnings` array. (The hard drift case — a positive server hitCount with zero
/// fetched rows — is a job failure, handled in each app.)
pub fn drift_warnings(items: &[(String, Value)]) -> Vec<String> {
    let mut warnings = Vec::new();
    let total = items.len();
    if total == 0 {
        return warnings;
    }
    let null_titles = items
        .iter()
        .filter(|(_, v)| v.get("title").and_then(Value::as_str).is_none())
        .count();
    let rate = null_titles as f64 / total as f64;
    if rate > TITLE_NULL_DRIFT_THRESHOLD {
        warnings.push(format!(
            "possible schema drift: {null_titles}/{total} ({:.0}%) normalized opportunities \
             have a null title — check the source's title field",
            rate * 100.0
        ));
    }
    warnings
}

/// Links cross-source near-duplicates (SimHash Hamming ≤ `max_distance`) into
/// `grants/duplicate_links`, keyed `a|b`. Same-source pairs are skipped — the
/// interesting signal is one grant syndicated on two portals.
pub async fn link_duplicates(ctx: &AppContext, max_distance: u32) -> Result<usize> {
    let pairs = ctx
        .datasets
        .duplicate_pairs(UNIFIED_APP, UNIFIED_DATASET, max_distance)
        .await?;
    let items: Vec<(String, Value)> = pairs
        .into_iter()
        .filter(|p| source_of(&p.a) != source_of(&p.b))
        .map(|p| {
            (
                format!("{}|{}", p.a, p.b),
                json!({ "a": p.a, "b": p.b, "distance": p.distance }),
            )
        })
        .collect();
    if !items.is_empty() {
        // A SimHash pairing over the stored corpus — job lineage only.
        ctx.datasets
            .upsert_many_stamped(
                UNIFIED_APP,
                DUP_DATASET,
                &items,
                None,
                Some(&stamp(ctx, None)),
            )
            .await?;
    }
    Ok(items.len())
}

fn source_of(key: &str) -> &str {
    key.split(':').next().unwrap_or("")
}

/// First non-empty string among candidate field names.
fn str_of(rec: &Value, fields: &[&str]) -> Option<String> {
    fields
        .iter()
        .filter_map(|f| rec.get(*f).and_then(Value::as_str))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(String::from)
}

/// A "; "-separated taxonomy string column → a JSON array of trimmed,
/// non-empty values (empty array when absent/blank). Only ';' splits, because
/// the portal's category names contain commas.
fn str_list(rec: &Value, fields: &[&str]) -> Value {
    let items: Vec<Value> = str_of(rec, fields)
        .map(|s| {
            s.split(';')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(|p| Value::String(p.to_string()))
                .collect()
        })
        .unwrap_or_default();
    Value::Array(items)
}

/// Joins an ALN/CFDA list value (`["15.931", ...]`) into a single `", "`-joined
/// string, or Null when absent/empty. Tolerates a bare string too.
fn aln_from_array(v: Option<&Value>) -> Value {
    let parts: Vec<String> = match v {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect(),
        Some(Value::String(s)) if !s.trim().is_empty() => vec![s.trim().to_string()],
        _ => vec![],
    };
    if parts.is_empty() {
        Value::Null
    } else {
        Value::String(parts.join(", "))
    }
}

/// All money amounts found in a string, left-to-right. Handles currency symbols,
/// thousands separators, decimals, and K/M/B magnitude suffixes
/// ("$1.5M" → 1_500_000, "$100k" → 100_000). Zero and unparseable tokens are
/// dropped, so "$0" and prose ("Dependant on submissions") yield an empty vec.
fn scan_amounts(s: &str) -> Vec<f64> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b',' || bytes[i] == b'.')
        {
            i += 1;
        }
        let digits: String = s[start..i].chars().filter(|c| *c != ',').collect();
        // Optional single magnitude suffix immediately after the number.
        let mult = match bytes.get(i).map(|b| *b as char) {
            Some('k') | Some('K') => {
                i += 1;
                1_000.0
            }
            Some('m') | Some('M') => {
                i += 1;
                1_000_000.0
            }
            Some('b') | Some('B') => {
                i += 1;
                1_000_000_000.0
            }
            _ => 1.0,
        };
        if let Ok(v) = digits.trim_matches('.').parse::<f64>() {
            let v = v * mult;
            if v > 0.0 {
                out.push(v);
            }
        }
    }
    out
}

/// Single money value for a scalar field: the first parseable amount among the
/// candidate columns (JSON numbers pass through). Null when none is found —
/// the shared honest-Null rule: `$0`, prose ("see NOFO"), and absent fields all
/// yield `Null`, never a fabricated zero. Public so source apps (e.g. the
/// grants-gov detail harvest) parse money identically to normalization.
pub fn money_scalar(rec: &Value, fields: &[&str]) -> Value {
    for f in fields {
        match rec.get(*f) {
            Some(Value::Number(n)) => {
                let v = n.as_f64().unwrap_or(0.0);
                if v > 0.0 {
                    return Value::from(v);
                }
            }
            Some(Value::String(s)) => {
                if let Some(v) = scan_amounts(s).into_iter().next() {
                    return Value::from(v);
                }
            }
            _ => {}
        }
    }
    Value::Null
}

/// (floor, ceiling) for a field that may express a range ("Between $100,000 and
/// $10,000,000", "$100k-$500k"): min and max of the amounts found. A lone value
/// collapses to (v, v); no amounts → (Null, Null).
fn money_range(rec: &Value, fields: &[&str]) -> (Value, Value) {
    for f in fields {
        let amounts = match rec.get(*f) {
            Some(Value::Number(n)) => {
                let v = n.as_f64().unwrap_or(0.0);
                if v > 0.0 {
                    vec![v]
                } else {
                    vec![]
                }
            }
            Some(Value::String(s)) => scan_amounts(s),
            _ => vec![],
        };
        if amounts.is_empty() {
            continue;
        }
        let min = amounts.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = amounts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        return (Value::from(min), Value::from(max));
    }
    (Value::Null, Value::Null)
}

/// The one date parser for the grant sources — used by normalization, the
/// close-date sweep, and the closing-soon digest so they can never diverge.
/// Tolerates the formats observed across grants.gov and the CA portal:
/// US `MM/DD/YYYY` (non-zero-padded ok, e.g. `7/1/2027`), ISO `YYYY-MM-DD`, and
/// ISO/space datetimes (`2026-11-02 23:59:00`, `2026-11-02T23:59:00Z`) whose
/// date prefix is taken. Empty/whitespace and unrecognized text yield `None`.
pub fn parse_date(s: &str) -> Option<chrono::NaiveDate> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
        .or_else(|_| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .or_else(|_| {
            // Datetime forms (`2026-11-02 23:59:00`, `2026-11-02T23:59:00Z`): take
            // the date part before the first space or `T`. Split on chars (not a
            // byte slice) so a non-ASCII value — e.g. an em-dash in "Deadline—see
            // website" — yields None instead of panicking on a non-char boundary.
            let date_part = s.split(['T', ' ']).next().unwrap_or(s);
            chrono::NaiveDate::parse_from_str(date_part, "%Y-%m-%d")
        })
        .ok()
}

/// Normalizes a date string to canonical `YYYY-MM-DD`, or `None` if unparseable.
fn norm_date(s: &str) -> Option<String> {
    parse_date(s).map(|d| d.to_string())
}

/// The companion of [`parse_date`] that keeps what `parse_date` throws away: an
/// exact instant, but ONLY when the source string carries an explicit UTC offset
/// (`…Z`, `…+02:00`). A bare date or an offset-less datetime
/// (`2026-11-02 23:59:00`, the CA portal's shape) is deliberately `None` — we do
/// not know its timezone, and inventing one would be exactly the fabrication the
/// rest of this crate refuses.
pub fn parse_instant(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

/// Canonical `close_at`: the source's deadline as an RFC3339 UTC instant when —
/// and only when — the source published a timezone. `Null` otherwise, which is
/// the honest answer for grants.gov and the CA portal and is what makes
/// [`deadline_end_utc`] take its conservative branch.
fn norm_instant(s: Option<&str>) -> Value {
    s.and_then(parse_instant)
        .map(|dt| Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)))
        .unwrap_or(Value::Null)
}

/// Hour (UTC) on the day AFTER an offset-less deadline date at which that date
/// is over *everywhere on Earth*.
///
/// End of day `D` in UTC-12 — the westernmost civil offset (Baker Island /
/// "anywhere on Earth") — is `D+1T11:59:59Z`. Midday `D+1` UTC is therefore the
/// first instant at which no inhabited timezone can still be on date `D`.
const AMBIGUOUS_DEADLINE_UTC_HOUR: u32 = 12;

/// When a published deadline actually lapses, as a UTC instant.
///
/// Two cases, and the split is the whole point:
/// - The source gave a **zoned** timestamp (`close_at`) → that exact instant.
///   SEDIA publishes `…T17:00:00Z`, so its topics retire to the second.
/// - The source gave only a **date** → the conservative end, midday UTC the
///   following day (see [`AMBIGUOUS_DEADLINE_UTC_HOUR`]). grants.gov publishes
///   `MM/DD/YYYY` and the CA portal an offset-less `23:59:00`, so neither tells
///   us its timezone. A grant closing 23:59 America/Los_Angeles is already
///   "yesterday" in UTC, and retiring it there would hide money that is still
///   claimable — so ambiguity resolves toward **keeping the grant open**, at the
///   cost of at most one extra day of a genuinely-expired row.
///
/// `None` when neither field parses — an unknown deadline is never a lapsed one.
fn deadline_end_utc(
    close_date: Option<&str>,
    close_at: Option<&str>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(instant) = close_at.and_then(parse_instant) {
        return Some(instant);
    }
    close_date.and_then(parse_date).map(end_of_ambiguous_day)
}

/// A date-only deadline `D` → `D+1T12:00:00Z`, the moment `D` is over in every
/// inhabited timezone.
fn end_of_ambiguous_day(d: chrono::NaiveDate) -> chrono::DateTime<chrono::Utc> {
    use chrono::TimeZone;
    let next = d.succ_opt().unwrap_or(d);
    chrono::Utc.from_utc_datetime(&next.and_hms_opt(AMBIGUOUS_DEADLINE_UTC_HOUR, 0, 0).unwrap())
}

/// When a single published deadline string lapses, whatever shape it has:
/// exact when it is zoned, conservatively end-of-ambiguous-day when it is a bare
/// date. Used to order SEDIA's multi-stage cutoffs by real time.
fn lapses_at(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_instant(raw).or_else(|| parse_date(raw).map(end_of_ambiguous_day))
}

/// Canonical status vocabulary: open | forecasted | closed (unknowns lowercase
/// through so nothing is silently lost).
fn norm_status(s: Option<&str>) -> Value {
    let Some(s) = s else { return Value::Null };
    let lower = s.trim().to_lowercase();
    let norm = match lower.as_str() {
        "posted" | "active" | "open" => "open",
        "forecasted" | "forecast" => "forecasted",
        "closed" | "archived" | "inactive" | "expired" => "closed",
        other => other,
    };
    Value::String(norm.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn grants_gov_normalizes_to_unified_schema() {
        let hit = json!({
            "id": "356037", "number": "TEST-24-001", "title": "Rural Health",
            "agency": "HHS", "oppStatus": "posted",
            "openDate": "07/01/2026", "closeDate": "08/15/2026",
            "cfdaList": ["93.912", "93.913"]
        });
        let (key, v) = normalize_grants_gov(&hit).unwrap();
        assert_eq!(key, "grants-gov:356037");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-08-15");
        // Search2 gives a bare date — no timezone to record, so no fabricated one.
        assert_eq!(v["close_at"], Value::Null);
        assert_eq!(v["award_ceiling"], Value::Null);
        // ALN joined from cfdaList; categories/eligibilities empty for this source.
        assert_eq!(v["aln"], "93.912, 93.913");
        assert_eq!(v["categories"], json!([]));
        assert_eq!(v["eligibilities"], json!([]));
    }

    #[test]
    fn ca_grants_parses_money_dates_and_status() {
        // Field values mirror the live portal sample (2026-07-13).
        let rec = json!({
            "PortalID": "CA-99", "Title": "Wildfire Prevention",
            "AgencyDept": "CAL FIRE", "Status": "active",
            "ApplicationDeadline": "2026-11-02 23:59:00",
            "EstAvailFunds": "$5,000,000",
            "EstAmounts": "Between $100,000 and $10,000,000",
            "Categories": "Environment & Water; Disadvantaged Communities",
            "ApplicantType": "Public Agency; Tribal Government",
            "GrantURL": "https://ca.gov/g/99"
        });
        let (key, v) = normalize_ca_grants(&rec).unwrap();
        assert_eq!(key, "ca-grants:CA-99");
        assert_eq!(v["status"], "open");
        assert_eq!(v["close_date"], "2026-11-02");
        // `23:59:00` with no offset is a wall clock, not an instant — Null, so
        // the sweep keeps the row open through the ambiguous day.
        assert_eq!(v["close_at"], Value::Null);
        assert_eq!(v["total_funding"], json!(5_000_000.0));
        // EstAmounts range → floor/ceiling.
        assert_eq!(v["award_floor"], json!(100_000.0));
        assert_eq!(v["award_ceiling"], json!(10_000_000.0));
        // "; "-split taxonomies; CA has no ALN.
        assert_eq!(
            v["categories"],
            json!(["Environment & Water", "Disadvantaged Communities"])
        );
        assert_eq!(
            v["eligibilities"],
            json!(["Public Agency", "Tribal Government"])
        );
        assert_eq!(v["aln"], Value::Null);
    }

    #[test]
    fn money_parsing_handles_suffixes_ranges_and_prose() {
        let m = |rec: &Value| money_scalar(rec, &["v"]);
        // K/M/B suffixes.
        assert_eq!(m(&json!({ "v": "$1.5M" })), json!(1_500_000.0));
        assert_eq!(m(&json!({ "v": "$100k" })), json!(100_000.0));
        assert_eq!(m(&json!({ "v": "$2B" })), json!(2_000_000_000.0));
        // Thousands separators + currency symbol.
        assert_eq!(m(&json!({ "v": "$370,000,000" })), json!(370_000_000.0));
        // JSON number passes through.
        assert_eq!(m(&json!({ "v": 250000 })), json!(250_000.0));
        // Prose / zero → null.
        assert_eq!(m(&json!({ "v": "Dependant on submissions" })), Value::Null);
        assert_eq!(m(&json!({ "v": "$0" })), Value::Null);

        // Ranges (real EstAmounts strings).
        let r = |s: &str| money_range(&json!({ "v": s }), &["v"]);
        assert_eq!(
            r("Between $100,000 and $10,000,000"),
            (json!(100_000.0), json!(10_000_000.0))
        );
        assert_eq!(r("$100k-$500k"), (json!(100_000.0), json!(500_000.0)));
        // Lone value collapses to (v, v).
        assert_eq!(r("$250,000"), (json!(250_000.0), json!(250_000.0)));
        // No amount → (Null, Null).
        assert_eq!(r("Dependant on submissions"), (Value::Null, Value::Null));
    }

    #[test]
    fn unmappable_rows_are_skipped_not_fabricated() {
        assert!(normalize_ca_grants(&json!({ "Title": "no id" })).is_none());
        assert!(normalize_grants_gov(&json!({})).is_none());
    }

    #[test]
    fn parse_date_handles_all_observed_formats() {
        // US MM/DD/YYYY (grants.gov), zero-padded and not.
        assert_eq!(parse_date("08/15/2026").unwrap().to_string(), "2026-08-15");
        assert_eq!(parse_date("7/1/2027").unwrap().to_string(), "2027-07-01");
        // ISO date.
        assert_eq!(parse_date("2026-09-30").unwrap().to_string(), "2026-09-30");
        // CA portal space-separated datetime + ISO 'T' datetime → date prefix.
        assert_eq!(
            parse_date("2026-11-02 23:59:00").unwrap().to_string(),
            "2026-11-02"
        );
        assert_eq!(
            parse_date("2026-11-02T23:59:00Z").unwrap().to_string(),
            "2026-11-02"
        );
        // Empty / unparseable → None.
        assert!(parse_date("").is_none());
        assert!(parse_date("   ").is_none());
        assert!(parse_date("not a date").is_none());
        // Regression: a non-ASCII char straddling byte 10 must not panic on a
        // non-char-boundary slice — an em-dash close-date cell yields None.
        assert!(parse_date("Deadline—see website").is_none());
        assert!(parse_date("—").is_none());
    }

    /// `YYYY-MM-DDTHH:MM:SSZ` → the instant. Test sugar only.
    fn at(s: &str) -> chrono::DateTime<chrono::Utc> {
        parse_instant(s).unwrap()
    }

    #[test]
    fn sweep_predicate_flips_only_past_due_open_or_forecasted() {
        // Same cases as before the timezone fix, re-anchored on an instant: a
        // date-only deadline lapses at midday UTC the following day, so "past
        // due" now means a day older than it used to.
        let now = at("2026-07-13T12:00:01Z");
        // Past-due open / forecasted → flip.
        assert!(is_past_due_open(
            Some("open"),
            Some("2026-07-12"),
            None,
            now
        ));
        assert!(is_past_due_open(
            Some("forecasted"),
            Some("07/12/2026"),
            None,
            now
        ));
        // Deadline exactly today has not passed → leave.
        assert!(!is_past_due_open(
            Some("open"),
            Some("2026-07-13"),
            None,
            now
        ));
        // Future, already-closed, missing/unparseable date → leave.
        assert!(!is_past_due_open(
            Some("open"),
            Some("2026-08-01"),
            None,
            now
        ));
        assert!(!is_past_due_open(
            Some("closed"),
            Some("2026-01-01"),
            None,
            now
        ));
        assert!(!is_past_due_open(Some("open"), None, None, now));
        assert!(!is_past_due_open(Some("open"), Some("n/a"), None, now));
    }

    #[test]
    fn sweep_does_not_retire_a_grant_still_open_in_its_own_timezone() {
        // A grant closing on 2026-07-12 with NO published timezone. It is still
        // 2026-07-12 somewhere on Earth until 12:00Z on 2026-07-13.
        let open_row =
            |now: &str| is_past_due_open(Some("open"), Some("2026-07-12"), None, at(now));

        // THE BUG: 00:30Z on the 13th is 17:30 on the 12th in Los Angeles — the
        // applicant still has six and a half hours. The old date-only compare
        // retired it here and it vanished from ?status=open and closing-soon.
        assert!(!open_row("2026-07-13T00:30:00Z"));
        // Hours either side of the boundary.
        assert!(!open_row("2026-07-13T11:59:59Z")); // still 07-12 in UTC-12
        assert!(open_row("2026-07-13T12:00:01Z")); // over everywhere
                                                   // And a whole day later, unambiguously.
        assert!(open_row("2026-07-14T00:00:00Z"));
    }

    #[test]
    fn sweep_uses_the_published_instant_when_the_source_gives_one() {
        // Timezone BEHIND UTC: 23:59 on 11-02 in UTC-07:00 == 06:59Z on 11-03.
        let behind = |now: &str| {
            is_past_due_open(
                Some("open"),
                Some("2026-11-02"),
                Some("2026-11-02T23:59:00-07:00"),
                at(now),
            )
        };
        assert!(!behind("2026-11-03T05:00:00Z")); // an hour before it lapses
        assert!(!behind("2026-11-03T06:59:00Z")); // exactly at the deadline
        assert!(behind("2026-11-03T07:00:00Z")); // an hour after

        // Timezone AHEAD of UTC: 23:59 on 11-02 in +09:00 == 14:59Z on 11-02 —
        // it lapses BEFORE UTC midnight, so the exact instant retires it a day
        // earlier than the conservative date-only rule would.
        let ahead = |now: &str| {
            is_past_due_open(
                Some("open"),
                Some("2026-11-02"),
                Some("2026-11-02T23:59:00+09:00"),
                at(now),
            )
        };
        assert!(!ahead("2026-11-02T14:00:00Z"));
        assert!(ahead("2026-11-02T15:00:00Z"));
        // Same row without the zoned field would still be open at that instant —
        // which is exactly the precision `close_at` buys.
        assert!(!is_past_due_open(
            Some("open"),
            Some("2026-11-02"),
            None,
            at("2026-11-02T15:00:00Z")
        ));

        // An unparseable/offset-less close_at must NOT be read as UTC — it falls
        // back to the conservative date rule rather than inventing a zone.
        let naive = |now: &str| {
            is_past_due_open(
                Some("open"),
                Some("2026-11-02"),
                Some("2026-11-02 23:59:00"),
                at(now),
            )
        };
        assert!(!naive("2026-11-03T06:00:00Z"));
        assert!(naive("2026-11-03T12:00:01Z"));
    }

    #[test]
    fn parse_instant_refuses_to_invent_a_timezone() {
        assert_eq!(
            parse_instant("2026-11-02T23:59:00Z").unwrap().to_rfc3339(),
            "2026-11-02T23:59:00+00:00"
        );
        assert_eq!(
            parse_instant("2026-11-02T23:59:00-07:00")
                .unwrap()
                .to_rfc3339(),
            "2026-11-03T06:59:00+00:00"
        );
        // Offset-less shapes are None — never silently UTC.
        assert!(parse_instant("2026-11-02 23:59:00").is_none());
        assert!(parse_instant("2026-11-02T23:59:00").is_none());
        assert!(parse_instant("2026-11-02").is_none());
        assert!(parse_instant("08/15/2026").is_none());
        assert!(parse_instant("").is_none());
        assert!(parse_instant("Deadline—see website").is_none());
    }

    #[test]
    fn eu_sedia_normalizes_with_status_codes_null_money_and_stage_deadline() {
        // A normalized eu-sedia record (the eu-sedia app's `normalize` output):
        // status is a numeric code, deadlineDate is a multi-stage array, budget
        // is EUR.
        let today = chrono::Utc::now().date_naive();
        let stage1 = (today - chrono::Duration::days(30))
            .format("%Y-%m-%dT17:00:00Z")
            .to_string();
        let stage2 = (today + chrono::Duration::days(60))
            .format("%Y-%m-%dT17:00:00Z")
            .to_string();
        let rec = json!({
            "identifier": "HORIZON-CL4-2026-DATA-01",
            "title": "AI & Robotics – Phase II",
            "status": "31094502",                               // as stored: a string code
            "frameworkProgramme": "Horizon Europe",
            "callIdentifier": "HORIZON-CL4-2026-01",
            "typesOfAction": "HORIZON-RIA",
            "programmePeriod": "2021-2027",
            "startDate": "2026-01-15",
            "deadlineDate": [stage1, stage2.clone()],
            "budgetOverview": "EUR 10 000 000",
            "url": "https://ec.europa.eu/x",
            "description_text": "Expected Outcome: projects contribute to a trustworthy AI single market.",
        });
        let (key, v) = normalize_eu_sedia(&rec).unwrap();
        assert_eq!(key, "eu-sedia:HORIZON-CL4-2026-DATA-01");
        assert_eq!(v["source"], "eu-sedia");
        // Numeric code mapped to the canonical word, NOT passed through literally.
        assert_eq!(v["status"], "open");
        // Money stays Null (EUR, no currency dimension) — never fabricated USD.
        assert_eq!(v["award_floor"], Value::Null);
        assert_eq!(v["award_ceiling"], Value::Null);
        assert_eq!(v["total_funding"], Value::Null);
        // Multi-stage deadline: the earliest cutoff still upcoming wins, not [0].
        assert_eq!(v["close_date"], stage2.split('T').next().unwrap());
        // …and its published 17:00Z instant survives instead of being truncated.
        assert_eq!(v["close_at"], json!(stage2));
        assert_eq!(v["agency"], "Horizon Europe — HORIZON-CL4-2026-01");
        assert_eq!(v["categories"], json!(["HORIZON-RIA", "2021-2027"]));
        assert_eq!(v["aln"], Value::Null);
        assert_eq!(v["eligibilities"], json!([]));
    }

    #[test]
    fn eu_sedia_unknown_status_code_is_null_not_passed_through() {
        // An unrecognized code must not leak into `status` (it would break
        // ?status=open and the sweep predicate).
        let rec = json!({ "identifier": "X", "status": "99999999" });
        let (_, v) = normalize_eu_sedia(&rec).unwrap();
        assert_eq!(v["status"], Value::Null);
    }

    #[test]
    fn eu_sedia_close_date_prefers_earliest_upcoming_else_latest_past() {
        let now = at("2026-07-16T00:00:00Z");
        let d = |s: &str| Value::String(s.to_string());
        let date = |v: &Value| sedia_deadline(v, now).0;
        // All future → earliest.
        let all_future = json!([d("2026-09-01"), d("2026-08-01")]);
        assert_eq!(date(&all_future).as_deref(), Some("2026-08-01"));
        // Mixed → earliest that is >= now (a passed first cutoff is skipped).
        let mixed = json!([d("2026-03-01"), d("2026-09-01")]);
        assert_eq!(date(&mixed).as_deref(), Some("2026-09-01"));
        // All past → latest (so the sweep can retire it).
        let all_past = json!([d("2026-01-01"), d("2026-05-01")]);
        assert_eq!(date(&all_past).as_deref(), Some("2026-05-01"));
        // Absent / unparseable → None (forecasted topics legitimately lack one).
        assert_eq!(sedia_deadline(&Value::Null, now), (None, Value::Null));
        assert_eq!(sedia_deadline(&json!([d("n/a")]), now), (None, Value::Null));
        // Date-only cutoffs carry no zone, so close_at stays Null.
        assert_eq!(sedia_deadline(&all_future, now).1, Value::Null);
    }

    #[test]
    fn eu_sedia_keeps_the_published_instant_instead_of_truncating_it() {
        // The multi-stage array is zoned; the selection runs on instants and the
        // chosen cutoff survives as `close_at` with its time-of-day intact.
        let now = at("2026-07-16T00:00:00Z");
        let deadline = json!(["2026-03-01T17:00:00Z", "2026-09-01T17:00:00Z"]);
        let (close_date, close_at) = sedia_deadline(&deadline, now);
        assert_eq!(close_date.as_deref(), Some("2026-09-01"));
        assert_eq!(close_at, json!("2026-09-01T17:00:00Z"));

        // Boundary: at 16:59:59Z on the cutoff day the topic has not lapsed; a
        // second after 17:00Z it has — to the second, not to UTC midnight.
        let picked = sedia_deadline(&deadline, at("2026-09-01T16:59:59Z"));
        assert_eq!(picked.1, json!("2026-09-01T17:00:00Z"));
        assert!(!is_past_due_open(
            Some("open"),
            picked.0.as_deref(),
            picked.1.as_str(),
            at("2026-09-01T16:59:59Z")
        ));
        assert!(is_past_due_open(
            Some("open"),
            picked.0.as_deref(),
            picked.1.as_str(),
            at("2026-09-01T17:00:01Z")
        ));
        // Without the instant, the same row would linger until midday the 2nd —
        // the conservative price of an unzoned source.
        assert!(!is_past_due_open(
            Some("open"),
            picked.0.as_deref(),
            None,
            at("2026-09-01T17:00:01Z")
        ));
    }

    #[test]
    fn eu_sedia_row_without_identifier_is_skipped() {
        assert!(normalize_eu_sedia(&json!({ "title": "no id" })).is_none());
    }

    // ---- amendment radar (classify_events) ----

    fn on(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn deadline_extended_fires_when_close_date_moves_later() {
        let old = json!({ "status": "open", "close_date": "2026-08-15" });
        let new = json!({ "status": "open", "close_date": "2026-09-30" });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::DeadlineExtended);
        assert_eq!(evs[0].field, "close_date");
        assert_eq!(evs[0].before, json!("2026-08-15"));
        assert_eq!(evs[0].after, json!("2026-09-30"));
    }

    #[test]
    fn deadline_accelerated_fires_when_close_date_moves_earlier() {
        // Mixed formats parse to the same canonical dates.
        let old = json!({ "status": "open", "close_date": "09/30/2026" });
        let new = json!({ "status": "open", "close_date": "2026-08-15" });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::DeadlineAccelerated);
        assert_eq!(evs[0].before, json!("2026-09-30"));
        assert_eq!(evs[0].after, json!("2026-08-15"));
    }

    #[test]
    fn forecast_posted_fires_on_forecasted_to_open() {
        let old = json!({ "status": "forecasted", "close_date": Value::Null });
        let new = json!({ "status": "open", "close_date": "2026-12-01" });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::ForecastPosted);
        assert_eq!(evs[0].before, json!("forecasted"));
        assert_eq!(evs[0].after, json!("open"));
    }

    #[test]
    fn award_raised_prefers_ceiling_and_requires_both_numbers() {
        // Ceiling raised → one event on award_ceiling (total_funding also rose,
        // but only the preferred field reports — one raise, one event).
        let old = json!({ "award_ceiling": 500_000.0, "total_funding": 1_000_000.0 });
        let new = json!({ "award_ceiling": 750_000.0, "total_funding": 2_000_000.0 });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::AwardRaised);
        assert_eq!(evs[0].field, "award_ceiling");
        assert_eq!(evs[0].before, json!(500_000.0));
        assert_eq!(evs[0].after, json!(750_000.0));

        // Fallback: no ceiling on either side, total_funding raised.
        let old = json!({ "award_ceiling": Value::Null, "total_funding": 1_000_000.0 });
        let new = json!({ "award_ceiling": Value::Null, "total_funding": 1_500_000.0 });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].field, "total_funding");

        // Null → number is "posted", not "raised" — both sides must parse.
        let old = json!({ "award_ceiling": Value::Null });
        let new = json!({ "award_ceiling": 750_000.0 });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());

        // A decrease is not in the v1 taxonomy.
        let old = json!({ "award_ceiling": 750_000.0 });
        let new = json!({ "award_ceiling": 500_000.0 });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
    }

    #[test]
    fn reopened_fires_only_with_a_not_yet_past_deadline() {
        // Genuine reopen: closed → open with a future deadline.
        let old = json!({ "status": "closed", "close_date": "2026-06-01" });
        let new = json!({ "status": "open", "close_date": "2026-10-01" });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 2); // deadline_extended + reopened
        assert!(evs.iter().any(|e| e.kind == EventKind::Reopened));
        assert!(evs.iter().any(|e| e.kind == EventKind::DeadlineExtended));

        // No deadline at all is still a reopen (rolling window).
        let old = json!({ "status": "closed", "close_date": Value::Null });
        let new = json!({ "status": "open", "close_date": Value::Null });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::Reopened);

        // Sweep flip-flop guard: our sweep closed a past-due row, the source
        // re-lists it open with the SAME stale deadline — not a reopening.
        let old = json!({ "status": "closed", "close_date": "2026-06-01" });
        let new = json!({ "status": "open", "close_date": "2026-06-01" });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
    }

    #[test]
    fn closed_early_requires_a_still_future_deadline() {
        // Closed 2 months before its own deadline → closed_early.
        let old = json!({ "status": "open", "close_date": "2026-10-01" });
        let new = json!({ "status": "closed", "close_date": "2026-10-01" });
        let evs = classify_events(&old, &new, on(2026, 7, 30));
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].kind, EventKind::ClosedEarly);
        assert_eq!(evs[0].field, "status");

        // Closed at/after the deadline is normal expiry, not news.
        let old = json!({ "status": "open", "close_date": "2026-07-30" });
        let new = json!({ "status": "closed", "close_date": "2026-07-30" });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
        let old = json!({ "status": "open", "close_date": "2026-06-01" });
        let new = json!({ "status": "closed", "close_date": "2026-06-01" });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());

        // No parseable deadline on either side → cannot claim "early".
        let old = json!({ "status": "open", "close_date": Value::Null });
        let new = json!({ "status": "closed", "close_date": Value::Null });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
    }

    #[test]
    fn unparseable_dates_never_fire_deadline_events() {
        // Source glitch blanks/garbles the date — no event, never a guess.
        let old = json!({ "status": "open", "close_date": "2026-08-15" });
        let new = json!({ "status": "open", "close_date": "Deadline—see website" });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
        let old = json!({ "status": "open", "close_date": Value::Null });
        let new = json!({ "status": "open", "close_date": "2026-08-15" });
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
    }

    #[test]
    fn flip_flop_within_a_run_emits_nothing() {
        // A→B→A: the stored snapshot ends where the prior revision started, so
        // the classifier sees equal values on every axis — no events.
        let old = json!({
            "status": "open", "close_date": "2026-08-15", "award_ceiling": 500_000.0
        });
        let new = old.clone();
        assert!(classify_events(&old, &new, on(2026, 7, 30)).is_empty());
        // An unrelated field changing (title) still emits no lifecycle event.
        let mut retitled = old.clone();
        retitled["title"] = json!("Renamed");
        assert!(classify_events(&old, &retitled, on(2026, 7, 30)).is_empty());
    }

    // ---- source-health gating of the shared unified layer (G-A) ----

    #[test]
    fn quarantined_source_contribution_is_not_silently_canonical() {
        // A healthy source writes the canonical dataset with no stamp (NULL ==
        // "stable") — today's behavior, unchanged.
        assert_eq!(
            contribution_target(SourceState::Healthy),
            ("unified".to_string(), None)
        );
        // Suspect is deliberately inert everywhere else; it must be inert here too.
        assert_eq!(
            contribution_target(SourceState::Suspect),
            ("unified".to_string(), None)
        );
        // Degrading: still canonical (other sources need the dataset), but the
        // rows are distinguishable by the EXISTING trust vocabulary.
        assert_eq!(
            contribution_target(SourceState::Degraded),
            ("unified".to_string(), Some("provisional"))
        );
        // Quarantined: diverted to the shadow dataset, never mixed into the layer
        // every consumer reads, and stamped on top of that.
        assert_eq!(
            contribution_target(SourceState::Quarantined),
            ("unified@q".to_string(), Some("quarantined"))
        );
        // The gating is per-CONTRIBUTION: nothing here can rename or divert the
        // dataset for the sources that are still fine — the target is a pure
        // function of one source's own state, computed per run.
        assert_ne!(
            contribution_target(SourceState::Quarantined).0,
            contribution_target(SourceState::Healthy).0
        );
    }

    #[test]
    fn degrading_contribution_is_not_offered_to_the_search_index() {
        // The worker's gate on the virtual ("grants","unified") pair can never
        // fire, so the producer withholds the spec instead of decorating it.
        assert!(indexable(SourceState::Healthy));
        assert!(indexable(SourceState::Suspect));
        assert!(!indexable(SourceState::Degraded));
        assert!(!indexable(SourceState::Quarantined));

        let outcome = |state: SourceState| {
            let (dataset, trust) = contribution_target(state);
            UnifiedOutcome {
                unified: UpsertSummary::default(),
                swept: 0,
                cross_source_dups: 0,
                warnings: vec![],
                events: 0,
                state,
                dataset,
                trust,
            }
        };
        let mut healthy = json!({});
        outcome(SourceState::Healthy).merge_into(&mut healthy);
        assert_eq!(
            healthy["index_datasets"],
            json!([{ "app": "grants", "dataset": "unified" }])
        );
        assert_eq!(healthy["unified"]["trust"], "stable");

        let mut quarantined = json!({});
        outcome(SourceState::Quarantined).merge_into(&mut quarantined);
        assert!(quarantined.get("index_datasets").is_none());
        assert_eq!(quarantined["unified"]["dataset"], "unified@q");
        assert_eq!(quarantined["unified"]["trust"], "quarantined");
        assert_eq!(quarantined["unified"]["sourceState"], "quarantined");

        let mut degraded = json!({});
        outcome(SourceState::Degraded).merge_into(&mut degraded);
        assert!(degraded.get("index_datasets").is_none());
        assert_eq!(degraded["unified"]["dataset"], "unified");
        assert_eq!(degraded["unified"]["trust"], "provisional");
    }

    // ---- federal award amounts from the detail corpus (G-B) ----

    /// A `grants/opportunity_details` record as `grants-gov::detail_record`
    /// writes it, with the `requirements` money parsed by `money_scalar` from a
    /// REAL fetchOpportunity `synopsis` (opportunity 141593, fetched
    /// 2026-08-04: `awardCeiling "55746"`, `estimatedFunding "55746"`,
    /// `awardFloor "none"`).
    fn detail_141593() -> Value {
        json!({
            "opportunity_id": "141593",
            "unified_key": "grants-gov:141593",
            "requirements": {
                "award_floor": money_scalar(&json!({ "awardFloor": "none" }), &["awardFloor"]),
                "award_ceiling": money_scalar(&json!({ "awardCeiling": "55746" }), &["awardCeiling"]),
                "estimated_total_funding":
                    money_scalar(&json!({ "estimatedFunding": "55746" }), &["estimatedFunding"]),
            }
        })
    }

    #[test]
    fn federal_amounts_come_from_the_detail_corpus_not_a_fabricated_zero() {
        // The live synopsis strings survive the shared money parser intact…
        let (floor, ceiling, total) = detail_amounts(&detail_141593());
        assert_eq!(ceiling, json!(55_746.0));
        assert_eq!(total, json!(55_746.0));
        // …and "none" is Null, never 0 — a fabricated zero would make this
        // opportunity match `min_award=0` and mis-rank against real figures.
        assert_eq!(floor, Value::Null);

        // Overlay onto the normalized Search2 hit, whose money is always Null.
        let hit = json!({
            "id": "141593", "number": "P12AC10113", "title": "Vegetation interns",
            "agency": "DOI", "oppStatus": "posted", "closeDate": "08/15/2026"
        });
        let (key, mut unified) = normalize_grants_gov(&hit).unwrap();
        assert_eq!(key, "grants-gov:141593");
        assert_eq!(
            unified["award_ceiling"],
            Value::Null,
            "Search2 has no money"
        );
        assert!(overlay_amounts(&mut unified, &detail_141593()));
        assert_eq!(unified["award_ceiling"], json!(55_746.0));
        assert_eq!(unified["total_funding"], json!(55_746.0));
        // The unpublished floor stays Null: this is where `min_award` legitimately
        // does not match, and that is upstream reality, not a gap to paper over.
        assert_eq!(unified["award_floor"], Value::Null);

        // An all-"none" detail (opportunity 357305, same live fetch) fills nothing.
        let empty = json!({
            "unified_key": "grants-gov:357305",
            "requirements": { "award_floor": Value::Null, "award_ceiling": Value::Null,
                              "estimated_total_funding": Value::Null }
        });
        let (_, mut untouched) = normalize_grants_gov(&hit).unwrap();
        assert!(!overlay_amounts(&mut untouched, &empty));
        assert_eq!(untouched["award_ceiling"], Value::Null);

        // A detail record with no `requirements` block at all is inert, not a panic.
        assert!(!overlay_amounts(
            &mut untouched,
            &json!({ "unified_key": "x" })
        ));
    }

    #[test]
    fn overlay_fills_but_never_overwrites_a_published_amount() {
        // ca-grants publishes amounts in the listing itself; a detail record must
        // never be able to replace a figure the source already gave us.
        let rec = json!({
            "PortalID": "CA-99", "Title": "Wildfire Prevention",
            "EstAmounts": "Between $100,000 and $10,000,000",
            "EstAvailFunds": "$5,000,000"
        });
        let (_, mut unified) = normalize_ca_grants(&rec).unwrap();
        let detail = json!({
            "unified_key": "ca-grants:CA-99",
            "requirements": { "award_floor": 1.0, "award_ceiling": 2.0,
                              "estimated_total_funding": 3.0 }
        });
        assert!(!overlay_amounts(&mut unified, &detail));
        assert_eq!(unified["award_floor"], json!(100_000.0));
        assert_eq!(unified["award_ceiling"], json!(10_000_000.0));
        assert_eq!(unified["total_funding"], json!(5_000_000.0));

        // Zero and negative figures in a detail record are not amounts.
        let zeroed = json!({
            "requirements": { "award_ceiling": 0.0, "award_floor": -1.0,
                              "estimated_total_funding": 0 }
        });
        let (_, mut fresh) = normalize_grants_gov(&json!({ "id": "1" })).unwrap();
        assert!(!overlay_amounts(&mut fresh, &zeroed));
        assert_eq!(fresh["award_ceiling"], Value::Null);
    }

    #[test]
    fn drift_warnings_fire_only_on_majority_null_titles() {
        let with_title = |t: Option<&str>| ("k".to_string(), json!({ "title": t }));
        // Mostly-present titles: no warning.
        let ok = vec![
            with_title(Some("A")),
            with_title(Some("B")),
            with_title(None),
        ];
        assert!(drift_warnings(&ok).is_empty());
        // Majority null: warning.
        let bad = vec![with_title(None), with_title(None), with_title(Some("C"))];
        assert_eq!(drift_warnings(&bad).len(), 1);
        // Empty input: no warning (no data is not drift).
        assert!(drift_warnings(&[]).is_empty());
    }
}
