//! Program registry (`grants/programs`) — the funding PROGRAM as the unit of
//! grants intelligence, not the individual posting.
//!
//! Three shipped relations each knew something about a program and none of them
//! stored one: [`crate::link_relations`] computes a
//! [`RecurrenceProjection`](crate::RecurrenceProjection) per program chain and
//! then writes it *per pair* into `grants/recurrence_links`;
//! [`crate::EVENTS_DATASET`]'s own doc-comment says the per-agency
//! extension-rate history "IS the product" while nothing ever rolled it up; and
//! `cordis/topic_stats` is already a program rollup keyed by Horizon topic
//! family, joined only onto eu-sedia rows. This module folds all three into one
//! row per program, written once per corpus cycle beside the sweep and the
//! relation pass.
//!
//! **Precision over recall on identity**, exactly as
//! [`crate::classify_relation`] applies it to pairs: a wrong program key merges
//! two programs and every number on the row — period, next window, extension
//! rate — becomes a confident lie. So [`program_key`] only ever answers from
//! identifiers the schema actually carries, and answers `None` rather than
//! guessing.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use pumper_core::datasets::DerivedPaths;
use pumper_core::{AppContext, Result};
use serde_json::{json, Value};

use crate::{
    norm_text, parse_date, program_title, project_recurrence, stamp, topic_lineage, EventKind,
    ProgramCycle, EVENTS_DATASET, RECURRENCE_DATASET, UNIFIED_APP, UNIFIED_DATASET,
};

/// The program registry (`grants/programs`), keyed by [`program_key`]. One row
/// per funding program, recomputed from the whole live unified corpus by
/// whichever producer owns the cycle's corpus pass.
pub const PROGRAMS_DATASET: &str = "programs";

/// The field [`program_key`] is stamped into on every `grants/unified` row, so
/// `GET /grants?program=` can filter in SQL instead of re-deriving identity per
/// consumer.
///
/// Declared **derived** at every unified write (see [`derived_paths`]): it is
/// computed from the row by us, not observed at the source, and hashing it
/// would make the first stamped run re-publish the entire corpus as `changed`
/// through watches, triggers, webhooks and the yield ledger.
pub const PROGRAM_KEY_FIELD: &str = "program_key";

/// The record paths the grants layer **derives** rather than observes at a
/// source. Passed at EVERY `grants/unified` write in this crate — a single
/// non-declaring write site would hash the stamp back in and mint a spurious
/// `changed` revision for every row it touched.
pub fn derived_paths() -> DerivedPaths {
    DerivedPaths::new([PROGRAM_KEY_FIELD])
}

/// Cap on the live-unified read behind the rollup. Sized ~75× the live corpus
/// (~2.6k rows), so reaching it is a REPORTABLE event rather than a throttle —
/// and it doubles as the switch that turns removal detection off (see
/// [`corpus_read_is_complete`]).
pub const PROGRAM_CORPUS_LIMIT: i64 = 200_000;

/// Cap on each per-kind `grants/events` read behind the extension-rate block.
/// The events dataset is explicitly designed to accumulate for years, so this
/// one WILL be reached eventually; when it is, `extension_rate` goes `Null`
/// rather than being computed from a window.
pub const PROGRAM_EVENTS_LIMIT: i64 = 200_000;

/// Cap on the `grants/recurrence_links` read behind `recurrence_linked`.
pub const PROGRAM_LINKS_LIMIT: i64 = 200_000;

/// Whether a capped read may be treated as the COMPLETE current state — the
/// precondition for `sync_many`-style removal detection.
///
/// The cordis `rollup_is_complete` idiom, restated for this rollup: a read that
/// came back AT the cap is a window over the corpus, not the corpus, and
/// syncing a window would tombstone every program whose postings fell outside
/// it.
pub fn corpus_read_is_complete(rows: usize, limit: i64) -> bool {
    (rows as i64) < limit
}

/// The identity of the funding PROGRAM one unified row belongs to, or `None`
/// when the row carries nothing that can name one.
///
/// Three grammars, in order, and each is an identifier the source publishes —
/// never an inference:
/// - **`aln:<listings>`** — the Assistance Listing Number(s). The one field
///   explicitly designed to stay constant across a program's annual cycles, and
///   the strongest identity signal in the schema. The listing SET is used, sorted
///   and deduplicated, not just the first entry: a row publishing `10.001` and a
///   row publishing `10.001,10.002` are kept apart. That costs recall (one
///   program that changed its listing set reads as two) and buys the thing this
///   feature cannot survive losing — two different programs never merging.
/// - **`family:<topic_lineage>`** — Horizon topics, whose `source_id` IS the
///   topic identifier and whose family key is already the join key onto
///   `cordis/topic_stats` (see [`topic_lineage`]).
/// - **`<agency-norm>|<program_title>`** — everything else: the same two signals
///   the recurrence chain pools pairs by, so the registry cannot draw a boundary
///   the recurrence relation does not. Both parts must be non-empty; a row
///   missing an agency or a title names no program.
pub fn program_key(row: &Value) -> Option<String> {
    let listings = aln_listings(row);
    if !listings.is_empty() {
        return Some(format!("aln:{}", listings.join(",")));
    }
    if let Some(family) = row
        .get("source_id")
        .and_then(Value::as_str)
        .and_then(topic_lineage)
    {
        return Some(format!("family:{family}"));
    }
    let agency = norm_text(row.get("agency").and_then(Value::as_str).unwrap_or(""));
    let title = program_title(row.get("title").and_then(Value::as_str).unwrap_or(""));
    if agency.is_empty() || title.is_empty() {
        return None;
    }
    Some(format!("{agency}|{title}"))
}

/// The row's ALN listings, trimmed, deduplicated and sorted. Sorting is what
/// makes the key independent of the order the source happened to publish them
/// in; an empty result means "this row publishes no listing", never a key.
fn aln_listings(row: &Value) -> Vec<String> {
    let Some(raw) = row.get("aln").and_then(Value::as_str) else {
        return Vec::new();
    };
    let mut listings: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_string)
        .collect();
    listings.sort();
    listings.dedup();
    listings
}

/// Returns `items` with [`PROGRAM_KEY_FIELD`] stamped onto every row that names
/// a program, leaving a row that already carries the right stamp untouched.
///
/// Applied at every unified write so the stamp survives a source's own daily
/// re-list: a source app writes the row it fetched, which carries no
/// `program_key`, and a non-declaring write would silently strip a stamp the
/// corpus pass had put there — leaving `GET /grants?program=` blank for that
/// source until the next cycle.
pub fn stamp_program_keys(items: &[(String, Value)]) -> Vec<(String, Value)> {
    items
        .iter()
        .map(|(key, value)| {
            let stamped = match program_key(value) {
                Some(pk) if value.get(PROGRAM_KEY_FIELD) != Some(&Value::String(pk.clone())) => {
                    let mut next = value.clone();
                    if let Value::Object(map) = &mut next {
                        map.insert(PROGRAM_KEY_FIELD.into(), Value::String(pk));
                    }
                    next
                }
                _ => value.clone(),
            };
            (key.clone(), stamped)
        })
        .collect()
}

/// Lifecycle counters for ONE opportunity, folded out of `grants/events`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EventTally {
    pub deadline_extended: usize,
    pub closed_early: usize,
}

/// `grants/events` values → per-opportunity counters. PURE.
///
/// Only the two kinds the program registry makes a claim about are counted;
/// the rest of the taxonomy stays queryable through `?filter=` and is not
/// silently folded into an "amendment" number nobody defined.
pub fn tally_events(events: &[Value]) -> HashMap<String, EventTally> {
    let mut out: HashMap<String, EventTally> = HashMap::new();
    for ev in events {
        let Some(key) = ev.get("opportunity_key").and_then(Value::as_str) else {
            continue;
        };
        let Some(kind) = ev.get("kind").and_then(Value::as_str) else {
            continue;
        };
        let slot = out.entry(key.to_string()).or_default();
        if kind == EventKind::DeadlineExtended.as_str() {
            slot.deadline_extended += 1;
        } else if kind == EventKind::ClosedEarly.as_str() {
            slot.closed_early += 1;
        }
    }
    out
}

/// Every opportunity key named by a `grants/recurrence_links` row. PURE.
pub fn linked_opportunities(links: &[Value]) -> HashSet<String> {
    let mut out = HashSet::new();
    for link in links {
        for side in ["a", "b"] {
            if let Some(key) = link.get(side).and_then(Value::as_str) {
                out.insert(key.to_string());
            }
        }
    }
    out
}

/// The live unified corpus grouped by [`program_key`]. PURE.
///
/// Rows that name no program are dropped, not bucketed under a placeholder: an
/// `unknown` program row would be a join of everything the schema could not
/// identify, and every number on it would be meaningless.
pub fn group_by_program(corpus: &[(String, Value)]) -> BTreeMap<String, Vec<(&str, &Value)>> {
    let mut groups: BTreeMap<String, Vec<(&str, &Value)>> = BTreeMap::new();
    for (key, row) in corpus {
        if let Some(pk) = program_key(row) {
            groups.entry(pk).or_default().push((key.as_str(), row));
        }
    }
    groups
}

/// Everything one program row is computed from, besides its own postings.
pub struct ProgramContext<'a> {
    pub events: &'a HashMap<String, EventTally>,
    /// Whether the events read behind `events` was complete. When it was not,
    /// `extension_rate` is `Null` rather than a rate over a window.
    pub events_complete: bool,
    pub linked: &'a HashSet<String>,
    /// `cordis/topic_stats` for this program's Horizon family, when there is
    /// one and it is not tombstoned.
    pub win_history: Option<&'a Value>,
}

/// One `grants/programs` record from one program's postings. PURE — the whole
/// rollup's judgment lives here so it is unit-testable like `classify_events`.
///
/// The projection is computed from THIS program's own dated postings through
/// [`project_recurrence`], the same function `link_relations` uses on a
/// SimHash-derived chain — so a program whose cycles were never near-duplicate
/// enough to pair still gets an honest period, and the two surfaces cannot
/// disagree about what evidence a prediction needs.
pub fn program_record(key: &str, members: &[(&str, &Value)], ctx: &ProgramContext) -> Value {
    let mut ordered: Vec<&(&str, &Value)> = members.iter().collect();
    ordered.sort_by_key(|(k, v)| (close_of(v), k.to_string()));

    let cycles: Vec<ProgramCycle> = ordered
        .iter()
        .filter_map(|(k, v)| {
            close_of(v).map(|close| ProgramCycle {
                key: (*k).to_string(),
                open: v
                    .get("open_date")
                    .and_then(Value::as_str)
                    .and_then(parse_date),
                close,
            })
        })
        .collect();
    let projection = project_recurrence(&cycles);

    // Title and agency come from the LATEST posting: a program's own name drifts
    // (a renamed office, a reworded call), and the newest posting is the one a
    // reader is about to apply to.
    let latest = ordered.last().map(|(_, v)| *v);
    let sources: BTreeSet<&str> = members
        .iter()
        .filter_map(|(_, v)| v.get("source").and_then(Value::as_str))
        .collect();
    let opportunities: Vec<&str> = {
        let mut keys: Vec<&str> = members.iter().map(|(k, _)| *k).collect();
        keys.sort_unstable();
        keys
    };

    let deadline_extended_count: usize = opportunities
        .iter()
        .map(|k| ctx.events.get(*k).map_or(0, |t| t.deadline_extended))
        .sum();
    let closed_early_count: usize = opportunities
        .iter()
        .map(|k| ctx.events.get(*k).map_or(0, |t| t.closed_early))
        .sum();
    let extended_postings = opportunities
        .iter()
        .filter(|k| ctx.events.get(**k).is_some_and(|t| t.deadline_extended > 0))
        .count();
    // A rate over a windowed events read would be a fraction of an unknown
    // denominator. Null says "we did not see the whole history"; 0.0 would claim
    // "this agency never moves a deadline".
    let extension_rate = if ctx.events_complete && !opportunities.is_empty() {
        json!(extended_postings as f64 / opportunities.len() as f64)
    } else {
        Value::Null
    };
    let recurrence_linked = opportunities
        .iter()
        .filter(|k| ctx.linked.contains(**k))
        .count();

    json!({
        "program_key": key,
        "title": latest.and_then(|v| v.get("title").cloned()).unwrap_or(Value::Null),
        "agency": latest.and_then(|v| v.get("agency").cloned()).unwrap_or(Value::Null),
        "sources": sources.iter().collect::<Vec<_>>(),
        // Postings this program has been observed under, and the subset of them
        // that carry a parsable deadline — only the latter can support a period,
        // and reporting one number for both would hide why a 4-posting program
        // has no projection.
        "cycles_observed": opportunities.len(),
        "dated_cycles": cycles.len(),
        "period_days": projection.as_ref().map(|p| p.period_days),
        "next_expected_open": projection
            .as_ref()
            .and_then(|p| p.next_open)
            .map(|d| d.to_string()),
        "next_expected_close": projection
            .as_ref()
            .and_then(|p| p.next_close)
            .map(|d| d.to_string()),
        "prediction_basis": projection
            .as_ref()
            .map(|p| p.basis.clone())
            .unwrap_or_else(|| no_projection_basis(cycles.len())),
        "opportunities": opportunities,
        // Corroboration, not evidence: how many of these postings the SimHash
        // relation pass also paired as annual cycles.
        "recurrence_linked": recurrence_linked,
        "deadline_extended_count": deadline_extended_count,
        "closed_early_count": closed_early_count,
        "extension_rate": extension_rate,
        "last_award_ceiling": last_award_ceiling(&ordered),
        // Horizon only, and `Null` — never a fabricated empty history — when
        // there is nothing to join (the eu-sedia `history_block` rule).
        "win_history": ctx.win_history.cloned().unwrap_or(Value::Null),
    })
}

/// Why a program has no projection at all, in the vocabulary
/// [`project_recurrence`] uses for the cases it CAN speak to.
fn no_projection_basis(dated: usize) -> String {
    format!(
        "{dated} posting(s) with a parsable deadline; a period needs at least 2 \
         (this program's other postings publish no usable close date)"
    )
}

fn close_of(row: &Value) -> Option<chrono::NaiveDate> {
    row.get("close_date")
        .and_then(Value::as_str)
        .and_then(parse_date)
}

/// The most recent published per-award ceiling, or `Null`. Walks newest-first
/// and stops at the first posting that actually published a number — a program
/// whose latest cycle omits the figure keeps the last one it named rather than
/// reporting nothing.
fn last_award_ceiling(ordered: &[&(&str, &Value)]) -> Value {
    ordered
        .iter()
        .rev()
        .find_map(|(_, v)| match v.get("award_ceiling") {
            Some(Value::Number(n)) => Some(Value::Number(n.clone())),
            _ => None,
        })
        .unwrap_or(Value::Null)
}

/// What the rollup produced, for the corpus-pass block of a source's result.
#[derive(Debug, Default, Clone)]
pub struct ProgramRollup {
    pub rows: usize,
    /// Programs that earned a predicted next window.
    pub with_projection: usize,
    /// Programs with at least one observed lifecycle event.
    pub with_events: usize,
    /// Unified rows whose `program_key` stamp was written or corrected.
    pub stamped: usize,
    /// Whether the unified read behind the rollup was the whole corpus — the
    /// precondition for removal detection.
    pub complete: bool,
    pub warnings: Vec<String>,
}

impl ProgramRollup {
    /// The `programs` block for the corpus-pass instrument.
    pub fn block(&self) -> Value {
        json!({
            "rows": self.rows,
            "withProjection": self.with_projection,
            "withEvents": self.with_events,
            "stamped": self.stamped,
            "complete": self.complete,
        })
    }
}

/// The I/O half: fold the live unified corpus + `grants/events` +
/// `grants/recurrence_links` + `cordis/topic_stats` into `grants/programs`, and
/// stamp [`PROGRAM_KEY_FIELD`] onto the unified rows that are missing it.
///
/// Runs inside the once-per-cycle corpus pass, after `link_relations`, on the
/// CANONICAL dataset only — like the sweep and the relation pass it is derived
/// from rows already stored for every source, not from the calling run's fetch.
///
/// **Retirement is conditional on a complete read**, the cordis
/// `rollup_is_complete` idiom: the batch is this dataset's whole current state
/// only when the corpus read was not itself a window, so a truncated read
/// upserts and says so in `warnings` instead of retiring programs it never
/// looked at. Departed programs are tombstoned **by name** (`tombstone_keys`),
/// because this pass holds both sides of the comparison and therefore has no
/// business inferring anything about keys it was not given.
pub async fn roll_up_programs(ctx: &AppContext) -> Result<ProgramRollup> {
    roll_up_programs_within(ctx, PROGRAM_CORPUS_LIMIT).await
}

/// [`roll_up_programs`] with the corpus cap as a parameter, so the
/// truncated-read path — the one that must NOT tombstone — is reachable from a
/// test without materializing 200 000 rows. Production always passes
/// [`PROGRAM_CORPUS_LIMIT`].
pub async fn roll_up_programs_within(ctx: &AppContext, corpus_limit: i64) -> Result<ProgramRollup> {
    let mut warnings: Vec<String> = Vec::new();

    let corpus = ctx
        .datasets
        .list_filtered(UNIFIED_APP, UNIFIED_DATASET, &[], None, corpus_limit)
        .await?;
    let complete = corpus_read_is_complete(corpus.len(), corpus_limit);
    if !complete {
        warnings.push(format!(
            "grants/programs rolled up only the newest {} live unified rows \
             (corpus limit = {corpus_limit}): the registry is PARTIAL, and removal detection \
             is switched off for this run so programs outside the window are not tombstoned",
            corpus.len()
        ));
    }
    let rows: Vec<(String, Value)> = corpus.into_iter().map(|r| (r.key, r.data)).collect();
    let groups = group_by_program(&rows);

    // Lifecycle history, one read per kind the registry makes a claim about.
    let mut events: Vec<Value> = Vec::new();
    let mut events_complete = true;
    for kind in [EventKind::DeadlineExtended, EventKind::ClosedEarly] {
        let filter = [pumper_core::datasets::JsonFilter::Eq {
            path: "$.kind".into(),
            value: kind.as_str().into(),
        }];
        let batch = ctx
            .datasets
            .list_filtered(
                UNIFIED_APP,
                EVENTS_DATASET,
                &filter,
                None,
                PROGRAM_EVENTS_LIMIT,
            )
            .await?;
        events_complete &= corpus_read_is_complete(batch.len(), PROGRAM_EVENTS_LIMIT);
        events.extend(batch.into_iter().map(|r| r.data));
    }
    if !events_complete {
        warnings.push(format!(
            "grants/events read reached PROGRAM_EVENTS_LIMIT = {PROGRAM_EVENTS_LIMIT}: \
             extension_rate is reported Null rather than computed over a window"
        ));
    }
    let tallies = tally_events(&events);

    let links = ctx
        .datasets
        .list_filtered(
            UNIFIED_APP,
            RECURRENCE_DATASET,
            &[],
            None,
            PROGRAM_LINKS_LIMIT,
        )
        .await?;
    let link_values: Vec<Value> = links.into_iter().map(|r| r.data).collect();
    let linked = linked_opportunities(&link_values);

    // Horizon win history, one read per distinct family (never per posting).
    let mut win_history: HashMap<String, Value> = HashMap::new();
    for key in groups.keys() {
        let Some(family) = key.strip_prefix("family:") else {
            continue;
        };
        if let Some(rec) = ctx.datasets.get("cordis", "topic_stats", family).await? {
            // A tombstoned rollup is a family whose projects left the corpus;
            // joining it anyway is how a ghost outlives what it was computed
            // from (the eu-sedia `history_block` rule).
            if rec.removed_at.is_none() {
                win_history.insert(
                    key.clone(),
                    json!({
                        "family": family,
                        "source": "cordis",
                        "as_of": rec.last_seen.to_rfc3339(),
                        "stats": rec.data,
                    }),
                );
            }
        }
    }

    let mut items: Vec<(String, Value)> = Vec::with_capacity(groups.len());
    let mut with_projection = 0usize;
    let mut with_events = 0usize;
    for (key, members) in &groups {
        let record = program_record(
            key,
            members,
            &ProgramContext {
                events: &tallies,
                events_complete,
                linked: &linked,
                win_history: win_history.get(key),
            },
        );
        if record["next_expected_close"].is_string() {
            with_projection += 1;
        }
        if record["deadline_extended_count"].as_u64().unwrap_or(0) > 0
            || record["closed_early_count"].as_u64().unwrap_or(0) > 0
        {
            with_events += 1;
        }
        items.push((key.clone(), record));
    }

    if !items.is_empty() {
        // Derived from thousands of stored rows, not one fetched URL — job
        // lineage only, exactly like the sweep and the relation pass.
        ctx.datasets
            .upsert_many_stamped(
                UNIFIED_APP,
                PROGRAMS_DATASET,
                &items,
                None,
                Some(&stamp(ctx, None)),
            )
            .await?;
        if complete {
            // Retirement is by NAME, not inferred from a snapshot. The registry
            // is a complete recompute, so a program whose last posting left the
            // corpus has to disappear — but this pass can *name* the departed
            // keys (it holds both sides), and `tombstone_keys` is the seam for
            // exactly that. `detect_removed` would be the wrong tool twice
            // over: it reasons about every key it was NOT given, and reaching
            // it requires a source-health guard that means nothing here, since
            // the rollup reads the stored corpus for all sources rather than
            // one run's fetch.
            let present: std::collections::HashSet<&str> =
                items.iter().map(|(k, _)| k.as_str()).collect();
            let live = ctx
                .datasets
                .list_filtered(UNIFIED_APP, PROGRAMS_DATASET, &[], None, corpus_limit)
                .await?;
            // Same rule as the corpus read: a registry read at its own cap is a
            // window, and a window cannot say which programs are gone.
            if corpus_read_is_complete(live.len(), corpus_limit) {
                let gone: Vec<String> = live
                    .into_iter()
                    .map(|r| r.key)
                    .filter(|k| !present.contains(k.as_str()))
                    .collect();
                if !gone.is_empty() {
                    ctx.datasets
                        .tombstone_keys(UNIFIED_APP, PROGRAMS_DATASET, &gone)
                        .await?;
                }
            }
        }
    }

    // Stamp the identity back onto the postings, so `GET /grants?program=`
    // filters in SQL. Only rows whose stamp is missing or stale are written,
    // and the write declares the field derived — so this is a body refresh with
    // no revision, not a corpus-wide republication.
    let restamp: Vec<(String, Value)> = stamp_program_keys(&rows)
        .into_iter()
        .zip(rows.iter())
        .filter(|((_, next), (_, prev))| next != prev)
        .map(|(next, _)| next)
        .collect();
    let stamped = restamp.len();
    if !restamp.is_empty() {
        ctx.datasets
            .upsert_many_derived(
                UNIFIED_APP,
                UNIFIED_DATASET,
                &restamp,
                None,
                Some(&stamp(ctx, None)),
                &derived_paths(),
            )
            .await?;
    }

    Ok(ProgramRollup {
        rows: items.len(),
        with_projection,
        with_events,
        stamped,
        complete,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, id: &str, title: &str, agency: &str, close: &str, aln: Value) -> Value {
        json!({
            "source": source,
            "source_id": id,
            "title": title,
            "agency": agency,
            "status": "closed",
            "open_date": Value::Null,
            "close_date": close,
            "award_ceiling": Value::Null,
            "aln": aln,
        })
    }

    // ── identity: precision over recall ──

    #[test]
    fn same_stripped_title_different_aln_is_not_one_program() {
        // The governing rule of the whole card: a merge is unrecoverable, a
        // miss is only a miss. Two agencies' rows can look identical after the
        // year tokens come off; the ALN is the field that says they are not.
        let a = row(
            "grants-gov",
            "1",
            "Rural Health Network FY2025",
            "HHS",
            "2025-06-01",
            json!("93.912"),
        );
        let b = row(
            "grants-gov",
            "2",
            "Rural Health Network FY2026",
            "HHS",
            "2026-06-01",
            json!("93.999"),
        );
        assert_ne!(program_key(&a), program_key(&b));
        assert_eq!(program_key(&a).unwrap(), "aln:93.912");
    }

    #[test]
    fn one_aln_across_cycles_is_one_program() {
        let a = row(
            "grants-gov",
            "1",
            "Rural Health Network FY2025",
            "HHS",
            "2025-06-01",
            json!("93.912"),
        );
        let b = row(
            "grants-gov",
            "2",
            "Rural Health Network FY2026",
            "Health and Human Services",
            "2026-06-01",
            json!("93.912"),
        );
        // The title drifted AND the agency string drifted; the listing did not.
        assert_eq!(program_key(&a), program_key(&b));
    }

    #[test]
    fn the_listing_set_is_order_independent_but_not_membership_independent() {
        let one = row(
            "grants-gov",
            "1",
            "T",
            "A",
            "2025-01-01",
            json!("10.2, 10.1"),
        );
        let same = row(
            "grants-gov",
            "2",
            "T",
            "A",
            "2026-01-01",
            json!("10.1,10.2"),
        );
        let wider = row(
            "grants-gov",
            "3",
            "T",
            "A",
            "2027-01-01",
            json!("10.1,10.2,10.3"),
        );
        assert_eq!(program_key(&one), program_key(&same));
        assert_ne!(program_key(&one), program_key(&wider));
    }

    #[test]
    fn a_horizon_topic_keys_on_its_family_not_its_call_year() {
        let a = row(
            "eu-sedia",
            "HORIZON-CL4-2024-DATA-01",
            "Data spaces",
            "Horizon Europe",
            "2024-04-01",
            Value::Null,
        );
        let b = row(
            "eu-sedia",
            "HORIZON-CL4-2026-DATA-01",
            "Data spaces",
            "Horizon Europe",
            "2026-04-01",
            Value::Null,
        );
        assert_eq!(program_key(&a).unwrap(), "family:HORIZON-CL4-DATA-01");
        assert_eq!(program_key(&a), program_key(&b));
    }

    #[test]
    fn a_row_with_no_agency_names_no_program() {
        // Honest absence: an unidentifiable row must not land in an `unknown`
        // bucket that would then carry a period and a predicted window.
        let orphan = row(
            "ca-grants",
            "9",
            "Some Grant",
            "",
            "2026-01-01",
            Value::Null,
        );
        assert_eq!(program_key(&orphan), None);
        let untitled = row("ca-grants", "9", "", "CalFire", "2026-01-01", Value::Null);
        assert_eq!(program_key(&untitled), None);
    }

    #[test]
    fn agency_and_title_are_normalized_the_same_way_the_recurrence_chain_normalizes_them() {
        let a = row(
            "ca-grants",
            "1",
            "Forest Health FY 26",
            "CAL FIRE",
            "2026-01-01",
            Value::Null,
        );
        let b = row(
            "ca-grants",
            "2",
            "Forest Health 2027",
            "Cal-Fire",
            "2027-01-01",
            Value::Null,
        );
        assert_eq!(program_key(&a), program_key(&b));
        assert_eq!(program_key(&a).unwrap(), "cal fire|forest health");
    }

    // ── the stamp ──

    #[test]
    fn stamping_is_idempotent_and_leaves_unidentifiable_rows_alone() {
        let items = vec![
            (
                "grants-gov:1".to_string(),
                row("grants-gov", "1", "T", "A", "2026-01-01", json!("10.1")),
            ),
            (
                "ca-grants:2".to_string(),
                row("ca-grants", "2", "", "", "2026-01-01", Value::Null),
            ),
        ];
        let once = stamp_program_keys(&items);
        assert_eq!(once[0].1[PROGRAM_KEY_FIELD], json!("aln:10.1"));
        assert!(once[1].1.get(PROGRAM_KEY_FIELD).is_none());
        assert_eq!(stamp_program_keys(&once), once);
    }

    // ── the rollup's judgment ──

    fn ctx_with(events: &HashMap<String, EventTally>, complete: bool) -> ProgramContext<'_> {
        static EMPTY: std::sync::OnceLock<HashSet<String>> = std::sync::OnceLock::new();
        ProgramContext {
            events,
            events_complete: complete,
            linked: EMPTY.get_or_init(HashSet::new),
            win_history: None,
        }
    }

    #[test]
    fn three_cycles_earn_a_projection_and_two_do_not() {
        let rows: Vec<(String, Value)> = (0..3)
            .map(|i| {
                (
                    format!("grants-gov:{i}"),
                    row(
                        "grants-gov",
                        &i.to_string(),
                        "Rural Health Network",
                        "HHS",
                        &format!("202{}-06-01", 4 + i),
                        json!("93.912"),
                    ),
                )
            })
            .collect();
        let groups = group_by_program(&rows);
        let members = &groups["aln:93.912"];
        let events = HashMap::new();
        let rec = program_record("aln:93.912", members, &ctx_with(&events, true));
        assert_eq!(rec["cycles_observed"], json!(3));
        assert_eq!(rec["dated_cycles"], json!(3));
        assert_eq!(rec["next_expected_close"], json!("2027-06-01"));
        assert!(rec["period_days"].as_i64().unwrap() >= 365);

        // Two postings observe a gap; they do not corroborate a rhythm.
        let two = &rows[..2].to_vec();
        let groups = group_by_program(two);
        let rec = program_record(
            "aln:93.912",
            &groups["aln:93.912"],
            &ctx_with(&events, true),
        );
        assert_eq!(rec["next_expected_close"], Value::Null);
        assert!(rec["prediction_basis"]
            .as_str()
            .unwrap()
            .contains("only from"));
    }

    #[test]
    fn a_windowed_events_read_reports_no_extension_rate_rather_than_zero() {
        let rows = vec![(
            "grants-gov:1".to_string(),
            row("grants-gov", "1", "T", "A", "2026-01-01", json!("10.1")),
        )];
        let groups = group_by_program(&rows);
        let events = HashMap::new();
        let partial = program_record("aln:10.1", &groups["aln:10.1"], &ctx_with(&events, false));
        assert_eq!(partial["extension_rate"], Value::Null);
        let full = program_record("aln:10.1", &groups["aln:10.1"], &ctx_with(&events, true));
        assert_eq!(full["extension_rate"], json!(0.0));
    }

    #[test]
    fn extension_rate_counts_postings_moved_not_events_fired() {
        // One posting extended twice is one moved deadline out of two postings,
        // not a 100% extension rate.
        let rows: Vec<(String, Value)> = (0..2)
            .map(|i| {
                (
                    format!("grants-gov:{i}"),
                    row(
                        "grants-gov",
                        &i.to_string(),
                        "T",
                        "A",
                        &format!("202{}-01-01", 5 + i),
                        json!("10.1"),
                    ),
                )
            })
            .collect();
        let groups = group_by_program(&rows);
        let mut events = HashMap::new();
        events.insert(
            "grants-gov:0".to_string(),
            EventTally {
                deadline_extended: 2,
                closed_early: 0,
            },
        );
        let rec = program_record("aln:10.1", &groups["aln:10.1"], &ctx_with(&events, true));
        assert_eq!(rec["deadline_extended_count"], json!(2));
        assert_eq!(rec["extension_rate"], json!(0.5));
    }

    #[test]
    fn the_last_published_ceiling_survives_a_cycle_that_omits_it() {
        let mut older = row("grants-gov", "1", "T", "A", "2025-01-01", json!("10.1"));
        older["award_ceiling"] = json!(750_000);
        let newer = row("grants-gov", "2", "T", "A", "2026-01-01", json!("10.1"));
        let rows = vec![
            ("grants-gov:1".to_string(), older),
            ("grants-gov:2".to_string(), newer),
        ];
        let groups = group_by_program(&rows);
        let events = HashMap::new();
        let rec = program_record("aln:10.1", &groups["aln:10.1"], &ctx_with(&events, true));
        assert_eq!(rec["last_award_ceiling"], json!(750_000));
    }

    #[test]
    fn tally_events_counts_only_the_two_kinds_the_registry_claims() {
        let events = vec![
            json!({ "opportunity_key": "a", "kind": "deadline_extended" }),
            json!({ "opportunity_key": "a", "kind": "award_raised" }),
            json!({ "opportunity_key": "a", "kind": "closed_early" }),
            json!({ "kind": "deadline_extended" }),
        ];
        let tally = tally_events(&events);
        assert_eq!(tally.len(), 1);
        assert_eq!(tally["a"].deadline_extended, 1);
        assert_eq!(tally["a"].closed_early, 1);
    }

    #[test]
    fn a_corpus_read_at_the_cap_is_a_window_not_the_corpus() {
        assert!(corpus_read_is_complete(199_999, PROGRAM_CORPUS_LIMIT));
        assert!(!corpus_read_is_complete(200_000, PROGRAM_CORPUS_LIMIT));
        assert!(corpus_read_is_complete(0, PROGRAM_CORPUS_LIMIT));
    }

    // ── the I/O half ──

    /// Three annual cycles of one federal program, plus one unrelated posting.
    async fn seeded(name: &str) -> (pumper_core::testing::TempStore, AppContext) {
        let store = pumper_core::testing::TempStore::new(name).await;
        let ctx = pumper_core::testing::TestContext::new(&store.storage, "grants-gov").build();
        let mut corpus: Vec<(String, Value)> = (0..3)
            .map(|i| {
                (
                    format!("grants-gov:{i}"),
                    row(
                        "grants-gov",
                        &i.to_string(),
                        &format!("Rural Health Network FY202{}", 4 + i),
                        "HHS",
                        &format!("202{}-06-01", 4 + i),
                        json!("93.912"),
                    ),
                )
            })
            .collect();
        corpus.push((
            "ca-grants:7".to_string(),
            row(
                "ca-grants",
                "7",
                "Forest Health 2026",
                "CAL FIRE",
                "2026-03-01",
                Value::Null,
            ),
        ));
        ctx.datasets
            .upsert_many(UNIFIED_APP, UNIFIED_DATASET, &corpus)
            .await
            .unwrap();
        (store, ctx)
    }

    #[tokio::test]
    async fn the_rollup_materializes_one_row_per_program_and_stamps_the_postings() {
        let (_store, ctx) = seeded("grants-programs-rollup").await;
        let out = roll_up_programs(&ctx).await.unwrap();
        assert_eq!(out.rows, 2);
        assert_eq!(out.with_projection, 1, "only the 3-cycle federal program");
        assert!(out.complete);
        assert_eq!(out.stamped, 4);

        let rows = ctx
            .datasets
            .list(UNIFIED_APP, PROGRAMS_DATASET, 100)
            .await
            .unwrap();
        let federal = rows.iter().find(|r| r.key == "aln:93.912").unwrap();
        assert_eq!(federal.data["cycles_observed"], json!(3));
        assert_eq!(federal.data["next_expected_close"], json!("2027-06-01"));
        assert_eq!(federal.data["sources"], json!(["grants-gov"]));
        // The state posting keys on agency|title and earns no projection.
        let state = rows
            .iter()
            .find(|r| r.key == "cal fire|forest health")
            .unwrap();
        assert_eq!(state.data["next_expected_close"], Value::Null);

        // The identity is queryable off the posting itself.
        let stamped = ctx
            .datasets
            .get(UNIFIED_APP, UNIFIED_DATASET, "grants-gov:0")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stamped.data[PROGRAM_KEY_FIELD], json!("aln:93.912"));
        // …and the stamp is not a publication: no revision beyond the seed.
        let revs = ctx
            .datasets
            .history(UNIFIED_APP, UNIFIED_DATASET, "grants-gov:0", 10)
            .await
            .unwrap();
        assert_eq!(revs.len(), 1, "a derived stamp must not append a revision");

        // Idempotent: a second pass rewrites nothing.
        let again = roll_up_programs(&ctx).await.unwrap();
        assert_eq!(again.stamped, 0);
        assert_eq!(again.rows, 2);
    }

    #[tokio::test]
    async fn a_truncated_corpus_read_does_not_tombstone_the_registry() {
        // The anti-pattern this guards: syncing a WINDOW. With the read capped
        // below the corpus, the batch is not the dataset's current state, so
        // every program outside the window would be tombstoned by removal
        // detection — the cordis `rollup_is_complete` lesson.
        let (_store, ctx) = seeded("grants-programs-window").await;
        roll_up_programs(&ctx).await.unwrap();
        let before = ctx
            .datasets
            .list(UNIFIED_APP, PROGRAMS_DATASET, 100)
            .await
            .unwrap()
            .len();
        assert_eq!(before, 2);

        // A cap of 1 returns one row, so at most one program is rebuilt.
        let windowed = roll_up_programs_within(&ctx, 1).await.unwrap();
        assert!(!windowed.complete);
        assert!(windowed.warnings.iter().any(|w| w.contains("PARTIAL")));
        let after = ctx
            .datasets
            .list(UNIFIED_APP, PROGRAMS_DATASET, 100)
            .await
            .unwrap();
        assert_eq!(
            after.len(),
            before,
            "a windowed read tombstoned programs it never looked at"
        );
    }

    #[tokio::test]
    async fn the_registry_retires_a_program_whose_postings_left_the_corpus() {
        // The other direction of the same switch: on a COMPLETE read the batch
        // IS the whole registry, so a program with no live postings must go.
        let (_store, ctx) = seeded("grants-programs-retire").await;
        roll_up_programs(&ctx).await.unwrap();
        ctx.datasets
            .delete_record(UNIFIED_APP, UNIFIED_DATASET, "ca-grants:7")
            .await
            .unwrap();
        let out = roll_up_programs(&ctx).await.unwrap();
        assert_eq!(out.rows, 1);
        let live = ctx
            .datasets
            .list_filtered(UNIFIED_APP, PROGRAMS_DATASET, &[], None, 100)
            .await
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].key, "aln:93.912");
    }
}
