//! Link-graph persistence (M08): the crawler already extracts, canonicalizes,
//! and filters every outbound link — then throws the edges away after
//! enqueueing. This module retains that byproduct: each kept page's outbound
//! links become `(from_url, to_url, depth, rel)` edge records streamed into the
//! `edges` dataset (key `{from_url}|{to_url}`), turning one crawl into two
//! datasets. v1 deliberately persists edges only — in-degree/PageRank as
//! datasets are out of scope; the run result carries a simple `top_linked`
//! within-run summary instead.
//!
//! Honesty notes (mirroring `frontier_dropped`-style accounting):
//! - A `same_domain` crawl's links were host-filtered in core before they got
//!   here, so cross-host edges are a truncated view on such runs.
//! - Edges are captured for KEPT pages only (near-duplicate pages' links feed
//!   the frontier but are not fingerprinted, so they emit no edges).
//! - Per-page out-degree is capped ([`OUT_DEGREE_CAP`]); overflow is counted,
//!   never silent.
//! - The WITHIN-RUN bookkeeping is capped too ([`MAX_TRACKED_EDGES`]): past the
//!   budget edges still stream to the dataset but stop being tracked, and the
//!   overflow is counted in [`EdgeGraph::untracked`] — never silent.

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::host_of;

/// Dataset holding the persisted link graph: one record per distinct
/// `(from_url, to_url)` pair, keyed `{from_url}|{to_url}` so re-crawls upsert
/// in place (unchanged edges are dedup'd for free by the store's change
/// detection, like every other dataset).
pub const EDGES_DATASET: &str = "edges";

/// Max edges emitted per page (out-degree cap). Link farms / mega-nav pages
/// would otherwise make edge volume O(links) with no bound; overflow beyond the
/// cap is tallied in [`EdgeGraph::dropped_out_degree`] and reported.
pub const OUT_DEGREE_CAP: usize = 200;

/// Entries in the per-run `top_linked` summary.
pub const TOP_LINKED_LIMIT: usize = 10;

/// Max **distinct edges the run RETAINS in memory** — the bound that makes this
/// module honor the crawler's bounded-memory promise, in the same cap/count/
/// report shape as core's `MAX_FRONTIER`.
///
/// Why a cap at all: [`OUT_DEGREE_CAP`] bounds one *page's* contribution, not the
/// run's total. A crawl the frontier alone permits to reach 100k pages could
/// retain 100k × 200 = **20 million** dedup keys — each two full URLs of text —
/// beside a frontier deliberately capped at 100k URLs. That is ~8 GB, and it is
/// exactly the "pointed it at a large site overnight and it started swapping"
/// report this cap answers.
///
/// Why 200_000, stated as memory rather than taste. Per tracked edge:
/// - the dedup key is a `String` of `len(from) + 1 + len(to)` bytes plus a 24-byte
///   header — ~224 B at the ~100-byte URLs core's frontier already budgets for;
/// - it also admits at most ONE in-degree entry (an `in_degree` insert only ever
///   follows a successful `seen` insert, so `in_degree.len() <= seen.len()` — one
///   cap bounds both structures): a `to_url` clone + `u64`, ~130 B;
/// - hashbrown holds both at a ≤ 7/8 load factor with a control byte per slot,
///   so multiply the ~355 B of payload by ~1.15 → **~410 B per tracked edge**.
///
/// 200_000 × ~410 B ≈ **80 MB worst case**, ≈ 45 MB at the ~50-byte URLs that are
/// more typical — the same order as the ~13 MB frontier it accompanies, and two
/// orders below the uncapped structure. It also covers a ~1000-page crawl at the
/// full per-page out-degree cap, i.e. every crawl smaller than the ones this cap
/// exists for is tracked end to end.
pub const MAX_TRACKED_EDGES: usize = 200_000;

/// Within-run edge state shared between the page sink (writer, per batch) and
/// the app (reader, into the job result). Guarded by a std Mutex in the sink —
/// quick map ops only, never held across an `.await`.
pub struct EdgeGraph {
    /// Within-run `(from, to)` dedup — the same edge is emitted at most once
    /// per run even when a page is fingerprinted twice or repeats an href.
    /// Capped at [`EdgeGraph::tracking_budget`].
    seen: HashSet<String>,
    /// Within-run in-degree tally per target URL, feeding `top_linked` only
    /// (deliberately NOT persisted — in-degree datasets are out of v1). Bounded
    /// by `seen` (every entry here follows a successful `seen` insert).
    in_degree: HashMap<String, u64>,
    /// Distinct edges this run may retain. [`MAX_TRACKED_EDGES`] in production;
    /// a field only so this crate's tests can drive saturation without
    /// allocating 200k keys — the same reason `crawl_flushing_telemetry` takes
    /// its flush interval as a parameter.
    tracking_budget: usize,
    /// Links skipped because the page exceeded [`OUT_DEGREE_CAP`].
    pub dropped_out_degree: usize,
    /// Links skipped as within-run duplicates of an already-emitted edge.
    pub deduped: usize,
    /// Edges WRITTEN to the dataset after the run spent its tracking budget, and
    /// therefore absent from the dedup set and from the in-degree tally.
    ///
    /// The degradation this counts is deliberate, and it is the one that keeps
    /// the product intact: the `edges` dataset is the deliverable and it streams
    /// to disk (bounded storage, not bounded memory), so refusing to emit at the
    /// cap would silently truncate the thing the user asked for in order to
    /// protect a top-10 list. Instead the *bookkeeping* degrades — the two
    /// in-memory structures freeze, edges keep flowing — and the freeze is total
    /// rather than partial: once new keys stop entering `seen`, a repeated edge
    /// can no longer be recognized, so continuing to increment `in_degree` would
    /// double-count it and quietly corrupt `top_linked`. Frozen, `top_linked`
    /// stays an exactly-defined thing — the in-degree of the run's first
    /// [`MAX_TRACKED_EDGES`] distinct edges — and the result says so via
    /// [`EdgeGraph::top_linked_complete`]. A re-emitted edge is harmless
    /// downstream: the dataset is keyed `{from}|{to}`, so the second write is a
    /// no-op upsert counted in `edges_unchanged`.
    pub untracked: usize,
}

impl Default for EdgeGraph {
    fn default() -> Self {
        Self::with_tracking_budget(MAX_TRACKED_EDGES)
    }
}

/// The run-result view of one crawl's link graph, snapshotted once the crawl has
/// returned. Carries the three skip classes plus the `top_linked` summary and
/// the verdict on whether that summary saw every edge.
pub struct EdgeSummary {
    pub dropped_out_degree: usize,
    pub deduped: usize,
    pub untracked: usize,
    pub top_linked: Vec<Value>,
    pub top_linked_complete: bool,
}

impl EdgeGraph {
    /// An edge graph that retains at most `budget` distinct edges. Production
    /// uses [`EdgeGraph::default`] ([`MAX_TRACKED_EDGES`]).
    pub fn with_tracking_budget(budget: usize) -> Self {
        Self {
            seen: HashSet::new(),
            in_degree: HashMap::new(),
            tracking_budget: budget,
            dropped_out_degree: 0,
            deduped: 0,
            untracked: 0,
        }
    }

    /// Distinct edges this graph may retain before tracking freezes.
    pub fn tracking_budget(&self) -> usize {
        self.tracking_budget
    }

    /// Whether `top_linked` ranks in-degree over EVERY edge this run emitted.
    /// `false` once the tracking budget was spent, in which case `top_linked`
    /// describes the run's first [`MAX_TRACKED_EDGES`] distinct edges only.
    ///
    /// The legible verdict beside the raw `untracked` counter — the same shape
    /// as the crawl's `coverage_complete` beside `frontier_dropped`: a caller
    /// should not have to know that a zero means "this ranking saw everything".
    pub fn top_linked_complete(&self) -> bool {
        self.untracked == 0
    }

    /// Turns one kept page's outbound links into dataset-ready edge records:
    /// `(key, value)` pairs keyed `{from}|{to}` with value
    /// `{from_url, to_url, depth, rel, job_id}`. Applies the per-page
    /// out-degree cap first (raw link order, so the cap is deterministic), then
    /// within-run dedup, then the run-wide tracking budget; all three skip
    /// classes are tallied, never silent.
    ///
    /// `rel` classifies the edge as `"internal"` (same host as `from_url`) or
    /// `"external"` — computable without core carrying anchor attributes.
    pub fn page_edges(
        &mut self,
        from_url: &str,
        depth: u32,
        links: &[String],
        job_id: &str,
    ) -> Vec<(String, Value)> {
        let from_host = host_of(from_url);
        let mut out = Vec::new();
        for to_url in links {
            // Cap applies before dedup (raw link order → deterministic): every
            // link past the page's first OUT_DEGREE_CAP emitted edges is
            // counted dropped, duplicates included.
            if out.len() >= OUT_DEGREE_CAP {
                self.dropped_out_degree += 1;
                continue;
            }
            let key = format!("{from_url}|{to_url}");
            // A lookup, not an insert: past the budget the set stops GROWING but
            // keeps recognizing everything it already holds, so the run's first
            // MAX_TRACKED_EDGES edges go on deduping for free.
            if self.seen.contains(&key) {
                self.deduped += 1;
                continue;
            }
            let rel = if host_of(to_url) == from_host {
                "internal"
            } else {
                "external"
            };
            if self.seen.len() < self.tracking_budget {
                self.seen.insert(key.clone());
                *self.in_degree.entry(to_url.clone()).or_insert(0) += 1;
            } else {
                // Budget spent: write the edge, freeze the bookkeeping, count
                // the gap. See `untracked` for why freezing beats evicting,
                // half-tracking, or dropping the edge.
                self.untracked += 1;
            }
            out.push((
                key,
                json!({
                    "from_url": from_url,
                    "to_url": to_url,
                    "depth": depth,
                    "rel": rel,
                    "job_id": job_id,
                }),
            ));
        }
        out
    }

    /// Per-run summary: the [`TOP_LINKED_LIMIT`] most-linked-to URLs this run,
    /// `[{url, links_in}]`, ordered by within-run in-degree (ties broken by URL
    /// for determinism). Empty when the crawl emitted no edges.
    pub fn top_linked(&self) -> Vec<Value> {
        let mut ranked: Vec<(&String, &u64)> = self.in_degree.iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        ranked
            .into_iter()
            .take(TOP_LINKED_LIMIT)
            .map(|(url, n)| json!({ "url": url, "links_in": n }))
            .collect()
    }

    /// One snapshot of everything the run result reports about the link graph.
    /// Taken once, after the crawl has returned, so the app never reads the
    /// three tallies and the ranking through three separate lock scopes that
    /// could disagree.
    pub fn summary(&self) -> EdgeSummary {
        EdgeSummary {
            dropped_out_degree: self.dropped_out_degree,
            deduped: self.deduped,
            untracked: self.untracked,
            top_linked: self.top_linked(),
            top_linked_complete: self.top_linked_complete(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn links(urls: &[&str]) -> Vec<String> {
        urls.iter().map(|u| u.to_string()).collect()
    }

    #[test]
    fn edge_records_carry_key_and_shape() {
        let mut g = EdgeGraph::default();
        let edges = g.page_edges(
            "https://a.example/",
            1,
            &links(&["https://a.example/x", "https://b.example/y"]),
            "job-1",
        );
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].0, "https://a.example/|https://a.example/x");
        assert_eq!(edges[0].1["rel"], "internal");
        assert_eq!(edges[0].1["depth"], 1);
        assert_eq!(edges[0].1["from_url"], "https://a.example/");
        assert_eq!(edges[0].1["to_url"], "https://a.example/x");
        assert_eq!(edges[0].1["job_id"], "job-1");
        assert_eq!(edges[1].1["rel"], "external");
        assert_eq!(g.dropped_out_degree, 0);
        assert_eq!(g.deduped, 0);
    }

    #[test]
    fn within_run_dedup_across_pages_and_repeats() {
        let mut g = EdgeGraph::default();
        // Same href twice on one page → one edge + one dedup.
        let e1 = g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x", "https://a.example/x"]),
            "j",
        );
        assert_eq!(e1.len(), 1);
        assert_eq!(g.deduped, 1);
        // The identical (from, to) pair seen again later in the run → dedup'd;
        // a new pair from the same page still emits.
        let e2 = g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x", "https://a.example/z"]),
            "j",
        );
        assert_eq!(e2.len(), 1);
        assert_eq!(e2[0].1["to_url"], "https://a.example/z");
        assert_eq!(g.deduped, 2);
        // Different from-page to the same target is a DIFFERENT edge.
        let e3 = g.page_edges(
            "https://a.example/p",
            1,
            &links(&["https://a.example/x"]),
            "j",
        );
        assert_eq!(e3.len(), 1);
    }

    #[test]
    fn out_degree_cap_drops_and_reports() {
        let mut g = EdgeGraph::default();
        let many: Vec<String> = (0..OUT_DEGREE_CAP + 25)
            .map(|i| format!("https://a.example/p{i}"))
            .collect();
        let edges = g.page_edges("https://a.example/", 0, &many, "j");
        assert_eq!(edges.len(), OUT_DEGREE_CAP);
        assert_eq!(g.dropped_out_degree, 25);
        // The cap is per PAGE: the next page emits fine.
        let e2 = g.page_edges(
            "https://a.example/2",
            1,
            &links(&["https://a.example/q"]),
            "j",
        );
        assert_eq!(e2.len(), 1);
        assert_eq!(g.dropped_out_degree, 25);
    }

    #[test]
    fn top_linked_ranks_by_in_degree_with_stable_ties() {
        let mut g = EdgeGraph::default();
        // Three pages link to /hub, one links to /leaf.
        for from in [
            "https://a.example/1",
            "https://a.example/2",
            "https://a.example/3",
        ] {
            g.page_edges(from, 1, &links(&["https://a.example/hub"]), "j");
        }
        g.page_edges(
            "https://a.example/4",
            1,
            &links(&["https://a.example/leaf"]),
            "j",
        );
        let top = g.top_linked();
        assert_eq!(top[0]["url"], "https://a.example/hub");
        assert_eq!(top[0]["links_in"], 3);
        assert_eq!(top[1]["url"], "https://a.example/leaf");
        assert_eq!(top[1]["links_in"], 1);
    }

    #[test]
    fn top_linked_caps_entries_and_is_empty_without_edges() {
        let mut g = EdgeGraph::default();
        assert!(g.top_linked().is_empty());
        let many: Vec<String> = (0..TOP_LINKED_LIMIT + 5)
            .map(|i| format!("https://a.example/t{i}"))
            .collect();
        g.page_edges("https://a.example/", 0, &many, "j");
        assert_eq!(g.top_linked().len(), TOP_LINKED_LIMIT);
    }

    // ── run-wide tracking budget (bounded memory) ───────────────────────────
    //
    // THE REFUTED BEHAVIOR: `seen` and `in_degree` grew for the WHOLE run with
    // no cap anywhere near them — OUT_DEGREE_CAP bounds one page, not the run —
    // so a crawl documented as bounded-memory retained two full URLs per edge
    // until the machine swapped. Every test below drives a tiny
    // `with_tracking_budget` rather than the real 200k cap, for the same reason
    // `crawl_flushing_telemetry` takes its interval as a parameter.

    #[test]
    fn tracking_budget_bounds_the_maps_not_the_edges_written() {
        let mut g = EdgeGraph::with_tracking_budget(3);
        let edges = g.page_edges(
            "https://a.example/",
            0,
            &links(&[
                "https://a.example/1",
                "https://a.example/2",
                "https://a.example/3",
                "https://a.example/4",
                "https://a.example/5",
            ]),
            "j",
        );
        // The dataset is the deliverable and it streams to disk — every edge
        // still ships. What stops growing is the in-memory bookkeeping.
        assert_eq!(
            edges.len(),
            5,
            "the cap must not truncate the `edges` dataset"
        );
        assert_eq!(edges[4].1["to_url"], "https://a.example/5");
        assert_eq!(g.seen.len(), 3, "the dedup set is frozen at the budget");
        assert!(
            g.in_degree.len() <= 3,
            "in-degree is bounded by the dedup set: {}",
            g.in_degree.len()
        );
        // ...and the gap is counted, not silent.
        assert_eq!(g.untracked, 2);
        assert!(!g.top_linked_complete());
        assert_eq!(g.dropped_out_degree, 0, "a different skip class entirely");
        assert_eq!(g.deduped, 0);
    }

    #[test]
    fn saturated_tracking_freezes_in_degree_instead_of_double_counting_it() {
        // Past the budget a repeated edge can no longer be RECOGNIZED, so a
        // graph that kept incrementing would inflate `top_linked` — the exact
        // silent corruption `deduped_edges_do_not_inflate_in_degree` forbids
        // below the budget.
        let mut g = EdgeGraph::with_tracking_budget(2);
        g.page_edges(
            "https://a.example/1",
            1,
            &links(&["https://a.example/hub"]),
            "j",
        );
        g.page_edges(
            "https://a.example/2",
            1,
            &links(&["https://a.example/hub"]),
            "j",
        );
        assert_eq!(g.top_linked()[0]["links_in"], 2);

        // Budget spent. The same untracked edge arriving twice must move
        // `untracked`, and nothing else.
        for _ in 0..2 {
            let e = g.page_edges(
                "https://a.example/3",
                1,
                &links(&["https://a.example/hub"]),
                "j",
            );
            assert_eq!(
                e.len(),
                1,
                "still written — the store upserts it idempotently"
            );
        }
        assert_eq!(g.untracked, 2);
        let top = g.top_linked();
        assert_eq!(top.len(), 1, "no new target entered the tally: {top:?}");
        assert_eq!(top[0]["links_in"], 2, "frozen, not inflated to 3 or 4");
    }

    #[test]
    fn a_saturated_graph_still_dedups_the_edges_it_already_holds() {
        // Freezing means the set stops GROWING, not that it stops answering:
        // everything tracked before the budget goes on deduping for free.
        let mut g = EdgeGraph::with_tracking_budget(1);
        g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x"]),
            "j",
        );
        g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/y"]),
            "j",
        );
        assert_eq!(g.untracked, 1, "/y arrived past the budget");

        let repeat = g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x"]),
            "j",
        );
        assert!(repeat.is_empty(), "the tracked edge is still recognized");
        assert_eq!(g.deduped, 1);
        assert_eq!(g.untracked, 1, "a dedup hit is not an untracked edge");
    }

    #[test]
    fn a_normal_sized_crawl_never_notices_the_tracking_budget() {
        let mut g = EdgeGraph::default();
        for page in 0..50 {
            let targets: Vec<String> = (0..100)
                .map(|i| format!("https://a.example/p{page}/l{i}"))
                .collect();
            let emitted = g.page_edges(&format!("https://a.example/p{page}"), 1, &targets, "j");
            assert_eq!(emitted.len(), 100);
        }
        assert_eq!(g.untracked, 0, "5,000 edges is nowhere near the budget");
        assert!(g.top_linked_complete());
        let s = g.summary();
        assert_eq!(s.untracked, 0);
        assert!(s.top_linked_complete);
        assert_eq!(s.deduped, 0);
        assert_eq!(s.dropped_out_degree, 0);
    }

    #[test]
    fn the_default_budget_is_the_production_cap_not_a_test_seam() {
        // The injectable budget exists for the tests above; production must get
        // the documented constant, and that constant must still be the number
        // its memory rationale was written for.
        let production = EdgeGraph::default().tracking_budget();
        assert_eq!(production, MAX_TRACKED_EDGES);
        assert_eq!(MAX_TRACKED_EDGES, 200_000);
        // ~410 B per tracked edge (see MAX_TRACKED_EDGES): raising the cap has
        // to face the memory it buys, which is the whole point of this module's
        // bound.
        const BYTES_PER_TRACKED_EDGE: usize = 410;
        let worst_case = production * BYTES_PER_TRACKED_EDGE;
        assert!(
            worst_case < 100 * 1024 * 1024,
            "the link graph's within-run state must stay under ~100 MB, not {worst_case} B"
        );
    }

    #[test]
    fn deduped_edges_do_not_inflate_in_degree() {
        let mut g = EdgeGraph::default();
        g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x"]),
            "j",
        );
        g.page_edges(
            "https://a.example/",
            0,
            &links(&["https://a.example/x"]),
            "j",
        );
        let top = g.top_linked();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0]["links_in"], 1);
    }
}
