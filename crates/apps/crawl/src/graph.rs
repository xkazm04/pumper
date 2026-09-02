//! Corpus graph intelligence (N27): the consumers the `edges` dataset never had.
//!
//! [`link_graph`](crate::link_graph) streams a complete link graph to disk and
//! then nothing reads it — ranking was a within-run, memory-capped top-10 and
//! the feature doc said a whole-corpus ranking "would have to be computed from
//! the `edges` dataset". This module computes it: `mode: "graph"` pages the
//! whole `edges` dataset into a bounded in-memory graph, runs damped PageRank
//! over it with a **checkpoint per pass**, and writes one `page_rank` record per
//! URL.
//!
//! Two consumers hang off that dataset and live elsewhere:
//! - the revisit frontier's opt-in importance term (`crate::importance_order`),
//! - the plugin observatory's `sample_by: "rank"`.
//!
//! Honesty notes, in the same shape as the crawler's own accounting:
//! - the graph is capped ([`MAX_GRAPH_NODES`] / [`MAX_GRAPH_EDGES`]) and every
//!   refused edge is counted, never silent (`nodes_dropped` / `edges_dropped`,
//!   with `graph_complete` as the legible verdict);
//! - `run_at` is declared a [`DerivedPaths`] path, so re-running over an
//!   unchanged graph rewrites the stamp without appending a revision — an
//!   unchanged corpus reports `unchanged` instead of churning every watch;
//! - structural drift is detected against the PREVIOUS rollup, so the first
//!   `graph` run over a corpus can and does report zero structure changes.

use std::collections::{BTreeMap, BTreeSet};

use pumper_core::datasets::{DerivedPaths, Provenance};
use pumper_core::{AppContext, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::link_graph::EDGES_DATASET;

/// Whole-corpus ranking, one record per URL keyed by the URL itself.
pub const PAGE_RANK_DATASET: &str = "page_rank";

/// Structural-drift records: one per hub whose out-edge set shrank past the
/// threshold between two `graph` runs.
pub const STRUCTURE_CHANGES_DATASET: &str = "structure_changes";

/// Standard PageRank damping factor.
pub const DEFAULT_DAMPING: f64 = 0.85;

/// Power iterations per run. 20 is where the 5-node fixture in this module's
/// tests has converged to 1e-6, and where a web-shaped graph is well inside the
/// 1e-3 the gate asks for.
pub const DEFAULT_ITERATIONS: usize = 20;

/// Fraction of a hub's out-links that must vanish before the drop is called a
/// structural change. 0.25 by default, so the card's "a hub losing 40% of its
/// out-links" is a change and ordinary link churn is not.
pub const DEFAULT_STRUCTURE_DROP: f64 = 0.25;

/// Nodes the in-memory graph may hold. Two structures are keyed by node — the
/// URL vector and the checkpointed rank map — so this is the cap that bounds
/// both the run's memory and the size of the blob it checkpoints per pass:
/// ~60 B of URL + ~24 B of `String` header + an 8-byte rank + the same URL again
/// in the JSON checkpoint ≈ **~160 B per node**, i.e. ~8 MB at the cap, in the
/// same order as the crawler's own 80 MB edge-tracking budget.
pub const MAX_GRAPH_NODES: usize = 50_000;

/// Edges the in-memory graph may hold — one `usize` in an adjacency list each,
/// ~8 B, so 500k edges is ~4 MB plus per-node `Vec` headers.
pub const MAX_GRAPH_EDGES: usize = 500_000;

/// Rows per keyset page of the `edges` scan — the extractor backfill's paging
/// unit, for the same reason: the whole archive must never land in memory as
/// one `list()`.
pub const EDGE_PAGE: i64 = 500;

/// Checkpoint blob version — a mismatch restarts the iteration rather than
/// resuming a shape this build cannot read.
const GRAPH_STATE_VERSION: u32 = 1;

/// Dataset the source pages' current producing job is read from — the crawl's
/// own per-page fingerprints.
const PAGES_DATASET: &str = "pages";

/// Hex characters of the sha256 kept for an out-edge-set digest. 16 hex chars
/// = 64 bits: collision-free at any corpus this cap admits, and short enough
/// that `{url}|{prev}|{now}` stays a sane record key.
const DIGEST_HEX: usize = 16;

// ── the graph ───────────────────────────────────────────────────────────────

/// A bounded, index-based view of the `edges` dataset: URLs interned to
/// `usize`, out-adjacency per node, in-degree per node. Both caps count their
/// refusals rather than silently truncating.
pub struct Graph {
    ids: BTreeMap<String, usize>,
    urls: Vec<String>,
    out: Vec<Vec<usize>>,
    in_degree: Vec<u64>,
    max_nodes: usize,
    max_edges: usize,
    /// Edges accepted into the adjacency.
    pub edges: usize,
    /// Edges refused because an endpoint would have been a new node past
    /// [`Graph::max_nodes`].
    pub nodes_dropped: usize,
    /// Edges refused at [`Graph::max_edges`].
    pub edges_dropped: usize,
}

impl Default for Graph {
    fn default() -> Self {
        Self::bounded(MAX_GRAPH_NODES, MAX_GRAPH_EDGES)
    }
}

impl Graph {
    /// A graph with explicit caps. Production uses [`Graph::default`]; the
    /// tests drive tiny bounds rather than allocating 50k nodes, the same seam
    /// as `EdgeGraph::with_tracking_budget`.
    pub fn bounded(max_nodes: usize, max_edges: usize) -> Self {
        Self {
            ids: BTreeMap::new(),
            urls: Vec::new(),
            out: Vec::new(),
            in_degree: Vec::new(),
            max_nodes,
            max_edges,
            edges: 0,
            nodes_dropped: 0,
            edges_dropped: 0,
        }
    }

    fn node_id(&mut self, url: &str) -> Option<usize> {
        if let Some(i) = self.ids.get(url) {
            return Some(*i);
        }
        if self.urls.len() >= self.max_nodes {
            return None;
        }
        let i = self.urls.len();
        self.ids.insert(url.to_string(), i);
        self.urls.push(url.to_string());
        self.out.push(Vec::new());
        self.in_degree.push(0);
        Some(i)
    }

    /// Adds one `(from, to)` edge. Returns false — and counts the refusal in
    /// the matching class — when either cap is spent. The `edges` dataset is
    /// keyed `{from}|{to}`, so a scan never offers the same edge twice and this
    /// does no dedup of its own.
    pub fn add_edge(&mut self, from: &str, to: &str) -> bool {
        if self.edges >= self.max_edges {
            self.edges_dropped += 1;
            return false;
        }
        let (Some(f), Some(t)) = (self.node_id(from), self.node_id(to)) else {
            self.nodes_dropped += 1;
            return false;
        };
        self.out[f].push(t);
        self.in_degree[t] += 1;
        self.edges += 1;
        true
    }

    /// Nodes in the graph.
    pub fn len(&self) -> usize {
        self.urls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.urls.is_empty()
    }

    /// Whether the whole scanned edge set made it in — the legible verdict
    /// beside the two raw drop counters, exactly like `coverage_complete`.
    pub fn complete(&self) -> bool {
        self.nodes_dropped == 0 && self.edges_dropped == 0
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    pub fn in_degree(&self, i: usize) -> u64 {
        self.in_degree[i]
    }

    pub fn out_degree(&self, i: usize) -> usize {
        self.out[i].len()
    }

    /// Sorted, de-duplicated target URLs of node `i` — the input the structural
    /// digest is taken over.
    pub fn out_targets(&self, i: usize) -> Vec<&str> {
        let unique: BTreeSet<&str> = self.out[i].iter().map(|&t| self.urls[t].as_str()).collect();
        unique.into_iter().collect()
    }

    /// One power-iteration pass of damped PageRank.
    ///
    /// Dangling nodes (no out-links) would otherwise leak their whole mass out
    /// of the vector every pass, so their rank is redistributed uniformly —
    /// which is what keeps the result a probability distribution (the tests
    /// pin the sum at 1.0).
    pub fn pagerank_pass(&self, ranks: &[f64], damping: f64) -> Vec<f64> {
        let n = self.urls.len();
        if n == 0 {
            return Vec::new();
        }
        let nf = n as f64;
        let dangling: f64 = self
            .out
            .iter()
            .enumerate()
            .filter(|(_, o)| o.is_empty())
            .map(|(i, _)| ranks.get(i).copied().unwrap_or(0.0))
            .sum();
        let base = (1.0 - damping) / nf + damping * dangling / nf;
        let mut next = vec![base; n];
        for (i, outs) in self.out.iter().enumerate() {
            if outs.is_empty() {
                continue;
            }
            let share = damping * ranks.get(i).copied().unwrap_or(0.0) / outs.len() as f64;
            for &t in outs {
                next[t] += share;
            }
        }
        next
    }

    /// The uniform starting vector: every node at `1/N`.
    pub fn uniform_ranks(&self) -> Vec<f64> {
        let n = self.urls.len();
        if n == 0 {
            return Vec::new();
        }
        vec![1.0 / n as f64; n]
    }

    /// `iterations` passes from the uniform vector — the whole computation in
    /// one call, for tests and for anyone who does not need the checkpoint.
    pub fn pagerank(&self, damping: f64, iterations: usize) -> Vec<f64> {
        let mut ranks = self.uniform_ranks();
        for _ in 0..iterations {
            ranks = self.pagerank_pass(&ranks, damping);
        }
        ranks
    }
}

/// Where one stored edge sits relative to its source page's latest crawl.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeVintage {
    /// The source page's latest crawl emitted this edge.
    Current,
    /// A crawl emitted it once; the page's latest crawl did not — history.
    Superseded,
    /// The source page has no live `pages` record, so nothing says which crawl
    /// is its latest. Admitted (a graph that silently dropped these would
    /// under-report structure) and counted separately, never folded into
    /// `Superseded`.
    Unattributed,
}

/// Whether a stored edge belongs to its source page's **current** out-edge set.
///
/// THE PROBLEM THIS SOLVES, and it is the whole reason structural drift is
/// detectable at all: `edges` is upsert-only — "an edge absent this run is NOT
/// removed" — so a hub that drops two of its five out-links still has five rows
/// in the dataset forever. Reading out-degree straight off the dataset reports
/// that hub as unchanged, which is exactly the blind spot the card names, and
/// it is what the first cut of this module did.
///
/// The join that closes it needs no new column and no clock: both datasets
/// already carry the producing `job_id` — the `pages` record's is rewritten by
/// whichever crawl last fingerprinted the page, and each edge's is the crawl
/// that emitted it. Equal means the page's latest crawl still emits this edge.
/// (Wall-clock freshness was the alternative and it is strictly worse: two
/// crawls inside one clock tick are indistinguishable, which is precisely the
/// case a test — or a fast re-crawl — produces.)
pub fn edge_vintage(edge_job: Option<&str>, page_job: Option<&str>) -> EdgeVintage {
    match (edge_job, page_job) {
        (_, None) => EdgeVintage::Unattributed,
        (Some(e), Some(p)) if e == p => EdgeVintage::Current,
        (Some(_), Some(_)) => EdgeVintage::Superseded,
        // An edge with no stamp at all predates provenance on this write path;
        // it cannot be shown to be superseded, so it is not called superseded.
        (None, Some(_)) => EdgeVintage::Unattributed,
    }
}

/// sha256-derived digest of one node's out-edge set — the structural
/// fingerprint stored on a `page_rank` record so the NEXT run can tell a
/// re-pointed nav bar from a shrunken one.
pub fn out_edges_digest(targets: &[&str]) -> String {
    let mut sorted: Vec<&str> = targets.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut h = Sha256::new();
    for t in sorted {
        h.update(t.as_bytes());
        h.update(b"\n");
    }
    format!("{:x}", h.finalize())[..DIGEST_HEX].to_string()
}

// ── structural drift ────────────────────────────────────────────────────────

/// One node's structural facts as the PREVIOUS `graph` run left them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PriorNode {
    pub out_degree: u64,
    pub digest: String,
}

/// Whether a hub's out-edge set shrank enough to be a structural change, and
/// the record that says so.
///
/// Extracted and tested rather than inlined in the run loop because the whole
/// signal is this predicate: edges are upsert-only (an edge absent this run is
/// NOT removed from the dataset), so a hub that loses 40% of its out-links
/// looks *unchanged* to every other detector the platform has — simhash sees a
/// different page, not a different site map, and the health detector sees
/// nothing at all.
///
/// Only SHRINKAGE counts. A hub that gained links, or re-pointed the same
/// number of them, is not a structural loss; the digest change is recorded on
/// the `page_rank` record either way, and inventing a "change" for growth would
/// make the dataset a change log rather than a drift signal.
pub fn structure_change(
    url: &str,
    prior: &PriorNode,
    out_degree: u64,
    digest: &str,
    threshold: f64,
    detected_at: &str,
    job_id: &str,
) -> Option<(String, Value)> {
    if prior.out_degree == 0 || out_degree >= prior.out_degree {
        return None;
    }
    let removed = prior.out_degree - out_degree;
    let fraction = removed as f64 / prior.out_degree as f64;
    if fraction < threshold {
        return None;
    }
    let key = format!("{url}|{}|{digest}", prior.digest);
    Some((
        key,
        json!({
            "url": url,
            "previous_out_degree": prior.out_degree,
            "out_degree": out_degree,
            "links_removed": removed,
            "dropped_fraction": (fraction * 10_000.0).round() / 10_000.0,
            "previous_digest": prior.digest,
            "digest": digest,
            "detected_at": detected_at,
            "job_id": job_id,
        }),
    ))
}

// ── checkpointed iteration ──────────────────────────────────────────────────

/// The resumable unit of a `graph` run: which pass finished and the rank vector
/// it produced.
///
/// The edge scan is NOT checkpointed — it is a keyset read of a dataset that
/// does not move under the run, so re-reading it on resume costs one scan,
/// while re-running the passes costs `iterations × edges`. The passes are what
/// the checkpoint buys back.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphState {
    pub v: u32,
    /// Passes already applied to `ranks`.
    #[serde(default)]
    pub iteration: usize,
    /// The rank vector keyed by URL, so a resumed run can map it back onto a
    /// graph whose node ORDER need not match the interrupted attempt's.
    #[serde(default)]
    pub ranks: BTreeMap<String, f64>,
}

impl GraphState {
    /// Advisory restore, in this repo's house style: anything that is not a
    /// current-version state **for this exact node set** restarts the
    /// iteration from the uniform vector rather than erroring. A checkpoint
    /// whose nodes no longer match the loaded graph is not "close enough" —
    /// resuming across a changed corpus would publish ranks that were never
    /// computed over it.
    pub fn restore(restored: Option<&Value>, nodes: &[String]) -> Self {
        let fresh = GraphState {
            v: GRAPH_STATE_VERSION,
            ..GraphState::default()
        };
        let Some(st) = restored
            .and_then(|v| serde_json::from_value::<GraphState>(v.clone()).ok())
            .filter(|s| s.v == GRAPH_STATE_VERSION && s.iteration > 0)
        else {
            return fresh;
        };
        if st.ranks.len() != nodes.len() || !nodes.iter().all(|u| st.ranks.contains_key(u)) {
            return fresh;
        }
        st
    }

    /// The checkpointed vector in this graph's node order.
    pub fn ranks_for(&self, nodes: &[String]) -> Option<Vec<f64>> {
        if self.iteration == 0 {
            return None;
        }
        nodes.iter().map(|u| self.ranks.get(u).copied()).collect()
    }

    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

// ── the run ─────────────────────────────────────────────────────────────────

fn f64_param(ctx: &AppContext, key: &str, default: f64, lo: f64, hi: f64) -> f64 {
    ctx.params
        .get(key)
        .and_then(Value::as_f64)
        .map(|v| v.clamp(lo, hi))
        .unwrap_or(default)
}

/// Prior `page_rank` records as `{url: PriorNode}` — the comparison base for
/// structural drift. Records that predate the digest field contribute an empty
/// digest, which changes nothing: the predicate keys on out-degree.
fn prior_nodes(records: &[pumper_core::datasets::Record]) -> BTreeMap<String, PriorNode> {
    let mut out = BTreeMap::new();
    for r in records {
        if r.removed_at.is_some() {
            continue;
        }
        let Some(out_degree) = r.data.get("out_degree").and_then(Value::as_u64) else {
            continue;
        };
        out.insert(
            r.key.clone(),
            PriorNode {
                out_degree,
                digest: r
                    .data
                    .get("out_edges_digest")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            },
        );
    }
    out
}

/// `mode: "graph"` — whole-corpus PageRank over the persisted `edges` dataset,
/// plus structural-drift records against the previous rollup.
pub async fn run_graph(ctx: &AppContext) -> Result<Value> {
    let damping = f64_param(ctx, "damping", DEFAULT_DAMPING, 0.0, 0.99);
    let iterations = ctx
        .params
        .get("iterations")
        .and_then(Value::as_u64)
        .map(|n| (n.max(1) as usize).min(100))
        .unwrap_or(DEFAULT_ITERATIONS);
    let threshold = f64_param(
        ctx,
        "structure_drop_threshold",
        DEFAULT_STRUCTURE_DROP,
        0.0,
        1.0,
    );

    // 1a. Which crawl each source page's latest fingerprint came from — the
    //     join key that separates today's site map from the upsert-only
    //     dataset's history. Bounded like every other structure here.
    let mut page_job: BTreeMap<String, String> = BTreeMap::new();
    let mut after: Option<(String, String)> = None;
    loop {
        let batch = ctx
            .datasets
            .list_page(&ctx.app, PAGES_DATASET, after.clone(), EDGE_PAGE, None)
            .await?;
        let Some(last) = batch.last() else { break };
        after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
        let short = (batch.len() as i64) < EDGE_PAGE;
        for r in &batch {
            if r.removed_at.is_some() || page_job.len() >= MAX_GRAPH_NODES {
                continue;
            }
            if let Some(job) = r.data.get("job_id").and_then(Value::as_str) {
                page_job.insert(r.key.clone(), job.to_string());
            }
        }
        if short {
            break;
        }
    }

    // 1b. Page the whole `edges` dataset into the bounded graph (the extractor
    //     backfill's keyset-paging shape: never one `list()` of the corpus).
    let mut graph = Graph::default();
    let mut edges_scanned = 0usize;
    let mut edges_superseded = 0usize;
    let mut edges_unattributed = 0usize;
    let mut edge_pages = 0usize;
    let mut after: Option<(String, String)> = None;
    loop {
        let batch = ctx
            .datasets
            .list_page(&ctx.app, EDGES_DATASET, after.clone(), EDGE_PAGE, None)
            .await?;
        let Some(last) = batch.last() else { break };
        after = Some((pumper_core::datasets::ts(last.updated_at), last.key.clone()));
        edge_pages += 1;
        let short = (batch.len() as i64) < EDGE_PAGE;
        for r in &batch {
            if r.removed_at.is_some() {
                continue;
            }
            let (Some(from), Some(to)) = (
                r.data.get("from_url").and_then(Value::as_str),
                r.data.get("to_url").and_then(Value::as_str),
            ) else {
                continue;
            };
            edges_scanned += 1;
            match edge_vintage(
                r.data.get("job_id").and_then(Value::as_str),
                page_job.get(from).map(String::as_str),
            ) {
                EdgeVintage::Current => {}
                // An edge the source page's latest crawl no longer emits is
                // history, not structure. Counted, never silently folded in.
                EdgeVintage::Superseded => {
                    edges_superseded += 1;
                    continue;
                }
                EdgeVintage::Unattributed => edges_unattributed += 1,
            }
            graph.add_edge(from, to);
        }
        if short {
            break;
        }
    }

    // 2. Power iterations, checkpointed per pass.
    let nodes: Vec<String> = graph.urls().to_vec();
    let mut st = GraphState::restore(ctx.restore(), &nodes);
    let resumed = st.iteration > 0;
    let mut ranks = st
        .ranks_for(&nodes)
        .unwrap_or_else(|| graph.uniform_ranks());
    while st.iteration < iterations && !graph.is_empty() {
        ranks = graph.pagerank_pass(&ranks, damping);
        st.iteration += 1;
        st.ranks = nodes
            .iter()
            .cloned()
            .zip(ranks.iter().copied())
            .collect::<BTreeMap<String, f64>>();
        ctx.checkpoint(st.to_value()).await;
    }

    // 3. Compare against the previous rollup BEFORE overwriting it.
    let prior = prior_nodes(
        &ctx.datasets
            .list(&ctx.app, PAGE_RANK_DATASET, MAX_GRAPH_NODES as i64)
            .await
            .unwrap_or_default(),
    );

    let run_at = chrono::Utc::now().to_rfc3339();
    let job_id = ctx.job_id.to_string();
    let mut rows: Vec<(String, Value)> = Vec::with_capacity(nodes.len());
    let mut changes: Vec<(String, Value)> = Vec::new();
    for (i, url) in nodes.iter().enumerate() {
        let digest = out_edges_digest(&graph.out_targets(i));
        let out_degree = graph.out_degree(i) as u64;
        if let Some(p) = prior.get(url) {
            if let Some(rec) =
                structure_change(url, p, out_degree, &digest, threshold, &run_at, &job_id)
            {
                changes.push(rec);
            }
        }
        rows.push((
            url.clone(),
            json!({
                "url": url,
                "rank": ranks.get(i).copied().unwrap_or(0.0),
                "in_degree": graph.in_degree(i),
                "out_degree": out_degree,
                "out_edges_digest": digest,
                "run_at": run_at,
            }),
        ));
    }

    // 4. Write. `run_at` is derived, so a re-run over an unchanged graph
    //    refreshes the stamp without appending a revision — `unchanged`.
    let prov = Provenance {
        job_id: Some(job_id.clone()),
        ..Provenance::default()
    };
    let derived = DerivedPaths::new(["run_at"]);
    let summary = if rows.is_empty() {
        Default::default()
    } else {
        ctx.datasets
            .upsert_many_derived(
                &ctx.app,
                PAGE_RANK_DATASET,
                &rows,
                None,
                Some(&prov),
                &derived,
            )
            .await?
    };
    let mut structure_changes_written = 0usize;
    if !changes.is_empty() {
        let s = ctx
            .datasets
            .upsert_many_stamped(
                &ctx.app,
                STRUCTURE_CHANGES_DATASET,
                &changes,
                None,
                Some(&prov),
            )
            .await?;
        structure_changes_written = s.new.len() + s.changed.len();
    }

    let mut out = json!({
        "mode": "graph",
        "graph_dataset": PAGE_RANK_DATASET,
        "structure_changes_dataset": STRUCTURE_CHANGES_DATASET,
        "edges_scanned": edges_scanned,
        // Rows the source page's latest crawl no longer emits — the
        // upsert-only dataset's history, excluded from today's structure and
        // counted rather than silently folded in. `edges_unattributed` are the
        // rows whose source page has no live `pages` record to date them
        // against: admitted, and named rather than mixed into either verdict.
        "edges_superseded": edges_superseded,
        "edges_unattributed": edges_unattributed,
        "edge_pages": edge_pages,
        "nodes": nodes.len(),
        // Honest capping, in the crawler's own shape: two raw counters plus the
        // verdict, so a caller never has to know that two zeros mean "the whole
        // edge set is in this ranking".
        "nodes_dropped": graph.nodes_dropped,
        "edges_dropped": graph.edges_dropped,
        "graph_complete": graph.complete(),
        "damping": damping,
        "iterations": st.iteration,
        "resumed": resumed,
        "page_rank_new": summary.new.len(),
        "page_rank_changed": summary.changed.len(),
        "page_rank_unchanged": summary.unchanged,
        "structure_drop_threshold": threshold,
        "structure_changes_written": structure_changes_written,
        "structure_baseline": !prior.is_empty(),
    });
    if let (false, Value::Object(map)) = (graph.complete(), &mut out) {
        map.insert(
            "warnings".into(),
            json!([format!(
                "PARTIAL graph: {} edges refused at the {}-node cap and {} at the {}-edge cap; \
                 the ranking covers only what was admitted",
                graph.nodes_dropped, MAX_GRAPH_NODES, graph.edges_dropped, MAX_GRAPH_EDGES
            )]),
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The card's fixture: A→{B,C}, B→C, C→A, D→{C,E}, E dangling.
    fn five_node() -> Graph {
        let mut g = Graph::default();
        for (f, t) in [
            ("A", "B"),
            ("A", "C"),
            ("B", "C"),
            ("C", "A"),
            ("D", "C"),
            ("D", "E"),
        ] {
            assert!(g.add_edge(f, t));
        }
        g
    }

    fn rank_of(g: &Graph, ranks: &[f64], url: &str) -> f64 {
        let i = g.urls().iter().position(|u| u == url).expect("node");
        ranks[i]
    }

    #[test]
    fn pagerank_matches_the_hand_computed_vector() {
        // Reference vector for A→{B,C}, B→C, C→A, D→{C,E}, E dangling at
        // d = 0.85, computed independently (power iteration to convergence,
        // dangling mass redistributed uniformly):
        //   A 0.350178  B 0.188417  C 0.365397  D 0.039591  E 0.056417
        let g = five_node();
        let ranks = g.pagerank(DEFAULT_DAMPING, DEFAULT_ITERATIONS);
        for (url, want) in [
            ("A", 0.350178),
            ("B", 0.188417),
            ("C", 0.365397),
            ("D", 0.039591),
            ("E", 0.056417),
        ] {
            let got = rank_of(&g, &ranks, url);
            assert!(
                (got - want).abs() < 1e-3,
                "{url}: {got} is not within 1e-3 of the hand-computed {want}"
            );
        }
        // A rank vector that does not sum to 1 is not a probability
        // distribution — the dangling-mass term is what keeps it one.
        let sum: f64 = ranks.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "ranks sum to {sum}, not 1");
    }

    #[test]
    fn a_dangling_node_does_not_leak_the_vector_away() {
        // THE REFUTED BEHAVIOR: without the dangling term, E's mass vanishes
        // every pass and the whole vector decays toward (1-d)/N — every rank
        // wrong, and wrong in a way that still "looks like" a ranking.
        let mut g = Graph::default();
        assert!(g.add_edge("A", "B")); // B is dangling
        let ranks = g.pagerank(DEFAULT_DAMPING, 50);
        let sum: f64 = ranks.iter().sum();
        assert!((sum - 1.0).abs() < 1e-9, "sum {sum}");
        assert!(
            rank_of(&g, &ranks, "B") > rank_of(&g, &ranks, "A"),
            "the linked-to node must still outrank the linker: {ranks:?}"
        );
    }

    #[test]
    fn an_empty_graph_ranks_nothing_instead_of_dividing_by_zero() {
        let g = Graph::default();
        assert!(g.is_empty());
        assert!(g.pagerank(DEFAULT_DAMPING, DEFAULT_ITERATIONS).is_empty());
        assert!(g.complete());
    }

    #[test]
    fn the_node_cap_refuses_edges_and_counts_them() {
        let mut g = Graph::bounded(2, 100);
        assert!(g.add_edge("A", "B"));
        assert!(!g.add_edge("A", "C"), "C would be a third node");
        assert_eq!(g.len(), 2);
        assert_eq!(g.nodes_dropped, 1);
        assert_eq!(g.edges, 1);
        assert!(!g.complete(), "a capped graph must never look complete");
    }

    #[test]
    fn the_edge_cap_refuses_edges_and_counts_them_separately() {
        let mut g = Graph::bounded(100, 1);
        assert!(g.add_edge("A", "B"));
        assert!(!g.add_edge("A", "C"));
        assert_eq!(g.edges_dropped, 1);
        assert_eq!(g.nodes_dropped, 0, "a different refusal class entirely");
        assert!(!g.complete());
    }

    #[test]
    fn in_and_out_degree_come_off_the_same_scan_as_the_rank() {
        let g = five_node();
        let i = |u: &str| g.urls().iter().position(|x| x == u).unwrap();
        assert_eq!(g.out_degree(i("A")), 2);
        assert_eq!(g.in_degree(i("C")), 3);
        assert_eq!(g.out_degree(i("E")), 0);
        assert_eq!(g.in_degree(i("D")), 0);
    }

    #[test]
    fn a_digest_is_order_independent_and_moves_when_a_target_moves() {
        let a = out_edges_digest(&["https://x/1", "https://x/2"]);
        let b = out_edges_digest(&["https://x/2", "https://x/1"]);
        assert_eq!(a, b, "link order is not structure");
        assert_ne!(a, out_edges_digest(&["https://x/1", "https://x/3"]));
        assert_eq!(a.len(), DIGEST_HEX);
    }

    // ── edge freshness ──────────────────────────────────────────────────────

    #[test]
    fn an_edge_the_latest_crawl_did_not_re_emit_is_not_current() {
        // THE REFUTED BEHAVIOR: reading out-degree straight off an upsert-only
        // dataset. A hub that dropped two of five links still has five rows, so
        // every structural loss read as "unchanged" — the exact blind spot this
        // module exists to close.
        assert_eq!(
            edge_vintage(Some("job-2"), Some("job-2")),
            EdgeVintage::Current
        );
        assert_eq!(
            edge_vintage(Some("job-1"), Some("job-2")),
            EdgeVintage::Superseded,
            "an older crawl's edge is history, not today's structure"
        );
    }

    #[test]
    fn an_edge_that_cannot_be_dated_is_admitted_and_named_not_called_stale() {
        // Honest absence: nothing here says the edge is gone, so it is neither
        // dropped nor counted as a structural loss — a graph that silently
        // dropped it would under-report every hub whose `pages` record was
        // pruned.
        assert_eq!(edge_vintage(Some("job-1"), None), EdgeVintage::Unattributed);
        assert_eq!(edge_vintage(None, Some("job-1")), EdgeVintage::Unattributed);
        assert_eq!(edge_vintage(None, None), EdgeVintage::Unattributed);
    }

    // ── structural drift ────────────────────────────────────────────────────

    #[test]
    fn a_hub_losing_forty_percent_of_its_out_links_is_a_structure_change() {
        let prior = PriorNode {
            out_degree: 10,
            digest: "old".into(),
        };
        let (key, rec) = structure_change(
            "https://x/hub",
            &prior,
            6,
            "new",
            DEFAULT_STRUCTURE_DROP,
            "2026-09-02T00:00:00Z",
            "job-1",
        )
        .expect("a 40% drop is a structural change");
        assert_eq!(key, "https://x/hub|old|new");
        assert_eq!(rec["previous_out_degree"], 10);
        assert_eq!(rec["out_degree"], 6);
        assert_eq!(rec["links_removed"], 4);
        assert_eq!(rec["dropped_fraction"], 0.4);
        assert_eq!(rec["url"], "https://x/hub");
        assert_eq!(rec["job_id"], "job-1");
    }

    #[test]
    fn ordinary_churn_and_growth_are_not_structure_changes() {
        let prior = PriorNode {
            out_degree: 10,
            digest: "old".into(),
        };
        let t = DEFAULT_STRUCTURE_DROP;
        let at = "2026-09-02T00:00:00Z";
        // One link of ten gone — below the threshold.
        assert!(structure_change("u", &prior, 9, "new", t, at, "j").is_none());
        // Re-pointed, same count: the digest moved, the structure did not shrink.
        assert!(structure_change("u", &prior, 10, "new", t, at, "j").is_none());
        // Grew.
        assert!(structure_change("u", &prior, 20, "new", t, at, "j").is_none());
        // A node that had no out-links cannot lose a fraction of them.
        let empty = PriorNode {
            out_degree: 0,
            digest: String::new(),
        };
        assert!(structure_change("u", &empty, 0, "new", t, at, "j").is_none());
    }

    #[test]
    fn a_hub_that_vanished_entirely_is_the_strongest_change_not_a_missing_one() {
        let prior = PriorNode {
            out_degree: 40,
            digest: "old".into(),
        };
        let (_, rec) =
            structure_change("u", &prior, 0, "empty", DEFAULT_STRUCTURE_DROP, "now", "j")
                .expect("losing every out-link is a change");
        assert_eq!(rec["dropped_fraction"], 1.0);
    }

    // ── checkpointed iteration ──────────────────────────────────────────────

    #[test]
    fn a_checkpoint_resumes_the_passes_it_had_already_paid_for() {
        let g = five_node();
        let nodes: Vec<String> = g.urls().to_vec();
        // Ten passes, checkpointed.
        let mut ranks = g.uniform_ranks();
        for _ in 0..10 {
            ranks = g.pagerank_pass(&ranks, DEFAULT_DAMPING);
        }
        let st = GraphState {
            v: GRAPH_STATE_VERSION,
            iteration: 10,
            ranks: nodes.iter().cloned().zip(ranks.iter().copied()).collect(),
        };
        // Restored, then run to 20 — identical to 20 passes from scratch.
        let restored = GraphState::restore(Some(&st.to_value()), &nodes);
        assert_eq!(restored.iteration, 10);
        let mut resumed = restored.ranks_for(&nodes).expect("a resumable vector");
        for _ in restored.iteration..DEFAULT_ITERATIONS {
            resumed = g.pagerank_pass(&resumed, DEFAULT_DAMPING);
        }
        let scratch = g.pagerank(DEFAULT_DAMPING, DEFAULT_ITERATIONS);
        for (a, b) in resumed.iter().zip(scratch.iter()) {
            assert!((a - b).abs() < 1e-12, "{a} vs {b}");
        }
    }

    #[test]
    fn a_checkpoint_over_a_different_corpus_restarts_instead_of_publishing_a_lie() {
        // THE ANTI-PATTERN: mapping a stale rank vector onto a graph that has
        // since gained or lost nodes and calling the result "resumed" —
        // publishing ranks that were never computed over this corpus.
        let g = five_node();
        let nodes: Vec<String> = g.urls().to_vec();
        let st = GraphState {
            v: GRAPH_STATE_VERSION,
            iteration: 10,
            ranks: [("A".to_string(), 0.5), ("Z".to_string(), 0.5)]
                .into_iter()
                .collect(),
        };
        let restored = GraphState::restore(Some(&st.to_value()), &nodes);
        assert_eq!(restored.iteration, 0, "a mismatched node set restarts");
        assert!(restored.ranks_for(&nodes).is_none());

        // Same for a blob from a future/older shape, and for no blob at all.
        let wrong_version = json!({"v": 99, "iteration": 5, "ranks": {}});
        assert_eq!(
            GraphState::restore(Some(&wrong_version), &nodes).iteration,
            0
        );
        assert_eq!(GraphState::restore(None, &nodes).iteration, 0);
        assert_eq!(
            GraphState::restore(Some(&json!("junk")), &nodes).iteration,
            0
        );
    }
}
