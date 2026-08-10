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

/// Within-run edge state shared between the page sink (writer, per batch) and
/// the app (reader, into the job result). Guarded by a std Mutex in the sink —
/// quick map ops only, never held across an `.await`.
#[derive(Default)]
pub struct EdgeGraph {
    /// Within-run `(from, to)` dedup — the same edge is emitted at most once
    /// per run even when a page is fingerprinted twice or repeats an href.
    seen: HashSet<String>,
    /// Within-run in-degree tally per target URL, feeding `top_linked` only
    /// (deliberately NOT persisted — in-degree datasets are out of v1).
    in_degree: HashMap<String, u64>,
    /// Links skipped because the page exceeded [`OUT_DEGREE_CAP`].
    pub dropped_out_degree: usize,
    /// Links skipped as within-run duplicates of an already-emitted edge.
    pub deduped: usize,
}

impl EdgeGraph {
    /// Turns one kept page's outbound links into dataset-ready edge records:
    /// `(key, value)` pairs keyed `{from}|{to}` with value
    /// `{from_url, to_url, depth, rel, job_id}`. Applies the per-page
    /// out-degree cap first (raw link order, so the cap is deterministic), then
    /// within-run dedup; both skip classes are tallied, never silent.
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
            if !self.seen.insert(key.clone()) {
                self.deduped += 1;
                continue;
            }
            let rel = if host_of(to_url) == from_host {
                "internal"
            } else {
                "external"
            };
            *self.in_degree.entry(to_url.clone()).or_insert(0) += 1;
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
