# Moonshot Batch 5 — Deferred Tier-1s, user-promoted (2026-07-30)

> 6 commits on `vibeman/moonshot-batch5-2026-07-30` (off merged master `0945c04` / PR #23). One wave of 5 agents.
> Baseline preserved: tests 626/0 → **670/0** (+44). Migration 0029 consumed, inventory green.

## Commits

| Commit | Item | Summary |
|---|---|---|
| `82f9501` | M33 | NOFO detail harvest — fetchOpportunity (contract pinned/assumed, loud drift), grants/opportunity_details + structured requirements (synopsis-only v1, money honest-Null, attachments stored not fetched) |
| `be00fb3` | M16 | Plugin observatory — deterministic corpus sampling, ok/trap/empty/schema-invalid taxonomy, TV-distance drift score, low_confidence honesty |
| `6ec76ce` | M10 | Extraction time machine — candidate-vs-baseline replay diff over the versioned archive, bisect_field boundaries, strictly read-only |
| `8a82559` | M13 | Saved-search materialization — searches become datasets (score-bucketed, self-reference guarded, removals via detect_removed), deltas feed triggers/watches |
| `073d77d` | M44 | Provisioner — research→sample→draft-RuleSet→dry-run-iterate proposal compiler; never writes catalog/schedules (tested invariant) |
| `b0c4e20` | — | integration (storage/config) |

## Verification

`cargo test --workspace` 670/0. Orchestrator fix-forward: engine-search test initializers needed `..Default::default()` for the new `SearchConfig.max_materialize_results` — third instance of the additive-struct-field pattern this campaign.

## Live-run follow-ups

1. **M33**: fetchOpportunity contract is ASSUMED (like cordis) — watch the first `harvestDetails:true` run; drift errors loudly.
2. **M44**: provisioner costs Claude money per run — budget ceiling respected, but first runs should use small budgets; proposals land in `provisioner/proposals` for human review → apply via `POST /catalog/reconcile`.
3. **M16/M10** need a populated crawl archive to be useful — value compounds after crawls run with the versioned sink.

## Campaign state after batch 5

19 original + 5 promoted = **24 M-ids shipped** across 5 batches / PRs #16, #23 + this branch. Remaining: M04 enforcement (live-data-gated) + 20 deferred moonshots in the vault.
