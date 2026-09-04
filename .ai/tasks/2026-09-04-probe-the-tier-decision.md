# Task — the tier decision is instrumented and nothing probes it

- **Opened:** 2026-09-04 by registry run `odr-2026-09-04`
- **Registry subject:** `software-engineering/llm-agent/evaluation-and-cost/eval-harness`
- **Technique:** `probe-the-decision-not-the-artifact` (new)
- **Mode:** `task`
- **Status:** proposed, first step not taken

## What the tree already gets right

The technique's hardest precondition — *the decision is observable in state,
not merely inferable from the output* — is **already met here, independently**.
`crates/core/src/fetcher.rs:304-320` defines `TierVerdict` as a closed
vocabulary (thin content, a block marker, a tier error), records one per
escalation, and its own comment says consumers should branch on the verdict
"instead of parsing" the human-readable trail. Most trees reach this technique
and discover they must add the record first. This one has it.

## The gap

`evals/tier3-extraction` measures the **artifact**: the Markdown the Claude tier
produced, against a golden. It is a good suite — real frozen fixtures, recorded
transcripts replayed so a re-run costs nothing (`recorded.total_cost_usd: 2.62`,
paid once).

Nothing measures the **decision** that put a page on that tier. Whether a page
should have escalated past HTTP, past the browser, or not at all is a choice
with three named answers, labellable from the frozen body by a competent
reviewer, and already emitted as a typed record. It is the cheapest suite this
repo is not running, and a wrong tier choice is more expensive than a mediocre
extraction: it either burns a metered tier-3 call on a page HTTP could read, or
returns thin content from a page it could not.

## The change

A second eval beside the first, over the **same frozen fixtures** (no new
capture, no new spend):

1. Add a `tier_expected` field per fixture in `evals/tier3-extraction/manifest.json`
   — or a sibling manifest — labelled by reading the frozen body.
2. A runner that drives the fetcher against the frozen body with the tier-3
   engine **stubbed to panic**, and asserts the `TierVerdict` sequence against
   the label. The stub is the point: a probe that reaches tier 3 has either
   found a real escalation or a bug, and either way it must not pay for it.
3. Assert the verdict's **position** as well as its value — which escalation
   this is — per the technique's first failure mode. A probe that cannot say
   which escalation it read is measuring a habit, not a decision.

## The measurable

Tier decisions correct over tier decisions labelled, on the frozen fixture set,
reported with the fixture count. It is a **threshold**, never optimized: the
right tier can still extract badly, and a fetcher tuned to this number would
route well and read poorly.

**Falsifier.** If every fixture's label is the tier the fetcher already picks,
the suite discriminates nothing and should not ship — the fixtures were selected
by text density for an *extraction* eval, and may all sit on one side of the
tier decision. Check the label distribution before writing the runner.

## Size and gate

One manifest field per fixture (11 fixtures), one runner in
`crates/core/tests/`, a panicking tier-3 stub: ~120-180 lines, no new
dependencies, runs in `cargo test` with no network and no spend.

## Why it is a branch

It adds a suite rather than changing behaviour, and step 0 (check the label
distribution) can kill it. Open `direction/tier-decision-probe`.
