---
name: trades-pricing
type: perfect/context
group: Market Data
category: lib
opportunity: 5
last_proposed: 2026-08-13
cooldown_until: after-round-23
directions: ["[[trades-numbers-mean-what-they-say]]"]
alias_of_old_map: "[[us-trades-wages-tax-valuation]] (round-1 context family)"
---

## Current state (scout brief digest, 2026-08-13, round 21 — first sweep on the 46-map)
Scouted "very thorough" **as one crate family with [[trades-operator-economics]]** — see that note
for the full topology, the adoption matrix and the shared-spine analysis. This context owns
`homewyse-pricing` (409 L), `valuation-multiples` (389 L) and `cms-fee-schedule` (1,140 L).
**Opportunity re-scored 4 → 5.**

**`cms-fee-schedule` is the odd one out and that is a finding, not a gap.** It does not use
`trades-common` at all and drives no LLM (`cost_class: Free`) — it is a deterministic CMS PFS
ingester with layout pins documented at `:24-52`, a real in-memory ZIP round-trip test and a sha256
determinism pin. **It is the best-tested surface in the entire family** and nothing in it cleared the
bar this round. Its one real defect is banked below.

**The finding this context owns.** `homewyse-pricing` is the family's *reference implementation* for
storing validated numbers rather than raw model JSON — and it wrote the reasoning into the code
(`:255-261`: *"a string-quoted price ("1234") passes validation via validate::num but, stored raw, is
read back as a non-number and silently dropped from the rollup"*). **Four of five siblings never
adopted it.** So `"top_marginal_rate": "13.3"` passes `require_rate` as 13.3, stores as a string, and
`sync_operator_economics:995`'s `.and_then(Value::as_f64)` silently drops that state from
`median_state_rate`. The catalog's `ranges` contract cannot see it either — ranges are checked only
when the value is *numeric*.

And the same app that fixed the numbers **fabricates a semantic field**: `"unit":
…unwrap_or("flat")` (`:254`), with `unit` absent from the schema's required list — so a $150/hour
electrician rate is stored as a $150 flat job. It is the one outright fabrication in a family that
otherwise honours honest-Null (`cms:42` even pins "never a fabricated 0.00").

**The unit-convention defect is the sharpest thing in the brief and it lives in a test.**
`state-tax`'s prompt demands percentages (*"Rates are percentages (e.g. 13.3…)"*, `:148-149`);
`require_rate` accepts `[0,100]`, which admits `13.3` and `0.133` identically; and `trades-common`'s
own consumer test `state_tax_context_carries_the_real_rate_not_a_median` (`:1294-1305`) uses
**fractions**. The producer's prompt and the consumer's only test disagree about the unit, nothing
records which is authoritative, and a downstream forecast reading `13.3` as a fraction computes a
1330% set-aside. **This is the family's one test pinning a wrong contract, and it sits in the shared
crate.**

## Direction history
Round 21 (2026-08-13) — gate: director-self-gated (autonomous, Athena-dispatched). 1 accepted here
(3 across the family), 2 rejected-deferred.

**ACCEPTED**
- [[trades-numbers-mean-what-they-say]] — robustness · M. Numeric-string storage + the undeclared
  rate unit + the unclosed `income_tax_type` vocabulary, with `homewyse`'s `unwrap_or("flat")`
  fabrication and `state-licensing`'s `unwrap_or(0.0)` cost as riders. Criterion 6 is the load-bearing
  one: **a fix applied to four of five apps is how this defect was born — every app adopts it.**

**REJECTED-DEFERRED (banked, with why now-is-wrong)**
- **`cms-fee-schedule` reports a failed corpus ingest as a green job** — a download/extract/parse
  failure is `tracing::error!`-logged and surfaces only as `parse: {status:"error"}` nested in the
  result (`:771-788`), with no `warnings[]` and no top-level flag, so a consumer keying on job status
  sees green over a stale corpus. It is **deliberate and documented** (`:20-22`), which is why it did
  not clear the bar as a bug this round — changing it is a product decision about whether a partial
  ingest should fail the job, and it deserves to be made explicitly rather than folded into a
  correctness sweep. **Banked as this context's anchor for r23+.**
- **`trade-wages` / `valuation-multiples` fork their freshness sentinel off the compile-time
  `Trade::ALL[0]`** (`:140`, `:114`) instead of the live registry taxonomy the same run prompts with.
  If a human governs Plumbing off via `trades/taxonomy`, the sentinel `US:Plumbing` is never written,
  the vintage/age gate can never hold, and **every run re-pays**. `state-licensing` avoided it by
  keying the sentinel on the trade being researched (`:230`). Genuinely valuable and cheap, but it is
  a cost-correctness item in a round whose three slots went to data-correctness; it needs the
  taxonomy-governance path traced before it is specified. **Banked.**

**Also recorded (no action, so a future round does not re-derive it):** `trade-wages`'s proposer mode
emits five structurally different undeclared result shapes and registers no provenance; `state-licensing`'s
`max_row_delta_pct = 5.0` is structurally inert (it needs `removed > 0`, and the app never tombstones)
and `docs/features/catalog.md:49`'s "known inert" list misses it; `state-licensing` is entirely absent
from `docs/features/apps.md` while three lines there still say "the four trades apps"; `apps.md:38`
documents two dead functions (`taxonomy::canonicalize`, `prompt_list`) as the ones the apps use.
The doc items are Director-applied Class C this round.

## Shipped
- (round 21 in flight — filled at wrap)
- (inherited, pre-46-map — see [[us-trades-wages-tax-valuation]])
