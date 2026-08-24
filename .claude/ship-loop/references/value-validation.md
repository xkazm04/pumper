# Dimension 9 — Source & cost value

Purpose: kill the "technically correct, practically pointless" failure mode. Dimensions 1–8 prove the service *works*; this one proves it's *worth running*: the upstream sources are still the right ones, the data is trustworthy enough for its stated `confidence`, the cost (time, bandwidth, and model spend) is defensible against the alternatives, and a fresh consumer reaches real value in their first session.

A scraper can be 🟢 on everything else and still be scraping a portal that has published an official bulk API for two years, or burning Claude-tier spend on a page a CSS selector would parse for free. This dimension is what blocks that ship.

## The three artifacts (all live in `.claude/ship-loop/value-case.md`)

### 1. Source alternatives map (web-researched, cited)
For every row in `catalog/data-sources.toml`, what else could supply this data TODAY — including "an official API the publisher added", "a bulk download", "a commercial dataset vendor", and "nothing, this scrape is the only way", which is often the real answer and the strongest justification.

| Source (catalog id) | Current mechanism | Best alternative found | Alternative's cost | Why we still scrape (or don't) | Citation (URL, date) |

Rules: one row per catalogued source; every claim cited (URL + access date); include at least one check for an **official API or bulk download** per source (publishers add them quietly and never retire the HTML). Close with **the verdict per source**: `keep-as-is` · `re-tier` (a cheaper engine would do) · `switch` (an official feed exists) · `cut` (data no longer worth acquiring) · `blocked` (source is gone). If a source's honest verdict is `switch` or `cut`, that's a 🔴 finding for the checkpoint, not something to wordsmith around.

### 2. Per-app cost/value table (the quantified case)
Extend dimension 5's value inventory with researched reality. For every app:

| App | Cost shape | Records per run | Cost per run (measured) | What the records are worth (quantified) | Basis (cited) | Value multiple | Verdict |

- **Cost per run** is measured, not guessed: wall-clock from the job row, `cost_usd` from `/jobs/{id}/costs` for claude-tier apps, and request count against the governor for politeness load. Bandwidth and time count too — an app that takes 40 minutes of a shared browser to fetch 12 rows has a real cost.
- **What the records are worth** = conservative value to the consumer, from web benchmarks: what a commercial vendor charges for the equivalent dataset, the prevailing hourly rate for the manual research it replaces, or the size of the decision the data informs. Never use the app's own description as the basis.
- **Multiple** = value ÷ cost. Verdicts: **strong** ≥10× · **plausible** 3–10× · **weak** <3×.
- A *weak* verdict is ship-blocking for unattended operation (it will keep spending forever): the fix is a checkpoint decision — re-tier to a cheaper engine, reduce cadence, narrow the scope, or cut.
- Aggregate check: a typical month of scheduled operation must cost less than the cheapest credible alternative for the same data OR produce something no alternative offers — state which, with numbers. Model spend is the line item to total explicitly.

### 3. Production-reality checklist (the "naive" audit)
Score each item ✓ / ✗ / ⚠ with one line of evidence:

- **Cold start:** a fresh checkout with an empty `data/` reaches one real, useful dataset in the first session, following only `README.md` + `ONBOARDING.md`. If value only appears after weeks of scheduled accumulation, what bridges the gap?
- **Freshness honesty:** does each source's actual observed cadence match the `cadence`/`cron` claimed in the catalog? A source claiming `daily` that has produced no change in six weeks is either dead or mis-keyed — both are findings.
- **Confidence honesty:** is the catalog's 1–5 `confidence` defensible? A number a downstream consumer trusts must be grounded in something (official publisher, stable schema, corroborated elsewhere), not in optimism.
- **Trust ladder:** what does a consumer have to accept to use this — an unauthenticated local API, cookies on disk, a model with permissions disabled? These are documented trades, but a *new* consumer must be able to find that out before depending on it.
- **Legal & politeness fit:** robots.txt honored where it applies, the per-domain governor spacing set to something a publisher would consider polite, terms-of-service posture noted per source. An app that would get the machine's IP banned is a value bug and an operational bug.
- **Schema drift exposure:** when the upstream page changes shape, does the app fail loudly (job `failed`, visible in `/metrics`) or silently return zero records that look like "no news"? Silent-zero is the single most dangerous failure mode in a scraper.
- **Month-2 rationale:** after the novelty, why is this app still scheduled? Which consumer reads its dataset, and how would anyone notice if it stopped?

## How it runs in the loop

- **Boot:** add a web-research lens (WebSearch/WebFetch) that builds the source alternatives map and collects the pricing/vendor benchmarks the cost/value table needs. Give it `catalog/data-sources.toml`; demand cited sources.
- **Main loop merges** research into the cost/value table and reality checklist (the judgment calls are the orchestrator's, the *facts* are the agent's), writes `value-case.md`, and files a backlog item per weak verdict / ✗ row.
- **Checkpoint:** value verdicts are PRODUCT decisions — present weak apps and failed reality checks as select-based questions (re-tier / narrow / cut / accept with lowered confidence). The loop never green-lights its own value judgment.
- **Runtime tie-in:** the cold-start item becomes an automated journey (fresh `data/` → boot → one app → real dataset within N minutes, default 10). This is the one dimension-9 item with machine evidence.
- **Catalog tie-in:** every verdict that changes a source's reality (`switch`, `cut`, `blocked`, a re-tier, a cadence change) must be written back into `catalog/data-sources.toml` in the same milestone — the catalog is the artifact consumers read, so a verdict that lives only in `value-case.md` has not shipped.
- **Ship gate additions:** `value-case.md` exists with citations ≤30 days old; no unaddressed weak verdict or ✗ checklist row; cold-start journey green; the user has explicitly confirmed the source/cost claims at a checkpoint.

## Research discipline
- Every number in the cost/value table carries a URL + access date; prefer official publisher pages and measured local numbers over blogs; when sources disagree, take the bound least favorable to us.
- Vendor marketing claims are labeled as claims, not facts.
- Web research is a snapshot — stamp `value-case.md` with its research date; the ship gate rejects a stale (>30 days) snapshot.
- **Measure before you research.** The cost half of every row comes from this repo's own job rows and `/costs` endpoint. Researching the value of something whose cost you guessed produces a confident wrong multiple.

## Green means
Every app ≥3× value multiple (headline apps ≥10×), the source alternatives map current and honest with no unaddressed `switch`/`cut` verdict, the reality checklist has no unaddressed ✗, the cold-start journey passes, the catalog reflects every verdict, and the user has signed off. Evidence: `value-case.md` + the journey run + the checkpoint decision log.
