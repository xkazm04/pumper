# Moonshot Scan — Economic & Labor Market Data Apps (2026-07-30)

> Total: 6 moonshots across 3 contexts.

## Context: US Trades Wages, Tax & Valuation

### 1. Taxonomy-as-data: self-expanding trade coverage from 5 trades to all of home services
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/trades-common/src/lib.rs (taxonomy + unified), crates/apps/trade-wages/src/lib.rs, crates/apps/homewyse-pricing/src/lib.rs, crates/apps/valuation-multiples/src/lib.rs
- **What it is**: Today the entire trades reference layer is hard-wired to a compile-time enum of 5 trades (`Trade::ALL`), and every unknown label the agent returns (e.g. "Roofing") is stored raw and merely flagged. Turn the taxonomy into a governed *data* registry — each trade a record with canonical label, SOC code, NAICS codes, keyword aliases, and an enabled flag — and drive prompt construction, canonicalization, `sync_operator_economics`, and the census NAICS lists from it. An agentic "taxonomy proposer" run maps a candidate trade (roofing, painting, pest control, garage doors, cleaning…) to its SOC/NAICS/pricing jobs, and once approved, all four research apps and both census apps automatically cover it on their next run.
- **Why it's a moonshot**: It converts a 5-trade Ledgerline demo into the full US home-services reference dataset (~25+ trades × 51 states of wages, pricing, tax, valuation, density) with zero new per-trade code. Coverage — the single biggest limit on every downstream product idea — becomes a config decision, a 10x expansion of the addressable market for the whole trades domain.
- **Differentiation**: No prior idea touches taxonomy expansion or trade coverage; the nearest (#118 operator digital-twin, #238 B2B reference API) both assume the fixed 5-trade universe and get 5x more valuable when the universe grows.
- **Path**: (1) Lift `taxonomy::Trade` into a `trades/taxonomy` dataset (or catalog TOML) seeded with the current 5, keeping the enum as fallback; (2) make `canonicalize`, `prompt_list`, and `soc_code` read the registry via `AppContext`; (3) loop `sync_operator_economics` over registry entries instead of `Trade::ALL`; (4) add an agentic `taxonomy-proposer` app (state-tax pattern: one structured call + validation) that drafts SOC/NAICS/alias mappings for a requested trade into a pending state; (5) feed registry NAICS codes into census-density/census-nonemp `params.naics`; (6) batch the research prompts (chunk trades per call) so 25 trades don't blow the turn budget.
- **Risks**:
  - Unreviewed agent-proposed mappings could poison keys across four datasets — needs an approve gate before a trade goes live.
  - Research cost scales with trade count on metered apps; must chunk prompts and lean on the existing freshness gates.

### 2. Cost-to-operate compliance layer: per-state licensing, bonding & insurance joined into operator economics
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/apps/state-tax/src/lib.rs (the template), crates/apps/trades-common/src/lib.rs (unified join)
- **What it is**: A new agentic app, `state-licensing`, cloned from state-tax's proven one-call/51-jurisdiction pattern: for each state × trade, the contractor license requirement (none / registration / exam+license), typical license + bond cost, insurance minimums, and workers'-comp base signal — validated with the existing `validate` guards and joined into the per-state `trades/operator_economics` rows as a `compliance` block beside `tax`.
- **Why it's a moonshot**: Operator economics currently answers "what will I earn and pay in tax" but not "what does it cost to legally exist" — the first question every new trades operator actually has. Adding it makes `operator_economics` the only dataset anywhere that gives a solo operator the complete P&L of starting in any state, and it is the missing input for every launch-ranking and onboarding product downstream.
- **Differentiation**: No prior idea touches licensing, bonding, insurance, or any regulatory-cost data; #25 (tax set-aside autopilot) and #55 (per-state wage bands) stay inside the existing four datasets.
- **Path**: (1) Scaffold `crates/apps/state-licensing` copying state-tax (roster check against `US_JURISDICTIONS`, vintage gate, json_schema); (2) prompt per trade × state batch (licensing is trade-specific, so chunk by trade — 5 calls of 51 rows); (3) add `require_rate`/`require_positive` plausibility guards for fees and bond amounts; (4) extend `state_tax_context`-style join in `unified::sync_operator_economics` with a `compliance` block on `<ST>:<trade>` rows; (5) register in the app registry + catalog.
- **Risks**:
  - Licensing rules are messier than tax brackets (county-level variation in TX/CO); records need an explicit `grain`/caveat field to stay honest.
  - 5 × 51-row research calls is a real metered cost; freshness gate must be vintage-style (rules change rarely).

## Context: Czech Labour Market (MPSV)

### 1. Vacancy survival ledger: time-to-fill and repost analytics from daily snapshot diffing
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/mpsv-vpm/src/lib.rs
- **What it is**: mpsv-vpm downloads all ~300k live postings daily, aggregates, and throws the per-posting layer away. Keep a compact per-posting *lifecycle ledger* (posting id → czIsco, kraj, salary band, first_seen, last_seen, seen_count) persisted as a chunked artifact or a bounded dataset, diffed against each day's feed. A posting that disappears has been filled or withdrawn — which yields, per occupation × kraj: median time-to-fill, survival curves, repost rate (same employer + same role reappearing), and vacancy churn. Publish as a `vacancy_lifecycle` aggregate dataset.
- **Why it's a moonshot**: Time-to-fill is *the* hiring-difficulty metric — recruiters, employers, and the ÚP itself do not have it at this grain for the Czech market, and it cannot be reconstructed later (the feed is replaced daily; every day not captured is lost forever). It turns pumper's daily fetch from "current stock counts" into a longitudinal labour-market flow instrument nobody else can replicate without also having run daily for months — a durable data moat.
- **Differentiation**: Nearest prior ideas #105/#86 (trending vs fading roles) count *stock deltas* of aggregate cells; this is *flow/survival* analysis over individual posting lifetimes (fill-time distributions, repost detection), which no prior idea touches.
- **Path**: (1) During the existing parse pass, emit the compact ledger tuple per posting (~40 bytes each, ~300k rows fits one ~15 MB artifact — no upsert_many round-trip problem); (2) load yesterday's ledger artifact at run start, diff ids to update first/last_seen and mark disappearances; (3) aggregate closed postings into `vacancy_lifecycle` (czIsco × kraj: median/p75 days-open, repost share, churn) with the existing `minCount` privacy floor; (4) detect reposts by (IČO, czIsco, kraj, salary band) match within N days; (5) after ~60 days of history, expose fill-time in the salary_gap/role_region_agg consumer surface.
- **Risks**:
  - Disappearance conflates filled vs withdrawn vs expired — must label the metric honestly (time-to-close) and use repost signal to de-noise.
  - A missed daily run creates gaps; needs a max-gap tolerance in the diff so one outage doesn't mark 300k postings closed.

### 2. Salary nowcast: project the lagged official ISPV distribution forward with daily posted-salary drift
- **Tier**: 2
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/mpsv-ispv/src/lib.rs, crates/apps/mpsv-vpm/src/lib.rs (salary_gap join)
- **What it is**: ISPV is the authoritative salary anchor but publishes quarterly-to-annually with a long lag; posted salaries in role_region_agg refresh daily. Build a nowcasting layer: per CZ-ISCO unit group, learn the stable relationship between the posted-salary distribution and the official ISPV distribution from historical pairs (the shipped `salary_gap` dataset is exactly this training signal), then apply the current posted drift to project today's ISPV-quality median/decile spread into a `cz-labour/salary_nowcast` dataset with an explicit confidence/staleness field.
- **Why it's a moonshot**: It manufactures an "official-grade, zero-lag" Czech salary figure — the thing every negotiation copilot, posting assistant, and benchmarking API prior ideas imagined would consume — from two feeds pumper already holds. The derived number is more current than anything the state publishes, which is a saleable data product in itself, not just a feature.
- **Differentiation**: #70 (cross-source calibration vs ISPV) and #24 (posted-vs-official gap API) *measure the gap backward*; nowcasting *predicts the current official distribution forward* — a prediction product, not a diagnostic, and no prior idea proposes projection.
- **Path**: (1) Start dead simple in-process: per unit group, ratio-adjust the latest ISPV median by the posted-median drift since the ISPV vintage date (data already joined in the salary_gap builder — extend that function); (2) persist `salary_nowcast` keyed `czIsco|sfera` with `basis_vintage`, `posted_drift_pct`, `confidence`; (3) guard with the existing sanity bands + minimum-cell-count so thin occupations emit no nowcast rather than a fabricated one; (4) once several ISPV releases have accumulated, backtest: does posted drift at release T predict the T+1 official figure? Report error per group; (5) suppress groups where backtest error exceeds a threshold.
- **Risks**:
  - Posted salaries are a biased sample of actual pay; the ratio model must be per-group and backtested, and the record must carry its uncertainty honestly.
  - Needs ≥2 ISPV releases under pumper's belt to validate — value compounds with time, thin at launch.

## Context: US Trades Business Density (Census)

### 1. Succession-wave engine: owner-age demographics (NES-D) turn density into an acquisition-timing map
- **Tier**: 1
- **Feasibility**: medium
- **Horizon**: months
- **Files**: crates/apps/census-nonemp/src/lib.rs (the template), crates/apps/census-density/src/lib.rs (blend join)
- **What it is**: Census publishes Nonemployer Statistics by Demographics (NES-D) — nonemployer counts by *owner age band* (also sex/veteran status) per NAICS × state, on the same key-free-with-census-key array-of-arrays API census-nonemp already speaks. A new `census-nesd` app pulls owner-age composition for the trade NAICS codes, and a join computes a **succession index** per state × trade: the share of solo operators aged 55+ (weighted by the shipped avg-receipts-per-operator and the valuation-multiples SDE bands) — i.e. where a retirement wave of trades businesses will hit, how big it is in dollars, and when.
- **Why it's a moonshot**: The "silver tsunami" of retiring trades owners is the defining M&A story of US home services, and nobody offers a state × trade quantification of it. Fusing owner demographics + receipts + valuation multiples produces a dataset PE roll-ups, search funds, and ambitious operators would pay for directly — it upgrades pumper from "market density stats" to acquisition-timing intelligence.
- **Differentiation**: #196 (roll-up target sourcing by fragmentation) ranks markets by establishment fragmentation only; no prior idea touches owner demographics, NES-D, or timing of ownership transition — the succession signal is a different axis entirely.
- **Path**: (1) Scaffold `census-nesd` by copying census-nonemp (same key, same header-by-name parsing, same 4-digit NAICS suppression handling) against `data/{year}/nesd` with the owner-age variable; (2) upsert `owner_age` dataset keyed `state|naics|age_band`; (3) extend the shipped census blend join with `pct_owners_55plus` and `succession_receipts_$` (55+ share × total receipts); (4) join valuation-multiples SDE bands to express the wave in estimated enterprise value; (5) expose via the existing `?filter=` surface and rank states per trade.
- **Risks**:
  - NES-D vintages/variable names shift between releases and cells are disclosure-suppressed — needs the same suppression-tolerant handling census-nonemp already has.
  - Age bands are coarse (e.g. 55–64); the index must be framed as a wave-size indicator, not a per-business prediction.

### 2. Formation-velocity radar: weekly Business Formation Statistics as the leading edge of competition
- **Tier**: 2
- **Feasibility**: high
- **Horizon**: weeks
- **Files**: crates/apps/census-density/src/lib.rs, crates/apps/census-nonemp/src/lib.rs
- **What it is**: CBP and NES describe the market as it was ~2 years ago. Census Business Formation Statistics (BFS) publishes *weekly and monthly new business applications* by NAICS sector and state through the same api.census.gov JSON interface (`/data/timeseries/bfs`). A new `census-bfs` app ingests application counts and high-propensity applications for the construction/services sectors, computes trailing 12-month formation velocity and acceleration per state, and joins it onto the density blend — "how fast is new competition entering this market *right now*", refreshed weekly by the existing scheduler.
- **Why it's a moonshot**: It collapses the 2-year blind spot in every launch-ranking and saturation read from years to days — the density stack becomes a live radar instead of a census archive. Weekly cadence also finally gives this context a stream worth wiring into the shipped reactive trigger DAGs (alert when formation in your state/trade accelerates 2σ), which nothing in the census stack currently exercises.
- **Differentiation**: #195 (housing-permit demand forecast) is a *demand*-side signal; #85/#214 (YoY trend from annual re-runs) still move at annual-vintage speed. No prior idea touches BFS, business-formation data, or any weekly-cadence source for this context — this is the *supply*-side, real-time counterpart.
- **Path**: (1) Scaffold `census-bfs` from census-nonemp's fetch/parse core against the BFS timeseries endpoint (same key, header-by-name contract); (2) upsert `formations` keyed `state|sector|period`; (3) derive `formation_velocity` (T12M sum, YoY delta, acceleration) per state × sector; (4) add a `formation_velocity` block to the blended market view so saturation is read against inbound competition; (5) give it a weekly `schedule()` and register a trigger-DAG example that fires on acceleration outliers.
- **Risks**:
  - BFS granularity is NAICS *sector* (23 Construction / 56 Admin-Support), not trade-level — honest labeling as a sector-grain signal; trade-level inference must stay out of the record.
  - Timeseries endpoint parameter conventions differ slightly from CBP/NES; verify the contract on first fetch and pin it in the doc header like the other apps do.
