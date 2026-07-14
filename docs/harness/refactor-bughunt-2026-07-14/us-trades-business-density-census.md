# US Trades Business Density (Census) — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 2, Medium: 3, Low: 0)
> Files scanned: `crates/apps/census-density/src/lib.rs`, `crates/apps/census-nonemp/src/lib.rs`

## 1. Negative Census annotation/jam sentinels are summed raw into CBP/NES totals (guarded only in the denominator)
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: sentinel-parse
- **File**: `crates/apps/census-density/src/lib.rs:241-252` and `crates/apps/census-nonemp/src/lib.rs:195-197`
- **Scenario**: A CBP `EMP`/`PAYANN` (or NES `NESTAB`/`NRCPTOT`) cell comes back as a Census annotation/jam value (`"-555555555"`, `"-666666666"`, `"-999999999"` — the codes Census uses for withheld / not-available). `row.get(i).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0)` parses it as a valid *negative* i64 and it is summed into `total_emp` / `total_estab` / `total_rcpt` and stored on the per-place record (`employees`, `annual_payroll_thousands`, `nonemployers`, `receipts_thousands`).
- **Root cause**: The author clearly knows Census emits negative sentinels — `fetch_denominator`'s `num` closure filters `.filter(|v| *v >= 0)` (census-density:670) and the `Denom` doc says "Jam values (negatives) → 0" (census-density:614) — but that guard was never applied to the two *primary* metric parsers. Defensive logic added in one place, forgotten in the parallel places.
- **Impact**: wrong value / fabricated data — a single withheld state can drive a trade's national `total_employees`/`total_receipts_thousands` sharply negative, and the corrupt per-place figure flows into the ranking, the upserted `establishments`/`nonemployers` records, and the downstream `census/market_blend` (`total_market`, `solo_share`).
- **Fix sketch**: Reuse the same `>= 0` filter in both row loops (e.g. `.parse::<i64>().ok().filter(|v| *v >= 0).unwrap_or(0)`), ideally via one shared numeric-cell helper so all three parsers stay consistent.

## 2. Disclosure-suppressed cells silently become 0 and are counted as genuinely-reported places
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: sentinel-parse / silent-failure
- **File**: `crates/apps/census-nonemp/src/lib.rs:194-219` (also `crates/apps/census-density/src/lib.rs:239-293`)
- **Scenario**: NES is disclosure-suppressed for *individual* `state × 4-digit NAICS` cells even when the whole-response `204` guard (line 142) does not fire. A suppressed `NESTAB` or `NRCPTOT` that is non-numeric (or the negative annotation from #1) fails `parse::<i64>()` and becomes `0`. That state is still pushed into `ranked`, counted in `states_reported = ranked.len()` (line 230), summed into `total_estab`/`total_rcpt`, and written to `all_records` as `{ nonemployers: 0, receipts_thousands: 0, avg_receipts_per_operator: 0 }`. If only `NRCPTOT` is suppressed, `avg = (0 * 1000) / estab = 0`, publishing "$0 average receipts per operator" for a real, active market.
- **Root cause**: `unwrap_or(0)` conflates "suppressed / unknown" with "measured zero." There is no distinction between an absent cell and a true zero, and no per-cell suppression marker on the record.
- **Impact**: fabricated data — a suppressed state is presented as an empty market (0 operators, $0 receipts), it dilutes national totals and `national_avg_receipts_per_operator`, and it feeds the market blend as `solo_operators: 0` / `solo_share: 0`, misleading Ledgerline's geographic launch ranking into treating suppressed regions as dead.
- **Fix sketch**: Parse to `Option<i64>`; on `None` (suppressed) either skip the row or emit the record with an explicit `suppressed: true` and `null` metrics, and exclude it from `states_reported`, totals, and the blend rather than counting it as a measured 0.

## 3. census-density hard-fails the entire run on a 204 / empty single-trade response; census-nonemp degrades gracefully
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / inconsistent-error-handling
- **File**: `crates/apps/census-density/src/lib.rs:185-205` vs `crates/apps/census-nonemp/src/lib.rs:142-148`
- **Scenario**: In county mode a sparse trade × state query can return `204 No Content` or an empty body. census-density has no 204/empty guard: `resp.is_success()` is true for 204, then `resp.body.trim_start().starts_with('[')` is false, so it returns `Err("response was not JSON")` — aborting the whole run and discarding every trade/state already fetched (and skipping the upsert + blend entirely). census-nonemp handles the identical case at lines 142-148 by recording a note and `continue`-ing.
- **Root cause**: The graceful-degradation pattern used elsewhere in census-density (normalization skip at 367, market_blend skip at 379) was not applied to the per-trade fetch loop; the two apps diverged on empty-response handling.
- **Impact**: crash-of-run — one empty/suppressed trade fails an otherwise-good multi-trade CBP scrape, and no records are persisted for the trades that *did* return data.
- **Fix sketch**: Mirror census-nonemp: if `resp.status == 204 || resp.body.trim().is_empty()`, push a `note` summary for that NAICS and `continue` instead of erroring.

## 4. Empty or wrongly-typed `naics` param produces a "successful" empty run
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / empty-as-success
- **File**: `crates/apps/census-density/src/lib.rs:129-147` and `crates/apps/census-nonemp/src/lib.rs:78-96`
- **Scenario**: A caller/scheduler passes `params.naics` as JSON numbers (`[238220, 238210]`) or an empty array. `arr.iter().filter_map(Value::as_str)` drops every non-string entry, so `trades` becomes empty; an empty array yields the same. The `for (naics, label) in &trades` loop then runs zero times, `all_records` stays empty, `upsert_many` is a no-op (partial semantics), and `run` returns `Ok` with `records: 0, new: 0` — a green run that scraped nothing.
- **Root cause**: `filter_map(as_str)` silently discards type-mismatched codes, and there is no "trades must be non-empty" validation before the fetch loop (unlike the explicit guards for missing api_key and county-without-states).
- **Impact**: silent no-op — a param typo (numbers vs strings, or `[]`) masquerades as a successful scrape, so the scheduled annual refresh appears healthy while writing no data and leaving stale records in place.
- **Fix sketch**: After building `trades`, `if trades.is_empty() { return Err(Error::App("no valid NAICS codes — pass 4/6-digit codes as JSON strings")) }`, and optionally warn on entries dropped by `as_str`.

## 5. Large verbatim + structural duplication across the two census apps
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/apps/census-density/src/lib.rs:780-795, 150-167, 129-147, 184-208` vs `crates/apps/census-nonemp/src/lib.rs:269-284, 98-115, 78-96, 139-170`
- **Scenario**: The two apps carry four near-identical blocks: (a) the 52-entry `state_abbr` FIPS→USPS match table is copied byte-for-byte; (b) the api_key resolution (param → `CENSUS_API_KEY` env → non-empty filter → `Error::App` with the same signup message); (c) the trades-param parsing (`naics` array with per-code label lookup, else `DEFAULT_TRADES`); (d) the per-trade fetch scaffold (`is_success` check, `starts_with('[')` + "invalid/missing API key" hint, `from_str::<Vec<Vec<String>>>`, `save_artifact`, `header.iter().position` idx closure, `rows.iter().skip(1)` loop).
- **Root cause**: The two apps grew in parallel with no shared `census-common` helper crate; each copied the other's boilerplate.
- **Impact**: wasted maintenance and drift risk — findings #1–#4 above each exist in one app but not the other precisely because the duplicated code diverged. A single fix must be made in two places.
- **Fix sketch**: Extract a `census-common` (or `pumper_core::census`) module holding `state_abbr`, `resolve_api_key`, `parse_trades`, and a `fetch_census_rows` helper (fetch + validate + `Vec<Vec<String>>` parse + name→index map + shared numeric-cell parser from #1/#2), and have both apps call it.
