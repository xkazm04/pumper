# US Grant Opportunities — refactor + bug-hunt findings

> Total: 5 findings (Critical: 1, High: 2, Medium: 2, Low: 0)
> Files scanned: `crates/apps/grants-gov/src/lib.rs`, `crates/apps/ca-grants/src/lib.rs`, `crates/apps/grants-common/src/lib.rs` (shared normalizer/dedup, read to confirm; findings anchored to the two source apps)

## 1. `parse_date` panics on a non-ASCII date value (byte-slice not on a char boundary)
- **Severity**: Critical
- **Lens**: bug-hunter
- **Category**: parse-panic
- **File**: `crates/apps/grants-gov/src/lib.rs:236-237` (direct call `grants_common::parse_date(close)`); also reached via `norm_date` from both normalizers. Root cause: `crates/apps/grants-common/src/lib.rs:346`.
- **Scenario**: The digest calls `grants_common::parse_date(close)` on the raw `closeDate` string straight from the API hit, and both apps run every record's `open_date`/`close_date` through `norm_date`→`parse_date`. When the two strict-format attempts fail, the third branch executes `&s[..s.len().min(10)]` — a **byte** slice. If a multibyte UTF-8 character straddles byte index 10 (or, when the string is shorter, its own final boundary is interior), the slice is not on a char boundary and Rust panics. Concrete repro: a CA `ApplicationDeadline` / grants.gov `closeDate` free-text value like `"Deadline—see website"` (`Deadline` = 8 bytes, em-dash `—` = bytes 8,9,10) → `&s[..10]` cuts inside the em-dash → `byte index 10 is not a char boundary` panic. Rolling/free-text deadline columns ("Rolling — see site", "Ongoing – contact…") make this realistic upstream.
- **Root cause**: The third fallback truncates a datetime to its 10-char date prefix using byte indexing with no `is_char_boundary`/`char_indices` guard, on the assumption date fields are always ASCII.
- **Impact**: A single record with a non-ASCII character in the first ~10 bytes of a date field panics `parse_date`, unwinding and hard-failing the entire scrape run — one malformed cell takes down the whole daily sync.
- **Fix sketch**: Slice on characters, not bytes: `s.chars().take(10).collect::<String>()` (or guard with `s.floor_char_boundary(10)` / `s.get(..10)` and bail to `None`). Add a non-ASCII date to the `parse_date` test.

## 2. `money_range` scans *all* numbers in `EstAmounts`, so incidental figures corrupt award floor/ceiling
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: money-parse
- **File**: `crates/apps/ca-grants/src/lib.rs:150-153` (normalize path) → `crates/apps/grants-common/src/lib.rs:59, 313-331, 248-286`
- **Scenario**: `normalize_ca_grants` derives `award_floor`/`award_ceiling` from the free-text `EstAmounts` column via `money_range`→`scan_amounts`, which greedily collects **every** dollar-ish number in the string and takes min/max. Real CA-portal phrasings break this: `"Up to $500,000"` → `[500000]` → floor **and** ceiling both `$500,000` (floor should be null/0); `"$100,000 per applicant, up to 5 awards"` → `[100000, 5]` → **floor `$5`**, ceiling `$100,000`; `"$1,000,000 over 3 years"` → floor `$3`. Any stray count, year fragment, or award-count in the range cell becomes the reported minimum award.
- **Root cause**: `scan_amounts` is a context-free number sweep with no notion of which numbers are amounts vs. counts/durations; `money_range` blindly takes min/max of whatever it finds. It only handles the clean documented "Between $X and $Y" shape.
- **Impact**: Wrong award-range data in the unified corpus (e.g. a $3 minimum award), surfaced to downstream search/exports/digests — misleading on the highest-stakes field a grant seeker filters on.
- **Fix sketch**: Only treat `$`-prefixed tokens as money in `scan_amounts` (drop bare integers like "5 awards"); recognize "up to"/"maximum"/"minimum" cues to set only ceiling or only floor; when a single amount is found with "up to", set floor `Null`, not `= ceiling`.

## 3. Near-total duplication of the two `run()` bodies + HTTP builders — the cross-source finalize belongs in `grants-common`
- **Severity**: High
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/apps/grants-gov/src/lib.rs:82-219, 260-278` vs `crates/apps/ca-grants/src/lib.rs:67-177, 198-216`
- **Scenario**: The two apps are structurally the same file with different field names: (a) the paginate-until-short-page loop (fetch → `is_success` guard → JSON parse → API-level error check → extract array with `unwrap_or_default` → extend → `pages+=1` → `offset/start += page_size` → identical break condition); (b) the "positive total but zero rows" drift guard (lines 148-153 ≡ 131-136); (c) the entire cross-source block `normalize_* → sync_unified → sweep_closed → link_duplicates → drift_warnings` (176-185 ≡ 150-159), copy-pasted verbatim; (d) the result JSON shape (source/total/fetched/pages/new/changed/unchanged/unified/swept/warnings/crossSourceDups/index_datasets); (e) `search2_request` (260-278) and `post_json` (198-216) are the same `HttpRequest` literal — `post_json` even already takes a `url` parameter.
- **Root cause**: Each new source re-implements the shared pagination/finalize/response skeleton inline instead of the shared crate owning it; only the truly source-specific bits (URL, request-body builder, success/total field names, `normalize_*`) differ.
- **Impact**: Every fix or contract change (e.g. finding #1's date guard, a new warning, a result-field rename) must be made twice and can silently drift between sources; the third source will copy-paste a third time.
- **Fix sketch**: Move `post_json`/the request literal and a `finalize_unified(ctx, unified_items) -> {unified, swept, dups, warnings}` helper into `grants-common`; optionally a generic `paginate(ctx, request_fn, total_path, records_path)` so each app only supplies its body builder, field paths, and `normalize_*`.

## 4. grants.gov "daily full sync" silently truncates at `maxPages` with no warning when `hitCount` exceeds the cap
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/grants-gov/src/lib.rs:75-80, 138-142, 200-219`
- **Scenario**: Default params are `rows: 100, maxPages: 25` → hard ceiling of 2,500 opportunities per run. grants.gov posted+forecasted open calls routinely approach/exceed that. When `hit_count > rows*max_pages`, the loop breaks on `pages >= max_pages` (line 139) with opportunities beyond page 25 never fetched. Because the raw `opportunities` dataset is `upsert_many` (never removes), those grants are simply never refreshed. The result JSON reports `hitCount` and `fetched` but emits **no warning** and the run is reported successful — an operator sees `fetched: 2500` next to `hitCount: 2900` only if they eyeball both.
- **Root cause**: The page cap is a silent stop condition with no surfaced signal that the snapshot is incomplete; only the *empty-array* drift case (148-153) is treated as noteworthy.
- **Impact**: Some genuinely-open federal grants silently missing from the corpus/digest on high-volume days, with a green run status hiding it.
- **Fix sketch**: When the loop exits via `pages >= max_pages` while `start < hit_count`, push a `warnings` entry (e.g. "truncated: fetched N of hitCount, raise maxPages") and/or raise the default `maxPages` to cover the realistic corpus size.

## 5. Non-idempotent positional fallback keys diverge the raw dataset from unified
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case
- **File**: `crates/apps/grants-gov/src/lib.rs:158-170`, `crates/apps/ca-grants/src/lib.rs:183-196`
- **Scenario**: When a record lacks its stable id, the raw `opportunities` key falls back to a positional value — grants-gov `format!("row-{i}")` (line 167), ca-grants `_id-{n}` / `row-{i}` (191-195). The `_id`/row index renumbers between runs (the ca-grants doc-comment itself warns `_id` renumbers on reload), so the same underlying grant gets a *different* key each run: it re-appears as `new`, and the previous run's row is never matched again (orphan accumulation under `upsert_many`). Meanwhile the unified normalizers require a real id (`str_of(hit, &["id","number"])?` / `&["PortalID","GrantID"]?`) and **skip** such records entirely — so an id-less record exists in raw `opportunities` (churning) but is absent from `unified`.
- **Root cause**: Two different missing-id policies — raw fabricates a positional key; unified drops the row — with no shared contract on what a keyless record means.
- **Impact**: Spurious `new` counts and orphaned rows in the raw dataset on any id-less/schema-drifted record, plus raw↔unified divergence that makes the datasets disagree on which grants exist.
- **Fix sketch**: Make raw and unified agree — either skip id-less records in the raw dataset too (mirroring the normalizer), or derive a *content-stable* fallback key (hash of title+agency+closeDate) shared via a `grants-common` helper so both layers key identically.
