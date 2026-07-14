# Full-Text Search Index — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 4, Low: 0)
> Files scanned: `crates/engine-search/src/lib.rs` (full); confirming reads of `crates/core/src/search.rs`, `crates/server/src/routes.rs` (search handler + ApiError mapping), `crates/server/src/worker.rs` (saved-search runner)

## 1. Malformed user query returns HTTP 500 and silently kills saved-search alerts
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / edge-case
- **File**: `crates/engine-search/src/lib.rs:217-219` (with `crates/server/src/routes.rs:154-161`, `crates/server/src/worker.rs:412-423`)
- **Scenario**: `parse_query(&req.q)` is the raw Tantivy `QueryParser`. Ordinary things a user types into a search box make it return `Err`: an unbalanced quote (`q="foo`), a lone operator or unclosed group (`q=(cost`), a colon that looks like a field selector on a non-existent field (`q=note: hello`, `q=price:5` → `FieldDoesNotExist`), or a bare range/boost char (`q=a^`, `q=[a`). The error is wrapped as `Error::App("bad search query: ...")` and propagated. At the HTTP boundary, `impl From<pumper_core::Error> for ApiError` (routes.rs:160) maps *every* `Error` to `StatusCode::INTERNAL_SERVER_ERROR`, so the user gets a **500 Internal Server Error** for their own punctuation, not a 400/422. Worse: the saved-search runner (worker.rs:419) stores arbitrary query strings and runs them after every job; a saved search whose stored query is syntactically invalid hits the same `Err`, is caught fail-open with only a `warn!` (worker.rs:421-422), and therefore **never matches and never alerts** — a permanently broken monitor with no user-visible signal.
- **Root cause**: Parse failure of *user input* is treated as an internal/server error. `Error::App` carries no "client input" classification, so the blanket `From` mapping cannot down-grade it to 400; and there is no graceful-degradation fallback (e.g., escaping operator chars and retrying as a plain term query).
- **Impact**: 500s on routine search input (looks like a server bug to clients/monitoring); saved-search alerts with any malformed query silently go dark.
- **Fix sketch**: In `query`, on `parse_query` `Err` either (a) fall back to a sanitized term query — strip/escape Tantivy operator characters and re-parse — so search degrades to plain-text matching, or (b) return a distinct client-input error variant that the server maps to 400/422. Whichever is chosen, surface saved-search parse failures to the owner instead of only logging.

## 2. Every query fetches up to 1,000 full stored documents (including the large body) just to count two facet fields
- **Severity**: Medium
- **Lens**: bug-hunter / code-refactor
- **Category**: resource-leak (wasted work / memory)
- **File**: `crates/engine-search/src/lib.rs:247-291`
- **Scenario**: `sample_size = req.limit.max(FACET_SAMPLE)` is always ≥ 1000. The loop at 259-291 calls `searcher.doc(*address)` for **every** sampled address (up to 1000), and for each one runs `doc.to_json(&schema)` and `serde_json::from_str(...)` — a full serialize-then-parse round trip of the entire stored document, whose `body` field is STORED and can be an entire scraped web page. For docs beyond `req.limit` (i.e. 900+ of them on a limit=100 query, 990+ on limit=10) only `app` and `dataset` are actually used, to increment the facet `BTreeMap`s. The full-doc load + JSON round trip for those hundreds/thousands of docs is pure overhead paid on *every* search request.
- **Root cause**: Facet counting piggybacks on full stored-document retrieval instead of reading the two facet fields from a columnar/fast field. `app`/`dataset` are declared `STRING | STORED` (not FAST), so there is no cheap columnar path, forcing a stored-doc fetch per sample row.
- **Impact**: High per-query CPU and allocation, growing with body size and index size; the hottest path in the search engine does ~1000 full-document deserializations for a page of ≤100 hits.
- **Fix sketch**: Declare `app` and `dataset` as FAST fields and count facets from the columnar reader (or a `FacetCollector`), fetching full stored docs (via `searcher.doc`) only for the ≤`req.limit` rows that actually become hits. Also note the reported `count` (page size ≤100) is inconsistent with facet counts summed over the ≤1000 sample — expose a `total`/sample size so the UI isn't misled.

## 3. A single unreadable document anywhere in the 1,000-doc sample fails the entire query
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / reliability
- **File**: `crates/engine-search/src/lib.rs:259-262`
- **Scenario**: Inside the facet loop, `searcher.doc(*address).map_err(|e| Error::App("fetch doc: ..."))?` aborts the whole `query` on the first doc that fails to load. Because the loop iterates the *entire* `FACET_SAMPLE` (up to 1000 score-ranked matches, far beyond the `req.limit` the caller wants), a single corrupt, partially-merged, or otherwise unreadable stored doc anywhere in the top-1000 ranked matches makes **every** query that ranks that doc in-sample return a 500 — even queries that would only display 10 hits and never surface the bad one.
- **Root cause**: Over-reading (sampling 1000 docs for facets) combined with a *fatal* per-doc read error. The blast radius of one bad document is amplified from "one missing hit" to "search down for a whole class of queries."
- **Impact**: Localized index corruption escalates into a broad, hard search outage; hard to diagnose because it depends on which queries rank the bad doc within the sample window.
- **Fix sketch**: Treat a per-doc load failure as non-fatal in the facet loop — `warn!` and `continue` — so a single bad doc drops from results/facets instead of failing the request. Only the ≤`req.limit` docs that become returned hits warrant harder handling. (Fixing finding #2 shrinks the exposure to just the returned page.)

## 4. Writer `Mutex` poisoning permanently disables all indexing and deletes
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: race-condition / reliability
- **File**: `crates/engine-search/src/lib.rs:124, 155, 191`
- **Scenario**: All three mutating paths acquire the shared writer with `writer.lock().unwrap()`. If any panic occurs while that lock is held inside the `spawn_blocking` closure — a Tantivy-internal panic in `add_document`/`commit`, an allocation abort, or a panic while the guard is alive during `commit`/`reader.reload` — the `Mutex` becomes **poisoned**. From then on, every `index`, `delete_ids`, and `delete_dataset` call panics at `.lock().unwrap()`; the panic is caught by `spawn_blocking` and surfaces forever as `"index task panicked"` / `"delete task panicked"`. The read/`query` path never locks the writer, so search keeps working and *masks* the fact that all writes are permanently dead until the process restarts.
- **Root cause**: `.unwrap()` on a poisoned lock rather than recovering the guard. A Tantivy `IndexWriter` is generally still usable after a panic that did not corrupt it, but Rust's poison flag blocks all further access.
- **Impact**: One transient panic silently and permanently stops all indexing and deletion (no new docs, no cleanup, no dataset retirement), while the healthy-looking read path hides the outage.
- **Fix sketch**: Recover from poison — `writer.lock().unwrap_or_else(|e| e.into_inner())` — and log once on recovery, or wrap writer access so a poisoned lock is re-derived. Combine with the helper in finding #5 so this is handled in one place.

## 5. Duplicated writer/commit/reload epilogue across the three mutating methods
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/engine-search/src/lib.rs:120-144` (`index`), `150-165` (`delete_ids`), `168-200` (`delete_dataset`)
- **Scenario**: All three write methods repeat the same skeleton: clone `writer`/`reader`/`fields`, `spawn_blocking`, `writer.lock().unwrap()`, do the mutation, then the identical epilogue `w.commit().map_err(...commit...)?; reader.reload().map_err(...reader reload...)?; Ok(())` followed by the identical `.await.map_err(...task panicked...)?`. The error-message strings and the commit+reload+await tail are copied three times. The copies have already drifted: `index` (117-119) and `delete_ids` (147-149) guard against empty input, but `delete_dataset` (167) has no such guard and will lock/commit/reload for an empty or no-op request.
- **Root cause**: No shared "run this mutation under the writer, then commit+reload" helper; each method re-implements the boilerplate.
- **Impact**: Maintenance drag and drift risk — the poison-handling fix (finding #4), any change to commit/reload ordering, or a new input guard must be applied in three places and is easy to miss (as the empty-guard omission shows).
- **Fix sketch**: Extract a private `fn write_blocking(&self, f: impl FnOnce(&mut IndexWriter) -> Result<()> + Send) -> Result<()>` that clones the handles, spawns the blocking task, locks the writer (with poison recovery), runs `f`, then commits and reloads once; have all three methods call it and keep only their per-method body inside `f`.
