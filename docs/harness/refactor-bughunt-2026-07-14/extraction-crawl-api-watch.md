# Extraction, Crawl & API Watch — refactor + bug-hunt findings

> Total: 5 findings (Critical: 1, High: 1, Medium: 2, Low: 1)
> Files scanned: `crates/apps/extractor/src/lib.rs`, `crates/apps/crawl/src/lib.rs`, `crates/apps/plugin/src/lib.rs`, `crates/apps/connector-api-watch/src/lib.rs` (confirmed against `crates/core/src/crawl.rs`, `crates/core/src/datasets.rs`, `crates/core/src/app.rs`)

## 1. Path traversal / arbitrary file read via unsanitized `artifact_path` + `job_id`
- **Severity**: Critical
- **Lens**: bug-hunter
- **Category**: path-traversal
- **File**: `crates/apps/extractor/src/lib.rs:117-142` (`read_source_body`)
- **Scenario**: In `source` mode the extractor reads each source record's stored body by building `root.join(source_app).join(job_id).join(artifact)` where `root = ctx.artifacts_dir.parent().parent()` (the shared `data/artifacts` root), `source_app` is the free-form `source.app` job param, and `job_id`/`artifact` come straight from `record.data["job_id"]` / `record.data["artifact_path"]`. None of the three components is sanitized. Because `Path::join` does not normalize `..` and lets an absolute component replace the whole path, a dataset record whose `artifact_path` is `"../../../../../../etc/passwd"` (or on Windows `"C:\\Windows\\win.ini"`, or an absolute UNC path), or whose `job_id` is `".."`, makes `tokio::fs::read_to_string` resolve outside the artifacts root. The file's contents are then run through the extractor rules, upserted into the output `dataset`, and echoed back in the job result `records`. An attacker who can influence any dataset the job names via `source.{app,dataset}` (crawl-written records, or any API-writable dataset) obtains arbitrary server-file disclosure. Even with benign data it silently reads across app/job boundaries (`source.app` + a record's `job_id` can point at another app's artifacts).
- **Root cause**: The app trusts stored record fields as safe path segments because the crawl *writes* `artifact_path` as a bare `page-NNNN.html` basename — but source mode consumes records from arbitrary user-named datasets, so the "always a basename" invariant does not hold at the read side. Notably the sibling crawl app already sanitizes its `checkpoint` name char-by-char (`crawl/src/lib.rs:198-201`), so the codebase knows the pattern; the extractor omits it.
- **Impact**: security — arbitrary file read (secrets, `.env`, `firebase-admin.json`, cross-tenant artifacts) exfiltrated through job output.
- **Fix sketch**: Reject any `artifact_path`/`job_id` that isn't a single safe path component (no separators, no `..`, not absolute) — e.g. require `Path::new(artifact).components()` to be exactly one `Normal` segment — then canonicalize the joined path and verify it still starts with `root.join(source_app).join(job_id)` before reading.

## 2. Plugin app persists fetch/plugin *error* records into the output dataset
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure
- **File**: `crates/apps/plugin/src/lib.rs:56-82`
- **Scenario**: Each task returns either real plugin JSON or an error object — `{"error":"fetch: …"}` on fetch failure, `{"error":"empty document"}` on an empty body, or `{"error": <plugin err>}` on plugin failure (lines 57-66). The app then builds `items` from **every** `url`→`rec` pair regardless of error and calls `ctx.upsert_many(&dataset, &items)` (lines 72-82). So a transient fetch timeout or an empty page is written into the plugin-output dataset as a stored record keyed by URL. On the next run, when the fetch succeeds, that key flips from `{error:…}` to real data and is reported as `changed`, firing any dataset trigger/watch on a bogus error→data transition; meanwhile consumers reading the dataset see error objects intermixed with genuine extractions. The reported `ran` count (errors excluded, line 71) also disagrees with the number of records actually upserted.
- **Root cause**: Missing the failure-filter the sibling extractor documents and implements ("Failed/empty fetches are attributed in `failed` and skipped — never upserted as all-null records", `extractor/src/lib.rs:187-237`). The plugin app upserts the raw result vector without partitioning successes from errors.
- **Impact**: wrong result / data corruption — the output dataset is polluted with non-data error records and emits spurious change events.
- **Fix sketch**: Partition results into successes vs. errors (`rec.get("error").is_none()`); upsert only successes, and surface the failures in a `failed`/`errors` array in the result JSON, mirroring the extractor.

## 3. Revisit treats a single 404/410 as permanently `gone` and never re-probes it
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure (change-detection false-negative)
- **File**: `crates/apps/crawl/src/lib.rs:107-134` (`DatasetPageSource::seeds`)
- **Scenario**: In `mode:"revisit"` the frontier is seeded only from `DatasetPageSource`, which filters out any record with `gone == true` (lines 113-116) so dead URLs aren't re-probed. But core flags a page `gone` on the *first* `404` **or** `410` seen during a revisit (`core/src/crawl.rs:830-832`), and the sink writes `{gone:true}` (`crawl/src/lib.rs:54-58`). A `404` is frequently transient (deploy blip, maintenance window, rate-limited path). Once a monthly revisit catches one such 404, the URL is marked gone and is permanently excluded from every subsequent revisit's seed set — so when the page returns 200 again it is never noticed. For a change-watch/sentinel product this is a silent, permanent monitoring gap that only a full fresh (non-revisit) crawl with link-discovery can heal.
- **Root cause**: `gone` is treated as terminal at the app's seed layer, but it conflates the permanent `410 Gone` with the routinely-transient `404 Not Found`; there is no re-probe/grace policy.
- **Impact**: wrong result — a recovered URL silently drops out of ongoing change detection.
- **Fix sketch**: Either still reseed `gone` records for a bounded number of confirmation revisits before retiring them, or only treat `410` as terminal and keep re-probing `404` (optionally with an N-consecutive-404 threshold before marking gone).

## 4. Duplicated "JSON-array-of-strings param" and strategy-parse idioms across all four apps
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `crates/apps/extractor/src/lib.rs:195-200,286-289`; `crates/apps/crawl/src/lib.rs:153-159`; `crates/apps/plugin/src/lib.rs:26-31`; `crates/apps/connector-api-watch/src/lib.rs:95-102`
- **Scenario**: The exact `params.get(key).and_then(Value::as_array).map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect()).unwrap_or_default()` idiom is re-implemented ~5 times (extractor twice, crawl's `str_array`, plugin's `urls`, connector's `only`). Separately, the `match params.get("strategy")` block is duplicated in extractor (`run_urls_mode`, lines 206-211) and plugin (lines 35-39) — and the plugin copy silently omits the `"auto_with_research"` arm the extractor has, so that value falls through to `Http` instead of erroring or matching.
- **Root cause**: No shared param accessor on `AppContext`; each app hand-rolls the same parsing.
- **Impact**: wasted maintenance + latent drift (the strategy divergence is already a live inconsistency).
- **Fix sketch**: Add `AppContext::string_array_param(&self, key) -> Vec<String>` and a shared `FetchStrategy::from_params`/`from_str` helper in core; replace the five call sites so strategy handling stays consistent.

## 5. Change-watch record embeds volatile `engine`/`label` fields, producing phantom "changed" revisions
- **Severity**: Low
- **Lens**: bug-hunter
- **Category**: silent-failure (change-detection false-positive)
- **File**: `crates/apps/connector-api-watch/src/lib.rs:159-169`
- **Scenario**: The stored `connector_docs` record is `{docs_url, label, hash, markdown, engine}`, and `upsert` computes change by hashing the whole value (`core/src/datasets.rs:139,152,164`). So when the fetch engine flaps between runs (`outcome.engine` differs though the markdown is byte-identical), or when someone edits a connector's cosmetic `label` in the manifest, the record hash flips and the dataset records a `changed` revision — firing any watch/trigger on `connector_docs` and cluttering the per-key revision history. The app's own `changes.json` output is shielded by the later `line_diff` empty-check (lines 176-179), so the noise is confined to dataset revisions rather than surfaced changes, which is why this is Low rather than High.
- **Root cause**: The change-detected value mixes content (`markdown`) with provenance/presentation (`engine`, `label`) and a redundant `hash` field, so change detection keys on more than the content that actually matters.
- **Impact**: wrong result (minor) — spurious `changed` revisions / trigger fires on `connector_docs`.
- **Fix sketch**: Store only the content in the change-detected value (e.g. `{markdown}`, or key the record purely on the `hash`) and carry `engine`/`label`/`docs_url` as non-diffed sidecar metadata, so only a genuine markdown change registers as changed.
