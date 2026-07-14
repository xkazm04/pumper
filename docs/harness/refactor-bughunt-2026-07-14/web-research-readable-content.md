# Web Research & Readable Content — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 2, Medium: 3, Low: 0)
> Files scanned: `crates/apps/research/src/lib.rs`, `crates/apps/readable/src/lib.rs`, `crates/apps/hackernews/src/lib.rs` (confirmed against `crates/core/src/engine.rs`, `fetcher.rs`, `app.rs`, `config.rs`, `extract.rs`)

## 1. Research accepts any LLM JSON shape as `structured: true` — the `json_schema` guardrail is never used
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: parse / silent-failure (hallucinated structure)
- **File**: `crates/apps/research/src/lib.rs:42-69`
- **Scenario**: The prompt asks the model (in prose) to "Respond with ONLY a JSON object" of shape `{summary, key_findings[], sources[{url,title}]}`. Nothing validates the result. If the agent returns valid JSON of a *different* shape — an array, a bare string that happens to parse as JSON, `{"answer": "..."}`, or an object missing `key_findings`/`sources` — then `output.json` is `Some(...)`, the app stores it verbatim as `report`, and stamps `structured: true` (line 64). A downstream consumer that reads `report.summary` / `report.key_findings` / `report.sources` silently gets missing/garbage fields with no error and a `true` "this is structured" signal.
- **Root cause**: The contract is enforced only by natural-language instruction. `ResearchRequest` exposes a real guardrail — `json_schema: Option<Value>` mapped to the CLI's `--json-schema` (engine.rs:262-264) — but the app leaves it `None`, so the engine never constrains or rejects a wrong-shaped answer.
- **Impact**: Wrong result presented as success; malformed reports flow downstream labeled `structured: true`.
- **Fix sketch**: Set `request.json_schema = Some(json!({...summary/key_findings/sources schema...}))` so the engine constrains the answer; additionally validate the returned `output.json` has the three expected keys and downgrade `structured` (or return an `Error::App`) when it doesn't.

## 2. Readable returns empty extraction as a successful result (empty-as-success)
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: silent-failure / empty-as-success
- **File**: `crates/apps/readable/src/lib.rs:46-62`
- **Scenario**: `markdown = outcome.markdown.or(outcome.text).unwrap_or_default()`. `min_content_chars` only *nudges tier escalation* inside the fetcher; it is not a hard floor. Confirmed in `fetcher.rs:230-290`: for an explicit `Http` strategy the fetch returns the tier result **regardless** of length (line 270), and for `Auto`/`AutoWithResearch` the pipeline still returns `Ok` with the last tier's output even when nothing meets `min_chars`. So a page that yields no article body (paywalled shell, JS-only page under `strategy:"http"`, empty/non-HTML body → `html_to_markdown` returns `""`) produces `markdown = ""`. The app then writes a 0-byte `page.md`, and returns `{status: Some(200), markdown_chars: 0, markdown: ""}` as a success.
- **Root cause**: The app treats "fetch returned Ok" as "extraction succeeded" and coerces the empty case with `unwrap_or_default()` instead of asserting a non-empty result / honoring `min_content_chars` as a post-condition.
- **Impact**: Callers cannot distinguish "page had no readable content / extraction failed" from a real empty document; scheduled pipelines store empty artifacts and mark the job green.
- **Fix sketch**: After building `markdown`, if it is empty (or shorter than the effective `min_content_chars`), return `Err(Error::App(...))` (or surface a `content_ok: false` flag) instead of saving/returning an empty success.

## 3. HackerNews treats an empty parse as success — soft rate-limit / markup drift silently yields zero stories
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: edge-case / empty-as-success
- **File**: `crates/apps/hackernews/src/lib.rs:45-56, 87-100`
- **Scenario**: The loop fetches up to 5 pages back-to-back with no throttle. HN soft-rate-limits by returning **HTTP 200** with a body like "Sorry, we're not able to serve your requests this quickly." — `response.is_success()` is `true`, so no error is raised, but `parse_front_page` finds zero `tr.athing` rows and returns an empty `Vec`. The same happens if HN renames the `athing`/`titleline`/`subtext` classes. The run then reports `count: 0, new: 0, changed: 0` as a success, and `upsert_many` records nothing new. (A hard 503 instead aborts the whole run at line 52, discarding stories already collected from earlier pages.)
- **Root cause**: `is_success()` (HTTP status) is used as the success signal, but HN signals throttling in-band at the HTTP-200 level; a zero-row parse is never checked, so "we got throttled / the markup changed" is indistinguishable from "the front page is empty."
- **Impact**: Silent data loss — a scheduled scrape can post green runs with no stories on every rate-limited/drifted execution; no alerting surface.
- **Fix sketch**: Detect a zero-row parse (`stories.is_empty()` after fetching page 1, or per-page) and return `Err(Error::App("HN returned no parseable stories — rate-limited or markup changed"))`; add a small inter-page delay to reduce soft rate-limiting when `pages > 1`.

## 4. Readable embeds the full Markdown inline in the response *and* saves it as an artifact (unbounded inline payload)
- **Severity**: Medium
- **Lens**: code-refactor / bug-hunter
- **Category**: unbounded
- **File**: `crates/apps/readable/src/lib.rs:53-62`
- **Scenario**: The whole markdown is written to `page.md` via `save_artifact` (line 53) — the mechanism that exists precisely to keep large content out of the job result — and then the *same* full string is also returned inline in the JSON `Value` (`"markdown": markdown`, line 61). For a large article/long page the content is held twice in memory and inflates every stored/serialized job result. Unlike `research`, whose inline `report` is a bounded summary, `readable` inlines arbitrarily large page content.
- **Root cause**: The artifact path and the return payload were both wired to the full content; the inline copy defeats the purpose of the artifact.
- **Impact**: Wasted memory/serialization cost and bloated job records proportional to page size; the `page.md` artifact already carries the content.
- **Fix sketch**: Return the artifact path plus `markdown_chars` (and maybe a truncated preview) instead of the full `markdown`; or gate the inline copy behind an opt-in param and cap its length.

## 5. HackerNews front page is a full snapshot but uses `upsert_many`, so delisted stories are never detected or removed
- **Severity**: Medium
- **Lens**: bug-hunter / code-refactor
- **Category**: sync-misuse
- **File**: `crates/apps/hackernews/src/lib.rs:61-71`
- **Scenario**: Each run scrapes the current front page across all requested pages — a complete snapshot of what is on the front page *right now*. It persists via `upsert_many` (partial-merge semantics, `app.rs:268-282`). Because upsert only adds/updates keys, stories that have dropped off the front page since the last run are never removed from the `stories` dataset and never reported. The dataset grows monotonically, and there is no "these stories left the front page" (`removed`) signal — which `sync_many` is documented to provide for exactly this "full listing → disappeared-record" case (`app.rs:281-282`).
- **Root cause**: A full-snapshot source is written through the partial (`upsert_many`) path. The convention is upsert = partial / sync = full snapshot; the front page matches the full-snapshot shape.
- **Impact**: Stale accumulation and no delisting detection; consumers can't tell a story fell off the front page, and dataset size is unbounded over time.
- **Fix sketch**: Use `sync_many("stories", &items)` (still keyed by HN id) so the summary yields `removed` keys for stories that left the front page; keep `upsert_many` only if the intent is truly an append-only archive.
