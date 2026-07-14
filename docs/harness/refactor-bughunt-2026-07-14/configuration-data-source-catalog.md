# Configuration & Data Source Catalog — refactor + bug-hunt findings

> Total: 4 findings (Critical: 0, High: 0, Medium: 2, Low: 2)
> Files scanned: `config.toml`, `catalog/data-sources.toml`, `catalog/connector-docs.json` (all in full); cross-referenced `crates/core/src/config.rs`, `crates/apps/connector-api-watch/src/lib.rs`, and the `schedule()`/`name()` of every app crate.

Context notes for the reader:
- `config.toml` ↔ `config.rs` was checked key-by-key: **every** key present in `config.toml` maps to a struct field, and every struct field omitted from `config.toml` has a manual `Default` impl. No config-key drift, no dead keys, no latent-panic key. The config side is clean; no finding is filed for it.
- `catalog/data-sources.toml` is **not deserialized by any Rust code** — it appears only in `//!` doc-comments and `catalog/README.md`. It is a human/LLM "single source of truth" reference doc, so its issues are stale-data/drift (no runtime panic is possible from it).
- `catalog/connector-docs.json` **is** loaded at runtime by the `connector-api-watch` app (`std::fs::read_to_string` + `serde_json`), which reads only `slug`, `label`, `docs_url`; it fetches each `docs_url`, converts to markdown, and upserts keyed by `slug`. (`icon`/`color` are ignored by pumper — they serve the personas UI, so they are not "dead".)

## 1. Catalog understates automation: two live daily pipelines are documented as un-scheduled (`cron = ""`)
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: stale-data / config-drift
- **File**: `catalog/data-sources.toml:123` (ca-grants), `catalog/data-sources.toml:237` (eu-sedia)
- **Scenario**: The catalog header defines `cron` as the "exact 6-field expr … when on the scheduler; \"\" otherwise", and states the file is the source of truth humans and LLMs read "to assess the state of the data pipelines at a glance." `ca-grants` is listed `status = "live"`, `cadence = "daily"`, `cron = ""`; `eu-sedia` likewise `status = "live"`, `cadence = "daily"`, `cron = ""`. But both apps ARE registered on the scheduler: `crates/apps/ca-grants/src/lib.rs:40` returns `Some("0 30 9 * * *")` and `crates/apps/eu-sedia/src/lib.rs:46` returns `Some("0 0 10 * * *")`. An operator or LLM reading the catalog concludes these two pipelines only run on-demand, when in reality they fire every day.
- **Root cause**: The `cron` column was not updated when the two apps gained a `schedule()`. Every other live-scheduled app in the catalog (`grants-gov` `0 0 9 * * *`, `mpsv-vpm` `0 0 6 * * *`, `mpsv-ispv` `0 0 7 1 */3 *`, `connector-api-watch` `0 0 6 1 * *`) correctly mirrors its app's `schedule()`; only these two drifted.
- **Impact**: Misleading pipeline-state view (wrong "is it automated?" answer for 2 of 6 live sources); risk of a human manually scheduling a duplicate cron or wrongly treating the feed as stale.
- **Fix sketch**: Set `cron = "0 30 9 * * *"` for `ca-grants` and `cron = "0 0 10 * * *"` for `eu-sedia` to match their `schedule()`; consider a small test/CI check that asserts each catalog `cron` equals the corresponding app's `schedule()`.

## 2. Four connector pairs share an identical `docs_url` → duplicate fetches, duplicate records, duplicate downstream events
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: duplication
- **File**: `catalog/connector-docs.json` — pairs at lines 78 & 85, 176 & 421, 295 & 358, 309 & 316
- **Scenario**: These distinct slugs point to byte-identical `docs_url`s:
  - `azure_devops_org` (L78) == `azure_devops` (L85) → `https://learn.microsoft.com/en-us/rest/api/azure/devops/`
  - `confluence` (L176) == `jira` (L421) → `https://id.atlassian.com/manage-profile/security/api-tokens`
  - `gemini_vision` (L295) == `google_gemini` (L358) → `https://ai.google.dev/gemini-api/docs`
  - `github_actions` (L309) == `github` (L316) → `https://github.com/settings/tokens`
  The watcher (`connector-api-watch/src/lib.rs:114-205`) iterates every entry and fetches `entry.docs_url`, so each shared URL is fetched twice per monthly run and stored twice (keyed by two different slugs). When one of those upstream pages changes, BOTH slugs upsert `ChangeKind::Changed` and each pushes a separate entry into `changes.json` (L194-204) — which is the hand-off bridged into personas' `builtin_shared_events.rs`.
- **Root cause**: The generator emitted the same "how to get a token / API index" URL for related-but-separate connectors instead of a per-connector documentation URL.
- **Impact**: Wasted duplicate fetches; duplicate rows in the `connector_docs` dataset; and, most materially, a single upstream doc change fires two near-identical curated connector-update events downstream to end users.
- **Fix sketch**: Give each connector a distinct, connector-specific docs URL (e.g. Azure DevOps org-aware vs base REST index; Gemini Vision vs Gemini text), or collapse true synonyms into one watched entry and fan the event out by slug at the personas side.

## 3. Several `docs_url`s point at auth-gated dashboards / marketing pages, not public docs (incl. a literal `_` placeholder)
- **Severity**: Low
- **Lens**: bug-hunter
- **Category**: stale-data / malformed
- **File**: `catalog/connector-docs.json:764` (supabase), `:211` (desktop_obsidian), `:169` (cloudflare), `:309`/`:316` (github*), `:596` (notion), `:582` (netlify), `:827` (vercel)
- **Scenario**: The file's own `note` says these are "connectors with a public docs_url", but many entries target login-walled token/dashboard pages or marketing homepages rather than documentation the watcher can diff:
  - `supabase` → `https://supabase.com/dashboard/project/_/settings/api` — contains the literal unresolved `_` project-id placeholder; this can never resolve to a real Supabase API-docs page.
  - `desktop_obsidian` → `https://obsidian.md/` — the Obsidian **marketing homepage**, while the separate `obsidian` slug (L615/617) already watches the real REST-API docs (`coddingtonbear.github.io/obsidian-local-rest-api/`).
  - `github`/`github_actions` → `github.com/settings/tokens`, `cloudflare` → `dash.cloudflare.com/profile/api-tokens`, `notion` → `notion.so/my-integrations`, `netlify` → `app.netlify.com/user/...`, `vercel` → `vercel.com/account/tokens` — all login-gated app/settings pages.
  When the fetcher hits these, it captures a login wall / marketing page, not API docs. The watcher's whole purpose (`summarize_change`, L268) is to flag endpoint/auth/param changes — which it will never see for these connectors, while it churns noise diffs whenever the login/marketing shell changes.
- **Root cause**: The upstream generator (`scripts/connectors/builtin/*.json`) used each connector's "where to get a key" URL as `docs_url` instead of its developer documentation URL.
- **Impact**: A subset of connectors are effectively un-watched (or emit noise); `supabase` in particular is guaranteed non-functional. Errors are caught (pushed to `errors[]`, no panic), so impact is bounded to a monthly enrichment path.
- **Fix sketch**: Repoint these to true public docs (e.g. Supabase → `supabase.com/docs/reference/api`, GitHub → `docs.github.com/rest`, desktop_obsidian → drop or reuse the real Obsidian REST-API docs); fix the generator so `docs_url` prefers a documentation host over a dashboard/token host.

## 4. `connector-api-watch` catalog row mislabels a local manifest path as a `url` and the tiered+Claude pipeline as `engine = "http"`
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: malformed / inconsistent-structure
- **File**: `catalog/data-sources.toml:411` (url), `:416` (engine)
- **Scenario**: The catalog defines `url` as "primary endpoint / portal" and `engine` as `http | browser | claude | bulk`. The `connector-api-watch` entry sets `url = "catalog/connector-docs.json"` — a relative local file path, not an endpoint — and `engine = "http"`. In reality the app (`connector-api-watch/src/lib.rs`) reads that JSON manifest from disk, fetches N external `docs_url`s through the **tiered** fetch engine (http→browser→claude escalation), and additionally invokes the **Claude** engine to summarize diffs (`summarize_change`, L268-298). So the single `url`/`engine` cells don't describe what this meta-source actually does.
- **Root cause**: The catalog schema assumes one source == one external endpoint + one engine; this fan-out watcher (1 manifest → many pages + an LLM pass) doesn't fit that shape, and the row was filled in with the manifest path and the primary tier only.
- **Impact**: The self-described "single source of truth" mildly misrepresents this pipeline to any human/LLM reading it (looks like a plain HTTP pull of one JSON file). No runtime effect (the catalog isn't loaded).
- **Fix sketch**: Either point `url` at the personas connector-docs index it ultimately serves and note the manifest path in `notes`, or extend the catalog schema/notes to mark fan-out sources; consider `engine = "claude"` or a `"mixed"` value to reflect the summarization pass.
