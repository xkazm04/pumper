# Content & Research Apps — moonshot scout report (2026-09-01)

Scout: read-only subagent over the group's contexts; cards in the scan-sweep §4.10 form. Deck ids (N-numbers) are in [INDEX.md](INDEX.md).

## CR1 — Compiled sources go live: promote a proposal straight into a scheduled, self-repairing pipeline

- deck item **N13**
- lens: `moonshot-architect` · size: **XL** · gate: **policy** · effort 8 / impact 10 / risk 6
- contexts: source-provisioner, declarative-extractor, web-crawler, page-monitor
- extends: M44 (proposal compiler) + M19 (catalog reconciler) + M10/M42 (replay + archive); v1 stops at a paste-ready TOML fragment and a human still does all five Path B steps

### Summary
The provisioner already compiles a sentence into a dry-run-verified RuleSet, seeds, cadence and a catalog row, and the server already has validate/promote routes and a catalog reconciler that turns `live` rows into schedules. What is missing is the last metre: nothing can EXECUTE a promoted proposal, because the catalog row has no app that runs a RuleSet, no `params`, and `promote` returns TOML instead of provisioning. This card makes a promoted proposal a running pipeline (crawl-or-urls extractor schedule under a generic app) with a health-driven re-compile loop, so sources become cattle: minted from a sentence, watched, and re-compiled when they drift.

### Description
- The compiler deliberately emits an inert row: `app: String::new()` (crates/apps/provisioner/src/lib.rs:646), `cron: String::new()` and `status: "planned"` (lib.rs:654-655), `dataset: proposed:<key>` (lib.rs:559-561). The feature doc lists the five human steps that remain — new crate, registry entry, paste+edit the row, create the dataset, `POST /schedules` (docs/features/apps.md "provisioner automates none of the Path B contract").
- `promote_proposal` only re-renders TOML and marks the record (crates/server/src/routes/provisioner.rs:231-246); the catalog `Source` struct has no `params` field at all (crates/core/src/catalog.rs:31-68), so even a `live` row cannot carry the proposal's rules/seeds to the reconciler (`apply_reconcile` creates schedules from `{app, cron}` only — crates/server/src/scheduler.rs:486-538).
- Yet the executor exists: the extractor's urls mode runs any RuleSet over any URL list and writes a dataset with provenance, health verdicts and `index_datasets` (crates/apps/extractor/src/lib.rs:1416-1538); the health detector renders a per-source verdict with `worst_fields` (lib.rs:866-950) and can quarantine/divert writes (lib.rs:691-705); the provisioner's repair loop already consumes exactly that vocabulary as feedback (`worst_fields`, `rejections` — provisioner lib.rs:1176-1204) and resumes the research session (`req.resume_session = session_id` lib.rs:1151).
- Technique that applies: the catalog rule (ONBOARDING §10) says the catalog is the control plane; extend it rather than bypass it. `Source` gains an optional `[source.run]` block `{app, params}` (registry-validated at load, like schedule params are validated at the door — scheduler.rs:346-357), and `promote` gains an opt-in `provision: true` that writes the row (status `live`, cron from cadence, `run = {app: "extractor", params: {urls: seeds, rules, dataset}}`) through the existing reconciler, so the drift gate keeps machine-written rows load-bearing.
- The self-repair loop closes M44's stated step 5: a dataset trigger on `web-reliability/host_observations` / the health store firing on `state` leaving `healthy` enqueues `provisioner` with `{prompt, repair_from: key, feedback: worst_fields}`; a validated repair replaces the row's rules via the same promote path, gated by `may_promote` (provisioner lib.rs:716-723). Replay-CI (extractor replay.rs:423-558) is the pre-flight: the repaired RuleSet is diffed against the stored corpus before it goes live, so a repair cannot regress silently.

### Flow
- Add `Source.run: Option<RunSpec{app, params}>` to core catalog + TOML; reconciler passes `params` into the schedule row; loader refuses a `run` whose app is unregistered or whose params fail the app schema (reuse `validate_schedule_params`).
- Extend `POST /provisioner/proposals/{key}/promote` with `provision: bool` (default false, keeps today's TOML-only contract): when true and `may_promote` passes, write the `[[source]]` row with `run` and `status = live`, then run the reconciler once; response carries `{schedule_id, catalog_row}`.
- Make `extractor` urls-mode the first-class executor for compiled sources: accept `cadence`-derived cron defaults and stamp `source_id` into provenance so `/reliability` and health verdicts key on the catalog id, not just `{app}/{dataset}`.
- Repair trigger: a built-in trigger (or `[provisioner] auto_repair = true`) listens for health `degraded|quarantined` on a compiled source, enqueues `provisioner` in repair mode (new `repair_from` param, checkpointed like discovery), and on an accepted+replay-clean draft re-promotes; every hop lands in the proposal's revision history.
- Budget rails: per-source `budget_usd` from the proposal, a global `[provisioner] max_auto_repairs_per_day`, and robots/politeness inherited from the fetcher; the row is never flipped to live without the human `provision: true` call.
- Docs + catalog drift gate test for `run` rows; a `just provision-preview` (dry-run plan) recipe.

### Expected impact
Analysts (or an agent via MCP `enqueue_job`) get a source from sentence to scheduled dataset in one approve click instead of a crate + registry PR; the number of sources the box can carry stops being bounded by engineering time. Measured by time-to-first-scheduled-record for a new source (today: hours/days of Path B; after: minutes) and by the share of degraded compiled sources that heal without a human. What could break: an auto-repaired RuleSet that passes the dry run but extracts the wrong thing at scale — hence replay-CI as a hard pre-flight and the human `provision` gate.

### Evaluation
Claim: user - a promoted proposal becomes a running, self-repairing pipeline without a new crate
Before: 0 of 5 Path B steps automated (docs/features/apps.md); `promote` returns TOML only (routes/provisioner.rs:240); `Source` has no params (catalog.rs:31-68)
After: 1 API call provisions; the instrument is `GET /provisioner/proposals` gaining `schedule_id`/`last_repair` and the catalog drift gate counting `run` rows
Method: probe - traced promote → reconciler → scheduler → extractor code paths; no executable exists for a promoted row today
Result: unmeasurable (the missing instrument is a provisioned-source count and a repair-success rate; both are new datasets this card creates)
Gate: policy

### Evidence

```
crates/apps/provisioner/src/lib.rs:646 `app: String::new()`; :654-655 `cron: String::new(), status: "planned"`; :716-723 `may_promote`; :1151 `req.resume_session = session_id.clone()`; :1176-1204 repair feedback from `dry.worst_fields`/`dry.rejections`
crates/server/src/routes/provisioner.rs:231-246 promote returns TOML, writes nothing
crates/core/src/catalog.rs:31-68 `pub struct Source` — no params/run field
crates/server/src/scheduler.rs:486-538 apply_reconcile creates schedules from catalog rows; :346-357 validate_schedule_params door
crates/apps/extractor/src/lib.rs:1416-1538 urls mode executor; :866-950 observe() health verdict + worst_fields; :691-705 quarantine diversion
crates/apps/extractor/src/replay.rs:423-558 read-only replay-CI pre-flight
docs/features/apps.md § Known gaps: "provisioner automates none of the Path B contract" (5 human steps)
```

## CR2 — Transact v2: approval-gated live submission with a transactions ledger and agent-facing approvals

- merged into **N01** (Transact v2: approval-gated live actions with a transactions ledger and agent tools)
- lens: `innovation-catalyst` · size: **XL** · gate: **irreversible** · effort 8 / impact 9 / risk 8
- contexts: browser-transact, agentic-research
- extends: M06 Transact v1 (dry-run only); v1 documents this exact next slice and threads idempotency_key + profile for it, but has no code path that can submit

### Summary
Pumper can already navigate, fill, click and wait to the confirmation state under a vault profile and emit a redacted evidence bundle — and then structurally stops. The next capability is the one the whole slice was designed for: a `transactions` ledger where a dry-run becomes a pending approval, a human (or a policy) approves it, and a second, replay-safe browser run performs the recorded `submit_action` exactly once, deduped on `idempotency_key`, with a post-submit evidence bundle. Exposed over MCP so an agent can request an action and a human can approve it from the same tool surface.

### Description
- v1 states the missing slice verbatim: "live submission requires the human-approval design (pending-approval transactions + `POST /transactions/{id}/approve` + a `transactions` table deduping on `idempotency_key`) — the documented next slice" (crates/apps/transact/src/lib.rs:12-16) and echoes it in every result as `next_slice` (lib.rs:245-247). `submit: true` is a 422 at the door (`"const": false`, lib.rs:107-111) and `submit_action` is captured into evidence as `would_submit` but never executed (lib.rs:102-106, 212).
- The seams are already threaded: `idempotency_key` is required and recorded on every bundle (lib.rs:76-81, 204), the vault `profile` binds the flow to a login identity (lib.rs:81-95), secrets are masked in-page so the evidence is shareable (lib.rs:23-28), and refusals are terminal, not retried (lib.rs:167-172) — the retry ladder can never double-submit by accident.
- Nothing server-side exists yet: `grep -i approve|transactions crates/server/src` hits only a comment in main.rs:651. The MCP server exposes `enqueue_job`/`wait_job`/`deep_research` but no approval primitive (docs/features/mcp.md:52-63).
- Design: a `transactions` table `{id, idempotency_key UNIQUE, dry_run_job_id, status: pending|approved|submitted|rejected|expired, approved_by, submit_job_id, evidence_before, evidence_after}`; the dry-run job writes `pending`; `POST /transactions/{id}/approve` (API-key gated, optional `[transact] approvers` allowlist) enqueues a `transact` job with `submit: true` whose params are copied from the ledger, not the caller; the engine gains `TransactRequest.submit` honoured only when a signed approval token is present in the host-injected `_transact` envelope (same host-owned-keys pattern as `_trigger`, lib.rs:69-72). Expiry and "page changed since dry-run" checks (DOM hash of `submit_target` — lib.rs:213) refuse a stale approval.
- Technique: the refusal-at-the-door + terminal-error doctrine this app already follows; the evidence bundle becomes the audit record, so the ledger is append-only and every state change is a revision.

### Flow
- Migration + `Storage` API for `transactions`; dry-run path upserts a `pending` row keyed by `idempotency_key` (UNIQUE) and returns `transaction_id` in the result.
- Routes: `GET /transactions`, `GET /transactions/{id}`, `POST /transactions/{id}/approve|reject`; approvals mint a single-use token stored on the row.
- Engine: `BrowserEngine::transact` accepts `submit: true` only with a valid token in `_transact`; performs `submit_action`, waits for `confirm_selector`, captures the post-submit DOM + screenshot (browser screenshots are a README still-open item — reuse if it lands, else DOM-only), marks `submitted` atomically before returning.
- Safety: stale-page refusal (hash of `submit_target` + `filled_fields` must match the dry-run), one-shot token, `[transact] allow_live = false` default, per-profile daily cap, webhooks `transaction.pending|submitted`.
- MCP: `request_transaction` (runs the dry-run, returns evidence + id) and `approve_transaction` (gated by a separate `[mcp] allow_transact_approve`, default false) so agent workflows can ask and humans can grant from any MCP client.
- Docs: extend docs/features/apps.md §transact with the lifecycle; conformance battery covers the submit path.

### Expected impact
Turns Pumper from a read-only web layer into an act-on-the-web layer with an audit trail — form submissions, portal filings, sign-ups — which is the capability agent fleets pay for and cannot get safely from a raw browser tool. Measured by submitted transactions with matching before/after evidence and zero duplicate submissions per idempotency key. What could break: a stale approval submitting against a changed page, or a token leaking — both are refused by construction (page hash + one-shot token), and `allow_live` stays off by default.

### Evaluation
Claim: user - live web actions become possible, gated by explicit approval and deduped by idempotency key
Before: 0 code paths can submit (transact lib.rs:12-16, 102-106); server has no transactions table or approve route (grep of crates/server/src)
After: one `POST /transactions/{id}/approve` performs exactly one submit; instrument = the `transactions` ledger's status counts and duplicate-key rejections
Method: probe - read the app, its schema door and result contract; grepped server for any approval surface
Result: unmeasurable (the instrument is the ledger this card creates)
Gate: irreversible

### Evidence

```
crates/apps/transact/src/lib.rs:12-16 "live submission requires the human-approval design ... the documented next slice"
crates/apps/transact/src/lib.rs:102-111 submit_action never executed; `submit` schema `"const": false`
crates/apps/transact/src/lib.rs:76-81,204 idempotency_key required + recorded; :81-95 vault profile; :23-28 in-page secret masking
crates/apps/transact/src/lib.rs:245-247 `next_slice` string in every result
crates/server/src/main.rs:651 only mention of 'transactions' in the server (a comment)
docs/features/mcp.md:52-63 tool table — no approval tool
docs/features/apps.md:44 "stops before the irreversible action ... live submission needs the human-approval slice"
```

## CR3 — Research as a living knowledge base: findings and cited sources become watched, re-derivable datasets

- deck item **N25**
- lens: `feature-scout` · size: **XL** · gate: **policy** · effort 7 / impact 8 / risk 5
- contexts: agentic-research, page-monitor, connector-api-watch, declarative-extractor
- extends: M23 (checkpointed research sessions) + M12 (provenance) + M43 (MCP deep_research); today a research run leaves only a job result and a `report.json` artifact

### Summary
The research app is the platform's most expensive producer and the only one whose output never becomes a dataset: findings and sources live in `jobs.result` and an artifact, the agent fetches its sources outside the tiered fetcher (no cache, no archive, no provenance), and nothing notices when a cited page changes. This card makes research compound: every run upserts `research/findings` and `research/sources` records with real provenance, every cited URL is snapshotted through the readable/archive path, cited sources get a standing `watch`, and a change on a source re-opens the run's session with the diff as the follow-up question. Research stops being a Q&A call and becomes a self-maintaining corpus that search, triggers, derived datasets and peers can consume.

### Description
- Zero dataset writes today: crates/apps/research/src/lib.rs has no `ctx.upsert*` and no `ctx.fetch` call (grep confirms); the run ends at `save_report_artifact` (lib.rs:614) with a nested `report: {summary, key_findings, sources}` (lib.rs:371-386). Sources are `[{url, title}]` strings the CLI agent found on its own (lib.rs:458-460) — invisible to the HTTP cache, the governor, the archive tier and the cost ledger.
- The substrate for the reverse is all shipped: `upsert_with_provenance` stamps `source_url`/`artifact_sha` (watch app crates/apps/watch/src/lib.rs:168-179); `readable` snapshots any URL as Markdown with optional archive tier (crates/apps/readable/src/lib.rs:121-126); `watch` fingerprints a URL and returns the field-level diff (watch lib.rs:182-197); dataset watches + triggers fire per key; session resume is a first-class param (`session_id` … "the query is then a follow-up question", research lib.rs:305-309, 511-514) and checkpoints already carry `session_id` (lib.rs:48-51).
- connector-api-watch proves the diff → Claude explanation loop works at cost ~1 turn per change (`change_summary_request` max_turns=1, cached by content — crates/apps/connector-api-watch/src/lib.rs:406-434), but it is Personas-specific and ends in an artifact hand-off (lib.rs:11-15).
- Design: (1) after `final_parsed`, upsert one `findings` record per key_finding keyed `{proposal-style slug of query}#{i}` and one `sources` record per cited URL keyed by URL, with `Provenance{source_url, job_id}`; (2) a `snapshot_sources: true` param fetches each cited URL through `ctx.fetch{to_markdown, archive_max_age}` and saves `source-N.md`, stamping `artifact_sha` so the citation is re-derivable (M12 vocabulary); (3) `watch_sources: true` creates dataset watches / a `watch` schedule per cited URL under `research/sources`; (4) a built-in trigger: `research/sources` changed → enqueue `research` with `{session_id, query: "Source <url> changed: <diff excerpt>. Update the findings.", max_budget_usd}`; (5) the extractor's induce/replay pattern is the model for a read-only `explain` mode that re-scores existing findings against snapshots without a new session.
- Registry subject that constrains it: llm-observability's cost/chokepoint doctrine already enforced here (`ctx.research` is the metered path, research lib.rs:536) — every new call rides it.

### Flow
- Add `findings`/`sources` upserts with provenance to the research app (behind `persist: true`, default on for structured runs), and `index_datasets` so search/saved-search alerts see them.
- Add `snapshot_sources` (readable path, archive-first) and `watch_sources` params; snapshots as artifacts + `artifact_sha` on the source record.
- Ship the change→follow-up trigger as a documented trigger recipe (or `[research] auto_followup = true`) with a per-topic budget cap and a daily fan-out cap.
- Generalise `summarize_change` out of connector-api-watch into `research` role "explain-diff" so any `pages` change can be explained at 1 turn (shared with the explained-change card).
- MCP: `deep_research` returns `session_id` + dataset keys; `query_dataset` on `research/findings` becomes the agent's memory read.
- Docs: research section in docs/features/apps.md, data model for the two datasets, spend rails.

### Expected impact
Agents and analysts get research that stays true: a finding whose source moved is re-checked automatically, citations are verifiable against a stored snapshot, and repeat questions hit the corpus (search/derived) before paying for a session. Measured by re-research spend per changed source versus a fresh run, and by the share of findings with a replayable source snapshot. What could break: follow-up storms on noisy sources — bounded by the fan-out cap and by `watch`'s empty-extraction refusal (watch lib.rs:134-150).

### Evaluation
Claim: quality - findings become provenance-stamped, monitored records instead of one-off JSON
Before: 0 dataset records per research run (no upsert in research/lib.rs); sources are unsnapshotted `{url,title}` pairs (lib.rs:458-460)
After: N findings + M sources per run with `source_url`/`artifact_sha`; instrument = `GET /datasets/research/sources` count and the follow-up trigger ledger
Method: probe - grepped the app for writes/fetches; traced session-resume and provenance seams in sibling apps
Result: unmeasurable (needs the datasets to exist; then re-research cost per change is directly readable from the cost ledger)
Gate: policy

### Evidence

```
crates/apps/research/src/lib.rs: no `upsert`/`ctx.fetch` calls (grep); :594-614 result + `save_report_artifact` only; :371-386 output_shape nesting; :458-460 sources shape; :305-309, 511-514 session_id follow-up semantics; :536 metered `ctx.research`
crates/apps/watch/src/lib.rs:168-179 `upsert_with_provenance{source_url, artifact_sha}`; :182-197 diff in result; :134-150 empty-extraction refusal
crates/apps/readable/src/lib.rs:121-126 archive tier + recipes per request
crates/apps/connector-api-watch/src/lib.rs:406-434 `change_summary_request` (max_turns=1, cacheable); :11-15 artifact hand-off only
docs/features/mcp.md:62 `deep_research` tool
```

## CR4 — Explained change: typed, LLM-summarised change events for any watched URL or dataset

- deck item **N26**
- lens: `business-strategist` · size: **L** · gate: **policy** · effort 5 / impact 8 / risk 4
- contexts: page-monitor, connector-api-watch, plugin-runner, web-crawler
- extends: page-monitor (`watch` app) + connector-api-watch's diff→summary stage + M21 webhooks/triggers; generalises a Personas-only monthly job into a platform capability

### Summary
Pumper has two halves of a Visualping-plus-explanation product and they do not meet: `watch` fingerprints any URL and returns a raw field diff, and `connector-api-watch` turns a doc diff into `{summary, tags, severity}` with one cached Claude turn — but only for a hard-coded Personas manifest, monthly, into an artifact. This card lifts the explain stage into a generic `explain` app/hook: any `pages`/`connector_docs`/crawl `page_versions` change can be turned into a typed change event record (`what changed, severity, tags, before/after excerpt`) in a `changes` dataset that webhooks, triggers, search and the SDK already consume.

### Description
- `watch` stores `{title, chars, content_sha256, excerpt}` and reports the field diff (crates/apps/watch/src/lib.rs:156-197); it deliberately keeps records compact and saves the full Markdown as an artifact (lib.rs:20-23, 151) — so the *content* of a change is recoverable but never explained.
- `connector-api-watch` has the explain stage: `line_diff` (crates/apps/connector-api-watch/src/lib.rs:367-384), prompt with a closed tag vocabulary and severity scale (lib.rs:413-434), deterministic fallback (lib.rs:398-404, 436-479), `max_turns=1` and content-keyed caching so re-runs are free (lib.rs:406-412). But it reads its watch list from `catalog/connector-docs.json` (lib.rs:36-37), is scheduled monthly (lib.rs:117-120), and its events live only in `changes.json` + the job result (lib.rs:345-354) — no dataset, so no webhook/trigger/search ever sees an individual change.
- The crawl archive keeps every changed revision with `fetched_at` and the body (crates/apps/crawl/src/lib.rs:590-694), so the same explain stage can run over `page_versions` pairs retroactively; the plugin observatory already flags `empty_rate_rising` per site (crates/apps/plugin/src/observatory.rs:286-290) — a structural-change signal that today has no explanation attached.
- Design: a new `explain` app (`crates/apps/explain`, core-only deps) with modes `{source: {app, dataset, keys?|_trigger.keys}}` — reads the previous and current revision bodies (dataset history + artifacts, the seam `read_source_artifact` already resolves), diffs, and writes `changes` records keyed `{key}@{revision}` with `{summary, tags, severity, lines_added, lines_removed, before_sha, after_sha}` and provenance. Fired by a dataset trigger on any watched dataset, so the Visualping loop becomes `schedule(watch) → trigger(explain) → webhook`. connector-api-watch collapses to a manifest-driven watch + explain configuration.

### Flow
- Extract the diff/summarise/fallback trio from connector-api-watch into the new app (pure functions ported with their tests: `line_diff`, `parse_summary_response`, `change_summary_request`).
- Implement `explain` source mode over `(app, dataset, keys)` using revision history + artifacts; add `changes` dataset with `index_datasets` so saved-search alerts work.
- Add a manifest example + trigger recipe: `watch` on cron → trigger `explain` with `_trigger.keys` → `dataset.changed` webhook on `explain/changes`.
- Backfill mode over crawl `page_versions` (pairs of consecutive revisions per URL) under a `max_pairs` budget and checkpoint, mirroring extractor backfill (crates/apps/extractor/src/lib.rs:1782-1969).
- Budget rails: per-run `max_budget_usd`, `summarize: false` deterministic mode, and the content-keyed research cache so identical diffs cost nothing.
- Re-point connector-api-watch at the shared stage; keep its `changes.json` hand-off as a projection.

### Expected impact
Any URL becomes a subscribable, explained feed for humans and agents ("pricing page: `breaking` — tier removed"), which is the sellable core of change-monitoring products, at ~1 Claude turn per real change and zero for noise. Measured by explained changes per day and by the ratio of `severity != patch` events to raw `changed` revisions (signal density). What could break: spend on churn-heavy pages — mitigated by the deterministic fallback, the cache and per-run ceilings.

### Evaluation
Claim: user - every watched change carries a typed explanation reachable by webhooks/search
Before: explained changes exist for 1 manifest of connector docs, monthly, artifact-only (connector-api-watch lib.rs:36-37, 117-120, 345-354); `watch` changes carry a raw diff only (watch lib.rs:182-197)
After: explained events for any dataset key; instrument = `explain/changes` record count and per-event `cost_usd`
Method: probe - traced both apps' write paths and the crawl archive's revision pairs
Result: unmeasurable (the `changes` dataset is the instrument)
Gate: policy

### Evidence

```
crates/apps/connector-api-watch/src/lib.rs:36-37 DEFAULT_MANIFEST; :117-120 monthly schedule; :367-384 line_diff; :406-434 change_summary_request (max_turns=1, cacheable); :436-479 parse_summary_response; :345-354 changes.json artifact only
crates/apps/watch/src/lib.rs:156-197 fingerprint record + diff; :151 full page.md artifact
crates/apps/crawl/src/lib.rs:590-694 archive_changed → page_versions with artifact_path + fetched_at
crates/apps/plugin/src/observatory.rs:286-290 empty_rate_rising canary
crates/apps/extractor/src/lib.rs:1782-1969 batched, checkpointed backfill pattern to mirror
```

## CR5 — Artifact peering: mirror the crawl archive, not just records, so any node can extract, replay and audit another node's corpus

- deck item **N19**
- lens: `integration-planner` · size: **XL** · gate: **contract** · effort 8 / impact 8 / risk 6
- contexts: dataset-peering, declarative-extractor, plugin-runner, web-crawler
- extends: M30 dataset peering (records only, artifact_sha deliberately dropped) + M42 versioned archive + M16 observatory; v1 cannot mirror bodies so a mirror is a dead end for every stored-body mode

### Summary
Peering replicates records but the platform's real asset — the versioned page archive that extractor, plugin, replay, induce and observatory all run over — stays on the node that crawled it. The mirror explicitly drops `artifact_sha` because it holds no body. This card adds content-addressed artifact replication to the peer walk: a mirror pulls the bodies its mirrored `pages`/`page_versions` records point at (by sha, resumable, budgeted), stores them under its own artifact root, and stamps the sha honestly — so one node can crawl while others extract, replay-CI, induce wrappers or run the plugin observatory over the same corpus. It also gives the archive off-box durability and reconciles hard deletes.

### Description
- The mirror stamps `artifact_sha: None` on purpose: "this node holds no such artifact. Mirroring it would make `Provenance::replayable` answer true for a record this node provably cannot re-derive" (crates/apps/peer/src/lib.rs:913-918, 919-926); the run reports `origin_artifact_sha_dropped` (lib.rs:573-574).
- Every stored-body consumer resolves through `read_source_artifact` on `{artifact_path, job_id}` (crawl lib.rs:520-528, 649-651): extractor source/backfill (crates/apps/extractor/src/lib.rs:1673, 1867), replay (replay.rs:464, 486), induce (induce.rs:156), plugin source/backfill (crates/apps/plugin/src/lib.rs:1189, 1426) and the observatory (observatory.rs:591-593). A mirrored `crawl/pages` record therefore fails with `missing` on every one of them.
- The archive is content-addressed already: `page_versions` records carry `artifact_sha` and the file is written from the hashed bytes (crawl lib.rs:630-662); the artifact retention janitor keys on the same sha. The peer walk is cursor-resumable with per-run budgets and ETag revalidation (peer lib.rs:363-462), so adding a second, sha-keyed stream is a natural extension rather than a new protocol.
- Known gaps this also closes: no reconcile pass for hard deletes and no `[[peer]]` scheduling (docs/features/peering.md § Known gaps).
- Design: (1) origin exposes `GET /artifacts/by-sha/{sha}` (range-capable, API-key gated) and a `HEAD` for existence; (2) `peer` gains `artifacts: true` + `max_artifact_bytes` per run: after applying a page's record it enqueues the sha into a `peer/artifact_queue` dataset, then drains it under budget, writing `data/artifacts/peer_<origin>/<job>/<sha>.html` and rewriting `artifact_path`/`job_id` on the mirrored record so `read_source_artifact` resolves locally; (3) `mirror_provenance` stamps `artifact_sha` only once the bytes verified; (4) a `reconcile: true` mode lists origin keys via the existing list route and tombstones ghosts; (5) `[[peer]]` config rows reconcile into schedules exactly like catalog rows.

### Flow
- Origin routes: `HEAD|GET /artifacts/by-sha/{sha}` resolving through the artifact index (revision → artifact_sha → path), with the API-key middleware.
- Peer: artifact queue dataset + drain loop with `max_artifact_bytes`, sha verification on write, and a `artifacts_mirrored`/`artifacts_pending_bytes` block in the per-dataset report.
- `mirror_provenance`: keep `artifact_sha` when the local copy exists; `replayable` becomes true only then.
- Reconcile pass (opt-in, O(dataset)): diff origin live keys vs mirror, tombstone ghosts; report `ghosts_removed`.
- `[[peer]]` config → scheduler reconcile (reuse the catalog plan/apply shape, scheduler.rs:479-538).
- E2E: extend `e2e/peer_mirror.rs` so the mirror runs `extractor` source mode and `replay` over mirrored bodies and gets identical records to the origin.

### Expected impact
The archive becomes a shared, durable, multi-node asset: a small crawl box feeds heavy extraction/observatory boxes, and a lost node no longer loses months of history. Measured by `replayable` revisions on the mirror (today 0 for mirrored rows) and by identical extractor output origin vs mirror. What could break: disk growth on mirrors — bounded by per-run byte budgets and the existing retention janitor; auth is a prerequisite (peering.md gap) and stays behind API keys.

### Evaluation
Claim: resilience - mirrored records become re-derivable and every stored-body mode works off-origin
Before: `origin_artifact_sha_dropped` = 100% of mirrored records (peer lib.rs:919-926); every stored-body mode reports `missing` on a mirror
After: `replayable` true for mirrored rows whose bytes verified; instrument = per-run `artifacts_mirrored` and a replay run producing 0 `missing`
Method: probe - traced `mirror_provenance` and every `read_source_artifact` call site
Result: unmeasurable (needs the artifact stream; the e2e in the flow is the measurement)
Gate: contract

### Evidence

```
crates/apps/peer/src/lib.rs:913-926 artifact_sha deliberately dropped; :573-574 `origin_artifact_sha_dropped`; :363-462 resumable, budgeted, ETag-aware walk
crates/apps/crawl/src/lib.rs:520-528 page_versions artifact contract; :630-662 sha-verified archive copy
crates/apps/extractor/src/lib.rs:1673,1867; replay.rs:464,486; induce.rs:156; crates/apps/plugin/src/lib.rs:1189,1426; observatory.rs:591-593 — all resolve bodies via read_source_artifact
docs/features/peering.md § Known gaps: no auth, hard deletes leave ghosts, no scheduling
crates/server/src/scheduler.rs:479-538 catalog plan/apply shape to reuse for [[peer]]
```

## CR6 — Corpus graph intelligence: PageRank, importance-weighted revisits and structural drift from the persisted link graph

- deck item **N27**
- lens: `moonshot-architect` · size: **L** · gate: **none** · effort 5 / impact 7 / risk 3
- contexts: web-crawler, plugin-runner, declarative-extractor
- extends: M08 (edges dataset, within-run top_linked only) + M07 (learned change cadence) + M11 (derived datasets); v1 persists edges and explicitly leaves in-degree/PageRank and any consumer out

### Summary
The crawler streams a complete link graph into `edges` and then nobody reads it: ranking is a within-run, memory-capped top-10, and the feature doc says a whole-corpus ranking "would have to be computed from the `edges` dataset". This card builds the consumers: a `graph` rollup (in-degree as a derived spec today, PageRank/HITS as a checkpointed job) into `crawl/page_rank`, an importance term in the revisit due-score so budgets go to pages that matter, importance-weighted sampling for the plugin observatory, and structural-drift detection (nav/hub edges vanishing) as a first-class change signal alongside content simhash.

### Description
- Edges are persisted per kept page as `{from_url, to_url, depth, rel, job_id}` keyed `{from}|{to}` (crates/apps/crawl/src/link_graph.rs:27-36, 165-216) but "v1 deliberately persists edges only — in-degree/PageRank as datasets are out of scope" (link_graph.rs:6-8); `top_linked` freezes at 200,000 tracked edges (link_graph.rs:66, 89-106) and the doc names the gap (docs/features/crawling.md § Known gaps: "the crawl does not persist in-degree"). `grep` finds no server or core consumer of `edges`.
- The revisit frontier spends `revisit_budget` by learned change cadence only (`due-score`, crawl lib.rs:1097-1110); importance is absent, so a rarely-changing hub and a leaf get the same treatment.
- Derived datasets already support `group_by` + `count` (docs/features/datasets.md:186-188), so in-degree is one spec away: `{source: crawl/edges, group_by: $.to_url, aggregates: count}`. PageRank needs iteration, i.e. a job — the extractor backfill's paging/checkpoint pattern (extractor lib.rs:1833-1930) fits.
- The observatory samples newest-half + seeded random per site (crates/apps/plugin/src/observatory.rs:349-368); weighting by rank makes drift on hub pages surface first.
- Structural drift: edges are upsert-only (crawl lib.rs:831-861, "an edge absent this run is NOT removed"), so a hub losing 40% of its out-links today looks unchanged; a per-run `edges_seen`/`edges_missing_since` fold on the rollup gives a "site map changed" signal that neither simhash nor the health detector carries.

### Flow
- Ship the in-degree derived spec as a documented recipe (and a `just graph-indegree` helper) — zero new code, immediate `top_linked` over the whole corpus.
- Add `crawl` mode `graph` (or a `graph` app, core-only): paged PageRank over `edges` with damping, N iterations, checkpointed per pass; writes `page_rank` keyed by URL with `{rank, in_degree, out_degree, hub, authority, run_at}` declared as derived paths so re-runs over an unchanged graph report `unchanged`.
- Revisit: `RevisitSeed` gains optional `importance`; `min_due_score`/`revisit_budget` ranking multiplies due-score by rank (opt-in `importance_weight`), read from `page_rank` at seed load (crawl lib.rs:879-920).
- Observatory: `sample_by: "rank"` option reading `page_rank` for the site's pages.
- Structural drift: `graph` mode compares this run's edge set per hub against the previous rollup and writes `structure_changes` records; the health detector or a trigger can consume it.
- Docs: crawling.md link-graph section + the derived recipe.

### Expected impact
Crawl budgets and monitoring effort follow importance instead of recency, and site restructures become visible before every leaf rule breaks. Measured by changed-pages-found per revisit fetch (budget efficiency) with vs without the importance term, and by lead time between a structure-change event and the first extraction health degradation. What could break: rank feedback loops on link farms — capped by `OUT_DEGREE_CAP` and `same_domain` defaults.

### Evaluation
Claim: performance - revisit budget efficiency and earlier drift detection from a graph the crawl already stores
Before: 0 consumers of `edges` (grep); ranking = within-run top-10 frozen at 200k edges (link_graph.rs:66)
After: whole-corpus `page_rank` dataset; instrument = changed/fetched ratio on revisit runs reported in the crawl result (`revisited`, `changed`, crawl lib.rs:1248-1256)
Method: probe - read link_graph.rs, the revisit seed path and the derived-dataset grammar
Result: unmeasurable (needs two revisit runs with/without the term over the same corpus; the crawl result already carries the counters to compute it)
Gate: none

### Evidence

```
crates/apps/crawl/src/link_graph.rs:6-8 "in-degree/PageRank as datasets are out of scope"; :27-36 EDGES_DATASET + key; :66,89-106 MAX_TRACKED_EDGES freeze; :165-216 page_edges
crates/apps/crawl/src/lib.rs:1097-1110 revisit_budget/min_due_score (cadence only); :879-920 RevisitSeed load; :831-861 edges upsert-only; :1248-1256 revisit counters
docs/features/crawling.md § Known gaps: whole-corpus in-degree must be computed from `edges`; crawl does not persist in-degree
docs/features/datasets.md:186-188 derived group_by + count
crates/apps/plugin/src/observatory.rs:349-368 sample_indices (newest half + seeded random)
grep -rln edges|EDGES_DATASET|link_graph crates/server/src crates/core/src → no consumers
```

