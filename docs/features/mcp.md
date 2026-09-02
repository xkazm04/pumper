# MCP server — Pumper as a native agent tool

Pumper mounts an MCP (Model Context Protocol) endpoint at `/mcp` beside the
REST router — `POST` for JSON-RPC exchanges, `GET` for a live SSE
notification stream — so any MCP-capable agent runtime (Claude Code, Claude
Desktop, or anything speaking streamable-HTTP) gets a live, queryable — and,
if you opt in, *actuatable* — web-data layer with zero glue code.

Everything it serves is derived from the same sources the REST surface uses:
the app registry (with each app's **manifest** — params JSON Schema, worked
examples, output shape, cost class), the dataset store and its `?filter=`
grammar, the full-text index, and `catalog/data-sources.toml`.

## Enabling

Default **OFF**. Three switches, deliberately separate:

```toml
[mcp]
enabled = true             # mount /mcp (POST JSON-RPC + GET SSE) at all
allow_enqueue = false      # offer the actuating tools (spend + target load)
allow_approve = false      # offer approve_transaction (a LIVE, irreversible web action)
max_job_budget_usd = 1.0   # hard clamp on any MCP-enqueued job's budget_usd
wait_job_max_secs = 60     # cap on wait_job's timeout_secs
```

With `allow_enqueue = false` (the default) the surface is **read-only**:
agents can discover apps, query datasets, search, watch events, and await
jobs, but cannot create them — nor **author** the reactive edges that create
them. `create_trigger` / `test_trigger` / `trigger_decisions` / `create_watch`
/ `create_ingress_source` ride the same switch, the two reads included: a
trigger is a *standing* commitment to enqueue work rather than a lesser
authority than one enqueue, and a dry-run or a decision ledger is only useful
to something that can author. When enabled, every enqueue's `budget_usd`
(`enqueue_job`, `deep_research`) is clamped to `max_job_budget_usd` (absent =
the ceiling itself; `0` = free tiers only) — an agent cannot ask its way past
the operator's rail.

`allow_approve` is a **third** switch rather than a reuse of `allow_enqueue`,
because the two authorities are not the same size: an enqueue spends money a
ceiling bounds, while approving a transaction submits a form on a live site
under the operator's logged-in profile and cannot be undone by anything this
process controls. It is additionally inert unless `[transact] allow_live = true`
— `approve_transaction` is not even listed until **both** are on, because a tool
that is offered and then refuses every call reads to an agent as a broken
server rather than as a policy. See [apps.md § transact](apps.md#transact-evidence-approval-submit).

## Client config (`.mcp.json`)

Claude Code / Desktop project snippet:

```json
{
  "mcpServers": {
    "pumper": {
      "type": "http",
      "url": "http://localhost:8088/mcp"
    }
  }
}
```

(Adjust the port to `[server] port`. The endpoint is unauthenticated like the
rest of the API — keep it on localhost, or front it with the same reverse
proxy you'd use for the REST surface.)

Pumper writes a config of exactly this shape for its **own** subprocess when
`[claude] self_hosted_tools` is on — with two headers added. See
[The self-hosted agent loop](#the-self-hosted-agent-loop-claude-self_hosted_tools).

## Tools

| Tool | Gated by | What it does |
|---|---|---|
| `list_apps` | — | Every registered app as an agent-ready tool definition: `inputSchema` (the app's params JSON Schema; permissive `{"type":"object"}` when undeclared), worked `examples`, `output_shape`, `cost_class` (`free`\|`metered`\|`claude`), schedule, readiness. |
| `query_dataset` | — | Records from `app`/`dataset`, with the shipped repeatable `$.path:op:value` filter grammar (`eq`\|`contains`\|`gte`\|`lte`\|`numgte`, ANDed) and a 1000-row clamp. |
| `search` | — | BM25 full-text search across indexed job results, scopable to app/dataset. |
| `wait_job` | — | Await one job's **settling**: a terminal status (`succeeded`\|`failed`\|`cancelled`) **or** `waiting` — a job that parked to ask *you* for something. On `waiting` the result carries `input_request` (what it needs) and a `note` saying it is parked, not finished; answer with `resume_job` and call `wait_job` again. Watches the event bus; `timeout_secs` clamped to `wait_job_max_secs`; hitting the deadline returns `timed_out: true` with the job's current snapshot (call again to keep waiting), not an error. |
| `resume_job` | `[mcp] allow_enqueue` | `{job_id, input?}` → answers a `waiting` job and lets it continue: it resumes from the checkpoint it parked at, reads `input` back through `ctx.restore_input()`, and **burns no retry attempt**. Refused (tool error) when the job is not waiting — including a second resume of one you already answered, which is the same `status = 'waiting'` fence `POST /jobs/{id}/resume` uses, so the two surfaces cannot disagree. Gated like `enqueue_job` because resuming lets a job spend the rest of its budget. See [runtime.md](runtime.md#jobs-that-wait-waiting). |
| `enqueue_job` | `[mcp] allow_enqueue` | Enqueue one job. `params` shallow-merge over the app's defaults and are **validated against the app's schema** (violations come back as a readable tool error with JSON-pointer paths). Budget clamped as above. |
| `fetch_readable` | `[mcp] allow_enqueue` | `{url}` → enqueues a `readable` job (URL → clean Markdown in the job's `page.md` artifact) through the exact gated path; returns the job id for `wait_job`. |
| `deep_research` | `[mcp] allow_enqueue` | `{query, budget_usd}` → enqueues a `research` job (agentic search + read + synthesize via the Claude engine). The clamped budget is both the job's spend ceiling and the app's own `max_budget_usd` param, so the rail also binds mid-run. |
| `run_workflow` | `[mcp] allow_enqueue` | `{workflow, budget_usd?, idempotency_key?}` → opens a run of a **declared multi-step workflow** (a DAG of ordinary jobs with fan-in join barriers) and returns its run id. `budget_usd` is the envelope for the **whole run** — each step's ceiling is clamped to what is left of it — and is itself clamped to `max_job_budget_usd`. Goes through the same door as `POST /workflows/{id}/runs`, so an agent and an operator get identical validation and idempotence. See [workflows.md](workflows.md). |
| `wait_workflow` | — | `{run_id, timeout_secs?}` → settles on a workflow run (`succeeded`\|`failed`\|`cancelled`) and returns the step matrix plus the **rolled-up receipt**: cost summed from `cost_events` over the run's whole job set, yield from `job_yield`. `timeout_secs` is clamped to the same `wait_job_max_secs` rail `wait_job` honours; hitting the deadline returns `timed_out: true` with the current matrix. A step that never became a job reports `cost_usd: null`, not `$0`. |
| `list_pending_transactions` | — | `{limit?}` → the approval inbox: every `transact` transaction awaiting a human decision (`transaction_id`, `idempotency_key`, `profile`, `state`, `evidence_sha`, derived `expires_at`), plus the node's `allow_live` and `approve_enabled` flags. Always offered — reading the inbox releases nothing — and stale rows are swept first, so nothing is listed that the approve door would refuse. Read the full evidence bundle through `wait_job`'s `input_request` or `GET /transactions/{id}`. |
| `approve_transaction` | `[mcp] allow_approve` **and** `[transact] allow_live` | `{transaction_id, evidence_sha}` → approves one pending transaction and resumes the job parked on it, which then performs the irreversible action **once**. `evidence_sha` is **required here** although the HTTP door treats it as optional: an agent approving without naming what it read is exactly the case this gate exists for. Goes through the same pure decision function and the same `state = 'pending'` SQL guard as the HTTP door, so an agent and a human cannot get different answers about a stale approval, and a race between them cannot submit twice. The commit re-probes the live page and refuses again if it drifted. |
| `create_trigger` | `[mcp] allow_enqueue` | Author one standing reactive **edge**: when a source event happens (`dataset` change batch, `job` terminal status, or `external` inbound webhook), enqueue a target app. `bind` steers the target's **own** params from the event — `{target param: JSON pointer}` resolved against the `{template, _trigger}` view, e.g. `{"url": "/_trigger/payload/repository/html_url"}` — and `each` (a pointer to an array) fans one event out into one job per element, which then read `/_trigger/item`. A pointer that resolves to nothing does **not** enqueue: it records `bind_miss`. Same handler, same validation as `POST /triggers`. See [triggers.md](triggers.md#param-binding-and-fan-out). |
| `test_trigger` | `[mcp] allow_enqueue` | `{trigger_id, fire?}` → **dry-run** one edge against its most recent matching source job: `would_fire`, the fully resolved params (binding and fan-out applied exactly as the live path applies them), the fan-out shape, and any hook incidents. Nothing is enqueued unless `fire: true`, which fires every planned hop with the idempotency key bypassed and is refused if the resolved params fail the target's schema. The tool an agent runs *before* relying on an edge it just wrote. |
| `trigger_decisions` | `[mcp] allow_enqueue` | `{trigger_id, limit?, cursor?}` → one page of the **decision ledger** plus the jobs the trigger enqueued. Every evaluation is recorded, skips included (`fired`, `bind_miss`, `fan_out_empty`, `filter_miss`, `no_change_match`, `status_mismatch`, `bad_params`, `dedup`, `cycle`, `depth`, `predicate_veto`, the hook faults, …), so "I wired it and nothing happened" has an answer. |
| `create_watch` | `[mcp] allow_enqueue` | `{app, dataset?, url?, secret?, sink?}` → subscribe a destination to a dataset's changes (`webhook` \| `file` \| `slack` \| `plugin:<name>`). Goes through the same namespace gate as `POST /watches`, so an `(app, dataset)` pair that could never fire is refused with the namespace that would. |
| `create_ingress_source` | `[mcp] allow_enqueue` | `{name, secret?}` → a named credential an external system POSTs signed events to at `/ingest/{id}`, which a `source_kind: "external"` trigger then reacts to. **The secret is returned by this call and never again.** The result also carries `ingress_enabled`, because the CRUD works while `[ingress] enabled = false` and an agent that did not know that would wonder why deliveries 409. |
| `fetch` | a **job token** | `{url, strategy?, profile?, archive_max_age?, to_markdown?}` → fetches one URL through this host's tiered fetcher and answers **synchronously** (unlike `fetch_readable`, which enqueues a job and hands back an id). Metered against the job named by the token: governor, response cache, learned tier router, session profile, archive tier, budget ceiling and cost ledger all apply. `strategy` is `http`\|`browser`\|`auto` (default) — `auto_with_research` is refused, because the caller of this tool *is* the research tier. See [The self-hosted agent loop](#the-self-hosted-agent-loop-claude-self_hosted_tools) below. |

## The self-hosted agent loop (`[claude] self_hosted_tools`)

Default **OFF**. Flipped on, the Claude research tier stops fetching the web
with the CLI's own `WebFetch` and starts fetching it through *this node*.

**Why.** `ClaudeEngine` launched the subprocess with
`--allowedTools WebSearch,WebFetch`, so the most expensive tier in the ladder
re-fetched the same URL the http and browser tiers had just tried — from the
same IP, with no politeness spacing, no cookie profile, no archive fallback, no
response cache, no VCR cassette, no tier-router learning and **no ledger row**.
Every other tier is governed and metered at the `AppContext::fetch` chokepoint;
this one was a hole in it, and the hole was on the tier that spends money.

**What happens with it on.** Per research run, the engine:

1. mints a **job token** — 256 bits, in-memory, bound to that one job, revoked
   when the run's guard drops (including on the cancel path, where none of the
   ordinary exit code runs);
2. writes a scratch `.mcp.json` naming `[claude] self_hosted_url` with the token
   in an `x-pumper-job-token` header, and passes `--mcp-config <file>
   --strict-mcp-config`;
3. passes `[claude] self_hosted_allowed_tools` (default `["mcp__pumper__fetch"]`)
   as `--allowedTools` **instead of** `allowed_tools` — so `WebFetch` is not on
   the subprocess's list at all;
4. deletes the config file and revokes the token when the run ends.

The tier-3 prompt names `mcp__pumper__fetch` and adds "otherwise fetch it
however you can", so the same sentence is correct in both config states.

```toml
[claude]
self_hosted_tools = true
self_hosted_url = "http://127.0.0.1:8088/mcp"   # must match [server] port
self_hosted_key = "..."                          # only in [auth] mode = "keys"
self_hosted_allowed_tools = ["mcp__pumper__fetch"]
self_hosted_token_ttl_secs = 3600                # backstop; the guard is the real bound

[mcp]
enabled = true    # the tool only exists when the MCP surface is mounted
```

### The token is attribution, not access

A job token says *"this fetch belongs to job X"*. It is **not** a credential for
the MCP surface, and it deliberately travels in its own header so it cannot be
confused with one.

- **`[auth] mode = "open"`** (the default): nothing else is needed. The token
  alone gets the `fetch` tool to identify its job.
- **`[auth] mode = "keys"`**: `POST /mcp` is a mutating route, so the identity
  layer requires a key with `admin` scope there like any other mutation — that
  is pre-existing behaviour of the MCP surface, not something this loop adds.
  Set `[claude] self_hosted_key` to such a key and the engine writes it into the
  run's config as `Authorization: Bearer <key>` beside the job token. Leave it
  unset in `keys` mode and every tool call the subprocess makes is a 401 from
  the identity layer, before the token is ever read. An absent or blank key is
  **omitted** from the config rather than written as an empty bearer.
  Known gap, inherited from N20: the narrowest key that works today is an
  `admin` key, and the subprocess reads untrusted scraped content, so treat the
  loop as giving that content's author a prompt-injection path to an admin key
  on this node. Run it on loopback, and see [auth.md](auth.md#known-gaps).

Every refusal is a readable tool error tagged with the status the REST surface
would have used:

| refusal | when |
|---|---|
| `[unauthorized] no job token: ...` | the header was absent — what an ordinary MCP client gets |
| `[unauthorized] unknown job token: ...` | never minted here, or already revoked with its run |
| `[unauthorized] expired job token: ...` | past `self_hosted_token_ttl_secs` |
| `[not_found] job '<id>' no longer exists` | the token names a job that has been deleted |
| `[conflict] job '<id>' is <status> ...` | the job is not `running`, so nothing of its is legitimately fetching |

### What the receipt says

`GET /jobs/{id}/receipt` gains `cost.self_hosted_fetches` — how many fetches the
*model* drove, counted off a zero-cost `claude_subfetch` marker row the tool
writes beside the priced row `AppContext::fetch` already wrote (the money is
counted once). It is `0` on every ordinary run, which is the honest answer and
the number that used to be zero *by construction* for research runs.

### Out of the v1 slice

- **Only `fetch`.** `search` and `query_dataset` through the loop are one config
  line away (`self_hosted_allowed_tools`) but are not in the shipped default and
  are untested from this direction.
- **No VCR.** The context this tool builds runs with VCR `Off`: recording into a
  cassette the worker's task owns would interleave two writers, and replay would
  have to resolve against a cassette this call never opened. So a recorded
  research run does **not** capture the model's own fetches, and replaying one
  will let them go live. Do not use `[vcr] replay` as an egress guarantee for a
  self-hosted research run.
- **No progress or checkpoints.** A fetch the model made is not a resumable step
  of the app, so both seams are no-ops in this context.
- **No per-call budget.** The job's `budget_usd` is the only ceiling; the tool
  cannot be given a smaller one of its own.
- **Tokens are Claude-only.** Nothing mints one for a non-Claude agent.
- **`self_hosted_url` is not derived.** Nothing checks it against
  `[server] port`; a mismatch surfaces as the subprocess reporting it has no
  working tools.

## Resources

- `pumper://catalog/sources` — the data-source catalog (markets, cadences,
  status, serving apps).
- `pumper://apps/{name}/manifest` — one per registered app; the same JSON as
  the `list_apps` tool entry.

The same tool-definition JSON is also served over plain REST at
`GET /apps?format=tools` for agent frameworks that consume tool definitions
without speaking MCP.

## Manifest enforcement on the REST surface

The manifest substrate is not MCP-only: `POST /apps/{name}/jobs` validates
the merged params of any app that declares a `params_schema` and rejects
violations with **422** (message carries `params/<pointer>` paths). Apps
without a declared schema behave exactly as before. Rich manifests currently
ship for `extractor`, `crawl`, `research`, `grants-gov`, and `plugin`; a
server test guarantees every manifest example (and every scheduled app's
`default_params`) passes its own schema.

## Protocol notes

- Transport: MCP **streamable-HTTP, stateless mode** — each `POST /mcp`
  carries one JSON-RPC message (or a batch) and gets one `application/json`
  response; notifications get `202`. Implemented in
  `crates/server/src/mcp/` by hand (the vocabulary is five methods:
  `initialize`, `tools/list`, `tools/call`, `resources/list`,
  `resources/read`) rather than via the `rmcp` crate — see the module doc.
- Protocol revisions: `2025-06-18` and `2025-03-26` (the client's choice is
  echoed when supported).
- **Notifications**: `GET /mcp` opens the transport's SSE half — a one-way
  stream of JSON-RPC notifications bridged from the event bus. Each SSE
  event's `data` is one `notifications/pumper/job` message
  (`params: {seq, event: {job_id, app, status, result?, error?}}`) and its
  `id` is the bus's monotonic sequence, so reconnecting with `Last-Event-ID`
  replays the missed gap from the ring; when the gap has been evicted, a
  single `notifications/pumper/reset` (`params: {latest_seq, reason}`) tells
  the client to resync. Per-connection filters: `?app=<name>` and
  `?kind=queued,running,waiting,succeeded,failed,cancelled,external`
  (comma-separated). Buffering is bounded (broadcast capacity + replay
  ring) — a consumer too slow to keep up drops events with a server-side
  warning and is recovered from the ring (or reset), never blocking the bus.
- `/mcp` is deliberately absent from `openapi.json`: it speaks JSON-RPC, not
  REST, and the spec-coverage test documents the REST inventory only.
