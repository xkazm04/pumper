# MCP server — Pumper as a native agent tool

Pumper mounts an MCP (Model Context Protocol) endpoint at `POST /mcp` beside
the REST router, so any MCP-capable agent runtime (Claude Code, Claude
Desktop, or anything speaking streamable-HTTP) gets a live, queryable — and,
if you opt in, *actuatable* — web-data layer with zero glue code.

Everything it serves is derived from the same sources the REST surface uses:
the app registry (with each app's **manifest** — params JSON Schema, worked
examples, output shape, cost class), the dataset store and its `?filter=`
grammar, the full-text index, and `catalog/data-sources.toml`.

## Enabling

Default **OFF**. Two switches, deliberately separate:

```toml
[mcp]
enabled = true             # mount POST /mcp at all
allow_enqueue = false      # offer the enqueue_job tool (spend + target load)
max_job_budget_usd = 1.0   # hard clamp on any MCP-enqueued job's budget_usd
```

With `allow_enqueue = false` (the default) the surface is **read-only**:
agents can discover apps, query datasets, and search, but cannot create jobs.
When enabled, every enqueue's `budget_usd` is clamped to
`max_job_budget_usd` (absent = the ceiling itself; `0` = free tiers only) —
an agent cannot ask its way past the operator's rail.

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

## Tools

| Tool | Gated by | What it does |
|---|---|---|
| `list_apps` | — | Every registered app as an agent-ready tool definition: `inputSchema` (the app's params JSON Schema; permissive `{"type":"object"}` when undeclared), worked `examples`, `output_shape`, `cost_class` (`free`\|`metered`\|`claude`), schedule, readiness. |
| `query_dataset` | — | Records from `app`/`dataset`, with the shipped repeatable `$.path:op:value` filter grammar (`eq`\|`contains`\|`gte`\|`lte`\|`numgte`, ANDed) and a 1000-row clamp. |
| `search` | — | BM25 full-text search across indexed job results, scopable to app/dataset. |
| `enqueue_job` | `[mcp] allow_enqueue` | Enqueue one job. `params` shallow-merge over the app's defaults and are **validated against the app's schema** (violations come back as a readable tool error with JSON-pointer paths). Budget clamped as above. |

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
- **Notifications seam**: bridging the EventBus replay ring into MCP
  `notifications/*` needs the transport's SSE half (`GET /mcp` + session
  ids). Deliberately not implemented; `GET /mcp` returns 405. Agents that
  want live events can consume the existing `GET /events` SSE stream, which
  already has `Last-Event-ID` resume.
- `/mcp` is deliberately absent from `openapi.json`: it speaks JSON-RPC, not
  REST, and the spec-coverage test documents the REST inventory only.
