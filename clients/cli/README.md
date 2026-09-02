# @pumper/cli

The `pumper` command: drive a running node from a shell, with types generated
from the same OpenAPI document as every other client.

```
npm --prefix clients/cli ci && npm --prefix clients/cli run build
node clients/cli/dist/pumper.js jobs ls --status failed
```

## Commands

```
pumper jobs ls [--app X] [--status queued|running|waiting|succeeded|failed|cancelled]
               [--limit N] [--cursor C] [--json]
pumper datasets export <app> <dataset> [--filter '$.status:eq:open']…
               [--format ndjson|json|csv] [--trust all] [--removed include]
pumper triggers test <trigger-id> [--fire]
```

Global flags: `--url` (default `$PUMPER_URL`, then `http://127.0.0.1:8088`) and
`--key` (default `$PUMPER_API_KEY`, only needed when `[auth] mode = "keys"`).

`jobs ls` handles **both** shapes of `GET /jobs` — the bare array served without
`?cursor=` and the keyset envelope served with it. That union is a schema in the
spec (`JobsResponse`), so the narrowing is typed rather than hopeful.

`datasets export` streams: one row is in memory at a time, so exporting a corpus
is bounded by the terminal rather than by RAM.

`triggers test` is a **dry run** unless you pass `--fire`. A dry run that reports
`would_fire: false` exits 0 — that is the answer you asked for, not a failure.

## Errors

Refusals print `error <status> <code>: <message>`. Branch on `code`
(`not_found`, `budget_exhausted`, `rate_limited`, …); the message is prose and is
not a contract. Only `rate_limited`, `unavailable` and `bad_gateway` are retried.

## Why this is not part of `@pumper/sync`

`@pumper/sync` is a mirror — a watermark loop with a persistence boundary — and
a CLI importing it would inherit a contract it does not want. What the two share
is the artifact they are both typed from, `clients/openapi.json`, which is why
their shapes cannot disagree even though neither imports the other.
