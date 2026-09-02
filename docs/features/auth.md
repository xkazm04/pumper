# Identity & tenancy — scoped API keys, per-principal budgets, audit ledger

Pumper's HTTP surface had **no identity concept at all** until this landed: the
only credential anywhere was the ingress HMAC, which authenticates a *webhook
sender*, not an operator. Every mutating route — including the ones that spend
real money through the Claude engine — answered anything that could reach the
listener.

**The default is still exactly that.** `[auth] mode = "open"` (the default, and
what an absent `[auth]` section means) behaves byte for byte as the server did
before: no credential is read, no table is consulted, nothing is throttled, and
every request resolves a synthetic `operator` principal carrying every scope and
no ceiling. Authentication exists only once an operator flips the key — the same
opt-in posture `[ingress]`, `[remote]` and `[mcp]` already take.

## Config

```toml
[auth]
mode = "open"                   # "open" (default) | "keys"
audit = true                    # audit every mutating request (default ON, both modes)
default_rate_limit_per_min = 0  # fallback throttle for keys with none of their own; 0 = none
```

`mode` is enforced on an **exact** match of `"keys"` (case- and
whitespace-insensitive). Any other value — including a typo like `"key"` — is
`open`. That direction is deliberate: `mode != "open"` as the predicate would
have let a one-letter slip lock an operator out of their own node.

## Principals

A principal is a caller identity: a name, a set of scopes, an optional daily
spend ceiling, an optional request throttle, and an `enabled` flag.

The key itself is **shown once**, at creation and at rotation — only its
SHA-256 digest is stored, exactly like an ingress source's signing secret, so no
later call can return it. Present it as either:

```
Authorization: Bearer <key>
x-pumper-key: <key>
```

`Authorization` wins when both are present.

### Scopes

| Scope | Grants |
| --- | --- |
| `admin` | everything, including the identity surface itself |
| `read` | every non-identity `GET`/`HEAD`/`OPTIONS` |
| `enqueue:<app>` | `POST /apps/<app>/jobs` — **and nothing else** |
| `enqueue:*` | the enqueue door for every app — and nothing else |

An `enqueue:` grant deliberately does **not** imply `read`: a key minted to run
one pipeline cannot also export every dataset on the node. A scope string
outside this vocabulary (`write`, `Admin`, `enqueue-*`) is refused at creation
with a `400` rather than becoming a key that silently authorizes nothing, and a
principal must carry at least one scope.

### What a route requires

The route → scope map is derived from the method and path alone, and it is
**closed by default**: a route this map has never heard of lands on `read` when
it only reads and `admin` when it mutates, so a route added by any other change
is guarded before anyone remembers this file exists.

| Route shape | Required |
| --- | --- |
| `GET /health`, `GET /metrics`, `GET /openapi.json` | nothing, in **both** modes |
| `POST /apps/{name}/jobs` | `enqueue:{name}` |
| `GET`/`HEAD`/`OPTIONS` on `/principals*`, `/audit` | `admin` |
| any other `GET`/`HEAD`/`OPTIONS` | `read` |
| any other mutation | `admin` |

The three public routes stay unauthenticated on purpose: a liveness probe that
needs a credential reports the credential's health, and a spec you cannot read
without a key cannot be used to obtain one.

### Refusals

Every refusal uses the service's standard `{"error", "code"}` envelope
([http-api.md](http-api.md)):

| Status | `code` | When |
| --- | --- | --- |
| 401 | `unauthorized` | no key presented, or the key is unknown |
| 403 | `forbidden` | the key is **disabled**, or lacks the route's scope |
| 429 | `rate_limited` | the principal's token bucket is empty |
| 402 | `budget_exhausted` | the principal's daily ceiling is already reached |

A disabled key is `403`, not `401`: it exists and was recognised, which is a
different fact for the caller than "who are you".

## Budgets and cost attribution

`budget_usd_per_day` is checked at the **enqueue door only** — the one that
spends. A read cannot exhaust a budget, and charging a ledger aggregate to every
`GET` would put it on the hot read path. The window is a trailing 24 hours.

Omitting `budget_usd_per_day` means **no ceiling**, the same convention
`budget_usd` uses on jobs, schedules and triggers — so `0`/negative is refused
with a `422` rather than reinterpreted as unlimited.

`cost_events.principal_id` is copied from the job row at the moment each engine
call is metered, so attribution needs no extra parameter threaded through the
metered seams. It is **NULL** for every row written before this feature, every
row written in `open` mode, and every job created by an internal producer (the
scheduler tick, a trigger hop, a retry) — those genuinely have no caller, and
`GET /principals/costs` reports them under `(unattributed)` rather than dropping
them, so the parts of the report sum to its total.

## Audit ledger

With `[auth] audit = true` (the default) every **mutating** request — accepted
or refused — appends one row to `audit_log`: `{principal_id, action, target, at,
detail}` where `action` is `"<METHOD> <path>"` and `detail` carries
`{status, mode, principal_name}`. Reads are not recorded: they are not mutations
and would drown the ledger the refusals live in.

`principal_id` is NULL when the request never resolved a stored identity —
including in `open` mode, where the operator is an identity this server
*invented*, not one it authenticated. Fabricating an id there would make an
unattributed node look attributed.

Auditing can never fail the request it records: a failed ledger write logs a
warning and the response is unchanged.

## API surface

| Route | Notes |
| --- | --- |
| `GET /principals` | `{count, mode, caller, principals}` — key digests are never listed; `caller` is who this request resolved as, with `synthetic: true` for the `open`-mode operator |
| `POST /principals` | `{name, scopes[], budget_usd_per_day?, rate_limit_per_min?}` ⇒ `201 {principal, key}` — **the key is shown once** |
| `POST /principals/{id}/disable` | `{id, enabled: false}`; 404 unknown |
| `POST /principals/{id}/rotate` | `{id, key}` — the new key, shown once; the previous one stops working immediately |
| `GET /audit?principal=&cursor=&limit=` | `{items, next_cursor}`, newest first, keyset-paged on the ledger's own row id |
| `GET /principals/costs?since=` | `{total_usd, by_principal}` |

All six require `admin`.

### Keys for mesh peers

A pumper node that pulls from this one ([mesh.md](mesh.md)) authenticates with
an ordinary principal key, presented as `x-pumper-key`. Mint it with **`read`
only**:

```
POST /principals {"name": "mesh-laptop", "scopes": ["read"]}
```

A pull only ever GETs `/host-weather/export`, `/recipes/export`,
`/datasets/{app}/{ds}/manifest` and `/datasets/{app}/{ds}/changes` — all reads.
Nothing a pull does needs `enqueue` or `admin`, and a mesh key carrying either
would let a peer create work, or spend money, on this node. Give each peer its
own key so `GET /audit?principal=` and `GET /principals/costs` can tell them
apart, and so revoking one does not lock out the fleet.

On the pulling side the key goes in `[[peer]] api_key`, and it should be
`env:VAR_NAME` rather than a literal: the schedule row and every job it enqueues
are readable on `GET /schedules` and `GET /jobs/{id}`.

## Bootstrapping

Creating a principal requires `admin`, and in `keys` mode there is no admin key
until one exists. **Create the first principal while `mode = "open"`, then flip
the key and restart.** There is deliberately no bootstrap escape hatch: an
"allow the first call" rule is indistinguishable from "allow any call after a
database reset".

## Data model

Migration `0041_principals.sql`:

- `principals (id, name, key_hash, scopes JSON, budget_usd_per_day, rate_limit_per_min, enabled, created_at)` — `key_hash` uniquely indexed
- `audit_log (id, principal_id, action, target, at, detail)`
- `jobs.principal_id` and `cost_events.principal_id`, both nullable

Additive throughout: legacy rows keep a NULL caller and are never re-attributed.

## Known gaps

- **`jobs.principal_id` is not yet written by the enqueue handler.** The column,
  the storage entry point (`Storage::enqueue_dedup_as`) and the request
  extension the door reads all exist and are tested; wiring the door itself
  touches `routes/jobs.rs`, which this change deliberately did not edit. Until
  that one call site changes, `cost_events.principal_id` stays NULL and
  `GET /principals/costs` reports everything as `(unattributed)`.
- `GET /costs?principal=` and a `by_principal` block on `GET /economics` are not
  wired; `GET /principals/costs` is the by-principal surface today.
- **MCP is not covered.** `GET /mcp` is merged inside the same layer stack, so
  in `keys` mode it requires a key like any other route, but there are no
  per-session keys and `[mcp] allow_enqueue` is still a global bit rather than a
  scope.
- No key expiry, no org/tenant grouping, no SDK support.
- The token bucket is **process-global and not persisted** (like the ingress
  one): a restart refills every bucket.
- Audit rows have no retention policy of their own; `audit_log` is not in
  `LEDGER_TABLES`.
