# Pumper mesh

A fleet of pumper nodes that shares what it learned, verifiably, on a schedule.
One node names the peers it trusts, the streams it wants from each, and how
often; the scheduler turns that into ordinary jobs. Three streams travel:

| Stream | What moves | Applied by |
| --- | --- | --- |
| `datasets` | dataset revisions from the origin's change feed, plus a **ghost reconcile** against its live-set digest | `peer` app → local namespace app |
| `weather` | learned per-host tier pins, HTTP strikes and politeness penalties | `TierMemory` + the penalty snapshot |
| `recipes` | discovered JSON-API endpoints (API X-ray) as local **candidates** | `api_recipes`, always `validated: false` |

Still puller-only: a node **pulls**; nothing is ever pushed to it. That is the
whole security posture — no inbound port, no shared cluster secret, and a bundle
that does not verify is refused rather than merged.

## Node identity

Every node has an ed25519 keypair, minted on first use at `node.key` beside the
SQLite database (`[storage] database_path`), `0600` where the platform has such
a thing.

```
GET /node
{ "node_id": "3f1c…",            // 32 hex: first 16 bytes of SHA-256 over the public key
  "legacy_id": "a91f0c2b7d4e1188", // the pre-mesh id (a hash of the database path)
  "algo": "ed25519",
  "public_key": "9f3c…",          // 64 hex — this is what a peer pins
  "key_path": "data/node.key",
  "key_created": false }
```

`node_id` **is** the key fingerprint. The old value — a `DefaultHasher` of the
database path, explicitly "not a security boundary" — survives one release as
`legacy_id` so an operator can map pins they wrote down onto the new id.

A key file that exists but cannot be parsed is a **hard error**, never a silent
re-key: minting a new identity would change this node's id and make every peer
that pinned it reject every bundle it sends, a failure that would be diagnosed
nowhere near the key file.

## Signed bundles

```json
{ "schema": "pumper.host-weather/2",
  "node_id": "3f1c…",
  "generated_at": "2026-09-01T10:00:00Z",
  "legacy_id": "a91f0c2b7d4e1188",
  "payload": { "min_observations": 3, "entries": [ … ] },
  "sig": "…128 hex…" }
```

The signature covers a domain-separated join of schema, node id, timestamp and a
**recursively key-sorted** serialisation of the payload
(`app_peer::envelope::signing_bytes`). The sort is the module's own property, not
`serde_json`'s default, so enabling `preserve_order` anywhere in the tree cannot
make two nodes disagree about what bytes a payload is.

| Schema | Route | Notes |
| --- | --- | --- |
| `pumper.host-weather/2` | `GET /host-weather/export` | Signed. `?schema=1` still emits the legacy flat unsigned body, for rolling a fleet forward one node at a time. |
| `pumper.host-weather/1` | — | Legacy, unsigned, flat. Import-only. |
| `pumper.recipes/1` | `GET /recipes/export` | Signed. `?validated_only=true` (default) exports only recipes a local replay proved. |

### Trust policy

Both import routes (`POST /host-weather/import`, `POST /recipes/import`, both
dry-run by default) and every scheduled pull run the same rule:

1. **No `[[peer]]` rows configured** → the node is not in a mesh, and imports
   behave exactly as they did before: an operator curling a bundle in by hand is
   trusted, because the only way it got here was an admin call they made.
2. **A peer whose pinned `public_key` fingerprints to the bundle's `node_id`** →
   that peer's rule. Signature checked; `verified: true` on success.
3. **Anything else, once peers exist** → unsigned or unverifiable, accepted only
   if some peer row says `allow_unsigned = true`. An operator who pinned keys has
   said, by doing so, that anonymous bundles are unwelcome.

`verified` in every import response is the honest verdict: `false` for an
accepted-but-unsigned bundle, never borrowed from a check that did not happen.

Refusals are distinct, greppable reasons — `SchemaMismatch`, `UnsignedRefused`,
`NodeMismatch`, `BadKey`, `SignatureInvalid` — because "the bundle was bad" is
not an operable diagnosis.

## `[[peer]]` — the whole federation surface

```toml
[[peer]]
name = "vps"
url = "https://vps.example:8088"
public_key = "9f3c…"                  # 64 hex, from the peer's GET /node
pull = ["datasets:grants-gov/opportunities", "weather", "recipes"]
every = "15m"
allow_unsigned = false
max_penalty_secs = 60
api_key = "env:PUMPER_VPS_KEY"        # presented when the PEER runs [auth] keys
# namespace = "edge_grants"           # default peer_{remote app}
# enabled = false                     # keep the row, stop scheduling it
```

| Key | Default | Notes |
| --- | --- | --- |
| `name` | the URL's host | Schedule id suffix; changing it re-creates the schedule row. |
| `url` | — | Required, `http://` or `https://`. |
| `public_key` | — | Empty = not pinned; then `allow_unsigned` must be true or **load fails**. |
| `pull` | `[]` | `"weather"`, `"recipes"`, `"datasets:<app>/<dataset>"`. A typo is a load error, never a peer that silently never syncs. Wildcards are not implemented. |
| `every` | `15m` | `s`/`m`/`h` suffixes or bare seconds. Must divide an hour (below an hour) or a day (at or above one) — see below. |
| `allow_unsigned` | `false` | Accept unsigned/unverifiable bundles from this peer. |
| `max_penalty_secs` | `0` | Ceiling on an imported politeness penalty. `0` = no ceiling beyond core's own import cap; it can never *widen* what core allowed. |
| `api_key` | — | Sent as `x-pumper-key`. **Use `env:VAR_NAME`**: the schedule row and every job it enqueues are readable on `GET /schedules` and `GET /jobs/{id}`, so a literal key there is a key on every operator's console. |
| `namespace` | `peer_{remote app}` | Local app mirrored records land under. |
| `enabled` | `true` | `false` disables the schedules rather than deleting them, so the history survives. |

The whole block is **absent by default**. A config with no `[[peer]]` rows
behaves byte-for-byte as it did before the mesh existed.

### Intervals and cron

The scheduler's unit of work is a cron row, and an arbitrary interval does not
map onto one: "every 7 minutes" has no cron expression that keeps its period
across an hour boundary. So `every` must divide an hour (`5m`, `15m`, `30m`) or,
at or above an hour, divide a day (`1h`, `2h`, `6h`, `12h`, `24h`). Anything else
is a **load error**, not a schedule that quietly drifts — a peer pulling at
:07, :14, … :56, :00 is not what "every 7m" promised. Sub-minute intervals are
refused outright.

### Reconcile into schedules

At boot, each `[[peer]]` row becomes one schedule **per stream**, running the
`peer` app:

```
peer-vps-weather                  0 */15 * * * *
peer-vps-recipes                  0 */15 * * * *
peer-vps-datasets-grants-gov-opportunities   0 */15 * * * *
```

Every write is SQL-fenced on `managed_by = 'peer'`, exactly as the catalog
reconcile is fenced on `catalog`, so a hand-made schedule can never be rewritten
by a peer row. A peer-managed schedule the config no longer asks for is
**disabled, not deleted** — the row carries `last_run`, `skipped_count` and the
job history keyed on its id.

The pass runs at **boot only**: `[[peer]]` is process config and cannot change
without a restart, unlike `catalog/data-sources.toml`, which an operator may edit
while the server runs.

Because pulls are ordinary jobs, budgets, receipts, retries, dedup, cancel, the
`/jobs` surface, its SSE stream and the decision ledger all apply unchanged. The
trust for a pull travels in the job's own params (`public_key`,
`allow_unsigned`, `max_penalty_secs`), so an operator can reproduce a scheduled
pull exactly by POSTing the same params by hand.

### Why the `peer` app and not a `mesh-pull` job kind

The design left the choice open. One app with a `stream` param, because
everything a scheduled pull needs already exists for an app run and none of it
exists for a bespoke job kind — and the three streams share the parts that
actually matter (peer URL, trust policy, auth header, status record), so
splitting them would have duplicated the security-relevant half.

## Ghost reconcile

The gap this closes: the change feed carries `removed` revisions, so ordinary
tombstones replicate — but a record deleted **outright** on the origin (`DELETE
/datasets/{app}/{ds}/records/{key}`, retention pruning) emits no revision at all,
so no puller could ever learn of it and the mirror keeps serving it forever.

```
GET /datasets/{app}/{dataset}/manifest[?keys=true]
{ "app": "grants-gov", "dataset": "opportunities",
  "count": 1240, "live_count": 1238,
  "digest": "…sha256 hex over the sorted live keys…",
  "complete": true, "cap": 50000 }
```

The digest is over **keys only**, not values: it answers "is the mirror holding
records the origin no longer has". A value-sensitive digest would go red on every
ordinary update and make the pass cry wolf. It is order-independent (the keys are
sorted) and set-based (duplicates collapse).

After a `datasets` walk — never before, or "not pulled yet" would be diagnosed as
"ghost" on every run — the mirror compares digests. On a mismatch it asks for the
key list and tombstones the keys the origin no longer has, reporting
`ghosts_removed` and `ghost_keys`.

Four things **stop** the pass, because a reconcile acting on a bad premise
deletes live records, which is worse than the ghosts it exists to remove:

| Stop | Why |
| --- | --- |
| Digests match | Nothing to do; the key list is never even fetched. |
| Origin manifest `complete: false` | Past the 50 000-key walk cap, keys it did not list are indistinguishable from keys it does not have. |
| Diff would empty the mirror | The puller's existing rule. Delete the namespace explicitly if that is intended. |
| More than 5 000 ghosts in one pass | At that scale the honest diagnosis is divergence, not deletion. |
| Origin answers `404` for `/manifest` | A pre-mesh build. Reported as `reconciled: false` with the reason, not silently skipped. |

A reconcile failure never fails a pull that worked: the revisions landed, and
"the ghosts are still there" is a degraded state, not a lost one.

## Weather and recipe streams

**Weather.** The merge is core's `plan_weather_import` unchanged — raise-only,
count-weighted, never downgrading a locally-observed pin, capped at the import
severity ceiling. The mesh adds two things: the signature check before it, and
the per-peer `max_penalty_secs` ceiling after it. That ceiling is the blast
radius of a leaked peer key stated as a number: the worst a compromised peer can
do is slow this node down by that much.

> **Known gap.** An app has no handle on the LIVE governor (it is server state),
> so a *scheduled* weather import lands in tier memory and in the persisted
> penalty snapshot, and the in-process governor adopts it at the next restart.
> The manual `POST /host-weather/import?apply=true`, which runs inside the
> server, still raises the live governor immediately. The result of every
> scheduled weather pull carries this note rather than implying otherwise.

**Recipes.** Every imported recipe lands `validated: false`, whatever the origin
claimed. Validation is a claim about a replay from a particular egress IP against
a live host, so adopting a peer's verdict would let one node's luck — or one
node's compromise — pin a tier for the whole fleet. The origin's flag travels as
`validated_at_origin`: provenance, not permission. The local validator
(`[recipes]`, `docs/features/fetching.md` § API recipes) proves it here exactly
as it would prove a locally-discovered candidate. The origin's row id is dropped
too, so two nodes that independently discovered the same endpoint cannot collide
on a primary key.

## `GET /mesh`

```json
{ "node_id": "3f1c…",
  "peers": [{
    "name": "vps", "url": "https://vps.example:8088",
    "key_pinned": true, "allow_unsigned": false,
    "every": "15m", "every_secs": 900, "enabled": true,
    "streams": [{
      "stream": "weather", "schedule_id": "peer-vps-weather", "scheduled": true,
      "last_attempt_at": "2026-09-01T10:15:00Z",
      "last_success_at": "2026-09-01T10:15:00Z",
      "lag_secs": 42, "ok": true, "verified": true,
      "pulls": 96, "signature_failures": 0, "ghosts_removed": 0, "detail": null
    }]
  }],
  "totals": { "peers": 1, "streams": 3, "pulls": 288,
              "signature_failures": 0, "ghosts_removed": 4 } }
```

Joined from three places that already existed: `[[peer]]` (desired state), the
`schedules` table (what the scheduler made of it), and the `peer/mesh` dataset
(what the pulls recorded). Nothing has its own truth — a mesh whose status lived
in its own table could disagree with the jobs that actually ran, and the failure
mode of a sync feature is precisely a dashboard that says green while nothing has
moved for a week.

`last_success_at` moves **only** on a successful pull, so a peer that broke a
week ago reports a week of lag rather than a fresh timestamp from its most recent
failure. `lag_secs` is `null`, not `0`, when nothing has ever succeeded.

`/metrics` carries the same totals as `pumper_mesh_peers`,
`pumper_mesh_streams`, `pumper_mesh_pulls_total`,
`pumper_mesh_signature_failures_total` and `pumper_mesh_ghosts_removed_total` —
**emitted at zero** on a node with no peers, like the egress counters, so a
dashboard panel exists before the first peer is configured rather than appearing
the moment one is.

## Auth (`[auth] mode = "keys"`)

A pull presents `api_key` as `x-pumper-key`. Mint the key on the **peer** with a
`read`-only scope: a pull only ever GETs `/host-weather/export`,
`/recipes/export`, `/datasets/{app}/{ds}/manifest` and
`/datasets/{app}/{ds}/changes`. Nothing a pull does needs `enqueue` or `admin`,
and a mesh key that carried either would let any peer spend money on this node.

See [auth.md](auth.md) for scopes and key minting.

# Dataset peering — the `datasets` stream in detail

- **App**: `peer` (`crates/apps/peer/`). Run it like any app:
  `POST /apps/peer/jobs`.
- **Transport**: `GET {peer}/datasets/{app}/{dataset}/changes` on the origin —
  the same public change feed `@pumper/sync` consumes. The origin needs no
  peering code at all.
- **Writes**: mirrored records land under a local **namespace app**
  (`peer_{remote app}` by default), never under the origin's app name.

## Params

```json
{
  "url": "http://origin:8877",
  "datasets": ["hackernews/stories", "hackernews/comments"],
  "namespace": "peer_hackernews",
  "max_records": 500
}
```

| Param | Required | Default | Notes |
| --- | --- | --- | --- |
| `url` | yes | — | Origin base URL; must be `http://` or `https://`. |
| `datasets` | for `stream=datasets` | — | Remote feeds as `"app/dataset"`. Max 20 per run. Not required (and unused) for the bundle streams — which is why the params schema requires only `url` and `run` enforces the rest. |
| `namespace` | no | `peer_{remote app}` | 1–64 chars of `[A-Za-z0-9_-]`. **May not equal the remote app name** — a mirror must not write into a namespace a local app may own. |
| `max_records` | no | 500 (cap 5000) | Per-dataset revision budget for **this run**. A capped walk suspends and the next run resumes it; it is a pacing knob, never a data-loss one. |
| `stream` | no | `datasets` | `datasets` \| `weather` \| `recipes`. Absent means `datasets`, so every pre-mesh job and stored schedule keeps its exact meaning. |
| `reconcile` | no | `true` | Run the ghost reconcile after the walk. |
| `peer_name` | no | the URL's host | Label this run is recorded under in `peer/mesh` and on `GET /mesh`. |
| `public_key` / `allow_unsigned` / `max_penalty_secs` / `api_key` | no | — | Trust and credentials for the bundle streams; the scheduler copies them from the `[[peer]]` row. |

The feed is always requested in cursor mode (`cursor=` is sent even when empty,
which is what selects `{items, next_cursor}` paging) at the origin's default
`trust=stable`: a mirror replicates what the origin stands behind.

## Result

```json
{
  "peer": "http://origin:8877",
  "max_records": 500,
  "status": "ok",
  "datasets": [{
    "dataset": "hackernews/stories", "namespace": "peer_hackernews",
    "status": "ok", "pulled": 120, "new": 100, "changed": 20, "unchanged": 0,
    "skipped_older_revisions": 3, "skipped_malformed": 0,
    "origin_provenance_kept": 120, "origin_artifact_sha_dropped": 118,
    "tombstones_applied": 2, "tombstones_deferred": 0,
    "capped": false, "walk_resumed": false, "walk_completed": true,
    "since": "2026-08-10T09:00:00.000000Z", "note": null
  }],
  "index_datasets": [{ "app": "peer_hackernews", "dataset": "stories" }],
  "tombstones": "applied from the feed's 'removed' revisions"
}
```

**Run status.** `ok` only when every requested dataset came back clean;
`partial` the moment one errored or froze on drift. If **every** dataset
errored the job itself **fails** — a peer whose origin has been unreachable for
a week must not read as a wall of green in the job history.

**Per-dataset status**: `ok` · `not_modified` (origin answered 304 to the
stored ETag) · `drift` (items the walk could not read; resume point frozen —
see below) · `error`.

## Provenance of a mirrored record

A mirror must not claim it scraped the origin. Each applied revision is stamped:

| Field | Value | Why |
| --- | --- | --- |
| `job_id` | the **local** pulling job | The remote's job id means nothing against this node's `jobs` table. |
| `source_url` | the **origin's** own `source_url`, verbatim | That is where the content genuinely came from. Unknown upstream stays `null` — the feed URL is a transport detail, not a source. |
| `rules_hash` | the origin's, verbatim | A content-addressed ruleset identity is still true off-node. |
| `artifact_sha` | **dropped** | It means "sha256 of the archived body **on disk**", and this node holds no such body. Mirroring it would make the record claim it is replayable here when it provably is not. |

`origin_artifact_sha_dropped` reports the count so the drop is visible rather
than implied.

## State model — `peer/state`

One record per `(peer URL, remote dataset, namespace)`, keyed
`{url}|{app}/{dataset}|{namespace}`, so two namespaces mirroring one origin feed
keep independent cursors.

| Field | Meaning |
| --- | --- |
| `since` | `created_at` high-water mark of the last **cleanly completed** walk. Stored as the honest observed maximum; sent on the wire rewound by one microsecond (see below). |
| `walk` | A suspended mid-walk position: `{next_cursor, newest, seen[]}`. `seen` (the applied-key set) persists so an older revision fetched by a later run cannot overwrite newer state already applied. Capped at 20 000 keys; a walk too large to resume safely is abandoned **without** advancing `since`. |
| `pending_tombstones` | Removals a run refused to apply, retried on every later run. Capped at 10 000, oldest kept. |
| `etag` / `etag_since` | The origin's `ETag`, replayed as `If-None-Match` on the next **fresh** walk with the same `since`. A 304 ends that dataset's pull at zero transfer. |

### Why `since` is sent one microsecond early

The origin's feed predicate is strict (`created_at > since`), the feed is ordered
by `created_at`, and **a whole upsert-chunk shares one timestamp**. A mirror that
stored page 1's newest stamp and sent it verbatim would permanently exclude every
revision carrying that same stamp that was committed after the page was served.

Stored stamps are fixed-width RFC 3339 micros, so rewinding by exactly one
microsecond turns `> (t − 1µs)` into `>= t` — an exact inclusive boundary, not a
fuzzy safety window. The cost is bounded: the boundary chunk is re-fetched once
per run and re-applied idempotently (identical content upserts as `Unchanged`,
which writes no revision), and it disappears as soon as the origin writes a newer
stamp.

## Failure and refusal semantics

| Situation | Behavior |
| --- | --- |
| **Schema drift** — items the walk cannot read (`skipped_malformed > 0`) | The resume point is **frozen** and the walk does not complete. Nothing is lost; the same window is re-read every run until the shape is understood. Status `drift`, count in the note. A field rename upstream used to be silent, permanent, total data loss with a green run. |
| **Tombstones would empty the mirror** | Refused **and deferred**: the keys persist in `pending_tombstones` and are retried every run. `tombstones_deferred > 0` means the mirror has not converged. A feed replaying every removal must not be able to wipe a mirror silently; a refusal is "not yet", never "never". |
| **Corrupt stored cursor** | The origin answers **400** and the run **errors** (see `datasets.md` § Querying & export). It previously restarted at the newest revision with a 200 — for a mirror that is a livelock, not a reset. |
| **Origin too old to page** | A legacy `{changes:[…]}` body (the origin ignored `cursor=`) is a typed error, not a silent unpaginated pull. |

## What propagates downstream

Mirrored data behaves like local data. Each run declares `index_datasets` for
every `(namespace, dataset)` it wrote, which routes it through the same widening
seam `grants/unified` uses (`worker::run_indexed_apps`) — nothing in the worker
special-cases `peer`. Through it, a mirrored write or tombstone reaches:

- **watches** on `peer_{origin}/{dataset}` — `dataset.changed`, whose payload
  `app` is the **namespace** (the only app the records can be read back from),
  not `peer`;
- **dataset triggers** scoped to the namespace;
- **full-text search** indexing, and therefore **saved-search alerts**.

None of this happened before the run batch was widened past `job.app`: writes
landed under the namespace while every downstream mechanism looked at `peer`, so
a mirror could not be watched at all.

## Known gaps

- **No push.** A node pulls; nothing is ever pushed to it. Push federation is a
  separate, later item.
- **Scheduled weather imports do not reach the live governor** until the next
  restart — see § Weather and recipe streams. The manual import route does.
- **`[[peer]]` is boot-only.** Editing it needs a restart; there is no
  `POST /mesh/reconcile`.
- **No per-peer namespace allow-list beyond `pull`.** A peer row names exact
  datasets; there are no wildcards and no "this peer may only write these
  namespaces" rule separate from what it is asked to pull.
- **No artifact mirroring.** A mirrored record carries no `artifact_sha`, so it
  is provably not replayable on the mirror (see § Provenance).
- **No key rotation story.** Replacing `node.key` changes `node_id`, and every
  peer that pinned the old key must be updated by hand. There is no revocation
  list and no overlap window.
- **Tombstone and reconcile scale are unmeasured.** Both the empty-the-mirror
  guard and the reconcile list live keys (`record_count` then `list`), which is
  O(dataset) on any run that carries removals or finds a digest mismatch. The
  manifest walk is capped at 50 000 keys and says `complete: false` past it.
- **One direction, one hop.** No transitive mirror-of-a-mirror trust
  propagation — a mirrored record's `trust` is the origin's value carried
  through, not re-derived.
- **The two-node proof shares a process.** `crates/server/src/e2e/peer_mirror.rs`
  runs a real origin server and a real mirror over a real socket, but both live
  in one process on loopback: clock skew between nodes and network partitions
  mid-walk are out of its reach. Node identity is keyed by the key file path
  precisely so the two nodes there do not share a keypair.
