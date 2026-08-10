# Dataset peering

Mirror another pumper node's datasets into this one, over the origin's existing
revision feed. Puller only — no push, no bidirectional sync, no new server
surface on either side.

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
| `datasets` | yes | — | Remote feeds as `"app/dataset"`. Max 20 per run. |
| `namespace` | no | `peer_{remote app}` | 1–64 chars of `[A-Za-z0-9_-]`. **May not equal the remote app name** — a mirror must not write into a namespace a local app may own. |
| `max_records` | no | 500 (cap 5000) | Per-dataset revision budget for **this run**. A capped walk suspends and the next run resumes it; it is a pacing knob, never a data-loss one. |

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

- **No auth.** Beyond whatever the peer URL itself embeds. A non-local origin
  should sit behind the API-key story first — see `docs/deployment.md` § auth
  posture.
- **Hard deletes leave ghosts.** The feed carries `removed` revisions, so
  ordinary tombstones replicate. A record deleted *outright* on the origin
  (`DELETE /datasets/...`, retention pruning) emits no revision, so the mirror
  keeps serving it. There is no reconcile pass.
- **No scheduling of its own.** Runs are on-demand jobs; put one on
  `POST /schedules` for a cadence. A server-side `[[peer]]` config block is the
  documented next slice, not built.
- **Tombstone scale is unmeasured.** The empty-the-mirror guard lists live keys
  to make its decision (`record_count` then `list`), which is O(dataset) on any
  run that carries removals.
- **One direction, one hop.** No push, no bidirectional sync, no transitive
  mirror-of-a-mirror trust propagation — a mirrored record's `trust` is the
  origin's value carried through, not re-derived.
- **The two-node proof shares a process.** `crates/server/src/e2e/peer_mirror.rs`
  runs a real origin server and a real mirror over a real socket, but both live
  in one process on loopback: clock skew between nodes, network partitions
  mid-walk, and auth are out of its reach.
