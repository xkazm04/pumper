// Canonical wire types + the SDK's consumer-facing contracts. These mirror the
// Rust shapes exported by pumper-core (`crates/core/src/datasets.rs`: Record,
// Revision, ChangeKind) so a consumer decodes Pumper's datasets without
// re-deriving them. Kept hand-written and small; regenerate against
// `GET /openapi.json` if the wire shapes ever drift.

/** A dataset address: `<app>/<name>` (e.g. `grants/unified`). */
export interface DatasetRef {
  app: string;
  name: string;
}

/** One stored record, as returned by `GET /datasets/{app}/{ds}` and `.../export`. */
export interface PumperRecord<T = unknown> {
  key: string;
  data: T;
  first_seen: string;
  last_seen: string;
  updated_at: string;
  /** Set once a full-snapshot sync stopped containing this key; else null. */
  removed_at: string | null;
  /** How much this record is stood behind: `stable`, `provisional` (written
   *  while its source was degrading) or `quarantined`. Always populated —
   *  server-side a stored `NULL` reads back as `stable`. */
  trust: string;
}

/** The lifecycle transition a revision records. Distinct from core's
 *  `ChangeKind` (New|Changed|Unchanged) — the change *feed* also emits 'removed'
 *  and never emits 'unchanged'. */
export type RevisionChange = "new" | "changed" | "removed";

/** One entry in the change feed (`GET /datasets/{app}/{ds}/changes`). Carries the
 *  full post-image in `data` for new/changed (null for removed), so a mirror
 *  applies the revision directly with no follow-up record read. */
export interface PumperRevision<T = unknown> {
  app: string;
  dataset: string;
  key: string;
  revision: number;
  change: RevisionChange;
  data: T | null;
  diff: Record<string, { from: unknown; to: unknown }> | null;
  created_at: string;
  /** Trust of the write that produced this revision. Present on every
   *  revision, pre-dating this drift check — kept for parity with
   *  `PumperRecord.trust`, and because `/changes?trust=` filters on exactly
   *  this value server-side. */
  trust: string;
  /** Derivation stamp of the write that produced this revision. Every field is
   *  honest-Null: `null` means UNKNOWN, never a fabricated value. The server
   *  flattens these onto the revision and never omits them, so all four keys
   *  are always present on the wire.
   *
   *  Pinned here because a real consumer reads them: pumper's own `peer` app
   *  (`crates/apps/peer`, `mirror_provenance`) mirrors a feed by carrying the
   *  ORIGIN's `source_url`/`rules_hash` through verbatim and deliberately
   *  dropping `artifact_sha` — the sha means "archived body on disk", which a
   *  mirror does not hold. A rename on either side silently breaks every
   *  mirror's provenance, which is why these are typed rather than ignored. */
  job_id: string | null;
  source_url: string | null;
  artifact_sha: string | null;
  rules_hash: string | null;
}

/** Direction of the change feed. `desc` (newest first) is the default and the
 *  historical shape; `asc` is a forward drain a mirror can checkpoint page by
 *  page. Anything else is a 400 from the server, never a silent fallback. */
export type FeedOrder = "asc" | "desc";

/** A keyset page of the change feed (cursor-mode response shape). */
export interface RevisionPage<T = unknown> {
  items: PumperRevision<T>[];
  next_cursor: string | null;
}

/** Where the SDK persists its per-dataset sync watermark. The product owns
 *  storage (a row, a KV entry, a file) — the SDK only reads/advances it.
 *
 *  The value is an OPAQUE POSITION in the change feed — `<created_at>|<rowid>`,
 *  the same tuple the feed pages by — not a date. Never parse it, compare it
 *  or derive one: a bare instant cannot name a boundary inside an
 *  `upsert_many` chunk, where up to 500 revisions share one `created_at`, and
 *  resuming from one skips the siblings the last run did not reach.
 *
 *  A value written by an earlier SDK version (a bare RFC3339 instant) is still
 *  accepted and is upgraded in place on the first run: no resync. */
export interface WatermarkStore {
  get(dataset: DatasetRef): Promise<string | null>;
  set(dataset: DatasetRef, watermark: string): Promise<void>;
}

/** The product's persistence boundary. The SDK hands it canonical records and
 *  removed keys; the product decides how they land (PGlite, Firestore, DuckDB,
 *  files). Returns the count actually written, purely for reporting. */
export interface SyncSink<T = unknown> {
  upsert(records: Array<{ key: string; data: T }>): Promise<number>;
  tombstone(keys: string[]): Promise<number>;
}

/** Context passed to the optional `map` so a product can massage a canonical
 *  record on the way in (rare — canonical datasets are already normalized). */
export interface MapContext {
  key: string;
  updatedAt: string;
  change: RevisionChange;
}

export type SyncMode = "snapshot" | "incremental";

/** Why a revision the run READ did not reach the sink.
 *
 *  Every field here used to be a silent `continue`. A run that discarded
 *  everything and a run with nothing to do both reported `{ upserted: 0 }`,
 *  which makes the commonest integration mistake — a `map` that returns null
 *  for every record — indistinguishable from success. Counting is the whole
 *  point: these are not errors, they are the denominator of `upserted`. */
export interface SyncSkipped {
  /** The consumer's `map` returned null for this revision. */
  droppedByMap: number;
  /** A non-`removed` revision carried `data: null`. The page STALLS on one of
   *  these (see `SyncResult.stalled`), so this is a named count of the same
   *  event `anomalies` counts, not a second population. */
  nullPostImage: number;
  /** A later revision for the same key in the SAME page replaced this one.
   *  Page-local by construction: the drain checkpoints per page, so a key
   *  repeated on a later page is re-applied idempotently rather than held
   *  back, and is not counted here. */
  supersededDuplicate: number;
}

export interface SyncResult {
  mode: SyncMode;
  upserted: number;
  tombstoned: number;
  /** The watermark persisted at the end of this run (null if the dataset was
   *  empty and nothing advanced it). Opaque — see `WatermarkStore`. */
  watermark: string | null;
  /** Feed items the SDK refused to apply (today: a non-`removed` revision with
   *  a null post-image). Previously these were dropped with no signal at all;
   *  a non-zero count means the mirror is behind on purpose, not quietly. */
  anomalies?: number;
  /** True when the drain stopped at an anomaly rather than reaching the end of
   *  the feed. The watermark is left BEFORE the offending revision, so the run
   *  is repeatable once the feed is fixed. */
  stalled?: boolean;
  /** Feed pages applied (incremental) or sink flushes performed (snapshot). */
  pagesRead?: number;
  /** Revisions/records the run READ. `revisionsSeen - upserted - tombstoned`
   *  is accounted for, item by item, in `skipped`. */
  revisionsSeen?: number;
  /** False only for `run({ dryRun: true })`: nothing reached the sink and the
   *  watermark was not advanced. Every other field is what the real run would
   *  have reported. */
  applied?: boolean;
  skipped?: SyncSkipped;
}

/** Options for one `run()`. Omitted entirely, a run behaves exactly as before. */
export interface SyncRunOptions {
  /** Walk the identical path with a counting no-op sink and no watermark
   *  write. The report is the real report — `applied` is the only field that
   *  differs — so a preview can be compared against, and trusted as, the run
   *  it previews.
   *
   *  The counts are what the SDK WOULD hand the sink. A sink that writes
   *  fewer rows than it is given (deduping, filtering) reports its own lower
   *  number on the real run; the dry run cannot know that. */
  dryRun?: boolean;
}

export interface SyncProgress {
  mode: SyncMode;
  upserted: number;
  tombstoned: number;
  /** 1-based index of the page/flush this callback reports. */
  page?: number;
  revisionsSeen?: number;
  /** Running totals, so a long backlog's drop rate is visible while it runs
   *  rather than only in the final report. */
  skipped?: SyncSkipped;
}

/** A read-only answer to "how far behind is this mirror, and is a still
 *  cursor health or lag?" — computed without touching the sink and without
 *  advancing the watermark. */
export interface SyncStatus {
  /** What `run()` would do next: `snapshot` when no watermark is stored. */
  mode: SyncMode;
  /** The stored position, verbatim and opaque. Null on a cold start. */
  watermark: string | null;
  /** `created_at` of the newest revision in the feed, or null if it is empty. */
  newestRevisionAt: string | null;
  /** Whole seconds between the watermark's stamp and the feed head, floored at
   *  0. Null when either side is unknown (cold start, or an empty feed). */
  lagSeconds: number | null;
  /** True only when the forward probe found nothing pending AND the head is
   *  not ahead of the watermark. A cold start is never up to date. */
  upToDate: boolean;
  /** A LOWER BOUND on pending revisions: how many one forward probe page
   *  returned (so it saturates at `batchSize`). Null on a cold start, where
   *  the pending population is the whole dataset and the feed cannot say. */
  pendingAtLeast: number | null;
}
