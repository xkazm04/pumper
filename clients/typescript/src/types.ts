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
}

/** A keyset page of the change feed (cursor-mode response shape). */
export interface RevisionPage<T = unknown> {
  items: PumperRevision<T>[];
  next_cursor: string | null;
}

/** Where the SDK persists its per-dataset sync watermark. The product owns
 *  storage (a row, a KV entry, a file) — the SDK only reads/advances it. The
 *  value is an opaque RFC3339 timestamp; treat it as a token, not a date. */
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

export interface SyncResult {
  mode: SyncMode;
  upserted: number;
  tombstoned: number;
  /** The watermark persisted at the end of this run (null if the dataset was
   *  empty and nothing advanced it). */
  watermark: string | null;
}

export interface SyncProgress {
  mode: SyncMode;
  upserted: number;
  tombstoned: number;
}
