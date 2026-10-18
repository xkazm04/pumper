// The SDK's types, in two halves.
//
// **Wire types are GENERATED, not mirrored.** Everything below that describes a
// payload Pumper serves is an alias into `./generated.ts`, which
// `openapi-typescript` emits from `clients/openapi.json` — the document the
// server's own router produces and a Rust test pins (`spec_snapshot_tests`).
// Regenerate both with `just clients`.
//
// This file used to hold the shapes by hand, with a comment asking whoever
// noticed a drift to please re-mirror them. That is the exact failure mode the
// fixture-conformance test in `crates/server/src/routes/datasets.rs` was
// written to catch, and a hand mirror can only ever be caught AFTER it is
// wrong. Now a renamed or dropped server field fails `npm run typecheck` here,
// because the alias no longer resolves.
//
// **The second half is the SDK's own contracts** — `WatermarkStore`, `SyncSink`,
// `MapContext`, `SyncResult` — which describe the boundary between this SDK and
// the product embedding it. Those are not wire shapes, no generator knows about
// them, and they stay hand-written on purpose.

import type { components } from "./generated.js";

/** Every component schema in the served OpenAPI document, by name. Exported so
 *  a consumer can reach a response shape this SDK does not wrap yet —
 *  `PumperSchemas["JobReceipt"]`, `PumperSchemas["EnforcementPreview"]` — with
 *  the same generated guarantee, instead of re-declaring it downstream. */
export type PumperSchemas = components["schemas"];

/** `Required` re-imposes what the wire actually does. utoipa renders a Rust
 *  `Option<T>` as a nullable, NON-required property, but every one of these
 *  fields is emitted by a `json!` literal, which writes `null` rather than
 *  omitting the key. So the generated type is looser than the server, and the
 *  aliases below tighten it back to `T | null` — present, possibly null — which
 *  is what the SDK has always promised and what its consumers branch on. */
type Wire<K extends keyof PumperSchemas> = Required<PumperSchemas[K]>;

/** A dataset address: `<app>/<name>` (e.g. `grants/unified`). */
export interface DatasetRef {
  app: string;
  name: string;
}

/** One stored record, as returned by `GET /datasets/{app}/{ds}` and `.../export`.
 *
 *  `data` is the one field the generator cannot type: it is the app's own
 *  canonical payload, free-form as far as the server is concerned, so the
 *  consumer supplies its shape as `T`. Every other field is pinned to the
 *  served schema. */
export type PumperRecord<T = unknown> = Omit<Wire<"RecordDto">, "data"> & {
  data: T;
};

/** The lifecycle transition a revision records. Distinct from core's
 *  `ChangeKind` (New|Changed|Unchanged) — the change *feed* also emits 'removed'
 *  and never emits 'unchanged'.
 *
 *  Narrower than the generated `change: string`: the server's Rust type is a
 *  plain `String` on the wire, so the spec cannot say more, but these three are
 *  the whole vocabulary and a consumer should be able to `switch` on them
 *  exhaustively. */
export type RevisionChange = "new" | "changed" | "removed";

/** One entry in the change feed (`GET /datasets/{app}/{ds}/changes`). Carries the
 *  full post-image in `data` for new/changed (null for removed), so a mirror
 *  applies the revision directly with no follow-up record read.
 *
 *  The four provenance fields (`job_id`, `source_url`, `artifact_sha`,
 *  `rules_hash`) are honest-Null: `null` means UNKNOWN, never a fabricated
 *  value, and the server never omits them. A real consumer reads them —
 *  pumper's own `peer` app mirrors a feed by carrying the ORIGIN's `source_url`
 *  and `rules_hash` through verbatim while deliberately dropping
 *  `artifact_sha`, which means "archived body on disk" and is a claim a mirror
 *  cannot make. A rename on the server side now breaks this file's compile
 *  rather than every mirror's provenance. */
export type PumperRevision<T = unknown> = Omit<
  Wire<"RevisionDto">,
  "data" | "change"
> & {
  data: T | null;
  change: RevisionChange;
};

/** Direction of the change feed. `desc` (newest first) is the default and the
 *  historical shape; `asc` is a forward drain a mirror can checkpoint page by
 *  page. Anything else is a 400 from the server, never a silent fallback. */
export type FeedOrder = "asc" | "desc";

/** A keyset page of the change feed (cursor-mode response shape). */
export type RevisionPage<T = unknown> = Omit<Wire<"RevisionPageDto">, "items"> & {
  items: PumperRevision<T>[];
};

/** One row of the durable event log (N05), as `GET /events/log` returns it. */
export type PumperEvent<T = unknown> = Omit<Wire<"EventRecordDto">, "payload"> & {
  payload: T;
};

/** One page of `GET /events/log`. */
export type PumperEventPage<T = unknown> = Omit<Wire<"EventLogPage">, "events"> & {
  events: PumperEvent<T>[];
};

// --- The SDK's own contracts (not wire shapes; nothing generates these) -----

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
