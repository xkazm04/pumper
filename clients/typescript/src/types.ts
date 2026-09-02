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
