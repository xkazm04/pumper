// @pumper/sync — the shared consumer SDK for Pumper's canonical datasets.
// Consume unified datasets incrementally into any product store instead of
// hand-rolling an export→normalize→upsert loop per product. See ./README.md.

export { PumperClient, type PumperClientConfig } from "./client.js";
export { createPumperSync, type PumperSyncConfig } from "./sync.js";
export { memoryWatermark, kvWatermark } from "./watermark.js";
export { PumperHttpError, type HttpOptions } from "./http.js";
export type {
  DatasetRef,
  PumperRecord,
  PumperRevision,
  RevisionChange,
  RevisionPage,
  WatermarkStore,
  SyncSink,
  SyncMode,
  SyncResult,
  SyncProgress,
  MapContext,
} from "./types.js";
