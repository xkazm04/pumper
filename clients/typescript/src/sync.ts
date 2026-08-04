// Watermark-driven incremental mirror. Cold start streams a filtered snapshot;
// every run after that pulls only the change-feed delta since the last
// watermark. Applies post-images straight from the feed (no follow-up reads),
// tombstones removed keys, and advances the watermark only after the sink
// commits — so a crash mid-run re-processes idempotently rather than skipping.

import { PumperClient, type PumperClientConfig } from "./client.js";
import type {
  DatasetRef,
  MapContext,
  SyncProgress,
  SyncResult,
  SyncSink,
  WatermarkStore,
} from "./types.js";

export interface PumperSyncConfig<TRaw = unknown, TOut = TRaw> extends PumperClientConfig {
  /** The canonical dataset to mirror — prefer a unified dataset (e.g.
   *  `{ app: "grants", name: "unified" }`) so the product never re-normalizes. */
  dataset: DatasetRef;
  /** Repeatable `<path>:<op>:<value>` predicates, ANDed and pushed into SQL, so
   *  a product mirrors only its slice (e.g. `["$.status:eq:open"]`). */
  filter?: string[];
  watermark: WatermarkStore;
  sink: SyncSink<TOut>;
  /** Optional per-record transform on the way into the sink. Return null to drop
   *  a record. Omit for a straight canonical mirror. */
  map?: (raw: TRaw, ctx: MapContext) => TOut | null;
  /** Flush the sink every N records (snapshot mode). Default 500. */
  batchSize?: number;
  onProgress?: (p: SyncProgress) => void;
  /** Stop a long snapshot stream. */
  signal?: AbortSignal;
}

/** Lexicographic max of two RFC3339 watermarks — valid because Pumper stamps
 *  fixed-width UTC micros, so string order == chronological order. */
function laterIso(a: string | null, b: string): string {
  return a === null || b > a ? b : a;
}

class PumperSync<TRaw, TOut> {
  private readonly client: PumperClient;
  private readonly batchSize: number;

  constructor(private readonly cfg: PumperSyncConfig<TRaw, TOut>) {
    this.client = new PumperClient(cfg);
    this.batchSize = cfg.batchSize ?? 500;
  }

  /** Run one sync cycle: snapshot on cold start (no watermark), else incremental. */
  async run(): Promise<SyncResult> {
    const since = await this.cfg.watermark.get(this.cfg.dataset);
    return since === null ? this.snapshot() : this.incremental(since);
  }

  private toOut(raw: TRaw, ctx: MapContext): TOut | null {
    return this.cfg.map ? this.cfg.map(raw, ctx) : (raw as unknown as TOut);
  }

  private async snapshot(): Promise<SyncResult> {
    const { dataset, filter, sink, onProgress } = this.cfg;
    let upserts: Array<{ key: string; data: TOut }> = [];
    let tombstones: string[] = [];
    let upserted = 0;
    let tombstoned = 0;
    let watermark: string | null = null;

    const flush = async () => {
      if (upserts.length) {
        upserted += await sink.upsert(upserts);
        upserts = [];
      }
      if (tombstones.length) {
        tombstoned += await sink.tombstone(tombstones);
        tombstones = [];
      }
      onProgress?.({ mode: "snapshot", upserted, tombstoned });
    };

    for await (const rec of this.client.exportRecords<TRaw>(dataset, filter, this.cfg.signal)) {
      watermark = laterIso(watermark, rec.updated_at);
      if (rec.removed_at) {
        tombstones.push(rec.key);
      } else {
        const out = this.toOut(rec.data, { key: rec.key, updatedAt: rec.updated_at, change: "changed" });
        if (out !== null) upserts.push({ key: rec.key, data: out });
      }
      if (upserts.length >= this.batchSize || tombstones.length >= this.batchSize) await flush();
    }
    await flush();

    if (watermark !== null) await this.cfg.watermark.set(dataset, watermark);
    return { mode: "snapshot", upserted, tombstoned, watermark };
  }

  private async incremental(since: string): Promise<SyncResult> {
    const { dataset, sink } = this.cfg;
    // The feed is newest-first, so the FIRST revision we see for a key is its
    // latest — track seen keys and skip older duplicates so the latest wins.
    const seen = new Set<string>();
    const upserts: Array<{ key: string; data: TOut }> = [];
    const tombstones: string[] = [];
    let watermark = since;
    let cursor = "";

    for (;;) {
      const page = await this.client.changesPage<TRaw>(dataset, since, cursor);
      for (const rev of page.items) {
        watermark = laterIso(watermark, rev.created_at);
        if (seen.has(rev.key)) continue;
        seen.add(rev.key);
        if (rev.change === "removed") {
          tombstones.push(rev.key);
        } else if (rev.data !== null) {
          const out = this.toOut(rev.data, { key: rev.key, updatedAt: rev.created_at, change: rev.change });
          if (out !== null) upserts.push({ key: rev.key, data: out });
        }
      }
      if (!page.next_cursor) break;
      cursor = page.next_cursor;
    }

    const upserted = upserts.length ? await sink.upsert(upserts) : 0;
    const tombstoned = tombstones.length ? await sink.tombstone(tombstones) : 0;
    this.cfg.onProgress?.({ mode: "incremental", upserted, tombstoned });

    await this.cfg.watermark.set(dataset, watermark);
    return { mode: "incremental", upserted, tombstoned, watermark };
  }
}

/** Build a mirror for one canonical dataset. Call `.run()` on a schedule (cron,
 *  a trigger, `POST /api/cron/...`); each call advances the watermark. */
export function createPumperSync<TRaw = unknown, TOut = TRaw>(
  cfg: PumperSyncConfig<TRaw, TOut>,
): { run: () => Promise<SyncResult> } {
  const sync = new PumperSync(cfg);
  return { run: () => sync.run() };
}
