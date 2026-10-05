// Watermark-driven incremental mirror. Cold start streams a filtered snapshot;
// every run after that pulls only the change-feed delta since the last
// watermark. Applies post-images straight from the feed (no follow-up reads),
// tombstones removed keys, and advances the watermark only after the sink
// commits — so a crash mid-run re-processes idempotently rather than skipping.

import { PumperClient, type PumperClientConfig } from "./client.js";
import { formatWatermark, normalizeStamp, parseWatermark } from "./watermark.js";
import type {
  DatasetRef,
  MapContext,
  PumperRevision,
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
  /** Page size: the snapshot flushes the sink every N records, and the
   *  incremental drain requests N revisions per page and checkpoints after
   *  each one. Default 500. (Before the feed gained a forward order the
   *  incremental path ignored this and buffered the whole backlog — it had
   *  to: a watermark taken from a newest-first page jumps over the older
   *  pages not yet applied.) */
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

/** What one ascending feed page means for the sink, decided in memory before
 *  anything is written.
 *
 *  Two anti-patterns it exists to forbid, both of which were inline in the
 *  paging loop:
 *
 *  - `seen.add(key)` ran BEFORE the `data !== null` test, so a `changed`
 *    revision whose post-image was null consumed the key for the rest of the
 *    run AND let the watermark advance past it. The record was then neither
 *    written nor reachable again: permanent divergence, no error, no counter.
 *  - "first wins" dedupe, which is only correct on a newest-first feed. Going
 *    forward, the LAST revision for a key in the page is its latest.
 *
 *  A null post-image on a non-`removed` revision is a contradiction in the
 *  feed, so the page STALLS: nothing from it is applied, the caller does not
 *  checkpoint, and the anomaly is counted rather than swallowed. */
export function planPage<TRaw, TOut>(
  items: PumperRevision<TRaw>[],
  toOut: (raw: TRaw, ctx: MapContext) => TOut | null,
): { upserts: Array<{ key: string; data: TOut }>; tombstones: string[]; anomalies: number; stalled: boolean } {
  const latest = new Map<string, { kind: "upsert"; data: TOut } | { kind: "tombstone" } | { kind: "drop" }>();
  for (const rev of items) {
    if (rev.change === "removed") {
      latest.set(rev.key, { kind: "tombstone" });
      continue;
    }
    if (rev.data === null) {
      return { upserts: [], tombstones: [], anomalies: 1, stalled: true };
    }
    const out = toOut(rev.data, { key: rev.key, updatedAt: rev.created_at, change: rev.change });
    latest.set(rev.key, out === null ? { kind: "drop" } : { kind: "upsert", data: out });
  }
  const upserts: Array<{ key: string; data: TOut }> = [];
  const tombstones: string[] = [];
  for (const [key, verdict] of latest) {
    if (verdict.kind === "upsert") upserts.push({ key, data: verdict.data });
    else if (verdict.kind === "tombstone") tombstones.push(key);
  }
  return { upserts, tombstones, anomalies: 0, stalled: false };
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

    // A snapshot has no feed position to stand on, so it writes the oldest
    // position at or below the newest record stamp it saw: rowid 0, which
    // re-reads that stamp's whole chunk on the next incremental run instead
    // of skipping the siblings committed after this stream started.
    const position = watermark === null ? null : `${normalizeStamp(watermark)}|0`;
    if (position !== null) await this.cfg.watermark.set(dataset, position);
    return { mode: "snapshot", upserted, tombstoned, watermark: position, anomalies: 0, stalled: false };
  }

  /** Drain the feed FORWARD from the stored position, applying and
   *  checkpointing one page at a time.
   *
   *  Forward is what makes the checkpoint sound. On the newest-first feed the
   *  only safe strategy was to buffer everything and write one watermark at
   *  the end, so a consumer that could not finish a single run never made any
   *  progress at all. Oldest-first, the last row of each applied page is a
   *  position every earlier revision is behind, so a crash costs at most one
   *  page of re-delivery. */
  private async incremental(stored: string): Promise<SyncResult> {
    const { dataset, sink } = this.cfg;
    const start = parseWatermark(stored);
    let watermark = start === null ? stored : formatWatermark(start);
    let upserted = 0;
    let tombstoned = 0;
    let anomalies = 0;
    let stalled = false;

    for (;;) {
      // The stored watermark IS the cursor: one token, one meaning.
      const page = await this.client.changesPage<TRaw>(
        dataset,
        null,
        watermark,
        this.batchSize,
        "stable",
        "asc",
      );
      if (page.items.length === 0) break;

      const plan = planPage<TRaw, TOut>(page.items, (raw, ctx) => this.toOut(raw, ctx));
      anomalies += plan.anomalies;
      if (plan.stalled) {
        // Nothing from this page is applied and the watermark does not move
        // past the anomaly: a revision the SDK cannot interpret must not be
        // silently consumed. The run reports it instead of diverging.
        stalled = true;
        break;
      }
      if (plan.upserts.length) upserted += await sink.upsert(plan.upserts);
      if (plan.tombstones.length) tombstoned += await sink.tombstone(plan.tombstones);
      this.cfg.onProgress?.({ mode: "incremental", upserted, tombstoned });

      // Checkpoint only after the sink settled for THIS page.
      if (page.next_cursor === null) break;
      watermark = page.next_cursor;
      await this.cfg.watermark.set(dataset, watermark);
      if (page.items.length < this.batchSize) break;
    }

    // One final write even when nothing moved: it upgrades a legacy bare
    // RFC3339 value to a position in place (no resync for anyone already
    // mirroring), and it keeps `result.watermark` equal to what is actually
    // stored. Skipped on a stall, where the stored value must stay BEFORE the
    // revision the drain refused.
    if (!stalled) await this.cfg.watermark.set(dataset, watermark);

    return { mode: "incremental", upserted, tombstoned, watermark, anomalies, stalled };
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
