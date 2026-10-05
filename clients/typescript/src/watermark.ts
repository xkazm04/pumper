// Ready-made WatermarkStore implementations, plus the watermark VALUE itself:
// a feed position, not a clock reading. Most products want a persistent store
// (a settings row, a Firestore doc, a KV entry) so the position survives
// restarts; `memoryWatermark` exists for tests and one-shot scripts.
//
// Why a position. Pumper's `upsert_many` commits in chunks of 500 and every
// record in one chunk gets ONE revision `created_at`
// (docs/features/datasets.md § Conventions). A watermark that is a bare
// instant therefore cannot name a boundary INSIDE a chunk: resuming at
// `created_at > S` skips every sibling of S the previous run had not read —
// up to 499 revisions, silently. The feed's own paging key is the tuple
// `(created_at, rowid)`, so the watermark is that tuple too.

import type { DatasetRef, WatermarkStore } from "./types.js";

/** A position in the change feed: the tuple the server's keyset pages by. */
export interface FeedPosition {
  /** Fixed-width RFC3339 UTC micros, exactly as the server stamps it. */
  createdAt: string;
  /** The feed's stable tiebreak inside one shared stamp. */
  rowid: number;
}

const RFC3339 = /^(\d{4}-\d{2}-\d{2})[Tt ](\d{2}:\d{2}:\d{2})(?:\.(\d+))?(?:(Z|z)|([+-]\d{2}:\d{2}))$/;
const TOKEN = /^(.+)\|(\d+)$/;

/** Widens any RFC3339 instant to the server's own fixed-width UTC micros.
 *  Required, not cosmetic: the server compares `created_at` LEXICOGRAPHICALLY
 *  in SQL, and `...:00Z` sorts AFTER `...:00.000000Z` because 'Z' > '.'. A
 *  legacy watermark handed over unwidened would therefore resume *past* the
 *  very instant it meant to resume at — the skip it was supposed to prevent. */
export function normalizeStamp(iso: string): string {
  const m = RFC3339.exec(iso.trim());
  if (m === null) throw new Error(`watermark '${iso}' is not an RFC3339 instant`);
  const [, date, time, frac, zulu, offset] = m;
  const micros = `${frac ?? ""}000000`.slice(0, 6);
  if (zulu !== undefined) return `${date}T${time}.${micros}Z`;
  const shifted = new Date(`${date}T${time}${offset}`);
  if (Number.isNaN(shifted.getTime())) throw new Error(`watermark '${iso}' is not a real instant`);
  return `${shifted.toISOString().slice(0, 19)}.${micros}Z`;
}

/** Reads a stored watermark into a feed position.
 *
 *  Accepts BOTH forms, which is what lets every consumer upgrade without a
 *  resync: the `<created_at>|<rowid>` token this SDK now writes, and the bare
 *  RFC3339 instant it used to write. A legacy value becomes `(stamp, 0)` —
 *  rowid 0 sits BELOW every real rowid, so the boundary stamp is re-read
 *  rather than skipped. Re-delivery is idempotent through the sink; loss is
 *  not recoverable from the watermark's side at all. */
export function parseWatermark(stored: string | null): FeedPosition | null {
  if (stored === null) return null;
  const token = TOKEN.exec(stored.trim());
  if (token !== null) {
    return { createdAt: normalizeStamp(token[1]!), rowid: Number(token[2]) };
  }
  return { createdAt: normalizeStamp(stored), rowid: 0 };
}

/** The stored form, which is byte-for-byte the `cursor` the feed accepts. */
export function formatWatermark(pos: FeedPosition): string {
  return `${pos.createdAt}|${pos.rowid}`;
}

function key(ds: DatasetRef): string {
  return `${ds.app}/${ds.name}`;
}

/** Non-persistent. Fine for tests and single-process one-shots; a restart forces
 *  a fresh snapshot. */
export function memoryWatermark(): WatermarkStore {
  const store = new Map<string, string>();
  return {
    async get(ds) {
      return store.get(key(ds)) ?? null;
    },
    async set(ds, watermark) {
      store.set(key(ds), watermark);
    },
  };
}

/** Adapt any key/value pair of async functions into a WatermarkStore — wrap a
 *  product's own settings table, Firestore doc, or KV without new schema.
 *  Example: `kvWatermark({ get: k => store.getMeta(k), set: (k,v) => store.setMeta(k,v) })`. */
export function kvWatermark(kv: {
  get(k: string): Promise<string | null>;
  set(k: string, v: string): Promise<void>;
}): WatermarkStore {
  return {
    get: (ds) => kv.get(`pumper:watermark:${key(ds)}`),
    set: (ds, watermark) => kv.set(`pumper:watermark:${key(ds)}`, watermark),
  };
}
