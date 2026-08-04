// Ready-made WatermarkStore implementations. Most products want `sqlWatermark`
// (or a Firestore twin) so the watermark survives restarts; `memoryWatermark`
// exists for tests and one-shot scripts.

import type { DatasetRef, WatermarkStore } from "./types.js";

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
