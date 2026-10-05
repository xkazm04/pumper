// What a sync run REPORTS, and what it can be asked without writing anything.
//
// The conformance suite pins what the mirror does to the sink. This one pins
// what the consumer can see: the counters behind an `upserted: 0`, a run that
// previews itself, and a read-only lag probe. The distinction these cases
// exist to force is the one the SDK could not make before — "nothing changed
// upstream" vs "everything was silently discarded" both returned
// `{ upserted: 0, tombstoned: 0 }`.
//
// Two of the seven cards' cases were already green when this file was written
// (wave 1 landed the keyset drain): a null post-image no longer vanishes, and
// `onProgress` already fires per page. Those are kept here as GUARDS — they
// assert behaviour this change must not regress — and are marked as such.

import { test } from "node:test";
import assert from "node:assert/strict";

import { createPumperSync } from "../src/index.js";
import type { PumperRevision, SyncProgress, SyncSink, WatermarkStore } from "../src/types.js";

interface FeedPageJson {
  items: PumperRevision[];
  next_cursor: string | null;
}

function revision(over: Partial<PumperRevision> = {}): PumperRevision {
  return {
    app: "grants",
    dataset: "unified",
    key: "k",
    revision: 1,
    change: "changed",
    data: { n: 1 },
    diff: null,
    created_at: "2026-08-01T00:00:00.000000Z",
    trust: "stable",
    job_id: null,
    source_url: null,
    artifact_sha: null,
    rules_hash: null,
    ...over,
  } as PumperRevision;
}

/** An ascending keyset feed: each page answers exactly the cursor that
 *  precedes it, so the SDK's own resume path selects the page. A `desc`
 *  request (what `status()` uses to find the head) is answered from
 *  `newest` instead, whatever the cursor. */
function feedServer(start: string, pages: FeedPageJson[], newest?: PumperRevision) {
  const byCursor = new Map<string, FeedPageJson>();
  let key = start;
  for (const page of pages) {
    byCursor.set(key, page);
    if (page.next_cursor === null) break;
    key = page.next_cursor;
  }
  const requests: Array<{ cursor: string; order: string | null; limit: string | null }> = [];
  const fetchImpl: typeof fetch = (async (url: string | URL) => {
    const q = new URL(String(url)).searchParams;
    const order = q.get("order");
    const cursor = q.get("cursor") ?? "";
    requests.push({ cursor, order, limit: q.get("limit") });
    const body: FeedPageJson =
      order === "desc"
        ? { items: newest === undefined ? [] : [newest], next_cursor: null }
        : (byCursor.get(cursor) ?? { items: [], next_cursor: null });
    return new Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl, requests };
}

function countingSink(): SyncSink & { upserts: Array<{ key: string; data: unknown }>; tombstones: string[]; calls: number } {
  const state = {
    upserts: [] as Array<{ key: string; data: unknown }>,
    tombstones: [] as string[],
    calls: 0,
    async upsert(records: Array<{ key: string; data: unknown }>) {
      state.calls += 1;
      state.upserts.push(...records);
      return records.length;
    },
    async tombstone(keys: string[]) {
      state.calls += 1;
      state.tombstones.push(...keys);
      return keys.length;
    },
  };
  return state;
}

function countingWatermark(initial: string | null): WatermarkStore & { sets: number } {
  let value = initial;
  const store = {
    sets: 0,
    async get() {
      return value;
    },
    async set(_ds: unknown, w: string) {
      store.sets += 1;
      value = w;
    },
  };
  return store as unknown as WatermarkStore & { sets: number };
}

const dataset = { app: "grants", name: "unified" };
const base = { baseUrl: "http://example.invalid:1", dataset };

// ---- status(): a surface that answers without writing ----------------------

test("status_not_a_run: no stored watermark reports snapshot mode and touches neither sink nor watermark", async () => {
  const feed = feedServer("", [], revision({ created_at: "2026-08-01T05:00:00.000000Z" }));
  const sink = countingSink();
  const watermark = countingWatermark(null);

  const status = await createPumperSync({ ...base, fetch: feed.fetch, sink, watermark }).status();

  assert.equal(status.mode, "snapshot");
  assert.equal(status.watermark, null);
  assert.equal(status.upToDate, false, "a cold start is never up to date — the whole dataset is pending");
  assert.equal(status.pendingAtLeast, null, "and how much is pending is unknowable before a snapshot");
  assert.equal(status.newestRevisionAt, "2026-08-01T05:00:00.000000Z");
  assert.equal(status.lagSeconds, null);
  assert.equal(sink.calls, 0, "status() must never write to the sink");
  assert.equal(watermark.sets, 0, "status() must never advance the watermark");
});

test("status_names_lag: head == watermark is lag 0 and upToDate; head an hour ahead is lag 3600", async () => {
  const stored = "2026-08-01T00:00:00.000000Z|12";
  const sink = countingSink();

  const atHead = feedServer(stored, [], revision({ created_at: "2026-08-01T00:00:00.000000Z" }));
  const level = await createPumperSync({
    ...base,
    fetch: atHead.fetch,
    sink,
    watermark: countingWatermark(stored),
  }).status();
  assert.equal(level.mode, "incremental");
  assert.equal(level.lagSeconds, 0);
  assert.equal(level.upToDate, true);
  assert.equal(level.watermark, stored, "status reports the stored position verbatim — it is opaque");

  const behind = feedServer(
    stored,
    [{ items: [revision({ key: "a" }), revision({ key: "b" })], next_cursor: null }],
    revision({ created_at: "2026-08-01T01:00:00.000000Z" }),
  );
  const lagging = await createPumperSync({
    ...base,
    fetch: behind.fetch,
    sink,
    watermark: countingWatermark(stored),
  }).status();
  assert.equal(lagging.lagSeconds, 3600);
  assert.equal(lagging.upToDate, false);
  assert.equal(lagging.pendingAtLeast, 2, "a lower bound, read from one forward probe page");
  assert.equal(sink.calls, 0);
});

// ---- The three uncounted discards -----------------------------------------

test("dropped_by_map_not_an_idle_feed: a mapper that returns null for everything is counted, not green", async () => {
  // The likeliest integration bug this SDK can produce: a wrong `map` yields
  // `{ upserted: 0 }`, byte-identical to a feed with nothing new in it.
  const start = "2026-07-01T00:00:00.000000Z|0";
  const feed = feedServer(start, [
    { items: [revision({ key: "a" }), revision({ key: "b" })], next_cursor: null },
  ]);
  const sink = countingSink();

  const result = await createPumperSync({
    ...base,
    fetch: feed.fetch,
    sink,
    watermark: memoryWatermark2(start),
    map: () => null,
  }).run();

  assert.equal(result.upserted, 0);
  assert.equal(result.skipped?.droppedByMap, 2, "both records were discarded by the mapper, and it says so");
  assert.equal(result.revisionsSeen, 2);
  assert.equal(sink.calls, 0);
});

test("null_post_image_is_named_in_the_report: GUARD on the stall wave 1 added, plus the counter it lacked", async () => {
  // GUARD (green before this change): wave 1's planPage already stalls the
  // page and leaves the watermark before the offending revision, so the record
  // is no longer permanently lost. This case must not regress.
  // NEW: the report names WHICH kind of skip it was.
  const start = "2026-07-01T00:00:00.000000Z|0";
  const feed = feedServer(start, [
    { items: [revision({ key: "bad", data: null })], next_cursor: "2026-08-01T00:00:00.000000Z|9" },
  ]);
  const sink = countingSink();
  const watermark = countingWatermark(start);

  const result = await createPumperSync({ ...base, fetch: feed.fetch, sink, watermark }).run();

  assert.equal(result.stalled, true, "GUARD: the drain stops rather than consuming the key");
  assert.equal(result.anomalies, 1, "GUARD: and the anomaly is counted");
  assert.equal(await watermark.get(dataset), start, "GUARD: the watermark stays before the anomaly");
  assert.equal(result.skipped?.nullPostImage, 1, "NEW: the report names the kind of skip");
  assert.equal(sink.calls, 0);
});

test("superseded_duplicate_is_counted_and_the_newest_post_image_wins", async () => {
  // Restated against wave 1's ASCENDING drain: within a page the LAST revision
  // for a key is the newest (the card was written against the newest-first
  // feed, where it was the first). Dedupe is page-local by construction — the
  // per-page checkpoint means an earlier page is already durable and gets
  // re-applied idempotently, not held back.
  const start = "2026-07-01T00:00:00.000000Z|0";
  const feed = feedServer(start, [
    {
      items: [revision({ key: "K", data: { v: 1 } }), revision({ key: "K", data: { v: 2 } })],
      next_cursor: null,
    },
  ]);
  const sink = countingSink();

  const result = await createPumperSync({
    ...base,
    fetch: feed.fetch,
    sink,
    watermark: memoryWatermark2(start),
  }).run();

  assert.equal(result.skipped?.supersededDuplicate, 1);
  assert.equal(result.upserted, 1);
  assert.deepEqual(sink.upserts, [{ key: "K", data: { v: 2 } }], "the newest post-image is the one applied");
  assert.equal(result.revisionsSeen, 2, "two revisions were read to produce one write");
});

// ---- dryRun ----------------------------------------------------------------

test("dry_run_writes_nothing_and_reports_exactly_what_the_real_run_reports", async () => {
  // A preview that computes its answer differently from the real run is a gate
  // watching a proxy. These two reports must be equal field for field.
  const start = "2026-07-01T00:00:00.000000Z|0";
  const pages = (): FeedPageJson[] => [
    {
      items: [
        revision({ key: "a" }),
        revision({ key: "b", change: "removed", data: null }),
        revision({ key: "c", data: { skip: true } }),
      ],
      next_cursor: null,
    },
  ];
  const map = (raw: unknown) => ((raw as { skip?: boolean }).skip ? null : raw);

  const realSink = countingSink();
  const realWm = countingWatermark(start);
  const real = await createPumperSync({
    ...base,
    fetch: feedServer(start, pages()).fetch,
    sink: realSink,
    watermark: realWm,
    map,
  }).run();

  const drySink = countingSink();
  const dryWm = countingWatermark(start);
  const dry = await createPumperSync({
    ...base,
    fetch: feedServer(start, pages()).fetch,
    sink: drySink,
    watermark: dryWm,
    map,
  }).run({ dryRun: true });

  assert.equal(drySink.calls, 0, "no upsert, no tombstone");
  assert.equal(dryWm.sets, 0, "and the watermark is not advanced");
  assert.equal(dry.applied, false);
  assert.equal(real.applied, true);
  assert.ok(realSink.calls > 0, "the real run really did write");
  assert.deepEqual(
    { ...dry, applied: undefined },
    { ...real, applied: undefined },
    "every count — including the watermark it WOULD have stored — matches the real run",
  );
});

// ---- Per-page visibility ---------------------------------------------------

test("per_page_progress_and_pagesRead: GUARD on one callback per page, plus the page count in the result", async () => {
  // GUARD (green before this change): wave 1's per-page drain already calls
  // onProgress once per applied page with a running total. Only `pagesRead`
  // and the per-page `skipped`/`page` fields are new.
  const start = "2026-07-01T00:00:00.000000Z|0";
  const page = (n: number, next: string | null): FeedPageJson => ({
    items: Array.from({ length: 4 }, (_, i) => revision({ key: `p${n}-k${i}` })),
    next_cursor: next,
  });
  const pages = [
    page(1, "2026-08-01T00:00:00.000000Z|4"),
    page(2, "2026-08-01T00:00:00.000000Z|8"),
    { items: [revision({ key: "p3-k0" })], next_cursor: null },
  ];
  const seen: SyncProgress[] = [];

  const result = await createPumperSync({
    ...base,
    fetch: feedServer(start, pages).fetch,
    sink: countingSink(),
    watermark: memoryWatermark2(start),
    batchSize: 4,
    onProgress: (p) => seen.push({ ...p }),
  }).run();

  assert.equal(seen.length, 3, "GUARD: one callback per page, not one at the end");
  for (let i = 1; i < seen.length; i += 1) {
    assert.ok(seen[i]!.upserted >= seen[i - 1]!.upserted, "GUARD: the running total never goes backwards");
  }
  assert.deepEqual(
    seen.map((p) => p.page),
    [1, 2, 3],
    "NEW: each callback names which page it is reporting",
  );
  assert.equal(result.pagesRead, 3, "NEW: the result says how many pages it read");
  assert.equal(result.upserted, 9);
});

/** A memoryWatermark pre-seeded with a position, so run() takes the incremental path. */
function memoryWatermark2(initial: string): WatermarkStore {
  let value: string | null = initial;
  return {
    async get() {
      return value;
    },
    async set(_ds, w) {
      value = w;
    },
  };
}
