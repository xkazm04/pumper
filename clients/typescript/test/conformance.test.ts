// Conformance test pinning the sync contract this SDK consumes: record/
// revision wire shapes, the `trust=`/`removed=` query params the client sends
// on each route, and the change feed's new/changed/removed lifecycle.
//
// Fixtures under ./fixtures/*.json are hand-authored to match the CURRENT
// server shapes (`crates/core/src/datasets.rs::Record`/`Revision`, as read by
// `crates/server/src/routes/datasets.rs`). A companion Rust test —
// `crates/server/src/routes/datasets.rs::sdk_fixture_conformance_tests` —
// asserts the server's *actual* serialization has the same field set as these
// same fixture files, so a Rust-side rename/removal breaks that test and a
// TypeScript-side parser regression breaks this one. Neither test proves the
// two are wired together end-to-end over real HTTP (that would need form (a),
// a live server); this form only proves both sides agree on the *shape* of
// the fixtures, which is what actually drifted here (the `removed=` default
// flip, `trust=` gaining teeth on `/export`).
//
// N23 turned the shape-pinning half into a GENERATED-vs-SERVED check. The field
// lists below used to be hand-written here, which meant they described what
// somebody believed the server sent and could never catch a field the server
// ADDED. They are now read out of `clients/openapi.json` — the document the
// router generates, a Rust test pins to the live router, and `src/generated.ts`
// is generated from — so the whole chain is answerable to one artifact:
//   router → clients/openapi.json → src/generated.ts → src/types.ts → these
//   fixtures.

import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { PumperClient } from "../src/client.js";
import { createPumperSync, memoryWatermark } from "../src/index.js";
import type { PumperRecord, PumperRevision, RevisionPage, SyncSink } from "../src/types.js";

const here = dirname(fileURLToPath(import.meta.url));
const fixture = <T>(name: string): T =>
  JSON.parse(readFileSync(join(here, "fixtures", name), "utf8")) as T;

const record = () => fixture<PumperRecord<{ title: string; status: string; amount: number }>>("record.json");
const removedRecord = () =>
  fixture<PumperRecord<{ title: string; status: string; amount: number }>>("record-removed.json");
const revisionPage = () => fixture<RevisionPage<Record<string, unknown>>>("revision-page.json");

// ---- Shape pinning ---------------------------------------------------------

// The served OpenAPI document, found by walking up from wherever this test is
// running (`test/` from source, `dist-test/test/` after the pretest compile).
// This is the artifact `src/generated.ts` is generated FROM and that a Rust test
// pins to the live router, so reading it here closes the loop:
//   router  →  clients/openapi.json  →  src/generated.ts  →  these fixtures.
const specPath = (() => {
  let dir = here;
  for (let i = 0; i < 8; i += 1) {
    const candidate = join(dir, "openapi.json");
    if (existsSync(candidate)) return candidate;
    const up = dirname(dir);
    if (up === dir) break;
    dir = up;
  }
  // `just clients` reads this file, it cannot produce it; only the router can.
  throw new Error("clients/openapi.json not found — run `just openapi` (regenerates it from the router)");
})();

type SchemaNode = { properties?: Record<string, unknown>; allOf?: SchemaNode[]; $ref?: string };
type SpecDoc = { components: { schemas: Record<string, SchemaNode> } };

const specDoc = (): SpecDoc => JSON.parse(readFileSync(specPath, "utf8")) as SpecDoc;

/** The field names a component schema declares, resolved through `$ref` and
 *  `allOf` (the served spec composes e.g. `JobDetail` as `allOf: [$ref JobDto,
 *  {properties}]`, which has no direct `properties` at all). Pure. */
const resolveSchemaFields = (doc: SpecDoc, name: string): string[] => {
  const seen = new Set<string>();
  const fields = new Set<string>();
  const walk = (node: SchemaNode | undefined): void => {
    if (!node) return;
    if (node.$ref) {
      const ref = node.$ref.replace("#/components/schemas/", "");
      if (seen.has(ref)) return;
      seen.add(ref);
      walk(doc.components.schemas[ref]);
    }
    for (const k of Object.keys(node.properties ?? {})) fields.add(k);
    for (const part of node.allOf ?? []) walk(part);
  };
  const found = doc.components.schemas[name];
  assert.ok(found, `the served spec has no component schema '${name}'`);
  seen.add(name);
  walk(found);
  return [...fields];
};

/** The served fields `value` lacks — refusing a schema that resolves to ZERO
 *  fields. A coverage comparison over an empty field list passes for every
 *  fixture, so "covers every served field" would be a claim measured over
 *  nothing; that is a broken resolver or a hollowed schema, never a pass. */
const missingServedFields = (doc: SpecDoc, name: string, value: Record<string, unknown>): string[] => {
  const fields = resolveSchemaFields(doc, name);
  assert.ok(
    fields.length > 0,
    `schema '${name}' resolved to zero fields — a coverage check over nothing is not coverage`,
  );
  return fields.filter((k) => !(k in value));
};

/** Every property the SERVED schema declares must be present in the fixture.
 *
 *  This replaces a hand-written field list. A list written here can only ever
 *  describe what somebody believed the server sent; reading the spec makes the
 *  fixtures answerable to what it actually sends, so a field ADDED server-side
 *  fails here too — which the old list could never do. */
const assertFixtureCoversSchema = (name: string, value: Record<string, unknown>) => {
  const missing = missingServedFields(specDoc(), name, value);
  assert.deepEqual(missing, [], `${name} fixture is missing served fields: ${missing.join(", ")}`);
};

test("schema_coverage_over_zero_fields_fails_not_passes", () => {
  const doc: SpecDoc = {
    components: {
      schemas: {
        Empty: {},
        Base: { properties: { id: {}, state: {} } },
        Composed: { allOf: [{ $ref: "#/components/schemas/Base" }, { properties: { progress: {} } }] },
      },
    },
  };
  // The anti-pattern: `found.properties ?? {}` made an empty schema cover every fixture.
  assert.throws(() => missingServedFields(doc, "Empty", { anything: 1 }), /zero fields/);
  // Composition is followed rather than read as "no properties".
  assert.deepEqual(resolveSchemaFields(doc, "Composed").sort(), ["id", "progress", "state"]);
  assert.deepEqual(missingServedFields(doc, "Composed", { id: 1, state: "x" }), ["progress"]);
  // And against the real spec, the composed schema really resolves its parent's fields.
  assert.ok(resolveSchemaFields(specDoc(), "JobDetail").includes("progress"));
  assert.ok(resolveSchemaFields(specDoc(), "JobDetail").length > 1);
});

test("record fixture carries every field the served RecordDto schema declares", () => {
  const r = record();
  assertFixtureCoversSchema("RecordDto", r as unknown as Record<string, unknown>);
  assert.equal(typeof r.trust, "string");
  assert.equal(r.removed_at, null, "live record fixture must have removed_at: null");
});

test("revision fixtures carry every field the served RevisionDto schema declares", () => {
  for (const item of revisionPage().items) {
    assertFixtureCoversSchema("RevisionDto", item as unknown as Record<string, unknown>);
  }
  assertFixtureCoversSchema(
    "RevisionPageDto",
    revisionPage() as unknown as Record<string, unknown>,
  );
});

test("removed record fixture carries a non-null removed_at (tombstone shape)", () => {
  const r = removedRecord();
  assert.notEqual(r.removed_at, null);
  assert.equal(typeof r.removed_at, "string");
});

test("revision fixture covers both a data-carrying and a removed (data: null) revision", () => {
  const page = revisionPage();
  assert.equal(page.items.length, 2);
  const [changed, removed] = page.items as [PumperRevision, PumperRevision];
  assert.equal(changed.change, "changed");
  assert.notEqual(changed.data, null, "'changed' revisions carry the post-image");
  assert.equal(removed.change, "removed");
  assert.equal(removed.data, null, "'removed' revisions carry no data — SDK must not dereference it");
  for (const field of ["app", "dataset", "key", "revision", "change", "data", "diff", "created_at", "trust"]) {
    assert.ok(field in changed, `revision fixture missing '${field}'`);
  }
  assert.equal(typeof page.next_cursor, "string", "opaque cursor token, not parsed by the SDK");
});

test("revision fixture carries the four provenance fields the peer app mirrors", () => {
  // Consumer: pumper's own `peer` app (`crates/apps/peer`, `mirror_provenance`)
  // reads source_url/rules_hash/artifact_sha straight off these feed items to
  // stamp mirrored records. Until these fields were in the fixture they were
  // unpinned on BOTH sides — the Rust half asserts fixture ⊆ actual, so a field
  // the fixture omitted could be renamed server-side with every test green, and
  // every mirror would silently lose the origin's provenance. Do not prune.
  const page = revisionPage();
  const [changed, removed] = page.items as [PumperRevision, PumperRevision];
  for (const field of ["job_id", "source_url", "artifact_sha", "rules_hash"]) {
    assert.ok(field in changed, `revision fixture missing provenance field '${field}'`);
    assert.ok(field in removed, `tombstone revision missing provenance field '${field}'`);
  }
  // The server flattens Provenance with no skip-if-none, so an unknown field is
  // present-and-null, never absent. Both shapes must be modelled.
  assert.equal(typeof changed.source_url, "string");
  assert.equal(typeof changed.rules_hash, "string");
  assert.equal(typeof changed.artifact_sha, "string", "one item carries a real archived-body sha");
  assert.equal(removed.source_url, null, "and one models honest-Null (unknown) provenance");
  assert.equal(removed.artifact_sha, null);
});

// ---- Client wire contract: query params sent per route ---------------------

function fakeFetch(body: string, contentType: string): typeof fetch {
  return (async () =>
    new Response(body, { status: 200, headers: { "content-type": contentType } })) as unknown as typeof fetch;
}

test("exportRecords requests trust=all&removed=include (DECISION: mirror sync must see tombstones)", async () => {
  const ndjson = JSON.stringify(record()) + "\n" + JSON.stringify(removedRecord()) + "\n";
  let requestedUrl = "";
  const fetchSpy: typeof fetch = (async (url: string | URL) => {
    requestedUrl = String(url);
    return new Response(ndjson, { status: 200, headers: { "content-type": "application/x-ndjson" } });
  }) as unknown as typeof fetch;

  const client = new PumperClient({ baseUrl: "http://example.invalid:1", fetch: fetchSpy });
  const out: PumperRecord[] = [];
  for await (const rec of client.exportRecords({ app: "grants", name: "unified" })) out.push(rec);

  assert.equal(out.length, 2);
  assert.equal(out[1]?.removed_at, removedRecord().removed_at, "tombstone survives the stream");

  const q = new URL(requestedUrl).searchParams;
  assert.equal(q.get("format"), "ndjson");
  assert.equal(q.get("trust"), "all", "export must ask for every trust tier — records self-stamp");
  assert.equal(
    q.get("removed"),
    "include",
    "export must ask for tombstones explicitly — the server default flipped to exclude",
  );
});

test("changesPage requests trust=stable by default and forwards since/cursor/limit", async () => {
  let requestedUrl = "";
  const fetchSpy: typeof fetch = (async (url: string | URL) => {
    requestedUrl = String(url);
    return new Response(JSON.stringify(revisionPage()), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;

  const client = new PumperClient({ baseUrl: "http://example.invalid:1", fetch: fetchSpy });
  const page = await client.changesPage({ app: "grants", name: "unified" }, "2026-08-01T00:00:00Z", "");

  assert.equal(page.items.length, 2);
  assert.equal(page.next_cursor, revisionPage().next_cursor);

  const q = new URL(requestedUrl).searchParams;
  assert.equal(q.get("since"), "2026-08-01T00:00:00Z");
  assert.equal(q.get("cursor"), "");
  assert.equal(q.get("trust"), "stable", "incremental sync only applies what Pumper stands behind, by default");
});

// ---- End-to-end through PumperSync (fixture-driven) -------------------------

test("incremental sync applies a 'changed' revision as upsert and a 'removed' one as tombstone", async () => {
  // The fixture's own next_cursor is non-null (it pins the paging contract for
  // the wire-level test above); a single-page sync run must terminate, so this
  // mock serves it once with next_cursor cleared — otherwise PumperSync's
  // "keep paging until next_cursor is null" loop would spin forever against a
  // mock that always answers the same non-null cursor.
  const onePage = { ...revisionPage(), next_cursor: null };
  const fetchSpy: typeof fetch = (async () =>
    new Response(JSON.stringify(onePage), {
      status: 200,
      headers: { "content-type": "application/json" },
    })) as unknown as typeof fetch;

  const upserts: Array<{ key: string; data: unknown }> = [];
  const tombstones: string[] = [];
  const sink: SyncSink = {
    async upsert(records) {
      upserts.push(...records);
      return records.length;
    },
    async tombstone(keys) {
      tombstones.push(...keys);
      return keys.length;
    },
  };

  const watermark = memoryWatermark();
  const dataset = { app: "grants", name: "unified" };
  await watermark.set(dataset, "2026-07-01T00:00:00Z"); // pre-set so run() takes the incremental path

  const sync = createPumperSync({
    baseUrl: "http://example.invalid:1",
    fetch: fetchSpy,
    dataset,
    watermark,
    sink,
  });

  const result = await sync.run();

  assert.equal(result.mode, "incremental");
  assert.equal(upserts.length, 1, "only the 'changed' revision upserts");
  assert.equal(upserts[0]?.key, "ca-grants|GR-0001");
  assert.equal(tombstones.length, 1, "the 'removed' revision tombstones, not upserts");
  assert.equal(tombstones[0], "ca-grants|GR-0002");
  assert.equal(await watermark.get(dataset), result.watermark, "watermark persisted only after the sink commits");
});

// ---- Keyset watermark: the feed is a position, not a clock reading ---------
//
// `upsert_many` stamps every record in one 500-row chunk with ONE revision
// `created_at` (docs/features/datasets.md § Conventions), so a watermark that
// is a bare instant cannot name the boundary inside that chunk: resuming at
// `created_at > S` drops every unread sibling of S. These tests pin the
// forward keyset drain, its per-page checkpoint, and the legacy-value upgrade.

import { parseWatermark, formatWatermark, normalizeStamp } from "../src/watermark.js";
import { planPage } from "../src/sync.js";

interface FeedPageJson {
  items: PumperRevision[];
  next_cursor: string | null;
}

/** A fake server that answers the ascending feed by keyset cursor: each page
 *  is served to exactly the cursor that precedes it, so the SDK's resume path
 *  is what selects the page, not a call counter. */
function keysetFeed(startCursor: string, pages: FeedPageJson[]) {
  const byCursor = new Map<string, FeedPageJson>();
  let key = startCursor;
  for (const page of pages) {
    byCursor.set(key, page);
    if (page.next_cursor === null) break;
    key = page.next_cursor;
  }
  const requests: Array<{ cursor: string; order: string | null }> = [];
  const fetchImpl: typeof fetch = (async (url: string | URL) => {
    const q = new URL(String(url)).searchParams;
    const cursor = q.get("cursor") ?? "";
    requests.push({ cursor, order: q.get("order") });
    const page = byCursor.get(cursor) ?? { items: [], next_cursor: null };
    return new Response(JSON.stringify(page), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl, requests };
}

function recordingSink(throwOnCall?: number): SyncSink & { keys: string[]; calls: number } {
  const state = {
    keys: [] as string[],
    calls: 0,
    async upsert(records: Array<{ key: string; data: unknown }>) {
      state.calls += 1;
      if (state.calls === throwOnCall) throw new Error("sink is down");
      for (const r of records) if (!state.keys.includes(r.key)) state.keys.push(r.key);
      return records.length;
    },
    async tombstone(keys: string[]) {
      for (const k of keys) {
        const i = state.keys.indexOf(k);
        if (i >= 0) state.keys.splice(i, 1);
      }
      return keys.length;
    },
  };
  return state;
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

test("watermark token round-trips, and a legacy bare RFC3339 reads as position (stamp, 0)", () => {
  assert.deepEqual(parseWatermark("2026-08-01T00:00:00.123456Z|41"), {
    createdAt: "2026-08-01T00:00:00.123456Z",
    rowid: 41,
  });
  assert.equal(formatWatermark({ createdAt: "2026-08-01T00:00:00.123456Z", rowid: 41 }), "2026-08-01T00:00:00.123456Z|41");
  // The legacy form is the one every existing consumer store holds today. It
  // must widen to a position that RE-reads the boundary stamp (rowid 0), never
  // one that skips it — re-delivery is idempotent, loss is not.
  assert.deepEqual(parseWatermark("2026-08-01T00:00:00Z"), {
    createdAt: "2026-08-01T00:00:00.000000Z",
    rowid: 0,
  });
  assert.equal(parseWatermark(null), null);
  // Fixed-width micros, because the server compares these lexicographically.
  assert.equal(normalizeStamp("2026-08-01T00:00:00Z"), "2026-08-01T00:00:00.000000Z");
  assert.equal(normalizeStamp("2026-08-01T00:00:00.5Z"), "2026-08-01T00:00:00.500000Z");
  assert.equal(normalizeStamp("2026-08-01T02:00:00+02:00"), "2026-08-01T00:00:00.000000Z");
});

test("planPage applies the last revision per key and drops nothing silently", () => {
  const plan = planPage(
    [
      revision({ key: "a", data: { v: 1 } }),
      revision({ key: "a", data: { v: 2 } }),
      revision({ key: "b", change: "removed", data: null }),
    ],
    (raw: unknown) => raw,
  );
  assert.equal(plan.stalled, false);
  assert.equal(plan.anomalies, 0);
  assert.deepEqual(plan.upserts, [{ key: "a", data: { v: 2 } }]);
  assert.deepEqual(plan.tombstones, ["b"]);
});

test("planPage stalls on a 'changed' revision with a null post-image instead of consuming the key", () => {
  // The anti-pattern: `seen.add(key)` ran BEFORE the `data !== null` test, so a
  // null post-image consumed the key for the whole run AND advanced the
  // watermark past it — permanent, unsignalled divergence.
  const plan = planPage([revision({ key: "a" }), revision({ key: "bad", data: null })], (raw: unknown) => raw);
  assert.equal(plan.stalled, true, "the page must not be applied past the anomaly");
  assert.equal(plan.anomalies, 1, "and it must be counted, not swallowed");
  assert.deepEqual(plan.upserts, [], "nothing from the stalled page is applied");
  assert.deepEqual(plan.tombstones, []);
});

test("a 'changed' revision with data:null does not upsert, does not consume the key, and does not advance the watermark", async () => {
  const start = "2026-07-01T00:00:00.000000Z|0";
  const feed = keysetFeed(start, [
    { items: [revision({ key: "bad", data: null })], next_cursor: "2026-08-01T00:00:00.000000Z|9" },
  ]);
  const sink = recordingSink();
  const watermark = memoryWatermark();
  const dataset = { app: "grants", name: "unified" };
  await watermark.set(dataset, start);

  const result = await createPumperSync({
    baseUrl: "http://example.invalid:1",
    fetch: feed.fetch,
    dataset,
    watermark,
    sink,
    batchSize: 7,
  }).run();

  assert.deepEqual(sink.keys, [], "the key is never upserted");
  assert.equal(result.stalled, true);
  assert.equal(result.anomalies, 1);
  assert.equal(
    await watermark.get(dataset),
    start,
    "the watermark must not advance past a revision the SDK refused to apply",
  );
});

test("a mid-run sink failure checkpoints the pages that DID land, and the next run resumes there", async () => {
  const dataset = { app: "grants", name: "unified" };
  const start = "2026-07-01T00:00:00.000000Z|0";
  const page = (n: number, cursor: string | null): FeedPageJson => ({
    items: Array.from({ length: 7 }, (_, i) => revision({ key: `p${n}-k${i}` })),
    next_cursor: cursor,
  });
  const pages = [
    page(1, "2026-08-01T00:00:00.000000Z|7"),
    page(2, "2026-08-01T00:00:00.000000Z|14"),
    { items: Array.from({ length: 3 }, (_, i) => revision({ key: `p3-k${i}` })), next_cursor: null },
  ];
  const feed = keysetFeed(start, pages);
  const sink = recordingSink(2); // page 1 lands, page 2's upsert throws
  const watermark = memoryWatermark();
  await watermark.set(dataset, start);

  const cfg = {
    baseUrl: "http://example.invalid:1",
    fetch: feed.fetch,
    dataset,
    watermark,
    sink,
    batchSize: 7,
  };
  await assert.rejects(() => createPumperSync(cfg).run(), /sink is down/);
  assert.equal(
    await watermark.get(dataset),
    pages[0]!.next_cursor,
    "page 1 applied, so page 1's position is the durable watermark — today nothing is written at all",
  );
  assert.equal(sink.keys.length, 7);

  // Second run: pages 2-3 only, and the first page is never re-requested.
  feed.requests.length = 0;
  const second = await createPumperSync(cfg).run();
  assert.equal(second.mode, "incremental");
  assert.deepEqual(
    feed.requests.map((r) => r.cursor),
    [pages[0]!.next_cursor, pages[1]!.next_cursor],
    "the second run starts from the stored position, not from the top of the feed",
  );
  assert.equal(sink.keys.length, 17, "7 + 7 + 3 keys, each exactly once");
  assert.ok(
    feed.requests.every((r) => r.order === "asc"),
    "the drain is forward-ordered — that is what makes per-page checkpointing sound",
  );
});

test("a legacy bare RFC3339 watermark keeps working, takes the incremental path, and is upgraded in place", async () => {
  const dataset = { app: "grants", name: "unified" };
  const legacy = "2026-07-01T00:00:00Z";
  const feed = keysetFeed("2026-07-01T00:00:00.000000Z|0", [
    { items: [revision({ key: "a" })], next_cursor: "2026-08-01T00:00:00.000000Z|3" },
  ]);
  const sink = recordingSink();
  const watermark = memoryWatermark();
  await watermark.set(dataset, legacy);

  const result = await createPumperSync({
    baseUrl: "http://example.invalid:1",
    fetch: feed.fetch,
    dataset,
    watermark,
    sink,
    batchSize: 7,
  }).run();

  assert.equal(result.mode, "incremental", "a legacy value must NOT force a full re-snapshot");
  assert.deepEqual(sink.keys, ["a"]);
  const stored = await watermark.get(dataset);
  assert.match(stored ?? "", /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\|\d+$/, "upgraded in place to a position token");
});

// ---- The verifier: replay the feed the Rust store actually emitted ---------

test("replaying the Rust-emitted shared-stamp feed across two runs loses no stamp-sibling", async () => {
  // feed-shared-stamp.json is NOT hand-authored: it is written by
  // crates/core/tests/change_feed_keyset.rs::shared_stamp_feed_fixture_is_emitted_from_a_real_store
  // from a real Datasets store, two upsert_many chunks each sharing ONE
  // revision created_at, paged so the boundaries cut through a stamp group.
  // That is the shape a timestamp watermark cannot resume from.
  const f = fixture<{
    distinct_keys: number;
    dataset: { app: string; name: string };
    pages: FeedPageJson[];
  }>("feed-shared-stamp.json");

  const dataset = { app: f.dataset.app, name: f.dataset.name };
  const start = "2026-01-01T00:00:00.000000Z|0";
  const feed = keysetFeed(start, f.pages);
  const sink = recordingSink(2); // run 1 dies partway, as a real consumer does
  const watermark = memoryWatermark();
  await watermark.set(dataset, start);

  const cfg = {
    baseUrl: "http://example.invalid:1",
    fetch: feed.fetch,
    dataset,
    watermark,
    sink,
    batchSize: 7,
  };
  await assert.rejects(() => createPumperSync(cfg).run());
  await createPumperSync(cfg).run();

  assert.equal(
    sink.keys.length,
    f.distinct_keys,
    "every key in the real store's feed reaches the sink — a timestamp watermark " +
      "loses the siblings of the stamp the first run stopped inside",
  );
  const expected = new Set(f.pages.flatMap((p) => p.items.map((i) => i.key)));
  assert.equal(expected.size, f.distinct_keys);
  for (const k of expected) assert.ok(sink.keys.includes(k), `stamp-sibling '${k}' was lost`);
});

test("subscribe walks the log forward from a cursor and stops when the server says caught up", async () => {
  // Two pages: the first FILLS (next_after set), the second does not (null).
  // The anti-pattern this pins: a poller that treats "no events" as "keep
  // asking immediately" and spins, or one that stops at the first page and
  // silently never sees the rest of its backlog.
  const pages = [
    {
      count: 2,
      next_after: 2,
      latest_seq: 3,
      retained: 3,
      retention_days: 7,
      events: [
        { seq: 1, kind: "job.succeeded", app: "fake", subject_id: "j1", payload: {}, created_at: "2026-09-02T00:00:00Z" },
        { seq: 2, kind: "dataset.changed", app: "grants", subject_id: "unified", payload: {}, created_at: "2026-09-02T00:00:01Z" },
      ],
    },
    {
      count: 1,
      next_after: null,
      latest_seq: 3,
      retained: 3,
      retention_days: 7,
      events: [
        { seq: 3, kind: "external", app: "src", subject_id: "e1", payload: {}, created_at: "2026-09-02T00:00:02Z" },
      ],
    },
  ];
  const requested: string[] = [];
  let call = 0;
  const fetchSpy: typeof fetch = (async (url: string | URL) => {
    requested.push(String(url));
    const body = JSON.stringify(pages[Math.min(call++, pages.length - 1)]);
    return new Response(body, { status: 200, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;

  const client = new PumperClient({ baseUrl: "http://example.invalid:1", fetch: fetchSpy });
  const seen: number[] = [];
  for await (const ev of client.subscribe({ cursor: 0, kind: "job.succeeded" })) seen.push(ev.seq);

  assert.deepEqual(seen, [1, 2, 3], "every page is walked, ascending, exactly once");
  assert.equal(requested.length, 2, "a non-full page ends the walk — no spin");

  const first = new URL(requested[0]!);
  assert.equal(first.pathname, "/events/log", "the cursor surface, not the SSE stream");
  assert.equal(first.searchParams.get("after"), "0");
  assert.equal(first.searchParams.get("kind"), "job.succeeded", "filters are pushed to the server");
  assert.equal(
    new URL(requested[1]!).searchParams.get("after"),
    "2",
    "the second call resumes from the page's next_after, not from 0",
  );
});
