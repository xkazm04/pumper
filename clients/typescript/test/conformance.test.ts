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
  throw new Error("clients/openapi.json not found — run `just clients`");
})();

const schema = (name: string): { properties: Record<string, unknown> } => {
  const doc = JSON.parse(readFileSync(specPath, "utf8")) as {
    components: { schemas: Record<string, { properties?: Record<string, unknown> }> };
  };
  const found = doc.components.schemas[name];
  assert.ok(found, `the served spec has no component schema '${name}'`);
  return { properties: found.properties ?? {} };
};

/** Every property the SERVED schema declares must be present in the fixture.
 *
 *  This replaces a hand-written field list. A list written here can only ever
 *  describe what somebody believed the server sent; reading the spec makes the
 *  fixtures answerable to what it actually sends, so a field ADDED server-side
 *  fails here too — which the old list could never do. */
const assertFixtureCoversSchema = (name: string, value: Record<string, unknown>) => {
  const missing = Object.keys(schema(name).properties).filter((k) => !(k in value));
  assert.deepEqual(missing, [], `${name} fixture is missing served fields: ${missing.join(", ")}`);
};

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
