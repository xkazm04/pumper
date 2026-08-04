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

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
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

test("record fixture has every field PumperRecord requires, including trust", () => {
  const r = record();
  for (const field of ["key", "data", "first_seen", "last_seen", "updated_at", "removed_at", "trust"]) {
    assert.ok(field in r, `record fixture missing '${field}'`);
  }
  assert.equal(typeof r.trust, "string");
  assert.equal(r.removed_at, null, "live record fixture must have removed_at: null");
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
