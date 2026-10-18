// CLI contract tests. No process, no server: `fetch` is replaced and every
// command prints through an injected sink, so what the CLI RENDERS is
// assertable — which is the only part of a CLI anybody actually depends on.

import { test } from "node:test";
import assert from "node:assert/strict";

import { datasetsExport, jobsLs, parseArgs, triggersTest } from "./commands.js";
import type { ApiOptions } from "./api.js";

const opts: ApiOptions = { baseUrl: "http://test", retries: 0 };

function withFetch<T>(impl: typeof fetch, body: () => Promise<T>): Promise<T> {
  const original = globalThis.fetch;
  globalThis.fetch = impl;
  return body().finally(() => {
    globalThis.fetch = original;
  });
}

const json = (value: unknown, status = 200): typeof fetch =>
  (async () =>
    new Response(JSON.stringify(value), {
      status,
      headers: { "content-type": "application/json" },
    })) as unknown as typeof fetch;

const job = (id: string, status: string) => ({
  id,
  app: "grants-gov",
  params: {},
  status,
  attempts: 1,
  max_attempts: 3,
  priority: 0,
  callback_url: null,
  budget_usd: null,
  schedule_id: null,
  trigger_id: null,
  result: null,
  error: null,
  input_request: null,
  waiting_since: null,
  waiting_expires_at: null,
  executor_id: null,
  created_at: "2026-09-01T00:00:00Z",
  available_at: "2026-09-01T00:00:00Z",
  started_at: null,
  finished_at: null,
});

test("parseArgs handles --flag value, --flag=value and bare flags", () => {
  const { positional, flags } = parseArgs(["grants", "--app=x", "--status", "queued", "--json"]);
  assert.deepEqual(positional, ["grants"]);
  assert.equal(flags.get("app"), "x");
  assert.equal(flags.get("status"), "queued");
  assert.equal(flags.get("json"), "true");
});

test("jobs ls renders BOTH dual-mode shapes of GET /jobs", async () => {
  // The bare array is the legacy shape and is still served without `?cursor=`.
  // A CLI that only understood the envelope would print nothing at all against
  // a default call — silently, which is the failure this test exists for.
  const bare = await withFetch(json([job("a", "queued")]), async () => {
    const lines: string[] = [];
    await jobsLs(opts, [], (l) => lines.push(l));
    return lines;
  });
  assert.equal(bare.length, 2, "header + one row");
  assert.match(bare[1] ?? "", /^a\tgrants-gov\tqueued\t1\/3\t/);

  const paged = await withFetch(
    json({ items: [job("b", "running")], next_cursor: null }),
    async () => {
      const lines: string[] = [];
      await jobsLs(opts, ["--cursor="], (l) => lines.push(l));
      return lines;
    },
  );
  assert.match(paged[1] ?? "", /^b\tgrants-gov\trunning\t/);
});

test("jobs ls says 'no jobs matched' rather than printing an empty table", async () => {
  const lines = await withFetch(json([]), async () => {
    const out: string[] = [];
    await jobsLs(opts, ["--status", "failed"], (l) => out.push(l));
    return out;
  });
  assert.deepEqual(lines, ["no jobs matched"]);
});

test("jobs ls forwards its filters as query params", async () => {
  let seen = "";
  const spy: typeof fetch = (async (url: string | URL) => {
    seen = String(url);
    return new Response("[]", { status: 200, headers: { "content-type": "application/json" } });
  }) as unknown as typeof fetch;
  await withFetch(spy, async () => {
    await jobsLs(opts, ["--app", "grants-gov", "--status", "failed", "--limit", "5"], () => {});
  });
  assert.match(seen, /app=grants-gov/);
  assert.match(seen, /status=failed/);
  assert.match(seen, /limit=5/);
});

test("datasets export streams NDJSON line by line and forwards repeated --filter", async () => {
  let seen = "";
  const ndjson: typeof fetch = (async (url: string | URL) => {
    seen = String(url);
    return new Response('{"key":"a"}\n{"key":"b"}\n', {
      status: 200,
      headers: { "content-type": "application/x-ndjson" },
    });
  }) as unknown as typeof fetch;
  const lines = await withFetch(ndjson, async () => {
    const out: string[] = [];
    await datasetsExport(
      opts,
      ["grants", "unified", "--filter=$.status:eq:open", "--filter=$.state:eq:CA"],
      (l) => out.push(l),
    );
    return out;
  });
  assert.deepEqual(lines, ['{"key":"a"}', '{"key":"b"}']);
  assert.equal((seen.match(/filter=/g) ?? []).length, 2, "both filters are ANDed server-side");
});

test("datasets export refuses without app and dataset instead of guessing", async () => {
  const out: string[] = [];
  const code = await datasetsExport(opts, ["grants"], (l) => out.push(l));
  assert.equal(code, 2);
  assert.match(out[0] ?? "", /^usage:/);
});

test("triggers test dry-runs by default and only fires when told to", async () => {
  const urls: string[] = [];
  const spy: typeof fetch = (async (url: string | URL) => {
    urls.push(String(url));
    return new Response(JSON.stringify({ would_fire: false, reason: "no matching changes" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;
  await withFetch(spy, async () => {
    await triggersTest(opts, ["t1"], () => {});
    await triggersTest(opts, ["t1", "--fire"], () => {});
  });
  assert.ok(!urls[0]?.includes("fire"), "the default must not enqueue anything");
  assert.match(urls[1] ?? "", /fire=true/);
});

test("a dry run that would not fire is exit 0, not a failure", async () => {
  const code = await withFetch(json({ would_fire: false, reason: "cycle guard" }), () =>
    triggersTest(opts, ["t1"], () => {}),
  );
  assert.equal(code, 0, "'it would not fire' is the answer you asked for");
});
