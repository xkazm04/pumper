// The three commands, as pure-ish functions over an `ApiOptions` and an argv
// tail, so each one is testable without a process, a terminal or a server.
//
// Every command prints through `out`, an injected sink, for the same reason:
// asserting on what a CLI printed is the only way to pin its contract, and a
// CLI that writes straight to `process.stdout` cannot be asserted on at all.

import { get, getLines, type ApiOptions, type Schemas } from "./api.js";

export type Out = (line: string) => void;

/** `--flag value` and `--flag=value`, plus bare positionals. Deliberately tiny:
 *  a CLI that ships a parser dependency to read three flags has bought a supply
 *  chain to save twenty lines. */
export function parseArgs(argv: string[]): { positional: string[]; flags: Map<string, string> } {
  const positional: string[] = [];
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i] as string;
    if (!arg.startsWith("--")) {
      positional.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    if (eq > 0) {
      flags.set(arg.slice(2, eq), arg.slice(eq + 1));
    } else {
      const next = argv[i + 1];
      if (next !== undefined && !next.startsWith("--")) {
        flags.set(arg.slice(2), next);
        i += 1;
      } else {
        flags.set(arg.slice(2), "true");
      }
    }
  }
  return { positional, flags };
}

function query(flags: Map<string, string>, keys: string[]): string {
  const q = new URLSearchParams();
  for (const key of keys) {
    const value = flags.get(key);
    if (value !== undefined) q.set(key, value);
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}

/** `pumper jobs ls [--app X] [--status queued] [--limit N] [--json]`
 *
 *  `GET /jobs` is dual-mode — a bare array without `cursor`, an envelope with
 *  it — so this normalizes both arms rather than guessing. That the union is
 *  now in the spec (`JobsResponse`) is what makes the narrowing below a typed
 *  operation instead of a hopeful cast. */
export async function jobsLs(opts: ApiOptions, argv: string[], out: Out): Promise<number> {
  const { flags } = parseArgs(argv);
  const url = `${opts.baseUrl}/jobs${query(flags, ["app", "status", "limit", "cursor"])}`;
  const body = await get<Schemas["JobsResponse"]>(url, opts);
  const jobs = Array.isArray(body) ? body : body.items;
  if (flags.get("json")) {
    out(JSON.stringify(jobs, null, 2));
    return 0;
  }
  if (jobs.length === 0) {
    // An empty queue and an over-narrow filter look identical in a table of
    // zero rows, so say which one this is.
    out("no jobs matched");
    return 0;
  }
  out(["ID", "APP", "STATUS", "ATTEMPTS", "CREATED"].join("\t"));
  for (const job of jobs) {
    out(
      [job.id, job.app, job.status, `${job.attempts}/${job.max_attempts}`, job.created_at].join(
        "\t",
      ),
    );
  }
  return 0;
}

/** `pumper datasets export <app> <dataset> [--filter F]… [--format ndjson|json|csv]`
 *
 *  Streams. A dataset export is the one call here with no bound on its size, so
 *  buffering it to pretty-print would be the difference between "works" and
 *  "works until the corpus grows". */
export async function datasetsExport(
  opts: ApiOptions,
  argv: string[],
  out: Out,
): Promise<number> {
  const { positional, flags } = parseArgs(argv);
  const [app, dataset] = positional;
  if (!app || !dataset) {
    out("usage: pumper datasets export <app> <dataset> [--filter F] [--trust all] [--format ndjson]");
    return 2;
  }
  const q = new URLSearchParams({ format: flags.get("format") ?? "ndjson" });
  for (const key of ["trust", "removed", "since"]) {
    const value = flags.get(key);
    if (value !== undefined) q.set(key, value);
  }
  // `--filter` is repeatable and each one is ANDed server-side.
  for (const f of argv.filter((a) => a.startsWith("--filter="))) q.append("filter", f.slice(9));
  const url =
    `${opts.baseUrl}/datasets/${encodeURIComponent(app)}/${encodeURIComponent(dataset)}` +
    `/export?${q.toString()}`;
  for await (const line of getLines(url, opts)) out(line);
  return 0;
}

/** `pumper triggers test <id> [--fire]`
 *
 *  Dry run by default. `--fire` really enqueues, which is why it is a flag and
 *  not a positional: the destructive form should be something you typed on
 *  purpose. */
export async function triggersTest(opts: ApiOptions, argv: string[], out: Out): Promise<number> {
  const { positional, flags } = parseArgs(argv);
  const [id] = positional;
  if (!id) {
    out("usage: pumper triggers test <trigger-id> [--fire]");
    return 2;
  }
  const fire = flags.get("fire") === "true" || flags.get("fire") === "1";
  const url = `${opts.baseUrl}/triggers/${encodeURIComponent(id)}/test${fire ? "?fire=true" : ""}`;
  const res = await fetch(url, {
    method: "POST",
    headers: opts.apiKey ? { authorization: `Bearer ${opts.apiKey}` } : {},
  });
  const body = (await res.json()) as Schemas["TriggerTestResponse"] & { code?: string };
  if (!res.ok) {
    out(`error ${res.status} ${body.code ?? ""}`.trim());
    return 1;
  }
  out(JSON.stringify(body, null, 2));
  // A dry run that would NOT fire is a legitimate answer, not a failure — it is
  // the answer you asked for. Exit 0 and let the caller read `would_fire`.
  return 0;
}

export const USAGE = `pumper — CLI for a running Pumper node

  pumper jobs ls [--app X] [--status queued|running|waiting|succeeded|failed|cancelled]
                 [--limit N] [--cursor C] [--json]
  pumper datasets export <app> <dataset> [--filter '$.status:eq:open']…
                 [--format ndjson|json|csv] [--trust all] [--removed include]
  pumper triggers test <trigger-id> [--fire]

Global:
  --url <base>     defaults to $PUMPER_URL, then http://127.0.0.1:8088
  --key <api-key>  defaults to $PUMPER_API_KEY (only needed when [auth] mode = keys)

Types come from clients/openapi.json, which the server generates and CI pins.
`;
