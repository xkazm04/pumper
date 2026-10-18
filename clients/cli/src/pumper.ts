#!/usr/bin/env node
// Entry point. Argument routing only — every command's behaviour lives in
// `commands.ts` so it can be tested without spawning a process.

import { DEFAULT_BASE_URL, PumperCliError, type ApiOptions } from "./api.js";
import { datasetsExport, jobsLs, parseArgs, triggersTest, USAGE } from "./commands.js";

export async function main(argv: string[], out: (line: string) => void): Promise<number> {
  const { flags } = parseArgs(argv.filter((a) => a.startsWith("--")));
  const opts: ApiOptions = {
    baseUrl: (flags.get("url") ?? process.env.PUMPER_URL ?? DEFAULT_BASE_URL).replace(/\/+$/, ""),
    apiKey: flags.get("key") ?? process.env.PUMPER_API_KEY,
  };
  const words = argv.filter((a) => !a.startsWith("--"));
  const rest = argv.filter((a) => a !== words[0] && a !== words[1]);
  const command = `${words[0] ?? ""} ${words[1] ?? ""}`.trim();

  try {
    switch (command) {
      case "jobs ls":
        return await jobsLs(opts, rest, out);
      case "datasets export":
        return await datasetsExport(opts, rest, out);
      case "triggers test":
        return await triggersTest(opts, rest, out);
      default:
        out(USAGE);
        return words.length === 0 || flags.has("help") ? 0 : 2;
    }
  } catch (err) {
    if (err instanceof PumperCliError) {
      // The stable `code`, then the prose. A script wrapping this branches on
      // the first token; a human reads the rest.
      out(`error ${err.status} ${err.code ?? "unknown"}: ${err.message}`);
      return 1;
    }
    out(`error: ${err instanceof Error ? err.message : String(err)}`);
    return 1;
  }
}

// `import.meta.url` guard so the module can be imported by tests without
// running the CLI and calling `process.exit` out from under the test runner.
const invokedDirectly =
  process.argv[1] !== undefined && import.meta.url.endsWith(process.argv[1].replace(/\\/g, "/"));
if (invokedDirectly) {
  main(process.argv.slice(2), (line) => process.stdout.write(`${line}\n`)).then((code) => {
    process.exitCode = code;
  });
}
