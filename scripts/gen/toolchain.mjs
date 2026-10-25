// The generator's toolchain predicate, split out of generate-clients.mjs so a
// fixture suite can import it without running the generator.
//
// Exit codes follow CONTRIBUTING: 0 clean / 1 drift / 2 findings / 3 cannot
// check. A missing generator is "cannot check" (3), never drift (1) - `3 is not
// a pass`, but it must not read as a stale client either.

import fs from "node:fs";
import path from "node:path";

export const EXIT_CANNOT_CHECK = 3;

/** Where the openapi-typescript CLI entrypoint lives under `root`. */
export function openapiTypescriptCli(root) {
  return path.join(root, "clients/typescript/node_modules/openapi-typescript/bin/cli.js");
}

/** True when the TypeScript generator is installed (exists = the check can run). */
export function toolchainPresent(root, exists = fs.existsSync) {
  return exists(openapiTypescriptCli(root));
}

export const CANNOT_CHECK_MESSAGE =
  "CANNOT CHECK: openapi-typescript is not installed - run `npm --prefix clients/typescript ci` first";
