#!/usr/bin/env node
// Generates every client's wire types from clients/openapi.json.
//
// The document is the contract. It is produced by the server's own router,
// committed to the tree, and pinned to the router by a Rust test
// (`routes::spec_snapshot_tests::committed_spec_matches_the_router`) — so this
// script never needs a running server, a Rust toolchain, or a network, and can
// run in the same Node-only CI job that type-checks the SDK.
//
// Two emitters:
//   - TypeScript, via `openapi-typescript` (a devDependency of
//     clients/typescript, so the SDK job's existing `npm ci` installs it). Its
//     output lands in BOTH clients/typescript/src/generated.ts and
//     clients/cli/src/generated.ts — the CLI is its own npm package and must not
//     reach into the SDK's source tree.
//   - Python, by this file. `datamodel-code-generator` would be the obvious
//     tool and is deliberately not used: it is not installed on the development
//     box or in any CI job here, and a generator that cannot run is a generator
//     that stops being run. The subset of OpenAPI this document uses (objects,
//     arrays, $refs, untagged oneOf, the five scalar types) fits in the ~120
//     lines below with no dependency at all.
//
// Usage:
//   node scripts/gen/generate-clients.mjs          # write
//   node scripts/gen/generate-clients.mjs --check  # exit 1 on drift
//
// `--check` is the CI gate: it regenerates into memory and diffs against the
// committed output, so a spec change that was not regenerated fails the build
// instead of shipping clients built from last week's contract.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "../..");
const SPEC = path.join(ROOT, "clients/openapi.json");
const TS_TARGETS = [
  path.join(ROOT, "clients/typescript/src/generated.ts"),
  path.join(ROOT, "clients/cli/src/generated.ts"),
];
const PY_TARGET = path.join(ROOT, "clients/python/pumper_sync/generated.py");

const check = process.argv.includes("--check");

// ---------------------------------------------------------------------------
// TypeScript
// ---------------------------------------------------------------------------

function generateTypeScript() {
  // The package's JS entrypoint, run through this same node — NOT the shim in
  // `.bin`. On Windows that shim is a `.cmd`, which `execFileSync` refuses with
  // EINVAL unless a shell is spawned, and spawning a shell to run a generator is
  // how a path with a space becomes a mystery. This is one process, no shell,
  // identical on both CI legs.
  const cli = path.join(
    ROOT,
    "clients/typescript/node_modules/openapi-typescript/bin/cli.js",
  );
  if (!fs.existsSync(cli)) {
    throw new Error(
      "openapi-typescript is not installed — run `npm --prefix clients/typescript ci` first",
    );
  }
  const out = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "pumper-gen-")), "generated.ts");
  execFileSync(process.execPath, [cli, SPEC, "-o", out], {
    stdio: ["ignore", "ignore", "inherit"],
  });
  return fs.readFileSync(out, "utf8");
}

// ---------------------------------------------------------------------------
// Python
// ---------------------------------------------------------------------------

const PY_HEADER = `"""Wire types for the Pumper HTTP API — GENERATED, do not edit.

Emitted from clients/openapi.json by scripts/gen/generate-clients.mjs
(\`just clients\`). The spec is produced by the server's router and pinned to it
by a Rust test, so a rename over there lands here on the next regeneration and
CI fails if it does not.

Every shape is a functional-syntax TypedDict rather than the class syntax,
because the served document contains keys the class syntax cannot express: the
economics report is windowed under \`"7d"\` / \`"30d"\`, and job receipts carry a
\`"yield"\` block, which is a Python keyword.

Fields the server may omit are \`NotRequired\`. A field that is present-but-null
is typed \`Optional\` and stays required — the distinction is load-bearing here:
\`null\` is Pumper's honest-unknown, and a consumer that cannot tell it from an
absent key cannot tell "we do not know" from "we did not say".
"""

from __future__ import annotations

from typing import Any, NotRequired, Optional, TypedDict, Union

#: A payload the server does not type: a record's \`data\`, an app's \`params\`, a
#: plugin's self-declared manifest. Deliberately \`Any\` — narrowing it here would
#: be inventing a contract the server does not enforce.
Json = Any

`;

/** OpenAPI type node -> a quoted Python annotation. */
function pyType(node) {
  if (!node || typeof node !== "object") return "Json";
  if (node.$ref) return node.$ref.replace("#/components/schemas/", "");
  if (node.oneOf) {
    const arms = node.oneOf.map(pyType);
    const nonNull = arms.filter((a) => a !== "None");
    const uniq = [...new Set(nonNull)];
    const union = uniq.length === 1 ? uniq[0] : `Union[${uniq.join(", ")}]`;
    return arms.length !== nonNull.length ? `Optional[${union}]` : union;
  }
  if (node.allOf && node.allOf.length === 1) return pyType(node.allOf[0]);
  const t = Array.isArray(node.type) ? node.type.find((x) => x !== "null") : node.type;
  const nullable = Array.isArray(node.type) ? node.type.includes("null") : false;
  let base;
  switch (t) {
    case "string":
      base = "str";
      break;
    case "integer":
      base = "int";
      break;
    case "number":
      base = "float";
      break;
    case "boolean":
      base = "bool";
      break;
    case "array":
      base = `list[${pyType(node.items)}]`;
      break;
    case "object":
      base = node.properties ? "dict[str, Json]" : "Json";
      break;
    default:
      base = "Json";
  }
  return nullable ? `Optional[${base}]` : base;
}

/** Make a Rust doc comment safe inside a `"""…"""` docstring: no stray
 *  backslash escapes, and no quote that could close the delimiter early. */
function pyDoc(doc) {
  return doc.replace(/\\/g, "\\\\").replace(/"/g, "'").replace(/'$/, "' ");
}

function generatePython(spec) {
  const schemas = spec.components?.schemas ?? {};
  const out = [PY_HEADER];
  const aliases = [];
  for (const name of Object.keys(schemas).sort()) {
    const node = schemas[name];
    const doc = (node.description ?? "").trim();
    // An untagged union (a dual-mode endpoint) has no properties of its own.
    if (node.oneOf || !node.properties) {
      aliases.push([name, pyType(node), doc]);
      continue;
    }
    const required = new Set(node.required ?? []);
    out.push(`${name} = TypedDict(\n    "${name}",\n    {\n`);
    for (const key of Object.keys(node.properties)) {
      const ann = pyType(node.properties[key]);
      const wrapped = required.has(key) ? ann : `NotRequired[${ann}]`;
      out.push(`        ${JSON.stringify(key)}: "${wrapped}",\n`);
    }
    out.push(`    },\n    total=True,\n)\n`);
    if (doc) out.push(`"""${pyDoc(doc)}"""\n`);
    out.push("\n");
  }
  // Aliases last: they may reference any TypedDict above.
  for (const [name, ann, doc] of aliases) {
    // EVERY line gets the comment marker. A description here is prose lifted
    // from a Rust doc comment and is routinely several lines long; commenting
    // only the first turns the rest into a syntax error.
    const lines = (doc || name).split("\n").map((l) => `#: ${l}`.trimEnd());
    out.push(`${lines.join("\n")}\n${name} = ${ann}\n\n`);
  }
  out.push("__all__ = [\n");
  for (const name of Object.keys(schemas).sort()) out.push(`    "${name}",\n`);
  out.push('    "Json",\n]\n');
  return out.join("");
}

// ---------------------------------------------------------------------------

function land(target, content) {
  const existing = fs.existsSync(target)
    ? fs.readFileSync(target, "utf8").replace(/\r\n/g, "\n")
    : null;
  const normalized = content.replace(/\r\n/g, "\n");
  if (existing === normalized) return false;
  if (check) {
    console.error(
      `DRIFT: ${path.relative(ROOT, target)} does not match what the spec generates.\n` +
        "  The OpenAPI document changed and the generated clients did not.\n" +
        "  Run `just clients` and commit the result.",
    );
    process.exitCode = 1;
    return true;
  }
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, normalized);
  console.log(`${existing === null ? "created" : "updated"} ${path.relative(ROOT, target)}`);
  return true;
}

if (!fs.existsSync(SPEC)) {
  console.error(
    "clients/openapi.json is missing. It is generated by the server's own router:\n" +
      "  UPDATE_OPENAPI=1 cargo test -p pumper-server --bin pumper spec_snapshot",
  );
  process.exit(2);
}

const spec = JSON.parse(fs.readFileSync(SPEC, "utf8"));
const ts = generateTypeScript();
let drifted = false;
for (const target of TS_TARGETS) drifted = land(target, ts) || drifted;
drifted = land(PY_TARGET, generatePython(spec)) || drifted;

if (check && !drifted) console.log("generated clients match the spec");
