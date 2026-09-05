#!/usr/bin/env node
// A CycloneDX 1.6 SBOM for the pumper binary, generated from the committed
// Cargo.lock.
//
// WHY FROM THE LOCKFILE, AND WHY NO TOOL. `Cargo.lock` already IS the bill of
// materials: it names every crate that will be linked in, at the exact version
// that will be linked in, and — for everything from crates.io — the SHA-256 of
// the `.crate` file cargo verified before unpacking it. That is provenance data
// this repo already commits and already reviews on every Dependabot PR. Reading
// it needs no network, no registry query and no cargo invocation, which is what
// lets this run inside the same offline, dependency-free budget every other gate
// in scripts/ci/ runs under — and lets its fixture suite run on EVERY change
// rather than only on a release. A release-only instrument is first exercised on
// the day it matters.
//
// The output is byte-for-byte deterministic: components are sorted, and there is
// no `serialNumber` and no `metadata.timestamp`. Both are optional in CycloneDX,
// and both would make two SBOMs of the same lockfile differ — which would make
// "did the dependency set change" unanswerable by diff, and would make the SBOM
// attestation in .github/workflows/release.yml describe a moving target.
//
// Usage:
//   node scripts/ci/sbom.mjs                       # to stdout
//   node scripts/ci/sbom.mjs --out dist/x.cdx.json # to a file
//   node scripts/ci/sbom.mjs --summary             # counts, for a log line

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

export const CRATES_IO = 'registry+https://github.com/rust-lang/crates.io-index';

/**
 * Parse the subset of TOML that `Cargo.lock` is.
 *
 * Deliberately not a general TOML parser, and deliberately not a dependency:
 * Cargo.lock is machine-generated with a fixed shape — a scalar `version`, then
 * a sequence of `[[package]]` tables whose only fields are four quoted strings
 * and one array of quoted strings. Anything outside that shape (a `[[patch.*]]`
 * table, a field added by a future lockfile version) is skipped rather than
 * guessed at, and `packages` is what the caller gets.
 *
 * @returns {{lockVersion: number|null, packages: {name: string, version: string, source?: string, checksum?: string, dependencies: string[]}[]}}
 */
export function parseCargoLock(text) {
  const lines = text.split('\n');
  const packages = [];
  let lockVersion = null;
  let current = null; // the [[package]] being filled, or null outside one
  let arrayKey = null; // the key whose multi-line array we are inside

  const push = () => {
    if (current && current.name && current.version) packages.push(current);
    current = null;
  };

  for (const raw of lines) {
    const line = raw.trim();

    if (arrayKey) {
      if (line.startsWith(']')) {
        arrayKey = null;
        continue;
      }
      const m = /^"([^"]*)"/.exec(line);
      if (m && current) current[arrayKey].push(m[1]);
      continue;
    }

    if (line.startsWith('#') || line === '') continue;

    if (line.startsWith('[[') || line.startsWith('[')) {
      push();
      if (line === '[[package]]') current = { name: '', version: '', dependencies: [] };
      continue;
    }

    const kv = /^([A-Za-z_][A-Za-z0-9_-]*)\s*=\s*(.*)$/.exec(line);
    if (!kv) continue;
    const [, key, rest] = kv;

    if (!current) {
      if (key === 'version') lockVersion = Number(rest) || null;
      continue;
    }

    if (rest === '[') {
      current[key] = [];
      arrayKey = key;
      continue;
    }
    // Single-line array: `dependencies = ["a", "b"]`
    if (rest.startsWith('[')) {
      current[key] = [...rest.matchAll(/"([^"]*)"/g)].map((m) => m[1]);
      continue;
    }
    const str = /^"([^"]*)"/.exec(rest);
    if (str) current[key] = str[1];
  }
  push();

  return { lockVersion, packages };
}

/**
 * A lockfile dependency entry -> the package name it refers to.
 *
 * Cargo writes `"name"` when the name is unambiguous in the graph, and
 * `"name version"` or `"name version (source)"` when two versions of the same
 * crate coexist. Taking the first whitespace-delimited token is correct for all
 * three, because a crate name cannot contain a space.
 */
export function depName(entry) {
  return entry.split(' ')[0];
}

/** `pkg:cargo/serde@1.0.2` — the Package URL a crates.io crate is addressed by. */
export function purlFor(pkg) {
  return `pkg:cargo/${encodeURIComponent(pkg.name)}@${encodeURIComponent(pkg.version)}`;
}

/** The stable identity a component and every edge pointing at it share. */
export function bomRef(pkg) {
  return `${pkg.name}@${pkg.version}`;
}

/**
 * One CycloneDX component per lockfile package.
 *
 * A package with NO `source` is a member of this workspace (a path dependency):
 * it is this repo's own code at this repo's own commit, so it gets no `purl` —
 * a `pkg:cargo/...` URL asserts resolvability from a registry, and asserting
 * that for a crate that was never published is the kind of confident-looking
 * wrong answer an SBOM exists to prevent.
 */
export function toComponent(pkg) {
  const fromCratesIo = pkg.source === CRATES_IO;
  const component = {
    type: 'library',
    'bom-ref': bomRef(pkg),
    name: pkg.name,
    version: pkg.version,
  };
  if (fromCratesIo) component.purl = purlFor(pkg);
  if (pkg.checksum) component.hashes = [{ alg: 'SHA-256', content: pkg.checksum }];
  component.properties = [
    { name: 'cargo:source', value: pkg.source ?? 'workspace-member' },
  ];
  return component;
}

/** The workspace `version` from the root Cargo.toml, for the metadata component. */
export function workspaceVersion(cargoTomlText) {
  const m = /\[workspace\.package\][\s\S]*?version\s*=\s*"([^"]+)"/.exec(cargoTomlText);
  return m ? m[1] : '0.0.0';
}

/**
 * The whole document, as a pure function of the two file contents — so the
 * fixture suite can assert its shape without a checkout.
 */
export function buildSbom({ lockText, cargoTomlText }) {
  const { packages } = parseCargoLock(lockText);
  const sorted = [...packages].sort(
    (a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version)
  );

  // A name may resolve to two versions in one graph; when a dependency entry
  // names only the crate, there is by construction exactly one candidate.
  const byName = new Map();
  for (const p of sorted) {
    if (!byName.has(p.name)) byName.set(p.name, []);
    byName.get(p.name).push(p);
  }

  const refs = new Set(sorted.map(bomRef));
  const dependencies = sorted.map((p) => {
    const dependsOn = [];
    for (const entry of p.dependencies ?? []) {
      const parts = entry.split(' ');
      const name = parts[0];
      const version = parts[1];
      const candidates = byName.get(name) ?? [];
      const hit = version
        ? candidates.find((c) => c.version === version)
        : candidates.length === 1
          ? candidates[0]
          : undefined;
      // An edge we cannot resolve is DROPPED, never invented: a wrong edge in a
      // signed SBOM is worse than a missing one, because it is believed.
      if (hit && refs.has(bomRef(hit))) dependsOn.push(bomRef(hit));
    }
    return { ref: bomRef(p), dependsOn: [...new Set(dependsOn)].sort() };
  });

  const version = workspaceVersion(cargoTomlText);

  return {
    bomFormat: 'CycloneDX',
    specVersion: '1.6',
    version: 1,
    metadata: {
      component: {
        type: 'application',
        'bom-ref': `pumper@${version}`,
        name: 'pumper',
        version,
        description:
          'Local-first scraping service: one Rust binary exposing an HTTP API over a durable SQLite job queue',
      },
      tools: {
        components: [
          { type: 'application', name: 'pumper-sbom', version: '1', description: 'scripts/ci/sbom.mjs' },
        ],
      },
      properties: [
        { name: 'cargo:lockfile', value: 'Cargo.lock' },
        // Named so a reader knows what this document does and does not cover:
        // it is the LINKED dependency set, not the runtime environment, and it
        // says nothing about the Chrome or `claude` CLI the browser and claude
        // engines shell out to (docs/deployment.md, "Not containerized").
        { name: 'scope', value: 'cargo dependency graph only; host Chrome and claude CLI are out of scope' },
      ],
    },
    components: sorted.map(toComponent),
    dependencies,
  };
}

function main(argv) {
  const lockText = fs.readFileSync(path.join(REPO_ROOT, 'Cargo.lock'), 'utf8');
  const cargoTomlText = fs.readFileSync(path.join(REPO_ROOT, 'Cargo.toml'), 'utf8');
  const sbom = buildSbom({ lockText, cargoTomlText });

  if (argv.includes('--summary')) {
    const withHash = sbom.components.filter((c) => c.hashes).length;
    console.log(
      `sbom: ${sbom.components.length} components, ${withHash} with a SHA-256 from Cargo.lock, ` +
        `${sbom.components.length - withHash} local/workspace`
    );
    return 0;
  }

  const json = `${JSON.stringify(sbom, null, 2)}\n`;
  const outIdx = argv.indexOf('--out');
  if (outIdx !== -1 && argv[outIdx + 1]) {
    const out = path.resolve(REPO_ROOT, argv[outIdx + 1]);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    fs.writeFileSync(out, json);
    console.log(`sbom: wrote ${sbom.components.length} components to ${argv[outIdx + 1]}`);
  } else {
    process.stdout.write(json);
  }
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exit(main(process.argv.slice(2)));
}
