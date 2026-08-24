// The ship inventory, as a gate: every package manifest in this tree must be
// claimed by some CI job.
//
// `cargo test --workspace` quantifies over the workspace's `members` list, not
// over the checkout, and `npm test` runs in whatever directory you point it at.
// So a shipped artifact can sit in the tree, reachable from no gated root, and
// every green badge the repo displays was measured over a population that
// excluded it — with nothing anywhere reporting the omission (registry:
// test-harness/out-of-graph-artifacts).
//
// This repo has shipped exactly that: `clients/typescript` is a published
// consumer SDK (`@pumper/sync`) that no job compiled, and `.ai/manifest.yaml`
// advertised an `sdk-typecheck` capability nothing invoked. The plugins under
// `plugins-src/` are the same shape (detached wasm32 workspaces) and ARE
// claimed — by the `plugins-install` / `plugins-test` steps that drive the
// justfile.
//
// The check is an inventory walk, not a diff walk, because the failure it
// exists to catch is an artifact nobody ever tracked. Run by `just inventory`
// and by the `Ship inventory` CI job.

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

/** Tracked package manifests, repo-relative and forward-slashed. */
function manifests() {
  const out = execFileSync('git', ['ls-files'], { cwd: REPO_ROOT, encoding: 'utf8' });
  return out
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => /(^|\/)(Cargo\.toml|package\.json)$/.test(l))
    .filter((l) => !l.includes('node_modules/'));
}

/** The workspace `members` globs from the root Cargo.toml. */
function workspaceMemberGlobs() {
  const root = fs.readFileSync(path.join(REPO_ROOT, 'Cargo.toml'), 'utf8');
  const block = root.match(/members\s*=\s*\[([^\]]*)\]/s);
  assert.ok(block, 'the root Cargo.toml must declare workspace members');
  return [...block[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]);
}

function claimedByWorkspace(dir, globs) {
  return globs.some((g) =>
    g.endsWith('/*') ? path.posix.dirname(dir) === g.slice(0, -2) : dir === g
  );
}

/**
 * Text of every file that defines what CI runs. A manifest's directory named in
 * any of them is claimed — the justfile counts because CI drives it (the plugin
 * steps do exactly that).
 */
function jobDefinitions() {
  return ['.github/workflows/ci.yml', 'justfile']
    .map((f) => fs.readFileSync(path.join(REPO_ROOT, f), 'utf8'))
    .join('\n');
}

test('every_package_manifest_is_claimed_by_a_ci_job', () => {
  const globs = workspaceMemberGlobs();
  const jobs = jobDefinitions();
  const unclaimed = [];
  for (const manifest of manifests()) {
    const dir = path.posix.dirname(manifest);
    if (dir === '.') continue; // the workspace root itself
    if (claimedByWorkspace(dir, globs)) continue;
    // Named outright (`clients/typescript`), or swept by a loop over its parent
    // (`for dir in plugins-src/*/` — how CI builds every plugin crate).
    if (jobs.includes(dir)) continue;
    if (jobs.includes(`${path.posix.dirname(dir)}/*`)) continue;
    unclaimed.push(manifest);
  }
  assert.deepEqual(
    unclaimed,
    [],
    `these shipped manifests are reachable from NO gated root — add a job named ` +
      `after the artifact, or a workspace member line:\n  ${unclaimed.join('\n  ')}`
  );
});

test('the_inventory_walk_actually_found_the_tree', () => {
  // Liveness: an empty or tiny walk is the signature of a moved directory or a
  // broken filter, and would pass the assertion above for the wrong reason.
  const found = manifests();
  assert.ok(
    found.length > 20,
    `the manifest walk found only ${found.length} manifests — it is looking in the wrong place`
  );
  assert.ok(found.includes('clients/typescript/package.json'));
  assert.ok(found.includes('plugins-src/busyloop/Cargo.toml'));
});
