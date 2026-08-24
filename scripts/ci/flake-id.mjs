// Stable test identity for a cargo workspace, plus the two readers built on it:
// a scanner over `#[ignore]` attributes in the source, and a parser over what
// libtest prints when a run happens.
//
// WHY AN IDENTITY MODULE EXISTS AT ALL
//
// Flake detection is a query over retained run history, and history keyed by an
// unstable name resets every time somebody tidies a file (registry:
// test-harness/flake-lifecycle, "detected by history, not by impression").
// libtest prints only the test's module path — `governor::tests::foo` — which is
// NOT unique in a workspace: the same module path exists in the lib target and
// in every integration-test binary that happens to declare it, and two crates
// can both have `tests::roundtrip`. Keying on that alone silently merges the
// histories of unrelated tests, which is worse than having none.
//
// THE ID
//
//     <package>::<target>::<module path>::<fn>
//     pumper-core::lib::governor::tests::distinct_hosts_run_in_parallel...
//     pumper-core::test:datasets_bulk_perf::bulk_upsert_50k_cost_report
//     pumper-server::bin:pumper::e2e::trigger_plugins::shipped_trigger_gate...
//
// `<target>` is cargo's own compilation unit — `lib`, `test:<name>`,
// `bin:<name>`, `bench:<name>`, `doctest` — because that is exactly the scope
// within which libtest's module path is unique.
//
// WHAT IT SURVIVES, AND WHAT IT DELIBERATELY DOES NOT
//
// Survives: reordering tests within a file; adding or removing sibling tests;
// moving a test between files that resolve to the same module path; renaming the
// file that backs `mod x` (the module path is what counts, not the file name);
// re-running on a different machine or OS.
//
// Does NOT survive: renaming the test function, renaming the package, or moving
// a test between targets. That is a choice, not an oversight — each of those is
// a redeclaration of what is being asserted, and carrying the old history
// forward would attribute one test's flakiness to another. The register catches
// the fallout instead: an entry whose id no longer resolves to a real test is a
// `flake:check` finding (an orphan), which forces a human to re-point it rather
// than letting the history quietly rot.
//
// Used by scripts/ci/flake-record.mjs (writes history), scripts/ci/flake-check.mjs
// (the gate) and scripts/ci/lane-certify.mjs (suite lanes).

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));

export function defaultRepoRoot() {
  return process.env.CLAUDE_PROJECT_DIR || path.resolve(HERE, '../..');
}

// --- the workspace's packages, and their target layout ----------------------

/**
 * Every crate directory under crates/, as { dir, name, binPaths, hasLib }.
 *
 * Read from Cargo.toml rather than assumed from the directory name: this repo's
 * directory `crates/core` is package `pumper-core` and `crates/apps/hackernews`
 * is `app-hackernews`, so directory-derived ids would be wrong for every crate
 * in the tree.
 */
export function packages(repoRoot = defaultRepoRoot()) {
  const out = [];
  const walk = (dir) => {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    if (entries.some((e) => e.isFile() && e.name === 'Cargo.toml')) {
      const manifest = fs.readFileSync(path.join(dir, 'Cargo.toml'), 'utf8');
      const name = manifest.match(/^\s*name\s*=\s*"([^"]+)"/m)?.[1];
      if (name) {
        out.push({
          dir: rel(repoRoot, dir),
          name,
          hasLib:
            fs.existsSync(path.join(dir, 'src/lib.rs')) || /^\s*\[lib\]/m.test(manifest),
          bins: binTargets(dir, manifest, name),
        });
      }
    }
    for (const e of entries) {
      if (e.isDirectory() && e.name !== 'target' && e.name !== 'src' && e.name !== 'tests') {
        walk(path.join(dir, e.name));
      }
    }
  };
  walk(path.join(repoRoot, 'crates'));
  return out;
}

/** Declared `[[bin]]` targets as { name, path }, plus the src/main.rs default. */
function binTargets(dir, manifest, pkgName) {
  const bins = [];
  for (const block of manifest.split(/^\s*\[\[bin\]\]\s*$/m).slice(1)) {
    const head = block.split(/^\s*\[/m)[0];
    const name = head.match(/^\s*name\s*=\s*"([^"]+)"/m)?.[1];
    const p = head.match(/^\s*path\s*=\s*"([^"]+)"/m)?.[1];
    if (name) bins.push({ name, path: p || `src/bin/${name}.rs` });
  }
  if (!bins.some((b) => b.path === 'src/main.rs') && fs.existsSync(path.join(dir, 'src/main.rs'))) {
    bins.push({ name: pkgName, path: 'src/main.rs' });
  }
  return bins;
}

function rel(root, p) {
  return path.relative(root, p).split(path.sep).join('/');
}

/**
 * The cargo target a repo-relative source file compiles into, as
 * { pkg, target } — or null when the file belongs to no package.
 *
 * Follows cargo's layout rules rather than a heuristic, because the target is
 * the half of the id that makes libtest's module path unique.
 */
export function targetForSource(relPath, pkgs) {
  const owner = pkgs
    .filter((p) => relPath === `${p.dir}/` || relPath.startsWith(`${p.dir}/`))
    // The deepest matching crate dir wins: crates/apps/foo is inside crates/,
    // and crates/ itself is not a package, but a nested workspace member could be.
    .sort((a, b) => b.dir.length - a.dir.length)[0];
  if (!owner) return null;
  const inner = relPath.slice(owner.dir.length + 1);
  const m = (re) => inner.match(re);
  // tests/<name>.rs is a target; tests/<name>/mod.rs is a shared helper module
  // included BY one, and compiles into whichever target declares `mod <name>`.
  let hit = m(/^tests\/([^/]+)\.rs$/);
  if (hit) return { pkg: owner.name, target: `test:${hit[1]}` };
  hit = m(/^benches\/([^/]+)\.rs$/);
  if (hit) return { pkg: owner.name, target: `bench:${hit[1]}` };
  hit = m(/^src\/bin\/([^/]+)\.rs$/);
  if (hit) return { pkg: owner.name, target: `bin:${hit[1]}` };
  if (inner.startsWith('src/')) {
    if (owner.hasLib) return { pkg: owner.name, target: 'lib' };
    // A binary-only crate: everything under src/ compiles into the bin whose
    // path roots it. `pumper-server` is exactly this shape — its whole e2e
    // suite lives in src/e2e/ and belongs to `bin:pumper`, not to a lib that
    // does not exist.
    const rooted = owner.bins.find((b) => b.path === 'src/main.rs');
    if (rooted) return { pkg: owner.name, target: `bin:${rooted.name}` };
  }
  return null;
}

/** The `Running <path>` line cargo prints, resolved back to { pkg, target }. */
export function targetForRunningPath(runPath, pkgs, repoRoot = defaultRepoRoot()) {
  const norm = runPath.split('\\').join('/');
  // Newer cargo prints the path relative to the workspace root; older cargo
  // prints it relative to the package. Try both, in that order.
  const direct = targetForSource(norm, pkgs);
  if (direct && fs.existsSync(path.join(repoRoot, norm))) return direct;
  const candidates = pkgs
    .map((p) => ({ p, full: `${p.dir}/${norm}` }))
    .filter(({ full }) => fs.existsSync(path.join(repoRoot, full)));
  if (candidates.length === 1) return targetForSource(candidates[0].full, pkgs);
  return null;
}

// --- reader 1: the `#[ignore]` attributes in the source ---------------------

/**
 * `#[ignore]` / `#[ignore = "reason"]` on one line, or null.
 *
 * Anchored at the start of the (trimmed) line so the dozen doc comments in this
 * repo that MENTION `#[ignore]` in prose are not read as quarantines — the same
 * trap crates/core/tests/fetch_chokepoint.rs documents for its own scanner.
 */
export function ignoreAttribute(line) {
  const m = line.trim().match(/^#\[ignore(?:\s*=\s*"((?:[^"\\]|\\.)*)")?\s*\]/);
  if (!m) return null;
  return { reason: m[1] === undefined ? null : m[1].replace(/\\(.)/g, '$1') };
}

/** The `fn NAME` an attribute applies to, searching forward over sibling attrs. */
export function fnAfter(lines, from) {
  for (let i = from; i < Math.min(lines.length, from + 12); i++) {
    const m = lines[i].match(/^\s*(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern\s+"[^"]*"\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)/);
    if (m) return m[1];
  }
  return null;
}

/**
 * Tracked Rust sources under crates/, repo-relative and forward-slashed.
 *
 * `git ls-files` rather than a directory walk, for the same reason
 * scripts/ci/ship-inventory.test.mjs uses it: an untracked scratch file must not
 * become a register obligation, and target/ must not be scanned.
 *
 * A failure here returns EMPTY rather than throwing, so the caller reaches its
 * own liveness assertion — a tree with nineteen `#[ignore]`s that scans to zero
 * is reported as CANNOT CHECK, not as a stack trace and not as a clean repo.
 */
export function rustSources(repoRoot = defaultRepoRoot()) {
  let out;
  try {
    out = execFileSync('git', ['ls-files', 'crates'], { cwd: repoRoot, encoding: 'utf8' });
  } catch {
    return [];
  }
  return out
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l.endsWith('.rs'));
}

/**
 * Every `#[ignore]`d test in the tree, as
 * { key, pkg, target, fn, file, line, reason }.
 *
 * `key` is `<pkg>::<target>::<fn>` — the id WITHOUT its module path, because a
 * line scanner cannot know the module nesting without a Rust parser and guessing
 * it would be worse than not claiming it. The module path lives in the register
 * entry's full id, and `registerKey()` below reduces one to the other so the two
 * directions still reconcile exactly.
 */
export function scanIgnoredTests(repoRoot = defaultRepoRoot(), pkgs = packages(repoRoot)) {
  const found = [];
  for (const file of rustSources(repoRoot)) {
    const target = targetForSource(file, pkgs);
    if (!target) continue;
    const text = fs.readFileSync(path.join(repoRoot, file), 'utf8');
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const attr = ignoreAttribute(lines[i]);
      if (!attr) continue;
      const name = fnAfter(lines, i + 1);
      if (!name) continue;
      found.push({
        key: `${target.pkg}::${target.target}::${name}`,
        pkg: target.pkg,
        target: target.target,
        fn: name,
        file,
        line: i + 1,
        reason: attr.reason,
      });
    }
  }
  return found;
}

/** The `<pkg>::<target>::<fn>` key a full register id reduces to. */
export function registerKey(id) {
  const parts = String(id).split('::').filter(Boolean);
  if (parts.length < 3) return null;
  return `${parts[0]}::${parts[1]}::${parts[parts.length - 1]}`;
}

// --- reader 2: what libtest prints when a run actually happens ---------------

export const OUTCOMES = ['ok', 'FAILED', 'ignored'];

/**
 * Parse a merged cargo-test transcript into { id, outcome } records.
 *
 * `text` must be stdout and stderr merged **in write order** — cargo prints the
 * `Running <target>` headers on stderr and libtest prints results on stdout, so
 * two separately-buffered pipes can hand them back interleaved wrongly and
 * attribute a whole binary's results to the previous target. flake-record.mjs
 * merges them at the OS level (one fd for both) for exactly this reason.
 *
 * A result line whose target could not be resolved is returned with
 * `target: null` and an id prefixed `unknown::` rather than silently dropped:
 * losing results is how a history quietly stops being a history.
 */
export function parseCargoTestOutput(text, pkgs, repoRoot = defaultRepoRoot()) {
  const records = [];
  let current = null;
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trimEnd();
    const running = line.match(/^\s*Running\s+(?:unittests\s+)?(\S+\.rs)\s+\(/);
    if (running) {
      current = targetForRunningPath(running[1], pkgs, repoRoot);
      continue;
    }
    const doctests = line.match(/^\s*Doc-tests\s+(\S+)/);
    if (doctests) {
      current = { pkg: doctests[1], target: 'doctest' };
      continue;
    }
    // `test <path> ... <outcome>`; the trailing `failures:` block re-lists names
    // with no `test ` prefix and no ` ... `, so it cannot double-count.
    const result = line.match(/^test\s+(\S.*?)\s+\.\.\.\s+(.+)$/);
    if (!result) continue;
    const name = result[1];
    const verdict = result[2].trim();
    let outcome;
    if (verdict === 'ok') outcome = 'ok';
    else if (verdict.startsWith('FAILED')) outcome = 'FAILED';
    else if (verdict.startsWith('ignored')) outcome = 'ignored';
    else continue; // `has been running for over 60 seconds`, bench output, etc.
    const prefix = current ? `${current.pkg}::${current.target}` : 'unknown::unknown';
    records.push({ id: `${prefix}::${name}`, outcome });
  }
  return records;
}
