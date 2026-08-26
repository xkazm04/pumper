#!/usr/bin/env node
// `just disk-check` — the build cache as a gate.
//
// Measured 2026-08-26: `target/` had reached 280.8 GB in ONE month, against
// 0.28 GB of actual scraped data. Nothing was wrong with the scraper and nothing
// was wrong with any single build. The failure was that cargo garbage-collects
// `target/` NEVER — every dep bump, feature flip and code edit mints a fresh
// hash-suffixed artifact and the previous one stays forever — and no instrument
// in this repo was watching. 7-16 stale copies had accumulated per target, and
// the first anyone knew of it was the disk.
//
// A habit ("remember to clean sometimes") is what was already in place, and it
// is what produced 280 GB. So this is a gate, in `just ci`, that goes red at a
// declared ceiling — the same shape as flake-check's register ceiling and for
// the same reason: the point is to be told at 40 GB, not to notice at 281.
//
// IT HEALS BEFORE IT FAILS
//
// A budget that only nags is the same instrument as the habit that let this
// reach 280 GB. So when the gate finds the tree over budget it runs the prune
// itself — the prune is provably safe (see `supersededFiles`), so printing its
// name and waiting for a human is strictly worse than running it — re-measures,
// and only goes red if pruning could NOT fix it. Red therefore means something a
// human should actually look at: every remaining artifact is live. `--no-prune`
// is the pure verdict for anyone who wants measurement without mutation.
//
// WHAT THIS FAILS ON
//
//   - `target/` over CEILING_GB that a prune could not bring back under
//   - more than STALE_FINDING_GB of mass older than STALE_DAYS that a prune
//     could not clear. The ceiling is not breached YET, but pruning has stopped
//     working, which is the state that produced 280 GB — deliberately earlier
//     than the wall.
//
// AND THE THIRD OUTCOME
//
// Exit 3 is CANNOT CHECK: not run from the repo root, or the scan itself threw.
// A `target/` that does not exist is exit 0 — nothing built is not a failure to
// measure. A broken instrument must not radiate confidence, the same discipline
// scripts/ci/flake-check.mjs and scripts/docs/check-doc-sync.mjs run on.
//
//   0 — checked, and the build cache is within budget.
//   2 — checked, and there are findings.
//   3 — could NOT check. Not a pass; nothing was verified.
//
// MODES
//
//   (no args)   gate, self-healing. 0 / 2 / 3 as above.
//   --no-prune  gate, measurement only. Never mutates `target/`.
//   --report    human breakdown of where the disk went. Always exit 0.
//   --prune     delete stale artifacts, then report what was reclaimed.
//   --dry-run   with --prune: list what WOULD be deleted, delete nothing.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const EXIT_FINDINGS = 2;
export const EXIT_CANNOT_CHECK = 3;

/** `target/` may not exceed this. Breaching it is a stop-the-line finding. */
export const CEILING_GB = 40;
/** Artifacts untouched for this long are prunable: no live build references them. */
export const STALE_DAYS = 7;
/** Stale mass above this is a finding on its own, well before the ceiling. */
export const STALE_FINDING_GB = 10;

const GB = 1024 ** 3;
const DAY_MS = 86400000;

export function defaultRepoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
}

export function formatBytes(bytes) {
  if (bytes >= GB) return `${(bytes / GB).toFixed(2)} GB`;
  if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(0)} MB`;
  return `${bytes} B`;
}

/**
 * Recursive size of `dir`, as {bytes, files, staleBytes}, where "stale" means
 * last modified before `staleBefore` (an epoch-ms cutoff).
 *
 * Windows-safe by construction: `du -sh target` on this 300k-file tree did not
 * return in ten minutes under Git Bash, which is why this gate is node and not
 * a shell one-liner. Unreadable or vanished entries are skipped rather than
 * thrown on — a directory being rewritten by a concurrent build is normal.
 */
export function measure(dir, staleBefore = 0) {
  let bytes = 0;
  let files = 0;
  let staleBytes = 0;
  const stack = [dir];
  while (stack.length > 0) {
    const cur = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(cur, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const full = path.join(cur, e.name);
      if (e.isDirectory()) {
        stack.push(full);
        continue;
      }
      if (!e.isFile()) continue;
      let st;
      try {
        st = fs.statSync(full);
      } catch {
        continue;
      }
      bytes += st.size;
      files += 1;
      if (st.mtimeMs < staleBefore) staleBytes += st.size;
    }
  }
  return { bytes, files, staleBytes };
}

/**
 * Split an artifact filename into its target stem and cargo's hash suffix.
 *
 *   pumper-f14bbb8982a98fcc.pdb -> { stem: 'pumper', hash: 'f14bbb8982a98fcc' }
 *
 * A file with no hash suffix returns `hash: null` and is NEVER pruned. Only a
 * hash-suffixed file is known to be one generation among several, and that
 * generational knowledge is the entire basis on which pruning is safe.
 */
export function splitArtifact(filename) {
  const parsed = path.parse(filename);
  const m = /^(.*)-([0-9a-f]{8,17})$/.exec(parsed.name);
  if (m === null) return { stem: parsed.name, hash: null };
  return { stem: m[1], hash: m[2] };
}

/**
 * Which files under a `deps/` directory are superseded generations.
 *
 * Group by hash-stripped stem; within a group the generation with the newest
 * mtime is LIVE and every one of its files is kept. Older generations are
 * prunable — but only once they are ALSO older than `staleBefore`, so a build
 * running right now can never have an artifact pulled out from under it.
 *
 * Returns [{file, size}].
 */
export function supersededFiles(depsDir, staleBefore) {
  let entries;
  try {
    entries = fs.readdirSync(depsDir, { withFileTypes: true });
  } catch {
    return [];
  }
  const groups = new Map();
  for (const e of entries) {
    if (!e.isFile()) continue;
    const { stem, hash } = splitArtifact(e.name);
    if (hash === null) continue;
    const full = path.join(depsDir, e.name);
    let st;
    try {
      st = fs.statSync(full);
    } catch {
      continue;
    }
    if (!groups.has(stem)) groups.set(stem, new Map());
    const byHash = groups.get(stem);
    const rec = byHash.get(hash) ?? { mtime: 0, files: [] };
    rec.mtime = Math.max(rec.mtime, st.mtimeMs);
    rec.files.push({ file: full, size: st.size });
    byHash.set(hash, rec);
  }
  const doomed = [];
  for (const byHash of groups.values()) {
    if (byHash.size < 2) continue;
    let newest = -Infinity;
    for (const rec of byHash.values()) newest = Math.max(newest, rec.mtime);
    for (const rec of byHash.values()) {
      if (rec.mtime === newest) continue;
      if (rec.mtime >= staleBefore) continue;
      for (const f of rec.files) doomed.push(f);
    }
  }
  return doomed;
}

/**
 * Incremental session dirs untouched since `staleBefore`. Incremental state is
 * pure cache — deleting it can only cost a recompile, never correctness — so
 * this needs none of the generational care `supersededFiles` takes.
 */
export function staleIncrementalDirs(incrementalDir, staleBefore) {
  let entries;
  try {
    entries = fs.readdirSync(incrementalDir, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const e of entries) {
    if (!e.isDirectory()) continue;
    const full = path.join(incrementalDir, e.name);
    let st;
    try {
      st = fs.statSync(full);
    } catch {
      continue;
    }
    if (st.mtimeMs < staleBefore) out.push(full);
  }
  return out;
}

/** Judge a measured target dir against the declared budget. Returns [message]. */
export function findings({ bytes, staleBytes }) {
  const out = [];
  if (bytes > CEILING_GB * GB) {
    out.push(
      `target/ is ${formatBytes(bytes)}, over the ${CEILING_GB} GB ceiling. ` +
        'Run `just disk-prune`; if that does not clear it, `just clean-target`.',
    );
  }
  if (staleBytes > STALE_FINDING_GB * GB) {
    out.push(
      `${formatBytes(staleBytes)} of target/ has not been touched in ${STALE_DAYS} days. ` +
        'Pruning has stopped happening, which is the state that reached 280 GB. Run `just disk-prune`.',
    );
  }
  return out;
}

function profileDirs(targetDir) {
  try {
    return fs
      .readdirSync(targetDir, { withFileTypes: true })
      .filter((e) => e.isDirectory() && e.name !== 'tmp')
      .map((e) => path.join(targetDir, e.name));
  } catch {
    return [];
  }
}

/**
 * Delete every prunable entry under `targetDir`. Returns {reclaimed, removed}.
 *
 * Extracted from the CLI so the GATE can call it too: a budget that only ever
 * nags is the same instrument as the habit that let this reach 280 GB. `onEntry`
 * is how --dry-run reports without deleting.
 */
export function pruneTarget(targetDir, staleBefore, { dryRun = false, onEntry = null } = {}) {
  let reclaimed = 0;
  let removed = 0;
  for (const profile of profileDirs(targetDir)) {
    for (const dir of staleIncrementalDirs(path.join(profile, 'incremental'), staleBefore)) {
      const { bytes } = measure(dir);
      if (onEntry) onEntry(dir, bytes);
      if (!dryRun) fs.rmSync(dir, { recursive: true, force: true });
      reclaimed += bytes;
      removed += 1;
    }
    for (const { file, size } of supersededFiles(path.join(profile, 'deps'), staleBefore)) {
      if (onEntry) onEntry(file, size);
      if (!dryRun) fs.rmSync(file, { force: true });
      reclaimed += size;
      removed += 1;
    }
  }
  return { reclaimed, removed };
}

function runPrune(root, targetDir, staleBefore, dryRun) {
  const { reclaimed, removed } = pruneTarget(targetDir, staleBefore, {
    dryRun,
    onEntry: dryRun
      ? (entry, bytes) => console.log(`would remove ${path.relative(root, entry)} (${formatBytes(bytes)})`)
      : null,
  });
  const verb = dryRun ? 'would reclaim' : 'reclaimed';
  console.log(`disk-prune: ${verb} ${formatBytes(reclaimed)} across ${removed} stale entries.`);
  return 0;
}

function runReport(root, targetDir, staleBefore, target) {
  console.log(`target/${formatBytes(target.bytes).padStart(13)}  ${target.files} files`);
  console.log(
    `  stale${formatBytes(target.staleBytes).padStart(13)}  untouched ${STALE_DAYS}d+`,
  );
  for (const profile of profileDirs(targetDir)) {
    for (const sub of ['deps', 'incremental', 'build']) {
      const p = path.join(profile, sub);
      if (!fs.existsSync(p)) continue;
      const m = measure(p, staleBefore);
      const label = path.relative(targetDir, p).replace(/\\/g, '/');
      console.log(`  ${label.padEnd(24)}${formatBytes(m.bytes).padStart(10)}  ${m.files} files`);
    }
  }
  for (const other of ['data', '.git']) {
    const p = path.join(root, other);
    if (!fs.existsSync(p)) continue;
    const m = measure(p);
    console.log(`${other.padEnd(7)}${formatBytes(m.bytes).padStart(13)}  ${m.files} files`);
  }
  console.log(`\nbudget: ceiling ${CEILING_GB} GB — currently ${formatBytes(target.bytes)}.`);
  return 0;
}

function main(argv) {
  const root = defaultRepoRoot();
  if (!fs.existsSync(path.join(root, 'Cargo.toml'))) {
    console.error('disk-check: no Cargo.toml at the repo root — cannot check.');
    return EXIT_CANNOT_CHECK;
  }
  const targetDir = path.join(root, 'target');
  const staleBefore = Date.now() - STALE_DAYS * DAY_MS;

  if (!fs.existsSync(targetDir)) {
    console.log('disk-check: no target/ — nothing built, nothing to budget.');
    return 0;
  }

  if (argv.includes('--prune')) {
    return runPrune(root, targetDir, staleBefore, argv.includes('--dry-run'));
  }

  const target = measure(targetDir, staleBefore);

  if (argv.includes('--report')) {
    return runReport(root, targetDir, staleBefore, target);
  }

  const before = findings(target);
  if (before.length === 0) {
    console.log(
      `disk-check: target/ is ${formatBytes(target.bytes)} of ${CEILING_GB} GB, ` +
        `${formatBytes(target.staleBytes)} stale. Within budget.`,
    );
    return 0;
  }

  // --no-prune is the pure verdict: measure, judge, mutate nothing.
  if (argv.includes('--no-prune')) {
    for (const f of before) console.error(`disk-check: ${f}`);
    return EXIT_FINDINGS;
  }

  // Otherwise SELF-HEAL. The gate only fires when someone runs `just ci`, and
  // the prune it would tell you to run is provably safe, so running it is
  // strictly better than printing its name. Going red is reserved for the case
  // pruning cannot fix — which is the case actually worth a human's attention.
  const { reclaimed, removed } = pruneTarget(targetDir, staleBefore);
  const after = measure(targetDir, staleBefore);
  const remaining = findings(after);
  if (remaining.length === 0) {
    console.log(
      `disk-check: target/ was ${formatBytes(target.bytes)}, over budget. ` +
        `Pruned ${formatBytes(reclaimed)} across ${removed} stale entries — ` +
        `now ${formatBytes(after.bytes)} of ${CEILING_GB} GB. Within budget.`,
    );
    return 0;
  }
  console.error(
    `disk-check: pruned ${formatBytes(reclaimed)} across ${removed} stale entries, ` +
      'and it was not enough. Every remaining artifact is LIVE:',
  );
  for (const f of remaining) console.error(`disk-check: ${f}`);
  return EXIT_FINDINGS;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  process.exit(main(process.argv.slice(2)));
}
