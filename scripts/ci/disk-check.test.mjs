// Tests for the build-cache gate. `node --test scripts/ci/disk-check.test.mjs`
// (also `just harness-test`) — no dependencies, node:test only.
//
// A gate certifies nothing until it has been green on a known-good tree AND red
// on a known-bad one. These are the planted reds: both findings are driven past
// their declared limit, and — the half that actually matters here — the pruner
// is shown REFUSING to delete a live artifact, since a pruner that is merely
// enthusiastic is worse than no pruner at all.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  CEILING_GB,
  STALE_DAYS,
  STALE_FINDING_GB,
  findings,
  formatBytes,
  measure,
  pruneTarget,
  splitArtifact,
  staleIncrementalDirs,
  supersededFiles,
} from './disk-check.mjs';

const GB = 1024 ** 3;
const DAY_MS = 86400000;

function scratch() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'disk-check-'));
}

/** Write a file and backdate it `ageDays` into the past. */
function plant(dir, name, bytes, ageDays) {
  fs.mkdirSync(dir, { recursive: true });
  const full = path.join(dir, name);
  fs.writeFileSync(full, Buffer.alloc(bytes));
  const when = new Date(Date.now() - ageDays * DAY_MS);
  fs.utimesSync(full, when, when);
  return full;
}

// --- the budget -------------------------------------------------------------

test('a_tree_within_budget_is_not_a_finding', () => {
  assert.deepEqual(findings({ bytes: 5 * GB, staleBytes: 1 * GB }), []);
});

test('a_breached_ceiling_goes_red', () => {
  const out = findings({ bytes: (CEILING_GB + 1) * GB, staleBytes: 0 });
  assert.equal(out.length, 1);
  assert.match(out[0], /over the \d+ GB ceiling/);
});

test('stale_mass_goes_red_before_the_ceiling_does', () => {
  // The whole point of the second finding: 280 GB was reached by a tree that
  // was under any plausible ceiling every single day until it wasn't. Being
  // told only at the wall is being told too late.
  const out = findings({ bytes: (CEILING_GB - 1) * GB, staleBytes: (STALE_FINDING_GB + 1) * GB });
  assert.equal(out.length, 1);
  assert.match(out[0], /has not been touched/);
});

test('both_findings_can_fire_at_once', () => {
  const out = findings({
    bytes: (CEILING_GB + 1) * GB,
    staleBytes: (STALE_FINDING_GB + 1) * GB,
  });
  assert.equal(out.length, 2);
});

// --- measurement ------------------------------------------------------------

test('measure_counts_nested_files_and_separates_stale_from_fresh', () => {
  const root = scratch();
  plant(path.join(root, 'a'), 'fresh.rlib', 1024, 0);
  plant(path.join(root, 'a', 'b'), 'old.rlib', 2048, STALE_DAYS + 1);
  const m = measure(root, Date.now() - STALE_DAYS * DAY_MS);
  assert.equal(m.files, 2);
  assert.equal(m.bytes, 3072);
  assert.equal(m.staleBytes, 2048);
  fs.rmSync(root, { recursive: true, force: true });
});

test('measure_survives_a_directory_that_is_not_there', () => {
  // A concurrent build rewrites target/ constantly. A scan that throws on that
  // is an instrument that reports "cannot check" during normal operation.
  const m = measure(path.join(os.tmpdir(), 'disk-check-does-not-exist'));
  assert.deepEqual(m, { bytes: 0, files: 0, staleBytes: 0 });
});

// --- generational identity --------------------------------------------------

test('splitArtifact_reads_cargos_hash_suffix', () => {
  assert.deepEqual(splitArtifact('pumper-f14bbb8982a98fcc.pdb'), {
    stem: 'pumper',
    hash: 'f14bbb8982a98fcc',
  });
  assert.deepEqual(splitArtifact('libpumper_core-0123456789abcdef.rlib'), {
    stem: 'libpumper_core',
    hash: '0123456789abcdef',
  });
});

test('an_unhashed_name_is_not_a_generation', () => {
  // `pumper.exe` is not one generation among several, so nothing may conclude
  // it has been superseded. Returning a null hash is what keeps it unprunable.
  assert.equal(splitArtifact('pumper.exe').hash, null);
  assert.equal(splitArtifact('CACHEDIR.TAG').hash, null);
  assert.equal(splitArtifact('pumper-notahash.pdb').hash, null);
});

// --- the pruner, and what it refuses to touch -------------------------------

test('superseded_generations_are_pruned', () => {
  const deps = path.join(scratch(), 'deps');
  plant(deps, 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, STALE_DAYS + 5);
  plant(deps, 'pumper-bbbbbbbbbbbbbbbb.pdb', 4096, 0);
  const doomed = supersededFiles(deps, Date.now() - STALE_DAYS * DAY_MS);
  assert.equal(doomed.length, 1);
  assert.match(doomed[0].file, /aaaaaaaaaaaaaaaa/);
  fs.rmSync(path.dirname(deps), { recursive: true, force: true });
});

test('the_newest_generation_is_never_pruned_however_old_it_is', () => {
  // A crate nobody has touched in a year is still the crate that links today.
  // Age alone must never condemn an artifact — only being superseded does.
  const deps = path.join(scratch(), 'deps');
  plant(deps, 'ancient-aaaaaaaaaaaaaaaa.rlib', 4096, 400);
  assert.deepEqual(supersededFiles(deps, Date.now() - STALE_DAYS * DAY_MS), []);
  fs.rmSync(path.dirname(deps), { recursive: true, force: true });
});

test('a_superseded_but_recent_generation_is_left_alone', () => {
  // The build that produced the newer hash may still be running. A week of
  // grace costs a few GB; deleting under a live link costs a broken build.
  const deps = path.join(scratch(), 'deps');
  plant(deps, 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, 1);
  plant(deps, 'pumper-bbbbbbbbbbbbbbbb.pdb', 4096, 0);
  assert.deepEqual(supersededFiles(deps, Date.now() - STALE_DAYS * DAY_MS), []);
  fs.rmSync(path.dirname(deps), { recursive: true, force: true });
});

test('every_file_of_a_superseded_generation_goes_together', () => {
  // .pdb/.exe/.d of one hash are one artifact. Pruning the pdb and keeping the
  // exe leaves a binary whose symbols have silently gone missing.
  const deps = path.join(scratch(), 'deps');
  plant(deps, 'render-aaaaaaaaaaaaaaaa.exe', 1024, STALE_DAYS + 5);
  plant(deps, 'render-aaaaaaaaaaaaaaaa.pdb', 2048, STALE_DAYS + 5);
  plant(deps, 'render-aaaaaaaaaaaaaaaa.d', 16, STALE_DAYS + 5);
  plant(deps, 'render-bbbbbbbbbbbbbbbb.exe', 1024, 0);
  const doomed = supersededFiles(deps, Date.now() - STALE_DAYS * DAY_MS);
  assert.equal(doomed.length, 3);
  assert.ok(doomed.every((d) => d.file.includes('aaaaaaaaaaaaaaaa')));
  fs.rmSync(path.dirname(deps), { recursive: true, force: true });
});

test('different_targets_are_not_each_others_generations', () => {
  // Grouping on the wrong key is how a pruner deletes a live crate: `pumper`
  // and `pumper_core` must never compete for the same "newest" slot.
  const deps = path.join(scratch(), 'deps');
  plant(deps, 'pumper-aaaaaaaaaaaaaaaa.rlib', 1024, STALE_DAYS + 5);
  plant(deps, 'pumper_core-bbbbbbbbbbbbbbbb.rlib', 1024, 0);
  assert.deepEqual(supersededFiles(deps, Date.now() - STALE_DAYS * DAY_MS), []);
  fs.rmSync(path.dirname(deps), { recursive: true, force: true });
});

test('stale_incremental_sessions_are_prunable_and_fresh_ones_are_not', () => {
  const root = scratch();
  const inc = path.join(root, 'incremental');
  const old = path.join(inc, 's-old');
  const fresh = path.join(inc, 's-fresh');
  plant(old, 'dep-graph.bin', 64, STALE_DAYS + 3);
  plant(fresh, 'dep-graph.bin', 64, 0);
  const when = new Date(Date.now() - (STALE_DAYS + 3) * DAY_MS);
  fs.utimesSync(old, when, when);
  const doomed = staleIncrementalDirs(inc, Date.now() - STALE_DAYS * DAY_MS);
  assert.deepEqual(doomed, [old]);
  fs.rmSync(root, { recursive: true, force: true });
});

// --- the self-heal ----------------------------------------------------------

test('pruneTarget_reclaims_both_kinds_and_reports_what_it_took', () => {
  const root = scratch();
  const debug = path.join(root, 'debug');
  plant(path.join(debug, 'deps'), 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, STALE_DAYS + 5);
  plant(path.join(debug, 'deps'), 'pumper-bbbbbbbbbbbbbbbb.pdb', 4096, 0);
  const sess = path.join(debug, 'incremental', 's-old');
  plant(sess, 'dep-graph.bin', 2048, STALE_DAYS + 3);
  const when = new Date(Date.now() - (STALE_DAYS + 3) * DAY_MS);
  fs.utimesSync(sess, when, when);

  const { reclaimed, removed } = pruneTarget(root, Date.now() - STALE_DAYS * DAY_MS);
  assert.equal(removed, 2);
  assert.equal(reclaimed, 4096 + 2048);
  assert.ok(!fs.existsSync(sess), 'stale incremental session survived the prune');
  assert.ok(
    fs.existsSync(path.join(debug, 'deps', 'pumper-bbbbbbbbbbbbbbbb.pdb')),
    'the LIVE generation was deleted — the pruner is not safe',
  );
  fs.rmSync(root, { recursive: true, force: true });
});

test('a_dry_run_reports_without_deleting_anything', () => {
  // The escape hatch has to be trustworthy on its own: if --dry-run ever
  // deletes, nobody can use it to check what a prune is about to do.
  const root = scratch();
  const deps = path.join(root, 'debug', 'deps');
  plant(deps, 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, STALE_DAYS + 5);
  plant(deps, 'pumper-bbbbbbbbbbbbbbbb.pdb', 4096, 0);
  const seen = [];
  const { removed } = pruneTarget(root, Date.now() - STALE_DAYS * DAY_MS, {
    dryRun: true,
    onEntry: (entry) => seen.push(entry),
  });
  assert.equal(removed, 1);
  assert.equal(seen.length, 1);
  assert.ok(fs.existsSync(path.join(deps, 'pumper-aaaaaaaaaaaaaaaa.pdb')), '--dry-run deleted');
  fs.rmSync(root, { recursive: true, force: true });
});

test('pruning_a_tree_with_nothing_stale_is_a_no_op', () => {
  const root = scratch();
  plant(path.join(root, 'debug', 'deps'), 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, 0);
  assert.deepEqual(pruneTarget(root, Date.now() - STALE_DAYS * DAY_MS), {
    reclaimed: 0,
    removed: 0,
  });
  fs.rmSync(root, { recursive: true, force: true });
});

test('a_tree_of_only_live_artifacts_cannot_be_healed_and_must_stay_red', () => {
  // The case that earns a human's attention: over budget with nothing prunable.
  // If the self-heal swallowed this, the gate would go quiet exactly when the
  // problem is real rather than merely untidy.
  const root = scratch();
  plant(path.join(root, 'debug', 'deps'), 'pumper-aaaaaaaaaaaaaaaa.pdb', 4096, 0);
  const { removed } = pruneTarget(root, Date.now() - STALE_DAYS * DAY_MS);
  assert.equal(removed, 0);
  assert.equal(findings({ bytes: (CEILING_GB + 1) * GB, staleBytes: 0 }).length, 1);
  fs.rmSync(root, { recursive: true, force: true });
});

// --- reporting --------------------------------------------------------------

test('formatBytes_scales_so_a_report_is_readable_at_both_ends', () => {
  assert.equal(formatBytes(512), '512 B');
  assert.equal(formatBytes(5 * 1024 ** 2), '5 MB');
  assert.equal(formatBytes(280.8 * GB), '280.80 GB');
});
