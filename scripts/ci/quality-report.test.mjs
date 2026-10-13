// Fixture suite for `scripts/ci/quality-report.mjs` — the posture report.
// `node --test scripts/ci/quality-report.test.mjs` (also `just harness-test`).
// node:test only, no dependencies.
//
// WHY EVERY CASE SPAWNS THE SCRIPT
//
// The card this suite was written for listed five of its seven acceptance cases
// as `just quality` invocations. That is wrong twice: `just` is not installed on
// every machine that must be able to prove this (it is not installed on the one
// this was built on), and a recipe is a one-line alias — asserting against it
// tests the alias, not the reporter. So every case below runs
// `node scripts/ci/quality-report.mjs [--markdown]` as a PROCESS and asserts its
// exit code and its stdout. `just quality` stays as the documented entry point
// and is never the thing under test; the one assertion about it is that the
// registry's recipe field and the justfile agree, which is
// gate-inventory.test.mjs's job, not this file's.
//
// A reporter is a checker, and this repo's rule for checkers is: seed the thing
// it looks for and watch it go red. So the fixtures plant a gate that exits 2, a
// gate that exits 3, a gate that cannot be run at all, a gate that exits 1 (a
// code its own contract does not allow), and a gate with no report mode — and
// each is asserted to land in its own column with its own exit contribution. A
// reporter that cannot tell those apart reports a clean codebase in a voice
// indistinguishable from success.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  EXIT_CANNOT_CHECK,
  EXIT_FINDINGS,
  VERDICT,
  buildRows,
  exitFor,
  firstFinding,
  parseArgs,
  remedyFor,
  remedyIsReachable,
  remedyPaths,
  tally,
  verdictForExit,
} from './quality-report.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '..', '..');
const SCRIPT = path.join(HERE, 'quality-report.mjs');
const REGISTRY = JSON.parse(fs.readFileSync(path.join(HERE, 'gates.json'), 'utf8'));

// --- fixture plumbing -------------------------------------------------------

const temps = [];
function tmp() {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'quality-report-'));
  temps.push(d);
  return d;
}
test.after(() => {
  for (const d of temps) fs.rmSync(d, { recursive: true, force: true });
});

/** A fake gate: a script that prints what it was told to and exits `code`. */
function plantGate(dir, name, code, line) {
  const file = path.join(dir, `${name}.mjs`);
  const stream = code === 0 ? 'stdout' : 'stderr';
  fs.writeFileSync(
    file,
    `process.${stream}.write(${JSON.stringify(`${line}\n`)});\nprocess.exit(${code});\n`,
  );
  return file;
}

/**
 * Write a fixture registry. `specs` is a list of
 * `{key, exit, line, wired = true, waived = false, command}`.
 */
function plantRegistry(dir, specs) {
  const gates = [];
  const reportModes = {};
  for (const s of specs) {
    const gate = { key: s.key, script: `scripts/ci/${s.key}.mjs`, recipe: s.key };
    if (s.waived) gate.verdictWaived = `declared waiver for ${s.key}: there is no standing tree to judge.`;
    gates.push(gate);
    if (s.command) {
      reportModes[s.key] = { command: s.command, remedy: `node scripts/ci/${s.key}.mjs` };
      continue;
    }
    if (s.wired === false) {
      reportModes[s.key] = { remedy: `node scripts/ci/${s.key}.mjs --by-hand` };
      continue;
    }
    const file = plantGate(dir, s.key, s.exit, s.line ?? `${s.key} said something`);
    reportModes[s.key] = { command: ['node', file], remedy: `node scripts/ci/${s.key}.mjs` };
  }
  const file = path.join(dir, 'gates.json');
  fs.writeFileSync(file, JSON.stringify({ gates, reportModes }, null, 2));
  return file;
}

function run(args, cwd = REPO_ROOT) {
  return spawnSync(process.execPath, [SCRIPT, ...args], { cwd, encoding: 'utf8' });
}

// --- case 1: a clean tree ---------------------------------------------------

test('clean_registry_exits_zero_with_one_row_per_gate_and_a_zeroed_footer', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'beta', exit: 0 },
    { key: 'gamma', exit: 0 },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 0, r.stdout + r.stderr);
  for (const k of ['alpha', 'beta', 'gamma']) {
    assert.match(r.stdout, new RegExp(`^\\s*${k}\\b.*CLEAN`, 'm'), `no CLEAN row for ${k}`);
  }
  assert.match(r.stdout, /3 clean \/ 0 findings \/ 0 cannot-check \/ 0 not wired/);
});

// --- case 2: one gate red, every other gate still gets a row ----------------

test('one_gate_with_findings_exits_two_and_the_other_gates_still_get_rows', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'beta', exit: 2, line: 'beta: the register has an expired row' },
    { key: 'gamma', exit: 0 },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, EXIT_FINDINGS);
  assert.equal(r.status, 2);
  // The whole point: `just ci` aborts at the first red rung, this does not.
  assert.match(r.stdout, /^\s*alpha\b.*CLEAN/m);
  assert.match(r.stdout, /^\s*beta\b.*FINDINGS/m);
  assert.match(r.stdout, /^\s*gamma\b.*CLEAN/m);
  assert.match(r.stdout, /the register has an expired row/);
  assert.match(r.stdout, /2 clean \/ 1 findings \/ 0 cannot-check \/ 0 not wired/);
});

// --- case 3: cannot-check is its own column, and its own exit ---------------

test('one_gate_that_cannot_check_exits_three_and_is_never_counted_clean', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'beta', exit: 3, line: 'beta: the register declares no ceiling' },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, EXIT_CANNOT_CHECK);
  assert.equal(r.status, 3);
  assert.match(r.stdout, /^\s*beta\b.*CANNOT CHECK/m);
  assert.match(r.stdout, /1 clean \/ 0 findings \/ 1 cannot-check \/ 0 not wired/);
  assert.doesNotMatch(r.stdout, /2 clean/);
});

// --- case 4: a finding must not hide a broken instrument --------------------

test('findings_dominate_the_exit_but_the_cannot_check_is_still_counted', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 2, line: 'alpha: over the ceiling' },
    { key: 'beta', exit: 3, line: 'beta: could not read the register' },
    { key: 'gamma', exit: 0 },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 2);
  assert.match(r.stdout, /1 clean \/ 1 findings \/ 1 cannot-check \/ 0 not wired/);
  assert.match(r.stdout, /^\s*beta\b.*CANNOT CHECK/m);
});

// --- case 5: a remedy on every red row --------------------------------------

test('every_non_clean_row_carries_a_runnable_remedy_that_resolves_in_the_repo', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 2 },
    { key: 'beta', exit: 3 },
    { key: 'gamma', wired: false },
    { key: 'delta', exit: 0 },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 2);
  for (const k of ['alpha', 'beta', 'gamma']) {
    const row = r.stdout.split('\n').find((l) => new RegExp(`^\\s*${k}\\b`).test(l));
    assert.ok(row, `no row for ${k}`);
    const remedy = row.split('|').pop().trim();
    assert.ok(remedy.length > 0, `${k}: red row with no remedy — half a verdict`);
    assert.ok(['just', 'node'].includes(remedy.split(/\s+/)[0]), `${k}: remedy "${remedy}"`);
  }
});

test('remedies_declared_in_the_real_registry_name_only_paths_that_exist', () => {
  // refusal-names-a-reachable-remedy, asserted against the registry that ships.
  const modes = REGISTRY.reportModes ?? {};
  const keys = Object.keys(modes).filter((k) => modes[k].self !== true);
  assert.ok(keys.length >= 4, `only ${keys.length} report modes declared`);
  for (const key of keys) {
    const remedy = remedyFor(
      REGISTRY.gates.find((g) => g.key === key) ?? { key },
      modes[key],
    );
    assert.ok(remedy, `${key}: no remedy`);
    assert.ok(['just', 'node'].includes(remedy.split(/\s+/)[0]), `${key}: "${remedy}"`);
    for (const p of remedyPaths(remedy)) {
      assert.ok(
        fs.existsSync(path.join(REPO_ROOT, p)),
        `${key}: remedy names ${p}, which does not exist — a remedy nobody can run`,
      );
    }
    assert.ok(remedyIsReachable(remedy, REPO_ROOT), `${key}: "${remedy}" is not reachable`);
  }
});

test('remedyIsReachable_rejects_a_path_outside_the_repo_and_a_bare_sentence', () => {
  assert.equal(remedyIsReachable('just disk-prune', REPO_ROOT), true);
  assert.equal(remedyIsReachable('node scripts/ci/disk-check.mjs --report', REPO_ROOT), true);
  assert.equal(remedyIsReachable('node ../elsewhere/fix.mjs', REPO_ROOT), false);
  assert.equal(remedyIsReachable('node scripts/ci/no-such-gate.mjs', REPO_ROOT), false);
  assert.equal(remedyIsReachable('ask someone', REPO_ROOT), false);
  assert.equal(remedyIsReachable('', REPO_ROOT), false);
});

// --- case 6: an inexpressible condition still enumerates --------------------

test('a_gate_with_no_report_command_renders_NOT_WIRED_instead_of_vanishing', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'gamma', wired: false },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.match(r.stdout, /^\s*gamma\b.*NOT WIRED/m);
  assert.match(r.stdout, /1 clean \/ 0 findings \/ 0 cannot-check \/ 1 not wired/);
  // Undeclared: nobody waived this rung, so the posture cannot claim green.
  assert.equal(r.status, 3);
});

test('a_NOT_WIRED_gate_whose_waiver_the_registry_declares_does_not_redden_the_run', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'gamma', wired: false, waived: true },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 0);
  assert.match(r.stdout, /^\s*gamma\b.*NOT WIRED/m);
  assert.match(r.stdout, /1 clean \/ 0 findings \/ 0 cannot-check \/ 1 not wired/);
});

// --- case 7: read-only by construction --------------------------------------

function snapshot(root) {
  const out = [];
  const walk = (dir, rel) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => (a.name < b.name ? -1 : 1))) {
      const full = path.join(dir, e.name);
      const r = rel ? `${rel}/${e.name}` : e.name;
      if (e.isDirectory()) {
        walk(full, r);
        continue;
      }
      const buf = fs.readFileSync(full);
      out.push(`${r} ${buf.length} ${crypto.createHash('sha256').update(buf).digest('hex')}`);
    }
  };
  walk(root, '');
  return out.join('\n');
}

test('two_runs_over_a_fixture_target_leave_it_byte_for_byte_identical', () => {
  const dir = tmp();
  const target = path.join(dir, 'target', 'debug', 'deps');
  fs.mkdirSync(target, { recursive: true });
  fs.writeFileSync(path.join(dir, 'Cargo.toml'), '[workspace]\n');
  for (let i = 0; i < 4; i += 1) {
    fs.writeFileSync(path.join(target, `libpumper-${i}.rlib`), 'x'.repeat(2048));
  }
  // The REAL disk-check, in the mode the posture must always use.
  const reg = plantRegistry(dir, [
    {
      key: 'disk-check',
      command: [
        'node',
        path.join(HERE, 'disk-check.mjs'),
        '--no-prune',
        '--root',
        dir,
        '--ceiling-gb',
        '999',
      ],
    },
  ]);
  const before = snapshot(path.join(dir, 'target'));
  const a = run(['--registry', reg, '--root', dir]);
  const mid = snapshot(path.join(dir, 'target'));
  const b = run(['--registry', reg, '--root', dir]);
  const after = snapshot(path.join(dir, 'target'));
  assert.equal(a.status, 0, a.stdout + a.stderr);
  assert.equal(b.status, 0, b.stdout + b.stderr);
  assert.equal(mid, before, 'the first run mutated target/');
  assert.equal(after, before, 'the second run mutated target/');
});

test('the_shipped_registry_invokes_disk_check_with_no_prune', () => {
  // The non-mutating contract is a property of the declared command, not of a
  // sentence: a later edit that drops --no-prune makes `just quality` a pruner.
  const mode = (REGISTRY.reportModes ?? {})['disk-check'];
  assert.ok(mode, 'gates.json declares no report mode for disk-check');
  assert.ok(
    mode.command.includes('--no-prune'),
    'disk-check must run --no-prune in the posture report: the report is read-only',
  );
});

// --- the instrument itself --------------------------------------------------

test('a_gate_that_cannot_be_spawned_at_all_is_CANNOT_CHECK_never_CLEAN', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'ghost', command: ['node', path.join(dir, 'does-not-exist.mjs')] },
  ]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 3);
  assert.match(r.stdout, /^\s*ghost\b.*CANNOT CHECK/m);
});

test('an_exit_code_outside_the_declared_contract_is_CANNOT_CHECK_not_a_pass', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [{ key: 'alpha', exit: 1, line: 'alpha: TypeError' }]);
  const r = run(['--registry', reg, '--root', dir]);
  assert.equal(r.status, 3);
  assert.match(r.stdout, /^\s*alpha\b.*CANNOT CHECK/m);
  assert.match(r.stdout, /exit 1/);
});

test('the_report_never_runs_itself', () => {
  // gates.json registers quality-report as a gate like any other, so the row
  // set must exclude the gate that IS the row set, or the run never terminates.
  const self = (REGISTRY.reportModes ?? {})['quality-report'];
  assert.ok(self, 'quality-report must declare its own report mode entry');
  assert.equal(self.self, true, 'the self entry must be marked, or the report recurses');
  const rows = buildRows(REGISTRY, () => ({ status: 0, stdout: '', stderr: '' }), REPO_ROOT);
  assert.ok(
    !rows.some((row) => row.key === 'quality-report'),
    'the posture report put itself in its own table',
  );
});

test('the_row_set_is_the_registry_and_not_a_hand_written_list', () => {
  const expected = REGISTRY.gates
    .map((g) => g.key)
    .filter((k) => (REGISTRY.reportModes[k] ?? {}).self !== true);
  const rows = buildRows(REGISTRY, () => ({ status: 0, stdout: '', stderr: '' }), REPO_ROOT);
  assert.deepEqual(rows.map((row) => row.key), expected);
});

test('markdown_mode_emits_a_GFM_table_with_the_same_verdicts_and_exit_code', () => {
  const dir = tmp();
  const reg = plantRegistry(dir, [
    { key: 'alpha', exit: 0 },
    { key: 'beta', exit: 2, line: 'beta: findings' },
  ]);
  const r = run(['--registry', reg, '--root', dir, '--markdown']);
  assert.equal(r.status, 2);
  assert.match(r.stdout, /^\| gate \| verdict \| finding \| remedy \|$/m);
  assert.match(r.stdout, /^\| --- \| --- \| --- \| --- \|$/m);
  assert.match(r.stdout, /^\| `alpha` \| CLEAN \|/m);
  assert.match(r.stdout, /^\| `beta` \| FINDINGS \|/m);
  assert.match(r.stdout, /1 clean \/ 1 findings \/ 0 cannot-check \/ 0 not wired/);
});

test('firstFinding_splits_CRLF_and_prefers_stderr', () => {
  // The bug wave 1 paid for: a JS `.` does not match `\r`, and this is a CRLF
  // checkout. A finding carrying a trailing \r corrupts every table it lands in.
  assert.equal(firstFinding('out one\r\nout two\r\n', ''), 'out one');
  assert.equal(firstFinding('out\n', 'err one\r\nerr two\r\n'), 'err one');
  assert.equal(firstFinding('\r\n\r\nthird\r\n', '\r\n'), 'third');
  assert.equal(firstFinding('', ''), '');
});

test('verdictForExit_maps_the_three_outcome_contract_and_nothing_else', () => {
  assert.equal(verdictForExit(0), VERDICT.CLEAN);
  assert.equal(verdictForExit(2), VERDICT.FINDINGS);
  assert.equal(verdictForExit(3), VERDICT.CANNOT_CHECK);
  assert.equal(verdictForExit(1), VERDICT.CANNOT_CHECK);
  assert.equal(verdictForExit(null), VERDICT.CANNOT_CHECK);
});

test('exitFor_is_the_worst_verdict_in_the_table', () => {
  const row = (verdict, waived = false) => ({ verdict, waived });
  assert.equal(exitFor([row(VERDICT.CLEAN), row(VERDICT.CLEAN)]), 0);
  assert.equal(exitFor([row(VERDICT.CLEAN), row(VERDICT.CANNOT_CHECK)]), 3);
  assert.equal(exitFor([row(VERDICT.CANNOT_CHECK), row(VERDICT.FINDINGS)]), 2);
  assert.equal(exitFor([row(VERDICT.NOT_WIRED)]), 3);
  assert.equal(exitFor([row(VERDICT.NOT_WIRED, true)]), 0);
});

test('tally_counts_cannot_check_in_its_own_column', () => {
  const t = tally([
    { verdict: VERDICT.CLEAN },
    { verdict: VERDICT.CLEAN },
    { verdict: VERDICT.FINDINGS },
    { verdict: VERDICT.CANNOT_CHECK },
    { verdict: VERDICT.NOT_WIRED },
  ]);
  assert.deepEqual(t, { clean: 2, findings: 1, cannotCheck: 1, notWired: 1 });
});

test('parseArgs_reads_the_registry_and_root_seams', () => {
  const a = parseArgs(['--markdown'], REPO_ROOT);
  assert.equal(a.markdown, true);
  assert.equal(a.root, REPO_ROOT);
  assert.equal(a.registryPath, path.join(REPO_ROOT, 'scripts', 'ci', 'gates.json'));
  const b = parseArgs(['--registry', 'x/gates.json', '--root', 'y'], REPO_ROOT);
  assert.equal(b.markdown, false);
  assert.equal(b.registryPath, path.resolve('x/gates.json'));
  assert.equal(b.root, path.resolve('y'));
});

test('an_unreadable_registry_is_exit_three_and_says_so', () => {
  const r = run(['--registry', path.join(tmp(), 'nope.json')]);
  assert.equal(r.status, 3);
  assert.match(r.stderr + r.stdout, /CANNOT CHECK/);
});

test('the_posture_over_this_tree_runs_and_returns_a_contract_exit_code', () => {
  // No fixture: the shipped registry against the real checkout. The verdict is
  // whatever this tree deserves; what is pinned is that the run completes, the
  // table has every registered row, and the code is one of 0/2/3.
  const r = run([]);
  assert.ok([0, 2, 3].includes(r.status), `exit ${r.status}\n${r.stdout}${r.stderr}`);
  for (const gate of REGISTRY.gates) {
    if ((REGISTRY.reportModes[gate.key] ?? {}).self === true) continue;
    assert.match(r.stdout, new RegExp(`^\\s*${gate.key}\\b`, 'm'), `no row for ${gate.key}`);
  }
  assert.match(r.stdout, /clean \/ .* findings \/ .* cannot-check \/ .* not wired/);
});
