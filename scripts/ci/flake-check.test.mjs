// Tests for the flake register and its gate. `node --test scripts/ci/flake-check.test.mjs`
// (also `just flake-check-test`) — no dependencies, node:test only.
//
// Every test is named for the anti-pattern it defends against, and every failure
// mode the gate claims is driven from a throwaway repo rather than by breaking
// the real tree. A gate that has never been observed to go red is
// indistinguishable from a gate that cannot.

import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  ignoreAttribute,
  fnAfter,
  packages,
  parseCargoTestOutput,
  registerKey,
  scanIgnoredTests,
  targetForSource,
} from './flake-id.mjs';
import { evaluateRegister, labelled, readsAsFlake, transitions } from './flake-check.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const CHECK = path.join(HERE, 'flake-check.mjs');

// --- stable identity --------------------------------------------------------

const PKGS = [
  { dir: 'crates/core', name: 'pumper-core', hasLib: true, bins: [] },
  { dir: 'crates/server', name: 'pumper-server', hasLib: false, bins: [{ name: 'pumper', path: 'src/main.rs' }, { name: 'reindex', path: 'src/bin/reindex.rs' }] },
];

test('a_module_path_alone_is_not_a_test_identity', () => {
  // `governor::tests::foo` exists in the lib AND could exist in any integration
  // binary. The target is what makes the module path unique, so it is part of
  // the id — merging two tests' histories is worse than having none.
  assert.deepEqual(targetForSource('crates/core/src/governor.rs', PKGS), {
    pkg: 'pumper-core',
    target: 'lib',
  });
  assert.deepEqual(targetForSource('crates/core/tests/datasets_bulk_perf.rs', PKGS), {
    pkg: 'pumper-core',
    target: 'test:datasets_bulk_perf',
  });
});

test('a_binary_only_crate_does_not_get_a_phantom_lib_target', () => {
  // pumper-server has no lib: its whole e2e suite lives under src/ and compiles
  // into `bin:pumper`. Calling that `lib` would make every server id wrong, and
  // wrong ids are invisible — they just never match anything.
  assert.deepEqual(targetForSource('crates/server/src/e2e/trigger_plugins.rs', PKGS), {
    pkg: 'pumper-server',
    target: 'bin:pumper',
  });
  assert.deepEqual(targetForSource('crates/server/src/bin/reindex.rs', PKGS), {
    pkg: 'pumper-server',
    target: 'bin:reindex',
  });
});

test('a_shared_test_helper_module_is_not_its_own_target', () => {
  // tests/<name>.rs is a target; tests/<name>/mod.rs is a module a target
  // includes. Treating the second as a target would invent a package-wide id
  // prefix that cargo never emits.
  assert.equal(targetForSource('crates/core/tests/lane_artifact/mod.rs', PKGS)?.target, undefined);
});

test('a_register_id_reduces_to_the_key_a_source_scan_can_produce', () => {
  assert.equal(
    registerKey('pumper-core::lib::governor::tests::distinct_hosts_run_in_parallel_but_each_host_spaces'),
    'pumper-core::lib::distinct_hosts_run_in_parallel_but_each_host_spaces'
  );
  assert.equal(registerKey('too::short'), null);
});

// --- the source scanner -----------------------------------------------------

test('a_doc_comment_that_mentions_ignore_is_not_a_quarantine', () => {
  // This repo documents `#[ignore]` in a dozen doc comments. A scanner that read
  // prose as quarantine would fill the register with sentences.
  assert.equal(ignoreAttribute('/// `#[ignore]`d with the other timing tests'), null);
  assert.equal(ignoreAttribute('//! 2. The `#[ignore]`d tests load the built artifacts'), null);
  assert.equal(ignoreAttribute('    // #[ignore] used to be here'), null);
  assert.deepEqual(ignoreAttribute('    #[ignore]'), { reason: null });
  assert.deepEqual(ignoreAttribute('#[ignore = "needs Chrome"]'), { reason: 'needs Chrome' });
  assert.deepEqual(ignoreAttribute('#[ignore = "says \\"hi\\""]'), { reason: 'says "hi"' });
});

test('an_ignore_finds_its_fn_past_the_other_attributes', () => {
  const lines = ['#[ignore = "x"]', '#[tokio::test(flavor = "multi_thread")]', 'async fn the_test() {'];
  assert.equal(fnAfter(lines, 1), 'the_test');
  assert.equal(fnAfter(['#[ignore]', 'struct NotATest;'], 1), null);
});

// --- the libtest reader -----------------------------------------------------

const RUN_LOG = [
  '   Compiling pumper-core v0.1.0',
  '     Running unittests src/governor.rs (target/debug/deps/pumper_core-1a2b)',
  'running 2 tests',
  'test governor::tests::spacing_holds ... ok',
  'test governor::tests::hosts_are_parallel ... FAILED',
  'test result: FAILED. 1 passed; 1 failed; 0 ignored',
  '     Running tests/datasets_bulk_perf.rs (target/debug/deps/datasets_bulk_perf-9f)',
  'test bulk_upsert_50k_cost_report ... ignored, long lane',
  'failures:',
  '    governor::tests::hosts_are_parallel',
  '',
  '   Doc-tests pumper-core',
  'test crates/core/src/lib.rs - datasets (line 12) ... ok',
].join('\n');

test('a_result_line_is_attributed_to_the_target_that_printed_it', () => {
  const records = parseCargoTestOutput(RUN_LOG, PKGS, REPO_ROOT);
  const ids = records.map((r) => r.id);
  assert.ok(ids.includes('pumper-core::lib::governor::tests::spacing_holds'));
  assert.ok(ids.includes('pumper-core::test:datasets_bulk_perf::bulk_upsert_50k_cost_report'));
  assert.ok(ids.includes('pumper-core::doctest::crates/core/src/lib.rs - datasets (line 12)'));
});

test('the_trailing_failures_block_does_not_double_count_a_failure', () => {
  const records = parseCargoTestOutput(RUN_LOG, PKGS, REPO_ROOT);
  const failed = records.filter((r) => r.outcome === 'FAILED');
  assert.equal(failed.length, 1, JSON.stringify(failed));
  assert.equal(failed[0].id, 'pumper-core::lib::governor::tests::hosts_are_parallel');
  // `test result: FAILED. ...` is a summary line, not a test.
  assert.ok(!records.some((r) => r.id.includes('result:')));
});

// --- transitions: the same code, or it is not a flake signal ----------------

function run(at, sha, outcomes, branch = 'master') {
  return { startedAt: at, sha, branch, tests: Object.entries(outcomes).map(([id, outcome]) => ({ id, outcome })) };
}

test('a_consistently_failing_test_is_broken_not_flaky', () => {
  // The whole reason detection counts transitions and not a failure rate: this
  // test failed 100% of the time and needs the opposite response to a flake.
  const runs = [
    run('2026-08-20T00:00:00Z', 'aaa', { t: 'FAILED' }),
    run('2026-08-21T00:00:00Z', 'aaa', { t: 'FAILED' }),
    run('2026-08-22T00:00:00Z', 'aaa', { t: 'FAILED' }),
  ];
  const tr = transitions(runs, { branch: 'master', windowDays: 14, today: '2026-08-24' });
  assert.equal(tr.byId.get('t').changed, 0);
  assert.deepEqual(labelled(tr, 2), []);
});

test('outcomes_compared_across_different_trees_are_not_a_flake_signal', () => {
  // Same-code is the load-bearing qualifier: ok -> FAILED on a DIFFERENT commit
  // measures the product's churn, not the test's stability.
  const runs = [
    run('2026-08-20T00:00:00Z', 'aaa', { t: 'ok' }),
    run('2026-08-21T00:00:00Z', 'bbb', { t: 'FAILED' }),
    run('2026-08-22T00:00:00Z', 'ccc', { t: 'ok' }),
  ];
  const tr = transitions(runs, { branch: 'master', windowDays: 14, today: '2026-08-24' });
  assert.equal(tr.byId.get('t').changed, 0);
  assert.equal(tr.byId.get('t').sameCodePairs, 0);
});

test('an_outcome_that_changed_on_the_same_commit_is_a_flake_and_carries_its_predicate', () => {
  const runs = [
    run('2026-08-20T00:00:00Z', 'aaa', { t: 'ok' }),
    run('2026-08-21T00:00:00Z', 'aaa', { t: 'FAILED' }),
    run('2026-08-22T00:00:00Z', 'aaa', { t: 'ok' }),
  ];
  const tr = transitions(runs, { branch: 'master', windowDays: 14, today: '2026-08-24' });
  assert.equal(tr.byId.get('t').changed, 2);
  assert.equal(tr.byId.get('t').sameCodePairs, 2);
  assert.match(tr.predicate, /window 14 days ending 2026-08-24, branch master, 3 recorded run\(s\)/);
  assert.equal(labelled(tr, 2).length, 1);
});

test('a_label_is_reversed_by_the_window_moving_not_by_someone_remembering', () => {
  // The half everyone forgets. There is no stored label to remove: the label IS
  // the query, so a test that has been stable for the window stops being called
  // flaky on the very next run.
  const runs = [
    run('2026-07-01T00:00:00Z', 'aaa', { t: 'ok' }),
    run('2026-07-02T00:00:00Z', 'aaa', { t: 'FAILED' }),
    run('2026-07-03T00:00:00Z', 'aaa', { t: 'ok' }),
    run('2026-08-22T00:00:00Z', 'zzz', { t: 'ok' }),
  ];
  const inWindow = transitions(runs, { branch: 'master', windowDays: 60, today: '2026-08-24' });
  assert.equal(labelled(inWindow, 2).length, 1);
  const aged = transitions(runs, { branch: 'master', windowDays: 14, today: '2026-08-24' });
  assert.equal(labelled(aged, 2).length, 0);
});

test('a_branch_filter_is_part_of_the_predicate_not_an_afterthought', () => {
  const runs = [
    run('2026-08-20T00:00:00Z', 'aaa', { t: 'ok' }, 'feature/x'),
    run('2026-08-21T00:00:00Z', 'aaa', { t: 'FAILED' }, 'feature/x'),
  ];
  const tr = transitions(runs, { branch: 'master', windowDays: 14, today: '2026-08-24' });
  assert.equal(tr.runs, 0);
  assert.equal(tr.byId.size, 0);
});

// --- the register's own rules -----------------------------------------------

const SCAN = [
  { key: 'p::lib::flaky_one', pkg: 'p', target: 'lib', fn: 'flaky_one', file: 'crates/p/src/a.rs', line: 10, reason: 'flaky under load' },
  { key: 'p::lib::needs_chrome', pkg: 'p', target: 'lib', fn: 'needs_chrome', file: 'crates/p/src/a.rs', line: 20, reason: 'requires local Chrome' },
];

function entry(over = {}) {
  return {
    id: 'p::lib::mod::flaky_one',
    owner: 'xkazm04',
    entered: '2026-08-01',
    expires: '2026-12-01',
    cause: 'test',
    form: 'muted',
    evidence: 'a link',
    ...over,
  };
}

const EXEMPT = { id: 'p::lib::mod::needs_chrome', gate: 'local-chrome', reason: 'no browser on the runner' };

function reg(over = {}) {
  return { ceiling: 4, quarantine: [entry()], exempt: [EXEMPT], ...over };
}

const kinds = (f) => f.map((x) => x.kind);

test('a_complete_register_over_a_matching_tree_is_clean', () => {
  const { findings, health } = evaluateRegister(reg(), SCAN, '2026-08-24');
  assert.deepEqual(findings, [], JSON.stringify(findings, null, 2));
  assert.equal(health.size, 1);
  assert.equal(health.oldest.ageDays, 23);
});

test('an_expired_entry_escalates_rather_than_extending_silently', () => {
  const { findings } = evaluateRegister(reg({ quarantine: [entry({ expires: '2026-08-01' })] }), SCAN, '2026-08-24');
  assert.ok(kinds(findings).includes('expired'), JSON.stringify(findings));
  assert.match(findings.find((f) => f.kind === 'expired').message, /EXPIRED on 2026-08-01 \(23 days ago\)/);
});

test('an_entry_naming_a_test_the_tree_no_longer_has_is_an_orphan', () => {
  const { findings } = evaluateRegister(reg({ quarantine: [entry({ id: 'p::lib::mod::renamed_away' })] }), SCAN, '2026-08-24');
  assert.ok(kinds(findings).includes('orphan'), JSON.stringify(findings));
  // And the ignore it used to cover is now undeclared, in the other direction.
  assert.ok(kinds(findings).includes('unregistered-flake'));
});

test('a_flake_reasoned_ignore_with_no_entry_is_not_quietly_tolerated', () => {
  const { findings } = evaluateRegister(reg({ quarantine: [] }), SCAN, '2026-08-24');
  const f = findings.find((x) => x.kind === 'unregistered-flake');
  assert.ok(f, JSON.stringify(findings));
  assert.match(f.message, /crates\/p\/src\/a\.rs:10/);
});

test('an_ignore_in_neither_table_must_be_classified', () => {
  const scan = [...SCAN, { key: 'p::lib::brand_new', pkg: 'p', target: 'lib', fn: 'brand_new', file: 'crates/p/src/b.rs', line: 3, reason: 'slow' }];
  const { findings } = evaluateRegister(reg(), scan, '2026-08-24');
  assert.ok(kinds(findings).includes('undeclared'), JSON.stringify(findings));
});

test('an_environment_exemption_is_not_a_laundry_for_a_flake', () => {
  // The move this catches: reclassify a timing flake as "environment-gated" and
  // it leaves the register without ever being fixed.
  const scan = [SCAN[0], { ...SCAN[1], reason: 'requires Chrome; also a bit flaky on CI' }];
  const { findings } = evaluateRegister(reg(), scan, '2026-08-24');
  assert.ok(kinds(findings).includes('laundered'), JSON.stringify(findings));
});

test('a_breached_ceiling_is_a_stop_the_line_finding', () => {
  const scan = [
    ...SCAN,
    { key: 'p::lib::f2', pkg: 'p', target: 'lib', fn: 'f2', file: 'x.rs', line: 1, reason: 'timing' },
    { key: 'p::lib::f3', pkg: 'p', target: 'lib', fn: 'f3', file: 'x.rs', line: 2, reason: 'timing' },
  ];
  const register = reg({
    ceiling: 2,
    quarantine: [entry(), entry({ id: 'p::lib::mod::f2' }), entry({ id: 'p::lib::mod::f3' })],
  });
  const { findings } = evaluateRegister(register, scan, '2026-08-24');
  const f = findings.find((x) => x.kind === 'ceiling');
  assert.ok(f, JSON.stringify(findings));
  assert.match(f.message, /STOP THE LINE: 3 quarantined tests against a ceiling of 2/);
});

test('a_team_is_not_an_owner', () => {
  const { findings } = evaluateRegister(reg({ quarantine: [entry({ owner: 'the platform team' })] }), SCAN, '2026-08-24');
  assert.ok(kinds(findings).includes('unowned'), JSON.stringify(findings));
});

test('an_incomplete_entry_is_not_a_quarantine_decision', () => {
  const bare = { id: 'p::lib::mod::flaky_one', owner: 'xkazm04' };
  const { findings } = evaluateRegister(reg({ quarantine: [bare] }), SCAN, '2026-08-24');
  const missing = findings.filter((f) => f.kind === 'incomplete').map((f) => f.message);
  for (const field of ['entered', 'expires', 'cause', 'form', 'evidence']) {
    assert.ok(missing.some((m) => m.includes(field)), `${field} not reported: ${missing.join('|')}`);
  }
});

test('a_skip_without_a_stated_reason_is_refused_because_muted_is_preferred', () => {
  const { findings } = evaluateRegister(reg({ quarantine: [entry({ form: 'skipped' })] }), SCAN, '2026-08-24');
  assert.ok(kinds(findings).includes('unjustified-skip'), JSON.stringify(findings));
  const ok = evaluateRegister(
    reg({ quarantine: [entry({ form: 'skipped', formReason: 'libtest has no muted form' })] }),
    SCAN,
    '2026-08-24'
  );
  assert.deepEqual(ok.findings, []);
});

test('two_ignored_tests_that_share_an_identity_are_reported_not_silently_merged', () => {
  const scan = [...SCAN, { ...SCAN[0], file: 'crates/p/src/c.rs', line: 5 }];
  const { findings } = evaluateRegister(reg(), scan, '2026-08-24');
  assert.ok(kinds(findings).includes('ambiguous'), JSON.stringify(findings));
});

test('flake_vocabulary_reads_a_reason_not_a_classification', () => {
  assert.equal(readsAsFlake('asserts wall-clock timing; flaky on loaded machines'), true);
  assert.equal(readsAsFlake('requires the built data/plugins/title.wasm'), false);
  assert.equal(readsAsFlake(null), false);
});

// --- end to end, through the script's own exit codes ------------------------

/** A throwaway git repo with one crate, so `git ls-files` has something to find. */
function fakeRepo(files) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-check-'));
  for (const [rel, body] of Object.entries(files)) {
    const p = path.join(root, rel);
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, body);
  }
  // stdio ignored: git's CRLF advice on a Windows box is noise that would drown
  // the test report, and nothing here reads git's output.
  execFileSync('git', ['init', '-q'], { cwd: root, stdio: 'ignore' });
  execFileSync('git', ['add', '-A'], { cwd: root, stdio: 'ignore' });
  return root;
}

const CRATE_TOML = '[package]\nname = "p"\nversion = "0.1.0"\n';
const SRC = (reason) => `#[cfg(test)]\nmod tests {\n    #[test]\n    #[ignore = "${reason}"]\n    fn the_test() {}\n}\n`;

function runCheck(root, extra = []) {
  return spawnSync(process.execPath, [CHECK, '--today', '2026-08-24', ...extra], {
    encoding: 'utf8',
    env: { ...process.env, CLAUDE_PROJECT_DIR: root },
  });
}

function registerFile(over = {}) {
  return JSON.stringify({
    ceiling: 4,
    quarantine: [{ ...entry(), id: 'p::lib::tests::the_test' }],
    exempt: [],
    ...over,
  });
}

test('the_gate_is_green_on_a_registered_flake', () => {
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('flaky under load'),
    '.flake/register.json': registerFile(),
  });
  const r = runCheck(root);
  assert.equal(r.status, 0, r.stdout + r.stderr);
});

test('the_gate_goes_red_on_an_expired_entry', () => {
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('flaky under load'),
    '.flake/register.json': registerFile({
      quarantine: [{ ...entry({ expires: '2026-01-01' }), id: 'p::lib::tests::the_test' }],
    }),
  });
  const r = runCheck(root);
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /\[expired\]/);
});

test('the_gate_goes_red_on_an_unregistered_flake_reasoned_ignore', () => {
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('timing-dependent, flaky'),
    '.flake/register.json': registerFile({ quarantine: [] }),
  });
  const r = runCheck(root);
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /\[unregistered-flake\]/);
  assert.match(r.stderr, /An agent NEVER quarantines a test to make a build green/);
});

test('the_gate_goes_red_on_an_entry_naming_a_test_that_does_not_exist', () => {
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('requires local Chrome'),
    '.flake/register.json': registerFile({
      quarantine: [{ ...entry(), id: 'p::lib::tests::deleted_last_year' }],
      exempt: [{ id: 'p::lib::tests::the_test', gate: 'local-chrome', reason: 'no browser' }],
    }),
  });
  const r = runCheck(root);
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /\[orphan\]/);
});

// --- the third outcome ------------------------------------------------------

test('a_missing_register_is_cannot_check_not_a_pass', () => {
  const root = fakeRepo({ 'crates/p/Cargo.toml': CRATE_TOML, 'crates/p/src/lib.rs': SRC('flaky') });
  const r = runCheck(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  assert.match(r.stderr, /CANNOT CHECK/);
});

test('a_register_with_no_ceiling_is_cannot_check_not_a_pass', () => {
  // The empty standard: it parses, it reconciles, and it can never stop the line.
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('flaky under load'),
    '.flake/register.json': JSON.stringify({ quarantine: [], exempt: [] }),
  });
  const r = runCheck(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  assert.match(r.stderr, /ceiling/);
});

test('a_scanner_that_finds_no_ignores_is_cannot_check_not_a_pass', () => {
  // A tree with nineteen quarantines that scans to zero looks EXACTLY like a
  // repo with nothing to quarantine. Three states, three exit codes.
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': '#[test]\nfn fine() {}\n',
    '.flake/register.json': JSON.stringify({ ceiling: 4, quarantine: [], exempt: [] }),
  });
  const r = runCheck(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  assert.match(r.stderr, /found zero #\[ignore\]d tests/);
});

test('a_corrupt_register_is_cannot_check_not_a_pass', () => {
  const root = fakeRepo({
    'crates/p/Cargo.toml': CRATE_TOML,
    'crates/p/src/lib.rs': SRC('flaky'),
    '.flake/register.json': '{ not json',
  });
  const r = runCheck(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
});

// --- the recorder in front of a required check ------------------------------
//
// scripts/ci/flake-record.mjs wraps `cargo test` on a branch-protection required
// check. Its whole contract is that it cannot change the verdict, so that is
// what these assert.

const RECORD = path.join(HERE, 'flake-record.mjs');

function runRecord(root, args, script) {
  return spawnSync(process.execPath, [RECORD, ...args, '--', process.execPath, '-e', script], {
    encoding: 'utf8',
    env: { ...process.env, CLAUDE_PROJECT_DIR: root },
  });
}

test('the_wrappers_exit_code_is_the_wrapped_commands_exit_code', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-'));
  for (const code of [0, 1, 7, 101]) {
    const r = runRecord(root, [], `console.log("test a::b ... ok"); process.exit(${code})`);
    assert.equal(r.status, code, `exit ${code} became ${r.status}`);
  }
});

test('the_wrapper_does_not_swallow_the_run_output', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-'));
  const r = runRecord(root, [], 'console.log("test a::b ... ok"); console.error("cargo says hello")');
  assert.match(r.stdout, /test a::b \.\.\. ok/);
  assert.match(r.stdout, /cargo says hello/);
});

test('a_run_is_recorded_with_its_outcomes_and_its_branch_and_sha', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-'));
  runRecord(root, [], 'console.log("test a::b ... ok\\ntest a::c ... FAILED"); process.exit(1)');
  const dir = path.join(root, '.flake/history/runs');
  const files = fs.readdirSync(dir);
  assert.equal(files.length, 1);
  const rec = JSON.parse(fs.readFileSync(path.join(dir, files[0]), 'utf8'));
  assert.equal(rec.exit, 1);
  assert.ok(rec.branch && 'sha' in rec && rec.startedAt);
  assert.deepEqual(
    rec.tests.map((t) => t.outcome),
    ['ok', 'FAILED']
  );
});

test('a_lane_record_never_overwrites_the_harnesss_own_measurement', () => {
  // The bug this test exists for: `--lane x` wrote .lanes/runs/x.json, which is
  // the name the Rust harness had just written its measured series to. A whole
  // artifact was replaced by a note saying the test passed — a lane reporting
  // green with nothing measured behind it.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-'));
  fs.mkdirSync(path.join(root, '.lanes/runs'), { recursive: true });
  const perf = () => {
    fs.writeFileSync(
      path.join(root, '.lanes/runs/x.json'),
      JSON.stringify({ lane: 'x', kind: 'perf', series: { hold_ms: [1, 2, 3] }, scalars: {} })
    );
  };
  perf();
  runRecord(root, ['--lane', 'x'], `require("fs").writeFileSync(${JSON.stringify(path.join(root, '.lanes/runs/x.json').split('\\').join('/'))}, ${JSON.stringify(JSON.stringify({ lane: 'x', kind: 'perf', series: { hold_ms: [1, 2, 3] }, scalars: {} }))}); console.log("test a::b ... ok")`);
  const files = fs.readdirSync(path.join(root, '.lanes/runs')).sort();
  assert.deepEqual(files, ['x--suite.json', 'x.json']);
  const kept = JSON.parse(fs.readFileSync(path.join(root, '.lanes/runs/x.json'), 'utf8'));
  assert.deepEqual(kept.series.hold_ms, [1, 2, 3]);
});

test('a_lane_run_that_emits_nothing_leaves_no_stale_artifact_to_be_judged', () => {
  // Last night's artifact plus tonight's crash must not read as tonight's pass.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-'));
  fs.mkdirSync(path.join(root, '.lanes/runs'), { recursive: true });
  fs.writeFileSync(path.join(root, '.lanes/runs/x.json'), JSON.stringify({ lane: 'x', series: {}, scalars: { a: 1 } }));
  runRecord(root, ['--lane', 'x'], 'console.log("boom"); process.exit(101)');
  assert.equal(fs.existsSync(path.join(root, '.lanes/runs/x.json')), false);
});

// --- canaries over the real tree --------------------------------------------

test('this_repos_own_register_reconciles_with_this_repos_own_tree', () => {
  const r = runCheck(REPO_ROOT);
  assert.equal(r.status, 0, r.stdout + r.stderr);
  assert.match(r.stdout, /flake register: \d+\/\d+ quarantined/);
});

test('the_source_scan_actually_found_this_tree', () => {
  // Liveness, the same shape ship-inventory.test.mjs uses: a walk that finds
  // nothing would satisfy every reconciliation above for the wrong reason.
  const pkgs = packages(REPO_ROOT);
  assert.ok(pkgs.length > 20, `only ${pkgs.length} packages found`);
  assert.ok(pkgs.some((p) => p.name === 'pumper-core'));
  assert.ok(pkgs.some((p) => p.name === 'pumper-server' && !p.hasLib));
  const scanned = scanIgnoredTests(REPO_ROOT, pkgs);
  assert.ok(scanned.length >= 15, `only ${scanned.length} #[ignore]d tests found`);
  assert.ok(scanned.some((s) => s.key === 'pumper-core::lib::distinct_hosts_run_in_parallel_but_each_host_spaces'));
  assert.ok(scanned.some((s) => s.key.startsWith('pumper-server::bin:pumper::shipped_')));
});
