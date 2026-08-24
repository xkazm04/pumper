// Tests for the long-lane certifier. `node --test scripts/ci/lane-certify.test.mjs`
// (also `just lanes-test`) — no dependencies, node:test only.
//
// A lane certifies nothing until it has been green on a known-good build AND red
// on a known-bad one. These are the planted reds for the certifier itself: every
// bound kind is driven past its declared limit from a fixture artifact, and every
// way of seeing nothing is checked to be spelled differently from a pass.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  certify,
  healthReport,
  judge,
  loadArtifact,
  percentile,
  secondHalfSlope,
  updateHealth,
} from './lane-certify.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const CERTIFY = path.join(HERE, 'lane-certify.mjs');

// --- the statistics ---------------------------------------------------------

test('a_percentile_is_a_value_the_run_actually_observed', () => {
  // Nearest-rank, not interpolated: escalating on a number no sample ever hit
  // is a poor way to argue with somebody about a latency budget.
  const v = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  assert.equal(percentile(v, 50), 5);
  assert.equal(percentile(v, 95), 10);
  assert.equal(percentile(v, 100), 10);
  assert.equal(percentile([], 95), null);
});

test('an_average_would_have_hidden_the_tail_the_lane_exists_to_see', () => {
  // 99 fast samples and one 5s stall: the mean is 55ms and reads fine.
  const v = [...Array(99).fill(5), 5000];
  const mean = v.reduce((a, b) => a + b, 0) / v.length;
  assert.ok(mean < 60);
  assert.equal(percentile(v, 99), 5);
  assert.equal(percentile(v, 100), 5000);
});

test('a_slope_over_the_second_half_separates_warm_up_from_growth', () => {
  // Warm-up: expensive early, flat after. The endpoint is high, the tail is flat.
  const warmup = [100, 80, 60, 40, 10, 10, 10, 10, 10, 10];
  assert.ok(Math.abs(secondHalfSlope(warmup)) < 0.001, String(secondHalfSlope(warmup)));
  // A leak: cheap early, climbing throughout. Same maximum, opposite meaning.
  const leak = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
  assert.ok(secondHalfSlope(leak) > 9, String(secondHalfSlope(leak)));
  assert.equal(secondHalfSlope([1, 2]), null);
});

// --- judging one criterion --------------------------------------------------

const ARTIFACT = {
  series: { hold_ms: [10, 10, 10, 10, 10, 11, 10, 12, 10, 11] },
  scalars: { starved: 0, banded_ms: 40, all_pairs_ms: 400, docs: 2000 },
};

test('a_criterion_inside_its_bound_passes_and_says_what_it_measured', () => {
  const v = judge({ kind: 'percentile', series: 'hold_ms', percentile: 95, max: 20 }, ARTIFACT);
  assert.equal(v.verdict, 'pass');
  assert.match(v.detail, /p95 of 10 samples = 12/);
});

test('a_planted_percentile_breach_reddens_the_lane', () => {
  const v = judge({ kind: 'percentile', series: 'hold_ms', percentile: 95, max: 11 }, ARTIFACT);
  assert.equal(v.verdict, 'fail');
  assert.equal(v.measured, 12);
});

test('a_planted_growth_slope_reddens_the_lane', () => {
  const leak = { series: { hold_ms: [10, 10, 10, 10, 10, 20, 30, 40, 50, 60] }, scalars: {} };
  const c = { kind: 'slope', series: 'hold_ms', maxPerSample: 1.0 };
  assert.equal(judge(c, ARTIFACT).verdict, 'pass');
  assert.equal(judge(c, leak).verdict, 'fail');
});

test('a_planted_scalar_and_ratio_breach_redden_the_lane', () => {
  assert.equal(judge({ kind: 'scalar', scalar: 'starved', max: 0 }, ARTIFACT).verdict, 'pass');
  assert.equal(judge({ kind: 'scalar', scalar: 'starved', max: -1 }, ARTIFACT).verdict, 'fail');
  assert.equal(judge({ kind: 'scalar', scalar: 'docs', min: 2000 }, ARTIFACT).verdict, 'pass');
  assert.equal(judge({ kind: 'scalar', scalar: 'docs', min: 2001 }, ARTIFACT).verdict, 'fail');
  const ratio = { kind: 'ratio', numerator: 'banded_ms', denominator: 'all_pairs_ms', max: 0.5 };
  assert.equal(judge(ratio, ARTIFACT).verdict, 'pass');
  assert.equal(judge({ ...ratio, max: 0.05 }, ARTIFACT).verdict, 'fail');
});

test('a_metric_the_artifact_never_carried_is_cannot_see_not_a_pass', () => {
  // The failure that matters most: a harness stops emitting a series and every
  // criterion over it silently starts passing.
  for (const c of [
    { kind: 'percentile', series: 'gone', percentile: 95, max: 1 },
    { kind: 'slope', series: 'gone', maxPerSample: 1 },
    { kind: 'scalar', scalar: 'gone', max: 1 },
    { kind: 'ratio', numerator: 'gone', denominator: 'all_pairs_ms', max: 1 },
    { kind: 'ratio', numerator: 'banded_ms', denominator: 'gone', max: 1 },
  ]) {
    assert.equal(judge(c, ARTIFACT).verdict, 'cannot-see', JSON.stringify(c));
  }
});

test('a_criterion_with_no_bound_certifies_nothing_and_says_so', () => {
  const v = judge({ kind: 'percentile', series: 'hold_ms', percentile: 95 }, ARTIFACT);
  assert.equal(v.verdict, 'cannot-see');
  assert.match(v.detail, /declares no bound/);
});

// --- artifacts and lanes ----------------------------------------------------

function artifactDir(files) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lanes-'));
  for (const [name, body] of Object.entries(files)) {
    fs.writeFileSync(path.join(dir, name), JSON.stringify(body));
  }
  return dir;
}

test('a_lane_measured_by_two_tests_is_merged_before_it_is_judged', () => {
  const dir = artifactDir({
    'x--a.json': { lane: 'x', part: 'a', scalars: { left: 2 }, series: {} },
    'x--b.json': { lane: 'x', part: 'b', scalars: { right: 4 }, series: {} },
  });
  const merged = loadArtifact('x', dir);
  assert.deepEqual(merged.scalars, { left: 2, right: 4 });
  assert.equal(merged.parts.length, 2);
});

test('a_lane_where_only_one_half_ran_is_cannot_see_not_a_pass_on_half_the_evidence', () => {
  const dir = artifactDir({ 'x--a.json': { lane: 'x', part: 'a', scalars: { left: 2 }, series: {} } });
  const criteria = {
    lanes: {
      x: { runsOn: ['linux'], criteria: [{ id: 'r', kind: 'ratio', numerator: 'left', denominator: 'right', max: 1 }] },
    },
  };
  assert.equal(certify(criteria, dir, 'linux')[0].verdict, 'cannot-see');
});

test('a_lane_that_produced_no_artifact_is_cannot_see_not_a_pass', () => {
  const dir = artifactDir({});
  const criteria = { lanes: { x: { runsOn: ['linux'], criteria: [{ id: 'r', kind: 'scalar', scalar: 'a', max: 1 }] } } };
  const r = certify(criteria, dir, 'linux')[0];
  assert.equal(r.verdict, 'cannot-see');
  assert.match(r.detail, /emitted NO artifact/);
});

test('a_lane_the_runner_cannot_host_is_cannot_run_never_a_pass_and_never_omitted', () => {
  const criteria = {
    lanes: { browser: { runsOn: [], unavailableReason: 'needs local Chrome', criteria: [] } },
  };
  const r = certify(criteria, artifactDir({}), 'linux');
  assert.equal(r.length, 1, 'the lane must still appear in the report');
  assert.equal(r[0].verdict, 'cannot-run');
  assert.match(r[0].detail, /needs local Chrome/);
});

test('a_lane_with_no_criteria_certifies_nothing_and_is_not_green', () => {
  // The exact shape this whole item was opened against: a harness that measures
  // and asserts nothing must not read as a passing lane.
  const dir = artifactDir({ 'x.json': { lane: 'x', scalars: { a: 1 }, series: {} } });
  const r = certify({ lanes: { x: { runsOn: ['linux'], criteria: [] } } }, dir, 'linux')[0];
  assert.equal(r.verdict, 'cannot-see');
  assert.match(r.detail, /declares no criteria/);
});

// --- lane health ------------------------------------------------------------

test('a_lane_that_has_never_passed_is_its_own_category_not_a_flaky_lane', () => {
  let health = { lanes: {} };
  for (let i = 0; i < 5; i++) {
    health = updateHealth(health, [{ lane: 'unbuilt', verdict: 'fail' }], { at: `2026-08-0${i + 1}`, sha: 'a' });
  }
  const line = healthReport(health)[0];
  assert.match(line, /NEVER GREEN — 0 of 5 attempted run\(s\) passed/);
  assert.match(line, /unbuilt lane wearing a gate's clothes/);
});

test('an_unobserved_lane_is_not_the_same_sentence_as_a_never_green_one', () => {
  assert.match(healthReport({ lanes: { fresh: { firstGreen: null, runs: [] } } })[0], /NO RUNS RECORDED/);
  const onlyCannotRun = { lanes: { chrome: { firstGreen: null, runs: [{ at: 'x', verdict: 'cannot-run' }] } } };
  assert.match(healthReport(onlyCannotRun)[0], /never attempted here/);
});

test('first_green_is_recorded_as_an_explicit_lane_event', () => {
  let health = { lanes: {} };
  health = updateHealth(health, [{ lane: 'x', verdict: 'fail' }], { at: '2026-08-01', sha: 'a' });
  health = updateHealth(health, [{ lane: 'x', verdict: 'pass' }], { at: '2026-08-02', sha: 'b' });
  health = updateHealth(health, [{ lane: 'x', verdict: 'pass' }], { at: '2026-08-03', sha: 'c' });
  assert.equal(health.lanes.x.firstGreen, '2026-08-02');
  assert.match(healthReport(health)[0], /first green 2026-08-02, 2\/3 attempted run\(s\) passed/);
});

// --- end to end, through the script's own exit codes ------------------------

function fakeRoot(criteria, runs) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'lane-root-'));
  fs.mkdirSync(path.join(root, '.lanes/runs'), { recursive: true });
  if (criteria !== null) {
    fs.writeFileSync(path.join(root, '.lanes/criteria.json'), JSON.stringify(criteria));
  }
  for (const [name, body] of Object.entries(runs || {})) {
    fs.writeFileSync(path.join(root, '.lanes/runs', name), JSON.stringify(body));
  }
  return root;
}

function runCertify(root, extra = []) {
  return spawnSync(process.execPath, [CERTIFY, '--platform', 'linux', ...extra], {
    encoding: 'utf8',
    env: { ...process.env, CLAUDE_PROJECT_DIR: root },
  });
}

const ONE_LANE = (max) => ({
  lanes: { x: { runsOn: ['linux'], criteria: [{ id: 'b', kind: 'scalar', scalar: 'a', max, predicate: 'a, measured once' }] } },
});

test('the_certifier_is_green_when_every_declared_bound_holds', () => {
  const root = fakeRoot(ONE_LANE(10), { 'x.json': { lane: 'x', scalars: { a: 5 }, series: {} } });
  const r = runCertify(root);
  assert.equal(r.status, 0, r.stdout + r.stderr);
  assert.match(r.stdout, /PASS/);
  assert.match(r.stdout, /predicate: a, measured once/);
});

test('the_certifier_goes_red_on_a_planted_criteria_breach', () => {
  const root = fakeRoot(ONE_LANE(1), { 'x.json': { lane: 'x', scalars: { a: 5 }, series: {} } });
  const r = runCertify(root);
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stdout, /FAIL/);
});

test('a_missing_artifact_exits_cannot_check_rather_than_green', () => {
  const root = fakeRoot(ONE_LANE(10), {});
  const r = runCertify(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  assert.match(r.stdout, /CANNOT-SEE/);
  assert.match(r.stderr, /"Found nothing" and "cannot see" are different/);
});

test('an_unreadable_or_empty_criteria_file_is_cannot_check_not_a_pass', () => {
  assert.equal(runCertify(fakeRoot(null, {})).status, 3);
  const empty = runCertify(fakeRoot({ lanes: {} }, {}));
  assert.equal(empty.status, 3);
  assert.match(empty.stderr, /zero lanes/);
});

test('the_certifier_writes_an_artifact_carrying_measurement_criteria_and_verdict', () => {
  // The verdict must be reproducible from the file alone — a dashboard that is a
  // sequence of verdicts with no measurements beside them cannot show a trend.
  const root = fakeRoot(ONE_LANE(10), { 'x.json': { lane: 'x', scalars: { a: 5 }, series: {} } });
  runCertify(root);
  const dir = path.join(root, '.lanes/verdicts');
  const files = fs.readdirSync(dir);
  assert.equal(files.length, 1);
  const written = JSON.parse(fs.readFileSync(path.join(dir, files[0]), 'utf8'));
  assert.equal(written.results[0].verdict, 'pass');
  assert.equal(written.results[0].criteria[0].max, 10);
  assert.equal(written.results[0].artifact.scalars.a, 5);
});

// --- canary over the repo's own criteria ------------------------------------

test('this_repos_criteria_file_declares_every_lane_it_claims_to_certify', () => {
  const criteria = JSON.parse(fs.readFileSync(path.join(REPO_ROOT, '.lanes/criteria.json'), 'utf8'));
  const lanes = Object.keys(criteria.lanes);
  assert.ok(lanes.length >= 6, `only ${lanes.length} lanes declared`);
  for (const [name, spec] of Object.entries(criteria.lanes)) {
    assert.ok(spec.command, `${name} declares no command`);
    assert.ok(Array.isArray(spec.runsOn), `${name} declares no runsOn`);
    if (spec.runsOn.length === 0) {
      assert.ok(spec.unavailableReason, `${name} is unavailable everywhere and does not say why`);
    } else {
      // Every runnable lane must have at least one bound, and every bound must
      // carry its predicate: "12% flaky" is not a finding and neither is "p95
      // under 400" with no statement of what was measured over what.
      assert.ok(spec.criteria.length > 0, `${name} is runnable and declares no criteria`);
      for (const c of spec.criteria) {
        assert.ok(c.id && c.predicate && c.basis, `${name}/${c.id || '?'} is missing id/predicate/basis`);
        assert.ok(
          typeof c.max === 'number' || typeof c.min === 'number' || typeof c.maxPerSample === 'number',
          `${name}/${c.id} declares no bound`
        );
      }
    }
  }
});
