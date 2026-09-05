// The branch-protection gate's fixture suite: proof that it can still go RED.
//
// This gate lands green by construction (the declaration was written from the
// workflows it describes), so "it passed" carries no information about whether it
// works — the same argument pinned-actions.test.mjs makes. The fixtures below
// drive `judge()` directly with synthetic workflows, which is why the gate's whole
// judgement is a pure function of its two inputs.
//
// The last two tests are liveness against this repo's REAL files: if either fails,
// `just protection-check` is red and the message says which side moved.
//
// Run by `just inventory` and by the `Ship inventory` CI job.

import assert from 'node:assert/strict';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  DECLARATION_PATH,
  contextsFor,
  jobsInWorkflow,
  judge,
  protectionPayload,
  readDeclaration,
  readWorkflows,
} from './branch-protection.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

const FIXTURE = '.github/workflows/fixture.yml';
const wf = (text) => [{ file: FIXTURE, text }];

const WORKFLOW = [
  'name: CI',
  'on:',
  '  pull_request:',
  'permissions:',
  '  contents: read',
  'jobs:',
  '  format:',
  '    name: Format',
  '    runs-on: ubuntu-latest',
  '    steps:',
  '      - name: Format',
  '        run: cargo fmt --check',
  '  test:',
  '    runs-on: ${{ matrix.os }}',
  '    strategy:',
  '      matrix:',
  '        os: [ubuntu-latest, windows-latest]',
  '    steps:',
  '      - run: cargo test',
].join('\n');

// A top-level key AFTER `jobs:`, as the real workflow has: it must end the jobs
// block rather than read as another job.
const WORKFLOW_WITH_ENV = `${WORKFLOW}\nenv:\n  CARGO_TERM_COLOR: always\n`;

/** The same workflow with one more job appended — the "a job was added" fixture. */
const plusJob = (lines) => `${WORKFLOW}\n${lines.join('\n')}\n`;

const SETTINGS = {
  strict: true,
  enforce_admins: true,
  required_approving_review_count: 0,
  require_code_owner_reviews: false,
  dismiss_stale_reviews: true,
  required_linear_history: true,
  required_conversation_resolution: true,
  allow_force_pushes: false,
  allow_deletions: false,
};

const decl = (over = {}) => ({
  branch: 'master',
  gated_workflows: [FIXTURE],
  required_checks: [
    { job: 'format', workflow: FIXTURE, contexts: ['Format'] },
    { job: 'test', workflow: FIXTURE, contexts: ['test (ubuntu-latest)', 'test (windows-latest)'] },
  ],
  not_required: [],
  settings: { ...SETTINGS },
  ...over,
});

test('jobs_are_read_by_id_name_and_matrix_legs_not_by_step', () => {
  const jobs = jobsInWorkflow(WORKFLOW);
  assert.deepEqual(jobs.map((j) => j.id), ['format', 'test']);
  // `name:` at step level (6 spaces) must not be mistaken for the job's own.
  assert.equal(jobs[0].name, 'Format');
  assert.equal(jobs[1].name, null, 'a job with no `name:` is identified by its id');
  assert.deepEqual(jobs[1].matrix, ['ubuntu-latest', 'windows-latest']);
});

test('a_top_level_key_after_jobs_does_not_become_a_job', () => {
  // `env:` sits below `jobs:` in the real workflow, so this is not hypothetical.
  assert.deepEqual(jobsInWorkflow(WORKFLOW_WITH_ENV).map((j) => j.id), ['format', 'test']);
});

test('a_matrix_job_reports_one_context_per_leg_not_its_bare_id', () => {
  const [format, matrix] = jobsInWorkflow(WORKFLOW);
  assert.deepEqual(contextsFor(format), ['Format']);
  assert.deepEqual(contextsFor(matrix), ['test (ubuntu-latest)', 'test (windows-latest)']);
});

test('a_dynamic_job_name_is_not_guessed_at', () => {
  assert.equal(contextsFor({ id: 'release', name: 'Binary (${{ matrix.target }})', matrix: [] }), null);
});

test('the_declaration_that_matches_the_workflow_is_clean', () => {
  const { findings, contexts } = judge(wf(WORKFLOW), decl());
  assert.deepEqual(findings, []);
  assert.deepEqual(contexts, ['Format', 'test (ubuntu-latest)', 'test (windows-latest)']);
});

test('requiring_a_matrix_job_by_its_bare_id_is_a_finding_not_a_near_miss', () => {
  // The exact drift that shipped in CODEOWNERS: `test` names no check GitHub ever
  // reports, so the rung reads as required and binds nothing.
  const d = decl();
  d.required_checks[1].contexts = ['test'];
  const { findings } = judge(wf(WORKFLOW), d);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /reports \["test \(ubuntu-latest\)"/);
});

test('a_renamed_job_turns_its_required_check_red', () => {
  const { findings } = judge(wf(WORKFLOW.replace('    name: Format', '    name: Formatting')), decl());
  assert.equal(findings.length, 1);
  assert.match(findings[0], /reports \["Formatting"\]/);
});

test('a_required_check_for_a_job_that_no_longer_exists_is_a_finding', () => {
  const d = decl();
  d.required_checks.push({ job: 'ghost', workflow: FIXTURE, contexts: ['Ghost'] });
  const { findings } = judge(wf(WORKFLOW), d);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /has no such job/);
});

test('a_new_job_nobody_classified_is_a_finding_not_a_shrug', () => {
  // Reconciliation in the OTHER direction: this is what stops a job arriving
  // un-required by omission, which is how a required-check list goes stale with
  // nobody editing it.
  const text = plusJob(['  audit:', '    name: Dependency audit', '    runs-on: ubuntu-latest']);
  const { findings } = judge(wf(text), decl());
  assert.equal(findings.length, 1);
  assert.match(findings[0], /neither requires nor excuses/);
});

test('a_job_excused_with_a_reason_is_allowed_to_stay_off_the_gate', () => {
  const text = plusJob(['  lanes:', '    name: Long lanes (nightly)', '    runs-on: ubuntu-latest']);
  const d = decl({ not_required: [{ job: 'lanes', workflow: FIXTURE, reason: 'nightly certification' }] });
  assert.deepEqual(judge(wf(text), d).findings, []);
});

test('an_excuse_without_a_reason_is_a_finding', () => {
  const text = plusJob(['  lanes:', '    runs-on: ubuntu-latest']);
  const d = decl({ not_required: [{ job: 'lanes', workflow: FIXTURE }] });
  const { findings } = judge(wf(text), d);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /no `reason`/);
});

test('weakening_a_settings_field_is_a_finding_per_field', () => {
  for (const [key, bad] of [['strict', false], ['enforce_admins', false], ['allow_force_pushes', true], ['allow_deletions', true]]) {
    const d = decl();
    d.settings[key] = bad;
    const { findings } = judge(wf(WORKFLOW), d);
    assert.equal(findings.length, 1, `weakening ${key} produced ${findings.length} findings`);
    assert.match(findings[0], new RegExp(`\`${key}\``));
  }
});

test('the_api_payload_omits_reviews_while_there_is_one_maintainer', () => {
  const p = protectionPayload(decl(), ['Format']);
  assert.equal(p.required_pull_request_reviews, null, 'a lone maintainer cannot approve their own PR');
  assert.deepEqual(p.required_status_checks, { strict: true, contexts: ['Format'] });
  assert.equal(p.restrictions, null, 'the API rejects a payload that omits this key');
  assert.equal(p.enforce_admins, true);
});

test('the_api_payload_carries_code_owner_review_once_it_is_turned_on', () => {
  // The one edit CONTRIBUTING.md tells a second maintainer to make.
  const d = decl();
  d.settings.require_code_owner_reviews = true;
  d.settings.required_approving_review_count = 1;
  const p = protectionPayload(d, []);
  assert.equal(p.required_pull_request_reviews.require_code_owner_reviews, true);
  assert.equal(p.required_pull_request_reviews.required_approving_review_count, 1);
});

test('this_repos_declaration_still_describes_its_workflows', () => {
  const d = readDeclaration(REPO_ROOT);
  assert.ok(d, `${DECLARATION_PATH} is missing or malformed`);
  const workflows = readWorkflows(REPO_ROOT, d.gated_workflows);
  assert.ok(workflows, 'a gated workflow named in the declaration is unreadable');
  const { findings } = judge(workflows, d);
  assert.deepEqual(findings, [], findings.join('\n'));
});

test('the_codeowners_header_names_the_same_checks_the_declaration_requires', () => {
  // CODEOWNERS tells a reader which checks to require alongside code-owner review.
  // It shipped a list that was already wrong in two places, and nothing could have
  // noticed: prose in a comment is the one part of a guardrail no gate reads.
  const d = readDeclaration(REPO_ROOT);
  const workflows = readWorkflows(REPO_ROOT, d.gated_workflows);
  const { contexts } = judge(workflows, d);
  const owners = readWorkflows(REPO_ROOT, ['.github/CODEOWNERS'])[0].text;
  for (const c of contexts) {
    assert.ok(
      owners.includes(c),
      `.github/CODEOWNERS does not name the required check "${c}" — its header list has drifted from ${DECLARATION_PATH}`
    );
  }
});
