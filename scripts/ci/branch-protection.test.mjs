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
  judgeLive,
  normalizeLive,
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

// --- the live rule, judged against the declaration ---------------------------
//
// `judge()` above can only see whether the declaration still describes the
// workflows. These cover the other half — whether GitHub HAS the rule — which is
// the half that was never checked at all: a declaration nobody applied reads
// exactly like a rule that is on, and both look green in a checkout.
//
// The payload shapes here are GitHub's, not this repo's: the read API answers
// with `{"enabled": true}` where the write API takes `true`, and a rule written
// through the newer API carries `checks` where the older one carried `contexts`.
// Reading either one wrong reports every required check as missing, so both are
// pinned by a fixture.

const CONTEXTS = ['Format', 'test (ubuntu-latest)', 'test (windows-latest)'];

/** What `GET /repos/{o}/{r}/branches/master/protection` returns for a rule that IS the declaration. */
function liveFor(d = decl(), over = {}) {
  const p = protectionPayload(d, CONTEXTS);
  return {
    required_status_checks: { strict: p.required_status_checks.strict, contexts: [...p.required_status_checks.contexts] },
    enforce_admins: { enabled: p.enforce_admins },
    ...(p.required_pull_request_reviews ? { required_pull_request_reviews: { ...p.required_pull_request_reviews } } : {}),
    required_linear_history: { enabled: p.required_linear_history },
    required_conversation_resolution: { enabled: p.required_conversation_resolution },
    allow_force_pushes: { enabled: p.allow_force_pushes },
    allow_deletions: { enabled: p.allow_deletions },
    ...over,
  };
}

const live = (over = {}, d = decl()) => judgeLive(liveFor(d, over), d, CONTEXTS);

test('an_enabled_object_is_read_as_the_boolean_the_payload_would_have_sent', () => {
  const n = normalizeLive(liveFor());
  assert.equal(n.shape, 'protection');
  assert.equal(n.enforce_admins, true, '{enabled: true} is not truthy-by-accident, it is read');
  assert.equal(normalizeLive(liveFor({ enforce_admins: { enabled: false } })).enforce_admins, false);
});

test('the_newer_checks_list_is_read_the_same_as_the_deprecated_contexts_list', () => {
  const payload = liveFor();
  payload.required_status_checks = { strict: true, checks: CONTEXTS.map((context) => ({ context, app_id: null })) };
  assert.deepEqual(normalizeLive(payload).required_status_checks.contexts, CONTEXTS);
  assert.deepEqual(judgeLive(payload, decl(), CONTEXTS).findings, []);
});

test('the_live_rule_that_matches_the_declaration_is_clean', () => {
  const { findings, partial, unreadable } = live();
  assert.deepEqual(findings, []);
  assert.equal(partial, false, 'a full protection payload is a full verdict');
  assert.equal(unreadable, false);
});

test('a_declared_check_github_does_not_require_is_a_finding', () => {
  // The failure this whole gate exists for: the rung runs, reports, goes red —
  // and nothing waits for it, because the settings page never learned its name.
  const { findings } = live({ required_status_checks: { strict: true, contexts: ['Format'] } });
  assert.equal(findings.length, 2);
  assert.match(findings.join('\n'), /does not require the check "test \(ubuntu-latest\)"/);
});

test('a_check_github_requires_that_nothing_declares_is_a_finding_too', () => {
  // Reconciliation in the other direction: GitHub waiting on a context no job
  // reports leaves every pull request pending forever, and says nothing about why.
  const { findings } = live({ required_status_checks: { strict: true, contexts: [...CONTEXTS, 'Ghost'] } });
  assert.equal(findings.length, 1);
  assert.match(findings[0], /requires the check "Ghost"/);
});

test('a_weakened_live_flag_is_a_finding_per_flag', () => {
  const weakened = [
    ['enforce_admins', { enabled: false }],
    ['allow_force_pushes', { enabled: true }],
    ['allow_deletions', { enabled: true }],
    ['required_linear_history', { enabled: false }],
    ['required_conversation_resolution', { enabled: false }],
  ];
  for (const [key, bad] of weakened) {
    const { findings } = live({ [key]: bad });
    assert.equal(findings.length, 1, `${key} produced ${findings.length} findings`);
    assert.match(findings[0], new RegExp(`\`${key}\``));
  }
  const stale = live({ required_status_checks: { strict: false, contexts: CONTEXTS } });
  assert.equal(stale.findings.length, 1);
  assert.match(stale.findings[0], /`strict`/);
});

test('code_owner_review_declared_but_not_applied_is_a_finding', () => {
  // The day a second maintainer joins, CONTRIBUTING.md §8 says to flip this field
  // AND re-apply. This is the gate that catches the second step being forgotten —
  // otherwise CODEOWNERS goes back to describing a routing nothing enforces, with
  // the declaration now claiming it does.
  const d = decl();
  d.settings.require_code_owner_reviews = true;
  d.settings.required_approving_review_count = 1;
  const payload = liveFor(d);
  delete payload.required_pull_request_reviews; // applied before the flip
  const { findings } = judgeLive(payload, d, CONTEXTS);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /requires no pull-request review/);
});

test('review_settings_that_drift_from_the_declaration_are_findings', () => {
  const d = decl();
  d.settings.require_code_owner_reviews = true;
  d.settings.required_approving_review_count = 1;
  const payload = liveFor(d, {
    required_pull_request_reviews: {
      required_approving_review_count: 1,
      require_code_owner_reviews: false,
      dismiss_stale_reviews: true,
    },
  });
  const { findings } = judgeLive(payload, d, CONTEXTS);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /`require_code_owner_reviews`/);
});

test('an_unprotected_branch_is_a_finding_from_the_shallow_payload_alone', () => {
  // The one live signal any read token can get. It cannot confirm the rule, but
  // "somebody turned protection off" is the largest single regression available
  // here, and this is what turns it red without a maintainer-scoped secret.
  const { findings, partial } = judgeLive({ name: 'master', protected: false }, decl(), CONTEXTS);
  assert.equal(partial, false);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /UNPROTECTED/);
});

test('a_protected_flag_alone_is_partial_not_a_pass', () => {
  // `protected: true` says A rule exists, not that it is THIS rule. Reporting
  // that as clean is precisely the 3-treated-as-0 the exit-code convention exists
  // to prevent, so the verdict carries `partial` and the CLI exits 3 on it.
  const { findings, partial } = judgeLive({ name: 'master', protected: true }, decl(), CONTEXTS);
  assert.deepEqual(findings, []);
  assert.equal(partial, true, 'a half-answer must not read as a pass');
});

test('a_payload_that_is_neither_shape_is_cannot_check_not_clean', () => {
  for (const junk of [null, 'nope', {}, { message: 'Not Found' }]) {
    const { unreadable, findings } = judgeLive(junk, decl(), CONTEXTS);
    assert.equal(unreadable, true, `${JSON.stringify(junk)} must not read as a verdict`);
    assert.deepEqual(findings, []);
  }
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
