// The pinning gate's fixture suite: proof that it can still go RED.
//
// A gate never observed failing is indistinguishable from a gate that cannot
// fail, and this one lands green by construction (every reference floating today
// is registered), so "it passed" carries no information about whether it works.
// The fixtures below drive `judge()` directly with synthetic workflows, which is
// why the gate's whole judgement is a pure function of its two inputs.
//
// Run by `just pin-check-test` and by the `Ship inventory` CI job.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  REGISTER_PATH,
  actionName,
  isLocal,
  isPinned,
  judge,
  readRegister,
  readWorkflows,
  usesInWorkflow,
} from './pinned-actions.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

const SHA = 'a'.repeat(40);
const wf = (text) => [{ file: '.github/workflows/fixture.yml', text }];
const register = (waived, ceiling = 10) => ({
  ceiling,
  waived: waived.map((action) => ({ action, reason: 'fixture', owner: 'nobody' })),
});

test('a_uses_line_is_found_with_or_without_the_sequence_dash', () => {
  const found = usesInWorkflow(['      - uses: actions/checkout@v7', '        uses: foo/bar@v1'].join('\n'));
  assert.deepEqual(found.map((u) => u.ref), ['actions/checkout@v7', 'foo/bar@v1']);
  assert.deepEqual(found.map((u) => u.line), [1, 2]);
});

test('only_a_forty_hex_ref_counts_as_pinned', () => {
  assert.equal(isPinned(`actions/checkout@${SHA}`), true);
  assert.equal(isPinned('actions/checkout@v7'), false, 'a tag can be re-pointed');
  assert.equal(isPinned('dtolnay/rust-toolchain@stable'), false, 'a branch moves every push');
  assert.equal(isPinned('actions/checkout@abc123'), false, 'a short sha is not the ref GitHub resolves');
  assert.equal(isPinned('actions/checkout'), false);
});

test('a_local_composite_action_is_not_third_party_code', () => {
  assert.equal(isLocal('./.github/actions/build'), true);
  assert.equal(isLocal('docker://alpine@sha256:deadbeef'), true);
  assert.equal(isLocal('actions/checkout@v7'), false);
});

test('the_action_identity_drops_the_ref', () => {
  assert.equal(actionName('actions/cache/restore@v6'), 'actions/cache/restore');
  assert.equal(actionName('actions/checkout'), 'actions/checkout');
});

test('a_new_floating_action_is_a_finding_not_a_shrug', () => {
  const { findings } = judge(wf('    - uses: evil/action@v1\n'), register([]));
  assert.equal(findings.length, 1);
  assert.match(findings[0], /evil\/action@v1/);
  assert.match(findings[0], /Pin it to a commit SHA/);
});

test('a_registered_floating_action_is_allowed_through', () => {
  const { findings, unpinned } = judge(wf('    - uses: actions/checkout@v7\n'), register(['actions/checkout']));
  assert.deepEqual(findings, []);
  assert.equal(unpinned.length, 1);
});

test('a_pinned_action_needs_no_waiver', () => {
  const { findings, pinned } = judge(wf(`    - uses: actions/checkout@${SHA}\n`), register([]));
  assert.deepEqual(findings, []);
  assert.equal(pinned, 1);
});

test('a_waiver_the_tree_outgrew_is_a_finding_not_a_leftover', () => {
  // Reconciliation in the OTHER direction. Without it the register accumulates
  // rows nobody can act on, its ceiling stops describing anything, and the whole
  // document quietly becomes decoration.
  const { findings } = judge(wf(`    - uses: actions/checkout@${SHA}\n`), register(['actions/checkout']));
  assert.equal(findings.length, 1);
  assert.match(findings[0], /no workflow uses it unpinned any more/);
});

test('the_ceiling_blocks_growth_it_does_not_merely_record_it', () => {
  const text = ['    - uses: a/one@v1', '    - uses: a/two@v1', '    - uses: a/three@v1'].join('\n');
  const reg = register(['a/one', 'a/two', 'a/three'], 2);
  const { findings } = judge(wf(text), reg);
  assert.equal(findings.length, 1);
  assert.match(findings[0], /ceiling is 2/);
});

test('a_waiver_without_a_reason_or_owner_is_a_finding', () => {
  const { findings } = judge(wf('    - uses: a/one@v1\n'), { ceiling: 9, waived: [{ action: 'a/one' }] });
  assert.equal(findings.length, 1);
  assert.match(findings[0], /missing `reason` or `owner`/);
});

test('this_repos_own_workflows_and_register_reconcile', () => {
  // Liveness plus the real verdict: if this fails, `just pin-check` is red and
  // the message says which side moved.
  const workflows = readWorkflows(REPO_ROOT);
  assert.ok(workflows && workflows.length > 0, 'no workflows found — the walk is looking in the wrong place');
  const reg = readRegister(REPO_ROOT);
  assert.ok(reg, `${REGISTER_PATH} is missing or malformed`);
  const { findings } = judge(workflows, reg);
  assert.deepEqual(findings, [], findings.join('\n'));
});

test('every_workflow_declares_a_permissions_block', () => {
  // The other half of the same supply-chain surface: a workflow with no
  // `permissions:` runs every third-party action above with whatever the
  // repository default grants, which has historically been a write-capable
  // GITHUB_TOKEN. Asserted here rather than trusted, because the failure mode is
  // a NEW workflow file that simply forgets — invisible in review, and invisible
  // in the logs of every run that never needed the extra scope.
  const dir = path.join(REPO_ROOT, '.github/workflows');
  const files = fs.readdirSync(dir).filter((f) => /\.ya?ml$/.test(f));
  assert.ok(files.length > 0);
  const missing = files.filter(
    (f) => !/^permissions:\s*$/m.test(fs.readFileSync(path.join(dir, f), 'utf8'))
  );
  assert.deepEqual(
    missing,
    [],
    `these workflows have no top-level \`permissions:\` block, so they inherit the repository default:\n  ${missing.join('\n  ')}`
  );
});
