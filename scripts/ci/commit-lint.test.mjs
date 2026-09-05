// The commit-message gate's own fixture suite, plus the two reconciliations
// that keep it from becoming a claim rather than a control.
//
// A gate that has never been observed to go red is indistinguishable from a
// gate that cannot (the reason `just harness-test` exists at all), so the first
// half of this file feeds it subjects that MUST be refused and asserts the exit
// code. The second half checks the wiring: that `.githooks/commit-msg` really
// delegates to the one implementation, that its `sh` fallback list still names
// the same types, and that the justfile's auto-install runs before the recipes
// a contributor actually types — because each of those is a place where the
// guardrail could quietly stop applying while every test here still passed.
//
// Run by `just inventory` (so `just ci` blocks on it) and by the `Ship
// inventory` CI job.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { TYPES, judgeSubject, lintSubjects, subjectFromMessage } from './commit-lint.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const SCRIPT = path.join(HERE, 'commit-lint.mjs');

/** Run the CLI the way a hook or a CI step does. */
function run(args, { input } = {}) {
  return spawnSync(process.execPath, [SCRIPT, ...args], {
    encoding: 'utf8',
    input: input ?? '',
    cwd: REPO_ROOT,
  });
}

function withMessageFile(text, fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'commit-lint-'));
  const file = path.join(dir, 'COMMIT_EDITMSG');
  fs.writeFileSync(file, text);
  try {
    return fn(file);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

// --- the rule itself ---------------------------------------------------------

test('conventional_subjects_pass', () => {
  const good = [
    'fix(api-surface): render the OpenAPI document once, not per request',
    'feat(crawl): add in-degree derivation over crawl/edges',
    'deps: bump the cargo minor/patch group',
    'chore: add supply-chain security starter',
    'refactor(engine-wasm)!: drop the v1 extract ABI',
    'ci(workflows/ci): split Format into its own job',
  ];
  for (const subject of good) {
    assert.equal(judgeSubject(subject).ok, true, `expected to pass: ${subject}`);
    assert.equal(judgeSubject(subject).kind, 'ok');
  }
});

test('unconventional_subjects_are_findings_not_warnings', () => {
  const bad = [
    'update stuff',
    'Fixed the thing',
    'wip',
    'feat add a route', // no colon
    'feat:no space after the colon',
    'feat:', // no description
    'nope(scope): a type that is not in the list',
    'FIX: shouting is a different type',
  ];
  for (const subject of bad) {
    const verdict = judgeSubject(subject);
    assert.equal(verdict.ok, false, `expected to be refused: ${subject}`);
    assert.equal(verdict.kind, 'malformed');
  }
});

test('git_generated_subjects_are_exempt_not_judged', () => {
  // Blocking these would break `git revert` and interactive rebase, which is how
  // a hook earns a permanent --no-verify.
  const generated = [
    'Merge branch master into feature',
    'Revert "feat(crawl): add in-degree derivation"',
    'fixup! fix(api-surface): render the document once',
    'squash! chore: tidy',
  ];
  const { judged, skipped, findings } = lintSubjects(generated);
  assert.equal(judged, 0);
  assert.equal(skipped, generated.length);
  assert.deepEqual(findings, []);
});

test('a_long_subject_warns_but_does_not_block', () => {
  const long = `fix(api-surface): ${'x'.repeat(80)}`;
  const verdict = judgeSubject(long);
  assert.equal(verdict.ok, true);
  assert.equal(verdict.warnings.length, 1);
});

test('subject_is_read_past_the_comment_block_git_appends', () => {
  const text = '# Please enter the commit message\nfeat(core): add a thing\n\n# On branch master\n';
  // git puts its instructions ABOVE nothing here, but a template can; the rule
  // is "first non-comment line", identical to the sh hook's grep -v '^#'.
  assert.equal(subjectFromMessage(text), 'feat(core): add a thing');
});

// --- the exit-code contract --------------------------------------------------

test('cli_exits_0_on_a_conventional_message_file', () => {
  withMessageFile('feat(core): add a thing\n\nbody\n', (file) => {
    const r = run(['--file', file]);
    assert.equal(r.status, 0, r.stderr);
  });
});

test('cli_exits_2_on_an_unconventional_message_file', () => {
  withMessageFile('update stuff\n', (file) => {
    const r = run(['--file', file]);
    assert.equal(r.status, 2, r.stdout + r.stderr);
    assert.match(r.stderr, /BLOCKED/);
  });
});

test('cli_exits_2_when_any_subject_on_stdin_is_unconventional', () => {
  const r = run([], { input: 'feat: a good one\nupdate stuff\nfix: another good one\n' });
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /update stuff/);
});

test('cli_exits_0_when_every_subject_on_stdin_is_conventional', () => {
  const r = run([], { input: 'feat: a good one\nfix(core): another\n' });
  assert.equal(r.status, 0, r.stderr);
});

test('cli_exits_3_when_it_cannot_check', () => {
  // Three shapes of cannot-check, and NONE of them may be reported as a pass:
  // a message file that is not there, a flag with no value, and empty input.
  assert.equal(run(['--file', path.join(os.tmpdir(), 'no-such-commit-msg')]).status, 3);
  assert.equal(run(['--range']).status, 3);
  assert.equal(run([], { input: '\n\n' }).status, 3);
});

test('cli_report_describes_the_rule_without_judging', () => {
  const r = run(['--report']);
  assert.equal(r.status, 0);
  assert.match(r.stdout, /conventional-commit shape/);
  for (const t of TYPES) assert.ok(r.stdout.includes(t), `--report should name the ${t} type`);
});

// --- the wiring: where this rule is actually applied -------------------------

test('the_commit_msg_hook_delegates_to_this_implementation', () => {
  const hook = fs.readFileSync(path.join(REPO_ROOT, '.githooks/commit-msg'), 'utf8');
  assert.match(
    hook,
    /scripts\/ci\/commit-lint\.mjs/,
    'the hook must run the shared implementation, not a second copy of the rule'
  );
});

test('the_hook_fallback_types_match_the_shared_list', () => {
  // EXPECTED-diff, the idiom this repo uses for "all sites agree" (see
  // crates/server/src/routes/mod.rs). The hook keeps an inline `sh` list for the
  // clone with no node on PATH; a type added here and not there would mean the
  // rule silently depends on which machine committed.
  const hook = fs.readFileSync(path.join(REPO_ROOT, '.githooks/commit-msg'), 'utf8');
  // `\s*$` rather than `$`: .gitattributes checks .githooks/* out with LF, but
  // this assertion must not become a line-ending test if that ever changes.
  const m = /^TYPES='([^']+)'\s*$/m.exec(hook);
  assert.ok(m, '.githooks/commit-msg must declare its fallback TYPES list');
  assert.deepEqual(m[1].split('|'), TYPES);
});

test('the_hooks_install_themselves_before_the_recipes_people_type', () => {
  // The gap this closes: hooks that only exist once someone remembers a setup
  // command are guardrails for whoever already knew about them. `_hooks-auto`
  // points core.hooksPath at .githooks/ on the first `just` invocation in a
  // clone, so every recipe below must depend on it — dropping the dependency is
  // exactly how the guardrail would go quiet again.
  const justfile = fs.readFileSync(path.join(REPO_ROOT, 'justfile'), 'utf8');
  const MUST_AUTO_INSTALL = ['check', 'build', 'test', 'lint', 'fmt', 'fmt-check', 'run', 'dev', 'ci'];
  const missing = MUST_AUTO_INSTALL.filter(
    (recipe) => !new RegExp(`^${recipe}(\\s+\\S+)*:.*\\b_hooks-auto\\b`, 'm').test(justfile)
  );
  assert.deepEqual(
    missing,
    [],
    `these recipes no longer auto-install the git hooks: ${missing.join(', ')}`
  );
});

test('ci_judges_commit_subjects_on_the_server_side', () => {
  // The rung no local configuration can skip. A contributor who never installs
  // the hooks, or who used --no-verify, still meets the rule here.
  const ci = fs.readFileSync(path.join(REPO_ROOT, '.github/workflows/ci.yml'), 'utf8');
  assert.match(ci, /commit-lint\.mjs/, 'CI must run the commit-message gate itself');
});
