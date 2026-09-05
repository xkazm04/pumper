#!/usr/bin/env node
// The conventional-commit shape, as ONE implementation that both the local hook
// and CI run.
//
// WHY THIS EXISTS AT ALL. `.githooks/commit-msg` already enforced the shape —
// but git hooks are per-clone and `.git/hooks` is not versioned, so on a clone
// that never ran `just hooks-install` the rule refused nothing. That is the
// classic local-only guardrail: real for whoever set it up, invisible for
// everyone else, and indistinguishable from enforcement when you read the repo.
// The justfile now points `core.hooksPath` at `.githooks/` on the first `just`
// invocation in a clone (see `_hooks-auto`), which closes the setup gap for
// anyone using the canonical runner; this file closes the remaining one, by
// giving CI a rung that judges the same subjects on the server side, where no
// local configuration and no `--no-verify` can reach.
//
// Deliberately no commitlint: that is a Node toolchain, a config file and a
// lockfile to keep current, for a regex. The types below are the ones this
// history actually uses plus the standard set — and they live HERE, with the
// hook delegating to this file, so the two can no longer drift apart. A test
// (`commit-lint.test.mjs`) reconciles the hook's `sh` fallback list against this
// one in the EXPECTED-diff idiom, because a convention stated twice is a
// convention that will eventually be stated differently.
//
// Three outcomes, three exit codes — the shape every gate in scripts/ci/ uses:
//   0  it checked, and every subject it judged is conventional
//   2  it checked and found problems
//   3  it COULD NOT CHECK (no input, unreadable message file, git range that
//      would not resolve) — NOT a pass
//
// Input, in the order the CLI looks for it:
//   --file <path>      one commit MESSAGE file (what commit-msg receives)
//   --range <A..B>     every subject in a git range (what the CI job passes)
//   <stdin>            one subject per line (`git log --format=%s | ...`)
//
// `--report` prints the rule instead of judging anything.
//
// Run by `.githooks/commit-msg`, by `just commit-lint`, and by the
// `Conventional commits` step of the `Ship inventory` CI job.

import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

/** The accepted commit types. The single definition — `.githooks/commit-msg`
 * carries a fallback copy for the case where node is not on PATH, and
 * `commit-lint.test.mjs` fails if the two lists ever differ. */
export const TYPES = [
  'feat',
  'fix',
  'docs',
  'style',
  'refactor',
  'perf',
  'test',
  'build',
  'ci',
  'chore',
  'revert',
  'deps',
];

/** `git log --oneline` and every GitHub view truncate here. A WARNING, not a
 * finding: a long subject is a readability cost, not a correctness one, and
 * blocking on it is how a gate earns a permanent `--no-verify`. */
export const SUBJECT_MAX = 72;

/** Subjects git itself generates. Rejecting these would block `git revert` and
 * interactive rebase for no benefit. */
const GENERATED = /^(Merge |Revert "|Revert |fixup! |squash! |amend! )/;

export function subjectPattern() {
  return new RegExp(`^(${TYPES.join('|')})(\\([a-z0-9._/-]+\\))?!?: .+`);
}

/**
 * Judge ONE subject line.
 *
 * `kind` distinguishes the three ways a subject can be acceptable, because
 * "passed" and "was never judged" are different facts and a gate that reports
 * them identically cannot show that it did any work: `ok` (conventional),
 * `generated` (git wrote it), `empty` (git aborts the commit on its own, so a
 * message here would only be noise on top of that).
 */
export function judgeSubject(subject) {
  const s = (subject ?? '').replace(/\r$/, '');
  if (s.trim() === '') return { ok: true, kind: 'empty', subject: s, warnings: [] };
  if (GENERATED.test(s)) return { ok: true, kind: 'generated', subject: s, warnings: [] };

  const warnings = [];
  if (s.length > SUBJECT_MAX) {
    warnings.push(`subject is ${s.length} chars; it will be truncated at ${SUBJECT_MAX} in most views`);
  }
  if (!subjectPattern().test(s)) {
    return { ok: false, kind: 'malformed', subject: s, warnings };
  }
  return { ok: true, kind: 'ok', subject: s, warnings };
}

/** Judge a list of subjects. Pure — every caller below turns some input shape
 * into this list first, so the verdict never depends on where the input came
 * from. */
export function lintSubjects(subjects) {
  const findings = [];
  const warnings = [];
  let judged = 0;
  let skipped = 0;
  for (const subject of subjects) {
    const verdict = judgeSubject(subject);
    if (verdict.kind === 'generated' || verdict.kind === 'empty') {
      skipped += 1;
      continue;
    }
    judged += 1;
    for (const w of verdict.warnings) warnings.push({ subject: verdict.subject, warning: w });
    if (!verdict.ok) findings.push({ subject: verdict.subject });
  }
  return { judged, skipped, findings, warnings };
}

/**
 * The subject of a commit MESSAGE file: the first line that is not part of the
 * comment block git appends. Same rule the hook's `grep -v '^#' | sed -n 1p`
 * applied, kept identical so delegating to this file cannot change a verdict.
 */
export function subjectFromMessage(text) {
  const lines = text.split('\n').filter((l) => !l.startsWith('#'));
  return (lines[0] ?? '').replace(/\r$/, '');
}

function explain(findings) {
  const out = [];
  out.push('');
  out.push('commit-lint: BLOCKED — these subjects are not conventional commits.');
  out.push('');
  for (const f of findings) out.push(`  got:      ${f.subject}`);
  out.push('');
  out.push('  expected: <type>[(scope)][!]: <description>');
  out.push(`  types:    ${TYPES.join(' ')}`);
  out.push('');
  out.push('  examples: fix(api-surface): render the OpenAPI document once, not per request');
  out.push('            feat(crawl): add in-degree derivation over crawl/edges');
  out.push('            deps: bump the cargo minor/patch group');
  out.push('');
  out.push('  local bypass: PUMPER_SKIP_HOOKS=1 git commit ...   (CI still judges it)');
  return out.join('\n');
}

function report() {
  const lines = [
    'commit-lint — the conventional-commit shape this repo enforces.',
    '',
    `  pattern:  ^(${TYPES.join('|')})(\\(scope\\))?!?: <description>`,
    `  types:    ${TYPES.join(' ')}`,
    `  scope:    optional, [a-z0-9._/-]+`,
    `  breaking: optional \`!\` before the colon`,
    `  subject:  warns over ${SUBJECT_MAX} chars, never blocks on length`,
    '',
    '  exempt:   Merge/Revert/fixup!/squash!/amend! (git writes those itself)',
    '',
    '  enforced: .githooks/commit-msg (local, once hooks are installed)',
    '            the `Conventional commits` CI step (server side, per PR)',
  ];
  return lines.join('\n');
}

function subjectsFromRange(range) {
  const out = execFileSync('git', ['log', '--format=%s', range], {
    encoding: 'utf8',
    cwd: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..'),
  });
  return out.split('\n').filter((l) => l.trim() !== '');
}

function readStdin() {
  if (process.stdin.isTTY) return null;
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return null;
  }
}

/** Exit codes are the contract, so `main` returns one rather than calling
 * `process.exit` from three places. */
export function main(argv = process.argv.slice(2)) {
  if (argv.includes('--help') || argv.includes('-h')) {
    console.log(report());
    return 0;
  }
  if (argv.includes('--report')) {
    console.log(report());
    return 0;
  }

  let subjects;
  const fileAt = argv.indexOf('--file');
  const rangeAt = argv.indexOf('--range');
  if (fileAt !== -1) {
    const p = argv[fileAt + 1];
    if (!p) {
      console.error('commit-lint: CANNOT CHECK — --file needs a path');
      return 3;
    }
    let text;
    try {
      text = fs.readFileSync(p, 'utf8');
    } catch (err) {
      console.error(`commit-lint: CANNOT CHECK — cannot read ${p}: ${err.message}`);
      return 3;
    }
    subjects = [subjectFromMessage(text)];
  } else if (rangeAt !== -1) {
    const range = argv[rangeAt + 1];
    if (!range) {
      console.error('commit-lint: CANNOT CHECK — --range needs a revision range');
      return 3;
    }
    try {
      subjects = subjectsFromRange(range);
    } catch (err) {
      console.error(`commit-lint: CANNOT CHECK — \`git log ${range}\` failed: ${err.message}`);
      return 3;
    }
    // An empty range is not a pass: it means the range resolved to nothing,
    // which is what a wrong base ref looks like from here.
    if (subjects.length === 0) {
      console.error(`commit-lint: CANNOT CHECK — ${range} contains no commits`);
      return 3;
    }
  } else {
    const stdin = readStdin();
    if (stdin === null) {
      console.error('commit-lint: CANNOT CHECK — no --file, no --range, and no subjects on stdin');
      return 3;
    }
    subjects = stdin.split('\n').filter((l) => l.trim() !== '');
    if (subjects.length === 0) {
      console.error('commit-lint: CANNOT CHECK — stdin held no subjects');
      return 3;
    }
  }

  const { judged, skipped, findings, warnings } = lintSubjects(subjects);
  for (const w of warnings) console.error(`commit-lint: note — ${w.warning}`);
  if (findings.length > 0) {
    console.error(explain(findings));
    return 2;
  }
  console.log(
    `commit-lint: ${judged} subject${judged === 1 ? '' : 's'} conventional` +
      (skipped > 0 ? ` (${skipped} generated by git, not judged)` : '')
  );
  return 0;
}

const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) process.exit(main());
