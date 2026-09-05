#!/usr/bin/env node
// The branch-protection rule as a VERSIONED declaration, reconciled against the
// workflows that produce the checks it requires.
//
// WHY. Every other guardrail in this repo is checked out with the code: the hooks
// are in .githooks/, the pinning register is .github/unpinned-actions.json, the
// quarantine register is .flake/register.json. Branch protection was the one
// control that existed only in GitHub's settings UI — unversioned, invisible in
// review, and (per the header of .github/CODEOWNERS) the thing that has to be on
// before code-owner routing binds at all. .github/branch-protection.json is that
// declaration; this file is what keeps it honest, and `--json` is what applies it.
//
// WHAT IT CAN AND CANNOT CHECK. It cannot ask GitHub whether the rule is enabled —
// no network, and a worktree has no token. What it CAN check is the half that
// rots: whether the declared required checks still name checks the workflows
// actually produce. A required-check context that no job reports is the worst
// failure available, because it fails SILENTLY in both directions — GitHub either
// waits forever for a check nobody will send, or the stale name is simply never
// matched and the rung stops binding with the settings page still showing it.
// (.github/CODEOWNERS shipped exactly that list, and two of its six names were
// already wrong: `test` is really two matrix legs, and the inventory job had been
// renamed.)
//
// `--verify-live` is the OTHER half, and it needs a caller who has a token: hand
// it whatever GitHub actually returns for the branch and it reconciles the LIVE
// rule against this declaration. It is a pure function of the payload, so the
// network round trip lives entirely in the caller (`just protection-verify`, and
// the weekly `Ship inventory` steps) and nothing here has to be trusted blind.
// Until that ran, "declared" and "applied" were two different claims and only the
// first one was ever checked — a declaration nobody applied looks exactly like a
// rule that is on.
//
// Reconciliation runs in BOTH directions, for the reason pinned-actions.mjs and
// flake-check.mjs do it: a declaration that outlives what it describes stops
// being read. So a job in a gated workflow that this file classifies neither as
// required nor as deliberately-not-required is a finding — a NEW job must not be
// able to arrive un-required by omission.
//
// Three outcomes, three exit codes:
//   0  it checked, and the declaration still describes the workflows
//   2  it checked and found problems
//   3  it COULD NOT CHECK (no workflows, unreadable declaration) — NOT a pass
//
// `--report` prints the required-check list and the one command that applies it.
// `--json`   prints the GitHub branch-protection API payload, for that command.
// `--verify-live <file|->` judges a live payload fetched by the caller.
//
// Run by `just protection-check` and by the `Ship inventory` CI job.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

export const DECLARATION_PATH = '.github/branch-protection.json';

/** The settings that may not be weakened, and what each one costs if it is. */
const REQUIRED_SETTINGS = {
  strict: [true, 'a check that passed against a stale base is a check about a tree nobody is merging'],
  enforce_admins: [true, 'a rule an admin can walk around is a rule that binds only the people who were never the risk'],
  allow_force_pushes: [false, 'a force push rewrites the history every one of these checks was run against'],
  allow_deletions: [false, 'the protected branch itself must not be deletable'],
};

const unquote = (s) => s.replace(/^["'](.*)["']$/, '$1');

/**
 * Every job in one workflow's text: its id, its display `name:` if it sets one,
 * and its `matrix.os` legs if it fans out over them.
 *
 * Deliberately a line scan and not a YAML parse, for the reason pinned-actions.mjs
 * gives: every gate in scripts/ci/ must run on a clone with zero dependencies.
 * The three shapes it reads are all leaf scalars at fixed, unambiguous indents —
 * a job id is the only 2-space key under `jobs:`, a job's `name:` the only 4-space
 * one, and `matrix.os` the only 8-space `os:` list.
 */
export function jobsInWorkflow(text) {
  const jobs = [];
  let inJobs = false;
  let current = null;
  text.split('\n').forEach((line, i) => {
    if (/^jobs:\s*$/.test(line)) {
      inJobs = true;
      current = null;
      return;
    }
    // Any other column-0 key ends the jobs block.
    if (/^[A-Za-z_][\w-]*:/.test(line)) {
      inJobs = false;
      current = null;
      return;
    }
    if (!inJobs) return;

    const job = /^ {2}([A-Za-z_][\w-]*):\s*$/.exec(line);
    if (job) {
      current = { id: job[1], name: null, matrix: [], line: i + 1 };
      jobs.push(current);
      return;
    }
    if (!current) return;

    const name = /^ {4}name:\s*(.+?)\s*$/.exec(line);
    if (name) {
      current.name = unquote(name[1]);
      return;
    }
    const os = /^ {8}os:\s*\[(.+)\]\s*$/.exec(line);
    if (os) current.matrix = os[1].split(',').map((s) => unquote(s.trim())).filter(Boolean);
  });
  return jobs;
}

/**
 * The check contexts GitHub will report for a job — which is what a required-check
 * list has to name, character for character.
 *
 * A matrix job reports one context PER LEG, with the leg's value in parentheses:
 * requiring the bare job id there names nothing and blocks nothing. Returns null
 * when the job's `name:` interpolates an expression, because the string GitHub
 * ends up printing is not derivable from the file alone — those jobs are checked
 * for existence only, and their contexts are trusted as declared.
 */
export function contextsFor(job) {
  const base = job.name ?? job.id;
  if (base.includes('${{')) return null;
  if (job.matrix.length) return job.matrix.map((leg) => `${base} (${leg})`);
  return [base];
}

export function readWorkflows(root, files) {
  const out = [];
  for (const file of files) {
    try {
      out.push({ file, text: fs.readFileSync(path.join(root, file), 'utf8') });
    } catch {
      return null; // cannot check
    }
  }
  return out.length ? out : null;
}

export function readDeclaration(root) {
  try {
    const parsed = JSON.parse(fs.readFileSync(path.join(root, DECLARATION_PATH), 'utf8'));
    if (!parsed || !Array.isArray(parsed.gated_workflows) || !Array.isArray(parsed.required_checks)) {
      return null;
    }
    return parsed;
  } catch {
    return null; // cannot check
  }
}

const same = (a, b) => {
  const x = [...a].sort();
  const y = [...b].sort();
  return x.length === y.length && x.every((v, i) => v === y[i]);
};

/**
 * The whole judgement, as a pure function of the two inputs — so the fixture suite
 * can drive it without a checkout and prove this gate can still go red.
 *
 * @returns {{findings: string[], contexts: string[]}}
 */
export function judge(workflows, decl) {
  const findings = [];
  const byFile = new Map(workflows.map((w) => [w.file, jobsInWorkflow(w.text)]));
  const contexts = [];

  const notRequired = Array.isArray(decl.not_required) ? decl.not_required : [];
  const classified = new Set();

  const locate = (entry, kind) => {
    const jobs = byFile.get(entry.workflow);
    if (!jobs) {
      findings.push(
        `${DECLARATION_PATH} ${kind} \`${entry.job}\` in \`${entry.workflow}\`, which is not one of the gated_workflows read here.`
      );
      return null;
    }
    const job = jobs.find((j) => j.id === entry.job);
    if (!job) {
      findings.push(
        `${DECLARATION_PATH} ${kind} job \`${entry.job}\`, but ${entry.workflow} has no such job — ` +
          `a required check nothing reports leaves every pull request pending forever.`
      );
      return null;
    }
    classified.add(`${entry.workflow}#${entry.job}`);
    return job;
  };

  // Direction 1: every required check names a real job, with the exact context
  // string that job will report.
  for (const entry of decl.required_checks) {
    const job = locate(entry, 'requires');
    if (!job) continue;
    const declared = Array.isArray(entry.contexts) ? entry.contexts : [];
    if (declared.length === 0) {
      findings.push(`${DECLARATION_PATH} entry \`${entry.job}\` declares no \`contexts\`.`);
      continue;
    }
    const derived = contextsFor(job);
    if (derived && !same(derived, declared)) {
      findings.push(
        `${DECLARATION_PATH} requires ${JSON.stringify(declared)} for job \`${entry.job}\`, but that job ` +
          `reports ${JSON.stringify(derived)}. GitHub matches these by exact string, so the difference is ` +
          `a rung that silently stopped binding.`
      );
    }
    contexts.push(...declared);
  }

  for (const entry of notRequired) {
    const job = locate(entry, 'excuses');
    if (job && !entry.reason) {
      findings.push(`${DECLARATION_PATH} excuses job \`${entry.job}\` with no \`reason\`.`);
    }
  }

  // Direction 2: a job nobody classified. This is the one that catches a NEW job
  // arriving un-required by omission — the way a required-check list goes stale
  // without anyone editing it.
  for (const [file, jobs] of byFile) {
    for (const job of jobs) {
      if (classified.has(`${file}#${job.id}`)) continue;
      findings.push(
        `${file} defines job \`${job.id}\` (line ${job.line}) which ${DECLARATION_PATH} neither requires nor ` +
          `excuses. Add it to \`required_checks\`, or to \`not_required\` with a reason.`
      );
    }
  }

  const settings = decl.settings ?? {};
  for (const [key, [want, why]] of Object.entries(REQUIRED_SETTINGS)) {
    if (settings[key] !== want) {
      findings.push(`${DECLARATION_PATH} sets \`${key}\` to ${JSON.stringify(settings[key])}, expected ${want} — ${why}.`);
    }
  }

  return { findings, contexts: [...new Set(contexts)].sort() };
}

/**
 * The declaration as GitHub's branch-protection API wants it, so applying this
 * file is one pipe rather than a walk through a settings page. `restrictions` is
 * explicitly null: nobody is push-restricted, and omitting the key is an error.
 */
export function protectionPayload(decl, contexts) {
  const s = decl.settings ?? {};
  return {
    required_status_checks: { strict: s.strict === true, contexts },
    enforce_admins: s.enforce_admins === true,
    required_pull_request_reviews:
      s.required_approving_review_count > 0 || s.require_code_owner_reviews
        ? {
            required_approving_review_count: s.required_approving_review_count ?? 0,
            require_code_owner_reviews: s.require_code_owner_reviews === true,
            dismiss_stale_reviews: s.dismiss_stale_reviews === true,
          }
        : null,
    restrictions: null,
    required_linear_history: s.required_linear_history === true,
    required_conversation_resolution: s.required_conversation_resolution === true,
    allow_force_pushes: s.allow_force_pushes === true,
    allow_deletions: s.allow_deletions === true,
  };
}

/**
 * What GitHub actually has, flattened into the shape `protectionPayload` emits.
 *
 * Two payloads can arrive here and they answer different questions, so the shape
 * is detected rather than assumed:
 *
 *   `GET /repos/{o}/{r}/branches/{branch}/protection` — the whole rule. Needs a
 *     token with ADMIN on the repo; a workflow's own GITHUB_TOKEN cannot get it
 *     (there is no `administration:` scope to grant in a `permissions:` block).
 *   `GET /repos/{o}/{r}/branches/{branch}` — carries `protected: true|false` and
 *     nothing else that binds. Any read token can fetch it, which makes it the
 *     one live signal available with zero setup: it cannot confirm the required
 *     checks, but it turns "somebody switched protection off" red.
 *
 * The API returns `{enabled: bool}` objects where the PUT payload takes bare
 * booleans, so every flag is normalised through `on()` before anything compares
 * it — a rule read as `{"enabled": false}` and compared against `false` would
 * otherwise be judged as drift in the safe direction and never in the unsafe one.
 *
 * @returns {null | {shape: 'branch', protected: boolean} | {shape: 'protection', ...}}
 */
export function normalizeLive(live) {
  if (!live || typeof live !== 'object') return null;

  // The shallow payload first: it is the one with `protected` at the top level.
  if (typeof live.protected === 'boolean' && !live.required_status_checks) {
    return { shape: 'branch', protected: live.protected };
  }

  const on = (v) => (typeof v === 'boolean' ? v : !!v && typeof v === 'object' && v.enabled === true);
  const rsc = live.required_status_checks;
  if (!rsc && !live.enforce_admins && !live.required_linear_history) return null;

  // `contexts` is the deprecated flat list and `checks` the current one; GitHub
  // still returns both, but a rule written through the newer API can arrive with
  // only `checks`, and reading the wrong key reports every required check missing.
  const contexts = Array.isArray(rsc?.contexts)
    ? rsc.contexts
    : Array.isArray(rsc?.checks)
      ? rsc.checks.map((c) => c.context).filter(Boolean)
      : [];
  const rev = live.required_pull_request_reviews ?? null;

  return {
    shape: 'protection',
    required_status_checks: { strict: rsc?.strict === true, contexts },
    enforce_admins: on(live.enforce_admins),
    required_pull_request_reviews: rev
      ? {
          required_approving_review_count: rev.required_approving_review_count ?? 0,
          require_code_owner_reviews: rev.require_code_owner_reviews === true,
          dismiss_stale_reviews: rev.dismiss_stale_reviews === true,
        }
      : null,
    required_linear_history: on(live.required_linear_history),
    required_conversation_resolution: on(live.required_conversation_resolution),
    allow_force_pushes: on(live.allow_force_pushes),
    allow_deletions: on(live.allow_deletions),
  };
}

/** Every flag that must match, and what its drift costs — the message IS the gate. */
const LIVE_FLAGS = {
  enforce_admins: 'a rule an admin can walk around binds only the people who were never the risk',
  required_linear_history: 'a merge commit hides which tree the required checks actually ran against',
  required_conversation_resolution: 'an unresolved review thread stops being a blocker',
  allow_force_pushes: 'a force push rewrites the history every one of these checks was run against',
  allow_deletions: 'the protected branch itself becomes deletable',
};

/**
 * The live rule against the declaration — the half `judge()` cannot see.
 *
 * Compared against `protectionPayload(decl, contexts)` rather than against the
 * declaration's raw `settings`, so the verifier and the applier can never disagree
 * about what the declaration MEANS: whatever `just protection-apply` would install
 * is exactly what this expects to find.
 *
 * Reconciles in both directions, for the reason `judge()` does. A context GitHub
 * requires that this repo does not declare is as much a finding as one it declares
 * and GitHub does not: the first leaves pull requests pending on a check nobody
 * reports, the second is a rung that has quietly stopped binding.
 *
 * @returns {{findings: string[], partial: boolean, unreadable: boolean}}
 */
export function judgeLive(live, decl, contexts) {
  const norm = normalizeLive(live);
  if (!norm) return { findings: [], partial: false, unreadable: true };

  const branch = decl.branch ?? 'the protected branch';

  if (norm.shape === 'branch') {
    if (!norm.protected) {
      return {
        findings: [
          `GitHub reports \`${branch}\` as UNPROTECTED. Every rung in ${DECLARATION_PATH} is declared and ` +
            `none of it binds: apply it with \`just protection-apply\`.`,
        ],
        partial: false,
        unreadable: false,
      };
    }
    // Protected, and that is genuinely all this payload can say.
    return { findings: [], partial: true, unreadable: false };
  }

  const want = protectionPayload(decl, contexts);
  const findings = [];

  const have = new Set(norm.required_status_checks.contexts);
  for (const c of want.required_status_checks.contexts) {
    if (!have.has(c)) {
      findings.push(
        `GitHub does not require the check "${c}" on \`${branch}\`, but ${DECLARATION_PATH} declares it. ` +
          `That rung is running and reporting, and merging does not wait for it.`
      );
    }
  }
  const declared = new Set(want.required_status_checks.contexts);
  for (const c of norm.required_status_checks.contexts) {
    if (!declared.has(c)) {
      findings.push(
        `GitHub requires the check "${c}" on \`${branch}\`, which ${DECLARATION_PATH} does not declare — ` +
          `either the declaration is stale, or every pull request is waiting on a check no job reports.`
      );
    }
  }

  if (want.required_status_checks.strict !== norm.required_status_checks.strict) {
    findings.push(
      `GitHub has \`strict\` (require branches to be up to date) = ${norm.required_status_checks.strict}, ` +
        `declared ${want.required_status_checks.strict} — a check that passed against a stale base is a ` +
        `check about a tree nobody is merging.`
    );
  }

  for (const [key, why] of Object.entries(LIVE_FLAGS)) {
    if (want[key] !== norm[key]) {
      findings.push(`GitHub has \`${key}\` = ${norm[key]}, declared ${want[key]} — ${why}.`);
    }
  }

  const wantRev = want.required_pull_request_reviews;
  const haveRev = norm.required_pull_request_reviews;
  if (wantRev && !haveRev) {
    findings.push(
      `GitHub requires no pull-request review on \`${branch}\`, but ${DECLARATION_PATH} declares one — ` +
        `.github/CODEOWNERS routes a request nobody has to satisfy.`
    );
  } else if (!wantRev && haveRev) {
    findings.push(
      `GitHub requires pull-request review on \`${branch}\` (code owners: ${haveRev.require_code_owner_reviews}), ` +
        `which ${DECLARATION_PATH} does not declare — apply the declaration, or update it to say so.`
    );
  } else if (wantRev && haveRev) {
    for (const key of ['required_approving_review_count', 'require_code_owner_reviews', 'dismiss_stale_reviews']) {
      if (wantRev[key] !== haveRev[key]) {
        findings.push(`GitHub has \`${key}\` = ${haveRev[key]}, declared ${wantRev[key]}.`);
      }
    }
  }

  return { findings, partial: false, unreadable: false };
}

function readLive(arg) {
  try {
    const raw = arg === '-' || arg === undefined ? fs.readFileSync(0, 'utf8') : fs.readFileSync(arg, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function verifyLive(argv, decl, contexts) {
  const at = argv.indexOf('--verify-live');
  const arg = argv[at + 1] && !argv[at + 1].startsWith('--') ? argv[at + 1] : '-';
  const live = readLive(arg);
  if (live === null) {
    console.error(
      `protection-verify: CANNOT CHECK — no readable branch payload at \`${arg}\`. Fetch one with\n` +
        '  gh api repos/xkazm04/pumper/branches/master/protection   # needs ADMIN on the repo\n' +
        '  gh api repos/xkazm04/pumper/branches/master              # any read token; `protected` only'
    );
    return 3;
  }

  const { findings, partial, unreadable } = judgeLive(live, decl, contexts);
  if (unreadable) {
    console.error('protection-verify: CANNOT CHECK — that payload is neither a branch nor a branch-protection rule');
    return 3;
  }
  if (findings.length) {
    console.error('protection-verify: FINDINGS — the live rule and the declaration disagree');
    for (const f of findings) console.error(`  - ${f}`);
    return 2;
  }
  if (partial) {
    // A 3, deliberately. `protected: true` says a rule exists, not that it is
    // THIS rule — and reporting a half-answer as green is exactly how a
    // required-check list rots behind a settings page that still looks right.
    console.error(
      `protection-verify: CANNOT CHECK the required-check list — GitHub confirms \`${decl.branch}\` is ` +
        'protected, but reading WHICH checks it requires needs a token with admin rights on the repo.\n' +
        '  gh secret set PROTECTION_AUDIT_TOKEN   # a fine-grained PAT with Administration: read\n' +
        'Until then this is a partial verdict, and a partial verdict is not a pass.'
    );
    return 3;
  }
  console.log(
    `protection-verify: ok — GitHub's rule for \`${decl.branch}\` matches ${DECLARATION_PATH} ` +
      `(${contexts.length} required checks)`
  );
  return 0;
}

function main(argv) {
  const decl = readDeclaration(REPO_ROOT);
  if (!decl) {
    console.error(`protection-check: CANNOT CHECK — ${DECLARATION_PATH} is missing or malformed`);
    return 3;
  }
  const workflows = readWorkflows(REPO_ROOT, decl.gated_workflows);
  if (!workflows) {
    console.error('protection-check: CANNOT CHECK — a gated workflow named in the declaration is unreadable');
    return 3;
  }

  const { findings, contexts } = judge(workflows, decl);

  if (argv.includes('--verify-live')) {
    // The same refusal `--json` makes, for the same reason: a live rule judged
    // against a declaration that no longer describes the workflows would report
    // drift against a required-check list that is itself wrong.
    if (findings.length) {
      console.error('protection-verify: refusing to judge — the declaration itself has findings:');
      for (const f of findings) console.error(`  - ${f}`);
      return 2;
    }
    return verifyLive(argv, decl, contexts);
  }

  if (argv.includes('--json')) {
    // Refuses on findings rather than emitting them: a payload built from a
    // declaration that no longer matches the workflows would install exactly the
    // stale required-check list this gate exists to catch — and installing it is
    // the one action here that is hard to notice and hard to undo.
    if (findings.length) {
      console.error('protection-check: refusing to emit a payload — the declaration has findings:');
      for (const f of findings) console.error(`  - ${f}`);
      return 2;
    }
    console.log(JSON.stringify(protectionPayload(decl, contexts), null, 2));
    return 0;
  }

  if (argv.includes('--report')) {
    console.log(`branch: ${decl.branch}`);
    console.log(`required checks (${contexts.length}):`);
    for (const c of contexts) console.log(`  ${c}`);
    for (const e of decl.not_required ?? []) console.log(`  (not required) ${e.job} — ${e.reason}`);
    console.log('apply (needs admin rights on the repo):');
    console.log('  node scripts/ci/branch-protection.mjs --json |');
    console.log('    gh api -X PUT repos/xkazm04/pumper/branches/master/protection --input -');
    console.log('verify what GitHub actually has (`just protection-verify`):');
    console.log('  gh api repos/xkazm04/pumper/branches/master/protection |');
    console.log('    node scripts/ci/branch-protection.mjs --verify-live -');
    return 0;
  }

  if (findings.length) {
    console.error('protection-check: FINDINGS');
    for (const f of findings) console.error(`  - ${f}`);
    return 2;
  }
  console.log(
    `protection-check: ok — ${contexts.length} required checks still match the jobs in ` +
      `${decl.gated_workflows.join(', ')}`
  );
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exit(main(process.argv.slice(2)));
}
