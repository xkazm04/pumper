#!/usr/bin/env node
// The action-pinning register as a gate: every `uses:` in .github/workflows must
// resolve to a 40-hex commit SHA, or be named — with a reason and an owner — in
// .github/unpinned-actions.json.
//
// WHY A REGISTER AND NOT A FLAT BAN. A third-party action referenced by a
// FLOATING tag (`actions/checkout@v7`) is code this repo executes with the
// workflow's credentials, fetched at run time from a ref its author can move
// after review. Tags are not immutable on GitHub: `v7` can be re-pointed at a
// different tree, and every subsequent run of every workflow takes the new one
// silently. A SHA cannot be re-pointed, which is the whole of the mitigation.
//
// This tree currently pins none of them, and resolving a tag to a SHA requires
// asking GitHub what it points at right now. So a flat ban would be a gate that
// is red on the day it lands — which is a gate nobody keeps. The register is the
// same shape .flake/register.json uses for quarantined tests: the CURRENT set is
// recorded with its reason, the gate blocks anything NEW, and the ceiling only
// ever moves down. Draining it is one line deleted per action pinned.
//
// Reconciliation runs in BOTH directions, for the reason flake-check does it:
// a register that outlives what it describes stops being read. A waiver naming
// an action no workflow uses is a finding, not a harmless leftover.
//
// Three outcomes, three exit codes:
//   0  it checked, and every unpinned reference is registered and under ceiling
//   2  it checked and found problems
//   3  it COULD NOT CHECK (no workflows, unreadable register) — NOT a pass
//
// `--report` prints the burn-down list instead of judging.
//
// Run by `just pin-check` and by the `Ship inventory` CI job.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');

export const WORKFLOW_DIR = '.github/workflows';
export const REGISTER_PATH = '.github/unpinned-actions.json';

const SHA = /^[0-9a-f]{40}$/;

/**
 * Every `uses:` reference in one workflow's text, with the line it sits on.
 *
 * Deliberately a line scan and not a YAML parse: this file must be runnable on a
 * clone with zero dependencies (the same constraint every other gate in
 * scripts/ci/ carries), and `uses:` is a leaf scalar whose grammar — optional
 * `- ` sequence dash, the key, one value, an optional `#` comment — has no
 * multi-line form to miss.
 */
export function usesInWorkflow(text) {
  const out = [];
  text.split('\n').forEach((line, i) => {
    const m = /^\s*(?:-\s+)?uses:\s*(\S+)/.exec(line);
    if (!m) return;
    const ref = m[1].replace(/^["']|["']$/g, '');
    out.push({ ref, line: i + 1 });
  });
  return out;
}

/**
 * Is this reference exempt from pinning by construction?
 *
 * A local composite action (`./.github/actions/x`) is this repository's own code
 * at this repository's own commit — there is no third party and no ref to move.
 * A reusable workflow in this same repo (`./.github/workflows/x.yml`) is the
 * same. `docker://` images are pinned by digest through their own syntax.
 */
export function isLocal(ref) {
  return ref.startsWith('./') || ref.startsWith('docker://');
}

/** A reference is pinned when what follows the LAST `@` is a 40-hex commit SHA. */
export function isPinned(ref) {
  const at = ref.lastIndexOf('@');
  if (at === -1) return false;
  return SHA.test(ref.slice(at + 1));
}

/** `actions/checkout@v7` -> `actions/checkout`. The identity a waiver names. */
export function actionName(ref) {
  const at = ref.lastIndexOf('@');
  return at === -1 ? ref : ref.slice(0, at);
}

/** Read every workflow file under `dir` (absolute). */
export function readWorkflows(root) {
  const dir = path.join(root, WORKFLOW_DIR);
  let entries;
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return null; // cannot check
  }
  const files = entries.filter((f) => /\.ya?ml$/.test(f)).sort();
  if (files.length === 0) return null;
  return files.map((f) => ({
    file: `${WORKFLOW_DIR}/${f}`,
    text: fs.readFileSync(path.join(dir, f), 'utf8'),
  }));
}

export function readRegister(root) {
  try {
    const raw = fs.readFileSync(path.join(root, REGISTER_PATH), 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.waived)) return null;
    return parsed;
  } catch {
    return null; // cannot check
  }
}

/**
 * The whole judgement, as a pure function of the two inputs — so the fixture
 * suite can drive it without a checkout and prove this gate can still go red.
 *
 * @returns {{findings: string[], unpinned: {action: string, file: string, line: number, ref: string}[], pinned: number}}
 */
export function judge(workflows, register) {
  const unpinned = [];
  let pinned = 0;
  for (const { file, text } of workflows) {
    for (const { ref, line } of usesInWorkflow(text)) {
      if (isLocal(ref)) continue;
      if (isPinned(ref)) {
        pinned += 1;
        continue;
      }
      unpinned.push({ action: actionName(ref), file, line, ref });
    }
  }

  const waived = new Set(register.waived.map((w) => w.action));
  const findings = [];

  // Direction 1: an unpinned reference nobody registered. This is the one that
  // catches a NEW floating tag arriving in a workflow edit.
  for (const u of unpinned) {
    if (!waived.has(u.action)) {
      findings.push(
        `${u.file}:${u.line} uses \`${u.ref}\` — a floating ref. Pin it to a commit SHA ` +
          `(\`gh api repos/${u.action}/commits/${u.ref.slice(u.ref.lastIndexOf('@') + 1)} --jq .sha\`), ` +
          `or add it to ${REGISTER_PATH} with a reason and an owner.`
      );
    }
  }

  // Direction 2: a waiver describing something the tree no longer does. A
  // register that outlives its subject stops being read, and its ceiling stops
  // meaning anything.
  const used = new Set(unpinned.map((u) => u.action));
  for (const w of register.waived) {
    if (!used.has(w.action)) {
      findings.push(
        `${REGISTER_PATH} waives \`${w.action}\`, but no workflow uses it unpinned any more — delete the entry.`
      );
    }
  }

  // The ceiling. It exists so the register cannot absorb growth: a waiver list
  // with no bound is a permanent exemption wearing a burn-down's clothes.
  const ceiling = Number(register.ceiling);
  if (!Number.isFinite(ceiling)) {
    findings.push(`${REGISTER_PATH} has no numeric \`ceiling\` — the register is unbounded.`);
  } else if (unpinned.length > ceiling) {
    findings.push(
      `${unpinned.length} unpinned action references, ceiling is ${ceiling}. The ceiling only moves DOWN: ` +
        `pin something before adding something.`
    );
  }

  for (const w of register.waived) {
    if (!w.reason || !w.owner) {
      findings.push(`${REGISTER_PATH} entry \`${w.action}\` is missing \`reason\` or \`owner\`.`);
    }
  }

  return { findings, unpinned, pinned };
}

function main(argv) {
  const report = argv.includes('--report');
  const workflows = readWorkflows(REPO_ROOT);
  if (!workflows) {
    console.error(`pin-check: CANNOT CHECK — no readable workflows under ${WORKFLOW_DIR}/`);
    return 3;
  }
  const register = readRegister(REPO_ROOT);
  if (!register) {
    console.error(`pin-check: CANNOT CHECK — ${REGISTER_PATH} is missing or malformed`);
    return 3;
  }

  const { findings, unpinned, pinned } = judge(workflows, register);

  if (report) {
    const total = pinned + unpinned.length;
    console.log(`action references: ${total} (${pinned} pinned to a SHA, ${unpinned.length} floating)`);
    console.log(`ceiling: ${register.ceiling}`);
    if (unpinned.length) {
      console.log('burn-down — one line deleted from the register per action pinned:');
      const byAction = new Map();
      for (const u of unpinned) byAction.set(u.action, (byAction.get(u.action) ?? 0) + 1);
      for (const [action, count] of [...byAction].sort()) {
        console.log(`  ${action}  (${count} reference${count === 1 ? '' : 's'})`);
      }
    }
    return 0;
  }

  if (findings.length) {
    console.error('pin-check: FINDINGS');
    for (const f of findings) console.error(`  - ${f}`);
    return 2;
  }
  console.log(
    `pin-check: ok — ${pinned} pinned, ${unpinned.length} registered as unpinned (ceiling ${register.ceiling})`
  );
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exit(main(process.argv.slice(2)));
}
