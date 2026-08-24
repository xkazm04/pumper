#!/usr/bin/env node
// `just flake-check` — the quarantine register as a gate.
//
// A flaky test is not a state, it is a process with an owner at every step
// (registry: test-harness/flake-lifecycle). This repo had the first step and
// none of the rest: two timing-flaky tests carried `#[ignore = "...flaky..."]`
// with a reason but no owner, no entry date, no expiry and no register, and the
// `--ignored` lane ran in no CI job — so nothing accumulated the history that
// would decide whether either test is still flaky, and nothing would ever have
// told anyone the quarantine had been open for a year.
//
// WHAT THIS FAILS ON
//
//   - an expired register entry (expiry ESCALATES; it never silently extends)
//   - a register entry naming a test the tree no longer has (an orphan)
//   - a flake-reasoned `#[ignore]` with no register entry
//   - an `#[ignore]` declared in NEITHER table — classify it, do not leave it
//   - an exempt row whose own source reason says "flaky": environment-gating is
//     an exemption from the register, not a laundry for a flake
//   - a breached register ceiling — a stop-the-line event for the suite
//   - a quarantine entry owned by a team rather than a person, or missing its
//     dates, cause, form or evidence
//
// AND THE THIRD OUTCOME
//
// Exit 3 is CANNOT CHECK: an unreadable register, a register with no ceiling,
// or a source scan that found zero `#[ignore]` in a tree that has nineteen.
// A broken instrument must not radiate confidence — the same discipline
// scripts/docs/check-doc-sync.mjs runs on, and for the same reason.
//
//   0 — checked, and the register is honest.
//   2 — checked, and there are findings.
//   3 — could NOT check. Not a pass; nothing was verified.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { defaultRepoRoot, packages, registerKey, scanIgnoredTests } from './flake-id.mjs';

export const EXIT_FINDINGS = 2;
export const EXIT_CANNOT_CHECK = 3;

/**
 * Words that make an `#[ignore]` reason a FLAKE claim rather than an
 * environment gate.
 *
 * Deliberately a small, blunt vocabulary. Its job is not to classify correctly
 * — the declared tables do that — but to catch the one move that would hollow
 * the register out: adding a new timing-flaky `#[ignore]` and never registering
 * it. A false positive here costs one line in `exempt` with a reason; a false
 * negative costs the register its meaning.
 */
export const FLAKE_WORDS = [
  'flake',
  'flaky',
  'intermittent',
  'timing',
  'wall-clock',
  'wall clock',
  'race',
  'nondeterministic',
  'non-deterministic',
  'sporadic',
  'unstable',
];

export function readsAsFlake(reason) {
  if (!reason) return false;
  const low = reason.toLowerCase();
  return FLAKE_WORDS.some((w) => low.includes(w));
}

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const REQUIRED = ['id', 'owner', 'entered', 'expires', 'cause', 'form', 'evidence'];
const CAUSES = ['test', 'harness', 'product'];
const FORMS = ['muted', 'skipped'];

function nonEmpty(v) {
  if (Array.isArray(v)) return v.length > 0 && v.every((s) => String(s).trim().length > 0);
  return typeof v === 'string' && v.trim().length > 0;
}

function daysBetween(a, b) {
  return Math.round((Date.parse(b) - Date.parse(a)) / 86_400_000);
}

/**
 * The whole decision, as a pure function over the register and the scan.
 *
 * Returns { findings, health }. Kept pure so the tests can drive every failure
 * mode from a fixture instead of mutating the real tree.
 */
export function evaluateRegister(register, scanned, today) {
  const findings = [];
  const add = (kind, message) => findings.push({ kind, message });

  const quarantine = register.quarantine || [];
  const exempt = register.exempt || [];

  // Scan side: one key per ignored test. Two ignored tests with the same fn
  // name in the same target would make every reconciliation below ambiguous,
  // so that is its own finding rather than a silently-picked winner.
  const byKey = new Map();
  for (const hit of scanned) {
    if (!byKey.has(hit.key)) byKey.set(hit.key, []);
    byKey.get(hit.key).push(hit);
  }
  for (const [key, hits] of byKey) {
    if (hits.length > 1) {
      add(
        'ambiguous',
        `two #[ignore]d tests share the identity ${key} (${hits
          .map((h) => `${h.file}:${h.line}`)
          .join(', ')}) — rename one; a register entry cannot point at either`
      );
    }
  }

  // --- direction 1: every register row must resolve to a real test ----------
  const claimed = new Map(); // key -> 'quarantine' | 'exempt'
  const rows = [
    ...quarantine.map((e) => ({ e, table: 'quarantine' })),
    ...exempt.map((e) => ({ e, table: 'exempt' })),
  ];
  for (const { e, table } of rows) {
    const key = registerKey(e.id);
    if (!key) {
      add('malformed-id', `${table} row has an unusable id ${JSON.stringify(e.id)} — ids are <package>::<target>::<module path>::<fn>`);
      continue;
    }
    if (!byKey.has(key)) {
      add(
        'orphan',
        `${table} row ${e.id} names a test that no longer exists (looked for ${key}). ` +
          `Re-point it at the test's new identity, or delete the row — a register that ` +
          `outlives its tests certifies nothing.`
      );
      continue;
    }
    if (claimed.has(key)) {
      add('double-claimed', `${e.id} appears in both tables — an ignore is a flake OR an environment gate, never both`);
      continue;
    }
    claimed.set(key, table);
  }

  // --- direction 2: every #[ignore] in the tree must be declared ------------
  for (const [key, hits] of byKey) {
    const hit = hits[0];
    const table = claimed.get(key);
    const where = `${hit.file}:${hit.line}`;
    if (!table) {
      if (readsAsFlake(hit.reason)) {
        add(
          'unregistered-flake',
          `${where} #[ignore = ${JSON.stringify(hit.reason)}] reads as a FLAKE and has no ` +
            `register entry. Quarantine is a decision with an owner, an entry date, an ` +
            `expiry and a suspected cause — add the row to .flake/register.json ` +
            `(id: ${key.split('::').slice(0, 2).join('::')}::<module path>::${hit.fn}).`
        );
      } else {
        add(
          'undeclared',
          `${where} #[ignore] on ${hit.fn} is in neither table. Declare it: a flake goes in ` +
            `\`quarantine\` with an owner and an expiry; an environment gate goes in \`exempt\` ` +
            `with the capability it needs and why.`
        );
      }
      continue;
    }
    if (table === 'exempt' && readsAsFlake(hit.reason)) {
      add(
        'laundered',
        `${where} is declared environment-gated in \`exempt\`, but its own reason says ` +
          `${JSON.stringify(hit.reason)}. An exemption is for a capability the runner lacks ` +
          `(Chrome, a built .wasm, live network, a perf corpus) — not for a flake with a ` +
          `plausible cover story. Move it to \`quarantine\` with an owner and an expiry.`
      );
    }
  }

  // --- the quarantine rows' own discipline ---------------------------------
  for (const e of quarantine) {
    for (const field of REQUIRED) {
      if (!nonEmpty(e[field])) {
        add('incomplete', `quarantine row ${e.id || '<no id>'} is missing \`${field}\``);
      }
    }
    if (typeof e.owner === 'string' && /team|squad|@|\bops\b|everyone/i.test(e.owner)) {
      add(
        'unowned',
        `quarantine row ${e.id} is owned by ${JSON.stringify(e.owner)} — the owner must be a ` +
          `named person. Unowned quarantine is never reviewed.`
      );
    }
    for (const field of ['entered', 'expires']) {
      if (typeof e[field] === 'string' && !DATE.test(e[field])) {
        add('bad-date', `quarantine row ${e.id} has a non-ISO \`${field}\`: ${e[field]}`);
      }
    }
    if (e.cause && !CAUSES.includes(e.cause)) {
      add('bad-cause', `quarantine row ${e.id} has cause ${JSON.stringify(e.cause)} — one of ${CAUSES.join(' | ')}`);
    }
    if (e.form && !FORMS.includes(e.form)) {
      add('bad-form', `quarantine row ${e.id} has form ${JSON.stringify(e.form)} — one of ${FORMS.join(' | ')}`);
    }
    if (e.form === 'skipped' && !nonEmpty(e.formReason)) {
      add(
        'unjustified-skip',
        `quarantine row ${e.id} is \`skipped\` with no \`formReason\`. Prefer muted: a muted ` +
          `test keeps producing the history that will eventually diagnose it, and a skipped ` +
          `one is indistinguishable from a deleted one after a month.`
      );
    }
    if (DATE.test(e.expires || '') && daysBetween(today, e.expires) < 0) {
      add(
        'expired',
        `quarantine row ${e.id} EXPIRED on ${e.expires} (${-daysBetween(today, e.expires)} days ` +
          `ago), owner ${e.owner}. Expiry escalates — fix and release the test, or record a new ` +
          `expiry with a stated reason. Extending it silently is how a register stops being debt.`
      );
    }
  }
  for (const e of exempt) {
    for (const field of ['id', 'gate', 'reason']) {
      if (!nonEmpty(e[field])) add('incomplete', `exempt row ${e.id || '<no id>'} is missing \`${field}\``);
    }
  }

  // --- the ceiling ---------------------------------------------------------
  const ceiling = register.ceiling;
  if (quarantine.length > ceiling) {
    add(
      'ceiling',
      `STOP THE LINE: ${quarantine.length} quarantined tests against a ceiling of ${ceiling}. ` +
        `Without a ceiling the register absorbs every hard problem and the suite quietly stops ` +
        `certifying anything. Fix one out before adding another.`
    );
  }

  const dated = quarantine.filter((e) => DATE.test(e.entered || ''));
  const oldest = dated.sort((a, b) => a.entered.localeCompare(b.entered))[0];
  const health = {
    size: quarantine.length,
    ceiling,
    exemptSize: exempt.length,
    ignoresInTree: scanned.length,
    oldest: oldest ? { id: oldest.id, entered: oldest.entered, ageDays: daysBetween(oldest.entered, today) } : null,
  };
  return { findings, health };
}

// --- history: detection by transition count, never by impression ------------

export function historyDir(repoRoot = defaultRepoRoot()) {
  return path.join(repoRoot, '.flake/history/runs');
}

export function loadRuns(dir) {
  let names;
  try {
    names = fs.readdirSync(dir).filter((f) => f.endsWith('.json'));
  } catch {
    return [];
  }
  const runs = [];
  for (const n of names) {
    try {
      const r = JSON.parse(fs.readFileSync(path.join(dir, n), 'utf8'));
      if (r && Array.isArray(r.tests) && r.startedAt) runs.push(r);
    } catch {
      // A half-written run record is one lost sample, not a dead instrument.
    }
  }
  return runs.sort((a, b) => String(a.startedAt).localeCompare(String(b.startedAt)));
}

/**
 * Outcome transitions per test, ON THE SAME CODE, over a window.
 *
 * Same-code is the load-bearing qualifier: outcomes compared across different
 * trees measure the product's churn, not the test's stability. So a pair of
 * consecutive runs only contributes when their commit shas match.
 *
 * A raw failure rate would be the wrong instrument — a consistently failing
 * test is BROKEN, not flaky, and the two need opposite responses.
 *
 * Returns { predicate, runs, byId } where predicate is the sentence every
 * figure derived from this must travel with.
 */
export function transitions(runs, { branch, windowDays, today }) {
  const cutoff = Date.parse(today) - windowDays * 86_400_000;
  const scoped = runs.filter(
    (r) => (!branch || r.branch === branch) && Date.parse(r.startedAt) >= cutoff
  );
  const byId = new Map();
  const outcomeOf = new Map(); // id -> [{sha, outcome}] in run order
  for (const run of scoped) {
    for (const t of run.tests) {
      if (t.outcome !== 'ok' && t.outcome !== 'FAILED') continue;
      if (!outcomeOf.has(t.id)) outcomeOf.set(t.id, []);
      outcomeOf.get(t.id).push({ sha: run.sha, outcome: t.outcome });
    }
  }
  for (const [id, seq] of outcomeOf) {
    let changed = 0;
    let sameCodePairs = 0;
    for (let i = 1; i < seq.length; i++) {
      if (!seq[i].sha || seq[i].sha !== seq[i - 1].sha) continue;
      sameCodePairs++;
      if (seq[i].outcome !== seq[i - 1].outcome) changed++;
    }
    byId.set(id, { changed, sameCodePairs, runs: seq.length });
  }
  return {
    byId,
    runs: scoped.length,
    predicate:
      `window ${windowDays} days ending ${today}, branch ` +
      `${branch || '<any>'}, ${scoped.length} recorded run(s)`,
  };
}

/**
 * The labelled set: automatic, derived, and therefore automatically REVERSED.
 *
 * There is no stored label to forget to remove — the label IS the query, so a
 * test that has been stable for the whole window stops being described as flaky
 * on the very next run. That is the half of labelling everyone forgets, and its
 * absence is why registers only ever grow.
 *
 * Labelling is not quarantining: a labelled test still blocks. The label is
 * information; the register is the decision.
 */
export function labelled(tr, threshold) {
  return [...tr.byId.entries()]
    .filter(([, s]) => s.changed >= threshold)
    .sort((a, b) => b[1].changed - a[1].changed)
    .map(([id, s]) => ({ id, ...s }));
}

// --- the report -------------------------------------------------------------

function cannotCheck(reason) {
  process.stderr.write(
    `flake:check: CANNOT CHECK — ${reason}.\n` +
      `This is not a pass: the quarantine register was not verified. Fix the instrument\n` +
      `(.flake/register.json, scripts/ci/flake-id.mjs) and re-run \`just flake-check\`.\n`
  );
  process.exit(EXIT_CANNOT_CHECK);
}

export function registerPath(repoRoot = defaultRepoRoot()) {
  return path.join(repoRoot, '.flake/register.json');
}

function main() {
  const repoRoot = defaultRepoRoot();
  const argv = process.argv.slice(2);
  const arg = (name, fallback) => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 && argv[i + 1] ? argv[i + 1] : fallback;
  };
  const today = arg('today', new Date().toISOString().slice(0, 10));
  const reportOnly = argv.includes('--report');

  // Instrument assertion #1: the standard.
  let register;
  try {
    register = JSON.parse(fs.readFileSync(registerPath(repoRoot), 'utf8'));
  } catch (e) {
    cannotCheck(`the register could not be read or parsed (${e.message})`);
  }
  if (!Array.isArray(register.quarantine) || !Array.isArray(register.exempt)) {
    cannotCheck('the register has no `quarantine` / `exempt` arrays, so nothing could be reconciled');
  }
  if (typeof register.ceiling !== 'number') {
    cannotCheck('the register declares no numeric `ceiling` — a register without one absorbs every hard problem');
  }

  // Instrument assertion #2: the scanner. A tree with nineteen `#[ignore]`s
  // that scans to zero is a broken scanner reporting a clean repo — the exact
  // false green this whole file exists to refuse.
  const pkgs = packages(repoRoot);
  if (pkgs.length === 0) cannotCheck('no cargo packages found under crates/ — the scanner is looking in the wrong place');
  const scanned = scanIgnoredTests(repoRoot, pkgs);
  if (scanned.length === 0) {
    cannotCheck(
      `the source scan found zero #[ignore]d tests across ${pkgs.length} packages — ` +
        `either every quarantine was just deleted, or the scanner is broken`
    );
  }

  const { findings, health } = evaluateRegister(register, scanned, today);

  // --- published health: size WITH its trend, and the oldest entry's age ----
  const windowDays = register.windowDays || 14;
  const branch = arg('branch', register.branch || 'master');
  const runs = loadRuns(historyDir(repoRoot));
  const tr = transitions(runs, { branch, windowDays, today });
  const labels = labelled(tr, register.labelThreshold ?? 2);
  const trend = recordSizeSample(repoRoot, today, health.size);

  const out = [];
  out.push(`flake register: ${health.size}/${health.ceiling} quarantined, ${health.exemptSize} environment-gated, ${health.ignoresInTree} #[ignore]s in tree`);
  out.push(`  trend:  ${trend}`);
  out.push(
    `  oldest: ${
      health.oldest
        ? `${health.oldest.id} — entered ${health.oldest.entered}, ${health.oldest.ageDays} days old`
        : 'none (register empty)'
    }`
  );
  if (runs.length === 0) {
    // "Found nothing" and "cannot see" are different sentences.
    out.push(`  history: NONE RECORDED YET — no run history under .flake/history/runs, so no test can be labelled. This is not "no flakes".`);
  } else {
    out.push(`  history: ${tr.byId.size} test(s) observed; predicate: ${tr.predicate}`);
    if (labels.length === 0) {
      out.push(`  labelled flaky: none — no test changed outcome on the same commit (${tr.predicate})`);
    } else {
      out.push(`  labelled flaky (still BLOCKING — a label is information, not a quarantine):`);
      for (const l of labels) {
        out.push(`    ${l.id}: changed outcome in ${l.changed} of ${l.sameCodePairs} same-commit run pairs (${tr.predicate})`);
      }
    }
  }
  process.stdout.write(`${out.join('\n')}\n`);
  writeLabels(repoRoot, { predicate: tr.predicate, labelled: labels });

  if (reportOnly) process.exit(0);

  if (findings.length > 0) {
    process.stderr.write(
      `\nflake:check found ${findings.length} problem(s) with the quarantine register:\n\n` +
        findings.map((f) => `  [${f.kind}] ${f.message}`).join('\n\n') +
        `\n\nAn agent NEVER quarantines a test to make a build green. If you are here because a\n` +
        `build went red, the answer is not a new row in .flake/register.json.\n`
    );
    process.exit(EXIT_FINDINGS);
  }
  process.exit(0);
}

/** Append today's register size and return the trend sentence for it. */
function recordSizeSample(repoRoot, today, size) {
  const file = path.join(repoRoot, '.flake/history/register-size.jsonl');
  let samples = [];
  try {
    samples = fs
      .readFileSync(file, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((l) => JSON.parse(l))
      .filter((s) => s && s.day && typeof s.size === 'number');
  } catch {
    samples = [];
  }
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    if (samples[samples.length - 1]?.day !== today) {
      fs.appendFileSync(file, `${JSON.stringify({ day: today, size })}\n`);
    }
  } catch {
    // Recording the sample is instrumentation; failing to write one must never
    // change this gate's verdict.
  }
  const prior = samples.filter((s) => s.day !== today);
  if (prior.length === 0) {
    return `FIRST RECORDED SAMPLE — no trend yet. A size with no trend is a number, not a finding.`;
  }
  const first = prior[0];
  const delta = size - first.size;
  const sign = delta > 0 ? `+${delta}` : `${delta}`;
  return `${sign} since ${first.day} (${first.size} -> ${size} over ${prior.length + 1} samples). A register growing monotonically is deletion with a slower fuse.`;
}

function writeLabels(repoRoot, payload) {
  try {
    const file = path.join(repoRoot, '.flake/history/labels.json');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, `${JSON.stringify(payload, null, 2)}\n`);
  } catch {
    // Same: the label file is a convenience for the CI summary, not the verdict.
  }
}

const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (invokedDirectly) main();
