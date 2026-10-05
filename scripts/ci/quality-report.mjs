#!/usr/bin/env node
// `just quality` — the whole posture in one run, with a remedy on every red row.
//
// WHY THIS EXISTS
//
// `just ci` is a `just` dependency chain. `just` aborts at the first non-zero
// rung, so an operator who breaks the flake register learns nothing about the
// disk budget, the lanes, the MCP parity or the doc-sync hook until they fix it
// and re-run ten rungs. The posture was published by four separate report
// recipes and one hand-listed CI step-summary block that named two of five
// gates with `|| true` — a projection of the gate set that both understated it
// and could not fail.
//
// This runs every gate the registry knows, continues past every failure, and
// prints ONE table: gate, verdict, the one-line finding, and the exact command
// that fixes it.
//
// THE ROW SET IS scripts/ci/gates.json, NEVER A LIST IN HERE
//
// A second hand-written list of gates is exactly the drift gate-inventory.mjs
// closed. Every row comes from `gates`, and the per-gate invocation + remedy
// come from the `reportModes` block beside it. A registered gate with no report
// mode still gets a row — NOT WIRED — because an inexpressible condition must
// still enumerate. The one row never emitted is this script's own: its mode is
// marked `self`, or the report would run itself forever.
//
// CANNOT CHECK IS A COLUMN, NOT A ROUNDING ERROR
//
// Each gate is careful to distinguish exit 3 at its own stderr and nothing ever
// added them up. Two broken instruments and zero findings looked, in a terminal,
// exactly like one broken instrument — and in the CI summary like nothing at
// all. The footer counts clean / findings / cannot-check / not wired in four
// separate columns, and an exit code outside a gate's declared 0/2/3 contract
// (a crash, a gate that cannot be spawned) is CANNOT CHECK, never a pass.
//
//   0 — every gate checked, and every gate is clean.
//   2 — at least one gate has findings. Findings dominate the exit.
//   3 — nothing has findings but at least one instrument could not be run, or
//       a registered gate has no report mode and no declared waiver.
//
// A NOT WIRED row whose registry entry carries `verdictWaived` does not redden
// the run: that rung is declared absent with a reason, which is a different
// thing from an instrument that broke.
//
// READ-ONLY BY CONSTRUCTION
//
// Every command is the gate's non-mutating mode — disk-check is invoked
// `--no-prune`, which its own header calls "measurement only. Never mutates
// target/". `two_runs_over_a_fixture_target_leave_it_byte_for_byte_identical`
// in the fixture suite holds that to byte equality.
//
// MODES AND TEST SEAMS
//
//   (no args)            the table, on a terminal.
//   --markdown           the same table as GFM, for the CI step summary.
//   --registry <file>    judge THIS registry instead of scripts/ci/gates.json.
//   --root <dir>         spawn the gates with THIS directory as their cwd.
//
// The two seams are what let the fixture suite plant a gate that exits 3 and
// watch the column it lands in; without them this reporter's own three outcomes
// would be unobservable, which is the sin it exists to report on.

import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const EXIT_FINDINGS = 2;
export const EXIT_CANNOT_CHECK = 3;

/** The four verdicts. These strings are the table's vocabulary. */
export const VERDICT = {
  CLEAN: 'CLEAN',
  FINDINGS: 'FINDINGS',
  CANNOT_CHECK: 'CANNOT CHECK',
  NOT_WIRED: 'NOT WIRED',
};

function defaultRepoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
}

/** The registry path, the spawn cwd and the output mode, pulled out of argv. */
export function parseArgs(argv, root = defaultRepoRoot()) {
  const value = (flag) => {
    const i = argv.indexOf(flag);
    return i !== -1 && argv[i + 1] ? argv[i + 1] : null;
  };
  const rootArg = value('--root');
  const registryArg = value('--registry');
  return {
    markdown: argv.includes('--markdown'),
    root: rootArg !== null ? path.resolve(rootArg) : root,
    registryPath:
      registryArg !== null
        ? path.resolve(registryArg)
        : path.join(root, 'scripts', 'ci', 'gates.json'),
  };
}

/**
 * The verdict a gate's exit code earns.
 *
 * Anything outside the declared 0/2/3 contract — a crash, a `node` that could
 * not find the script, a signal — is CANNOT CHECK. A gate that exits 1 did not
 * say the tree is clean; it said nothing, and nothing is not a pass.
 */
export function verdictForExit(code) {
  if (code === 0) return VERDICT.CLEAN;
  if (code === EXIT_FINDINGS) return VERDICT.FINDINGS;
  return VERDICT.CANNOT_CHECK;
}

/**
 * The one line that explains a verdict: the first non-empty line of stderr,
 * else of stdout.
 *
 * Splits on /\r?\n/ deliberately. A JS `.` does not match `\r` and this is a
 * CRLF checkout; wave 1 lost a cycle to exactly that, and a finding dragging a
 * trailing \r into a table corrupts every row after it.
 */
export function firstFinding(stdout, stderr) {
  for (const stream of [stderr, stdout]) {
    for (const line of String(stream ?? '').split(/\r?\n/)) {
      const t = line.trim();
      if (t !== '') return t;
    }
  }
  return '';
}

/**
 * The command that answers a red row.
 *
 * Declared in the registry's report mode; failing that, derived from the gate's
 * own justfile recipe, failing that from its script. Never empty, because
 * refusal-names-a-reachable-remedy: a verdict a human cannot act on is half a
 * verdict, and "no remedy declared" is the asymmetry this column exists to make
 * reviewable.
 */
export function remedyFor(gate, mode) {
  const declared = (mode ?? {}).remedy;
  if (typeof declared === 'string' && declared.trim() !== '') return declared.trim();
  if (gate.recipe) return `just ${gate.recipe}`;
  if (gate.script) return `node ${String(gate.script).split(path.sep).join('/')}`;
  return '';
}

/** The repo-relative path-looking tokens a remedy names. */
export function remedyPaths(remedy) {
  return String(remedy ?? '')
    .split(/\s+/)
    .filter((t) => t.includes('/') && !t.startsWith('-'));
}

/**
 * True when a remedy is something a human can actually run here: it starts with
 * `just` or `node`, and every path it names resolves to a file inside the repo.
 */
export function remedyIsReachable(remedy, root) {
  const tokens = String(remedy ?? '').trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return false;
  if (!['just', 'node'].includes(tokens[0])) return false;
  for (const p of remedyPaths(remedy)) {
    const full = path.resolve(root, p);
    if (!full.startsWith(path.resolve(root) + path.sep)) return false;
    if (!fs.existsSync(full)) return false;
  }
  return true;
}

/** Spawn one gate and reduce it to {status, stdout, stderr}. */
export function spawnGate(command, root) {
  const [bin, ...args] = command;
  const r = spawnSync(bin === 'node' ? process.execPath : bin, args, {
    cwd: root,
    encoding: 'utf8',
  });
  if (r.error) return { status: null, stdout: '', stderr: String(r.error.message) };
  return { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
}

/**
 * One row per registered gate, in registry order, minus this script's own.
 *
 * `runner(command, root)` is the spawn seam; the fixture suite passes a stub to
 * assert the row SET without paying for five child processes.
 */
export function buildRows(registry, runner, root) {
  const modes = registry.reportModes ?? {};
  const rows = [];
  for (const gate of registry.gates ?? []) {
    const mode = modes[gate.key];
    if ((mode ?? {}).self === true) continue;
    const waived = typeof gate.verdictWaived === 'string' && gate.verdictWaived.length > 0;
    const remedy = remedyFor(gate, mode);
    if (!mode || !Array.isArray(mode.command) || mode.command.length === 0) {
      rows.push({
        key: gate.key,
        verdict: VERDICT.NOT_WIRED,
        finding: waived
          ? `no standing verdict rung: ${gate.verdictWaived}`
          : 'gates.json declares no report mode and no verdictWaived for this gate',
        remedy,
        waived,
      });
      continue;
    }
    const got = runner(mode.command, root);
    const verdict = verdictForExit(got.status);
    const unexpected = got.status !== 0 && got.status !== EXIT_FINDINGS && got.status !== EXIT_CANNOT_CHECK;
    let finding = firstFinding(got.stdout, got.stderr);
    if (unexpected) {
      finding = `exit ${got.status === null ? 'none' : got.status} (outside the gate's declared 0/2/3): ${finding}`;
    }
    rows.push({
      key: gate.key,
      verdict,
      finding: verdict === VERDICT.CLEAN ? '' : finding,
      remedy,
      waived,
    });
  }
  return rows;
}

/** Four columns, counted separately. cannot-check is never folded into clean. */
export function tally(rows) {
  const t = { clean: 0, findings: 0, cannotCheck: 0, notWired: 0 };
  for (const row of rows) {
    if (row.verdict === VERDICT.CLEAN) t.clean += 1;
    else if (row.verdict === VERDICT.FINDINGS) t.findings += 1;
    else if (row.verdict === VERDICT.CANNOT_CHECK) t.cannotCheck += 1;
    else t.notWired += 1;
  }
  return t;
}

/** The worst verdict in the table, as the same 0/2/3 the gates themselves keep. */
export function exitFor(rows) {
  if (rows.some((r) => r.verdict === VERDICT.FINDINGS)) return EXIT_FINDINGS;
  if (rows.some((r) => r.verdict === VERDICT.CANNOT_CHECK)) return EXIT_CANNOT_CHECK;
  if (rows.some((r) => r.verdict === VERDICT.NOT_WIRED && !r.waived)) return EXIT_CANNOT_CHECK;
  return 0;
}

export function footer(t) {
  return `${t.clean} clean / ${t.findings} findings / ${t.cannotCheck} cannot-check / ${t.notWired} not wired`;
}

const clip = (s, n) => (s.length <= n ? s : `${s.slice(0, n - 1)}…`);

export function renderTable(rows) {
  const t = tally(rows);
  const w = (sel, min) => Math.max(min, ...rows.map((r) => sel(r).length));
  const kw = w((r) => r.key, 4);
  const vw = w((r) => r.verdict, 7);
  const out = [`quality posture: ${rows.length} gates from scripts/ci/gates.json`, ''];
  for (const row of rows) {
    const finding = clip(row.finding, 72);
    const head = `  ${row.key.padEnd(kw)}  ${row.verdict.padEnd(vw)}  `;
    const remedy = row.verdict === VERDICT.CLEAN ? '' : row.remedy;
    // The remedy column only exists where there is something to remedy: a clean
    // row ending in a bare `|` reads as a remedy nobody wrote down.
    out.push(remedy === '' ? (head + finding).trimEnd() : `${head}${finding.padEnd(72)} | ${remedy}`);
  }
  out.push('', `  ${footer(t)}`);
  if (t.findings === 0 && t.cannotCheck === 0 && rows.every((r) => r.verdict !== VERDICT.NOT_WIRED || r.waived)) {
    out.push('  every gate checked, and every gate is clean.');
  } else {
    out.push('  run the remedy on each red row above; a CANNOT CHECK is a broken instrument, not a pass.');
  }
  return out.join('\n');
}

export function renderMarkdown(rows) {
  const t = tally(rows);
  const cell = (s) => String(s).replace(/\|/g, '\\|');
  const out = ['| gate | verdict | finding | remedy |', '| --- | --- | --- | --- |'];
  for (const row of rows) {
    const remedy = row.verdict === VERDICT.CLEAN ? '' : `\`${row.remedy}\``;
    out.push(`| \`${cell(row.key)}\` | ${row.verdict} | ${cell(clip(row.finding, 160))} | ${remedy} |`);
  }
  out.push('', `**${footer(t)}**`);
  return out.join('\n');
}

function main(argv) {
  const { markdown, root, registryPath } = parseArgs(argv);
  let registry;
  try {
    registry = JSON.parse(fs.readFileSync(registryPath, 'utf8'));
  } catch (err) {
    process.stdout.write(
      `quality posture: CANNOT CHECK — cannot read the gate registry ${registryPath}: ${err.message}\n` +
        '  remedy: node --test scripts/ci/gate-inventory.test.mjs\n',
    );
    return EXIT_CANNOT_CHECK;
  }
  if (!Array.isArray(registry.gates) || registry.gates.length === 0) {
    process.stdout.write(
      `quality posture: CANNOT CHECK — ${registryPath} declares no gates. A registry that ` +
        'enumerates nothing agrees with a clean tree just as happily as with a broken one.\n',
    );
    return EXIT_CANNOT_CHECK;
  }
  const rows = buildRows(registry, spawnGate, root);
  process.stdout.write(`${markdown ? renderMarkdown(rows) : renderTable(rows)}\n`);
  return exitFor(rows);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  process.exit(main(process.argv.slice(2)));
}
