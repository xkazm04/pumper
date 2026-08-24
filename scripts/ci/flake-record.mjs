#!/usr/bin/env node
// Runs a cargo test command, forwards its exit code EXACTLY, and records what
// each test did — so flakiness is detected from retained history rather than
// from somebody's impression of which test "is a bit flaky".
//
//   node scripts/ci/flake-record.mjs -- cargo test --workspace
//   node scripts/ci/flake-record.mjs --lane wasm-plugin-artifacts -- cargo test ...
//
// WHY A WRAPPER AND NOT cargo-nextest
//
// nextest gives per-test JSON for free, and it was the first thing considered.
// It was rejected for THIS repo: the `test` job is a branch-protection required
// check (`test (ubuntu-latest)` / `test (windows-latest)`), and nextest does not
// run doctests at all and runs every test in its own process. Against a 2039-test
// baseline that is a change to WHAT IS RUN on the rung the branch is protected
// by — a silent coverage cut dressed as a tooling upgrade, which is precisely the
// out-of-graph failure this repo already has a gate for (scripts/ci/ship-inventory).
// A wrapper changes nothing about what runs; it only reads the output.
//
// THE THREE RULES THIS WRAPPER LIVES BY
//
// 1. **The exit code is cargo's, byte for byte.** Nothing below can turn a red
//    build green or a green build red. Every recording step is wrapped, and a
//    recording failure is a warning on stderr, never a verdict.
// 2. **Output is not swallowed.** stdout and stderr are merged into one file at
//    the OS level (a single fd handed to both slots) so cargo's `Running <target>`
//    headers and libtest's result lines keep their true write order — two
//    separately-buffered node pipes can hand them back interleaved wrongly and
//    attribute a whole binary's results to the previous target. The file is
//    tailed to the console as it is written, so a ten-minute run still streams.
// 3. **A label is visible where the test appears.** After the run, every test it
//    touched that is currently quarantined or currently labelled flaky by history
//    is printed with its predicate — in the run output, not only in a register
//    nobody opens.

import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { defaultRepoRoot, packages, parseCargoTestOutput, registerKey } from './flake-id.mjs';
import { historyDir, labelled, loadRuns, transitions } from './flake-check.mjs';

/**
 * Strip SGR/CSI escapes.
 *
 * Not optional: .github/workflows/ci.yml sets `CARGO_TERM_COLOR: always`, so the
 * captured transcript carries colour even though nothing here is a terminal, and an
 * un-stripped `test foo ... <esc>[32mok<esc>[0m` parses as no outcome at all — a
 * reader that silently returns zero results for every CI run.
 *
 * Built from a string so the ESC byte is spelled out rather than pasted invisibly
 * into a regex literal, where an editor or a re-encode can eat it.
 */
const ANSI = new RegExp('\\u001B\\[[0-9;?]*[ -/]*[@-~]', 'g');

export function stripAnsi(text) {
  return text.replace(ANSI, '');
}

function git(repoRoot, args, fallback) {
  try {
    return execFileSync('git', args, { cwd: repoRoot, encoding: 'utf8' }).trim() || fallback;
  } catch {
    return fallback;
  }
}

function parseArgs(argv) {
  const sep = argv.indexOf('--');
  const before = sep === -1 ? [] : argv.slice(0, sep);
  const command = sep === -1 ? argv : argv.slice(sep + 1);
  const flag = (name) => {
    const i = before.indexOf(`--${name}`);
    return i >= 0 ? before[i + 1] : null;
  };
  return { lane: flag('lane'), command };
}

/**
 * Stream `file` to stdout as it grows. Returns a stop function that drains the
 * remainder first — the tail must never lose the last lines of a failing run.
 */
function tail(file) {
  let pos = 0;
  let fd = null;
  const drain = () => {
    try {
      if (fd === null) fd = fs.openSync(file, 'r');
      const size = fs.fstatSync(fd).size;
      while (pos < size) {
        const len = Math.min(65536, size - pos);
        const buf = Buffer.allocUnsafe(len);
        const read = fs.readSync(fd, buf, 0, len, pos);
        if (read <= 0) break;
        pos += read;
        process.stdout.write(buf.subarray(0, read));
      }
    } catch {
      // The console echo is a convenience; losing it must not fail the run.
    }
  };
  const timer = setInterval(drain, 200);
  timer.unref?.();
  return () => {
    clearInterval(timer);
    drain();
    if (fd !== null) {
      try {
        fs.closeSync(fd);
      } catch {
        /* already gone */
      }
    }
  };
}

async function main() {
  const repoRoot = defaultRepoRoot();
  const { lane, command } = parseArgs(process.argv.slice(2));
  if (command.length === 0) {
    process.stderr.write('flake-record: usage: flake-record.mjs [--lane NAME] -- <cargo test ...>\n');
    process.exit(3);
  }

  // A lane run starts from NO artifact. Without this, a harness that panicked
  // before emitting would leave last night's artifact in place and the certifier
  // would judge stale measurements as if they were this run's — a green with no
  // run behind it, which is the exact failure the cannot-see verdict exists to
  // report. Pruning first makes "the harness did not emit" indistinguishable
  // from nothing, which is what it is.
  if (lane) pruneLaneArtifacts(repoRoot, lane);

  const capture = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'flake-record-')), 'run.log');
  const fd = fs.openSync(capture, 'w');
  const stop = tail(capture);
  const startedAt = new Date().toISOString();

  const status = await new Promise((resolve) => {
    // One fd for BOTH stdout and stderr: the merge happens in the kernel, in
    // real write order, which is the only way the `Running <target>` header and
    // the result lines that follow it stay associated.
    const child = spawn(command[0], command.slice(1), {
      cwd: repoRoot,
      stdio: ['inherit', fd, fd],
    });
    child.on('error', (e) => {
      process.stderr.write(`flake-record: could not run ${command[0]}: ${e.message}\n`);
      resolve({ code: 3, spawnFailed: true });
    });
    child.on('close', (code, signal) => resolve({ code, signal }));
  });

  stop();
  try {
    fs.closeSync(fd);
  } catch {
    /* already closed */
  }

  // The exit code is decided HERE and nothing after this line may change it.
  const exitCode = status.spawnFailed
    ? 3
    : status.code !== null && status.code !== undefined
      ? status.code
      : 1;

  if (!status.spawnFailed) {
    try {
      record({ repoRoot, capture, lane, startedAt, exitCode, command });
    } catch (e) {
      // Instrumentation must never be the reason a build reports differently
      // from cargo. This is the one branch that makes the wrapper safe to put
      // in front of a required check.
      process.stderr.write(
        `flake-record: WARNING — the run was NOT recorded (${e.message}). ` +
          `cargo's verdict is unaffected; the history has a gap.\n`
      );
    }
  }
  process.exit(exitCode);
}

function record({ repoRoot, capture, lane, startedAt, exitCode, command }) {
  const text = stripAnsi(fs.readFileSync(capture, 'utf8'));
  const pkgs = packages(repoRoot);
  const tests = parseCargoTestOutput(text, pkgs, repoRoot);

  const run = {
    schema: 1,
    startedAt,
    finishedAt: new Date().toISOString(),
    lane: lane || null,
    command: command.join(' '),
    exit: exitCode,
    branch: process.env.GITHUB_REF_NAME || git(repoRoot, ['rev-parse', '--abbrev-ref', 'HEAD'], 'unknown'),
    sha: process.env.GITHUB_SHA || git(repoRoot, ['rev-parse', 'HEAD'], null),
    os: process.platform,
    runId: process.env.GITHUB_RUN_ID || null,
    tests,
  };

  const dir = historyDir(repoRoot);
  fs.mkdirSync(dir, { recursive: true });
  const stamp = startedAt.replace(/[:.]/g, '-');
  fs.writeFileSync(
    path.join(dir, `${stamp}-${run.os}-${lane || 'suite'}.json`),
    `${JSON.stringify(run, null, 2)}\n`
  );

  if (tests.length === 0) {
    // Found-nothing and cannot-see are different sentences: a cargo run that
    // compiled nothing new and cached every result still prints its test lines,
    // so zero parsed tests means the reader broke, not that nothing ran.
    process.stderr.write(
      `flake-record: WARNING — parsed 0 test results from this run. The history got an ` +
        `empty sample, which is a hole, not a clean run.\n`
    );
  }

  announceLabels(repoRoot, run);

  if (lane) {
    // Written as a PART (`<lane>--suite.json`), never as `<lane>.json`: a perf
    // lane's Rust harness owns that name, and writing it here overwrote the
    // measured series with a test-count summary — the whole artifact, replaced
    // by a note saying the test passed. Parts merge, so a perf lane now carries
    // both its series and the fact that its harness exited 0.
    const laneDir = path.join(repoRoot, '.lanes/runs');
    fs.mkdirSync(laneDir, { recursive: true });
    const ran = tests.filter((t) => t.outcome !== 'ignored');
    fs.writeFileSync(
      path.join(laneDir, `${lane}--suite.json`),
      `${JSON.stringify(
        {
          lane,
          kind: 'suite',
          part: 'suite',
          emittedAt: run.finishedAt,
          host: { os: run.os, arch: process.arch, cpus: os.cpus().length },
          command: run.command,
          exit: exitCode,
          scalars: {
            tests_run: ran.length,
            tests_failed: ran.filter((t) => t.outcome === 'FAILED').length,
          },
          tests: ran,
        },
        null,
        2
      )}\n`
    );
  }
}

/** Remove every artifact file belonging to `lane`, so the run starts blind. */
function pruneLaneArtifacts(repoRoot, lane) {
  const dir = path.join(repoRoot, '.lanes/runs');
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return;
  }
  for (const n of names) {
    if (n === `${lane}.json` || n.startsWith(`${lane}--`)) {
      try {
        fs.unlinkSync(path.join(dir, n));
      } catch {
        /* a file we cannot remove will be overwritten or reported as stale */
      }
    }
  }
}

/**
 * Print the label where the test appears — the run's own output.
 *
 * A register nobody opens is a register nobody reads. Two populations get a
 * line: tests this run touched that are quarantined (with owner and expiry), and
 * tests this run touched that history currently labels flaky (with the full
 * predicate, never a bare percentage).
 */
function announceLabels(repoRoot, run) {
  let register;
  try {
    register = JSON.parse(fs.readFileSync(path.join(repoRoot, '.flake/register.json'), 'utf8'));
  } catch {
    return;
  }
  const touched = new Set(run.tests.map((t) => t.id));
  const touchedKeys = new Set([...touched].map(registerKey).filter(Boolean));
  const lines = [];
  for (const e of register.quarantine || []) {
    const key = registerKey(e.id);
    if (!touched.has(e.id) && !touchedKeys.has(key)) continue;
    lines.push(
      `  QUARANTINED  ${e.id}\n` +
        `               owner ${e.owner}, entered ${e.entered}, expires ${e.expires}, ` +
        `cause ${e.cause}, form ${e.form}`
    );
  }
  const windowDays = register.windowDays || 14;
  const today = new Date().toISOString().slice(0, 10);
  const tr = transitions(loadRuns(historyDir(repoRoot)), {
    branch: register.branch || 'master',
    windowDays,
    today,
  });
  for (const l of labelled(tr, register.labelThreshold ?? 2)) {
    if (!touched.has(l.id)) continue;
    lines.push(
      `  LABELLED     ${l.id}\n` +
        `               changed outcome in ${l.changed} of ${l.sameCodePairs} same-commit run ` +
        `pairs (${tr.predicate}). A label is information — this test still BLOCKS.`
    );
  }
  if (lines.length > 0) {
    process.stdout.write(`\nflake register — tests in this run:\n${lines.join('\n')}\n`);
  }
}

const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (invokedDirectly) main();
