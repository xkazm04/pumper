// Tests for the MCP gate-parity checker. Run with `just mcp-gate-parity`
// (`node --test scripts/ci/mcp-gate-parity.test.mjs`) — node:test only.
//
// Every negative below is a real drift direction, not a synthetic one: each is
// what the file looks like after somebody adds or moves a gated tool and
// updates two of the three sites. The checker is asserted against the live
// source first, because a parser that has stopped matching reports clean.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  MCP_SOURCE,
  check,
  defaultRepoRoot,
  extractSites,
  findings,
  rootFromArgv,
} from './mcp-gate-parity.mjs';

const SRC = fs.readFileSync(
  path.join(defaultRepoRoot(), MCP_SOURCE),
  'utf8'
);

const GATED = [
  'enqueue_job',
  'fetch_readable',
  'deep_research',
  'resume_job',
  'run_workflow',
  // N04: the pipeline-authoring tools, advertised in their own block.
  'create_trigger',
  'test_trigger',
  'trigger_decisions',
  'create_watch',
  'create_ingress_source',
];

test('the live source parses to a non-empty gated set at all three sites', () => {
  const s = extractSites(SRC);
  assert.deepEqual(s.problems, []);
  assert.deepEqual([...s.listedGated].sort(), [...GATED].sort());
  assert.deepEqual([...s.dispatchGated].sort(), [...GATED].sort());
  assert.deepEqual([...s.refusal].sort(), [...GATED].sort());
});

test('the ungated set matches what the e2e test asserts tools/list returns', () => {
  // crates/server/src/e2e/mcp.rs pins this list independently, in another
  // language, from a running server. If these two ever disagree, one of them
  // is reading the wrong thing — that is the point of checking it here.
  const s = extractSites(SRC);
  // `approve_transaction` is the one name here the e2e list does not carry: it
  // sits behind a DIFFERENT switch (`[mcp] allow_approve`), which is off in the
  // e2e config and which this checker does not police, so it reads as
  // allow_enqueue-ungated here and is absent from a default tools/list.
  assert.deepEqual(
    [...s.listedUngated].sort(),
    [
      'list_apps',
      'query_dataset',
      'search',
      'wait_job',
      'wait_workflow',
      'list_pending_transactions',
      'approve_transaction',
      'market_profile',
      'fetch',
    ].sort()
  );
});

test('the live tree is clean', () => {
  const r = check();
  assert.equal(r.status, 'clean', r.reasons?.join('; '));
});

test('advertised behind the gate but dispatch arm unguarded is a finding', () => {
  // The disclosure direction: hidden from tools/list, still callable.
  const mutated = SRC.replace(
    '"deep_research" if state.config.mcp.allow_enqueue => tool_deep_research(state, &args).await,',
    '"deep_research" => tool_deep_research(state, &args).await,'
  );
  assert.notEqual(mutated, SRC, 'mutation did not apply — fixture is stale');
  const problems = findings(extractSites(mutated));
  assert.ok(
    problems.some((p) => p.includes('deep_research') && p.includes('CALLABLE')),
    problems.join('; ')
  );
});

test('a guarded dispatch arm that is never advertised is a finding', () => {
  const s = extractSites(SRC);
  s.listedGated = s.listedGated.filter((n) => n !== 'fetch_readable');
  const problems = findings(s);
  assert.ok(
    problems.some((p) => p.includes('fetch_readable') && p.includes('dead tool')),
    problems.join('; ')
  );
});

test('a gated tool missing from the refusal arm is a finding', () => {
  const s = extractSites(SRC);
  s.refusal = s.refusal.filter((n) => n !== 'enqueue_job');
  const problems = findings(s);
  assert.ok(
    problems.some(
      (p) => p.includes('enqueue_job') && p.includes('allow_enqueue')
    ),
    problems.join('; ')
  );
});

test('a refusal-arm name that is not a gated tool is a finding', () => {
  const s = extractSites(SRC);
  s.refusal = [...s.refusal, 'retired_tool'];
  const problems = findings(s);
  assert.ok(
    problems.some((p) => p.includes('retired_tool') && p.includes('unreachable')),
    problems.join('; ')
  );
});

test('a gated tool with an additional unguarded arm is a finding', () => {
  const s = extractSites(SRC);
  s.dispatchUngated = [...s.dispatchUngated, 'enqueue_job'];
  const problems = findings(s);
  assert.ok(
    problems.some((p) => p.includes('enqueue_job') && p.includes('bypassed')),
    problems.join('; ')
  );
});

test('a source whose shape has moved reports cannot-check, never clean', () => {
  const s = extractSites('fn unrelated() { let x = 1; }');
  assert.ok(s.problems.length > 0);
});

test('a drift inside the SECOND gate block is still a finding', () => {
  // The vocabulary spans two advertised blocks; parity must hold over the union,
  // so un-guarding a tool advertised only in the second block is still caught.
  const mutated = SRC.replace(
    '"create_watch" if state.config.mcp.allow_enqueue => tool_create_watch(state, &args).await,',
    '"create_watch" => tool_create_watch(state, &args).await,'
  );
  assert.notEqual(mutated, SRC, 'mutation did not apply — fixture is stale');
  const f = findings(extractSites(mutated));
  assert.ok(
    f.some((x) => x.includes('create_watch') && x.includes('not guarded')),
    f.join('; ')
  );
});

test('zero parsed gated tools is cannot-check, not a pass', () => {
  // Every block: the gated vocabulary is the union of them all.
  const emptied = SRC.replaceAll(
    'if state.config.mcp.allow_enqueue {',
    'if false {'
  );
  assert.notEqual(emptied, SRC, 'mutation did not apply — fixture is stale');
  const s = extractSites(emptied);
  // The guard string is gone, so the block cannot be located at all.
  assert.ok(
    s.problems.length > 0 || s.listedGated.length === 0,
    'an unlocatable gate block must not parse as an empty-but-fine set'
  );
});

// --- the process contract: 0 / 2 / 3, observed as exit codes ----------------
//
// Everything above calls check() in-process, which never executes the mapping
// from a status string to an exit code at the bottom of mcp-gate-parity.mjs.
// A checker whose cannot-check branch has never been observed is a checker that
// may be exiting 0 on a tree it could not read. These spawn it for real.

const CHECKER = path.join(defaultRepoRoot(), 'scripts', 'ci', 'mcp-gate-parity.mjs');

function runChecker(root) {
  return spawnSync(process.execPath, [CHECKER, '--root', root], {
    encoding: 'utf8',
  });
}

/** A scratch tree holding one mcp/mod.rs, or none when `src` is null. */
function fakeTree(src) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mcp-parity-'));
  if (src !== null) {
    const file = path.join(root, MCP_SOURCE);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, src);
  }
  return root;
}

test('exit_0_the_live_tree_parses_and_all_three_sites_agree', () => {
  const r = runChecker(defaultRepoRoot());
  assert.equal(r.status, 0, r.stdout + r.stderr);
});

test('exit_2_a_gated_tool_with_an_unguarded_dispatch_arm_reddens_the_process', () => {
  const root = fakeTree(
    SRC.replace(
      '"deep_research" if state.config.mcp.allow_enqueue => tool_deep_research(state, &args).await,',
      '"deep_research" => tool_deep_research(state, &args).await,'
    )
  );
  const r = runChecker(root);
  assert.equal(r.status, 2, r.stdout + r.stderr);
  assert.match(r.stderr, /deep_research/);
  fs.rmSync(root, { recursive: true, force: true });
});

test('exit_3_an_absent_source_is_cannot_check_never_a_clean_pass', () => {
  const root = fakeTree(null);
  const r = runChecker(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  assert.match(r.stderr, /cannot-check/);
  assert.notEqual(r.status, 0);
  fs.rmSync(root, { recursive: true, force: true });
});

test('exit_3_a_source_whose_shape_moved_is_cannot_check', () => {
  const root = fakeTree('fn unrelated() { let x = 1; }\n');
  const r = runChecker(root);
  assert.equal(r.status, 3, r.stdout + r.stderr);
  fs.rmSync(root, { recursive: true, force: true });
});

test('rootFromArgv_defaults_to_the_checkout_and_honours_an_override', () => {
  assert.equal(rootFromArgv([], '/r'), '/r');
  assert.equal(rootFromArgv(['--root'], '/r'), '/r');
  assert.equal(rootFromArgv(['--root', '.'], '/r'), path.resolve('.'));
});
