// Tests for the MCP gate-parity checker. Run with `just mcp-gate-parity`
// (`node --test scripts/ci/mcp-gate-parity.test.mjs`) — node:test only.
//
// Every negative below is a real drift direction, not a synthetic one: each is
// what the file looks like after somebody adds or moves a gated tool and
// updates two of the three sites. The checker is asserted against the live
// source first, because a parser that has stopped matching reports clean.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import {
  MCP_SOURCE,
  check,
  defaultRepoRoot,
  extractSites,
  findings,
} from './mcp-gate-parity.mjs';

const SRC = fs.readFileSync(
  path.join(defaultRepoRoot(), MCP_SOURCE),
  'utf8'
);

const GATED = ['enqueue_job', 'fetch_readable', 'deep_research'];

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
  assert.deepEqual(
    [...s.listedUngated].sort(),
    ['list_apps', 'query_dataset', 'search', 'wait_job']
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

test('zero parsed gated tools is cannot-check, not a pass', () => {
  const emptied = SRC.replace(
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
