// Tests for the doc-sync Stop hook. Run with `just doc-sync`
// (`node --test scripts/docs/`) — no dependencies, node:test only.
//
// This hook detected nothing for its entire life because nothing exercised it.
// Every test below is named for the anti-pattern it defends against, and the
// transcript fixtures under fixtures/ carry the envelope Claude Code actually
// records — see fixtures/README.md.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  collectEditedFilesFromTranscript,
  evaluateEditedFiles,
  isToolResultEntry,
  isUserTurnBoundary,
  normalizeEditedPath,
} from './check-doc-sync.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(HERE, '../..');
const FIXTURES = path.join(HERE, 'fixtures');
const HOOK = path.join(HERE, 'check-doc-sync.mjs');
const MAP = JSON.parse(fs.readFileSync(path.join(HERE, 'feature-doc-map.json'), 'utf8'));

/** Materialize a fixture with {{REPO}} bound to this checkout. */
function materialize(name) {
  const raw = fs.readFileSync(path.join(FIXTURES, name), 'utf8');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'doc-sync-'));
  const out = path.join(dir, name);
  fs.writeFileSync(out, raw.split('{{REPO}}').join(REPO_ROOT.split(path.sep).join('/')));
  return out;
}

/** Run the hook exactly as .claude/settings.json does: payload on stdin. */
function runHook(payload) {
  return spawnSync(process.execPath, [HOOK], {
    input: JSON.stringify(payload),
    encoding: 'utf8',
    env: { ...process.env, CLAUDE_PROJECT_DIR: REPO_ROOT },
  });
}

function editsIn(name) {
  return collectEditedFilesFromTranscript(materialize(name), REPO_ROOT);
}

// --- the boundary predicate -------------------------------------------------

test('a_tool_result_is_not_a_user_turn_boundary', () => {
  // The exact shape Claude Code records for a tool result (see fixtures/README.md).
  const toolResult = {
    type: 'user',
    message: { role: 'user', content: [{ type: 'tool_result', content: 'x', tool_use_id: 't1' }] },
    toolUseResult: { filePath: 'x' },
    sourceToolAssistantUUID: 'a1',
  };
  assert.equal(isToolResultEntry(toolResult), true);
  assert.equal(isUserTurnBoundary(toolResult), false);
});

test('a_tool_result_without_the_claude_code_annotations_is_still_not_a_boundary', () => {
  // Degradation case: if the envelope keys ever vanish, the Messages API
  // content shape alone must still keep this out of the boundary set.
  const bare = {
    type: 'user',
    message: { role: 'user', content: [{ type: 'tool_result', content: 'x', tool_use_id: 't1' }] },
  };
  assert.equal(isToolResultEntry(bare), true);
  assert.equal(isUserTurnBoundary(bare), false);
});

test('a_system_reminder_injection_is_not_a_user_turn_boundary', () => {
  const meta = { type: 'user', isMeta: true, message: { role: 'user', content: '<system-reminder>' } };
  assert.equal(isUserTurnBoundary(meta), false);
});

test('a_genuine_prompt_is_still_a_user_turn_boundary', () => {
  assert.equal(
    isUserTurnBoundary({ type: 'user', message: { role: 'user', content: 'do the thing' } }),
    true,
  );
  assert.equal(
    isUserTurnBoundary({
      type: 'user',
      message: { role: 'user', content: [{ type: 'text', text: 'do the thing' }] },
    }),
    true,
  );
  assert.equal(isUserTurnBoundary({ type: 'assistant', message: { role: 'assistant' } }), false);
});

// --- scanning a real turn ---------------------------------------------------

test('an_edit_after_tool_results_is_not_invisible', () => {
  const edited = editsIn('turn-with-edits.jsonl');
  assert.deepEqual([...edited], ['crates/core/src/datasets.rs']);
});

test('a_previous_turns_edit_is_not_attributed_to_this_turn', () => {
  assert.deepEqual([...editsIn('turn-previous-turn-edits.jsonl')], []);
});

test('an_edit_outside_the_repo_is_not_this_repos_feature_source', () => {
  assert.deepEqual([...editsIn('turn-outside-repo.jsonl')], []);
  assert.equal(normalizeEditedPath('/somewhere/else/foo.rs', '/repo/root'), null);
  assert.equal(
    normalizeEditedPath(path.join(REPO_ROOT, 'crates/core/src/datasets.rs'), REPO_ROOT),
    'crates/core/src/datasets.rs',
  );
});

// --- the decision -----------------------------------------------------------

test('mapped_source_without_a_doc_edit_is_not_silent', () => {
  const verdict = evaluateEditedFiles(new Set(['crates/core/src/datasets.rs']), MAP);
  assert.equal(verdict.fired, true);
  assert.ok(verdict.docHits.has('docs/features/datasets.md'));
});

test('a_turn_that_updated_the_doc_is_not_nagged', () => {
  const verdict = evaluateEditedFiles(
    new Set(['crates/core/src/doctor.rs', 'docs/features/datasets.md']),
    MAP,
  );
  assert.equal(verdict.fired, false);
  assert.equal(verdict.reason, 'docs-touched');
});

test('a_test_only_turn_is_not_nagged', () => {
  const verdict = evaluateEditedFiles(new Set(['crates/core/tests/datasets.rs']), MAP);
  assert.equal(verdict.fired, false);
  assert.equal(verdict.reason, 'skipped-only');
});

test('a_read_only_turn_is_not_nagged', () => {
  const verdict = evaluateEditedFiles(new Set(), MAP);
  assert.equal(verdict.fired, false);
  assert.equal(verdict.reason, 'no-edits');
});

// --- end to end, through the hook's own stdin contract ----------------------

test('the_hook_exits_2_on_a_turn_that_edited_mapped_source', () => {
  const r = runHook({ transcript_path: materialize('turn-with-edits.jsonl') });
  assert.equal(r.status, 2, r.stderr);
  assert.match(r.stderr, /docs\/features\/datasets\.md/);
});

test('the_hook_is_silent_on_a_read_only_turn', () => {
  const r = runHook({ transcript_path: materialize('turn-without-edits.jsonl') });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stderr, '');
});

test('the_hook_is_silent_when_the_doc_was_updated_too', () => {
  const r = runHook({ transcript_path: materialize('turn-docs-touched.jsonl') });
  assert.equal(r.status, 0, r.stderr);
});

test('the_hook_is_silent_on_a_test_only_turn', () => {
  const r = runHook({ transcript_path: materialize('turn-test-only.jsonl') });
  assert.equal(r.status, 0, r.stderr);
});

test('a_stop_hook_reentry_is_not_a_second_reminder', () => {
  const r = runHook({
    transcript_path: materialize('turn-with-edits.jsonl'),
    stop_hook_active: true,
  });
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stderr, '');
});

test('a_missing_transcript_is_not_a_hook_crash', () => {
  const r = runHook({ transcript_path: path.join(os.tmpdir(), 'no-such-transcript.jsonl') });
  assert.equal(r.status, 0, r.stderr);
});

test('a_transcript_argument_replays_the_hook_without_a_payload', () => {
  const r = spawnSync(process.execPath, [HOOK, materialize('turn-with-edits.jsonl')], {
    encoding: 'utf8',
    env: { ...process.env, CLAUDE_PROJECT_DIR: REPO_ROOT },
  });
  assert.equal(r.status, 2, r.stderr);
});

// --- canary: the recorded transcript shape has not moved --------------------

test('the_recorded_tool_result_shape_has_not_drifted_from_the_fixtures', (t) => {
  const dir = path.join(
    os.homedir(),
    '.claude/projects',
    REPO_ROOT.split(path.sep).join('-').replace(/[:\\/]/g, '-'),
  );
  const projects = path.join(os.homedir(), '.claude', 'projects');
  let transcripts = [];
  if (fs.existsSync(dir)) {
    transcripts = fs
      .readdirSync(dir)
      .filter((f) => f.endsWith('.jsonl'))
      .map((f) => path.join(dir, f));
  } else if (fs.existsSync(projects)) {
    const guess = fs.readdirSync(projects).find((d) => d.toLowerCase().includes('pumper'));
    if (guess) {
      const gd = path.join(projects, guess);
      transcripts = fs
        .readdirSync(gd)
        .filter((f) => f.endsWith('.jsonl'))
        .map((f) => path.join(gd, f));
    }
  }
  if (transcripts.length === 0) {
    t.skip('no recorded transcripts on this machine');
    return;
  }
  // Real transcripts must still contain user-role entries that are tool
  // results — i.e. the failure mode this hook was blind to still exists.
  let sawToolResultUserEntry = false;
  let sawGenuinePrompt = false;
  for (const f of transcripts.slice(-5)) {
    for (const line of fs.readFileSync(f, 'utf8').split('\n')) {
      if (!line) continue;
      let evt;
      try {
        evt = JSON.parse(line);
      } catch {
        continue;
      }
      if (evt.type !== 'user' || evt.message?.role !== 'user') continue;
      if (isToolResultEntry(evt)) sawToolResultUserEntry = true;
      else if (isUserTurnBoundary(evt)) sawGenuinePrompt = true;
    }
  }
  assert.ok(sawToolResultUserEntry, 'no tool-result user entries found — transcript format moved');
  assert.ok(sawGenuinePrompt, 'no genuine user prompts classified — boundary predicate is too strict');
});
