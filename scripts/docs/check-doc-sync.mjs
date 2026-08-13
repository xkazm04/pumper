#!/usr/bin/env node
// Stop hook: nudge Claude when feature source changed in this turn but the
// coupled feature doc under docs/features/ was not updated.
//
// Adapted from the personas repo's three-surface checker; pumper has ONE docs
// surface (docs/features/). The design choice is per-session gap-prevention,
// not a periodic catch-up: development happens through Claude CLI sessions
// with no second human reviewer, so drift compounds per session unless every
// session leaves the docs consistent with what it changed.
//
// Triggered by .claude/settings.json -> hooks.Stop. Reads the JSONL
// transcript at $payload.transcript_path, scans the most recent assistant
// turn for Edit/Write/MultiEdit/NotebookEdit calls, and matches edited paths
// against scripts/docs/feature-doc-map.json. Honors `stop_hook_active`.
//
// Dismiss path: if the change is internal-only (refactor, bugfix without
// behavior shift, test-only), reply with one short sentence acknowledging
// "internal-only, no doc update needed" and stop.
//
// ---------------------------------------------------------------------------
// TURN BOUNDARY — why this is not just `evt.type === 'user'`
//
// This hook silently detected nothing for its entire life (replayed over all
// 31 recorded transcripts of this project: 1,136 Edit/Write tool calls, zero
// detections). The backward scan stopped at the first `type:'user'` +
// `message.role:'user'` entry, but Claude Code records every TOOL RESULT in
// exactly that shape — 3,837 of 4,187 user-role entries in those transcripts
// are tool results. A turn's last entries are almost always tool results, so
// the scan broke on line one and the edited set was always empty.
//
// The fix layers three independent signals, primary first, so the predicate
// degrades safely if any one of them changes shape:
//
//   1. CONTENT SHAPE (primary). A tool result is a user-role message whose
//      content is entirely `tool_result` blocks. That is the Anthropic
//      Messages API wire format, which the transcript embeds verbatim — a
//      public, versioned contract, unlike the envelope around it.
//   2. TRANSCRIPT ANNOTATIONS (corroborating). Claude Code tags those same
//      entries with `toolUseResult` / `sourceToolAssistantUUID`. Internal
//      fields, so they are a second opinion and never the only one.
//   3. `isMeta` (corroborating). Synthetic user-role injections — system
//      reminders, command output — are not a human prompt either, and
//      stopping on one truncates the scan exactly like the original bug.
//
// Rejecting on any signal is the conservative direction: a missed boundary
// widens the scan (worst case, attributing an older edit to this turn), while
// a false boundary re-creates the silent-failure bug. Proven by
// check-doc-sync.test.mjs against fixtures carrying the recorded envelope;
// run it with `just doc-sync`.
// ---------------------------------------------------------------------------

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const EDIT_TOOLS = ['Edit', 'Write', 'MultiEdit', 'NotebookEdit'];

export const SKIP_PATTERNS = [
  /\/tests\//,
  /_test\.rs$/,
  /^docs\//,
  /^catalog\//,
  /^plugins-src\//,
  /^target\//,
  /Cargo\.lock$/,
  /^\.claude\//,
  /^\.perfect\//,
  /^scripts\//,
];

export function defaultRepoRoot() {
  return process.env.CLAUDE_PROJECT_DIR || process.cwd();
}

export function mapPath(repoRoot = defaultRepoRoot()) {
  return path.join(repoRoot, 'scripts/docs/feature-doc-map.json');
}

function readStdin() {
  try {
    return fs.readFileSync(0, 'utf8');
  } catch {
    return '';
  }
}

function safeJson(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

/**
 * True when a transcript entry is a tool result wearing a user-role costume.
 *
 * Primary signal is the content shape (Messages API wire format); the two
 * Claude Code envelope annotations corroborate it. Any one is enough, so the
 * predicate survives either half of the format changing.
 */
export function isToolResultEntry(evt) {
  if (!evt || typeof evt !== 'object') return false;
  if (evt.toolUseResult !== undefined) return true;
  if (typeof evt.sourceToolAssistantUUID === 'string') return true;
  const content = evt.message?.content;
  if (Array.isArray(content) && content.length > 0) {
    return content.every((block) => block?.type === 'tool_result');
  }
  return false;
}

/**
 * True only for an entry that is a genuine human prompt — the point at which
 * the backward scan over this turn should stop.
 */
export function isUserTurnBoundary(evt) {
  if (!evt || evt.type !== 'user' || evt.message?.role !== 'user') return false;
  if (isToolResultEntry(evt)) return false;
  if (evt.isMeta) return false;
  const content = evt.message.content;
  if (typeof content === 'string') return content.trim().length > 0;
  if (Array.isArray(content)) return content.some((block) => block?.type !== 'tool_result');
  return false;
}

/**
 * Repo-relative, forward-slashed path for an edited file, or null when the
 * edit landed outside this repo. Sessions routinely edit sibling checkouts
 * (`../politicas/...`) and the user's memory dir; those are not this repo's
 * feature source and must never be map-matched.
 */
export function normalizeEditedPath(filePath, repoRoot = defaultRepoRoot()) {
  if (typeof filePath !== 'string' || filePath.length === 0) return null;
  const root = path.resolve(repoRoot);
  const rel = path.relative(root, path.resolve(root, filePath)).split(path.sep).join('/');
  if (rel === '' || rel === '..' || rel.startsWith('../')) return null;
  return rel;
}

export function compileGlob(pattern) {
  const re = pattern
    .split('/')
    .map((segment) => {
      if (segment === '**') return '__GLOBSTAR__';
      return segment
        .replace(/[.+?^${}()|[\]\\]/g, '\\$&')
        .replace(/\*/g, '[^/]*');
    })
    .join('/')
    .replace(/\/__GLOBSTAR__\//g, '(/.*)?/')
    .replace(/^__GLOBSTAR__\//, '(.*/)?')
    .replace(/\/__GLOBSTAR__$/, '(/.*)?')
    .replace(/__GLOBSTAR__/g, '.*');
  return new RegExp(`^${re}$`);
}

export function collectEditedFilesFromTranscript(transcriptPath, repoRoot = defaultRepoRoot()) {
  if (!transcriptPath || !fs.existsSync(transcriptPath)) return new Set();
  const lines = fs.readFileSync(transcriptPath, 'utf8').split('\n').filter(Boolean);
  const edited = new Set();
  // Walk backwards until the most recent GENUINE user prompt; assistant events
  // after that boundary are this turn's tool calls. Tool results share the
  // user role and must not end the scan — see the TURN BOUNDARY note above.
  for (let i = lines.length - 1; i >= 0; i--) {
    const evt = safeJson(lines[i]);
    if (!evt) continue;
    if (isUserTurnBoundary(evt)) break;
    if (evt.type !== 'assistant') continue;
    const content = evt.message?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type !== 'tool_use') continue;
      if (!EDIT_TOOLS.includes(block.name)) continue;
      const rel = normalizeEditedPath(block.input?.file_path, repoRoot);
      if (rel) edited.add(rel);
    }
  }
  return edited;
}

/**
 * The whole decision, as one pure function over an edited-path set: which
 * feature docs this turn should have touched, or why the hook stays silent.
 * Returns { fired, reason, docHits } where docHits maps doc path -> files.
 */
export function evaluateEditedFiles(edited, map) {
  const editedArr = [...edited];
  if (editedArr.length === 0) return { fired: false, reason: 'no-edits', docHits: new Map() };

  const meaningful = editedArr.filter((f) => !SKIP_PATTERNS.some((re) => re.test(f)));
  if (meaningful.length === 0) return { fired: false, reason: 'skipped-only', docHits: new Map() };

  const compiled = (map?.entries || []).map((entry) => ({
    doc: entry.doc,
    matchers: (entry.sourceGlobs || []).map(compileGlob),
  }));

  const docHits = new Map(); // doc path -> [files that triggered it]
  for (const f of meaningful) {
    for (const entry of compiled) {
      if (!entry.matchers.some((re) => re.test(f))) continue;
      if (!docHits.has(entry.doc)) docHits.set(entry.doc, []);
      docHits.get(entry.doc).push(f);
    }
  }
  if (docHits.size === 0) return { fired: false, reason: 'unmapped', docHits };

  // Satisfaction is PER ENTRY: a mapped doc is answered by editing THAT doc.
  //
  // This used to be a blanket "any `docs/features/*` edit silences everything",
  // which had two failure modes pulling in opposite directions. It let an edit
  // to one feature doc suppress the reminder for an unrelated one — the loud
  // failure. And it hardcoded `docs/features/` as the only satisfying prefix, so
  // a map entry whose `doc` lives elsewhere (ONBOARDING.md) would fire a
  // reminder the author **could not clear by editing the very file named in
  // it** — the quiet failure, and the worse of the two: a nag that cannot be
  // answered is one people learn to ignore, which is how this hook's silence
  // went unnoticed for its whole life in the first place.
  const unanswered = new Map([...docHits].filter(([doc]) => !edited.has(doc)));
  if (unanswered.size === 0) return { fired: false, reason: 'docs-touched', docHits: new Map() };
  return { fired: true, reason: 'mapped-source-without-doc', docHits: unanswered };
}

export function formatReminder(docHits) {
  const summary = [...docHits.entries()]
    .map(([doc, files]) => {
      const head = files.slice(0, 4).join(', ');
      const tail = files.length > 4 ? ` (+${files.length - 4} more)` : '';
      return `  - ${doc} <- ${head}${tail}`;
    })
    .join('\n');

  return (
    `Doc-sync reminder: this turn edited feature source without updating the doc mapped to it.\n\n` +
    `Mapped feature doc(s) likely affected:\n${summary}\n\n` +
    `Per CLAUDE.md "Documentation Sync": if the change is user/API-visible (new endpoint or\n` +
    `param, changed dataset shape, new app, changed trigger/webhook contract, new config key),\n` +
    `update the doc in this same session. If it is internal-only (refactor, bugfix without\n` +
    `behavior shift), dismiss with one short sentence — e.g. "internal-only, no doc update\n` +
    `needed" — and stop.\n`
  );
}

function main() {
  // `node scripts/docs/check-doc-sync.mjs <transcript.jsonl>` replays the hook
  // over a recorded transcript without a hook payload — see `just doc-sync`.
  const argvTranscript = process.argv[2];
  const payload = argvTranscript
    ? { transcript_path: argvTranscript }
    : safeJson(readStdin()) || {};
  if (payload.stop_hook_active) process.exit(0);

  const repoRoot = defaultRepoRoot();
  const edited = collectEditedFilesFromTranscript(payload.transcript_path, repoRoot);

  let map;
  try {
    map = JSON.parse(fs.readFileSync(mapPath(repoRoot), 'utf8'));
  } catch {
    process.exit(0);
  }

  const { fired, docHits } = evaluateEditedFiles(edited, map);
  if (!fired) process.exit(0);

  process.stderr.write(formatReminder(docHits));
  process.exit(2);
}

const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (invokedDirectly) main();
