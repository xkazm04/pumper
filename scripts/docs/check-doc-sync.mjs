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

import fs from 'node:fs';
import path from 'node:path';

const REPO_ROOT = process.env.CLAUDE_PROJECT_DIR || process.cwd();
const MAP_PATH = path.join(REPO_ROOT, 'scripts/docs/feature-doc-map.json');

const SKIP_PATTERNS = [
  /\/tests\//,
  /_test\.rs$/,
  /^docs\//,
  /^catalog\//,
  /^plugins-src\//,
  /^target\//,
  /Cargo\.lock$/,
  /^\.claude\//,
  /^scripts\//,
];

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

function normalize(p) {
  return path.relative(REPO_ROOT, p).split(path.sep).join('/');
}

function compileGlob(pattern) {
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

function collectEditedFilesFromTranscript(transcriptPath) {
  if (!transcriptPath || !fs.existsSync(transcriptPath)) return new Set();
  const lines = fs.readFileSync(transcriptPath, 'utf8').split('\n').filter(Boolean);
  const edited = new Set();
  // Walk backwards until the most recent user message; assistant events after
  // that boundary are this turn's tool calls.
  for (let i = lines.length - 1; i >= 0; i--) {
    const evt = safeJson(lines[i]);
    if (!evt) continue;
    if (evt.type === 'user' && evt.message?.role === 'user') break;
    if (evt.type !== 'assistant') continue;
    const content = evt.message?.content;
    if (!Array.isArray(content)) continue;
    for (const block of content) {
      if (block.type !== 'tool_use') continue;
      if (!['Edit', 'Write', 'MultiEdit', 'NotebookEdit'].includes(block.name)) continue;
      const fp = block.input?.file_path;
      if (typeof fp === 'string' && fp.length) edited.add(normalize(fp));
    }
  }
  return edited;
}

function main() {
  const payload = safeJson(readStdin()) || {};
  if (payload.stop_hook_active) process.exit(0);

  const edited = collectEditedFilesFromTranscript(payload.transcript_path);
  if (edited.size === 0) process.exit(0);

  const editedArr = [...edited];
  const docsTouched = editedArr.some((f) => f.startsWith('docs/features/'));
  if (docsTouched) process.exit(0);

  const meaningful = editedArr.filter((f) => !SKIP_PATTERNS.some((re) => re.test(f)));
  if (meaningful.length === 0) process.exit(0);

  let map;
  try {
    map = JSON.parse(fs.readFileSync(MAP_PATH, 'utf8'));
  } catch {
    process.exit(0);
  }

  const compiled = (map.entries || []).map((entry) => ({
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
  if (docHits.size === 0) process.exit(0);

  const summary = [...docHits.entries()]
    .map(([doc, files]) => {
      const head = files.slice(0, 4).join(', ');
      const tail = files.length > 4 ? ` (+${files.length - 4} more)` : '';
      return `  - ${doc} <- ${head}${tail}`;
    })
    .join('\n');

  process.stderr.write(
    `Doc-sync reminder: this turn edited feature source but no docs/features/* was touched.\n\n` +
    `Mapped feature doc(s) likely affected:\n${summary}\n\n` +
    `Per CLAUDE.md "Documentation Sync": if the change is user/API-visible (new endpoint or\n` +
    `param, changed dataset shape, new app, changed trigger/webhook contract, new config key),\n` +
    `update the doc in this same session. If it is internal-only (refactor, bugfix without\n` +
    `behavior shift), dismiss with one short sentence — e.g. "internal-only, no doc update\n` +
    `needed" — and stop.\n`,
  );
  process.exit(2);
}

main();
