#!/usr/bin/env node
// `just mcp-gate-parity` — asserts that the MCP surface's gated-tool vocabulary
// is spelled identically at every site that spells it.
//
// WHY THIS EXISTS
//
// The actuating MCP tools sit behind `[mcp] allow_enqueue`, which is read from
// config once and cannot change while the process runs. Because it is
// process-constant, the surface enforces it the strong way: the tools are not
// advertised at all when the switch is off, so `tools/list` shows a read-only
// surface and a client generated from it cannot even spell the calls
// (registry: security/identity-and-access/authorization ->
// unregistered-is-stronger-than-refused). That is the right construction and
// this checker is not trying to change it.
//
// What it defends is the seam that construction opens. The gated set is
// currently written out THREE times in crates/server/src/mcp/mod.rs:
//
//   1. the `if state.config.mcp.allow_enqueue { ... }` block in `server_tools`,
//      which decides what is ADVERTISED;
//   2. the guarded dispatch arms `"x" if state.config.mcp.allow_enqueue =>`,
//      which decide what is REACHABLE;
//   3. the refusal arm `"a" | "b" | "c" => Err(...)`, which decides what gets a
//      readable "the operator must set [mcp] allow_enqueue" instead of
//      `unknown tool`.
//
// Three hand-maintained copies of one closed vocabulary is a race with a delay
// fuse (registry law: one-authority-per-vocabulary). They agree today. Nothing
// in the compiler makes them agree tomorrow, and the drift directions are not
// equally loud:
//
//   listed but NOT guarded in dispatch  -> the tool is hidden while the switch
//       is off and still CALLABLE by anyone who knows its name. Silent, and it
//       actuates. This is the disclosure direction.
//   guarded but NOT listed              -> dead tool, nobody can discover it.
//   in the refusal arm only             -> unreachable; the arm is dead code.
//   listed but missing from refusal     -> calling it while gated off answers
//       `unknown tool` instead of naming the switch, so an operator debugging a
//       disabled surface is told the tool does not exist.
//
// The fix that removes the class is one authority the other sites derive from.
// Until then this is the floor: a build-time check over the registration sites.
//
// Exit codes follow this repo's lane convention: 0 clean, 2 findings,
// 3 cannot-check (the file moved or the shapes it parses are gone — which is a
// result, not a pass, because a parser that silently matches nothing is exactly
// how a checker like this rots into a green no-op).

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const EXIT_FINDINGS = 2;
export const EXIT_CANNOT_CHECK = 3;

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const defaultRepoRoot = () => path.resolve(HERE, '..', '..');
export const MCP_SOURCE = path.join(
  'crates',
  'server',
  'src',
  'mcp',
  'mod.rs'
);

const GATE = 'state.config.mcp.allow_enqueue';

// Return the substring starting at `open` (index of a `{`) up to its matching
// close brace. Rust string literals in this file can contain braces, so track
// quotes and escapes rather than counting naively.
export function braceBlock(src, open) {
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = open; i < src.length; i += 1) {
    const c = src[i];
    if (esc) {
      esc = false;
      continue;
    }
    if (c === '\\') {
      if (inStr) esc = true;
      continue;
    }
    if (c === '"') {
      inStr = !inStr;
      continue;
    }
    if (inStr) continue;
    if (c === '{') depth += 1;
    else if (c === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(open, i + 1);
    }
  }
  return null;
}

export function fnBody(src, signature) {
  const at = src.indexOf(signature);
  if (at === -1) return null;
  const open = src.indexOf('{', at);
  if (open === -1) return null;
  return braceBlock(src, open);
}

const toolNames = (chunk) =>
  [...chunk.matchAll(/"name":\s*"([a-z0-9_]+)"/g)].map((m) => m[1]);

// The three sites, each parsed from its own syntax.
export function extractSites(src) {
  const problems = [];

  const list = fnBody(src, 'fn server_tools(');
  if (!list) problems.push('cannot find fn server_tools');

  let listedGated = [];
  let listedUngated = [];
  if (list) {
    const guardAt = list.indexOf(`if ${GATE}`);
    if (guardAt === -1) {
      problems.push(`server_tools has no \`if ${GATE}\` block`);
    } else {
      const open = list.indexOf('{', guardAt);
      const block = braceBlock(list, open);
      if (!block) problems.push('unbalanced braces in the allow_enqueue block');
      else {
        listedGated = toolNames(block);
        listedUngated = toolNames(
          list.slice(0, guardAt) + list.slice(guardAt + block.length)
        );
      }
    }
  }

  const dispatch = fnBody(src, 'async fn tools_call(');
  if (!dispatch) problems.push('cannot find async fn tools_call');

  let dispatchGated = [];
  let dispatchUngated = [];
  let refusal = [];
  if (dispatch) {
    dispatchGated = [
      ...dispatch.matchAll(
        new RegExp(
          `"([a-z0-9_]+)"\\s*if\\s*${GATE.replace(/\./g, '\\.')}\\s*=>`,
          'g'
        )
      ),
    ].map((m) => m[1]);

    // The refusal arm: two or more alternated string patterns arrowing to Err.
    const alt = dispatch.match(
      /((?:"[a-z0-9_]+"\s*\|\s*)+"[a-z0-9_]+")\s*=>\s*Err\(/
    );
    if (alt) refusal = [...alt[1].matchAll(/"([a-z0-9_]+)"/g)].map((m) => m[1]);

    // Plain `"x" =>` arms with no guard and no alternation.
    dispatchUngated = [
      ...dispatch.matchAll(/^\s*"([a-z0-9_]+)"\s*=>/gm),
    ].map((m) => m[1]);
  }

  return {
    listedGated,
    listedUngated,
    dispatchGated,
    dispatchUngated,
    refusal,
    problems,
  };
}

const setOf = (a) => new Set(a);
const missing = (from, of_) => [...setOf(of_)].filter((x) => !setOf(from).has(x));

export function findings(sites) {
  const out = [];
  const { listedGated, dispatchGated, refusal, dispatchUngated } = sites;

  for (const name of missing(dispatchGated, listedGated))
    out.push(
      `'${name}' is advertised behind the gate but its dispatch arm is not guarded — ` +
        `it stays CALLABLE while the switch is off (silent, and it actuates)`
    );
  for (const name of missing(listedGated, dispatchGated))
    out.push(
      `'${name}' has a guarded dispatch arm but is never advertised — dead tool`
    );
  for (const name of missing(refusal, listedGated))
    out.push(
      `'${name}' is gated but absent from the refusal arm — calling it while ` +
        `disabled answers 'unknown tool' instead of naming [mcp] allow_enqueue`
    );
  for (const name of missing(listedGated, refusal))
    out.push(
      `'${name}' is in the refusal arm but is not a gated tool — unreachable arm`
    );
  for (const name of dispatchUngated)
    if (setOf(listedGated).has(name))
      out.push(
        `'${name}' is gated in the list but also has an UNGUARDED dispatch arm — ` +
          `the guard is bypassed`
      );

  return out;
}

export function check(root = defaultRepoRoot()) {
  const file = path.join(root, MCP_SOURCE);
  if (!fs.existsSync(file))
    return { status: 'cannot-check', reasons: [`${MCP_SOURCE} not found`] };

  const sites = extractSites(fs.readFileSync(file, 'utf8'));
  if (sites.problems.length)
    return { status: 'cannot-check', reasons: sites.problems, sites };

  // A parser that matched nothing must not report clean. The gated set is
  // non-empty by construction: the surface has actuating tools.
  if (sites.listedGated.length === 0)
    return {
      status: 'cannot-check',
      reasons: [
        'parsed zero gated tools — the shape this checker reads has changed',
      ],
      sites,
    };

  const problems = findings(sites);
  return {
    status: problems.length ? 'findings' : 'clean',
    reasons: problems,
    sites,
  };
}

const isMain =
  process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1]);

if (isMain) {
  const result = check();
  const s = result.sites;
  if (s) {
    console.log(`gated (advertised): ${s.listedGated.join(', ') || '-'}`);
    console.log(`gated (dispatch)  : ${s.dispatchGated.join(', ') || '-'}`);
    console.log(`refusal arm       : ${s.refusal.join(', ') || '-'}`);
    console.log(`ungated tools     : ${s.listedUngated.join(', ') || '-'}`);
  }
  if (result.status === 'clean') {
    console.log('\nmcp gate parity OK — all three sites spell one vocabulary');
    process.exit(0);
  }
  console.error(`\n${result.status}:`);
  for (const r of result.reasons) console.error(`  - ${r}`);
  process.exit(
    result.status === 'cannot-check' ? EXIT_CANNOT_CHECK : EXIT_FINDINGS
  );
}
