// The gate registry's inventory test — `node --test scripts/ci/gate-inventory.test.mjs`
// (also `just harness-test`). node:test only, no dependencies.
//
// WHY THIS EXISTS
//
// This repo built five good gates and then described their coverage THREE times
// by hand: `just harness-test` named three test files, `just inventory` named
// three others, and the CI workflow named two. Those lists had already drifted —
// `disk-check` was a blocking rung of `just ci` that the merge pipeline never
// ran, and whose fixture suite the merge pipeline never ran either.
//
// .claude/CLAUDE.md: "conventions are enforced with an inventory test (the
// EXPECTED-diff idiom), not with a sentence in a doc." The 0/2/3 contract was a
// sentence in five file headers. This is the inventory test that policy demands.
//
// WHAT IT ASSERTS, per scripts/ci/gates.json
//
//   1. DISCOVERY, both directions. A script under scripts/ci or scripts/docs
//      that exports EXIT_CANNOT_CHECK — the repo's own definition of a gate —
//      and has no registry entry is red; so is a registry entry whose script
//      the tree no longer has.
//   2. PROOF OF EACH OUTCOME. Each gate's paired test must assert every exit
//      code it declares against a SPAWNED process. An in-process call to the
//      checker's pure helpers does not exercise the exit-code mapping, which is
//      exactly how disk-check and mcp-gate-parity came to have zero process-
//      level coverage of a contract their headers stated in full.
//   3. ENFORCEMENT BINDING. The gate's verdict runs on the binding rung (a CI
//      step by name) and locally (a justfile recipe), and its fixture suite runs
//      in both too. A check that runs locally and not on the merge pipeline is a
//      courtesy, not a gate.
//   4. THE RECIPE BODIES ARE A PROJECTION, not a second list: every `node --test`
//      line inside a declared fixture recipe must map back to a registered gate.
//      Deleting a line from `harness-test` reddens here.
//
// THE CANARY
//
// Discovery strips comments before it matches, and a source-scanning matcher
// that has stopped matching reports a clean codebase in a voice indistinguishable
// from success. `discovery_ignores_the_predicate_when_it_appears_only_in_a_comment`
// plants both halves — a file that merely TALKS about the predicate and a file
// that really exports it — and fails if the scanner cannot tell them apart.

import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(HERE, '..', '..');
export const REGISTRY_PATH = path.join(HERE, 'gates.json');

const registry = JSON.parse(fs.readFileSync(REGISTRY_PATH, 'utf8'));
const GATES = registry.gates;

// CRLF normalised on the way in: `.` in a JS regex does not match `\r`, so a
// line-anchored matcher over a CRLF checkout silently matches NOTHING — the
// precise failure mode this file exists to catch.
const read = (rel) =>
  fs.readFileSync(path.join(REPO_ROOT, rel), 'utf8').replace(/\r\n/g, '\n');
const exists = (rel) => fs.existsSync(path.join(REPO_ROOT, rel));
const posix = (p) => p.split(path.sep).join('/');

// --- comment stripping ------------------------------------------------------

/**
 * Remove `//` and block comments from JavaScript source, leaving string and
 * template literals intact.
 *
 * A gate that matches raw source cannot tell a rule from a sentence ABOUT the
 * rule. This repo has been bitten by that twice, so every match below runs on
 * stripped text and `discovery_ignores_the_predicate_when_it_appears_only_in_a_comment`
 * proves the stripper still works.
 */
export function stripJsComments(src) {
  let out = '';
  let i = 0;
  let quote = null;
  while (i < src.length) {
    const c = src[i];
    const d = src[i + 1];
    if (quote !== null) {
      out += c;
      if (c === '\\') {
        out += src[i + 1] ?? '';
        i += 2;
        continue;
      }
      if (c === quote) quote = null;
      i += 1;
      continue;
    }
    if (c === '"' || c === "'" || c === '`') {
      quote = c;
      out += c;
      i += 1;
      continue;
    }
    if (c === '/' && d === '/') {
      while (i < src.length && src[i] !== '\n') i += 1;
      continue;
    }
    if (c === '/' && d === '*') {
      i += 2;
      while (i < src.length && !(src[i] === '*' && src[i + 1] === '/')) i += 1;
      i += 2;
      continue;
    }
    out += c;
    i += 1;
  }
  return out;
}

/** Strip `#` line comments from a justfile, keeping line structure. */
export function stripJustComments(src) {
  return src
    .split('\n')
    .map((line) => (/^\s*#/.test(line) ? '' : line))
    .join('\n');
}

// --- discovery: the ground truth the registry must agree with ---------------

/**
 * Every `.mjs` under `scanRoots` whose COMMENT-STRIPPED source contains
 * `predicate`. That is the repo's definition of a gate: a script that can say
 * "I could not check" as its own exit code.
 */
export function discoverGateScripts(root, scanRoots, predicate) {
  const found = [];
  for (const dir of scanRoots) {
    const full = path.join(root, dir);
    let entries;
    try {
      entries = fs.readdirSync(full, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      if (!e.isFile()) continue;
      if (!e.name.endsWith('.mjs')) continue;
      if (e.name.endsWith('.test.mjs')) continue;
      const src = stripJsComments(
        fs.readFileSync(path.join(full, e.name), 'utf8').replace(/\r\n/g, '\n'),
      );
      if (src.includes(predicate)) found.push(`${dir}/${e.name}`);
    }
  }
  return found.sort();
}

// --- justfile recipes -------------------------------------------------------

/** Map of recipe name -> {deps, body} for every recipe in a justfile. */
export function parseJustRecipes(src) {
  const lines = stripJustComments(src.replace(/\r\n/g, '\n')).split('\n');
  const out = new Map();
  let cur = null;
  for (const line of lines) {
    const header = /^([a-z][a-z0-9-]*)(\s+[^:]*)?:(.*)$/.exec(line);
    if (header !== null) {
      cur = { deps: header[3].trim().split(/\s+/).filter(Boolean), body: [] };
      out.set(header[1], cur);
      continue;
    }
    if (cur !== null && /^\s+\S/.test(line)) {
      cur.body.push(line.trim());
      continue;
    }
    if (line.trim() === '') continue;
    cur = null;
  }
  return out;
}

/** The `node --test <file>` targets a recipe body runs, repo-relative. */
export function nodeTestTargets(body) {
  const out = [];
  for (const line of body) {
    const m = /node\s+--test\s+(\S+)/.exec(line);
    if (m !== null) out.push(m[1]);
  }
  return out;
}

const JUSTFILE = read('justfile');
const RECIPES = parseJustRecipes(JUSTFILE);
const CI_YML = read(path.join('.github', 'workflows', 'ci.yml'));

/** Every `- name:` step in the CI workflow. */
export function ciStepNames(src) {
  return src
    .split('\n')
    .map((l) => /^\s*-\s+name:\s*(.+?)\s*$/.exec(l))
    .filter((m) => m !== null)
    .map((m) => m[1].replace(/^["']|["']$/g, ''));
}

const CI_STEPS = ciStepNames(CI_YML);

// --- 1. discovery, both directions ------------------------------------------

test('every_discovered_gate_script_is_registered_and_every_registered_one_exists', () => {
  const discovered = discoverGateScripts(
    REPO_ROOT,
    registry.scanRoots,
    registry.gatePredicate,
  );
  const declared = GATES.map((g) => posix(g.script)).sort();
  assert.deepEqual(
    discovered,
    declared,
    'scripts/ci/gates.json is the ONE enumeration of this repo\'s gates. ' +
      `discovered: ${discovered.join(', ')}\ndeclared:   ${declared.join(', ')}\n` +
      'A script exporting EXIT_CANNOT_CHECK with no registry entry is a gate ' +
      'nobody proves; a registry entry with no script is a ghost rung.',
  );
});

test('the_scanner_is_alive_and_finds_the_five_gates_this_tree_actually_has', () => {
  // A discovery pass that has stopped matching agrees with an empty registry
  // just as happily as with a correct one. Pin the population.
  const discovered = discoverGateScripts(
    REPO_ROOT,
    registry.scanRoots,
    registry.gatePredicate,
  );
  assert.ok(
    discovered.length >= 5,
    `scanner found ${discovered.length} gates in a tree that has at least five`,
  );
});

test('discovery_ignores_the_predicate_when_it_appears_only_in_a_comment', () => {
  // THE CANARY. Seed both halves and watch the scanner separate them.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'gate-inventory-'));
  const dir = path.join(root, 'scripts', 'ci');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, 'talks-about-it.mjs'),
    '// This script does not export const EXIT_CANNOT_CHECK, it only mentions it.\n' +
      '/* export const EXIT_CANNOT_CHECK = 3; */\nexport const NOT_A_GATE = 1;\n',
  );
  fs.writeFileSync(
    path.join(dir, 'really-a-gate.mjs'),
    '// a real one\nexport const EXIT_CANNOT_CHECK = 3;\n',
  );
  const found = discoverGateScripts(root, ['scripts/ci'], registry.gatePredicate);
  assert.deepEqual(found, ['scripts/ci/really-a-gate.mjs']);
  fs.rmSync(root, { recursive: true, force: true });
});

test('parseJustRecipes_still_matches_on_a_CRLF_checkout', () => {
  // This cost a cycle: `.` in a JS regex does not match `\r`, so `^name:(.*)$`
  // found ZERO recipes in this (CRLF) checkout and every binding assertion
  // below reported "justfile has no recipe" instead of the drift it was for.
  const r = parseJustRecipes('ci: alpha beta\r\nharness-test:\r\n    node --test a.mjs\r\n');
  assert.deepEqual(r.get('ci').deps, ['alpha', 'beta']);
  assert.deepEqual(nodeTestTargets(r.get('harness-test').body), ['a.mjs']);
});

test('stripJsComments_keeps_string_literals_whole', () => {
  assert.equal(stripJsComments('const a = "http://x"; // gone'), 'const a = "http://x"; ');
  assert.equal(stripJsComments('a /* mid */ b'), 'a  b');
});

// --- 2. every gate proves every outcome it declares -------------------------

for (const gate of GATES) {
  test(`${gate.key}_script_and_paired_test_both_exist`, () => {
    assert.ok(exists(gate.script), `${gate.script} is registered but missing`);
    assert.ok(exists(gate.test), `${gate.key}: paired test ${gate.test} is missing`);
  });

  test(`${gate.key}_declares_the_cannot_check_constant_in_code_not_in_a_comment`, () => {
    // Exported, not merely described: this constant IS the gate predicate that
    // discovery above scans for, so a gate that only talks about it in a header
    // comment would fall out of the registry's reach entirely. Exit 2 is NOT
    // asserted as a constant — check-doc-sync spells it inline — because the
    // binding proof of an exit code is the spawned assertion below, not a name.
    const src = stripJsComments(read(gate.script));
    assert.match(src, /export const EXIT_CANNOT_CHECK\s*=\s*3/, `${gate.script}`);
  });

  test(`${gate.key}_proves_each_declared_exit_code_against_a_spawned_process`, () => {
    const src = stripJsComments(read(gate.test));
    assert.match(
      src,
      /\b(spawnSync|execFileSync)\s*\(/,
      `${gate.test} has no process spawn: its 0/2/3 contract is prose with an ` +
        '`if` underneath. Call the gate as a PROCESS and assert its status.',
    );
    for (const code of gate.exits) {
      assert.match(
        src,
        new RegExp(`\\.status,\\s*${code}\\b`),
        `${gate.test} never asserts exit ${code} on a spawned process. ` +
          `${gate.key} claims 0/2/3; ${code} is unproven — and exit 3 ` +
          'unproven is a cannot-check branch that may be reporting success.',
      );
    }
  });
}

// --- 3. enforcement binding -------------------------------------------------

for (const gate of GATES) {
  test(`${gate.key}_verdict_runs_locally_and_on_the_binding_rung`, () => {
    if (gate.recipe === null || gate.ciStep === null) {
      assert.equal(
        typeof gate.verdictWaived,
        'string',
        `${gate.key}: recipe or ciStep is null, so verdictWaived must say why`,
      );
      assert.ok(gate.verdictWaived.length > 20, `${gate.key}: verdictWaived is not a reason`);
      return;
    }
    const recipe = RECIPES.get(gate.recipe);
    assert.ok(recipe, `justfile has no \`${gate.recipe}\` recipe for ${gate.key}`);
    assert.ok(
      recipe.body.some((l) => l.includes(posix(gate.script))),
      `justfile recipe \`${gate.recipe}\` does not run ${gate.script}`,
    );
    assert.ok(
      CI_STEPS.includes(gate.ciStep),
      `.github/workflows/ci.yml has no step named "${gate.ciStep}". ` +
        `${gate.key} runs locally and not on the merge pipeline, which makes it ` +
        'a courtesy and not a gate.',
    );
  });

  test(`${gate.key}_fixture_suite_runs_locally_and_on_the_binding_rung`, () => {
    assert.ok(gate.fixtureRecipes.length > 0, `${gate.key}: no fixture recipe`);
    for (const name of gate.fixtureRecipes) {
      const recipe = RECIPES.get(name);
      assert.ok(recipe, `justfile has no \`${name}\` recipe for ${gate.key}`);
      assert.ok(
        nodeTestTargets(recipe.body).includes(posix(gate.test)),
        `justfile recipe \`${name}\` does not run ${gate.test}`,
      );
    }
    assert.ok(
      CI_STEPS.includes(gate.ciFixtureStep),
      `ci.yml has no step named "${gate.ciFixtureStep}"`,
    );
    assert.ok(
      CI_YML.includes(`node --test ${posix(gate.test)}`),
      `ci.yml never runs ${gate.test}: this gate's fixture suite — the only ` +
        'proof it can still go red — is absent from the merge pipeline.',
    );
  });

  test(`${gate.key}_blocksCi_matches_the_ci_recipe_dependency_list`, () => {
    const ci = RECIPES.get('ci');
    assert.ok(ci, 'justfile has no `ci` recipe');
    if (gate.recipe === null) return;
    // The VERDICT recipe, not the fixture one: `lane-certify`'s fixtures run on
    // every change while its lanes are a nightly certification, and collapsing
    // those two into one answer is how the local ladder drifts from CI.
    const blocks = ci.deps.includes(gate.recipe);
    assert.equal(
      blocks,
      gate.blocksCi,
      `${gate.key}: blocksCi=${gate.blocksCi} but \`just ci\` ` +
        `${blocks ? 'does' : 'does not'} depend on \`${gate.recipe}\`. ` +
        'The local ladder and the merge pipeline must be one policy.',
    );
  });
}

// --- 4. the recipe bodies are a projection of the registry ------------------

test('no_fixture_recipe_runs_a_test_the_registry_does_not_know', () => {
  // The EXPECTED-diff direction that makes case 8 bite: delete one `node --test`
  // line from `harness-test` and the per-gate assertion above reddens; ADD one
  // that belongs to no gate and this reddens.
  const allowed = new Set([
    ...GATES.map((g) => posix(g.test)),
    posix(registry.inventoryTest),
    ...registry.extraFixtureTests.map(posix),
  ]);
  const recipes = new Set(GATES.flatMap((g) => g.fixtureRecipes));
  const stray = [];
  for (const name of recipes) {
    for (const target of nodeTestTargets(RECIPES.get(name)?.body ?? [])) {
      if (!allowed.has(target)) stray.push(`${name}: ${target}`);
    }
  }
  assert.deepEqual(
    stray,
    [],
    'a fixture recipe runs a suite with no gates.json entry — either register ' +
      'the gate or add it to extraFixtureTests with a reason',
  );
});

test('the_inventory_test_itself_runs_in_harness_test_and_in_ci', () => {
  // A registry whose own guard nobody runs is a sentence in a doc again.
  const harness = RECIPES.get('harness-test');
  assert.ok(harness, 'justfile has no `harness-test` recipe');
  assert.ok(
    nodeTestTargets(harness.body).includes(posix(registry.inventoryTest)),
    `\`harness-test\` does not run ${registry.inventoryTest}`,
  );
  assert.ok(
    CI_YML.includes(`node --test ${posix(registry.inventoryTest)}`),
    `ci.yml does not run ${registry.inventoryTest}`,
  );
});

test('registry_keys_are_unique_and_field_order_is_stable', () => {
  const keys = GATES.map((g) => g.key);
  assert.equal(new Set(keys).size, keys.length, 'duplicate gate key');
  const expected = [
    'key',
    'script',
    'test',
    'recipe',
    'fixtureRecipes',
    'ciStep',
    'ciFixtureStep',
    'blocksCi',
    'exits',
    'verdictWaived',
  ];
  for (const gate of GATES) {
    const got = Object.keys(gate);
    assert.deepEqual(
      got,
      expected.filter((k) => got.includes(k)),
      `${gate.key}: field order drifted — a later card must be able to append ` +
        'an entry without reflowing the file',
    );
  }
});
