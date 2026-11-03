// Tests for the client generator's toolchain predicate.
// `node --test scripts/ci/generate-clients.test.mjs` (also `just harness-test`).

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  EXIT_CANNOT_CHECK,
  openapiTypescriptCli,
  toolchainPresent,
} from '../gen/toolchain.mjs';

const GEN = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../gen/generate-clients.mjs');

test('a_missing_toolchain_is_cannot_check_not_drift', () => {
  assert.equal(EXIT_CANNOT_CHECK, 3);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'gen-clients-'));
  assert.equal(toolchainPresent(root), false);
  fs.mkdirSync(path.dirname(openapiTypescriptCli(root)), { recursive: true });
  fs.writeFileSync(openapiTypescriptCli(root), '');
  assert.equal(toolchainPresent(root), true);
});

test('the_generator_exits_3_when_the_toolchain_is_absent', (t) => {
  const real = path.resolve(path.dirname(GEN), '../..');
  if (fs.existsSync(openapiTypescriptCli(real))) {
    t.skip('toolchain installed here; the predicate test covers the absent case');
    return;
  }
  const r = spawnSync(process.execPath, [GEN, '--check'], { encoding: 'utf8' });
  assert.equal(r.status, 3);
  assert.match(r.stderr, /CANNOT CHECK/);
});
