#!/usr/bin/env node
// The long lanes' canary — a lane whose only job is to be caught.
//
// It emits one artifact, `.lanes/runs/canary.json`, carrying a value that sits
// outside the bound `.lanes/criteria.json` declares for it. Enrolled in the same
// population as the real lanes, on the same clock, judged by the same judge, it
// is the standing answer to two questions no other lane in this repo can answer:
//
//   1. did the lanes get scheduled at all on this run (a missing canary artifact
//      means the step that runs the lanes did not run), and
//   2. does the judge still fire (a canary that PASSES means the certifier's
//      bound checking broke, or a bound was relaxed, and every other lane's
//      green on that run is unsupported).
//
// The certifier reads it inverted: its failure is `canary-alive`, and its pass
// or its absence is `canary-dead` and exits 3. See scripts/ci/lane-certify.mjs.
//
// Never make this lane pass to make a red run green. The value below is a
// constant, not a measurement, and the only correct edit to it is one that keeps
// it outside the declared bound.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { defaultRepoRoot } from './flake-id.mjs';

/** Outside the lane's declared `max: 0` by construction. */
const PLANTED = 1;

function main() {
  const repoRoot = defaultRepoRoot();
  const dir = path.join(repoRoot, '.lanes/runs');
  fs.mkdirSync(dir, { recursive: true });
  const artifact = {
    lane: 'canary',
    kind: 'canary',
    part: 'canary',
    workload: 'a constant planted outside its own bound; no measurement is taken',
    host: `${os.platform()}/${os.arch()} ${os.cpus().length} cpu`,
    emittedAtUnix: Math.floor(Date.now() / 1000),
    series: {},
    scalars: { planted: PLANTED },
  };
  const file = path.join(dir, 'canary.json');
  fs.writeFileSync(file, `${JSON.stringify(artifact, null, 2)}\n`);
  process.stdout.write(
    `lane-canary: planted ${PLANTED} in ${path.relative(repoRoot, file)} — the certifier must catch it\n`
  );
}

main();
