#!/usr/bin/env node
// `just lane-certify` — judges the long lanes against criteria that were
// declared BEFORE the run, and keeps each lane's health record.
//
// Long lanes are certifications, not gates: they run on their own clock, judge
// statistically, and their unit of value is the trend across runs rather than
// the verdict of one (registry: test-harness/long-lane-certification). This repo
// had the harnesses and none of that — crates/core/tests/datasets_bulk_perf.rs
// measured write-lock hold time as another app experiences it, which is a
// genuinely good measurement, and then printed it: no percentile, no ceiling, no
// schedule, no artifact, no trend. A harness that asserts nothing can only ever
// report that it ran.
//
// THE SPLIT
//
//   measurement -> the Rust harness, which emits .lanes/runs/<lane>.json
//   judgement   -> here, against .lanes/criteria.json
//
// Judging outside the harness is what makes "declared before, judged after"
// enforceable: a bound cannot be quietly relaxed in the same commit that broke
// it, and any run's verdict is reproducible by anyone holding the artifact and
// the criteria. Every emitted verdict carries both, for exactly that reason.
//
// FOUR VERDICTS, BECAUSE THREE ARE NOT ENOUGH
//
//   pass        every criterion met, on this runner, for this workload
//   fail        a declared bound was breached
//   cannot-see  the lane should have run here and produced no artifact (or the
//               artifact is missing a metric a criterion names). NOT a pass.
//   cannot-run  the lane is declared unavailable on this runner (no Chrome, no
//               live network). Recorded as its own category, never counted as a
//               pass, never silently omitted.
//
// Exit: 2 if any lane failed, else 3 if any lane could not be seen or the
// instrument is broken, else 0.

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

import { defaultRepoRoot } from './flake-id.mjs';

export const EXIT_FINDINGS = 2;
export const EXIT_CANNOT_CHECK = 3;

export const VERDICTS = ['pass', 'fail', 'cannot-see', 'cannot-run'];

// --- statistics -------------------------------------------------------------

/**
 * Nearest-rank percentile.
 *
 * Percentiles, never averages: an average hides exactly the tail the lane exists
 * to see. Nearest-rank rather than an interpolating definition because the
 * bound must be a value the run actually observed — an interpolated p95 is a
 * number no request experienced, which is a poor thing to escalate on.
 */
export function percentile(values, p) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length));
  return sorted[Math.min(rank, sorted.length) - 1];
}

/**
 * Least-squares slope over the second half of a series, in metric-units per
 * sample.
 *
 * The second half, specifically: a ceiling met at the finish line is compatible
 * with linear growth that clears it an hour later, and the first half of any run
 * is warm-up. The slope over the tail is what distinguishes warm-up from a leak.
 */
export function secondHalfSlope(values) {
  if (!Array.isArray(values) || values.length < 4) return null;
  const tail = values.slice(Math.floor(values.length / 2));
  const n = tail.length;
  const meanX = (n - 1) / 2;
  const meanY = tail.reduce((a, b) => a + b, 0) / n;
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i++) {
    num += (i - meanX) * (tail[i] - meanY);
    den += (i - meanX) ** 2;
  }
  return den === 0 ? null : num / den;
}

// --- judging one criterion --------------------------------------------------

/** { verdict, measured, detail } for one criterion against one artifact. */
export function judge(criterion, artifact) {
  const series = artifact.series || {};
  const scalars = artifact.scalars || {};
  const missing = (what) => ({
    verdict: 'cannot-see',
    measured: null,
    detail: `the artifact carries no ${what} — the harness did not measure it, so nothing was certified`,
  });

  switch (criterion.kind) {
    case 'percentile': {
      const values = series[criterion.series];
      if (!Array.isArray(values) || values.length === 0) return missing(`series \`${criterion.series}\``);
      const measured = percentile(values, criterion.percentile);
      return bound(criterion, measured, `p${criterion.percentile} of ${values.length} samples`);
    }
    case 'slope': {
      const values = series[criterion.series];
      if (!Array.isArray(values) || values.length === 0) return missing(`series \`${criterion.series}\``);
      const measured = secondHalfSlope(values);
      if (measured === null) {
        return {
          verdict: 'cannot-see',
          measured: null,
          detail: `series \`${criterion.series}\` has ${values.length} samples — too few for a slope`,
        };
      }
      return bound(criterion, measured, `slope over the second half of ${values.length} samples`);
    }
    case 'scalar': {
      const measured = scalars[criterion.scalar];
      if (typeof measured !== 'number') return missing(`scalar \`${criterion.scalar}\``);
      return bound(criterion, measured, `scalar \`${criterion.scalar}\``);
    }
    case 'ratio': {
      const num = scalars[criterion.numerator];
      const den = scalars[criterion.denominator];
      if (typeof num !== 'number') return missing(`scalar \`${criterion.numerator}\``);
      if (typeof den !== 'number') return missing(`scalar \`${criterion.denominator}\``);
      if (den === 0) {
        return { verdict: 'cannot-see', measured: null, detail: `\`${criterion.denominator}\` is zero — the ratio is undefined` };
      }
      return bound(criterion, num / den, `${criterion.numerator} / ${criterion.denominator}`);
    }
    default:
      return { verdict: 'cannot-see', measured: null, detail: `unknown criterion kind ${JSON.stringify(criterion.kind)}` };
  }
}

function bound(criterion, measured, how) {
  const max = criterion.max ?? criterion.maxPerSample;
  const { min } = criterion;
  const parts = [`${how} = ${round(measured)}`];
  let ok = true;
  if (typeof max === 'number') {
    parts.push(`bound <= ${max}`);
    if (measured > max) ok = false;
  }
  if (typeof min === 'number') {
    parts.push(`bound >= ${min}`);
    if (measured < min) ok = false;
  }
  if (typeof max !== 'number' && typeof min !== 'number') {
    return { verdict: 'cannot-see', measured, detail: `criterion declares no bound — ${parts[0]}` };
  }
  return { verdict: ok ? 'pass' : 'fail', measured, detail: parts.join(', ') };
}

function round(n) {
  if (typeof n !== 'number' || !Number.isFinite(n)) return String(n);
  const abs = Math.abs(n);
  return abs >= 100 ? n.toFixed(1) : abs >= 1 ? n.toFixed(3) : n.toPrecision(3);
}

// --- artifacts --------------------------------------------------------------

export function runsDir(repoRoot = defaultRepoRoot()) {
  return path.join(repoRoot, '.lanes/runs');
}

/**
 * The artifact for one lane, merged across its parts.
 *
 * A lane can be measured by several `#[test]`s in one binary (the shared-DOM
 * comparison is two), and two tests racing on one file is not a merge strategy —
 * so each writes `<lane>--<part>.json` and they are combined here. A part that
 * did not run simply leaves its metrics absent, which surfaces as cannot-see on
 * the criteria that name them rather than as a pass on the ones that survived.
 */
export function loadArtifact(lane, dir) {
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return null;
  }
  const files = names.filter((f) => f === `${lane}.json` || f.startsWith(`${lane}--`));
  if (files.length === 0) return null;
  const merged = { lane, parts: [], series: {}, scalars: {}, workload: null, host: null, emittedAtUnix: 0, kind: null };
  for (const f of files.sort()) {
    let a;
    try {
      a = JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8'));
    } catch {
      continue;
    }
    merged.parts.push(a.part || f);
    merged.kind = merged.kind || a.kind || null;
    Object.assign(merged.series, a.series || {});
    Object.assign(merged.scalars, a.scalars || {});
    merged.workload = merged.workload || a.workload || null;
    merged.host = merged.host || a.host || null;
    merged.emittedAtUnix = Math.max(merged.emittedAtUnix, a.emittedAtUnix || 0);
    if (typeof a.exit === 'number') merged.scalars.exit = a.exit;
  }
  return merged.parts.length > 0 ? merged : null;
}

// --- certifying every lane --------------------------------------------------

export function certify(criteria, dir, platform) {
  const results = [];
  for (const [lane, spec] of Object.entries(criteria.lanes || {})) {
    const runsOn = spec.runsOn || [];
    if (!runsOn.includes(platform)) {
      results.push({
        lane,
        verdict: 'cannot-run',
        detail: spec.unavailableReason || `not declared to run on ${platform}`,
        criteria: [],
      });
      continue;
    }
    const artifact = loadArtifact(lane, dir);
    if (!artifact) {
      results.push({
        lane,
        verdict: 'cannot-see',
        detail:
          `declared to run on ${platform} and emitted NO artifact under ${path.basename(dir)}/. ` +
          `This is not a pass — the lane produced no evidence that it ran at all.`,
        criteria: [],
      });
      continue;
    }
    const judged = (spec.criteria || []).map((c) => ({ ...c, ...judge(c, artifact) }));
    if (judged.length === 0) {
      results.push({
        lane,
        verdict: 'cannot-see',
        detail: 'the lane declares no criteria — an unjudged measurement certifies nothing',
        criteria: [],
        artifact,
      });
      continue;
    }
    const verdict = judged.some((j) => j.verdict === 'fail')
      ? 'fail'
      : judged.some((j) => j.verdict === 'cannot-see')
        ? 'cannot-see'
        : 'pass';
    results.push({
      lane,
      verdict,
      detail: `${judged.filter((j) => j.verdict === 'pass').length}/${judged.length} criteria met`,
      criteria: judged,
      artifact,
    });
  }
  return results;
}

// --- lane health: earned green, planted red, and NEVER green ----------------

export function healthPath(repoRoot = defaultRepoRoot()) {
  return path.join(repoRoot, '.lanes/health.json');
}

export function loadHealth(file) {
  try {
    const h = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (h && typeof h.lanes === 'object') return h;
  } catch {
    /* no ledger yet */
  }
  return { schema: 1, lanes: {} };
}

export function updateHealth(health, results, { at, sha }) {
  for (const r of results) {
    const entry = health.lanes[r.lane] || { firstGreen: null, runs: [] };
    entry.runs.push({ at, sha, verdict: r.verdict });
    // Keep the ledger bounded; the lane's dashboard is the sequence, and 200
    // runs is more than a nightly lane accumulates in half a year.
    if (entry.runs.length > 200) entry.runs = entry.runs.slice(-200);
    if (r.verdict === 'pass' && !entry.firstGreen) entry.firstGreen = at;
    health.lanes[r.lane] = entry;
  }
  return health;
}

/**
 * Each lane's pass-rate history, with "never green" called out as its own
 * category.
 *
 * A lane that has never passed is the deadlier pathology, because red is normal
 * there and every failure after the first is wallpaper: a lane at a 100%
 * historical failure rate is not flaky, it is an unbuilt lane wearing a gate's
 * clothes, and the finding it reports is about the harness rather than about the
 * product. So `first-green` is tracked as an explicit lane event, and "no runs
 * recorded" is a different sentence from "never green".
 */
export function healthReport(health) {
  const lines = [];
  for (const [lane, e] of Object.entries(health.lanes)) {
    const runs = e.runs || [];
    const attempts = runs.filter((r) => r.verdict === 'pass' || r.verdict === 'fail' || r.verdict === 'cannot-see');
    const greens = runs.filter((r) => r.verdict === 'pass').length;
    if (runs.length === 0) {
      lines.push(`  ${lane}: NO RUNS RECORDED — the ledger has never seen this lane. Not "never green"; unobserved.`);
    } else if (attempts.length === 0) {
      lines.push(`  ${lane}: never attempted here — ${runs.length} recorded run(s), all cannot-run on their runner.`);
    } else if (greens === 0) {
      lines.push(
        `  ${lane}: NEVER GREEN — 0 of ${attempts.length} attempted run(s) passed. A lane at a 100% ` +
          `historical failure rate is an unbuilt lane wearing a gate's clothes; the finding is about ` +
          `the harness, not the product.`
      );
    } else {
      lines.push(
        `  ${lane}: first green ${e.firstGreen}, ${greens}/${attempts.length} attempted run(s) passed ` +
          `(predicate: recorded runs in .lanes/health.json, newest ${runs[runs.length - 1].at}).`
      );
    }
  }
  return lines;
}

// --- the report -------------------------------------------------------------

function cannotCheck(reason) {
  process.stderr.write(
    `lane-certify: CANNOT CHECK — ${reason}.\n` +
      `This is not a pass: no lane was certified. Fix the instrument (.lanes/criteria.json)\n` +
      `and re-run \`just lane-certify\`.\n`
  );
  process.exit(EXIT_CANNOT_CHECK);
}

function main() {
  const repoRoot = defaultRepoRoot();
  const argv = process.argv.slice(2);
  const arg = (name, fallback) => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 && argv[i + 1] ? argv[i + 1] : fallback;
  };
  const reportOnly = argv.includes('--report');
  const platform = arg('platform', process.platform);
  const dir = arg('runs', runsDir(repoRoot));

  let criteria;
  try {
    criteria = JSON.parse(fs.readFileSync(path.join(repoRoot, '.lanes/criteria.json'), 'utf8'));
  } catch (e) {
    cannotCheck(`the criteria could not be read or parsed (${e.message})`);
  }
  if (!criteria.lanes || Object.keys(criteria.lanes).length === 0) {
    cannotCheck('the criteria file declares zero lanes, so nothing could be judged');
  }

  const health = loadHealth(healthPath(repoRoot));

  if (reportOnly) {
    process.stdout.write(`long-lane health\n${healthReport(health).join('\n') || '  (no lanes recorded yet)'}\n`);
    process.exit(0);
  }

  const results = certify(criteria, dir, platform);
  const at = new Date().toISOString();
  let sha = process.env.GITHUB_SHA || null;
  if (!sha) {
    try {
      sha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8' }).trim();
    } catch {
      sha = null;
    }
  }

  const out = [`long lanes certified on ${platform} at ${at}`, ''];
  for (const r of results) {
    out.push(`  ${r.verdict.toUpperCase().padEnd(11)} ${r.lane} — ${r.detail}`);
    for (const c of r.criteria) {
      out.push(`      ${c.verdict.padEnd(10)} ${c.id}: ${c.detail}`);
      if (c.predicate) out.push(`                 predicate: ${c.predicate}`);
    }
  }
  out.push('');
  out.push('lane health (pass-rate history; "never green" is its own category)');
  out.push(...healthReport(updateHealth(health, results, { at, sha })));
  process.stdout.write(`${out.join('\n')}\n`);

  writeJson(healthPath(repoRoot), health);
  // The artifact of THIS run: measurement + criteria + verdict together, so the
  // verdict is reproducible from the file alone. The lane's dashboard is the
  // sequence of these, which is what CI uploads.
  writeJson(path.join(repoRoot, '.lanes/verdicts', `${at.replace(/[:.]/g, '-')}-${platform}.json`), {
    at,
    sha,
    platform,
    results,
  });

  if (results.some((r) => r.verdict === 'fail')) process.exit(EXIT_FINDINGS);
  if (results.some((r) => r.verdict === 'cannot-see')) {
    process.stderr.write(
      `\nOne or more lanes produced no evidence. "Found nothing" and "cannot see" are different\n` +
        `results, and only one of them is a pass — this is the other one.\n`
    );
    process.exit(EXIT_CANNOT_CHECK);
  }
  process.exit(0);
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

const invokedDirectly =
  process.argv[1] && path.resolve(process.argv[1]) === path.resolve(fileURLToPath(import.meta.url));
if (invokedDirectly) main();
