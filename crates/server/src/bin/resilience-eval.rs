//! `resilience-eval` — the measurement that has to exist before any repair may
//! ship (`docs/features/resilient-extraction.md` §12.1).
//!
//! Every threshold in the resilience design is, in the design's own words, "a
//! starting guess", and `IMPLEMENTATION-NOTES.md` records that **no recall or
//! false-positive-rate number in this implementation has ever been measured**.
//! This binary is that number. It applies the mutation taxonomy
//! ([`pumper_core::resilience::mutate`]) to a corpus, runs the real detector
//! over the result, and reports recall per class and the false-positive rate on
//! the negative controls.
//!
//! It reads no database and writes nothing. The corpus is generated
//! deterministically, so the run is reproducible on a fresh clone with no
//! `data/` directory — which is what lets it sit in CI as a standing regression
//! rather than as a thing somebody remembers to run.
//!
//! Usage:
//!   cargo run -p pumper-server --bin resilience-eval
//!   cargo run -p pumper-server --bin resilience-eval -- --json
//!   cargo run -p pumper-server --bin resilience-eval -- --cohorts 5,30,200
//!
//! Exit codes follow the repo's gate convention (`just flake-check`,
//! `just disk-check`):
//!   0 — every §12.1 target met
//!   2 — findings: a target was missed, printed with the number that missed it
//!   3 — could not check (bad arguments); a cannot-run is never a pass

use pumper_core::config::ResilienceConfig;
use pumper_core::resilience::mutate::{evaluate_corpus, EvalTargets};

/// Cohort sizes measured by default — the design's "realistic cohort sizes".
/// 5 is below the fleet's `min_cohort_docs` floor on purpose: a source that
/// thin is *unmonitored*, and the report should show that as zero recall rather
/// than let it hide inside an average.
const DEFAULT_COHORTS: [usize; 3] = [5, 30, 200];

/// Clean runs pooled into the baseline before the mutation is applied. Three is
/// the detector's own `MIN_BASELINE_RUNS`; four gives every distributional test
/// something to work with without making the harness slow.
const DEFAULT_BASELINE_RUNS: usize = 4;

fn main() -> std::process::ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let json = args.iter().any(|a| a == "--json");
    let cohorts = match parse_cohorts(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("resilience-eval: {e}");
            eprintln!("usage: resilience-eval [--json] [--cohorts 5,30,200]");
            // 3, not 2: "could not check" is not "found nothing".
            return std::process::ExitCode::from(3);
        }
    };

    let cfg = ResilienceConfig::default();
    let targets = EvalTargets::default();
    let report = evaluate_corpus(&cfg, &cohorts, DEFAULT_BASELINE_RUNS);
    let findings = report.findings(&targets);

    if json {
        let mut out = serde_json::to_value(&report).unwrap_or_default();
        if let Some(map) = out.as_object_mut() {
            map.insert("findings".into(), serde_json::json!(findings));
            map.insert(
                "targets".into(),
                serde_json::json!({
                    "hard_recall": targets.hard_recall,
                    "silent_recall": targets.silent_recall,
                    "false_positive_rate": targets.false_positive_rate,
                }),
            );
        }
        println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
    } else {
        println!(
            "resilience-eval — mutation harness over a synthetic corpus\n\
             baseline runs: {}  cohorts: {:?}  degrade_score: {}\n",
            report.baseline_runs, report.cohorts, cfg.degrade_score
        );
        println!(
            "{:<18} {:>7} {:>7} {:>7} {:>16} {:>9}",
            "class", "cohort", "fired", "score", "verdict", "expected"
        );
        for r in &report.results {
            println!(
                "{:<18} {:>7} {:>7} {:>7.3} {:>16} {:>9}",
                r.class.as_str(),
                r.cohort,
                r.fired,
                r.score,
                r.verdict,
                r.class.should_fire()
            );
        }
        if !report.unmonitored_cohorts.is_empty() {
            println!(
                "\ncohorts {:?} are below min_cohort_docs ({}): every run there is \
                 `below_cohort`.\nThose sources are UNMONITORED, not missed — they are \
                 excluded from recall and\nincluded in the false-positive rate.",
                report.unmonitored_cohorts, cfg.min_cohort_docs
            );
        }
        println!(
            "\nrecall measured at cohorts {:?}\n\
             hard-break recall        {:.3}  (target >= {:.2})\n\
             silent-corruption recall {:.3}  (target >= {:.2})\n\
             false-positive rate      {:.3}  (ceiling <= {:.2})",
            report.judged_cohorts,
            report.hard_recall,
            targets.hard_recall,
            report.silent_recall,
            targets.silent_recall,
            report.false_positive_rate,
            targets.false_positive_rate
        );
        if findings.is_empty() {
            println!("\nOK — every §12.1 target met.");
        } else {
            println!();
            for f in &findings {
                println!("FINDING: {f}");
            }
        }
    }

    if findings.is_empty() {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::from(2)
    }
}

/// `--cohorts 5,30,200` → `[5, 30, 200]`. An unparseable or empty list is an
/// error rather than a silent fallback to the default: a harness that quietly
/// measured something other than what was asked for is worse than one that
/// refuses.
fn parse_cohorts(args: &[String]) -> Result<Vec<usize>, String> {
    let Some(i) = args.iter().position(|a| a == "--cohorts") else {
        return Ok(DEFAULT_COHORTS.to_vec());
    };
    let raw = args
        .get(i + 1)
        .ok_or_else(|| "--cohorts needs a value, e.g. --cohorts 5,30".to_string())?;
    let parsed: Result<Vec<usize>, _> = raw
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("bad --cohorts value '{raw}': {e}"));
    let parsed = parsed?;
    if parsed.is_empty() || parsed.contains(&0) {
        return Err(format!("bad --cohorts value '{raw}': sizes must be >= 1"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::parse_cohorts;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_bad_cohort_list_refuses_instead_of_measuring_something_else() {
        assert!(parse_cohorts(&args(&["--cohorts", "5,oops"])).is_err());
        assert!(parse_cohorts(&args(&["--cohorts"])).is_err());
        assert!(parse_cohorts(&args(&["--cohorts", "0"])).is_err());
        assert_eq!(
            parse_cohorts(&args(&["--cohorts", "5, 30"])).unwrap(),
            vec![5, 30]
        );
        assert_eq!(parse_cohorts(&args(&["--json"])).unwrap(), vec![5, 30, 200]);
    }
}
