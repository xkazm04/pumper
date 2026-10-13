//! One classifier for every way a `claude` run can end.
//!
//! **The anti-pattern this file exists for.** `research()` used to turn a
//! subprocess run into a `Result` at eleven separate sites, each choosing one
//! of five [`ClaudeFailure`] members by hand. Enumerating the run's actual
//! endings from the code gives twenty-two distinct shapes; four were mapped to
//! a member whose own doc comment contradicts the fact, and three were
//! discarded outright (`let _ = …`). Because
//! [`pumper_core::error::ClaudeSpend::ledger_event`] derives the `cost_events`
//! row from the member, every mislabel was a mislabelled **spend**:
//!
//! - a `child.wait()` io error was reported as `Timeout`, which writes
//!   `unmetered_timeout` — the ledger asserting that a *paid* call vanished,
//!   for a call that may never have been paid for at all;
//! - a missing pipe handle on an already-spawned child was reported as
//!   `Spawn`, whose `ledger_event()` is `None`, so a **running** child left no
//!   cost row whatsoever;
//! - a panicked stdout-drain task was reported as `Unparseable` ("stdout was
//!   not a parseable envelope") when stdout had never been read;
//! - a process killed by a signal was reported as `NonZeroExit` ("the process
//!   exited non-zero"), which it did not do;
//! - and the prompt's own delivery result was thrown away, so a CLI that read
//!   EOF instead of a question answered nothing and was billed for it.
//!
//! The fix is structural, not a fifth special case: the mapping from "what
//! happened" to "which class, and therefore which ledger row" lives in exactly
//! one total function, over plain data. `lib.rs` constructs no Claude error of
//! its own after `cmd.spawn()` — pinned by the source inventory test in
//! `crates/core/tests/llm_chokepoint.rs`.
//!
//! Registry: `model-call-outcome-integrity` /
//! *elimination-reasons-are-a-closed-vocabulary* (one enumeration, one owner),
//! *unattempted-is-not-failed* (a prompt that never arrived, a host task that
//! panicked and a failed wait syscall are facts about **pumper**, not about the
//! model), *spend-precedes-the-error* (the ledger row is derived from the
//! class, so the class has to be true); `subprocess-lifecycle` /
//! *termination-and-reaping* (a signalled process is not an exited one).

use pumper_core::error::ClaudeFailure;
use pumper_core::Error;

/// How one **post-spawn** `claude` run ended, as plain data.
///
/// Deliberately post-spawn only. A refusal raised *before* `cmd.spawn()` — an
/// unknown role, a hostile argv, an unwritable scratch dir — is not a way a run
/// ended, because no run began; those stay `ClaudeFailure::Spawn` at their own
/// site, where "nothing ran, so nothing was spent" is literally true and the
/// ledger's silence is honest. Everything below means a child exists and may
/// already be spending.
///
/// Each payload is the operator-facing message. The newtype variants are also
/// usable as `fn(String) -> RunOutcome`, which is how
/// [`crate::RunScope::abandon`] is handed its class instead of hardcoding one.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RunOutcome {
    /// The prompt never reached the child's stdin: the write failed, the EOF
    /// that terminates the prompt failed, or the task doing both did.
    PromptUndelivered(String),
    /// A pipe the host needs was missing or failed after a successful spawn:
    /// no handle on a live child, an io error mid-drain (which leaves a
    /// TRUNCATED buffer that must never be parsed as a whole envelope), or a
    /// drain task that did not finish.
    PipeFailure(String),
    /// `child.wait()` itself returned an io error. No deadline elapsed and
    /// nothing was killed, which is exactly what separates it from
    /// [`Self::TimedOut`].
    WaitFailed(String),
    /// The engine deadline elapsed and the process tree was killed mid-run.
    TimedOut(String),
    /// The child exited without being killed by us. `code` is `None` only when
    /// a signal ended it (unix); a signalled process did not "exit non-zero".
    Exited {
        code: Option<i32>,
        why: String,
        /// `total_cost_usd` from the envelope on stdout, if one parsed — a
        /// non-zero exit routinely still prints a complete, billed envelope.
        reported_cost: Option<f64>,
    },
    /// The child exited 0 and stdout did not parse as an envelope. Exit 0 means
    /// the model answered, so this is a run that was billed and whose bill we
    /// cannot read — not a free call.
    Unparseable(String),
    /// The child exited 0 and returned a well-formed envelope with
    /// `is_error: true`: the single most expensive shape, because the run
    /// happened in full.
    CliError {
        why: String,
        reported_cost: Option<f64>,
    },
}

/// The one mapping from a run ending to `(class, spend, message)`.
///
/// Total by construction: the `match` is exhaustive over [`RunOutcome`], so a
/// new ending cannot be added without deciding its class here, and the class
/// cannot be added to `ClaudeFailure` without deciding its ledger row there.
/// One truth table, readable in one screen.
pub(crate) fn classify(outcome: RunOutcome) -> (ClaudeFailure, Option<f64>, String) {
    match outcome {
        RunOutcome::PromptUndelivered(why) => (ClaudeFailure::PromptUndelivered, None, why),
        RunOutcome::PipeFailure(why) => (ClaudeFailure::PipeFailure, None, why),
        RunOutcome::WaitFailed(why) => (ClaudeFailure::WaitFailed, None, why),
        RunOutcome::TimedOut(why) => (ClaudeFailure::Timeout, None, why),
        // `code: None` is the signal case and the ONLY thing that separates
        // these two: `ExitStatus::success()` is false for both, which is why
        // the old `!status.success()` test could not tell them apart.
        RunOutcome::Exited {
            code: None,
            why,
            reported_cost,
        } => (ClaudeFailure::Interrupted, reported_cost, why),
        RunOutcome::Exited {
            code: Some(_),
            why,
            reported_cost,
        } => (ClaudeFailure::NonZeroExit, reported_cost, why),
        RunOutcome::Unparseable(why) => (ClaudeFailure::Unparseable, None, why),
        RunOutcome::CliError { why, reported_cost } => {
            (ClaudeFailure::CliError, reported_cost, why)
        }
    }
}

/// [`classify`] as the engine uses it: the **only** place a Claude error is
/// constructed for a run that actually started.
pub(crate) fn classify_error(outcome: RunOutcome) -> Error {
    let (class, cost_usd, message) = classify(outcome);
    Error::claude_spent(class, cost_usd, message)
}

#[cfg(test)]
mod tests {
    use super::{classify, classify_error, RunOutcome};
    use pumper_core::error::{ClaudeFailure, SpendCertainty};

    /// The class and the ledger row a run ending must produce, together —
    /// asserting the class alone would miss the half that costs money.
    fn row(
        outcome: RunOutcome,
    ) -> (
        ClaudeFailure,
        Option<f64>,
        Option<(f64, SpendCertainty, String)>,
    ) {
        let err = classify_error(outcome);
        let spend = err
            .claude_spend()
            .expect("a classified run ending is always a Claude failure");
        (spend.class, spend.cost_usd, spend.ledger_event())
    }

    /// THE anti-pattern, and the most expensive one: `child.wait()` failing is
    /// a fact about *our* syscall, not about the model — but it routed through
    /// `abandon`, which hardcoded `Timeout` for all three of its callers. The
    /// ledger then wrote `unmetered_timeout`, which **asserts that a paid call
    /// vanished**, for a call that may have spent nothing at all. Nothing was
    /// killed and no deadline elapsed, so the two endings are not the same
    /// fact and must not share a row.
    #[test]
    fn a_failed_wait_is_not_an_unmetered_timeout() {
        let (class, cost, event) = row(RunOutcome::WaitFailed("cli failed: os error 5".into()));
        assert_eq!(class, ClaudeFailure::WaitFailed);
        assert_eq!(cost, None);
        assert_eq!(
            event,
            Some((
                0.0,
                SpendCertainty::Unknown,
                "failed_spend_unreported (wait_failed)".to_string()
            )),
            "a failed wait() must not claim a paid call vanished — the typed certainty is \
             `unknown`, not `unmetered`"
        );

        let timeout = row(RunOutcome::TimedOut("timed out after 600s".into()));
        assert_eq!(timeout.0, ClaudeFailure::Timeout);
        assert_eq!(
            timeout.2,
            Some((
                0.0,
                SpendCertainty::Unmetered,
                "unmetered_timeout".to_string()
            )),
            "the genuine timeout keeps the row that says a paid call vanished"
        );
        assert_ne!(
            event, timeout.2,
            "two different endings sharing one ledger row is the whole defect"
        );
    }

    /// THE anti-pattern: `!status.success()` was the only test, so a process
    /// the OS killed read as `NonZeroExit` — "the process exited non-zero",
    /// which it did not do. `subprocess-lifecycle` / *termination-and-reaping*:
    /// a signalled process is a different operational fact from one that chose
    /// its exit code, and its stdout is far likelier to be truncated.
    #[test]
    fn a_signalled_process_is_not_a_clean_nonzero_exit() {
        let (class, cost, _) = row(RunOutcome::Exited {
            code: None,
            why: "exited with signal: 9".into(),
            reported_cost: None,
        });
        assert_eq!(class, ClaudeFailure::Interrupted);
        assert_eq!(cost, None);

        let (class, cost, _) = row(RunOutcome::Exited {
            code: Some(3),
            why: "exited with 3".into(),
            reported_cost: Some(0.31),
        });
        assert_eq!(
            class,
            ClaudeFailure::NonZeroExit,
            "a real non-zero exit keeps its class"
        );
        assert_eq!(
            cost,
            Some(0.31),
            "stdout can hold a complete, billed envelope and then exit non-zero"
        );
    }

    /// A signalled run can still have printed a complete envelope before it was
    /// killed, and that money is real.
    #[test]
    fn an_interrupted_run_still_carries_the_money_its_envelope_reported() {
        let (_, cost, event) = row(RunOutcome::Exited {
            code: None,
            why: "exited with signal: 15".into(),
            reported_cost: Some(0.22),
        });
        assert_eq!(cost, Some(0.22));
        assert_eq!(
            event,
            Some((
                0.22,
                SpendCertainty::Billed,
                "failed_spend (interrupted)".to_string()
            ))
        );
    }

    /// THE anti-pattern, twice. A drain task that panicked was reported as
    /// `Unparseable` — whose doc says "stdout was not a parseable envelope",
    /// when stdout was never read at all. And a missing pipe handle on an
    /// already-spawned child was reported as `Spawn`, documented as "could not
    /// be started at all… nothing was spent" and mapped to **no ledger row**:
    /// a live child, quite possibly spending, with nothing in `cost_events` to
    /// say it ever existed.
    #[test]
    fn a_host_pipe_fault_is_neither_unparseable_stdout_nor_a_dead_spawn() {
        for why in ["no stdout handle", "stdout reader failed: panicked"] {
            let (class, _, event) = row(RunOutcome::PipeFailure(why.into()));
            assert_eq!(class, ClaudeFailure::PipeFailure);
            assert_eq!(
                event,
                Some((
                    0.0,
                    SpendCertainty::Unknown,
                    "failed_spend_unreported (pipe_failure)".to_string()
                )),
                "a child that IS running must leave a row: {why}"
            );
            assert!(
                event.is_some(),
                "Spawn's silent ledger is only honest when no process exists"
            );
        }
    }

    /// Exit 0 means the model answered, so the call was billed — the amount is
    /// simply unreadable. The row has to say *unknown*, not *free*: a silent
    /// `$0` reads identically to a call that never happened, and the per-job
    /// budget clamp cannot tell them apart either.
    #[test]
    fn unreadable_stdout_after_exit_zero_is_unknown_spend_not_a_free_call() {
        let (class, cost, event) = row(RunOutcome::Unparseable(
            "unparseable cli output: expected value".into(),
        ));
        assert_eq!(class, ClaudeFailure::Unparseable);
        assert_eq!(cost, None, "nothing readable reported a cost");
        let (amount, certainty, detail) = event.expect("a run that answered leaves a row");
        assert_eq!(amount, 0.0);
        assert_eq!(
            certainty,
            SpendCertainty::Unmetered,
            "exit 0 means the model answered: the operator WAS billed, the amount is              merely unreadable"
        );
        assert!(
            detail.starts_with("failed_spend_unreported"),
            "an unknown spend must not read as a metered $0: {detail}"
        );
        assert!(detail.contains("unparseable"), "{detail}");
    }

    /// The adopted backlog item (optimize-2026-10-01) as a unit: the prompt not
    /// arriving is its own ending, and none of the five old members could say
    /// so — the CLI exited 0 with a valid envelope, so every signal available
    /// at the old call sites said *success*.
    #[test]
    fn an_undelivered_prompt_has_a_class_of_its_own() {
        let (class, cost, event) = row(RunOutcome::PromptUndelivered(
            "the prompt never reached the cli: Broken pipe (os error 32)".into(),
        ));
        assert_eq!(class, ClaudeFailure::PromptUndelivered);
        assert_eq!(cost, None);
        assert_eq!(
            event,
            Some((
                0.0,
                SpendCertainty::Unmetered,
                "failed_spend_unreported (prompt_undelivered)".to_string()
            ))
        );
    }

    /// The message is the operator's whole view of what happened, so the
    /// classifier must not eat it on any arm.
    #[test]
    fn every_ending_keeps_its_message() {
        let endings = [
            RunOutcome::PromptUndelivered("m1".into()),
            RunOutcome::PipeFailure("m2".into()),
            RunOutcome::WaitFailed("m3".into()),
            RunOutcome::TimedOut("m4".into()),
            RunOutcome::Exited {
                code: None,
                why: "m5".into(),
                reported_cost: None,
            },
            RunOutcome::Exited {
                code: Some(1),
                why: "m6".into(),
                reported_cost: None,
            },
            RunOutcome::Unparseable("m7".into()),
            RunOutcome::CliError {
                why: "m8".into(),
                reported_cost: Some(0.9),
            },
        ];
        let mut classes = Vec::new();
        for (i, ending) in endings.into_iter().enumerate() {
            let (class, _, message) = classify(ending);
            assert_eq!(message, format!("m{}", i + 1));
            classes.push(class);
        }
        // Every ending reaches a class, and the two `Exited` shapes reach
        // DIFFERENT ones — the mislabel this file was written for.
        assert_eq!(classes.len(), 8);
        assert_ne!(classes[4], classes[5]);
    }
}
