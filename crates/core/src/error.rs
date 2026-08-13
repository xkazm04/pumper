/// How a Claude-engine call failed, and **what it had already spent when it
/// did**.
///
/// The CLI reports `total_cost_usd` in the *same* JSON envelope it reports a
/// failure in, so an error that cannot carry cost throws away the spend of
/// exactly the runs that spent the most: a job that burns to its ceiling and
/// then errors used to record `$0` in `cost_events`, which made the budget
/// ceiling structurally unenforceable for the expensive runs it exists to stop.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClaudeSpend {
    /// USD the CLI reported as **already spent** before it failed. `None` means
    /// no envelope was ever produced (timeout, spawn failure), so the spend is
    /// genuinely unknown — which is not the same fact as `Some(0.0)`.
    pub cost_usd: Option<f64>,
    /// Which failure this was, for the ledger's `detail` column.
    pub class: ClaudeFailure,
}

impl ClaudeSpend {
    /// A failure that produced no envelope: the spend is unknown, not zero.
    pub const fn unreported(class: ClaudeFailure) -> Self {
        Self {
            cost_usd: None,
            class,
        }
    }

    /// A failure whose envelope reported `cost_usd` as already spent.
    pub const fn reported(class: ClaudeFailure, cost_usd: f64) -> Self {
        Self {
            cost_usd: Some(cost_usd),
            class,
        }
    }

    /// The cost event this failure must leave in the ledger, as
    /// `(cost_usd, detail)` — or `None` when staying silent is honest.
    ///
    /// Three cases, deliberately distinguished (the anti-pattern is one silent
    /// `$0` that reads identically to "no call was made"):
    ///
    /// - **Reported spend** → the real amount, `failed_spend (<class>)`. This is
    ///   the money the budget clamp has to see.
    /// - **Timeout** → `$0` with `unmetered_timeout`. A killed run cannot report
    ///   what it spent, and the ledger has to show *that a paid call vanished
    ///   unmetered* rather than show nothing at all.
    /// - **Nothing ran** (`Spawn`) → `None`. No process, no spend, and a row
    ///   would be noise claiming otherwise.
    pub fn ledger_event(&self) -> Option<(f64, String)> {
        match (self.cost_usd, self.class) {
            (Some(cost), class) => Some((cost, format!("failed_spend ({})", class.as_str()))),
            (None, ClaudeFailure::Timeout) => Some((0.0, "unmetered_timeout".to_string())),
            (None, ClaudeFailure::Spawn) => None,
            (None, class) => Some((0.0, format!("failed_spend_unreported ({})", class.as_str()))),
        }
    }
}

/// The failure classes a Claude-engine call can end in. Typed rather than
/// stringly so the ledger's `detail` values cannot drift between call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeFailure {
    /// The CLI could not be started at all (bad `binary`, not on PATH).
    /// Nothing ran, so nothing was spent.
    Spawn,
    /// The engine deadline elapsed and the process tree was killed mid-run. The
    /// run may have spent anything up to its ceiling and no envelope survives to
    /// say how much.
    Timeout,
    /// The process exited non-zero. Its stdout may still hold a valid envelope
    /// (with a cost), so this class is not automatically unreported.
    NonZeroExit,
    /// The CLI returned a well-formed envelope with `is_error: true` — the
    /// single most expensive failure shape, because the run happened.
    CliError,
    /// stdout was not a parseable envelope, so nothing can be read from it.
    Unparseable,
}

impl ClaudeFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            ClaudeFailure::Spawn => "spawn",
            ClaudeFailure::Timeout => "timeout",
            ClaudeFailure::NonZeroExit => "nonzero_exit",
            ClaudeFailure::CliError => "is_error",
            ClaudeFailure::Unparseable => "unparseable",
        }
    }
}

/// How a sandboxed WASM plugin call failed.
///
/// Typed rather than stringly for the same reason [`ClaudeFailure`] is: the
/// consumers of a plugin failure **classify** it — the observatory buckets
/// replays into ok/trap/empty/schema_invalid, the trigger ledger records a
/// distinct outcome per class — and they used to do it by matching substrings
/// of the host's own `format!` messages. Rewording one message silently
/// reclassified every row it produced, with no test anywhere failing.
///
/// The classes are drawn along the lines an OPERATOR can act on, not along the
/// wasmtime API's seams: "the sandbox stopped it" is one fact whether the stop
/// was a trap, fuel exhaustion or the memory cap (all three arrive as traps and
/// all three mean *this plugin is too expensive or broken*), while "the module
/// isn't there" and "the module is there but doesn't export the ABI" are
/// genuinely different deployment mistakes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginFailure {
    /// No module of that name is loaded — never installed, or the name is a
    /// typo. Usually means `just plugins-install` never ran.
    Unknown,
    /// The plugin subsystem is switched off (`[plugins] enabled = false`), so
    /// *no* name would resolve. Distinct from [`Unknown`](Self::Unknown)
    /// because the fix is a config change, not a build step — and because a
    /// disabled host reports every configured hook missing at once, which a
    /// caller may want to dedupe rather than amplify.
    Disabled,
    /// The module loaded but does not export the ABI the call needs (`memory`,
    /// `alloc`, or neither `extract_v2` nor `extract`). A describe-only
    /// dynamic-app module hits this: it is a legitimate module, just not an
    /// executable extraction/hook plugin.
    MissingExport,
    /// The sandbox stopped it mid-run: an explicit trap (`unreachable`, a guest
    /// panic), CPU fuel exhaustion, or the linear-memory cap. The limits held —
    /// this is the sandbox working, not the host failing.
    Trap,
    /// It ran to completion and returned, but what it returned is not the
    /// contract: a packed pointer/length outside its own memory, unreadable
    /// bytes, or output that is not UTF-8 JSON.
    MalformedOutput,
    /// The **host** failed around the call — store setup, the blocking task
    /// panicking, the admission gate closing. Not the plugin's fault, and the
    /// one class that means "file a bug against pumper".
    Host,
}

impl PluginFailure {
    /// Stable snake_case token for logs, ledger `detail` values and dataset
    /// rows. These strings ARE a contract (the trigger ledger's outcome
    /// vocabulary is built from them); the human message is not.
    pub fn as_str(self) -> &'static str {
        match self {
            PluginFailure::Unknown => "unknown_plugin",
            PluginFailure::Disabled => "plugins_disabled",
            PluginFailure::MissingExport => "missing_export",
            PluginFailure::Trap => "trap",
            PluginFailure::MalformedOutput => "malformed_output",
            PluginFailure::Host => "host_error",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http engine: {0}")]
    Http(String),
    #[error("browser engine: {0}")]
    Browser(String),
    /// A Claude-engine failure. The struct variant exists to carry
    /// [`ClaudeSpend`]: money the CLI reports on its way out is real money, and
    /// a `String`-only variant had nowhere to put it. Build it with
    /// [`Error::claude`] / [`Error::claude_spent`].
    #[error("claude engine: {message}")]
    Claude { message: String, spend: ClaudeSpend },
    #[cfg(feature = "storage")]
    #[error("storage: {0}")]
    Storage(#[from] sqlx::Error),
    /// A session-vault profile problem: an unsafe/unusable profile name, or a
    /// profile dir that can't be prepared. Typed so a bad `profile` on a request
    /// is distinguishable from a transport failure.
    #[error("profile: {0}")]
    Profile(String),
    /// A transact-flow rejection: the request asked for something the current
    /// slice deliberately refuses (live `submit: true` before the human-approval
    /// design exists) or the flow itself is malformed (empty idempotency key).
    /// Typed so "we refused to act" is never confused with "the browser broke".
    #[error("transact: {0}")]
    Transact(String),
    #[error("parse: {0}")]
    Parse(String),
    #[error("config: {0}")]
    Config(String),
    #[error("app: {0}")]
    App(String),
    /// A sandboxed WASM plugin call failed. The struct variant exists to carry
    /// [`PluginFailure`]: the classification is what every consumer actually
    /// wants, and a `String`-only variant forced them to re-derive it by
    /// matching substrings of the message. Build it with [`Error::plugin`].
    #[error("plugin '{plugin}' ({}): {message}", kind.as_str())]
    Plugin {
        /// The module the call named. Present even for
        /// [`PluginFailure::Unknown`] — that IS the actionable fact.
        plugin: String,
        kind: PluginFailure,
        message: String,
    },
    /// A metered seam refused to start further paid work because the job's
    /// `budget_usd` ceiling is already reached.
    ///
    /// Typed because this failure is **deterministic**, and the runtime has to
    /// be able to tell. Every other failure the worker sees (an engine hiccup,
    /// a 503, a locked database) may well succeed on the next attempt; this one
    /// cannot. A retry re-seeds the job's spend from the cost ledger — the money
    /// really was spent — and re-refuses on its first metered call, so the
    /// backoff ladder buys nothing and burns every remaining attempt on
    /// identical refusals. See [`Error::is_terminal_for_job`].
    #[error("budget: {0}")]
    BudgetExhausted(String),
    /// A VCR replay could not be served from the recorded cassette (missing
    /// cassette, unrecorded request, or a body truncated at record time).
    /// Typed so a replay MISS is distinguishable from an app failure — and so
    /// it can never be confused with (or silently downgraded to) a live fetch.
    #[error("vcr replay miss: {0}")]
    ReplayMiss(String),
    /// Client-supplied input the server understood but rejected (a malformed
    /// query, filter, or rule). Maps to HTTP 400 at the request boundary — unlike
    /// `Parse`, which also covers server-internal decode failures (HTTP 500).
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// A Claude-engine failure that spent nothing, or whose spend is unknown.
    pub fn claude(class: ClaudeFailure, message: impl Into<String>) -> Self {
        Error::Claude {
            message: message.into(),
            spend: ClaudeSpend::unreported(class),
        }
    }

    /// A Claude-engine failure carrying the USD the CLI reported as **already
    /// spent** — the whole point of the struct variant.
    pub fn claude_spent(
        class: ClaudeFailure,
        cost_usd: Option<f64>,
        message: impl Into<String>,
    ) -> Self {
        Error::Claude {
            message: message.into(),
            spend: ClaudeSpend { cost_usd, class },
        }
    }

    /// A sandboxed-plugin failure of a known class, naming the module.
    pub fn plugin(
        kind: PluginFailure,
        plugin: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Error::Plugin {
            plugin: plugin.into(),
            kind,
            message: message.into(),
        }
    }

    /// How a plugin call failed, if this failure came from one. Consumers
    /// classify on THIS rather than on the message — that is the whole point of
    /// the typed variant.
    pub fn plugin_failure(&self) -> Option<PluginFailure> {
        match self {
            Error::Plugin { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// What this failure reports about money already spent, if it is the kind of
    /// failure that can. Metered seams consult it *before* propagating, so the
    /// ledger sees the spend of a call that failed.
    pub fn claude_spend(&self) -> Option<ClaudeSpend> {
        match self {
            Error::Claude { spend, .. } => Some(*spend),
            _ => None,
        }
    }

    /// Whether this failure is **deterministic and terminal for the job**: a
    /// retry cannot change the outcome, so the runtime must fail the job once
    /// instead of running it down the retry/backoff ladder.
    ///
    /// The bar is deliberately high, and today exactly two variants clear it:
    ///
    /// - [`Error::BudgetExhausted`] — a fact about the job's own ledger, which
    ///   a retry re-reads and re-refuses on.
    /// - [`Error::Transact`] — a **pre-flight** flow refusal, from one of two
    ///   producers. `TransactRequest::validate`'s three refusals (`submit:
    ///   true`, a blank idempotency key, a profile the vault does not hold) are
    ///   pure functions of the request; `crate::engine::unsupported_transact`
    ///   (the `Browser::transact` default) is a pure function of which engine is
    ///   wired. Request and wiring are both immutable for the life of the job,
    ///   so every attempt reaches the identical refusal *before touching a
    ///   browser*. This one matters more than most: transact is the app that
    ///   ACTS on live pages, and a refusal is precisely the case where the
    ///   ladder must not keep trying.
    ///
    /// Everything else — engine errors, storage errors, parse failures, even a
    /// replay miss — is either transient or caught earlier, and classifying any
    /// of them here would silently take away the retries that make this runtime
    /// reliable. Note the boundary that keeps `Transact` honest: a failure
    /// *during* a flow is an `Error::Browser`, which stays retryable; only the
    /// deterministic pre-flight refusal is typed `Transact`.
    ///
    /// The anti-pattern this replaces: the worker treated every app error as
    /// transient, so a job that exhausted its budget three seconds into attempt
    /// 1 was re-queued with backoff, re-seeded the same spend from the ledger,
    /// and re-exhausted instantly — three attempts and ~30s of backoff spent
    /// producing the same refusal three times.
    pub fn is_terminal_for_job(&self) -> bool {
        matches!(self, Error::BudgetExhausted(_) | Error::Transact(_))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::{ClaudeFailure, ClaudeSpend, Error, PluginFailure};

    /// The anti-pattern this variant replaces: every sandbox failure was an
    /// `Error::App(format!(...))`, so the observatory and the trigger ledger
    /// classified rows by matching substrings of the host's prose. Rewording a
    /// message reclassified data, and no test anywhere failed.
    ///
    /// The class must therefore survive **any** message.
    #[test]
    fn a_plugin_failure_classifies_by_kind_not_by_message_wording() {
        for message in [
            "plugin trapped (fuel/memory/panic): all fuel consumed",
            "wholly reworded prose that mentions nothing recognisable",
            "",
        ] {
            let err = Error::plugin(PluginFailure::Trap, "delta-slim", message);
            assert_eq!(
                err.plugin_failure(),
                Some(PluginFailure::Trap),
                "the class is a field, not a substring of {message:?}"
            );
        }
    }

    /// Each class is a different operator action — a missing build step, a
    /// config flag, a plugin that never exported the ABI, an over-budget
    /// module, a contract violation, a host bug. Collapsing any two of them
    /// loses the action.
    #[test]
    fn plugin_failure_classes_are_distinct_and_named() {
        let all = [
            PluginFailure::Unknown,
            PluginFailure::Disabled,
            PluginFailure::MissingExport,
            PluginFailure::Trap,
            PluginFailure::MalformedOutput,
            PluginFailure::Host,
        ];
        let mut tokens: Vec<&str> = all.iter().map(|k| k.as_str()).collect();
        tokens.sort_unstable();
        let unique = {
            let mut t = tokens.clone();
            t.dedup();
            t
        };
        assert_eq!(tokens, unique, "two classes share a token: {tokens:?}");
        for kind in all {
            assert!(
                !kind.as_str().is_empty() && kind.as_str().is_ascii(),
                "{kind:?} needs a stable snake_case token"
            );
        }
    }

    /// The module name is the actionable half of the failure, so it travels in
    /// a field (and still reaches the human message).
    #[test]
    fn a_plugin_failure_names_the_module_it_came_from() {
        let err = Error::plugin(PluginFailure::Unknown, "trigger-gate", "not loaded");
        let Error::Plugin { plugin, kind, .. } = &err else {
            panic!("wrong variant");
        };
        assert_eq!(plugin, "trigger-gate");
        assert_eq!(*kind, PluginFailure::Unknown);
        let shown = err.to_string();
        assert!(shown.contains("trigger-gate"), "{shown}");
        assert!(shown.contains("unknown_plugin"), "{shown}");
    }

    /// The mirror risk: anything that is not a plugin call must not answer this
    /// question, or a caller's `match` on the class silently swallows unrelated
    /// failures into a plugin bucket.
    #[test]
    fn non_plugin_failures_report_no_plugin_class() {
        assert!(Error::App("plugin trapped, allegedly".into())
            .plugin_failure()
            .is_none());
        assert!(Error::Http("connection reset".into())
            .plugin_failure()
            .is_none());
    }

    /// A plugin trap is the sandbox WORKING. Retrying it may well succeed
    /// (a different document, a raised fuel budget), so it must not join the
    /// terminal set and take a job's retries away.
    #[test]
    fn a_plugin_failure_is_not_terminal_for_the_job() {
        for kind in [
            PluginFailure::Trap,
            PluginFailure::Unknown,
            PluginFailure::MalformedOutput,
        ] {
            assert!(!Error::plugin(kind, "p", "x").is_terminal_for_job());
        }
    }

    /// The anti-pattern this variant was reshaped for: a run that burns to its
    /// budget and *then* fails reported `$0`, because the cost lived in the same
    /// envelope the error discarded. The spend must survive the error.
    #[test]
    fn a_failed_run_still_reports_what_it_spent() {
        let err = Error::claude_spent(ClaudeFailure::CliError, Some(0.42), "cli reported error: x");
        let spend = err.claude_spend().expect("a claude error carries spend");
        assert_eq!(spend.cost_usd, Some(0.42));
        assert_eq!(
            spend.ledger_event(),
            Some((0.42, "failed_spend (is_error)".to_string())),
            "the ledger row must carry the real money AND name the failure class"
        );
        assert!(
            !err.to_string().contains("0.42"),
            "the cost travels in a field, not smuggled into the message: {err}"
        );
    }

    /// A killed run cannot report its spend — but the ledger must still show
    /// that a paid call vanished, or an operator reading `cost_events` cannot
    /// tell an expensive timeout from a call that never happened.
    #[test]
    fn a_timeout_leaves_an_unmetered_marker_not_silence() {
        let spend = ClaudeSpend::unreported(ClaudeFailure::Timeout);
        assert_eq!(
            spend.ledger_event(),
            Some((0.0, "unmetered_timeout".to_string()))
        );
    }

    /// The mirror risk: a row for a call that never ran is a different lie. A
    /// spawn failure means no process, so the ledger stays quiet.
    #[test]
    fn a_call_that_never_started_writes_no_cost_row() {
        assert_eq!(
            ClaudeSpend::unreported(ClaudeFailure::Spawn).ledger_event(),
            None
        );
    }

    /// A non-zero exit or unparseable output may still have burned money we
    /// cannot read. `$0` is recorded, but under a detail that says the number is
    /// unknown rather than pretending the call was free.
    #[test]
    fn unreadable_spend_is_labelled_not_silently_zero() {
        for class in [ClaudeFailure::NonZeroExit, ClaudeFailure::Unparseable] {
            let (cost, detail) = ClaudeSpend::unreported(class)
                .ledger_event()
                .expect("a run that happened is recorded");
            assert_eq!(cost, 0.0);
            assert!(
                detail.starts_with("failed_spend_unreported"),
                "an unknown spend must not read as a metered $0: {detail}"
            );
            assert!(detail.contains(class.as_str()), "class missing: {detail}");
        }
    }

    /// Only the Claude engine reports spend; asking anything else must not
    /// invent one.
    #[test]
    fn other_failures_report_no_spend() {
        assert!(Error::Http("connection reset".into())
            .claude_spend()
            .is_none());
        assert!(Error::App("nonsense".into()).claude_spend().is_none());
    }

    /// The anti-pattern: budget exhaustion re-queued with backoff, re-seeded
    /// from the ledger, and re-exhausted — every remaining attempt burned for
    /// zero work. It must be classified terminal.
    #[test]
    fn budget_exhaustion_is_terminal_not_retryable() {
        assert!(Error::BudgetExhausted("no headroom".into()).is_terminal_for_job());
    }

    /// The anti-pattern, for the app that ACTS on live pages: a transact
    /// refusal (`submit: true`, a blank idempotency key, a profile the vault
    /// does not hold) is a pure function of the request, which is immutable for
    /// the life of the job — so the backoff ladder re-derives the identical
    /// refusal on every attempt, buying nothing and burning them all. A refusal
    /// must fail ONCE.
    #[test]
    fn transact_refusal_not_retried() {
        assert!(Error::Transact("submit: true is not available".into()).is_terminal_for_job());
    }

    /// The boundary that keeps the classification above honest: a failure
    /// *during* a flow (Chrome died, the page never loaded) is an
    /// `Error::Browser` and stays retryable. Only the deterministic pre-flight
    /// refusal is typed `Transact`.
    #[test]
    fn a_flow_that_broke_is_not_a_refusal() {
        assert!(!Error::Browser("chrome died mid-flow".into()).is_terminal_for_job());
        assert!(!Error::Profile("cookie jar unreadable".into()).is_terminal_for_job());
    }

    /// The mirror risk, and the more dangerous one: over-classifying. A
    /// transient failure marked terminal loses the job its retries silently.
    #[test]
    fn transient_failures_stay_retryable_not_terminal() {
        for e in [
            Error::Http("connection reset".into()),
            Error::Browser("chrome died".into()),
            Error::claude(ClaudeFailure::NonZeroExit, "cli exited 1"),
            Error::App("the source returned nonsense".into()),
            Error::Parse("bad html".into()),
            Error::Config("missing key".into()),
            Error::ReplayMiss("no recorded response".into()),
            Error::BadRequest("bad filter".into()),
            Error::Io(std::io::Error::other("disk hiccup")),
        ] {
            assert!(
                !e.is_terminal_for_job(),
                "{e} must stay retryable — marking it terminal takes the job's retries away"
            );
        }
    }
}
