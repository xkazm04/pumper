use std::time::Duration;

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

/// The typed cause a failure was built from, when the raise site had one.
///
/// `None` is honest and common: plenty of failures are raised from a condition
/// rather than from another error. What is not honest is the shape this
/// replaces — `.map_err(|e| Error::Config(format!("{}: {e}", path.display())))`
/// — where at the moment the cause is most structured (a `toml::de::Error` with
/// a span, line and column; a `reqwest::Error` that can answer `is_timeout()`)
/// it is consumed into prose, and every consumer downstream has the same prose
/// and no way back.
///
/// Boxed because `Error` is returned from every fallible function in the
/// workspace and four variants must not grow the enum by an unbounded payload;
/// `Send + Sync` because errors cross `tokio::spawn` everywhere. `Error` does
/// not derive `Clone`, which is what makes a plain `Box` (rather than an `Arc`)
/// the right shape.
///
/// **A chain is not a classification.** [`Error::is_terminal_for_job`] and
/// [`Error::is_router_failure`] match on the *variant*, exhaustively, and must
/// never consult a source: a decision that depends on a cause whose depth and
/// content no signature constrains is the drift both predicates exist to end.
/// The chain is for the human reading a receipt and for a classifier that wants
/// a typed *sibling* fact (`reqwest::Error::is_timeout`), never for the two
/// axes above.
#[derive(Debug)]
pub struct CauseValue {
    /// The concrete type the cause was at the raise site —
    /// `"toml::de::Error"`, `"reqwest::Error"`. Captured there because a
    /// `Box<dyn Error>` cannot be asked its type afterwards, and because the
    /// *type* is the part an operator can group a week of failures by. The
    /// message is already in the sentence.
    kind: &'static str,
    error: Box<dyn std::error::Error + Send + Sync>,
}

/// The optional cause a failure carries.
pub type Cause = Option<CauseValue>;

impl CauseValue {
    /// Captures a cause **with its type name**, which is only knowable here:
    /// the generic parameter is the concrete type, and one line later it is a
    /// `dyn Error` that cannot be asked.
    fn of<E: std::error::Error + Send + Sync + 'static>(error: E) -> Self {
        Self {
            // The full path, not a trimmed one: `toml::de::Error` and
            // `serde_json::Error` are already short, and trimming to a tail
            // makes two crates' `de::Error` the same label — which is exactly
            // the collision the column exists to avoid.
            kind: std::any::type_name::<E>(),
            error: Box::new(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An HTTP-engine failure, and **the wait the server asked for on its way
    /// out**.
    ///
    /// The struct variant exists to carry `retry_after`, for the same reason
    /// [`Error::Claude`] is one: a fact the failure knows and no consumer can
    /// re-derive. `engine-http` reads `Retry-After` properly, honours both RFC
    /// 7231 forms, treats it as a floor rather than a replacement — and then
    /// gives up when the stated wait will not fit the fetch budget
    /// (`capped_retry_sleep`), at which point the number it learned used to go
    /// into prose. The job ladder then re-queued in 10 seconds and ran the whole
    /// fetch back into the rate limit the server had asked us to wait ten
    /// minutes for. Build it with [`Error::http`] / [`Error::http_after`].
    #[error("http engine: {message}")]
    Http {
        message: String,
        /// A server-stated delay this failure could not honour, if there was
        /// one. `None` means the server asked for nothing (not "asked for
        /// zero"), which is why the requeue policy reads it as an override
        /// rather than as a value.
        retry_after: Option<Duration>,
        /// The `reqwest::Error` this failure was built from. See [`Cause`].
        cause: Cause,
    },
    /// A browser-engine failure and its driver-level cause. See [`Cause`].
    #[error("browser engine: {message}")]
    Browser { message: String, cause: Cause },
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
    /// A decode failure and the decoder's own error. See [`Cause`]: a
    /// `serde_json::Error` knows the line and column it failed at, and one
    /// `format!` away that is prose.
    #[error("parse: {message}")]
    Parse { message: String, cause: Cause },
    /// A configuration failure and the parser's own error. See [`Cause`]: a
    /// `toml::de::Error` carries a span into the file the operator has to edit.
    #[error("config: {message}")]
    Config { message: String, cause: Cause },
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
    /// cassette, unrecorded request, a body truncated at record time, a
    /// corrupt/unknown-version cassette). Typed so a replay MISS is
    /// distinguishable from an app failure — and so it can never be confused
    /// with (or silently downgraded to) a live fetch.
    ///
    /// Its sibling is [`Error::BadRequest`], which is what
    /// [`crate::vcr::refuse_replay`] returns when the app *itself* cannot be
    /// replayed (its work never reaches the chokepoint). That is a fact about
    /// the app, not about the cassette, so it is not a miss.
    ///
    /// **Terminal for a job** ([`Error::is_terminal_for_job`]): the cassette is
    /// a file written by an already-finished job and the request is derived from
    /// params frozen at enqueue, so a retry re-reads the identical bytes and
    /// re-misses in the identical place.
    #[error("vcr replay miss: {0}")]
    ReplayMiss(String),
    /// Client-supplied input the server understood but rejected (a malformed
    /// query, filter, or rule; an unsafe session-profile name). Maps to HTTP 400
    /// at the request boundary — unlike `Parse`, which also covers
    /// server-internal decode failures (HTTP 500).
    ///
    /// **Terminal for a job** ([`Error::is_terminal_for_job`]): every producer is
    /// a pure function of input that is immutable for the life of the job, so a
    /// retry re-parses the identical text and re-refuses.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// An upstream source's response no longer has the shape the app parses: a
    /// renamed or moved field, a changed query grammar, a once-verified contract
    /// the app can no longer find. Raised only by a **pre-write refusal** — the
    /// app parsed nothing usable and is declining to write, so no dataset is
    /// touched.
    ///
    /// Typed because "the source changed its schema" and "the source was down"
    /// are different events with different remedies, and an operator reading a
    /// job row has to be able to tell them apart. Both used to arrive as
    /// [`Error::App`], whose text is app prose — so the only way to classify was
    /// to match substrings of a sentence anybody was free to reword, which is
    /// the same anti-pattern [`crate::error::PluginFailure`] exists to kill.
    ///
    /// **Terminal for a job** ([`Error::is_terminal_for_job`]): the refusal is a
    /// pure function of a response fetched from params frozen at enqueue, so
    /// attempt #2 re-issues the identical request, re-parses an identically
    /// shaped response, and re-refuses in the identical place. The grants fleet
    /// is the worked example — a permanent upstream rename burned three attempts
    /// plus backoff on every scheduled run, every day, indefinitely, and read in
    /// the job log exactly like the source being down.
    ///
    /// **The boundary that keeps it honest.** Only pre-write *listing* refusals
    /// are this variant. Warn-only drift signals stay warnings, and a per-item
    /// degradation that aborts a stage rather than the job stays an
    /// [`Error::App`] — a partial harvest is a fact about one attempt, not about
    /// the contract, and it really can come out differently next time.
    #[error("source drift: {0}")]
    SourceDrift(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// An HTTP-engine failure the server asked nothing about — today's shape,
    /// and what every site that has no stated wait to carry should build.
    pub fn http(message: impl Into<String>) -> Self {
        Error::Http {
            message: message.into(),
            retry_after: None,
            cause: None,
        }
    }

    /// An HTTP-engine failure that keeps the error it was built from — the
    /// `reqwest::Error` that knows whether it was a connect, a timeout or a
    /// decode, which the sentence can only describe.
    pub fn http_from(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::Http {
            message: message.into(),
            retry_after: None,
            cause: Some(CauseValue::of(cause)),
        }
    }

    /// A browser-engine failure with no typed cause to keep.
    pub fn browser(message: impl Into<String>) -> Self {
        Error::Browser {
            message: message.into(),
            cause: None,
        }
    }

    /// A browser-engine failure keeping the driver's own error.
    pub fn browser_from(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::Browser {
            message: message.into(),
            cause: Some(CauseValue::of(cause)),
        }
    }

    /// A decode failure raised from a condition rather than from a decoder.
    pub fn parse(message: impl Into<String>) -> Self {
        Error::Parse {
            message: message.into(),
            cause: None,
        }
    }

    /// A decode failure keeping the decoder's error — the span, line and column
    /// a `serde_json::Error` or a `scraper` failure carries.
    pub fn parse_from(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::Parse {
            message: message.into(),
            cause: Some(CauseValue::of(cause)),
        }
    }

    /// A configuration failure raised from a condition rather than a parser.
    pub fn config(message: impl Into<String>) -> Self {
        Error::Config {
            message: message.into(),
            cause: None,
        }
    }

    /// A configuration failure keeping the parser's error — the `toml::de::Error`
    /// span that names the line of the file the operator has to edit.
    pub fn config_from(
        message: impl Into<String>,
        cause: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::Config {
            message: message.into(),
            cause: Some(CauseValue::of(cause)),
        }
    }

    /// The typed cause this failure was built from, if it kept one.
    ///
    /// **The machine half of the chain.** A consumer that wants to act on the
    /// cause downcasts it — `e.cause().and_then(|c|
    /// c.downcast_ref::<reqwest::Error>()).is_some_and(|r| r.is_timeout())` —
    /// which is a classification the compiler checks, unlike the substring
    /// matching the same question used to require.
    ///
    /// Not exposed as [`std::error::Error::source`]: `thiserror`'s `#[source]`
    /// cannot take an `Option<Box<dyn Error>>`, and the alternative (a
    /// non-optional wrapper) would make every causeless failure report a source
    /// that renders as nothing — a chain that lies about having a link is worse
    /// than no chain. If `#[source]` learns optional fields, this becomes the
    /// derive and the method stays as the named accessor.
    pub fn cause(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        Some(&*self.cause_value()?.error)
    }

    /// The cause's own type, as it was at the raise site — `"toml::de::Error"`.
    ///
    /// The operator answer: the two failures anyone has to tell apart (*the
    /// origin refused* vs *our own client could not build a request*) become
    /// distinguishable in a query, the way `is_terminal_for_job` made *terminal*
    /// queryable. A label, never a classification: a decision that has to act on
    /// the cause downcasts it.
    pub fn cause_kind(&self) -> Option<&'static str> {
        Some(self.cause_value()?.kind)
    }

    fn cause_value(&self) -> Option<&CauseValue> {
        match self {
            Error::Http { cause, .. }
            | Error::Browser { cause, .. }
            | Error::Parse { cause, .. }
            | Error::Config { cause, .. } => cause.as_ref(),
            _ => None,
        }
    }

    /// The cause chain under this failure, rendered one level per segment —
    /// `None` when there is nothing under it.
    ///
    /// The **human** half, for the one place a person deliberately reads an
    /// error rather than a machine classifying it (`GET /jobs/{id}/receipt`),
    /// and the place where a lost span/line/column costs most.
    pub fn cause_chain(&self) -> Option<String> {
        let mut cause: &(dyn std::error::Error + 'static) = self.cause()?;
        let mut out = vec![cause.to_string()];
        while let Some(next) = cause.source() {
            out.push(next.to_string());
            cause = next;
        }
        Some(out.join(": "))
    }

    /// An HTTP-engine failure carrying **the delay the server stated and this
    /// fetch could not honour** — the whole point of the struct variant.
    ///
    /// One carrier, never two: this is minted where the wait is learned and
    /// discarded (`engine-http`'s budget-exhausted arm), so the job ladder can
    /// honour it without a second mechanism disagreeing about what the server
    /// said.
    pub fn http_after(message: impl Into<String>, retry_after: Duration) -> Self {
        Error::Http {
            message: message.into(),
            retry_after: Some(retry_after),
            cause: None,
        }
    }

    /// The wait the origin asked for, if this failure carries one. Read by
    /// [`crate::storage::requeue_after`], which lets it outrank the ladder.
    pub fn stated_retry_after(&self) -> Option<Duration> {
        match self {
            Error::Http { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

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
    /// The bar is deliberately high, and today exactly five variants clear it:
    ///
    /// - [`Error::BudgetExhausted`] — a fact about the job's own ledger, which
    ///   a retry re-reads and re-refuses on.
    /// - [`Error::Transact`] — a **pre-flight** flow refusal, from one of two
    ///   producers. `TransactRequest::validate`'s refusals (`submit: true`, a
    ///   blank idempotency key) are pure functions of the request, as is
    ///   `crate::engine::require_existing_profile` (a profile the vault does not
    ///   hold); `crate::engine::unsupported_transact` (the `Browser::transact`
    ///   default) is a pure function of which engine is wired. Request and
    ///   wiring are both immutable for the life of the job, so every attempt
    ///   reaches the identical refusal *before touching a browser*. This one
    ///   matters more than most: transact is the app that ACTS on live pages,
    ///   and a refusal is precisely the case where the ladder must not keep
    ///   trying.
    /// - [`Error::BadRequest`] — client-supplied input the server understood and
    ///   rejected: a malformed dataset filter/aggregate/derived spec, a
    ///   syntactically bad search query, an unsafe session-profile name
    ///   (`crate::engine::require_safe_profile_name`), or a `replay_of` against
    ///   an app that structurally cannot be replayed
    ///   (`crate::vcr::refuse_replay`). Every producer is a pure function of
    ///   input that is immutable for the life of the job, so the ladder can only
    ///   re-derive it and re-fail. A job's params are fixed at enqueue; nothing
    ///   about attempt 4 makes `"$.a:bogus:1"` parse, or makes an app that
    ///   drives a browser session route through the fetch chokepoint.
    /// - [`Error::ReplayMiss`] — a VCR replay that could not be served. Every
    ///   construction site was audited before this variant was widened, because
    ///   a variant with one transient producer must NOT be classified here (see
    ///   `Error::Profile` below):
    ///   `vcr::Cassette::resolve` (the request is absent from the map),
    ///   `vcr::truncated_miss` (the body was dropped at record time),
    ///   `vcr::to_fetch_outcome` (the entry names an engine no tier produces),
    ///   `vcr::Cassette::load` (no cassette, no readable entries, a forged
    ///   `req_hash`, an unknown format version). All but one are pure functions
    ///   of an already-loaded, immutable cassette and a request derived from
    ///   params frozen at enqueue. (The refusal to replay an app that drives
    ///   engines raw is NOT one of these sites: nothing about a cassette is in
    ///   question there, so `vcr::refuse_replay` is a `BadRequest` — see above.)
    ///   The one that touches IO is `load`'s file read — and
    ///   that site is **already** permanent by call site (the worker resolves
    ///   the cassette before the run and `fail_permanently`s on any load error),
    ///   so widening the variant cannot take retries away from it.
    /// - [`Error::SourceDrift`] — a pre-write refusal raised because an
    ///   upstream response no longer has the shape the app parses. Every
    ///   producer was audited before the variant was classified here, and all of
    ///   them are pure functions of one already-fetched response body: the
    ///   grants fleet's eight listing guards (`ca-grants`'s
    ///   `total > 0 && records.is_empty()`, `grants-gov`'s `empty_page_is_drift`
    ///   and `empty_listing_is_drift`, `cordis`'s four, `eu-sedia`'s one). The
    ///   request that produced the body is derived from params frozen at
    ///   enqueue, so attempt #2 asks the identical question, gets an identically
    ///   shaped answer, and re-refuses before writing anything. A rename does
    ///   not un-rename itself between attempt 1 and attempt 3.
    ///
    ///   The transient lookalike is deliberately NOT this variant: a source that
    ///   is *down* fails in the fetch chokepoint as an `Error::Http` and keeps
    ///   its retries. That separation is the whole point — the two used to be
    ///   indistinguishable `Error::App`s.
    ///
    /// Everything else — engine errors, storage errors, parse failures — is
    /// either transient or caught earlier, and classifying any of them here
    /// would silently take away the retries that make this runtime reliable.
    /// Note the boundary that keeps `Transact` honest: a failure *during* a flow
    /// is an `Error::Browser`, which stays retryable; only the deterministic
    /// pre-flight refusal is typed `Transact`.
    ///
    /// **Why `Error::Profile` is NOT here**, though an unsafe profile name is as
    /// deterministic as anything above: the variant has a genuinely transient
    /// producer. `engine-http`'s `ProfileJar::load` types an unreadable cookie
    /// jar as `Error::Profile` — a sharing violation while the flusher renames
    /// its temp file, a momentary EACCES — and those succeed on the next
    /// attempt. Classifying the whole variant terminal to catch the name check
    /// would take the retries away from the IO case, so the *name* refusal is
    /// retyped at its seams instead (`require_safe_profile_name`) and the
    /// variant stays retryable.
    ///
    /// The anti-pattern this replaces: the worker treated every app error as
    /// transient, so a job that exhausted its budget three seconds into attempt
    /// 1 was re-queued with backoff, re-seeded the same spend from the ledger,
    /// and re-exhausted instantly — three attempts and ~30s of backoff spent
    /// producing the same refusal three times.
    pub fn is_terminal_for_job(&self) -> bool {
        matches!(
            self,
            Error::BudgetExhausted(_)
                | Error::Transact(_)
                | Error::BadRequest(_)
                | Error::ReplayMiss(_)
                | Error::SourceDrift(_)
        )
    }

    /// Whether this failure originated in **pumper itself** — its config, its
    /// routing, its own refusals before an engine was reached — rather than in
    /// the upstream an engine was talking to. The question is *"would every
    /// engine produce this?"*.
    ///
    /// A **second, independent** axis from [`Error::is_terminal_for_job`], not a
    /// re-partition of it: terminality asks *is this worth trying again*, origin
    /// asks *is there anywhere else to try it*. `SourceDrift` is terminal and
    /// theirs; `Http` is transient and theirs; `Transact` is terminal and ours —
    /// the cells that prove the two are different questions.
    ///
    /// **One carrier, never two.** This predicate is the only mark. pumper is
    /// one process with one typed error enum, so the classification is a
    /// property of the variant and nothing has to survive a boundary; a second
    /// mechanism (a header, a sentinel in the message) is the drift that lets
    /// the two spellings disagree.
    ///
    /// Read by the fetch ladder ([`crate::fetcher::Fetcher::fetch`]), which
    /// stops on it: a router failure reproduces identically on every remaining
    /// tier because the tier was never the variable, so continuing spends one
    /// engine invocation per tier to re-derive the same sentence — and the
    /// browser tier's http un-skip overturns a correct routing decision on
    /// evidence that says nothing about the http tier.
    ///
    /// **Exhaustive on purpose, and the default is theirs.** A new variant stops
    /// this compiling until someone decides whose failure it is; a `_ =>` arm
    /// would let "ours" be reached by accident, and the two directions do not
    /// cost the same — a theirs-error misfiled as ours loses a document the
    /// ladder would have fetched, while an ours-error misfiled as theirs costs
    /// only what happens today.
    pub fn is_router_failure(&self) -> bool {
        match self {
            // Our own configuration, identical on every tier. `validate()`
            // catches 29 of these at boot; the ones that reach a running ladder
            // are the ones it cannot.
            Error::Config { .. } => true,
            // Pre-flight refusals — pumper declining to act before any engine is
            // touched. `Transact` is this exact fix already made in one
            // capability (`engine::Browser::transact`'s default); `ReplayMiss`
            // is our cassette, not an origin's answer.
            Error::Transact(_) | Error::ReplayMiss(_) => true,
            // Plugins split by kind, and only the load side is ours: an unknown
            // module, a disabled subsystem, a missing export are facts about our
            // own artifacts. A trap, malformed output or a host error happened
            // while the plugin ran over a body an engine fetched, so a different
            // body really can come out differently.
            Error::Plugin { kind, .. } => matches!(
                kind,
                PluginFailure::Unknown | PluginFailure::Disabled | PluginFailure::MissingExport
            ),
            // Theirs — engine transport, the upstream's answer, the store. Plus
            // the two that are neither and must not be folded into either side:
            // `BudgetExhausted` is our clamp, not a fact about any tier, and
            // `BadRequest` is the caller's input. Both are already terminal for
            // the job, which is the axis that actually stops them.
            Error::Http { .. }
            | Error::Browser { .. }
            | Error::Claude { .. }
            | Error::Profile(_)
            | Error::Parse { .. }
            | Error::App(_)
            | Error::SourceDrift(_)
            | Error::BudgetExhausted(_)
            | Error::BadRequest(_)
            | Error::Io(_)
            | Error::Json(_)
            | Error::Other(_) => false,
            #[cfg(feature = "storage")]
            Error::Storage(_) => false,
        }
    }

    /// Whether this failure is the store reporting **contention** rather than a
    /// defect: SQLite answering `SQLITE_BUSY` / `SQLITE_LOCKED`, or the
    /// connection pool timing out before a connection came free.
    ///
    /// The store instrumentation counts these under their own outcome, because
    /// they are the database-specific fact that separates engine work from
    /// contention: a p95 driven by lock waits indicts the pool sizing or a
    /// writer-hog, not the query plan, and the two have disjoint remedies.
    ///
    /// **Classified by code, never by message.** The driver renders SQLite's
    /// extended result code through `DatabaseError::code()`; the primary code
    /// is its low byte (`5` = busy, `6` = locked), so `SQLITE_BUSY_SNAPSHOT`
    /// (517) and `SQLITE_BUSY_TIMEOUT` (261) classify with plain busy without
    /// this having to enumerate them. The messages SQLite attaches — "database
    /// is locked", "database table is locked" — are famously ambiguous prose
    /// that the driver itself apologises for; matching on them is the anti-
    /// pattern [`PluginFailure`] exists to kill, one layer down.
    ///
    /// Pool timeouts join the set because they are contention too, and the
    /// instrument's `phase` label is what keeps them distinguishable: an
    /// acquire-phase busy is a pool-sizing finding, an execute-phase busy is a
    /// writer-hog finding.
    #[cfg(feature = "storage")]
    pub fn is_store_contention(&self) -> bool {
        match self {
            Error::Storage(sqlx::Error::PoolTimedOut) => true,
            Error::Storage(sqlx::Error::Database(db)) => {
                matches!(sqlite_primary_code(db.as_ref()), Some(5 | 6))
            }
            _ => false,
        }
    }

    /// Without the `storage` feature there is no database to be contended over.
    #[cfg(not(feature = "storage"))]
    pub fn is_store_contention(&self) -> bool {
        false
    }
}

/// SQLite's **primary** result code for a driver error, extracted from the
/// extended code the driver reports. `None` when the error carries no code at
/// all (a non-SQLite `DatabaseError`, or one that never reached the engine).
///
/// Extracted as a named function rather than inlined so the classification is
/// testable against a fabricated code without needing a genuinely locked
/// database — and so the "low byte is the primary code" rule appears once.
#[cfg(feature = "storage")]
fn sqlite_primary_code(db: &dyn sqlx::error::DatabaseError) -> Option<i32> {
    db.code()?.parse::<i32>().ok().map(|code| code & 0xff)
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
        assert!(Error::http("connection reset").plugin_failure().is_none());
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
        assert!(Error::http("connection reset").claude_spend().is_none());
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
        assert!(!Error::browser("chrome died mid-flow").is_terminal_for_job());
        assert!(!Error::Profile("cookie jar unreadable".into()).is_terminal_for_job());
    }

    /// The anti-pattern, for input the caller cannot fix by waiting: a bad
    /// dataset filter, a malformed aggregate spec, an unparseable search query
    /// or an unsafe profile name is a pure function of text that is frozen into
    /// the job row at enqueue. Every attempt re-parses the identical string and
    /// re-fails, so the ladder buys nothing and bills four times for it.
    #[test]
    fn malformed_input_not_retried_four_times() {
        for e in [
            Error::BadRequest("bad filter".into()),
            Error::BadRequest(
                "unknown aggregate 'mediun' (expected 'count' or 'sum($.path)')".into(),
            ),
            Error::BadRequest("profile name '../etc' contains '/'".into()),
        ] {
            assert!(
                e.is_terminal_for_job(),
                "{e} is deterministic — retrying it re-parses the same string"
            );
        }
    }

    /// The reason `Error::Profile` is not in the terminal set even though an
    /// unsafe *name* is deterministic: the variant also carries genuinely
    /// transient IO (`engine-http`'s cookie-jar load racing its own flusher's
    /// rename). The name refusal is retyped at its seams instead; the variant
    /// keeps its retries.
    #[test]
    fn a_transient_profile_io_failure_keeps_its_retries() {
        assert!(!Error::Profile(
            "opening data/profiles/acme/cookies.json: The process cannot access the file".into()
        )
        .is_terminal_for_job());
    }

    /// The mirror risk, and the more dangerous one: over-classifying. A
    /// transient failure marked terminal loses the job its retries silently.
    #[test]
    fn transient_failures_stay_retryable_not_terminal() {
        for e in [
            Error::http("connection reset"),
            Error::browser("chrome died"),
            Error::claude(ClaudeFailure::NonZeroExit, "cli exited 1"),
            Error::App("the source returned nonsense".into()),
            Error::parse("bad html"),
            Error::config("missing key"),
            // NOT `ReplayMiss`: see `a_replay_miss_does_not_ride_the_retry_ladder`.
            // It sat here as if transient, while the load-time miss was already
            // being failed permanently by the worker — same feature, same
            // determinism, opposite handling.
            // NOT `BadRequest`: see `malformed_input_not_retried_four_times`.
            // It sat here as if transient, which its own doc never claimed —
            // "input the server understood and rejected" cannot become valid on
            // attempt 2.
            Error::Io(std::io::Error::other("disk hiccup")),
        ] {
            assert!(
                !e.is_terminal_for_job(),
                "{e} must stay retryable — marking it terminal takes the job's retries away"
            );
        }
    }

    /// The anti-pattern, and the asymmetry that gave it away: a replay whose
    /// **cassette** was missing already failed permanently (the worker resolves
    /// it before the run), while a replay that reached an **unrecorded request**
    /// was re-queued and re-ran from the top `max_attempts` times — re-doing
    /// every live-free step that preceded the miss and missing again in exactly
    /// the same place. Same feature, same determinism, opposite handling.
    ///
    /// A miss is a pure function of an immutable cassette and params frozen at
    /// enqueue: the backoff ladder cannot change the answer.
    #[test]
    fn a_replay_miss_does_not_ride_the_retry_ladder() {
        for e in [
            Error::ReplayMiss("no recorded response for GET https://x/a".into()),
            Error::ReplayMiss("cassette entry ... was truncated at record time".into()),
            Error::ReplayMiss("job ...'s cassette holds no readable entries".into()),
        ] {
            assert!(
                e.is_terminal_for_job(),
                "{e} is deterministic — retrying re-reads the same cassette"
            );
        }
    }

    /// THE anti-pattern this variant replaces: a schema-drift refusal is
    /// deterministic — the field is renamed, the params are frozen at enqueue,
    /// so attempt #2 re-parses an identically shaped response and re-refuses —
    /// but it shipped as `Error::App`, which is retryable. A permanent upstream
    /// rename therefore burned three attempts plus backoff on every scheduled
    /// run, every day, indefinitely.
    #[test]
    fn source_drift_is_terminal_not_three_identical_refusals() {
        let drift = Error::SourceDrift(
            "grants.gov schema drift: hitCount=8412 but page 1 parsed 0 oppHits".into(),
        );
        assert!(
            drift.is_terminal_for_job(),
            "a renamed field does not un-rename itself between attempt 1 and 3"
        );
        // The lookalike keeps its ladder: a source that is *down* fails in the
        // fetch chokepoint, and that one really can succeed next time.
        assert!(!Error::http("connect grants.gov: timed out").is_terminal_for_job());
        assert!(!Error::App("partial harvest".into()).is_terminal_for_job());
    }

    /// The operator-facing half of the same change: the two failures an
    /// operator has to tell apart must not render as the same kind of line.
    /// Classification is by TYPE, so it survives any rewording of the message —
    /// the prefix comes from the variant, not from app prose.
    #[test]
    fn a_drift_refusal_does_not_read_like_the_source_being_down() {
        let drift = Error::SourceDrift("result.records missing or not an array".into());
        let outage = Error::http("connect https://data.ca.gov: timed out");
        assert!(drift.to_string().starts_with("source drift:"), "{drift}");
        assert!(!outage.to_string().starts_with("source drift:"), "{outage}");
        assert_ne!(drift.is_terminal_for_job(), outage.is_terminal_for_job());
        // No message wording can move a failure between the two classes.
        for message in ["", "wholly reworded prose", "the source was down"] {
            assert!(Error::SourceDrift(message.into()).is_terminal_for_job());
        }
    }

    /// The classification EVERY variant must have, as an **exhaustive** match.
    ///
    /// This is the inventory guard (the EXPECTED-diff idiom): adding a variant
    /// to `Error` stops this compiling until someone decides whether a retry
    /// could change its answer. A `_ =>` arm here would make "retryable" the
    /// silent default again — which is exactly how `BadRequest` and then
    /// `ReplayMiss` each shipped as deterministic refusals riding the whole
    /// backoff ladder.
    fn expected_terminal(e: &Error) -> bool {
        match e {
            // Deterministic refusals: the input is immutable for the life of
            // the job, so every attempt re-derives the identical answer.
            Error::BudgetExhausted(_)
            | Error::Transact(_)
            | Error::BadRequest(_)
            | Error::ReplayMiss(_)
            | Error::SourceDrift(_) => true,
            // Everything else can genuinely succeed on the next attempt.
            Error::Http { .. }
            | Error::Browser { .. }
            | Error::Claude { .. }
            | Error::Profile(_)
            | Error::Parse { .. }
            | Error::Config { .. }
            | Error::App(_)
            | Error::Plugin { .. }
            | Error::Io(_)
            | Error::Json(_)
            | Error::Other(_) => false,
            #[cfg(feature = "storage")]
            Error::Storage(_) => false,
        }
    }

    /// One instance of every variant, checked against the exhaustive table
    /// above — so the table cannot drift from `is_terminal_for_job` itself.
    #[test]
    fn every_error_variant_has_a_decided_retry_classification() {
        let all = vec![
            Error::http("reset"),
            Error::browser("chrome died"),
            Error::claude(ClaudeFailure::NonZeroExit, "cli exited 1"),
            Error::Profile("jar unreadable".into()),
            Error::Transact("submit: true refused".into()),
            Error::parse("bad html"),
            Error::config("missing key"),
            Error::App("source returned nonsense".into()),
            Error::plugin(PluginFailure::Trap, "delta-slim", "all fuel consumed"),
            Error::BudgetExhausted("no headroom".into()),
            Error::ReplayMiss("no recorded response".into()),
            Error::BadRequest("bad filter".into()),
            Error::SourceDrift("hitCount>0 but 0 rows parsed".into()),
            Error::Io(std::io::Error::other("disk hiccup")),
            Error::Json(serde_json::from_str::<u8>("nope").unwrap_err()),
            Error::Other(anyhow::anyhow!("something else")),
            #[cfg(feature = "storage")]
            Error::Storage(sqlx::Error::RowNotFound),
        ];
        for e in &all {
            assert_eq!(
                e.is_terminal_for_job(),
                expected_terminal(e),
                "{e} is classified against the table's intent"
            );
        }
    }

    // ---- store contention -------------------------------------------------

    /// A `DatabaseError` carrying an arbitrary SQLite result code, so the
    /// classification can be driven across the whole code space without
    /// arranging a genuinely locked file for each one. The real thing is
    /// exercised end-to-end in `crates/core/tests/store_instrument_chokepoint.rs`,
    /// which takes an actual `SQLITE_BUSY` off a second connection.
    #[cfg(feature = "storage")]
    #[derive(Debug)]
    struct CodedDbError(i32);

    #[cfg(feature = "storage")]
    impl std::fmt::Display for CodedDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // Deliberately prose that says nothing recognisable: the class must
            // survive ANY message, exactly as `PluginFailure` must.
            write!(f, "something went wrong somewhere")
        }
    }

    #[cfg(feature = "storage")]
    impl std::error::Error for CodedDbError {}

    #[cfg(feature = "storage")]
    impl sqlx::error::DatabaseError for CodedDbError {
        fn message(&self) -> &str {
            "something went wrong somewhere"
        }
        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(self.0.to_string().into())
        }
        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    #[cfg(feature = "storage")]
    fn coded(code: i32) -> Error {
        Error::Storage(sqlx::Error::Database(Box::new(CodedDbError(code))))
    }

    /// The anti-pattern this classifier replaces: SQLite's own messages for the
    /// two contention codes are "database is locked" and "database table is
    /// locked" — near-identical prose the driver itself flags as ambiguous — so
    /// any classifier built on them is one upstream reword away from
    /// misfiling every lock wait in the store. The code is the fact.
    #[cfg(feature = "storage")]
    #[test]
    fn contention_is_classified_by_result_code_not_by_sqlite_prose() {
        // SQLITE_BUSY (5) and SQLITE_LOCKED (6), plus the extended forms that
        // share their low byte: BUSY_RECOVERY 261, BUSY_SNAPSHOT 517,
        // BUSY_TIMEOUT 773, LOCKED_SHAREDCACHE 262, LOCKED_VTAB 518.
        for code in [5, 6, 261, 517, 773, 262, 518] {
            assert!(
                coded(code).is_store_contention(),
                "extended code {code} has primary code {} — contention",
                code & 0xff
            );
        }
        // The pool's own refusal is contention too; the instrument's phase
        // label is what keeps a sizing finding distinct from a writer-hog one.
        assert!(Error::Storage(sqlx::Error::PoolTimedOut).is_store_contention());
    }

    /// The mirror risk, and the more dangerous one: over-classifying. A
    /// constraint violation or a syntax error counted as "busy" would send an
    /// operator to the pool sizing while the actual defect sat untouched.
    #[cfg(feature = "storage")]
    #[test]
    fn a_real_defect_is_never_counted_as_contention() {
        // SQLITE_ERROR (1), SQLITE_CONSTRAINT (19), CONSTRAINT_UNIQUE (2067),
        // SQLITE_READONLY (8), SQLITE_FULL (13), SQLITE_CORRUPT (11).
        for code in [1, 19, 2067, 8, 13, 11] {
            assert!(
                !coded(code).is_store_contention(),
                "code {code} is a defect, not contention"
            );
        }
        assert!(!Error::Storage(sqlx::Error::RowNotFound).is_store_contention());
        // And nothing outside the storage variant may answer this at all, or a
        // `match` on the outcome silently swallows unrelated failures.
        assert!(!Error::App("database is locked, allegedly".into()).is_store_contention());
        assert!(!Error::http("connection reset").is_store_contention());
    }
}
