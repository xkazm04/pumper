#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http engine: {0}")]
    Http(String),
    #[error("browser engine: {0}")]
    Browser(String),
    #[error("claude engine: {0}")]
    Claude(String),
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
    /// Whether this failure is **deterministic and terminal for the job**: a
    /// retry cannot change the outcome, so the runtime must fail the job once
    /// instead of running it down the retry/backoff ladder.
    ///
    /// The bar is deliberately high, and today exactly two variants clear it:
    ///
    /// - [`Error::BudgetExhausted`] — a fact about the job's own ledger, which
    ///   a retry re-reads and re-refuses on.
    /// - [`Error::Transact`] — produced **only** by `TransactRequest::validate`,
    ///   whose three refusals (`submit: true`, a blank idempotency key, a
    ///   profile the vault does not hold) are pure functions of the request. The
    ///   request is immutable for the life of the job, so every attempt reaches
    ///   the identical refusal *before touching a browser*. This one matters
    ///   more than most: transact is the app that ACTS on live pages, and a
    ///   refusal is precisely the case where the ladder must not keep trying.
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
    use super::Error;

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
            Error::Claude("cli exited 1".into()),
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
