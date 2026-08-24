//! The activity gauge: a live count of in-flight **foreground** work.
//!
//! This exists because a wall clock does not know about interactions. Both
//! janitors used to fire on a bare `sleep(interval)`, so over enough sessions
//! one was guaranteed to land mid-scrape holding the writer lock, and the stall
//! got charged to whatever the operator was doing. The fix is not a better
//! interval — it is to stop scheduling and start **measuring** (registry:
//! embedded-db/quiet-window-maintenance).
//!
//! ## What it observes, and why not a proxy
//!
//! `gate-sees-target`: the gate must observe *actual demand for the machine*,
//! not something correlated with it. The proxies that fail in practice are all
//! tempting and all wrong here — time-of-day (this server is scheduled and runs
//! at night by design), "idle since the last query" measured inside the storage
//! layer (misses a job that is CPU-bound in an extractor and will need the
//! database in 200ms), OS-level idle (fires while a long crawl runs
//! unattended).
//!
//! So it is fed at the application's own front doors, and only those:
//!
//! - **HTTP requests being handled** — one middleware layer around the whole
//!   router, incremented on entry and decremented when the response is
//!   produced.
//! - **Jobs currently running** — incremented where the worker spawns a run and
//!   decremented when `execute` returns, which is the same seam that already
//!   owns the per-app running counts and the cancel-token registry.
//!
//! A third signal lives elsewhere and is deliberately not duplicated here:
//! **pool saturation**, which `StoreInstrument::pool_saturated` derives from
//! the acquire-phase rings. A saturated pool is the strongest possible "not a
//! quiet window" signal and is invisible to a counter of requests and jobs, so
//! the gate reads both.
//!
//! ## Why a guard and not a pair of calls
//!
//! Every increment is bound to an RAII [`ActivityGuard`]. A manual decrement is
//! one `?` away from being skipped on an error path, and a gauge that leaks a
//! single count never reads zero again — which does not fail loudly, it just
//! silently converts quiet-window maintenance into no maintenance. The guard
//! makes the decrement structural, including on unwind.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// A count of in-flight foreground work. Cheap enough to read on every gate
/// tick: one relaxed atomic load.
#[derive(Debug, Default)]
pub struct ActivityGauge {
    /// Signed on purpose. An unbalanced decrement is a bug, and `i64` lets the
    /// gauge *show* it as a negative rather than wrapping to `u64::MAX` and
    /// reading as "permanently, catastrophically busy" — which would disable
    /// maintenance forever with no visible cause.
    inflight: AtomicI64,
}

impl ActivityGauge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one unit of in-flight foreground work until the returned guard
    /// is dropped.
    pub fn enter(self: &Arc<Self>) -> ActivityGuard {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        ActivityGuard {
            gauge: self.clone(),
        }
    }

    /// The current reading, clamped at zero.
    ///
    /// Clamped because the gate's question is "is anything happening", and a
    /// negative reading (an unbalanced decrement) must never answer "less than
    /// nothing is happening, so run twice as freely". [`Self::raw`] keeps the
    /// unclamped value for the diagnostic surface, so the bug stays visible
    /// where it can be read instead of being laundered here.
    pub fn reading(&self) -> u64 {
        self.inflight.load(Ordering::Acquire).max(0) as u64
    }

    /// The unclamped counter, for diagnostics. A negative value is a leaked
    /// decrement and is a bug worth seeing.
    pub fn raw(&self) -> i64 {
        self.inflight.load(Ordering::Acquire)
    }
}

/// Holds one unit of the gauge for its lifetime. Decrements on drop —
/// including on an early return, a `?`, or a panic unwinding through the
/// handler.
#[derive(Debug)]
pub struct ActivityGuard {
    gauge: Arc<ActivityGauge>,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.gauge.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The axum layer that feeds the gauge from the HTTP front door.
///
/// Applied once, outermost, around the whole router — including `/metrics` and
/// `/health`. Counting the scrape is correct rather than pedantic: rendering
/// `/metrics` runs three aggregate queries against the same store maintenance
/// would compete with, so a scrape genuinely IS demand for the machine.
///
/// The guard is dropped when the handler produces its response, not when the
/// body finishes streaming. That is deliberate for the one case where it
/// differs: an SSE subscriber holds an open stream for hours while asking
/// nothing of the store, and counting it would mean this process never sees a
/// quiet window again.
pub fn with_activity<S>(router: axum::Router<S>, gauge: Arc<ActivityGauge>) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let gauge = gauge.clone();
            async move {
                let _busy = gauge.enter();
                next.run(req).await
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gauge has to read zero again when work finishes, or quiet-window
    /// maintenance silently becomes no maintenance — the failure that does not
    /// announce itself and surfaces months later as a disk-full report.
    #[test]
    fn the_gauge_returns_to_zero_when_work_finishes() {
        let gauge = Arc::new(ActivityGauge::new());
        assert_eq!(gauge.reading(), 0);
        let a = gauge.enter();
        let b = gauge.enter();
        assert_eq!(gauge.reading(), 2);
        drop(a);
        assert_eq!(gauge.reading(), 1);
        drop(b);
        assert_eq!(gauge.reading(), 0, "a leaked count never reads zero again");
    }

    /// The decrement must be structural. A manual one is a single `?` away from
    /// being skipped, and an error path that skips it pins the gauge above zero
    /// for the life of the process.
    #[test]
    fn an_error_path_still_releases_its_count() {
        let gauge = Arc::new(ActivityGauge::new());
        fn fallible(gauge: &Arc<ActivityGauge>) -> Result<(), &'static str> {
            let _busy = gauge.enter();
            Err("the early return every manual decrement eventually meets")
        }
        assert!(fallible(&gauge).is_err());
        assert_eq!(gauge.reading(), 0);
    }

    /// A panic unwinding through a handler must not pin the gauge either — the
    /// HTTP stack catches panics and keeps serving, so a leaked count here
    /// would outlive the request that caused it with nothing to blame.
    #[test]
    fn a_panicking_handler_still_releases_its_count() {
        let gauge = Arc::new(ActivityGauge::new());
        let g = gauge.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _busy = g.enter();
            panic!("handler blew up");
        }));
        assert!(result.is_err());
        assert_eq!(gauge.reading(), 0);
    }

    /// An unbalanced decrement is a bug, and it must present as a visible
    /// negative rather than wrapping to a colossal unsigned value that reads as
    /// "permanently busy" and disables maintenance forever with no cause an
    /// operator could find.
    #[test]
    fn an_unbalanced_decrement_shows_as_negative_not_as_permanently_busy() {
        let gauge = Arc::new(ActivityGauge::new());
        gauge.inflight.fetch_sub(1, Ordering::AcqRel);
        assert_eq!(gauge.raw(), -1, "the bug stays visible");
        assert_eq!(
            gauge.reading(),
            0,
            "but the gate reads 'nothing is happening', not u64::MAX"
        );
    }
}
