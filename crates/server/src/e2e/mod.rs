//! End-to-end tests over the server's composition layer — the joints between
//! well-tested storage and the outside world: the worker's success fan-out,
//! the webhook wire contract, the router's HTTP surface, the scheduler's
//! overlap guard, and the graceful-shutdown drain.
//!
//! Lives under `src/` (not `tests/`) because `pumper-server` is a binary-only
//! crate; unit-test position gives full crate access. Everything runs headless
//! over `AppState::from_parts` + `pumper_core::testing` — no Chrome, no
//! network beyond loopback, no real engines.

mod body_limit;
mod durable;
mod fetch_proxy;
mod dynamic_apps;
mod harness;
mod host_weather;
mod mcp;
mod mcp_live;
mod router;
mod scheduler_overlap;
mod shutdown_drain;
mod sink_delivery;
mod webhook_contract;
mod worker_fanout;
