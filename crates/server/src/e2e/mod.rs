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
mod datahub_bridge;
mod dataset_reads;
mod durable;
mod dynamic_apps;
mod fanout_offslot;
mod fetch_proxy;
mod harness;
mod host_weather;
mod ingress_gates;
mod job_receipt;
mod mcp;
mod mcp_live;
mod panic_containment;
mod router;
mod scheduler_overlap;
mod shutdown_drain;
mod sink_delivery;
mod trigger_cache;
mod trigger_hops;
mod trigger_ledger;
mod trigger_plugins;
mod webhook_contract;
mod worker_fanout;
mod worker_lifecycle;
