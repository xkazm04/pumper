//! End-to-end tests over the server's composition layer — the joints between
//! well-tested storage and the outside world: the worker's success fan-out,
//! the webhook wire contract, the router's HTTP surface, the scheduler's
//! overlap guard, and the graceful-shutdown drain.
//!
//! Lives under `src/` (not `tests/`) because `pumper-server` is a binary-only
//! crate; unit-test position gives full crate access. Everything runs headless
//! over `AppState::from_parts` + `pumper_core::testing` — no Chrome, no
//! network beyond loopback, no real engines.

mod app_fetch_chokepoint;
mod body_limit;
mod budget_terminal;
mod datahub_bridge;
mod dataset_reads;
mod durable;
mod dynamic_apps;
mod enqueue_door_parity;
mod error_contract;
mod fanout_offslot;
mod fetch_proxy;
mod harness;
mod host_memory_reset;
mod host_weather;
mod ingress_gates;
mod job_budget_floor;
mod job_receipt;
mod mcp;
mod mcp_live;
mod panic_containment;
mod peer_mirror;
mod provisioner_lifecycle;
mod request_panics;
mod router;
mod schedule_truth;
mod scheduler_overlap;
mod search_degraded;
mod shutdown_bounds;
mod shutdown_drain;
mod sink_delivery;
mod trigger_cache;
mod trigger_hops;
mod trigger_ledger;
mod trigger_plugins;
mod vcr_attempts;
mod watch_honesty;
mod webhook_contract;
mod worker_fanout;
mod worker_lifecycle;
