//! PostgreSQL wire-protocol plumbing for pgshard: framing, startup
//! negotiation, backend authentication, and (for the phase-1 spike) a
//! transparent passthrough proxy.

pub mod backend;
pub mod frame;
pub mod proxy;
pub mod startup;

pub use proxy::{ProxyConfig, ProxyError, run, run_on_listener};
