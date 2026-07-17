//! The pgshard agent: PID 1 of every PostgreSQL pod (CNPG-style). It supervises
//! the local instance and exposes the `AgentService` the operator polls for
//! status and drives through the failover handshake. This first step implements
//! the HA path (status, epoch-guarded promote/fence/rejoin) over an [`Instance`]
//! abstraction; backups, restore, replication, DDL, and CDC follow.
//!
//! [`Instance`]: crate::instance::Instance

pub mod epoch;
pub mod instance;
pub mod pg;
pub mod service;
pub mod status;
